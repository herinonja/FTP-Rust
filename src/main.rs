use anyhow::{anyhow, Context};
use async_trait::async_trait;
use libunftp::ServerBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use unftp_core::auth::UserDetail;
use unftp_core::storage::{Error, ErrorKind, Fileinfo, StorageBackend};
use wtransport::{Connection, Endpoint, Identity, ServerConfig};

const DEFAULT_FTP_BIND: &str = "0.0.0.0:2120";
const DEFAULT_WEBTRANSPORT_BIND: &str = "0.0.0.0:4433";
const DEFAULT_KODI_HOST: &str = "127.0.0.1";
const DEFAULT_KODI_PORT: u16 = 8080;

type Registry = Arc<PhoneRegistry>;

#[derive(Debug, Clone)]
struct KodiConfig {
    host: String,
    port: u16,
}

#[derive(Debug)]
struct PhoneSession {
    id: String,
    display_name: String,
    connection: Arc<Connection>,
    request_lock: Mutex<()>,
}

#[derive(Debug, Default)]
struct PhoneRegistry {
    phones: RwLock<HashMap<String, Arc<PhoneSession>>>,
}

impl PhoneRegistry {
    async fn register(&self, registration: PhoneRegister, connection: Arc<Connection>) {
        let id = safe_device_id(&registration.device_id);
        let display_name = registration
            .display_name
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| id.clone());

        let session = Arc::new(PhoneSession {
            id: id.clone(),
            display_name,
            connection,
            request_lock: Mutex::new(()),
        });

        self.phones.write().await.insert(id, session);
    }

    async fn unregister_connection(&self, connection: &Arc<Connection>) {
        self.phones
            .write()
            .await
            .retain(|_, phone| !Arc::ptr_eq(&phone.connection, connection));
    }

    async fn list(&self) -> Vec<Arc<PhoneSession>> {
        let mut phones: Vec<_> = self.phones.read().await.values().cloned().collect();
        phones.sort_by(|a, b| a.display_name.cmp(&b.display_name));
        phones
    }

    async fn get(&self, id: &str) -> Option<Arc<PhoneSession>> {
        self.phones.read().await.get(id).cloned()
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum RadxaRequest {
    #[serde(rename = "phone.register")]
    PhoneRegister {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "displayName")]
        display_name: Option<String>,
    },
    #[serde(rename = "kodi.command")]
    KodiCommand { command: String },
    #[serde(rename = "kodi.jsonrpc")]
    KodiJsonRpc {
        method: String,
        params: Option<Value>,
    },
}

#[derive(Debug)]
struct PhoneRegister {
    device_id: String,
    display_name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum PhoneRequest<'a> {
    #[serde(rename = "media.list")]
    MediaList { path: &'a str },
    #[serde(rename = "media.stat")]
    MediaStat { path: &'a str },
    #[serde(rename = "media.get")]
    MediaGet {
        path: &'a str,
        #[serde(rename = "start")]
        start_pos: u64,
    },
}

#[derive(Debug, Deserialize)]
struct PhoneResponse {
    ok: Option<bool>,
    error: Option<String>,
    entries: Option<Vec<PhoneEntry>>,
    #[serde(rename = "isDirectory")]
    is_directory: Option<bool>,
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PhoneEntry {
    name: String,
    #[serde(rename = "isDirectory")]
    is_directory: bool,
    #[serde(default)]
    size: u64,
}

#[derive(Debug, Clone)]
pub struct ProxyStorage {
    registry: Registry,
}

impl ProxyStorage {
    fn new(registry: Registry) -> Self {
        Self { registry }
    }

    async fn resolve_path(&self, path: &Path) -> unftp_core::storage::Result<(String, String)> {
        let mut components = path.components();
        if path.is_absolute() {
            components.next();
        }

        let phone_component = components.next().ok_or_else(|| {
            Error::new(
                ErrorKind::FileNameNotAllowedError,
                "telephone manquant dans le chemin",
            )
        })?;

        let phone_id = phone_component.as_os_str().to_string_lossy().into_owned();
        let target_path = components
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");

        Ok((phone_id, format!("/{target_path}")))
    }

    async fn phone(&self, id: &str) -> unftp_core::storage::Result<Arc<PhoneSession>> {
        self.registry.get(id).await.ok_or_else(|| {
            Error::new(
                ErrorKind::ConnectionClosed,
                format!("telephone WebTransport indisponible: {id}"),
            )
        })
    }
}

#[derive(Debug)]
pub struct ProxyMetadata {
    pub is_dir: bool,
    pub size: u64,
}

impl unftp_core::storage::Metadata for ProxyMetadata {
    fn len(&self) -> u64 {
        self.size
    }

    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn is_file(&self) -> bool {
        !self.is_dir
    }

    fn is_symlink(&self) -> bool {
        false
    }

    fn modified(&self) -> unftp_core::storage::Result<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }

    fn gid(&self) -> u32 {
        0
    }

    fn uid(&self) -> u32 {
        0
    }
}

#[async_trait]
impl<User: UserDetail + Send + Sync + Debug> StorageBackend<User> for ProxyStorage {
    type Metadata = ProxyMetadata;

    fn name(&self) -> &str {
        "TrooznWebTransportProxy"
    }

    async fn get<P>(
        &self,
        _user: &User,
        path: P,
        start_pos: u64,
    ) -> unftp_core::storage::Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>>
    where
        P: AsRef<Path> + Send,
    {
        let (phone_id, target_path) = self.resolve_path(path.as_ref()).await?;
        let phone = self.phone(&phone_id).await?;
        let (reader, mut writer) = tokio::io::duplex(1024 * 1024);

        tokio::spawn(async move {
            let result = async {
                let _guard = phone.request_lock.lock().await;
                let (mut tx, mut rx) = phone
                    .connection
                    .open_bi()
                    .await
                    .context("ouverture du flux media.get")?
                    .await
                    .context("initialisation du flux media.get")?;
                write_json(
                    &mut tx,
                    &PhoneRequest::MediaGet {
                        path: &target_path,
                        start_pos,
                    },
                )
                .await?;

                let mut buffer = vec![0u8; 128 * 1024];
                loop {
                    match rx.read(&mut buffer).await? {
                        Some(0) | None => break,
                        Some(bytes_read) => writer.write_all(&buffer[..bytes_read]).await?,
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;

            if let Err(error) = result {
                eprintln!("media.get failed: {error:?}");
            }
        });

        Ok(Box::new(reader))
    }

    async fn metadata<P>(
        &self,
        _user: &User,
        path: P,
    ) -> unftp_core::storage::Result<Self::Metadata>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        if is_root(path) {
            return Ok(ProxyMetadata {
                is_dir: true,
                size: 0,
            });
        }

        let (phone_id, target_path) = self.resolve_path(path).await?;
        if target_path == "/" {
            return Ok(ProxyMetadata {
                is_dir: true,
                size: 0,
            });
        }

        let phone = self.phone(&phone_id).await?;
        let stat = phone_json_request(
            &phone,
            &PhoneRequest::MediaStat { path: &target_path },
            Duration::from_secs(8),
        )
        .await
        .map_err(storage_error)?;

        Ok(ProxyMetadata {
            is_dir: stat.is_directory.unwrap_or(true),
            size: stat.size.unwrap_or(0),
        })
    }

    async fn list<P>(
        &self,
        _user: &User,
        path: P,
    ) -> unftp_core::storage::Result<Vec<Fileinfo<PathBuf, Self::Metadata>>>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        if is_root(path) {
            return Ok(self
                .registry
                .list()
                .await
                .into_iter()
                .map(|phone| Fileinfo {
                    path: PathBuf::from(phone.id.clone()),
                    metadata: ProxyMetadata {
                        is_dir: true,
                        size: 0,
                    },
                })
                .collect());
        }

        let (phone_id, target_path) = self.resolve_path(path).await?;
        let phone = self.phone(&phone_id).await?;
        let response = phone_json_request(
            &phone,
            &PhoneRequest::MediaList { path: &target_path },
            Duration::from_secs(12),
        )
        .await
        .map_err(storage_error)?;

        Ok(response
            .entries
            .unwrap_or_default()
            .into_iter()
            .filter(|entry| entry.name != "." && entry.name != "..")
            .map(|entry| Fileinfo {
                path: PathBuf::from(entry.name),
                metadata: ProxyMetadata {
                    is_dir: entry.is_directory,
                    size: entry.size,
                },
            })
            .collect())
    }

    async fn cwd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Ok(())
    }

    async fn rename<P>(&self, _user: &User, _from: P, _to: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }

    async fn put<P, R>(
        &self,
        _user: &User,
        _input: R,
        _path: P,
        _start_pos: u64,
    ) -> unftp_core::storage::Result<u64>
    where
        P: AsRef<Path> + Send,
        R: tokio::io::AsyncRead + Send + Sync + Unpin,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }

    async fn del<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }

    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }

    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ftp_bind = std::env::var("TROOZN_FTP_BIND").unwrap_or_else(|_| DEFAULT_FTP_BIND.into());
    let webtransport_bind = std::env::var("TROOZN_WEBTRANSPORT_BIND")
        .unwrap_or_else(|_| DEFAULT_WEBTRANSPORT_BIND.into());
    let kodi = KodiConfig {
        host: std::env::var("TROOZN_KODI_HOST").unwrap_or_else(|_| DEFAULT_KODI_HOST.into()),
        port: std::env::var("TROOZN_KODI_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_KODI_PORT),
    };
    let registry: Registry = Arc::new(PhoneRegistry::default());

    let wt_registry = registry.clone();
    let wt_kodi = kodi.clone();
    let wt_bind = webtransport_bind.clone();
    tokio::spawn(async move {
        if let Err(error) = run_webtransport_server(&wt_bind, wt_registry, wt_kodi).await {
            eprintln!("Erreur serveur WebTransport TROOZN: {error:?}");
        }
    });

    println!("=============================================================");
    println!(" TROOZN RADXA PROXY");
    println!(" Kodi lit: ftp://{ftp_bind}/");
    println!(" Telephones: WebTransport sur https://{webtransport_bind}/");
    println!(" Kodi JSON-RPC local: {}:{}", kodi.host, kodi.port);
    println!("=============================================================");

    let server = ServerBuilder::new(Box::new(move || ProxyStorage::new(registry.clone())))
        .build()
        .context("creation du serveur FTP Kodi")?;

    server
        .listen(&ftp_bind)
        .await
        .context("ecoute du serveur FTP Kodi")?;
    Ok(())
}

async fn run_webtransport_server(
    bind: &str,
    registry: Registry,
    kodi: KodiConfig,
) -> anyhow::Result<()> {
    let identity = Identity::self_signed(["localhost", "127.0.0.1", "0.0.0.0"])?;
    let cert_hash = identity
        .certificate_chain()
        .as_slice()
        .first()
        .ok_or_else(|| anyhow!("certificat WebTransport manquant"))?
        .hash();

    println!("Empreinte WebTransport TROOZN a copier dans les telephones:");
    println!("{cert_hash}");

    let config = ServerConfig::builder()
        .with_bind_address(bind.parse()?)
        .with_identity(&identity)
        .build();
    let endpoint = Endpoint::server(config)?;

    loop {
        let incoming_session = endpoint.accept().await;
        let registry = registry.clone();
        let kodi = kodi.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_webtransport_client(incoming_session, registry, kodi).await {
                eprintln!("Session WebTransport terminee: {error:?}");
            }
        });
    }
}

async fn handle_webtransport_client(
    incoming_session: wtransport::endpoint::IncomingSession,
    registry: Registry,
    kodi: KodiConfig,
) -> anyhow::Result<()> {
    let session_request = incoming_session.await?;
    let connection = Arc::new(session_request.accept().await?);

    loop {
        let (mut tx, mut rx) = match connection.accept_bi().await {
            Ok(stream) => stream,
            Err(error) => {
                registry.unregister_connection(&connection).await;
                return Err(error.into());
            }
        };

        let registry = registry.clone();
        let kodi = kodi.clone();
        let connection = connection.clone();
        tokio::spawn(async move {
            let result = async {
                let raw = read_stream_text(&mut rx).await?;
                let request: RadxaRequest =
                    serde_json::from_str(&raw).context("requete WebTransport invalide")?;
                match request {
                    RadxaRequest::PhoneRegister {
                        device_id,
                        display_name,
                    } => {
                        registry
                            .register(
                                PhoneRegister {
                                    device_id,
                                    display_name,
                                },
                                connection,
                            )
                            .await;
                        tx.write_all(json!({"ok":true}).to_string().as_bytes())
                            .await?;
                    }
                    RadxaRequest::KodiCommand { command } => {
                        let response = run_kodi_command(&kodi, &command).await?;
                        tx.write_all(response.to_string().as_bytes()).await?;
                    }
                    RadxaRequest::KodiJsonRpc { method, params } => {
                        let response =
                            kodi_json_rpc(&kodi, &method, params.unwrap_or(Value::Null)).await?;
                        tx.write_all(response.to_string().as_bytes()).await?;
                    }
                }
                tx.finish().await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;

            if let Err(error) = result {
                let _ = tx
                    .write_all(
                        json!({"ok":false,"error":error.to_string()})
                            .to_string()
                            .as_bytes(),
                    )
                    .await;
                let _ = tx.finish().await;
            }
        });
    }
}

async fn phone_json_request(
    phone: &PhoneSession,
    request: &PhoneRequest<'_>,
    timeout: Duration,
) -> anyhow::Result<PhoneResponse> {
    let _guard = phone.request_lock.lock().await;
    let (mut tx, mut rx) = tokio::time::timeout(timeout, phone.connection.open_bi())
        .await
        .context("timeout ouverture flux telephone")??
        .await
        .context("initialisation flux telephone")?;
    write_json(&mut tx, request).await?;

    let raw = tokio::time::timeout(timeout, read_stream_text(&mut rx))
        .await
        .context("timeout reponse telephone")??;
    let response: PhoneResponse = serde_json::from_str(&raw).context("JSON telephone invalide")?;
    if response.ok == Some(false) {
        return Err(anyhow!(
            "{}",
            response.error.unwrap_or_else(|| "erreur telephone".into())
        ));
    }
    Ok(response)
}

async fn write_json<T: Serialize>(
    tx: &mut wtransport::SendStream,
    value: &T,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(value)?;
    tx.write_all(&body).await?;
    tx.finish().await?;
    Ok(())
}

async fn read_stream_text(rx: &mut wtransport::RecvStream) -> anyhow::Result<String> {
    let mut response = Vec::new();
    let mut buffer = vec![0u8; 64 * 1024];
    while let Some(bytes_read) = rx.read(&mut buffer).await? {
        if bytes_read == 0 {
            break;
        }
        response.extend_from_slice(&buffer[..bytes_read]);
    }
    String::from_utf8(response).context("reponse non UTF-8")
}

async fn run_kodi_command(kodi: &KodiConfig, command: &str) -> anyhow::Result<Value> {
    let normalized = command.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "CMD_UP" => kodi_json_rpc(kodi, "Input.Up", Value::Null).await,
        "CMD_DOWN" => kodi_json_rpc(kodi, "Input.Down", Value::Null).await,
        "CMD_LEFT" => kodi_json_rpc(kodi, "Input.Left", Value::Null).await,
        "CMD_RIGHT" => kodi_json_rpc(kodi, "Input.Right", Value::Null).await,
        "CMD_SELECT" => kodi_json_rpc(kodi, "Input.Select", Value::Null).await,
        "CMD_BACK" => kodi_json_rpc(kodi, "Input.Back", Value::Null).await,
        "CMD_HOME" => kodi_json_rpc(kodi, "Input.Home", Value::Null).await,
        "CMD_PLAY_PAUSE" | "CMD_PLAYPAUSE" => {
            kodi_json_rpc(
                kodi,
                "Player.PlayPause",
                json!({"playerid": 0, "play": "toggle"}),
            )
            .await
        }
        "CMD_STOP" => kodi_json_rpc(kodi, "Player.Stop", json!({"playerid": 0})).await,
        other => Err(anyhow!("commande inconnue: {other}")),
    }
}

async fn kodi_json_rpc(kodi: &KodiConfig, method: &str, params: Value) -> anyhow::Result<Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": "troozn-webtransport",
        "method": method,
        "params": params,
    })
    .to_string();

    let mut stream = TcpStream::connect((kodi.host.as_str(), kodi.port))
        .await
        .with_context(|| format!("Kodi indisponible sur {}:{}", kodi.host, kodi.port))?;
    let request = format!(
        "POST /jsonrpc HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        kodi.host,
        kodi.port,
        body.as_bytes().len(),
        body,
    );
    stream.write_all(request.as_bytes()).await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let text = String::from_utf8_lossy(&response);
    let (_, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow!("reponse HTTP Kodi invalide"))?;
    let value: Value = serde_json::from_str(body).context("JSON-RPC Kodi invalide")?;
    Ok(json!({"ok": true, "kodi": value}))
}

fn is_root(path: &Path) -> bool {
    path == Path::new("") || path == Path::new("/")
}

fn safe_device_id(value: &str) -> String {
    let safe = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if safe.is_empty() {
        "phone".into()
    } else {
        safe
    }
}

fn storage_error(error: anyhow::Error) -> Error {
    Error::new(ErrorKind::LocalError, error.to_string())
}
