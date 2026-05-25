use anyhow::{anyhow, Context};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use libunftp::ServerBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hasher;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StdUdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Mutex, RwLock};
use tokio_util::io::ReaderStream;
use unftp_core::auth::UserDetail;
use unftp_core::storage::{Error, ErrorKind, Fileinfo, StorageBackend, FEATURE_RESTART};
use wtransport::tls::Sha256DigestFmt;
use wtransport::{Connection, Endpoint, Identity, ServerConfig};

const DEFAULT_FTP_BIND: &str = "127.0.0.1:2120";
const DEFAULT_HTTP_BIND: &str = "127.0.0.1:8787";
const DEFAULT_UPNP_BIND: &str = "0.0.0.0:8788";
const DEFAULT_WEBTRANSPORT_BIND: &str = "0.0.0.0:4433";
const DEFAULT_KODI_HOST: &str = "127.0.0.1";
const DEFAULT_KODI_PORT: u16 = 8080;
const WEBTRANSPORT_CERT_MAX_AGE_SECONDS: u64 = 13 * 24 * 60 * 60;
const TROOZN_PROTOCOL_VERSION: u8 = 1;
const MEDIA_STREAM_BUFFER_BYTES: usize = 2 * 1024 * 1024;
const MEDIA_COPY_BUFFER_BYTES: usize = 512 * 1024;
const MEDIA_STAT_TIMEOUT: Duration = Duration::from_secs(5);
const MEDIA_LIST_TIMEOUT: Duration = Duration::from_secs(10);
const PHONE_DISCONNECT_GRACE_SECONDS: u64 = 30;
const MIN_HEAD_CACHE_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_HEAD_CACHE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_HEAD_CACHE_BYTES: u64 = 50 * 1024 * 1024;
const DEFAULT_HEAD_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const UPNP_ROOT_UUID: &str = "fdab0000-7472-6f6f-7a6e-000000000001";
const UPNP_BOOT_ID: u32 = 1;
const UPNP_CONFIG_ID: u32 = 1;
const UPNP_ADVERTISE_INTERVAL: Duration = Duration::from_secs(30);
const UPNP_MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 255, 250);
const UPNP_MULTICAST_PORT: u16 = 1900;

type Registry = Arc<PhoneRegistry>;

#[derive(Debug)]
struct HeadCache {
    dir: PathBuf,
    file_bytes: u64,
    max_bytes: u64,
    locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    prefetching: RwLock<HashSet<String>>,
}

#[derive(Debug, Clone)]
struct HttpGatewayState {
    registry: Registry,
    cache: Arc<HeadCache>,
}

#[derive(Debug, Clone)]
struct UpnpState {
    registry: Registry,
    upnp_base_url: String,
    media_base_url: String,
}

#[derive(Debug, Clone)]
struct UpnpDeviceInfo {
    key: String,
    uuid: String,
    friendly_name: String,
    phone_id: Option<String>,
}

#[derive(Debug, Clone)]
struct UpnpDidlItem {
    id: String,
    parent_id: String,
    title: String,
    upnp_class: String,
    is_container: bool,
    child_count: usize,
    protocol_info: Option<String>,
    url: Option<String>,
    size: Option<u64>,
}

#[derive(Debug, Clone)]
struct KodiConfig {
    host: String,
    port: u16,
    username: Option<String>,
    password: String,
}

#[derive(Debug)]
struct PhoneSession {
    id: String,
    display_name: String,
    folder_name: String,
    role: String,
    source_visible: bool,
    connection: Arc<Connection>,
    disconnected_at: AtomicU64,
}

#[derive(Debug, Default)]
struct PhoneRegistry {
    phones: RwLock<HashMap<String, Arc<PhoneSession>>>,
    admin_device_id: RwLock<Option<String>>,
}

impl PhoneRegistry {
    async fn register(
        &self,
        registration: PhoneRegister,
        connection: Arc<Connection>,
    ) -> anyhow::Result<()> {
        let id = safe_device_id(&registration.device_id);
        let role = normalize_phone_role(&registration.role);
        if role == "admin" {
            let mut admin_device_id = self.admin_device_id.write().await;
            match admin_device_id.as_deref() {
                Some(existing) if existing != id => {
                    return Err(anyhow!("admin_already_claimed"));
                }
                Some(_) => {}
                None => {
                    *admin_device_id = Some(id.clone());
                    println!("Admin TROOZN verrouille pour ce lancement: {id}");
                }
            }
        }
        let display_name = registration
            .display_name
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| id.clone());
        let mut phones = self.phones.write().await;
        let folder_name = unique_folder_name(&display_name, &id, &phones);

        let session = Arc::new(PhoneSession {
            id: id.clone(),
            display_name,
            folder_name,
            role,
            source_visible: registration.source_visible,
            connection,
            disconnected_at: AtomicU64::new(0),
        });

        println!(
            "Telephone WebTransport enregistre: {} -> {} (role={}, sources={})",
            session.display_name,
            session.folder_name,
            session.role,
            if session.source_visible { "visible" } else { "masque" }
        );
        phones.insert(id, session);
        Ok(())
    }

    async fn unregister_connection(&self, connection: &Arc<Connection>) {
        let now = unix_timestamp();
        let phones = self.phones.read().await;
        for phone in phones.values() {
            if Arc::ptr_eq(&phone.connection, connection) {
                phone.disconnected_at.store(now, Ordering::Relaxed);
                println!(
                    "Telephone WebTransport marque deconnecte: {} (grace {}s)",
                    phone.folder_name, PHONE_DISCONNECT_GRACE_SECONDS
                );
            }
        }
    }

    async fn list(&self) -> Vec<Arc<PhoneSession>> {
        let mut phones = self.phones.write().await;
        prune_stale_phones(&mut phones);
        let mut phones: Vec<_> = phones
            .values()
            .filter(|phone| phone_is_published(phone))
            .cloned()
            .collect();
        phones.sort_by(|a, b| a.display_name.cmp(&b.display_name));
        phones
    }

    async fn get_by_folder(&self, folder_name: &str) -> Option<Arc<PhoneSession>> {
        let mut phones = self.phones.write().await;
        prune_stale_phones(&mut phones);
        phones
            .values()
            .filter(|phone| phone_is_connected(phone))
            .find(|phone| phone.folder_name == folder_name)
            .cloned()
            .or_else(|| {
                phones
                    .get(folder_name)
                    .filter(|phone| phone_is_connected(phone))
                    .cloned()
            })
    }

    async fn get_by_id(&self, id: &str) -> Option<Arc<PhoneSession>> {
        let mut phones = self.phones.write().await;
        prune_stale_phones(&mut phones);
        phones.get(id).filter(|phone| phone_is_published(phone)).cloned()
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
        #[serde(default = "default_phone_role")]
        role: String,
        #[serde(default = "default_source_visible", rename = "sourceVisible")]
        source_visible: bool,
    },
    #[serde(rename = "kodi.command")]
    KodiCommand { command: String },
    #[serde(rename = "kodi.jsonrpc")]
    KodiJsonRpc {
        method: String,
        params: Option<Value>,
    },
    #[serde(rename = "proxy.list")]
    ProxyList { path: String },
}

#[derive(Debug)]
struct PhoneRegister {
    device_id: String,
    display_name: Option<String>,
    role: String,
    source_visible: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum PhoneRequest<'a> {
    #[serde(rename = "media.list")]
    MediaList {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        path: &'a str,
    },
    #[serde(rename = "media.stat")]
    MediaStat {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        path: &'a str,
    },
    #[serde(rename = "media.get")]
    MediaGet {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        path: &'a str,
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
        self.registry.get_by_folder(id).await.ok_or_else(|| {
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

    fn supported_features(&self) -> u32 {
        FEATURE_RESTART
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
        let (reader, mut writer) = tokio::io::duplex(MEDIA_STREAM_BUFFER_BYTES);

        tokio::spawn(async move {
            let result = async {
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
                        protocol_version: TROOZN_PROTOCOL_VERSION,
                        path: &target_path,
                        start_pos,
                    },
                )
                .await?;

                let mut buffer = vec![0u8; MEDIA_COPY_BUFFER_BYTES];
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
            &PhoneRequest::MediaStat {
                protocol_version: TROOZN_PROTOCOL_VERSION,
                path: &target_path,
            },
            MEDIA_STAT_TIMEOUT,
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
                    path: PathBuf::from(phone.folder_name.clone()),
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
            &PhoneRequest::MediaList {
                protocol_version: TROOZN_PROTOCOL_VERSION,
                path: &target_path,
            },
            MEDIA_LIST_TIMEOUT,
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
    let http_bind = std::env::var("TROOZN_HTTP_BIND").unwrap_or_else(|_| DEFAULT_HTTP_BIND.into());
    let upnp_bind =
        std::env::var("TROOZN_UPNP_BIND").unwrap_or_else(|_| DEFAULT_UPNP_BIND.into());
    let webtransport_bind = std::env::var("TROOZN_WEBTRANSPORT_BIND")
        .unwrap_or_else(|_| DEFAULT_WEBTRANSPORT_BIND.into());
    let state_dir = troozn_state_dir();
    let head_cache_dir = std::env::var("TROOZN_HEAD_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| state_dir.join("head-cache"));
    let head_cache_file_bytes = configured_cache_bytes(
        "TROOZN_HEAD_CACHE_BYTES",
        DEFAULT_HEAD_CACHE_BYTES,
        Some((MIN_HEAD_CACHE_BYTES, MAX_HEAD_CACHE_BYTES)),
    );
    let head_cache_max_bytes = configured_cache_bytes(
        "TROOZN_HEAD_CACHE_MAX_BYTES",
        DEFAULT_HEAD_CACHE_MAX_BYTES,
        None,
    );
    let webtransport_cert_dir = std::env::var("TROOZN_WEBTRANSPORT_CERT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| state_dir.join("webtransport"));
    let proxy_status_path = std::env::var("TROOZN_PROXY_STATUS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| state_dir.join("proxy-status.json"));
    let kodi = KodiConfig {
        host: std::env::var("TROOZN_KODI_HOST").unwrap_or_else(|_| DEFAULT_KODI_HOST.into()),
        port: std::env::var("TROOZN_KODI_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_KODI_PORT),
        username: std::env::var("TROOZN_KODI_USERNAME")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        password: std::env::var("TROOZN_KODI_PASSWORD").unwrap_or_default(),
    };
    let registry: Registry = Arc::new(PhoneRegistry::default());
    let head_cache = Arc::new(HeadCache {
        dir: head_cache_dir.clone(),
        file_bytes: head_cache_file_bytes,
        max_bytes: head_cache_max_bytes,
        locks: RwLock::new(HashMap::new()),
        prefetching: RwLock::new(HashSet::new()),
    });

    let wt_registry = registry.clone();
    let wt_kodi = kodi.clone();
    let wt_bind = webtransport_bind.clone();
    let wt_cert_dir = webtransport_cert_dir.clone();
    let wt_status_path = proxy_status_path.clone();
    tokio::spawn(async move {
        if let Err(error) = run_webtransport_server(
            &wt_bind,
            &wt_cert_dir,
            &wt_status_path,
            wt_registry,
            wt_kodi,
        )
        .await
        {
            eprintln!("Erreur serveur WebTransport TROOZN: {error:?}");
        }
    });

    let http_registry = registry.clone();
    let http_cache = head_cache.clone();
    let http_bind_for_task = http_bind.clone();
    tokio::spawn(async move {
        if let Err(error) =
            run_http_media_gateway(&http_bind_for_task, http_registry, http_cache).await
        {
            eprintln!("Erreur serveur HTTP media TROOZN: {error:?}");
        }
    });

    let upnp_registry = registry.clone();
    let upnp_bind_for_task = upnp_bind.clone();
    let http_bind_for_upnp = http_bind.clone();
    tokio::spawn(async move {
        if let Err(error) =
            run_upnp_server(&upnp_bind_for_task, &http_bind_for_upnp, upnp_registry).await
        {
            eprintln!("Erreur serveur UPnP TROOZN: {error:?}");
        }
    });

    println!("=============================================================");
    println!(" TROOZN RADXA PROXY");
    println!(" Kodi lit legacy: ftp://{ftp_bind}/");
    println!(" Kodi lit media: http://{http_bind}/media/<telephone>/<chemin>");
    println!(" Kodi decouvre UPnP/DLNA: http://{upnp_bind}/upnp/root/device.xml");
    println!(" Telephones: WebTransport sur https://{webtransport_bind}/");
    println!(
        " Certificat WebTransport: {}",
        webtransport_cert_dir.display()
    );
    println!(
        " Cache tete video: {} Mo/fichier, max {} Mo, {}",
        head_cache_file_bytes / 1024 / 1024,
        head_cache_max_bytes / 1024 / 1024,
        head_cache_dir.display()
    );
    println!(" Status proxy: {}", proxy_status_path.display());
    println!(
        " Kodi JSON-RPC local: {}:{} auth={}",
        kodi.host,
        kodi.port,
        if kodi.username.is_some() { "oui" } else { "non" }
    );
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
    cert_dir: &Path,
    status_path: &Path,
    registry: Registry,
    kodi: KodiConfig,
) -> anyhow::Result<()> {
    let identity = load_or_create_webtransport_identity(cert_dir).await?;
    let cert_hash = identity
        .certificate_chain()
        .as_slice()
        .first()
        .ok_or_else(|| anyhow!("certificat WebTransport manquant"))?
        .hash();
    let cert_hash_text = cert_hash.fmt(Sha256DigestFmt::DottedHex);

    println!("Empreinte WebTransport TROOZN:");
    println!("{cert_hash_text}");
    write_proxy_status(status_path, bind, &cert_hash_text).await?;

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

async fn run_http_media_gateway(
    bind: &str,
    registry: Registry,
    cache: Arc<HeadCache>,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&cache.dir)
        .await
        .with_context(|| format!("creation cache media {}", cache.dir.display()))?;
    let state = HttpGatewayState { registry, cache };
    let app = Router::new()
        .route("/health", get(http_health))
        .route("/media/*path", get(http_get_media).head(http_head_media))
        .with_state(state);
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("ecoute HTTP media {bind}"))?;
    println!("Serveur HTTP media TROOZN: http://{bind}/");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn http_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn run_upnp_server(bind: &str, http_bind: &str, registry: Registry) -> anyhow::Result<()> {
    let upnp_port = bind_port(bind, 8788);
    let upnp_host = std::env::var("TROOZN_UPNP_PUBLIC_HOST")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(guess_local_ipv4);
    let state = UpnpState {
        registry,
        upnp_base_url: format!("http://{upnp_host}:{upnp_port}"),
        media_base_url: loopback_base_url(http_bind, 8787),
    };

    let ssdp_state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = run_upnp_ssdp(ssdp_state).await {
            eprintln!("Erreur SSDP UPnP TROOZN: {error:?}");
        }
    });

    let app = Router::new()
        .route("/", get(upnp_root_page))
        .route("/upnp/:device/device.xml", get(upnp_device_description))
        .route(
            "/upnp/:device/ContentDirectory.xml",
            get(upnp_content_directory_scpd),
        )
        .route(
            "/upnp/:device/ConnectionManager.xml",
            get(upnp_connection_manager_scpd),
        )
        .route(
            "/upnp/:device/control/ContentDirectory",
            post(upnp_content_directory_control),
        )
        .route(
            "/upnp/:device/control/ConnectionManager",
            post(upnp_connection_manager_control),
        )
        .with_state(state.clone());

    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("ecoute UPnP {bind}"))?;
    println!("Serveur UPnP TROOZN: {}/upnp/root/device.xml", state.upnp_base_url);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn upnp_root_page() -> impl IntoResponse {
    (StatusCode::OK, "TROOZN UPnP/DLNA")
}

async fn upnp_device_description(
    State(state): State<UpnpState>,
    AxumPath(device_key): AxumPath<String>,
) -> Response {
    let Some(device) = upnp_device_for_key(&state, &device_key).await else {
        return simple_response(StatusCode::NOT_FOUND, "device UPnP introuvable");
    };
    xml_response(upnp_device_description_xml(&state, &device))
}

async fn upnp_content_directory_scpd() -> Response {
    xml_response(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <scpd xmlns=\"urn:schemas-upnp-org:service-1-0\">\
        <specVersion><major>1</major><minor>0</minor></specVersion>\
        <actionList>\
        <action><name>GetSearchCapabilities</name><argumentList><argument><name>SearchCaps</name><direction>out</direction><relatedStateVariable>SearchCapabilities</relatedStateVariable></argument></argumentList></action>\
        <action><name>GetSortCapabilities</name><argumentList><argument><name>SortCaps</name><direction>out</direction><relatedStateVariable>SortCapabilities</relatedStateVariable></argument></argumentList></action>\
        <action><name>GetSystemUpdateID</name><argumentList><argument><name>Id</name><direction>out</direction><relatedStateVariable>SystemUpdateID</relatedStateVariable></argument></argumentList></action>\
        <action><name>Browse</name><argumentList>\
        <argument><name>ObjectID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ObjectID</relatedStateVariable></argument>\
        <argument><name>BrowseFlag</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_BrowseFlag</relatedStateVariable></argument>\
        <argument><name>Filter</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Filter</relatedStateVariable></argument>\
        <argument><name>StartingIndex</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Index</relatedStateVariable></argument>\
        <argument><name>RequestedCount</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>\
        <argument><name>SortCriteria</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_SortCriteria</relatedStateVariable></argument>\
        <argument><name>Result</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Result</relatedStateVariable></argument>\
        <argument><name>NumberReturned</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>\
        <argument><name>TotalMatches</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>\
        <argument><name>UpdateID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_UpdateID</relatedStateVariable></argument>\
        </argumentList></action>\
        <action><name>UpdateObject</name><argumentList>\
        <argument><name>ObjectID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ObjectID</relatedStateVariable></argument>\
        <argument><name>CurrentTagValue</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_TagValueList</relatedStateVariable></argument>\
        <argument><name>NewTagValue</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_TagValueList</relatedStateVariable></argument>\
        </argumentList></action>\
        </actionList>\
        <serviceStateTable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_ObjectID</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_BrowseFlag</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_Filter</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_Index</name><dataType>ui4</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_Count</name><dataType>ui4</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_SortCriteria</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_Result</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_UpdateID</name><dataType>ui4</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>A_ARG_TYPE_TagValueList</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>SearchCapabilities</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>SortCapabilities</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"yes\"><name>SystemUpdateID</name><dataType>ui4</dataType></stateVariable>\
        </serviceStateTable>\
        </scpd>"
            .to_string(),
    )
}

async fn upnp_connection_manager_scpd() -> Response {
    xml_response(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <scpd xmlns=\"urn:schemas-upnp-org:service-1-0\">\
        <specVersion><major>1</major><minor>0</minor></specVersion>\
        <actionList>\
        <action><name>GetProtocolInfo</name><argumentList>\
        <argument><name>Source</name><direction>out</direction><relatedStateVariable>SourceProtocolInfo</relatedStateVariable></argument>\
        <argument><name>Sink</name><direction>out</direction><relatedStateVariable>SinkProtocolInfo</relatedStateVariable></argument>\
        </argumentList></action>\
        <action><name>GetCurrentConnectionIDs</name><argumentList><argument><name>ConnectionIDs</name><direction>out</direction><relatedStateVariable>CurrentConnectionIDs</relatedStateVariable></argument></argumentList></action>\
        </actionList>\
        <serviceStateTable>\
        <stateVariable sendEvents=\"no\"><name>SourceProtocolInfo</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>SinkProtocolInfo</name><dataType>string</dataType></stateVariable>\
        <stateVariable sendEvents=\"no\"><name>CurrentConnectionIDs</name><dataType>string</dataType></stateVariable>\
        </serviceStateTable>\
        </scpd>"
            .to_string(),
    )
}

async fn upnp_content_directory_control(
    State(state): State<UpnpState>,
    AxumPath(device_key): AxumPath<String>,
    body: String,
) -> Response {
    let Some(device) = upnp_device_for_key(&state, &device_key).await else {
        return simple_response(StatusCode::NOT_FOUND, "device UPnP introuvable");
    };

    if body.contains("GetSearchCapabilities") {
        return soap_response(
            "ContentDirectory",
            "GetSearchCapabilitiesResponse",
            "<SearchCaps></SearchCaps>",
        );
    }
    if body.contains("GetSortCapabilities") {
        return soap_response(
            "ContentDirectory",
            "GetSortCapabilitiesResponse",
            "<SortCaps>dc:title</SortCaps>",
        );
    }
    if body.contains("GetSystemUpdateID") {
        return soap_response(
            "ContentDirectory",
            "GetSystemUpdateIDResponse",
            "<Id>1</Id>",
        );
    }
    if body.contains("UpdateObject") {
        return soap_response("ContentDirectory", "UpdateObjectResponse", "");
    }

    let object_id = normalize_upnp_object_id(&soap_tag(&body, "ObjectID").unwrap_or_else(|| "0".into()));
    let browse_flag =
        soap_tag(&body, "BrowseFlag").unwrap_or_else(|| "BrowseDirectChildren".into());
    let starting_index = soap_tag(&body, "StartingIndex")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let requested_count = soap_tag(&body, "RequestedCount")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);

    let items = match upnp_browse(&state, &device, &object_id, &browse_flag).await {
        Ok(items) => items,
        Err(error) => {
            eprintln!(
                "UPnP Browse failed device={} object={}: {error:?}",
                device.key, object_id
            );
            Vec::new()
        }
    };
    let total = items.len();
    let selected = slice_upnp_items(items, starting_index, requested_count);
    let didl = upnp_didl(&selected);
    soap_response(
        "ContentDirectory",
        "BrowseResponse",
        &format!(
            "<Result>{}</Result><NumberReturned>{}</NumberReturned><TotalMatches>{}</TotalMatches><UpdateID>1</UpdateID>",
            xml_text(&didl),
            selected.len(),
            total
        ),
    )
}

async fn upnp_connection_manager_control(body: String) -> Response {
    if body.contains("GetProtocolInfo") {
        return soap_response(
            "ConnectionManager",
            "GetProtocolInfoResponse",
            &format!(
                "<Source>{}</Source><Sink></Sink>",
                xml_text(&upnp_source_protocol_info())
            ),
        );
    }
    if body.contains("GetCurrentConnectionIDs") {
        return soap_response(
            "ConnectionManager",
            "GetCurrentConnectionIDsResponse",
            "<ConnectionIDs>0</ConnectionIDs>",
        );
    }
    soap_response(
        "ConnectionManager",
        "GetCurrentConnectionInfoResponse",
        "<RcsID>-1</RcsID><AVTransportID>-1</AVTransportID><ProtocolInfo></ProtocolInfo><PeerConnectionManager></PeerConnectionManager><PeerConnectionID>-1</PeerConnectionID><Direction>Output</Direction><Status>OK</Status>",
    )
}

async fn http_head_media(
    State(state): State<HttpGatewayState>,
    AxumPath(path): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    match media_stat_for_http(&state, &path).await {
        Ok((_phone, _target_path, size)) => {
            let Ok((start, end, partial)) = parse_http_range(&headers, size) else {
                return simple_response(StatusCode::RANGE_NOT_SATISFIABLE, "range invalide");
            };
            media_headers_response(size, start, end, partial, Body::empty())
        }
        Err(error) => simple_response(StatusCode::NOT_FOUND, &error.to_string()),
    }
}

async fn http_get_media(
    State(state): State<HttpGatewayState>,
    AxumPath(path): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    let (phone, target_path, size) = match media_stat_for_http(&state, &path).await {
        Ok(value) => value,
        Err(error) => return simple_response(StatusCode::NOT_FOUND, &error.to_string()),
    };
    let (start, end, partial) = match parse_http_range(&headers, size) {
        Ok(value) => value,
        Err(status) => return simple_response(status, "range invalide"),
    };

    if size == 0 {
        return media_headers_response(size, 0, 0, partial, Body::empty());
    }

    let length = end.saturating_sub(start).saturating_add(1);
    if start < state.cache.file_bytes && (partial || length <= state.cache.file_bytes) {
        let cache_path = cache_path_for(&state.cache, &phone.folder_name, &target_path);
        if let Ok(cache_len) = file_len(&cache_path).await {
            if end < cache_len {
                if let Ok(body) = cached_file_body(&cache_path, start, length).await {
                    println!(
                        "http.media cache hit {}{} bytes={}-{}",
                        phone.folder_name, target_path, start, end
                    );
                    refresh_cache_recency(&cache_path).await;
                    schedule_head_cache_prefetch(
                        state.cache.clone(),
                        phone.clone(),
                        target_path.clone(),
                        size,
                    )
                    .await;
                    return media_headers_response(size, start, end, partial, body);
                }
            }
        }
        schedule_head_cache_prefetch(state.cache.clone(), phone.clone(), target_path.clone(), size)
            .await;
    }

    match phone_media_body(phone.clone(), target_path.clone(), start, length).await {
        Ok(body) => {
            println!(
                "http.media stream {}{} bytes={}-{}",
                phone.folder_name, target_path, start, end
            );
            media_headers_response(size, start, end, partial, body)
        }
        Err(error) => simple_response(StatusCode::BAD_GATEWAY, &error.to_string()),
    }
}

async fn media_stat_for_http(
    state: &HttpGatewayState,
    raw_path: &str,
) -> anyhow::Result<(Arc<PhoneSession>, String, u64)> {
    let (phone_folder, target_path) = resolve_http_media_path(raw_path)?;
    let phone = state
        .registry
        .get_by_folder(&phone_folder)
        .await
        .ok_or_else(|| anyhow!("telephone WebTransport indisponible: {phone_folder}"))?;
    let stat = phone_json_request(
        &phone,
        &PhoneRequest::MediaStat {
            protocol_version: TROOZN_PROTOCOL_VERSION,
            path: &target_path,
        },
        MEDIA_STAT_TIMEOUT,
    )
    .await?;
    if stat.is_directory.unwrap_or(false) {
        return Err(anyhow!("le chemin est un dossier"));
    }
    Ok((phone, target_path, stat.size.unwrap_or(0)))
}

fn resolve_http_media_path(raw_path: &str) -> anyhow::Result<(String, String)> {
    let decoded_path = percent_decode_path(raw_path);
    let normalized = decoded_path.trim_matches('/');
    let mut parts = normalized.split('/');
    let phone_folder = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("telephone manquant dans l'URL media"))?
        .to_string();
    let target = parts.collect::<Vec<_>>().join("/");
    if target.is_empty() {
        return Err(anyhow!("fichier manquant dans l'URL media"));
    }
    Ok((phone_folder, format!("/{target}")))
}

fn percent_decode_path(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
            {
                decoded.push((high << 4) | low);
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(decoded).unwrap_or_else(|_| value.to_string())
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn parse_http_range(headers: &HeaderMap, size: u64) -> Result<(u64, u64, bool), StatusCode> {
    if size == 0 {
        return Ok((0, 0, false));
    }
    let Some(range) = headers.get(header::RANGE) else {
        return Ok((0, size - 1, false));
    };
    let range = range.to_str().map_err(|_| StatusCode::RANGE_NOT_SATISFIABLE)?;
    let Some(spec) = range.strip_prefix("bytes=") else {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    };
    let first = spec.split(',').next().unwrap_or_default().trim();
    let Some((start_text, end_text)) = first.split_once('-') else {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    };
    if start_text.is_empty() {
        let suffix = end_text
            .parse::<u64>()
            .map_err(|_| StatusCode::RANGE_NOT_SATISFIABLE)?;
        if suffix == 0 {
            return Err(StatusCode::RANGE_NOT_SATISFIABLE);
        }
        let start = size.saturating_sub(suffix);
        return Ok((start, size - 1, true));
    }

    let start = start_text
        .parse::<u64>()
        .map_err(|_| StatusCode::RANGE_NOT_SATISFIABLE)?;
    if start >= size {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    }
    let end = if end_text.trim().is_empty() {
        size - 1
    } else {
        end_text
            .parse::<u64>()
            .map_err(|_| StatusCode::RANGE_NOT_SATISFIABLE)?
            .min(size - 1)
    };
    if end < start {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    }
    Ok((start, end, true))
}

fn media_headers_response(size: u64, start: u64, end: u64, partial: bool, body: Body) -> Response {
    let status = if partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let length = if size == 0 {
        0
    } else {
        end.saturating_sub(start).saturating_add(1)
    };
    let mut response = Response::builder()
        .status(status)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, length.to_string());
    if partial && size > 0 {
        response = response.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{size}"),
        );
    }
    response.body(body).unwrap_or_else(|_| {
        simple_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "reponse HTTP media invalide",
        )
    })
}

fn simple_response(status: StatusCode, message: &str) -> Response {
    (status, message.to_string()).into_response()
}

fn xml_response(xml: String) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml; charset=utf-8")
        .header("EXT", "")
        .header("SERVER", upnp_server_header())
        .body(Body::from(xml))
        .unwrap_or_else(|_| simple_response(StatusCode::INTERNAL_SERVER_ERROR, "XML invalide"))
}

fn soap_response(service: &str, action: &str, inner_xml: &str) -> Response {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
        s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\
        <s:Body><u:{action} xmlns:u=\"urn:schemas-upnp-org:service:{service}:1\">\
        {inner_xml}\
        </u:{action}></s:Body></s:Envelope>"
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/xml; charset=utf-8")
        .header("EXT", "")
        .header("SERVER", upnp_server_header())
        .body(Body::from(xml))
        .unwrap_or_else(|_| simple_response(StatusCode::INTERNAL_SERVER_ERROR, "SOAP invalide"))
}

fn upnp_device_description_xml(state: &UpnpState, device: &UpnpDeviceInfo) -> String {
    let key = percent_encode_component(&device.key);
    let base = &state.upnp_base_url;
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <root xmlns=\"urn:schemas-upnp-org:device-1-0\">\
        <specVersion><major>1</major><minor>0</minor></specVersion>\
        <URLBase>{base}/upnp/{key}/</URLBase>\
        <device>\
        <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>\
        <friendlyName>{}</friendlyName>\
        <manufacturer>TROOZN</manufacturer>\
        <modelDescription>TROOZN Radxa WebTransport UPnP/DLNA bridge</modelDescription>\
        <modelName>TROOZN Radxa Media Bridge</modelName>\
        <modelNumber>1</modelNumber>\
        <serialNumber>{}</serialNumber>\
        <UDN>uuid:{}</UDN>\
        <presentationURL>{base}/</presentationURL>\
        <serviceList>\
        <service>\
        <serviceType>urn:schemas-upnp-org:service:ContentDirectory:1</serviceType>\
        <serviceId>urn:upnp-org:serviceId:ContentDirectory</serviceId>\
        <SCPDURL>/upnp/{key}/ContentDirectory.xml</SCPDURL>\
        <controlURL>/upnp/{key}/control/ContentDirectory</controlURL>\
        <eventSubURL>/upnp/{key}/event/ContentDirectory</eventSubURL>\
        </service>\
        <service>\
        <serviceType>urn:schemas-upnp-org:service:ConnectionManager:1</serviceType>\
        <serviceId>urn:upnp-org:serviceId:ConnectionManager</serviceId>\
        <SCPDURL>/upnp/{key}/ConnectionManager.xml</SCPDURL>\
        <controlURL>/upnp/{key}/control/ConnectionManager</controlURL>\
        <eventSubURL>/upnp/{key}/event/ConnectionManager</eventSubURL>\
        </service>\
        </serviceList>\
        </device>\
        </root>",
        xml_text(&device.friendly_name),
        xml_text(&device.key),
        xml_text(&device.uuid),
    )
}

async fn upnp_browse(
    state: &UpnpState,
    device: &UpnpDeviceInfo,
    object_id: &str,
    browse_flag: &str,
) -> anyhow::Result<Vec<UpnpDidlItem>> {
    if browse_flag == "BrowseMetadata" {
        return Ok(upnp_metadata(state, device, object_id).await?.into_iter().collect());
    }

    if object_id == "0" {
        if let Some(phone_id) = &device.phone_id {
            let phone = state
                .registry
                .get_by_id(phone_id)
                .await
                .ok_or_else(|| anyhow!("telephone UPnP indisponible: {phone_id}"))?;
            return upnp_list_phone_directory(state, &phone, "/", "0").await;
        }

        let phones = state.registry.list().await;
        return Ok(phones
            .into_iter()
            .map(|phone| UpnpDidlItem {
                id: phone_container_id(&phone),
                parent_id: "0".into(),
                title: phone.display_name.clone(),
                upnp_class: "object.container.storageFolder".into(),
                is_container: true,
                child_count: 0,
                protocol_info: None,
                url: None,
                size: None,
            })
            .collect());
    }

    if let Some(phone_id) = parse_phone_container_id(object_id) {
        let phone = state
            .registry
            .get_by_id(&phone_id)
            .await
            .ok_or_else(|| anyhow!("telephone UPnP indisponible: {phone_id}"))?;
        return upnp_list_phone_directory(state, &phone, "/", object_id).await;
    }

    if let Some((phone_id, path)) = parse_path_object_id(object_id, "folder") {
        let phone = state
            .registry
            .get_by_id(&phone_id)
            .await
            .ok_or_else(|| anyhow!("telephone UPnP indisponible: {phone_id}"))?;
        return upnp_list_phone_directory(state, &phone, &path, object_id).await;
    }

    Ok(Vec::new())
}

async fn upnp_metadata(
    state: &UpnpState,
    device: &UpnpDeviceInfo,
    object_id: &str,
) -> anyhow::Result<Option<UpnpDidlItem>> {
    if object_id == "0" {
        return Ok(Some(UpnpDidlItem {
            id: "0".into(),
            parent_id: "-1".into(),
            title: device.friendly_name.clone(),
            upnp_class: "object.container.storageFolder".into(),
            is_container: true,
            child_count: 0,
            protocol_info: None,
            url: None,
            size: None,
        }));
    }

    if let Some(phone_id) = parse_phone_container_id(object_id) {
        let Some(phone) = state.registry.get_by_id(&phone_id).await else {
            return Ok(None);
        };
        return Ok(Some(UpnpDidlItem {
            id: object_id.into(),
            parent_id: "0".into(),
            title: phone.display_name.clone(),
            upnp_class: "object.container.storageFolder".into(),
            is_container: true,
            child_count: 0,
            protocol_info: None,
            url: None,
            size: None,
        }));
    }

    if let Some((phone_id, path)) = parse_path_object_id(object_id, "folder") {
        let Some(phone) = state.registry.get_by_id(&phone_id).await else {
            return Ok(None);
        };
        return Ok(Some(UpnpDidlItem {
            id: object_id.into(),
            parent_id: parent_id_for_phone_path(device, &phone, &parent_media_path(&path)),
            title: media_basename(&path).unwrap_or_else(|| phone.display_name.clone()),
            upnp_class: "object.container.storageFolder".into(),
            is_container: true,
            child_count: 0,
            protocol_info: None,
            url: None,
            size: None,
        }));
    }

    if let Some((phone_id, path)) = parse_path_object_id(object_id, "item") {
        let Some(phone) = state.registry.get_by_id(&phone_id).await else {
            return Ok(None);
        };
        let stat = phone_json_request(
            &phone,
            &PhoneRequest::MediaStat {
                protocol_version: TROOZN_PROTOCOL_VERSION,
                path: &path,
            },
            MEDIA_STAT_TIMEOUT,
        )
        .await?;
        return Ok(Some(upnp_file_item(
            state,
            device,
            &phone,
            &path,
            stat.size.unwrap_or(0),
        )));
    }

    Ok(None)
}

async fn upnp_list_phone_directory(
    state: &UpnpState,
    phone: &Arc<PhoneSession>,
    path: &str,
    parent_id: &str,
) -> anyhow::Result<Vec<UpnpDidlItem>> {
    let response = phone_json_request(
        phone,
        &PhoneRequest::MediaList {
            protocol_version: TROOZN_PROTOCOL_VERSION,
            path,
        },
        MEDIA_LIST_TIMEOUT,
    )
    .await?;

    let mut entries = response
        .entries
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| entry.name != "." && entry.name != "..")
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| {
        b.is_directory
            .cmp(&a.is_directory)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });

    Ok(entries
        .into_iter()
        .map(|entry| {
            let child_path = join_media_path(path, &entry.name);
            if entry.is_directory {
                UpnpDidlItem {
                    id: folder_object_id(phone, &child_path),
                    parent_id: parent_id.into(),
                    title: entry.name,
                    upnp_class: "object.container.storageFolder".into(),
                    is_container: true,
                    child_count: 0,
                    protocol_info: None,
                    url: None,
                    size: None,
                }
            } else {
                upnp_file_item_for_parent(state, phone, &child_path, entry.size, parent_id)
            }
        })
        .collect())
}

fn upnp_file_item(
    state: &UpnpState,
    device: &UpnpDeviceInfo,
    phone: &Arc<PhoneSession>,
    path: &str,
    size: u64,
) -> UpnpDidlItem {
    let parent_id = parent_id_for_phone_path(device, phone, &parent_media_path(path));
    upnp_file_item_for_parent(state, phone, path, size, &parent_id)
}

fn upnp_file_item_for_parent(
    state: &UpnpState,
    phone: &Arc<PhoneSession>,
    path: &str,
    size: u64,
    parent_id: &str,
) -> UpnpDidlItem {
    let mime = mime_type_for_path(path);
    UpnpDidlItem {
        id: item_object_id(phone, path),
        parent_id: parent_id.into(),
        title: media_basename(path).unwrap_or_else(|| "media".into()),
        upnp_class: upnp_class_for_mime(mime).into(),
        is_container: false,
        child_count: 0,
        protocol_info: Some(protocol_info_for_mime(mime)),
        url: Some(upnp_media_url(state, phone, path)),
        size: Some(size),
    }
}

fn upnp_didl(items: &[UpnpDidlItem]) -> String {
    let mut buffer = String::from(
        "<DIDL-Lite xmlns=\"urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/\" \
        xmlns:dc=\"http://purl.org/dc/elements/1.1/\" \
        xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\">",
    );
    for item in items {
        if item.is_container {
            buffer.push_str(&format!(
                "<container id=\"{}\" parentID=\"{}\" restricted=\"1\" searchable=\"0\" childCount=\"{}\">\
                <dc:title>{}</dc:title><upnp:class>{}</upnp:class></container>",
                xml_text(&item.id),
                xml_text(&item.parent_id),
                item.child_count,
                xml_text(&item.title),
                xml_text(&item.upnp_class),
            ));
        } else {
            let protocol_info = item
                .protocol_info
                .as_deref()
                .unwrap_or("http-get:*:application/octet-stream:*");
            let size_attr = item
                .size
                .filter(|size| *size > 0)
                .map(|size| format!(" size=\"{size}\""))
                .unwrap_or_default();
            buffer.push_str(&format!(
                "<item id=\"{}\" parentID=\"{}\" restricted=\"1\">\
                <dc:title>{}</dc:title><upnp:class>{}</upnp:class>\
                <res protocolInfo=\"{}\"{}>{}</res></item>",
                xml_text(&item.id),
                xml_text(&item.parent_id),
                xml_text(&item.title),
                xml_text(&item.upnp_class),
                xml_text(protocol_info),
                size_attr,
                xml_text(item.url.as_deref().unwrap_or_default()),
            ));
        }
    }
    buffer.push_str("</DIDL-Lite>");
    buffer
}

fn slice_upnp_items(
    items: Vec<UpnpDidlItem>,
    starting_index: usize,
    requested_count: usize,
) -> Vec<UpnpDidlItem> {
    if starting_index >= items.len() {
        return Vec::new();
    }
    if requested_count == 0 {
        return items[starting_index..].to_vec();
    }
    let end = items.len().min(starting_index + requested_count);
    items[starting_index..end].to_vec()
}

async fn cached_file_body(cache_path: &Path, start: u64, length: u64) -> anyhow::Result<Body> {
    let mut file = File::open(cache_path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let reader = file.take(length);
    Ok(Body::from_stream(ReaderStream::new(reader)))
}

async fn refresh_cache_recency(cache_path: &Path) {
    let Ok(metadata) = tokio::fs::metadata(cache_path).await else {
        return;
    };
    if let Ok(file) = tokio::fs::OpenOptions::new().write(true).open(cache_path).await {
        let _ = file.set_len(metadata.len()).await;
    }
}

async fn phone_media_body(
    phone: Arc<PhoneSession>,
    target_path: String,
    start: u64,
    length: u64,
) -> anyhow::Result<Body> {
    let reader = phone_media_reader(phone, target_path, start, length).await?;
    Ok(Body::from_stream(ReaderStream::new(reader)))
}

async fn phone_media_reader(
    phone: Arc<PhoneSession>,
    target_path: String,
    start: u64,
    length: u64,
) -> anyhow::Result<tokio::io::DuplexStream> {
    let (reader, mut writer) = tokio::io::duplex(MEDIA_STREAM_BUFFER_BYTES);
    tokio::spawn(async move {
        let result = copy_phone_media_range(&phone, &target_path, start, length, &mut writer).await;
        if let Err(error) = result {
            eprintln!("http media.get failed: {error:?}");
        }
    });
    Ok(reader)
}

async fn copy_phone_media_range<W>(
    phone: &PhoneSession,
    target_path: &str,
    start: u64,
    max_bytes: u64,
    writer: &mut W,
) -> anyhow::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
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
            protocol_version: TROOZN_PROTOCOL_VERSION,
            path: target_path,
            start_pos: start,
        },
    )
    .await?;

    let mut remaining = max_bytes;
    let mut buffer = vec![0u8; MEDIA_COPY_BUFFER_BYTES];
    while remaining > 0 {
        let Some(bytes_read) = rx.read(&mut buffer).await? else {
            break;
        };
        if bytes_read == 0 {
            break;
        }
        let write_len = (bytes_read as u64).min(remaining) as usize;
        writer.write_all(&buffer[..write_len]).await?;
        remaining -= write_len as u64;
        if write_len < bytes_read {
            break;
        }
    }
    Ok(())
}

async fn ensure_head_cache(
    cache: &Arc<HeadCache>,
    phone: &Arc<PhoneSession>,
    target_path: &str,
    size: u64,
) -> anyhow::Result<PathBuf> {
    if cache.file_bytes == 0 || size == 0 {
        return Err(anyhow!("cache desactive"));
    }
    let key = cache_key(&phone.folder_name, target_path);
    let lock = cache_entry_lock(cache, &key).await;
    let _guard = lock.lock().await;

    tokio::fs::create_dir_all(&cache.dir).await?;
    let cache_path = cache_path_for(cache, &phone.folder_name, target_path);
    let desired_len = cache.file_bytes.min(size);
    let current_len = file_len(&cache_path).await.unwrap_or(0).min(desired_len);
    if current_len < desired_len {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cache_path)
            .await?;
        copy_phone_media_range(
            phone,
            target_path,
            current_len,
            desired_len - current_len,
            &mut file,
        )
        .await?;
        file.flush().await?;
        println!(
            "head-cache fill {}{} -> {} Mo",
            phone.folder_name,
            target_path,
            desired_len / 1024 / 1024
        );
    }
    prune_head_cache(cache).await;
    Ok(cache_path)
}

async fn schedule_head_cache_prefetch(
    cache: Arc<HeadCache>,
    phone: Arc<PhoneSession>,
    target_path: String,
    size: u64,
) {
    if cache.file_bytes == 0 || size == 0 {
        return;
    }
    let key = cache_key(&phone.folder_name, &target_path);
    {
        let mut prefetching = cache.prefetching.write().await;
        if !prefetching.insert(key.clone()) {
            return;
        }
    }

    tokio::spawn(async move {
        let result = ensure_head_cache(&cache, &phone, &target_path, size).await;
        if let Err(error) = result {
            eprintln!("head-cache prefetch failed: {error:?}");
        }
        cache.prefetching.write().await.remove(&key);
    });
}

fn cache_path_for(cache: &HeadCache, phone_folder: &str, target_path: &str) -> PathBuf {
    cache
        .dir
        .join(format!("{}.head", cache_key(phone_folder, target_path)))
}

async fn cache_entry_lock(cache: &Arc<HeadCache>, key: &str) -> Arc<Mutex<()>> {
    if let Some(lock) = cache.locks.read().await.get(key).cloned() {
        return lock;
    }
    let mut locks = cache.locks.write().await;
    locks
        .entry(key.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

async fn prune_head_cache(cache: &HeadCache) {
    if cache.max_bytes == 0 {
        return;
    }
    let Ok(mut entries) = cache_entries(&cache.dir).await else {
        return;
    };
    let mut total: u64 = entries.iter().map(|entry| entry.1).sum();
    if total <= cache.max_bytes {
        return;
    }
    entries.sort_by_key(|entry| entry.2);
    for (path, len, _) in entries {
        if total <= cache.max_bytes {
            break;
        }
        if tokio::fs::remove_file(&path).await.is_ok() {
            total = total.saturating_sub(len);
        }
    }
}

async fn cache_entries(dir: &Path) -> anyhow::Result<Vec<(PathBuf, u64, SystemTime)>> {
    let mut entries = Vec::new();
    let mut read_dir = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("head") {
            continue;
        }
        let metadata = entry.metadata().await?;
        entries.push((
            path,
            metadata.len(),
            metadata.modified().unwrap_or(UNIX_EPOCH),
        ));
    }
    Ok(entries)
}

async fn file_len(path: &Path) -> anyhow::Result<u64> {
    Ok(tokio::fs::metadata(path).await?.len())
}

fn cache_key(phone_folder: &str, target_path: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(phone_folder.as_bytes());
    hasher.write_u8(0);
    hasher.write(target_path.as_bytes());
    format!("{:016x}", hasher.finish())
}

async fn run_upnp_ssdp(state: UpnpState) -> anyhow::Result<()> {
    let socket = Arc::new(create_ssdp_socket()?);
    send_full_upnp_advertisement(&state, &socket).await;

    let advertise_state = state.clone();
    let advertise_socket = socket.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(UPNP_ADVERTISE_INTERVAL);
        loop {
            interval.tick().await;
            send_full_upnp_advertisement(&advertise_state, &advertise_socket).await;
        }
    });

    let mut buffer = vec![0u8; 4096];
    loop {
        let (len, sender) = socket.recv_from(&mut buffer).await?;
        let packet = String::from_utf8_lossy(&buffer[..len]).to_ascii_uppercase();
        if packet.contains("M-SEARCH") && packet.contains("SSDP:DISCOVER") {
            send_upnp_search_responses(&state, &socket, sender).await;
        }
    }
}

fn create_ssdp_socket() -> anyhow::Result<UdpSocket> {
    let std_socket = match StdUdpSocket::bind(SocketAddrV4::new(
        Ipv4Addr::UNSPECIFIED,
        UPNP_MULTICAST_PORT,
    )) {
        Ok(socket) => socket,
        Err(error) => {
            eprintln!(
                "SSDP port {} indisponible ({error}); annonces NOTIFY seulement.",
                UPNP_MULTICAST_PORT
            );
            StdUdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))?
        }
    };
    std_socket.set_nonblocking(true)?;
    let _ = std_socket.set_multicast_loop_v4(true);
    let _ = std_socket.set_multicast_ttl_v4(4);
    let _ = std_socket.join_multicast_v4(&UPNP_MULTICAST_ADDR, &Ipv4Addr::UNSPECIFIED);
    Ok(UdpSocket::from_std(std_socket)?)
}

async fn send_full_upnp_advertisement(state: &UpnpState, socket: &UdpSocket) {
    for device in upnp_devices(state).await {
        for target in upnp_targets(&device) {
            let packet = ssdp_packet(&[
                "NOTIFY * HTTP/1.1".into(),
                format!("HOST: {UPNP_MULTICAST_ADDR}:{UPNP_MULTICAST_PORT}"),
                format!("CACHE-CONTROL: max-age={}", UPNP_ADVERTISE_INTERVAL.as_secs() * 4),
                format!("LOCATION: {}", upnp_device_location(state, &device)),
                format!("NT: {}", target.0),
                "NTS: ssdp:alive".into(),
                format!("SERVER: {}", upnp_server_header()),
                format!("USN: {}", target.1),
                format!("BOOTID.UPNP.ORG: {UPNP_BOOT_ID}"),
                format!("CONFIGID.UPNP.ORG: {UPNP_CONFIG_ID}"),
            ]);
            let _ = socket
                .send_to(
                    packet.as_bytes(),
                    SocketAddrV4::new(UPNP_MULTICAST_ADDR, UPNP_MULTICAST_PORT),
                )
                .await;
        }
    }
}

async fn send_upnp_search_responses(state: &UpnpState, socket: &UdpSocket, sender: SocketAddr) {
    for device in upnp_devices(state).await {
        for target in upnp_targets(&device) {
            let packet = ssdp_packet(&[
                "HTTP/1.1 200 OK".into(),
                format!("CACHE-CONTROL: max-age={}", UPNP_ADVERTISE_INTERVAL.as_secs() * 4),
                "EXT:".into(),
                format!("LOCATION: {}", upnp_device_location(state, &device)),
                format!("SERVER: {}", upnp_server_header()),
                format!("ST: {}", target.0),
                format!("USN: {}", target.1),
                format!("BOOTID.UPNP.ORG: {UPNP_BOOT_ID}"),
                format!("CONFIGID.UPNP.ORG: {UPNP_CONFIG_ID}"),
            ]);
            let _ = socket.send_to(packet.as_bytes(), sender).await;
        }
    }
}

fn ssdp_packet(lines: &[String]) -> String {
    format!("{}\r\n\r\n", lines.join("\r\n"))
}

async fn upnp_devices(state: &UpnpState) -> Vec<UpnpDeviceInfo> {
    let mut devices = vec![UpnpDeviceInfo {
        key: "root".into(),
        uuid: UPNP_ROOT_UUID.into(),
        friendly_name: "TROOZN".into(),
        phone_id: None,
    }];
    devices.extend(
        state
            .registry
            .list()
            .await
            .into_iter()
            .map(|phone| UpnpDeviceInfo {
                key: phone.id.clone(),
                uuid: deterministic_uuid(&format!("troozn-phone:{}", phone.id)),
                friendly_name: format!("TROOZN - {}", phone.display_name),
                phone_id: Some(phone.id.clone()),
            }),
    );
    devices
}

async fn upnp_device_for_key(state: &UpnpState, key: &str) -> Option<UpnpDeviceInfo> {
    if key == "root" {
        return Some(UpnpDeviceInfo {
            key: "root".into(),
            uuid: UPNP_ROOT_UUID.into(),
            friendly_name: "TROOZN".into(),
            phone_id: None,
        });
    }
    state.registry.get_by_id(key).await.map(|phone| UpnpDeviceInfo {
        key: phone.id.clone(),
        uuid: deterministic_uuid(&format!("troozn-phone:{}", phone.id)),
        friendly_name: format!("TROOZN - {}", phone.display_name),
        phone_id: Some(phone.id.clone()),
    })
}

fn upnp_targets(device: &UpnpDeviceInfo) -> Vec<(String, String)> {
    let uuid = format!("uuid:{}", device.uuid);
    vec![
        ("upnp:rootdevice".into(), format!("{uuid}::upnp:rootdevice")),
        (uuid.clone(), uuid.clone()),
        (
            "urn:schemas-upnp-org:device:MediaServer:1".into(),
            format!("{uuid}::urn:schemas-upnp-org:device:MediaServer:1"),
        ),
        (
            "urn:schemas-upnp-org:service:ContentDirectory:1".into(),
            format!("{uuid}::urn:schemas-upnp-org:service:ContentDirectory:1"),
        ),
        (
            "urn:schemas-upnp-org:service:ConnectionManager:1".into(),
            format!("{uuid}::urn:schemas-upnp-org:service:ConnectionManager:1"),
        ),
    ]
}

fn upnp_device_location(state: &UpnpState, device: &UpnpDeviceInfo) -> String {
    format!(
        "{}/upnp/{}/device.xml",
        state.upnp_base_url,
        percent_encode_component(&device.key)
    )
}

fn upnp_server_header() -> &'static str {
    "Linux UPnP/1.1 TROOZN-Radxa/1.0"
}

fn phone_container_id(phone: &PhoneSession) -> String {
    format!("phone:{}", percent_encode_component(&phone.id))
}

fn folder_object_id(phone: &PhoneSession, path: &str) -> String {
    format!(
        "folder:{}:{}",
        percent_encode_component(&phone.id),
        percent_encode_component(&normalize_media_path(path))
    )
}

fn item_object_id(phone: &PhoneSession, path: &str) -> String {
    format!(
        "item:{}:{}",
        percent_encode_component(&phone.id),
        percent_encode_component(&normalize_media_path(path))
    )
}

fn parse_phone_container_id(object_id: &str) -> Option<String> {
    object_id
        .strip_prefix("phone:")
        .map(percent_decode_path)
        .filter(|value| !value.is_empty())
}

fn parse_path_object_id(object_id: &str, prefix: &str) -> Option<(String, String)> {
    let raw = object_id.strip_prefix(&format!("{prefix}:"))?;
    let (phone_id, path) = raw.split_once(':')?;
    let phone_id = percent_decode_path(phone_id);
    let path = normalize_media_path(&percent_decode_path(path));
    Some((phone_id, path))
}

fn parent_id_for_phone_path(
    device: &UpnpDeviceInfo,
    phone: &PhoneSession,
    parent_path: &str,
) -> String {
    if parent_path == "/" {
        if device.phone_id.is_some() {
            "0".into()
        } else {
            phone_container_id(phone)
        }
    } else {
        folder_object_id(phone, parent_path)
    }
}

fn parent_media_path(path: &str) -> String {
    let normalized = normalize_media_path(path);
    if normalized == "/" {
        return "/".into();
    }
    let trimmed = normalized.trim_end_matches('/');
    let Some(index) = trimmed.rfind('/') else {
        return "/".into();
    };
    if index == 0 {
        "/".into()
    } else {
        trimmed[..index].to_string()
    }
}

fn join_media_path(parent: &str, name: &str) -> String {
    let parent = normalize_media_path(parent);
    let clean_name = name.trim_matches('/');
    if parent == "/" {
        format!("/{clean_name}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), clean_name)
    }
}

fn normalize_media_path(path: &str) -> String {
    let normalized = path
        .replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/");
    if normalized.is_empty() {
        "/".into()
    } else {
        format!("/{normalized}")
    }
}

fn media_basename(path: &str) -> Option<String> {
    normalize_media_path(path)
        .trim_matches('/')
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn upnp_media_url(state: &UpnpState, phone: &PhoneSession, path: &str) -> String {
    format!(
        "{}/media/{}/{}",
        state.media_base_url,
        percent_encode_component(&phone.folder_name),
        percent_encode_media_path(path)
    )
}

fn percent_encode_media_path(path: &str) -> String {
    normalize_media_path(path)
        .trim_start_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .map(percent_encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

fn mime_type_for_path(path: &str) -> &'static str {
    let extension = path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "mp3" => "audio/mpeg",
        "m4a" | "aac" => "audio/aac",
        "flac" => "audio/flac",
        "wav" => "audio/wav",
        "ogg" | "oga" => "audio/ogg",
        "mp4" | "m4v" => "video/mp4",
        "mkv" => "video/x-matroska",
        "avi" => "video/x-msvideo",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "bmp" => "image/bmp",
        "heic" => "image/heic",
        "heif" => "image/heif",
        _ => "application/octet-stream",
    }
}

fn upnp_class_for_mime(mime: &str) -> &'static str {
    if mime.starts_with("audio/") {
        "object.item.audioItem.musicTrack"
    } else if mime.starts_with("video/") {
        "object.item.videoItem"
    } else if mime.starts_with("image/") {
        "object.item.imageItem.photo"
    } else {
        "object.item"
    }
}

fn protocol_info_for_mime(mime: &str) -> String {
    format!("http-get:*:{mime}:DLNA.ORG_OP=01;DLNA.ORG_CI=0")
}

fn upnp_source_protocol_info() -> String {
    [
        "audio/mpeg",
        "audio/aac",
        "audio/flac",
        "audio/wav",
        "audio/ogg",
        "video/mp4",
        "video/x-matroska",
        "video/x-msvideo",
        "video/quicktime",
        "video/webm",
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/gif",
        "image/bmp",
        "image/heic",
        "image/heif",
        "application/octet-stream",
    ]
    .into_iter()
    .map(protocol_info_for_mime)
    .collect::<Vec<_>>()
    .join(",")
}

fn soap_tag(xml: &str, tag: &str) -> Option<String> {
    let lower = xml.to_ascii_lowercase();
    let tag = tag.to_ascii_lowercase();
    let mut search = 0usize;

    while let Some(start_rel) = lower[search..].find('<') {
        let start = search + start_rel;
        if lower[start + 1..].starts_with('/') {
            search = start + 1;
            continue;
        }
        let Some(gt_rel) = lower[start..].find('>') else {
            return None;
        };
        let gt = start + gt_rel;
        let name = lower[start + 1..gt]
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_end_matches('/');
        let local = name.rsplit(':').next().unwrap_or(name);
        if local == tag {
            let body_start = gt + 1;
            let mut close_search = body_start;
            while let Some(close_rel) = lower[close_search..].find("</") {
                let close_start = close_search + close_rel;
                let Some(close_gt_rel) = lower[close_start..].find('>') else {
                    return None;
                };
                let close_gt = close_start + close_gt_rel;
                let close_name = lower[close_start + 2..close_gt].trim();
                let close_local = close_name.rsplit(':').next().unwrap_or(close_name);
                if close_local == tag {
                    return Some(xml[body_start..close_start].trim().to_string());
                }
                close_search = close_gt + 1;
            }
        }
        search = gt + 1;
    }

    None
}

fn normalize_upnp_object_id(object_id: &str) -> String {
    let mut normalized = object_id.trim().to_string();
    if normalized.is_empty() {
        return "0".into();
    }
    if normalized.starts_with("upnp://") {
        if let Some(last) = normalized
            .split('/')
            .filter(|part| !part.trim().is_empty())
            .last()
        {
            normalized = last.to_string();
        }
    }
    for _ in 0..2 {
        if !normalized.contains('%') {
            break;
        }
        let decoded = percent_decode_path(&normalized);
        if decoded == normalized {
            break;
        }
        normalized = decoded;
    }
    while normalized.ends_with('/') {
        normalized.pop();
    }
    if normalized.is_empty() {
        "0".into()
    } else {
        normalized
    }
}

fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(*byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn deterministic_uuid(value: &str) -> String {
    let mut first = std::collections::hash_map::DefaultHasher::new();
    first.write(value.as_bytes());
    let a = first.finish();

    let mut second = std::collections::hash_map::DefaultHasher::new();
    second.write(b"troozn-upnp");
    second.write(value.as_bytes());
    let b = second.finish();

    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (a >> 32) as u32,
        ((a >> 16) & 0xffff) as u16,
        (a & 0xffff) as u16,
        (b >> 48) as u16,
        b & 0x0000ffffffffffff,
    )
}

fn bind_port(bind: &str, default_port: u16) -> u16 {
    bind.rsplit(':')
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_port)
}

fn loopback_base_url(bind: &str, default_port: u16) -> String {
    format!("http://127.0.0.1:{}", bind_port(bind, default_port))
}

fn guess_local_ipv4() -> String {
    StdUdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
        .and_then(|socket| {
            socket.connect(SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 80))?;
            socket.local_addr()
        })
        .ok()
        .and_then(|addr| match addr {
            SocketAddr::V4(addr) if !addr.ip().is_loopback() => Some(addr.ip().to_string()),
            _ => None,
        })
        .unwrap_or_else(|| "127.0.0.1".into())
}

fn configured_cache_bytes(name: &str, default: u64, clamp: Option<(u64, u64)>) -> u64 {
    let value = std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(default);
    if value == 0 {
        return 0;
    }
    match clamp {
        Some((min, max)) => value.clamp(min, max),
        None => value,
    }
}

async fn load_or_create_webtransport_identity(cert_dir: &Path) -> anyhow::Result<Identity> {
    let cert_path = cert_dir.join("cert.pem");
    let key_path = cert_dir.join("key.pem");
    let metadata_path = cert_dir.join("identity.json");

    if cert_path.exists() && key_path.exists() && !identity_is_stale(&metadata_path).await {
        return Identity::load_pemfiles(&cert_path, &key_path)
            .await
            .context("chargement identite WebTransport persistante");
    }

    tokio::fs::create_dir_all(cert_dir)
        .await
        .with_context(|| format!("creation du repertoire {}", cert_dir.display()))?;

    let identity = Identity::self_signed(["localhost", "127.0.0.1", "0.0.0.0"])?;
    identity
        .certificate_chain()
        .store_pemfile(&cert_path)
        .await
        .with_context(|| format!("ecriture du certificat {}", cert_path.display()))?;
    identity
        .private_key()
        .store_secret_pemfile(&key_path)
        .await
        .with_context(|| format!("ecriture de la cle {}", key_path.display()))?;
    restrict_private_key_permissions(&key_path).await;

    let created_at = unix_timestamp();
    let metadata = json!({
        "createdAt": created_at,
        "expiresAfter": created_at + (14 * 24 * 60 * 60),
        "rotateAfter": created_at + WEBTRANSPORT_CERT_MAX_AGE_SECONDS,
    });
    tokio::fs::write(&metadata_path, metadata.to_string())
        .await
        .with_context(|| format!("ecriture metadata {}", metadata_path.display()))?;

    Ok(identity)
}

async fn identity_is_stale(metadata_path: &Path) -> bool {
    let Ok(raw) = tokio::fs::read_to_string(metadata_path).await else {
        return true;
    };
    let Ok(value) = serde_json::from_str::<Value>(&raw) else {
        return true;
    };
    let created_at = value
        .get("createdAt")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    created_at == 0
        || unix_timestamp().saturating_sub(created_at) >= WEBTRANSPORT_CERT_MAX_AGE_SECONDS
}

async fn restrict_private_key_permissions(key_path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(metadata) = tokio::fs::metadata(key_path).await {
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o600);
            let _ = tokio::fs::set_permissions(key_path, permissions).await;
        }
    }
}

async fn write_proxy_status(status_path: &Path, bind: &str, cert_hash: &str) -> anyhow::Result<()> {
    if let Some(parent) = status_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let status = json!({
        "state": "running",
        "transport": "webtransport",
        "ftpBind": std::env::var("TROOZN_FTP_BIND").unwrap_or_else(|_| DEFAULT_FTP_BIND.into()),
        "httpBind": std::env::var("TROOZN_HTTP_BIND").unwrap_or_else(|_| DEFAULT_HTTP_BIND.into()),
        "upnpBind": std::env::var("TROOZN_UPNP_BIND").unwrap_or_else(|_| DEFAULT_UPNP_BIND.into()),
        "webTransportBind": bind,
        "webTransportPort": bind.rsplit(':').next().and_then(|value| value.parse::<u16>().ok()).unwrap_or(4433),
        "webTransportCertHash": cert_hash,
        "kodiAuthConfigured": std::env::var("TROOZN_KODI_USERNAME").ok().map(|value| !value.trim().is_empty()).unwrap_or(false),
        "headCacheBytes": configured_cache_bytes("TROOZN_HEAD_CACHE_BYTES", DEFAULT_HEAD_CACHE_BYTES, Some((MIN_HEAD_CACHE_BYTES, MAX_HEAD_CACHE_BYTES))),
        "headCacheMaxBytes": configured_cache_bytes("TROOZN_HEAD_CACHE_MAX_BYTES", DEFAULT_HEAD_CACHE_MAX_BYTES, None),
        "headCacheDir": std::env::var("TROOZN_HEAD_CACHE_DIR").unwrap_or_else(|_| troozn_state_dir().join("head-cache").display().to_string()),
        "updatedAt": unix_timestamp(),
    });
    tokio::fs::write(status_path, format!("{status}\n")).await?;
    Ok(())
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
                        role,
                        source_visible,
                    } => {
                        match registry
                            .register(
                                PhoneRegister {
                                    device_id,
                                    display_name,
                                    role,
                                    source_visible,
                                },
                                connection,
                            )
                            .await
                        {
                            Ok(()) => {
                                tx.write_all(json!({"ok":true}).to_string().as_bytes())
                                    .await?;
                            }
                            Err(error) if error.to_string() == "admin_already_claimed" => {
                                tx.write_all(
                                    json!({
                                        "ok": false,
                                        "code": "admin_already_claimed",
                                        "message": "Un téléphone admin est déjà connecté à cette session Kodi."
                                    })
                                    .to_string()
                                    .as_bytes(),
                                )
                                .await?;
                            }
                            Err(error) => return Err(error),
                        }
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
                    RadxaRequest::ProxyList { path } => {
                        let response = proxy_list(&registry, &path).await?;
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

async fn proxy_list(registry: &Registry, raw_path: &str) -> anyhow::Result<Value> {
    let path = normalize_proxy_path(raw_path);
    if path == "/" {
        let phones = registry.list().await;
        println!("proxy.list / -> {} telephones", phones.len());
        return Ok(json!({
            "ok": true,
            "entries": phones.into_iter().map(|phone| {
                json!({
                    "name": phone.folder_name.clone(),
                    "displayName": phone.display_name.clone(),
                    "isDirectory": true,
                    "size": 0,
                })
            }).collect::<Vec<_>>(),
        }));
    }

    let mut parts = path.trim_matches('/').split('/');
    let phone_folder = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("telephone manquant dans le chemin"))?;
    let target_path = format!("/{}", parts.collect::<Vec<_>>().join("/"));
    let phone = registry
        .get_by_folder(phone_folder)
        .await
        .ok_or_else(|| anyhow!("telephone WebTransport indisponible: {phone_folder}"))?;
    let response = phone_json_request(
        &phone,
        &PhoneRequest::MediaList {
            protocol_version: TROOZN_PROTOCOL_VERSION,
            path: &target_path,
        },
        MEDIA_LIST_TIMEOUT,
    )
    .await?;
    let entries = response.entries.unwrap_or_default();
    println!(
        "proxy.list {} -> {} entrees via {}",
        path,
        entries.len(),
        phone.folder_name
    );
    Ok(json!({
        "ok": true,
        "entries": entries.into_iter().filter(|entry| entry.name != "." && entry.name != "..").map(|entry| {
            json!({
                "name": entry.name,
                "isDirectory": entry.is_directory,
                "size": entry.size,
            })
        }).collect::<Vec<_>>(),
    }))
}

fn normalize_proxy_path(raw_path: &str) -> String {
    let trimmed = raw_path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return "/".into();
    }
    let without_scheme = trimmed
        .strip_prefix("ftp://127.0.0.1:2120")
        .or_else(|| trimmed.strip_prefix("ftp://localhost:2120"))
        .unwrap_or(trimmed);
    let normalized = without_scheme.replace('\\', "/");
    let parts = normalized
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>();
    if parts.is_empty() {
        "/".into()
    } else {
        format!("/{}", parts.join("/"))
    }
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
        "CMD_PLAY_PAUSE" | "CMD_PLAYPAUSE" => match active_player_id(kodi).await? {
            Some(player_id) => {
                kodi_json_rpc(
                    kodi,
                    "Player.PlayPause",
                    json!({"playerid": player_id, "play": "toggle"}),
                )
                .await
            }
            None => {
                kodi_json_rpc(kodi, "Player.Open", json!({"item": {"partymode": "music"}})).await
            }
        },
        "CMD_STOP" => player_command(kodi, "Player.Stop", json!({})).await,
        "CMD_NEXT" => player_command(kodi, "Player.GoTo", json!({"to": "next"})).await,
        "CMD_PREVIOUS" | "CMD_PREV" => {
            player_command(kodi, "Player.GoTo", json!({"to": "previous"})).await
        }
        "CMD_SEEK_FORWARD" | "CMD_FORWARD" => {
            player_command(kodi, "Player.Seek", json!({"value": {"step": "smallforward"}})).await
        }
        "CMD_SEEK_BACKWARD" | "CMD_REWIND" | "CMD_BACKWARD" => {
            player_command(kodi, "Player.Seek", json!({"value": {"step": "smallbackward"}})).await
        }
        other => Err(anyhow!("commande inconnue: {other}")),
    }
}

async fn player_command(kodi: &KodiConfig, method: &str, mut params: Value) -> anyhow::Result<Value> {
    let player_id = active_player_id(kodi)
        .await?
        .ok_or_else(|| anyhow!("aucune lecture active"))?;
    if let Some(object) = params.as_object_mut() {
        object.insert("playerid".to_string(), json!(player_id));
    }
    kodi_json_rpc(kodi, method, params).await
}

async fn active_player_id(kodi: &KodiConfig) -> anyhow::Result<Option<i64>> {
    let response = kodi_json_rpc(kodi, "Player.GetActivePlayers", Value::Null).await?;
    let Some(players) = response
        .get("kodi")
        .and_then(|value| value.get("result"))
        .and_then(Value::as_array)
    else {
        return Ok(None);
    };
    Ok(players
        .first()
        .and_then(|player| player.get("playerid"))
        .and_then(Value::as_i64))
}

async fn kodi_json_rpc(kodi: &KodiConfig, method: &str, params: Value) -> anyhow::Result<Value> {
    let mut payload = json!({
        "jsonrpc": "2.0",
        "id": "troozn-webtransport",
        "method": method,
    });
    if !params.is_null() {
        payload["params"] = params;
    }
    let body = payload.to_string();

    let mut stream = TcpStream::connect((kodi.host.as_str(), kodi.port))
        .await
        .with_context(|| format!("Kodi indisponible sur {}:{}", kodi.host, kodi.port))?;
    let auth_header = kodi_authorization_header(kodi)
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "POST /jsonrpc HTTP/1.1\r\nHost: {}:{}\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        kodi.host,
        kodi.port,
        auth_header,
        body.as_bytes().len(),
        body,
    );
    stream.write_all(request.as_bytes()).await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let body = parse_http_response_body(&response).context("reponse HTTP Kodi invalide")?;
    let body = std::str::from_utf8(&body).context("reponse Kodi non UTF-8")?;
    let value: Value = serde_json::from_str(body.trim())
        .with_context(|| format!("JSON-RPC Kodi invalide: {}", preview_text(body, 160)))?;
    Ok(json!({"ok": true, "kodi": value}))
}

fn parse_http_response_body(response: &[u8]) -> anyhow::Result<Vec<u8>> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| anyhow!("en-tetes HTTP absents"))?;
    let headers =
        std::str::from_utf8(&response[..header_end]).context("en-tetes HTTP non UTF-8")?;
    let body = &response[header_end + 4..];

    let status_code = http_status_code(headers)
        .ok_or_else(|| anyhow!("ligne de statut HTTP Kodi absente"))?;
    if !(200..300).contains(&status_code) {
        let body_text = String::from_utf8_lossy(body);
        return Err(anyhow!(
            "Kodi HTTP {status_code}: {}",
            preview_text(&body_text, 160)
        ));
    }

    if header_value(headers, "transfer-encoding")
        .map(|value| value.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false)
    {
        return decode_chunked_body(body);
    }

    if let Some(length) = header_value(headers, "content-length")
        .and_then(|value| value.parse::<usize>().ok())
    {
        if body.len() < length {
            return Err(anyhow!(
                "corps HTTP incomplet: {} octets recus sur {length}",
                body.len()
            ));
        }
        return Ok(body[..length].to_vec());
    }

    Ok(body.to_vec())
}

fn http_status_code(headers: &str) -> Option<u16> {
    let status = headers.lines().next()?;
    let mut parts = status.split_whitespace();
    parts.next()?;
    parts.next()?.parse().ok()
}

fn header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.trim().eq_ignore_ascii_case(name) {
            Some(value.trim())
        } else {
            None
        }
    })
}

fn decode_chunked_body(body: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(body.len());
    let mut offset = 0usize;

    loop {
        let line_end = find_crlf(&body[offset..])
            .ok_or_else(|| anyhow!("taille de chunk HTTP absente"))?
            + offset;
        let size_line = std::str::from_utf8(&body[offset..line_end])
            .context("taille de chunk HTTP non UTF-8")?;
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .with_context(|| format!("taille de chunk HTTP invalide: {size_hex}"))?;
        offset = line_end + 2;

        if size == 0 {
            break;
        }

        let chunk_end = offset
            .checked_add(size)
            .ok_or_else(|| anyhow!("taille de chunk HTTP trop grande"))?;
        if body.len() < chunk_end + 2 {
            return Err(anyhow!("chunk HTTP incomplet"));
        }
        decoded.extend_from_slice(&body[offset..chunk_end]);
        if &body[chunk_end..chunk_end + 2] != b"\r\n" {
            return Err(anyhow!("terminateur de chunk HTTP invalide"));
        }
        offset = chunk_end + 2;
    }

    Ok(decoded)
}

fn kodi_authorization_header(kodi: &KodiConfig) -> Option<String> {
    let username = kodi.username.as_deref()?;
    Some(format!(
        "Basic {}",
        base64_encode(format!("{username}:{}", kodi.password).as_bytes())
    ))
}

fn base64_encode(value: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(((value.len() + 2) / 3) * 4);
    for chunk in value.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        output.push(TABLE[(b0 >> 2) as usize] as char);
        output.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

fn find_crlf(value: &[u8]) -> Option<usize> {
    value.windows(2).position(|window| window == b"\r\n")
}

fn preview_text(value: &str, max_chars: usize) -> String {
    let mut preview = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
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

fn normalize_phone_role(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "admin" | "principal" => "admin".into(),
        _ => "guest".into(),
    }
}

fn safe_folder_name(value: &str) -> String {
    let trimmed = value.trim();
    let safe = trimmed
        .chars()
        .map(|ch| {
            if ch == '/' || ch == '\\' || ch.is_control() {
                '_'
            } else {
                ch
            }
        })
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();
    if safe.is_empty() {
        "Telephone".into()
    } else {
        safe
    }
}

fn unique_folder_name(
    display_name: &str,
    device_id: &str,
    phones: &HashMap<String, Arc<PhoneSession>>,
) -> String {
    let base = safe_folder_name(display_name);
    let collides = phones
        .values()
        .any(|phone| phone.id != device_id && phone.folder_name == base);
    if !collides {
        return base;
    }

    let suffix = safe_device_id(device_id);
    let candidate = format!("{base} ({suffix})");
    if phones
        .values()
        .any(|phone| phone.id != device_id && phone.folder_name == candidate)
    {
        format!("{base} ({})", unix_timestamp())
    } else {
        candidate
    }
}

fn default_source_visible() -> bool {
    true
}

fn default_phone_role() -> String {
    "guest".into()
}

fn phone_is_connected(phone: &PhoneSession) -> bool {
    let disconnected_at = phone.disconnected_at.load(Ordering::Relaxed);
    disconnected_at == 0
        || unix_timestamp().saturating_sub(disconnected_at) <= PHONE_DISCONNECT_GRACE_SECONDS
}

fn phone_is_published(phone: &PhoneSession) -> bool {
    phone.source_visible && phone_is_connected(phone)
}

fn prune_stale_phones(phones: &mut HashMap<String, Arc<PhoneSession>>) {
    let now = unix_timestamp();
    phones.retain(|_, phone| {
        let disconnected_at = phone.disconnected_at.load(Ordering::Relaxed);
        disconnected_at == 0
            || now.saturating_sub(disconnected_at) <= PHONE_DISCONNECT_GRACE_SECONDS
    });
}

fn troozn_state_dir() -> PathBuf {
    std::env::var("TROOZN_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./troozn-state"))
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn storage_error(error: anyhow::Error) -> Error {
    Error::new(ErrorKind::LocalError, error.to_string())
}
