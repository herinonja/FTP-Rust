use anyhow::{anyhow, Context};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use libunftp::ServerBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio_util::io::ReaderStream;
use unftp_core::auth::UserDetail;
use unftp_core::storage::{Error, ErrorKind, Fileinfo, StorageBackend, FEATURE_RESTART};
use wtransport::tls::Sha256DigestFmt;
use wtransport::{Connection, Endpoint, Identity, ServerConfig};

const DEFAULT_FTP_BIND: &str = "127.0.0.1:2120";
const DEFAULT_HTTP_BIND: &str = "127.0.0.1:8787";
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
struct KodiConfig {
    host: String,
    port: u16,
}

#[derive(Debug)]
struct PhoneSession {
    id: String,
    display_name: String,
    folder_name: String,
    connection: Arc<Connection>,
    disconnected_at: AtomicU64,
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
        let mut phones = self.phones.write().await;
        let folder_name = unique_folder_name(&display_name, &id, &phones);

        let session = Arc::new(PhoneSession {
            id: id.clone(),
            display_name,
            folder_name,
            connection,
            disconnected_at: AtomicU64::new(0),
        });

        println!(
            "Telephone WebTransport enregistre: {} -> {}",
            session.display_name, session.folder_name
        );
        phones.insert(id, session);
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
            .filter(|phone| phone_is_visible(phone))
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
            .filter(|phone| phone_is_visible(phone))
            .find(|phone| phone.folder_name == folder_name)
            .cloned()
            .or_else(|| phones.get(folder_name).filter(|phone| phone_is_visible(phone)).cloned())
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
    #[serde(rename = "proxy.list")]
    ProxyList { path: String },
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

    println!("=============================================================");
    println!(" TROOZN RADXA PROXY");
    println!(" Kodi lit legacy: ftp://{ftp_bind}/");
    println!(" Kodi lit media: http://{http_bind}/media/<telephone>/<chemin>");
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
        "webTransportBind": bind,
        "webTransportPort": bind.rsplit(':').next().and_then(|value| value.parse::<u16>().ok()).unwrap_or(4433),
        "webTransportCertHash": cert_hash,
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

fn phone_is_visible(phone: &PhoneSession) -> bool {
    let disconnected_at = phone.disconnected_at.load(Ordering::Relaxed);
    disconnected_at == 0
        || unix_timestamp().saturating_sub(disconnected_at) <= PHONE_DISCONNECT_GRACE_SECONDS
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
