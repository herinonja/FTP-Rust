use axum::{routing::post, Json, Extension, Router};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

type PhoneRegistry = Arc<RwLock<HashMap<String, String>>>;

#[derive(Deserialize, Debug)]
struct RegisterPayload {
    phone_id: String,
    phone_ip: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let registry: PhoneRegistry = Arc::new(RwLock::new(HashMap::new()));

    // -----------------------------------------------------------------
    // TÂCHE 1 : Serveur HTTP d'Enregistrement (Port 3000)
    // -----------------------------------------------------------------
    let http_registry = registry.clone();
    let http_app = Router::new()
        .route("/register", post(register_phone))
        .layer(Extension(http_registry));

    tokio::spawn(async move {
        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        info!("Serveur d'enregistrement HTTP actif sur http://{}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, http_app).await.unwrap();
    });

    // -----------------------------------------------------------------
    // TÂCHE 2 : Serveur FTP Virtuel pour Kodi (Port 2121)
    // -----------------------------------------------------------------
    let ftp_registry = registry.clone();
    
    let ftp_server = libunftp::ServerBuilder::new(Box::new(move || {
        VirtualStorage::new(ftp_registry.clone())
    }))
    .greeting("Bienvenue sur le Proxy FTP de votre Radxa Zero 3W")
    .passive_ports(50000..=50100)
    .build()?;

    let ftp_addr = "0.0.0.0:2121";
    info!("Serveur FTP virtuel prêt sur {}", ftp_addr);
    
    ftp_server.listen(ftp_addr).await?;

    Ok(())
}

async fn register_phone(
    Extension(registry): Extension<PhoneRegistry>,
    Json(payload): Json<RegisterPayload>,
) -> &'static str {
    let mut table = registry.write().await;
    table.insert(payload.phone_id.clone(), payload.phone_ip.clone());
    info!("Téléphone enregistré : {} -> IP: {}", payload.phone_id, payload.phone_ip);
    "Enregistrement réussi !"
}

// -----------------------------------------------------------------
// SYSTÈME DE FICHIERS VIRTUEL FIXÉ
// -----------------------------------------------------------------
use unftp_core::storage::{StorageBackend, Fileinfo, Metadata, Error, ErrorKind};

#[derive(Debug)]
struct VirtualStorage {
    registry: PhoneRegistry,
}

impl VirtualStorage {
    fn new(registry: PhoneRegistry) -> Self {
        Self { registry }
    }
}

#[derive(Debug)]
struct VirtualMetadata {
    is_dir: bool,
}

impl Metadata for VirtualMetadata {
    fn len(&self) -> u64 { 0 }
    fn is_dir(&self) -> bool { self.is_dir }
    fn is_file(&self) -> bool { !self.is_dir }
    fn is_symlink(&self) -> bool { false }
    fn modified(&self) -> unftp_core::storage::Result<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }
    fn gid(&self) -> u32 { 0 }
    fn uid(&self) -> u32 { 0 }
}

#[async_trait::async_trait]
impl<User: unftp_core::auth::UserDetail> StorageBackend<User> for VirtualStorage {
    type Metadata = VirtualMetadata;

    async fn list<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Vec<Fileinfo<std::path::PathBuf, Self::Metadata>>>
    where
        P: AsRef<std::path::Path> + Send,
    {
        let path = path.as_ref();
        
        if path == std::path::Path::new("") || path == std::path::Path::new("/") {
            let table = self.registry.read().await;
            let mut list = Vec::new();

            for phone_id in table.keys() {
                let file_info = Fileinfo {
                    path: std::path::PathBuf::from(phone_id),
                    metadata: VirtualMetadata { is_dir: true },
                };
                list.push(file_info);
            }
            return Ok(list);
        }

        Ok(vec![])
    }

    async fn metadata<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Self::Metadata>
    where P: AsRef<std::path::Path> + Send {
        let path = path.as_ref();
        if path == std::path::Path::new("") || path == std::path::Path::new("/") {
            return Ok(VirtualMetadata { is_dir: true });
        }
        Ok(VirtualMetadata { is_dir: true })
    }

    // AJOUT DE LA MÉTHODE MANQUANTE CWD
    // Permet à Kodi de naviguer dans le chemin virtuel
    async fn cwd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> 
    where P: AsRef<std::path::Path> + Send {
        // On autorise la navigation pour l'instant
        Ok(())
    }

    // CORRECTION DE LA SIGNATURE DE GET
    // En retournant un flux de lecture dynamique mis dans une Box, on satisfait le compilateur
    async fn get<P>(&self, _user: &User, _path: P, _start_pos: u64) -> unftp_core::storage::Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>> 
    where P: AsRef<std::path::Path> + Send { 
        Err(Error::new(ErrorKind::PermanentFileNotAvailable, "En cours de développement")) 
    }
    
    async fn put<P, R>(&self, _user: &User, _bytes: R, _path: P, _start_pos: u64) -> unftp_core::storage::Result<u64> where P: AsRef<std::path::Path> + Send, R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static { Ok(0) }
    async fn del<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
    async fn rename<P>(&self, _user: &User, _from: P, _to: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
}
