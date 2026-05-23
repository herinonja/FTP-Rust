use axum::{routing::post, Json, Extension, Router};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};

// Définition de la table de routage partagée (ID du téléphone -> Adresse IP)
type PhoneRegistry = Arc<RwLock<HashMap<String, String>>>;

#[derive(Deserialize, Debug)]
struct RegisterPayload {
    phone_id: String,
    phone_ip: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // L'état de notre application : la table de routage en mémoire
    let registry: PhoneRegistry = Arc::new(RwLock::new(HashMap::new()));

    // -----------------------------------------------------------------
    // TÂCHE 1 : Démarrer le Serveur HTTP d'Enregistrement (Port 3000)
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
    // TÂCHE 2 : Démarrer le Serveur FTP Virtuel pour Kodi (Port 2121)
    // -----------------------------------------------------------------
    let ftp_registry = registry.clone();
    
    // On instancie le serveur unftp en lui injectant notre système de fichier personnalisé
    let ftp_server = unftp_server::Server::new(move || {
        VirtualStorage::new(ftp_registry.clone())
    });

    let ftp_addr = "0.0.0.0:2121";
    info!("Serveur FTP virtuel actif sur {}", ftp_addr);
    
    // Lance le serveur FTP au premier plan (bloquant)
    ftp_server.listen(ftp_addr).await?;

    Ok(())
}

// Handler HTTP pour enregistrer les téléphones
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
// SYSTÈME DE FICHIERS VIRTUEL POUR KODI
// -----------------------------------------------------------------
// Cette structure implémente les fonctions que le serveur FTP appelle
// lorsque Kodi explore les dossiers.
// -----------------------------------------------------------------
use unftp_server::storage::{StorageBackend, Fileinfo, Metadata};

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
    fn modified(&self) -> unftp_server::storage::Result<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }
    fn gid(&self) -> u32 { 0 }
    fn uid(&self) -> u32 { 0 }
}

#[async_trait::async_trait]
impl<User> StorageBackend<User> for VirtualStorage {
    type Metadata = VirtualMetadata;

    // Quand Kodi liste le contenu d'un dossier (Commande FTP: LIST)
    async fn list<P>(&self, _user: &User, path: P) -> unftp_server::storage::Result<Vec<Fileinfo<std::path::PathBuf, Self::Metadata>>>
    where
        P: AsRef<std::path::Path> + Send + Sync,
    {
        let path = path.as_ref();
        
        // Si Kodi est à la racine "/" du FTP
        if path == std::path::Path::new("") || path == std::path::Path::new("/") {
            let table = self.registry.read().await;
            let mut list = Vec::new();

            // Pour chaque téléphone connecté, on crée un "faux" dossier
            for phone_id in table.keys() {
                let file_info = Fileinfo {
                    path: std::path::PathBuf::from(phone_id),
                    metadata: VirtualMetadata { is_dir: true },
                };
                list.push(file_info);
            }
            return Ok(list);
        }

        // Étape 4 (prochaine étape) : Gérer l'exploration à l'intérieur d'un dossier de téléphone
        Ok(vec![])
    }

    // Fonctions obligatoires minimales pour le compilateur
    async fn metadata<P>(&self, _user: &User, path: P) -> unftp_server::storage::Result<Self::Metadata>
    where P: AsRef<std::path::Path> + Send + Sync {
        let path = path.as_ref();
        // La racine est toujours un dossier
        if path == std::path::Path::new("") || path == std::path::Path::new("/") {
            return Ok(VirtualMetadata { is_dir: true });
        }
        // Par défaut pour l'instant on dit que c'est un dossier
        Ok(VirtualMetadata { is_dir: true })
    }

    // Les méthodes ci-dessous seront implémentées à l'étape 4 pour le streaming de fichiers.
    // On met des implémentations vides pour que le code compile à l'étape 3.
    async fn get<P>(&self, _user: &User, _path: P, _start_pos: u64) -> unftp_server::storage::Result<unftp_server::storage::GetResult> where P: AsRef<std::path::Path> + Send + Sync { 
        Err(unftp_server::storage::StorageError::new(unftp_server::storage::StorageErrorKind::FileNotFound, "Non implémenté")) 
    }
    async fn put<P, R>(&self, _user: &User, _bytes: R, _path: P, _start_pos: u64) -> unftp_server::storage::Result<u64> where P: AsRef<std::path::Path> + Send + Sync, R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static { Ok(0) }
    async fn del<P>(&self, _user: &User, _path: P) -> unftp_server::storage::Result<()> where P: AsRef<std::path::Path> + Send + Sync { Ok(()) }
    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_server::storage::Result<()> where P: AsRef<std::path::Path> + Send + Sync { Ok(()) }
    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_server::storage::Result<()> where P: AsRef<std::path::Path> + Send + Sync { Ok(()) }
    async fn rename<P>(&self, _user: &User, _from: P, _to: P) -> unftp_server::storage::Result<()> where P: AsRef<std::path::Path> + Send + Sync { Ok(()) }
}
