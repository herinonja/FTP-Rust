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
// SYSTÈME DE FICHIERS VIRTUEL PROXY DYNAMIQUE
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

    // Extrait l'ID du téléphone et cherche son IP associée
    // Exemple: "/Tel_Alex/DCIM/video.mp4" -> IP du téléphone, et chemin: "DCIM/video.mp4"
    async fn resolve_path(&self, path: &std::path::Path) -> Result<(String, std::path::PathBuf), Error> {
        let mut components = path.components();
        
        // On passe le premier slash "/" si présent
        if path.is_absolute() {
            components.next();
        }

        // Le premier dossier réel correspond à notre ID de téléphone
        let phone_id = match components.next() {
            Some(std::path::Component::Normal(name)) => name.to_string_lossy().into_owned(),
            _ => return Err(Error::new(ErrorKind::PermanentFileNotAvailable, "ID de téléphone manquant")),
        };

        // On reconstruit le reste du chemin destiné au stockage du téléphone
        let remaining_path: std::path::PathBuf = components.collect();

        // On cherche l'IP dans le registre
        let table = self.registry.read().await;
        if let Some(ip) = table.get(&phone_id) {
            Ok((ip.clone(), remaining_path))
        } else {
            Err(Error::new(ErrorKind::PermanentFileNotAvailable, "Téléphone non connecté ou introuvable"))
        }
    }

    // Ouvre une connexion FTP à la volée vers le téléphone cible
    async fn connect_to_phone(&self, ip: &str) -> Result<suppaftp::AsyncFtpStream, Error> {
        // Port sur lequel votre application mobile fait tourner son serveur FTP
        let addr = format!("{}:2121", ip); 
        let mut ftp_stream = suppaftp::AsyncFtpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionClosed, e.to_string()))?;
        
        // Connexion anonyme par défaut vers le téléphone
        ftp_stream.login("anonymous", "anonymous")
            .await
            .map_err(|e| Error::new(ErrorKind::LocalError, e.to_string()))?;

        Ok(ftp_stream)
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
        
        // CAS 1 : Kodi explore la racine absolue -> on liste les smartphones connectés
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

        // CAS 2 : Kodi accède à un répertoire à l'intérieur d'un téléphone particulier
        let (phone_ip, target_path) = self.resolve_path(path).await?;
        let mut client = self.connect_to_phone(&phone_ip).await?;

        let path_str = target_path.to_string_lossy().into_owned();
        let remote_files = client.list(Some(&path_str))
            .await
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, e.to_string()))?;

        let mut list = Vec::new();
        for file_line in remote_files {
            // Extraction basique du nom du fichier en bout de ligne d'une réponse de type 'ls'
            let filename = file_line.split_whitespace().last().unwrap_or("fichier").to_string();
            
            list.push(Fileinfo {
                path: std::path::PathBuf::from(filename),
                // Détection rudimentaire : si la ligne commence par 'd' c'est un répertoire Unix standard
                metadata: VirtualMetadata { is_dir: file_line.starts_with('d') },
            });
        }

        Ok(list)
    }

    async fn metadata<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Self::Metadata>
    where P: AsRef<std::path::Path> + Send {
        let path = path.as_ref();
        if path == std::path::Path::new("") || path == std::path::Path::new("/") {
            return Ok(VirtualMetadata { is_dir: true });
        }

        // Si on demande la racine exacte d'un téléphone (/Tel_Alex), c'est obligatoirement un dossier
        let mut components = path.components();
        if path.is_absolute() { components.next(); }
        components.next(); 
        
        if components.next().is_none() {
            return Ok(VirtualMetadata { is_dir: true });
        }

        // Pour éviter d'introduire des latences réseaux sur l'analyse de chaque métadonnée demandée par Kodi,
        // on trie pragmatiquement selon l'extension de fichier connue.
        let is_video = path.extension()
            .map(|ext| ext == "mp4" || ext == "mkv" || ext == "avi" || ext == "mov")
            .unwrap_or(false);

        Ok(VirtualMetadata { is_dir: !is_video })
    }

    async fn cwd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> 
    where P: AsRef<std::path::Path> + Send {
        // Navigation virtuelle validée d'office pour autoriser le parcours
        Ok(())
    }

    async fn get<P>(&self, _user: &User, path: P, start_pos: u64) -> unftp_core::storage::Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>> 
    where P: AsRef<std::path::Path> + Send { 
        let path = path.as_ref();
        let (phone_ip, target_path) = self.resolve_path(path).await?;
        let mut client = self.connect_to_phone(&phone_ip).await?;

        let path_str = target_path.to_string_lossy().into_owned();
        
        // Prise en charge native du Seek (Avancer/Reculer dans le temps sur Kodi)
        if start_pos > 0 {
            client.restart_from(start_pos)
                .await
                .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, e.to_string()))?;
        }

        // Extraction directe du flux de données
        let data_stream = client.retr_as_stream(&path_str)
            .await
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, e.to_string()))?;

        Ok(Box::new(data_stream))
    }
    
    async fn put<P, R>(&self, _user: &User, _bytes: R, _path: P, _start_pos: u64) -> unftp_core::storage::Result<u64> where P: AsRef<std::path::Path> + Send, R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static { Ok(0) }
    async fn del<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
    async fn rename<P>(&self, _user: &User, _from: P, _to: P) -> unftp_core::storage::Result<()> where P: AsRef<std::path::Path> + Send { Ok(()) }
}
