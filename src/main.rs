use async_trait::async_trait;
use libunftp::Server;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use unftp_core::auth::UserDetail;
use unftp_core::storage::{Error, ErrorKind, Fileinfo, StorageBackend};
use tokio_util::compat::FuturesAsyncReadCompatExt;

// -------------------------------------------------------------------------
// 1. DÉFINITION DU STOCKAGE ET DES MÉTADONNÉES
// -------------------------------------------------------------------------

#[derive(Debug)]
pub struct ProxyStorage;

impl ProxyStorage {
    pub fn new() -> Self {
        ProxyStorage {}
    }

    async fn resolve_path(&self, path: &Path) -> unftp_core::storage::Result<(String, PathBuf)> {
        let mut components = path.components();
        components.next(); // Ignorer la racine (/)
        
        let ip_component = components.next().ok_or_else(|| {
            // CORRECTION: FileNameNotAllowedError au lieu de FileNameNotAllowed
            Error::new(ErrorKind::FileNameNotAllowedError, "Format de chemin invalide (IP du téléphone manquante)")
        })?;
        
        let ip = ip_component.as_os_str().to_string_lossy().into_owned();
        let target_path: PathBuf = components.collect();
        
        Ok((ip, target_path))
    }

    async fn connect_to_phone(&self, ip: &str) -> unftp_core::storage::Result<suppaftp::AsyncFtpStream> {
        let addr = format!("{}:2121", ip); 
        
        let mut ftp_stream = suppaftp::AsyncFtpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionClosed, format!("Impossible de se connecter: {}", e)))?;
        
        ftp_stream.login("anonymous", "anonymous")
            .await
            .map_err(|e| Error::new(ErrorKind::LocalError, format!("Erreur d'authentification: {}", e)))?;

        Ok(ftp_stream)
    }
}

// CORRECTION: Création d'une structure Metadata factice pour satisfaire le contrat.
// Kodi ne l'utilise généralement pas pour simplement lire un flux.
#[derive(Debug)]
pub struct ProxyMetadata;

impl unftp_core::storage::Metadata for ProxyMetadata {
    fn len(&self) -> u64 { 0 }
    fn is_dir(&self) -> bool { false }
    fn is_file(&self) -> bool { true }
    fn is_symlink(&self) -> bool { false }
    fn modified(&self) -> unftp_core::storage::Result<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }
    fn gid(&self) -> u32 { 0 }
    fn uid(&self) -> u32 { 0 }
}

// -------------------------------------------------------------------------
// 2. IMPLÉMENTATION DU SERVEUR FTP
// -------------------------------------------------------------------------

// CORRECTION: Ajout du trait UserDetail dans les contraintes de User
#[async_trait]
impl<User: UserDetail + Send + Sync + Debug> StorageBackend<User> for ProxyStorage {
    // CORRECTION: Association de notre type Metadata au backend
    type Metadata = ProxyMetadata;

    fn name(&self) -> &str {
        "KodiFtpProxy"
    }

    // CORRECTION: P: AsRef<Path> + Send (suppression du + Sync qui était de trop)
    async fn get<P>(&self, _user: &User, path: P, start_pos: u64) -> unftp_core::storage::Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        let (phone_ip, target_path) = self.resolve_path(path).await?;
        
        let mut client = self.connect_to_phone(&phone_ip).await?;
        let path_str = target_path.to_string_lossy().into_owned();
        
        if start_pos > 0 {
            client.resume_transfer(start_pos as usize);
        }

        let data_stream = client.retr_as_stream(&path_str)
            .await
            .map_err(|e| {
                eprintln!("Erreur lors du téléchargement FTP : {}", e);
                Error::new(ErrorKind::PermanentFileNotAvailable, "Impossible de lire le flux depuis le téléphone")
            })?;

        let tokio_stream = data_stream.compat();

        Ok(Box::new(tokio_stream))
    }
    
    // CORRECTION: Utilisation de Self::Metadata
    async fn metadata<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<Self::Metadata>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::CommandNotImplemented, "metadata non implémenté"))
    }

    // CORRECTION: Utilisation de Self::Metadata
    async fn list<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<Vec<Fileinfo<PathBuf, Self::Metadata>>>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::CommandNotImplemented, "list non implémenté"))
    }

    async fn cwd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Ok(())
    }

    // CORRECTION: Ajout de la méthode rename obligatoire
    async fn rename<P>(&self, _user: &User, _from: P, _to: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Rename interdit)"))
    }

    async fn put<P, R>(&self, _user: &User, _input: R, _path: P, _start_pos: u64) -> unftp_core::storage::Result<u64>
    where
        P: AsRef<Path> + Send,
        R: tokio::io::AsyncRead + Send + Sync + Unpin,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Put interdit)"))
    }

    async fn del<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Del interdit)"))
    }

    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Mkd interdit)"))
    }

    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Rmd interdit)"))
    }
}

// -------------------------------------------------------------------------
// 3. POINT D'ENTRÉE DU SERVEUR
// -------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let port = 2120;
    let addr = format!("0.0.0.0:{}", port);
    
    println!("Démarrage du Kodi FTP Proxy sur ftp://{}", addr);

    // CORRECTION: Initialisation avec Server::new et une closure pour notre stockage personnalisé
    let server = Server::new(Box::new(move || ProxyStorage::new()));

    if let Err(e) = server.listen(&addr).await {
        eprintln!("Erreur critique du serveur : {}", e);
    }
}
