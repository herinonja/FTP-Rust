use async_trait::async_trait;
use libunftp::Server;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use unftp_core::storage::{Error, ErrorKind, Fileinfo, Metadata, StorageBackend};
use tokio_util::compat::FuturesAsyncReadCompatExt;

// -------------------------------------------------------------------------
// 1. DÉFINITION DU STOCKAGE (PROXY)
// -------------------------------------------------------------------------

#[derive(Debug)]
pub struct ProxyStorage;

impl ProxyStorage {
    pub fn new() -> Self {
        ProxyStorage {}
    }

    // Résolution du chemin: Extrait l'IP du téléphone et le chemin cible.
    // NOTE : Modifiez cette logique selon le format exact de vos requêtes Kodi.
    // Exemple ici : /192.168.1.10/DCIM/video.mp4 -> IP: 192.168.1.10, Cible: /DCIM/video.mp4
    async fn resolve_path(&self, path: &Path) -> unftp_core::storage::Result<(String, PathBuf)> {
        let mut components = path.components();
        components.next(); // Ignorer la racine (/)
        
        let ip_component = components.next().ok_or_else(|| {
            Error::new(ErrorKind::FileNameNotAllowed, "Format de chemin invalide (IP du téléphone manquante)")
        })?;
        
        let ip = ip_component.as_os_str().to_string_lossy().into_owned();
        let target_path: PathBuf = components.collect();
        
        Ok((ip, target_path))
    }

    // Ouvre une connexion FTP à la volée vers le téléphone cible
    async fn connect_to_phone(&self, ip: &str) -> unftp_core::storage::Result<suppaftp::AsyncFtpStream> {
        // Port 2121, typique des serveurs FTP sur Android/iOS
        let addr = format!("{}:2121", ip); 
        
        let mut ftp_stream = suppaftp::AsyncFtpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionClosed, format!("Impossible de se connecter au téléphone: {}", e)))?;
        
        ftp_stream.login("anonymous", "anonymous")
            .await
            .map_err(|e| Error::new(ErrorKind::LocalError, format!("Erreur d'authentification sur le téléphone: {}", e)))?;

        Ok(ftp_stream)
    }
}

// -------------------------------------------------------------------------
// 2. IMPLÉMENTATION DU SERVEUR FTP (StorageBackend)
// -------------------------------------------------------------------------

#[async_trait]
impl<User: Send + Sync + Debug> StorageBackend<User> for ProxyStorage {
    fn name(&self) -> &str {
        "KodiFtpProxy"
    }

    // -- RÉCUPÉRATION DE FICHIER (LA MAGIE OPÈRE ICI) --
    async fn get<P>(&self, _user: &User, path: P, start_pos: u64) -> unftp_core::storage::Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>>
    where
        P: AsRef<Path> + Send + Sync,
    {
        let path = path.as_ref();
        let (phone_ip, target_path) = self.resolve_path(path).await?;
        
        // 1. Connexion au serveur source (le téléphone)
        let mut client = self.connect_to_phone(&phone_ip).await?;
        let path_str = target_path.to_string_lossy().into_owned();
        
        // 2. Gestion de l'avance rapide (Seek)
        if start_pos > 0 {
            // Transmet la commande REST au téléphone pour démarrer au bon octet
            client.resume_transfer(start_pos as usize);
        }

        // 3. Lancement du téléchargement (commande RETR)
        let data_stream = client.retr_as_stream(&path_str)
            .await
            .map_err(|e| {
                eprintln!("Erreur lors du téléchargement FTP : {}", e);
                Error::new(ErrorKind::PermanentFileNotAvailable, "Impossible de lire le flux depuis le téléphone")
            })?;

        // 4. Bridge Asynchrone (futures::io -> tokio::io)
        let tokio_stream = data_stream.compat();

        Ok(Box::new(tokio_stream))
    }

    // -- FONCTIONNALITÉS EN LECTURE (À COMPLÉTER SELON VOS BESOINS) --
    
    async fn metadata<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<Metadata>
    where
        P: AsRef<Path> + Send + Sync,
    {
        // TODO: Implémentez la commande SIZE/MDTM vers le téléphone si Kodi en a besoin.
        Err(Error::new(ErrorKind::CommandNotImplemented, "metadata non implémenté"))
    }

    async fn list<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<Vec<Fileinfo<PathBuf, Metadata>>>
    where
        P: AsRef<Path> + Send + Sync,
    {
        // TODO: Implémentez la commande LIST vers le téléphone.
        Err(Error::new(ErrorKind::CommandNotImplemented, "list non implémenté"))
    }

    async fn cwd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send + Sync,
    {
        Ok(()) // Autorise toujours la navigation
    }

    // -- FONCTIONNALITÉS EN ÉCRITURE (BLOQUÉES POUR UN PROXY DE LECTURE) --

    async fn put<P, R>(&self, _user: &User, _input: R, _path: P, _start_pos: u64) -> unftp_core::storage::Result<u64>
    where
        P: AsRef<Path> + Send + Sync,
        R: tokio::io::AsyncRead + Send + Sync + Unpin,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Put interdit)"))
    }

    async fn del<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send + Sync,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Del interdit)"))
    }

    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send + Sync,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Mkd interdit)"))
    }

    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where
        P: AsRef<Path> + Send + Sync,
    {
        Err(Error::new(ErrorKind::PermissionDenied, "Ce proxy est en lecture seule (Rmd interdit)"))
    }
}

// -------------------------------------------------------------------------
// 3. POINT D'ENTRÉE DU SERVEUR
// -------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let port = 2120; // Le port sur lequel votre Radxa (le proxy) va écouter
    let addr = format!("0.0.0.0:{}", port);
    
    println!("Démarrage du Kodi FTP Proxy sur ftp://{}", addr);

    let server = Server::with_fs(ProxyStorage::new());

    if let Err(e) = server.listen(&addr).await {
        eprintln!("Erreur critique du serveur : {}", e);
    }
}
