use async_trait::async_trait;
use libunftp::ServerBuilder;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use unftp_core::auth::UserDetail;
use unftp_core::storage::{Error, ErrorKind, Fileinfo, StorageBackend};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use regex::Regex;

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
        
        if path.is_absolute() {
            components.next();
        }
        
        let ip_component = components.next().ok_or_else(|| {
            Error::new(ErrorKind::FileNameNotAllowedError, "IP du téléphone manquante dans le chemin")
        })?;
        
        let ip = ip_component.as_os_str().to_string_lossy().into_owned();
        let target_path: PathBuf = components.collect();
        
        Ok((ip, target_path))
    }

    async fn connect_to_phone(&self, ip: &str) -> unftp_core::storage::Result<suppaftp::AsyncFtpStream> {
        let addr = format!("{}:2121", ip); 
        
        let mut ftp_stream = suppaftp::AsyncFtpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionClosed, format!("Connexion échouée: {}", e)))?;
        
        ftp_stream.login("anonymous", "anonymous")
            .await
            .map_err(|e| Error::new(ErrorKind::LocalError, format!("Refus d'authentification: {}", e)))?;

        Ok(ftp_stream)
    }
}

#[derive(Debug)]
pub struct ProxyMetadata {
    pub is_dir: bool,
    pub size: u64,
}

impl unftp_core::storage::Metadata for ProxyMetadata {
    fn len(&self) -> u64 { self.size }
    fn is_dir(&self) -> bool { self.is_dir }
    fn is_file(&self) -> bool { !self.is_dir }
    fn is_symlink(&self) -> bool { false }
    fn modified(&self) -> unftp_core::storage::Result<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }
    fn gid(&self) -> u32 { 0 }
    fn uid(&self) -> u32 { 0 }
}

// -------------------------------------------------------------------------
// 2. IMPLÉMENTATION DES COMMANDES FTP
// -------------------------------------------------------------------------

#[async_trait]
impl<User: UserDetail + Send + Sync + Debug> StorageBackend<User> for ProxyStorage {
    type Metadata = ProxyMetadata;

    fn name(&self) -> &str {
        "KodiFtpProxy"
    }

    async fn get<P>(&self, _user: &User, path: P, start_pos: u64) -> unftp_core::storage::Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        let (phone_ip, target_path) = self.resolve_path(path).await?;
        let mut client = self.connect_to_phone(&phone_ip).await?;
        let path_str = target_path.to_string_lossy().into_owned();
        
        if start_pos > 0 {
            client.resume_transfer(start_pos as usize)
                .await
                .map_err(|e| Error::new(ErrorKind::LocalError, format!("Erreur Seek : {}", e)))?;
        }

        let data_stream = client.retr_as_stream(&path_str)
            .await
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("Erreur RETR: {}", e)))?;

        Ok(Box::new(data_stream.compat()))
    }

    async fn metadata<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Self::Metadata>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        
        if path == Path::new("") || path == Path::new("/") {
            return Ok(ProxyMetadata { is_dir: true, size: 0 });
        }

        let (phone_ip, target_path) = match self.resolve_path(path).await {
            Ok(res) => res,
            Err(_) => return Ok(ProxyMetadata { is_dir: true, size: 0 }),
        };

        if target_path == Path::new("") || target_path == Path::new("/") {
            return Ok(ProxyMetadata { is_dir: true, size: 0 });
        }

        // Par défaut pour les requêtes individuelles de Kodi
        let has_extension = path.extension().is_some();
        Ok(ProxyMetadata {
            is_dir: !has_extension,
            size: if has_extension { 1024 * 1024 * 500 } else { 0 },
        })
    }

    // LIST corrigé avec décodage Regex du format UNIX standard
    async fn list<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Vec<Fileinfo<PathBuf, Self::Metadata>>>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();

        if path == Path::new("") || path == Path::new("/") {
            return Ok(vec![]); 
        }

        let (phone_ip, target_path) = self.resolve_path(path).await?;
        let mut client = self.connect_to_phone(&phone_ip).await?;
        let path_str = target_path.to_string_lossy().into_owned();

        let remote_lines = client.list(Some(&path_str))
            .await
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("Erreur LIST : {}", e)))?;

        let mut file_infos = Vec::new();

        // Regex UNIX classique: capture les droits, le nombre de liens, l'owner, le group, la TAILLE, la date, et TOUT le reste de la ligne (nom avec espaces)
        // Exemple de ligne capturée : drwxr-xr-x  4 user group  4096 May 23 21:00 Mon Dossier Avec Espaces
        let file_regex = Regex::new(r#"^([drwx-]+)\s+\d+\s+\w+\s+\w+\s+(\d+)\s+\w+\s+\d+\s+[\d:]+\s+(.+)$"#).unwrap();

        for line in remote_lines {
            if let Some(caps) = file_regex.captures(&line) {
                let permissions = caps.get(1).unwrap().as_str();
                let size_str = caps.get(2).unwrap().as_str();
                let filename = caps.get(3).unwrap().as_str();

                if filename == "." || filename == ".." {
                    continue;
                }

                let is_dir = permissions.starts_with('d');
                let size = size_str.parse::<u64>().unwrap_or(0);

                let info = Fileinfo {
                    path: PathBuf::from(filename),
                    metadata: ProxyMetadata { is_dir, size },
                };
                file_infos.push(info);
            }
        }

        Ok(file_infos)
    }

    async fn cwd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send { Ok(()) }
    async fn rename<P>(&self, _user: &User, _from: P, _to: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send { Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule")) }
    async fn put<P, R>(&self, _user: &User, _input: R, _path: P, _start_pos: u64) -> unftp_core::storage::Result<u64> where P: AsRef<Path> + Send, R: tokio::io::AsyncRead + Send + Sync + Unpin { Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule")) }
    async fn del<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send { Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule")) }
    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send { Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule")) }
    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send { Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule")) }
}

// -------------------------------------------------------------------------
// 3. POINT D'ENTRÉE DU SERVEUR
// -------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let port = 2120;
    let addr = format!("0.0.0.0:{}", port);
    
    println!("Démarrage du Kodi FTP Proxy sur ftp://{}", addr);

    let server = ServerBuilder::new(Box::new(move || ProxyStorage::new()))
        .build()
        .unwrap();

    if let Err(e) = server.listen(&addr).await {
        eprintln!("Erreur critique du serveur : {}", e);
    }
}
