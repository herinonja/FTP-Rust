use async_trait::async_trait;
use libunftp::ServerBuilder;
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

    // Découpe le chemin pour extraire l'IP du téléphone et le chemin local sur le téléphone
    async fn resolve_path(&self, path: &Path) -> unftp_core::storage::Result<(String, PathBuf)> {
        let mut components = path.components();
        
        // On passe la racine absolue si elle est présente
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

    // Connexion FTP à la volée vers le smartphone cible
    async fn connect_to_phone(&self, ip: &str) -> unftp_core::storage::Result<suppaftp::AsyncFtpStream> {
        let addr = format!("{}:2121", ip); 
        
        let mut ftp_stream = suppaftp::AsyncFtpStream::connect(addr)
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionClosed, format!("Connexion échouée au téléphone {}: {}", ip, e)))?;
        
        ftp_stream.login("anonymous", "anonymous")
            .await
            .map_err(|e| Error::new(ErrorKind::LocalError, format!("Refus d'authentification anonyme: {}", e)))?;

        Ok(ftp_stream)
    }
}

// Structure réelle pour transporter les métadonnées des fichiers du téléphone
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
// 2. IMPLÉMENTATION DES COMMANDES FTP INTERCEPTÉES
// -------------------------------------------------------------------------

#[async_trait]
impl<User: UserDetail + Send + Sync + Debug> StorageBackend<User> for ProxyStorage {
    type Metadata = ProxyMetadata;

    fn name(&self) -> &str {
        "KodiFtpProxy"
    }

    // Téléchargement d'un fichier avec support du Seek
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
                .map_err(|e| Error::new(ErrorKind::LocalError, format!("Erreur Seek (REST) : {}", e)))?;
        }

        let data_stream = client.retr_as_stream(&path_str)
            .await
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("Erreur RETR: {}", e)))?;

        Ok(Box::new(data_stream.compat()))
    }

    // Détermine si un élément demandé est un dossier ou un fichier
    async fn metadata<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Self::Metadata>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        
        // Si on interroge la racine brute (/) c'est un dossier virtuel
        if path == Path::new("") || path == Path::new("/") {
            return Ok(ProxyMetadata { is_dir: true, size: 0 });
        }

        let (phone_ip, target_path) = match self.resolve_path(path).await {
            Ok(res) => res,
            Err(_) => return Ok(ProxyMetadata { is_dir: true, size: 0 }),
        };

        // Si le chemin ne contient que l'IP (ex: /192.168.1.50), c'est le dossier racine du téléphone
        if target_path == Path::new("") || target_path == Path::new("/") {
            return Ok(ProxyMetadata { is_dir: true, size: 0 });
        }

        // Sinon, on applique une heuristique rapide basée sur l'extension pour éviter des requêtes réseaux lourdes
        let has_extension = path.extension().is_some();
        Ok(ProxyMetadata {
            is_dir: !has_extension,
            size: if has_extension { 1024 * 1024 * 100 } else { 0 }, // Taille fictive pour les fichiers
        })
    }

    // Liste le contenu d'un répertoire à la demande de FileZilla ou Kodi
    async fn list<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Vec<Fileinfo<PathBuf, Self::Metadata>>>
    where
        P: AsRef<Path> + Send,
    {
        let path = path.as_ref();

        // Sécurité pour la racine vide
        if path == Path::new("") || path == Path::new("/") {
            return Ok(vec![]); 
        }

        let (phone_ip, target_path) = self.resolve_path(path).await?;
        let mut client = self.connect_to_phone(&phone_ip).await?;
        let path_str = target_path.to_string_lossy().into_owned();

        // On récupère la liste brute des fichiers (format texte 'ls' standard du FTP)
        let remote_lines = client.list(Some(&path_str))
            .await
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("Erreur LIST depuis le téléphone: {}", e)))?;

        let mut file_infos = Vec::new();

        for line in remote_lines {
            // Un listing FTP classique ressemble à : "-rw-r--r-- 1 user group 12345 Jan 1 2026 video.mp4"
            // On extrait le dernier élément qui correspond au nom du fichier/dossier
            if let Some(filename) = line.split_whitespace().last() {
                if filename == "." || filename == ".." {
                    continue;
                }

                // Si la ligne commence par 'd', c'est un répertoire (norme Unix classique)
                let is_dir = line.starts_with('d');

                let info = Fileinfo {
                    path: PathBuf::from(filename),
                    metadata: ProxyMetadata { is_dir, size: 0 },
                };
                file_infos.push(info);
            }
        }

        Ok(file_infos)
    }

    async fn cwd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()>
    where P: AsRef<Path> + Send {
        Ok(()) // Autorise FileZilla à changer de répertoire virtuellement
    }

    // -- METHODES EN ECRITURE BLOQUEES --
    async fn rename<P>(&self, _user: &User, _from: P, _to: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }
    async fn put<P, R>(&self, _user: &User, _input: R, _path: P, _start_pos: u64) -> unftp_core::storage::Result<u64> where P: AsRef<Path> + Send, R: tokio::io::AsyncRead + Send + Sync + Unpin {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }
    async fn del<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }
    async fn mkd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
    }
    async fn rmd<P>(&self, _user: &User, _path: P) -> unftp_core::storage::Result<()> where P: AsRef<Path> + Send {
        Err(Error::new(ErrorKind::PermissionDenied, "Lecture seule"))
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

    let server = ServerBuilder::new(Box::new(move || ProxyStorage::new()))
        .build()
        .unwrap();

    if let Err(e) = server.listen(&addr).await {
        eprintln!("Erreur critique du serveur : {}", e);
    }
}
