use async_trait::async_trait;
use libunftp::ServerBuilder;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::time::Duration;
use unftp_core::auth::UserDetail;
use unftp_core::storage::{Error, ErrorKind, Fileinfo, StorageBackend};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use regex::Regex;
use tokio::net::TcpStream;

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

    // Fonction de découverte automatique par balayage TCP asynchrone
    async fn discover_phones(&self) -> Vec<String> {
        let mut active_phones = Vec::new();

        // 1. Récupérer l'IP locale du Radxa
        if let Ok(local_ip) = local_ip_address::local_ip() {
            if let std::net::IpAddr::V4(ipv4) = local_ip {
                let octets = ipv4.octets();
                let mut tasks = Vec::new();

                // 2. Créer 254 requêtes de connexion simultanées (pour toute la plage 1 à 254)
                for i in 1..=254 {
                    // Ignorer notre propre IP
                    if i == octets[3] { continue; }

                    let target_ip = format!("{}.{}.{}.{}", octets[0], octets[1], octets[2], i);
                    
                    // On lance la vérification en arrière-plan (Green thread Tokio)
                    tasks.push(tokio::spawn(async move {
                        let addr = format!("{}:2121", target_ip);
                        // Timeout très court (150ms) suffisant sur un réseau Wi-Fi/Filaire local
                        match tokio::time::timeout(Duration::from_millis(150), TcpStream::connect(&addr)).await {
                            Ok(Ok(_)) => Some(target_ip), // Un serveur FTP écoute ici !
                            _ => None,
                        }
                    }));
                }

                // 3. Attendre que toutes les vérifications se terminent en même temps
                for task in tasks {
                    if let Ok(Some(ip)) = task.await {
                        active_phones.push(ip);
                    }
                }
            }
        }
        active_phones
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
    where P: AsRef<Path> + Send,
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
    where P: AsRef<Path> + Send,
    {
        let path = path.as_ref();
        
        // 1. Racine du proxy
        if path == Path::new("") || path == Path::new("/") {
            return Ok(ProxyMetadata { is_dir: true, size: 0 });
        }

        // 2. Racine d'un téléphone (ex: /10.42.0.226)
        let (phone_ip, target_path) = match self.resolve_path(path).await {
            Ok(res) => res,
            Err(_) => return Ok(ProxyMetadata { is_dir: true, size: 0 }),
        };

        if target_path == Path::new("") || target_path == Path::new("/") {
            return Ok(ProxyMetadata { is_dir: true, size: 0 });
        }

        // 3. Vrai fichier ou dossier sur le téléphone : on interroge le smartphone
        let mut client = self.connect_to_phone(&phone_ip).await?;
        let path_str = target_path.to_string_lossy().into_owned();

        // On tente de récupérer la vraie taille avec la commande SIZE
        match client.size(&path_str).await {
            Ok(exact_size) => {
                // Si SIZE fonctionne, c'est obligatoirement un fichier
                Ok(ProxyMetadata {
                    is_dir: false,
                    size: exact_size as u64,
                })
            }
            Err(_) => {
                // Si SIZE échoue, c'est très probablement un dossier (ou un fichier inexistant)
                // Dans le doute des requêtes complexes de Kodi, on valide si c'est un dossier
                Ok(ProxyMetadata {
                    is_dir: true,
                    size: 0,
                })
            }
        }
    }

    async fn list<P>(&self, _user: &User, path: P) -> unftp_core::storage::Result<Vec<Fileinfo<PathBuf, Self::Metadata>>>
    where P: AsRef<Path> + Send,
    {
        let path = path.as_ref();

        // -----------------------------------------------------------------
        // DYNAMISME À LA RACINE : Si l'utilisateur est sur /, on liste les téléphones vivants
        // -----------------------------------------------------------------
        if path == Path::new("") || path == Path::new("/") {
            let online_ips = self.discover_phones().await;
            let mut devices = Vec::new();
            
            for ip in online_ips {
                devices.push(Fileinfo {
                    path: PathBuf::from(ip),
                    metadata: ProxyMetadata { is_dir: true, size: 0 },
                });
            }
            return Ok(devices);
        }

        // --- Logique standard si on est déjà INSIDE un téléphone ---
        let (phone_ip, target_path) = self.resolve_path(path).await?;
        let mut client = self.connect_to_phone(&phone_ip).await?;
        let path_str = target_path.to_string_lossy().into_owned();

        let remote_lines = client.list(Some(&path_str))
            .await
            .map_err(|e| Error::new(ErrorKind::PermanentFileNotAvailable, format!("Erreur LIST : {}", e)))?;

        let mut file_infos = Vec::new();
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
    
    println!("Démarrage du Kodi FTP Proxy avec scan dynamique sur ftp://{}", addr);

    let server = ServerBuilder::new(Box::new(move || ProxyStorage::new()))
        .build()
        .unwrap();

    if let Err(e) = server.listen(&addr).await {
        eprintln!("Erreur critique du serveur : {}", e);
    }
}
