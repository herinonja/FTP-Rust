use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock};

use crate::HttpGatewayState;

const DEFAULT_LIMIT: usize = 20;
const MAX_LIMIT: usize = 100;

// Important : on privilégie les flux MP4 progressifs HTTPS.
// Ça évite autant que possible de donner à Kodi un manifest HLS index.m3u8.
// Fallback HLS conservé si YouTube ne propose pas de MP4 progressif compatible.
const YTDLP_FAST_FORMAT: &str =
    "best[height<=720][ext=mp4][vcodec^=avc1][acodec^=mp4a][protocol^=https]/\
     best[height<=720][ext=mp4][protocol^=https]/\
     best[ext=mp4][protocol^=https]/\
     best[height<=720]/\
     best";

// Durée courte : les URLs YouTube expirent.
// 15 minutes suffit pour absorber les HEAD/GET/probes de Kodi sans garder une URL trop vieille.
const RESOLVED_URL_CACHE_SECONDS: u64 = 15 * 60;

#[derive(Debug, Clone)]
pub struct YoutubeLibrary {
    pub db_path: PathBuf,
    pub library_dir: PathBuf,
    pub current_dir: PathBuf,
    pub public_base_url: String,
    pub ytdlp_bin: PathBuf,

    // Cache mémoire : item_id -> URL finale déjà résolue par yt-dlp -g.
    // Évite que Kodi déclenche plusieurs yt-dlp pour HEAD/GET/probe/retry.
    resolved_cache: Arc<RwLock<HashMap<String, ResolvedUrlCache>>>,

    // Verrou par item : si plusieurs requêtes arrivent en même temps,
    // un seul yt-dlp tourne pour cet item.
    resolve_locks: Arc<RwLock<HashMap<String, Arc<Mutex<()>>>>>,
}

#[derive(Debug, Clone)]
struct ResolvedUrlCache {
    url: String,
    expires_at: u64,
}

#[derive(Debug, Deserialize)]
pub struct YoutubeSubmitRequest {
    pub url: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YoutubeItem {
    pub item_id: String,
    pub index: usize,
    pub title: String,
    pub source_url: String,
    pub webpage_url: String,
    pub thumbnail: Option<String>,
    pub local_thumb: Option<String>,
    pub duration: Option<u64>,
    pub channel: Option<String>,
    pub uploader: Option<String>,
    pub upload_date: Option<String>,
    pub plot: Option<String>,
    pub strm_path: PathBuf,
    pub nfo_path: PathBuf,
    pub thumb_path: Option<PathBuf>,
    pub play_url: String,
    pub created_at: u64,
}

#[derive(Debug, Serialize)]
pub struct YoutubeSubmitResponse {
    pub ok: bool,
    pub count: usize,
    pub library_dir: PathBuf,
    pub playlist_path: PathBuf,
    pub items: Vec<YoutubeItem>,
}

#[derive(Debug, Clone)]
struct VideoMetadata {
    title: String,
    source_url: String,
    webpage_url: String,
    thumbnail: Option<String>,
    duration: Option<u64>,
    channel: Option<String>,
    uploader: Option<String>,
    upload_date: Option<String>,
    plot: Option<String>,
}

impl YoutubeLibrary {
    pub fn new_default(http_bind: &str) -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());

        let public_base_url = if http_bind.starts_with("http://") || http_bind.starts_with("https://") {
            http_bind.trim_end_matches('/').to_string()
        } else {
            format!("http://{http_bind}")
        };

        let state_dir = PathBuf::from("/tmp/troozn-youtube");
        let library_dir = PathBuf::from(home).join(".kodi/userdata/TROOZN");
        let current_dir = library_dir.join("current");

        Self {
            db_path: state_dir.join("items.json"),
            library_dir,
            current_dir,
            public_base_url,
            ytdlp_bin: PathBuf::from(
                std::env::var("TROOZN_YTDLP").unwrap_or_else(|_| "/usr/local/bin/yt-dlp".to_string()),
            ),
            resolved_cache: Arc::new(RwLock::new(HashMap::new())),
            resolve_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn ensure_dirs(&self) -> anyhow::Result<()> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        fs::create_dir_all(&self.current_dir).await?;
        Ok(())
    }

    async fn clear_current_library(&self) -> anyhow::Result<()> {
        self.ensure_dirs().await?;

        if fs::try_exists(&self.current_dir).await.unwrap_or(false) {
            fs::remove_dir_all(&self.current_dir).await.ok();
        }

        fs::create_dir_all(&self.current_dir).await?;

        // Quand on génère une nouvelle queue, les anciens item_id ne doivent plus polluer le cache.
        self.resolved_cache.write().await.clear();

        Ok(())
    }

    async fn load_db(&self) -> HashMap<String, YoutubeItem> {
        match fs::read_to_string(&self.db_path).await {
            Ok(text) => serde_json::from_str::<HashMap<String, YoutubeItem>>(&text).unwrap_or_default(),
            Err(_) => HashMap::new(),
        }
    }

    async fn save_db(&self, db: &HashMap<String, YoutubeItem>) -> anyhow::Result<()> {
        self.ensure_dirs().await?;

        let tmp = self.db_path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(db)?;

        fs::write(&tmp, text).await?;
        fs::rename(tmp, &self.db_path).await?;

        Ok(())
    }

    async fn ytdlp_json(&self, url: &str, playlist: bool, limit: usize) -> anyhow::Result<Value> {
        let mut cmd = Command::new(&self.ytdlp_bin);

        if let Some(deno_args) = deno_args_if_available().await {
            cmd.args(deno_args);
        }

        if playlist {
            let limit_text = limit.to_string();
            cmd.args([
                "--flat-playlist",
                "--no-warnings",
                "--playlist-end",
                limit_text.as_str(),
                "-J",
                url,
            ]);
        } else {
            cmd.args(["--no-playlist", "--no-warnings", "-J", url]);
        }

        let output = tokio::time::timeout(Duration::from_secs(120), cmd.output()).await??;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("yt-dlp JSON failed: {stderr}");
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let value = serde_json::from_str::<Value>(&stdout)?;

        Ok(value)
    }

    async fn ytdlp_play_url(&self, source_url: &str) -> anyhow::Result<String> {
        let mut cmd = Command::new(&self.ytdlp_bin);

        if let Some(deno_args) = deno_args_if_available().await {
            cmd.args(deno_args);
        }

        cmd.args([
            "--no-playlist",
            "--no-warnings",
            "-f",
            YTDLP_FAST_FORMAT,
            "-g",
            source_url,
        ]);

        let output = tokio::time::timeout(Duration::from_secs(90), cmd.output()).await??;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("yt-dlp play URL failed: {stderr}");
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        let urls = stdout
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("http://") || line.starts_with("https://"))
            .map(str::to_string)
            .collect::<Vec<_>>();

        let Some(first_url) = urls.first() else {
            anyhow::bail!("yt-dlp returned no playable URL");
        };

        Ok(first_url.clone())
    }

    pub async fn submit(&self, url: String, limit: usize) -> anyhow::Result<YoutubeSubmitResponse> {
        self.clear_current_library().await?;

        let limit = limit.clamp(1, MAX_LIMIT);
        let mut db = self.load_db().await;

        let mut metadata_list = Vec::new();

        if is_probably_playlist_url(&url) {
            match self.ytdlp_json(&url, true, limit).await {
                Ok(value) => {
                    if let Some(entries) = value.get("entries").and_then(Value::as_array) {
                        for entry in entries {
                            if let Some(meta) = metadata_from_flat_entry(entry) {
                                metadata_list.push(meta);
                            }
                        }
                    }
                }
                Err(err) => {
                    eprintln!("yt-dlp flat playlist failed: {err:?}");

                    if let Some(seed_url) = youtube_mix_seed_url(&url) {
                        let value = self.ytdlp_json(&seed_url, false, limit).await?;
                        metadata_list.push(metadata_from_video_info(&value, &seed_url));
                    } else {
                        return Err(err);
                    }
                }
            }
        } else {
            let value = self.ytdlp_json(&url, false, limit).await?;
            metadata_list.push(metadata_from_video_info(&value, &url));
        }

        let mut items = Vec::new();

        for (idx, meta) in metadata_list.into_iter().enumerate() {
            let item = self.create_library_item(idx + 1, meta).await?;
            db.insert(item.item_id.clone(), item.clone());
            items.push(item);
        }

        let playlist_path = self.write_playlist(&items).await?;
        self.save_db(&db).await?;

        Ok(YoutubeSubmitResponse {
            ok: true,
            count: items.len(),
            library_dir: self.current_dir.clone(),
            playlist_path,
            items,
        })
    }

    async fn create_library_item(&self, index: usize, meta: VideoMetadata) -> anyhow::Result<YoutubeItem> {
        let item_id = item_id_for_url(&meta.source_url);
        let safe_title = sanitize_filename(&meta.title, &format!("video-{index:03}"));
        let base = format!("{index:03} - {safe_title}");

        let strm_path = self.current_dir.join(format!("{base}.strm"));
        let nfo_path = self.current_dir.join(format!("{base}.nfo"));
        let thumb_path = meta.thumbnail.as_ref().map(|_| self.current_dir.join(format!("{base}-thumb.jpg")));

        let play_url = format!("{}/youtube/item/{}/play", self.public_base_url, item_id);

        let item = YoutubeItem {
            item_id,
            index,
            title: meta.title,
            source_url: meta.source_url,
            webpage_url: meta.webpage_url,
            thumbnail: meta.thumbnail,
            local_thumb: thumb_path.as_ref().map(|p| p.to_string_lossy().to_string()),
            duration: meta.duration,
            channel: meta.channel,
            uploader: meta.uploader,
            upload_date: meta.upload_date,
            plot: meta.plot,
            strm_path,
            nfo_path,
            thumb_path,
            play_url,
            created_at: unix_timestamp(),
        };

        fs::write(&item.strm_path, format!("{}\n", item.play_url)).await?;
        fs::write(&item.nfo_path, movie_nfo_xml(&item)).await?;

        if let (Some(remote), Some(local)) = (&item.thumbnail, &item.thumb_path) {
            download_thumbnail(remote, local).await.ok();
        }

        Ok(item)
    }

    async fn write_playlist(&self, items: &[YoutubeItem]) -> anyhow::Result<PathBuf> {
        let playlist_path = self.current_dir.join("playlist.m3u8");

        let mut text = String::from("#EXTM3U\n");

        for item in items {
            let duration = item.duration.unwrap_or(0);
            text.push_str(&format!("#EXTINF:{duration},{}\n", item.title));
            text.push_str(&format!("{}\n", item.strm_path.to_string_lossy()));
        }

        fs::write(&playlist_path, text).await?;
        Ok(playlist_path)
    }

    pub async fn play_redirect(&self, item_id: &str) -> anyhow::Result<String> {
        let now = unix_timestamp();

        {
            let cache = self.resolved_cache.read().await;
            if let Some(entry) = cache.get(item_id) {
                if entry.expires_at > now {
                    eprintln!("youtube cache hit item={item_id}");
                    return Ok(entry.url.clone());
                }
            }
        }

        let lock = self.resolve_lock_for(item_id).await;
        let _guard = lock.lock().await;

        // Double check après acquisition du verrou.
        {
            let cache = self.resolved_cache.read().await;
            if let Some(entry) = cache.get(item_id) {
                if entry.expires_at > unix_timestamp() {
                    eprintln!("youtube cache hit after lock item={item_id}");
                    return Ok(entry.url.clone());
                }
            }
        }

        let db = self.load_db().await;

        let item = db
            .get(item_id)
            .ok_or_else(|| anyhow::anyhow!("unknown YouTube item: {item_id}"))?;

        eprintln!("youtube resolving via yt-dlp item={item_id} title={}", item.title);

        let url = self.ytdlp_play_url(&item.source_url).await?;

        {
            let mut cache = self.resolved_cache.write().await;
            cache.insert(
                item_id.to_string(),
                ResolvedUrlCache {
                    url: url.clone(),
                    expires_at: unix_timestamp() + RESOLVED_URL_CACHE_SECONDS,
                },
            );
        }

        Ok(url)
    }

    async fn resolve_lock_for(&self, item_id: &str) -> Arc<Mutex<()>> {
        {
            let locks = self.resolve_locks.read().await;
            if let Some(lock) = locks.get(item_id) {
                return lock.clone();
            }
        }

        let mut locks = self.resolve_locks.write().await;
        locks
            .entry(item_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub async fn items(&self) -> HashMap<String, YoutubeItem> {
        self.load_db().await
    }
}

pub async fn youtube_health(State(state): State<HttpGatewayState>) -> Response {
    let response = json!({
        "ok": true,
        "library_dir": state.youtube.current_dir,
        "db_path": state.youtube.db_path,
        "base_url": state.youtube.public_base_url,
    });

    Json(response).into_response()
}

pub async fn youtube_submit(
    State(state): State<HttpGatewayState>,
    Json(payload): Json<YoutubeSubmitRequest>,
) -> Response {
    if payload.url.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": "missing url"
            })),
        )
            .into_response();
    }

    let limit = payload.limit.unwrap_or(DEFAULT_LIMIT);

    match state.youtube.submit(payload.url, limit).await {
        Ok(result) => Json(result).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "ok": false,
                "error": err.to_string()
            })),
        )
            .into_response(),
    }
}

pub async fn youtube_play(
    State(state): State<HttpGatewayState>,
    AxumPath(item_id): AxumPath<String>,
) -> Response {
    match state.youtube.play_redirect(&item_id).await {
        Ok(target) => {
            let mut response = StatusCode::FOUND.into_response();

            let Ok(location) = HeaderValue::from_str(&target) else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "ok": false,
                        "error": "invalid redirect URL"
                    })),
                )
                    .into_response();
            };

            response.headers_mut().insert(header::LOCATION, location);
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
            );

            response
        }
        Err(err) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "ok": false,
                "error": err.to_string()
            })),
        )
            .into_response(),
    }
}

pub async fn youtube_items(State(state): State<HttpGatewayState>) -> Response {
    Json(state.youtube.items().await).into_response()
}

async fn deno_args_if_available() -> Option<Vec<String>> {
    let deno = PathBuf::from("/home/troozn/.deno/bin/deno");

    if fs::try_exists(&deno).await.unwrap_or(false) {
        Some(vec![
            "--js-runtimes".to_string(),
            format!("deno:{}", deno.to_string_lossy()),
            "--remote-components".to_string(),
            "ejs:github".to_string(),
        ])
    } else {
        None
    }
}

async fn download_thumbnail(url: &str, path: &Path) -> anyhow::Result<()> {
    let response = reqwest::get(url).await?;

    if !response.status().is_success() {
        anyhow::bail!("thumbnail HTTP status {}", response.status());
    }

    let bytes = response.bytes().await?;
    fs::write(path, bytes).await?;

    Ok(())
}

fn metadata_from_video_info(value: &Value, fallback_url: &str) -> VideoMetadata {
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Vidéo YouTube")
        .to_string();

    let uploader = value
        .get("uploader")
        .or_else(|| value.get("channel"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| uploader.clone());

    let webpage_url = value
        .get("webpage_url")
        .and_then(Value::as_str)
        .unwrap_or(fallback_url)
        .to_string();

    VideoMetadata {
        title,
        source_url: webpage_url.clone(),
        webpage_url,
        thumbnail: value.get("thumbnail").and_then(Value::as_str).map(str::to_string),
        duration: value.get("duration").and_then(Value::as_u64),
        channel,
        uploader,
        upload_date: value
            .get("upload_date")
            .and_then(Value::as_str)
            .and_then(parse_yt_date),
        plot: value
            .get("description")
            .and_then(Value::as_str)
            .map(|s| compact_text(s, 1200)),
    }
}

fn metadata_from_flat_entry(value: &Value) -> Option<VideoMetadata> {
    let raw_url = value
        .get("webpage_url")
        .or_else(|| value.get("url"))
        .or_else(|| value.get("original_url"))
        .and_then(Value::as_str)?;

    let source_url = if raw_url.starts_with("http://") || raw_url.starts_with("https://") {
        raw_url.to_string()
    } else {
        youtube_watch_url(raw_url)
    };

    let title = value
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Vidéo YouTube")
        .to_string();

    let uploader = value
        .get("uploader")
        .and_then(Value::as_str)
        .map(str::to_string);

    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| uploader.clone());

    Some(VideoMetadata {
        title,
        source_url: source_url.clone(),
        webpage_url: source_url,
        thumbnail: value.get("thumbnail").and_then(Value::as_str).map(str::to_string),
        duration: value.get("duration").and_then(Value::as_u64),
        channel,
        uploader,
        upload_date: None,
        plot: None,
    })
}

fn is_probably_playlist_url(url: &str) -> bool {
    url.contains("/playlist") || url.contains("list=")
}

fn youtube_watch_url(video_id: &str) -> String {
    format!("https://www.youtube.com/watch?v={video_id}")
}

fn youtube_mix_seed_url(url: &str) -> Option<String> {
    let list_marker = "list=RD";
    let index = url.find(list_marker)?;
    let start = index + list_marker.len();

    let rest = &url[start..];
    let seed = rest
        .split(['&', '#'])
        .next()
        .unwrap_or("")
        .trim();

    if seed.len() >= 8 {
        Some(youtube_watch_url(seed))
    } else {
        None
    }
}

fn item_id_for_url(url: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize();
    hex_lower(&digest[..8])
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sanitize_filename(name: &str, fallback: &str) -> String {
    let mut output = String::new();

    for ch in name.chars() {
        if ch.is_alphanumeric()
            || ch == ' '
            || ch == '.'
            || ch == '-'
            || ch == '_'
            || ch == '('
            || ch == ')'
            || ch == '['
            || ch == ']'
            || ch == '&'
            || ch == '\''
            || ch == ','
        {
            output.push(ch);
        }
    }

    let output = output.split_whitespace().collect::<Vec<_>>().join(" ");

    if output.is_empty() {
        fallback.to_string()
    } else {
        output.chars().take(140).collect()
    }
}

fn movie_nfo_xml(item: &YoutubeItem) -> String {
    let title = xml_escape(&item.title);
    let plot = xml_escape(item.plot.as_deref().unwrap_or(""));
    let studio = xml_escape(
        item.channel
            .as_deref()
            .or(item.uploader.as_deref())
            .unwrap_or(""),
    );
    let thumb = xml_escape(item.local_thumb.as_deref().unwrap_or(""));
    let premiered = xml_escape(item.upload_date.as_deref().unwrap_or(""));
    let source_url = xml_escape(&item.source_url);
    let runtime = item.duration.unwrap_or(0) / 60;

    format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<movie>
  <title>{title}</title>
  <originaltitle>{title}</originaltitle>
  <plot>{plot}</plot>
  <runtime>{runtime}</runtime>
  <studio>{studio}</studio>
  <premiered>{premiered}</premiered>
  <thumb>{thumb}</thumb>
  <uniqueid type="troozn" default="true">{}</uniqueid>
  <trailer>{source_url}</trailer>
</movie>
"#,
        xml_escape(&item.item_id)
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn compact_text(value: &str, max_chars: usize) -> String {
    let mut text = value.replace("\r\n", "\n").replace('\r', "\n");

    while text.contains("\n\n\n") {
        text = text.replace("\n\n\n", "\n\n");
    }

    let text = text.trim();

    if text.chars().count() <= max_chars {
        text.to_string()
    } else {
        text.chars().take(max_chars).collect::<String>() + "…"
    }
}

fn parse_yt_date(value: &str) -> Option<String> {
    if value.len() == 8 && value.chars().all(|c| c.is_ascii_digit()) {
        Some(format!("{}-{}-{}", &value[0..4], &value[4..6], &value[6..8]))
    } else {
        None
    }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
