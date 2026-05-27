use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::fs;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};

use crate::HttpGatewayState;

const LIVE_DIR: &str = "/tmp/troozn-live";
const YTDLP_BIN: &str = "/usr/local/bin/yt-dlp";
const MAX_ITEMS: usize = 20;

const PUBLIC_HLS_URL: &str = "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8";

// Priorité 720p progressif H.264/AAC.
// 22 = 720p MP4 progressif quand disponible.
// 18 = MP4 progressif fallback, souvent 360p.
const YTDLP_720_FORMAT: &str =
    "22/best[ext=mp4][vcodec^=avc1][acodec^=mp4a][height<=720]/18";

#[derive(Debug)]
pub struct TrooznLive {
    pub root_dir: PathBuf,
    ffmpeg_child: Mutex<Option<Child>>,
    now: Mutex<TrooznLiveNow>,
    queue: Mutex<Vec<TrooznLiveItem>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TrooznLiveNow {
    pub state: String,
    pub title: String,
    pub source_url: String,
    pub hls_url: String,
    pub item_id: String,
    pub index: usize,
    pub position: u64,
    pub duration: Option<u64>,
    pub thumbnail: Option<String>,
    pub channel: Option<String>,
    pub started_at: u64,
    pub item_started_at: u64,
    pub next_title: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrooznLiveItem {
    pub item_id: String,
    pub index: usize,
    pub title: String,
    pub source_url: String,
    pub webpage_url: Option<String>,
    pub duration: Option<u64>,
    pub thumbnail: Option<String>,
    pub channel: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TrooznLiveSubmitRequest {
    pub url: String,
    pub title: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct TrooznLiveSubmitResponse {
    pub ok: bool,
    pub hls_url: String,
    pub live_dir: PathBuf,
    pub count: usize,
    pub queue: Vec<TrooznLiveItem>,
    pub now: TrooznLiveNow,
}

impl TrooznLive {
    pub fn new_default() -> Self {
        Self {
            root_dir: PathBuf::from(LIVE_DIR),
            ffmpeg_child: Mutex::new(None),
            now: Mutex::new(TrooznLiveNow {
                state: "idle".to_string(),
                hls_url: PUBLIC_HLS_URL.to_string(),
                ..Default::default()
            }),
            queue: Mutex::new(Vec::new()),
        }
    }

    async fn ensure_clean_dir(&self) -> anyhow::Result<()> {
        if self.root_dir.exists() {
            fs::remove_dir_all(&self.root_dir).await.ok();
        }

        fs::create_dir_all(&self.root_dir)
            .await
            .with_context(|| format!("création {}", self.root_dir.display()))?;

        Ok(())
    }

    async fn stop_current_ffmpeg(&self) {
        let mut guard = self.ffmpeg_child.lock().await;

        if let Some(child) = guard.as_mut() {
            child.start_kill().ok();
        }

        *guard = None;
    }

    pub async fn start_youtube_live_queue(
        self: std::sync::Arc<Self>,
        source_url: &str,
        title: Option<String>,
        limit: usize,
    ) -> anyhow::Result<TrooznLiveSubmitResponse> {
        self.stop_current_ffmpeg().await;
        self.ensure_clean_dir().await?;

        let limit = limit.clamp(1, MAX_ITEMS);
        let items = extract_youtube_items(source_url, limit).await?;

        if items.is_empty() {
            anyhow::bail!("Aucun item YouTube trouvé");
        }

        {
            let mut q = self.queue.lock().await;
            *q = items.clone();
        }

        let now = TrooznLiveNow {
            state: "starting".to_string(),
            title: title.unwrap_or_else(|| "Playlist Youtube".to_string()),
            source_url: source_url.to_string(),
            hls_url: PUBLIC_HLS_URL.to_string(),
            started_at: unix_timestamp(),
            ..Default::default()
        };

        {
            let mut guard = self.now.lock().await;
            *guard = now.clone();
        }

        let live = self.clone();
        let items_for_worker = items.clone();

        tokio::spawn(async move {
            if let Err(err) = live.run_hls_worker(items_for_worker).await {
                eprintln!("TROOZN_LIVE_WORKER_ERROR: {err:?}");

                let mut guard = live.now.lock().await;
                guard.state = "error".to_string();
                guard.last_error = Some(err.to_string());
            }
        });

        // Important :
        // /submit répond immédiatement après démarrage du worker.
        // default.py/Kodi attendra ensuite que index.m3u8 existe réellement.
        let now = self.current_now().await;

        Ok(TrooznLiveSubmitResponse {
            ok: true,
            hls_url: PUBLIC_HLS_URL.to_string(),
            live_dir: self.root_dir.clone(),
            count: items.len(),
            queue: items,
            now,
        })
    }

    async fn run_hls_worker(
        self: std::sync::Arc<Self>,
        items: Vec<TrooznLiveItem>,
    ) -> anyhow::Result<()> {
        let mut appended_any = false;
        let stream_started_at = unix_timestamp();

        for item in items.iter() {
            let next_title = items
                .iter()
                .find(|candidate| candidate.index > item.index)
                .map(|candidate| candidate.title.clone());

            {
                let mut guard = self.now.lock().await;
                guard.state = "preparing".to_string();
                guard.title = item.title.clone();
                guard.source_url = item.source_url.clone();
                guard.hls_url = PUBLIC_HLS_URL.to_string();
                guard.item_id = item.item_id.clone();
                guard.index = item.index;
                guard.position = 0;
                guard.duration = item.duration;
                guard.thumbnail = item.thumbnail.clone();
                guard.channel = item.channel.clone();
                guard.started_at = stream_started_at;
                guard.item_started_at = 0;
                guard.next_title = next_title.clone();
                guard.last_error = None;
            }

            eprintln!(
                "TROOZN_LIVE_RESOLVE_START index={} title={}",
                item.index, item.title
            );

            let play_url = match resolve_youtube_720_url(&item.source_url).await {
                Ok(url) => url,
                Err(err) => {
                    eprintln!(
                        "TROOZN_LIVE_SKIP_UNPLAYABLE index={} title={} error={err:?}",
                        item.index, item.title
                    );

                    let mut guard = self.now.lock().await;
                    guard.last_error = Some(format!(
                        "Item ignoré: {} - {}",
                        item.title,
                        err
                    ));

                    continue;
                }
            };

            let item_started_at = unix_timestamp();

            {
                let mut guard = self.now.lock().await;
                *guard = TrooznLiveNow {
                    state: "playing".to_string(),
                    title: item.title.clone(),
                    source_url: item.source_url.clone(),
                    hls_url: PUBLIC_HLS_URL.to_string(),
                    item_id: item.item_id.clone(),
                    index: item.index,
                    position: 0,
                    duration: item.duration,
                    thumbnail: item.thumbnail.clone(),
                    channel: item.channel.clone(),
                    started_at: stream_started_at,
                    item_started_at,
                    next_title,
                    last_error: None,
                };
            }

            let index_path = self.root_dir.join("index.m3u8");
            let segment_pattern = self.root_dir.join("seg-%05d.ts");
            let start_number = next_segment_number(&self.root_dir).await.unwrap_or(0);

            // Première vidéo : pas de DISCONTINUITY.
            // Vidéos suivantes : FFmpeg ajoute DISCONTINUITY au début de ce nouvel append.
            let hls_flags = if appended_any {
                "append_list+omit_endlist+program_date_time+discont_start".to_string()
            } else {
                "append_list+omit_endlist+program_date_time".to_string()
            };

            eprintln!(
                "TROOZN_LIVE_FFMPEG_START index={} title={} start_number={} flags={}",
                item.index, item.title, start_number, hls_flags
            );

            let mut cmd = Command::new("ffmpeg");

            cmd.args([
                "-hide_banner",
                "-y",
                "-re",
                "-i",
                &play_url,
                "-c",
                "copy",
                "-f",
                "hls",
                "-hls_time",
                "4",
                "-hls_list_size",
                "0",
                "-start_number",
                &start_number.to_string(),
                "-hls_flags",
                &hls_flags,
                "-hls_segment_filename",
            ]);

            cmd.arg(segment_pattern.to_string_lossy().to_string());
            cmd.arg(index_path.to_string_lossy().to_string());

            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::inherit());

            let child = cmd.spawn().context("lancement ffmpeg HLS live")?;

            {
                let mut guard = self.ffmpeg_child.lock().await;
                *guard = Some(child);
            }

            appended_any = true;

            loop {
                sleep(Duration::from_millis(500)).await;

                normalize_hls_event_playlist(&index_path).await.ok();

                {
                    let mut now = self.now.lock().await;
                    if now.item_id == item.item_id && now.item_started_at > 0 {
                        now.position = unix_timestamp().saturating_sub(now.item_started_at);
                    }
                }

                let finished = {
                    let mut guard = self.ffmpeg_child.lock().await;

                    match guard.as_mut() {
                        Some(child) => match child.try_wait() {
                            Ok(Some(status)) => {
                                eprintln!(
                                    "TROOZN_LIVE_FFMPEG_DONE index={} title={} status={status}",
                                    item.index, item.title
                                );
                                *guard = None;
                                true
                            }
                            Ok(None) => false,
                            Err(err) => {
                                eprintln!(
                                    "TROOZN_LIVE_FFMPEG_WAIT_ERROR index={} title={} error={err:?}",
                                    item.index, item.title
                                );
                                *guard = None;
                                true
                            }
                        },
                        None => true,
                    }
                };

                if finished {
                    normalize_hls_event_playlist(&index_path).await.ok();
                    break;
                }
            }
        }

        finalize_playlist(&self.root_dir.join("index.m3u8")).await.ok();

        {
            let mut guard = self.now.lock().await;
            guard.state = "ended".to_string();
        }

        Ok(())
    }

    pub async fn current_now(&self) -> TrooznLiveNow {
        let mut now = self.now.lock().await.clone();

        if now.item_started_at > 0 && now.state == "playing" {
            now.position = unix_timestamp().saturating_sub(now.item_started_at);
        }

        now
    }

    pub async fn current_queue(&self) -> Vec<TrooznLiveItem> {
        self.queue.lock().await.clone()
    }
}

async fn extract_youtube_items(source_url: &str, limit: usize) -> anyhow::Result<Vec<TrooznLiveItem>> {
    let mut cmd = Command::new(YTDLP_BIN);

    add_ytdlp_common_args(&mut cmd).await;

    cmd.args([
        "--flat-playlist",
        "--no-warnings",
        "--playlist-end",
        &limit.to_string(),
        "-J",
        source_url,
    ]);

    let output = timeout(Duration::from_secs(90), cmd.output())
        .await
        .context("timeout yt-dlp playlist")?
        .context("exécution yt-dlp playlist")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("yt-dlp playlist a échoué: {}", stderr.trim());
    }

    let root: Value = serde_json::from_slice(&output.stdout).context("parse yt-dlp JSON")?;

    let mut out = Vec::new();

    if let Some(entries) = root.get("entries").and_then(Value::as_array) {
        for (idx, entry) in entries.iter().take(limit).enumerate() {
            if let Some(item) = item_from_ytdlp_value(idx + 1, entry) {
                out.push(item);
            }
        }
    } else if let Some(item) = item_from_ytdlp_value(1, &root) {
        out.push(item);
    }

    Ok(out)
}

fn item_from_ytdlp_value(index: usize, v: &Value) -> Option<TrooznLiveItem> {
    let title = v
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Vidéo TROOZN")
        .to_string();

    let id = v
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| v.get("url").and_then(Value::as_str))
        .unwrap_or(&title)
        .to_string();

    let webpage_url = v
        .get("webpage_url")
        .and_then(Value::as_str)
        .map(str::to_string);

    let source_url = if let Some(url) = webpage_url.clone() {
        url
    } else if id.starts_with("http://") || id.starts_with("https://") {
        id.clone()
    } else {
        format!("https://www.youtube.com/watch?v={id}")
    };

    let duration = v.get("duration").and_then(Value::as_u64);

    let thumbnail = v
        .get("thumbnail")
        .and_then(Value::as_str)
        .map(str::to_string);

    let channel = v
        .get("channel")
        .and_then(Value::as_str)
        .or_else(|| v.get("uploader").and_then(Value::as_str))
        .map(str::to_string);

    Some(TrooznLiveItem {
        item_id: item_id_for_url(&source_url),
        index,
        title,
        source_url,
        webpage_url,
        duration,
        thumbnail,
        channel,
    })
}

async fn resolve_youtube_720_url(source_url: &str) -> anyhow::Result<String> {
    let mut last_error = String::new();

    for attempt in 1..=3 {
        let mut cmd = Command::new(YTDLP_BIN);

        add_ytdlp_common_args(&mut cmd).await;

        cmd.args([
            "--no-playlist",
            "--no-warnings",
            "--force-ipv4",
            "--socket-timeout",
            "20",
            "--retries",
            "3",
            "--fragment-retries",
            "3",
            "-f",
            YTDLP_720_FORMAT,
            "-g",
            source_url,
        ]);

        let output = match timeout(Duration::from_secs(90), cmd.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(err)) => {
                last_error = format!("exécution yt-dlp: {err}");
                sleep(Duration::from_millis(600 * attempt)).await;
                continue;
            }
            Err(_) => {
                last_error = "timeout yt-dlp".to_string();
                sleep(Duration::from_millis(600 * attempt)).await;
                continue;
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            last_error = stderr.trim().to_string();
            sleep(Duration::from_millis(600 * attempt)).await;
            continue;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        if let Some(url) = stdout
            .lines()
            .map(str::trim)
            .find(|line| line.starts_with("http://") || line.starts_with("https://"))
        {
            return Ok(url.to_string());
        }

        last_error = "yt-dlp n'a retourné aucune URL jouable".to_string();
        sleep(Duration::from_millis(600 * attempt)).await;
    }

    anyhow::bail!("yt-dlp a échoué après retries: {last_error}");
}

async fn add_ytdlp_common_args(cmd: &mut Command) {
    if Path::new("/home/troozn/.deno/bin/deno").exists() {
        cmd.args([
            "--js-runtimes",
            "deno:/home/troozn/.deno/bin/deno",
            "--remote-components",
            "ejs:github",
        ]);
    }
}

async fn next_segment_number(root_dir: &Path) -> anyhow::Result<u64> {
    let mut max_seen: Option<u64> = None;

    let mut rd = match fs::read_dir(root_dir).await {
        Ok(rd) => rd,
        Err(_) => return Ok(0),
    };

    while let Some(entry) = rd.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();

        if !name.starts_with("seg-") || !name.ends_with(".ts") {
            continue;
        }

        let number_text = name
            .trim_start_matches("seg-")
            .trim_end_matches(".ts");

        if let Ok(n) = number_text.parse::<u64>() {
            max_seen = Some(max_seen.map_or(n, |m| m.max(n)));
        }
    }

    Ok(max_seen.map_or(0, |n| n + 1))
}

async fn normalize_hls_event_playlist(index_path: &Path) -> anyhow::Result<()> {
    let content = match fs::read_to_string(index_path).await {
        Ok(content) => content,
        Err(_) => return Ok(()),
    };

    if !content.contains("#EXTM3U") {
        return Ok(());
    }

    let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();

    // Ajoute PLAYLIST-TYPE:EVENT juste après EXT-X-VERSION.
    if !lines.iter().any(|l| l.starts_with("#EXT-X-PLAYLIST-TYPE:")) {
        if let Some(pos) = lines.iter().position(|l| l.starts_with("#EXT-X-VERSION:")) {
            lines.insert(pos + 1, "#EXT-X-PLAYLIST-TYPE:EVENT".to_string());
        }
    }

    // Supprime une ou plusieurs discontinuités accidentelles avant le premier segment.
    // Cas typique à éviter :
    // #EXT-X-MEDIA-SEQUENCE:0
    // #EXT-X-DISCONTINUITY
    // #EXTINF:...
    loop {
        let first_extinf = lines.iter().position(|l| l.starts_with("#EXTINF:"));
        let first_discontinuity = lines
            .iter()
            .position(|l| l.trim() == "#EXT-X-DISCONTINUITY");

        match (first_discontinuity, first_extinf) {
            (Some(d), Some(e)) if d < e => {
                lines.remove(d);
            }
            _ => break,
        }
    }

    let updated = lines.join("\n") + "\n";

    if updated != content {
        fs::write(index_path, updated).await?;
    }

    Ok(())
}

async fn finalize_playlist(index_path: &Path) -> anyhow::Result<()> {
    normalize_hls_event_playlist(index_path).await.ok();

    let content = match fs::read_to_string(index_path).await {
        Ok(content) => content,
        Err(_) => return Ok(()),
    };

    if content.contains("#EXT-X-ENDLIST") {
        return Ok(());
    }

    let mut updated = content;

    if !updated.ends_with('\n') {
        updated.push('\n');
    }

    updated.push_str("#EXT-X-ENDLIST\n");
    fs::write(index_path, updated).await?;
    Ok(())
}

fn item_id_for_url(url: &str) -> String {
    let digest = sha1::Sha1::from(url).digest().to_string();
    digest.chars().take(16).collect()
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub async fn troozn_live_health() -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "service": "troozn-live",
        "mode": "hls",
        "quality": "720p-copy",
        "hls_url": PUBLIC_HLS_URL
    }))
}

pub async fn troozn_live_submit(
    State(state): State<HttpGatewayState>,
    Json(req): Json<TrooznLiveSubmitRequest>,
) -> Response {
    let live = state.live.clone();

    match live
        .start_youtube_live_queue(&req.url, req.title, req.limit.unwrap_or(MAX_ITEMS))
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => {
            eprintln!("TROOZN_LIVE_SUBMIT_ERROR: {err:?}");

            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "ok": false,
                    "error": err.to_string()
                })),
            )
                .into_response()
        }
    }
}

pub async fn troozn_live_now(State(state): State<HttpGatewayState>) -> impl IntoResponse {
    Json(state.live.current_now().await)
}

pub async fn troozn_live_queue(State(state): State<HttpGatewayState>) -> impl IntoResponse {
    Json(json!({
        "items": state.live.current_queue().await
    }))
}

pub async fn troozn_live_file(
    State(state): State<HttpGatewayState>,
    AxumPath(path): AxumPath<String>,
) -> Response {
    let requested = path.trim_start_matches('/');

    let relative = match requested {
        "" => "index.m3u8",
        "index.m3u8" => "index.m3u8",
        "playlist-youtube.m3u8" => "index.m3u8",
        other => other,
    };

    if relative.contains("..") || relative.starts_with('/') {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let file_path = state.live.root_dir.join(relative);

    let data = match fs::read(&file_path).await {
        Ok(data) => data,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                format!("not found: {}", file_path.display()),
            )
                .into_response();
        }
    };

    let content_type = if relative.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else if relative.ends_with(".ts") {
        "video/mp2t"
    } else {
        "application/octet-stream"
    };

    let mut response = Response::new(Body::from(data));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));

    response
}
