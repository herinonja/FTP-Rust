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
use sha1::{Digest, Sha1};

const LIVE_DIR: &str = "/tmp/troozn-live";
const YTDLP_BIN: &str = "/usr/local/bin/yt-dlp";
const MAX_ITEMS: usize = 20;

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
                hls_url: "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8".to_string(),
                ..Default::default()
            }),
            queue: Mutex::new(Vec::new()),
        }
    }

    async fn ensure_clean_dir(&self) -> anyhow::Result<()> {
        if self.root_dir.exists() {
            let _ = fs::remove_dir_all(&self.root_dir).await;
        }

        fs::create_dir_all(&self.root_dir)
            .await
            .with_context(|| format!("création {}", self.root_dir.display()))?;

        Ok(())
    }

    async fn stop_current_ffmpeg(&self) {
        let mut guard = self.ffmpeg_child.lock().await;

        if let Some(child) = guard.as_mut() {
            let _ = child.start_kill();
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
            hls_url: "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8".to_string(),
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
            if let Err(err) = live.clone().run_hls_worker(items_for_worker).await {
                eprintln!("TROOZN_LIVE_WORKER_ERROR: {err:?}");

                let mut guard = live.now.lock().await;
                guard.state = "error".to_string();
                guard.last_error = Some(err.to_string());
            }
        });

        // Ne pas attendre ici que le premier segment HLS soit prêt.
        // /submit doit répondre rapidement; default.py attendra ensuite le manifeste.
        let now = self.current_now().await;

        Ok(TrooznLiveSubmitResponse {
            ok: true,
            hls_url: "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8".to_string(),
            live_dir: self.root_dir.clone(),
            count: items.len(),
            queue: items,
            now,
        })
    }

    async fn run_hls_worker(self: std::sync::Arc<Self>, items: Vec<TrooznLiveItem>) -> anyhow::Result<()> {
        let mut appended_any = false;
        let stream_started_at = unix_timestamp();

        for item in items.iter() {
            let next_title = items
                .iter()
                .find(|candidate| candidate.index > item.index)
                .map(|candidate| candidate.title.clone());

            let play_url = match resolve_youtube_720_url(&item.source_url).await {
                Ok(url) => url,
                Err(err) => {
                    eprintln!(
                        "TROOZN_LIVE_SKIP_UNPLAYABLE index={} title={} error={err:?}",
                        item.index, item.title
                    );
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
                    hls_url: "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8".to_string(),
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

            let start_number = count_ts_segments(&self.root_dir).await.unwrap_or(0);
            let index_path = self.root_dir.join("index.m3u8");
            let segment_pattern = self.root_dir.join("seg-%05d.ts");

            let mut hls_flags = "append_list+omit_endlist+program_date_time".to_string();

            if appended_any {
                hls_flags.push_str("+discont_start");
                insert_discontinuity_if_needed(&index_path).await.ok();
            }

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

                {
                    let mut now = self.now.lock().await;
                    if now.item_id == item.item_id {
                        now.position = unix_timestamp().saturating_sub(item_started_at);
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

    if Path::new("/home/troozn/.deno/bin/deno").exists() {
        cmd.args([
            "--js-runtimes",
            "deno:/home/troozn/.deno/bin/deno",
            "--remote-components",
            "ejs:github",
        ]);
    }

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
    let mut cmd = Command::new(YTDLP_BIN);

    if Path::new("/home/troozn/.deno/bin/deno").exists() {
        cmd.args([
            "--js-runtimes",
            "deno:/home/troozn/.deno/bin/deno",
            "--remote-components",
            "ejs:github",
        ]);
    }

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

    let output = timeout(Duration::from_secs(90), cmd.output())
        .await
        .context("timeout yt-dlp")?
        .context("exécution yt-dlp")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("yt-dlp a échoué: {}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    let url = stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("http://") || line.starts_with("https://"))
        .ok_or_else(|| anyhow!("yt-dlp n'a retourné aucune URL jouable"))?;

    Ok(url.to_string())
}

async fn wait_for_hls_ready(index_path: &Path, root_dir: &Path) -> anyhow::Result<()> {
    for _ in 0..160 {
        let index_ok = index_path.exists();

        let mut has_segment = false;

        if let Ok(mut rd) = fs::read_dir(root_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(".ts") {
                    has_segment = true;
                    break;
                }
            }
        }

        if index_ok && has_segment {
            return Ok(());
        }

        sleep(Duration::from_millis(250)).await;
    }

    anyhow::bail!("HLS non prêt: index.m3u8 ou segment .ts introuvable");
}

async fn count_ts_segments(root_dir: &Path) -> anyhow::Result<u64> {
    let mut count = 0_u64;

    let mut rd = match fs::read_dir(root_dir).await {
        Ok(rd) => rd,
        Err(_) => return Ok(0),
    };

    while let Some(entry) = rd.next_entry().await? {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".ts") {
            count += 1;
        }
    }

    Ok(count)
}

async fn insert_discontinuity_if_needed(index_path: &Path) -> anyhow::Result<()> {
    let content = match fs::read_to_string(index_path).await {
        Ok(content) => content,
        Err(_) => return Ok(()),
    };

    if content.trim_end().ends_with("#EXT-X-DISCONTINUITY") {
        return Ok(());
    }

    let mut updated = content;
    updated.push_str("#EXT-X-DISCONTINUITY\n");
    fs::write(index_path, updated).await?;
    Ok(())
}

async fn finalize_playlist(index_path: &Path) -> anyhow::Result<()> {
    let content = match fs::read_to_string(index_path).await {
        Ok(content) => content,
        Err(_) => return Ok(()),
    };

    if content.contains("#EXT-X-ENDLIST") {
        return Ok(());
    }

    let mut updated = content;
    updated.push_str("#EXT-X-ENDLIST\n");
    fs::write(index_path, updated).await?;
    Ok(())
}

fn item_id_for_url(url: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(url.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
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
        "hls_url": "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8"
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
