use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};

use crate::HttpGatewayState;

const LIVE_DIR: &str = "/tmp/troozn-live";
const YTDLP_BIN: &str = "/usr/local/bin/yt-dlp";

const YTDLP_720_FORMAT: &str =
    "22/best[ext=mp4][vcodec^=avc1][acodec^=mp4a][height<=720]/18";

#[derive(Debug)]
pub struct TrooznLive {
    pub root_dir: PathBuf,
    ffmpeg_child: Mutex<Option<Child>>,
    now: Mutex<TrooznLiveNow>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TrooznLiveNow {
    pub state: String,
    pub title: String,
    pub source_url: String,
    pub hls_url: String,
    pub started_at: u64,
}

#[derive(Debug, Deserialize)]
pub struct TrooznLiveSubmitRequest {
    pub url: String,
    pub title: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TrooznLiveSubmitResponse {
    pub ok: bool,
    pub hls_url: String,
    pub live_dir: PathBuf,
    pub now: TrooznLiveNow,
}

impl TrooznLive {
    pub fn new_default() -> Self {
        Self {
            root_dir: PathBuf::from(LIVE_DIR),
            ffmpeg_child: Mutex::new(None),
            now: Mutex::new(TrooznLiveNow {
                state: "idle".to_string(),
                ..Default::default()
            }),
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

    pub async fn start_youtube_live(
        &self,
        source_url: &str,
        title: Option<String>,
    ) -> anyhow::Result<TrooznLiveNow> {
        self.stop_current_ffmpeg().await;
        self.ensure_clean_dir().await?;

        let play_url = resolve_youtube_720_url(source_url).await?;

        let index_path = self.root_dir.join("index.m3u8");
        let segment_pattern = self.root_dir.join("seg-%05d.ts");

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
            "6",
            "-hls_flags",
            "delete_segments+append_list+omit_endlist+program_date_time",
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

        wait_for_hls_ready(&index_path, &self.root_dir).await?;

        let now = TrooznLiveNow {
            state: "playing".to_string(),
            title: title.unwrap_or_else(|| "Playlist Youtube".to_string()),
            source_url: source_url.to_string(),
            hls_url: "http://127.0.0.1:8787/troozn-live/Playlist%20Youtube.m3u8".to_string(),
            started_at: unix_timestamp(),
        };

        {
            let mut guard = self.now.lock().await;
            *guard = now.clone();
        }

        Ok(now)
    }

    pub async fn current_now(&self) -> TrooznLiveNow {
        self.now.lock().await.clone()
    }
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
    for _ in 0..80 {
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
        "quality": "720p-copy"
    }))
}

pub async fn troozn_live_submit(
    State(state): State<HttpGatewayState>,
    Json(req): Json<TrooznLiveSubmitRequest>,
) -> Response {
    match state.live.start_youtube_live(&req.url, req.title).await {
        Ok(now) => {
            let response = TrooznLiveSubmitResponse {
                ok: true,
                hls_url: now.hls_url.clone(),
                live_dir: state.live.root_dir.clone(),
                now,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
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

pub async fn troozn_live_file(
    State(state): State<HttpGatewayState>,
    AxumPath(path): AxumPath<String>,
) -> Response {
    let requested = path.trim_start_matches('/');

    let relative = match requested {
        "" => "index.m3u8",
        "index.m3u8" => "index.m3u8",
        "Playlist Youtube.m3u8" => "index.m3u8",
        "Playlist%20Youtube.m3u8" => "index.m3u8",
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
