use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use tokio::fs;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};

use crate::HttpGatewayState;

const LIVE_DIR: &str = "/tmp/troozn-live";
const YTDLP_BIN: &str = "/usr/local/bin/yt-dlp";
const MAX_ITEMS: usize = 20;

const PUBLIC_HLS_URL: &str = "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8";

// 22 = MP4 progressif 720p H.264 + AAC quand disponible.
// 18 = fallback MP4 progressif, souvent 360p.
const YTDLP_720_FORMAT: &str =
    "22/best[ext=mp4][vcodec^=avc1][acodec^=mp4a][height<=720]/18";

#[derive(Debug)]
pub struct TrooznLive {
    pub root_dir: PathBuf,
    ffmpeg_child: Mutex<Option<Child>>,
    producer_now: Mutex<TrooznLiveNow>,
    playback_now: Mutex<TrooznLiveNow>,
    queue: Mutex<Vec<TrooznLiveItem>>,
    master_entries: Mutex<Vec<MasterEntry>>,
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

#[derive(Debug, Clone)]
struct MasterEntry {
    item_index: usize,
    duration: String,
    program_date_time: Option<String>,
    segment: String,
    discontinuity_before: bool,
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
        let idle = TrooznLiveNow {
            state: "idle".to_string(),
            hls_url: PUBLIC_HLS_URL.to_string(),
            ..Default::default()
        };

        Self {
            root_dir: PathBuf::from(LIVE_DIR),
            ffmpeg_child: Mutex::new(None),
            producer_now: Mutex::new(idle.clone()),
            playback_now: Mutex::new(idle),
            queue: Mutex::new(Vec::new()),
            master_entries: Mutex::new(Vec::new()),
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

        {
            let mut entries = self.master_entries.lock().await;
            entries.clear();
        }

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
            let mut guard = self.producer_now.lock().await;
            *guard = now.clone();
        }

        {
            let mut guard = self.playback_now.lock().await;
            *guard = now.clone();
        }

        let live = self.clone();
        let items_for_worker = items.clone();

        tokio::spawn(async move {
            let live_for_error = live.clone();

            if let Err(err) = live.run_hls_worker(items_for_worker).await {
                eprintln!("TROOZN_LIVE_WORKER_ERROR: {err:?}");

                let mut producer = live_for_error.producer_now.lock().await;
                producer.state = "error".to_string();
                producer.last_error = Some(err.to_string());

                let mut playback = live_for_error.playback_now.lock().await;
                if playback.state != "playing" {
                    playback.state = "error".to_string();
                    playback.last_error = Some(err.to_string());
                }
            }
        });

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
        let stream_started_at = unix_timestamp();
        let mut appended_any = false;

        write_empty_master_playlist(&self.root_dir.join("index.m3u8")).await?;

        for item in items.iter() {
            let next_title = items
                .iter()
                .find(|candidate| candidate.index > item.index)
                .map(|candidate| candidate.title.clone());

            {
                let mut guard = self.producer_now.lock().await;
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

                    let mut guard = self.producer_now.lock().await;
                    guard.last_error = Some(format!("Item ignoré: {} - {}", item.title, err));
                    continue;
                }
            };

            let item_started_at = unix_timestamp();

            {
                let mut guard = self.producer_now.lock().await;
                *guard = TrooznLiveNow {
                    state: "preparing".to_string(),
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

            let item_prefix = format!("item-{:04}", item.index);
            let item_manifest = self.root_dir.join(format!("{item_prefix}.m3u8"));
            let segment_pattern = self.root_dir.join(format!("{item_prefix}-%05d.ts"));

            eprintln!(
                "TROOZN_LIVE_FFMPEG_START index={} title={} manifest={}",
                item.index,
                item.title,
                item_manifest.display()
            );

            let mut cmd = Command::new("ffmpeg");

            // Pas de -re : on pré-segmente plus vite que la lecture.
            cmd.args([
                "-hide_banner",
                "-y",
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
                "-hls_flags",
                "omit_endlist+program_date_time",
                "-hls_segment_filename",
            ]);

            cmd.arg(segment_pattern.to_string_lossy().to_string());
            cmd.arg(item_manifest.to_string_lossy().to_string());

            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::inherit());

            let child = cmd.spawn().context("lancement ffmpeg HLS item")?;

            {
                let mut guard = self.ffmpeg_child.lock().await;
                *guard = Some(child);
            }

            let mut imported_segments = 0_usize;

            loop {
                sleep(Duration::from_millis(500)).await;

                {
                    let mut producer = self.producer_now.lock().await;
                    if producer.item_id == item.item_id && producer.item_started_at > 0 {
                        producer.position = unix_timestamp().saturating_sub(producer.item_started_at);
                    }
                }

                let new_count = self
                    .import_item_manifest_incremental(
                        item.index,
                        &item_manifest,
                        appended_any,
                    )
                    .await
                    .unwrap_or(imported_segments);

                if new_count > imported_segments {
                    imported_segments = new_count;
                    appended_any = true;
                    self.rewrite_master_playlist(false).await.ok();
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
                    let final_count = self
                        .import_item_manifest_incremental(item.index, &item_manifest, appended_any)
                        .await
                        .unwrap_or(imported_segments);

                    if final_count > imported_segments {
                        appended_any = true;
                    }

                    self.rewrite_master_playlist(false).await.ok();
                    break;
                }
            }
        }

        self.rewrite_master_playlist(true).await.ok();

        {
            let mut producer = self.producer_now.lock().await;
            producer.state = "ended".to_string();
        }

        Ok(())
    }

    async fn import_item_manifest_incremental(
        &self,
        item_index: usize,
        item_manifest: &Path,
        has_previous_item: bool,
    ) -> anyhow::Result<usize> {
        let content = match fs::read_to_string(item_manifest).await {
            Ok(content) => content,
            Err(_) => return Ok(0),
        };

        let parsed = parse_item_hls_entries(item_index, &content, has_previous_item);
        let parsed_count = parsed.len();

        if parsed.is_empty() {
            return Ok(0);
        }

        let mut entries = self.master_entries.lock().await;
        let existing_for_item = entries.iter().filter(|e| e.item_index == item_index).count();

        if parsed_count <= existing_for_item {
            return Ok(existing_for_item);
        }

        for entry in parsed.into_iter().skip(existing_for_item) {
            entries.push(entry);
        }

        Ok(parsed_count)
    }

    async fn rewrite_master_playlist(&self, ended: bool) -> anyhow::Result<()> {
        let entries = self.master_entries.lock().await.clone();
        let index_path = self.root_dir.join("index.m3u8");

        let mut target_duration = 4_u64;

        for entry in &entries {
            if let Ok(v) = entry.duration.parse::<f64>() {
                let ceil = v.ceil() as u64;
                if ceil > target_duration {
                    target_duration = ceil;
                }
            }
        }

        let mut out = String::new();
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:3\n");
        // Pas de EXT-X-PLAYLIST-TYPE:EVENT :
        // on veut que Kodi démarre depuis le début, pas près du live edge.
        out.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));
        out.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");

        let mut discontinuity_seen_for_item = HashSet::new();

        for entry in entries {
            if entry.discontinuity_before && discontinuity_seen_for_item.insert(entry.item_index) {
                out.push_str("#EXT-X-DISCONTINUITY\n");
            }

            out.push_str(&format!("#EXTINF:{},\n", entry.duration));

            if let Some(pdt) = entry.program_date_time {
                out.push_str(&format!("#EXT-X-PROGRAM-DATE-TIME:{pdt}\n"));
            }

            out.push_str(&entry.segment);
            out.push('\n');
        }

        if ended {
            out.push_str("#EXT-X-ENDLIST\n");
        }

        fs::write(index_path, out).await?;
        Ok(())
    }

    async fn note_segment_served(&self, relative: &str) {
        let Some((item_index, segment_number)) = parse_item_segment_name(relative) else {
            return;
        };

        let queue = self.queue.lock().await.clone();
        let Some(item) = queue.iter().find(|item| item.index == item_index).cloned() else {
            return;
        };

        let entries = self.master_entries.lock().await.clone();

        let mut position_f64 = 0.0_f64;

        for entry in entries.iter().filter(|entry| entry.item_index == item_index) {
            if entry.segment == relative {
                break;
            }

            if let Ok(d) = entry.duration.parse::<f64>() {
                position_f64 += d;
            }
        }

        let next_title = queue
            .iter()
            .find(|candidate| candidate.index > item.index)
            .map(|candidate| candidate.title.clone());

        let now = TrooznLiveNow {
            state: "playing".to_string(),
            title: item.title.clone(),
            source_url: item.source_url.clone(),
            hls_url: PUBLIC_HLS_URL.to_string(),
            item_id: item.item_id.clone(),
            index: item.index,
            position: position_f64.floor() as u64,
            duration: item.duration,
            thumbnail: item.thumbnail.clone(),
            channel: item.channel.clone(),
            started_at: unix_timestamp(),
            item_started_at: unix_timestamp().saturating_sub(position_f64.floor() as u64),
            next_title,
            last_error: None,
        };

        {
            let mut guard = self.playback_now.lock().await;
            *guard = now;
        }

        eprintln!(
            "TROOZN_LIVE_SEGMENT_SERVED item={} segment={} file={}",
            item_index, segment_number, relative
        );
    }

    pub async fn current_now(&self) -> TrooznLiveNow {
        let playback = self.playback_now.lock().await.clone();

        if playback.state == "playing" && playback.index > 0 {
            return playback;
        }

        self.producer_now.lock().await.clone()
    }

    pub async fn producer_now(&self) -> TrooznLiveNow {
        self.producer_now.lock().await.clone()
    }

    pub async fn current_queue(&self) -> Vec<TrooznLiveItem> {
        self.queue.lock().await.clone()
    }
}

fn parse_item_hls_entries(
    item_index: usize,
    content: &str,
    has_previous_item: bool,
) -> Vec<MasterEntry> {
    let mut out = Vec::new();

    let mut pending_duration: Option<String> = None;
    let mut pending_program_date_time: Option<String> = None;

    for raw in content.lines() {
        let line = raw.trim();

        if let Some(rest) = line.strip_prefix("#EXTINF:") {
            let duration = rest.trim_end_matches(',').trim().to_string();
            pending_duration = Some(duration);
            continue;
        }

        if let Some(rest) = line.strip_prefix("#EXT-X-PROGRAM-DATE-TIME:") {
            pending_program_date_time = Some(rest.trim().to_string());
            continue;
        }

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if !line.ends_with(".ts") {
            continue;
        }

        let segment_name = Path::new(line)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| line.to_string());

        let duration = pending_duration.take().unwrap_or_else(|| "4.000000".to_string());
        let program_date_time = pending_program_date_time.take();

        out.push(MasterEntry {
            item_index,
            duration,
            program_date_time,
            segment: segment_name,
            discontinuity_before: has_previous_item && out.is_empty(),
        });
    }

    out
}

fn parse_item_segment_name(relative: &str) -> Option<(usize, usize)> {
    let name = Path::new(relative).file_name()?.to_string_lossy();

    if !name.starts_with("item-") || !name.ends_with(".ts") {
        return None;
    }

    let without_ext = name.trim_end_matches(".ts");
    let parts: Vec<&str> = without_ext.split('-').collect();

    if parts.len() != 3 {
        return None;
    }

    let item_index = parts[1].parse::<usize>().ok()?;
    let segment_number = parts[2].parse::<usize>().ok()?;

    Some((item_index, segment_number))
}

async fn write_empty_master_playlist(index_path: &Path) -> anyhow::Result<()> {
    let content = "\
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0
";

    fs::write(index_path, content).await?;
    Ok(())
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

fn item_id_for_url(url: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize();

    digest
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
        .chars()
        .take(16)
        .collect()
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

pub async fn troozn_live_producer(State(state): State<HttpGatewayState>) -> impl IntoResponse {
    Json(state.live.producer_now().await)
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

    if relative.ends_with(".ts") {
        state.live.note_segment_served(relative).await;
    }

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
