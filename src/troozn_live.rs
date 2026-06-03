use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
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
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};
use tokio::time::{sleep, timeout, Duration};

use crate::HttpGatewayState;

const LIVE_DIR: &str = "/home/troozn/.kodi/userdata/TROOZN/live";
const TROOZN_LIVE_BUILD_TAG: &str = "v2.9-deno-json-resolve-2026-06-03";

const YTDLP_BIN: &str = "/home/troozn/.local/bin/yt-dlp";
const LIVE_MAX_DIR_BYTES: u64 = 512 * 1024 * 1024;
const LIVE_TARGET_DIR_BYTES: u64 = 384 * 1024 * 1024;
const LIVE_KEEP_BEHIND_SECONDS: f64 = 75.0;
const LIVE_PRESSURE_KEEP_BEHIND_SECONDS: f64 = 30.0;
const LIVE_CLEANUP_INTERVAL_SECONDS: u64 = 3;
const LIVE_ORPHAN_GRACE_SECONDS: u64 = 45;
const LIVE_TMP_GRACE_SECONDS: u64 = 20;
const LIVE_MAX_PRODUCER_AHEAD_SECONDS: f64 = 240.0;
const LIVE_RESUME_PRODUCER_AHEAD_SECONDS: f64 = 160.0;
const LIVE_AHEAD_WAIT_MAX_SECONDS: u64 = 90;
const PLAYLIST_PAGE_SIZE: usize = 20;
const PLAYLIST_REFILL_THRESHOLD: usize = 5;
const MAX_ITEMS: usize = 20;
const PREWARM_AHEAD_ITEMS: usize = 1;
const PREWARM_CACHE_WAIT_MAX_MS: u64 = 90_000;
const PREWARM_CACHE_WAIT_STEP_MS: u64 = 300;
const PLAYLIST_ACTIVE_SCAN_EXTRA: usize = 6;
const PLAYLIST_ACTIVE_SCAN_MAX: usize = 26;
const PLAYLIST_INITIAL_ACTIVE_TARGET: usize = 2;
const YOUTUBE_QUICK_VALIDATE_CONCURRENCY: usize = 2;
const YOUTUBE_QUICK_VALIDATE_BATCH_PAUSE_MS: u64 = 250;
const YOUTUBE_QUICK_VALIDATE_TIMEOUT_SECONDS: u64 = 4;
const YTDLP_MAX_PARALLEL_PROCESSES: usize = 2;
const YTDLP_SLOT_WAIT_TIMEOUT_SECONDS: u64 = 20;
const STARTUP_BOOST_PREPARE_THROUGH_ITEM_INDEX: usize = 2;
const STARTUP_BOOST_SINGLE_ITEM_READY_SEGMENTS: usize = 3;
const YTDLP_PLAYLIST_EXTRACT_TIMEOUT_SECONDS: u64 = 30;
const YTDLP_YOUTUBE_FAST_RESOLVE_TIMEOUT_SECONDS: u64 = 12;
const YTDLP_YOUTUBE_DASH_RESOLVE_TIMEOUT_SECONDS: u64 = 14;
const YTDLP_YOUTUBE_LIST_FORMATS_TIMEOUT_SECONDS: u64 = 15;
const YTDLP_YOUTUBE_DENO_RESOLVE_TIMEOUT_SECONDS: u64 = 45;
const YTDLP_YOUTUBE_DENO_JSON_TIMEOUT_SECONDS: u64 = 60;
const YTDLP_YOUTUBE_DENO_LIST_FORMATS_TIMEOUT_SECONDS: u64 = 45;
const HLS_SEGMENT_SECONDS: &str = "2";
const PREFERRED_VIDEO_HEIGHT: u64 = 1080;
const FALLBACK_VIDEO_HEIGHT: u64 = 720;
const MIN_SELECTED_VIDEO_HEIGHT: u64 = 480;

const PUBLIC_HLS_BASE_URL: &str = "http://127.0.0.1:8787/troozn-live";
const DEFAULT_PLAYLIST_NAME: &str = "Playlist_Troozn.m3u8";
const DEFAULT_PUBLIC_HLS_URL: &str = "http://127.0.0.1:8787/troozn-live/Playlist_Troozn.m3u8";

const YTDLP_COOKIES_FILE: &str = "/home/troozn/.config/troozn/youtube-cookies.txt";
const YTDLP_YOUTUBE_FAST_FORMAT: &str = "96/22/95/94";
const YTDLP_YOUTUBE_DASH_FORMAT: &str = "137+140/136+140/135+140";
const YTDLP_YOUTUBE_VALIDATE_FORMAT: &str = "96/22/95/94/137+140/136+140/135+140";
const YTDLP_GENERIC_SINGLE_FORMAT: &str =
    "best[height<=1080][height>=720][vcodec!=none][acodec!=none]/best[height=1080][vcodec!=none][acodec!=none]/best[height=720][vcodec!=none][acodec!=none]/best[height<=720][height>=480][vcodec!=none][acodec!=none]/best[height=480][vcodec!=none][acodec!=none]";
const YTDLP_GENERIC_SEPARATE_FORMAT: &str =
    "bv*[height<=1080][height>=720][vcodec!=none]+ba[acodec!=none]/bv*[height=1080][vcodec!=none]+ba[acodec!=none]/bv*[height=720][vcodec!=none]+ba[acodec!=none]/bv*[height<=720][height>=480][vcodec!=none]+ba[acodec!=none]/bv*[height=480][vcodec!=none]+ba[acodec!=none]";

const YTDLP_GENERIC_AUDIO_FORMAT: &str =
    "bestaudio[acodec!=none]/best[acodec!=none]/bestaudio/best";

#[derive(Debug, Clone)]
struct PlaylistRefillState {
    source_url: String,
    next_start: usize,
    exhausted: bool,
    active: bool,
}

pub struct TrooznLive {
    pub root_dir: PathBuf,
    ffmpeg_child: Mutex<Option<ActiveFfmpegChild>>,
    producer_now: Mutex<TrooznLiveNow>,
    playback_now: Mutex<TrooznLiveNow>,
    queue: Mutex<Vec<TrooznLiveItem>>,
    master_entries: Mutex<Vec<MasterEntry>>,
    last_served_segment: Mutex<Option<(usize, usize)>>,
    media_sequence_base: Mutex<u64>,
    discontinuity_sequence_base: Mutex<u64>,
    last_cleanup_at: Mutex<u64>,
    playlist_refill: Mutex<Option<PlaylistRefillState>>,
    resolved_inputs: Mutex<HashMap<String, ResolvedMediaInput>>,
    resolving_inputs: Mutex<HashSet<String>>,
    session_id: Mutex<String>,
    worker_running: Mutex<bool>,
    generation_id: Mutex<u64>,
    playback_anchor_item: Mutex<usize>,
    playlist_name: Mutex<String>,
}

struct ActiveFfmpegChild {
    generation: u64,
    item_id: String,
    child: Child,
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
    pub description: Option<String>,
    pub upload_date: Option<String>,
    pub uploader: Option<String>,
    pub started_at: u64,
    pub item_started_at: u64,
    pub next_title: Option<String>,
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buffer_segments: Option<usize>,
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
    pub description: Option<String>,
    pub upload_date: Option<String>,
    pub uploader: Option<String>,
}

#[derive(Debug, Clone)]
struct MasterEntry {
    item_index: usize,
    duration: String,
    program_date_time: Option<String>,
    segment: String,
    discontinuity_before: bool,
}

#[derive(Debug, Default)]
struct LiveDirUsage {
    bytes: u64,
    files: usize,
    ts_files: usize,
}

#[derive(Debug, Default)]
struct CleanupStats {
    removed_files: usize,
    removed_bytes: u64,
    removed_playlist_entries: usize,
}

impl CleanupStats {
    fn merge(&mut self, other: CleanupStats) {
        self.removed_files += other.removed_files;
        self.removed_bytes = self.removed_bytes.saturating_add(other.removed_bytes);
        self.removed_playlist_entries += other.removed_playlist_entries;
    }
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

#[derive(Debug, Clone)]
struct FullVideoMetadata {
    title: Option<String>,
    webpage_url: Option<String>,
    duration: Option<u64>,
    thumbnail: Option<String>,
    channel: Option<String>,
    description: Option<String>,
    upload_date: Option<String>,
    uploader: Option<String>,
}

async fn live_audit(root_dir: &Path, line: impl AsRef<str>) {
    use tokio::io::AsyncWriteExt;

    let path = root_dir.join("audit.log");
    let msg = format!(
        "{}
",
        line.as_ref()
    );

    match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(mut file) => {
            if let Err(err) = file.write_all(msg.as_bytes()).await {
                eprintln!(
                    "TROOZN_LIVE_AUDIT_WRITE_ERROR path={} state={err:?}",
                    path.display()
                );
            }
        }
        Err(err) => {
            eprintln!(
                "TROOZN_LIVE_AUDIT_OPEN_ERROR path={} state={err:?}",
                path.display()
            );
        }
    }
}

fn ytdlp_semaphore() -> &'static Semaphore {
    static YTDLP_SEMAPHORE: OnceLock<Semaphore> = OnceLock::new();

    YTDLP_SEMAPHORE.get_or_init(|| Semaphore::new(YTDLP_MAX_PARALLEL_PROCESSES))
}

fn ytdlp_serial_mutex() -> &'static Mutex<()> {
    static YTDLP_SERIAL_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

    YTDLP_SERIAL_MUTEX.get_or_init(|| Mutex::new(()))
}

fn ytdlp_startup_boost() -> &'static AtomicBool {
    static YTDLP_STARTUP_BOOST: AtomicBool = AtomicBool::new(false);

    &YTDLP_STARTUP_BOOST
}

fn ytdlp_startup_boost_active() -> bool {
    ytdlp_startup_boost().load(Ordering::Relaxed)
}

fn set_ytdlp_startup_boost(active: bool, reason: &str) {
    let previous = ytdlp_startup_boost().swap(active, Ordering::Relaxed);

    if previous != active {
        eprintln!("TROOZN_LIVE_YTDLP_BOOST active={active} reason={reason}");
    }
}

struct YtdlpSlot {
    _serial_guard: Option<tokio::sync::MutexGuard<'static, ()>>,
    _permit: SemaphorePermit<'static>,
}

async fn acquire_ytdlp_slot(label: &'static str) -> anyhow::Result<YtdlpSlot> {
    let serial_guard = if ytdlp_startup_boost_active() {
        None
    } else {
        Some(
            timeout(
                Duration::from_secs(YTDLP_SLOT_WAIT_TIMEOUT_SECONDS),
                ytdlp_serial_mutex().lock(),
            )
            .await
            .with_context(|| format!("timeout attente série {label}"))?,
        )
    };

    let permit = timeout(
        Duration::from_secs(YTDLP_SLOT_WAIT_TIMEOUT_SECONDS),
        ytdlp_semaphore().acquire(),
    )
    .await
    .with_context(|| format!("timeout attente slot {label}"))?
    .with_context(|| format!("acquisition slot {label}"))?;

    Ok(YtdlpSlot {
        _serial_guard: serial_guard,
        _permit: permit,
    })
}

async fn run_ytdlp_output(
    mut cmd: Command,
    label: &'static str,
    timeout_seconds: u64,
) -> anyhow::Result<std::process::Output> {
    let _permit = acquire_ytdlp_slot(label).await?;
    cmd.kill_on_drop(true);
    cmd.process_group(0);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let child = cmd.spawn().with_context(|| format!("spawn {label}"))?;
    let process_group_id = child.id();

    match timeout(
        Duration::from_secs(timeout_seconds),
        child.wait_with_output(),
    )
    .await
    {
        Ok(result) => result.with_context(|| format!("wait {label}")),
        Err(_) => {
            if let Some(pid) = process_group_id {
                terminate_process_group(pid, label).await;
            }

            anyhow::bail!("timeout {label}");
        }
    }
}

async fn terminate_process_group(pid: u32, label: &'static str) {
    let group = format!("-{pid}");

    eprintln!("TROOZN_LIVE_YTDLP_KILL_GROUP signal=TERM label={label} pgid={pid}");

    Command::new("kill")
        .args(["-TERM", &group])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .ok();

    sleep(Duration::from_millis(350)).await;

    eprintln!("TROOZN_LIVE_YTDLP_KILL_GROUP signal=KILL label={label} pgid={pid}");

    Command::new("kill")
        .args(["-KILL", &group])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .ok();
}

fn parse_item_index_from_live_filename(name: &str) -> Option<usize> {
    if !name.starts_with("item-") {
        return None;
    }

    let rest = name.strip_prefix("item-")?;
    let index_part = rest.get(0..4)?;

    index_part.parse::<usize>().ok()
}

fn count_item_ts_files(root_dir: &Path, item_index: usize) -> usize {
    let prefix = format!("item-{item_index:04}-");
    let Ok(entries) = std::fs::read_dir(root_dir) else {
        return 0;
    };

    entries
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            name.starts_with(&prefix) && name.ends_with(".ts")
        })
        .count()
}

fn count_manifest_ts_lines(path: &Path) -> usize {
    let Ok(content) = std::fs::read_to_string(path) else {
        return 0;
    };

    content
        .lines()
        .filter(|line| {
            let line = line.trim();
            !line.starts_with('#') && line.ends_with(".ts")
        })
        .count()
}

#[derive(Debug, Clone)]
enum ResolvedMediaInput {
    Single {
        url: String,
        format_selector: String,
    },
    SeparateAv {
        video_url: String,
        audio_url: String,
        format_selector: String,
    },
    AudioOnly {
        url: String,
        format_selector: String,
    },
}

fn media_input_summary(input: &ResolvedMediaInput) -> String {
    match input {
        ResolvedMediaInput::Single {
            url,
            format_selector,
        } => {
            format!(
                "single format={} url={}",
                format_selector,
                url.chars().take(80).collect::<String>()
            )
        }
        ResolvedMediaInput::SeparateAv {
            video_url,
            audio_url: _,
            format_selector,
        } => {
            format!(
                "dash-av format={} video={}",
                format_selector,
                video_url.chars().take(80).collect::<String>()
            )
        }
        ResolvedMediaInput::AudioOnly {
            url,
            format_selector,
        } => {
            format!(
                "audio-only format={} url={}",
                format_selector,
                url.chars().take(80).collect::<String>()
            )
        }
    }
}

fn media_type_for_input(input: &ResolvedMediaInput) -> &'static str {
    match input {
        ResolvedMediaInput::AudioOnly { .. } => "audio",
        ResolvedMediaInput::Single { url, .. } => infer_media_type_from_url(url).unwrap_or("video"),
        ResolvedMediaInput::SeparateAv { .. } => "video",
    }
}

fn infer_media_type_from_url(source_url: &str) -> Option<&'static str> {
    let ext = url_extension(source_url)?;

    if matches!(
        ext.as_str(),
        "mp3" | "m4a" | "aac" | "flac" | "wav" | "ogg" | "oga" | "opus" | "wma" | "aiff"
    ) {
        return Some("audio");
    }

    if matches!(
        ext.as_str(),
        "mp4"
            | "mkv"
            | "webm"
            | "mov"
            | "m4v"
            | "avi"
            | "ts"
            | "m2ts"
            | "mpg"
            | "mpeg"
            | "3gp"
            | "flv"
            | "m3u8"
    ) {
        return Some("video");
    }

    None
}

async fn direct_media_input(source_url: &str) -> Option<ResolvedMediaInput> {
    if !source_url.starts_with("http://") && !source_url.starts_with("https://") {
        return None;
    }

    match infer_media_type_from_url(source_url)? {
        "audio" => Some(ResolvedMediaInput::AudioOnly {
            url: source_url.to_string(),
            format_selector: "direct-audio".to_string(),
        }),
        "video" => match probe_direct_video_height(source_url).await {
            Ok(Some(height)) if height >= MIN_SELECTED_VIDEO_HEIGHT => {
                Some(ResolvedMediaInput::Single {
                    url: source_url.to_string(),
                    format_selector: format!("direct-video-{height}p"),
                })
            }
            Ok(Some(height)) => {
                eprintln!(
                    "TROOZN_LIVE_DIRECT_VIDEO_TOO_LOW height={} min={} url={}",
                    height, MIN_SELECTED_VIDEO_HEIGHT, source_url
                );
                None
            }
            Ok(None) => {
                eprintln!(
                    "TROOZN_LIVE_DIRECT_VIDEO_UNKNOWN_HEIGHT min={} url={}",
                    MIN_SELECTED_VIDEO_HEIGHT, source_url
                );
                None
            }
            Err(err) => {
                eprintln!(
                    "TROOZN_LIVE_DIRECT_VIDEO_PROBE_FAILED min={} url={} state={err:?}",
                    MIN_SELECTED_VIDEO_HEIGHT, source_url
                );
                None
            }
        },
        _ => None,
    }
}

fn url_extension(source_url: &str) -> Option<String> {
    let clean = source_url
        .split(['?', '#'])
        .next()
        .unwrap_or(source_url)
        .trim_end_matches('/');
    let ext = clean.rsplit('.').next()?.to_ascii_lowercase();

    if ext.len() > 6 || ext == clean {
        return None;
    }

    Some(ext)
}

async fn probe_direct_video_height(source_url: &str) -> anyhow::Result<Option<u64>> {
    let mut cmd = Command::new("ffprobe");

    cmd.args([
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=height",
        "-of",
        "json",
        source_url,
    ]);

    let output = timeout(Duration::from_secs(8), cmd.output())
        .await
        .context("timeout ffprobe direct video")?
        .context("spawn ffprobe direct video")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ffprobe direct video failed: {}", stderr.trim());
    }

    let root: Value =
        serde_json::from_slice(&output.stdout).context("parse ffprobe direct video JSON")?;
    let height = root
        .get("streams")
        .and_then(Value::as_array)
        .and_then(|streams| streams.first())
        .and_then(|stream| stream.get("height"))
        .and_then(Value::as_u64);

    Ok(height)
}

fn playlist_name_for_source(source_url: &str) -> String {
    let host = source_host(source_url).unwrap_or_else(|| source_url.to_ascii_lowercase());
    let label = if host.contains("youtube.com") || host.contains("youtu.be") {
        "Youtube".to_string()
    } else if host.contains("soundcloud.com") {
        "Soundcloud".to_string()
    } else if host.contains("vimeo.com") {
        "Vimeo".to_string()
    } else if host.contains("dailymotion.com") || host.contains("dai.ly") {
        "Dailymotion".to_string()
    } else if host.contains("twitch.tv") {
        "Twitch".to_string()
    } else if host.contains("spotify.com") {
        "Spotify".to_string()
    } else if host.contains("deezer.com") {
        "Deezer".to_string()
    } else if host.contains("mixcloud.com") {
        "Mixcloud".to_string()
    } else if host.contains("bandcamp.com") {
        "Bandcamp".to_string()
    } else {
        let base = host
            .trim_start_matches("www.")
            .split('.')
            .next()
            .unwrap_or("Troozn");
        title_case_ascii(base)
    };

    format!("Playlist_{}.m3u8", sanitize_playlist_label(&label))
}

fn source_host(source_url: &str) -> Option<String> {
    let lower = source_url.to_ascii_lowercase();
    let after_scheme = lower
        .strip_prefix("https://")
        .or_else(|| lower.strip_prefix("http://"))?;
    let host = after_scheme.split(['/', '?', '#']).next()?.trim();

    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

fn title_case_ascii(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return "Troozn".to_string();
    };

    format!(
        "{}{}",
        first.to_ascii_uppercase(),
        chars.collect::<String>().to_ascii_lowercase()
    )
}

fn sanitize_playlist_label(value: &str) -> String {
    let mut out = String::new();

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else if ch == '-' || ch == '_' {
            out.push('_');
        }
    }

    if out.is_empty() {
        "Troozn".to_string()
    } else {
        out
    }
}

fn is_public_playlist_alias(requested: &str, current_playlist_name: &str) -> bool {
    let lower = requested.to_ascii_lowercase();

    requested == current_playlist_name
        || lower == current_playlist_name.to_ascii_lowercase()
        || lower == "playlist-youtube.m3u8"
        || (requested.starts_with("Playlist_") && requested.ends_with(".m3u8"))
}

fn playlist_title_from_name(playlist_name: &str) -> String {
    let label = playlist_name
        .strip_prefix("Playlist_")
        .unwrap_or("Troozn")
        .trim_end_matches(".m3u8")
        .replace('_', " ");

    format!("Playlist {label}")
}

impl TrooznLive {
    pub fn new_default() -> Self {
        let idle = TrooznLiveNow {
            state: "idle".to_string(),
            hls_url: DEFAULT_PUBLIC_HLS_URL.to_string(),
            ..Default::default()
        };

        Self {
            root_dir: PathBuf::from(LIVE_DIR),
            ffmpeg_child: Mutex::new(None),
            producer_now: Mutex::new(idle.clone()),
            playback_now: Mutex::new(idle),
            queue: Mutex::new(Vec::new()),
            master_entries: Mutex::new(Vec::new()),
            last_served_segment: Mutex::new(None),
            media_sequence_base: Mutex::new(0),
            discontinuity_sequence_base: Mutex::new(0),
            last_cleanup_at: Mutex::new(0),
            playlist_refill: Mutex::new(None),
            resolved_inputs: Mutex::new(HashMap::new()),
            resolving_inputs: Mutex::new(HashSet::new()),
            session_id: Mutex::new(unix_timestamp().to_string()),
            worker_running: Mutex::new(false),
            generation_id: Mutex::new(0),
            playback_anchor_item: Mutex::new(1),
            playlist_name: Mutex::new(DEFAULT_PLAYLIST_NAME.to_string()),
        }
    }

    async fn current_public_hls_url(&self) -> String {
        let playlist_name = self.playlist_name.lock().await.clone();
        format!("{PUBLIC_HLS_BASE_URL}/{playlist_name}")
    }

    async fn current_hls_url(&self) -> String {
        let session_id = self.session_id.lock().await.clone();
        let public_hls_url = self.current_public_hls_url().await;
        format!("{}?session={}", public_hls_url, session_id)
    }

    async fn current_playlist_name(&self) -> String {
        self.playlist_name.lock().await.clone()
    }

    async fn set_playlist_name_for_source(&self, source_url: &str) -> String {
        let name = playlist_name_for_source(source_url);
        let mut guard = self.playlist_name.lock().await;
        *guard = name.clone();
        name
    }

    async fn new_session_id(&self) -> String {
        let id = unix_timestamp().to_string();
        let mut guard = self.session_id.lock().await;
        *guard = id.clone();
        id
    }

    async fn append_items_to_queue(&self, items: Vec<TrooznLiveItem>) -> Vec<TrooznLiveItem> {
        let mut queue = self.queue.lock().await;
        let base = queue.len();

        let mut added = Vec::new();

        for (offset, mut item) in items.into_iter().enumerate() {
            item.index = base + offset + 1;
            added.push(item.clone());
            queue.push(item);
        }

        added
    }

    async fn maybe_refill_playlist_queue(&self, current_index: usize) {
        let queue_len = {
            let queue = self.queue.lock().await;
            queue.len()
        };

        if queue_len <= current_index {
            return;
        }

        let remaining = queue_len.saturating_sub(current_index);

        if remaining > PLAYLIST_REFILL_THRESHOLD {
            return;
        }

        let refill_state = {
            let mut guard = self.playlist_refill.lock().await;

            let Some(state) = guard.as_mut() else {
                return;
            };

            if state.exhausted || state.active {
                return;
            }

            state.active = true;
            state.clone()
        };

        let start = refill_state.next_start;
        let end = start + PLAYLIST_PAGE_SIZE - 1;
        let source_url = refill_state.source_url.clone();

        eprintln!(
            "TROOZN_LIVE_REFILL_START current_index={} remaining={} start={} end={}",
            current_index, remaining, start, end
        );

        let result = extract_youtube_items_range_with_retry(&source_url, start, end).await;

        let mut guard = self.playlist_refill.lock().await;

        match result {
            Ok(items) if !items.is_empty() => {
                let count = items.len();

                drop(guard);

                let added = self.append_items_to_queue(items).await;

                let mut guard = self.playlist_refill.lock().await;

                if let Some(state) = guard.as_mut() {
                    state.next_start = start + count;
                    state.active = false;

                    if count < PLAYLIST_PAGE_SIZE {
                        state.exhausted = true;
                    }
                }

                eprintln!(
                    "TROOZN_LIVE_REFILL_DONE added={} next_start={}",
                    added.len(),
                    start + count
                );
            }
            Ok(_) => {
                if let Some(state) = guard.as_mut() {
                    state.exhausted = true;
                    state.active = false;
                }

                eprintln!("TROOZN_LIVE_REFILL_EXHAUSTED start={} end={}", start, end);
            }
            Err(err) => {
                if let Some(state) = guard.as_mut() {
                    state.active = false;
                }

                eprintln!(
                    "TROOZN_LIVE_REFILL_ERROR start={} end={} state={err:?}",
                    start, end
                );
            }
        }
    }

    async fn next_title_after(&self, index: usize) -> Option<String> {
        let queue = self.queue.lock().await;

        queue
            .iter()
            .find(|candidate| candidate.index > index)
            .map(|candidate| candidate.title.clone())
    }

    async fn current_buffer_stats(&self) -> (f64, usize) {
        let last_served = *self.last_served_segment.lock().await;
        let playback = self.playback_now.lock().await.clone();
        let anchor_item = if playback.index > 0 {
            playback.index
        } else {
            *self.playback_anchor_item.lock().await
        };
        let entries = self.master_entries.lock().await.clone();

        let mut seconds = 0.0_f64;
        let mut segments = 0_usize;

        for entry in entries {
            if entry.item_index < anchor_item {
                continue;
            }

            if let Some((served_item, served_segment)) = last_served {
                if entry.item_index < served_item {
                    continue;
                }

                if entry.item_index == served_item {
                    let Some((_, entry_segment)) = parse_item_segment_name(&entry.segment) else {
                        continue;
                    };

                    if entry_segment <= served_segment {
                        continue;
                    }
                }
            }

            if let Ok(duration) = entry.duration.parse::<f64>() {
                seconds += duration;
                segments += 1;
            }
        }

        (seconds, segments)
    }

    async fn refresh_buffer_status(&self) {
        let (buffer_seconds, buffer_segments) = self.current_buffer_stats().await;

        {
            let mut producer = self.producer_now.lock().await;
            producer.buffer_seconds = Some(buffer_seconds);
            producer.buffer_segments = Some(buffer_segments);
        }

        {
            let mut playback = self.playback_now.lock().await;
            playback.buffer_seconds = Some(buffer_seconds);
            playback.buffer_segments = Some(buffer_segments);
        }
    }

    async fn cached_or_resolve_media_input(
        self: std::sync::Arc<Self>,
        item: &TrooznLiveItem,
    ) -> anyhow::Result<ResolvedMediaInput> {
        if let Some(input) = self.resolved_inputs.lock().await.remove(&item.item_id) {
            eprintln!(
                "TROOZN_LIVE_RESOLVE_CACHE_HIT index={} title={}",
                item.index, item.title
            );
            return Ok(input);
        }

        let is_being_prewarmed = {
            let resolving = self.resolving_inputs.lock().await;
            resolving.contains(&item.item_id)
        };

        if is_being_prewarmed {
            eprintln!(
                "TROOZN_LIVE_RESOLVE_WAIT_PREWARM index={} title={}",
                item.index, item.title
            );

            let mut waited_ms = 0_u64;

            while waited_ms < PREWARM_CACHE_WAIT_MAX_MS {
                sleep(Duration::from_millis(PREWARM_CACHE_WAIT_STEP_MS)).await;
                waited_ms = waited_ms.saturating_add(PREWARM_CACHE_WAIT_STEP_MS);

                if let Some(input) = self.resolved_inputs.lock().await.remove(&item.item_id) {
                    eprintln!(
                        "TROOZN_LIVE_RESOLVE_PREWARM_READY index={} title={} waited_ms={}",
                        item.index, item.title, waited_ms
                    );
                    return Ok(input);
                }

                let still_resolving = {
                    let resolving = self.resolving_inputs.lock().await;
                    resolving.contains(&item.item_id)
                };

                if !still_resolving {
                    break;
                }
            }

            eprintln!(
                "TROOZN_LIVE_RESOLVE_PREWARM_MISS index={} title={} waited_ms={}",
                item.index, item.title, waited_ms
            );
        }

        resolve_media_input(&item.source_url).await
    }

    async fn spawn_prewarm_after(self: std::sync::Arc<Self>, index: usize) {
        let candidates = {
            let queue = self.queue.lock().await;
            queue
                .iter()
                .filter(|candidate| candidate.index > index)
                .take(PREWARM_AHEAD_ITEMS)
                .cloned()
                .collect::<Vec<_>>()
        };

        for item in candidates {
            {
                let cache = self.resolved_inputs.lock().await;
                if cache.contains_key(&item.item_id) {
                    continue;
                }
            }

            {
                let mut resolving = self.resolving_inputs.lock().await;
                if !resolving.insert(item.item_id.clone()) {
                    continue;
                }
            }

            let live = self.clone();
            tokio::spawn(async move {
                eprintln!(
                    "TROOZN_LIVE_PREWARM_START index={} title={}",
                    item.index, item.title
                );
                let result = resolve_media_input(&item.source_url).await;
                let mut resolving = live.resolving_inputs.lock().await;
                resolving.remove(&item.item_id);
                drop(resolving);

                match result {
                    Ok(input) => {
                        live.resolved_inputs
                            .lock()
                            .await
                            .insert(item.item_id.clone(), input);
                        eprintln!(
                            "TROOZN_LIVE_PREWARM_DONE index={} title={}",
                            item.index, item.title
                        );
                    }
                    Err(err) => {
                        eprintln!(
                            "TROOZN_LIVE_PREWARM_FAILED index={} title={} state={err:?}",
                            item.index, item.title
                        );
                    }
                }
            });
        }
    }

    async fn extract_items_for_live(
        &self,
        source_url: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<TrooznLiveItem>> {
        let limit = limit.clamp(1, MAX_ITEMS);

        let mut extraction_urls: Vec<String> = vec![source_url.to_string()];

        if let Some(normalized) = normalize_rd_playlist_to_watch_url(source_url) {
            if normalized != source_url {
                eprintln!(
                    "TROOZN_LIVE_RD_NORMALIZED source_url={} normalized={}",
                    source_url, normalized
                );
                extraction_urls.push(normalized);
            }
        }

        let mut last_error: Option<String> = None;

        for candidate_url in extraction_urls.iter() {
            match extract_youtube_items_with_retry(candidate_url, limit).await {
                Ok(found) if !found.is_empty() => {
                    eprintln!(
                        "TROOZN_LIVE_EXTRACT_OK url={} count={}",
                        candidate_url,
                        found.len()
                    );
                    return Ok(found);
                }
                Ok(_) => {
                    last_error = Some(format!("Extraction vide pour {}", candidate_url));
                    eprintln!("TROOZN_LIVE_EXTRACT_EMPTY url={}", candidate_url);
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                    eprintln!(
                        "TROOZN_LIVE_EXTRACT_FAILED url={} state={err:?}",
                        candidate_url
                    );
                }
            }
        }

        if let Some(item) = fallback_single_item_from_url(source_url) {
            eprintln!(
                "TROOZN_LIVE_EXTRACT_FALLBACK_SINGLE source_url={} item_url={}",
                source_url, item.source_url
            );
            return Ok(vec![item]);
        }

        anyhow::bail!(
            "Aucun item extractible pour ce lien. Dernière erreur: {}",
            last_error.unwrap_or_else(|| "inconnue".to_string())
        );
    }

    async fn bump_generation(&self) -> u64 {
        let mut guard = self.generation_id.lock().await;
        *guard = guard.saturating_add(1);
        *guard
    }

    async fn current_generation(&self) -> u64 {
        *self.generation_id.lock().await
    }

    async fn ensure_worker_running(self: std::sync::Arc<Self>) {
        {
            let mut running = self.worker_running.lock().await;

            if *running {
                return;
            }

            *running = true;
        }

        let live = self.clone();
        let worker_generation = self.current_generation().await;

        tokio::spawn(async move {
            eprintln!("TROOZN_LIVE_WORKER_SPAWN generation={}", worker_generation);

            if let Err(err) = live.clone().run_hls_worker(worker_generation).await {
                eprintln!("TROOZN_LIVE_WORKER_ERROR: {err:?}");

                let mut producer = live.producer_now.lock().await;
                producer.state = "error".to_string();
                producer.last_error = Some(err.to_string());
            }

            if live.current_generation().await == worker_generation {
                let mut running = live.worker_running.lock().await;
                *running = false;
            }

            eprintln!("TROOZN_LIVE_WORKER_EXIT generation={}", worker_generation);
        });
    }

    async fn ensure_clean_dir(&self) -> anyhow::Result<()> {
        if self.root_dir.exists() {
            fs::remove_dir_all(&self.root_dir).await.ok();
        }

        fs::create_dir_all(&self.root_dir).await?;

        {
            let mut entries = self.master_entries.lock().await;
            entries.clear();
        }
        self.resolved_inputs.lock().await.clear();
        self.resolving_inputs.lock().await.clear();
        *self.last_served_segment.lock().await = None;
        *self.media_sequence_base.lock().await = 0;
        *self.discontinuity_sequence_base.lock().await = 0;
        *self.last_cleanup_at.lock().await = 0;
        *self.playlist_name.lock().await = DEFAULT_PLAYLIST_NAME.to_string();

        Ok(())
    }

    async fn stop_current_ffmpeg(&self) {
        let active = {
            let mut guard = self.ffmpeg_child.lock().await;
            guard.take()
        };

        if let Some(mut active) = active {
            eprintln!(
                "TROOZN_LIVE_FFMPEG_STOP generation={} item_id={}",
                active.generation, active.item_id
            );
            active.child.start_kill().ok();
            timeout(Duration::from_secs(2), active.child.wait())
                .await
                .ok();
        }
    }

    async fn stop_ffmpeg_for_generation(&self, generation: u64) {
        let active = {
            let mut guard = self.ffmpeg_child.lock().await;
            if guard
                .as_ref()
                .map(|active| active.generation == generation)
                .unwrap_or(false)
            {
                guard.take()
            } else {
                None
            }
        };

        if let Some(mut active) = active {
            eprintln!(
                "TROOZN_LIVE_FFMPEG_STOP_STALE generation={} item_id={}",
                active.generation, active.item_id
            );
            active.child.start_kill().ok();
            timeout(Duration::from_secs(2), active.child.wait())
                .await
                .ok();
        }
    }

    pub async fn start_youtube_live_queue(
        self: std::sync::Arc<Self>,
        source_url: &str,
        title: Option<String>,
        limit: usize,
    ) -> anyhow::Result<TrooznLiveSubmitResponse> {
        self.stop_current_ffmpeg().await;
        set_ytdlp_startup_boost(true, "session-start");

        self.bump_generation().await;
        {
            let mut running = self.worker_running.lock().await;
            *running = false;
        }

        self.ensure_clean_dir().await?;
        self.new_session_id().await;
        let playlist_name = self.set_playlist_name_for_source(source_url).await;
        let default_title = playlist_title_from_name(&playlist_name);
        let public_hls_url = self.current_public_hls_url().await;

        live_audit(
            &self.root_dir,
            format!(
                "SESSION_START build_tag={} source_url={} playlist_name={}",
                TROOZN_LIVE_BUILD_TAG, source_url, playlist_name
            ),
        )
        .await;

        {
            let mut anchor = self.playback_anchor_item.lock().await;
            *anchor = 1;
        }

        {
            let mut entries = self.master_entries.lock().await;
            entries.clear();
        }

        let limit = limit.clamp(1, MAX_ITEMS);
        let playlist_like = is_probably_playlist_url(source_url);
        let mut extraction_urls: Vec<String> = vec![source_url.to_string()];

        if let Some(normalized) = normalize_rd_playlist_to_watch_url(source_url) {
            if normalized != source_url {
                eprintln!(
                    "TROOZN_LIVE_RD_NORMALIZED source_url={} normalized={}",
                    source_url, normalized
                );
                extraction_urls.push(normalized);
            }
        }

        let mut last_error: Option<String> = None;
        let mut items: Vec<TrooznLiveItem> = Vec::new();

        for candidate_url in extraction_urls.iter() {
            match extract_youtube_items_with_retry(candidate_url, limit).await {
                Ok(found) if !found.is_empty() => {
                    eprintln!(
                        "TROOZN_LIVE_EXTRACT_OK url={} count={}",
                        candidate_url,
                        found.len()
                    );
                    items = found;
                    break;
                }
                Ok(_) => {
                    last_error = Some(format!("Extraction vide pour {}", candidate_url));
                    eprintln!("TROOZN_LIVE_EXTRACT_EMPTY url={}", candidate_url);
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                    eprintln!(
                        "TROOZN_LIVE_EXTRACT_FAILED url={} state={err:?}",
                        candidate_url
                    );
                }
            }
        }

        if items.is_empty() {
            if playlist_like {
                eprintln!(
                    "TROOZN_LIVE_PLAYLIST_FALLBACK_SINGLE source_url={} last_error={}",
                    source_url,
                    last_error.clone().unwrap_or_else(|| "inconnue".to_string())
                );
            }

            items = fallback_single_item_from_url(source_url)
                .map(|item| vec![item])
                .ok_or_else(|| anyhow::anyhow!("Aucun item yt-dlp trouvé"))?;
        }

        if items.is_empty() {
            anyhow::bail!("Aucun item yt-dlp trouvé");
        }

        {
            let mut q = self.queue.lock().await;
            *q = items.clone();
        }

        let now = TrooznLiveNow {
            state: "starting".to_string(),
            title: title.unwrap_or(default_title),
            source_url: source_url.to_string(),
            hls_url: public_hls_url,
            started_at: unix_timestamp(),
            media_type: infer_media_type_from_url(source_url).map(str::to_string),
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

        if is_probably_playlist_url(source_url) && items.len() >= PLAYLIST_PAGE_SIZE {
            let mut refill = self.playlist_refill.lock().await;
            *refill = Some(PlaylistRefillState {
                source_url: source_url.to_string(),
                next_start: PLAYLIST_PAGE_SIZE + 1,
                exhausted: false,
                active: false,
            });

            eprintln!(
                "TROOZN_LIVE_REFILL_REGISTER source_url={} next_start={}",
                source_url,
                PLAYLIST_PAGE_SIZE + 1
            );
        } else {
            let mut refill = self.playlist_refill.lock().await;
            *refill = None;
        }

        self.clone().ensure_worker_running().await;

        let now = self.current_now().await;
        let hls_url = self.current_hls_url().await;

        Ok(TrooznLiveSubmitResponse {
            ok: true,
            hls_url,
            live_dir: self.root_dir.clone(),
            count: items.len(),
            queue: items,
            now,
        })
    }

    pub async fn add_youtube_live_queue(
        self: std::sync::Arc<Self>,
        source_url: &str,
        _title: Option<String>,
        limit: usize,
    ) -> anyhow::Result<TrooznLiveSubmitResponse> {
        let items = self.extract_items_for_live(source_url, limit).await?;
        let added = self.append_items_to_queue(items).await;

        if is_probably_playlist_url(source_url) && added.len() >= PLAYLIST_PAGE_SIZE {
            let mut refill = self.playlist_refill.lock().await;
            *refill = Some(PlaylistRefillState {
                source_url: source_url.to_string(),
                next_start: PLAYLIST_PAGE_SIZE + 1,
                exhausted: false,
                active: false,
            });

            eprintln!(
                "TROOZN_LIVE_REFILL_REGISTER_ADD source_url={} next_start={}",
                source_url,
                PLAYLIST_PAGE_SIZE + 1
            );
        }

        if let Some(first_added) = added.first() {
            let mut anchor = self.playback_anchor_item.lock().await;
            *anchor = first_added.index;
        }

        if added.is_empty() {
            anyhow::bail!("Aucun item ajouté à TROOZN Live");
        }

        {
            let mut producer = self.producer_now.lock().await;

            if producer.state == "ended" || producer.state == "idle" {
                producer.state = "waiting".to_string();
                producer.last_error = Some("Nouveaux items ajoutés".to_string());
            }
        }

        self.clone().ensure_worker_running().await;

        let now = self.current_now().await;
        let hls_url = self.current_hls_url().await;

        Ok(TrooznLiveSubmitResponse {
            ok: true,
            hls_url,
            live_dir: self.root_dir.clone(),
            count: added.len(),
            queue: added,
            now,
        })
    }

    async fn run_hls_worker(
        self: std::sync::Arc<Self>,
        worker_generation: u64,
    ) -> anyhow::Result<()> {
        let stream_started_at = unix_timestamp();
        let mut appended_any = false;

        write_empty_master_playlist(&self.root_dir.join("index.m3u8")).await?;

        let mut cursor: usize = 0;

        loop {
            if self.current_generation().await != worker_generation {
                eprintln!(
                    "TROOZN_LIVE_WORKER_STALE_EXIT generation={}",
                    worker_generation
                );
                return Ok(());
            }

            let item = loop {
                if self.current_generation().await != worker_generation {
                    eprintln!(
                        "TROOZN_LIVE_WORKER_STALE_EXIT_IN_WAIT generation={}",
                        worker_generation
                    );
                    return Ok(());
                }

                let queue = self.queue.lock().await;

                if cursor < queue.len() {
                    let item = queue[cursor].clone();
                    cursor += 1;
                    break item;
                }

                drop(queue);

                {
                    let mut producer = self.producer_now.lock().await;
                    producer.state = "waiting".to_string();
                    producer.last_error = Some("En attente de nouveaux items".to_string());
                }

                sleep(Duration::from_millis(1000)).await;
            };
            live_audit(
                &self.root_dir,
                format!(
                    "ITEM_START index={} title={} url={}",
                    item.index, item.title, item.source_url
                ),
            )
            .await;

            self.wait_until_future_buffer_needed(item.index).await;

            let next_title = self.next_title_after(item.index).await;
            let public_hls_url = self.current_public_hls_url().await;

            {
                let mut guard = self.producer_now.lock().await;
                guard.state = "preparing".to_string();
                guard.title = item.title.clone();
                guard.source_url = item.source_url.clone();
                guard.hls_url = public_hls_url.clone();
                guard.item_id = item.item_id.clone();
                guard.index = item.index;
                guard.position = 0;
                guard.duration = item.duration;
                guard.thumbnail = item.thumbnail.clone();
                guard.channel = item.channel.clone();
                guard.description = item.description.clone();
                guard.upload_date = item.upload_date.clone();
                guard.uploader = item.uploader.clone();
                guard.media_type = infer_media_type_from_url(&item.source_url).map(str::to_string);
                guard.started_at = stream_started_at;
                guard.item_started_at = 0;
                guard.next_title = next_title.clone();
                guard.last_error = None;
            }

            eprintln!(
                "TROOZN_LIVE_RESOLVE_START index={} title={}",
                item.index, item.title
            );

            // Ne pas bloquer le démarrage HLS sur les métadonnées complètes.
            // On clone l'item flat-playlist pour démarrer vite, puis on enrichit en arrière-plan.
            let item = item.clone();

            {
                let mut guard = self.producer_now.lock().await;
                guard.last_error = Some("Résolution URL vidéo 1080p/720p en cours".to_string());
            }

            let media_input = match self.clone().cached_or_resolve_media_input(&item).await {
                Ok(input) => input,
                Err(err) => {
                    // Échec silencieux par item :
                    // on n'arrête pas le producer, on passe simplement à l'item suivant.
                    eprintln!(
                        "TROOZN_LIVE_SKIP_ITEM index={} title={} source_url={} state={err:?}",
                        item.index, item.title, item.source_url
                    );

                    live_audit(
                        &self.root_dir,
                        format!(
                            "ITEM_YTDLP_FAIL index={} title={} url={} state={err:?}",
                            item.index, item.title, item.source_url
                        ),
                    )
                    .await;

                    {
                        let mut guard = self.producer_now.lock().await;
                        guard.state = "skipping".to_string();
                        guard.title = item.title.clone();
                        guard.source_url = item.source_url.clone();
                        guard.item_id = item.item_id.clone();
                        guard.index = item.index;
                        guard.position = 0;
                        guard.duration = item.duration;
                        guard.thumbnail = item.thumbnail.clone();
                        guard.channel = item.channel.clone();
                        guard.description = item.description.clone();
                        guard.upload_date = item.upload_date.clone();
                        guard.uploader = item.uploader.clone();
                        guard.media_type =
                            infer_media_type_from_url(&item.source_url).map(str::to_string);
                        guard.buffer_seconds = Some(0.0);
                        guard.buffer_segments = Some(0);
                        guard.last_error = Some(format!("Item ignoré: {}", item.title));
                    }

                    sleep(Duration::from_millis(150)).await;
                    continue;
                }
            };

            live_audit(
                &self.root_dir,
                format!(
                    "ITEM_YTDLP_OK index={} title={} play_url_prefix={}",
                    item.index,
                    item.title,
                    media_input_summary(&media_input)
                ),
            )
            .await;

            let item_started_at = unix_timestamp();

            {
                let mut guard = self.producer_now.lock().await;
                *guard = TrooznLiveNow {
                    state: "preparing".to_string(),
                    title: item.title.clone(),
                    source_url: item.source_url.clone(),
                    hls_url: public_hls_url,
                    item_id: item.item_id.clone(),
                    index: item.index,
                    position: 0,
                    duration: item.duration,
                    thumbnail: item.thumbnail.clone(),
                    channel: item.channel.clone(),
                    description: item.description.clone(),
                    upload_date: item.upload_date.clone(),
                    uploader: item.uploader.clone(),
                    started_at: stream_started_at,
                    item_started_at,
                    next_title,
                    last_error: None,
                    media_type: Some(media_type_for_input(&media_input).to_string()),
                    buffer_seconds: Some(0.0),
                    buffer_segments: Some(0),
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

            cmd.args(["-hide_banner", "-nostdin", "-loglevel", "warning", "-y"]);

            match &media_input {
                ResolvedMediaInput::Single {
                    url,
                    format_selector,
                } => {
                    eprintln!(
                        "TROOZN_LIVE_FFMPEG_INPUT_SINGLE index={} format={}",
                        item.index, format_selector
                    );

                    cmd.args([
                        "-reconnect",
                        "1",
                        "-reconnect_streamed",
                        "1",
                        "-reconnect_on_network_error",
                        "1",
                        "-reconnect_delay_max",
                        "4",
                        "-rw_timeout",
                        "15000000",
                        "-i",
                        url,
                    ]);
                }
                ResolvedMediaInput::SeparateAv {
                    video_url,
                    audio_url,
                    format_selector,
                } => {
                    eprintln!(
                        "TROOZN_LIVE_FFMPEG_INPUT_DASH_AV index={} format={}",
                        item.index, format_selector
                    );

                    cmd.args([
                        "-reconnect",
                        "1",
                        "-reconnect_streamed",
                        "1",
                        "-reconnect_on_network_error",
                        "1",
                        "-reconnect_delay_max",
                        "4",
                        "-rw_timeout",
                        "15000000",
                        "-i",
                        video_url,
                        "-reconnect",
                        "1",
                        "-reconnect_streamed",
                        "1",
                        "-reconnect_on_network_error",
                        "1",
                        "-reconnect_delay_max",
                        "4",
                        "-rw_timeout",
                        "15000000",
                        "-i",
                        audio_url,
                        "-map",
                        "0:v:0",
                        "-map",
                        "1:a:0",
                    ]);
                }
                ResolvedMediaInput::AudioOnly {
                    url,
                    format_selector,
                } => {
                    eprintln!(
                        "TROOZN_LIVE_FFMPEG_INPUT_AUDIO_ONLY index={} format={}",
                        item.index, format_selector
                    );

                    cmd.args([
                        "-reconnect",
                        "1",
                        "-reconnect_streamed",
                        "1",
                        "-reconnect_on_network_error",
                        "1",
                        "-reconnect_delay_max",
                        "4",
                        "-rw_timeout",
                        "15000000",
                        "-i",
                        url,
                    ]);
                }
            }

            cmd.args([
                "-fflags",
                "+genpts",
                "-avoid_negative_ts",
                "make_zero",
                "-max_muxing_queue_size",
                "1024",
            ]);

            match &media_input {
                ResolvedMediaInput::Single { .. } => {
                    cmd.args(["-map", "0:v:0?", "-map", "0:a:0?", "-c", "copy"]);
                }
                ResolvedMediaInput::SeparateAv { .. } => {
                    cmd.args([
                        "-c:v",
                        "copy",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "128k",
                        "-ar",
                        "44100",
                        "-ac",
                        "2",
                        "-af",
                        "aresample=async=1:first_pts=0",
                    ]);
                }
                ResolvedMediaInput::AudioOnly { .. } => {
                    cmd.args([
                        "-vn", "-c:a", "aac", "-b:a", "160k", "-ac", "2", "-ar", "44100",
                    ]);
                }
            }

            cmd.args([
                "-f",
                "hls",
                "-hls_time",
                HLS_SEGMENT_SECONDS,
                "-hls_init_time",
                "1",
                "-hls_list_size",
                "0",
                "-hls_flags",
                "omit_endlist+program_date_time+temp_file",
                "-hls_segment_filename",
            ]);

            cmd.arg(segment_pattern.to_string_lossy().to_string());
            cmd.arg(item_manifest.to_string_lossy().to_string());

            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::inherit());

            {
                let mut guard = self.producer_now.lock().await;
                guard.last_error = Some("Démarrage FFmpeg HLS en cours".to_string());
            }

            live_audit(
                &self.root_dir,
                format!(
                    "ITEM_FFMPEG_START index={} title={} manifest={} segment_pattern={}",
                    item.index,
                    item.title,
                    item_manifest.display(),
                    segment_pattern.display()
                ),
            )
            .await;

            let child = cmd.spawn().context("lancement ffmpeg HLS item")?;

            {
                let mut guard = self.ffmpeg_child.lock().await;
                *guard = Some(ActiveFfmpegChild {
                    generation: worker_generation,
                    item_id: item.item_id.clone(),
                    child,
                });
            }

            // Métadonnées complètes en arrière-plan seulement après démarrage FFmpeg.
            // Elles ne doivent jamais retarder les premiers segments HLS.

            let mut imported_segments = 0_usize;
            let mut prewarm_started = false;

            loop {
                sleep(Duration::from_millis(500)).await;

                if self.current_generation().await != worker_generation {
                    eprintln!(
                        "TROOZN_LIVE_WORKER_STALE_EXIT_IN_FFMPEG generation={} item={}",
                        worker_generation, item.index
                    );
                    self.stop_ffmpeg_for_generation(worker_generation).await;
                    return Ok(());
                }

                {
                    let mut producer = self.producer_now.lock().await;
                    if producer.item_id == item.item_id && producer.item_started_at > 0 {
                        producer.position =
                            unix_timestamp().saturating_sub(producer.item_started_at);
                    }
                }

                let new_count = self
                    .import_item_manifest_incremental(item.index, &item_manifest, appended_any)
                    .await
                    .unwrap_or(imported_segments);

                if new_count > imported_segments {
                    imported_segments = new_count;
                    appended_any = true;
                    self.rewrite_master_playlist(false).await.ok();
                    self.refresh_buffer_status().await;
                    self.maybe_cleanup_live_files(false).await.ok();
                    self.maybe_finish_startup_boost(item.index, imported_segments)
                        .await;

                    if !prewarm_started {
                        prewarm_started = true;
                        self.clone().spawn_prewarm_after(item.index).await;
                    }
                }

                let finished = {
                    let mut guard = self.ffmpeg_child.lock().await;

                    match guard.as_mut() {
                        Some(active)
                            if active.generation == worker_generation
                                && active.item_id == item.item_id =>
                        {
                            match active.child.try_wait() {
                                Ok(Some(status)) => {
                                    eprintln!(
                                        "TROOZN_LIVE_FFMPEG_DONE index={} title={} status={status}",
                                        item.index, item.title
                                    );

                                    self.maybe_refill_playlist_queue(item.index).await;
                                    live_audit(
                                    &self.root_dir,
                                    format!(
                                        "ITEM_FFMPEG_DONE index={} title={} status={} ts_files={} manifest_lines={}",
                                        item.index,
                                        item.title,
                                        status,
                                        count_item_ts_files(&self.root_dir, item.index),
                                        count_manifest_ts_lines(&item_manifest)
                                    ),
                                )
                                .await;
                                    *guard = None;
                                    true
                                }
                                Ok(None) => false,
                                Err(err) => {
                                    eprintln!(
                                    "TROOZN_LIVE_FFMPEG_WAIT_ERROR index={} title={} state={err:?}",
                                    item.index, item.title
                                );
                                    *guard = None;
                                    true
                                }
                            }
                        }
                        Some(active) => {
                            eprintln!(
                                "TROOZN_LIVE_FFMPEG_OWNERSHIP_CHANGED worker_generation={} active_generation={} item={}",
                                worker_generation,
                                active.generation,
                                item.index
                            );
                            return Ok(());
                        }
                        None => true,
                    }
                };

                if finished {
                    if self.current_generation().await != worker_generation {
                        eprintln!(
                            "TROOZN_LIVE_WORKER_STALE_SKIP_FINAL_IMPORT generation={} item={}",
                            worker_generation, item.index
                        );
                        return Ok(());
                    }

                    let final_count = self
                        .import_item_manifest_incremental(item.index, &item_manifest, appended_any)
                        .await
                        .unwrap_or(imported_segments);

                    if final_count > imported_segments {
                        appended_any = true;
                    }

                    self.rewrite_master_playlist(false).await.ok();
                    self.refresh_buffer_status().await;
                    self.maybe_cleanup_live_files(true).await.ok();
                    break;
                }
            }
        }

        // Worker persistant : il attend de nouveaux items jusqu'à génération obsolète.
    }

    async fn enrich_item_metadata(&self, item: &TrooznLiveItem) -> TrooznLiveItem {
        let meta = match extract_full_video_metadata(&item.source_url).await {
            Ok(meta) => meta,
            Err(err) => {
                eprintln!(
                    "TROOZN_LIVE_METADATA_FAILED index={} title={} state={err:?}",
                    item.index, item.title
                );
                return item.clone();
            }
        };

        let mut enriched = item.clone();

        if let Some(title) = meta.title {
            enriched.title = title;
        }

        if meta.webpage_url.is_some() {
            enriched.webpage_url = meta.webpage_url;
        }

        if meta.duration.is_some() {
            enriched.duration = meta.duration;
        }

        if meta.thumbnail.is_some() {
            enriched.thumbnail = meta.thumbnail;
        }

        if meta.channel.is_some() {
            enriched.channel = meta.channel;
        }

        if meta.description.is_some() {
            enriched.description = meta.description;
        }

        if meta.upload_date.is_some() {
            enriched.upload_date = meta.upload_date;
        }

        if meta.uploader.is_some() {
            enriched.uploader = meta.uploader;
        }

        {
            let mut queue = self.queue.lock().await;

            if let Some(slot) = queue.iter_mut().find(|q| q.item_id == item.item_id) {
                *slot = enriched.clone();
            }
        }

        enriched
    }

    async fn wait_until_future_buffer_needed(&self, next_item_index: usize) {
        if next_item_index <= STARTUP_BOOST_PREPARE_THROUGH_ITEM_INDEX {
            eprintln!(
                "TROOZN_LIVE_PRODUCER_WAIT_SKIP_STARTUP next_item={} prepare_through={}",
                next_item_index, STARTUP_BOOST_PREPARE_THROUGH_ITEM_INDEX
            );
            return;
        }

        let started = unix_timestamp();

        loop {
            self.maybe_cleanup_live_files(false).await.ok();

            let (buffer_seconds, buffer_segments) = self.current_buffer_stats().await;
            let usage = self.live_dir_usage().await.unwrap_or_default();
            let too_far_ahead = buffer_seconds > LIVE_MAX_PRODUCER_AHEAD_SECONDS;
            let disk_pressure = usage.bytes > LIVE_MAX_DIR_BYTES;

            if !too_far_ahead && !disk_pressure {
                return;
            }

            if unix_timestamp().saturating_sub(started) >= LIVE_AHEAD_WAIT_MAX_SECONDS {
                eprintln!(
                    "TROOZN_LIVE_PRODUCER_WAIT_TIMEOUT next_item={} buffer_seconds={:.1} buffer_segments={} usage_bytes={}",
                    next_item_index,
                    buffer_seconds,
                    buffer_segments,
                    usage.bytes
                );
                return;
            }

            eprintln!(
                "TROOZN_LIVE_PRODUCER_WAIT next_item={} buffer_seconds={:.1} buffer_segments={} usage_bytes={} max_bytes={}",
                next_item_index,
                buffer_seconds,
                buffer_segments,
                usage.bytes,
                LIVE_MAX_DIR_BYTES
            );

            sleep(Duration::from_millis(1000)).await;

            let (buffer_seconds, _) = self.current_buffer_stats().await;
            let usage = self.live_dir_usage().await.unwrap_or_default();

            if buffer_seconds <= LIVE_RESUME_PRODUCER_AHEAD_SECONDS
                && usage.bytes <= LIVE_TARGET_DIR_BYTES
            {
                return;
            }
        }
    }

    async fn maybe_finish_startup_boost(&self, item_index: usize, imported_segments: usize) {
        if !ytdlp_startup_boost_active() {
            return;
        }

        let queue_len = self.queue.lock().await.len();
        let playlist_ready =
            queue_len > 1 && item_index >= STARTUP_BOOST_PREPARE_THROUGH_ITEM_INDEX;
        let single_ready =
            queue_len <= 1 && imported_segments >= STARTUP_BOOST_SINGLE_ITEM_READY_SEGMENTS;

        if !playlist_ready && !single_ready {
            return;
        }

        set_ytdlp_startup_boost(false, "initial-continuity-ready");

        live_audit(
            &self.root_dir,
            format!(
                "BOOST_END item={} imported_segments={} queue_len={} reason=initial-continuity-ready",
                item_index, imported_segments, queue_len
            ),
        )
        .await;
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
        let existing_for_item = entries
            .iter()
            .filter(|e| e.item_index == item_index)
            .count();

        if parsed_count <= existing_for_item {
            return Ok(existing_for_item);
        }

        for entry in parsed.into_iter().skip(existing_for_item) {
            entries.push(entry);
        }

        Ok(parsed_count)
    }

    async fn render_playback_playlist_from_anchor(&self) -> String {
        let anchor = *self.playback_anchor_item.lock().await;
        let entries = self.master_entries.lock().await.clone();
        let media_sequence = *self.media_sequence_base.lock().await;
        let discontinuity_sequence = *self.discontinuity_sequence_base.lock().await;

        let filtered: Vec<MasterEntry> = entries
            .into_iter()
            .filter(|entry| entry.item_index >= anchor)
            .collect();

        let selected = if filtered.is_empty() {
            self.master_entries.lock().await.clone()
        } else {
            filtered
        };

        let target_duration = selected
            .iter()
            .filter_map(|entry| entry.duration.parse::<f64>().ok())
            .map(|duration| duration.ceil() as u64)
            .max()
            .unwrap_or(6)
            .max(2);

        let mut out = String::new();

        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:3\n");
        out.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", target_duration));
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n"));
        out.push_str(&format!(
            "#EXT-X-DISCONTINUITY-SEQUENCE:{discontinuity_sequence}\n"
        ));
        out.push_str("#EXT-X-START:TIME-OFFSET=0,PRECISE=YES\n");

        let mut last_item: Option<usize> = None;

        for entry in selected.iter() {
            if last_item.map(|v| v != entry.item_index).unwrap_or(false) {
                out.push_str("#EXT-X-DISCONTINUITY\n");
            }

            last_item = Some(entry.item_index);

            out.push_str(&format!("#EXTINF:{:.6},\n", entry.duration));
            out.push_str(&format!("{}\n", entry.segment));
        }

        out
    }

    async fn maybe_cleanup_live_files(&self, force: bool) -> anyhow::Result<CleanupStats> {
        let now = unix_timestamp();

        {
            let mut last = self.last_cleanup_at.lock().await;

            if !force && now.saturating_sub(*last) < LIVE_CLEANUP_INTERVAL_SECONDS {
                return Ok(CleanupStats::default());
            }

            *last = now;
        }

        let mut stats = self
            .prune_served_master_entries(LIVE_KEEP_BEHIND_SECONDS)
            .await?;

        let usage = self.live_dir_usage().await.unwrap_or_default();
        let pressure = usage.bytes > LIVE_MAX_DIR_BYTES;

        if pressure {
            let extra = self
                .prune_served_master_entries(LIVE_PRESSURE_KEEP_BEHIND_SECONDS)
                .await?;
            stats.merge(extra);
        }

        let extra = self.cleanup_unreferenced_live_files(pressure).await?;
        stats.merge(extra);

        if stats.removed_files > 0 || stats.removed_playlist_entries > 0 {
            self.rewrite_master_playlist(false).await.ok();
            self.refresh_buffer_status().await;

            let usage_after = self.live_dir_usage().await.unwrap_or_default();
            eprintln!(
                "TROOZN_LIVE_CLEANUP_DONE removed_files={} removed_bytes={} removed_entries={} usage_bytes={} ts_files={}",
                stats.removed_files,
                stats.removed_bytes,
                stats.removed_playlist_entries,
                usage_after.bytes,
                usage_after.ts_files
            );

            if usage_after.bytes > LIVE_MAX_DIR_BYTES {
                eprintln!(
                    "TROOZN_LIVE_CLEANUP_PRESSURE_REMAINS usage_bytes={} max_bytes={} protected_ahead=true",
                    usage_after.bytes,
                    LIVE_MAX_DIR_BYTES
                );
            }
        }

        Ok(stats)
    }

    async fn prune_served_master_entries(
        &self,
        keep_behind_seconds: f64,
    ) -> anyhow::Result<CleanupStats> {
        let Some((served_item, served_segment)) = *self.last_served_segment.lock().await else {
            return Ok(CleanupStats::default());
        };

        let (removed, remaining_items) = {
            let mut entries = self.master_entries.lock().await;

            let Some(served_pos) = entries.iter().position(|entry| {
                parse_item_segment_name(&entry.segment)
                    .map(|(item, segment)| item == served_item && segment == served_segment)
                    .unwrap_or(false)
            }) else {
                return Ok(CleanupStats::default());
            };

            let current_item_first_pos = entries
                .iter()
                .position(|entry| entry.item_index == served_item)
                .unwrap_or(served_pos);
            let mut keep_from = current_item_first_pos;
            let mut retained_seconds = 0.0_f64;

            for idx in (0..current_item_first_pos).rev() {
                retained_seconds += entry_duration_seconds(&entries[idx]);
                keep_from = idx;

                if retained_seconds >= keep_behind_seconds {
                    break;
                }
            }

            if keep_from == 0 {
                return Ok(CleanupStats::default());
            }

            let removed = entries.drain(0..keep_from).collect::<Vec<_>>();
            let remaining_items = entries
                .iter()
                .map(|entry| entry.item_index)
                .collect::<HashSet<_>>();

            (removed, remaining_items)
        };

        if removed.is_empty() {
            return Ok(CleanupStats::default());
        }

        {
            let mut media_sequence = self.media_sequence_base.lock().await;
            *media_sequence = media_sequence.saturating_add(removed.len() as u64);
        }

        {
            let removed_discontinuities = removed
                .iter()
                .filter(|entry| entry.discontinuity_before)
                .count() as u64;
            let mut discontinuity_sequence = self.discontinuity_sequence_base.lock().await;
            *discontinuity_sequence =
                discontinuity_sequence.saturating_add(removed_discontinuities);
        }

        let mut stats = CleanupStats {
            removed_playlist_entries: removed.len(),
            ..Default::default()
        };
        let mut removed_items = HashSet::new();

        for entry in removed {
            removed_items.insert(entry.item_index);
            let path = self.root_dir.join(entry.segment);

            if let Some(bytes) = remove_live_file(&path).await {
                stats.removed_files += 1;
                stats.removed_bytes = stats.removed_bytes.saturating_add(bytes);
            }
        }

        for item_index in removed_items {
            if remaining_items.contains(&item_index) {
                continue;
            }

            let path = self.root_dir.join(format!("item-{item_index:04}.m3u8"));

            if let Some(bytes) = remove_live_file(&path).await {
                stats.removed_files += 1;
                stats.removed_bytes = stats.removed_bytes.saturating_add(bytes);
            }
        }

        Ok(stats)
    }

    async fn cleanup_unreferenced_live_files(
        &self,
        pressure: bool,
    ) -> anyhow::Result<CleanupStats> {
        let entries = self.master_entries.lock().await.clone();
        let producer_index = self.producer_now.lock().await.index;
        let mut protected = HashSet::new();

        protected.insert("index.m3u8".to_string());
        protected.insert("playlist-youtube.m3u8".to_string());
        protected.insert("audit.log".to_string());

        for entry in entries {
            protected.insert(entry.segment);
            protected.insert(format!("item-{:04}.m3u8", entry.item_index));
        }

        if producer_index > 0 {
            protected.insert(format!("item-{producer_index:04}.m3u8"));
        }

        let mut rd = fs::read_dir(&self.root_dir).await?;
        let mut stats = CleanupStats::default();
        let mut pressure_candidates: Vec<(std::time::SystemTime, u64, PathBuf)> = Vec::new();

        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let Some(name) = path.file_name().map(|v| v.to_string_lossy().to_string()) else {
                continue;
            };

            let is_live_file =
                name.ends_with(".ts") || name.ends_with(".m3u8") || name.ends_with(".tmp");

            if !is_live_file || protected.contains(&name) {
                continue;
            }

            let Ok(meta) = entry.metadata().await else {
                continue;
            };

            let size = meta.len();
            let modified = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let age = file_age_seconds(modified);
            let is_current_producer_file = producer_index > 0
                && name.starts_with(&format!("item-{producer_index:04}-"))
                && age < LIVE_ORPHAN_GRACE_SECONDS;

            if is_current_producer_file {
                continue;
            }

            let stale_tmp = name.ends_with(".tmp") && age >= LIVE_TMP_GRACE_SECONDS;
            let stale_orphan = age >= LIVE_ORPHAN_GRACE_SECONDS;

            if stale_tmp || stale_orphan {
                if let Some(bytes) = remove_live_file(&path).await {
                    stats.removed_files += 1;
                    stats.removed_bytes = stats.removed_bytes.saturating_add(bytes);
                }
                continue;
            }

            if pressure {
                pressure_candidates.push((modified, size, path));
            }
        }

        if pressure {
            pressure_candidates.sort_by_key(|(modified, _, _)| *modified);

            let mut usage = self.live_dir_usage().await.unwrap_or_default().bytes;

            for (_, size, path) in pressure_candidates {
                if usage <= LIVE_TARGET_DIR_BYTES {
                    break;
                }

                if let Some(bytes) = remove_live_file(&path).await {
                    stats.removed_files += 1;
                    stats.removed_bytes = stats.removed_bytes.saturating_add(bytes);
                    usage = usage.saturating_sub(size.max(bytes));
                }
            }
        }

        Ok(stats)
    }

    async fn live_dir_usage(&self) -> anyhow::Result<LiveDirUsage> {
        let mut usage = LiveDirUsage::default();
        let mut rd = fs::read_dir(&self.root_dir).await?;

        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();

            let Ok(meta) = entry.metadata().await else {
                continue;
            };

            if !meta.is_file() {
                continue;
            }

            usage.files += 1;
            usage.bytes = usage.bytes.saturating_add(meta.len());

            if path
                .file_name()
                .map(|name| name.to_string_lossy().ends_with(".ts"))
                .unwrap_or(false)
            {
                usage.ts_files += 1;
            }
        }

        Ok(usage)
    }

    async fn rewrite_master_playlist(&self, ended: bool) -> anyhow::Result<()> {
        let entries = self.master_entries.lock().await.clone();
        let media_sequence = *self.media_sequence_base.lock().await;
        let discontinuity_sequence = *self.discontinuity_sequence_base.lock().await;
        let index_path = self.root_dir.join("index.m3u8");

        let mut target_duration = HLS_SEGMENT_SECONDS.parse::<u64>().unwrap_or(2).max(2);

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
        out.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));
        out.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{media_sequence}\n"));
        out.push_str(&format!(
            "#EXT-X-DISCONTINUITY-SEQUENCE:{discontinuity_sequence}\n"
        ));
        out.push_str("#EXT-X-START:TIME-OFFSET=0,PRECISE=YES\n");

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

        write_text_atomic(&index_path, out).await?;
        Ok(())
    }

    async fn note_segment_served(&self, relative: &str) {
        let Some((item_index, segment_number)) = parse_item_segment_name(relative) else {
            return;
        };

        {
            let mut served = self.last_served_segment.lock().await;
            *served = Some((item_index, segment_number));
        }

        let queue = self.queue.lock().await.clone();
        let Some(item) = queue.iter().find(|item| item.index == item_index).cloned() else {
            return;
        };

        let entries = self.master_entries.lock().await.clone();

        let mut position_f64 = 0.0_f64;

        for entry in entries
            .iter()
            .filter(|entry| entry.item_index == item_index)
        {
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
        let public_hls_url = self.current_public_hls_url().await;

        let now = TrooznLiveNow {
            state: "playing".to_string(),
            title: item.title.clone(),
            source_url: item.source_url.clone(),
            hls_url: public_hls_url,
            item_id: item.item_id.clone(),
            index: item.index,
            position: position_f64.floor() as u64,
            duration: item.duration,
            thumbnail: item.thumbnail.clone(),
            channel: item.channel.clone(),
            description: item.description.clone(),
            upload_date: item.upload_date.clone(),
            uploader: item.uploader.clone(),
            started_at: unix_timestamp(),
            item_started_at: unix_timestamp().saturating_sub(position_f64.floor() as u64),
            next_title,
            last_error: None,
            media_type: infer_media_type_from_url(&item.source_url)
                .or(Some("video"))
                .map(str::to_string),
            buffer_seconds: None,
            buffer_segments: None,
        };

        {
            let mut guard = self.playback_now.lock().await;
            *guard = now;
        }

        self.refresh_buffer_status().await;
        self.maybe_cleanup_live_files(false).await.ok();

        eprintln!(
            "TROOZN_LIVE_SEGMENT_SERVED item={} segment={} file={}",
            item_index, segment_number, relative
        );
    }

    pub async fn current_now(&self) -> TrooznLiveNow {
        self.refresh_buffer_status().await;
        let playback = self.playback_now.lock().await.clone();

        if playback.state == "playing" && playback.index > 0 {
            return playback;
        }

        self.producer_now.lock().await.clone()
    }

    pub async fn producer_now(&self) -> TrooznLiveNow {
        self.refresh_buffer_status().await;
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

        let duration = pending_duration
            .take()
            .unwrap_or_else(|| "4.000000".to_string());
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

fn entry_duration_seconds(entry: &MasterEntry) -> f64 {
    entry
        .duration
        .parse::<f64>()
        .unwrap_or_else(|_| HLS_SEGMENT_SECONDS.parse::<f64>().unwrap_or(2.0).max(1.0))
}

fn file_age_seconds(modified: std::time::SystemTime) -> u64 {
    SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default()
        .as_secs()
}

async fn remove_live_file(path: &Path) -> Option<u64> {
    let bytes = fs::metadata(path).await.map(|meta| meta.len()).unwrap_or(0);

    match fs::remove_file(path).await {
        Ok(_) => {
            eprintln!(
                "TROOZN_LIVE_CLEANUP_REMOVE file={} bytes={}",
                path.display(),
                bytes
            );
            Some(bytes)
        }
        Err(err) => {
            eprintln!(
                "TROOZN_LIVE_CLEANUP_REMOVE_FAILED file={} state={err:?}",
                path.display()
            );
            None
        }
    }
}

async fn write_text_atomic(path: &Path, content: String) -> anyhow::Result<()> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "playlist.m3u8".to_string());
    let tmp_path =
        path.with_file_name(format!("{file_name}.{}.{}.tmp", std::process::id(), suffix));

    fs::write(&tmp_path, content).await?;

    if let Err(err) = fs::rename(&tmp_path, path).await {
        fs::remove_file(&tmp_path).await.ok();
        return Err(err.into());
    }

    Ok(())
}

async fn write_empty_master_playlist(index_path: &Path) -> anyhow::Result<()> {
    let target_duration = HLS_SEGMENT_SECONDS.parse::<u64>().unwrap_or(2).max(2);
    let content = format!(
        "\
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:{target_duration}
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-START:TIME-OFFSET=0,PRECISE=YES
"
    );

    write_text_atomic(index_path, content).await?;
    Ok(())
}

fn is_probably_playlist_url(source_url: &str) -> bool {
    let lower = source_url.to_lowercase();

    lower.contains("list=")
        || lower.contains("/playlist?")
        || lower.contains("youtube.com/playlist")
        || lower.contains("/sets/")
        || lower.contains("/album/")
        || lower.contains("/channel/")
        || lower.contains("/playlist/")
}

fn is_youtube_url(source_url: &str) -> bool {
    let lower = source_url.to_lowercase();
    lower.contains("youtube.com/") || lower.contains("youtu.be/")
}

fn is_youtube_mix_list(source_url: &str) -> bool {
    let Some(list) = query_param(source_url, "list") else {
        return false;
    };

    list.starts_with("RD")
        || list.starts_with("RDEM")
        || list.starts_with("RDMM")
        || list.starts_with("RDGM")
}

fn normalize_rd_playlist_to_watch_url(source_url: &str) -> Option<String> {
    let list = query_param(source_url, "list")?;

    if let Some(video_id) = list.strip_prefix("RDMM") {
        if looks_like_youtube_id(video_id) {
            return Some(format!(
                "https://www.youtube.com/watch?v={}&list={}&start_radio=1",
                video_id, list
            ));
        }
    }

    if let Some(video_id) = list.strip_prefix("RD") {
        if looks_like_youtube_id(video_id) {
            return Some(format!(
                "https://www.youtube.com/watch?v={}&list={}&start_radio=1",
                video_id, list
            ));
        }
    }

    None
}

fn fallback_single_item_from_url(source_url: &str) -> Option<TrooznLiveItem> {
    let watch_url = if let Some(video_id) = extract_youtube_video_id(source_url) {
        format!("https://www.youtube.com/watch?v={}", video_id)
    } else if source_url.starts_with("http://") || source_url.starts_with("https://") {
        source_url.to_string()
    } else {
        return None;
    };

    eprintln!(
        "TROOZN_LIVE_FALLBACK_SINGLE source_url={} watch_url={}",
        source_url, watch_url
    );

    Some(TrooznLiveItem {
        item_id: item_id_for_url(&watch_url),
        index: 1,
        title: fallback_title_for_url(&watch_url),
        source_url: watch_url.clone(),
        webpage_url: Some(watch_url),
        duration: None,
        thumbnail: None,
        channel: None,
        description: None,
        upload_date: None,
        uploader: None,
    })
}

fn extract_youtube_video_id(source_url: &str) -> Option<String> {
    // Cas standard: watch?v=VIDEO_ID
    if let Some(v) = query_param(source_url, "v") {
        if looks_like_youtube_id(&v) {
            return Some(v);
        }
    }

    // Cas court: youtu.be/VIDEO_ID
    if let Some(pos) = source_url.find("youtu.be/") {
        let rest = &source_url[pos + "youtu.be/".len()..];
        let id = rest
            .split(|c| c == '?' || c == '&' || c == '/' || c == '#')
            .next()
            .unwrap_or("")
            .to_string();

        if looks_like_youtube_id(&id) {
            return Some(id);
        }
    }

    // Cas embed/shorts: /embed/VIDEO_ID ou /shorts/VIDEO_ID
    for marker in ["/embed/", "/shorts/"] {
        if let Some(pos) = source_url.find(marker) {
            let rest = &source_url[pos + marker.len()..];
            let id = rest
                .split(|c| c == '?' || c == '&' || c == '/' || c == '#')
                .next()
                .unwrap_or("")
                .to_string();

            if looks_like_youtube_id(&id) {
                return Some(id);
            }
        }
    }

    // Cas radio/mix simple : list=RDVIDEO_ID ou list=RDMMVIDEO_ID
    if let Some(list) = query_param(source_url, "list") {
        let candidates = [list.strip_prefix("RDMM"), list.strip_prefix("RD")];

        for candidate in candidates.into_iter().flatten() {
            let id = candidate
                .split(|c| c == '?' || c == '&' || c == '/' || c == '#')
                .next()
                .unwrap_or("")
                .to_string();

            if looks_like_youtube_id(&id) {
                return Some(id);
            }
        }

        eprintln!("TROOZN_LIVE_FALLBACK_NO_VIDEO_ID_IN_LIST list={}", list);
    }

    None
}

fn query_param(source_url: &str, key: &str) -> Option<String> {
    let query = source_url
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or(source_url);

    for part in query.split('&') {
        let Some((k, v)) = part.split_once('=') else {
            continue;
        };

        if k == key {
            return Some(percent_decode_minimal(v));
        }
    }

    None
}

fn fallback_title_for_url(source_url: &str) -> String {
    let without_query = source_url
        .split(['?', '#'])
        .next()
        .unwrap_or(source_url)
        .trim_end_matches('/');
    let last = without_query
        .rsplit('/')
        .next()
        .unwrap_or("media")
        .replace(['-', '_'], " ");
    if last.trim().is_empty() {
        "Media TROOZN".to_string()
    } else {
        last
    }
}

fn percent_decode_minimal(input: &str) -> String {
    let mut out = String::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v as char);
                    i += 3;
                    continue;
                }
            }
        }

        if bytes[i] == b'+' {
            out.push(' ');
        } else {
            out.push(bytes[i] as char);
        }

        i += 1;
    }

    out
}

fn looks_like_youtube_id(value: &str) -> bool {
    value.len() == 11
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

async fn extract_youtube_items_range_with_retry(
    source_url: &str,
    start: usize,
    end: usize,
) -> anyhow::Result<Vec<TrooznLiveItem>> {
    match extract_youtube_items_range(source_url, start, end).await {
        Ok(items) => Ok(items),
        Err(err) => {
            eprintln!(
                "TROOZN_LIVE_RANGE_EXTRACT_FAIL start={} end={} state={err:?}",
                start, end
            );
            Err(err)
        }
    }
}

async fn extract_youtube_items_range(
    source_url: &str,
    start: usize,
    end: usize,
) -> anyhow::Result<Vec<TrooznLiveItem>> {
    let mut cmd = Command::new(YTDLP_BIN);

    cmd.args([
        "--flat-playlist",
        "--dump-single-json",
        "--no-warnings",
        "--force-ipv4",
        "--socket-timeout",
        "20",
        "--playlist-start",
        &start.to_string(),
        "--playlist-end",
        &end.to_string(),
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_RANGE_EXTRACT_START start={} end={} url={}",
        start, end, source_url
    );

    let output = run_ytdlp_output(cmd, "yt-dlp range extract", 45).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("yt-dlp range extract failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let root: serde_json::Value =
        serde_json::from_str(&stdout).context("parse yt-dlp range json")?;

    let mut items = Vec::new();

    if let Some(entries) = root.get("entries").and_then(|v| v.as_array()) {
        for entry in entries {
            if entry.is_null() {
                continue;
            }

            if let Some(item) = troozn_live_item_from_ytdlp_entry(entry) {
                items.push(item);
            }
        }
    }

    eprintln!(
        "TROOZN_LIVE_RANGE_EXTRACT_DONE start={} end={} count={}",
        start,
        end,
        items.len()
    );

    Ok(items)
}

fn stable_item_id(source_url: &str) -> String {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    hasher.update(source_url.as_bytes());
    let digest = hasher.finalize();

    format!("{:x}", digest).chars().take(16).collect::<String>()
}

fn troozn_live_item_from_ytdlp_entry(entry: &serde_json::Value) -> Option<TrooznLiveItem> {
    let id = entry
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();

    let url = entry
        .get("url")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();

    let webpage_url = entry
        .get("webpage_url")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);

    let source_url = if let Some(webpage_url) = webpage_url.clone() {
        webpage_url
    } else if url.starts_with("http://") || url.starts_with("https://") {
        url
    } else if !id.is_empty() && !id.starts_with("http://") && !id.starts_with("https://") {
        if looks_like_youtube_id(&id) {
            format!("https://www.youtube.com/watch?v={}", id)
        } else {
            return None;
        }
    } else if !url.is_empty() {
        if looks_like_youtube_id(&url) {
            format!("https://www.youtube.com/watch?v={}", url)
        } else {
            return None;
        }
    } else {
        return None;
    };

    let title = entry
        .get("title")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("YouTube")
        .to_string();

    let duration = entry
        .get("duration")
        .and_then(serde_json::Value::as_f64)
        .map(|v| v as u64);

    let thumbnail = entry
        .get("thumbnail")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);

    let channel = entry
        .get("channel")
        .and_then(serde_json::Value::as_str)
        .or_else(|| entry.get("uploader").and_then(serde_json::Value::as_str))
        .map(ToString::to_string);

    Some(TrooznLiveItem {
        item_id: stable_item_id(&source_url),
        index: 0,
        title,
        source_url,
        webpage_url,
        duration,
        thumbnail,
        channel,
        description: None,
        upload_date: None,
        uploader: None,
    })
}

async fn extract_youtube_items_with_retry(
    source_url: &str,
    limit: usize,
) -> anyhow::Result<Vec<TrooznLiveItem>> {
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=1 {
        match extract_youtube_items(source_url, limit).await {
            Ok(items) if !items.is_empty() => {
                if attempt > 1 {
                    eprintln!(
                        "TROOZN_LIVE_PLAYLIST_RETRY_OK attempt={} count={}",
                        attempt,
                        items.len()
                    );
                }

                return Ok(items);
            }
            Ok(_) => {
                eprintln!(
                    "TROOZN_LIVE_PLAYLIST_EMPTY attempt={} source_url={}",
                    attempt, source_url
                );
            }
            Err(err) => {
                eprintln!(
                    "TROOZN_LIVE_PLAYLIST_EXTRACT_RETRY_FAILED attempt={} source_url={} state={err:?}",
                    attempt,
                    source_url
                );
                last_error = Some(err);
            }
        }

        sleep(Duration::from_millis(700 * attempt)).await;
    }

    match last_error {
        Some(err) => Err(err),
        None => anyhow::bail!("Extraction playlist vide après retries"),
    }
}

async fn extract_youtube_items(
    source_url: &str,
    limit: usize,
) -> anyhow::Result<Vec<TrooznLiveItem>> {
    let mut cmd = Command::new(YTDLP_BIN);

    add_ytdlp_common_args(&mut cmd).await;

    let scan_limit = if is_youtube_url(source_url) && is_probably_playlist_url(source_url) {
        limit
            .saturating_add(PLAYLIST_ACTIVE_SCAN_EXTRA)
            .min(PLAYLIST_ACTIVE_SCAN_MAX)
    } else {
        limit
    };

    cmd.args([
        "--flat-playlist",
        "--no-warnings",
        "--playlist-end",
        &scan_limit.to_string(),
        "-J",
        source_url,
    ]);

    let output = run_ytdlp_output(
        cmd,
        "yt-dlp playlist",
        YTDLP_PLAYLIST_EXTRACT_TIMEOUT_SECONDS,
    )
    .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("yt-dlp playlist a échoué: {}", stderr.trim());
    }

    let root: Value = serde_json::from_slice(&output.stdout).context("parse yt-dlp JSON")?;

    let mut out = Vec::new();

    if let Some(entries) = root.get("entries").and_then(Value::as_array) {
        for (idx, entry) in entries.iter().take(scan_limit).enumerate() {
            if let Some(item) = item_from_ytdlp_value(idx + 1, entry) {
                out.push(item);
            }
        }
    } else if let Some(item) = item_from_ytdlp_value(1, &root) {
        out.push(item);
    }

    let out = filter_active_youtube_items(out, limit).await;

    eprintln!(
        "TROOZN_LIVE_PLAYLIST_ACTIVE_FILTER source_url={} wanted={} scanned={} active={}",
        source_url,
        limit,
        scan_limit,
        out.len()
    );

    Ok(out)
}

async fn filter_active_youtube_items(
    items: Vec<TrooznLiveItem>,
    wanted: usize,
) -> Vec<TrooznLiveItem> {
    if items.is_empty() {
        return items;
    }

    let original = items.clone();
    let total = items.len();
    let mut active = Vec::new();
    let mut rejected_ids = HashSet::new();
    let mut iter = items.into_iter();
    let target_active = wanted.min(PLAYLIST_INITIAL_ACTIVE_TARGET).max(1);

    while active.len() < target_active {
        let chunk = iter
            .by_ref()
            .take(YOUTUBE_QUICK_VALIDATE_CONCURRENCY)
            .collect::<Vec<_>>();

        if chunk.is_empty() {
            break;
        }

        let handles = chunk
            .into_iter()
            .map(|item| {
                tokio::spawn(async move {
                    let is_active = if is_youtube_url(&item.source_url) {
                        quick_validate_youtube_item(&item).await
                    } else {
                        true
                    };

                    (item, is_active)
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            match handle.await {
                Ok((item, true)) => active.push(item),
                Ok((item, false)) => {
                    rejected_ids.insert(item.item_id.clone());
                    eprintln!(
                        "TROOZN_LIVE_PLAYLIST_SKIP_INACTIVE index={} title={} url={}",
                        item.index, item.title, item.source_url
                    );
                }
                Err(err) => {
                    eprintln!("TROOZN_LIVE_PLAYLIST_VALIDATE_JOIN_ERROR state={err:?}");
                }
            }
        }

        if active.len() < target_active {
            sleep(Duration::from_millis(YOUTUBE_QUICK_VALIDATE_BATCH_PAUSE_MS)).await;
        }
    }

    let mut merged = if active.is_empty() {
        eprintln!(
            "TROOZN_LIVE_PLAYLIST_VALIDATE_NO_FAST_ACTIVE fallback=unvalidated wanted={}",
            wanted
        );
        let filtered = original
            .iter()
            .filter(|item| !rejected_ids.contains(&item.item_id))
            .take(wanted)
            .cloned()
            .collect::<Vec<_>>();

        if filtered.is_empty() {
            original.into_iter().take(wanted).collect::<Vec<_>>()
        } else {
            filtered
        }
    } else {
        active
            .into_iter()
            .chain(iter)
            .take(wanted)
            .collect::<Vec<_>>()
    };

    for (idx, item) in merged.iter_mut().enumerate() {
        item.index = idx + 1;
    }

    eprintln!(
        "TROOZN_LIVE_PLAYLIST_VALIDATE_DONE scanned={} kept={} wanted={} target_active={}",
        total,
        merged.len(),
        wanted,
        target_active
    );

    merged
}

async fn quick_validate_youtube_item(item: &TrooznLiveItem) -> bool {
    let mut cmd = Command::new(YTDLP_BIN);
    add_ytdlp_cookies_if_available(&mut cmd);

    cmd.args([
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--force-ipv4",
        "--socket-timeout",
        "5",
        "--retries",
        "0",
        "--fragment-retries",
        "0",
        "-f",
        YTDLP_YOUTUBE_VALIDATE_FORMAT,
        "-g",
        &item.source_url,
    ]);

    let output = match run_ytdlp_output(
        cmd,
        "yt-dlp quick validate",
        YOUTUBE_QUICK_VALIDATE_TIMEOUT_SECONDS,
    )
    .await
    {
        Ok(output) => output,
        Err(err) => {
            eprintln!(
                "TROOZN_LIVE_PLAYLIST_VALIDATE_ERROR index={} title={} state={err:?}",
                item.index, item.title
            );
            return false;
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = if stderr.trim().is_empty() {
        stdout.to_string()
    } else {
        format!("{}\n{}", stdout.trim(), stderr.trim())
    };

    if output.status.success()
        && stdout
            .lines()
            .any(|line| line.starts_with("http://") || line.starts_with("https://"))
    {
        return true;
    }

    if is_youtube_terminal_unavailable_error(&combined) {
        eprintln!(
            "TROOZN_LIVE_PLAYLIST_VALIDATE_UNAVAILABLE index={} title={} state={}",
            item.index,
            item.title,
            combined.trim()
        );
        return false;
    }

    eprintln!(
        "TROOZN_LIVE_PLAYLIST_VALIDATE_FAIL index={} title={} status={} state={}",
        item.index,
        item.title,
        output.status,
        combined.trim()
    );

    false
}

fn item_from_ytdlp_value(index: usize, v: &Value) -> Option<TrooznLiveItem> {
    let title = v
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Vidéo TROOZN")
        .to_string();

    let title_lc = title.to_lowercase();

    if title_lc.contains("private video")
        || title_lc.contains("deleted video")
        || title_lc.contains("video unavailable")
        || title_lc.contains("vidéo privée")
        || title_lc.contains("vidéo supprimée")
    {
        eprintln!("TROOZN_LIVE_FLAT_SKIP index={} title={}", index, title);
        return None;
    }

    if v.get("availability")
        .and_then(Value::as_str)
        .map(|s| s != "public" && s != "unlisted")
        .unwrap_or(false)
    {
        eprintln!(
            "TROOZN_LIVE_FLAT_SKIP_UNAVAILABLE index={} title={} availability={:?}",
            index,
            title,
            v.get("availability")
        );
        return None;
    }

    let id = v
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(&title)
        .to_string();

    let flat_url = v
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();

    let webpage_url = v
        .get("webpage_url")
        .and_then(Value::as_str)
        .map(str::to_string);

    let source_url = if let Some(url) = webpage_url.clone() {
        url
    } else if flat_url.starts_with("http://") || flat_url.starts_with("https://") {
        flat_url.clone()
    } else if id.starts_with("http://") || id.starts_with("https://") {
        id.clone()
    } else if looks_like_youtube_id(&id) {
        format!("https://www.youtube.com/watch?v={id}")
    } else {
        eprintln!(
            "TROOZN_LIVE_FLAT_SKIP_NO_URL index={} title={} id={} url={}",
            index, title, id, flat_url
        );
        return None;
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

    let description = v
        .get("description")
        .and_then(Value::as_str)
        .map(str::to_string);

    let upload_date = v
        .get("upload_date")
        .and_then(Value::as_str)
        .map(str::to_string);

    let uploader = v
        .get("uploader")
        .and_then(Value::as_str)
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
        description,
        upload_date,
        uploader,
    })
}

async fn extract_full_video_metadata(source_url: &str) -> anyhow::Result<FullVideoMetadata> {
    let mut last_error = String::new();

    for attempt in 1..=3 {
        let mut cmd = Command::new(YTDLP_BIN);

        add_ytdlp_common_args(&mut cmd).await;

        cmd.args([
            "--no-playlist",
            "--no-warnings",
            "--skip-download",
            "-J",
            source_url,
        ]);

        let output = match run_ytdlp_output(cmd, "yt-dlp metadata", 8).await {
            Ok(output) => output,
            Err(err) => {
                last_error = format!("yt-dlp metadata: {err:?}");
                sleep(Duration::from_millis(500 * attempt)).await;
                continue;
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);

            last_error = format!(
                "yt-dlp command failed: build_tag={} bin={} ignore_config=true format={} url={} status={} stderr={} stdout={}",
                TROOZN_LIVE_BUILD_TAG,
                YTDLP_BIN,
                YTDLP_GENERIC_SINGLE_FORMAT,
                source_url,
                output.status,
                stderr.trim(),
                stdout.trim()
            );
            sleep(Duration::from_millis(500 * attempt)).await;
            continue;
        }

        let root: Value =
            serde_json::from_slice(&output.stdout).context("parse yt-dlp metadata JSON")?;

        let meta = FullVideoMetadata {
            title: root
                .get("title")
                .and_then(Value::as_str)
                .map(str::to_string),
            webpage_url: root
                .get("webpage_url")
                .and_then(Value::as_str)
                .map(str::to_string),
            duration: root.get("duration").and_then(Value::as_u64),
            thumbnail: best_thumbnail_from_value(&root),
            channel: root
                .get("channel")
                .and_then(Value::as_str)
                .or_else(|| root.get("uploader").and_then(Value::as_str))
                .map(str::to_string),
            description: root
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
            upload_date: root
                .get("upload_date")
                .and_then(Value::as_str)
                .map(str::to_string),
            uploader: root
                .get("uploader")
                .and_then(Value::as_str)
                .map(str::to_string),
        };

        return Ok(meta);
    }

    anyhow::bail!("yt-dlp metadata a échoué après retries: {last_error}");
}

fn best_thumbnail_from_value(root: &Value) -> Option<String> {
    if let Some(url) = root.get("thumbnail").and_then(Value::as_str) {
        if !url.trim().is_empty() {
            return Some(url.to_string());
        }
    }

    let thumbnails = root.get("thumbnails").and_then(Value::as_array)?;

    thumbnails
        .iter()
        .filter_map(|thumb| {
            let url = thumb.get("url").and_then(Value::as_str)?;
            let width = thumb.get("width").and_then(Value::as_u64).unwrap_or(0);
            let height = thumb.get("height").and_then(Value::as_u64).unwrap_or(0);
            Some((width.saturating_mul(height), url.to_string()))
        })
        .max_by_key(|(score, _)| *score)
        .map(|(_, url)| url)
}

fn is_youtube_auth_or_bot_error(text: &str) -> bool {
    let lower = text.to_lowercase();

    lower.contains("sign in to confirm")
        || lower.contains("not a bot")
        || lower.contains("use --cookies")
        || lower.contains("please sign in")
        || lower.contains("confirm you're not a bot")
        || lower.contains("confirm you’re not a bot")
}

fn is_youtube_terminal_unavailable_error(text: &str) -> bool {
    let lower = text.to_lowercase();

    lower.contains("video unavailable")
        || lower.contains("this video is not available")
        || lower.contains("private video")
        || lower.contains("deleted video")
        || lower.contains("has been removed")
        || lower.contains("copyright claim")
        || lower.contains("blocked in your country")
        || lower.contains("not available in your country")
}

fn is_ytdlp_timeout_error(text: &str) -> bool {
    let lower = text.to_lowercase();

    lower.contains("timeout yt-dlp")
        || lower.contains("deadline has elapsed")
        || lower.contains("timeout attente slot")
}

fn add_ytdlp_cookies_if_available(_cmd: &mut Command) {
    // TROOZN Live v1: cookies désactivés par défaut.
    // Un fichier cookies invalide peut provoquer des erreurs YouTube difficiles à diagnostiquer.
    // On réactivera plus tard via une option explicite si nécessaire.
}

fn ytdlp_deno_available() -> bool {
    Path::new("/home/troozn/.deno/bin/deno").exists()
}

fn add_ytdlp_deno_args_if_available(cmd: &mut Command, reason: &str) -> anyhow::Result<()> {
    if !ytdlp_deno_available() {
        anyhow::bail!("deno indisponible pour fallback yt-dlp");
    }

    eprintln!("TROOZN_LIVE_YTDLP_DENO_FALLBACK reason={reason}");

    cmd.args([
        "--js-runtimes",
        "deno:/home/troozn/.deno/bin/deno",
        "--remote-components",
        "ejs:github",
    ]);

    Ok(())
}

fn best_youtube_fast_format_from_list_formats(text: &str) -> Option<&'static str> {
    // Formats autorisés, dans l'ordre de préférence :
    // 96 = 1080p HLS
    // 95 = 720p HLS
    // 94 = 480p HLS
    // 22 = 720p MP4 progressif
    let allowed = ["96", "22", "95", "94"];

    for wanted in allowed {
        for line in text.lines() {
            let mut parts = line.split_whitespace();

            let Some(format_id) = parts.next() else {
                continue;
            };

            if format_id == wanted {
                return Some(wanted);
            }
        }
    }

    None
}

async fn ytdlp_list_formats_text(source_url: &str, use_deno: bool) -> anyhow::Result<String> {
    let mut cmd = Command::new(YTDLP_BIN);
    let socket_timeout = if use_deno { "20" } else { "10" };

    if use_deno {
        add_ytdlp_deno_args_if_available(&mut cmd, "list-formats")?;
    }

    cmd.args([
        "--ignore-config",
        "--force-ipv4",
        "--no-warnings",
        "--socket-timeout",
        socket_timeout,
        "--list-formats",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_LIST_FORMATS_CMD bin={} deno={} url={}",
        YTDLP_BIN, use_deno, source_url
    );

    let timeout_seconds = if use_deno {
        YTDLP_YOUTUBE_DENO_LIST_FORMATS_TIMEOUT_SECONDS
    } else {
        YTDLP_YOUTUBE_LIST_FORMATS_TIMEOUT_SECONDS
    };
    let label = if use_deno {
        "yt-dlp list-formats deno"
    } else {
        "yt-dlp list-formats"
    };

    let output = run_ytdlp_output(cmd, label, timeout_seconds).await?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    let combined = if stderr.trim().is_empty() {
        stdout
    } else {
        format!(
            "{}
{}",
            stdout, stderr
        )
    };

    if !output.status.success() {
        anyhow::bail!(
            "yt-dlp list-formats failed status={} output={}",
            output.status,
            combined.trim()
        );
    }

    Ok(combined)
}

async fn resolve_youtube_url_with_format(
    source_url: &str,
    format_selector: &str,
    use_deno: bool,
) -> anyhow::Result<String> {
    let mut cmd = Command::new(YTDLP_BIN);
    add_ytdlp_cookies_if_available(&mut cmd);
    let socket_timeout = if use_deno { "20" } else { "8" };
    let retries = if use_deno { "1" } else { "0" };

    if use_deno {
        add_ytdlp_deno_args_if_available(&mut cmd, "single-url")?;
    }

    cmd.args([
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--force-ipv4",
        "--socket-timeout",
        socket_timeout,
        "--retries",
        retries,
        "--fragment-retries",
        retries,
        "-f",
        format_selector,
        "-g",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_YTDLP_RESOLVE_FORMAT bin={} deno={} format={} url={}",
        YTDLP_BIN, use_deno, format_selector, source_url
    );

    let timeout_seconds = if use_deno {
        YTDLP_YOUTUBE_DENO_RESOLVE_TIMEOUT_SECONDS
    } else {
        YTDLP_YOUTUBE_FAST_RESOLVE_TIMEOUT_SECONDS
    };
    let label = if use_deno {
        "yt-dlp -g deno"
    } else {
        "yt-dlp -g"
    };

    let output = run_ytdlp_output(cmd, label, timeout_seconds).await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);

        anyhow::bail!(
            "yt-dlp -g failed: bin={} format={} url={} status={} stderr={} stdout={}",
            YTDLP_BIN,
            format_selector,
            source_url,
            output.status,
            stderr.trim(),
            stdout.trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    let Some(url) = stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("http://") || line.starts_with("https://"))
    else {
        anyhow::bail!(
            "yt-dlp -g OK mais aucune URL: format={} stdout={}",
            format_selector,
            stdout.trim()
        );
    };

    eprintln!(
        "TROOZN_LIVE_RESOLVED_ITAG format={} itag95={} itag94={} itag93={} itag18={} prefix={}",
        format_selector,
        url.contains("itag/95") || url.contains("itag=95"),
        url.contains("itag/94") || url.contains("itag=94"),
        url.contains("itag/93") || url.contains("itag=93"),
        url.contains("itag/18") || url.contains("itag=18"),
        url.chars().take(160).collect::<String>()
    );

    Ok(url.to_string())
}

async fn resolve_urls_with_format(
    source_url: &str,
    format_selector: &str,
    timeout_seconds: u64,
) -> anyhow::Result<Vec<String>> {
    let mut cmd = Command::new(YTDLP_BIN);
    add_ytdlp_cookies_if_available(&mut cmd);

    cmd.args([
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--force-ipv4",
        "--socket-timeout",
        "20",
        "--retries",
        "2",
        "--fragment-retries",
        "2",
        "-f",
        format_selector,
        "-g",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_YTDLP_RESOLVE_GENERIC bin={} format={} url={}",
        YTDLP_BIN, format_selector, source_url
    );

    let output = run_ytdlp_output(cmd, "yt-dlp generic -g", timeout_seconds).await?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        anyhow::bail!(
            "yt-dlp generic -g failed: format={} url={} status={} stderr={} stdout={}",
            format_selector,
            source_url,
            output.status,
            stderr.trim(),
            stdout.trim()
        );
    }

    let urls = stdout
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("http://") || line.starts_with("https://"))
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if urls.is_empty() {
        anyhow::bail!(
            "yt-dlp generic -g OK mais aucune URL: format={} stdout={}",
            format_selector,
            stdout.trim()
        );
    }

    Ok(urls)
}

fn best_dash_av_format_from_list_formats(text: &str) -> Option<&'static str> {
    let has = |wanted: &str| -> bool {
        text.lines().any(|line| {
            line.split_whitespace()
                .next()
                .map(|fmt| fmt == wanted)
                .unwrap_or(false)
        })
    };

    // Profil Radxa : 1080p H.264 si disponible, puis 720p, puis 480p.
    // La vidéo est copiée, le Radxa décodera via l'accélération matérielle Kodi.
    if has("137") && has("140") {
        return Some("137+140");
    }

    if has("136") && has("140") {
        return Some("136+140");
    }

    if has("135") && has("140") {
        return Some("135+140");
    }

    None
}

fn youtube_json_format_url(root: &Value, format_id: &str) -> Option<String> {
    let formats = root.get("formats").and_then(Value::as_array)?;

    formats.iter().find_map(|format| {
        let id = format.get("format_id").and_then(Value::as_str)?;

        if id != format_id {
            return None;
        }

        format
            .get("url")
            .and_then(Value::as_str)
            .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
            .map(str::to_string)
    })
}

fn best_youtube_json_single_url(root: &Value) -> Option<(&'static str, String)> {
    for format_id in ["96", "22", "95", "94"] {
        if let Some(url) = youtube_json_format_url(root, format_id) {
            return Some((format_id, url));
        }
    }

    None
}

fn best_youtube_json_dash_av(root: &Value) -> Option<(&'static str, String, String)> {
    for (selector, video_id, audio_id) in [
        ("137+140", "137", "140"),
        ("136+140", "136", "140"),
        ("135+140", "135", "140"),
    ] {
        let Some(video_url) = youtube_json_format_url(root, video_id) else {
            continue;
        };
        let Some(audio_url) = youtube_json_format_url(root, audio_id) else {
            continue;
        };

        return Some((selector, video_url, audio_url));
    }

    None
}

async fn resolve_youtube_from_json_with_deno(
    source_url: &str,
) -> anyhow::Result<ResolvedMediaInput> {
    let mut cmd = Command::new(YTDLP_BIN);

    add_ytdlp_cookies_if_available(&mut cmd);
    add_ytdlp_deno_args_if_available(&mut cmd, "json-formats")?;

    cmd.args([
        "--no-playlist",
        "--no-warnings",
        "--force-ipv4",
        "--socket-timeout",
        "25",
        "--retries",
        "1",
        "--fragment-retries",
        "1",
        "--skip-download",
        "-J",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_YTDLP_JSON_DENO_CMD bin={} url={}",
        YTDLP_BIN, source_url
    );

    let output = run_ytdlp_output(
        cmd,
        "yt-dlp json deno",
        YTDLP_YOUTUBE_DENO_JSON_TIMEOUT_SECONDS,
    )
    .await?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        anyhow::bail!(
            "yt-dlp json deno failed: status={} stderr={} stdout={}",
            output.status,
            stderr.trim(),
            stdout.trim()
        );
    }

    let root: Value = serde_json::from_str(&stdout).context("parse yt-dlp json deno")?;

    if let Some((format_id, url)) = best_youtube_json_single_url(&root) {
        eprintln!(
            "TROOZN_LIVE_YTDLP_JSON_PICK_SINGLE format={} url={}",
            format_id, source_url
        );

        return Ok(ResolvedMediaInput::Single {
            url,
            format_selector: format_id.to_string(),
        });
    }

    if let Some((selector, video_url, audio_url)) = best_youtube_json_dash_av(&root) {
        eprintln!(
            "TROOZN_LIVE_YTDLP_JSON_PICK_DASH format={} url={}",
            selector, source_url
        );

        return Ok(ResolvedMediaInput::SeparateAv {
            video_url,
            audio_url,
            format_selector: selector.to_string(),
        });
    }

    anyhow::bail!("yt-dlp json deno OK mais aucun format 1080p/720p/480p exploitable")
}

async fn resolve_youtube_separate_av_with_format(
    source_url: &str,
    format_selector: &str,
    use_deno: bool,
) -> anyhow::Result<ResolvedMediaInput> {
    let mut cmd = Command::new(YTDLP_BIN);
    add_ytdlp_cookies_if_available(&mut cmd);
    let socket_timeout = if use_deno { "20" } else { "8" };
    let retries = if use_deno { "1" } else { "0" };

    if use_deno {
        add_ytdlp_deno_args_if_available(&mut cmd, "dash-av")?;
    }

    cmd.args([
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--force-ipv4",
        "--socket-timeout",
        socket_timeout,
        "--retries",
        retries,
        "--fragment-retries",
        retries,
        "-f",
        format_selector,
        "-g",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_YTDLP_DASH_AV_CMD bin={} deno={} format={} url={}",
        YTDLP_BIN, use_deno, format_selector, source_url
    );

    let timeout_seconds = if use_deno {
        YTDLP_YOUTUBE_DENO_RESOLVE_TIMEOUT_SECONDS
    } else {
        YTDLP_YOUTUBE_DASH_RESOLVE_TIMEOUT_SECONDS
    };
    let label = if use_deno {
        "yt-dlp dash av -g deno"
    } else {
        "yt-dlp dash av -g"
    };

    let output = run_ytdlp_output(cmd, label, timeout_seconds).await?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        anyhow::bail!(
            "yt-dlp dash av failed: format={} status={} stderr={} stdout={}",
            format_selector,
            output.status,
            stderr.trim(),
            stdout.trim()
        );
    }

    let urls = stdout
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("http://") || line.starts_with("https://"))
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if urls.len() < 2 {
        anyhow::bail!(
            "yt-dlp dash av OK mais moins de 2 URLs: format={} stdout={}",
            format_selector,
            stdout.trim()
        );
    }

    eprintln!(
        "TROOZN_LIVE_DASH_AV_RESOLVED format={} video_prefix={} audio_prefix={}",
        format_selector,
        urls[0].chars().take(100).collect::<String>(),
        urls[1].chars().take(100).collect::<String>()
    );

    Ok(ResolvedMediaInput::SeparateAv {
        video_url: urls[0].clone(),
        audio_url: urls[1].clone(),
        format_selector: format_selector.to_string(),
    })
}

async fn resolve_media_input(source_url: &str) -> anyhow::Result<ResolvedMediaInput> {
    if let Some(input) = direct_media_input(source_url).await {
        eprintln!(
            "TROOZN_LIVE_DIRECT_MEDIA_INPUT type={} url={}",
            media_type_for_input(&input),
            source_url
        );
        return Ok(input);
    }

    if is_youtube_url(source_url) {
        return resolve_youtube_media_input(source_url).await;
    }

    resolve_generic_media_input(source_url).await
}

async fn resolve_youtube_media_input(source_url: &str) -> anyhow::Result<ResolvedMediaInput> {
    let mut errors = Vec::new();
    let mut fast_timed_out = false;

    match resolve_youtube_preferred_single_url(source_url).await {
        Ok(url) => {
            return Ok(ResolvedMediaInput::Single {
                url,
                format_selector: YTDLP_YOUTUBE_FAST_FORMAT.to_string(),
            });
        }
        Err(first_err) => {
            eprintln!(
                "TROOZN_LIVE_YOUTUBE_FAST_INPUT_FAIL url={} state={first_err:?}",
                source_url
            );

            let message = first_err.to_string();
            if is_youtube_terminal_unavailable_error(&message)
                || is_youtube_auth_or_bot_error(&message)
            {
                anyhow::bail!("yt-dlp YouTube indisponible: {}", message);
            }

            if is_ytdlp_timeout_error(&message) {
                fast_timed_out = true;
            }

            errors.push(format!("fast={message}"));
        }
    }

    if fast_timed_out {
        if let Some(input) = try_youtube_deno_resolution(source_url, &mut errors).await? {
            return Ok(input);
        }
    }

    match resolve_youtube_separate_av_with_format(source_url, YTDLP_YOUTUBE_DASH_FORMAT, false)
        .await
    {
        Ok(input) => return Ok(input),
        Err(err) => {
            let message = err.to_string();
            eprintln!(
                "TROOZN_LIVE_YOUTUBE_DASH_DIRECT_FAIL url={} state={message}",
                source_url
            );

            if is_youtube_terminal_unavailable_error(&message)
                || is_youtube_auth_or_bot_error(&message)
            {
                anyhow::bail!("yt-dlp YouTube indisponible: {}", message);
            }

            errors.push(format!("dash_direct={message}"));
        }
    }

    if !fast_timed_out {
        if let Some(input) = try_youtube_deno_resolution(source_url, &mut errors).await? {
            return Ok(input);
        }
    }

    let list_with_deno = ytdlp_deno_available();

    match ytdlp_list_formats_text(source_url, list_with_deno).await {
        Ok(list_text) => {
            if let Some(best_format) = best_youtube_fast_format_from_list_formats(&list_text) {
                eprintln!(
                    "TROOZN_LIVE_YTDLP_LIST_FORMATS_PICK_SINGLE format={} url={}",
                    best_format, source_url
                );

                match resolve_youtube_url_with_format(source_url, best_format, list_with_deno).await
                {
                    Ok(url) => {
                        return Ok(ResolvedMediaInput::Single {
                            url,
                            format_selector: best_format.to_string(),
                        });
                    }
                    Err(err) => {
                        let message = err.to_string();
                        eprintln!(
                            "TROOZN_LIVE_YOUTUBE_LIST_SINGLE_FAIL url={} format={} state={message}",
                            source_url, best_format
                        );
                        errors.push(format!("list_single_{best_format}={message}"));
                    }
                }
            }

            if let Some(format_selector) = best_dash_av_format_from_list_formats(&list_text) {
                eprintln!(
                    "TROOZN_LIVE_YTDLP_LIST_FORMATS_PICK_DASH format={} url={}",
                    format_selector, source_url
                );

                match resolve_youtube_separate_av_with_format(
                    source_url,
                    format_selector,
                    list_with_deno,
                )
                .await
                {
                    Ok(input) => return Ok(input),
                    Err(err) => {
                        let message = err.to_string();
                        eprintln!(
                            "TROOZN_LIVE_YOUTUBE_LIST_DASH_FAIL url={} format={} state={message}",
                            source_url, format_selector
                        );
                        errors.push(format!("list_dash_{format_selector}={message}"));
                    }
                }
            }
        }
        Err(err) => {
            let message = err.to_string();
            eprintln!(
                "TROOZN_LIVE_YOUTUBE_LIST_FORMATS_FAIL url={} state={message}",
                source_url
            );
            errors.push(format!("list_formats={message}"));
        }
    }

    anyhow::bail!(
        "aucun format YouTube exploitable trouvé: muxé={} DASH={} erreurs={}",
        YTDLP_YOUTUBE_FAST_FORMAT,
        YTDLP_YOUTUBE_DASH_FORMAT,
        errors.join(" | ")
    )
}

async fn try_youtube_deno_resolution(
    source_url: &str,
    errors: &mut Vec<String>,
) -> anyhow::Result<Option<ResolvedMediaInput>> {
    if !ytdlp_deno_available() {
        errors.push("deno_fallback=deno indisponible".to_string());
        return Ok(None);
    }

    eprintln!("TROOZN_LIVE_YOUTUBE_DENO_RESOLVE_START url={source_url}");

    match resolve_youtube_from_json_with_deno(source_url).await {
        Ok(input) => return Ok(Some(input)),
        Err(err) => {
            let message = err.to_string();
            eprintln!(
                "TROOZN_LIVE_YOUTUBE_DENO_JSON_FAIL url={} state={message}",
                source_url
            );

            if is_youtube_terminal_unavailable_error(&message)
                || is_youtube_auth_or_bot_error(&message)
            {
                anyhow::bail!("yt-dlp YouTube indisponible: {}", message);
            }

            errors.push(format!("deno_json={message}"));
        }
    }

    match resolve_youtube_url_with_format(source_url, YTDLP_YOUTUBE_FAST_FORMAT, true).await {
        Ok(url) => {
            return Ok(Some(ResolvedMediaInput::Single {
                url,
                format_selector: YTDLP_YOUTUBE_FAST_FORMAT.to_string(),
            }));
        }
        Err(err) => {
            let message = err.to_string();
            eprintln!(
                "TROOZN_LIVE_YOUTUBE_DENO_SINGLE_FAIL url={} state={message}",
                source_url
            );

            if is_youtube_terminal_unavailable_error(&message)
                || is_youtube_auth_or_bot_error(&message)
            {
                anyhow::bail!("yt-dlp YouTube indisponible: {}", message);
            }

            errors.push(format!("deno_single={message}"));
        }
    }

    match resolve_youtube_separate_av_with_format(source_url, YTDLP_YOUTUBE_DASH_FORMAT, true).await
    {
        Ok(input) => Ok(Some(input)),
        Err(err) => {
            let message = err.to_string();
            eprintln!(
                "TROOZN_LIVE_YOUTUBE_DENO_DASH_FAIL url={} state={message}",
                source_url
            );

            if is_youtube_terminal_unavailable_error(&message)
                || is_youtube_auth_or_bot_error(&message)
            {
                anyhow::bail!("yt-dlp YouTube indisponible: {}", message);
            }

            errors.push(format!("deno_dash={message}"));
            Ok(None)
        }
    }
}

async fn resolve_generic_media_input(source_url: &str) -> anyhow::Result<ResolvedMediaInput> {
    match resolve_urls_with_format(source_url, YTDLP_GENERIC_SINGLE_FORMAT, 40).await {
        Ok(urls) if urls.len() == 1 => {
            return Ok(ResolvedMediaInput::Single {
                url: urls[0].clone(),
                format_selector: YTDLP_GENERIC_SINGLE_FORMAT.to_string(),
            });
        }
        Ok(urls) if urls.len() >= 2 => {
            return Ok(ResolvedMediaInput::SeparateAv {
                video_url: urls[0].clone(),
                audio_url: urls[1].clone(),
                format_selector: YTDLP_GENERIC_SINGLE_FORMAT.to_string(),
            });
        }
        Ok(_) => {}
        Err(err) => {
            eprintln!(
                "TROOZN_LIVE_GENERIC_SINGLE_INPUT_FAIL url={} state={err:?}",
                source_url
            );
        }
    }

    match resolve_urls_with_format(source_url, YTDLP_GENERIC_SEPARATE_FORMAT, 45).await {
        Ok(urls) if urls.len() >= 2 => {
            return Ok(ResolvedMediaInput::SeparateAv {
                video_url: urls[0].clone(),
                audio_url: urls[1].clone(),
                format_selector: YTDLP_GENERIC_SEPARATE_FORMAT.to_string(),
            });
        }
        Ok(urls) if urls.len() == 1 => {
            return Ok(ResolvedMediaInput::Single {
                url: urls[0].clone(),
                format_selector: YTDLP_GENERIC_SEPARATE_FORMAT.to_string(),
            });
        }
        Ok(_) => {}
        Err(err) => {
            eprintln!(
                "TROOZN_LIVE_GENERIC_SEPARATE_INPUT_FAIL url={} state={err:?}",
                source_url
            );
        }
    }

    match resolve_urls_with_format(source_url, YTDLP_GENERIC_AUDIO_FORMAT, 35).await {
        Ok(urls) if !urls.is_empty() => {
            return Ok(ResolvedMediaInput::AudioOnly {
                url: urls[0].clone(),
                format_selector: YTDLP_GENERIC_AUDIO_FORMAT.to_string(),
            });
        }
        Ok(_) => {}
        Err(err) => {
            eprintln!(
                "TROOZN_LIVE_GENERIC_AUDIO_INPUT_FAIL url={} state={err:?}",
                source_url
            );
        }
    }

    anyhow::bail!("aucun format yt-dlp exploitable trouvé pour ce lien")
}

async fn resolve_youtube_preferred_single_url(source_url: &str) -> anyhow::Result<String> {
    resolve_youtube_url_with_format(source_url, YTDLP_YOUTUBE_FAST_FORMAT, false).await
}

async fn add_ytdlp_common_args(cmd: &mut Command) {
    let deno_enabled = std::env::var("TROOZN_YTDLP_USE_DENO")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);

    if !deno_enabled {
        return;
    }

    if !Path::new("/home/troozn/.deno/bin/deno").exists() {
        eprintln!("TROOZN_LIVE_YTDLP_DENO_SKIP reason=missing_deno_bin");
        return;
    }

    eprintln!("TROOZN_LIVE_YTDLP_DENO_ENABLED");

    cmd.args([
        "--js-runtimes",
        "deno:/home/troozn/.deno/bin/deno",
        "--remote-components",
        "ejs:github",
    ]);
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
        "build_tag": TROOZN_LIVE_BUILD_TAG,
        "mode": "hls",
        "yt_dlp_bin": YTDLP_BIN,
        "target_format": YTDLP_YOUTUBE_FAST_FORMAT,
        "preferred_video_height": PREFERRED_VIDEO_HEIGHT,
        "fallback_video_height": FALLBACK_VIDEO_HEIGHT,
        "minimum_selected_video_height": MIN_SELECTED_VIDEO_HEIGHT,
        "youtube_dash_fallback": YTDLP_YOUTUBE_DASH_FORMAT,
        "generic_single_format": YTDLP_GENERIC_SINGLE_FORMAT,
        "generic_separate_format": YTDLP_GENERIC_SEPARATE_FORMAT,
        "generic_audio_format": YTDLP_GENERIC_AUDIO_FORMAT,
        "hls_segment_seconds": HLS_SEGMENT_SECONDS,
        "live_max_dir_bytes": LIVE_MAX_DIR_BYTES,
        "live_target_dir_bytes": LIVE_TARGET_DIR_BYTES,
        "live_keep_behind_seconds": LIVE_KEEP_BEHIND_SECONDS,
        "live_pressure_keep_behind_seconds": LIVE_PRESSURE_KEEP_BEHIND_SECONDS,
        "live_max_producer_ahead_seconds": LIVE_MAX_PRODUCER_AHEAD_SECONDS,
        "dynamic_playlist_names": true,
        "actual_resolution": null,
        "note": "La résolution réelle est celle du flux choisi par yt-dlp puis décodé par Kodi.",
        "hls_url": DEFAULT_PUBLIC_HLS_URL
    }))
}

pub async fn troozn_live_start(
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
            eprintln!("TROOZN_LIVE_START_ERROR: {err:?}");

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

pub async fn troozn_live_add(
    State(state): State<HttpGatewayState>,
    Json(req): Json<TrooznLiveSubmitRequest>,
) -> Response {
    let live = state.live.clone();

    match live
        .add_youtube_live_queue(&req.url, req.title, req.limit.unwrap_or(MAX_ITEMS))
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => {
            eprintln!("TROOZN_LIVE_ADD_ERROR: {err:?}");

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
    let current_playlist_name = state.live.current_playlist_name().await;

    let relative = match requested {
        "" => "index.m3u8",
        "index.m3u8" => "index.m3u8",
        other if is_public_playlist_alias(other, &current_playlist_name) => "index.m3u8",
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
