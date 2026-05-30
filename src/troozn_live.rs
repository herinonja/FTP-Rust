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

const LIVE_DIR: &str = "/home/troozn/.kodi/userdata/TROOZN/live";
const TROOZN_LIVE_BUILD_TAG: &str = "v1-quality-strict-96-95-94-22-ignore-config-2026-05-29";

const YTDLP_BIN: &str = "/home/troozn/.local/bin/yt-dlp";
const LIVE_KEEP_BEHIND_ITEMS: usize = 2;
const LIVE_MAX_DIR_BYTES: u64 = 1500 * 1024 * 1024;
const PLAYLIST_PAGE_SIZE: usize = 20;
const PLAYLIST_REFILL_THRESHOLD: usize = 5;
const LIVE_CONSUME_MODE: bool = false;
const LIVE_CONSUME_KEEP_BEHIND_ITEMS: usize = 2;
const MAX_ITEMS: usize = 20;
const MAX_PRODUCER_AHEAD_ITEMS: usize = 20;

const PUBLIC_HLS_URL: &str = "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8";

const YTDLP_COOKIES_FILE: &str = "/home/troozn/.config/troozn/youtube-cookies.txt";
const YTDLP_720_FORMAT: &str = "96/95/94/22";

#[derive(Debug, Clone)]
struct PlaylistRefillState {
    source_url: String,
    next_start: usize,
    exhausted: bool,
    active: bool,
}


pub struct TrooznLive {
    pub root_dir: PathBuf,
    ffmpeg_child: Mutex<Option<Child>>,
    producer_now: Mutex<TrooznLiveNow>,
    playback_now: Mutex<TrooznLiveNow>,
    queue: Mutex<Vec<TrooznLiveItem>>,
    master_entries: Mutex<Vec<MasterEntry>>,
    playlist_refill: Mutex<Option<PlaylistRefillState>>,
    session_id: Mutex<String>,
    worker_running: Mutex<bool>,
    generation_id: Mutex<u64>,
    playback_anchor_item: Mutex<usize>,
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
    let msg = format!("{}
", line.as_ref());

    match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(mut file) => {
            if let Err(err) = file.write_all(msg.as_bytes()).await {
                eprintln!("TROOZN_LIVE_AUDIT_WRITE_ERROR path={} error={err:?}", path.display());
            }
        }
        Err(err) => {
            eprintln!("TROOZN_LIVE_AUDIT_OPEN_ERROR path={} error={err:?}", path.display());
        }
    }
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
            playlist_refill: Mutex::new(None),
            session_id: Mutex::new(unix_timestamp().to_string()),
            worker_running: Mutex::new(false),
            generation_id: Mutex::new(0),
            playback_anchor_item: Mutex::new(1),
        }
    }

    async fn current_hls_url(&self) -> String {
        let session_id = self.session_id.lock().await.clone();
        format!("{}?session={}", PUBLIC_HLS_URL, session_id)
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
            current_index,
            remaining,
            start,
            end
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
                    "TROOZN_LIVE_REFILL_ERROR start={} end={} error={err:?}",
                    start,
                    end
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
                        "TROOZN_LIVE_EXTRACT_FAILED url={} error={err:?}",
                        candidate_url
                    );
                }
            }
        }

        if let Some(item) = fallback_single_item_from_url(source_url) {
            eprintln!(
                "TROOZN_LIVE_EXTRACT_FALLBACK_SINGLE source_url={} item_url={}",
                source_url,
                item.source_url
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
            eprintln!(
                "TROOZN_LIVE_WORKER_SPAWN generation={}",
                worker_generation
            );

            if let Err(err) = live.clone().run_hls_worker(worker_generation).await {
                eprintln!("TROOZN_LIVE_WORKER_ERROR: {err:?}");

                let mut producer = live.producer_now.lock().await;
                producer.state = "error".to_string();
                producer.last_error = Some(err.to_string());
            }

            let mut running = live.worker_running.lock().await;
            *running = false;

            eprintln!(
                "TROOZN_LIVE_WORKER_EXIT generation={}",
                worker_generation
            );
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

        self.bump_generation().await;
        {
            let mut running = self.worker_running.lock().await;
            *running = false;
        }

        self.ensure_clean_dir().await?;
        self.new_session_id().await;

        {
            let mut anchor = self.playback_anchor_item.lock().await;
            *anchor = 1;
        }

        {
            let mut entries = self.master_entries.lock().await;
            entries.clear();
        }

        let limit = limit.clamp(1, MAX_ITEMS);
        let playlist_like = is_youtube_playlist_like_url(source_url);
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
                        "TROOZN_LIVE_PLAYLIST_EXTRACT_OK url={} count={}",
                        candidate_url,
                        found.len()
                    );
                    items = found;
                    break;
                }
                Ok(_) => {
                    last_error = Some(format!("Extraction vide pour {}", candidate_url));
                    eprintln!("TROOZN_LIVE_PLAYLIST_EMPTY url={}", candidate_url);
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                    eprintln!(
                        "TROOZN_LIVE_PLAYLIST_EXTRACT_FAILED url={} error={err:?}",
                        candidate_url
                    );
                }
            }
        }

        if items.is_empty() {
            if playlist_like {
                anyhow::bail!(
                    "Playlist/mix YouTube non extractible. Aucun flux playlist ne sera lancé. Dernière erreur: {}",
                    last_error.unwrap_or_else(|| "inconnue".to_string())
                );
            }

            items = fallback_single_item_from_url(source_url)
                .map(|item| vec![item])
                .ok_or_else(|| anyhow::anyhow!("Aucun item YouTube trouvé"))?;
        }

        if playlist_like && items.len() <= 1 {
            anyhow::bail!(
                "Le lien partagé ressemble à une playlist/mix, mais yt-dlp n'a retourné qu'un seul item. \
Lecture annulée pour éviter l'arrêt après une seule vidéo. Partage une vraie URL watch?v=... ou une playlist PL extractible."
            );
        }

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

        if is_probably_youtube_playlist_url(source_url) && items.len() >= PLAYLIST_PAGE_SIZE {
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

        
        if is_probably_youtube_playlist_url(source_url) && added.len() >= PLAYLIST_PAGE_SIZE {
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

            // Ne pas bloquer le démarrage HLS sur les métadonnées complètes.
            // On clone l'item flat-playlist pour démarrer vite, puis on enrichit en arrière-plan.
            let item = item.clone();


            {
                let mut guard = self.producer_now.lock().await;
                guard.last_error = Some("Résolution URL vidéo 720p en cours".to_string());
            }

            let media_input = match resolve_youtube_media_input(&item.source_url).await {
                Ok(input) => input,
                Err(err) => {
                    // Échec silencieux par item :
                    // on n'arrête pas le producer, on passe simplement à l'item suivant.
                    eprintln!(
                        "TROOZN_LIVE_SKIP_ITEM index={} title={} source_url={} error={err:?}",
                        item.index,
                        item.title,
                        item.source_url
                    );

                    live_audit(
                        &self.root_dir,
                        format!(
                            "ITEM_YTDLP_FAIL index={} title={} url={} error={err:?}",
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
                    match &media_input {
                        ResolvedMediaInput::Single { url, format_selector } => {
                            format!("single format={} url={}", format_selector, url.chars().take(80).collect::<String>())
                        }
                        ResolvedMediaInput::SeparateAv { video_url, audio_url: _, format_selector } => {
                            format!("dash-av format={} video={}", format_selector, video_url.chars().take(80).collect::<String>())
                        }
                    }
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
                    hls_url: PUBLIC_HLS_URL.to_string(),
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

            let mut cmd = Command::new("nice");
            cmd.args(["-n", "10", "ionice", "-c", "2", "-n", "7", "ffmpeg"]);

            cmd.args([
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "warning",
                "-y",
            ]);

            match &media_input {
                ResolvedMediaInput::Single { url, format_selector } => {
                    eprintln!(
                        "TROOZN_LIVE_FFMPEG_INPUT_SINGLE index={} format={}",
                        item.index,
                        format_selector
                    );

                    cmd.args([
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
                        item.index,
                        format_selector
                    );

                    cmd.args([
                        "-i",
                        video_url,
                        "-i",
                        audio_url,
                        "-map",
                        "0:v:0",
                        "-map",
                        "1:a:0",
                    ]);
                }
            }

            cmd.args([
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
                *guard = Some(child);
            }

            // Métadonnées complètes en arrière-plan seulement après démarrage FFmpeg.
            // Elles ne doivent jamais retarder les premiers segments HLS.


            let mut imported_segments = 0_usize;

            loop {
                sleep(Duration::from_millis(500)).await;

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

        // Worker persistant : il attend de nouveaux items jusqu'à génération obsolète.
    }


    async fn enrich_item_metadata(&self, item: &TrooznLiveItem) -> TrooznLiveItem {
        let meta = match extract_full_video_metadata(&item.source_url).await {
            Ok(meta) => meta,
            Err(err) => {
                eprintln!(
                    "TROOZN_LIVE_METADATA_FAILED index={} title={} error={err:?}",
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

    async fn wait_until_future_buffer_needed(&self, _next_item_index: usize) {
        // TROOZN Live v1 : producer rapide.
        // Ne dépend pas de l'item actuellement lu par Kodi.
        return;
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
        out.push_str("#EXT-X-PLAYLIST-TYPE:EVENT\n");
        out.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", target_duration));
        out.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");
        out.push_str("#EXT-X-DISCONTINUITY-SEQUENCE:0\n");

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

    async fn cleanup_old_live_files(&self) -> anyhow::Result<()> {
        let current_index = {
            let now = self.playback_now.lock().await;
            now.index
        };

        if current_index <= LIVE_KEEP_BEHIND_ITEMS + 1 {
            return Ok(());
        }

        let keep_from = current_index.saturating_sub(LIVE_KEEP_BEHIND_ITEMS);

        self.cleanup_items_before(keep_from).await?;
        self.cleanup_by_size_limit().await?;

        Ok(())
    }

    async fn cleanup_items_before(&self, keep_from: usize) -> anyhow::Result<()> {
        let mut rd = fs::read_dir(&self.root_dir).await?;

        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();

            let Some(name) = path.file_name().map(|v| v.to_string_lossy().to_string()) else {
                continue;
            };

            let Some(item_index) = parse_item_index_from_live_filename(&name) else {
                continue;
            };

            if item_index >= keep_from {
                continue;
            }

            if name.ends_with(".ts") || name.ends_with(".m3u8") {
                match fs::remove_file(&path).await {
                    Ok(_) => {
                        eprintln!(
                            "TROOZN_LIVE_CLEANUP_REMOVE keep_from={} file={}",
                            keep_from,
                            name
                        );
                    }
                    Err(err) => {
                        eprintln!(
                            "TROOZN_LIVE_CLEANUP_REMOVE_FAILED file={} error={err:?}",
                            name
                        );
                    }
                }
            }
        }

        // Supprimer aussi les entrées master en mémoire devenues trop anciennes.
        {
            let mut entries = self.master_entries.lock().await;
            entries.retain(|entry| entry.item_index >= keep_from);
        }

        self.rewrite_master_playlist(false).await.ok();

        Ok(())
    }

    async fn cleanup_by_size_limit(&self) -> anyhow::Result<()> {
        let mut files: Vec<(usize, std::time::SystemTime, u64, PathBuf)> = Vec::new();
        let mut total_size: u64 = 0;

        let mut rd = fs::read_dir(&self.root_dir).await?;

        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();

            let Some(name) = path.file_name().map(|v| v.to_string_lossy().to_string()) else {
                continue;
            };

            if !name.ends_with(".ts") && !name.ends_with(".m3u8") {
                continue;
            }

            if name == "index.m3u8" || name == "playlist-youtube.m3u8" {
                continue;
            }

            let Some(item_index) = parse_item_index_from_live_filename(&name) else {
                continue;
            };

            let Ok(meta) = entry.metadata().await else {
                continue;
            };

            let size = meta.len();
            let modified = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);

            total_size = total_size.saturating_add(size);
            files.push((item_index, modified, size, path));
        }

        if total_size <= LIVE_MAX_DIR_BYTES {
            return Ok(());
        }

        files.sort_by_key(|(_, modified, _, _)| *modified);

        let current_index = {
            let now = self.playback_now.lock().await;
            now.index
        };

        let protected_from = current_index.saturating_sub(LIVE_KEEP_BEHIND_ITEMS);

        for (item_index, _, size, path) in files {
            if total_size <= LIVE_MAX_DIR_BYTES {
                break;
            }

            if item_index >= protected_from {
                continue;
            }

            if fs::remove_file(&path).await.is_ok() {
                total_size = total_size.saturating_sub(size);

                eprintln!(
                    "TROOZN_LIVE_CLEANUP_SIZE_REMOVE item={} remaining_bytes={} file={}",
                    item_index,
                    total_size,
                    path.display()
                );
            }
        }

        Ok(())
    }

    async fn consume_cleanup_before_item(&self, current_item_index: usize) {
        if !LIVE_CONSUME_MODE {
            return;
        }

        if current_item_index <= LIVE_CONSUME_KEEP_BEHIND_ITEMS + 1 {
            return;
        }

        let keep_from = current_item_index.saturating_sub(LIVE_CONSUME_KEEP_BEHIND_ITEMS);

        eprintln!(
            "TROOZN_LIVE_CONSUME_CLEANUP current_item={} keep_from={}",
            current_item_index,
            keep_from
        );

        let mut removed_count: usize = 0;
        let mut removed_bytes: u64 = 0;

        let mut rd = match fs::read_dir(&self.root_dir).await {
            Ok(rd) => rd,
            Err(err) => {
                eprintln!("TROOZN_LIVE_CONSUME_READDIR_ERROR error={err:?}");
                return;
            }
        };

        while let Ok(Some(entry)) = rd.next_entry().await {
            let path = entry.path();

            let Some(name) = path.file_name().map(|v| v.to_string_lossy().to_string()) else {
                continue;
            };

            if name == "index.m3u8"
                || name == "playlist-youtube.m3u8"
                || name == "audit.log"
            {
                continue;
            }

            if !name.ends_with(".ts") && !name.ends_with(".m3u8") {
                continue;
            }

            let Some(item_index) = parse_item_index_from_live_filename(&name) else {
                continue;
            };

            if item_index >= keep_from {
                continue;
            }

            let size = match entry.metadata().await {
                Ok(meta) => meta.len(),
                Err(_) => 0,
            };

            match fs::remove_file(&path).await {
                Ok(_) => {
                    removed_count += 1;
                    removed_bytes = removed_bytes.saturating_add(size);
                    eprintln!(
                        "TROOZN_LIVE_CONSUME_REMOVE item={} file={} bytes={}",
                        item_index,
                        name,
                        size
                    );
                }
                Err(err) => {
                    eprintln!(
                        "TROOZN_LIVE_CONSUME_REMOVE_FAILED file={} error={err:?}",
                        name
                    );
                }
            }
        }

        {
            let mut entries = self.master_entries.lock().await;
            entries.retain(|entry| entry.item_index >= keep_from);
        }

        self.rewrite_master_playlist(false).await.ok();

        eprintln!(
            "TROOZN_LIVE_CONSUME_DONE current_item={} keep_from={} removed_files={} removed_bytes={}",
            current_item_index,
            keep_from,
            removed_count,
            removed_bytes
        );
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
            description: item.description.clone(),
            upload_date: item.upload_date.clone(),
            uploader: item.uploader.clone(),
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
    
        if let Some(current_item_index) = parse_item_index_from_live_filename(relative) {
            self.consume_cleanup_before_item(current_item_index).await;
        }
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


fn is_youtube_playlist_like_url(source_url: &str) -> bool {
    query_param(source_url, "list").is_some()
        || source_url.contains("/playlist?")
        || source_url.contains("youtube.com/playlist")
}

fn is_probably_youtube_playlist_url(source_url: &str) -> bool {
    let lower = source_url.to_lowercase();

    lower.contains("list=")
        || lower.contains("/playlist?")
        || lower.contains("youtube.com/playlist")
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
    let video_id = extract_youtube_video_id(source_url)?;
    let watch_url = format!("https://www.youtube.com/watch?v={}", video_id);

    eprintln!(
        "TROOZN_LIVE_FALLBACK_SINGLE source_url={} watch_url={}",
        source_url, watch_url
    );

    Some(TrooznLiveItem {
        item_id: item_id_for_url(&watch_url),
        index: 1,
        title: "Playlist Youtube".to_string(),
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
                "TROOZN_LIVE_RANGE_EXTRACT_FAIL start={} end={} error={err:?}",
                start,
                end
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
        start,
        end,
        source_url
    );

    let output = timeout(Duration::from_secs(45), cmd.output())
        .await
        .context("timeout yt-dlp range extract")?
        .context("spawn yt-dlp range extract")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("yt-dlp range extract failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let root: serde_json::Value = serde_json::from_str(&stdout)
        .context("parse yt-dlp range json")?;

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

    format!("{:x}", digest)
        .chars()
        .take(16)
        .collect::<String>()
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
    } else if !id.is_empty() && !id.starts_with("http://") && !id.starts_with("https://") {
        format!("https://www.youtube.com/watch?v={}", id)
    } else if url.starts_with("http://") || url.starts_with("https://") {
        url
    } else if !url.is_empty() {
        format!("https://www.youtube.com/watch?v={}", url)
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

    for attempt in 1..=3 {
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
                    "TROOZN_LIVE_PLAYLIST_EXTRACT_RETRY_FAILED attempt={} source_url={} error={err:?}",
                    attempt,
                    source_url
                );
                last_error = Some(err);
            }
        }

        sleep(Duration::from_millis(1200 * attempt)).await;
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

        let output = match timeout(Duration::from_secs(8), cmd.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(err)) => {
                last_error = format!("exécution yt-dlp metadata: {err}");
                sleep(Duration::from_millis(500 * attempt)).await;
                continue;
            }
            Err(_) => {
                last_error = "timeout yt-dlp metadata".to_string();
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
                YTDLP_720_FORMAT,
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


fn add_ytdlp_cookies_if_available(_cmd: &mut Command) {
    // TROOZN Live v1: cookies désactivés par défaut.
    // Un fichier cookies invalide peut provoquer des erreurs YouTube difficiles à diagnostiquer.
    // On réactivera plus tard via une option explicite si nécessaire.
}



fn best_allowed_format_from_list_formats(text: &str) -> Option<&'static str> {
    // Formats autorisés, dans l'ordre de préférence :
    // 96 = 1080p HLS
    // 95 = 720p HLS
    // 94 = 480p HLS
    // 22 = 720p MP4 progressif
    let allowed = ["96", "95", "94", "22"];

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

async fn ytdlp_list_formats_text(source_url: &str) -> anyhow::Result<String> {
    let mut cmd = Command::new(YTDLP_BIN);

    cmd.args([
        "--ignore-config",
        "--force-ipv4",
        "--no-warnings",
        "--socket-timeout",
        "20",
        "--list-formats",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_LIST_FORMATS_CMD bin={} url={}",
        YTDLP_BIN,
        source_url
    );

    let output = timeout(Duration::from_secs(30), cmd.output())
        .await
        .context("timeout yt-dlp list-formats")?
        .context("spawn yt-dlp list-formats")?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    let combined = if stderr.trim().is_empty() {
        stdout
    } else {
        format!("{}
{}", stdout, stderr)
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
) -> anyhow::Result<String> {
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
        "1",
        "--fragment-retries",
        "1",
        "-f",
        format_selector,
        "-g",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_YTDLP_RESOLVE_FORMAT bin={} format={} url={}",
        YTDLP_BIN,
        format_selector,
        source_url
    );

    let output = timeout(Duration::from_secs(30), cmd.output())
        .await
        .context("timeout yt-dlp -g")?
        .context("spawn yt-dlp -g")?;

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
        "TROOZN_LIVE_RESOLVED_ITAG format={} itag96={} itag95={} itag94={} itag93={} itag18={} prefix={}",
        format_selector,
        url.contains("itag/96") || url.contains("itag=96"),
        url.contains("itag/95") || url.contains("itag=95"),
        url.contains("itag/94") || url.contains("itag=94"),
        url.contains("itag/93") || url.contains("itag=93"),
        url.contains("itag/18") || url.contains("itag=18"),
        url.chars().take(160).collect::<String>()
    );

    Ok(url.to_string())
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

async fn resolve_youtube_separate_av_with_format(
    source_url: &str,
    format_selector: &str,
) -> anyhow::Result<ResolvedMediaInput> {
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
        "1",
        "--fragment-retries",
        "1",
        "-f",
        format_selector,
        "-g",
        source_url,
    ]);

    eprintln!(
        "TROOZN_LIVE_YTDLP_DASH_AV_CMD bin={} format={} url={}",
        YTDLP_BIN,
        format_selector,
        source_url
    );

    let output = timeout(Duration::from_secs(35), cmd.output())
        .await
        .context("timeout yt-dlp dash av -g")?
        .context("spawn yt-dlp dash av -g")?;

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

async fn resolve_youtube_media_input(source_url: &str) -> anyhow::Result<ResolvedMediaInput> {
    match resolve_youtube_720_url(source_url).await {
        Ok(url) => {
            return Ok(ResolvedMediaInput::Single {
                url,
                format_selector: YTDLP_720_FORMAT.to_string(),
            });
        }
        Err(first_err) => {
            eprintln!(
                "TROOZN_LIVE_SINGLE_INPUT_FAIL url={} error={first_err:?}",
                source_url
            );
        }
    }

    let list_text = ytdlp_list_formats_text(source_url).await?;

    let Some(format_selector) = best_dash_av_format_from_list_formats(&list_text) else {
        anyhow::bail!(
            "aucun format muxé 96/95/94/22 ni DASH séparé 137+140/136+140/135+140 disponible"
        );
    };

    resolve_youtube_separate_av_with_format(source_url, format_selector).await
}

async fn resolve_youtube_720_url(source_url: &str) -> anyhow::Result<String> {
    let mut last_error = String::new();

    // 1) Tentative directe stricte : 1080p HLS, 720p HLS, 480p HLS, 720p MP4.
    match resolve_youtube_url_with_format(source_url, YTDLP_720_FORMAT).await {
        Ok(url) => return Ok(url),
        Err(err) => {
            last_error = err.to_string();

            eprintln!(
                "TROOZN_LIVE_YTDLP_STRICT_FAIL url={} error={}",
                source_url,
                last_error
            );

            if is_youtube_auth_or_bot_error(&last_error) {
                anyhow::bail!(
                    "yt-dlp a échoué après 1 tentative(s): {}",
                    last_error
                );
            }
        }
    }

    // 2) Si yt-dlp a dit format indisponible, on vérifie les formats réels.
    // Certains appels -g sont intermittents alors que --list-formats voit bien 96/95/94/22.
    let list_text = match ytdlp_list_formats_text(source_url).await {
        Ok(text) => text,
        Err(err) => {
            anyhow::bail!(
                "yt-dlp a échoué après fallback list-formats: premier_error={} list_error={}",
                last_error,
                err
            );
        }
    };

    let Some(best_format) = best_allowed_format_from_list_formats(&list_text) else {
        let interesting_formats = list_text
            .lines()
            .map(str::trim)
            .filter(|line| {
                let first = line.split_whitespace().next().unwrap_or("");
                matches!(
                    first,
                    "96" | "95" | "94" | "93" | "22" | "18" | "137" | "136" | "135" | "134" | "140"
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");

        anyhow::bail!(
            "yt-dlp a échoué: aucun format autorisé 96/95/94/22 trouvé. premier_error={} formats_detectes={}",
            last_error,
            interesting_formats
        );
    };

    eprintln!(
        "TROOZN_LIVE_YTDLP_LIST_FORMATS_PICK format={} url={}",
        best_format,
        source_url
    );

    // 3) Relance avec le format exact détecté.
    match resolve_youtube_url_with_format(source_url, best_format).await {
        Ok(url) => Ok(url),
        Err(err) => {
            anyhow::bail!(
                "yt-dlp a échoué après fallback format exact {}: premier_error={} final_error={}",
                best_format,
                last_error,
                err
            );
        }
    }
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
        "build_tag": TROOZN_LIVE_BUILD_TAG,
        "mode": "hls",
        "yt_dlp_bin": YTDLP_BIN,
        "target_format": YTDLP_720_FORMAT,
        "actual_resolution": null,
        "note": "La résolution réelle est celle du flux choisi par yt-dlp puis décodé par Kodi.",
        "hls_url": PUBLIC_HLS_URL
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
