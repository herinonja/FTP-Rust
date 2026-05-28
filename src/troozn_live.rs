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
const YTDLP_BIN: &str = "/usr/local/bin/yt-dlp";
const MAX_ITEMS: usize = 20;
const MAX_PRODUCER_AHEAD_ITEMS: usize = 1;

const PUBLIC_HLS_URL: &str = "http://127.0.0.1:8787/troozn-live/playlist-youtube.m3u8";

const YTDLP_720_FORMAT: &str = "22/best[ext=mp4][vcodec^=avc1][acodec^=mp4a][height<=720]/18";

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
            self.wait_until_future_buffer_needed(item.index).await;

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

            // Ne pas bloquer le démarrage HLS sur les métadonnées complètes.
            // On clone l'item flat-playlist pour démarrer vite, puis on enrichit en arrière-plan.
            let item = item.clone();

            {
                let live_for_meta = self.clone();
                let item_for_meta = item.clone();

                tokio::spawn(async move {
                    let enriched = live_for_meta.enrich_item_metadata(&item_for_meta).await;

                    {
                        let mut producer = live_for_meta.producer_now.lock().await;
                        if producer.item_id == enriched.item_id {
                            producer.title = enriched.title.clone();
                            producer.duration = enriched.duration;
                            producer.thumbnail = enriched.thumbnail.clone();
                            producer.channel = enriched.channel.clone();
                            producer.description = enriched.description.clone();
                            producer.upload_date = enriched.upload_date.clone();
                            producer.uploader = enriched.uploader.clone();
                        }
                    }

                    {
                        let mut playback = live_for_meta.playback_now.lock().await;
                        if playback.item_id == enriched.item_id {
                            playback.title = enriched.title.clone();
                            playback.duration = enriched.duration;
                            playback.thumbnail = enriched.thumbnail.clone();
                            playback.channel = enriched.channel.clone();
                            playback.description = enriched.description.clone();
                            playback.upload_date = enriched.upload_date.clone();
                            playback.uploader = enriched.uploader.clone();
                        }
                    }

                    eprintln!(
                        "TROOZN_LIVE_METADATA_READY index={} title={} thumb={} uploader={}",
                        enriched.index,
                        enriched.title,
                        enriched.thumbnail.as_deref().unwrap_or("-"),
                        enriched.uploader.as_deref().unwrap_or("-")
                    );
                });
            }

            {
                let mut guard = self.producer_now.lock().await;
                guard.last_error = Some("Résolution URL vidéo 720p en cours".to_string());
            }

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

            let mut cmd = Command::new("ffmpeg");

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

            {
                let mut guard = self.producer_now.lock().await;
                guard.last_error = Some("Démarrage FFmpeg HLS en cours".to_string());
            }

            let child = cmd.spawn().context("lancement ffmpeg HLS item")?;

            {
                let mut guard = self.ffmpeg_child.lock().await;
                *guard = Some(child);
            }

            // Métadonnées complètes en arrière-plan seulement après démarrage FFmpeg.
            // Elles ne doivent jamais retarder les premiers segments HLS.
            {
                let live_for_meta = self.clone();
                let item_for_meta = item.clone();

                tokio::spawn(async move {
                    let enriched = live_for_meta.enrich_item_metadata(&item_for_meta).await;

                    {
                        let mut queue = live_for_meta.queue.lock().await;
                        if let Some(slot) = queue.iter_mut().find(|q| q.item_id == enriched.item_id) {
                            *slot = enriched.clone();
                        }
                    }

                    {
                        let mut producer = live_for_meta.producer_now.lock().await;
                        if producer.item_id == enriched.item_id {
                            producer.title = enriched.title.clone();
                            producer.duration = enriched.duration;
                            producer.thumbnail = enriched.thumbnail.clone();
                            producer.channel = enriched.channel.clone();
                            producer.description = enriched.description.clone();
                            producer.upload_date = enriched.upload_date.clone();
                            producer.uploader = enriched.uploader.clone();
                        }
                    }

                    {
                        let mut playback = live_for_meta.playback_now.lock().await;
                        if playback.item_id == enriched.item_id {
                            playback.title = enriched.title.clone();
                            playback.duration = enriched.duration;
                            playback.thumbnail = enriched.thumbnail.clone();
                            playback.channel = enriched.channel.clone();
                            playback.description = enriched.description.clone();
                            playback.upload_date = enriched.upload_date.clone();
                            playback.uploader = enriched.uploader.clone();
                        }
                    }

                    eprintln!(
                        "TROOZN_LIVE_METADATA_READY index={} title={} thumb={} uploader={}",
                        enriched.index,
                        enriched.title,
                        enriched.thumbnail.as_deref().unwrap_or("-"),
                        enriched.uploader.as_deref().unwrap_or("-")
                    );
                });
            }

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

        let has_any_segment = {
            let entries = self.master_entries.lock().await;
            !entries.is_empty()
        };

        self.rewrite_master_playlist(true).await.ok();

        {
            let mut producer = self.producer_now.lock().await;

            if has_any_segment {
                producer.state = "ended".to_string();
            } else {
                producer.state = "error".to_string();

                if producer.last_error.is_none() {
                    producer.last_error = Some("Aucun segment HLS généré".to_string());
                }
            }
        }

        Ok(())
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

    async fn wait_until_future_buffer_needed(&self, next_item_index: usize) {
        if next_item_index <= 1 {
            return;
        }

        loop {
            let playback = self.playback_now.lock().await.clone();

            // Si Kodi n'a pas encore commencé à demander des segments,
            // on prend item 1 comme base. Ça autorise la préparation d'un
            // futur item jouable avant le démarrage réel de Kodi.
            let base_index = if playback.index == 0 {
                1
            } else {
                playback.index
            };

            let entries = self.master_entries.lock().await.clone();

            let mut future_items = std::collections::HashSet::new();

            for entry in entries.iter() {
                if entry.item_index > base_index {
                    future_items.insert(entry.item_index);
                }
            }

            // Important :
            // On limite l'avance aux items réellement segmentés.
            // Si item 2 est bloqué/ignoré et ne produit aucun segment,
            // item 3 reste autorisé.
            if future_items.len() < MAX_PRODUCER_AHEAD_ITEMS {
                return;
            }

            {
                let mut producer = self.producer_now.lock().await;
                producer.state = "waiting".to_string();
                producer.last_error = Some(format!(
                    "Buffer futur déjà prêt: base item {}, futurs prêts {:?}, prochain item {}",
                    base_index, future_items, next_item_index
                ));
            }

            sleep(Duration::from_millis(1000)).await;
        }
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

    for attempt in 1..=1 {
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
            last_error = stderr.trim().to_string();
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

async fn resolve_youtube_720_url(source_url: &str) -> anyhow::Result<String> {
    let mut last_error = String::new();

    for attempt in 1..=1 {
        let mut cmd = Command::new(YTDLP_BIN);

        // Pas de add_ytdlp_common_args ici.
        // Le test manuel yt-dlp -g fonctionne sans Deno/remote-components.
        // On garde donc cette résolution aussi simple et rapide que possible.

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

        let output = match timeout(Duration::from_secs(30), cmd.output()).await {
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
