use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use eyre::Result;
use hashbrown::HashMap;
use infinisvm_core::{indexer::Indexer, s3::S3FsClient};
use infinisvm_logger::{error, info};
use infinisvm_retirement::InOrderRetirement;
use infinisvm_sync::{http_client::HttpClient, slots::SlotData};
use infinisvm_types::sync::{JobEffects, ShredId, SyncFinalization};
use tokio::{
    sync::Mutex,
    task::{self, JoinSet},
    time::{sleep, timeout},
};

use super::slots_sync_progress::SlotsSyncProgressRecorder;

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(3600);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_CONCURRENT_CHUNKS: usize = 5;
const CHUNK_SIZE: u64 = 100;
const AWAIT_STREAM_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct BackfillManager {
    http_client: Arc<HttpClient>,
    recorder: SlotsSyncProgressRecorder,
    s3_client: Option<S3FsClient>,
    indexer: Arc<Mutex<dyn Indexer>>,
    wait_timeout: Duration,
    poll_interval: Duration,
}

impl BackfillManager {
    pub fn new(
        http_client: Arc<HttpClient>,
        recorder: SlotsSyncProgressRecorder,
        s3_client: Option<S3FsClient>,
        indexer: Arc<Mutex<dyn Indexer>>,
    ) -> Self {
        Self {
            http_client,
            recorder,
            s3_client,
            indexer,
            wait_timeout: DEFAULT_WAIT_TIMEOUT,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    async fn wait_for_stream_slot(&self, previous_slot: u64) -> Result<Option<u64>> {
        let start = Instant::now();
        let mut last_log = Instant::now();

        loop {
            if let Some(latest) = self.read_latest_slot().await? {
                if latest > previous_slot {
                    return Ok(Some(latest));
                }
            }

            if start.elapsed() >= self.wait_timeout {
                return Ok(None);
            }

            if last_log.elapsed() >= AWAIT_STREAM_LOG_INTERVAL {
                info!(
                    "Still waiting for streamed slot newer than {}; elapsed {:?}",
                    previous_slot,
                    start.elapsed()
                );
                last_log = Instant::now();
            }

            sleep(self.poll_interval).await;
        }
    }

    pub async fn backfill_range(&self, start_slot: u64, end_slot: u64) -> Result<Option<u64>> {
        if start_slot > end_slot {
            return Ok(None);
        }

        info!(
            "Backfilling slots (HTTP -> S3 fallback) between {} and {} (inclusive)",
            start_slot, end_slot
        );

        let chunks = make_chunks(start_slot, end_slot, CHUNK_SIZE);

        let mut total_slots_backfilled: u64 = 0;
        let mut join_set = JoinSet::new();
        let mut chunk_iter = chunks.into_iter();

        let progress_recorder = self.recorder.clone();
        let retirement_state = Arc::new(Mutex::new(InOrderRetirement::new(start_slot)));

        spawn_next_chunks(
            &mut join_set,
            &mut chunk_iter,
            MAX_CONCURRENT_CHUNKS,
            &self.http_client,
            &self.s3_client,
            &self.indexer,
        );

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((chunk_start, _chunk_end, Ok(Some((slots_count, last_slot))))) => {
                    total_slots_backfilled += slots_count;

                    let current_backfill_start = self.read_backfill_start().await?;
                    let base_frontier = current_backfill_start.unwrap_or(start_slot);
                    let maybe_advanced_to = {
                        let mut tracker = retirement_state.lock().await;
                        let mut updated = tracker.sync_frontier(base_frontier);
                        if let Some(frontier) = tracker.record_range(chunk_start, last_slot) {
                            updated = Some(frontier);
                        }
                        updated
                    };

                    if let Some(new_start) = maybe_advanced_to {
                        if let Err(e) = progress_recorder.record_backfill_start(new_start) {
                            error!("Failed to record backfill_start {}: {}", new_start, e);
                        }
                    }
                }
                Ok((_chunk_start, _chunk_end, Ok(None))) => {
                    panic!("Backfill worker returned None for chunk {_chunk_start}-{_chunk_end}");
                }
                Ok((_chunk_start, _chunk_end, Err(e))) => return Err(e),
                Err(join_err) => return Err(eyre::eyre!("Backfill worker panicked: {}", join_err)),
            }

            spawn_next_chunks(
                &mut join_set,
                &mut chunk_iter,
                1,
                &self.http_client,
                &self.s3_client,
                &self.indexer,
            );
        }

        Ok(Some(total_slots_backfilled))
    }

    pub async fn read_latest_slot(&self) -> Result<Option<u64>> {
        let recorder = self.recorder.clone();
        task::spawn_blocking(move || recorder.get_latest_slot())
            .await
            .map_err(|e| eyre::eyre!("Failed to join recorder task: {}", e))?
    }

    pub async fn read_backfill_start(&self) -> Result<Option<u64>> {
        let recorder = self.recorder.clone();
        task::spawn_blocking(move || recorder.get_backfill_start())
            .await
            .map_err(|e| eyre::eyre!("Failed to join recorder task: {}", e))?
    }

    pub async fn wait_for_latest_slot_update(&self, previous_slot: Option<u64>) -> Result<Option<u64>> {
        match previous_slot {
            Some(slot) => self.wait_for_stream_slot(slot).await,
            None => {
                let start = Instant::now();
                let mut last_log = Instant::now();

                loop {
                    if let Some(latest) = self.read_latest_slot().await? {
                        return Ok(Some(latest));
                    }

                    if start.elapsed() >= self.wait_timeout {
                        return Ok(None);
                    }

                    if last_log.elapsed() >= AWAIT_STREAM_LOG_INTERVAL {
                        info!("Still waiting for first streamed slot; elapsed {:?}", start.elapsed());
                        last_log = Instant::now();
                    }

                    sleep(self.poll_interval).await;
                }
            }
        }
    }
}

async fn process_chunk(
    http_client: Arc<HttpClient>,
    s3_client: Option<S3FsClient>,
    indexer: Arc<Mutex<dyn Indexer>>,
    chunk_start: u64,
    chunk_end: u64,
) -> Result<Option<(u64, u64)>> {
    let fetch_future = async {
        let mut slots_map = http_client.get_slots(chunk_start, chunk_end).await?;

        if slots_map.is_empty() {
            if let Some(ref s3) = s3_client {
                info!(
                    "HTTP backfill returned no slots for chunk {}-{}; attempting S3 fallback",
                    chunk_start, chunk_end
                );
                slots_map = fetch_slots_from_s3(s3, chunk_start, chunk_end).await?;
                if slots_map.is_empty() {
                    info!("S3 backfill returned no slots for chunk {}-{}", chunk_start, chunk_end);
                    return Ok(None);
                }
            } else {
                info!(
                    "HTTP backfill returned no slots for chunk {}-{} and no S3 fallback configured",
                    chunk_start, chunk_end
                );
                return Ok(None);
            }
        }

        if chunk_start <= chunk_end {
            if let Some(ref s3) = s3_client {
                for slot in chunk_start..=chunk_end {
                    if slots_map.contains_key(&slot) {
                        continue;
                    }

                    match fetch_slot_from_s3(s3, slot).await {
                        Ok(Some(slot_data)) => {
                            info!(
                                "Recovered missing slot {} via S3 fallback (info_bytes={}, shards={})",
                                slot,
                                slot_data.info.len(),
                                slot_data.shards.len()
                            );
                            slots_map.insert(slot, slot_data);
                        }
                        Ok(None) => {
                            error!(
                                "S3 fallback did not provide missing slot {} (chunk {}-{})",
                                slot, chunk_start, chunk_end
                            );
                        }
                        Err(e) => {
                            error!(
                                "S3 fallback failed to fetch missing slot {} (chunk {}-{}): {}",
                                slot, chunk_start, chunk_end, e
                            );
                        }
                    }
                }
            } else {
                let missing = (chunk_start..=chunk_end)
                    .filter(|slot| !slots_map.contains_key(slot))
                    .count();
                if missing > 0 {
                    info!(
                        "HTTP backfill missing {} slots in chunk {}-{} and no S3 fallback configured",
                        missing, chunk_start, chunk_end
                    );
                }
            }
        }

        let mut slots: Vec<u64> = slots_map.keys().copied().collect();
        slots.sort_unstable();

        let expected = chunk_end.saturating_sub(chunk_start) + 1;
        if slots.len() as u64 != expected {
            panic!(
                "Backfill returned {} of {} expected slots for chunk {}-{}",
                slots.len(),
                expected,
                chunk_start,
                chunk_end
            );
        }

        for slot in &slots {
            let Some(slot_data) = slots_map.get(slot) else {
                panic!("Slot {slot} not found after fetching data from HTTP/S3");
            };

            let slot_metadata: SyncFinalization = match bincode::deserialize(&slot_data.info) {
                Ok(metadata) => metadata,
                Err(e) => {
                    panic!("Failed to deserialize slot {slot} metadata: {e}");
                }
            };

            info!("Indexing slot {} with metadata: {:?}", slot, slot_metadata);

            let mut total_effects = 0;
            for (shard_idx, shard) in slot_data.shards.iter().enumerate() {
                match bincode::deserialize::<Vec<JobEffects>>(shard) {
                    Ok(effects) => {
                        let effect_count = effects.len();
                        let shred_id = ShredId::new(slot_metadata.slot, shard_idx);
                        let mut guard = indexer.lock().await;
                        guard.index_transactions(effects, slot_metadata.block_unix_timestamp, shred_id);
                        drop(guard);
                        total_effects += effect_count;
                    }
                    Err(e) => {
                        panic!("Failed to deserialize shard {shard_idx} for slot {slot}: {e}");
                    }
                }
            }

            info!("Indexed {} transactions for slot {}", total_effects, slot);

            // Index block metadata after all transactions are indexed
            {
                let mut guard = indexer.lock().await;
                guard.index_block(
                    slot_metadata.slot,
                    slot_metadata.block_unix_timestamp,
                    slot_metadata.hash,
                    slot_metadata.parent_hash,
                );
            }

            info!("Indexed block metadata for slot {}", slot);
        }

        let slots_count = slots.len() as u64;
        let last_slot = slots.last().copied().unwrap_or(chunk_end);
        Ok(Some((slots_count, last_slot)))
    };

    match timeout(DEFAULT_WAIT_TIMEOUT, fetch_future).await {
        Ok(result) => result,
        Err(_) => panic!("Backfill chunk {chunk_start}-{chunk_end} exceeded timeout of {DEFAULT_WAIT_TIMEOUT:?}"),
    }
}

fn make_chunks(start: u64, end: u64, size: u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::new();
    let mut chunk_start = start;
    while chunk_start <= end {
        let chunk_end = std::cmp::min(chunk_start.saturating_add(size - 1), end);
        chunks.push((chunk_start, chunk_end));
        if chunk_end == u64::MAX {
            break;
        }
        chunk_start = chunk_end.saturating_add(1);
    }
    chunks
}

fn spawn_next_chunks(
    join_set: &mut JoinSet<(u64, u64, Result<Option<(u64, u64)>>)>,
    chunk_iter: &mut impl Iterator<Item = (u64, u64)>,
    limit: usize,
    http_client: &Arc<HttpClient>,
    s3_client: &Option<S3FsClient>,
    indexer: &Arc<Mutex<dyn Indexer>>,
) {
    for _ in 0..limit {
        if let Some((chunk_start, chunk_end)) = chunk_iter.next() {
            let client = http_client.clone();
            let s3_client = s3_client.clone();
            let indexer = indexer.clone();
            join_set.spawn(async move {
                let result: Result<Option<(u64, u64)>> =
                    process_chunk(client, s3_client, indexer, chunk_start, chunk_end).await;
                (chunk_start, chunk_end, result)
            });
        } else {
            break;
        }
    }
}

async fn fetch_slots_from_s3(s3: &S3FsClient, start_slot: u64, end_slot: u64) -> Result<HashMap<u64, SlotData>> {
    let mut map = HashMap::new();
    for slot in start_slot..=end_slot {
        if let Some(slot_data) = fetch_slot_from_s3(s3, slot).await? {
            map.insert(slot, slot_data);
        }
    }
    Ok(map)
}

async fn fetch_slot_from_s3(s3: &S3FsClient, slot: u64) -> Result<Option<SlotData>> {
    let dir_key = slot_dir_key(slot);
    let files = match s3.list_dir(dir_key.clone()).await {
        Ok(files) => files,
        Err(e) => {
            error!("S3 list_dir failed for slot {} (prefix {}): {}", slot, dir_key, e);
            return Ok(None);
        }
    };

    if !files.iter().any(|f| f == "info") {
        return Ok(None);
    }

    let info_key = format!("{dir_key}/info");
    let info = match s3.get_object(info_key).await {
        Ok(data) => data,
        Err(e) => {
            info!("S3 get_object failed for slot {} info: {}", slot, e);
            return Ok(None);
        }
    };

    let mut shard_indices: Vec<usize> = files.iter().filter_map(|name| name.parse::<usize>().ok()).collect();
    shard_indices.sort_unstable();

    let mut shards = Vec::new();
    for idx in shard_indices {
        let shard_key = format!("{dir_key}/{idx}");
        match s3.get_object(shard_key.clone()).await {
            Ok(data) => shards.push(data),
            Err(e) => {
                info!(
                    "S3 get_object failed for slot {} shard {} ({}): {}",
                    slot, idx, shard_key, e
                );
            }
        }
    }

    Ok(Some(SlotData { info, shards }))
}

fn slot_dir_key(slot: u64) -> String {
    format!("{}/{}/{}", slot % 256, slot % 65_535, slot)
}
