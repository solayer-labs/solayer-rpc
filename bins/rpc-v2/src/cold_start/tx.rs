use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::{DashMap, DashSet};
use eyre::eyre;
use hashbrown::HashMap;
use infinisvm_core::{
    bank::{Bank, TransactionStatus},
    indexer::Indexer,
    subscription::SubscriptionProcessor,
    DEFAULT_DB_MERGE_SLOT_INTERVAL, DEFAULT_FINALIZED_SLOTS_WINDOW_SLOTS, DEFAULT_PENDING_BATCHES_TTL_SLOTS,
    DEFAULT_SEEN_SHREDS_WINDOW_SLOTS, DEFAULT_STAGED_BATCHES_TTL_SLOTS,
};
use infinisvm_db::{
    db_chain::{DBChain, DBMeta},
    in_memory_db::NoopDB,
    Database, MemoryDB,
};
use infinisvm_logger::{error, info};
use infinisvm_types::{
    convert::materialize_job_effect_account_updates,
    sync::{CommitBatchNotification, ShredId, ShredIndex, SignedFinalization, SyncBatchShred, SyncFinalization},
};
use metrics::{counter, gauge, histogram};
use solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey, signature::Signature};
use tokio::{
    sync::{mpsc::Receiver, Mutex, Semaphore},
    task::JoinHandle,
};
use tonic::{Code, Status};

use super::utils::{record_dashmap_len, record_dashset_len, record_staged_batches_metrics};
use crate::{
    cold_start::slots_sync_progress::SlotsSyncProgressRecorder,
    p2p::{PeerManager, PeerNotification},
};

mod shred_manager;
use self::shred_manager::ShredManager;

const DB_MERGE_SLOT_INTERVAL: u64 = DEFAULT_DB_MERGE_SLOT_INTERVAL;
const FINALIZED_SLOTS_WINDOW_SLOTS: u64 = DEFAULT_FINALIZED_SLOTS_WINDOW_SLOTS;

/// Spawn the single transaction processor and the prune task.
///
/// Returns the handles for:
///  - the processor task
///  - the prune task
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_tx_processors(
    receiver: Receiver<PeerNotification>,
    db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    indexer: Arc<Mutex<dyn Indexer>>,
    bank: Arc<RwLock<Bank>>,
    subscription_processor: Arc<SubscriptionProcessor>,
    seen_shreds: Arc<DashSet<ShredId>>,
    staged_batches: Arc<DashMap<Slot, BTreeMap<ShredIndex, SyncBatchShred>>>,
    finalized_slots: Arc<DashSet<Slot>>,
    finalized_timestamps: Arc<DashMap<Slot, u64>>,
    finalized_job_ids: Arc<RwLock<HashMap<u64, ShredIndex>>>,
    peer_manager: Arc<PeerManager>,
    finalizer_refetching: Arc<DashSet<Slot>>,
    blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    current_slot: Arc<AtomicU64>,
    pending_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
    shred_sources: Arc<DashMap<ShredId, [u8; 32]>>,
    sequencer_pubkey: Pubkey,
    progress_recorder: Option<SlotsSyncProgressRecorder>,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();

    info!("Starting single transaction batch processing thread");

    let shred_manager = ShredManager::new(staged_batches.clone());
    let refetch_sem = Arc::new(Semaphore::new(16));
    let pending_finalizations: Arc<DashMap<Slot, SyncFinalization>> = Arc::new(DashMap::new());

    let processor = SlotProcessor::new(
        db_chain.clone(),
        indexer.clone(),
        bank.clone(),
        subscription_processor.clone(),
        seen_shreds.clone(),
        shred_manager,
        finalized_slots.clone(),
        finalized_timestamps.clone(),
        finalized_job_ids.clone(),
        peer_manager.clone(),
        refetch_sem.clone(),
        finalizer_refetching.clone(),
        FINALIZED_SLOTS_WINDOW_SLOTS,
        blockhash_to_signatures.clone(),
        current_slot.clone(),
        pending_batches.clone(),
        shred_sources.clone(),
        pending_finalizations.clone(),
        sequencer_pubkey,
        progress_recorder.clone(),
    );

    let processor_handle = tokio::spawn(async move {
        processor.run(receiver).await;
    });
    handles.push(processor_handle);

    let prune_handle = spawn_prune_task(
        seen_shreds,
        finalized_slots,
        finalized_timestamps,
        staged_batches,
        finalized_job_ids,
        current_slot,
        pending_batches,
        shred_sources,
        pending_finalizations,
        FINALIZED_SLOTS_WINDOW_SLOTS,
    );
    handles.push(prune_handle);

    handles
}

#[allow(clippy::too_many_arguments)]
fn spawn_prune_task(
    seen_shreds: Arc<DashSet<ShredId>>,
    finalized_slots: Arc<DashSet<Slot>>,
    finalized_timestamps: Arc<DashMap<Slot, u64>>,
    staged_batches: Arc<DashMap<Slot, BTreeMap<ShredIndex, SyncBatchShred>>>,
    finalized_job_ids: Arc<RwLock<HashMap<u64, ShredIndex>>>,
    current_slot: Arc<AtomicU64>,
    pending_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
    shred_sources: Arc<DashMap<ShredId, [u8; 32]>>,
    pending_finalizations: Arc<DashMap<Slot, SyncFinalization>>,
    finalized_window: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let prune_interval_secs = std::env::var("PRUNE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(120);
        let seen_window = std::env::var("SEEN_SHREDS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SEEN_SHREDS_WINDOW_SLOTS);
        let staged_ttl = std::env::var("STAGED_BATCHES_TTL_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_STAGED_BATCHES_TTL_SLOTS);
        let finalized_ids_window = std::env::var("FINALIZED_JOB_IDS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(FINALIZED_SLOTS_WINDOW_SLOTS);
        let pending_batches_ttl = std::env::var("PENDING_BATCHES_TTL_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_PENDING_BATCHES_TTL_SLOTS);

        let mut ticker = tokio::time::interval(Duration::from_secs(prune_interval_secs));
        loop {
            ticker.tick().await;

            let cur = current_slot.load(Ordering::SeqCst);
            if cur == 0 {
                continue;
            }

            let seen_cutoff = cur.saturating_sub(seen_window);
            let finalized_cutoff = cur.saturating_sub(finalized_window);
            let staged_cutoff = cur.saturating_sub(staged_ttl);
            let finalized_ids_cutoff = cur.saturating_sub(finalized_ids_window);
            let pending_batches_cutoff = cur.saturating_sub(pending_batches_ttl);

            let mut removed_seen = 0usize;
            seen_shreds.retain(|shred_id| {
                let keep = shred_id.slot >= seen_cutoff;
                if !keep {
                    removed_seen += 1;
                }
                keep
            });
            seen_shreds.shrink_to_fit();
            gauge!("seen_shreds_len").set(seen_shreds.len() as f64);
            histogram!("prune_seen_shreds_removed").record(removed_seen as f64);

            let mut removed_finalized = 0usize;
            finalized_slots.retain(|slot| {
                let keep = *slot >= finalized_cutoff;
                if !keep {
                    removed_finalized += 1;
                }
                keep
            });
            finalized_slots.shrink_to_fit();
            gauge!("finalized_slots_len").set(finalized_slots.len() as f64);
            histogram!("prune_finalized_slots_removed").record(removed_finalized as f64);

            let mut removed_ts = 0usize;
            finalized_timestamps.retain(|slot, _| {
                let keep = *slot >= finalized_cutoff;
                if !keep {
                    removed_ts += 1;
                }
                keep
            });
            finalized_timestamps.shrink_to_fit();
            gauge!("finalized_timestamps_len").set(finalized_timestamps.len() as f64);
            histogram!("prune_finalized_timestamps_removed").record(removed_ts as f64);

            let mut removed_staged_slots = 0usize;
            staged_batches.retain(|slot, _| {
                let keep = *slot >= staged_cutoff;
                if !keep {
                    removed_staged_slots += 1;
                }
                keep
            });
            staged_batches.shrink_to_fit();
            record_staged_batches_metrics(staged_batches.as_ref());
            histogram!("prune_staged_batches_slots_removed").record(removed_staged_slots as f64);

            let mut removed_finalized_ids = 0usize;
            {
                let mut guard = finalized_job_ids.write().unwrap();
                guard.retain(|slot, _| {
                    let keep = *slot >= finalized_ids_cutoff;
                    if !keep {
                        removed_finalized_ids += 1;
                    }
                    keep
                });
                gauge!("finalized_job_ids_len").set(guard.len() as f64);
            }
            histogram!("prune_finalized_job_ids_removed").record(removed_finalized_ids as f64);

            let mut removed_pending_batches = 0usize;
            pending_batches.retain(|shred_id, _| {
                let keep = shred_id.slot >= pending_batches_cutoff;
                if !keep {
                    removed_pending_batches += 1;
                }
                keep
            });
            pending_batches.shrink_to_fit();
            gauge!("pending_batches_len").set(pending_batches.len() as f64);
            histogram!("prune_pending_batches_removed").record(removed_pending_batches as f64);

            let mut removed_pending_finalizations = 0usize;
            pending_finalizations.retain(|slot, _| {
                let keep = *slot >= pending_batches_cutoff;
                if !keep {
                    removed_pending_finalizations += 1;
                }
                keep
            });
            pending_finalizations.shrink_to_fit();
            gauge!("pending_finalizations_len").set(pending_finalizations.len() as f64);
            histogram!("prune_pending_finalizations_removed").record(removed_pending_finalizations as f64);

            let mut removed_sources = 0usize;
            shred_sources.retain(|shred_id, _| {
                let keep = shred_id.slot >= pending_batches_cutoff;
                if !keep {
                    removed_sources += 1;
                }
                keep
            });
            shred_sources.shrink_to_fit();
            histogram!("prune_shred_sources_removed").record(removed_sources as f64);
        }
    })
}

/// The single slot/shred processing engine.
///
/// Owns the staging, finalization, DBChain / Bank / indexer updates for
/// commit-batch notifications.
struct SlotProcessor {
    db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    indexer: Arc<Mutex<dyn Indexer>>,
    bank: Arc<RwLock<Bank>>,
    subscription_processor: Arc<SubscriptionProcessor>,

    seen_shreds: Arc<DashSet<ShredId>>,
    shred_manager: ShredManager,
    finalized_slots: Arc<DashSet<Slot>>,
    finalized_timestamps: Arc<DashMap<Slot, u64>>,
    finalized_job_ids: Arc<RwLock<HashMap<u64, ShredIndex>>>,

    peer_manager: Arc<PeerManager>,
    refetch_sem: Arc<Semaphore>,
    finalizer_refetching: Arc<DashSet<Slot>>,
    finalized_window_slots: u64,
    blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    current_slot: Arc<AtomicU64>,
    pending_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
    shred_sources: Arc<DashMap<ShredId, [u8; 32]>>,
    pending_finalizations: Arc<DashMap<Slot, SyncFinalization>>,
    sequencer_pubkey: Pubkey,
    latest_signed_finalization: Option<SignedFinalization>,
    progress_recorder: Option<SlotsSyncProgressRecorder>,
}

impl SlotProcessor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
        indexer: Arc<Mutex<dyn Indexer>>,
        bank: Arc<RwLock<Bank>>,
        subscription_processor: Arc<SubscriptionProcessor>,
        seen_shreds: Arc<DashSet<ShredId>>,
        shred_manager: ShredManager,
        finalized_slots: Arc<DashSet<Slot>>,
        finalized_timestamps: Arc<DashMap<Slot, u64>>,
        finalized_job_ids: Arc<RwLock<HashMap<u64, ShredIndex>>>,
        peer_manager: Arc<PeerManager>,
        refetch_sem: Arc<Semaphore>,
        finalizer_refetching: Arc<DashSet<Slot>>,
        finalized_window_slots: u64,
        blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
        current_slot: Arc<AtomicU64>,
        pending_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
        shred_sources: Arc<DashMap<ShredId, [u8; 32]>>,
        pending_finalizations: Arc<DashMap<Slot, SyncFinalization>>,
        sequencer_pubkey: Pubkey,
        progress_recorder: Option<SlotsSyncProgressRecorder>,
    ) -> Self {
        Self {
            db_chain,
            indexer,
            bank,
            subscription_processor,
            seen_shreds,
            shred_manager,
            finalized_slots,
            finalized_timestamps,
            finalized_job_ids,
            peer_manager,
            refetch_sem,
            finalizer_refetching,
            finalized_window_slots,
            blockhash_to_signatures,
            current_slot,
            pending_batches,
            shred_sources,
            pending_finalizations,
            sequencer_pubkey,
            latest_signed_finalization: None,
            progress_recorder,
        }
    }

    async fn run(mut self, mut receiver: Receiver<PeerNotification>) {
        info!("Transaction batch processor started");
        while let Some(notification) = receiver.recv().await {
            self.handle_notification(notification).await;
        }
        info!("Transaction batch processor terminated (channel closed)");
    }

    async fn handle_notification(&mut self, notification: PeerNotification) {
        match notification.notification.as_ref() {
            CommitBatchNotification::Finalization(marker) => {
                error!(
                    "Received unsigned finalization for slot {}; signed finalizations are required",
                    marker.slot
                );
                self.peer_manager.penalize_invalid_finalizer(notification.peer_id);
            }
            CommitBatchNotification::SignedFinalization(signed) => {
                let prev_latest_signed_slot = self
                    .latest_signed_finalization
                    .as_ref()
                    .map(|finalization| finalization.finalization.slot);
                let marker = &signed.finalization;
                if !verify_signed_finalization(signed, &self.sequencer_pubkey) {
                    error!("Invalid signed finalization for slot {}; ignoring", marker.slot);
                    self.peer_manager.penalize_invalid_finalizer(notification.peer_id);
                    return;
                }
                if marker.shred_hashes.len() != marker.num_shreds as usize {
                    error!(
                        "Signed finalization shred hash list length mismatch for slot {}; penalizing peer",
                        marker.slot
                    );
                    self.peer_manager.penalize_invalid_finalizer(notification.peer_id);
                    return;
                }
                self.peer_manager
                    .observe_signed_finalization(notification.peer_id, marker.slot);

                // Track the maximum observed signed finalization slot (do not regress).
                if prev_latest_signed_slot.map(|prev| marker.slot > prev).unwrap_or(true) {
                    self.latest_signed_finalization = Some(signed.clone());
                }
                let received_unix_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_secs())
                    .unwrap_or_default();
                if marker.num_shreds == 0 {
                    info!(
                        "Received signed finalization marker for empty slot {}, block_timestamp={}, received_unix_timestamp={}",
                        marker.slot, marker.block_unix_timestamp, received_unix_timestamp
                    );
                } else {
                    info!(
                        "Received signed finalization marker for slot {}, job_ids=0-{}, block_timestamp={}, received_unix_timestamp={}",
                        marker.slot,
                        marker.num_shreds - 1,
                        marker.block_unix_timestamp,
                        received_unix_timestamp
                    );
                }
                self.maybe_refetch_missing_finalizer(prev_latest_signed_slot, marker.slot)
                    .await;
                if let Err(e) = self.finalize_slot(marker).await {
                    error!("Failed to finalize slot {}: {}", marker.slot, e);
                }
            }
            CommitBatchNotification::Batch(batch) => {
                self.handle_batch(notification.peer_id, batch.clone()).await;
            }
        }
    }

    async fn handle_batch(&mut self, peer_id: [u8; 32], batch: SyncBatchShred) {
        if batch.compute_shred_hash() != batch.shred_hash {
            error!(
                "Invalid shred payload hash for slot {} index {}; penalizing peer",
                batch.shred_id.slot, batch.shred_id.index
            );
            self.peer_manager.penalize_invalid_shred(peer_id);
            return;
        }

        let batch_slot = batch.shred_id.slot;
        self.shred_sources.insert(batch.shred_id.clone(), peer_id);

        // If we already have a pending finalization marker for this slot, prioritize
        // staging and finalizing it (even if the slot is far behind `current_slot`).
        if let Some((_, pending)) = self.pending_finalizations.remove(&batch_slot) {
            let slot_len = self.shred_manager.add_shred(batch);
            histogram!("staged_batches_slot_jobs").record(slot_len as f64);
            record_staged_batches_metrics(self.shred_manager.staged_batches());

            if let Err(e) = self.finalize_slot(&pending).await {
                error!(
                    "Failed to finalize pending slot {} after new shred arrival: {}",
                    pending.slot, e
                );
                self.pending_finalizations.insert(pending.slot, pending);
            }
            return;
        }

        let current_slot_value = self.current_slot.load(Ordering::SeqCst).saturating_sub(1);
        if batch_slot < current_slot_value || self.finalized_slots.contains(&batch_slot) {
            info!(
                "Batch for already-finalized slot {} (job_id={}, current_slot={}) - adding to pending pool",
                batch.shred_id.slot, batch.shred_id.index, current_slot_value
            );
            self.pending_batches.insert(batch.shred_id.clone(), batch);
            return;
        }
        let slot_len = self.shred_manager.add_shred(batch);
        histogram!("staged_batches_slot_jobs").record(slot_len as f64);
        record_staged_batches_metrics(self.shred_manager.staged_batches());
    }

    async fn maybe_refetch_missing_finalizer(&self, prev_slot: Option<Slot>, latest_slot: Slot) {
        let current_slot = self.current_slot.load(Ordering::SeqCst);
        let baseline_slot = prev_slot.map(|prev| prev.min(current_slot)).unwrap_or(current_slot);
        let mut cursor = latest_slot;
        let high_water_slot = latest_slot.max(current_slot);
        let cutoff = high_water_slot.saturating_sub(self.finalized_window_slots);

        // Drop any stale in-flight markers older than our retention window to avoid
        // refetching pruned slots.
        self.finalizer_refetching.retain(|slot| *slot > cutoff);

        loop {
            // Scan backwards from the newest observed slot to fill any gaps between
            // the previous head and this marker. Using the last observed head (or
            // the bootstrap `current_slot` for the first marker) bounds the work
            // during cold-start.
            if cursor <= baseline_slot.saturating_add(1) {
                break;
            }

            let target_slot = cursor - 1;
            if target_slot <= cutoff {
                break;
            }

            // Skip slots that are already finalized, or for which we already have a
            // finalization marker pending shred arrival.
            if self.finalized_slots.contains(&target_slot) || self.pending_finalizations.contains_key(&target_slot) {
                cursor = target_slot;
                continue;
            }

            if !self.finalizer_refetching.insert(target_slot) {
                // Another in-flight refetch is already running
                break;
            }

            info!(
                "Finalization missing for slot {} (observed slot {}), attempting refetch",
                target_slot, latest_slot
            );

            let permit = self.refetch_sem.acquire().await.expect("semaphore not closed");
            let fetch_result = get_block_finalizer(&self.peer_manager, target_slot, &self.sequencer_pubkey).await;
            drop(permit);
            self.finalizer_refetching.remove(&target_slot);

            match fetch_result {
                Ok(signed_finalization) => {
                    if let Err(e) = self.finalize_slot(&signed_finalization.finalization).await {
                        error!("Failed to apply refetched finalization for slot {}: {}", target_slot, e);
                        break;
                    }
                    // Continue walking backward to cover chained gaps.
                    cursor = target_slot;
                }
                Err(RefetchErr::NotFound) => {
                    info!(
                        "Finalization not found for slot {} during refetch triggered by slot {}",
                        target_slot, latest_slot
                    );
                    break;
                }
                Err(RefetchErr::Other(e)) => {
                    error!(
                        "Error refetching finalization for slot {} (triggered by slot {}): {}",
                        target_slot, latest_slot, e
                    );
                    break;
                }
            }
        }
    }

    async fn finalize_slot(&self, marker: &SyncFinalization) -> eyre::Result<()> {
        if marker.shred_hashes.len() != marker.num_shreds as usize {
            return Err(eyre!(
                "finalizer shred hash list length mismatch for slot {}: expected {}, got {}",
                marker.slot,
                marker.num_shreds,
                marker.shred_hashes.len()
            ));
        }

        if self.finalized_slots.contains(&marker.slot) {
            return Ok(());
        }

        if let Err(e) = self.finalize_staged_slot_once(marker).await {
            self.pending_finalizations.insert(marker.slot, marker.clone());
            return Err(e);
        }
        self.pending_finalizations.remove(&marker.slot);

        // Track the expected shred count for DB merge planning (only if there is at
        // least one shred)
        if marker.num_shreds > 0 {
            let shred_count = marker.num_shreds as usize;
            let mut guard = self.finalized_job_ids.write().unwrap();
            guard.insert(marker.slot, shred_count);
            gauge!("finalized_job_ids_len").set(guard.len() as f64);
        }

        // Record timestamp for this slot
        self.finalized_timestamps
            .insert(marker.slot, marker.block_unix_timestamp);
        record_dashmap_len(self.finalized_timestamps.as_ref(), "finalized_timestamps_len");

        self.finalized_slots.insert(marker.slot);
        record_dashset_len(self.finalized_slots.as_ref(), "finalized_slots_len");

        // Flush blockhash->signatures into the Bank
        let blockhash_to_signatures = {
            let mut guard = self.blockhash_to_signatures.write().unwrap();
            std::mem::take(&mut *guard)
        };

        {
            let mut bank_writer = self.bank.write().unwrap();
            let bank_slot = bank_writer.get_latest_slot_hash_timestamp().0;

            if marker.slot < bank_slot {
                // Do not move the Bank backwards if we finalize out-of-order.
                bank_writer.set_slot_metadata(marker.slot, marker.hash, marker.block_unix_timestamp);
            } else {
                bank_writer.tick_as_slave(marker.slot, marker.hash, marker.block_unix_timestamp);
            }

            if !blockhash_to_signatures.is_empty() {
                bank_writer.commit_blockhash_to_signatures(blockhash_to_signatures);
            }
            self.current_slot.fetch_max(marker.slot, Ordering::SeqCst);
        }

        // Merge DB chain periodically
        if marker.slot > 0 && marker.slot.is_multiple_of(DB_MERGE_SLOT_INTERVAL) {
            info!("Attempting merge at slot {}", marker.slot);
            counter!("cold_start_merge_attempts_total").increment(1);
            let t_merge = Instant::now();
            let merge_result = {
                let mut db_chain = self.db_chain.write().unwrap();
                info!("Acquired lock. Pre-merge: {}", db_chain.summary());
                let plan = self.finalized_job_ids.read().unwrap().clone();
                let res = db_chain.merge(plan);
                info!("Merge finished; Post-merge: {}", db_chain.summary());
                res
            };
            let latest_slot = match merge_result {
                Ok(latest_slot) => latest_slot,
                Err(e) => {
                    error!("Error merging db_chain at slot {}: {}", marker.slot, e);
                    counter!("cold_start_merge_errors_total").increment(1);
                    None
                }
            };
            histogram!("cold_start_merge_attempt_ms").record(t_merge.elapsed().as_secs_f64() * 1000.0);
            if let Some(latest_slot) = latest_slot {
                info!("Successfully merged db_chain to slot {}", latest_slot);
                counter!("cold_start_merge_success_total").increment(1);
                {
                    let mut guard = self.finalized_job_ids.write().unwrap();
                    let before = guard.len();
                    guard.retain(|slot_key, _| *slot_key > latest_slot);
                    if before != guard.len() {
                        counter!("finalized_job_ids_pruned_total").increment((before - guard.len()) as u64);
                    }
                    gauge!("finalized_job_ids_len").set(guard.len() as f64);
                }
            } else {
                info!("Merge returned None at slot {} (no confirmed slot yet)", marker.slot);
            }
        }

        // Index block metadata
        let t_index_block = Instant::now();
        self.indexer.lock().await.index_block(
            marker.slot,
            marker.block_unix_timestamp,
            marker.hash,
            marker.parent_hash,
        );
        histogram!("slot_index_block_ms").record(t_index_block.elapsed().as_secs_f64() * 1000.0);

        // Record latest processed slot to progress database
        if let Some(ref recorder) = self.progress_recorder {
            if let Err(e) = recorder.record_latest_slot(marker.slot) {
                error!("Failed to record latest slot {}: {}", marker.slot, e);
            }
        }

        Ok(())
    }

    async fn finalize_staged_slot_once(&self, marker: &SyncFinalization) -> eyre::Result<()> {
        let slot = marker.slot;
        let slot_timestamp = marker.block_unix_timestamp;
        let num_shreds = marker.num_shreds;
        let staged_entry = self.shred_manager.staged_batches().remove(&slot);
        record_staged_batches_metrics(self.shred_manager.staged_batches());
        let mut batches = staged_entry
            .map(|(_, b)| {
                histogram!("staged_batches_slot_jobs").record(b.len() as f64);
                b
            })
            .unwrap_or_default();

        let have: HashSet<ShredIndex> = batches.keys().copied().collect();
        let want: HashSet<ShredIndex> = (0..num_shreds as usize).collect();
        let mut missing_shred_indices: HashSet<ShredIndex> = want.difference(&have).copied().collect();
        let mut unresolved_shreds: Vec<ShredIndex> = Vec::new();

        let invalid_shred_indices: Vec<ShredIndex> = batches
            .iter()
            .filter_map(|(shred_index, batch)| match verify_shred_hash(marker, batch) {
                Ok(()) => None,
                Err(ShredValidationError::MissingIndex) => {
                    error!(
                        "Shred index {} out of range for slot {} (num_shreds={}); dropping",
                        shred_index, slot, num_shreds
                    );
                    let shred_id = ShredId::new(slot, *shred_index);
                    if let Some((_, peer_id)) = self.shred_sources.remove(&shred_id) {
                        self.peer_manager.mark_inexist(peer_id);
                    }
                    Some(*shred_index)
                }
                Err(_) => {
                    error!(
                        "Invalid shred hash for slot {} index {}; dropping and refetching",
                        slot, shred_index
                    );
                    let shred_id = ShredId::new(slot, *shred_index);
                    if let Some((_, peer_id)) = self.shred_sources.remove(&shred_id) {
                        self.peer_manager.penalize_invalid_shred(peer_id);
                    }
                    Some(*shred_index)
                }
            })
            .collect();

        if !invalid_shred_indices.is_empty() {
            for shred_index in invalid_shred_indices {
                batches.remove(&shred_index);
                if shred_index < num_shreds as usize {
                    missing_shred_indices.insert(shred_index);
                }
            }
        }

        if !missing_shred_indices.is_empty() {
            info!(
                "Finalizing slot {}, missing {} shred indices, refetching...",
                slot,
                missing_shred_indices.len()
            );
            for shred_index in missing_shred_indices {
                // Check pending pool first before refetching
                let shred_id = ShredId::new(slot, shred_index);
                if let Some((_shred_id, pending_batch)) = self.pending_batches.remove(&shred_id) {
                    match verify_shred_hash(marker, &pending_batch) {
                        Ok(()) => {
                            info!(
                                "Found batch for slot {} shred index {} in pending pool",
                                slot, shred_index
                            );
                            batches.insert(shred_index, pending_batch);
                            continue;
                        }
                        Err(ShredValidationError::MissingIndex) => {
                            error!(
                                "Pending batch missing hash index for slot {} shred index {}; refetching",
                                slot, shred_index
                            );
                            if let Some((_, peer_id)) = self.shred_sources.remove(&shred_id) {
                                self.peer_manager.mark_inexist(peer_id);
                            }
                        }
                        Err(_) => {
                            error!(
                                "Pending batch failed hash verification for slot {} shred index {}; refetching",
                                slot, shred_index
                            );
                            if let Some((_, peer_id)) = self.shred_sources.remove(&shred_id) {
                                self.peer_manager.penalize_invalid_shred(peer_id);
                            }
                        }
                    }
                }

                let permit = self.refetch_sem.acquire().await.expect("semaphore not closed");
                match get_and_decode_batch(&self.peer_manager, slot, shred_index).await {
                    Ok(refetched) => {
                        let maybe_batch = refetched.batch;
                        if maybe_batch.shred_id.slot != slot || maybe_batch.shred_id.index != shred_index {
                            error!(
                                "Refetched batch id mismatch: expected slot {} index {}, got {:?}",
                                slot, shred_index, maybe_batch.shred_id
                            );
                            self.peer_manager.penalize_invalid_shred(refetched.peer_id);
                            unresolved_shreds.push(shred_index);
                        } else {
                            match verify_shred_hash(marker, &maybe_batch) {
                                Ok(()) => {
                                    self.shred_sources
                                        .insert(maybe_batch.shred_id.clone(), refetched.peer_id);
                                    batches.insert(shred_index, maybe_batch);
                                }
                                Err(ShredValidationError::MissingIndex) => {
                                    error!(
                                        "Refetched batch missing hash index for slot {} shred index {}",
                                        slot, shred_index
                                    );
                                    self.peer_manager.mark_inexist(refetched.peer_id);
                                    unresolved_shreds.push(shred_index);
                                }
                                Err(_) => {
                                    error!(
                                        "Refetched batch failed hash verification for slot {} shred index {}",
                                        slot, shred_index
                                    );
                                    self.peer_manager.penalize_invalid_shred(refetched.peer_id);
                                    unresolved_shreds.push(shred_index);
                                }
                            }
                        }
                    }
                    Err(RefetchErr::NotFound) => {
                        unresolved_shreds.push(shred_index);
                    }
                    Err(e) => {
                        error!(
                            "Error refetching missing shard for slot {}. shred index {}: {:?}",
                            slot, shred_index, e
                        );
                        unresolved_shreds.push(shred_index);
                    }
                }
                drop(permit);
            }
        }

        if !unresolved_shreds.is_empty() {
            if let Some(mut entry) = self.shred_manager.staged_batches().get_mut(&slot) {
                for (index, batch) in batches.into_iter() {
                    entry.insert(index, batch);
                }
            } else if !batches.is_empty() {
                self.shred_manager.staged_batches().insert(slot, batches);
            }
            record_staged_batches_metrics(self.shred_manager.staged_batches());
            return Err(eyre!(
                "missing {} shreds after refetch for slot {}",
                unresolved_shreds.len(),
                slot
            ));
        }

        if batches.is_empty() {
            info!("No batches to apply for finalized slot {}", slot);
            return Ok(());
        }

        info!(
            "Finalizing slot {} (timestamp={}) with {} buffered batches",
            slot,
            slot_timestamp,
            batches.len()
        );

        for (shred_index, batch) in batches.into_iter() {
            write_to_bank_cache(&batch, &self.blockhash_to_signatures)?;

            histogram!("tx_batch_effects_count").record(batch.effects.len() as f64);

            let mut shred_db = MemoryDB::new_no_underlying();
            let t_build = Instant::now();
            for effect in batch.effects.iter() {
                let status = match &effect.status {
                    Ok(()) => TransactionStatus::Executed(None, slot),
                    Err(e) => TransactionStatus::Executed(Some(e.clone()), slot),
                };
                let signature = effect.versioned_tx.signatures[0];
                self.subscription_processor.notify_signature_update(&signature, &status);
                self.bank.write().unwrap().write_status_cache(&signature, status);

                for (pubkey, account) in materialize_job_effect_account_updates(effect) {
                    shred_db.write_account(pubkey, account);
                }
            }
            histogram!("tx_batch_build_shard_ms").record(t_build.elapsed().as_secs_f64() * 1000.0);

            let meta = DBMeta::from_shred(slot, shred_index);
            if self.seen_shreds.insert(ShredId::new(slot, shred_index)) {
                record_dashset_len(self.seen_shreds.as_ref(), "seen_shreds_len");
                let mut chain = self.db_chain.write().unwrap();
                let before = chain.len();
                let t_add = Instant::now();
                info!("adding shred {:?}; chain size {} -> {}?", meta, before, before + 1);
                chain.add_db(Arc::new(RwLock::new(shred_db)), meta);
                histogram!("db_chain_add_shred_ms", "source" => "tx_batch")
                    .record(t_add.elapsed().as_secs_f64() * 1000.0);
                counter!("db_chain_shreds_added_total", "source" => "tx_batch").increment(1);
            }

            #[cfg(not(feature = "no_index"))]
            {
                let t_index = Instant::now();
                self.indexer.lock().await.index_transactions(
                    batch.effects.clone(),
                    slot_timestamp,
                    ShredId::new(slot, shred_index),
                );
                histogram!("tx_batch_index_ms").record(t_index.elapsed().as_secs_f64() * 1000.0);
            }
        }

        info!(
            "Completed finalization for slot {} (timestamp={})",
            slot, slot_timestamp
        );

        Ok(())
    }
}

fn write_to_bank_cache(
    parsed: &SyncBatchShred,
    blockhash_to_signatures: &Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
) -> eyre::Result<()> {
    if !parsed.effects.is_empty() {
        let mut signatures = HashMap::new();
        for effect in parsed.effects.iter() {
            let transaction = effect.sanitized_tx()?;
            let signature = *transaction.signature();
            let blockhash = *transaction.message().recent_blockhash();
            signatures.entry(blockhash).or_insert_with(Vec::new).push(signature);
        }
        if !signatures.is_empty() {
            let mut guard = blockhash_to_signatures.write().unwrap();
            let mut added_signatures = 0usize;
            for (bh, sigs) in signatures.into_iter() {
                let entry = guard.entry(bh).or_insert_with(Vec::new);
                let before = entry.len();
                entry.extend(sigs);
                added_signatures += entry.len() - before;
            }
            let pending_blockhashes = guard.len();
            gauge!("blockhash_signature_pending_blockhashes").set(pending_blockhashes as f64);
            if added_signatures > 0 {
                histogram!("blockhash_signature_batch_inserted").record(added_signatures as f64);
            }
        }
    }
    Ok(())
}

fn verify_signed_finalization(sf: &SignedFinalization, sequencer_pubkey: &Pubkey) -> bool {
    if sf.sequencer_pubkey != sequencer_pubkey.to_bytes() {
        return false;
    }
    let msg = match bincode::serialize(&sf.finalization) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let sig = Signature::from(sf.signature);
    sig.verify(sequencer_pubkey.as_ref(), &msg)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShredValidationError {
    PayloadMismatch,
    MissingIndex,
    HashMismatch,
}

fn verify_shred_hash(finalizer: &SyncFinalization, shred: &SyncBatchShred) -> Result<(), ShredValidationError> {
    let expected = shred.compute_shred_hash();
    if expected != shred.shred_hash {
        return Err(ShredValidationError::PayloadMismatch);
    }
    let idx = shred.shred_id.index;
    let Some(hash) = finalizer.shred_hashes.get(idx) else {
        return Err(ShredValidationError::MissingIndex);
    };
    if *hash != shred.shred_hash {
        return Err(ShredValidationError::HashMismatch);
    }
    Ok(())
}

#[derive(Debug)]
pub(super) enum RefetchErr {
    NotFound,
    #[allow(dead_code)]
    Other(String),
}

struct RefetchedBatch {
    batch: SyncBatchShred,
    peer_id: [u8; 32],
}

pub(super) async fn get_block_finalizer(
    peer_manager: &Arc<PeerManager>,
    slot: u64,
    sequencer_pubkey: &Pubkey,
) -> std::result::Result<SignedFinalization, RefetchErr> {
    let mut saw_not_found = false;
    let mut saw_other_error = false;
    let peers = peer_manager.pick_refetch_peers();
    if peers.is_empty() {
        return Err(RefetchErr::Other("no peers available for finalizer refetch".into()));
    }

    for peer in peers {
        let mut client = peer.rpc_client.lock().await;
        match client.get_block_finalizer(slot).await {
            Ok(signed_finalization) => {
                if verify_signed_finalization(&signed_finalization, sequencer_pubkey) {
                    if signed_finalization.finalization.slot != slot {
                        peer_manager.penalize_invalid_finalizer(peer.node_id);
                        continue;
                    }
                    if signed_finalization.finalization.shred_hashes.len() !=
                        signed_finalization.finalization.num_shreds as usize
                    {
                        peer_manager.penalize_invalid_finalizer(peer.node_id);
                        continue;
                    }
                    if let Ok(size) = bincode::serialized_size(&signed_finalization) {
                        peer_manager.observe_bytes(peer.node_id, size);
                    }
                    peer_manager.observe_signed_finalization(peer.node_id, slot);
                    return Ok(signed_finalization);
                }
                peer_manager.penalize_invalid_finalizer(peer.node_id);
                continue;
            }
            Err(err) => {
                if let Some(status) = err.downcast_ref::<Status>() {
                    match status.code() {
                        Code::NotFound => {
                            saw_not_found = true;
                            peer_manager.mark_inexist(peer.node_id);
                        }
                        Code::ResourceExhausted => {
                            saw_other_error = true;
                            peer_manager.mark_rate_limit(peer.node_id);
                        }
                        _ => {
                            saw_other_error = true;
                            peer_manager.mark_failure(peer.node_id);
                        }
                    }
                } else {
                    saw_other_error = true;
                    peer_manager.mark_failure(peer.node_id);
                }
            }
        }
    }

    Err(if saw_other_error {
        RefetchErr::Other("no servers returned finalization".into())
    } else if saw_not_found {
        RefetchErr::NotFound
    } else {
        RefetchErr::Other("no servers returned finalization".into())
    })
}

async fn get_and_decode_batch(
    peer_manager: &Arc<PeerManager>,
    slot: u64,
    job_id: usize,
) -> std::result::Result<RefetchedBatch, RefetchErr> {
    let mut saw_not_found = false;
    let mut saw_other_error = false;
    let peers = peer_manager.pick_refetch_peers();
    if peers.is_empty() {
        return Err(RefetchErr::Other("no peers available for batch refetch".into()));
    }

    for peer in peers {
        let mut client = peer.rpc_client.lock().await;
        match client.get_batch_shred(ShredId::new(slot, job_id)).await {
            Ok(resp) => {
                if resp.compute_shred_hash() != resp.shred_hash {
                    peer_manager.penalize_invalid_shred(peer.node_id);
                    continue;
                }
                if let Ok(size) = bincode::serialized_size(&resp) {
                    peer_manager.observe_bytes(peer.node_id, size);
                }
                return Ok(RefetchedBatch {
                    batch: resp,
                    peer_id: peer.node_id,
                });
            }
            Err(err) => {
                if let Some(status) = err.downcast_ref::<Status>() {
                    match status.code() {
                        Code::NotFound => {
                            saw_not_found = true;
                            peer_manager.mark_inexist(peer.node_id)
                        }
                        Code::ResourceExhausted => {
                            saw_other_error = true;
                            peer_manager.mark_rate_limit(peer.node_id)
                        }
                        _ => {
                            saw_other_error = true;
                            peer_manager.mark_failure(peer.node_id)
                        }
                    }
                } else {
                    saw_other_error = true;
                    peer_manager.mark_failure(peer.node_id);
                }
            }
        }
    }

    Err(if saw_other_error {
        RefetchErr::Other("no servers returned batch".into())
    } else if saw_not_found {
        RefetchErr::NotFound
    } else {
        RefetchErr::Other("no servers returned batch".into())
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::SocketAddr,
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
    };

    use dashmap::{DashMap, DashSet};
    use infinisvm_sync::grpc::{
        service::{
            GetBatchShredRequest, GetBlockFinalizerRequest, InfiniSvmService, InfiniSvmServiceServer,
            InjectCommitBatchRequest, InjectCommitBatchResponse, SubscribeTransactionBatchRequest,
        },
        SyncClient,
    };
    use infinisvm_types::sync::{
        CommitBatchNotification, GetPeerStatusRequest, GetPeerStatusResponse, PeerStatus, ShredId, SignedFinalization,
        SyncBatchShred, SyncFinalization,
    };
    use solana_sdk::{
        hash::hashv,
        signature::{Keypair, Signer},
    };
    use tokio::{
        net::TcpListener,
        sync::{Mutex, Semaphore},
    };
    use tokio_stream::{wrappers::TcpListenerStream, Stream};
    use tonic::{Code, Response, Status};

    use super::{get_block_finalizer, shred_manager::ShredManager, SlotProcessor, FINALIZED_SLOTS_WINDOW_SLOTS};
    use crate::p2p::PeerManager;

    #[derive(Clone)]
    enum FinalizerBehavior {
        Ok,
        Mismatch,
        Error(Code),
    }

    #[derive(Clone)]
    enum BatchBehavior {
        NotFound,
        Error(Code),
    }

    #[derive(Clone)]
    struct FinalizerService {
        behavior: FinalizerBehavior,
        batch_behavior: BatchBehavior,
        status: PeerStatus,
        keypair: Arc<Keypair>,
    }

    impl FinalizerService {
        fn new(keypair: Arc<Keypair>, grpc_addr: String, status_slot: u64, behavior: FinalizerBehavior) -> Self {
            let status_finalization = build_signed_finalization(&keypair, status_slot);
            let node_id = hashv(&[grpc_addr.as_bytes()]).to_bytes();
            let status = PeerStatus {
                node_id,
                grpc_addr,
                rate_limit_per_sec: 0,
                rate_limit_burst: 0,
                latest_signed_finalization: Some(status_finalization.clone()),
                ancestry_canary: Some(status_finalization),
                stream_parent: None,
                canary_path: vec![node_id],
                topology_pubkey: keypair.pubkey().to_bytes(),
                ancestry_delegations: Vec::new(),
                observed_head: status_slot,
                capabilities: 0,
                setup: None,
            };
            Self {
                behavior,
                batch_behavior: BatchBehavior::NotFound,
                status,
                keypair,
            }
        }

        fn with_batch_behavior(mut self, batch_behavior: BatchBehavior) -> Self {
            self.batch_behavior = batch_behavior;
            self
        }
    }

    fn build_signed_finalization(keypair: &Keypair, slot: u64) -> SignedFinalization {
        let finalization = SyncFinalization {
            slot,
            num_shreds: 0,
            hash: solana_sdk::hash::Hash::new_unique(),
            parent_hash: solana_sdk::hash::Hash::new_unique(),
            block_unix_timestamp: 0,
            shred_hashes: vec![],
        };
        let msg = bincode::serialize(&finalization).expect("serialize finalization");
        let sig = keypair.sign_message(&msg);
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(sig.as_ref());
        SignedFinalization {
            finalization,
            sequencer_pubkey: keypair.pubkey().to_bytes(),
            signature: sig_bytes,
        }
    }

    #[tonic::async_trait]
    impl InfiniSvmService for FinalizerService {
        type SubscribeTransactionBatchesStream =
            Pin<Box<dyn Stream<Item = Result<Arc<CommitBatchNotification>, Status>> + Send + 'static>>;

        async fn subscribe_commit_batch_notifications(
            &self,
            _request: tonic::Request<SubscribeTransactionBatchRequest>,
        ) -> Result<Response<Self::SubscribeTransactionBatchesStream>, Status> {
            Ok(Response::new(Box::pin(tokio_stream::empty())))
        }

        async fn get_batch_shred(
            &self,
            _request: tonic::Request<GetBatchShredRequest>,
        ) -> Result<Response<SyncBatchShred>, Status> {
            match self.batch_behavior {
                BatchBehavior::NotFound => Err(Status::not_found("not found")),
                BatchBehavior::Error(code) => Err(Status::new(code, "e2e error")),
            }
        }

        async fn get_block_finalizer(
            &self,
            request: tonic::Request<GetBlockFinalizerRequest>,
        ) -> Result<Response<SignedFinalization>, Status> {
            let slot = request.into_inner().slot;
            match self.behavior {
                FinalizerBehavior::Error(code) => Err(Status::new(code, "e2e error")),
                FinalizerBehavior::Mismatch => Ok(Response::new(build_signed_finalization(&self.keypair, slot + 1))),
                FinalizerBehavior::Ok => Ok(Response::new(build_signed_finalization(&self.keypair, slot))),
            }
        }

        async fn get_peer_status(
            &self,
            _request: tonic::Request<GetPeerStatusRequest>,
        ) -> Result<Response<GetPeerStatusResponse>, Status> {
            Ok(Response::new(GetPeerStatusResponse {
                status: self.status.clone(),
                delegation: None,
            }))
        }

        async fn inject_commit_batch_notification(
            &self,
            _request: tonic::Request<InjectCommitBatchRequest>,
        ) -> Result<Response<InjectCommitBatchResponse>, Status> {
            Ok(Response::new(InjectCommitBatchResponse { ok: true }))
        }
    }

    async fn connect_peer(addr: SocketAddr) -> (SyncClient, SyncClient, PeerStatus) {
        let url = format!("http://{addr}");
        let status_client = SyncClient::connect(&url).await.expect("status client");
        let mut status_client_mut = status_client;
        let status = status_client_mut.get_peer_status().await.expect("status");
        let stream_client = SyncClient::connect(&url).await.expect("stream client");
        let rpc_client = SyncClient::connect(&url).await.expect("rpc client");
        (stream_client, rpc_client, status)
    }

    #[tokio::test]
    async fn refetch_slot_mismatch_penalizes_peer() {
        let keypair = Arc::new(Keypair::new());
        let listener_bad = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr_bad = listener_bad.local_addr().expect("addr");
        let service_bad = FinalizerService::new(
            Arc::clone(&keypair),
            addr_bad.to_string(),
            200,
            FinalizerBehavior::Mismatch,
        );
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service_bad);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener_bad))
                .await
                .expect("serve");
        });

        let listener_good = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr_good = listener_good.local_addr().expect("addr");
        let service_good =
            FinalizerService::new(Arc::clone(&keypair), addr_good.to_string(), 100, FinalizerBehavior::Ok);
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service_good);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener_good))
                .await
                .expect("serve");
        });

        let sequencer_pubkey = keypair.pubkey();
        let manager = PeerManager::new([9u8; 32], sequencer_pubkey);

        let (stream_bad, rpc_bad, status_bad) = connect_peer(addr_bad).await;
        manager.upsert_peer(
            status_bad.node_id,
            status_bad.grpc_addr.clone(),
            Arc::new(Mutex::new(stream_bad)),
            Arc::new(Mutex::new(rpc_bad)),
            Some(status_bad),
        );

        let (stream_good, rpc_good, status_good) = connect_peer(addr_good).await;
        manager.upsert_peer(
            status_good.node_id,
            status_good.grpc_addr.clone(),
            Arc::new(Mutex::new(stream_good)),
            Arc::new(Mutex::new(rpc_good)),
            Some(status_good),
        );

        let target_slot = 55;
        let result = get_block_finalizer(&Arc::new(manager), target_slot, &sequencer_pubkey).await;
        let signed = result.expect("refetch success");
        assert_eq!(signed.finalization.slot, target_slot);
    }

    #[tokio::test]
    async fn refetch_resource_exhausted_continues() {
        let keypair = Arc::new(Keypair::new());
        let listener_err = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr_err = listener_err.local_addr().expect("addr");
        let service_err = FinalizerService::new(
            Arc::clone(&keypair),
            addr_err.to_string(),
            120,
            FinalizerBehavior::Error(Code::ResourceExhausted),
        );
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service_err);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener_err))
                .await
                .expect("serve");
        });

        let listener_ok = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr_ok = listener_ok.local_addr().expect("addr");
        let service_ok = FinalizerService::new(Arc::clone(&keypair), addr_ok.to_string(), 110, FinalizerBehavior::Ok);
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service_ok);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener_ok))
                .await
                .expect("serve");
        });

        let sequencer_pubkey = keypair.pubkey();
        let manager = PeerManager::new([9u8; 32], sequencer_pubkey);

        let (stream_err, rpc_err, status_err) = connect_peer(addr_err).await;
        manager.upsert_peer(
            status_err.node_id,
            status_err.grpc_addr.clone(),
            Arc::new(Mutex::new(stream_err)),
            Arc::new(Mutex::new(rpc_err)),
            Some(status_err),
        );

        let (stream_ok, rpc_ok, status_ok) = connect_peer(addr_ok).await;
        manager.upsert_peer(
            status_ok.node_id,
            status_ok.grpc_addr.clone(),
            Arc::new(Mutex::new(stream_ok)),
            Arc::new(Mutex::new(rpc_ok)),
            Some(status_ok),
        );

        let target_slot = 77;
        let result = get_block_finalizer(&Arc::new(manager), target_slot, &sequencer_pubkey).await;
        let signed = result.expect("refetch success");
        assert_eq!(signed.finalization.slot, target_slot);
    }

    #[tokio::test]
    async fn refetch_finalizer_not_found_with_other_error_returns_other() {
        let keypair = Arc::new(Keypair::new());

        let listener_not_found = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr_not_found = listener_not_found.local_addr().expect("addr");
        let service_not_found = FinalizerService::new(
            Arc::clone(&keypair),
            addr_not_found.to_string(),
            200,
            FinalizerBehavior::Error(Code::NotFound),
        );
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service_not_found);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener_not_found))
                .await
                .expect("serve");
        });

        let listener_rate_limited = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr_rate_limited = listener_rate_limited.local_addr().expect("addr");
        let service_rate_limited = FinalizerService::new(
            Arc::clone(&keypair),
            addr_rate_limited.to_string(),
            100,
            FinalizerBehavior::Error(Code::ResourceExhausted),
        );
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service_rate_limited);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener_rate_limited))
                .await
                .expect("serve");
        });

        let sequencer_pubkey = keypair.pubkey();
        let manager = PeerManager::new([9u8; 32], sequencer_pubkey);

        let (stream_nf, rpc_nf, status_nf) = connect_peer(addr_not_found).await;
        manager.upsert_peer(
            status_nf.node_id,
            status_nf.grpc_addr.clone(),
            Arc::new(Mutex::new(stream_nf)),
            Arc::new(Mutex::new(rpc_nf)),
            Some(status_nf),
        );

        let (stream_rl, rpc_rl, status_rl) = connect_peer(addr_rate_limited).await;
        manager.upsert_peer(
            status_rl.node_id,
            status_rl.grpc_addr.clone(),
            Arc::new(Mutex::new(stream_rl)),
            Arc::new(Mutex::new(rpc_rl)),
            Some(status_rl),
        );

        let target_slot = 88;
        let result = get_block_finalizer(&Arc::new(manager), target_slot, &sequencer_pubkey).await;
        assert!(matches!(result, Err(super::RefetchErr::Other(_))));
    }

    #[tokio::test]
    async fn batch_refetch_resource_exhausted_returns_other() {
        let keypair = Arc::new(Keypair::new());
        let listener_err = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr_err = listener_err.local_addr().expect("addr");
        let service_err = FinalizerService::new(Arc::clone(&keypair), addr_err.to_string(), 120, FinalizerBehavior::Ok)
            .with_batch_behavior(BatchBehavior::Error(Code::ResourceExhausted));
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service_err);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener_err))
                .await
                .expect("serve");
        });

        let sequencer_pubkey = keypair.pubkey();
        let peer_manager = Arc::new(PeerManager::new([9u8; 32], sequencer_pubkey));

        let (stream, rpc, status) = connect_peer(addr_err).await;
        peer_manager.upsert_peer(
            status.node_id,
            status.grpc_addr.clone(),
            Arc::new(Mutex::new(stream)),
            Arc::new(Mutex::new(rpc)),
            Some(status),
        );

        let result = super::get_and_decode_batch(&peer_manager, 42, 0).await;
        assert!(matches!(result, Err(super::RefetchErr::Other(_))));
    }

    #[tokio::test]
    async fn signed_finalization_allows_empty_slot_without_evicting_peer() {
        struct NoopIndexer;

        impl infinisvm_core::indexer::Indexer for NoopIndexer {}

        let keypair = Arc::new(Keypair::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let service = FinalizerService::new(Arc::clone(&keypair), addr.to_string(), 100, FinalizerBehavior::Ok);
        tokio::spawn(async move {
            let svc = InfiniSvmServiceServer::new(service);
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve");
        });

        let sequencer_pubkey = keypair.pubkey();
        let peer_manager = Arc::new(PeerManager::new([9u8; 32], sequencer_pubkey));

        let (stream, rpc, status) = connect_peer(addr).await;
        let peer_id = status.node_id;
        peer_manager.upsert_peer(
            peer_id,
            status.grpc_addr.clone(),
            Arc::new(Mutex::new(stream)),
            Arc::new(Mutex::new(rpc)),
            Some(status),
        );

        let db_chain = Arc::new(RwLock::new(infinisvm_db::db_chain::DBChain::new()));
        let indexer = Arc::new(Mutex::new(NoopIndexer));
        let exit = Arc::new(AtomicBool::new(false));
        let bank = Arc::new(RwLock::new(infinisvm_core::bank::Bank::new_slave(Arc::clone(&exit))));
        let subscription_processor = Arc::new(infinisvm_core::subscription::SubscriptionProcessor::new());

        let seen_shreds = Arc::new(DashSet::new());
        let staged_batches: Arc<DashMap<u64, BTreeMap<usize, SyncBatchShred>>> = Arc::new(DashMap::new());
        let shred_manager = ShredManager::new(Arc::clone(&staged_batches));
        let finalized_slots = Arc::new(DashSet::new());
        let finalized_timestamps = Arc::new(DashMap::new());
        let finalized_job_ids = Arc::new(RwLock::new(hashbrown::HashMap::new()));
        let refetch_sem = Arc::new(Semaphore::new(1));
        let finalizer_refetching = Arc::new(DashSet::new());
        let blockhash_to_signatures = Arc::new(RwLock::new(hashbrown::HashMap::new()));
        let current_slot = Arc::new(AtomicU64::new(0));
        let pending_batches = Arc::new(DashMap::new());
        let shred_sources = Arc::new(DashMap::new());
        let pending_finalizations = Arc::new(DashMap::new());

        // Simulate a peer incorrectly streaming a shred for a slot that the
        // sequencer finalized as empty. We should drop the shred and still
        // finalize the slot.
        let mut out_of_range = SyncBatchShred {
            shred_id: ShredId::new(1, 0),
            worker_id: 0,
            effects: vec![],
            shred_hash: [0u8; 32],
        };
        out_of_range.shred_hash = out_of_range.compute_shred_hash();
        staged_batches
            .entry(1)
            .or_default()
            .insert(out_of_range.shred_id.index, out_of_range.clone());
        shred_sources.insert(out_of_range.shred_id.clone(), peer_id);

        let mut processor = SlotProcessor::new(
            db_chain,
            indexer,
            bank,
            subscription_processor,
            seen_shreds,
            shred_manager,
            Arc::clone(&finalized_slots),
            finalized_timestamps,
            finalized_job_ids,
            Arc::clone(&peer_manager),
            refetch_sem,
            finalizer_refetching,
            FINALIZED_SLOTS_WINDOW_SLOTS,
            blockhash_to_signatures,
            Arc::clone(&current_slot),
            pending_batches,
            shred_sources,
            pending_finalizations,
            sequencer_pubkey,
            None,
        );

        let slot = 1;
        let signed = build_signed_finalization(&keypair, slot);
        let notification = crate::p2p::PeerNotification {
            peer_id,
            peer_addr: "127.0.0.1".to_string(),
            notification: Arc::new(CommitBatchNotification::SignedFinalization(signed)),
        };

        processor.handle_notification(notification).await;

        assert!(peer_manager.has_peer(peer_id));
        assert!(finalized_slots.contains(&slot));
        assert_eq!(current_slot.load(Ordering::SeqCst), slot);

        exit.store(true, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn bank_slot_is_monotonic_across_out_of_order_finalizations() {
        struct NoopIndexer;

        impl infinisvm_core::indexer::Indexer for NoopIndexer {}

        let db_chain = Arc::new(RwLock::new(infinisvm_db::db_chain::DBChain::new()));
        let indexer = Arc::new(Mutex::new(NoopIndexer));
        let exit = Arc::new(AtomicBool::new(false));
        let bank = Arc::new(RwLock::new(infinisvm_core::bank::Bank::new_slave(Arc::clone(&exit))));
        let subscription_processor = Arc::new(infinisvm_core::subscription::SubscriptionProcessor::new());

        let sequencer_pubkey = Keypair::new().pubkey();
        let peer_manager = Arc::new(PeerManager::new([9u8; 32], sequencer_pubkey));

        let seen_shreds = Arc::new(DashSet::new());
        let staged_batches: Arc<DashMap<u64, BTreeMap<usize, SyncBatchShred>>> = Arc::new(DashMap::new());
        let shred_manager = ShredManager::new(Arc::clone(&staged_batches));
        let finalized_slots = Arc::new(DashSet::new());
        let finalized_timestamps = Arc::new(DashMap::new());
        let finalized_job_ids = Arc::new(RwLock::new(hashbrown::HashMap::new()));
        let refetch_sem = Arc::new(Semaphore::new(1));
        let finalizer_refetching = Arc::new(DashSet::new());
        let blockhash_to_signatures = Arc::new(RwLock::new(hashbrown::HashMap::new()));
        let current_slot = Arc::new(AtomicU64::new(0));
        let pending_batches = Arc::new(DashMap::new());
        let shred_sources = Arc::new(DashMap::new());
        let pending_finalizations = Arc::new(DashMap::new());

        let mut batch_1 = SyncBatchShred {
            shred_id: ShredId::new(1, 0),
            worker_id: 0,
            effects: vec![],
            shred_hash: [0u8; 32],
        };
        batch_1.shred_hash = batch_1.compute_shred_hash();
        staged_batches
            .entry(batch_1.shred_id.slot)
            .or_default()
            .insert(batch_1.shred_id.index, batch_1.clone());

        let mut batch_2 = SyncBatchShred {
            shred_id: ShredId::new(2, 0),
            worker_id: 0,
            effects: vec![],
            shred_hash: [0u8; 32],
        };
        batch_2.shred_hash = batch_2.compute_shred_hash();
        staged_batches
            .entry(batch_2.shred_id.slot)
            .or_default()
            .insert(batch_2.shred_id.index, batch_2.clone());

        let processor = SlotProcessor::new(
            db_chain,
            indexer,
            Arc::clone(&bank),
            subscription_processor,
            seen_shreds,
            shred_manager,
            Arc::clone(&finalized_slots),
            finalized_timestamps,
            finalized_job_ids,
            peer_manager,
            refetch_sem,
            finalizer_refetching,
            FINALIZED_SLOTS_WINDOW_SLOTS,
            blockhash_to_signatures,
            Arc::clone(&current_slot),
            pending_batches,
            shred_sources,
            pending_finalizations,
            sequencer_pubkey,
            None,
        );

        let finalization_2 = SyncFinalization {
            slot: 2,
            num_shreds: 1,
            hash: solana_sdk::hash::Hash::new_unique(),
            parent_hash: solana_sdk::hash::Hash::new_unique(),
            block_unix_timestamp: 2,
            shred_hashes: vec![batch_2.shred_hash],
        };

        let finalization_1 = SyncFinalization {
            slot: 1,
            num_shreds: 1,
            hash: solana_sdk::hash::Hash::new_unique(),
            parent_hash: solana_sdk::hash::Hash::new_unique(),
            block_unix_timestamp: 1,
            shred_hashes: vec![batch_1.shred_hash],
        };

        processor.finalize_slot(&finalization_2).await.expect("finalize slot 2");
        assert_eq!(bank.read().unwrap().get_latest_slot_hash_timestamp().0, 2);
        assert_eq!(current_slot.load(Ordering::SeqCst), 2);

        // Apply an older finalization after a newer one.
        processor.finalize_slot(&finalization_1).await.expect("finalize slot 1");
        assert_eq!(bank.read().unwrap().get_latest_slot_hash_timestamp().0, 2);
        assert_eq!(current_slot.load(Ordering::SeqCst), 2);
        assert!(finalized_slots.contains(&1));
        assert!(finalized_slots.contains(&2));

        exit.store(true, Ordering::Relaxed);
    }
}
