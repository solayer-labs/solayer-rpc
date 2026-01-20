use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use dashmap::{DashMap, DashSet};
use hashbrown::HashMap;
use infinisvm_core::{
    bank::{Bank, TransactionStatus},
    indexer::Indexer,
    subscription::SubscriptionProcessor,
};
use infinisvm_db::{
    db_chain::{DBChain, DBMeta},
    in_memory_db::NoopDB,
    Database, MemoryDB,
};
use infinisvm_logger::{error, info};
use infinisvm_sync::grpc::client::SyncClient;
use infinisvm_types::sync::{CommitBatchNotification, ShredId, ShredIndex, SyncBatchShred, SyncFinalization};
use metrics::{counter, gauge, histogram};
use solana_sdk::{clock::Slot, hash::Hash, signature::Signature};
use tokio::{
    sync::{mpsc::Receiver, Mutex, Semaphore},
    task::JoinHandle,
};
use tonic::{Code, Status};

use super::utils::{record_dashmap_len, record_dashset_len, record_staged_batches_metrics};
use crate::cold_start::slots_sync_progress::SlotsSyncProgressRecorder;

mod shred_manager;
use self::shred_manager::ShredManager;

const DB_MERGE_SLOT_INTERVAL: u64 = 4;
const FINALIZED_SLOTS_WINDOW_SLOTS: u64 = 1000;

/// Spawn the single transaction processor and the prune task.
///
/// Returns the handles for:
///  - the processor task
///  - the prune task
pub(super) fn spawn_tx_processors(
    receiver: Receiver<Arc<CommitBatchNotification>>,
    db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    indexer: Arc<Mutex<dyn Indexer>>,
    bank: Arc<RwLock<Bank>>,
    subscription_processor: Arc<SubscriptionProcessor>,
    seen_shreds: Arc<DashSet<ShredId>>,
    staged_batches: Arc<DashMap<Slot, BTreeMap<ShredIndex, SyncBatchShred>>>,
    finalized_slots: Arc<DashSet<Slot>>,
    finalized_timestamps: Arc<DashMap<Slot, u64>>,
    finalized_job_ids: Arc<RwLock<HashMap<u64, ShredIndex>>>,
    refetch_pool: Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    finalizer_refetching: Arc<DashSet<Slot>>,
    blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    current_slot: Arc<AtomicU64>,
    pending_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
    progress_recorder: Option<SlotsSyncProgressRecorder>,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();

    info!("Starting single transaction batch processing thread");

    let shred_manager = ShredManager::new(staged_batches.clone());
    let refetch_sem = Arc::new(Semaphore::new(16));

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
        refetch_pool.clone(),
        refetch_sem.clone(),
        finalizer_refetching.clone(),
        FINALIZED_SLOTS_WINDOW_SLOTS,
        blockhash_to_signatures.clone(),
        current_slot.clone(),
        pending_batches.clone(),
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
            .unwrap_or(300);
        let staged_ttl = std::env::var("STAGED_BATCHES_TTL_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(50);
        let finalized_ids_window = std::env::var("FINALIZED_JOB_IDS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1000);
        let pending_batches_ttl = std::env::var("PENDING_BATCHES_TTL_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100);

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

    refetch_pool: Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    refetch_sem: Arc<Semaphore>,
    finalizer_refetching: Arc<DashSet<Slot>>,
    finalized_window_slots: u64,
    blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    current_slot: Arc<AtomicU64>,
    pending_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
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
        refetch_pool: Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
        refetch_sem: Arc<Semaphore>,
        finalizer_refetching: Arc<DashSet<Slot>>,
        finalized_window_slots: u64,
        blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
        current_slot: Arc<AtomicU64>,
        pending_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
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
            refetch_pool,
            refetch_sem,
            finalizer_refetching,
            finalized_window_slots,
            blockhash_to_signatures,
            current_slot,
            pending_batches,
            progress_recorder,
        }
    }

    async fn run(mut self, mut receiver: Receiver<Arc<CommitBatchNotification>>) {
        info!("Transaction batch processor started");
        while let Some(notification) = receiver.recv().await {
            self.handle_notification(notification).await;
        }
        info!("Transaction batch processor terminated (channel closed)");
    }

    async fn handle_notification(&mut self, notification: Arc<CommitBatchNotification>) {
        match notification.as_ref() {
            CommitBatchNotification::Finalization(marker) => {
                info!(
                    "Received finalization marker for slot {}, job_ids=0-{:?}",
                    marker.slot,
                    marker.num_shreds - 1
                );
                self.maybe_refetch_missing_finalizer(marker.slot).await;
                if let Err(e) = self.finalize_slot(marker).await {
                    error!("Failed to finalize slot {}: {}", marker.slot, e);
                }
            }
            CommitBatchNotification::Batch(batch) => {
                self.handle_batch(batch.clone()).await;
            }
        }
    }

    async fn handle_batch(&mut self, batch: SyncBatchShred) {
        let current_slot_value = self.current_slot.load(Ordering::SeqCst).saturating_sub(1);
        if batch.shred_id.slot < current_slot_value || self.finalized_slots.contains(&batch.shred_id.slot) {
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

    async fn maybe_refetch_missing_finalizer(&self, latest_slot: Slot) {
        let mut cursor = latest_slot;
        let high_water_slot = latest_slot.max(self.current_slot.load(Ordering::SeqCst));
        let cutoff = high_water_slot.saturating_sub(self.finalized_window_slots);

        // Drop any stale in-flight markers older than our retention window to avoid
        // refetching pruned slots.
        self.finalizer_refetching.retain(|slot| *slot > cutoff);

        loop {
            if cursor < 5 {
                break;
            }

            let target_slot = cursor - 5;
            if target_slot <= cutoff {
                break;
            }

            if self.finalized_slots.contains(&target_slot) {
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
            let fetch_result = get_block_finalizer(&self.refetch_pool, target_slot).await;
            drop(permit);
            self.finalizer_refetching.remove(&target_slot);

            match fetch_result {
                Ok(finalization) => {
                    if let Err(e) = self.finalize_slot(&finalization).await {
                        error!("Failed to apply refetched finalization for slot {}: {}", target_slot, e);
                        break;
                    }
                    // Continue walking backward to cover chained gaps (e.g., N+5 and N both
                    // missing)
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
        // Track max job id for DB merge planning (only if there is at least one shred)
        if marker.num_shreds > 0 {
            let max_job_id = marker.num_shreds - 1;
            let mut guard = self.finalized_job_ids.write().unwrap();
            guard.insert(marker.slot, max_job_id as usize);
            gauge!("finalized_job_ids_len").set(guard.len() as f64);
        }

        // Record timestamp for this slot
        self.finalized_timestamps
            .insert(marker.slot, marker.block_unix_timestamp);
        record_dashmap_len(self.finalized_timestamps.as_ref(), "finalized_timestamps_len");

        // Only apply staged batches once per slot
        let is_first_finalization = self.finalized_slots.insert(marker.slot);
        if is_first_finalization {
            record_dashset_len(self.finalized_slots.as_ref(), "finalized_slots_len");
            self.finalize_staged_slot_once(marker.slot, marker.block_unix_timestamp, marker.num_shreds)
                .await?;
        }

        // Flush blockhash->signatures into the Bank
        let blockhash_to_signatures = {
            let mut guard = self.blockhash_to_signatures.write().unwrap();
            std::mem::take(&mut *guard)
        };

        {
            let mut bank_writer = self.bank.write().unwrap();
            bank_writer.tick_as_slave(marker.slot, marker.hash, marker.block_unix_timestamp);
            if !blockhash_to_signatures.is_empty() {
                bank_writer.commit_blockhash_to_signatures(blockhash_to_signatures);
            }
            self.current_slot.store(marker.slot, Ordering::SeqCst);
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

    async fn finalize_staged_slot_once(&self, slot: Slot, slot_timestamp: u64, num_shreds: u64) -> eyre::Result<()> {
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
        let missing_shred_indices: Vec<ShredIndex> = want.difference(&have).copied().collect();

        if !missing_shred_indices.is_empty() {
            info!(
                "Finalizing slot {}, missing {} shred indices, refetching...",
                slot,
                missing_shred_indices.len()
            );
            for shred_index in missing_shred_indices {
                // Check pending pool first before refetching
                if let Some((_shred_id, pending_batch)) = self.pending_batches.remove(&ShredId::new(slot, shred_index))
                {
                    info!(
                        "Found batch for slot {} shred index {} in pending pool",
                        slot, shred_index
                    );
                    batches.insert(shred_index, pending_batch);
                    continue;
                }

                let permit = self.refetch_sem.acquire().await.expect("semaphore not closed");
                match get_and_decode_batch(&self.refetch_pool, slot, shred_index).await {
                    Ok(maybe_batch) => {
                        batches.insert(shred_index, maybe_batch);
                    }
                    Err(RefetchErr::NotFound) => {
                        batches.insert(
                            shred_index,
                            SyncBatchShred {
                                shred_id: ShredId::new(slot, shred_index),
                                worker_id: 0,
                                effects: vec![],
                            },
                        );
                    }
                    Err(e) => {
                        error!(
                            "Error refetching missing shard for slot {}. shred index {}: {:?}",
                            slot, shred_index, e
                        );
                    }
                }
                drop(permit);
            }
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

                let pre_accounts = effect.job_effect_diff.pre_accounts.clone();
                let job_effect_diff = effect.job_effect_diff.clone();

                for ((pubkey, account), diffs) in pre_accounts.into_iter().zip(job_effect_diff.diffs.into_iter()) {
                    let mut account = account.unwrap_or_default();
                    for diff in diffs {
                        diff.apply_to_account(&mut account);
                    }
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

#[derive(Debug)]
pub(super) enum RefetchErr {
    NotFound,
    #[allow(dead_code)]
    Other(String),
}

pub(super) async fn get_block_finalizer(
    pool: &Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    slot: u64,
) -> std::result::Result<SyncFinalization, RefetchErr> {
    let mut saw_not_found = false;
    for client_mutex in pool.iter() {
        let mut client = client_mutex.lock().await;
        match client.get_block_finalizer(slot).await {
            Ok(finalization) => {
                return Ok(finalization);
            }
            Err(err) => {
                if let Some(status) = err.downcast_ref::<Status>() {
                    if status.code() == Code::NotFound {
                        saw_not_found = true;
                    }
                }
            }
        }
    }

    Err(if saw_not_found {
        RefetchErr::NotFound
    } else {
        RefetchErr::Other("no servers returned finalization".into())
    })
}

pub(super) async fn get_and_decode_batch(
    pool: &Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    slot: u64,
    job_id: usize,
) -> std::result::Result<SyncBatchShred, RefetchErr> {
    let mut saw_not_found = false;
    let mut notification_opt = None;
    for client_mutex in pool.iter() {
        let mut client = client_mutex.lock().await;
        for s in [slot, slot.saturating_sub(1), slot.saturating_add(1)] {
            match client.get_batch_shred(ShredId::new(s, job_id)).await {
                Ok(resp) => {
                    notification_opt = Some(resp);
                    break;
                }
                Err(_) => {
                    // TODO: Better error handling
                    saw_not_found = true;
                    continue;
                }
            }
        }
        if notification_opt.is_some() {
            break;
        }
    }
    let notification = if let Some(n) = notification_opt {
        n
    } else {
        return Err(if saw_not_found {
            RefetchErr::NotFound
        } else {
            RefetchErr::Other("no servers returned batch".into())
        });
    };
    Ok(notification)
}
