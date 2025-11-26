use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Instant,
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
use infinisvm_logger::{debug, error, info, trace};
use infinisvm_sync::{
    grpc::{batch_subscriber::process_commit_notification, client::SyncClient},
    types::{SerializableBatch, SerializableNotification},
};
use infinisvm_types::sync::grpc::CommitBatchNotification;
use metrics::{counter, gauge, histogram};
use solana_sdk::{hash::Hash, signature::Signature};
use tokio::{
    sync::{mpsc, Mutex, Semaphore},
    task::JoinHandle,
};

use super::utils::{record_dashmap_len, record_dashset_len, record_staged_batches_metrics};

pub(super) struct TxProcessorConfig {
    pub receivers: Vec<mpsc::Receiver<Arc<CommitBatchNotification>>>,
    pub db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    pub indexer: Arc<Mutex<dyn Indexer>>,
    pub bank: Arc<RwLock<Bank>>,
    pub subscription_processor: Arc<SubscriptionProcessor>,
    pub num_transactions: Arc<AtomicU64>,
    pub seen_shreds: Arc<DashSet<(u64, u64)>>,
    pub staged_batches: Arc<DashMap<u64, BTreeMap<u64, SerializableBatch>>>,
    pub finalized_slots: Arc<DashSet<u64>>,
    pub finalized_timestamps: Arc<DashMap<u64, u64>>,
    pub finalized_job_ids: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
    pub refetch_pool: Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    pub blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    pub current_slot: Arc<AtomicU64>,
    pub pending_batches: Arc<DashMap<(u64, u64), SerializableBatch>>,
}

pub(super) fn spawn_tx_processors(config: TxProcessorConfig) -> Vec<JoinHandle<()>> {
    let TxProcessorConfig {
        receivers,
        db_chain,
        indexer,
        bank,
        subscription_processor,
        num_transactions,
        seen_shreds,
        staged_batches,
        finalized_slots,
        finalized_timestamps,
        finalized_job_ids,
        refetch_pool,
        blockhash_to_signatures,
        current_slot,
        pending_batches,
    } = config;

    let mut handles = Vec::new();

    info!("Starting {} transaction batch processing threads", receivers.len());

    for (i, mut tx_receiver) in receivers.into_iter().enumerate() {
        let db_chain_ref_clone = db_chain.clone();
        let indexer_clone = indexer.clone();
        let bank_clone = bank.clone();
        let subscription_processor_clone = subscription_processor.clone();
        let num_transactions_clone = num_transactions.clone();
        let seen_shreds_clone = seen_shreds.clone();
        let staged_batches_clone = staged_batches.clone();
        let finalized_slots_clone = finalized_slots.clone();
        let finalized_timestamps_clone = finalized_timestamps.clone();
        let finalized_job_ids_clone = finalized_job_ids.clone();
        let refetch_pool_clone = refetch_pool.clone();
        let refetch_semaphore = Arc::new(Semaphore::new(16));
        let blockhash_to_signatures_clone = blockhash_to_signatures.clone();
        let current_slot_tracker = current_slot.clone();
        let indexer_for_block = indexer_clone.clone();
        let pending_batches_clone = pending_batches.clone();
        let handle = tokio::spawn({
            let indexer = indexer_clone;
            async move {
                info!("Transaction batch processor {} started", i);
                // Two paths:
                // 1. active broadcast path
                //    - a. randomly selected
                //    - b. broadcast finalize
                //    - c. flush (empty batch)
                // 2. refetch path
                while let Some(tx_batch) = tx_receiver.recv().await {
                    let parsed = match process_commit_notification(tx_batch.as_ref()) {
                        Ok(parsed) => parsed,
                        Err(e) => {
                            error!("Processor {}: Error parsing tx_batch: {}", i, e);
                            continue;
                        }
                    };

                    let parsed = match parsed {
                        SerializableNotification::Finalization(marker) => {
                            info!(
                                "Processor {}: Received finalization marker for slot {}, job_ids={:?}",
                                i, marker.slot, marker.job_ids
                            );
                            let mut marker_job_ids = marker.job_ids.clone();
                            marker_job_ids.sort_unstable();
                            marker_job_ids.dedup();
                            {
                                let mut guard = finalized_job_ids_clone.write().unwrap();
                                guard.insert(marker.slot, marker_job_ids.clone());
                                gauge!("finalized_job_ids_len").set(guard.len() as f64);
                            }
                            if let Err(e) = finalize_staged_slot(
                                marker.slot,
                                marker.timestamp,
                                marker_job_ids,
                                &staged_batches_clone,
                                &db_chain_ref_clone,
                                &bank_clone,
                                &subscription_processor_clone,
                                &indexer,
                                &num_transactions_clone,
                                &seen_shreds_clone,
                                &finalized_slots_clone,
                                &finalized_timestamps_clone,
                                &refetch_pool_clone,
                                &refetch_semaphore,
                                &pending_batches_clone,
                                &blockhash_to_signatures_clone,
                                i,
                            )
                            .await
                            {
                                error!("Processor {}: Failed to finalize slot {}: {}", i, marker.slot, e);
                            }
                            // Flush blockhash_to_signatures and call bank.tick_as_slave()
                            let blockhash_to_signatures = {
                                let mut guard = blockhash_to_signatures_clone.write().unwrap();
                                std::mem::take(&mut *guard)
                            };

                            // Call bank.tick_as_slave() to update slot/hash/timestamp
                            {
                                let mut bank_writer = bank_clone.write().unwrap();
                                bank_writer.tick_as_slave(&infinisvm_core::bank::RawSlot {
                                    slot: marker.slot,
                                    hash: marker.hash,
                                    parent_hash: marker.parent_hash,
                                    timestamp: marker.timestamp,
                                    job_ids: vec![],
                                    is_finalized: false,
                                });
                                if !blockhash_to_signatures.is_empty() {
                                    bank_writer.commit_blockhash_to_signatures(blockhash_to_signatures);
                                }
                                // Update current slot tracker after bank is updated
                                current_slot_tracker.store(marker.slot, Ordering::SeqCst);
                            }

                            // Handle DB merging every 4 slots
                            if marker.slot > 0 && marker.slot % 4 == 0 {
                                info!("Processor {}: Attempting merge at slot {}", i, marker.slot);
                                counter!("cold_start_merge_attempts_total").increment(1);
                                let t_merge = Instant::now();
                                let merge_result = {
                                    let mut db_chain = db_chain_ref_clone.write().unwrap();
                                    info!("Acquired lock. Pre-merge: {}", db_chain.summary());
                                    let plan = finalized_job_ids_clone.read().unwrap().clone();
                                    let res = db_chain.merge(plan);
                                    info!("Merge finished; Post-merge: {}", db_chain.summary());
                                    res
                                };
                                let latest_slot = match merge_result {
                                    Ok(latest_slot) => latest_slot,
                                    Err(e) => {
                                        error!(
                                            "Processor {}: Error merging db_chain at slot {}: {}",
                                            i, marker.slot, e
                                        );
                                        counter!("cold_start_merge_errors_total").increment(1);
                                        None // Continue processing even if
                                             // merge fails
                                    }
                                };
                                histogram!("cold_start_merge_attempt_ms")
                                    .record(t_merge.elapsed().as_secs_f64() * 1000.0);
                                if let Some(latest_slot) = latest_slot {
                                    info!("Processor {}: Successfully merged db_chain to slot {}", i, latest_slot);
                                    counter!("cold_start_merge_success_total").increment(1);
                                    {
                                        let mut guard = finalized_job_ids_clone.write().unwrap();
                                        let before = guard.len();
                                        guard.retain(|slot_key, _| *slot_key > latest_slot);
                                        if before != guard.len() {
                                            counter!("finalized_job_ids_pruned_total")
                                                .increment((before - guard.len()) as u64);
                                        }
                                        gauge!("finalized_job_ids_len").set(guard.len() as f64);
                                    }
                                } else {
                                    info!(
                                        "Processor {}: Merge returned None at slot {} (no confirmed slot yet)",
                                        i, marker.slot
                                    );
                                }
                            }

                            // Index block
                            let t_index_block = Instant::now();
                            indexer_for_block.lock().await.index_block(
                                marker.slot,
                                marker.timestamp,
                                marker.hash,
                                marker.parent_hash,
                            );
                            histogram!("slot_index_block_ms").record(t_index_block.elapsed().as_secs_f64() * 1000.0);

                            continue;
                        }
                        SerializableNotification::Batch(batch) => batch,
                    };

                    // Check if slot is already finalized - add to pending pool instead of staging
                    // Check both finalized_slots (for recent slots) and current_slot (for pruned
                    // ancient slots)
                    let current_slot_value = current_slot_tracker.load(Ordering::SeqCst) - 1;
                    if parsed.slot < current_slot_value || finalized_slots_clone.contains(&parsed.slot) {
                        info!(
                            "Processor {}: Batch for already-finalized slot {} (job_id={}, current_slot={}) - adding to pending pool",
                            i, parsed.slot, parsed.job_id, current_slot_value
                        );
                        pending_batches_clone.insert((parsed.slot, parsed.job_id as u64), parsed);
                        continue;
                    }

                    // not finalized yet. stage
                    let job_id_u64 = parsed.job_id as u64;
                    let target_slot = parsed.slot;

                    // Stage the batch (transaction processing happens during finalization via
                    // write_to_indexer)
                    let slot_len = {
                        let mut entry = staged_batches_clone.entry(target_slot).or_default();
                        entry.insert(job_id_u64, parsed);
                        entry.len()
                    };
                    histogram!("staged_batches_slot_jobs").record(slot_len as f64);
                    record_staged_batches_metrics(staged_batches_clone.as_ref());
                }
                info!("Transaction batch processor {} terminated (channel closed)", i);
            }
        });
        handles.push(handle);
    }

    handles
}

fn write_to_indexer(
    processor_id: usize,
    parsed: &SerializableBatch,
    blockhash_to_signatures_clone: &Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
) -> eyre::Result<()> {
    if !parsed.transactions.is_empty() {
        let mut signatures = HashMap::new();
        for tx in parsed.transactions.iter() {
            let transaction = tx.get_transaction()?;
            let blockhash = *transaction.message.recent_blockhash();
            let signature = tx.get_signature();
            trace!("Processor {}: Processing transaction {}", processor_id, signature);
            signatures.entry(blockhash).or_insert_with(Vec::new).push(signature);
        }
        if !signatures.is_empty() {
            let mut guard = blockhash_to_signatures_clone.write().unwrap();
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

async fn finalize_staged_slot(
    slot: u64,
    slot_timestamp: u64,
    job_ids: Vec<u64>,
    staged_batches: &DashMap<u64, BTreeMap<u64, SerializableBatch>>,
    db_chain: &Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    bank: &Arc<RwLock<Bank>>,
    subscription_processor: &Arc<SubscriptionProcessor>,
    indexer: &Arc<Mutex<dyn Indexer>>,
    num_transactions: &Arc<AtomicU64>,
    seen_shreds: &Arc<DashSet<(u64, u64)>>,
    finalized_slots: &Arc<DashSet<u64>>,
    finalized_timestamps: &Arc<DashMap<u64, u64>>,
    refetch_pool: &Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    refetch_sem: &Arc<Semaphore>,
    pending_batches: &Arc<DashMap<(u64, u64), SerializableBatch>>,
    blockhash_to_signatures: &Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    processor_id: usize,
) -> eyre::Result<()> {
    if finalized_slots.insert(slot) {
        record_dashset_len(finalized_slots.as_ref(), "finalized_slots_len");
    } else {
        return Ok(());
    }
    finalized_timestamps.insert(slot, slot_timestamp);
    record_dashmap_len(finalized_timestamps.as_ref(), "finalized_timestamps_len");

    finalize_staged_slot_once(
        slot,
        slot_timestamp,
        job_ids,
        staged_batches,
        db_chain,
        bank,
        subscription_processor,
        indexer,
        num_transactions,
        seen_shreds,
        refetch_pool,
        refetch_sem,
        pending_batches,
        blockhash_to_signatures,
        processor_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_staged_slot_once(
    slot: u64,
    slot_timestamp: u64,
    job_ids: Vec<u64>,
    staged_batches: &DashMap<u64, BTreeMap<u64, SerializableBatch>>,
    db_chain: &Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    bank: &Arc<RwLock<Bank>>,
    subscription_processor: &Arc<SubscriptionProcessor>,
    indexer: &Arc<Mutex<dyn Indexer>>,
    num_transactions: &Arc<AtomicU64>,
    seen_shreds: &Arc<DashSet<(u64, u64)>>,
    refetch_pool: &Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    refetch_sem: &Arc<Semaphore>,
    pending_batches: &Arc<DashMap<(u64, u64), SerializableBatch>>,
    blockhash_to_signatures: &Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    processor_id: usize,
) -> eyre::Result<()> {
    let staged_entry = staged_batches.remove(&slot);
    record_staged_batches_metrics(staged_batches);
    let mut batches = staged_entry
        .map(|(_, b)| {
            histogram!("staged_batches_slot_jobs").record(b.len() as f64);
            b
        })
        .unwrap_or_default();

    let have: HashSet<u64> = batches.keys().copied().collect();
    let want: HashSet<u64> = job_ids.into_iter().collect();
    let missing_job_ids: Vec<u64> = want.difference(&have).copied().collect();

    if !missing_job_ids.is_empty() {
        info!(
            "Processor {}: Finalizing slot {}, missing {} job_ids, refetching...",
            processor_id,
            slot,
            missing_job_ids.len()
        );
        for job_id in missing_job_ids {
            // Check pending pool first before refetching
            if let Some((_, pending_batch)) = pending_batches.remove(&(slot, job_id)) {
                info!(
                    "Processor {}: Found batch for slot {} job_id {} in pending pool",
                    processor_id, slot, job_id
                );
                batches.insert(job_id, pending_batch);
                continue;
            }

            let permit = refetch_sem.acquire().await.expect("semaphore not closed");
            match get_and_decode_batch(refetch_pool, slot, job_id, pending_batches).await {
                Ok(maybe_batch) => {
                    batches.insert(job_id, maybe_batch);
                }
                Err(RefetchErr::NotFound) => {
                    batches.insert(
                        job_id,
                        SerializableBatch {
                            slot,
                            timestamp: slot_timestamp,
                            job_id: job_id as usize,
                            transactions: vec![],
                            worker_id: 0,
                        },
                    );
                }
                Err(e) => {
                    error!(
                        "Processor {}: Error refetching missing shard for slot {}. job {}: {:?}",
                        processor_id, slot, job_id, e
                    );
                }
            }
            drop(permit);
        }
    }

    if batches.is_empty() {
        info!(
            "Processor {}: No batches to apply for finalized slot {}",
            processor_id, slot
        );
        return Ok(());
    }

    info!(
        "Processor {}: Finalizing slot {} (timestamp={}) with {} buffered batches",
        processor_id,
        slot,
        slot_timestamp,
        batches.len()
    );

    const INDEXER_CHUNK: usize = 512;

    for (job_id, batch) in batches.into_iter() {
        if batch.transactions.is_empty() {
            if seen_shreds.insert((batch.slot, job_id)) {
                record_dashset_len(seen_shreds.as_ref(), "seen_shreds_len");
                if slot != batch.slot {
                    info!(
                        "Processor {}: relocating shred from slot {} to slot {}",
                        processor_id, batch.slot, slot
                    );
                }
                let meta = DBMeta::from_shred(slot, job_id);
                let mut chain = db_chain.write().unwrap();
                let before = chain.len();
                let t_add = Instant::now();
                debug!(
                    "Processor {}: applying empty shard {:?}; chain size {} -> {}?",
                    processor_id,
                    meta,
                    before,
                    before + 1
                );
                chain.add_db(Arc::new(RwLock::new(MemoryDB::new_no_underlying())), meta);
                histogram!("db_chain_add_shred_ms", "source" => "tx_stream_empty")
                    .record(t_add.elapsed().as_secs_f64() * 1000.0);
                counter!("db_chain_shreds_added_total", "source" => "tx_stream_empty").increment(1);
                debug!("Processor {}: post-add summary: {}", processor_id, chain.summary());
            } else {
                info!(
                    "Processor {}: empty shard already applied for slot={} job_id={}, skipping",
                    processor_id, batch.slot, job_id
                );
            }
            continue;
        }

        write_to_indexer(processor_id, &batch, blockhash_to_signatures)?;

        histogram!("tx_batch_transactions_count").record(batch.transactions.len() as f64);
        num_transactions.fetch_add(batch.transactions.len() as u64, Ordering::SeqCst);

        let mut shred_db = MemoryDB::new_no_underlying();
        let t_build = Instant::now();
        for tx in batch.transactions.iter() {
            let result = tx.get_result().expect("serialized batch result");

            let status = match result.status {
                Ok(()) => TransactionStatus::Executed(None, slot),
                Err(e) => TransactionStatus::Executed(Some(e), slot),
            };
            trace!(
                "Processor {}: Processing finalized transaction {}",
                processor_id,
                tx.get_signature()
            );
            subscription_processor.notify_signature_update(&tx.get_signature(), &status);
            bank.write().unwrap().write_status_cache(&tx.get_signature(), status);

            let pre_accounts = tx.get_pre_accounts().expect("serialized batch pre-accounts");

            for ((pubkey, account), diffs) in pre_accounts.into_iter().zip(result.diffs.into_iter()) {
                let mut account = account.unwrap_or_default();
                for diff in diffs {
                    diff.apply_to_account(&mut account);
                }
                shred_db.write_account(pubkey, account);
            }
        }
        histogram!("tx_batch_build_shard_ms").record(t_build.elapsed().as_secs_f64() * 1000.0);

        let meta = DBMeta::from_shred(slot, job_id);
        if seen_shreds.insert((batch.slot, job_id)) {
            record_dashset_len(seen_shreds.as_ref(), "seen_shreds_len");
            let mut chain = db_chain.write().unwrap();
            let before = chain.len();
            let t_add = Instant::now();
            info!(
                "Processor {}: adding shard {:?}; chain size {} -> {}?",
                processor_id,
                meta,
                before,
                before + 1
            );
            chain.add_db(Arc::new(RwLock::new(shred_db)), meta);
            histogram!("db_chain_add_shred_ms", "source" => "tx_batch").record(t_add.elapsed().as_secs_f64() * 1000.0);
            counter!("db_chain_shreds_added_total", "source" => "tx_batch").increment(1);
            debug!("Processor {}: post-add summary: {}", processor_id, chain.summary());
        }

        let t_index = Instant::now();
        for chunk in batch.transactions.chunks(INDEXER_CHUNK) {
            let mut guard = indexer.lock().await;
            for tx in chunk.iter() {
                let _ = guard.index_serializable_tx(tx.clone()).await;
            }
            drop(guard);
        }
        histogram!("tx_batch_index_ms").record(t_index.elapsed().as_secs_f64() * 1000.0);
    }

    info!(
        "Processor {}: Completed finalization for slot {} (timestamp={})",
        processor_id, slot, slot_timestamp
    );

    Ok(())
}

pub(super) async fn get_and_decode_batch(
    pool: &Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    slot: u64,
    job_id: u64,
    pending_batches: &Arc<DashMap<(u64, u64), SerializableBatch>>,
) -> std::result::Result<SerializableBatch, RefetchErr> {
    // Check pending pool first before making network requests
    if let Some((_, pending_batch)) = pending_batches.remove(&(slot, job_id)) {
        return Ok(pending_batch);
    }

    let mut saw_not_found = false;
    let mut notification_opt = None;
    for client_mutex in pool.iter() {
        let mut client = client_mutex.lock().await;
        for s in [slot, slot.saturating_sub(1), slot.saturating_add(1)] {
            match client.get_transaction_batch_status(s, job_id).await {
                Ok(resp) => {
                    notification_opt = Some(resp);
                    break;
                }
                Err(status) => {
                    if status.code() == tonic::Code::NotFound {
                        saw_not_found = true;
                    }
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

    let batch_data = match notification {
        CommitBatchNotification::Batch(data) => data,
        CommitBatchNotification::Finalization(_) => {
            return Err(RefetchErr::Other("received finalization instead of batch".into()));
        }
    };

    if batch_data.batch_size == 0 || batch_data.compressed_transactions.is_empty() {
        return Ok(SerializableBatch {
            // Important: use the server-reported slot (may differ from the probed one if we searched slot±1).
            slot: batch_data.slot,
            timestamp: batch_data.timestamp,
            job_id: job_id as usize,
            transactions: vec![],
            worker_id: batch_data.worker_id,
        });
    }

    let compressed = batch_data.compressed_transactions;
    let decompressed =
        zstd::decode_all(&compressed[..]).map_err(|e| RefetchErr::Other(format!("decompress error: {e}")))?;
    bincode::deserialize(&decompressed).map_err(|e| RefetchErr::Other(format!("deserialize error: {e}")))
}
