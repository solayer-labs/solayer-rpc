use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use dashmap::{DashMap, DashSet};
use hashbrown::HashMap;
use infinisvm_core::{
    bank::{Bank, RawSlot},
    indexer::Indexer,
};
use infinisvm_db::{db_chain::DBChain, in_memory_db::NoopDB, MemoryDB};
use infinisvm_logger::{error, info};
use infinisvm_sync::types::SerializableBatch;
use metrics::{counter, gauge, histogram};
use solana_sdk::{hash::Hash, signature::Signature};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};

use super::utils::record_staged_batches_metrics;

pub(super) struct SlotProcessorConfig {
    pub receivers: Vec<mpsc::Receiver<RawSlot>>,
    pub db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    pub bank: Arc<RwLock<Bank>>,
    pub indexer: Arc<Mutex<dyn Indexer>>,
    pub num_slots: Arc<AtomicU64>,
    pub current_slot: Arc<AtomicU64>,
    pub finalized_job_ids: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
    pub blockhash_to_signatures: Arc<RwLock<HashMap<Hash, Vec<Signature>>>>,
    pub pending_signature_count: Arc<AtomicU64>,
}

pub(super) fn spawn_slot_processors(config: SlotProcessorConfig) -> Vec<JoinHandle<()>> {
    let SlotProcessorConfig {
        receivers,
        db_chain,
        bank,
        indexer,
        num_slots,
        current_slot,
        finalized_job_ids,
        blockhash_to_signatures,
        pending_signature_count,
    } = config;

    info!("Starting {} slot processing threads", receivers.len());

    let mut handles = Vec::new();

    for (i, mut slot_receiver) in receivers.into_iter().enumerate() {
        let db_chain_ref_clone = db_chain.clone();
        let bank_clone = bank.clone();
        let indexer_clone = indexer.clone();
        let num_slots_counter = num_slots.clone();
        let current_slot_tracker = current_slot.clone();
        let finalized_job_ids_clone = finalized_job_ids.clone();
        let blockhash_to_signatures_clone = blockhash_to_signatures.clone();
        let pending_signature_count_clone = pending_signature_count.clone();
        let handle = tokio::spawn(async move {
            info!("Slot processor {} started", i);
            while let Some(slot) = slot_receiver.recv().await {
                counter!("slots_received_total").increment(1);
                let (slot, blockhash, parent_blockhash, timestamp, job_ids) =
                    (slot.slot, slot.hash, slot.parent_hash, slot.timestamp, slot.job_ids);
                info!("Processor {}: Received slot {} (Hash: {:?})", i, slot, blockhash);
                current_slot_tracker.store(slot, Ordering::SeqCst);
                num_slots_counter.fetch_add(1, Ordering::SeqCst);
                histogram!("slot_job_ids_count").record(job_ids.len() as f64);

                let (blockhash_to_signatures, flush_blockhashes, flush_signatures) = {
                    let mut guard = blockhash_to_signatures_clone.write().unwrap();
                    let taken = std::mem::take(&mut *guard);
                    let flush_blockhashes = taken.len();
                    let flush_signatures = taken.values().map(Vec::len).sum::<usize>();
                    (taken, flush_blockhashes, flush_signatures)
                };
                if flush_blockhashes > 0 {
                    histogram!("blockhash_signature_flush_blockhashes").record(flush_blockhashes as f64);
                    histogram!("blockhash_signature_flush_signatures").record(flush_signatures as f64);
                }
                let remaining_signatures = if flush_signatures == 0 {
                    pending_signature_count_clone.load(Ordering::SeqCst)
                } else {
                    let prev = pending_signature_count_clone.fetch_sub(flush_signatures as u64, Ordering::SeqCst);
                    if flush_signatures as u64 > prev {
                        pending_signature_count_clone.store(0, Ordering::SeqCst);
                        0
                    } else {
                        prev - flush_signatures as u64
                    }
                };
                gauge!("blockhash_signature_pending_blockhashes").set(0.0);
                gauge!("blockhash_signature_pending_signatures").set(remaining_signatures as f64);

                {
                    let mut bank_writer = bank_clone.write().unwrap();
                    bank_writer.tick_as_slave(&RawSlot {
                        slot,
                        hash: blockhash,
                        parent_hash: parent_blockhash,
                        timestamp,
                        job_ids: vec![],
                        is_finalized: false,
                    });
                    if !blockhash_to_signatures.is_empty() {
                        bank_writer.commit_blockhash_to_signatures(blockhash_to_signatures);
                    }
                }

                if slot > 0 && slot % 4 == 0 {
                    info!("Processor {}: Attempting merge at slot {}", i, slot);
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
                            error!("Processor {}: Error merging db_chain at slot {}: {}", i, slot, e);
                            counter!("cold_start_merge_errors_total").increment(1);
                            continue;
                        }
                    };
                    histogram!("cold_start_merge_attempt_ms").record(t_merge.elapsed().as_secs_f64() * 1000.0);
                    if let Some(latest_slot) = latest_slot {
                        info!("Processor {}: Successfully merged db_chain to slot {}", i, latest_slot);
                        counter!("cold_start_merge_success_total").increment(1);
                        {
                            let mut guard = finalized_job_ids_clone.write().unwrap();
                            let before = guard.len();
                            guard.retain(|slot_key, _| *slot_key > latest_slot);
                            if before != guard.len() {
                                counter!("finalized_job_ids_pruned_total").increment((before - guard.len()) as u64);
                            }
                            gauge!("finalized_job_ids_len").set(guard.len() as f64);
                        }
                    } else {
                        info!(
                            "Processor {}: Merge returned None at slot {} (no confirmed slot yet)",
                            i, slot
                        );
                    }
                }

                let t_index_block = Instant::now();
                indexer_clone
                    .lock()
                    .await
                    .index_block(slot, timestamp, blockhash, parent_blockhash);
                histogram!("slot_index_block_ms").record(t_index_block.elapsed().as_secs_f64() * 1000.0);
            }
            info!("Slot processor {} terminated (channel closed)", i);
        });

        handles.push(handle);
    }

    handles
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_prune_task(
    seen_shreds: Arc<DashSet<(u64, u64)>>,
    finalized_slots: Arc<DashSet<u64>>,
    finalized_timestamps: Arc<DashMap<u64, u64>>,
    staged_batches: Arc<DashMap<u64, BTreeMap<u64, SerializableBatch>>>,
    finalized_job_ids: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
    current_slot: Arc<AtomicU64>,
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
        let finalized_window = std::env::var("FINALIZED_SLOTS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1000);
        let staged_ttl = std::env::var("STAGED_BATCHES_TTL_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(50);
        let finalized_ids_window = std::env::var("FINALIZED_JOB_IDS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1000);

        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(prune_interval_secs));
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

            let mut removed_seen = 0usize;
            seen_shreds.retain(|entry| {
                let keep = entry.0 >= seen_cutoff;
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
        }
    })
}
