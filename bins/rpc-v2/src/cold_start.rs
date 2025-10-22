use std::{
    collections::{HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use dashmap::{DashMap, DashSet};
use eyre::Result;
use hashbrown::HashMap;
use infinisvm_core::{
    bank::{Bank, RawSlot, TransactionStatus, FEE_ACCOUNTS},
    committer::PerfSample,
    indexer::Indexer,
    subscription::SubscriptionProcessor,
};
use infinisvm_db::{
    db_chain::{DBChain, DBMeta},
    in_memory_db::NoopDB,
    Database, MemoryDB,
};
use infinisvm_logger::{debug, error, info, trace, warn};
use infinisvm_sync::{
    grpc::{batch_subscriber::process_commit_notification, client::SyncClient},
    http_client::{parse_data, reduce_data, Downloader, HttpClient},
};
use infinisvm_types::sync::grpc::{CommitBatchNotification, SlotDataResponse};
use metrics::{counter, gauge, histogram};
use solana_sdk::{
    account::{AccountSharedData, WritableAccount},
    hash::Hash,
};
use tokio::{
    sync::{mpsc, Mutex, Semaphore},
    task::JoinHandle,
};

pub struct ColdStartResult {
    pub db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    pub slot_plan: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
    pub local_slot_plan: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
}

// 1. Download all checkpoints
// 2. Parse all checkpoints
// 3. Reduce all checkpoints to form bank of SLOT_h
// 4. Start listening for tx_batch and build a diff from SLOT_g
// 5. Wait for new checkpoints until SLOT_h' > SLOT_g
// 6. Merge
pub async fn cold_start(
    http_client: Arc<HttpClient>,
    tx_receivers: Vec<mpsc::Receiver<Arc<CommitBatchNotification>>>,
    slot_receivers: Vec<mpsc::Receiver<SlotDataResponse>>,
    indexer: Arc<Mutex<dyn Indexer>>,
    bank: Arc<RwLock<Bank>>,
    subscription_processor: Arc<SubscriptionProcessor>,
    total_transaction_count: Arc<AtomicU64>,
    samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>,
    refetch_pool: Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
) -> Result<(Vec<JoinHandle<Result<(), eyre::Report>>>, ColdStartResult)> {
    info!("Starting cold start process");
    counter!("cold_start_attempts_total").increment(1);
    let cs_start = Instant::now();

    let t_get_snapshots = Instant::now();
    let snapshots = http_client.get_snapshots().await?;
    histogram!("cold_start_get_snapshots_ms").record(t_get_snapshots.elapsed().as_secs_f64() * 1000.0);
    let ckpts_to_download = snapshots.get_ckpts_to_download().expect("No checkpoints to download");
    info!("Retrieved snapshots to download");
    debug!("Checkpoints to download: {:?}", ckpts_to_download);

    let mut downloader = Downloader::default();

    let mut all_ckpts = vec![ckpts_to_download.ckpt];
    all_ckpts.extend(ckpts_to_download.accounts);
    info!("Starting bulk download of {} checkpoints", all_ckpts.len());

    let t_bulk_download = Instant::now();
    let data = downloader
        .bulk_download(&http_client, all_ckpts, Arc::new(parse_data))
        .await
        .expect("Bulk download failed");
    histogram!("cold_start_bulk_download_ms").record(t_bulk_download.elapsed().as_secs_f64() * 1000.0);
    gauge!("cold_start_last_slot").set(downloader.last_slot() as f64);
    info!("Completed bulk download of checkpoints");

    let t_reduce = Instant::now();
    let reduced = reduce_data(data)?;
    histogram!("cold_start_reduce_data_ms").record(t_reduce.elapsed().as_secs_f64() * 1000.0);
    debug!("Reduced data size: {} entries", reduced.len());

    info!("Finished applying snapshots");
    let db = MemoryDB::from_hashmap(reduced);
    let mut db_chain = DBChain::default();
    let last_slot = downloader.last_slot();
    info!("Initializing DB chain with last slot {}", last_slot);
    db_chain.add_db(Arc::new(RwLock::new(db)), DBMeta::from_ckpt(last_slot));
    gauge!("cold_start_initial_chain_len").set(db_chain.len() as f64);
    debug!("DBChain initialized: {}", db_chain.summary());

    let db_chain_ref = Arc::new(RwLock::new(db_chain));
    let mut handles = Vec::new();

    info!("Starting file polling thread");
    let (tx, mut rx) = mpsc::channel(10240);
    let http_client_poll = http_client.clone();
    let handle = tokio::spawn(async move {
        info!("File poller task started");
        let res = downloader
            .poll_for_new_files(&http_client_poll, tx, Arc::new(parse_data))
            .await;
        match &res {
            Ok(_) => info!("File poller task finished cleanly (unexpected)"),
            Err(e) => error!("File poller task exited with error: {}", e),
        }
        res
    });
    handles.push(handle);

    info!("Starting DB chain update thread");
    let db_chain_ref_clone = db_chain_ref.clone();
    let handle = tokio::spawn(async move {
        info!("DB chain update task started");
        while let Some((slot, data)) = rx.recv().await {
            // Record file receipt and basic sizes
            let file_kind = match &slot {
                infinisvm_db::persistence::DBFile::Checkpoint(_) => "checkpoint",
                infinisvm_db::persistence::DBFile::Account(_) => "account",
                infinisvm_db::persistence::DBFile::Shred(_, _) => "shred",
            };
            counter!("file_poller_received_total", "type" => file_kind).increment(1);
            histogram!("file_poller_records_len", "type" => file_kind).record(data.len() as f64);

            let new_db = MemoryDB::from_hashmap(HashMap::from_iter(data.into_iter()));
            let meta = DBMeta::from_db_file(slot);
            let mut chain = db_chain_ref_clone.write().unwrap();
            let before = chain.len();
            let t_add = Instant::now();
            info!(
                "File poller: adding {:?}; chain size {} -> {}?",
                meta,
                before,
                before + 1
            );
            chain.add_db(Arc::new(RwLock::new(new_db)), meta);
            histogram!("file_poller_add_db_ms").record(t_add.elapsed().as_secs_f64() * 1000.0);
            debug!("File poller: post-add summary: {}", chain.summary());
        }
        info!("DB chain update task terminated (channel closed)");
        Ok(()) as Result<(), eyre::Report>
    });
    handles.push(handle);

    info!("Starting {} transaction batch processing threads", tx_receivers.len());

    let num_transactions = Arc::new(AtomicU64::new(0));
    let num_slots = Arc::new(AtomicU64::new(0));
    // Track the latest observed slot from the slot stream for accurate sampling
    let current_slot = Arc::new(AtomicU64::new(last_slot));
    // Remote slot plan as advertised by the leader (updated by both tx and slot
    // processors)
    let slot_plan = Arc::new(RwLock::new(HashMap::<u64, Vec<u64>>::new()));
    // Local slot plan built from tx_receivers after indexing
    let local_slot_plan = Arc::new(RwLock::new(HashMap::<u64, Vec<u64>>::new()));
    // Global in-flight refetch tracker and applied-shreds tracker to reduce
    // duplication
    let inflight_refetch: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    let seen_shreds: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    let job_slot_overrides: Arc<DashMap<u64, u64>> = Arc::new(DashMap::new());
    for (i, mut tx_receiver) in tx_receivers.into_iter().enumerate() {
        let db_chain_ref_clone = db_chain_ref.clone();
        let indexer_clone = indexer.clone();
        let bank_clone = bank.clone();
        let subscription_processor_clone = subscription_processor.clone();
        let num_transactions_clone = num_transactions.clone();
        let local_slot_plan_clone = local_slot_plan.clone();
        let slot_plan_clone = slot_plan.clone();
        let inflight_refetch_clone = inflight_refetch.clone();
        let seen_shreds_clone = seen_shreds.clone();
        let job_slot_overrides_clone = job_slot_overrides.clone();
        let handle = tokio::spawn({
            let indexer = indexer_clone;
            async move {
                info!("Transaction batch processor {} started", i);
                while let Some(tx_batch) = tx_receiver.recv().await {
                    counter!("tx_batches_received_total").increment(1);
                    histogram!("tx_batch_jobs_count").record(tx_batch.batch_size as f64);
                    info!(
                        "Processor {}: Received tx-batch slot={} job_id={} size={} comp_ratio={}",
                        i, tx_batch.slot, tx_batch.job_id, tx_batch.batch_size, tx_batch.compression_ratio
                    );

                    let info_slot = tx_batch.slot;
                    let info_job_id = tx_batch.job_id;
                    let info_batch_size = tx_batch.batch_size;

                    let parsed = match process_commit_notification(tx_batch.as_ref()) {
                        Ok(parsed) => parsed,
                        Err(e) => {
                            error!("Processor {}: Error parsing tx_batch: {}", i, e);
                            continue;
                        }
                    };
                    let job_id_u64 = parsed.job_id as u64;
                    let mut target_slot = parsed.slot;
                    if let Some(override_slot) = job_slot_overrides_clone.get(&job_id_u64) {
                        let override_slot_value = *override_slot.value();
                        if override_slot_value != target_slot {
                            info!(
                                "Processor {}: slot override for job {} from {} to {} based on remote plan",
                                i, job_id_u64, target_slot, override_slot_value
                            );
                        }
                        target_slot = override_slot_value;
                    }

                    let existing_slot_hint = {
                        let chain = db_chain_ref_clone.read().unwrap();
                        chain.find_shred_slot(job_id_u64)
                    };
                    if let Some(existing_slot) = existing_slot_hint {
                        if existing_slot != target_slot {
                            info!(
                                "Processor {}: adjusting target slot for job {} from {} to {} based on current chain placement",
                                i, job_id_u64, target_slot, existing_slot
                            );
                            target_slot = existing_slot;
                        }
                    }

                    // Empty payloads carry (slot, job_id). Mark presence and add empty shard if
                    // new; cancel refetch.
                    if parsed.transactions.is_empty() {
                        counter!("tx_batch_empty_total").increment(1);
                        info!(
                            "Processor {}: Empty batch after parse for slot={} job_id={} (orig_size={}) — marking presence",
                            i,
                            info_slot,
                            info_job_id,
                            info_batch_size
                        );
                        {
                            let mut lsp = local_slot_plan_clone.write().unwrap();
                            if parsed.slot != target_slot {
                                if let Some(old_vec) = lsp.get_mut(&parsed.slot) {
                                    old_vec.retain(|jid| *jid != job_id_u64);
                                }
                            }
                            let entry = lsp.entry(target_slot).or_insert_with(Vec::new);
                            if !entry.contains(&job_id_u64) {
                                entry.push(job_id_u64);
                                entry.sort_unstable();
                            }
                            gauge!("local_slot_plan_len").set(lsp.len() as f64);
                        }
                        inflight_refetch_clone.remove(&(parsed.slot, job_id_u64));
                        inflight_refetch_clone.remove(&(target_slot, job_id_u64));
                        if parsed.slot != target_slot {
                            seen_shreds_clone.remove(&(parsed.slot, job_id_u64));
                        }
                        seen_shreds_clone.remove(&(target_slot, job_id_u64));
                        if seen_shreds_clone.insert((target_slot, job_id_u64)) {
                            let meta = DBMeta::from_shred(target_slot, job_id_u64);
                            let mut chain = db_chain_ref_clone.write().unwrap();
                            let before = chain.len();
                            let t_add = Instant::now();
                            debug!(
                                "Processor {}: adding empty shred from stream {:?}; chain size {} -> {}?",
                                i,
                                meta,
                                before,
                                before + 1
                            );
                            chain.add_db(Arc::new(RwLock::new(MemoryDB::new_no_underlying())), meta);
                            histogram!("db_chain_add_shred_ms", "source" => "tx_stream_empty")
                                .record(t_add.elapsed().as_secs_f64() * 1000.0);
                            counter!("db_chain_shreds_added_total", "source" => "tx_stream_empty").increment(1);
                            debug!("Processor {}: post-add summary: {}", i, chain.summary());
                        } else {
                            info!(
                                "Processor {}: empty shard already applied for slot={} job_id={}, skipping",
                                i, target_slot, job_id_u64
                            );
                        }
                        job_slot_overrides_clone.remove(&job_id_u64);
                        {
                            let mut sp = slot_plan_clone.write().unwrap();
                            reconcile_slot_plan_entry(&mut sp, job_id_u64, target_slot, "processor_empty");
                        }
                        continue;
                    }
                    // Non-empty: record presence in local plan immediately
                    {
                        let mut lsp = local_slot_plan_clone.write().unwrap();
                        if parsed.slot != target_slot {
                            if let Some(old_vec) = lsp.get_mut(&parsed.slot) {
                                old_vec.retain(|jid| *jid != job_id_u64);
                            }
                        }
                        let entry = lsp.entry(target_slot).or_insert_with(Vec::new);
                        if !entry.contains(&job_id_u64) {
                            entry.push(job_id_u64);
                            entry.sort_unstable();
                        }
                        gauge!("local_slot_plan_len").set(lsp.len() as f64);
                    }
                    // Cancel in-flight refetch for this shard if present
                    inflight_refetch_clone.remove(&(parsed.slot, job_id_u64));
                    inflight_refetch_clone.remove(&(target_slot, job_id_u64));
                    if parsed.slot != target_slot {
                        seen_shreds_clone.remove(&(parsed.slot, job_id_u64));
                    }
                    seen_shreds_clone.remove(&(target_slot, job_id_u64));
                    let mut shred_db = MemoryDB::new_no_underlying();
                    histogram!("tx_batch_transactions_count").record(parsed.transactions.len() as f64);

                    num_transactions_clone.fetch_add(parsed.transactions.len() as u64, Ordering::SeqCst);

                    let t_build = Instant::now();
                    for tx in parsed.transactions.iter() {
                        let result = tx.get_result().unwrap();

                        let status = match result.status {
                            Ok(()) => TransactionStatus::Executed(None, target_slot),
                            Err(e) => TransactionStatus::Executed(Some(e), target_slot),
                        };
                        trace!("Processor {}: Processing transaction {}", i, tx.get_signature());
                        subscription_processor_clone.notify_signature_update(&tx.get_signature(), &status);
                        bank_clone
                            .write()
                            .unwrap()
                            .write_status_cache(&tx.get_signature(), status);

                        let pre_accounts = tx.get_pre_accounts().unwrap();

                        for ((pubkey, account), diffs) in pre_accounts.into_iter().zip(result.diffs.into_iter()) {
                            let mut account = account.unwrap_or_default();
                            for diff in diffs {
                                diff.apply_to_account(&mut account);
                            }
                            shred_db.write_account(pubkey, account);
                        }
                    }
                    histogram!("tx_batch_build_shard_ms").record(t_build.elapsed().as_secs_f64() * 1000.0);

                    let meta = DBMeta::from_shred(target_slot, job_id_u64);
                    if seen_shreds_clone.insert((target_slot, job_id_u64)) {
                        {
                            let mut chain = db_chain_ref_clone.write().unwrap();
                            let before = chain.len();
                            let t_add = Instant::now();
                            info!(
                                "Processor {}: adding shred {:?}; chain size {} -> {}?",
                                i,
                                meta,
                                before,
                                before + 1
                            );
                            chain.add_db(Arc::new(RwLock::new(shred_db)), meta);
                            histogram!("db_chain_add_shred_ms", "source" => "tx_batch")
                                .record(t_add.elapsed().as_secs_f64() * 1000.0);
                            counter!("db_chain_shreds_added_total", "source" => "tx_batch").increment(1);
                            debug!("Processor {}: post-add summary: {}", i, chain.summary());
                        }
                    } else {
                        info!(
                            "Processor {}: shard already applied for slot={} job_id={}, skipping add",
                            i, target_slot, job_id_u64
                        );
                    }

                    job_slot_overrides_clone.remove(&job_id_u64);

                    {
                        let mut sp = slot_plan_clone.write().unwrap();
                        reconcile_slot_plan_entry(&mut sp, job_id_u64, target_slot, "processor");
                    }

                    // Process transactions in chunks to reduce mutex thrash
                    const INDEXER_CHUNK: usize = 512;
                    let t_index = Instant::now();
                    for chunk in parsed.transactions.chunks(INDEXER_CHUNK) {
                        let mut guard = indexer.lock().await;
                        for tx in chunk.iter() {
                            let _ = guard.index_serializable_tx(tx.clone()).await;
                        }
                        drop(guard);
                    }
                    histogram!("tx_batch_index_ms").record(t_index.elapsed().as_secs_f64() * 1000.0);

                    // Status cache was already written per-transaction above
                    // with the correct success or error
                    // status. Do not overwrite here.
                }
                info!("Transaction batch processor {} terminated (channel closed)", i);
                Ok(()) as Result<(), eyre::Report>
            }
        });
        handles.push(handle);
    }

    info!("Starting {} slot processing threads", slot_receivers.len());
    // Cap concurrent refetch across all slot processors
    let max_concurrent_refetch = std::env::var("MAX_CONCURRENT_REFETCH")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8);
    let refetch_semaphore = Arc::new(Semaphore::new(max_concurrent_refetch));
    for (i, mut slot_receiver) in slot_receivers.into_iter().enumerate() {
        let db_chain_ref_clone = db_chain_ref.clone();
        let slot_plan_clone = slot_plan.clone();
        let local_slot_plan_clone = local_slot_plan.clone();
        let bank_clone = bank.clone();
        let indexer_clone = indexer.clone();
        let inflight_clone = inflight_refetch.clone();
        let seen_shreds_clone2 = seen_shreds.clone();
        let refetch_sem_clone = refetch_semaphore.clone();
        let refetch_pool_clone = refetch_pool.clone();
        let subscription_processor_clone2 = subscription_processor.clone();
        let num_slots_counter = num_slots.clone();
        let current_slot_tracker = current_slot.clone();
        let job_slot_overrides_clone = job_slot_overrides.clone();
        let handle = tokio::spawn(async move {
            info!("Slot processor {} started", i);
            while let Some(slot) = slot_receiver.recv().await {
                counter!("slots_received_total").increment(1);
                info!(
                    "Processor {}: Received slot {} (Hash: {:?})",
                    i,
                    slot.slot,
                    Hash::new(slot.blockhash.as_ref())
                );
                let (slot, blockhash, parent_blockhash, timestamp, job_ids) = (
                    slot.slot,
                    slot.blockhash,
                    slot.parent_blockhash,
                    slot.timestamp,
                    slot.job_ids,
                );
                // Update counters for sampling
                current_slot_tracker.store(slot, Ordering::SeqCst);
                num_slots_counter.fetch_add(1, Ordering::SeqCst);
                trace!(
                    "Processor {}: Processing slot {} with {} job IDs",
                    i,
                    slot,
                    job_ids.len()
                );
                histogram!("slot_job_ids_count").record(job_ids.len() as f64);
                bank_clone.write().unwrap().tick_as_slave(&RawSlot {
                    slot,
                    hash: Hash::new(blockhash.as_ref()),
                    parent_hash: Hash::new(parent_blockhash.as_ref()),
                    timestamp,
                    job_ids: vec![], // we don't need job_ids for posting tick
                });
                let remote_ids: Vec<u64> = {
                    // Deduplicate remote job_ids to avoid over-counting in DBChain merge
                    let mut ids = job_ids;
                    ids.sort_unstable();
                    ids.dedup();
                    let ids_clone = ids.clone();
                    let mut sp = slot_plan_clone.write().unwrap();
                    let ids_len = ids.len();
                    let sample: Vec<u64> = ids.iter().copied().take(6).collect();
                    // Diagnostics: observe whether we overwrite a larger set with a smaller one
                    // (regression)
                    let prev_len = sp.get(&slot).map(|v| v.len()).unwrap_or(0);
                    let union_len = match sp.get(&slot) {
                        Some(prev) => {
                            use std::collections::HashSet;
                            let set: HashSet<u64> = prev.iter().copied().chain(ids.iter().copied()).collect();
                            set.len()
                        }
                        None => ids_len,
                    };
                    sp.insert(slot, ids);
                    info!(
                        "Processor {}: slot_plan updated for slot {} (ids_len={}, prev_len={}, union_len={}, sample={:?}); total entries {}",
                        i,
                        slot,
                        ids_len,
                        prev_len,
                        union_len,
                        sample,
                        sp.len()
                    );
                    if ids_len < prev_len || ids_len < union_len {
                        warn!(
                            "Processor {}: slot_plan update regression at slot {} (new_len={} < prev_len={} or union_len={})",
                            i,
                            slot,
                            ids_len,
                            prev_len,
                            union_len
                        );
                    }
                    gauge!("slot_plan_len").set(sp.len() as f64);
                    ids_clone
                };

                {
                    let mut lsp = local_slot_plan_clone.write().unwrap();
                    lsp.entry(slot).or_insert_with(Vec::new);
                }

                let mut relocated: Vec<(u64, u64)> = Vec::new();
                {
                    let mut chain = db_chain_ref_clone.write().unwrap();
                    for job_id in &remote_ids {
                        if chain.contains_shred(slot, *job_id) {
                            job_slot_overrides_clone.remove(job_id);
                            continue;
                        }
                        if let Some(existing_slot) = chain.find_shred_slot(*job_id) {
                            if chain.relocate_shred(existing_slot, *job_id, slot) {
                                relocated.push((existing_slot, *job_id));
                                job_slot_overrides_clone.remove(job_id);
                            }
                        } else {
                            job_slot_overrides_clone.insert(*job_id, slot);
                        }
                    }
                }

                if !relocated.is_empty() {
                    info!(
                        "Processor {}: relocated {} shreds into slot {}: {:?}",
                        i,
                        relocated.len(),
                        slot,
                        relocated
                    );
                    {
                        let mut lsp = local_slot_plan_clone.write().unwrap();
                        for (old_slot, job_id) in &relocated {
                            if let Some(old_vec) = lsp.get_mut(old_slot) {
                                old_vec.retain(|jid| *jid != *job_id);
                            }
                            let entry = lsp.entry(slot).or_insert_with(Vec::new);
                            if !entry.contains(job_id) {
                                entry.push(*job_id);
                                entry.sort_unstable();
                            }
                        }
                    }
                    {
                        let mut sp = slot_plan_clone.write().unwrap();
                        let mut slots_to_drop: Vec<u64> = Vec::new();
                        for (old_slot, job_id) in &relocated {
                            if let Some(old_vec) = sp.get_mut(old_slot) {
                                let before_len = old_vec.len();
                                old_vec.retain(|jid| *jid != *job_id);
                                if before_len != old_vec.len() {
                                    debug!(
                                        "Processor {}: pruned job {} from remote plan slot {} after relocation",
                                        i, job_id, old_slot
                                    );
                                }
                                if old_vec.is_empty() {
                                    slots_to_drop.push(*old_slot);
                                }
                            }
                            let entry = sp.entry(slot).or_insert_with(Vec::new);
                            if !entry.contains(job_id) {
                                entry.push(*job_id);
                                entry.sort_unstable();
                            }
                        }
                        for drop_slot in slots_to_drop {
                            sp.remove(&drop_slot);
                        }
                        debug!("Processor {}: slot_plan adjusted for relocation at slot {}", i, slot);
                    }
                    for (old_slot, job_id) in &relocated {
                        seen_shreds_clone2.remove(&(*old_slot, *job_id));
                        seen_shreds_clone2.insert((slot, *job_id));
                    }
                }
                // Compare and refetch (sequentially) using this task's dedicated client
                let remote_opt = slot_plan_clone.read().unwrap().get(&slot).cloned();
                let local_opt = local_slot_plan_clone.read().unwrap().get(&slot).cloned();
                if let (Some(remote_vec), Some(local_vec)) = (remote_opt, local_opt) {
                    let remote_set: HashSet<u64> = remote_vec.iter().copied().collect();
                    let local_set: HashSet<u64> = local_vec.iter().copied().collect();
                    let mut missing: Vec<u64> = remote_set.difference(&local_set).copied().collect();
                    if !missing.is_empty() {
                        // Debounce to allow in-flight tx-batches to arrive
                        let debounce_ms = std::env::var("SLOT_MISMATCH_DEBOUNCE_MS")
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(75);
                        if debounce_ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(debounce_ms)).await;
                            if let Some(local_after_vec) = local_slot_plan_clone.read().unwrap().get(&slot).cloned() {
                                let local_after_set: HashSet<u64> = local_after_vec.iter().copied().collect();
                                missing = remote_set.difference(&local_after_set).copied().collect();
                            }
                        }
                        if missing.is_empty() {
                            continue;
                        }
                        missing.sort_unstable();
                        log_slot_plan_mismatch(slot, &local_vec, &remote_vec);
                        gauge!("grpc_refetch_missing_jobs").set(missing.len() as f64);

                        // For missing jobs, schedule refetch; if definitively NotFound after retries,
                        // treat as empty
                        for job_id in missing {
                            // Skip if a refetch for (slot, job_id) is already in-flight
                            if !inflight_clone.insert((slot, job_id)) {
                                info!("Refetch already in flight for slot={}, job_id={}", slot, job_id);
                                continue;
                            }
                            counter!("grpc_refetch_scheduled_total").increment(1);
                            gauge!("grpc_refetch_inflight").set(inflight_clone.len() as f64);
                            gauge!("grpc_refetch_concurrency_available")
                                .set(refetch_sem_clone.available_permits() as f64);
                            info!("Scheduling refetch for slot={}, job_id={} on a new task", slot, job_id);
                            let pool = refetch_pool_clone.clone();
                            let indexer_task = indexer_clone.clone();
                            let bank_task = bank_clone.clone();
                            let subscription_task = subscription_processor_clone2.clone();
                            let lsp_task = local_slot_plan_clone.clone();
                            let db_chain_task = db_chain_ref_clone.clone();
                            let slot_plan_task = slot_plan_clone.clone();
                            let inflight_task = inflight_clone.clone();
                            let seen_shreds_task = seen_shreds_clone2.clone();
                            let overrides_task = job_slot_overrides_clone.clone();
                            let sem = refetch_sem_clone.clone();
                            tokio::spawn(async move {
                                info!("Refetch task spawned for slot={}, job_id={}", slot, job_id);
                                // Retry loop with backoff on NotFound (batch not yet cached on server)
                                let mut attempt: u32 = 0;
                                let max_retries: u32 = std::env::var("MAX_REFETCH_RETRIES")
                                    .ok()
                                    .and_then(|s| s.parse::<u32>().ok())
                                    .unwrap_or(5);
                                let mut backoff_ms: u64 = 200;
                                loop {
                                    // Acquire permit to cap concurrent refetches; release before any backoff sleep
                                    let permit = sem.acquire().await.expect("semaphore not closed");
                                    counter!("grpc_refetch_attempts_total").increment(1);
                                    let attempt_result = refetch_and_apply_shred_from_pool(
                                        &pool,
                                        indexer_task.clone(),
                                        bank_task.clone(),
                                        subscription_task.clone(),
                                        lsp_task.clone(),
                                        slot_plan_task.clone(),
                                        db_chain_task.clone(),
                                        seen_shreds_task.clone(),
                                        overrides_task.clone(),
                                        slot,
                                        job_id,
                                    )
                                    .await;
                                    // Always release permit before any potential sleep or loop iteration
                                    drop(permit);
                                    match attempt_result {
                                        Ok(()) => {
                                            info!("Successfully refetched for slot={}, job_id={}", slot, job_id);
                                            break;
                                        }
                                        Err(RefetchErr::NotFound) if attempt < max_retries => {
                                            info!(
                                                "Slot {} job {} not yet cached (attempt {}/{}). Retrying in {}ms",
                                                slot,
                                                job_id,
                                                attempt + 1,
                                                max_retries,
                                                backoff_ms
                                            );
                                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                                            backoff_ms = (backoff_ms.saturating_mul(2)).min(5000);
                                            attempt += 1;
                                            continue;
                                        }
                                        Err(RefetchErr::Other(msg)) => {
                                            error!("Refetch failed for slot {}, job {}: {}", slot, job_id, msg);
                                            counter!("grpc_refetch_failure_total").increment(1);
                                            break;
                                        }
                                        Err(RefetchErr::NotFound) => {
                                            // Definitive NotFound after retries: treat as empty and add placeholder
                                            // shard
                                            info!(
                                                "Refetch NotFound exhausted for slot {}, job {}: adding empty shard",
                                                slot, job_id
                                            );
                                            // Update local plan
                                            {
                                                let mut lsp = lsp_task.write().unwrap();
                                                let entry = lsp.entry(slot).or_insert_with(Vec::new);
                                                if !entry.contains(&job_id) {
                                                    entry.push(job_id);
                                                }
                                            }
                                            // Add empty shard if not already applied elsewhere
                                            if seen_shreds_task.insert((slot, job_id)) {
                                                {
                                                    let meta = DBMeta::from_shred(slot, job_id);
                                                    let mut chain = db_chain_task.write().unwrap();
                                                    let t_add = Instant::now();
                                                    chain.add_db(
                                                        Arc::new(RwLock::new(MemoryDB::new_no_underlying())),
                                                        meta,
                                                    );
                                                    histogram!("db_chain_add_shred_ms", "source" => "refetch_empty")
                                                        .record(t_add.elapsed().as_secs_f64() * 1000.0);
                                                    counter!("db_chain_shreds_added_total", "source" => "refetch_empty")
                                                        .increment(1);
                                                    info!("Refetch(empty): post-add summary: {}", chain.summary());
                                                }
                                            } else {
                                                info!(
                                                    "Refetch(empty): shard already applied for slot={} job_id={}, skipping",
                                                    slot, job_id
                                                );
                                            }
                                            // Count as success for purposes of progressing merges
                                            counter!("grpc_refetch_success_total").increment(1);
                                            break;
                                        }
                                    }
                                }
                                // Remove from in-flight set regardless of success/failure
                                inflight_task.remove(&(slot, job_id));
                                gauge!("grpc_refetch_inflight").set(inflight_task.len() as f64);
                                info!("Refetch task ended for slot={}, job_id={}", slot, job_id);
                            });
                        }
                    }
                }
                if slot > 0 && slot % 4 == 0 {
                    info!("Processor {}: Attempting merge at slot {}", i, slot);
                    info!("Attempting to acquire lock for merge at slot {}", slot);
                    counter!("cold_start_merge_attempts_total").increment(1);
                    let t_merge = Instant::now();
                    let merge_result = {
                        let mut db_chain = db_chain_ref_clone.write().unwrap();
                        info!("Acquired lock. Pre-merge: {}", db_chain.summary());
                        let res = db_chain.merge(slot_plan_clone.read().unwrap().clone());
                        info!("Merge finished at slot {}; Post-merge: {}", slot, db_chain.summary());
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
                        let mut slot_plan = slot_plan_clone.write().unwrap();
                        let removed_count = slot_plan.len();
                        slot_plan.retain(|&slot, _| slot > latest_slot);
                        let pruned = removed_count - slot_plan.len();
                        counter!("slot_plan_entries_pruned_total").increment(pruned as u64);
                        gauge!("slot_plan_len").set(slot_plan.len() as f64);
                        info!(
                            "Processor {}: Removed {} old slots from plan",
                            i,
                            removed_count - slot_plan.len()
                        );
                    } else {
                        info!(
                            "Processor {}: Merge returned None at slot {} (no confirmed slot yet)",
                            i, slot
                        );
                    }
                }
                let t_index_block = Instant::now();
                indexer_clone.lock().await.index_block(
                    slot,
                    timestamp,
                    Hash::new(blockhash.as_ref()),
                    Hash::new(parent_blockhash.as_ref()),
                );
                histogram!("slot_index_block_ms").record(t_index_block.elapsed().as_secs_f64() * 1000.0);
            }
            info!("Slot processor {} terminated (channel closed)", i);
            Ok(()) as Result<(), eyre::Report>
        });

        handles.push(handle);
    }

    // Create a sampling thread to track performance metrics
    let samples_clone = samples.clone();
    let total_transaction_count_clone = total_transaction_count.clone();
    let num_transactions_clone = num_transactions.clone();
    let num_slots_clone = num_slots.clone();
    let current_slot_clone = current_slot.clone();
    let handle = tokio::spawn(async move {
        info!("Sampler task started");
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            {
                let mut samples = samples_clone.write().unwrap();
                let duration = samples.0.elapsed().as_secs();
                samples.0 = Instant::now();
                let num_slots_val = num_slots_clone.load(Ordering::SeqCst);
                let num_transactions_val = num_transactions_clone.load(Ordering::SeqCst);
                let cur_slot = current_slot_clone.load(Ordering::SeqCst);
                samples
                    .1
                    .push_back((cur_slot, num_transactions_val, num_slots_val, duration));
                num_slots_clone.store(0, Ordering::SeqCst);
                num_transactions_clone.store(0, Ordering::SeqCst);

                if samples.1.len() > 720 {
                    samples.1.pop_front();
                }

                total_transaction_count_clone.fetch_add(num_transactions_val, Ordering::SeqCst);

                // Publish rolling throughput gauges
                let secs = duration.max(1) as f64;
                let tps = (num_transactions_val as f64) / secs;
                let sps = (num_slots_val as f64) / secs;
                gauge!("window_tps").set(tps);
                gauge!("window_sps").set(sps);
            }
        }
    });
    handles.push(handle);

    histogram!("cold_start_total_ms").record(cs_start.elapsed().as_secs_f64() * 1000.0);
    counter!("cold_start_completed_total").increment(1);
    info!("Cold start completed successfully");
    Ok((
        handles,
        ColdStartResult {
            db_chain: db_chain_ref,
            slot_plan,
            local_slot_plan,
        },
    ))
}

fn log_slot_plan_mismatch(slot: u64, local_vec: &[u64], remote_vec: &[u64]) {
    use std::collections::{HashMap as StdHashMap, HashSet};

    let lset: HashSet<u64> = local_vec.iter().copied().collect();
    let rset: HashSet<u64> = remote_vec.iter().copied().collect();

    let missing_in_local: Vec<u64> = rset.difference(&lset).copied().collect();
    let extra_in_local: Vec<u64> = lset.difference(&rset).copied().collect();

    let mut lcounts: StdHashMap<u64, usize> = StdHashMap::new();
    for id in local_vec.iter().copied() {
        *lcounts.entry(id).or_insert(0) += 1;
    }
    let mut rcounts: StdHashMap<u64, usize> = StdHashMap::new();
    for id in remote_vec.iter().copied() {
        *rcounts.entry(id).or_insert(0) += 1;
    }
    let ldups: StdHashMap<u64, usize> = lcounts
        .into_iter()
        .filter_map(|(k, v)| if v > 1 { Some((k, v)) } else { None })
        .collect();
    let rdups: StdHashMap<u64, usize> = rcounts
        .into_iter()
        .filter_map(|(k, v)| if v > 1 { Some((k, v)) } else { None })
        .collect();

    error!(
        "Slot plan mismatch at slot {}:\n  remote_count={} local_count={}\n  remote_unique={} local_unique={}\n  missing_in_local={:?}\n  extra_in_local={:?}\n  local_duplicates={:?}\n  remote_duplicates={:?}\n  remote_ids={:?}\n  local_ids={:?}",
        slot,
        remote_vec.len(),
        local_vec.len(),
        rset.len(),
        lset.len(),
        missing_in_local,
        extra_in_local,
        ldups,
        rdups,
        remote_vec,
        local_vec
    );
}

fn reconcile_slot_plan_entry(slot_plan: &mut HashMap<u64, Vec<u64>>, job_id: u64, target_slot: u64, reason: &str) {
    let mut removed_from: Vec<u64> = Vec::new();
    let mut slots_to_drop: Vec<u64> = Vec::new();

    for (slot, ids) in slot_plan.iter_mut() {
        if *slot == target_slot {
            continue;
        }
        let before = ids.len();
        ids.retain(|jid| *jid != job_id);
        if ids.len() != before {
            removed_from.push(*slot);
        }
        if ids.is_empty() {
            slots_to_drop.push(*slot);
        }
    }

    for slot in slots_to_drop {
        slot_plan.remove(&slot);
    }

    let entry = slot_plan.entry(target_slot).or_default();
    if !entry.contains(&job_id) {
        entry.push(job_id);
        entry.sort_unstable();
    }

    if !removed_from.is_empty() {
        debug!(
            "Slot plan reconcile ({}) moved job {} to slot {} from {:?}",
            reason, job_id, target_slot, removed_from
        );
    }
}

enum RefetchErr {
    NotFound,
    Other(String),
}

async fn refetch_and_apply_shred_from_pool(
    pool: &Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
    indexer: Arc<Mutex<dyn Indexer>>,
    bank: Arc<RwLock<Bank>>,
    subscription_processor: Arc<SubscriptionProcessor>,
    local_slot_plan: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
    slot_plan: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
    db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    seen_shreds: Arc<DashSet<(u64, u64)>>,
    job_slot_overrides: Arc<DashMap<u64, u64>>,
    slot: u64,
    job_id: u64,
) -> Result<(), RefetchErr> {
    // If another path already applied this shard, skip early
    let mut target_slot = slot;
    if let Some(override_slot) = job_slot_overrides.get(&job_id) {
        let override_slot_value = *override_slot.value();
        if override_slot_value != target_slot {
            info!(
                "Refetch: applying override for job {} from slot {} to slot {}",
                job_id, target_slot, override_slot_value
            );
        }
        target_slot = override_slot_value;
    }

    let existing_slot_hint = {
        let chain = db_chain.read().unwrap();
        chain.find_shred_slot(job_id)
    };

    if let Some(existing_slot) = existing_slot_hint {
        if existing_slot != target_slot {
            info!(
                "Refetch: adjusting target slot for job {} from {} to {} based on current chain placement",
                job_id, target_slot, existing_slot
            );
            target_slot = existing_slot;
        }
    }

    if seen_shreds.contains(&(target_slot, job_id)) {
        job_slot_overrides.remove(&job_id);
        return Ok(());
    }
    if target_slot != slot && seen_shreds.contains(&(slot, job_id)) {
        seen_shreds.remove(&(slot, job_id));
    }
    // Try all servers until one returns the batch
    let mut saw_not_found = false;
    let mut notification_opt = None;
    for client_mutex in pool.iter() {
        let mut client = client_mutex.lock().await;
        // Try current slot, then slot-1, then slot+1 as a tolerance for slot skew
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

    // Handle empty batches (no payload) explicitly; mark presence and add empty
    // shred
    if notification.batch_size == 0 || notification.compressed_transactions.is_empty() {
        let existing_slot = {
            let chain = db_chain.read().unwrap();
            chain.find_shred_slot(job_id)
        };
        {
            let mut lsp = local_slot_plan.write().unwrap();
            if let Some(old_slot) = existing_slot {
                if old_slot != target_slot {
                    if let Some(old_vec) = lsp.get_mut(&old_slot) {
                        old_vec.retain(|jid| *jid != job_id);
                    }
                }
            }
            let entry = lsp.entry(target_slot).or_default();
            if !entry.contains(&job_id) {
                entry.push(job_id);
                entry.sort_unstable();
            }
        }
        if let Some(old_slot) = existing_slot {
            if old_slot != target_slot {
                seen_shreds.remove(&(old_slot, job_id));
            }
        }
        job_slot_overrides.remove(&job_id);
        let mut chain = db_chain.write().unwrap();
        let mut applied = false;
        if let Some(old_slot) = existing_slot {
            if old_slot != target_slot {
                applied = chain.relocate_shred(old_slot, job_id, target_slot);
            } else if chain.contains_shred(target_slot, job_id) {
                applied = true;
            }
        }
        seen_shreds.remove(&(target_slot, job_id));
        if !applied {
            if seen_shreds.insert((target_slot, job_id)) {
                let meta = DBMeta::from_shred(target_slot, job_id);
                let before = chain.len();
                debug!(
                    "Refetch(empty): adding shred {:?}; chain size {} -> {}?",
                    meta,
                    before,
                    before + 1
                );
                let t_add = Instant::now();
                chain.add_db(Arc::new(RwLock::new(MemoryDB::new_no_underlying())), meta);
                histogram!("db_chain_add_shred_ms", "source" => "refetch")
                    .record(t_add.elapsed().as_secs_f64() * 1000.0);
                counter!("db_chain_shreds_added_total", "source" => "refetch").increment(1);
                debug!("Refetch(empty): post-add summary: {}", chain.summary());
            } else {
                info!(
                    "Refetch(empty): shard already applied for slot={} job_id={}, skipping",
                    target_slot, job_id
                );
            }
        } else {
            seen_shreds.insert((target_slot, job_id));
        }
        {
            let mut sp = slot_plan.write().unwrap();
            reconcile_slot_plan_entry(&mut sp, job_id, target_slot, "refetch_empty");
        }
        counter!("grpc_refetch_success_total").increment(1);
        return Ok(());
    }

    // Decode and deserialize in-place; this runs in a separate task already
    let compressed = notification.compressed_transactions;
    let t_decompress = Instant::now();
    let decompressed =
        zstd::decode_all(&compressed[..]).map_err(|e| RefetchErr::Other(format!("decompress error: {e}")))?;
    histogram!("grpc_refetch_decompress_ms").record(t_decompress.elapsed().as_secs_f64() * 1000.0);
    let t_deser = Instant::now();
    let batch: infinisvm_sync::types::SerializableBatch =
        bincode::deserialize(&decompressed).map_err(|e| RefetchErr::Other(format!("deserialize error: {e}")))?;
    histogram!("grpc_refetch_deserialize_ms").record(t_deser.elapsed().as_secs_f64() * 1000.0);
    let txs = batch.transactions;
    histogram!("grpc_refetch_txs_count").record(txs.len() as f64);

    {
        let mut idx = indexer.lock().await;
        for tx in txs.iter() {
            let _ = idx.index_serializable_tx(tx.clone()).await;
        }
    }
    // Status will be written with accurate Ok/Err per-transaction during shard
    // build below.

    // Build MemoryDB shard mirroring the transaction-batch path
    let mut shred_db = MemoryDB::new_no_underlying();
    let t_build = Instant::now();
    let mut fees = 0;
    for tx in txs.iter() {
        let result = tx
            .get_result()
            .map_err(|e| RefetchErr::Other(format!("get_result error: {e}")))?;
        fees += result.fee;

        // Write the correct status (success or error) to the follower's status cache
        // and notify subscribers
        let status = match result.status {
            Ok(()) => TransactionStatus::Executed(None, target_slot),
            Err(e) => TransactionStatus::Executed(Some(e), target_slot),
        };
        subscription_processor.notify_signature_update(&tx.get_signature(), &status);
        bank.write().unwrap().write_status_cache(&tx.get_signature(), status);
        let pre_accounts = tx
            .get_pre_accounts()
            .map_err(|e| RefetchErr::Other(format!("get_pre_accounts error: {e}")))?;
        for ((pubkey, account_opt), diffs) in pre_accounts.into_iter().zip(result.diffs.into_iter()) {
            let mut account = account_opt.unwrap_or_default();
            for diff in diffs {
                diff.apply_to_account(&mut account);
            }
            shred_db.write_account(pubkey, account);
        }
    }
    let fee_account = FEE_ACCOUNTS[batch.worker_id];
    let mut fee_account_data = match shred_db.get_account(fee_account) {
        Ok(Some(account)) => account,
        _ => AccountSharedData::default(),
    };
    fee_account_data.checked_add_lamports(fees).unwrap();
    shred_db.write_account(fee_account, fee_account_data);
    histogram!("grpc_refetch_build_shard_ms").record(t_build.elapsed().as_secs_f64() * 1000.0);

    // Update local plan to reflect presence of this job
    let existing_slot = {
        let chain = db_chain.read().unwrap();
        chain.find_shred_slot(job_id)
    };

    {
        let mut lsp = local_slot_plan.write().unwrap();
        if let Some(old_slot) = existing_slot {
            if old_slot != target_slot {
                if let Some(old_vec) = lsp.get_mut(&old_slot) {
                    old_vec.retain(|jid| *jid != job_id);
                }
            }
        }
        let entry = lsp.entry(target_slot).or_default();
        if !entry.contains(&job_id) {
            entry.push(job_id);
            entry.sort_unstable();
        }
    }

    if let Some(old_slot) = existing_slot {
        if old_slot != target_slot {
            seen_shreds.remove(&(old_slot, job_id));
        }
    }

    job_slot_overrides.remove(&job_id);

    let mut chain = db_chain.write().unwrap();
    let mut applied = false;
    if let Some(old_slot) = existing_slot {
        if old_slot != target_slot {
            applied = chain.relocate_shred(old_slot, job_id, target_slot);
        } else if chain.contains_shred(target_slot, job_id) {
            applied = true;
        }
    }

    seen_shreds.remove(&(target_slot, job_id));
    if applied {
        seen_shreds.insert((target_slot, job_id));
        debug!(
            "Refetch: shard for job {} already present after relocation; skipping re-add",
            job_id
        );
    } else if seen_shreds.insert((target_slot, job_id)) {
        let meta = DBMeta::from_shred(target_slot, job_id);
        let before = chain.len();
        debug!(
            "Refetch: adding shred {:?}; chain size {} -> {}?",
            meta,
            before,
            before + 1
        );
        let t_add = Instant::now();
        chain.add_db(Arc::new(RwLock::new(shred_db)), meta);
        histogram!("db_chain_add_shred_ms", "source" => "refetch").record(t_add.elapsed().as_secs_f64() * 1000.0);
        counter!("db_chain_shreds_added_total", "source" => "refetch").increment(1);
        debug!("Refetch: post-add summary: {}", chain.summary());
    } else {
        info!(
            "Refetch: shard already applied for slot={} job_id={}, skipping",
            target_slot, job_id
        );
    }

    {
        let mut sp = slot_plan.write().unwrap();
        reconcile_slot_plan_entry(&mut sp, job_id, target_slot, "refetch");
    }

    info!(
        "Refetch applied and shard added for slot {}, job {}",
        target_slot, job_id
    );
    counter!("grpc_refetch_success_total").increment(1);

    Ok(())
}
