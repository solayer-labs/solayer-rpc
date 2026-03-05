use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant, SystemTime},
};

use crossbeam_channel::{Receiver, Sender};
use in_flight_tracker::InFlightTracker;
use infinisvm_logger::{debug, info};
use infinisvm_types::sync::{ShredId, SyncFinalization};
use rand::Rng;
use read_write_account_set::ReadWriteAccountSet;
use solana_sdk::{hash::Hash, transaction::SanitizedTransaction};
use solana_svm::transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig};
use thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId, ThreadSet};
use transaction_state_container::{TransactionState, TransactionStateContainer};

use crate::{
    bank::Bank,
    committer::CommitEvent,
    metrics::SchedulerMetrics,
    scheduler::core_batch_shred::CoreBatchShred,
    worker::{ScheduledJob, Worker},
};

pub(crate) mod consumed_job;
pub(crate) mod core_batch_shred;
mod in_flight_tracker;
mod read_write_account_set;
mod thread_aware_account_locks;
pub(crate) mod transaction_batch_id;
pub(crate) mod transaction_id;
mod transaction_priority_id;
mod transaction_state_container;

use consumed_job::ConsumedJob;
use transaction_batch_id::TransactionBatchId;
use transaction_id::TransactionId;
use transaction_priority_id::TransactionPriorityId;

pub struct Scheduler {
    tx_container: TransactionStateContainer,
    tx_receiver: Receiver<(SanitizedTransaction, u64)>, // tx, priority
    bench_tx_receiver: Receiver<(SanitizedTransaction, u64)>, // tx, priority
    tx_id_generator: TransactionIdGenerator,
    bank: Arc<RwLock<Bank>>,

    batch_size: usize,
    next_shred_index: HashMap<u64, PendingFinalization>, // slot -> metadata for finalization
    worker_progress: HashMap<usize, u64>,                // worker_id -> last reported slot completion
    last_finalized_slot: u64,
    last_observed_bank_slot: u64,

    account_locks: ThreadAwareAccountLocks,
    in_flight_tracker: InFlightTracker,
    commit_sender: Sender<CommitEvent>,
    consume_work_senders: Vec<Sender<ScheduledJob>>,
    callback_receiver: Receiver<(Vec<ConsumedJob>, TransactionBatchId)>,
    working_account_set: ReadWriteAccountSet,
    unschedulables: Vec<TransactionPriorityId>,
    metrics: SchedulerMetrics,
}

#[derive(Debug)]
struct PendingFinalization {
    num_shreds: u64,
    hash: Hash,
    parent_hash: Hash,
    block_unix_timestamp: u64,
}

impl PendingFinalization {
    pub fn new(num_shreds: u64, hash: Hash, parent_hash: Hash, block_unix_timestamp: u64) -> Self {
        Self {
            num_shreds,
            hash,
            parent_hash,
            block_unix_timestamp,
        }
    }
}

impl Scheduler {
    pub fn new(
        tx_receiver: Receiver<(SanitizedTransaction, u64)>, // tx, priority
        bench_tx_receiver: Receiver<(SanitizedTransaction, u64)>, // tx, priority
        bank: Arc<RwLock<Bank>>,
        batch_size: usize,
        commit_sender: Sender<CommitEvent>,
    ) -> Self {
        let mut consume_work_senders = Vec::with_capacity(crate::SCHEDULER_WORKER_COUNT);
        let (callback_sender, callback_receiver) = crossbeam_channel::unbounded();

        for i in 0..crate::SCHEDULER_WORKER_COUNT {
            let callback_sender = callback_sender.clone();
            let (sender, receiver) = crossbeam_channel::unbounded();
            let bank_clone = bank.clone();
            std::thread::Builder::new()
                .name(format!("scheduler-worker-{i}"))
                .spawn(move || {
                    let worker = Worker::new(
                        // todo: refer to agave
                        // may 20: make sure configs are the same as agave
                        TransactionProcessingConfig {
                            account_overrides: None,
                            check_program_modification_slot: false,
                            compute_budget: None,
                            log_messages_bytes_limit: None,
                            limit_to_load_programs: true,
                            recording_config: ExecutionRecordingConfig::new_single_setting(true),
                            transaction_account_lock_limit: None,
                        },
                        receiver,
                        i,
                    );
                    worker.run_loop(bank_clone, callback_sender);
                })
                .unwrap();
            consume_work_senders.push(sender);
        }

        let initial_slot = bank.read().unwrap().get_latest_slot_hash_timestamp().0;

        Self {
            tx_container: TransactionStateContainer::with_capacity(1000000),
            tx_receiver,
            bench_tx_receiver,
            tx_id_generator: TransactionIdGenerator::default(),
            bank,
            batch_size,
            commit_sender,
            consume_work_senders,
            callback_receiver,
            account_locks: ThreadAwareAccountLocks::new(crate::SCHEDULER_WORKER_COUNT),
            in_flight_tracker: InFlightTracker::new(crate::SCHEDULER_WORKER_COUNT),
            working_account_set: ReadWriteAccountSet::default(),
            unschedulables: Vec::new(),
            metrics: SchedulerMetrics::default(),
            worker_progress: HashMap::from_iter(
                (0..crate::SCHEDULER_WORKER_COUNT).map(|worker_id| (worker_id, initial_slot)),
            ),
            last_finalized_slot: initial_slot.saturating_sub(1),
            next_shred_index: HashMap::new(),
            last_observed_bank_slot: initial_slot,
        }
    }

    pub fn run_loop(&mut self, scheduler_exit: Arc<AtomicBool>, global_exit: Arc<AtomicBool>) {
        // pin the cpu
        if let Some(cores) = core_affinity::get_core_ids() {
            // bind to core 2
            core_affinity::set_for_current(cores[2]);
        }

        let mut last_shrink_time = Instant::now();

        let bank = self.bank.clone();
        let recent_blockhashes = bank.read().unwrap().blockhash_ref();
        let mut batches = Vec::with_capacity(crate::SCHEDULER_WORKER_COUNT);
        let mut tx_ids = Vec::with_capacity(crate::SCHEDULER_WORKER_COUNT);
        for _ in 0..crate::SCHEDULER_WORKER_COUNT {
            batches.push(Vec::with_capacity(self.batch_size));
            tx_ids.push(Vec::with_capacity(self.batch_size));
        }

        while !scheduler_exit.load(Ordering::Relaxed) {
            let start = Instant::now();

            // shrink the account locks map every 60s
            if start - last_shrink_time > Duration::from_secs(60) {
                self.account_locks.shrink_to_fit();
                self.tx_container.shrink_to_fit();
                last_shrink_time = start;
            }

            // receive for 25ms. each recv max 5ms
            let num_txs_received = self.receive_txs(
                // very few txs. receive longer
                if self.tx_container.len() < 10000 {
                    Duration::from_millis(50)
                } else {
                    Duration::from_millis(25)
                },
                self.batch_size,
                &recent_blockhashes,
            );
            let buffer_len = self.tx_container.len();

            let receive_time = start.elapsed();

            // release locks and send to indexer
            let num_completed = self.receive_completed();
            let receive_completed_time = start.elapsed() - receive_time;
            // do schedule
            let (num_scanned, num_scheduled, num_unschedulable) =
                self.schedule_txs(&recent_blockhashes, &mut batches, &mut tx_ids);
            let schedule_time = start.elapsed() - receive_time - receive_completed_time;

            let finalize_start = Instant::now();

            let (slot, hash, timestamp, parent_hash) = bank.read().unwrap().get_latest_slot_hash_timestamp_parent();

            if slot < self.last_observed_bank_slot {
                panic!("Bank slot went backwards: {} -> {}", self.last_observed_bank_slot, slot);
            }
            self.last_observed_bank_slot = slot;
            self.refresh_idle_worker_progress(slot);

            if !self.is_flushed(slot) {
                if let Entry::Vacant(entry) = self.next_shred_index.entry(slot) {
                    entry.insert(PendingFinalization::new(0, hash, parent_hash, timestamp));
                }
            }

            if !self.in_flight_tracker.has_in_flight() && self.commit_sender.is_empty() {
                let mut unfinalized_slots: Vec<u64> = self
                    .next_shred_index
                    .keys()
                    .copied()
                    .filter(|s| *s > self.last_finalized_slot)
                    .collect();

                if !unfinalized_slots.is_empty() {
                    unfinalized_slots.sort_unstable();

                    let global_min_reported = self.worker_progress.values().copied().min().unwrap_or(slot);
                    let worker_safe_upper = global_min_reported.saturating_sub(1);
                    let bank_safe_upper = slot.saturating_sub(2);
                    let flush_upper = worker_safe_upper.max(bank_safe_upper);

                    if flush_upper > self.last_finalized_slot {
                        let mut highest_flushed_slot = self.last_finalized_slot;

                        for pending_slot in unfinalized_slots {
                            if pending_slot > flush_upper {
                                break;
                            }

                            let finalization = self.take_slot_finalization(pending_slot).unwrap_or_else(|| {
                                panic!(
                                    "attempted to flush slot {pending_slot} but it was missing from next_shred_index"
                                )
                            });

                            self.commit_sender
                                .send(CommitEvent::Flush(SyncFinalization {
                                    slot: pending_slot,
                                    num_shreds: finalization.num_shreds,
                                    hash: finalization.hash,
                                    parent_hash: finalization.parent_hash,
                                    block_unix_timestamp: finalization.block_unix_timestamp,
                                    shred_hashes: Vec::new(),
                                }))
                                .expect("commit channel must be alive while scheduler is running");

                            highest_flushed_slot = highest_flushed_slot.max(pending_slot);
                        }

                        self.last_finalized_slot = highest_flushed_slot;
                    }
                }
            }

            let finalize_time = finalize_start.elapsed();
            let total_time = start.elapsed();

            let pending_fin_count = self.next_shred_index.len();
            let has_in_flight = self.in_flight_tracker.has_in_flight();
            let commit_q_len = self.commit_sender.len();
            let callback_q_len = self.callback_receiver.len();
            let worker_qs: Vec<usize> = self.consume_work_senders.iter().map(|s| s.len()).collect();

            info!(
                "timings: recv={:?} completed={:?} sched={:?} finalize={:?} total={:?} \
                 pipeline: recv={} completed={} sched={}/{} unsched={} \
                 state: buf={} in_flight={} commit_q={} cb_q={} worker_qs={:?} \
                 slots: bank={} finalized={} pending_fin={}",
                receive_time,
                receive_completed_time,
                schedule_time,
                finalize_time,
                total_time,
                num_txs_received,
                num_completed,
                num_scheduled,
                num_scanned,
                num_unschedulable,
                buffer_len,
                has_in_flight,
                commit_q_len,
                callback_q_len,
                worker_qs,
                slot,
                self.last_finalized_slot,
                pending_fin_count,
            );

            self.metrics.report();
        }

        println!("scheduler exit");
        // make sure all executed transactions are flushed
        while self.receive_completed() > 0 {}
        println!("receive completed");

        global_exit.store(true, Ordering::SeqCst);
    }

    fn receive_txs(
        &mut self,
        total_duration: Duration,
        batch_size: usize,
        recent_blockhashes: &Arc<RwLock<VecDeque<Hash>>>,
    ) -> usize {
        let now = Instant::now();
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if self.tx_container.len() > 500_000 {
            return 0;
        }

        // If the receiver queue is more than 50x the container size, drain excess
        // transactions.
        let max_receiver_len = self.tx_container.len() * 50;
        if self.tx_receiver.len() > max_receiver_len && self.tx_receiver.len() > 500_000 {
            while self.tx_receiver.len() > max_receiver_len {
                if self.tx_receiver.try_recv().is_err() {
                    break;
                }
            }
        }

        if self.bench_tx_receiver.len() > max_receiver_len && self.bench_tx_receiver.len() > 500_000 {
            while self.bench_tx_receiver.len() > max_receiver_len {
                if self.bench_tx_receiver.try_recv().is_err() {
                    break;
                }
            }
        }

        let mut num_normal_txs = 0;
        let mut num_bench_txs = 0;

        while now.elapsed() < total_duration {
            let (tx, priority, is_bench_tx) = match self.tx_receiver.try_recv() {
                Ok((tx, priority)) => {
                    if !recent_blockhashes
                        .read()
                        .unwrap()
                        .contains(tx.message().recent_blockhash())
                    {
                        self.metrics.increase_total_tx_early_expired_receiving(1);
                        continue;
                    }
                    (tx, priority, false)
                }

                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // if we have enough txs in the container
                    // or there are some txs in the callback channel, break
                    if self.callback_receiver.len() > 100 || self.tx_container.len() > batch_size * 4 {
                        break;
                    }

                    // if normal txs are not enough, receive bench txs
                    match self.bench_tx_receiver.try_recv() {
                        Ok((tx, priority)) => {
                            if !recent_blockhashes
                                .read()
                                .unwrap()
                                .contains(tx.message().recent_blockhash())
                            {
                                self.metrics.increase_total_bench_tx_early_expired_receiving(1);
                                continue;
                            }
                            (tx, priority, true)
                        }
                        Err(_) => continue,
                    }
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    unreachable!("tx receiver disconnected");
                }
            };
            let tx_id = self.tx_id_generator.next();
            self.tx_container.insert_new_transaction(tx_id, tx, priority, timestamp);
            if is_bench_tx {
                num_bench_txs += 1;
            } else {
                num_normal_txs += 1;
            }
        }

        self.metrics.increase_total_tx_received(num_normal_txs as u64);
        self.metrics.increase_total_bench_tx_received(num_bench_txs as u64);
        self.metrics.set_current_container_size(self.tx_container.len() as u64);
        self.metrics.set_current_channel_len(self.tx_receiver.len() as u64);

        debug!("receive: ({} normal txs, {} bench txs)", num_normal_txs, num_bench_txs);

        num_normal_txs + num_bench_txs
    }

    fn schedule_txs(
        &mut self,
        recent_blockhashes: &Arc<RwLock<VecDeque<Hash>>>,
        batches: &mut [Vec<SanitizedTransaction>], // avoid dealloc
        tx_ids: &mut [Vec<TransactionId>],         // avoid dealloc
    ) -> (usize, usize, usize) {
        // (num_scanned, num_scheduled, num_unschedulable)

        let num_threads = self.consume_work_senders.len();
        let mut schedulable_threads = ThreadSet::any(num_threads);
        for thread_id in 0..num_threads {
            if self.consume_work_senders[thread_id].len() >= 100 {
                schedulable_threads.remove(thread_id);
            }
        }

        if schedulable_threads.is_empty() {
            return (0, 0, 0);
        }

        let mut num_scanned: usize = 0;
        let mut num_scheduled: usize = 0;
        let mut num_unschedulable: usize = 0;
        let mut num_early_expired: usize = 0;
        let mut num_sent: usize = 0;

        let current_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        while num_scanned < self.batch_size * schedulable_threads.num_threads() as usize &&
            !schedulable_threads.is_empty() &&
            !self.tx_container.is_empty()
        {
            let Some(mut id) = self.tx_container.pop() else {
                unreachable!("container is not empty")
            };

            id.increment_tries();
            if id.num_tries > 3 {
                self.tx_container.remove_by_id(&id.id);
                num_early_expired += 1;
                continue;
            }

            // a simple remove expired txs
            if current_timestamp - id.timestamp > 61 {
                self.tx_container.remove_by_id(&id.id);
                num_early_expired += 1;
                continue;
            }

            {
                let Some(tx_ref) = self.tx_container.get_transaction(&id.id) else {
                    unreachable!("transaction must exist")
                };

                let tx = tx_ref.transaction().expect("transaction must exist");
                if !recent_blockhashes
                    .read()
                    .unwrap()
                    .contains(tx.message().recent_blockhash())
                {
                    drop(tx_ref);
                    self.tx_container.remove_by_id(&id.id);
                    num_early_expired += 1;
                    continue;
                }

                num_scanned += 1;

                if !self.working_account_set.check_locks(tx.message()) {
                    drop(tx_ref);
                    self.working_account_set.clear();
                    num_sent += self.send_batches(batches, tx_ids);
                }
            }

            let Some(mut transaction_state) = self.tx_container.get_mut_transaction_state(&id.id) else {
                panic!("transaction state must exist")
            };

            match try_schedule_transaction(
                transaction_state.value_mut(),
                &mut self.account_locks,
                schedulable_threads,
                select_thread,
            ) {
                Some((thread_id, transaction)) => {
                    self.working_account_set.take_locks(transaction.message());
                    num_scheduled += 1;
                    batches[thread_id].push(transaction.clone());
                    tx_ids[thread_id].push(id.id);
                }

                None => {
                    num_unschedulable += 1;
                    self.unschedulables.push(id);
                }
            }
        }

        self.working_account_set.clear();
        num_sent += self.send_batches(batches, tx_ids);

        self.metrics.increase_total_tx_scheduled(num_scheduled as u64);
        self.metrics.increase_total_tx_unschedulable(num_unschedulable as u64);
        self.metrics.increase_total_tx_sent(num_sent as u64);
        self.metrics
            .increase_total_tx_early_expired_scheduling(num_early_expired as u64);

        for id in self.unschedulables.drain(..) {
            self.tx_container.push_id_into_queue(id);
        }

        (num_scanned, num_scheduled, num_unschedulable)
    }

    fn send_batches(&mut self, batches: &mut [Vec<SanitizedTransaction>], tx_ids: &mut [Vec<TransactionId>]) -> usize {
        (0..self.consume_work_senders.len())
            .map(|thread_index| self.send_batch(batches, tx_ids, thread_index))
            .sum()
    }

    fn send_batch(
        &mut self,
        batches: &mut [Vec<SanitizedTransaction>],
        tx_ids: &mut [Vec<TransactionId>],
        thread_index: usize,
    ) -> usize {
        if batches[thread_index].is_empty() {
            self.metrics.increment_total_empty_batches();
            return 0;
        }

        let transactions: Vec<SanitizedTransaction> = batches[thread_index].drain(..).collect();
        let tx_ids: Vec<TransactionId> = tx_ids[thread_index].drain(..).collect();
        let num_transactions = transactions.len();

        let batch_id = self.in_flight_tracker.track_batch(num_transactions, thread_index);

        let work = (transactions, tx_ids, batch_id);
        self.consume_work_senders[thread_index].send(work).unwrap();

        self.metrics.increment_total_batches_sent();
        num_transactions
    }

    fn receive_completed(&mut self) -> usize {
        let t0 = Instant::now();
        let mut num_txs = 0;
        let mut num_batches = 0usize;
        let mut num_finalize_events = 0usize;
        let mut max_jobs_per_batch = 0usize;

        let mut recv_time = Duration::ZERO;
        let mut collect_time = Duration::ZERO;
        let mut release_time = Duration::ZERO;
        let mut remove_time = Duration::ZERO;
        let mut batch_commit_time = Duration::ZERO;
        let mut progress_finalize_time = Duration::ZERO;

        while num_txs <= 100_000 {
            let recv_start = Instant::now();
            let Ok((jobs, batch_id)) = self.callback_receiver.try_recv() else {
                recv_time += recv_start.elapsed();
                break;
            };
            recv_time += recv_start.elapsed();

            num_batches += 1;
            max_jobs_per_batch = max_jobs_per_batch.max(jobs.len());

            let collect_start = Instant::now();
            let transactions: Vec<&SanitizedTransaction> = jobs.iter().map(|job| &job.sanitized_transaction).collect();
            collect_time += collect_start.elapsed();

            num_txs += transactions.len();

            // release locks for executed transactions
            let release_start = Instant::now();
            self.complete_batch(batch_id, &transactions);
            release_time += release_start.elapsed();

            // remove all transactions from container
            let remove_start = Instant::now();
            for job in &jobs {
                self.tx_container.id_to_transaction_state.remove(&job.transaction_id);
            }
            remove_time += remove_start.elapsed();

            let Some(first_job) = jobs.first() else {
                continue;
            };

            let batch_commit_start = Instant::now();
            let slot = first_job.slot;
            let worker_id = first_job.worker_id;
            let blockhash = first_job.blockhash;
            let parent_blockhash = first_job.parent_blockhash;
            let timestamp = first_job.timestamp;

            if self.is_flushed(slot) {
                panic!(
                    "received batch for already-finalized slot {slot}; \
                     this should be impossible under scheduler invariants"
                );
            }

            let pending = self.next_shred_index.entry(slot).or_insert(PendingFinalization::new(
                0,
                blockhash,
                parent_blockhash,
                timestamp,
            ));

            let shred_id = ShredId::new(slot, pending.num_shreds as usize);
            pending.num_shreds += 1;

            self.commit_sender
                .send(CommitEvent::Batch(CoreBatchShred {
                    shred_id,
                    worker_id,
                    jobs,
                }))
                .unwrap();
            batch_commit_time += batch_commit_start.elapsed();

            let progress_finalize_start = Instant::now();
            let new_finalized_slots = self.record_worker_progress(worker_id, slot);

            let mut highest_finalized_slot = self.last_finalized_slot;

            for finalized_slot in new_finalized_slots {
                if self.is_flushed(finalized_slot) {
                    panic!("slot {finalized_slot} was returned for finalization but is already flushed");
                }

                let finalization = self.take_slot_finalization(finalized_slot).unwrap_or_else(|| {
                    panic!("finalization requested for slot {finalized_slot} but no PendingFinalization exists")
                });

                self.commit_sender
                    .send(CommitEvent::Finalize(SyncFinalization {
                        slot: finalized_slot,
                        num_shreds: finalization.num_shreds,
                        hash: finalization.hash,
                        parent_hash: finalization.parent_hash,
                        block_unix_timestamp: finalization.block_unix_timestamp,
                        shred_hashes: Vec::new(),
                    }))
                    .unwrap();

                num_finalize_events += 1;
                highest_finalized_slot = highest_finalized_slot.max(finalized_slot);
            }

            self.last_finalized_slot = highest_finalized_slot;
            progress_finalize_time += progress_finalize_start.elapsed();
        }

        self.metrics.increase_total_tx_completed(num_txs as u64);

        let total = t0.elapsed();
        if total > Duration::from_millis(10) {
            info!(
                "receive_completed slow: total={:?} recv={:?} collect={:?} release={:?} remove={:?} batch_commit={:?} progress_finalize={:?} batches={} txs={} finalize_events={} max_jobs_per_batch={} cb_q={} commit_q={}",
                total,
                recv_time,
                collect_time,
                release_time,
                remove_time,
                batch_commit_time,
                progress_finalize_time,
                num_batches,
                num_txs,
                num_finalize_events,
                max_jobs_per_batch,
                self.callback_receiver.len(),
                self.commit_sender.len(),
            );
        }
        num_txs
    }

    fn record_worker_progress(&mut self, worker_id: usize, slot: u64) -> Vec<u64> {
        let last_slot = self.worker_progress.entry(worker_id).or_insert(slot);
        if slot < *last_slot {
            panic!(
                "worker {} reported non-monotonic slot progress: {} -> {}",
                worker_id, *last_slot, slot
            );
        }
        *last_slot = slot;

        self.refresh_idle_worker_progress(slot);

        let current_min = self
            .worker_progress
            .values()
            .copied()
            .min()
            .expect("worker_progress must be non-empty");
        let safe_upper = current_min.saturating_sub(1);

        let start = self.last_finalized_slot.saturating_add(1);
        if safe_upper < start {
            Vec::new()
        } else {
            (start..=safe_upper).collect()
        }
    }

    fn complete_batch(&mut self, batch_id: TransactionBatchId, transactions: &[&SanitizedTransaction]) {
        let thread_id = self.in_flight_tracker.complete_batch(batch_id);
        for transaction in transactions {
            let account_keys = transaction.message().account_keys();
            let write_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| transaction.message().is_writable(index).then_some(key));
            let read_account_locks = account_keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| (!transaction.message().is_writable(index)).then_some(key));
            self.account_locks
                .unlock_accounts(write_account_locks, read_account_locks, thread_id);
        }
    }

    fn is_flushed(&self, slot: u64) -> bool {
        slot <= self.last_finalized_slot
    }

    fn take_slot_finalization(&mut self, slot: u64) -> Option<PendingFinalization> {
        self.next_shred_index.remove(&slot)
    }

    fn refresh_idle_worker_progress(&mut self, reference_slot: u64) {
        for worker_id in 0..crate::SCHEDULER_WORKER_COUNT {
            if self.in_flight_tracker.is_thread_idle(worker_id) {
                let progress = self.worker_progress.entry(worker_id).or_insert(reference_slot);
                *progress = (*progress).max(reference_slot);
            }
        }
    }
}

pub struct TransactionIdGenerator {
    next_id: u64,
}

impl Default for TransactionIdGenerator {
    fn default() -> Self {
        Self { next_id: u64::MAX }
    }
}

impl TransactionIdGenerator {
    #[allow(clippy::should_implement_trait)]
    pub(crate) fn next(&mut self) -> TransactionId {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_sub(1);
        TransactionId::new(id)
    }
}

fn try_schedule_transaction(
    transaction_state: &mut TransactionState,
    account_locks: &mut ThreadAwareAccountLocks,
    schedulable_threads: ThreadSet,
    thread_selector: impl Fn(ThreadSet) -> ThreadId,
) -> Option<(ThreadId, SanitizedTransaction)> {
    let transaction = transaction_state.transaction().unwrap();

    // Schedule the transaction if it can be.
    let account_keys = transaction.message().account_keys();
    let write_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| transaction.message().is_writable(index).then_some(key));
    let read_account_locks = account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| (!transaction.message().is_writable(index)).then_some(key));

    let thread_id = account_locks.try_lock_accounts(
        write_account_locks,
        read_account_locks,
        schedulable_threads,
        thread_selector,
    )?;

    Some((thread_id, transaction_state.transition_to_pending()))
}

fn select_thread(thread_set: ThreadSet) -> ThreadId {
    // randomly select a thread
    let all_threads = thread_set.contained_threads_iter().collect::<Vec<_>>();
    let mut rng = rand::thread_rng();

    all_threads[rng.gen_range(0..all_threads.len())]
}
