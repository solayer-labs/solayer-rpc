use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use crossbeam_channel::Receiver;
use infinisvm_logger::{info, warn};
use infinisvm_sync::{grpc::TransactionBatchBroadcaster, types::SerializableBatch};
use infinisvm_types::jobs::ConsumedJob;
use metrics::gauge;
use rand::Rng;
use solana_hash::Hash;
use solana_sha256_hasher::{hashv, Hasher};

use crate::{bank::Bank, wal};

pub type PerfSample = (u64, u64, u64, u64); // slot(sampled at), num_transactions, num_slots, sample_duration

pub enum CommitEvent {
    Batch(Vec<ConsumedJob>),
    Flush,
    Finalize { slot: u64, timestamp: u64 },
}

struct SlotHashAccumulator {
    hasher: Hasher,
}

impl SlotHashAccumulator {
    fn new(slot: u64) -> Self {
        let mut hasher = Hasher::default();
        let slot_bytes = slot.to_le_bytes();
        hasher.hash(&slot_bytes);
        Self { hasher }
    }

    fn absorb_jobs(&mut self, jobs: &[ConsumedJob]) {
        if jobs.is_empty() {
            return;
        }

        let serializable = SerializableBatch::from_consumed_jobs(jobs);
        let job_id_bytes = (serializable.job_id as u64).to_le_bytes();
        let timestamp_bytes = serializable.timestamp.to_le_bytes();
        let tx_count_bytes = (serializable.transactions.len() as u64).to_le_bytes();

        self.hasher.hash(&job_id_bytes);
        self.hasher.hash(&timestamp_bytes);
        self.hasher.hash(&tx_count_bytes);

        for tx in &serializable.transactions {
            self.hasher.hash(tx.signature.as_slice());
            self.hasher.hash(tx.transaction.as_slice());
            self.hasher.hash(tx.result.as_slice());
            self.hasher.hash(tx.pre_accounts.as_slice());

            self.hasher.hash(&tx.slot.to_le_bytes());
            self.hasher.hash(&tx.block_unix_timestamp.to_le_bytes());
            self.hasher.hash(&tx.seq_number.to_le_bytes());
        }
    }

    fn finalize(self) -> Hash {
        self.hasher.result()
    }
}

pub struct Committer {
    commit_receiver: Receiver<CommitEvent>,

    // gRPC batch broadcaster
    batch_broadcaster: Option<Vec<Arc<TransactionBatchBroadcaster>>>,
    batch_buffer: Vec<ConsumedJob>,
    pending_finalizations: VecDeque<(u64, u64)>,
    slot_hash_accumulators: HashMap<u64, SlotHashAccumulator>,
    bank: Arc<RwLock<Bank>>,

    // sampling things
    samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>, // slot(sampled at), num_transactions, num_slots

    // tps printing
    tx_count: Arc<RwLock<(usize, Instant)>>,
    total_transaction_count: Arc<AtomicU64>,
}

impl Committer {
    pub fn new(
        commit_receiver: Receiver<CommitEvent>,
        samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>,
        total_transaction_count: Arc<AtomicU64>,
        bank: Arc<RwLock<Bank>>,
    ) -> Self {
        Self {
            commit_receiver,
            batch_broadcaster: None,
            batch_buffer: Vec::new(),
            pending_finalizations: VecDeque::new(),
            slot_hash_accumulators: HashMap::new(),
            bank,
            samples,
            tx_count: Arc::new(RwLock::new((0, Instant::now()))),
            total_transaction_count,
        }
    }

    pub fn with_batch_broadcaster(mut self, broadcaster: Vec<Arc<TransactionBatchBroadcaster>>) -> Self {
        self.batch_broadcaster = Some(broadcaster);
        self
    }

    pub fn run_loop(&mut self, exit: Arc<AtomicBool>) {
        let tx_count = self.tx_count.clone();
        let exit_clone = exit.clone();
        std::thread::Builder::new()
            .name("tpsPrinter".to_string())
            .spawn(move || {
                while !exit_clone.load(Ordering::Relaxed) {
                    // print tps every second
                    if tx_count.read().unwrap().1.elapsed().as_millis() >= 1000 {
                        let elapsed = tx_count.read().unwrap().1.elapsed();
                        let tpms = tx_count.read().unwrap().0 as f64 * 1000.0 / elapsed.as_millis() as f64;
                        info!("tps: {}", tpms);

                        let mut tx_count = tx_count.write().unwrap();
                        tx_count.0 = 0;
                        tx_count.1 = Instant::now();
                    }
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
            })
            .unwrap();

        let mut num_slots = 0;
        let mut last_slot = 0;
        let mut num_transactions = 0;
        while !exit.load(Ordering::Relaxed) {
            match self.commit_receiver.recv() {
                Ok(CommitEvent::Batch(commit_batch)) => {
                    if commit_batch.is_empty() {
                        continue;
                    }

                    self.record_batch_for_hash(&commit_batch);

                    let num_txs = commit_batch.len();
                    let first_job = commit_batch.first().expect("non-empty batch has first job");
                    let slot = first_job.slot;

                    if last_slot < slot {
                        let slot_diff = if last_slot == 0 { 1 } else { slot - last_slot };
                        last_slot = slot;
                        num_slots += slot_diff;
                    }
                    num_transactions += num_txs as u64;

                    let (sample_duration, should_rotate_samples) = {
                        let samples_read = self.samples.read().unwrap();
                        let duration = samples_read.0.elapsed().as_secs();
                        if duration > 60 {
                            gauge!("commit_receiver_length").set(self.commit_receiver.len() as f64);
                        }
                        (duration, duration > 60)
                    };

                    if should_rotate_samples {
                        let mut samples = self.samples.write().unwrap();
                        samples.0 = Instant::now();
                        samples
                            .1
                            .push_back((slot, num_transactions, num_slots, sample_duration));
                        num_slots = 0;
                        num_transactions = 0;

                        if samples.1.len() > 720 {
                            samples.1.pop_front();
                        }
                    }

                    self.tx_count.write().unwrap().0 += num_txs;
                    self.total_transaction_count
                        .fetch_add(num_txs as u64, std::sync::atomic::Ordering::Relaxed);

                    let same_job = self.batch_buffer.last().map(|job| job.job_id) == Some(first_job.job_id);

                    if same_job {
                        self.batch_buffer.extend(commit_batch);
                    } else {
                        let previous_batch = std::mem::take(&mut self.batch_buffer);
                        self.batch_buffer = commit_batch;

                        if let Some(ref broadcaster) = self.batch_broadcaster {
                            if !previous_batch.is_empty() {
                                if let Err(e) = wal::persist_batch(&previous_batch) {
                                    warn!(
                                        "Failed to persist WAL for job before broadcast: {}. Will retry later.",
                                        e
                                    );
                                    let mut restored = previous_batch;
                                    restored.extend(std::mem::take(&mut self.batch_buffer));
                                    self.batch_buffer = restored;
                                } else if let Err(e) = broadcaster[rand::thread_rng().gen_range(0..broadcaster.len())]
                                    .broadcast_batch(previous_batch)
                                {
                                    warn!("Failed to broadcast commit batch: {}", e);
                                }
                            }
                        }
                    }

                    self.process_pending_finalizations();
                }
                Ok(CommitEvent::Flush) => {
                    if self.flush_pending_batch() {
                        self.process_pending_finalizations();
                    }
                }
                Ok(CommitEvent::Finalize { slot, timestamp }) => {
                    self.pending_finalizations.push_back((slot, timestamp));
                    self.process_pending_finalizations();
                }
                Err(_) => break,
            }
        }
    }

    fn record_batch_for_hash(&mut self, jobs: &[ConsumedJob]) {
        if jobs.is_empty() {
            return;
        }

        let slot = jobs[0].slot;
        let accumulator = self
            .slot_hash_accumulators
            .entry(slot)
            .or_insert_with(|| SlotHashAccumulator::new(slot));
        accumulator.absorb_jobs(jobs);
    }

    fn finalize_slot_hash(&mut self, slot: u64, timestamp: u64) {
        let hash = self
            .slot_hash_accumulators
            .remove(&slot)
            .map(SlotHashAccumulator::finalize)
            .unwrap_or_else(|| {
                let slot_bytes = slot.to_le_bytes();
                let timestamp_bytes = timestamp.to_le_bytes();
                hashv(&[&slot_bytes, &timestamp_bytes])
            });

        if let Ok(mut bank) = self.bank.write() {
            bank.finalize_blockhash(slot, hash);
        } else {
            warn!("Failed to acquire bank lock when recording blockhash for slot {slot}");
        }
    }

    fn flush_pending_batch(&mut self) -> bool {
        if self.batch_buffer.is_empty() {
            return true;
        }

        let batch = std::mem::take(&mut self.batch_buffer);
        info!(
            "Broadcasting batch of size {} with job_id {}",
            batch.len(),
            batch[0].job_id
        );

        if let Some(ref broadcaster) = self.batch_broadcaster {
            if let Err(e) = wal::persist_batch(&batch) {
                warn!(
                    "Failed to persist WAL for job before broadcast: {}. Will retry later.",
                    e
                );
                self.batch_buffer = batch;
                return false;
            } else if let Err(e) =
                broadcaster[rand::thread_rng().gen_range(0..broadcaster.len())].broadcast_batch(batch)
            {
                warn!("Failed to broadcast commit batch: {}", e);
            }
        }

        true
    }

    fn process_pending_finalizations(&mut self) {
        while let Some((slot, timestamp)) = self.pending_finalizations.front().copied() {
            if !self.flush_pending_batch() {
                break;
            }

            let _ = self.pending_finalizations.pop_front();
            self.broadcast_finalization(slot, timestamp);
            self.finalize_slot_hash(slot, timestamp);
        }
    }

    fn broadcast_finalization(&self, slot: u64, timestamp: u64) {
        info!("Broadcasting finalization for slot {slot}");
        if let Some(ref broadcaster) = self.batch_broadcaster {
            if let Err(e) =
                broadcaster[rand::thread_rng().gen_range(0..broadcaster.len())].broadcast_finalization(slot, timestamp)
            {
                warn!("Failed to broadcast block finalization for slot {}: {}", slot, e);
            }
        }
    }
}
