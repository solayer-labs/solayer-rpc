use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use crossbeam_channel::Receiver;
use infinisvm_logger::{debug, info, warn};
use infinisvm_sync::grpc::TransactionBatchBroadcaster;
use infinisvm_types::jobs::ConsumedJob;
use metrics::gauge;
use rand::Rng;

use crate::{indexer::Indexer, wal};

#[derive(Clone, Copy, Debug)]
pub enum IndexingBehavior {
    BroadcastToExternalSubscribers,
    Index,
}

pub type PerfSample = (u64, u64, u64, u64); // slot(sampled at), num_transactions, num_slots, sample_duration

pub struct Committer {
    commit_receiver: Receiver<Vec<ConsumedJob>>,

    indexer: Option<Arc<RwLock<dyn Indexer>>>,

    // gRPC batch broadcaster
    batch_broadcaster: Option<Vec<Arc<TransactionBatchBroadcaster>>>,
    batch_buffer: Vec<ConsumedJob>,

    // sampling things
    samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>, // slot(sampled at), num_transactions, num_slots

    // tps printing
    tx_count: Arc<RwLock<(usize, Instant)>>,
    total_transaction_count: Arc<AtomicU64>,

    indexing_behavior: IndexingBehavior,
}

impl Committer {
    pub fn new(
        commit_receiver: Receiver<Vec<ConsumedJob>>,
        indexer: Option<Arc<RwLock<dyn Indexer>>>,
        samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>,
        total_transaction_count: Arc<AtomicU64>,
        indexing_behavior: IndexingBehavior,
    ) -> Self {
        Self {
            commit_receiver,
            indexer,
            batch_broadcaster: None,
            batch_buffer: Vec::new(),
            samples,
            tx_count: Arc::new(RwLock::new((0, Instant::now()))),
            total_transaction_count,
            indexing_behavior,
        }
    }

    pub fn with_batch_broadcaster(mut self, broadcaster: Vec<Arc<TransactionBatchBroadcaster>>) -> Self {
        self.batch_broadcaster = Some(broadcaster);
        self
    }

    fn flush_buffer(&mut self) {
        if self.batch_buffer.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.batch_buffer);
        if let Some(ref broadcaster) = self.batch_broadcaster {
            if let Err(e) = wal::persist_batch(&batch) {
                warn!(
                    "Failed to persist WAL for job before broadcast on flush: {}. Dropping flush.",
                    e
                );
                // Put it back so caller may retry later if needed
                self.batch_buffer = batch;
            } else if let Err(e) =
                broadcaster[rand::thread_rng().gen_range(0..broadcaster.len())].broadcast_batch(batch)
            {
                warn!("Failed to broadcast commit batch on flush: {}", e);
            }
        }
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
            if let Ok(commit_batch) = self.commit_receiver.recv() {
                if let Some(indexer) = &self.indexer {
                    let mut indexer = indexer.write().unwrap();
                    let timestamp = if !commit_batch.is_empty() {
                        let num_txs = commit_batch.len();
                        let first_job = commit_batch.first().unwrap();
                        let ConsumedJob { slot, timestamp, .. } = first_job;

                        {
                            if last_slot < *slot {
                                // it's possible that last_slot > slot
                                let slot_diff = if last_slot == 0 { 1 } else { *slot - last_slot };
                                last_slot = *slot;
                                num_slots += slot_diff;
                            }
                            num_transactions += num_txs as u64;

                            let samples = self.samples.read().unwrap();
                            let sample_duration = samples.0.elapsed().as_secs();
                            if sample_duration > 60 {
                                // report channel length
                                gauge!("commit_receiver_length").set(self.commit_receiver.len() as f64);

                                // append new sample
                                drop(samples);

                                let mut samples = self.samples.write().unwrap();
                                samples.0 = Instant::now();
                                samples
                                    .1
                                    .push_back((*slot, num_transactions, num_slots, sample_duration));
                                num_slots = 0;
                                num_transactions = 0;

                                if samples.1.len() > 720 {
                                    samples.1.pop_front();
                                }
                            }
                        }

                        self.tx_count.write().unwrap().0 += num_txs;
                        self.total_transaction_count
                            .fetch_add(num_txs as u64, std::sync::atomic::Ordering::Relaxed);

                        let start = std::time::Instant::now();
                        let duration = start.elapsed();
                        debug!("index_transactions {} time: {:?}", num_txs, duration);

                        *timestamp
                    } else {
                        0
                    };

                    match self.indexing_behavior {
                        IndexingBehavior::BroadcastToExternalSubscribers => {
                            // Broadcast to gRPC subscribers after making batch durable (non-blocking)
                            if !commit_batch.is_empty() {
                                // only send when job_id is different. otherwise buffer it
                                let broadcast_batch =
                                    if self.batch_buffer.last().map(|job| job.job_id) == Some(commit_batch[0].job_id) {
                                        self.batch_buffer.extend(commit_batch);
                                        None
                                    } else {
                                        let batch = std::mem::take(&mut self.batch_buffer);
                                        self.batch_buffer = commit_batch;
                                        Some(batch)
                                    };

                                if let Some(ref broadcaster) = self.batch_broadcaster {
                                    if let Some(batch) = broadcast_batch {
                                        // First, persist WAL to ensure durability before broadcast
                                        if let Err(e) = wal::persist_batch(&batch) {
                                            warn!(
                                                "Failed to persist WAL for job before broadcast: {}. Will retry later.",
                                                e
                                            );
                                            // Put the batch back into buffer and retry on next loop
                                            self.batch_buffer = batch;
                                        } else if let Err(e) = broadcaster
                                            [rand::thread_rng().gen_range(0..broadcaster.len())]
                                        .broadcast_batch(batch)
                                        {
                                            warn!("Failed to broadcast commit batch: {}", e);
                                        }
                                    }
                                }
                            } else {
                                // if there's batch in buffer, try to broadcast it
                                if !self.batch_buffer.is_empty() {
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
                                        } else if let Err(e) = broadcaster
                                            [rand::thread_rng().gen_range(0..broadcaster.len())]
                                        .broadcast_batch(batch)
                                        {
                                            warn!("Failed to broadcast commit batch: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                        IndexingBehavior::Index => {
                            #[cfg(not(feature = "no_index"))]
                            indexer.index_transactions(commit_batch, timestamp);
                        }
                    }
                }
            }
        }

        // when exit, make sure to flush all the data
        #[cfg(not(feature = "no_index"))]
        while let Ok(commit_batch) = self.commit_receiver.try_recv() {
            if let Some(indexer) = &self.indexer {
                let mut indexer = indexer.write().unwrap();
                indexer.index_transactions(commit_batch, 0);
            }
        }

        // Flush any buffered broadcast batch on shutdown
        self.flush_buffer();
    }
}

impl Drop for Committer {
    fn drop(&mut self) {
        // Best-effort flush in case run_loop wasn't able to finish cleanly
        self.flush_buffer();
    }
}
