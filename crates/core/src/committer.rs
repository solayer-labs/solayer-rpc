use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Instant, SystemTime},
};

use crossbeam_channel::Receiver;
use infinisvm_logger::{info, warn};
use infinisvm_sync::grpc::TransactionBatchBroadcaster;
use infinisvm_types::{core_batch_shred::CoreBatchShred, sync::SyncFinalization};
use metrics::gauge;
use rand::seq::SliceRandom;

use crate::wal_writer::WalWriter;

pub type PerfSample = (u64, u64, u64, u64); // slot(sampled at), num_transactions, num_slots, sample_duration

#[derive(Debug)]
pub enum CommitEvent {
    #[allow(private_interfaces)]
    Batch(CoreBatchShred),
    Flush(SyncFinalization), // in case no tx and no batch, we still need to flush
    Finalize(SyncFinalization),
}

pub struct Committer {
    commit_receiver: Receiver<CommitEvent>,

    // gRPC batch broadcaster
    batch_broadcaster: Option<Vec<Arc<TransactionBatchBroadcaster>>>,
    pending_finalizations: VecDeque<SyncFinalization>,

    // wal
    wal_writer: WalWriter,

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
    ) -> Self {
        Self {
            commit_receiver,
            batch_broadcaster: None,
            pending_finalizations: VecDeque::new(),
            wal_writer: WalWriter::new(),
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
                Ok(event) => {
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64();
                    gauge!("committer_last_event_ts").set(now);
                    match event {
                        CommitEvent::Batch(commit_batch) => {
                            info!("commit batch {:?}", commit_batch.shred_id);
                            if commit_batch.is_empty() {
                                continue;
                            }

                            let num_txs = commit_batch.len();
                            let slot = commit_batch.slot();

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

                            let sync_batch = commit_batch.into_sync_batch_shred();
                            if !sync_batch.effects.is_empty() {
                                self.wal_writer
                                    .cache_slot_transactions(sync_batch.shred_id.clone(), sync_batch.effects.clone());
                            }

                            // broadcast batch
                            if let Some(ref broadcasters) = self.batch_broadcaster {
                                // pick one broadcaster randomly
                                let broadcaster = broadcasters.choose(&mut rand::thread_rng()).unwrap();
                                if let Err(e) = broadcaster.broadcast_batch(sync_batch) {
                                    warn!("Failed to broadcast batch: {e}");
                                }
                            }
                        }
                        CommitEvent::Flush(finalization) => {
                            info!("flush finalization {:?}", finalization);
                            // in case no tx and no batch. this is optimistic finalization
                            self.pending_finalizations.push_back(finalization);
                            self.process_pending_finalizations();
                        }
                        CommitEvent::Finalize(finalization) => {
                            info!("finalize finalization {:?}", finalization);
                            self.pending_finalizations.push_back(finalization);
                            self.process_pending_finalizations();
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }

    fn process_pending_finalizations(&mut self) {
        while let Some(finalization) = self.pending_finalizations.pop_front() {
            let shreds = self.wal_writer.take_slot_transactions(finalization.slot);
            if let Err(err) = WalWriter::persist_slot(self.wal_writer.slots_path(), finalization.clone(), shreds) {
                panic!("Failed to persist WAL for finalized slot {finalization:?}: {err}");
            }

            self.broadcast_finalization(finalization);
        }
    }

    fn broadcast_finalization(&self, finalization: SyncFinalization) {
        let slot = finalization.slot;
        info!("Broadcasting finalization for slot {}", slot);
        if let Some(ref broadcasters) = self.batch_broadcaster {
            for broadcaster in broadcasters {
                if let Err(e) = broadcaster.broadcast_finalization(finalization.clone()) {
                    warn!("Failed to broadcast block finalization for slot {}: {}", slot, e);
                }
            }
        }
    }
}
