use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Instant, SystemTime},
};

use crossbeam_channel::Receiver;
use infinisvm_logger::{info, warn};
use infinisvm_sync::grpc::TransactionBatchBroadcaster;
use infinisvm_types::sync::{SignedFinalization, SyncBatchShred, SyncFinalization};
use metrics::gauge;
use rand::seq::SliceRandom;
use solana_sdk::signature::{Keypair, Signer};

use crate::{scheduler::core_batch_shred::CoreBatchShred, wal_writer::WalWriter};

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
    pending_shred_hashes: HashMap<u64, Vec<[u8; 32]>>,
    finalizer_signer: Option<FinalizerSigner>,

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
            pending_shred_hashes: HashMap::new(),
            finalizer_signer: None,
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

    pub fn with_finalizer_signer(mut self, signer: FinalizerSigner) -> Self {
        self.finalizer_signer = Some(signer);
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
                            self.record_shred_hash(&sync_batch);
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
        while let Some(mut finalization) = self.pending_finalizations.pop_front() {
            finalization.shred_hashes = self.take_shred_hashes(finalization.slot, finalization.num_shreds);
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
            let signer = self
                .finalizer_signer
                .as_ref()
                .expect("finalizer signer must be configured");
            let signed = signer.sign(&finalization);
            for broadcaster in broadcasters {
                if let Err(e) = broadcaster.broadcast_signed_finalization(signed.clone()) {
                    warn!("Failed to broadcast signed finalization for slot {}: {}", slot, e);
                }
            }
        }
    }

    fn record_shred_hash(&mut self, batch: &SyncBatchShred) {
        let entry = self.pending_shred_hashes.entry(batch.shred_id.slot).or_default();
        let index = batch.shred_id.index;
        if entry.len() <= index {
            entry.resize(index + 1, [0u8; 32]);
        }
        entry[index] = batch.shred_hash;
    }

    fn take_shred_hashes(&mut self, slot: u64, num_shreds: u64) -> Vec<[u8; 32]> {
        let hashes = self.pending_shred_hashes.remove(&slot).unwrap_or_default();
        let expected = num_shreds as usize;
        if hashes.len() != expected {
            panic!(
                "shred hash list length mismatch for slot {}: expected {}, got {}",
                slot,
                expected,
                hashes.len()
            );
        }
        hashes
    }
}

pub struct FinalizerSigner {
    keypair: Keypair,
    pubkey: [u8; 32],
}

impl FinalizerSigner {
    pub fn new(keypair: Keypair) -> Self {
        let pubkey = keypair.pubkey().to_bytes();
        Self { keypair, pubkey }
    }

    pub fn pubkey_bytes(&self) -> [u8; 32] {
        self.pubkey
    }

    pub fn sign(&self, finalization: &SyncFinalization) -> SignedFinalization {
        let msg = bincode::serialize(finalization).expect("serialize finalization");
        let sig = self.keypair.sign_message(&msg);
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(sig.as_ref());
        SignedFinalization {
            finalization: finalization.clone(),
            sequencer_pubkey: self.pubkey,
            signature: sig_bytes,
        }
    }
}
