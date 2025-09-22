use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
    time::Instant,
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use infinisvm_logger::{debug, info};
use infinisvm_types::sync::CommitBatchNotification;
use tokio::sync::broadcast;

use infinisvm_types::jobs::ConsumedJob;

use crate::types::SerializableBatch;

const COMPRESSION_LEVEL: i32 = 3; // Balance between speed and compression ratio
const BROADCASTER_THREADS: usize = 4; // Number of threads for broadcasting

pub struct TransactionBatchBroadcaster {
    // Input channel for pre-serialized notifications
    notification_sender: Sender<Arc<CommitBatchNotification>>,

    // Output broadcast channel for notifications
    notification_broadcast: broadcast::Sender<Arc<CommitBatchNotification>>,

    // Thread handles for broadcasting
    _broadcaster_handles: Vec<thread::JoinHandle<()>>,

    // Round-robin counter for broadcast threads
    broadcast_counter: Arc<AtomicUsize>,
}

impl TransactionBatchBroadcaster {
    pub fn new() -> Self {
        // Channel from committer to broadcaster threads (after serialization)
        let (notification_sender, notification_receiver) = unbounded::<Arc<CommitBatchNotification>>();

        // Final broadcast channel to subscribers
        let (notification_broadcast, _) = broadcast::channel::<Arc<CommitBatchNotification>>(512);

        let broadcast_counter = Arc::new(AtomicUsize::new(0));

        // Create broadcaster threads with round-robin distribution
        let mut broadcaster_handles = Vec::new();
        for broadcaster_id in 0..BROADCASTER_THREADS {
            let notification_receiver = notification_receiver.clone();
            let notification_broadcast = notification_broadcast.clone();
            let broadcast_counter = broadcast_counter.clone();

            let handle = thread::Builder::new()
                .name(format!("batch-broadcaster-{}", broadcaster_id))
                .spawn(move || {
                    Self::broadcaster_loop(
                        broadcaster_id,
                        notification_receiver,
                        notification_broadcast,
                        broadcast_counter,
                    );
                })
                .expect("Failed to spawn batch broadcaster thread");

            broadcaster_handles.push(handle);
        }

        Self {
            notification_sender,
            notification_broadcast,
            _broadcaster_handles: broadcaster_handles,
            broadcast_counter,
        }
    }

    // This method does serialization inline but sends to broadcast threads non-blocking
    pub fn broadcast_batch(&self, batch: Vec<ConsumedJob>) -> Result<(), String> {
        // Serialize and compress in the calling thread (committer thread)
        let start_time = Instant::now();

        let original_len = batch.len();
        if original_len == 0 {
            // Shouldn't happen in normal operation (committer guards this),
            // but avoid propagating a misleading 0/0 notification.
            return Err("broadcast_batch called with empty batch".to_string());
        }

        let meta = batch.first().map(|j| (j.slot, j.job_id, j.timestamp));

        // Only successful txs are included in the broadcast payload today.
        // Log what we are dropping to aid debugging follower slot-plan mismatches.
        let filtered: Vec<ConsumedJob> = batch
            .into_iter()
            .filter(|job| job.processed_transaction.is_ok())
            .collect();
        let success_len = filtered.len();
        if let Some((slot, job_id, timestamp)) = meta {
            if success_len == 0 && original_len > 0 {
                // All transactions failed. Preserve the real (slot, job_id) metadata
                // and broadcast an empty payload so receivers can mark presence
                // and refetch against the correct cache key.
                infinisvm_logger::warn!(
                    "Broadcaster: all txs failed for slot={} job_id={} (orig_len={}); sending empty payload with preserved metadata",
                    slot,
                    job_id,
                    original_len
                );
                metrics::counter!("broadcast_batches_all_failed_total").increment(1);

                let notification = infinisvm_types::sync::CommitBatchNotification {
                    slot,
                    timestamp,
                    batch_size: 0,
                    compressed_transactions: Vec::new(),
                    compression_ratio: 0,
                    job_id: job_id as u64,
                };

                let processing_time = start_time.elapsed();
                infinisvm_logger::debug!(
                    "Serialized batch in {:?}, compression ratio: {}%",
                    processing_time,
                    notification.compression_ratio
                );

                let notification = Arc::new(notification);
                return self
                    .notification_sender
                    .send(notification)
                    .map_err(|e| format!("Failed to send notification to broadcaster: {}", e));
            } else if original_len > success_len {
                infinisvm_logger::debug!(
                    "Broadcaster: dropping {} failed txs for slot={} job_id={} (successes={})",
                    original_len - success_len,
                    slot,
                    job_id,
                    success_len
                );
                metrics::counter!("broadcast_txs_dropped_errors_total").increment((original_len - success_len) as u64);
            }
        }

        let notification = Self::serialize_and_compress_batch(filtered)?;

        let processing_time = start_time.elapsed();
        debug!(
            "Serialized batch in {:?}, compression ratio: {}%",
            processing_time, notification.compression_ratio
        );

        // Send to broadcaster threads (non-blocking)
        let notification = Arc::new(notification);
        self.notification_sender
            .send(notification)
            .map_err(|e| format!("Failed to send notification to broadcaster: {}", e))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<CommitBatchNotification>> {
        self.notification_broadcast.subscribe()
    }

    fn broadcaster_loop(
        broadcaster_id: usize,
        notification_receiver: Receiver<Arc<CommitBatchNotification>>,
        notification_broadcast: broadcast::Sender<Arc<CommitBatchNotification>>,
        _broadcast_counter: Arc<AtomicUsize>,
    ) {
        info!("Batch broadcaster worker {} started", broadcaster_id);

        // All threads compete to process notifications - simple work stealing
        while let Ok(notification) = notification_receiver.recv() {
            let start_time = Instant::now();

            // Broadcast to all subscribers
            match notification_broadcast.send(notification.clone()) {
                Ok(subscriber_count) => {
                    let broadcast_time = start_time.elapsed();
                    debug!(
                        "Broadcaster {} sent batch (slot: {}, {} transactions) to {} subscribers in {:?}",
                        broadcaster_id, notification.slot, notification.batch_size, subscriber_count, broadcast_time
                    );
                }
                Err(_) => {
                    debug!("Broadcaster {} - no active subscribers", broadcaster_id);
                }
            }
        }

        info!("Batch broadcaster worker {} shutting down", broadcaster_id);
    }

    fn serialize_and_compress_batch(batch: Vec<ConsumedJob>) -> Result<CommitBatchNotification, String> {
        // Serialize the batch
        if batch.is_empty() {
            // Should not be used for "all failed" case; caller preserves metadata there.
            return Err("serialize_and_compress_batch called with empty batch".to_string());
        }
        let serialized = bincode::serialize(&SerializableBatch::from_consumed_jobs(&batch))
            .map_err(|e| format!("Failed to serialize batch: {}", e))?;

        let original_size = serialized.len();

        // Compress with zstd
        let compressed = zstd::encode_all(&serialized[..], COMPRESSION_LEVEL)
            .map_err(|e| format!("Failed to compress batch: {}", e))?;

        let compressed_size = compressed.len();
        let compression_ratio = if compressed_size > 0 {
            (original_size * 100) / compressed_size
        } else {
            100
        };

        debug!(
            "Batch serialization: original {}KB, compressed {}KB, ratio {}%",
            original_size / 1024,
            compressed_size / 1024,
            compression_ratio
        );

        Ok(CommitBatchNotification {
            slot: batch[0].slot,
            timestamp: batch[0].timestamp,
            batch_size: batch.len() as u32,
            compressed_transactions: compressed,
            compression_ratio: compression_ratio as u64,
            job_id: batch[0].job_id as u64,
        })
    }
}

impl Drop for TransactionBatchBroadcaster {
    fn drop(&mut self) {
        info!("TransactionBatchBroadcaster shutting down...");
        // The sender being dropped will cause all threads to exit their loops
    }
}
