use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use infinisvm_logger::info;
use infinisvm_types::sync::{CommitBatchNotification, SignedFinalization, SyncBatchShred, SyncFinalization};
use tokio::sync::broadcast;

const BROADCASTER_THREADS: usize = 4; // Number of threads for broadcasting

pub struct TransactionBatchBroadcaster {
    // Input channel for pre-serialized notifications
    notification_sender: Sender<Arc<CommitBatchNotification>>,

    // Output broadcast channel for notifications
    notification_broadcast: broadcast::Sender<Arc<CommitBatchNotification>>,

    // Thread handles for broadcasting
    _broadcaster_handles: Vec<thread::JoinHandle<()>>,
}

impl TransactionBatchBroadcaster {
    pub fn new() -> Self {
        let (notification_sender, notification_receiver) = unbounded::<Arc<CommitBatchNotification>>();
        let (notification_broadcast, _) = broadcast::channel::<Arc<CommitBatchNotification>>(512);

        let broadcast_counter = Arc::new(AtomicUsize::new(0));

        // Create broadcaster threads with round-robin distribution
        let mut broadcaster_handles = Vec::new();
        for broadcaster_id in 0..BROADCASTER_THREADS {
            let notification_receiver = notification_receiver.clone();
            let notification_broadcast = notification_broadcast.clone();
            let broadcast_counter = broadcast_counter.clone();

            let handle = thread::Builder::new()
                .name(format!("batch-broadcaster-{broadcaster_id}"))
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
        }
    }

    // This method does serialization inline but sends to broadcast threads
    // non-blocking
    pub fn broadcast_batch(&self, batch: SyncBatchShred) -> Result<(), String> {
        // Send to broadcaster threads (non-blocking)
        let notification = Arc::new(CommitBatchNotification::Batch(batch));
        self.notification_sender
            .send(notification)
            .map_err(|e| format!("Failed to send notification to broadcaster: {e}"))
    }

    pub fn broadcast_finalization(&self, finalization: SyncFinalization) -> Result<(), String> {
        let notification = Arc::new(CommitBatchNotification::Finalization(finalization));
        self.notification_sender
            .send(notification)
            .map_err(|e| format!("Failed to send finalization to broadcaster: {e}"))
    }

    pub fn broadcast_signed_finalization(&self, finalization: SignedFinalization) -> Result<(), String> {
        let notification = Arc::new(CommitBatchNotification::SignedFinalization(finalization));
        self.notification_sender
            .send(notification)
            .map_err(|e| format!("Failed to send signed finalization to broadcaster: {e}"))
    }

    pub fn publish_notification(&self, notification: Arc<CommitBatchNotification>) -> Result<(), String> {
        self.notification_sender
            .send(notification)
            .map_err(|e| format!("Failed to send notification to broadcaster: {e}"))
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
            // Broadcast to all subscribers
            if notification_broadcast.send(notification).is_ok() {}
        }

        info!("Batch broadcaster worker {} shutting down", broadcaster_id);
    }
}

impl Default for TransactionBatchBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}
