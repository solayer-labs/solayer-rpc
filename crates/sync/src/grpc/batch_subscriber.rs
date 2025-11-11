use std::time::Duration;

use infinisvm_logger::{error, info, warn};
use infinisvm_types::sync::{
    grpc::infini_svm_service_client::InfiniSvmServiceClient, CommitBatchNotification, TransactionBatchRequest,
};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_stream::StreamExt;
use tonic::{
    transport::{Channel, Endpoint},
    Request,
};

use crate::types::{FinalizationMarker, SerializableBatch, SerializableNotification};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub fn process_commit_notification(
    notification: &CommitBatchNotification,
) -> Result<SerializableNotification, Box<dyn std::error::Error + Send + Sync>> {
    match notification {
        CommitBatchNotification::Finalization(finalization_data) => {
            info!("Received finalization notification: slot={}", finalization_data.slot);
            return Ok(SerializableNotification::Finalization(FinalizationMarker {
                slot: finalization_data.slot,
                timestamp: finalization_data.timestamp,
                job_ids: finalization_data.job_ids.clone(),
                hash: finalization_data.hash,
                parent_hash: finalization_data.parent_hash,
            }));
        }
        CommitBatchNotification::Batch(batch_data) => {
            info!(
                "Received batch notification: slot={}, batch_size={}, compression_ratio={}%",
                batch_data.slot, batch_data.batch_size, batch_data.compression_ratio
            );

            // Handle empty batches gracefully to avoid zstd "incomplete frame" errors
            if batch_data.batch_size == 0 || batch_data.compressed_transactions.is_empty() {
                // Preserve real metadata so receivers can mark presence for (slot, job_id)
                return Ok(SerializableNotification::Batch(SerializableBatch {
                    slot: batch_data.slot,
                    timestamp: batch_data.timestamp,
                    job_id: batch_data.job_id as usize,
                    transactions: Vec::new(),
                    worker_id: batch_data.worker_id,
                }));
            }

            // Decompress the transaction data
            let decompressed = zstd::decode_all(&batch_data.compressed_transactions[..])?;

            // Deserialize the batch
            let batch: SerializableBatch = bincode::deserialize(&decompressed)?;

            Ok(SerializableNotification::Batch(batch))
        }
    }
}

pub struct TransactionBatchSubscriber {
    endpoint: String,
    client: Option<InfiniSvmServiceClient<Channel>>,
    notification_sender: mpsc::UnboundedSender<SerializableNotification>,
    _notification_receiver: mpsc::UnboundedReceiver<SerializableNotification>,
}

impl TransactionBatchSubscriber {
    pub async fn new(grpc_endpoint: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (notification_sender, notification_receiver) = mpsc::unbounded_channel();

        let mut subscriber = Self {
            endpoint: grpc_endpoint,
            client: None,
            notification_sender,
            _notification_receiver: notification_receiver,
        };

        subscriber.connect().await?;

        Ok(subscriber)
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to gRPC server at {}", self.endpoint);

        let endpoint = Endpoint::from_shared(self.endpoint.clone())?
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(Duration::from_secs(10));

        let channel = endpoint.connect().await?;
        // No custom CA here; rely on webpki roots or plaintext based on endpoint scheme
        self.client = Some(InfiniSvmServiceClient::new(channel, self.endpoint.clone(), None));

        info!("Successfully connected to gRPC server");
        Ok(())
    }

    pub async fn subscribe(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match self.try_subscribe().await {
                Ok(()) => {
                    warn!("Transaction batch subscription ended normally");
                }
                Err(e) => {
                    error!("Transaction batch subscription failed: {}", e);
                }
            }

            info!("Reconnecting in {:?}...", RECONNECT_DELAY);
            sleep(RECONNECT_DELAY).await;

            if let Err(e) = self.connect().await {
                error!("Failed to reconnect: {}", e);
                continue;
            }
        }
    }

    async fn try_subscribe(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.as_mut().ok_or("Client not connected")?;

        let request = Request::new(TransactionBatchRequest {});
        let response = timeout(REQUEST_TIMEOUT, client.subscribe_transaction_batches(request)).await??;

        let mut stream = response.into_inner();
        info!("Transaction batch subscription established");

        while let Some(result) = stream.next().await {
            match result {
                Ok(notification) => {
                    if let Err(e) = self.process_notification(notification).await {
                        error!("Failed to process notification: {}", e);
                    }
                }
                Err(e) => {
                    error!("Stream error: {}", e);
                    return Err(Box::new(e));
                }
            }
        }

        Ok(())
    }

    async fn process_notification(
        &self,
        notification: CommitBatchNotification,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let notification = process_commit_notification(&notification)?;

        self.notification_sender.send(notification)?;

        Ok(())
    }

    pub fn get_notification_receiver(&self) -> mpsc::UnboundedReceiver<SerializableNotification> {
        let (_sender, receiver) = mpsc::unbounded_channel();
        // This is a simplified version - in a real implementation you'd want to
        // properly handle multiple receivers or use a broadcast channel
        receiver
    }
}

// Utility function to start a subscriber as a background task
pub async fn start_subscriber_task(
    grpc_endpoint: String,
    mut batch_handler: impl FnMut(SerializableNotification) -> Result<(), String> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut subscriber = TransactionBatchSubscriber::new(grpc_endpoint).await?;
    let mut receiver = subscriber.get_notification_receiver();

    // Spawn subscription task
    tokio::spawn(async move {
        if let Err(e) = subscriber.subscribe().await {
            error!("Subscriber task failed: {}", e);
        }
    });

    // Handle incoming batches
    tokio::spawn(async move {
        while let Some(batch) = receiver.recv().await {
            if let Err(e) = batch_handler(batch) {
                error!("Batch handler error: {}", e);
            }
        }
    });

    Ok(())
}
