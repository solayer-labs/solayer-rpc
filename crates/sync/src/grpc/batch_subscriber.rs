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

use crate::types::SerializableBatch;

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub fn process_commit_notification(
    notification: &CommitBatchNotification,
) -> Result<SerializableBatch, Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "Received batch notification: slot={}, batch_size={}, compression_ratio={}%",
        notification.slot, notification.batch_size, notification.compression_ratio
    );

    if notification.is_final {
        return Ok(SerializableBatch {
            slot: notification.slot,
            timestamp: notification.timestamp,
            job_id: notification.job_id as usize,
            transactions: Vec::new(),
            worker_id: notification.worker_id,
            is_final: true,
        });
    }

    // Handle empty batches gracefully to avoid zstd "incomplete frame" errors
    if notification.batch_size == 0 || notification.compressed_transactions.is_empty() {
        // Preserve real metadata so receivers can mark presence for (slot, job_id)
        return Ok(SerializableBatch {
            slot: notification.slot,
            timestamp: notification.timestamp,
            job_id: notification.job_id as usize,
            transactions: Vec::new(),
            worker_id: notification.worker_id,
            is_final: false,
        });
    }

    // Decompress the transaction data
    let decompressed = zstd::decode_all(&notification.compressed_transactions[..])?;

    // Deserialize the batch
    let mut batch: SerializableBatch = bincode::deserialize(&decompressed)?;
    batch.is_final = notification.is_final;

    Ok(batch)
}

pub struct TransactionBatchSubscriber {
    endpoint: String,
    client: Option<InfiniSvmServiceClient<Channel>>,
    notification_sender: mpsc::UnboundedSender<SerializableBatch>,
    _notification_receiver: mpsc::UnboundedReceiver<SerializableBatch>,
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
        let batch = process_commit_notification(&notification)?;

        self.notification_sender.send(batch)?;

        Ok(())
    }

    pub fn get_notification_receiver(&self) -> mpsc::UnboundedReceiver<SerializableBatch> {
        let (_sender, receiver) = mpsc::unbounded_channel();
        // This is a simplified version - in a real implementation you'd want to
        // properly handle multiple receivers or use a broadcast channel
        receiver
    }
}

// Utility function to start a subscriber as a background task
pub async fn start_subscriber_task(
    grpc_endpoint: String,
    mut batch_handler: impl FnMut(SerializableBatch) -> Result<(), String> + Send + 'static,
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
