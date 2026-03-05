use std::time::Duration;

use infinisvm_logger::{error, info, warn};
use infinisvm_types::sync::CommitBatchNotification;
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_stream::StreamExt;
use tonic::Request;

use crate::grpc::service::{InfiniSvmServiceClient, SubscribeTransactionBatchRequest};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct TransactionBatchSubscriber {
    endpoint: String,
    client: Option<InfiniSvmServiceClient>,
    notification_sender: mpsc::UnboundedSender<CommitBatchNotification>,
    _notification_receiver: mpsc::UnboundedReceiver<CommitBatchNotification>,
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

        self.client = Some(InfiniSvmServiceClient::new(self.endpoint.clone()));

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

        let request = Request::new(SubscribeTransactionBatchRequest {});
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
        self.notification_sender.send(notification)?;
        Ok(())
    }
}
