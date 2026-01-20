use std::{pin::Pin, sync::Arc};

use dashmap::DashMap;
use infinisvm_logger::{error, info};
use infinisvm_types::sync::{CommitBatchNotification, ShredId, SyncBatchShred, SyncFinalization};
use metrics::{counter, gauge};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::{
    service::{
        GetBatchShredRequest, GetBlockFinalizerRequest, InfiniSvmService, InfiniSvmServiceServer,
        SubscribeTransactionBatchRequest,
    },
    TransactionBatchBroadcaster,
};

#[derive(Clone)]
pub struct InfiniSVMServiceImpl {
    batch_broadcaster: Arc<TransactionBatchBroadcaster>,
    recent_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
    recent_finalizations: Arc<DashMap<u64, SyncFinalization>>,
}

impl InfiniSVMServiceImpl {
    pub async fn new(batch_broadcaster: Arc<TransactionBatchBroadcaster>) -> Self {
        let recent_batches = Arc::new(DashMap::new());
        let recent_finalizations = Arc::new(DashMap::new());

        // Build a small cache of recent batches keyed by (slot, job_id)
        {
            let recent_batches_clone = recent_batches.clone();
            let recent_finalizations_clone = recent_finalizations.clone();
            let mut internal_batch_rx = batch_broadcaster.subscribe();
            tokio::spawn(async move {
                loop {
                    match internal_batch_rx.recv().await {
                        Ok(batch_notification) => {
                            match batch_notification.as_ref() {
                                CommitBatchNotification::Batch(batch_data) => {
                                    info!("Adding batch to cache: shred_id={:?}", batch_data.shred_id);
                                    recent_batches_clone.insert(batch_data.shred_id.clone(), batch_data.clone());
                                    gauge!("grpc_cache_size").set(recent_batches_clone.len() as f64);
                                    if recent_batches_clone.len() > 10000 {
                                        let mut removed = 0usize;
                                        let last_slot = batch_data.shred_id.slot;

                                        // remove slots older than 1000
                                        let old_entries: Vec<_> = recent_batches_clone
                                            .iter()
                                            .filter(|entry| entry.key().slot < last_slot.saturating_sub(1000))
                                            .map(|entry| entry.key().clone())
                                            .collect();

                                        for shred_id in old_entries {
                                            info!("Evicting batch from cache: shred_id={:?}", shred_id);
                                            recent_batches_clone.remove(&shred_id);
                                            removed += 1;
                                        }

                                        info!("Pruned recent_batches cache by {} entries", removed);
                                        counter!("grpc_cache_evictions_total").increment(removed as u64);
                                        gauge!("grpc_cache_size").set(recent_batches_clone.len() as f64);
                                    }
                                }
                                CommitBatchNotification::Finalization(finalization) => {
                                    info!("Adding finalization to cache: slot={}", finalization.slot);
                                    recent_finalizations_clone.insert(finalization.slot, finalization.clone());
                                    gauge!("grpc_finalizer_cache_size").set(recent_finalizations_clone.len() as f64);
                                    if recent_finalizations_clone.len() > 10_000 {
                                        let last_slot = finalization.slot;
                                        let old_entries: Vec<_> = recent_finalizations_clone
                                            .iter()
                                            .filter(|entry| *entry.key() < last_slot.saturating_sub(1000))
                                            .map(|entry| *entry.key())
                                            .collect();

                                        let mut removed = 0usize;
                                        for slot in old_entries {
                                            info!("Evicting finalization from cache: slot={}", slot);
                                            recent_finalizations_clone.remove(&slot);
                                            removed += 1;
                                        }
                                        info!("Pruned finalization cache by {} entries", removed);
                                        counter!("grpc_finalizer_cache_evictions_total").increment(removed as u64);
                                        gauge!("grpc_finalizer_cache_size")
                                            .set(recent_finalizations_clone.len() as f64);
                                    }
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            counter!("grpc_cache_builder_lagged_total").increment(n);
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            error!("Internal batch receiver channel closed for cache builder");
                            break;
                        }
                    }
                }
            });
        }

        Self {
            batch_broadcaster,
            recent_batches,
            recent_finalizations,
        }
    }

    pub fn get_batch_broadcaster(&self) -> Arc<TransactionBatchBroadcaster> {
        self.batch_broadcaster.clone()
    }

    pub fn into_service(self) -> InfiniSvmServiceServer<Self> {
        InfiniSvmServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl InfiniSvmService for InfiniSVMServiceImpl {
    type SubscribeTransactionBatchesStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<Arc<CommitBatchNotification>, Status>> + Send>>;

    async fn subscribe_commit_batch_notifications(
        &self,
        _request: Request<SubscribeTransactionBatchRequest>,
    ) -> Result<Response<Self::SubscribeTransactionBatchesStream>, Status> {
        let (tx, rx) = mpsc::channel(512);
        let mut batch_receiver = self.batch_broadcaster.subscribe();

        // Observability: track active streams and openings
        gauge!("grpc_server_active_streams", "stream" => "batches").increment(1.0);
        counter!("grpc_server_streams_opened_total", "stream" => "batches").increment(1);

        // Spawn a task to send batch notifications
        tokio::spawn(async move {
            loop {
                match batch_receiver.recv().await {
                    Ok(batch_notification) => {
                        if tx.send(Ok(batch_notification.clone())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        metrics::counter!("grpc_batch_stream_lagged_total").increment(n);
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
                counter!("grpc_server_messages_sent_total", "stream" => "batches").increment(1);
            }

            // Stream ended
            gauge!("grpc_server_active_streams", "stream" => "batches").decrement(1.0);
            counter!("grpc_server_streams_closed_total", "stream" => "batches").increment(1);
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_batch_shred(
        &self,
        request: Request<GetBatchShredRequest>,
    ) -> Result<Response<SyncBatchShred>, Status> {
        let req = request.into_inner();
        if let Some(value) = self.recent_batches.get(&req.shred_id) {
            return Ok(Response::new(value.clone()));
        }
        // Log cache miss to correlate follower refetch NotFound
        info!("get_transaction_batch cache miss: shred_id={:?}", req.shred_id);
        counter!("grpc_cache_misses_total").increment(1);
        Err(Status::not_found(format!(
            "batch not found for shred_id={:?}",
            req.shred_id
        )))
    }

    async fn get_block_finalizer(
        &self,
        request: Request<GetBlockFinalizerRequest>,
    ) -> Result<Response<SyncFinalization>, Status> {
        let req = request.into_inner();
        if let Some(value) = self.recent_finalizations.get(&req.slot) {
            return Ok(Response::new(value.clone()));
        }

        info!("get_block_finalizer cache miss: slot={}", req.slot);
        counter!("grpc_finalizer_cache_misses_total").increment(1);
        Err(Status::not_found(format!(
            "finalization not found for slot={}",
            req.slot
        )))
    }
}
