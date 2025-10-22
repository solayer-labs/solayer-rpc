use std::{pin::Pin, sync::Arc, time::Instant};

use hashbrown::HashMap;
use infinisvm_logger::{error, info};
use infinisvm_types::sync::{
    grpc::{infini_svm_service_server::InfiniSvmService, InfiniSvmServiceServer},
    CommitBatchNotification, GetLatestSlotRequest, GetLatestSlotResponse, GetTransactionBatchRequest, SlotDataResponse,
    StartReceivingSlotsRequest, TransactionBatchRequest,
};
use metrics::{counter, gauge, histogram};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{grpc::TransactionBatchBroadcaster, state::SyncState};

#[derive(Clone)]
pub struct InfiniSVMServiceImpl {
    sync_state: Arc<RwLock<SyncState>>,
    slot_broadcast_sender: broadcast::Sender<Arc<SlotDataResponse>>,
    batch_broadcaster: Arc<TransactionBatchBroadcaster>,
    recent_batches: Arc<RwLock<HashMap<(u64, u64), (Arc<CommitBatchNotification>, Instant)>>>,
}

impl InfiniSVMServiceImpl {
    pub async fn new(
        sync_state: Arc<RwLock<SyncState>>,
        latest_slot_receiver: crossbeam_channel::Receiver<Arc<SlotDataResponse>>,
        batch_broadcaster: Arc<TransactionBatchBroadcaster>,
    ) -> Self {
        let (slot_broadcast_sender, mut slot_broadcast_receiver) = broadcast::channel::<Arc<SlotDataResponse>>(128);

        tokio::spawn({
            async move {
                while slot_broadcast_receiver.recv().await.is_ok() {}
                error!("Slot broadcast receiver channel closed");
            }
        });

        tokio::task::spawn_blocking({
            let slot_broadcast_sender = slot_broadcast_sender.clone();

            move || {
                while let Ok(slot_data) = latest_slot_receiver.recv() {
                    slot_broadcast_sender.send(slot_data).unwrap();
                }
            }
        });

        let recent_batches: Arc<RwLock<HashMap<(u64, u64), (Arc<CommitBatchNotification>, Instant)>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Build a small cache of recent batches keyed by (slot, job_id)
        {
            let recent_batches_clone = recent_batches.clone();
            let mut internal_batch_rx = batch_broadcaster.subscribe();
            tokio::spawn(async move {
                loop {
                    match internal_batch_rx.recv().await {
                        Ok(batch_notification) => {
                            let key = (batch_notification.slot, batch_notification.job_id);
                            info!(
                                "Adding batch to cache: slot={}, job_id={}",
                                batch_notification.slot, batch_notification.job_id
                            );
                            if batch_notification.slot == 0 || batch_notification.job_id == 0 {
                                info!(
                                    "Cached empty/placeholder batch (slot={}, job_id={}, size={})",
                                    batch_notification.slot, batch_notification.job_id, batch_notification.batch_size
                                );
                            }
                            let mut map = recent_batches_clone.write().await;
                            map.insert(key, (batch_notification.clone(), Instant::now()));
                            gauge!("grpc_cache_size").set(map.len() as f64);
                            if map.len() > 10000 {
                                let mut removed = 0usize;
                                let now = Instant::now();
                                let old_entries: Vec<_> = map
                                    .iter()
                                    .filter(|(_, (_, timestamp))| now.duration_since(*timestamp).as_secs() > 300)
                                    .map(|((slot, job_id), _)| (*slot, *job_id))
                                    .collect();

                                for (slot, job_id) in old_entries {
                                    info!("Evicting batch from cache: slot={}, job_id={}", slot, job_id);
                                    map.remove(&(slot, job_id));
                                    removed += 1;
                                }

                                info!("Pruned recent_batches cache by {} entries", removed);
                                counter!("grpc_cache_evictions_total").increment(removed as u64);
                                gauge!("grpc_cache_size").set(map.len() as f64);
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
            sync_state,
            slot_broadcast_sender,
            batch_broadcaster,
            recent_batches,
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
    type StartReceivingSlotsStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<SlotDataResponse, Status>> + Send>>;
    type SubscribeTransactionBatchesStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<CommitBatchNotification, Status>> + Send>>;

    async fn start_receiving_slots(
        &self,
        _request: Request<StartReceivingSlotsRequest>,
    ) -> Result<Response<Self::StartReceivingSlotsStream>, Status> {
        let (tx, rx) = mpsc::channel(512);
        let mut broadcast_receiver = self.slot_broadcast_sender.subscribe();

        // Observability: track active streams and openings
        gauge!("grpc_server_active_streams", "stream" => "slots").increment(1.0);
        counter!("grpc_server_streams_opened_total", "stream" => "slots").increment(1);

        // Spawn a task to send slot data
        tokio::spawn(async move {
            while let Ok(slot_data) = broadcast_receiver.recv().await {
                // Avoid cloning
                if tx.send(Ok((*slot_data).clone())).await.is_err() {
                    break;
                }
                counter!("grpc_server_messages_sent_total", "stream" => "slots").increment(1);
            }

            // Stream ended
            gauge!("grpc_server_active_streams", "stream" => "slots").decrement(1.0);
            counter!("grpc_server_streams_closed_total", "stream" => "slots").increment(1);
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_latest_slot(
        &self,
        _request: Request<GetLatestSlotRequest>,
    ) -> Result<Response<GetLatestSlotResponse>, Status> {
        let start = Instant::now();
        let (slot, hash, parent_blockhash, timestamp, job_ids) = self.sync_state.read().await.latest_slot.clone();
        let response = GetLatestSlotResponse {
            slot,
            hash,
            parent_blockhash,
            timestamp,
            job_ids,
        };
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        histogram!("grpc_server_unary_latency_ms", "method" => "GetLatestSlot").record(elapsed_ms);
        Ok(Response::new(response))
    }

    async fn subscribe_transaction_batches(
        &self,
        _request: Request<TransactionBatchRequest>,
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
                        if tx.send(Ok((*batch_notification).clone())).await.is_err() {
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

    async fn get_transaction_batch(
        &self,
        request: Request<GetTransactionBatchRequest>,
    ) -> Result<Response<CommitBatchNotification>, Status> {
        let req = request.into_inner();
        let key = (req.slot, req.job_id);
        if let Some((value, _)) = self.recent_batches.read().await.get(&key).cloned() {
            return Ok(Response::new((*value).clone()));
        }
        // Log cache miss to correlate follower refetch NotFound
        info!(
            "get_transaction_batch cache miss: slot={} job_id={}",
            req.slot, req.job_id
        );
        counter!("grpc_cache_misses_total").increment(1);
        Err(Status::not_found(format!(
            "batch not found for slot {} job_id {}",
            req.slot, req.job_id
        )))
    }
}
