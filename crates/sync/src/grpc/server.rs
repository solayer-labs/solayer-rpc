use std::{net::SocketAddr, pin::Pin, sync::Arc};

use dashmap::DashMap;
use infinisvm_logger::{error, info};
use infinisvm_types::sync::{
    CommitBatchNotification, GetPeerStatusRequest, GetPeerStatusResponse, PeerStatus, Setup, ShredId,
    SignedFinalization, SyncBatchShred,
};
use metrics::{counter, gauge};
use solana_sdk::hash::hashv;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::{
    service::{
        GetBatchShredRequest, GetBlockFinalizerRequest, InfiniSvmService, InfiniSvmServiceServer,
        InjectCommitBatchRequest, InjectCommitBatchResponse, SubscribeTransactionBatchRequest,
    },
    PeerNotification, TransactionBatchBroadcaster,
};

#[derive(Clone)]
pub struct InfiniSVMServiceImpl {
    batch_broadcaster: Arc<TransactionBatchBroadcaster>,
    recent_batches: Arc<DashMap<ShredId, SyncBatchShred>>,
    recent_signed_finalizations: Arc<DashMap<u64, SignedFinalization>>,
    peer_status: Arc<RwLock<PeerStatus>>,
    e2e_sender: Option<mpsc::Sender<PeerNotification>>,
}

impl InfiniSVMServiceImpl {
    pub async fn new(
        batch_broadcaster: Arc<TransactionBatchBroadcaster>,
        grpc_addr: SocketAddr,
        grpc_advertise_addr: Option<String>,
        e2e_sender: Option<mpsc::Sender<PeerNotification>>,
    ) -> Self {
        let recent_batches = Arc::new(DashMap::new());
        let recent_signed_finalizations: Arc<DashMap<u64, SignedFinalization>> = Arc::new(DashMap::new());
        let grpc_addr_string = grpc_advertise_addr
            .as_deref()
            .map(normalize_grpc_advertise_addr)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| grpc_addr.to_string());
        let node_id = hashv(&[grpc_addr_string.as_bytes()]).to_bytes();
        let setup = load_setup_from_env();
        let rate_limit_per_sec = setup.ratelimit_per_ip.min(u32::MAX as u64) as u32;
        let rate_limit_burst = setup.max_stream_per_ip.min(u32::MAX as u64) as u32;
        let peer_status = Arc::new(RwLock::new(PeerStatus {
            node_id,
            grpc_addr: grpc_addr_string,
            rate_limit_per_sec,
            rate_limit_burst,
            latest_signed_finalization: None,
            observed_head: 0,
            capabilities: 0,
            setup: Some(setup),
        }));

        // Build a small cache of recent batches keyed by (slot, job_id)
        {
            let recent_batches_clone = recent_batches.clone();
            let recent_signed_finalizations_clone = recent_signed_finalizations.clone();
            let peer_status_clone = peer_status.clone();
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
                                    info!("Received unsigned finalization: slot={}", finalization.slot);
                                    {
                                        let mut status = peer_status_clone.write().await;
                                        status.observed_head = status.observed_head.max(finalization.slot);
                                    }
                                }
                                CommitBatchNotification::SignedFinalization(signed) => {
                                    let finalization = &signed.finalization;
                                    info!("Adding signed finalization to cache: slot={}", finalization.slot);
                                    recent_signed_finalizations_clone.insert(finalization.slot, signed.clone());
                                    {
                                        let mut status = peer_status_clone.write().await;
                                        status.observed_head = status.observed_head.max(finalization.slot);
                                        status.latest_signed_finalization = Some(signed.clone());
                                    }
                                    if recent_signed_finalizations_clone.len() > 10_000 {
                                        let last_slot = finalization.slot;
                                        let old_entries: Vec<_> = recent_signed_finalizations_clone
                                            .iter()
                                            .filter(|entry| *entry.key() < last_slot.saturating_sub(1000))
                                            .map(|entry| *entry.key())
                                            .collect();

                                        let mut removed = 0usize;
                                        for slot in old_entries {
                                            recent_signed_finalizations_clone.remove(&slot);
                                            removed += 1;
                                        }
                                        counter!("grpc_finalizer_cache_evictions_total").increment(removed as u64);
                                    }

                                    gauge!("grpc_finalizer_cache_size")
                                        .set(recent_signed_finalizations_clone.len() as f64);
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
            recent_signed_finalizations,
            peer_status,
            e2e_sender,
        }
    }

    pub fn get_batch_broadcaster(&self) -> Arc<TransactionBatchBroadcaster> {
        self.batch_broadcaster.clone()
    }

    pub fn into_service(self) -> InfiniSvmServiceServer<Self> {
        InfiniSvmServiceServer::new(self)
    }
}

fn normalize_grpc_advertise_addr(addr: &str) -> String {
    addr.trim()
        .trim_end_matches('/')
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

fn load_setup_from_env() -> Setup {
    fn parse_u64(name: &str) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn parse_ip_list(name: &str) -> Vec<std::net::IpAddr> {
        std::env::var(name)
            .ok()
            .map(|v| {
                v.split(',')
                    .filter_map(|s| s.trim().parse::<std::net::IpAddr>().ok())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    Setup {
        ratelimit_per_ip: parse_u64("RPC_SETUP_RATELIMIT_PER_IP"),
        max_stream_per_ip: parse_u64("RPC_SETUP_MAX_STREAM_PER_IP"),
        ratelimit_per_functional_rpc: parse_u64("RPC_SETUP_RATELIMIT_PER_FUNCTIONAL_RPC"),
        max_bytes_until_functional_rpc: parse_u64("RPC_SETUP_MAX_BYTES_UNTIL_FUNCTIONAL_RPC"),
        max_bytes_until_functional_rpc_reset: parse_u64("RPC_SETUP_MAX_BYTES_UNTIL_FUNCTIONAL_RPC_RESET"),
        ip_whitelist: parse_ip_list("RPC_SETUP_IP_WHITELIST"),
        pubkey_whitelist: parse_ip_list("RPC_SETUP_PUBKEY_WHITELIST"),
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
        let (tx, rx) = mpsc::channel(4096);
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
    ) -> Result<Response<SignedFinalization>, Status> {
        let req = request.into_inner();
        if let Some(value) = self.recent_signed_finalizations.get(&req.slot) {
            return Ok(Response::new(value.clone()));
        }

        info!("get_block_finalizer cache miss: slot={}", req.slot);
        counter!("grpc_finalizer_cache_misses_total").increment(1);
        Err(Status::not_found(format!(
            "finalization not found for slot={}",
            req.slot
        )))
    }

    async fn get_peer_status(
        &self,
        _request: Request<GetPeerStatusRequest>,
    ) -> Result<Response<GetPeerStatusResponse>, Status> {
        let status = self.peer_status.read().await.clone();
        Ok(Response::new(GetPeerStatusResponse { status }))
    }

    async fn inject_commit_batch_notification(
        &self,
        request: Request<InjectCommitBatchRequest>,
    ) -> Result<Response<InjectCommitBatchResponse>, Status> {
        #[cfg(feature = "e2e")]
        {
            let Some(sender) = self.e2e_sender.as_ref() else {
                return Err(Status::unimplemented("e2e injection is disabled"));
            };
            let req = request.into_inner();
            let notification = Arc::new(req.notification);

            if let Err(e) = self.batch_broadcaster.publish_notification(notification.clone()) {
                return Err(Status::internal(format!("Failed to publish notification: {e}")));
            }

            let peer_notification = PeerNotification {
                peer_id: req.peer_id,
                peer_addr: req.peer_addr,
                notification,
            };

            sender
                .send(peer_notification)
                .await
                .map_err(|_| Status::internal("e2e injection channel closed"))?;

            Ok(Response::new(InjectCommitBatchResponse { ok: true }))
        }
        #[cfg(not(feature = "e2e"))]
        {
            let _ = request;
            Err(Status::unimplemented("e2e feature disabled"))
        }
    }
}
