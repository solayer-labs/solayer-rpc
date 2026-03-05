use std::time::{Duration, Instant};

use super::PeerState;

fn recent(ts: Option<Instant>, window: Duration) -> bool {
    ts.map(|t| t.elapsed() < window).unwrap_or(false)
}

pub fn score_stream(peer: &PeerState, head: u64, propagator_weight: f64) -> f64 {
    if !peer.has_signed_bf {
        return 0.0;
    }
    if !peer.has_grpc_stream {
        return 0.0;
    }
    let lag = head.saturating_sub(peer.latest_signed_slot).max(1) as f64;
    let mut rate_penalty = if recent(peer.last_rate_limit, Duration::from_secs(30)) ||
        recent(peer.observed_limits.last_rate_limit_ts, Duration::from_secs(30)) ||
        recent(peer.last_stream_drop, Duration::from_secs(30)) ||
        recent(peer.observed_limits.last_stream_drop_ts, Duration::from_secs(30)) ||
        recent(peer.last_max_streams, Duration::from_secs(30)) ||
        recent(peer.observed_limits.last_max_streams_ts, Duration::from_secs(30))
    {
        1e-9
    } else {
        1.0
    };
    if let Some(setup) = peer.advertised_setup.as_ref() {
        if setup.ratelimit_per_ip > 0 && peer.observed_limits.effective_ratelimit_per_ip > 0 {
            let ratio = peer.observed_limits.effective_ratelimit_per_ip as f64 / setup.ratelimit_per_ip as f64;
            rate_penalty *= ratio.clamp(0.0, 1.0);
        }
        if setup.max_stream_per_ip > 0 && peer.observed_limits.effective_max_streams_per_ip > 0 {
            let ratio = peer.observed_limits.effective_max_streams_per_ip as f64 / setup.max_stream_per_ip as f64;
            rate_penalty *= ratio.clamp(0.0, 1.0);
        }
    }
    propagator_weight * (1.0 / lag) * rate_penalty * peer.reliability * peer.bandwidth_score
}

pub fn score_refetch(peer: &PeerState, head: u64, refetch_weight: f64) -> f64 {
    if !peer.has_signed_bf {
        return 0.0;
    }
    let lag = head.saturating_sub(peer.latest_signed_slot).max(1) as f64;
    let mut rate_penalty = if recent(peer.last_rate_limit, Duration::from_secs(30)) ||
        recent(peer.observed_limits.last_rate_limit_ts, Duration::from_secs(30))
    {
        1e-9
    } else {
        1.0
    };
    let inexist_penalty = if recent(peer.last_inexist, Duration::from_secs(60)) ||
        recent(peer.last_over_bytes, Duration::from_secs(60)) ||
        recent(peer.observed_limits.last_over_bytes_ts, Duration::from_secs(60))
    {
        1e-6
    } else {
        1.0
    };
    if let Some(setup) = peer.advertised_setup.as_ref() {
        if setup.ratelimit_per_ip > 0 && peer.observed_limits.effective_ratelimit_per_ip > 0 {
            let ratio = peer.observed_limits.effective_ratelimit_per_ip as f64 / setup.ratelimit_per_ip as f64;
            rate_penalty *= ratio.clamp(0.0, 1.0);
        }
    }
    refetch_weight * (1.0 / lag) * rate_penalty * inexist_penalty * peer.reliability
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

    use infinisvm_sync::grpc::{
        service::{
            GetBatchShredRequest, GetBlockFinalizerRequest, InfiniSvmService, InfiniSvmServiceServer,
            InjectCommitBatchRequest, InjectCommitBatchResponse, SubscribeTransactionBatchRequest,
        },
        SyncClient,
    };
    use infinisvm_types::sync::{
        CommitBatchNotification, GetPeerStatusRequest, GetPeerStatusResponse, PeerStatus, Setup, SignedFinalization,
        SyncBatchShred,
    };
    use solana_sdk::hash::hashv;
    use tokio::{net::TcpListener, sync::Mutex};
    use tokio_stream::{wrappers::TcpListenerStream, Stream};
    use tonic::{Response, Status};

    use super::{score_refetch, score_stream};
    use crate::p2p::{peer_manager::PeerObservedLimits, PeerState};

    #[derive(Clone)]
    struct StubService;

    #[tonic::async_trait]
    impl InfiniSvmService for StubService {
        type SubscribeTransactionBatchesStream =
            Pin<Box<dyn Stream<Item = Result<Arc<CommitBatchNotification>, Status>> + Send + 'static>>;

        async fn subscribe_commit_batch_notifications(
            &self,
            _request: tonic::Request<SubscribeTransactionBatchRequest>,
        ) -> Result<Response<Self::SubscribeTransactionBatchesStream>, Status> {
            Ok(Response::new(Box::pin(tokio_stream::empty())))
        }

        async fn get_batch_shred(
            &self,
            _request: tonic::Request<GetBatchShredRequest>,
        ) -> Result<Response<SyncBatchShred>, Status> {
            Err(Status::not_found("not found"))
        }

        async fn get_block_finalizer(
            &self,
            _request: tonic::Request<GetBlockFinalizerRequest>,
        ) -> Result<Response<SignedFinalization>, Status> {
            Err(Status::not_found("not found"))
        }

        async fn get_peer_status(
            &self,
            _request: tonic::Request<GetPeerStatusRequest>,
        ) -> Result<Response<GetPeerStatusResponse>, Status> {
            let grpc_addr = "127.0.0.1:0".to_string();
            let status = PeerStatus {
                node_id: hashv(&[grpc_addr.as_bytes()]).to_bytes(),
                grpc_addr,
                rate_limit_per_sec: 0,
                rate_limit_burst: 0,
                latest_signed_finalization: None,
                observed_head: 0,
                capabilities: 0,
                setup: None,
            };
            Ok(Response::new(GetPeerStatusResponse { status }))
        }

        async fn inject_commit_batch_notification(
            &self,
            _request: tonic::Request<InjectCommitBatchRequest>,
        ) -> Result<Response<InjectCommitBatchResponse>, Status> {
            Ok(Response::new(InjectCommitBatchResponse { ok: true }))
        }
    }

    async fn spawn_stub() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let service = InfiniSvmServiceServer::new(StubService);
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve stub");
        });
        addr
    }

    async fn make_peer_state(has_signed_bf: bool) -> PeerState {
        let addr = spawn_stub().await;
        let url = format!("http://{addr}");
        let stream_client = Arc::new(Mutex::new(SyncClient::connect(&url).await.expect("client")));
        let rpc_client = Arc::new(Mutex::new(SyncClient::connect(&url).await.expect("client")));
        PeerState {
            node_id: [0u8; 32],
            grpc_addr: addr.to_string(),
            stream_client,
            rpc_client,
            latest_signed_slot: 100,
            has_signed_bf,
            has_grpc_stream: true,
            last_rate_limit: None,
            last_inexist: None,
            last_over_bytes: None,
            last_stream_drop: None,
            last_max_streams: None,
            bandwidth_score: 1.0,
            reliability: 1.0,
            last_progress: Instant::now(),
            last_bytes_reset: Instant::now(),
            observed_limits: PeerObservedLimits::default(),
            advertised_setup: None,
        }
    }

    #[tokio::test]
    async fn score_stream_requires_signed_bf() {
        let peer = make_peer_state(false).await;
        let score = score_stream(&peer, 100, 1.0);
        assert_eq!(score, 0.0);
    }

    #[tokio::test]
    async fn score_stream_applies_rate_ratio() {
        let mut peer = make_peer_state(true).await;
        peer.advertised_setup = Some(Setup {
            ratelimit_per_ip: 100,
            max_stream_per_ip: 10,
            ratelimit_per_functional_rpc: 0,
            max_bytes_until_functional_rpc: 0,
            max_bytes_until_functional_rpc_reset: 0,
            ip_whitelist: vec![],
            pubkey_whitelist: vec![],
        });
        peer.observed_limits.effective_ratelimit_per_ip = 10;
        peer.observed_limits.effective_max_streams_per_ip = 10;
        let score = score_stream(&peer, 100, 1.0);
        assert!((score - 0.1).abs() < 1e-6, "score={score}");
    }

    #[tokio::test]
    async fn score_stream_penalizes_recent_rate_limit() {
        let mut peer = make_peer_state(true).await;
        peer.last_rate_limit = Some(Instant::now());
        let score = score_stream(&peer, 100, 1.0);
        assert!(score < 1e-6, "score={score}");
    }

    #[tokio::test]
    async fn score_refetch_penalizes_over_bytes() {
        let mut peer = make_peer_state(true).await;
        peer.last_over_bytes = Some(Instant::now());
        let score = score_refetch(&peer, 100, 1.0);
        assert!(score < 1e-5, "score={score}");
    }

    #[tokio::test]
    async fn score_scales_with_reliability() {
        let mut peer = make_peer_state(true).await;
        peer.reliability = 0.5;
        let score = score_stream(&peer, 100, 1.0);
        assert!((score - 0.5).abs() < 1e-6, "score={score}");
    }
}
