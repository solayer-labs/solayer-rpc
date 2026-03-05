use std::{
    cmp::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use infinisvm_logger::warn;
use infinisvm_sync::grpc::client::SyncClient;
use infinisvm_types::sync::{PeerStatus, Setup, SignedFinalization};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio::sync::Mutex;

use super::score::{score_refetch, score_stream};

const DEFAULT_STREAM_WEIGHT: f64 = 1.0;
const DEFAULT_REFETCH_WEIGHT: f64 = 1.0;
const STREAM_STALL_SECS: u64 = 10;
const STREAM_MAX_LAG: u64 = 8;

#[derive(Clone)]
pub struct PeerHandle {
    pub node_id: [u8; 32],
    pub grpc_addr: String,
    pub stream_client: Arc<Mutex<SyncClient>>,
    pub rpc_client: Arc<Mutex<SyncClient>>,
}

#[derive(Clone, Default)]
pub struct PeerObservedLimits {
    pub effective_ratelimit_per_ip: u64,
    pub effective_max_streams_per_ip: u64,
    pub last_rate_limit_ts: Option<Instant>,
    pub last_over_bytes_ts: Option<Instant>,
    pub last_stream_drop_ts: Option<Instant>,
    pub last_max_streams_ts: Option<Instant>,
    pub bytes_served: u64,
}

#[derive(Clone)]
pub struct PeerState {
    pub node_id: [u8; 32],
    pub grpc_addr: String,
    pub stream_client: Arc<Mutex<SyncClient>>,
    pub rpc_client: Arc<Mutex<SyncClient>>,
    pub latest_signed_slot: u64,
    pub has_signed_bf: bool,
    pub has_grpc_stream: bool,
    pub last_rate_limit: Option<Instant>,
    pub last_inexist: Option<Instant>,
    pub last_over_bytes: Option<Instant>,
    pub last_stream_drop: Option<Instant>,
    pub last_max_streams: Option<Instant>,
    pub bandwidth_score: f64,
    pub reliability: f64,
    pub last_progress: Instant,
    pub last_bytes_reset: Instant,
    pub observed_limits: PeerObservedLimits,
    pub advertised_setup: Option<Setup>,
}

pub struct PeerManager {
    peers: DashMap<[u8; 32], PeerState>,
    stream_weight: f64,
    refetch_weight: f64,
    sequencer_pubkey: Pubkey,
}

impl PeerManager {
    pub fn new(sequencer_pubkey: Pubkey) -> Self {
        Self {
            peers: DashMap::new(),
            stream_weight: DEFAULT_STREAM_WEIGHT,
            refetch_weight: DEFAULT_REFETCH_WEIGHT,
            sequencer_pubkey,
        }
    }

    pub fn upsert_peer(
        &self,
        node_id: [u8; 32],
        grpc_addr: String,
        stream_client: Arc<Mutex<SyncClient>>,
        rpc_client: Arc<Mutex<SyncClient>>,
        status: Option<PeerStatus>,
    ) {
        let mut latest_signed_slot = 0;
        let mut has_signed_bf = false;
        let mut advertised_setup = None;
        let mut observed_limits = PeerObservedLimits::default();
        if let Some(status) = status {
            advertised_setup = status.setup;
            if let Some(sf) = status.latest_signed_finalization {
                if self.verify_signed_finalization(&sf) {
                    latest_signed_slot = sf.finalization.slot;
                    has_signed_bf = true;
                } else {
                    warn!("Rejecting peer {node_id:?} due to invalid signed finalization in status");
                    self.evict_peer(node_id, "invalid signed finalization in status");
                    return;
                }
            }
        }
        if let Some(setup) = advertised_setup.as_ref() {
            observed_limits.effective_ratelimit_per_ip = setup.ratelimit_per_ip;
            observed_limits.effective_max_streams_per_ip = setup.max_stream_per_ip;
        }

        self.peers.insert(
            node_id,
            PeerState {
                node_id,
                grpc_addr,
                stream_client,
                rpc_client,
                latest_signed_slot,
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
                observed_limits,
                advertised_setup,
            },
        );
    }

    pub fn update_peer_status(&self, node_id: [u8; 32], status: PeerStatus) {
        let mut invalid_signed_finalization = false;
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.advertised_setup = status.setup;
            self.refresh_effective_limits(&mut entry);
            entry.has_grpc_stream = true;
            if let Some(sf) = status.latest_signed_finalization {
                if self.verify_signed_finalization(&sf) {
                    entry.latest_signed_slot = entry.latest_signed_slot.max(sf.finalization.slot);
                    entry.has_signed_bf = true;
                    entry.last_progress = Instant::now();
                } else {
                    invalid_signed_finalization = true;
                }
            }
        }
        if invalid_signed_finalization {
            warn!("Penalizing peer {node_id:?} due to invalid signed finalization in status");
            self.penalize_invalid_finalizer(node_id);
        }
    }

    pub fn observe_signed_finalization(&self, node_id: [u8; 32], slot: u64) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.latest_signed_slot = entry.latest_signed_slot.max(slot);
            entry.has_signed_bf = true;
            entry.last_progress = Instant::now();
            entry.reliability = (entry.reliability + 0.01).min(1.0);
        }
    }

    pub fn mark_stream_ready(&self, node_id: [u8; 32]) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.has_grpc_stream = true;
            entry.last_progress = Instant::now();
            entry.reliability = (entry.reliability + 0.02).min(1.0);
            entry.bandwidth_score = (entry.bandwidth_score + 0.02).min(1.0);
        }
    }

    pub fn mark_stream_drop(&self, node_id: [u8; 32]) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.has_grpc_stream = false;
            entry.last_stream_drop = Some(Instant::now());
            entry.observed_limits.last_stream_drop_ts = Some(Instant::now());
            entry.reliability = (entry.reliability * 0.9).max(0.01);
        }
    }

    pub fn mark_max_streams(&self, node_id: [u8; 32]) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.last_max_streams = Some(Instant::now());
            entry.observed_limits.last_max_streams_ts = Some(Instant::now());
            entry.reliability = (entry.reliability * 0.9).max(0.01);
            if let Some(setup) = entry.advertised_setup.as_ref() {
                if setup.max_stream_per_ip > 0 {
                    let current = entry.observed_limits.effective_max_streams_per_ip;
                    let base = if current > 0 { current } else { setup.max_stream_per_ip };
                    entry.observed_limits.effective_max_streams_per_ip = (base / 2).max(1);
                }
            }
        }
    }

    pub fn mark_rate_limit(&self, node_id: [u8; 32]) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.last_rate_limit = Some(Instant::now());
            entry.observed_limits.last_rate_limit_ts = Some(Instant::now());
            entry.reliability = (entry.reliability * 0.95).max(0.01);
            entry.bandwidth_score = (entry.bandwidth_score * 0.9).max(0.01);
            if let Some(setup) = entry.advertised_setup.as_ref() {
                if setup.ratelimit_per_ip > 0 {
                    let current = entry.observed_limits.effective_ratelimit_per_ip;
                    let base = if current > 0 { current } else { setup.ratelimit_per_ip };
                    entry.observed_limits.effective_ratelimit_per_ip = (base / 2).max(1);
                }
            }
        }
    }

    pub fn mark_inexist(&self, node_id: [u8; 32]) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.last_inexist = Some(Instant::now());
            entry.reliability = (entry.reliability * 0.9).max(0.01);
            entry.bandwidth_score = (entry.bandwidth_score * 0.9).max(0.01);
        }
    }

    pub fn mark_over_bytes(&self, node_id: [u8; 32]) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.last_over_bytes = Some(Instant::now());
            entry.observed_limits.last_over_bytes_ts = Some(Instant::now());
            entry.reliability = (entry.reliability * 0.9).max(0.01);
            entry.bandwidth_score = (entry.bandwidth_score * 0.9).max(0.01);
            if let Some(setup) = entry.advertised_setup.as_ref() {
                if setup.ratelimit_per_ip > 0 {
                    let current = entry.observed_limits.effective_ratelimit_per_ip;
                    let base = if current > 0 { current } else { setup.ratelimit_per_ip };
                    entry.observed_limits.effective_ratelimit_per_ip = (base / 2).max(1);
                }
            }
        }
    }

    pub fn mark_failure(&self, node_id: [u8; 32]) {
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            entry.reliability = (entry.reliability * 0.9).max(0.01);
            entry.bandwidth_score = (entry.bandwidth_score * 0.95).max(0.01);
        }
    }

    pub fn penalize_invalid_finalizer(&self, node_id: [u8; 32]) {
        self.evict_peer(node_id, "invalid signed finalizer");
    }

    pub fn penalize_invalid_shred(&self, node_id: [u8; 32]) {
        self.evict_peer(node_id, "invalid shred");
    }

    pub fn current_head(&self) -> u64 {
        self.peers
            .iter()
            .map(|entry| entry.latest_signed_slot)
            .max()
            .unwrap_or(0)
    }

    pub fn pick_stream_peer(&self) -> Option<PeerHandle> {
        let head = self.current_head();
        let mut best: Option<(f64, PeerState)> = None;
        for entry in self.peers.iter() {
            let score = score_stream(entry.value(), head, self.stream_weight);
            if best.as_ref().map(|(best_score, _)| score > *best_score).unwrap_or(true) {
                best = Some((score, entry.value().clone()));
            }
        }

        if let Some((score, peer)) = best {
            if score <= 0.0 {
                return None;
            }
            return Some(PeerHandle {
                node_id: peer.node_id,
                grpc_addr: peer.grpc_addr,
                stream_client: peer.stream_client,
                rpc_client: peer.rpc_client,
            });
        }
        None
    }

    pub fn pick_refetch_peers(&self) -> Vec<PeerHandle> {
        let head = self.current_head();
        let mut peers: Vec<(f64, PeerState)> = self
            .peers
            .iter()
            .map(|entry| {
                let score = score_refetch(entry.value(), head, self.refetch_weight);
                (score, entry.value().clone())
            })
            .collect();

        peers.sort_by(|(a, _), (b, _)| b.partial_cmp(a).unwrap_or(Ordering::Equal));

        let selected: Vec<PeerHandle> = peers
            .iter()
            .filter(|(score, _)| *score > 0.0)
            .map(|(_, peer)| PeerHandle {
                node_id: peer.node_id,
                grpc_addr: peer.grpc_addr.clone(),
                stream_client: peer.stream_client.clone(),
                rpc_client: peer.rpc_client.clone(),
            })
            .collect();
        selected
    }

    pub fn peer_handles(&self) -> Vec<PeerHandle> {
        self.peers
            .iter()
            .map(|entry| PeerHandle {
                node_id: entry.node_id,
                grpc_addr: entry.grpc_addr.clone(),
                stream_client: entry.stream_client.clone(),
                rpc_client: entry.rpc_client.clone(),
            })
            .collect()
    }

    pub fn has_peer(&self, node_id: [u8; 32]) -> bool {
        self.peers.contains_key(&node_id)
    }

    pub fn stream_should_failover(&self, node_id: [u8; 32]) -> bool {
        let head = self.current_head();
        if let Some(entry) = self.peers.get(&node_id) {
            let lag = head.saturating_sub(entry.latest_signed_slot);
            if lag >= STREAM_MAX_LAG {
                return true;
            }
            if entry.last_progress.elapsed() > Duration::from_secs(STREAM_STALL_SECS) {
                return true;
            }
            return false;
        }
        warn!("stream_should_failover called for unknown peer");
        false
    }

    pub fn observe_bytes(&self, node_id: [u8; 32], bytes: u64) {
        let mut should_mark_over_bytes = false;
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            let (reset_secs, max_bytes) = match entry.advertised_setup.as_ref() {
                Some(setup) => (
                    setup.max_bytes_until_functional_rpc_reset,
                    setup.max_bytes_until_functional_rpc,
                ),
                None => return,
            };
            if reset_secs > 0 && entry.last_bytes_reset.elapsed().as_secs() >= reset_secs {
                entry.observed_limits.bytes_served = 0;
                entry.last_bytes_reset = Instant::now();
            }
            entry.observed_limits.bytes_served = entry.observed_limits.bytes_served.saturating_add(bytes);
            if max_bytes > 0 && entry.observed_limits.bytes_served > max_bytes {
                should_mark_over_bytes = true;
            }
        }
        if should_mark_over_bytes {
            self.mark_over_bytes(node_id);
        }
    }

    fn refresh_effective_limits(&self, entry: &mut PeerState) {
        if let Some(setup) = entry.advertised_setup.as_ref() {
            if entry.observed_limits.effective_ratelimit_per_ip == 0 {
                entry.observed_limits.effective_ratelimit_per_ip = setup.ratelimit_per_ip;
            } else if setup.ratelimit_per_ip > 0 {
                entry.observed_limits.effective_ratelimit_per_ip = entry
                    .observed_limits
                    .effective_ratelimit_per_ip
                    .min(setup.ratelimit_per_ip);
            }
            if entry.observed_limits.effective_max_streams_per_ip == 0 {
                entry.observed_limits.effective_max_streams_per_ip = setup.max_stream_per_ip;
            } else if setup.max_stream_per_ip > 0 {
                entry.observed_limits.effective_max_streams_per_ip = entry
                    .observed_limits
                    .effective_max_streams_per_ip
                    .min(setup.max_stream_per_ip);
            }
        }
    }

    fn verify_signed_finalization(&self, sf: &SignedFinalization) -> bool {
        if sf.sequencer_pubkey != self.sequencer_pubkey.to_bytes() {
            return false;
        }
        let msg = match bincode::serialize(&sf.finalization) {
            Ok(m) => m,
            Err(_) => return false,
        };
        let sig = Signature::from(sf.signature);
        sig.verify(self.sequencer_pubkey.as_ref(), &msg)
    }

    fn evict_peer(&self, node_id: [u8; 32], reason: &str) {
        let was_present = self.peers.remove(&node_id).is_some();
        if was_present {
            warn!("Evicting peer {node_id:?}: {reason}");
        } else {
            warn!("Requesting eviction for unknown peer {node_id:?}: {reason}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, pin::Pin, sync::Arc};

    use infinisvm_sync::grpc::{
        client::SyncClient,
        service::{
            GetBatchShredRequest, GetBlockFinalizerRequest, InfiniSvmService, InfiniSvmServiceServer,
            InjectCommitBatchRequest, InjectCommitBatchResponse, SubscribeTransactionBatchRequest,
        },
    };
    use infinisvm_types::sync::{
        CommitBatchNotification, GetPeerStatusRequest, GetPeerStatusResponse, PeerStatus, Setup, SignedFinalization,
        SyncBatchShred,
    };
    use solana_sdk::{hash::hashv, pubkey::Pubkey};
    use tokio::{net::TcpListener, sync::Mutex};
    use tokio_stream::{wrappers::TcpListenerStream, Stream};
    use tonic::{Response, Status};

    use super::{PeerManager, PeerObservedLimits, PeerState};

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

    async fn insert_peer(manager: &PeerManager) -> [u8; 32] {
        let addr = spawn_stub().await;
        let url = format!("http://{addr}");
        let client = Arc::new(Mutex::new(SyncClient::connect(&url).await.expect("client")));
        let node_id = hashv(&[addr.to_string().as_bytes()]).to_bytes();
        manager.peers.insert(
            node_id,
            PeerState {
                node_id,
                grpc_addr: addr.to_string(),
                stream_client: client.clone(),
                rpc_client: client,
                latest_signed_slot: 100,
                has_signed_bf: true,
                has_grpc_stream: true,
                last_rate_limit: None,
                last_inexist: None,
                last_over_bytes: None,
                last_stream_drop: None,
                last_max_streams: None,
                bandwidth_score: 1.0,
                reliability: 1.0,
                last_progress: std::time::Instant::now(),
                last_bytes_reset: std::time::Instant::now(),
                observed_limits: PeerObservedLimits::default(),
                advertised_setup: None,
            },
        );
        node_id
    }

    #[tokio::test]
    async fn penalize_invalid_shred_clears_signed_state() {
        let manager = PeerManager::new(Pubkey::new_unique());
        let node_id = insert_peer(&manager).await;

        manager.penalize_invalid_shred(node_id);

        assert!(!manager.has_peer(node_id));
    }

    #[tokio::test]
    async fn penalize_invalid_finalizer_clears_signed_state() {
        let manager = PeerManager::new(Pubkey::new_unique());
        let node_id = insert_peer(&manager).await;

        manager.penalize_invalid_finalizer(node_id);

        assert!(!manager.has_peer(node_id));
    }

    #[tokio::test]
    async fn observe_bytes_marks_over_bytes() {
        let manager = PeerManager::new(Pubkey::new_unique());
        let node_id = insert_peer(&manager).await;
        {
            let mut entry = manager.peers.get_mut(&node_id).expect("peer");
            entry.advertised_setup = Some(Setup {
                ratelimit_per_ip: 100,
                max_stream_per_ip: 4,
                ratelimit_per_functional_rpc: 0,
                max_bytes_until_functional_rpc: 8,
                max_bytes_until_functional_rpc_reset: 10,
                ip_whitelist: vec![],
                pubkey_whitelist: vec![],
            });
        }

        manager.observe_bytes(node_id, 16);

        let entry = manager.peers.get(&node_id).expect("peer");
        assert!(entry.last_over_bytes.is_some());
        assert!(entry.observed_limits.last_over_bytes_ts.is_some());
        assert!(entry.observed_limits.effective_ratelimit_per_ip > 0);
    }

    #[tokio::test]
    async fn mark_rate_limit_reduces_effective_limit() {
        let manager = PeerManager::new(Pubkey::new_unique());
        let node_id = insert_peer(&manager).await;
        {
            let mut entry = manager.peers.get_mut(&node_id).expect("peer");
            entry.advertised_setup = Some(Setup {
                ratelimit_per_ip: 100,
                max_stream_per_ip: 4,
                ratelimit_per_functional_rpc: 0,
                max_bytes_until_functional_rpc: 0,
                max_bytes_until_functional_rpc_reset: 0,
                ip_whitelist: vec![],
                pubkey_whitelist: vec![],
            });
            entry.observed_limits.effective_ratelimit_per_ip = 100;
        }

        manager.mark_rate_limit(node_id);

        let entry = manager.peers.get(&node_id).expect("peer");
        assert!(entry.last_rate_limit.is_some());
        assert!(entry.observed_limits.last_rate_limit_ts.is_some());
        assert!(entry.observed_limits.effective_ratelimit_per_ip < 100);
    }
}
