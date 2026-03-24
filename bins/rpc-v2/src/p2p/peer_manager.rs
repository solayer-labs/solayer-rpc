use std::{
    cmp::Ordering,
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use infinisvm_logger::warn;
use infinisvm_sync::grpc::client::SyncClient;
use infinisvm_types::sync::{PeerStatus, Setup, SignedAncestryDelegation, SignedFinalization};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio::sync::Mutex;

use super::score::{score_refetch, score_stream};

const DEFAULT_STREAM_WEIGHT: f64 = 1.0;
const DEFAULT_REFETCH_WEIGHT: f64 = 1.0;
const STREAM_STALL_SECS: u64 = 10;
const STREAM_MAX_LAG: u64 = 8;
const MAX_CANARY_PATH_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatusValidation {
    Valid,
    NotReady(&'static str),
    Invalid(&'static str),
}

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
    pub advertised_stream_parent: Option<[u8; 32]>,
    pub topology_pubkey: [u8; 32],
    pub ancestry_canary: Option<SignedFinalization>,
    pub ancestry_delegations: Vec<SignedAncestryDelegation>,
}

pub struct PeerManager {
    peers: DashMap<[u8; 32], PeerState>,
    stream_weight: f64,
    refetch_weight: f64,
    self_node_id: [u8; 32],
    sequencer_pubkey: Pubkey,
}

impl PeerManager {
    pub fn new(self_node_id: [u8; 32], sequencer_pubkey: Pubkey) -> Self {
        Self {
            peers: DashMap::new(),
            stream_weight: DEFAULT_STREAM_WEIGHT,
            refetch_weight: DEFAULT_REFETCH_WEIGHT,
            self_node_id,
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
        let mut observed_limits = PeerObservedLimits::default();

        let Some(status) = status else {
            warn!("Skipping peer {node_id:?}: peer status/canary not ready yet");
            return;
        };

        match self.validate_peer_status(node_id, Some(&status)) {
            PeerStatusValidation::Valid => {}
            PeerStatusValidation::NotReady(reason) => {
                warn!("Skipping peer {node_id:?}: {reason}");
                return;
            }
            PeerStatusValidation::Invalid(reason) => {
                warn!("Rejecting peer {node_id:?} due to invalid canary in status: {reason}");
                self.evict_peer(node_id, reason);
                return;
            }
        }
        let advertised_setup = status.setup.clone();
        let advertised_stream_parent = status.stream_parent;
        let sf = status
            .latest_signed_finalization
            .as_ref()
            .expect("validated peer status must contain signed canary");
        let latest_signed_slot = sf.finalization.slot;
        let has_signed_bf = true;
        let topology_pubkey = status.topology_pubkey;
        let ancestry_canary = status.ancestry_canary.clone();
        let ancestry_delegations = status.ancestry_delegations.clone();

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
                advertised_stream_parent,
                topology_pubkey,
                ancestry_canary,
                ancestry_delegations,
            },
        );
    }

    pub fn update_peer_status(&self, node_id: [u8; 32], status: PeerStatus) {
        let mut evict_reason = None;
        if let Some(mut entry) = self.peers.get_mut(&node_id) {
            match self.validate_peer_status(node_id, Some(&status)) {
                PeerStatusValidation::Valid => {
                    entry.advertised_setup = status.setup.clone();
                    entry.advertised_stream_parent = status.stream_parent;
                    self.refresh_effective_limits(&mut entry);
                    entry.has_grpc_stream = true;
                    let sf = status
                        .latest_signed_finalization
                        .as_ref()
                        .expect("validated peer status must contain signed canary");
                    entry.latest_signed_slot = entry.latest_signed_slot.max(sf.finalization.slot);
                    entry.has_signed_bf = true;
                    entry.last_progress = Instant::now();
                    entry.topology_pubkey = status.topology_pubkey;
                    entry.ancestry_canary = status.ancestry_canary.clone();
                    entry.ancestry_delegations = status.ancestry_delegations.clone();
                }
                PeerStatusValidation::NotReady(_reason) => {}
                PeerStatusValidation::Invalid(reason) => {
                    evict_reason = Some(reason);
                }
            }
        }
        if let Some(reason) = evict_reason {
            warn!("Evicting peer {node_id:?} due to invalid status/canary: {reason}");
            self.evict_peer(node_id, reason);
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

    pub fn validate_peer_status(&self, node_id: [u8; 32], status: Option<&PeerStatus>) -> PeerStatusValidation {
        let Some(status) = status else {
            return PeerStatusValidation::NotReady("missing peer status/canary");
        };
        let Some(sf) = status.latest_signed_finalization.as_ref() else {
            return PeerStatusValidation::NotReady("missing signed finalization in status");
        };
        if !self.verify_signed_finalization(sf) {
            return PeerStatusValidation::Invalid("invalid signed finalization in status");
        }
        let Some(canary) = status.ancestry_canary.as_ref() else {
            return PeerStatusValidation::NotReady("missing ancestry canary");
        };
        if !self.verify_signed_finalization(canary) {
            return PeerStatusValidation::Invalid("invalid ancestry canary");
        }
        match self.validate_ancestry(status, node_id) {
            Ok(_) => PeerStatusValidation::Valid,
            Err(reason) => PeerStatusValidation::Invalid(reason),
        }
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

    fn validate_ancestry(
        &self,
        status: &PeerStatus,
        node_id: [u8; 32],
    ) -> Result<Vec<SignedAncestryDelegation>, &'static str> {
        let Some(canary) = status.ancestry_canary.as_ref() else {
            return Err("missing ancestry canary");
        };
        if status.ancestry_delegations.len() > MAX_CANARY_PATH_LEN {
            return Err("ancestry chain too long");
        }
        match status.stream_parent {
            None => {
                if !status.ancestry_delegations.is_empty() {
                    return Err("root ancestry must not have delegations");
                }
                if node_id == self.self_node_id {
                    return Err("ancestry loops back to this node");
                }
                return Ok(Vec::new());
            }
            Some(parent) => {
                if status.ancestry_delegations.is_empty() {
                    return Err("missing ancestry delegations");
                }
                let mut seen_nodes = HashSet::new();
                let mut previous_child_node: Option<[u8; 32]> = None;
                let mut previous_child_pubkey: Option<[u8; 32]> = None;
                for (idx, delegation) in status.ancestry_delegations.iter().enumerate() {
                    if !delegation.verify() {
                        return Err("invalid ancestry delegation signature");
                    }
                    if delegation.root_slot != canary.finalization.slot || delegation.root_signature != canary.signature
                    {
                        return Err("delegation root does not match ancestry canary");
                    }
                    if delegation.expires_at_unix_secs < current_unix_timestamp() {
                        return Err("ancestry delegation expired");
                    }
                    if let Some(expected_node) = previous_child_node {
                        if delegation.parent_node_id != expected_node {
                            return Err("delegation parent node mismatch");
                        }
                    } else if !seen_nodes.insert(delegation.parent_node_id) {
                        return Err("ancestry contains duplicate hop");
                    }
                    if previous_child_pubkey.is_some_and(|pubkey| delegation.parent_topology_pubkey != pubkey) {
                        return Err("delegation parent pubkey mismatch");
                    }
                    if !seen_nodes.insert(delegation.child_node_id) {
                        return Err("ancestry contains duplicate hop");
                    }
                    if delegation.parent_node_id == self.self_node_id || delegation.child_node_id == self.self_node_id {
                        return Err("ancestry loops back to this node");
                    }
                    if idx == 0 && delegation.parent_node_id == self.self_node_id {
                        return Err("ancestry loops back to this node");
                    }
                    previous_child_node = Some(delegation.child_node_id);
                    previous_child_pubkey = Some(delegation.child_topology_pubkey);
                }
                let last = status
                    .ancestry_delegations
                    .last()
                    .expect("non-empty ancestry delegations already checked");
                if last.child_node_id != node_id {
                    return Err("ancestry does not end at peer node");
                }
                if last.child_topology_pubkey != status.topology_pubkey {
                    return Err("peer topology pubkey does not match ancestry tail");
                }
                if last.parent_node_id != parent {
                    return Err("stream parent does not match ancestry tail");
                }
            }
        }
        Ok(status.ancestry_delegations.clone())
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

fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
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
        CommitBatchNotification, GetPeerStatusRequest, GetPeerStatusResponse, PeerStatus, Setup,
        SignedAncestryDelegation, SignedFinalization, SyncBatchShred,
    };
    use solana_sdk::{
        hash::{hashv, Hash},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    };
    use tokio::{net::TcpListener, sync::Mutex};
    use tokio_stream::{wrappers::TcpListenerStream, Stream};
    use tonic::{Response, Status};

    use super::{PeerManager, PeerObservedLimits, PeerState, PeerStatusValidation};

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
                ancestry_canary: None,
                stream_parent: None,
                canary_path: Vec::new(),
                topology_pubkey: [0u8; 32],
                ancestry_delegations: Vec::new(),
                observed_head: 0,
                capabilities: 0,
                setup: None,
            };
            Ok(Response::new(GetPeerStatusResponse {
                status,
                delegation: None,
            }))
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
                advertised_stream_parent: None,
                topology_pubkey: [0u8; 32],
                ancestry_canary: None,
                ancestry_delegations: Vec::new(),
            },
        );
        node_id
    }

    fn signed_finalization(keypair: &Keypair, slot: u64) -> SignedFinalization {
        let finalization = infinisvm_types::sync::SyncFinalization {
            slot,
            num_shreds: 1,
            hash: Hash::new_unique(),
            parent_hash: Hash::new_unique(),
            block_unix_timestamp: 0,
            shred_hashes: vec![[7u8; 32]],
        };
        let msg = bincode::serialize(&finalization).expect("serialize finalization");
        let signature: [u8; 64] = keypair.sign_message(&msg).into();
        SignedFinalization {
            finalization,
            sequencer_pubkey: keypair.pubkey().to_bytes(),
            signature,
        }
    }

    #[tokio::test]
    async fn penalize_invalid_shred_clears_signed_state() {
        let manager = PeerManager::new([9u8; 32], Pubkey::new_unique());
        let node_id = insert_peer(&manager).await;

        manager.penalize_invalid_shred(node_id);

        assert!(!manager.has_peer(node_id));
    }

    #[tokio::test]
    async fn penalize_invalid_finalizer_clears_signed_state() {
        let manager = PeerManager::new([9u8; 32], Pubkey::new_unique());
        let node_id = insert_peer(&manager).await;

        manager.penalize_invalid_finalizer(node_id);

        assert!(!manager.has_peer(node_id));
    }

    #[tokio::test]
    async fn observe_bytes_marks_over_bytes() {
        let manager = PeerManager::new([9u8; 32], Pubkey::new_unique());
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
        let manager = PeerManager::new([9u8; 32], Pubkey::new_unique());
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

    #[test]
    fn validate_peer_status_rejects_loopback_canary() {
        let keypair = Keypair::new();
        let self_node_id = [9u8; 32];
        let peer_node_id = [5u8; 32];
        let manager = PeerManager::new(self_node_id, keypair.pubkey());
        let canary = signed_finalization(&keypair, 42);
        let status = PeerStatus {
            node_id: peer_node_id,
            grpc_addr: "127.0.0.1:15005".to_string(),
            rate_limit_per_sec: 0,
            rate_limit_burst: 0,
            latest_signed_finalization: Some(canary.clone()),
            ancestry_canary: Some(canary.clone()),
            stream_parent: Some(self_node_id),
            canary_path: Vec::new(),
            topology_pubkey: [5u8; 32],
            ancestry_delegations: vec![SignedAncestryDelegation::sign(
                canary.finalization.slot,
                canary.signature,
                self_node_id,
                &keypair,
                peer_node_id,
                [5u8; 32],
                super::current_unix_timestamp().saturating_add(300),
            )],
            observed_head: 42,
            capabilities: 0,
            setup: None,
        };

        assert_eq!(
            manager.validate_peer_status(peer_node_id, Some(&status)),
            PeerStatusValidation::Invalid("ancestry loops back to this node")
        );
    }

    #[test]
    fn validate_peer_status_accepts_multi_hop_ancestry() {
        let sequencer = Keypair::new();
        let parent = Keypair::new();
        let child = Keypair::new();
        let parent_node_id = [3u8; 32];
        let child_node_id = [4u8; 32];
        let leaf_node_id = [5u8; 32];
        let leaf_topology_key = Keypair::new();
        let canary = signed_finalization(&sequencer, 55);
        let manager = PeerManager::new([9u8; 32], sequencer.pubkey());
        let expiry = super::current_unix_timestamp().saturating_add(300);

        let status = PeerStatus {
            node_id: leaf_node_id,
            grpc_addr: "127.0.0.1:15005".to_string(),
            rate_limit_per_sec: 0,
            rate_limit_burst: 0,
            latest_signed_finalization: Some(canary.clone()),
            ancestry_canary: Some(canary.clone()),
            stream_parent: Some(child_node_id),
            canary_path: Vec::new(),
            topology_pubkey: leaf_topology_key.pubkey().to_bytes(),
            ancestry_delegations: vec![
                SignedAncestryDelegation::sign(
                    canary.finalization.slot,
                    canary.signature,
                    parent_node_id,
                    &parent,
                    child_node_id,
                    child.pubkey().to_bytes(),
                    expiry,
                ),
                SignedAncestryDelegation::sign(
                    canary.finalization.slot,
                    canary.signature,
                    child_node_id,
                    &child,
                    leaf_node_id,
                    leaf_topology_key.pubkey().to_bytes(),
                    expiry,
                ),
            ],
            observed_head: 55,
            capabilities: 0,
            setup: None,
        };

        assert_eq!(
            manager.validate_peer_status(leaf_node_id, Some(&status)),
            PeerStatusValidation::Valid
        );
    }
}
