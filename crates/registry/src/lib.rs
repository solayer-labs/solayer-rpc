use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub mod fisherman;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcPeerInfo {
    pub node_id: [u8; 32],
    pub grpc_addr: String,
    pub last_seen_ts: u64,
    pub score_hint: f64,
}

#[derive(Serialize, Deserialize)]
pub struct RpcSetResponse {
    pub peers: Vec<RpcPeerInfo>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcRegisterRequest {
    pub grpc_addr: String,
    #[serde(default)]
    pub score_hint: f64,
}

#[derive(Clone, Default)]
pub struct RegistryStore {
    inner: Arc<RwLock<HashMap<[u8; 32], RpcPeerInfo>>>,
}

impl RegistryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn upsert_peer(&self, node_id: [u8; 32], grpc_addr: String, score_hint: f64) -> RpcPeerInfo {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let peer = RpcPeerInfo {
            node_id,
            grpc_addr,
            last_seen_ts: now,
            score_hint,
        };
        let mut registry = self.inner.write().await;
        registry.insert(peer.node_id, peer.clone());
        peer
    }

    pub async fn set_peers(&self, peers: Vec<RpcPeerInfo>) {
        let mut registry = self.inner.write().await;
        registry.clear();
        for peer in peers {
            registry.insert(peer.node_id, peer);
        }
    }

    pub async fn list(&self) -> Vec<RpcPeerInfo> {
        let registry = self.inner.read().await;
        registry.values().cloned().collect::<Vec<_>>()
    }

    pub async fn evict(&self, node_id: [u8; 32]) -> bool {
        let mut registry = self.inner.write().await;
        registry.remove(&node_id).is_some()
    }

    pub async fn len(&self) -> usize {
        let registry = self.inner.read().await;
        registry.len()
    }

    pub async fn is_empty(&self) -> bool {
        let registry = self.inner.read().await;
        registry.is_empty()
    }
}
