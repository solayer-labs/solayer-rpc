use std::time::Duration;

use infinisvm_types::sync::PeerStatus;
use rand::Rng;
use tokio::time::{sleep, timeout};

use crate::RegistryStore;

#[derive(Debug, Clone)]
pub struct FishermanConfig {
    pub poll_secs: u64,
    pub max_recent_offset: u64,
    pub probe_timeout: Duration,
    pub initial_delay: Duration,
}

impl Default for FishermanConfig {
    fn default() -> Self {
        Self {
            poll_secs: 60,
            max_recent_offset: 5,
            probe_timeout: Duration::from_millis(3000),
            initial_delay: Duration::from_secs(0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    Valid,
    Invalid,
}

#[derive(Debug, Clone)]
pub struct ProbeError {
    pub message: String,
}

impl ProbeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ProbeError {}

#[async_trait::async_trait]
pub trait PeerProbe: Send + Sync + 'static {
    async fn fetch_peer_status(&self, grpc_addr: &str) -> Result<PeerStatus, ProbeError>;
    async fn probe_finalizer(&self, grpc_addr: &str, slot: u64) -> Result<ProbeOutcome, ProbeError>;
}

pub struct RegistryFisherman<P: PeerProbe> {
    store: RegistryStore,
    probe: P,
    config: FishermanConfig,
}

impl<P: PeerProbe> RegistryFisherman<P> {
    pub fn new(store: RegistryStore, probe: P, config: FishermanConfig) -> Self {
        Self { store, probe, config }
    }

    pub async fn run(self) {
        if !self.config.initial_delay.is_zero() {
            sleep(self.config.initial_delay).await;
        }

        loop {
            let peers = self.store.list().await;
            if peers.is_empty() {
                sleep(Duration::from_secs(self.config.poll_secs)).await;
                continue;
            }

            let mut heads = Vec::with_capacity(peers.len());
            let mut statuses = Vec::with_capacity(peers.len());
            let mut fetch_failures = Vec::new();

            for peer in &peers {
                let status =
                    match timeout(self.config.probe_timeout, self.probe.fetch_peer_status(&peer.grpc_addr)).await {
                        Ok(Ok(status)) => {
                            if status.observed_head > 0 {
                                heads.push(status.observed_head);
                            } else if let Some(sf) = status.latest_signed_finalization.as_ref() {
                                heads.push(sf.finalization.slot);
                            }
                            Some(status)
                        }
                        _ => {
                            fetch_failures.push(peer.node_id);
                            None
                        }
                    };
                statuses.push(status);
            }

            // If a peer is unreachable for status probing, evict it immediately.
            for node_id in fetch_failures {
                let _ = self.store.evict(node_id).await;
            }

            let global_head = heads.into_iter().max().unwrap_or(0);

            for (peer, status) in peers.iter().zip(statuses.iter()) {
                if status.is_none() {
                    continue;
                }

                let Some(slot) = pick_probe_slot(status.as_ref(), global_head, self.config.max_recent_offset) else {
                    continue;
                };

                let probe_result = timeout(
                    self.config.probe_timeout,
                    self.probe.probe_finalizer(&peer.grpc_addr, slot),
                )
                .await;

                let should_evict = match probe_result {
                    Ok(Ok(ProbeOutcome::Valid)) => false,
                    Ok(Ok(ProbeOutcome::Invalid)) => true,
                    Ok(Err(_)) => true,
                    Err(_) => true,
                };

                if should_evict {
                    let _ = self.store.evict(peer.node_id).await;
                }
            }

            sleep(Duration::from_secs(self.config.poll_secs)).await;
        }
    }
}

fn pick_probe_slot(status: Option<&PeerStatus>, global_head: u64, max_offset: u64) -> Option<u64> {
    if let Some(status) = status {
        if let Some(sf) = status.latest_signed_finalization.as_ref() {
            return Some(sf.finalization.slot);
        }
        if status.observed_head > 0 {
            let offset = rand::thread_rng().gen_range(0..=max_offset.min(status.observed_head));
            return Some(status.observed_head.saturating_sub(offset));
        }
    }

    if global_head > 0 {
        let offset = rand::thread_rng().gen_range(0..=max_offset.min(global_head));
        return Some(global_head.saturating_sub(offset));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingStatusProbe;

    #[async_trait::async_trait]
    impl PeerProbe for FailingStatusProbe {
        async fn fetch_peer_status(&self, _grpc_addr: &str) -> Result<PeerStatus, ProbeError> {
            Err(ProbeError::new("peer disconnected"))
        }

        async fn probe_finalizer(&self, _grpc_addr: &str, _slot: u64) -> Result<ProbeOutcome, ProbeError> {
            Ok(ProbeOutcome::Valid)
        }
    }

    #[tokio::test]
    async fn evicts_peer_when_status_fetch_fails() {
        let store = RegistryStore::new();
        let node_id = [7u8; 32];
        store.upsert_peer(node_id, "127.0.0.1:15005".to_string(), 0.0).await;

        let fisherman = RegistryFisherman::new(
            store.clone(),
            FailingStatusProbe,
            FishermanConfig {
                poll_secs: 60,
                max_recent_offset: 5,
                probe_timeout: Duration::from_millis(20),
                initial_delay: Duration::from_millis(0),
            },
        );

        let handle = tokio::spawn(async move {
            fisherman.run().await;
        });

        let evicted = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if store.len().await == 0 {
                    break true;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or(false);

        handle.abort();
        assert!(evicted, "expected disconnected peer to be evicted");
    }
}
