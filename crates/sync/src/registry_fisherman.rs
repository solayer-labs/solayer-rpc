use async_trait::async_trait;
use infinisvm_registry::{
    fisherman::{FishermanConfig, PeerProbe, ProbeError, ProbeOutcome, RegistryFisherman},
    RegistryStore,
};
use infinisvm_types::sync::PeerStatus;
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::grpc::client::SyncClient;

#[derive(Clone)]
pub struct GrpcPeerProbe {
    sequencer_pubkey: Pubkey,
}

impl GrpcPeerProbe {
    pub fn new(sequencer_pubkey: Pubkey) -> Self {
        Self { sequencer_pubkey }
    }
}

#[async_trait]
impl PeerProbe for GrpcPeerProbe {
    async fn fetch_peer_status(&self, grpc_addr: &str) -> Result<PeerStatus, ProbeError> {
        let addr = normalize_grpc_addr(grpc_addr);
        let mut client = SyncClient::connect(&addr)
            .await
            .map_err(|e| ProbeError::new(e.to_string()))?;
        client
            .get_peer_status()
            .await
            .map_err(|e| ProbeError::new(e.to_string()))
    }

    async fn probe_finalizer(&self, grpc_addr: &str, slot: u64) -> Result<ProbeOutcome, ProbeError> {
        let addr = normalize_grpc_addr(grpc_addr);
        let mut client = SyncClient::connect(&addr)
            .await
            .map_err(|e| ProbeError::new(e.to_string()))?;
        let sf = client
            .get_block_finalizer(slot)
            .await
            .map_err(|e| ProbeError::new(e.to_string()))?;
        if verify_signed_finalization(&sf, &self.sequencer_pubkey) {
            Ok(ProbeOutcome::Valid)
        } else {
            Ok(ProbeOutcome::Invalid)
        }
    }
}

pub fn spawn_registry_fisherman(
    registry: RegistryStore,
    sequencer_pubkey: Pubkey,
    config: FishermanConfig,
) -> tokio::task::JoinHandle<()> {
    let probe = GrpcPeerProbe::new(sequencer_pubkey);
    tokio::spawn(async move {
        RegistryFisherman::new(registry, probe, config).run().await;
    })
}

fn verify_signed_finalization(sf: &infinisvm_types::sync::SignedFinalization, sequencer_pubkey: &Pubkey) -> bool {
    if sf.sequencer_pubkey != sequencer_pubkey.to_bytes() {
        return false;
    }
    let msg = match bincode::serialize(&sf.finalization) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let sig = Signature::from(sf.signature);
    sig.verify(sequencer_pubkey.as_ref(), &msg)
}

fn normalize_grpc_addr(addr: &str) -> String {
    if addr.starts_with("http://") {
        addr.to_string()
    } else if addr.starts_with("https://") {
        panic!("https:// gRPC addresses are not supported: {addr}");
    } else {
        format!("http://{addr}")
    }
}
