use std::{collections::HashSet, net::IpAddr};

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use solana_hash::Hash;
use solana_sdk::{
    inner_instruction::InnerInstructionsList,
    message::{v0::LoadedAddresses, SimpleAddressLoader},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::{self, MessageHash, SanitizedTransaction, TransactionError, VersionedTransaction},
};
use solana_transaction_context::TransactionReturnData;

use crate::convert::JobEffectDiff;

pub type ShredIndex = usize;

// Bincode-based gRPC message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartReceivingSlotsRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ShredId {
    pub slot: u64,
    pub index: ShredIndex,
}

impl ShredId {
    pub fn new(slot: u64, index: ShredIndex) -> Self {
        Self { slot, index }
    }
}

// BatchShred type to be used in the sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchShred {
    pub shred_id: ShredId,
    pub worker_id: usize,
    pub effects: Vec<JobEffects>,
    pub shred_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEffects {
    pub versioned_tx: VersionedTransaction,
    pub execution_result: Result<(), TransactionError>,
    pub job_effect_diff: JobEffectDiff,

    // original structs for TransactionExecutionDetailsSerializable
    pub status: transaction::Result<()>,
    pub log_messages: Option<Vec<String>>,
    pub inner_instructions: Option<InnerInstructionsList>,
    pub return_data: Option<TransactionReturnData>,
    pub executed_units: u64,
    pub accounts_data_len_delta: i64,
    pub fee: u64,
}

impl JobEffects {
    pub fn sanitized_tx(&self) -> Result<SanitizedTransaction, TransactionError> {
        SanitizedTransaction::try_create(
            self.versioned_tx.clone(),
            MessageHash::Compute,
            Some(false),
            SimpleAddressLoader::Enabled(LoadedAddresses::default()),
            &HashSet::new(),
        )
    }
}

impl SyncBatchShred {
    pub fn payload_for_hash(&self) -> Result<Vec<u8>, bincode::Error> {
        #[derive(Serialize)]
        struct SyncBatchShredPayload<'a> {
            shred_id: &'a ShredId,
            worker_id: &'a usize,
            effects: &'a [JobEffects],
            shred_hash: [u8; 32],
        }

        let payload = SyncBatchShredPayload {
            shred_id: &self.shred_id,
            worker_id: &self.worker_id,
            effects: &self.effects,
            shred_hash: [0u8; 32],
        };
        bincode::serialize(&payload)
    }

    pub fn compute_shred_hash(&self) -> [u8; 32] {
        let payload = self.payload_for_hash().expect("serialize shred payload for hash");
        *blake3::hash(&payload).as_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncFinalization {
    pub slot: u64,
    pub num_shreds: u64, // max_job_id = num_shreds - 1
    pub hash: Hash,
    pub parent_hash: Hash,
    pub block_unix_timestamp: u64,
    pub shred_hashes: Vec<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedFinalization {
    pub finalization: SyncFinalization,
    pub sequencer_pubkey: [u8; 32],
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotManifestFile {
    pub filename: String,
    #[serde(with = "BigArray")]
    pub blake3_hash: [u8; 32],
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub version: u16,
    pub checkpoint_slot: u64,
    pub files: Vec<SnapshotManifestFile>,
}

impl SnapshotManifest {
    pub const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedSnapshotManifest {
    pub manifest: SnapshotManifest,
    pub sequencer_pubkey: [u8; 32],
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

impl SignedSnapshotManifest {
    pub fn sign(manifest: SnapshotManifest, keypair: &Keypair) -> Self {
        let msg = bincode::serialize(&manifest).expect("serialize snapshot manifest");
        let sig = keypair.sign_message(&msg);
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(sig.as_ref());
        Self {
            manifest,
            sequencer_pubkey: keypair.pubkey().to_bytes(),
            signature: sig_bytes,
        }
    }

    pub fn verify(&self, sequencer_pubkey: &Pubkey) -> bool {
        if self.sequencer_pubkey != sequencer_pubkey.to_bytes() {
            return false;
        }
        let msg = match bincode::serialize(&self.manifest) {
            Ok(msg) => msg,
            Err(_) => return false,
        };
        Signature::from(self.signature).verify(sequencer_pubkey.as_ref(), &msg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedAncestryDelegation {
    pub root_slot: u64,
    #[serde(with = "BigArray")]
    pub root_signature: [u8; 64],
    pub parent_node_id: [u8; 32],
    pub parent_topology_pubkey: [u8; 32],
    pub child_node_id: [u8; 32],
    pub child_topology_pubkey: [u8; 32],
    pub expires_at_unix_secs: u64,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

impl SignedAncestryDelegation {
    pub fn sign(
        root_slot: u64,
        root_signature: [u8; 64],
        parent_node_id: [u8; 32],
        parent_keypair: &Keypair,
        child_node_id: [u8; 32],
        child_topology_pubkey: [u8; 32],
        expires_at_unix_secs: u64,
    ) -> Self {
        let parent_topology_pubkey = parent_keypair.pubkey().to_bytes();
        let msg = bincode::serialize(&(
            root_slot,
            root_signature.to_vec(),
            parent_node_id,
            parent_topology_pubkey,
            child_node_id,
            child_topology_pubkey,
            expires_at_unix_secs,
        ))
        .expect("serialize ancestry delegation");
        let signature = parent_keypair.sign_message(&msg);
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(signature.as_ref());
        Self {
            root_slot,
            root_signature,
            parent_node_id,
            parent_topology_pubkey,
            child_node_id,
            child_topology_pubkey,
            expires_at_unix_secs,
            signature: sig_bytes,
        }
    }

    pub fn verify(&self) -> bool {
        let msg = match bincode::serialize(&(
            self.root_slot,
            self.root_signature.to_vec(),
            self.parent_node_id,
            self.parent_topology_pubkey,
            self.child_node_id,
            self.child_topology_pubkey,
            self.expires_at_unix_secs,
        )) {
            Ok(msg) => msg,
            Err(_) => return false,
        };
        Signature::from(self.signature).verify(&self.parent_topology_pubkey, &msg)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Setup {
    pub ratelimit_per_ip: u64,
    pub max_stream_per_ip: u64,
    pub ratelimit_per_functional_rpc: u64,
    pub max_bytes_until_functional_rpc: u64,
    pub max_bytes_until_functional_rpc_reset: u64,
    pub ip_whitelist: Vec<IpAddr>,
    pub pubkey_whitelist: Vec<IpAddr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPeerStatusRequest {
    pub requester_node_id: Option<[u8; 32]>,
    pub requester_topology_pubkey: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerStatus {
    pub node_id: [u8; 32],
    pub grpc_addr: String,
    pub rate_limit_per_sec: u32,
    pub rate_limit_burst: u32,
    pub latest_signed_finalization: Option<SignedFinalization>,
    pub ancestry_canary: Option<SignedFinalization>,
    pub stream_parent: Option<[u8; 32]>,
    pub canary_path: Vec<[u8; 32]>,
    pub topology_pubkey: [u8; 32],
    pub ancestry_delegations: Vec<SignedAncestryDelegation>,
    pub observed_head: u64,
    pub capabilities: u64,
    pub setup: Option<Setup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPeerStatusResponse {
    pub status: PeerStatus,
    pub delegation: Option<SignedAncestryDelegation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommitBatchNotification {
    Batch(SyncBatchShred),
    Finalization(SyncFinalization),
    SignedFinalization(SignedFinalization),
}

#[cfg(test)]
mod tests {
    use solana_sdk::signature::Signer;

    use super::{SignedAncestryDelegation, SignedSnapshotManifest, SnapshotManifest, SnapshotManifestFile};

    #[test]
    fn signed_snapshot_manifest_verifies_and_detects_tampering() {
        let keypair = solana_sdk::signature::Keypair::new();
        let manifest = SnapshotManifest {
            version: SnapshotManifest::VERSION,
            checkpoint_slot: 42,
            files: vec![SnapshotManifestFile {
                filename: "ckpt_000000000000000042.bin".to_string(),
                blake3_hash: [7u8; 32],
                size_bytes: 123,
            }],
        };

        let signed = SignedSnapshotManifest::sign(manifest.clone(), &keypair);
        assert!(signed.verify(&keypair.pubkey()));

        let mut tampered = signed.clone();
        tampered.manifest = SnapshotManifest {
            checkpoint_slot: 43,
            ..manifest
        };
        assert!(!tampered.verify(&keypair.pubkey()));
    }

    #[test]
    fn signed_ancestry_delegation_verifies_and_detects_tampering() {
        let parent = solana_sdk::signature::Keypair::new();
        let child = solana_sdk::signature::Keypair::new();
        let delegation = SignedAncestryDelegation::sign(
            42,
            [7u8; 64],
            [1u8; 32],
            &parent,
            [2u8; 32],
            child.pubkey().to_bytes(),
            1234,
        );
        assert!(delegation.verify());

        let mut tampered = delegation.clone();
        tampered.child_node_id = [3u8; 32];
        assert!(!tampered.verify());
    }
}
