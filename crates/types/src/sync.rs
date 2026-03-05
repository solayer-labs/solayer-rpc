use std::{collections::HashSet, net::IpAddr};

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use solana_hash::Hash;
use solana_sdk::{
    inner_instruction::InnerInstructionsList,
    message::{v0::LoadedAddresses, SimpleAddressLoader},
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
pub struct GetPeerStatusRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerStatus {
    pub node_id: [u8; 32],
    pub grpc_addr: String,
    pub rate_limit_per_sec: u32,
    pub rate_limit_burst: u32,
    pub latest_signed_finalization: Option<SignedFinalization>,
    pub observed_head: u64,
    pub capabilities: u64,
    pub setup: Option<Setup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPeerStatusResponse {
    pub status: PeerStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommitBatchNotification {
    Batch(SyncBatchShred),
    Finalization(SyncFinalization),
    SignedFinalization(SignedFinalization),
}
