use std::collections::HashSet;

use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncFinalization {
    pub slot: u64,
    pub num_shreds: u64, // max_job_id = num_shreds - 1
    pub hash: Hash,
    pub parent_hash: Hash,
    pub block_unix_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommitBatchNotification {
    Batch(SyncBatchShred),
    Finalization(SyncFinalization),
}
