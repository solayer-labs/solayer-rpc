pub mod codec;
pub mod convert;
pub mod jobs;
pub mod serializable;
pub mod sync;
pub mod tid;

use serde::{Deserialize, Serialize};
use solana_sdk::{signature::Signature, transaction::VersionedTransaction};
use solana_transaction_status_client_types::TransactionStatusMeta;

pub enum SignatureFilters {
    TimeRange(Option<u64>, Option<u64>),
    Signature(Option<Signature>, Option<Signature>),
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionWithMetadata {
    pub transaction: VersionedTransaction,
    pub metadata: TransactionStatusMeta,
    pub slot: u64,
    pub unix_timestamp_in_millis: u64,
    pub seq_number: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockWithTransactions {
    pub slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub parent_slot: u64,
    pub block_unix_timestamp: u64,
    pub transactions: Vec<TransactionWithMetadata>,
    pub signatures: Vec<String>,
    pub tx_count: u64,
}

impl BlockWithTransactions {
    pub fn new(block: BlockWithSignatures, transactions: Vec<TransactionWithMetadata>) -> Self {
        Self {
            tx_count: transactions.len() as u64,
            slot: block.slot,
            parent_blockhash: block.parent_blockhash,
            blockhash: block.blockhash,
            parent_slot: block.parent_slot,
            block_unix_timestamp: block.block_unix_timestamp,
            transactions,
            signatures: block.signatures,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockWithSignatures {
    pub slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub parent_slot: u64,
    pub block_unix_timestamp: u64,
    pub signatures: Vec<String>,
}
