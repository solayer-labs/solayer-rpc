use async_trait::async_trait;
use infinisvm_types::sync::{JobEffects, ShredId};
use solana_hash::Hash;
use solana_sdk::signature::Signature;

#[derive(Debug, Clone)]
pub enum SignatureFilters {
    TimeRange(Option<u64>, Option<u64>),
    Signature(Option<Signature>, Option<Signature>),
    None,
}

#[async_trait]
pub trait Indexer: Send + Sync {
    // indexing
    fn index_block(&mut self, _slot: u64, _timestamp: u64, _blockhash: Hash, _parent_blockhash: Hash) {}

    // must filter out non-executed transactions (load error)
    fn index_transactions(&mut self, _batch: Vec<JobEffects>, _block_unix_timestamp: u64, _shred_id: ShredId) {}

    // to be called when exiting
    fn flush(&mut self) {}
}
