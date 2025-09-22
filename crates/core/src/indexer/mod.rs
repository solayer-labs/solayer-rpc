use async_trait::async_trait;
use solana_hash::Hash;
use solana_sdk::signature::Signature;

use infinisvm_types::{jobs::ConsumedJob, serializable::SerializableTxRow};

pub mod index_worker;

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
    fn index_transactions(&mut self, _batch: Vec<ConsumedJob>, _block_unix_timestamp: u64) {}

    async fn index_serializable_tx(
        &mut self,
        _tx: SerializableTxRow,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    // to be called when exiting
    fn flush(&mut self) {}
}
