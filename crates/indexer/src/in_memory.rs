use std::collections::VecDeque;

use async_trait::async_trait;
use infinisvm_core::indexer::Indexer;
use infinisvm_jsonrpc::rpc_state::RpcIndexer;
use infinisvm_types::{jobs::ConsumedJob, BlockWithTransactions, TransactionWithMetadata};
use solana_hash::Hash;
use solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature};

use crate::to_transaction_with_metadata;

#[derive(Default)]
struct BlockMetadata {
    slot: Slot,
    blockhash: Hash,
    parent_blockhash: Hash,
    parent_slot: Slot,
    block_unix_timestamp: u64,
}

pub struct IndexState {
    block: VecDeque<(BlockMetadata, Vec<ConsumedJob>)>,
}

pub struct InMemoryIndexer {
    state: IndexState,
}

// expected blocks per epoch
pub const MAX_BLOCKS: usize = 432000;

impl InMemoryIndexer {
    pub fn new() -> Self {
        Self {
            state: IndexState {
                block: VecDeque::with_capacity(MAX_BLOCKS),
            },
        }
    }
}

impl Default for InMemoryIndexer {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for InMemoryIndexer {}
unsafe impl Sync for InMemoryIndexer {}

impl Indexer for InMemoryIndexer {
    fn index_block(&mut self, slot: u64, timestamp: u64, blockhash: Hash, parent_blockhash: Hash) {
        // modify last block metadata
        match self.state.block.back_mut() {
            Some(last_block) => {
                // if previous block is not the same slot, push a new block
                if last_block.0.slot != slot {
                    self.state.block.push_back((
                        BlockMetadata {
                            slot,
                            blockhash,
                            parent_blockhash,
                            parent_slot: slot.saturating_sub(1),
                            block_unix_timestamp: timestamp,
                        },
                        vec![],
                    ));
                } else {
                    // only update metadata
                    last_block.0.blockhash = blockhash;
                    last_block.0.parent_blockhash = parent_blockhash;
                    last_block.0.parent_slot = slot.saturating_sub(1);
                    last_block.0.block_unix_timestamp = timestamp;
                }
            }
            None => {
                // first block
                self.state.block.push_back((
                    BlockMetadata {
                        slot,
                        blockhash,
                        parent_blockhash,
                        parent_slot: slot.saturating_sub(1),
                        block_unix_timestamp: timestamp,
                    },
                    vec![],
                ));
            }
        }

        if self.state.block.len() == MAX_BLOCKS {
            self.state.block.pop_front();
        }
    }

    fn index_transactions(&mut self, batch: Vec<ConsumedJob>, _block_unix_timestamp: u64) {
        // all txs in the batch should be in the same block
        // find the slot and push the batch to the block
        let slot = batch[0].slot;
        let block_metadata = self.state.block.back_mut().unwrap();

        if block_metadata.0.slot != slot {
            // push a new block
            self.state.block.push_back((
                BlockMetadata {
                    slot,
                    ..Default::default()
                },
                batch,
            ));
        } else {
            self.state.block.back_mut().unwrap().1.extend(batch);
        }
    }

    fn flush(&mut self) {}
}

#[async_trait]
impl RpcIndexer for InMemoryIndexer {
    async fn find_accounts_owned_by(&self, _: &Pubkey, _: usize, _: usize) -> Vec<Pubkey> {
        vec![]
    }

    async fn find_token_accounts_owned_by(
        &self,
        _owner: &Pubkey,
        _program_id: Option<Pubkey>,
        _mint: Option<Pubkey>,
        _limit: usize,
        _offset: usize,
    ) -> Vec<Pubkey> {
        vec![]
    }


    async fn find_token_accounts_by_mint(
        &self,
        _program_id: Option<Pubkey>,
        _mint: Pubkey,
        _limit: usize,
        _offset: usize,
    ) -> Vec<Pubkey> {
        vec![]
    }
    
    async fn get_block_with_transactions(
        &self,
        slot: u64,
        offset: u64,
        limit: u64,
    ) -> eyre::Result<Option<BlockWithTransactions>> {
        let block = self.state.block.iter().find(|(metadata, _)| metadata.slot == slot);
        if let Some((metadata, inner)) = block {
            let transactions = inner
                .iter()
                .skip(offset as usize)
                .take(limit as usize)
                .filter(|job| job.processed_transaction.is_ok())
                .map(|job| TransactionWithMetadata {
                    transaction: job.sanitized_transaction.to_versioned_transaction(),
                    metadata: to_transaction_with_metadata(
                        job.processed_transaction.as_ref().unwrap(),
                        &job.sanitized_transaction,
                    ),
                    slot: job.slot,
                    unix_timestamp_in_millis: 0,
                    seq_number: job.job_id as u64,
                })
                .collect();

            let signatures = inner
                .iter()
                .skip(offset as usize)
                .take(limit as usize)
                .map(|job| job.sanitized_transaction.signature().to_string())
                .collect();

            Ok(Some(BlockWithTransactions {
                slot: metadata.slot,
                parent_blockhash: metadata.parent_blockhash.to_string(),
                blockhash: metadata.blockhash.to_string(),
                parent_slot: metadata.parent_slot,
                block_unix_timestamp: metadata.block_unix_timestamp,
                transactions,
                signatures,
                tx_count: 0,
            }))
        } else {
            Ok(None)
        }
    }

    async fn find_signatures_of_account(
        &self,
        _: &Pubkey,
        _: infinisvm_types::SignatureFilters,
        _: usize,
    ) -> eyre::Result<Vec<Signature>> {
        Ok(vec![])
    }

    async fn get_transaction_with_metadata(
        &self,
        signature: &Signature,
    ) -> eyre::Result<Option<TransactionWithMetadata>> {
        // iterate over all blocks and find the transaction with the signature
        for (_, block) in self.state.block.iter() {
            for job in block.iter() {
                let ConsumedJob {
                    sanitized_transaction,
                    slot,
                    processed_transaction,
                    ..
                } = job;
                if let Ok(processed_transaction) = processed_transaction {
                    if sanitized_transaction.signature() == signature {
                        return Ok(Some(TransactionWithMetadata {
                            transaction: sanitized_transaction.to_versioned_transaction(),
                            metadata: to_transaction_with_metadata(processed_transaction, sanitized_transaction),
                            slot: *slot,
                            unix_timestamp_in_millis: 0,
                            seq_number: job.job_id as u64,
                        }));
                    }
                } else {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }
}
