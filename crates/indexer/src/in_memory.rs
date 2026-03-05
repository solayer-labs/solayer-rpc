use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use infinisvm_core::indexer::Indexer;
use infinisvm_jsonrpc::rpc_state::RpcIndexer;
use infinisvm_types::{
    convert::{to_signature_rows, to_tx_row, JobEffectAccountDiffOps, TxOrdering},
    serializable::{SignatureRow, TxRow},
    sync::{JobEffects, ShredId},
    BlockWithTransactions, SignatureFilters, TransactionWithMetadata,
};
use solana_hash::Hash;
use solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature};
use spl_token;

#[derive(Default)]
struct BlockMetadata {
    slot: Slot,
    blockhash: Hash,
    parent_blockhash: Hash,
    parent_slot: Slot,
    block_unix_timestamp: u64,
}

pub struct IndexState {
    block: VecDeque<(BlockMetadata, Vec<JobEffects>)>,
    tx_by_signature: HashMap<Signature, TxRow>,
    signatures_by_account: HashMap<Pubkey, Vec<SignatureRow>>, // account -> sorted by ordering
    account_ops: HashMap<Pubkey, HashSet<Pubkey>>,             // owner -> accounts
    account_ops_mint: HashMap<(Pubkey, u8, Pubkey), HashSet<Pubkey>>, // (owner, account_type, mint) -> accounts
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
                tx_by_signature: HashMap::new(),
                signatures_by_account: HashMap::new(),
                account_ops: HashMap::new(),
                account_ops_mint: HashMap::new(),
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

    fn index_transactions(&mut self, batch: Vec<JobEffects>, block_unix_timestamp: u64, shred_id: ShredId) {
        if batch.is_empty() {
            return;
        }

        let slot = shred_id.slot;

        // Process each transaction and index it
        for (kth, job) in batch.iter().enumerate() {
            // Skip failed transactions
            if job.execution_result.is_err() {
                continue;
            }

            // Extract signature rows and transaction row
            let ordering = TxOrdering::from_shred_id(&shred_id, kth as u64);
            let sanitized_tx = match job.sanitized_tx() {
                Ok(sanitized_tx) => sanitized_tx,
                Err(_) => continue,
            };
            let signature_rows = to_signature_rows(&sanitized_tx, slot, block_unix_timestamp, ordering.clone());
            let (tx_row, account_delta) = to_tx_row(job.clone(), &sanitized_tx, slot, block_unix_timestamp, ordering);

            // Store transaction by signature
            let signature = Signature::try_from(tx_row.signature.as_ref()).expect("Invalid signature bytes");
            self.state.tx_by_signature.insert(signature, tx_row);

            // Store signatures by account
            for sig_row in signature_rows {
                let account = Pubkey::try_from(sig_row.account.as_ref()).expect("Invalid account bytes");
                self.state
                    .signatures_by_account
                    .entry(account)
                    .or_default()
                    .push(sig_row);
            }

            // Process account operations if available
            // Handle account_ops create
            let JobEffectAccountDiffOps {
                account_ops_create,
                account_ops_delete,
                account_ops_mint_create,
                account_ops_mint_delete,
            } = account_delta;
            for (owner, account) in account_ops_create {
                self.state.account_ops.entry(owner).or_default().insert(account);
            }

            // Handle account_ops delete
            for (owner, account) in account_ops_delete {
                if let Some(accounts) = self.state.account_ops.get_mut(&owner) {
                    accounts.remove(&account);
                }
            }

            // Handle account_ops_mint create
            for (owner, account, account_type, mint) in account_ops_mint_create {
                let key = (owner, account_type, mint);
                self.state.account_ops_mint.entry(key).or_default().insert(account);
            }

            // Handle account_ops_mint delete
            for (owner, account) in account_ops_mint_delete {
                // Need to find and remove from all matching entries
                // Since we don't have account_type and mint in delete, we need to iterate
                self.state.account_ops_mint.retain(|(o, _, _), accounts| {
                    if *o == owner {
                        accounts.remove(&account);
                    }
                    !accounts.is_empty()
                });
            }
        }

        // Keep existing block storage logic
        match self.state.block.back_mut() {
            Some(block_metadata) if block_metadata.0.slot == slot => {
                block_metadata.1.extend(batch);
            }
            _ => {
                self.state.block.push_back((
                    BlockMetadata {
                        slot,
                        ..Default::default()
                    },
                    batch,
                ));
            }
        }
    }

    fn flush(&mut self) {}
}

#[async_trait]
impl RpcIndexer for InMemoryIndexer {
    async fn find_accounts_owned_by(&self, owner: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey> {
        self.state
            .account_ops
            .get(owner)
            .map(|accounts| accounts.iter().skip(offset).take(limit).copied().collect())
            .unwrap_or_default()
    }

    async fn find_accounts_by_program(&self, _program_id: &Pubkey, _limit: usize, _offset: usize) -> Vec<Pubkey> {
        // Program account indexing not implemented for in-memory indexer
        vec![]
    }

    async fn find_token_accounts_owned_by(
        &self,
        owner: &Pubkey,
        program_id: Option<Pubkey>,
        mint: Option<Pubkey>,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        let account_type = program_id.map(|pid| if pid == spl_token::id() { 1u8 } else { 2u8 });

        let mut results: Vec<Pubkey> = Vec::new();

        for ((o, at, m), accounts) in &self.state.account_ops_mint {
            // Check owner match (required)
            if *o != *owner {
                continue;
            }

            // Check account_type match if provided
            if let Some(at_filter) = account_type {
                if *at != at_filter {
                    continue;
                }
            }

            // Check mint match if provided
            if let Some(mint_filter) = mint {
                if *m != mint_filter {
                    continue;
                }
            }

            // Collect accounts from this entry
            results.extend(accounts.iter().copied());
        }

        // Apply offset and limit
        results.into_iter().skip(offset).take(limit).collect()
    }

    async fn find_token_accounts_by_mint(
        &self,
        program_id: Option<Pubkey>,
        mint: Pubkey,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        let account_type = program_id.map(|pid| if pid == spl_token::id() { 1u8 } else { 2u8 });

        let mut results: Vec<Pubkey> = Vec::new();

        for ((_owner, at, m), accounts) in &self.state.account_ops_mint {
            // Check mint match (required)
            if *m != mint {
                continue;
            }

            // Check account_type match if provided
            if let Some(at_filter) = account_type {
                if *at != at_filter {
                    continue;
                }
            }

            // Collect accounts from this entry
            results.extend(accounts.iter().copied());
        }

        // Apply offset and limit
        results.into_iter().skip(offset).take(limit).collect()
    }

    async fn get_block_with_transactions(
        &self,
        _slot: u64,
        _offset: u64,
        _limit: u64,
    ) -> eyre::Result<Option<BlockWithTransactions>> {
        // Transaction content/block reconstruction is not implemented for the in-memory
        // indexer.
        Ok(None)
    }

    async fn find_signatures_of_account(
        &self,
        account: &Pubkey,
        filters: SignatureFilters,
        limit: usize,
    ) -> eyre::Result<Vec<Signature>> {
        let signatures = self
            .state
            .signatures_by_account
            .get(account)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        // Apply filters
        let filtered: Vec<Signature> = signatures
            .iter()
            .filter_map(|i| {
                let sig_row = i;
                let sig = Signature::try_from(sig_row.signature.as_ref()).ok()?;

                // Apply time range filter
                match &filters {
                    SignatureFilters::TimeRange(Some(start), Some(end)) => {
                        if sig_row.block_unix_timestamp < *start || sig_row.block_unix_timestamp > *end {
                            return None;
                        }
                    }
                    SignatureFilters::TimeRange(Some(start), None) => {
                        if sig_row.block_unix_timestamp < *start {
                            return None;
                        }
                    }
                    SignatureFilters::TimeRange(None, Some(end)) => {
                        if sig_row.block_unix_timestamp > *end {
                            return None;
                        }
                    }
                    SignatureFilters::TimeRange(None, None) | SignatureFilters::None => {}
                    SignatureFilters::Signature(Some(start_sig), Some(end_sig)) => {
                        if sig < *start_sig || sig > *end_sig {
                            return None;
                        }
                    }
                    SignatureFilters::Signature(Some(start_sig), None) => {
                        if sig < *start_sig {
                            return None;
                        }
                    }
                    SignatureFilters::Signature(None, Some(end_sig)) => {
                        if sig > *end_sig {
                            return None;
                        }
                    }
                    SignatureFilters::Signature(None, None) => {}
                }

                Some(sig)
            })
            .take(limit)
            .collect();

        Ok(filtered)
    }

    async fn get_transaction_with_metadata(
        &self,
        _signature: &Signature,
    ) -> eyre::Result<Option<TransactionWithMetadata>> {
        // Transaction content reconstruction is not implemented for the in-memory
        // indexer.
        Ok(None)
    }
}
