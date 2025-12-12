use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use infinisvm_core::indexer::Indexer;
use infinisvm_jsonrpc::rpc_state::RpcIndexer;
use infinisvm_types::{
    convert::{to_signature_rows, to_tx_row},
    jobs::ConsumedJob,
    serializable::{SignatureRow, TxRow},
    BlockWithTransactions, SignatureFilters, TransactionWithMetadata,
};
use solana_hash::Hash;
use solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature};
use spl_token;

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
    // Keep existing block storage
    block: VecDeque<(BlockMetadata, Vec<ConsumedJob>)>,

    // HashMap-based indices for fast lookups
    tx_by_signature: HashMap<Signature, TxRow>,
    signatures_by_account: HashMap<Pubkey, Vec<SignatureRow>>, // account -> sorted by seq_number
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

    fn index_transactions(&mut self, batch: Vec<ConsumedJob>, _block_unix_timestamp: u64) {
        if batch.is_empty() {
            return;
        }

        let slot = batch[0].slot;

        // Process each transaction and index it
        for job in &batch {
            // Skip failed transactions
            if job.processed_transaction.is_err() {
                continue;
            }

            // Extract signature rows and transaction row
            let signature_rows = to_signature_rows(job);
            let (tx_row, account_delta) = to_tx_row(job);

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
            if let Some((account_ops_create, account_ops_delete, account_ops_mint_create, account_ops_mint_delete)) =
                account_delta
            {
                // Handle account_ops create
                for (owner, account) in account_ops_create {
                    self.state
                        .account_ops
                        .entry(owner)
                        .or_default()
                        .insert(account);
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
                    self.state
                        .account_ops_mint
                        .entry(key)
                        .or_default()
                        .insert(account);
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
        }

        // Keep existing block storage logic
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
    async fn find_accounts_owned_by(&self, owner: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey> {
        self.state
            .account_ops
            .get(owner)
            .map(|accounts| accounts.iter().skip(offset).take(limit).copied().collect())
            .unwrap_or_default()
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

        // Create a sorted vector of references
        let mut sorted_indices: Vec<usize> = (0..signatures.len()).collect();
        sorted_indices.sort_by_key(|&i| signatures[i].seq_number);

        // Apply filters
        let filtered: Vec<Signature> = sorted_indices
            .into_iter()
            .filter_map(|i| {
                let sig_row = &signatures[i];
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
        signature: &Signature,
    ) -> eyre::Result<Option<TransactionWithMetadata>> {
        // First try to find in blocks (for backward compatibility and full metadata)
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
                }
            }
        }
        Ok(None)
    }
}
