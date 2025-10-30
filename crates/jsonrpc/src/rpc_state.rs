use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc, RwLock},
    time::Instant,
};

use async_trait::async_trait;
use crossbeam_channel::Sender;
use eyre::Context;
use infinisvm_core::{bank::Bank, committer::PerfSample, indexer::Indexer, subscription::SubscriptionProcessor};
use infinisvm_db::Database;
use infinisvm_logger::{error, tracing};
use infinisvm_types::{BlockWithTransactions, SignatureFilters, TransactionWithMetadata};
use jsonrpsee::{core::RpcResult, types::ErrorCode};
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig};
use solana_hash::Hash;
use solana_sdk::{
    account::AccountSharedData,
    clock::Slot,
    instruction::AccountMeta,
    message::{v0::LoadedAddresses, SimpleAddressLoader},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    system_instruction::transfer,
    transaction::{MessageHash, SanitizedTransaction, Transaction, TransactionError, VersionedTransaction},
};
use solana_svm::transaction_processing_result::ProcessedTransaction;

use crate::{
    rpc_impl::{RpcFilterType, RpcKeyedAccount, RpcResponse, RpcResponseContext, RpcTokenAccountsFilter},
    ws_helper::WebSocketMiddleware,
};

pub struct RpcContext {
    identity: Keypair,
    tpu: SocketAddr,
}

pub trait RpcBank: Send + Sync {
    fn get_latest_slot_hash_timestamp(&self) -> (u64, Hash, u64);
    fn is_blockhash_valid(&self, blockhash: &Hash) -> bool;
    fn get_tx_status(&self, signature: &Signature) -> Option<infinisvm_core::bank::TransactionStatus>;

    fn simulate_transaction(
        &self,
        tx: SanitizedTransaction,
    ) -> Result<(ProcessedTransaction, Vec<(Pubkey, AccountSharedData)>), TransactionError>;

    // Helper functions
    fn get_current_slot(&self) -> u64 {
        self.get_latest_slot_hash_timestamp().0
    }
    fn is_tx_blockhash_valid(&self, tx: &SanitizedTransaction) -> bool {
        self.is_blockhash_valid(tx.message().recent_blockhash())
    }
}

impl RpcBank for Bank {
    fn get_latest_slot_hash_timestamp(&self) -> (u64, Hash, u64) {
        self.get_latest_slot_hash_timestamp()
    }
    fn is_blockhash_valid(&self, blockhash: &Hash) -> bool {
        self.is_blockhash_valid(blockhash)
    }
    fn get_tx_status(&self, signature: &Signature) -> Option<infinisvm_core::bank::TransactionStatus> {
        self.get_tx_status(signature)
    }
    fn simulate_transaction(
        &self,
        tx: SanitizedTransaction,
    ) -> Result<(ProcessedTransaction, Vec<(Pubkey, AccountSharedData)>), TransactionError> {
        self.simulate_transaction(tx)
    }
}

pub struct RpcServerState {
    pub db: Arc<RwLock<dyn Database>>,
    pub bank: Arc<RwLock<dyn RpcBank>>,
    pub indexer: Arc<dyn RpcIndexer>,
    pub samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>,
    pub total_transaction_count: Arc<AtomicU64>,
    pub tx_service: TxService,
    pub forward_to: Option<String>,
    pub subscription_processor: Arc<SubscriptionProcessor>,
    pub middleware: WebSocketMiddleware,

    _ctx: RpcContext,
}

pub struct TxService {
    keypair: Keypair,
    sender: Sender<(SanitizedTransaction, u64)>,
}

// FUND4EFuH8XaPmFkFLvABVQzfBZ2GQ7grYHhWV6ZYTQm
pub const IDENTITY_KEYPAIR_BYTES: [u8; 64] = [
    168, 189, 101, 67, 3, 226, 68, 16, 6, 31, 16, 253, 130, 228, 79, 72, 227, 35, 71, 68, 22, 91, 6, 120, 35, 105, 60,
    99, 58, 192, 217, 155, 215, 6, 71, 83, 55, 44, 31, 79, 188, 179, 201, 159, 126, 75, 17, 148, 171, 189, 12, 209, 78,
    98, 110, 124, 188, 136, 235, 198, 175, 106, 143, 130,
];

impl TxService {
    pub fn new(sender: Sender<(SanitizedTransaction, u64)>) -> Self {
        let keypair = Keypair::try_from(&IDENTITY_KEYPAIR_BYTES[..]).unwrap();
        Self { keypair, sender }
    }

    pub fn build_airdrop_transaction(&self, pubkey: &Pubkey, blockhash: Hash, lamports: u64) -> Transaction {
        let mut ix = transfer(&self.keypair.pubkey(), pubkey, lamports);

        // prevent dup tx
        ix.accounts.push(AccountMeta::new(Pubkey::new_unique(), false));

        Transaction::new_signed_with_payer(
            &[ix], // 0.1 SOL
            Some(&self.keypair.pubkey()),
            &[&self.keypair],
            blockhash,
        )
    }

    pub fn send_transaction(&self, tx: VersionedTransaction) -> eyre::Result<String> {
        let sanitized_tx = SanitizedTransaction::try_create(
            tx,
            MessageHash::Compute,
            Some(false), // is_simple_vote_tx
            SimpleAddressLoader::Enabled(LoadedAddresses::default()),
            &std::collections::HashSet::new(), // reserved_account_keys
        )?; // TransactionError will be propagated by ?
        let signature = sanitized_tx.signature().to_string();
        self.sender.send((sanitized_tx, 1))?;
        Ok(signature)
    }

    pub fn request_airdrop(&self, pubkey: &Pubkey, blockhash: Hash, lamports: u64) -> eyre::Result<String> {
        let tx = self.build_airdrop_transaction(pubkey, blockhash, lamports);

        let sanitized_tx = SanitizedTransaction::try_from_legacy_transaction(tx, &std::collections::HashSet::new())?;
        let signature = sanitized_tx.signature().to_string();

        self.sender.send((sanitized_tx, 1))?;
        Ok(signature)
    }
}

pub const RECENT_BLOCKHASHES_HISTORY_SIZE: usize = 1024;

#[async_trait]
pub trait RpcIndexer: Indexer + Send + Sync {
    // rpc stuff
    async fn find_accounts_owned_by(&self, _: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey>;

    async fn find_token_accounts_owned_by(
        &self,
        _: &Pubkey,
        program_id: Option<Pubkey>,
        mint: Option<Pubkey>,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey>;
    async fn find_token_accounts_by_mint(
        &self,
        program_id: Option<Pubkey>,
        mint: Pubkey,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey>;
    async fn get_block_with_transactions(
        &self,
        slot: u64,
        offset: u64,
        limit: u64,
    ) -> eyre::Result<Option<BlockWithTransactions>>;
    async fn find_signatures_of_account(
        &self,
        account: &Pubkey,
        filters: SignatureFilters,
        limit: usize,
    ) -> eyre::Result<Vec<Signature>>;
    async fn get_transaction_with_metadata(
        &self,
        signature: &Signature,
    ) -> eyre::Result<Option<TransactionWithMetadata>>;
}

impl RpcServerState {
    pub fn new(
        bank: Arc<RwLock<dyn RpcBank>>,
        db: Arc<RwLock<dyn Database>>,
        indexer: Arc<dyn RpcIndexer>,
        samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>,
        total_transaction_count: Arc<AtomicU64>,
        tx_sender: Sender<(SanitizedTransaction, u64)>,
        forward_to: Option<String>,
        tpu: SocketAddr,
        subscription_processor: Arc<SubscriptionProcessor>,
    ) -> Self {
        Self {
            bank,
            db,
            indexer,
            samples,
            total_transaction_count,
            tx_service: TxService::new(tx_sender),
            forward_to,
            subscription_processor,
            middleware: WebSocketMiddleware::new(),
            _ctx: RpcContext {
                identity: Keypair::new(),
                tpu,
            },
        }
    }

    pub fn get_middleware(&self) -> &WebSocketMiddleware {
        &self.middleware
    }

    pub fn get_subscription_processor(&self) -> &SubscriptionProcessor {
        &self.subscription_processor
    }

    pub fn wrap_with_rpc_context<T>(&self, value: T) -> RpcResult<RpcResponse<T>> {
        let bank = self.bank.read().map_err(|e| {
            error!("wrap_with_rpc_context error: {:?}", e);
            ErrorCode::InternalError
        })?;

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot: bank.get_current_slot(),
            },
            value,
        })
    }

    /// Try to forward the request to another RPC endpoint. Return true if
    /// forwarding is enabled.
    #[tracing::instrument(name = "forward-request", skip_all, fields(%method))]
    pub fn try_forward_to(&self, method: &str, params: serde_json::Value) -> bool {
        if let Some(forward_to) = &self.forward_to {
            // info!(?params, "Forwarding request");

            let client = reqwest::blocking::Client::new();

            let r = client
                .post(forward_to)
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                    "id": 1,
                }))
                .send()
                .context("Failed to send request")
                .and_then(|v| v.error_for_status().context("Status error"))
                .and_then(|v| v.json::<serde_json::Value>().context("Failed to parse response"));

            if let Err(e) = r {
                // TODO: metric
                error!("Failed to forward request: {:?}", e);
            }

            return true;
        }

        false
    }

    pub fn get_current_slot(&self) -> RpcResult<u64> {
        Ok(self
            .bank
            .read()
            .map_err(|e| {
                error!("get_current_slot error: {:?}", e);
                ErrorCode::InternalError
            })?
            .get_current_slot())
    }

    pub fn get_latest_context(&self) -> RpcResult<(u64, Hash)> {
        let (slot, latest_hash, _timestamp) = self
            .bank
            .read()
            .map_err(|e| {
                error!("get_latest_context error: {:?}", e);
                ErrorCode::InternalError
            })?
            .get_latest_slot_hash_timestamp();

        Ok((slot, latest_hash))
    }

    pub fn get_block_height(&self) -> RpcResult<u64> {
        self.get_current_slot()
    }

    pub fn get_last_valid_block_height(&self) -> RpcResult<u64> {
        let slot = self.get_current_slot()?;
        // ? @shou
        let last_valid_block_height = slot.saturating_add(RECENT_BLOCKHASHES_HISTORY_SIZE as u64);

        Ok(last_valid_block_height)
    }

    pub fn is_blockhash_valid(&self, blockhash: &Hash) -> RpcResult<bool> {
        Ok(self
            .bank
            .read()
            .map_err(|e| {
                error!("is_blockhash_valid error: {:?}", e);
                ErrorCode::InternalError
            })?
            .is_blockhash_valid(blockhash))
    }

    pub async fn get_program_accounts(
        &self,
        _: &Pubkey,
        _: Vec<RpcFilterType>,
        _: Option<UiAccountEncoding>,
        _: Option<UiDataSliceConfig>,
    ) -> RpcResult<(Slot, Vec<RpcKeyedAccount>)> {
        // todo: implement
        Err(ErrorCode::InternalError.into())
    }

    pub async fn get_token_accounts_by_owner(
        &self,
        _: &Pubkey,
        _: &RpcTokenAccountsFilter,
        _: Option<UiAccountEncoding>,
        _: Option<UiDataSliceConfig>,
    ) -> RpcResult<(Slot, Vec<RpcKeyedAccount>)> {
        // todo: implement
        Err(ErrorCode::InternalError.into())
    }

    pub fn identity(&self) -> &Keypair {
        &self._ctx.identity
    }

    pub fn tpu(&self) -> &SocketAddr {
        &self._ctx.tpu
    }
}
