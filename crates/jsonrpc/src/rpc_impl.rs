use std::{borrow::Cow, collections::{BTreeMap, HashMap}, io::Write, str::FromStr, sync::atomic::Ordering};

use base64::{engine::general_purpose::STANDARD as b64, Engine};
use infinisvm_core::bank::{get_feature_set, TransactionStatus};
use infinisvm_types::{BlockWithTransactions, SignatureFilters, TransactionWithMetadata};
use jsonrpsee::{
    core::{async_trait, RpcResult, SubscriptionResult},
    proc_macros::rpc,
    types::{
        error::{INVALID_PARAMS_CODE, INVALID_PARAMS_MSG, INVALID_REQUEST_CODE},
        ErrorCode, ErrorObject, ErrorObjectOwned,
    },
    PendingSubscriptionSink, SubscriptionMessage,
};
use metrics::counter;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use solana_account_decoder::{
    parse_token::UiTokenAmount, UiAccount, UiAccountData, UiAccountEncoding, UiDataSliceConfig,
};
use solana_compute_budget_instruction::compute_budget_instruction_details::ComputeBudgetInstructionDetails;
use solana_hash::Hash;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    clock::{Epoch, Slot},
    fee_calculator::FeeCalculator,
    instruction::InstructionError,
    message::{v0::LoadedAddresses, Message, SimpleAddressLoader},
    program_utils::limited_deserialize,
    pubkey::Pubkey,
    rent::Rent,
    signature::Signature,
    signer::Signer,
    system_program,
    transaction::{MessageHash, SanitizedTransaction, Transaction, TransactionError, VersionedTransaction},
};
use solana_svm::transaction_processing_result::ProcessedTransaction;
use solana_transaction_status::{
    map_inner_instructions, Encodable, EncodedTransaction, EncodedTransactionWithStatusMeta, TransactionDetails,
    TransactionStatusMeta, TransactionWithStatusMeta, UiConfirmedBlock, UiInnerInstructions, UiReturnDataEncoding,
    UiTransactionEncoding, UiTransactionReturnData, VersionedTransactionWithStatusMeta,
};
use spl_token_2022::{extension::StateWithExtensions, state::Mint};
use thiserror::Error;
use tracing::error;

use crate::rpc_state::{RpcBank, RpcServerState};

pub const MAX_TOKEN_ACCOUNTS_QUERY_LIMIT: usize = 1 << 32;

pub fn deserialize_checked(input: &[u8]) -> Option<VersionedTransaction> {
    match limited_deserialize::<VersionedTransaction>(input) {
        Ok(tx) => {
            tx.sanitize().ok()?;
            Some(tx)
        }
        Err(_) => match limited_deserialize::<Transaction>(input) {
            Ok(tx) => Some(tx.into()),
            Err(_) => None,
        },
    }
}

pub fn sigverify(versioned_transaction: &VersionedTransaction) -> bool {
    let verified: bool = {
        let mut verification_status = true;
        for (i, sig) in versioned_transaction.signatures.iter().enumerate() {
            let verified = sig.verify(
                versioned_transaction.message.static_account_keys()[i].as_ref(),
                &versioned_transaction.message.serialize(),
            );
            if !verified {
                verification_status = false;
                break;
            }
        }
        verification_status
    };

    verified
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct RpcVersionInfo {
    /// The current version of solana-core
    pub solana_core: String,
    /// first 4 bytes of the FeatureSet identifier
    pub feature_set: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcContextConfig {
    #[serde(flatten)]
    pub commitment: Option<String>,
    pub min_context_slot: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcResponseContext {
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcResponse<T> {
    pub context: RpcResponseContext,
    pub value: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransactionConfirmationStatus {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename = "camelCase")]
pub enum TransactionExecutionStatus {
    NotFound,
    Processing,
    Success,
    Failure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionStatus {
    pub slot: u64,
    pub confirmations: Option<usize>,         // None = rooted
    pub status: Result<(), TransactionError>, // legacy field
    pub err: Option<TransactionError>,
    pub confirmation_status: Option<TransactionConfirmationStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcClusterNode {
    pub pubkey: String,
    pub gossip: String,
    pub rpc: String,
    pub tpu: String,
    pub version: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockConfig {
    pub encoding: Option<UiTransactionEncoding>,
    pub transaction_details: Option<TransactionDetails>,
    pub rewards: Option<bool>,
    #[serde(flatten)]
    pub commitment: Option<String>,
    pub max_supported_transaction_version: Option<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename = "camelCase")]
pub struct GetAccountInfoResponse {
    pub data: String,
    pub encoding: UiAccountEncoding,
    pub executable: bool,
    pub owner: String,
    pub rent_epoch: u64,
    pub lamports: u64,
    pub space: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename = "camelCase")]
pub struct TransactionResponse {
    pub signature: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename = "camelCase")]
pub struct GetSignatureStatusesResponse {
    pub statuses: Vec<TransactionExecutionStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSignaturesForAddressConfig {
    pub before: Option<String>,        // Signature as base-58 string
    pub until: Option<String>,         // Signature as base-58 string
    pub before_timestamp: Option<u64>, // Unix timestamp in milliseconds
    pub until_timestamp: Option<u64>,  // Unix timestamp in milliseconds
    pub limit: Option<usize>,
    pub detailed: Option<bool>, // Whether to return the transaction details
    #[serde(flatten)]
    pub commitment: Option<String>,
    pub min_context_slot: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcConfirmedTransactionStatusWithSignature {
    pub signature: String,
    pub slot: u64,
    pub err: Option<TransactionError>,
    pub memo: Option<String>,
    pub block_time: Option<u64>,
    pub confirmation_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<EncodedTransactionWithStatusMeta>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcRequestAirdropConfig {
    pub recent_blockhash: Option<String>, // base-58 encoded blockhash
    #[serde(flatten)]
    pub commitment: Option<String>,
}

// Keep in sync with https://github.com/solana-labs/solana-web3.js/blob/master/src/errors.ts
pub const JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP: i64 = -32001;
pub const JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE: i64 = -32002;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE: i64 = -32003;
pub const JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE: i64 = -32004;
pub const JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY: i64 = -32005;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE: i64 = -32006;
pub const JSON_RPC_SERVER_ERROR_SLOT_SKIPPED: i64 = -32007;
pub const JSON_RPC_SERVER_ERROR_NO_SNAPSHOT: i64 = -32008;
pub const JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32009;
pub const JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX: i64 = -32010;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE: i64 = -32011;
pub const JSON_RPC_SCAN_ERROR: i64 = -32012;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH: i64 = -32013;
pub const JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET: i64 = -32014;
pub const JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION: i64 = -32015;
pub const JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED: i64 = -32016;

#[derive(Error, Debug)]
pub enum RpcCustomError {
    #[error("BlockCleanedUp")]
    BlockCleanedUp { slot: Slot, first_available_block: Slot },
    #[error("SendTransactionPreflightFailure")]
    SendTransactionPreflightFailure {
        message: String,
        result: RpcSimulateTransactionResult,
    },
    #[error("TransactionSignatureVerificationFailure")]
    TransactionSignatureVerificationFailure,
    #[error("BlockNotAvailable")]
    BlockNotAvailable { slot: Slot },
    #[error("NodeUnhealthy")]
    NodeUnhealthy { num_slots_behind: Option<Slot> },
    #[error("TransactionPrecompileVerificationFailure")]
    TransactionPrecompileVerificationFailure(TransactionError),
    #[error("SlotSkipped")]
    SlotSkipped { slot: Slot },
    #[error("NoSnapshot")]
    NoSnapshot,
    #[error("LongTermStorageSlotSkipped")]
    LongTermStorageSlotSkipped { slot: Slot },
    #[error("KeyExcludedFromSecondaryIndex")]
    KeyExcludedFromSecondaryIndex { index_key: String },
    #[error("TransactionHistoryNotAvailable")]
    TransactionHistoryNotAvailable,
    #[error("ScanError")]
    ScanError { message: String },
    #[error("TransactionSignatureLenMismatch")]
    TransactionSignatureLenMismatch,
    #[error("BlockStatusNotAvailableYet")]
    BlockStatusNotAvailableYet { slot: Slot },
    #[error("UnsupportedTransactionVersion")]
    UnsupportedTransactionVersion(u8),
    #[error("MinContextSlotNotReached")]
    MinContextSlotNotReached { context_slot: Slot },
    #[error("Airdrop timed out")]
    AirdropTimeout,
    #[error("TransactionAlreadyProcessed")]
    TransactionAlreadyProcessed,
    #[error("TransactionBlockhashNotFound")]
    TransactionBlockhashNotFound,
    #[error("TransactionSimulationFailure")]
    TransactionSimulationFailure(String),
    #[error("ForwardingError")]
    ForwardingError(String),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeUnhealthyErrorData {
    pub num_slots_behind: Option<Slot>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MinContextSlotNotReachedErrorData {
    pub context_slot: Slot,
}

#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum EncodeError {
    #[error("Encoding does not support transaction version {0}")]
    UnsupportedTransactionVersion(u8),
}

impl From<EncodeError> for RpcCustomError {
    fn from(err: EncodeError) -> Self {
        match err {
            EncodeError::UnsupportedTransactionVersion(version) => Self::UnsupportedTransactionVersion(version),
        }
    }
}

impl From<RpcCustomError> for ErrorObjectOwned {
    fn from(e: RpcCustomError) -> Self {
        match e {
            RpcCustomError::BlockCleanedUp {
                slot,
                first_available_block,
            } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP as i32,
                format!(
                    "Block {slot} cleaned up, does not exist on node. First available block: {first_available_block}",
                ),
                None::<()>,
            ),
            RpcCustomError::SendTransactionPreflightFailure { message, result } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE as i32,
                message,
                Some(serde_json::json!(result)),
            ),
            RpcCustomError::TransactionSignatureVerificationFailure => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE as i32,
                "Transaction signature verification failure".to_string(),
                None::<()>,
            ),
            RpcCustomError::BlockNotAvailable { slot } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE as i32,
                format!("Block not available for slot {slot}"),
                None::<()>,
            ),
            RpcCustomError::NodeUnhealthy { num_slots_behind } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY as i32,
                if let Some(num_slots_behind) = num_slots_behind {
                    format!("Node is behind by {num_slots_behind} slots")
                } else {
                    "Node is unhealthy".to_string()
                },
                Some(serde_json::json!(NodeUnhealthyErrorData { num_slots_behind })),
            ),
            RpcCustomError::TransactionPrecompileVerificationFailure(e) => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE as i32,
                format!("Transaction precompile verification failure {e:?}"),
                None::<()>,
            ),
            RpcCustomError::SlotSkipped { slot } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_SLOT_SKIPPED as i32,
                format!("Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"),
                None::<()>,
            ),
            RpcCustomError::NoSnapshot => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_NO_SNAPSHOT as i32,
                "No snapshot".to_string(),
                None::<()>,
            ),
            RpcCustomError::LongTermStorageSlotSkipped { slot } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED as i32,
                format!("Slot {slot} was skipped, or missing in long-term storage"),
                None::<()>,
            ),
            RpcCustomError::KeyExcludedFromSecondaryIndex { index_key } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX as i32,
                format!(
                    "{index_key} excluded from account secondary indexes; \
                    this RPC method unavailable for key"
                ),
                None::<()>,
            ),
            RpcCustomError::TransactionHistoryNotAvailable => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE as i32,
                "Transaction history is not available from this node".to_string(),
                None::<()>,
            ),

            RpcCustomError::ScanError { message } => {
                ErrorObject::owned(JSON_RPC_SCAN_ERROR as i32, message, None::<()>)
            }
            RpcCustomError::TransactionSignatureLenMismatch => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH as i32,
                "Transaction signature length mismatch".to_string(),
                None::<()>,
            ),

            RpcCustomError::BlockStatusNotAvailableYet { slot } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET as i32,
                format!("Block status not yet available for slot {slot}"),
                None::<()>,
            ),
            RpcCustomError::UnsupportedTransactionVersion(version) => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION as i32,
                format!(
                    "Transaction version ({version}) is not supported by the requesting client. \
                    Please try the request again with the following configuration parameter: \
                    \"maxSupportedTransactionVersion\": {version}"
                ),
                None::<()>,
            ),
            RpcCustomError::MinContextSlotNotReached { context_slot } => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32,
                "Minimum context slot has not been reached".to_string(),
                Some(serde_json::json!(MinContextSlotNotReachedErrorData { context_slot })),
            ),
            RpcCustomError::AirdropTimeout => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE as i32,
                "Airdrop timed out".to_string(),
                None::<()>,
            ),
            RpcCustomError::TransactionAlreadyProcessed => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE as i32,
                "Transaction already processed".to_string(),
                None::<()>,
            ),
            RpcCustomError::TransactionBlockhashNotFound => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE as i32,
                "Transaction blockhash not found".to_string(),
                None::<()>,
            ),
            RpcCustomError::TransactionSimulationFailure(message) => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE as i32,
                message,
                None::<()>,
            ),
            RpcCustomError::ForwardingError(message) => ErrorObject::owned(
                JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE as i32,
                message,
                None::<()>,
            ),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcAccountInfoConfig {
    pub encoding: Option<UiAccountEncoding>,
    pub data_slice: Option<UiDataSliceConfig>,
    #[serde(flatten)]
    pub commitment: Option<String>,
    pub min_context_slot: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionConfig {
    pub encoding: Option<UiTransactionEncoding>,
    #[serde(flatten)]
    pub commitment: Option<String>,
    pub max_supported_transaction_version: Option<u8>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSendTransactionConfig {
    #[serde(default)]
    pub skip_preflight: bool,
    pub preflight_commitment: Option<String>,
    pub encoding: Option<UiTransactionEncoding>,
    pub max_retries: Option<usize>,
    pub min_context_slot: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockhash {
    pub blockhash: String,
    pub last_valid_block_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSignatureStatusConfig {
    pub search_transaction_history: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcTokenAccountsFilter {
    Mint(String),
    ProgramId(String),
}


#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DelegateSearch {
    pub mint: String,
    pub program_id: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EpochInfo {
    /// The current epoch
    pub epoch: u64,

    /// The current slot, relative to the start of the current epoch
    pub slot_index: u64,

    /// The number of slots in this epoch
    pub slots_in_epoch: u64,

    /// The absolute current slot
    pub absolute_slot: u64,

    /// The current block height
    pub block_height: u64,

    /// Total number of transactions processed without error since genesis
    pub transaction_count: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSimulateTransactionAccountsConfig {
    pub encoding: Option<UiAccountEncoding>,
    pub addresses: Vec<String>,
    pub data_slice: Option<UiDataSliceConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionLogsConfig {
    #[serde(flatten)]
    pub commitment: Option<CommitmentConfig>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSimulateTransactionConfig {
    #[serde(default)]
    pub sig_verify: bool,
    #[serde(default)]
    pub replace_recent_blockhash: bool,
    #[serde(flatten)]
    pub commitment: Option<String>,
    pub encoding: Option<UiTransactionEncoding>,
    pub accounts: Option<RpcSimulateTransactionAccountsConfig>,
    pub min_context_slot: Option<u64>,
    #[serde(default)]
    pub inner_instructions: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcSimulateTransactionResult {
    pub err: Option<TransactionError>,
    pub logs: Option<Vec<String>>,
    pub accounts: Option<Vec<Option<UiAccount>>>,
    pub units_consumed: Option<u64>,
    pub return_data: Option<UiTransactionReturnData>,
    pub inner_instructions: Option<Vec<UiInnerInstructions>>,
    pub replacement_blockhash: Option<RpcBlockhash>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcPerfSample {
    pub slot: Slot,
    pub num_transactions: u64,
    pub num_non_vote_transactions: Option<u64>,
    pub num_slots: u64,
    pub sample_period_secs: u16,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcGetMinimumBalanceForRentExemptionConfig {
    #[serde(flatten)]
    pub commitment: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransformedAccountDataConfig {
    pub package: String,
    pub command: String,
    pub params: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcParseAccountDataConfig {
    pub account: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockSubscribeConfig {
    pub encoding: Option<UiTransactionEncoding>,
    pub transaction_details: Option<TransactionDetails>,
    pub show_rewards: Option<bool>,
    pub max_supported_transaction_version: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcBlockSubscribeFilter {
    All,
    MentionsAccountOrProgram(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcTransactionLogsFilter {
    All,
    AllWithVotes,
    Mentions(Vec<String>), // base58-encoded list of addresses
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSignatureSubscribeConfig {
    pub enable_received_notification: Option<bool>,
}

const MAX_DATA_SIZE: usize = 128;
const MAX_DATA_BASE58_SIZE: usize = 175;
const MAX_DATA_BASE64_SIZE: usize = 172;

#[derive(Error, PartialEq, Eq, Debug)]
pub enum RpcFilterError {
    #[error("encoded binary data should be less than 129 bytes")]
    DataTooLarge,
    #[error("base58 decode error")]
    Base58DecodeError(#[from] bs58::decode::Error),
    #[error("base64 decode error")]
    Base64DecodeError(#[from] base64::DecodeError),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcFilterType {
    DataSize(u64),
    Memcmp(Memcmp),
    TokenAccountState,
}
impl RpcFilterType {
    pub fn allows(&self, account: &AccountSharedData) -> bool {
        match self {
            RpcFilterType::DataSize(size) => account.data().len() as u64 == *size,
            RpcFilterType::Memcmp(compare) => compare.bytes_match(account.data()),
            // RpcFilterType::TokenAccountState => Account::valid_account_data(account.data()),
            // TODO: Currently uses token2022, figure out why and if we need this.
            RpcFilterType::TokenAccountState => true,
        }
    }

    #[allow(deprecated)]
    pub fn valid(&self) -> Result<(), RpcFilterError> {
        match self {
            RpcFilterType::DataSize(_) => Ok(()),
            RpcFilterType::Memcmp(compare) => {
                use MemcmpEncodedBytes::*;
                match &compare.bytes {
                    Binary(bytes) | Base58(bytes) => {
                        if bytes.len() > MAX_DATA_BASE58_SIZE {
                            return Err(RpcFilterError::DataTooLarge);
                        }
                        let bytes = bs58::decode(&bytes).into_vec()?;
                        if bytes.len() > MAX_DATA_SIZE {
                            Err(RpcFilterError::DataTooLarge)
                        } else {
                            Ok(())
                        }
                    }
                    Base64(bytes) => {
                        if bytes.len() > MAX_DATA_BASE64_SIZE {
                            return Err(RpcFilterError::DataTooLarge);
                        }
                        let bytes = base64::decode(bytes)?;
                        if bytes.len() > MAX_DATA_SIZE {
                            Err(RpcFilterError::DataTooLarge)
                        } else {
                            Ok(())
                        }
                    }
                    Bytes(bytes) => {
                        if bytes.len() > MAX_DATA_SIZE {
                            return Err(RpcFilterError::DataTooLarge);
                        }
                        Ok(())
                    }
                }
            }
            RpcFilterType::TokenAccountState => Ok(()),
        }
    }
}

// Internal struct used to specify explicit Base58 and Base64 encoding
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcMemcmpEncoding {
    Base58,
    Base64,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum MemcmpEncodedBytes {
    Binary(String),
    Base58(String),
    Base64(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemcmpEncoding {
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Memcmp {
    /// Data offset to begin match
    pub offset: usize,
    /// Bytes, encoded with specified encoding, or default Binary
    pub bytes: MemcmpEncodedBytes,
}

impl Memcmp {
    pub fn new(offset: usize, encoded_bytes: MemcmpEncodedBytes) -> Self {
        Self {
            offset,
            bytes: encoded_bytes,
        }
    }

    pub fn new_raw_bytes(offset: usize, bytes: Vec<u8>) -> Self {
        Self {
            offset,
            bytes: MemcmpEncodedBytes::Bytes(bytes),
        }
    }

    pub fn new_base58_encoded(offset: usize, bytes: &[u8]) -> Self {
        Self {
            offset,
            bytes: MemcmpEncodedBytes::Base58(bs58::encode(bytes).into_string()),
        }
    }
    pub fn bytes(&self) -> Option<Cow<Vec<u8>>> {
        use MemcmpEncodedBytes::*;
        match &self.bytes {
            Binary(bytes) | Base58(bytes) => bs58::decode(bytes).into_vec().ok().map(Cow::Owned),
            Base64(bytes) => b64.decode(bytes).ok().map(Cow::Owned),
            Bytes(bytes) => Some(Cow::Borrowed(bytes)),
        }
    }

    pub fn bytes_match(&self, data: &[u8]) -> bool {
        match self.bytes() {
            Some(bytes) => {
                if self.offset > data.len() {
                    return false;
                }
                if data[self.offset..].len() < bytes.len() {
                    return false;
                }
                data[self.offset..self.offset + bytes.len()] == bytes[..]
            }
            None => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptionalContext<T> {
    Context(Response<T>),
    NoContext(T),
}

impl<T> OptionalContext<T> {
    pub fn parse_value(self) -> T {
        match self {
            Self::Context(response) => response.value,
            Self::NoContext(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response<T> {
    pub context: RpcResponseContext,
    pub value: T,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcProgramAccountsConfig {
    pub filters: Option<Vec<RpcFilterType>>,
    #[serde(flatten)]
    pub account_config: RpcAccountInfoConfig,
    pub with_context: Option<bool>,
    // TODO: This functionality is currently not implemented.
    pub sort_results: Option<bool>,
}

#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct CommitmentConfig {
    pub commitment: CommitmentLevel,
}

impl CommitmentConfig {
    pub const fn finalized() -> Self {
        Self {
            commitment: CommitmentLevel::Finalized,
        }
    }

    pub const fn confirmed() -> Self {
        Self {
            commitment: CommitmentLevel::Confirmed,
        }
    }

    pub const fn processed() -> Self {
        Self {
            commitment: CommitmentLevel::Processed,
        }
    }

    pub fn ok(self) -> Option<Self> {
        if self == Self::default() {
            None
        } else {
            Some(self)
        }
    }

    pub fn is_finalized(&self) -> bool {
        self.commitment == CommitmentLevel::Finalized
    }

    pub fn is_confirmed(&self) -> bool {
        self.commitment == CommitmentLevel::Confirmed
    }

    pub fn is_processed(&self) -> bool {
        self.commitment == CommitmentLevel::Processed
    }

    pub fn is_at_least_confirmed(&self) -> bool {
        self.is_confirmed() || self.is_finalized()
    }

    pub fn use_deprecated_commitment(commitment: CommitmentConfig) -> Self {
        commitment
    }
}

impl FromStr for CommitmentConfig {
    type Err = ParseCommitmentLevelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CommitmentLevel::from_str(s).map(|commitment| Self { commitment })
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
/// An attribute of a slot. It describes how finalized a block is at some point
/// in time. For example, a slot is said to be at the max level immediately
/// after the cluster recognizes the block at that slot as finalized. When
/// querying the ledger state, use lower levels of commitment to report progress
/// and higher levels to ensure state changes will not be rolled back.
pub enum CommitmentLevel {
    /// The highest slot of the heaviest fork processed by the node. Ledger
    /// state at this slot is not derived from a confirmed or finalized
    /// block, but if multiple forks are present, is from the fork the
    /// validator believes is most likely to finalize.
    Processed,

    /// The highest slot that has been voted on by supermajority of the cluster,
    /// ie. is confirmed. Confirmation incorporates votes from gossip and
    /// replay. It does not count votes on descendants of a block, only
    /// direct votes on that block, and upholds "optimistic confirmation"
    /// guarantees in release 1.3 and onwards.
    Confirmed,

    /// The highest slot having reached max vote lockout, as recognized by a
    /// supermajority of the cluster.
    Finalized,
}

impl Default for CommitmentLevel {
    fn default() -> Self {
        Self::Finalized
    }
}

impl FromStr for CommitmentLevel {
    type Err = ParseCommitmentLevelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "processed" => Ok(CommitmentLevel::Processed),
            "confirmed" => Ok(CommitmentLevel::Confirmed),
            "finalized" => Ok(CommitmentLevel::Finalized),
            _ => Err(ParseCommitmentLevelError::Invalid),
        }
    }
}

impl std::fmt::Display for CommitmentLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = match self {
            CommitmentLevel::Processed => "processed",
            CommitmentLevel::Confirmed => "confirmed",
            CommitmentLevel::Finalized => "finalized",
        };
        write!(f, "{s}")
    }
}

#[derive(Error, Debug)]
pub enum ParseCommitmentLevelError {
    #[error("invalid variant")]
    Invalid,
}

// impl From<GetSignaturesForAddressResponse> for
// RpcConfirmedTransactionStatusWithSignature {     fn from(value:
// GetSignaturesForAddressResponse) -> Self {         Self {
//             signature: value.signature,
//             slot: value.slot,
//             err: value.err,
//             memo: value.memo,
//             block_time: value.block_time,
//             confirmation_status: value.confirmation_status,
//         }
//     }
// }
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionStatusResponse {
    pub signature: String, // Signature as base58 string
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcApiVersion(semver::Version);

impl std::ops::Deref for RpcApiVersion {
    type Target = semver::Version;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for RpcApiVersion {
    fn default() -> Self {
        Self(semver::Version::new(2, 0, 0))
    }
}

impl Serialize for RpcApiVersion {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RpcApiVersion {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Ok(RpcApiVersion(
            semver::Version::from_str(&s).map_err(serde::de::Error::custom)?,
        ))
    }
}

impl RpcResponseContext {
    pub fn new(slot: Slot) -> Self {
        Self { slot }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockCommitment<T> {
    pub commitment: Option<T>,
    pub total_stake: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockhashFeeCalculator {
    pub blockhash: String,
    pub fee_calculator: FeeCalculator,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcFeeCalculator {
    pub fee_calculator: FeeCalculator,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotInfo {
    pub slot: Slot,
    pub parent: Slot,
    pub root: Slot,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SlotTransactionStats {
    pub num_transaction_entries: u64,
    pub num_successful_transactions: u64,
    pub num_failed_transactions: u64,
    pub max_transactions_per_entry: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SlotUpdate {
    FirstShredReceived {
        slot: Slot,
        timestamp: u64,
    },
    Completed {
        slot: Slot,
        timestamp: u64,
    },
    CreatedBank {
        slot: Slot,
        parent: Slot,
        timestamp: u64,
    },
    Frozen {
        slot: Slot,
        timestamp: u64,
        stats: SlotTransactionStats,
    },
    Dead {
        slot: Slot,
        timestamp: u64,
        err: String,
    },
    OptimisticConfirmation {
        slot: Slot,
        timestamp: u64,
    },
    Root {
        slot: Slot,
        timestamp: u64,
    },
}

impl SlotUpdate {
    pub fn slot(&self) -> Slot {
        match self {
            Self::FirstShredReceived { slot, .. } => *slot,
            Self::Completed { slot, .. } => *slot,
            Self::CreatedBank { slot, .. } => *slot,
            Self::Frozen { slot, .. } => *slot,
            Self::Dead { slot, .. } => *slot,
            Self::OptimisticConfirmation { slot, .. } => *slot,
            Self::Root { slot, .. } => *slot,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", untagged)]
pub enum RpcSignatureResult {
    ProcessedSignature(ProcessedSignatureResult),
    ReceivedSignature(ReceivedSignatureResult),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcLogsResponse {
    pub signature: String, // Signature as base58 string
    pub err: Option<TransactionError>,
    pub logs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedSignatureResult {
    pub err: Option<TransactionError>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ReceivedSignatureResult {
    ReceivedSignature,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockProductionRange {
    pub first_slot: Slot,
    pub last_slot: Slot,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcEpochSchedule {
    pub first_normal_epoch: Epoch,
    pub first_normal_slot: Slot,
    pub leader_schedule_slot_offset: u64,
    pub slots_per_epoch: u64,
    pub warmup: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockProduction {
    /// Map of leader base58 identity pubkeys to a tuple of `(number of leader
    /// slots, number of blocks produced)`
    pub by_identity: HashMap<String, (usize, usize)>,
    pub range: RpcBlockProductionRange,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct RpcIdentity {
    /// The current node identity pubkey
    pub identity: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcSignatureConfirmation {
    pub confirmations: usize,
    pub status: solana_sdk::transaction::Result<()>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcAccountBalance {
    pub address: String,
    pub lamports: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcSupply {
    pub total: u64,
    pub circulating: u64,
    pub non_circulating: u64,
    pub non_circulating_accounts: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RpcTokenAccountBalance {
    pub address: String,
    #[serde(flatten)]
    pub amount: UiTokenAmount,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcSnapshotSlotInfo {
    pub full: Slot,
    pub incremental: Option<Slot>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcPrioritizationFee {
    pub slot: Slot,
    pub prioritization_fee: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockUpdate {
    pub slot: Slot,
    pub block: Option<UiConfirmedBlock>,
    pub err: Option<RpcBlockUpdateError>,
}

#[derive(Clone, Deserialize, Serialize, Debug, Error, Eq, PartialEq)]
pub enum RpcBlockUpdateError {
    #[error("block store error")]
    BlockStoreError,

    #[error("unsupported transaction version ({0})")]
    UnsupportedTransactionVersion(u8),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcKeyedAccount {
    pub pubkey: String,
    pub account: UiAccount,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RpcTokenAccountValueOnly {
    pub address: String,
    pub amount: String,
    pub decimals: u8,
    pub ui_amount: Option<f64>,
    pub ui_amount_string: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcVote {
    /// Vote account address, as base-58 encoded string
    pub vote_pubkey: String,
    pub slots: Vec<Slot>,
    pub hash: String,
    pub timestamp: Option<u64>,
    pub signature: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EncodedConfirmedTransactionWithStatusMeta {
    pub slot: u64,
    #[serde(flatten)]
    pub transaction: EncodedTransactionWithStatusMeta,
    pub block_time: Option<i64>,
}

#[rpc(server)]
pub trait Rpc {
    #[method(name = "getVersion")]
    fn get_version(&self) -> RpcResult<RpcVersionInfo>;

    #[method(name = "getBalance")]
    fn get_balance(&self, pubkey_str: String, config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<u64>>;

    #[method(name = "getBlock")]
    async fn get_block(
        &self,
        slot: Slot,
        config: Option<RpcBlockConfig>,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> RpcResult<Option<UiConfirmedBlock>>;

    #[method(name = "getFirstAvailableBlock")]
    fn get_first_available_block(&self) -> RpcResult<Slot>;

    #[method(name = "getBlocks")]
    async fn get_blocks(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
        commitment: Option<HashMap<String, String>>, /* Hack. For some reason, solana client connection default
                                                      * sends a map for this call */
    ) -> RpcResult<Vec<Slot>>;

    #[method(name = "getBlockTime")]
    async fn get_block_time(&self, slot: Slot) -> RpcResult<Option<u64>>;

    #[method(name = "getAccountInfo")]
    fn get_account_info(
        &self,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Option<UiAccount>>>;

    #[method(name = "sendTransaction")]
    async fn send_transaction(&self, data: String, config: Option<RpcSendTransactionConfig>) -> RpcResult<String>;

    #[method(name = "getLatestBlockhash")]
    fn get_latest_blockhash(&self, config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<RpcBlockhash>>;

    #[method(name = "getGenesisHash")]
    fn get_genesis_hash(&self) -> RpcResult<String>;

    #[method(name = "getEpochSchedule")]
    fn get_epoch_schedule(&self) -> RpcResult<RpcEpochSchedule>;

    #[method(name = "isBlockhashValid")]
    fn is_blockhash_valid(&self, blockhash: String, config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<bool>>;

    #[method(name = "getFeeForMessage")]
    fn get_fee_for_message(
        &self,
        data: String,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<RpcResponse<Option<u64>>>;

    #[method(name = "getRecentPrioritizationFees")]
    fn get_recent_prioritization_fees(&self, pubkey_strs: Option<Vec<String>>) -> RpcResult<Vec<RpcPrioritizationFee>>;

    #[method(name = "getStakeMinimumDelegation")]
    fn get_stake_minimum_delegation(&self, config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<u64>>;

    #[method(name = "getSignatureStatuses")]
    async fn get_signature_statuses(
        &self,
        signature_strs: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<RpcTransactionStatus>>>>;

    #[method(name = "getMinimumBalanceForRentExemption")]
    fn get_minimum_balance_for_rent_exemption(
        &self,
        data_len: usize,
        config: Option<RpcGetMinimumBalanceForRentExemptionConfig>,
    ) -> RpcResult<u64>;

    #[method(name = "getSlot")]
    fn get_slot(&self, config: Option<RpcContextConfig>) -> RpcResult<u64>;

    #[method(name = "getEpochInfo")]
    fn get_epoch_info(&self, config: Option<RpcContextConfig>) -> RpcResult<EpochInfo>;

    #[method(name = "getSlotLeaders")]
    fn get_slot_leaders(&self, start_slot: u64, limit: u64) -> RpcResult<Vec<String>>;

    #[method(name = "getClusterNodes")]
    fn get_cluster_nodes(&self) -> RpcResult<Vec<RpcClusterNode>>;

    #[method(name = "getBlockHeight")]
    fn get_block_height(&self, config: Option<RpcContextConfig>) -> RpcResult<u64>;

    #[method(name = "getProgramAccounts")]
    async fn get_program_accounts(
        &self,
        program_id_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<OptionalContext<Vec<RpcKeyedAccount>>>;

    #[method(name = "getTokenAccountBalance")]
    fn get_token_account_balance(
        &self,
        pubkey_str: String,
        config: Option<serde_json::Value>,
    ) -> RpcResult<RpcResponse<UiTokenAmount>>;

    #[method(name = "getTokenAccountsByDelegate")]
    async fn get_token_accounts_by_delegate(
        &self,
        pubkey: String,
        needle: DelegateSearch,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<RpcKeyedAccount>>>;

    #[method(name = "getTokenLargestAccounts")]
    async fn get_token_largest_accounts(
        &self,
        mint: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<RpcTokenAccountValueOnly>>>;

    #[method(name = "getTokenSupply")]
    fn get_token_supply(
        &self,
        mint_str: String,
        config: Option<serde_json::Value>,
    ) -> RpcResult<RpcResponse<UiTokenAmount>>;

    #[method(name = "getTokenAccountsByOwner")]
    async fn get_token_accounts_by_owner(
        &self,
        owner_str: String,
        filter: RpcTokenAccountsFilter,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<RpcKeyedAccount>>>;

    #[method(name = "getMultipleAccounts")]
    async fn get_multiple_accounts(
        &self,
        pubkey_strs: Vec<String>,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<UiAccount>>>>;

    #[method(name = "getTransaction")]
    async fn get_transaction(
        &self,
        signature_str: String,
        config: Option<RpcTransactionConfig>,
    ) -> RpcResult<Option<EncodedConfirmedTransactionWithStatusMeta>>;

    #[method(name = "getSignaturesForAddress")]
    async fn get_signatures_for_address(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>>;

    // DEPRECATED
    #[method(name = "getConfirmedSignaturesForAddress2")]
    async fn get_confirmed_signatures_for_address2(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>>;

    #[method(name = "simulateTransaction")]
    async fn simulate_transaction(
        &self,
        data: String,
        config: Option<RpcSimulateTransactionConfig>,
    ) -> RpcResult<RpcResponse<RpcSimulateTransactionResult>>;

    #[method(name = "getTransactionCount")]
    fn get_transaction_count(&self) -> RpcResult<u64>;

    #[method(name = "getRecentPerformanceSamples")]
    fn get_recent_performance_samples(&self, limit: u64) -> RpcResult<Vec<RpcPerfSample>>;

    #[method(name = "requestAirdrop")]
    fn request_airdrop(&self, pubkey_str: String, lamports: u64) -> RpcResult<String>;

    /////// BEGIN WeBSocket RPC ///////

    // Get notification every time account data is changed
    // Accepts pubkey parameter as base-58 encoded string
    #[subscription(name = "accountSubscribe", unsubscribe = "accountUnsubscribe", item = RpcResponse<UiAccount>)]
    async fn account_subscribe(&self, pubkey_str: String, config: Option<RpcAccountInfoConfig>) -> SubscriptionResult;

    // Get notification every time account data owned by a particular program is
    // changed Accepts pubkey parameter as base-58 encoded string
    // #[subscription(name = "programSubscribe", unsubscribe = "programUnsubscribe",
    // item = RpcResponse<RpcKeyedAccount>)] async fn program_subscribe(
    // 	&self,
    // 	pubkey_str: String,
    // 	config: Option<RpcProgramAccountsConfig>,
    // ) -> SubscriptionResult;

    // Get logs for all transactions that reference the specified address
    // #[subscription(name = "logsSubscribe", unsubscribe = "logsUnsubscribe", item
    // = RpcResponse<RpcLogsResponse>)] async fn logs_subscribe(
    // 	&self,
    // 	filter: RpcTransactionLogsFilter,
    // 	config: Option<RpcTransactionLogsConfig>,
    // ) -> SubscriptionResult;

    // Get notification when signature is verified
    // Accepts signature parameter as base-58 encoded string
    #[subscription(name = "signatureSubscribe", unsubscribe = "signatureUnsubscribe", item = RpcResponse<RpcSignatureResult>)]
    async fn signature_subscribe(
        &self,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> SubscriptionResult;

    // Get notification when slot is encountered
    #[subscription(name = "slotSubscribe", unsubscribe = "slotUnsubscribe", item = SlotInfo)]
    async fn slot_subscribe(&self) -> SubscriptionResult;

    // Get series of updates for all slots
    // #[subscription(name = "slotsUpdatesSubscribe", unsubscribe =
    // "slotsUpdatesUnsubscribe", item = Arc<SlotUpdate>)]
    // async fn slots_updates_subscribe(&self) -> SubscriptionResult;

    // Subscribe to block data and content
    #[subscription(name = "blockSubscribe", unsubscribe = "blockUnsubscribe", item = Arc<RpcBlockUpdate>)]
    async fn block_subscribe(
        &self,
        filter: RpcBlockSubscribeFilter,
        config: Option<RpcBlockSubscribeConfig>,
    ) -> SubscriptionResult;

    // Get notification when a new root is set
    #[subscription(name = "rootSubscribe", unsubscribe = "rootUnsubscribe", item = Slot)]
    async fn root_subscribe(&self) -> SubscriptionResult;
}

pub fn verify_pubkey(input: &str) -> RpcResult<Pubkey> {
    Pubkey::from_str(input).map_err(|_| invalid_params_error("Invalid pubkey"))
}

pub fn verify_signature(input: &str) -> RpcResult<Signature> {
    Signature::from_str(input).map_err(|_| invalid_params_error("Invalid signature"))
}

fn real_number_string(amount: u64, decimals: u8) -> String {
    let decimals = decimals as usize;
    if decimals > 0 {
        // Left-pad zeros to decimals + 1, so we at least have an integer zero
        let mut s = format!("{:01$}", amount, decimals + 1);
        // Add the decimal point (Sorry, "," locales!)
        s.insert(s.len() - decimals, '.');
        s
    } else {
        amount.to_string()
    }
}
fn real_number_string_trimmed(amount: u64, decimals: u8) -> String {
    let mut s = real_number_string(amount, decimals);
    if decimals > 0 {
        let zeros_trimmed = s.trim_end_matches('0');
        s = zeros_trimmed.trim_end_matches('.').to_string();
    }
    s
}

fn token_amount_to_ui_amount(amount: u64, decimals: u8) -> UiTokenAmount {
    let ui_amount = 10_usize
        .checked_pow(decimals as u32)
        .map(|dividend| amount as f64 / dividend as f64);
    let ui_amount_string = real_number_string_trimmed(amount, decimals);

    UiTokenAmount {
        ui_amount,
        decimals,
        amount: amount.to_string(),
        ui_amount_string,
    }
}

pub(crate) fn invalid_params_error(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(
        INVALID_PARAMS_CODE,
        format!("{}: {}", INVALID_PARAMS_MSG, message.into()),
        None::<()>,
    )
}

pub(crate) fn invalid_request_error(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(
        INVALID_REQUEST_CODE,
        format!("{}: {}", INVALID_REQUEST_CODE, message.into()),
        None::<()>,
    )
}

fn get_mint_decimals(data: &[u8]) -> RpcResult<u8> {
    StateWithExtensions::<Mint>::unpack(data)
        .map_err(|_| invalid_params_error("Token mint could not be unpacked"))
        .map(|mint| mint.base.decimals)
}

pub fn get_account_data_encoding_and_slice(
    config: &Option<RpcAccountInfoConfig>,
    account_arc: &AccountSharedData,
) -> RpcResult<(String, UiAccountEncoding)> {
    if let Some(config) = config {
        let data = match config.data_slice {
            Some(data_slice) => account_arc.data()[data_slice.offset..(data_slice.offset + data_slice.length)].to_vec(),
            None => account_arc.data().to_vec(),
        };
        let encoded_data = match config.encoding {
            Some(encoding) => match encoding {
                UiAccountEncoding::Base58 => (bs58::encode(data).into_string(), encoding),
                UiAccountEncoding::Base64 => (b64.encode(data), encoding),
                UiAccountEncoding::Base64Zstd => {
                    let mut zstd_encoder = zstd::stream::write::Encoder::new(Vec::new(), 0).map_err(|e| {
                        error!("get_account_data_encoding_and_slice error: {:?}", e);
                        ErrorCode::InternalError
                    })?;
                    if let Ok(zstd_data) = zstd_encoder.write_all(&data).and_then(|()| zstd_encoder.finish()) {
                        (b64.encode(zstd_data), encoding)
                    } else {
                        return Ok((b64.encode(data), UiAccountEncoding::Base64));
                    }
                }
                _ => {
                    return Ok((b64.encode(data), UiAccountEncoding::Base64));
                }
            },
            None => (b64.encode(data), UiAccountEncoding::Base64),
        };
        Ok(encoded_data)
    } else {
        Ok((b64.encode(account_arc.data()), UiAccountEncoding::Base64))
    }
}

pub fn to_ui_account(config: &Option<RpcAccountInfoConfig>, a: &AccountSharedData) -> RpcResult<UiAccount> {
    let (encoded_sliced_data, encoding) = get_account_data_encoding_and_slice(config, a)?;
    Ok(UiAccount {
        lamports: a.lamports(),
        data: UiAccountData::Binary(encoded_sliced_data, encoding),
        owner: a.owner().to_string(),
        executable: a.executable(),
        rent_epoch: a.rent_epoch(),
        space: Some(a.data().len() as u64),
    })
}

pub fn to_ui_block(config: Option<RpcBlockConfig>, block_data: BlockWithTransactions) -> RpcResult<UiConfirmedBlock> {
    let config = config.unwrap_or_default();
    let transaction_detail_level = config.transaction_details.unwrap_or(TransactionDetails::Full);
    let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);

    let (transactions, signatures) = match transaction_detail_level {
        TransactionDetails::Full => {
            let transactions = block_data.transactions;

            // Map StoredTransations into EncodedTransactionWithStatusMeta
            let transactions = transactions
                .into_iter()
                .map(|tx| {
                    let tx = TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                        transaction: tx.transaction,
                        meta: tx.metadata,
                    });
                    tx.encode(
                        encoding,
                        config.max_supported_transaction_version,
                        config.rewards.unwrap_or(false),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    error!("get_block error: {:?}", e);
                    ErrorCode::InternalError
                })?;

            Ok((Some(transactions), None))
        }
        TransactionDetails::Signatures => {
            let signatures = Some(block_data.signatures.iter().map(|s| s.to_string()).collect());
            Ok((None, signatures))
        }
        TransactionDetails::None => Ok((None, None)),
        TransactionDetails::Accounts => Err(invalid_params_error("TransactionDetails::Accounts is not supported")),
    }?;

    Ok(UiConfirmedBlock {
        previous_blockhash: block_data.parent_blockhash,
        blockhash: block_data.blockhash,
        parent_slot: block_data.parent_slot,
        transactions,
        signatures,
        rewards: None,
        block_time: Some(block_data.block_unix_timestamp as i64),
        block_height: Some(block_data.slot),
        num_reward_partitions: None,
    })
}

#[async_trait]
impl RpcServer for RpcServerState {
    fn get_version(&self) -> RpcResult<RpcVersionInfo> {
        counter!("rpc", "method" => "getVersion").increment(1);
        // Return a dummy value for now
        Ok(RpcVersionInfo {
            solana_core: "2.0.0".to_string(),
            feature_set: None,
        })
    }

    fn get_balance(&self, pubkey_str: String, _config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<u64>> {
        counter!("rpc", "method" => "getBalance").increment(1);
        let slot = self.get_current_slot()?;

        let pubkey = verify_pubkey(&pubkey_str)?;

        let lamports = self
            .db
            .read()
            .map_err(|e| {
                error!("get_balance error: {:?}", e);
                ErrorCode::InternalError
            })?
            .get_account(pubkey)
            .expect("Failed to get account")
            .map_or(0, |account| account.lamports());

        let rpc_response = RpcResponse::<u64> {
            context: RpcResponseContext { slot },
            value: lamports,
        };
        Ok(rpc_response)
    }

    async fn get_block(
        &self,
        slot: Slot,
        config: Option<RpcBlockConfig>,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> RpcResult<Option<UiConfirmedBlock>> {
        counter!("rpc", "method" => "getBlock").increment(1);
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(100);

        let block_data = self
            .indexer
            .get_block_with_transactions(slot, offset, limit)
            .await
            .map_err(|err| {
                error!("get_block error: {:?}", err);
                ErrorCode::InternalError
            })?;

        if block_data.is_none() {
            return Ok(None);
        }

        let block_data = block_data.unwrap();

        let ui_confirmed_block = to_ui_block(config, block_data)?;
        Ok(Some(ui_confirmed_block))
    }

    fn get_first_available_block(&self) -> RpcResult<Slot> {
        counter!("rpc", "method" => "getFirstAvailableBlock").increment(1);
        Ok(0)
    }

    async fn get_blocks(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
        _commitment: Option<HashMap<String, String>>,
    ) -> RpcResult<Vec<Slot>> {
        counter!("rpc", "method" => "getBlocks").increment(1);
        let current_slot = self.get_current_slot()?;
        let end_slot = end_slot.unwrap_or(current_slot);

        let blocks = (start_slot..end_slot.min(current_slot)).collect();

        Ok(blocks)
    }

    async fn get_block_time(&self, slot: Slot) -> RpcResult<Option<u64>> {
        counter!("rpc", "method" => "getBlockTime").increment(1);
        let (current_slot, _, timestamp) = self.bank.read().unwrap().get_latest_slot_hash_timestamp();
        let diff = slot.abs_diff(current_slot);

        Ok(Some(if slot > current_slot {
            (timestamp * 1000 + diff * 400) / 1000
        } else {
            (timestamp * 1000 - diff * 400) / 1000
        }))
    }

    fn get_account_info(
        &self,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Option<UiAccount>>> {
        counter!("rpc", "method" => "getAccountInfo").increment(1);
        let slot = self.get_current_slot()?;

        let pubkey = verify_pubkey(&pubkey_str)?;
        let account = match self
            .db
            .read()
            .map_err(|e| {
                error!("get_account_info error: {:?}", e);
                ErrorCode::InternalError
            })?
            .get_account(pubkey)
            .expect("Failed to get account")
        {
            Some(account) => {
                if account.data().is_empty() && account.owner() == &system_program::id() && account.lamports() == 0 {
                    return Ok(RpcResponse::<Option<UiAccount>> {
                        context: RpcResponseContext { slot },
                        value: None,
                    });
                }

                let (encoded_sliced_data, encoding) = get_account_data_encoding_and_slice(&config, &account)?;

                RpcResponse::<Option<UiAccount>> {
                    context: RpcResponseContext { slot },
                    value: Some(UiAccount {
                        lamports: account.lamports(),
                        data: UiAccountData::Binary(encoded_sliced_data, encoding),
                        owner: account.owner().to_string(),
                        executable: account.executable(),
                        rent_epoch: account.rent_epoch(),
                        space: Some(account.data().len() as u64),
                    }),
                }
            }
            None => RpcResponse::<Option<UiAccount>> {
                context: RpcResponseContext { slot },
                value: None,
            },
        };
        Ok(account)
    }

    fn get_epoch_schedule(&self) -> RpcResult<RpcEpochSchedule> {
        counter!("rpc", "method" => "getEpochSchedule").increment(1);
        Ok(RpcEpochSchedule {
            first_normal_epoch: 0,
            first_normal_slot: 0,
            leader_schedule_slot_offset: 432000,
            slots_per_epoch: 432000,
            warmup: false,
        })
    }

    async fn get_multiple_accounts(
        &self,
        pubkey_strs: Vec<String>,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<UiAccount>>>> {
        counter!("rpc", "method" => "getMultipleAccounts").increment(1);
        let num_keys = pubkey_strs.len();
        let pubkeys: Vec<Pubkey> = pubkey_strs
            .into_iter()
            .filter_map(|pubkey_str| Pubkey::from_str(&pubkey_str).ok())
            .collect();

        if pubkeys.len() != num_keys {
            return Err(invalid_params_error("Invalid pubkey"));
        }

        let slot = self.get_current_slot()?;
        let db_guard = self.db.read().map_err(|e| {
            error!("get_account_info error: {:?}", e);
            ErrorCode::InternalError
        })?;

        let account_results: Vec<_> = pubkeys.iter().map(|pubkey| db_guard.get_account(*pubkey)).collect();

        // Map the reference to the desired return type
        let accounts = account_results
            .into_iter()
            .map(|account_result| {
                if let Some(account) = account_result.expect("Failed to get account") {
                    if account.data().is_empty() && account.owner() == &system_program::id() && account.lamports() == 0
                    {
                        return None;
                    }
                    let encoded_data_and_encoding = get_account_data_encoding_and_slice(&config, &account);
                    if let Ok((encoded_data, encoding)) = encoded_data_and_encoding {
                        Some(UiAccount {
                            lamports: account.lamports(),
                            data: UiAccountData::Binary(encoded_data, encoding),
                            owner: account.owner().to_string(),
                            executable: account.executable(),
                            rent_epoch: account.rent_epoch(),
                            space: Some(account.data().len() as u64),
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        Ok(RpcResponse::<Vec<Option<UiAccount>>> {
            context: RpcResponseContext { slot },
            value: accounts,
        })
    }

    async fn send_transaction(&self, data: String, config: Option<RpcSendTransactionConfig>) -> RpcResult<String> {
        counter!("rpc", "method" => "sendTransaction").increment(1);
        let mut simulation_error: Option<ErrorObjectOwned> = None;

        let config = config.unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);

        // simulate is hammering the node, skipping fixes deploy issues
        // maybe StandaloneSVM is hammering memory

        // todo: add simulate tx
        // may 20: add simulate tx
        // directly forward to sequencer if forward_to is set
        if !config.skip_preflight && self.forward_to.is_some() {
            match self
                .simulate_transaction(
                    data.clone(),
                    Some(RpcSimulateTransactionConfig {
                        // Ensure that the encoding is propagated to the transaction simulation
                        encoding: Some(encoding),
                        // we should always sigverify here to catch transactions with invalid signatures before hitting
                        // the sequencer the simulate_transaction endpoint should allow for
                        // skipping sigverify for partially signed txs
                        sig_verify: true,
                        // we cant replace the blockhash before sending - transaction assumed to be pre-signed at this
                        // point
                        replace_recent_blockhash: false,
                        ..Default::default()
                    }),
                )
                .await
            {
                Ok(response) => {
                    if response.value.err.is_some() {
                        let message: String = handle_simulation_err(response.value.err.as_ref().unwrap());

                        simulation_error = Some(
                            RpcCustomError::SendTransactionPreflightFailure {
                                message,
                                result: response.value,
                            }
                            .into(),
                        );
                    }
                }
                Err(simulation_err) => {
                    simulation_error = Some(simulation_err);
                }
            }
        }
        let decoded_transaction: Vec<u8> = match encoding {
            UiTransactionEncoding::Base58 => match bs58::decode(&data).into_vec() {
                Ok(tx) => tx,
                Err(_) => {
                    return Err(invalid_request_error("Failed to decode base58 string"));
                }
            },
            UiTransactionEncoding::Base64 => match b64.decode(&data) {
                Ok(tx) => tx,
                Err(_) => {
                    return Err(invalid_request_error("Failed to decode base64 string"));
                }
            },
            _ => {
                return Err(invalid_params_error(format!("Unknown encoding: {encoding:?}")));
            }
        };

        let versioned_transaction = deserialize_checked(&decoded_transaction)
            .ok_or_else(|| invalid_request_error("Failed to deserialize transaction"))?;

        if !sigverify(&versioned_transaction) {
            return Err(RpcCustomError::TransactionSignatureVerificationFailure.into());
        }

        if let Some(err) = simulation_error {
            return Err(err);
        }

        self.try_forward_to("sendTransaction", json!([data, config]));

        self.tx_service.send_transaction(versioned_transaction).map_err(|e| {
            error!("send_transaction error: {:?}", e);
            ErrorCode::InternalError.into()
        })
    }

    fn get_latest_blockhash(&self, config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<RpcBlockhash>> {
        counter!("rpc", "method" => "getLatestBlockhash").increment(1);
        let (slot, recent_hash) = self.get_latest_context()?;
        if let Some(min_context_slot) = config.unwrap_or_default().min_context_slot {
            if slot < min_context_slot {
                return Err(RpcCustomError::MinContextSlotNotReached { context_slot: slot }.into());
            }
        }
        let last_valid_block_height = self.get_last_valid_block_height()?;

        Ok(RpcResponse {
            context: RpcResponseContext { slot },
            value: RpcBlockhash {
                blockhash: recent_hash.to_string(),
                last_valid_block_height,
            },
        })
    }

    fn get_genesis_hash(&self) -> RpcResult<String> {
        counter!("rpc", "method" => "getGenesisHash").increment(1);
        Ok(Hash::default().to_string())
    }

    fn is_blockhash_valid(&self, blockhash: String, _config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<bool>> {
        counter!("rpc", "method" => "isBlockhashValid").increment(1);
        let value = self.is_blockhash_valid(&Hash::from_str(&blockhash).map_err(|e| {
            error!("is_blockhash_valid error: {:?}", e);
            ErrorCode::InternalError
        })?)?;
        Ok(RpcResponse {
            context: RpcResponseContext {
                slot: self.get_current_slot()?,
            },
            value,
        })
    }

    fn get_fee_for_message(
        &self,
        data: String,
        _config: Option<RpcContextConfig>,
    ) -> RpcResult<RpcResponse<Option<u64>>> {
        counter!("rpc", "method" => "getFeeForMessage").increment(1);
        // parse the message and check for cu
        // first convert data (base64) to bytes
        let decoded_transaction: Vec<u8> =
            match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &data) {
                Ok(tx) => tx,
                Err(_) => {
                    return Err(invalid_request_error("Failed to decode base64 string"));
                }
            };

        let message: Message = bincode::deserialize(&decoded_transaction[..]).map_err(|e| {
            error!("get_fee_for_message error: {:?}", e);
            ErrorCode::InvalidParams
        })?;

        let mut expected_fee = 5000 * message.signer_keys().len() as u64;

        let mut ix_vec = vec![];
        for ix in &message.instructions {
            let program_id = match message.account_keys.get(ix.program_id_index as usize) {
                Some(program_id) => program_id,
                None => return Err(ErrorCode::InvalidParams.into()),
            };

            ix_vec.push((
                program_id,
                solana_svm_transaction::instruction::SVMInstruction::from(ix),
            ));
        }
        let compute_budget_limits = ComputeBudgetInstructionDetails::try_from(ix_vec.into_iter())
            .map_err(|e| {
                error!("get_fee_for_message error: {:?}", e);
                ErrorCode::InvalidParams
            })?
            .sanitize_and_convert_to_compute_budget_limits(&get_feature_set())
            .map_err(|e| {
                error!("get_fee_for_message error: {:?}", e);
                ErrorCode::InvalidParams
            })?;
        expected_fee += compute_budget_limits.compute_unit_limit as u64 * compute_budget_limits.compute_unit_price;

        Ok(RpcResponse::<Option<u64>> {
            context: RpcResponseContext {
                slot: self.get_current_slot()?,
            },
            value: Some(expected_fee),
        })
    }

    fn get_recent_prioritization_fees(&self, pubkey_strs: Option<Vec<String>>) -> RpcResult<Vec<RpcPrioritizationFee>> {
        counter!("rpc", "method" => "getRecentPrioritizationFees").increment(1);
        let mut fees = vec![];
        for _ in pubkey_strs.unwrap_or_default() {
            fees.push(RpcPrioritizationFee {
                slot: self.get_current_slot()?,
                prioritization_fee: 0,
            });
        }
        Ok(fees)
    }

    fn get_stake_minimum_delegation(&self, _config: Option<RpcContextConfig>) -> RpcResult<RpcResponse<u64>> {
        Ok(RpcResponse::<u64> {
            context: RpcResponseContext {
                slot: self.get_current_slot()?,
            },
            value: 0,
        })
    }

    async fn get_signature_statuses(
        &self,
        signature_strs: Vec<String>,
        _config: Option<RpcSignatureStatusConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<RpcTransactionStatus>>>> {
        counter!("rpc", "method" => "getSignatureStatuses").increment(1);
        // wait really long
        let slot = self.get_current_slot()?;

        let signatures = signature_strs
            .iter()
            .map(|signature| verify_signature(signature))
            .collect::<Vec<_>>();
        if signatures.iter().any(|signature| signature.is_err()) {
            return Err(invalid_params_error("Invalid signature"));
        }

        let mut statuses = Vec::with_capacity(signatures.len());
        {
            let bank: std::sync::RwLockReadGuard<'_, dyn RpcBank> = self.bank.read().unwrap();

            for signature in &signatures {
                statuses.push(bank.get_tx_status(signature.as_ref().unwrap()));
            }
            drop(bank);
        }

        // for all None, query the indexer again
        for (i, signature) in signatures.iter().enumerate() {
            if statuses[i].is_none() {
                match self
                    .indexer
                    .get_transaction_with_metadata(signature.as_ref().unwrap())
                    .await
                {
                    Ok(Some(tx_status)) => {
                        statuses[i] = Some(TransactionStatus::Executed(
                            tx_status.metadata.status.err(),
                            tx_status.slot,
                        ));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("get_signature_statuses error: {:?}", e);
                        return Err(ErrorCode::InternalError.into());
                    }
                }
            }
        }

        let statuses = statuses
            .iter()
            .map(|maybe_tx| {
                let tx_status = match maybe_tx.as_ref() {
                    Some(tx_status) => tx_status,
                    None => return None,
                };
                match tx_status {
                    infinisvm_core::bank::TransactionStatus::Executing => None,
                    infinisvm_core::bank::TransactionStatus::Executed(transaction_error, slot) => {
                        Some(RpcTransactionStatus {
                            slot: *slot,
                            confirmations: None,
                            status: match transaction_error {
                                Some(transaction_error) => Err(transaction_error.clone()),
                                None => Ok(()),
                            },
                            err: transaction_error.clone(),
                            confirmation_status: Some(TransactionConfirmationStatus::Finalized),
                        })
                    }
                }
            })
            .collect();

        let rpc_response = RpcResponse::<Vec<Option<RpcTransactionStatus>>> {
            context: RpcResponseContext { slot },
            value: statuses,
        };
        Ok(rpc_response)
    }

    fn get_minimum_balance_for_rent_exemption(
        &self,
        data_len: usize,
        _config: Option<RpcGetMinimumBalanceForRentExemptionConfig>,
    ) -> RpcResult<u64> {
        counter!("rpc", "method" => "getMinimumBalanceForRentExemption").increment(1);
        let rent = Rent::default();
        Ok(rent.minimum_balance(data_len))
    }

    fn get_slot(&self, _config: Option<RpcContextConfig>) -> RpcResult<u64> {
        counter!("rpc", "method" => "getSlot").increment(1);
        self.get_current_slot()
    }

    fn get_epoch_info(&self, _config: Option<RpcContextConfig>) -> RpcResult<EpochInfo> {
        counter!("rpc", "method" => "getEpochInfo").increment(1);
        // assume 1 epoch has 432000 slots
        let slot = self.get_current_slot()?;
        Ok(EpochInfo {
            epoch: 0,
            slot_index: slot % 432000,
            slots_in_epoch: 432000,
            absolute_slot: slot,
            block_height: slot,
            transaction_count: Some(self.total_transaction_count.load(Ordering::Relaxed)),
        })
    }

    fn get_slot_leaders(&self, _start_slot: u64, _limit: u64) -> RpcResult<Vec<String>> {
        counter!("rpc", "method" => "getSlotLeaders").increment(1);
        if _limit > 5000 {
            return Err(invalid_params_error("limit must be less than 50"));
        }
        let leader_pubkey = self.identity().to_base58_string();
        Ok(vec![leader_pubkey; _limit as usize])
    }

    fn get_cluster_nodes(&self) -> RpcResult<Vec<RpcClusterNode>> {
        counter!("rpc", "method" => "getClusterNodes").increment(1);
        let nodes = vec![RpcClusterNode {
            pubkey: self.identity().pubkey().to_string(),
            gossip: "0.0.0.0:8001".to_string(),
            rpc: "0.0.0.0:8899".to_string(),
            tpu: self.tpu().to_string(),
            version: "infinisvm/0.1.0 (endpoint for solana-cli compatibility)".to_string(),
        }];
        Ok(nodes)
    }

    fn get_block_height(&self, _config: Option<RpcContextConfig>) -> RpcResult<u64> {
        counter!("rpc", "method" => "getBlockHeight").increment(1);
        let block_height = self.get_block_height()?;
        Ok(block_height)
    }

    async fn get_program_accounts(
        &self,
        program_id_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<OptionalContext<Vec<RpcKeyedAccount>>> {
        counter!("rpc", "method" => "getProgramAccounts").increment(1);
        let program_id = verify_pubkey(&program_id_str)?;

        let (config, filters, with_context) = if let Some(config) = config {
            (
                Some(config.account_config),
                config.filters.unwrap_or_default(),
                config.with_context.unwrap_or_default(),
            )
        } else {
            (None, vec![], false)
        };

        let RpcAccountInfoConfig {
            encoding,
            data_slice: data_slice_config,
            commitment: _,
            min_context_slot: _,
        } = config.unwrap_or_default();

        let (request_slot, program_accounts) = self
            .get_program_accounts(&program_id, filters, encoding, data_slice_config)
            .await?;

        Ok(match with_context {
            true => OptionalContext::Context(Response {
                context: RpcResponseContext { slot: request_slot },
                value: program_accounts,
            }),
            false => OptionalContext::NoContext(program_accounts),
        })
    }

    fn get_token_account_balance(
        &self,
        pubkey_str: String,
        _config: Option<serde_json::Value>,
    ) -> RpcResult<RpcResponse<UiTokenAmount>> {
        counter!("rpc", "method" => "getTokenAccountBalance").increment(1);
        let slot = self.get_current_slot()?;

        let token_account_pubkey = verify_pubkey(&pubkey_str)?;

        let db_guard = self.db.read().map_err(|e| {
            error!("get_token_account_balance error: {:?}", e);
            ErrorCode::InternalError
        })?;

        let token_account = db_guard
            .get_account(token_account_pubkey)
            .expect("Failed to find token account in accounts_db")
            .ok_or_else(|| {
                invalid_params_error(format!(
                    "Failed to find token account in accounts_db {token_account_pubkey}"
                ))
            })?;

        if token_account.owner() != &spl_token::id() && token_account.owner() != &spl_token_2022::id() {
            return Err(invalid_params_error("not a Token account"));
        }

        let token_account_data = StateWithExtensions::<spl_token_2022::state::Account>::unpack(token_account.data())
            .map_err(|e| {
                error!("Failed to unpack token account data: {:?}", e);
                ErrorCode::InternalError
            })?;

        let mint_account_key: Pubkey = Pubkey::new_from_array(token_account_data.base.mint.to_bytes());

        let mint_account = db_guard
            .get_account(mint_account_key)
            .expect("Failed to find mint account in accounts_db")
            .ok_or_else(|| {
                invalid_params_error(format!("Failed to find mint account in accounts_db {mint_account_key}"))
            })?;

        let mint_decimals = get_mint_decimals(mint_account.data())
            .map_err(|_| invalid_params_error("Token mint could not be unpacked"))?;

        let rpc_response = RpcResponse::<UiTokenAmount> {
            context: RpcResponseContext { slot },
            value: token_amount_to_ui_amount(token_account_data.base.amount, mint_decimals),
        };

        Ok(rpc_response)
    }


    async fn get_token_accounts_by_delegate(
        &self,
        pubkey: String,
        needle: DelegateSearch,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<RpcKeyedAccount>>> {
        counter!("rpc", "method" => "getTokenAccountsByDelegate").increment(1);
        let pubkey = verify_pubkey(&pubkey)?;
        let mint_pubkey = verify_pubkey(&needle.mint)?;
        let program_id_pubkey = needle.program_id.map(|program_id| verify_pubkey(&program_id)).transpose()?;
        let token_accounts = self.indexer
            .find_token_accounts_by_mint(program_id_pubkey, mint_pubkey, MAX_TOKEN_ACCOUNTS_QUERY_LIMIT, 0)
            .await;
        let filter = |token_account: &StateWithExtensions<spl_token_2022::state::Account>| -> bool {
            token_account.base.delegate.is_some() && token_account.base.delegate.unwrap() == pubkey
        };
        let db_reader = self.db.read().map_err(|e| {
            error!("get_token_accounts_by_delegate error: {:?}", e);
            ErrorCode::InternalError
        })?;
        let mut filtered_accounts = Vec::new();
        for pubkey in token_accounts {
            let token_account = db_reader.get_account(pubkey).unwrap().unwrap_or_default();
            let token_account_data = StateWithExtensions::<spl_token_2022::state::Account>::unpack(token_account.data())
                .map_err(|e| {
                    error!("Failed to unpack token account data: {:?}", e);
                    ErrorCode::InternalError
                })?;
            if filter(&token_account_data) {
                let (encoded_sliced_data, encoding) = get_account_data_encoding_and_slice(&config, &token_account)?;
                filtered_accounts.push(RpcKeyedAccount {
                    pubkey: pubkey.to_string(),
                    account: UiAccount {
                        lamports: token_account.lamports(),
                        data: UiAccountData::Binary(encoded_sliced_data, encoding),
                        owner: token_account.owner().to_string(),
                        executable: token_account.executable(),
                        rent_epoch: token_account.rent_epoch(),
                        space: Some(token_account.data().len() as u64),
                    },
                });
            }
        }
        drop(db_reader);

        let slot = self.get_current_slot()?;

        Ok(RpcResponse::<Vec<RpcKeyedAccount>> {
            context: RpcResponseContext { slot },
            value: filtered_accounts,
        })
    }

    async fn get_token_largest_accounts(
        &self,
        mint: String,
        _config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<RpcTokenAccountValueOnly>>> {
        counter!("rpc", "method" => "getTokenLargestAccounts").increment(1);
        let mint_pubkey = verify_pubkey(&mint)?;
        let slot = self.get_current_slot()?;
        let token_accounts = self.indexer
            .find_token_accounts_by_mint(None, mint_pubkey, MAX_TOKEN_ACCOUNTS_QUERY_LIMIT, 0)
            .await;
        let db_reader = self.db.read().map_err(|e| {
            error!("get_token_largest_accounts error: {:?}", e);
            ErrorCode::InternalError
        })?;

        let mint_account = db_reader
            .get_account(mint_pubkey)
            .expect("Failed to find mint account in accounts_db")
            .ok_or_else(|| invalid_params_error(format!("Failed to find mint account in accounts_db {mint_pubkey}")))?;

        let mint_account = StateWithExtensions::<Mint>::unpack(mint_account.data()).map_err(|e| {
            error!("Failed to unpack mint account data: {:?}", e);
            ErrorCode::InternalError
        })?;
        let mut token_accounts_with_amounts = Vec::new();

        for pubkey in token_accounts {
            let token_account = db_reader.get_account(pubkey).unwrap().unwrap_or_default();

            let token_account_data = StateWithExtensions::<spl_token_2022::state::Account>::unpack(token_account.data())
                .map_err(|e| {
                    error!("Failed to unpack token account data: {:?}", e);
                    ErrorCode::InternalError
                })?;

            token_accounts_with_amounts.push((token_account_data.base.amount, pubkey));
        }

        drop(db_reader);

        // sort descending by amount
        token_accounts_with_amounts.sort_by_key(|(amount, _)| std::cmp::Reverse(*amount));

        // take top 20
        let top_20: Vec<_> = token_accounts_with_amounts.into_iter().take(20).collect();

        let mut parsed_token_accounts = vec![];
        for (amount, pubkey) in top_20 {
            let amount = token_amount_to_ui_amount(amount, mint_account.base.decimals);
            parsed_token_accounts.push(RpcTokenAccountValueOnly {
                address: pubkey.to_string(),
                amount: amount.amount,
                decimals: mint_account.base.decimals,
                ui_amount: amount.ui_amount,
                ui_amount_string: amount.ui_amount_string,
            });
        }
        Ok(RpcResponse::<Vec<RpcTokenAccountValueOnly>> {
            context: RpcResponseContext { slot },
            value: parsed_token_accounts,
        })
    }

    fn get_token_supply(
        &self,
        mint_str: String,
        _config: Option<serde_json::Value>,
    ) -> RpcResult<RpcResponse<UiTokenAmount>> {
        counter!("rpc", "method" => "getTokenSupply").increment(1);
        let slot = self.get_current_slot()?;
        let mint_pubkey = verify_pubkey(&mint_str)?;

        let mint_account = self
            .db
            .read()
            .map_err(|e| {
                error!("get_token_supply error: {:?}", e);
                ErrorCode::InternalError
            })?
            .get_account(mint_pubkey)
            .expect("Failed to find mint account in accounts_db")
            .ok_or_else(|| invalid_params_error(format!("Failed to find mint account in accounts_db {mint_pubkey}")))?;

        let mint_account = StateWithExtensions::<Mint>::unpack(mint_account.data()).map_err(|e| {
            error!("Failed to unpack mint account data: {:?}", e);
            ErrorCode::InternalError
        })?;

        Ok(RpcResponse::<UiTokenAmount> {
            context: RpcResponseContext { slot },
            value: token_amount_to_ui_amount(mint_account.base.supply, mint_account.base.decimals),
        })
    }

    async fn get_token_accounts_by_owner(
        &self,
        owner_str: String,
        filter: RpcTokenAccountsFilter,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<RpcKeyedAccount>>> {
        counter!("rpc", "method" => "getTokenAccountsByOwner").increment(1);
        let owner_pubkey = verify_pubkey(&owner_str)?;
        let slot = self.get_current_slot()?;

        let token_accounts = match filter {
            RpcTokenAccountsFilter::Mint(mint) => {
                let mint = verify_pubkey(&mint)?;
                self.indexer
                    .find_token_accounts_owned_by(&owner_pubkey, None, Some(mint), 10, 0)
                    .await
            }
            RpcTokenAccountsFilter::ProgramId(program_id) => {
                let program_id = verify_pubkey(&program_id)?;
                self.indexer
                    .find_token_accounts_owned_by(&owner_pubkey, Some(program_id), None, 10, 0)
                    .await
            }
        };

        let db_reader = self.db.read().map_err(|e| {
            error!("get_token_accounts_by_owner error: {:?}", e);
            ErrorCode::InternalError
        })?;

        let mut parsed_token_accounts = vec![];

        for pubkey in token_accounts {
            let account = db_reader.get_account(pubkey).expect("Failed to get account").unwrap_or_default();
            let (encoded_sliced_data, encoding) = get_account_data_encoding_and_slice(&config, &account)?;
            parsed_token_accounts.push(RpcKeyedAccount {
                pubkey: pubkey.to_string(),
                account: UiAccount {
                    lamports: account.lamports(),
                    data: UiAccountData::Binary(encoded_sliced_data, encoding),
                    owner: account.owner().to_string(),
                    executable: account.executable(),
                    rent_epoch: account.rent_epoch(),
                    space: Some(account.data().len() as u64),
                },
            });
        }

        Ok(RpcResponse {
            context: RpcResponseContext { slot },
            value: parsed_token_accounts,
        })
    }

    async fn get_transaction(
        &self,
        signature_str: String,
        config: Option<RpcTransactionConfig>,
    ) -> RpcResult<Option<EncodedConfirmedTransactionWithStatusMeta>> {
        counter!("rpc", "method" => "getTransaction").increment(1);
        let signature = Signature::from_str(&signature_str).map_err(|_| ErrorCode::InvalidParams)?;

        let transaction = self
            .indexer
            .get_transaction_with_metadata(&signature)
            .await
            .map_err(|_| ErrorCode::InvalidParams)?;

        if let Some(transaction) = transaction {
            let encoded_transaction_with_status_meta =
                to_encoded_confirmed_transaction_with_status_meta(transaction, config)
                    .map_err(|_| ErrorCode::InvalidParams)?;
            return Ok(Some(encoded_transaction_with_status_meta));
        }

        Ok(None)
    }

    async fn get_signatures_for_address(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        counter!("rpc", "method" => "getSignaturesForAddress").increment(1);
        let config = config.unwrap_or_default();
        // Set the limit to 1000 to conform with the Solana JSON RPC
        let limit = config.limit.unwrap_or(1000).min(1000);

        let address = verify_pubkey(&address)?;

        macro_rules! string_to_signature {
            ($s:expr) => {
                Signature::from_str(&$s).map_err(|_| ErrorCode::InvalidParams)?
            };
        }

        let signature_filters = match (
            config.before,
            config.until,
            config.before_timestamp,
            config.until_timestamp,
        ) {
            // Signature range
            (Some(before), Some(until), None, None) => {
                SignatureFilters::Signature(Some(string_to_signature!(before)), Some(string_to_signature!(until)))
            }
            (Some(before), None, None, None) => SignatureFilters::Signature(Some(string_to_signature!(before)), None),
            (None, Some(until), None, None) => SignatureFilters::Signature(None, Some(string_to_signature!(until))),
            // Time range
            (None, None, Some(before), Some(until)) => SignatureFilters::TimeRange(Some(before), Some(until)),
            (None, None, Some(before), None) => SignatureFilters::TimeRange(Some(before), None),
            (None, None, None, Some(until)) => SignatureFilters::TimeRange(None, Some(until)),
            // Signature
            (None, None, None, None) => SignatureFilters::None,
            _ => return Err(ErrorCode::InvalidParams.into()),
        };

        let signatures = self
            .indexer
            .find_signatures_of_account(&address, signature_filters, limit)
            .await
            .map_err(|_| ErrorCode::InvalidParams)?;

        let mut results = Vec::with_capacity(signatures.len());
        // Only include heavy transaction details when explicitly requested.
        let include_details = config.detailed.unwrap_or(false);

        for signature in signatures {
            let tx = self.indexer.get_transaction_with_metadata(&signature).await;
            if let Ok(Some(tx)) = tx {
                results.push(RpcConfirmedTransactionStatusWithSignature {
                    signature: signature.to_string(),
                    slot: tx.slot,
                    err: tx.metadata.status.clone().err(),
                    memo: None,
                    block_time: Some(tx.unix_timestamp_in_millis),
                    confirmation_status: None,
                    details: if include_details {
                        Some(EncodedTransactionWithStatusMeta {
                            transaction: tx.transaction.encode(UiTransactionEncoding::Json),
                            meta: Some(tx.metadata.into()),
                            version: None,
                        })
                    } else {
                        None
                    },
                })
            }
        }

        Ok(results)
    }

    async fn get_confirmed_signatures_for_address2(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        counter!("rpc", "method" => "getConfirmedSignaturesForAddress2").increment(1);
        self.get_signatures_for_address(address, config).await
    }

    async fn simulate_transaction(
        &self,
        data: String,
        config: Option<RpcSimulateTransactionConfig>,
    ) -> RpcResult<RpcResponse<RpcSimulateTransactionResult>> {
        counter!("rpc", "method" => "simulateTransaction").increment(1);

        let config = config.unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base64);
        let accounts_config = config.accounts.unwrap_or_default();

        let decoded_transaction: Vec<u8> = match encoding {
            UiTransactionEncoding::Base58 => match bs58::decode(&data).into_vec() {
                Ok(tx) => tx,
                Err(_) => {
                    return Err(invalid_request_error("Failed to decode base58 string"));
                }
            },
            UiTransactionEncoding::Base64 => match b64.decode(&data) {
                Ok(tx) => tx,
                Err(_) => {
                    return Err(invalid_request_error("Failed to decode base64 string"));
                }
            },
            _ => {
                return Err(invalid_params_error(format!("Unknown encoding: {encoding:?}")));
            }
        };

        let versioned_transaction = deserialize_checked(&decoded_transaction)
            .ok_or_else(|| invalid_request_error("Failed to deserialize transaction"))?;

        if config.sig_verify && !sigverify(&versioned_transaction) {
            return Err(RpcCustomError::TransactionSignatureVerificationFailure.into());
        }

        let sanitized_tx = SanitizedTransaction::try_create(
            versioned_transaction,
            MessageHash::Compute,
            Some(false), // is_simple_vote_tx
            SimpleAddressLoader::Enabled(LoadedAddresses::default()),
            &std::collections::HashSet::new(), // reserved_account_keys
        )
        .map_err(|_| invalid_request_error("Failed to sanitize transaction"))?;

        let bank = self.bank.read().unwrap();

        if !config.replace_recent_blockhash && !bank.is_tx_blockhash_valid(&sanitized_tx) {
            return Err(RpcCustomError::TransactionBlockhashNotFound.into());
        }

        if !config.replace_recent_blockhash
            && bank
                .get_tx_status(sanitized_tx.signature())
                .is_some_and(|status| matches!(status, infinisvm_core::bank::TransactionStatus::Executed(_, _)))
        {
            return Err(RpcCustomError::TransactionAlreadyProcessed.into());
        }
        let sim_slot = bank.get_current_slot();
        let latest_blockhash = bank.get_latest_slot_hash_timestamp().1;
        let processed_tx = bank.simulate_transaction(sanitized_tx.clone());

        drop(bank);

        match processed_tx {
            Ok((processed_tx, accounts_changed)) => Ok({
                let tsm = to_transaction_with_metadata(&processed_tx, &sanitized_tx);

                RpcResponse {
                    context: RpcResponseContext { slot: sim_slot },
                    value: RpcSimulateTransactionResult {
                        err: tsm.status.err(),
                        logs: tsm.log_messages,
                        return_data: tsm.return_data.map(|return_data| UiTransactionReturnData {
                            program_id: return_data.program_id.to_string(),
                            data: (b64.encode(return_data.data), UiReturnDataEncoding::Base64),
                        }),
                        accounts: Some(
                            accounts_changed
                                .into_iter()
                                .map(|(_, account)| {
                                    let data = match accounts_config.data_slice {
                                        Some(data_slice) => account.data()
                                            [data_slice.offset..(data_slice.offset + data_slice.length)]
                                            .to_vec(),
                                        None => account.data().to_vec(),
                                    };
                                    let (encoded_data, encoding) = match accounts_config.encoding {
                                        Some(encoding) => match encoding {
                                            UiAccountEncoding::Base58 => (bs58::encode(data).into_string(), encoding),
                                            UiAccountEncoding::Base64 => (b64.encode(data), encoding),
                                            UiAccountEncoding::Base64Zstd => {
                                                let zstd_encoder = zstd::stream::write::Encoder::new(Vec::new(), 0)
                                                    .map_err(|e| {
                                                        error!("get_account_data_encoding_and_slice error: {:?}", e);
                                                        ErrorCode::InternalError
                                                    });

                                                if let Ok(mut zstd_encoder) = zstd_encoder {
                                                    if let Ok(zstd_data) = zstd_encoder
                                                        .write_all(&data)
                                                        .and_then(|()| zstd_encoder.finish())
                                                    {
                                                        (b64.encode(zstd_data), encoding)
                                                    } else {
                                                        (b64.encode(data), UiAccountEncoding::Base64)
                                                    }
                                                } else {
                                                    (b64.encode(data), UiAccountEncoding::Base64)
                                                }
                                            }
                                            _ => (b64.encode(data), UiAccountEncoding::Base64),
                                        },
                                        None => (b64.encode(data), UiAccountEncoding::Base64),
                                    };
                                    Some(UiAccount {
                                        lamports: account.lamports(),
                                        data: UiAccountData::Binary(encoded_data, encoding),
                                        owner: account.owner().to_string(),
                                        executable: account.executable(),
                                        rent_epoch: account.rent_epoch(),
                                        space: Some(account.data().len() as u64),
                                    })
                                })
                                .collect::<Vec<Option<UiAccount>>>(),
                        ),
                        units_consumed: tsm.compute_units_consumed,
                        inner_instructions: Some(
                            tsm.inner_instructions
                                .unwrap_or_default()
                                .iter()
                                .map(|inner_instructions| UiInnerInstructions::from(inner_instructions.clone()))
                                .collect(),
                        ),
                        replacement_blockhash: Some(RpcBlockhash {
                            blockhash: latest_blockhash.to_string(),
                            last_valid_block_height: sim_slot,
                        }),
                    },
                }
            }),
            Err(err) => {
                let err_str = handle_simulation_err(&err);
                Err(RpcCustomError::TransactionSimulationFailure(err_str).into())
            }
        }
    }

    fn get_transaction_count(&self) -> RpcResult<u64> {
        counter!("rpc", "method" => "getTransactionCount").increment(1);
        Ok(self.total_transaction_count.load(Ordering::Relaxed))
    }

    fn get_recent_performance_samples(&self, limit: u64) -> RpcResult<Vec<RpcPerfSample>> {
        counter!("rpc", "method" => "getRecentPerformanceSamples").increment(1);
        let limit = limit.min(720) as usize;

        if let Some(forwards) = &self.forward_to {
            let client = reqwest::blocking::Client::new();
            let result = client
                .post(forwards)
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "getRecentPerformanceSamples",
                    "params": [limit],
                    "id": 1,
                }))
                .send()
                .map_err(|e| RpcCustomError::ForwardingError(e.to_string()))?;

            let result_json = result
                .json::<serde_json::Value>()
                .map_err(|e| RpcCustomError::ForwardingError(e.to_string()))?;

            result_json
                .get("result")
                .and_then(|v| v.as_array())
                .map(|v| {
                    v.iter()
                        .map(|v| RpcPerfSample {
                            slot: v.get("slot").and_then(|v| v.as_u64()).unwrap(),
                            num_transactions: v.get("numTransactions").and_then(|v| v.as_u64()).unwrap(),
                            num_non_vote_transactions: v.get("numNonVoteTransactions").and_then(|v| v.as_u64()),
                            num_slots: v.get("numSlots").and_then(|v| v.as_u64()).unwrap(),
                            sample_period_secs: v.get("samplePeriodSecs").and_then(|v| v.as_u64()).unwrap() as u16,
                        })
                        .collect()
                })
                .ok_or_else(|| RpcCustomError::ForwardingError("Failed to parse response".to_string()).into())
        } else {
            let samples = self.samples.read().unwrap();
            let mut result = Vec::new();
            for i in 0..limit {
                if let Some(sample) = samples.1.get(i) {
                    result.push(RpcPerfSample {
                        slot: sample.0,
                        num_transactions: sample.1,
                        num_non_vote_transactions: Some(sample.1),
                        num_slots: sample.2,
                        sample_period_secs: sample.3 as u16,
                    });
                }
            }
            Ok(result)
        }
    }

    fn request_airdrop(&self, pubkey_str: String, lamports: u64) -> RpcResult<String> {
        counter!("rpc", "method" => "requestAirdrop").increment(1);

        let lamports = lamports.min(1_000_000_000); // 1 SOL
        let pubkey = verify_pubkey(&pubkey_str)?;
        let (_, recent_hash) = self.get_latest_context()?;

        if self.forward_to.is_some() {
            let airdrop_tx = self
                .tx_service
                .build_airdrop_transaction(&pubkey, recent_hash, lamports);

            let tx = match airdrop_tx.encode(UiTransactionEncoding::Binary) {
                EncodedTransaction::LegacyBinary(tx) => tx,
                _ => unreachable!(),
            };
            self.try_forward_to("sendTransaction", json!([tx,]));

            Ok(airdrop_tx.signatures[0].to_string())
        } else {
            self.tx_service
                .request_airdrop(&pubkey, recent_hash, lamports)
                .map_err(|_| ErrorCode::InternalError.into())
        }
    }

    async fn account_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        let middleware = self.get_middleware();
        let pending = pending.accept().await.unwrap();
        middleware.register_connection(pending.connection_id());

        let pubkey = verify_pubkey(&pubkey_str)?;
        let mut receiver = self
            .subscription_processor
            .register_pubkey_subscription(pubkey)
            .await
            .unwrap();
        while let Ok(account) = receiver.recv().await {
            let ui_account = to_ui_account(&config, &account)?;
            let data = self.wrap_with_rpc_context(ui_account)?;
            middleware.track_message_data(&pending.connection_id(), get_size(&data))?;
            let notif = SubscriptionMessage::from_json(&data)?;
            if pending.send(notif).await.is_err() {
                self.subscription_processor.unregister_pubkey_subscription(&pubkey);
                break;
            }
        }
        Ok(())
    }

    // async fn program_subscribe(
    //     &self,
    //     pending: PendingSubscriptionSink,
    //     pubkey_str: String,
    //     config: Option<RpcProgramAccountsConfig>,
    // ) -> SubscriptionResult {
    //     todo!("not implemented")
    // }

    // async fn logs_subscribe(
    //     &self,
    //     pending: PendingSubscriptionSink,
    //     filter: RpcTransactionLogsFilter,
    //     config: Option<RpcTransactionLogsConfig>,
    // ) -> SubscriptionResult {
    //     todo!("logs_subscribe not implemented")
    // }

    async fn signature_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> SubscriptionResult {
        let pending = pending.accept().await.unwrap();
        let middleware = self.get_middleware();
        middleware.register_connection(pending.connection_id());

        let enable_received_notification = config.unwrap_or_default().enable_received_notification.unwrap_or(false);
        let signature = verify_signature(&signature_str)?;
        let mut receiver = self
            .subscription_processor
            .register_signature_subscription(signature)
            .await
            .unwrap();

        while let Ok(status) = receiver.recv().await {
            match status {
                TransactionStatus::Executing => {
                    if enable_received_notification {
                        let data = self.wrap_with_rpc_context("receivedSignature")?;
                        middleware.track_message_data(&pending.connection_id(), get_size(&data))?;
                        let notif = SubscriptionMessage::from_json(&data)?;
                        if pending.send(notif).await.is_err() {
                            self.subscription_processor
                                .unregister_signature_subscription(&signature);
                            break;
                        }
                    }
                }
                TransactionStatus::Executed(error, _) => {
                    let data = self.wrap_with_rpc_context(error)?;
                    middleware.track_message_data(&pending.connection_id(), get_size(&data))?;
                    let notif = SubscriptionMessage::from_json(&data)?;
                    if pending.send(notif).await.is_err() {
                        self.subscription_processor
                            .unregister_signature_subscription(&signature);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn slot_subscribe(&self, pending: PendingSubscriptionSink) -> SubscriptionResult {
        let pending = pending.accept().await.unwrap();
        let mut receiver = self.subscription_processor.register_all_slots_subscription().unwrap();
        while let Ok(slot) = receiver.recv().await {
            let notif = SubscriptionMessage::from_json(&self.wrap_with_rpc_context(SlotInfo {
                slot,
                parent: slot.wrapping_sub(1),
                root: 0,
            })?)?;
            if pending.send(notif).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    // async fn slots_updates_subscribe(
    //     &self,
    //     pending: PendingSubscriptionSink,
    // ) -> SubscriptionResult {
    //     let pending = pending.accept().await.unwrap();
    //     let mut receiver =
    // self.subscription_processor.register_all_slots_subscription().unwrap();
    //     while let Ok(slot) = receiver.recv().await {
    //         let notif =
    // SubscriptionMessage::from_json(&self.wrap_with_rpc_context(slot)?)?;
    //         if pending.send(notif).await.is_err() {
    //             break;
    //         }
    //     }
    //     Ok(())
    // }

    async fn block_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        filter: RpcBlockSubscribeFilter,
        config: Option<RpcBlockSubscribeConfig>,
    ) -> SubscriptionResult {
        let pending = pending.accept().await.unwrap();
        let middleware = self.get_middleware();
        middleware.register_connection(pending.connection_id());

        fn convert_block_to_rpc_block(
            config: Option<RpcBlockSubscribeConfig>,
            block: BlockWithTransactions,
        ) -> RpcResult<RpcBlockUpdate> {
            let config = config.unwrap_or_default();
            let config = RpcBlockConfig {
                encoding: config.encoding,
                transaction_details: config.transaction_details,
                max_supported_transaction_version: config.max_supported_transaction_version,
                ..Default::default()
            };
            Ok(RpcBlockUpdate {
                slot: block.slot,
                block: Some(to_ui_block(Some(config), block)?),
                err: None,
            })
        }
        match filter {
            RpcBlockSubscribeFilter::MentionsAccountOrProgram(pubkey) => {
                let pubkey = verify_pubkey(&pubkey)?;
                let mut receiver = self
                    .subscription_processor
                    .register_block_by_pubkey_subscription(pubkey)
                    .await
                    .unwrap();
                while let Ok(block) = receiver.recv().await {
                    let data = self.wrap_with_rpc_context(convert_block_to_rpc_block(config.clone(), block)?)?;
                    middleware.track_message_data(&pending.connection_id(), get_size(&data))?;
                    let notif = SubscriptionMessage::from_json(&data)?;
                    if pending.send(notif).await.is_err() {
                        self.subscription_processor
                            .unregister_block_by_pubkey_subscription(&pubkey);
                        break;
                    }
                }
            }
            RpcBlockSubscribeFilter::All => {
                let mut receiver = self.subscription_processor.register_all_blocks_subscription().unwrap();
                while let Ok(block) = receiver.recv().await {
                    let data = self.wrap_with_rpc_context(convert_block_to_rpc_block(config.clone(), block)?)?;
                    middleware.track_message_data(&pending.connection_id(), get_size(&data))?;
                    let notif = SubscriptionMessage::from_json(&data)?;
                    if pending.send(notif).await.is_err() {
                        self.subscription_processor.unregister_all_blocks_subscription();
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn root_subscribe(&self, pending: PendingSubscriptionSink) -> SubscriptionResult {
        // infinisvm never emits a root event
        let _ = pending.accept().await.unwrap();

        Ok(())
    }
}

fn get_size<T: ?Sized>(x: &T) -> usize {
    std::mem::size_of_val(x)
}

fn handle_simulation_err(tx_err: &TransactionError) -> String {
    match tx_err {
        TransactionError::InstructionError(idx, ix_err) => {
            // Expected error message for anchor assertions
            // SendTransactionError: failed to send transaction: Transaction simulation
            // failed: Error processing Instruction 0: custom program error: 0x7d6
            // Custom(0) is expected to log - incorrect program id for instruction. Seems
            // like our VM is not handling errors correctly and throws this Custom(0) every
            // time. TODO: If token program, should we try to cast to a
            // TokenError to get a human read-able message?
            match ix_err {
                InstructionError::Custom(code) => format!(
                    "Transaction simulation failed: Error processing Instruction {idx}: custom program error: 0x{code:x}"
                ),
                _ => format!(
                    "Transaction simulation failed: Error processing Instruction {idx}: {ix_err:?}: {ix_err}", /* {} will print the error message, 'instruction spent from the balance of an account it
                             * does not own' */
                ),
            }
        }
        _ => {
            format!("Transaction simulation failed: {tx_err:?}: {tx_err}")
        }
    }
}

pub fn to_transaction_with_metadata(
    processed_transaction: &ProcessedTransaction,
    sanitized_transaction: &SanitizedTransaction,
) -> TransactionStatusMeta {
    match processed_transaction {
        ProcessedTransaction::Executed(executed_transaction) => {
            TransactionStatusMeta {
                status: executed_transaction.execution_details.status.clone(),
                fee: executed_transaction.loaded_transaction.fee_details.total_fee(),
                pre_balances: vec![],
                post_balances: vec![],
                inner_instructions: {
                    let inner_instructions = executed_transaction.execution_details.inner_instructions.clone();
                    inner_instructions.map(|inner_instructions| map_inner_instructions(inner_instructions).collect())
                },
                log_messages: executed_transaction.execution_details.log_messages.clone(),
                pre_token_balances: None,  // todo: index token
                post_token_balances: None, // todo: index token
                rewards: None,
                loaded_addresses: sanitized_transaction.get_loaded_addresses(),
                return_data: executed_transaction.execution_details.return_data.clone(),
                compute_units_consumed: Some(executed_transaction.execution_details.executed_units),
            }
        }
        ProcessedTransaction::FeesOnly(_) => {
            // todo: do we have fees only txs?
            // may 20: failed txs are fee only. add (copy from agave)
            TransactionStatusMeta::default()
        }
    }
}

pub fn to_encoded_transaction_with_status_meta(
    stored_tx: TransactionWithMetadata,
    config: Option<RpcTransactionConfig>,
) -> Result<EncodedTransactionWithStatusMeta, ErrorCode> {
    let config: RpcTransactionConfig = config.unwrap_or_default();
    let max_tx_version = config.max_supported_transaction_version.unwrap_or(0);
    let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);

    let transaction_input = stored_tx.transaction;
    let versioned_transaction_meta = VersionedTransactionWithStatusMeta {
        transaction: transaction_input,
        meta: stored_tx.metadata,
    };
    let transaction_with_status_meta = TransactionWithStatusMeta::Complete(versioned_transaction_meta);

    let encoded_transaction_with_status_meta = transaction_with_status_meta
        .encode(encoding, Some(max_tx_version), false)
        .map_err(|_| ErrorCode::InternalError)?;
    Ok(encoded_transaction_with_status_meta)
}

pub fn to_encoded_confirmed_transaction_with_status_meta(
    stored_tx: TransactionWithMetadata,
    config: Option<RpcTransactionConfig>,
) -> Result<EncodedConfirmedTransactionWithStatusMeta, ErrorCode> {
    let (slot, block_time) = (stored_tx.slot, stored_tx.unix_timestamp_in_millis);

    let encoded_transaction_with_status_meta = to_encoded_transaction_with_status_meta(stored_tx, config)?;

    Ok(EncodedConfirmedTransactionWithStatusMeta {
        slot,
        transaction: encoded_transaction_with_status_meta,
        block_time: Some(block_time as i64),
    })
}
