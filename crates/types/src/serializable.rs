use cdrs_tokio::types::ByIndex;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount, WritableAccount},
    clock::Epoch,
    inner_instruction::InnerInstructionsList,
    signature::Signature,
    transaction::{self, VersionedTransaction},
};
use solana_transaction_context::TransactionReturnData;

use crate::{convert::JobEffectAccountDiffOps, sync::JobEffects};

#[derive(Debug)]
pub struct SignatureRow {
    pub account: [u8; 32],
    pub signature: [u8; 64],
    pub slot: u64,
    pub block_unix_timestamp: u64,

    pub ordering: num_bigint::BigInt,
}

impl RowTy for SignatureRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.account)),
            format!("0x{}", hex::encode(self.signature)),
            self.slot.to_string(),
            self.block_unix_timestamp.to_string(),
            self.ordering.to_string(),
        ]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            account: {
                let bytes = row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec();
                bytes
                    .try_into()
                    .map_err(|_| cdrs_tokio::Error::General("Failed to convert account to [u8; 32]".into()))?
            },
            signature: {
                let bytes = row.r_by_index::<cdrs_tokio::types::blob::Blob>(1)?.into_vec();
                bytes
                    .try_into()
                    .map_err(|_| cdrs_tokio::Error::General("Failed to convert signature to [u8; 64]".into()))?
            },
            slot: row.r_by_index::<i64>(2).map(|v| v as u64)?,
            block_unix_timestamp: row.r_by_index::<i64>(3).map(|v| v as u64)?,
            ordering: row.r_by_index::<cdrs_tokio::types::decimal::Decimal>(4)?.as_plain(),
        })
    }
}

#[derive(Debug)]
pub struct TxRow {
    pub signature: [u8; 64],
    pub transaction: klickhouse::Bytes,
    pub result: klickhouse::Bytes,
    pub slot: u64,
    pub pre_accounts: klickhouse::Bytes,
    pub block_unix_timestamp: u64,
    pub ordering: BigInt,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerializableTxRow {
    pub signature: Vec<u8>,
    pub transaction: Vec<u8>,
    pub result: Vec<u8>,
    pub slot: u64,
    pub pre_accounts: Vec<u8>,
    pub block_unix_timestamp: u64,
    pub ordering: BigInt,
}

impl SerializableTxRow {
    pub fn get_signature(&self) -> Signature {
        Signature::try_from(self.signature.as_slice()).unwrap()
    }

    pub fn get_slot(&self) -> u64 {
        self.slot
    }

    pub fn get_transaction(&self) -> Result<VersionedTransaction, bincode::Error> {
        bincode::deserialize(&self.transaction)
    }

    pub fn get_result(&self) -> Result<TransactionExecutionDetailsSerializable, bincode::Error> {
        bincode::deserialize(&self.result)
    }

    pub fn get_pre_accounts(&self) -> Result<Vec<(Pubkey, Option<AccountSharedData>)>, bincode::Error> {
        bincode::deserialize(&self.pre_accounts)
    }
}

impl TxRow {
    pub fn to_serializable(&self) -> SerializableTxRow {
        SerializableTxRow {
            signature: self.signature.to_vec(),
            transaction: self.transaction.to_vec(),
            result: self.result.to_vec(),
            slot: self.slot,
            pre_accounts: self.pre_accounts.to_vec(),
            block_unix_timestamp: self.block_unix_timestamp,
            ordering: self.ordering.clone(),
        }
    }

    pub fn from_serializable(serialized: SerializableTxRow) -> Self {
        Self {
            signature: serialized.signature.try_into().unwrap(),
            transaction: serialized.transaction.into(),
            result: serialized.result.into(),
            slot: serialized.slot,
            pre_accounts: serialized.pre_accounts.into(),
            block_unix_timestamp: serialized.block_unix_timestamp,
            ordering: serialized.ordering,
        }
    }
}

impl RowTy for TxRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.signature)),
            format!("0x{}", hex::encode(self.transaction.to_vec())),
            format!("0x{}", hex::encode(self.result.to_vec())),
            self.slot.to_string(),
            format!("0x{}", hex::encode(self.pre_accounts.to_vec())),
            self.block_unix_timestamp.to_string(),
            self.ordering.to_string(),
        ]
    }

    fn try_from_row(_row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        unreachable!()
    }
}

#[derive(Debug)]
pub struct TxRowWithTimestamp {
    pub signature: klickhouse::Bytes,
    pub transaction: klickhouse::Bytes,
    pub result: klickhouse::Bytes,
    pub slot: u64,
    pub pre_accounts: klickhouse::Bytes,
    pub block_unix_timestamp: u64,
    pub ordering: BigInt,
}

impl TxRowWithTimestamp {
    pub fn from_tx_row(tx_row: TxRow) -> Self {
        Self {
            signature: klickhouse::Bytes::from(tx_row.signature.to_vec()),
            transaction: tx_row.transaction,
            result: tx_row.result,
            slot: tx_row.slot,
            pre_accounts: tx_row.pre_accounts,
            block_unix_timestamp: tx_row.block_unix_timestamp,
            ordering: tx_row.ordering,
        }
    }
}

impl RowTy for TxRowWithTimestamp {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.signature.to_vec())),
            format!("0x{}", hex::encode(self.transaction.to_vec())),
            format!("0x{}", hex::encode(self.result.to_vec())),
            self.slot.to_string(),
            format!("0x{}", hex::encode(self.pre_accounts.to_vec())),
            self.block_unix_timestamp.to_string(),
            self.ordering.to_string(),
        ]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            signature: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec()),
            transaction: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(1)?.into_vec()),
            result: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(2)?.into_vec()),
            slot: row.r_by_index::<i64>(3)? as u64,
            pre_accounts: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(4)?.into_vec()),
            block_unix_timestamp: row.r_by_index::<i64>(5)? as u64,
            ordering: row.r_by_index::<cdrs_tokio::types::decimal::Decimal>(6)?.as_plain(),
        })
    }
}

#[derive(Debug)]
pub struct SlotRow {
    pub slot: u64,
    pub block_unix_timestamp: u64,
    pub blockhash: klickhouse::Bytes,
    pub parent_blockhash: klickhouse::Bytes,
}

impl RowTy for SlotRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            self.slot.to_string(),
            self.block_unix_timestamp.to_string(),
            format!("0x{}", hex::encode(self.blockhash.to_vec())),
            format!("0x{}", hex::encode(self.parent_blockhash.to_vec())),
        ]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            slot: row.r_by_index::<i64>(0)? as u64,
            block_unix_timestamp: row.r_by_index::<i64>(1)? as u64,
            blockhash: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(2)?.into_vec()),
            parent_blockhash: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(3)?.into_vec()),
        })
    }
}

#[derive(Debug)]
pub struct AccountOwnerRow {
    pub account: [u8; 32],
    pub owner: [u8; 32],
}

impl RowTy for AccountOwnerRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.account)),
            format!("0x{}", hex::encode(self.owner)),
        ]
    }

    fn try_from_row(_row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        unreachable!()
    }
}

#[derive(Debug)]
pub struct ProgramAccountRow {
    pub program_id: [u8; 32],
    pub account: [u8; 32],
}

impl RowTy for ProgramAccountRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.program_id)),
            format!("0x{}", hex::encode(self.account)),
        ]
    }

    fn try_from_row(_row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        unreachable!()
    }
}

#[derive(Debug)]
pub struct AccountMintRow {
    pub account: [u8; 32],
    pub owner: [u8; 32],
    pub mint: [u8; 32],
    pub account_type: u8,
}

impl RowTy for AccountMintRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.account)),
            format!("0x{}", hex::encode(self.owner)),
            format!("0x{}", hex::encode(self.mint)),
            self.account_type.to_string(),
        ]
    }

    fn try_from_row(_row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        unreachable!()
    }
}

#[derive(Debug)]
pub struct SingleAccountRow {
    pub account: klickhouse::Bytes,
}

impl RowTy for SingleAccountRow {
    fn into_query_values(self) -> Vec<String> {
        vec![format!("0x{}", hex::encode(self.account.to_vec()))]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            account: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec()),
        })
    }
}

#[derive(Debug)]
pub struct AccountMintQueryRow {
    pub account: klickhouse::Bytes,
    pub mint: klickhouse::Bytes,
    pub account_type: u8,
}

impl RowTy for AccountMintQueryRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.account.to_vec())),
            format!("0x{}", hex::encode(self.mint.to_vec())),
            self.account_type.to_string(),
        ]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            account: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec()),
            mint: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(1)?.into_vec()),
            account_type: row.r_by_index::<i8>(2)? as u8,
        })
    }
}

#[derive(Debug)]
pub struct AccountMintQueryRowWithoutType {
    pub account: klickhouse::Bytes,
    pub mint: klickhouse::Bytes,
}

impl RowTy for AccountMintQueryRowWithoutType {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.account.to_vec())),
            format!("0x{}", hex::encode(self.mint.to_vec())),
        ]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            account: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec()),
            mint: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(1)?.into_vec()),
        })
    }
}

#[derive(Debug)]
pub struct SingleSignatureRow {
    pub signature: klickhouse::Bytes,
    pub ordering: BigInt,
}

#[derive(Debug)]
pub struct SingleOrderingRow {
    pub ordering: BigInt,
}

impl RowTy for SingleSignatureRow {
    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.signature.to_vec())),
            self.ordering.to_string(),
        ]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            signature: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec()),
            ordering: row.r_by_index::<cdrs_tokio::types::decimal::Decimal>(1)?.as_plain(),
        })
    }
}

impl RowTy for SingleOrderingRow {
    fn into_query_values(self) -> Vec<String> {
        vec![self.ordering.to_string()]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            ordering: row.r_by_index::<cdrs_tokio::types::decimal::Decimal>(0)?.as_plain(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AccountDataDiff {
    Lamports(u64),
    Executable(bool),
    Owner(Pubkey),
    RentEpoch(Epoch),
    Data(Vec<u8>),
}

impl AccountDataDiff {
    pub fn from_account(pre_account: &Option<AccountSharedData>, post_account: &AccountSharedData) -> Vec<Self> {
        let mut diffs = Vec::new();
        let post_lamports = post_account.lamports();
        if post_lamports != pre_account.as_ref().map_or(0, |a| a.lamports()) {
            diffs.push(Self::Lamports(post_lamports));
        }

        let post_executable = post_account.executable();
        if post_executable != pre_account.as_ref().is_some_and(|a| a.executable()) {
            diffs.push(Self::Executable(post_executable));
        }

        let post_owner = post_account.owner();
        if *post_owner != pre_account.as_ref().map_or(Pubkey::default(), |a| *a.owner()) {
            diffs.push(Self::Owner(*post_owner));
        }

        let post_rent_epoch = post_account.rent_epoch();
        if post_rent_epoch != pre_account.as_ref().map_or(Epoch::default(), |a| a.rent_epoch()) {
            diffs.push(Self::RentEpoch(post_rent_epoch));
        }

        let post_data = post_account.data();
        if post_data != pre_account.as_ref().map_or(Vec::new(), |a| a.data().to_vec()) {
            diffs.push(Self::Data(post_data.to_vec()));
        }
        diffs
    }

    pub fn to_account(self, account: &mut AccountSharedData) {
        match self {
            Self::Lamports(lamports) => account.set_lamports(lamports),
            Self::Executable(executable) => account.set_executable(executable),
            Self::Owner(owner) => account.set_owner(owner),
            Self::RentEpoch(rent_epoch) => account.set_rent_epoch(rent_epoch),
            Self::Data(data) => account.set_data_from_slice(&data),
        }
    }

    pub fn apply_to_account(&self, account: &mut AccountSharedData) {
        match self {
            Self::Lamports(lamports) => account.set_lamports(*lamports),
            Self::Executable(executable) => account.set_executable(*executable),
            Self::Owner(owner) => account.set_owner(*owner),
            Self::RentEpoch(rent_epoch) => account.set_rent_epoch(*rent_epoch),
            Self::Data(data) => account.set_data_from_slice(data),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionExecutionDetailsSerializable {
    pub status: transaction::Result<()>,
    pub log_messages: Option<Vec<String>>,
    pub inner_instructions: Option<InnerInstructionsList>,
    pub return_data: Option<TransactionReturnData>,
    pub executed_units: u64,
    /// The change in accounts data len for this transaction.
    /// NOTE: This value is valid IFF `status` is `Ok`.
    pub accounts_data_len_delta: i64,

    pub fee: u64,
    pub diffs: Vec<Vec<AccountDataDiff>>,
    pub pre_balances: Vec<u64>,
}

impl TransactionExecutionDetailsSerializable {
    pub fn from_job_effect_diff(job_effect: JobEffects) -> (Self, JobEffectAccountDiffOps) {
        let diff = job_effect.job_effect_diff;

        let JobEffects {
            status,
            log_messages,
            inner_instructions,
            return_data,
            executed_units,
            accounts_data_len_delta,
            fee,
            ..
        } = job_effect;

        (
            Self {
                status,
                log_messages,
                inner_instructions,
                return_data,
                executed_units,
                accounts_data_len_delta,
                fee,
                diffs: diff.diffs,
                pre_balances: diff.pre_balances,
            },
            diff.account_diff_ops,
        )
    }
}

pub trait RowTy: Sized + Send + Sync + 'static {
    fn into_query_values(self) -> Vec<String>;
    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error>;
}
