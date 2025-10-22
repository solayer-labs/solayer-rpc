use std::pin::Pin;

use async_trait::async_trait;
use cdrs_tokio::types::ByIndex;
use klickhouse::Row;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount, WritableAccount},
    clock::Epoch,
    inner_instruction::InnerInstructionsList,
    signature::Signature,
    transaction::{self, VersionedTransaction},
};
use solana_svm::transaction_execution_result::ExecutedTransaction;
use solana_transaction_context::TransactionReturnData;
use tokio_postgres::binary_copy::BinaryCopyInWriter;

use crate::convert::calculate_diff_successful_tx_for_processed_tx;

macro_rules! _fixed_size_array_to_cassandra_tuple {
    ($array:expr, 32) => {
        (
            $array[0], $array[1], $array[2], $array[3], $array[4], $array[5], $array[6], $array[7], $array[8],
            $array[9], $array[10], $array[11], $array[12], $array[13], $array[14], $array[15], $array[16], $array[17],
            $array[18], $array[19], $array[20], $array[21], $array[22], $array[23], $array[24], $array[25], $array[26],
            $array[27], $array[28], $array[29], $array[30], $array[31],
        )
    };
    ($array:expr, 64) => {
        (
            $array[0], $array[1], $array[2], $array[3], $array[4], $array[5], $array[6], $array[7], $array[8],
            $array[9], $array[10], $array[11], $array[12], $array[13], $array[14], $array[15], $array[16], $array[17],
            $array[18], $array[19], $array[20], $array[21], $array[22], $array[23], $array[24], $array[25], $array[26],
            $array[27], $array[28], $array[29], $array[30], $array[31], $array[32], $array[33], $array[34], $array[35],
            $array[36], $array[37], $array[38], $array[39], $array[40], $array[41], $array[42], $array[43], $array[44],
            $array[45], $array[46], $array[47], $array[48], $array[49], $array[50], $array[51], $array[52], $array[53],
            $array[54], $array[55], $array[56], $array[57], $array[58], $array[59], $array[60], $array[61], $array[62],
            $array[63],
        )
    };
}

#[derive(Row, Debug)]
pub struct SignatureRow {
    pub account: [u8; 32],
    pub signature: [u8; 64],
    pub slot: u64,
    pub block_unix_timestamp: u64,
    pub seq_number: u64,
}

#[async_trait]
impl RowTy for SignatureRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::BYTEA, // account
            tokio_postgres::types::Type::BYTEA, // signature
            tokio_postgres::types::Type::INT8,  // slot
            tokio_postgres::types::Type::INT8,  // block_unix_timestamp
            tokio_postgres::types::Type::INT8,  // seq_number
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer
            .write(&[
                &self.account.to_vec(),
                &self.signature.to_vec(),
                &(self.slot as i64),
                &(self.block_unix_timestamp as i64),
                &(self.seq_number as i64),
            ])
            .await?;
        Ok(())
    }

    fn from_postgres(_: tokio_postgres::Row) -> Self {
        unreachable!()
    }

    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.account)),
            format!("0x{}", hex::encode(self.signature)),
            self.slot.to_string(),
            self.block_unix_timestamp.to_string(),
            self.seq_number.to_string(),
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
            seq_number: row.r_by_index::<i64>(4).map(|v| v as u64)?,
        })
    }
}

#[derive(Row, Debug)]
pub struct TxRow {
    pub signature: [u8; 64],
    pub transaction: klickhouse::Bytes,
    pub result: klickhouse::Bytes,
    pub slot: u64,
    pub pre_accounts: klickhouse::Bytes,
    pub block_unix_timestamp: u64,
    pub seq_number: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerializableTxRow {
    pub signature: Vec<u8>,
    pub transaction: Vec<u8>,
    pub result: Vec<u8>,
    pub slot: u64,
    pub pre_accounts: Vec<u8>,
    pub block_unix_timestamp: u64,
    pub seq_number: u64,
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

    pub fn get_all_rows(
        &self,
    ) -> Result<
        (
            Vec<SignatureRow>,
            Vec<(Pubkey, Pubkey)>,
            Vec<(Pubkey, Pubkey)>,
            Vec<(Pubkey, Pubkey, u8, Pubkey)>,
            Vec<(Pubkey, Pubkey)>,
        ),
        bincode::Error,
    > {
        // we dont have ALT
        let transaction = self.get_transaction()?;
        let accounts = transaction.message.static_account_keys();
        let signature = transaction
            .signatures
            .first()
            .ok_or_else(|| bincode::Error::new(bincode::ErrorKind::Custom("No signatures found".to_string())))?;
        let slot = self.get_slot();
        let block_unix_timestamp = self.block_unix_timestamp;
        let seq_number = self.seq_number;

        let signature_rows = accounts
            .iter()
            .filter(|account| **account != Pubkey::new_from_array([0u8; 32]))
            .map(|account| SignatureRow {
                account: account.to_bytes(),
                signature: *signature.as_array(),
                slot,
                block_unix_timestamp,
                seq_number,
            });

        let pre_accounts = self.get_pre_accounts()?;
        let account_diffs = self.get_result()?.diffs;
        let (account_ops_create, account_ops_delete, account_ops_mint_create, account_ops_mint_delete) =
            calculate_diff_successful_tx_for_processed_tx(&pre_accounts, &account_diffs);

        Ok((
            signature_rows.collect(),
            account_ops_create,
            account_ops_delete,
            account_ops_mint_create,
            account_ops_mint_delete,
        ))
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
            seq_number: self.seq_number,
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
            seq_number: serialized.seq_number,
        }
    }
}

impl PartialEq for TxRow {
    fn eq(&self, other: &Self) -> bool {
        self.seq_number == other.seq_number
    }
}

impl PartialOrd for TxRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.seq_number.cmp(&other.seq_number))
    }
}

impl Eq for TxRow {}

impl Ord for TxRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq_number.cmp(&other.seq_number)
    }
}

#[async_trait]
impl RowTy for TxRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::BYTEA, // signature
            tokio_postgres::types::Type::BYTEA, // transaction
            tokio_postgres::types::Type::BYTEA, // result
            tokio_postgres::types::Type::INT8,  // slot
            tokio_postgres::types::Type::BYTEA, // pre_accounts
            tokio_postgres::types::Type::INT8,  // block_unix_timestamp
            tokio_postgres::types::Type::INT8,  // seq_number
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer
            .write(&[
                &self.signature.to_vec(),
                &self.transaction.to_vec(),
                &self.result.to_vec(),
                &(self.slot as i64),
                &self.pre_accounts.to_vec(),
                &(self.block_unix_timestamp as i64),
                &(self.seq_number as i64),
            ])
            .await?;
        Ok(())
    }

    fn from_postgres(_: tokio_postgres::Row) -> Self {
        unreachable!()
    }

    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.signature)),
            format!("0x{}", hex::encode(self.transaction.to_vec())),
            format!("0x{}", hex::encode(self.result.to_vec())),
            self.slot.to_string(),
            format!("0x{}", hex::encode(self.pre_accounts.to_vec())),
            self.block_unix_timestamp.to_string(),
            self.seq_number.to_string(),
        ]
    }

    fn try_from_row(_row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        unreachable!()
    }
}

#[derive(Row, Debug)]
pub struct TxRowWithTimestamp {
    pub signature: klickhouse::Bytes,
    pub transaction: klickhouse::Bytes,
    pub result: klickhouse::Bytes,
    pub slot: u64,
    pub pre_accounts: klickhouse::Bytes,
    pub block_unix_timestamp: u64,
    pub seq_number: u64,
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
            seq_number: tx_row.seq_number,
        }
    }
}

macro_rules! to_klickhouse_bytes {
    ($row:expr, $col:expr) => {
        klickhouse::Bytes::from($row.get::<_, &[u8]>($col).to_vec())
    };
}

macro_rules! to_u64 {
    ($row:expr, $col:expr) => {
        $row.get::<_, i64>($col) as u64
    };
}

#[async_trait]
impl RowTy for TxRowWithTimestamp {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::BYTEA, // signature
            tokio_postgres::types::Type::BYTEA, // transaction
            tokio_postgres::types::Type::BYTEA, // result
            tokio_postgres::types::Type::INT8,  // slot
            tokio_postgres::types::Type::BYTEA, // pre_accounts
            tokio_postgres::types::Type::INT8,  // block_unix_timestamp
            tokio_postgres::types::Type::INT8,  // seq_number
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer
            .write(&[
                &self.signature.to_vec(),
                &self.transaction.to_vec(),
                &self.result.to_vec(),
                &(self.slot as i64),
                &self.pre_accounts.to_vec(),
                &(self.block_unix_timestamp as i64),
                &(self.seq_number as i64),
            ])
            .await?;
        Ok(())
    }

    fn from_postgres(row: tokio_postgres::Row) -> Self {
        Self {
            signature: to_klickhouse_bytes!(row, 0),
            transaction: to_klickhouse_bytes!(row, 1),
            result: to_klickhouse_bytes!(row, 2),
            slot: to_u64!(row, 3),
            pre_accounts: to_klickhouse_bytes!(row, 4),
            block_unix_timestamp: to_u64!(row, 5),
            seq_number: to_u64!(row, 6),
        }
    }

    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.signature.to_vec())),
            format!("0x{}", hex::encode(self.transaction.to_vec())),
            format!("0x{}", hex::encode(self.result.to_vec())),
            self.slot.to_string(),
            format!("0x{}", hex::encode(self.pre_accounts.to_vec())),
            self.block_unix_timestamp.to_string(),
            self.seq_number.to_string(),
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
            seq_number: row.r_by_index::<i64>(6)? as u64,
        })
    }
}

#[derive(Row, Debug)]
pub struct SlotRow {
    pub slot: u64,
    pub block_unix_timestamp: u64,
    pub blockhash: klickhouse::Bytes,
    pub parent_blockhash: klickhouse::Bytes,
}

#[async_trait]
impl RowTy for SlotRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::INT8,  // slot
            tokio_postgres::types::Type::INT8,  // block_unix_timestamp
            tokio_postgres::types::Type::BYTEA, // blockhash
            tokio_postgres::types::Type::BYTEA, // parent_blockhash
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer
            .write(&[
                &(self.slot as i64),
                &(self.block_unix_timestamp as i64),
                &self.blockhash.to_vec(),
                &self.parent_blockhash.to_vec(),
            ])
            .await?;
        Ok(())
    }

    fn from_postgres(row: tokio_postgres::Row) -> Self {
        Self {
            slot: to_u64!(row, 0),
            block_unix_timestamp: to_u64!(row, 1),
            blockhash: to_klickhouse_bytes!(row, 2),
            parent_blockhash: to_klickhouse_bytes!(row, 3),
        }
    }

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

#[derive(Row, Debug)]
pub struct AccountOwnerRow {
    pub account: [u8; 32],
    pub owner: [u8; 32],
}

#[async_trait]
impl RowTy for AccountOwnerRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::BYTEA, // account
            tokio_postgres::types::Type::BYTEA, // owner
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer.write(&[&self.account.to_vec(), &self.owner.to_vec()]).await?;
        Ok(())
    }

    fn from_postgres(_: tokio_postgres::Row) -> Self {
        unreachable!()
    }

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

#[derive(Row, Debug)]
pub struct AccountMintRow {
    pub account: [u8; 32],
    pub owner: [u8; 32],
    pub mint: [u8; 32],
    pub account_type: u8,
}

#[async_trait]
impl RowTy for AccountMintRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::BYTEA, // account
            tokio_postgres::types::Type::BYTEA, // owner
            tokio_postgres::types::Type::BYTEA, // mint
            tokio_postgres::types::Type::INT2,  // account_type
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer
            .write(&[
                &self.account.to_vec(),
                &self.owner.to_vec(),
                &self.mint.to_vec(),
                &(self.account_type as i16),
            ])
            .await?;
        Ok(())
    }

    fn from_postgres(_: tokio_postgres::Row) -> Self {
        unreachable!()
    }

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

#[derive(Row, Debug)]
pub struct SingleAccountRow {
    pub account: klickhouse::Bytes,
}

#[async_trait]
impl RowTy for SingleAccountRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::BYTEA, // account
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer.write(&[&self.account.to_vec()]).await?;
        Ok(())
    }

    fn from_postgres(row: tokio_postgres::Row) -> Self {
        Self {
            account: to_klickhouse_bytes!(row, 0),
        }
    }

    fn into_query_values(self) -> Vec<String> {
        vec![format!("0x{}", hex::encode(self.account.to_vec()))]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            account: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec()),
        })
    }
}

#[derive(Row, Debug)]
pub struct SingleSignatureRow {
    pub signature: klickhouse::Bytes,
    pub seq_number: u64,
}

#[derive(Row, Debug)]
pub struct SingleSeqNumberRow {
    pub seq_number: u64,
}

#[async_trait]
impl RowTy for SingleSignatureRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![
            tokio_postgres::types::Type::BYTEA, // signature
            tokio_postgres::types::Type::INT8,  // seq_number
        ]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer
            .write(&[&self.signature.to_vec(), &(self.seq_number as i64)])
            .await?;
        Ok(())
    }

    fn from_postgres(row: tokio_postgres::Row) -> Self {
        Self {
            signature: to_klickhouse_bytes!(row, 0),
            seq_number: to_u64!(row, 1),
        }
    }

    fn into_query_values(self) -> Vec<String> {
        vec![
            format!("0x{}", hex::encode(self.signature.to_vec())),
            self.seq_number.to_string(),
        ]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            signature: klickhouse::Bytes::from(row.r_by_index::<cdrs_tokio::types::blob::Blob>(0)?.into_vec()),
            seq_number: row.r_by_index::<i64>(1).unwrap_or(0) as u64,
        })
    }
}

#[async_trait]
impl RowTy for SingleSeqNumberRow {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type> {
        vec![tokio_postgres::types::Type::INT8]
    }

    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error> {
        writer.write(&[&(self.seq_number as i64)]).await?;
        Ok(())
    }

    fn from_postgres(row: tokio_postgres::Row) -> Self {
        Self {
            seq_number: to_u64!(row, 0),
        }
    }

    fn into_query_values(self) -> Vec<String> {
        vec![self.seq_number.to_string()]
    }

    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error> {
        Ok(Self {
            seq_number: row.r_by_index::<i64>(0).unwrap_or(0) as u64,
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
    pub fn from_execution_details(
        details: Box<ExecutedTransaction>,
        diffs: Vec<Vec<AccountDataDiff>>,
        pre_balances: Vec<u64>,
    ) -> Self {
        let fee = details.loaded_transaction.fee_details.total_fee();
        let details = details.execution_details;
        Self {
            status: details.status,
            log_messages: details.log_messages,
            inner_instructions: details.inner_instructions,
            return_data: details.return_data,
            executed_units: details.executed_units,
            accounts_data_len_delta: details.accounts_data_len_delta,
            fee,
            diffs,
            pre_balances,
        }
    }
}

#[async_trait]
pub trait RowTy: Row + Send + Sync + 'static {
    fn postgres_types(&self) -> Vec<tokio_postgres::types::Type>;
    async fn write_to_postgres(&self, writer: Pin<&mut BinaryCopyInWriter>) -> Result<(), tokio_postgres::Error>;
    fn from_postgres(row: tokio_postgres::Row) -> Self;
    fn into_query_values(self) -> Vec<String>;
    fn try_from_row(row: &cassandra_protocol::types::rows::Row) -> Result<Self, cdrs_tokio::Error>;
}
