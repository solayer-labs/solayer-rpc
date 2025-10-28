#![allow(clippy::type_complexity)]

use solana_pubkey::Pubkey;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    system_program,
    transaction::SanitizedTransaction,
};
use solana_svm::{
    account_loader::FeesOnlyTransaction, rollback_accounts::RollbackAccounts,
    transaction_execution_result::ExecutedTransaction, transaction_processing_result::ProcessedTransaction,
};
use spl_token_2022::extension::StateWithExtensions;

use crate::{
    jobs::ConsumedJob,
    serializable::{AccountDataDiff, SignatureRow, TransactionExecutionDetailsSerializable, TxRow},
};

pub fn account_type_and_owner(account: &AccountSharedData) -> Option<(u8, Pubkey, Option<Pubkey>)> {
    let owner = account.owner();
    if *owner == spl_token::id() || *owner == spl_token_2022::id() {
        let parsed_account: Result<
            StateWithExtensions<'_, spl_token_2022::state::Account>,
            solana_sdk::program_error::ProgramError,
        > = StateWithExtensions::<spl_token_2022::state::Account>::unpack(account.data());
        if let Ok(account) = parsed_account {
            if *owner == spl_token::id() {
                Some((1, account.base.owner, Some(account.base.mint)))
            } else {
                Some((2, account.base.owner, Some(account.base.mint)))
            }
        } else {
            Some((0, *owner, None))
        }
    } else if *owner != system_program::id() {
        Some((0, *owner, None))
    } else {
        None
    }
}

// (program, owner, mint, pre_balance, post_balance)
pub fn token_balance_diff_from_diffs(
    data: &Option<AccountSharedData>,
    diffs: &[AccountDataDiff],
) -> Option<(Pubkey, Pubkey, Pubkey, u64, u64)> {
    let mut owner = *data.as_ref().map_or(&Pubkey::default(), |a| a.owner());
    for diff in diffs.iter() {
        if let AccountDataDiff::Owner(new_owner) = diff {
            owner = *new_owner;
        }
    }

    if owner != spl_token::id() && owner != spl_token_2022::id() {
        return None;
    }

    for diff in diffs.iter() {
        if let AccountDataDiff::Data(new_data) = diff {
            let pre_amount = match StateWithExtensions::<spl_token_2022::state::Account>::unpack(
                data.as_ref().map_or(&[0; 85], |a| a.data()),
            ) {
                Ok(token_account_data) => token_account_data.base.amount,
                Err(_) => 0,
            };

            let post_token_account_data = match StateWithExtensions::<spl_token_2022::state::Account>::unpack(new_data)
            {
                Ok(token_account_data) => token_account_data,
                Err(_) => return None,
            };

            return Some((
                owner,
                post_token_account_data.base.owner,
                post_token_account_data.base.mint,
                pre_amount,
                post_token_account_data.base.amount,
            ));
        }
    }
    None
}

pub fn calculate_diff_successful_tx(
    details: Box<ExecutedTransaction>,
    pre_accounts: &Vec<Option<AccountSharedData>>,
    transaction: &SanitizedTransaction,
) -> (
    Vec<(Pubkey, Option<AccountSharedData>)>, // Pre accounts
    Vec<Vec<AccountDataDiff>>,                // Account diffs
    Vec<u64>,                                 // Pre balances
    Vec<(Pubkey, Pubkey)>,                    // Account ops create
    Vec<(Pubkey, Pubkey)>,                    // Account ops delete
    Vec<(Pubkey, Pubkey, u8, Pubkey)>,        // Account ops mint create
    Vec<(Pubkey, Pubkey)>,                    // Account ops mint delete
) {
    let message = transaction.message();
    let mut pre_accounts_filtered = Vec::with_capacity(message.account_keys().len());
    let mut diffs = Vec::with_capacity(message.account_keys().len());
    let mut pre_balances = Vec::with_capacity(message.account_keys().len());
    let mut account_ops_create = Vec::with_capacity(message.account_keys().len());
    let mut account_ops_delete = Vec::with_capacity(message.account_keys().len());
    let mut account_ops_mint_create = Vec::with_capacity(message.account_keys().len());
    let mut account_ops_mint_delete = Vec::with_capacity(message.account_keys().len());
    for (i, ((addr, post_account), pre_account)) in
        details.loaded_transaction.accounts.iter().zip(pre_accounts).enumerate()
    {
        pre_balances.push(pre_account.as_ref().map_or(0, |a| a.lamports()));
        // Skip accounts that aren't writable or are invoked but not instruction
        // accounts
        if !message.is_writable(i) || (message.is_invoked(i) && !message.is_instruction_account(i)) {
            continue;
        }

        diffs.push(AccountDataDiff::from_account(pre_account, post_account));

        let pre_owner = *pre_account.as_ref().map(|p| p.owner()).unwrap_or(&Pubkey::default());
        let pre_account_type = pre_account
            .as_ref()
            .and_then(|a| account_type_and_owner(a).map(|(t, _, _)| t));

        if let Some((post_account_type, post_owner, post_mint)) = account_type_and_owner(post_account) {
            match (pre_owner == Pubkey::default(), post_owner == Pubkey::default()) {
                (true, false) => {
                    // New account creation
                    match post_mint {
                        Some(mint) => {
                            account_ops_mint_create.push((*addr, post_owner, post_account_type, mint));
                        }
                        None => {
                            account_ops_create.push((*addr, post_owner));
                        }
                    }
                }
                (false, true) => {
                    // Account deletion
                    match post_mint {
                        Some(_mint) => {
                            account_ops_mint_delete.push((*addr, pre_owner));
                        }
                        None => {
                            account_ops_delete.push((*addr, pre_owner));
                        }
                    }
                }
                (false, false) if pre_owner != post_owner || pre_account_type != Some(post_account_type) => {
                    // Owner or type update between non-default accounts
                    match post_mint {
                        Some(mint) => {
                            account_ops_mint_delete.push((*addr, pre_owner));
                            account_ops_mint_create.push((*addr, post_owner, post_account_type, mint));
                        }
                        None => {
                            account_ops_delete.push((*addr, pre_owner));
                            account_ops_create.push((*addr, post_owner));
                        }
                    }
                }
                _ => {}
            }
        }
        pre_accounts_filtered.push((addr.to_owned(), pre_account.clone()));
    }

    (
        pre_accounts_filtered,
        diffs,
        pre_balances,
        account_ops_create,
        account_ops_delete,
        account_ops_mint_create,
        account_ops_mint_delete,
    )
}

pub fn calculate_diff_successful_tx_for_processed_tx(
    pre_accounts: &[(Pubkey, Option<AccountSharedData>)],
    account_diffs: &Vec<Vec<AccountDataDiff>>,
) -> (
    Vec<(Pubkey, Pubkey)>,             // Account ops create
    Vec<(Pubkey, Pubkey)>,             // Account ops delete
    Vec<(Pubkey, Pubkey, u8, Pubkey)>, // Account ops mint create
    Vec<(Pubkey, Pubkey)>,             // Account ops mint delete
) {
    let mut account_ops_create = Vec::with_capacity(pre_accounts.len());
    let mut account_ops_delete = Vec::with_capacity(pre_accounts.len());
    let mut account_ops_mint_create = Vec::with_capacity(pre_accounts.len());
    let mut account_ops_mint_delete = Vec::with_capacity(pre_accounts.len());
    for ((addr, pre_account), account_diff) in pre_accounts.iter().zip(account_diffs) {
        let pre_owner = *pre_account.as_ref().map(|p| p.owner()).unwrap_or(&Pubkey::default());
        let pre_account_type = pre_account
            .as_ref()
            .and_then(|a| account_type_and_owner(a).map(|(t, _, _)| t));

        let mut post_account = pre_account.clone().unwrap_or_default();
        for diff in account_diff.iter() {
            diff.apply_to_account(&mut post_account);
        }

        if let Some((post_account_type, post_owner, post_mint)) = account_type_and_owner(&post_account) {
            match (pre_owner == Pubkey::default(), post_owner == Pubkey::default()) {
                (true, false) => {
                    // New account creation
                    match post_mint {
                        Some(mint) => {
                            account_ops_mint_create.push((*addr, post_owner, post_account_type, mint));
                        }
                        None => {
                            account_ops_create.push((*addr, post_owner));
                        }
                    }
                }
                (false, true) => {
                    // Account deletion
                    match post_mint {
                        Some(_mint) => {
                            account_ops_mint_delete.push((*addr, pre_owner));
                        }
                        None => {
                            account_ops_delete.push((*addr, pre_owner));
                        }
                    }
                }
                (false, false) if pre_owner != post_owner || pre_account_type != Some(post_account_type) => {
                    // Owner or type update between non-default accounts
                    match post_mint {
                        Some(mint) => {
                            account_ops_mint_delete.push((*addr, pre_owner));
                            account_ops_mint_create.push((*addr, post_owner, post_account_type, mint));
                        }
                        None => {
                            account_ops_delete.push((*addr, pre_owner));
                            account_ops_create.push((*addr, post_owner));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    (
        account_ops_create,
        account_ops_delete,
        account_ops_mint_create,
        account_ops_mint_delete,
    )
}

pub fn calculate_diff_unsuccessful_tx(
    details: Box<FeesOnlyTransaction>,
    transaction: &SanitizedTransaction,
) -> (Vec<(Pubkey, Option<AccountSharedData>)>, Vec<Vec<AccountDataDiff>>) {
    let fee_payer_address = transaction.message().fee_payer();
    let mut collected_accounts = Vec::new();
    let mut diffs = Vec::new();
    match details.rollback_accounts {
        RollbackAccounts::FeePayerOnly { fee_payer_account } => {
            diffs.push(vec![AccountDataDiff::Lamports(fee_payer_account.lamports())]);
            collected_accounts.push((*fee_payer_address, Some(fee_payer_account)));
        }
        RollbackAccounts::SameNonceAndFeePayer { nonce } => {
            collected_accounts.push((*nonce.address(), Some(nonce.account().clone())));
            diffs.push(vec![AccountDataDiff::Data(nonce.account().data().to_vec())]);
        }
        RollbackAccounts::SeparateNonceAndFeePayer {
            nonce,
            fee_payer_account,
        } => {
            diffs.push(vec![AccountDataDiff::Lamports(fee_payer_account.lamports())]);
            collected_accounts.push((*fee_payer_address, Some(fee_payer_account)));

            collected_accounts.push((*nonce.address(), Some(nonce.account().clone())));
            diffs.push(vec![AccountDataDiff::Data(nonce.account().data().to_vec())]);
        }
    }
    (collected_accounts, diffs)
}

pub fn to_tx_row(
    job: &ConsumedJob,
) -> (
    TxRow,
    Option<(
        Vec<(Pubkey, Pubkey)>,
        Vec<(Pubkey, Pubkey)>,
        Vec<(Pubkey, Pubkey, u8, Pubkey)>,
        Vec<(Pubkey, Pubkey)>,
    )>,
) {
    let accounts = job.sanitized_transaction.message().account_keys();
    let signature = *job.sanitized_transaction.signature();
    let signature_bytes: [u8; 64] = signature.into();

    // Pre-allocate signature rows based on accounts count
    let mut signature_rows = Vec::with_capacity(accounts.len());
    for signer in accounts.iter() {
        signature_rows.push(SignatureRow {
            account: signer.to_bytes(),
            signature: signature_bytes,
            slot: job.slot,
            block_unix_timestamp: job.timestamp,
            seq_number: job.job_id as u64,
        });
    }

    // Serialize transaction once to avoid duplication
    let serialized_tx = bincode::serialize(&job.sanitized_transaction.to_versioned_transaction()).unwrap_or_default();
    match job.processed_transaction.as_ref().unwrap() {
        ProcessedTransaction::Executed(execution_details) => {
            let (
                pre_accounts,
                diff,
                pre_balances,
                account_ops_create,
                account_ops_delete,
                account_ops_mint_create,
                account_ops_mint_delete,
            ) = calculate_diff_successful_tx(execution_details.clone(), &job.pre_accounts, &job.sanitized_transaction);

            let serialized_pre_accounts = bincode::serialize(&pre_accounts).unwrap_or_default();

            let details = TransactionExecutionDetailsSerializable::from_execution_details(
                execution_details.clone(),
                diff,
                pre_balances,
            );
            let serialized_result = bincode::serialize(&details).unwrap_or_default();

            (
                TxRow {
                    signature: signature_bytes,
                    transaction: klickhouse::Bytes::from(serialized_tx),
                    result: klickhouse::Bytes::from(serialized_result),
                    slot: job.slot,
                    seq_number: job.job_id as u64,
                    pre_accounts: klickhouse::Bytes::from(serialized_pre_accounts),
                    block_unix_timestamp: job.timestamp,
                },
                Some((
                    account_ops_create,
                    account_ops_delete,
                    account_ops_mint_create,
                    account_ops_mint_delete,
                )),
            )
        }
        ProcessedTransaction::FeesOnly(fees_only_tx) => {
            let (pre_accounts, diffs) =
                calculate_diff_unsuccessful_tx(fees_only_tx.clone(), &job.sanitized_transaction);

            let serialized_pre_accounts = bincode::serialize(&pre_accounts).unwrap_or_default();

            let details = TransactionExecutionDetailsSerializable {
                status: Err(fees_only_tx.load_error.clone()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
                diffs,
                fee: 0,
                pre_balances: pre_accounts
                    .iter()
                    .map(|(_, a)| a.as_ref().map_or(0, |a| a.lamports()))
                    .collect(),
            };
            let serialized_result = bincode::serialize(&details).unwrap_or_default();
            (
                TxRow {
                    signature: signature_bytes,
                    transaction: klickhouse::Bytes::from(serialized_tx),
                    result: klickhouse::Bytes::from(serialized_result),
                    slot: job.slot,
                    seq_number: job.job_id as u64,
                    pre_accounts: klickhouse::Bytes::from(serialized_pre_accounts),
                    block_unix_timestamp: job.timestamp,
                },
                None,
            )
        }
    }
}

pub fn to_signature_rows(job: &ConsumedJob) -> Vec<SignatureRow> {
    let accounts = job.sanitized_transaction.message().account_keys();
    let signature = *job.sanitized_transaction.signature();
    let signature_bytes: [u8; 64] = signature.into();

    let mut signature_rows = Vec::with_capacity(accounts.len());
    for signer in accounts.iter() {
        signature_rows.push(SignatureRow {
            account: signer.to_bytes(),
            signature: signature_bytes,
            slot: job.slot,
            block_unix_timestamp: job.timestamp,
            seq_number: job.job_id as u64,
        });
    }
    signature_rows
}
