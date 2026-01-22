use solana_hash::Hash;
use solana_sdk::{
    account::AccountSharedData,
    clock::Slot,
    transaction::{SanitizedTransaction, TransactionError},
};
use solana_svm::transaction_processing_result::ProcessedTransaction;

use crate::{convert::JobEffectDiff, sync::JobEffects, transaction_id::TransactionId};

pub struct ConsumedJob {
    pub worker_id: usize,
    pub processed_transaction: Result<ProcessedTransaction, TransactionError>,
    pub sanitized_transaction: SanitizedTransaction,
    pub transaction_id: TransactionId, // used for pruning
    pub slot: Slot,
    pub timestamp: u64,
    pub blockhash: Hash,
    pub parent_blockhash: Hash,
    pub pre_accounts: Vec<Option<AccountSharedData>>,
}

impl std::fmt::Debug for ConsumedJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumedJob")
            .field("worker_id", &self.worker_id)
            .field("transaction_id", &self.transaction_id)
            .field("slot", &self.slot)
            .field("timestamp", &self.timestamp)
            .field("processed_transaction_ok", &self.processed_transaction.is_ok())
            .finish()
    }
}

impl Clone for ConsumedJob {
    fn clone(&self) -> Self {
        Self {
            worker_id: self.worker_id,
            sanitized_transaction: self.sanitized_transaction.clone(),
            transaction_id: self.transaction_id,
            slot: self.slot,
            timestamp: self.timestamp,
            blockhash: self.blockhash,
            parent_blockhash: self.parent_blockhash,
            pre_accounts: self.pre_accounts.clone(),
            processed_transaction: match &self.processed_transaction {
                Ok(ProcessedTransaction::Executed(executed_transaction)) => {
                    Ok(ProcessedTransaction::Executed(executed_transaction.clone()))
                }
                Ok(ProcessedTransaction::FeesOnly(fees_only_transaction)) => {
                    Ok(ProcessedTransaction::FeesOnly(fees_only_transaction.clone()))
                }
                Err(transaction_error) => Err(transaction_error.clone()),
            },
        }
    }
}

impl ConsumedJob {
    pub fn into_job_effects(self) -> JobEffects {
        let ConsumedJob {
            processed_transaction,
            sanitized_transaction,
            pre_accounts,
            ..
        } = self;

        let versioned_tx = sanitized_transaction.to_versioned_transaction();

        match processed_transaction {
            Ok(processed_transaction) => {
                let fee = processed_transaction.fee_details().total_fee();
                let job_effect_diff = JobEffectDiff::from_processed_transaction(
                    &processed_transaction,
                    &pre_accounts,
                    &sanitized_transaction,
                );
                let (status, log_messages, inner_instructions, return_data, executed_units, accounts_data_len_delta) =
                    match processed_transaction {
                        ProcessedTransaction::Executed(executed_transaction) => {
                            let details = executed_transaction.execution_details;
                            (
                                details.status,
                                details.log_messages,
                                details.inner_instructions,
                                details.return_data,
                                details.executed_units,
                                details.accounts_data_len_delta,
                            )
                        }
                        ProcessedTransaction::FeesOnly(fees_only_transaction) => {
                            (Err(fees_only_transaction.load_error), None, None, None, 0, 0)
                        }
                    };

                JobEffects {
                    versioned_tx,
                    execution_result: Ok(()),
                    job_effect_diff,
                    status,
                    log_messages,
                    inner_instructions,
                    return_data,
                    executed_units,
                    accounts_data_len_delta,
                    fee,
                }
            }
            Err(err) => JobEffects {
                versioned_tx,
                execution_result: Err(err.clone()),
                job_effect_diff: JobEffectDiff::default(),
                status: Err(err.clone()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
                fee: 0,
            },
        }
    }
}
