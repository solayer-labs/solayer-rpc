use solana_sdk::{
    account::AccountSharedData,
    clock::Slot,
    transaction::{SanitizedTransaction, TransactionError},
};
use solana_svm::transaction_processing_result::ProcessedTransaction;

use std::hash::{Hash, Hasher};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionPriorityId {
    pub priority: u64,
    pub id: TransactionId,
    pub timestamp: u64,
    pub num_tries: u64,
}

impl TransactionPriorityId {
    pub fn new(priority: u64, id: TransactionId, timestamp: u64) -> Self {
        Self {
            priority,
            id,
            timestamp,
            num_tries: 0,
        }
    }

    #[inline(always)]
    pub fn increment_tries(&mut self) {
        self.num_tries += 1;
    }
}

impl Hash for TransactionPriorityId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

pub struct ConsumedJob {
    pub job_id: usize,
    pub processed_transaction: Result<ProcessedTransaction, TransactionError>,
    pub sanitized_transaction: SanitizedTransaction,
    pub transaction_id: TransactionId, // used for pruning
    pub slot: Slot,
    pub timestamp: u64,
    pub pre_accounts: Vec<Option<AccountSharedData>>,
}

impl Clone for ConsumedJob {
    fn clone(&self) -> Self {
        Self {
            job_id: self.job_id,
            sanitized_transaction: self.sanitized_transaction.clone(),
            transaction_id: self.transaction_id,
            slot: self.slot,
            timestamp: self.timestamp,
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
