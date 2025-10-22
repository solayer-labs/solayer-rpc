pub mod db;
pub mod db_args;
pub mod in_memory;
pub mod s3;

mod metrics;

pub use infinisvm_types::{BlockWithSignatures, BlockWithTransactions, TransactionWithMetadata};
use solana_sdk::transaction::SanitizedTransaction;
use solana_svm::{
    transaction_execution_result::InnerInstructionsList, transaction_processing_result::ProcessedTransaction,
};
use solana_transaction_status_client_types::{InnerInstruction, InnerInstructions, TransactionStatusMeta};

pub fn map_inner_instructions(inner_instructions: InnerInstructionsList) -> impl Iterator<Item = InnerInstructions> {
    inner_instructions
        .into_iter()
        .enumerate()
        .map(|(index, instructions)| InnerInstructions {
            index: index as u8,
            instructions: instructions
                .into_iter()
                .map(|info| InnerInstruction {
                    stack_height: Some(u32::from(info.stack_height)),
                    instruction: info.instruction,
                })
                .collect(),
        })
        .filter(|i| !i.instructions.is_empty())
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
