use std::{
    sync::{Arc, LazyLock, RwLock},
    time::Instant,
};

use crossbeam_channel::{Receiver, Sender};
use infinisvm_logger::{error, info};
use infinisvm_types::{
    jobs::{ConsumedJob, TransactionId},
    tid::TransactionBatchId,
};
use solana_sdk::transaction::SanitizedTransaction;
use solana_svm::transaction_processor::{LoadAndExecuteSanitizedTransactionsOutput, TransactionProcessingConfig};

use crate::{bank::Bank, metrics::WorkerMetrics};

// Global metrics instance for worker
static WORKER_METRICS: LazyLock<WorkerMetrics> = LazyLock::new(WorkerMetrics::default);

// txs with transaction_id, job_id, batch_id
// job_id can be duplicate. batch_id is unique.
pub type ScheduledJob = (Vec<SanitizedTransaction>, Vec<TransactionId>, usize, TransactionBatchId);

pub struct Worker<'a> {
    transaction_processing_config: TransactionProcessingConfig<'a>,
    job_receiver: Receiver<ScheduledJob>,
    id: usize,
}

impl<'a> Worker<'a> {
    pub fn new(
        transaction_processing_config: TransactionProcessingConfig<'a>,
        job_receiver: Receiver<ScheduledJob>,
        id: usize,
    ) -> Self {
        Self {
            transaction_processing_config,
            job_receiver,
            id,
        }
    }

    pub fn run_loop(
        &self,
        bank: Arc<RwLock<Bank>>,
        // worker -> scheduler for unlocking accounts
        callback_sender: Sender<(Vec<ConsumedJob>, TransactionBatchId)>,
    ) {
        #[cfg(not(feature = "no_bind_cpu"))]
        if let Some(cores) = core_affinity::get_core_ids() {
            // we have cpu 2 for scheduler
            core_affinity::set_for_current(cores[self.id + 3]);
        }

        // A reusable buffer for all keys
        let mut num_txs_executed = 0;
        let mut instant = Instant::now();
        loop {
            if let Ok((sanitized_txs, transaction_ids, job_id, batch_id)) = self.job_receiver.recv() {
                let loop_start = Instant::now();

                // update sysvar
                let bank_read = bank.read().unwrap();
                let transaction_processor = bank_read.get_transaction_processor();
                let (slot, _, timestamp) = bank_read.get_latest_slot_hash_timestamp();

                let sysvar_time = loop_start.elapsed();
                WORKER_METRICS.record_sysvar_time(sysvar_time);

                let check_results = bank_read.check_results(&sanitized_txs, false);

                let check_time = loop_start.elapsed() - sysvar_time;
                WORKER_METRICS.record_check_time(check_time);

                let LoadAndExecuteSanitizedTransactionsOutput {
                    error_metrics: _,
                    execute_timings: _,
                    processing_results,
                } = {
                    transaction_processor.load_and_execute_sanitized_transactions(
                        &*bank_read,
                        &sanitized_txs,
                        check_results,
                        &bank_read.get_transaction_processing_environment(),
                        &self.transaction_processing_config,
                    )
                };

                let execute_time = loop_start.elapsed() - check_time - sysvar_time;
                WORKER_METRICS.record_execute_time(execute_time);

                let num_txs = sanitized_txs.len();

                let db = bank_read.db_cloned();
                let all_keys = sanitized_txs
                    .iter()
                    .flat_map(|tx| tx.message().account_keys().iter().copied())
                    .collect::<Vec<_>>();
                let mut all_account_data = db
                    .read()
                    .unwrap()
                    .bulk_read_account(all_keys)
                    .unwrap_or_default()
                    .into_iter();
                drop(db);
                drop(bank_read);

                let mut commit_batch = Vec::with_capacity(num_txs);

                let mut num_actual_executed = 0;
                for ((tx_result, tx), tx_id) in processing_results
                    .into_iter()
                    .zip(sanitized_txs.into_iter())
                    .zip(transaction_ids.into_iter())
                {
                    match tx_result {
                        Ok(tx_result) => {
                            num_actual_executed += 1;
                            // Pre-allocate account keys vector
                            let num_account_keys = tx.message().account_keys().len();
                            let mut pre_accounts = Vec::with_capacity(num_account_keys);
                            for _ in 0..num_account_keys {
                                pre_accounts.push(all_account_data.next().unwrap().1);
                            }

                            commit_batch.push(ConsumedJob {
                                job_id,
                                processed_transaction: Ok(tx_result),
                                slot,
                                timestamp,
                                pre_accounts,
                                sanitized_transaction: tx,
                                transaction_id: tx_id,
                                worker_id: self.id,
                            });
                        }
                        Err(load_err) => {
                            for _ in 0..tx.message().account_keys().len() {
                                let _ = all_account_data.next().unwrap();
                            }

                            error!(
                                signature = ?tx.signature(),
                                hash = ?tx.message().recent_blockhash(),
                                "Failed to execute tx: {:#?}", load_err);

                            commit_batch.push(ConsumedJob {
                                job_id,
                                processed_transaction: Err(load_err),
                                slot,
                                timestamp,
                                pre_accounts: Vec::new(),
                                sanitized_transaction: tx,
                                transaction_id: tx_id,
                                worker_id: self.id,
                            });
                        }
                    }
                }

                let prepare_commit_time = loop_start.elapsed() - execute_time - check_time - sysvar_time;
                WORKER_METRICS.record_prepare_commit_time(prepare_commit_time);
                {
                    bank.write().unwrap().commit_transactions(&commit_batch, self.id);
                }

                let job_len = callback_sender.len();
                // Send results through channel
                callback_sender.send((commit_batch, batch_id)).unwrap();
                let send_time = loop_start.elapsed() - prepare_commit_time - execute_time - check_time - sysvar_time;
                WORKER_METRICS.record_send_time(send_time);

                num_txs_executed += num_actual_executed;
                WORKER_METRICS.increment_transactions_executed(num_actual_executed as u64);

                // Record total loop time
                WORKER_METRICS.record_total_loop_time(loop_start.elapsed());

                // Update job queue length
                WORKER_METRICS.set_job_queue_length(self.job_receiver.len());

                // print txs/s every second
                if instant.elapsed().as_millis() >= 400 {
                    let elapsed = instant.elapsed();
                    let nanos = elapsed.as_nanos();
                    let txs_per_nano = num_txs_executed as f64 / nanos as f64;
                    let txs_per_sec = txs_per_nano * 1_000_000_000f64;

                    // Update transactions per second metric
                    WORKER_METRICS.set_transactions_per_second(txs_per_sec);

                    info!(
                        "({}) ({}) {} txs in {} ms = {} txs/s\nTiming breakdown:\n  sysvar: {:?}\n  check: {:?}\n  execute: {:?}\n  prepare_commit: {:?}\n  send: {:?} {}\n  total: {:?}",
                        slot,
                        self.id,
                        num_txs_executed,
                        elapsed.as_millis(),
                        txs_per_sec,
                        sysvar_time,
                        check_time,
                        execute_time,
                        prepare_commit_time,
                        send_time,
                        job_len,
                        loop_start.elapsed()
                    );
                    instant = Instant::now();
                    num_txs_executed = 0;
                }
            }

            if self.job_receiver.len() > 100 {
                // info!("job len: {}", self.job_receiver.len());
            }
        }
    }
}
