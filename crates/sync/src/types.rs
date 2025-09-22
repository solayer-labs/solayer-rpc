use infinisvm_types::{convert::to_tx_row, jobs::ConsumedJob, serializable::SerializableTxRow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SerializableBatch {
    pub slot: u64,
    pub timestamp: u64,
    pub job_id: usize,
    pub transactions: Vec<SerializableTxRow>,
}

impl SerializableBatch {
    pub fn from_consumed_jobs(jobs: &[ConsumedJob]) -> Self {
        if jobs.is_empty() {
            return Self {
                slot: 0,
                timestamp: 0,
                job_id: 0,
                transactions: Vec::new(),
            };
        }

        let first_job = &jobs[0];
        // Only include successfully processed transactions
        let transactions: Vec<SerializableTxRow> = jobs
            .iter()
            .filter(|job| job.processed_transaction.is_ok())
            .map(|job| {
                let (tx_row, _) = to_tx_row(job);
                tx_row.to_serializable()
            })
            .collect();

        Self {
            slot: first_job.slot,
            timestamp: first_job.timestamp,
            job_id: first_job.job_id,
            transactions,
        }
    }
}
