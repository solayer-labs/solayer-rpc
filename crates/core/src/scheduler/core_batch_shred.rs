use infinisvm_types::sync::{ShredId, SyncBatchShred};

use crate::scheduler::consumed_job::ConsumedJob;

#[derive(Debug)]
pub(crate) struct CoreBatchShred {
    pub shred_id: ShredId,
    pub worker_id: usize,
    pub jobs: Vec<ConsumedJob>,
}

impl CoreBatchShred {
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn slot(&self) -> u64 {
        self.jobs.first().map(|job| job.slot).unwrap_or_default()
    }

    pub fn into_sync_batch_shred(self) -> SyncBatchShred {
        let mut shred = SyncBatchShred {
            shred_id: self.shred_id,
            worker_id: self.worker_id,
            effects: self
                .jobs
                .into_iter()
                .filter(|job| job.processed_transaction.is_ok())
                .map(|job| job.into_job_effects())
                .collect(),
            shred_hash: [0u8; 32],
        };
        shred.shred_hash = shred.compute_shred_hash();
        shred
    }
}
