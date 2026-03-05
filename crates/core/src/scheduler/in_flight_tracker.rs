use ahash::{HashMap, HashMapExt};

use super::thread_aware_account_locks::ThreadId;
use crate::scheduler::transaction_batch_id::TransactionBatchId;

pub struct InFlightTracker {
    num_in_flight_per_thread: Vec<usize>,
    batches: HashMap<TransactionBatchId, BatchEntry>,
    batch_id_generator: BatchIdGenerator,
}

impl InFlightTracker {
    pub fn new(num_threads: usize) -> Self {
        Self {
            num_in_flight_per_thread: vec![0; num_threads],
            batches: HashMap::new(),
            batch_id_generator: BatchIdGenerator::default(),
        }
    }

    pub fn in_flight_for_thread(&self, thread_id: ThreadId) -> usize {
        self.num_in_flight_per_thread[thread_id]
    }

    pub fn is_thread_idle(&self, thread_id: ThreadId) -> bool {
        self.in_flight_for_thread(thread_id) == 0
    }

    /// Tracks number of transactions and CUs in-flight for the `thread_id`.
    /// Returns a `TransactionBatchId` that can be used to stop tracking the
    /// batch when it is complete.
    pub fn track_batch(&mut self, num_transactions: usize, thread_id: ThreadId) -> TransactionBatchId {
        let batch_id = self.batch_id_generator.next();
        self.num_in_flight_per_thread[thread_id] += num_transactions;
        self.batches.insert(
            batch_id,
            BatchEntry {
                thread_id,
                num_transactions,
            },
        );

        batch_id
    }

    /// Stop tracking the batch with given `batch_id`.
    /// Removes the number of transactions for the scheduled thread.
    /// Returns the thread id that the batch was scheduled on.
    ///
    /// # Panics
    /// Panics if the batch id does not exist in the tracker.
    pub fn complete_batch(&mut self, batch_id: TransactionBatchId) -> ThreadId {
        let Some(BatchEntry {
            thread_id,
            num_transactions,
        }) = self.batches.remove(&batch_id)
        else {
            panic!("batch id {batch_id:?} is not being tracked");
        };
        self.num_in_flight_per_thread[thread_id] -= num_transactions;

        thread_id
    }

    /// Returns true if any worker still has in-flight transactions.
    pub fn has_in_flight(&self) -> bool {
        self.num_in_flight_per_thread.iter().any(|count| *count > 0)
    }
}

#[derive(Default)]
pub struct BatchIdGenerator {
    next_id: u64,
}

impl BatchIdGenerator {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> TransactionBatchId {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_sub(1);
        TransactionBatchId::new(id)
    }
}

struct BatchEntry {
    thread_id: ThreadId,
    num_transactions: usize,
}
