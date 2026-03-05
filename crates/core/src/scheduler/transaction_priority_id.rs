use crate::scheduler::transaction_id::TransactionId;

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

impl std::hash::Hash for TransactionPriorityId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
