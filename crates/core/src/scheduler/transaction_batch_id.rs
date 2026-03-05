/// A unique identifier for a transaction batch.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct TransactionBatchId(u64);

impl TransactionBatchId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}
