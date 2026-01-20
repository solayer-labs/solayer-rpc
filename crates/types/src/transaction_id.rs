#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}
