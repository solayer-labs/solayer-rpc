use std::sync::Arc;

use infinisvm_types::sync::CommitBatchNotification;

#[derive(Clone)]
pub struct PeerNotification {
    pub peer_id: [u8; 32],
    pub peer_addr: String,
    pub notification: Arc<CommitBatchNotification>,
}
