use std::{collections::BTreeMap, sync::Arc};

use dashmap::DashMap;
use infinisvm_types::sync::{ShredIndex, SyncBatchShred};
use solana_sdk::clock::Slot;

/// Manages in-memory staging of batch shreds indexed by (slot, shred index).
///
/// This is a thin wrapper around the shared `staged_batches` DashMap used by
/// the transaction processor and prune task.
pub struct ShredManager {
    staged_batches: Arc<DashMap<Slot, BTreeMap<ShredIndex, SyncBatchShred>>>,
}

impl ShredManager {
    pub fn new(staged_batches: Arc<DashMap<Slot, BTreeMap<ShredIndex, SyncBatchShred>>>) -> Self {
        Self { staged_batches }
    }

    /// Add (or replace) a staged shred.
    ///
    /// Returns the number of shreds currently staged for this slot after the
    /// insertion, which is useful for metrics.
    pub fn add_shred(&mut self, shred: SyncBatchShred) -> usize {
        let slot = shred.shred_id.slot;
        let index = shred.shred_id.index;
        let mut entry = self.staged_batches.entry(slot).or_default();
        entry.insert(index, shred);
        entry.len()
    }

    /// Expose the underlying DashMap so that existing helpers (metrics, prune
    /// task, etc.) can continue to operate on the shared structure.
    pub fn staged_batches(&self) -> &DashMap<Slot, BTreeMap<ShredIndex, SyncBatchShred>> {
        &self.staged_batches
    }
}
