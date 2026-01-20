use std::collections::BTreeMap;

use dashmap::{DashMap, DashSet};
use infinisvm_types::sync::SyncBatchShred;
use metrics::gauge;

pub(super) fn record_dashmap_len<K, V>(map: &DashMap<K, V>, metric: &'static str)
where
    K: Eq + std::hash::Hash,
{
    gauge!(metric).set(map.len() as f64);
}

pub(super) fn record_dashset_len<T>(set: &DashSet<T>, metric: &'static str)
where
    T: Eq + std::hash::Hash,
{
    gauge!(metric).set(set.len() as f64);
}

pub(super) fn record_staged_batches_metrics(staged_batches: &DashMap<u64, BTreeMap<usize, SyncBatchShred>>) {
    record_dashmap_len(staged_batches, "staged_batches_slots");
}
