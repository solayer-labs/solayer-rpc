use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use infinisvm_types::sync::{CommitBatchNotification, SignedFinalization, SyncBatchShred, SyncFinalization};
use metrics::{counter, gauge};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum NotificationKey {
    Batch {
        slot: u64,
        index: usize,
        shred_hash: [u8; 32],
    },
    SignedFinalization {
        slot: u64,
        signature: [u8; 64],
    },
    Finalization {
        slot: u64,
        hash: [u8; 32],
        num_shreds: u64,
    },
}

impl NotificationKey {
    fn from_notification(notification: &CommitBatchNotification) -> Self {
        match notification {
            CommitBatchNotification::Batch(batch) => Self::from_batch(batch),
            CommitBatchNotification::SignedFinalization(signed) => Self::from_signed_finalization(signed),
            CommitBatchNotification::Finalization(finalization) => Self::from_finalization(finalization),
        }
    }

    fn from_batch(batch: &SyncBatchShred) -> Self {
        Self::Batch {
            slot: batch.shred_id.slot,
            index: batch.shred_id.index,
            shred_hash: batch.shred_hash,
        }
    }

    fn from_signed_finalization(signed: &SignedFinalization) -> Self {
        Self::SignedFinalization {
            slot: signed.finalization.slot,
            signature: signed.signature,
        }
    }

    fn from_finalization(finalization: &SyncFinalization) -> Self {
        Self::Finalization {
            slot: finalization.slot,
            hash: finalization.hash.to_bytes(),
            num_shreds: finalization.num_shreds,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Batch { .. } => "batch",
            Self::SignedFinalization { .. } => "signed_finalization",
            Self::Finalization { .. } => "finalization",
        }
    }
}

#[derive(Clone)]
pub struct NotificationDeduper {
    entries: Arc<DashMap<NotificationKey, Instant>>,
    ttl: Duration,
    max_entries: usize,
    prune_every: usize,
    observe_count: Arc<AtomicUsize>,
}

impl Default for NotificationDeduper {
    fn default() -> Self {
        let ttl_secs = std::env::var("RPC_NOTIFICATION_DEDUPE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(300)
            .max(1);
        let max_entries = std::env::var("RPC_NOTIFICATION_DEDUPE_MAX_ENTRIES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(200_000)
            .max(1);
        let prune_every = std::env::var("RPC_NOTIFICATION_DEDUPE_PRUNE_EVERY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1024)
            .max(1);
        Self::new(Duration::from_secs(ttl_secs), max_entries, prune_every)
    }
}

impl NotificationDeduper {
    pub fn new(ttl: Duration, max_entries: usize, prune_every: usize) -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            ttl,
            max_entries: max_entries.max(1),
            prune_every: prune_every.max(1),
            observe_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn observe(&self, notification: &CommitBatchNotification) -> bool {
        let key = NotificationKey::from_notification(notification);
        let kind = key.kind();
        let now = Instant::now();

        if let Some(mut existing) = self.entries.get_mut(&key) {
            if now.duration_since(*existing.value()) <= self.ttl {
                counter!("rpc_notification_dropped_duplicate_total", "kind" => kind).increment(1);
                return false;
            }
            *existing = now;
        } else {
            self.entries.insert(key, now);
        }

        gauge!("rpc_notification_seen_cache_size").set(self.entries.len() as f64);
        let observed = self.observe_count.fetch_add(1, Ordering::Relaxed) + 1;
        if observed.is_multiple_of(self.prune_every) || self.entries.len() > self.max_entries {
            self.prune(now);
        }
        true
    }

    fn prune(&self, now: Instant) {
        let stale_keys: Vec<_> = self
            .entries
            .iter()
            .filter_map(|entry| (now.duration_since(*entry.value()) > self.ttl).then(|| entry.key().clone()))
            .collect();
        for key in stale_keys {
            self.entries.remove(&key);
        }

        let len = self.entries.len();
        if len > self.max_entries {
            let mut entries: Vec<_> = self
                .entries
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect();
            entries.sort_by_key(|(_, seen_at)| *seen_at);
            let overflow = len - self.max_entries;
            for (key, _) in entries.into_iter().take(overflow) {
                self.entries.remove(&key);
            }
        }

        gauge!("rpc_notification_seen_cache_size").set(self.entries.len() as f64);
    }
}

#[cfg(test)]
mod tests {
    use infinisvm_types::sync::ShredId;
    use solana_sdk::{
        hash::Hash,
        signature::{Keypair, Signer},
    };

    use super::*;

    fn sample_batch(slot: u64, index: usize) -> CommitBatchNotification {
        let mut batch = SyncBatchShred {
            shred_id: ShredId::new(slot, index),
            worker_id: 0,
            effects: Vec::new(),
            shred_hash: [0u8; 32],
        };
        batch.shred_hash = batch.compute_shred_hash();
        CommitBatchNotification::Batch(batch)
    }

    fn sample_signed_finalization(slot: u64) -> CommitBatchNotification {
        let keypair = Keypair::new();
        let finalization = SyncFinalization {
            slot,
            num_shreds: 1,
            hash: Hash::new_unique(),
            parent_hash: Hash::new_unique(),
            block_unix_timestamp: 0,
            shred_hashes: vec![[7u8; 32]],
        };
        let msg = bincode::serialize(&finalization).expect("serialize finalization");
        let signature: [u8; 64] = keypair.sign_message(&msg).into();
        CommitBatchNotification::SignedFinalization(SignedFinalization {
            finalization,
            sequencer_pubkey: keypair.pubkey().to_bytes(),
            signature,
        })
    }

    #[test]
    fn drops_duplicate_batch_notifications() {
        let deduper = NotificationDeduper::new(Duration::from_secs(60), 128, 1);
        let batch = sample_batch(42, 0);

        assert!(deduper.observe(&batch));
        assert!(!deduper.observe(&batch));
    }

    #[test]
    fn tracks_signed_finalizations_independently() {
        let deduper = NotificationDeduper::new(Duration::from_secs(60), 128, 1);
        let batch = sample_batch(42, 0);
        let finalization = sample_signed_finalization(42);

        assert!(deduper.observe(&batch));
        assert!(deduper.observe(&finalization));
        assert!(!deduper.observe(&finalization));
    }

    #[test]
    fn prunes_oldest_entries_when_capacity_is_exceeded() {
        let deduper = NotificationDeduper::new(Duration::from_secs(60), 1, 1);
        let first = sample_batch(1, 0);
        let second = sample_batch(2, 0);

        assert!(deduper.observe(&first));
        assert!(deduper.observe(&second));
        assert!(deduper.observe(&first));
    }
}
