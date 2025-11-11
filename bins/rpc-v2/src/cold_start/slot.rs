use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use dashmap::{DashMap, DashSet};
use hashbrown::HashMap;
use infinisvm_sync::types::SerializableBatch;
use metrics::{gauge, histogram};
use tokio::task::JoinHandle;

use super::utils::record_staged_batches_metrics;

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_prune_task(
    seen_shreds: Arc<DashSet<(u64, u64)>>,
    finalized_slots: Arc<DashSet<u64>>,
    finalized_timestamps: Arc<DashMap<u64, u64>>,
    staged_batches: Arc<DashMap<u64, BTreeMap<u64, SerializableBatch>>>,
    finalized_job_ids: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
    current_slot: Arc<AtomicU64>,
    pending_batches: Arc<DashMap<(u64, u64), SerializableBatch>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let prune_interval_secs = std::env::var("PRUNE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(120);
        let seen_window = std::env::var("SEEN_SHREDS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(300);
        let finalized_window = std::env::var("FINALIZED_SLOTS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1000);
        let staged_ttl = std::env::var("STAGED_BATCHES_TTL_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(50);
        let finalized_ids_window = std::env::var("FINALIZED_JOB_IDS_WINDOW_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1000);
        let pending_batches_ttl = std::env::var("PENDING_BATCHES_TTL_SLOTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100);

        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(prune_interval_secs));
        loop {
            ticker.tick().await;

            let cur = current_slot.load(Ordering::SeqCst);
            if cur == 0 {
                continue;
            }

            let seen_cutoff = cur.saturating_sub(seen_window);
            let finalized_cutoff = cur.saturating_sub(finalized_window);
            let staged_cutoff = cur.saturating_sub(staged_ttl);
            let finalized_ids_cutoff = cur.saturating_sub(finalized_ids_window);
            let pending_batches_cutoff = cur.saturating_sub(pending_batches_ttl);

            let mut removed_seen = 0usize;
            seen_shreds.retain(|entry| {
                let keep = entry.0 >= seen_cutoff;
                if !keep {
                    removed_seen += 1;
                }
                keep
            });
            seen_shreds.shrink_to_fit();
            gauge!("seen_shreds_len").set(seen_shreds.len() as f64);
            histogram!("prune_seen_shreds_removed").record(removed_seen as f64);

            let mut removed_finalized = 0usize;
            finalized_slots.retain(|slot| {
                let keep = *slot >= finalized_cutoff;
                if !keep {
                    removed_finalized += 1;
                }
                keep
            });
            finalized_slots.shrink_to_fit();
            gauge!("finalized_slots_len").set(finalized_slots.len() as f64);
            histogram!("prune_finalized_slots_removed").record(removed_finalized as f64);

            let mut removed_ts = 0usize;
            finalized_timestamps.retain(|slot, _| {
                let keep = *slot >= finalized_cutoff;
                if !keep {
                    removed_ts += 1;
                }
                keep
            });
            finalized_timestamps.shrink_to_fit();
            gauge!("finalized_timestamps_len").set(finalized_timestamps.len() as f64);
            histogram!("prune_finalized_timestamps_removed").record(removed_ts as f64);

            let mut removed_staged_slots = 0usize;
            staged_batches.retain(|slot, _| {
                let keep = *slot >= staged_cutoff;
                if !keep {
                    removed_staged_slots += 1;
                }
                keep
            });
            staged_batches.shrink_to_fit();
            record_staged_batches_metrics(staged_batches.as_ref());
            histogram!("prune_staged_batches_slots_removed").record(removed_staged_slots as f64);

            let mut removed_finalized_ids = 0usize;
            {
                let mut guard = finalized_job_ids.write().unwrap();
                guard.retain(|slot, _| {
                    let keep = *slot >= finalized_ids_cutoff;
                    if !keep {
                        removed_finalized_ids += 1;
                    }
                    keep
                });
                gauge!("finalized_job_ids_len").set(guard.len() as f64);
            }
            histogram!("prune_finalized_job_ids_removed").record(removed_finalized_ids as f64);

            let mut removed_pending_batches = 0usize;
            pending_batches.retain(|(slot, _), _| {
                let keep = *slot >= pending_batches_cutoff;
                if !keep {
                    removed_pending_batches += 1;
                }
                keep
            });
            pending_batches.shrink_to_fit();
            gauge!("pending_batches_len").set(pending_batches.len() as f64);
            histogram!("prune_pending_batches_removed").record(removed_pending_batches as f64);
        }
    })
}
