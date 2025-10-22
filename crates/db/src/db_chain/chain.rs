use std::sync::{Arc, RwLock};

use hashbrown::{HashMap, HashSet};
use infinisvm_logger::{debug, info, warn};
use metrics::gauge;

use super::meta::DBMeta;
use crate::{persistence::DBFile, MergeableDB};

pub struct DBChain<T: MergeableDB> {
    pub(super) dbs: Vec<(DBMeta, Arc<RwLock<T>>)>,
}

impl<T: MergeableDB> Default for DBChain<T> {
    fn default() -> Self {
        Self { dbs: Vec::new() }
    }
}

impl<T: MergeableDB> DBChain<T> {
    pub fn len(&self) -> usize {
        self.dbs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dbs.is_empty()
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// A lightweight, human-friendly summary of the chain contents.
    /// Intended strictly for logging/diagnostics; avoid heavy per-segment
    /// introspection.
    pub fn summary(&self) -> String {
        let mut ckpt_cnt = 0usize;
        let mut last_ckpt_slot = None;
        let mut account_cnt = 0usize;
        let mut last_account_slot = None;
        let mut shred_cnt = 0usize;
        let mut shred_slots: HashMap<u64, usize> = HashMap::new();

        let mut min_slot = u64::MAX;
        let mut max_slot = 0u64;

        for (meta, _) in &self.dbs {
            let slot = meta.slot.slot();
            min_slot = min_slot.min(slot);
            max_slot = max_slot.max(slot);
            match meta.slot {
                DBFile::Checkpoint(s) => {
                    ckpt_cnt += 1;
                    last_ckpt_slot = Some(s);
                }
                DBFile::Account(s) => {
                    account_cnt += 1;
                    last_account_slot = Some(s);
                }
                DBFile::Shred(s, _job) => {
                    shred_cnt += 1;
                    *shred_slots.entry(s).or_insert(0) += 1;
                }
            }
        }

        let window = if self.dbs.is_empty() {
            "empty".to_string()
        } else {
            format!("{min_slot}..{max_slot}")
        };

        // Show top 3 shred slot counts (by slot ascending) to hint at gaps/pressure
        // points.
        let mut shred_slots_vec: Vec<(u64, usize)> = shred_slots.into_iter().collect();
        shred_slots_vec.sort_by_key(|(s, _)| *s);
        let shred_sample: Vec<String> = shred_slots_vec
            .into_iter()
            .take(3)
            .map(|(s, c)| format!("{s}:{c}"))
            .collect();

        // Publish gauges for quick observability
        gauge!("db_chain_segments").set(self.dbs.len() as f64);
        gauge!("db_chain_checkpoints").set(ckpt_cnt as f64);
        gauge!("db_chain_accounts").set(account_cnt as f64);
        gauge!("db_chain_shreds").set(shred_cnt as f64);
        if self.dbs.is_empty() {
            gauge!("db_chain_window_min_slot").set(0.0);
            gauge!("db_chain_window_max_slot").set(0.0);
        } else {
            gauge!("db_chain_window_min_slot").set(min_slot as f64);
            gauge!("db_chain_window_max_slot").set(max_slot as f64);
        }
        if let Some(s) = last_ckpt_slot {
            gauge!("db_chain_last_checkpoint_slot").set(s as f64);
        }
        if let Some(s) = last_account_slot {
            gauge!("db_chain_last_account_slot").set(s as f64);
        }

        format!(
            "size={}, window={}, ckpts={} (last={:?}), accounts={} (last={:?}), shreds={} (slots_sample=[{}])",
            self.dbs.len(),
            window,
            ckpt_cnt,
            last_ckpt_slot,
            account_cnt,
            last_account_slot,
            shred_cnt,
            shred_sample.join(", ")
        )
    }

    pub fn add_db(&mut self, db: Arc<RwLock<T>>, meta: DBMeta) {
        let before = self.dbs.len();
        info!("Adding DB {:?}; size: {} -> {}? (pre-insert)", meta, before, before + 1);
        if self.dbs.iter().any(|(m, _)| m == &meta) {
            warn!("Duplicate DB ignored: {:?}; size remains: {}", meta, self.dbs.len());
            return;
        }

        // Drop Account files that are at-or-before the latest checkpoint slot to keep
        // chain canonical
        if let DBFile::Account(account_slot) = meta.slot {
            if let Some((last_ckpt_slot, _)) = self.dbs.iter().rev().find_map(|(m, db)| match m.slot {
                DBFile::Checkpoint(s) => Some((s, db)),
                _ => None,
            }) {
                if account_slot <= last_ckpt_slot {
                    warn!(
                        "Ignoring Account({}) at-or-before last checkpoint {} to avoid redundant segments",
                        account_slot, last_ckpt_slot
                    );
                    return;
                }
            }
        }

        let insert_pos = self.dbs.partition_point(|(m, _)| m < &meta);
        self.dbs.insert(insert_pos, (meta, db));
        info!(
            "DB added at position {}; size is now {}; summary: {}",
            insert_pos,
            self.dbs.len(),
            self.summary()
        );
    }

    pub fn contains_shred(&self, slot: u64, job_id: u64) -> bool {
        self.dbs
            .iter()
            .any(|(meta, _)| matches!(meta.slot, DBFile::Shred(s, j) if s == slot && j == job_id))
    }

    pub fn find_shred_slot(&self, job_id: u64) -> Option<u64> {
        self.dbs.iter().find_map(|(meta, _)| match meta.slot {
            DBFile::Shred(slot, j) if j == job_id => Some(slot),
            _ => None,
        })
    }

    pub fn relocate_shred(&mut self, from_slot: u64, job_id: u64, to_slot: u64) -> bool {
        if from_slot == to_slot {
            return self.contains_shred(to_slot, job_id);
        }

        let from_idx = self
            .dbs
            .iter()
            .position(|(meta, _)| matches!(meta.slot, DBFile::Shred(slot, j) if slot == from_slot && j == job_id));

        let Some(from_idx) = from_idx else {
            warn!(
                "relocate_shred: missing source shard slot={} job_id={}",
                from_slot, job_id
            );
            return false;
        };

        let (_, db) = self.dbs.remove(from_idx);

        if let Some(existing_idx) = self
            .dbs
            .iter()
            .position(|(meta, _)| matches!(meta.slot, DBFile::Shred(slot, j) if slot == to_slot && j == job_id))
        {
            info!(
                "relocate_shred: removing existing destination shard slot={} job_id={} before relocation",
                to_slot, job_id
            );
            self.dbs.remove(existing_idx);
        }

        let meta = DBMeta::from_shred(to_slot, job_id);
        let insert_pos = self.dbs.partition_point(|(m, _)| m < &meta);
        self.dbs.insert(insert_pos, (meta, db));
        info!(
            "relocate_shred: moved job_id={} from slot {} to slot {}; summary: {}",
            job_id,
            from_slot,
            to_slot,
            self.summary()
        );
        true
    }

    pub(crate) fn last_confirmed_slot(&self, slot_plan: &HashMap<u64, Vec<u64>>) -> Option<u64> {
        debug!("Computing last_confirmed_slot; chain: {}", self.summary());
        // expected jobs per slot from the remote plan
        let mut slot_plan_to_fill = slot_plan
            .iter()
            .map(|(s, p)| (*s, p.len()))
            .collect::<HashMap<u64, usize>>();
        let mut last_confirmed_slot = None;

        // Find the last checkpoint; do not treat Account files as baselines
        let last_checkpoint_idx = self
            .dbs
            .iter()
            .rposition(|(meta, _)| matches!(meta.slot, DBFile::Checkpoint(_)));

        let checkpoint_offset = match last_checkpoint_idx {
            Some(idx) => {
                if let DBFile::Checkpoint(s) = self.dbs[idx].0.slot {
                    last_confirmed_slot = Some(s);
                }
                idx
            }
            None => {
                debug!("No checkpoint present; cannot compute confirmed slot");
                return None;
            }
        };

        info!("Last confirmed slot via Account files: {:?}", last_confirmed_slot);

        if checkpoint_offset == self.dbs.len() - 1 {
            info!(
                "Only checkpoint(s) present; last_confirmed at {:?}",
                last_confirmed_slot
            );
            return last_confirmed_slot;
        }

        info!("Slot plans to fill: {:?}", slot_plan_to_fill);

        // Count present jobs per slot in the chain tail (after checkpoint)
        // Treat Account(slot) segments as fully satisfying that slot (complete state
        // for that slot), and Shred(slot, job) segments as partial progress
        // toward completion.
        let mut present_counts: HashMap<u64, usize> = HashMap::new();
        let mut slots_available: Vec<u64> = Vec::new();
        let mut seen_slot: HashSet<u64> = HashSet::new();
        for (meta, _) in self.dbs.iter().skip(checkpoint_offset + 1) {
            match meta.slot {
                DBFile::Account(s) => {
                    if seen_slot.insert(s) {
                        slots_available.push(s);
                    }
                    // Mark this slot as fully satisfied regardless of remote plan entry
                    slot_plan_to_fill.insert(s, 0);
                    // Track that we observed an account segment for this slot
                    present_counts.insert(s, usize::MAX);
                }
                DBFile::Shred(s, _) => {
                    if seen_slot.insert(s) {
                        slots_available.push(s);
                    }
                    let cnt = present_counts.entry(s).or_insert(0);
                    *cnt = cnt.saturating_add(1);
                    // If an Account(s) is present (cnt==usize::MAX), treat additional shreds as
                    // benign and skip plan updates
                    if *present_counts.get(&s).unwrap_or(&0) == usize::MAX {
                        continue;
                    }
                    if let Some(v) = slot_plan_to_fill.get_mut(&s) {
                        if *v == 0 {
                            // This can occur when Account(s) satisfied the slot earlier and shreds arrive
                            // later; downgrade severity
                            warn!(
                                "Slot {} appears overfilled relative to plan; current shred count: {}, remaining plan count: {}",
                                s,
                                present_counts.get(&s).unwrap_or(&0),
                                slot_plan_to_fill.get(&s).unwrap_or(&0)
                            );
                        } else {
                            *v -= 1;
                        }
                    }
                }
                DBFile::Checkpoint(_) => unreachable!("Checkpoint should not be present in the chain"),
            }
        }

        // Walk slots in chain order; stop at first incomplete
        let mut blocking_slot: Option<(u64, usize, usize, usize, bool)> = None;
        for slot in slots_available {
            match slot_plan_to_fill.get(&slot) {
                Some(remaining) if *remaining == 0 => {
                    last_confirmed_slot = Some(slot);
                }
                Some(remaining) => {
                    let expected = slot_plan.get(&slot).map(|v| v.len()).unwrap_or(0);
                    let present = present_counts.get(&slot).copied().unwrap_or(0);
                    blocking_slot = Some((slot, expected, present, *remaining, false));
                    break;
                }
                None => {
                    let expected = slot_plan.get(&slot).map(|v| v.len()).unwrap_or(0);
                    let present = present_counts.get(&slot).copied().unwrap_or(0);
                    blocking_slot = Some((slot, expected, present, usize::MAX, true));
                    break;
                }
            }
        }

        if let Some((slot, expected, present, remaining, missing_plan)) = blocking_slot {
            let mut local_job_ids: Vec<u64> = self
                .dbs
                .iter()
                .filter_map(|(meta, _)| {
                    (meta.slot.slot() == slot && matches!(meta.slot, DBFile::Shred(_, _))).then_some(meta.job_id)
                })
                .collect();
            local_job_ids.sort_unstable();
            let present_display = if present == usize::MAX {
                "account_segment".to_string()
            } else {
                present.to_string()
            };
            info!(
                "Merge checkpoint stall at slot {}: expected_jobs={} present_jobs={} remaining_jobs={} missing_in_plan={} local_job_ids={:?}",
                slot,
                expected,
                present_display,
                if remaining == usize::MAX {
                    "unknown".to_string()
                } else {
                    remaining.to_string()
                },
                missing_plan,
                local_job_ids
            );

            // Provide additional, focused diagnostics on what is actually missing
            // (cap samples to keep logs bounded)
            let expected_ids_sample: Vec<u64> = slot_plan
                .get(&slot)
                .map(|v| v.iter().copied().take(10).collect())
                .unwrap_or_default();
            let present_set: HashSet<u64> = local_job_ids.iter().copied().collect();
            let missing_ids_sample: Vec<u64> = slot_plan
                .get(&slot)
                .map(|v| {
                    v.iter()
                        .copied()
                        .filter(|id| !present_set.contains(id))
                        .take(10)
                        .collect()
                })
                .unwrap_or_default();
            info!(
                "Blocking slot {} details: expected_ids_sample={:?} missing_ids_sample={:?}",
                slot, expected_ids_sample, missing_ids_sample
            );
        }

        if last_confirmed_slot.is_none() {
            // Identify the earliest incomplete slot and log expected vs present
            let mut keys: Vec<u64> = slot_plan_to_fill.keys().copied().collect();
            keys.sort_unstable();
            for k in keys {
                let rem = *slot_plan_to_fill.get(&k).unwrap_or(&usize::MAX);
                if rem != 0 {
                    let expected = slot_plan.get(&k).map(|v| v.len()).unwrap_or(0);
                    let present = present_counts.get(&k).copied().unwrap_or(0);
                    info!(
                        "No confirmed slot yet; earliest incomplete slot {} expected_jobs={} present_jobs={} remaining_jobs={}",
                        k, expected, present, rem
                    );
                    // Emit gauges for the earliest incomplete slot to aid alerting
                    gauge!("db_chain_incomplete_slot_expected").set(expected as f64);
                    gauge!("db_chain_incomplete_slot_present").set(present as f64);
                    gauge!("db_chain_incomplete_slot_remaining").set(rem as f64);
                    break;
                }
            }
        }

        last_confirmed_slot
    }
}
