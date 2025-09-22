use hashbrown::{HashMap, HashSet};
use infinisvm_logger::{debug, error, info, warn};
use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

use crate::{
    persistence::DBFile,
    versioned::{AccountVersion, MergeableDB, VersionedDB},
};
use metrics::{counter, gauge, histogram};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBMeta {
    pub job_id: u64,
    pub slot: DBFile,
}

pub const KEEP_COUNT: usize = 8;

impl DBMeta {
    pub fn from_ckpt(slot: u64) -> Self {
        Self {
            job_id: 0,
            slot: DBFile::Checkpoint(slot),
        }
    }

    pub fn from_shred(slot: u64, job_id: u64) -> Self {
        Self {
            job_id,
            slot: DBFile::Shred(slot, job_id),
        }
    }

    pub fn from_account(slot: u64) -> Self {
        Self {
            job_id: 0,
            slot: DBFile::Account(slot),
        }
    }

    pub fn from_db_file(db_file: DBFile) -> Self {
        match db_file {
            DBFile::Checkpoint(slot) => Self::from_ckpt(slot),
            DBFile::Shred(slot, job_id) => Self::from_shred(slot, job_id),
            DBFile::Account(slot) => Self::from_account(slot),
        }
    }
}

impl DBMeta {
    fn kind_rank(&self) -> u8 {
        match self.slot {
            DBFile::Checkpoint(_) => 0,
            DBFile::Account(_) => 1,
            DBFile::Shred(_, _) => 2,
        }
    }
}

impl Ord for DBMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.slot.slot(), self.kind_rank(), self.job_id).cmp(&(other.slot.slot(), other.kind_rank(), other.job_id))
    }
}

impl PartialOrd for DBMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct DBChain<T: MergeableDB> {
    dbs: Vec<(DBMeta, Arc<RwLock<T>>)>,
}

impl<T: MergeableDB> Default for DBChain<T> {
    fn default() -> Self {
        Self { dbs: Vec::new() }
    }
}

impl<T: MergeableDB> DBChain<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current number of DB segments stored in the chain.
    pub fn len(&self) -> usize {
        self.dbs.len()
    }

    /// A lightweight, human-friendly summary of the chain contents.
    /// Intended strictly for logging/diagnostics; avoid heavy per-segment introspection.
    pub fn summary(&self) -> String {
        let mut ckpt_cnt = 0usize;
        let mut last_ckpt_slot = None;
        let mut account_cnt = 0usize;
        let mut last_account_slot = None;
        let mut shred_cnt = 0usize;
        let mut shred_slots: hashbrown::HashMap<u64, usize> = hashbrown::HashMap::new();

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
            format!("{}..{}", min_slot, max_slot)
        };

        // Show top 3 shred slot counts (by slot ascending) to hint at gaps/pressure points.
        let mut shred_slots_vec: Vec<(u64, usize)> = shred_slots.into_iter().collect();
        shred_slots_vec.sort_by_key(|(s, _)| *s);
        let shred_sample: Vec<String> = shred_slots_vec
            .into_iter()
            .take(3)
            .map(|(s, c)| format!("{}:{}", s, c))
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

        // Drop Account files that are at-or-before the latest checkpoint slot to keep chain canonical
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

        let (mut meta, db) = self.dbs.remove(from_idx);

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

        meta = DBMeta::from_shred(to_slot, job_id);
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

    pub fn last_confirmed_slot(&self, slot_plan: &HashMap<u64, Vec<u64>>) -> Option<u64> {
        debug!("Computing last_confirmed_slot; chain: {}", self.summary());
        // expected jobs per slot from the remote plan
        let mut slot_plan_to_fill = slot_plan
            .iter()
            .map(|(s, p)| (*s, p.len()))
            .collect::<hashbrown::HashMap<u64, usize>>();
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
        // Treat Account(slot) segments as fully satisfying that slot (complete state for that slot),
        // and Shred(slot, job) segments as partial progress toward completion.
        let mut present_counts: hashbrown::HashMap<u64, usize> = hashbrown::HashMap::new();
        let mut slots_available: Vec<u64> = Vec::new();
        let mut seen_slot: hashbrown::HashSet<u64> = hashbrown::HashSet::new();
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
                    if *cnt != usize::MAX {
                        *cnt += 1;
                    }
                    // If an Account(s) is present (cnt==usize::MAX), treat additional shreds as benign and skip plan updates
                    if *present_counts.get(&s).unwrap_or(&0) == usize::MAX {
                        continue;
                    }
                    if let Some(v) = slot_plan_to_fill.get_mut(&s) {
                        if *v == 0 {
                            // This can occur when Account(s) satisfied the slot earlier and shreds arrive later; downgrade severity
                            warn!("Slot {} appears overfilled relative to plan; current shred count: {}, remaining plan count: {}", s, present_counts.get(&s).unwrap_or(&0), slot_plan_to_fill.get(&s).unwrap_or(&0));
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
                if remaining == usize::MAX { "unknown".to_string() } else { remaining.to_string() },
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

    pub fn merge(&mut self, slot_plan: HashMap<u64, Vec<u64>>) -> eyre::Result<Option<u64>> {
        let start_time = Instant::now();
        counter!("db_chain_merge_total", "event" => "attempt").increment(1);
        if self.dbs.is_empty() {
            counter!("db_chain_merge_total", "event" => "skipped_empty").increment(1);
            return Ok(None);
        }

        // Find the last checkpoint
        let last_checkpoint_idx = self
            .dbs
            .iter()
            .rposition(|(meta, _)| matches!(meta.slot, DBFile::Checkpoint(_)));

        let last_checkpoint_idx = match last_checkpoint_idx {
            Some(idx) => idx,
            None => {
                info!("Merge skipped: no checkpoint found; {}", self.summary());
                counter!("db_chain_merge_total", "event" => "skipped_no_checkpoint").increment(1);
                return Ok(None);
            } // No checkpoint found, nothing to merge
        };

        // Remove everything before the last checkpoint
        let drain_count = last_checkpoint_idx;
        if drain_count > 0 {
            info!(
                "Draining {} segments before last checkpoint; pre-drain summary: {}",
                drain_count,
                self.summary()
            );
            counter!("db_chain_merge_segments_drained_total").increment(drain_count as u64);
            self.dbs.drain(0..last_checkpoint_idx);
            debug!("Post-drain summary: {}", self.summary());
        }

        let last_confirmed_slot = self.last_confirmed_slot(&slot_plan);
        info!("Last confirmed slot: {:?}", last_confirmed_slot);
        if last_confirmed_slot.is_none() {
            // Log a compact snapshot of the plan around the head
            let mut keys: Vec<u64> = slot_plan.keys().copied().collect();
            keys.sort_unstable();
            let head_sample: Vec<(u64, usize)> = keys.into_iter().take(5).map(|k| (k, slot_plan[&k].len())).collect();
            info!(
                "Merge aborted: no confirmed slot; {}. slot_plan_head={:?}",
                self.summary(),
                head_sample
            );
            counter!("db_chain_merge_total", "event" => "skipped_no_confirmed_slot").increment(1);
            return Ok(None);
        }
        let find_elapsed = start_time.elapsed();
        histogram!("db_chain_merge_find_confirmed_slot_ms").record(find_elapsed.as_secs_f64() * 1000.0);

        let last_confirmed_slot = last_confirmed_slot.unwrap();
        info!(
            "Finding last confirmed slot {}, took {:?}",
            last_confirmed_slot, find_elapsed
        );

        let merge_start_time = Instant::now();
        let initial_size = self.dbs.len();

        let split_at = self.dbs.partition_point(|(m, _)| m.slot.slot() <= last_confirmed_slot);
        let mut dbs_to_merge = self.dbs[..split_at].to_vec();
        info!(
            "Merging {} segments into new checkpoint at slot {}; tail remains: {}; pre-merge summary: {}",
            dbs_to_merge.len(),
            last_confirmed_slot,
            self.dbs.len().saturating_sub(split_at),
            self.summary()
        );
        counter!("db_chain_merge_segments_merged_total").increment(dbs_to_merge.len() as u64);
        self.dbs = self.dbs.split_off(split_at);

        let mut ckpt_db_write = dbs_to_merge[0].1.write().unwrap();
        for (_, db) in dbs_to_merge.iter().skip(1) {
            ckpt_db_write.merge(&*db.read().unwrap())?;
        }
        drop(ckpt_db_write);

        let db0 = dbs_to_merge.remove(0).1;

        self.dbs.insert(
            0,
            (
                DBMeta {
                    job_id: 0,
                    slot: DBFile::Checkpoint(last_confirmed_slot),
                },
                db0,
            ),
        );

        let merge_time = merge_start_time.elapsed();
        histogram!("db_chain_merge_apply_duration_ms").record(merge_time.as_secs_f64() * 1000.0);
        counter!("db_chain_merge_total", "event" => "completed").increment(1);
        let removed = initial_size - self.dbs.len();
        if removed > 0 {
            counter!("db_chain_merge_segments_removed_total").increment(removed as u64);
        }
        info!(
            "DBChain shrink: {} -> {} ({} removed) in {:?}; post-merge summary: {}",
            initial_size,
            self.dbs.len(),
            removed,
            merge_time,
            self.summary()
        );

        Ok(Some(last_confirmed_slot))
    }
}

impl<T: MergeableDB> VersionedDB for DBChain<T> {
    fn get_account_with_version(&self, pubkey: Pubkey) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>> {
        for (_, db) in self.dbs.iter().rev() {
            if let Some((account, version)) = db.read().unwrap().get_account_with_version(pubkey)? {
                return Ok(Some((account, version)));
            }
        }
        Ok(None)
    }

    fn write_account_with_version(&mut self, pubkey: Pubkey, account: AccountSharedData, version: AccountVersion) {
        let last_db = match self.dbs.last_mut() {
            Some(db) => db,
            None => {
                error!("DBChain is empty, cannot write account");
                return;
            }
        };
        last_db
            .1
            .write()
            .unwrap()
            .write_account_with_version(pubkey, account, version);
    }

    fn tx_version(&self) -> u64 {
        panic!("DBChain does not support reading tx_version");
    }

    fn set_tx_version(&mut self, _version: u64) {
        panic!("DBChain does not support setting tx_version");
    }

    fn commit_changes_raw(&mut self, changes: Vec<(Pubkey, AccountSharedData, AccountVersion)>) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    #[derive(Debug, Default)]
    struct MockDB {
        payload: Vec<u64>,
    }

    impl MockDB {
        fn new(tag: u64) -> Self {
            Self { payload: vec![tag] }
        }
    }

    impl MergeableDB for MockDB {
        fn merge(&mut self, other: &Self) -> eyre::Result<()> {
            self.payload.extend_from_slice(&other.payload);
            Ok(())
        }
    }

    impl VersionedDB for MockDB {
        // All methods are stubs – the test never calls them.
        fn get_account_with_version(
            &self,
            _pubkey: Pubkey,
        ) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>> {
            Ok(None)
        }
        fn write_account_with_version(
            &mut self,
            _pubkey: Pubkey,
            _account: AccountSharedData,
            _version: AccountVersion,
        ) {
            unreachable!("MockDB is read‑only in this test");
        }
        fn tx_version(&self) -> u64 {
            0
        }
        fn set_tx_version(&mut self, _version: u64) {
            unreachable!("MockDB is read‑only in this test");
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // The test itself
    // ──────────────────────────────────────────────────────────────────────────
    #[test]
    fn merge_stops_at_first_gap() -> eyre::Result<()> {
        // 0. slot‑plan from the problem statement
        let slot_plan = HashMap::from([(1_u64, vec![1, 2, 3]), (2_u64, vec![4, 5, 6])]);

        // 1. build the chain: ckpt(0) (1,1)(1,2)(1,3)(2,4)(2,6)
        let mut chain: DBChain<MockDB> = DBChain::new();

        // helper to add a DB
        let mut push = |slot: u64, job: u64, tag: u64, is_ckpt: bool| {
            chain.add_db(
                Arc::new(RwLock::new(MockDB::new(tag))),
                DBMeta {
                    job_id: job,
                    slot: if is_ckpt {
                        DBFile::Checkpoint(slot)
                    } else {
                        DBFile::Shred(slot, job) // adjust to your enum
                    },
                },
            );
        };

        push(0, 0, 0, true); // initial checkpoint
        push(1, 1, 11, false);
        push(1, 2, 12, false);
        push(1, 3, 13, false);
        push(2, 4, 24, false);
        push(2, 6, 26, false); // gap: (2,5) is missing

        // 2. run merge
        chain.merge(slot_plan)?;

        // 3‑a. there should now be exactly 3 DBs in the chain
        assert_eq!(chain.dbs.len(), 3);

        // 3‑b. first one must be a *new* checkpoint for slot 1
        match chain.dbs[0].0.slot {
            DBFile::Checkpoint(s) => assert_eq!(s, 1),
            _ => panic!("first DB is not a checkpoint"),
        }

        // 3‑c. second DB is the (2,4) tail that was NOT merged
        assert_eq!(chain.dbs[1].0.slot.slot(), 2);
        assert_eq!(chain.dbs[1].0.job_id, 4);

        // 3‑c. third DB is the (2,6) tail that was NOT merged
        assert_eq!(chain.dbs[2].0.slot.slot(), 2);
        assert_eq!(chain.dbs[2].0.job_id, 6);

        // 3‑d. merged checkpoint contains tags 0,11,12,13 – but NOT 24 or 26
        let payload = &chain.dbs[0].1.read().unwrap().payload;
        assert_eq!(payload, &[0, 11, 12, 13]);

        Ok(())
    }
}
