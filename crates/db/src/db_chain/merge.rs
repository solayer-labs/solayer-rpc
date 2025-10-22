use std::time::Instant;

use hashbrown::HashMap;
use infinisvm_logger::{debug, info};
use metrics::{counter, histogram};

use super::{chain::DBChain, meta::DBMeta};
use crate::{persistence::DBFile, MergeableDB};

impl<T: MergeableDB> DBChain<T> {
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
