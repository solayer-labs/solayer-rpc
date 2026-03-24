pub mod bank;

mod blockhash_generator;
pub mod committer;
pub mod s3;


pub mod fork_graph;

pub mod indexer;
pub mod metrics;

pub mod subscription;

pub const BLOCK_TIME_MS: u64 = 50;
pub const LEGACY_BLOCK_TIME_MS: u64 = 400;
pub const DEFAULT_SCHEDULER_BATCH_SIZE: usize = 1024;

pub const SCHEDULER_RX_IDLE_BUDGET_MS: u64 = 3;
pub const SCHEDULER_RX_BUSY_BUDGET_MS: u64 = 2;
pub const SCHEDULER_COMPLETE_BUDGET_MS: u64 = 5;
pub const SCHEDULER_SCHEDULE_BUDGET_MS: u64 = 8;
pub const SCHEDULER_MAX_TRANSACTION_TRIES: u64 = 24;
pub const SCHEDULER_MAX_QUEUE_AGE_MS: u64 = 61_000;
pub const SCHEDULER_LOG_INTERVAL_MS: u64 = 1_000;

pub const BLOCKHASH_TTL_MS: u64 = 60_000;
pub const LEGACY_BLOCKHASH_SIGNATURE_SET_CAPACITY: u64 = 200_000;
pub const DEFAULT_SLOT_BUFFER_WALL_MS: u64 = 400_000;
pub const DEFAULT_DB_MERGE_INTERVAL_MS: u64 = 1_600;
pub const DEFAULT_FINALIZED_WINDOW_MS: u64 = 400_000;
pub const DEFAULT_SEEN_SHREDS_WINDOW_MS: u64 = 120_000;
pub const DEFAULT_STAGED_BATCHES_TTL_MS: u64 = 20_000;
pub const DEFAULT_PENDING_BATCHES_TTL_MS: u64 = 40_000;
pub const DEFAULT_STREAM_MAX_LAG_MS: u64 = 3_200;
pub const DEFAULT_RPC_REGISTRY_MAX_OFFSET_MS: u64 = 2_000;

pub const fn slots_for_millis(duration_ms: u64) -> u64 {
    let mut slots = duration_ms / BLOCK_TIME_MS;
    if !duration_ms.is_multiple_of(BLOCK_TIME_MS) {
        slots += 1;
    }
    slots
}

pub const BLOCKHASH_HISTORY_SLOTS: usize = slots_for_millis(BLOCKHASH_TTL_MS) as usize;
pub const BLOCKHASH_SIGNATURE_SET_CAPACITY: usize =
    (LEGACY_BLOCKHASH_SIGNATURE_SET_CAPACITY * BLOCK_TIME_MS).div_ceil(LEGACY_BLOCK_TIME_MS) as usize;
pub const DEFAULT_SLOT_BUFFER_SLOTS: u64 = slots_for_millis(DEFAULT_SLOT_BUFFER_WALL_MS);
pub const DEFAULT_DB_MERGE_SLOT_INTERVAL: u64 = slots_for_millis(DEFAULT_DB_MERGE_INTERVAL_MS);
pub const DEFAULT_FINALIZED_SLOTS_WINDOW_SLOTS: u64 = slots_for_millis(DEFAULT_FINALIZED_WINDOW_MS);
pub const DEFAULT_SEEN_SHREDS_WINDOW_SLOTS: u64 = slots_for_millis(DEFAULT_SEEN_SHREDS_WINDOW_MS);
pub const DEFAULT_STAGED_BATCHES_TTL_SLOTS: u64 = slots_for_millis(DEFAULT_STAGED_BATCHES_TTL_MS);
pub const DEFAULT_PENDING_BATCHES_TTL_SLOTS: u64 = slots_for_millis(DEFAULT_PENDING_BATCHES_TTL_MS);
pub const DEFAULT_STREAM_MAX_LAG_SLOTS: u64 = slots_for_millis(DEFAULT_STREAM_MAX_LAG_MS);
pub const DEFAULT_RPC_REGISTRY_MAX_OFFSET_SLOTS: u64 = slots_for_millis(DEFAULT_RPC_REGISTRY_MAX_OFFSET_MS);
