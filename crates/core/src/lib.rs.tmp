pub mod bank;

mod blockhash_generator;
pub mod committer;
pub mod quic_tx_receiver;
pub mod ticker;
pub mod wal;
pub mod worker;

#[cfg(feature = "devnet")]
pub mod bench_initializer;

pub const SCHEDULER_WORKER_COUNT: usize = 12;

pub mod fork_graph;

pub mod indexer;
pub mod metrics;
pub mod pusher;

pub mod subscription;

#[cfg(feature = "track_memory")]
extern "C" {
    fn get_memory_usage() -> usize;
}
