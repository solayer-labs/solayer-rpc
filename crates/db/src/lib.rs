pub type SlotHash = (u64, solana_hash::Hash);
pub type SlotHashTimestamp = (u64, solana_hash::Hash, u64);

pub mod db_chain;
pub mod encoding;
pub mod in_memory_db;
pub mod merger;
pub mod persistence;
pub mod standard;

pub use in_memory_db::MemoryDB;
pub use standard::{Database, MergeableDB};
