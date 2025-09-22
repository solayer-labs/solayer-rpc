pub type SlotHash = (u64, solana_hash::Hash);
pub type SlotHashTimestamp = (u64, solana_hash::Hash, u64);

pub mod db_chain;
pub mod encoding;
pub mod in_memory_db;
pub mod local_rocks_db;
pub mod merger;
pub mod mock_db;
pub mod persistence;
pub mod standard;
pub mod versioned;

pub use in_memory_db::MemoryDB;
pub use local_rocks_db::LocalRocksDB;
pub use mock_db::MockDB;
pub use standard::Database;
pub use versioned::VersionedDB;

#[cfg(test)]
pub(crate) mod tests;
