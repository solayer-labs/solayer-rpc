use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use eyre::Result;
use infinisvm_logger::{error, info};
use libmdbx::{Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode};

const LATEST_SLOT_KEY: &[u8] = b"latest_slot";
const BACKFILL_START_KEY: &[u8] = b"backfill_start";
const TABLE_NAME: &str = "slots_progress_recorder";

/// Slots sync progress recorder using libmdbx
pub struct SlotsSyncProgress {
    db: Arc<Database<NoWriteMap>>,
}

impl SlotsSyncProgress {
    /// Open or create the progress database at the given path
    pub fn open(path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path)?;
        let db_path = path.join("slots_progress.mdbx");

        let options = DatabaseOptions {
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                ..Default::default()
            }),
            max_tables: Some(1),
            ..Default::default()
        };

        let db = Arc::new(Database::open_with_options(&db_path, options)?);

        // Ensure the table exists by creating it in a write transaction
        let txn = db.begin_rw_txn()?;
        txn.create_table(Some(TABLE_NAME), Default::default())?;
        txn.commit()?;

        info!("Opened slots sync progress database at {:?}", db_path);

        Ok(Self { db })
    }

    /// Get the latest processed slot
    pub fn get_latest_slot(&self) -> Result<Option<u64>> {
        let txn = self.db.begin_ro_txn()?;
        let table = txn.open_table(Some(TABLE_NAME))?;

        match txn.get::<Vec<u8>>(&table, LATEST_SLOT_KEY) {
            Ok(Some(bytes)) => {
                let slot = bincode::deserialize::<u64>(&bytes)?;
                Ok(Some(slot))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(eyre::eyre!("Failed to read latest slot: {}", e)),
        }
    }

    pub fn get_backfill_start(&self) -> Result<Option<u64>> {
        let txn = self.db.begin_ro_txn()?;
        let table = txn.open_table(Some(TABLE_NAME))?;

        match txn.get::<Vec<u8>>(&table, BACKFILL_START_KEY) {
            Ok(Some(bytes)) => {
                let slot = bincode::deserialize::<u64>(&bytes)?;
                Ok(Some(slot))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(eyre::eyre!("Failed to read backfill start: {}", e)),
        }
    }

    /// Record the latest processed slot
    /// Ensures that slot updates are monotonic and warns on non-sequential
    /// progress (each new slot ideally should be exactly one more than the
    /// previous).
    pub fn record_latest_slot(&self, slot: u64) -> Result<()> {
        let txn = self.db.begin_rw_txn()?;
        let table = txn.open_table(Some(TABLE_NAME))?;

        // Check current latest slot to ensure sequential updates
        let current_latest = match txn.get::<Vec<u8>>(&table, LATEST_SLOT_KEY) {
            Ok(Some(bytes)) => {
                let current = bincode::deserialize::<u64>(&bytes)?;
                Some(current)
            }
            Ok(None) => None,
            Err(e) => return Err(eyre::eyre!("Failed to read current latest slot: {}", e)),
        };

        // Validate sequential update
        match current_latest {
            Some(current) => {
                if slot <= current {
                    error!(
                        "Ignoring non-monotonic latest_slot update: current={}, attempted={}",
                        current, slot
                    );
                    return Ok(());
                }
                if slot != current + 1 {
                    error!(
                        "Non-sequential slot update: expected slot {}, got slot {}",
                        current + 1,
                        slot
                    );
                }
            }
            None => {
                // First slot recorded, allow any value
            }
        }

        let bytes = bincode::serialize(&slot)?;
        txn.put(&table, LATEST_SLOT_KEY, &bytes, libmdbx::WriteFlags::empty())?;
        txn.commit()?;

        info!("Slots progress recorder: recorded latest slot {}", slot);
        Ok(())
    }

    pub fn record_backfill_start(&self, slot: u64) -> Result<()> {
        let txn = self.db.begin_rw_txn()?;
        let table = txn.open_table(Some(TABLE_NAME))?;

        let bytes = bincode::serialize(&slot)?;
        txn.put(&table, BACKFILL_START_KEY, &bytes, libmdbx::WriteFlags::empty())?;
        txn.commit()?;

        info!("Slots progress recorder: recorded backfill start {}", slot);
        Ok(())
    }

    pub fn clear_backfill_start(&self) -> Result<()> {
        let txn = self.db.begin_rw_txn()?;
        let table = txn.open_table(Some(TABLE_NAME))?;
        let _ = txn.del(&table, BACKFILL_START_KEY, None)?;
        txn.commit()?;
        info!("Slots progress recorder: cleared backfill start");
        Ok(())
    }
}

/// Thread-safe wrapper for SlotsSyncProgress
#[derive(Clone)]
pub struct SlotsSyncProgressRecorder {
    progress: Arc<RwLock<SlotsSyncProgress>>,
}

impl SlotsSyncProgressRecorder {
    pub fn new(progress: SlotsSyncProgress) -> Self {
        Self {
            progress: Arc::new(RwLock::new(progress)),
        }
    }

    pub fn get_latest_slot(&self) -> Result<Option<u64>> {
        let progress = self.progress.read().unwrap();
        progress.get_latest_slot()
    }

    pub fn get_backfill_start(&self) -> Result<Option<u64>> {
        let progress = self.progress.read().unwrap();
        progress.get_backfill_start()
    }

    pub fn record_latest_slot(&self, slot: u64) -> Result<()> {
        let progress = self.progress.write().unwrap();
        match progress.record_latest_slot(slot) {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Failed to record latest slot {}: {}", slot, e);
                Err(e)
            }
        }
    }

    pub fn record_backfill_start(&self, slot: u64) -> Result<()> {
        let progress = self.progress.write().unwrap();
        match progress.record_backfill_start(slot) {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Failed to record backfill start {}: {}", slot, e);
                Err(e)
            }
        }
    }

    pub fn clear_backfill_start(&self) -> Result<()> {
        let progress = self.progress.write().unwrap();
        match progress.clear_backfill_start() {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Failed to clear backfill start: {}", e);
                Err(e)
            }
        }
    }
}
