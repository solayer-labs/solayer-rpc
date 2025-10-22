use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use infinisvm_db::persistence::DB_DIRECTORY;
use infinisvm_logger::{error, info};
use infinisvm_types::serializable::TransactionExecutionDetailsSerializable;
use solana_pubkey::Pubkey;
use solana_sdk::account::AccountSharedData;

use crate::bank::Bank;

/// Directory under the DB root where WAL files are stored.
/// Layout: <DB_DIRECTORY>/wal/<slot>/<job_id>.bin
fn wal_root() -> PathBuf {
    if let Ok(root) = std::env::var("INFINISVM_DB_PATH") {
        PathBuf::from(root).join("wal")
    } else {
        PathBuf::from(DB_DIRECTORY).join("wal")
    }
}

fn wal_slot_dir(slot: u64) -> PathBuf {
    wal_root().join(format!("{slot:018}"))
}

fn wal_job_file(slot: u64, job_id: u64) -> PathBuf {
    wal_slot_dir(slot).join(format!("{job_id:018}.bin"))
}

/// Persist a completed job batch as a WAL record.
/// The file contains a bincode-serialized `SerializableBatch` (uncompressed)
/// for simplicity.
pub fn persist_batch(batch: &[infinisvm_types::jobs::ConsumedJob]) -> std::io::Result<()> {
    // Filter to only successfully processed transactions to avoid panics
    let filtered: Vec<_> = batch
        .iter()
        .filter(|j| j.processed_transaction.is_ok())
        .cloned()
        .collect();

    if filtered.is_empty() {
        return Ok(());
    }

    // All jobs in the batch must share the same job_id and slot
    let slot = filtered[0].slot;
    let job_id = filtered[0].job_id as u64;

    // Build SerializableBatch without compression
    let serializable = infinisvm_sync::types::SerializableBatch::from_consumed_jobs(&filtered);
    let bytes = bincode::serialize(&serializable).map_err(|e| std::io::Error::other(format!("serialize WAL: {e}")))?;

    // Ensure directory exists
    let slot_dir = wal_slot_dir(slot);
    fs::create_dir_all(&slot_dir)?;

    // Write to .wip then rename to .bin atomically
    let final_path = wal_job_file(slot, job_id);
    let tmp_path = final_path.with_extension("wip");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Replay WAL entries from `from_slot` (inclusive) into the Bank.
/// Returns number of WAL files applied.
pub fn replay(bank: &mut Bank, from_slot: u64) -> eyre::Result<usize> {
    let root = wal_root();
    if !root.exists() {
        info!("No WAL directory at {:?}; nothing to replay", root);
        return Ok(0);
    }

    let mut applied = 0usize;

    // Read slot directories and sort ascending
    let mut slots: Vec<u64> = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(slot) = parse_u64_name(entry.file_name()) {
                if slot >= from_slot {
                    slots.push(slot);
                }
            }
        }
    }
    slots.sort_unstable();

    for slot in slots {
        let dir = wal_slot_dir(slot);
        // Collect job files and sort by job_id
        let mut jobs: Vec<u64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let name = entry.file_name();
                if let Some(job_id) = parse_u64_stem(name) {
                    jobs.push(job_id);
                }
            }
        }
        jobs.sort_unstable();

        for job_id in jobs {
            let path = wal_job_file(slot, job_id);
            if let Err(err) = apply_wal_file(bank, &path) {
                error!("Failed to apply WAL {:?}: {:#}", path, err);
                // Do not delete; try again on next startup
            } else {
                applied += 1;
            }
        }
    }

    Ok(applied)
}

/// Delete all WAL files for a given `slot` after successful external
/// persistence. Returns the number of files deleted. Best-effort: continues on
/// individual file errors.
pub fn delete_slot(slot: u64) -> std::io::Result<usize> {
    let dir = wal_slot_dir(slot);
    if !dir.exists() {
        return Ok(0);
    }

    let mut deleted = 0usize;
    let mut maybe_empty = true;

    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let name = entry.file_name();
            // Only delete finalized .bin files; leave any .wip files alone.
            if let Some(stem) = name.to_str() {
                if stem.ends_with(".bin") {
                    if fs::remove_file(entry.path()).is_ok() {
                        deleted += 1;
                    }
                } else {
                    maybe_empty = false; // other files remain
                }
            } else {
                maybe_empty = false;
            }
        } else {
            maybe_empty = false;
        }
    }

    // Try to remove the slot directory if empty
    if maybe_empty {
        let _ = fs::remove_dir(&dir);
    }

    Ok(deleted)
}

fn apply_wal_file(bank: &mut Bank, path: &Path) -> eyre::Result<()> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let batch: infinisvm_sync::types::SerializableBatch = bincode::deserialize(&buf)?;

    // Aggregate account changes across all transactions (last writer wins)
    let mut changes: HashMap<Pubkey, AccountSharedData> = HashMap::new();

    for tx in batch.transactions.into_iter() {
        let result: TransactionExecutionDetailsSerializable = tx.get_result()?;
        let pre_accounts: Vec<(Pubkey, Option<AccountSharedData>)> = tx.get_pre_accounts()?;

        for ((pubkey, maybe_pre), diffs) in pre_accounts.into_iter().zip(result.diffs.into_iter()) {
            let mut account = maybe_pre.unwrap_or_default();
            for diff in diffs {
                diff.apply_to_account(&mut account);
            }
            changes.insert(pubkey, account);
        }
    }

    if !changes.is_empty() {
        let items: Vec<(Pubkey, AccountSharedData)> = changes.into_iter().collect();
        bank.commit_changes(items);
    }

    Ok(())
}

fn parse_u64_name(name: std::ffi::OsString) -> Option<u64> {
    name.to_str()?.parse::<u64>().ok()
}

fn parse_u64_stem(name: std::ffi::OsString) -> Option<u64> {
    let s = name.to_str()?;
    let stem = s.strip_suffix(".bin")?;
    stem.parse::<u64>().ok()
}

pub fn spawn(exit: Arc<AtomicBool>) {
    std::thread::Builder::new()
        .name("walDeleter".to_string())
        .spawn(move || {
            worker(exit);
        })
        .unwrap();
}

fn worker(exit: Arc<AtomicBool>) {
    info!("walDeleter started");

    const CHECK_INTERVAL_SECS: u64 = 60;

    while !exit.load(Ordering::Relaxed) {
        delete_old_batches();
        std::thread::sleep(Duration::from_secs(CHECK_INTERVAL_SECS));
    }

    println!("walDeleter exited");
}

fn delete_old_batches() {
    // remove all files under wal_root() older than 2 days, recursively
    let root = wal_root();
    if !root.exists() {
        return;
    }

    let now = std::time::SystemTime::now();
    let cutoff = Duration::from_secs(60 * 60 * 24 * 2);

    let mut num_deleted = 0;
    prune_dir_recursively(&root, now, cutoff, &mut num_deleted);
    info!("Deleted {} old WAL files", num_deleted);
}

fn prune_dir_recursively(path: &Path, now: std::time::SystemTime, cutoff: Duration, num_deleted: &mut usize) {
    let entries = match fs::read_dir(path) {
        Ok(e) => e,
        Err(e) => {
            error!("Failed to read WAL dir {:?}: {}", path, e);
            return;
        }
    };

    for entry_result in entries {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                error!("Failed to read entry in {:?}: {}", path, e);
                continue;
            }
        };

        let entry_path = entry.path();
        let file_type = match entry.file_type() {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to get type for {:?}: {}", entry_path, e);
                continue;
            }
        };

        if file_type.is_dir() {
            prune_dir_recursively(&entry_path, now, cutoff, num_deleted);
            continue;
        }

        if file_type.is_file() {
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to get metadata for {:?}: {}", entry_path, e);
                    continue;
                }
            };

            let modified = match metadata.modified() {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to read modified time for {:?}: {}", entry_path, e);
                    continue;
                }
            };

            match now.duration_since(modified) {
                Ok(age) if age > cutoff => {
                    if let Err(e) = fs::remove_file(&entry_path) {
                        error!("Failed to delete old WAL file {:?}: {}", entry_path, e);
                    } else {
                        *num_deleted += 1;
                    }
                }
                Ok(_) => {}
                Err(_) => {
                    // modified time is in the future; skip
                }
            }
        }
    }
}
