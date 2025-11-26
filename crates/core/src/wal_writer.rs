use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
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
