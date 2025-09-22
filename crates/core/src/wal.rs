use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use infinisvm_db::persistence::DB_DIRECTORY;
use infinisvm_logger::{error, info};
use solana_pubkey::Pubkey;
use solana_sdk::account::AccountSharedData;

use crate::bank::Bank;
use infinisvm_types::serializable::{SerializableTxRow, TransactionExecutionDetailsSerializable};

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
    wal_root().join(format!("{:018}", slot))
}

fn wal_job_file(slot: u64, job_id: u64) -> PathBuf {
    wal_slot_dir(slot).join(format!("{:018}.bin", job_id))
}

/// Persist a completed job batch as a WAL record.
/// The file contains a bincode-serialized `SerializableBatch` (uncompressed) for simplicity.
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
    let bytes = bincode::serialize(&serializable)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("serialize WAL: {e}")))?;

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

/// Delete all WAL files for a given `slot` after successful external persistence.
/// Returns the number of files deleted. Best-effort: continues on individual file errors.
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
        let items: Vec<(Pubkey, AccountSharedData, u64)> = changes.into_iter().map(|(k, v)| (k, v, 0u64)).collect();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    use infinisvm_logger::console;
    use solana_sdk::account::{AccountSharedData, ReadableAccount, WritableAccount};

    fn setup_logger() {
        let _ = console();
    }

    fn mk_account_with_lamports(lamports: u64) -> AccountSharedData {
        let mut a = AccountSharedData::default();
        a.set_lamports(lamports);
        a
    }

    #[test]
    fn wal_replay_applies_account_diffs() {
        setup_logger();

        // 1) Prepare temp wal root
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("INFINISVM_DB_PATH", tmp.path().display().to_string());

        // 2) Create a small SerializableBatch with one tx that sets lamports
        let slot = 1u64;
        let job_id = 42u64;
        let timestamp = 123_456u64;
        let target = solana_pubkey::Pubkey::new_unique();

        // Pre-state: account has 1 lamport
        let pre = mk_account_with_lamports(1);
        // Post-state: lamports become 777
        let result = TransactionExecutionDetailsSerializable {
            status: Ok(()),
            log_messages: None,
            inner_instructions: None,
            return_data: None,
            executed_units: 0,
            accounts_data_len_delta: 0,
            fee: 0,
            diffs: vec![vec![infinisvm_types::serializable::AccountDataDiff::Lamports(777)]],
            pre_balances: vec![1],
        };

        let s_tx = SerializableTxRow {
            signature: vec![0; 64],
            transaction: vec![], // not used by replay
            result: bincode::serialize(&result).unwrap(),
            slot,
            pre_accounts: bincode::serialize(&vec![(target, Some(pre))]).unwrap(),
            block_unix_timestamp: timestamp,
            seq_number: job_id,
        };

        let ser_batch = infinisvm_sync::types::SerializableBatch {
            slot,
            timestamp,
            job_id: job_id as usize,
            transactions: vec![s_tx],
        };

        // 3) Write the WAL file directly
        let dir = wal_slot_dir(slot);
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = wal_job_file(slot, job_id);
        let tmp_path = wal_path.with_extension("wip");
        let mut f = File::create(&tmp_path).unwrap();
        let bytes = bincode::serialize(&ser_batch).unwrap();
        f.write_all(&bytes).unwrap();
        f.sync_all().unwrap();
        std::fs::rename(&tmp_path, &wal_path).unwrap();

        // 4) Build a Bank (slave mode uses in-memory DB)
        let exit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut bank = crate::bank::Bank::new_slave(exit);

        // 5) Ensure account is not present before replay
        let before = bank.get_account_shared_data_public(&target);
        assert!(before.is_none());

        // 6) Replay
        let applied = replay(&mut bank, 0).unwrap();
        assert_eq!(applied, 1);

        // 7) Verify account now has 777 lamports
        let after = bank.get_account_shared_data_public(&target).unwrap();
        assert_eq!(after.lamports(), 777);
    }
}
