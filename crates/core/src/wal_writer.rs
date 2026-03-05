use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

use eyre::Result as EyreResult;
use infinisvm_logger::{error, info};
use infinisvm_sync::slots::{enumerate_archives_in_order, slot_directory};
use infinisvm_types::{
    convert::materialize_job_effect_account_updates,
    sync::{JobEffects, ShredId, ShredIndex, SyncFinalization},
};
use solana_pubkey::Pubkey;
use solana_sdk::account::AccountSharedData;

use crate::bank::Bank;

const DEFAULT_WAL_PATH: &str = "/mnt/data/slots";

pub struct WalWriter {
    slot_transactions: HashMap<u64, SlotTransactions>,
    slots_path: PathBuf,
}

impl Default for WalWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl WalWriter {
    pub fn new() -> Self {
        Self {
            slot_transactions: HashMap::new(),
            slots_path: resolve_slots_path(),
        }
    }

    pub fn cache_slot_transactions(&mut self, shred_id: ShredId, effects: Vec<JobEffects>) {
        if effects.is_empty() {
            return;
        }

        let buffer = self.slot_transactions.entry(shred_id.slot).or_default();
        buffer.append(shred_id.index, effects);
    }

    pub fn slots_path(&self) -> PathBuf {
        self.slots_path.clone()
    }

    pub fn take_slot_transactions(&mut self, slot: u64) -> Vec<(ShredIndex, Vec<JobEffects>)> {
        self.slot_transactions
            .remove(&slot)
            .map(SlotTransactions::into_ordered)
            .unwrap_or_default()
    }

    pub fn persist_slot(
        slots_path: PathBuf,
        slot_metadata: SyncFinalization,
        shreds: Vec<(ShredIndex, Vec<JobEffects>)>,
    ) -> EyreResult<()> {
        let slot = slot_metadata.slot;
        let slot_dir = slot_directory(&slots_path, slot);

        info!("Persisting slot {} to {}", slot, slot_dir.display());

        if slot_dir.exists() {
            if let Err(err) = fs::remove_dir_all(&slot_dir) {
                error!("Failed to clear existing slot directory {:?}: {}", slot_dir, err);
            }
        }
        fs::create_dir_all(&slot_dir)?;

        if !shreds.is_empty() {
            let mut shreds = shreds;
            shreds.sort_by_key(|(index, _)| *index);
            write_shred_files(&slot_dir, &shreds)?;
        }

        write_info_file(&slot_dir, &slot_metadata)?;
        info!("Finished persisting slot {} to {}", slot, slot_dir.display());
        Ok(())
    }
}

#[derive(Default)]
struct SlotTransactions {
    shreds: BTreeMap<ShredIndex, Vec<JobEffects>>,
}

impl SlotTransactions {
    fn append(&mut self, shred_index: ShredIndex, effects: Vec<JobEffects>) {
        if effects.is_empty() {
            return;
        }

        if self.shreds.contains_key(&shred_index) {
            panic!("duplicate shred index {shred_index} detected while caching WAL");
        }

        self.shreds.insert(shred_index, effects);
    }

    fn into_ordered(self) -> Vec<(ShredIndex, Vec<JobEffects>)> {
        self.shreds.into_iter().collect()
    }
}

fn write_info_file(slot_dir: &Path, slot_metadata: &SyncFinalization) -> EyreResult<()> {
    let serialized = bincode::serialize(&slot_metadata)?;
    write_bytes_atomic(&slot_dir.join("info"), &serialized)?;
    Ok(())
}

fn write_shred_files(slot_dir: &Path, shreds: &[(ShredIndex, Vec<JobEffects>)]) -> EyreResult<()> {
    for (shred_index, effects) in shreds {
        if effects.is_empty() {
            continue;
        }

        let serialized = bincode::serialize(effects)?;
        write_bytes_atomic(&slot_dir.join(shred_index.to_string()), &serialized)?;
    }
    Ok(())
}

fn write_bytes_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("wip");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
    }

    fs::rename(&tmp_path, path)?;
    Ok(())
}

pub fn replay(bank: &mut Bank, from_slot: u64) -> EyreResult<usize> {
    let slots_path = resolve_slots_path();
    if !slots_path.exists() {
        info!("No WAL directory at {:?}; nothing to replay", slots_path);
        return Ok(0);
    }

    info!("Replaying WAL from slot {}", from_slot);

    let archives = enumerate_archives_in_order(&slots_path, Some(from_slot), None)?;
    if archives.is_empty() {
        return Ok(0);
    }

    info!(
        "Replay till slot {} ({} archives)",
        archives.last().unwrap().slot,
        archives.len()
    );

    let mut applied = 0usize;
    for archive in archives {
        let slot = archive.slot;
        if slot < from_slot {
            continue;
        }

        match load_slot_archive(&slots_path, slot)? {
            Some((slot_metadata, job_effects)) => {
                apply_job_effects(bank, job_effects)?;
                bank.set_slot_blockhash(slot_metadata.slot, slot_metadata.hash);
                applied += 1;
            }
            None => continue,
        }
    }

    info!(
        "Finished replaying WAL from slot {} with {} applied",
        from_slot, applied
    );

    Ok(applied)
}

fn resolve_slots_path() -> PathBuf {
    PathBuf::from(std::env::var("INFINISVM_WAL_PATH").unwrap_or_else(|_| DEFAULT_WAL_PATH.to_string()))
}

fn load_slot_archive(slots_path: &Path, slot: u64) -> EyreResult<Option<(SyncFinalization, Vec<JobEffects>)>> {
    let slot_dir = slot_directory(slots_path, slot);
    if !slot_dir.exists() {
        return Ok(None);
    }

    let info_path = slot_dir.join("info");
    if !info_path.is_file() {
        info!(
            "Skipping WAL replay for slot {} because info file is missing at {:?}",
            slot, info_path
        );
        return Ok(None);
    }

    let info_bytes = fs::read(&info_path)?;
    let slot_metadata = deserialize_slot_metadata(slot, &info_bytes)?;

    let mut shard_files: Vec<(usize, PathBuf)> = Vec::new();
    for entry in fs::read_dir(&slot_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else { continue };
        if name == "info" {
            continue;
        }
        if let Ok(idx) = name.parse::<ShredIndex>() {
            shard_files.push((idx, entry.path()));
        }
    }
    shard_files.sort_by_key(|(idx, _)| *idx);

    let mut effects = Vec::new();
    for (_, shard_path) in shard_files {
        let shard_bytes = fs::read(&shard_path)?;
        let mut shard_effects: Vec<JobEffects> = bincode::deserialize(&shard_bytes)?;
        effects.append(&mut shard_effects);
    }

    Ok(Some((slot_metadata, effects)))
}

fn deserialize_slot_metadata(expected_slot: u64, info_bytes: &[u8]) -> EyreResult<SyncFinalization> {
    let slot_metadata = bincode::deserialize::<SyncFinalization>(info_bytes)?;
    if slot_metadata.slot != expected_slot {
        panic!(
            "slot mismatch while replaying WAL: expected {}, found {}",
            expected_slot, slot_metadata.slot
        );
    }

    Ok(slot_metadata)
}

fn apply_job_effects(bank: &mut Bank, effects: Vec<JobEffects>) -> EyreResult<()> {
    if effects.is_empty() {
        return Ok(());
    }

    let mut changes: HashMap<Pubkey, AccountSharedData> = HashMap::new();
    for effect in effects {
        for (pubkey, account) in materialize_job_effect_account_updates(&effect) {
            changes.insert(pubkey, account);
        }
    }

    if !changes.is_empty() {
        bank.commit_changes(changes.into_iter().collect());
    }

    Ok(())
}
