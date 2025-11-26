use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use eyre::Result;
use futures_util::future;
use serde::{Deserialize, Serialize};
use tokio::task;
use walkdir::{DirEntry, WalkDir};
use zstd::decode_all;

pub const MAX_SLOT_RANGE: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotArchiveMetadata {
    pub slot: u64,
    pub shard_count: usize,
}

#[derive(Default)]
struct SlotAccumulator {
    has_info: bool,
    shard_count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct SlotData {
    pub info: Vec<u8>,
    pub shards: Vec<Vec<u8>>,
}

/// Compute the canonical on-disk directory for a given slot archive.
pub fn slot_directory(root: &Path, slot: u64) -> PathBuf {
    root.join((slot % 256).to_string())
        .join((slot % 65_535).to_string())
        .join(slot.to_string())
}

fn slot_for_entry(entry: &DirEntry) -> Option<u64> {
    entry
        .path()
        .components()
        .last()
        .and_then(|component| component.as_os_str().to_str())
        .and_then(|s| s.parse::<u64>().ok())
}

fn slot_for_child(entry: &DirEntry) -> Option<u64> {
    entry
        .path()
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|name| name.to_str())
        .and_then(|s| s.parse::<u64>().ok())
}

fn in_range(slot: u64, min_slot: Option<u64>, max_slot: Option<u64>) -> bool {
    if let Some(min) = min_slot {
        if slot < min {
            return false;
        }
    }
    if let Some(max) = max_slot {
        if slot > max {
            return false;
        }
    }
    true
}

/// Enumerate all slot archives located under the provided root directory,
/// applying optional lower/upper slot bounds.
pub fn enumerate_archives(
    root: &Path,
    min_slot: Option<u64>,
    max_slot: Option<u64>,
) -> Result<Vec<SlotArchiveMetadata>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut slots: HashMap<u64, SlotAccumulator> = HashMap::new();

    for entry in WalkDir::new(root)
        .min_depth(3)
        .max_depth(4)
        .into_iter()
        .filter_map(|res| res.ok())
    {
        let depth = entry.depth();
        if depth == 3 && entry.file_type().is_dir() {
            if let Some(slot) = slot_for_entry(&entry) {
                if in_range(slot, min_slot, max_slot) {
                    slots.entry(slot).or_default();
                }
            }
            continue;
        }

        if depth == 4 && entry.file_type().is_file() {
            if let Some(slot) = slot_for_child(&entry) {
                if !in_range(slot, min_slot, max_slot) {
                    continue;
                }
                let name = entry.file_name();
                match name.to_str() {
                    Some("info") => {
                        slots.entry(slot).or_default().has_info = true;
                    }
                    Some(file_name) => {
                        if file_name.parse::<usize>().is_ok() {
                            slots.entry(slot).or_default().shard_count += 1;
                        }
                    }
                    None => continue,
                }
            }
        }
    }

    let mut archives: Vec<SlotArchiveMetadata> = slots
        .into_iter()
        .filter_map(|(slot, acc)| {
            acc.has_info.then_some(SlotArchiveMetadata {
                slot,
                shard_count: acc.shard_count,
            })
        })
        .collect();
    archives.sort_by_key(|meta| meta.slot);
    Ok(archives)
}

pub async fn read_slot_file(slots_path: &Path, slot: u64, file_name: &str) -> Result<Option<Vec<u8>>> {
    let dir = slot_directory(slots_path, slot);
    let file_path = dir.join(file_name);

    match tokio::fs::read(&file_path).await {
        Ok(compressed) => {
            let data = task::spawn_blocking(move || {
                decode_all(&compressed[..]).map_err(|e| eyre::eyre!("zstd decode failed: {}", e))
            })
            .await??;
            Ok(Some(data))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(eyre::eyre!("Failed to read slot file: {}", err)),
    }
}

/// Load a single slot's data from files.
/// Reads and decompresses the info file and all shard files for the slot.
/// Returns `Ok(None)` if the slot directory or info file doesn't exist.
/// Returns `Ok(Some(SlotData))` if the slot data is successfully loaded.
/// Returns an error only if there's an I/O error reading the files.
pub async fn load_slot(root: &Path, slot: u64) -> Result<Option<SlotData>> {
    let slot_dir = slot_directory(root, slot);

    // Check if the slot directory exists
    if !slot_dir.exists() || !slot_dir.is_dir() {
        return Ok(None);
    }

    // Read the info file - required for all slots
    // If info file doesn't exist, return None
    let info = match read_slot_file(root, slot, "info").await? {
        Some(data) => data,
        None => return Ok(None),
    };

    // Read all shard files
    let mut shards = Vec::new();
    let mut entries = tokio::fs::read_dir(&slot_dir).await?;
    let mut shard_files: Vec<(usize, String)> = Vec::new();

    // Collect all shard files with their indices
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let name_str = match file_name.to_str() {
            Some(s) => s,
            None => continue,
        };

        // Skip info file
        if name_str == "info" {
            continue;
        }

        // Parse as usize to get shard index
        if let Ok(shard_idx) = name_str.parse::<usize>() {
            shard_files.push((shard_idx, name_str.to_string()));
        }
    }

    // Sort by shard index to ensure correct order
    shard_files.sort_by_key(|(idx, _)| *idx);

    // Read and decompress each shard file
    for (_shard_idx, shard_name) in shard_files {
        if let Some(shard_data) = read_slot_file(root, slot, &shard_name).await? {
            shards.push(shard_data);
        }
    }

    Ok(Some(SlotData { info, shards }))
}

/// Load slot data from files for the given slot range.
/// Reads and decompresses the info file and all shard files for each slot.
/// Only returns slots that have a valid info file.
pub async fn load_slots(root: &Path, min_slot: u64, max_slot: u64) -> Result<HashMap<u64, SlotData>> {
    let mut slots = HashMap::new();

    // Validate input
    if min_slot > max_slot {
        return Ok(slots);
    }

    if max_slot - min_slot > MAX_SLOT_RANGE as u64 {
        return Err(eyre::eyre!("Slot range is too large: {} - {}", min_slot, max_slot));
    }

    // Create futures for loading all slots in parallel
    let load_futures: Vec<_> = (min_slot..=max_slot)
        .map(|slot| {
            let root = root.to_path_buf();
            async move {
                // load_slot returns Result<Option<SlotData>>
                // If there's an error reading files, skip the slot
                // If None, the slot doesn't exist, skip it
                match load_slot(&root, slot).await {
                    Ok(Some(slot_data)) => Some((slot, slot_data)),
                    Ok(None) | Err(_) => None,
                }
            }
        })
        .collect();

    // Execute all loads in parallel and collect results
    let results = future::join_all(load_futures).await;

    // Insert successful loads into the HashMap
    for (slot, slot_data) in results.into_iter().flatten() {
        slots.insert(slot, slot_data);
    }

    Ok(slots)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use zstd::stream::encode_all;

    use super::*;

    fn create_slot(root: &Path, slot: u64, shard_counts: &[usize]) {
        let path = slot_directory(root, slot);
        std::fs::create_dir_all(&path).unwrap();
        let info_path = path.join("info");
        let compressed = encode_all(balance_bytes(slot).as_slice(), 0).unwrap();
        std::fs::write(info_path, compressed).unwrap();
        for shard in shard_counts {
            let shard_path = path.join(shard.to_string());
            let compressed = encode_all(shard_bytes(slot, *shard).as_slice(), 0).unwrap();
            std::fs::write(shard_path, compressed).unwrap();
        }
    }

    fn balance_bytes(slot: u64) -> Vec<u8> {
        bincode::serialize(&(slot, slot * 10, [0u8; 32], [1u8; 32])).unwrap()
    }

    fn shard_bytes(slot: u64, shard: usize) -> Vec<u8> {
        bincode::serialize(&(slot, shard)).unwrap()
    }

    #[test]
    fn enumerate_archives_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let archives = enumerate_archives(tmp.path(), None, None).unwrap();
        assert!(archives.is_empty());
    }

    #[test]
    fn enumerate_archives_filters_by_range() {
        let tmp = TempDir::new().unwrap();
        create_slot(tmp.path(), 10, &[0, 1, 2]);
        create_slot(tmp.path(), 20, &[0]);
        create_slot(tmp.path(), 30, &[]);

        let archives = enumerate_archives(tmp.path(), Some(15), Some(25)).unwrap();
        assert_eq!(archives.len(), 1);
        assert_eq!(
            archives[0],
            SlotArchiveMetadata {
                slot: 20,
                shard_count: 1
            }
        );
    }

    #[test]
    fn enumerate_archives_skips_without_info() {
        let tmp = TempDir::new().unwrap();
        let slot_dir = slot_directory(tmp.path(), 42);
        std::fs::create_dir_all(&slot_dir).unwrap();
        std::fs::write(slot_dir.join("0"), b"test").unwrap();

        let archives = enumerate_archives(tmp.path(), None, None).unwrap();
        assert!(archives.is_empty());
    }

    #[test]
    fn enumerate_archives_counts_numeric_shards_only() {
        let tmp = TempDir::new().unwrap();
        create_slot(tmp.path(), 5, &[0, 1]);
        let slot_dir = slot_directory(tmp.path(), 5);
        std::fs::write(slot_dir.join("not-a-number"), b"junk").unwrap();

        let archives = enumerate_archives(tmp.path(), None, None).unwrap();
        assert_eq!(archives.len(), 1);
        assert_eq!(
            archives[0],
            SlotArchiveMetadata {
                slot: 5,
                shard_count: 2
            }
        );
    }
}
