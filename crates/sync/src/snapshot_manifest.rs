use std::{
    collections::HashSet,
    fs,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use blake3::Hasher;
use eyre::{Context, Result};
use infinisvm_db::persistence::DBFile;
use infinisvm_logger::{info, warn};
use infinisvm_types::sync::{SignedSnapshotManifest, SnapshotManifest, SnapshotManifestFile};
use solana_sdk::signature::read_keypair_file;
use tokio::{fs as tokio_fs, sync::RwLock};

#[derive(Clone, Default)]
pub struct SnapshotManifestStore {
    inner: Arc<RwLock<SnapshotManifestState>>,
}

#[derive(Clone, Default)]
struct SnapshotManifestState {
    manifest: Option<SignedSnapshotManifest>,
    ready: bool,
    fresh: bool,
}

// A healthy manifest usually includes account files that bring its effective
// state head close to the latest signed finalization. Allow a small buffer for
// the dropped latest account flush plus poll / propagation jitter.
pub const SNAPSHOT_HEAD_MAX_LAG_SLOTS: u64 = 128;

impl SnapshotManifestStore {
    pub async fn get(&self) -> Option<SignedSnapshotManifest> {
        self.inner.read().await.manifest.clone()
    }

    pub async fn get_if_ready(&self) -> Option<SignedSnapshotManifest> {
        let state = self.inner.read().await;
        state.ready.then(|| state.manifest.clone()).flatten()
    }

    pub async fn get_if_serving(&self) -> Option<SignedSnapshotManifest> {
        let state = self.inner.read().await;
        (state.ready && state.fresh).then(|| state.manifest.clone()).flatten()
    }

    pub async fn is_ready(&self) -> bool {
        self.inner.read().await.ready
    }

    pub async fn is_fresh(&self) -> bool {
        self.inner.read().await.fresh
    }

    pub async fn is_serving_ready(&self) -> bool {
        let state = self.inner.read().await;
        state.ready && state.fresh
    }

    pub async fn set_bootstrap_manifest(&self, manifest: Option<SignedSnapshotManifest>) {
        let mut state = self.inner.write().await;
        state.manifest = manifest;
        state.ready = false;
        state.fresh = false;
    }

    pub async fn set_ready_manifest(&self, manifest: Option<SignedSnapshotManifest>) {
        let mut state = self.inner.write().await;
        state.ready = manifest.is_some();
        state.fresh = manifest.is_some();
        state.manifest = manifest;
    }

    pub async fn set_freshness(&self, fresh: bool) {
        let mut state = self.inner.write().await;
        state.fresh = fresh && state.ready && state.manifest.is_some();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRestoreSet {
    pub checkpoint_slot: u64,
    pub files: Vec<DBFile>,
}

pub fn spawn_snapshot_manifest_refresher(
    store: SnapshotManifestStore,
    db_path: String,
    keypair_path: PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let keypair = match read_keypair_file(&keypair_path) {
            Ok(keypair) => keypair,
            Err(err) => {
                warn!(
                    "Failed to read snapshot manifest signing key {}: {}",
                    keypair_path.display(),
                    err
                );
                return;
            }
        };

        let poll_secs = std::env::var("SNAPSHOT_MANIFEST_POLL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1);
        let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
        let db_path = PathBuf::from(db_path);
        let mut last_layout: Option<Vec<DBFile>> = None;

        loop {
            interval.tick().await;

            let layout = match tokio::task::spawn_blocking({
                let db_path = db_path.clone();
                move || list_snapshot_db_files(&db_path)
            })
            .await
            {
                Ok(Ok(layout)) => layout,
                Ok(Err(err)) => {
                    warn!("Failed to scan snapshot layout for manifest refresh: {err}");
                    continue;
                }
                Err(err) => {
                    warn!("Snapshot manifest refresh task failed to join: {err}");
                    continue;
                }
            };

            if last_layout.as_ref() == Some(&layout) {
                continue;
            }

            let manifest = match tokio::task::spawn_blocking({
                let db_path = db_path.clone();
                let layout = layout.clone();
                move || build_snapshot_manifest_from_layout(&db_path, &layout)
            })
            .await
            {
                Ok(Ok(manifest)) => manifest,
                Ok(Err(err)) => {
                    warn!("Failed to build snapshot manifest: {err}");
                    continue;
                }
                Err(err) => {
                    warn!("Snapshot manifest build task failed to join: {err}");
                    continue;
                }
            };

            let Some(manifest) = manifest else {
                last_layout = Some(layout);
                store.set_bootstrap_manifest(None).await;
                continue;
            };

            let signed = SignedSnapshotManifest::sign(manifest, &keypair);
            let checkpoint_slot = signed.manifest.checkpoint_slot;
            let file_count = signed.manifest.files.len();
            store.set_ready_manifest(Some(signed)).await;
            last_layout = Some(layout);
            info!(checkpoint_slot, file_count, "Refreshed signed snapshot manifest");
        }
    })
}

pub fn list_snapshot_db_files(db_path: &Path) -> Result<Vec<DBFile>> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(db_path).with_context(|| format!("read snapshot dir {}", db_path.display()))? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(db_file) = DBFile::from_path(&path) else {
            continue;
        };
        files.push(db_file);
    }
    files.sort_by_key(file_sort_key);
    Ok(files)
}

pub fn select_snapshot_restore_set(files: &[DBFile]) -> Option<SnapshotRestoreSet> {
    let mut largest_checkpoint = None;
    let mut second_largest_checkpoint = None;
    let mut largest_account = None;

    for file in files {
        match file {
            DBFile::Checkpoint(slot) => match largest_checkpoint {
                None => largest_checkpoint = Some(*slot),
                Some(max) if *slot > max => {
                    second_largest_checkpoint = largest_checkpoint;
                    largest_checkpoint = Some(*slot);
                }
                Some(max) if *slot < max => {
                    if second_largest_checkpoint.is_none_or(|second| *slot > second) {
                        second_largest_checkpoint = Some(*slot);
                    }
                }
                _ => {}
            },
            DBFile::Account(slot) => {
                if largest_account.is_none_or(|max| *slot > max) {
                    largest_account = Some(*slot);
                }
            }
            DBFile::Shred(_, _) => {}
        }
    }

    let checkpoint_slot = second_largest_checkpoint?;
    let mut selected = vec![DBFile::Checkpoint(checkpoint_slot)];
    selected.extend(files.iter().filter_map(|file| match file {
        DBFile::Account(slot) if *slot > checkpoint_slot && largest_account.is_some_and(|largest| *slot < largest) => {
            Some(file.clone())
        }
        _ => None,
    }));
    selected.sort_by_key(file_sort_key);

    Some(SnapshotRestoreSet {
        checkpoint_slot,
        files: selected,
    })
}

pub fn build_snapshot_manifest(db_path: &Path) -> Result<Option<SnapshotManifest>> {
    let files = list_snapshot_db_files(db_path)?;
    build_snapshot_manifest_from_layout(db_path, &files)
}

pub fn build_snapshot_manifest_from_layout(db_path: &Path, files: &[DBFile]) -> Result<Option<SnapshotManifest>> {
    let Some(restore_set) = select_snapshot_restore_set(files) else {
        return Ok(None);
    };

    let mut manifest_files = Vec::with_capacity(restore_set.files.len());
    for file in restore_set.files {
        let filename = file.to_string();
        let path = db_path.join(&filename);
        let (blake3_hash, size_bytes) =
            hash_file(&path).with_context(|| format!("hash snapshot file {} for manifest", path.display()))?;
        manifest_files.push(SnapshotManifestFile {
            filename,
            blake3_hash,
            size_bytes,
        });
    }

    Ok(Some(SnapshotManifest {
        version: SnapshotManifest::VERSION,
        checkpoint_slot: restore_set.checkpoint_slot,
        files: manifest_files,
    }))
}

pub fn manifest_files(manifest: &SnapshotManifest) -> Result<Vec<DBFile>> {
    let mut files = Vec::with_capacity(manifest.files.len());
    for entry in &manifest.files {
        let path = PathBuf::from(&entry.filename);
        let db_file = DBFile::from_path(&path)
            .ok_or_else(|| eyre::eyre!("invalid snapshot manifest filename '{}'", entry.filename))?;
        files.push(db_file);
    }
    Ok(files)
}

pub fn manifest_effective_head_slot(manifest: &SnapshotManifest) -> Result<u64> {
    let mut head = manifest.checkpoint_slot;
    for file in manifest_files(manifest)? {
        head = head.max(file.slot());
    }
    Ok(head)
}

pub fn snapshot_head_lag_slots(snapshot_head_slot: u64, signed_head_slot: u64) -> u64 {
    signed_head_slot.saturating_sub(snapshot_head_slot)
}

pub fn snapshot_head_is_fresh(snapshot_head_slot: u64, signed_head_slot: u64) -> bool {
    snapshot_head_lag_slots(snapshot_head_slot, signed_head_slot) <= SNAPSHOT_HEAD_MAX_LAG_SLOTS
}

pub async fn prune_mirror_dir_to_manifests(mirror_dir: &Path, manifests: &[&SnapshotManifest]) -> Result<()> {
    if !mirror_dir.exists() {
        return Ok(());
    }

    let mut keep = HashSet::new();
    for manifest in manifests {
        keep.extend(manifest_files(manifest)?);
    }

    let mut entries = tokio_fs::read_dir(mirror_dir)
        .await
        .with_context(|| format!("read mirrored snapshot dir {}", mirror_dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !entry.file_type().await?.is_file() {
            continue;
        }

        let Some(db_file) = DBFile::from_path(&path) else {
            continue;
        };
        if keep.contains(&db_file) {
            continue;
        }

        tokio_fs::remove_file(&path)
            .await
            .with_context(|| format!("remove stale mirrored snapshot file {}", path.display()))?;
    }

    Ok(())
}

fn hash_file(path: &Path) -> Result<([u8; 32], u64)> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 1024 * 1024];
    let mut size_bytes = 0u64;

    loop {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        size_bytes += read as u64;
    }

    Ok((*hasher.finalize().as_bytes(), size_bytes))
}

fn file_sort_key(file: &DBFile) -> (u64, u8, usize) {
    match file {
        DBFile::Checkpoint(slot) => (*slot, 0, 0),
        DBFile::Account(slot) => (*slot, 1, 0),
        DBFile::Shred(slot, index) => (*slot, 2, *index),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use infinisvm_types::sync::{SignedSnapshotManifest, SnapshotManifestFile};
    use solana_sdk::signature::Keypair;
    use tempfile::TempDir;

    use super::{
        build_snapshot_manifest, manifest_effective_head_slot, prune_mirror_dir_to_manifests,
        select_snapshot_restore_set, snapshot_head_is_fresh, snapshot_head_lag_slots, SnapshotManifest,
        SnapshotManifestStore,
    };

    #[test]
    fn selects_second_largest_checkpoint_restore_set() {
        let files = vec![
            infinisvm_db::persistence::DBFile::Checkpoint(10),
            infinisvm_db::persistence::DBFile::Checkpoint(20),
            infinisvm_db::persistence::DBFile::Checkpoint(30),
            infinisvm_db::persistence::DBFile::Account(21),
            infinisvm_db::persistence::DBFile::Account(22),
            infinisvm_db::persistence::DBFile::Account(29),
            infinisvm_db::persistence::DBFile::Account(31),
        ];

        let restore_set = select_snapshot_restore_set(&files).expect("restore set");
        assert_eq!(restore_set.checkpoint_slot, 20);
        assert_eq!(
            restore_set.files,
            vec![
                infinisvm_db::persistence::DBFile::Checkpoint(20),
                infinisvm_db::persistence::DBFile::Account(21),
                infinisvm_db::persistence::DBFile::Account(22),
                infinisvm_db::persistence::DBFile::Account(29),
            ]
        );
    }

    #[test]
    fn builds_manifest_for_restore_set() {
        let dir = TempDir::new().expect("temp dir");
        fs::write(dir.path().join("ckpt_000000000000000010.bin"), b"10").expect("write ckpt10");
        fs::write(dir.path().join("ckpt_000000000000000020.bin"), b"20").expect("write ckpt20");
        fs::write(dir.path().join("ckpt_000000000000000030.bin"), b"30").expect("write ckpt30");
        fs::write(dir.path().join("accounts_000000000000000021.bin"), b"21").expect("write acc21");
        fs::write(dir.path().join("accounts_000000000000000029.bin"), b"29").expect("write acc29");
        fs::write(dir.path().join("accounts_000000000000000031.bin"), b"31").expect("write acc31");

        let manifest = build_snapshot_manifest(dir.path())
            .expect("build manifest")
            .expect("manifest");
        assert_eq!(manifest.checkpoint_slot, 20);
        let filenames = manifest
            .files
            .iter()
            .map(|file| file.filename.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            filenames,
            vec![
                "ckpt_000000000000000020.bin",
                "accounts_000000000000000021.bin",
                "accounts_000000000000000029.bin",
            ]
        );
    }

    #[tokio::test]
    async fn prunes_mirror_dir_to_current_manifest() {
        let dir = TempDir::new().expect("temp dir");
        fs::write(dir.path().join("ckpt_000000000000000020.bin"), b"20").expect("write ckpt20");
        fs::write(dir.path().join("accounts_000000000000000021.bin"), b"21").expect("write acc21");
        fs::write(dir.path().join("accounts_000000000000000029.bin"), b"29").expect("write acc29");

        let manifest = SnapshotManifest {
            version: SnapshotManifest::VERSION,
            checkpoint_slot: 20,
            files: vec![
                SnapshotManifestFile {
                    filename: "ckpt_000000000000000020.bin".to_string(),
                    blake3_hash: [1; 32],
                    size_bytes: 2,
                },
                SnapshotManifestFile {
                    filename: "accounts_000000000000000021.bin".to_string(),
                    blake3_hash: [2; 32],
                    size_bytes: 2,
                },
            ],
        };

        prune_mirror_dir_to_manifests(dir.path(), &[&manifest])
            .await
            .expect("prune mirror dir");

        assert!(dir.path().join("ckpt_000000000000000020.bin").is_file());
        assert!(dir.path().join("accounts_000000000000000021.bin").is_file());
        assert!(!dir.path().join("accounts_000000000000000029.bin").exists());
    }

    #[tokio::test]
    async fn bootstrap_manifest_is_hidden_until_ready_refresh() {
        let store = SnapshotManifestStore::default();
        let keypair = Keypair::new();
        let manifest = SnapshotManifest {
            version: SnapshotManifest::VERSION,
            checkpoint_slot: 20,
            files: vec![SnapshotManifestFile {
                filename: "ckpt_000000000000000020.bin".to_string(),
                blake3_hash: [1; 32],
                size_bytes: 2,
            }],
        };
        let signed = SignedSnapshotManifest::sign(manifest, &keypair);

        store.set_bootstrap_manifest(Some(signed.clone())).await;
        assert!(!store.is_ready().await);
        assert!(!store.is_fresh().await);
        assert!(!store.is_serving_ready().await);
        assert!(store.get_if_ready().await.is_none());
        assert!(store.get_if_serving().await.is_none());
        assert!(store.get().await.is_some());

        store.set_ready_manifest(Some(signed)).await;
        assert!(store.is_ready().await);
        assert!(store.is_fresh().await);
        assert!(store.is_serving_ready().await);
        assert!(store.get_if_ready().await.is_some());
        assert!(store.get_if_serving().await.is_some());

        store.set_freshness(false).await;
        assert!(store.is_ready().await);
        assert!(!store.is_fresh().await);
        assert!(!store.is_serving_ready().await);
        assert!(store.get_if_ready().await.is_some());
        assert!(store.get_if_serving().await.is_none());

        store.set_bootstrap_manifest(None).await;
        assert!(!store.is_ready().await);
        assert!(!store.is_fresh().await);
        assert!(store.get().await.is_none());
    }

    #[test]
    fn manifest_effective_head_uses_latest_file_slot() {
        let manifest = SnapshotManifest {
            version: SnapshotManifest::VERSION,
            checkpoint_slot: 100,
            files: vec![
                SnapshotManifestFile {
                    filename: "ckpt_000000000000000100.bin".to_string(),
                    blake3_hash: [1; 32],
                    size_bytes: 10,
                },
                SnapshotManifestFile {
                    filename: "accounts_000000000000000164.bin".to_string(),
                    blake3_hash: [2; 32],
                    size_bytes: 10,
                },
                SnapshotManifestFile {
                    filename: "accounts_000000000000000132.bin".to_string(),
                    blake3_hash: [3; 32],
                    size_bytes: 10,
                },
            ],
        };

        assert_eq!(manifest_effective_head_slot(&manifest).expect("head slot"), 164);
    }

    #[test]
    fn snapshot_head_freshness_uses_lag_window() {
        assert_eq!(snapshot_head_lag_slots(200, 220), 20);
        assert!(snapshot_head_is_fresh(200, 328));
        assert!(!snapshot_head_is_fresh(200, 329));
    }
}
