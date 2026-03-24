use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use eyre::{eyre, Result};
use hashbrown::HashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use infinisvm_db::persistence::DBFile;
use infinisvm_logger::{error, info, warn};
use infinisvm_registry::{RpcPeerInfo, RpcRegisterRequest, RpcSetResponse};
use infinisvm_types::sync::{SignedSnapshotManifest, SnapshotManifest};
use reqwest::Client;
use serde::Deserialize;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};
use tempfile::tempdir;
use tokio::{
    fs,
    process::Command,
    sync::{mpsc, watch},
    time::sleep,
};

use crate::{
    http::BatchSlotsResponse,
    slots::SlotData,
    snapshot_manifest::{
        manifest_effective_head_slot, manifest_files, prune_mirror_dir_to_manifests, snapshot_head_is_fresh,
        snapshot_head_lag_slots, SnapshotManifestStore,
    },
};

const MIN_ARIA2C_VERSION: (u32, u32, u32) = (1, 35, 0);

#[derive(Debug, Deserialize)]
pub struct SnapshotsResponse {
    pub files: Vec<String>,
}

pub struct Snapshots {
    pub files: Vec<DBFile>,
}

pub struct ManifestPollContext<T> {
    pub http_client: Arc<HttpClient>,
    pub sequencer_pubkey: Pubkey,
    pub manifest_store: SnapshotManifestStore,
    pub signed_finalization_slot: watch::Receiver<u64>,
    pub mirror_dir: PathBuf,
    pub sender: mpsc::Sender<(DBFile, T)>,
    pub parser: fn(Vec<u8>) -> Result<T>,
}

impl From<SnapshotsResponse> for Snapshots {
    fn from(response: SnapshotsResponse) -> Self {
        Self {
            files: response
                .files
                .into_iter()
                .filter_map(|f| {
                    let path = PathBuf::from(&f);
                    match DBFile::from_path(&path) {
                        Some(db_file) => Some(db_file),
                        None => {
                            infinisvm_logger::error!("Failed to parse file path: {}", f);
                            None
                        }
                    }
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargestCheckpoint {
    pub ckpt: DBFile,
    pub accounts: Vec<DBFile>,
}

impl Snapshots {
    /// Returns the second largest checkpoint and its associated account files
    /// that should be downloaded.
    ///
    /// This function finds the second largest checkpoint and all account files
    /// with slots between that checkpoint and the largest account file.
    /// This ensures we get a consistent snapshot while avoiding the most
    /// recent (potentially incomplete) checkpoint.
    ///
    /// Returns `None` if there are no checkpoints available.
    pub fn get_ckpts_to_download(&self) -> Option<LargestCheckpoint> {
        let mut largest_ckpt = None;
        let mut second_largest_ckpt = None;
        let mut largest_account = None;

        // Find largest and second largest checkpoints and largest account
        for file in &self.files {
            match file {
                DBFile::Checkpoint(slot) => match largest_ckpt {
                    None => largest_ckpt = Some(*slot),
                    Some(max) if *slot > max => {
                        second_largest_ckpt = largest_ckpt;
                        largest_ckpt = Some(*slot);
                    }
                    Some(max) if *slot < max => {
                        if second_largest_ckpt.is_none_or(|second| *slot > second) {
                            second_largest_ckpt = Some(*slot);
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

        // Get second largest checkpoint and associated accounts
        second_largest_ckpt.map(|ckpt_slot| {
            let accounts = self
                .files
                .iter()
                .filter_map(|file| match file {
                    DBFile::Account(slot)
                        if *slot > ckpt_slot && largest_account.is_some_and(|largest| *slot < largest) =>
                    {
                        Some(file.clone())
                    }
                    _ => None,
                })
                .collect();

            LargestCheckpoint {
                ckpt: DBFile::Checkpoint(ckpt_slot),
                accounts,
            }
        })
    }

    pub fn get_second_largest_slot(&self) -> Option<u64> {
        let mut largest = None;
        let mut second = None;

        for slot in self.files.iter().map(|file| file.slot()) {
            match largest {
                None => largest = Some(slot),
                Some(max) if slot > max => {
                    second = largest;
                    largest = Some(slot);
                }
                Some(max) if slot < max => {
                    if second.is_none_or(|s| slot > s) {
                        second = Some(slot);
                    }
                }
                _ => {}
            }
        }
        second
    }

    pub fn filter_files_by_slot(&self, min_slot: u64, max_slot: u64) -> Vec<DBFile> {
        self.files
            .iter()
            .filter(|file| file.slot() >= min_slot && file.slot() <= max_slot)
            .cloned()
            .collect()
    }

    pub fn since_slot(&self, slot: u64) -> Vec<DBFile> {
        let second_largest_slot = self.get_second_largest_slot().unwrap_or(u64::MAX);
        self.files
            .iter()
            .filter(|file| file.slot() > slot && file.slot() <= second_largest_slot)
            .cloned()
            .collect()
    }
}

pub struct HttpClient {
    client: Client,
    base_url: String,
}

impl HttpClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    pub async fn get_snapshots_once(&self) -> Result<SnapshotsResponse> {
        let url = format!("{}/solayer/snapshots", self.base_url);
        info!("Getting snapshots from {}", url);
        let response = self.client.get(&url).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(eyre!("get snapshots failed with status {}", status));
        }
        Ok(response.json::<SnapshotsResponse>().await?)
    }

    pub async fn ensure_manifest_files_available(&self, manifest: &SnapshotManifest) -> Result<()> {
        let snapshots = self.get_snapshots_once().await?;
        let available = snapshots.files.into_iter().collect::<std::collections::HashSet<_>>();
        let missing = manifest
            .files
            .iter()
            .filter(|entry| !available.contains(&entry.filename))
            .map(|entry| entry.filename.clone())
            .collect::<Vec<_>>();

        if missing.is_empty() {
            return Ok(());
        }

        Err(eyre!("snapshot listing missing manifest files: {}", missing.join(", ")))
    }

    pub async fn get_snapshots(&self) -> Result<Snapshots> {
        // Infinite retry with exponential backoff (capped at 1 minute)
        const INITIAL_BACKOFF_MS: u64 = 100;
        const MAX_BACKOFF_MS: u64 = 60_000; // 1 minute
        const BACKOFF_MULTIPLIER: f64 = 2.0;

        let mut attempt = 1u32;
        loop {
            match self.get_snapshots_once().await {
                Ok(snapshots) => return Ok(Snapshots::from(snapshots)),
                Err(e) => {
                    warn!("Failed to get snapshots (attempt {}): {}, retrying...", attempt, e);
                }
            }

            // Calculate exponential backoff with cap
            let exponential_backoff = (INITIAL_BACKOFF_MS as f64 * BACKOFF_MULTIPLIER.powi(attempt as i32 - 1)) as u64;
            let backoff_ms = exponential_backoff.min(MAX_BACKOFF_MS);

            warn!("Retrying get_snapshots in {}ms (attempt {})", backoff_ms, attempt);
            sleep(Duration::from_millis(backoff_ms)).await;
            attempt += 1;
        }
    }

    pub async fn get_snapshot_manifest(&self) -> Result<SignedSnapshotManifest> {
        let url = format!("{}/solayer/snapshot-manifest", self.base_url);
        info!("Getting snapshot manifest from {}", url);
        let response = self.client.get(&url).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(eyre!("get snapshot manifest failed with status {}", status));
        }
        Ok(response.json::<SignedSnapshotManifest>().await?)
    }

    pub async fn get_file(&self, filename: &DBFile) -> Result<Vec<u8>> {
        let url = format!("{}/solayer/files/{}", self.base_url, filename.to_string());
        info!("Getting file {} from {}", filename.to_string(), url);
        let response = self.client.get(&url).send().await?;
        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }

    /// Get a single slot's data
    pub async fn get_slot(&self, slot: u64) -> Result<Option<SlotData>> {
        let url = format!("{}/solayer/slots/{}", self.base_url, slot);
        info!("Getting slot {} from {}", slot, url);
        let response = self.client.get(&url).send().await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !status.is_success() {
            return Err(eyre::eyre!("Failed to fetch slot {} (status {})", slot, status));
        }

        let slot_data = response.json::<SlotData>().await?;
        Ok(Some(slot_data))
    }

    /// Get multiple slots' data in a range
    pub async fn get_slots(&self, min_slot: u64, max_slot: u64) -> Result<HashMap<u64, SlotData>> {
        let url = format!(
            "{}/solayer/slots?min_slot={}&max_slot={}",
            self.base_url, min_slot, max_slot
        );
        info!("Getting slots {}-{} from {}", min_slot, max_slot, url);
        let response = self.client.get(&url).send().await?;

        let status = response.status();
        if !status.is_success() {
            return Err(eyre::eyre!(
                "Failed to fetch slots {}-{} (status {})",
                min_slot,
                max_slot,
                status
            ));
        }

        let batch_response = response.json::<BatchSlotsResponse>().await?;
        Ok(batch_response.slots.into_iter().collect())
    }

    /// Get a slot's info file as binary data
    pub async fn get_slot_info(&self, slot: u64) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/solayer/slots/{}/info", self.base_url, slot);
        info!("Getting slot {} info from {}", slot, url);
        let response = self.client.get(&url).send().await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !status.is_success() {
            return Err(eyre::eyre!("Failed to fetch slot info {} (status {})", slot, status));
        }

        let bytes = response.bytes().await?;
        Ok(Some(bytes.to_vec()))
    }

    /// Get a slot's shard file as binary data
    pub async fn get_slot_shard(&self, slot: u64, shard: u64) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/solayer/slots/{}/shards/{}", self.base_url, slot, shard);
        info!("Getting slot {} shard {} from {}", slot, shard, url);
        let response = self.client.get(&url).send().await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !status.is_success() {
            return Err(eyre::eyre!(
                "Failed to fetch slot shard {}:{} (status {})",
                slot,
                shard,
                status
            ));
        }

        let bytes = response.bytes().await?;
        Ok(Some(bytes.to_vec()))
    }

    pub async fn get_rpc_set(&self) -> Result<Vec<RpcPeerInfo>> {
        let url = format!("{}/rpc/set", self.base_url);
        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(eyre!("get rpc set failed with status {}", response.status()));
        }
        let resp: RpcSetResponse = response.json().await?;
        Ok(resp.peers)
    }

    pub async fn register_rpc_peer(&self, grpc_addr: String, score_hint: f64) -> Result<RpcPeerInfo> {
        let url = format!("{}/rpc/register", self.base_url);
        let request = RpcRegisterRequest { grpc_addr, score_hint };
        let response = self.client.post(&url).json(&request).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let body = body.trim();
            return Err(if body.is_empty() {
                eyre!("register rpc peer failed with status {status}")
            } else {
                eyre!("register rpc peer failed with status {status}: {body}")
            });
        }
        Ok(response.json().await?)
    }
}

#[derive(Default)]
pub struct Downloader {
    last_slot: u64,
}

fn manifest_hash_map(manifest: &SnapshotManifest) -> Result<HashMap<DBFile, [u8; 32]>> {
    let files = manifest_files(manifest)?;
    let mut hashes = HashMap::new();
    for (db_file, entry) in files.into_iter().zip(manifest.files.iter()) {
        hashes.insert(db_file, entry.blake3_hash);
    }
    Ok(hashes)
}

fn manifest_files_requiring_download(
    manifest: &SnapshotManifest,
    current_manifest: Option<&SignedSnapshotManifest>,
    mirror_dir: &Path,
) -> Result<Vec<DBFile>> {
    let current_hashes = current_manifest
        .map(|manifest| manifest_hash_map(&manifest.manifest))
        .transpose()?;
    let mut files = Vec::new();

    for (db_file, entry) in manifest_files(manifest)?.into_iter().zip(manifest.files.iter()) {
        let already_have = current_hashes
            .as_ref()
            .and_then(|hashes| hashes.get(&db_file))
            .is_some_and(|hash| *hash == entry.blake3_hash) &&
            mirror_dir.join(db_file.to_string()).is_file();
        if !already_have {
            files.push(db_file);
        }
    }

    Ok(files)
}

async fn persist_snapshot_file(persist_dir: &Path, file: &DBFile, bytes: &[u8]) -> Result<()> {
    fs::create_dir_all(persist_dir).await?;

    let final_path = persist_dir.join(file.to_string());
    let temp_path = persist_dir.join(format!(
        "{}.{}.{}.tmp",
        file.to_string(),
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    fs::write(&temp_path, bytes).await?;
    fs::rename(&temp_path, &final_path).await?;
    Ok(())
}

pub fn reduce_data(
    data: HashMap<DBFile, Vec<(Pubkey, AccountSharedData)>>,
) -> Result<HashMap<Pubkey, AccountSharedData>> {
    let mut sorted_data: Vec<_> = data.into_iter().collect();
    sorted_data.sort_by_key(|(file, _)| file.slot());

    let mut accounts = HashMap::new();
    for (_, account_data) in sorted_data {
        for (pubkey, account) in account_data {
            accounts.insert(pubkey, account);
        }
    }

    Ok(accounts)
}

fn parse_aria2c_version(version_output: &str) -> Option<(u32, u32, u32)> {
    let version_line = version_output.lines().find(|line| {
        line.to_ascii_lowercase().contains("aria2c version") || line.to_ascii_lowercase().contains("aria2 version")
    })?;
    let version_str = version_line
        .split_whitespace()
        .find(|token| token.chars().next().is_some_and(|c| c.is_ascii_digit()))?;
    let mut components = version_str.split('.');
    let major = components.next()?.parse().ok()?;
    let minor = components.next().unwrap_or("0").parse().ok()?;
    let patch = components.next().unwrap_or("0").parse().ok()?;
    Some((major, minor, patch))
}

fn version_is_supported(version: (u32, u32, u32)) -> bool {
    version >= MIN_ARIA2C_VERSION
}

async fn ensure_aria2c_ready() -> Result<()> {
    let output = Command::new("aria2c")
        .arg("--version")
        .output()
        .await
        .map_err(|e| eyre::eyre!("failed to execute aria2c --version: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre::eyre!(
            "aria2c --version exited with status {}{}",
            output.status,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(", stderr: {}", stderr.trim())
            }
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    if let Some(version) = parse_aria2c_version(&stdout) {
        if version_is_supported(version) {
            return Ok(());
        }
        return Err(eyre::eyre!(
            "aria2c version {:?} detected; minimum required is {:?}. Update aria2c to continue.",
            version,
            MIN_ARIA2C_VERSION
        ));
    }

    Err(eyre::eyre!(
        "unable to parse aria2c version from output: {}",
        stdout.lines().next().unwrap_or("<empty>")
    ))
}

impl Downloader {
    pub fn last_slot(&self) -> u64 {
        self.last_slot
    }

    pub async fn bulk_download<T: Send + Sync + 'static>(
        &mut self,
        http_client: &HttpClient,
        files: Vec<DBFile>,
        parser: fn(Vec<u8>) -> Result<T>,
    ) -> Result<HashMap<DBFile, T>> {
        self.bulk_download_with_hashes(http_client, files, None, None, parser)
            .await
    }

    pub async fn bulk_download_verified<T: Send + Sync + 'static>(
        &mut self,
        http_client: &HttpClient,
        manifest: &SnapshotManifest,
        files: Vec<DBFile>,
        parser: fn(Vec<u8>) -> Result<T>,
    ) -> Result<HashMap<DBFile, T>> {
        let expected_hashes = manifest_hash_map(manifest)?;
        self.bulk_download_with_hashes(http_client, files, Some(expected_hashes), None, parser)
            .await
    }

    pub async fn bulk_download_verified_and_persist<T: Send + Sync + 'static>(
        &mut self,
        http_client: &HttpClient,
        manifest: &SnapshotManifest,
        files: Vec<DBFile>,
        persist_dir: &Path,
        parser: fn(Vec<u8>) -> Result<T>,
    ) -> Result<HashMap<DBFile, T>> {
        let expected_hashes = manifest_hash_map(manifest)?;
        self.bulk_download_with_hashes(
            http_client,
            files,
            Some(expected_hashes),
            Some(persist_dir.to_path_buf()),
            parser,
        )
        .await
    }

    async fn bulk_download_with_hashes<T: Send + Sync + 'static>(
        &mut self,
        http_client: &HttpClient,
        files: Vec<DBFile>,
        expected_hashes: Option<HashMap<DBFile, [u8; 32]>>,
        persist_dir: Option<PathBuf>,
        parser: fn(Vec<u8>) -> Result<T>,
    ) -> Result<HashMap<DBFile, T>> {
        let base_url = http_client.base_url.clone();
        if let Err(e) = ensure_aria2c_ready().await {
            error!("aria2c is not ready: {}", e);
            return Err(e);
        }

        let multi_progress = MultiProgress::new();
        let progress_style = ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-");

        let mut tasks = Vec::with_capacity(files.len());
        let temp_dir = tempdir()?;
        let temp_dir_path = temp_dir.path().to_path_buf();
        let connection_count = std::thread::available_parallelism()
            .map(|v| v.get())
            .unwrap_or(4)
            .clamp(2, 16);
        let connection_arg = connection_count.to_string();
        info!(
            "Using aria2c with {} parallel connections per download",
            connection_count
        );

        // Create tasks for each file
        for file in files {
            let url = format!("{}/solayer/files/{}", base_url, file.to_string());
            info!("Downloading file {}", url);
            let pb = multi_progress.add(ProgressBar::new(100));
            pb.set_style(progress_style.clone());
            pb.set_message(file.to_string());
            let temp_dir_path = temp_dir_path.clone();
            let connection_arg = connection_arg.clone();
            let expected_hash = expected_hashes.as_ref().and_then(|hashes| hashes.get(&file).copied());
            let persist_dir = persist_dir.clone();

            // Capture identifiers for error-context logging inside the task
            let file_for_task = file.clone();
            let url_for_task = url.clone();

            let task = tokio::spawn(async move {
                let start = Instant::now();
                let file_name = file_for_task.to_string();
                let output_path = temp_dir_path.join(&file_name);

                let output = Command::new("aria2c")
                    .arg("--allow-overwrite=true")
                    .arg("--auto-file-renaming=false")
                    .arg("--summary-interval=0")
                    .arg("--console-log-level=warn")
                    .arg("--max-connection-per-server")
                    .arg(&connection_arg)
                    .arg("--split")
                    .arg(&connection_arg)
                    .arg("--min-split-size=1M")
                    .arg("--dir")
                    .arg(&temp_dir_path)
                    .arg("--out")
                    .arg(&file_name)
                    .arg(&url_for_task)
                    .output()
                    .await
                    .map_err(|e| {
                        (
                            file_for_task.clone(),
                            eyre::eyre!(
                                "failed to spawn aria2c for {} ({}): {}",
                                file_for_task.to_string(),
                                url_for_task,
                                e
                            ),
                        )
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err((
                        file_for_task.clone(),
                        eyre::eyre!(
                            "aria2c failed for {} ({}): {}{}",
                            file_for_task.to_string(),
                            url_for_task,
                            output.status,
                            if stderr.trim().is_empty() {
                                String::new()
                            } else {
                                format!(", stderr: {}", stderr.trim())
                            }
                        ),
                    ));
                }

                let bytes = fs::read(&output_path).await.map_err(|e| {
                    (
                        file_for_task.clone(),
                        eyre::eyre!(
                            "failed to read downloaded file {} ({}): {}",
                            file_for_task.to_string(),
                            output_path.display(),
                            e
                        ),
                    )
                })?;
                if let Err(e) = fs::remove_file(&output_path).await {
                    info!(
                        "failed to remove temporary file {} after parsing: {}",
                        output_path.display(),
                        e
                    );
                }
                pb.set_length(bytes.len() as u64);
                pb.set_position(bytes.len() as u64);
                pb.finish();
                let ms = start.elapsed().as_millis();
                let size = bytes.len();
                infinisvm_logger::info!("Downloaded {} ({} bytes) in {} ms", url_for_task, size, ms);

                if let Some(expected_hash) = expected_hash {
                    let actual_hash = *blake3::hash(&bytes).as_bytes();
                    if actual_hash != expected_hash {
                        return Err((
                            file_for_task.clone(),
                            eyre::eyre!(
                                "hash mismatch for {} ({}): expected {}, got {}",
                                file_for_task.to_string(),
                                url_for_task,
                                hex::encode(expected_hash),
                                hex::encode(actual_hash)
                            ),
                        ));
                    }
                }

                if let Some(persist_dir) = persist_dir.as_ref() {
                    persist_snapshot_file(persist_dir, &file_for_task, &bytes)
                        .await
                        .map_err(|e| {
                            (
                                file_for_task.clone(),
                                eyre::eyre!(
                                    "failed to persist verified snapshot file {}: {}",
                                    file_for_task.to_string(),
                                    e
                                ),
                            )
                        })?;
                }

                let parsed = parser(bytes).map_err(|e| {
                    (
                        file_for_task.clone(),
                        eyre::eyre!(
                            "parse error for {} ({}): {}",
                            file_for_task.to_string(),
                            url_for_task,
                            e
                        ),
                    )
                })?;

                Ok::<(DBFile, T), (DBFile, eyre::Error)>((file_for_task, parsed))
            });

            tasks.push(task);
        }

        // Wait for all downloads to complete
        let mut results = HashMap::new();
        let total = tasks.len();
        let mut completed = 0usize;
        for task in tasks {
            match task.await {
                Ok(Ok((file, result))) => {
                    completed += 1;
                    if completed.is_multiple_of(100) || completed == total {
                        infinisvm_logger::info!("bulk_download progress: {}/{} files completed", completed, total);
                    }
                    if file.slot() > self.last_slot {
                        self.last_slot = file.slot();
                    }
                    results.insert(file, result);
                }
                Ok(Err((file, e))) => {
                    infinisvm_logger::error!("bulk_download failed for {}: {}", file.to_string(), e);
                    return Err(e);
                }
                Err(join_err) => {
                    infinisvm_logger::error!("bulk_download task join error: {}", join_err);
                    return Err(eyre::eyre!("join error: {}", join_err));
                }
            }
        }
        infinisvm_logger::info!(
            "bulk_download finished: {} files; last_slot={}",
            results.len(),
            self.last_slot
        );

        Ok(results)
    }

    pub async fn poll_for_new_manifest_files<T: Send + Sync + 'static>(&mut self, ctx: ManifestPollContext<T>) -> ! {
        let ManifestPollContext {
            http_client,
            sequencer_pubkey,
            manifest_store,
            signed_finalization_slot,
            mirror_dir,
            sender,
            parser,
        } = ctx;
        info!(
            "Downloader.poll_for_new_manifest_files started (initial last_slot={})",
            self.last_slot
        );
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut current_manifest = manifest_store.get().await;
        let mut serving_fresh = manifest_store.is_fresh().await;
        loop {
            interval.tick().await;
            info!("Polling for new manifest files since slot {}", self.last_slot);
            let signed_head_slot = *signed_finalization_slot.borrow();
            if let Some(current) = current_manifest.as_ref() {
                let fresh = match manifest_effective_head_slot(&current.manifest) {
                    Ok(snapshot_head_slot) => {
                        signed_head_slot == 0 || snapshot_head_is_fresh(snapshot_head_slot, signed_head_slot)
                    }
                    Err(err) => {
                        warn!("Failed to compute current snapshot manifest head slot: {err}");
                        false
                    }
                };
                if fresh != serving_fresh {
                    if fresh {
                        info!(signed_head_slot, "Current snapshot mirror is fresh enough to serve");
                    } else {
                        let lag_slots = manifest_effective_head_slot(&current.manifest)
                            .map(|snapshot_head_slot| snapshot_head_lag_slots(snapshot_head_slot, signed_head_slot))
                            .unwrap_or_default();
                        warn!(
                            signed_head_slot,
                            lag_slots, "Current snapshot mirror is stale; withholding snapshot serving"
                        );
                    }
                    manifest_store.set_freshness(fresh).await;
                    serving_fresh = fresh;
                }
            }
            let signed_manifest = match http_client.get_snapshot_manifest().await {
                Ok(manifest) => manifest,
                Err(e) => {
                    warn!("Failed to get snapshot manifest: {}", e);
                    continue;
                }
            };
            if !signed_manifest.verify(&sequencer_pubkey) {
                warn!(
                    checkpoint_slot = signed_manifest.manifest.checkpoint_slot,
                    "Ignoring snapshot manifest with invalid signature"
                );
                continue;
            }
            let snapshot_head_slot = match manifest_effective_head_slot(&signed_manifest.manifest) {
                Ok(slot) => slot,
                Err(err) => {
                    warn!("Failed to compute fetched snapshot manifest head slot: {}", err);
                    continue;
                }
            };
            if signed_head_slot > 0 && !snapshot_head_is_fresh(snapshot_head_slot, signed_head_slot) {
                let lag_slots = snapshot_head_lag_slots(snapshot_head_slot, signed_head_slot);
                if serving_fresh {
                    manifest_store.set_freshness(false).await;
                    serving_fresh = false;
                }
                warn!(
                    snapshot_head_slot,
                    signed_head_slot, lag_slots, "Ignoring stale snapshot manifest from upstream source"
                );
                continue;
            }
            let files = match manifest_files_requiring_download(
                &signed_manifest.manifest,
                current_manifest.as_ref(),
                &mirror_dir,
            ) {
                Ok(files) => files,
                Err(e) => {
                    warn!("Failed to parse snapshot manifest files: {}", e);
                    continue;
                }
            };
            if files.is_empty() {
                info!("No new files found in this tick");
            } else {
                info!("Found {} new files since slot {}", files.len(), self.last_slot);
            }
            // Retry up to 5 times with exponential backoff
            let mut results = None;
            const MAX_RETRIES: u32 = 20;
            const INITIAL_BACKOFF_MS: u64 = 1000; // Start with 1 second
            const BACKOFF_MULTIPLIER: f64 = 2.0;

            for attempt in 1..=MAX_RETRIES {
                match self
                    .bulk_download_verified_and_persist(
                        http_client.as_ref(),
                        &signed_manifest.manifest,
                        files.clone(),
                        &mirror_dir,
                        parser,
                    )
                    .await
                {
                    Ok(res) => {
                        results = Some(res);
                        break;
                    }
                    Err(e) => {
                        if attempt < MAX_RETRIES {
                            let backoff_ms =
                                (INITIAL_BACKOFF_MS as f64 * BACKOFF_MULTIPLIER.powi(attempt as i32 - 1)) as u64;
                            warn!(
                                "Bulk download failed (attempt {}/{}): {}, retrying in {}ms",
                                attempt, MAX_RETRIES, e, backoff_ms
                            );
                            sleep(Duration::from_millis(backoff_ms)).await;
                        } else {
                            error!("Bulk download failed after {} attempts: {}", MAX_RETRIES, e);
                        }
                    }
                }
            }

            let results = if files.is_empty() {
                HashMap::new()
            } else {
                let Some(results) = results else {
                    warn!("Skipping manifest update after bulk download failure; local manifest remains unchanged");
                    continue;
                };
                results
            };

            let mut keep_manifests = vec![&signed_manifest.manifest];
            if let Some(current) = current_manifest.as_ref() {
                keep_manifests.push(&current.manifest);
            }
            if let Err(err) = prune_mirror_dir_to_manifests(&mirror_dir, &keep_manifests).await {
                warn!("Failed to prune stale mirrored snapshot files: {}", err);
            }

            manifest_store.set_ready_manifest(Some(signed_manifest.clone())).await;
            current_manifest = Some(signed_manifest);
            serving_fresh = true;
            info!("Download complete: {} files", results.len());
            for (file, result) in results {
                let file_display = file.to_string();
                if let Err(e) = sender.send((file, result)).await {
                    error!(
                        "Failed to enqueue downloaded file {} to DB-chain updater: {}",
                        file_display, e
                    );
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use infinisvm_db::persistence::DBFile;
    use infinisvm_types::sync::{SignedSnapshotManifest, SnapshotManifest, SnapshotManifestFile};
    use solana_sdk::signature::Keypair;
    use tempfile::tempdir;

    use super::manifest_files_requiring_download;

    fn sign_manifest(checkpoint_slot: u64, files: &[(DBFile, [u8; 32])]) -> SignedSnapshotManifest {
        let keypair = Keypair::new();
        let manifest = SnapshotManifest {
            version: SnapshotManifest::VERSION,
            checkpoint_slot,
            files: files
                .iter()
                .map(|(file, blake3_hash)| SnapshotManifestFile {
                    filename: file.to_string(),
                    blake3_hash: *blake3_hash,
                    size_bytes: 1,
                })
                .collect(),
        };
        SignedSnapshotManifest::sign(manifest, &keypair)
    }

    #[test]
    fn manifest_diff_downloads_rollover_checkpoint_even_if_slot_decreases() {
        let mirror_dir = tempdir().expect("tempdir");
        let current = sign_manifest(
            100,
            &[
                (DBFile::Checkpoint(100), [1; 32]),
                (DBFile::Account(101), [2; 32]),
                (DBFile::Account(102), [3; 32]),
            ],
        );

        for entry in &current.manifest.files {
            fs::write(mirror_dir.path().join(&entry.filename), [0u8]).expect("write mirrored snapshot file");
        }

        let next = SnapshotManifest {
            version: SnapshotManifest::VERSION,
            checkpoint_slot: 101,
            files: vec![
                SnapshotManifestFile {
                    filename: DBFile::Checkpoint(101).to_string(),
                    blake3_hash: [4; 32],
                    size_bytes: 1,
                },
                SnapshotManifestFile {
                    filename: DBFile::Account(102).to_string(),
                    blake3_hash: [3; 32],
                    size_bytes: 1,
                },
                SnapshotManifestFile {
                    filename: DBFile::Account(103).to_string(),
                    blake3_hash: [5; 32],
                    size_bytes: 1,
                },
            ],
        };

        let files =
            manifest_files_requiring_download(&next, Some(&current), mirror_dir.path()).expect("compute manifest diff");

        assert_eq!(files, vec![DBFile::Checkpoint(101), DBFile::Account(103)]);
    }

    #[test]
    fn manifest_diff_redownloads_missing_local_file_even_if_manifest_matches() {
        let mirror_dir = tempdir().expect("tempdir");
        let current = sign_manifest(
            100,
            &[(DBFile::Checkpoint(100), [1; 32]), (DBFile::Account(101), [2; 32])],
        );

        fs::write(mirror_dir.path().join(DBFile::Checkpoint(100).to_string()), [0u8])
            .expect("write mirrored checkpoint");

        let files = manifest_files_requiring_download(&current.manifest, Some(&current), mirror_dir.path())
            .expect("compute manifest diff");

        assert_eq!(files, vec![DBFile::Account(101)]);
    }
}
