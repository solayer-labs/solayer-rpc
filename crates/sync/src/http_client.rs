use eyre::Result;
use hashbrown::HashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use infinisvm_db::{persistence::DBFile, versioned::AccountVersion};
use infinisvm_logger::{debug, error, info};
use reqwest::Client;
use serde::Deserialize;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::tempdir;
use tokio::{fs, process::Command, sync::mpsc};

const MIN_ARIA2C_VERSION: (u32, u32, u32) = (1, 35, 0);

#[derive(Debug, Deserialize)]
pub struct SnapshotsResponse {
    pub files: Vec<String>,
}

pub struct Snapshots {
    pub files: Vec<DBFile>,
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
    /// Returns the second largest checkpoint and its associated account files that should be downloaded.
    ///
    /// This function finds the second largest checkpoint and all account files with slots between
    /// that checkpoint and the largest account file. This ensures we get a consistent snapshot
    /// while avoiding the most recent (potentially incomplete) checkpoint.
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

    pub async fn get_snapshots(&self) -> Result<Snapshots> {
        let url = format!("{}/solayer/snapshots", self.base_url);
        info!("Getting snapshots from {}", url);
        let response = self.client.get(&url).send().await?;
        let snapshots = response.json::<SnapshotsResponse>().await?;
        Ok(Snapshots::from(snapshots))
    }

    pub async fn get_file(&self, filename: &DBFile) -> Result<Vec<u8>> {
        let url = format!("{}/solayer/files/{}", self.base_url, filename.to_string());
        info!("Getting file {} from {}", filename.to_string(), url);
        let response = self.client.get(&url).send().await?;
        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }
}

#[derive(Default)]
pub struct Downloader {
    last_slot: u64,
}

pub fn parse_data(bytes: Vec<u8>) -> Result<Vec<(Pubkey, AccountSharedData, AccountVersion)>> {
    let checkpoint = bincode::deserialize(&bytes)?;
    Ok(checkpoint)
}

pub fn reduce_data(
    data: HashMap<DBFile, Vec<(Pubkey, AccountSharedData, AccountVersion)>>,
) -> Result<HashMap<Pubkey, (AccountSharedData, AccountVersion)>> {
    let mut sorted_data: Vec<_> = data.into_iter().collect();
    sorted_data.sort_by_key(|(file, _)| file.slot());

    let mut accounts = HashMap::new();
    for (_, account_data) in sorted_data {
        for (pubkey, account, version) in account_data {
            accounts.insert(pubkey, (account, version));
        }
    }

    Ok(accounts)
}

fn parse_aria2c_version(version_output: &str) -> Option<(u32, u32, u32)> {
    let version_line = version_output
        .lines()
        .find(|line| line.to_ascii_lowercase().contains("aria2c version"))?;
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
            debug!("aria2c version {:?} detected", version);
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
        parser: Arc<impl Fn(Vec<u8>) -> Result<T> + Send + Sync + 'static>,
    ) -> Result<HashMap<DBFile, T>> {
        let base_url = http_client.base_url.clone();
        // ensure_aria2c_ready().await?;

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
            let parser = parser.clone();
            let temp_dir_path = temp_dir_path.clone();
            let connection_arg = connection_arg.clone();

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
                    debug!(
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
                    if completed % 100 == 0 || completed == total {
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

    pub async fn poll_for_new_files<T: Send + Sync + 'static>(
        &mut self,
        http_client: &HttpClient,
        sender: mpsc::Sender<(DBFile, T)>,
        parser: Arc<impl Fn(Vec<u8>) -> Result<T> + Send + Sync + 'static>,
    ) -> Result<()> {
        info!(
            "Downloader.poll_for_new_files started (initial last_slot={})",
            self.last_slot
        );
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            debug!("Polling for new files since slot {}", self.last_slot);
            let snapshots = http_client.get_snapshots().await?;
            let files = snapshots.since_slot(self.last_slot);
            if files.is_empty() {
                debug!("No new files found in this tick");
            } else {
                info!("Found {} new files since slot {}", files.len(), self.last_slot);
            }
            let results = self.bulk_download(http_client, files, parser.clone()).await?;
            info!("Download complete: {} files", results.len());
            for (file, result) in results {
                if let Err(e) = sender.send((file, result)).await {
                    error!("Failed to enqueue downloaded file to DB-chain updater: {}", e);
                    return Err(e.into());
                }
            }
        }
    }
}
