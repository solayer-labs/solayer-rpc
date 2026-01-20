use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use infinisvm_logger::{error, info, info_span, tracing, warn};
use jsonrpsee::tracing::Instrument;

use crate::s3::S3FsClient;

const DEFAULT_SLOT_BUFFER_SIZE: u64 = 1000;
const DEFAULT_POLL_INTERVAL_SECS: u64 = 5;
const DEFAULT_MAX_WORKERS: usize = 24;

pub struct SlotUploaderConfig {
    pub base_dir: PathBuf,
    pub s3_client: S3FsClient,
    pub slot_buffer_size: u64,
    pub poll_interval: Duration,
    pub max_workers: usize,
}

impl SlotUploaderConfig {
    pub fn new(base_dir: PathBuf, s3_client: S3FsClient) -> Self {
        let slot_buffer_size = std::env::var("SLOT_BUFFER_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SLOT_BUFFER_SIZE);

        let poll_interval_secs = std::env::var("SLOT_UPLOAD_POLL_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS);

        let max_workers = std::env::var("SLOT_UPLOAD_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_WORKERS);

        Self {
            base_dir,
            s3_client,
            slot_buffer_size,
            poll_interval: Duration::from_secs(poll_interval_secs),
            max_workers,
        }
    }
}

pub fn run_slot_uploader(config: SlotUploaderConfig, exit: Arc<AtomicBool>) {
    info!(
        "Starting slot uploader: base_dir={:?}, buffer_size={}, poll_interval={:?}, max_workers={}",
        config.base_dir, config.slot_buffer_size, config.poll_interval, config.max_workers
    );

    // Create a tokio runtime for async operations
    // Use multi_thread runtime to properly handle S3FsClient's blocking operations
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("slot-uploader")
        .build()
        .unwrap();

    while !exit.load(Ordering::Relaxed) {
        if let Err(e) = rt.block_on(upload_cycle(&config, exit.clone())) {
            error!("Error in upload cycle: {e:?}");
        }

        // Wait for poll interval or early exit
        let mut elapsed = Duration::ZERO;
        while elapsed < config.poll_interval && !exit.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
            elapsed += Duration::from_secs(1);
        }
    }

    info!("Slot uploader shutting down");
}

async fn upload_cycle(config: &SlotUploaderConfig, exit: Arc<AtomicBool>) -> eyre::Result<()> {
    info!("Scanning filesystem for slots...");
    let all_slots = discover_slots(&config.base_dir).await?;
    info!("Discovered {} slots", all_slots.len());

    let Some(latest_slot) = all_slots.iter().max().copied() else {
        info!("No slots found, waiting for next scan");
        return Ok(());
    };

    info!("Latest slot: {}", latest_slot);
    let cutoff = latest_slot.saturating_sub(config.slot_buffer_size);
    let slots_to_upload: Vec<u64> = all_slots.into_iter().filter(|&slot| slot < cutoff).collect::<Vec<_>>();

    info!(
        "Slots to upload: {} (keeping latest {} slots as buffer)",
        slots_to_upload.len(),
        config.slot_buffer_size
    );

    if slots_to_upload.is_empty() {
        info!("No slots to upload (all slots are in buffer)");
        return Ok(());
    }

    // Upload slots in parallel with semaphore to limit concurrency
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_workers));
    let mut handles = Vec::new();

    for slot in slots_to_upload {
        if exit.load(Ordering::Relaxed) {
            info!("Exit flag set, stopping slot uploads");
            break;
        }

        let permit = semaphore.clone().acquire_owned().await?;
        let s3_client = config.s3_client.clone();
        let base_dir = config.base_dir.clone();

        handles.push(tokio::task::spawn(async move {
            let _p = permit;
            upload_slot(&base_dir, &s3_client, slot)
                .instrument(info_span!("upload_slot", slot))
                .await
        }));
    }

    // Wait for all uploads to complete
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("Upload task failed: {e:?}"),
            Err(e) => error!("Failed to join upload task: {e:?}"),
        }
    }

    info!("Upload batch completed");
    Ok(())
}

async fn discover_slots(base_dir: &Path) -> eyre::Result<HashSet<u64>> {
    let mut slots = HashSet::new();

    if !base_dir.exists() {
        warn!("Base directory does not exist: {:?}", base_dir);
        return Ok(slots);
    }

    let mut read_dir = tokio::fs::read_dir(base_dir).await?;

    while let Some(p256_entry) = read_dir.next_entry().await? {
        let p256_path = p256_entry.path();
        if !p256_path.is_dir() {
            continue;
        }

        let p256_name = p256_path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| s.parse::<u64>().ok());

        let Some(p256) = p256_name else {
            continue;
        };

        let mut p65535_entries = match tokio::fs::read_dir(&p256_path).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read directory {:?}: {e}", p256_path);
                continue;
            }
        };

        while let Some(p65535_entry) = p65535_entries.next_entry().await? {
            let p65535_path = p65535_entry.path();
            if !p65535_path.is_dir() {
                continue;
            }

            let p65535_name = p65535_path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| s.parse::<u64>().ok());

            let Some(p65535_val) = p65535_name else {
                continue;
            };

            let mut slot_entries = match tokio::fs::read_dir(&p65535_path).await {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("Failed to read directory {:?}: {e}", p65535_path);
                    continue;
                }
            };

            while let Some(slot_entry) = slot_entries.next_entry().await? {
                let slot_path = slot_entry.path();
                if !slot_path.is_dir() {
                    continue;
                }

                let slot_name = slot_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.parse::<u64>().ok());

                let Some(slot) = slot_name else {
                    continue;
                };

                // Verify the slot path matches the expected pattern
                if slot % 256 == p256 && slot % 65535 == p65535_val {
                    slots.insert(slot);
                }
            }
        }
    }

    Ok(slots)
}

#[tracing::instrument(skip_all, fields(slot))]
async fn upload_slot(base_dir: &Path, s3_client: &S3FsClient, slot: u64) -> eyre::Result<()> {
    let slot_dir = slot_directory(base_dir, slot);
    if !slot_dir.exists() || !slot_dir.is_dir() {
        return Ok(());
    }

    // Find all files in the slot directory
    let files = find_files_in_dir_async(slot_dir.clone()).await?;
    if files.is_empty() {
        return Ok(());
    }

    info!("Started ({} files)", files.len());

    let mut uploaded_count = 0;
    let start = Instant::now();

    // Upload all files
    for file_path in &files {
        let relative_path = file_path.strip_prefix(base_dir)?;
        let key = relative_path
            .to_str()
            .ok_or_else(|| eyre::eyre!("Invalid path encoding"))?
            .replace('\\', "/"); // Normalize path separators

        let data = tokio::fs::read(file_path).await?;
        let start = Instant::now();

        if let Err(e) = s3_client.put_object(key.clone(), data.into()).await {
            error!("Failed to upload {}: {e:?}", key);
            return Err(e);
        }

        info!(elapsed = ?start.elapsed(), %key, "Uploaded");
        uploaded_count += 1;
    }

    info!(elapsed = ?start.elapsed(), "Finished: {} uploaded", uploaded_count);

    // Remove the slot files and directory after successful upload
    for file_path in &files {
        if let Err(e) = tokio::fs::remove_file(file_path).await {
            warn!("Failed to delete file {:?}: {e:#}", file_path);
        }
    }

    if let Err(err) = tokio::fs::remove_dir_all(&slot_dir).await {
        warn!("Failed to delete slot directory {:?}: {err:#}", slot_dir);
    } else {
        info!("Slot {} directory removed", slot);
    }

    Ok(())
}

fn slot_directory(base_dir: &Path, slot: u64) -> PathBuf {
    base_dir
        .join((slot % 256).to_string())
        .join((slot % 65535).to_string())
        .join(slot.to_string())
}

fn find_files_in_dir(dir: PathBuf) -> eyre::Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if !dir.exists() {
        return Ok(files);
    }

    let entries = std::fs::read_dir(dir)?;

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            files.push(path);
        } else if path.is_dir() {
            // Recursively find files in subdirectories
            let sub_files = find_files_in_dir(path)?;
            files.extend(sub_files);
        }
    }

    Ok(files)
}

async fn find_files_in_dir_async(dir: PathBuf) -> eyre::Result<Vec<PathBuf>> {
    tokio::task::spawn_blocking(|| find_files_in_dir(dir)).await?
}
