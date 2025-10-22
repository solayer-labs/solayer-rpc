use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock, RwLock,
    },
    time::Duration,
};

use eyre::Result;
use infinisvm_logger::{error, info};
use object_store::aws::{AmazonS3, AmazonS3Builder};

use crate::persistence::PersistedInMemoryDB;

#[derive(Debug)]
pub struct S3UploadConfig {
    client: Arc<AmazonS3>,
    base_prefix: String,
}

impl S3UploadConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bucket: String,
        prefix: Option<String>,
        region: String,
        endpoint: Option<String>,
        access_key_id: String,
        secret_access_key: String,
    ) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .with_region(region);

        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        let client = builder
            .build()
            .map_err(|err| eyre::eyre!("Failed to construct S3 client: {err}"))?;

        let normalized_prefix = prefix
            .and_then(|p| {
                let trimmed = p.trim_matches('/');
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .unwrap_or_default();

        let base_prefix = if normalized_prefix.is_empty() {
            "db".to_string()
        } else {
            format!("{normalized_prefix}/db")
        };

        Ok(Self {
            client: Arc::new(client),
            base_prefix,
        })
    }

    pub fn object_key(&self, file_name: &str) -> String {
        format!("{}/{}", self.base_prefix, file_name)
    }

    pub fn client(&self) -> Arc<AmazonS3> {
        Arc::clone(&self.client)
    }
}

fn s3_config_lock() -> &'static RwLock<Option<Arc<S3UploadConfig>>> {
    static S3_CONFIG: OnceLock<RwLock<Option<Arc<S3UploadConfig>>>> = OnceLock::new();
    S3_CONFIG.get_or_init(|| RwLock::new(None))
}

pub fn configure_s3_upload(config: Option<S3UploadConfig>) {
    let mut guard = s3_config_lock().write().unwrap();
    *guard = config.map(Arc::new);
}

pub(crate) fn current_s3_upload_config() -> Option<Arc<S3UploadConfig>> {
    s3_config_lock().read().unwrap().clone()
}

pub fn spawn(exit: Arc<AtomicBool>) {
    std::thread::Builder::new()
        .name("dbMerger".to_string())
        .spawn(move || {
            worker(exit);
        })
        .unwrap();
}

fn worker(exit: Arc<AtomicBool>) {
    info!("dbMerger started");

    let mut elapsed = 0;
    const MERGE_INTERVAL_SECS: u64 = 100;
    const CHECK_INTERVAL_SECS: u64 = 2;

    while !exit.load(Ordering::Relaxed) {
        // Sleep in small intervals to respond quickly to exit signal
        std::thread::sleep(Duration::from_secs(CHECK_INTERVAL_SECS));
        elapsed += CHECK_INTERVAL_SECS;

        // Only merge when we've reached the full interval
        if elapsed >= MERGE_INTERVAL_SECS {
            if let Err(err) = PersistedInMemoryDB::merge_accounts() {
                error!("Failed to merge accounts db files: {:?}", err);
            }
            elapsed = 0; // Reset counter
        }
    }

    println!("dbMerger exited");
}
