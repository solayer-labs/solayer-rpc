use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use futures_util::StreamExt;
use hashbrown::HashMap;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path,
    ObjectStore,
};
use zstd::stream::{decode_all, encode_all};

#[derive(Debug, Clone)]
pub struct S3FsClient {
    local_tmp_path: PathBuf,
    lock: Arc<RwLock<HashMap<String, Arc<RwLock<()>>>>>,
    access_key_id: Option<String>,
    secret_key: Option<String>,
}

pub const REGION: &str = "auto";
pub const BUCKET_NAME: &str = "sequencer-slots";

impl S3FsClient {
    pub fn new(local_tmp_path: PathBuf) -> Self {
        Self {
            local_tmp_path,
            lock: Arc::new(RwLock::new(HashMap::new())),
            access_key_id: None,
            secret_key: None,
        }
    }

    pub fn new_with_credentials(
        local_tmp_path: PathBuf,
        access_key_id: Option<String>,
        secret_key: Option<String>,
    ) -> Self {
        Self {
            local_tmp_path,
            lock: Arc::new(RwLock::new(HashMap::new())),
            access_key_id,
            secret_key,
        }
    }

    fn get_s3(&self) -> eyre::Result<AmazonS3> {
        let access_key_id = self
            .access_key_id
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("S3_ACCESS_KEY_ID").ok())
            .ok_or_else(|| eyre::eyre!("Missing S3 access key id: provide via CLI or S3_ACCESS_KEY_ID env"))?;
        let secret_key = self
            .secret_key
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("S3_SECRET_KEY").ok())
            .ok_or_else(|| eyre::eyre!("Missing S3 secret key: provide via CLI or S3_SECRET_KEY env"))?;

        let s3 = AmazonS3Builder::new()
            .with_region(REGION)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_key)
            .with_bucket_name(format!(
                "{}{}",
                BUCKET_NAME,
                if std::env::var("ENV").unwrap_or_else(|_| "dev".to_string()) == "dev" {
                    "-dev"
                } else {
                    ""
                }
            ))
            .with_endpoint("s3.us-west-2.amazonaws.com")
            .build()
            .map_err(|e| eyre::eyre!("Failed to create S3 client: {}", e))?;
        Ok(s3)
    }

    pub async fn list_dir(&self, key: String) -> eyre::Result<Vec<String>> {
        // Check local filesystem first
        let local_path = self.local_tmp_path.join(key.clone());
        if local_path.exists() && local_path.is_dir() {
            let mut files = Vec::new();
            for entry in std::fs::read_dir(local_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    if let Some(name) = path.file_name() {
                        if let Some(name_str) = name.to_str() {
                            files.push(name_str.to_string());
                        }
                    }
                }
            }
            return Ok(files);
        }

        let mut files = Vec::new();
        let s3 = match self.get_s3() {
            Ok(s3) => s3,
            Err(e) => {
                // No credentials or S3 client not available; return local-only listing
                eprintln!("S3 disabled for list_dir: {e}");
                return Ok(files);
            }
        };
        let mut stream = s3.list(Some(&Path::from(key))).fuse();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if let Some(name) = meta.location.filename() {
                files.push(name.to_string());
            }
        }
        Ok(files)
    }

    pub async fn get_object(&self, key: String) -> eyre::Result<Vec<u8>> {
        // Check local filesystem
        let local_path = self.local_tmp_path.join(key.clone());

        // Check if we need to wait for a write lock
        let should_wait = {
            let locker = self.lock.read().unwrap();
            locker.contains_key(&key)
        };

        // If there's a write in progress, wait for it to complete
        if should_wait {
            let locker = self.lock.read().unwrap();
            if let Some(lock) = locker.get(&key) {
                let _x = lock.read().unwrap();
            }
        }

        // Check local filesystem again after waiting
        if local_path.exists() {
            let compressed = std::fs::read(local_path)?;
            return Ok(decode_all(&compressed[..])?);
        }

        // Fetch from remote S3
        let s3 = self.get_s3()?;
        let object = s3.get(&Path::from(key)).await?;
        let compressed = object.bytes().await?.to_vec();
        Ok(decode_all(&compressed[..])?)
        // Err(eyre::eyre!("Not Found"))
    }

    pub async fn put_object(&self, key: String, data: Vec<u8>) -> eyre::Result<()> {
        // Get write lock for this key
        let mut locker = self.lock.write().unwrap();
        let lock = Arc::new(RwLock::new(()));
        locker.insert(key.clone(), lock.clone());
        let _write_lock = lock.write().unwrap();

        // Compress data
        let compressed = encode_all(&data[..], 0)?;

        // Save to local filesystem first
        let local_path = self.local_tmp_path.join(key.clone());
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&local_path, &compressed)?;

        drop(_write_lock);
        locker.remove(&key);
        drop(locker);

        Ok(())
    }
}
