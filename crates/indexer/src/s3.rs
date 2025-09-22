use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use futures_util::StreamExt;
use hashbrown::HashMap;
use object_store::aws::AmazonS3;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use zstd::stream::{decode_all, encode_all};

#[derive(Debug, Clone)]
pub struct S3FsClient {
    // s3: AmazonS3,
    local_tmp_path: PathBuf,
    lock: Arc<RwLock<HashMap<String, Arc<RwLock<()>>>>>,
    access_key_id: Option<String>,
    secret_key: Option<String>,
    // file_system_only: bool,
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
        // let s3 = AmazonS3Builder::new()
        //     .with_region(REGION)
        //     .with_bucket_name(BUCKET_NAME)
        //     .with_access_key_id(ACCESS_KEY_ID)
        //     .with_secret_access_key(SECRET_KEY)
        //     .with_endpoint("https://57e7777c531440a7095f6b86d24d79f6.r2.cloudflarestorage.com")
        //     .build()
        //     .expect("Failed to create S3 client");

        Self {
            // s3,
            local_tmp_path,
            lock: Arc::new(RwLock::new(HashMap::new())),
            access_key_id,
            secret_key,
            // file_system_only,
        }
    }

    // pub fn new_bulk(path: PathBuf, cnt: usize, file_system_only: bool) -> Vec<Self> {
    //     let lock = Arc::new(RwLock::new(HashMap::new()));
    //     let mut clients = Vec::new();
    //     for _ in 0..cnt {
    //         let client = S3FsClient::new(path.clone(), lock.clone(), file_system_only);
    //         clients.push(client);
    //     }
    //     clients
    // }

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
            .with_endpoint("https://57e7777c531440a7095f6b86d24d79f6.r2.cloudflarestorage.com")
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
                eprintln!("S3 disabled for list_dir: {}", e);
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

        // Clone necessary data for background task
        // if !self.file_system_only {
        //     let s3 = self.s3.clone();
        //     let key = key.clone();
        //     let local_path = local_path.clone();
        //     let locker = self.lock.clone();

        //     // Spawn background task to upload and cleanup
        //     let path = Path::from(key.clone());
        //     if let Err(e) = s3.put(&path, PutPayload::from(compressed)).await {
        //         eprintln!("Failed to upload to S3: {}", e);
        //     }

        //     // Clean up local file
        //     let mut locker = locker.write().unwrap();
        //     let _write_lock = locker.get_mut(&key).unwrap().write().unwrap();

        //     if let Err(e) = std::fs::remove_file(&local_path) {
        //         eprintln!("Failed to remove local file: {}", e);
        //     }

        //     drop(_write_lock);
        //     locker.remove(&key);
        //     drop(locker);
        // }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eyre::Result;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_local() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let store = S3FsClient::new(tmp_dir.path().to_path_buf());

        let key = "test/file.txt".to_string();
        let data = b"hello world".to_vec();

        store.put_object(key.clone(), data.clone()).await?;

        // Verify file exists locally
        let local_path = tmp_dir.path().join(&key);
        assert!(local_path.exists());

        // Verify contents (need to decompress)
        let compressed = std::fs::read(&local_path)?;
        let decompressed = decode_all(&compressed[..])?;
        assert_eq!(decompressed, data);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_concurrent() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let store = Arc::new(S3FsClient::new(tmp_dir.path().to_path_buf()));

        let key = "test/file.txt".to_string();
        let data = b"hello world".to_vec();

        let mut handles = vec![];

        // Spawn multiple concurrent writes
        for i in 0..10 {
            let store = store.clone();
            let key = format!("{}{}", key, i);
            let data = data.clone();

            handles.push(tokio::spawn(async move { store.put_object(key, data).await }));
        }

        // Wait for all writes to complete
        for handle in handles {
            handle.await??;
        }

        // Verify all files exist
        for i in 0..10 {
            let path = tmp_dir.path().join(format!("{}{}", key, i));
            assert!(path.exists());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_read() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let store = S3FsClient::new(tmp_dir.path().to_path_buf());

        let key = "test/file.txt".to_string();
        let data = b"hello world".to_vec();

        // Write file first
        store.put_object(key.clone(), data.clone()).await?;

        // Read file back
        let read_data = store.get_object(key).await?;

        // Verify contents match
        assert_eq!(read_data, data);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_object() -> Result<()> {
        let store = S3FsClient::new(PathBuf::from("."));
        let data = store.get_object("0/1/65536/info".to_string()).await?;
        println!("{:?}", data);
        Ok(())
    }

    #[tokio::test]
    async fn test_walk_dir() -> Result<()> {
        let store = S3FsClient::new(PathBuf::from("."));
        let files = store.list_dir("0/".to_string()).await?;
        println!("{:?}", files);
        Ok(())
    }
}
