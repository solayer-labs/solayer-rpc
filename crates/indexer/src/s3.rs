use std::{
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, RwLock},
    thread,
};

use bytes::Bytes;
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures_util::StreamExt;
use hashbrown::HashMap;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path,
    ObjectStore,
};
use tokio::{runtime::Runtime, sync::oneshot};
use zstd::stream::{decode_all, encode_all};

#[derive(Clone)]
pub struct S3FsClient {
    inner: Arc<S3FsClientInner>,
}

struct S3FsClientInner {
    local_tmp_path: PathBuf,
    lock: RwLock<HashMap<String, Arc<RwLock<()>>>>,
    access_key_id: Option<String>,
    secret_key: Option<String>,
    s3_client: Mutex<Option<AmazonS3>>,
    uploader: Uploader,
}

#[derive(Debug)]
struct Uploader {
    sender: OnceLock<Sender<UploadTask>>,
    worker_count: usize,
}

#[derive(Debug)]
struct UploadTask {
    key: String,
    local_path: PathBuf,
    completion: oneshot::Sender<eyre::Result<()>>,
}

pub const REGION: &str = "auto";
pub const BUCKET_NAME: &str = "sequencer-slots";
const DEFAULT_UPLOAD_THREADS: usize = 8;

impl S3FsClient {
    pub fn new(local_tmp_path: PathBuf) -> Self {
        Self::new_with_credentials(local_tmp_path, None, None)
    }

    pub fn new_with_credentials(
        local_tmp_path: PathBuf,
        access_key_id: Option<String>,
        secret_key: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(S3FsClientInner::new(local_tmp_path, access_key_id, secret_key)),
        }
    }

    fn maybe_s3(&self) -> eyre::Result<Option<AmazonS3>> {
        self.inner.ensure_client()
    }

    fn get_s3(&self) -> eyre::Result<AmazonS3> {
        self.maybe_s3()?
            .ok_or_else(|| eyre::eyre!("Missing S3 access key id: provide via CLI or S3_ACCESS_KEY_ID env"))
    }

    fn maybe_uploader_sender(&self) -> eyre::Result<Option<Sender<UploadTask>>> {
        let client = match self.maybe_s3()? {
            Some(client) => client,
            None => return Ok(None),
        };
        Ok(Some(self.inner.uploader.sender(client)))
    }

    pub async fn list_dir(&self, key: String) -> eyre::Result<Vec<String>> {
        let local_path = self.inner.local_tmp_path.join(key.clone());
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
        let local_path = self.inner.local_tmp_path.join(key.clone());

        let should_wait = {
            let locker = self.inner.lock.read().unwrap();
            locker.contains_key(&key)
        };

        if should_wait {
            let locker = self.inner.lock.read().unwrap();
            if let Some(lock) = locker.get(&key) {
                let _guard = lock.read().unwrap();
                drop(_guard);
            }
        }

        if local_path.exists() {
            let compressed = std::fs::read(local_path)?;
            return Ok(decode_all(&compressed[..])?);
        }

        let s3 = self.get_s3()?;
        let object = s3.get(&Path::from(key)).await?;
        let compressed = object.bytes().await?.to_vec();
        Ok(decode_all(&compressed[..])?)
    }

    pub async fn put_object(&self, key: String, data: Vec<u8>) -> eyre::Result<()> {
        let mut locker = self.inner.lock.write().unwrap();
        let lock = Arc::new(RwLock::new(()));
        locker.insert(key.clone(), lock.clone());
        let write_guard = lock.write().unwrap();

        let compressed = encode_all(&data[..], 0)?;

        let local_path = self.inner.local_tmp_path.join(key.clone());
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&local_path, &compressed)?;

        drop(write_guard);
        locker.remove(&key);
        drop(locker);

        if let Some(sender) = self.maybe_uploader_sender()? {
            let (tx, rx) = oneshot::channel();
            sender
                .send(UploadTask {
                    key: key.clone(),
                    local_path: local_path.clone(),
                    completion: tx,
                })
                .map_err(|e| eyre::eyre!("Failed to enqueue S3 upload for {}: {}", key, e))?;

            rx.await
                .map_err(|_| eyre::eyre!("S3 upload worker dropped for {}", key))??;
        }

        Ok(())
    }
}

impl fmt::Debug for S3FsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3FsClient")
            .field("local_tmp_path", &self.inner.local_tmp_path)
            .field("has_access_key", &self.inner.access_key_id.is_some())
            .field("has_secret_key", &self.inner.secret_key.is_some())
            .finish()
    }
}

impl S3FsClientInner {
    fn new(local_tmp_path: PathBuf, access_key_id: Option<String>, secret_key: Option<String>) -> Self {
        Self {
            local_tmp_path,
            lock: RwLock::new(HashMap::new()),
            access_key_id,
            secret_key,
            s3_client: Mutex::new(None),
            uploader: Uploader::new(resolved_worker_count()),
        }
    }

    fn ensure_client(&self) -> eyre::Result<Option<AmazonS3>> {
        {
            let guard = self.s3_client.lock().unwrap();
            if let Some(client) = guard.as_ref() {
                return Ok(Some(client.clone()));
            }
        }

        let access_key_id = self
            .access_key_id
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("S3_ACCESS_KEY_ID").ok());
        let secret_key = self
            .secret_key
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("S3_SECRET_KEY").ok());

        let Some(access_key_id) = access_key_id else {
            return Ok(None);
        };
        let Some(secret_key) = secret_key else {
            return Ok(None);
        };

        let bucket = format!(
            "{}{}",
            BUCKET_NAME,
            if std::env::var("ENV").unwrap_or_else(|_| "dev".to_string()) == "dev" {
                "-dev"
            } else {
                ""
            }
        );

        let s3 = AmazonS3Builder::new()
            .with_region(REGION)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_key)
            .with_bucket_name(bucket)
            .with_endpoint("s3.us-west-2.amazonaws.com")
            .build()
            .map_err(|e| eyre::eyre!("Failed to create S3 client: {}", e))?;

        let mut guard = self.s3_client.lock().unwrap();
        if let Some(existing) = guard.as_ref() {
            return Ok(Some(existing.clone()));
        }
        *guard = Some(s3.clone());
        Ok(Some(s3))
    }
}

impl Uploader {
    fn new(worker_count: usize) -> Self {
        Self {
            sender: OnceLock::new(),
            worker_count: worker_count.max(1),
        }
    }

    fn sender(&self, client: AmazonS3) -> Sender<UploadTask> {
        self.sender
            .get_or_init(move || {
                let (tx, rx) = unbounded::<UploadTask>();
                for idx in 0..self.worker_count {
                    let worker_rx = rx.clone();
                    let worker_client = client.clone();
                    thread::Builder::new()
                        .name(format!("s3-upload-{idx}"))
                        .spawn(move || run_upload_worker(worker_rx, worker_client))
                        .expect("failed to spawn s3 upload worker");
                }
                tx
            })
            .clone()
    }
}

fn resolved_worker_count() -> usize {
    std::env::var("S3_UPLOAD_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(DEFAULT_UPLOAD_THREADS)
}

fn run_upload_worker(rx: Receiver<UploadTask>, client: AmazonS3) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build s3 upload runtime");

    for task in rx.iter() {
        let UploadTask {
            key,
            local_path,
            completion,
        } = task;
        let result = process_upload(&runtime, &client, key, local_path);
        let _ = completion.send(result);
    }
}

fn process_upload(runtime: &Runtime, client: &AmazonS3, key: String, local_path: PathBuf) -> eyre::Result<()> {
    let data = std::fs::read(&local_path)
        .map_err(|e| eyre::eyre!("Failed to read {} for upload: {}", local_path.display(), e))?;
    let location = Path::from(key.clone());
    let location_repr = location.to_string();

    runtime.block_on(async {
        client
            .put(&location, Bytes::from(data).into())
            .await
            .map_err(|e| eyre::eyre!("Failed to upload {location_repr} to S3: {e}"))
    })?;

    if let Err(err) = std::fs::remove_file(&local_path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            return Err(eyre::eyre!(
                "Failed to remove {} after upload: {}",
                local_path.display(),
                err
            ));
        }
    }

    Ok(())
}
