use std::{
    error::Error as StdError,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use eyre::{bail, Context as _, ContextCompat as _};
use futures_util::StreamExt;
use hashbrown::HashMap;
use infinisvm_logger::warn;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path,
    ObjectStore,
};
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot, Mutex, RwLock,
};
use zstd::stream::{decode_all, encode_all};

#[derive(Clone)]
pub struct S3FsClient {
    inner: Arc<S3FsClientInner>,
}

struct S3FsClientInner {
    local_tmp_path: PathBuf,
    lock: RwLock<HashMap<String, Arc<RwLock<()>>>>,
    s3_client: AmazonS3,
    task_tx: OnceLock<UnboundedSender<UploadTask>>,
}

#[derive(Debug)]
struct UploadTask {
    key: String,
    local_path: PathBuf,
    data: Bytes,
    completion: oneshot::Sender<eyre::Result<()>>,
}

pub const REGION: &str = "auto";
const DEFAULT_UPLOAD_THREADS: usize = 8;

struct S3Config {
    s3_path: String,
    access_key_id: String,
    secret_key: String,
    region: String,
    endpoint: Option<String>,
}

impl S3FsClient {
    pub fn new_with_credentials(
        local_tmp_path: PathBuf,
        access_key_id: String,
        secret_key: String,
        s3_path: String,
        region: String,
    ) -> eyre::Result<Self> {
        Ok(Self {
            inner: Arc::new(S3FsClientInner::new(
                local_tmp_path,
                access_key_id,
                secret_key,
                s3_path,
                region,
            )?),
        })
    }

    pub async fn list_dir(&self, key: String) -> eyre::Result<Vec<String>> {
        let local_path = self.inner.local_tmp_path.join(key.clone());

        if let Ok(meta) = tokio::fs::metadata(&local_path).await {
            if meta.is_dir() {
                let mut files = Vec::new();
                let mut read_dir = tokio::fs::read_dir(local_path).await?;

                while let Some(entry) = read_dir.next_entry().await? {
                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }

                    if let Some(name) = path.file_name() {
                        if let Some(name_str) = name.to_str() {
                            files.push(name_str.to_string());
                        }
                    }
                }

                return Ok(files);
            }
        }

        let mut files = Vec::new();
        let mut stream = self.inner.s3_client.list(Some(&Path::from(key))).fuse();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if let Some(name) = meta.location.filename() {
                files.push(name.to_string());
            }
        }
        Ok(files)
    }

    pub async fn get_object(&self, key: String) -> eyre::Result<Bytes> {
        let local_path = self.inner.local_tmp_path.join(key.clone());

        {
            let locker = self.inner.lock.read().await;
            if let Some(lock) = locker.get(&key) {
                let _guard = lock.read().await;
                drop(_guard);
            }
        }

        let compressed = match tokio::fs::read(&local_path).await {
            Ok(data) => Some(data.into()),
            Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
                warn!(%key, ?local_path, "Error reading local cached object: {err:#}");
                None
            }
            _ => {
                // File not found
                None
            }
        };

        let compressed = match compressed {
            Some(compressed) => compressed,
            None => self.inner.s3_client.get(&Path::from(key)).await?.bytes().await?,
        };

        Ok(decode_all(&compressed[..])?.into())
    }

    pub async fn put_object(&self, key: String, data: Bytes) -> eyre::Result<()> {
        let mut locker = self.inner.lock.write().await;
        let lock = Arc::new(RwLock::new(()));
        locker.insert(key.clone(), lock.clone());
        let write_guard = lock.write().await;

        let compressed: Bytes = encode_all(&data[..], 0)?.into();

        let local_path = self.inner.local_tmp_path.join(key.clone());
        let parent = local_path.parent().context("Error getting parent directory")?;
        tokio::fs::create_dir_all(parent)
            .await
            .context("Error creating parent directory")?;
        tokio::fs::write(&local_path, &compressed)
            .await
            .context("Error writing file")?;

        drop(write_guard);
        locker.remove(&key);
        drop(locker);

        let sender = self.inner.sender();
        let (tx, rx) = oneshot::channel();

        sender
            .send(UploadTask {
                key: key.clone(),
                local_path: local_path.clone(),
                data: compressed,
                completion: tx,
            })
            .context("Error enqueuing S3 upload")?;

        rx.await.context("S3 upload worker dropped")?
    }
}

impl S3FsClientInner {
    fn new(
        local_tmp_path: PathBuf,
        access_key_id: String,
        secret_key: String,
        s3_path: String,
        region: String,
    ) -> eyre::Result<Self> {
        let config = Self::build_config(access_key_id, secret_key, s3_path, region)?;
        let client = Self::create_client(&config)?;

        Ok(Self {
            local_tmp_path,
            s3_client: client,
            lock: RwLock::new(HashMap::new()),
            task_tx: OnceLock::new(),
        })
    }

    fn build_config(
        access_key_id: String,
        secret_key: String,
        s3_path: String,
        region: String,
    ) -> eyre::Result<S3Config> {
        let endpoint = std::env::var("S3_ENDPOINT").ok();

        Ok(S3Config {
            access_key_id,
            secret_key,
            s3_path,
            region,
            endpoint,
        })
    }

    fn create_client(config: &S3Config) -> eyre::Result<AmazonS3> {
        let mut builder = AmazonS3Builder::new()
            .with_region(&config.region)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_key)
            .with_bucket_name(&config.s3_path);

        // Only set endpoint if explicitly provided via environment variable
        // The object_store library constructs the endpoint automatically for standard
        // AWS regions
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        builder.build().context("Failed to create S3 client")
    }

    pub fn sender(&self) -> UnboundedSender<UploadTask> {
        self.task_tx
            .get_or_init(move || {
                let (tx, rx) = mpsc::unbounded_channel();
                let rx = Arc::new(Mutex::new(rx));
                for _ in 0..resolved_worker_count() {
                    let worker_rx = rx.clone();
                    let worker_client = self.s3_client.clone();
                    tokio::spawn(run_upload_worker(worker_rx, worker_client));
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

async fn run_upload_worker(rx: Arc<Mutex<UnboundedReceiver<UploadTask>>>, client: AmazonS3) {
    loop {
        let task = {
            let mut rx = rx.lock().await;
            match rx.recv().await {
                Some(task) => task,
                None => {
                    // channel closed
                    break;
                }
            }
        };

        let UploadTask {
            key,
            local_path,
            data,
            completion,
        } = task;
        let result = process_upload(&client, key, local_path, data).await;
        let _ = completion.send(result);
    }
}

async fn process_upload(client: &AmazonS3, key: String, local_path: PathBuf, data: Bytes) -> eyre::Result<()> {
    let location = Path::from(key.clone());

    client.put(&location, data.into()).await.map_err(|e| {
        // Log detailed error information for debugging
        let error_msg = format!("Failed to upload {location} to S3: {e:?}");
        if let Some(source) = e.source() {
            eyre::eyre!("{} (source: {})", error_msg, source)
        } else {
            eyre::eyre!("{}", error_msg)
        }
    })?;

    if let Err(err) = tokio::fs::remove_file(&local_path).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            bail!("Failed to remove {} after upload: {err:#}", local_path.display());
        }
    }

    Ok(())
}
