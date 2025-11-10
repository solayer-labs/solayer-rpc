use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

use eyre::Result;
use hashbrown::HashMap;
use infinisvm_db::{
    db_chain::{DBChain, DBMeta},
    in_memory_db::NoopDB,
    MemoryDB,
};
use infinisvm_logger::{debug, error, info};
use infinisvm_sync::http_client::{reduce_data, Downloader, HttpClient};
use metrics::{counter, gauge, histogram};
use solana_pubkey::Pubkey;
use solana_sdk::account::AccountSharedData;
use tokio::{sync::mpsc, task::JoinHandle};

pub(super) struct BootstrapOutput {
    pub db_chain: Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>,
    pub last_slot: u64,
    pub handles: Vec<JoinHandle<()>>,
}

// 1. download snapshots and initialize db chain
// 2. spawn file poller downloading thread and processing thread
//        + download files and push to channel
//        + process files and insert db chain
pub(super) async fn bootstrap(http_client: Arc<HttpClient>) -> Result<BootstrapOutput> {
    let t_get_snapshots = Instant::now();
    let snapshots = http_client.get_snapshots().await?;
    histogram!("cold_start_get_snapshots_ms").record(t_get_snapshots.elapsed().as_secs_f64() * 1000.0);
    let ckpts_to_download = snapshots.get_ckpts_to_download().expect("No checkpoints to download");
    debug!("Checkpoints to download: {:?}", ckpts_to_download);

    let mut downloader = Downloader::default();

    let mut all_ckpts = vec![ckpts_to_download.ckpt];
    all_ckpts.extend(ckpts_to_download.accounts);
    info!("Starting bulk download of {} checkpoints", all_ckpts.len());

    let t_bulk_download = Instant::now();
    let data = downloader
        .bulk_download(&http_client, all_ckpts, |bytes: Vec<u8>| {
            Ok(bincode::deserialize::<Vec<(Pubkey, AccountSharedData)>>(&bytes)?)
        })
        .await
        .expect("Bulk download failed");
    histogram!("cold_start_bulk_download_ms").record(t_bulk_download.elapsed().as_secs_f64() * 1000.0);
    gauge!("cold_start_last_slot").set(downloader.last_slot() as f64);
    info!("Completed bulk download of checkpoints");

    let t_reduce = Instant::now();
    let reduced = reduce_data(data)?;
    histogram!("cold_start_reduce_data_ms").record(t_reduce.elapsed().as_secs_f64() * 1000.0);
    debug!("Reduced data size: {} entries", reduced.len());

    let mut db_chain = DBChain::default();
    let last_slot = downloader.last_slot();
    info!("Initializing DB chain with last slot {}", last_slot);
    db_chain.add_db(
        Arc::new(RwLock::new(MemoryDB::from_hashmap(reduced))),
        DBMeta::from_ckpt(last_slot),
    );
    gauge!("cold_start_initial_chain_len").set(db_chain.len() as f64);
    debug!("DBChain initialized: {}", db_chain.summary());

    let db_chain_ref = Arc::new(RwLock::new(db_chain));
    let mut handles = Vec::new();

    info!("Starting file polling thread");
    let (tx, mut rx) = mpsc::channel(10240);
    let http_client_poll = http_client.clone();
    let file_poller_handle = tokio::spawn(async move {
        info!("File poller task started");
        downloader
            .poll_for_new_files(&http_client_poll, tx, |bytes: Vec<u8>| {
                Ok(bincode::deserialize::<Vec<(Pubkey, AccountSharedData)>>(&bytes)?)
            })
            .await;
    });
    handles.push(file_poller_handle);

    info!("Starting DB chain update thread");
    let db_chain_ref_clone = db_chain_ref.clone();
    let file_poller_update_handle = tokio::spawn(async move {
        info!("DB chain update task started");
        while let Some((slot, data)) = rx.recv().await {
            let file_kind = match &slot {
                infinisvm_db::persistence::DBFile::Checkpoint(_) => "checkpoint",
                infinisvm_db::persistence::DBFile::Account(_) => "account",
                infinisvm_db::persistence::DBFile::Shred(_, _) => "shred",
            };
            counter!("file_poller_received_total", "type" => file_kind).increment(1);
            histogram!("file_poller_records_len", "type" => file_kind).record(data.len() as f64);

            let new_db = MemoryDB::from_hashmap(HashMap::from_iter(data.into_iter()));
            let meta = DBMeta::from_db_file(slot);
            let mut chain = db_chain_ref_clone.write().unwrap();
            let before = chain.len();
            let t_add = Instant::now();
            info!(
                "File poller: adding {:?}; chain size {} -> {}?",
                meta,
                before,
                before + 1
            );
            chain.add_db(Arc::new(RwLock::new(new_db)), meta);
            histogram!("file_poller_add_db_ms").record(t_add.elapsed().as_secs_f64() * 1000.0);
            debug!("File poller: post-add summary: {}", chain.summary());
        }

        error!("file poller send channel closed");
    });
    handles.push(file_poller_update_handle);

    Ok(BootstrapOutput {
        db_chain: db_chain_ref,
        last_slot,
        handles,
    })
}
