mod bootstrap;
mod slot;
mod tx;
mod utils;

use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
    time::Instant,
};

use dashmap::{DashMap, DashSet};
use eyre::Result;
use hashbrown::HashMap;
use infinisvm_core::{bank::Bank, indexer::Indexer, subscription::SubscriptionProcessor};
use infinisvm_db::{db_chain::DBChain, in_memory_db::NoopDB, MemoryDB};
use infinisvm_logger::info;
use infinisvm_sync::{grpc::client::SyncClient, http_client::HttpClient, types::SerializableBatch};
use infinisvm_types::sync::grpc::{CommitBatchNotification, RawSlot};
use metrics::{counter, histogram};
use solana_sdk::{hash::Hash, signature::Signature};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};

pub async fn cold_start(
    http_client: Arc<HttpClient>,
    tx_receivers: Vec<mpsc::Receiver<Arc<CommitBatchNotification>>>,
    _slot_receivers: Vec<mpsc::Receiver<RawSlot>>, // No longer used, kept for API compatibility
    indexer: Arc<Mutex<dyn Indexer>>,
    bank: Arc<RwLock<Bank>>,
    subscription_processor: Arc<SubscriptionProcessor>,
    refetch_pool: Arc<Vec<tokio::sync::Mutex<SyncClient>>>,
) -> Result<(Vec<JoinHandle<()>>, Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>)> {
    info!("Starting cold start process");
    counter!("cold_start_attempts_total").increment(1);
    let cs_start = Instant::now();

    let bootstrap = bootstrap::bootstrap(http_client).await?;
    let mut handles = bootstrap.handles;
    let db_chain_ref = bootstrap.db_chain;
    let last_slot = bootstrap.last_slot;

    let num_transactions = Arc::new(AtomicU64::new(0));
    let current_slot = Arc::new(AtomicU64::new(last_slot));
    let seen_shreds: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    let staged_batches: Arc<DashMap<u64, BTreeMap<u64, SerializableBatch>>> = Arc::new(DashMap::new());
    let finalized_slots: Arc<DashSet<u64>> = Arc::new(DashSet::new());
    let finalized_timestamps: Arc<DashMap<u64, u64>> = Arc::new(DashMap::new());
    let finalized_job_ids = Arc::new(RwLock::new(HashMap::<u64, Vec<u64>>::new()));
    let blockhash_to_signatures = Arc::new(RwLock::new(HashMap::<Hash, Vec<Signature>>::new()));
    let pending_batches: Arc<DashMap<(u64, u64), SerializableBatch>> = Arc::new(DashMap::new());

    let tx_handles = tx::spawn_tx_processors(tx::TxProcessorConfig {
        receivers: tx_receivers,
        db_chain: db_chain_ref.clone(),
        indexer: indexer.clone(),
        bank: bank.clone(),
        subscription_processor: subscription_processor.clone(),
        num_transactions: num_transactions.clone(),
        seen_shreds: seen_shreds.clone(),
        staged_batches: staged_batches.clone(),
        finalized_slots: finalized_slots.clone(),
        finalized_timestamps: finalized_timestamps.clone(),
        finalized_job_ids: finalized_job_ids.clone(),
        refetch_pool: refetch_pool.clone(),
        blockhash_to_signatures: blockhash_to_signatures.clone(),
        current_slot: current_slot.clone(),
        pending_batches: pending_batches.clone(),
    });
    handles.extend(tx_handles);

    let prune_handle = slot::spawn_prune_task(
        seen_shreds.clone(),
        finalized_slots.clone(),
        finalized_timestamps.clone(),
        staged_batches.clone(),
        finalized_job_ids.clone(),
        current_slot.clone(),
        pending_batches.clone(),
    );
    handles.push(tokio::spawn(async move {
        prune_handle.await.expect("Prune task exited with error")
    }));

    histogram!("cold_start_total_ms").record(cs_start.elapsed().as_secs_f64() * 1000.0);
    counter!("cold_start_completed_total").increment(1);
    info!("Cold start completed successfully");

    Ok((handles, db_chain_ref))
}
