mod backfill;
mod bootstrap;
pub mod slots_sync_progress;
mod tx;
mod utils;

use std::{
    sync::{atomic::AtomicU64, Arc, RwLock},
    time::Instant,
};

use dashmap::{DashMap, DashSet};
use eyre::Result;
use hashbrown::HashMap;
use infinisvm_core::{bank::Bank, indexer::Indexer, s3::S3FsClient, subscription::SubscriptionProcessor};
use infinisvm_db::{db_chain::DBChain, in_memory_db::NoopDB, MemoryDB};
use infinisvm_logger::info;
use infinisvm_sync::http_client::HttpClient;
use metrics::{counter, histogram};
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};

use self::backfill::BackfillManager;
use crate::{
    cold_start::{slots_sync_progress::SlotsSyncProgressRecorder, tx::spawn_tx_processors},
    p2p::{PeerManager, PeerNotification},
};

#[derive(Clone, Debug)]
pub enum StartSlot {
    Latest,
    Checkpoint,
    Slot(u64),
}

impl std::str::FromStr for StartSlot {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(Self::Latest),
            "checkpoint" => Ok(Self::Checkpoint),
            _ => s
                .parse::<u64>()
                .map(Self::Slot)
                .map_err(|_| format!("Invalid start_slot '{s}'. Expected 'latest', 'checkpoint', or a u64 value")),
        }
    }
}

impl std::fmt::Display for StartSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Latest => write!(f, "latest"),
            Self::Checkpoint => write!(f, "checkpoint"),
            Self::Slot(slot) => write!(f, "{slot}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn cold_start(
    http_client: Arc<HttpClient>,
    tx_receiver: mpsc::Receiver<PeerNotification>,
    indexer: Arc<Mutex<dyn Indexer>>,
    bank: Arc<RwLock<Bank>>,
    subscription_processor: Arc<SubscriptionProcessor>,
    peer_manager: Arc<PeerManager>,
    progress_recorder: Option<SlotsSyncProgressRecorder>,
    s3_client: Option<S3FsClient>,
    sequencer_pubkey: Pubkey,
    start_slot: StartSlot,
) -> Result<(Vec<JoinHandle<()>>, Arc<RwLock<DBChain<MemoryDB<NoopDB>>>>)> {
    info!("Starting cold start process");
    counter!("cold_start_attempts_total").increment(1);
    let cs_start = Instant::now();

    let bootstrap = bootstrap::bootstrap(http_client.clone()).await?;
    let mut handles = bootstrap.handles;
    let db_chain_ref = bootstrap.db_chain;
    let last_slot_from_bootstrap = bootstrap.last_slot;

    let backfill_manager = progress_recorder.as_ref().map(|recorder| {
        BackfillManager::new(
            http_client.clone(),
            recorder.clone(),
            s3_client.clone(),
            indexer.clone(),
        )
    });

    let initial_latest_slot = if let Some(manager) = backfill_manager.as_ref() {
        let latest = manager.read_latest_slot().await?;
        info!("Initial latest_slot from progress recorder: {:?}", latest);
        latest
    } else {
        None
    };

    let resolved_start_slot = match start_slot.clone() {
        StartSlot::Latest => {
            info!("start_slot=latest: skipping backfill planning");
            progress_recorder
                .as_ref()
                .map(|recorder| recorder.clear_backfill_start())
                .transpose()?;
            None
        }
        StartSlot::Slot(requested_start) => {
            info!("start_slot={} (manual)", requested_start);
            progress_recorder
                .as_ref()
                .map(|recorder| recorder.record_backfill_start(requested_start))
                .transpose()?;
            Some(requested_start)
        }
        StartSlot::Checkpoint => {
            if let Some(manager) = backfill_manager.as_ref() {
                let backfill_start = manager.read_backfill_start().await?;
                info!(
                    "Checkpoint backfill metadata: latest_slot={:?}, backfill_start={:?}",
                    initial_latest_slot, backfill_start
                );

                match (initial_latest_slot, backfill_start) {
                    (Some(latest), Some(start)) => Some(latest.min(start)),
                    (Some(latest), None) => Some(latest),
                    (None, Some(start)) => Some(start),
                    (None, None) => None,
                }
            } else {
                info!("No slots progress recorder configured; skipping checkpoint backfill");
                None
            }
        }
    };

    if let Some(slot) = resolved_start_slot {
        info!("Resolved start_slot={}", slot);
    } else {
        info!("Resolved start_slot=latest");
    }

    let current_slot = Arc::new(AtomicU64::new(last_slot_from_bootstrap));
    let seen_shreds = Arc::new(DashSet::new());
    let staged_batches = Arc::new(DashMap::new());
    let finalized_slots = Arc::new(DashSet::new());
    let finalized_timestamps = Arc::new(DashMap::new());
    let finalized_job_ids = Arc::new(RwLock::new(HashMap::<u64, usize>::new()));
    let blockhash_to_signatures = Arc::new(RwLock::new(HashMap::<Hash, Vec<Signature>>::new()));
    let pending_batches = Arc::new(DashMap::new());
    let finalizer_refetching = Arc::new(DashSet::new());
    let shred_sources = Arc::new(DashMap::new());

    let tx_handle = spawn_tx_processors(
        tx_receiver,
        db_chain_ref.clone(),
        indexer,
        bank,
        subscription_processor,
        seen_shreds,
        staged_batches,
        finalized_slots,
        finalized_timestamps,
        finalized_job_ids,
        peer_manager,
        finalizer_refetching,
        blockhash_to_signatures,
        current_slot,
        pending_batches,
        shred_sources,
        sequencer_pubkey,
        progress_recorder.clone(),
    );
    handles.extend(tx_handle);

    if let (Some(manager), Some(start_slot)) = (backfill_manager.clone(), resolved_start_slot) {
        match manager.wait_for_latest_slot_update(initial_latest_slot).await? {
            Some(target_slot) => {
                info!("Backfill target slot: {:?}", target_slot);
                if target_slot < start_slot {
                    info!(
                        "Backfill target slot {} is behind start slot {}; skipping backfill",
                        target_slot, start_slot
                    );
                } else {
                    info!("Backfill planned for slots {}-{} (inclusive)", start_slot, target_slot);
                    let manager_clone = manager.clone();
                    let progress_recorder_copy = progress_recorder.clone();
                    handles.push(tokio::spawn(async move {
                        match manager_clone.backfill_range(start_slot, target_slot).await {
                            Ok(Some(total_slots_backfilled)) => info!(
                                "Backfill of slots {}-{} completed (total slots backfilled: {})",
                                start_slot, target_slot, total_slots_backfilled
                            ),
                            Ok(None) => panic!("Backfill of slots {start_slot}-{target_slot} filled no slots"),
                            Err(e) => panic!("Backfill of slots {start_slot}-{target_slot} failed: {e}"),
                        }
                        if let Some(recorder) = progress_recorder_copy {
                            if let Err(e) = recorder.clear_backfill_start() {
                                panic!("Failed to clear backfill_start after backfill completion: {e}");
                            }
                        }
                    }));
                }
            }
            None => {
                panic!("Timed out waiting for latest slot update after spawning processors; skipping backfill");
            }
        }
    } else if resolved_start_slot.is_some() {
        panic!("Backfill requested but slots progress recorder unavailable; skipping backfill execution");
    }

    histogram!("cold_start_total_ms").record(cs_start.elapsed().as_secs_f64() * 1000.0);
    counter!("cold_start_completed_total").increment(1);
    info!("Cold start completed successfully");

    Ok((handles, db_chain_ref))
}
