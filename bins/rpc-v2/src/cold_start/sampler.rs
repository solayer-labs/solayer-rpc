use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use infinisvm_core::committer::PerfSample;
use infinisvm_logger::info;
use metrics::gauge;
use tokio::task::JoinHandle;

pub(super) fn spawn_sampler(
    samples: Arc<RwLock<(Instant, VecDeque<PerfSample>)>>,
    total_transaction_count: Arc<AtomicU64>,
    num_transactions: Arc<AtomicU64>,
    num_slots: Arc<AtomicU64>,
    current_slot: Arc<AtomicU64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Sampler task started");
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            {
                let mut samples = samples.write().unwrap();
                let duration = samples.0.elapsed().as_secs();
                samples.0 = Instant::now();
                let num_slots_val = num_slots.load(Ordering::SeqCst);
                let num_transactions_val = num_transactions.load(Ordering::SeqCst);
                let cur_slot = current_slot.load(Ordering::SeqCst);
                samples
                    .1
                    .push_back((cur_slot, num_transactions_val, num_slots_val, duration));
                num_slots.store(0, Ordering::SeqCst);
                num_transactions.store(0, Ordering::SeqCst);

                if samples.1.len() > 720 {
                    samples.1.pop_front();
                }

                total_transaction_count.fetch_add(num_transactions_val, Ordering::SeqCst);

                let secs = duration.max(1) as f64;
                let tps = (num_transactions_val as f64) / secs;
                let sps = (num_slots_val as f64) / secs;
                gauge!("window_tps").set(tps);
                gauge!("window_sps").set(sps);
            }
        }
    })
}
