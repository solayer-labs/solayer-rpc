use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use infinisvm_logger::{error, info};

use crate::persistence::PersistedInMemoryDB;

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
