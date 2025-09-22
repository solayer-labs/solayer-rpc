use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use crate::bank::Bank;

pub struct Ticker {
    bank: Arc<RwLock<Bank>>,
}

impl Ticker {
    pub fn new(bank: Arc<RwLock<Bank>>) -> Self {
        Self { bank }
    }

    pub fn run_loop(&mut self, exit: Arc<AtomicBool>) {
        let crossbeam_ticker = crossbeam_channel::tick(Duration::from_millis(400));
        while !exit.load(Ordering::Relaxed) {
            if crossbeam_ticker.recv().is_ok() {
                self.bank.write().unwrap().tick();

                #[cfg(feature = "track_memory")]
                {
                    infinisvm_logger::info!("{:?} memory usage: {} MB", std::thread::current().name(), unsafe {
                        crate::get_memory_usage() as f64 / 1024.0 / 1024.0
                    });
                }
            }
        }
    }
}
