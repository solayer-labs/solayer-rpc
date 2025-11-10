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
            }
        }
    }
}
