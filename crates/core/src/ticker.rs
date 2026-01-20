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
        let period = Duration::from_millis(400);
        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::after(period).recv().ok();

            if exit.load(Ordering::Relaxed) {
                break;
            }

            self.bank.write().unwrap().tick();
        }
    }
}
