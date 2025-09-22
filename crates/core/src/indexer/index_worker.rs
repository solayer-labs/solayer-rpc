use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use crossbeam_channel::Receiver;
use solana_hash::Hash;

use crate::indexer::Indexer;

pub struct IndexerWorker {
    indexer: Arc<RwLock<dyn Indexer>>,
    receiver: Receiver<(u64, u64, Hash, Hash)>, // slot, timestamp, blockhash, parent_blockhash
}

impl IndexerWorker {
    pub fn new(indexer: Arc<RwLock<dyn Indexer>>, receiver: Receiver<(u64, u64, Hash, Hash)>) -> Self {
        Self { indexer, receiver }
    }

    pub fn run_loop(&mut self, exit: Arc<AtomicBool>) {
        while !exit.load(Ordering::Relaxed) {
            if let Ok((slot, timestamp, blockhash, parent_blockhash)) = self.receiver.recv() {
                self.indexer
                    .write()
                    .unwrap()
                    .index_block(slot, timestamp, blockhash, parent_blockhash);
            }
        }

        println!("flushing indexer");
        self.indexer.write().unwrap().flush();
    }
}
