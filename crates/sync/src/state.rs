use std::{collections::HashMap, sync::Arc};

use infinisvm_types::sync::grpc::SlotDataResponse;

const MAX_RETAINED_SLOTS: usize = 9000;

pub struct SyncState {
    cached_slots: HashMap<u64, Arc<SlotDataResponse>>,
    max_slot: u64,
    min_slot: u64,

    latest_slot_sender: crossbeam_channel::Sender<Arc<SlotDataResponse>>,
    pub latest_slot: (u64, Vec<u8>, Vec<u8>, u64, Vec<u64>),
}

impl SyncState {
    pub fn new(
        latest_slot: (u64, Vec<u8>, Vec<u8>, u64, Vec<u64>),
    ) -> (Self, crossbeam_channel::Receiver<Arc<SlotDataResponse>>) {
        let (latest_slot_sender, latest_slot_receiver) = crossbeam_channel::bounded(128);

        let state = Self {
            cached_slots: HashMap::new(),
            max_slot: 0,
            min_slot: u64::MAX,

            latest_slot_sender,
            latest_slot,
        };

        (state, latest_slot_receiver)
    }

    pub fn get_slot(&self, slot: u64) -> Option<Arc<SlotDataResponse>> {
        self.cached_slots.get(&slot).cloned()
    }

    pub fn notify_new_slot(&mut self, slot_data: (u64, Vec<u8>, Vec<u8>, u64, Vec<u64>)) {
        let (slot, blockhash, parent_blockhash, timestamp, job_ids) = slot_data;
        while self.cached_slots.len() > MAX_RETAINED_SLOTS {
            self.cached_slots.remove(&self.min_slot);
            self.min_slot += 1;
        }

        if slot > self.max_slot {
            self.max_slot = slot;
        }
        if slot < self.min_slot {
            self.min_slot = slot;
        }

        let arc_slot: Arc<SlotDataResponse> = Arc::new(SlotDataResponse {
            slot,
            blockhash,
            parent_blockhash,
            timestamp,
            job_ids,
        });

        self.cached_slots.insert(arc_slot.slot, arc_slot.clone());
        self.latest_slot_sender.send(arc_slot).unwrap();
    }
}
