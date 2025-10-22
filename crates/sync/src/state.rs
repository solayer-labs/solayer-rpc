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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_new_slot_caches_and_emits() {
        let initial = (0, vec![], vec![], 0, vec![]);
        let (mut state, receiver) = SyncState::new(initial);

        let slot_tuple = (5u64, b"hash".to_vec(), b"parent".to_vec(), 123, vec![1, 2, 3]);
        state.notify_new_slot(slot_tuple.clone());

        let received = receiver.recv().expect("slot message");
        assert_eq!(received.slot, slot_tuple.0);
        assert_eq!(received.blockhash, slot_tuple.1);
        assert_eq!(received.parent_blockhash, slot_tuple.2);
        assert_eq!(received.timestamp, slot_tuple.3);
        assert_eq!(received.job_ids, slot_tuple.4);

        let cached = state.get_slot(slot_tuple.0).expect("cached slot");
        assert!(Arc::ptr_eq(&cached, &received));
    }

    #[test]
    fn notify_new_slot_prunes_when_capacity_exceeded() {
        let initial = (0, vec![], vec![], 0, vec![]);
        let (mut state, receiver) = SyncState::new(initial);

        for slot in 0..=super::MAX_RETAINED_SLOTS as u64 + 1 {
            let tuple = (slot, vec![slot as u8], vec![], slot, vec![]);
            state.notify_new_slot(tuple);
            receiver.recv().expect("drain receiver");
        }

        assert!(state.get_slot(0).is_none());
        let last_slot = super::MAX_RETAINED_SLOTS as u64 + 1;
        assert!(state.get_slot(last_slot).is_some());
    }
}
