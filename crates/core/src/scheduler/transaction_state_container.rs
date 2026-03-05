use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use itertools::MinMaxResult;
use min_max_heap::MinMaxHeap;
use solana_sdk::transaction::SanitizedTransaction;

use crate::scheduler::{transaction_id::TransactionId, transaction_priority_id::TransactionPriorityId};

#[allow(clippy::large_enum_variant)]
pub(crate) enum TransactionState {
    /// The transaction is available for scheduling.
    Unprocessed {
        transaction: SanitizedTransaction,
        priority: u64,
    },
    /// The transaction is currently scheduled or being processed.
    Pending { priority: u64 },
    /// Only used during transition.
    Transitioning,
}

impl TransactionState {
    pub fn new(transaction: SanitizedTransaction, priority: u64) -> Self {
        Self::Unprocessed { transaction, priority }
    }

    pub fn transition_to_unprocessed(&mut self, transaction: SanitizedTransaction) {
        // only from pending to unprocessed
        match self.take() {
            TransactionState::Pending { priority } => {
                *self = TransactionState::Unprocessed { transaction, priority };
            }
            _ => panic!("invalid transition"),
        }
    }

    pub fn transition_to_pending(&mut self) -> SanitizedTransaction {
        match self.take() {
            TransactionState::Unprocessed { transaction, priority } => {
                *self = TransactionState::Pending { priority };
                transaction
            }
            _ => unreachable!(),
        }
    }

    fn take(&mut self) -> Self {
        core::mem::replace(self, Self::Transitioning)
    }

    pub fn priority(&self) -> u64 {
        match self {
            TransactionState::Unprocessed { priority, .. } => *priority,
            TransactionState::Pending { priority, .. } => *priority,
            TransactionState::Transitioning => panic!("transaction is transitioning"),
        }
    }

    pub(crate) fn transaction(&self) -> Option<&SanitizedTransaction> {
        match self {
            TransactionState::Unprocessed { transaction, .. } => Some(transaction),
            TransactionState::Pending { .. } => None,
            TransactionState::Transitioning => None,
        }
    }
}

pub(crate) struct TransactionStateContainer {
    priority_queue: MinMaxHeap<TransactionPriorityId>,
    pub id_to_transaction_state: DashMap<TransactionId, TransactionState>,
}

impl TransactionStateContainer {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            priority_queue: MinMaxHeap::with_capacity(capacity),
            id_to_transaction_state: DashMap::with_capacity(capacity),
        }
    }

    /// Returns true if the queue is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.priority_queue.is_empty()
    }

    /// Returns the remaining capacity of the queue
    pub(crate) fn remaining_queue_capacity(&self) -> usize {
        self.priority_queue.capacity() - self.priority_queue.len()
    }

    /// Get the top transaction id in the priority queue.
    pub(crate) fn pop(&mut self) -> Option<TransactionPriorityId> {
        self.priority_queue.pop_max()
    }

    pub(crate) fn len(&self) -> usize {
        self.priority_queue.len()
    }

    /// Get mutable transaction state by id.
    pub(crate) fn get_mut_transaction_state(
        &mut self,
        id: &TransactionId,
    ) -> Option<RefMut<'_, TransactionId, TransactionState>> {
        self.id_to_transaction_state.get_mut(id)
    }

    pub(crate) fn get_transaction(&self, id: &TransactionId) -> Option<Ref<'_, TransactionId, TransactionState>> {
        self.id_to_transaction_state.get(id)
    }

    /// Insert a new transaction into the container's queues and maps.
    /// Returns `true` if a packet was dropped due to capacity limits.
    pub(crate) fn insert_new_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction: SanitizedTransaction,
        priority: u64,
        timestamp: u64,
    ) -> bool {
        let priority_id = TransactionPriorityId::new(priority, transaction_id, timestamp);
        self.id_to_transaction_state
            .insert(transaction_id, TransactionState::new(transaction, priority));
        self.push_id_into_queue(priority_id)
    }

    /// Retries a transaction - inserts transaction back into map (but not
    /// packet). This transitions the transaction to `Unprocessed` state.
    pub(crate) fn retry_transaction(
        &mut self,
        transaction_id: TransactionId,
        transaction: SanitizedTransaction,
        timestamp: u64,
    ) {
        let priority_id = {
            let mut transaction_state = self
                .get_mut_transaction_state(&transaction_id)
                .expect("transaction must exist");
            let priority_id = TransactionPriorityId::new(transaction_state.priority(), transaction_id, timestamp);
            transaction_state.value_mut().transition_to_unprocessed(transaction);
            priority_id
        };
        self.push_id_into_queue(priority_id);
    }

    /// Pushes a transaction id into the priority queue. If the queue is full,
    /// the lowest priority transaction will be dropped (removed from the
    /// queue and map). Returns `true` if a packet was dropped due to
    /// capacity limits.
    pub(crate) fn push_id_into_queue(&mut self, priority_id: TransactionPriorityId) -> bool {
        if self.remaining_queue_capacity() == 0 {
            let popped_id = self.priority_queue.push_pop_min(priority_id);
            self.remove_by_id(&popped_id.id);
            true
        } else {
            self.priority_queue.push(priority_id);
            false
        }
    }

    /// Remove transaction by id.
    pub(crate) fn remove_by_id(&mut self, id: &TransactionId) {
        self.id_to_transaction_state.remove(id).expect("transaction must exist");
    }

    pub(crate) fn shrink_to_fit(&mut self) {
        self.id_to_transaction_state.shrink_to_fit();
    }

    pub(crate) fn get_min_max_priority(&self) -> MinMaxResult<u64> {
        match self.priority_queue.peek_min() {
            Some(min) => match self.priority_queue.peek_max() {
                Some(max) => MinMaxResult::MinMax(min.priority, max.priority),
                None => MinMaxResult::OneElement(min.priority),
            },
            None => MinMaxResult::NoElements,
        }
    }
}
