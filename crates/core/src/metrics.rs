use std::sync::atomic::{AtomicU64, Ordering};

use metrics::counter;

#[derive(Debug, Default)]
pub struct BankMetrics {
    // check_results
    total_tx_checked: AtomicU64,
    total_tx_expired: AtomicU64,
    total_tx_duplicate: AtomicU64,
    parent_blockhash_missing: AtomicU64,
    parent_blockhash_missing_last_slot: AtomicU64,
}


impl BankMetrics {
    pub fn increase_total_tx_checked(&self, count: u64) {
        self.total_tx_checked.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_total_tx_expired(&self) {
        self.total_tx_expired.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_total_tx_duplicate(&self) {
        self.total_tx_duplicate.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_parent_blockhash_missing(&self) {
        self.parent_blockhash_missing.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_parent_blockhash_missing(&self, slot: u64) -> bool {
        let previous_slot = self.parent_blockhash_missing_last_slot.swap(slot, Ordering::Relaxed);
        if previous_slot == slot {
            return false;
        }

        self.increment_parent_blockhash_missing();
        true
    }

    pub fn report(&self) {
        // Total transactions checked (for expired, duplicate, etc.)
        counter!("bank_transactions_total", "result" => "checked")
            .absolute(self.total_tx_checked.load(Ordering::Relaxed));

        // Total transactions expired (due to time)
        counter!("bank_transactions_total", "result" => "expired")
            .absolute(self.total_tx_expired.load(Ordering::Relaxed));

        // Total transactions duplicate (due to signature)
        counter!("bank_transactions_total", "result" => "duplicate")
            .absolute(self.total_tx_duplicate.load(Ordering::Relaxed));

        counter!("bank_parent_blockhash_lookup_total", "result" => "miss")
            .absolute(self.parent_blockhash_missing.load(Ordering::Relaxed));
    }
}



