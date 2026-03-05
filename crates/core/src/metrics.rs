use std::sync::atomic::{AtomicU64, Ordering};

use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

#[derive(Debug, Default)]
pub struct BankMetrics {
    // check_results
    total_tx_checked: AtomicU64,
    total_tx_expired: AtomicU64,
    total_tx_duplicate: AtomicU64,
    parent_blockhash_missing: AtomicU64,
    parent_blockhash_missing_last_slot: AtomicU64,
}

// =============================== SEQUENCER_METHODS
#[derive(Debug, Default)]
pub struct SchedulerMetrics {
    // Transaction counts
    total_tx_received: AtomicU64,
    total_bench_tx_received: AtomicU64,
    total_tx_scheduled: AtomicU64,
    total_tx_unschedulable: AtomicU64,
    total_tx_completed: AtomicU64,
    total_tx_sent: AtomicU64,
    total_tx_early_expired_receiving: AtomicU64,
    total_bench_tx_early_expired_receiving: AtomicU64,
    total_tx_early_expired_scheduling: AtomicU64,
    // Batch metrics
    total_batches_sent: AtomicU64,
    total_empty_batches: AtomicU64,

    // Queue metrics
    current_container_size: AtomicU64,
    current_channel_len: AtomicU64,
}

// =============================== END_SEQUENCER_METHODS

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

// =============================== SEQUENCER_METHODS
impl SchedulerMetrics {
    pub fn increase_total_tx_received(&self, count: u64) {
        self.total_tx_received.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increase_total_bench_tx_received(&self, count: u64) {
        self.total_bench_tx_received.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increase_total_tx_scheduled(&self, count: u64) {
        self.total_tx_scheduled.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increase_total_tx_unschedulable(&self, count: u64) {
        self.total_tx_unschedulable.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increase_total_tx_completed(&self, count: u64) {
        self.total_tx_completed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_total_batches_sent(&self) {
        self.total_batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_total_empty_batches(&self) {
        self.total_empty_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increase_total_tx_sent(&self, count: u64) {
        self.total_tx_sent.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increase_total_tx_early_expired_receiving(&self, count: u64) {
        self.total_tx_early_expired_receiving
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increase_total_bench_tx_early_expired_receiving(&self, count: u64) {
        self.total_bench_tx_early_expired_receiving
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increase_total_tx_early_expired_scheduling(&self, count: u64) {
        self.total_tx_early_expired_scheduling
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn set_current_container_size(&self, size: u64) {
        self.current_container_size.store(size, Ordering::Relaxed);
    }

    pub fn set_current_channel_len(&self, len: u64) {
        self.current_channel_len.store(len, Ordering::Relaxed);
    }

    pub fn report(&self) {
        // Total received transactions (from channel)
        counter!("scheduler_transactions_total", "phase" => "received", "type" => "normal")
            .absolute(self.total_tx_received.load(Ordering::Relaxed));

        // Total received benchmark transactions (from channel)
        counter!("scheduler_transactions_total", "phase" => "received", "type" => "bench")
            .absolute(self.total_bench_tx_received.load(Ordering::Relaxed));

        // Total scheduled transactions (to workers)
        counter!("scheduler_transactions_total", "phase" => "scheduled")
            .absolute(self.total_tx_scheduled.load(Ordering::Relaxed));

        // Total unschedulable transactions (due to account lock)
        counter!("scheduler_transactions_total", "phase" => "unschedulable")
            .absolute(self.total_tx_unschedulable.load(Ordering::Relaxed));

        // Total early expired transactions (due to time)
        counter!("scheduler_transactions_total", "phase" => "early_expired_receiving", "type" => "normal")
            .absolute(self.total_tx_early_expired_receiving.load(Ordering::Relaxed));

        // Total early expired benchmark transactions (due to time)
        counter!("scheduler_transactions_total", "phase" => "early_expired_receiving", "type" => "bench")
            .absolute(self.total_bench_tx_early_expired_receiving.load(Ordering::Relaxed));

        counter!("scheduler_transactions_total", "phase" => "early_expired_scheduling")
            .absolute(self.total_tx_early_expired_scheduling.load(Ordering::Relaxed));

        // Total sent transactions (to workers)
        counter!("scheduler_transactions_total", "phase" => "sent")
            .absolute(self.total_tx_sent.load(Ordering::Relaxed));

        // Total completed transactions (callback from workers)
        counter!("scheduler_transactions_total", "phase" => "completed")
            .absolute(self.total_tx_completed.load(Ordering::Relaxed));

        // Total batches sent (to workers)
        counter!("scheduler_batches_total", "kind" => "sent").absolute(self.total_batches_sent.load(Ordering::Relaxed));

        // Total empty batches (due to no transactions)
        counter!("scheduler_batches_total", "kind" => "empty")
            .absolute(self.total_empty_batches.load(Ordering::Relaxed));

        // Current tx buffer size (already received; maybe scheduled, not yet processed)
        gauge!("scheduler_queue_items", "queue" => "container")
            .set(self.current_container_size.load(Ordering::Relaxed) as f64);

        // Current receiving channel length
        gauge!("scheduler_queue_items", "queue" => "channel")
            .set(self.current_channel_len.load(Ordering::Relaxed) as f64);
    }
}

// =============================== END_SEQUENCER_METHODS

#[derive(Debug, Default)]
pub struct QuicTxReceiverMetrics {
    active_connections: AtomicU64,
    total_tx_received: AtomicU64,
}

impl QuicTxReceiverMetrics {
    pub fn increase_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrease_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn increase_total_tx_received(&self, count: u64) {
        self.total_tx_received.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_tx_size_bytes(&self, size_bytes: u64) {
        histogram!("quic_tx_receiver_transaction_size_bytes").record(size_bytes as f64);
    }

    pub fn report(&self) {
        gauge!("quic_tx_receiver_active_connections").set(self.active_connections.load(Ordering::Relaxed) as f64);
        counter!("quic_tx_receiver_transactions_total").absolute(self.total_tx_received.load(Ordering::Relaxed));
    }
}

/// WorkerMetrics tracks metrics related to the worker component.
pub struct WorkerMetrics {
    /// Histogram for sysvar update time
    pub sysvar_time: Histogram,

    /// Histogram for transaction check time
    pub check_time: Histogram,

    /// Histogram for transaction execution time
    pub execute_time: Histogram,

    /// Histogram for prepare commit time
    pub prepare_commit_time: Histogram,

    /// Histogram for send time
    pub send_time: Histogram,

    /// Histogram for total loop time
    pub total_loop_time: Histogram,

    /// Counter for transactions executed
    pub transactions_executed: Counter,

    /// Gauge for current job queue length
    pub job_queue_length: Gauge,

    /// Gauge for transactions per second
    pub transactions_per_second: Gauge,
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self {
            sysvar_time: histogram!("worker_loop_seconds", "phase" => "sysvar"),
            check_time: histogram!("worker_loop_seconds", "phase" => "check"),
            execute_time: histogram!("worker_loop_seconds", "phase" => "execute"),
            prepare_commit_time: histogram!("worker_loop_seconds", "phase" => "prepare_commit"),
            send_time: histogram!("worker_loop_seconds", "phase" => "send"),
            total_loop_time: histogram!("worker_loop_seconds", "phase" => "total"),
            transactions_executed: counter!("worker_loop_transactions_executed_total"),
            job_queue_length: gauge!("worker_loop_job_queue_length"),
            transactions_per_second: gauge!("worker_loop_transactions_per_second"),
        }
    }
}

impl WorkerMetrics {
    pub fn record_sysvar_time(&self, duration: std::time::Duration) {
        self.sysvar_time.record(duration.as_secs_f64());
    }

    pub fn record_check_time(&self, duration: std::time::Duration) {
        self.check_time.record(duration.as_secs_f64());
    }

    pub fn record_execute_time(&self, duration: std::time::Duration) {
        self.execute_time.record(duration.as_secs_f64());
    }

    pub fn record_prepare_commit_time(&self, duration: std::time::Duration) {
        self.prepare_commit_time.record(duration.as_secs_f64());
    }

    pub fn record_send_time(&self, duration: std::time::Duration) {
        self.send_time.record(duration.as_secs_f64());
    }

    pub fn record_total_loop_time(&self, duration: std::time::Duration) {
        self.total_loop_time.record(duration.as_secs_f64());
    }

    pub fn increment_transactions_executed(&self, count: u64) {
        self.transactions_executed.increment(count);
    }

    pub fn set_job_queue_length(&self, length: usize) {
        self.job_queue_length.set(length as f64);
    }

    pub fn set_transactions_per_second(&self, tps: f64) {
        self.transactions_per_second.set(tps);
    }
}
