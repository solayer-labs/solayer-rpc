use std::sync::atomic::{AtomicU64, Ordering};

use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

#[derive(Debug, Default)]
pub struct BankMetrics {
    // check_results
    total_tx_checked: AtomicU64,
    total_tx_expired: AtomicU64,
    total_tx_duplicate: AtomicU64,
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
    }
}


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

/// PusherMetrics tracks metrics related to the QUIC pusher component.
pub struct PusherMetrics {
    /// Number of active QUIC connections
    pub active_connections: Gauge,

    /// Bytes queued for sending to each connection
    pub bytes_queued: Counter,

    /// Total bytes sent over QUIC connections
    pub bytes_sent: Counter,
}

impl Default for PusherMetrics {
    fn default() -> Self {
        Self {
            active_connections: gauge!("pusher_active_connections"),
            bytes_queued: counter!("pusher_queued_bytes"),
            bytes_sent: counter!("pusher_sent_bytes"),
        }
    }
}

impl PusherMetrics {
    pub fn increase_active_connections(&self) {
        self.active_connections.increment(1);
    }

    pub fn decrease_active_connections(&self) {
        self.active_connections.decrement(1);
    }

    pub fn increase_bytes_queued(&self, bytes: u64) {
        self.bytes_queued.increment(bytes);
    }

    pub fn increase_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.increment(bytes);
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
