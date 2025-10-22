use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

/// Metrics for DatabaseIndexer operations
pub struct DatabaseIndexerMetrics {
    // Counter metrics
    pub counter_transactions_indexed: Counter,
    pub counter_signatures_indexed: Counter,
    pub counter_blocks_indexed: Counter,
    pub counter_tx_cache_flushes: Counter,
    pub counter_signature_cache_flushes: Counter,
    pub counter_account_ops_cache_flushes: Counter,

    // Gauge metrics
    pub gauge_slot_processing_seconds: Gauge,
    pub gauge_last_slot_timestamp_seconds: Gauge,
    pub gauge_cache_size_transactions: Gauge,
    pub gauge_cache_size_signatures: Gauge,
    pub gauge_cache_size_account_ops_create: Gauge,
    pub gauge_cache_size_account_ops_delete: Gauge,
    pub gauge_cache_size_account_ops_mint_create: Gauge,
    pub gauge_cache_size_account_ops_mint_delete: Gauge,
    pub gauge_pusher_sender_channel_len: Gauge,

    // Latency metrics
    pub histogram_get_slot: Histogram,
    pub histogram_get_transaction_by_signature: Histogram,
    pub histogram_get_transactions_by_slot: Histogram,
    pub histogram_get_signatures_by_account: Histogram,
    pub histogram_get_token_accounts_owned_by_account: Histogram,
    pub histogram_get_accounts_owned_by_account: Histogram,

    pub _local_tx_cache_flushes: Arc<AtomicU64>,
    pub _local_signature_cache_flushes: Arc<AtomicU64>,

    // New gauge metrics
    pub gauge_capacity_transactions: Gauge,
    pub gauge_capacity_signatures: Gauge,
    pub gauge_capacity_account_ops_create: Gauge,
    pub gauge_capacity_account_ops_delete: Gauge,
    pub gauge_capacity_account_ops_mint_create: Gauge,
    pub gauge_capacity_account_ops_mint_delete: Gauge,
}

impl Default for DatabaseIndexerMetrics {
    fn default() -> Self {
        let latency_metric_name = "indexer_latency_ms";
        Self {
            // Initialize counters
            counter_transactions_indexed: counter!("indexer_transactions_total", "type" => "indexed"),
            counter_signatures_indexed: counter!("indexer_signatures_total", "type" => "indexed"),
            counter_blocks_indexed: counter!("indexer_blocks_total", "type" => "indexed"),
            counter_tx_cache_flushes: counter!("indexer_cache_flushes_total", "type" => "transactions"),
            counter_signature_cache_flushes: counter!("indexer_cache_flushes_total", "type" => "signatures"),
            counter_account_ops_cache_flushes: counter!("indexer_cache_flushes_total", "type" => "account_ops"),

            // Initialize gauges
            gauge_slot_processing_seconds: gauge!("indexer_slot_processing_seconds"),
            gauge_last_slot_timestamp_seconds: gauge!("indexer_last_slot_timestamp_seconds"),
            gauge_cache_size_transactions: gauge!("indexer_cache_size", "type" => "transactions"),
            gauge_cache_size_signatures: gauge!("indexer_cache_size", "type" => "signatures"),
            gauge_cache_size_account_ops_create: gauge!("indexer_cache_size", "type" => "account_ops_create"),
            gauge_cache_size_account_ops_delete: gauge!("indexer_cache_size", "type" => "account_ops_delete"),
            gauge_cache_size_account_ops_mint_create: gauge!("indexer_cache_size", "type" => "account_ops_mint_create"),
            gauge_cache_size_account_ops_mint_delete: gauge!("indexer_cache_size", "type" => "account_ops_mint_delete"),
            gauge_pusher_sender_channel_len: gauge!("indexer_channel_len", "type" => "pusher_sender"),

            // Initialize histograms
            histogram_get_slot: histogram!(latency_metric_name, "op" => "get_slot"),
            histogram_get_transaction_by_signature: histogram!(latency_metric_name, "op" => "get_transaction_by_signature"),
            histogram_get_transactions_by_slot: histogram!(latency_metric_name, "op" => "get_transaction_by_slot"),
            histogram_get_signatures_by_account: histogram!(latency_metric_name, "op" => "get_signatures_by_account"),
            histogram_get_token_accounts_owned_by_account: histogram!(latency_metric_name, "op" => "get_token_accounts_owned_by_account"),
            histogram_get_accounts_owned_by_account: histogram!(latency_metric_name, "op" => "get_accounts_owned_by_account"),

            _local_tx_cache_flushes: Arc::new(AtomicU64::new(0)),
            _local_signature_cache_flushes: Arc::new(AtomicU64::new(0)),

            // New gauge metrics
            gauge_capacity_transactions: gauge!("indexer_cache_capacity", "type" => "transactions"),
            gauge_capacity_signatures: gauge!("indexer_cache_capacity", "type" => "signatures"),
            gauge_capacity_account_ops_create: gauge!("indexer_cache_capacity", "type" => "account_ops_create"),
            gauge_capacity_account_ops_delete: gauge!("indexer_cache_capacity", "type" => "account_ops_delete"),
            gauge_capacity_account_ops_mint_create: gauge!("indexer_cache_capacity", "type" => "account_ops_mint_create"),
            gauge_capacity_account_ops_mint_delete: gauge!("indexer_cache_capacity", "type" => "account_ops_mint_delete"),
        }
    }
}

impl DatabaseIndexerMetrics {
    pub fn report_tx_indexed(&self, count: u64) {
        self.counter_transactions_indexed.increment(count);
        self._local_tx_cache_flushes.fetch_add(count, Ordering::Relaxed);
    }

    pub fn report_signatures_indexed(&self, count: u64) {
        self.counter_signatures_indexed.increment(count);
        self._local_signature_cache_flushes.fetch_add(count, Ordering::Relaxed);
    }

    pub fn report_blocks_indexed(&self) {
        self.counter_blocks_indexed.increment(1);
    }

    pub fn report_tx_cache_flush(&self) {
        self.counter_tx_cache_flushes.increment(1);
    }

    pub fn report_signature_cache_flush(&self) {
        self.counter_signature_cache_flushes.increment(1);
    }

    pub fn report_account_ops_cache_flush(&self) {
        self.counter_account_ops_cache_flushes.increment(1);
    }

    pub fn report_db_error(&self, operation: &str) {
        let labeled_counter = counter!("indexer_errors_total", "type" => "db", "operation" => operation.to_string());
        labeled_counter.increment(1);
    }

    pub fn report_slot_processing_time(&self, duration_secs: f64) {
        self.gauge_slot_processing_seconds.set(duration_secs);
    }

    pub fn report_last_slot_processed_timestamp(&self, timestamp_secs: u64) {
        self.gauge_last_slot_timestamp_seconds.set(timestamp_secs as f64);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn report_cache_sizes(
        &self,
        tx_cache_len: usize,
        signature_cache_len: usize,
        account_ops_create_len: usize,
        account_ops_delete_len: usize,
        account_ops_mint_create_len: usize,
        account_ops_mint_delete_len: usize,
        pusher_sender_len: usize,
        tx_cache_capacity: usize,
        signature_cache_capacity: usize,
        account_ops_create_capacity: usize,
        account_ops_delete_capacity: usize,
        account_ops_mint_create_capacity: usize,
        account_ops_mint_delete_capacity: usize,
    ) {
        self.gauge_cache_size_transactions.set(tx_cache_len as f64);
        self.gauge_cache_size_signatures.set(signature_cache_len as f64);
        self.gauge_cache_size_account_ops_create
            .set(account_ops_create_len as f64);
        self.gauge_cache_size_account_ops_delete
            .set(account_ops_delete_len as f64);
        self.gauge_cache_size_account_ops_mint_create
            .set(account_ops_mint_create_len as f64);
        self.gauge_cache_size_account_ops_mint_delete
            .set(account_ops_mint_delete_len as f64);
        self.gauge_pusher_sender_channel_len.set(pusher_sender_len as f64);
        self.gauge_capacity_transactions.set(tx_cache_capacity as f64);
        self.gauge_capacity_signatures.set(signature_cache_capacity as f64);
        self.gauge_capacity_account_ops_create
            .set(account_ops_create_capacity as f64);
        self.gauge_capacity_account_ops_delete
            .set(account_ops_delete_capacity as f64);
        self.gauge_capacity_account_ops_mint_create
            .set(account_ops_mint_create_capacity as f64);
        self.gauge_capacity_account_ops_mint_delete
            .set(account_ops_mint_delete_capacity as f64);
    }
}

/// Metrics for MultiDatabaseIndexer operations
#[derive(Clone)]
pub struct MultiDatabaseIndexerMetrics {
    // Counter metrics
    pub counter_commands_dispatched: Counter,
    pub counter_s3_slot_info_write: Counter,
    pub counter_s3_tx_shard_write: Counter,

    // Gauge metrics
    pub gauge_processed_transactions_total: Gauge,
    pub gauge_cache_size_slot: Gauge,
    pub gauge_cache_size_next_slot: Gauge,
    pub gauge_capacity_slot: Gauge,
    pub gauge_capacity_next_slot: Gauge,
}

impl Default for MultiDatabaseIndexerMetrics {
    fn default() -> Self {
        Self {
            // Initialize counters
            counter_commands_dispatched: counter!("indexer_commands_total", "type" => "dispatched"),
            counter_s3_slot_info_write: counter!("indexer_s3_operations_total", "type" => "slot_info_write"),
            counter_s3_tx_shard_write: counter!("indexer_s3_operations_total", "type" => "tx_shard_write"),

            // Initialize gauges
            gauge_processed_transactions_total: gauge!("indexer_processed_transactions_total"),
            gauge_cache_size_slot: gauge!("indexer_cache_size", "type" => "slot"),
            gauge_cache_size_next_slot: gauge!("indexer_cache_size", "type" => "next_slot"),
            gauge_capacity_slot: gauge!("indexer_cache_capacity", "type" => "slot"),
            gauge_capacity_next_slot: gauge!("indexer_cache_capacity", "type" => "next_slot"),
        }
    }
}

impl MultiDatabaseIndexerMetrics {
    pub fn report_commands_dispatched(&self) {
        self.counter_commands_dispatched.absolute(1);
    }

    pub fn report_total_processed(&self, count: u64) {
        self.gauge_processed_transactions_total.set(count as f64);
    }

    pub fn report_s3_slot_info_write(&self) {
        self.counter_s3_slot_info_write.absolute(1);
    }

    pub fn report_s3_tx_shard_write(&self, shard_idx: usize) {
        self.counter_s3_tx_shard_write.absolute(shard_idx as u64);
    }

    pub fn report_cache_sizes(
        &self,
        slot_cache_len: usize,
        next_slot_cache_len: usize,
        slot_cache_capacity: usize,
        next_slot_cache_capacity: usize,
    ) {
        self.gauge_cache_size_slot.set(slot_cache_len as f64);
        self.gauge_cache_size_next_slot.set(next_slot_cache_len as f64);
        self.gauge_capacity_slot.set(slot_cache_capacity as f64);
        self.gauge_capacity_next_slot.set(next_slot_cache_capacity as f64);
    }
}
