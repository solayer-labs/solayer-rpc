use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use cdrs_tokio::{
    authenticators::NoneAuthenticatorProvider,
    cluster::{
        session::{Session, SessionBuilder, TcpSessionBuilder},
        NodeTcpConfigBuilder, TcpConnectionManager,
    },
    load_balancing::RoundRobinLoadBalancingStrategy,
    transport::TransportTcp,
};
use infinisvm_core::{indexer::Indexer, s3::S3FsClient};
use infinisvm_jsonrpc::rpc_state::RpcIndexer;
use infinisvm_logger::{debug, error, info, timer::ScopedTimer};
use infinisvm_types::{
    convert::{to_signature_rows, to_tx_row, token_balance_diff_from_diffs, JobEffectDiff, TxOrdering},
    serializable::{
        AccountDataDiff, AccountMintQueryRow, AccountMintQueryRowWithoutType, AccountMintRow, AccountOwnerRow,
        ProgramAccountRow, RowTy, SerializableTxRow, SignatureRow, SingleAccountRow, SingleOrderingRow,
        SingleSignatureRow, SlotRow, TransactionExecutionDetailsSerializable, TxRow, TxRowWithTimestamp,
    },
    sync::{JobEffects, ShredId},
    BlockWithTransactions, TransactionWithMetadata,
};
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    clock::Slot,
    message::v0::LoadedAddresses,
    signature::Signature,
    transaction::VersionedTransaction,
};
use solana_transaction_status_client_types::{TransactionStatusMeta, TransactionTokenBalance};

// Removed SortedVec usage to avoid O(n) per-insert cost under contention
use crate::{
    map_inner_instructions,
    metrics::{DatabaseIndexerMetrics, MultiDatabaseIndexerMetrics},
};

const DEFAULT_CACHE_SIZE: usize = 5000;
const FILE_CHUNK_SIZE: usize = 200000;

const SLOT_TABLE_NAME: &str = "indexer_dev.slots";
const SIGNATURE_TABLE_NAME: &str = "indexer_dev.signatures";
const TX_TABLE_NAME: &str = "indexer_dev.tx";
const ACCOUNT_MINT_TABLE_NAME: &str = "indexer_dev.account_ops_mint";
const ACCOUNT_TABLE_NAME: &str = "indexer_dev.account_ops";
const PROGRAM_ACCOUNT_TABLE_NAME: &str = "indexer_dev.program_accounts";

pub struct DatabaseIndexer<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> {
    tx_client: TX,
    slot_client: SLOT,
    signature_client: SIGNATURE,
    account_client: ACCOUNT,
    // Shared runtime for all async operations
    runtime: Arc<tokio::runtime::Runtime>,
    // Cache tx
    tx_cache: Vec<TxRow>,
    // Cache account ops
    pub(crate) account_ops_create_cache: Vec<(Pubkey, Pubkey)>,
    pub(crate) account_ops_delete_cache: Vec<(Pubkey, Pubkey)>,
    pub(crate) account_ops_mint_create_cache: Vec<(Pubkey, Pubkey, u8, Pubkey)>,
    pub(crate) account_ops_mint_delete_cache: Vec<(Pubkey, Pubkey)>,
    pub(crate) program_account_create_cache: Vec<(Pubkey, Pubkey)>,
    pub(crate) program_account_delete_cache: Vec<(Pubkey, Pubkey)>,
    // Cache signature
    signature_cache: Vec<SignatureRow>,
    // slot_time
    slot_time: u64,
    // Cache size configuration
    max_cache_size: usize,

    s3: Option<S3FsClient>,

    // Metrics for DatabaseIndexer
    metrics: DatabaseIndexerMetrics,
}

#[async_trait]
pub trait IndexerDB: Send + Sync + Clone + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn insert_native_block<T: RowTy>(
        &self,
        table_name: &str,
        columns: &[&str],
        block: Vec<T>,
    ) -> Result<(), Self::Error>;
    async fn execute(&self, query: &str) -> Result<(), Self::Error>;
    async fn query_collect<T: RowTy>(&self, query: &str) -> Result<Vec<T>, Self::Error>;
    fn to_array_string(bytes: &[u8]) -> String;

    // whether the DB requires _dist append to table name
    fn require_distributed() -> bool;
}

#[derive(Clone)]
pub struct NoopIndexerDB;

#[async_trait]
impl IndexerDB for NoopIndexerDB {
    type Error = cdrs_tokio::Error;

    async fn insert_native_block<T: RowTy>(
        &self,
        _table_name: &str,
        _columns: &[&str],
        _block: Vec<T>,
    ) -> Result<(), Self::Error> {
        unreachable!()
    }

    async fn execute(&self, _query: &str) -> Result<(), Self::Error> {
        unreachable!()
    }

    async fn query_collect<T: RowTy>(&self, _query: &str) -> Result<Vec<T>, Self::Error> {
        unreachable!()
    }

    fn to_array_string(_bytes: &[u8]) -> String {
        unreachable!()
    }

    fn require_distributed() -> bool {
        unreachable!()
    }
}

type CurrentSession =
    Session<TransportTcp, TcpConnectionManager, RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>>;

#[derive(Clone)]
pub struct CassandraIndexerDB {
    client: Arc<CurrentSession>,
}

impl CassandraIndexerDB {
    pub async fn new(host: &str, port: u16) -> Self {
        let cluster_config = NodeTcpConfigBuilder::new()
            .with_contact_point(format!("{host}:{port}").into())
            .with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
            .build()
            .await
            .unwrap();
        let lb = RoundRobinLoadBalancingStrategy::new();
        let session: Arc<CurrentSession> = Arc::new(TcpSessionBuilder::new(lb, cluster_config).build().await.unwrap());
        Self { client: session }
    }

    fn build_insert_query(table_name: &str, columns: &[&str], values_batch: Vec<Vec<String>>) -> String {
        if values_batch.is_empty() {
            return String::new();
        }

        let values_clauses: Vec<String> = values_batch
            .into_iter()
            .map(|values| format!("({})", values.join(", ")))
            .collect();

        format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_name,
            columns.join(", "),
            values_clauses.join(", ")
        )
    }
}

// Lightweight aggregator for periodic per-table insert counts
struct InsertAggregator {
    last_flush: Instant,
    counters: HashMap<String, (u64, u64, u64)>, // intended, success, failed
}

static INSERT_AGGREGATOR: OnceLock<Mutex<InsertAggregator>> = OnceLock::new();

fn update_insert_aggregates(table: &str, intended: usize, success: usize, failed: usize) {
    let lock = INSERT_AGGREGATOR.get_or_init(|| {
        Mutex::new(InsertAggregator {
            last_flush: Instant::now(),
            counters: HashMap::new(),
        })
    });
    if let Ok(mut agg) = lock.lock() {
        let entry = agg.counters.entry(table.to_string()).or_insert((0u64, 0u64, 0u64));
        entry.0 += intended as u64;
        entry.1 += success as u64;
        entry.2 += failed as u64;

        // Flush roughly once per minute
        if agg.last_flush.elapsed().as_secs() >= 60 {
            for (table, (i, s, f)) in agg.counters.iter() {
                info!(
                    "cassandra_insert_agg_1m: table={} intended_rows={} success_rows={} failed_rows={}",
                    table, i, s, f
                );
            }
            agg.counters.clear();
            agg.last_flush = Instant::now();
        }
    }
}

#[async_trait]
impl IndexerDB for CassandraIndexerDB {
    type Error = cdrs_tokio::Error;

    async fn insert_native_block<T: RowTy>(
        &self,
        table_name: &str,
        columns: &[&str],
        block: Vec<T>,
    ) -> Result<(), Self::Error> {
        let intended = block.len();
        if intended == 0 {
            return Ok(());
        }

        let mut futures = Vec::with_capacity(intended);
        for item in block {
            let values = item.into_query_values();
            let query = Self::build_insert_query(table_name, columns, vec![values]);
            futures.push(self.client.query(query));
        }

        // Collect all results to count successes for logging
        let results = futures_util::future::join_all(futures).await;
        let success = results.iter().filter(|r| r.is_ok()).count();
        let failed = intended.saturating_sub(success);

        // Periodic aggregation (1-minute buckets)
        update_insert_aggregates(table_name, intended, success, failed);

        // Preserve error semantics: return first error if any failed
        if let Some(err) = results.into_iter().find_map(|r| r.err()) {
            return Err(err);
        }

        Ok(())
    }

    async fn execute(&self, query: &str) -> Result<(), Self::Error> {
        self.client.query(query).await?;
        Ok(())
    }

    async fn query_collect<T: RowTy>(&self, query: &str) -> Result<Vec<T>, Self::Error> {
        let re = regex::Regex::new(r"(?i)(OFFSET\s+\d+|LIMIT\s+\d+)").unwrap();
        let mut offset = None;
        let mut limit = None;

        let query = format!("{query} ALLOW FILTERING");

        for cap in re.find_iter(&query) {
            let part = cap.as_str().to_uppercase();
            if part.starts_with("OFFSET") {
                if let Some(num) = part.split_whitespace().nth(1) {
                    offset = num.parse::<i64>().ok();
                }
            } else if part.starts_with("LIMIT") {
                if let Some(num) = part.split_whitespace().nth(1) {
                    limit = num.parse::<i64>().ok();
                }
            }
        }

        if offset.is_some() {
            let offset_free_query = re.replace_all(&query, "").to_string();
            let offset = offset.unwrap();
            let limit = limit.unwrap_or(1000);
            let mut all_rows = Vec::with_capacity((limit + offset) as usize);
            let mut pager = self.client.paged(1000);
            let mut query_pager = pager.query(&offset_free_query);

            // Keep fetching pages until we have enough rows
            loop {
                let rows = query_pager.next().await?;
                all_rows.extend(rows);

                if !query_pager.has_more() {
                    break;
                }

                // Check if we have enough rows to satisfy offset + limit
                if all_rows.len() as i64 >= offset + limit {
                    break;
                }
            }

            // Apply offset and limit to the collected rows
            let start = (offset as usize).min(all_rows.len());
            let end = (start + limit as usize).min(all_rows.len());

            return Ok(all_rows[start..end]
                .iter()
                .map(|row| T::try_from_row(row).unwrap())
                .collect());
        }
        debug!("query: {}", query);
        let rows = self
            .client
            .query(query)
            .await?
            .response_body()?
            .into_rows()
            .unwrap_or_default();
        Ok(rows.into_iter().map(|row| T::try_from_row(&row).unwrap()).collect())
    }

    fn to_array_string(bytes: &[u8]) -> String {
        format!("0x{}", hex::encode(bytes))
    }

    fn require_distributed() -> bool {
        false
    }
}

impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB>
    DatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
    pub fn new(tx_client: TX, slot_client: SLOT, signature_client: SIGNATURE, account_client: ACCOUNT) -> Self {
        Self::with_config(
            tx_client,
            slot_client,
            signature_client,
            account_client,
            DEFAULT_CACHE_SIZE,
        )
    }

    pub fn with_config(
        tx_client: TX,
        slot_client: SLOT,
        signature_client: SIGNATURE,
        account_client: ACCOUNT,
        cache_size: usize,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(5)
            .enable_all()
            .thread_name("indexerRT")
            .build()
            .expect("Failed to create tokio runtime for DatabaseIndexer");

        Self {
            tx_client,
            slot_client,
            signature_client,
            account_client,
            runtime: Arc::new(runtime),
            tx_cache: Vec::with_capacity(cache_size),
            signature_cache: Vec::with_capacity(cache_size),
            account_ops_create_cache: Vec::with_capacity(cache_size),
            account_ops_delete_cache: Vec::with_capacity(cache_size),
            account_ops_mint_create_cache: Vec::with_capacity(cache_size),
            account_ops_mint_delete_cache: Vec::with_capacity(cache_size),
            program_account_create_cache: Vec::with_capacity(cache_size),
            program_account_delete_cache: Vec::with_capacity(cache_size),
            slot_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            max_cache_size: cache_size,
            s3: None,
            metrics: DatabaseIndexerMetrics::default(),
        }
    }

    pub fn with_runtime(
        tx_client: TX,
        slot_client: SLOT,
        signature_client: SIGNATURE,
        account_client: ACCOUNT,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            tx_client,
            slot_client,
            signature_client,
            account_client,
            runtime,
            tx_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            signature_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            account_ops_create_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            account_ops_delete_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            account_ops_mint_create_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            account_ops_mint_delete_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            program_account_create_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            program_account_delete_cache: Vec::with_capacity(DEFAULT_CACHE_SIZE),
            slot_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            max_cache_size: DEFAULT_CACHE_SIZE,
            s3: None,
            metrics: DatabaseIndexerMetrics::default(),
        }
    }

    pub fn add_s3(&mut self, s3: S3FsClient) {
        self.s3 = Some(s3);
    }

    pub async fn flush_tx_cache_async(&mut self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        if self.tx_cache.is_empty() {
            debug!("tx_cache is empty, nothing to flush");
            return Ok(0);
        }

        self.metrics.report_tx_cache_flush();

        let block = std::mem::take(&mut self.tx_cache);
        let block_len = block.len();
        let slot = block[0].slot;
        debug!("Flushing {} transactions to DB for slot {}", block_len, slot);

        // Execute the query
        let result = match self
            .tx_client
            .insert_native_block(
                TX_TABLE_NAME,
                &[
                    "signature",
                    "transaction",
                    "result",
                    "slot",
                    "pre_accounts",
                    "block_unix_timestamp",
                    "ordering",
                ],
                block,
            )
            .await
        {
            Ok(_) => Ok(block_len),
            Err(e) => {
                error!("Failed to flush tx_cache: {}", e);
                self.metrics.report_db_error("flush_tx_cache");
                Err(Box::new(e))
            }
        };

        // Update cache size metrics after flush
        self.metrics.report_cache_sizes(
            self.tx_cache.len(),
            self.signature_cache.len(),
            self.account_ops_create_cache.len(),
            self.account_ops_delete_cache.len(),
            self.account_ops_mint_create_cache.len(),
            self.account_ops_mint_delete_cache.len(),
            self.tx_cache.capacity(),
            self.signature_cache.capacity(),
            self.account_ops_create_cache.capacity(),
            self.account_ops_delete_cache.capacity(),
            self.account_ops_mint_create_cache.capacity(),
            self.account_ops_mint_delete_cache.capacity(),
        );

        Ok(result.map_err(Box::new)?)
    }

    pub async fn flush_signature_cache_async(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.signature_cache.is_empty() {
            debug!("signature_cache is empty, nothing to flush");
            return Ok(());
        }

        self.metrics.report_signature_cache_flush();

        let block = std::mem::take(&mut self.signature_cache);
        // Execute the query
        let result = match self
            .signature_client
            .insert_native_block(
                SIGNATURE_TABLE_NAME,
                &["account", "signature", "slot", "block_unix_timestamp", "ordering"],
                block,
            )
            .await
        {
            Ok(_) => {
                debug!("Successfully flushed signatures to database");
                Ok(())
            }
            Err(e) => {
                error!("Failed to flush signature_cache: {}", e);
                self.metrics.report_db_error("flush_signature_cache");
                Err(Box::new(e))
            }
        };

        // Update cache size metrics after flush
        self.metrics.report_cache_sizes(
            self.tx_cache.len(),
            self.signature_cache.len(),
            self.account_ops_create_cache.len(),
            self.account_ops_delete_cache.len(),
            self.account_ops_mint_create_cache.len(),
            self.account_ops_mint_delete_cache.len(),
            self.tx_cache.capacity(),
            self.signature_cache.capacity(),
            self.account_ops_create_cache.capacity(),
            self.account_ops_delete_cache.capacity(),
            self.account_ops_mint_create_cache.capacity(),
            self.account_ops_mint_delete_cache.capacity(),
        );

        Ok(result.map_err(Box::new)?)
    }

    pub async fn flush_account_ops_cache_async(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.account_ops_create_cache.is_empty() &&
            self.account_ops_delete_cache.is_empty() &&
            self.account_ops_mint_create_cache.is_empty() &&
            self.account_ops_mint_delete_cache.is_empty() &&
            self.program_account_create_cache.is_empty() &&
            self.program_account_delete_cache.is_empty()
        {
            debug!("account_ops_cache is empty, nothing to flush");
            return Ok(());
        }

        self.metrics.report_account_ops_cache_flush();

        let block: Vec<AccountOwnerRow> = std::mem::take(&mut self.account_ops_create_cache)
            .into_iter()
            .map(|(account, owner)| AccountOwnerRow {
                account: account.to_bytes(),
                owner: owner.to_bytes(),
            })
            .collect();

        if !block.is_empty() {
            if let Err(e) = self
                .account_client
                .insert_native_block(ACCOUNT_TABLE_NAME, &["account", "owner"], block)
                .await
            {
                self.metrics.report_db_error("flush_account_ops_create_cache");
                error!("Failed to flush account_ops_create_cache: {}", e);
                return Err(Box::new(e));
            }
        }

        let block: Vec<AccountMintRow> = std::mem::take(&mut self.account_ops_mint_create_cache)
            .into_iter()
            .map(|(account, owner, account_type, mint)| AccountMintRow {
                account: account.to_bytes(),
                owner: owner.to_bytes(),
                account_type,
                mint: mint.to_bytes(),
            })
            .collect();

        if !block.is_empty() {
            if let Err(e) = self
                .account_client
                .insert_native_block(
                    ACCOUNT_MINT_TABLE_NAME,
                    &["account", "owner", "mint", "account_type"],
                    block,
                )
                .await
            {
                self.metrics.report_db_error("flush_account_ops_mint_create_cache");
                error!("Failed to flush account_ops_mint_create_cache: {}", e);
                return Err(Box::new(e));
            }
        }

        let block: Vec<ProgramAccountRow> = std::mem::take(&mut self.program_account_create_cache)
            .into_iter()
            .map(|(program_id, account)| ProgramAccountRow {
                program_id: program_id.to_bytes(),
                account: account.to_bytes(),
            })
            .collect();

        if !block.is_empty() {
            if let Err(e) = self
                .account_client
                .insert_native_block(PROGRAM_ACCOUNT_TABLE_NAME, &["program_id", "account"], block)
                .await
            {
                self.metrics.report_db_error("flush_program_account_create_cache");
                error!("Failed to flush program_account_create_cache: {}", e);
                return Err(Box::new(e));
            }
        }

        macro_rules! process_delete_cache {
            ($conn:expr, $table:expr, $cache:expr) => {
                let base_query = format!(
                    "DELETE FROM {} WHERE ",
                    if ACCOUNT::require_distributed() {
                        format!("{}_dist", $table)
                    } else {
                        $table.to_string()
                    }
                );

                // Process operation deletions in batches to avoid exceeding max query size
                let block = std::mem::take($cache);

                // Pre-calculate finder strings and their sizes
                let finders: Vec<(String, usize)> = block
                    .into_iter()
                    .map(|(account, owner)| {
                        let finder = format!(
                            "account = {} AND owner = {}",
                            ACCOUNT::to_array_string(&account.to_bytes()),
                            ACCOUNT::to_array_string(&owner.to_bytes())
                        );
                        let finder_size = finder.len();
                        (finder, finder_size)
                    })
                    .collect();

                // Process finders in batches
                for (finder, _finder_size) in finders {
                    let query = format!("{}{}", base_query, finder);
                    if let Err(e) = $conn.execute(&query).await {
                        error!("Failed to flush {}_delete_cache batch: {}", $table, e);
                        self.metrics
                            .report_db_error(&format!("flush_{}_delete_cache", $table));
                        return Err(Box::new(e));
                    }
                }
            };
        }

        if !self.account_ops_delete_cache.is_empty() {
            // Process account_ops deletions
            process_delete_cache!(
                self.account_client,
                ACCOUNT_TABLE_NAME,
                &mut self.account_ops_delete_cache
            );
        }

        if !self.account_ops_mint_delete_cache.is_empty() {
            // Process account_ops_mint deletions
            process_delete_cache!(
                self.account_client,
                ACCOUNT_MINT_TABLE_NAME,
                &mut self.account_ops_mint_delete_cache
            );
        }

        if !self.program_account_delete_cache.is_empty() {
            let table_name = if ACCOUNT::require_distributed() {
                format!("{PROGRAM_ACCOUNT_TABLE_NAME}_dist")
            } else {
                PROGRAM_ACCOUNT_TABLE_NAME.to_string()
            };

            for (program_id, account) in std::mem::take(&mut self.program_account_delete_cache) {
                let query = format!(
                    "DELETE FROM {} WHERE program_id = {} AND account = {}",
                    table_name,
                    ACCOUNT::to_array_string(&program_id.to_bytes()),
                    ACCOUNT::to_array_string(&account.to_bytes())
                );

                if let Err(e) = self.account_client.execute(&query).await {
                    error!("Failed to flush program_account_delete_cache batch: {}", e);
                    self.metrics.report_db_error("flush_program_account_delete_cache");
                    return Err(Box::new(e));
                }
            }
        }

        // Update cache size metrics after flush
        self.metrics.report_cache_sizes(
            self.tx_cache.len(),
            self.signature_cache.len(),
            self.account_ops_create_cache.len(),
            self.account_ops_delete_cache.len(),
            self.account_ops_mint_create_cache.len(),
            self.account_ops_mint_delete_cache.len(),
            self.tx_cache.capacity(),
            self.signature_cache.capacity(),
            self.account_ops_create_cache.capacity(),
            self.account_ops_delete_cache.capacity(),
            self.account_ops_mint_create_cache.capacity(),
            self.account_ops_mint_delete_cache.capacity(),
        );

        Ok(())
    }

    fn collect_program_account_ops(job_effect_diff: &JobEffectDiff) -> (Vec<(Pubkey, Pubkey)>, Vec<(Pubkey, Pubkey)>) {
        let mut creates = Vec::new();
        let mut deletes = Vec::new();

        for ((address, pre_account), diffs) in job_effect_diff.pre_accounts.iter().zip(&job_effect_diff.diffs) {
            let pre_owner = pre_account.as_ref().map(|a| *a.owner()).unwrap_or_default();
            let mut post_account = pre_account.clone().unwrap_or_default();

            for diff in diffs {
                diff.apply_to_account(&mut post_account);
            }

            let post_owner = *post_account.owner();

            // Account deleted or owner changed
            if pre_owner != Pubkey::default() && pre_owner != post_owner {
                deletes.push((pre_owner, *address));
            }

            // Account created or moved under a new owner
            if post_owner != Pubkey::default() && pre_owner != post_owner {
                creates.push((post_owner, *address));
            }
        }

        (creates, deletes)
    }

    pub async fn index_transaction(
        &mut self,
        job: JobEffects,
        block_unix_timestamp: u64,
        ordering: TxOrdering,
    ) -> usize {
        let slot = ordering.slot();
        let ordering_for_sigs = ordering.clone();
        let sanitized_tx = match job.sanitized_tx() {
            Ok(sanitized_tx) => sanitized_tx,
            Err(_) => return 0,
        };
        let signature_rows = to_signature_rows(&sanitized_tx, slot, block_unix_timestamp, ordering_for_sigs);
        let (program_account_creates, program_account_deletes) =
            Self::collect_program_account_ops(&job.job_effect_diff);
        let (tx_row, account_delta) = to_tx_row(job, &sanitized_tx, slot, block_unix_timestamp, ordering);

        // Report metrics for indexed signatures
        self.metrics.report_signatures_indexed(signature_rows.len() as u64);

        self.signature_cache.extend(signature_rows);

        // Report metrics for indexed transactions
        self.metrics.report_tx_indexed(1);

        self.tx_cache.push(tx_row);

        self.account_ops_create_cache.extend(account_delta.account_ops_create);
        self.account_ops_delete_cache.extend(account_delta.account_ops_delete);
        self.account_ops_mint_create_cache
            .extend(account_delta.account_ops_mint_create);
        self.account_ops_mint_delete_cache
            .extend(account_delta.account_ops_mint_delete);
        self.program_account_create_cache
            .extend(program_account_creates.into_iter());
        self.program_account_delete_cache
            .extend(program_account_deletes.into_iter());

        // Update cache size metrics
        self.metrics.report_cache_sizes(
            self.tx_cache.len(),
            self.signature_cache.len(),
            self.account_ops_create_cache.len(),
            self.account_ops_delete_cache.len(),
            self.account_ops_mint_create_cache.len(),
            self.account_ops_mint_delete_cache.len(),
            self.tx_cache.capacity(),
            self.signature_cache.capacity(),
            self.account_ops_create_cache.capacity(),
            self.account_ops_delete_cache.capacity(),
            self.account_ops_mint_create_cache.capacity(),
            self.account_ops_mint_delete_cache.capacity(),
        );

        debug!(
            "tx_cache size: {}, signature_cache size: {}",
            self.tx_cache.len(),
            self.signature_cache.len()
        );

        let mut flushed_tx = 0;
        if self.tx_cache.len() >= self.max_cache_size {
            flushed_tx = self.flush_tx_cache_async().await.unwrap_or(0);
        }

        if self.signature_cache.len() >= self.max_cache_size {
            if let Err(e) = self.flush_signature_cache_async().await {
                error!("Error flushing signature cache: {}", e);
            }
        }

        if self.account_ops_create_cache.len() >= self.max_cache_size ||
            self.account_ops_delete_cache.len() >= self.max_cache_size ||
            self.account_ops_mint_create_cache.len() >= self.max_cache_size ||
            self.account_ops_mint_delete_cache.len() >= self.max_cache_size ||
            self.program_account_create_cache.len() >= self.max_cache_size ||
            self.program_account_delete_cache.len() >= self.max_cache_size
        {
            if let Err(e) = self.flush_account_ops_cache_async().await {
                error!("Error flushing account ops cache: {}", e);
            }
        }

        flushed_tx
    }

    // Batch process transactions for better efficiency
    pub async fn batch_index_transactions(
        &mut self,
        transactions: Vec<JobEffects>,
        block_unix_timestamp: u64,
        shred_id: ShredId,
    ) -> usize {
        let total = transactions.len();
        debug!("Batch indexing {} transactions", total);

        // Pre-allocate space for new items to avoid reallocations
        self.tx_cache.reserve(total);
        self.signature_cache.reserve(total * 4); // Assuming average of 4 accounts per tx

        let mut valid_tx_count = 0;
        let mut flushed_tx = 0;
        for (kth, job) in transactions.into_iter().enumerate() {
            if job.execution_result.is_err() {
                continue;
            }
            valid_tx_count += 1;
            flushed_tx += self
                .index_transaction(
                    job,
                    block_unix_timestamp,
                    TxOrdering::from_shred_id(&shred_id, kth as u64),
                )
                .await;
        }

        // Update metrics for the batch
        debug!("Batch indexed {} out of {} transactions", valid_tx_count, total);
        flushed_tx
    }

    pub async fn index_block_async(
        &mut self,
        slot: u64,
        timestamp: u64,
        blockhash: Hash,
        parent_blockhash: Hash,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        info!("Indexing block at slot {}", slot);
        let start_time = Instant::now();

        // Always flush caches when indexing a new block
        let tx_flushed = self.flush_tx_cache_async().await?;
        self.flush_signature_cache_async().await?;
        self.flush_account_ops_cache_async().await?;

        self.slot_time = timestamp;

        // Execute the query
        let query = format!(
            "INSERT INTO {} (slot, block_unix_timestamp, blockhash, parent_blockhash) VALUES ({}, {}, {}, {})",
            if SLOT::require_distributed() {
                format!("{SLOT_TABLE_NAME}_dist")
            } else {
                SLOT_TABLE_NAME.to_string()
            },
            slot,
            timestamp,
            SLOT::to_array_string(blockhash.as_ref()),
            SLOT::to_array_string(parent_blockhash.as_ref())
        );

        info!("index_block_async query: {}", query);

        match self.slot_client.execute(&query).await {
            Ok(_) => {
                self.metrics.report_blocks_indexed();
                // Log per-table insert counts for slots
                info!(
                    "cassandra_insert: table={} intended_rows=1 success_rows=1 failed_rows=0 slot={}",
                    SLOT_TABLE_NAME, slot
                );
                update_insert_aggregates(SLOT_TABLE_NAME, 1, 1, 0);
            }
            Err(e) => {
                error!("Failed to insert slot data: {e:#}");
                self.metrics.report_db_error("index_block");
                // Log per-table insert counts for slots (failed)
                info!(
                    "cassandra_insert: table={} intended_rows=1 success_rows=0 failed_rows=1 slot={}",
                    SLOT_TABLE_NAME, slot
                );
                update_insert_aggregates(SLOT_TABLE_NAME, 1, 0, 1);
            }
        };

        // Report per-slot processing time
        let processing_time = start_time.elapsed();
        self.metrics.report_slot_processing_time(processing_time.as_secs_f64());

        // Report timestamp of last processed slot
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.metrics.report_last_slot_processed_timestamp(current_time);

        Ok(tx_flushed)
    }

    pub async fn get_slot_async(&self, slot: u64) -> Option<BlockWithTransactions> {
        let _timer = ScopedTimer::new(&self.metrics.histogram_get_slot);

        // Execute the query
        let result = if let Some(s3) = &self.s3 {
            let result = s3
                .get_object(format!("{}/{}/{}/info", slot % 256, slot % 65535, slot))
                .await
                .ok()?;

            let (slot, timestamp, blockhash, parent_blockhash): (u64, u64, Hash, Hash) =
                bincode::deserialize(&result).ok()?;

            vec![SlotRow {
                slot,
                block_unix_timestamp: timestamp,
                blockhash: klickhouse::Bytes::from(blockhash.as_ref().to_vec()),
                parent_blockhash: klickhouse::Bytes::from(parent_blockhash.as_ref().to_vec()),
            }]
        } else {
            let query = format!(
                "SELECT * FROM {} WHERE slot = {}",
                if SLOT::require_distributed() {
                    format!("{SLOT_TABLE_NAME}_dist")
                } else {
                    SLOT_TABLE_NAME.to_string()
                },
                slot
            );
            match self.slot_client.query_collect::<SlotRow>(&query).await {
                Ok(result) => result,
                Err(e) => {
                    error!("Failed to get slot data: {}", e);
                    return None;
                }
            }
        };

        result.first().map(|row| BlockWithTransactions {
            slot: row.slot,
            block_unix_timestamp: row.block_unix_timestamp,
            #[allow(deprecated)]
            blockhash: Hash::new(&row.blockhash.0).to_string(),
            #[allow(deprecated)]
            parent_blockhash: Hash::new(&row.parent_blockhash.0).to_string(),
            parent_slot: row.slot.saturating_sub(1),
            transactions: vec![],
            signatures: vec![],
            tx_count: 0,
        })
    }

    pub async fn get_transaction_by_signature_async(&self, signature: &Signature) -> Option<TransactionWithMetadata> {
        let _timer = ScopedTimer::new(&self.metrics.histogram_get_transaction_by_signature);

        let sig_bytes = TX::to_array_string(signature.as_ref());
        // Execute the query
        let query = format!(
            "SELECT signature, transaction, result, slot, pre_accounts, block_unix_timestamp, ordering FROM {} WHERE signature = {} LIMIT 1",
            if TX::require_distributed() { format!("{TX_TABLE_NAME}_dist") } else { TX_TABLE_NAME.to_string() },
            sig_bytes
        );

        match self.tx_client.query_collect::<TxRowWithTimestamp>(&query).await {
            Ok(result) if !result.is_empty() => Self::tx_row_to_transaction_with_metadata(&result[0]),
            Ok(_) => None,
            Err(e) => {
                error!("Error querying transaction by signature {}: {}", sig_bytes, e);
                None
            }
        }
    }

    pub fn tx_row_to_transaction_with_metadata(tx: &TxRowWithTimestamp) -> Option<TransactionWithMetadata> {
        let transaction: Result<VersionedTransaction, bincode::Error> = bincode::deserialize(&tx.transaction);
        let result: Result<TransactionExecutionDetailsSerializable, bincode::Error> = bincode::deserialize(&tx.result);
        let pre_accounts: Result<Vec<(Pubkey, Option<AccountSharedData>)>, bincode::Error> =
            bincode::deserialize(&tx.pre_accounts);

        match (transaction, result, pre_accounts) {
            (Ok(transaction), Ok(result), Ok(pre_accounts)) => {
                let addresses = transaction.message.static_account_keys().to_vec();
                let mut readonly = vec![];
                let mut writable = vec![];
                let pre_balances = result.pre_balances.clone();
                let mut post_balances = vec![];
                let mut pre_token_balances = vec![];
                let mut post_token_balances = vec![];
                for (i, address) in addresses.into_iter().enumerate() {
                    if transaction.message.is_maybe_writable(i, None) {
                        writable.push(address);
                    } else {
                        readonly.push(address);
                    }
                    let mut new_balance = *pre_balances.get(i).unwrap_or(&0);
                    // Guard against out-of-bounds or missing diffs for this account index
                    let account_diffs: &[AccountDataDiff] = match result.diffs.get(i) {
                        Some(v) => v.as_slice(),
                        None => &[],
                    };
                    for diff in account_diffs.iter() {
                        if let AccountDataDiff::Lamports(post_balance) = diff {
                            new_balance = *post_balance;
                        }
                    }
                    post_balances.push(new_balance);

                    if let Some((_, pre_account)) = pre_accounts.iter().find(|(pubkey, _)| *pubkey == address) {
                        if let Some((program_id, owner, mint, pre_amount, post_amount)) =
                            token_balance_diff_from_diffs(pre_account, account_diffs)
                        {
                            pre_token_balances.push(TransactionTokenBalance {
                                account_index: i as u8,
                                mint: mint.to_string(),
                                ui_token_amount: UiTokenAmount {
                                    amount: pre_amount.to_string(),
                                    decimals: 9,
                                    ui_amount: Some(pre_amount as f64 / 10f64.powi(9)),
                                    ui_amount_string: (pre_amount as f64 / 10f64.powi(9)).to_string(),
                                },
                                owner: owner.to_string(),
                                program_id: program_id.to_string(),
                            });

                            post_token_balances.push(TransactionTokenBalance {
                                account_index: i as u8,
                                mint: mint.to_string(),
                                ui_token_amount: UiTokenAmount {
                                    amount: post_amount.to_string(),
                                    decimals: 9,
                                    ui_amount: Some(post_amount as f64 / 10f64.powi(9)),
                                    ui_amount_string: (post_amount as f64 / 10f64.powi(9)).to_string(),
                                },
                                owner: owner.to_string(),
                                program_id: program_id.to_string(),
                            });
                        }
                    }
                }
                Some(TransactionWithMetadata {
                    transaction,
                    metadata: TransactionStatusMeta {
                        status: result.status.clone(),
                        fee: result.fee,
                        pre_balances,
                        post_balances,
                        inner_instructions: result
                            .inner_instructions
                            .map(|inner_instructions| map_inner_instructions(inner_instructions).collect()),
                        log_messages: result.log_messages.clone(),
                        pre_token_balances: Some(pre_token_balances),
                        post_token_balances: Some(post_token_balances),
                        rewards: None,
                        loaded_addresses: LoadedAddresses { writable, readonly },
                        return_data: result.return_data.clone(),
                        compute_units_consumed: Some(result.executed_units),
                    },
                    slot: tx.slot,
                    unix_timestamp_in_millis: tx.block_unix_timestamp,
                })
            }
            _ => {
                error!("Error deserializing transaction");
                None
            }
        }
    }

    pub async fn get_transactions_by_slot_async(
        &self,
        slot: Slot,
        offset: u64,
        limit: u64,
    ) -> (Vec<TransactionWithMetadata>, Vec<String>, u64) {
        let _timer = ScopedTimer::new(&self.metrics.histogram_get_transactions_by_slot);
        if let Some(s3) = &self.s3 {
            // calculate counts
            let dir = s3
                .list_dir(format!("{}/{}/{}/", slot % 256, slot % 65535, slot))
                .await
                .unwrap_or_default();

            // skip 2 files: info and last shard

            let tx_count = if dir.len() > 2 {
                let last_shard = dir
                    .iter()
                    .filter(|x| x.contains("info"))
                    .map(|x| x.parse::<u64>().unwrap())
                    .max()
                    .unwrap_or(0);
                let last_shard_data = s3
                    .get_object(format!("{}/{}/{}/{}", slot % 256, slot % 65535, slot, last_shard))
                    .await;

                if let Ok(last_shard_data) = last_shard_data {
                    match bincode::deserialize::<Vec<SerializableTxRow>>(&last_shard_data) {
                        Ok(data) => (dir.len() as u64 - 2) * FILE_CHUNK_SIZE as u64 + data.len() as u64,
                        Err(e) => {
                            error!("indexerrpc: error deserializing shard {}: {}", last_shard, e);
                            0
                        }
                    }
                } else {
                    0
                }
            } else {
                0
            };

            // get txs
            let mut current_shard_idx = (offset as usize) / FILE_CHUNK_SIZE;
            let mut skip = (offset as usize) % FILE_CHUNK_SIZE;
            let mut remaining_limit = limit;
            let mut all_result = vec![];

            // Break the loop if we've collected enough results or hit an error
            while remaining_limit > 0 {
                let result = match s3
                    .get_object(format!(
                        "{}/{}/{}/{}",
                        slot % 256,
                        slot % 65535,
                        slot,
                        current_shard_idx
                    ))
                    .await
                {
                    Ok(data) => data,
                    Err(e) => {
                        error!("indexerrpc: error loading shard {}: {}", current_shard_idx, e);
                        break;
                    }
                };

                let deserialized: Vec<SerializableTxRow> = match bincode::deserialize(&result) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("indexerrpc: error deserializing shard {}: {}", current_shard_idx, e);
                        break;
                    }
                };

                // If we got an empty shard, break the loop
                if deserialized.is_empty() {
                    error!("indexerrpc: empty shard {}", current_shard_idx);
                    break;
                }

                let result_chunk: Vec<TxRow> = deserialized
                    .into_iter()
                    .skip(skip)
                    .take(remaining_limit as usize)
                    .map(TxRow::from_serializable)
                    .collect();

                // If we didn't get any results after skipping, break
                if result_chunk.is_empty() {
                    error!("indexerrpc: empty shard 2 {}", current_shard_idx);
                    break;
                }

                // Update counters for next iteration
                skip = 0;
                current_shard_idx += 1;
                remaining_limit -= result_chunk.len() as u64;
                all_result.extend(result_chunk);
            }

            let mut signatures = vec![];
            let mut transactions = vec![];

            for tx in all_result {
                signatures.push(Signature::try_from(tx.signature.as_ref()).unwrap().to_string());
                if let Some(transaction) =
                    Self::tx_row_to_transaction_with_metadata(&TxRowWithTimestamp::from_tx_row(tx))
                {
                    transactions.push(transaction);
                } else {
                    signatures.pop();
                }
            }

            (transactions, signatures, tx_count)
        } else {
            let mut signatures = vec![];

            // Execute the query
            let query = format!("SELECT signature, transaction, result, slot, pre_accounts, block_unix_timestamp, ordering FROM {} WHERE slot = {} LIMIT {} OFFSET {}",
                if TX::require_distributed() { format!("{TX_TABLE_NAME}_dist") } else { TX_TABLE_NAME.to_string() },
                slot,
                limit,
                offset
            );

            match self.tx_client.query_collect::<TxRowWithTimestamp>(&query).await {
                Ok(result) => (
                    result
                        .into_iter()
                        .filter_map(|tx| {
                            signatures.push(Signature::try_from(tx.signature.as_ref()).unwrap().to_string());
                            Self::tx_row_to_transaction_with_metadata(&tx)
                        })
                        .collect(),
                    signatures,
                    0,
                ),
                Err(e) => {
                    error!("Error querying transactions by slot {}: {}", slot, e);
                    (Vec::new(), Vec::new(), 0)
                }
            }
        }
    }

    pub async fn get_signatures_by_account_async(
        &self,
        account: &Pubkey,
        filters: infinisvm_types::SignatureFilters,
        limit: usize,
    ) -> Vec<Signature> {
        if limit == 0 {
            return vec![];
        }

        let _timer = ScopedTimer::new(&self.metrics.histogram_get_signatures_by_account);

        let account_hex = SIGNATURE::to_array_string(&account.to_bytes());

        let table = if SIGNATURE::require_distributed() {
            format!("{SIGNATURE_TABLE_NAME}_dist")
        } else {
            SIGNATURE_TABLE_NAME.to_string()
        };

        // Helper to convert DB rows into (ordering, Signature)
        fn rows_to_pairs(rows: Vec<SingleSignatureRow>) -> Vec<(num_bigint::BigInt, Signature)> {
            rows.into_iter()
                .filter_map(|r| match (r.ordering, Signature::try_from(r.signature.as_ref())) {
                    (ordering, Ok(s)) => Some((ordering, s)),
                    (_, Err(_)) => None,
                })
                .collect()
        }

        if let infinisvm_types::SignatureFilters::Signature(before, after) = filters {
            //
            // BEFORE branch: (Some(sig), None)
            //
            if let (Some(sig), None) = (before, after) {
                let sig_bytes = SIGNATURE::to_array_string(sig.as_ref());

                // 1) Find the ordering of the anchor signature
                let ordering_lookup = format!(
                    "SELECT ordering FROM {table} \
                     WHERE account = {account_hex} AND signature = {sig_bytes} \
                     ORDER BY ordering DESC \
                     LIMIT 1"
                );
                let current_ordering = match self
                    .signature_client
                    .query_collect::<SingleOrderingRow>(&ordering_lookup)
                    .await
                {
                    Ok(rows) if !rows.is_empty() => rows[0].ordering.clone(),
                    _ => return vec![],
                };

                let mut signatures: Vec<(num_bigint::BigInt, Signature)> = Vec::with_capacity(limit);

                // 2) Same-ordering, earlier signatures (all rows for that ordering; filter in
                //    Rust)
                let query_same = format!(
                    "SELECT signature, ordering FROM {table} \
                     WHERE account = {account_hex} AND ordering = {current_ordering}"
                );
                if let Ok(rows) = self
                    .signature_client
                    .query_collect::<SingleSignatureRow>(&query_same)
                    .await
                {
                    let mut pairs = rows_to_pairs(rows);
                    pairs.retain(|(ordering, s)| ordering == &current_ordering && s < &sig);
                    signatures.extend(pairs);
                }

                // If we already have enough and only touched current_ordering, we can return
                if signatures.len() >= limit {
                    signatures.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
                    signatures.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
                    signatures.truncate(limit);
                    return signatures.into_iter().map(|(_, s)| s).collect();
                }

                // 3) Earlier orderings (< current_ordering)
                let remaining = limit - signatures.len();
                if remaining > 0 {
                    let query_more = format!(
                        "SELECT signature, ordering FROM {table} \
                         WHERE account = {account_hex} AND ordering < {current_ordering} \
                         ORDER BY ordering DESC \
                         LIMIT {remaining}"
                    );
                    if let Ok(rows) = self
                        .signature_client
                        .query_collect::<SingleSignatureRow>(&query_more)
                        .await
                    {
                        signatures.extend(rows_to_pairs(rows));
                    }
                }

                if signatures.is_empty() {
                    return vec![];
                }

                // 4) Boundary fix: if we cut through the *oldest* ordering among those <
                //    current_ordering,
                // re-fetch that entire ordering to ensure we don't miss higher-signature rows.
                let min_earlier_ordering = signatures
                    .iter()
                    .filter(|(ordering, _)| ordering < &current_ordering)
                    .map(|(ordering, _)| ordering)
                    .min()
                    .cloned();

                if let Some(last_ordering) = min_earlier_ordering {
                    let query_less = format!(
                        "SELECT signature, ordering FROM {table} \
                         WHERE account = {account_hex} AND ordering = {last_ordering}"
                    );
                    if let Ok(rows) = self
                        .signature_client
                        .query_collect::<SingleSignatureRow>(&query_less)
                        .await
                    {
                        signatures.extend(rows_to_pairs(rows));
                    }
                }

                // Final canonical order for "before": ordering DESC, signature DESC
                signatures.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
                signatures.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
                signatures.truncate(limit);
                return signatures.into_iter().map(|(_, s)| s).collect();
            }

            //
            // AFTER branch: (None, Some(sig))
            //
            if let (None, Some(sig)) = (before, after) {
                let sig_bytes = SIGNATURE::to_array_string(sig.as_ref());

                // 1) Find ordering of anchor signature
                let ordering_lookup = format!(
                    "SELECT ordering FROM {table} \
                     WHERE account = {account_hex} AND signature = {sig_bytes} \
                     ORDER BY ordering DESC \
                     LIMIT 1"
                );
                let current_ordering = match self
                    .signature_client
                    .query_collect::<SingleOrderingRow>(&ordering_lookup)
                    .await
                {
                    Ok(rows) if !rows.is_empty() => rows[0].ordering.clone(),
                    _ => return vec![],
                };

                let mut signatures: Vec<(num_bigint::BigInt, Signature)> = Vec::with_capacity(limit);

                // 2) Same-ordering, later signatures (all rows for that ordering; filter in
                //    Rust)
                let query_same = format!(
                    "SELECT signature, ordering FROM {table} \
                     WHERE account = {account_hex} AND ordering = {current_ordering}"
                );
                if let Ok(rows) = self
                    .signature_client
                    .query_collect::<SingleSignatureRow>(&query_same)
                    .await
                {
                    let mut pairs = rows_to_pairs(rows);
                    pairs.retain(|(ordering, s)| ordering == &current_ordering && s > &sig);
                    signatures.extend(pairs);
                }

                // If enough from same ordering, return sorted
                if signatures.len() >= limit {
                    // ordering ASC, signature DESC (your original after-branch behavior)
                    signatures.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
                    signatures.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
                    signatures.truncate(limit);
                    return signatures.into_iter().map(|(_, s)| s).collect();
                }

                // 3) Later orderings (> current_ordering)
                let remaining = limit - signatures.len();
                if remaining > 0 {
                    let query_more = format!(
                        "SELECT signature, ordering FROM {table} \
                         WHERE account = {account_hex} AND ordering > {current_ordering} \
                         ORDER BY ordering ASC \
                         LIMIT {remaining}"
                    );
                    if let Ok(rows) = self
                        .signature_client
                        .query_collect::<SingleSignatureRow>(&query_more)
                        .await
                    {
                        signatures.extend(rows_to_pairs(rows));
                    }
                }

                if signatures.is_empty() {
                    return vec![];
                }

                // 4) Boundary fix for "later" side: we may have partially fetched the *largest*
                // ordering > current_ordering; re-fetch that full ordering.
                let max_later_ordering = signatures
                    .iter()
                    .filter(|(ordering, _)| ordering > &current_ordering)
                    .map(|(ordering, _)| ordering)
                    .max()
                    .cloned();

                if let Some(last_ordering) = max_later_ordering {
                    let query_same_order = format!(
                        "SELECT signature, ordering FROM {table} \
                         WHERE account = {account_hex} AND ordering = {last_ordering}"
                    );
                    if let Ok(rows) = self
                        .signature_client
                        .query_collect::<SingleSignatureRow>(&query_same_order)
                        .await
                    {
                        signatures.extend(rows_to_pairs(rows));
                    }
                }

                // Final order for "after": ordering ASC, signature DESC
                signatures.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
                signatures.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
                signatures.truncate(limit);
                return signatures.into_iter().map(|(_, s)| s).collect();
            }
        }

        //
        // No before/after filter (or unsupported combination): return latest `limit`
        // in canonical order (ordering DESC, signature DESC) without using signature
        // in SQL ORDER BY.
        //
        let query = format!(
            "SELECT signature, ordering FROM {table} \
             WHERE account = {account_hex} \
             ORDER BY ordering DESC \
             LIMIT {limit}"
        );
        debug!("get_signatures_by_account_async query: {}", query);

        let rows = match self.signature_client.query_collect::<SingleSignatureRow>(&query).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("Error querying signatures by account {}: {}", account_hex, e);
                return Vec::new();
            }
        };

        if rows.is_empty() {
            return vec![];
        }

        let mut signatures = rows_to_pairs(rows);

        // Boundary fix as in the branches: we might be cutting through the oldest
        // ordering included in this limited query. Re-fetch that ordering completely.
        let min_ordering = signatures.iter().map(|(ordering, _)| ordering).min().cloned();

        if let Some(last_ordering) = min_ordering {
            let query_boundary = format!(
                "SELECT signature, ordering FROM {table} \
                 WHERE account = {account_hex} AND ordering = {last_ordering}"
            );
            if let Ok(rows) = self
                .signature_client
                .query_collect::<SingleSignatureRow>(&query_boundary)
                .await
            {
                signatures.extend(rows_to_pairs(rows));
            }
        }

        // Global canonical ordering for default case: ordering DESC, signature DESC
        signatures.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        signatures.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        signatures.truncate(limit);
        signatures.into_iter().map(|(_, s)| s).collect()
    }

    pub async fn flush_caches_conditionally(&mut self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut tx_flushed = 0;

        if self.tx_cache.len() >= self.max_cache_size {
            tx_flushed += if let Ok(flushed) = self.flush_tx_cache_async().await {
                flushed
            } else {
                error!("Error flushing tx cache");
                0
            };
        }

        if self.signature_cache.len() >= self.max_cache_size {
            if let Err(e) = self.flush_signature_cache_async().await {
                error!("Error flushing signature cache: {}", e);
            }
        }

        if self.account_ops_create_cache.len() >= self.max_cache_size ||
            self.account_ops_delete_cache.len() >= self.max_cache_size ||
            self.account_ops_mint_create_cache.len() >= self.max_cache_size ||
            self.account_ops_mint_delete_cache.len() >= self.max_cache_size ||
            self.program_account_create_cache.len() >= self.max_cache_size ||
            self.program_account_delete_cache.len() >= self.max_cache_size
        {
            if let Err(e) = self.flush_account_ops_cache_async().await {
                error!("Error flushing account ops cache: {}", e);
            }
        }

        Ok(tx_flushed)
    }

    pub async fn index_transactions_async(
        &mut self,
        batch: Vec<JobEffects>,
        block_unix_timestamp: u64,
        shred_id: ShredId,
    ) -> usize {
        let mut tx_flushed = if !batch.is_empty() {
            debug!("Indexing batch of {} transactions", batch.len());
            self.batch_index_transactions(batch, block_unix_timestamp, shred_id)
                .await
        } else {
            0
        };

        tx_flushed += if let Ok(flushed) = self.flush_caches_conditionally().await {
            flushed
        } else {
            error!("Error flushing caches");
            0
        };

        tx_flushed
    }

    pub async fn get_token_account_owned_by_owner_async(
        &self,
        owner: Option<Pubkey>,
        program_id: Option<Pubkey>,
        mint: Option<Pubkey>,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        let _timer = ScopedTimer::new(&self.metrics.histogram_get_token_accounts_owned_by_account);

        // Cassandra requires filtering on partition key (owner)
        let owner = match owner {
            Some(owner) => owner,
            None => return Vec::new(),
        };

        let table_name = if ACCOUNT::require_distributed() {
            format!("{ACCOUNT_MINT_TABLE_NAME}_dist")
        } else {
            ACCOUNT_MINT_TABLE_NAME.to_string()
        };

        // Build valid Cassandra query: must start with partition key (owner)
        // Note: Cannot filter on account_type without filtering on account first
        // (PRIMARY KEY order: owner, account, account_type) So we fetch all
        // rows and filter by account_type and mint in application code
        // Only select account_type if we need to filter by it
        let select_columns = if program_id.is_some() {
            "account, mint, account_type"
        } else {
            "account, mint"
        };

        let mut query = format!(
            "SELECT {} FROM {} WHERE owner = {}",
            select_columns,
            table_name,
            ACCOUNT::to_array_string(&owner.to_bytes())
        );

        // Cassandra LIMIT clause uses Int32, max value is 2,147,483,647
        const CASSANDRA_MAX_LIMIT: usize = i32::MAX as usize;

        // Calculate how many rows to fetch based on filters
        // We need to fetch more if filtering by account_type or mint since we filter in
        // application code
        let needs_app_filtering = program_id.is_some() || mint.is_some();
        let fetch_limit = if needs_app_filtering {
            // Fetch more rows to account for filtering (multiply by a factor to ensure we
            // get enough)
            ((limit + offset) * 3).min(CASSANDRA_MAX_LIMIT)
        } else {
            // No filtering needed, use offset/limit directly
            if offset > 0 {
                (limit + offset).min(CASSANDRA_MAX_LIMIT)
            } else {
                limit.min(CASSANDRA_MAX_LIMIT)
            }
        };

        // Build query with limit
        query = format!("{query} LIMIT {fetch_limit}");

        let query_result = if program_id.is_some() {
            self.account_client.query_collect::<AccountMintQueryRow>(&query).await
        } else {
            // Use a simpler row type when account_type is not selected
            self.account_client
                .query_collect::<AccountMintQueryRowWithoutType>(&query)
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|r| AccountMintQueryRow {
                            account: r.account,
                            mint: r.mint,
                            account_type: 0, // Default value, won't be used for filtering
                        })
                        .collect()
                })
        };

        match query_result {
            Ok(mut rows) => {
                // Filter by account_type if program_id is provided
                // Cannot filter in WHERE clause because account_type requires account to be
                // filtered first
                if let Some(program_id) = program_id {
                    let account_type = if program_id == spl_token::id() { 1 } else { 2 };
                    rows.retain(|r| r.account_type == account_type);
                }

                // Filter by mint if provided (mint is not part of PRIMARY KEY)
                if let Some(mint_filter) = mint {
                    rows.retain(|r| r.mint.0.as_slice() == mint_filter.to_bytes());
                }

                // Apply offset and limit in application code
                let final_rows: Vec<_> = rows.into_iter().skip(offset).take(limit).collect();

                final_rows
                    .into_iter()
                    .map(|r| Pubkey::try_from(r.account.0.as_slice()).unwrap())
                    .collect()
            }
            Err(e) => {
                error!("Error querying account_ops_mint: {}", e);
                Vec::new()
            }
        }
    }

    pub async fn get_token_account_owned_by_account_async(
        &self,
        account: Option<Pubkey>,
        program_id: Option<Pubkey>,
        mint: Option<Pubkey>,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        let _timer = ScopedTimer::new(&self.metrics.histogram_get_token_accounts_owned_by_account);

        let mut query = format!(
            "SELECT account FROM {} WHERE 1=1",
            if ACCOUNT::require_distributed() {
                format!("{ACCOUNT_MINT_TABLE_NAME}_dist")
            } else {
                ACCOUNT_MINT_TABLE_NAME.to_string()
            }
        );

        if let Some(account) = account {
            query = format!(
                "{query} AND account = {}",
                ACCOUNT::to_array_string(&account.to_bytes())
            );
        }

        if let Some(program_id) = program_id {
            query = format!(
                "{query} AND account_type = {}",
                if program_id == spl_token::id() { 1 } else { 2 }
            );
        }

        if let Some(mint) = mint {
            query = format!("{query} AND mint = {}", ACCOUNT::to_array_string(&mint.to_bytes()));
        }

        query = format!("{query} LIMIT {limit} OFFSET {offset}");
        let result = self.account_client.query_collect::<SingleAccountRow>(&query).await;
        match result {
            Ok(result) => result
                .into_iter()
                .map(|r| Pubkey::try_from(r.account.0.as_slice()).unwrap())
                .collect(),
            Err(e) => {
                error!("Error querying account_ops: {}", e);
                Vec::new()
            }
        }
    }

    pub async fn get_account_owned_by_account_async(
        &self,
        account: &Pubkey,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        let _timer = ScopedTimer::new(&self.metrics.histogram_get_accounts_owned_by_account);

        let account_hex = ACCOUNT::to_array_string(&account.to_bytes());
        let mut query = format!(
            "SELECT account FROM {} WHERE owner = {}",
            if ACCOUNT::require_distributed() {
                format!("{ACCOUNT_TABLE_NAME}_dist")
            } else {
                ACCOUNT_TABLE_NAME.to_string()
            },
            account_hex
        );

        query = format!("{query} LIMIT {limit} OFFSET {offset}");

        let result = self.account_client.query_collect::<SingleAccountRow>(&query).await;
        match result {
            Ok(result) => result
                .into_iter()
                .map(|r| Pubkey::try_from(r.account.0.as_slice()).unwrap())
                .collect(),
            Err(e) => {
                error!("Error querying account_ops: {}", e);
                Vec::new()
            }
        }
    }

    pub async fn get_program_accounts_async(&self, program_id: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey> {
        let program_hex = ACCOUNT::to_array_string(&program_id.to_bytes());
        let mut query = format!(
            "SELECT account FROM {} WHERE program_id = {}",
            if ACCOUNT::require_distributed() {
                format!("{PROGRAM_ACCOUNT_TABLE_NAME}_dist")
            } else {
                PROGRAM_ACCOUNT_TABLE_NAME.to_string()
            },
            program_hex
        );

        query = format!("{query} LIMIT {limit} OFFSET {offset}");

        let result = self.account_client.query_collect::<SingleAccountRow>(&query).await;
        match result {
            Ok(result) => result
                .into_iter()
                .map(|r| Pubkey::try_from(r.account.0.as_slice()).unwrap())
                .collect(),
            Err(e) => {
                error!("Error querying program_accounts: {}", e);
                Vec::new()
            }
        }
    }

    pub fn report_metrics(&self) {
        // Report current cache sizes
        self.metrics.report_cache_sizes(
            self.tx_cache.len(),
            self.signature_cache.len(),
            self.account_ops_create_cache.len(),
            self.account_ops_delete_cache.len(),
            self.account_ops_mint_create_cache.len(),
            self.account_ops_mint_delete_cache.len(),
            self.tx_cache.capacity(),
            self.signature_cache.capacity(),
            self.account_ops_create_cache.capacity(),
            self.account_ops_delete_cache.capacity(),
            self.account_ops_mint_create_cache.capacity(),
            self.account_ops_mint_delete_cache.capacity(),
        );

        // Report timestamp of last processed slot
        self.metrics.report_last_slot_processed_timestamp(self.slot_time);
    }
}

#[async_trait]
impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> Indexer
    for DatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
    fn index_block(&mut self, slot: u64, timestamp: u64, blockhash: Hash, parent_blockhash: Hash) {
        let runtime = self.runtime.clone();
        if let Err(e) = runtime.block_on(self.index_block_async(slot, timestamp, blockhash, parent_blockhash)) {
            error!("Error indexing block: {}", e);
        }
    }

    fn index_transactions(&mut self, batch: Vec<JobEffects>, block_unix_timestamp: u64, shred_id: ShredId) {
        if batch.is_empty() {
            return;
        }

        let runtime = self.runtime.clone();
        let _ = runtime.block_on(self.index_transactions_async(batch, block_unix_timestamp, shred_id));
    }

    fn flush(&mut self) {
        self.runtime.clone().block_on(async {
            println!("flushing indexer tx cache");
            if let Err(e) = self.flush_tx_cache_async().await {
                error!("Error flushing tx cache: {}", e);
            }
            println!("flushing indexer signature cache");
            if let Err(e) = self.flush_signature_cache_async().await {
                error!("Error flushing signature cache: {}", e);
            }
            println!("flushing indexer account ops cache");
            if let Err(e) = self.flush_account_ops_cache_async().await {
                error!("Error flushing account ops cache: {}", e);
            }
        });
    }
}

#[async_trait]
impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> RpcIndexer
    for DatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
    async fn find_accounts_owned_by(&self, owner: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey> {
        self.get_account_owned_by_account_async(owner, limit, offset).await
    }

    async fn find_accounts_by_program(&self, program_id: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey> {
        self.get_program_accounts_async(program_id, limit, offset).await
    }

    async fn find_token_accounts_owned_by(
        &self,
        owner: &Pubkey,
        program_id: Option<Pubkey>,
        mint: Option<Pubkey>,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        self.get_token_account_owned_by_owner_async(Some(*owner), program_id, mint, limit, offset)
            .await
    }

    async fn find_token_accounts_by_mint(
        &self,
        program_id: Option<Pubkey>,
        mint: Pubkey,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        self.get_token_account_owned_by_account_async(None, program_id, Some(mint), limit, offset)
            .await
    }

    async fn get_block_with_transactions(
        &self,
        slot: u64,
        offset: u64,
        limit: u64,
    ) -> eyre::Result<Option<BlockWithTransactions>> {
        let (transactions, signatures, tx_count) = self.get_transactions_by_slot_async(slot, offset, limit).await;
        let block = self.get_slot_async(slot).await;

        if let Some(block) = block {
            Ok(Some(BlockWithTransactions {
                slot,
                parent_blockhash: block.parent_blockhash,
                blockhash: block.blockhash,
                parent_slot: block.parent_slot,
                block_unix_timestamp: block.block_unix_timestamp,
                tx_count,
                transactions,
                signatures,
            }))
        } else {
            Ok(None)
        }
    }

    async fn find_signatures_of_account(
        &self,
        account: &Pubkey,
        filters: infinisvm_types::SignatureFilters,
        limit: usize,
    ) -> eyre::Result<Vec<Signature>> {
        let signatures = self.get_signatures_by_account_async(account, filters, limit).await;
        Ok(signatures)
    }

    async fn get_transaction_with_metadata(
        &self,
        signature: &Signature,
    ) -> eyre::Result<Option<TransactionWithMetadata>> {
        let transaction = self.get_transaction_by_signature_async(signature).await;
        Ok(transaction)
    }
}

pub struct MultiDatabaseIndexer<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> {
    // Workers for write operations
    senders: Vec<tokio::sync::mpsc::UnboundedSender<IndexerCommand>>,
    // Direct access to clients for read operations
    clients: Vec<DatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>>,
    client_cnt: usize,
    // Last worker used, for round-robin load balancing
    last_worker: Mutex<usize>,
    total_processed: Arc<AtomicU64>,

    s3: Option<S3FsClient>,
    current_slot: u64,
    slot_cache: Vec<TxRow>,
    next_slot: Vec<TxRow>,

    // Metrics for MultiDatabaseIndexer
    metrics: MultiDatabaseIndexerMetrics,
}

// Commands to be sent to worker processes - now only write operations
enum IndexerCommand {
    IndexBlock {
        slot: u64,
        timestamp: u64,
        blockhash: Hash,
        parent_blockhash: Hash,
    },
    IndexTransactions {
        batch: Vec<JobEffects>,
        block_unix_timestamp: u64,
        shred_id: ShredId,
    },
    Flush,
    Shutdown,
}

// Implement Send + Sync
unsafe impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> Send
    for MultiDatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
}
unsafe impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> Sync
    for MultiDatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
}

impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB>
    MultiDatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
    pub fn new(clients: Vec<(TX, SLOT, SIGNATURE, ACCOUNT)>, s3: Option<S3FsClient>) -> Self {
        let client_cnt = clients.len();
        if client_cnt == 0 {
            panic!("No clients provided");
        }

        // Create a shared runtime for all operations
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(client_cnt)
                .enable_all()
                .thread_name("indexerRT")
                .build()
                .expect("Failed to create tokio runtime for MultiDatabaseIndexer"),
        );

        let mut senders = Vec::with_capacity(client_cnt);
        let mut indexers = Vec::with_capacity(client_cnt);
        // Use Arc to share the atomic counter across all workers
        let total_processed = Arc::new(AtomicU64::new(0));

        // Spawn a worker for each client
        for (tx_client, slot_client, signature_client, account_client) in clients {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<IndexerCommand>();
            senders.push(sender);
            // Create a new DatabaseIndexer for this worker, sharing the same runtime
            let runtime_clone = Arc::clone(&runtime);
            let mut indexer = DatabaseIndexer::with_runtime(
                tx_client.clone(),
                slot_client.clone(),
                signature_client.clone(),
                account_client.clone(),
                runtime_clone.clone(),
            );
            if let Some(s3_client) = s3.clone() {
                indexer.add_s3(s3_client);
            }
            indexers.push(indexer);

            // Create worker indexer with its own pool
            let mut worker_indexer =
                DatabaseIndexer::with_runtime(tx_client, slot_client, signature_client, account_client, runtime_clone);
            if let Some(s3_client) = s3.clone() {
                worker_indexer.add_s3(s3_client);
            }
            // Create a clone of the counter for this worker
            let total_processed_clone = Arc::clone(&total_processed);
            // Spawn a worker task in the runtime
            runtime.spawn(async move {
                while let Some(cmd) = receiver.recv().await {
                    match cmd {
                        IndexerCommand::IndexTransactions {
                            batch,
                            block_unix_timestamp,
                            shred_id,
                        } => {
                            if batch.is_empty() {
                                return;
                            }
                            let tx_flushed = worker_indexer
                                .index_transactions_async(batch, block_unix_timestamp, shred_id)
                                .await;
                            if tx_flushed > 0 {
                                debug!("Indexed {} transactions", tx_flushed);
                                total_processed_clone.fetch_add(tx_flushed as u64, Ordering::Relaxed);
                            }
                        }
                        IndexerCommand::IndexBlock {
                            slot,
                            timestamp,
                            blockhash,
                            parent_blockhash,
                        } => {
                            let _ = worker_indexer
                                .index_block_async(slot, timestamp, blockhash, parent_blockhash)
                                .await;
                        }
                        IndexerCommand::Flush => {
                            let tx_flushed = worker_indexer.flush_tx_cache_async().await.unwrap_or(0);
                            if let Err(e) = worker_indexer.flush_signature_cache_async().await {
                                error!("Error flushing signature cache: {}", e);
                            }
                            if let Err(e) = worker_indexer.flush_account_ops_cache_async().await {
                                error!("Error flushing account ops cache: {}", e);
                            }

                            if tx_flushed > 0 {
                                debug!("Flushed {} transactions", tx_flushed);
                                total_processed_clone.fetch_add(tx_flushed as u64, Ordering::Relaxed);
                            }
                        }
                        IndexerCommand::Shutdown => {
                            // Flush any remaining data before shutdown
                            if let Err(e) = worker_indexer.flush_tx_cache_async().await {
                                error!("Error flushing tx cache: {}", e);
                            }
                            if let Err(e) = worker_indexer.flush_signature_cache_async().await {
                                error!("Error flushing signature cache: {}", e);
                            }
                            if let Err(e) = worker_indexer.flush_account_ops_cache_async().await {
                                error!("Error flushing account ops cache: {}", e);
                            }
                            break;
                        }
                    }
                }
            });
        }

        Self {
            senders,
            clients: indexers,
            client_cnt,
            last_worker: Mutex::new(0),
            total_processed,
            s3,
            slot_cache: Vec::with_capacity(500_000),
            next_slot: Vec::with_capacity(500_000),
            current_slot: u64::MAX,
            metrics: MultiDatabaseIndexerMetrics::default(),
        }
    }

    // Helper to distribute commands in a round-robin fashion for better load
    // balancing
    fn dispatch_command(&self, command: IndexerCommand) {
        if self.client_cnt == 0 {
            return;
        }

        let mut last_worker = self.last_worker.lock().unwrap();
        *last_worker = (*last_worker + 1) % self.client_cnt;

        if let Some(sender) = self.senders.get(*last_worker) {
            // It's ok to ignore errors here - if the channel is closed, the worker is gone
            let _ = sender.send(command);
            // Report command dispatched metric
            self.metrics.report_commands_dispatched();
        }
    }

    pub fn total_processed(&self) -> u64 {
        let count = self.total_processed.load(Ordering::Relaxed);
        // Report total processed count
        self.metrics.report_total_processed(count);
        count
    }

    // Flush all worker caches
    pub fn flush_all(&self) {
        for sender in &self.senders {
            if let Err(e) = sender.send(IndexerCommand::Flush) {
                error!("Error flushing indexer: {}", e);
            }
        }
    }

    // Report metrics from this indexer and its clients
    pub fn report_metrics(&self) {
        // Report MultiDatabaseIndexer metrics
        self.metrics.report_cache_sizes(
            self.slot_cache.len(),
            self.next_slot.len(),
            self.slot_cache.capacity(),
            self.next_slot.capacity(),
        );
        self.metrics.report_total_processed(self.total_processed());

        // Report metrics from all clients
        for client in &self.clients {
            client.report_metrics();
        }
    }
}

impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> Drop
    for MultiDatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
    fn drop(&mut self) {
        info!("Shutting down MultiDatabaseIndexer...");

        // Send shutdown command to all workers
        for sender in &self.senders {
            if let Err(e) = sender.send(IndexerCommand::Shutdown) {
                error!("Error sending shutdown command: {}", e);
            }
        }

        // Allow some time for workers to process shutdown
        std::thread::sleep(std::time::Duration::from_millis(500));

        info!("MultiDatabaseIndexer shutdown complete");
    }
}

#[async_trait]
impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> Indexer
    for MultiDatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
    fn index_block(&mut self, slot: u64, timestamp: u64, blockhash: Hash, parent_blockhash: Hash) {
        debug!("MultiDatabaseIndexer: Indexing block at slot {}", slot);

        // Flush all workers before indexing a new block
        self.flush_all();

        let mut shard_idx = 0;
        // Move out current slot cache, swap in next slot's buffer. This is O(1).
        let mut all_txs = std::mem::take(&mut self.slot_cache);
        std::mem::swap(&mut self.slot_cache, &mut self.next_slot);
        self.current_slot = slot + 1;

        // @chaz: still needed? How do RPC records slots?
        if let Some(s3) = self.s3.clone() {
            std::thread::spawn(move || {
                // Sort outside the critical section to minimize index_block latency
                all_txs.sort_unstable_by(|a, b| a.ordering.cmp(&b.ordering));
                let mut futures = Vec::new();
                let file_path = format!("{}/{}/{}/info", slot % 256, slot % 65535, slot);

                futures.push(
                    s3.put_object(
                        file_path,
                        bincode::serialize(&(slot, timestamp, blockhash, parent_blockhash))
                            .unwrap()
                            .into(),
                    ),
                );

                loop {
                    let txs: Vec<_> = all_txs.drain(0..FILE_CHUNK_SIZE.min(all_txs.len())).collect();
                    if txs.is_empty() {
                        break;
                    }

                    let file_path = format!("{}/{}/{}/{}", slot % 256, slot % 65535, slot, shard_idx);
                    let fut = s3.put_object(
                        file_path,
                        bincode::serialize(&txs.iter().map(|tx| tx.to_serializable()).collect::<Vec<_>>())
                            .unwrap()
                            .into(),
                    );
                    shard_idx += 1;
                    futures.push(fut);
                }
                tokio::runtime::Runtime::new().unwrap().block_on(async move {
                    for fut in futures {
                        if let Err(e) = fut.await {
                            error!("Error writing to S3: {}", e);
                        }
                    }
                });
            });
        }

        // save to database
        self.dispatch_command(IndexerCommand::IndexBlock {
            slot,
            timestamp,
            blockhash,
            parent_blockhash,
        });

        // Report cache sizes
        self.metrics.report_cache_sizes(
            self.slot_cache.len(),
            self.next_slot.len(),
            self.slot_cache.capacity(),
            self.next_slot.capacity(),
        );

        // Report S3 operations
        if shard_idx > 0 {
            self.metrics.report_s3_slot_info_write();
            self.metrics.report_s3_tx_shard_write(shard_idx);
        }
    }

    fn index_transactions(&mut self, batch: Vec<JobEffects>, block_unix_timestamp: u64, shred_id: ShredId) {
        if batch.is_empty() {
            return;
        }

        // @chaz: still needed? How do RPC records slots?
        for (job_k, tx) in batch.iter().enumerate() {
            if tx.execution_result.is_err() {
                continue;
            }
            let ordering = TxOrdering::from_shred_id(&shred_id, job_k as u64);
            let sanitized_tx = match tx.sanitized_tx() {
                Ok(sanitized_tx) => sanitized_tx,
                Err(_) => continue,
            };
            let (tx_row, _) = to_tx_row(tx.clone(), &sanitized_tx, shred_id.slot, block_unix_timestamp, ordering);
            // Before first index_block, buffer into next_slot to avoid huge first sort
            if self.current_slot == u64::MAX {
                self.next_slot.push(tx_row);
            } else if shred_id.slot <= self.current_slot {
                self.slot_cache.push(tx_row);
            } else {
                self.next_slot.push(tx_row);
            }
        }

        // Report cache sizes
        self.metrics.report_cache_sizes(
            self.slot_cache.len(),
            self.next_slot.len(),
            self.slot_cache.capacity(),
            self.next_slot.capacity(),
        );

        debug!("MultiDatabaseIndexer: Indexing batch of {} transactions", batch.len());
        self.dispatch_command(IndexerCommand::IndexTransactions {
            batch,
            block_unix_timestamp,
            shred_id,
        });
    }

    fn flush(&mut self) {
        self.flush_all();
    }
}

#[async_trait]
impl<TX: IndexerDB, SLOT: IndexerDB, SIGNATURE: IndexerDB, ACCOUNT: IndexerDB> RpcIndexer
    for MultiDatabaseIndexer<TX, SLOT, SIGNATURE, ACCOUNT>
{
    async fn find_accounts_owned_by(&self, pubkey: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey> {
        // Use round-robin client selection for read operations
        self.clients[0].find_accounts_owned_by(pubkey, limit, offset).await
    }

    async fn find_accounts_by_program(&self, program_id: &Pubkey, limit: usize, offset: usize) -> Vec<Pubkey> {
        // Use round-robin client selection for read operations
        self.clients[0]
            .find_accounts_by_program(program_id, limit, offset)
            .await
    }

    async fn find_token_accounts_by_mint(
        &self,
        program_id: Option<Pubkey>,
        mint: Pubkey,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        // Use round-robin client selection for read operations
        self.clients[0]
            .find_token_accounts_by_mint(program_id, mint, limit, offset)
            .await
    }

    async fn find_token_accounts_owned_by(
        &self,
        owner: &Pubkey,
        program_id: Option<Pubkey>,
        mint: Option<Pubkey>,
        limit: usize,
        offset: usize,
    ) -> Vec<Pubkey> {
        // Use round-robin client selection for read operations
        self.clients[0]
            .find_token_accounts_owned_by(owner, program_id, mint, limit, offset)
            .await
    }

    async fn get_block_with_transactions(
        &self,
        slot: u64,
        offset: u64,
        limit: u64,
    ) -> eyre::Result<Option<BlockWithTransactions>> {
        // Use round-robin client selection for read operations
        self.clients[0].get_block_with_transactions(slot, offset, limit).await
    }

    async fn find_signatures_of_account(
        &self,
        account: &Pubkey,
        filters: infinisvm_types::SignatureFilters,
        limit: usize,
    ) -> eyre::Result<Vec<Signature>> {
        // Use round-robin client selection for read operations
        self.clients[0]
            .find_signatures_of_account(account, filters, limit)
            .await
    }

    async fn get_transaction_with_metadata(
        &self,
        signature: &Signature,
    ) -> eyre::Result<Option<TransactionWithMetadata>> {
        // Use round-robin client selection for read operations
        self.clients[0].get_transaction_with_metadata(signature).await
    }
}

pub struct NoopIndexer;

impl Indexer for NoopIndexer {}

#[async_trait]
impl RpcIndexer for NoopIndexer {
    async fn find_accounts_owned_by(&self, _: &Pubkey, _limit: usize, _offset: usize) -> Vec<Pubkey> {
        error!("NoopIndexer: find_accounts_owned_by");
        vec![]
    }

    async fn find_accounts_by_program(&self, _: &Pubkey, _limit: usize, _offset: usize) -> Vec<Pubkey> {
        error!("NoopIndexer: find_accounts_by_program");
        vec![]
    }

    async fn find_token_accounts_owned_by(
        &self,
        _: &Pubkey,
        _program_id: Option<Pubkey>,
        _mint: Option<Pubkey>,
        _limit: usize,
        _offset: usize,
    ) -> Vec<Pubkey> {
        error!("NoopIndexer: find_token_accounts_owned_by");
        vec![]
    }

    async fn find_token_accounts_by_mint(
        &self,
        _program_id: Option<Pubkey>,
        _mint: Pubkey,
        _limit: usize,
        _offset: usize,
    ) -> Vec<Pubkey> {
        error!("NoopIndexer: find_token_accounts_by_mint");
        vec![]
    }

    async fn get_block_with_transactions(
        &self,
        _slot: u64,
        _offset: u64,
        _limit: u64,
    ) -> eyre::Result<Option<BlockWithTransactions>> {
        error!("NoopIndexer: get_block_with_transactions");
        Ok(None)
    }

    async fn find_signatures_of_account(
        &self,
        _account: &Pubkey,
        _filters: infinisvm_types::SignatureFilters,
        _limit: usize,
    ) -> eyre::Result<Vec<Signature>> {
        error!("NoopIndexer: find_signatures_of_account");
        Ok(vec![])
    }

    async fn get_transaction_with_metadata(
        &self,
        _signature: &Signature,
    ) -> eyre::Result<Option<TransactionWithMetadata>> {
        error!("NoopIndexer: get_transaction_with_metadata");
        Ok(None)
    }
}
