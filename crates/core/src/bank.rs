use std::{
    collections::{BTreeMap, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use agave_feature_set::FeatureSet;
use ahash::{HashSet, HashSetExt};
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashMap;
use hashbrown::HashMap;
use infinisvm_db::{merger, persistence::PersistedInMemoryDB, Database, SlotHashTimestamp};
use infinisvm_logger::{info, warn};
use infinisvm_types::jobs::ConsumedJob;
use metrics::gauge;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use solana_bpf_loader_program::syscalls::{
    create_program_runtime_environment_v1, create_program_runtime_environment_v2,
};
use solana_builtins::BUILTINS;
use solana_compute_budget::compute_budget::ComputeBudget;
use solana_fee::FeeFeatures;
use solana_hash::Hash;
use solana_program_runtime::loaded_programs::ProgramCacheEntry;
use solana_pubkey::Pubkey;
use solana_sdk::{
    account::{Account, AccountSharedData, ReadableAccount, WritableAccount},
    clock::Clock,
    ed25519_program,
    epoch_rewards::EpochRewards,
    epoch_schedule::EpochSchedule,
    fee::{FeeBin, FeeDetails, FeeStructure},
    native_loader,
    rent::Rent,
    secp256k1_program,
    signature::Signature,
    slot_hashes::SlotHashes,
    stake_history::StakeHistory,
    sysvar::{last_restart_slot::LastRestartSlot, SysvarId},
    transaction::{SanitizedTransaction, TransactionError, VersionedTransaction},
    transaction_context::TransactionAccount,
};
use solana_svm::{
    account_loader::{CheckedTransactionDetails, TransactionCheckResult},
    rollback_accounts::RollbackAccounts,
    transaction_processing_callback::TransactionProcessingCallback,
    transaction_processing_result::ProcessedTransaction,
    transaction_processor::{
        LoadAndExecuteSanitizedTransactionsOutput, TransactionBatchProcessor, TransactionProcessingConfig,
        TransactionProcessingEnvironment,
    },
};
use solana_svm_transaction::svm_message::SVMMessage;

use crate::{
    blockhash_generator::DummyRpcBlockhashGenerator, fork_graph::EmptyForkGraph, metrics::BankMetrics,
    subscription::Notifier, wal, SCHEDULER_WORKER_COUNT,
};

pub fn get_feature_set() -> FeatureSet {
    let mut feature_set = FeatureSet::default();
    feature_set.activate(&agave_feature_set::move_precompile_verification_to_svm::id(), 1);
    feature_set
}

#[cfg(feature = "devnet")]
macro_rules! init_v3_program {
    ($self:ident, $program_id_str:expr, $program_buffer_str:expr, $program_upgrade_authority_str:expr) => {{
        // create program account
        let program_id = Pubkey::from_str_const($program_id_str);
        let program_account_data =
            include_bytes!(concat!("../../../bins/genesis-generator/elf/", $program_id_str, ".bin"));
        static_assertions::const_assert!(!include_bytes!(concat!(
            "../../../bins/genesis-generator/elf/",
            $program_id_str,
            ".bin"
        ))
        .is_empty());
        let program_account = {
            let mut account = AccountSharedData::default();
            account.set_lamports(Rent::default().minimum_balance(program_account_data.len()));
            account.set_owner(Pubkey::from_str_const("BPFLoaderUpgradeab1e11111111111111111111111"));
            account.set_executable(true);
            account.set_rent_epoch(u64::MAX);
            account.set_data_from_slice(program_account_data);
            account
        };
        $self.db.write().unwrap().write_account(program_id, program_account);

        // create program buffer account
        let program_buffer = Pubkey::from_str_const($program_buffer_str);

        let program_buffer_data = include_bytes!(concat!(
            "../../../bins/genesis-generator/elf/",
            $program_buffer_str,
            ".bin"
        ));
        static_assertions::const_assert!(!include_bytes!(concat!(
            "../../../bins/genesis-generator/elf/",
            $program_buffer_str,
            ".bin"
        ))
        .is_empty());

        let program_buffer_account = {
            let mut account = AccountSharedData::default();
            account.set_lamports(Rent::default().minimum_balance(program_buffer_data.len()));
            account.set_owner(Pubkey::from_str_const("BPFLoaderUpgradeab1e11111111111111111111111"));
            account.set_executable(false);
            account.set_rent_epoch(u64::MAX);
            account.set_data_from_slice(program_buffer_data);
            account
        };
        $self
            .db
            .write()
            .unwrap()
            .write_account(program_buffer, program_buffer_account);

        // create upgrade authority account (leave it empty)
        let upgrade_authority = Pubkey::from_str_const($program_upgrade_authority_str);
        let upgrade_authority_account = {
            let mut account = AccountSharedData::default();
            account.set_rent_epoch(u64::MAX);
            account
        };
        $self
            .db
            .write()
            .unwrap()
            .write_account(upgrade_authority, upgrade_authority_account);
    }};
}

// PDA of FeezfYUeAv85jB7uFEBcg8TVZDe11umr6FVgUqS9KC4F + worker_id(u8)
pub static FEE_ACCOUNTS: [Pubkey; SCHEDULER_WORKER_COUNT] = [
    Pubkey::from_str_const("FCvMVqjHcUdHybpc4Gw9epWoevw835UuhWvLv3MKtjr8"),
    Pubkey::from_str_const("AXegJad62JQ4mEoZgGNAs54SeEce6bQ7iJ1HY72jqAEV"),
    Pubkey::from_str_const("Gew43KABMFtRwX96iAThs8r9MAuEcdhCGQoQUQy641Xy"),
    Pubkey::from_str_const("E2N1C4eDVrkR56yJVJYuZcGcDbJ1fy7i59NS6Yirc6DB"),
    Pubkey::from_str_const("CMAazeiF2ssbFaPEcjhYhPQtkCLcGyaXkM9C18tBFP2N"),
    Pubkey::from_str_const("EujQFYnwMuGvdeKm5fHztHJYg4TJrGB3Whjy7uy8Pttb"),
    Pubkey::from_str_const("JDo2i8b11ETQiAvjHS1qChHczjaYN2SDJ7z8ZVKAow28"),
    Pubkey::from_str_const("5HUvbRyDyQm6MK2bxKc9W6cA3griE2H2Pz34v959KEBe"),
    Pubkey::from_str_const("Ce5dqkNN171ZfAHJQ7WpDK3zF9ha9sKSzjqXpnpmQB4S"),
    Pubkey::from_str_const("CE7KJLAJ3soio7QsVNPHWWa7PqkwUViDfK2HCQZhE5ga"),
    Pubkey::from_str_const("EmEzBFMnFXEBNRAaTdqXRxaVX8TLZg3eFTpKfEaaikm6"),
    Pubkey::from_str_const("4G2dckVTkzVR5RBWRrpaNFbLqpcDs8zGEYCTsc9mdUvN"),
];

/*
when executing we don't care about account version

life cycle of normal tx:
  1. (scheduler) received from somewhere
  2. (scheduler) put into scheduler buffer
  3. (scheduler) wait some time and then a batch is created
  4. (scheduler) lock accounts and send to worker
  5. (worker)    batch is executed
  6. (worker)    send back to scheduler to unlock accounts
  7. (committer) for every tx, check version
  8. (committer) if good, commit (status cache + versioned db). if not send back to scheduler

life cycle of tx from pre-exec:
  1. (committer) check account versions
  2. (committer) if good, commit (status cache + versioned db). if not send back to scheduler (note: we have a trust assumption here)
*/

#[derive(Debug, Clone)]
pub enum TransactionStatus {
    Executing,
    Executed(Option<TransactionError>, u64),
}

pub struct RawSlot {
    pub slot: u64,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub timestamp: u64,
    pub job_ids: Vec<u64>,
}

const JOB_ID_RETENTION_SLOTS: u64 = 512;

#[derive(Clone, Copy)]
struct SlotMeta {
    hash: Hash,
    parent_hash: Hash,
    timestamp: u64,
}

pub struct Bank {
    db: Arc<RwLock<dyn Database>>,
    status_cache: Arc<DashMap<Signature, TransactionStatus>>,

    blockhash_pruner_sender: Sender<(Hash, HashSet<Signature>)>,
    blockhash_signature_map: DashMap<Hash, HashSet<Signature>>,

    previous_hashes: Arc<RwLock<VecDeque<Hash>>>, // 150 valid hashes

    slot_hash_timestamp: SlotHashTimestamp,
    hash_generator: DummyRpcBlockhashGenerator,

    new_block_sender: Option<Sender<(u64, u64, Hash, Hash)>>,
    raw_slot_sender: Option<Sender<RawSlot>>,
    // A container for storing the consumed jobs in current slot
    transactions: Vec<VersionedTransaction>,

    transaction_processor: TransactionBatchProcessor<EmptyForkGraph>,
    _feature_set: Arc<FeatureSet>,
    _fork_graph: Arc<RwLock<EmptyForkGraph>>,
    fee_structure: FeeStructure,

    subscription_processor: Option<Arc<dyn Notifier>>,
    slot_job_ids: BTreeMap<u64, Vec<u64>>,
    emitted_slot_meta: BTreeMap<u64, SlotMeta>,
    metrics: BankMetrics,
}

unsafe impl Send for Bank {}
unsafe impl Sync for Bank {}


impl Bank {
    pub fn set_db(&mut self, db: Arc<RwLock<dyn Database>>) {
        self.db = db;
    }

    pub fn add_subscriber(&mut self, subscriber: Arc<dyn Notifier>) {
        self.subscription_processor = Some(subscriber);
    }

    pub fn shutdown_streams(&mut self) {
        // Drop outbound slot publishers so downstream listeners can exit cleanly.
        self.new_block_sender.take();
        self.raw_slot_sender.take();
    }

    pub fn new_slave(exit: Arc<AtomicBool>) -> Self {
        let hash_generator = DummyRpcBlockhashGenerator::new();
        let (blockhash_pruner_sender, blockhash_pruner_receiver) = crossbeam_channel::unbounded();
        let status_cache = Arc::new(DashMap::with_capacity(42_000_000));

        let status_cache_clone = status_cache.clone();
        std::thread::Builder::new()
            .name("blockhashPruner".to_string())
            .spawn(move || {
                Bank::pruner_thread(blockhash_pruner_receiver, status_cache_clone, exit);
            })
            .unwrap();

        let fork_graph = Arc::new(RwLock::new(EmptyForkGraph));
        let pdb = PersistedInMemoryDB::default();
        let (slot, hash, timestamp) = (0, Hash::default(), 0);

        let transaction_processor = TransactionBatchProcessor::new_uninitialized(slot, 0);
        let feature_set = Arc::new(get_feature_set());
        transaction_processor
            .program_cache
            .write()
            .unwrap()
            .set_fork_graph(Arc::downgrade(&fork_graph));

        transaction_processor
            .program_cache
            .write()
            .unwrap()
            .environments
            .program_runtime_v1 = Arc::new(
            create_program_runtime_environment_v1(
                &feature_set,
                &ComputeBudget::default(),
                false, /* deployment */
                false, /* debugging_features */
            )
            .unwrap(),
        );

        transaction_processor
            .program_cache
            .write()
            .unwrap()
            .environments
            .program_runtime_v2 = Arc::new(create_program_runtime_environment_v2(
            &ComputeBudget::default(),
            false, /* debugging_features */
        ));

        // added: clock, rent
        // todo:  epoch_schedule, epoch_rewards, slot_hashes, stake_history,
        // last_restart_slot may 20: we may not need epoch_schedule,
        // epoch_rewards, stake_history         but we may need slot_hashes,
        // last_restart_slot
        let sysvar_setter = |pubkey: &Pubkey, callback: &mut dyn FnMut(&[u8])| {
            if pubkey == &Clock::id() {
                let clock = Clock {
                    slot,
                    epoch: 0,
                    epoch_start_timestamp: 0,
                    leader_schedule_epoch: 0,
                    unix_timestamp: timestamp as i64,
                };
                callback(&bincode::serialize(&clock).unwrap());
            } else if pubkey == &EpochSchedule::id() {
                let epoch_schedule = EpochSchedule::default();
                callback(&bincode::serialize(&epoch_schedule).unwrap());
            } else if pubkey == &EpochRewards::id() {
                let epoch_rewards = EpochRewards::default();
                callback(&bincode::serialize(&epoch_rewards).unwrap());
            } else if pubkey == &Rent::id() {
                let rent = Rent::default();
                callback(&bincode::serialize(&rent).unwrap());
            } else if pubkey == &SlotHashes::id() {
                let slot_hashes = SlotHashes::default();
                callback(&bincode::serialize(&slot_hashes).unwrap());
            } else if pubkey == &StakeHistory::id() {
                let stake_history = StakeHistory::default();
                callback(&bincode::serialize(&stake_history).unwrap());
            } else if pubkey == &LastRestartSlot::id() {
                let last_restart_slot = LastRestartSlot::default();
                callback(&bincode::serialize(&last_restart_slot).unwrap());
            }
        };

        transaction_processor
            .sysvar_cache_mut()
            .fill_missing_entries(sysvar_setter);

        let bank = Self {
            db: Arc::new(RwLock::new(pdb)),
            // 350000 tps, expire every 150 slots
            // we need to store 21000000 txs
            blockhash_signature_map: DashMap::with_capacity(150),
            previous_hashes: Arc::new(RwLock::new(VecDeque::with_capacity(150))),
            slot_hash_timestamp: (slot, hash, timestamp),
            hash_generator,
            new_block_sender: None,
            raw_slot_sender: None,
            transactions: vec![],

            blockhash_pruner_sender,
            status_cache,
            transaction_processor,
            _feature_set: feature_set,
            _fork_graph: fork_graph,
            fee_structure: FeeStructure {
                lamports_per_signature: 5000,
                lamports_per_write_lock: 0,
                compute_fee_bins: vec![FeeBin { limit: 1400000, fee: 0 }],
            },
            subscription_processor: None,
            slot_job_ids: BTreeMap::new(),
            emitted_slot_meta: BTreeMap::new(),
            metrics: BankMetrics::default(),
        };

        for builtin in BUILTINS {
            bank.transaction_processor.add_builtin(
                &bank,
                builtin.program_id,
                builtin.name,
                ProgramCacheEntry::new_builtin(0, builtin.name.len(), builtin.entrypoint),
            );
        }

        // no genesis map for slave
        bank.init_hardcoded_accounts(std::collections::HashMap::new());

        bank
    }

    // instead of using TransactionProcessingEnvironment that introduces a lifetime,
    // store the fields separately
    pub fn get_transaction_processing_environment(&self) -> TransactionProcessingEnvironment {
        TransactionProcessingEnvironment {
            blockhash: self.current_blockhash(),
            epoch_total_stake: 0,
            feature_set: Arc::new(get_feature_set()),
            blockhash_lamports_per_signature: self.fee_structure.lamports_per_signature,
            fee_lamports_per_signature: 5000,
            rent_collector: None,
        }
    }

    pub fn get_latest_slot_hash_timestamp(&self) -> SlotHashTimestamp {
        self.slot_hash_timestamp
    }

    pub fn write_status_cache(&self, signature: &Signature, status: TransactionStatus) {
        self.status_cache.insert(*signature, status);
    }

    pub fn check_results(&self, sanitized_txs: &[SanitizedTransaction], simulate: bool) -> Vec<TransactionCheckResult> {
        let mut results = Vec::with_capacity(sanitized_txs.len());

        for tx in sanitized_txs {
            if !self.is_tx_blockhash_valid(tx) {
                self.metrics.increment_total_tx_expired();
                results.push(Err(TransactionError::BlockhashNotFound));
                continue;
            }

            if self.is_tx_processed(tx) {
                self.metrics.increment_total_tx_duplicate();
                results.push(Err(TransactionError::AlreadyProcessed));
                continue;
            }

            // Only insert into the status cache if under sequencer mode
            if !simulate
            /* && self.raw_slot_sender.is_some() */
            {
                // set status cache
                self.status_cache.insert(*tx.signature(), TransactionStatus::Executing);
                if let Some(subscription_processor) = self.subscription_processor.as_ref() {
                    subscription_processor.notify_signature_update(tx.signature(), &TransactionStatus::Executing);
                }
                match self.blockhash_signature_map.get_mut(tx.message().recent_blockhash()) {
                    Some(mut signatures) => {
                        signatures.insert(*tx.signature());
                    }
                    None => {
                        unreachable!();
                    }
                }
            }

            results.push(Ok(CheckedTransactionDetails::new(None, 5000)));
        }

        self.metrics.increase_total_tx_checked(sanitized_txs.len() as u64);

        results
    }

    pub fn is_tx_blockhash_valid(&self, tx: &SanitizedTransaction) -> bool {
        let blockhash = tx.message().recent_blockhash();
        self.is_blockhash_valid(blockhash)
    }

    pub fn is_blockhash_valid(&self, blockhash: &Hash) -> bool {
        self.previous_hashes.read().unwrap().contains(blockhash)
    }

    pub fn is_tx_processed(&self, tx: &SanitizedTransaction) -> bool {
        let signature = tx.signature();
        self.status_cache.contains_key(signature)
    }

    pub fn get_tx_status(&self, signature: &Signature) -> Option<TransactionStatus> {
        self.status_cache.get(signature).map(|r| r.clone())
    }

    pub fn register_job_id(&mut self, slot: u64, job_id: u64) {
        let mut needs_resend = false;
        {
            let entry = self.slot_job_ids.entry(slot).or_default();
            match entry.binary_search(&job_id) {
                Ok(_) => {}
                Err(pos) => {
                    entry.insert(pos, job_id);
                    if slot < self.slot_hash_timestamp.0 {
                        needs_resend = true;
                    }
                }
            }
        }

        if needs_resend {
            self.resend_slot(slot);
        }
    }

    fn resend_slot(&mut self, slot: u64) {
        let Some(raw_slot_sender) = self.raw_slot_sender.as_ref() else {
            return;
        };

        let Some(meta) = self.emitted_slot_meta.get(&slot).copied() else {
            warn!(
                "Late job_id registered for slot {} but slot metadata no longer retained; skipping resend",
                slot
            );
            return;
        };

        let Some(job_ids) = self.slot_job_ids.get_mut(&slot) else {
            warn!(
                "Late job_id registered for slot {} but slot entry missing; skipping resend",
                slot
            );
            return;
        };

        job_ids.sort_unstable();
        job_ids.dedup();
        let raw_slot = RawSlot {
            slot,
            hash: meta.hash,
            parent_hash: meta.parent_hash,
            timestamp: meta.timestamp,
            job_ids: job_ids.clone(),
        };

        if let Err(err) = raw_slot_sender.send(raw_slot) {
            warn!(
                "Failed to resend raw slot {} after late job registration: {}",
                slot, err
            );
        } else {
            info!(
                "Resent raw slot {} with {} job IDs after late registration",
                slot,
                job_ids.len()
            );
        }
    }

    fn prune_slot_history(&mut self, current_slot: u64) {
        let min_slot = current_slot.saturating_sub(JOB_ID_RETENTION_SLOTS);

        let slots_to_remove: Vec<u64> = self
            .emitted_slot_meta
            .keys()
            .copied()
            .filter(|slot| *slot < min_slot)
            .collect();

        for slot in slots_to_remove {
            self.emitted_slot_meta.remove(&slot);
            self.slot_job_ids.remove(&slot);
        }
    }

    pub fn tick(&mut self) {
        let (slot, blockhash, timestamp) = self.slot_hash_timestamp;
        let parent_hash = self.last_blockhash().unwrap_or_default();

        if let Some(new_block_sender) = self.new_block_sender.as_ref() {
            let _ = new_block_sender.send((slot, timestamp, blockhash, parent_hash));
        }

        let _transactions = std::mem::take(&mut self.transactions);

        if let Some(raw_slot_sender) = self.raw_slot_sender.as_ref() {
            let job_ids = {
                let ids = self.slot_job_ids.entry(slot).or_default();
                ids.sort_unstable();
                ids.dedup();
                ids.clone()
            };

            let raw_slot = RawSlot {
                slot,
                hash: blockhash,
                parent_hash,
                timestamp,
                job_ids,
            };

            if let Err(err) = raw_slot_sender.send(raw_slot) {
                warn!("Failed to publish raw slot {} to subscribers: {}", slot, err);
            }

            self.emitted_slot_meta.insert(
                slot,
                SlotMeta {
                    hash: blockhash,
                    parent_hash,
                    timestamp,
                },
            );
            self.prune_slot_history(slot);
        }

        let unix_timestamp = std::time::UNIX_EPOCH.elapsed().unwrap().as_secs();
        self.slot_hash_timestamp = (
            self.slot_hash_timestamp.0 + 1,
            self.hash_generator.next(),
            // todo: hash relates to tx
            // may 20: do this when adding consensus
            unix_timestamp,
        );

        self.db.write().unwrap().commit(slot);

        #[cfg(feature = "devnet")]
        self.init_accounts_for_test(
            &Pubkey::from_str_const("FUND4EFuH8XaPmFkFLvABVQzfBZ2GQ7grYHhWV6ZYTQm"),
            false,
        );

        self.post_tick(slot, unix_timestamp, true);
    }

    pub fn commit_blockhash_to_signatures(&mut self, blockhash_to_signatures: HashMap<Hash, Vec<Signature>>) {
        for (bh, signatures) in blockhash_to_signatures.into_iter() {
            self.blockhash_signature_map
                .entry(bh)
                .or_insert_with(|| HashSet::with_capacity(200_000))
                .extend(signatures.into_iter());
        }
    }

    pub fn tick_as_slave(&mut self, slot_data: &RawSlot) {
        // Advance to the provided slot/hash/timestamp from the sequencer
        self.slot_hash_timestamp = (slot_data.slot, slot_data.hash, slot_data.timestamp);
        // Update blockhash window and Clock/sysvars for the current slot (not the
        // previous)
        self.post_tick(slot_data.slot, slot_data.timestamp, true);
    }

    pub fn commit_changes(&mut self, db_changes: Vec<(Pubkey, AccountSharedData)>) {
        let mut db = self.db.write().unwrap();
        db.commit_changes_raw(db_changes);
    }

    /// Set the status of the bank after tick moved forward, pass in previous
    /// slot and timestamp
    fn post_tick(&mut self, slot: u64, unix_timestamp: u64, update_sysvar: bool) {
        self.previous_hashes
            .write()
            .unwrap()
            .push_back(self.slot_hash_timestamp.1);
        self.blockhash_signature_map
            .insert(self.slot_hash_timestamp.1, HashSet::with_capacity(200_000));

        if self.previous_hashes.read().unwrap().len() > 150 {
            if let Some(blockhash) = self.previous_hashes.write().unwrap().pop_front() {
                match self.blockhash_signature_map.remove(&blockhash) {
                    Some((bh, signatures)) => {
                        let _ = self.blockhash_pruner_sender.send((bh, signatures));
                    }
                    None => {
                        warn!("blockhash missing in signature map during prune: {:?}", blockhash);
                    }
                }
                self.blockhash_signature_map.shrink_to_fit();
            }
        }

        let clock = Clock {
            slot,
            epoch: 0,
            epoch_start_timestamp: 0,
            leader_schedule_epoch: 0,
            unix_timestamp: unix_timestamp as i64,
        };

        if update_sysvar {
            let clock_data = bincode::serialize(&clock).unwrap();
            self.db.write().unwrap().write_account(
                Clock::id(),
                AccountSharedData::from(Account {
                    lamports: Rent::default().minimum_balance(clock_data.len()),
                    data: clock_data,
                    owner: Pubkey::from_str_const("Sysvar1111111111111111111111111111111111111"),
                    executable: false,
                    rent_epoch: u64::MAX,
                }),
            );
        }

        self.transaction_processor
            .sysvar_cache_mut()
            .set_sysvar_for_tests(&clock);

        // self.transaction_processor.increment_slot();
        self.transaction_processor.set_slot(slot);
        self.transaction_processor
            .program_cache
            .write()
            .unwrap()
            .latest_root_slot = slot;

        // todo: SysvarS1otHashes111111111111111111111111111

        info!("new slot: {:?}", self.slot_hash_timestamp);
        self.metrics.report();
    }

    pub fn pruner_thread(
        receiver: Receiver<(Hash, HashSet<Signature>)>,
        status_cache: Arc<DashMap<Signature, TransactionStatus>>,
        exit: Arc<AtomicBool>,
    ) {
        let ticker = crossbeam_channel::tick(Duration::from_secs(60));

        while !exit.load(Ordering::Relaxed) {
            if ticker.try_recv().is_ok() {
                gauge!("status_cache_size").set(status_cache.len() as f64);
                status_cache.shrink_to_fit();
            }

            // Process all currently available messages from the receiver without blocking
            while let Ok((blockhash, signatures)) = receiver.try_recv() {
                info!("Pruning {} signatures for blockhash {:?}", signatures.len(), blockhash);
                signatures.into_par_iter().for_each(|signature| {
                    status_cache.remove(&signature);
                });
            }
        }
    }

    pub fn current_blockhash(&self) -> Hash {
        self.slot_hash_timestamp.1
    }

    pub fn last_blockhash(&self) -> Option<Hash> {
        self.previous_hashes.read().unwrap().back().cloned()
    }

    pub fn recent_blockhashes(&self) -> VecDeque<Hash> {
        self.previous_hashes.read().unwrap().clone()
    }

    pub fn blockhash_ref(&self) -> Arc<RwLock<VecDeque<Hash>>> {
        self.previous_hashes.clone()
    }

    #[cfg(feature = "devnet")]
    pub fn init_bench_shared_accounts(&self) {
        let layer_id = Pubkey::from_str_const("LAYER4xPpTCb3QL8S9u41EAhAX7mhBn8Q6xMTwY2Yzc");
        let layer_account = {
            let mut account = AccountSharedData::default();
            let account_data =
                include_bytes!("../../../bins/genesis-generator/elf/LAYER4xPpTCb3QL8S9u41EAhAX7mhBn8Q6xMTwY2Yzc.bin");
            account.set_lamports(Rent::default().minimum_balance(account_data.len()));
            account.set_owner(Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"));
            account.set_executable(false);
            account.set_rent_epoch(u64::MAX);
            account.set_data_from_slice(account_data);
            account
        };
        self.db.write().unwrap().write_account(layer_id, layer_account);

        init_v3_program!(
            self,
            "rp7km3qAmYb8ciKKS23v5nmyYU9dFTc5RTAyx7zQSAz",
            "4WzoXzrZBidLu1MTj26c1iyLBd7LN3Sj5HkugJ1AKVxw",
            "SahScoe6eHCbC4a8M6BPp27bHqFVaQiDPqYpFeDCwFb"
        );
    }

    #[cfg(feature = "devnet")]
    pub fn init_accounts_for_test(&self, pubkey: &Pubkey, with_token: bool) {
        let mut default_account = AccountSharedData::default();
        default_account.set_lamports(1_000_000_000_000_000); // 1000000 SOL
        self.db.write().unwrap().write_account(*pubkey, default_account);
        if !with_token {
            return;
        }

        // generate test mint ata
        let (test_mint_ata, _) = Pubkey::find_program_address(
            &[
                pubkey.to_bytes().as_ref(),
                Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
                    .to_bytes()
                    .as_ref(),
                Pubkey::from_str_const("LAYER4xPpTCb3QL8S9u41EAhAX7mhBn8Q6xMTwY2Yzc")
                    .to_bytes()
                    .as_ref(),
            ],
            &Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"),
        );

        let mut layer_account = AccountSharedData::default();
        let mut layer_account_data: Vec<u8> = vec![0; 165];
        layer_account_data[0..32]
            .copy_from_slice(&Pubkey::from_str_const("LAYER4xPpTCb3QL8S9u41EAhAX7mhBn8Q6xMTwY2Yzc").to_bytes());
        layer_account_data[32..64].copy_from_slice(&pubkey.to_bytes());
        let amount: u64 = 100_000_000_000_000; // 100000 LAYER
        layer_account_data[64..72].copy_from_slice(&amount.to_le_bytes());
        layer_account_data[72..165].copy_from_slice(&[0; 93]);
        // 108: 01
        layer_account_data[108] = 1;
        layer_account.set_lamports(100_000_000_000); // 1 SOL
        layer_account.set_owner(Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"));
        layer_account.set_executable(false);
        layer_account.set_rent_epoch(u64::MAX);
        layer_account.set_data_from_slice(&layer_account_data);
        self.db.write().unwrap().write_account(test_mint_ata, layer_account);

        // generate wsol ata
        let (wsol_ata, _) = Pubkey::find_program_address(
            &[
                pubkey.to_bytes().as_ref(),
                Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
                    .to_bytes()
                    .as_ref(),
                Pubkey::from_str_const("So11111111111111111111111111111111111111112")
                    .to_bytes()
                    .as_ref(),
            ],
            &Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"),
        );

        let mut wsol_account: AccountSharedData = AccountSharedData::default();
        let mut wsol_account_data: Vec<u8> = vec![0; 165];
        wsol_account_data[0..32]
            .copy_from_slice(&Pubkey::from_str_const("So11111111111111111111111111111111111111112").to_bytes());
        wsol_account_data[32..64].copy_from_slice(&pubkey.to_bytes());
        let amount: u64 = 100_000_000_000_000; // 100000 WSOL
        wsol_account_data[64..72].copy_from_slice(&amount.to_le_bytes());
        //108-116 01 01 00 00 00 f0 1d 1f
        wsol_account_data[72..108].copy_from_slice(&[0; 36]);
        wsol_account_data[108..116].copy_from_slice(&[1, 1, 0, 0, 0, 240, 29, 31]);
        wsol_account_data[116..165].copy_from_slice(&[0; 49]);
        wsol_account.set_lamports(100_000_000_000_000); // 100000 SOL
        wsol_account.set_owner(Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"));
        wsol_account.set_executable(false);
        wsol_account.set_rent_epoch(u64::MAX);
        wsol_account.set_data_from_slice(&wsol_account_data);
        self.db.write().unwrap().write_account(wsol_ata, wsol_account);
    }

    fn init_hardcoded_accounts(&self, genesis_map: std::collections::HashMap<String, AccountSharedData>) {
        self.add_precompile(&secp256k1_program::id(), b"secp256k1_program".to_vec());
        self.add_precompile(&ed25519_program::id(), b"ed25519_program".to_vec());

        // disabled since solana also disabled
        // self.add_precompile(&secp256r1_program::id(), "secp256r1_program");

        let default_rent = Rent::default();
        let default_rent_data = bincode::serialize(&default_rent).unwrap();
        self.db.write().unwrap().write_account(
            Pubkey::from_str_const("SysvarRent111111111111111111111111111111111"),
            AccountSharedData::from(Account {
                lamports: default_rent.minimum_balance(default_rent_data.len()),
                data: default_rent_data,
                owner: Pubkey::from_str_const("11111111111111111111111111111111"),
                executable: false,
                rent_epoch: u64::MAX,
            }),
        );

        for (key, value) in genesis_map {
            self.db
                .write()
                .unwrap()
                .write_account(Pubkey::from_str(&key).unwrap(), value);
        }
    }

    fn add_precompile(&self, program_id: &Pubkey, data: Vec<u8>) {
        self.add_precompiled_account_with_owner(program_id, native_loader::id(), data)
    }

    fn add_precompiled_account_with_owner(&self, program_id: &Pubkey, owner: Pubkey, data: Vec<u8>) {
        if let Some(account) = self.get_account_shared_data(program_id) {
            if account.executable() {
                return;
            }
        };

        let account = AccountSharedData::from(Account {
            lamports: 1,
            owner,
            data,
            executable: true,
            rent_epoch: u64::MAX,
        });
        self.db.write().unwrap().write_account(*program_id, account);
    }

    pub fn commit_transactions(&mut self, consumed_jobs: &[ConsumedJob], worker_id: usize) {
        let mut total_fees = 0;
        let mut accounts_changed = HashMap::with_capacity(consumed_jobs.len() * 64);

        for consumed_job in consumed_jobs {
            let ConsumedJob {
                processed_transaction: processing_result,
                sanitized_transaction: tx,
                slot,
                ..
            } = consumed_job;

            if processing_result.is_err() {
                // load error is not execution error
                continue;
            }

            let processing_result = processing_result.as_ref().unwrap();

            total_fees += processing_result.fee_details().total_fee();

            // two things to store/do:
            // 1. account data
            // 2. program cache entry

            // https://github.com/solayer-labs/agave/blob/d8f33ca6a339cf3ebff6ae541e4b9c6888d7e694/runtime/src/bank.rs#L3947

            // find all accounts modified by tx
            let collect_capacity = match &processing_result {
                ProcessedTransaction::Executed(executed_tx) => match executed_tx.execution_details.status {
                    Ok(_) => tx.message().num_write_locks() as usize,
                    Err(_) => executed_tx.loaded_transaction.rollback_accounts.count(),
                },
                ProcessedTransaction::FeesOnly(fees_only_tx) => fees_only_tx.rollback_accounts.count(),
            };
            let mut accounts = Vec::with_capacity(collect_capacity);

            match &processing_result {
                ProcessedTransaction::Executed(executed_transaction) => {
                    if executed_transaction.execution_details.status.is_ok() {
                        collect_accounts_for_successful_tx(
                            &mut accounts,
                            tx,
                            &executed_transaction.loaded_transaction.accounts,
                        );
                    } else {
                        collect_accounts_for_failed_tx(
                            &mut accounts,
                            tx,
                            &executed_transaction.loaded_transaction.rollback_accounts,
                        )
                    }
                }
                ProcessedTransaction::FeesOnly(fees_only_transaction) => {
                    collect_accounts_for_failed_tx(&mut accounts, tx, &fees_only_transaction.rollback_accounts)
                }
            }

            // store accounts
            for (address, account) in accounts {
                accounts_changed.insert(*address, account);
            }

            // do we need to store program cache?
            {
                let mut cache = None;
                if let ProcessedTransaction::Executed(executed_tx) = processing_result {
                    let programs_modified_by_tx = &executed_tx.programs_modified_by_tx;
                    if executed_tx.was_successful() && !programs_modified_by_tx.is_empty() {
                        cache
                            .get_or_insert_with(|| self.transaction_processor.program_cache.write().unwrap())
                            .merge(programs_modified_by_tx);
                    }
                }
            }

            // set status cache
            let signature = tx.signature();
            let status = match processing_result {
                ProcessedTransaction::Executed(executed_tx) => {
                    TransactionStatus::Executed(executed_tx.execution_details.status.clone().err(), *slot)
                }
                ProcessedTransaction::FeesOnly(fees_only_tx) => {
                    TransactionStatus::Executed(Some(fees_only_tx.load_error.clone()), *slot)
                }
            };

            if let Some(subscription_processor) = self.subscription_processor.as_ref() {
                subscription_processor.notify_signature_update(signature, &status);
            }
            self.status_cache.insert(*signature, status);
        }

        let mut db: std::sync::RwLockWriteGuard<'_, dyn Database> = self.db.write().unwrap();
        for (address, account) in accounts_changed.into_iter() {
            db.write_account(address, account.clone());
        }

        let fee_account = FEE_ACCOUNTS[worker_id];
        let mut fee_account_data = match db.get_account(fee_account) {
            Ok(Some(account)) => account,
            _ => AccountSharedData::default(),
        };
        fee_account_data.checked_add_lamports(total_fees).unwrap();
        db.write_account(fee_account, fee_account_data);

        self.transactions.extend(
            consumed_jobs
                .iter()
                .map(|job| job.sanitized_transaction.to_versioned_transaction()),
        );
    }

    pub fn db_cloned(&self) -> Arc<RwLock<dyn Database>> {
        self.db.clone()
    }

    pub fn get_current_slot(&self) -> u64 {
        self.slot_hash_timestamp.0
    }

    pub fn get_transaction_processor(&self) -> &TransactionBatchProcessor<EmptyForkGraph> {
        &self.transaction_processor
    }

    pub fn simulate_transaction(
        &self,
        tx: SanitizedTransaction,
    ) -> Result<(ProcessedTransaction, Vec<(Pubkey, AccountSharedData)>), TransactionError> {
        let tx_arr = vec![tx];
        let check_results = self.check_results(&tx_arr, true);
        let LoadAndExecuteSanitizedTransactionsOutput {
            error_metrics: _,
            execute_timings: _,
            processing_results,
        } = {
            self.transaction_processor.load_and_execute_sanitized_transactions(
                self,
                &tx_arr,
                check_results,
                &self.get_transaction_processing_environment(),
                &TransactionProcessingConfig::default(),
            )
        };

        let mut accounts = Vec::new();

        let processing_result = processing_results.into_iter().take(1).next().unwrap()?;
        let tx = tx_arr.into_iter().take(1).next().unwrap();

        match &processing_result {
            ProcessedTransaction::Executed(executed_transaction) => {
                if executed_transaction.execution_details.status.is_ok() {
                    collect_accounts_for_successful_tx(
                        &mut accounts,
                        &tx,
                        &executed_transaction.loaded_transaction.accounts,
                    );
                } else {
                    collect_accounts_for_failed_tx(
                        &mut accounts,
                        &tx,
                        &executed_transaction.loaded_transaction.rollback_accounts,
                    )
                }
            }
            ProcessedTransaction::FeesOnly(fees_only_transaction) => {
                collect_accounts_for_failed_tx(&mut accounts, &tx, &fees_only_transaction.rollback_accounts)
            }
        }

        let accounts_changed = accounts
            .into_iter()
            .map(|(address, account)| (*address, account.clone()))
            .collect();

        Ok((processing_result, accounts_changed))
    }

    pub fn get_account_shared_data_public(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.db.read().unwrap().get_account(*pubkey).ok()?
    }
}

fn collect_accounts_for_failed_tx<'a>(
    collected_accounts: &mut Vec<(&'a Pubkey, &'a AccountSharedData)>,
    transaction: &'a SanitizedTransaction,
    rollback_accounts: &'a RollbackAccounts,
) {
    let fee_payer_address = transaction.message().fee_payer();
    match rollback_accounts {
        RollbackAccounts::FeePayerOnly { fee_payer_account } => {
            collected_accounts.push((fee_payer_address, fee_payer_account));
        }
        RollbackAccounts::SameNonceAndFeePayer { nonce } => {
            collected_accounts.push((nonce.address(), nonce.account()));
        }
        RollbackAccounts::SeparateNonceAndFeePayer {
            nonce,
            fee_payer_account,
        } => {
            collected_accounts.push((fee_payer_address, fee_payer_account));

            collected_accounts.push((nonce.address(), nonce.account()));
        }
    }
}

fn collect_accounts_for_successful_tx<'a>(
    collected_accounts: &mut Vec<(&'a Pubkey, &'a AccountSharedData)>,
    transaction: &'a SanitizedTransaction,
    transaction_accounts: &'a [TransactionAccount],
) {
    for (i, (address, account)) in (0..transaction.message().account_keys().len()).zip(transaction_accounts) {
        if !transaction.message().is_writable(i) {
            continue;
        }

        // Accounts that are invoked and also not passed as an instruction
        // account to a program don't need to be stored because it's assumed
        // to be impossible for a committable transaction to modify an
        // invoked account if said account isn't passed to some program.
        if transaction.message().is_invoked(i) && !transaction.message().is_instruction_account(i) {
            continue;
        }

        collected_accounts.push((address, account));
    }
}

impl TransactionProcessingCallback for Bank {
    fn account_matches_owners(&self, account: &Pubkey, owners: &[Pubkey]) -> Option<usize> {
        self.get_account_shared_data(account)
            .and_then(|account| owners.iter().position(|key| account.owner().eq(key)))
    }

    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.db.read().unwrap().get_account(*pubkey).ok()?
    }

    fn add_builtin_account(&self, name: &str, program_id: &Pubkey) {
        let existing_genuine_program = self.get_account_shared_data(program_id).and_then(|account| {
            if native_loader::check_id(account.owner()) {
                Some(account)
            } else {
                None
            }
        });

        if existing_genuine_program.is_some() {
            return;
        }

        let account = native_loader::create_loadable_account_with_fields(name, (1, 0));
        self.db.write().unwrap().write_account(*program_id, account);
    }

    fn calculate_fee(
        &self,
        message: &impl SVMMessage,
        lamports_per_signature: u64,
        prioritization_fee: u64,
        _feature_set: &FeatureSet,
    ) -> FeeDetails {
        solana_fee::calculate_fee_details(
            message,
            false, /* zero_fees_for_test */
            lamports_per_signature,
            prioritization_fee,
            FeeFeatures {
                enable_secp256r1_precompile: false,
            },
        )
    }
}
