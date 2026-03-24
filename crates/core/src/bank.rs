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
use infinisvm_db::{persistence::PersistedInMemoryDB, Database, SlotHashTimestamp};
use infinisvm_logger::{info, warn};
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
    account::{Account, AccountSharedData, ReadableAccount},
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
    transaction::{SanitizedTransaction, TransactionError},
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
    subscription::Notifier, BLOCKHASH_HISTORY_SLOTS, BLOCKHASH_SIGNATURE_SET_CAPACITY,
};

pub fn get_feature_set() -> FeatureSet {
    let mut feature_set = FeatureSet::default();
    feature_set.activate(&agave_feature_set::move_precompile_verification_to_svm::id(), 1);
    feature_set
}

#[cfg(feature = "devnet")]

macro_rules! init_v3_program {
    ($self:ident, $program_address_str:expr, $program_buffer_address_str:expr, $deployment_slot:expr, $program_upgrade_authority_address_str:expr) => {{
        // create program account and point to program buffer
        let program_id = Pubkey::from_str_const($program_address_str);
        let program_buffer = Pubkey::from_str_const($program_buffer_address_str);
        // 2u32 .. program buffer address
        let mut program_account_data = 2u32.to_le_bytes().to_vec();
        program_account_data.extend_from_slice(program_buffer.to_bytes().as_ref());
        let program_account = {
            let mut account = AccountSharedData::default();
            account.set_lamports(Rent::default().minimum_balance(program_account_data.len()));
            account.set_owner(Pubkey::from_str_const("BPFLoaderUpgradeab1e11111111111111111111111"));
            account.set_executable(true);
            account.set_rent_epoch(u64::MAX);
            account.set_data_from_slice(&program_account_data);
            account
        };
        $self.db.write().unwrap().write_account(program_id, program_account);

        // create program buffer account
        // 3u32 .. u64 deployment slot .. 1u8 .. upgrade authority address .. elf bytes
        let mut program_buffer_data = 3u32.to_le_bytes().to_vec();
        let deployment_slot: u64 = $deployment_slot;
        program_buffer_data.extend_from_slice(&deployment_slot.to_le_bytes());
        program_buffer_data.extend_from_slice(&1u8.to_le_bytes());
        program_buffer_data.extend_from_slice(
            Pubkey::from_str_const($program_upgrade_authority_address_str)
                .to_bytes()
                .as_ref(),
        );
        let elf_bytes = include_bytes!(concat!(
            "../../../bins/genesis-generator/elf/",
            $program_address_str,
            ".so"
        ));
        static_assertions::const_assert!(!include_bytes!(concat!(
            "../../../bins/genesis-generator/elf/",
            $program_address_str,
            ".so"
        ))
        .is_empty());
        program_buffer_data.extend_from_slice(elf_bytes);

        let program_buffer_account = {
            let mut account = AccountSharedData::default();
            account.set_lamports(Rent::default().minimum_balance(program_buffer_data.len()));
            account.set_owner(Pubkey::from_str_const("BPFLoaderUpgradeab1e11111111111111111111111"));
            account.set_executable(false);
            account.set_rent_epoch(u64::MAX);
            account.set_data_from_slice(&program_buffer_data);
            account
        };
        $self
            .db
            .write()
            .unwrap()
            .write_account(program_buffer, program_buffer_account);
    }};
}


#[derive(Debug, Clone)]
pub enum TransactionStatus {
    Executing,
    Executed(Option<TransactionError>, u64),
}

pub struct Bank {
    db: Arc<RwLock<dyn Database>>,
    status_cache: Arc<DashMap<Signature, TransactionStatus>>,

    blockhash_pruner_sender: Sender<(Hash, HashSet<Signature>)>,
    blockhash_signature_map: DashMap<Hash, HashSet<Signature>>,

    previous_hashes: Arc<RwLock<VecDeque<Hash>>>, // recent valid blockhashes (50ms -> 1200 blockhashes)
    slot_blockhashes: BTreeMap<u64, Hash>,
    slot_timestamps: BTreeMap<u64, u64>,

    slot_hash_timestamp: SlotHashTimestamp,
    hash_generator: DummyRpcBlockhashGenerator,

    transaction_processor: TransactionBatchProcessor<EmptyForkGraph>,
    _feature_set: Arc<FeatureSet>,
    _fork_graph: Arc<RwLock<EmptyForkGraph>>,
    fee_structure: FeeStructure,

    subscription_processor: Option<Arc<dyn Notifier>>,
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

        let mut slot_blockhashes = BTreeMap::new();
        slot_blockhashes.insert(slot, hash);
        let mut slot_timestamps = BTreeMap::new();
        slot_timestamps.insert(slot, timestamp);
        let previous_hashes = Arc::new(RwLock::new(VecDeque::with_capacity(BLOCKHASH_HISTORY_SLOTS)));
        {
            let mut prev = previous_hashes.write().unwrap();
            prev.push_back(hash);
        }

        let bank = Self {
            db: Arc::new(RwLock::new(pdb)),
            // Keep signature history aligned with the recent blockhash window.
            blockhash_signature_map: DashMap::with_capacity(BLOCKHASH_HISTORY_SLOTS),
            previous_hashes,
            slot_blockhashes,
            slot_timestamps,
            slot_hash_timestamp: (slot, hash, timestamp),
            hash_generator,

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
            metrics: BankMetrics::default(),
        };

        bank.blockhash_signature_map
            .insert(hash, HashSet::with_capacity(BLOCKHASH_SIGNATURE_SET_CAPACITY));

        for builtin in BUILTINS {
            bank.transaction_processor.add_builtin(
                &bank,
                builtin.program_id,
                builtin.name,
                ProgramCacheEntry::new_builtin(0, builtin.name.len(), builtin.entrypoint),
            );
        }

        // no genesis map for slave
        bank.init_hardcoded_accounts(BTreeMap::new());

        bank
    }

    // instead of using TransactionProcessingEnvironment that introduces a lifetime,
    // store the fields separately
    pub fn get_transaction_processing_environment(&self) -> TransactionProcessingEnvironment<'_> {
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

    pub fn get_slot_hash_timestamp_parent(&self, slot: u64) -> Option<(Hash, u64, Hash)> {
        let hash = self.slot_blockhashes.get(&slot).copied()?;
        let timestamp = self.slot_timestamps.get(&slot).copied()?;
        let parent_hash = self.parent_blockhash_of_slot(slot).unwrap_or_default();
        Some((hash, timestamp, parent_hash))
    }

    pub fn parent_blockhash_of_slot(&self, slot: u64) -> Option<Hash> {
        let parent_slot = slot.checked_sub(1)?;

        match self.slot_blockhashes.get(&parent_slot).copied() {
            Some(hash) => Some(hash),
            None => {
                if self.metrics.note_parent_blockhash_missing(slot) {
                    let oldest_slot = self.slot_blockhashes.keys().next().copied();
                    let newest_slot = self.slot_blockhashes.keys().next_back().copied();
                    warn!(
                        slot,
                        parent_slot,
                        ?oldest_slot,
                        ?newest_slot,
                        slot_blockhashes_len = self.slot_blockhashes.len(),
                        "parent blockhash not found in slot_blockhashes"
                    );
                }
                None
            }
        }
    }

    pub fn get_latest_slot_hash_timestamp_parent(&self) -> (u64, Hash, u64, Hash) {
        let slot = self.slot_hash_timestamp.0;
        let parent_hash = self.parent_blockhash_of_slot(slot).unwrap_or_default();
        (
            slot,
            self.slot_hash_timestamp.1,
            self.slot_hash_timestamp.2,
            parent_hash,
        )
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

    fn stage_blockhash(&mut self, slot: u64, hash: Hash) {
        let existing = self.slot_blockhashes.get(&slot).copied();
        if existing == Some(hash) {
            return;
        }

        if let Some(old_hash) = existing {
            if old_hash != hash {
                self.blockhash_signature_map.remove(&old_hash);
            }
        }

        self.slot_blockhashes.insert(slot, hash);
        self.blockhash_signature_map
            .entry(hash)
            .or_insert_with(|| HashSet::with_capacity(BLOCKHASH_SIGNATURE_SET_CAPACITY));

        self.trim_blockhash_history();
        self.rebuild_previous_hashes();
    }

    fn trim_blockhash_history(&mut self) {
        while self.slot_blockhashes.len() > BLOCKHASH_HISTORY_SLOTS {
            let oldest_slot = match self.slot_blockhashes.keys().next().copied() {
                Some(slot) => slot,
                None => break,
            };

            self.slot_timestamps.remove(&oldest_slot);
            if let Some(old_hash) = self.slot_blockhashes.remove(&oldest_slot) {
                if let Some((_, signatures)) = self.blockhash_signature_map.remove(&old_hash) {
                    let _ = self.blockhash_pruner_sender.send((old_hash, signatures));
                }
            }
        }

        self.blockhash_signature_map.shrink_to_fit();
    }

    fn rebuild_previous_hashes(&mut self) {
        let mut previous = self.previous_hashes.write().unwrap();
        previous.clear();
        for hash in self.slot_blockhashes.values() {
            previous.push_back(*hash);
        }
    }

    pub fn set_slot_blockhash(&mut self, slot: u64, hash: Hash) {
        let existing = self.slot_blockhashes.get(&slot).copied();
        if existing == Some(hash) {
            return;
        }

        let signatures = if let Some(old_hash) = existing {
            self.blockhash_signature_map
                .remove(&old_hash)
                .map(|(_, set)| set)
                .unwrap_or_else(|| HashSet::with_capacity(BLOCKHASH_SIGNATURE_SET_CAPACITY))
        } else {
            HashSet::with_capacity(BLOCKHASH_SIGNATURE_SET_CAPACITY)
        };

        self.slot_blockhashes.insert(slot, hash);
        self.blockhash_signature_map.insert(hash, signatures);

        self.trim_blockhash_history();
        self.rebuild_previous_hashes();

        if self.slot_hash_timestamp.0 == slot {
            self.slot_hash_timestamp.1 = hash;
        }
    }

    pub fn set_slot_metadata(&mut self, slot: u64, hash: Hash, timestamp: u64) {
        self.slot_timestamps.insert(slot, timestamp);
        self.set_slot_blockhash(slot, hash);
    }

    pub fn tick(&mut self) {
        let slot = self.slot_hash_timestamp.0;

        let unix_timestamp = std::time::UNIX_EPOCH.elapsed().unwrap().as_secs();
        let next_slot = self.slot_hash_timestamp.0 + 1;
        let next_hash = self.hash_generator.next();
        self.stage_blockhash(next_slot, next_hash);
        self.slot_timestamps.insert(next_slot, unix_timestamp);
        self.slot_hash_timestamp = (next_slot, next_hash, unix_timestamp);

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
                .or_insert_with(|| HashSet::with_capacity(BLOCKHASH_SIGNATURE_SET_CAPACITY))
                .extend(signatures.into_iter());
        }
    }

    pub fn tick_as_slave(&mut self, slot: u64, hash: Hash, timestamp: u64) {
        // Advance to the provided slot/hash/timestamp from the sequencer
        self.slot_hash_timestamp = (slot, hash, timestamp);
        self.set_slot_metadata(slot, hash, timestamp);
        // Update blockhash window and Clock/sysvars for the current slot (not the
        // previous)
        self.post_tick(slot, timestamp, true);
    }

    pub fn commit_changes(&mut self, db_changes: Vec<(Pubkey, AccountSharedData)>) {
        let mut db = self.db.write().unwrap();
        db.commit_changes_raw(db_changes);
    }

    /// Set the status of the bank after tick moved forward, pass in previous
    /// slot and timestamp
    fn post_tick(&mut self, slot: u64, unix_timestamp: u64, update_sysvar: bool) {
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
            1u64,
            "GjtMGFA81gyP5FRX5skKmwKRT3VV4LdhCJLexwyM4Hjd"
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

    fn init_hardcoded_accounts(&self, genesis_map: BTreeMap<String, AccountSharedData>) {
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
