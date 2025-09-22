use std::sync::RwLock;

use hashbrown::HashMap;
use solana_hash::Hash;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

use crate::{
    standard::Database,
    versioned::{AccountVersion, MergeableDB, VersionedDB},
    SlotHash,
};

#[derive(Default)]
pub struct MemoryDB<T> {
    accounts: HashMap<Pubkey, (AccountSharedData, AccountVersion)>,
    underlying: T,
    tx_version: u64,
    slot_info: RwLock<SlotHash>,
}

impl<T: Database> MemoryDB<T> {
    pub fn new(underlying: T) -> Self {
        let slot_info = underlying.get_slot_info().unwrap();
        Self {
            accounts: HashMap::new(),
            underlying,
            slot_info: RwLock::new(slot_info),
            tx_version: 0,
        }
    }
}

impl MemoryDB<NoopDB> {
    pub fn new_no_underlying() -> Self {
        let default_slot_hash: SlotHash = (0, Hash::default());
        Self {
            accounts: HashMap::new(),
            underlying: NoopDB {},
            slot_info: RwLock::new(default_slot_hash),
            tx_version: 0,
        }
    }

    pub fn from_hashmap(accounts: HashMap<Pubkey, (AccountSharedData, AccountVersion)>) -> Self {
        Self {
            accounts,
            underlying: NoopDB {},
            tx_version: 0,
            slot_info: RwLock::new((0, Hash::default())),
        }
    }
}

impl<T: MergeableDB> MergeableDB for MemoryDB<T> {
    fn merge(&mut self, other: &Self) -> eyre::Result<()> {
        for (pubkey, (account, version)) in other.accounts.iter() {
            self.accounts.insert(*pubkey, (account.clone(), *version));
        }
        self.underlying.merge(&other.underlying)?;
        Ok(())
    }
}

impl<T: Database> Database for MemoryDB<T> {
    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        if let Some(account) = self.accounts.get(&pubkey) {
            return Ok(Some(account.0.clone()));
        }
        self.underlying.get_account(pubkey)
    }

    fn write_account(&mut self, pubkey: Pubkey, account: &AccountSharedData) {
        self.accounts.insert(pubkey, (account.clone(), 0));
    }

    fn get_slot_info(&self) -> eyre::Result<SlotHash> {
        Ok(*self.slot_info.read().unwrap())
    }

    fn update_slot_info(&mut self, slot_info: SlotHash) {
        *self.slot_info.write().unwrap() = slot_info;
    }
}

impl<T: VersionedDB> VersionedDB for MemoryDB<T> {
    fn get_account_with_version(&self, pubkey: Pubkey) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>> {
        if let Some((account, version)) = self.accounts.get(&pubkey) {
            return Ok(Some((account.clone(), *version)));
        }
        self.underlying.get_account_with_version(pubkey)
    }

    fn write_account_with_version(&mut self, pubkey: Pubkey, account: AccountSharedData, version: AccountVersion) {
        self.accounts.insert(pubkey, (account, version));
    }

    fn tx_version(&self) -> u64 {
        self.tx_version
    }

    fn set_tx_version(&mut self, version: u64) {
        self.tx_version = version;
    }
}

#[derive(Default)]
pub struct NoopDB {}

impl Database for NoopDB {
    fn get_account(&self, _: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        Ok(None)
    }

    fn write_account(&mut self, _: Pubkey, _: &AccountSharedData) {
        unreachable!()
    }

    fn get_slot_info(&self) -> eyre::Result<SlotHash> {
        unreachable!()
    }

    fn update_slot_info(&mut self, _: SlotHash) {
        unreachable!()
    }
}

impl VersionedDB for NoopDB {
    fn get_account_with_version(&self, _: Pubkey) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>> {
        Ok(None)
    }

    fn write_account_with_version(&mut self, _: Pubkey, _: AccountSharedData, _: AccountVersion) {
        unreachable!()
    }

    fn tx_version(&self) -> u64 {
        unreachable!()
    }

    fn set_tx_version(&mut self, _: u64) {
        unreachable!()
    }
}

impl<T: MergeableDB> MergeableDB for RwLock<T> {
    fn merge(&mut self, other: &Self) -> eyre::Result<()> {
        self.write().unwrap().merge(&*other.read().unwrap())
    }
}

impl MergeableDB for NoopDB {
    fn merge(&mut self, _other: &Self) -> eyre::Result<()> {
        Ok(())
    }
}
