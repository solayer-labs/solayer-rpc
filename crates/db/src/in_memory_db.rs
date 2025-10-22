use std::sync::RwLock;

use hashbrown::HashMap;
use solana_hash::Hash;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

use crate::{Database, MergeableDB, SlotHash};

#[derive(Default)]
pub struct MemoryDB<T> {
    accounts: HashMap<Pubkey, AccountSharedData>,
    underlying: T,
    slot_info: RwLock<SlotHash>,
}

impl<T: Database> MemoryDB<T> {
    pub fn new(underlying: T) -> Self {
        let slot_info = underlying.get_slot_info().unwrap();
        Self {
            accounts: HashMap::new(),
            underlying,
            slot_info: RwLock::new(slot_info),
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
        }
    }

    pub fn from_hashmap(accounts: HashMap<Pubkey, AccountSharedData>) -> Self {
        Self {
            accounts,
            underlying: NoopDB {},
            slot_info: RwLock::new((0, Hash::default())),
        }
    }
}

impl<T: MergeableDB> MergeableDB for MemoryDB<T> {
    fn merge(&mut self, other: &Self) -> eyre::Result<()> {
        for (pubkey, account) in other.accounts.iter() {
            self.accounts.insert(*pubkey, account.clone());
        }
        self.underlying.merge(&other.underlying)?;
        Ok(())
    }
}

impl<T: Database> Database for MemoryDB<T> {
    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        if let Some(account) = self.accounts.get(&pubkey) {
            return Ok(Some(account.clone()));
        }
        self.underlying.get_account(pubkey)
    }

    fn write_account(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        self.accounts.insert(pubkey, account);
    }

    fn get_slot_info(&self) -> eyre::Result<SlotHash> {
        Ok(*self.slot_info.read().unwrap())
    }

    fn update_slot_info(&mut self, slot_info: SlotHash) {
        *self.slot_info.write().unwrap() = slot_info;
    }
}

#[derive(Default)]
pub struct NoopDB {}

impl Database for NoopDB {
    fn get_account(&self, _: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        Ok(None)
    }

    fn write_account(&mut self, _: Pubkey, _: AccountSharedData) {
        unreachable!()
    }

    fn get_slot_info(&self) -> eyre::Result<SlotHash> {
        unreachable!()
    }

    fn update_slot_info(&mut self, _: SlotHash) {
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
