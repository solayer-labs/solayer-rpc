use std::sync::RwLock;

use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

use crate::SlotHash;

pub trait Database: Send + Sync {
    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>>;
    fn write_account(&mut self, pubkey: Pubkey, account: AccountSharedData);

    fn get_slot_info(&self) -> eyre::Result<SlotHash>;
    fn update_slot_info(&mut self, slot_info: SlotHash);

    fn commit_changes_raw(&mut self, _changes: Vec<(Pubkey, AccountSharedData)>) {
        unreachable!("commit_changes_raw should not be called on Database")
    }

    fn commit(&mut self, _slot: u64) {
        // default no-op
    }

    fn bulk_read_account(&self, pubkeys: Vec<Pubkey>) -> eyre::Result<Vec<(Pubkey, Option<AccountSharedData>)>> {
        let mut accounts = Vec::with_capacity(pubkeys.len());
        for pubkey in pubkeys.into_iter() {
            let account = self.get_account(pubkey)?;
            accounts.push((pubkey, account));
        }
        Ok(accounts)
    }

    fn bulk_write_account(&mut self, accounts: Vec<(Pubkey, AccountSharedData)>) {
        for (pubkey, account) in accounts.into_iter() {
            self.write_account(pubkey, account);
        }
    }
}

impl<T: Database> Database for RwLock<T> {
    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        self.read().unwrap().get_account(pubkey)
    }

    fn write_account(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        self.write().unwrap().write_account(pubkey, account);
    }

    fn get_slot_info(&self) -> eyre::Result<SlotHash> {
        self.read().unwrap().get_slot_info()
    }

    fn update_slot_info(&mut self, slot_info: SlotHash) {
        self.write().unwrap().update_slot_info(slot_info);
    }

    fn commit_changes_raw(&mut self, changes: Vec<(Pubkey, AccountSharedData)>) {
        self.write().unwrap().commit_changes_raw(changes);
    }

    fn commit(&mut self, slot: u64) {
        self.write().unwrap().commit(slot);
    }
}

pub trait MergeableDB: Send + Sync + Database {
    fn merge(&mut self, other: &Self) -> eyre::Result<()>;
}
