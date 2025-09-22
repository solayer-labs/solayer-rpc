use hashbrown::HashMap;
use solana_sdk::{
    account::{AccountSharedData, WritableAccount},
    pubkey::Pubkey,
};

use crate::{
    standard::Database,
    versioned::{AccountVersion, VersionedDB},
    SlotHash,
};

#[derive(Default)]
pub struct MockDB {
    accounts: HashMap<Pubkey, (AccountSharedData, AccountVersion)>,
    slot_info: SlotHash,
    tx_version: u64,
}

impl MockDB {
    pub fn init_accounts(&mut self, init_keys: Vec<Pubkey>) {
        for key in init_keys {
            let mut account = AccountSharedData::default();
            account.set_lamports(1_000_000_000);
            self.accounts.insert(key, (account, AccountVersion::default()));
        }
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Database for MockDB {
    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        Ok(self.accounts.get(&pubkey).map(|(account, _)| account.clone()))
    }

    fn write_account(&mut self, pubkey: Pubkey, account: &AccountSharedData) {
        self.accounts
            .insert(pubkey, (account.clone(), AccountVersion::default()));
    }

    fn get_slot_info(&self) -> eyre::Result<crate::SlotHash> {
        Ok(self.slot_info)
    }

    fn update_slot_info(&mut self, slot_info: crate::SlotHash) {
        self.slot_info = slot_info;
    }
}

impl VersionedDB for MockDB {
    fn get_account_with_version(&self, pubkey: Pubkey) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>> {
        Ok(self
            .accounts
            .get(&pubkey)
            .map(|(account, version)| (account.clone(), *version)))
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
