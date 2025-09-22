use std::sync::RwLock;

use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;

pub type AccountVersion = u64;
pub const EMPTY_ACCOUNT_VERSION: AccountVersion = 0;

pub trait VersionedDB: Send + Sync {
    fn get_account_with_version(&self, pubkey: Pubkey) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>>;
    fn write_account_with_version(&mut self, pubkey: Pubkey, account: AccountSharedData, version: AccountVersion);
    fn tx_version(&self) -> u64;
    fn set_tx_version(&mut self, version: u64);
    fn commit_changes_raw(&mut self, changes: Vec<(Pubkey, AccountSharedData, AccountVersion)>) {
        unreachable!("commit_changes_raw should not be called on VersionedDB")
    }

    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        Ok(self.get_account_with_version(pubkey)?.map(|(account, _)| account))
    }

    fn write_account(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        self.write_account_with_version(pubkey, account, 0);
    }

    fn batch_get_account_versions(&self, pubkeys: Vec<Pubkey>) -> eyre::Result<Vec<AccountVersion>> {
        let mut versions = Vec::new();
        for pubkey in pubkeys.into_iter() {
            let version = self
                .get_account_with_version(pubkey)?
                .map(|(_, version)| version)
                .unwrap_or(EMPTY_ACCOUNT_VERSION);
            versions.push(version);
        }
        Ok(versions)
    }

    fn bulk_read_account(&self, pubkeys: Vec<Pubkey>) -> eyre::Result<Vec<(Pubkey, Option<AccountSharedData>)>> {
        let mut accounts = Vec::new();
        for pubkey in pubkeys.into_iter() {
            if let Some((account, _)) = self.get_account_with_version(pubkey)? {
                accounts.push((pubkey, Some(account)));
            } else {
                accounts.push((pubkey, None));
            }
        }
        Ok(accounts)
    }

    fn bulk_write_account_with_version(
        &mut self,
        accounts: Vec<(Pubkey, AccountSharedData)>,
        account_versions: &[AccountVersion],
    ) {
        for (i, (pubkey, account)) in accounts.into_iter().enumerate() {
            self.write_account_with_version(pubkey, account, account_versions[i] + 1);
        }
        self.set_tx_version(self.tx_version() + 1);
    }

    fn commit(&mut self, _slot: u64) {
        // do nothing
    }
}

impl<T: VersionedDB> VersionedDB for RwLock<T> {
    fn get_account_with_version(&self, pubkey: Pubkey) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>> {
        self.read().unwrap().get_account_with_version(pubkey)
    }

    fn write_account_with_version(&mut self, pubkey: Pubkey, account: AccountSharedData, version: AccountVersion) {
        self.write()
            .unwrap()
            .write_account_with_version(pubkey, account, version);
    }

    fn tx_version(&self) -> u64 {
        self.read().unwrap().tx_version()
    }

    fn set_tx_version(&mut self, version: u64) {
        self.write().unwrap().set_tx_version(version);
    }
}

pub trait MergeableDB: Send + Sync + VersionedDB {
    fn merge(&mut self, other: &Self) -> eyre::Result<()>;
}
