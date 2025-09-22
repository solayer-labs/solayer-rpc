use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

use crate::{standard::Database, versioned::VersionedDB, SlotHash};

pub struct LocalRocksDB {
    db: rocksdb::DB,
}

impl LocalRocksDB {
    pub fn new(path: &str) -> eyre::Result<Self> {
        let db = rocksdb::DB::open_default(path)?;
        Ok(Self { db })
    }

    pub fn new_for_tests(path: &str) -> eyre::Result<Self> {
        let db = rocksdb::DB::open_default(path)?;
        db.put(b"slot_info", bincode::serialize(&SlotHash::default()).unwrap())
            .unwrap();
        Ok(Self { db })
    }
}

impl Database for LocalRocksDB {
    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        let value = self.db.get(pubkey.to_bytes()).unwrap();
        if let Some(value) = value {
            let (account, _): (AccountSharedData, u64) = bincode::deserialize(&value).unwrap();
            return Ok(Some(account));
        }
        Ok(None)
    }

    fn write_account(&mut self, pubkey: Pubkey, account: &AccountSharedData) {
        let version = 0u64;
        self.db
            .put(pubkey.to_bytes(), bincode::serialize(&(account, version)).unwrap())
            .unwrap();
    }

    fn update_slot_info(&mut self, slot_info: SlotHash) {
        self.db
            .put(b"slot_info", bincode::serialize(&slot_info).unwrap())
            .unwrap();
    }

    fn get_slot_info(&self) -> eyre::Result<SlotHash> {
        let value = self.db.get(b"slot_info").unwrap();
        if let Some(value) = value {
            let slot_info = bincode::deserialize(&value).unwrap();
            Ok(slot_info)
        } else {
            // panic for now
            Err(eyre::eyre!("slot info not found"))
        }
    }
}

impl VersionedDB for LocalRocksDB {
    fn get_account_with_version(
        &self,
        pubkey: Pubkey,
    ) -> eyre::Result<Option<(AccountSharedData, crate::versioned::AccountVersion)>> {
        let value = self.db.get(pubkey.to_bytes()).unwrap();
        if let Some(value) = value {
            let (account, version): (AccountSharedData, u64) = bincode::deserialize(&value).unwrap();
            Ok(Some((account, version)))
        } else {
            Ok(None)
        }
    }

    fn write_account_with_version(
        &mut self,
        pubkey: Pubkey,
        account: AccountSharedData,
        version: crate::versioned::AccountVersion,
    ) {
        self.db
            .put(pubkey.to_bytes(), bincode::serialize(&(account, version)).unwrap())
            .unwrap();
    }

    fn tx_version(&self) -> u64 {
        let value = self.db.get("tx_version").unwrap().unwrap();
        bincode::deserialize(&value).unwrap()
    }

    fn set_tx_version(&mut self, version: u64) {
        self.db
            .put("tx_version", bincode::serialize(&version).unwrap())
            .unwrap();
    }
}
