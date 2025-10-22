use infinisvm_logger::error;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

use super::chain::DBChain;
use crate::{Database, MergeableDB};

impl<T: MergeableDB> Database for DBChain<T> {
    fn get_account(&self, pubkey: Pubkey) -> eyre::Result<Option<AccountSharedData>> {
        for (_, db) in self.dbs.iter().rev() {
            if let Some(account) = db.read().unwrap().get_account(pubkey)? {
                return Ok(Some(account));
            }
        }
        Ok(None)
    }

    fn write_account(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        let last_db = match self.dbs.last_mut() {
            Some(db) => db,
            None => {
                error!("DBChain is empty, cannot write account");
                return;
            }
        };
        last_db.1.write().unwrap().write_account(pubkey, account);
    }

    fn get_slot_info(&self) -> eyre::Result<crate::SlotHash> {
        panic!("DBChain does not support get_slot_info");
    }

    fn update_slot_info(&mut self, _slot_info: crate::SlotHash) {
        panic!("DBChain does not support update_slot_info");
    }
}
