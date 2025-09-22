use crate::{
    local_rocks_db::LocalRocksDB,
    versioned::{AccountVersion, VersionedDB},
    MemoryDB,
};

use solana_sdk::{
    account::{Account, AccountSharedData},
    pubkey::Pubkey,
};

#[test]
fn test_memory_db() {
    let underlying = LocalRocksDB::new_for_tests("test_db/versioned").unwrap();
    let mut db = MemoryDB::new(underlying);

    let pubkey = Pubkey::new_unique();
    let account = AccountSharedData::from(Account {
        lamports: 100,
        data: vec![1, 2, 3],
        owner: Pubkey::new_unique(),
        executable: false,
        rent_epoch: 0,
    });
    let version: AccountVersion = 1;

    // Test writing and reading account
    db.write_account_with_version(pubkey, account.clone(), version);
    let (read_account, read_version) = db.get_account_with_version(pubkey).unwrap().unwrap();
    assert_eq!(read_account, account);
    assert_eq!(read_version, version);

    // Test reading non-existent account
    let missing_pubkey = Pubkey::new_unique();
    assert!(VersionedDB::get_account(&db, missing_pubkey).unwrap().is_none());
}

#[test]
fn test_local_rocks_db() {
    let db_path = format!(
        "test_db_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut db = LocalRocksDB::new_for_tests(&db_path).unwrap();

    let pubkey = Pubkey::new_unique();
    let account = AccountSharedData::from(Account {
        lamports: 100,
        data: vec![1, 2, 3],
        owner: Pubkey::new_unique(),
        executable: false,
        rent_epoch: 0,
    });
    let version: AccountVersion = 1;

    // Test writing and reading account
    db.write_account_with_version(pubkey, account.clone(), version);
    let (read_account, read_version) = db.get_account_with_version(pubkey).unwrap().unwrap();
    assert_eq!(read_account, account);
    assert_eq!(read_version, version);

    // Test reading non-existent account
    let missing_pubkey = Pubkey::new_unique();
    assert!(VersionedDB::get_account(&db, missing_pubkey).unwrap().is_none());

    // Cleanup
    std::fs::remove_dir_all(db_path).unwrap();
}
