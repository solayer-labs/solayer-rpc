use std::{
    collections::VecDeque,
    ffi::OsStr,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    thread::JoinHandle,
    time::Instant,
};

use hashbrown::HashMap;
use infinisvm_logger::{error, info};
use rayon::prelude::*;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

use crate::{
    encoding,
    versioned::{AccountVersion, VersionedDB},
};

#[derive(Default)]
pub struct PersistedInMemoryDB {
    accounts: HashMap<Pubkey, (AccountSharedData, AccountVersion)>,
    uncommitted_accounts: HashMap<Pubkey, (AccountSharedData, AccountVersion)>,

    persisted: bool,
    tasks: VecDeque<JoinHandle<()>>,
}

pub const DB_DIRECTORY: &str = "/mnt/data/chaindata";
pub const CHECKPOINT_PREFIX: &str = "ckpt_";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DBFile {
    Checkpoint(u64),
    Account(u64),
    Shred(u64, u64),
}

impl DBFile {
    fn convert_file_name_to_slot(file_name: &str) -> Option<u64> {
        fn strip_prefix_0s(s: &str) -> String {
            if s.is_empty() {
                return "0".to_string(); // Return "0" for empty string or all
                                        // zeros
            }
            let result = s.chars().skip_while(|c| *c == '0').collect::<String>();
            if result.is_empty() {
                "0".to_string() // Return "0" if the string was all zeros
            } else {
                result
            }
        }

        if file_name.starts_with(CHECKPOINT_PREFIX) {
            match strip_prefix_0s(&file_name[CHECKPOINT_PREFIX.len()..file_name.len() - 4]).parse::<u64>() {
                Ok(slot) => Some(slot),
                Err(_) => {
                    error!("Invalid slot found in db directory: {}", file_name);
                    None
                }
            }
        } else if file_name.starts_with("accounts_") && file_name.ends_with(".bin") {
            match strip_prefix_0s(&file_name[9..file_name.len() - 4]).parse::<u64>() {
                Ok(slot) => Some(slot),
                Err(_) => {
                    error!("Invalid slot found in db directory: {}", file_name);
                    None
                }
            }
        } else {
            error!("Invalid file name found in db directory: {}", file_name);
            None
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DBFile::Checkpoint(slot) => format!("{}{:018}.bin", CHECKPOINT_PREFIX, slot),
            DBFile::Account(slot) => format!("accounts_{:018}.bin", slot),
            DBFile::Shred(slot, shred_idx) => format!("shred_{:018}_{:018}.bin", slot, shred_idx),
        }
    }

    pub fn from_path(path: &Path) -> Option<DBFile> {
        if let Some(file_name) = path.file_name() {
            if let Some(file_name_str) = file_name.to_str() {
                if file_name_str.starts_with(CHECKPOINT_PREFIX) && file_name_str.ends_with(".bin") {
                    Self::convert_file_name_to_slot(file_name_str).map(DBFile::Checkpoint)
                } else if file_name_str.starts_with("accounts_") && file_name_str.ends_with(".bin") {
                    Self::convert_file_name_to_slot(file_name_str).map(DBFile::Account)
                } else {
                    error!("Invalid file name found in db directory: {:?}", file_name);
                    None
                }
            } else {
                error!("Invalid file name found in db directory: {:?}", file_name);
                None
            }
        } else {
            error!("Invalid path found in db directory: {:?}", path);
            None
        }
    }

    pub fn slot(&self) -> u64 {
        match self {
            DBFile::Checkpoint(slot) => *slot,
            DBFile::Account(slot) => *slot,
            DBFile::Shred(slot, _) => *slot,
        }
    }
}

impl Drop for PersistedInMemoryDB {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            task.join().unwrap();
        }
    }
}

impl PersistedInMemoryDB {
    pub fn persisted_db_from_disk() -> (Self, u64, Vec<PathBuf>) {
        let mut db = Self::default();
        db.persisted = true;

        let (slot, files) = db.load_from_disk();
        (db, slot, files)
    }

    pub fn latest_checkpoint_file() -> (u64, PathBuf) {
        let mut latest_slot = 0;
        let mut latest_path = PathBuf::new();

        if let Ok(entries) = std::fs::read_dir(DB_DIRECTORY) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(db_file) = DBFile::from_path(&path) {
                    if let DBFile::Checkpoint(slot) = db_file {
                        if slot > latest_slot {
                            latest_slot = slot;
                            latest_path = path.clone();
                        }
                    }
                }
            }
        }

        (latest_slot, latest_path)
    }

    pub fn load_accounts_from_file(path: &Path) -> std::io::Result<Vec<(Pubkey, AccountSharedData, AccountVersion)>> {
        let file = File::open(path)?;
        let buf_reader = std::io::BufReader::new(file);
        let result =
            bincode::deserialize_from(buf_reader).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
        info!("Deserialized accounts from file {:?}", path);
        result
    }

    pub fn list_incremental_files(since_slot: u64) -> Vec<PathBuf> {
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(DB_DIRECTORY) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(db_file) = DBFile::from_path(&path) {
                    match db_file {
                        DBFile::Account(slot) if slot > since_slot => {
                            files.push(path);
                        }
                        _ => {}
                    }
                }
            }
        }

        files
    }

    pub fn load_from_disk(&mut self) -> (u64, Vec<PathBuf>) {
        // Create DB_DIRECTORY if it doesn't exist
        if !std::path::Path::new(DB_DIRECTORY).exists() {
            std::fs::create_dir_all(DB_DIRECTORY).unwrap();
            info!("Created directory: {}", DB_DIRECTORY);
            return (0, vec![]); // Return slot 0 for a new database
        }

        let mut files = Vec::new();

        // Single filesystem scan to avoid race condition
        let mut latest_checkpoint = None;
        let mut account_files = Vec::new();
        let mut latest_slot = 0u64;
        let mut second_latest_slot = 0u64;

        for entry in std::fs::read_dir(DB_DIRECTORY).unwrap().filter_map(Result::ok) {
            let path = entry.path();
            if let Some(db_file) = DBFile::from_path(&path) {
                let slot = match db_file {
                    DBFile::Checkpoint(slot) => {
                        if path
                            .file_name()
                            .and_then(OsStr::to_str)
                            .is_some_and(|name| name.starts_with(CHECKPOINT_PREFIX) && name.ends_with(".bin"))
                        {
                            latest_checkpoint = match latest_checkpoint {
                                None => Some((slot, path)),
                                Some((current_slot, _)) if slot > current_slot => Some((slot, path)),
                                _ => latest_checkpoint,
                            };
                        }
                        slot
                    }
                    DBFile::Account(slot) => {
                        account_files.push((slot, path));
                        slot
                    }
                    DBFile::Shred(_, _) => unreachable!(),
                };

                // Track latest and second_latest slots
                if slot > latest_slot {
                    second_latest_slot = latest_slot;
                    latest_slot = slot;
                } else if slot > second_latest_slot {
                    second_latest_slot = slot;
                }
            }
        }

        // Calculate load_until (equivalent to get_latest_confirmed_db_slot)
        let load_until = second_latest_slot;

        info!("Loading from disk until slot {}", load_until);

        let mut loaded_latest_slot = 0;

        // Load the latest checkpoint if it exists
        if let Some((slot, checkpoint_path)) = latest_checkpoint {
            info!("Loading checkpoint from slot {}", slot);
            match Self::load_accounts_from_file(&checkpoint_path) {
                Ok(accounts) => {
                    for (pubkey, account, version) in accounts {
                        self.accounts.insert(pubkey, (account, version));
                    }
                    loaded_latest_slot = slot;
                }
                Err(e) => error!("Failed to load checkpoint: {e:#}"),
            }
        }

        info!(
            "checkpoint {} loaded, {} accounts loaded",
            loaded_latest_slot,
            self.accounts.len()
        );

        // Sort account files by slot
        account_files.sort_by_key(|(slot, _)| *slot);

        // Filter and prepare files for parallel loading
        let files_to_load: Vec<_> = account_files
            .into_iter()
            .filter(|(slot, _)| *slot > loaded_latest_slot && *slot <= load_until)
            .collect();

        for (_, path) in &files_to_load {
            files.push(path.clone());
        }

        // Load accounts in parallel
        let start = Instant::now();
        let loaded_accounts: Vec<_> = files_to_load
            .par_iter()
            .map(|(slot, path)| {
                info!("Deserializing additional accounts from slot {}", slot);
                match Self::load_accounts_from_file(path) {
                    Ok(accounts) => Some((*slot, accounts)),
                    Err(e) => {
                        error!("Failed to load accounts from slot {slot}: {e:#}");
                        None
                    }
                }
            })
            .filter_map(|x| x)
            .collect();

        info!(
            "loaded {} accounts bins in {:?}, starting to aggregate",
            loaded_accounts.len(),
            start.elapsed()
        );

        // Aggregate results sequentially
        let start = Instant::now();
        let loaded_accounts_cnt = loaded_accounts.len();
        for (nth, (slot, accounts)) in loaded_accounts.into_iter().enumerate() {
            assert!(
                slot > loaded_latest_slot,
                "slot {} is not greater than loaded_latest_slot {}",
                slot,
                loaded_latest_slot
            );
            for (pubkey, account, version) in accounts {
                self.accounts.insert(pubkey, (account, version));
            }
            loaded_latest_slot = slot;
            info!(
                "aggregated {} accounts ({}/{}) in {:?}",
                self.accounts.len(),
                nth + 1,
                loaded_accounts_cnt,
                start.elapsed()
            );
        }

        info!("aggregated {} accounts in {:?}", self.accounts.len(), start.elapsed());

        // Remove files that are newer than load_until
        let files_to_remove: Vec<_> = std::fs::read_dir(DB_DIRECTORY)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                if let Some(db_file) = DBFile::from_path(&path) {
                    match db_file {
                        DBFile::Account(slot) if slot > load_until => Some(path),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect();

        for path in files_to_remove {
            std::fs::remove_file(path).expect("Failed to remove not fully committed account file");
        }

        assert!(
            loaded_latest_slot == load_until,
            "loaded_latest_slot {} is not equal to load_until {}",
            loaded_latest_slot,
            load_until
        );
        info!("Finished loading {} accounts", self.accounts.len());
        (loaded_latest_slot, files)
    }

    pub fn merge_accounts() -> std::io::Result<()> {
        let (mut db, commited_slot, files) = PersistedInMemoryDB::persisted_db_from_disk();
        let checkpoint_path = format!("{}/{}{:018}.wip", DB_DIRECTORY, CHECKPOINT_PREFIX, commited_slot);
        let mut file = File::create(&checkpoint_path)?;

        // Serialize all accounts to the checkpoint file
        let accounts = std::mem::take(&mut db.accounts);
        let bytes =
            encoding::encode_hashmap(accounts).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        file.write_all(&bytes)?;
        file.sync_all()?;
        file.flush()?;
        std::fs::rename(
            &checkpoint_path,
            format!("{}/{}{:018}.bin", DB_DIRECTORY, CHECKPOINT_PREFIX, commited_slot),
        )?;

        for file in files {
            std::fs::remove_file(file)?;
        }

        Ok(())
    }

    /// Commit the uncommitted accounts to the database
    /// and persist them to disk if `persisted` is true.
    /// Returns true if there is available changes
    pub fn commit_changes(&mut self, slot: u64) -> bool {
        if slot % 4 == 0 && !self.uncommitted_accounts.is_empty() {
            let accounts = std::mem::take(&mut self.uncommitted_accounts);

            // Extend the accounts map with the uncommitted accounts
            self.accounts.extend(
                accounts
                    .iter()
                    .map(|(pubkey, (account, version))| (*pubkey, (account.clone(), *version))),
            );

            if self.persisted {
                // Use std::thread instead of tokio to avoid runtime dependency
                let task = std::thread::spawn(move || {
                    let filename = format!("{}/accounts_{:018}.bin", DB_DIRECTORY, slot);
                    info!("Committing accounts to disk for slot {}", slot);

                    // Unlikely to fail, unless under critical error
                    let data = encoding::encode_hashmap(accounts).unwrap();

                    if let Err(err) = File::create(&filename).and_then(|file| {
                        let mut writer = std::io::BufWriter::new(file);
                        writer.write_all(&data)
                    }) {
                        error!(slot, "Failed to write accounts: {err:#}");
                        return;
                    }

                    info!(slot, "Done committing accounts to disk for slot");
                });
                self.tasks.push_back(task);
                if self.tasks.len() > 10 {
                    self.tasks.pop_front().unwrap().join().unwrap();
                }
            }
        }

        true
    }

    pub fn active_account_db_delta(&self) -> HashMap<Pubkey, (AccountSharedData, AccountVersion)> {
        self.uncommitted_accounts.clone()
    }
}

impl VersionedDB for PersistedInMemoryDB {
    fn get_account_with_version(&self, pubkey: Pubkey) -> eyre::Result<Option<(AccountSharedData, AccountVersion)>> {
        // Check uncommitted accounts first, then committed accounts
        if let Some((account, version)) = self
            .uncommitted_accounts
            .get(&pubkey)
            .or_else(|| self.accounts.get(&pubkey))
        {
            return Ok(Some((account.clone(), *version)));
        }
        Ok(None)
    }

    fn write_account_with_version(&mut self, pubkey: Pubkey, account: AccountSharedData, version: AccountVersion) {
        self.uncommitted_accounts.insert(pubkey, (account, version));
    }

    fn tx_version(&self) -> u64 {
        unreachable!("tx_version should not be called on PersistedInMemoryDB")
    }

    fn set_tx_version(&mut self, _version: u64) {
        unreachable!("set_tx_version should not be called on PersistedInMemoryDB")
    }

    fn commit(&mut self, slot: u64) {
        self.commit_changes(slot);
    }

    fn commit_changes_raw(&mut self, changes: Vec<(Pubkey, AccountSharedData, AccountVersion)>) {
        // Extend the accounts map with the uncommitted accounts
        self.accounts.extend(
            changes
                .iter()
                .map(|(pubkey, account, version)| (*pubkey, (account.clone(), *version))),
        );
    }
}
