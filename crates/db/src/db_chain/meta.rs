use crate::persistence::DBFile;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBMeta {
    pub shred_index: usize,
    pub slot: DBFile,
}

impl DBMeta {
    pub fn from_ckpt(slot: u64) -> Self {
        Self {
            shred_index: 0,
            slot: DBFile::Checkpoint(slot),
        }
    }

    pub fn from_shred(slot: u64, shred_index: usize) -> Self {
        Self {
            shred_index,
            slot: DBFile::Shred(slot, shred_index),
        }
    }

    pub fn from_account(slot: u64) -> Self {
        Self {
            shred_index: 0,
            slot: DBFile::Account(slot),
        }
    }

    pub fn from_db_file(db_file: DBFile) -> Self {
        match db_file {
            DBFile::Checkpoint(slot) => Self::from_ckpt(slot),
            DBFile::Shred(slot, shred_index) => Self::from_shred(slot, shred_index),
            DBFile::Account(slot) => Self::from_account(slot),
        }
    }
}

impl DBMeta {
    fn kind_rank(&self) -> u8 {
        match self.slot {
            DBFile::Checkpoint(_) => 0,
            DBFile::Account(_) => 1,
            DBFile::Shred(_, _) => 2,
        }
    }
}

impl Ord for DBMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.slot.slot(), self.kind_rank(), self.shred_index).cmp(&(
            other.slot.slot(),
            other.kind_rank(),
            other.shred_index,
        ))
    }
}

impl PartialOrd for DBMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
