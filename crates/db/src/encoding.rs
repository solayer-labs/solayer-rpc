use bytes::Bytes;
use hashbrown::HashMap;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

pub fn encode_hashmap(hashmap: HashMap<Pubkey, AccountSharedData>) -> eyre::Result<Bytes> {
    encode(hashmap.into_iter())
}

pub fn encode(accounts: impl Iterator<Item = (Pubkey, AccountSharedData)>) -> eyre::Result<Bytes> {
    let data = bincode::serialize(&accounts.collect::<Vec<_>>())?;
    Ok(data.into())
}

pub fn decode(data: &[u8]) -> eyre::Result<Vec<(Pubkey, AccountSharedData)>> {
    let accounts: Vec<(Pubkey, AccountSharedData)> = bincode::deserialize(data)?;
    Ok(accounts)
}
