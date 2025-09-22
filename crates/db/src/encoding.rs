use bytes::Bytes;
use hashbrown::HashMap;
use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};

pub fn encode_hashmap(hashmap: HashMap<Pubkey, (AccountSharedData, u64)>) -> eyre::Result<Bytes> {
    encode(
        hashmap
            .into_iter()
            .map(|(pubkey, (account, version))| (pubkey, account, version)),
    )
}

pub fn encode(accounts: impl Iterator<Item = (Pubkey, AccountSharedData, u64)>) -> eyre::Result<Bytes> {
    let data = bincode::serialize(
        &accounts
            .map(|(pubkey, account, version)| (pubkey.to_bytes(), account, version))
            .collect::<Vec<_>>(),
    )?;

    Ok(data.into())
}

pub fn decode(data: &[u8]) -> eyre::Result<Vec<(Pubkey, AccountSharedData, u64)>> {
    let accounts: Vec<(Pubkey, AccountSharedData, u64)> = bincode::deserialize(data)?;
    Ok(accounts)
}
