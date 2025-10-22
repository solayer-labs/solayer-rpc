use std::{
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;

const MAX_DATA_PER_CONNECTION: u64 = 100 * 1024 * 1024; // 100 MiB

#[derive(Debug)]
pub struct ConnectionData {
    bytes_received: AtomicU64,
    created_at: Instant,
}

impl ConnectionData {
    fn new() -> Self {
        Self {
            bytes_received: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    fn add_bytes(&self, bytes: u64) -> bool {
        let current = self.bytes_received.fetch_add(bytes, AtomicOrdering::SeqCst);
        current + bytes <= MAX_DATA_PER_CONNECTION
    }

    fn get_bytes_received(&self) -> u64 {
        self.bytes_received.load(AtomicOrdering::SeqCst)
    }
}

pub struct ConnectionRateLimiter<T: std::hash::Hash + Eq + Send + Sync> {
    connections: DashMap<T, (ConnectionData, std::sync::atomic::AtomicUsize)>,
}

impl<T: std::hash::Hash + Eq + Send + Sync> ConnectionRateLimiter<T> {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
        }
    }

    pub fn register_connection(&self, connection_id: T) {
        if let Some(existing_entry) = self.connections.get_mut(&connection_id) {
            existing_entry.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        } else {
            let (data, ref_count) = (ConnectionData::new(), std::sync::atomic::AtomicUsize::new(1));
            self.connections.insert(connection_id, (data, ref_count));
        }
    }

    pub fn check_data_limit(&self, connection_id: &T, bytes: u64) -> Result<(), &'static str> {
        match self.connections.get_mut(connection_id) {
            Some(conn) => {
                if conn.0.add_bytes(bytes) {
                    Ok(())
                } else {
                    Err("Data limit exceeded: connection has received more than 100MiB")
                }
            }
            None => Err("Connection not found"),
        }
    }

    pub fn remove_connection(&self, connection_id: &T) {
        self.connections.remove(connection_id);
    }

    pub fn get_connection_stats(&self, connection_id: &T) -> Option<(u64, u64, Duration)> {
        self.connections.get(connection_id).map(|conn| {
            (
                conn.0.get_bytes_received(),
                conn.1.load(std::sync::atomic::Ordering::SeqCst) as u64,
                conn.0.created_at.elapsed(),
            )
        })
    }

    pub fn cleanup_old_connections(&self) {
        const CONNECTION_TIMEOUT: Duration = Duration::from_secs(3600); // 1 hour
        let now = Instant::now();

        self.connections
            .retain(|_, conn| now.duration_since(conn.0.created_at) < CONNECTION_TIMEOUT);
    }
}

impl<T: std::hash::Hash + Eq + Send + Sync> Default for ConnectionRateLimiter<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WebSocketMiddleware {
    rate_limiter: Arc<ConnectionRateLimiter<jsonrpsee::ConnectionId>>,
}

impl WebSocketMiddleware {
    pub fn new() -> Self {
        let rate_limiter = Arc::new(ConnectionRateLimiter::new());
        Self { rate_limiter }
    }

    pub fn register_connection(&self, connection_id: jsonrpsee::ConnectionId) {
        self.rate_limiter.cleanup_old_connections();
        self.rate_limiter.register_connection(connection_id);
    }

    pub fn track_message_data(
        &self,
        connection_id: &jsonrpsee::ConnectionId,
        message_size: usize,
    ) -> Result<(), &'static str> {
        self.rate_limiter.check_data_limit(connection_id, message_size as u64)
    }

    pub fn cleanup_connection(&self, connection_id: &jsonrpsee::ConnectionId) {
        self.rate_limiter.remove_connection(connection_id);
    }
}

impl Default for WebSocketMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use infinisvm_types::BlockWithTransactions;
    use solana_sdk::{
        account::{Account, AccountSharedData},
        pubkey::Pubkey,
    };

    fn create_test_account() -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports: 100,
            data: vec![1, 2, 3],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        })
    }

    fn create_test_block() -> BlockWithTransactions {
        BlockWithTransactions {
            slot: 42,
            parent_slot: 41,
            parent_blockhash: solana_hash::Hash::new_unique().to_string(),
            blockhash: solana_hash::Hash::new_unique().to_string(),
            block_unix_timestamp: 1234567890,
            transactions: vec![],
            signatures: vec![],
            tx_count: 0,
        }
    }
}
