use std::collections::HashMap;

use infinisvm_types::BlockWithTransactions;
use jsonrpsee::types::ErrorObjectOwned;
use solana_sdk::{account::AccountSharedData, clock::Slot, pubkey::Pubkey, signature::Signature};
use tokio::sync::broadcast;

use crate::bank::TransactionStatus;

pub const SUBSCRIPTION_CHANNEL_SIZE: usize = 1000;

pub struct SubscriptionProcessor {
    pubkey: dashmap::DashMap<Pubkey, (broadcast::Sender<AccountSharedData>, std::sync::atomic::AtomicUsize)>,
    signature: dashmap::DashMap<Signature, (broadcast::Sender<TransactionStatus>, std::sync::atomic::AtomicUsize)>,
    block_by_pubkey:
        dashmap::DashMap<Pubkey, (broadcast::Sender<BlockWithTransactions>, std::sync::atomic::AtomicUsize)>,

    all_blocks: (
        broadcast::Sender<BlockWithTransactions>,
        broadcast::Receiver<BlockWithTransactions>,
        std::sync::atomic::AtomicUsize,
    ),
    all_slots: (
        broadcast::Sender<Slot>,
        broadcast::Receiver<Slot>,
        std::sync::atomic::AtomicUsize,
    ),
}

macro_rules! define_subscription_handlers {
    ($field:ident, $data_type:ty, $key:ty) => {
        paste::paste! {
            pub async fn [<register_ $field _subscription>](&self, key: $key) -> Result<broadcast::Receiver<$data_type>, ErrorObjectOwned> {
                // Get or create the broadcast sender for this key
                let sender = if let Some(existing_entry) = self.$field.get_mut(&key) {
                    // Increment ref count and use existing sender
                    existing_entry.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    existing_entry.0.clone()
                } else {
                    // Create new sender if none exists
                    let (sender, _) = broadcast::channel::<$data_type>(SUBSCRIPTION_CHANNEL_SIZE);
                    let ref_count = std::sync::atomic::AtomicUsize::new(1);
                    self.$field.insert(key.clone(), (sender.clone(), ref_count));
                    sender
                };

                // Create a new receiver from the sender (allows multiple receivers)
                let receiver = sender.subscribe();
                Ok(receiver)
            }

            pub fn [<unregister_ $field _subscription>](&self, key: &$key) {
                if let Some(entry) = self.$field.get_mut(key) {
                    let ref_count = entry.1.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    if ref_count == 1 {
                        drop(entry); // Release the mutable reference before removing
                        self.$field.remove(key);
                    }
                }
            }

            pub fn [<notify_ $field _update>](&self, key: &$key, data: &$data_type) {
                if let Some(entry) = self.$field.get(key) {
                    let _ = entry.0.send(data.clone());
                }
            }
        }
    };
}

macro_rules! define_subscription_handlers_no_params {
    ($field:ident, $data_type:ty) => {
        paste::paste! {
            pub fn [<register_ $field _subscription>](&self) -> Result<broadcast::Receiver<$data_type>, ErrorObjectOwned> {
                self.$field.2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Ok(self.$field.0.subscribe());
            }

            pub fn [<unregister_ $field _subscription>](&self) {
                self.$field.2.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }

            pub fn [<notify_ $field _update>](&self, data: &$data_type) {
                let _ = self.$field.0.send(data.clone());
            }
        }
    };
}

impl SubscriptionProcessor {
    pub fn new() -> Self {
        let (all_blocks_sender, all_blocks_receiver) =
            broadcast::channel::<BlockWithTransactions>(SUBSCRIPTION_CHANNEL_SIZE);
        let (all_slots_sender, all_slots_receiver) = broadcast::channel::<Slot>(SUBSCRIPTION_CHANNEL_SIZE);
        Self {
            pubkey: dashmap::DashMap::new(),
            signature: dashmap::DashMap::new(),
            block_by_pubkey: dashmap::DashMap::new(),

            all_blocks: (
                all_blocks_sender,
                all_blocks_receiver,
                std::sync::atomic::AtomicUsize::new(0),
            ),
            all_slots: (
                all_slots_sender,
                all_slots_receiver,
                std::sync::atomic::AtomicUsize::new(0),
            ),
        }
    }

    define_subscription_handlers!(pubkey, AccountSharedData, Pubkey);
    define_subscription_handlers!(signature, TransactionStatus, Signature);
    define_subscription_handlers!(block_by_pubkey, BlockWithTransactions, Pubkey);

    define_subscription_handlers_no_params!(all_blocks, BlockWithTransactions);
    define_subscription_handlers_no_params!(all_slots, Slot);

    // called after commit to bank!
    pub fn notify_block_only(
        &self,
        block: &BlockWithTransactions,
        account_updates: HashMap<Pubkey, AccountSharedData>,
    ) {
        for tx in &block.transactions {
            let error = tx.metadata.status.clone().err();
            let tx_status = TransactionStatus::Executed(error, 0);
            for sig in &tx.transaction.signatures {
                self.notify_signature_update(sig, &tx_status);
            }
        }
        for (pubkey, account) in account_updates {
            self.notify_pubkey_update(&pubkey, &account);
            self.notify_block_by_pubkey_update(&pubkey, block);
        }

        self.notify_all_blocks_update(block);
        self.notify_all_slots_update(&block.slot);
    }
}

impl Default for SubscriptionProcessor {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Notifier {
    fn notify_pubkey_update(&self, pubkey: &Pubkey, account: &AccountSharedData);
    fn notify_signature_update(&self, signature: &Signature, status: &TransactionStatus);
    fn notify_block_by_pubkey_update(&self, pubkey: &Pubkey, block: &BlockWithTransactions);
    fn notify_all_blocks_update(&self, block: &BlockWithTransactions);
    fn notify_all_slots_update(&self, slot: &Slot);
}

impl Notifier for SubscriptionProcessor {
    fn notify_pubkey_update(&self, pubkey: &Pubkey, account: &AccountSharedData) {
        self.notify_pubkey_update(pubkey, account);
    }

    fn notify_signature_update(&self, signature: &Signature, status: &TransactionStatus) {
        self.notify_signature_update(signature, status);
    }

    fn notify_block_by_pubkey_update(&self, pubkey: &Pubkey, block: &BlockWithTransactions) {
        self.notify_block_by_pubkey_update(pubkey, block);
    }

    fn notify_all_blocks_update(&self, block: &BlockWithTransactions) {
        self.notify_all_blocks_update(block);
    }

    fn notify_all_slots_update(&self, slot: &Slot) {
        self.notify_all_slots_update(slot);
    }
}

pub struct NoopNotifier;

impl Notifier for NoopNotifier {
    fn notify_pubkey_update(&self, _pubkey: &Pubkey, _account: &AccountSharedData) {}
    fn notify_signature_update(&self, _signature: &Signature, _status: &TransactionStatus) {}
    fn notify_block_by_pubkey_update(&self, _pubkey: &Pubkey, _block: &BlockWithTransactions) {}
    fn notify_all_blocks_update(&self, _block: &BlockWithTransactions) {}
    fn notify_all_slots_update(&self, _slot: &Slot) {}
}
