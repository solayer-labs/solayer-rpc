use crate::db::DatabaseIndexer;
use infinisvm_core::committer::ConsumedJob;
use infinisvm_core::indexer::Indexer;
use klickhouse::{bb8::Pool, ConnectionManager};
use solana_hash::Hash;
use solana_sdk::fee::FeeDetails;
use solana_sdk::message::SimpleAddressLoader;
use solana_sdk::signer::Signer;
use solana_sdk::system_program;
use solana_sdk::transaction::SanitizedTransaction;
use solana_sdk::transaction::TransactionError;
use solana_sdk::{
    message::{Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    transaction::{Transaction, VersionedTransaction},
};
use solana_svm::account_loader::FeesOnlyTransaction;
use solana_svm::account_loader::LoadedTransaction;
use solana_svm::rollback_accounts::RollbackAccounts;
use solana_svm::transaction_execution_result::ExecutedTransaction;
use solana_svm::transaction_processing_result::ProcessedTransaction;
use std::collections::HashSet;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

// This test requires a running Clickhouse server on localhost
mod tests {
    use std::thread::sleep;

    use infinisvm_core::scheduler::transaction_priority_id::TransactionId;
    use infinisvm_jsonrpc::rpc_state::RpcIndexer;
    use rand::seq::index;
    use solana_sdk::pubkey;

    use crate::db::{ClickhouseIndexerDB, IndexerDB};

    use super::*;

    fn test_indexer<DB: IndexerDB>(db: DB) {
        // Create a ClickhouseIndexer
        let mut indexer = DatabaseIndexer::new(db.clone(), db.clone(), db.clone(), db.clone(), None);

        // Test index_block
        let slot = 123456789;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let blockhash = Hash::new_unique();
        let parent_blockhash = Hash::new_unique();

        indexer.index_block(slot, timestamp, blockhash, parent_blockhash);

        // Create a test transaction
        let payer = Keypair::new();
        let recipient = Pubkey::new_unique();

        let instruction = solana_sdk::system_instruction::transfer(&payer.pubkey(), &recipient, 1000);

        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let recent_blockhash = blockhash;

        let tx = Transaction::new(&[&payer], message, recent_blockhash);
        let versioned_tx = VersionedTransaction::from(tx);

        // Convert to SanitizedTransaction
        let sanitized_tx = SanitizedTransaction::try_create(
            versioned_tx,
            blockhash,
            None,                          // is_gossip
            SimpleAddressLoader::Disabled, // slot_id
            &HashSet::new(),               // reserved_account_keys
        )
        .expect("Failed to create sanitized transaction");

        // Create a mock ProcessedTransaction
        // Note: This is a simplified mock since we can't easily create a real ProcessedTransaction
        let processed_tx = ProcessedTransaction::FeesOnly(Box::new(FeesOnlyTransaction {
            load_error: TransactionError::AccountInUse,
            rollback_accounts: RollbackAccounts::FeePayerOnly {
                fee_payer_account: Default::default(),
            },
            fee_details: FeeDetails::default(),
        }));

        // Create a ConsumedJob
        let consumed_job = ConsumedJob {
            transaction_id: TransactionId::new(0),
            timestamp: 0,
            sanitized_transaction: sanitized_tx,
            processed_transaction: Ok(processed_tx),
            slot,
            pre_accounts: vec![],
            job_id: 0,
        };

        // Test index_transactions
        indexer.index_transactions(vec![consumed_job], timestamp);
        indexer.index_block(slot, timestamp, blockhash, parent_blockhash);

        // Flush caches to ensure data is written
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            indexer.flush_tx_cache_async().await.unwrap();
            indexer.flush_signature_cache_async().await.unwrap();
            indexer.flush_account_ops_cache_async().await.unwrap();
        });
        sleep(Duration::from_millis(500));

        // Test get_transaction_by_signature
        let signature = Signature::new_unique();
        let tx_result = rt.block_on(indexer.get_transaction_by_signature_async(&signature));
        assert!(tx_result.is_none()); // We expect None since we're querying for a different signature than we indexed

        // Test get_transactions_by_slot
        let (txs_by_slot, _) = rt.block_on(indexer.get_transactions_by_slot_async(slot, 0, 100));
        assert!(!txs_by_slot.is_empty()); // We should get the transaction we indexed

        // Test get_signatures_by_account
        let signatures = rt.block_on(indexer.get_signatures_by_account_async(
            &payer.pubkey(),
            infinisvm_types::SignatureFilters::None,
            1,
        ));
        assert!(!signatures.is_empty()); // We should get the signature we indexed

        // Test RpcIndexer methods
        // Test find_accounts_owned_by
        let accounts = rt.block_on(indexer.find_accounts_owned_by(&payer.pubkey(), 10, 0));
        // Since we're using mock data, we don't expect actual results, just testing API
        assert!(accounts.is_empty());

        // Test find_token_accounts_owned_by
        let token_accounts =
            rt.block_on(indexer.find_token_accounts_owned_by(&payer.pubkey(), Some(spl_token::id()), None, 10, 0));
        assert!(token_accounts.is_empty());

        let token2022_accounts =
            rt.block_on(indexer.find_token_accounts_owned_by(&payer.pubkey(), Some(spl_token_2022::id()), None, 10, 0));
        assert!(token2022_accounts.is_empty());

        // Test with a non-token program id
        let non_token_accounts = rt.block_on(indexer.find_accounts_owned_by(&payer.pubkey(), 10, 0));
        assert!(non_token_accounts.is_empty());

        // Insert some fake account info
        let unique_owner = Keypair::new();
        rt.block_on(async {
            indexer
                .account_ops_create_cache
                .push((payer.pubkey(), unique_owner.pubkey()));
            indexer.flush_account_ops_cache_async().await.unwrap();
        });

        let accounts = rt.block_on(indexer.find_accounts_owned_by(&unique_owner.pubkey(), 10, 0));
        assert!(!accounts.is_empty());
        let token_accounts = rt.block_on(indexer.find_token_accounts_owned_by(
            &unique_owner.pubkey(),
            Some(spl_token_2022::id()),
            None,
            10,
            0,
        ));
        assert!(token_accounts.is_empty());

        let random_token = Keypair::new();
        rt.block_on(async {
            indexer.account_ops_mint_create_cache.push((
                payer.pubkey(),
                unique_owner.pubkey(),
                2,
                random_token.pubkey(),
            ));
            indexer.flush_account_ops_cache_async().await.unwrap();
        });
        let token_accounts = rt.block_on(indexer.find_token_accounts_owned_by(
            &unique_owner.pubkey(),
            Some(spl_token_2022::id()),
            None,
            10,
            0,
        ));
        assert!(!token_accounts.is_empty());

        let token_accounts = rt.block_on(indexer.find_token_accounts_owned_by(
            &unique_owner.pubkey(),
            Some(spl_token_2022::id()),
            Some(random_token.pubkey()),
            10,
            0,
        ));
        assert!(!token_accounts.is_empty());

        let accounts = rt.block_on(async {
            indexer
                .account_ops_delete_cache
                .push((payer.pubkey(), unique_owner.pubkey()));
            indexer.flush_account_ops_cache_async().await.unwrap();
            indexer.find_accounts_owned_by(&unique_owner.pubkey(), 10, 0).await
        });
        assert!(accounts.is_empty());

        let token_accounts = rt.block_on(async {
            indexer
                .account_ops_mint_delete_cache
                .push((payer.pubkey(), unique_owner.pubkey()));
            indexer.flush_account_ops_cache_async().await.unwrap();
            indexer
                .find_token_accounts_owned_by(&unique_owner.pubkey(), Some(spl_token_2022::id()), None, 10, 0)
                .await
        });
        assert!(token_accounts.is_empty());

        println!("ClickhouseIndexer test completed successfully!");
    }

    #[test]
    fn test_clickhouse_indexer() {
        // Create a runtime for async operations
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Create a connection to Clickhouse
        let manager = rt.block_on(async {
            ConnectionManager::new(
                "10.20.30.215:9000",
                klickhouse::ClientOptions {
                    username: "default".to_string(),
                    password: "Solayer123".to_string(),
                    default_database: "default".to_string(),
                    tcp_nodelay: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        });
        let pool = rt.block_on(async { Pool::builder().max_size(5).build(manager).await.unwrap() });

        // Create tables if they don't exist
        rt.block_on(async {
            let conn = pool.get().await.expect("Failed to get connection");

            // Drop tables if they exist (to start with a clean slate)
            let _ = conn.execute("DROP TABLE IF EXISTS tx").await;
            let _ = conn.execute("DROP TABLE IF EXISTS signatures").await;
            let _ = conn.execute("DROP TABLE IF EXISTS slots").await;
            let _ = conn.execute("DROP TABLE IF EXISTS account_ops").await;
            let _ = conn.execute("DROP TABLE IF EXISTS account_ops_mint").await;
            // Create tx table
            let create_tx = r#"
        CREATE TABLE IF NOT EXISTS tx (
            signature Array(UInt8),
            transaction Array(UInt8),
            result Array(UInt8),
            slot UInt64,
            pre_accounts Array(UInt8),
            block_unix_timestamp UInt64,
            seq_number UInt64
        ) ENGINE = MergeTree()
        ORDER BY (signature, slot)
        "#;

            // Create signatures table
            let create_signatures = r#"
        CREATE TABLE IF NOT EXISTS signatures (
            account Array(UInt8),
            signature Array(UInt8),
            slot UInt64,
            block_unix_timestamp UInt64,
            seq_number UInt64
        ) ENGINE = MergeTree()
        ORDER BY (account, slot)
        "#;

            // Create slots table
            let create_slots = r#"
        CREATE TABLE IF NOT EXISTS slots (
            slot UInt64,
            block_unix_timestamp UInt64,
            blockhash Array(UInt8),
            parent_blockhash Array(UInt8)
        ) ENGINE = MergeTree()
        ORDER BY slot
        "#;

            // Create account_ops table
            let create_account_ops = r#"
        CREATE TABLE IF NOT EXISTS account_ops (
            account Array(UInt8),
            owner Array(UInt8),
        ) ENGINE = MergeTree()
        ORDER BY (account, owner)
        "#;

            let create_account_ops_mint = r#"
        CREATE TABLE IF NOT EXISTS account_ops_mint (
            account Array(UInt8),
            owner Array(UInt8),
            account_type UInt8,
            mint Array(UInt8)
        ) ENGINE = MergeTree()
        ORDER BY (account, owner)   
        "#;

            conn.execute(create_tx).await.expect("Failed to create tx table");
            conn.execute(create_signatures)
                .await
                .expect("Failed to create signatures table");
            conn.execute(create_slots).await.expect("Failed to create slots table");
            conn.execute(create_account_ops)
                .await
                .expect("Failed to create account_ops table");
            conn.execute(create_account_ops_mint)
                .await
                .expect("Failed to create account_ops_mint table");
        });

        test_indexer(ClickhouseIndexerDB::new(pool.clone()));

        // Cleanup
        rt.block_on(async {
            let conn = pool.get().await.expect("Failed to get connection");
            let _ = conn.execute("DROP TABLE IF EXISTS tx").await;
            let _ = conn.execute("DROP TABLE IF EXISTS signatures").await;
            let _ = conn.execute("DROP TABLE IF EXISTS slots").await;
            let _ = conn.execute("DROP TABLE IF EXISTS account_ops").await;
            let _ = conn.execute("DROP TABLE IF EXISTS account_ops_mint").await;
        });
    }

    #[test]
    fn test_postgres_indexer() {
        let rt2 = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(10)
            .enable_all()
            .build()
            .unwrap();
        let db = rt2.block_on(async {
            let db = crate::db::PostgresIndexerDB::new(
                "postgresql://s:Solayer123@127.0.0.1:5432/indexer_dev?sslmode=disable",
            )
            .await;
            // Drop tables if they exist
            db.execute("DROP TABLE IF EXISTS tx").await.unwrap();
            db.execute("DROP TABLE IF EXISTS signatures").await.unwrap();
            db.execute("DROP TABLE IF EXISTS slots").await.unwrap();
            db.execute("DROP TABLE IF EXISTS account_ops").await.unwrap();
            db.execute("DROP TABLE IF EXISTS account_ops_mint").await.unwrap();

            // Create tx table
            let create_tx = r#"
            CREATE TABLE IF NOT EXISTS tx (
                signature BYTEA,
                transaction BYTEA,
                result BYTEA,
                slot BIGINT,
                pre_accounts BYTEA,
                block_unix_timestamp BIGINT,
                seq_number BIGINT
            )
            "#;

            // Create signatures table
            let create_signatures = r#"
            CREATE TABLE IF NOT EXISTS signatures (
                account BYTEA,
                signature BYTEA,
                slot BIGINT,
                block_unix_timestamp BIGINT,
                seq_number BIGINT
            )
            "#;

            // Create slots table
            let create_slots = r#"
            CREATE TABLE IF NOT EXISTS slots (
                slot BIGINT,
                block_unix_timestamp BIGINT,
                blockhash BYTEA,
                parent_blockhash BYTEA
            )
            "#;

            // Create account_ops table
            let create_account_ops = r#"
            CREATE TABLE IF NOT EXISTS account_ops (
                account BYTEA,
                owner BYTEA
            )
            "#;

            let create_account_ops_mint = r#"
            CREATE TABLE IF NOT EXISTS account_ops_mint (
                account BYTEA,
                owner BYTEA,
                account_type SMALLINT,
                mint BYTEA
            )
            "#;

            db.execute(create_tx).await.expect("Failed to create tx table");
            db.execute(create_signatures)
                .await
                .expect("Failed to create signatures table");
            db.execute(create_slots).await.expect("Failed to create slots table");
            db.execute(create_account_ops)
                .await
                .expect("Failed to create account_ops table");
            db.execute(create_account_ops_mint)
                .await
                .expect("Failed to create account_ops_mint table");

            db
        });

        test_indexer(db);
    }
}
