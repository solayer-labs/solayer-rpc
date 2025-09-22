CREATE DATABASE IF NOT EXISTS indexer_dev ON CLUSTER cluster_10S_1R;

DROP TABLE IF EXISTS indexer_dev.tx ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.signatures ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.slots ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.account_ops ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.account_ops_mint ON CLUSTER cluster_10S_1R;

DROP TABLE IF EXISTS indexer_dev.tx_dist ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.signatures_dist ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.slots_dist ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.account_ops_dist ON CLUSTER cluster_10S_1R;
DROP TABLE IF EXISTS indexer_dev.account_ops_mint_dist ON CLUSTER cluster_10S_1R;

CREATE TABLE IF NOT EXISTS indexer_dev.tx ON CLUSTER cluster_10S_1R  (
    signature Array(UInt8),
    transaction Array(UInt8),
    result Array(UInt8),
    slot UInt64,
    pre_accounts Array(UInt8),
    block_unix_timestamp UInt64,
    seq_number UInt64
) ENGINE = MergeTree()
ORDER BY (signature);


CREATE TABLE IF NOT EXISTS indexer_dev.signatures ON CLUSTER cluster_10S_1R (
    account Array(UInt8),
    signature Array(UInt8),
    slot UInt64,
    block_unix_timestamp UInt64,
    seq_number UInt64
) ENGINE = MergeTree()
ORDER BY (account, seq_number, signature);

CREATE TABLE IF NOT EXISTS indexer_dev.slots ON CLUSTER cluster_10S_1R (
    slot UInt64,
    block_unix_timestamp UInt64,
    blockhash Array(UInt8),
    parent_blockhash Array(UInt8)
) ENGINE = MergeTree()
ORDER BY slot;

CREATE TABLE IF NOT EXISTS indexer_dev.account_ops ON CLUSTER cluster_10S_1R (
    account Array(UInt8),
    owner Array(UInt8),
) ENGINE = MergeTree()
ORDER BY (owner);

CREATE TABLE IF NOT EXISTS indexer_dev.account_ops_mint ON CLUSTER cluster_10S_1R (
    account Array(UInt8),
    owner Array(UInt8),
    account_type UInt8,
    mint Array(UInt8)
) ENGINE = MergeTree()
ORDER BY (owner, account_type);

CREATE TABLE indexer_dev.tx_dist ON CLUSTER cluster_10S_1R
(
    `signature` Array(UInt8),
    `transaction` Array(UInt8),
    `result` Array(UInt8),
    `slot` UInt64,
    `pre_accounts` Array(UInt8),
    `block_unix_timestamp` UInt64,
    `seq_number` UInt64
)
ENGINE = Distributed('cluster_10S_1R', 'indexer_dev', 'tx', rand());

-- Add projections to signatures table for optimized queries
ALTER TABLE indexer_dev.signatures ON CLUSTER cluster_10S_1R
    ADD PROJECTION p_slot 
    (
        SELECT account, signature, slot, block_unix_timestamp, seq_number
        ORDER BY (account, slot, seq_number)
    );


-- Materialize the projections
ALTER TABLE indexer_dev.signatures ON CLUSTER cluster_10S_1R
    MATERIALIZE PROJECTION p_slot;


CREATE TABLE indexer_dev.signatures_dist ON CLUSTER cluster_10S_1R
(
    `account` Array(UInt8),
    `signature` Array(UInt8),
    `slot` UInt64,
    `block_unix_timestamp` UInt64,
    `seq_number` UInt64
)
ENGINE = Distributed('cluster_10S_1R', 'indexer_dev', 'signatures', rand());

CREATE TABLE indexer_dev.slots_dist ON CLUSTER cluster_10S_1R
(
    `slot` UInt64,
    `block_unix_timestamp` UInt64,
    `blockhash` Array(UInt8),
    `parent_blockhash` Array(UInt8)
)
ENGINE = Distributed('cluster_10S_1R', 'indexer_dev', 'slots', rand());

CREATE TABLE indexer_dev.account_ops_dist ON CLUSTER cluster_10S_1R
(
    `account` Array(UInt8),
    `owner` Array(UInt8),
)
ENGINE = Distributed('cluster_10S_1R', 'indexer_dev', 'account_ops', rand());

CREATE TABLE indexer_dev.account_ops_mint_dist ON CLUSTER cluster_10S_1R
(
    `account` Array(UInt8),
    `owner` Array(UInt8),
    `account_type` UInt8,
    `mint` Array(UInt8)
)
ENGINE = Distributed('cluster_10S_1R', 'indexer_dev', 'account_ops_mint', rand());


