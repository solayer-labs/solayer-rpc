/* --------------------------------------------------------- */
/* 3.  (Optional) drop the whole keyspace – removes           */
/*     any remaining objects under `indexer_dev`.            */
/* --------------------------------------------------------- */
DROP KEYSPACE IF EXISTS indexer_dev;

CREATE KEYSPACE IF NOT EXISTS indexer_dev WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS indexer_dev.tx (
    slot                 bigint,
    signature            blob,
    transaction          blob,
    result               blob,
    pre_accounts         blob,
    block_unix_timestamp bigint,
    seq_number           bigint,
    PRIMARY KEY (signature)
) WITH compression = {'sstable_compression': ''};

CREATE TABLE IF NOT EXISTS indexer_dev.account_ops (
  owner   blob,
  account blob,
  PRIMARY KEY (owner, account)
);

CREATE TABLE IF NOT EXISTS indexer_dev.account_ops_mint (
  owner        blob,
  account_type tinyint,
  mint         blob,
  account      blob,
  PRIMARY KEY (owner, account, account_type)
);

CREATE TABLE IF NOT EXISTS indexer_dev.slots (
    slot                 bigint,
    block_unix_timestamp bigint,
    blockhash           blob,
    parent_blockhash    blob,
    PRIMARY KEY (slot)
);


CREATE TABLE IF NOT EXISTS indexer_dev.signatures (
    account blob,
    seq_number bigint,
    signature blob,
    slot bigint,
    block_unix_timestamp bigint,
    PRIMARY KEY  (account, seq_number)
);
