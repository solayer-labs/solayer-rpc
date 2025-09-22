# InfiniSVM RPC

Rust implementation of InfiniSVM RPC V2.  

### Hardware Requirement

Chain: 

| Chain Name | Requirement |
|------------|------------|
| internalnet       | 2TB NVMe + 64 Cores + 1TB DDR5 RAM |
| mainnet-alpha       |  2TB NVMe + 64 Cores + 256GB DDR5 RAM |


ScyllaDB Database: 

| Chain Name | Requirement With Signature Indexing | Requirement Without Signature Indexing |
|------------|------------|------------|
| internalnet       | 3 Node of (32TB NVMe + 128 Cores + 1TB DDR5 RAM) | Single Node 8TB NVMe + 128 Cores + 1TB DDR5 RAM |
| mainnet-alpha       | Single Node 8TB NVMe + 128 Cores + 1TB DDR5 RAM | Single Node 8TB NVMe + 128 Cores + 1TB DDR5 RAM |

### Step 1: Setup ScyllaDB

Install and set up ScyllaDB: https://docs.scylladb.com/manual/stable/getting-started/install-scylla/. We recommend the multiple-node setup. 

Initialize ScyllaDB with `cassandra_tables.sql`. 

Note: the code is also compatible with Cassandra, but we do not recommend using it. 

### Step 2: Setup RPC

Setup environment:
```bash

sudo apt install unzip clang cmake pkg-config libssl-dev build-essential python3-pip protobuf-compiler aria2 -y
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
. "$HOME/.cargo/env" 

```

Run the RPC:
```bash
./target/release/rpc-v2 \
    --cassandra-hosts <COMMA SEPARATED LIST OF LOCAL SCYLLADB HOSTS> \
    --cassandra-replication-factor 10 \
    --s3-path <S3 PATH / LOCAL PATH> \
    --grpc-server-pubkeys <SEQUENCER TLS KEY> \
    --tpu-host <SEQUENCER TPU HOST> \
    --sequencer-host <SEQUENCER RPC> \
    --host <SEQUENCER gRPC HOST> \
    --host <SEQUENCER gRPC PORT> \
    --http-host <SEQUENCER HTTP HOST> \
    --http-port <SEQUENCER HTTP PORT>
```

### Sequencer Keys

| Chain Name | Public Key |
|------------|------------|
| internalnet       | 5dad9b08e48e7649a344edf52e9c3ffd9bd1b230f38a567ab45cdcc565cc190d |
| mainnet-alpha       | 5dad9b08e48e7649a344edf52e9c3ffd9bd1b230f38a567ab45cdcc565cc190d |
