use std::{
    collections::VecDeque,
    error::Error,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use base64::Engine;
use clap::Parser;
use dashmap::DashSet;
use eyre::Context as _;
use infinisvm_core::{bank::Bank, indexer::Indexer, s3::S3FsClient, subscription::SubscriptionProcessor};
use infinisvm_db::persistence::DB_DIRECTORY;
#[cfg(not(feature = "no_index"))]
use infinisvm_indexer::db::MultiDatabaseIndexer;
use infinisvm_indexer::{db::NoopIndexer, in_memory::InMemoryIndexer};
use infinisvm_jsonrpc::{rpc_impl::RpcServer, rpc_state::RpcIndexer};
use infinisvm_logger::{error, info, warn};
use infinisvm_registry::RegistryStore;
use infinisvm_sync::{
    grpc::{client::SyncClient, server::InfiniSVMServiceImpl, TransactionBatchBroadcaster},
    http_client::HttpClient,
};
use infinisvm_types::sync::{CommitBatchNotification, SignedFinalization};
use jsonrpsee::server::Server;
use metrics_exporter_prometheus::PrometheusBuilder;
use solana_sdk::{hash::hashv, pubkey::Pubkey};
use tokio::sync::{mpsc, watch, Mutex};
use tonic::{transport::Server as TonicServer, Code};

use crate::cold_start::{
    slots_sync_progress::{SlotsSyncProgress, SlotsSyncProgressRecorder},
    StartSlot,
};

mod cold_start;
mod memory;
mod p2p;
mod pyroscope;

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(feature = "track_oom")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

fn parse_socket_addr(s: &str) -> Result<SocketAddr, String> {
    match s.parse() {
        Ok(addr) => Ok(addr),
        Err(_) => Err(format!("Invalid socket address: {s}")),
    }
}

#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// Sequencer server address
    #[arg(long, short = 's', default_value = "127.0.0.1")]
    sequencer_host: String,

    /// Sequencer gRPC server address, overrides sequencer_host
    #[arg(long, default_value = "")]
    sequencer_grpc_server_addr: String,

    /// Local gRPC listen address for downstream subscribers
    #[arg(long, value_parser = parse_socket_addr, default_value = "0.0.0.0:15005")]
    grpc_listen_addr: SocketAddr,

    /// Public gRPC address to advertise/register in /rpc/set.
    ///
    /// This is useful when `--grpc-listen-addr` uses a bind address like
    /// `0.0.0.0` / `[::]`, which is not dialable by other peers.
    ///
    /// Examples: `127.0.0.1:15005`, `rpc-v2:15005`.
    #[arg(long, default_value = "", env = "RPC_GRPC_ADVERTISE_ADDR")]
    grpc_advertise_addr: String,

    /// Sequencer HTTP server address, overrides sequencer_host
    #[arg(long, default_value = "")]
    sequencer_http_server_addr: String,

    /// Local HTTP listen address for serving /rpc/set (registry) and snapshot
    /// endpoints.
    ///
    /// Defaults to `grpc_listen_addr.port + registry_port_offset`.
    #[arg(long, value_parser = parse_socket_addr)]
    http_listen_addr: Option<SocketAddr>,

    /// Port offset used to derive a peer's HTTP registry port from its gRPC
    /// port.
    ///
    /// By convention, sequencer defaults to gRPC `:5005` and HTTP `:6005`
    /// (+1000).
    #[arg(long, default_value_t = 1000)]
    registry_port_offset: u16,

    /// RPC registry HTTP server addresses (comma-separated or repeated)
    #[arg(long, value_delimiter = ',', default_value = "")]
    rpc_registry_addrs: Vec<String>,

    /// JSON-RPC listen addr (host:port)
    #[arg(long, default_value = "127.0.0.1:18899")]
    listen_addr: String,

    /// Prometheus metrics listen address
    #[arg(long, default_value = "127.0.0.1:3002")]
    metric_addr: SocketAddr,

    /// TPU server address
    #[arg(long, default_value = "127.0.0.1:5005", value_parser = parse_socket_addr)]
    tpu_host: SocketAddr,

    /// Cassandra host addresses (optional, comma-delimited). If omitted, uses
    /// in-memory indexer. default_value = "127.0.0.1:9042"
    #[cfg(not(feature = "no_index"))]
    #[arg(long, value_delimiter = ',')]
    pub cassandra_hosts: Option<Vec<String>>,

    /// Cassandra instance replication factor (optional, defaults to 1)
    #[cfg(not(feature = "no_index"))]
    #[arg(long)]
    pub cassandra_replication_factor: Option<u8>,

    /// Sequencer RPC server address, overrides sequencer_host
    #[arg(long, default_value = "")]
    sequencer_rpc_server_addr: String,

    /// Sequencer public key for signed finalizer verification
    /// (hex/base58/base64)
    #[arg(long)]
    sequencer_pubkey: String,

    /// Enable e2e gRPC injection endpoint
    #[arg(long, default_value_t = false)]
    e2e_enable: bool,

    /// If set, do not connect to sequencer gRPC on startup.
    ///
    /// Instead, wait for a usable upstream gRPC peer to be discovered via the
    /// RPC registry (see `--rpc-registry-addrs`). Defaults to `true`; pass
    /// `--wait-for-grpc-peer false` to disable.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    wait_for_grpc_peer: bool,

    /// Max seconds to wait for a usable gRPC peer when `--wait-for-grpc-peer`
    /// is enabled. `0` means wait forever.
    #[arg(long, default_value_t = 0)]
    wait_for_grpc_peer_timeout_secs: u64,

    #[arg(long, default_value = "/mnt/data/slots-rpc")]
    pub local_slots_path: PathBuf,

    /// S3 region (optional) for storing slots
    #[arg(long, default_value = "us-west-2", env = "S3_REGION")]
    pub s3_region: String,

    #[arg(long, default_value = "s3://solayer-devnet")]
    pub s3_path: String,

    /// S3 access key id (optional) for storing slots
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    pub s3_access_key_id: Option<String>,

    /// S3 secret key (optional) for storing slots
    #[arg(long, env = "S3_SECRET_KEY")]
    pub s3_secret_key: Option<String>,

    /// Starting point for slot backfill: 'latest', 'checkpoint', or slot number
    #[arg(long, default_value = "latest")]
    pub start_slot: StartSlot,
}

type BoxError = Box<dyn Error + Send + Sync>;

#[cfg(not(feature = "no_index"))]
async fn create_indexer(args: &Args) -> (Arc<Mutex<dyn Indexer>>, Arc<dyn RpcIndexer>) {
    // Fallback to in-memory indexer if Cassandra hosts are not provided
    let hosts = args.cassandra_hosts.as_deref().unwrap_or(&[]);
    if hosts.is_empty() {
        info!("No Cassandra hosts provided; using in-memory indexer");
        let indexer = Arc::new(Mutex::new(NoopIndexer));
        let rpc_indexer = Arc::new(InMemoryIndexer::new());
        return (indexer, rpc_indexer);
    }

    let rep_factor = args.cassandra_replication_factor.unwrap_or(1);
    let mut pools = Vec::with_capacity(hosts.len());
    let mut readonly_pools = Vec::with_capacity(hosts.len());
    for host in hosts {
        for _ in 0..rep_factor {
            {
                // Create a connection to Cassandra
                let host_splitted = host.split(':').collect::<Vec<&str>>();
                assert!(host_splitted.len() == 2, "Invalid host: {host}");
                let host = host_splitted[0];
                let port = host_splitted[1].parse::<u16>().unwrap();
                let cassandra_pool = infinisvm_indexer::db::CassandraIndexerDB::new(host, port).await;

                pools.push((
                    cassandra_pool.clone(), // TX
                    cassandra_pool.clone(), // SLOT
                    cassandra_pool.clone(), // SIGNATURE
                    cassandra_pool.clone(), // ACCOUNT
                ));
                readonly_pools.push((
                    cassandra_pool.clone(), // TX
                    cassandra_pool.clone(), // SLOT
                    cassandra_pool.clone(), // SIGNATURE
                    cassandra_pool.clone(), // ACCOUNT
                ));
            }
            info!("Connected to Cassandra: {}", host);
        }
    }
    // Set S3 to None temporarily so that we don't read from S3 when serving
    // getBlockWithTransactions
    let cassandra_indexer = Arc::new(Mutex::new(MultiDatabaseIndexer::new(pools, None)));
    let cassandra_indexer_rpc = Arc::new(MultiDatabaseIndexer::new(readonly_pools, None));

    (cassandra_indexer, cassandra_indexer_rpc)
}

#[cfg(feature = "no_index")]
async fn create_indexer(_args: &Args) -> (Arc<Mutex<dyn Indexer>>, Arc<dyn RpcIndexer>) {
    info!("Indexing disabled via feature 'no_index'; using in-memory/noop indexers");
    let indexer = Arc::new(Mutex::new(NoopIndexer));
    let rpc_indexer = Arc::new(InMemoryIndexer::new());
    (indexer, rpc_indexer)
}

fn parse_pubkey(s: &str) -> Option<[u8; 32]> {
    if let Ok(bytes) = hex::decode(s) {
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            return Some(arr);
        }
    }

    if let Ok(bytes) = bs58::decode(s).into_vec() {
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            return Some(arr);
        }
    }

    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s) {
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            return Some(arr);
        }
    }

    info!("Invalid pubkey: {}", s);
    None
}

struct GrpcClientConfig {
    host: String,
    port: u16,
}

fn config_error(msg: impl Into<String>) -> BoxError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, msg.into()).into()
}

fn normalize_grpc_addr(addr: &str) -> String {
    if addr.starts_with("http://") {
        addr.to_string()
    } else if addr.starts_with("https://") {
        panic!("https:// gRPC addresses are not supported: {addr}");
    } else {
        format!("http://{addr}")
    }
}

async fn wait_for_usable_grpc_peer(peer_manager: &Arc<p2p::PeerManager>, timeout_secs: u64) -> Result<(), BoxError> {
    let timeout = (timeout_secs != 0).then(|| Duration::from_secs(timeout_secs));
    let start = Instant::now();
    let mut last_log = Instant::now();

    loop {
        if peer_manager.pick_stream_peer().is_some() {
            return Ok(());
        }

        if let Some(timeout) = timeout {
            if start.elapsed() >= timeout {
                return Err(config_error(format!(
                    "Timed out waiting for a usable gRPC peer after {}s",
                    timeout_secs
                )));
            }
        }

        if last_log.elapsed() >= Duration::from_secs(5) {
            let peer_count = peer_manager.peer_handles().len();
            let head = peer_manager.current_head();
            info!(
                "Waiting for a usable upstream gRPC peer (peers={}, head={})",
                peer_count, head
            );
            last_log = Instant::now();
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn seed_initial_peer_from_grpc_config(
    manager: &Arc<p2p::PeerManager>,
    grpc_config: &GrpcClientConfig,
) -> Result<(), BoxError> {
    let mut status_client = connect_grpc_clients(grpc_config).await?;
    let status = status_client.get_peer_status().await.ok();
    let node_id = status
        .as_ref()
        .map(|s| s.node_id)
        .unwrap_or_else(|| hashv(&[format!("{}:{}", grpc_config.host, grpc_config.port).as_bytes()]).to_bytes());

    let stream_client = Arc::new(Mutex::new(status_client));
    let rpc_client = Arc::new(Mutex::new(connect_grpc_clients(grpc_config).await?));
    manager.upsert_peer(
        node_id,
        format!("{}:{}", grpc_config.host, grpc_config.port),
        stream_client,
        rpc_client,
        status,
    );
    Ok(())
}

fn init_metrics(metric_addr: SocketAddr) -> Result<(), BoxError> {
    PrometheusBuilder::new()
        .with_http_listener(metric_addr)
        .install()
        .map(|_| ())
        .map_err(|e| e.into())
}

async fn setup_downstream_grpc(
    batch_broadcaster: Arc<TransactionBatchBroadcaster>,
    grpc_listen_addr: SocketAddr,
    grpc_advertise_addr: Option<String>,
    e2e_sender: Option<mpsc::Sender<p2p::PeerNotification>>,
) -> Result<(), BoxError> {
    let grpc_service_impl =
        InfiniSVMServiceImpl::new(batch_broadcaster, grpc_listen_addr, grpc_advertise_addr, e2e_sender).await;
    let grpc_service = grpc_service_impl.into_service();

    tokio::spawn(async move {
        info!("rpc-v2 gRPC server listening on {}", grpc_listen_addr);
        if let Err(e) = TonicServer::builder()
            .tcp_nodelay(true)
            .add_service(grpc_service)
            .serve(grpc_listen_addr)
            .await
        {
            error!("rpc-v2 gRPC server failed: {}", e);
        }
    });

    Ok(())
}

fn normalize_grpc_advertise_addr(addr: &str) -> String {
    addr.trim()
        .trim_end_matches('/')
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

fn is_loopback_host(host: &str) -> bool {
    let trimmed = host.trim();
    if trimmed.eq_ignore_ascii_case("localhost") {
        return true;
    }
    trimmed.parse::<IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false)
}

fn grpc_advertise_addr(args: &Args) -> Option<String> {
    if !args.grpc_advertise_addr.trim().is_empty() {
        return Some(args.grpc_advertise_addr.trim().to_string());
    }

    // Only derive from listen addr when it's routable.
    if args.grpc_listen_addr.ip().is_unspecified() {
        // For local/dev setups we frequently bind on 0.0.0.0 but still want a
        // dialable address in the registry.
        if is_loopback_host(&args.sequencer_host) {
            Some(format!("127.0.0.1:{}", args.grpc_listen_addr.port()))
        } else {
            None
        }
    } else {
        Some(args.grpc_listen_addr.to_string())
    }
}

async fn register_with_registry(base_url: &str, grpc_addr: String) -> Result<(), BoxError> {
    let http = HttpClient::new(base_url.to_string());
    http.register_rpc_peer(grpc_addr, 0.0).await?;
    Ok(())
}

fn spawn_registry_registration_task(
    registry_addrs: Vec<String>,
    grpc_addr: Option<String>,
    mut signed_finalization_slot: watch::Receiver<u64>,
) {
    let Some(grpc_addr) = grpc_addr else {
        warn!(
            "Skipping registry self-registration: no routable gRPC address; set --grpc-advertise-addr (or RPC_GRPC_ADVERTISE_ADDR)"
        );
        return;
    };

    tokio::spawn(async move {
        if *signed_finalization_slot.borrow() == 0 {
            info!("Waiting for signed finalization before registry registration");
            while signed_finalization_slot.changed().await.is_ok() {
                if *signed_finalization_slot.borrow() > 0 {
                    break;
                }
            }
        }

        // Keep trying (light backoff) so that rpc-v2 registers even if the
        // registry isn't reachable at startup, and periodically refresh so we
        // re-appear after registry restarts/evictions.
        let mut registered = std::collections::HashSet::<String>::new();
        let mut backoff = Duration::from_secs(1);
        loop {
            let mut all_ok = true;
            for base_url in &registry_addrs {
                match register_with_registry(base_url, grpc_addr.clone()).await {
                    Ok(()) => {
                        if registered.insert(base_url.clone()) {
                            info!(
                                base_url = base_url.as_str(),
                                grpc_addr = grpc_addr.as_str(),
                                "Registered with rpc registry"
                            );
                        }
                    }
                    Err(e) => {
                        all_ok = false;
                        registered.remove(base_url.as_str());
                        warn!(
                            base_url = base_url.as_str(),
                            "Failed to register with rpc registry: {e}"
                        );
                    }
                }
            }

            if all_ok {
                backoff = Duration::from_secs(1);
                tokio::time::sleep(Duration::from_secs(30)).await;
            } else {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    });
}

fn spawn_registry_fisherman_task(registry: RegistryStore, sequencer_pubkey: Pubkey) {
    tokio::spawn(async move {
        let poll_secs = std::env::var("RPC_REGISTRY_FISHERMAN_POLL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(60);
        let max_recent_offset = std::env::var("RPC_REGISTRY_FISHERMAN_MAX_OFFSET")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5);
        let probe_timeout_ms = std::env::var("RPC_REGISTRY_FISHERMAN_PROBE_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(3000);
        let initial_delay_secs = std::env::var("RPC_REGISTRY_FISHERMAN_INITIAL_DELAY_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let config = infinisvm_registry::fisherman::FishermanConfig {
            poll_secs,
            max_recent_offset,
            probe_timeout: Duration::from_millis(probe_timeout_ms),
            initial_delay: Duration::from_secs(initial_delay_secs),
        };

        let probe = infinisvm_sync::registry_fisherman::GrpcPeerProbe::new(sequencer_pubkey);
        infinisvm_registry::fisherman::RegistryFisherman::new(registry, probe, config)
            .run()
            .await;
    });
}

fn prepare_grpc_client_config(args: &Args) -> Result<GrpcClientConfig, BoxError> {
    let address = if args.sequencer_grpc_server_addr.is_empty() {
        format!("http://{}:5005", args.sequencer_host)
    } else {
        normalize_grpc_addr(&args.sequencer_grpc_server_addr)
    };

    let url = address
        .parse::<url::Url>()
        .map_err(|e| config_error(format!("Invalid gRPC server address '{address}': {e}")))?;

    let host = url
        .host_str()
        .ok_or_else(|| config_error(format!("Missing host in gRPC server address '{address}'")))?
        .to_string();

    let port = url.port_or_known_default().ok_or_else(|| {
        config_error(format!(
            "No port found for gRPC server address '{address}'; must specify port (e.g., http://host:port)"
        ))
    })?;

    Ok(GrpcClientConfig { host, port })
}

async fn connect_grpc_clients(config: &GrpcClientConfig) -> Result<SyncClient, BoxError> {
    info!("Connecting to gRPC server at: {}:{}", config.host, config.port);

    let client_addr = format!("http://{}:{}", config.host, config.port);
    info!("Connecting gRPC client to {}", client_addr);
    let client = SyncClient::connect(&client_addr).await?;
    Ok(client)
}

async fn subscribe_and_forward_streams(
    peer_manager: Arc<p2p::PeerManager>,
    sequencer_pubkey: Pubkey,
    batch_broadcaster: Arc<TransactionBatchBroadcaster>,
    forward_tx: mpsc::Sender<p2p::PeerNotification>,
    signed_finalization_slot: watch::Sender<u64>,
) -> Result<(), BoxError> {
    let broadcaster_clone = batch_broadcaster.clone();
    tokio::spawn(async move {
        info!("Transaction forwarder started");
        loop {
            let Some(peer) = peer_manager.pick_stream_peer() else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            let mut stream = {
                let mut client = peer.stream_client.lock().await;
                match client.subscribe_commit_batch_notifications().await {
                    Ok(rx) => {
                        peer_manager.mark_stream_ready(peer.node_id);
                        rx
                    }
                    Err(e) => {
                        error!("Failed to subscribe to stream from {}: {}", peer.grpc_addr, e);
                        if let Some(status) = e.downcast_ref::<tonic::Status>() {
                            if status.code() == Code::ResourceExhausted {
                                peer_manager.mark_rate_limit(peer.node_id);
                                peer_manager.mark_max_streams(peer.node_id);
                            }
                        } else if e.to_string().contains("ResourceExhausted") {
                            peer_manager.mark_rate_limit(peer.node_id);
                            peer_manager.mark_max_streams(peer.node_id);
                        }
                        peer_manager.mark_stream_drop(peer.node_id);
                        peer_manager.mark_failure(peer.node_id);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };

            loop {
                match stream.recv().await {
                    Some(batch) => {
                        match &batch {
                            CommitBatchNotification::SignedFinalization(sf) => {
                                if verify_signed_finalization(sf, &sequencer_pubkey) {
                                    peer_manager.observe_signed_finalization(peer.node_id, sf.finalization.slot);
                                    let _ = signed_finalization_slot.send(sf.finalization.slot);
                                } else {
                                    peer_manager.penalize_invalid_finalizer(peer.node_id);
                                    break;
                                }
                            }
                            CommitBatchNotification::Finalization(finalization) => {
                                error!(
                                    "Received unsigned finalization for slot {} from {}; evicting peer",
                                    finalization.slot, peer.grpc_addr
                                );
                                peer_manager.penalize_invalid_finalizer(peer.node_id);
                                break;
                            }
                            CommitBatchNotification::Batch(_) => {}
                        }

                        let shared_batch = Arc::new(batch);
                        if let Ok(size) = bincode::serialized_size(shared_batch.as_ref()) {
                            peer_manager.observe_bytes(peer.node_id, size);
                        }
                        if let Err(e) = broadcaster_clone.publish_notification(shared_batch.clone()) {
                            error!("Failed to publish batch: {}", e);
                        }
                        if forward_tx
                            .send(p2p::PeerNotification {
                                peer_id: peer.node_id,
                                peer_addr: peer.grpc_addr.clone(),
                                notification: shared_batch,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }

                        if peer_manager.stream_should_failover(peer.node_id) {
                            info!("Failing over from stream peer {}", peer.grpc_addr);
                            break;
                        }
                    }
                    None => {
                        peer_manager.mark_stream_drop(peer.node_id);
                        peer_manager.mark_failure(peer.node_id);
                        break;
                    }
                }
            }
        }
    });
    Ok(())
}

fn verify_signed_finalization(sf: &SignedFinalization, sequencer_pubkey: &Pubkey) -> bool {
    if sf.sequencer_pubkey != sequencer_pubkey.to_bytes() {
        return false;
    }
    let msg = match bincode::serialize(&sf.finalization) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let sig = solana_sdk::signature::Signature::from(sf.signature);
    sig.verify(sequencer_pubkey.as_ref(), &msg)
}

fn build_http_server_addr(args: &Args) -> String {
    if args.sequencer_http_server_addr.is_empty() {
        format!("http://{}:6005", args.sequencer_host)
    } else {
        args.sequencer_http_server_addr.clone()
    }
}

fn build_registry_addrs(args: &Args) -> Vec<String> {
    let mut addrs: Vec<String> = args
        .rpc_registry_addrs
        .iter()
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .collect();
    if addrs.is_empty() {
        addrs.push(build_http_server_addr(args));
    }
    addrs
}

fn build_sequencer_rpc_addr(args: &Args) -> String {
    if args.sequencer_rpc_server_addr.is_empty() {
        format!("http://{}:8899", args.sequencer_host)
    } else {
        args.sequencer_rpc_server_addr.clone()
    }
}

fn normalize_http_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

fn derive_registry_base_from_grpc_addr(grpc_addr: &str, offset: u16) -> Option<String> {
    let trimmed = grpc_addr.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }

    let mut url = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        url::Url::parse(trimmed).ok()?
    } else {
        url::Url::parse(&format!("http://{trimmed}")).ok()?
    };

    let port = url.port()?;
    let derived = port.checked_add(offset)?;
    url.set_scheme("http").ok()?;
    url.set_port(Some(derived)).ok()?;
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string().trim_end_matches('/').to_string())
}

fn derive_http_addr_from_grpc(grpc: SocketAddr, offset: u16) -> SocketAddr {
    let port = grpc.port().checked_add(offset).unwrap_or_else(|| grpc.port());
    SocketAddr::new(grpc.ip(), port)
}

fn main() -> Result<(), BoxError> {
    #[cfg(feature = "track_oom")]
    memory::init_jemalloc_profiling();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed building the Runtime");
    runtime.block_on(do_main())
}

async fn do_main() -> Result<(), BoxError> {
    infinisvm_logger::console();
    #[cfg(feature = "pyroscope")]
    pyroscope::init_pyroscope("rpc-v2");

    let args = Args::parse();
    init_metrics(args.metric_addr)?;

    let http_listen_addr = args
        .http_listen_addr
        .unwrap_or_else(|| derive_http_addr_from_grpc(args.grpc_listen_addr, args.registry_port_offset));

    // Registry store shared with our HTTP server and peer discovery.
    let rpc_registry = RegistryStore::new();

    // Determine what gRPC address we should advertise to other peers.
    let advertised_grpc_addr = grpc_advertise_addr(&args);
    let advertised_grpc_addr_normalized = advertised_grpc_addr.as_deref().map(normalize_grpc_advertise_addr);

    // Cache a stable ID for ourselves (used to avoid self-peering when polling
    // registries). This must match the registry's ID derivation (hash of the
    // advertised gRPC address).
    let self_grpc_identity = advertised_grpc_addr_normalized
        .clone()
        .unwrap_or_else(|| args.grpc_listen_addr.to_string());
    let self_node_id = hashv(&[self_grpc_identity.as_bytes()]).to_bytes();

    // Only advertise ourselves in our local /rpc/set registry if the listen
    // address is routable. `0.0.0.0` / `[::]` is a bind address, not a valid
    // peer address.
    if let Some(advertised) = advertised_grpc_addr_normalized.as_ref() {
        if !advertised.is_empty() {
            rpc_registry.upsert_peer(self_node_id, advertised.clone(), 0.0).await;
        }
    }

    // Always seed our local registry from env so this node can act as a seed.
    // (The http server also calls this, but doing it here keeps the store
    // populated even if the server fails to start.)
    if let Ok(seeds) = std::env::var("RPC_REGISTRY_SEEDS") {
        let seeds = seeds.trim();
        if !seeds.is_empty() {
            for raw in seeds.split(',') {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let normalized = trimmed
                    .trim_end_matches('/')
                    .trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .to_string();
                if normalized.is_empty() {
                    continue;
                }
                let node_id = hashv(&[normalized.as_bytes()]).to_bytes();
                rpc_registry.upsert_peer(node_id, normalized, 0.0).await;
            }
        }
    }

    let sequencer_pubkey_bytes = parse_pubkey(&args.sequencer_pubkey)
        .ok_or_else(|| config_error(format!("Invalid --sequencer-pubkey '{}'", args.sequencer_pubkey)))?;
    let sequencer_pubkey = Pubkey::new_from_array(sequencer_pubkey_bytes);
    spawn_registry_fisherman_task(rpc_registry.clone(), sequencer_pubkey);

    let batch_broadcaster = Arc::new(TransactionBatchBroadcaster::new());
    let (forward_tx, forward_rx) = mpsc::channel(1024);
    let e2e_sender = if args.e2e_enable {
        Some(forward_tx.clone())
    } else {
        None
    };
    setup_downstream_grpc(
        batch_broadcaster.clone(),
        args.grpc_listen_addr,
        advertised_grpc_addr.clone(),
        e2e_sender,
    )
    .await?;

    let grpc_config = prepare_grpc_client_config(&args)?;
    // Start local HTTP server (serves /rpc/set registry + snapshots).
    let rpc_registry_for_http = rpc_registry.clone();
    let http_db_path = DB_DIRECTORY.to_string();
    let http_slots_path = args.local_slots_path.to_string_lossy().to_string();
    let sequencer_http_addr = build_http_server_addr(&args);
    tokio::spawn(async move {
        if let Err(e) = infinisvm_sync::http::start_http_server(
            http_listen_addr,
            http_db_path,
            http_slots_path,
            rpc_registry_for_http,
            Some(sequencer_pubkey),
        )
        .await
        {
            error!("rpc-v2 http server failed: {e}");
        }
    });

    let http_client = Arc::new(HttpClient::new(sequencer_http_addr));

    let snapshots = http_client.get_snapshots().await?;
    info!("Successfully got snapshots: {:?}", snapshots.get_ckpts_to_download());

    let (indexer, indexer_rpc) = create_indexer(&args).await;

    let exit = Arc::new(AtomicBool::new(false));
    let bank = Arc::new(RwLock::new(Bank::new_slave(exit.clone())));

    // handle subscriptions from others
    let subscription_processor = Arc::new(SubscriptionProcessor::new());
    let total_transaction_count = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(RwLock::new((Instant::now(), VecDeque::new())));

    let registry_addrs = build_registry_addrs(&args)
        .into_iter()
        .map(|addr| normalize_http_addr(&addr))
        .collect::<Vec<_>>();
    let registry_clients: Vec<Arc<p2p::registry_client::RegistryClient>> = registry_addrs
        .iter()
        .cloned()
        .map(|addr| Arc::new(p2p::registry_client::RegistryClient::new(addr)))
        .collect();
    if registry_clients.is_empty() {
        return Err(config_error("No registry addresses configured"));
    }

    // Track known registry endpoints so we can keep discovering peers by asking
    // newly found nodes for their /rpc/set.
    let known_registries: Arc<DashSet<String>> = Arc::new(DashSet::new());
    for client in &registry_clients {
        known_registries.insert(client.base_url().to_string());
    }

    let (signed_finalization_slot_tx, signed_finalization_slot_rx) = watch::channel(0u64);
    let peer_manager = Arc::new(p2p::PeerManager::new(sequencer_pubkey));

    let peer_manager_registry = peer_manager.clone();
    let registry_poll_clients = registry_clients.clone();
    let local_registry = rpc_registry.clone();
    let known_registries_for_task = known_registries.clone();
    tokio::spawn(async move {
        let poll_secs = std::env::var("RPC_REGISTRY_POLL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30);
        let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
        let mut initial_sync = true;
        loop {
            if !initial_sync {
                interval.tick().await;
            }
            initial_sync = false;

            let mut missing = 0usize;
            // Build the full set of registry endpoints to poll.
            let mut poll_clients: Vec<Arc<p2p::registry_client::RegistryClient>> = Vec::new();
            poll_clients.extend(registry_poll_clients.iter().cloned());
            for base_url in known_registries_for_task.iter() {
                let base_url = base_url.to_string();
                if poll_clients.iter().any(|c| c.base_url() == base_url) {
                    continue;
                }
                poll_clients.push(Arc::new(p2p::registry_client::RegistryClient::new(base_url)));
            }

            for registry_client in poll_clients.iter() {
                let Ok(peers) = registry_client.list().await else {
                    continue;
                };
                for peer in peers {
                    if peer.node_id == self_node_id {
                        continue;
                    }

                    // Merge into our local registry so other nodes can learn from us.
                    local_registry
                        .upsert_peer(peer.node_id, peer.grpc_addr.clone(), peer.score_hint)
                        .await;

                    // Learn the peer's registry endpoint based on the conventional
                    // gRPC->HTTP offset so we can query its /rpc/set in future polls.
                    if let Some(base) = derive_registry_base_from_grpc_addr(&peer.grpc_addr, args.registry_port_offset)
                    {
                        known_registries_for_task.insert(base);
                    }

                    let addr = normalize_grpc_addr(&peer.grpc_addr);
                    let mut status_client = match SyncClient::connect(&addr).await {
                        Ok(client) => client,
                        Err(_) => {
                            missing += 1;
                            continue;
                        }
                    };

                    let status = status_client.get_peer_status().await.ok();
                    let node_id = status.as_ref().map(|s| s.node_id).unwrap_or(peer.node_id);
                    if peer_manager_registry.has_peer(node_id) {
                        continue;
                    }

                    let stream_client = Arc::new(Mutex::new(status_client));
                    let rpc_client = match SyncClient::connect(&addr).await {
                        Ok(client) => Arc::new(Mutex::new(client)),
                        Err(_) => {
                            missing += 1;
                            continue;
                        }
                    };
                    peer_manager_registry.upsert_peer(node_id, peer.grpc_addr, stream_client, rpc_client, status);
                }
            }

            if missing > 0 {
                warn!("Registry sync pending {} peers; retrying", missing);
            }
        }
    });

    let peer_manager_probe = peer_manager.clone();
    tokio::spawn(async move {
        let poll_secs = std::env::var("RPC_PEER_STATUS_POLL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10);
        let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
        loop {
            interval.tick().await;
            for peer in peer_manager_probe.peer_handles() {
                let mut client = peer.rpc_client.lock().await;
                if let Ok(status) = client.get_peer_status().await {
                    peer_manager_probe.update_peer_status(peer.node_id, status);
                }
            }
        }
    });

    if !args.e2e_enable {
        if args.wait_for_grpc_peer {
            match wait_for_usable_grpc_peer(&peer_manager, args.wait_for_grpc_peer_timeout_secs).await {
                Ok(()) => {
                    info!("Found usable gRPC peer from registry bootstrap");
                }
                Err(wait_err) => {
                    if args.wait_for_grpc_peer_timeout_secs == 0 {
                        return Err(wait_err);
                    }

                    warn!(
                        "{}; falling back to sequencer gRPC bootstrap at {}:{}",
                        wait_err, grpc_config.host, grpc_config.port
                    );
                    seed_initial_peer_from_grpc_config(&peer_manager, &grpc_config).await?;
                    wait_for_usable_grpc_peer(&peer_manager, args.wait_for_grpc_peer_timeout_secs).await?;
                }
            }
        } else {
            seed_initial_peer_from_grpc_config(&peer_manager, &grpc_config).await?;
        }
    }

    subscribe_and_forward_streams(
        peer_manager.clone(),
        sequencer_pubkey,
        batch_broadcaster.clone(),
        forward_tx.clone(),
        signed_finalization_slot_tx,
    )
    .await?;

    // Initialize slots sync progress recorder
    let progress_path = std::env::var("RPC_SLOTS_SYNC_PROGRESS_PATH")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/mnt/data"));
    let progress_recorder = match SlotsSyncProgress::open(progress_path) {
        Ok(progress) => {
            info!("Initialized slots sync progress recorder");
            Some(SlotsSyncProgressRecorder::new(progress))
        }
        Err(e) => {
            panic!("Failed to initialize slots sync progress recorder: {e}");
        }
    };

    let start_slot = args.start_slot.clone();
    info!("Configured start_slot={}", start_slot);

    let backfill_s3_client = if matches!(start_slot.clone(), StartSlot::Latest) {
        None
    } else if let (Some(s3_access_key_id), Some(s3_secret_key)) =
        (args.s3_access_key_id.clone(), args.s3_secret_key.clone())
    {
        // Extract bucket name from S3 URI (e.g., "s3://solayer-internalnet" ->
        // "solayer-internalnet")
        let s3_bucket_name = if let Some((bucket, _)) = parse_s3_uri(&args.s3_path) {
            bucket
        } else {
            // If not a URI, assume it's already just the bucket name
            args.s3_path.clone()
        };

        Some(
            S3FsClient::new_with_credentials(
                args.local_slots_path.clone(),
                s3_access_key_id,
                s3_secret_key,
                s3_bucket_name.clone(),
                args.s3_region.clone(),
            )
            .context("Failed to create S3FsClient")?,
        )
    } else {
        warn!("Backfill needed but S3 credentials are not configured");
        None
    };

    let (handles, db_chain) = cold_start::cold_start(
        http_client,
        forward_rx,
        indexer,
        bank.clone(),
        subscription_processor.clone(),
        peer_manager,
        progress_recorder,
        backfill_s3_client,
        sequencer_pubkey,
        start_slot,
    )
    .await?;
    {
        bank.write().unwrap().set_db(db_chain.clone());
    }

    spawn_registry_registration_task(
        registry_addrs.clone(),
        advertised_grpc_addr.clone(),
        signed_finalization_slot_rx,
    );

    let (tx_sender, tx_receiver) = crossbeam_channel::unbounded();
    // Drain the local sendTransaction channel to avoid unbounded growth; follower
    // forwards writes upstream
    std::thread::spawn(move || {
        while let Ok((_tx, _prio)) = tx_receiver.recv() {
            // Intentionally drop
            // rpc-v2 forwards writes to the sequencer via HTTP
        }
    });

    let jsonrpc_state = infinisvm_jsonrpc::rpc_state::RpcServerState::new(
        bank,
        db_chain,
        indexer_rpc,
        samples,
        total_transaction_count,
        tx_sender,
        Some(build_sequencer_rpc_addr(&args)),
        args.tpu_host,
        subscription_processor,
    );
    let module = jsonrpc_state.into_rpc();
    let cors = tower_http::cors::CorsLayer::new()
        // Allow `POST` and `OPTIONS` when accessing the resource
        .allow_methods([hyper::Method::POST, hyper::Method::OPTIONS, hyper::Method::GET])
        // Allow requests from any origin
        .allow_origin(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any)
        .max_age(Duration::from_secs(3600));
    let middleware = tower::ServiceBuilder::new().layer(cors);

    let server = Server::builder()
        .max_response_body_size(1024 * 1024 * 1024)
        .max_connections(10000)
        .max_subscriptions_per_connection(5000)
        .set_http_middleware(middleware)
        .build(args.listen_addr.clone())
        .await
        .unwrap();
    info!("Starting RPC server on {}", args.listen_addr);
    let rpc_handle = server.start(module);
    tokio::select! {
        _ = async {
            for (idx, handle) in handles.into_iter().enumerate() {
                match handle.await {
                    Ok(_) => info!("Background task #{} completed cleanly (unexpected)", idx),
                    Err(e) => error!("Background task #{} returned error: {}", idx, e),
                }
            }
        } => {
            panic!("One or more background tasks exited; see logs for details");
        }
        _ = rpc_handle.stopped() => {
            info!("RPC server stopped");
        }
    }

    Ok(())
}

fn parse_s3_uri(uri: &str) -> Option<(String, Option<String>)> {
    let trimmed = uri.trim();
    if !trimmed.starts_with("s3://") {
        return None;
    }

    let remainder = &trimmed[5..];
    if remainder.is_empty() {
        return None;
    }

    let mut parts = remainder.splitn(2, '/');
    let bucket = parts.next()?.trim();
    if bucket.is_empty() {
        return None;
    }

    let prefix = parts
        .next()
        .map(|s| s.trim_matches('/').to_string())
        .filter(|s| !s.is_empty());

    Some((bucket.to_string(), prefix))
}
