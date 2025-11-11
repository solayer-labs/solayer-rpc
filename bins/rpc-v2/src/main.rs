use std::{
    collections::VecDeque,
    error::Error,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use base64::Engine;
use clap::Parser;
use infinisvm_core::{bank::Bank, indexer::Indexer, subscription::SubscriptionProcessor};
use infinisvm_indexer::{
    db::{MultiDatabaseIndexer, NoopIndexer},
    in_memory::InMemoryIndexer,
    s3::S3FsClient,
};
use infinisvm_jsonrpc::{rpc_impl::RpcServer, rpc_state::RpcIndexer};
use infinisvm_logger::{error, info};
use infinisvm_sync::{
    grpc::{client::SyncClient, server::InfiniSVMServiceImpl, TransactionBatchBroadcaster},
    http_client::HttpClient,
    SyncState,
};
use infinisvm_types::sync::grpc::{CommitBatchNotification, RawSlot};
use jsonrpsee::server::Server;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::{mpsc, Mutex, RwLock as TokioRwLock};
use tonic::transport::Server as TonicServer;

mod cold_start;
mod memory;
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

    /// Sequencer HTTP server address, overrides sequencer_host
    #[arg(long, default_value = "")]
    sequencer_http_server_addr: String,

    /// JSON-RPC listen addr (host:port)
    #[arg(long, default_value = "127.0.0.1:18899")]
    listen_addr: String,

    /// Prometheus metrics listen address
    #[arg(long, default_value = "127.0.0.1:3002")]
    metric_addr: SocketAddr,

    /// Number of threads to use
    #[arg(short, long, default_value = "10")]
    num_threads: u64,

    /// TPU server address
    #[arg(long, default_value = "127.0.0.1:5005", value_parser = parse_socket_addr)]
    tpu_host: SocketAddr,

    /// Cassandra host addresses (optional, comma-delimited). If omitted, uses
    /// in-memory indexer. default_value = "127.0.0.1:9042"
    #[arg(long, value_delimiter = ',')]
    pub cassandra_hosts: Option<Vec<String>>,

    /// Cassandra instance replication factor (optional, defaults to 1)
    #[arg(long)]
    pub cassandra_replication_factor: Option<u8>,

    /// Sequencer RPC server address, overrides sequencer_host
    #[arg(long, default_value = "")]
    sequencer_rpc_server_addr: String,

    #[arg(long, default_value = "/mnt/data/slots-internal")]
    pub local_slots_path: PathBuf,

    /// S3 region (optional) for storing slots
    #[arg(long, default_value = "us-west-2")]
    pub s3_region: Option<String>,

    #[arg(long, default_value = "s3://solayer-devnet/")]
    pub s3_path: String,

    /// S3 access key id (optional) for storing slots
    #[arg(long)]
    pub s3_access_key_id: Option<String>,

    /// S3 secret key (optional) for storing slots
    #[arg(long)]
    pub s3_secret_key: Option<String>,

    /// One or more Ed25519 server public keys (hex/base58/base64) for TLS
    /// verification
    #[arg(long, value_delimiter = ',')]
    pub grpc_server_pubkeys: Vec<String>,

    /// Trust this server certificate/CA for TLS (PEM). Useful for self-signed
    /// certs.
    #[arg(long)]
    pub grpc_server_cert: Option<String>,
}

type BoxError = Box<dyn Error + Send + Sync>;

async fn create_indexer(args: &Args) -> (Arc<Mutex<dyn Indexer>>, Arc<dyn RpcIndexer>) {
    // Fallback to in-memory indexer if Cassandra hosts are not provided
    let hosts = args.cassandra_hosts.as_deref().unwrap_or(&[]);
    if hosts.is_empty() {
        info!("No Cassandra hosts provided; using in-memory indexer");
        let indexer = Arc::new(Mutex::new(NoopIndexer));
        let rpc_indexer = Arc::new(InMemoryIndexer::new());
        return (indexer, rpc_indexer);
    }

    // Region will be determined from S3_REGION env var or default to us-west-2
    let s3 = S3FsClient::new_with_credentials(
        args.local_slots_path.clone(),
        args.s3_access_key_id.clone(),
        args.s3_secret_key.clone(),
        args.s3_path.clone(),
        args.s3_region.clone(),
    );

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

    let cassandra_indexer = Arc::new(Mutex::new(MultiDatabaseIndexer::new(pools, Some(s3.clone()))));
    let cassandra_indexer_rpc = Arc::new(MultiDatabaseIndexer::new(readonly_pools, Some(s3.clone())));

    (cassandra_indexer, cassandra_indexer_rpc)
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
    allowed_server_pubkeys: Vec<[u8; 32]>,
    root_ca_pem: Option<Vec<u8>>,
    use_tls: bool,
}

fn config_error(msg: impl Into<String>) -> BoxError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, msg.into()).into()
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
) -> Result<Arc<TokioRwLock<SyncState>>, BoxError> {
    let (sync_state_inner, latest_slot_receiver) = SyncState::new(RawSlot::default());
    let sync_state = Arc::new(TokioRwLock::new(sync_state_inner));
    let grpc_service_impl =
        InfiniSVMServiceImpl::new(sync_state.clone(), latest_slot_receiver, batch_broadcaster).await;
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

    Ok(sync_state)
}

fn prepare_grpc_client_config(args: &Args) -> Result<GrpcClientConfig, BoxError> {
    let address = if args.sequencer_grpc_server_addr.is_empty() {
        format!("http://{}:5005", args.sequencer_host)
    } else {
        args.sequencer_grpc_server_addr.clone()
    };

    let allowed_server_pubkeys: Vec<[u8; 32]> = args
        .grpc_server_pubkeys
        .iter()
        .filter_map(|s| parse_pubkey(s))
        .collect();

    let root_ca_pem: Option<Vec<u8>> = match &args.grpc_server_cert {
        Some(path) => match std::fs::read(path) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                eprintln!("Failed to read --grpc-server-cert {path}: {e}");
                None
            }
        },
        None => None,
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
            "No port found for gRPC server address '{address}'; must specify port (e.g., https://host:port)"
        ))
    })?;

    let use_tls = !allowed_server_pubkeys.is_empty() || root_ca_pem.is_some();

    Ok(GrpcClientConfig {
        host,
        port,
        allowed_server_pubkeys,
        root_ca_pem,
        use_tls,
    })
}

async fn connect_grpc_clients(config: &GrpcClientConfig, num_threads: u64) -> Result<Vec<SyncClient>, BoxError> {
    info!(
        "Connecting to gRPC server at: {}:{} - {} threads",
        config.host, config.port, num_threads
    );

    let mut clients = Vec::new();

    for i in 0..num_threads {
        let scheme = if config.use_tls { "https" } else { "http" };
        let client_addr = format!("{}://{}:{}", scheme, config.host, config.port + i as u16);
        info!(
            "Connecting gRPC client {} to {} (tls={})",
            i, client_addr, config.use_tls
        );
        let client = if config.use_tls {
            SyncClient::connect_with_tls(
                &client_addr,
                Default::default(),
                if config.allowed_server_pubkeys.is_empty() {
                    None
                } else {
                    Some(config.allowed_server_pubkeys.clone())
                },
                config.root_ca_pem.clone(),
            )
            .await?
        } else {
            SyncClient::connect(&client_addr).await?
        };
        info!("gRPC client {} connected", i);
        clients.push(client);
    }

    info!("Successfully connected to gRPC server");
    Ok(clients)
}

async fn subscribe_and_forward_streams(
    clients: &mut [SyncClient],
    batch_broadcaster: Arc<TransactionBatchBroadcaster>,
    sync_state: Arc<TokioRwLock<SyncState>>,
) -> Result<
    (
        Vec<mpsc::Receiver<Arc<CommitBatchNotification>>>,
        Vec<mpsc::Receiver<RawSlot>>,
    ),
    BoxError,
> {
    let mut tx_receivers = Vec::new();
    let mut slot_receivers = Vec::new();
    for (i, client) in clients.iter_mut().enumerate() {
        info!("Subscribing transactions stream on client {}", i);
        let tx_receiver = client.subscribe_transactions().await?;
        info!("Subscribed transactions stream on client {}", i);
        tx_receivers.push(tx_receiver);

        info!("Subscribing slots stream on client {}", i);
        let slot_receiver = client.subscribe_slots().await?;
        info!("Subscribed slots stream on client {}", i);
        slot_receivers.push(slot_receiver);
    }

    let tx_receivers = tx_receivers
        .into_iter()
        .enumerate()
        .map(|(i, mut upstream_rx)| {
            let (forward_tx, forward_rx) = mpsc::channel(1024);
            let broadcaster_clone = batch_broadcaster.clone();
            tokio::spawn(async move {
                info!("Transaction forwarder {} started", i);
                while let Some(batch) = upstream_rx.recv().await {
                    let shared_batch = Arc::new(batch);
                    if let Err(e) = broadcaster_clone.publish_notification(shared_batch.clone()) {
                        error!("Forwarder {} failed to publish batch: {}", i, e);
                    }
                    if forward_tx.send(shared_batch).await.is_err() {
                        break;
                    }
                }
                info!("Transaction forwarder {} terminated", i);
            });
            forward_rx
        })
        .collect::<Vec<_>>();

    let slot_receivers = slot_receivers
        .into_iter()
        .enumerate()
        .map(|(i, mut upstream_rx)| {
            let (forward_tx, forward_rx) = mpsc::channel(1024);
            let sync_state_clone = sync_state.clone();
            tokio::spawn(async move {
                info!("Slot forwarder {} started", i);
                while let Some(slot) = upstream_rx.recv().await {
                    {
                        let mut state = sync_state_clone.write().await;
                        state.latest_slot = slot.clone();
                        state.notify_new_slot(slot.clone());
                    }
                    // if forward_tx.send(slot).await.is_err() {
                    //     break;
                    // }
                }
                info!("Slot forwarder {} terminated", i);
            });
            forward_rx
        })
        .collect::<Vec<_>>();

    Ok((tx_receivers, slot_receivers))
}

async fn create_refetch_pool(
    config: &GrpcClientConfig,
    num_threads: u64,
) -> Result<Arc<Vec<tokio::sync::Mutex<SyncClient>>>, BoxError> {
    let mut refetch_clients = Vec::new();
    for i in 0..num_threads {
        let scheme = if config.use_tls { "https" } else { "http" };
        let client_addr = format!("{}://{}:{}", scheme, config.host, config.port + i as u16);
        let client = if config.use_tls {
            SyncClient::connect_with_tls(
                &client_addr,
                Default::default(),
                if config.allowed_server_pubkeys.is_empty() {
                    None
                } else {
                    Some(config.allowed_server_pubkeys.clone())
                },
                config.root_ca_pem.clone(),
            )
            .await?
        } else {
            SyncClient::connect(&client_addr).await?
        };
        refetch_clients.push(tokio::sync::Mutex::new(client));
    }

    Ok(Arc::new(refetch_clients))
}

fn build_http_server_addr(args: &Args) -> String {
    if args.sequencer_http_server_addr.is_empty() {
        format!("http://{}:6005", args.sequencer_host)
    } else {
        args.sequencer_http_server_addr.clone()
    }
}

fn build_sequencer_rpc_addr(args: &Args) -> String {
    if args.sequencer_rpc_server_addr.is_empty() {
        format!("http://{}:8899", args.sequencer_host)
    } else {
        args.sequencer_rpc_server_addr.clone()
    }
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

    let batch_broadcaster = Arc::new(TransactionBatchBroadcaster::new());
    let sync_state = setup_downstream_grpc(batch_broadcaster.clone(), args.grpc_listen_addr).await?;

    let grpc_config = prepare_grpc_client_config(&args)?;
    let mut clients = connect_grpc_clients(&grpc_config, args.num_threads).await?;
    let (tx_receivers, slot_receivers) =
        subscribe_and_forward_streams(&mut clients, batch_broadcaster.clone(), sync_state.clone()).await?;

    let http_client = Arc::new(HttpClient::new(build_http_server_addr(&args)));

    let snapshots = http_client.get_snapshots().await?;
    info!("Successfully got snapshots: {:?}", snapshots.get_ckpts_to_download());

    let (indexer, indexer_rpc) = create_indexer(&args).await;

    let exit = Arc::new(AtomicBool::new(false));
    let bank = Arc::new(RwLock::new(Bank::new_slave(exit.clone())));

    // handle subscriptions from others
    let subscription_processor = Arc::new(SubscriptionProcessor::new());
    let total_transaction_count = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(RwLock::new((Instant::now(), VecDeque::new())));

    let refetch_pool = create_refetch_pool(&grpc_config, args.num_threads).await?;

    info!(
        "Launching cold_start with {} tx streams and {} slot streams",
        tx_receivers.len(),
        slot_receivers.len()
    );
    let (handles, db_chain) = cold_start::cold_start(
        http_client,
        tx_receivers,
        slot_receivers,
        indexer,
        bank.clone(),
        subscription_processor.clone(),
        refetch_pool,
    )
    .await?;
    {
        bank.write().unwrap().set_db(db_chain.clone());
    }

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
