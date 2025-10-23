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
use infinisvm_logger::{error, info, trace};
use infinisvm_sync::{
    grpc::{client::SyncClient, server::InfiniSVMServiceImpl, TransactionBatchBroadcaster},
    http_client::HttpClient,
    SyncState,
};
use jsonrpsee::server::Server;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::{mpsc, Mutex, RwLock as TokioRwLock};
use tonic::transport::Server as TonicServer;

mod bank;
mod cold_start;

#[cfg(not(feature = "track_memory"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(feature = "track_oom")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

#[cfg(feature = "track_memory")]
#[global_allocator]
static ALLOCATOR: cap::Cap<tikv_jemallocator::Jemalloc> = cap::Cap::new(tikv_jemallocator::Jemalloc, usize::MAX);

#[cfg(feature = "track_memory")]
#[no_mangle]
pub extern "C" fn get_memory_usage() -> usize {
    ALLOCATOR.allocated()
}

#[cfg(feature = "pyroscope")]
fn init_pyroscope(service_name: &str) {
    use pyroscope::PyroscopeAgent;
    use pyroscope_pprofrs::{pprof_backend, PprofConfig};

    let user = match std::env::var("PYROSCOPE_USER") {
        Ok(s) if !s.is_empty() => s,
        _ => "1107578".to_string(),
    };

    let password = match std::env::var("PYROSCOPE_PASSWORD") {
        Ok(s) if !s.is_empty() => s,
        _ => "...".to_string(),
    };

    let server = match std::env::var("PYROSCOPE_SERVER") {
        Ok(s) if !s.is_empty() => s,
        _ => "https://profiles-prod-001.grafana.net".to_string(),
    };
    let app_name = std::env::var("PYROSCOPE_APP_NAME").unwrap_or_else(|_| service_name.to_string());
    let sample_rate: u32 = std::env::var("PYROSCOPE_SAMPLE_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let backend = pprof_backend(PprofConfig::new().sample_rate(sample_rate).report_thread_name());

    let mut builder = PyroscopeAgent::builder(server, app_name)
        .basic_auth(user, password)
        .backend(backend);
    // Add a basic tag to distinguish services
    builder = builder.tags(vec![("service", service_name)]);

    // Optionally add auth token
    if let Ok(token) = std::env::var("PYROSCOPE_AUTH_TOKEN") {
        if !token.is_empty() {
            builder = builder.auth_token(token);
        }
    }

    match builder.build().and_then(|a| a.start()) {
        Ok(agent) => {
            info!("pyroscope up");
            // Leak the agent to keep it running for process lifetime
            let _ = Box::leak(Box::new(agent));
        }
        Err(e) => {
            eprintln!("Failed to start pyroscope: {e}");
        }
    }
}

#[cfg(not(feature = "pyroscope"))]
fn init_pyroscope(_service_name: &str) {}

fn parse_socket_addr(s: &str) -> Result<SocketAddr, String> {
    match s.parse() {
        Ok(addr) => Ok(addr),
        Err(_) => Err(format!("Invalid socket address: {s}")),
    }
}

#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// gRPC server address (e.g., "http://localhost:5005")
    #[arg(short = 'o', long, default_value = "localhost")]
    host: String,

    /// gRPC server port
    #[arg(short, long, default_value = "5005")]
    port: u16,

    /// Local gRPC listen address for downstream subscribers
    #[arg(long, value_parser = parse_socket_addr, default_value = "0.0.0.0:15005")]
    grpc_listen_addr: SocketAddr,

    /// HTTP sync server (host:port)
    #[arg(long, default_value = "localhost:6005")]
    http_addr: String,

    /// JSON-RPC listen addr (host:port)
    #[arg(long, default_value = "127.0.0.1:18899")]
    rpc_addr: String,

    /// Prometheus metrics listen address
    #[arg(long, default_value = "127.0.0.1:3002")]
    metric_addr: SocketAddr,

    /// Number of threads to use
    #[arg(short, long, default_value = "10")]
    num_threads: u64,

    /// TPU server address
    #[arg(long, default_value = "127.0.0.1:5005", value_parser = parse_socket_addr)]
    tpu_host: SocketAddr,

    /// Sequencer server address
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    sequencer_host: String,

    /// Cassandra host addresses (optional, comma-delimited). If omitted, uses in-memory indexer.
    /// default_value = "127.0.0.1:9042"
    #[arg(long, value_delimiter = ',')]
    pub cassandra_hosts: Option<Vec<String>>,

    /// Cassandra instance replication factor (optional, defaults to 1)
    #[arg(long)]
    pub cassandra_replication_factor: Option<u8>,

    #[arg(long, default_value = "s3://infinisvm-dev/")]
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

async fn create_indexer(args: &Args) -> (Arc<Mutex<dyn Indexer>>, Arc<dyn RpcIndexer>) {
    // Fallback to in-memory indexer if Cassandra hosts are not provided
    let hosts = args.cassandra_hosts.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);
    if hosts.is_empty() {
        info!("No Cassandra hosts provided; using in-memory indexer");
        let indexer = Arc::new(Mutex::new(NoopIndexer));
        let rpc_indexer = Arc::new(InMemoryIndexer::new());
        return (indexer, rpc_indexer);
    }

    let s3 = S3FsClient::new_with_credentials(
        PathBuf::from(args.s3_path.clone()),
        args.s3_access_key_id.clone(),
        args.s3_secret_key.clone(),
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

    let cassandra_indexer = Arc::new(Mutex::new(MultiDatabaseIndexer::new(pools, None, Some(s3.clone()))));
    let cassandra_indexer_rpc = Arc::new(MultiDatabaseIndexer::new(readonly_pools, None, Some(s3.clone())));

    (cassandra_indexer, cassandra_indexer_rpc)
}

#[cfg(feature = "track_oom")]
fn init_jemalloc_profiling() {
    use axum::{http::StatusCode, response::IntoResponse};

    std::thread::Builder::new()
        .name("seqPprof".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                pub async fn handle_get_heap() -> Result<impl IntoResponse, (StatusCode, String)> {
                    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
                    require_profiling_activated(&prof_ctl)?;
                    let pprof = prof_ctl
                        .dump_pprof()
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                    Ok(pprof)
                }

                /// Checks whether jemalloc profiling is activated an
                /// returns an error response if not.
                fn require_profiling_activated(
                    prof_ctl: &jemalloc_pprof::JemallocProfCtl,
                ) -> Result<(), (StatusCode, String)> {
                    if prof_ctl.activated() {
                        Ok(())
                    } else {
                        Err((axum::http::StatusCode::FORBIDDEN, "heap profiling not activated".into()))
                    }
                }

                pub async fn handle_get_heap_flamegraph() -> Result<impl IntoResponse, (StatusCode, String)> {
                    use axum::{body::Body, http::header::CONTENT_TYPE, response::Response};

                    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
                    require_profiling_activated(&prof_ctl)?;
                    let svg = prof_ctl
                        .dump_flamegraph()
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                    Response::builder()
                        .header(CONTENT_TYPE, "image/svg+xml")
                        .body(Body::from(svg))
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
                }

                let a = jemalloc_pprof::PROF_CTL.as_ref();
                if a.is_some() {
                    println!("Jemalloc profiling activated");
                } else {
                    println!("Jemalloc profiling not activated");
                }

                let app = axum::Router::new()
                    .route("/debug/pprof/heap", axum::routing::get(handle_get_heap))
                    .route(
                        "/debug/pprof/heap/flamegraph",
                        axum::routing::get(handle_get_heap_flamegraph),
                    );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        })
        .unwrap();
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "track_oom")]
    init_jemalloc_profiling();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed building the Runtime");
    runtime.block_on(do_main())
}

async fn do_main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logger
    infinisvm_logger::console();

    // Start Pyroscope if configured via env
    init_pyroscope("rpc-v2");

    let args = Args::parse();

    // Start Prometheus metrics exporter
    let builder = PrometheusBuilder::new().with_http_listener(args.metric_addr);
    builder.install().expect("Failed to install recorder/exporter");

    let batch_broadcaster = Arc::new(TransactionBatchBroadcaster::new());
    let (sync_state_inner, latest_slot_receiver) = SyncState::new((0, vec![], vec![], 0, vec![]));
    let sync_state = Arc::new(TokioRwLock::new(sync_state_inner));
    let grpc_service_impl =
        InfiniSVMServiceImpl::new(sync_state.clone(), latest_slot_receiver, batch_broadcaster.clone()).await;
    let grpc_service = grpc_service_impl.into_service();
    let grpc_listen_addr = args.grpc_listen_addr;
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

    info!(
        "Connecting to gRPC server at: {}:({}-{})",
        args.host,
        args.port,
        args.port + args.num_threads as u16 - 1
    );

    // Parse allowed server pubkeys if provided
    fn parse_pubkey(s: &str) -> Option<[u8; 32]> {
        // hex
        if let Ok(bytes) = hex::decode(s) {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                return Some(arr);
            }
        }
        // base58
        if let Ok(bytes) = bs58::decode(s).into_vec() {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                return Some(arr);
            }
        }
        // base64
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
    let use_tls = !allowed_server_pubkeys.is_empty() || root_ca_pem.is_some();

    // Create gRPC client
    let mut clients = Vec::new();
    for i in 0..args.num_threads {
        let scheme = if use_tls { "https" } else { "http" };
        let client_addr = format!("{}://{}:{}", scheme, args.host, args.port + i as u16);
        info!("Connecting gRPC client {} to {} (tls={})", i, client_addr, use_tls);
        let client = if use_tls {
            SyncClient::connect_with_tls(
                &client_addr,
                Default::default(),
                if allowed_server_pubkeys.is_empty() {
                    None
                } else {
                    Some(allowed_server_pubkeys.clone())
                },
                root_ca_pem.clone(),
            )
            .await?
        } else {
            SyncClient::connect(&client_addr).await?
        };
        info!("gRPC client {} connected", i);
        clients.push(client);
    }
    info!("Successfully connected to gRPC server");

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
                    let slot_tuple = (
                        slot.slot,
                        slot.blockhash.clone(),
                        slot.parent_blockhash.clone(),
                        slot.timestamp,
                        slot.job_ids.clone(),
                    );
                    {
                        let mut state = sync_state_clone.write().await;
                        state.latest_slot = slot_tuple.clone();
                        state.notify_new_slot(slot_tuple);
                    }
                    if forward_tx.send(slot).await.is_err() {
                        break;
                    }
                }
                info!("Slot forwarder {} terminated", i);
            });
            forward_rx
        })
        .collect::<Vec<_>>();

    let http_client = Arc::new(HttpClient::new(format!("http://{}", args.http_addr)));

    let snapshots = http_client.get_snapshots().await?;
    info!("Successfully got snapshots: {:?}", snapshots.get_ckpts_to_download());

    let (indexer, indexer_rpc) = create_indexer(&args).await;

    let exit = Arc::new(AtomicBool::new(false));
    let bank = Arc::new(RwLock::new(Bank::new_slave(exit.clone())));

    let subscription_processor = Arc::new(SubscriptionProcessor::new());
    let total_transaction_count = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(RwLock::new((Instant::now(), VecDeque::new())));

    // Create dedicated refetch clients (one per server) shared across tasks
    let mut refetch_clients = Vec::new();
    for i in 0..args.num_threads {
        let scheme = if use_tls { "https" } else { "http" };
        let client_addr = format!("{}://{}:{}", scheme, args.host, args.port + i as u16);
        let client = if use_tls {
            SyncClient::connect_with_tls(
                &client_addr,
                Default::default(),
                if allowed_server_pubkeys.is_empty() {
                    None
                } else {
                    Some(allowed_server_pubkeys.clone())
                },
                root_ca_pem.clone(),
            )
            .await?
        } else {
            SyncClient::connect(&client_addr).await?
        };
        refetch_clients.push(tokio::sync::Mutex::new(client));
    }
    let refetch_pool: Arc<Vec<tokio::sync::Mutex<SyncClient>>> = Arc::new(refetch_clients);

    info!(
        "Launching cold_start with {} tx streams and {} slot streams",
        tx_receivers.len(),
        slot_receivers.len()
    );
    let (handles, cold_start_result) = cold_start::cold_start(
        http_client,
        tx_receivers,
        slot_receivers,
        indexer,
        bank.clone(),
        subscription_processor.clone(),
        total_transaction_count.clone(),
        samples.clone(),
        refetch_pool,
    )
    .await?;
    {
        let mut bank_guard = bank.write().unwrap();
        bank_guard.set_db(cold_start_result.db_chain.clone());
    }

    let (tx_sender, tx_receiver) = crossbeam_channel::unbounded();
    // Drain the local sendTransaction channel to avoid unbounded growth; follower
    // forwards writes upstream
    std::thread::spawn(move || {
        while let Ok((_tx, _prio)) = tx_receiver.recv() {
            // Intentionally drop; rpc-v2 forwards writes to the sequencer via
            // HTTP
        }
    });

    let jsonrpc_state = infinisvm_jsonrpc::rpc_state::RpcServerState::new(
        bank,
        cold_start_result.db_chain,
        indexer_rpc,
        samples,
        total_transaction_count,
        tx_sender,
        Some(args.sequencer_host.to_string()),
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

    let middleware = tower::ServiceBuilder::new()
        .layer(tower_http::trace::TraceLayer::new_for_http().on_body_chunk(
            |chunk: &hyper::body::Bytes, latency: Duration, _: &jsonrpsee::tracing::Span| {
                trace!(?chunk, chunk.size = chunk.len(), ?latency, "Sending body chunk")
            },
        ))
        .layer(cors);

    let server = Server::builder()
        .max_response_body_size(1024 * 1024 * 1024)
        .max_connections(10000)
        .max_subscriptions_per_connection(5000)
        .set_http_middleware(middleware)
        .build(args.rpc_addr.clone())
        .await
        .unwrap();
    info!("Starting RPC server on {}", args.rpc_addr);
    let handle = server.start(module);
    info!("Background task count: {}", handles.len());
    // Wait for any handle to exit, then crash
    tokio::select! {
        _ = async {
            for (idx, handle) in handles.into_iter().enumerate() {
                match handle.await {
                    Ok(Ok(_)) => info!("Background task #{} completed cleanly (unexpected)", idx),
                    Ok(Err(e)) => error!("Background task #{} returned error: {}", idx, e),
                    Err(join_err) => error!("Background task #{} panicked or was cancelled: {}", idx, join_err),
                }
            }
        } => {
            panic!("One or more background tasks exited; see logs for details");
        }
        _ = handle.stopped() => {
            info!("RPC server stopped");
        }
    }

    Ok(())
}
