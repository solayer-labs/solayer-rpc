pub mod grpc;
pub mod http;
pub mod http_client;
pub mod registry_fisherman;
pub mod slots;
pub mod snapshot_manifest;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::SystemTime};

// Re-export commonly used types and functions
pub use grpc::client::SyncClient;
pub use http::start_http_server;
use infinisvm_logger::{error, info};
use infinisvm_registry::RegistryStore;
use metrics::{counter, gauge};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair as SolanaKeypair},
};
use tonic::transport::Server;

use crate::{
    grpc::{server::InfiniSVMServiceImpl, service::InfiniSvmServiceServer, TransactionBatchBroadcaster},
    snapshot_manifest::{spawn_snapshot_manifest_refresher, SnapshotManifestStore},
};

#[allow(clippy::too_many_arguments)]
pub async fn start_server(
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    db_path: String,
    slots_path: String,
    broadcaster: Arc<TransactionBatchBroadcaster>,
    rpc_registry: RegistryStore,
    // If provided, enables fisherman-like challenge on /rpc/register.
    sequencer_pubkey: Pubkey,
    snapshot_manifest_keypair: Option<PathBuf>,
    grpc_rate_limit_per_sec: u32,
    grpc_rate_limit_burst: u32,
) -> eyre::Result<()> {
    let topology_keypair = if let Some(keypair_path) = snapshot_manifest_keypair.as_ref() {
        Arc::new(read_keypair_file(keypair_path).map_err(|e| eyre::eyre!(e.to_string()))?)
    } else {
        Arc::new(SolanaKeypair::new())
    };
    let service = InfiniSVMServiceImpl::new(broadcaster, grpc_addr, None, true, topology_keypair, None).await;
    let snapshot_manifest_store = SnapshotManifestStore::default();

    if let Some(keypair_path) = snapshot_manifest_keypair {
        spawn_snapshot_manifest_refresher(snapshot_manifest_store.clone(), db_path.clone(), keypair_path);
    }

    info!("InfiniSVM gRPC Server listening on {}", grpc_addr);
    let grpc_port_label = grpc_addr.port().to_string();
    let start_ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    gauge!("grpc_server_last_start_ts", "port" => grpc_port_label.clone()).set(start_ts);
    gauge!("grpc_server_up", "port" => grpc_port_label.clone()).set(1.0);

    // Enable gRPC server with bincode message types
    // Note: This uses tonic's default protobuf transport but with bincode message
    // structs For full bincode transport, additional codec implementation would
    // be needed
    let grpc_service =
        InfiniSvmServiceServer::new(service).with_rate_limit(grpc_rate_limit_per_sec, grpc_rate_limit_burst);
    tokio::spawn({
        let grpc_port_label = grpc_port_label.clone();
        async move {
            // Tune gRPC server for lower latency and better h2 performance
            let server = Server::builder().tcp_nodelay(true);

            // These methods may not exist on all tonic versions; keep them grouped
            // to simplify potential future adjustments.
            #[allow(unused_mut)]
            let mut server = server;
            #[cfg(any())]
            {
                server = server
                    .http2_keepalive_interval(std::time::Duration::from_secs(10))
                    .http2_keepalive_timeout(std::time::Duration::from_secs(30))
                    .http2_adaptive_window(true);
            }

            if let Err(e) = server.add_service(grpc_service).serve(grpc_addr).await {
                error!("gRPC server failed: {}", e);
                counter!("grpc_server_failures_total", "port" => grpc_port_label.clone()).increment(1);
                gauge!("grpc_server_up", "port" => grpc_port_label.clone()).set(0.0);
                let error_ts = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();
                gauge!("grpc_server_last_error_ts", "port" => grpc_port_label.clone()).set(error_ts);
            }
        }
    });

    tokio::spawn(start_http_server(
        http_addr,
        db_path.clone(),
        slots_path.clone(),
        rpc_registry,
        Some(sequencer_pubkey),
        snapshot_manifest_store,
    ));

    Ok(())
}
