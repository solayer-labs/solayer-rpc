pub mod grpc;
pub mod http;
pub mod http_client;
pub mod registry_fisherman;
pub mod slots;

use std::{net::SocketAddr, sync::Arc, time::SystemTime};

// Re-export commonly used types and functions
pub use grpc::client::SyncClient;
pub use http::start_http_server;
use infinisvm_logger::{error, info};
use infinisvm_registry::RegistryStore;
use metrics::{counter, gauge};
use solana_sdk::pubkey::Pubkey;
use tonic::transport::Server;

use crate::grpc::{server::InfiniSVMServiceImpl, service::InfiniSvmServiceServer, TransactionBatchBroadcaster};

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
    grpc_rate_limit_per_sec: u32,
    grpc_rate_limit_burst: u32,
) -> eyre::Result<()> {
    let service = InfiniSVMServiceImpl::new(broadcaster, grpc_addr, None, None).await;

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
    ));

    Ok(())
}
