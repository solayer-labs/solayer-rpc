pub mod grpc;
pub mod http;
pub mod http_client;
pub mod state;
pub mod types;

use std::{net::SocketAddr, path::Path, sync::Arc};

// Re-export commonly used types and functions
pub use grpc::client::SyncClient;
pub use http::start_http_server;
use infinisvm_logger::{error, info};
use infinisvm_types::sync::grpc::infini_svm_service_server::InfiniSvmServiceServer;
use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
pub use state::SyncState;
use tokio::sync::RwLock;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use crate::grpc::{server::InfiniSVMServiceImpl, TransactionBatchBroadcaster};

pub async fn start_server(
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    db_path: String,
    latest_slot: (u64, Vec<u8>, Vec<u8>, u64, Vec<u64>),
    broadcaster: Arc<TransactionBatchBroadcaster>,
    // If provided, enables TLS for gRPC using a self-signed Ed25519 certificate generated
    // from the given private key material. The value can be either a file path to a PEM-encoded
    // Ed25519 PKCS#8 private key, or the PEM contents themselves.
    grpc_tls_ed25519_key: Option<String>,
) -> eyre::Result<Arc<RwLock<SyncState>>> {
    let (sync_state, latest_slot_receiver) = SyncState::new(latest_slot);
    let sync_state = Arc::new(RwLock::new(sync_state));

    let service = InfiniSVMServiceImpl::new(sync_state.clone(), latest_slot_receiver, broadcaster).await;

    info!("InfiniSVM gRPC Server listening on {}", grpc_addr);

    // Enable gRPC server with bincode message types
    // Note: This uses tonic's default protobuf transport but with bincode message
    // structs For full bincode transport, additional codec implementation would
    // be needed
    let grpc_service = InfiniSvmServiceServer::new(service);
    tokio::spawn(async move {
        // Tune gRPC server for lower latency and better h2 performance
        let mut server = Server::builder().tcp_nodelay(true);

        // If TLS is enabled, generate a self-signed Ed25519 certificate from the
        // provided key and configure tonic's server with it.
        if let Some(key_or_path) = grpc_tls_ed25519_key {
            // Install ring provider for rustls 0.23 if not already installed
            let _ = rustls::crypto::ring::default_provider().install_default();

            match load_identity_from_key_or_bundle(&key_or_path, grpc_addr) {
                Ok(identity) => {
                    server = server
                        .tls_config(ServerTlsConfig::new().identity(identity))
                        .expect("failed to apply TLS config");
                    info!("gRPC TLS enabled with Ed25519 self-signed certificate");
                }
                Err(e) => {
                    error!("Failed to initialize gRPC TLS: {}", e);
                }
            }
        }

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
        }
    });

    tokio::spawn(start_http_server(http_addr, db_path.clone(), sync_state.clone()));

    Ok(sync_state)
}

/// Load an Identity from an Ed25519 key (PEM or path) by generating a
/// self-signed end-entity certificate (CA=false) suitable for TLS server
/// authentication. If the input contains both a certificate and a private key,
/// we will prefer using the private key to generate a fresh leaf certificate to
/// avoid CA-as-leaf issues.
fn load_identity_from_key_or_bundle(key_or_path: &str, grpc_addr: SocketAddr) -> eyre::Result<Identity> {
    // Load PEM contents either directly or from file path
    let pem = if Path::new(key_or_path).exists() {
        std::fs::read_to_string(key_or_path)?
    } else {
        key_or_path.to_string()
    };

    // Try to extract a private key block
    let key_begin_markers = ["-----BEGIN PRIVATE KEY-----", "-----BEGIN ED25519 PRIVATE KEY-----"];
    let key_pem = key_begin_markers
        .iter()
        .find_map(|m| pem.find(m).map(|idx| pem[idx..].to_string()));

    if let Some(key_pem) = key_pem {
        // Build SANs: localhost + bound IP
        let mut sans = vec!["localhost".to_string()];
        let ip_str = grpc_addr.ip().to_string();
        if !sans.iter().any(|s| s == &ip_str) {
            sans.push(ip_str);
        }

        let mut params = CertificateParams::new(sans)?;
        params.is_ca = IsCa::NoCa;
        params.extended_key_usages.push(ExtendedKeyUsagePurpose::ServerAuth);

        // Use provided Ed25519 private key
        let keypair = KeyPair::from_pem(&key_pem)?;
        let cert = params.self_signed(&keypair)?;
        let cert_pem = cert.pem();
        return Ok(Identity::from_pem(cert_pem, key_pem));
    }

    // Fallback: input must contain both cert and key blocks
    let cert_begin = "-----BEGIN CERTIFICATE-----";
    let key_begin = "-----BEGIN ";
    let cert_start = pem
        .find(cert_begin)
        .ok_or_else(|| eyre::eyre!("missing certificate PEM in input"))?;
    let key_start = pem
        .find(key_begin)
        .ok_or_else(|| eyre::eyre!("missing key PEM in input"))?;
    let (cert_pem, key_pem) = if cert_start < key_start {
        // Certificate comes first, then key
        let cert_pem = pem[cert_start..key_start].trim().to_string();
        let key_pem = pem[key_start..].trim().to_string();
        (cert_pem, key_pem)
    } else {
        // Key comes first, then certificate
        let key_pem = pem[key_start..cert_start].trim().to_string();
        let cert_pem = pem[cert_start..].trim().to_string();
        (cert_pem, key_pem)
    };
    Ok(Identity::from_pem(cert_pem, key_pem))
}
