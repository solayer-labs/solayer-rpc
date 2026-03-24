use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use infinisvm_logger::info;
use infinisvm_registry::{RegistryStore, RpcPeerInfo, RpcRegisterRequest, RpcSetResponse};
use infinisvm_types::sync::SignedSnapshotManifest;
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::hashv, pubkey::Pubkey, signature::Signature};
use tower_http::compression::CompressionLayer;

use crate::{
    grpc::client::{RetryConfig, SyncClient},
    slots::{self, SlotData},
    snapshot_manifest::SnapshotManifestStore,
};

#[derive(Clone)]
pub struct AppState {
    pub db_path: String,
    pub slots_path: String,
    pub rpc_registry: RegistryStore,
    pub sequencer_pubkey: Option<Pubkey>,
    pub snapshot_manifest_store: SnapshotManifestStore,
}

#[derive(Serialize)]
pub struct SnapshotsResponse {
    pub files: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct BatchSlotsResponse {
    pub slots: std::collections::HashMap<u64, SlotData>,
}

#[derive(Deserialize)]
struct BatchSlotQuery {
    min_slot: Option<u64>,
    max_slot: Option<u64>,
}

// Standalone HTTP server function
pub async fn start_http_server(
    addr: SocketAddr,
    db_path: String,
    slots_path: String,
    rpc_registry: RegistryStore,
    sequencer_pubkey: Option<Pubkey>,
    snapshot_manifest_store: SnapshotManifestStore,
) -> eyre::Result<()> {
    let app_state = Arc::new(AppState {
        db_path,
        slots_path,
        rpc_registry,
        sequencer_pubkey,
        snapshot_manifest_store,
    });

    seed_registry_from_env(&app_state.rpc_registry).await;

    let app = Router::new()
        .route("/solayer/snapshots", get(handle_snapshots))
        .route("/solayer/snapshot-manifest", get(handle_snapshot_manifest))
        .route("/solayer/files/{filename}", get(handle_files))
        .route("/solayer/slots/{slot}", get(handle_single_slot))
        .route("/solayer/slots", get(handle_batch_slots))
        .route("/solayer/slots/{slot}/info", get(handle_slot_info))
        .route("/solayer/slots/{slot}/shards/{shard}", get(handle_slot_shard))
        .route("/rpc/set", get(handle_rpc_set))
        .route("/rpc/register", post(handle_rpc_register))
        .layer(CompressionLayer::new())
        .with_state(app_state);

    info!("InfiniSVM HTTP Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn seed_registry_from_env(registry: &RegistryStore) {
    if registry.len().await > 0 {
        return;
    }
    let seeds = match std::env::var("RPC_REGISTRY_SEEDS") {
        Ok(value) => value,
        Err(_) => return,
    };
    let seeds = seeds.trim();
    if seeds.is_empty() {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut peers = Vec::new();
    for raw in seeds.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = normalize_seed_addr(trimmed);
        if normalized.is_empty() {
            continue;
        }
        let node_id = hashv(&[normalized.as_bytes()]).to_bytes();
        peers.push(RpcPeerInfo {
            node_id,
            grpc_addr: normalized,
            last_seen_ts: now,
            score_hint: 0.0,
        });
    }
    if !peers.is_empty() {
        registry.set_peers(peers).await;
    }
}

fn is_dialable_grpc_addr(addr: &str) -> bool {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        return false;
    }
    let host = trimmed.split_once(':').map(|(h, _)| h).unwrap_or(trimmed).trim();

    // Disallow typical bind-all hosts.
    if host == "0.0.0.0" || host == "::" || host == "[::]" {
        return false;
    }

    true
}

fn normalize_seed_addr(addr: &str) -> String {
    addr.trim()
        .trim_end_matches('/')
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

// Handler for snapshots API
async fn handle_snapshots(State(state): State<Arc<AppState>>) -> Response {
    if !state.snapshot_manifest_store.is_serving_ready().await {
        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Snapshots not ready").into_response();
    }

    let db_path = PathBuf::from(&state.db_path);

    if !db_path.exists() {
        return (axum::http::StatusCode::NOT_FOUND, "Database directory not found").into_response();
    }

    let mut files = Vec::new();

    if let Ok(mut entries) = tokio::fs::read_dir(&db_path).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.ends_with(".bin") {
                    files.push(file_name.to_string());
                }
            }
        }
    }

    files.sort();

    let response = SnapshotsResponse { files };
    axum::Json(response).into_response()
}

async fn handle_snapshot_manifest(State(state): State<Arc<AppState>>) -> Response {
    match state.snapshot_manifest_store.get_if_serving().await {
        Some(manifest) => axum::Json::<SignedSnapshotManifest>(manifest).into_response(),
        None => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Snapshot manifest not ready",
        )
            .into_response(),
    }
}

// Handler for files API
async fn handle_files(State(state): State<Arc<AppState>>, Path(filename): Path<String>) -> Response {
    info!(filename, "Served file request");
    let db_path = PathBuf::from(&state.db_path).join(&filename);

    if !db_path.exists() {
        return (axum::http::StatusCode::NOT_FOUND, "File not found").into_response();
    }

    // Read and return the file content
    match tokio::fs::read(&db_path).await {
        Ok(content) => {
            let response = axum::http::Response::builder()
                .status(200)
                .header("Content-Type", "application/octet-stream")
                .body(axum::body::Body::from(content))
                .unwrap();
            response.into_response()
        }
        Err(_) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response(),
    }
}

async fn handle_single_slot(State(state): State<Arc<AppState>>, Path(slot): Path<u64>) -> Response {
    let slots_path = PathBuf::from(&state.slots_path);

    match slots::load_slot(&slots_path, slot).await {
        Ok(Some(slot_data)) => axum::Json(slot_data).into_response(),
        Ok(None) => (axum::http::StatusCode::NOT_FOUND, "Slot info not found").into_response(),
        Err(e) => {
            infinisvm_logger::error!("Failed to read slot info: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to read slot info",
            )
                .into_response()
        }
    }
}

async fn handle_batch_slots(State(state): State<Arc<AppState>>, Query(query): Query<BatchSlotQuery>) -> Response {
    let slots_path = PathBuf::from(&state.slots_path);

    // Validate and extract slot range
    let min_slot = query.min_slot.unwrap_or(0);
    let max_slot = match query.max_slot {
        Some(max) => max,
        None => {
            return (axum::http::StatusCode::BAD_REQUEST, "max_slot parameter is required").into_response();
        }
    };

    // Validate slot range
    if min_slot > max_slot {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!("min_slot ({min_slot}) must be <= max_slot ({max_slot})"),
        )
            .into_response();
    }

    info!(min_slot = min_slot, max_slot = max_slot, "Served batch slots request");

    if max_slot - min_slot > slots::MAX_SLOT_RANGE as u64 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Slot range is too large: {min_slot} - {max_slot}"),
        )
            .into_response();
    }

    info!("Loading slots from {min_slot} to {max_slot}");

    // Load slots
    match slots::load_slots(&slots_path, min_slot, max_slot).await {
        Ok(slots_map) => axum::Json(BatchSlotsResponse { slots: slots_map }).into_response(),
        Err(e) => {
            infinisvm_logger::error!("Failed to load batch slots: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to load slots: {e}"),
            )
                .into_response()
        }
    }
}

async fn handle_slot_info(State(state): State<Arc<AppState>>, Path(slot): Path<u64>) -> Response {
    let slots_path = PathBuf::from(&state.slots_path);

    match slots::read_slot_file(&slots_path, slot, "info").await {
        Ok(Some(data)) => build_binary_response(data),
        Ok(None) => (axum::http::StatusCode::NOT_FOUND, "Slot info not found").into_response(),
        Err(e) => {
            infinisvm_logger::error!("Failed to read slot info: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to read slot info",
            )
                .into_response()
        }
    }
}

async fn handle_slot_shard(State(state): State<Arc<AppState>>, Path((slot, shard)): Path<(u64, u64)>) -> Response {
    let slots_path = PathBuf::from(&state.slots_path);
    let shard_name = shard.to_string();
    match slots::read_slot_file(&slots_path, slot, &shard_name).await {
        Ok(Some(data)) => build_binary_response(data),
        Ok(None) => (axum::http::StatusCode::NOT_FOUND, "Slot shard not found").into_response(),
        Err(e) => {
            infinisvm_logger::error!("Failed to read slot shard: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to read slot shard",
            )
                .into_response()
        }
    }
}

async fn handle_rpc_set(State(state): State<Arc<AppState>>) -> Response {
    let peers = state.rpc_registry.list().await;
    axum::Json(RpcSetResponse { peers }).into_response()
}

async fn handle_rpc_register(
    State(state): State<Arc<AppState>>,
    axum::Json(req): axum::Json<RpcRegisterRequest>,
) -> Response {
    let normalized = normalize_seed_addr(&req.grpc_addr);
    if normalized.is_empty() {
        return (StatusCode::BAD_REQUEST, "grpc_addr is required").into_response();
    }

    if !is_dialable_grpc_addr(&normalized) {
        return (
            StatusCode::BAD_REQUEST,
            "grpc_addr must be dialable (not 0.0.0.0 / ::); set --grpc-advertise-addr",
        )
            .into_response();
    }

    let node_id = hashv(&[normalized.as_bytes()]).to_bytes();

    if let Some(sequencer_pubkey) = state.sequencer_pubkey {
        let timeout_ms = std::env::var("RPC_REGISTRY_REGISTER_PROBE_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3000);
        let probe_timeout = Duration::from_millis(timeout_ms);
        match tokio::time::timeout(probe_timeout, probe_rpc_peer(&normalized, node_id, sequencer_pubkey)).await {
            Ok(Ok(())) => {}
            Ok(Err((status, message))) => {
                infinisvm_logger::warn!(grpc_addr = normalized.as_str(), "Rejected rpc registration: {message}");
                return (status, message).into_response();
            }
            Err(_) => {
                infinisvm_logger::warn!(
                    grpc_addr = normalized.as_str(),
                    timeout_ms,
                    "Rejected rpc registration: probe timed out"
                );
                return (StatusCode::GATEWAY_TIMEOUT, "rpc peer probe timed out").into_response();
            }
        }
    }

    let peer = state
        .rpc_registry
        .upsert_peer(node_id, normalized.clone(), req.score_hint)
        .await;
    info!(grpc_addr = normalized, "Registered rpc peer");
    axum::Json(peer).into_response()
}

async fn probe_rpc_peer(
    grpc_addr: &str,
    expected_node_id: [u8; 32],
    sequencer_pubkey: Pubkey,
) -> Result<(), (StatusCode, String)> {
    let addr = normalize_grpc_addr(grpc_addr);
    let retry_config = RetryConfig {
        max_retries: 0,
        enable_circuit_breaker: false,
        ..RetryConfig::default()
    };

    let mut client = SyncClient::connect_with_config(&addr, retry_config)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, format!("grpc connect failed: {e}")))?;
    let status = client
        .get_peer_status()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, format!("get_peer_status failed: {e}")))?;

    if status.node_id != expected_node_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "peer status node_id does not match advertised grpc_addr".to_string(),
        ));
    }

    let latest = status.latest_signed_finalization.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "peer not ready (no signed finalization yet)".to_string(),
        )
    })?;
    let slot = latest.finalization.slot;

    let signed = client.get_block_finalizer(slot).await.map_err(|e| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("get_block_finalizer({slot}) failed: {e}"),
        )
    })?;
    if signed.finalization.slot != slot {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "peer returned signed finalization for slot {} (expected {slot})",
                signed.finalization.slot
            ),
        ));
    }
    if !verify_signed_finalization(&signed, &sequencer_pubkey) {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid signed finalization signature".to_string(),
        ));
    }

    Ok(())
}

fn verify_signed_finalization(sf: &infinisvm_types::sync::SignedFinalization, sequencer_pubkey: &Pubkey) -> bool {
    if sf.sequencer_pubkey != sequencer_pubkey.to_bytes() {
        return false;
    }
    let msg = match bincode::serialize(&sf.finalization) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let sig = Signature::from(sf.signature);
    sig.verify(sequencer_pubkey.as_ref(), &msg)
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

fn build_binary_response(data: Bytes) -> Response {
    axum::http::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(data))
        .unwrap()
        .into_response()
}
