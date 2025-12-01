use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use infinisvm_logger::info;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tower_http::compression::CompressionLayer;

use crate::{
    slots::{self, SlotData},
    state::SyncState,
};

#[derive(Clone)]
pub struct AppState {
    pub db_path: String,
    pub slots_path: String,
    pub sync_state: Arc<RwLock<SyncState>>,
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
    sync_state: Arc<RwLock<SyncState>>,
) -> eyre::Result<()> {
    let app_state = Arc::new(AppState {
        db_path,
        slots_path,
        sync_state,
    });

    let app = Router::new()
        .route("/solayer/snapshots", get(handle_snapshots))
        .route("/solayer/files/{filename}", get(handle_files))
        .route("/solayer/slots/{slot}", get(handle_single_slot))
        .route("/solayer/slots", get(handle_batch_slots))
        .route("/solayer/slots/{slot}/info", get(handle_slot_info))
        .route("/solayer/slots/{slot}/shards/{shard}", get(handle_slot_shard))
        .layer(CompressionLayer::new())
        .with_state(app_state);

    info!("InfiniSVM HTTP Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// Handler for snapshots API
async fn handle_snapshots(State(state): State<Arc<AppState>>) -> Response {
    let db_path = PathBuf::from(&state.db_path);

    if !db_path.exists() {
        return (axum::http::StatusCode::NOT_FOUND, "Database directory not found").into_response();
    }

    let mut files = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&db_path) {
        for entry in entries.filter_map(Result::ok) {
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

fn build_binary_response(data: Vec<u8>) -> Response {
    axum::http::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(data))
        .unwrap()
        .into_response()
}
