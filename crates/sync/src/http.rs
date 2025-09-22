use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use infinisvm_logger::info;
use serde::Serialize;
use tokio::sync::RwLock;
use tower_http::compression::CompressionLayer;

use crate::state::SyncState;

#[derive(Clone)]
pub struct AppState {
    pub db_path: String,
    pub sync_state: Arc<RwLock<SyncState>>,
}

#[derive(Serialize)]
pub struct SnapshotsResponse {
    pub files: Vec<String>,
}

// Standalone HTTP server function
pub async fn start_http_server(
    addr: SocketAddr,
    db_path: String,
    sync_state: Arc<RwLock<SyncState>>,
) -> eyre::Result<()> {
    let app_state = Arc::new(AppState { db_path, sync_state });

    let app = Router::new()
        .route("/solayer/snapshots", get(handle_snapshots))
        .route("/solayer/files/{filename}", get(handle_files))
        .layer(CompressionLayer::new())
        .with_state(app_state);

    info!("InfiniSVM HTTP Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// Handler for snapshots API
async fn handle_snapshots(State(state): State<Arc<AppState>>) -> Response {
    info!("Served snapshots request");
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
