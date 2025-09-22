//! Mock gRPC server for testing retry functionality
//!
//! This module provides a configurable mock server that can simulate various
//! failure scenarios for testing the retry behavior of gRPC clients.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Server, Code, Request, Response, Status};

use infinisvm_types::sync::grpc::{
    infini_svm_service_server::{InfiniSvmService, InfiniSvmServiceServer},
    CommitBatchNotification, GetLatestSlotRequest, GetLatestSlotResponse, SlotDataResponse, StartReceivingSlotsRequest,
    TransactionBatchRequest,
};

/// Types of failures the mock server can simulate
#[derive(Debug, Clone, PartialEq)]
pub enum FailureMode {
    /// Connection refused errors
    ConnectionRefused,
    /// Request timeout errors
    Timeout,
    /// Server internal errors
    InternalError,
    /// Invalid argument errors (non-retryable)
    InvalidArgument,
    /// Permission denied errors (non-retryable)
    PermissionDenied,
    /// Intermittent failures (random)
    Intermittent,
    /// Stream disconnection
    StreamDisconnection,
    /// Slow responses
    SlowResponse,
}

/// Configuration for failure simulation
#[derive(Debug, Clone)]
pub struct FailureConfig {
    pub mode: FailureMode,
    pub failure_count: u32,
    pub failure_rate: f64, // 0.0 to 1.0
    pub delay: Duration,
}

/// Mock gRPC server for testing
pub struct MockGrpcServer {
    server_handle: Option<tokio::task::JoinHandle<()>>,
    address: String,
    port: u16,

    // Failure simulation
    failure_config: Arc<Mutex<Option<FailureConfig>>>,
    failure_count_remaining: Arc<AtomicU32>,

    // Metrics
    connection_attempts: Arc<AtomicU32>,
    request_count: Arc<AtomicU32>,
    reconnection_count: Arc<AtomicU32>,

    // Control
    shutdown_signal: Arc<AtomicBool>,
}

impl MockGrpcServer {
    /// Create a new mock server
    pub async fn new() -> Self {
        let port = find_available_port().await;
        let address = format!("127.0.0.1:{}", port);

        Self {
            server_handle: None,
            address: address.clone(),
            port,
            failure_config: Arc::new(Mutex::new(None)),
            failure_count_remaining: Arc::new(AtomicU32::new(0)),
            connection_attempts: Arc::new(AtomicU32::new(0)),
            request_count: Arc::new(AtomicU32::new(0)),
            reconnection_count: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the mock server
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr: SocketAddr = self.address.parse()?;

        let service = MockInfiniSvmService {
            failure_config: self.failure_config.clone(),
            failure_count_remaining: self.failure_count_remaining.clone(),
            connection_attempts: self.connection_attempts.clone(),
            request_count: self.request_count.clone(),
            reconnection_count: self.reconnection_count.clone(),
        };

        let server = Server::builder()
            .add_service(InfiniSvmServiceServer::new(service))
            .serve(addr);

        let shutdown_signal = self.shutdown_signal.clone();
        self.server_handle = Some(tokio::spawn(async move {
            let graceful = server.with_graceful_shutdown(async move {
                while !shutdown_signal.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });

            if let Err(e) = graceful.await {
                eprintln!("Mock server error: {}", e);
            }
        }));

        // Wait a bit for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    /// Stop the mock server
    pub async fn stop(&mut self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
        if let Some(handle) = self.server_handle.take() {
            let _ = handle.await;
        }
    }

    /// Get the server address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Set failure mode for a specific number of requests
    pub fn set_failure_mode(&mut self, mode: FailureMode, count: u32) {
        let config = FailureConfig {
            mode,
            failure_count: count,
            failure_rate: 1.0,
            delay: Duration::from_millis(0),
        };

        *self.failure_config.lock().unwrap() = Some(config);
        self.failure_count_remaining.store(count, Ordering::Relaxed);
    }

    /// Set failure rate for intermittent failures
    pub fn set_failure_rate(&mut self, rate: f64) {
        let config = FailureConfig {
            mode: FailureMode::Intermittent,
            failure_count: u32::MAX, // Unlimited
            failure_rate: rate.clamp(0.0, 1.0),
            delay: Duration::from_millis(0),
        };

        *self.failure_config.lock().unwrap() = Some(config);
        self.failure_count_remaining.store(u32::MAX, Ordering::Relaxed);
    }

    /// Set response delay
    pub fn set_response_delay(&mut self, delay: Duration) {
        if let Some(ref mut config) = *self.failure_config.lock().unwrap() {
            config.delay = delay;
        } else {
            let config = FailureConfig {
                mode: FailureMode::SlowResponse,
                failure_count: u32::MAX,
                failure_rate: 1.0,
                delay,
            };
            *self.failure_config.lock().unwrap() = Some(config);
        }
    }

    /// Simulate connection loss
    pub async fn simulate_connection_loss(&mut self) {
        self.reconnection_count.fetch_add(1, Ordering::Relaxed);
        // In a real implementation, this would close active connections
        // For now, just increment the reconnection counter
    }

    /// Clear failure mode
    pub fn clear_failure_mode(&mut self) {
        *self.failure_config.lock().unwrap() = None;
        self.failure_count_remaining.store(0, Ordering::Relaxed);
    }

    /// Get connection attempt count
    pub fn connection_attempts(&self) -> u32 {
        self.connection_attempts.load(Ordering::Relaxed)
    }

    /// Get request count
    pub fn request_count(&self) -> u32 {
        self.request_count.load(Ordering::Relaxed)
    }

    /// Get reconnection count
    pub fn reconnection_count(&self) -> u32 {
        self.reconnection_count.load(Ordering::Relaxed)
    }

    /// Reset all metrics
    pub fn reset_metrics(&self) {
        self.connection_attempts.store(0, Ordering::Relaxed);
        self.request_count.store(0, Ordering::Relaxed);
        self.reconnection_count.store(0, Ordering::Relaxed);
    }
}

impl Drop for MockGrpcServer {
    fn drop(&mut self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }
}

/// Mock implementation of the InfiniSvmService
#[derive(Clone)]
struct MockInfiniSvmService {
    failure_config: Arc<Mutex<Option<FailureConfig>>>,
    failure_count_remaining: Arc<AtomicU32>,
    connection_attempts: Arc<AtomicU32>,
    request_count: Arc<AtomicU32>,
    reconnection_count: Arc<AtomicU32>,
}

impl MockInfiniSvmService {
    /// Check if request should fail based on configuration
    fn should_fail(&self) -> Option<Status> {
        self.request_count.fetch_add(1, Ordering::Relaxed);

        let config_guard = self.failure_config.lock().unwrap();
        let config = match config_guard.as_ref() {
            Some(config) => config.clone(),
            None => return None,
        };
        drop(config_guard);

        match config.mode {
            FailureMode::Intermittent => {
                if rand::random::<f64>() < config.failure_rate {
                    Some(Status::unavailable("Intermittent failure"))
                } else {
                    None
                }
            }
            _ => {
                let remaining = self.failure_count_remaining.load(Ordering::Relaxed);
                if remaining > 0 {
                    self.failure_count_remaining.fetch_sub(1, Ordering::Relaxed);
                    Some(self.status_for_failure_mode(&config.mode))
                } else {
                    None
                }
            }
        }
    }

    fn status_for_failure_mode(&self, mode: &FailureMode) -> Status {
        match mode {
            FailureMode::ConnectionRefused => Status::unavailable("Connection refused"),
            FailureMode::Timeout => Status::deadline_exceeded("Request timeout"),
            FailureMode::InternalError => Status::internal("Internal server error"),
            FailureMode::InvalidArgument => Status::invalid_argument("Invalid request"),
            FailureMode::PermissionDenied => Status::permission_denied("Access denied"),
            FailureMode::Intermittent => Status::unavailable("Service temporarily unavailable"),
            FailureMode::StreamDisconnection => Status::aborted("Stream disconnected"),
            FailureMode::SlowResponse => Status::deadline_exceeded("Slow response"),
        }
    }

    async fn apply_delay(&self) {
        let config_guard = self.failure_config.lock().unwrap();
        if let Some(config) = config_guard.as_ref() {
            if !config.delay.is_zero() {
                let delay = config.delay;
                drop(config_guard);
                sleep(delay).await;
            }
        }
    }
}

#[tonic::async_trait]
impl InfiniSvmService for MockInfiniSvmService {
    async fn get_latest_slot(
        &self,
        _request: Request<GetLatestSlotRequest>,
    ) -> Result<Response<GetLatestSlotResponse>, Status> {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);

        // Apply response delay if configured
        self.apply_delay().await;

        // Check if request should fail
        if let Some(status) = self.should_fail() {
            return Err(status);
        }

        let response = GetLatestSlotResponse { slot: 12345 };

        Ok(Response::new(response))
    }

    type StartReceivingSlotsStream = Pin<Box<dyn Stream<Item = Result<SlotDataResponse, Status>> + Send>>;

    async fn start_receiving_slots(
        &self,
        _request: Request<StartReceivingSlotsRequest>,
    ) -> Result<Response<Self::StartReceivingSlotsStream>, Status> {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);

        // Check if request should fail
        if let Some(status) = self.should_fail() {
            return Err(status);
        }

        let failure_config = self.failure_config.clone();
        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let mut slot_counter = 0u64;

            loop {
                // Check for stream disconnection
                {
                    let config_guard = failure_config.lock().unwrap();
                    if let Some(config) = config_guard.as_ref() {
                        if config.mode == FailureMode::StreamDisconnection {
                            let _ = tx.send(Err(Status::aborted("Stream disconnected"))).await;
                            break;
                        }
                    }
                }

                let slot_data = SlotDataResponse {
                    slot: slot_counter,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    blockhash: vec![0u8; 32], // Mock blockhash
                };

                if tx.send(Ok(slot_data)).await.is_err() {
                    break; // Receiver dropped
                }

                slot_counter += 1;
                sleep(Duration::from_millis(100)).await; // 10 slots per second
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type SubscribeTransactionBatchesStream =
        Pin<Box<dyn Stream<Item = Result<CommitBatchNotification, Status>> + Send>>;

    async fn subscribe_transaction_batches(
        &self,
        _request: Request<TransactionBatchRequest>,
    ) -> Result<Response<Self::SubscribeTransactionBatchesStream>, Status> {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);

        // Check if request should fail
        if let Some(status) = self.should_fail() {
            return Err(status);
        }

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let mut batch_counter = 0u64;

            loop {
                let batch_data = CommitBatchNotification {
                    slot: batch_counter,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    batch_size: 100,
                    compression_ratio: 80,
                    compressed_transactions: vec![0u8; 1000], // Mock transaction data
                    job_id: batch_counter,
                };

                if tx.send(Ok(batch_data)).await.is_err() {
                    break; // Receiver dropped
                }

                batch_counter += 1;
                sleep(Duration::from_millis(200)).await; // 5 batches per second
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Find an available port for the mock server
async fn find_available_port() -> u16 {
    for port in 50000..60000 {
        if tokio::net::TcpListener::bind(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
    }
    50000 // Fallback
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_server_startup() {
        let mut server = MockGrpcServer::new().await;
        assert!(server.start().await.is_ok());
        assert!(server.address().contains("127.0.0.1"));
        server.stop().await;
    }

    #[tokio::test]
    async fn test_failure_mode_configuration() {
        let mut server = MockGrpcServer::new().await;

        // Test setting failure mode
        server.set_failure_mode(FailureMode::ConnectionRefused, 3);
        assert_eq!(server.failure_count_remaining.load(Ordering::Relaxed), 3);

        // Test clearing failure mode
        server.clear_failure_mode();
        assert_eq!(server.failure_count_remaining.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_metrics_tracking() {
        let server = MockGrpcServer::new().await;

        // Test initial state
        assert_eq!(server.connection_attempts(), 0);
        assert_eq!(server.request_count(), 0);
        assert_eq!(server.reconnection_count(), 0);

        // Test reset
        server.reset_metrics();
        assert_eq!(server.connection_attempts(), 0);
    }
}
