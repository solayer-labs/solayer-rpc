use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use infinisvm_logger::{debug, error, info, warn};
use infinisvm_types::sync::{
    CommitBatchNotification, GetPeerStatusRequest, PeerStatus, ShredId, SignedFinalization, SyncBatchShred,
};
use metrics::counter;
use tokio::{
    sync::{mpsc, RwLock},
    time::sleep,
};
use tokio_stream::StreamExt;
use tonic::{Code, Request, Status};

use crate::grpc::service::{
    GetBatchShredRequest, GetBlockFinalizerRequest, InfiniSvmServiceClient, InjectCommitBatchRequest,
    InjectCommitBatchResponse, SubscribeTransactionBatchRequest,
};

/// Retry configuration for gRPC operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial backoff delay in milliseconds
    pub initial_backoff_ms: u64,
    /// Maximum backoff delay in milliseconds
    pub max_backoff_ms: u64,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Jitter factor (0.0 to 1.0) to add randomness to backoff
    pub jitter_factor: f64,
    /// Whether to enable circuit breaker functionality
    pub enable_circuit_breaker: bool,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker timeout in seconds
    pub circuit_breaker_timeout_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_secs: 30,
        }
    }
}

/// Circuit breaker state
#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for managing connection failures
#[derive(Debug)]
struct CircuitBreaker {
    state: RwLock<CircuitState>,
    failure_count: AtomicU32,
    last_failure_time: AtomicU64,
    threshold: u32,
    timeout_duration: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, timeout_duration: Duration) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU32::new(0),
            last_failure_time: AtomicU64::new(0),
            threshold,
            timeout_duration,
        }
    }

    async fn can_execute(&self) -> bool {
        let state = self.state.read().await;
        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let last_failure = self.last_failure_time.load(Ordering::Relaxed);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now - last_failure >= self.timeout_duration.as_secs() {
                    drop(state);
                    let mut state = self.state.write().await;
                    *state = CircuitState::HalfOpen;
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    async fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        let mut state = self.state.write().await;
        *state = CircuitState::Closed;
    }

    async fn record_failure(&self) {
        let current_failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_failure_time.store(now, Ordering::Relaxed);

        if current_failures >= self.threshold {
            let mut state = self.state.write().await;
            *state = CircuitState::Open;
            warn!("Circuit breaker opened after {} failures", current_failures);
        }
    }
}

/// Enhanced gRPC client with retry and circuit breaker functionality
pub struct SyncClient {
    client: InfiniSvmServiceClient,
    retry_config: RetryConfig,
    circuit_breaker: Arc<CircuitBreaker>,
    connection_url: String,
}

impl SyncClient {
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Create a new SyncClient with default retry configuration
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::connect_with_config(addr, RetryConfig::default()).await
    }

    /// Create a new SyncClient with custom retry configuration
    pub async fn connect_with_config(
        addr: &str,
        retry_config: RetryConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            retry_config.circuit_breaker_threshold,
            Duration::from_secs(retry_config.circuit_breaker_timeout_secs),
        ));

        let client = Self::connect_with_retry(addr, &retry_config, &circuit_breaker).await?;

        Ok(Self {
            client,
            retry_config,
            circuit_breaker,
            connection_url: addr.to_string(),
        })
    }

    /// Internal method to establish connection with retry logic
    async fn connect_with_retry(
        addr: &str,
        retry_config: &RetryConfig,
        circuit_breaker: &CircuitBreaker,
    ) -> Result<InfiniSvmServiceClient, Box<dyn std::error::Error + Send + Sync>> {
        let mut attempt = 0;
        let mut last_error: Option<String> = None;

        while attempt <= retry_config.max_retries {
            // Check circuit breaker
            if retry_config.enable_circuit_breaker && !circuit_breaker.can_execute().await {
                return Err("Circuit breaker is open".into());
            }

            match Self::connect_once(addr).await {
                Ok(client) => {
                    let client = client.max_decoding_message_size(1024 * 1024 * 1024); // 1GB
                    info!("Successfully connected to gRPC server at {}", addr);
                    counter!("grpc_client_connects_total").increment(1);
                    if retry_config.enable_circuit_breaker {
                        circuit_breaker.record_success().await;
                    }
                    return Ok(client);
                }
                Err(e) => {
                    attempt += 1;
                    // Log detailed transport error with debug formatting and source chain
                    let mut msg = format!("{e:?}");
                    let mut src = e.source();
                    let mut i = 0;
                    while let Some(s) = src {
                        i += 1;
                        msg.push_str(&format!("; source[{i}]: {s}"));
                        src = s.source();
                    }
                    last_error = Some(msg.clone());
                    counter!("grpc_client_connect_errors_total").increment(1);

                    if retry_config.enable_circuit_breaker {
                        circuit_breaker.record_failure().await;
                    }

                    if attempt <= retry_config.max_retries {
                        let backoff_ms = Self::calculate_backoff(
                            attempt,
                            retry_config.initial_backoff_ms,
                            retry_config.max_backoff_ms,
                            retry_config.backoff_multiplier,
                            retry_config.jitter_factor,
                        );

                        warn!(
                            "gRPC connection attempt {} failed: {}, retrying in {}ms",
                            attempt, msg, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        let error_msg = last_error.unwrap_or_else(|| "Failed to connect after all retries".to_string());
        Err(error_msg.into())
    }

    async fn connect_once(addr: &str) -> Result<InfiniSvmServiceClient, Box<dyn std::error::Error + Send + Sync>> {
        let uri: http::Uri = addr.parse()?;
        let base_uri = format!(
            "{}://{}:{}",
            uri.scheme_str().unwrap_or("http"),
            uri.host().unwrap_or("localhost"),
            uri.port_u16().unwrap_or(5005)
        );
        Ok(InfiniSvmServiceClient::new(base_uri))
    }

    /// Calculate exponential backoff with jitter
    fn calculate_backoff(
        attempt: u32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
        multiplier: f64,
        jitter_factor: f64,
    ) -> u64 {
        let exponential_backoff = (initial_backoff_ms as f64 * multiplier.powi(attempt as i32 - 1)) as u64;
        let backoff_with_cap = exponential_backoff.min(max_backoff_ms);

        // Add jitter to avoid thundering herd
        let jitter = (backoff_with_cap as f64 * jitter_factor * rand::random::<f64>()) as u64;
        backoff_with_cap + jitter
    }

    /// Check if an error is retryable
    fn is_retryable_error(status: &Status) -> bool {
        match status.code() {
            Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => true,
            Code::Internal => {
                // Some internal errors might be transient
                let message = status.message().to_lowercase();
                message.contains("connection") || message.contains("timeout") || message.contains("network")
            }
            _ => false,
        }
    }

    /// Execute a gRPC operation with retry logic
    async fn execute_with_retry<T>(
        &mut self,
        operation_name: &str,
        operation: impl Fn(&mut InfiniSvmServiceClient) -> futures_util::future::BoxFuture<'_, Result<T, Status>>,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
        let mut attempt = 0;
        let mut last_error: Option<String> = None;

        while attempt <= self.retry_config.max_retries {
            // Check circuit breaker
            if self.retry_config.enable_circuit_breaker && !self.circuit_breaker.can_execute().await {
                return Err("Circuit breaker is open".into());
            }

            match operation(&mut self.client).await {
                Ok(result) => {
                    if attempt > 0 {
                        info!("Operation '{}' succeeded after {} retries", operation_name, attempt);
                    }
                    if self.retry_config.enable_circuit_breaker {
                        self.circuit_breaker.record_success().await;
                    }
                    return Ok(result);
                }
                Err(status) => {
                    attempt += 1;
                    last_error = Some(status.to_string());

                    if !Self::is_retryable_error(&status) {
                        debug!("Non-retryable error for operation '{}': {}", operation_name, status);
                        return Err(status.to_string().into());
                    }

                    if self.retry_config.enable_circuit_breaker {
                        self.circuit_breaker.record_failure().await;
                    }

                    if attempt <= self.retry_config.max_retries {
                        let backoff_ms = Self::calculate_backoff(
                            attempt,
                            self.retry_config.initial_backoff_ms,
                            self.retry_config.max_backoff_ms,
                            self.retry_config.backoff_multiplier,
                            self.retry_config.jitter_factor,
                        );

                        warn!(
                            "Operation '{}' attempt {} failed ({}), retrying in {}ms",
                            operation_name, attempt, status, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        let error_msg = last_error.unwrap_or_else(|| format!("Operation '{operation_name}' failed after all retries"));
        Err(error_msg.into())
    }

    // Single RPC without the generic retry wrapper; returns tonic::Status for
    // precise error handling
    pub async fn get_batch_shred(
        &mut self,
        shred_id: ShredId,
    ) -> Result<SyncBatchShred, Box<dyn std::error::Error + Send + Sync>> {
        let request = Request::new(GetBatchShredRequest { shred_id });
        let result = self.client.get_batch_shred(request).await?;
        Ok(result.into_inner())
    }

    pub async fn get_block_finalizer(
        &mut self,
        slot: u64,
    ) -> Result<SignedFinalization, Box<dyn std::error::Error + Send + Sync>> {
        let request = Request::new(GetBlockFinalizerRequest { slot });
        let result = self.client.get_block_finalizer(request).await?;
        Ok(result.into_inner())
    }

    pub async fn get_peer_status(&mut self) -> Result<PeerStatus, Box<dyn std::error::Error + Send + Sync>> {
        let request = Request::new(GetPeerStatusRequest {});
        let result = self.client.get_peer_status(request).await?;
        Ok(result.into_inner().status)
    }

    pub async fn inject_commit_batch_notification(
        &mut self,
        request: InjectCommitBatchRequest,
    ) -> Result<InjectCommitBatchResponse, Box<dyn std::error::Error + Send + Sync>> {
        let result = self
            .client
            .inject_commit_batch_notification(Request::new(request))
            .await?;
        Ok(result.into_inner())
    }

    pub async fn subscribe_commit_batch_notifications(
        &mut self,
    ) -> Result<mpsc::Receiver<CommitBatchNotification>, Box<dyn std::error::Error + Send + Sync>> {
        let stream = self
            .execute_with_retry("subscribe_transactions", |client| {
                Box::pin(async move {
                    let request = Request::new(SubscribeTransactionBatchRequest {});
                    client.subscribe_transaction_batches(request).await
                })
            })
            .await?
            .into_inner();

        let (tx, rx) = mpsc::channel(128);
        let retry_config = self.retry_config.clone();
        let circuit_breaker = Arc::clone(&self.circuit_breaker);
        let connection_url = self.connection_url.clone();
        let retry_config_for_task = retry_config.clone();
        let circuit_breaker_for_task = Arc::clone(&circuit_breaker);

        // Function to resubscribe the transactions stream on failure
        let make_new_stream = move || {
            let connection_url = connection_url.clone();
            async move {
                match SyncClient::connect_once(&connection_url).await {
                    Ok(client) => {
                        let mut client = client.max_decoding_message_size(1024 * 1024 * 1024);
                        let request = Request::new(SubscribeTransactionBatchRequest {});
                        match client.subscribe_transaction_batches(request).await {
                            Ok(resp) => Ok(resp.into_inner()),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
        };

        tokio::spawn(async move {
            Self::handle_stream(
                stream,
                tx,
                "transaction_batch",
                retry_config_for_task,
                circuit_breaker_for_task,
                make_new_stream,
            )
            .await;
        });

        Ok(rx)
    }

    /// Handle streaming operations with automatic reconnection on failure
    async fn handle_stream<T, S, F, Fut>(
        mut stream: S,
        tx: mpsc::Sender<T>,
        stream_name: &str,
        retry_config: RetryConfig,
        circuit_breaker: Arc<CircuitBreaker>,
        mut resubscribe: F,
    ) where
        T: Send + 'static,
        S: StreamExt<Item = Result<T, Status>> + Send + Unpin + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<S, String>> + Send,
    {
        let mut consecutive_failures: u32 = 0;

        'outer: loop {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(data) => {
                        // reset failures on successful receive
                        if consecutive_failures != 0 {
                            consecutive_failures = 0;
                        }
                        if let Err(e) = tx.send(data).await {
                            error!("Error sending {}: {}", stream_name, e);
                            break 'outer;
                        }
                    }
                    Err(status) => {
                        // Record metrics and log error
                        counter!("grpc_stream_errors_total").increment(1);
                        error!("Error receiving {}: {}", stream_name, status);

                        // Non-retryable errors: exit
                        if !Self::is_retryable_error(&status) {
                            warn!("{} stream error is non-retryable. Stopping.", stream_name);
                            break 'outer;
                        }

                        // Retryable: attempt to reconnect with backoff
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if retry_config.enable_circuit_breaker {
                            circuit_breaker.record_failure().await;
                        }

                        let backoff_ms = Self::calculate_backoff(
                            consecutive_failures,
                            retry_config.initial_backoff_ms,
                            retry_config.max_backoff_ms,
                            retry_config.backoff_multiplier,
                            retry_config.jitter_factor,
                        );

                        warn!(
                            "{} stream failed (attempt {}), reconnecting in {}ms",
                            stream_name, consecutive_failures, backoff_ms
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;

                        // Keep trying to resubscribe until success
                        loop {
                            match resubscribe().await {
                                Ok(new_stream) => {
                                    counter!("grpc_stream_reconnects_total").increment(1);
                                    info!("Reconnected {} stream", stream_name);
                                    if retry_config.enable_circuit_breaker {
                                        circuit_breaker.record_success().await;
                                    }
                                    stream = new_stream;
                                    continue 'outer;
                                }
                                Err(err) => {
                                    counter!("grpc_stream_reconnect_errors_total").increment(1);
                                    warn!("Failed to resubscribe {}: {}. Retrying...", stream_name, err);
                                    let backoff_ms = Self::calculate_backoff(
                                        consecutive_failures,
                                        retry_config.initial_backoff_ms,
                                        retry_config.max_backoff_ms,
                                        retry_config.backoff_multiplier,
                                        retry_config.jitter_factor,
                                    );
                                    sleep(Duration::from_millis(backoff_ms)).await;
                                    consecutive_failures = consecutive_failures.saturating_add(1);
                                }
                            }
                        }
                    }
                }
            }

            // Stream ended gracefully (None). Attempt to resubscribe as well.
            warn!("Stream {} ended. Attempting to resubscribe...", stream_name);
            consecutive_failures = consecutive_failures.saturating_add(1);
            let backoff_ms = Self::calculate_backoff(
                consecutive_failures,
                retry_config.initial_backoff_ms,
                retry_config.max_backoff_ms,
                retry_config.backoff_multiplier,
                retry_config.jitter_factor,
            );
            sleep(Duration::from_millis(backoff_ms)).await;
            loop {
                match resubscribe().await {
                    Ok(new_stream) => {
                        counter!("grpc_stream_reconnects_total").increment(1);
                        info!("Reconnected {} stream", stream_name);
                        if retry_config.enable_circuit_breaker {
                            circuit_breaker.record_success().await;
                        }
                        stream = new_stream;
                        break; // back to outer loop to read
                    }
                    Err(err) => {
                        counter!("grpc_stream_reconnect_errors_total").increment(1);
                        warn!("Failed to resubscribe {}: {}. Retrying...", stream_name, err);
                        let backoff_ms = Self::calculate_backoff(
                            consecutive_failures,
                            retry_config.initial_backoff_ms,
                            retry_config.max_backoff_ms,
                            retry_config.backoff_multiplier,
                            retry_config.jitter_factor,
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                        consecutive_failures = consecutive_failures.saturating_add(1);
                    }
                }
            }
        }
        info!("Stream {} handler exiting", stream_name);
    }

    /// Get current retry configuration
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Update retry configuration
    pub fn set_retry_config(&mut self, config: RetryConfig) {
        self.retry_config = config;
    }

    /// Get circuit breaker status
    pub async fn circuit_breaker_status(&self) -> (String, u32) {
        let state = self.circuit_breaker.state.read().await;
        let failure_count = self.circuit_breaker.failure_count.load(Ordering::Relaxed);
        let state_str = match *state {
            CircuitState::Closed => "Closed",
            CircuitState::Open => "Open",
            CircuitState::HalfOpen => "HalfOpen",
        };
        (state_str.to_string(), failure_count)
    }

    /// Force circuit breaker reset (for testing/debugging)
    pub async fn reset_circuit_breaker(&self) {
        self.circuit_breaker.failure_count.store(0, Ordering::Relaxed);
        let mut state = self.circuit_breaker.state.write().await;
        *state = CircuitState::Closed;
        info!("Circuit breaker manually reset");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Test default retry configuration
    #[test]
    fn test_default_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 100);
        assert_eq!(config.max_backoff_ms, 5000);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert_eq!(config.jitter_factor, 0.1);
        assert!(config.enable_circuit_breaker);
        assert_eq!(config.circuit_breaker_threshold, 5);
        assert_eq!(config.circuit_breaker_timeout_secs, 30);
    }

    /// Test exponential backoff calculation
    #[test]
    fn test_calculate_backoff() {
        let initial_backoff = 100;
        let max_backoff = 5000;
        let multiplier = 2.0;
        let jitter_factor = 0.0; // No jitter for predictable testing

        // Test first attempt
        let backoff1 = SyncClient::calculate_backoff(1, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff1, 100);

        // Test second attempt
        let backoff2 = SyncClient::calculate_backoff(2, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff2, 200);

        // Test third attempt
        let backoff3 = SyncClient::calculate_backoff(3, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff3, 400);

        // Test backoff cap
        let backoff_large = SyncClient::calculate_backoff(10, initial_backoff, max_backoff, multiplier, jitter_factor);
        assert_eq!(backoff_large, max_backoff);
    }

    /// Test retryable error detection
    #[test]
    fn test_is_retryable_error() {
        use tonic::{Code, Status};

        // Retryable errors
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Unavailable,
            "Service unavailable"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::DeadlineExceeded,
            "Timeout"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::ResourceExhausted,
            "Rate limited"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Aborted,
            "Request aborted"
        )));

        // Retryable internal errors
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Internal,
            "Connection error occurred"
        )));
        assert!(SyncClient::is_retryable_error(&Status::new(
            Code::Internal,
            "Network timeout detected"
        )));

        // Non-retryable errors
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::InvalidArgument,
            "Bad request"
        )));
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::NotFound,
            "Resource not found"
        )));
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::PermissionDenied,
            "Access denied"
        )));
        assert!(!SyncClient::is_retryable_error(&Status::new(
            Code::Internal,
            "Database corruption"
        )));
    }

    /// Test circuit breaker functionality
    #[tokio::test]
    async fn test_circuit_breaker() {
        let threshold = 3;
        let timeout_duration = Duration::from_secs(1);
        let circuit_breaker = CircuitBreaker::new(threshold, timeout_duration);

        // Initially should be closed
        assert!(circuit_breaker.can_execute().await);

        // Record failures
        for _ in 0..threshold {
            circuit_breaker.record_failure().await;
        }

        // Should be open now
        assert!(!circuit_breaker.can_execute().await);

        // Wait for timeout
        tokio::time::sleep(timeout_duration + Duration::from_millis(200)).await;

        // Should be half-open now
        assert!(circuit_breaker.can_execute().await);

        // Record success to close circuit
        circuit_breaker.record_success().await;
        assert!(circuit_breaker.can_execute().await);
    }
}
