//! Comprehensive test suite for gRPC client retry functionality
//!
//! This module contains tests for:
//! - Retry configuration validation
//! - Exponential backoff algorithm
//! - Connection retry scenarios  
//! - Stream reconnection behavior
//! - Circuit breaker functionality
//! - Performance under retry conditions

use std::sync::{Arc, AtomicBool, AtomicU32, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tonic::{transport::Channel, Code, Status};

use super::client::SyncClient;
use crate::grpc::mock_server::MockGrpcServer;

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

/// Enhanced SyncClient with retry capabilities
pub struct RetrySyncClient {
    base_client: SyncClient,
    config: RetryConfig,
    circuit_breaker: Arc<CircuitBreaker>,
}

/// Circuit breaker implementation for gRPC client
#[derive(Debug)]
pub struct CircuitBreaker {
    failure_count: AtomicU32,
    last_failure_time: Mutex<Option<Instant>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    state: Mutex<CircuitBreakerState>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            failure_count: AtomicU32::new(0),
            last_failure_time: Mutex::new(None),
            failure_threshold,
            recovery_timeout,
            state: Mutex::new(CircuitBreakerState::Closed),
        }
    }

    pub fn can_execute(&self) -> bool {
        let state = *self.state.lock().unwrap();
        match state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                if let Some(last_failure) = *self.last_failure_time.lock().unwrap() {
                    if last_failure.elapsed() >= self.recovery_timeout {
                        *self.state.lock().unwrap() = CircuitBreakerState::HalfOpen;
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            }
            CircuitBreakerState::HalfOpen => true,
        }
    }

    pub fn on_success(&self) {
        self.failure_count.store(0, std::sync::atomic::Ordering::Relaxed);
        *self.state.lock().unwrap() = CircuitBreakerState::Closed;
    }

    pub fn on_failure(&self) {
        let failures = self.failure_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        *self.last_failure_time.lock().unwrap() = Some(Instant::now());

        if failures >= self.failure_threshold {
            *self.state.lock().unwrap() = CircuitBreakerState::Open;
        }
    }

    pub fn get_state(&self) -> CircuitBreakerState {
        *self.state.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::test;

    /// Test retry configuration validation
    #[test]
    fn test_retry_config_validation() {
        // Valid configuration
        let valid_config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 1.5,
            jitter: true,
        };
        assert!(validate_retry_config(&valid_config).is_ok());

        // Invalid configurations
        let invalid_configs = vec![
            RetryConfig {
                max_retries: 0,
                ..valid_config.clone()
            },
            RetryConfig {
                initial_delay: Duration::from_secs(0),
                ..valid_config.clone()
            },
            RetryConfig {
                max_delay: Duration::from_millis(50),
                initial_delay: Duration::from_millis(100),
                ..valid_config.clone()
            },
            RetryConfig {
                backoff_multiplier: 0.5,
                ..valid_config.clone()
            },
        ];

        for config in invalid_configs {
            assert!(validate_retry_config(&config).is_err());
        }
    }

    /// Test exponential backoff algorithm accuracy
    #[test]
    fn test_exponential_backoff_algorithm() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };

        let delays = calculate_backoff_delays(&config, 5);

        // Expected delays: 100ms, 200ms, 400ms, 800ms, 1600ms
        assert_eq!(delays[0], Duration::from_millis(100));
        assert_eq!(delays[1], Duration::from_millis(200));
        assert_eq!(delays[2], Duration::from_millis(400));
        assert_eq!(delays[3], Duration::from_millis(800));
        assert_eq!(delays[4], Duration::from_millis(1600));
    }

    /// Test exponential backoff with jitter
    #[test]
    fn test_exponential_backoff_with_jitter() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter: true,
            ..Default::default()
        };

        let delays1 = calculate_backoff_delays(&config, 5);
        let delays2 = calculate_backoff_delays(&config, 5);

        // With jitter, delays should be different but within bounds
        for i in 0..5 {
            assert!(delays1[i] != delays2[i] || i == 0); // First delay might be the same
            assert!(delays1[i] <= config.max_delay);
            assert!(delays2[i] <= config.max_delay);
        }
    }

    /// Test maximum delay capping
    #[test]
    fn test_max_delay_capping() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            backoff_multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };

        let delays = calculate_backoff_delays(&config, 10);

        // After a few retries, all delays should be capped at max_delay
        for delay in delays.iter().skip(3) {
            assert_eq!(*delay, config.max_delay);
        }
    }

    /// Test circuit breaker state transitions
    #[test]
    fn test_circuit_breaker_states() {
        let circuit_breaker = CircuitBreaker::new(3, Duration::from_millis(100));

        // Initially closed
        assert_eq!(circuit_breaker.get_state(), CircuitBreakerState::Closed);
        assert!(circuit_breaker.can_execute());

        // Record failures
        for _ in 0..2 {
            circuit_breaker.on_failure();
            assert_eq!(circuit_breaker.get_state(), CircuitBreakerState::Closed);
        }

        // Third failure should open the circuit
        circuit_breaker.on_failure();
        assert_eq!(circuit_breaker.get_state(), CircuitBreakerState::Open);
        assert!(!circuit_breaker.can_execute());
    }

    /// Test circuit breaker recovery
    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let circuit_breaker = CircuitBreaker::new(2, Duration::from_millis(50));

        // Trigger circuit breaker
        circuit_breaker.on_failure();
        circuit_breaker.on_failure();
        assert_eq!(circuit_breaker.get_state(), CircuitBreakerState::Open);

        // Wait for recovery timeout
        sleep(Duration::from_millis(60)).await;

        // Should transition to half-open
        assert!(circuit_breaker.can_execute());
        assert_eq!(circuit_breaker.get_state(), CircuitBreakerState::HalfOpen);

        // Success should close the circuit
        circuit_breaker.on_success();
        assert_eq!(circuit_breaker.get_state(), CircuitBreakerState::Closed);
    }

    /// Test connection retry scenarios with network failures
    #[tokio::test]
    async fn test_connection_retry_with_network_failures() {
        let mut mock_server = MockGrpcServer::new().await;
        mock_server.set_failure_mode(FailureMode::ConnectionRefused, 3);

        let config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let start_time = Instant::now();
        let result = connect_with_retry(&mock_server.address(), &config).await;
        let elapsed = start_time.elapsed();

        // Should succeed after retries
        assert!(result.is_ok());
        // Should have taken time for retries
        assert!(elapsed >= Duration::from_millis(30)); // At least 3 retries with delays
        assert_eq!(mock_server.connection_attempts(), 4); // Initial + 3 retries
    }

    /// Test timeout scenarios
    #[tokio::test]
    async fn test_timeout_scenarios() {
        let mut mock_server = MockGrpcServer::new().await;
        mock_server.set_failure_mode(FailureMode::Timeout, 2);

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let result = connect_with_retry(&mock_server.address(), &config).await;
        assert!(result.is_ok());
        assert_eq!(mock_server.connection_attempts(), 3);
    }

    /// Test server errors that should not be retried
    #[tokio::test]
    async fn test_non_retryable_errors() {
        let mut mock_server = MockGrpcServer::new().await;
        mock_server.set_failure_mode(FailureMode::InvalidArgument, 1);

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let result = connect_with_retry(&mock_server.address(), &config).await;
        assert!(result.is_err());
        assert_eq!(mock_server.connection_attempts(), 1); // Should not retry
    }

    /// Test stream reconnection behavior
    #[tokio::test]
    async fn test_stream_reconnection() {
        let mut mock_server = MockGrpcServer::new().await;
        let client = RetrySyncClient::new(&mock_server.address(), RetryConfig::default())
            .await
            .unwrap();

        // Start streaming
        let mut receiver = client.subscribe_slots_with_retry().await.unwrap();

        // Receive some messages
        for i in 0..5 {
            let slot = receiver.recv().await.unwrap();
            assert_eq!(slot.slot, i);
        }

        // Simulate connection loss
        mock_server.simulate_connection_loss().await;

        // Stream should automatically reconnect and continue
        for i in 5..10 {
            let slot = receiver.recv().await.unwrap();
            assert_eq!(slot.slot, i);
        }

        assert!(mock_server.reconnection_count() > 0);
    }

    /// Test concurrent client behavior under retry conditions
    #[tokio::test]
    async fn test_concurrent_clients_with_retries() {
        let mut mock_server = MockGrpcServer::new().await;
        mock_server.set_failure_mode(FailureMode::Intermittent, 0);

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(5),
            ..Default::default()
        };

        let num_clients = 10;
        let mut handles = Vec::new();

        for i in 0..num_clients {
            let server_addr = mock_server.address().clone();
            let client_config = config.clone();

            let handle = tokio::spawn(async move {
                let client = RetrySyncClient::new(&server_addr, client_config).await?;
                let response = client.get_latest_slot_with_retry().await?;
                Ok::<_, Box<dyn std::error::Error>>(response.slot)
            });
            handles.push(handle);
        }

        // All clients should eventually succeed
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }

    /// Test performance benchmarks for retry scenarios
    #[tokio::test]
    async fn test_retry_performance_benchmarks() {
        let mut mock_server = MockGrpcServer::new().await;

        // Test different failure rates
        let failure_rates = vec![0.0, 0.1, 0.3, 0.5];
        let mut results = Vec::new();

        for failure_rate in failure_rates {
            mock_server.set_failure_rate(failure_rate);

            let config = RetryConfig {
                max_retries: 3,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            };

            let start_time = Instant::now();
            let num_requests = 100;
            let mut success_count = 0;

            for _ in 0..num_requests {
                let client = RetrySyncClient::new(&mock_server.address(), config.clone())
                    .await
                    .unwrap();
                if client.get_latest_slot_with_retry().await.is_ok() {
                    success_count += 1;
                }
            }

            let elapsed = start_time.elapsed();
            let throughput = num_requests as f64 / elapsed.as_secs_f64();

            results.push(BenchmarkResult {
                failure_rate,
                throughput,
                success_rate: success_count as f64 / num_requests as f64,
                avg_latency: elapsed / num_requests,
            });
        }

        // Validate performance characteristics
        for result in &results {
            println!(
                "Failure rate: {:.1}%, Throughput: {:.2} req/s, Success rate: {:.1}%, Avg latency: {:?}",
                result.failure_rate * 100.0,
                result.throughput,
                result.success_rate * 100.0,
                result.avg_latency
            );

            // Success rate should be high even with failures due to retries
            assert!(result.success_rate >= 0.9);
        }
    }

    /// Test memory usage under retry conditions
    #[tokio::test]
    async fn test_memory_usage_with_retries() {
        let mut mock_server = MockGrpcServer::new().await;
        mock_server.set_failure_mode(FailureMode::Intermittent, 0);

        let config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        };

        let initial_memory = get_memory_usage();

        // Create many clients with retry operations
        let num_operations = 1000;
        for _ in 0..num_operations {
            let client = RetrySyncClient::new(&mock_server.address(), config.clone())
                .await
                .unwrap();
            let _ = client.get_latest_slot_with_retry().await;
        }

        let final_memory = get_memory_usage();
        let memory_increase = final_memory - initial_memory;

        // Memory increase should be reasonable (less than 100MB for 1000 operations)
        assert!(memory_increase < 100 * 1024 * 1024);
    }
}

/// Helper functions for testing

fn validate_retry_config(config: &RetryConfig) -> Result<(), String> {
    if config.max_retries == 0 {
        return Err("max_retries must be greater than 0".to_string());
    }
    if config.initial_delay.is_zero() {
        return Err("initial_delay must be greater than 0".to_string());
    }
    if config.max_delay < config.initial_delay {
        return Err("max_delay must be greater than or equal to initial_delay".to_string());
    }
    if config.backoff_multiplier <= 1.0 {
        return Err("backoff_multiplier must be greater than 1.0".to_string());
    }
    Ok(())
}

fn calculate_backoff_delays(config: &RetryConfig, num_retries: usize) -> Vec<Duration> {
    let mut delays = Vec::new();
    let mut current_delay = config.initial_delay;

    for _ in 0..num_retries {
        if config.jitter {
            let jitter_range = current_delay.as_millis() as f64 * 0.1;
            let jitter = (rand::random::<f64>() - 0.5) * 2.0 * jitter_range;
            let jittered_delay = Duration::from_millis((current_delay.as_millis() as f64 + jitter).max(1.0) as u64);
            delays.push(std::cmp::min(jittered_delay, config.max_delay));
        } else {
            delays.push(std::cmp::min(current_delay, config.max_delay));
        }

        current_delay = Duration::from_millis((current_delay.as_millis() as f64 * config.backoff_multiplier) as u64);
    }

    delays
}

async fn connect_with_retry(address: &str, config: &RetryConfig) -> Result<SyncClient, Box<dyn std::error::Error>> {
    let mut attempt = 0;
    let delays = calculate_backoff_delays(config, config.max_retries as usize);

    loop {
        match SyncClient::connect(address).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                if attempt >= config.max_retries {
                    return Err(e);
                }

                // Check if error is retryable
                if !is_retryable_error(&e) {
                    return Err(e);
                }

                if attempt < delays.len() {
                    sleep(delays[attempt as usize]).await;
                }
                attempt += 1;
            }
        }
    }
}

fn is_retryable_error(error: &Box<dyn std::error::Error>) -> bool {
    // In a real implementation, you would check the specific error types
    // For now, assume all errors are retryable except for certain status codes
    let error_str = error.to_string().to_lowercase();
    !error_str.contains("invalid_argument") && !error_str.contains("permission_denied")
}

fn get_memory_usage() -> usize {
    // Simplified memory usage calculation
    // In a real implementation, you would use a proper memory profiling library
    std::alloc::System.used()
}

#[derive(Debug)]
struct BenchmarkResult {
    failure_rate: f64,
    throughput: f64,
    success_rate: f64,
    avg_latency: Duration,
}

impl RetrySyncClient {
    pub async fn new(address: &str, config: RetryConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let base_client = connect_with_retry(address, &config).await?;
        let circuit_breaker = Arc::new(CircuitBreaker::new(5, Duration::from_secs(30)));

        Ok(Self {
            base_client,
            config,
            circuit_breaker,
        })
    }

    pub async fn get_latest_slot_with_retry(
        &mut self,
    ) -> Result<infinisvm_types::sync::grpc::GetLatestSlotResponse, Box<dyn std::error::Error>> {
        if !self.circuit_breaker.can_execute() {
            return Err("Circuit breaker is open".into());
        }

        let mut attempt = 0;
        let delays = calculate_backoff_delays(&self.config, self.config.max_retries as usize);

        loop {
            match self.base_client.get_latest_slot().await {
                Ok(response) => {
                    self.circuit_breaker.on_success();
                    return Ok(response);
                }
                Err(e) => {
                    self.circuit_breaker.on_failure();

                    if attempt >= self.config.max_retries || !is_retryable_error(&e) {
                        return Err(e);
                    }

                    if attempt < delays.len() {
                        sleep(delays[attempt as usize]).await;
                    }
                    attempt += 1;
                }
            }
        }
    }

    pub async fn subscribe_slots_with_retry(
        &mut self,
    ) -> Result<mpsc::Receiver<infinisvm_types::sync::grpc::SlotDataResponse>, Box<dyn std::error::Error>> {
        // Implementation would include retry logic for stream subscriptions
        self.base_client.subscribe_slots().await
    }
}
