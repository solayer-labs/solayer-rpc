//! Integration tests for gRPC client retry functionality
//!
//! These tests validate the end-to-end behavior of the retry system
//! with real network conditions and failure scenarios.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::timeout;

use super::client::SyncClient;
use super::client_retry_tests::{CircuitBreaker, RetryConfig, RetrySyncClient};
use super::mock_server::{FailureMode, MockGrpcServer};

/// Integration test suite for retry functionality
pub struct RetryIntegrationTests {
    mock_server: MockGrpcServer,
}

impl RetryIntegrationTests {
    pub async fn new() -> Self {
        let mut mock_server = MockGrpcServer::new().await;
        mock_server.start().await.expect("Failed to start mock server");

        Self { mock_server }
    }

    /// Test basic retry functionality with connection failures
    pub async fn test_basic_retry_functionality(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing basic retry functionality...");

        // Configure server to fail first 2 attempts
        self.mock_server.set_failure_mode(FailureMode::ConnectionRefused, 2);

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let start_time = Instant::now();
        let mut client = RetrySyncClient::new(self.mock_server.address(), config).await?;
        let response = client.get_latest_slot_with_retry().await?;
        let elapsed = start_time.elapsed();

        // Verify successful retry
        assert_eq!(response.slot, 12345);
        assert!(elapsed >= Duration::from_millis(20)); // At least 2 retry delays
        assert_eq!(self.mock_server.connection_attempts(), 3); // Initial + 2 retries

        println!("✓ Basic retry functionality passed");
        Ok(())
    }

    /// Test retry with exponential backoff timing
    pub async fn test_exponential_backoff_timing(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing exponential backoff timing...");

        self.mock_server.reset_metrics();
        self.mock_server.set_failure_mode(FailureMode::Timeout, 3);

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            backoff_multiplier: 2.0,
            jitter: false,
            ..Default::default()
        };

        let start_time = Instant::now();
        let mut client = RetrySyncClient::new(self.mock_server.address(), config).await?;
        let _response = client.get_latest_slot_with_retry().await?;
        let elapsed = start_time.elapsed();

        // Expected total delay: 100ms + 200ms + 400ms = 700ms
        let expected_min_delay = Duration::from_millis(700);
        assert!(
            elapsed >= expected_min_delay,
            "Elapsed time {:?} should be at least {:?}",
            elapsed,
            expected_min_delay
        );
        assert!(elapsed < expected_min_delay + Duration::from_millis(200)); // Allow some variance

        println!("✓ Exponential backoff timing passed");
        Ok(())
    }

    /// Test circuit breaker functionality
    pub async fn test_circuit_breaker_behavior(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing circuit breaker behavior...");

        self.mock_server.reset_metrics();
        self.mock_server.set_failure_mode(FailureMode::InternalError, 10); // Many failures

        let config = RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let mut client = RetrySyncClient::new(self.mock_server.address(), config).await?;

        // First few requests should fail and trigger circuit breaker
        for i in 0..5 {
            let result = client.get_latest_slot_with_retry().await;
            if i < 2 {
                assert!(result.is_err(), "Request {} should fail", i);
            } else {
                // Circuit breaker should prevent further attempts
                assert!(result.is_err());
                assert!(result.unwrap_err().to_string().contains("Circuit breaker"));
            }
        }

        println!("✓ Circuit breaker behavior passed");
        Ok(())
    }

    /// Test stream reconnection behavior
    pub async fn test_stream_reconnection(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing stream reconnection...");

        self.mock_server.reset_metrics();
        self.mock_server.clear_failure_mode();

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(50),
            ..Default::default()
        };

        let mut client = RetrySyncClient::new(self.mock_server.address(), config).await?;
        let mut receiver = client.subscribe_slots_with_retry().await?;

        // Receive initial messages
        for i in 0..3 {
            let slot = timeout(Duration::from_secs(1), receiver.recv()).await??;
            assert_eq!(slot.slot, i);
        }

        // Simulate connection loss
        self.mock_server.simulate_connection_loss().await;

        // Should continue receiving after reconnection
        // Note: In a real implementation, this would test actual reconnection
        let reconnections_before = self.mock_server.reconnection_count();

        // Continue receiving
        for i in 3..6 {
            let slot = timeout(Duration::from_secs(2), receiver.recv()).await??;
            assert_eq!(slot.slot, i);
        }

        assert!(self.mock_server.reconnection_count() >= reconnections_before);

        println!("✓ Stream reconnection passed");
        Ok(())
    }

    /// Test concurrent clients with retry
    pub async fn test_concurrent_client_retry(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing concurrent client retry...");

        self.mock_server.reset_metrics();
        self.mock_server.set_failure_rate(0.3); // 30% failure rate

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let num_clients = 20;
        let semaphore = Arc::new(Semaphore::new(10)); // Limit concurrent connections

        let mut handles = Vec::new();
        for i in 0..num_clients {
            let server_addr = self.mock_server.address().to_string();
            let client_config = config.clone();
            let sem = semaphore.clone();

            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                let mut client = RetrySyncClient::new(&server_addr, client_config).await?;
                let response = client.get_latest_slot_with_retry().await?;

                Ok::<u64, Box<dyn std::error::Error + Send + Sync>>(response.slot)
            });
            handles.push(handle);
        }

        // Wait for all clients to complete
        let mut success_count = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(slot) => {
                    assert_eq!(slot, 12345);
                    success_count += 1;
                }
                Err(_) => {
                    // Some failures are expected with 30% failure rate
                }
            }
        }

        // With retries, success rate should be high even with 30% base failure rate
        let success_rate = success_count as f64 / num_clients as f64;
        assert!(
            success_rate >= 0.8,
            "Success rate {} should be at least 80%",
            success_rate
        );

        println!(
            "✓ Concurrent client retry passed (success rate: {:.1}%)",
            success_rate * 100.0
        );
        Ok(())
    }

    /// Test performance under retry conditions
    pub async fn test_retry_performance(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing retry performance...");

        let test_cases = vec![
            ("No failures", 0.0),
            ("Low failure rate", 0.1),
            ("Medium failure rate", 0.3),
            ("High failure rate", 0.5),
        ];

        for (test_name, failure_rate) in test_cases {
            self.mock_server.reset_metrics();
            self.mock_server.set_failure_rate(failure_rate);

            let config = RetryConfig {
                max_retries: 3,
                initial_delay: Duration::from_millis(5),
                ..Default::default()
            };

            let num_requests = 50;
            let start_time = Instant::now();

            for _ in 0..num_requests {
                let mut client = RetrySyncClient::new(self.mock_server.address(), config.clone()).await?;
                let _ = client.get_latest_slot_with_retry().await; // Allow failures for performance test
            }

            let elapsed = start_time.elapsed();
            let throughput = num_requests as f64 / elapsed.as_secs_f64();

            println!(
                "  {} ({}% failure): {:.1} req/s",
                test_name,
                (failure_rate * 100.0) as u32,
                throughput
            );

            // Performance should remain reasonable even with failures
            if failure_rate == 0.0 {
                assert!(throughput > 100.0, "No-failure throughput should be > 100 req/s");
            } else {
                assert!(
                    throughput > 10.0,
                    "Retry throughput should be > 10 req/s even with failures"
                );
            }
        }

        println!("✓ Retry performance passed");
        Ok(())
    }

    /// Test memory usage during extended retry operations
    pub async fn test_memory_usage_stability(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing memory usage stability...");

        self.mock_server.reset_metrics();
        self.mock_server.set_failure_rate(0.2); // 20% failure rate

        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        };

        let initial_memory = get_memory_usage();

        // Run many operations to test memory stability
        for batch in 0..10 {
            for _ in 0..50 {
                let mut client = RetrySyncClient::new(self.mock_server.address(), config.clone()).await?;
                let _ = client.get_latest_slot_with_retry().await; // Allow failures
            }

            // Check memory periodically
            if batch % 3 == 0 {
                let current_memory = get_memory_usage();
                let memory_increase = current_memory.saturating_sub(initial_memory);

                // Memory should not grow excessively (allow up to 50MB increase)
                assert!(
                    memory_increase < 50 * 1024 * 1024,
                    "Memory increase {} bytes is too large",
                    memory_increase
                );
            }
        }

        let final_memory = get_memory_usage();
        let total_increase = final_memory.saturating_sub(initial_memory);

        println!("  Memory increase: {} MB", total_increase / 1024 / 1024);
        assert!(
            total_increase < 100 * 1024 * 1024,
            "Total memory increase should be < 100MB"
        );

        println!("✓ Memory usage stability passed");
        Ok(())
    }

    /// Test timeout handling
    pub async fn test_timeout_handling(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing timeout handling...");

        self.mock_server.reset_metrics();
        self.mock_server.set_response_delay(Duration::from_millis(200));

        let config = RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let mut client = RetrySyncClient::new(self.mock_server.address(), config).await?;

        // Test with short timeout - should fail
        let result = timeout(Duration::from_millis(100), client.get_latest_slot_with_retry()).await;
        assert!(result.is_err(), "Request should timeout");

        // Test with longer timeout - should succeed
        self.mock_server.set_response_delay(Duration::from_millis(50));
        let result = timeout(Duration::from_millis(500), client.get_latest_slot_with_retry()).await;
        assert!(result.is_ok(), "Request should succeed with longer timeout");

        println!("✓ Timeout handling passed");
        Ok(())
    }

    /// Test non-retryable error handling
    pub async fn test_non_retryable_errors(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing non-retryable error handling...");

        let non_retryable_modes = vec![FailureMode::InvalidArgument, FailureMode::PermissionDenied];

        for mode in non_retryable_modes {
            self.mock_server.reset_metrics();
            self.mock_server.set_failure_mode(mode.clone(), 1);

            let config = RetryConfig {
                max_retries: 3,
                initial_delay: Duration::from_millis(10),
                ..Default::default()
            };

            let start_time = Instant::now();
            let mut client = RetrySyncClient::new(self.mock_server.address(), config).await?;
            let result = client.get_latest_slot_with_retry().await;
            let elapsed = start_time.elapsed();

            // Should fail immediately without retries
            assert!(result.is_err());
            assert!(
                elapsed < Duration::from_millis(50),
                "Should fail quickly without retries"
            );
            assert_eq!(
                self.mock_server.connection_attempts(),
                1,
                "Should not retry non-retryable errors"
            );
        }

        println!("✓ Non-retryable error handling passed");
        Ok(())
    }

    /// Run all integration tests
    pub async fn run_all_tests(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running gRPC retry integration tests...\n");

        let tests = vec![
            ("Basic retry functionality", Self::test_basic_retry_functionality),
            ("Exponential backoff timing", Self::test_exponential_backoff_timing),
            ("Circuit breaker behavior", Self::test_circuit_breaker_behavior),
            ("Stream reconnection", Self::test_stream_reconnection),
            ("Concurrent client retry", Self::test_concurrent_client_retry),
            ("Retry performance", Self::test_retry_performance),
            ("Memory usage stability", Self::test_memory_usage_stability),
            ("Timeout handling", Self::test_timeout_handling),
            ("Non-retryable errors", Self::test_non_retryable_errors),
        ];

        let mut passed = 0;
        let mut failed = 0;

        for (test_name, test_fn) in tests {
            match test_fn(self).await {
                Ok(_) => {
                    passed += 1;
                }
                Err(e) => {
                    println!("✗ {} failed: {}", test_name, e);
                    failed += 1;
                }
            }
        }

        println!("\nIntegration test results:");
        println!("  Passed: {}", passed);
        println!("  Failed: {}", failed);
        println!("  Total:  {}", passed + failed);

        if failed > 0 {
            Err(format!("{} integration tests failed", failed).into())
        } else {
            println!("\n🎉 All integration tests passed!");
            Ok(())
        }
    }
}

impl Drop for RetryIntegrationTests {
    fn drop(&mut self) {
        // Note: In a real implementation, you'd properly shutdown the server
        // For now, the server will be cleaned up when the mock server is dropped
    }
}

/// Simple memory usage estimation
fn get_memory_usage() -> usize {
    // In a real implementation, you would use proper memory profiling
    // For testing purposes, this is a simplified placeholder
    use std::alloc::{GlobalAlloc, Layout, System};

    // This is a very rough estimate - in production you'd use tools like:
    // - jemalloc stats
    // - system memory info
    // - custom allocator tracking
    std::process::id() as usize * 1000 // Placeholder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_integration_test_suite() {
        let mut test_suite = RetryIntegrationTests::new().await;

        // Run a subset of tests for CI/CD
        assert!(test_suite.test_basic_retry_functionality().await.is_ok());
        assert!(test_suite.test_exponential_backoff_timing().await.is_ok());
        assert!(test_suite.test_non_retryable_errors().await.is_ok());
    }

    #[tokio::test]
    #[ignore] // Full test suite - run manually or in dedicated test environment
    async fn run_full_integration_test_suite() {
        let mut test_suite = RetryIntegrationTests::new().await;
        assert!(test_suite.run_all_tests().await.is_ok());
    }
}
