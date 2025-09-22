#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

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

    /// Test backoff with jitter
    #[test]
    fn test_calculate_backoff_with_jitter() {
        let initial_backoff = 100;
        let max_backoff = 5000;
        let multiplier = 2.0;
        let jitter_factor = 0.1;

        for attempt in 1..=5 {
            let backoff = SyncClient::calculate_backoff(attempt, initial_backoff, max_backoff, multiplier, jitter_factor);
            let expected_base = (initial_backoff as f64 * multiplier.powi(attempt as i32 - 1)) as u64;
            let expected_capped = expected_base.min(max_backoff);
            let max_jitter = (expected_capped as f64 * jitter_factor) as u64;
            
            // Backoff should be between base and base + max_jitter
            assert!(backoff >= expected_capped);
            assert!(backoff <= expected_capped + max_jitter);
        }
    }

    /// Test retryable error detection
    #[test]
    fn test_is_retryable_error() {
        use tonic::{Code, Status};

        // Retryable errors
        assert!(SyncClient::is_retryable_error(&Status::new(Code::Unavailable, "Service unavailable")));
        assert!(SyncClient::is_retryable_error(&Status::new(Code::DeadlineExceeded, "Timeout")));
        assert!(SyncClient::is_retryable_error(&Status::new(Code::ResourceExhausted, "Rate limited")));
        assert!(SyncClient::is_retryable_error(&Status::new(Code::Aborted, "Request aborted")));

        // Retryable internal errors
        assert!(SyncClient::is_retryable_error(&Status::new(Code::Internal, "Connection error occurred")));
        assert!(SyncClient::is_retryable_error(&Status::new(Code::Internal, "Network timeout detected")));

        // Non-retryable errors
        assert!(!SyncClient::is_retryable_error(&Status::new(Code::InvalidArgument, "Bad request")));
        assert!(!SyncClient::is_retryable_error(&Status::new(Code::NotFound, "Resource not found")));
        assert!(!SyncClient::is_retryable_error(&Status::new(Code::PermissionDenied, "Access denied")));
        assert!(!SyncClient::is_retryable_error(&Status::new(Code::Internal, "Database corruption")));
    }

    /// Test circuit breaker functionality
    #[tokio::test]
    async fn test_circuit_breaker() {
        let threshold = 3;
        let timeout_duration = Duration::from_millis(100);
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
        tokio::time::sleep(timeout_duration + Duration::from_millis(10)).await;

        // Should be half-open now
        assert!(circuit_breaker.can_execute().await);

        // Record success to close circuit
        circuit_breaker.record_success().await;
        assert!(circuit_breaker.can_execute().await);
    }

    /// Test circuit breaker state transitions
    #[tokio::test]
    async fn test_circuit_breaker_states() {
        let threshold = 2;
        let timeout_duration = Duration::from_millis(50);
        let circuit_breaker = CircuitBreaker::new(threshold, timeout_duration);

        // Test Closed -> Open transition
        assert!(circuit_breaker.can_execute().await);
        circuit_breaker.record_failure().await;
        assert!(circuit_breaker.can_execute().await); // Still closed
        circuit_breaker.record_failure().await;
        assert!(!circuit_breaker.can_execute().await); // Now open

        // Test Open -> HalfOpen transition
        tokio::time::sleep(timeout_duration + Duration::from_millis(10)).await;
        assert!(circuit_breaker.can_execute().await); // Now half-open

        // Test HalfOpen -> Closed transition
        circuit_breaker.record_success().await;
        assert!(circuit_breaker.can_execute().await); // Now closed
    }

    /// Test custom retry configuration
    #[test]
    fn test_custom_retry_config() {
        let config = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 200,
            max_backoff_ms: 10000,
            backoff_multiplier: 1.5,
            jitter_factor: 0.2,
            enable_circuit_breaker: false,
            circuit_breaker_threshold: 10,
            circuit_breaker_timeout_secs: 60,
        };

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 10000);
        assert_eq!(config.backoff_multiplier, 1.5);
        assert_eq!(config.jitter_factor, 0.2);
        assert!(!config.enable_circuit_breaker);
        assert_eq!(config.circuit_breaker_threshold, 10);
        assert_eq!(config.circuit_breaker_timeout_secs, 60);
    }

    /// Integration test for retry configuration validation
    #[test]
    fn test_retry_config_validation() {
        // Test valid configuration
        let valid_config = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout_secs: 30,
        };

        // Should be able to create a circuit breaker with this config
        let _circuit_breaker = CircuitBreaker::new(
            valid_config.circuit_breaker_threshold,
            Duration::from_secs(valid_config.circuit_breaker_timeout_secs),
        );

        // Verify backoff calculations are reasonable
        for attempt in 1..=valid_config.max_retries {
            let backoff = SyncClient::calculate_backoff(
                attempt,
                valid_config.initial_backoff_ms,
                valid_config.max_backoff_ms,
                valid_config.backoff_multiplier,
                valid_config.jitter_factor,
            );
            assert!(backoff >= valid_config.initial_backoff_ms);
            assert!(backoff <= valid_config.max_backoff_ms + (valid_config.max_backoff_ms as f64 * valid_config.jitter_factor) as u64);
        }
    }

    /// Test error handling in retry logic
    #[test]
    fn test_error_categorization() {
        use tonic::{Code, Status};

        // Test various error codes
        let test_cases = vec![
            (Code::Ok, false),
            (Code::Cancelled, false),
            (Code::Unknown, false),
            (Code::InvalidArgument, false),
            (Code::DeadlineExceeded, true),
            (Code::NotFound, false),
            (Code::AlreadyExists, false),
            (Code::PermissionDenied, false),
            (Code::ResourceExhausted, true),
            (Code::FailedPrecondition, false),
            (Code::Aborted, true),
            (Code::OutOfRange, false),
            (Code::Unimplemented, false),
            (Code::Internal, false), // Unless contains specific keywords
            (Code::Unavailable, true),
            (Code::DataLoss, false),
            (Code::Unauthenticated, false),
        ];

        for (code, expected_retryable) in test_cases {
            let status = Status::new(code, "Test message");
            assert_eq!(
                SyncClient::is_retryable_error(&status),
                expected_retryable,
                "Code {:?} should be retryable: {}",
                code,
                expected_retryable
            );
        }

        // Test internal errors with connection-related messages
        let retryable_internal_messages = vec![
            "Connection reset by peer",
            "Network timeout occurred",
            "Connection error",
            "Timeout waiting for response",
        ];

        for message in retryable_internal_messages {
            let status = Status::new(Code::Internal, message);
            assert!(
                SyncClient::is_retryable_error(&status),
                "Internal error with message '{}' should be retryable",
                message
            );
        }

        // Test internal errors with non-connection-related messages
        let non_retryable_internal_messages = vec![
            "Database corruption detected",
            "Invalid state",
            "Logic error",
            "Assertion failed",
        ];

        for message in non_retryable_internal_messages {
            let status = Status::new(Code::Internal, message);
            assert!(
                !SyncClient::is_retryable_error(&status),
                "Internal error with message '{}' should not be retryable",
                message
            );
        }
    }
}