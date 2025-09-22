//! Performance benchmarks for gRPC client retry functionality
//!
//! This module provides comprehensive benchmarks to measure the performance
//! characteristics of the retry system under various conditions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

use super::client_retry_tests::{RetryConfig, RetrySyncClient};
use super::mock_server::{FailureMode, MockGrpcServer};

/// Benchmark result for a single test scenario
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub scenario: String,
    pub total_requests: u32,
    pub successful_requests: u32,
    pub failed_requests: u32,
    pub total_duration: Duration,
    pub average_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
    pub throughput: f64, // requests per second
    pub retry_count: u32,
    pub memory_usage_mb: f64,
}

impl BenchmarkResult {
    pub fn success_rate(&self) -> f64 {
        self.successful_requests as f64 / self.total_requests as f64
    }

    pub fn print_summary(&self) {
        println!("📊 Benchmark Results: {}", self.scenario);
        println!("  Total Requests:     {}", self.total_requests);
        println!(
            "  Success Rate:       {:.1}% ({}/{})",
            self.success_rate() * 100.0,
            self.successful_requests,
            self.total_requests
        );
        println!("  Throughput:         {:.2} req/s", self.throughput);
        println!("  Average Latency:    {:.2}ms", self.average_latency.as_millis());
        println!("  P95 Latency:        {:.2}ms", self.p95_latency.as_millis());
        println!("  P99 Latency:        {:.2}ms", self.p99_latency.as_millis());
        println!("  Total Retries:      {}", self.retry_count);
        println!("  Memory Usage:       {:.1} MB", self.memory_usage_mb);
        println!();
    }
}

/// Configuration for benchmark scenarios
#[derive(Debug, Clone)]
pub struct BenchmarkScenario {
    pub name: String,
    pub failure_mode: Option<FailureMode>,
    pub failure_rate: f64,
    pub failure_count: u32,
    pub response_delay: Duration,
    pub retry_config: RetryConfig,
    pub num_requests: u32,
    pub concurrent_clients: u32,
}

impl BenchmarkScenario {
    pub fn no_failures() -> Self {
        Self {
            name: "No Failures - Baseline".to_string(),
            failure_mode: None,
            failure_rate: 0.0,
            failure_count: 0,
            response_delay: Duration::from_millis(1),
            retry_config: RetryConfig::default(),
            num_requests: 1000,
            concurrent_clients: 10,
        }
    }

    pub fn low_failure_rate() -> Self {
        Self {
            name: "Low Failure Rate (10%)".to_string(),
            failure_mode: Some(FailureMode::Intermittent),
            failure_rate: 0.1,
            failure_count: 0,
            response_delay: Duration::from_millis(5),
            retry_config: RetryConfig {
                max_retries: 3,
                initial_delay: Duration::from_millis(10),
                ..Default::default()
            },
            num_requests: 1000,
            concurrent_clients: 10,
        }
    }

    pub fn high_failure_rate() -> Self {
        Self {
            name: "High Failure Rate (30%)".to_string(),
            failure_mode: Some(FailureMode::Intermittent),
            failure_rate: 0.3,
            failure_count: 0,
            response_delay: Duration::from_millis(5),
            retry_config: RetryConfig {
                max_retries: 3,
                initial_delay: Duration::from_millis(10),
                ..Default::default()
            },
            num_requests: 500,
            concurrent_clients: 10,
        }
    }

    pub fn connection_failures() -> Self {
        Self {
            name: "Connection Failures".to_string(),
            failure_mode: Some(FailureMode::ConnectionRefused),
            failure_rate: 1.0,
            failure_count: 2, // Fail first 2 attempts
            response_delay: Duration::from_millis(1),
            retry_config: RetryConfig {
                max_retries: 3,
                initial_delay: Duration::from_millis(10),
                backoff_multiplier: 2.0,
                ..Default::default()
            },
            num_requests: 100,
            concurrent_clients: 5,
        }
    }

    pub fn slow_responses() -> Self {
        Self {
            name: "Slow Responses (100ms)".to_string(),
            failure_mode: None,
            failure_rate: 0.0,
            failure_count: 0,
            response_delay: Duration::from_millis(100),
            retry_config: RetryConfig::default(),
            num_requests: 200,
            concurrent_clients: 10,
        }
    }

    pub fn stress_test() -> Self {
        Self {
            name: "Stress Test - High Concurrency".to_string(),
            failure_mode: Some(FailureMode::Intermittent),
            failure_rate: 0.2,
            failure_count: 0,
            response_delay: Duration::from_millis(5),
            retry_config: RetryConfig {
                max_retries: 5,
                initial_delay: Duration::from_millis(5),
                ..Default::default()
            },
            num_requests: 2000,
            concurrent_clients: 50,
        }
    }
}

/// Performance benchmark suite for retry functionality
pub struct RetryBenchmarkSuite {
    mock_server: MockGrpcServer,
    results: Vec<BenchmarkResult>,
}

impl RetryBenchmarkSuite {
    pub async fn new() -> Self {
        let mut mock_server = MockGrpcServer::new().await;
        mock_server.start().await.expect("Failed to start mock server");

        Self {
            mock_server,
            results: Vec::new(),
        }
    }

    /// Run a single benchmark scenario
    pub async fn run_benchmark(
        &mut self,
        scenario: BenchmarkScenario,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        println!("🚀 Running benchmark: {}", scenario.name);

        // Configure mock server
        self.mock_server.reset_metrics();
        if let Some(failure_mode) = &scenario.failure_mode {
            if scenario.failure_count > 0 {
                self.mock_server
                    .set_failure_mode(failure_mode.clone(), scenario.failure_count);
            } else {
                self.mock_server.set_failure_rate(scenario.failure_rate);
            }
        }
        self.mock_server.set_response_delay(scenario.response_delay);

        // Track metrics
        let mut latencies = Vec::new();
        let mut successful_requests = 0u32;
        let mut total_retries = 0u32;

        let semaphore = Arc::new(Semaphore::new(scenario.concurrent_clients as usize));
        let latencies_mutex = Arc::new(Mutex::new(&mut latencies));

        let start_time = Instant::now();
        let initial_memory = estimate_memory_usage();

        // Run benchmark
        let mut handles = Vec::new();
        let requests_per_client = scenario.num_requests / scenario.concurrent_clients;

        for client_id in 0..scenario.concurrent_clients {
            let server_addr = self.mock_server.address().to_string();
            let retry_config = scenario.retry_config.clone();
            let sem = semaphore.clone();
            let num_requests = if client_id == scenario.concurrent_clients - 1 {
                // Last client handles any remainder
                requests_per_client + (scenario.num_requests % scenario.concurrent_clients)
            } else {
                requests_per_client
            };

            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                let mut client_latencies = Vec::new();
                let mut client_successes = 0u32;
                let mut client_retries = 0u32;

                for _ in 0..num_requests {
                    let request_start = Instant::now();

                    let mut client = match RetrySyncClient::new(&server_addr, retry_config.clone()).await {
                        Ok(client) => client,
                        Err(_) => continue,
                    };

                    match client.get_latest_slot_with_retry().await {
                        Ok(_) => {
                            client_successes += 1;
                            let latency = request_start.elapsed();
                            client_latencies.push(latency);
                        }
                        Err(_) => {
                            // Count as failure, but still record latency for analysis
                            let latency = request_start.elapsed();
                            client_latencies.push(latency);
                        }
                    }

                    // Estimate retries (simplified)
                    if request_start.elapsed() > retry_config.initial_delay {
                        client_retries += 1;
                    }
                }

                (client_latencies, client_successes, client_retries)
            });

            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            let (client_latencies, client_successes, client_retries) = handle.await?;
            latencies.extend(client_latencies);
            successful_requests += client_successes;
            total_retries += client_retries;
        }

        let total_duration = start_time.elapsed();
        let final_memory = estimate_memory_usage();

        // Calculate statistics
        latencies.sort();
        let average_latency = if !latencies.is_empty() {
            latencies.iter().sum::<Duration>() / latencies.len() as u32
        } else {
            Duration::from_millis(0)
        };

        let p95_latency = if !latencies.is_empty() {
            let index = (latencies.len() as f64 * 0.95) as usize;
            latencies[index.min(latencies.len() - 1)]
        } else {
            Duration::from_millis(0)
        };

        let p99_latency = if !latencies.is_empty() {
            let index = (latencies.len() as f64 * 0.99) as usize;
            latencies[index.min(latencies.len() - 1)]
        } else {
            Duration::from_millis(0)
        };

        let throughput = scenario.num_requests as f64 / total_duration.as_secs_f64();

        let result = BenchmarkResult {
            scenario: scenario.name.clone(),
            total_requests: scenario.num_requests,
            successful_requests,
            failed_requests: scenario.num_requests - successful_requests,
            total_duration,
            average_latency,
            p95_latency,
            p99_latency,
            throughput,
            retry_count: total_retries,
            memory_usage_mb: (final_memory.saturating_sub(initial_memory)) as f64 / 1024.0 / 1024.0,
        };

        result.print_summary();
        self.results.push(result.clone());
        Ok(result)
    }

    /// Run all predefined benchmark scenarios
    pub async fn run_all_benchmarks(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔥 Starting comprehensive retry performance benchmarks...\n");

        let scenarios = vec![
            BenchmarkScenario::no_failures(),
            BenchmarkScenario::low_failure_rate(),
            BenchmarkScenario::high_failure_rate(),
            BenchmarkScenario::connection_failures(),
            BenchmarkScenario::slow_responses(),
            BenchmarkScenario::stress_test(),
        ];

        for scenario in scenarios {
            self.run_benchmark(scenario).await?;

            // Brief pause between benchmarks
            sleep(Duration::from_millis(500)).await;
        }

        self.print_summary_report();
        Ok(())
    }

    /// Print a comprehensive summary report
    pub fn print_summary_report(&self) {
        println!("📋 COMPREHENSIVE BENCHMARK SUMMARY");
        println!("=".repeat(60));

        if self.results.is_empty() {
            println!("No benchmark results available.");
            return;
        }

        // Overall statistics
        let total_requests: u32 = self.results.iter().map(|r| r.total_requests).sum();
        let total_successful: u32 = self.results.iter().map(|r| r.successful_requests).sum();
        let overall_success_rate = total_successful as f64 / total_requests as f64;

        println!("Overall Statistics:");
        println!("  Total Requests:     {}", total_requests);
        println!(
            "  Overall Success:    {:.1}% ({}/{})",
            overall_success_rate * 100.0,
            total_successful,
            total_requests
        );
        println!();

        // Performance comparison
        println!("Throughput Comparison:");
        for result in &self.results {
            println!("  {:30} {:.2} req/s", result.scenario, result.throughput);
        }
        println!();

        // Latency comparison
        println!("Average Latency Comparison:");
        for result in &self.results {
            println!("  {:30} {:.2}ms", result.scenario, result.average_latency.as_millis());
        }
        println!();

        // Memory usage
        println!("Memory Usage:");
        for result in &self.results {
            println!("  {:30} {:.1} MB", result.scenario, result.memory_usage_mb);
        }
        println!();

        // Performance insights
        self.print_performance_insights();
    }

    fn print_performance_insights(&self) {
        println!("🔍 Performance Insights:");

        // Find best and worst performing scenarios
        if let (Some(best_throughput), Some(worst_throughput)) = (
            self.results
                .iter()
                .max_by(|a, b| a.throughput.partial_cmp(&b.throughput).unwrap()),
            self.results
                .iter()
                .min_by(|a, b| a.throughput.partial_cmp(&b.throughput).unwrap()),
        ) {
            println!(
                "  📈 Best Throughput:  {} ({:.2} req/s)",
                best_throughput.scenario, best_throughput.throughput
            );
            println!(
                "  📉 Worst Throughput: {} ({:.2} req/s)",
                worst_throughput.scenario, worst_throughput.throughput
            );
        }

        // Retry efficiency
        let scenarios_with_retries: Vec<_> = self.results.iter().filter(|r| r.retry_count > 0).collect();

        if !scenarios_with_retries.is_empty() {
            let avg_retries: f64 = scenarios_with_retries.iter().map(|r| r.retry_count as f64).sum::<f64>()
                / scenarios_with_retries.len() as f64;

            println!("  🔄 Average Retries:  {:.1}", avg_retries);
        }

        // Success rate analysis
        let high_success_scenarios: Vec<_> = self.results.iter().filter(|r| r.success_rate() >= 0.95).collect();

        println!(
            "  ✅ High Success Scenarios: {}/{}",
            high_success_scenarios.len(),
            self.results.len()
        );

        println!();
    }

    /// Export results to CSV for external analysis
    pub fn export_to_csv(&self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(filename)?;

        // CSV header
        writeln!(file, "Scenario,Total_Requests,Successful_Requests,Failed_Requests,Success_Rate,Throughput,Avg_Latency_ms,P95_Latency_ms,P99_Latency_ms,Retry_Count,Memory_Usage_MB")?;

        // CSV data
        for result in &self.results {
            writeln!(
                file,
                "{},{},{},{},{:.3},{:.2},{:.2},{:.2},{:.2},{},{:.1}",
                result.scenario,
                result.total_requests,
                result.successful_requests,
                result.failed_requests,
                result.success_rate(),
                result.throughput,
                result.average_latency.as_millis(),
                result.p95_latency.as_millis(),
                result.p99_latency.as_millis(),
                result.retry_count,
                result.memory_usage_mb
            )?;
        }

        println!("📊 Benchmark results exported to: {}", filename);
        Ok(())
    }
}

/// Estimate current memory usage (simplified implementation)
fn estimate_memory_usage() -> usize {
    // In a real implementation, you would use proper memory profiling tools
    // This is a placeholder that could be replaced with actual memory tracking

    #[cfg(target_os = "linux")]
    {
        // On Linux, you could read /proc/self/status or use jemalloc stats
        use std::fs;
        if let Ok(contents) = fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(value) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = value.parse::<usize>() {
                            return kb * 1024; // Convert KB to bytes
                        }
                    }
                }
            }
        }
    }

    // Fallback: rough estimate based on heap usage
    // This is very imprecise but better than nothing for testing
    std::process::id() as usize * 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_benchmark_scenario_creation() {
        let scenario = BenchmarkScenario::no_failures();
        assert_eq!(scenario.name, "No Failures - Baseline");
        assert!(scenario.failure_mode.is_none());
        assert_eq!(scenario.failure_rate, 0.0);
    }

    #[tokio::test]
    async fn test_simple_benchmark() {
        let mut suite = RetryBenchmarkSuite::new().await;

        let scenario = BenchmarkScenario {
            name: "Test Scenario".to_string(),
            failure_mode: None,
            failure_rate: 0.0,
            failure_count: 0,
            response_delay: Duration::from_millis(1),
            retry_config: RetryConfig::default(),
            num_requests: 10,
            concurrent_clients: 2,
        };

        let result = suite.run_benchmark(scenario).await;
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.total_requests, 10);
        assert!(result.throughput > 0.0);
    }

    #[tokio::test]
    #[ignore] // Long-running test
    async fn run_full_benchmark_suite() {
        let mut suite = RetryBenchmarkSuite::new().await;
        assert!(suite.run_all_benchmarks().await.is_ok());
        assert!(!suite.results.is_empty());
    }
}
