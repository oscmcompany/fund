//! PostgreSQL event bus stress test.
//!
//! Inserts synthetic events at a configurable rate and measures insert latency,
//! sustained throughput, and LISTEN/NOTIFY delivery latency. Results are printed
//! as a summary table to stdout.
//!
//! Usage: `stress_test_events [--rate <msgs/sec>] [--duration <seconds>] [--batch-size <n>]`
//!
//! Defaults: rate=1000, duration=30, batch-size=100.
//!
//! ## Baseline results (local dev PostgreSQL, single-row `emit_event` inserts)
//!
//! | Target rate | Actual rate | Batch size | p50 latency | p99 latency | Max latency |
//! |-------------|-------------|------------|-------------|-------------|-------------|
//! | 1,000/s     | 981/s       | 100        | 25.39ms     | 64.46ms     | 64.46ms     |
//! | 5,000/s     | 4,569/s     | 100        | 14.41ms     | 26.10ms     | 49.34ms     |
//! | 10,000/s    | 7,434/s     | 100        | 13.18ms     | 16.27ms     | 53.87ms     |
//! | 10,000/s    | 7,463/s     | 500        | 65.93ms     | 104.10ms    | 106.30ms    |
//! | 20,000/s    | 6,952/s     | 100        | 14.20ms     | 18.72ms     | 147.81ms    |
//!
//! Throughput ceiling is approximately 7,400 events/sec with individual stored
//! procedure calls. NOTIFY round-trip latency is stable at 13-15ms p50 regardless
//! of load. Degradation at high rates is graceful (max latency increases but median
//! stays stable). Batch size does not improve throughput because each event is a
//! separate stored procedure call.

use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{error, info};

use fund::common::events::{emit_event, EventType};
use fund::common::observability::init_tracing;

const USAGE: &str =
    "Usage: stress_test_events [--rate <msgs/sec>] [--duration <seconds>] [--batch-size <n>]";

const DEFAULT_RATE: u64 = 1000;
const DEFAULT_DURATION_SECONDS: u64 = 30;
const DEFAULT_BATCH_SIZE: u64 = 100;

/// Dedicated variant for stress testing. Uses a text column so no schema
/// migration needed. Events are cleaned up after the test.
const STRESS_TEST_EVENT_TYPE: EventType = EventType::StressTest;

#[derive(Debug)]
struct Arguments {
    rate: u64,
    duration_seconds: u64,
    batch_size: u64,
}

impl Default for Arguments {
    fn default() -> Self {
        Self {
            rate: DEFAULT_RATE,
            duration_seconds: DEFAULT_DURATION_SECONDS,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

fn parse_arguments(arguments: &[String]) -> Result<Arguments, String> {
    let mut result = Arguments::default();
    let mut index = 0;

    while index < arguments.len() {
        match arguments[index].as_str() {
            "--rate" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| "--rate requires a value".to_string())?;
                result.rate = value.parse().map_err(|_| {
                    format!("Invalid rate '{}': expected a positive integer", value)
                })?;
                if result.rate == 0 {
                    return Err("--rate must be greater than zero".to_string());
                }
            }
            "--duration" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| "--duration requires a value".to_string())?;
                result.duration_seconds = value.parse().map_err(|_| {
                    format!("Invalid duration '{}': expected a positive integer", value)
                })?;
                if result.duration_seconds == 0 {
                    return Err("--duration must be greater than zero".to_string());
                }
            }
            "--batch-size" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| "--batch-size requires a value".to_string())?;
                result.batch_size = value.parse().map_err(|_| {
                    format!(
                        "Invalid batch-size '{}': expected a positive integer",
                        value
                    )
                })?;
                if result.batch_size == 0 {
                    return Err("--batch-size must be greater than zero".to_string());
                }
            }
            other => {
                return Err(format!("Unknown argument '{}'\n{}", other, USAGE));
            }
        }
        index += 1;
    }

    Ok(result)
}

/// Latency statistics computed from a sorted list of durations.
struct LatencyStatistics {
    count: usize,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    maximum: Duration,
}

impl LatencyStatistics {
    fn from_sorted(sorted_latencies: &[Duration]) -> Option<Self> {
        if sorted_latencies.is_empty() {
            return None;
        }
        let count = sorted_latencies.len();
        Some(Self {
            count,
            p50: sorted_latencies[count / 2],
            p95: sorted_latencies[count * 95 / 100],
            p99: sorted_latencies[count * 99 / 100],
            maximum: sorted_latencies[count - 1],
        })
    }

    fn print(&self, label: &str) {
        println!(
            "  {}: n={}, p50={:.2}ms, p95={:.2}ms, p99={:.2}ms, max={:.2}ms",
            label,
            self.count,
            self.p50.as_secs_f64() * 1000.0,
            self.p95.as_secs_f64() * 1000.0,
            self.p99.as_secs_f64() * 1000.0,
            self.maximum.as_secs_f64() * 1000.0,
        );
    }
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    let _tracing_guard = init_tracing("stress-test-events.log", Some("warn"), "stress-test-events");

    let raw_arguments: Vec<String> = std::env::args().skip(1).collect();
    let arguments = match parse_arguments(&raw_arguments) {
        Ok(arguments) => arguments,
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(2);
        }
    };

    println!(
        "Stress test: rate={} msgs/sec, duration={}s, batch_size={}",
        arguments.rate, arguments.duration_seconds, arguments.batch_size
    );

    let database_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("DATABASE_URL environment variable must be set");
            std::process::exit(1);
        }
    };

    let pool = match PgPool::connect(&database_url).await {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("Failed to connect to PostgreSQL: {}", error);
            std::process::exit(1);
        }
    };

    info!("Connected to PostgreSQL");

    let test_result = run_stress_test(&pool, &arguments).await;

    // Always clean up synthetic events, even if the test failed.
    println!("\n--- Cleanup ---");
    let deleted = sqlx::query_scalar::<_, i64>(
        "WITH deleted AS (
            DELETE FROM events
            WHERE event_type = $1
              AND payload @> '{\"stress_test\": true}'::jsonb
            RETURNING 1
        ) SELECT COUNT(*) FROM deleted",
    )
    .bind(STRESS_TEST_EVENT_TYPE.as_str())
    .fetch_one(&pool)
    .await;

    match deleted {
        Ok(count) => println!("Cleaned up {} stress test events", count),
        Err(error) => eprintln!("Warning: cleanup failed: {}", error),
    }

    if let Err(error) = test_result {
        error!(error = %error, "Stress test failed");
        eprintln!("Stress test failed: {}", error);
        std::process::exit(1);
    }
}

async fn run_stress_test(
    pool: &PgPool,
    arguments: &Arguments,
) -> Result<(), Box<dyn std::error::Error>> {
    // --- Phase 1: Insert throughput test ---
    println!("\n--- Phase 1: Insert throughput ---");

    let total_messages = arguments.rate * arguments.duration_seconds;
    let batch_count = total_messages / arguments.batch_size;
    if batch_count == 0 {
        return Err(format!(
            "Total events ({}) is less than batch size ({}). Reduce --batch-size or increase --rate/--duration.",
            total_messages, arguments.batch_size
        )
        .into());
    }
    let interval_per_batch =
        Duration::from_secs_f64(arguments.batch_size as f64 / arguments.rate as f64);

    println!(
        "Target: {} total events in {} batches of {}",
        total_messages, batch_count, arguments.batch_size
    );

    let mut batch_latencies: Vec<Duration> = Vec::with_capacity(batch_count as usize);
    let test_start = Instant::now();
    let mut events_inserted: u64 = 0;

    for batch_index in 0..batch_count {
        let batch_start = Instant::now();

        for _ in 0..arguments.batch_size {
            let payload = serde_json::json!({
                "stress_test": true,
                "batch": batch_index,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            });
            emit_event(pool, STRESS_TEST_EVENT_TYPE, &payload).await?;
            events_inserted += 1;
        }

        let batch_elapsed = batch_start.elapsed();
        batch_latencies.push(batch_elapsed);

        // Pace to maintain target rate.
        if batch_elapsed < interval_per_batch {
            tokio::time::sleep(interval_per_batch - batch_elapsed).await;
        }
    }

    let test_elapsed = test_start.elapsed();
    let actual_rate = events_inserted as f64 / test_elapsed.as_secs_f64();

    println!(
        "Inserted {} events in {:.2}s ({:.0} events/sec)",
        events_inserted,
        test_elapsed.as_secs_f64(),
        actual_rate
    );

    batch_latencies.sort();
    if let Some(statistics) = LatencyStatistics::from_sorted(&batch_latencies) {
        statistics.print(&format!(
            "Batch latency (batch_size={})",
            arguments.batch_size
        ));
    }

    // --- Phase 2: LISTEN/NOTIFY round-trip latency ---
    println!("\n--- Phase 2: LISTEN/NOTIFY round-trip latency ---");

    let notify_count: u64 = 100;
    let (sender, mut receiver) = mpsc::channel::<Instant>(notify_count as usize);

    // Spawn a listener task.
    let listener_pool = pool.clone();
    let listener_handle = tokio::spawn(async move {
        let mut listener = sqlx::postgres::PgListener::connect_with(&listener_pool)
            .await
            .expect("Failed to create PG listener");
        listener
            .listen("events")
            .await
            .expect("Failed to LISTEN on events channel");

        let mut latencies: Vec<Duration> = Vec::with_capacity(notify_count as usize);

        loop {
            tokio::select! {
                result = listener.recv() => {
                    match result {
                        Ok(notification) => {
                            // Filter: only process our probe notifications.
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(notification.payload()) {
                                if parsed.get("notify_latency_probe").and_then(|value| value.as_bool()) == Some(true) {
                                    if let Some(sent_at) = receiver.recv().await {
                                        latencies.push(sent_at.elapsed());
                                        if latencies.len() as u64 >= notify_count {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            eprintln!("Listener error: {}", error);
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    eprintln!("Listener timed out after 30 seconds");
                    break;
                }
            }
        }

        latencies
    });

    // Give the listener time to connect and subscribe.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Emit events one at a time, recording send timestamps.
    for _ in 0..notify_count {
        let sent_at = Instant::now();
        let _ = sender.send(sent_at).await;
        let payload = serde_json::json!({
            "stress_test": true,
            "notify_latency_probe": true,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });
        emit_event(pool, STRESS_TEST_EVENT_TYPE, &payload).await?;
        // Small delay between probes to avoid batching effects.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut notify_latencies = listener_handle.await?;
    notify_latencies.sort();

    if let Some(statistics) = LatencyStatistics::from_sorted(&notify_latencies) {
        statistics.print("NOTIFY round-trip");
    } else {
        println!("  No NOTIFY latencies recorded");
    }

    println!("\n--- Summary ---");
    println!("  Target rate: {} events/sec", arguments.rate);
    println!("  Actual rate: {:.0} events/sec", actual_rate);
    println!("  Duration: {:.2}s", test_elapsed.as_secs_f64());
    println!("  Total events: {}", events_inserted);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_arguments_defaults() {
        let arguments = parse_arguments(&[]).unwrap();
        assert_eq!(arguments.rate, DEFAULT_RATE);
        assert_eq!(arguments.duration_seconds, DEFAULT_DURATION_SECONDS);
        assert_eq!(arguments.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn test_parse_arguments_all_flags() {
        let arguments = parse_arguments(&[
            "--rate".to_string(),
            "5000".to_string(),
            "--duration".to_string(),
            "60".to_string(),
            "--batch-size".to_string(),
            "200".to_string(),
        ])
        .unwrap();
        assert_eq!(arguments.rate, 5000);
        assert_eq!(arguments.duration_seconds, 60);
        assert_eq!(arguments.batch_size, 200);
    }

    #[test]
    fn test_parse_arguments_rejects_zero_rate() {
        let error = parse_arguments(&["--rate".to_string(), "0".to_string()]).unwrap_err();
        assert!(error.contains("greater than zero"));
    }

    #[test]
    fn test_parse_arguments_rejects_zero_duration() {
        let error = parse_arguments(&["--duration".to_string(), "0".to_string()]).unwrap_err();
        assert!(error.contains("greater than zero"));
    }

    #[test]
    fn test_parse_arguments_rejects_zero_batch_size() {
        let error = parse_arguments(&["--batch-size".to_string(), "0".to_string()]).unwrap_err();
        assert!(error.contains("greater than zero"));
    }

    #[test]
    fn test_parse_arguments_rejects_unknown_flag() {
        let error = parse_arguments(&["--unknown".to_string()]).unwrap_err();
        assert!(error.contains("Unknown argument"));
    }

    #[test]
    fn test_parse_arguments_rejects_missing_value() {
        assert!(parse_arguments(&["--rate".to_string()]).is_err());
        assert!(parse_arguments(&["--duration".to_string()]).is_err());
        assert!(parse_arguments(&["--batch-size".to_string()]).is_err());
    }

    #[test]
    fn test_parse_arguments_rejects_non_numeric() {
        assert!(parse_arguments(&["--rate".to_string(), "abc".to_string()]).is_err());
    }

    #[test]
    fn test_latency_statistics_from_sorted() {
        let latencies: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let statistics = LatencyStatistics::from_sorted(&latencies).unwrap();
        assert_eq!(statistics.count, 100);
        // Index-based: count/2=50 → element at index 50 is 51ms (1-indexed values).
        assert_eq!(statistics.p50, Duration::from_millis(51));
        assert_eq!(statistics.p95, Duration::from_millis(96));
        assert_eq!(statistics.p99, Duration::from_millis(100));
        assert_eq!(statistics.maximum, Duration::from_millis(100));
    }

    #[test]
    fn test_latency_statistics_empty() {
        assert!(LatencyStatistics::from_sorted(&[]).is_none());
    }

    #[test]
    fn test_latency_statistics_single_element() {
        let latencies = vec![Duration::from_millis(42)];
        let statistics = LatencyStatistics::from_sorted(&latencies).unwrap();
        assert_eq!(statistics.count, 1);
        assert_eq!(statistics.p50, Duration::from_millis(42));
        assert_eq!(statistics.maximum, Duration::from_millis(42));
    }
}
