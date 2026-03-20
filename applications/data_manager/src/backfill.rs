use crate::equity_bars::{date_already_synced, sync_date};
use crate::state::State;
use chrono::{Datelike, Duration, Utc, Weekday};
use metrics::{counter, gauge};
use tracing::{error, info, warn};

const DEFAULT_LOOKBACK_DAYS: i64 = 730; // 2 years (Massive subscription limit)
const MAX_BACKOFF_SECONDS: u64 = 300; // 5 minutes
const BASE_DELAY_MILLIS: u64 = 500;
const HOURLY_SYNC_LOOKBACK_DAYS: i64 = 5;
const HOURLY_SYNC_INTERVAL_SECONDS: u64 = 3600;

pub fn spawn_backfill(state: State) {
    let lookback_days: i64 = std::env::var("BACKFILL_LOOKBACK_DAYS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_LOOKBACK_DAYS);

    let skip_backfill = std::env::var("SKIP_DATA_SYNC")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if skip_backfill {
        info!("Backfill skipped (SKIP_DATA_SYNC=true)");
        return;
    }

    tokio::spawn(async move {
        wait_for_storage(&state).await;

        info!(
            "Object storage ready, starting equity bars backfill ({} days)",
            lookback_days
        );
        run_backfill(&state, lookback_days).await;

        info!("Starting hourly sync loop");
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(HOURLY_SYNC_INTERVAL_SECONDS)).await;
            info!("Running hourly data sync");
            run_backfill(&state, HOURLY_SYNC_LOOKBACK_DAYS).await;
        }
    });
}

const STORAGE_READY_TIMEOUT_SECONDS: u64 = 120;

async fn wait_for_storage(state: &State) {
    let endpoint = std::env::var("AWS_ENDPOINT_URL").unwrap_or_default();
    let is_local = endpoint.contains("localhost") || endpoint.contains("127.0.0.1");

    if !is_local {
        info!("Using cloud storage, skipping readiness check");
        return;
    }

    info!("Waiting for local object storage to be ready");
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(STORAGE_READY_TIMEOUT_SECONDS);
    loop {
        if state
            .s3_client
            .head_bucket()
            .bucket(&state.bucket_name)
            .send()
            .await
            .is_ok()
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            error!(
                "Object storage not ready after {}s, proceeding anyway",
                STORAGE_READY_TIMEOUT_SECONDS
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn run_backfill(state: &State, lookback_days: i64) {
    let today = Utc::now();
    let mut synced = 0u64;
    let mut skipped = 0u64;
    let mut failed = 0u64;
    let mut consecutive_rate_limits: u32 = 0;

    for days_ago in 0..lookback_days {
        let date = today - Duration::days(days_ago);
        let weekday = date.weekday();
        if weekday == Weekday::Sat || weekday == Weekday::Sun {
            continue;
        }

        let formatted = date.format("%Y-%m-%d");

        if date_already_synced(state, &date).await {
            skipped += 1;
            consecutive_rate_limits = 0;
            continue;
        }

        match sync_date(state, &date).await {
            Ok(_) => {
                synced += 1;
                consecutive_rate_limits = 0;
                counter!("data_sync_dates_synced_total").increment(1);
                gauge!("data_sync_last_success_timestamp")
                    .set(Utc::now().timestamp() as f64);
                info!("Backfilled {}", formatted);
            }
            Err(message) if message.contains("No market data") => {
                skipped += 1;
                consecutive_rate_limits = 0;
            }
            Err(message) if message.contains("Rate limited") => {
                consecutive_rate_limits += 1;
                counter!("data_sync_rate_limits_total").increment(1);

                let backoff_seconds = calculate_backoff(consecutive_rate_limits);
                warn!(
                    "Rate limited on {}, backing off {}s (attempt {})",
                    formatted, backoff_seconds, consecutive_rate_limits
                );
                tokio::time::sleep(std::time::Duration::from_secs(backoff_seconds)).await;

                // Retry once after backoff
                match sync_date(state, &date).await {
                    Ok(_) => {
                        synced += 1;
                        consecutive_rate_limits = 0;
                        counter!("data_sync_dates_synced_total").increment(1);
                        gauge!("data_sync_last_success_timestamp")
                            .set(Utc::now().timestamp() as f64);
                        info!("Backfilled {} after rate limit retry", formatted);
                    }
                    Err(retry_message) => {
                        failed += 1;
                        counter!("data_sync_errors_total").increment(1);
                        warn!("Backfill failed for {} after retry: {}", formatted, retry_message);
                    }
                }
            }
            Err(message) => {
                failed += 1;
                consecutive_rate_limits = 0;
                counter!("data_sync_errors_total").increment(1);
                warn!("Backfill failed for {}: {}", formatted, message);
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(BASE_DELAY_MILLIS)).await;
    }

    gauge!("data_sync_last_run_timestamp").set(Utc::now().timestamp() as f64);
    info!(
        "Backfill complete: {} synced, {} skipped, {} failed",
        synced, skipped, failed
    );

    if failed > 0 {
        error!("{} dates failed during backfill", failed);
    }
}

fn calculate_backoff(consecutive_failures: u32) -> u64 {
    let backoff = 2u64.saturating_pow(consecutive_failures);
    backoff.min(MAX_BACKOFF_SECONDS)
}
