use crate::common::events::{
    emit_event, events_after, get_consumer_offset, latest_event_after, update_consumer_offset,
    EventType, CONSUMER_DATA_DATABASE_BACKUP, CONSUMER_DATA_EQUITY_BARS_EXPORT,
    CONSUMER_DATA_EQUITY_BARS_SYNC, CONSUMER_DATA_TRADING_HISTORY_EXPORT,
};
use crate::data::equity_bars::fetch_and_store_equity_bars;
use crate::data::equity_details;
use crate::data::export;
use crate::data::market_calendar;
use crate::data::state::State;
use crate::data::types::TradingDate;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc, Weekday};
use chrono_tz::US::Eastern;
use sqlx::postgres::PgListener;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const SYNC_HOUR: u32 = 1;
const SYNC_MINUTE: u32 = 0;
const SYNC_DEDUP_TTL_SECS: u64 = 300;

/// Maximum number of retry attempts for a single Massive API fetch.
const FETCH_MAX_RETRIES: u32 = 3;

/// Number of calendar days to look back for gap detection during self-healing sync.
const GAP_DETECTION_LOOKBACK_DAYS: i64 = 90;

fn prior_trading_day(date: NaiveDate) -> NaiveDate {
    let mut prior = date.pred_opt().unwrap();
    while !market_calendar::is_trading_day(prior) {
        prior = prior.pred_opt().unwrap();
    }
    prior
}

fn duration_until_next_sync(now: DateTime<Utc>) -> Duration {
    let now_eastern = now.with_timezone(&Eastern);
    let sync_time = NaiveTime::from_hms_opt(SYNC_HOUR, SYNC_MINUTE, 0).unwrap();

    let mut target_date = now_eastern.date_naive();
    let mut target_eastern = Eastern
        .from_local_datetime(&target_date.and_time(sync_time))
        .earliest()
        .unwrap();

    if now_eastern >= target_eastern {
        target_date = target_date.succ_opt().unwrap();
        target_eastern = Eastern
            .from_local_datetime(&target_date.and_time(sync_time))
            .earliest()
            .unwrap();
    }

    while matches!(target_eastern.weekday(), Weekday::Sat | Weekday::Sun) {
        let next_date = target_eastern.date_naive().succ_opt().unwrap();
        target_eastern = Eastern
            .from_local_datetime(&next_date.and_time(sync_time))
            .earliest()
            .unwrap();
    }

    (target_eastern.with_timezone(&Utc) - now)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

fn sync_date_for(now: DateTime<Utc>) -> TradingDate {
    TradingDate::from_naive_date(prior_trading_day(now.with_timezone(&Eastern).date_naive()))
        .expect("prior_trading_day always returns a weekday")
}

/// Parses an export date from an event payload, falling back to today's UTC date.
///
/// pg_cron jobs include `{"date": "YYYY-MM-DD"}` in the payload so that a
/// catch-up run can export the correct historical date rather than defaulting to
/// the restart date.
fn export_date_from_payload(payload: &serde_json::Value) -> NaiveDate {
    payload
        .get("date")
        .and_then(|value| value.as_str())
        .and_then(|string| NaiveDate::parse_from_str(string, "%Y-%m-%d").ok())
        .unwrap_or_else(|| Utc::now().date_naive())
}

/// Spawns the data sync scheduler loops as background tasks.
///
/// Returns join handles that callers must await after cancelling the
/// `shutdown_token` to allow in-flight work to drain before exit.
pub fn spawn_sync_scheduler(
    state: State,
    shutdown_token: CancellationToken,
) -> Vec<JoinHandle<()>> {
    // Warn if the market calendar holiday table does not cover the current year.
    // Gap detection degrades to weekday-only without holiday coverage, which
    // causes false-positive gap alerts on holidays.
    let current_year = Utc::now().with_timezone(&Eastern).date_naive().year();
    if !market_calendar::has_holiday_coverage(current_year) {
        warn!(
            year = current_year,
            "Market calendar has no holiday data for the current year; \
             gap detection will treat holidays as missing data. \
             Update NYSE_HOLIDAYS in src/data/market_calendar.rs"
        );
    }

    let listen_state = state.clone();
    let mut handles = Vec::new();
    // sync_loop is a fallback timer-based scheduler used only when PostgreSQL is
    // unavailable (e.g., local development without a database). In production the
    // pg_cron + LISTEN/NOTIFY path (listen_loop) is the sole trigger mechanism.
    if !state.database.is_configured() {
        handles.push(tokio::spawn(sync_loop(state, shutdown_token.clone())));
    }
    handles.push(tokio::spawn(listen_loop(listen_state, shutdown_token)));
    handles
}

/// Fetches equity bars for a single trading date with exponential-backoff retry.
///
/// Retries up to [`FETCH_MAX_RETRIES`] times on transient failures, with
/// delays of 1s, 2s between attempts (no delay after the final attempt).
async fn fetch_with_retry(
    state: &State,
    trading_date: &TradingDate,
) -> Result<Option<usize>, String> {
    let mut last_error = String::new();
    for attempt in 0..FETCH_MAX_RETRIES {
        match fetch_and_store_equity_bars(state, trading_date).await {
            Ok(result) => return Ok(result),
            Err(error) => {
                last_error = error;
                if attempt + 1 < FETCH_MAX_RETRIES {
                    let backoff = Duration::from_secs(1 << attempt);
                    warn!(
                        attempt = attempt + 1,
                        max = FETCH_MAX_RETRIES,
                        backoff_seconds = backoff.as_secs(),
                        date = %trading_date.as_naive_date(),
                        error = %last_error,
                        "Equity bar fetch failed, retrying"
                    );
                    sleep(backoff).await;
                }
            }
        }
    }
    Err(last_error)
}

/// Returns expected trading days that are missing from the covered set, excluding
/// any dates in the `exclude` list (typically today and the just-synced primary date).
fn detect_coverage_gaps(
    expected_days: &[NaiveDate],
    covered_dates: &std::collections::HashSet<NaiveDate>,
    exclude: &[NaiveDate],
) -> Vec<NaiveDate> {
    expected_days
        .iter()
        .filter(|date| !covered_dates.contains(date))
        .filter(|date| !exclude.contains(date))
        .copied()
        .collect()
}

/// Self-healing equity bar sync: fetches yesterday's data, then detects and
/// backfills any gaps in the lookback window.
async fn run_equity_bar_sync(state: &State) -> Result<Option<usize>, String> {
    let trading_date = sync_date_for(Utc::now());
    info!(
        "Starting equity bar sync for {}",
        trading_date.as_naive_date().format("%Y-%m-%d")
    );

    // Sync the primary target date first (yesterday's trading day).
    let primary_count = fetch_with_retry(state, &trading_date).await?;
    let mut total_bars = primary_count.unwrap_or(0);

    // Self-healing: detect and backfill gaps in the lookback window.
    let pool = match state.database.pool() {
        Some(pool) => pool,
        None => return Ok(primary_count),
    };

    let today = Utc::now().with_timezone(&Eastern).date_naive();
    let lookback_start = today - chrono::Duration::days(GAP_DETECTION_LOOKBACK_DAYS);
    let expected_days = market_calendar::trading_days_in_range(lookback_start, today);

    let covered_dates =
        match crate::data::database::distinct_equity_bar_dates(pool, lookback_start, today).await {
            Ok(dates) => dates,
            Err(error) => {
                warn!(error = %error, "Gap detection query failed, skipping backfill this run");
                return Ok(Some(total_bars));
            }
        };

    let gaps = detect_coverage_gaps(
        &expected_days,
        &covered_dates,
        &[today, trading_date.as_naive_date()],
    );

    if gaps.is_empty() {
        info!("No gaps detected in equity bar coverage");
        return Ok(Some(total_bars));
    }

    info!(
        gap_count = gaps.len(),
        "Detected gaps in equity bar coverage, backfilling"
    );

    let mut backfilled = 0usize;
    let mut failed = 0usize;
    for gap_date in &gaps {
        let Some(gap_trading_date) = TradingDate::from_naive_date(*gap_date) else {
            continue;
        };
        match fetch_with_retry(state, &gap_trading_date).await {
            Ok(Some(count)) => {
                backfilled += 1;
                total_bars += count;
                info!(
                    date = %gap_date,
                    bars = count,
                    "Backfilled gap"
                );
            }
            Ok(None) => {
                info!(date = %gap_date, "No data available for gap date");
            }
            Err(error) => {
                failed += 1;
                warn!(
                    date = %gap_date,
                    error = %error,
                    "Failed to backfill gap"
                );
            }
        }
    }

    info!(
        gaps_detected = gaps.len(),
        gaps_backfilled = backfilled,
        gaps_failed = failed,
        total_bars = total_bars,
        "Self-healing sync complete"
    );

    Ok(Some(total_bars))
}

async fn sync_loop(state: State, shutdown_token: CancellationToken) {
    loop {
        let wait_duration = duration_until_next_sync(Utc::now());
        info!(
            seconds = wait_duration.as_secs(),
            "Waiting for next equity bar sync"
        );
        tokio::select! {
            _ = sleep(wait_duration) => {}
            _ = shutdown_token.cancelled() => {
                info!("Sync loop stopped for shutdown");
                break;
            }
        }

        if shutdown_token.is_cancelled() {
            break;
        }

        if state.synced_recently(SYNC_DEDUP_TTL_SECS) {
            info!("Skipping sync loop run, synced recently");
            continue;
        }

        match run_equity_bar_sync(&state).await {
            Ok(Some(bar_count)) => {
                info!(rows = bar_count, "Equity bar sync completed");
                state.mark_synced();
            }
            Ok(None) => {
                info!("No equity bar data available for scheduled sync");
            }
            Err(error) => {
                error!(error = %error, "Equity bar sync failed");
            }
        }
    }
}

async fn listen_loop(state: State, shutdown_token: CancellationToken) {
    let pool = match state.database.pool() {
        Some(pool) => pool.clone(),
        None => {
            info!("PostgreSQL not available, LISTEN handler disabled");
            return;
        }
    };

    loop {
        match run_listener(&state, &pool, &shutdown_token).await {
            Ok(()) => {
                if shutdown_token.is_cancelled() {
                    info!("LISTEN handler stopped for shutdown");
                    break;
                }
                info!("LISTEN handler exited, restarting");
            }
            Err(error) => {
                if shutdown_token.is_cancelled() {
                    info!("LISTEN handler stopped for shutdown");
                    break;
                }
                warn!("LISTEN handler error: {}, restarting in 30s", error);
                tokio::select! {
                    _ = sleep(Duration::from_secs(30)) => {}
                    _ = shutdown_token.cancelled() => {
                        info!("LISTEN handler stopped for shutdown");
                        break;
                    }
                }
            }
        }
    }
}

async fn run_listener(
    state: &State,
    pool: &sqlx::PgPool,
    shutdown_token: &CancellationToken,
) -> Result<(), sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen("events").await?;
    info!("Event consumer connected, listening on channel 'events'");

    if shutdown_token.is_cancelled() {
        return Ok(());
    }

    // Catch up on any missed one-time actionable events (latest missed instance each).
    let sync_offset = get_consumer_offset(pool, CONSUMER_DATA_EQUITY_BARS_SYNC).await?;
    if let Some(event_id) =
        latest_event_after(pool, EventType::EquityBarsSyncRequested, sync_offset).await?
    {
        info!(event_id, "Catching up on missed equity_bars_sync_requested");
        handle_equity_bars_sync(state, pool, event_id).await;
    }

    // Export events carry date-specific payloads, so every missed event must be
    // replayed in order. Skipping to the latest would permanently lose export dates
    // for any intermediate days the service was down.
    let export_bars_offset = get_consumer_offset(pool, CONSUMER_DATA_EQUITY_BARS_EXPORT).await?;
    for (event_id, payload) in events_after(
        pool,
        EventType::EquityBarsExportRequested,
        export_bars_offset,
    )
    .await?
    {
        info!(
            event_id,
            "Catching up on missed equity_bars_export_requested"
        );
        handle_equity_bars_export(state, pool, event_id, &payload).await;
    }

    let export_history_offset =
        get_consumer_offset(pool, CONSUMER_DATA_TRADING_HISTORY_EXPORT).await?;
    for (event_id, payload) in events_after(
        pool,
        EventType::TradingHistoryExportRequested,
        export_history_offset,
    )
    .await?
    {
        info!(
            event_id,
            "Catching up on missed trading_history_export_requested"
        );
        handle_trading_history_export(state, pool, event_id, &payload).await;
    }

    let backup_offset = get_consumer_offset(pool, CONSUMER_DATA_DATABASE_BACKUP).await?;
    if let Some(event_id) =
        latest_event_after(pool, EventType::DatabaseBackupRequested, backup_offset).await?
    {
        info!(event_id, "Catching up on missed database_backup_requested");
        handle_database_backup(state, pool, event_id).await;
    }

    // Main event loop.
    loop {
        let notification = tokio::select! {
            result = listener.recv() => result?,
            _ = shutdown_token.cancelled() => {
                info!("Shutdown signal received, draining");
                break;
            }
        };
        let parsed: serde_json::Value = match serde_json::from_str(notification.payload()) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let event_type = parsed
            .get("event_type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let event_id = parsed
            .get("event_id")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let empty_payload = serde_json::Value::Object(Default::default());
        let payload = parsed.get("payload").unwrap_or(&empty_payload);

        if event_type == EventType::EquityBarsSyncRequested.as_str() {
            info!(event_id, "Received equity_bars_sync_requested");
            handle_equity_bars_sync(state, pool, event_id).await;
        } else if event_type == EventType::EquityBarsExportRequested.as_str() {
            info!(event_id, "Received equity_bars_export_requested");
            handle_equity_bars_export(state, pool, event_id, payload).await;
        } else if event_type == EventType::TradingHistoryExportRequested.as_str() {
            info!(event_id, "Received trading_history_export_requested");
            handle_trading_history_export(state, pool, event_id, payload).await;
        } else if event_type == EventType::DatabaseBackupRequested.as_str() {
            info!(event_id, "Received database_backup_requested");
            handle_database_backup(state, pool, event_id).await;
        }
    }

    Ok(())
}

async fn handle_equity_bars_sync(state: &State, pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::EquityBarsSyncStarted,
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit equity_bars_sync_started");
    }

    match run_equity_bar_sync(state).await {
        Ok(Some(bar_count)) => {
            info!(rows = bar_count, "Equity bar sync completed");
            if let Err(error) = emit_event(
                pool,
                EventType::EquityBarsSyncCompleted,
                &serde_json::json!({"bar_count": bar_count}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit equity_bars_sync_completed");
            }
            state.mark_synced();
        }
        Ok(None) => {
            info!("No equity bar data available for sync");
            if let Err(error) = emit_event(
                pool,
                EventType::EquityBarsSyncCompleted,
                &serde_json::json!({"bar_count": 0}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit equity_bars_sync_completed");
            }
        }
        Err(ref error) => {
            error!(error = %error, "Equity bar sync errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::EquityBarsSyncErrored,
                &serde_json::json!({"error": error}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit equity_bars_sync_errored");
            }
        }
    }

    // Self-healing equity details: refresh from embedded CSV on every sync
    // so that sector/industry reclassifications are picked up automatically.
    run_equity_details_sync(state, pool).await;

    if let Err(error) = update_consumer_offset(pool, CONSUMER_DATA_EQUITY_BARS_SYNC, event_id).await
    {
        warn!(error = %error, "Failed to update equity-bars-sync consumer offset");
    }
}

/// Re-seeds equity details from the compile-time embedded CSV.
///
/// Uses `ON CONFLICT DO UPDATE` so sector/industry changes propagate.
/// Also uploads the CSV to S3 to keep the durable store in sync.
async fn run_equity_details_sync(state: &State, pool: &sqlx::PgPool) {
    let details = match equity_details::parse_embedded_equity_details() {
        Ok(details) => details,
        Err(error) => {
            warn!(error = %error, "Failed to parse embedded equity details");
            return;
        }
    };

    match crate::data::database::seed_equity_details(pool, &details).await {
        Ok(count) => info!(rows = count, "Equity details refreshed in PostgreSQL"),
        Err(error) => warn!(error = %error, "Failed to refresh equity details in PostgreSQL"),
    }

    let csv_bytes = equity_details::embedded_csv().as_bytes();
    let key = "data/equity/details/details.csv";
    if let Err(error) = state
        .s3_client
        .put_object()
        .bucket(&state.bucket_name)
        .key(key)
        .body(ByteStream::from(csv_bytes.to_vec()))
        .send()
        .await
    {
        warn!(error = %error, "Failed to upload equity details CSV to S3");
    } else {
        info!(key = key, "Uploaded equity details CSV to S3");
    }
}

async fn handle_equity_bars_export(
    state: &State,
    pool: &sqlx::PgPool,
    event_id: i64,
    payload: &serde_json::Value,
) {
    let export_date = export_date_from_payload(payload);
    if let Err(error) = emit_event(
        pool,
        EventType::EquityBarsExportStarted,
        &serde_json::json!({"date": export_date.to_string()}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit equity_bars_export_started");
    }

    match run_export_job(state, "export-equity-bars", export_date).await {
        Ok(count) => {
            info!(rows = count, "Equity bars export completed");
            if let Err(error) = emit_event(
                pool,
                EventType::EquityBarsExportCompleted,
                &serde_json::json!({"count": count, "date": export_date.to_string()}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit equity_bars_export_completed");
            }
        }
        Err(ref error) => {
            error!(error = %error, "Equity bars export errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::EquityBarsExportErrored,
                &serde_json::json!({"error": error, "date": export_date.to_string()}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit equity_bars_export_errored");
            }
        }
    }

    if let Err(error) =
        update_consumer_offset(pool, CONSUMER_DATA_EQUITY_BARS_EXPORT, event_id).await
    {
        warn!(error = %error, "Failed to update equity-bars-export consumer offset");
    }
}

async fn handle_trading_history_export(
    state: &State,
    pool: &sqlx::PgPool,
    event_id: i64,
    payload: &serde_json::Value,
) {
    let export_date = export_date_from_payload(payload);
    if let Err(error) = emit_event(
        pool,
        EventType::TradingHistoryExportStarted,
        &serde_json::json!({"date": export_date.to_string()}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit trading_history_export_started");
    }

    match run_export_job(state, "export-trading-history", export_date).await {
        Ok(count) => {
            info!(rows = count, "Trading history export completed");
            if let Err(error) = emit_event(
                pool,
                EventType::TradingHistoryExportCompleted,
                &serde_json::json!({"count": count, "date": export_date.to_string()}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit trading_history_export_completed");
            }
        }
        Err(ref error) => {
            error!(error = %error, "Trading history export errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::TradingHistoryExportErrored,
                &serde_json::json!({"error": error, "date": export_date.to_string()}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit trading_history_export_errored");
            }
        }
    }

    if let Err(error) =
        update_consumer_offset(pool, CONSUMER_DATA_TRADING_HISTORY_EXPORT, event_id).await
    {
        warn!(error = %error, "Failed to update trading-history-export consumer offset");
    }
}

async fn handle_database_backup(state: &State, pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::DatabaseBackupStarted,
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit database_backup_started");
    }

    match run_backup_job(state).await {
        Ok(byte_count) => {
            info!(bytes = byte_count, "Database backup completed");
            if let Err(error) = emit_event(
                pool,
                EventType::DatabaseBackupCompleted,
                &serde_json::json!({"byte_count": byte_count}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit database_backup_completed");
            }
        }
        Err(ref error) => {
            error!(error = %error, "Database backup errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::DatabaseBackupErrored,
                &serde_json::json!({"error": error}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit database_backup_errored");
            }
        }
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_DATA_DATABASE_BACKUP, event_id).await
    {
        warn!(error = %error, "Failed to update database-backup consumer offset");
    }
}

async fn run_export_job(
    state: &State,
    job_name: &str,
    export_date: NaiveDate,
) -> Result<usize, String> {
    match job_name {
        "export-equity-bars" => export::export_equity_bars(state, export_date).await,
        "export-trading-history" => export::export_equity_trading_history(state, export_date).await,
        "backup-database" => run_backup_job(state).await,
        _ => {
            let message = format!("Unknown export job: {}", job_name);
            Err(message)
        }
    }
}

/// Runs a full pg_dump of the `fund` database, compresses the output with gzip,
/// and uploads the result to S3.
///
/// The S3 key defaults to `database/backups/fund-latest.dump.gz` and can be
/// overridden with the `AWS_S3_DATABASE_BACKUP_KEY` environment variable.
/// Returns the number of bytes uploaded.
async fn run_backup_job(state: &State) -> Result<usize, String> {
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL not set for database backup".to_string())?;

    let (host, username, port, dbname, password) = parse_postgres_url(&database_url)
        .map_err(|error| format!("Failed to parse DATABASE_URL for pg_dump: {}", error))?;

    let backup_key = std::env::var("AWS_S3_DATABASE_BACKUP_KEY")
        .unwrap_or_else(|_| "database/backups/fund-latest.dump.gz".to_string());

    info!(key = backup_key, "Starting database backup");

    let dump_path = "/tmp/fund-backup.dump";
    let dump_gz_path = "/tmp/fund-backup.dump.gz";

    let mut args = vec![
        "--format=custom".to_string(),
        "--file".to_string(),
        dump_path.to_string(),
        "--host".to_string(),
        host,
        "--port".to_string(),
        port.to_string(),
        "--dbname".to_string(),
        dbname,
    ];
    if let Some(ref name) = username {
        args.push("--username".to_string());
        args.push(name.clone());
    }

    let mut command = tokio::process::Command::new("pg_dump");
    command.args(&args);
    if let Some(ref pass) = password {
        command.env("PGPASSWORD", pass);
    }

    let dump_status = command
        .status()
        .await
        .map_err(|error| format!("Failed to spawn pg_dump: {}", error))?;

    if !dump_status.success() {
        let _ = tokio::fs::remove_file(dump_path).await;
        let message = format!("pg_dump exited with status {}", dump_status);
        return Err(message);
    }

    let gzip_status = tokio::process::Command::new("gzip")
        .args(["--force", dump_path])
        .status()
        .await
        .map_err(|error| {
            let _ = std::fs::remove_file(dump_path);
            format!("Failed to spawn gzip: {}", error)
        })?;

    if !gzip_status.success() {
        let _ = tokio::fs::remove_file(dump_path).await;
        let message = format!("gzip exited with status {}", gzip_status);
        return Err(message);
    }

    let byte_count = tokio::fs::metadata(dump_gz_path)
        .await
        .map_err(|error| format!("Failed to stat {}: {}", dump_gz_path, error))?
        .len() as usize;

    let body = ByteStream::from_path(dump_gz_path)
        .await
        .map_err(|error| format!("Failed to open {} for upload: {}", dump_gz_path, error))?;

    state
        .s3_client
        .put_object()
        .bucket(&state.bucket_name)
        .key(&backup_key)
        .body(body)
        .send()
        .await
        .map_err(|error| {
            let _ = std::fs::remove_file(dump_gz_path);
            format!("Failed to upload backup to S3 {}: {}", backup_key, error)
        })?;

    let _ = tokio::fs::remove_file(dump_gz_path).await;

    info!(
        bytes = byte_count,
        key = backup_key,
        "Database backup uploaded"
    );
    Ok(byte_count)
}

/// Parses a PostgreSQL connection URL into its components.
///
/// Returns `(host, username, port, dbname, password)`.
/// Supports `postgres://` and `postgresql://` schemes.
/// Handles credential-less URLs (e.g., `postgresql://localhost:5432/fund`)
/// by returning `None` for username and password, allowing the caller to
/// omit `--username` and `PGPASSWORD` so libpq falls back to OS defaults.
/// Uses `rsplit_once('@')` so passwords containing `@` are parsed correctly.
fn parse_postgres_url(
    url: &str,
) -> Result<(String, Option<String>, u16, String, Option<String>), String> {
    let without_scheme = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .ok_or_else(|| "DATABASE_URL must start with postgres:// or postgresql://".to_string())?;

    let (username, password, hostinfo_and_db) = match without_scheme.rsplit_once('@') {
        Some((userinfo, rest)) => {
            let (username, password) = match userinfo.split_once(':') {
                Some((user, pass)) => (Some(user.to_string()), Some(pass.to_string())),
                None => (Some(userinfo.to_string()), None),
            };
            (username, password, rest)
        }
        None => (None, None, without_scheme),
    };

    let (hostinfo, dbname) = hostinfo_and_db
        .split_once('/')
        .ok_or_else(|| "DATABASE_URL missing database name after '/'".to_string())?;

    let (host, port_str) = hostinfo.split_once(':').unwrap_or((hostinfo, "5432"));

    let port: u16 = port_str
        .parse()
        .map_err(|_| format!("DATABASE_URL has invalid port: '{}'", port_str))?;
    if port == 0 {
        return Err("DATABASE_URL has invalid port: '0'".to_string());
    }

    Ok((
        host.to_string(),
        username,
        port,
        dbname.to_string(),
        password,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        detect_coverage_gaps, duration_until_next_sync, export_date_from_payload, listen_loop,
        parse_postgres_url, prior_trading_day, run_export_job, spawn_sync_scheduler, sync_date_for,
    };
    use chrono::{NaiveDate, TimeZone, Utc};
    use chrono_tz::US::Eastern;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// RAII guard that removes an environment variable for the duration of a test
    /// and restores the original value (or absence) on drop.
    ///
    /// Tests using this guard must be marked `#[serial_test::serial]` to prevent
    /// concurrent mutation of the process environment.
    struct EnvironmentVariableGuard {
        name: &'static str,
        original: Option<String>,
    }

    impl EnvironmentVariableGuard {
        /// Removes `name` from the environment and returns a guard that restores it on drop.
        fn remove(name: &'static str) -> Self {
            let original = std::env::var(name).ok();
            // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
            unsafe {
                std::env::remove_var(name);
            }
            Self { name, original }
        }
    }

    impl Drop for EnvironmentVariableGuard {
        fn drop(&mut self) {
            // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
            unsafe {
                match self.original.as_ref() {
                    Some(value) => std::env::set_var(self.name, value),
                    None => std::env::remove_var(self.name),
                }
            }
        }
    }

    #[test]
    fn test_duration_until_next_sync_is_positive() {
        let duration = duration_until_next_sync(Utc::now());
        assert!(
            duration.as_secs() > 0,
            "Expected positive wait time, got {:?}",
            duration
        );
    }

    #[test]
    fn test_duration_until_next_sync_is_within_one_week() {
        let duration = duration_until_next_sync(Utc::now());
        let one_week = std::time::Duration::from_secs(7 * 24 * 60 * 60);
        assert!(
            duration <= one_week,
            "Expected wait time within one week, got {:?}",
            duration
        );
    }

    #[test]
    fn test_prior_trading_day_wednesday_returns_tuesday() {
        let wednesday = NaiveDate::from_ymd_opt(2026, 4, 29).unwrap();
        let prior = prior_trading_day(wednesday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 28).unwrap());
    }

    #[test]
    fn test_prior_trading_day_monday_returns_friday() {
        let monday = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let prior = prior_trading_day(monday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 24).unwrap());
    }

    #[test]
    fn test_prior_trading_day_tuesday_returns_monday() {
        let tuesday = NaiveDate::from_ymd_opt(2026, 4, 28).unwrap();
        let prior = prior_trading_day(tuesday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 27).unwrap());
    }

    #[test]
    fn test_duration_until_next_sync_fires_within_one_minute_just_before_1am_et() {
        // Monday 2026-04-27 at 00:59 ET — should fire in ≤ 60 seconds
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 27, 0, 59, 0)
            .unwrap()
            .with_timezone(&Utc);
        let duration = duration_until_next_sync(now);
        assert!(
            duration.as_secs() <= 60,
            "Expected ≤ 60s, got {:?}",
            duration
        );
    }

    #[test]
    fn test_duration_until_next_sync_after_1am_waits_until_next_weekday() {
        // Monday 2026-04-27 at 02:00 ET — next fire is Tuesday 2026-04-28 at 01:00 ET (~23h)
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 27, 2, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let duration = duration_until_next_sync(now);
        let twenty_two_hours = Duration::from_secs(22 * 3600);
        let twenty_four_hours = Duration::from_secs(24 * 3600);
        assert!(
            duration > twenty_two_hours && duration < twenty_four_hours,
            "Expected ~23h, got {:?}",
            duration
        );
    }

    #[test]
    fn test_duration_until_next_sync_from_friday_skips_to_monday() {
        // Friday 2026-05-01 at 02:00 ET — next fire is Monday 2026-05-04 at 01:00 ET (~71h)
        let now = Eastern
            .with_ymd_and_hms(2026, 5, 1, 2, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let duration = duration_until_next_sync(now);
        let seventy_hours = Duration::from_secs(70 * 3600);
        let seventy_two_hours = Duration::from_secs(72 * 3600);
        assert!(
            duration > seventy_hours && duration < seventy_two_hours,
            "Expected ~71h, got {:?}",
            duration
        );
    }

    #[test]
    fn test_sync_date_for_tuesday_fire_is_monday() {
        // Tuesday 2026-04-28 at 01:00 ET — should sync Monday 2026-04-27
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 28, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(
            sync_date.as_naive_date(),
            NaiveDate::from_ymd_opt(2026, 4, 27).unwrap()
        );
    }

    #[test]
    fn test_sync_date_for_monday_fire_is_friday() {
        // Monday 2026-04-27 at 01:00 ET — should sync Friday 2026-04-24
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 27, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(
            sync_date.as_naive_date(),
            NaiveDate::from_ymd_opt(2026, 4, 24).unwrap()
        );
    }

    #[test]
    fn test_sync_date_for_wednesday_fire_is_tuesday() {
        // Wednesday 2026-04-29 at 01:00 ET — should sync Tuesday 2026-04-28
        let now = Eastern
            .with_ymd_and_hms(2026, 4, 29, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(
            sync_date.as_naive_date(),
            NaiveDate::from_ymd_opt(2026, 4, 28).unwrap()
        );
    }

    #[test]
    fn test_export_date_from_payload_parses_date_field() {
        let payload = serde_json::json!({"date": "2026-06-13"});
        let date = export_date_from_payload(&payload);
        assert_eq!(date, NaiveDate::from_ymd_opt(2026, 6, 13).unwrap());
    }

    #[test]
    fn test_export_date_from_payload_falls_back_to_today_on_missing_field() {
        let payload = serde_json::json!({});
        let date = export_date_from_payload(&payload);
        // Fallback is today; allow one day of slack for tests crossing midnight.
        let today = chrono::Utc::now().date_naive();
        assert!(
            date >= today - chrono::Duration::days(1) && date <= today,
            "Expected date near today ({today}), got {date}"
        );
    }

    #[test]
    fn test_export_date_from_payload_falls_back_on_invalid_format() {
        let payload = serde_json::json!({"date": "not-a-date"});
        let date = export_date_from_payload(&payload);
        let today = chrono::Utc::now().date_naive();
        assert!(
            date >= today - chrono::Duration::days(1) && date <= today,
            "Expected date near today ({today}), got {date}"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_run_backup_job_returns_error_when_database_url_missing() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data::state::{MassiveSecrets, State};
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            // Ensure DATABASE_URL is unset so run_backup_job returns early with an error.
            // The guard restores the original value (or absence) when it drops.
            let _database_url_guard = EnvironmentVariableGuard::remove("DATABASE_URL");

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = run_export_job(&state, "backup-database", date).await;

            assert!(result.is_err());
            assert!(result.unwrap_err().contains("DATABASE_URL not set"));
        });
    }

    #[test]
    fn test_parse_postgres_url_full_url() {
        let (host, username, port, dbname, password) =
            parse_postgres_url("postgres://alice:s3cr3t@db.example.com:5433/mydb").unwrap();
        assert_eq!(host, "db.example.com");
        assert_eq!(username, Some("alice".to_string()));
        assert_eq!(port, 5433);
        assert_eq!(dbname, "mydb");
        assert_eq!(password, Some("s3cr3t".to_string()));
    }

    #[test]
    fn test_parse_postgres_url_defaults_port_to_5432() {
        let (host, _, port, _, _) =
            parse_postgres_url("postgres://user:pass@localhost/fund").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 5432);
    }

    #[test]
    fn test_parse_postgres_url_postgresql_scheme() {
        let result = parse_postgres_url("postgresql://user:pass@host/db");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_postgres_url_invalid_scheme_returns_error() {
        let result = parse_postgres_url("mysql://user:pass@host/db");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("postgres:// or postgresql://"));
    }

    #[test]
    fn test_parse_postgres_url_credential_less() {
        let (host, username, port, dbname, password) =
            parse_postgres_url("postgresql://localhost:5432/fund").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(username, None);
        assert_eq!(port, 5432);
        assert_eq!(dbname, "fund");
        assert_eq!(password, None);
    }

    #[test]
    fn test_parse_postgres_url_credential_less_default_port() {
        let (host, username, port, dbname, password) =
            parse_postgres_url("postgres://localhost/fund").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(username, None);
        assert_eq!(port, 5432);
        assert_eq!(dbname, "fund");
        assert_eq!(password, None);
    }

    #[test]
    fn test_parse_postgres_url_missing_dbname_returns_error() {
        let result = parse_postgres_url("postgres://user:pass@host");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("database name"));
    }

    #[test]
    fn test_run_export_job_returns_error_for_unknown_job() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data::state::{MassiveSecrets, State};
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = run_export_job(&state, "unknown-job", date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("Unknown export job"));
        });
    }

    // --- Additional pure-logic tests ---

    #[test]
    fn test_prior_trading_day_thursday_returns_wednesday() {
        let thursday = NaiveDate::from_ymd_opt(2026, 4, 30).unwrap();
        let prior = prior_trading_day(thursday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 29).unwrap());
    }

    #[test]
    fn test_prior_trading_day_friday_returns_thursday() {
        let friday = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
        let prior = prior_trading_day(friday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 4, 30).unwrap());
    }

    #[test]
    fn test_prior_trading_day_sunday_returns_friday() {
        // Sunday's prior trading day should skip Saturday and land on Friday.
        let sunday = NaiveDate::from_ymd_opt(2026, 5, 3).unwrap();
        let prior = prior_trading_day(sunday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 5, 1).unwrap());
    }

    #[test]
    fn test_prior_trading_day_saturday_returns_friday() {
        let saturday = NaiveDate::from_ymd_opt(2026, 5, 2).unwrap();
        let prior = prior_trading_day(saturday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 5, 1).unwrap());
    }

    #[test]
    fn test_prior_trading_day_skips_holiday() {
        // Christmas 2026 is Friday Dec 25. The day after (Dec 28) is Monday.
        // prior_trading_day(Dec 28) should skip the weekend AND Christmas,
        // landing on Wednesday Dec 24.
        let monday = NaiveDate::from_ymd_opt(2026, 12, 28).unwrap();
        let prior = prior_trading_day(monday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 12, 24).unwrap());
    }

    #[test]
    fn test_prior_trading_day_skips_observed_holiday() {
        // Independence Day 2026 is Saturday July 4; observed Friday July 3.
        // prior_trading_day(Mon July 6) should skip weekend + observed holiday,
        // landing on Thursday July 2.
        let monday = NaiveDate::from_ymd_opt(2026, 7, 6).unwrap();
        let prior = prior_trading_day(monday);
        assert_eq!(prior, NaiveDate::from_ymd_opt(2026, 7, 2).unwrap());
    }

    // --- detect_coverage_gaps ---

    #[test]
    fn test_detect_coverage_gaps_finds_missing_dates() {
        let expected = vec![
            NaiveDate::from_ymd_opt(2026, 6, 8).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 9).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
        ];
        let covered: std::collections::HashSet<NaiveDate> =
            [NaiveDate::from_ymd_opt(2026, 6, 8).unwrap()]
                .into_iter()
                .collect();
        let gaps = detect_coverage_gaps(&expected, &covered, &[]);
        assert_eq!(gaps.len(), 2);
        assert!(gaps.contains(&NaiveDate::from_ymd_opt(2026, 6, 9).unwrap()));
        assert!(gaps.contains(&NaiveDate::from_ymd_opt(2026, 6, 10).unwrap()));
    }

    #[test]
    fn test_detect_coverage_gaps_excludes_specified_dates() {
        let expected = vec![
            NaiveDate::from_ymd_opt(2026, 6, 8).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 9).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
        ];
        let covered: std::collections::HashSet<NaiveDate> = std::collections::HashSet::new();
        let exclude = [NaiveDate::from_ymd_opt(2026, 6, 10).unwrap()];
        let gaps = detect_coverage_gaps(&expected, &covered, &exclude);
        assert_eq!(gaps.len(), 2);
        assert!(!gaps.contains(&NaiveDate::from_ymd_opt(2026, 6, 10).unwrap()));
    }

    #[test]
    fn test_detect_coverage_gaps_returns_empty_when_fully_covered() {
        let expected = vec![
            NaiveDate::from_ymd_opt(2026, 6, 8).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 9).unwrap(),
        ];
        let covered: std::collections::HashSet<NaiveDate> = expected.iter().copied().collect();
        let gaps = detect_coverage_gaps(&expected, &covered, &[]);
        assert!(gaps.is_empty());
    }

    #[test]
    fn test_detect_coverage_gaps_empty_expected() {
        let covered: std::collections::HashSet<NaiveDate> = std::collections::HashSet::new();
        let gaps = detect_coverage_gaps(&[], &covered, &[]);
        assert!(gaps.is_empty());
    }

    #[test]
    fn test_sync_date_for_day_after_holiday_skips_holiday() {
        // Tuesday Dec 29, 2026 at 01:00 ET — prior trading day is Wednesday Dec 24
        // (skips Christmas on Dec 25 + weekend Dec 26-27).
        let now = chrono_tz::US::Eastern
            .with_ymd_and_hms(2026, 12, 29, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(
            sync_date.as_naive_date(),
            NaiveDate::from_ymd_opt(2026, 12, 28).unwrap()
        );
    }

    #[test]
    fn test_parse_postgres_url_username_only() {
        let (host, username, port, dbname, password) =
            parse_postgres_url("postgres://useronly@host/db").unwrap();
        assert_eq!(host, "host");
        assert_eq!(username, Some("useronly".to_string()));
        assert_eq!(port, 5432);
        assert_eq!(dbname, "db");
        assert_eq!(password, None);
    }

    #[test]
    fn test_parse_postgres_url_password_containing_at() {
        let (host, username, port, dbname, password) =
            parse_postgres_url("postgres://user:p@ss@host:5432/db").unwrap();
        assert_eq!(host, "host");
        assert_eq!(username, Some("user".to_string()));
        assert_eq!(port, 5432);
        assert_eq!(dbname, "db");
        assert_eq!(password, Some("p@ss".to_string()));
    }

    #[test]
    fn test_parse_postgres_url_invalid_port_returns_error() {
        let result = parse_postgres_url("postgres://user:pass@host:notaport/db");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid port"));
    }

    #[test]
    fn test_export_date_from_payload_parses_various_valid_dates() {
        let cases = [
            ("2026-01-01", (2026, 1, 1)),
            ("2025-12-31", (2025, 12, 31)),
            ("2026-06-18", (2026, 6, 18)),
        ];
        for (date_str, (year, month, day)) in cases {
            let payload = serde_json::json!({"date": date_str});
            let date = export_date_from_payload(&payload);
            assert_eq!(
                date,
                chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap(),
                "Failed for date string: {}",
                date_str
            );
        }
    }

    #[test]
    fn test_export_date_from_payload_falls_back_on_non_string_date_value() {
        // A numeric "date" field is not a valid date string; must fall back to today.
        let payload = serde_json::json!({"date": 20260618});
        let date = export_date_from_payload(&payload);
        let today = chrono::Utc::now().date_naive();
        assert!(
            date >= today - chrono::Duration::days(1) && date <= today,
            "Expected date near today ({today}), got {date}"
        );
    }

    #[tokio::test]
    async fn test_listen_loop_exits_immediately_when_no_pool() {
        // listen_loop returns immediately when the database state has no pool.
        // This covers the early-return path at the top of listen_loop.
        use crate::data::state::{MassiveSecrets, State};
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;

        let credentials =
            Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(credentials)
            .endpoint_url("http://127.0.0.1:9")
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(true)
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
        let state = State::new(
            reqwest::Client::new(),
            MassiveSecrets {
                base: "http://127.0.0.1:1".to_string(),
                key: "test-api-key".to_string(),
            },
            s3_client,
            "test-bucket".to_string(),
        );
        // listen_loop must return immediately (no pool configured).
        let token = CancellationToken::new();
        listen_loop(state, token).await;
    }

    #[tokio::test]
    async fn test_spawn_sync_scheduler_does_not_panic_without_database() {
        // spawn_sync_scheduler must not panic when called with a state that has
        // no database pool. It spawns background tasks that terminate immediately
        // once the runtime drops.
        use crate::data::state::{MassiveSecrets, State};
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;

        let credentials =
            Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(credentials)
            .endpoint_url("http://127.0.0.1:9")
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(true)
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
        let state = State::new(
            reqwest::Client::new(),
            MassiveSecrets {
                base: "http://127.0.0.1:1".to_string(),
                key: "test-api-key".to_string(),
            },
            s3_client,
            "test-bucket".to_string(),
        );
        // DatabaseState::NotConfigured so is_configured() == false, which
        // causes both the sync_loop and listen_loop tasks to be spawned.
        // listen_loop returns immediately without a pool.
        let token = CancellationToken::new();
        let handles = spawn_sync_scheduler(state, token.clone());
        // Cancel so the sync_loop task exits its sleep and terminates.
        token.cancel();
        for handle in handles {
            let _ = handle.await;
        }
    }

    #[test]
    fn test_parse_postgres_url_port_zero_returns_error() {
        // Port 0 is not a valid listening port; the parser must reject it.
        let result = parse_postgres_url("postgres://user:pass@host:0/db");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid port"));
    }

    #[test]
    fn test_parse_postgres_url_max_port_is_valid() {
        let result = parse_postgres_url("postgres://user:pass@host:65535/db");
        assert!(result.is_ok());
        let (_, _, port, _, _) = result.unwrap();
        assert_eq!(port, 65535);
    }

    #[test]
    fn test_export_date_from_payload_non_string_date_falls_back_to_today() {
        // A numeric value under "date" is not a string — must fall back to today.
        let payload = serde_json::json!({"date": 20260613});
        let date = export_date_from_payload(&payload);
        let today = chrono::Utc::now().date_naive();
        assert!(
            date >= today - chrono::Duration::days(1) && date <= today,
            "Expected date near today ({today}), got {date}"
        );
    }

    #[test]
    fn test_export_date_from_payload_null_date_falls_back_to_today() {
        let payload = serde_json::json!({"date": null});
        let date = export_date_from_payload(&payload);
        let today = chrono::Utc::now().date_naive();
        assert!(
            date >= today - chrono::Duration::days(1) && date <= today,
            "Expected date near today ({today}), got {date}"
        );
    }

    #[test]
    fn test_duration_until_next_sync_exactly_at_1am_advances_to_next_weekday() {
        // Monday 2026-04-27 at exactly 01:00 ET — target has already been reached,
        // so the next sync should be on Tuesday 2026-04-28 at 01:00 ET (~24h away).
        let now = chrono_tz::US::Eastern
            .with_ymd_and_hms(2026, 4, 27, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let duration = duration_until_next_sync(now);
        let twenty_three_hours = Duration::from_secs(23 * 3600);
        let twenty_five_hours = Duration::from_secs(25 * 3600);
        assert!(
            duration > twenty_three_hours && duration < twenty_five_hours,
            "Expected ~24h, got {:?}",
            duration
        );
    }

    #[test]
    fn test_sync_date_for_thursday_fire_is_wednesday() {
        // Thursday 2026-04-30 at 01:00 ET — should sync Wednesday 2026-04-29
        let now = chrono_tz::US::Eastern
            .with_ymd_and_hms(2026, 4, 30, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(
            sync_date.as_naive_date(),
            NaiveDate::from_ymd_opt(2026, 4, 29).unwrap()
        );
    }

    #[test]
    fn test_sync_date_for_friday_fire_is_thursday() {
        // Friday 2026-05-01 at 01:00 ET — should sync Thursday 2026-04-30
        let now = chrono_tz::US::Eastern
            .with_ymd_and_hms(2026, 5, 1, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let sync_date = sync_date_for(now);
        assert_eq!(
            sync_date.as_naive_date(),
            NaiveDate::from_ymd_opt(2026, 4, 30).unwrap()
        );
    }

    #[test]
    fn test_run_export_job_export_equity_bars_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data::state::{MassiveSecrets, State};
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = run_export_job(&state, "export-equity-bars", date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("database not connected"));
        });
    }

    #[test]
    fn test_run_export_job_export_trading_history_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data::state::{MassiveSecrets, State};
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = run_export_job(&state, "export-trading-history", date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("database not connected"));
        });
    }
}
