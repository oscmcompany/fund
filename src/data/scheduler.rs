use crate::common::events::{
    emit_event, events_after, get_consumer_offset, latest_event_after, run_event_listener,
    update_consumer_offset, EventType, Outcome, CONSUMER_DATA_DATABASE_BACKUP,
    CONSUMER_DATA_DATABASE_EXPORT, CONSUMER_DATA_DATABASE_PURGE, CONSUMER_DATA_EQUITY_BARS_SYNC,
    CONSUMER_DATA_MARKET_CALENDAR, CONSUMER_DATA_MODEL_ARTIFACT, CONSUMER_DATA_SCHEDULER_HEALTH,
};
use crate::data::equity_bars::fetch_and_store_equity_bars;
use crate::data::equity_details;
use crate::data::export;
use crate::data::market_calendar;
use crate::data::market_calendar_sync;
use crate::data::model_artifact;
use crate::data::state::State;
use crate::data::types::TradingDate;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::US::Eastern;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Maximum number of retry attempts for a single Massive API fetch.
const FETCH_MAX_RETRIES: u32 = 3;

/// Number of calendar days to look back for gap detection during self-healing sync.
const GAP_DETECTION_LOOKBACK_DAYS: i64 = 90;

/// pg_cron job names that must be present for the nightly pipeline to function.
///
/// There is deliberately no evaluation job here, and none in `schema.sql` either:
/// intraday work is driven by the live-quote evaluator, which emits
/// `portfolio_evaluation_requested` only on a real threshold crossing. Cron opens
/// and closes the session; it does not drive the work in between.
///
/// Every entry except `cron-run-details-cleanup` is named for the event it emits.
/// That job runs a `DELETE` directly and emits nothing, which is why it does not
/// carry an event-shaped name.
const EXPECTED_CRON_JOBS: &[&str] = &[
    "market-calendar-sync-requested",
    "equity-bars-sync-requested",
    "equity-predictions-requested",
    "trading-session-started",
    "portfolio-liquidation-requested",
    "database-export-requested",
    "scheduler-health-check-requested",
    "model-artifact-check-requested",
    "cron-run-details-cleanup",
];

/// Event types that must fire on every trading day.
///
/// Freshness is a trading-day question, not an elapsed-hours one. This was a
/// list of `(event_type, hours)` pairs, both 26 hours, and a fixed hour count
/// false-positives on exactly the events it covered: on Monday morning the last
/// `equity_bars_sync_requested` is Friday's, roughly 72 hours old, which tripped
/// a 26-hour threshold every single week. The same arithmetic is why the session
/// events were excluded entirely, leaving the event that begins each trading day
/// with no monitoring at all.
///
/// Keyed on [`EventType`] rather than on strings: the stored name is derivable
/// from the variant, and a string here could name an event no build emits.
const MONITORED_EVENTS: &[EventType] = &[
    EventType::EquityBarsSync(Outcome::Requested),
    EventType::DatabaseExport(Outcome::Requested),
    EventType::TradingSessionStarted,
    EventType::EquityPredictions(Outcome::Requested),
    EventType::PortfolioLiquidation(Outcome::Requested),
];

fn prior_trading_day(date: NaiveDate) -> NaiveDate {
    let mut prior = date.pred_opt().unwrap();
    while !market_calendar::is_trading_day(prior) {
        prior = prior.pred_opt().unwrap();
    }
    prior
}

/// Returns the most recent trading day at `now` — today when today trades,
/// otherwise the previous trading day.
fn most_recent_trading_day(now: DateTime<Utc>) -> NaiveDate {
    let today = now.with_timezone(&Eastern).date_naive();
    if market_calendar::is_trading_day(today) {
        today
    } else {
        prior_trading_day(today)
    }
}

/// Returns the instant the most recent trading day began, in Eastern terms.
///
/// An event that has not fired since this instant has missed a trading day.
fn most_recent_trading_day_start(now: DateTime<Utc>) -> DateTime<Utc> {
    let date = most_recent_trading_day(now);
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid time of day");
    Eastern
        .from_local_datetime(&midnight)
        .single()
        // US DST transitions happen at 02:00 local, so Eastern midnight is
        // always unambiguous; `earliest` is a total fallback rather than a real
        // case.
        .unwrap_or_else(|| {
            Eastern
                .from_local_datetime(&midnight)
                .earliest()
                .expect("Eastern midnight always resolves")
        })
        .with_timezone(&Utc)
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

    // pg_cron plus LISTEN/NOTIFY is the sole trigger mechanism. A timer-based
    // fallback scheduler used to run when DATABASE_URL was unset, but PostgreSQL is
    // configured in devenv and in production alike, so that branch was unreachable.
    vec![tokio::spawn(listen_loop(state, shutdown_token))]
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

/// Outcome of comparing the registered pg_cron jobs against the expected set.
#[derive(Debug, Default, PartialEq, Eq)]
struct CronJobReport {
    /// Jobs `schema.sql` creates that are absent from `cron.job`.
    missing: Vec<String>,
    /// Jobs present in `cron.job` that no current `schema.sql` creates.
    unexpected: Vec<String>,
    /// Whether pg_cron is installed at all. `false` means neither list is meaningful.
    pg_cron_available: bool,
}

impl CronJobReport {
    /// Returns `true` when the schedule matches `schema.sql` in both directions.
    fn is_healthy(&self) -> bool {
        self.missing.is_empty() && self.unexpected.is_empty()
    }
}

/// Compares the registered pg_cron jobs against [`EXPECTED_CRON_JOBS`], both ways.
///
/// Gracefully skips if pg_cron is not installed (vanilla PostgreSQL in tests or
/// local development without extensions).
///
/// The unexpected-job direction is as load-bearing as the missing-job one.
/// `schema.sql` is additive and idempotent by convention, so it has no way to
/// remove a job it no longer creates: a job scheduled by an older schema version
/// survives forever and nothing notices. Three such orphans were found by hand in
/// the development database in July 2026 — two emitting event types that no
/// longer parse, and one emitting a live control event that would have triggered
/// a second nightly backup once the application ran.
async fn validate_cron_jobs(pool: &sqlx::PgPool) -> CronJobReport {
    // Check whether the cron schema exists before querying it.
    let cron_exists = match sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'cron')",
    )
    .fetch_one(pool)
    .await
    {
        Ok(value) => value,
        Err(error) => {
            warn!(error = %error, "Failed to check pg_cron availability, skipping job validation");
            return CronJobReport::default();
        }
    };

    if !cron_exists {
        info!("pg_cron not available, skipping job validation");
        return CronJobReport::default();
    }

    let registered: Vec<String> = match sqlx::query_scalar("SELECT jobname FROM cron.job")
        .fetch_all(pool)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            warn!(error = %error, "Failed to query pg_cron jobs, skipping job validation");
            return CronJobReport {
                pg_cron_available: true,
                ..CronJobReport::default()
            };
        }
    };

    let report = compare_cron_jobs(&registered);

    for job_name in &report.missing {
        error!(job_name, "Expected pg_cron job is missing");
    }
    for job_name in &report.unexpected {
        warn!(
            job_name,
            "Unexpected pg_cron job is registered; no current schema.sql creates it"
        );
    }

    if report.is_healthy() {
        info!(
            job_count = EXPECTED_CRON_JOBS.len(),
            "Registered pg_cron jobs match the expected schedule"
        );
    } else {
        warn!(
            missing = report.missing.len(),
            unexpected = report.unexpected.len(),
            total = EXPECTED_CRON_JOBS.len(),
            "Registered pg_cron jobs do not match the expected schedule"
        );
    }

    report
}

/// Compares registered job names against [`EXPECTED_CRON_JOBS`] in both directions.
///
/// Separated from the query so the comparison is testable without pg_cron.
fn compare_cron_jobs(registered: &[String]) -> CronJobReport {
    CronJobReport {
        missing: EXPECTED_CRON_JOBS
            .iter()
            .filter(|expected| !registered.iter().any(|name| name == *expected))
            .map(|expected| (*expected).to_string())
            .collect(),
        unexpected: registered
            .iter()
            .filter(|name| !EXPECTED_CRON_JOBS.contains(&name.as_str()))
            .cloned()
            .collect(),
        pg_cron_available: true,
    }
}

/// Checks that each monitored event has fired on the most recent trading day.
///
/// An overdue event means pg_cron is not running, or a job's SQL is failing
/// silently, or the job was never scheduled at all.
///
/// Only sees events inside the nightly purge retention window. The purge deletes
/// from `events` with a one-day cutoff (`data::database::run_database_purge`), so
/// absence here means "not within retention", not "never happened" — see the note
/// at the `events` entry in that table list, which points back at this function.
/// Shortening that retention, or running the purge on weekends, blinds this check.
///
/// One known edge: a startup before a trading day's jobs have fired — the bars
/// sync runs at 05:00 UTC — reports that day's events as overdue, because they
/// genuinely have not fired yet. The scheduled run at 10:00 Eastern is past every
/// daily job, so it does not see this.
async fn check_event_freshness(pool: &sqlx::PgPool) -> Vec<EventType> {
    let now = Utc::now();
    let trading_day_start = most_recent_trading_day_start(now);
    let mut stale = Vec::new();

    for &event_type in MONITORED_EVENTS {
        let last_seen: Option<DateTime<Utc>> = match sqlx::query_scalar(
            "SELECT created_at FROM events WHERE event_type = $1 ORDER BY id DESC LIMIT 1",
        )
        .bind(event_type.as_str())
        .fetch_optional(pool)
        .await
        {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    event_type = event_type.as_str(),
                    error = %error,
                    "Failed to query event freshness, skipping"
                );
                continue;
            }
        };

        if !is_event_stale(last_seen, trading_day_start) {
            continue;
        }

        stale.push(event_type);
        match last_seen {
            Some(timestamp) => warn!(
                event_type = event_type.as_str(),
                last_seen = %timestamp,
                trading_day_start = %trading_day_start,
                "Event has not fired on the most recent trading day"
            ),
            None => warn!(
                event_type = event_type.as_str(),
                "Event has never been recorded within the retention window"
            ),
        }
    }

    if stale.is_empty() {
        info!(
            event_count = MONITORED_EVENTS.len(),
            "All monitored event types fired on the most recent trading day"
        );
    }

    stale
}

/// Returns `true` when the event has not fired since the trading day began.
///
/// Never seen counts as stale: within the retention window, an event that has
/// left no row has not fired.
fn is_event_stale(last_seen: Option<DateTime<Utc>>, trading_day_start: DateTime<Utc>) -> bool {
    match last_seen {
        None => true,
        Some(timestamp) => timestamp < trading_day_start,
    }
}

async fn run_listener(
    state: &State,
    pool: &sqlx::PgPool,
    shutdown_token: &CancellationToken,
) -> Result<(), sqlx::Error> {
    run_event_listener(
        pool,
        shutdown_token,
        "data",
        || run_startup_catch_up(state, pool),
        |notification| async move {
            let event_id = notification.event_id();
            match notification.event_type() {
                EventType::EquityBarsSync(Outcome::Requested) => {
                    info!(event_id, "Received equity_bars_sync_requested");
                    handle_equity_bars_sync(state, pool, event_id).await;
                }
                EventType::DatabaseExport(Outcome::Requested) => {
                    info!(event_id, "Received database_export_requested");
                    handle_database_export(state, pool, event_id, notification.payload()).await;
                }
                EventType::DatabaseBackup(Outcome::Requested) => {
                    info!(event_id, "Received database_backup_requested");
                    handle_database_backup(state, pool, event_id).await;
                }
                EventType::DatabasePurge(Outcome::Requested) => {
                    info!(event_id, "Received database_purge_requested");
                    handle_database_purge(pool, event_id).await;
                }
                EventType::MarketCalendarSync(Outcome::Requested) => {
                    info!(event_id, "Received market_calendar_sync_requested");
                    handle_market_calendar_sync(state, pool, event_id).await;
                }
                EventType::SchedulerHealthCheck(Outcome::Requested) => {
                    info!(event_id, "Received scheduler_health_check_requested");
                    handle_scheduler_health_check(pool, event_id).await;
                }
                EventType::ModelArtifactCheck(Outcome::Requested) => {
                    info!(event_id, "Received model_artifact_check_requested");
                    handle_model_artifact_check(state, pool, event_id).await;
                }
                // Every consumer receives every event; the rest belong to other
                // services or are audit records. Listed rather than caught by a
                // wildcard so that adding a family, or an `Outcome` to one of
                // the families this consumer handles, fails the build here
                // instead of being silently ignored.
                EventType::EquityBarsSync(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::DatabaseExport(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::DatabaseBackup(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::DatabasePurge(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::MarketCalendarSync(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::SchedulerHealthCheck(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::ModelArtifactCheck(
                    Outcome::Started | Outcome::Completed | Outcome::Errored,
                )
                | EventType::ModelArtifactPublished
                | EventType::ModelArtifactStale
                | EventType::EquityPredictions(_)
                | EventType::PortfolioRebalance(_)
                | EventType::PortfolioLiquidation(_)
                | EventType::TradingSessionStarted
                | EventType::PortfolioEvaluationRequested
                | EventType::StressTest => {}
            }
        },
    )
    .await
}

/// Validates the schedule and replays the events this consumer missed.
///
/// Runs on every connection, not once per process: a reconnect means delivery
/// had a gap, and the same replay that covers a restart covers that gap too.
async fn run_startup_catch_up(state: &State, pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    // Validate pg_cron jobs and event freshness on startup. Event freshness
    // is only meaningful when pg_cron is installed (otherwise no cron-triggered
    // events exist and every check would produce a false warning).
    // Publish the persisted calendar first: everything below asks trading-day
    // questions, and answering them from the hardcoded fallback when a synced
    // calendar exists in the database would be a needless downgrade.
    publish_persisted_calendar(pool).await;

    let calendar_offset = get_consumer_offset(pool, CONSUMER_DATA_MARKET_CALENDAR).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::MarketCalendarSync(Outcome::Requested),
        calendar_offset,
    )
    .await?
    {
        info!(
            event_id,
            "Catching up on missed market_calendar_sync_requested"
        );
        handle_market_calendar_sync(state, pool, event_id).await;
    }

    let report = validate_cron_jobs(pool).await;
    if report.pg_cron_available {
        check_event_freshness(pool).await;
    }

    // Catch up on any missed one-time actionable events (latest missed instance each).
    let sync_offset = get_consumer_offset(pool, CONSUMER_DATA_EQUITY_BARS_SYNC).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::EquityBarsSync(Outcome::Requested),
        sync_offset,
    )
    .await?
    {
        info!(event_id, "Catching up on missed equity_bars_sync_requested");
        handle_equity_bars_sync(state, pool, event_id).await;
    }

    // Export events carry date-specific payloads, so every missed event must be
    // replayed in order. Skipping to the latest would permanently lose export dates
    // for any intermediate days the service was down.
    let export_offset = get_consumer_offset(pool, CONSUMER_DATA_DATABASE_EXPORT).await?;
    for (event_id, payload) in events_after(
        pool,
        EventType::DatabaseExport(Outcome::Requested),
        export_offset,
    )
    .await?
    {
        info!(event_id, "Catching up on missed database_export_requested");
        handle_database_export(state, pool, event_id, &payload).await;
    }

    let backup_offset = get_consumer_offset(pool, CONSUMER_DATA_DATABASE_BACKUP).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::DatabaseBackup(Outcome::Requested),
        backup_offset,
    )
    .await?
    {
        info!(event_id, "Catching up on missed database_backup_requested");
        handle_database_backup(state, pool, event_id).await;
    }

    let health_check_offset = get_consumer_offset(pool, CONSUMER_DATA_SCHEDULER_HEALTH).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::SchedulerHealthCheck(Outcome::Requested),
        health_check_offset,
    )
    .await?
    {
        info!(
            event_id,
            "Catching up on missed scheduler_health_check_requested"
        );
        handle_scheduler_health_check(pool, event_id).await;
    }

    let artifact_offset = get_consumer_offset(pool, CONSUMER_DATA_MODEL_ARTIFACT).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::ModelArtifactCheck(Outcome::Requested),
        artifact_offset,
    )
    .await?
    {
        info!(
            event_id,
            "Catching up on missed model_artifact_check_requested"
        );
        handle_model_artifact_check(state, pool, event_id).await;
    }

    let purge_offset = get_consumer_offset(pool, CONSUMER_DATA_DATABASE_PURGE).await?;
    if let Some(event_id) = latest_event_after(
        pool,
        EventType::DatabasePurge(Outcome::Requested),
        purge_offset,
    )
    .await?
    {
        info!(event_id, "Catching up on missed database_purge_requested");
        handle_database_purge(pool, event_id).await;
    }

    Ok(())
}

/// Loads the persisted calendar and publishes it for the process.
///
/// Best-effort: a database that has never synced, or a failed read, leaves the
/// hardcoded holiday fallback in place rather than blocking startup. That
/// fallback is why every calendar lookup can stay synchronous and infallible.
pub async fn publish_persisted_calendar(pool: &sqlx::PgPool) {
    match market_calendar_sync::load_persisted_calendar(pool).await {
        Ok(calendar) if calendar.is_empty() => {
            info!(
                "No persisted market calendar; using the static holiday fallback until first sync"
            )
        }
        Ok(calendar) => {
            let sessions = calendar.len();
            let horizon = calendar.horizon();
            market_calendar::install(calendar);
            match horizon {
                Some((start, end)) => info!(
                    sessions,
                    %start,
                    %end,
                    "Published the persisted market calendar"
                ),
                None => info!(sessions, "Published the persisted market calendar"),
            }
        }
        Err(error) => {
            warn!(error = %error, "Could not load the persisted market calendar; using the static holiday fallback")
        }
    }
}

/// Refreshes the published trading calendar from Alpaca.
///
/// The calendar is the only source that knows about half-days, so a sync that
/// fails leaves the system treating every early close as a full session — worth
/// an event rather than a log line.
async fn handle_market_calendar_sync(state: &State, pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::MarketCalendarSync(Outcome::Started),
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit market_calendar_sync_started");
    }

    let (outcome, payload) = match market_calendar_sync::run_market_calendar_sync(state, pool).await
    {
        Ok(session_count) => (
            Outcome::Completed,
            serde_json::json!({"sessions": session_count}),
        ),
        Err(error) => {
            error!(error = %error, "Market calendar sync errored");
            (
                Outcome::Errored,
                serde_json::json!({"error": error.to_string()}),
            )
        }
    };

    if let Err(error) = emit_event(pool, EventType::MarketCalendarSync(outcome), &payload).await {
        warn!(error = %error, "Failed to emit the market calendar sync outcome");
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_DATA_MARKET_CALENDAR, event_id).await
    {
        warn!(error = %error, "Failed to update market-calendar consumer offset");
    }
}

/// Re-runs the startup health checks on a schedule and reports the result.
///
/// The startup run answers "did we miss anything while we were down"; this
/// answers "has anything stopped while we were up". A process under a tmux
/// restart loop can stay up for weeks, so without a scheduled run a cron job
/// that silently stopped firing produced no warning from anywhere.
///
/// A failure emits `scheduler_health_check_errored` rather than only logging, so
/// a missed cron reaches the dashboard feed and the fund report alongside
/// everything else.
async fn handle_scheduler_health_check(pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::SchedulerHealthCheck(Outcome::Started),
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit scheduler_health_check_started");
    }

    let cron_report = validate_cron_jobs(pool).await;
    let stale_events = if cron_report.pg_cron_available {
        check_event_freshness(pool).await
    } else {
        Vec::new()
    };

    let outcome = if cron_report.is_healthy() && stale_events.is_empty() {
        Outcome::Completed
    } else {
        Outcome::Errored
    };
    let payload = serde_json::json!({
        "missing_jobs": cron_report.missing,
        "unexpected_jobs": cron_report.unexpected,
        "stale_events": stale_events
            .iter()
            .map(|event_type| event_type.as_str())
            .collect::<Vec<&str>>(),
    });

    match outcome {
        Outcome::Completed => info!("Scheduler health check found no problems"),
        _ => error!(
            missing_jobs = cron_report.missing.len(),
            unexpected_jobs = cron_report.unexpected.len(),
            stale_events = stale_events.len(),
            "Scheduler health check found problems"
        ),
    }

    if let Err(error) = emit_event(pool, EventType::SchedulerHealthCheck(outcome), &payload).await {
        warn!(error = %error, "Failed to emit the scheduler health check outcome");
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_DATA_SCHEDULER_HEALTH, event_id).await
    {
        warn!(error = %error, "Failed to update scheduler-health consumer offset");
    }
}

/// Checks whether the trainer published a fresh artifact, and says so either way.
///
/// The application cannot enforce that training happened before the day's
/// predictions — the trainer is on another VM with no database connection — so
/// this observes instead. A new key becomes `model_artifact_published`, which
/// inference consumes to load the model; an artifact that has not moved for two
/// trading days becomes `model_artifact_stale`, which previously was silence.
async fn handle_model_artifact_check(state: &State, pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::ModelArtifactCheck(Outcome::Started),
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit model_artifact_check_started");
    }

    let outcome = match run_model_artifact_check(state, pool).await {
        Ok(status) => {
            model_artifact::report(&status);
            match status {
                model_artifact::ArtifactStatus::Published {
                    artifact_key,
                    trained_at,
                } => {
                    if let Err(error) = emit_event(
                        pool,
                        EventType::ModelArtifactPublished,
                        &serde_json::json!({
                            "artifact_key": artifact_key,
                            "trained_at": trained_at.map(|instant| instant.to_rfc3339()),
                        }),
                    )
                    .await
                    {
                        warn!(error = %error, "Failed to emit model_artifact_published");
                    }
                }
                model_artifact::ArtifactStatus::Stale {
                    artifact_key,
                    trading_days_old,
                } => {
                    if let Err(error) = emit_event(
                        pool,
                        EventType::ModelArtifactStale,
                        &serde_json::json!({
                            "artifact_key": artifact_key,
                            "trading_days_old": trading_days_old,
                        }),
                    )
                    .await
                    {
                        warn!(error = %error, "Failed to emit model_artifact_stale");
                    }
                }
                model_artifact::ArtifactStatus::Unchanged { .. } => {}
            }
            Outcome::Completed
        }
        Err(error) => {
            error!(error = %error, "Model artifact check errored");
            Outcome::Errored
        }
    };

    if let Err(error) = emit_event(
        pool,
        EventType::ModelArtifactCheck(outcome),
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit the model artifact check outcome");
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_DATA_MODEL_ARTIFACT, event_id).await {
        warn!(error = %error, "Failed to update model-artifact consumer offset");
    }
}

/// Resolves the newest artifact and compares it against the recorded lineage.
async fn run_model_artifact_check(
    state: &State,
    pool: &sqlx::PgPool,
) -> Result<model_artifact::ArtifactStatus, String> {
    let latest_key =
        model_artifact::resolve_latest_artifact_key(&state.s3_client, &state.bucket_name)
            .await
            .map_err(|error| format!("Could not resolve the latest artifact: {error}"))?;

    let recorded_key = model_artifact::latest_recorded_artifact_key(pool)
        .await
        .map_err(|error| format!("Could not read the recorded artifact key: {error}"))?;

    Ok(model_artifact::classify(
        latest_key,
        recorded_key.as_deref(),
        Utc::now(),
    ))
}

async fn handle_equity_bars_sync(state: &State, pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::EquityBarsSync(Outcome::Started),
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
                EventType::EquityBarsSync(Outcome::Completed),
                &serde_json::json!({"bar_count": bar_count}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit equity_bars_sync_completed");
            }
        }
        Ok(None) => {
            info!("No equity bar data available for sync");
            if let Err(error) = emit_event(
                pool,
                EventType::EquityBarsSync(Outcome::Completed),
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
                EventType::EquityBarsSync(Outcome::Errored),
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

    // Update the data lake manifest so downstream consumers can discover datasets.
    crate::data::manifest::write_manifest(&state.s3_client, &state.bucket_name).await;

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

async fn handle_database_export(
    state: &State,
    pool: &sqlx::PgPool,
    event_id: i64,
    payload: &serde_json::Value,
) {
    let export_date = export_date_from_payload(payload);
    if let Err(error) = emit_event(
        pool,
        EventType::DatabaseExport(Outcome::Started),
        &serde_json::json!({"date": export_date.to_string()}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit database_export_started");
    }

    match export::export_database(state, export_date).await {
        Ok(count) => {
            info!(rows = count, "Database export completed");
            if let Err(error) = emit_event(
                pool,
                EventType::DatabaseExport(Outcome::Completed),
                &serde_json::json!({"count": count, "date": export_date.to_string()}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit database_export_completed");
            }

            // Chain: export success → backup
            if let Err(error) = emit_event(
                pool,
                EventType::DatabaseBackup(Outcome::Requested),
                &serde_json::json!({}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit database_backup_requested");
            }
        }
        Err(ref error) => {
            error!(error = %error, "Database export errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::DatabaseExport(Outcome::Errored),
                &serde_json::json!({"error": error, "date": export_date.to_string()}),
            )
            .await
            {
                warn!(error = %emit_error, "Failed to emit database_export_errored");
            }
        }
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_DATA_DATABASE_EXPORT, event_id).await
    {
        warn!(error = %error, "Failed to update database-export consumer offset");
    }
}

async fn handle_database_backup(state: &State, pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::DatabaseBackup(Outcome::Started),
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
                EventType::DatabaseBackup(Outcome::Completed),
                &serde_json::json!({"byte_count": byte_count}),
            )
            .await
            {
                warn!(error = %error, "Failed to emit database_backup_completed");
            }
            // Chain: backup success → purge
            if let Err(chain_error) = emit_event(
                pool,
                EventType::DatabasePurge(Outcome::Requested),
                &serde_json::json!({}),
            )
            .await
            {
                warn!(error = %chain_error, "Failed to emit database_purge_requested");
            }
        }
        Err(ref error) => {
            error!(error = %error, "Database backup errored");
            if let Err(emit_error) = emit_event(
                pool,
                EventType::DatabaseBackup(Outcome::Errored),
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

/// Selects the terminal event for a purge run and builds its payload.
///
/// A table that fails to purge is skipped rather than aborting the run, so the
/// outcome is partial rather than fatal. Reporting it as completed would let a
/// table fail every night with the event log claiming success each time, which is
/// exactly the failure the nightly chain exists to make visible.
fn purge_outcome_event(
    summary: &crate::data::database::PurgeSummary,
    total_rows_deleted: u64,
) -> (EventType, serde_json::Value) {
    if summary.failed_tables.is_empty() {
        (
            EventType::DatabasePurge(Outcome::Completed),
            serde_json::json!({ "total_rows_deleted": total_rows_deleted }),
        )
    } else {
        (
            EventType::DatabasePurge(Outcome::Errored),
            serde_json::json!({
                "total_rows_deleted": total_rows_deleted,
                "failed_tables": summary.failed_tables,
            }),
        )
    }
}

async fn handle_database_purge(pool: &sqlx::PgPool, event_id: i64) {
    if let Err(error) = emit_event(
        pool,
        EventType::DatabasePurge(Outcome::Started),
        &serde_json::json!({}),
    )
    .await
    {
        warn!(error = %error, "Failed to emit database_purge_started");
    }

    let summary = crate::data::database::purge_ephemeral_tables(pool).await;
    let total: u64 = summary.rows_deleted.iter().map(|(_, count)| count).sum();
    for (table, count) in &summary.rows_deleted {
        if *count > 0 {
            info!(table, rows = count, "Purged old rows");
        }
    }

    let (event_type, payload) = purge_outcome_event(&summary, total);
    if summary.failed_tables.is_empty() {
        info!(total_rows = total, "Database purge completed");
    } else {
        error!(
            total_rows = total,
            failed_tables = ?summary.failed_tables,
            "Database purge completed with failures"
        );
    }

    if let Err(error) = emit_event(pool, event_type, &payload).await {
        warn!(error = %error, event_type = event_type.as_str(), "Failed to emit purge outcome");
    }

    if let Err(error) = update_consumer_offset(pool, CONSUMER_DATA_DATABASE_PURGE, event_id).await {
        warn!(error = %error, "Failed to update database-purge consumer offset");
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
        compare_cron_jobs, detect_coverage_gaps, export_date_from_payload, is_event_stale,
        listen_loop, most_recent_trading_day, most_recent_trading_day_start, parse_postgres_url,
        prior_trading_day, purge_outcome_event, spawn_sync_scheduler, sync_date_for, EventType,
        Outcome, EXPECTED_CRON_JOBS, MONITORED_EVENTS,
    };
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use chrono_tz::US::Eastern;
    use tokio_util::sync::CancellationToken;

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
        // DatabaseState::NotConfigured, so listen_loop returns immediately without
        // a pool. The scheduler spawns exactly one task regardless.
        let token = CancellationToken::new();
        let handles = spawn_sync_scheduler(state, token.clone());
        assert_eq!(handles.len(), 1, "Expected a single listen_loop task");
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
    fn test_expected_cron_jobs_list_is_complete() {
        assert_eq!(EXPECTED_CRON_JOBS.len(), 9);
        assert!(EXPECTED_CRON_JOBS.contains(&"equity-bars-sync-requested"));
        assert!(EXPECTED_CRON_JOBS.contains(&"equity-predictions-requested"));
        assert!(EXPECTED_CRON_JOBS.contains(&"trading-session-started"));
        assert!(EXPECTED_CRON_JOBS.contains(&"portfolio-liquidation-requested"));
        assert!(EXPECTED_CRON_JOBS.contains(&"database-export-requested"));
        assert!(EXPECTED_CRON_JOBS.contains(&"market-calendar-sync-requested"));
        assert!(EXPECTED_CRON_JOBS.contains(&"scheduler-health-check-requested"));
        assert!(EXPECTED_CRON_JOBS.contains(&"model-artifact-check-requested"));
        assert!(EXPECTED_CRON_JOBS.contains(&"cron-run-details-cleanup"));
    }

    #[test]
    fn test_expected_cron_jobs_excludes_retired_evaluation_job() {
        // schema.sql does not schedule portfolio-evaluation-requested; the live-quote
        // evaluator emits portfolio_evaluation_requested instead. Expecting the job
        // made validate_cron_jobs log a missing-job error on every data-service
        // startup.
        assert!(!EXPECTED_CRON_JOBS.contains(&"portfolio-evaluation-requested"));
    }

    #[test]
    fn test_purge_outcome_event_reports_completed_when_no_table_failed() {
        let summary = crate::data::database::PurgeSummary {
            rows_deleted: vec![("events".to_string(), 42)],
            failed_tables: Vec::new(),
        };
        let (event_type, payload) = purge_outcome_event(&summary, 42);
        assert_eq!(
            event_type,
            crate::common::events::EventType::DatabasePurge(
                crate::common::events::Outcome::Completed
            )
        );
        assert_eq!(payload["total_rows_deleted"], 42);
        assert!(payload.get("failed_tables").is_none());
    }

    #[test]
    fn test_purge_outcome_event_reports_errored_when_a_table_failed() {
        // A partial purge must not be announced as a completed one: the failing
        // table would otherwise be invisible in the events table and the dashboard,
        // and could fail nightly without anything noticing.
        let summary = crate::data::database::PurgeSummary {
            rows_deleted: vec![("events".to_string(), 42)],
            failed_tables: vec!["equity_pairs".to_string()],
        };
        let (event_type, payload) = purge_outcome_event(&summary, 42);
        assert_eq!(
            event_type,
            crate::common::events::EventType::DatabasePurge(
                crate::common::events::Outcome::Errored
            )
        );
        assert_eq!(payload["total_rows_deleted"], 42);
        assert_eq!(payload["failed_tables"][0], "equity_pairs");
    }

    #[test]
    fn test_purge_outcome_event_carries_every_failed_table() {
        let summary = crate::data::database::PurgeSummary {
            rows_deleted: Vec::new(),
            failed_tables: vec![
                "equity_orders".to_string(),
                "events".to_string(),
                "model_runs".to_string(),
            ],
        };
        let (event_type, payload) = purge_outcome_event(&summary, 0);
        assert_eq!(
            event_type,
            crate::common::events::EventType::DatabasePurge(
                crate::common::events::Outcome::Errored
            )
        );
        assert_eq!(payload["failed_tables"].as_array().unwrap().len(), 3);
        assert_eq!(payload["total_rows_deleted"], 0);
    }

    #[test]
    fn test_event_emitting_cron_jobs_are_named_for_their_event() {
        // Every job that emits an event is named for it, hyphenated. The one
        // exception is cron-run-details-cleanup, which runs a DELETE directly and
        // emits nothing, so an event-shaped name would point at an event that does
        // not exist. This caught equity-predictions-request, which emitted
        // equity_predictions_requested but was missing the suffix.
        for job_name in EXPECTED_CRON_JOBS {
            if *job_name == "cron-run-details-cleanup" {
                continue;
            }
            let event_type = job_name.replace('-', "_");
            assert!(
                crate::common::events::EventType::parse(&event_type).is_some(),
                "cron job '{}' implies event type '{}', which is not an EventType variant",
                job_name,
                event_type
            );
        }
    }

    #[test]
    fn test_cleanup_job_is_not_named_for_an_event() {
        // Guards the exception above: if this job ever gains an event-shaped name,
        // the convention test would silently start covering it.
        assert!(EXPECTED_CRON_JOBS.contains(&"cron-run-details-cleanup"));
        assert!(crate::common::events::EventType::parse("cron_run_details_cleanup").is_none());
    }

    #[test]
    fn test_expected_cron_jobs_covers_session_start() {
        // trading-session-started is scheduled in schema.sql and is the only emitter
        // of the event that opens the trading day. Its absence from this list meant a
        // disappearance would go unreported.
        assert!(EXPECTED_CRON_JOBS.contains(&"trading-session-started"));
    }

    #[test]
    fn test_monitored_events_cover_every_daily_job() {
        // The session events were previously excluded because a fixed hour count
        // false-positived on them outside trading windows. A trading-day
        // question does not, so the event that begins each trading day is
        // monitored at last.
        assert!(MONITORED_EVENTS.contains(&EventType::EquityBarsSync(Outcome::Requested)));
        assert!(MONITORED_EVENTS.contains(&EventType::DatabaseExport(Outcome::Requested)));
        assert!(MONITORED_EVENTS.contains(&EventType::TradingSessionStarted));
        assert!(MONITORED_EVENTS.contains(&EventType::EquityPredictions(Outcome::Requested)));
        assert!(MONITORED_EVENTS.contains(&EventType::PortfolioLiquidation(Outcome::Requested)));
    }

    /// 2026-07-31 is a Friday; 2026-08-03 is the following Monday.
    fn eastern(date: (i32, u32, u32), time: (u32, u32)) -> DateTime<Utc> {
        Eastern
            .with_ymd_and_hms(date.0, date.1, date.2, time.0, time.1, 0)
            .single()
            .expect("fixture instant is unambiguous")
            .with_timezone(&Utc)
    }

    #[test]
    fn test_is_event_stale_never_seen() {
        let trading_day_start = most_recent_trading_day_start(Utc::now());
        assert!(is_event_stale(None, trading_day_start));
    }

    #[test]
    fn test_event_that_fired_today_is_fresh() {
        let now = eastern((2026, 7, 31), (10, 0));
        let fired = eastern((2026, 7, 31), (5, 0));
        assert!(!is_event_stale(
            Some(fired),
            most_recent_trading_day_start(now)
        ));
    }

    #[test]
    fn test_event_that_last_fired_the_previous_trading_day_is_stale() {
        // Friday morning, last seen Thursday: a day was missed.
        let now = eastern((2026, 7, 31), (10, 0));
        let fired = eastern((2026, 7, 30), (5, 0));
        assert!(is_event_stale(
            Some(fired),
            most_recent_trading_day_start(now)
        ));
    }

    /// The false positive that ran every week under the old fixed hour count.
    #[test]
    fn test_a_weekend_does_not_make_a_friday_event_stale() {
        let fired = eastern((2026, 7, 31), (5, 0));

        for now in [
            eastern((2026, 8, 1), (10, 0)), // Saturday
            eastern((2026, 8, 2), (10, 0)), // Sunday
        ] {
            assert!(
                !is_event_stale(Some(fired), most_recent_trading_day_start(now)),
                "a Friday event must stay fresh through the weekend"
            );
        }

        // Roughly 77 hours old by Monday morning, which tripped the old
        // 26-hour threshold every week. It is Monday's own firing that is now
        // expected, so Friday's no longer counts.
        let monday = eastern((2026, 8, 3), (10, 0));
        assert!(is_event_stale(
            Some(fired),
            most_recent_trading_day_start(monday)
        ));
    }

    #[test]
    fn test_most_recent_trading_day_rolls_back_over_a_weekend() {
        assert_eq!(
            most_recent_trading_day(eastern((2026, 8, 2), (10, 0))),
            NaiveDate::from_ymd_opt(2026, 7, 31).unwrap()
        );
        assert_eq!(
            most_recent_trading_day(eastern((2026, 7, 31), (10, 0))),
            NaiveDate::from_ymd_opt(2026, 7, 31).unwrap()
        );
    }

    #[test]
    fn test_compare_cron_jobs_reports_both_directions() {
        let registered: Vec<String> = EXPECTED_CRON_JOBS
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        let report = compare_cron_jobs(&registered);
        assert!(report.is_healthy());
        assert!(report.pg_cron_available);
    }

    #[test]
    fn test_compare_cron_jobs_reports_a_missing_job() {
        let registered: Vec<String> = EXPECTED_CRON_JOBS
            .iter()
            .skip(1)
            .map(|name| (*name).to_string())
            .collect();
        let report = compare_cron_jobs(&registered);
        assert_eq!(report.missing, vec![EXPECTED_CRON_JOBS[0].to_string()]);
        assert!(report.unexpected.is_empty());
        assert!(!report.is_healthy());
    }

    /// The direction that did not exist, and that three real orphans needed.
    ///
    /// `schema.sql` is additive and idempotent, so it cannot remove a job it no
    /// longer creates. Without this check a job scheduled by an older schema
    /// version survives forever and nothing notices — including one that emitted
    /// a live control event and would have doubled the nightly backup.
    #[test]
    fn test_compare_cron_jobs_reports_an_unexpected_job() {
        let mut registered: Vec<String> = EXPECTED_CRON_JOBS
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        registered.push("database-backup-requested".to_string());

        let report = compare_cron_jobs(&registered);
        assert!(report.missing.is_empty());
        assert_eq!(
            report.unexpected,
            vec!["database-backup-requested".to_string()]
        );
        assert!(!report.is_healthy());
    }

    /// An event fired at the very start of the trading day is fresh.
    ///
    /// The comparison is `timestamp < trading_day_start`, so the boundary
    /// instant itself counts as having fired within the day.
    #[test]
    fn test_event_at_the_trading_day_boundary_is_fresh() {
        let now = eastern((2026, 7, 31), (10, 0));
        let start = most_recent_trading_day_start(now);
        assert!(!is_event_stale(Some(start), start));
    }
}
