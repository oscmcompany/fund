//! One function per [`Command`], and the state they share.
//!
//! This is the only place that knows how a command name turns into work. The listener parses a
//! notification and calls [`handle`]; everything else is a module doing one thing.
//!
//! Every handler returns a JSON summary, which becomes the `_completed` payload. That is what
//! makes the nightly export of `events` worth reading — a row carrying the pairs opened and closed,
//! rows synced, and the model run used is the record of the trading day.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::common::alpaca::{AlpacaCredentials, ClientError, MarketDataClient, TradingClient};
use crate::common::events::{self, Command, EventError};
use crate::common::massive::MassiveClient;
use crate::common::types::BarInterval;
use crate::data::bars::{self, CloseHistoryCache, HISTORY_LOOKBACK_DAYS};
use crate::data::calendar::{CalendarCache, SessionDate, TradingCalendar};
use crate::data::details;
use crate::data::export;
use crate::data::purge;
use crate::data::universe::{UniverseCache, UniverseError};
use crate::models::tide::{artifact, predict};
use crate::portfolio::account;
use crate::portfolio::evaluate::{self, EvaluationContext};
use crate::portfolio::execute::ExecutionSettings;
use crate::portfolio::screen::CORRELATION_WINDOW_SESSIONS;
use crate::portfolio::size::SizingParameters;
use tokio_util::sync::CancellationToken;

/// Sessions of history the post-close bar sync re-fetches.
///
/// Three rather than one. A sync that missed a day for any reason silently leaves a hole in the
/// correlation window, and re-fetching two extra sessions costs one request while closing that
/// class of gap entirely — the upsert makes the overlap free.
const BAR_SYNC_LOOKBACK_SESSIONS: usize = 3;

/// Anything that stops a handler finishing.
#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("Alpaca is unreachable: {0}")]
    Alpaca(#[from] ClientError),
    #[error("database access failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("event bus write failed: {0}")]
    Events(#[from] EventError),
    #[error("evaluation pass failed: {0}")]
    Evaluation(#[from] evaluate::EvaluationError),
    #[error("account sync failed: {0}")]
    Account(#[from] account::AccountError),
    #[error("bar sync failed: {0}")]
    Bars(#[from] bars::BarsError),
    #[error("detail sync failed: {0}")]
    Details(#[from] details::DetailsError),
    #[error("model artifact could not be resolved or loaded: {0}")]
    Artifact(#[from] artifact::ArtifactError),
    #[error("tradable universe could not be built: {0}")]
    Universe(#[from] UniverseError),
    #[error("prediction pipeline failed at {stage}: {message}")]
    Prediction {
        stage: &'static str,
        message: String,
    },
    #[error("configuration is missing or unusable: {0}")]
    Configuration(String),
}

/// Everything the service holds for the life of the process.
///
/// The three caches are values held here and passed explicitly, not process-wide statics. That is
/// what makes them testable — the singleton version of the calendar is the specific problem
/// recorded in `rust_test_pitfalls`.
pub struct ServiceState {
    pool: PgPool,
    trading: TradingClient,
    /// Alpaca, for intraday snapshots. The book is priced against the venue it trades on.
    market_data: MarketDataClient,
    /// Massive, for daily bars. See [`crate::common::massive`] for why the two are split.
    massive: MassiveClient,
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    artifact_prefix: String,
    model_version: String,
    calendar_cache: CalendarCache,
    universe_cache: UniverseCache,
    close_history_cache: CloseHistoryCache,
    sizing: SizingParameters,
    execution: ExecutionSettings,
    /// Cancelled when the process is asked to stop.
    ///
    /// Held here so a handler can decline to *start* more work it would not be able to finish. The
    /// drain in `bin/fund.rs` bounds how long shutdown waits; this is what keeps the thing being
    /// waited on inside that bound.
    shutdown: CancellationToken,
    /// A standard-library mutex, not a `tokio` one. The critical section is a single set
    /// insertion or removal and is never held across an await, and the release happens in `Drop` —
    /// which is synchronous and so cannot await an async lock at all.
    in_flight: std::sync::Mutex<HashSet<Command>>,
}

impl ServiceState {
    /// Builds the state from the environment.
    pub async fn from_env(pool: PgPool, shutdown: CancellationToken) -> Result<Self, HandlerError> {
        let credentials = AlpacaCredentials::from_env()
            .map_err(|error| HandlerError::Configuration(error.to_string()))?;
        let massive = MassiveClient::from_env()
            .map_err(|error| HandlerError::Configuration(error.to_string()))?;

        let bucket = std::env::var("AWS_S3_BUCKET_NAME").map_err(|_| {
            HandlerError::Configuration("AWS_S3_BUCKET_NAME is not set".to_string())
        })?;
        let artifact_prefix = std::env::var("AWS_S3_MODEL_ARTIFACT_PATH")
            .unwrap_or_else(|_| "models/tide/".to_string());
        let model_version = std::env::var("MODEL_VERSION").unwrap_or_else(|_| "latest".to_string());

        Ok(Self {
            pool,
            trading: TradingClient::from_env(credentials.clone()),
            market_data: MarketDataClient::from_env(credentials),
            massive,
            s3_client: crate::common::aws::s3_client().await,
            bucket,
            artifact_prefix,
            model_version,
            calendar_cache: CalendarCache::new(),
            universe_cache: UniverseCache::new(),
            close_history_cache: CloseHistoryCache::new(),
            sizing: SizingParameters::from_env(),
            execution: ExecutionSettings::default(),
            shutdown,
            in_flight: std::sync::Mutex::new(HashSet::new()),
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Claims a command, or reports that one is already running.
    ///
    /// An in-process flag rather than a query against the `events` table. There is one process, so
    /// a flag cannot race, and a query would be a round trip to learn something this process
    /// already knows. The guard releases on drop, including on a panic or an early return.
    fn claim(&self, command: Command) -> Option<CommandGuard<'_>> {
        let claimed = self
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(command);
        claimed.then_some(CommandGuard {
            state: self,
            command,
        })
    }
}

/// Releases an in-flight claim when it goes out of scope.
struct CommandGuard<'a> {
    state: &'a ServiceState,
    command: Command,
}

impl Drop for CommandGuard<'_> {
    fn drop(&mut self) {
        self.state
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.command);
    }
}

/// Dispatches one command, emitting its terminal event either way.
///
/// A command already in flight is dropped with a log line and no event: the request it came from
/// will be answered by the run that is already going, and emitting a second terminal outcome would
/// make the recovery scan's "requested with no terminal outcome" test wrong.
pub async fn handle(state: &ServiceState, command: Command) {
    let Some(_guard) = state.claim(command) else {
        warn!(
            command = command.as_str(),
            "Command already in flight; this request is dropped"
        );
        return;
    };

    let started = std::time::Instant::now();
    let result = dispatch(state, command).await;
    let duration_milliseconds = started.elapsed().as_millis() as u64;

    match result {
        Ok(mut summary) => {
            if let Value::Object(map) = &mut summary {
                map.insert("duration_ms".to_string(), json!(duration_milliseconds));
            }
            info!(
                command = command.as_str(),
                duration_milliseconds, "Command completed"
            );
            if let Err(error) = events::emit_completed(state.pool(), command, summary).await {
                error!(command = command.as_str(), %error, "Failed to record completion");
            }
        }
        Err(error) => {
            error!(
                command = command.as_str(),
                %error,
                duration_milliseconds,
                "Command failed"
            );
            if let Err(emit_error) =
                events::emit_errored(state.pool(), command, &error.to_string()).await
            {
                error!(command = command.as_str(), %emit_error, "Failed to record failure");
            }
        }
    }
}

async fn dispatch(state: &ServiceState, command: Command) -> Result<Value, HandlerError> {
    match command {
        Command::Predictions => handle_predictions(state).await,
        Command::PortfolioEvaluation => handle_portfolio_evaluation(state).await,
        Command::PortfolioLiquidation => handle_portfolio_liquidation(state).await,
        Command::AccountSync => handle_account_sync(state).await,
        Command::MarketDataSync => handle_market_data_sync(state).await,
        Command::DatabaseExport => handle_database_export(state).await,
    }
}

/// Pre-open: warm the caches, resolve the newest artifact, run inference, write predictions.
///
/// The previous session's post-close commands are checked here, and the artifact resolved here,
/// rather than on schedules of their own — both only matter immediately before a session.
async fn handle_predictions(state: &ServiceState) -> Result<Value, HandlerError> {
    let now = Utc::now();
    let today = SessionDate::at(now);

    let calendar = state.calendar_cache.get(&state.trading, now).await?;
    if !calendar.is_trading_day(today) {
        info!(%today, "Not a trading day; predictions skipped");
        return Ok(json!({ "skipped": "not_a_trading_day", "session_date": today }));
    }

    let universe = state
        .universe_cache
        .get(&state.trading, &state.pool, now)
        .await?;
    let close_history = state
        .close_history_cache
        .get(
            &state.pool,
            BarInterval::OneDay,
            CORRELATION_WINDOW_SESSIONS,
            now,
        )
        .await?;

    let unfinished = previous_session_gaps(state, &calendar, today).await?;
    if !unfinished.is_empty() {
        warn!(
            commands = ?unfinished,
            "Previous session left post-close commands unfinished"
        );
    }

    let artifact_key = artifact::resolve_artifact_key(
        &state.s3_client,
        &state.bucket,
        &state.artifact_prefix,
        &state.model_version,
        None,
    )
    .await?;
    let model_state =
        artifact::download_and_load_model(&state.s3_client, &state.bucket, &artifact_key, None)
            .await?;

    let artifact_staleness_sessions =
        artifact_staleness_sessions(model_state.run_id(), &calendar, today);
    if artifact_staleness_sessions.is_some_and(|sessions| sessions >= 1) {
        // Not an error and not an event of its own. Running on an older model is a normal outcome
        // of two machines that share no database, and the staleness is recorded here so a session
        // that was decided by one says so in its own completion row.
        warn!(
            run_id = model_state.run_id(),
            artifact_staleness_sessions,
            "Predictions are running on a model that missed at least one session"
        );
    }

    let correlation_id = Uuid::new_v4();
    let (predictions, rows) = run_inference(state, &model_state, correlation_id).await?;

    Ok(json!({
        "session_date": today,
        "correlation_id": correlation_id,
        "model_run_id": model_state.run_id(),
        "artifact_key": artifact_key,
        "artifact_staleness_sessions": artifact_staleness_sessions,
        "predictions": predictions,
        "rows_written": rows,
        "universe": universe.len(),
        "close_history_tickers": close_history.len(),
        "previous_session_unfinished": unfinished,
    }))
}

/// Runs the inference pipeline and persists the result.
async fn run_inference(
    state: &ServiceState,
    model_state: &artifact::ModelState,
    correlation_id: Uuid,
) -> Result<(usize, u64), HandlerError> {
    fn at(stage: &'static str) -> impl Fn(String) -> HandlerError {
        move |message| HandlerError::Prediction { stage, message }
    }

    let equity_bars =
        bars::load_bars_dataframe(&state.pool, BarInterval::OneDay, HISTORY_LOOKBACK_DAYS)
            .await
            .map_err(|error| at("load_bars")(error.to_string()))?;
    let equity_details = details::load_details_dataframe(&state.pool)
        .await
        .map_err(|error| at("load_details")(error.to_string()))?;

    let consolidated = predict::consolidate_data(equity_bars, equity_details)
        .map_err(|error| at("consolidate")(error.to_string()))?;
    let filtered = predict::filter_equity_bars(
        consolidated,
        crate::common::types::MINIMUM_CLOSE_PRICE,
        crate::common::types::MINIMUM_VOLUME,
    )
    .map_err(|error| at("filter_bars")(error.to_string()))?;
    let trained = predict::filter_to_trained_tickers(filtered, model_state)
        .map_err(|error| at("filter_tickers")(error.to_string()))?;

    let predictions = predict::generate_predictions(trained, model_state)
        .map_err(|error| at("generate")(error.to_string()))?;

    let array = predictions.as_array().cloned().unwrap_or_default();
    predict::validate_predictions(&array).map_err(at("validate"))?;

    // Split back apart rather than propagated as one error: a malformed payload is a bug in the
    // model output this function just produced, and belongs with the other pipeline stages; a
    // database failure is the database, and keeps reading as one.
    let rows =
        predict::insert_predictions(&state.pool, &array, correlation_id, model_state.run_id())
            .await
            .map_err(|error| match error {
                predict::InsertPredictionsError::Payload(message) => at("insert")(message),
                predict::InsertPredictionsError::Database(error) => HandlerError::Database(error),
            })?;

    Ok((array.len(), rows))
}

/// Every five minutes: price the book, close what should close, open into vacant slots.
async fn handle_portfolio_evaluation(state: &ServiceState) -> Result<Value, HandlerError> {
    let now = Utc::now();
    let today = SessionDate::at(now);

    let calendar = state.calendar_cache.get(&state.trading, now).await?;
    if !calendar.is_trading_day(today) {
        // The holiday gate moved from SQL into Rust when `market_calendar` was dropped, so a
        // holiday produces a no-op pass rather than no pass at all. Cheap enough not to fight.
        return Ok(json!({ "skipped": "not_a_trading_day", "session_date": today }));
    }

    let universe = state
        .universe_cache
        .get(&state.trading, &state.pool, now)
        .await?;
    let close_history = state
        .close_history_cache
        .get(
            &state.pool,
            BarInterval::OneDay,
            CORRELATION_WINDOW_SESSIONS,
            now,
        )
        .await?;

    let context = EvaluationContext {
        pool: &state.pool,
        trading: &state.trading,
        market_data: &state.market_data,
        calendar: &calendar,
        universe: &universe,
        close_history: &close_history,
        sizing: state.sizing,
        execution: state.execution,
        shutdown: &state.shutdown,
        now,
    };

    let summary = evaluate::run_pass(&context).await?;
    Ok(serde_json::to_value(summary).unwrap_or_else(|_| json!({})))
}

/// Pre-close: flatten the book. Runs on a holiday too, and should.
///
/// The calendar is deliberately not consulted. A liquidation on a day with no session closes
/// nothing and costs one API call; a liquidation skipped because the calendar was wrong or
/// unreachable carries positions overnight. The asymmetry is the whole argument.
async fn handle_portfolio_liquidation(state: &ServiceState) -> Result<Value, HandlerError> {
    let summary = evaluate::run_liquidation(&state.pool, &state.trading, Utc::now()).await?;
    Ok(serde_json::to_value(summary).unwrap_or_else(|_| json!({})))
}

/// Post-close: balances, activities, and pair attribution from Alpaca.
async fn handle_account_sync(state: &ServiceState) -> Result<Value, HandlerError> {
    let now = Utc::now();
    let today = SessionDate::at(now);

    let calendar = state.calendar_cache.get(&state.trading, now).await?;
    if !calendar.is_trading_day(today) {
        return Ok(json!({ "skipped": "not_a_trading_day", "session_date": today }));
    }

    let summary = account::sync_account(&state.pool, &state.trading, today).await?;
    Ok(serde_json::to_value(summary).unwrap_or_else(|_| json!({})))
}

/// Post-close: the session's bars and detail changes into PostgreSQL.
///
/// Chains the export rather than letting cron schedule it, so the export can never run against a
/// half-synced database. The chain is emitted only on success for the same reason.
async fn handle_market_data_sync(state: &ServiceState) -> Result<Value, HandlerError> {
    let now = Utc::now();
    let today = SessionDate::at(now);

    let calendar = state.calendar_cache.get(&state.trading, now).await?;
    if !calendar.is_trading_day(today) {
        return Ok(json!({ "skipped": "not_a_trading_day", "session_date": today }));
    }

    // Deliberately no universe load. `load_liquidity` builds the universe from averages over
    // `equity_bars`, so fetching only `universe.symbols()` would mean the sole tickers getting
    // fresh bars are the ones already in it. Past `LIQUIDITY_LOOKBACK_DAYS`, nothing outside would
    // have bars in the window and a stock that became liquid could never enter — the universe
    // ratchets closed and can only shrink. Massive's grouped endpoint takes a date rather than a
    // symbol list, so every symbol that traded is stored and the liquidity
    // screen is a real re-selection each day.

    // A span wide enough to hold the requested sessions even through a holiday week. Six calendar
    // days was not: Thanksgiving plus two weekends leaves fewer than three sessions inside it, and
    // the previous `unwrap_or(today)` then collapsed the window to a single session — closing none
    // of the gap the constant exists to close, and saying nothing about it.
    let span = calendar.trading_days_in_range(
        today.plus_calendar_days(-(BAR_SYNC_LOOKBACK_SESSIONS as i64 * 4)),
        today,
    );
    let sessions: Vec<_> = if span.len() > BAR_SYNC_LOOKBACK_SESSIONS {
        span[span.len() - BAR_SYNC_LOOKBACK_SESSIONS..].to_vec()
    } else {
        // Fewer sessions than requested is a narrower window, and the log says so rather than
        // leaving it to be inferred.
        if span.len() < BAR_SYNC_LOOKBACK_SESSIONS {
            warn!(
                requested = BAR_SYNC_LOOKBACK_SESSIONS,
                found = span.len(),
                "Fewer trading sessions in the lookback span than requested; syncing all of them"
            );
        }
        span
    };

    let fetched = bars::fetch_daily_bars(&state.massive, &sessions).await;
    let bar_rows = bars::store_bars(&state.pool, &fetched.bars).await?;

    let detail_rows =
        details::store_details(&state.pool, &details::parse_embedded_details()?).await?;

    // The cached history now predates the rows just written, so it is dropped rather than
    // overwritten with an empty map -- an empty map keyed to today would pin "no history" for the
    // rest of the Eastern date.
    state.close_history_cache.invalidate().await;

    events::emit(
        &state.pool,
        crate::common::events::EventType::new(
            Command::DatabaseExport,
            crate::common::events::Outcome::Requested,
        ),
        json!({ "chained_from": Command::MarketDataSync.as_str(), "session_date": today }),
    )
    .await?;

    Ok(json!({
        "session_date": today,
        "sessions_requested": sessions.len(),
        "sessions_failed": fetched.dates_failed,
        "bars_fetched": fetched.bars.len(),
        "bar_rows_written": bar_rows,
        "detail_rows_written": detail_rows,
        "export_chained": true,
    }))
}

/// Chained from a completed market data sync: export to S3, then purge.
///
/// The order is fixed and the purge is conditional on the export being clean. Purging after a
/// partial export deletes rows that never reached S3, and nothing afterwards can tell that it
/// happened.
async fn handle_database_export(state: &ServiceState) -> Result<Value, HandlerError> {
    let today = SessionDate::at(Utc::now());
    let export = export::export_database(&state.pool, &state.s3_client, &state.bucket, today).await;

    if !export.is_clean() {
        warn!(
            failed = export.failed.len(),
            "Export was incomplete; the purge is skipped"
        );
        return Ok(json!({
            "session_date": today,
            "exported": export.exported,
            "failed": export.failed.iter().map(|(dataset, error)| json!({
                "dataset": dataset, "error": error.to_string()
            })).collect::<Vec<_>>(),
            "purged": Value::Null,
        }));
    }

    let purged = purge::purge_exported_tables(&state.pool).await;
    Ok(json!({
        "session_date": today,
        "exported": export.exported,
        "total_rows_exported": export.total_rows(),
        "purged_rows": purged.total_rows(),
        "purge_clean": purged.is_clean(),
    }))
}

/// Post-close commands from the previous trading day that never reached a terminal outcome.
///
/// This is what replaced the `scheduler_health_check` cron job. It answers the only question that
/// job existed to answer — did last night's work finish — at the one moment the answer changes what
/// happens next.
async fn previous_session_gaps(
    state: &ServiceState,
    calendar: &TradingCalendar,
    today: SessionDate,
) -> Result<Vec<String>, HandlerError> {
    let Some(previous) = calendar.previous_trading_day(today) else {
        return Ok(Vec::new());
    };
    let (start, end) = previous.bounds();

    let rows = sqlx::query!(
        r#"
        SELECT request.event_type AS "event_type!"
        FROM events AS request
        WHERE request.created_at >= $1
          AND request.created_at < $2
          AND request.event_type LIKE '%\_requested'
          AND NOT EXISTS (
              SELECT 1 FROM events AS terminal
              WHERE terminal.created_at >= $1
                AND terminal.id > request.id
                AND terminal.event_type IN (
                    replace(request.event_type, '_requested', '_completed'),
                    replace(request.event_type, '_requested', '_errored')
                )
          )
        "#,
        start,
        end,
    )
    .fetch_all(&state.pool)
    .await?;

    Ok(rows.into_iter().map(|row| row.event_type).collect())
}

/// How many trading sessions the artifact skipped, when its run identifier can be read as a date.
///
/// Zero is the healthy answer, and calendar days cannot express that. The trainer publishes after
/// one session's close for the *next* session, so a healthy artifact is always dated at least one
/// calendar day back — one midweek, three across a weekend, four across a long one. Counting days
/// reported every one of those as stale; counting the sessions strictly between the artifact's date
/// and today reports none of them, and still reports a genuinely skipped night.
///
/// Bounded by the calendar's horizon, so an artifact older than [`HORIZON_DAYS_BACKWARD`] undercounts.
/// That only understates a number already past the threshold, so the warning still fires.
///
/// `None` rather than a guess when the run identifier is not date-prefixed. The trainer names runs
/// `YYYY-MM-DD-HH-MM-SS-mmm`, but a hand-uploaded artifact need not, and reporting staleness
/// computed from an unparsed prefix would be worse than reporting none.
///
/// [`HORIZON_DAYS_BACKWARD`]: crate::data::calendar
fn artifact_staleness_sessions(
    run_id: &str,
    calendar: &TradingCalendar,
    today: SessionDate,
) -> Option<i64> {
    // The run identifier's date prefix is an Eastern session, written by the trainer through
    // `eastern_datetime`, so wrapping it is `from_date`'s case rather than a derivation.
    let artifact_date =
        SessionDate::from_date(NaiveDate::parse_from_str(run_id.get(..10)?, "%Y-%m-%d").ok()?);
    // Half-open on both ends: the artifact's own session is not a session it missed, and today's
    // has not happened yet. `trading_days_in_range` takes an inclusive range and panics on an
    // inverted one, so the empty case is answered here rather than passed down.
    let first_missed = artifact_date.plus_calendar_days(1);
    let last_missed = today.plus_calendar_days(-1);
    if first_missed > last_missed {
        return Some(0);
    }
    Some(
        calendar
            .trading_days_in_range(first_missed, last_missed)
            .len() as i64,
    )
}

/// Re-runs commands that were requested today and never finished.
///
/// Called once at startup. This is what replaces a consumer offset table: a `_requested` row with
/// no terminal outcome is, by construction, work that was issued and never completed — which is
/// what both a process killed mid-handler and a process that was simply not running when cron
/// fired leave behind.
pub async fn recover(state: Arc<ServiceState>) {
    let commands = match events::recover_missed_commands(state.pool()).await {
        Ok(commands) => commands,
        Err(error) => {
            error!(%error, "Recovery scan failed; starting without replay");
            return;
        }
    };

    if commands.is_empty() {
        info!("No unfinished commands to recover");
        return;
    }

    info!(count = commands.len(), "Replaying unfinished commands");
    for command in commands {
        handle(&state, command).await;
    }
}

/// Reads the current Eastern date, for callers that need it alongside a handler.
pub fn session_date(now: DateTime<Utc>) -> SessionDate {
    SessionDate::at(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::from_ymd_opt(year, month, day).expect("test date must be valid"),
        )
    }

    /// Weekday sessions spanning 2026-07-27 to 2026-08-07. 2026-08-01 is a Saturday and
    /// 2026-08-02 a Sunday, so the weekend is absent.
    fn weekday_calendar() -> TradingCalendar {
        use crate::common::alpaca::CalendarDay;
        let open = chrono::NaiveTime::from_hms_opt(9, 30, 0).expect("valid time");
        let close = chrono::NaiveTime::from_hms_opt(16, 0, 0).expect("valid time");
        let days = (0..12)
            .map(|offset| date(2026, 7, 27).plus_calendar_days(offset))
            .filter(|day| !day.is_weekend())
            .map(|day| {
                CalendarDay::new(day.date(), open, close).expect("test session must be valid")
            })
            .collect();
        TradingCalendar::from_days(days)
    }

    /// The trainer names runs `YYYY-MM-DD-...`, which is what makes staleness readable at all.
    #[test]
    fn test_artifact_staleness_reads_the_date_prefix() {
        let calendar = weekday_calendar();
        // 2026-07-30 is a Thursday, 2026-07-31 the Friday after it.
        assert_eq!(
            artifact_staleness_sessions("2026-07-30-19-21-25-195", &calendar, date(2026, 7, 31)),
            Some(0)
        );
        // 2026-07-29 is the Wednesday, so Thursday's session was skipped.
        assert_eq!(
            artifact_staleness_sessions("2026-07-29-19-21-25-195", &calendar, date(2026, 7, 31)),
            Some(1)
        );
    }

    /// The case a calendar-day count gets wrong, and the reason this is measured in sessions.
    ///
    /// The trainer publishes after one session's close for the next session, so Friday evening's
    /// artifact is what Monday is *supposed* to run on. Three calendar days separate them and zero
    /// trading sessions do. Counting days warned on every healthy Monday.
    #[test]
    fn test_an_artifact_from_friday_is_not_stale_on_monday() {
        let calendar = weekday_calendar();
        let friday = date(2026, 7, 31);
        let monday = date(2026, 8, 3);
        assert_eq!(
            (monday.date() - friday.date()).num_days(),
            3,
            "three calendar days apart"
        );
        assert_eq!(
            artifact_staleness_sessions("2026-07-31-19-21-25-195", &calendar, monday),
            Some(0),
            "the weekend holds no session to have missed"
        );
        // And a real skip is still caught across the same weekend.
        assert_eq!(
            artifact_staleness_sessions("2026-07-30-19-21-25-195", &calendar, monday),
            Some(1),
            "Friday's session was missed"
        );
    }

    /// An artifact dated today or later has missed nothing, and must not index an inverted range.
    #[test]
    fn test_an_artifact_from_today_has_missed_no_sessions() {
        let calendar = weekday_calendar();
        assert_eq!(
            artifact_staleness_sessions("2026-07-31-19-21-25-195", &calendar, date(2026, 7, 31)),
            Some(0)
        );
        assert_eq!(
            artifact_staleness_sessions("2026-08-03-19-21-25-195", &calendar, date(2026, 7, 31)),
            Some(0)
        );
    }

    /// A run identifier that is not date-prefixed yields no staleness. Reporting a number derived
    /// from an unparsed prefix would put a fabricated value into the completion payload, which is
    /// worse than reporting that it is unknown.
    #[test]
    fn test_an_undatable_run_identifier_has_no_staleness() {
        let calendar = weekday_calendar();
        let today = date(2026, 7, 31);
        assert_eq!(
            artifact_staleness_sessions("manual-upload", &calendar, today),
            None
        );
        assert_eq!(artifact_staleness_sessions("short", &calendar, today), None);
        assert_eq!(artifact_staleness_sessions("", &calendar, today), None);
    }

    #[test]
    fn test_session_date_is_eastern_not_utc() {
        // 01:00 UTC is still the previous evening in New York.
        let instant = DateTime::parse_from_rfc3339("2026-07-31T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(session_date(instant), date(2026, 7, 30));
    }
}
