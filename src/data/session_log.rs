//! Append-only record of what the application observed before it acted.
//!
//! One JSONL file per session on local disk, sealed at the close and written to S3 as Parquet.
//! This is the only original the application owns: every other store is a fold or a query over it.

use std::path::{Path, PathBuf};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use serde::Serialize;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::common::aws::date_partitioned_key;
use crate::data::calendar::SessionDate;

/// Version stamped on every record written by this build.
///
/// Readers map old versions forward rather than rewriting files, so this only ever goes up.
pub const SCHEMA_VERSION: u32 = 1;

/// S3 prefix the sealed sessions are written under.
pub const EXPORT_PREFIX: &str = "exports/session_log";

/// Calendar days of sealed sessions kept on local disk after a clean export.
///
/// The window in which a bad Parquet conversion can still be repaired from the original bytes.
pub const RETENTION_DAYS: i64 = 7;

/// Anything that stops a record reaching the disk.
#[derive(Debug, thiserror::Error)]
pub enum SessionLogError {
    #[error("session log directory {directory} is unusable: {source}")]
    Directory {
        directory: String,
        #[source]
        source: std::io::Error,
    },
    #[error("session log write failed: {0}")]
    Write(#[from] std::io::Error),
    #[error("observation could not be serialized: {0}")]
    Serialize(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One observation, tagged with what kind it is.
///
/// Observations only — no slippage, profit and loss, or exposure totals, each of which is a query
/// over these rows. Variants are additive and past files are never rewritten.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "event_type", content = "payload", rename_all = "snake_case")]
pub enum Observation {
    /// One five-minute pass: what it saw, what it decided, and what stopped it doing more.
    EvaluationPass(Box<EvaluationPass>),
    /// An order as it was sent, written before the request leaves the process.
    OrderSubmitted(OrderSubmitted),
    /// How the broker settled that order, filled or not.
    OrderResolved(OrderResolved),
    /// A position the application asked Alpaca to close.
    PositionCloseRequested(PositionCloseRequested),
    /// The pre-close flattening.
    LiquidationRun(LiquidationRun),
    /// The pre-open inference run and the artifact it resolved.
    PredictionsGenerated(PredictionsGenerated),
    /// The post-close account state, which Alpaca cannot fully report again for a past date.
    AccountObserved(AccountObserved),
}

impl Observation {
    /// The stable name this observation serializes under.
    pub fn event_type(&self) -> &'static str {
        match self {
            Observation::EvaluationPass(_) => "evaluation_pass",
            Observation::OrderSubmitted(_) => "order_submitted",
            Observation::OrderResolved(_) => "order_resolved",
            Observation::PositionCloseRequested(_) => "position_close_requested",
            Observation::LiquidationRun(_) => "liquidation_run",
            Observation::PredictionsGenerated(_) => "predictions_generated",
            Observation::AccountObserved(_) => "account_observed",
        }
    }
}

/// One evaluation pass, whole.
///
/// `prices` holds every reading once; pair and candidate rows name tickers without repeating it.
/// `candidates` holds only the pairs the screen scored, not the quadratic cross product.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct EvaluationPass {
    pub account_equity: Option<f64>,
    pub previous_session_equity: Option<f64>,
    pub gross_exposure_used: Option<f64>,
    pub gross_exposure_cap: Option<f64>,
    pub drawdown: Option<f64>,
    pub minutes_until_close: Option<i64>,
    pub open_pairs_at_start: usize,
    pub vacant_slots: Option<usize>,
    pub universe_size: usize,
    pub predictions_available: usize,
    pub eligible_tickers: usize,
    pub candidates_screened: usize,
    pub model_run_id: Option<String>,
    /// Why the entry half did not run at all, if it did not.
    pub session_block: Option<String>,
    pub prices: Vec<PriceReading>,
    pub open_pairs: Vec<OpenPairReading>,
    /// Every forecast that reached the screen, as the screen received it.
    pub screen_inputs: Vec<ScreenInputReading>,
    /// Every forecast that did not, and the first test it failed.
    pub excluded: Vec<ExcludedTickerReading>,
    pub candidates: Vec<CandidateReading>,
}

/// One forecast as the screen consumed it.
///
/// Derived from the stored quantiles and the session's universe, so not recoverable from
/// `equity_predictions` alone.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScreenInputReading {
    pub ticker: String,
    pub expected_return: f64,
    pub confidence: f64,
    pub is_shortable: bool,
}

/// One forecast the eligibility filter removed, and why.
///
/// Written every pass, because `held` changes within a session even though the other tests do not.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExcludedTickerReading {
    pub ticker: String,
    /// `already_held`, `no_sector`, `no_close_history`, `outside_universe`, `unpriced`, or
    /// `unusable_input`.
    pub reason: String,
}

/// One symbol's reference price, and which snapshot field it came from.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PriceReading {
    pub ticker: String,
    pub price: f64,
    pub price_source: String,
}

/// An open pair as this pass measured it, whether or not it closed.
///
/// Carries every input to the z-score, which on its own cannot distinguish a price move from a
/// refit.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OpenPairReading {
    pub pair_id: String,
    pub long_ticker: String,
    pub short_ticker: String,
    pub stored_hedge_ratio: f64,
    pub model_hedge_ratio: Option<f64>,
    pub spread_mean: Option<f64>,
    pub spread_standard_deviation: Option<f64>,
    pub z_score: Option<f64>,
    pub entry_z_score: f64,
    pub stop_at: f64,
    pub entry_session: SessionDate,
    pub minutes_held: i64,
    /// `held`, `convergence`, `stop_loss`, `end_of_day`, `position_missing`, `unpriced`,
    /// `no_spread_model`, `unreadable_spread`, or `close_failed`.
    pub decision: String,
}

/// A scored candidate and what became of it.
///
/// The sizing fields are set for every candidate that reached the sizer, refused ones included.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CandidateReading {
    pub pair_id: String,
    pub long_ticker: String,
    pub short_ticker: String,
    pub hedge_ratio: f64,
    pub entry_z_score: f64,
    pub signal_strength: f64,
    pub rank_score: f64,
    /// Dollars the long leg was sized to, absent for a candidate that was never selected.
    pub long_notional: Option<f64>,
    /// Whole shares the short leg was sized to.
    pub short_shares: Option<f64>,
    pub gross_exposure: Option<f64>,
    /// `opened`, `not_selected`, `risk_refused`, `unfilled`, or `abandoned_at_shutdown`.
    pub decision: String,
    /// The risk gate's rendered reason, when `decision` is `risk_refused`.
    pub refusal: Option<String>,
}

/// An order at the moment it was sent, before the broker has said anything about it.
///
/// Keyed by `client_order_id` because this is written before the request leaves the process, when
/// the broker's identifier does not exist yet.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderSubmitted {
    pub client_order_id: String,
    pub ticker: String,
    pub side: String,
    /// Set for the short leg, which is sized in shares.
    pub shares: Option<f64>,
    /// Set for the long leg, which is sized in dollars.
    pub notional: Option<f64>,
}

/// How the broker settled an order.
///
/// One follows every [`OrderSubmitted`], so an unresolved submission means the process died between
/// the two. Slippage is absent: it is this row joined to the pass that produced the order.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderResolved {
    pub client_order_id: String,
    /// Absent when the order never reached the broker.
    pub alpaca_order_id: Option<String>,
    pub ticker: String,
    /// `filled`, `submit_failed`, `broker_unreachable`, or the broker's terminal status.
    pub outcome: String,
    pub filled_shares: Option<f64>,
    pub filled_average_price: Option<f64>,
    /// True when the fill was read after a cancel raced it.
    pub filled_after_cancel: bool,
    /// The broker's error, when the order failed rather than settled.
    pub error: Option<String>,
}

/// One position the application asked Alpaca to close.
///
/// Unlike an entry, an exit is not polled to a terminal state, so there is no fill price here —
/// `alpaca_order_id` is the join to the fill once the post-close activity sync lands it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PositionCloseRequested {
    pub ticker: String,
    /// The pair this leg belonged to, absent when the account is flattened without consulting them.
    pub pair_id: Option<String>,
    /// Absent when there was no position, or when Alpaca accepted the close and returned a body
    /// this client could not read.
    pub alpaca_order_id: Option<String>,
    pub side: Option<String>,
    pub quantity: Option<f64>,
    /// `pair_exit`, `entry_unwind`, or `liquidation`.
    pub reason: String,
    /// False when Alpaca refused the close, or when there was no position to close.
    pub accepted: bool,
    /// Alpaca's per-symbol status, on the bulk path that reports one.
    pub status: Option<u16>,
    /// The broker's error, set only when the request itself failed. `accepted: false` with no
    /// error means there was no position, which is the opposite state.
    pub error: Option<String>,
}

/// The pre-close flattening.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LiquidationRun {
    pub pairs_closed: usize,
    pub positions_refused: usize,
    pub pairs_still_open: Vec<String>,
}

/// The pre-open inference run.
///
/// The forecasts live in `equity_predictions`; what only this run knows is which artifact made
/// them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PredictionsGenerated {
    pub model_run_id: String,
    pub artifact_key: String,
    pub artifact_staleness_sessions: Option<i64>,
    pub predictions: usize,
    pub rows_written: u64,
    pub universe_size: usize,
}

/// The post-close account state.
///
/// Recorded in full because Alpaca backfills only equity for a past date, never the rest.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountObserved {
    /// The session these balances describe, which is not always the one the record was written in:
    /// a post-close sync re-run after Eastern midnight observes yesterday from today.
    pub session_date: SessionDate,
    /// `None` when the broker's decimal will not fit an `f64`. A fabricated zero would be a false
    /// observation, which is the one thing this log must not contain.
    pub equity: Option<f64>,
    pub cash: Option<f64>,
    pub buying_power: Option<f64>,
    pub long_market_value: Option<f64>,
    pub short_market_value: Option<f64>,
}

/// One line of the log: an observation with the envelope that makes it addressable.
///
/// `session_date` is derived from `created_at`, so a record cannot be filed under a session it did
/// not happen in.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Record {
    pub schema_version: u32,
    pub event_id: Uuid,
    /// Threads a pass to the orders it caused to the fills they produced.
    pub correlation_id: Uuid,
    pub session_date: SessionDate,
    pub created_at: DateTime<Utc>,
    #[serde(flatten)]
    pub observation: Observation,
}

impl Record {
    /// Stamps an observation with a fresh identity at `created_at`.
    pub fn new(correlation_id: Uuid, created_at: DateTime<Utc>, observation: Observation) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            event_id: Uuid::new_v4(),
            correlation_id,
            session_date: SessionDate::at(created_at),
            created_at,
            observation,
        }
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// The file the writer currently holds open, and the session it belongs to.
struct OpenSession {
    session_date: SessionDate,
    file: tokio::fs::File,
}

/// Appends records to the current session's file.
///
/// The file rolls when the Eastern date does, so the session comes from the record rather than from
/// construction.
pub struct SessionLog {
    directory: PathBuf,
    open_session: tokio::sync::Mutex<Option<OpenSession>>,
}

impl SessionLog {
    /// Opens a log against `directory`, creating it if needed.
    pub fn new(directory: impl Into<PathBuf>) -> Result<Self, SessionLogError> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|source| SessionLogError::Directory {
            directory: directory.display().to_string(),
            source,
        })?;
        Ok(Self {
            directory,
            open_session: tokio::sync::Mutex::new(None),
        })
    }

    /// Opens a log at `FUND_SESSION_LOG_DIR`, or `sessions/` under the configured log directory.
    ///
    /// Defaulting inside `FUND_LOG_DIR` inherits that path's existing writability fallback.
    pub fn from_env() -> Result<Self, SessionLogError> {
        let directory = match std::env::var("FUND_SESSION_LOG_DIR") {
            Ok(directory) => PathBuf::from(directory),
            Err(_) => Path::new(
                &std::env::var("FUND_LOG_DIR").unwrap_or_else(|_| "/var/log/fund".to_string()),
            )
            .join("sessions"),
        };
        Self::new(directory)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Appends one record and returns once it is fsynced to the disk.
    ///
    /// A caller that awaits this before acting knows the observation survives the crash the action
    /// might cause.
    pub async fn append(&self, record: &Record) -> Result<(), SessionLogError> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');

        let mut open_session = self.open_session.lock().await;
        let session = match open_session.as_mut() {
            Some(session) if session.session_date == record.session_date => session,
            _ => {
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.directory.join(file_name(record.session_date)))
                    .await?;
                open_session.insert(OpenSession {
                    session_date: record.session_date,
                    file,
                })
            }
        };

        session.file.write_all(&line).await?;
        session.file.flush().await?;
        session.file.sync_all().await?;
        Ok(())
    }

    /// Appends a record, reporting a failure without propagating it.
    ///
    /// A log write must not become a new way for the fund to stop trading, so acting unobserved
    /// beats refusing to act.
    pub async fn record(
        &self,
        correlation_id: Uuid,
        created_at: DateTime<Utc>,
        observation: Observation,
    ) {
        let record = Record::new(correlation_id, created_at, observation);
        let event_type = record.observation.event_type();
        match self.append(&record).await {
            Ok(()) => debug!(event_type, %correlation_id, "Session log record written"),
            Err(error) => error!(
                event_type,
                %correlation_id,
                %error,
                "Session log record could not be written; the action proceeds unobserved"
            ),
        }
    }
}

impl SessionLog {
    /// Blocks appends for as long as the returned guard is held.
    ///
    /// The export takes this before reading, so no reader sees a half-written line. The open handle
    /// is dropped, so the next append reopens the file.
    async fn seal(&self) -> tokio::sync::MutexGuard<'_, Option<OpenSession>> {
        let mut open_session = self.open_session.lock().await;
        *open_session = None;
        open_session
    }
}

/// The file one session's records live in.
fn file_name(session_date: SessionDate) -> String {
    format!("session-{}.jsonl", session_date.date())
}

/// Recovers the session from a name built by [`file_name`], or `None` for anything else.
///
/// `None` rather than an error, because a stray file is something to skip, not to fail a run
/// over.
fn session_from_file_name(name: &str) -> Option<SessionDate> {
    let date = name.strip_prefix("session-")?.strip_suffix(".jsonl")?;
    let session_date = SessionDate::from_date(NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?);
    // Accepted only if it is exactly what the writer would have produced. `%Y-%m-%d` also parses
    // `2026-8-11`, and admitting both spellings would let one session reach the export twice under
    // a single key, where whichever was read last silently wins.
    (file_name(session_date) == name).then_some(session_date)
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

/// What one export run accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionLogExportSummary {
    /// `(session_date, records)` for each session written to S3.
    pub exported: Vec<(NaiveDate, usize)>,
    /// `(session_date, error)` for each session that failed.
    pub failed: Vec<(NaiveDate, String)>,
    /// Sessions whose local file was deleted after the retention window.
    pub deleted: Vec<NaiveDate>,
    /// Lines skipped as unreadable, across every session.
    ///
    /// A torn final line per crashed session is expected; more than that means something else is
    /// wrong.
    pub unparsable_lines: usize,
}

impl SessionLogExportSummary {
    pub fn total_records(&self) -> usize {
        self.exported.iter().map(|(_, records)| records).sum()
    }

    pub fn is_clean(&self) -> bool {
        self.failed.is_empty()
    }
}

/// Converts every sealed session on disk to Parquet in S3, then deletes what has aged out.
///
/// Every file present is exported, not just the retention window: one whole file at a deterministic
/// key makes a repeat a byte-identical overwrite, so a failed run repairs itself. Local files are
/// deleted only after a clean run.
pub async fn export_session_logs(
    log: &SessionLog,
    s3_client: &S3Client,
    bucket: &str,
    today: SessionDate,
) -> SessionLogExportSummary {
    let mut summary = SessionLogExportSummary::default();

    let mut sessions = match sealed_sessions(log.directory(), today) {
        Ok(sessions) => sessions,
        Err(error) => {
            warn!(%error, "Session log directory could not be read; nothing exported");
            return summary;
        }
    };
    sessions.sort();

    // Held across the reads so no append can interleave with one. Today's file is sealed by the
    // clock rather than by anything structural, and a recovery replay can run a pass at any hour.
    let sealed = log.seal().await;

    for session_date in &sessions {
        let path = log.directory().join(file_name(*session_date));
        match read_session_frame(&path) {
            Ok((mut frame, unparsable)) => {
                summary.unparsable_lines += unparsable;
                let key = date_partitioned_key(EXPORT_PREFIX, session_date.date());
                match write_frame(s3_client, bucket, &key, &mut frame).await {
                    Ok(()) => summary.exported.push((session_date.date(), frame.height())),
                    Err(error) => summary.failed.push((session_date.date(), error)),
                }
            }
            Err(error) => summary.failed.push((session_date.date(), error)),
        }
    }

    drop(sealed);

    if summary.is_clean() {
        summary.deleted = delete_aged_out(log.directory(), &sessions, today);
    }

    info!(
        sessions = summary.exported.len(),
        records = summary.total_records(),
        failed = summary.failed.len(),
        deleted = summary.deleted.len(),
        unparsable_lines = summary.unparsable_lines,
        "Session log export finished"
    );
    for (session_date, error) in &summary.failed {
        warn!(%session_date, error, "Session log failed to export");
    }
    summary
}

/// Sessions on disk whose trading day has finished.
///
/// Today counts, because this runs after the close; a future-dated file does not, and exporting one
/// would seal a session still being written.
fn sealed_sessions(
    directory: &Path,
    today: SessionDate,
) -> Result<Vec<SessionDate>, std::io::Error> {
    let mut sessions = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(session_date) = session_from_file_name(&name) else {
            continue;
        };
        if session_date > today {
            warn!(%session_date, %today, "Session log is dated ahead of today; not sealed");
            continue;
        }
        sessions.push(session_date);
    }
    Ok(sessions)
}

/// Removes the local copies of sessions older than the retention window.
///
/// Called only after a clean run, so a session cannot age out without reaching S3. A file that will
/// not delete is a warning: a stale local copy costs disk and nothing else.
fn delete_aged_out(
    directory: &Path,
    sessions: &[SessionDate],
    today: SessionDate,
) -> Vec<NaiveDate> {
    let oldest_kept = today.plus_calendar_days(-RETENTION_DAYS);
    let mut deleted = Vec::new();
    for session_date in sessions.iter().filter(|session| **session < oldest_kept) {
        let path = directory.join(file_name(*session_date));
        match std::fs::remove_file(&path) {
            Ok(()) => deleted.push(session_date.date()),
            Err(error) => warn!(
                path = %path.display(),
                %error,
                "Aged-out session log could not be deleted"
            ),
        }
    }
    deleted
}

/// Reads one session file into a frame, returning it with the number of lines skipped.
///
/// Discarding a torn final line is why the original is JSONL rather than Parquet.
fn read_session_frame(path: &Path) -> Result<(DataFrame, usize), String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;

    let mut schema_versions: Vec<Option<i64>> = Vec::new();
    let mut event_ids: Vec<Option<String>> = Vec::new();
    let mut correlation_ids: Vec<Option<String>> = Vec::new();
    let mut event_types: Vec<Option<String>> = Vec::new();
    let mut session_dates: Vec<Option<String>> = Vec::new();
    let mut created_ats: Vec<Option<i64>> = Vec::new();
    let mut payloads: Vec<Option<String>> = Vec::new();
    let mut unparsable = 0usize;

    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(Value::Object(record)) = serde_json::from_str::<Value>(line) else {
            unparsable += 1;
            continue;
        };
        let text = |key: &str| record.get(key).and_then(Value::as_str).map(str::to_string);

        // A record missing any of its envelope is discarded rather than written with nulls, so a
        // query cannot mistake an unaddressable row for a good one.
        let (Some(event_id), Some(event_type), Some(created_at)) = (
            text("event_id"),
            text("event_type"),
            text("created_at").and_then(|stamp| {
                DateTime::parse_from_rfc3339(&stamp)
                    .ok()
                    .map(|instant| instant.timestamp_millis())
            }),
        ) else {
            unparsable += 1;
            continue;
        };

        schema_versions.push(record.get("schema_version").and_then(Value::as_i64));
        event_ids.push(Some(event_id));
        correlation_ids.push(text("correlation_id"));
        event_types.push(Some(event_type));
        session_dates.push(text("session_date"));
        created_ats.push(Some(created_at));
        payloads.push(record.get("payload").map(Value::to_string));
    }

    if unparsable > 0 {
        warn!(
            path = %path.display(),
            unparsable,
            "Skipped session log lines that would not parse"
        );
    }

    let frame = DataFrame::new(vec![
        Column::new("schema_version".into(), schema_versions),
        Column::new("event_id".into(), event_ids),
        Column::new("correlation_id".into(), correlation_ids),
        Column::new("event_type".into(), event_types),
        Column::new("session_date".into(), session_dates),
        Column::new("created_at".into(), created_ats),
        Column::new("payload".into(), payloads),
    ])
    .map_err(|error| format!("failed to build frame for {}: {error}", path.display()))?;

    Ok((frame, unparsable))
}

/// Serializes a frame to Parquet and puts it at `key`.
async fn write_frame(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    frame: &mut DataFrame,
) -> Result<(), String> {
    let mut buffer: Vec<u8> = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(frame)
        .map_err(|error| format!("failed to serialize Parquet: {error}"))?;

    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(buffer))
        .content_type("application/vnd.apache.parquet")
        .send()
        .await
        .map_err(|error| format!("failed to write s3://{bucket}/{key}: {error}"))?;

    info!(key, records = frame.height(), "Session log exported");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).expect("valid date"))
    }

    fn instant(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid instant")
            .with_timezone(&Utc)
    }

    fn account_observation() -> Observation {
        Observation::AccountObserved(AccountObserved {
            session_date: session(2026, 8, 11),
            equity: Some(104_812.55),
            cash: Some(12_000.0),
            buying_power: Some(200_000.0),
            long_market_value: Some(50_000.0),
            short_market_value: Some(-50_000.0),
        })
    }

    /// A directory unique to this run.
    ///
    /// The process and counter suffix stop two overlapping `cargo test` invocations deleting and
    /// recreating one path underneath each other.
    fn temporary_directory(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "fund-session-log-{name}-{}-{unique}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        directory
    }

    #[test]
    fn test_a_file_name_round_trips_through_its_session() {
        for date in [
            session(2026, 8, 11),
            session(2025, 12, 31),
            session(2024, 2, 29),
        ] {
            assert_eq!(session_from_file_name(&file_name(date)), Some(date));
        }
    }

    #[test]
    fn test_unrecognized_file_names_are_skipped() {
        for name in [
            "",
            "session-2026-08-11.parquet",
            "session-2026-8-11.jsonl",
            "2026-08-11.jsonl",
            "session-not-a-date.jsonl",
            "session-2026-02-30.jsonl",
        ] {
            assert_eq!(session_from_file_name(name), None, "name: {name}");
        }
    }

    /// The envelope files a record under the session its instant falls in, not the UTC date. 01:00
    /// UTC is still the previous evening in New York, and getting this wrong splits one session
    /// across two files for part of the year.
    #[test]
    fn test_the_session_is_derived_from_the_instant_in_eastern_terms() {
        let record = Record::new(
            Uuid::new_v4(),
            instant("2026-08-12T01:00:00Z"),
            account_observation(),
        );
        assert_eq!(record.session_date, session(2026, 8, 11));
    }

    #[test]
    fn test_a_record_serializes_with_its_type_and_payload() {
        let record = Record::new(
            Uuid::nil(),
            instant("2026-08-11T20:15:00Z"),
            account_observation(),
        );
        let value: Value = serde_json::to_value(&record).expect("record must serialize");

        assert_eq!(value["schema_version"], json_number(SCHEMA_VERSION as i64));
        assert_eq!(value["event_type"], "account_observed");
        assert_eq!(value["session_date"], "2026-08-11");
        assert_eq!(value["payload"]["equity"], 104_812.55);
        assert!(
            value.get("event_id").and_then(Value::as_str).is_some(),
            "every record is addressable by its own identifier"
        );
    }

    fn json_number(value: i64) -> Value {
        Value::Number(value.into())
    }

    /// No conclusions in the log. Storing a computed value would create a second thing that can
    /// disagree with the observations it was derived from.
    #[test]
    fn test_a_fill_records_the_observation_and_not_the_slippage() {
        let filled = OrderResolved {
            client_order_id: "kopep-long".to_string(),
            alpaca_order_id: Some("order-1".to_string()),
            ticker: "KO".to_string(),
            outcome: "filled".to_string(),
            filled_shares: Some(412.0),
            filled_average_price: Some(62.44),
            filled_after_cancel: false,
            error: None,
        };
        let value = serde_json::to_value(&filled).expect("fill must serialize");
        let object = value.as_object().expect("a fill is an object");
        assert!(!object.contains_key("slippage"));
        assert!(!object.contains_key("realized_profit_and_loss"));
    }

    #[tokio::test]
    async fn test_records_append_to_one_file_per_session() {
        let directory = temporary_directory("append");
        let log = SessionLog::new(&directory).expect("the directory must be creatable");

        log.record(
            Uuid::new_v4(),
            instant("2026-08-11T14:35:00Z"),
            account_observation(),
        )
        .await;
        log.record(
            Uuid::new_v4(),
            instant("2026-08-11T18:00:00Z"),
            account_observation(),
        )
        .await;

        let contents = std::fs::read_to_string(directory.join("session-2026-08-11.jsonl"))
            .expect("the session file must exist");
        assert_eq!(contents.lines().count(), 2);
        for line in contents.lines() {
            serde_json::from_str::<Value>(line).expect("every line must be a complete object");
        }
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The file rolls when the Eastern date does. A writer that held one file for the life of the
    /// process would put two sessions in one archive object.
    #[tokio::test]
    async fn test_a_new_session_opens_a_new_file() {
        let directory = temporary_directory("rollover");
        let log = SessionLog::new(&directory).expect("the directory must be creatable");

        log.record(
            Uuid::new_v4(),
            instant("2026-08-11T20:00:00Z"),
            account_observation(),
        )
        .await;
        log.record(
            Uuid::new_v4(),
            instant("2026-08-12T20:00:00Z"),
            account_observation(),
        )
        .await;

        assert!(directory.join("session-2026-08-11.jsonl").is_file());
        assert!(directory.join("session-2026-08-12.jsonl").is_file());
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// A crash mid-append leaves a partial final line. Discarding it is the whole reason the
    /// original is JSONL: the same truncation in a Parquet file loses the entire session.
    #[test]
    fn test_a_torn_final_line_is_skipped_rather_than_failing_the_session() {
        let directory = temporary_directory("torn");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let complete = serde_json::to_string(&Record::new(
            Uuid::nil(),
            instant("2026-08-11T14:35:00Z"),
            account_observation(),
        ))
        .expect("the record must serialize");
        std::fs::write(&path, format!("{complete}\n{{\"schema_version\":1,\"eve"))
            .expect("the file must be writable");

        let (frame, unparsable) = read_session_frame(&path).expect("the session must still read");
        assert_eq!(frame.height(), 1);
        assert_eq!(unparsable, 1);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Valid JSON is not enough: a row without an addressable envelope would reach Parquet with
    /// nulls no query could tell apart from a good record.
    #[test]
    fn test_a_record_missing_its_envelope_is_counted_unparsable() {
        let directory = temporary_directory("envelope");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let complete = serde_json::to_string(&Record::new(
            Uuid::nil(),
            instant("2026-08-11T20:15:00Z"),
            account_observation(),
        ))
        .expect("the record must serialize");
        let lines = [
            complete.as_str(),
            r#"{"schema_version":1,"event_type":"account_observed","created_at":"2026-08-11T20:15:00Z"}"#,
            r#"{"schema_version":1,"event_id":"a","event_type":"account_observed","created_at":"not a time"}"#,
        ]
        .join("\n");
        std::fs::write(&path, lines).expect("the file must be writable");

        let (frame, unparsable) = read_session_frame(&path).expect("the session must read");
        assert_eq!(
            frame.height(),
            1,
            "only the complete record reaches the frame"
        );
        assert_eq!(
            unparsable, 2,
            "a missing event_id and an unparseable instant"
        );
    }

    #[test]
    fn test_a_frame_carries_the_envelope_as_columns_and_the_payload_as_json() {
        let directory = temporary_directory("frame");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let record = Record::new(
            Uuid::nil(),
            instant("2026-08-11T20:15:00Z"),
            account_observation(),
        );
        std::fs::write(
            &path,
            format!(
                "{}\n",
                serde_json::to_string(&record).expect("the record must serialize")
            ),
        )
        .expect("the file must be writable");

        let (frame, _) = read_session_frame(&path).expect("the session must read");
        assert_eq!(
            frame.get_column_names(),
            [
                "schema_version",
                "event_id",
                "correlation_id",
                "event_type",
                "session_date",
                "created_at",
                "payload"
            ]
        );
        let payload = frame
            .column("payload")
            .expect("payload column")
            .str()
            .expect("payload is text")
            .get(0)
            .expect("one row");
        let parsed: Value = serde_json::from_str(payload).expect("the payload stays valid JSON");
        assert_eq!(parsed["equity"], 104_812.55);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Today's file is sealed because the export runs after the close; a future-dated one is not,
    /// and exporting it would seal a session still being written.
    #[test]
    fn test_only_sessions_up_to_today_are_sealed() {
        let directory = temporary_directory("sealed");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        for name in [
            "session-2026-08-09.jsonl",
            "session-2026-08-11.jsonl",
            "session-2026-08-12.jsonl",
            "notes.txt",
        ] {
            std::fs::write(directory.join(name), "").expect("the file must be writable");
        }

        let mut sealed = sealed_sessions(&directory, session(2026, 8, 11)).expect("readable");
        sealed.sort();
        assert_eq!(sealed, vec![session(2026, 8, 9), session(2026, 8, 11)]);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// RAII guard that restores an environment variable on drop, panic-safe.
    ///
    /// Tests using this must be marked `#[serial]` to prevent concurrent env access.
    struct EnvVarRestoreGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarRestoreGuard {
        fn save(key: &'static str) -> Self {
            Self {
                key,
                previous: std::env::var(key).ok(),
            }
        }
    }

    impl Drop for EnvVarRestoreGuard {
        fn drop(&mut self) {
            // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    /// The explicit override wins, and the directory is created rather than assumed.
    ///
    /// This is what runs at startup: a service that cannot resolve a writable directory here fails
    /// to construct at all, which is the loud failure rather than a session silently unrecorded.
    #[test]
    #[serial_test::serial]
    fn test_from_env_prefers_the_explicit_directory() {
        let directory = temporary_directory("from-env-explicit");
        let _restore_session = EnvVarRestoreGuard::save("FUND_SESSION_LOG_DIR");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { std::env::set_var("FUND_SESSION_LOG_DIR", &directory) };

        let log = SessionLog::from_env().expect("the log must resolve");

        assert_eq!(log.directory(), directory);
        assert!(directory.is_dir());
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Falling back inside `FUND_LOG_DIR` is what lets this ship without provisioning: that path is
    /// already created and already falls back to a writable location on an un-bootstrapped machine.
    #[test]
    #[serial_test::serial]
    fn test_from_env_falls_back_beneath_the_log_directory() {
        let log_directory = temporary_directory("from-env-fallback");
        let _restore_session = EnvVarRestoreGuard::save("FUND_SESSION_LOG_DIR");
        let _restore_log = EnvVarRestoreGuard::save("FUND_LOG_DIR");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe {
            std::env::remove_var("FUND_SESSION_LOG_DIR");
            std::env::set_var("FUND_LOG_DIR", &log_directory);
        }

        let log = SessionLog::from_env().expect("the log must resolve");

        assert_eq!(log.directory(), log_directory.join("sessions"));
        assert!(log.directory().is_dir());
        let _ = std::fs::remove_dir_all(&log_directory);
    }

    /// The frame survives a Parquet round trip with its envelope typed and its payload intact.
    ///
    /// This is the artifact DuckDB reads, so a serialization that silently dropped a column or
    /// widened the payload out of shape would take the whole archive with it.
    #[test]
    fn test_a_session_frame_round_trips_through_parquet() {
        let directory = temporary_directory("parquet");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let mut lines = String::new();
        for equity in [104_812.55_f64, 105_000.0] {
            let record = Record::new(
                Uuid::new_v4(),
                instant("2026-08-11T20:15:00Z"),
                Observation::AccountObserved(AccountObserved {
                    session_date: session(2026, 8, 11),
                    equity: Some(equity),
                    cash: Some(12_000.0),
                    buying_power: Some(200_000.0),
                    long_market_value: Some(50_000.0),
                    short_market_value: Some(-50_000.0),
                }),
            );
            lines.push_str(&serde_json::to_string(&record).expect("the record must serialize"));
            lines.push('\n');
        }
        std::fs::write(&path, lines).expect("the file must be writable");

        let (mut frame, _) = read_session_frame(&path).expect("the session must read");
        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer)
            .finish(&mut frame)
            .expect("the frame must serialize to Parquet");

        let restored = ParquetReader::new(std::io::Cursor::new(buffer))
            .finish()
            .expect("the Parquet must read back");
        assert_eq!(restored.height(), 2);
        assert_eq!(restored.get_column_names(), frame.get_column_names());
        // The instant survives as milliseconds, which is what lets a reader recover 16:15 Eastern.
        assert_eq!(
            restored
                .column("created_at")
                .expect("created_at column")
                .i64()
                .expect("created_at is an integer")
                .get(0),
            Some(instant("2026-08-11T20:15:00Z").timestamp_millis())
        );
        let payload = restored
            .column("payload")
            .expect("payload column")
            .str()
            .expect("payload is text")
            .get(0)
            .expect("one row");
        let parsed: Value = serde_json::from_str(payload).expect("the payload stays valid JSON");
        assert_eq!(parsed["equity"], 104_812.55);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Only what has aged out goes, and the window's own edge stays. The local file is the
    /// crash-exact original, so deleting one still inside the window would remove the only thing a
    /// bad Parquet conversion could be repaired from.
    #[test]
    fn test_only_sessions_past_the_retention_window_are_deleted() {
        let directory = temporary_directory("retention");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");

        let today = session(2026, 8, 11);
        let sessions = [
            today.plus_calendar_days(-RETENTION_DAYS - 1),
            today.plus_calendar_days(-RETENTION_DAYS),
            today.plus_calendar_days(-1),
            today,
        ];
        for session_date in sessions {
            std::fs::write(directory.join(file_name(session_date)), "")
                .expect("the file must be writable");
        }

        let deleted = delete_aged_out(&directory, &sessions, today);

        assert_eq!(deleted, vec![sessions[0].date()]);
        assert!(!directory.join(file_name(sessions[0])).exists());
        for session_date in &sessions[1..] {
            assert!(
                directory.join(file_name(*session_date)).exists(),
                "{session_date} is still inside the window"
            );
        }
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The key is a pure function of the session, which is what makes a repeat export an overwrite
    /// rather than a duplicate — and why there is no cursor to keep in sync.
    #[test]
    fn test_the_export_key_is_determined_by_the_session_alone() {
        let key = date_partitioned_key(EXPORT_PREFIX, session(2026, 8, 11).date());
        assert_eq!(
            key,
            "exports/session_log/year=2026/month=08/day=11/data.parquet"
        );
        assert_eq!(
            key,
            date_partitioned_key(EXPORT_PREFIX, session(2026, 8, 11).date())
        );
    }
}
