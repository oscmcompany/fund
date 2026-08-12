//! Append-only record of what the application observed before it acted.
//!
//! One JSONL file per session on local disk. This is the only original the application owns: every
//! other store is a fold or a query over it. Sealing and shipping it is [`crate::data::export`]'s.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};
use uuid::Uuid;

use crate::common::types::SessionDate;

/// Version stamped on every record written by this build.
///
/// Readers map old versions forward rather than rewriting files, so this only ever goes up.
pub const SCHEMA_VERSION: u32 = 1;

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
    pub async fn seal(&self) -> SessionLogGuard<'_> {
        let mut open_session = self.open_session.lock().await;
        *open_session = None;
        SessionLogGuard {
            _appends_blocked: open_session,
        }
    }
}

/// Proof that no append can run while it is alive.
///
/// Opaque on purpose: holding it is the whole contract, and there is nothing inside worth reading.
pub struct SessionLogGuard<'a> {
    _appends_blocked: tokio::sync::MutexGuard<'a, Option<OpenSession>>,
}

/// The file one session's records live in.
pub(crate) fn file_name(session_date: SessionDate) -> String {
    format!("session-{}.jsonl", session_date.date())
}

/// Recovers the session from a name built by [`file_name`], or `None` for anything else.
///
/// `None` rather than an error, because a stray file is something to skip, not to fail a run
/// over.
pub(crate) fn session_from_file_name(name: &str) -> Option<SessionDate> {
    let date = name.strip_prefix("session-")?.strip_suffix(".jsonl")?;
    let session_date = SessionDate::from_date(NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?);
    // Accepted only if it is exactly what the writer would have produced. `%Y-%m-%d` also parses
    // `2026-8-11`, and admitting both spellings would let one session reach the export twice under
    // a single key, where whichever was read last silently wins.
    (file_name(session_date) == name).then_some(session_date)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

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
}
