//! Append-only record of what the application observed before it acted.
//!
//! One JSONL file per session on local disk, and the only original this application owns — every
//! other store is a fold over it. Sealing and shipping it is [`crate::data::export`]'s.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};
use uuid::Uuid;

use crate::common::types::SessionDate;

/// Version stamped on every record written by this build.
///
/// Readers map old versions forward rather than rewriting files, so this only ever goes up. What
/// each version held is documented beside the DuckDB view in `tools/duckdb_initialization.sql`.
pub const SCHEMA_VERSION: u32 = 2;

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
    /// One scheduled command, however it ended.
    CommandFinished(CommandFinished),
    /// One five-minute pass: what it decided, and what stopped it doing more.
    PassEvaluated(Box<PassEvaluated>),
    /// What one price fetch returned, and what it asked for and did not get.
    PricesObserved(PricesObserved),
    /// The eligibility funnel: what reached the screen and what did not.
    UniverseScreened(UniverseScreened),
    /// Every open pair as one pass measured it, closed or held.
    OpenPairsObserved(OpenPairsObserved),
    /// A pair as it was written to the book.
    PairOpened(PairOpened),
    /// A pair as it was marked closed.
    PairClosed(PairClosed),
    /// What the post-close sync attributed to a closed pair.
    PairAttributed(PairAttributed),
    /// An order as it was sent, written before the request leaves the process.
    OrderSubmitted(OrderSubmitted),
    /// How the broker settled that order, filled or not.
    OrderResolved(OrderResolved),
    /// A position the application asked Alpaca to close.
    PositionCloseRequested(PositionCloseRequested),
    /// The pre-close flattening, whether or not it flattened anything.
    LiquidationAttempted(LiquidationAttempted),
    /// The pre-open inference run, the artifact it resolved, and the predictions it produced.
    PredictionsGenerated(Box<PredictionsGenerated>),
    /// One activity as Alpaca reported it, fill or transfer.
    ActivityObserved(ActivityObserved),
    /// The post-close account state, which Alpaca cannot fully report again for a past date.
    AccountObserved(AccountObserved),
}

impl Observation {
    /// The stable name this observation serializes under.
    pub fn event_type(&self) -> &'static str {
        match self {
            Observation::CommandFinished(_) => "command_finished",
            Observation::PassEvaluated(_) => "pass_evaluated",
            Observation::PricesObserved(_) => "prices_observed",
            Observation::UniverseScreened(_) => "universe_screened",
            Observation::OpenPairsObserved(_) => "open_pairs_observed",
            Observation::PairOpened(_) => "pair_opened",
            Observation::PairClosed(_) => "pair_closed",
            Observation::PairAttributed(_) => "pair_attributed",
            Observation::OrderSubmitted(_) => "order_submitted",
            Observation::OrderResolved(_) => "order_resolved",
            Observation::PositionCloseRequested(_) => "position_close_requested",
            Observation::LiquidationAttempted(_) => "liquidation_attempted",
            Observation::PredictionsGenerated(_) => "predictions_generated",
            Observation::ActivityObserved(_) => "activity_observed",
            Observation::AccountObserved(_) => "account_observed",
        }
    }
}

/// One scheduled command, however it ended.
///
/// Written for every firing, including the ones that do no work: a holiday, a dropped duplicate,
/// and a crashed process are otherwise the same absence. `correlation_id` is shared with everything
/// the command did, which is what makes the duration attributable.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CommandFinished {
    pub command: String,
    /// `completed`, `errored`, `dropped_in_flight`, or `skipped_` and the handler's reason.
    pub outcome: String,
    /// Absent for a command that never ran.
    pub duration_milliseconds: Option<u64>,
    /// The rendered error, when the command failed.
    pub error: Option<String>,
    /// The handler's own summary, as it reached the completion event.
    pub summary: Option<serde_json::Value>,
}

/// One evaluation pass: what it decided and what stopped it.
///
/// The readings it acted on are their own records sharing this pass's `correlation_id` — prices,
/// the screen funnel, the open book. `candidates` stays here because a candidate's decision is not
/// known until the pass ends.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct PassEvaluated {
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
    /// The error that ended the pass early, if one did.
    ///
    /// Set on every failing path, so a pass that died after pricing the book still says what it had
    /// measured. Its absence is the claim that the pass ran to completion.
    pub failure: Option<String>,
    pub candidates: Vec<CandidateReading>,
}

/// What one price fetch returned.
///
/// Written per fetch rather than per pass: the exit half prices the open book and the entry half
/// asks only for what it is missing, and those are two separate readings of the market.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct PricesObserved {
    /// What the fetch was for — `open_pairs` or `screen_candidates`.
    pub purpose: String,
    pub readings: Vec<PriceReading>,
    pub unavailable: Vec<UnavailablePrice>,
}

/// A symbol the fetch asked for and did not get a usable price for.
///
/// Distinguishing the causes is the point: an absent price with no cause is indistinguishable from
/// a symbol nobody asked about. A `quote_rejected` row carries the book it was refused on, because
/// that is the reading the limits most need to be judged against — the guard cost the pass this
/// symbol entirely, and "how far outside" is not answerable from the cause alone.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UnavailablePrice {
    pub ticker: String,
    /// `no_quote`, `chunk_failed`, or `quote_rejected` when the guard refused the only book there
    /// was and no last trade stood behind it.
    pub cause: String,
    /// The refused book, set only on `quote_rejected`.
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub quote_timestamp: Option<DateTime<Utc>>,
    /// `stale_quote` or `wide_quote`.
    pub quote_rejection: Option<String>,
}

/// The eligibility funnel for one pass.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct UniverseScreened {
    /// Every prediction that reached the screen, as the screen received it.
    pub inputs: Vec<ScreenInputReading>,
    /// Every prediction that did not, and the first test it failed.
    pub excluded: Vec<ExcludedTickerReading>,
}

/// Every open pair as one pass measured it.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct OpenPairsObserved {
    pub readings: Vec<OpenPairReading>,
}

/// One prediction as the screen consumed it.
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

/// One prediction the eligibility filter removed, and why.
///
/// Written every pass, because `held` changes within a session even though the other tests do not.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExcludedTickerReading {
    pub ticker: String,
    /// `already_held`, `no_sector`, `no_close_history`, `outside_universe`, `unpriced`,
    /// `unusable_input`, or `structural_break`.
    pub reason: String,
    /// The reading the reason ruled on, where the name alone does not say how far outside it fell.
    ///
    /// Set for `structural_break` and absent for the set-membership tests, which have no number to
    /// report. A limit can only be moved from the readings it refused.
    pub detail: Option<String>,
}

/// One symbol's reference price, and which snapshot field it came from.
///
/// Both readings behind the price are recorded whether or not the price came from them. A reading
/// that fell back to the last trade still carries the quote that was refused and the reason, because
/// a log holding only the quotes that passed cannot say whether the limits are set anywhere near
/// right.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PriceReading {
    pub ticker: String,
    pub price: f64,
    pub price_source: String,
    /// The book as it was offered. Absent when the snapshot carried no quote at all.
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    /// When the quote was published, not when it was read — the gap between them is the staleness.
    pub quote_timestamp: Option<DateTime<Utc>>,
    /// `stale_quote` or `wide_quote`, set only when a quote existed and the guard refused it.
    pub quote_rejection: Option<String>,
    /// When the last trade printed, on the same terms as `quote_timestamp`.
    ///
    /// Unlike the quote, nothing refuses a trade for being old, so this is the only place a stale
    /// fallback price can be noticed at all. Absent when the snapshot carried no usable trade.
    pub trade_timestamp: Option<DateTime<Utc>>,
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

/// A pair as it was written to the book.
///
/// `equity_pairs` is mutated in place three times over a pair's life, so the log needs three
/// records where the table has one row. This is the only one carrying the entry rationale: nothing
/// external knows which long was paired with which short, or on what.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PairOpened {
    /// The `equity_pairs` primary key, which the two later records join on.
    pub pair_uuid: String,
    pub pair_id: String,
    pub long_ticker: String,
    pub short_ticker: String,
    pub hedge_ratio: f64,
    pub entry_z_score: f64,
    pub signal_strength: f64,
    pub model_run_id: Option<String>,
    pub opened_at: DateTime<Utc>,
    /// The prices `entry_z_score` was computed from.
    ///
    /// Recorded because the z-score alone cannot be checked after the fact: the spread model is fit
    /// from daily closes and held for the session, so a z that disagrees with the next pass's is a
    /// disagreement about these two numbers and nothing else.
    pub long_decision_price: f64,
    pub short_decision_price: f64,
    /// What the legs actually filled at. Distinct from the decision prices, and the gap between
    /// them is entry slippage.
    pub long_fill_price: f64,
    pub short_fill_price: f64,
}

/// A pair as it was marked closed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PairClosed {
    pub pair_uuid: String,
    /// `convergence`, `stop_loss`, `end_of_day`, or `position_missing`.
    pub reason: String,
    pub closed_at: DateTime<Utc>,
    /// False when the pair was already closed, so this write changed nothing.
    ///
    /// A pass racing the pre-close liquidation is the ordinary way to reach it, and the collision
    /// is worth seeing.
    pub updated: bool,
}

/// The attribution the post-close sync wrote to a closed pair.
///
/// A record of a write, not of a conclusion — the same kind of thing as [`OrderSubmitted`]. The
/// amount is derivable from [`PairOpened`], [`PairClosed`], and the session's [`ActivityObserved`]
/// rows; what is not derivable is that this process put that number on that row and that it landed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PairAttributed {
    pub pair_uuid: String,
    pub realized_profit_and_loss: Option<f64>,
    /// False when no closed pair matched, so the attribution landed nowhere.
    pub updated: bool,
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
    /// Alpaca's own status when this resolution was written, absent when nothing reached it.
    ///
    /// `outcome` is this application's word and `timed_out` is the case it cannot explain alone:
    /// an order Alpaca still held at `pending_new` never reached the market, while one at
    /// `accepted` reached it and found no contra side.
    pub broker_status: Option<String>,
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
///
/// Written whether or not the flattening succeeded. This is the last fail-safe before positions
/// carry overnight, and a run that failed at the broker used to leave no trace of having been
/// attempted at all.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct LiquidationAttempted {
    pub pairs_closed: usize,
    pub positions_refused: usize,
    pub pairs_still_open: Vec<String>,
    /// The error that ended the run early, if one did. Its absence is the claim that the run
    /// finished, not that the book is flat — `pairs_still_open` answers that.
    pub failure: Option<String>,
}

/// The pre-open inference run and everything it produced.
///
/// The quantiles are here rather than left to `equity_predictions` and the nightly export, because
/// those are the model's actual output and the log is meant to hold the originals. They arrive at
/// one moment from one place, so unlike the pass readings there is nothing to gain by splitting
/// them out.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PredictionsGenerated {
    pub model_run_id: String,
    pub artifact_key: String,
    pub artifact_staleness_sessions: Option<i64>,
    /// Rows the database accepted, which is not the prediction count when an upsert collapses a
    /// re-run.
    pub rows_written: u64,
    pub universe_size: usize,
    pub predictions: Vec<PredictionReading>,
}

/// One ticker's prediction, as the model produced it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PredictionReading {
    pub ticker: String,
    pub timestamp: DateTime<Utc>,
    pub quantile_10: f64,
    pub quantile_50: f64,
    pub quantile_90: f64,
}

/// One activity as Alpaca reported it.
///
/// Kept as our own record rather than re-fetched, because Alpaca's retention bounds how long the
/// question can be asked. `net_amount` is the reason this matters most: it is the only field
/// saying how much a deposit or withdrawal moved, and it reaches no other store the application
/// owns.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ActivityObserved {
    /// Alpaca's activity identifier, which is what makes the sync idempotent.
    pub activity_id: String,
    /// `FILL`, `CSD`, `CSW`, or `JNLC`.
    pub activity_type: String,
    pub transaction_time: DateTime<Utc>,
    pub ticker: Option<String>,
    pub side: Option<String>,
    pub quantity: Option<f64>,
    pub price: Option<f64>,
    /// Set on transfers, which carry this instead of a quantity and price.
    pub net_amount: Option<f64>,
    /// Joins a fill back to the order that produced it.
    pub order_id: Option<String>,
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

    /// The wire names are a contract: `tests/test_handlers.rs` selects records by them and the
    /// DuckDB view documents them as query filters. A typo in one arm compiles and breaks a reader.
    ///
    /// Matched exhaustively rather than iterated over a list, so a new variant does not compile
    /// until it is named here.
    #[test]
    fn test_every_observation_serializes_under_its_documented_name() {
        fn expected(observation: &Observation) -> &'static str {
            match observation {
                Observation::CommandFinished(_) => "command_finished",
                Observation::PassEvaluated(_) => "pass_evaluated",
                Observation::PricesObserved(_) => "prices_observed",
                Observation::UniverseScreened(_) => "universe_screened",
                Observation::OpenPairsObserved(_) => "open_pairs_observed",
                Observation::PairOpened(_) => "pair_opened",
                Observation::PairClosed(_) => "pair_closed",
                Observation::PairAttributed(_) => "pair_attributed",
                Observation::OrderSubmitted(_) => "order_submitted",
                Observation::OrderResolved(_) => "order_resolved",
                Observation::PositionCloseRequested(_) => "position_close_requested",
                Observation::LiquidationAttempted(_) => "liquidation_attempted",
                Observation::PredictionsGenerated(_) => "predictions_generated",
                Observation::ActivityObserved(_) => "activity_observed",
                Observation::AccountObserved(_) => "account_observed",
            }
        }

        for observation in every_observation() {
            // Against `event_type()`, which the writer logs by, and against the serialized tag,
            // which is what actually reaches the archive. The two are separate code paths.
            assert_eq!(observation.event_type(), expected(&observation));
            let value = serde_json::to_value(Record::new(
                Uuid::nil(),
                instant("2026-08-11T20:15:00Z"),
                observation.clone(),
            ))
            .expect("every observation must serialize");
            assert_eq!(
                value["event_type"],
                expected(&observation),
                "the serialized tag is what a reader filters on"
            );
        }
    }

    /// One value per variant, so the compiler forces this list to grow with the enum.
    fn every_observation() -> Vec<Observation> {
        vec![
            Observation::CommandFinished(CommandFinished {
                command: "portfolio_evaluation".to_string(),
                outcome: "completed".to_string(),
                duration_milliseconds: Some(12),
                error: None,
                summary: None,
            }),
            Observation::PassEvaluated(Box::default()),
            Observation::PricesObserved(PricesObserved::default()),
            Observation::UniverseScreened(UniverseScreened::default()),
            Observation::OpenPairsObserved(OpenPairsObserved::default()),
            Observation::PairOpened(PairOpened {
                pair_uuid: Uuid::nil().to_string(),
                pair_id: "AAAA-BBBB".to_string(),
                long_ticker: "AAAA".to_string(),
                short_ticker: "BBBB".to_string(),
                hedge_ratio: 1.0,
                entry_z_score: 2.5,
                signal_strength: 0.03,
                model_run_id: None,
                opened_at: instant("2026-08-11T14:35:00Z"),
                long_decision_price: 100.0,
                short_decision_price: 50.0,
                long_fill_price: 100.05,
                short_fill_price: 49.95,
            }),
            Observation::PairClosed(PairClosed {
                pair_uuid: Uuid::nil().to_string(),
                reason: "convergence".to_string(),
                closed_at: instant("2026-08-11T18:00:00Z"),
                updated: true,
            }),
            Observation::PairAttributed(PairAttributed {
                pair_uuid: Uuid::nil().to_string(),
                realized_profit_and_loss: Some(100.0),
                updated: true,
            }),
            Observation::OrderSubmitted(OrderSubmitted {
                client_order_id: "k-long".to_string(),
                ticker: "AAAA".to_string(),
                side: "buy".to_string(),
                shares: None,
                notional: Some(1_000.0),
            }),
            Observation::OrderResolved(OrderResolved {
                client_order_id: "k-long".to_string(),
                alpaca_order_id: Some("o1".to_string()),
                ticker: "AAAA".to_string(),
                outcome: "filled".to_string(),
                filled_shares: Some(10.0),
                filled_average_price: Some(100.0),
                filled_after_cancel: false,
                broker_status: Some("filled".to_string()),
                error: None,
            }),
            Observation::PositionCloseRequested(PositionCloseRequested {
                ticker: "AAAA".to_string(),
                pair_id: None,
                alpaca_order_id: None,
                side: None,
                quantity: None,
                reason: "liquidation".to_string(),
                accepted: true,
                status: Some(200),
                error: None,
            }),
            Observation::LiquidationAttempted(LiquidationAttempted::default()),
            Observation::PredictionsGenerated(Box::new(PredictionsGenerated {
                model_run_id: "run-1".to_string(),
                artifact_key: "models/tide/run-1".to_string(),
                artifact_staleness_sessions: None,
                rows_written: 0,
                universe_size: 0,
                predictions: Vec::new(),
            })),
            Observation::ActivityObserved(ActivityObserved {
                activity_id: "a1".to_string(),
                activity_type: "FILL".to_string(),
                transaction_time: instant("2026-08-11T18:00:00Z"),
                ticker: Some("AAAA".to_string()),
                side: Some("buy".to_string()),
                quantity: Some(10.0),
                price: Some(100.0),
                net_amount: None,
                order_id: Some("o1".to_string()),
            }),
            account_observation(),
        ]
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
            broker_status: Some("filled".to_string()),
            error: None,
        };
        let value = serde_json::to_value(&filled).expect("fill must serialize");
        let object = value.as_object().expect("a fill is an object");
        assert!(!object.contains_key("slippage"));
        assert!(!object.contains_key("realized_profit_and_loss"));
    }

    /// A refused quote has to survive into the record. A log holding only the books that passed
    /// says nothing about where the limits should sit, which is the whole reason they are recorded.
    #[test]
    fn test_a_refused_quote_is_recorded_with_the_book_that_was_refused() {
        let refused = PriceReading {
            ticker: "AER".to_string(),
            price: 150.60,
            price_source: "last_trade".to_string(),
            bid_price: Some(128.0),
            ask_price: Some(150.86),
            quote_timestamp: Some(instant("2026-08-12T14:40:00Z")),
            quote_rejection: Some("wide_quote".to_string()),
            trade_timestamp: Some(instant("2026-08-12T13:12:00Z")),
        };

        let value = serde_json::to_value(&refused).expect("a reading must serialize");
        assert_eq!(value["price_source"], "last_trade");
        assert_eq!(value["quote_rejection"], "wide_quote");
        assert_eq!(value["bid_price"], 128.0);
        assert_eq!(value["ask_price"], 150.86);
        assert!(
            value["quote_timestamp"].is_string(),
            "staleness is the gap between the quote and the read, so the quote's own time is kept"
        );
        assert_eq!(value["trade_timestamp"], "2026-08-12T13:12:00Z");
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
