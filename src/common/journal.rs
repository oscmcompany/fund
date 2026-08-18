//! Append-only record of what the application observed before it acted.
//!
//! One JSONL file per session on local disk, and the only original this application owns — every
//! other store is a fold over it. Sealing and shipping it is [`crate::data::export`]'s.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};
use uuid::Uuid;

use crate::common::alpaca::{ActivityType, OrderSide, PositionSide, PriceSource, QuoteRejection};
use crate::common::events::Command;
use crate::common::types::{CloseReason, Dataset, PairID, SessionDate, Ticker};

/// Version stamped on every record written by this build.
///
/// Readers map old versions forward rather than rewriting files, so this only ever goes up. What
/// each version held is documented beside the DuckDB view in `tools/duckdb_initialization.sql`.
pub const SCHEMA_VERSION: u32 = 5;

/// Anything that stops a record reaching the disk.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("journal directory {directory} is unusable: {source}")]
    Directory {
        directory: String,
        #[source]
        source: std::io::Error,
    },
    #[error("journal write failed: {0}")]
    Write(#[from] std::io::Error),
    #[error("observation could not be serialized: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// One observation, tagged with what kind it is.
///
/// Observations only — no slippage, profit and loss, or exposure totals, each of which is a query
/// over these rows. Variants are additive and past files are never rewritten.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "event_type", content = "payload", rename_all = "snake_case")]
pub enum Observation {
    CommandFinished(CommandFinished),
    PassEvaluated(Box<PassEvaluated>),
    PricesObserved(PricesObserved),
    UniverseScreened(UniverseScreened),
    OpenPairsObserved(OpenPairsObserved),
    PlanDecided(PlanDecided),
    PairOpened(PairOpened),
    PairClosed(PairClosed),
    PairAttributed(PairAttributed),
    OrderSubmitted(OrderSubmitted),
    OrderResolved(OrderResolved),
    PositionCloseRequested(PositionCloseRequested),
    LiquidationAttempted(LiquidationAttempted),
    PredictionsGenerated(Box<PredictionsGenerated>),
    ActivityObserved(ActivityObserved),
    AccountObserved(AccountObserved),
    PositionsObserved(PositionsObserved),
    BarsIngested(BarsIngested),
    UniverseRefreshed(UniverseRefreshed),
    CalendarObserved(CalendarObserved),
    JournalExported(JournalExported),
    DatabaseExported(DatabaseExported),
    LogsExported(LogsExported),
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
            Observation::PlanDecided(_) => "plan_decided",
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
            Observation::PositionsObserved(_) => "positions_observed",
            Observation::BarsIngested(_) => "bars_ingested",
            Observation::UniverseRefreshed(_) => "universe_refreshed",
            Observation::CalendarObserved(_) => "calendar_observed",
            Observation::JournalExported(_) => "journal_exported",
            Observation::DatabaseExported(_) => "database_exported",
            Observation::LogsExported(_) => "logs_exported",
        }
    }
}

/// Why a scheduled command did no work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    NotATradingDay,
    MarketHalted,
}

impl SkipReason {
    pub const ALL: [SkipReason; 2] = [SkipReason::NotATradingDay, SkipReason::MarketHalted];

    pub fn as_str(self) -> &'static str {
        match self {
            SkipReason::NotATradingDay => "not_a_trading_day",
            SkipReason::MarketHalted => "market_halted",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        SkipReason::ALL.into_iter().find(|it| it.as_str() == raw)
    }
}

/// How a scheduled command ended.
///
/// `Skipped` carries its reason rather than collapsing to a flag, because a holiday and a halt are
/// the same absence of work for opposite causes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutcome {
    Completed,
    Errored,
    /// The process was shutting down and never ran it.
    DroppedInFlight,
    Skipped(SkipReason),
}

impl CommandOutcome {
    pub fn as_string(self) -> String {
        match self {
            CommandOutcome::Completed => "completed".to_string(),
            CommandOutcome::Errored => "errored".to_string(),
            CommandOutcome::DroppedInFlight => "dropped_in_flight".to_string(),
            CommandOutcome::Skipped(reason) => format!("skipped_{}", reason.as_str()),
        }
    }
}

impl Serialize for CommandOutcome {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_string())
    }
}

/// One scheduled command, however it ended.
///
/// Written for every firing, including the ones that do no work: a holiday, a dropped duplicate,
/// and a crashed process are otherwise the same absence. `correlation_id` is shared with everything
/// the command did, which is what makes the duration attributable.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CommandFinished {
    pub command: Command,
    pub outcome: CommandOutcome,
    /// Absent for a command that never ran.
    pub duration_milliseconds: Option<u64>,
    pub error: Option<String>,
    pub summary: Option<serde_json::Value>,
}

/// One evaluation pass: what it decided and what stopped it.
///
/// The readings it acted on are their own records sharing this pass's `correlation_id` — prices,
/// the screen funnel, the open book. `candidates` stays here because a candidate's decision is not
/// known until the pass ends.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct PassEvaluated {
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub account_equity: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub previous_session_equity: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub gross_exposure_used: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub gross_exposure_cap: Option<Decimal>,
    pub drawdown: Option<f64>,
    pub minutes_until_close: Option<i64>,
    pub open_pairs_at_start: usize,
    pub vacant_slots: Option<usize>,
    pub universe_size: usize,
    pub predictions_available: usize,
    pub eligible_tickers: usize,
    pub candidates_screened: usize,
    pub model_run_id: Option<String>,
    /// The risk gate's rendered reason for not running the entry half at all.
    pub session_block: Option<String>,
    /// The error that ended the pass early, if one did.
    ///
    /// Set on every failing path, so a pass that died after pricing the book still says what it had
    /// measured. Its absence is the claim that the pass ran to completion.
    pub error: Option<String>,
    pub candidates: Vec<CandidateReading>,
}

/// Which of a pass's two price fetches a reading came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PricePurpose {
    /// The exit half, pricing the book it already holds.
    OpenPairs,
    /// The entry half, pricing what the screen is missing.
    ScreenCandidates,
}

/// What one price fetch returned.
///
/// Written per fetch rather than per pass: the exit half prices the open book and the entry half
/// asks only for what it is missing, and those are two separate readings of the market.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PricesObserved {
    pub purpose: PricePurpose,
    pub readings: Vec<PriceReading>,
    pub unavailable: Vec<UnavailablePrice>,
}

/// Why a symbol the fetch asked for came back without a usable price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableCause {
    /// Alpaca returned the symbol with neither a quote nor a trade.
    NoQuote,
    /// The request carrying the symbol failed, so it was never asked about.
    ChunkFailed,
    /// The guard refused the only book there was, with no last trade behind it.
    QuoteRejected,
}

/// A symbol the fetch asked for and did not get a usable price for.
///
/// Distinguishing the causes is the point: an absent price with no cause is indistinguishable from
/// a symbol nobody asked about. A `quote_rejected` row carries the book it was refused on, because
/// that is the reading the limits most need to be judged against — the guard cost the pass this
/// symbol entirely, and "how far outside" is not answerable from the cause alone.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UnavailablePrice {
    pub ticker: Ticker,
    pub cause: UnavailableCause,
    /// The refused book, set only on `quote_rejected`.
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub quote_timestamp: Option<DateTime<Utc>>,
    pub quote_rejection: Option<QuoteRejection>,
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

/// Which round of a pass a plan belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanPhase {
    Exits,
    Entries,
}

/// What a planned action would do to a pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannedActionKind {
    Close,
    Open,
}

/// Why a planned action was chosen.
///
/// The two arms are not the same kind of answer: an exit is chosen by a rule, an entry by its
/// position in an ordering, and collapsing both into one string made an empty plan unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedReason {
    Close(CloseReason),
    /// The entry's one-based rank among the candidates that cleared.
    Rank(u32),
}

impl Serialize for PlannedReason {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            PlannedReason::Close(reason) => serializer.serialize_str(reason.as_str()),
            PlannedReason::Rank(rank) => serializer.serialize_str(&format!("rank_{rank}")),
        }
    }
}

/// What one round of a pass resolved to do, written before any of it is attempted.
///
/// Its own record rather than a field on [`PassEvaluated`], which is written when the pass ends: a
/// plan on that row would reach disk only after acting, so a process that died mid-execution would
/// leave what it intended unrecorded.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlanDecided {
    pub phase: PlanPhase,
    pub actions: Vec<PlannedAction>,
    /// Everything the round weighed, actions included. Subtract `actions.len()` for what it passed
    /// over, which is what makes an empty plan legible.
    pub considered: usize,
}

/// One thing a plan calls for, carrying enough to reconstruct the attempt if it never completes.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlannedAction {
    pub pair_id: PairID,
    pub action: PlannedActionKind,
    pub reason: PlannedReason,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    /// Present for an entry, which is sized before it is sent.
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub long_notional: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub short_quantity: Option<Decimal>,
}

/// One prediction as the screen consumed it.
///
/// Derived from the stored quantiles and the session's universe, so not recoverable from
/// `equity_predictions` alone.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScreenInputReading {
    pub ticker: Ticker,
    pub expected_return: f64,
    pub confidence: f64,
    pub is_shortable: bool,
}

/// Which eligibility test a prediction failed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionReason {
    AlreadyHeld,
    NoSector,
    NoCloseHistory,
    OutsideUniverse,
    Unpriced,
    UnusableInput,
    StructuralBreak,
}

/// One prediction the eligibility filter removed, and why.
///
/// Written every pass, because `already_held` changes within a session even though the other tests
/// do not.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExcludedTickerReading {
    pub ticker: Ticker,
    pub reason: ExclusionReason,
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
    pub ticker: Ticker,
    pub price: f64,
    pub price_source: PriceSource,
    /// The book as it was offered. Absent when the snapshot carried no quote at all.
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    /// When the quote was published, not when it was read — the gap between them is the staleness.
    pub quote_timestamp: Option<DateTime<Utc>>,
    /// Set only when a quote existed and the guard refused it.
    pub quote_rejection: Option<QuoteRejection>,
    /// When the last trade printed, on the same terms as `quote_timestamp`.
    ///
    /// Unlike the quote, nothing refuses a trade for being old, so this is the only place a stale
    /// fallback price can be noticed at all. Absent when the snapshot carried no usable trade.
    pub trade_timestamp: Option<DateTime<Utc>>,
}

/// What a pass resolved to do about one open pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairDecision {
    /// Kept, because nothing said otherwise.
    Held,
    Convergence,
    StopLoss,
    EndOfDay,
    PositionMissing,
    /// No usable price for at least one leg.
    Unpriced,
    /// The session's spread model does not cover this pair.
    NoSpreadModel,
    /// The spread model covers it but its statistics could not be read.
    UnreadableSpread,
    /// A close was decided and the broker refused it.
    CloseFailed,
}

impl From<CloseReason> for PairDecision {
    fn from(reason: CloseReason) -> Self {
        match reason {
            CloseReason::Convergence => PairDecision::Convergence,
            CloseReason::StopLoss => PairDecision::StopLoss,
            CloseReason::EndOfDay => PairDecision::EndOfDay,
            CloseReason::PositionMissing => PairDecision::PositionMissing,
        }
    }
}

/// An open pair as this pass measured it, whether or not it closed.
///
/// Carries every input to the z-score, which on its own cannot distinguish a price move from a
/// refit.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OpenPairReading {
    pub pair_id: PairID,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    pub stored_hedge_ratio: f64,
    pub model_hedge_ratio: Option<f64>,
    pub spread_mean: Option<f64>,
    pub spread_standard_deviation: Option<f64>,
    pub z_score: Option<f64>,
    pub entry_z_score: f64,
    pub stop_at: f64,
    pub entry_session: SessionDate,
    pub minutes_held: i64,
    pub decision: PairDecision,
}

/// What became of a scored candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateDecision {
    Opened,
    /// Selected and written into the plan, but the pass ended before it was attempted.
    Planned,
    NotSelected,
    RiskRefused,
    Unfilled,
    AbandonedAtShutdown,
}

/// A scored candidate and what became of it.
///
/// The sizing fields are set for every candidate that reached the sizer, refused ones included.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CandidateReading {
    pub pair_id: PairID,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    pub hedge_ratio: f64,
    pub entry_z_score: f64,
    pub signal_strength: f64,
    pub rank_score: f64,
    /// Dollars the long leg was sized to, absent for a candidate that was never selected.
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub long_notional: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub short_quantity: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub gross_exposure: Option<Decimal>,
    pub decision: CandidateDecision,
    /// The risk gate's rendered reason, when `decision` is `risk_refused`.
    pub refusal: Option<String>,
}

/// A pair as it was written to the book.
///
/// `equity_pairs` is mutated in place three times over a pair's life, so the journal needs three
/// records where the table has one row. This is the only one carrying the entry rationale: nothing
/// external knows which long was paired with which short, or on what.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PairOpened {
    /// The `equity_pairs` primary key, which the two later records join on.
    pub equity_pair_id: Uuid,
    pub pair_id: PairID,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
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
    #[serde(with = "crate::common::types::decimal_number")]
    pub long_fill_price: Decimal,
    #[serde(with = "crate::common::types::decimal_number")]
    pub short_fill_price: Decimal,
}

/// A pair as it was marked closed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PairClosed {
    pub equity_pair_id: Uuid,
    pub reason: CloseReason,
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
    pub equity_pair_id: Uuid,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub realized_profit_and_loss: Option<Decimal>,
    /// False when no closed pair matched, so the attribution landed nowhere.
    pub updated: bool,
}

/// An order at the moment it was sent, before the broker has said anything about it.
///
/// Keyed by `client_order_id` because this is written before the request leaves the process, when
/// the broker's identifier does not exist yet.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderSubmitted {
    pub client_order_id: Uuid,
    pub ticker: Ticker,
    pub side: OrderSide,
    /// Set for the short leg, which is sized in shares.
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub quantity: Option<Decimal>,
    /// Set for the long leg, which is sized in dollars.
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub notional: Option<Decimal>,
}

/// How an order ended, in this application's vocabulary.
///
/// `TimedOut` is our word and not the broker's: it is the case `broker_status` exists to explain,
/// since an order Alpaca still held at `pending_new` never reached the market while one at
/// `accepted` reached it and found no contra side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderOutcome {
    Filled,
    /// The request itself was refused, so nothing reached the market.
    SubmitFailed,
    /// The request could not be sent or its answer could not be read.
    BrokerUnreachable,
    /// This process stopped waiting before the order reached a terminal state.
    TimedOut,
    /// Alpaca ended it without a complete fill.
    Abandoned,
}

/// How the broker settled an order.
///
/// One follows every [`OrderSubmitted`], so an unresolved submission means the process died between
/// the two. Slippage is absent: it is this row joined to the pass that produced the order.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderResolved {
    pub client_order_id: Uuid,
    /// Absent when the order never reached the broker.
    pub alpaca_order_id: Option<String>,
    pub ticker: Ticker,
    pub outcome: OrderOutcome,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub filled_quantity: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub filled_average_price: Option<Decimal>,
    /// True when the fill was read after a cancel raced it.
    pub filled_after_cancel: bool,
    /// Alpaca's own status when this resolution was written, absent when nothing reached it.
    pub broker_status: Option<String>,
    pub error: Option<String>,
}

/// Why a position was asked to close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseRequestReason {
    /// One leg of a pair the exit half decided to close.
    PairExit,
    /// A leg whose partner never filled, being unwound.
    EntryUnwind,
    /// The pre-close flattening, which does not consult the pair book.
    Liquidation,
}

/// One position the application asked Alpaca to close.
///
/// Unlike an entry, an exit is not polled to a terminal state, so there is no fill price here —
/// `alpaca_order_id` is the join to the fill once the post-close activity sync lands it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PositionCloseRequested {
    pub ticker: Ticker,
    /// The pair this leg belonged to, absent when the account is flattened without consulting them.
    pub pair_id: Option<PairID>,
    /// Absent when there was no position, or when Alpaca accepted the close and returned a body
    /// this client could not read.
    pub alpaca_order_id: Option<String>,
    pub side: Option<OrderSide>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub quantity: Option<Decimal>,
    pub reason: CloseRequestReason,
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
    pub pairs_still_open: Vec<PairID>,
    /// The error that ended the run early, if one did. Its absence is the claim that the run
    /// finished, not that the book is flat — `pairs_still_open` answers that.
    pub error: Option<String>,
}

/// The pre-open inference run and everything it produced.
///
/// The quantiles are here rather than left to `equity_predictions` and the nightly export, because
/// those are the model's actual output and the journal is meant to hold the originals. They arrive
/// at one moment from one place, so unlike the pass readings there is nothing to gain by splitting
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
    pub ticker: Ticker,
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
    pub activity_type: ActivityType,
    pub transaction_time: DateTime<Utc>,
    pub ticker: Option<Ticker>,
    pub side: Option<OrderSide>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub quantity: Option<Decimal>,
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub price: Option<Decimal>,
    /// Set on transfers, which carry this instead of a quantity and price.
    #[serde(with = "crate::common::types::decimal_number_option")]
    pub net_amount: Option<Decimal>,
    /// Joins a fill back to the order that produced it.
    pub alpaca_order_id: Option<String>,
}

/// The post-close account state.
///
/// Recorded in full because Alpaca backfills only equity for a past date, never the rest. The
/// balances are `Decimal`, so nothing between Alpaca's response and this record rounds.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountObserved {
    /// The session these balances describe, which is not always the one the record was written in:
    /// a post-close sync re-run after Eastern midnight observes yesterday from today.
    pub session_date: SessionDate,
    #[serde(with = "crate::common::types::decimal_number")]
    pub equity: Decimal,
    #[serde(with = "crate::common::types::decimal_number")]
    pub cash: Decimal,
    #[serde(with = "crate::common::types::decimal_number")]
    pub buying_power: Decimal,
    #[serde(with = "crate::common::types::decimal_number")]
    pub long_market_value: Decimal,
    #[serde(with = "crate::common::types::decimal_number")]
    pub short_market_value: Decimal,
}

/// The position book as Alpaca reported it.
///
/// The companion to [`AccountObserved`], and recorded for the same reason: Alpaca answers what is
/// held now and never what was held on a past date, so a session whose book went unrecorded cannot
/// have it reconstructed from anything but these rows.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct PositionsObserved {
    pub readings: Vec<PositionReading>,
    /// Rows Alpaca reported that could not be read, so an empty book and an unreadable one are
    /// distinguishable.
    pub unreadable: usize,
}

/// One held position, as Alpaca reported it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PositionReading {
    pub ticker: Ticker,
    pub side: PositionSide,
    /// Always positive; the direction is `side`.
    #[serde(with = "crate::common::types::decimal_number")]
    pub quantity: Decimal,
    #[serde(with = "crate::common::types::decimal_number")]
    pub market_value: Decimal,
    #[serde(with = "crate::common::types::decimal_number")]
    pub unrealized_profit_and_loss: Decimal,
}

/// One session's bar sync, from the whole-market feed to the archive.
///
/// The bar path is the most consequential thing the application does that the broker cannot be
/// re-asked about: Massive serves a date once and the archive partition is frozen afterwards.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BarsIngested {
    /// The session the sync ran for, which is not the only session it covers: the window reaches
    /// back far enough to close a gap left by a missed run.
    pub session_date: SessionDate,
    pub sessions_requested: usize,
    /// Sessions the provider did not answer for, which are the gaps a later run has to close.
    pub sessions_failed: Vec<SessionDate>,
    /// Rows that became a validated bar. A row the provider sent and this build could not read is
    /// dropped inside the parse, so this is the count that reached the database.
    pub bars_parsed: usize,
    pub rows_written: u64,
    pub error: Option<String>,
}

/// One universe refresh: what became eligible to trade, and what fell out.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct UniverseRefreshed {
    /// Symbols Alpaca reports as tradable, before the liquidity screen.
    pub alpaca_tradable: usize,
    pub alpaca_shortable: usize,
    /// Symbols clearing the liquidity thresholds.
    pub liquid: usize,
    pub universe_size: usize,
    /// Tickers this refresh admitted that the previous universe did not hold.
    pub admitted: Vec<Ticker>,
    /// Tickers the previous universe held that this refresh did not admit.
    pub removed: Vec<Ticker>,
    pub error: Option<String>,
}

/// The exchange calendar as Alpaca published it.
///
/// Every command consults this before doing anything, and the early closes are the part no local
/// table can hold: a half-day's real 13:00 close is knowable only from this fetch, and the entry
/// gate sizes its remaining-minutes check against it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CalendarObserved {
    pub horizon_start: SessionDate,
    pub horizon_end: SessionDate,
    pub sessions: usize,
    pub trades_today: bool,
    /// Sessions in the horizon closing before 16:00 Eastern, with the close Alpaca published.
    pub early_closes: Vec<EarlyClose>,
}

/// One published session that ends before the usual bell.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EarlyClose {
    pub session_date: SessionDate,
    /// The Eastern wall-clock close, as Alpaca publishes it.
    pub session_close: NaiveTime,
}

/// One run of the seal-ship-delete cycle over the journal itself.
///
/// Written after the seal releases, so it lands in the session the export ran in rather than one
/// it just shipped, and reaches S3 only on the next run.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct JournalExported {
    pub sessions_exported: usize,
    pub records_exported: usize,
    /// Sessions whose upload failed, which stay on local disk for the next run.
    pub sessions_failed: Vec<SessionDate>,
    /// Sessions deleted from local disk, which had uploaded cleanly and aged out.
    pub sessions_deleted: Vec<SessionDate>,
    /// Lines the Parquet does not hold. A session with any is never deleted, so its original
    /// remains the only copy of them.
    pub unparsable_lines: usize,
}

/// One run of the seal-free ship-delete cycle over the diagnostic logs.
///
/// Unlike [`JournalExported`], today's files are still open: what ships is a snapshot the next run
/// replaces, so a count here rising for the same date is ordinary rather than a duplicate.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct LogsExported {
    pub files_exported: usize,
    pub lines_exported: usize,
    /// Services whose upload failed, which stay on local disk for the next run.
    pub files_failed: usize,
    /// Dates deleted from local disk, which had uploaded cleanly and aged out.
    pub dates_deleted: Vec<NaiveDate>,
    /// Lines the Parquet does not hold, which keep their file from being deleted.
    pub unparsable_lines: usize,
}

/// One run of the nightly database export and the purge chained behind it.
///
/// The purge is gated on the export being clean, so `rows_purged` being absent while datasets
/// exported is the skip rather than a failure — `purge_skipped` is what tells the two apart.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatabaseExported {
    pub session_date: SessionDate,
    /// `(dataset, rows)` for each table written to S3.
    pub exported: Vec<(Dataset, usize)>,
    /// `(dataset, error)` for each table that did not write.
    pub failed: Vec<(Dataset, String)>,
    /// `(dataset, rows)` deleted from PostgreSQL once S3 held them.
    pub purged: Vec<(Dataset, u64)>,
    /// True when the export was incomplete and the purge was therefore not attempted.
    pub purge_skipped: bool,
}

/// One line of the journal: an observation with the envelope that makes it addressable.
///
/// `session_date` is derived from `timestamp`, so a record cannot be filed under a session it did
/// not happen in.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Record {
    pub schema_version: u32,
    pub event_id: Uuid,
    /// Threads a pass to the orders it caused to the fills they produced.
    pub correlation_id: Uuid,
    pub session_date: SessionDate,
    pub timestamp: DateTime<Utc>,
    #[serde(flatten)]
    pub observation: Observation,
}

impl Record {
    /// Stamps an observation with a fresh identity at `timestamp`.
    pub fn new(correlation_id: Uuid, timestamp: DateTime<Utc>, observation: Observation) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            event_id: Uuid::new_v4(),
            correlation_id,
            session_date: SessionDate::at(timestamp),
            timestamp,
            observation,
        }
    }
}

/// The file the writer currently holds open, and the session it belongs to.
struct OpenSession {
    session_date: SessionDate,
    file: tokio::fs::File,
}

/// Appends records to the current session's file.
///
/// The file rolls when the Eastern date does, so the session comes from the record rather than from
/// construction.
pub struct Journal {
    directory: PathBuf,
    open_session: tokio::sync::Mutex<Option<OpenSession>>,
}

impl Journal {
    /// Opens a journal against `directory`, creating it if needed.
    pub fn new(directory: impl Into<PathBuf>) -> Result<Self, JournalError> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory).map_err(|source| JournalError::Directory {
            directory: directory.display().to_string(),
            source,
        })?;
        Ok(Self {
            directory,
            open_session: tokio::sync::Mutex::new(None),
        })
    }

    /// Opens a journal at `FUND_JOURNAL_DIRECTORY`, or `/var/journal/fund`.
    pub fn from_env() -> Result<Self, JournalError> {
        let directory = std::env::var("FUND_JOURNAL_DIRECTORY")
            .unwrap_or_else(|_| DEFAULT_JOURNAL_DIRECTORY.to_string());
        Self::new(directory)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Appends one record and returns once it is fsynced to the disk.
    ///
    /// A caller that awaits this before acting knows the observation survives the crash the action
    /// might cause.
    pub async fn append(&self, record: &Record) -> Result<(), JournalError> {
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
    /// A journal write must not become a new way for the fund to stop trading, so acting unobserved
    /// beats refusing to act.
    pub async fn record(
        &self,
        correlation_id: Uuid,
        timestamp: DateTime<Utc>,
        observation: Observation,
    ) {
        let record = Record::new(correlation_id, timestamp, observation);
        let event_type = record.observation.event_type();
        match self.append(&record).await {
            Ok(()) => debug!(event_type, %correlation_id, "Journal record written"),
            Err(error) => error!(
                event_type,
                %correlation_id,
                %error,
                "Journal record could not be written; the action proceeds unobserved"
            ),
        }
    }

    /// Blocks appends for as long as the returned guard is held.
    ///
    /// The export takes this before reading, so no reader sees a half-written line. The open handle
    /// is dropped, so the next append reopens the file.
    pub async fn seal(&self) -> JournalGuard<'_> {
        let mut open_session = self.open_session.lock().await;
        *open_session = None;
        JournalGuard {
            _appends_blocked: open_session,
        }
    }
}

/// Proof that no append can run while it is alive.
///
/// Opaque on purpose: holding it is the whole contract, and there is nothing inside worth reading.
pub struct JournalGuard<'a> {
    _appends_blocked: tokio::sync::MutexGuard<'a, Option<OpenSession>>,
}

/// Where the journal lives when nothing overrides it.
///
/// Its own tree rather than a subdirectory of the log directory: this is data, and the retention,
/// permissions, and backup a log directory gets are the wrong ones for the only original the
/// application owns.
pub const DEFAULT_JOURNAL_DIRECTORY: &str = "/var/journal/fund";

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

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).expect("valid ticker")
    }

    fn decimal(raw: &str) -> Decimal {
        raw.parse().expect("valid decimal")
    }

    fn account_observation() -> Observation {
        Observation::AccountObserved(AccountObserved {
            session_date: session(2026, 8, 11),
            equity: decimal("104812.55"),
            cash: decimal("12000.07"),
            buying_power: decimal("200000.00"),
            long_market_value: decimal("50000.00"),
            short_market_value: decimal("-50000.01"),
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
                Observation::PlanDecided(_) => "plan_decided",
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
                Observation::PositionsObserved(_) => "positions_observed",
                Observation::BarsIngested(_) => "bars_ingested",
                Observation::UniverseRefreshed(_) => "universe_refreshed",
                Observation::CalendarObserved(_) => "calendar_observed",
                Observation::JournalExported(_) => "journal_exported",
                Observation::DatabaseExported(_) => "database_exported",
                Observation::LogsExported(_) => "logs_exported",
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
                command: Command::PortfolioEvaluation,
                outcome: CommandOutcome::Completed,
                duration_milliseconds: Some(12),
                error: None,
                summary: None,
            }),
            Observation::PassEvaluated(Box::default()),
            Observation::PricesObserved(PricesObserved {
                purpose: PricePurpose::OpenPairs,
                readings: Vec::new(),
                unavailable: Vec::new(),
            }),
            Observation::UniverseScreened(UniverseScreened::default()),
            Observation::OpenPairsObserved(OpenPairsObserved::default()),
            Observation::PlanDecided(PlanDecided {
                phase: PlanPhase::Entries,
                actions: vec![PlannedAction {
                    pair_id: PairID::new(ticker("AAAA"), ticker("BBBB")),
                    action: PlannedActionKind::Open,
                    reason: PlannedReason::Rank(1),
                    long_ticker: ticker("AAAA"),
                    short_ticker: ticker("BBBB"),
                    long_notional: Some(decimal("1000.00")),
                    short_quantity: Some(decimal("12")),
                }],
                considered: 4,
            }),
            Observation::PairOpened(PairOpened {
                equity_pair_id: Uuid::nil(),
                pair_id: PairID::new(ticker("AAAA"), ticker("BBBB")),
                long_ticker: ticker("AAAA"),
                short_ticker: ticker("BBBB"),
                hedge_ratio: 1.0,
                entry_z_score: 2.5,
                signal_strength: 0.03,
                model_run_id: None,
                opened_at: instant("2026-08-11T14:35:00Z"),
                long_decision_price: 100.0,
                short_decision_price: 50.0,
                long_fill_price: decimal("100.05"),
                short_fill_price: decimal("49.95"),
            }),
            Observation::PairClosed(PairClosed {
                equity_pair_id: Uuid::nil(),
                reason: CloseReason::Convergence,
                closed_at: instant("2026-08-11T18:00:00Z"),
                updated: true,
            }),
            Observation::PairAttributed(PairAttributed {
                equity_pair_id: Uuid::nil(),
                realized_profit_and_loss: Some(decimal("100.00")),
                updated: true,
            }),
            Observation::OrderSubmitted(OrderSubmitted {
                client_order_id: Uuid::nil(),
                ticker: ticker("AAAA"),
                side: OrderSide::Buy,
                quantity: None,
                notional: Some(decimal("1000.00")),
            }),
            Observation::OrderResolved(OrderResolved {
                client_order_id: Uuid::nil(),
                alpaca_order_id: Some("o1".to_string()),
                ticker: ticker("AAAA"),
                outcome: OrderOutcome::Filled,
                filled_quantity: Some(decimal("10")),
                filled_average_price: Some(decimal("100.00")),
                filled_after_cancel: false,
                broker_status: Some("filled".to_string()),
                error: None,
            }),
            Observation::PositionCloseRequested(PositionCloseRequested {
                ticker: ticker("AAAA"),
                pair_id: None,
                alpaca_order_id: None,
                side: None,
                quantity: None,
                reason: CloseRequestReason::Liquidation,
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
                activity_type: ActivityType::Fill,
                transaction_time: instant("2026-08-11T18:00:00Z"),
                ticker: Some(ticker("AAAA")),
                side: Some(OrderSide::Buy),
                quantity: Some(decimal("10")),
                price: Some(decimal("100.00")),
                net_amount: None,
                alpaca_order_id: Some("o1".to_string()),
            }),
            account_observation(),
            Observation::PositionsObserved(PositionsObserved::default()),
            Observation::BarsIngested(BarsIngested {
                session_date: session(2026, 8, 11),
                sessions_requested: 3,
                sessions_failed: Vec::new(),
                bars_parsed: 9_800,
                rows_written: 9_800,
                error: None,
            }),
            Observation::UniverseRefreshed(UniverseRefreshed::default()),
            Observation::CalendarObserved(CalendarObserved {
                horizon_start: session(2026, 7, 1),
                horizon_end: session(2026, 9, 30),
                sessions: 64,
                trades_today: true,
                early_closes: vec![EarlyClose {
                    session_date: session(2026, 7, 3),
                    session_close: NaiveTime::from_hms_opt(13, 0, 0).expect("a valid wall clock"),
                }],
            }),
            Observation::JournalExported(JournalExported::default()),
            Observation::DatabaseExported(DatabaseExported {
                session_date: session(2026, 8, 17),
                exported: vec![(Dataset::Events, 240)],
                failed: Vec::new(),
                purged: vec![(Dataset::Events, 180)],
                purge_skipped: false,
            }),
            Observation::LogsExported(LogsExported::default()),
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
            "fund-journal-{name}-{}-{unique}",
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

        assert_eq!(value["schema_version"], Value::Number(5.into()));
        assert_eq!(value["event_type"], "account_observed");
        assert_eq!(value["session_date"], "2026-08-11");
        assert_eq!(value["timestamp"], "2026-08-11T20:15:00Z");
        assert!(
            value.get("event_id").and_then(Value::as_str).is_some(),
            "every record is addressable by its own identifier"
        );
    }

    /// Balances reach the archive as numbers rather than quoted strings, so a reader can aggregate
    /// them without casting. The value is the one Alpaca sent: nothing between the response and
    /// this record rounds it.
    #[test]
    fn test_account_balances_are_recorded_as_numbers() {
        let value =
            serde_json::to_value(account_observation()).expect("an observation must serialize");
        assert_eq!(value["payload"]["equity"], 104_812.55);
        assert_eq!(value["payload"]["short_market_value"], -50_000.01);
        assert!(
            value["payload"]["cash"].is_number(),
            "a balance is a number, not a string a query would have to cast"
        );
    }

    /// The field cannot be absent. Its `Option<f64>` predecessor could be, for a conversion that
    /// never actually fails, which put a null in the archive where a balance belonged.
    #[test]
    fn test_a_balance_is_never_absent() {
        let value =
            serde_json::to_value(account_observation()).expect("an observation must serialize");
        for field in [
            "equity",
            "cash",
            "buying_power",
            "long_market_value",
            "short_market_value",
        ] {
            assert!(!value["payload"][field].is_null(), "field: {field}");
        }
    }

    /// No conclusions in the journal. Storing a computed value would create a second thing that can
    /// disagree with the observations it was derived from.
    #[test]
    fn test_a_fill_records_the_observation_and_not_the_slippage() {
        let filled = OrderResolved {
            client_order_id: Uuid::nil(),
            alpaca_order_id: Some("order-1".to_string()),
            ticker: ticker("KO"),
            outcome: OrderOutcome::Filled,
            filled_quantity: Some(decimal("412")),
            filled_average_price: Some(decimal("62.44")),
            filled_after_cancel: false,
            broker_status: Some("filled".to_string()),
            error: None,
        };
        let value = serde_json::to_value(&filled).expect("fill must serialize");
        let object = value.as_object().expect("a fill is an object");
        assert!(!object.contains_key("slippage"));
        assert!(!object.contains_key("realized_profit_and_loss"));
    }

    /// A refused quote has to survive into the record. A journal holding only the books that passed
    /// says nothing about where the limits should sit, which is the whole reason they are recorded.
    #[test]
    fn test_a_refused_quote_is_recorded_with_the_book_that_was_refused() {
        let refused = PriceReading {
            ticker: ticker("AER"),
            price: 150.60,
            price_source: PriceSource::LastTrade,
            bid_price: Some(128.0),
            ask_price: Some(150.86),
            quote_timestamp: Some(instant("2026-08-12T14:40:00Z")),
            quote_rejection: Some(QuoteRejection::Wide {
                relative_spread: 0.15,
                limit: 0.02,
            }),
            trade_timestamp: Some(instant("2026-08-12T13:12:00Z")),
        };

        let value = serde_json::to_value(&refused).expect("a reading must serialize");
        assert_eq!(value["price_source"], "last_trade");
        assert_eq!(value["quote_rejection"]["reason"], "wide_quote");
        // How far outside the limit, not merely that it was outside: a journal that records only
        // the verdict cannot say where the limit should have been.
        assert_eq!(value["quote_rejection"]["relative_spread"], 0.15);
        assert_eq!(value["quote_rejection"]["limit"], 0.02);
        assert_eq!(value["bid_price"], 128.0);
        assert_eq!(value["ask_price"], 150.86);
        assert!(
            value["quote_timestamp"].is_string(),
            "staleness is the gap between the quote and the read, so the quote's own time is kept"
        );
        assert_eq!(value["trade_timestamp"], "2026-08-12T13:12:00Z");
    }

    /// A skip keeps its reason in the outcome, so a holiday and a halt stay distinguishable in a
    /// query that filters on one field.
    #[test]
    fn test_a_skipped_command_carries_why_it_skipped() {
        let value = serde_json::to_value(CommandOutcome::Skipped(SkipReason::NotATradingDay))
            .expect("an outcome must serialize");
        assert_eq!(value, "skipped_not_a_trading_day");
        assert_eq!(
            serde_json::to_value(CommandOutcome::Completed).expect("an outcome must serialize"),
            "completed"
        );
    }

    /// An entry's rank and an exit's rule are different kinds of answer, and the plan says which.
    #[test]
    fn test_a_planned_reason_distinguishes_a_rank_from_a_rule() {
        assert_eq!(
            serde_json::to_value(PlannedReason::Rank(3)).expect("a reason must serialize"),
            "rank_3"
        );
        assert_eq!(
            serde_json::to_value(PlannedReason::Close(CloseReason::StopLoss))
                .expect("a reason must serialize"),
            "stop_loss"
        );
    }

    #[tokio::test]
    async fn test_records_append_to_one_file_per_session() {
        let directory = temporary_directory("append");
        let journal = Journal::new(&directory).expect("the directory must be creatable");

        journal
            .record(
                Uuid::new_v4(),
                instant("2026-08-11T14:35:00Z"),
                account_observation(),
            )
            .await;
        journal
            .record(
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
        let journal = Journal::new(&directory).expect("the directory must be creatable");

        journal
            .record(
                Uuid::new_v4(),
                instant("2026-08-11T20:00:00Z"),
                account_observation(),
            )
            .await;
        journal
            .record(
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

    /// This is what runs at startup: a service that cannot resolve a writable directory here fails
    /// to construct at all, which is the loud failure rather than a session silently unrecorded.
    #[test]
    #[serial_test::serial]
    fn test_from_env_uses_the_explicit_directory() {
        let directory = temporary_directory("from-env-explicit");
        let _restore = EnvVarRestoreGuard::save("FUND_JOURNAL_DIRECTORY");
        // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
        unsafe { std::env::set_var("FUND_JOURNAL_DIRECTORY", &directory) };

        let journal = Journal::from_env().expect("the journal must resolve");

        assert_eq!(journal.directory(), directory);
        assert!(directory.is_dir());
        let _ = std::fs::remove_dir_all(&directory);
    }
}
