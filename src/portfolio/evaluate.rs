//! The five-minute pass, and the pre-close liquidation that backs it up.
//!
//! Two properties the implementation has to preserve, and both are easy to lose:
//!
//! 1. **Exits run unconditionally.** Every early return below is in the entry half.
//! 2. **The entry half reuses the prices the exit half already fetched**, asking Alpaca only for
//!    symbols it does not have. Re-fetching an open leg between the two decisions would make them
//!    two opinions that can disagree; here they are one measurement used twice.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::alpaca::{
    AccountSnapshot, CheckedPrice, ClientError, MarketDataClient, QuoteLimits, QuoteRejection,
    TradingClient,
};
use crate::common::session_log::{
    CandidateReading, ExcludedTickerReading, LiquidationAttempted, Observation, OpenPairReading,
    OpenPairsObserved, PairClosed, PairOpened, PassEvaluated, PlanDecided, PlanPhase,
    PlannedAction, PlannedActionKind, PositionCloseRequested, PriceReading, PricesObserved,
    ScreenInputReading, SessionLog, UnavailablePrice, UniverseScreened,
};
use crate::common::types::{EquityPrediction, EquityQuote, PairID, SessionDate, Ticker};
use crate::data::calendar::TradingCalendar;
use crate::data::details::{self, DetailsError};
use crate::data::universe::Universe;
use crate::models::tide::predict;
use crate::portfolio::account::{self, AccountError};
use crate::portfolio::execute::{
    self, ExecutionContext, ExecutionError, ExecutionSettings, OpenOutcome,
};
use crate::portfolio::pairs::{self, CloseReason, OpenPair, PairsError};
use crate::portfolio::risk::{RiskBlock, RiskGate};
use crate::portfolio::screen::{
    self, ScreenInput, SpreadModel, CONVERGENCE_Z_SCORE, CORRELATION_WINDOW_SESSIONS,
    STOP_LOSS_WIDENING,
};
use crate::portfolio::size::{self, SizedPair, SizingParameters};

/// Errors that abort a pass.
///
/// Deliberately short. Almost everything that can go wrong in a pass is a condition to record and
/// carry on from — an unpriceable symbol, a pair with no history, a leg that would not fill. Only a
/// failure to reach the broker or the database stops the pass, because after either of those the
/// pass cannot know what it has done.
#[derive(Debug, thiserror::Error)]
pub enum EvaluationError {
    #[error("Alpaca is unreachable: {0}")]
    Alpaca(#[from] ClientError),
    #[error("pair record access failed: {0}")]
    Pairs(#[from] PairsError),
    #[error("account access failed: {0}")]
    Account(#[from] AccountError),
    #[error("order execution failed: {0}")]
    Execution(#[from] ExecutionError),
    #[error("sector metadata is unreadable: {0}")]
    Details(#[from] DetailsError),
    #[error("prediction access failed: {0}")]
    Predictions(#[from] sqlx::Error),
}

/// Everything a pass needs that it does not compute itself.
pub struct EvaluationContext<'a> {
    pub pool: &'a PgPool,
    pub trading: &'a TradingClient,
    pub market_data: &'a MarketDataClient,
    pub calendar: &'a TradingCalendar,
    pub universe: &'a Universe,
    /// Session-aligned daily closes, from the cache the handler warms once per date.
    pub close_history: &'a HashMap<Ticker, Vec<f64>>,
    /// Whether the splits table was available, so `close_history` is on today's share basis.
    ///
    /// False blocks the entry half at the gate. The exit half runs regardless, because closing
    /// reduces exposure rather than committing to a price.
    pub prices_adjustable: bool,
    pub sizing: SizingParameters,
    pub execution: ExecutionSettings,
    /// Where the pass records what it observed before acting on it.
    pub session_log: &'a SessionLog,
    /// The dispatching command's identifier, carried onto every order the pass sends.
    pub correlation_id: uuid::Uuid,
    /// Cancelled when the process is asked to stop.
    ///
    /// Checked between pairs in the entry half so the pass stops *starting* positions it could not
    /// finish opening. Nothing here aborts work already in flight — that is the drain's job, and
    /// this is what keeps the drain's bound honest.
    pub shutdown: &'a CancellationToken,
    pub now: DateTime<Utc>,
}

/// One pair the pass closed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClosedRecord {
    pub pair_id: String,
    pub reason: String,
}

/// What the exit round read from the world, before any judgment is applied to it.
///
/// Carries `session` and `now` as observed values rather than a clock, so [`decide_exits`] can be
/// replayed against a stored reading and reach the same plan.
pub struct ExitReading {
    pub open_pairs: Vec<OpenPair>,
    pub prices: HashMap<Ticker, CheckedPrice>,
    pub models: HashMap<PairID, SpreadModel>,
    pub session: SessionDate,
    pub now: DateTime<Utc>,
}

/// One close the exit round resolved on, carrying everything needed to attempt it.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedClose {
    pub pair_uuid: uuid::Uuid,
    pub pair_id: String,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    pub reason: CloseReason,
}

/// What the exit round resolved to do, and what it measured to get there.
///
/// `readings` covers every open pair, held or closing; `closes` is only the subset to attempt. The
/// two are separate because a plan is what will be tried, and the reading is what was seen.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExitPlan {
    pub closes: Vec<PlannedClose>,
    pub readings: Vec<OpenPairReading>,
    pub unpriced: usize,
}

/// What attempting an [`ExitPlan`] actually achieved.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExitOutcome {
    pub closed: HashSet<uuid::Uuid>,
    pub closed_records: Vec<ClosedRecord>,
    pub failed: Vec<String>,
    /// The plan's readings with each attempt's result written in.
    pub readings: Vec<OpenPairReading>,
}

/// What the entry round read about the account, before the screen has been paid for.
pub struct AccountReading {
    pub account: AccountSnapshot,
    pub previous_equity: Option<Decimal>,
    pub minutes_until_close: Option<i64>,
    pub remaining_open: usize,
    pub model_run_id: Option<String>,
}

/// Whether the entry round runs at all.
///
/// Its own decision because it gates the screen, which is the expensive half of the round: a pass
/// blocked here never pays to build screen inputs it would immediately discard.
#[derive(Debug, Clone, PartialEq)]
pub enum Admission {
    Blocked(RiskBlock),
    Open,
}

/// What the entry round read once admitted.
pub struct CandidatesReading {
    pub screened: ScreenedUniverse,
    pub held: HashSet<Ticker>,
}

/// One entry the round resolved on, sized and cleared by the gate.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedOpen {
    pub pair: SizedPair,
    pub model_run_id: Option<String>,
}

/// What the entry round resolved to do, and every candidate it weighed to get there.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EntryPlan {
    pub opens: Vec<PlannedOpen>,
    /// Every scored candidate keyed by pair identifier, seeded with the decision it reached here.
    pub candidates: HashMap<String, CandidateReading>,
    pub refusals: Vec<String>,
    pub candidates_screened: usize,
}

/// What attempting an [`EntryPlan`] actually achieved.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EntryOutcome {
    pub opened: Vec<String>,
    pub refused: Vec<String>,
    pub abandoned: usize,
    /// The plan's candidates with each attempt's result written in.
    pub candidates: HashMap<String, CandidateReading>,
}

/// What one pass did, and what stopped it doing more.
///
/// This is the `portfolio_evaluation_completed` payload. It carries reasons rather than counts
/// wherever a count would be ambiguous: a pass that opened nothing because the drawdown gate fired
/// and one that opened nothing because no pair cleared the screen are the same number and very
/// different days.
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct EvaluationSummary {
    pub open_pairs_at_start: usize,
    pub pairs_closed: Vec<ClosedRecord>,
    pub pairs_opened: Vec<String>,
    /// Open pairs that could not be priced this pass.
    ///
    /// Counted per pair, not per leg: a pair missing either price cannot be measured, so one
    /// increment covers both. Named for what it counts — the previous `legs_unpriced` implied a leg
    /// count and reported a pair count, which is the kind of number that gets divided by two in a
    /// dashboard six months later.
    pub pairs_unpriced: usize,
    /// Open pairs whose close was attempted and failed at the broker.
    ///
    /// These stay open in the record and are retried next pass. Non-empty means the book is holding
    /// something it tried to let go of.
    pub exits_failed: Vec<String>,
    pub candidates_screened: usize,
    /// Why the entry half did not run at all, if it did not.
    ///
    /// The stable name and the rendered detail, joined — `drawdown: drawdown of 0.1240 exceeds the
    /// threshold of 0.1000`. The name alone is greppable but says nothing about how close the pass
    /// came to running, which is the question anyone reading the row actually has.
    pub entries_blocked: Option<String>,
    /// Why individual candidates were refused, if any were.
    pub entries_refused: Vec<String>,
    /// The model run the session's predictions came from.
    ///
    /// Recorded on the pass rather than only on the pair, so a session that opened nothing still
    /// says which model it was deciding with. That is the difference between "the model saw no
    /// opportunity" and "the model was yesterday's".
    pub model_run_id: Option<String>,
    /// Approved pairs the pass declined to open because shutdown was requested mid-entry.
    ///
    /// Distinct from `entries_refused`, which is the risk gate turning a pair down on its merits.
    /// These cleared every check and were simply not reached. Reported rather than left implicit,
    /// because the alternative is a short `pairs_opened` that looks like a quiet screen.
    pub entries_abandoned: usize,
}

/// Decides whether an open pair should be closed at this spread reading.
///
/// The spread is `ln(short) - hedge_ratio * ln(long)` and entry is always above
/// [`crate::portfolio::screen::ENTRY_Z_SCORE`], so convergence is a fall back through zero. There is
/// no sign handling here because the orientation invariant already removed it.
///
/// The stop is measured from `entry_z_score` rather than from a fixed line. An absolute stop cannot
/// be right for every pair at once: it silently forbids entries above itself, and the screen has no
/// matching upper bound, so pairs were opened already past it and closed on the next pass having
/// never moved. Convergence stays absolute, because crossing the mean is the move the position was
/// taken to capture regardless of where it started.
pub fn exit_reason(z_score: f64, entry_z_score: f64) -> Option<CloseReason> {
    if !z_score.is_finite() || !entry_z_score.is_finite() {
        return None;
    }
    if z_score <= CONVERGENCE_Z_SCORE {
        return Some(CloseReason::Convergence);
    }
    if z_score >= entry_z_score + STOP_LOSS_WIDENING {
        return Some(CloseReason::StopLoss);
    }
    None
}

/// The convergence half of [`exit_reason`], for a pair whose stored entry score is not comparable.
///
/// Crossing the mean is a statement each window makes about itself, so it survives a change of
/// window where the relative stop does not.
pub fn convergence_only(z_score: f64) -> Option<CloseReason> {
    if !z_score.is_finite() {
        return None;
    }
    (z_score <= CONVERGENCE_Z_SCORE).then_some(CloseReason::Convergence)
}

/// Runs one evaluation pass, recording what it measured whether or not it finished.
///
/// The pass is written on every path out, failures included: a pass that died after pricing the
/// book had already observed something, and discarding it leaves the log silent at exactly the
/// moment it is worth reading.
pub async fn run_pass(
    context: &EvaluationContext<'_>,
) -> Result<EvaluationSummary, EvaluationError> {
    // The command's identifier, carried onto every order the pass sends and every fill they
    // produce. That thread is what turns "the session lost money" into "it was lost at sizing" or
    // "it was lost at execution".
    let correlation_id = context.correlation_id;
    let mut observation = PassEvaluated {
        universe_size: context.universe.len(),
        ..PassEvaluated::default()
    };

    let result = evaluate_pass(context, correlation_id, &mut observation).await;
    if let Err(error) = &result {
        observation.failure = Some(error.to_string());
    }
    context
        .session_log
        .record(
            correlation_id,
            context.now,
            Observation::PassEvaluated(Box::new(observation)),
        )
        .await;
    result
}

/// The pass itself, free to propagate: [`run_pass`] records the observation either way.
async fn evaluate_pass(
    context: &EvaluationContext<'_>,
    correlation_id: uuid::Uuid,
    observation: &mut PassEvaluated,
) -> Result<EvaluationSummary, EvaluationError> {
    let mut summary = EvaluationSummary::default();
    let execution = ExecutionContext {
        client: context.trading,
        settings: context.execution,
        session_log: context.session_log,
        correlation_id,
    };

    let open_pairs = pairs::load_open_pairs(context.pool).await?;
    summary.open_pairs_at_start = open_pairs.len();
    observation.open_pairs_at_start = open_pairs.len();

    // --- exits, unconditionally ---

    let prices = fetch_prices(
        context,
        correlation_id,
        "open_pairs",
        &leg_symbols(&open_pairs),
    )
    .await?;
    let exit_reading = observe_exits(context, open_pairs, prices);
    let exit_plan = decide_exits(&exit_reading);
    record_plan(
        context,
        correlation_id,
        PlanPhase::Exits,
        &exit_plan_actions(&exit_plan),
        exit_reading.open_pairs.len(),
    )
    .await;
    let (exits, exit_failure) = apply_exits(context, &execution, &exit_plan).await;

    // From the outcome, not the plan: a pass that died partway through closing had already closed
    // pairs, and reporting those as held would put the log at odds with the broker and the book.
    context
        .session_log
        .record(
            correlation_id,
            context.now,
            Observation::OpenPairsObserved(OpenPairsObserved {
                readings: exits.readings.clone(),
            }),
        )
        .await;

    summary.pairs_unpriced = exit_plan.unpriced;
    summary.pairs_closed = exits.closed_records.clone();
    summary.exits_failed = exits.failed.clone();
    if let Some(error) = exit_failure {
        return Err(error);
    }

    let open_pairs = exit_reading.open_pairs;
    let mut prices = exit_reading.prices;
    let held: HashSet<Ticker> = open_pairs
        .iter()
        .filter(|pair| !exits.closed.contains(&pair.id()))
        .flat_map(|pair| [pair.long_ticker().clone(), pair.short_ticker().clone()])
        .collect();
    let remaining_open = open_pairs.len() - exits.closed.len();

    // --- entries, conditionally ---

    let account_reading = observe_account(context, remaining_open).await?;
    let (mut gate, admission) =
        decide_admission(&account_reading, context.sizing, context.prices_adjustable);

    observation.account_equity = account_reading.account.equity().to_f64();
    observation.previous_session_equity = account_reading
        .previous_equity
        .and_then(|equity| equity.to_f64());
    observation.gross_exposure_used = account_reading.account.gross_exposure().to_f64();
    observation.gross_exposure_cap = gate.gross_exposure_cap().to_f64();
    observation.drawdown = gate.drawdown();
    observation.minutes_until_close = account_reading.minutes_until_close;
    observation.vacant_slots = Some(gate.vacant_slots());
    summary.model_run_id = account_reading.model_run_id.clone();
    observation.model_run_id = summary.model_run_id.clone();

    match admission {
        Admission::Blocked(block) => {
            info!(block = block.as_str(), reason = %block, "Entry half skipped");
            summary.entries_blocked = Some(format!("{}: {block}", block.as_str()));
            observation.session_block = summary.entries_blocked.clone();
            record_plan(context, correlation_id, PlanPhase::Entries, &[], 0).await;
            return Ok(summary);
        }
        Admission::Open => {}
    }

    let candidates_reading = observe_candidates(context, correlation_id, held, &mut prices).await?;
    observation.predictions_available = candidates_reading.screened.predictions_available;
    observation.eligible_tickers = candidates_reading.screened.eligible;

    let entry_plan = decide_entries(
        &account_reading,
        &candidates_reading,
        &mut gate,
        &context.sizing,
    );
    summary.candidates_screened = entry_plan.candidates_screened;
    observation.candidates_screened = entry_plan.candidates_screened;
    summary.entries_refused = entry_plan.refusals.clone();

    record_plan(
        context,
        correlation_id,
        PlanPhase::Entries,
        &entry_plan_actions(&entry_plan),
        entry_plan.candidates_screened,
    )
    .await;

    let (entries, entry_failure) = apply_entries(context, &execution, &entry_plan).await;

    // From the outcome on both paths, for the reason the exits round records its readings from one:
    // a pass that died opening its third pair had already opened two.
    observation.candidates = entries.candidates.clone().into_values().collect();
    observation
        .candidates
        .sort_by(|left, right| left.pair_id.cmp(&right.pair_id));

    summary.pairs_opened = entries.opened;
    summary.entries_refused.extend(entries.refused);
    summary.entries_abandoned = entries.abandoned;
    if let Some(error) = entry_failure {
        return Err(error);
    }

    info!(
        closed = summary.pairs_closed.len(),
        opened = summary.pairs_opened.len(),
        screened = summary.candidates_screened,
        "Evaluation pass complete"
    );
    Ok(summary)
}

/// What the pre-close liquidation did.
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct LiquidationSummary {
    pub pairs_closed: usize,
    pub positions_refused: usize,
    /// Pairs left open because Alpaca would not close a leg. Non-empty means the book is not flat.
    pub pairs_still_open: Vec<String>,
}

/// Flattens the account, then marks the pair records closed.
///
/// The order is not negotiable: marking records first would, on failure, leave the application
/// believing it holds nothing while the account holds positions overnight.
///
/// A pair whose leg Alpaca refused to close stays open in the record, which is what makes
/// `pairs_still_open` a usable alarm.
pub async fn run_liquidation(
    pool: &PgPool,
    client: &TradingClient,
    session_log: &SessionLog,
    correlation_id: uuid::Uuid,
    now: DateTime<Utc>,
) -> Result<LiquidationSummary, EvaluationError> {
    let mut summary = LiquidationSummary::default();
    let result = liquidate(pool, client, session_log, correlation_id, now, &mut summary).await;

    session_log
        .record(
            correlation_id,
            now,
            Observation::LiquidationAttempted(LiquidationAttempted {
                pairs_closed: summary.pairs_closed,
                positions_refused: summary.positions_refused,
                pairs_still_open: summary.pairs_still_open.clone(),
                failure: result.as_ref().err().map(ToString::to_string),
            }),
        )
        .await;
    result.map(|()| summary)
}

/// The flattening itself, free to propagate: [`run_liquidation`] records it either way.
async fn liquidate(
    pool: &PgPool,
    client: &TradingClient,
    session_log: &SessionLog,
    correlation_id: uuid::Uuid,
    now: DateTime<Utc>,
    summary: &mut LiquidationSummary,
) -> Result<(), EvaluationError> {
    let outcomes = client.close_all_positions().await?;

    // One record per position, not just the totals. The bulk close is the only path that touches
    // positions the application does not know about — a leg from a pass that died before recording
    // its pair — and a count cannot name those.
    for outcome in &outcomes {
        session_log
            .record(
                correlation_id,
                now,
                Observation::PositionCloseRequested(PositionCloseRequested {
                    ticker: outcome.ticker().to_string(),
                    pair_id: None,
                    alpaca_order_id: outcome.alpaca_order_id().map(str::to_string),
                    side: None,
                    quantity: outcome.quantity(),
                    reason: "liquidation".to_string(),
                    accepted: outcome.succeeded(),
                    status: Some(outcome.status()),
                    // The bulk path reports a per-symbol status rather than an error body, so a
                    // refusal is legible from `status` alone.
                    error: None,
                }),
            )
            .await;
    }
    let refused: HashSet<Ticker> = outcomes
        .iter()
        .filter(|outcome| !outcome.succeeded())
        .map(|outcome| outcome.ticker().clone())
        .collect();

    summary.positions_refused = refused.len();

    for pair in pairs::load_open_pairs(pool).await? {
        if refused.contains(pair.long_ticker()) || refused.contains(pair.short_ticker()) {
            warn!(
                pair_id = %pair.pair_id(),
                "Pair left open: Alpaca refused to close a leg"
            );
            summary.pairs_still_open.push(pair.pair_id().to_string());
            continue;
        }
        let updated = pairs::record_close(pool, pair.id(), CloseReason::EndOfDay, now).await?;
        session_log
            .record(
                correlation_id,
                now,
                Observation::PairClosed(PairClosed {
                    pair_uuid: pair.id().to_string(),
                    reason: CloseReason::EndOfDay.as_str().to_string(),
                    closed_at: now,
                    updated,
                }),
            )
            .await;
        if updated {
            summary.pairs_closed += 1;
        }
    }

    if summary.pairs_still_open.is_empty() {
        info!(
            closed = summary.pairs_closed,
            "Book flattened for the session"
        );
    } else {
        warn!(
            still_open = summary.pairs_still_open.len(),
            "Liquidation did not flatten the book"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pass internals
// ---------------------------------------------------------------------------

fn leg_symbols(open_pairs: &[OpenPair]) -> Vec<String> {
    let tickers: HashSet<&Ticker> = open_pairs
        .iter()
        .flat_map(|pair| [pair.long_ticker(), pair.short_ticker()])
        .collect();
    tickers.iter().map(|ticker| ticker.to_string()).collect()
}

/// Fetches reference prices for a symbol list, keyed by ticker, and records the reading.
///
/// Symbols Alpaca returns with no usable price are simply absent from the map. The callers treat an
/// absent price as "no reading", never as zero — and the record names them, so an absence has a
/// cause rather than only a gap.
async fn fetch_prices(
    context: &EvaluationContext<'_>,
    correlation_id: uuid::Uuid,
    purpose: &str,
    symbols: &[String],
) -> Result<HashMap<Ticker, CheckedPrice>, EvaluationError> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }
    let fetched = context.market_data.fetch_snapshots(symbols).await?;
    let failed: HashSet<&str> = fetched.failed_symbols.iter().map(String::as_str).collect();
    let limits = QuoteLimits::default();

    // Each snapshot is read once, with the book kept beside the price it produced. The map the
    // callers receive has already collapsed the quote away, so a refused book is recorded here or
    // nowhere — and a log holding only the quotes that passed cannot say whether `limits` is set
    // anywhere near right.
    let mut prices: HashMap<Ticker, CheckedPrice> = HashMap::new();
    let mut readings: Vec<PriceReading> = Vec::new();
    let mut refused: HashMap<&str, (&EquityQuote, QuoteRejection)> = HashMap::new();

    for snapshot in &fetched.snapshots {
        let quote = snapshot.latest_quote();
        let trade = snapshot.latest_trade();
        match snapshot.reference_price_checked(context.now, limits) {
            Some(checked) => {
                readings.push(PriceReading {
                    ticker: snapshot.ticker().to_string(),
                    price: checked.price(),
                    price_source: checked.source().as_str().to_string(),
                    bid_price: quote.map(|quote| quote.bid_price()),
                    ask_price: quote.map(|quote| quote.ask_price()),
                    quote_timestamp: quote.map(|quote| quote.timestamp()),
                    quote_rejection: checked
                        .rejection()
                        .map(|rejection| rejection.as_str().to_string()),
                    trade_timestamp: trade.map(|trade| trade.timestamp()),
                });
                prices.insert(snapshot.ticker().clone(), checked);
            }
            // A refused book with no last trade behind it. The symbol goes unpriced, which is the
            // point of refusing it, and this is the reading the limits most need judging against —
            // the guard cost the pass the symbol outright, so the book travels with the cause.
            None => {
                if let Some((quote, rejection)) =
                    quote.and_then(|quote| Some((quote, limits.refusal(quote, context.now)?)))
                {
                    refused.insert(snapshot.ticker().as_str(), (quote, rejection));
                }
            }
        }
    }
    readings.sort_by(|left, right| left.ticker.cmp(&right.ticker));

    let mut unavailable: Vec<UnavailablePrice> = symbols
        .iter()
        .filter(|symbol| {
            !prices
                .keys()
                .any(|ticker| ticker.as_str() == symbol.as_str())
        })
        .map(|symbol| {
            let refusal = refused.get(symbol.as_str());
            UnavailablePrice {
                ticker: symbol.clone(),
                cause: if failed.contains(symbol.as_str()) {
                    "chunk_failed"
                } else if refusal.is_some() {
                    "quote_rejected"
                } else {
                    "no_quote"
                }
                .to_string(),
                bid_price: refusal.map(|(quote, _)| quote.bid_price()),
                ask_price: refusal.map(|(quote, _)| quote.ask_price()),
                quote_timestamp: refusal.map(|(quote, _)| quote.timestamp()),
                quote_rejection: refusal.map(|(_, rejection)| rejection.as_str().to_string()),
            }
        })
        .collect();
    unavailable.sort_by(|left, right| left.ticker.cmp(&right.ticker));

    context
        .session_log
        .record(
            correlation_id,
            context.now,
            Observation::PricesObserved(PricesObserved {
                purpose: purpose.to_string(),
                readings,
                unavailable,
            }),
        )
        .await;
    Ok(prices)
}

/// Reads the account, its drawdown baseline, and the clock the gate judges against.
///
/// The model run identifier is read here, before the gate, not after. A pass blocked by drawdown,
/// capacity, or the hold window is the most common way a session opens nothing, and those are
/// exactly the rows where "which model was deciding" matters — the difference between the model
/// seeing no opportunity and the model being three days old. One row rather than seven thousand, so
/// a blocked pass still does not pay for the screen.
async fn observe_account(
    context: &EvaluationContext<'_>,
    remaining_open: usize,
) -> Result<AccountReading, EvaluationError> {
    Ok(AccountReading {
        account: context.trading.fetch_account().await?,
        previous_equity: previous_session_equity(context).await?,
        minutes_until_close: context.calendar.minutes_until_close(context.now),
        remaining_open,
        model_run_id: current_model_run_id(context).await?,
    })
}

/// Builds the round's risk gate and asks it whether the round runs at all. Pure.
///
/// Returns the gate as well as the verdict because the gate accumulates exposure across the entries
/// it later admits, so the one that answered this question must be the one that admits them.
pub fn decide_admission(
    reading: &AccountReading,
    sizing: SizingParameters,
    prices_adjustable: bool,
) -> (RiskGate, Admission) {
    let gate = RiskGate::new(
        &reading.account,
        reading.previous_equity,
        reading.remaining_open,
        reading.minutes_until_close,
        sizing,
        prices_adjustable,
    );
    let admission = match gate.session_block() {
        Some(block) => Admission::Blocked(block),
        None => Admission::Open,
    };
    (gate, admission)
}

/// Builds the screen's inputs, priced and filtered, applying no judgment about which to take.
async fn observe_candidates(
    context: &EvaluationContext<'_>,
    correlation_id: uuid::Uuid,
    held: HashSet<Ticker>,
    prices: &mut HashMap<Ticker, CheckedPrice>,
) -> Result<CandidatesReading, EvaluationError> {
    let screened = build_screen_inputs(context, correlation_id, &held, prices).await?;
    Ok(CandidatesReading { screened, held })
}

/// Scores, selects, sizes, and admits. Pure, and the round's whole decision.
///
/// Every scored candidate is seeded as `not_selected` and relabelled as it survives each stage —
/// `risk_refused` if the gate turned it down, `planned` if it cleared — so a candidate that fell out
/// at selection is recorded as precisely as one that was approved.
pub fn decide_entries(
    account: &AccountReading,
    reading: &CandidatesReading,
    gate: &mut RiskGate,
    sizing: &SizingParameters,
) -> EntryPlan {
    let candidates = screen::score_candidates(&reading.screened.inputs);
    let selected = screen::select_disjoint(
        &candidates,
        gate.vacant_slots(),
        &reading.held,
        &reading.screened.sectors,
    );
    let sized = size::size_pairs(&selected, account.account.equity(), sizing);
    let (approved, refusals) = gate.admit_all(&sized);

    let mut plan = EntryPlan {
        candidates_screened: candidates.len(),
        refusals: refusals
            .iter()
            .map(|refusal| format!("{}: {}", refusal.block.as_str(), refusal.block))
            .collect(),
        candidates: candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.pair_id().as_str().to_string(),
                    CandidateReading {
                        pair_id: candidate.pair_id().as_str().to_string(),
                        long_ticker: candidate.long_ticker().to_string(),
                        short_ticker: candidate.short_ticker().to_string(),
                        hedge_ratio: candidate.hedge_ratio(),
                        entry_z_score: candidate.entry_z_score(),
                        signal_strength: candidate.signal_strength(),
                        rank_score: candidate.rank_score(),
                        long_notional: None,
                        short_shares: None,
                        gross_exposure: None,
                        decision: "not_selected".to_string(),
                        refusal: None,
                    },
                )
            })
            .collect(),
        opens: Vec::new(),
    };

    // Every candidate that reached the sizer carries what it was sized to, refused or not.
    for pair in &sized {
        if let Some(candidate) = plan.candidates.get_mut(pair.candidate().pair_id().as_str()) {
            candidate.long_notional = pair.long_notional().value().to_f64();
            candidate.short_shares = Some(f64::from(pair.short_shares().get()));
            candidate.gross_exposure = pair.gross_exposure().to_f64();
        }
    }
    for refusal in &refusals {
        if let Some(candidate) = plan.candidates.get_mut(refusal.pair_id.as_str()) {
            candidate.decision = "risk_refused".to_string();
            candidate.refusal = Some(format!("{}: {}", refusal.block.as_str(), refusal.block));
        }
    }

    plan.opens = approved
        .into_iter()
        .map(|pair| PlannedOpen {
            pair,
            model_run_id: reading.screened.model_run_id.clone(),
        })
        .collect();
    // Approved, not yet attempted. Without this an approved pair still reads `not_selected` on the
    // path where applying fails, contradicting the plan record written moments earlier.
    for planned in &plan.opens {
        if let Some(candidate) = plan
            .candidates
            .get_mut(planned.pair.candidate().pair_id().as_str())
        {
            candidate.decision = "planned".to_string();
        }
    }
    plan
}

/// Opens every entry the plan calls for, applying no judgment of its own.
async fn apply_entries(
    context: &EvaluationContext<'_>,
    execution: &ExecutionContext<'_>,
    plan: &EntryPlan,
) -> (EntryOutcome, Option<EvaluationError>) {
    let mut outcome = EntryOutcome {
        candidates: plan.candidates.clone(),
        ..Default::default()
    };

    for (index, planned) in plan.opens.iter().enumerate() {
        let pair = &planned.pair;
        // Between pairs, never inside one: opening a pair is two broker legs and an insert, so the
        // pass finishes what it started and declines to start the next. This is what makes the
        // drain timeout in `bin/fund.rs` a real bound rather than a hope.
        //
        // Entry half only. The exit round ran earlier and is never skipped — a close reduces risk,
        // and declining one leaves the book holding what it decided to let go of.
        if context.shutdown.is_cancelled() {
            outcome.abandoned = plan.opens.len() - index;
            warn!(
                abandoned = outcome.abandoned,
                opened = outcome.opened.len(),
                "Shutdown requested mid-entry; opening no further pairs"
            );
            for remaining in &plan.opens[index..] {
                if let Some(candidate) = outcome
                    .candidates
                    .get_mut(remaining.pair.candidate().pair_id().as_str())
                {
                    candidate.decision = "abandoned_at_shutdown".to_string();
                }
            }
            break;
        }

        let opened = match execute::open_pair(execution, pair, planned.model_run_id.clone()).await {
            Ok(opened) => opened,
            Err(error) => return (outcome, Some(error.into())),
        };
        match opened {
            OpenOutcome::Opened {
                entry,
                long_fill,
                short_fill,
            } => {
                // Both legs are already filled and no transaction spans the broker and the
                // database. If this insert fails the position is live with no `equity_pairs` row,
                // so no later pass can exit it on a signal. The pre-close liquidation is the
                // backstop, which is why it flattens the *account* rather than known pairs; this
                // log is what makes the pair reconstructable by hand before then.
                let pair_uuid = match pairs::record_open(context.pool, &entry, context.now).await {
                    Ok(pair_uuid) => pair_uuid,
                    Err(error) => {
                        error!(
                            pair_id = %entry.pair_id(),
                            long_ticker = %long_fill.ticker(),
                            long_shares = long_fill.shares(),
                            long_price = long_fill.average_price(),
                            short_ticker = %short_fill.ticker(),
                            short_shares = short_fill.shares(),
                            short_price = short_fill.average_price(),
                            %error,
                            "Pair filled at the broker but could not be recorded; the position is \
                             live with no pair row and will be flattened by the pre-close \
                             liquidation"
                        );
                        return (outcome, Some(error.into()));
                    }
                };
                context
                    .session_log
                    .record(
                        execution.correlation_id,
                        context.now,
                        Observation::PairOpened(PairOpened {
                            pair_uuid: pair_uuid.to_string(),
                            pair_id: entry.pair_id().to_string(),
                            long_ticker: entry.long_ticker().to_string(),
                            short_ticker: entry.short_ticker().to_string(),
                            hedge_ratio: entry.hedge_ratio(),
                            entry_z_score: entry.entry_z_score(),
                            signal_strength: entry.signal_strength(),
                            model_run_id: entry.model_run_id().map(str::to_string),
                            opened_at: context.now,
                            long_decision_price: pair.candidate().long_price(),
                            short_decision_price: pair.candidate().short_price(),
                            long_fill_price: long_fill.average_price(),
                            short_fill_price: short_fill.average_price(),
                        }),
                    )
                    .await;
                if let Some(candidate) = outcome.candidates.get_mut(entry.pair_id().as_str()) {
                    candidate.decision = "opened".to_string();
                }
                outcome.opened.push(entry.pair_id().to_string());
            }
            OpenOutcome::Abandoned { ticker, reason } => {
                warn!(%ticker, reason, "Pair entry abandoned");
                if let Some(candidate) = outcome
                    .candidates
                    .get_mut(pair.candidate().pair_id().as_str())
                {
                    candidate.decision = "unfilled".to_string();
                    candidate.refusal = Some(reason.clone());
                }
                outcome.refused.push(format!("unfilled:{ticker}"));
            }
        }
    }

    (outcome, None)
}

/// The entry plan rendered as log actions, in the order they will be attempted.
fn entry_plan_actions(plan: &EntryPlan) -> Vec<PlannedAction> {
    plan.opens
        .iter()
        .enumerate()
        .map(|(rank, planned)| {
            let candidate = planned.pair.candidate();
            PlannedAction {
                pair_id: candidate.pair_id().as_str().to_string(),
                action: PlannedActionKind::Open,
                reason: format!("rank {}", rank + 1),
                long_ticker: candidate.long_ticker().to_string(),
                short_ticker: candidate.short_ticker().to_string(),
                long_notional: planned.pair.long_notional().value().to_f64(),
                short_shares: Some(f64::from(planned.pair.short_shares().get())),
            }
        })
        .collect()
}

/// Writes one round's plan to the session log before any of it is attempted.
///
/// Before, not after, and that is the whole reason it is its own record: a pass that dies partway
/// through applying leaves a plan saying what it meant to do, against orders saying what it managed.
async fn record_plan(
    context: &EvaluationContext<'_>,
    correlation_id: uuid::Uuid,
    phase: PlanPhase,
    actions: &[PlannedAction],
    considered: usize,
) {
    context
        .session_log
        .record(
            correlation_id,
            context.now,
            Observation::PlanDecided(PlanDecided {
                phase,
                actions: actions.to_vec(),
                considered,
            }),
        )
        .await;
}

/// The exit plan rendered as log actions.
fn exit_plan_actions(plan: &ExitPlan) -> Vec<PlannedAction> {
    plan.closes
        .iter()
        .map(|close| PlannedAction {
            pair_id: close.pair_id.clone(),
            action: PlannedActionKind::Close,
            reason: close.reason.as_str().to_string(),
            long_ticker: close.long_ticker.to_string(),
            short_ticker: close.short_ticker.to_string(),
            long_notional: None,
            short_shares: None,
        })
        .collect()
}

/// Rebuilds each open pair's spread model from history already in hand.
///
/// Not `async`: the book and the prices were fetched by the caller, which owns the I/O the pass
/// cannot avoid ordering. This assembles them into the value [`decide_exits`] reads.
fn observe_exits(
    context: &EvaluationContext<'_>,
    open_pairs: Vec<OpenPair>,
    prices: HashMap<Ticker, CheckedPrice>,
) -> ExitReading {
    let models = screen::exit_models(
        open_pairs
            .iter()
            .map(|pair| (pair.pair_id(), pair.hedge_ratio())),
        context.close_history,
    );

    ExitReading {
        open_pairs,
        prices,
        models,
        session: SessionDate::at(context.now),
        now: context.now,
    }
}

/// Scores every open pair and resolves which to close. Pure, and the whole point of the split.
///
/// A pair that cannot be priced or has no rebuildable spread model is recorded and held rather than
/// closed: a close it cannot measure is a guess, and holding is what the next pass retries from.
pub fn decide_exits(reading: &ExitReading) -> ExitPlan {
    let mut plan = ExitPlan::default();

    for pair in &reading.open_pairs {
        let mut open_pair_reading = OpenPairReading {
            pair_id: pair.pair_id().to_string(),
            long_ticker: pair.long_ticker().to_string(),
            short_ticker: pair.short_ticker().to_string(),
            stored_hedge_ratio: pair.hedge_ratio(),
            model_hedge_ratio: None,
            spread_mean: None,
            spread_standard_deviation: None,
            z_score: None,
            entry_z_score: pair.entry_z_score(),
            stop_at: pair.entry_z_score() + screen::STOP_LOSS_WIDENING,
            entry_session: SessionDate::at(pair.opened_at()),
            minutes_held: pair.minutes_held(reading.now),
            decision: "held".to_string(),
        };

        let (Some(long_price), Some(short_price)) = (
            reading.prices.get(pair.long_ticker()),
            reading.prices.get(pair.short_ticker()),
        ) else {
            plan.unpriced += 1;
            warn!(pair_id = %pair.pair_id(), "Open pair could not be priced this pass");
            open_pair_reading.decision = "unpriced".to_string();
            plan.readings.push(open_pair_reading);
            continue;
        };
        let Some(model) = reading.models.get(pair.pair_id()) else {
            warn!(pair_id = %pair.pair_id(), "Open pair has no rebuildable spread model");
            open_pair_reading.decision = "no_spread_model".to_string();
            plan.readings.push(open_pair_reading);
            continue;
        };

        open_pair_reading.model_hedge_ratio = Some(model.hedge_ratio());
        open_pair_reading.spread_mean = Some(model.mean());
        open_pair_reading.spread_standard_deviation = Some(model.standard_deviation());

        let Some(z_score) = model.z_score(long_price.price(), short_price.price()) else {
            open_pair_reading.decision = "unreadable_spread".to_string();
            plan.readings.push(open_pair_reading);
            continue;
        };
        open_pair_reading.z_score = Some(z_score);

        // Every input to the z-score, on every pass, whether or not the pair closes.
        //
        // A z-score on its own is unfalsifiable: a pair recorded at entry z 6.09 and read at 1.25
        // five minutes later is indistinguishable from a price move, a refitted distribution, and a
        // bug, and the exit path previously logged only its decision. These five fields are what
        // separate those cases, and the pass that does not close anything is the one that needs
        // them most.
        debug!(
            pair_id = %pair.pair_id(),
            z_score,
            entry_z_score = pair.entry_z_score(),
            stop_at = pair.entry_z_score() + screen::STOP_LOSS_WIDENING,
            spread_mean = model.mean(),
            spread_standard_deviation = model.standard_deviation(),
            hedge_ratio = model.hedge_ratio(),
            stored_hedge_ratio = pair.hedge_ratio(),
            long_price = long_price.price(),
            long_price_source = %long_price.source(),
            short_price = short_price.price(),
            short_price_source = %short_price.source(),
            "Priced an open pair"
        );

        // A pair that outlived its session is scored on convergence alone.
        //
        // `entry_z_score` was standardized against the 60 sessions ending before the pair opened;
        // this z was standardized against the window ending today. Those are different samples with
        // different means and deviations, so their difference is not a number of standard
        // deviations and the relative stop cannot be read from it. Convergence survives because it
        // asks whether the spread crossed its own mean, which each window answers about itself.
        //
        // The 15:45 liquidation is what normally makes this unreachable; arriving here means it did
        // not run, which is worth a warning on its own.
        let entry_session = SessionDate::at(pair.opened_at());
        let reason = if entry_session == reading.session {
            exit_reason(z_score, pair.entry_z_score())
        } else {
            warn!(
                pair_id = %pair.pair_id(),
                %entry_session,
                current_session = %reading.session,
                "Open pair predates this session; scoring convergence only"
            );
            convergence_only(z_score)
        };
        let Some(reason) = reason else {
            plan.readings.push(open_pair_reading);
            continue;
        };

        plan.closes.push(PlannedClose {
            pair_uuid: pair.id(),
            pair_id: pair.pair_id().to_string(),
            long_ticker: pair.long_ticker().clone(),
            short_ticker: pair.short_ticker().clone(),
            reason,
        });
        plan.readings.push(open_pair_reading);
    }

    plan
}

/// Attempts every close the plan calls for, applying no judgment of its own.
///
/// One pair's close failing must not take the others down with it: aborting the loop would turn a
/// single broker error into every later pair staying open for another five minutes. The pair stays
/// open in the record, which is the honest state, and the next pass retries.
async fn apply_exits(
    context: &EvaluationContext<'_>,
    execution: &ExecutionContext<'_>,
    plan: &ExitPlan,
) -> (ExitOutcome, Option<EvaluationError>) {
    let mut outcome = ExitOutcome {
        readings: plan.readings.clone(),
        ..Default::default()
    };

    for close in &plan.closes {
        // Located by identifier rather than by a stored index: an index is a second thing that can
        // be wrong about the same list, and nothing here needs one.
        let Some(reading) = outcome
            .readings
            .iter_mut()
            .find(|reading| reading.pair_id == close.pair_id)
        else {
            continue;
        };
        let broker_outcome = match execute::close_pair(
            execution,
            close.pair_id.as_str(),
            &close.long_ticker,
            &close.short_ticker,
        )
        .await
        {
            Ok(broker_outcome) => broker_outcome,
            Err(error) => {
                error!(
                    pair_id = %close.pair_id,
                    %error,
                    "Closing a pair failed; it stays open and the next pass retries"
                );
                outcome.failed.push(close.pair_id.clone());
                reading.decision = "close_failed".to_string();
                continue;
            }
        };
        // The plan said why it wanted the pair closed; the broker is the only thing that can say
        // the position was already gone, so the recorded reason is settled here rather than there.
        let reason = if broker_outcome.was_already_gone() {
            CloseReason::PositionMissing
        } else {
            close.reason
        };
        let updated =
            match pairs::record_close(context.pool, close.pair_uuid, reason, context.now).await {
                Ok(updated) => updated,
                Err(error) => {
                    reading.decision = "close_failed".to_string();
                    // The outcome travels with the error. Pairs closed before this one reached the
                    // broker and the database, and reporting them as never attempted is worse than
                    // the failure itself.
                    return (outcome, Some(error.into()));
                }
            };
        context
            .session_log
            .record(
                execution.correlation_id,
                context.now,
                Observation::PairClosed(PairClosed {
                    pair_uuid: close.pair_uuid.to_string(),
                    reason: reason.as_str().to_string(),
                    closed_at: context.now,
                    updated,
                }),
            )
            .await;

        outcome.closed.insert(close.pair_uuid);
        reading.decision = reason.as_str().to_string();
        outcome.closed_records.push(ClosedRecord {
            pair_id: close.pair_id.clone(),
            reason: reason.as_str().to_string(),
        });
    }

    (outcome, None)
}

/// The model run behind the current session's predictions, without loading them.
///
/// One row rather than seven thousand, so this is cheap enough to run before the risk gate on every
/// pass. See the call site for why it belongs there.
async fn current_model_run_id(
    context: &EvaluationContext<'_>,
) -> Result<Option<String>, EvaluationError> {
    let (start, end) = SessionDate::at(context.now).bounds();
    let row = sqlx::query!(
        r#"SELECT model_run_id AS "model_run_id!"
           FROM equity_predictions
           WHERE timestamp >= $1 AND timestamp < $2
           ORDER BY timestamp DESC
           LIMIT 1"#,
        start,
        end,
    )
    .fetch_optional(context.pool)
    .await?;
    Ok(row.map(|row| row.model_run_id))
}

/// The equity recorded for the previous trading day, if any.
async fn previous_session_equity(
    context: &EvaluationContext<'_>,
) -> Result<Option<rust_decimal::Decimal>, EvaluationError> {
    let today = SessionDate::at(context.now);
    let Some(previous) = context.calendar.previous_trading_day(today) else {
        return Ok(None);
    };
    Ok(account::load_equity_for(context.pool, previous).await?)
}

/// The screen's inputs, and the model run they came from.
pub struct ScreenedUniverse {
    pub inputs: Vec<ScreenInput>,
    /// Predictions that passed the eligibility filter, before pricing removed any.
    pub eligible: usize,
    pub model_run_id: Option<String>,
    /// Predictions the session had before any eligibility test, for the session log. The gap
    /// between this and `inputs.len()` is how much the filters removed.
    pub predictions_available: usize,
    /// Every ticker's sector, not just the screened ones. `select_disjoint` needs the held legs
    /// too, and those are filtered out of `inputs` before it ever sees them.
    pub sectors: HashMap<Ticker, String>,
}

/// Assembles the screen's inputs, fetching only the prices the exit half did not already have.
///
/// Records the funnel — what reached the screen and what each removed ticker failed on — before
/// returning, so the reading exists whether or not the rest of the pass completes.
async fn build_screen_inputs(
    context: &EvaluationContext<'_>,
    correlation_id: uuid::Uuid,
    held: &HashSet<Ticker>,
    prices: &mut HashMap<Ticker, CheckedPrice>,
) -> Result<ScreenedUniverse, EvaluationError> {
    let (start, end) = SessionDate::at(context.now).bounds();
    let predictions = predict::load_predictions_between(context.pool, start, end).await?;
    if predictions.is_empty() {
        info!("No predictions for the current session; no entries will be screened");
        return Ok(ScreenedUniverse {
            inputs: Vec::new(),
            eligible: 0,
            model_run_id: None,
            predictions_available: 0,
            sectors: HashMap::new(),
        });
    }

    // The newest prediction's run, not the first row's. A re-run leaves a mixed batch, and "whichever
    // ticker sorted first" is not an answer worth recording.
    let model_run_id = predictions
        .iter()
        .max_by_key(|prediction| prediction.timestamp())
        .map(|prediction| prediction.model_run_id().to_string());

    let sectors = details::load_sectors(context.pool).await?;

    // Only predictions that can produce a candidate: in the universe, with a sector, with enough
    // history, and not already on the book. Every test here is a set lookup, because this runs once
    // per prediction on a pass that runs every five minutes.
    //
    // The sector test survives the removal of the different-sector rule, for a new reason. A ticker
    // whose sector is unknown cannot be counted against `MAXIMUM_LEGS_PER_SECTOR`, so admitting one
    // would let missing metadata quietly become unbounded concentration. Refusing to *open* what
    // cannot be measured is the conservative side of that trade; `select_disjoint` still tolerates
    // an unknown sector on a *held* leg, because a position already on the book cannot be
    // retroactively declined.
    // Partitioned rather than filtered, so the tickers that fall out are as recorded as the ones
    // that stay. The first failing test is the one reported: the order below is the order the
    // filter applies them in, and a ticker failing two is not two facts.
    let mut eligible: Vec<&EquityPrediction> = Vec::new();
    let mut excluded: Vec<ExcludedTickerReading> = Vec::new();
    for prediction in &predictions {
        let ticker = prediction.ticker();
        let reason = if held.contains(ticker) {
            Some("already_held")
        } else if !sectors.contains_key(ticker) {
            Some("no_sector")
        } else if !context.close_history.contains_key(ticker) {
            Some("no_close_history")
        } else if !context.universe.contains(ticker) {
            Some("outside_universe")
        } else {
            None
        };
        match reason {
            Some(reason) => excluded.push(ExcludedTickerReading {
                ticker: ticker.to_string(),
                reason: reason.to_string(),
                detail: None,
            }),
            None => eligible.push(prediction),
        }
    }

    let missing: Vec<String> = eligible
        .iter()
        .filter(|prediction| !prices.contains_key(prediction.ticker()))
        .map(|prediction| prediction.ticker().to_string())
        .collect();
    prices.extend(fetch_prices(context, correlation_id, "screen_candidates", &missing).await?);

    let mut inputs: Vec<ScreenInput> = Vec::with_capacity(eligible.len());
    let mut readings: Vec<ScreenInputReading> = Vec::with_capacity(eligible.len());
    for prediction in &eligible {
        let ticker = prediction.ticker();
        // Eligible and still unpriceable: Alpaca returned no usable quote or trade for it this
        // pass. That is a fifth way out of the funnel and it belongs with the other four.
        let (Some(window), Some(reference)) =
            (context.close_history.get(ticker), prices.get(ticker))
        else {
            excluded.push(ExcludedTickerReading {
                ticker: ticker.to_string(),
                reason: "unpriced".to_string(),
                detail: None,
            });
            continue;
        };
        let input = match ScreenInput::new(
            ticker.clone(),
            window.clone(),
            reference.price(),
            prediction.expected_return(),
            prediction.confidence(),
            context.universe.is_shortable(ticker),
        ) {
            Ok(input) => input,
            Err(rejection) => {
                excluded.push(ExcludedTickerReading {
                    ticker: ticker.to_string(),
                    reason: rejection.as_str().to_string(),
                    detail: rejection.detail(),
                });
                continue;
            }
        };
        readings.push(ScreenInputReading {
            ticker: ticker.to_string(),
            expected_return: prediction.expected_return(),
            confidence: prediction.confidence(),
            is_shortable: context.universe.is_shortable(ticker),
        });
        inputs.push(input);
    }

    info!(
        predictions = predictions.len(),
        eligible = eligible.len(),
        inputs = inputs.len(),
        window = CORRELATION_WINDOW_SESSIONS,
        "Screen inputs assembled"
    );
    context
        .session_log
        .record(
            correlation_id,
            context.now,
            Observation::UniverseScreened(UniverseScreened {
                inputs: readings,
                excluded,
            }),
        )
        .await;
    Ok(ScreenedUniverse {
        inputs,
        eligible: eligible.len(),
        model_run_id,
        predictions_available: predictions.len(),
        sectors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::screen::ENTRY_Z_SCORE;

    /// One open pair, its spread model, and prices placing the spread at `z_at_close`.
    fn exit_reading_for(z_at_close: f64, priced: bool) -> ExitReading {
        exit_reading_with(z_at_close, priced, true, 0)
    }

    /// The same, with the spread model optionally withheld and the session optionally advanced.
    fn exit_reading_with(
        z_at_close: f64,
        priced: bool,
        modelled: bool,
        sessions_later: i64,
    ) -> ExitReading {
        use crate::common::alpaca::PriceSource;

        let long = Ticker::new("AAAA").unwrap();
        let short = Ticker::new("BBBB").unwrap();
        let pair_id = PairID::new(long.clone(), short.clone());

        // Only the short leg moves: wiggling both proportionally holds `ln(short) - ln(long)`
        // constant, and a spread with no dispersion is the fixture that makes a test assert nothing.
        let long_closes: Vec<f64> = vec![100.0; CORRELATION_WINDOW_SESSIONS];
        let short_closes: Vec<f64> = (0..CORRELATION_WINDOW_SESSIONS)
            .map(|index| if index % 2 == 0 { 50.0 } else { 51.0 })
            .collect();
        let model = SpreadModel::with_hedge_ratio(1.0, &long_closes, &short_closes)
            .expect("the fixture series must fit");

        // Solve for the short price that puts the spread at the requested z.
        let short_price =
            (model.mean() + z_at_close * model.standard_deviation() + 100.0_f64.ln()).exp();

        let mut prices = HashMap::new();
        if priced {
            prices.insert(
                long.clone(),
                CheckedPrice::for_test(100.0, PriceSource::LastTrade),
            );
            prices.insert(
                short.clone(),
                CheckedPrice::for_test(short_price, PriceSource::LastTrade),
            );
        }

        let opened_at = DateTime::from_timestamp(1_770_000_000, 0).unwrap();
        let mut models = HashMap::new();
        if modelled {
            models.insert(pair_id.clone(), model);
        }

        let read_at = opened_at + chrono::Duration::days(sessions_later);
        ExitReading {
            open_pairs: vec![OpenPair::new(
                uuid::Uuid::nil(),
                pair_id,
                1.0,
                ENTRY_Z_SCORE,
                opened_at,
            )],
            prices,
            models,
            session: SessionDate::at(read_at),
            now: read_at,
        }
    }

    /// The point of the split: the exit decision is a pure function of a value, so it runs with no
    /// runtime, no broker, and no database. If this test ever needs `#[tokio::test]`, an `await`
    /// has moved into `decide_exits` and the boundary has been lost.
    #[test]
    fn test_the_exit_decision_runs_without_a_runtime() {
        // Below the mean rather than exactly on it: `CONVERGENCE_Z_SCORE` is 0.0 and the test is
        // `z <= 0.0`, so a z solved to land on the mean can come back at +2e-14 and hold.
        let plan = decide_exits(&exit_reading_for(-1.0, true));

        assert_eq!(
            plan.closes.len(),
            1,
            "a spread back through its mean has converged"
        );
        assert_eq!(plan.closes[0].reason, CloseReason::Convergence);
        assert_eq!(plan.closes[0].pair_id, "AAAA-BBBB");
        assert_eq!(plan.readings.len(), 1, "every open pair is measured");
        assert_eq!(plan.unpriced, 0);
    }

    /// A pair the pass cannot price is held and counted, never closed. Closing on a spread it could
    /// not measure would be a guess, and the next pass is what retries.
    #[test]
    fn test_an_unpriceable_pair_is_held_rather_than_closed() {
        let plan = decide_exits(&exit_reading_for(0.0, false));

        assert!(plan.closes.is_empty(), "nothing is closed unmeasured");
        assert_eq!(plan.unpriced, 1);
        assert_eq!(plan.readings[0].decision, "unpriced");
        assert_eq!(plan.readings[0].z_score, None);
    }

    /// A spread still out at its entry score is left alone, which is the ordinary pass.
    #[test]
    fn test_a_spread_that_has_not_moved_produces_an_empty_plan() {
        let plan = decide_exits(&exit_reading_for(ENTRY_Z_SCORE, true));

        assert!(plan.closes.is_empty());
        assert_eq!(plan.readings[0].decision, "held");
        assert_eq!(plan.unpriced, 0);
    }

    /// A pair that outlived its session is scored on convergence alone, so a spread sitting past
    /// its stop is *held* rather than stopped out — the two windows standardized against different
    /// samples, and their difference is not a number of standard deviations.
    ///
    /// The branch that picks the rule, not `convergence_only` itself: a wrong session comparison
    /// would silently apply the session-local stop to an overnight pair.
    #[test]
    fn test_a_pair_from_an_earlier_session_is_scored_on_convergence_alone() {
        let widened = ENTRY_Z_SCORE + STOP_LOSS_WIDENING + 1.0;

        let same_session = decide_exits(&exit_reading_with(widened, true, true, 0));
        assert_eq!(
            same_session.closes.len(),
            1,
            "within its own session the stop applies"
        );
        assert_eq!(same_session.closes[0].reason, CloseReason::StopLoss);

        let overnight = decide_exits(&exit_reading_with(widened, true, true, 1));
        assert!(
            overnight.closes.is_empty(),
            "across sessions the stop is unreadable, so the pair is held"
        );
        assert_eq!(overnight.readings[0].decision, "held");
    }

    /// A pair whose spread model could not be rebuilt is held and named, never closed.
    #[test]
    fn test_a_pair_without_a_spread_model_is_held_and_named() {
        let plan = decide_exits(&exit_reading_with(-1.0, true, false, 0));

        assert!(plan.closes.is_empty());
        assert_eq!(plan.readings[0].decision, "no_spread_model");
        assert_eq!(
            plan.unpriced, 0,
            "it was priced; what it lacked was the model"
        );
    }

    /// The entry threshold, the convergence threshold, and the stop have to sit in that order or
    /// the pair is closed by the same reading that opened it.
    #[test]
    fn test_the_exit_thresholds_bracket_the_entry_threshold() {
        const _: () = assert!(CONVERGENCE_Z_SCORE < ENTRY_Z_SCORE);
        const _: () = assert!(STOP_LOSS_WIDENING > 0.0);
        assert_eq!(exit_reason(ENTRY_Z_SCORE, ENTRY_Z_SCORE), None);
    }

    #[test]
    fn test_a_spread_back_through_its_mean_is_convergence() {
        assert_eq!(exit_reason(0.0, 2.5), Some(CloseReason::Convergence));
        assert_eq!(exit_reason(-1.5, 2.5), Some(CloseReason::Convergence));
    }

    #[test]
    fn test_a_spread_widening_past_the_stop_is_a_stop_loss() {
        let entry = 2.5;
        assert_eq!(
            exit_reason(entry + STOP_LOSS_WIDENING, entry),
            Some(CloseReason::StopLoss)
        );
        assert_eq!(exit_reason(entry + 3.0, entry), Some(CloseReason::StopLoss));
    }

    /// Between the two thresholds the pair is working as intended and is held.
    #[test]
    fn test_a_spread_inside_the_band_is_held() {
        assert_eq!(exit_reason(1.0, 2.5), None);
        assert_eq!(exit_reason(2.5 + STOP_LOSS_WIDENING - 0.01, 2.5), None);
    }

    /// The whole point of measuring from entry: a wide entry gets the same room as a narrow one.
    ///
    /// Under the previous absolute stop of 4.0 the first assertion closed a pair that had not moved,
    /// which is what happened to three of the first ten pairs opened in production.
    #[test]
    fn test_the_stop_travels_with_the_entry_spread() {
        assert_eq!(exit_reason(6.09, 6.09), None, "an untouched pair is held");
        assert_eq!(exit_reason(6.5, 6.09), None, "still inside its own band");
        assert_eq!(
            exit_reason(7.6, 6.09),
            Some(CloseReason::StopLoss),
            "widened a full stop beyond entry"
        );
        // The same absolute reading is a stop for a pair entered narrow and a hold for one entered
        // wide. That is the asymmetry an absolute threshold cannot express.
        assert_eq!(exit_reason(3.6, 2.0), Some(CloseReason::StopLoss));
        assert_eq!(exit_reason(3.6, 3.0), None);
    }

    /// A pair from an earlier session is judged on convergence alone.
    ///
    /// Its `entry_z_score` was standardized against a different 60-session sample, so the difference
    /// between the two is not a number of deviations and the relative stop cannot be read from it.
    #[test]
    fn test_convergence_only_ignores_the_stop() {
        assert_eq!(convergence_only(0.0), Some(CloseReason::Convergence));
        assert_eq!(convergence_only(-2.0), Some(CloseReason::Convergence));
        // Far beyond any stop, and still held: the comparison that would fire is not available.
        assert_eq!(convergence_only(9.0), None);
        assert_eq!(convergence_only(f64::NAN), None);
    }

    /// The same reading closes under the session-local rule and is held under the cross-session one.
    #[test]
    fn test_the_cross_session_rule_differs_from_the_session_local_rule() {
        let entry = 2.0;
        let widened = entry + STOP_LOSS_WIDENING;
        assert_eq!(exit_reason(widened, entry), Some(CloseReason::StopLoss));
        assert_eq!(convergence_only(widened), None);
    }

    /// A pair the screen approves must not be closable by the very next reading of the same spread.
    ///
    /// This is the composition the two halves never checked against each other. Entry admitted
    /// anything at or above `ENTRY_Z_SCORE` with no upper bound while the stop was a fixed 4.0, so
    /// every candidate above 4.0 was born closable. The property holds for any entry the screen can
    /// now emit, which is what the cap and the relative stop together guarantee.
    #[test]
    fn test_no_admissible_entry_is_closable_at_entry() {
        let mut z = ENTRY_Z_SCORE;
        while z <= crate::portfolio::screen::ENTRY_Z_SCORE_CAP {
            assert_eq!(
                exit_reason(z, z),
                None,
                "a pair entered at z={z} must not close on its own entry reading"
            );
            z += 0.01;
        }
    }

    /// A non-finite reading is not an exit signal in either direction. Treating it as convergence
    /// would close every pair the moment a price went bad; treating it as a stop would do the same
    /// and record the loss reason.
    #[test]
    fn test_a_non_finite_reading_is_not_an_exit_signal() {
        assert_eq!(exit_reason(f64::NAN, 2.5), None);
        assert_eq!(exit_reason(f64::INFINITY, 2.5), None);
        // A corrupt stored entry is equally disqualifying: the stop would otherwise be computed
        // against NaN and compare false, silently making the pair unstoppable.
        assert_eq!(exit_reason(3.0, f64::NAN), None);
    }

    #[test]
    fn test_leg_symbols_deduplicates_across_pairs() {
        let pair = |long: &str, short: &str| {
            OpenPair::new(
                uuid::Uuid::new_v4(),
                crate::common::types::PairID::new(
                    Ticker::new(long).unwrap(),
                    Ticker::new(short).unwrap(),
                ),
                1.0,
                2.5,
                Utc::now(),
            )
        };
        let symbols = leg_symbols(&[pair("AAAA", "BBBB"), pair("AAAA", "CCCC")]);
        assert_eq!(symbols.len(), 3);
    }

    #[test]
    fn test_leg_symbols_is_empty_for_an_empty_book() {
        assert!(leg_symbols(&[]).is_empty());
    }
}
