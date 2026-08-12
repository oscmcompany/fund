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
use serde::Serialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::common::alpaca::{ClientError, MarketDataClient, PriceSource, TradingClient};
use crate::common::types::{EquityPrediction, Ticker};
use crate::data::calendar::{SessionDate, TradingCalendar};
use crate::data::details::{self, DetailsError};
use crate::data::session_log::{
    CandidateReading, EvaluationPass, LiquidationRun, Observation, OpenPairReading, PriceReading,
    SessionLog,
};
use crate::data::universe::Universe;
use crate::models::tide::predict;
use crate::portfolio::account::{self, AccountError};
use crate::portfolio::execute::{
    self, ExecutionContext, ExecutionError, ExecutionSettings, OpenOutcome,
};
use crate::portfolio::pairs::{self, CloseReason, OpenPair, PairsError};
use crate::portfolio::risk::RiskGate;
use crate::portfolio::screen::{
    self, ScreenInput, CONVERGENCE_Z_SCORE, CORRELATION_WINDOW_SESSIONS, STOP_LOSS_WIDENING,
};
use crate::portfolio::size::{self, SizingParameters};

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
    pub sizing: SizingParameters,
    pub execution: ExecutionSettings,
    /// Where the pass records what it observed before acting on it.
    pub session_log: &'a SessionLog,
    /// Cancelled when the process is asked to stop.
    ///
    /// Checked between pairs in the entry half so the pass stops *starting* positions it could not
    /// finish opening. Nothing here aborts work already in flight — that is the drain's job, and
    /// this is what keeps the drain's bound honest.
    pub shutdown: &'a CancellationToken,
    pub now: DateTime<Utc>,
}

/// A price as one pass read it, with the snapshot field it came from.
///
/// The source travels with the number because the two branches are not interchangeable: a midpoint
/// moves with the book while a last trade can be minutes stale, so the same symbol read from
/// different fields on consecutive passes is not one series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReferencePrice {
    price: f64,
    source: PriceSource,
}

impl ReferencePrice {
    pub fn price(self) -> f64 {
        self.price
    }

    pub fn source(self) -> PriceSource {
        self.source
    }
}

/// One pair the pass closed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClosedRecord {
    pub pair_id: String,
    pub reason: String,
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
    /// The model run the session's forecasts came from.
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

/// Runs one evaluation pass.
pub async fn run_pass(
    context: &EvaluationContext<'_>,
) -> Result<EvaluationSummary, EvaluationError> {
    // One identifier for the pass, carried onto every order it sends and every fill they produce.
    // That thread is what turns "the session lost money" into "it was lost at sizing" or "it was
    // lost at execution".
    let correlation_id = uuid::Uuid::new_v4();
    let mut summary = EvaluationSummary::default();
    let mut observation = EvaluationPass {
        universe_size: context.universe.len(),
        ..EvaluationPass::default()
    };
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

    let mut prices = fetch_prices(context.market_data, &leg_symbols(&open_pairs)).await?;
    let closed = close_what_should_close(
        context,
        &execution,
        &open_pairs,
        &prices,
        &mut summary,
        &mut observation,
    )
    .await?;

    let held: HashSet<Ticker> = open_pairs
        .iter()
        .filter(|pair| !closed.contains(&pair.id()))
        .flat_map(|pair| [pair.long_ticker().clone(), pair.short_ticker().clone()])
        .collect();
    let remaining_open = open_pairs.len() - closed.len();

    // --- entries, conditionally ---

    let account = context.trading.fetch_account().await?;
    let previous_equity = previous_session_equity(context).await?;
    let minutes_until_close = context.calendar.minutes_until_close(context.now);

    let mut gate = RiskGate::new(
        &account,
        previous_equity,
        remaining_open,
        minutes_until_close,
        context.sizing,
    );

    observation.account_equity = account.equity().to_f64();
    observation.previous_session_equity = previous_equity.and_then(|equity| equity.to_f64());
    observation.gross_exposure_used = account.gross_exposure().to_f64();
    observation.gross_exposure_cap = gate.gross_exposure_cap().to_f64();
    observation.drawdown = gate.drawdown();
    observation.minutes_until_close = minutes_until_close;
    observation.vacant_slots = Some(gate.vacant_slots());

    // Before the gate, not after. A pass blocked by drawdown, capacity, or the hold window is the
    // most common way a session opens nothing, and those are exactly the rows where "which model
    // was deciding" matters — the difference between the model seeing no opportunity and the model
    // being three days old. Only the identifier is read here; the forecasts themselves stay behind
    // the gate, so a blocked pass still does not pay for the screen.
    summary.model_run_id = current_model_run_id(context).await?;
    observation.model_run_id = summary.model_run_id.clone();

    if let Some(block) = gate.session_block() {
        info!(block = block.as_str(), reason = %block, "Entry half skipped");
        summary.entries_blocked = Some(format!("{}: {block}", block.as_str()));
        observation.session_block = summary.entries_blocked.clone();
        record_pass(context, correlation_id, &mut observation, &prices).await;
        return Ok(summary);
    }

    let screened = build_screen_inputs(context, &held, &mut prices).await?;
    observation.predictions_available = screened.predictions_available;
    observation.eligible_tickers = screened.inputs.len();

    let candidates = screen::score_candidates(&screened.inputs);
    summary.candidates_screened = candidates.len();
    observation.candidates_screened = candidates.len();

    let selected =
        screen::select_disjoint(&candidates, gate.vacant_slots(), &held, &screened.sectors);
    let sized = size::size_pairs(&selected, account.equity(), &context.sizing);
    let (approved, refusals) = gate.admit_all(&sized);
    summary.entries_refused = refusals
        .iter()
        .map(|refusal| format!("{}: {}", refusal.block.as_str(), refusal.block))
        .collect();

    // Every scored candidate, with what became of it. Seeded as `not_selected` and overwritten as
    // the pass gets further with each one, so a candidate that fell out at selection is recorded as
    // precisely as one that opened.
    let mut candidate_decisions: HashMap<String, CandidateReading> = candidates
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
                    decision: "not_selected".to_string(),
                    refusal: None,
                },
            )
        })
        .collect();
    for refusal in &refusals {
        if let Some(reading) = candidate_decisions.get_mut(refusal.pair_id.as_str()) {
            reading.decision = "risk_refused".to_string();
            reading.refusal = Some(format!("{}: {}", refusal.block.as_str(), refusal.block));
        }
    }

    for (index, pair) in approved.iter().enumerate() {
        // Between pairs, never inside one: opening a pair is two broker legs and an insert, so the
        // pass finishes what it started and declines to start the next. This is what makes the
        // drain timeout in `bin/fund.rs` a real bound rather than a hope.
        //
        // Entry half only. `close_what_should_close` ran earlier and is never skipped — a close
        // reduces risk, and declining one leaves the book holding what it decided to let go of.
        if context.shutdown.is_cancelled() {
            summary.entries_abandoned = approved.len() - index;
            warn!(
                abandoned = summary.entries_abandoned,
                opened = summary.pairs_opened.len(),
                "Shutdown requested mid-entry; opening no further pairs"
            );
            for remaining in &approved[index..] {
                if let Some(reading) =
                    candidate_decisions.get_mut(remaining.candidate().pair_id().as_str())
                {
                    reading.decision = "abandoned_at_shutdown".to_string();
                }
            }
            break;
        }

        match execute::open_pair(&execution, pair, screened.model_run_id.clone()).await? {
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
                if let Err(error) = pairs::record_open(context.pool, &entry, context.now).await {
                    error!(
                        pair_id = %entry.pair_id(),
                        long_ticker = %long_fill.ticker(),
                        long_shares = long_fill.shares(),
                        long_price = long_fill.average_price(),
                        short_ticker = %short_fill.ticker(),
                        short_shares = short_fill.shares(),
                        short_price = short_fill.average_price(),
                        %error,
                        "Pair filled at the broker but could not be recorded; the position is live \
                         with no pair row and will be flattened by the pre-close liquidation"
                    );
                    return Err(error.into());
                }
                if let Some(reading) = candidate_decisions.get_mut(entry.pair_id().as_str()) {
                    reading.decision = "opened".to_string();
                }
                summary.pairs_opened.push(entry.pair_id().to_string());
            }
            OpenOutcome::Abandoned { ticker, reason } => {
                warn!(%ticker, reason, "Pair entry abandoned");
                if let Some(reading) =
                    candidate_decisions.get_mut(pair.candidate().pair_id().as_str())
                {
                    reading.decision = "unfilled".to_string();
                    reading.refusal = Some(reason.clone());
                }
                summary.entries_refused.push(format!("unfilled:{ticker}"));
            }
        }
    }

    observation.candidates = candidate_decisions.into_values().collect();
    observation
        .candidates
        .sort_by(|left, right| left.pair_id.cmp(&right.pair_id));
    record_pass(context, correlation_id, &mut observation, &prices).await;

    info!(
        closed = summary.pairs_closed.len(),
        opened = summary.pairs_opened.len(),
        screened = summary.candidates_screened,
        "Evaluation pass complete"
    );
    Ok(summary)
}

/// Writes the pass observation, attaching every price it read.
///
/// Called on both paths out of a pass, including the one the risk gate blocks — a pass that opened
/// nothing because the drawdown gate fired and one that opened nothing because no pair cleared the
/// screen look identical in a count and are very different days.
async fn record_pass(
    context: &EvaluationContext<'_>,
    correlation_id: uuid::Uuid,
    observation: &mut EvaluationPass,
    prices: &HashMap<Ticker, ReferencePrice>,
) {
    observation.prices = price_readings(prices);
    context
        .session_log
        .record(
            correlation_id,
            context.now,
            Observation::EvaluationPass(Box::new(std::mem::take(observation))),
        )
        .await;
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
    now: DateTime<Utc>,
) -> Result<LiquidationSummary, EvaluationError> {
    let correlation_id = uuid::Uuid::new_v4();
    let outcomes = client.close_all_positions().await?;
    let refused: HashSet<Ticker> = outcomes
        .iter()
        .filter(|outcome| !outcome.succeeded())
        .map(|outcome| outcome.ticker().clone())
        .collect();

    let mut summary = LiquidationSummary {
        positions_refused: refused.len(),
        ..Default::default()
    };

    for pair in pairs::load_open_pairs(pool).await? {
        if refused.contains(pair.long_ticker()) || refused.contains(pair.short_ticker()) {
            warn!(
                pair_id = %pair.pair_id(),
                "Pair left open: Alpaca refused to close a leg"
            );
            summary.pairs_still_open.push(pair.pair_id().to_string());
            continue;
        }
        if pairs::record_close(pool, pair.id(), CloseReason::EndOfDay, now).await? {
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

    session_log
        .record(
            correlation_id,
            now,
            Observation::LiquidationRun(LiquidationRun {
                pairs_closed: summary.pairs_closed,
                positions_refused: summary.positions_refused,
                pairs_still_open: summary.pairs_still_open.clone(),
            }),
        )
        .await;
    Ok(summary)
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

/// Fetches reference prices for a symbol list, keyed by ticker.
///
/// Symbols Alpaca returns with no usable price are simply absent from the map. The callers treat an
/// absent price as "no reading", never as zero.
async fn fetch_prices(
    client: &MarketDataClient,
    symbols: &[String],
) -> Result<HashMap<Ticker, ReferencePrice>, EvaluationError> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }
    let snapshots = client.fetch_snapshots(symbols).await?;
    Ok(snapshots
        .into_iter()
        .filter_map(|snapshot| {
            let (price, source) = snapshot.reference_price_with_source()?;
            Some((snapshot.ticker().clone(), ReferencePrice { price, source }))
        })
        .collect())
}

/// Every price the pass used, as rows for the session log.
///
/// Recorded once at pass level rather than repeated onto each pair, so a candidate row and an open
/// pair row that share a leg are provably reading the same number.
fn price_readings(prices: &HashMap<Ticker, ReferencePrice>) -> Vec<PriceReading> {
    let mut readings: Vec<PriceReading> = prices
        .iter()
        .map(|(ticker, reference)| PriceReading {
            ticker: ticker.to_string(),
            price: reference.price,
            price_source: reference.source.as_str().to_string(),
        })
        .collect();
    readings.sort_by(|left, right| left.ticker.cmp(&right.ticker));
    readings
}

/// Prices the open book and closes whatever the spread says to close.
///
/// Returns the identifiers of the pairs that were closed. A pair with no price or no rebuildable
/// spread model is left open with a warning — the pre-close liquidation closes it regardless, so
/// the worst case is holding it until 15:45 rather than exiting on a signal.
async fn close_what_should_close(
    context: &EvaluationContext<'_>,
    execution: &ExecutionContext<'_>,
    open_pairs: &[OpenPair],
    prices: &HashMap<Ticker, ReferencePrice>,
    summary: &mut EvaluationSummary,
    observation: &mut EvaluationPass,
) -> Result<HashSet<uuid::Uuid>, EvaluationError> {
    let models = screen::exit_models(
        open_pairs
            .iter()
            .map(|pair| (pair.pair_id(), pair.hedge_ratio())),
        context.close_history,
    );

    let mut closed = HashSet::new();
    for pair in open_pairs {
        // Filled in as the pair is measured and pushed on every path out of the iteration, so a
        // pair that could not be priced is as visible in the log as one that closed.
        let mut reading = OpenPairReading {
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
            entry_session: SessionDate::at(pair.opened_at()).date(),
            minutes_held: pair.minutes_held(context.now),
            decision: "held".to_string(),
        };

        let (Some(long_price), Some(short_price)) = (
            prices.get(pair.long_ticker()),
            prices.get(pair.short_ticker()),
        ) else {
            summary.pairs_unpriced += 1;
            warn!(pair_id = %pair.pair_id(), "Open pair could not be priced this pass");
            reading.decision = "unpriced".to_string();
            observation.open_pairs.push(reading);
            continue;
        };
        let Some(model) = models.get(pair.pair_id()) else {
            warn!(pair_id = %pair.pair_id(), "Open pair has no rebuildable spread model");
            reading.decision = "no_spread_model".to_string();
            observation.open_pairs.push(reading);
            continue;
        };

        reading.model_hedge_ratio = Some(model.hedge_ratio());
        reading.spread_mean = Some(model.mean());
        reading.spread_standard_deviation = Some(model.standard_deviation());

        let Some(z_score) = model.z_score(long_price.price, short_price.price) else {
            reading.decision = "unreadable_spread".to_string();
            observation.open_pairs.push(reading);
            continue;
        };
        reading.z_score = Some(z_score);

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
            long_price = long_price.price,
            long_price_source = %long_price.source,
            short_price = short_price.price,
            short_price_source = %short_price.source,
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
        let current_session = SessionDate::at(context.now);
        let reason = if entry_session == current_session {
            exit_reason(z_score, pair.entry_z_score())
        } else {
            warn!(
                pair_id = %pair.pair_id(),
                %entry_session,
                %current_session,
                "Open pair predates this session; scoring convergence only"
            );
            convergence_only(z_score)
        };
        let Some(mut reason) = reason else {
            observation.open_pairs.push(reading);
            continue;
        };

        // One pair's close failing must not take the other pairs' exits down with it. Propagating
        // here would abort the loop, so a single broker error on the first pair would leave every
        // later pair open for another five minutes — turning one stuck position into all of them.
        // The pair stays open in the record, which is the honest state, and the next pass retries.
        let outcome =
            match execute::close_pair(execution, pair.long_ticker(), pair.short_ticker()).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    error!(
                        pair_id = %pair.pair_id(),
                        %error,
                        "Closing a pair failed; it stays open and the next pass retries"
                    );
                    summary.exits_failed.push(pair.pair_id().to_string());
                    reading.decision = "close_failed".to_string();
                    observation.open_pairs.push(reading);
                    continue;
                }
            };
        if outcome.was_already_gone() {
            reason = CloseReason::PositionMissing;
        }
        pairs::record_close(context.pool, pair.id(), reason, context.now).await?;

        closed.insert(pair.id());
        reading.decision = reason.as_str().to_string();
        observation.open_pairs.push(reading);
        summary.pairs_closed.push(ClosedRecord {
            pair_id: pair.pair_id().to_string(),
            reason: reason.as_str().to_string(),
        });
    }
    Ok(closed)
}

/// The model run behind the current session's forecasts, without loading the forecasts.
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
struct ScreenedUniverse {
    inputs: Vec<ScreenInput>,
    model_run_id: Option<String>,
    /// Forecasts the session had before any eligibility test, for the session log. The gap between
    /// this and `inputs.len()` is how much the filters removed.
    predictions_available: usize,
    /// Every ticker's sector, not just the screened ones. `select_disjoint` needs the held legs
    /// too, and those are filtered out of `inputs` before it ever sees them.
    sectors: HashMap<Ticker, String>,
}

/// Assembles the screen's inputs, fetching only the prices the exit half did not already have.
async fn build_screen_inputs(
    context: &EvaluationContext<'_>,
    held: &HashSet<Ticker>,
    prices: &mut HashMap<Ticker, ReferencePrice>,
) -> Result<ScreenedUniverse, EvaluationError> {
    let (start, end) = SessionDate::at(context.now).bounds();
    let predictions = predict::load_predictions_between(context.pool, start, end).await?;
    if predictions.is_empty() {
        info!("No predictions for the current session; no entries will be screened");
        return Ok(ScreenedUniverse {
            inputs: Vec::new(),
            model_run_id: None,
            predictions_available: 0,
            sectors: HashMap::new(),
        });
    }

    // The newest forecast's run, not the first row's. A re-run leaves a mixed batch, and "whichever
    // ticker sorted first" is not an answer worth recording.
    let model_run_id = predictions
        .iter()
        .max_by_key(|prediction| prediction.timestamp())
        .map(|prediction| prediction.model_run_id().to_string());

    let sectors = details::load_sectors(context.pool).await?;

    // Only forecasts that can produce a candidate: in the universe, with a sector, with enough
    // history, and not already on the book. Every test here is a set lookup, because this runs once
    // per forecast on a pass that runs every five minutes.
    //
    // The sector test survives the removal of the different-sector rule, for a new reason. A ticker
    // whose sector is unknown cannot be counted against `MAXIMUM_LEGS_PER_SECTOR`, so admitting one
    // would let missing metadata quietly become unbounded concentration. Refusing to *open* what
    // cannot be measured is the conservative side of that trade; `select_disjoint` still tolerates
    // an unknown sector on a *held* leg, because a position already on the book cannot be
    // retroactively declined.
    let eligible: Vec<&EquityPrediction> = predictions
        .iter()
        .filter(|prediction| {
            let ticker = prediction.ticker();
            !held.contains(ticker)
                && sectors.contains_key(ticker)
                && context.close_history.contains_key(ticker)
                && context.universe.contains(ticker)
        })
        .collect();

    let missing: Vec<String> = eligible
        .iter()
        .filter(|prediction| !prices.contains_key(prediction.ticker()))
        .map(|prediction| prediction.ticker().to_string())
        .collect();
    prices.extend(fetch_prices(context.market_data, &missing).await?);

    let inputs: Vec<ScreenInput> = eligible
        .iter()
        .filter_map(|prediction| {
            let ticker = prediction.ticker();
            ScreenInput::new(
                ticker.clone(),
                context.close_history.get(ticker)?.clone(),
                prices.get(ticker)?.price,
                prediction.expected_return(),
                prediction.confidence(),
                context.universe.is_shortable(ticker),
            )
        })
        .collect();

    info!(
        predictions = predictions.len(),
        eligible = eligible.len(),
        inputs = inputs.len(),
        window = CORRELATION_WINDOW_SESSIONS,
        "Screen inputs assembled"
    );
    Ok(ScreenedUniverse {
        inputs,
        model_run_id,
        predictions_available: predictions.len(),
        sectors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::screen::ENTRY_Z_SCORE;

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
