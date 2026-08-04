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
use serde::Serialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::common::alpaca::{ClientError, MarketDataClient, TradingClient};
use crate::common::types::{EquityPrediction, Ticker};
use crate::data::calendar::{SessionDate, TradingCalendar};
use crate::data::details::{self, DetailsError};
use crate::data::universe::Universe;
use crate::models::tide::predict;
use crate::portfolio::account::{self, AccountError};
use crate::portfolio::execute::{self, ExecutionError, ExecutionSettings, OpenOutcome};
use crate::portfolio::pairs::{self, CloseReason, OpenPair, PairsError};
use crate::portfolio::risk::RiskGate;
use crate::portfolio::screen::{
    self, ScreenInput, CONVERGENCE_Z_SCORE, CORRELATION_WINDOW_SESSIONS, STOP_LOSS_Z_SCORE,
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
/// [`crate::portfolio::screen::ENTRY_Z_SCORE`], so convergence is a fall back through zero and a
/// stop is a rise past the stop threshold. There is no sign handling here because the orientation
/// invariant already removed it.
pub fn exit_reason(z_score: f64) -> Option<CloseReason> {
    if !z_score.is_finite() {
        return None;
    }
    if z_score <= CONVERGENCE_Z_SCORE {
        return Some(CloseReason::Convergence);
    }
    if z_score >= STOP_LOSS_Z_SCORE {
        return Some(CloseReason::StopLoss);
    }
    None
}

/// Runs one evaluation pass.
pub async fn run_pass(
    context: &EvaluationContext<'_>,
) -> Result<EvaluationSummary, EvaluationError> {
    let mut summary = EvaluationSummary::default();

    let open_pairs = pairs::load_open_pairs(context.pool).await?;
    summary.open_pairs_at_start = open_pairs.len();

    // --- exits, unconditionally ---

    let mut prices = fetch_prices(context.market_data, &leg_symbols(&open_pairs)).await?;
    let closed = close_what_should_close(context, &open_pairs, &prices, &mut summary).await?;

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

    // Before the gate, not after. A pass blocked by drawdown, capacity, or the hold window is the
    // most common way a session opens nothing, and those are exactly the rows where "which model
    // was deciding" matters — the difference between the model seeing no opportunity and the model
    // being three days old. Only the identifier is read here; the forecasts themselves stay behind
    // the gate, so a blocked pass still does not pay for the screen.
    summary.model_run_id = current_model_run_id(context).await?;

    if let Some(block) = gate.session_block() {
        info!(block = block.as_str(), reason = %block, "Entry half skipped");
        summary.entries_blocked = Some(format!("{}: {block}", block.as_str()));
        return Ok(summary);
    }

    let screened = build_screen_inputs(context, &held, &mut prices).await?;
    let candidates = screen::score_candidates(&screened.inputs);
    summary.candidates_screened = candidates.len();

    let selected = screen::select_disjoint(&candidates, gate.vacant_slots(), &held);
    let sized = size::size_pairs(&selected, account.equity(), &context.sizing);
    let (approved, refusals) = gate.admit_all(&sized);
    summary.entries_refused = refusals
        .iter()
        .map(|block| format!("{}: {block}", block.as_str()))
        .collect();

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
            break;
        }

        match execute::open_pair(
            context.trading,
            pair,
            screened.model_run_id.clone(),
            context.execution,
        )
        .await?
        {
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
                summary.pairs_opened.push(entry.pair_id().to_string());
            }
            OpenOutcome::Abandoned { ticker, reason } => {
                warn!(%ticker, reason, "Pair entry abandoned");
                summary.entries_refused.push(format!("unfilled:{ticker}"));
            }
        }
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
    now: DateTime<Utc>,
) -> Result<LiquidationSummary, EvaluationError> {
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
) -> Result<HashMap<Ticker, f64>, EvaluationError> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }
    let snapshots = client.fetch_snapshots(symbols).await?;
    Ok(snapshots
        .into_iter()
        .filter_map(|snapshot| {
            let price = snapshot.reference_price()?;
            Some((snapshot.ticker().clone(), price))
        })
        .collect())
}

/// Prices the open book and closes whatever the spread says to close.
///
/// Returns the identifiers of the pairs that were closed. A pair with no price or no rebuildable
/// spread model is left open with a warning — the pre-close liquidation closes it regardless, so
/// the worst case is holding it until 15:45 rather than exiting on a signal.
async fn close_what_should_close(
    context: &EvaluationContext<'_>,
    open_pairs: &[OpenPair],
    prices: &HashMap<Ticker, f64>,
    summary: &mut EvaluationSummary,
) -> Result<HashSet<uuid::Uuid>, EvaluationError> {
    let models = screen::exit_models(
        open_pairs
            .iter()
            .map(|pair| (pair.pair_id(), pair.hedge_ratio())),
        context.close_history,
    );

    let mut closed = HashSet::new();
    for pair in open_pairs {
        let (Some(long_price), Some(short_price)) = (
            prices.get(pair.long_ticker()),
            prices.get(pair.short_ticker()),
        ) else {
            summary.pairs_unpriced += 1;
            warn!(pair_id = %pair.pair_id(), "Open pair could not be priced this pass");
            continue;
        };
        let Some(model) = models.get(pair.pair_id()) else {
            warn!(pair_id = %pair.pair_id(), "Open pair has no rebuildable spread model");
            continue;
        };
        let Some(z_score) = model.z_score(*long_price, *short_price) else {
            continue;
        };
        let Some(mut reason) = exit_reason(z_score) else {
            continue;
        };

        // One pair's close failing must not take the other pairs' exits down with it. Propagating
        // here would abort the loop, so a single broker error on the first pair would leave every
        // later pair open for another five minutes — turning one stuck position into all of them.
        // The pair stays open in the record, which is the honest state, and the next pass retries.
        let outcome =
            match execute::close_pair(context.trading, pair.long_ticker(), pair.short_ticker())
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    error!(
                        pair_id = %pair.pair_id(),
                        %error,
                        "Closing a pair failed; it stays open and the next pass retries"
                    );
                    summary.exits_failed.push(pair.pair_id().to_string());
                    continue;
                }
            };
        if outcome.was_already_gone() {
            reason = CloseReason::PositionMissing;
        }
        pairs::record_close(context.pool, pair.id(), reason, context.now).await?;

        closed.insert(pair.id());
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
}

/// Assembles the screen's inputs, fetching only the prices the exit half did not already have.
async fn build_screen_inputs(
    context: &EvaluationContext<'_>,
    held: &HashSet<Ticker>,
    prices: &mut HashMap<Ticker, f64>,
) -> Result<ScreenedUniverse, EvaluationError> {
    let (start, end) = SessionDate::at(context.now).bounds();
    let predictions = predict::load_predictions_between(context.pool, start, end).await?;
    if predictions.is_empty() {
        info!("No predictions for the current session; no entries will be screened");
        return Ok(ScreenedUniverse {
            inputs: Vec::new(),
            model_run_id: None,
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
                *prices.get(ticker)?,
                sectors.get(ticker)?.clone(),
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
        const _: () = assert!(ENTRY_Z_SCORE < STOP_LOSS_Z_SCORE);
        assert_eq!(exit_reason(ENTRY_Z_SCORE), None);
    }

    #[test]
    fn test_a_spread_back_through_its_mean_is_convergence() {
        assert_eq!(exit_reason(0.0), Some(CloseReason::Convergence));
        assert_eq!(exit_reason(-1.5), Some(CloseReason::Convergence));
    }

    #[test]
    fn test_a_spread_widening_past_the_stop_is_a_stop_loss() {
        assert_eq!(exit_reason(STOP_LOSS_Z_SCORE), Some(CloseReason::StopLoss));
        assert_eq!(exit_reason(7.0), Some(CloseReason::StopLoss));
    }

    /// Between the two thresholds the pair is working as intended and is held.
    #[test]
    fn test_a_spread_inside_the_band_is_held() {
        assert_eq!(exit_reason(1.0), None);
        assert_eq!(exit_reason(3.9), None);
    }

    /// A non-finite reading is not an exit signal in either direction. Treating it as convergence
    /// would close every pair the moment a price went bad; treating it as a stop would do the same
    /// and record the loss reason.
    #[test]
    fn test_a_non_finite_reading_is_not_an_exit_signal() {
        assert_eq!(exit_reason(f64::NAN), None);
        assert_eq!(exit_reason(f64::INFINITY), None);
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
