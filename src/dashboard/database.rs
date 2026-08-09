//! Read-only queries behind the dashboard, and the aggregations computed from them.
//!
//! Every query uses raw `sqlx::query` rather than the compile-time macros, so the dashboard adds
//! no entries to the `.sqlx` offline cache. It connects as `dashboard_reader`, which holds SELECT
//! on exactly the tables below and nothing else.
//!
//! The aggregations are separated from the queries deliberately: each is a pure function over rows
//! it was handed, which is what makes win rate, profit factor, and period returns testable without
//! a database.

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use tracing::warn;

use crate::common::alpaca::TRANSFER_ACTIVITY_TYPES;
use crate::common::types::{PairID, Ticker};
use crate::dashboard::cache::{
    AccountSnapshot, ClosedPair, ClosedSummary, EventEntry, OpenPair, PeriodReturns, Prediction,
};
use crate::data::calendar::SessionDate;
use crate::portfolio::pairs::CloseReason;

/// Sessions of account history fetched for the equity curve.
const EQUITY_HISTORY_DAYS: i64 = 365;

/// Closed pairs fetched for the trade table and its summary.
const CLOSED_PAIRS_LIMIT: i64 = 200;

/// Rows shown from the most recent prediction batch.
const PREDICTIONS_LIMIT: i64 = 50;

/// Events read from the table to seed the ring buffer at startup.
const RECENT_EVENTS_LIMIT: i64 = 100;

/// One poll cycle's worth of data.
pub struct DashboardData {
    pub equity_history: Vec<AccountSnapshot>,
    pub period_returns: PeriodReturns,
    pub open_pairs: Vec<OpenPair>,
    pub closed_pairs: Vec<ClosedPair>,
    pub closed_summary: ClosedSummary,
    pub predictions: Vec<Prediction>,
    pub prediction_model_run_id: Option<String>,
    pub prediction_timestamp: Option<DateTime<Utc>>,
    pub recent_events: Vec<EventEntry>,
    pub latest_bars_inserted_at: Option<DateTime<Utc>>,
}

/// Runs every query for one refresh. A failure in any one fails the cycle, and the caller keeps
/// the previous state.
pub async fn fetch_dashboard_data(pool: &PgPool) -> Result<DashboardData, sqlx::Error> {
    let equity_history = fetch_equity_history(pool).await?;
    let transfer_sessions = match (equity_history.first(), equity_history.last()) {
        (Some(first), Some(last)) => {
            fetch_transfer_sessions(pool, first.session_date, last.session_date).await?
        }
        // No history means no baseline to measure from, so there is nothing a transfer could spoil.
        _ => Vec::new(),
    };
    let period_returns = compute_period_returns(&equity_history, &transfer_sessions);
    let open_pairs = fetch_open_pairs(pool).await?;
    let closed_pairs = fetch_closed_pairs(pool).await?;
    let closed_summary = compute_closed_summary(&closed_pairs);
    let (predictions, prediction_model_run_id, prediction_timestamp) =
        fetch_latest_predictions(pool).await?;
    let recent_events = fetch_recent_events(pool).await?;
    let latest_bars_inserted_at = fetch_latest_bars_inserted_at(pool).await?;

    Ok(DashboardData {
        equity_history,
        period_returns,
        open_pairs,
        closed_pairs,
        closed_summary,
        predictions,
        prediction_model_run_id,
        prediction_timestamp,
        recent_events,
        latest_bars_inserted_at,
    })
}

/// Fetches the account history oldest-first, which is the order every horizon calculation wants.
///
/// Balances come back as `Option`, because a session reconstructed by `backfill_account_snapshots`
/// carries equity and four NULLs. Only `equity` is guaranteed, and it is the only column the
/// horizon calculations read.
async fn fetch_equity_history(pool: &PgPool) -> Result<Vec<AccountSnapshot>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT session_date, equity, cash, buying_power, long_market_value, short_market_value
         FROM account_snapshots
         WHERE session_date >= CURRENT_DATE - ($1::BIGINT || ' days')::INTERVAL
         ORDER BY session_date ASC",
    )
    .bind(EQUITY_HISTORY_DAYS)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(AccountSnapshot {
                session_date: SessionDate::from_date(row.try_get("session_date")?),
                equity: row.try_get("equity")?,
                cash: row.try_get("cash")?,
                buying_power: row.try_get("buying_power")?,
                long_market_value: row.try_get("long_market_value")?,
                short_market_value: row.try_get("short_market_value")?,
            })
        })
        .collect()
}

/// Fetches the sessions in which capital moved into or out of the account, within a session range.
///
/// Bounded to the equity history the dashboard displays, because a transfer outside it cannot change
/// any published figure: every baseline [`compute_period_returns`] measures from comes out of that
/// same history. Bounding also keeps the scan off the whole table —
/// `idx_account_activities_transaction_time` covers this predicate, and there is no index on
/// `activity_type`.
///
/// Timestamps come back raw and become sessions through [`SessionDate::at`], rather than being
/// converted in SQL: `(transaction_time AT TIME ZONE 'America/New_York')::date` would hide the column
/// behind an expression and defeat that index, which is the same reasoning
/// [`SessionDate::bounds`] documents for itself.
async fn fetch_transfer_sessions(
    pool: &PgPool,
    first: SessionDate,
    last: SessionDate,
) -> Result<Vec<SessionDate>, sqlx::Error> {
    let (start, _) = first.bounds();
    let (_, end) = last.bounds();

    let rows = sqlx::query(
        "SELECT transaction_time
         FROM account_activities
         WHERE activity_type = ANY($1)
           AND transaction_time >= $2
           AND transaction_time < $3",
    )
    .bind(TRANSFER_ACTIVITY_TYPES)
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(SessionDate::at(
                row.try_get::<DateTime<Utc>, _>("transaction_time")?,
            ))
        })
        .collect()
}

/// Fetches open pairs, newest first.
///
/// A row whose stored tickers no longer parse is skipped with a warning rather than failing the
/// poll. The dashboard is an observer; a malformed row is something to report, not a reason to
/// take the page down.
async fn fetch_open_pairs(pool: &PgPool) -> Result<Vec<OpenPair>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT pair_id, long_ticker, short_ticker, hedge_ratio, entry_z_score,
                signal_strength, opened_at
         FROM equity_pairs
         WHERE status = 'open'
         ORDER BY opened_at DESC",
    )
    .fetch_all(pool)
    .await?;

    let mut pairs = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_pair_id: String = row.try_get("pair_id")?;
        let raw_long: String = row.try_get("long_ticker")?;
        let raw_short: String = row.try_get("short_ticker")?;

        let (Some(pair_id), Some(long_ticker), Some(short_ticker)) = (
            PairID::parse(&raw_pair_id),
            Ticker::new(&raw_long),
            Ticker::new(&raw_short),
        ) else {
            warn!(pair_id = %raw_pair_id, "Open pair has unparseable identifiers, skipping");
            continue;
        };

        pairs.push(OpenPair {
            pair_id,
            long_ticker,
            short_ticker,
            hedge_ratio: row.try_get("hedge_ratio")?,
            entry_z_score: row.try_get("entry_z_score")?,
            signal_strength: row.try_get("signal_strength")?,
            opened_at: row.try_get("opened_at")?,
        });
    }
    Ok(pairs)
}

/// Fetches recently closed pairs, newest first.
///
/// `closed_at` and `close_reason` are typed as nullable in the table but cannot be null on a closed
/// row: `equity_pairs_closure_is_consistent` rejects that combination. The filter states the
/// constraint anyway so the query does not depend on it, and a reason outside the vocabulary is
/// skipped rather than guessed at.
async fn fetch_closed_pairs(pool: &PgPool) -> Result<Vec<ClosedPair>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT pair_id, long_ticker, short_ticker, entry_z_score, realized_profit_and_loss,
                close_reason, opened_at, closed_at
         FROM equity_pairs
         WHERE status = 'closed' AND closed_at IS NOT NULL AND close_reason IS NOT NULL
         ORDER BY closed_at DESC
         LIMIT $1",
    )
    .bind(CLOSED_PAIRS_LIMIT)
    .fetch_all(pool)
    .await?;

    let mut pairs = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_pair_id: String = row.try_get("pair_id")?;
        let raw_long: String = row.try_get("long_ticker")?;
        let raw_short: String = row.try_get("short_ticker")?;
        let raw_reason: String = row.try_get("close_reason")?;

        let (Some(pair_id), Some(long_ticker), Some(short_ticker), Some(close_reason)) = (
            PairID::parse(&raw_pair_id),
            Ticker::new(&raw_long),
            Ticker::new(&raw_short),
            CloseReason::parse(&raw_reason),
        ) else {
            warn!(
                pair_id = %raw_pair_id,
                close_reason = %raw_reason,
                "Closed pair has unparseable fields, skipping"
            );
            continue;
        };

        pairs.push(ClosedPair {
            pair_id,
            long_ticker,
            short_ticker,
            entry_z_score: row.try_get("entry_z_score")?,
            realized_profit_and_loss: row.try_get("realized_profit_and_loss")?,
            close_reason,
            opened_at: row.try_get("opened_at")?,
            closed_at: row.try_get("closed_at")?,
        });
    }
    Ok(pairs)
}

/// Fetches the most recent prediction batch, ranked by median forecast.
///
/// A batch is identified by its shared `correlation_id`, not by timestamp: selecting on the maximum
/// timestamp alone would mix rows from two runs if one ever wrote a ticker the other did not.
async fn fetch_latest_predictions(
    pool: &PgPool,
) -> Result<(Vec<Prediction>, Option<String>, Option<DateTime<Utc>>), sqlx::Error> {
    let rows = sqlx::query(
        "WITH latest_batch AS (
             SELECT correlation_id
             FROM equity_predictions
             ORDER BY timestamp DESC
             LIMIT 1
         )
         SELECT p.ticker, p.quantile_10, p.quantile_50, p.quantile_90,
                p.model_run_id, p.timestamp
         FROM equity_predictions p
         JOIN latest_batch b ON p.correlation_id = b.correlation_id
         ORDER BY p.quantile_50 DESC
         LIMIT $1",
    )
    .bind(PREDICTIONS_LIMIT)
    .fetch_all(pool)
    .await?;

    let mut model_run_id = None;
    let mut timestamp = None;
    let mut predictions = Vec::with_capacity(rows.len());

    for row in rows {
        let raw_ticker: String = row.try_get("ticker")?;
        let Some(ticker) = Ticker::new(&raw_ticker) else {
            warn!(ticker = %raw_ticker, "Prediction has an unparseable ticker, skipping");
            continue;
        };
        if model_run_id.is_none() {
            model_run_id = Some(row.try_get::<String, _>("model_run_id")?);
            timestamp = Some(row.try_get::<DateTime<Utc>, _>("timestamp")?);
        }
        predictions.push(Prediction {
            ticker,
            quantile_10: row.try_get("quantile_10")?,
            quantile_50: row.try_get("quantile_50")?,
            quantile_90: row.try_get("quantile_90")?,
        });
    }

    Ok((predictions, model_run_id, timestamp))
}

/// Seeds the event ring buffer from the table, newest first.
async fn fetch_recent_events(pool: &PgPool) -> Result<Vec<EventEntry>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT event_type, payload, created_at
         FROM events
         ORDER BY created_at DESC, id DESC
         LIMIT $1",
    )
    .bind(RECENT_EVENTS_LIMIT)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(EventEntry {
                event_type: row.try_get("event_type")?,
                payload: row.try_get("payload")?,
                created_at: row.try_get("created_at")?,
            })
        })
        .collect()
}

/// The freshness signal: when the most recent daily bar was written.
async fn fetch_latest_bars_inserted_at(
    pool: &PgPool,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT MAX(inserted_at) AS latest FROM equity_bars WHERE bar_interval = 'one_day'",
    )
    .fetch_one(pool)
    .await?;
    row.try_get("latest")
}

/// Percentage change between two equity values.
///
/// Returns `None` for a zero or negative baseline: a percentage against zero equity is a division
/// by zero, and against negative equity it changes sign without changing meaning.
fn percentage_change(baseline: Decimal, latest: Decimal) -> Option<f64> {
    if baseline <= Decimal::ZERO {
        return None;
    }
    let baseline = baseline.to_f64()?;
    let latest = latest.to_f64()?;
    Some((latest - baseline) / baseline * 100.0)
}

/// The last snapshot at or before `cutoff`, which is the baseline for a horizon.
///
/// "At or before" rather than "exactly on": the series is sessions, not calendar days, so the date
/// exactly one week back is a Saturday two times in seven.
fn baseline_at_or_before(
    history: &[AccountSnapshot],
    cutoff: SessionDate,
) -> Option<&AccountSnapshot> {
    history
        .iter()
        .rev()
        .find(|snapshot| snapshot.session_date <= cutoff)
}

/// Computes the return over each horizon from an ascending-ordered account history.
///
/// **Flow-blind by construction.** Every figure here is a raw equity-to-equity change, so a deposit
/// inside a horizon reads as performance — a $10,000 contribution into a flat $20,000 book publishes
/// as +50%. Any horizon spanning a capital flow therefore returns `None` rather than a plausible
/// wrong number. `nav_per_unit` replaces this calculation outright when the unit ledger lands, at
/// which point the guard has nothing left to protect; see `.scratchpad/unit_accounting.md`.
pub fn compute_period_returns(
    history: &[AccountSnapshot],
    transfer_sessions: &[SessionDate],
) -> PeriodReturns {
    let Some(latest) = history.last() else {
        return PeriodReturns::default();
    };

    // Flows are treated as arriving at the end of their session, so a transfer *on* the baseline
    // session is already inside the baseline equity and does not distort the comparison. Anything
    // strictly after it does.
    let change_from = |baseline: &AccountSnapshot| {
        let spans_a_flow = transfer_sessions
            .iter()
            .any(|session| *session > baseline.session_date && *session <= latest.session_date);
        if spans_a_flow {
            return None;
        }
        percentage_change(baseline.equity, latest.equity)
    };

    let horizon = |days: i64| {
        baseline_at_or_before(history, latest.session_date.plus_calendar_days(-days))
            .and_then(change_from)
    };

    // Year to date measures from the last session of the previous year, so the first session of
    // January is a full day of performance rather than a flat zero.
    let year_start = NaiveDate::from_ymd_opt(latest.session_date.date().year(), 1, 1);
    let year_to_date = year_start
        .and_then(|start| baseline_at_or_before(history, SessionDate::from_date(start.pred_opt()?)))
        .or_else(|| history.first())
        .and_then(change_from);

    PeriodReturns {
        one_day: horizon(1),
        one_week: horizon(7),
        one_month: horizon(30),
        year_to_date,
        since_inception: history.first().and_then(change_from),
    }
}

/// Computes the aggregates shown under the closed-pair table.
pub fn compute_closed_summary(closed: &[ClosedPair]) -> ClosedSummary {
    if closed.is_empty() {
        return ClosedSummary {
            by_close_reason: CloseReason::ALL
                .into_iter()
                .map(|reason| (reason, 0))
                .collect(),
            ..ClosedSummary::default()
        };
    }

    let realized: Vec<Decimal> = closed
        .iter()
        .filter_map(|pair| pair.realized_profit_and_loss)
        .collect();

    let wins = realized
        .iter()
        .filter(|value| **value > Decimal::ZERO)
        .count();
    let losses = realized
        .iter()
        .filter(|value| **value < Decimal::ZERO)
        .count();
    let decided = wins + losses;

    let gross_profit: Decimal = realized
        .iter()
        .filter(|value| **value > Decimal::ZERO)
        .sum();
    let gross_loss: Decimal = realized
        .iter()
        .filter(|value| **value < Decimal::ZERO)
        .sum::<Decimal>()
        .abs();

    let total_realized: Decimal = realized.iter().sum();
    let average_realized = if realized.is_empty() {
        None
    } else {
        Some(total_realized / Decimal::from(realized.len()))
    };

    // A book with no losing trade has no profit factor rather than an infinite one. Reporting
    // infinity here would put the most flattering number on the smallest sample.
    let profit_factor = if gross_loss > Decimal::ZERO {
        gross_profit
            .to_f64()
            .zip(gross_loss.to_f64())
            .map(|(profit, loss)| profit / loss)
    } else {
        None
    };

    let by_close_reason: Vec<(CloseReason, usize)> = CloseReason::ALL
        .into_iter()
        .map(|reason| {
            let count = closed
                .iter()
                .filter(|pair| pair.close_reason == reason)
                .count();
            (reason, count)
        })
        .collect();

    let signal_exits = closed
        .iter()
        .filter(|pair| pair.close_reason.is_signal())
        .count();

    let average_holding_hours =
        closed.iter().map(ClosedPair::holding_hours).sum::<f64>() / closed.len() as f64;

    ClosedSummary {
        total_closed: closed.len(),
        wins,
        losses,
        win_rate: if decided > 0 {
            Some(wins as f64 / decided as f64 * 100.0)
        } else {
            None
        },
        total_realized,
        average_realized,
        profit_factor,
        average_holding_hours: Some(average_holding_hours),
        by_close_reason,
        signal_exit_share: Some(signal_exits as f64 / closed.len() as f64 * 100.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use std::str::FromStr;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("a valid test decimal")
    }

    fn snapshot(date: &str, equity: &str) -> AccountSnapshot {
        AccountSnapshot {
            session_date: SessionDate::from_date(
                NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("a valid test date"),
            ),
            equity: decimal(equity),
            // The horizon calculations read `equity` and `session_date` and nothing else, so these
            // are left absent rather than zeroed — a zero balance is a claim, `None` is not.
            cash: None,
            buying_power: None,
            long_market_value: None,
            short_market_value: None,
        }
    }

    fn closed(realized: Option<&str>, reason: CloseReason, hours: i64) -> ClosedPair {
        let opened_at = Utc.with_ymd_and_hms(2026, 7, 30, 14, 0, 0).unwrap();
        ClosedPair {
            pair_id: PairID::parse("AAA-BBB").expect("a valid pair"),
            long_ticker: Ticker::new("AAA").expect("a valid ticker"),
            short_ticker: Ticker::new("BBB").expect("a valid ticker"),
            entry_z_score: 2.5,
            realized_profit_and_loss: realized.map(decimal),
            close_reason: reason,
            opened_at,
            closed_at: opened_at + Duration::hours(hours),
        }
    }

    #[test]
    fn test_period_returns_are_empty_without_history() {
        assert_eq!(compute_period_returns(&[], &[]), PeriodReturns::default());
    }

    #[test]
    fn test_a_horizon_longer_than_the_history_has_no_answer() {
        let history = vec![
            snapshot("2026-07-30", "100000"),
            snapshot("2026-07-31", "101000"),
        ];
        let returns = compute_period_returns(&history, &[]);
        assert_eq!(returns.one_day, Some(1.0));
        // Nothing sits at or before 2026-07-24, so a weekly return cannot be computed.
        assert_eq!(returns.one_week, None);
        assert_eq!(returns.since_inception, Some(1.0));
    }

    /// The baseline for a horizon is the last session at or before the cutoff, because the
    /// calendar date one week back lands on a weekend two times in seven.
    #[test]
    fn test_a_horizon_falling_on_a_weekend_uses_the_prior_session() {
        let history = vec![
            snapshot("2026-07-24", "100000"),
            snapshot("2026-07-27", "104000"),
            snapshot("2026-07-31", "110000"),
        ];
        // 2026-07-31 minus seven days is 2026-07-24, a Friday, and it is in the series.
        assert_eq!(compute_period_returns(&history, &[]).one_week, Some(10.0));

        let shifted = vec![
            snapshot("2026-07-23", "100000"),
            snapshot("2026-07-27", "104000"),
            snapshot("2026-07-31", "110000"),
        ];
        // The cutoff now falls on a day with no session, so the prior one is used.
        assert_eq!(compute_period_returns(&shifted, &[]).one_week, Some(10.0));
    }

    #[test]
    fn test_year_to_date_measures_from_the_last_session_of_the_prior_year() {
        let history = vec![
            snapshot("2025-12-31", "100000"),
            snapshot("2026-01-02", "102000"),
            snapshot("2026-07-31", "120000"),
        ];
        assert_eq!(
            compute_period_returns(&history, &[]).year_to_date,
            Some(20.0)
        );
    }

    #[test]
    fn test_year_to_date_falls_back_to_inception_within_the_first_year() {
        let history = vec![
            snapshot("2026-01-02", "100000"),
            snapshot("2026-07-31", "125000"),
        ];
        assert_eq!(
            compute_period_returns(&history, &[]).year_to_date,
            Some(25.0)
        );
    }

    /// A zero baseline is a division by zero, and it is reachable: the first snapshot of an account
    /// that has been funded but holds nothing.
    #[test]
    fn test_a_zero_baseline_yields_no_return_rather_than_an_infinity() {
        let history = vec![
            snapshot("2026-07-30", "0"),
            snapshot("2026-07-31", "100000"),
        ];
        let returns = compute_period_returns(&history, &[]);
        assert_eq!(returns.one_day, None);
        assert_eq!(returns.since_inception, None);
    }

    fn session(date: &str) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("a valid test date"),
        )
    }

    /// The bug this guard exists for: a flat book that receives a deposit publishes the deposit as
    /// performance. $10,000 into a flat $20,000 book reads as +50%, and the error never washes out
    /// of a cumulative series. Withholding the figure is the only correct answer until the return
    /// series comes from `nav_per_unit`.
    #[test]
    fn test_a_horizon_spanning_a_capital_flow_publishes_no_return() {
        let history = vec![
            snapshot("2026-07-30", "20000"),
            snapshot("2026-07-31", "30000"),
        ];

        let unguarded = compute_period_returns(&history, &[]);
        assert_eq!(
            unguarded.one_day,
            Some(50.0),
            "without the flow this really is a 50% gain"
        );

        let guarded = compute_period_returns(&history, &[session("2026-07-31")]);
        assert_eq!(guarded.one_day, None);
        assert_eq!(guarded.since_inception, None);
    }

    /// Flows land at the end of their session, so a transfer *on* the baseline session is already
    /// inside the baseline equity. Refusing there would blank the dashboard for a year after a
    /// contribution that does not affect the comparison at all.
    #[test]
    fn test_a_flow_on_the_baseline_session_does_not_withhold_the_return() {
        let history = vec![
            snapshot("2026-07-30", "20000"),
            snapshot("2026-07-31", "22000"),
        ];
        let returns = compute_period_returns(&history, &[session("2026-07-30")]);
        assert_eq!(returns.one_day, Some(10.0));
    }

    /// A flow outside the window says nothing about the window. Only horizons reaching back across
    /// it are withheld, so a deposit does not blank every figure on the page forever.
    #[test]
    fn test_a_flow_outside_the_window_leaves_shorter_horizons_intact() {
        let history = vec![
            snapshot("2026-07-24", "100000"),
            snapshot("2026-07-27", "100000"),
            snapshot("2026-07-31", "110000"),
        ];
        let returns = compute_period_returns(&history, &[session("2026-07-27")]);
        assert_eq!(
            returns.one_week, None,
            "the weekly baseline is 2026-07-24, so its window spans the flow"
        );
        assert_eq!(
            returns.one_day,
            Some(10.0),
            "the daily baseline is 2026-07-27 itself, so the flow is already inside it"
        );
    }

    #[test]
    fn test_an_empty_close_summary_still_lists_every_reason_at_zero() {
        let summary = compute_closed_summary(&[]);
        assert_eq!(summary.total_closed, 0);
        assert_eq!(summary.by_close_reason.len(), CloseReason::ALL.len());
        assert!(summary.by_close_reason.iter().all(|(_, count)| *count == 0));
    }

    #[test]
    fn test_win_rate_and_profit_factor_over_a_mixed_book() {
        let pairs = vec![
            closed(Some("300"), CloseReason::Convergence, 4),
            closed(Some("100"), CloseReason::Convergence, 6),
            closed(Some("-200"), CloseReason::StopLoss, 2),
        ];
        let summary = compute_closed_summary(&pairs);
        assert_eq!(summary.total_closed, 3);
        assert_eq!(summary.wins, 2);
        assert_eq!(summary.losses, 1);
        let win_rate = summary.win_rate.expect("a win rate over a decided book");
        assert!(
            (win_rate - 200.0 / 3.0).abs() < 1e-9,
            "expected two thirds, got {win_rate}"
        );
        assert_eq!(summary.total_realized, decimal("200"));
        assert_eq!(summary.profit_factor, Some(2.0));
        assert_eq!(summary.average_holding_hours, Some(4.0));
    }

    /// Infinity is the most flattering number available and it appears on the smallest sample, so
    /// a book with no losing trade reports no profit factor at all.
    #[test]
    fn test_no_losing_trade_yields_no_profit_factor() {
        let pairs = vec![closed(Some("300"), CloseReason::Convergence, 4)];
        assert_eq!(compute_closed_summary(&pairs).profit_factor, None);
    }

    /// The measurement the section exists for: a book that only ever exits at the pre-close
    /// fail-safe is holding for a shorter period than it forecasts.
    #[test]
    fn test_the_signal_exit_share_separates_opinion_from_the_fail_safe() {
        let pairs = vec![
            closed(Some("300"), CloseReason::Convergence, 4),
            closed(Some("-50"), CloseReason::EndOfDay, 6),
            closed(Some("-20"), CloseReason::EndOfDay, 6),
            closed(None, CloseReason::PositionMissing, 6),
        ];
        let summary = compute_closed_summary(&pairs);
        assert_eq!(summary.signal_exit_share, Some(25.0));

        let count_for = |wanted: CloseReason| {
            summary
                .by_close_reason
                .iter()
                .find(|(reason, _)| *reason == wanted)
                .map(|(_, count)| *count)
                .expect("every reason is listed")
        };
        assert_eq!(count_for(CloseReason::Convergence), 1);
        assert_eq!(count_for(CloseReason::EndOfDay), 2);
        assert_eq!(count_for(CloseReason::StopLoss), 0);
    }

    /// A pair with no attributed profit and loss is still a close. It counts toward the exit-reason
    /// breakdown and the holding period, and it must not count as a loss.
    #[test]
    fn test_an_unattributed_pair_counts_as_a_close_but_not_as_a_loss() {
        let pairs = vec![
            closed(Some("100"), CloseReason::Convergence, 4),
            closed(None, CloseReason::PositionMissing, 4),
        ];
        let summary = compute_closed_summary(&pairs);
        assert_eq!(summary.total_closed, 2);
        assert_eq!(summary.wins, 1);
        assert_eq!(summary.losses, 0);
        assert_eq!(summary.win_rate, Some(100.0));
        assert_eq!(summary.average_realized, Some(decimal("100")));
    }
}
