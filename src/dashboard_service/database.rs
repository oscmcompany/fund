//! Read-only database queries for the dashboard service.
//!
//! All queries use raw `sqlx::query` (no compile-time macros) since the
//! dashboard connects to production with a read-only user whose schema
//! matches the live database, not the local `.sqlx` cache.

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};

use crate::dashboard_service::cache::{
    ClosedTrade, ClosedTradesSummary, OpenPosition, PerformanceSnapshot, PeriodReturns,
    PredictionRow,
};

/// How many days of performance snapshot history to fetch for Tab 2.
const PERFORMANCE_HISTORY_DAYS: i64 = 365;

/// Maximum number of closed trades to fetch for Tab 3.
const CLOSED_TRADES_LIMIT: i64 = 200;

/// All data produced by a single poll cycle, consumed by [`fetch_dashboard_data`].
pub struct DashboardData {
    pub open_positions: Vec<OpenPosition>,
    pub gross_exposure: Decimal,
    pub net_exposure: Decimal,
    pub performance_history: Vec<PerformanceSnapshot>,
    pub period_returns: PeriodReturns,
    pub closed_trades: Vec<ClosedTrade>,
    pub closed_trades_summary: ClosedTradesSummary,
    pub predictions: Vec<PredictionRow>,
}

/// Fetches all data needed for a full dashboard state refresh.
///
/// Executes queries for all four static views sequentially against the
/// read-only pool. A failure in any query returns an error for the whole
/// cycle; the caller retains the previous state and records the error.
pub async fn fetch_dashboard_data(pool: &PgPool) -> Result<DashboardData, sqlx::Error> {
    let open_positions = fetch_open_positions(pool).await?;
    let (gross_exposure, net_exposure) = compute_exposures(&open_positions);
    let performance_history = fetch_performance_history(pool).await?;
    let period_returns = compute_period_returns(&performance_history);
    let closed_trades = fetch_closed_trades(pool).await?;
    let closed_trades_summary = compute_closed_trades_summary(&closed_trades);
    let predictions = fetch_latest_predictions(pool).await?;

    Ok(DashboardData {
        open_positions,
        gross_exposure,
        net_exposure,
        performance_history,
        period_returns,
        closed_trades,
        closed_trades_summary,
        predictions,
    })
}

/// Fetches all open long/short pair positions with per-leg dollar amounts.
///
/// Dollar amounts are summed from `equity_allocations` grouped by side so
/// a single pair row represents both legs.
async fn fetch_open_positions(pool: &PgPool) -> Result<Vec<OpenPosition>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT
             p.pair_id,
             p.long_ticker,
             p.short_ticker,
             p.z_score,
             p.hedge_ratio,
             p.signal_strength,
             p.opened_at,
             COALESCE(SUM(a.dollar_amount) FILTER (WHERE a.side = 'LONG'),  0) AS long_dollar_amount,
             COALESCE(SUM(a.dollar_amount) FILTER (WHERE a.side = 'SHORT'), 0) AS short_dollar_amount
         FROM equity_pairs p
         LEFT JOIN equity_allocations a ON a.equity_pair_id = p.id
         WHERE p.status = 'open'
         GROUP BY p.id, p.pair_id, p.long_ticker, p.short_ticker,
                  p.z_score, p.hedge_ratio, p.signal_strength, p.opened_at
         ORDER BY p.opened_at DESC",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(OpenPosition {
                pair_id: row.try_get::<crate::domain::market::PairID, _>("pair_id")?,
                long_ticker: row.try_get::<crate::domain::market::Ticker, _>("long_ticker")?,
                short_ticker: row.try_get::<crate::domain::market::Ticker, _>("short_ticker")?,
                z_score: row.try_get("z_score")?,
                hedge_ratio: row.try_get("hedge_ratio")?,
                signal_strength: row.try_get("signal_strength")?,
                long_dollar_amount: row.try_get("long_dollar_amount")?,
                short_dollar_amount: row.try_get("short_dollar_amount")?,
                opened_at: row.try_get("opened_at")?,
            })
        })
        .collect()
}

/// Computes gross and net exposure from the set of open positions.
///
/// Returns `(Decimal::ZERO, Decimal::ZERO)` when no positions are open.
fn compute_exposures(positions: &[OpenPosition]) -> (Decimal, Decimal) {
    if positions.is_empty() {
        return (Decimal::ZERO, Decimal::ZERO);
    }
    let gross: Decimal = positions
        .iter()
        .map(|position| position.long_dollar_amount + position.short_dollar_amount)
        .sum();
    let net: Decimal = positions
        .iter()
        .map(|position| position.long_dollar_amount - position.short_dollar_amount)
        .sum();
    (gross, net)
}

/// Fetches portfolio NAV snapshots for the past year joined with SPY close prices.
///
/// SPY closes are pulled from `equity_bars` by matching date so the performance
/// view can display a benchmark comparison without a separate SPY data source.
/// Rows without a matching SPY bar have `spy_close = None`.
async fn fetch_performance_history(pool: &PgPool) -> Result<Vec<PerformanceSnapshot>, sqlx::Error> {
    let cutoff: DateTime<Utc> = Utc::now() - Duration::days(PERFORMANCE_HISTORY_DAYS);
    let rows = sqlx::query(
        "SELECT
             s.snapshot_timestamp,
             s.net_asset_value,
             s.gross_return,
             s.net_return,
             s.total_slippage_cost,
             b.close_price AS spy_close
         FROM equity_portfolio_snapshots s
         LEFT JOIN equity_bars b
             ON b.ticker = 'SPY'
             AND b.timestamp::date = s.snapshot_timestamp::date
         WHERE s.snapshot_timestamp >= $1
         ORDER BY s.snapshot_timestamp DESC",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PerformanceSnapshot {
                snapshot_timestamp: row.try_get("snapshot_timestamp")?,
                net_asset_value: row.try_get("net_asset_value")?,
                gross_return: row.try_get("gross_return")?,
                net_return: row.try_get("net_return")?,
                total_slippage_cost: row.try_get("total_slippage_cost")?,
                spy_close: row.try_get("spy_close")?,
            })
        })
        .collect()
}

/// Fetches the most recent closed pair trades up to [`CLOSED_TRADES_LIMIT`].
async fn fetch_closed_trades(pool: &PgPool) -> Result<Vec<ClosedTrade>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT
             pair_id,
             long_ticker,
             short_ticker,
             realized_profit_and_loss,
             return_percent,
             EXTRACT(EPOCH FROM (closed_at - opened_at))::BIGINT AS holding_seconds,
             close_reason,
             closed_at
         FROM equity_pairs
         WHERE status = 'closed'
         ORDER BY closed_at DESC
         LIMIT $1",
    )
    .bind(CLOSED_TRADES_LIMIT)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(ClosedTrade {
                pair_id: row.try_get::<crate::domain::market::PairID, _>("pair_id")?,
                long_ticker: row.try_get::<crate::domain::market::Ticker, _>("long_ticker")?,
                short_ticker: row.try_get::<crate::domain::market::Ticker, _>("short_ticker")?,
                realized_profit_and_loss: row.try_get("realized_profit_and_loss")?,
                return_percent: row.try_get("return_percent")?,
                holding_seconds: row.try_get("holding_seconds")?,
                close_reason: row.try_get("close_reason")?,
                closed_at: row.try_get("closed_at")?,
            })
        })
        .collect()
}

/// Computes aggregate trade statistics from the fetched closed trades.
///
/// All metrics (win rate, profit factor, averages) are computed only from
/// trades that have a non-null `realized_profit_and_loss` convertible to `f64`;
/// `total_closed` reflects the full slice length regardless of null fields.
/// Win rate is `None` when all trades are break-even (no winners or losers).
/// Profit factor is `None` when there are no losing trades (gross loss is zero).
pub fn compute_closed_trades_summary(trades: &[ClosedTrade]) -> ClosedTradesSummary {
    let total_closed = trades.len();
    if total_closed == 0 {
        return ClosedTradesSummary::default();
    }

    let profit_and_loss_values: Vec<f64> = trades
        .iter()
        .filter_map(|trade| {
            trade
                .realized_profit_and_loss
                .and_then(|value| value.to_f64())
        })
        .collect();

    if profit_and_loss_values.is_empty() {
        return ClosedTradesSummary {
            total_closed,
            ..Default::default()
        };
    }

    let winners: Vec<f64> = profit_and_loss_values
        .iter()
        .copied()
        .filter(|&value| value > 0.0)
        .collect();
    let losers: Vec<f64> = profit_and_loss_values
        .iter()
        .copied()
        .filter(|&value| value < 0.0)
        .collect();

    let decided = winners.len() + losers.len();
    let win_rate = (decided > 0).then(|| winners.len() as f64 / decided as f64);

    let gross_profit: f64 = winners.iter().sum();
    let gross_loss: f64 = losers.iter().map(|value| value.abs()).sum();
    let profit_factor = if gross_loss == 0.0 {
        None
    } else {
        Some(gross_profit / gross_loss)
    };

    let return_values: Vec<f64> = trades
        .iter()
        .filter_map(|trade| trade.return_percent.and_then(|value| value.to_f64()))
        .collect();
    let average_return_percent = if return_values.is_empty() {
        None
    } else {
        Some(return_values.iter().sum::<f64>() / return_values.len() as f64)
    };

    let holding_second_values: Vec<f64> = trades
        .iter()
        .filter_map(|trade| trade.holding_seconds.map(|seconds| seconds as f64))
        .collect();
    let average_holding_seconds = if holding_second_values.is_empty() {
        None
    } else {
        Some(holding_second_values.iter().sum::<f64>() / holding_second_values.len() as f64)
    };

    let total_realized_profit_and_loss: Decimal = trades
        .iter()
        .filter_map(|trade| trade.realized_profit_and_loss)
        .sum();

    ClosedTradesSummary {
        total_closed,
        win_rate,
        profit_factor,
        average_return_percent,
        average_holding_seconds,
        total_realized_profit_and_loss: Some(total_realized_profit_and_loss),
    }
}

/// Fetches all predictions from the most recent model run batch.
///
/// Uses a subquery on MAX(timestamp) so the view always shows a coherent
/// single-cycle snapshot rather than mixing predictions across batches.
async fn fetch_latest_predictions(pool: &PgPool) -> Result<Vec<PredictionRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT ticker, quantile_10, quantile_50, quantile_90, model_run_id, timestamp
         FROM equity_predictions
         WHERE timestamp = (SELECT MAX(timestamp) FROM equity_predictions)
         ORDER BY ticker ASC",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PredictionRow {
                ticker: row.try_get::<crate::domain::market::Ticker, _>("ticker")?,
                quantile_10: row.try_get("quantile_10")?,
                quantile_50: row.try_get("quantile_50")?,
                quantile_90: row.try_get("quantile_90")?,
                model_run_id: row.try_get("model_run_id")?,
                timestamp: row.try_get("timestamp")?,
            })
        })
        .collect()
}

/// Returns the first snapshot (newest-to-oldest order) whose timestamp is at or
/// before `cutoff`, giving the most recent baseline for a return period.
fn find_snapshot_at_or_before(
    history: &[PerformanceSnapshot],
    cutoff: DateTime<Utc>,
) -> Option<&PerformanceSnapshot> {
    history
        .iter()
        .find(|snapshot| snapshot.snapshot_timestamp <= cutoff)
}

/// Computes the percentage return from `baseline` NAV to `current_net_asset_value`.
///
/// Returns `None` when baseline NAV converts to zero (division guard).
fn nav_period_return(current_net_asset_value: f64, baseline: &PerformanceSnapshot) -> Option<f64> {
    let baseline_net_asset_value = baseline.net_asset_value.to_f64()?;
    if baseline_net_asset_value == 0.0 {
        return None;
    }
    Some((current_net_asset_value - baseline_net_asset_value) / baseline_net_asset_value * 100.0)
}

/// Computes the percentage return from `baseline` SPY close to `current_spy`.
///
/// Returns `None` when either close price is absent or baseline is zero.
fn spy_period_return(current_spy: f64, baseline: &PerformanceSnapshot) -> Option<f64> {
    let base_spy = baseline.spy_close?;
    if base_spy == 0.0 {
        return None;
    }
    Some((current_spy - base_spy) / base_spy * 100.0)
}

/// Computes period returns from the cached snapshot history.
///
/// `history` is expected to be sorted newest-first (matching the query
/// `ORDER BY snapshot_timestamp DESC`). Returns are expressed as percentages.
/// Any period for which no baseline snapshot exists returns `None`.
pub fn compute_period_returns(history: &[PerformanceSnapshot]) -> PeriodReturns {
    if history.is_empty() {
        return PeriodReturns::default();
    }

    let current = &history[0];
    let Some(current_net_asset_value) = current.net_asset_value.to_f64() else {
        return PeriodReturns::default();
    };
    if current_net_asset_value == 0.0 {
        return PeriodReturns::default();
    }

    let now = current.snapshot_timestamp;
    let one_day_cutoff = now - Duration::days(1);
    let one_week_cutoff = now - Duration::weeks(1);
    let one_month_cutoff = now - Duration::days(30);
    let year_start_cutoff = Utc
        .with_ymd_and_hms(now.year(), 1, 1, 0, 0, 0)
        .single()
        .unwrap_or(now);

    let baseline_one_day = find_snapshot_at_or_before(history, one_day_cutoff);
    let baseline_one_week = find_snapshot_at_or_before(history, one_week_cutoff);
    let baseline_one_month = find_snapshot_at_or_before(history, one_month_cutoff);
    let baseline_year_to_date = find_snapshot_at_or_before(history, year_start_cutoff);
    let inception = history.last();

    let current_spy = current.spy_close;

    PeriodReturns {
        fund_one_day: baseline_one_day.and_then(|b| nav_period_return(current_net_asset_value, b)),
        fund_one_week: baseline_one_week
            .and_then(|b| nav_period_return(current_net_asset_value, b)),
        fund_one_month: baseline_one_month
            .and_then(|b| nav_period_return(current_net_asset_value, b)),
        fund_year_to_date: baseline_year_to_date
            .and_then(|b| nav_period_return(current_net_asset_value, b)),
        fund_since_inception: inception.and_then(|b| nav_period_return(current_net_asset_value, b)),
        spy_one_day: current_spy
            .and_then(|spy| baseline_one_day.and_then(|b| spy_period_return(spy, b))),
        spy_one_week: current_spy
            .and_then(|spy| baseline_one_week.and_then(|b| spy_period_return(spy, b))),
        spy_one_month: current_spy
            .and_then(|spy| baseline_one_month.and_then(|b| spy_period_return(spy, b))),
        spy_year_to_date: current_spy
            .and_then(|spy| baseline_year_to_date.and_then(|b| spy_period_return(spy, b))),
        spy_since_inception: current_spy
            .and_then(|spy| inception.and_then(|b| spy_period_return(spy, b))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_closed_trade(
        profit_and_loss: Option<&str>,
        return_percent: Option<&str>,
        holding_seconds: Option<i64>,
    ) -> ClosedTrade {
        ClosedTrade {
            pair_id: crate::domain::market::PairID::parse("AAPL-MSFT").unwrap(),
            long_ticker: crate::domain::market::Ticker::new("AAPL").unwrap(),
            short_ticker: crate::domain::market::Ticker::new("MSFT").unwrap(),
            realized_profit_and_loss: profit_and_loss
                .map(|s| s.parse::<Decimal>().expect("valid decimal")),
            return_percent: return_percent.map(|s| s.parse::<Decimal>().expect("valid decimal")),
            holding_seconds,
            close_reason: None,
            closed_at: None,
        }
    }

    #[test]
    fn test_compute_exposures_empty() {
        let (gross, net) = compute_exposures(&[]);
        assert_eq!(gross, Decimal::ZERO);
        assert_eq!(net, Decimal::ZERO);
    }

    #[test]
    fn test_compute_exposures_single_position() {
        let position = OpenPosition {
            pair_id: crate::domain::market::PairID::parse("AAPL-MSFT").unwrap(),
            long_ticker: crate::domain::market::Ticker::new("AAPL").unwrap(),
            short_ticker: crate::domain::market::Ticker::new("MSFT").unwrap(),
            z_score: Decimal::new(15, 1),
            hedge_ratio: Decimal::ONE,
            signal_strength: Decimal::new(8, 1),
            long_dollar_amount: Decimal::new(10000, 0),
            short_dollar_amount: Decimal::new(9500, 0),
            opened_at: Utc::now(),
        };
        let (gross, net) = compute_exposures(&[position]);
        assert_eq!(gross, Decimal::new(19500, 0));
        assert_eq!(net, Decimal::new(500, 0));
    }

    #[test]
    fn test_compute_exposures_multiple_positions() {
        let make_position = |long: i64, short: i64| OpenPosition {
            pair_id: crate::domain::market::PairID::parse("AAPL-MSFT").unwrap(),
            long_ticker: crate::domain::market::Ticker::new("AAPL").unwrap(),
            short_ticker: crate::domain::market::Ticker::new("MSFT").unwrap(),
            z_score: Decimal::ONE,
            hedge_ratio: Decimal::ONE,
            signal_strength: Decimal::ONE,
            long_dollar_amount: Decimal::new(long, 0),
            short_dollar_amount: Decimal::new(short, 0),
            opened_at: Utc::now(),
        };
        let positions = vec![make_position(10000, 9000), make_position(8000, 7500)];
        let (gross, net) = compute_exposures(&positions);
        assert_eq!(gross, Decimal::new(34500, 0));
        assert_eq!(net, Decimal::new(1500, 0));
    }

    #[test]
    fn test_compute_closed_trades_summary_empty() {
        let summary = compute_closed_trades_summary(&[]);
        assert_eq!(summary.total_closed, 0);
        assert!(summary.win_rate.is_none());
        assert!(summary.profit_factor.is_none());
        assert!(summary.total_realized_profit_and_loss.is_none());
    }

    #[test]
    fn test_compute_closed_trades_summary_no_pnl_data() {
        let trades = vec![make_closed_trade(None, None, None)];
        let summary = compute_closed_trades_summary(&trades);
        assert_eq!(summary.total_closed, 1);
        assert!(summary.win_rate.is_none());
    }

    #[test]
    fn test_compute_closed_trades_summary_all_winners() {
        // holding_seconds: 5 min and 3 min → average 4 min = 240 seconds
        let trades = vec![
            make_closed_trade(Some("100"), Some("2"), Some(300)),
            make_closed_trade(Some("200"), Some("4"), Some(180)),
        ];
        let summary = compute_closed_trades_summary(&trades);
        assert_eq!(summary.total_closed, 2);
        assert_eq!(summary.win_rate, Some(1.0));
        // No losers means gross_loss == 0 → profit_factor is None.
        assert!(summary.profit_factor.is_none());
        assert_eq!(summary.average_return_percent, Some(3.0));
        assert_eq!(summary.average_holding_seconds, Some(240.0));
        assert_eq!(
            summary.total_realized_profit_and_loss,
            Some(Decimal::new(300, 0))
        );
    }

    #[test]
    fn test_compute_closed_trades_summary_mixed_wins_and_losses() {
        let trades = vec![
            make_closed_trade(Some("100"), Some("2"), Some(400)),
            make_closed_trade(Some("-50"), Some("-1"), Some(200)),
        ];
        let summary = compute_closed_trades_summary(&trades);
        assert_eq!(summary.win_rate, Some(0.5));
        let profit_factor = summary.profit_factor.expect("profit factor is set");
        assert!((profit_factor - 2.0).abs() < 1e-10);
        assert_eq!(
            summary.total_realized_profit_and_loss,
            Some(Decimal::new(50, 0))
        );
    }

    #[test]
    fn test_compute_closed_trades_summary_all_losers() {
        let trades = vec![
            make_closed_trade(Some("-100"), Some("-2"), Some(300)),
            make_closed_trade(Some("-50"), Some("-1"), Some(120)),
        ];
        let summary = compute_closed_trades_summary(&trades);
        assert_eq!(summary.win_rate, Some(0.0));
        // No winners means gross_profit == 0 → profit_factor is Some(0.0).
        assert_eq!(summary.profit_factor, Some(0.0));
    }

    #[test]
    fn test_compute_closed_trades_summary_break_even_trades() {
        // All trades at exactly 0.0 PnL — no winners or losers → win_rate is None.
        let trades = vec![
            make_closed_trade(Some("0"), Some("0"), Some(300)),
            make_closed_trade(Some("0"), Some("0"), Some(120)),
        ];
        let summary = compute_closed_trades_summary(&trades);
        assert_eq!(summary.total_closed, 2);
        assert!(summary.win_rate.is_none());
        assert!(summary.profit_factor.is_none());
    }

    #[test]
    fn test_compute_closed_trades_summary_missing_return_percent() {
        // PnL is present but return_percent is None → average_return_percent should be None.
        let trades = vec![
            make_closed_trade(Some("100"), None, Some(300)),
            make_closed_trade(Some("200"), None, Some(180)),
        ];
        let summary = compute_closed_trades_summary(&trades);
        assert_eq!(summary.total_closed, 2);
        assert_eq!(summary.win_rate, Some(1.0));
        assert!(summary.average_return_percent.is_none());
        assert_eq!(summary.average_holding_seconds, Some(240.0));
    }

    #[test]
    fn test_compute_closed_trades_summary_missing_holding_seconds() {
        // PnL and return_percent present but holding_seconds is None → average_holding_seconds None.
        let trades = vec![
            make_closed_trade(Some("100"), Some("2"), None),
            make_closed_trade(Some("-50"), Some("-1"), None),
        ];
        let summary = compute_closed_trades_summary(&trades);
        assert_eq!(summary.total_closed, 2);
        assert!(summary.average_holding_seconds.is_none());
        assert_eq!(summary.average_return_percent, Some(0.5));
    }

    /// Builds a snapshot at an exact offset from a shared `now` so that all
    /// timestamps in a single test are derived from the same instant. This
    /// prevents the race where two `Utc::now()` calls return slightly different
    /// values, causing a baseline snapshot to appear microseconds newer than
    /// the period cutoff and therefore not be found by
    /// `find_snapshot_at_or_before`.
    fn make_snapshot(
        now: DateTime<Utc>,
        days_ago: i64,
        nav: i64,
        spy: Option<f64>,
    ) -> PerformanceSnapshot {
        PerformanceSnapshot {
            snapshot_timestamp: now - Duration::days(days_ago),
            net_asset_value: Decimal::new(nav, 0),
            gross_return: None,
            net_return: None,
            total_slippage_cost: Decimal::ZERO,
            spy_close: spy,
        }
    }

    #[test]
    fn test_compute_period_returns_empty_history() {
        let returns = compute_period_returns(&[]);
        assert!(returns.fund_one_day.is_none());
        assert!(returns.fund_one_week.is_none());
        assert!(returns.fund_since_inception.is_none());
        assert!(returns.spy_one_day.is_none());
    }

    #[test]
    fn test_compute_period_returns_single_snapshot() {
        // Only one snapshot — it's both current and inception.
        // since_inception baseline is history.last() == history[0] == current.
        // (current - current) / current = 0%
        let now = Utc::now();
        let history = vec![make_snapshot(now, 0, 100_000, Some(450.0))];
        let returns = compute_period_returns(&history);
        assert_eq!(returns.fund_since_inception, Some(0.0));
        // No older snapshots → other periods are None.
        assert!(returns.fund_one_day.is_none());
        assert!(returns.fund_one_week.is_none());
    }

    #[test]
    fn test_compute_period_returns_one_day() {
        // Current = 110_000, 1-day baseline = 100_000 → 10%
        let now = Utc::now();
        let history = vec![
            make_snapshot(now, 0, 110_000, Some(455.0)),
            make_snapshot(now, 1, 100_000, Some(450.0)),
        ];
        let returns = compute_period_returns(&history);
        let one_day = returns.fund_one_day.expect("fund_one_day should be Some");
        assert!((one_day - 10.0).abs() < 1e-9, "expected 10%, got {one_day}");
    }

    #[test]
    fn test_compute_period_returns_spy_one_day() {
        // SPY: current=455, baseline=450 → (455−450)/450×100 ≈ 1.111%
        let now = Utc::now();
        let history = vec![
            make_snapshot(now, 0, 110_000, Some(455.0)),
            make_snapshot(now, 1, 100_000, Some(450.0)),
        ];
        let returns = compute_period_returns(&history);
        let spy = returns.spy_one_day.expect("spy_one_day should be Some");
        assert!((spy - (5.0 / 450.0 * 100.0)).abs() < 1e-9);
    }

    #[test]
    fn test_compute_period_returns_since_inception() {
        // Oldest snapshot = 80_000; current = 110_000 → 37.5%
        let now = Utc::now();
        let history = vec![
            make_snapshot(now, 0, 110_000, None),
            make_snapshot(now, 7, 105_000, None),
            make_snapshot(now, 365, 80_000, None),
        ];
        let returns = compute_period_returns(&history);
        let inception = returns
            .fund_since_inception
            .expect("fund_since_inception should be Some");
        assert!((inception - 37.5).abs() < 1e-9);
    }

    #[test]
    fn test_compute_period_returns_missing_spy_gives_none() {
        let now = Utc::now();
        let history = vec![
            make_snapshot(now, 0, 110_000, None),
            make_snapshot(now, 1, 100_000, None),
        ];
        let returns = compute_period_returns(&history);
        assert!(returns.spy_one_day.is_none());
        // Fund return is still computed even when SPY data is absent.
        assert!(returns.fund_one_day.is_some());
    }

    #[test]
    fn test_compute_period_returns_zero_baseline_nav_gives_none() {
        let now = Utc::now();
        let history = vec![
            make_snapshot(now, 0, 110_000, None),
            make_snapshot(now, 1, 0, None), // zero baseline NAV → division guard
        ];
        let returns = compute_period_returns(&history);
        assert!(returns.fund_one_day.is_none());
    }
}
