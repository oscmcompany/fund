//! Shared in-memory cache for dashboard data.
//!
//! A single background task polls production Postgres every [`POLL_INTERVAL_SECONDS`]
//! seconds and updates [`DashboardState`] behind a [`SharedState`] lock. All
//! ratatui render passes read from the cache without touching the database
//! directly, so viewer count does not affect Postgres connection load.
//!
//! A separate [`spawn_event_listener_task`] subscribes to the `events` NOTIFY
//! channel and appends incoming events to the ring buffer in real time.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::domain::market::{PairID, Ticker};

/// How often the background task refreshes all static view data from Postgres.
const POLL_INTERVAL_SECONDS: u64 = 30;

/// Backoff duration between event listener reconnect attempts.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// Maximum number of events retained in the event ring buffer.
const EVENT_BUFFER_CAPACITY: usize = 500;

/// Data for a single open long/short pair position (Tab 1).
#[derive(Debug, Clone)]
pub struct OpenPosition {
    pub pair_id: PairID,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    pub z_score: Decimal,
    pub hedge_ratio: Decimal,
    pub signal_strength: Decimal,
    pub long_dollar_amount: Decimal,
    pub short_dollar_amount: Decimal,
    pub opened_at: DateTime<Utc>,
}

/// A single portfolio NAV snapshot with optional SPY benchmark close (Tab 2).
#[derive(Debug, Clone)]
pub struct PerformanceSnapshot {
    pub snapshot_timestamp: DateTime<Utc>,
    pub net_asset_value: Decimal,
    pub gross_return: Option<Decimal>,
    pub net_return: Option<Decimal>,
    pub total_slippage_cost: Decimal,
    pub spy_close: Option<f64>,
}

/// Data for a single closed pair trade (Tab 3).
#[derive(Debug, Clone)]
pub struct ClosedTrade {
    pub pair_id: PairID,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    pub realized_profit_and_loss: Option<Decimal>,
    pub return_percent: Option<Decimal>,
    /// Seconds the position was held; computed from `opened_at`/`closed_at` timestamps.
    pub holding_seconds: Option<i64>,
    pub close_reason: Option<String>,
    pub closed_at: Option<DateTime<Utc>>,
}

/// Aggregate statistics across all fetched closed trades (Tab 3 footer).
#[derive(Debug, Clone, Default)]
pub struct ClosedTradesSummary {
    pub total_closed: usize,
    pub win_rate: Option<f64>,
    pub profit_factor: Option<f64>,
    pub average_return_percent: Option<f64>,
    /// Average holding duration in seconds across closed trades.
    pub average_holding_seconds: Option<f64>,
    pub total_realized_profit_and_loss: Option<Decimal>,
}

/// A single model quantile prediction row (Tab 4).
#[derive(Debug, Clone)]
pub struct PredictionRow {
    pub ticker: Ticker,
    pub quantile_10: f64,
    pub quantile_50: f64,
    pub quantile_90: f64,
    pub model_run_id: String,
    pub timestamp: DateTime<Utc>,
}

/// Pre-computed period returns for the Performance view (Tab 2).
///
/// All values are percentages (e.g. `5.0` means +5%). `None` indicates
/// insufficient snapshot history for that horizon.
#[derive(Debug, Clone, Default)]
pub struct PeriodReturns {
    pub fund_one_day: Option<f64>,
    pub fund_one_week: Option<f64>,
    pub fund_one_month: Option<f64>,
    pub fund_year_to_date: Option<f64>,
    pub fund_since_inception: Option<f64>,
    pub spy_one_day: Option<f64>,
    pub spy_one_week: Option<f64>,
    pub spy_one_month: Option<f64>,
    pub spy_year_to_date: Option<f64>,
    pub spy_since_inception: Option<f64>,
}

/// Metadata from the most recently completed model run (Tab 4 header).
#[derive(Debug, Clone)]
pub struct ModelRunInformation {
    completed_at: DateTime<Utc>,
    start_date: Option<chrono::NaiveDate>,
    end_date: Option<chrono::NaiveDate>,
    continuous_ranked_probability_score: Option<f64>,
    directional_accuracy: Option<f64>,
}

impl ModelRunInformation {
    /// Constructs a `ModelRunInformation`, validating that `start_date < end_date` when both are
    /// provided. Returns an error if the date range is invalid.
    pub fn new(
        completed_at: DateTime<Utc>,
        start_date: Option<chrono::NaiveDate>,
        end_date: Option<chrono::NaiveDate>,
        continuous_ranked_probability_score: Option<f64>,
        directional_accuracy: Option<f64>,
    ) -> Result<Self, &'static str> {
        if let (Some(start), Some(end)) = (start_date, end_date) {
            if start >= end {
                return Err("start_date must be before end_date");
            }
        }
        Ok(Self {
            completed_at,
            start_date,
            end_date,
            continuous_ranked_probability_score,
            directional_accuracy,
        })
    }

    pub fn completed_at(&self) -> DateTime<Utc> {
        self.completed_at
    }

    pub fn start_date(&self) -> Option<chrono::NaiveDate> {
        self.start_date
    }

    pub fn end_date(&self) -> Option<chrono::NaiveDate> {
        self.end_date
    }

    pub fn continuous_ranked_probability_score(&self) -> Option<f64> {
        self.continuous_ranked_probability_score
    }

    pub fn directional_accuracy(&self) -> Option<f64> {
        self.directional_accuracy
    }
}

/// A single event received from the Postgres `events` NOTIFY channel (Tab 5).
#[derive(Debug, Clone)]
pub struct EventEntry {
    pub event_id: i64,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub received_at: DateTime<Utc>,
}

/// All data shown across the five dashboard tabs.
///
/// Updated atomically by the background polling task and the event listener.
/// The ratatui render loop reads this under a shared lock without ever blocking
/// on database I/O.
#[derive(Debug, Default)]
pub struct DashboardState {
    /// Open long/short pair positions (Tab 1).
    pub open_positions: Vec<OpenPosition>,
    /// Sum of all long and short dollar amounts across open positions (Tab 1 footer).
    pub gross_exposure: Decimal,
    /// Net of long minus short dollar amounts across open positions (Tab 1 footer).
    pub net_exposure: Decimal,
    /// Portfolio NAV snapshots, newest first (Tab 2).
    pub performance_history: Vec<PerformanceSnapshot>,
    /// Closed pair trades, newest first (Tab 3).
    pub closed_trades: Vec<ClosedTrade>,
    /// Aggregate statistics computed from closed trades (Tab 3 footer).
    pub closed_trades_summary: ClosedTradesSummary,
    /// Latest model quantile predictions, sorted by ticker (Tab 4).
    pub predictions: Vec<PredictionRow>,
    /// Pre-computed period returns for the Performance view (Tab 2).
    pub period_returns: PeriodReturns,
    /// Bounded event ring buffer, newest first (Tab 5).
    pub events: VecDeque<EventEntry>,
    /// Metadata from the most recently completed model run (Tab 4 header). `None` until first poll.
    pub model_run_information: Option<ModelRunInformation>,
    /// Timestamp of the most recently inserted equity bar row (Tab 4 header freshness).
    /// `None` until first poll or when the table is empty.
    pub latest_bars_inserted_at: Option<DateTime<Utc>>,
    /// Completion time of the most recent rebalance session (Tab 1 header freshness).
    /// `None` until first poll or when no sessions have completed.
    pub last_rebalance_completed_at: Option<DateTime<Utc>>,
    /// Timestamp of the most recent successful poll. `None` until first poll completes.
    pub last_updated: Option<DateTime<Utc>>,
    /// Last database error message, if any. Cleared on successful poll.
    pub database_error: Option<String>,
}

/// Thread-safe shared reference to dashboard state.
pub type SharedState = Arc<RwLock<DashboardState>>;

/// Spawns the background task that polls Postgres every [`POLL_INTERVAL_SECONDS`]
/// seconds and refreshes all static view data in [`DashboardState`].
///
/// Errors from individual polls are recorded in `state.database_error` without
/// terminating the task. A successful poll clears any previous error.
pub fn spawn_polling_task(state: SharedState, pool: PgPool) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECONDS));
        loop {
            interval.tick().await;
            match super::database::fetch_dashboard_data(&pool).await {
                Ok(data) => {
                    let mut guard = state.write().await;
                    guard.open_positions = data.open_positions;
                    guard.gross_exposure = data.gross_exposure;
                    guard.net_exposure = data.net_exposure;
                    guard.performance_history = data.performance_history;
                    guard.period_returns = data.period_returns;
                    guard.closed_trades = data.closed_trades;
                    guard.closed_trades_summary = data.closed_trades_summary;
                    guard.predictions = data.predictions;
                    guard.model_run_information = data.model_run_information;
                    guard.latest_bars_inserted_at = data.latest_bars_inserted_at;
                    guard.last_rebalance_completed_at = data.last_rebalance_completed_at;
                    guard.last_updated = Some(Utc::now());
                    guard.database_error = None;
                    info!("Dashboard state refreshed");
                }
                Err(error) => {
                    warn!(error = %error, "Dashboard poll failed");
                    state.write().await.database_error = Some(error.to_string());
                }
            }
        }
    });
}

/// Spawns the background task that subscribes to the Postgres `events` NOTIFY
/// channel and appends incoming notifications to the event ring buffer.
///
/// The NOTIFY payload produced by the `events_notify` trigger is a JSON object
/// with `event_id`, `event_type`, and `payload` fields. Malformed notifications
/// are logged and skipped rather than synthesised as placeholder events.
///
/// On any connection or receive error the task reconnects after
/// [`RECONNECT_BACKOFF`] so transient Postgres restarts do not permanently
/// stop real-time event delivery.
pub fn spawn_event_listener_task(state: SharedState, pool: PgPool) {
    tokio::spawn(async move {
        loop {
            let mut listener = match PgListener::connect_with(&pool).await {
                Ok(listener) => listener,
                Err(error) => {
                    error!(error = %error, "Event listener connection failed, retrying");
                    tokio::time::sleep(RECONNECT_BACKOFF).await;
                    continue;
                }
            };
            if let Err(error) = listener.listen("events").await {
                error!(error = %error, "Event listener subscribe failed, retrying");
                tokio::time::sleep(RECONNECT_BACKOFF).await;
                continue;
            }
            info!("Event listener subscribed to events channel");
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let parsed: serde_json::Value =
                            match serde_json::from_str(notification.payload()) {
                                Ok(value) => value,
                                Err(error) => {
                                    warn!(error = %error, "Invalid event notification payload");
                                    continue;
                                }
                            };
                        let Some(event_id) =
                            parsed.get("event_id").and_then(serde_json::Value::as_i64)
                        else {
                            warn!("Event notification missing event_id field");
                            continue;
                        };
                        let Some(event_type) =
                            parsed.get("event_type").and_then(serde_json::Value::as_str)
                        else {
                            warn!("Event notification missing event_type field");
                            continue;
                        };
                        let entry = EventEntry {
                            event_id,
                            event_type: event_type.to_string(),
                            payload: parsed
                                .get("payload")
                                .cloned()
                                .unwrap_or(serde_json::Value::Null),
                            received_at: Utc::now(),
                        };
                        let mut guard = state.write().await;
                        guard.events.push_front(entry);
                        if guard.events.len() > EVENT_BUFFER_CAPACITY {
                            guard.events.pop_back();
                        }
                    }
                    Err(error) => {
                        error!(error = %error, "Event listener receive error, reconnecting");
                        break;
                    }
                }
            }
            tokio::time::sleep(RECONNECT_BACKOFF).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboard_state_default_is_empty() {
        let state = DashboardState::default();
        assert!(state.open_positions.is_empty());
        assert!(state.performance_history.is_empty());
        assert!(state.closed_trades.is_empty());
        assert!(state.predictions.is_empty());
        assert!(state.events.is_empty());
        assert_eq!(state.gross_exposure, Decimal::ZERO);
        assert_eq!(state.net_exposure, Decimal::ZERO);
        assert!(state.last_updated.is_none());
        assert!(state.database_error.is_none());
        assert!(state.model_run_information.is_none());
        assert!(state.latest_bars_inserted_at.is_none());
        assert!(state.last_rebalance_completed_at.is_none());
    }

    #[test]
    fn test_model_run_information_fields() {
        use chrono::NaiveDate;
        let info = ModelRunInformation::new(
            Utc::now(),
            Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
            Some(0.123),
            Some(0.725),
        )
        .unwrap();
        assert!(info.continuous_ranked_probability_score().is_some());
        assert!(info.directional_accuracy().is_some());
        assert!(info.start_date().unwrap() < info.end_date().unwrap());
    }

    #[test]
    fn test_model_run_information_rejects_invalid_date_range() {
        use chrono::NaiveDate;
        let result = ModelRunInformation::new(
            Utc::now(),
            Some(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
            Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_model_run_information_allows_null_dates() {
        let result = ModelRunInformation::new(Utc::now(), None, None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_entry_fields() {
        let entry = EventEntry {
            event_id: 42,
            event_type: "portfolio_rebalance_completed".to_string(),
            payload: serde_json::json!({"session_id": "abc"}),
            received_at: Utc::now(),
        };
        assert_eq!(entry.event_id, 42);
        assert_eq!(entry.event_type, "portfolio_rebalance_completed");
        assert_eq!(entry.payload["session_id"], "abc");
    }

    #[test]
    fn test_closed_trades_summary_default() {
        let summary = ClosedTradesSummary::default();
        assert_eq!(summary.total_closed, 0);
        assert!(summary.win_rate.is_none());
        assert!(summary.profit_factor.is_none());
        assert!(summary.average_return_percent.is_none());
        assert!(summary.average_holding_seconds.is_none());
        assert!(summary.total_realized_profit_and_loss.is_none());
    }

    #[test]
    fn test_closed_trade_fields() {
        let trade = ClosedTrade {
            pair_id: PairID::parse("TSLA-NVDA").unwrap(),
            long_ticker: Ticker::new("TSLA").unwrap(),
            short_ticker: Ticker::new("NVDA").unwrap(),
            realized_profit_and_loss: Some(rust_decimal::Decimal::new(500, 0)),
            return_percent: Some(rust_decimal::Decimal::new(5, 2)),
            holding_seconds: Some(3600),
            close_reason: Some("profit_taken".to_string()),
            closed_at: Some(Utc::now()),
        };
        assert_eq!(trade.pair_id.as_str(), "TSLA-NVDA");
        assert_eq!(trade.long_ticker, "TSLA");
        assert_eq!(trade.holding_seconds, Some(3600));
    }

    #[test]
    fn test_prediction_row_fields() {
        let row = PredictionRow {
            ticker: Ticker::new("AAPL").unwrap(),
            quantile_10: 0.1,
            quantile_50: 0.5,
            quantile_90: 0.9,
            model_run_id: "run-abc".to_string(),
            timestamp: Utc::now(),
        };
        assert_eq!(row.ticker, "AAPL");
        assert!(row.quantile_10 < row.quantile_50);
        assert!(row.quantile_50 < row.quantile_90);
    }

    #[test]
    fn test_performance_snapshot_fields() {
        let snapshot = PerformanceSnapshot {
            snapshot_timestamp: Utc::now(),
            net_asset_value: rust_decimal::Decimal::new(100000, 0),
            gross_return: Some(rust_decimal::Decimal::new(5, 2)),
            net_return: Some(rust_decimal::Decimal::new(4, 2)),
            total_slippage_cost: rust_decimal::Decimal::new(50, 0),
            spy_close: Some(450.0),
        };
        assert!(snapshot.gross_return.is_some());
        assert!(snapshot.spy_close.is_some());
    }

    #[test]
    fn test_open_position_fields() {
        let position = OpenPosition {
            pair_id: PairID::parse("AAPL-MSFT").unwrap(),
            long_ticker: Ticker::new("AAPL").unwrap(),
            short_ticker: Ticker::new("MSFT").unwrap(),
            z_score: Decimal::new(15, 1),
            hedge_ratio: Decimal::ONE,
            signal_strength: Decimal::new(8, 1),
            long_dollar_amount: Decimal::new(10000, 0),
            short_dollar_amount: Decimal::new(9500, 0),
            opened_at: Utc::now(),
        };
        assert_eq!(position.long_ticker, "AAPL");
        assert_eq!(
            position.long_dollar_amount + position.short_dollar_amount,
            Decimal::new(19500, 0)
        );
    }
}
