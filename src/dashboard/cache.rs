//! Shared in-memory state for the dashboard, and the two tasks that keep it current.
//!
//! A polling task refreshes everything every [`POLL_INTERVAL`] behind an `RwLock`; a listener
//! appends `events` as they arrive. Request handlers read the lock and never touch the database, so
//! viewer count has no bearing on the connection pool.
//!
//! A failed poll leaves the previous state and records the error rather than blanking the page —
//! stale numbers with a visible "last updated" beat no numbers.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::common::events::Notification;
use crate::common::types::{PairID, SessionDate, Ticker};

use crate::portfolio::pairs::CloseReason;

/// How often the background task refreshes every view from PostgreSQL.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait before reconnecting a dropped listener.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// Events retained in the ring buffer.
const EVENT_BUFFER_CAPACITY: usize = 500;

/// One session's account state.
///
/// Balances are optional because a session reconstructed by `backfill_account_snapshots` has none;
/// `equity` is the only field both sources supply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub session_date: SessionDate,
    pub equity: Decimal,
    pub cash: Option<Decimal>,
    pub buying_power: Option<Decimal>,
    pub long_market_value: Option<Decimal>,
    pub short_market_value: Option<Decimal>,
}

impl AccountSnapshot {
    /// Total capital at risk, adding the magnitudes of both sides.
    ///
    /// Alpaca reports `short_market_value` as a negative number, so summing the two raw values
    /// nets a market-neutral book to roughly zero — which is the one number that says nothing
    /// about how much is deployed.
    ///
    /// `None` unless both sides are known: one side alone is an unknown exposure, not a smaller one.
    pub fn gross_exposure(&self) -> Option<Decimal> {
        Some(self.long_market_value?.abs() + self.short_market_value?.abs())
    }

    /// Directional tilt: how far the book is from balanced.
    pub fn net_exposure(&self) -> Option<Decimal> {
        Some(self.long_market_value? + self.short_market_value?)
    }
}

/// An open pair and the signal that justified opening it.
#[derive(Debug, Clone)]
pub struct OpenPair {
    pub pair_id: PairID,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    pub hedge_ratio: f64,
    pub entry_z_score: f64,
    pub signal_strength: f64,
    pub opened_at: DateTime<Utc>,
}

/// A closed pair and what it realized.
#[derive(Debug, Clone)]
pub struct ClosedPair {
    pub pair_id: PairID,
    pub long_ticker: Ticker,
    pub short_ticker: Ticker,
    pub entry_z_score: f64,
    pub realized_profit_and_loss: Option<Decimal>,
    pub close_reason: CloseReason,
    pub opened_at: DateTime<Utc>,
    pub closed_at: DateTime<Utc>,
}

impl ClosedPair {
    /// How long the pair was held.
    pub fn holding_hours(&self) -> f64 {
        (self.closed_at - self.opened_at).num_seconds() as f64 / 3_600.0
    }
}

/// One ticker's quantile forecast from the most recent prediction batch.
#[derive(Debug, Clone)]
pub struct Prediction {
    pub ticker: Ticker,
    pub quantile_10: f64,
    pub quantile_50: f64,
    pub quantile_90: f64,
}

/// A row from the event log, or a live arrival from the NOTIFY channel.
#[derive(Debug, Clone)]
pub struct EventEntry {
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub payload: Value,
}

/// Return over each horizon, as a percentage. `None` means the history does not reach back far
/// enough to answer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PeriodReturns {
    pub one_day: Option<f64>,
    pub one_week: Option<f64>,
    pub one_month: Option<f64>,
    pub year_to_date: Option<f64>,
    pub since_inception: Option<f64>,
}

/// Aggregates over the fetched closed pairs.
#[derive(Debug, Clone, Default)]
pub struct ClosedSummary {
    pub total_closed: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: Option<f64>,
    pub total_realized: Decimal,
    pub average_realized: Option<Decimal>,
    pub profit_factor: Option<f64>,
    pub average_holding_hours: Option<f64>,
    /// Every [`CloseReason`] with its count, including the ones at zero.
    ///
    /// Listing the zeros is the point. The question this section exists to answer is whether the
    /// book ever exits on its own opinion, and an absent row reads as "not measured" where a zero
    /// reads as "measured, and it never happened".
    pub by_close_reason: Vec<(CloseReason, usize)>,
    /// Share of closes that reflect the strategy's own opinion rather than the pre-close fail-safe.
    pub signal_exit_share: Option<f64>,
}

/// Everything the page renders.
#[derive(Debug, Clone, Default)]
pub struct DashboardState {
    pub account: Option<AccountSnapshot>,
    pub account_snapshot_history: Vec<AccountSnapshot>,
    pub period_returns: PeriodReturns,
    pub open_pairs: Vec<OpenPair>,
    pub closed_pairs: Vec<ClosedPair>,
    pub closed_summary: ClosedSummary,
    pub predictions: Vec<Prediction>,
    pub prediction_model_run_id: Option<String>,
    pub prediction_timestamp: Option<DateTime<Utc>>,
    pub events: VecDeque<EventEntry>,
    pub latest_bars_inserted_at: Option<DateTime<Utc>>,
    pub last_updated: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

pub type SharedState = Arc<RwLock<DashboardState>>;

/// Applies one poll's results, leaving the event ring buffer untouched.
///
/// The buffer is owned by the listener, which appends to it between polls. Replacing the whole
/// state would discard every event that arrived since the last refresh.
pub async fn apply_poll(state: &SharedState, data: crate::dashboard::database::DashboardData) {
    let mut guard = state.write().await;
    guard.account = data.account_snapshot_history.last().cloned();
    guard.account_snapshot_history = data.account_snapshot_history;
    guard.period_returns = data.period_returns;
    guard.open_pairs = data.open_pairs;
    guard.closed_pairs = data.closed_pairs;
    guard.closed_summary = data.closed_summary;
    guard.predictions = data.predictions;
    guard.prediction_model_run_id = data.prediction_model_run_id;
    guard.prediction_timestamp = data.prediction_timestamp;
    guard.latest_bars_inserted_at = data.latest_bars_inserted_at;
    guard.last_updated = Some(Utc::now());
    guard.last_error = None;

    // Only seeded when empty, so the listener's live arrivals are not overwritten by a snapshot
    // taken thirty seconds ago.
    if guard.events.is_empty() {
        guard.events = data.recent_events.into_iter().collect();
    }
}

/// Appends one event, evicting the oldest once the buffer is full.
pub async fn append_event(state: &SharedState, entry: EventEntry) {
    let mut guard = state.write().await;
    if guard.events.len() >= EVENT_BUFFER_CAPACITY {
        guard.events.pop_back();
    }
    guard.events.push_front(entry);
}

/// Refreshes every view on a fixed interval until the process ends.
pub fn spawn_polling_task(state: SharedState, pool: PgPool) {
    tokio::spawn(async move {
        loop {
            match crate::dashboard::database::fetch_dashboard_data(&pool).await {
                Ok(data) => apply_poll(&state, data).await,
                Err(error) => {
                    // The previous state stays on the page, and the header says how old it is.
                    // Blanking the dashboard because one query timed out would remove the
                    // information needed to tell whether anything is actually wrong.
                    error!(%error, "Dashboard poll failed; serving the previous state");
                    let mut guard = state.write().await;
                    guard.last_error = Some(error.to_string());
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}

/// Subscribes to the `events` channel and appends arrivals in real time.
///
/// Reconnects on a fixed delay. The dashboard is read-only, so a listener that stays down costs
/// live event rows until the next poll reseeds them, not correctness.
pub fn spawn_event_listener_task(state: SharedState, pool: PgPool) {
    tokio::spawn(async move {
        loop {
            match PgListener::connect_with(&pool).await {
                Ok(mut listener) => {
                    if let Err(error) = listener.listen("events").await {
                        warn!(%error, "Failed to subscribe to the events channel");
                        tokio::time::sleep(RECONNECT_BACKOFF).await;
                        continue;
                    }
                    info!("Dashboard listening for events");
                    loop {
                        match listener.recv().await {
                            Ok(notification) => {
                                let Some(parsed) = Notification::parse(notification.payload())
                                else {
                                    warn!(
                                        raw = notification.payload(),
                                        "Unparseable event notification, skipping"
                                    );
                                    continue;
                                };
                                append_event(
                                    &state,
                                    EventEntry {
                                        event_type: parsed.event_type.as_str().to_string(),
                                        created_at: Utc::now(),
                                        payload: parsed.payload,
                                    },
                                )
                                .await;
                            }
                            Err(error) => {
                                warn!(%error, "Event listener dropped; reconnecting");
                                break;
                            }
                        }
                    }
                }
                Err(error) => warn!(%error, "Failed to connect the event listener; retrying"),
            }
            tokio::time::sleep(RECONNECT_BACKOFF).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone};
    use std::str::FromStr;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("a valid test decimal")
    }

    fn snapshot(long: &str, short: &str) -> AccountSnapshot {
        AccountSnapshot {
            session_date: SessionDate::from_date(
                NaiveDate::from_ymd_opt(2026, 7, 31).expect("a valid test date"),
            ),
            equity: decimal("100000"),
            cash: Some(decimal("50000")),
            buying_power: Some(decimal("200000")),
            long_market_value: Some(decimal(long)),
            short_market_value: Some(decimal(short)),
        }
    }

    /// The whole reason `gross_exposure` adds magnitudes: a balanced book nets to zero, and zero
    /// is the one answer that hides how much capital is deployed.
    #[test]
    fn test_gross_exposure_adds_magnitudes_where_net_cancels() {
        let balanced = snapshot("50000", "-50000");
        assert_eq!(balanced.gross_exposure(), Some(decimal("100000")));
        assert_eq!(balanced.net_exposure(), Some(decimal("0")));
    }

    #[test]
    fn test_net_exposure_shows_a_directional_tilt() {
        let tilted = snapshot("60000", "-40000");
        assert_eq!(tilted.gross_exposure(), Some(decimal("100000")));
        assert_eq!(tilted.net_exposure(), Some(decimal("20000")));
    }

    /// An unrecorded book is unknown, not flat, so both exposures withhold rather than return zero.
    #[test]
    fn test_a_reconstructed_session_reports_no_exposure() {
        let mut reconstructed = snapshot("60000", "-40000");
        reconstructed.long_market_value = None;
        reconstructed.short_market_value = None;
        assert_eq!(reconstructed.gross_exposure(), None);
        assert_eq!(reconstructed.net_exposure(), None);
    }

    #[test]
    fn test_holding_hours_spans_the_open_window() {
        let pair = ClosedPair {
            pair_id: PairID::parse("AAA-BBB").expect("a valid pair"),
            long_ticker: Ticker::new("AAA").expect("a valid ticker"),
            short_ticker: Ticker::new("BBB").expect("a valid ticker"),
            entry_z_score: 2.5,
            realized_profit_and_loss: Some(decimal("100")),
            close_reason: CloseReason::Convergence,
            opened_at: Utc.with_ymd_and_hms(2026, 7, 30, 14, 0, 0).unwrap(),
            closed_at: Utc.with_ymd_and_hms(2026, 7, 31, 20, 0, 0).unwrap(),
        };
        assert_eq!(pair.holding_hours(), 30.0);
    }

    #[tokio::test]
    async fn test_the_ring_buffer_evicts_the_oldest_and_keeps_the_newest_first() {
        let state: SharedState = Arc::new(RwLock::new(DashboardState::default()));
        for index in 0..EVENT_BUFFER_CAPACITY + 10 {
            append_event(
                &state,
                EventEntry {
                    event_type: format!("event_{index}"),
                    created_at: Utc::now(),
                    payload: Value::Null,
                },
            )
            .await;
        }

        let guard = state.read().await;
        assert_eq!(guard.events.len(), EVENT_BUFFER_CAPACITY);
        assert_eq!(
            guard.events.front().expect("a newest event").event_type,
            format!("event_{}", EVENT_BUFFER_CAPACITY + 9)
        );
    }
}
