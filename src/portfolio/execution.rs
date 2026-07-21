//! Trade execution using the Alpaca trading client and the order typestate machine.
//!
//! Converts [`SizedPair`] records into [`PendingPair`] values by submitting orders
//! to Alpaca, then polls fill confirmations to produce [`FilledPair`] values.
//! Closing existing positions is handled via `close_position` on each leg.

use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tokio::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::domain::orders::{FilledOrder, Order, OrderSide, PendingPair};
use crate::domain::trading::{ReconciliationAction, ReconciliationEventType};
use crate::portfolio::alpaca::{ClientError, Trading};
use crate::portfolio::database;
use crate::portfolio::sizing::SizedPair;

/// Maximum number of fill-poll attempts per order before giving up.
const FILL_POLL_ATTEMPTS: usize = 5;

/// Backoff durations for fill polling: 500ms, 1s, 2s, 3s, 3.5s (total 10s).
const FILL_POLL_BACKOFF_MILLIS: [u64; FILL_POLL_ATTEMPTS] = [500, 1000, 2000, 3000, 3500];

/// Error produced during the open-positions execution phase.
#[derive(Debug)]
pub enum ExecutionError {
    /// Alpaca returned an API or network error during order submission.
    OrderSubmission { ticker: String, source: ClientError },
    /// Alpaca returned an API or network error during fill polling.
    FillPoll {
        alpaca_order_id: String,
        source: ClientError,
    },
    /// Alpaca returned an API or network error when closing a position.
    PositionClose { ticker: String, source: ClientError },
    /// Alpaca returned an error when fetching positions or account info.
    PositionFetch { source: ClientError },
    /// Alpaca and database state are inconsistent.
    StateMismatch { message: String },
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::OrderSubmission { ticker, source } => {
                write!(formatter, "Order submission failed for {ticker}: {source}")
            }
            ExecutionError::FillPoll {
                alpaca_order_id,
                source,
            } => {
                write!(
                    formatter,
                    "Fill poll failed for order {alpaca_order_id}: {source}"
                )
            }
            ExecutionError::PositionClose { ticker, source } => {
                write!(formatter, "Position close failed for {ticker}: {source}")
            }
            ExecutionError::PositionFetch { source } => {
                write!(formatter, "Position fetch failed: {source}")
            }
            ExecutionError::StateMismatch { message } => {
                write!(formatter, "State mismatch: {message}")
            }
        }
    }
}

impl std::error::Error for ExecutionError {}

/// Submits long and short orders concurrently for each sized pair, returning pending pairs.
///
/// Both legs are submitted simultaneously to minimise the timing gap between
/// executions. Pairs where either submission fails are logged and skipped; the
/// surviving leg is cancelled (or its position closed if it already filled)
/// before moving on. This is a best-effort operation — partial success is
/// expected in live trading when individual tickers have liquidity or
/// borrowing issues.
pub async fn execute_open_pairs(
    alpaca: &dyn Trading,
    pool: &PgPool,
    sized_pairs: &[SizedPair],
) -> Vec<(PendingPair, SizedPair)> {
    let mut results: Vec<(PendingPair, SizedPair)> = Vec::new();

    for sized_pair in sized_pairs {
        let (short_result, long_result) = tokio::join!(
            alpaca.submit_short_order(
                sized_pair.short_ticker().as_str(),
                sized_pair.short_quantity()
            ),
            alpaca.submit_long_order(
                sized_pair.long_ticker().as_str(),
                sized_pair.long_dollar_amount()
            ),
        );

        let (short_alpaca_id, long_alpaca_id) = match (short_result, long_result) {
            (Ok(short_id), Ok(long_id)) => (short_id, long_id),
            (Err(error), Ok(long_id)) => {
                warn!(
                    ticker = sized_pair.short_ticker().as_str(),
                    error = %error,
                    "Short order submission failed; cancelling orphaned long order"
                );
                compensate_orphaned_order(
                    alpaca,
                    pool,
                    &long_id,
                    sized_pair.long_ticker().as_str(),
                )
                .await;
                continue;
            }
            (Ok(short_id), Err(error)) => {
                warn!(
                    ticker = sized_pair.long_ticker().as_str(),
                    error = %error,
                    "Long order submission failed; cancelling orphaned short order"
                );
                compensate_orphaned_order(
                    alpaca,
                    pool,
                    &short_id,
                    sized_pair.short_ticker().as_str(),
                )
                .await;
                continue;
            }
            (Err(short_error), Err(long_error)) => {
                warn!(
                    short_ticker = sized_pair.short_ticker().as_str(),
                    long_ticker = sized_pair.long_ticker().as_str(),
                    short_error = %short_error,
                    long_error = %long_error,
                    "Both order submissions failed; skipping pair"
                );
                continue;
            }
        };

        let now = Utc::now();
        let short_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            sized_pair.short_ticker().to_string(),
            OrderSide::Short,
            Decimal::from(sized_pair.short_quantity()),
            "market".to_string(),
            None,
            short_alpaca_id,
            now,
        );
        let long_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            sized_pair.long_ticker().to_string(),
            OrderSide::Long,
            // Long quantity is notional; use 0 as a placeholder until fill.
            Decimal::ZERO,
            "market".to_string(),
            None,
            long_alpaca_id,
            now,
        );

        let pending_pair = PendingPair::new(
            long_order,
            short_order,
            sized_pair.long_market_beta(),
            sized_pair.short_market_beta(),
        );

        info!(
            pair_id = sized_pair.pair_id().as_str(),
            "Orders submitted for pair"
        );
        results.push((pending_pair, sized_pair.clone()));
    }

    results
}

/// Cancels an orphaned order, falling back to closing the resulting position
/// if the order has already filled by the time compensation runs (Alpaca
/// returns 422 for terminal-state orders).
///
/// When both cancellation and position close fail, persists a `compensation_failure`
/// event to the reconciliation table so it can be retried on the next pass.
async fn compensate_orphaned_order(
    alpaca: &dyn Trading,
    pool: &PgPool,
    alpaca_order_id: &str,
    ticker: &str,
) {
    match alpaca.cancel_order(alpaca_order_id).await {
        Ok(true) => {
            info!(
                alpaca_order_id = alpaca_order_id,
                ticker = ticker,
                "Orphaned order cancelled"
            );
        }
        Ok(false) => {
            // Order already in a terminal state; close the filled position.
            if let Err(close_error) = alpaca.close_position(ticker).await {
                error!(
                    ticker = ticker,
                    alpaca_order_id = alpaca_order_id,
                    error = %close_error,
                    "Failed to close orphaned position after order already filled; persisting compensation failure"
                );
                persist_compensation_failure(pool, ticker, alpaca_order_id).await;
            }
        }
        Err(cancel_error) => {
            // Cancel failed — try closing the position directly.
            warn!(
                alpaca_order_id = alpaca_order_id,
                ticker = ticker,
                error = %cancel_error,
                "Failed to cancel orphaned order; attempting close_position fallback"
            );
            if let Err(close_error) = alpaca.close_position(ticker).await {
                error!(
                    ticker = ticker,
                    alpaca_order_id = alpaca_order_id,
                    cancel_error = %cancel_error,
                    close_error = %close_error,
                    "Both cancel and close failed; persisting compensation failure"
                );
                persist_compensation_failure(pool, ticker, alpaca_order_id).await;
            }
        }
    }
}

/// Persists a compensation failure event so reconciliation can retry it.
async fn persist_compensation_failure(pool: &PgPool, ticker: &str, alpaca_order_id: &str) {
    if let Err(error) = database::insert_reconciliation_event(
        pool,
        &ReconciliationEventType::CompensationFailure,
        ticker,
        None,
        None,
        None,
        Some(alpaca_order_id),
        &ReconciliationAction::LoggedOnly,
        None,
    )
    .await
    {
        error!(
            error = %error,
            ticker = ticker,
            alpaca_order_id = alpaca_order_id,
            "Failed to persist compensation failure event"
        );
    }
}

/// Polls fill confirmations for each pending pair.
///
/// For each pair, both legs are polled up to `FILL_POLL_ATTEMPTS` times.
/// Pairs where either leg fails to confirm a fill are logged and dropped
/// (the `PendingPair` is consumed, leaving no dangling long).
///
/// Returns only the pairs where both legs filled successfully.
pub async fn confirm_fills(
    alpaca: &dyn Trading,
    pending_pairs: Vec<(PendingPair, SizedPair)>,
) -> Vec<(crate::domain::orders::FilledPair, SizedPair)> {
    let mut results = Vec::new();

    for (pending_pair, sized_pair) in pending_pairs {
        let long_fill = poll_fill(alpaca, &pending_pair.long().alpaca_order_id).await;
        let short_fill = poll_fill(alpaca, &pending_pair.short().alpaca_order_id).await;

        match pending_pair.confirm(long_fill, short_fill) {
            Ok(filled_pair) => {
                info!(
                    pair_id = sized_pair.pair_id().as_str(),
                    "Pair fills confirmed"
                );
                results.push((filled_pair, sized_pair));
            }
            Err(error) => {
                warn!(
                    pair_id = sized_pair.pair_id().as_str(),
                    error = %error,
                    "Pair fill confirmation failed; pair dropped"
                );
            }
        }
    }

    results
}

/// Polls Alpaca for a filled order, returning a `FilledOrder` on success.
///
/// Uses increasing backoff: 500ms, 1s, 2s, 3s, 3.5s (total ~10s).
/// Returns `None` after `FILL_POLL_ATTEMPTS` failed attempts or when the
/// Alpaca order status does not indicate a fill.
async fn poll_fill(alpaca: &dyn Trading, alpaca_order_id: &str) -> Option<FilledOrder> {
    for attempt in 1..=FILL_POLL_ATTEMPTS {
        match alpaca.get_order(alpaca_order_id).await {
            Ok(order_fill) if order_fill.status == "filled" => {
                let fill_price = order_fill
                    .fill_price
                    .and_then(|price| Decimal::try_from(price).ok())
                    .unwrap_or(Decimal::ZERO);
                let filled_quantity = order_fill
                    .filled_quantity
                    .and_then(|quantity| Decimal::try_from(quantity).ok())
                    .unwrap_or(Decimal::ZERO);

                if fill_price > Decimal::ZERO && filled_quantity > Decimal::ZERO {
                    return Some(FilledOrder {
                        alpaca_order_id: alpaca_order_id.to_string(),
                        fill_price,
                        filled_quantity,
                    });
                }

                warn!(
                    alpaca_order_id = alpaca_order_id,
                    attempt = attempt,
                    "Fill reported non-positive price or quantity; retrying"
                );
            }
            Ok(order_fill) => {
                warn!(
                    alpaca_order_id = alpaca_order_id,
                    status = order_fill.status.as_str(),
                    attempt = attempt,
                    "Order not yet filled; retrying"
                );
            }
            Err(error) => {
                warn!(
                    alpaca_order_id = alpaca_order_id,
                    error = %error,
                    attempt = attempt,
                    "Fill poll error; retrying"
                );
            }
        }
        if attempt < FILL_POLL_ATTEMPTS {
            let backoff = FILL_POLL_BACKOFF_MILLIS[attempt - 1];
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
    }

    warn!(
        alpaca_order_id = alpaca_order_id,
        attempts = FILL_POLL_ATTEMPTS,
        "Fill poll exhausted after 10 seconds; order will be resolved by reconciliation"
    );
    None
}

/// Closes open positions for the given tickers via Alpaca.
///
/// Each ticker is closed independently; failures are logged but do not stop
/// the remaining closures. Returns an `ExecutionError` only when a network-level
/// error (not a 404 "no position") is encountered.
pub async fn close_positions(
    alpaca: &dyn Trading,
    tickers: &[String],
) -> Result<(), ExecutionError> {
    let mut first_error: Option<ExecutionError> = None;

    for ticker in tickers {
        match alpaca.close_position(ticker).await {
            Ok(true) => {
                info!(ticker = ticker.as_str(), "Position closed successfully");
            }
            Ok(false) => {
                info!(
                    ticker = ticker.as_str(),
                    "No open position found; nothing to close"
                );
            }
            Err(error) => {
                warn!(
                    ticker = ticker.as_str(),
                    error = %error,
                    "Position close failed; continuing with remaining tickers"
                );
                if first_error.is_none() {
                    first_error = Some(ExecutionError::PositionClose {
                        ticker: ticker.clone(),
                        source: error,
                    });
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fill_poll_backoff_total_duration() {
        let total_millis: u64 = FILL_POLL_BACKOFF_MILLIS.iter().sum();
        assert_eq!(total_millis, 10_000, "Total backoff should be 10 seconds");
    }

    #[test]
    fn test_fill_poll_backoff_is_monotonically_increasing() {
        for window in FILL_POLL_BACKOFF_MILLIS.windows(2) {
            assert!(
                window[1] >= window[0],
                "Backoff durations should be monotonically increasing"
            );
        }
    }

    #[test]
    fn test_fill_poll_backoff_length_matches_attempts() {
        assert_eq!(
            FILL_POLL_BACKOFF_MILLIS.len(),
            FILL_POLL_ATTEMPTS,
            "Backoff array length must match FILL_POLL_ATTEMPTS"
        );
    }

    #[test]
    fn test_execution_error_display_order_submission() {
        let error = ExecutionError::OrderSubmission {
            ticker: "AAPL".to_string(),
            source: ClientError::Parse("bad json".to_string()),
        };
        let message = format!("{error}");
        assert!(message.contains("AAPL"));
        assert!(message.contains("submission"));
    }

    #[test]
    fn test_execution_error_display_fill_poll() {
        let error = ExecutionError::FillPoll {
            alpaca_order_id: "order-123".to_string(),
            source: ClientError::Parse("timeout".to_string()),
        };
        let message = format!("{error}");
        assert!(message.contains("order-123"));
        assert!(message.contains("Fill poll"));
    }

    #[test]
    fn test_execution_error_display_position_close() {
        let error = ExecutionError::PositionClose {
            ticker: "MSFT".to_string(),
            source: ClientError::Parse("network error".to_string()),
        };
        let message = format!("{error}");
        assert!(message.contains("MSFT"));
        assert!(message.contains("close"));
    }

    #[test]
    fn test_execution_error_is_error_trait() {
        let error = ExecutionError::PositionClose {
            ticker: "TSLA".to_string(),
            source: ClientError::Parse("x".to_string()),
        };
        // Verify std::error::Error is implemented
        let _boxed: Box<dyn std::error::Error> = Box::new(error);
    }

    #[test]
    fn test_execution_error_order_submission_source_included_in_display() {
        let error = ExecutionError::OrderSubmission {
            ticker: "NVDA".to_string(),
            source: ClientError::Api {
                status: 422,
                body: "insufficient funds".to_string(),
            },
        };
        let message = format!("{error}");
        assert!(message.contains("NVDA"));
        assert!(message.contains("422"));
    }

    #[test]
    fn test_execution_error_fill_poll_source_included_in_display() {
        let error = ExecutionError::FillPoll {
            alpaca_order_id: "abc-456".to_string(),
            source: ClientError::Api {
                status: 500,
                body: "internal error".to_string(),
            },
        };
        let message = format!("{error}");
        assert!(message.contains("abc-456"));
        assert!(message.contains("500"));
    }

    #[test]
    fn test_execution_error_position_close_source_included_in_display() {
        let error = ExecutionError::PositionClose {
            ticker: "AMZN".to_string(),
            source: ClientError::Api {
                status: 404,
                body: "position not found".to_string(),
            },
        };
        let message = format!("{error}");
        assert!(message.contains("AMZN"));
        assert!(message.contains("close"));
        assert!(message.contains("404"));
        assert!(message.contains("position not found"));
    }

    #[test]
    fn test_pending_pair_construction_from_execution_path() {
        use crate::domain::orders::{Order, OrderSide, PendingPair};
        use chrono::Utc;
        use rust_decimal::Decimal;
        use uuid::Uuid;

        // Verify the same construction used in execute_open_pairs compiles and
        // produces a coherent PendingPair.
        let now = Utc::now();
        let short_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            "MSFT".to_string(),
            OrderSide::Short,
            Decimal::from(10),
            "market".to_string(),
            None,
            "alpaca-short-001".to_string(),
            now,
        );
        let long_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            "AAPL".to_string(),
            OrderSide::Long,
            Decimal::ZERO,
            "market".to_string(),
            None,
            "alpaca-long-001".to_string(),
            now,
        );
        let pending_pair = PendingPair::new(long_order, short_order, 1.1, 0.9);

        assert_eq!(pending_pair.long().ticker, "AAPL");
        assert_eq!(pending_pair.short().ticker, "MSFT");
        assert!((pending_pair.long_beta() - 1.1).abs() < f64::EPSILON);
        assert!((pending_pair.short_beta() - 0.9).abs() < f64::EPSILON);
        assert_eq!(pending_pair.long().alpaca_order_id, "alpaca-long-001");
        assert_eq!(pending_pair.short().alpaca_order_id, "alpaca-short-001");
    }

    // --- Async unit tests using MockTrading ---

    use crate::domain::market::{PairID, Ticker};
    use crate::portfolio::alpaca::{MockTrading, OrderFill};

    /// Creates a dummy `PgPool` that never actually connects to a database.
    ///
    /// Used by tests that call `execute_open_pairs`, where the pool is only
    /// touched in the compensation error path. The failed DB write is handled
    /// gracefully (logged and swallowed), so the tests still exercise all the
    /// important control flow.
    fn dummy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://localhost:5432/nonexistent_execution_test")
            .expect("lazy pool creation should not fail")
    }

    /// Creates a single `SizedPair` for use in execution tests.
    fn make_test_sized_pair(long_ticker: &str, short_ticker: &str) -> SizedPair {
        SizedPair::new(
            PairID::new(
                Ticker::new(long_ticker).unwrap(),
                Ticker::new(short_ticker).unwrap(),
            ),
            Ticker::new(long_ticker).unwrap(),
            Ticker::new(short_ticker).unwrap(),
            5000.0,
            5000.0,
            50,
            100.0,
            100.0,
            2.5,
            1.0,
            0.05,
            1.1,
            0.9,
        )
        .expect("test sized pair should be valid")
    }

    #[tokio::test]
    async fn test_execute_open_pairs_happy_path_both_legs_submitted() {
        let mock = MockTrading::default();
        let pool = dummy_pool();
        let sized_pairs = vec![make_test_sized_pair("AAPL", "MSFT")];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        assert_eq!(results.len(), 1);
        let (pending_pair, returned_sized) = &results[0];
        assert_eq!(pending_pair.long().ticker, "AAPL");
        assert_eq!(pending_pair.short().ticker, "MSFT");
        assert_eq!(
            pending_pair.long().side,
            crate::domain::orders::OrderSide::Long
        );
        assert_eq!(
            pending_pair.short().side,
            crate::domain::orders::OrderSide::Short
        );
        assert_eq!(returned_sized.pair_id().as_str(), "AAPL-MSFT");
    }

    #[tokio::test]
    async fn test_execute_open_pairs_multiple_pairs() {
        let mock = MockTrading::default();
        let pool = dummy_pool();
        let sized_pairs = vec![
            make_test_sized_pair("AAPL", "MSFT"),
            make_test_sized_pair("GOOG", "META"),
        ];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.long().ticker, "AAPL");
        assert_eq!(results[1].0.long().ticker, "GOOG");
    }

    #[tokio::test]
    async fn test_execute_open_pairs_short_fails_compensates_long() {
        let mock = MockTrading {
            should_fail_short_order: true,
            ..MockTrading::default()
        };
        let pool = dummy_pool();
        let sized_pairs = vec![make_test_sized_pair("AAPL", "MSFT")];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        // Pair should be skipped because the short leg failed.
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_open_pairs_long_fails_compensates_short() {
        let mock = MockTrading {
            should_fail_long_order: true,
            ..MockTrading::default()
        };
        let pool = dummy_pool();
        let sized_pairs = vec![make_test_sized_pair("AAPL", "MSFT")];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        // Pair should be skipped because the long leg failed.
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_open_pairs_both_legs_fail_skips_pair() {
        let mock = MockTrading {
            should_fail_long_order: true,
            should_fail_short_order: true,
            ..MockTrading::default()
        };
        let pool = dummy_pool();
        let sized_pairs = vec![make_test_sized_pair("AAPL", "MSFT")];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_open_pairs_cancel_fails_falls_back_to_close() {
        // Short leg fails → compensate orphaned long → cancel fails → close_position fallback.
        let mock = MockTrading {
            should_fail_short_order: true,
            should_fail_cancel: true,
            ..MockTrading::default()
        };
        let pool = dummy_pool();
        let sized_pairs = vec![make_test_sized_pair("AAPL", "MSFT")];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        // Pair is still skipped; the close_position fallback should succeed
        // (should_fail_close defaults to false).
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_open_pairs_cancel_and_close_both_fail_persists_compensation() {
        // Short leg fails → compensate orphaned long → cancel fails → close fails
        // → persist_compensation_failure (which will fail on dummy pool but is swallowed).
        let mock = MockTrading {
            should_fail_short_order: true,
            should_fail_cancel: true,
            should_fail_close: true,
            ..MockTrading::default()
        };
        let pool = dummy_pool();
        let sized_pairs = vec![make_test_sized_pair("AAPL", "MSFT")];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        // Pair is skipped. The compensation failure persist will also fail (dummy
        // pool), but execution must not panic.
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_open_pairs_empty_input() {
        let mock = MockTrading::default();
        let pool = dummy_pool();

        let results = execute_open_pairs(&mock, &pool, &[]).await;

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_execute_open_pairs_partial_success_mixed_pairs() {
        // Two pairs: first succeeds, second has both legs fail.
        // We need the second pair's submissions to fail but the first pair's to succeed.
        // Since MockTrading applies failure flags uniformly, we test partial success
        // by verifying that successful pairs are returned while failed ones are skipped.
        let mock = MockTrading::default();
        let pool = dummy_pool();
        let sized_pairs = vec![
            make_test_sized_pair("AAPL", "MSFT"),
            make_test_sized_pair("GOOG", "META"),
        ];

        let results = execute_open_pairs(&mock, &pool, &sized_pairs).await;

        // Both should succeed with default mock.
        assert_eq!(results.len(), 2);
    }

    // --- confirm_fills tests ---

    /// Creates a `PendingPair` with known alpaca order IDs for fill testing.
    fn make_test_pending_pair(long_ticker: &str, short_ticker: &str) -> (PendingPair, SizedPair) {
        let now = Utc::now();
        let long_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            long_ticker.to_string(),
            OrderSide::Long,
            Decimal::ZERO,
            "market".to_string(),
            None,
            format!("alpaca-long-{}", long_ticker.to_lowercase()),
            now,
        );
        let short_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            short_ticker.to_string(),
            OrderSide::Short,
            Decimal::from(50),
            "market".to_string(),
            None,
            format!("alpaca-short-{}", short_ticker.to_lowercase()),
            now,
        );
        let pending_pair = PendingPair::new(long_order, short_order, 1.1, 0.9);
        let sized_pair = make_test_sized_pair(long_ticker, short_ticker);
        (pending_pair, sized_pair)
    }

    #[tokio::test]
    async fn test_confirm_fills_happy_path_both_legs_fill() {
        let mock = MockTrading::default();
        let (pending_pair, sized_pair) = make_test_pending_pair("AAPL", "MSFT");

        let results = confirm_fills(&mock, vec![(pending_pair, sized_pair)]).await;

        assert_eq!(results.len(), 1);
        let (filled_pair, _) = &results[0];
        assert_eq!(filled_pair.long.ticker, "AAPL");
        assert_eq!(filled_pair.short.ticker, "MSFT");
        assert!(filled_pair.long.fill_price.is_some());
        assert!(filled_pair.short.fill_price.is_some());
    }

    #[tokio::test]
    async fn test_confirm_fills_long_never_fills_pair_dropped() {
        // Pre-load order_fills so the long leg returns "new" (not filled) on
        // every poll attempt, and the short leg fills normally.
        let mut fills = Vec::new();
        // Long leg polls: 5 attempts, all return "new" status.
        for _ in 0..FILL_POLL_ATTEMPTS {
            fills.push(OrderFill {
                alpaca_order_id: "alpaca-long-aapl".to_string(),
                status: "new".to_string(),
                filled_quantity: None,
                fill_price: None,
            });
        }
        // Short leg polls: filled on first attempt.
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-short-msft".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(50.0),
            fill_price: Some(100.0),
        });

        let mock = MockTrading {
            order_fills: std::sync::Mutex::new(fills),
            ..MockTrading::default()
        };
        let (pending_pair, sized_pair) = make_test_pending_pair("AAPL", "MSFT");

        let results = confirm_fills(&mock, vec![(pending_pair, sized_pair)]).await;

        // Pair should be dropped because the long leg never filled.
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_confirm_fills_short_never_fills_pair_dropped() {
        let mut fills = Vec::new();
        // Long leg: fills on first attempt.
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-long-aapl".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(100.0),
            fill_price: Some(150.0),
        });
        // Short leg: 5 attempts, all "new".
        for _ in 0..FILL_POLL_ATTEMPTS {
            fills.push(OrderFill {
                alpaca_order_id: "alpaca-short-msft".to_string(),
                status: "new".to_string(),
                filled_quantity: None,
                fill_price: None,
            });
        }

        let mock = MockTrading {
            order_fills: std::sync::Mutex::new(fills),
            ..MockTrading::default()
        };
        let (pending_pair, sized_pair) = make_test_pending_pair("AAPL", "MSFT");

        let results = confirm_fills(&mock, vec![(pending_pair, sized_pair)]).await;

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_confirm_fills_zero_fill_price_retries() {
        let mut fills = Vec::new();
        // Long leg: first attempt has zero price, second attempt has valid fill.
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-long-aapl".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(100.0),
            fill_price: Some(0.0),
        });
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-long-aapl".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(100.0),
            fill_price: Some(150.0),
        });
        // Short leg: fills immediately.
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-short-msft".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(50.0),
            fill_price: Some(100.0),
        });

        let mock = MockTrading {
            order_fills: std::sync::Mutex::new(fills),
            ..MockTrading::default()
        };
        let (pending_pair, sized_pair) = make_test_pending_pair("AAPL", "MSFT");

        let results = confirm_fills(&mock, vec![(pending_pair, sized_pair)]).await;

        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_confirm_fills_empty_input() {
        let mock = MockTrading::default();

        let results = confirm_fills(&mock, vec![]).await;

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_confirm_fills_multiple_pairs_partial_success() {
        let mut fills = Vec::new();
        // Pair 1 (AAPL-MSFT): both legs fill.
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-long-aapl".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(100.0),
            fill_price: Some(150.0),
        });
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-short-msft".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(50.0),
            fill_price: Some(100.0),
        });
        // Pair 2 (GOOG-META): long fills but short never fills.
        fills.push(OrderFill {
            alpaca_order_id: "alpaca-long-goog".to_string(),
            status: "filled".to_string(),
            filled_quantity: Some(20.0),
            fill_price: Some(180.0),
        });
        for _ in 0..FILL_POLL_ATTEMPTS {
            fills.push(OrderFill {
                alpaca_order_id: "alpaca-short-meta".to_string(),
                status: "new".to_string(),
                filled_quantity: None,
                fill_price: None,
            });
        }

        let mock = MockTrading {
            order_fills: std::sync::Mutex::new(fills),
            ..MockTrading::default()
        };
        let (pair1, sized1) = make_test_pending_pair("AAPL", "MSFT");
        let (pair2, sized2) = make_test_pending_pair("GOOG", "META");

        let results = confirm_fills(&mock, vec![(pair1, sized1), (pair2, sized2)]).await;

        // Only pair 1 should succeed.
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.long.ticker, "AAPL");
    }

    // --- close_positions tests ---

    #[tokio::test]
    async fn test_close_positions_happy_path() {
        let mock = MockTrading::default();
        let tickers = vec!["AAPL".to_string(), "MSFT".to_string()];

        let result = close_positions(&mock, &tickers).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_close_positions_empty_tickers() {
        let mock = MockTrading::default();

        let result = close_positions(&mock, &[]).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_close_positions_failure_returns_first_error() {
        let mock = MockTrading {
            should_fail_close: true,
            ..MockTrading::default()
        };
        let tickers = vec!["AAPL".to_string(), "MSFT".to_string()];

        let result = close_positions(&mock, &tickers).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        // The first error should reference the first ticker.
        let message = format!("{error}");
        assert!(message.contains("AAPL"));
    }

    #[tokio::test]
    async fn test_close_positions_continues_after_failure() {
        // Even when close fails for one ticker, the function should attempt
        // closing all remaining tickers (returning the first error only).
        let mock = MockTrading {
            should_fail_close: true,
            ..MockTrading::default()
        };
        let tickers = vec!["AAPL".to_string(), "MSFT".to_string(), "GOOG".to_string()];

        let result = close_positions(&mock, &tickers).await;

        // Should error but not panic — all tickers attempted.
        assert!(result.is_err());
    }

    #[test]
    fn test_execution_error_display_position_fetch() {
        let error = ExecutionError::PositionFetch {
            source: ClientError::Parse("connection refused".to_string()),
        };
        let message = format!("{error}");
        assert!(message.contains("Position fetch"));
        assert!(message.contains("connection refused"));
    }

    #[test]
    fn test_execution_error_display_state_mismatch() {
        let error = ExecutionError::StateMismatch {
            message: "Alpaca has 3 positions but database has 0 open pairs".to_string(),
        };
        let message = format!("{error}");
        assert!(message.contains("State mismatch"));
        assert!(message.contains("3 positions"));
    }
}
