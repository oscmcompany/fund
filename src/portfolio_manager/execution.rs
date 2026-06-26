//! Trade execution using the Alpaca trading client and the order typestate machine.
//!
//! Converts [`SizedPair`] records into [`PendingPair`] values by submitting orders
//! to Alpaca, then polls fill confirmations to produce [`FilledPair`] values.
//! Closing existing positions is handled via `close_position` on each leg.

use chrono::Utc;
use rust_decimal::Decimal;
use tokio::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

use crate::domain::orders::{FilledOrder, Order, OrderSide, PendingPair};
use crate::portfolio_manager::alpaca::{AlpacaError, AlpacaTradingClient};
use crate::portfolio_manager::sizing::SizedPair;

/// Maximum number of fill-poll attempts per order before giving up.
const FILL_POLL_ATTEMPTS: usize = 5;

/// Error produced during the open-positions execution phase.
#[derive(Debug)]
pub enum ExecutionError {
    /// Alpaca returned an API or network error during order submission.
    OrderSubmission { ticker: String, source: AlpacaError },
    /// Alpaca returned an API or network error during fill polling.
    FillPoll {
        alpaca_order_id: String,
        source: AlpacaError,
    },
    /// Alpaca returned an API or network error when closing a position.
    PositionClose { ticker: String, source: AlpacaError },
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
    alpaca: &AlpacaTradingClient,
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
                compensate_orphaned_order(alpaca, &long_id, sized_pair.long_ticker().as_str())
                    .await;
                continue;
            }
            (Ok(short_id), Err(error)) => {
                warn!(
                    ticker = sized_pair.long_ticker().as_str(),
                    error = %error,
                    "Long order submission failed; cancelling orphaned short order"
                );
                compensate_orphaned_order(alpaca, &short_id, sized_pair.short_ticker().as_str())
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
async fn compensate_orphaned_order(
    alpaca: &AlpacaTradingClient,
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
            if let Err(error) = alpaca.close_position(ticker).await {
                warn!(
                    ticker = ticker,
                    error = %error,
                    "Failed to close orphaned position after order already filled"
                );
            }
        }
        Err(error) => {
            warn!(
                alpaca_order_id = alpaca_order_id,
                ticker = ticker,
                error = %error,
                "Failed to cancel orphaned order"
            );
        }
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
    alpaca: &AlpacaTradingClient,
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
/// Returns `None` after `FILL_POLL_ATTEMPTS` failed attempts or when the
/// Alpaca order status does not indicate a fill.
async fn poll_fill(alpaca: &AlpacaTradingClient, alpaca_order_id: &str) -> Option<FilledOrder> {
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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    warn!(
        alpaca_order_id = alpaca_order_id,
        attempts = FILL_POLL_ATTEMPTS,
        "Fill poll exhausted; order not confirmed"
    );
    None
}

/// Closes open positions for the given tickers via Alpaca.
///
/// Each ticker is closed independently; failures are logged but do not stop
/// the remaining closures. Returns an `ExecutionError` only when a network-level
/// error (not a 404 "no position") is encountered.
pub async fn close_positions(
    alpaca: &AlpacaTradingClient,
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
    fn test_execution_error_display_order_submission() {
        let error = ExecutionError::OrderSubmission {
            ticker: "AAPL".to_string(),
            source: AlpacaError::Parse("bad json".to_string()),
        };
        let message = format!("{error}");
        assert!(message.contains("AAPL"));
        assert!(message.contains("submission"));
    }

    #[test]
    fn test_execution_error_display_fill_poll() {
        let error = ExecutionError::FillPoll {
            alpaca_order_id: "order-123".to_string(),
            source: AlpacaError::Parse("timeout".to_string()),
        };
        let message = format!("{error}");
        assert!(message.contains("order-123"));
        assert!(message.contains("Fill poll"));
    }

    #[test]
    fn test_execution_error_display_position_close() {
        let error = ExecutionError::PositionClose {
            ticker: "MSFT".to_string(),
            source: AlpacaError::Parse("network error".to_string()),
        };
        let message = format!("{error}");
        assert!(message.contains("MSFT"));
        assert!(message.contains("close"));
    }

    #[test]
    fn test_execution_error_is_error_trait() {
        let error = ExecutionError::PositionClose {
            ticker: "TSLA".to_string(),
            source: AlpacaError::Parse("x".to_string()),
        };
        // Verify std::error::Error is implemented
        let _boxed: Box<dyn std::error::Error> = Box::new(error);
    }

    #[test]
    fn test_execution_error_order_submission_source_included_in_display() {
        let error = ExecutionError::OrderSubmission {
            ticker: "NVDA".to_string(),
            source: AlpacaError::Api {
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
            source: AlpacaError::Api {
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
            source: AlpacaError::Api {
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
}
