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

/// Submits long and short orders for each sized pair, returning pending pairs.
///
/// Pairs whose order submission fails are logged and skipped; they do not
/// appear in the returned vec. This is a best-effort operation — partial
/// success is expected in live trading when individual tickers have liquidity
/// or borrowing issues.
pub async fn execute_open_pairs(
    alpaca: &AlpacaTradingClient,
    sized_pairs: &[SizedPair],
) -> Vec<(PendingPair, SizedPair)> {
    let mut results: Vec<(PendingPair, SizedPair)> = Vec::new();

    for sized_pair in sized_pairs {
        // Submit the short leg first so we know it is borrowable before tying
        // up capital on the long leg.
        let short_alpaca_id = match alpaca
            .submit_short_order(sized_pair.short_ticker(), sized_pair.short_quantity())
            .await
        {
            Ok(order_id) => order_id,
            Err(error) => {
                warn!(
                    ticker = sized_pair.short_ticker(),
                    error = %error,
                    "Short order submission failed; skipping pair"
                );
                continue;
            }
        };

        let long_alpaca_id = match alpaca
            .submit_long_order(sized_pair.long_ticker(), sized_pair.long_dollar_amount())
            .await
        {
            Ok(order_id) => order_id,
            Err(error) => {
                warn!(
                    ticker = sized_pair.long_ticker(),
                    error = %error,
                    "Long order submission failed; skipping pair"
                );
                // Compensate: attempt to close the short leg that already filled.
                if let Err(close_error) = alpaca.close_position(sized_pair.short_ticker()).await {
                    warn!(
                        ticker = sized_pair.short_ticker(),
                        error = %close_error,
                        "Compensation close of orphaned short leg failed"
                    );
                }
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

        info!(pair_id = sized_pair.pair_id(), "Orders submitted for pair");
        results.push((pending_pair, sized_pair.clone()));
    }

    results
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
                info!(pair_id = sized_pair.pair_id(), "Pair fills confirmed");
                results.push((filled_pair, sized_pair));
            }
            Err(error) => {
                warn!(
                    pair_id = sized_pair.pair_id(),
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
}
