//! Order submission and fill confirmation. The only module that sends an order.
//!
//! Opening a pair is two orders that have to both work or neither hold. The short leg goes first
//! and its fill is confirmed before the long leg is submitted at all, which is not the fastest
//! ordering but is the one that fails cleanly: a short can be rejected for borrow reasons a long
//! never is, and submitting them together means the common failure leaves a naked long to unwind.
//! Sequenced this way, the usual rejection costs nothing to recover from because there is nothing
//! yet to recover.
//!
//! Unwinding is still implemented, because the uncommon case — the short fills and the long does
//! not — is the one that leaves an unhedged position, and it is the one worth having a path for.

use std::time::Duration;

use tracing::{error, info, warn};

use crate::common::alpaca::{ClientError, OrderIntent, OrderState, TradingClient};
use crate::common::types::Ticker;
use crate::portfolio::pairs::PairEntry;
use crate::portfolio::size::SizedPair;

/// How long to wait for a market order to reach a terminal state.
///
/// A market order during regular hours fills in well under a second. This is a bound on how long to
/// keep an unhedged leg outstanding before unwinding it, not an expectation of how long a fill
/// takes.
pub const FILL_TIMEOUT: Duration = Duration::from_secs(30);

/// How often to ask Alpaca whether an order is done.
pub const FILL_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Failures executing against the broker.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("Alpaca rejected an order operation: {0}")]
    Alpaca(#[from] ClientError),
    #[error("pair entry could not be recorded: {0}")]
    Entry(#[from] crate::portfolio::pairs::InvalidEntryError),
}

/// Timeouts governing fill confirmation. Separated so tests do not wait thirty seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionSettings {
    fill_timeout: Duration,
    poll_interval: Duration,
}

impl ExecutionSettings {
    pub fn new(fill_timeout: Duration, poll_interval: Duration) -> Self {
        Self {
            fill_timeout,
            poll_interval,
        }
    }
}

impl Default for ExecutionSettings {
    fn default() -> Self {
        Self {
            fill_timeout: FILL_TIMEOUT,
            poll_interval: FILL_POLL_INTERVAL,
        }
    }
}

/// One leg that filled.
#[derive(Debug, Clone, PartialEq)]
pub struct LegFill {
    ticker: Ticker,
    order_id: String,
    shares: f64,
    average_price: f64,
}

impl LegFill {
    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    pub fn shares(&self) -> f64 {
        self.shares
    }

    pub fn average_price(&self) -> f64 {
        self.average_price
    }
}

/// What came of trying to open a pair.
///
/// [`OpenOutcome::Abandoned`] is not an error: the account is back where it started and the pass
/// should carry on to the next candidate. An `Err` from [`open_pair`] means the opposite — the
/// unwind itself failed, and something is held that nobody meant to hold.
#[derive(Debug, Clone, PartialEq)]
pub enum OpenOutcome {
    /// Both legs filled. The pair is on the book.
    Opened {
        entry: PairEntry,
        long_fill: LegFill,
        short_fill: LegFill,
    },
    /// A leg did not fill. Anything that did has been unwound.
    Abandoned { ticker: Ticker, reason: String },
}

/// What came of trying to close a pair.
///
/// Both flags matter separately. Neither leg being found means the position was already gone,
/// which is a different close reason from one the strategy chose; one leg being found and not the
/// other means the account was briefly unhedged and the logs should say so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseOutcome {
    long_closed: bool,
    short_closed: bool,
}

impl CloseOutcome {
    pub fn long_closed(self) -> bool {
        self.long_closed
    }

    pub fn short_closed(self) -> bool {
        self.short_closed
    }

    /// Whether Alpaca had no position for either leg.
    ///
    /// The pair is closed either way; this only decides whether the recorded reason is the one the
    /// strategy chose or [`crate::portfolio::pairs::CloseReason::PositionMissing`].
    pub fn was_already_gone(self) -> bool {
        !self.long_closed && !self.short_closed
    }
}

/// Opens both legs of a sized pair, unwinding whatever filled if either leg fails.
///
/// Returns [`OpenOutcome::Abandoned`] when a leg does not fill and the unwind succeeded, and an
/// error only when the unwind itself failed — the case where something is held that nothing knows
/// about, which the caller must surface rather than absorb.
pub async fn open_pair(
    client: &TradingClient,
    pair: &SizedPair,
    model_run_id: Option<String>,
    settings: ExecutionSettings,
) -> Result<OpenOutcome, ExecutionError> {
    let candidate = pair.candidate();
    let short_ticker = candidate.short_ticker().clone();
    let long_ticker = candidate.long_ticker().clone();

    // The short leg first, and confirmed, before the long is submitted. A borrow rejection then
    // costs nothing to recover from, because nothing is held yet.
    let short_fill = match submit_and_confirm(
        client,
        &OrderIntent::OpenShort {
            ticker: short_ticker.clone(),
            shares: pair.short_shares(),
        },
        settings,
    )
    .await?
    {
        Filled::Yes(fill) => fill,
        Filled::No(reason) => {
            info!(
                pair_id = %candidate.pair_id(),
                ticker = %short_ticker,
                reason,
                "Short leg did not fill; nothing to unwind"
            );
            return Ok(OpenOutcome::Abandoned {
                ticker: short_ticker,
                reason,
            });
        }
    };

    let long_fill = match submit_and_confirm(
        client,
        &OrderIntent::OpenLong {
            ticker: long_ticker.clone(),
            notional: pair.long_notional(),
        },
        settings,
    )
    .await?
    {
        Filled::Yes(fill) => fill,
        Filled::No(reason) => {
            warn!(
                pair_id = %candidate.pair_id(),
                ticker = %long_ticker,
                reason,
                "Long leg did not fill; unwinding the short leg"
            );
            // If the unwind itself fails, the short leg is held and no `equity_pairs` row exists
            // for it — `PairEntry` is not built until below. Name the held symbol and its size
            // before propagating, because the error carries only the broker's message and the
            // warning above names the leg that *failed*, not the one still on the book.
            if let Err(error) = client.close_position(&short_ticker).await {
                error!(
                    pair_id = %candidate.pair_id(),
                    held_ticker = %short_ticker,
                    held_shares = short_fill.shares,
                    %error,
                    "Unwind failed; an unhedged short position is held with no pair record"
                );
                return Err(error.into());
            }
            return Ok(OpenOutcome::Abandoned {
                ticker: long_ticker,
                reason,
            });
        }
    };

    let entry = PairEntry::new(
        candidate.pair_id().clone(),
        candidate.hedge_ratio(),
        candidate.entry_z_score(),
        candidate.signal_strength(),
        model_run_id,
    )?;

    info!(
        pair_id = %candidate.pair_id(),
        long_shares = long_fill.shares,
        short_shares = short_fill.shares,
        "Pair opened on Alpaca"
    );
    Ok(OpenOutcome::Opened {
        entry,
        long_fill,
        short_fill,
    })
}

/// Closes both legs of a pair.
///
/// **Both closes are always attempted, including when the first one errors.** A missing position is
/// `Ok(false)` and was never the hard case; a 500, a timeout, or a dropped connection is. Returning
/// early on one of those would leave a live short leg on the book — the naked directional position
/// the pair structure exists to avoid — and it would do so in exactly the situation where the leg is
/// most likely still held.
///
/// The error is reported only after both attempts have been made, so a failure on the long leg costs
/// the caller its error and nothing else.
pub async fn close_pair(
    client: &TradingClient,
    long_ticker: &Ticker,
    short_ticker: &Ticker,
) -> Result<CloseOutcome, ExecutionError> {
    let long_result = client.close_position(long_ticker).await;
    let short_result = client.close_position(short_ticker).await;

    if let Err(error) = &long_result {
        warn!(ticker = %long_ticker, %error, "Closing the long leg failed");
    }
    if let Err(error) = &short_result {
        warn!(ticker = %short_ticker, %error, "Closing the short leg failed");
    }

    let long_closed = long_result?;
    let short_closed = short_result?;

    if long_closed != short_closed {
        warn!(
            long = %long_ticker,
            short = %short_ticker,
            long_closed,
            short_closed,
            "Only one leg of a pair had a position to close"
        );
    }
    Ok(CloseOutcome {
        long_closed,
        short_closed,
    })
}

/// Whether a submitted order reached a fill.
enum Filled {
    Yes(LegFill),
    No(String),
}

/// Submits one order and polls until it reaches a terminal state or the timeout expires.
///
/// A timed-out order is cancelled before returning. Leaving it working would mean reporting the leg
/// as unfilled while Alpaca still holds a live order that could fill afterwards, into a pair the
/// application has already given up on.
async fn submit_and_confirm(
    client: &TradingClient,
    intent: &OrderIntent,
    settings: ExecutionSettings,
) -> Result<Filled, ExecutionError> {
    let order_id = client.submit_order(intent).await?;
    let deadline = tokio::time::Instant::now() + settings.fill_timeout;

    loop {
        let state = client.fetch_order(&order_id).await?;
        match state {
            OrderState::Filled {
                filled_shares,
                average_price,
            } => {
                return Ok(Filled::Yes(LegFill {
                    ticker: intent.ticker().clone(),
                    order_id,
                    shares: filled_shares,
                    average_price,
                }))
            }
            OrderState::Abandoned {
                status,
                filled_shares,
            } => {
                // A partial fill that then terminated leaves shares held. Close the position rather
                // than reporting the leg cleanly unfilled.
                if filled_shares > 0.0 {
                    warn!(
                        ticker = %intent.ticker(),
                        order_id,
                        filled_shares,
                        "Order terminated after a partial fill; closing the remainder"
                    );
                    client.close_position(intent.ticker()).await?;
                }
                return Ok(Filled::No(status));
            }
            OrderState::Working { .. } => {
                if tokio::time::Instant::now() >= deadline {
                    warn!(
                        ticker = %intent.ticker(),
                        order_id,
                        "Order did not reach a terminal state before the timeout; cancelling"
                    );
                    client.cancel_order(&order_id).await?;
                    // Read once more: the cancel may have raced a fill.
                    if let OrderState::Filled {
                        filled_shares,
                        average_price,
                    } = client.fetch_order(&order_id).await?
                    {
                        return Ok(Filled::Yes(LegFill {
                            ticker: intent.ticker().clone(),
                            order_id,
                            shares: filled_shares,
                            average_price,
                        }));
                    }
                    client.close_position(intent.ticker()).await?;
                    return Ok(Filled::No("timed_out".to_string()));
                }
                tokio::time::sleep(settings.poll_interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::alpaca::AlpacaCredentials;
    use crate::common::types::{Dollars, PairID, Ticker};
    use crate::portfolio::screen::PairCandidate;
    use crate::portfolio::size::size_pair;
    use rust_decimal::Decimal;

    fn credentials() -> AlpacaCredentials {
        AlpacaCredentials::new("key".to_string(), "secret".to_string()).unwrap()
    }

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).unwrap()
    }

    fn settings() -> ExecutionSettings {
        ExecutionSettings::new(Duration::from_millis(50), Duration::from_millis(5))
    }

    fn pair() -> SizedPair {
        let candidate = PairCandidate::new(
            PairID::new(ticker("AAAA"), ticker("BBBB")),
            1.0,
            2.5,
            0.02,
            100.0,
            100.0,
        )
        .unwrap();
        size_pair(&candidate, Dollars::new(Decimal::from(5_000)).unwrap()).unwrap()
    }

    fn filled_body(order_id: &str) -> String {
        format!(
            r#"{{"id":"{order_id}","status":"filled","filled_qty":"50",
                 "filled_avg_price":"100.00"}}"#
        )
    }

    #[tokio::test]
    async fn test_open_pair_fills_both_legs() {
        let mut server = mockito::Server::new_async().await;
        let submit = server
            .mock("POST", "/v2/orders")
            .with_status(200)
            .with_body(r#"{"id":"order-1","status":"accepted"}"#)
            .expect(2)
            .create_async()
            .await;
        let confirm = server
            .mock("GET", "/v2/orders/order-1")
            .with_status(200)
            .with_body(filled_body("order-1"))
            .expect(2)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcome = open_pair(&client, &pair(), Some("run-1".to_string()), settings())
            .await
            .expect("the open must succeed");

        match outcome {
            OpenOutcome::Opened {
                entry,
                long_fill,
                short_fill,
            } => {
                assert_eq!(entry.model_run_id(), Some("run-1"));
                assert_eq!(long_fill.ticker().as_str(), "AAAA");
                assert_eq!(short_fill.ticker().as_str(), "BBBB");
                assert_eq!(short_fill.average_price(), 100.0);
            }
            other => panic!("expected both legs to fill, got {other:?}"),
        }
        submit.assert_async().await;
        confirm.assert_async().await;
    }

    /// The short goes first precisely so this case is cheap. A rejected borrow must leave the
    /// account untouched, which means no long order may have been sent at all.
    #[tokio::test]
    async fn test_a_rejected_short_leg_never_submits_the_long_leg() {
        let mut server = mockito::Server::new_async().await;
        let submit = server
            .mock("POST", "/v2/orders")
            .with_status(200)
            .with_body(r#"{"id":"order-1","status":"accepted"}"#)
            .expect(1)
            .create_async()
            .await;
        let confirm = server
            .mock("GET", "/v2/orders/order-1")
            .with_status(200)
            .with_body(r#"{"id":"order-1","status":"rejected","filled_qty":"0"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcome = open_pair(&client, &pair(), None, settings())
            .await
            .expect("an unfilled leg is not an error");

        assert!(matches!(
            outcome,
            OpenOutcome::Abandoned { ref ticker, .. } if ticker.as_str() == "BBBB"
        ));
        submit.assert_async().await;
        confirm.assert_async().await;
    }

    /// The case worth having an unwind for: the short filled, so the account is holding an unhedged
    /// position that nothing will otherwise close until the pre-close fail-safe.
    #[tokio::test]
    async fn test_a_failed_long_leg_unwinds_the_filled_short_leg() {
        let mut server = mockito::Server::new_async().await;
        let short_submit = server
            .mock("POST", "/v2/orders")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "symbol": "BBBB"
            })))
            .with_status(200)
            .with_body(r#"{"id":"short-1","status":"accepted"}"#)
            .create_async()
            .await;
        let short_confirm = server
            .mock("GET", "/v2/orders/short-1")
            .with_status(200)
            .with_body(filled_body("short-1"))
            .create_async()
            .await;
        let long_submit = server
            .mock("POST", "/v2/orders")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "symbol": "AAAA"
            })))
            .with_status(200)
            .with_body(r#"{"id":"long-1","status":"accepted"}"#)
            .create_async()
            .await;
        let long_confirm = server
            .mock("GET", "/v2/orders/long-1")
            .with_status(200)
            .with_body(r#"{"id":"long-1","status":"rejected","filled_qty":"0"}"#)
            .create_async()
            .await;
        let unwind = server
            .mock("DELETE", "/v2/positions/BBBB?percentage=100")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcome = open_pair(&client, &pair(), None, settings())
            .await
            .expect("the unwind must succeed");

        assert!(matches!(
            outcome,
            OpenOutcome::Abandoned { ref ticker, .. } if ticker.as_str() == "AAAA"
        ));
        short_submit.assert_async().await;
        short_confirm.assert_async().await;
        long_submit.assert_async().await;
        long_confirm.assert_async().await;
        unwind.assert_async().await;
    }

    /// An order still working at the deadline is cancelled and the symbol flattened. Reporting the
    /// leg unfilled while a live order remains at the broker is how a pair the application has
    /// given up on gets filled behind its back.
    #[tokio::test]
    async fn test_a_timed_out_order_is_cancelled_and_flattened() {
        let mut server = mockito::Server::new_async().await;
        let _submit = server
            .mock("POST", "/v2/orders")
            .with_status(200)
            .with_body(r#"{"id":"order-1","status":"accepted"}"#)
            .create_async()
            .await;
        let _confirm = server
            .mock("GET", "/v2/orders/order-1")
            .with_status(200)
            .with_body(r#"{"id":"order-1","status":"new","filled_qty":"0"}"#)
            .create_async()
            .await;
        let cancel = server
            .mock("DELETE", "/v2/orders/order-1")
            .with_status(204)
            .expect(1)
            .create_async()
            .await;
        let flatten = server
            .mock("DELETE", "/v2/positions/BBBB?percentage=100")
            .with_status(404)
            .expect(1)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcome = open_pair(&client, &pair(), None, settings())
            .await
            .expect("a timeout is not an error");

        assert!(matches!(
            outcome,
            OpenOutcome::Abandoned { ref reason, .. } if reason == "timed_out"
        ));
        cancel.assert_async().await;
        flatten.assert_async().await;
    }

    /// A partial fill that then terminates leaves shares held. Reporting the leg cleanly unfilled
    /// would abandon them.
    #[tokio::test]
    async fn test_a_partially_filled_then_cancelled_order_closes_the_remainder() {
        let mut server = mockito::Server::new_async().await;
        let _submit = server
            .mock("POST", "/v2/orders")
            .with_status(200)
            .with_body(r#"{"id":"order-1","status":"accepted"}"#)
            .create_async()
            .await;
        let _confirm = server
            .mock("GET", "/v2/orders/order-1")
            .with_status(200)
            .with_body(r#"{"id":"order-1","status":"canceled","filled_qty":"12"}"#)
            .create_async()
            .await;
        let flatten = server
            .mock("DELETE", "/v2/positions/BBBB?percentage=100")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcome = open_pair(&client, &pair(), None, settings())
            .await
            .expect("a partial fill is not an error");

        assert!(matches!(outcome, OpenOutcome::Abandoned { .. }));
        flatten.assert_async().await;
    }

    /// Both legs are always attempted. Stopping after a missing long would leave a live short on
    /// the book — the naked directional position the pair structure exists to avoid.
    #[tokio::test]
    async fn test_close_pair_attempts_both_legs_even_when_the_first_is_missing() {
        let mut server = mockito::Server::new_async().await;
        let long = server
            .mock("DELETE", "/v2/positions/AAAA?percentage=100")
            .with_status(404)
            .expect(1)
            .create_async()
            .await;
        let short = server
            .mock("DELETE", "/v2/positions/BBBB?percentage=100")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcome = close_pair(&client, &ticker("AAAA"), &ticker("BBBB"))
            .await
            .expect("the close must succeed");

        assert!(!outcome.long_closed());
        assert!(outcome.short_closed());
        assert!(!outcome.was_already_gone());
        long.assert_async().await;
        short.assert_async().await;
    }

    /// A broker error on the long leg must not skip the short close. The 404 case was never the
    /// hard one; a 500 is, because that is when the leg is most likely still held — and returning
    /// early would leave a live short on the book overnight.
    #[tokio::test]
    async fn test_close_pair_still_closes_the_short_leg_when_the_long_leg_errors() {
        let mut server = mockito::Server::new_async().await;
        let long = server
            .mock("DELETE", "/v2/positions/AAAA?percentage=100")
            .with_status(500)
            .with_body("internal error")
            .expect(1)
            .create_async()
            .await;
        let short = server
            .mock("DELETE", "/v2/positions/BBBB?percentage=100")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let result = close_pair(&client, &ticker("AAAA"), &ticker("BBBB")).await;

        assert!(result.is_err(), "the broker failure must still be reported");
        long.assert_async().await;
        // The assertion that matters: the short close was attempted despite the long leg failing.
        short.assert_async().await;
    }

    #[tokio::test]
    async fn test_close_pair_reports_a_pair_that_was_already_gone() {
        let mut server = mockito::Server::new_async().await;
        let _long = server
            .mock("DELETE", "/v2/positions/AAAA?percentage=100")
            .with_status(404)
            .create_async()
            .await;
        let _short = server
            .mock("DELETE", "/v2/positions/BBBB?percentage=100")
            .with_status(404)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcome = close_pair(&client, &ticker("AAAA"), &ticker("BBBB"))
            .await
            .unwrap();
        assert!(outcome.was_already_gone());
    }
}
