//! Order submission and fill confirmation. The only module that sends an order.
//!
//! Opening a pair is two orders that must both work or neither hold. The short leg goes first and
//! its fill is confirmed before the long is submitted: a short can be rejected for borrow reasons a
//! long never is, so the common failure costs nothing to recover from because nothing is held yet.
//!
//! Unwinding is still implemented for the uncommon case — the short fills and the long does not —
//! which is the one that leaves an unhedged position.

use std::time::Duration;

use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::common::alpaca::{ClientError, OrderIntent, OrderState, PositionClose, TradingClient};
use crate::common::types::Ticker;
use crate::data::session_log::{
    Observation, OrderResolved, OrderSubmitted, PositionCloseRequested, SessionLog,
};
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

/// Everything an order needs beyond the order itself.
///
/// The log and the correlation identifier travel with the client rather than as further parameters
/// so that no path can send an order without carrying the thread back to the pass that decided it.
pub struct ExecutionContext<'a> {
    pub client: &'a TradingClient,
    pub settings: ExecutionSettings,
    pub session_log: &'a SessionLog,
    /// The evaluation pass this order belongs to.
    pub correlation_id: Uuid,
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
    context: &ExecutionContext<'_>,
    pair: &SizedPair,
    model_run_id: Option<String>,
) -> Result<OpenOutcome, ExecutionError> {
    let candidate = pair.candidate();
    let short_ticker = candidate.short_ticker().clone();
    let long_ticker = candidate.long_ticker().clone();

    // The short leg first, and confirmed, before the long is submitted. A borrow rejection then
    // costs nothing to recover from, because nothing is held yet.
    let short_fill = match submit_and_confirm(
        context,
        &OrderIntent::OpenShort {
            ticker: short_ticker.clone(),
            shares: pair.short_shares(),
        },
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
        context,
        &OrderIntent::OpenLong {
            ticker: long_ticker.clone(),
            notional: pair.long_notional(),
        },
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
            let unwind = context.client.close_position(&short_ticker).await;
            record_close(
                context,
                &short_ticker,
                Some(candidate.pair_id().as_str()),
                "entry_unwind",
                &unwind,
            )
            .await;
            if let Err(error) = unwind {
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
/// **Both closes are always attempted, even when the first errors.** A missing position is
/// `Ok(false)`; a 500 or a timeout is the hard case, and returning early on one would leave a live
/// short leg — the naked directional position the pair structure exists to avoid — in exactly the
/// situation where it is most likely still held. The error is reported after both attempts.
pub async fn close_pair(
    context: &ExecutionContext<'_>,
    pair_id: &str,
    long_ticker: &Ticker,
    short_ticker: &Ticker,
) -> Result<CloseOutcome, ExecutionError> {
    let long_result = context.client.close_position(long_ticker).await;
    let short_result = context.client.close_position(short_ticker).await;

    // Recorded before the errors are propagated. A leg whose close failed is exactly the one worth
    // having a record of, and `?` below returns without reaching any later write.
    for (ticker, result) in [(long_ticker, &long_result), (short_ticker, &short_result)] {
        record_close(context, ticker, Some(pair_id), "pair_exit", result).await;
    }

    if let Err(error) = &long_result {
        warn!(ticker = %long_ticker, %error, "Closing the long leg failed");
    }
    if let Err(error) = &short_result {
        warn!(ticker = %short_ticker, %error, "Closing the short leg failed");
    }

    let long_closed = long_result?.is_some();
    let short_closed = short_result?.is_some();

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

/// Records one close attempt, whatever came of it.
///
/// Takes the `Result` rather than a success value so a refused close is as visible as an accepted
/// one: a leg the broker would not let go of is the state the book is wrong about.
async fn record_close(
    context: &ExecutionContext<'_>,
    ticker: &Ticker,
    pair_id: Option<&str>,
    reason: &str,
    result: &Result<Option<PositionClose>, ClientError>,
) {
    let close = result.as_ref().ok().and_then(Option::as_ref);
    context
        .session_log
        .record(
            context.correlation_id,
            Utc::now(),
            Observation::PositionCloseRequested(PositionCloseRequested {
                ticker: ticker.to_string(),
                pair_id: pair_id.map(str::to_string),
                alpaca_order_id: close
                    .and_then(|close| close.alpaca_order_id())
                    .map(str::to_string),
                side: close.and_then(|close| close.side()).map(str::to_string),
                quantity: close.and_then(PositionClose::quantity),
                reason: reason.to_string(),
                accepted: close.is_some(),
                status: None,
            }),
        )
        .await;
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
///
/// The intent is written to the session log and fsynced *before* the request is sent. That ordering
/// is the whole point: a crash between the two leaves a record of an order that may exist, which is
/// recoverable, rather than an order that exists with no record, which is not.
async fn submit_and_confirm(
    context: &ExecutionContext<'_>,
    intent: &OrderIntent,
) -> Result<Filled, ExecutionError> {
    let client_order_id = Uuid::new_v4().to_string();
    let (shares, notional) = match intent {
        OrderIntent::OpenShort { shares, .. } => (Some(f64::from(shares.get())), None),
        OrderIntent::OpenLong { notional, .. } => (None, notional.value().to_f64()),
    };
    context
        .session_log
        .record(
            context.correlation_id,
            Utc::now(),
            Observation::OrderSubmitted(OrderSubmitted {
                client_order_id: client_order_id.clone(),
                ticker: intent.ticker().to_string(),
                side: intent.side().to_string(),
                shares,
                notional,
            }),
        )
        .await;

    let order_id = context
        .client
        .submit_order(intent, &client_order_id)
        .await?;
    let deadline = tokio::time::Instant::now() + context.settings.fill_timeout;

    /// Records how the broker settled the order, then hands back what the caller returns.
    macro_rules! resolved {
        ($outcome:expr, $shares:expr, $price:expr, $after_cancel:expr) => {
            context
                .session_log
                .record(
                    context.correlation_id,
                    Utc::now(),
                    Observation::OrderResolved(OrderResolved {
                        client_order_id: client_order_id.clone(),
                        alpaca_order_id: order_id.clone(),
                        ticker: intent.ticker().to_string(),
                        outcome: $outcome,
                        filled_shares: $shares,
                        filled_average_price: $price,
                        filled_after_cancel: $after_cancel,
                    }),
                )
                .await
        };
    }

    loop {
        let state = context.client.fetch_order(&order_id).await?;
        match state {
            OrderState::Filled {
                filled_shares,
                average_price,
            } => {
                resolved!(
                    "filled".to_string(),
                    Some(filled_shares),
                    Some(average_price),
                    false
                );
                return Ok(Filled::Yes(LegFill {
                    ticker: intent.ticker().clone(),
                    order_id,
                    shares: filled_shares,
                    average_price,
                }));
            }
            OrderState::Abandoned {
                status,
                filled_shares,
            } => {
                resolved!(status.clone(), Some(filled_shares), None, false);
                // A partial fill that then terminated leaves shares held. Close the position rather
                // than reporting the leg cleanly unfilled.
                if filled_shares > 0.0 {
                    warn!(
                        ticker = %intent.ticker(),
                        order_id,
                        filled_shares,
                        "Order terminated after a partial fill; closing the remainder"
                    );
                    let cleanup = context.client.close_position(intent.ticker()).await;
                    record_close(context, intent.ticker(), None, "entry_unwind", &cleanup).await;
                    cleanup?;
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
                    context.client.cancel_order(&order_id).await?;
                    // Read once more: the cancel may have raced a fill.
                    if let OrderState::Filled {
                        filled_shares,
                        average_price,
                    } = context.client.fetch_order(&order_id).await?
                    {
                        resolved!(
                            "filled".to_string(),
                            Some(filled_shares),
                            Some(average_price),
                            true
                        );
                        return Ok(Filled::Yes(LegFill {
                            ticker: intent.ticker().clone(),
                            order_id,
                            shares: filled_shares,
                            average_price,
                        }));
                    }
                    resolved!("timed_out".to_string(), None, None, false);
                    let cleanup = context.client.close_position(intent.ticker()).await;
                    record_close(context, intent.ticker(), None, "entry_unwind", &cleanup).await;
                    cleanup?;
                    return Ok(Filled::No("timed_out".to_string()));
                }
                tokio::time::sleep(context.settings.poll_interval).await;
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

    /// A log in a directory of this test's own. Every order path writes to one, so the tests
    /// exercise the record-before-submit ordering rather than mocking it away.
    fn session_log(name: &str) -> SessionLog {
        let directory = std::env::temp_dir().join(format!("fund-execute-{name}"));
        let _ = std::fs::remove_dir_all(&directory);
        SessionLog::new(directory).expect("the log directory must be creatable")
    }

    /// Every record a log holds, in the order it was written.
    fn recorded(log: &SessionLog) -> Vec<serde_json::Value> {
        let mut files: Vec<_> = std::fs::read_dir(log.directory())
            .expect("the log directory must be readable")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect();
        files.sort();
        files
            .iter()
            .filter_map(|path| std::fs::read_to_string(path).ok())
            .flat_map(|contents| {
                contents
                    .lines()
                    .filter_map(|line| serde_json::from_str(line).ok())
                    .collect::<Vec<serde_json::Value>>()
            })
            .collect()
    }

    fn context<'a>(client: &'a TradingClient, log: &'a SessionLog) -> ExecutionContext<'a> {
        ExecutionContext {
            client,
            settings: settings(),
            session_log: log,
            correlation_id: Uuid::nil(),
        }
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
        let log = session_log("fills-both-legs");
        let outcome = open_pair(&context(&client, &log), &pair(), Some("run-1".to_string()))
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
        let log = session_log("rejected-short");
        let outcome = open_pair(&context(&client, &log), &pair(), None)
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
        let log = session_log("failed-long");
        let outcome = open_pair(&context(&client, &log), &pair(), None)
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
        let log = session_log("timed-out");
        let outcome = open_pair(&context(&client, &log), &pair(), None)
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
        let log = session_log("partial-fill");
        let outcome = open_pair(&context(&client, &log), &pair(), None)
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
        let log = session_log("close-both-legs");
        let outcome = close_pair(
            &context(&client, &log),
            "AAAA-BBBB",
            &ticker("AAAA"),
            &ticker("BBBB"),
        )
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
        let log = session_log("close-long-errors");
        let result = close_pair(
            &context(&client, &log),
            "AAAA-BBBB",
            &ticker("AAAA"),
            &ticker("BBBB"),
        )
        .await;

        assert!(result.is_err(), "the broker failure must still be reported");
        long.assert_async().await;
        // The assertion that matters: the short close was attempted despite the long leg failing.
        short.assert_async().await;
    }

    /// Both legs of every exit are recorded, including the leg that had nothing to close.
    ///
    /// Exits were invisible for as long as entries were logged and closes were not, which made
    /// realized profit and loss attributable in one direction only. A close that found no position
    /// is recorded too: `accepted: false` with no order is a different fact from a filled exit.
    #[tokio::test]
    async fn test_closing_a_pair_records_both_legs() {
        let mut server = mockito::Server::new_async().await;
        let _long = server
            .mock("DELETE", "/v2/positions/AAAA?percentage=100")
            .with_status(200)
            .with_body(r#"{"id":"close-1","qty":"50","side":"sell"}"#)
            .create_async()
            .await;
        let _short = server
            .mock("DELETE", "/v2/positions/BBBB?percentage=100")
            .with_status(404)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let log = session_log("records-both-legs");
        close_pair(
            &context(&client, &log),
            "AAAA-BBBB",
            &ticker("AAAA"),
            &ticker("BBBB"),
        )
        .await
        .expect("the close must succeed");

        let records = recorded(&log);
        assert_eq!(records.len(), 2, "one record per leg");
        assert!(records
            .iter()
            .all(|record| record["event_type"] == "position_close_requested"
                && record["payload"]["pair_id"] == "AAAA-BBBB"
                && record["payload"]["reason"] == "pair_exit"));

        let filled = &records[0];
        assert_eq!(filled["payload"]["ticker"], "AAAA");
        assert_eq!(filled["payload"]["accepted"], true);
        // The order identifier is the join to the fill: a close is not polled, so the price
        // arrives later through the post-close activity sync.
        assert_eq!(filled["payload"]["alpaca_order_id"], "close-1");
        assert_eq!(filled["payload"]["quantity"], 50.0);

        let missing = &records[1];
        assert_eq!(missing["payload"]["ticker"], "BBBB");
        assert_eq!(missing["payload"]["accepted"], false);
        assert!(missing["payload"]["alpaca_order_id"].is_null());
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
        let log = session_log("already-gone");
        let outcome = close_pair(
            &context(&client, &log),
            "AAAA-BBBB",
            &ticker("AAAA"),
            &ticker("BBBB"),
        )
        .await
        .unwrap();
        assert!(outcome.was_already_gone());
    }
}
