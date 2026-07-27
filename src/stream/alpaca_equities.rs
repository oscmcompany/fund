//! Alpaca equity quote stream producer.
//!
//! Connects to the Alpaca market data WebSocket, subscribes to the legs of
//! currently open pairs, and publishes each quote into the shared
//! [`MarketDataBuffer`]. Quotes are [`DataBoundary::Ephemeral`]: they live only
//! in the broadcast channel and never reach PostgreSQL.
//!
//! Only open position legs are streamed, never entry candidates. Exits are the
//! time-critical decision — a spread can converge at any moment and the position
//! is already at risk — whereas candidates only need pricing once per evaluation
//! pass, which `select_size_execute` already covers with a single batched REST
//! snapshot. Streaming candidates would also not fit: the free tier allows 30
//! symbols against a candidate pool that ranks thousands of pairs.
//!
//! Subscriptions are refreshed by reconnecting rather than by sending a new
//! subscribe frame mid-session. [`run_connection`] sends startup messages once
//! at connect time, and the portfolio turns over a handful of times per day, so
//! a one-second reconnect on change is cheaper than threading an outbound
//! channel through shared connection infrastructure. The gap is covered by the
//! consumer's staleness guard, which falls back to the prior close.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::common::alpaca::AlpacaCredentials;
use crate::common::market_hours::{
    duration_until_quote_stream_window, is_within_quote_stream_window,
};
use crate::domain::market::{EquityQuote, Ticker};
use crate::domain::portfolio::MinimumPairs;
use crate::portfolio::database::fetch_open_pairs;
use crate::stream::buffer::MarketDataBuffer;
use crate::stream::connection::{run_connection, ConnectionConfiguration, MessagePayload};

/// Default Alpaca market data feed. IEX is the only feed available on the free
/// plan; `sip` requires Algo Trader Plus.
const DEFAULT_FEED_URL: &str = "wss://stream.data.alpaca.markets/v2/iex";

/// Default maximum symbols per connection.
///
/// The free plan permits 30 symbols for trades and quotes across a single
/// concurrent connection. Paid plans lift the symbol cap but not the connection
/// count, so raising this value is the only change needed after an upgrade.
const DEFAULT_SYMBOL_LIMIT: usize = 30;

/// How often the open-pair set is re-read to detect a subscription change.
const SUBSCRIPTION_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait before retrying when no pairs are open.
const IDLE_RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum symbols that may be subscribed on one connection.
///
/// A value in scope is proof that the fund's pair capacity fits inside the
/// broker's cap, checked once at startup rather than discovered as a runtime
/// error 405 mid-session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolSubscriptionLimit(usize);

impl SymbolSubscriptionLimit {
    /// Validates that both legs of every pair fit within `limit`.
    ///
    /// Each pair holds a long and a short position, so a portfolio of
    /// `minimum_pairs` pairs needs `2 × minimum_pairs` symbols streamed.
    pub fn new(limit: usize, minimum_pairs: MinimumPairs) -> Result<Self, SymbolLimitError> {
        let required = 2 * minimum_pairs.0.get() as usize;
        if required > limit {
            return Err(SymbolLimitError {
                required,
                limit,
                minimum_pairs: minimum_pairs.0.get(),
            });
        }
        Ok(Self(limit))
    }

    pub fn value(&self) -> usize {
        self.0
    }
}

/// Returned when the configured pair count cannot fit inside the symbol cap.
#[derive(Debug, PartialEq, Eq)]
pub struct SymbolLimitError {
    required: usize,
    limit: usize,
    minimum_pairs: u8,
}

impl std::fmt::Display for SymbolLimitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} pairs require {} streamed symbols but the quote stream limit is {}; \
             lower PORTFOLIO_MINIMUM_PAIRS or raise FUND_QUOTE_STREAM_SYMBOL_LIMIT \
             (the Alpaca free plan allows 30)",
            self.minimum_pairs, self.required, self.limit
        )
    }
}

impl std::error::Error for SymbolLimitError {}

/// Validated configuration for the quote stream producer.
#[derive(Debug, Clone)]
pub struct QuoteStreamConfiguration {
    feed_url: String,
    symbol_limit: SymbolSubscriptionLimit,
}

impl QuoteStreamConfiguration {
    /// Builds configuration from the environment, validating the symbol cap
    /// against the configured pair count.
    ///
    /// Reads `FUND_QUOTE_STREAM_FEED_URL` and `FUND_QUOTE_STREAM_SYMBOL_LIMIT`.
    pub fn from_env(minimum_pairs: MinimumPairs) -> Result<Self, SymbolLimitError> {
        let feed_url =
            std::env::var("FUND_QUOTE_STREAM_FEED_URL").unwrap_or_else(|_| DEFAULT_FEED_URL.into());
        let limit = std::env::var("FUND_QUOTE_STREAM_SYMBOL_LIMIT")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_SYMBOL_LIMIT);

        Ok(Self {
            feed_url,
            symbol_limit: SymbolSubscriptionLimit::new(limit, minimum_pairs)?,
        })
    }

    pub fn feed_url(&self) -> &str {
        &self.feed_url
    }

    pub fn symbol_limit(&self) -> SymbolSubscriptionLimit {
        self.symbol_limit
    }
}

/// Builds the Alpaca authentication frame.
fn authentication_message(credentials: &AlpacaCredentials) -> String {
    serde_json::json!({
        "action": "auth",
        "key": credentials.key_id(),
        "secret": credentials.secret(),
    })
    .to_string()
}

/// Builds the quote subscription frame for `symbols`.
fn subscription_message(symbols: &BTreeSet<String>) -> String {
    serde_json::json!({
        "action": "subscribe",
        "quotes": symbols.iter().collect::<Vec<_>>(),
    })
    .to_string()
}

/// A single message from the Alpaca stock stream.
///
/// Only quote fields are deserialized; control frames (`success`, `error`,
/// `subscription`) and other data types are recognized by `message_type` and
/// skipped.
#[derive(Debug, Deserialize)]
struct StreamMessage {
    #[serde(rename = "T")]
    message_type: String,
    #[serde(rename = "S")]
    symbol: Option<String>,
    #[serde(rename = "bp")]
    bid_price: Option<f64>,
    #[serde(rename = "ap")]
    ask_price: Option<f64>,
    #[serde(rename = "bs")]
    bid_size: Option<i32>,
    #[serde(rename = "as")]
    ask_size: Option<i32>,
    #[serde(rename = "t")]
    timestamp: Option<DateTime<Utc>>,
    #[serde(rename = "msg")]
    message: Option<String>,
}

/// Parses a stream frame into quotes.
///
/// Alpaca batches messages into a JSON array. Non-quote frames are skipped, and
/// a quote missing any field needed to form an `EquityQuote` is dropped rather
/// than defaulted, because a zero bid or ask would corrupt the spread it feeds.
fn parse_quotes(payload: &str) -> Vec<EquityQuote> {
    let messages: Vec<StreamMessage> = match serde_json::from_str(payload) {
        Ok(messages) => messages,
        Err(error) => {
            warn!(error = %error, "Failed to parse quote stream frame");
            return Vec::new();
        }
    };

    let mut quotes = Vec::new();
    for message in messages {
        match message.message_type.as_str() {
            "q" => {
                let Some(quote) = build_quote(&message) else {
                    warn!(symbol = ?message.symbol, "Skipping incomplete quote");
                    continue;
                };
                quotes.push(quote);
            }
            "error" => {
                // Code 405 is the symbol-limit rejection the startup check exists
                // to prevent; surfacing the raw message keeps it diagnosable.
                warn!(message = ?message.message, "Quote stream reported an error");
            }
            "success" | "subscription" => {
                info!(
                    message_type = %message.message_type,
                    message = ?message.message,
                    "Quote stream control frame"
                );
            }
            _ => {}
        }
    }
    quotes
}

/// Assembles an `EquityQuote` when every required field is present.
fn build_quote(message: &StreamMessage) -> Option<EquityQuote> {
    let ticker = Ticker::new(message.symbol.as_deref()?)?;
    Some(EquityQuote::new(
        message.timestamp?,
        ticker,
        message.bid_price?,
        message.ask_price?,
        message.bid_size.unwrap_or(0),
        message.ask_size.unwrap_or(0),
    ))
}

/// Returns the set of symbols to subscribe to, capped at the configured limit.
///
/// Pairs are read in database order, so a portfolio exceeding the cap streams a
/// stable prefix rather than a set that churns between polls. Exceeding the cap
/// is logged rather than silently truncated.
async fn desired_symbols(
    pool: &PgPool,
    limit: SymbolSubscriptionLimit,
) -> Result<BTreeSet<String>, sqlx::Error> {
    let open_pairs = fetch_open_pairs(pool).await?;

    let mut symbols = BTreeSet::new();
    for pair in &open_pairs {
        symbols.insert(pair.long_ticker().to_string());
        symbols.insert(pair.short_ticker().to_string());
    }

    if symbols.len() > limit.value() {
        let dropped = symbols.len() - limit.value();
        warn!(
            requested = symbols.len(),
            limit = limit.value(),
            dropped,
            "Open pair legs exceed the quote stream symbol limit; some legs will \
             fall back to prior-close pricing"
        );
        symbols = symbols.into_iter().take(limit.value()).collect();
    }

    Ok(symbols)
}

/// Resolves once the open-pair symbol set differs from `current`.
///
/// A database error leaves the current subscription in place: dropping a live
/// connection over a transient query failure would lose quotes for positions
/// that are still open.
async fn wait_for_subscription_change(
    pool: &PgPool,
    current: &BTreeSet<String>,
    limit: SymbolSubscriptionLimit,
    shutdown_token: &CancellationToken,
) {
    loop {
        tokio::select! {
            _ = sleep(SUBSCRIPTION_POLL_INTERVAL) => {}
            _ = shutdown_token.cancelled() => return,
        }

        match desired_symbols(pool, limit).await {
            Ok(symbols) if &symbols != current => {
                info!(
                    previous = current.len(),
                    current = symbols.len(),
                    "Open pair legs changed; resubscribing"
                );
                return;
            }
            Ok(_) => {}
            Err(error) => {
                warn!(error = %error, "Could not refresh quote stream symbols; keeping current subscription");
            }
        }
    }
}

/// Runs the quote stream producer until shutdown.
///
/// Sleeps outside the quote-stream window, waits for open pairs to exist, then
/// holds a subscription until the open-pair set changes.
pub async fn run_quote_stream(
    configuration: QuoteStreamConfiguration,
    credentials: AlpacaCredentials,
    pool: PgPool,
    buffer: Arc<MarketDataBuffer<EquityQuote>>,
    shutdown_token: CancellationToken,
) {
    info!(
        feed_url = configuration.feed_url(),
        symbol_limit = configuration.symbol_limit().value(),
        "Quote stream producer started"
    );

    while !shutdown_token.is_cancelled() {
        if !is_within_quote_stream_window() {
            let wait = duration_until_quote_stream_window(Utc::now());
            info!(
                wait_seconds = wait.as_secs(),
                "Outside quote stream window; sleeping"
            );
            tokio::select! {
                _ = sleep(wait) => {}
                _ = shutdown_token.cancelled() => break,
            }
            continue;
        }

        let symbols = match desired_symbols(&pool, configuration.symbol_limit()).await {
            Ok(symbols) => symbols,
            Err(error) => {
                warn!(error = %error, "Could not read open pairs; retrying");
                tokio::select! {
                    _ = sleep(IDLE_RETRY_INTERVAL) => {}
                    _ = shutdown_token.cancelled() => break,
                }
                continue;
            }
        };

        if symbols.is_empty() {
            info!("No open pairs; quote stream idle");
            tokio::select! {
                _ = sleep(IDLE_RETRY_INTERVAL) => {}
                _ = shutdown_token.cancelled() => break,
            }
            continue;
        }

        run_subscription_session(
            &configuration,
            &credentials,
            &pool,
            &buffer,
            &symbols,
            &shutdown_token,
        )
        .await;
    }

    info!("Quote stream producer stopped");
}

/// Holds one subscription until the symbol set changes or shutdown fires.
async fn run_subscription_session(
    configuration: &QuoteStreamConfiguration,
    credentials: &AlpacaCredentials,
    pool: &PgPool,
    buffer: &Arc<MarketDataBuffer<EquityQuote>>,
    symbols: &BTreeSet<String>,
    shutdown_token: &CancellationToken,
) {
    let config = ConnectionConfiguration::new(configuration.feed_url())
        .with_startup_message(authentication_message(credentials))
        .with_startup_message(subscription_message(symbols));

    info!(symbols = symbols.len(), "Subscribing to quote stream");

    // A child token ends only this session, leaving the producer loop free to
    // reconnect with the new symbol set.
    let session_token = shutdown_token.child_token();
    let publish_buffer = Arc::clone(buffer);

    let handler = move |payload: MessagePayload| {
        match payload {
            MessagePayload::Text(text) => {
                for quote in parse_quotes(&text) {
                    // A send error means no subscribers are attached yet, which is
                    // normal at startup and not a reason to drop the connection.
                    let _ = publish_buffer.publish(quote);
                }
            }
            MessagePayload::Binary(bytes) => {
                warn!(
                    bytes = bytes.len(),
                    "Received a binary quote frame; expected JSON"
                );
            }
        }
        true
    };

    tokio::select! {
        _ = run_connection(&config, &session_token, handler) => {}
        _ = wait_for_subscription_change(
            pool,
            symbols,
            configuration.symbol_limit(),
            shutdown_token,
        ) => {
            session_token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU8;

    fn minimum_pairs(count: u8) -> MinimumPairs {
        MinimumPairs(NonZeroU8::new(count).expect("count must be non-zero"))
    }

    // --- SymbolSubscriptionLimit ---

    #[test]
    fn test_symbol_limit_accepts_pairs_that_fit() {
        // Ten pairs need twenty symbols, inside the free-plan cap of thirty.
        let limit = SymbolSubscriptionLimit::new(30, minimum_pairs(10)).unwrap();
        assert_eq!(limit.value(), 30);
    }

    #[test]
    fn test_symbol_limit_accepts_exactly_at_capacity() {
        assert!(SymbolSubscriptionLimit::new(30, minimum_pairs(15)).is_ok());
    }

    #[test]
    fn test_symbol_limit_rejects_one_pair_over_capacity() {
        // Sixteen pairs need thirty-two symbols; this must fail at startup
        // rather than as an Alpaca error 405 mid-session.
        let error = SymbolSubscriptionLimit::new(30, minimum_pairs(16)).unwrap_err();
        assert_eq!(error.required, 32);
        assert_eq!(error.limit, 30);
    }

    #[test]
    fn test_symbol_limit_error_names_both_remedies() {
        let error = SymbolSubscriptionLimit::new(30, minimum_pairs(20)).unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains("PORTFOLIO_MINIMUM_PAIRS"));
        assert!(rendered.contains("FUND_QUOTE_STREAM_SYMBOL_LIMIT"));
    }

    // --- message construction ---

    #[test]
    fn test_authentication_message_shape() {
        let credentials =
            AlpacaCredentials::new("key-123".to_string(), "secret-456".to_string()).unwrap();
        let message: serde_json::Value =
            serde_json::from_str(&authentication_message(&credentials)).unwrap();
        assert_eq!(message["action"], "auth");
        assert_eq!(message["key"], "key-123");
        assert_eq!(message["secret"], "secret-456");
    }

    #[test]
    fn test_subscription_message_lists_quotes_only() {
        let symbols: BTreeSet<String> = ["AAPL", "MSFT"].iter().map(|s| s.to_string()).collect();
        let message: serde_json::Value =
            serde_json::from_str(&subscription_message(&symbols)).unwrap();
        assert_eq!(message["action"], "subscribe");
        assert_eq!(message["quotes"][0], "AAPL");
        assert_eq!(message["quotes"][1], "MSFT");
        // Trades and bars are not subscribed: they consume the same symbol cap
        // without feeding the spread calculation.
        assert!(message.get("trades").is_none());
        assert!(message.get("bars").is_none());
    }

    #[test]
    fn test_subscription_message_is_order_stable() {
        // BTreeSet ordering keeps the frame identical for the same set, so an
        // unchanged portfolio cannot produce a spurious resubscribe.
        let forward: BTreeSet<String> = ["AAPL", "MSFT"].iter().map(|s| s.to_string()).collect();
        let reverse: BTreeSet<String> = ["MSFT", "AAPL"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            subscription_message(&forward),
            subscription_message(&reverse)
        );
    }

    // --- parse_quotes ---

    #[test]
    fn test_parse_quote_frame() {
        let payload = r#"[{"T":"q","S":"AMD","bx":"K","bp":91.95,"bs":2,"ax":"Q","ap":91.98,"as":1,"c":["R"],"z":"C","t":"2023-04-06T11:54:21.670905508Z"}]"#;
        let quotes = parse_quotes(payload);

        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker().as_str(), "AMD");
        assert!((quotes[0].bid_price() - 91.95).abs() < f64::EPSILON);
        assert!((quotes[0].ask_price() - 91.98).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_multiple_quotes_in_one_frame() {
        let payload = r#"[
            {"T":"q","S":"AAPL","bp":180.0,"bs":1,"ap":180.1,"as":1,"t":"2023-04-06T11:54:21Z"},
            {"T":"q","S":"MSFT","bp":300.0,"bs":1,"ap":300.2,"as":1,"t":"2023-04-06T11:54:22Z"}
        ]"#;
        assert_eq!(parse_quotes(payload).len(), 2);
    }

    #[test]
    fn test_parse_skips_control_frames() {
        let payload = r#"[{"T":"success","msg":"authenticated"}]"#;
        assert!(parse_quotes(payload).is_empty());
    }

    #[test]
    fn test_parse_skips_error_frames() {
        // Error 405 is the symbol-limit rejection; it must not panic or parse
        // as a quote.
        let payload = r#"[{"T":"error","code":405,"msg":"symbol limit exceeded"}]"#;
        assert!(parse_quotes(payload).is_empty());
    }

    #[test]
    fn test_parse_skips_trade_frames() {
        let payload = r#"[{"T":"t","S":"AAPL","p":180.0,"s":100,"t":"2023-04-06T11:54:21Z"}]"#;
        assert!(parse_quotes(payload).is_empty());
    }

    #[test]
    fn test_parse_drops_quote_missing_ask() {
        // Defaulting a missing ask to zero would produce a nonsense spread, so
        // the quote is dropped instead.
        let payload = r#"[{"T":"q","S":"AAPL","bp":180.0,"bs":1,"t":"2023-04-06T11:54:21Z"}]"#;
        assert!(parse_quotes(payload).is_empty());
    }

    #[test]
    fn test_parse_drops_quote_with_invalid_ticker() {
        let payload =
            r#"[{"T":"q","S":"","bp":180.0,"bs":1,"ap":180.1,"as":1,"t":"2023-04-06T11:54:21Z"}]"#;
        assert!(parse_quotes(payload).is_empty());
    }

    #[test]
    fn test_parse_keeps_valid_quotes_alongside_invalid_ones() {
        let payload = r#"[
            {"T":"q","S":"AAPL","bp":180.0,"bs":1,"t":"2023-04-06T11:54:21Z"},
            {"T":"q","S":"MSFT","bp":300.0,"bs":1,"ap":300.2,"as":1,"t":"2023-04-06T11:54:22Z"}
        ]"#;
        let quotes = parse_quotes(payload);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker().as_str(), "MSFT");
    }

    #[test]
    fn test_parse_malformed_payload_returns_empty() {
        assert!(parse_quotes("not json").is_empty());
    }

    #[test]
    fn test_parse_defaults_missing_sizes_to_zero() {
        // Sizes do not feed the mid-price, so their absence is tolerable where a
        // missing price is not.
        let payload = r#"[{"T":"q","S":"AAPL","bp":180.0,"ap":180.1,"t":"2023-04-06T11:54:21Z"}]"#;
        let quotes = parse_quotes(payload);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].bid_size(), 0);
    }
}
