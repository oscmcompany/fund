use crate::common::events::EventType;
use crate::data_manager::data::EquityQuote;
use crate::data_manager::database;
use crate::data_manager::state::State;
use crate::domain::market::Ticker;
use chrono::DateTime;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use sqlx::postgres::PgListener;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

const FLUSH_INTERVAL_SECS: u64 = 5;
const FLUSH_BATCH_SIZE: usize = 1000;
const ALPACA_WS_BASE_URL: &str = "wss://stream.data.alpaca.markets/v2";

// Market session check interval mirrors the pg_cron schedule in schema.sql (market-session-check job).
// 5 minutes is a conservative starting point; tighten to 60 if signal latency requires it.
// The flush interval must never exceed this value — quotes must be committed to the database
// before portfolio-manager queries them in response to a market_session_check event.
const MARKET_SESSION_CHECK_INTERVAL_SECS: u64 = 5 * 60;
const _: () = assert!(
    FLUSH_INTERVAL_SECS <= MARKET_SESSION_CHECK_INTERVAL_SECS,
    "FLUSH_INTERVAL_SECS must not exceed MARKET_SESSION_CHECK_INTERVAL_SECS"
);

pub fn spawn_quote_stream(state: State) {
    if state.database.pool().is_none() {
        info!("PostgreSQL not available, quote stream disabled");
        return;
    }
    if state.alpaca_credentials.is_none() {
        info!("Alpaca credentials not configured, quote stream disabled");
        return;
    }
    tokio::spawn(quote_stream_supervisor(state));
}

async fn quote_stream_supervisor(state: State) {
    loop {
        match run_quote_stream(&state).await {
            Ok(()) => {
                info!("Quote stream exited cleanly, reconnecting in 5s");
                sleep(Duration::from_secs(5)).await;
            }
            Err(error) => {
                warn!("Quote stream error: {}, restarting in 30s", error);
                sleep(Duration::from_secs(30)).await;
            }
        }
    }
}

async fn refresh_active_symbols(state: &State, pool: &sqlx::PgPool) {
    match database::get_active_tickers(pool).await {
        Ok(tickers) => {
            let symbol_set: HashSet<Ticker> = tickers.into_iter().collect();
            info!("Refreshed active symbols, count: {}", symbol_set.len());
            let mut guard = state.active_symbols.write().await;
            *guard = symbol_set;
        }
        Err(error) => warn!("Failed to refresh active symbols: {}", error),
    }
}

async fn run_quote_stream(state: &State) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pool = state
        .database
        .pool()
        .ok_or("database pool not initialized")?;
    let credentials = state
        .alpaca_credentials
        .as_ref()
        .ok_or("Alpaca credentials not configured")?;

    refresh_active_symbols(state, pool).await;

    let url = format!("{}/{}", ALPACA_WS_BASE_URL, credentials.feed());
    info!(
        "Connecting to Alpaca quote stream, feed: {}",
        credentials.feed()
    );

    let (ws_stream, _) = connect_async(&url).await?;
    let (mut write, mut read) = ws_stream.split();

    // Wait for the initial "connected" control message
    if let Some(Ok(Message::Text(text))) = read.next().await {
        let messages: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap_or_default();
        for message in &messages {
            if message.get("T").and_then(|t| t.as_str()) == Some("connected") {
                info!("Alpaca WebSocket connected");
            }
        }
    }

    // Authenticate
    let auth_json = serde_json::json!({
        "action": "auth",
        "key": credentials.key_id(),
        "secret": credentials.secret(),
    })
    .to_string();
    write.send(Message::Text(auth_json.into())).await?;

    // Wait for auth response
    let mut authenticated = false;
    if let Some(Ok(Message::Text(text))) = read.next().await {
        let messages: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap_or_default();
        for message in &messages {
            match message.get("T").and_then(|t| t.as_str()) {
                Some("error") => {
                    let code = message.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
                    let error_message = message
                        .get("msg")
                        .and_then(|m| m.as_str())
                        .unwrap_or("unknown");
                    return Err(format!("Alpaca auth error {}: {}", code, error_message).into());
                }
                Some("success") => {
                    info!("Alpaca authentication successful");
                    authenticated = true;
                }
                _ => {}
            }
        }
    }
    if !authenticated {
        return Err("Alpaca authentication response not received".into());
    }

    // Subscribe to current active symbols
    {
        let symbols: Vec<Ticker> = state.active_symbols.read().await.iter().cloned().collect();
        if !symbols.is_empty() {
            let subscribe_json = serde_json::json!({
                "action": "subscribe",
                "quotes": symbols,
            })
            .to_string();
            write.send(Message::Text(subscribe_json.into())).await?;
            info!("Subscribed to {} symbol(s)", symbols.len());
        } else {
            info!("No active symbols, skipping initial subscription");
        }
    }

    let mut pg_listener = PgListener::connect_with(pool).await?;
    pg_listener.listen("events").await?;
    info!("Listening on PostgreSQL events channel for rebalance events");

    let mut quote_buffer: Vec<EquityQuote> = Vec::new();
    let mut flush_timer = interval(Duration::from_secs(FLUSH_INTERVAL_SECS));
    flush_timer.tick().await; // consume first immediate tick

    loop {
        tokio::select! {
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let parsed = parse_quote_messages(&text);
                        quote_buffer.extend(parsed);
                        if quote_buffer.len() >= FLUSH_BATCH_SIZE {
                            flush_quotes(pool, &mut quote_buffer).await;
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("Alpaca WebSocket closed by server");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => {
                        info!("Alpaca WebSocket stream ended");
                        break;
                    }
                }
            }
            notification = pg_listener.recv() => {
                match notification {
                    Ok(notification) => {
                        let parsed: serde_json::Value =
                            match serde_json::from_str(notification.payload()) {
                                Ok(value) => value,
                                Err(_) => continue,
                            };
                        let portfolio_rebalance_completed =
                            EventType::PortfolioRebalanceCompleted.as_str();
                        let portfolio_liquidation_completed =
                            EventType::PortfolioLiquidationCompleted.as_str();
                        match parsed.get("event_type").and_then(|v| v.as_str()) {
                            Some(event_type)
                                if event_type == portfolio_rebalance_completed =>
                            {
                                info!("Received portfolio_rebalance_completed, refreshing quote subscriptions");
                                refresh_active_symbols(state, pool).await;

                                // Unsubscribe from all, then resubscribe with the updated symbol set
                                let unsubscribe_json = serde_json::json!({
                                    "action": "unsubscribe",
                                    "quotes": ["*"],
                                })
                                .to_string();
                                write.send(Message::Text(unsubscribe_json.into())).await?;

                                let symbols: Vec<Ticker> =
                                    state.active_symbols.read().await.iter().cloned().collect();
                                if !symbols.is_empty() {
                                    let subscribe_json = serde_json::json!({
                                        "action": "subscribe",
                                        "quotes": symbols,
                                    })
                                    .to_string();
                                    write.send(Message::Text(subscribe_json.into())).await?;
                                    info!("Resubscribed to {} symbol(s)", symbols.len());
                                }
                            }
                            Some(event_type)
                                if event_type == portfolio_liquidation_completed =>
                            {
                                info!("Received portfolio_liquidation_completed, unsubscribing from all symbols");
                                let unsubscribe_json = serde_json::json!({
                                    "action": "unsubscribe",
                                    "quotes": ["*"],
                                })
                                .to_string();
                                write.send(Message::Text(unsubscribe_json.into())).await?;
                                let mut guard = state.active_symbols.write().await;
                                *guard = HashSet::new();
                                info!("Unsubscribed from all symbols for end of day");
                            }
                            _ => {}
                        }
                    }
                    Err(error) => {
                        warn!("PostgreSQL events listener error: {}", error);
                        return Err(error.into());
                    }
                }
            }
            _ = flush_timer.tick() => {
                if !quote_buffer.is_empty() {
                    flush_quotes(pool, &mut quote_buffer).await;
                }
            }
        }
    }

    // Final flush on clean disconnect
    if !quote_buffer.is_empty() {
        flush_quotes(pool, &mut quote_buffer).await;
    }

    Ok(())
}

async fn flush_quotes(pool: &sqlx::PgPool, buffer: &mut Vec<EquityQuote>) {
    let quotes = std::mem::take(buffer);
    let count = quotes.len();
    match database::insert_equity_quotes(pool, &quotes).await {
        Ok(_) => info!("Flushed {} quote(s) to database", count),
        Err(error) => {
            error!("Failed to flush quotes to database: {}", error);
            *buffer = quotes;
        }
    }
}

pub fn parse_quote_messages(text: &str) -> Vec<EquityQuote> {
    let messages: Vec<serde_json::Value> = match serde_json::from_str(text) {
        Ok(messages) => messages,
        Err(_) => return Vec::new(),
    };

    messages
        .into_iter()
        .filter_map(|message| {
            if message.get("T").and_then(|t| t.as_str()) != Some("q") {
                return None;
            }

            let ticker = message
                .get("S")
                .and_then(|s| s.as_str())
                .and_then(Ticker::new)?;
            let bid_price = message.get("bp").and_then(|v| v.as_f64())?;
            let ask_price = message.get("ap").and_then(|v| v.as_f64())?;
            let bid_size_raw = message.get("bs").and_then(|v| v.as_i64())?;
            let ask_size_raw = message.get("as").and_then(|v| v.as_i64())?;
            if bid_size_raw < 0 || ask_size_raw < 0 {
                return None;
            }
            let bid_size = i32::try_from(bid_size_raw).ok()?;
            let ask_size = i32::try_from(ask_size_raw).ok()?;
            let timestamp_str = message.get("t").and_then(|v| v.as_str())?;
            let timestamp = timestamp_str.parse::<DateTime<Utc>>().ok()?;

            Some(EquityQuote::new(
                timestamp, ticker, bid_price, ask_price, bid_size, ask_size,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{parse_quote_messages, spawn_quote_stream};
    use crate::data_manager::state::{DatabaseState, MassiveSecrets, State};

    #[test]
    fn test_parse_quote_messages_returns_quotes() {
        let text = r#"[
            {"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z","bx":"Q","ax":"C","c":["R"],"z":"C"},
            {"T":"q","S":"MSFT","bp":420.10,"ap":420.20,"bs":2,"as":4,"t":"2026-05-23T14:30:00.001Z","bx":"Q","ax":"C","c":["R"],"z":"C"}
        ]"#;

        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 2);
        assert_eq!(quotes[0].ticker(), "AAPL");
        assert_eq!(quotes[0].bid_price(), 150.50);
        assert_eq!(quotes[0].ask_price(), 150.55);
        assert_eq!(quotes[0].bid_size(), 5);
        assert_eq!(quotes[0].ask_size(), 3);
        assert_eq!(quotes[1].ticker(), "MSFT");
    }

    #[test]
    fn test_parse_quote_messages_filters_non_quote_types() {
        let text = r#"[
            {"T":"connected","msg":"connected"},
            {"T":"success","msg":"authenticated"},
            {"T":"subscription","quotes":["AAPL"]},
            {"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}
        ]"#;

        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "AAPL");
    }

    #[test]
    fn test_parse_quote_messages_handles_invalid_json() {
        let quotes = parse_quote_messages("not valid json");
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_required_fields() {
        // Missing "ap" (ask_price) and "t" (timestamp)
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"bs":5,"as":3}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_returns_empty_for_empty_array() {
        let quotes = parse_quote_messages("[]");
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_invalid_timestamp() {
        let text =
            r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"not-a-timestamp"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_invalid_ticker() {
        // Ticker with more than 5 base characters is rejected by Ticker::new.
        let text = r#"[{"T":"q","S":"TOOLONG","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_ticker_field() {
        let text =
            r#"[{"T":"q","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_bid_price() {
        let text =
            r#"[{"T":"q","S":"AAPL","ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_bid_size_overflow() {
        // bid_size values that exceed i32::MAX cannot be represented and must be dropped.
        let oversized: i64 = i64::from(i32::MAX) + 1;
        let text = format!(
            r#"[{{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":{},"as":3,"t":"2026-05-23T14:30:00.000Z"}}]"#,
            oversized
        );
        let quotes = parse_quote_messages(&text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_ask_size_overflow() {
        let oversized: i64 = i64::from(i32::MAX) + 1;
        let text = format!(
            r#"[{{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":{},"t":"2026-05-23T14:30:00.000Z"}}]"#,
            oversized
        );
        let quotes = parse_quote_messages(&text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_accepts_class_share_ticker() {
        // BRK.B is a valid Alpaca ticker and must be accepted.
        let text = r#"[{"T":"q","S":"BRK.B","bp":300.00,"ap":300.10,"bs":2,"as":2,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "BRK.B");
    }

    #[test]
    fn test_parse_quote_messages_mixed_valid_and_invalid() {
        // Only the valid quote (AAPL) should be returned; the invalid one (TOOLONG) is dropped.
        let text = r#"[
            {"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"},
            {"T":"q","S":"TOOLONG","bp":200.00,"ap":200.10,"bs":1,"as":1,"t":"2026-05-23T14:30:01.000Z"}
        ]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "AAPL");
    }

    #[test]
    fn test_parse_quote_messages_timestamp_preserves_value() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        let expected = "2026-05-23T14:30:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        assert_eq!(quotes[0].timestamp(), expected);
    }

    #[test]
    fn test_parse_quote_messages_non_array_json_returns_empty() {
        // The Alpaca stream always sends arrays; a non-array object must be ignored.
        let text = r#"{"T":"q","S":"AAPL","bp":150.50}"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_ask_price() {
        let text =
            r#"[{"T":"q","S":"AAPL","bp":150.50,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_bid_size() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_ask_size() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_timestamp_field() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_single_element_array() {
        let text = r#"[{"T":"q","S":"NVDA","bp":800.00,"ap":800.10,"bs":1,"as":2,"t":"2026-05-23T09:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "NVDA");
        assert_eq!(quotes[0].bid_price(), 800.00);
        assert_eq!(quotes[0].ask_price(), 800.10);
        assert_eq!(quotes[0].bid_size(), 1);
        assert_eq!(quotes[0].ask_size(), 2);
    }

    #[test]
    fn test_parse_quote_messages_ticker_field_is_not_string() {
        // If the "S" field is not a string the message is skipped.
        let text = r#"[{"T":"q","S":12345,"bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_empty_ticker_string_skipped() {
        let text = r#"[{"T":"q","S":"","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_type_field_missing_is_filtered() {
        // A message without a "T" field is treated as a non-quote message type.
        let text = r#"[{"S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_ask_price_is_not_number() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":"not-a-number","bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_bid_size_negative_value_is_rejected() {
        // Negative bid_size is invalid domain data; the parser must reject it.
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":-1,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    /// Constructs a minimal `State` suitable for testing `spawn_quote_stream`.
    /// Uses a stub S3 endpoint that refuses connections so no real AWS call occurs.
    async fn make_test_state_with_database(database: DatabaseState) -> State {
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;

        let credentials = Credentials::new("test-key", "test-secret", None, None, "tests");
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(credentials)
            .endpoint_url("http://127.0.0.1:9")
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
            .force_path_style(true)
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let mut state = State::new(
            reqwest::Client::new(),
            MassiveSecrets {
                base: "http://127.0.0.1:1".to_string(),
                key: "test-api-key".to_string(),
            },
            s3_client,
            "test-bucket".to_string(),
        );
        // Override the database field via a reconstructed state with the given
        // database variant. State::new always sets NotConfigured, so we reach
        // into the public field directly.
        state.database = database;
        state
    }

    #[tokio::test]
    async fn test_spawn_quote_stream_no_database_returns_immediately() {
        // When the database pool is None, spawn_quote_stream logs and returns
        // without spawning a task. The function must complete without panic.
        let state = make_test_state_with_database(DatabaseState::NotConfigured).await;
        // spawn_quote_stream is synchronous and returns immediately when no pool
        // is present. Calling it inside a tokio runtime must not panic.
        spawn_quote_stream(state);
    }

    #[tokio::test]
    async fn test_spawn_quote_stream_connect_failed_returns_immediately() {
        // When the database connection failed (pool is None), spawn_quote_stream
        // exits at the no-pool guard without spawning a WebSocket task.
        let state = make_test_state_with_database(DatabaseState::ConnectFailed).await;
        spawn_quote_stream(state);
    }
}
