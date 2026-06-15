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
const INTRADAY_CHECK_INTERVAL_SECS: u64 = 5 * 60;
const _: () = assert!(
    FLUSH_INTERVAL_SECS <= INTRADAY_CHECK_INTERVAL_SECS,
    "FLUSH_INTERVAL_SECS must not exceed INTRADAY_CHECK_INTERVAL_SECS"
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
            let bid_size = i32::try_from(message.get("bs").and_then(|v| v.as_i64())?).ok()?;
            let ask_size = i32::try_from(message.get("as").and_then(|v| v.as_i64())?).ok()?;
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
    use super::parse_quote_messages;

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
}
