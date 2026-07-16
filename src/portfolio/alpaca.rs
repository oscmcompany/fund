//! Alpaca trading REST API client for the portfolio module.
//!
//! Uses `reqwest` for HTTP and the Alpaca v2 API endpoints for order
//! submission, position management, and shortability checks. Authentication
//! uses the `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` headers.

use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::common::alpaca::AlpacaCredentials;

/// Base URL for paper trading (sandbox environment).
const PAPER_BASE_URL: &str = "https://paper-api.alpaca.markets";

/// Base URL for live trading.
const LIVE_BASE_URL: &str = "https://api.alpaca.markets";

/// Base URL for the Alpaca market data API (quotes, snapshots).
const DATA_BASE_URL: &str = "https://data.alpaca.markets";

/// Header name for the Alpaca API key ID.
const HEADER_KEY_ID: &str = "APCA-API-KEY-ID";

/// Header name for the Alpaca API secret key.
const HEADER_SECRET_KEY: &str = "APCA-API-SECRET-KEY";

/// Account information returned by the Alpaca account endpoint.
///
/// # Margin account terminology
///
/// - **Equity**: net account value (`cash + long_market_value + short_market_value`).
///   This is what you actually own. Equivalent to `portfolio_value` for margin accounts.
/// - **Cash**: settled cash balance. Increases on sells/shorts, decreases on buys.
///   Not the same as "available to spend" — margin requirements are separate.
/// - **Buying power**: remaining capacity to open new positions given current margin
///   usage. With a 4x multiplier: `4 * (equity - initial_margin)`. Shrinks as
///   positions consume margin; starts at `4 * equity` when fully cash.
/// - **Long market value**: current market value of all long positions.
/// - **Short market value**: current market value of all short positions (negative).
/// - **Initial margin**: margin required to hold current positions under Reg T.
/// - **Maintenance margin**: minimum equity before a margin call triggers forced
///   liquidation.
#[derive(Debug, Clone)]
pub struct AccountInfo {
    /// Cash available (uninvested).
    pub cash_amount: f64,
    /// Total buying power, including margin.
    pub buying_power: f64,
    /// Total equity value of the account.
    pub equity: f64,
}

/// A position currently held in the Alpaca account.
#[derive(Debug, Clone)]
pub struct Position {
    /// Ticker symbol.
    pub symbol: String,
    /// Position side (`"long"` or `"short"`).
    pub side: String,
    /// Current market value (negative for shorts).
    pub market_value: f64,
    /// Unrealised profit or loss on this position.
    pub unrealized_profit_and_loss: f64,
}

/// Latest quote snapshot for a single symbol from the Alpaca data API.
#[derive(Debug, Clone)]
pub struct LatestQuote {
    /// Ticker symbol.
    pub symbol: String,
    /// Mid price computed as `(bid + ask) / 2`.
    pub mid_price: f64,
}

/// The fill information for a submitted order.
#[derive(Debug, Clone)]
pub struct OrderFill {
    /// Alpaca-assigned order identifier.
    pub alpaca_order_id: String,
    /// Current order status (e.g. `"filled"`, `"new"`, `"rejected"`).
    pub status: String,
    /// Number of shares filled. `None` when the order has not yet been filled.
    pub filled_quantity: Option<f64>,
    /// Average fill price. `None` when the order has not yet been filled.
    pub fill_price: Option<f64>,
}

/// Error returned by the Alpaca trading client.
#[derive(Debug)]
pub enum ClientError {
    /// The HTTP request itself failed.
    Request(reqwest::Error),
    /// The API returned an unexpected or error response.
    Api { status: u16, body: String },
    /// The API response body could not be parsed.
    Parse(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Request(error) => write!(formatter, "Request error: {error}"),
            ClientError::Api { status, body } => {
                write!(formatter, "API error {status}: {body}")
            }
            ClientError::Parse(message) => write!(formatter, "Parse error: {message}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<reqwest::Error> for ClientError {
    fn from(error: reqwest::Error) -> Self {
        ClientError::Request(error)
    }
}

/// Alpaca trading REST client.
#[derive(Clone)]
pub struct TradingClient {
    http_client: Client,
    credentials: AlpacaCredentials,
    base_url: String,
    data_base_url: String,
}

impl TradingClient {
    /// Constructs an `TradingClient`.
    ///
    /// When `is_paper` is `true`, requests go to the paper trading sandbox.
    pub fn new(credentials: AlpacaCredentials, is_paper: bool) -> Self {
        let base_url = if is_paper {
            PAPER_BASE_URL.to_string()
        } else {
            LIVE_BASE_URL.to_string()
        };
        Self {
            http_client: Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build Alpaca HTTP client"),
            credentials,
            base_url,
            data_base_url: DATA_BASE_URL.to_string(),
        }
    }

    /// Constructs an `TradingClient` from a base URL (for testing).
    pub fn with_base_url(credentials: AlpacaCredentials, base_url: String) -> Self {
        Self {
            http_client: Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build Alpaca HTTP client"),
            credentials,
            data_base_url: base_url.clone(),
            base_url,
        }
    }

    #[cfg(test)]
    fn auth_headers(&self) -> [(&'static str, String); 2] {
        [
            (HEADER_KEY_ID, self.credentials.key_id().to_string()),
            (HEADER_SECRET_KEY, self.credentials.secret().to_string()),
        ]
    }

    /// Returns account details (cash, buying power, equity).
    pub async fn get_account(&self) -> Result<AccountInfo, ClientError> {
        let url = format!("{}/v2/account", self.base_url);
        let response = self
            .http_client
            .get(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let account: AccountResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse account response: {error}"))
        })?;

        let cash_amount = account
            .cash
            .parse::<f64>()
            .map_err(|error| ClientError::Parse(format!("Failed to parse cash amount: {error}")))?;
        let buying_power = account.buying_power.parse::<f64>().map_err(|error| {
            ClientError::Parse(format!("Failed to parse buying power: {error}"))
        })?;
        let equity = account
            .equity
            .parse::<f64>()
            .map_err(|error| ClientError::Parse(format!("Failed to parse equity: {error}")))?;

        Ok(AccountInfo {
            cash_amount,
            buying_power,
            equity,
        })
    }

    /// Submits a market buy order using notional dollar amount.
    ///
    /// Returns the Alpaca order ID on success.
    pub async fn submit_long_order(
        &self,
        ticker: &str,
        notional: f64,
    ) -> Result<String, ClientError> {
        let url = format!("{}/v2/orders", self.base_url);
        let body = OrderRequest {
            symbol: ticker.to_ascii_uppercase(),
            side: "buy".to_string(),
            order_type: "market".to_string(),
            time_in_force: "day".to_string(),
            notional: Some(format!("{notional:.2}")),
            qty: None,
            position_intent: Some("buy_to_open".to_string()),
        };

        let response = self
            .http_client
            .post(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .json(&body)
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let order: OrderResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse order response: {error}"))
        })?;

        info!(
            ticker = ticker,
            alpaca_order_id = order.id,
            "Long order submitted"
        );
        Ok(order.id)
    }

    /// Submits a market sell (short) order using a whole-share quantity.
    ///
    /// Returns the Alpaca order ID on success.
    pub async fn submit_short_order(
        &self,
        ticker: &str,
        quantity: i64,
    ) -> Result<String, ClientError> {
        let url = format!("{}/v2/orders", self.base_url);
        let body = OrderRequest {
            symbol: ticker.to_ascii_uppercase(),
            side: "sell".to_string(),
            order_type: "market".to_string(),
            time_in_force: "day".to_string(),
            notional: None,
            qty: Some(quantity),
            position_intent: Some("sell_to_open".to_string()),
        };

        let response = self
            .http_client
            .post(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .json(&body)
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let order: OrderResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse order response: {error}"))
        })?;

        info!(
            ticker = ticker,
            alpaca_order_id = order.id,
            "Short order submitted"
        );
        Ok(order.id)
    }

    /// Retrieves the current fill status of an order by its Alpaca order ID.
    pub async fn get_order(&self, alpaca_order_id: &str) -> Result<OrderFill, ClientError> {
        let url = format!("{}/v2/orders/{alpaca_order_id}", self.base_url);
        let response = self
            .http_client
            .get(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let order: OrderResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse order response: {error}"))
        })?;

        Ok(OrderFill {
            alpaca_order_id: order.id,
            status: order.status,
            filled_quantity: order.filled_qty.as_deref().and_then(|qty| qty.parse().ok()),
            fill_price: order
                .filled_avg_price
                .as_deref()
                .and_then(|price| price.parse().ok()),
        })
    }

    /// Closes the full position for `ticker`.
    ///
    /// Returns `true` when the position was closed, `false` when the position
    /// did not exist (404). Propagates other errors.
    pub async fn close_position(&self, ticker: &str) -> Result<bool, ClientError> {
        let url = format!(
            "{}/v2/positions/{}?percentage=100",
            self.base_url,
            ticker.to_ascii_uppercase()
        );
        let response = self
            .http_client
            .delete(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        if response.status().as_u16() == 404 {
            info!(ticker = ticker, "Position not found; already closed");
            return Ok(false);
        }

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        info!(ticker = ticker, "Position close submitted");
        Ok(true)
    }

    /// Fetches all active US equity assets and partitions them into tradable and
    /// shortable sets.
    ///
    /// The **tradable** set contains every active asset with `tradable = true`
    /// (eligible for buy orders on the long leg). The **shortable** subset further
    /// requires `shortable = true` and `easy_to_borrow = true` (eligible for
    /// sell-short orders on the short leg).
    ///
    /// Alpaca asset reference: <https://docs.alpaca.markets/us/reference/get-v2-assets-1>
    ///
    /// Callers should cache the result for the duration of a trading session
    /// rather than calling it on every rebalance cycle.
    pub async fn fetch_tradable_assets(&self) -> Result<TradableAssets, ClientError> {
        let url = format!(
            "{}/v2/assets?status=active&asset_class=us_equity",
            self.base_url
        );
        let response = self
            .http_client
            .get(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let assets: Vec<AssetResponse> = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse assets response: {error}"))
        })?;

        let mut tradable = std::collections::HashSet::new();
        let mut shortable = std::collections::HashSet::new();

        for asset in assets {
            if asset.tradable.unwrap_or(false) && asset.fractionable.unwrap_or(false) {
                tradable.insert(asset.symbol.clone());
                if asset.shortable.unwrap_or(false) && asset.easy_to_borrow.unwrap_or(false) {
                    shortable.insert(asset.symbol);
                }
            }
        }

        info!(
            tradable = tradable.len(),
            shortable = shortable.len(),
            "Tradable asset universe fetched"
        );

        Ok(TradableAssets {
            tradable,
            shortable,
        })
    }

    /// Attempts to cancel an open order by its Alpaca order ID.
    ///
    /// Returns `Ok(true)` when the order was cancelled, `Ok(false)` when Alpaca
    /// returns 422 (the order is already in a terminal state and cannot be
    /// cancelled), and `Err` for all other failures.
    pub async fn cancel_order(&self, alpaca_order_id: &str) -> Result<bool, ClientError> {
        let url = format!("{}/v2/orders/{alpaca_order_id}", self.base_url);
        let response = self
            .http_client
            .delete(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        if response.status().as_u16() == 422 {
            info!(
                alpaca_order_id = alpaca_order_id,
                "Order already in terminal state; cannot cancel"
            );
            return Ok(false);
        }

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        info!(alpaca_order_id = alpaca_order_id, "Order cancelled");
        Ok(true)
    }

    /// Returns whether the market is currently open.
    pub async fn is_market_open(&self) -> Result<bool, ClientError> {
        let url = format!("{}/v2/clock", self.base_url);
        let response = self
            .http_client
            .get(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let clock: ClockResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse clock response: {error}"))
        })?;

        Ok(clock.is_open)
    }

    /// Fetches all open positions from the Alpaca account.
    pub async fn fetch_positions(&self) -> Result<Vec<Position>, ClientError> {
        let url = format!("{}/v2/positions", self.base_url);
        let response = self
            .http_client
            .get(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let positions: Vec<PositionResponse> = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse positions response: {error}"))
        })?;

        let result: Vec<Position> = positions
            .into_iter()
            .filter_map(|position| {
                let market_value = position.market_value.parse::<f64>().ok()?;
                let unrealized_profit_and_loss = position.unrealized_pl.parse::<f64>().ok()?;
                Some(Position {
                    symbol: position.symbol,
                    side: position.side,
                    market_value,
                    unrealized_profit_and_loss,
                })
            })
            .collect();

        info!(positions = result.len(), "Alpaca positions fetched");
        Ok(result)
    }

    /// Fetches latest quote snapshots for the given symbols from the Alpaca data API.
    ///
    /// Returns a map from ticker to mid price `(bid + ask) / 2`. Symbols without
    /// a valid quote are omitted from the result.
    pub async fn fetch_latest_quotes(
        &self,
        symbols: &[String],
    ) -> Result<Vec<LatestQuote>, ClientError> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let symbols_param = symbols.join(",");
        let url = format!(
            "{}/v2/stocks/snapshots?symbols={}&feed=iex",
            self.data_base_url, symbols_param
        );
        let response = self
            .http_client
            .get(&url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
            .send()
            .await?;

        let status = response.status().as_u16();
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        let snapshots: std::collections::HashMap<String, SnapshotResponse> =
            response.json().await.map_err(|error| {
                ClientError::Parse(format!("Failed to parse snapshots response: {error}"))
            })?;

        let quotes: Vec<LatestQuote> = snapshots
            .into_iter()
            .filter_map(|(symbol, snapshot)| {
                let quote = snapshot.latest_quote?;
                let bid = quote.bp?;
                let ask = quote.ap?;
                if bid <= 0.0 || ask <= 0.0 {
                    return None;
                }
                Some(LatestQuote {
                    symbol,
                    mid_price: (bid + ask) / 2.0,
                })
            })
            .collect();

        info!(
            requested = symbols.len(),
            returned = quotes.len(),
            "Latest quotes fetched from Alpaca data API"
        );
        Ok(quotes)
    }
}

/// Partitioned view of the Alpaca active asset universe.
///
/// `tradable` contains all active US equities that accept buy orders.
/// `shortable` is a subset that also accepts sell-short orders
/// (`shortable = true`, `easy_to_borrow = true`).
#[derive(Debug, Clone)]
pub struct TradableAssets {
    tradable: std::collections::HashSet<String>,
    shortable: std::collections::HashSet<String>,
}

impl TradableAssets {
    /// Returns `true` when the symbol can be bought (long leg eligibility).
    pub fn is_tradable(&self, symbol: &str) -> bool {
        self.tradable.contains(symbol)
    }

    /// Returns `true` when the symbol can be sold short (short leg eligibility).
    pub fn is_shortable(&self, symbol: &str) -> bool {
        self.shortable.contains(symbol)
    }

    /// Number of tradable symbols.
    pub fn tradable_count(&self) -> usize {
        self.tradable.len()
    }

    /// Number of shortable symbols.
    pub fn shortable_count(&self) -> usize {
        self.shortable.len()
    }
}

// ---------- API response structs ----------

#[derive(Deserialize)]
struct AccountResponse {
    cash: String,
    buying_power: String,
    equity: String,
}

#[derive(Serialize)]
struct OrderRequest {
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    time_in_force: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    notional: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    qty: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position_intent: Option<String>,
}

#[derive(Deserialize)]
struct OrderResponse {
    id: String,
    status: String,
    filled_qty: Option<String>,
    filled_avg_price: Option<String>,
}

#[derive(Deserialize)]
struct AssetResponse {
    symbol: String,
    tradable: Option<bool>,
    fractionable: Option<bool>,
    shortable: Option<bool>,
    easy_to_borrow: Option<bool>,
}

#[derive(Deserialize)]
struct ClockResponse {
    is_open: bool,
}

#[derive(Deserialize)]
struct PositionResponse {
    symbol: String,
    side: String,
    market_value: String,
    unrealized_pl: String,
}

#[derive(Deserialize)]
struct SnapshotResponse {
    #[serde(rename = "latestQuote")]
    latest_quote: Option<SnapshotQuote>,
}

#[derive(Deserialize)]
struct SnapshotQuote {
    bp: Option<f64>,
    ap: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    fn make_credentials() -> AlpacaCredentials {
        AlpacaCredentials::new("test-key".to_string(), "test-secret".to_string()).unwrap()
    }

    #[test]
    fn test_alpaca_error_display_request() {
        // Build a reqwest error via a deliberately bad URL.
        let error = ClientError::Api {
            status: 500,
            body: "internal server error".to_string(),
        };
        let message = format!("{error}");
        assert!(message.contains("500"));
        assert!(message.contains("internal server error"));
    }

    #[test]
    fn test_alpaca_error_display_parse() {
        let error = ClientError::Parse("unexpected field".to_string());
        assert!(format!("{error}").contains("unexpected field"));
    }

    #[tokio::test]
    async fn test_get_account_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/account")
            .with_status(200)
            .with_body(r#"{"cash": "10000.00", "buying_power": "20000.00", "equity": "30000.00"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let account = client.get_account().await.unwrap();

        assert!((account.cash_amount - 10_000.0).abs() < 1e-6);
        assert!((account.buying_power - 20_000.0).abs() < 1e-6);
        assert!((account.equity - 30_000.0).abs() < 1e-6);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_account_api_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/account")
            .with_status(401)
            .with_body(r#"{"message": "Unauthorized"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let result = client.get_account().await;

        assert!(matches!(result, Err(ClientError::Api { status: 401, .. })));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_submit_long_order_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/v2/orders")
            .with_status(200)
            .with_body(r#"{"id": "order-123", "status": "new", "filled_qty": null, "filled_avg_price": null}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let order_id = client.submit_long_order("AAPL", 10_000.0).await.unwrap();

        assert_eq!(order_id, "order-123");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_submit_short_order_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/v2/orders")
            .with_status(200)
            .with_body(r#"{"id": "order-456", "status": "new", "filled_qty": null, "filled_avg_price": null}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let order_id = client.submit_short_order("MSFT", 50).await.unwrap();

        assert_eq!(order_id, "order-456");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_order_filled() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/orders/order-123")
            .with_status(200)
            .with_body(r#"{"id": "order-123", "status": "filled", "filled_qty": "100.0", "filled_avg_price": "150.25"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let fill = client.get_order("order-123").await.unwrap();

        assert_eq!(fill.status, "filled");
        assert!((fill.filled_quantity.unwrap() - 100.0).abs() < 1e-6);
        assert!((fill.fill_price.unwrap() - 150.25).abs() < 1e-6);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_close_position_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/positions/AAPL?percentage=100")
            .with_status(200)
            .with_body(r#"{"id": "order-789", "status": "pending_cancel"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let closed = client.close_position("AAPL").await.unwrap();

        assert!(closed);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_close_position_not_found() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/positions/AAPL?percentage=100")
            .with_status(404)
            .with_body(r#"{"message": "position not found"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let closed = client.close_position("AAPL").await.unwrap();

        assert!(!closed);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_tradable_assets_partitions_correctly() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/assets?status=active&asset_class=us_equity")
            .with_status(200)
            .with_body(
                r#"[
                {"symbol": "AAPL", "tradable": true,  "fractionable": true,  "shortable": true,  "easy_to_borrow": true},
                {"symbol": "MSFT", "tradable": true,  "fractionable": true,  "shortable": false, "easy_to_borrow": true},
                {"symbol": "GOOG", "tradable": true,  "fractionable": true,  "shortable": true,  "easy_to_borrow": false},
                {"symbol": "NVDA", "tradable": true,  "fractionable": true,  "shortable": true,  "easy_to_borrow": true},
                {"symbol": "AMZN", "tradable": true,  "fractionable": false, "shortable": true,  "easy_to_borrow": true},
                {"symbol": "META", "tradable": false, "fractionable": true,  "shortable": true,  "easy_to_borrow": true}
            ]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let assets = client.fetch_tradable_assets().await.unwrap();

        // Tradable set: tradable + fractionable (long leg eligibility via notional orders).
        assert!(assets.is_tradable("AAPL"));
        assert!(assets.is_tradable("MSFT"));
        assert!(assets.is_tradable("GOOG"));
        assert!(assets.is_tradable("NVDA"));
        assert!(!assets.is_tradable("AMZN")); // not fractionable
        assert!(!assets.is_tradable("META")); // not tradable

        // Shortable set: tradable + fractionable + shortable + easy_to_borrow (short leg eligibility).
        assert!(assets.is_shortable("AAPL"));
        assert!(!assets.is_shortable("MSFT")); // not shortable
        assert!(!assets.is_shortable("GOOG")); // not easy_to_borrow
        assert!(assets.is_shortable("NVDA"));
        assert!(!assets.is_shortable("AMZN")); // not fractionable
        assert!(!assets.is_shortable("META")); // not tradable

        assert_eq!(assets.tradable_count(), 4);
        assert_eq!(assets.shortable_count(), 2);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_tradable_assets_api_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/assets?status=active&asset_class=us_equity")
            .with_status(401)
            .with_body(r#"{"message": "Unauthorized"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let result = client.fetch_tradable_assets().await;

        assert!(matches!(result, Err(ClientError::Api { status: 401, .. })));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_is_market_open_true() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/clock")
            .with_status(200)
            .with_body(r#"{"is_open": true, "next_open": "2026-01-01T09:30:00Z", "next_close": "2026-01-01T16:00:00Z"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        assert!(client.is_market_open().await.unwrap());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_is_market_open_false() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/clock")
            .with_status(200)
            .with_body(r#"{"is_open": false, "next_open": "2026-01-02T09:30:00Z", "next_close": "2026-01-02T16:00:00Z"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        assert!(!client.is_market_open().await.unwrap());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_cancel_order_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/orders/order-123")
            .with_status(204)
            .with_body("")
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let cancelled = client.cancel_order("order-123").await.unwrap();

        assert!(cancelled);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_cancel_order_already_terminal() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/orders/order-456")
            .with_status(422)
            .with_body(r#"{"message": "order is not cancellable"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let cancelled = client.cancel_order("order-456").await.unwrap();

        assert!(!cancelled);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_cancel_order_api_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/orders/order-789")
            .with_status(403)
            .with_body(r#"{"message": "forbidden"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let result = client.cancel_order("order-789").await;

        assert!(matches!(result, Err(ClientError::Api { status: 403, .. })));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_positions_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/positions")
            .with_status(200)
            .with_body(
                r#"[
                {"symbol": "AAPL", "side": "long", "market_value": "15000.50", "unrealized_pl": "500.25"},
                {"symbol": "MSFT", "side": "short", "market_value": "-8000.00", "unrealized_pl": "-200.75"}
            ]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let positions = client.fetch_positions().await.unwrap();

        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].symbol, "AAPL");
        assert_eq!(positions[0].side, "long");
        assert!((positions[0].market_value - 15_000.50).abs() < 1e-6);
        assert!((positions[0].unrealized_profit_and_loss - 500.25).abs() < 1e-6);
        assert_eq!(positions[1].symbol, "MSFT");
        assert_eq!(positions[1].side, "short");
        assert!((positions[1].market_value - (-8_000.0)).abs() < 1e-6);
        assert!((positions[1].unrealized_profit_and_loss - (-200.75)).abs() < 1e-6);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_positions_empty() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/positions")
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let positions = client.fetch_positions().await.unwrap();

        assert!(positions.is_empty());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_positions_api_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/positions")
            .with_status(401)
            .with_body(r#"{"message": "Unauthorized"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let result = client.fetch_positions().await;

        assert!(matches!(result, Err(ClientError::Api { status: 401, .. })));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_latest_quotes_success() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/snapshots?symbols=AAPL,MSFT&feed=iex")
            .with_status(200)
            .with_body(
                r#"{
                "AAPL": {"latestQuote": {"bp": 150.00, "ap": 150.20}},
                "MSFT": {"latestQuote": {"bp": 420.00, "ap": 420.40}}
            }"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let symbols = vec!["AAPL".to_string(), "MSFT".to_string()];
        let mut quotes = client.fetch_latest_quotes(&symbols).await.unwrap();

        assert_eq!(quotes.len(), 2);
        quotes.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        assert_eq!(quotes[0].symbol, "AAPL");
        assert!((quotes[0].mid_price - 150.10).abs() < 1e-6);
        assert_eq!(quotes[1].symbol, "MSFT");
        assert!((quotes[1].mid_price - 420.20).abs() < 1e-6);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_latest_quotes_empty_symbols() {
        let client = TradingClient::with_base_url(
            make_credentials(),
            "http://should-not-be-called".to_string(),
        );
        let quotes = client.fetch_latest_quotes(&[]).await.unwrap();

        assert!(quotes.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_latest_quotes_api_error() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/snapshots?symbols=AAPL&feed=iex")
            .with_status(500)
            .with_body(r#"{"message": "Internal Server Error"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let symbols = vec!["AAPL".to_string()];
        let result = client.fetch_latest_quotes(&symbols).await;

        assert!(matches!(result, Err(ClientError::Api { status: 500, .. })));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_latest_quotes_skips_missing_quote() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/snapshots?symbols=AAPL,MSFT&feed=iex")
            .with_status(200)
            .with_body(
                r#"{
                "AAPL": {"latestQuote": {"bp": 150.00, "ap": 150.20}},
                "MSFT": {"latestQuote": null}
            }"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(make_credentials(), server.url());
        let symbols = vec!["AAPL".to_string(), "MSFT".to_string()];
        let quotes = client.fetch_latest_quotes(&symbols).await.unwrap();

        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].symbol, "AAPL");
        assert!((quotes[0].mid_price - 150.10).abs() < 1e-6);
        mock.assert_async().await;
    }

    #[test]
    fn test_new_uses_paper_url_when_paper_is_true() {
        let client = TradingClient::new(make_credentials(), true);
        assert!(client.base_url.contains("paper-api"));
    }

    #[test]
    fn test_new_uses_live_url_when_paper_is_false() {
        let client = TradingClient::new(make_credentials(), false);
        assert!(!client.base_url.contains("paper-api"));
        assert!(client.base_url.contains("api.alpaca.markets"));
    }

    #[test]
    fn test_auth_headers_returns_key_and_secret() {
        let client = TradingClient::new(make_credentials(), true);
        let headers = client.auth_headers();
        assert_eq!(headers[0].0, HEADER_KEY_ID);
        assert_eq!(headers[0].1, "test-key");
        assert_eq!(headers[1].0, HEADER_SECRET_KEY);
        assert_eq!(headers[1].1, "test-secret");
    }
}
