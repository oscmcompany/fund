//! The Alpaca integration: credentials, the trading API, and the market data API.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::common::types::{
    BoundaryReason, Dollars, EquityQuote, EquityTrade, SeriesBoundary, SessionDate, Ticker,
};

const PAPER_BASE_URL: &str = "https://paper-api.alpaca.markets";

const LIVE_BASE_URL: &str = "https://api.alpaca.markets";

const DATA_BASE_URL: &str = "https://data.alpaca.markets";

const HEADER_KEY_ID: &str = "APCA-API-KEY-ID";

const HEADER_SECRET_KEY: &str = "APCA-API-SECRET-KEY";

/// Failures reaching or interpreting an Alpaca endpoint.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The request produced no usable response: connection refused, timed out, TLS failure, or a
    /// body that did not arrive intact.
    #[error("Alpaca request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// Alpaca answered with a non-success status.
    #[error("Alpaca returned status {status}: {body}")]
    Api { status: u16, body: String },
    /// Alpaca answered successfully with something that could not be interpreted.
    #[error("Alpaca response could not be parsed: {0}")]
    Parse(String),
}

impl ClientError {
    /// Whether another attempt could succeed where this one did not.
    ///
    /// A body that did not arrive and a body that is not JSON are one error to `reqwest`, so both
    /// land in [`ClientError::Request`] and both are retried. [`ClientError::Parse`] is what this
    /// module raises about a response it *did* read whole, which repeating cannot change.
    pub fn is_transient(&self) -> bool {
        match self {
            ClientError::Api { status, .. } => *status == 429 || (500..600).contains(status),
            ClientError::Request(_) => true,
            ClientError::Parse(_) => false,
        }
    }
}

/// Pause before a page's next attempt, growing with the attempt number.
fn page_retry_delay(attempt: usize) -> std::time::Duration {
    std::time::Duration::from_millis(250 << attempt.min(4))
}

/// Builds the shared HTTP client.
///
/// The two timeouts answer different questions: `connect_timeout` bounds how long to wait for a
/// connection that may never be established, `timeout` bounds the whole request. A snapshot request
/// carrying a thousand symbols is large but not slow, so thirty seconds is generous.
fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build Alpaca HTTP client")
}

/// Turns a non-success response into a [`ClientError::Api`], reading the body for context.
async fn error_for_status(response: reqwest::Response) -> Result<reqwest::Response, ClientError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Err(ClientError::Api { status, body })
}

/// Why credentials could not be constructed.
///
/// Typed rather than a `String` so a caller can tell an absent variable from an empty one without
/// matching on message text, and so the variable name travels with the error.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CredentialsError {
    #[error("{variable} environment variable is not set")]
    Missing { variable: &'static str },
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
}

/// Alpaca API credentials, shared by both clients.
///
/// Constructed via [`AlpacaCredentials::from_env`] to read from the environment, or via
/// [`AlpacaCredentials::new`] for explicit construction in tests.
#[derive(Clone)]
pub struct AlpacaCredentials {
    key_id: String,
    secret_key: String,
}

impl AlpacaCredentials {
    pub fn new(key_id: String, secret_key: String) -> Result<Self, CredentialsError> {
        if key_id.is_empty() {
            return Err(CredentialsError::Empty { field: "key_id" });
        }
        if secret_key.is_empty() {
            return Err(CredentialsError::Empty {
                field: "secret_key",
            });
        }
        Ok(Self { key_id, secret_key })
    }

    pub fn from_env() -> Result<Self, CredentialsError> {
        let key_id = std::env::var("ALPACA_API_KEY_ID").map_err(|_| CredentialsError::Missing {
            variable: "ALPACA_API_KEY_ID",
        })?;
        let secret_key =
            std::env::var("ALPACA_API_SECRET").map_err(|_| CredentialsError::Missing {
                variable: "ALPACA_API_SECRET",
            })?;
        Self::new(key_id, secret_key)
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }
}

/// One published trading day, with the hours it actually keeps.
///
/// Times are Eastern local, as Alpaca publishes them, so a half-day carries its real 13:00 close
/// rather than an assumed 16:00. This is the only source in the system that knows about half-days:
/// a hardcoded holiday table cannot express a shortened session, and the clock knows the real close
/// only for the session it is currently in.
#[derive(Debug, Clone, PartialEq)]
pub struct CalendarDay {
    session_date: NaiveDate,
    session_open: NaiveTime,
    session_close: NaiveTime,
}

impl CalendarDay {
    pub fn new(
        session_date: NaiveDate,
        session_open: NaiveTime,
        session_close: NaiveTime,
    ) -> Option<Self> {
        if session_close <= session_open {
            return None;
        }
        Some(Self {
            session_date,
            session_open,
            session_close,
        })
    }

    pub fn session_date(&self) -> NaiveDate {
        self.session_date
    }

    pub fn session_open(&self) -> NaiveTime {
        self.session_open
    }

    pub fn session_close(&self) -> NaiveTime {
        self.session_close
    }
}

/// The active US equity universe, partitioned by what can be done with each symbol.
///
/// The **tradable** set is every active asset eligible for a buy order. The **shortable** subset
/// further requires that Alpaca reports the symbol both shortable and easy to borrow, which is what
/// the short leg of a pair needs.
#[derive(Debug, Clone, Default)]
pub struct TradableAssets {
    tradable: HashSet<Ticker>,
    shortable: HashSet<Ticker>,
}

impl TradableAssets {
    pub fn from_sets(tradable: HashSet<Ticker>, shortable: HashSet<Ticker>) -> Self {
        Self {
            tradable,
            shortable,
        }
    }

    pub fn is_tradable(&self, ticker: &Ticker) -> bool {
        self.tradable.contains(ticker)
    }

    pub fn is_shortable(&self, ticker: &Ticker) -> bool {
        self.shortable.contains(ticker)
    }

    pub fn tradable_count(&self) -> usize {
        self.tradable.len()
    }

    pub fn shortable_count(&self) -> usize {
        self.shortable.len()
    }
}

/// REST client for the Alpaca trading API.
#[derive(Clone)]
pub struct TradingClient {
    http_client: reqwest::Client,
    credentials: AlpacaCredentials,
    base_url: String,
}

impl TradingClient {
    /// Constructs a client against either the paper sandbox or live trading.
    pub fn new(credentials: AlpacaCredentials, is_paper: bool) -> Self {
        let base_url = if is_paper {
            PAPER_BASE_URL.to_string()
        } else {
            LIVE_BASE_URL.to_string()
        };
        Self {
            http_client: build_http_client(),
            credentials,
            base_url,
        }
    }

    /// Constructs a client against an explicit base URL, for tests against a mock server.
    pub fn with_base_url(credentials: AlpacaCredentials, base_url: String) -> Self {
        Self {
            http_client: build_http_client(),
            credentials,
            base_url,
        }
    }

    /// Reads the paper/live choice from `ALPACA_IS_PAPER`, defaulting to paper.
    ///
    /// The variable name is fixed by `secretspec.toml` and is not ours to spell differently.
    ///
    /// Defaulting to paper is deliberate, and it is what makes a misspelling here survivable in one
    /// direction only: an unreadable variable sends orders to the sandbox, never to a live account.
    pub fn from_env(credentials: AlpacaCredentials) -> Self {
        let is_paper = std::env::var("ALPACA_IS_PAPER")
            .map(|value| !value.trim().eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        Self::new(credentials, is_paper)
    }

    fn get(&self, url: &str) -> reqwest::RequestBuilder {
        self.http_client
            .get(url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret_key())
    }

    /// Fetches published trading days over an inclusive date range.
    ///
    /// Days Alpaca reports with unusable hours — a close at or before the open — are dropped rather
    /// than kept, since a session with no duration cannot gate anything correctly.
    pub async fn fetch_calendar(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<CalendarDay>, ClientError> {
        let url = format!("{}/v2/calendar", self.base_url);
        let response = error_for_status(
            self.get(&url)
                .query(&[
                    ("start", start.to_string().as_str()),
                    ("end", end.to_string().as_str()),
                ])
                .send()
                .await?,
        )
        .await?;
        let entries: Vec<CalendarResponse> = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse calendar response: {error}"))
        })?;

        let published = entries.len();
        let days: Vec<CalendarDay> = entries
            .into_iter()
            .filter_map(|entry| {
                let session_date = NaiveDate::parse_from_str(&entry.date, "%Y-%m-%d").ok()?;
                let session_open = NaiveTime::parse_from_str(&entry.open, "%H:%M").ok()?;
                let session_close = NaiveTime::parse_from_str(&entry.close, "%H:%M").ok()?;
                CalendarDay::new(session_date, session_open, session_close)
            })
            .collect();

        info!(
            published,
            usable = days.len(),
            start = %start,
            end = %end,
            "Trading calendar fetched"
        );
        Ok(days)
    }

    /// Fetches all active US equity assets, partitioned into tradable and shortable sets.
    ///
    /// Alpaca asset reference: <https://docs.alpaca.markets/us/reference/get-v2-assets-1>
    ///
    /// Callers cache the result for a whole session rather than re-fetching; the universe does not
    /// change intraday.
    pub async fn fetch_tradable_assets(&self) -> Result<TradableAssets, ClientError> {
        let url = format!(
            "{}/v2/assets?status=active&asset_class=us_equity",
            self.base_url
        );
        let response = error_for_status(self.get(&url).send().await?).await?;
        let assets: Vec<AssetResponse> = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse assets response: {error}"))
        })?;

        let mut tradable = HashSet::new();
        let mut shortable = HashSet::new();
        let mut inactive_rejected: usize = 0;
        let mut unparsable_rejected: usize = 0;

        for asset in assets {
            // Checked here as well as in the query string. The `status=active` parameter already
            // filters server-side, so this rejects nothing today -- it exists so a change to the
            // URL cannot silently admit delisted or suspended symbols into the universe.
            if asset.status.as_deref() != Some("active") {
                inactive_rejected += 1;
                continue;
            }
            let Some(ticker) = Ticker::new(&asset.symbol) else {
                unparsable_rejected += 1;
                continue;
            };
            if asset.tradable.unwrap_or(false) {
                if asset.shortable.unwrap_or(false) && asset.easy_to_borrow.unwrap_or(false) {
                    shortable.insert(ticker.clone());
                }
                tradable.insert(ticker);
            }
        }

        info!(
            tradable = tradable.len(),
            shortable = shortable.len(),
            inactive_rejected,
            unparsable_rejected,
            "Tradable asset universe fetched"
        );
        Ok(TradableAssets {
            tradable,
            shortable,
        })
    }
}

#[derive(Deserialize)]
struct CalendarResponse {
    date: String,
    open: String,
    close: String,
}

#[derive(Deserialize)]
struct AssetResponse {
    symbol: String,
    status: Option<String>,
    tradable: Option<bool>,
    shortable: Option<bool>,
    easy_to_borrow: Option<bool>,
}

/// Activities requested per page. 100 is the endpoint's documented maximum.
const ACTIVITIES_PAGE_SIZE: usize = 100;

/// Pages walked before a paginated activity fetch gives up.
///
/// A bound rather than an unbounded loop, because the page token is the previous page's last
/// activity ID and a server that returned a full page of the same rows forever would otherwise
/// spin. At the page size above this covers ten thousand activities in a session, which is two
/// orders of magnitude more than ten pairs opening and closing can produce.
const ACTIVITIES_MAXIMUM_PAGES: usize = 100;

/// Portfolio history resolution. Daily because `account_snapshots` is keyed by session date, and an
/// intraday timeframe would return several points per session with no rule for picking one.
const PORTFOLIO_HISTORY_TIMEFRAME: &str = "1D";

/// Which way an order or fill runs, in Alpaca's vocabulary.
///
/// `SellShort` is a report and never a request: a short leg is submitted as `Sell` carrying a
/// `sell_to_open` position intent, and Alpaca names the resulting fill `sell_short`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderSide {
    Buy,
    Sell,
    SellShort,
}

impl OrderSide {
    pub const ALL: [OrderSide; 3] = [OrderSide::Buy, OrderSide::Sell, OrderSide::SellShort];

    pub fn as_str(self) -> &'static str {
        match self {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
            OrderSide::SellShort => "sell_short",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        OrderSide::ALL.into_iter().find(|side| side.as_str() == raw)
    }
}

impl std::fmt::Display for OrderSide {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for OrderSide {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// What an order is meant to do, carrying the sizing form that side actually accepts.
///
/// The two variants differ in more than direction. A long leg is submitted as a dollar notional and
/// Alpaca fills it fractionally, so both legs of a pair can be sized to the same dollar amount. A
/// short leg cannot be: Alpaca rejects a fractional short, so it is submitted as whole shares.
/// Making the sizing form a property of the variant rather than two nullable fields turns a
/// fractional short from a runtime rejection into a state that cannot be written down.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderIntent {
    /// Buy to open the long leg, sized in dollars.
    OpenLong { ticker: Ticker, notional: Dollars },
    /// Sell to open the short leg, sized in whole shares.
    OpenShort {
        ticker: Ticker,
        quantity: NonZeroU32,
    },
}

impl OrderIntent {
    pub fn ticker(&self) -> &Ticker {
        match self {
            OrderIntent::OpenLong { ticker, .. } | OrderIntent::OpenShort { ticker, .. } => ticker,
        }
    }

    pub fn side(&self) -> OrderSide {
        match self {
            OrderIntent::OpenLong { .. } => OrderSide::Buy,
            OrderIntent::OpenShort { .. } => OrderSide::Sell,
        }
    }

    /// Builds the request body Alpaca expects.
    ///
    /// `position_intent` is sent explicitly rather than left to Alpaca's inference. Without it a
    /// sell against an existing long is read as a close rather than a short, which for a strategy
    /// that holds both sides of a pair is the difference between opening a hedge and unwinding one.
    fn to_request(&self, client_order_id: Uuid) -> OrderRequest {
        match self {
            OrderIntent::OpenLong { ticker, notional } => OrderRequest {
                symbol: ticker.as_str().to_string(),
                side: OrderSide::Buy.as_str(),
                order_type: "market",
                time_in_force: "day",
                notional: Some(format!("{:.2}", notional.value())),
                qty: None,
                position_intent: "buy_to_open",
                client_order_id: client_order_id.to_string(),
            },
            OrderIntent::OpenShort { ticker, quantity } => OrderRequest {
                symbol: ticker.as_str().to_string(),
                side: OrderSide::Sell.as_str(),
                order_type: "market",
                time_in_force: "day",
                notional: None,
                qty: Some(quantity.get()),
                position_intent: "sell_to_open",
                client_order_id: client_order_id.to_string(),
            },
        }
    }
}

/// Where a submitted order stands, reduced to the three answers a caller acts on.
///
/// Alpaca publishes fifteen order statuses. Collapsing them here rather than at each call site is
/// what makes the caller's `match` exhaustive over outcomes it can actually respond to — keep
/// waiting, use the fill, or unwind — instead of over a string it has to remember the meaning of.
/// Every variant carries the broker's own status alongside the collapse, because the collapse is
/// what the caller acts on and the raw status is what explains it afterwards. An order this process
/// gave up on is recorded as `timed_out`, which is our word; whether Alpaca had it as `pending_new`
/// or `accepted` at that moment is the difference between never reaching the market and reaching it
/// with no contra side.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderState {
    /// Alpaca still has it: accepted, queued, or partially filled. Ask again.
    Working {
        status: String,
        filled_quantity: Decimal,
    },
    /// Terminal and completely filled.
    Filled {
        status: String,
        filled_quantity: Decimal,
        average_price: Decimal,
    },
    /// Terminal without a complete fill: canceled, expired, rejected, or done for the day.
    ///
    /// `filled_quantity` is non-zero when a partial fill was terminated, which is the case that
    /// makes this more than a failure flag — those shares are held and have to be unwound.
    Abandoned {
        status: String,
        filled_quantity: Decimal,
    },
}

impl OrderState {
    /// Alpaca's own word for where the order stood when this state was read.
    pub fn broker_status(&self) -> &str {
        match self {
            OrderState::Working { status, .. }
            | OrderState::Filled { status, .. }
            | OrderState::Abandoned { status, .. } => status,
        }
    }

    /// Whether Alpaca is finished with this order, however it ended.
    pub fn is_terminal(&self) -> bool {
        match self {
            OrderState::Working { .. } => false,
            OrderState::Filled { .. } | OrderState::Abandoned { .. } => true,
        }
    }

    /// Shares Alpaca reports as filled, whatever state the order reached.
    pub fn filled_quantity(&self) -> Decimal {
        match self {
            OrderState::Working {
                filled_quantity, ..
            }
            | OrderState::Filled {
                filled_quantity, ..
            }
            | OrderState::Abandoned {
                filled_quantity, ..
            } => *filled_quantity,
        }
    }
}

/// Which direction a held position runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    pub const ALL: [PositionSide; 2] = [PositionSide::Long, PositionSide::Short];

    pub fn parse(raw: &str) -> Option<Self> {
        PositionSide::ALL
            .into_iter()
            .find(|side| side.as_str() == raw)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
        }
    }
}

impl std::fmt::Display for PositionSide {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for PositionSide {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// What one position fetch returned, and what it could not read.
///
/// `unreadable` separates a flat book from one whose rows could not be interpreted, which are the
/// same empty list and opposite situations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PositionFetch {
    pub positions: Vec<Position>,
    pub unreadable: usize,
}

/// One open position as Alpaca reports it.
///
/// `quantity` is the absolute count and the direction lives in `side`; `market_value` and
/// `unrealized_profit_and_loss` keep Alpaca's sign, so a short reports both as negative.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    ticker: Ticker,
    side: PositionSide,
    quantity: Decimal,
    market_value: Decimal,
    unrealized_profit_and_loss: Decimal,
}

impl Position {
    pub fn new(
        ticker: Ticker,
        side: PositionSide,
        quantity: Decimal,
        market_value: Decimal,
        unrealized_profit_and_loss: Decimal,
    ) -> Self {
        Self {
            ticker,
            side,
            quantity: quantity.abs(),
            market_value,
            unrealized_profit_and_loss,
        }
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn side(&self) -> PositionSide {
        self.side
    }

    pub fn quantity(&self) -> Decimal {
        self.quantity
    }

    pub fn market_value(&self) -> Decimal {
        self.market_value
    }

    pub fn unrealized_profit_and_loss(&self) -> Decimal {
        self.unrealized_profit_and_loss
    }
}

/// Account balances as Alpaca reports them at a point in time.
///
/// Stored verbatim rather than validated into a narrower shape. Every field here can legitimately
/// be negative under margin — `short_market_value` always is — so there is no invariant to enforce
/// that would not also reject a truthful response.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSnapshot {
    equity: Decimal,
    cash: Decimal,
    buying_power: Decimal,
    long_market_value: Decimal,
    short_market_value: Decimal,
}

impl AccountSnapshot {
    pub fn new(
        equity: Decimal,
        cash: Decimal,
        buying_power: Decimal,
        long_market_value: Decimal,
        short_market_value: Decimal,
    ) -> Self {
        Self {
            equity,
            cash,
            buying_power,
            long_market_value,
            short_market_value,
        }
    }

    pub fn equity(&self) -> Decimal {
        self.equity
    }

    pub fn cash(&self) -> Decimal {
        self.cash
    }

    pub fn buying_power(&self) -> Decimal {
        self.buying_power
    }

    pub fn long_market_value(&self) -> Decimal {
        self.long_market_value
    }

    pub fn short_market_value(&self) -> Decimal {
        self.short_market_value
    }

    /// Gross exposure: long plus the magnitude of short.
    ///
    /// The magnitude rather than the signed value, because a market-neutral book nets to roughly
    /// zero and the quantity the exposure cap is about is how much is at work, not how much is net.
    pub fn gross_exposure(&self) -> Decimal {
        self.long_market_value + self.short_market_value.abs()
    }
}

/// One point on Alpaca's equity curve: what the account was worth, and when.
///
/// An instant rather than a session date, per the rule that transport modules stay in transport
/// terms — the caller wraps it with `SessionDate::at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EquityPoint {
    timestamp: DateTime<Utc>,
    equity: Decimal,
}

impl EquityPoint {
    pub fn new(timestamp: DateTime<Utc>, equity: Decimal) -> Self {
        Self { timestamp, equity }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn equity(&self) -> Decimal {
        self.equity
    }
}

/// One of Alpaca's dividend, interest, and fee codes.
///
/// Held in the broker's own spelling rather than expanded into named variants: the fourteen differ
/// in tax treatment rather than in anything this application does with one, and the withholdings
/// among them — `DIVFEE`, `DIVNRA`, `DIVFT`, `DIVTW` — are reductions, so syncing `DIV` alone would
/// report gross income as net.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReturnCode(&'static str);

impl ReturnCode {
    pub const ALL: [ReturnCode; 14] = [
        ReturnCode("DIV"),
        ReturnCode("DIVCGL"),
        ReturnCode("DIVCGS"),
        ReturnCode("DIVFEE"),
        ReturnCode("DIVFT"),
        ReturnCode("DIVNRA"),
        ReturnCode("DIVROC"),
        ReturnCode("DIVTW"),
        ReturnCode("DIVTXEX"),
        ReturnCode("CGD"),
        ReturnCode("INT"),
        ReturnCode("INTNRA"),
        ReturnCode("INTTW"),
        ReturnCode("FEE"),
    ];

    pub fn parse(raw: &str) -> Option<Self> {
        ReturnCode::ALL.into_iter().find(|code| code.0 == raw)
    }

    pub fn as_str(self) -> &'static str {
        self.0
    }
}

/// One of Alpaca's account activity types.
///
/// The distinction the named variants draw is external flow versus return, not cash versus
/// non-cash: `INT`, `DIV`, and `FEE` also move the balance, but they are performance and must never
/// be netted out of a return. `Other` holds anything this build does not recognize, so a type
/// Alpaca adds is stored unclassified rather than dropped.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActivityType {
    /// A trade fill: `FILL`.
    Fill,
    /// A bank deposit: `CSD`.
    CashDeposit,
    /// A bank withdrawal: `CSW`.
    CashWithdrawal,
    /// Cash journalled between accounts on Alpaca's own books, which is how paper accounts are
    /// funded: `JNLC`.
    CashJournal,
    /// A dividend, interest, or fee.
    Return(ReturnCode),
    /// A type this build does not classify.
    Other(String),
}

impl ActivityType {
    /// Every type moving capital into or out of the account from outside it.
    pub const TRANSFERS: [ActivityType; 3] = [
        ActivityType::CashDeposit,
        ActivityType::CashWithdrawal,
        ActivityType::CashJournal,
    ];

    pub fn parse(raw: &str) -> Self {
        match raw {
            "FILL" => ActivityType::Fill,
            "CSD" => ActivityType::CashDeposit,
            "CSW" => ActivityType::CashWithdrawal,
            "JNLC" => ActivityType::CashJournal,
            other => match ReturnCode::parse(other) {
                Some(code) => ActivityType::Return(code),
                None => ActivityType::Other(other.to_string()),
            },
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            ActivityType::Fill => "FILL",
            ActivityType::CashDeposit => "CSD",
            ActivityType::CashWithdrawal => "CSW",
            ActivityType::CashJournal => "JNLC",
            ActivityType::Return(code) => code.as_str(),
            ActivityType::Other(raw) => raw,
        }
    }

    /// Whether capital crossed the account boundary, which is what a return must never net out.
    pub fn is_transfer(&self) -> bool {
        ActivityType::TRANSFERS.contains(self)
    }

    /// Whether the balance moved as performance rather than as flow.
    pub fn is_return(&self) -> bool {
        matches!(self, ActivityType::Return(_))
    }

    /// Every type asked for over the trailing window, which is everything Alpaca stamps with a date
    /// rather than a time.
    pub fn windowed() -> Vec<ActivityType> {
        ReturnCode::ALL
            .into_iter()
            .map(ActivityType::Return)
            .chain(ActivityType::TRANSFERS)
            .collect()
    }
}

impl std::fmt::Display for ActivityType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for ActivityType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// What one activity fetch returned, and what it knows it missed.
///
/// `truncated` is the one that matters: the page bound is a bound on a loop, not on reality, and a
/// session that hit it is holding an incomplete record of itself rather than an empty one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ActivityFetch {
    pub activities: Vec<AccountActivity>,
    /// Activities dropped for carrying neither a transaction time nor a date.
    pub undated: usize,
    /// True when pagination stopped at its bound with more to fetch.
    pub truncated: bool,
}

/// One account activity: a fill, a fee, a dividend, a transfer.
///
/// Fields beyond the identity are optional because Alpaca omits rather than zeroes, and which ones
/// are present depends on the activity type — a `FILL` carries a symbol, side, quantity, and price;
/// a `FEE` carries none of them; a transfer carries only `net_amount`.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountActivity {
    activity_id: String,
    activity_type: ActivityType,
    transaction_time: DateTime<Utc>,
    ticker: Option<Ticker>,
    side: Option<OrderSide>,
    quantity: Option<Decimal>,
    price: Option<Decimal>,
    net_amount: Option<Decimal>,
    alpaca_order_id: Option<String>,
}

impl AccountActivity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        activity_id: String,
        activity_type: ActivityType,
        transaction_time: DateTime<Utc>,
        ticker: Option<Ticker>,
        side: Option<OrderSide>,
        quantity: Option<Decimal>,
        price: Option<Decimal>,
        net_amount: Option<Decimal>,
        alpaca_order_id: Option<String>,
    ) -> Self {
        Self {
            activity_id,
            activity_type,
            transaction_time,
            ticker,
            side,
            quantity,
            price,
            net_amount,
            alpaca_order_id,
        }
    }

    /// Alpaca's own activity identifier, and the primary key of `account_activities`.
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    pub fn activity_type(&self) -> &ActivityType {
        &self.activity_type
    }

    pub fn transaction_time(&self) -> DateTime<Utc> {
        self.transaction_time
    }

    pub fn ticker(&self) -> Option<&Ticker> {
        self.ticker.as_ref()
    }

    pub fn side(&self) -> Option<OrderSide> {
        self.side
    }

    pub fn quantity(&self) -> Option<Decimal> {
        self.quantity
    }

    pub fn price(&self) -> Option<Decimal> {
        self.price
    }

    /// The signed cash effect of a non-trade activity, as Alpaca reports it.
    ///
    /// Transfers carry this instead of quantity and price, so it is the only field that says how
    /// much a deposit moved. Negative for withdrawals and fees.
    pub fn net_amount(&self) -> Option<Decimal> {
        self.net_amount
    }

    pub fn alpaca_order_id(&self) -> Option<&str> {
        self.alpaca_order_id.as_deref()
    }

    /// Signed cash effect of a fill: negative when shares were bought, positive when sold.
    ///
    /// `None` for any activity that is not a two-sided trade, which is what keeps a fee or a
    /// dividend from being attributed to a pair as though it were a leg.
    pub fn signed_cash_flow(&self) -> Option<Decimal> {
        let quantity = self.quantity?;
        let price = self.price?;
        match self.side? {
            OrderSide::Buy => Some(-(quantity * price)),
            OrderSide::Sell | OrderSide::SellShort => Some(quantity * price),
        }
    }
}

/// The order raised to close one position.
///
/// The price is absent because a close is not polled to a terminal state; `alpaca_order_id` joins
/// this to the fill when the post-close activity sync lands it.
#[derive(Debug, Clone, PartialEq)]
pub struct PositionClose {
    ticker: Ticker,
    /// Absent when Alpaca accepted the close but returned a body this client could not read.
    alpaca_order_id: Option<String>,
    side: Option<OrderSide>,
    quantity: Option<Decimal>,
}

impl PositionClose {
    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn alpaca_order_id(&self) -> Option<&str> {
        self.alpaca_order_id.as_deref()
    }

    pub fn side(&self) -> Option<OrderSide> {
        self.side
    }

    pub fn quantity(&self) -> Option<Decimal> {
        self.quantity
    }
}

/// The result of asking Alpaca to close one position during a bulk liquidation.
///
/// Alpaca answers a bulk close with `207 Multi-Status` and a per-symbol status, so a single
/// success/failure for the request as a whole would discard exactly the information that says
/// whether the book is actually flat.
#[derive(Debug, Clone, PartialEq)]
pub struct LiquidationOutcome {
    ticker: Ticker,
    status: u16,
    /// The order Alpaca raised, absent when it refused the close.
    alpaca_order_id: Option<String>,
    quantity: Option<Decimal>,
}

impl LiquidationOutcome {
    pub fn new(ticker: Ticker, status: u16) -> Self {
        Self {
            ticker,
            status,
            alpaca_order_id: None,
            quantity: None,
        }
    }

    /// The outcome together with the order that carried it out.
    pub fn with_order(
        ticker: Ticker,
        status: u16,
        alpaca_order_id: Option<String>,
        quantity: Option<Decimal>,
    ) -> Self {
        Self {
            ticker,
            status,
            alpaca_order_id,
            quantity,
        }
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn alpaca_order_id(&self) -> Option<&str> {
        self.alpaca_order_id.as_deref()
    }

    pub fn quantity(&self) -> Option<Decimal> {
        self.quantity
    }

    /// Whether Alpaca accepted the close for this symbol.
    pub fn succeeded(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

impl TradingClient {
    fn post(&self, url: &str) -> reqwest::RequestBuilder {
        self.http_client
            .post(url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret_key())
    }

    fn delete(&self, url: &str) -> reqwest::RequestBuilder {
        self.http_client
            .delete(url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret_key())
    }

    /// Fetches current account balances.
    pub async fn fetch_account(&self) -> Result<AccountSnapshot, ClientError> {
        let url = format!("{}/v2/account", self.base_url);
        let response = error_for_status(self.get(&url).send().await?).await?;
        let account: AccountResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse account response: {error}"))
        })?;

        Ok(AccountSnapshot::new(
            parse_decimal(&account.equity, "equity")?,
            parse_decimal(&account.cash, "cash")?,
            parse_decimal(&account.buying_power, "buying_power")?,
            parse_decimal(&account.long_market_value, "long_market_value")?,
            parse_decimal(&account.short_market_value, "short_market_value")?,
        ))
    }

    /// Fetches every open position.
    ///
    /// Positions whose symbol or side cannot be interpreted are dropped with a warning rather than
    /// failing the call. The caller uses this to decide what to close, and one unrecognizable row
    /// must not stop the rest of the book from being flattened.
    pub async fn fetch_positions(&self) -> Result<PositionFetch, ClientError> {
        let url = format!("{}/v2/positions", self.base_url);
        let response = error_for_status(self.get(&url).send().await?).await?;
        let payloads: Vec<PositionResponse> = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse positions response: {error}"))
        })?;

        let reported = payloads.len();
        let mut positions = Vec::with_capacity(reported);
        let mut unreadable = 0;
        for payload in payloads {
            let (Some(ticker), Some(side)) = (
                Ticker::new(&payload.symbol),
                PositionSide::parse(&payload.side),
            ) else {
                warn!(
                    symbol = %payload.symbol,
                    side = %payload.side,
                    "Dropped an Alpaca position with an unrecognized symbol or side"
                );
                unreadable += 1;
                continue;
            };
            positions.push(Position::new(
                ticker,
                side,
                parse_decimal(&payload.qty, "qty")?,
                parse_decimal(&payload.market_value, "market_value")?,
                parse_decimal(&payload.unrealized_pl, "unrealized_pl")?,
            ));
        }

        debug!(
            positions = positions.len(),
            reported, unreadable, "Alpaca positions fetched"
        );
        Ok(PositionFetch {
            positions,
            unreadable,
        })
    }

    /// Sends an order under a caller-chosen `client_order_id`.
    ///
    /// The identifier is the caller's because it must exist before the request does, so a crash
    /// before Alpaca answers still leaves an order something can name.
    pub async fn submit_order(
        &self,
        intent: &OrderIntent,
        client_order_id: Uuid,
    ) -> Result<String, ClientError> {
        let url = format!("{}/v2/orders", self.base_url);
        let response = error_for_status(
            self.post(&url)
                .json(&intent.to_request(client_order_id))
                .send()
                .await?,
        )
        .await?;
        let order: OrderResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse order response: {error}"))
        })?;

        info!(
            ticker = %intent.ticker(),
            alpaca_order_id = %order.id,
            %client_order_id,
            "Order submitted"
        );
        Ok(order.id)
    }

    /// Reads the current state of a submitted order.
    pub async fn fetch_order(&self, order_id: &str) -> Result<OrderState, ClientError> {
        let url = format!("{}/v2/orders/{order_id}", self.base_url);
        let response = error_for_status(self.get(&url).send().await?).await?;
        let order: OrderResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse order response: {error}"))
        })?;
        order_state_from(order)
    }

    /// Cancels an open order.
    ///
    /// Returns `false` when Alpaca reports the order already terminal (`422`), which is a race
    /// rather than a failure: the order filled between the decision to cancel and the request.
    pub async fn cancel_order(&self, order_id: &str) -> Result<bool, ClientError> {
        let url = format!("{}/v2/orders/{order_id}", self.base_url);
        let response = self.delete(&url).send().await?;
        if response.status().as_u16() == 422 {
            debug!(order_id, "Order was already terminal; nothing to cancel");
            return Ok(false);
        }
        error_for_status(response).await?;
        info!(order_id, "Order cancelled");
        Ok(true)
    }

    /// Closes one position, returning the order Alpaca raised to do it.
    ///
    /// `None` means there was no position, the expected answer on a retry. The order identifier is
    /// the only handle joining an exit to the fill that settles it in `account_activities`.
    pub async fn close_position(
        &self,
        ticker: &Ticker,
    ) -> Result<Option<PositionClose>, ClientError> {
        let url = format!("{}/v2/positions/{}", self.base_url, ticker.as_str());
        let response = self
            .delete(&url)
            .query(&[("percentage", "100")])
            .send()
            .await?;

        if response.status().as_u16() == 404 {
            info!(ticker = %ticker, "No position to close");
            return Ok(None);
        }
        let response = error_for_status(response).await?;

        // A body that will not parse is not a failed close: Alpaca accepted it, and refusing here
        // would make the caller unwind a position that is already on its way out. The identifier is
        // lost and the close is not.
        let order: Option<ClosedPositionOrder> = response.json().await.ok();
        let alpaca_order_id = order.as_ref().and_then(|order| order.id.clone());
        info!(
            ticker = %ticker,
            alpaca_order_id = alpaca_order_id.as_deref().unwrap_or("unknown"),
            "Position close submitted"
        );
        Ok(Some(PositionClose {
            ticker: ticker.clone(),
            alpaca_order_id,
            side: order
                .as_ref()
                .and_then(|order| order.side.as_deref())
                .and_then(OrderSide::parse),
            quantity: order
                .as_ref()
                .and_then(|order| order.qty.as_deref())
                .and_then(decimal_or_none),
        }))
    }

    /// Closes every open position and cancels every open order.
    ///
    /// This is the pre-close fail-safe, and it deliberately does not consult `equity_pairs` first:
    /// the requirement is that the account is flat overnight, not that the pairs the application
    /// knows about are flat. A leg opened by a pass that died before it could record the pair would
    /// otherwise be held.
    pub async fn close_all_positions(&self) -> Result<Vec<LiquidationOutcome>, ClientError> {
        let url = format!("{}/v2/positions", self.base_url);
        let response = error_for_status(
            self.delete(&url)
                .query(&[("cancel_orders", "true")])
                .send()
                .await?,
        )
        .await?;

        let payloads: Vec<ClosePositionResponse> = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse bulk close response: {error}"))
        })?;

        let mut outcomes = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let Some(ticker) = Ticker::new(&payload.symbol) else {
                warn!(
                    symbol = %payload.symbol,
                    "Dropped a bulk close result with an unrecognized symbol"
                );
                continue;
            };
            if !(200..300).contains(&payload.status) {
                warn!(ticker = %ticker, status = payload.status, "Alpaca refused to close a position");
            }
            outcomes.push(LiquidationOutcome::with_order(
                ticker,
                payload.status,
                payload.body.as_ref().and_then(|order| order.id.clone()),
                payload
                    .body
                    .as_ref()
                    .and_then(|order| order.qty.as_deref())
                    .and_then(decimal_or_none),
            ));
        }

        info!(
            requested = outcomes.len(),
            closed = outcomes.iter().filter(|o| o.succeeded()).count(),
            "Bulk liquidation submitted"
        );
        Ok(outcomes)
    }

    /// Fetches one activity type for a single session date, walking pagination.
    pub async fn fetch_activities(
        &self,
        activity_type: &ActivityType,
        date: NaiveDate,
    ) -> Result<ActivityFetch, ClientError> {
        let url = format!(
            "{}/v2/account/activities/{}",
            self.base_url,
            activity_type.as_str()
        );
        let date_text = date.to_string();
        self.paginate_activities(&url, &[("date", &date_text)], activity_type.as_str())
            .await
    }

    /// Fetches every activity of `activity_types` Alpaca has recorded since `after`.
    ///
    /// Windowed on Alpaca's record time, not the session an activity belongs to: a fee for session
    /// S is not created until roughly S+1 00:15 UTC. Callers file each row under its own date.
    pub async fn fetch_activities_since(
        &self,
        activity_types: &[ActivityType],
        after: NaiveDate,
    ) -> Result<ActivityFetch, ClientError> {
        let url = format!("{}/v2/account/activities", self.base_url);
        let types = activity_types
            .iter()
            .map(ActivityType::as_str)
            .collect::<Vec<_>>()
            .join(",");
        let after_text = after.to_string();
        self.paginate_activities(
            &url,
            &[("activity_types", &types), ("after", &after_text)],
            &types,
        )
        .await
    }

    /// Walks one activity query's pagination into a single fetch result.
    ///
    /// Activities with neither a `transaction_time` nor a `date` are dropped with a warning. They
    /// cannot be stored — `account_activities.transaction_time` is `NOT NULL` — and synthesizing a
    /// timestamp would put a fabricated time into the record the dashboard reads as fact.
    async fn paginate_activities(
        &self,
        url: &str,
        base_query: &[(&str, &str)],
        activity_types: &str,
    ) -> Result<ActivityFetch, ClientError> {
        let page_size = ACTIVITIES_PAGE_SIZE.to_string();

        let mut activities: Vec<AccountActivity> = Vec::new();
        let mut page_token: Option<String> = None;
        let mut undated: usize = 0;
        let mut truncated = false;

        for page in 0..ACTIVITIES_MAXIMUM_PAGES {
            let mut query: Vec<(&str, &str)> = base_query.to_vec();
            query.push(("page_size", page_size.as_str()));
            if let Some(token) = page_token.as_deref() {
                query.push(("page_token", token));
            }

            let response = error_for_status(self.get(url).query(&query).send().await?).await?;
            let payloads: Vec<ActivityResponse> = response.json().await.map_err(|error| {
                ClientError::Parse(format!("Failed to parse activities response: {error}"))
            })?;

            let returned = payloads.len();
            let last_id = payloads.last().map(|payload| payload.id.clone());

            for payload in payloads {
                let Some(transaction_time) = payload
                    .transaction_time
                    .or_else(|| payload.date.map(eastern_midnight))
                else {
                    undated += 1;
                    continue;
                };
                activities.push(AccountActivity::new(
                    payload.id,
                    ActivityType::parse(&payload.activity_type),
                    transaction_time,
                    payload.symbol.as_deref().and_then(Ticker::new),
                    payload.side.as_deref().and_then(OrderSide::parse),
                    payload
                        .qty
                        .as_deref()
                        .map(str::trim)
                        .and_then(decimal_or_none),
                    payload
                        .price
                        .as_deref()
                        .map(str::trim)
                        .and_then(decimal_or_none),
                    payload
                        .net_amount
                        .as_deref()
                        .map(str::trim)
                        .and_then(decimal_or_none),
                    payload.order_id,
                ));
            }

            if returned < ACTIVITIES_PAGE_SIZE {
                break;
            }
            let Some(token) = last_id else { break };
            page_token = Some(token);

            if page + 1 == ACTIVITIES_MAXIMUM_PAGES {
                truncated = true;
                warn!(
                    activity_types,
                    pages = ACTIVITIES_MAXIMUM_PAGES,
                    "Activity pagination hit its page bound; the tail was not fetched"
                );
            }
        }

        if undated > 0 {
            warn!(
                activity_types,
                undated, "Dropped activities carrying neither a transaction time nor a date"
            );
        }
        debug!(
            activity_types,
            activities = activities.len(),
            "Account activities fetched"
        );
        Ok(ActivityFetch {
            activities,
            undated,
            truncated,
        })
    }

    /// Fetches the daily equity curve over an inclusive date range.
    ///
    /// **Alpaca's own `end` is exclusive**, so the day after it is what gets sent, making this
    /// signature inclusive and sparing every caller the knowledge. Verified against the paper
    /// account 2026-08-09: asking through Monday 2026-06-15 stopped at Friday the 12th. Left
    /// uncompensated it drops the newest session, which is the one most likely to need repair.
    ///
    /// Days with a `null` equity are dropped rather than zeroed, since a zero would claim the
    /// account was worthless rather than unvalued.
    pub async fn fetch_portfolio_history(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<EquityPoint>, ClientError> {
        let url = format!("{}/v2/account/portfolio/history", self.base_url);
        let exclusive_end = end
            .succ_opt()
            .ok_or_else(|| ClientError::Parse(format!("End date {end} has no following day")))?;
        let response = error_for_status(
            self.get(&url)
                .query(&[
                    ("start", start.to_string().as_str()),
                    ("end", exclusive_end.to_string().as_str()),
                    ("timeframe", PORTFOLIO_HISTORY_TIMEFRAME),
                ])
                .send()
                .await?,
        )
        .await?;

        let payload: PortfolioHistoryResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!(
                "Failed to parse portfolio history response: {error}"
            ))
        })?;

        if payload.timestamp.len() != payload.equity.len() {
            warn!(
                timestamps = payload.timestamp.len(),
                equities = payload.equity.len(),
                "Portfolio history returned mismatched arrays, using the shorter"
            );
        }

        let mut points = Vec::with_capacity(payload.timestamp.len());
        let mut unusable: usize = 0;
        for (seconds, equity) in payload.timestamp.iter().zip(payload.equity.iter()) {
            let (Some(measured_at), Some(equity)) = (
                DateTime::from_timestamp(*seconds, 0),
                equity.and_then(Decimal::from_f64_retain),
            ) else {
                unusable += 1;
                continue;
            };
            points.push(EquityPoint::new(measured_at, equity.round_dp(2)));
        }

        if unusable > 0 {
            warn!(
                unusable,
                "Dropped portfolio history points with no equity or an unrepresentable timestamp"
            );
        }
        debug!(%start, %end, points = points.len(), "Portfolio history fetched");
        Ok(points)
    }
}

/// Midnight Eastern on a settlement date, as the equivalent UTC instant.
///
/// Non-trade activities carry a `date` and no time, but `account_activities.transaction_time` is
/// `NOT NULL`, so an instant has to be chosen. Eastern midnight is the only choice that round-trips:
/// consumers recover the session with `SessionDate::at`, which reads the Eastern calendar day, so
/// midnight UTC would resolve to the *previous* session for the whole Eastern evening and file a
/// transfer against the wrong day's capital flows.
///
/// Duplicates `SessionDate::midnight`, which cannot be called from here: it lives in
/// `data::calendar`, and that module already depends on this one.
fn eastern_midnight(date: NaiveDate) -> DateTime<Utc> {
    let local_midnight = date
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid wall-clock time");
    New_York
        .from_local_datetime(&local_midnight)
        .earliest()
        .map(|zoned| zoned.with_timezone(&Utc))
        .unwrap_or_else(|| local_midnight.and_utc())
}

/// Collapses Alpaca's order status vocabulary into [`OrderState`].
///
/// Statuses not named here are treated as working, which is the safe default: a caller that keeps
/// waiting on an order that is in fact dead times out and unwinds, whereas one that treats an
/// unfamiliar working status as terminal abandons a live position.
fn order_state_from(order: OrderResponse) -> Result<OrderState, ClientError> {
    let filled_quantity = order
        .filled_qty
        .as_deref()
        .map(str::trim)
        .and_then(decimal_or_none)
        .unwrap_or_default();

    let status = order.status.clone();
    match status.as_str() {
        "filled" => {
            let average_price = order
                .filled_avg_price
                .as_deref()
                .map(str::trim)
                .and_then(decimal_or_none)
                .ok_or_else(|| {
                    ClientError::Parse(format!(
                        "Alpaca reported order {} filled with no average fill price",
                        order.id
                    ))
                })?;
            Ok(OrderState::Filled {
                status: order.status,
                filled_quantity,
                average_price,
            })
        }
        "canceled" | "expired" | "rejected" | "done_for_day" => Ok(OrderState::Abandoned {
            status: order.status,
            filled_quantity,
        }),
        _ => Ok(OrderState::Working {
            status: order.status,
            filled_quantity,
        }),
    }
}

fn parse_decimal(raw: &str, field: &'static str) -> Result<Decimal, ClientError> {
    raw.trim()
        .parse::<Decimal>()
        .map_err(|error| ClientError::Parse(format!("Failed to parse {field} '{raw}': {error}")))
}

/// Parses an optional decimal field, discarding a value that will not parse.
///
/// Unlike the balance fields, an unparsable quantity on one activity is not worth failing a whole
/// session's sync for; the column is nullable and a null reads as "Alpaca did not say".
fn decimal_or_none(raw: &str) -> Option<Decimal> {
    raw.parse::<Decimal>().ok()
}

#[derive(Deserialize)]
struct AccountResponse {
    equity: String,
    cash: String,
    buying_power: String,
    long_market_value: String,
    short_market_value: String,
}

/// Column-oriented, unlike every other response this module parses: the equity at `timestamp[i]` is
/// `equity[i]`. The reader zips them and stops at the shorter, since mismatched lengths are
/// malformed and indexing one by the other would panic. Alpaca sends `null` for an unvalued day.
#[derive(Deserialize)]
struct PortfolioHistoryResponse {
    timestamp: Vec<i64>,
    equity: Vec<Option<f64>>,
}

/// Alpaca's wire names are pinned in the `serde` attributes; the Rust fields spell them out where
/// they differ. `qty` and `type` are the exception — they are the external contract's own spelling
/// and renaming the Rust field would only hide which key is on the wire.
#[derive(serde::Serialize)]
struct OrderRequest {
    symbol: String,
    side: &'static str,
    #[serde(rename = "type")]
    order_type: &'static str,
    time_in_force: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    notional: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    qty: Option<u32>,
    position_intent: &'static str,
    /// The caller's own identifier, chosen before the order is sent.
    ///
    /// This is what makes an order recoverable after a crash between the journal write and
    /// Alpaca's response: the broker's identifier does not exist yet at that point, and this one
    /// does.
    client_order_id: String,
}

#[derive(Deserialize)]
struct OrderResponse {
    id: String,
    status: String,
    filled_qty: Option<String>,
    filled_avg_price: Option<String>,
}

#[derive(Deserialize)]
struct PositionResponse {
    symbol: String,
    side: String,
    qty: String,
    market_value: String,
    unrealized_pl: String,
}

#[derive(Deserialize)]
struct ClosePositionResponse {
    symbol: String,
    status: u16,
    /// The order Alpaca raised, present when it accepted the close.
    body: Option<ClosedPositionOrder>,
}

/// The order returned by a position close, single or bulk.
///
/// Every field is optional, `id` included: a bulk close puts an error object where a refused
/// symbol's order would be, so a required field would fail the parse for the whole liquidation.
#[derive(Deserialize)]
struct ClosedPositionOrder {
    id: Option<String>,
    qty: Option<String>,
    side: Option<String>,
}

#[derive(Deserialize)]
struct ActivityResponse {
    id: String,
    activity_type: String,
    /// Present on trade activities.
    transaction_time: Option<DateTime<Utc>>,
    /// Present on non-trade activities, which carry a settlement date and no time.
    date: Option<NaiveDate>,
    symbol: Option<String>,
    side: Option<String>,
    qty: Option<String>,
    price: Option<String>,
    /// Present on non-trade activities, which carry a net cash amount rather than a quantity and a
    /// price.
    net_amount: Option<String>,
    order_id: Option<String>,
}

/// Symbols requested per snapshot call.
///
/// Not the API's limit — the endpoint has been measured accepting 2,000 symbols in a
/// 9,424-character URL — but a bound on request-line length, which intermediaries do restrict.
///
/// Chunking also limits the blast radius of one failed request, but only because
/// [`MarketDataClient::fetch_snapshots`] keeps the chunks that succeeded.
const SNAPSHOT_SYMBOLS_PER_REQUEST: usize = 1_000;

/// Which consolidated tape the market data API serves from.
///
/// `iex` covers a few percent of consolidated volume, so spreads are wide and often stale outside
/// the largest names — noise that looks exactly like signal. `sip` is the full tape and needs a
/// paid subscription.
///
/// An enum so an unrecognized value is rejected at the edge rather than reaching Alpaca as a 400 on
/// every request of the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFeed {
    Iex,
    Sip,
}

impl DataFeed {
    pub fn as_str(self) -> &'static str {
        match self {
            DataFeed::Iex => "iex",
            DataFeed::Sip => "sip",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "iex" => Some(DataFeed::Iex),
            "sip" => Some(DataFeed::Sip),
            _ => None,
        }
    }
}

impl std::fmt::Display for DataFeed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What one snapshot fetch returned, and what it could not ask about.
///
/// A symbol is absent from `snapshots` either because Alpaca had no usable price for it or because
/// the request carrying it failed. `failed_tickers` is what separates the two.
#[derive(Debug, Clone, Default)]
pub struct SnapshotFetch {
    pub snapshots: Vec<Snapshot>,
    pub failed_tickers: Vec<Ticker>,
}

/// One symbol's point-in-time market state.
///
/// Both readings are optional because Alpaca omits rather than zeroes: a symbol that is halted may
/// have no quote, and one that has not traded today no trade. A missing value and a zero one are
/// indistinguishable after the fact, so the distinction is preserved here and resolved by the
/// caller.
#[derive(Debug, Clone)]
pub struct Snapshot {
    ticker: Ticker,
    latest_quote: Option<EquityQuote>,
    latest_trade: Option<EquityTrade>,
}

impl Snapshot {
    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn latest_quote(&self) -> Option<&EquityQuote> {
        self.latest_quote.as_ref()
    }

    pub fn latest_trade(&self) -> Option<&EquityTrade> {
        self.latest_trade.as_ref()
    }

    /// The reference price, with a book that fails `limits` refused.
    ///
    /// A refused midpoint falls through to the last trade, and a refusal with no last trade behind
    /// it yields `None` so the symbol goes unpriced rather than silently mispriced. The refusal
    /// travels with the price because a caller that cannot see which quotes were rejected cannot
    /// tell whether `limits` is set anywhere near right.
    pub fn reference_price_checked(
        &self,
        now: DateTime<Utc>,
        limits: QuoteLimits,
    ) -> Option<CheckedPrice> {
        let rejection = match self.latest_quote.as_ref() {
            Some(quote) => match limits.refusal(quote, now) {
                None => {
                    return Some(CheckedPrice {
                        price: quote.mid_price(),
                        source: PriceSource::QuoteMidpoint,
                        rejection: None,
                    });
                }
                Some(rejection) => Some(rejection),
            },
            None => None,
        };

        self.latest_trade.as_ref().map(|trade| CheckedPrice {
            price: trade.price(),
            source: PriceSource::LastTrade,
            rejection,
        })
    }
}

/// Seconds a quote may be old before its midpoint stops describing the current market.
///
/// Provisional. Chosen loose enough that the last-trade fallback stays the exception, and meant to
/// be replaced once a session has logged quote ages.
pub const MAXIMUM_QUOTE_AGE_SECONDS: i64 = 60;

/// Widest bid-ask spread, as a fraction of the midpoint, a quote may carry and still be priced.
///
/// Provisional, on the same terms as [`MAXIMUM_QUOTE_AGE_SECONDS`]. A liquid name quotes inside
/// 0.1%; the readings that produced false entries on 2026-08-12 were an order of magnitude wider.
pub const MAXIMUM_RELATIVE_QUOTE_SPREAD: f64 = 0.01;

/// What a book must satisfy for its midpoint to be worth taking.
///
/// Both bounds describe the book rather than the symbol, so one set covers the universe.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteLimits {
    maximum_age: chrono::Duration,
    maximum_relative_spread: f64,
}

impl QuoteLimits {
    /// Constructs limits, refusing bounds that would admit everything or nothing.
    pub fn new(maximum_age: chrono::Duration, maximum_relative_spread: f64) -> Option<Self> {
        if maximum_age <= chrono::Duration::zero() {
            return None;
        }
        if !maximum_relative_spread.is_finite() || maximum_relative_spread <= 0.0 {
            return None;
        }
        Some(Self {
            maximum_age,
            maximum_relative_spread,
        })
    }

    /// Why this book cannot be priced, or `None` if it can.
    ///
    /// Public because a refused book with no last trade behind it leaves no [`CheckedPrice`] to
    /// carry the verdict, and that is the reading most worth recording.
    ///
    /// The midpoint is a safe divisor because [`EquityQuote::new`] has already refused a
    /// non-positive side.
    pub fn refusal(&self, quote: &EquityQuote, now: DateTime<Utc>) -> Option<QuoteRejection> {
        // Magnitude, not sign. A quote stamped after `now` — feed clock skew is ordinary — describes
        // the current market no better than one stamped too far before it.
        let age = now.signed_duration_since(quote.timestamp());
        if age.abs() > self.maximum_age {
            return Some(QuoteRejection::Stale {
                age_seconds: age.num_seconds(),
                limit_seconds: self.maximum_age.num_seconds(),
            });
        }

        let relative_spread = (quote.ask_price() - quote.bid_price()) / quote.mid_price();
        if relative_spread > self.maximum_relative_spread {
            return Some(QuoteRejection::Wide {
                relative_spread,
                limit: self.maximum_relative_spread,
            });
        }

        None
    }
}

impl Default for QuoteLimits {
    /// The production limits, from [`MAXIMUM_QUOTE_AGE_SECONDS`] and
    /// [`MAXIMUM_RELATIVE_QUOTE_SPREAD`].
    fn default() -> Self {
        Self {
            maximum_age: chrono::Duration::seconds(MAXIMUM_QUOTE_AGE_SECONDS),
            maximum_relative_spread: MAXIMUM_RELATIVE_QUOTE_SPREAD,
        }
    }
}

/// Why a quote midpoint was not taken as the reference price.
///
/// Each variant carries the reading that produced it, so the journal can say how far outside
/// the bound the book was rather than only that it was.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QuoteRejection {
    /// The quote is older than the market it is supposed to describe.
    Stale {
        age_seconds: i64,
        limit_seconds: i64,
    },
    /// The book is too wide for its midpoint to be a price anyone would transact at.
    Wide { relative_spread: f64, limit: f64 },
}

impl QuoteRejection {
    /// A stable short name for the journal and the structured logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            QuoteRejection::Stale { .. } => "stale_quote",
            QuoteRejection::Wide { .. } => "wide_quote",
        }
    }
}

impl Serialize for QuoteRejection {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        let mut record = serializer.serialize_struct("QuoteRejection", 3)?;
        record.serialize_field("reason", self.as_str())?;
        match self {
            QuoteRejection::Stale {
                age_seconds,
                limit_seconds,
            } => {
                record.serialize_field("age_seconds", age_seconds)?;
                record.serialize_field("limit_seconds", limit_seconds)?;
            }
            QuoteRejection::Wide {
                relative_spread,
                limit,
            } => {
                record.serialize_field("relative_spread", relative_spread)?;
                record.serialize_field("limit", limit)?;
            }
        }
        record.end()
    }
}

impl std::fmt::Display for QuoteRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuoteRejection::Stale {
                age_seconds,
                limit_seconds,
            } => write!(
                formatter,
                "the quote is {age_seconds} seconds old, past the {limit_seconds} second limit"
            ),
            QuoteRejection::Wide {
                relative_spread,
                limit,
            } => write!(
                formatter,
                "the book is {relative_spread:.4} wide, past the {limit:.4} limit"
            ),
        }
    }
}

/// A reference price together with the verdict on the book that was offered.
///
/// `rejection` is set when a quote existed and was refused, which is the case the price alone
/// cannot express: the price then came from the last trade, and the reason it had to is the part
/// worth recording.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CheckedPrice {
    price: f64,
    source: PriceSource,
    rejection: Option<QuoteRejection>,
}

impl CheckedPrice {
    /// A price with no snapshot behind it, for tests of the pure decision path.
    ///
    /// Test-only because the guard is the point of this type everywhere else: a price reaches the
    /// pass by surviving [`Snapshot::reference_price`], never by being asserted.
    #[cfg(test)]
    pub(crate) fn for_test(price: f64, source: PriceSource) -> Self {
        Self {
            price,
            source,
            rejection: None,
        }
    }

    pub fn price(&self) -> f64 {
        self.price
    }

    pub fn source(&self) -> PriceSource {
        self.source
    }

    pub fn rejection(&self) -> Option<QuoteRejection> {
        self.rejection
    }
}

/// Which field of a [`Snapshot`] a reference price was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceSource {
    QuoteMidpoint,
    LastTrade,
}

impl PriceSource {
    /// A stable short name for the journal and the structured logs.
    pub fn as_str(self) -> &'static str {
        match self {
            PriceSource::QuoteMidpoint => "quote_midpoint",
            PriceSource::LastTrade => "last_trade",
        }
    }
}

impl Serialize for PriceSource {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl std::fmt::Display for PriceSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Rows per quote page. The endpoint's maximum.
const QUOTES_PAGE_SIZE: usize = 10_000;

/// Page bound for the quote cursor, so a token that never clears cannot loop forever.
///
/// Ten million quotes for one name over one session, against a measured worst case of 904,543
/// (XLI, 2026-08-20). Generous because the tail is nothing like the median: the same session's
/// quote counts ranged from 7,337 to that, and the count tracks price level and tick size rather
/// than dollar volume.
const QUOTES_PAGE_LIMIT: usize = 1_000;

/// One tick of the consolidated quote stream, carrying no ticker.
///
/// Its own type rather than an [`EquityQuote`] because a single name-session runs to most of a
/// million of these, and cloning a [`Ticker`] into every one is an allocation per row for a field
/// the caller named in the request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteTick {
    timestamp: DateTime<Utc>,
    bid_price: f64,
    ask_price: f64,
    bid_size: i32,
    ask_size: i32,
}

impl QuoteTick {
    /// Constructs a `QuoteTick`, refusing a book no spread can be read off.
    ///
    /// `None` rather than an error because these are refused in bulk and counted, not reported one
    /// by one: a crossed consolidated book is ordinary around the open, where 20 of AAPL's first
    /// 30,126 quotes on 2026-08-20 were crossed.
    pub fn new(
        timestamp: DateTime<Utc>,
        bid_price: f64,
        ask_price: f64,
        bid_size: i32,
        ask_size: i32,
    ) -> Option<Self> {
        let usable = |price: f64| price.is_finite() && price > 0.0;
        if !usable(bid_price) || !usable(ask_price) || bid_price > ask_price {
            return None;
        }
        if bid_size < 0 || ask_size < 0 {
            return None;
        }
        Some(Self {
            timestamp,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
        })
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn bid_size(&self) -> i32 {
        self.bid_size
    }

    pub fn ask_size(&self) -> i32 {
        self.ask_size
    }

    /// The quoted width of the book.
    pub fn spread(&self) -> f64 {
        self.ask_price - self.bid_price
    }

    /// The midpoint, which [`QuoteTick::new`] has already established is a positive price.
    pub fn mid_price(&self) -> f64 {
        (self.bid_price + self.ask_price) / 2.0
    }
}

/// Attempts per quote page before the fetch gives up on the whole symbol.
///
/// Four, because the page is the unit that fails: one session of AAPL is 118 pages and a single
/// dropped connection anywhere in the chain used to cost the name. Bounded rather than generous —
/// a page that fails four times running is not a blip.
const QUOTES_PAGE_ATTEMPTS: usize = 4;

/// One page and what it took to get it.
struct FetchedPage {
    page: QuotesResponse,
    retries: usize,
}

/// What one quote fetch moved, which is the only trace of it that survives.
///
/// The ticks themselves are handed to the caller's fold and dropped; these counts are what remains
/// to say whether the fold saw a whole session. `rejected` is the tally [`QuoteTick::new`] refused,
/// and `retries` is how many pages had to be asked for twice — a run where it climbs is a feed
/// degrading, which no other number here would show.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuoteFetch {
    pub received: usize,
    pub rejected: usize,
    pub pages: usize,
    pub retries: usize,
}

/// Rows per corporate-actions page. The endpoint's maximum.
const CORPORATE_ACTIONS_PAGE_SIZE: usize = 1_000;

/// Page bound for the corporate-actions cursor, so a token that never clears cannot loop forever.
const CORPORATE_ACTIONS_PAGE_LIMIT: usize = 200;

/// The action types that bound a price series.
///
/// Splits are absent deliberately: they come from Massive and are folded rather than bounded.
/// Mergers and worthless removals are absent because a terminated symbol stops appearing in the bar
/// feed, so a recent window excludes it without being told to.
const BOUNDARY_ACTION_TYPES: &str =
    "name_change,spin_off,rights_distribution,unit_split,reorganization";

/// The historical-quotes envelope: rows keyed by symbol, plus the cursor.
///
/// `quotes` is absent rather than empty when a window holds none, which a name that had not listed
/// yet produces for every session before it did.
#[derive(Debug, Deserialize)]
struct QuotesResponse {
    quotes: Option<HashMap<String, Vec<HistoricalQuotePayload>>>,
    next_page_token: Option<String>,
}

/// One row of the historical quote stream, in the feed's own abbreviations.
///
/// Only the five fields a spread is read off are taken. The exchange codes, condition flags, and
/// tape identifier are parsed past: the SIP feed returns the consolidated best bid and offer, so
/// the venue behind each side is not what this measures.
#[derive(Debug, Deserialize)]
struct HistoricalQuotePayload {
    #[serde(rename = "t")]
    timestamp: Option<DateTime<Utc>>,
    #[serde(rename = "bp", default)]
    bid_price: f64,
    #[serde(rename = "ap", default)]
    ask_price: f64,
    #[serde(rename = "bs", default)]
    bid_size: i32,
    #[serde(rename = "as", default)]
    ask_size: i32,
}

impl HistoricalQuotePayload {
    fn into_tick(self) -> Option<QuoteTick> {
        QuoteTick::new(
            self.timestamp?,
            self.bid_price,
            self.ask_price,
            self.bid_size,
            self.ask_size,
        )
    }
}

/// The corporate-actions envelope: categories keyed by name, plus the cursor.
#[derive(Debug, Deserialize)]
struct CorporateActionsResponse {
    #[serde(default)]
    corporate_actions: CorporateActionCategories,
    next_page_token: Option<String>,
}

/// Every category is optional: the endpoint omits one entirely when a page has none of it.
#[derive(Debug, Default, Deserialize)]
struct CorporateActionCategories {
    #[serde(default)]
    name_changes: Vec<NameChangePayload>,
    #[serde(default)]
    spin_offs: Vec<SpinOffPayload>,
    #[serde(default)]
    rights_distributions: Vec<RightsDistributionPayload>,
    #[serde(default)]
    unit_splits: Vec<UnitSplitPayload>,
    #[serde(default)]
    reorganizations: Vec<ReorganizationPayload>,
}

#[derive(Debug, Deserialize)]
struct NameChangePayload {
    id: String,
    old_symbol: String,
    new_symbol: String,
    process_date: Option<NaiveDate>,
}

#[derive(Debug, Deserialize)]
struct SpinOffPayload {
    id: String,
    source_symbol: String,
    new_symbol: String,
    ex_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

#[derive(Debug, Deserialize)]
struct RightsDistributionPayload {
    id: String,
    source_symbol: String,
    ex_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

#[derive(Debug, Deserialize)]
struct UnitSplitPayload {
    id: String,
    old_symbol: String,
    effective_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

#[derive(Debug, Deserialize)]
struct ReorganizationPayload {
    id: String,
    symbol: String,
    effective_date: Option<NaiveDate>,
    process_date: Option<NaiveDate>,
}

/// Turns one page's categories into boundaries, dropping rows that cannot be one.
///
/// Most dropped rows are the feed's CUSIP placeholders in symbol fields — `038CVR031` and the like
/// — which [`Ticker::new`] rejects for containing digits. A name change whose symbol did not change
/// is dropped by [`SeriesBoundary::new`], and it is the single most common row in the feed.
fn parse_boundaries(categories: CorporateActionCategories) -> Vec<SeriesBoundary> {
    let mut boundaries: Vec<SeriesBoundary> = Vec::new();
    let mut push = |id: String,
                    symbol: &str,
                    date: Option<NaiveDate>,
                    process_date: Option<NaiveDate>,
                    reason: BoundaryReason| {
        // The process date is required rather than defaulted: it is what a refresh matches its own
        // window against, and a row without one could never be recognised as cancelled.
        let (Some(ticker), Some(date), Some(process_date)) =
            (Ticker::new(symbol), date, process_date)
        else {
            return;
        };
        if let Ok(boundary) = SeriesBoundary::new(
            id,
            ticker,
            SessionDate::from_date(date),
            SessionDate::from_date(process_date),
            reason,
        ) {
            boundaries.push(boundary);
        }
    };

    for payload in categories.name_changes {
        // The successor has to parse for the row to say anything: a rename onto a CUSIP placeholder
        // records that the symbol stopped, but not where to follow it.
        let Some(to) = Ticker::new(&payload.new_symbol) else {
            continue;
        };
        push(
            payload.id,
            &payload.old_symbol,
            payload.process_date,
            payload.process_date,
            BoundaryReason::Renamed { to },
        );
    }
    for payload in categories.spin_offs {
        let Some(spin_off_company) = Ticker::new(&payload.new_symbol) else {
            continue;
        };
        push(
            payload.id,
            &payload.source_symbol,
            payload.ex_date,
            payload.process_date,
            BoundaryReason::SpunOff { spin_off_company },
        );
    }
    for payload in categories.rights_distributions {
        push(
            payload.id,
            &payload.source_symbol,
            payload.ex_date,
            payload.process_date,
            BoundaryReason::RightsDistributed,
        );
    }
    for payload in categories.unit_splits {
        push(
            payload.id,
            &payload.old_symbol,
            payload.effective_date,
            payload.process_date,
            BoundaryReason::UnitSeparated,
        );
    }
    for payload in categories.reorganizations {
        push(
            payload.id,
            &payload.symbol,
            payload.effective_date,
            payload.process_date,
            BoundaryReason::Reorganized,
        );
    }

    boundaries
}

/// REST client for the Alpaca market data API.
#[derive(Clone)]
pub struct MarketDataClient {
    http_client: reqwest::Client,
    credentials: AlpacaCredentials,
    base_url: String,
    feed: DataFeed,
}

impl MarketDataClient {
    /// Constructs a client against the production data API.
    pub fn new(credentials: AlpacaCredentials, feed: DataFeed) -> Self {
        Self {
            http_client: build_http_client(),
            credentials,
            base_url: DATA_BASE_URL.to_string(),
            feed,
        }
    }

    /// Constructs a client against an explicit base URL, for tests against a mock server.
    pub fn with_base_url(credentials: AlpacaCredentials, base_url: String, feed: DataFeed) -> Self {
        Self {
            http_client: build_http_client(),
            credentials,
            base_url,
            feed,
        }
    }

    /// Reads the data feed from `ALPACA_DATA_FEED`, defaulting to [`DataFeed::Iex`].
    ///
    /// Defaulting to `iex` keeps an unconfigured deployment working rather than failing on an
    /// entitlement error. An unrecognized value warns and falls back rather than failing startup,
    /// for the same reason -- but it warns, because silently serving a different feed than the
    /// operator asked for changes every price the strategy sees.
    pub fn from_env(credentials: AlpacaCredentials) -> Self {
        let feed = match std::env::var("ALPACA_DATA_FEED") {
            Ok(raw) => DataFeed::parse(&raw).unwrap_or_else(|| {
                warn!(
                    requested = %raw,
                    "Unrecognized ALPACA_DATA_FEED, falling back to iex"
                );
                DataFeed::Iex
            }),
            Err(_) => DataFeed::Iex,
        };
        Self::new(credentials, feed)
    }

    fn get(&self, url: &str) -> reqwest::RequestBuilder {
        self.http_client
            .get(url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret_key())
    }

    /// Fetches the corporate actions that bound a price series, between `start` and `end`.
    ///
    /// Bounded by a date range rather than fetched whole, unlike the splits table: this endpoint
    /// has no all-time form, so the caller decides how far back the archive should reach.
    ///
    /// Rows the feed reports but this cannot use are dropped and counted rather than failing the
    /// page, because most of them are expected — the feed puts CUSIP placeholders in symbol fields
    /// and reports far more unchanged-symbol renames than real ones.
    pub async fn fetch_corporate_actions(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<SeriesBoundary>, ClientError> {
        let url = format!("{}/v1/corporate-actions", self.base_url);
        let start_text = start.to_string();
        let end_text = end.to_string();
        let page_size = CORPORATE_ACTIONS_PAGE_SIZE.to_string();

        let mut boundaries: Vec<SeriesBoundary> = Vec::new();
        let mut page_token: Option<String> = None;
        let mut received: usize = 0;

        for page in 1..=CORPORATE_ACTIONS_PAGE_LIMIT {
            let mut query: Vec<(&str, &str)> = vec![
                ("start", start_text.as_str()),
                ("end", end_text.as_str()),
                ("types", BOUNDARY_ACTION_TYPES),
                ("limit", page_size.as_str()),
            ];
            if let Some(token) = page_token.as_deref() {
                query.push(("page_token", token));
            }

            let response = error_for_status(self.get(&url).query(&query).send().await?).await?;
            let payload: CorporateActionsResponse = response.json().await.map_err(|error| {
                ClientError::Parse(format!("Failed to parse corporate actions: {error}"))
            })?;

            let categories = payload.corporate_actions;
            received += categories.name_changes.len()
                + categories.spin_offs.len()
                + categories.rights_distributions.len()
                + categories.unit_splits.len()
                + categories.reorganizations.len();
            boundaries.extend(parse_boundaries(categories));

            let Some(token) = payload.next_page_token else {
                let dropped = received.saturating_sub(boundaries.len());
                if dropped > 0 {
                    // Routine, and the majority: placeholders and renames that changed no symbol.
                    warn!(
                        dropped,
                        received, "Dropped corporate action rows that bound nothing"
                    );
                }
                info!(
                    boundaries = boundaries.len(),
                    pages = page,
                    %start,
                    %end,
                    "Corporate actions fetched"
                );
                return Ok(boundaries);
            };
            page_token = Some(token);
        }

        Err(ClientError::Parse(format!(
            "corporate action pagination did not end within {CORPORATE_ACTIONS_PAGE_LIMIT} pages"
        )))
    }

    /// Fetches one page, retrying the page itself while the failure is transient.
    ///
    /// Retried here rather than by the caller because the caller's unit is the whole symbol: AAPL
    /// is 118 pages over one session, so restarting it to recover one dropped connection discards
    /// 117 pages of work and gives the largest names both the highest failure odds and the dearest
    /// recovery. The token identifies the page, so resuming at it is exact.
    /// One attempt at one page, with every failure mode expressed as the returned error.
    ///
    /// Separated from the retry loop so there is one place an attempt can fail from. A connection
    /// reset during `send` is the same transport fault as a body that arrives truncated, and the
    /// two have to be indistinguishable here or only one of them gets retried.
    async fn attempt_quote_page(
        &self,
        url: &str,
        query: &[(&str, &str)],
    ) -> Result<QuotesResponse, ClientError> {
        let response = self
            .get(url)
            .query(query)
            .send()
            .await
            .map_err(ClientError::Request)?;
        // `Request`, not `Parse`: calling a dropped body unparseable cost twelve names their
        // retries on the first real run.
        error_for_status(response)
            .await?
            .json::<QuotesResponse>()
            .await
            .map_err(ClientError::Request)
    }

    async fn quote_page(
        &self,
        url: &str,
        query: &[(&str, &str)],
        ticker: &Ticker,
        page: usize,
    ) -> Result<FetchedPage, ClientError> {
        let mut retries = 0usize;
        let mut last_error = None;
        for attempt in 0..QUOTES_PAGE_ATTEMPTS {
            // No `?` anywhere in here. Every way the attempt can fail has to reach the match below
            // or it escapes the retry it was written for -- which is how the send arm was missed.
            match self.attempt_quote_page(url, query).await {
                Ok(page) => return Ok(FetchedPage { page, retries }),
                Err(error) if error.is_transient() => {
                    retries += 1;
                    debug!(%ticker, page, attempt, %error, "Retrying a quote page");
                    last_error = Some(error);
                    tokio::time::sleep(page_retry_delay(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        // The last real failure, not a summary of them. Reporting exhaustion as a parse error would
        // tell the caller a transport fault is permanent, which is the bug this retry exists for.
        Err(last_error.expect("a failed attempt records its error"))
    }

    /// Streams one symbol's quotes over `[start, end)` through `accept`, oldest first.
    ///
    /// A fold rather than a `Vec` because the volume forbids collecting: AAPL alone printed 846,305
    /// quotes on 2026-08-20, which is 110MB of JSON, and the archive wants a dozen numbers out of
    /// it. Each page is dropped once `accept` has seen it, so peak memory is one page.
    ///
    /// Ticks arrive in ascending time order, which every time-weighted fold downstream depends on,
    /// and unusable books are refused here so `accept` only ever sees a quote worth weighing.
    ///
    /// `as_of` is the day whose mapping resolves `ticker`, and must be the session being fetched:
    /// the default is today, which would read a historical window through today's symbol table.
    pub async fn fetch_quotes<F>(
        &self,
        ticker: &Ticker,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        as_of: NaiveDate,
        mut accept: F,
    ) -> Result<QuoteFetch, ClientError>
    where
        F: FnMut(QuoteTick),
    {
        let url = format!("{}/v2/stocks/quotes", self.base_url);
        let start_text = start.to_rfc3339();
        let end_text = end.to_rfc3339();
        let as_of_text = as_of.to_string();
        let page_size = QUOTES_PAGE_SIZE.to_string();
        let feed = self.feed.as_str();

        let mut fetch = QuoteFetch::default();
        let mut page_token: Option<String> = None;

        for _ in 1..=QUOTES_PAGE_LIMIT {
            let mut query: Vec<(&str, &str)> = vec![
                ("symbols", ticker.as_str()),
                ("start", start_text.as_str()),
                ("end", end_text.as_str()),
                ("limit", page_size.as_str()),
                ("feed", feed),
                // Stated rather than inherited. The fold weighs each quote by the time until the
                // next one, so a descending page would silently weigh every tick at zero.
                ("sort", "asc"),
                // The default is today, so a reassigned ticker resolves to its current owner.
                ("asof", as_of_text.as_str()),
            ];
            if let Some(token) = page_token.as_deref() {
                query.push(("page_token", token));
            }

            let payload = self
                .quote_page(&url, &query, ticker, fetch.pages + 1)
                .await?;
            fetch.retries += payload.retries;
            let payload = payload.page;

            fetch.pages += 1;
            let rows = payload
                .quotes
                .and_then(|mut symbols| symbols.remove(ticker.as_str()))
                .unwrap_or_default();
            fetch.received += rows.len();
            for row in rows {
                match row.into_tick() {
                    Some(tick) => accept(tick),
                    None => fetch.rejected += 1,
                }
            }

            let Some(token) = payload.next_page_token else {
                if fetch.rejected > 0 {
                    // Ordinary around the open and not worth a warning, but the ratio is the only
                    // signal that would distinguish a bad feed day from a normal one.
                    debug!(
                        %ticker,
                        rejected = fetch.rejected,
                        received = fetch.received,
                        "Dropped quotes no spread can be read off"
                    );
                }
                return Ok(fetch);
            };
            page_token = Some(token);
        }

        Err(ClientError::Parse(format!(
            "{ticker} quote pagination did not end within {QUOTES_PAGE_LIMIT} pages"
        )))
    }

    /// Fetches point-in-time snapshots for `tickers`, in bounded chunks.
    ///
    /// A chunk that fails is logged and skipped; its symbols simply go unpriced. Partial pricing
    /// narrows the entry set and holds the exits it cannot price, both of which beat pricing
    /// nothing at all. Only a total failure — every chunk failing — is reported as an error, which
    /// keeps that report meaningful for the common single-chunk case.
    ///
    /// The failed chunk's symbols come back named. Downstream, a symbol Alpaca had no quote for and
    /// one whose request never completed are the same absence, and they are not the same problem.
    pub async fn fetch_snapshots(&self, tickers: &[Ticker]) -> Result<SnapshotFetch, ClientError> {
        if tickers.is_empty() {
            return Ok(SnapshotFetch::default());
        }

        let mut snapshots: Vec<Snapshot> = Vec::new();
        let mut failed_tickers: Vec<Ticker> = Vec::new();
        let mut failed_chunks: usize = 0;
        let mut requests: usize = 0;
        let mut last_error: Option<ClientError> = None;

        for chunk in tickers.chunks(SNAPSHOT_SYMBOLS_PER_REQUEST) {
            requests += 1;
            match self.fetch_snapshot_chunk(chunk).await {
                Ok(chunk_snapshots) => snapshots.extend(chunk_snapshots),
                Err(error) => {
                    warn!(
                        error = %error,
                        symbols = chunk.len(),
                        "Snapshot chunk failed; its symbols stay unpriced"
                    );
                    failed_chunks += 1;
                    failed_tickers.extend(chunk.iter().cloned());
                    last_error = Some(error);
                }
            }
        }

        if failed_chunks == requests {
            return Err(last_error.expect("a failed chunk records its error"));
        }

        info!(
            requested = tickers.len(),
            returned = snapshots.len(),
            requests,
            failed_chunks,
            "Snapshots fetched"
        );
        Ok(SnapshotFetch {
            snapshots,
            failed_tickers,
        })
    }

    async fn fetch_snapshot_chunk(&self, tickers: &[Ticker]) -> Result<Vec<Snapshot>, ClientError> {
        let url = format!("{}/v2/stocks/snapshots", self.base_url);
        let symbols = tickers
            .iter()
            .map(Ticker::as_str)
            .collect::<Vec<_>>()
            .join(",");
        let response = error_for_status(
            self.get(&url)
                .query(&[("symbols", symbols.as_str()), ("feed", self.feed.as_str())])
                .send()
                .await?,
        )
        .await?;
        let payload: std::collections::HashMap<String, SnapshotResponse> =
            response.json().await.map_err(|error| {
                ClientError::Parse(format!("Failed to parse snapshots response: {error}"))
            })?;

        Ok(payload
            .into_iter()
            .filter_map(|(symbol, snapshot)| {
                let ticker = Ticker::new(&symbol)?;
                Some(Snapshot {
                    latest_quote: snapshot
                        .latest_quote
                        .and_then(|quote| quote.into_equity_quote(&ticker)),
                    latest_trade: snapshot
                        .latest_trade
                        .and_then(|trade| trade.into_equity_trade(&ticker)),
                    ticker,
                })
            })
            .collect())
    }
}

/// Alpaca's wire names are pinned in the `serde` attributes, which is what the external contract
/// actually is; the Rust fields spell them out.
#[derive(Deserialize)]
struct SnapshotResponse {
    #[serde(rename = "latestQuote")]
    latest_quote: Option<QuotePayload>,
    #[serde(rename = "latestTrade")]
    latest_trade: Option<TradePayload>,
}

#[derive(Deserialize)]
struct QuotePayload {
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
}

impl QuotePayload {
    /// A quote missing any of its four book fields or its timestamp is dropped rather than
    /// defaulted: a zero bid is a real price, and an absent one is not.
    ///
    /// A present-but-incoherent book — crossed, or with a zero side — is dropped the same way, via
    /// the constructor's own validation. Both cases mean "this symbol is unpriced right now", which
    /// callers already handle.
    fn into_equity_quote(self, ticker: &Ticker) -> Option<EquityQuote> {
        EquityQuote::new(
            ticker.clone(),
            self.timestamp?,
            self.bid_price?,
            self.ask_price?,
            self.bid_size?,
            self.ask_size?,
        )
        .inspect_err(|error| {
            debug!(ticker = %ticker, error = %error, "Dropped an incoherent quote");
        })
        .ok()
    }
}

#[derive(Deserialize)]
struct TradePayload {
    #[serde(rename = "t")]
    timestamp: Option<DateTime<Utc>>,
    #[serde(rename = "p")]
    price: Option<f64>,
}

impl TradePayload {
    /// A trade missing its price or its timestamp is dropped, on the same terms as
    /// [`QuotePayload::into_equity_quote`], costing the symbol its fallback price.
    fn into_equity_trade(self, ticker: &Ticker) -> Option<EquityTrade> {
        EquityTrade::new(ticker.clone(), self.timestamp?, self.price?)
            .inspect_err(|error| {
                debug!(ticker = %ticker, error = %error, "Dropped an incoherent trade");
            })
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serial_test::serial;

    #[test]
    fn test_new_stores_fields() {
        let credentials =
            AlpacaCredentials::new("key123".to_string(), "secret456".to_string()).unwrap();
        assert_eq!(credentials.key_id(), "key123");
        assert_eq!(credentials.secret_key(), "secret456");
    }

    #[test]
    fn test_new_rejects_empty_key_id() {
        // `matches!` rather than `assert_eq!`: `AlpacaCredentials` deliberately does not derive
        // `Debug`, because it holds a secret and a derived `Debug` would print it into any log line
        // or panic message that formatted the value.
        assert!(matches!(
            AlpacaCredentials::new(String::new(), "secret456".to_string()),
            Err(CredentialsError::Empty { field: "key_id" })
        ));
    }

    #[test]
    fn test_new_rejects_empty_secret_key() {
        assert!(matches!(
            AlpacaCredentials::new("key123".to_string(), String::new()),
            Err(CredentialsError::Empty {
                field: "secret_key"
            })
        ));
    }

    #[test]
    fn test_clone() {
        let credentials =
            AlpacaCredentials::new("key123".to_string(), "secret456".to_string()).unwrap();
        let cloned = credentials.clone();
        assert_eq!(cloned.key_id(), "key123");
        assert_eq!(cloned.secret_key(), "secret456");
    }

    /// RAII guard restoring one environment variable on drop, so an assertion failure cannot leave
    /// a removal in place for the next `#[serial]` test in the file.
    struct EnvironmentVariableGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvironmentVariableGuard {
        fn save(key: &'static str) -> Self {
            Self {
                key,
                previous: std::env::var(key).ok(),
            }
        }
    }

    impl Drop for EnvironmentVariableGuard {
        fn drop(&mut self) {
            // SAFETY: protected by #[serial_test::serial] — no concurrent environment access.
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    #[serial]
    fn test_from_env_reports_the_missing_variable() {
        let _key_guard = EnvironmentVariableGuard::save("ALPACA_API_KEY_ID");
        let _secret_guard = EnvironmentVariableGuard::save("ALPACA_API_SECRET");
        // SAFETY: protected by #[serial_test::serial] — no concurrent environment access.
        unsafe {
            std::env::remove_var("ALPACA_API_KEY_ID");
            std::env::remove_var("ALPACA_API_SECRET");
        }

        assert!(matches!(
            AlpacaCredentials::from_env(),
            Err(CredentialsError::Missing {
                variable: "ALPACA_API_KEY_ID"
            })
        ));
    }

    #[test]
    fn test_data_feed_round_trips_and_rejects_unknown() {
        for feed in [DataFeed::Iex, DataFeed::Sip] {
            assert_eq!(DataFeed::parse(feed.as_str()), Some(feed));
        }
        assert_eq!(DataFeed::parse("SIP"), Some(DataFeed::Sip));
        assert_eq!(DataFeed::parse(" iex "), Some(DataFeed::Iex));
        assert_eq!(DataFeed::parse("otc"), None);
        assert_eq!(DataFeed::parse(""), None);
    }

    fn credentials() -> AlpacaCredentials {
        AlpacaCredentials::new("key".to_string(), "secret".to_string()).unwrap()
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn time(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    /// A session whose close is not after its open cannot gate anything, so construction refuses it
    /// rather than producing a zero-length or inverted trading day.
    #[test]
    fn test_calendar_day_rejects_non_positive_session() {
        assert!(CalendarDay::new(date(2026, 6, 10), time(9, 30), time(9, 30)).is_none());
        assert!(CalendarDay::new(date(2026, 6, 10), time(16, 0), time(9, 30)).is_none());
        assert!(CalendarDay::new(date(2026, 6, 10), time(9, 30), time(16, 0)).is_some());
    }

    #[test]
    fn test_tradable_assets_partitions_membership() {
        let assets = TradableAssets::from_sets(
            HashSet::from([ticker("AAPL"), ticker("MSFT")]),
            HashSet::from([ticker("AAPL")]),
        );
        assert!(assets.is_tradable(&ticker("AAPL")));
        assert!(assets.is_tradable(&ticker("MSFT")));
        assert!(assets.is_shortable(&ticker("AAPL")));
        assert!(!assets.is_shortable(&ticker("MSFT")));
        assert!(!assets.is_tradable(&ticker("NVDA")));
        assert_eq!(assets.tradable_count(), 2);
        assert_eq!(assets.shortable_count(), 1);
    }

    /// A half-day must survive with its real close. Dropping the published hours in favour of an
    /// assumed 16:00 is the specific bug this endpoint exists to prevent.
    #[tokio::test]
    async fn test_fetch_calendar_preserves_early_close() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/calendar?start=2026-11-27&end=2026-11-27")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"[{"date":"2026-11-27","open":"09:30","close":"13:00"}]"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let days = client
            .fetch_calendar(date(2026, 11, 27), date(2026, 11, 27))
            .await
            .expect("calendar must parse");

        assert_eq!(days.len(), 1);
        assert_eq!(days[0].session_close(), time(13, 0));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_calendar_drops_unusable_days() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/calendar?start=2026-06-10&end=2026-06-11")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"date":"2026-06-10","open":"16:00","close":"09:30"},
                    {"date":"2026-06-11","open":"09:30","close":"16:00"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let days = client
            .fetch_calendar(date(2026, 6, 10), date(2026, 6, 11))
            .await
            .expect("calendar must parse");

        assert_eq!(days.len(), 1, "the inverted session must be dropped");
        assert_eq!(days[0].session_date(), date(2026, 6, 11));
        mock.assert_async().await;
    }

    /// The response is column-oriented, so the pairing is positional and easy to get backwards.
    /// Two points with distinguishable equities catch a transposition that equal values would hide.
    #[tokio::test]
    async fn test_portfolio_history_pairs_each_equity_with_its_own_timestamp() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                "/v2/account/portfolio/history?start=2026-05-14&end=2026-05-16&timeframe=1D",
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"timestamp":[1778788800,1778875200],
                    "equity":[20100.5,20559.16],
                    "profit_loss":[0,458.66],"base_value":20100.5,"timeframe":"1D"}"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let points = client
            .fetch_portfolio_history(
                NaiveDate::from_ymd_opt(2026, 5, 14).expect("a valid date"),
                NaiveDate::from_ymd_opt(2026, 5, 15).expect("a valid date"),
            )
            .await
            .expect("portfolio history must parse");

        assert_eq!(points.len(), 2);
        // Both halves: asserting equities alone would pass with the timestamps transposed.
        assert_eq!(points[0].timestamp().timestamp(), 1_778_788_800);
        assert_eq!(points[0].equity(), Decimal::new(2010050, 2));
        assert_eq!(points[1].timestamp().timestamp(), 1_778_875_200);
        assert_eq!(points[1].equity(), Decimal::new(2055916, 2));
        mock.assert_async().await;
    }

    /// The fixture stamps 20:00 UTC, which is the 16:00 Eastern close. A point at midnight UTC
    /// would read as the previous Eastern session and shift the whole curve back a day.
    #[tokio::test]
    async fn test_a_daily_point_lands_on_the_eastern_session_it_measured() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                "/v2/account/portfolio/history?start=2026-05-15&end=2026-05-16&timeframe=1D",
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"timestamp":[1778875200],"equity":[20559.16]}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let points = client
            .fetch_portfolio_history(
                NaiveDate::from_ymd_opt(2026, 5, 15).expect("a valid date"),
                NaiveDate::from_ymd_opt(2026, 5, 15).expect("a valid date"),
            )
            .await
            .expect("portfolio history must parse");

        let eastern = points[0].timestamp().with_timezone(&New_York).date_naive();
        assert_eq!(
            eastern,
            NaiveDate::from_ymd_opt(2026, 5, 15).expect("a valid date")
        );
        mock.assert_async().await;
    }

    /// The mock answers only `end=2026-05-16`, pinning the exclusive-end compensation. Without it
    /// every session but the last returns and the run reports a tidy fill over a surviving gap.
    #[tokio::test]
    async fn test_the_requested_end_date_is_included_in_the_response() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                "/v2/account/portfolio/history?start=2026-05-15&end=2026-05-16&timeframe=1D",
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"timestamp":[1778875200],"equity":[20559.16]}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let day = NaiveDate::from_ymd_opt(2026, 5, 15).expect("a valid date");
        let points = client
            .fetch_portfolio_history(day, day)
            .await
            .expect("portfolio history must parse");

        assert_eq!(points.len(), 1, "the end date itself must come back");
        mock.assert_async().await;
    }

    /// A `null` equity means no valuation; zero would claim a wiped-out account.
    #[tokio::test]
    async fn test_portfolio_history_drops_points_with_no_equity() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                "/v2/account/portfolio/history?start=2026-05-14&end=2026-05-16&timeframe=1D",
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"timestamp":[1778788800,1778875200],"equity":[null,20559.16]}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let points = client
            .fetch_portfolio_history(
                NaiveDate::from_ymd_opt(2026, 5, 14).expect("a valid date"),
                NaiveDate::from_ymd_opt(2026, 5, 15).expect("a valid date"),
            )
            .await
            .expect("portfolio history must parse");

        assert_eq!(points.len(), 1, "the valueless day must be dropped");
        assert_eq!(points[0].equity(), Decimal::new(2055916, 2));
        mock.assert_async().await;
    }

    /// Truncating to the shorter array keeps the pairings that are certainly correct.
    #[tokio::test]
    async fn test_portfolio_history_truncates_mismatched_arrays() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                "/v2/account/portfolio/history?start=2026-05-14&end=2026-05-16&timeframe=1D",
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"timestamp":[1778788800,1778875200],"equity":[20100.5]}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let points = client
            .fetch_portfolio_history(
                NaiveDate::from_ymd_opt(2026, 5, 14).expect("a valid date"),
                NaiveDate::from_ymd_opt(2026, 5, 15).expect("a valid date"),
            )
            .await
            .expect("portfolio history must parse");

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].equity(), Decimal::new(2010050, 2));
        mock.assert_async().await;
    }

    /// Shortability requires both flags. A symbol Alpaca marks shortable but hard to borrow must
    /// not reach the short leg, where the order would be rejected at submission.
    #[tokio::test]
    async fn test_fetch_tradable_assets_requires_both_short_flags() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/assets?status=active&asset_class=us_equity")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[
                    {"symbol":"AAPL","status":"active","tradable":true,"shortable":true,"easy_to_borrow":true},
                    {"symbol":"MSFT","status":"active","tradable":true,"shortable":true,"easy_to_borrow":false},
                    {"symbol":"XYZ","status":"inactive","tradable":true,"shortable":true,"easy_to_borrow":true},
                    {"symbol":"NOPE","status":"active","tradable":false,"shortable":true,"easy_to_borrow":true}
                ]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let assets = client
            .fetch_tradable_assets()
            .await
            .expect("assets must parse");

        assert!(assets.is_shortable(&ticker("AAPL")));
        assert!(assets.is_tradable(&ticker("MSFT")));
        assert!(
            !assets.is_shortable(&ticker("MSFT")),
            "hard to borrow is not shortable"
        );
        assert!(
            !assets.is_tradable(&ticker("XYZ")),
            "inactive assets are excluded"
        );
        assert!(
            !assets.is_tradable(&ticker("NOPE")),
            "non-tradable assets are excluded"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_api_error_carries_status_and_body() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(403)
            .with_body("forbidden")
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let error = client
            .fetch_calendar(date(2026, 6, 10), date(2026, 6, 10))
            .await
            .expect_err("403 must be an error");

        match error {
            ClientError::Api { status, body } => {
                assert_eq!(status, 403);
                assert_eq!(body, "forbidden");
            }
            other => panic!("expected an API error, got {other:?}"),
        }
        mock.assert_async().await;
    }

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).expect("the test ticker must be valid")
    }

    fn shares(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).expect("the test share count must be non-zero")
    }

    fn order_response(status: &str, filled_qty: &str, filled_avg_price: &str) -> OrderResponse {
        OrderResponse {
            id: "order-1".to_string(),
            status: status.to_string(),
            filled_qty: Some(filled_qty.to_string()),
            filled_avg_price: Some(filled_avg_price.to_string()),
        }
    }

    /// A long leg is sized in dollars and a short leg in whole shares, and the two must not be
    /// interchangeable. Sending `qty` on a long would lose the fractional sizing that lets the two
    /// legs match on notional; sending `notional` on a short is rejected by Alpaca outright.
    #[test]
    fn test_order_intent_sizes_each_side_in_its_own_units() {
        let long = OrderIntent::OpenLong {
            ticker: ticker("AAPL"),
            notional: Dollars::new(Decimal::new(123456, 2)).unwrap(),
        }
        .to_request(Uuid::nil());
        assert_eq!(long.side, "buy");
        assert_eq!(long.notional.as_deref(), Some("1234.56"));
        assert_eq!(long.qty, None);
        assert_eq!(long.position_intent, "buy_to_open");

        let short = OrderIntent::OpenShort {
            ticker: ticker("MSFT"),
            quantity: shares(40),
        }
        .to_request(Uuid::nil());
        assert_eq!(short.side, "sell");
        assert_eq!(short.notional, None);
        assert_eq!(short.qty, Some(40));
        assert_eq!(short.position_intent, "sell_to_open");
    }

    /// `sell_to_open` rather than a bare sell. Without the explicit intent Alpaca reads a sell
    /// against an existing long as a close, so a pair whose long leg is already held would have its
    /// hedge silently turned into an unwind.
    #[test]
    fn test_short_order_serializes_the_opening_intent() {
        let body = serde_json::to_value(
            OrderIntent::OpenShort {
                ticker: ticker("MSFT"),
                quantity: shares(10),
            }
            .to_request(Uuid::nil()),
        )
        .unwrap();
        assert_eq!(body["position_intent"], "sell_to_open");
        assert_eq!(body["type"], "market");
        assert_eq!(body["time_in_force"], "day");
        assert!(body.get("notional").is_none());
    }

    #[test]
    fn test_order_state_maps_terminal_and_working_statuses() {
        assert_eq!(
            order_state_from(order_response("filled", "12", "101.25")).unwrap(),
            OrderState::Filled {
                status: "filled".to_string(),
                filled_quantity: Decimal::new(12, 0),
                average_price: Decimal::new(10125, 2),
            }
        );
        for status in ["canceled", "expired", "rejected", "done_for_day"] {
            assert!(matches!(
                order_state_from(order_response(status, "3", "100")).unwrap(),
                OrderState::Abandoned {
                    filled_quantity,
                    ..
                } if filled_quantity == Decimal::new(3, 0)
            ));
        }
        for status in ["new", "accepted", "partially_filled", "pending_cancel"] {
            assert!(matches!(
                order_state_from(order_response(status, "0", "0")).unwrap(),
                OrderState::Working { .. }
            ));
        }
    }

    /// Neither status ends an order. `stopped` means a trade is guaranteed at a stated price and
    /// has not settled yet; `suspended` means temporarily ineligible to trade. Both were mapped to
    /// `Abandoned`, so `is_terminal` stopped the poll on an order that could still fill and leave
    /// an unhedged leg on the book.
    #[test]
    fn test_stopped_and_suspended_orders_are_still_working() {
        for status in ["stopped", "suspended"] {
            let state = order_state_from(order_response(status, "0", "0")).unwrap();
            assert!(
                matches!(state, OrderState::Working { .. }),
                "{status} can still fill, so the caller has to keep waiting"
            );
            assert!(!state.is_terminal());
        }
    }

    /// An unfamiliar status reads as working, not as dead. A caller that keeps waiting on an order
    /// that has in fact ended times out and unwinds; one that abandons a live order leaves an
    /// unhedged leg on the book.
    #[test]
    fn test_unknown_order_status_is_treated_as_working() {
        assert!(matches!(
            order_state_from(order_response("held_for_some_new_reason", "0", "0")).unwrap(),
            OrderState::Working { .. }
        ));
    }

    /// A partial fill that was then cancelled still holds shares, so the count has to survive into
    /// the terminal state — it is what the unwind is sized against.
    #[test]
    fn test_abandoned_order_retains_its_partial_fill() {
        let state = order_state_from(order_response("canceled", "7.5", "0")).unwrap();
        assert_eq!(state.filled_quantity(), Decimal::new(75, 1));
        assert!(state.is_terminal());
    }

    /// Alpaca reports `filled` and then omits the price only when something is wrong. Defaulting it
    /// to zero would record a free fill, which the P&L attribution would then believe.
    #[test]
    fn test_filled_order_without_a_price_is_an_error() {
        let order = OrderResponse {
            id: "order-1".to_string(),
            status: "filled".to_string(),
            filled_qty: Some("10".to_string()),
            filled_avg_price: None,
        };
        assert!(matches!(
            order_state_from(order),
            Err(ClientError::Parse(_))
        ));
    }

    #[tokio::test]
    async fn test_fetch_account_reads_every_balance() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/account")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"equity":"105000.50","cash":"25000.00",
                    "buying_power":"210000.00","long_market_value":"80000.00",
                    "short_market_value":"-79000.00"}"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let account = client.fetch_account().await.expect("account must parse");

        assert_eq!(account.equity(), Decimal::new(10500050, 2));
        assert_eq!(account.cash(), Decimal::new(2500000, 2));
        assert_eq!(account.buying_power(), Decimal::new(21000000, 2));
        assert_eq!(account.long_market_value(), Decimal::new(8000000, 2));
        assert_eq!(account.short_market_value(), Decimal::new(-7900000, 2));
        mock.assert_async().await;
    }

    /// A market-neutral book nets to roughly zero, so gross exposure has to add the magnitudes.
    /// Summing the signed values would report this account as holding a thousand dollars.
    #[test]
    fn test_gross_exposure_adds_magnitudes_rather_than_netting() {
        let account = AccountSnapshot::new(
            Decimal::new(100000, 0),
            Decimal::new(20000, 0),
            Decimal::new(200000, 0),
            Decimal::new(80000, 0),
            Decimal::new(-79000, 0),
        );
        assert_eq!(account.gross_exposure(), Decimal::new(159000, 0));
    }

    #[tokio::test]
    async fn test_fetch_positions_reports_absolute_share_counts() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/positions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"symbol":"AAPL","side":"long","qty":"10","market_value":"1500.00",
                     "unrealized_pl":"25.00"},
                    {"symbol":"MSFT","side":"short","qty":"-4","market_value":"-1480.00",
                     "unrealized_pl":"-12.00"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let positions = client
            .fetch_positions()
            .await
            .expect("positions must parse");

        assert_eq!(positions.positions.len(), 2);
        assert_eq!(positions.positions[1].side(), PositionSide::Short);
        assert_eq!(positions.positions[1].quantity(), Decimal::new(4, 0));
        assert_eq!(
            positions.positions[1].market_value(),
            Decimal::new(-1480, 0)
        );
        mock.assert_async().await;
    }

    /// One unreadable row must not stop the rest of the book from being flattened. This is the
    /// pre-close path, and the cost of failing the whole call is a position held overnight.
    #[tokio::test]
    async fn test_fetch_positions_drops_an_unreadable_row_and_keeps_the_rest() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/positions")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"symbol":"AAPL","side":"sideways","qty":"10","market_value":"1500.00",
                     "unrealized_pl":"25.00"},
                    {"symbol":"MSFT","side":"long","qty":"4","market_value":"1480.00",
                     "unrealized_pl":"12.00"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let positions = client
            .fetch_positions()
            .await
            .expect("positions must parse");

        assert_eq!(positions.positions.len(), 1);
        assert_eq!(
            positions.unreadable, 1,
            "the unreadable row is counted, not silently dropped"
        );
        assert_eq!(positions.positions[0].ticker(), &ticker("MSFT"));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_submit_order_returns_the_order_identifier() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/v2/orders")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "symbol": "AAPL",
                "side": "buy",
                "notional": "5000.00",
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"id":"abc-123","status":"accepted"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let order_id = client
            .submit_order(
                &OrderIntent::OpenLong {
                    ticker: ticker("AAPL"),
                    notional: Dollars::new(Decimal::new(5000, 0)).unwrap(),
                },
                Uuid::nil(),
            )
            .await
            .expect("order must submit");

        assert_eq!(order_id, "abc-123");
        mock.assert_async().await;
    }

    /// Closing a position that is not there is the expected answer on a retry, or after Alpaca
    /// liquidated the leg itself. Treating the 404 as an error would turn a no-op into an incident
    /// and stall the rest of the liquidation behind it.
    #[tokio::test]
    async fn test_close_position_reports_a_missing_position_without_failing() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/positions/AAPL?percentage=100")
            .with_status(404)
            .with_body(r#"{"message":"position not found"}"#)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        assert!(client
            .close_position(&ticker("AAPL"))
            .await
            .expect("a missing position is not an error")
            .is_none());
        mock.assert_async().await;
    }

    /// Alpaca answers a bulk close with 207 and a per-symbol status. Reading only the outer status
    /// would report a liquidation that left half the book open as a complete success.
    #[tokio::test]
    async fn test_close_all_positions_reports_each_symbol_separately() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/positions?cancel_orders=true")
            .with_status(207)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"symbol":"AAPL","status":200,"body":{}},
                    {"symbol":"MSFT","status":422,"body":{}}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let outcomes = client
            .close_all_positions()
            .await
            .expect("bulk close must parse");

        assert_eq!(outcomes.len(), 2);
        assert!(outcomes[0].succeeded());
        assert!(!outcomes[1].succeeded());
        assert_eq!(outcomes[1].ticker(), &ticker("MSFT"));
        mock.assert_async().await;
    }

    /// A cancel that races a fill comes back 422. That is the order having filled, not the cancel
    /// having failed, and the caller has to be able to tell them apart.
    #[tokio::test]
    async fn test_cancel_order_treats_an_already_terminal_order_as_no_op() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("DELETE", "/v2/orders/order-1")
            .with_status(422)
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        assert!(!client
            .cancel_order("order-1")
            .await
            .expect("a terminal order is not an error"));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_fetch_activities_follows_pagination() {
        let mut server = mockito::Server::new_async().await;
        let full_page: Vec<serde_json::Value> = (0..ACTIVITIES_PAGE_SIZE)
            .map(|index| {
                serde_json::json!({
                    "id": format!("activity-{index}"),
                    "activity_type": "FILL",
                    "transaction_time": "2026-07-30T18:00:00Z",
                    "symbol": "AAPL",
                    "side": "buy",
                    "qty": "1",
                    "price": "150.00",
                    "order_id": "order-1",
                })
            })
            .collect();
        let last_id = format!("activity-{}", ACTIVITIES_PAGE_SIZE - 1);

        let first = server
            .mock("GET", "/v2/account/activities/FILL")
            .match_query(mockito::Matcher::UrlEncoded(
                "date".to_string(),
                "2026-07-30".to_string(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(serde_json::to_string(&full_page).unwrap())
            .create_async()
            .await;
        let second = server
            .mock("GET", "/v2/account/activities/FILL")
            .match_query(mockito::Matcher::UrlEncoded(
                "page_token".to_string(),
                last_id,
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"activity-tail","activity_type":"FILL",
                     "transaction_time":"2026-07-30T18:05:00Z","symbol":"MSFT","side":"sell_short",
                     "qty":"2","price":"370.00","order_id":"order-2"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let activities = client
            .fetch_activities(&ActivityType::Fill, date(2026, 7, 30))
            .await
            .expect("activities must parse");

        assert_eq!(activities.activities.len(), ACTIVITIES_PAGE_SIZE + 1);
        assert_eq!(
            activities.activities.last().unwrap().activity_id(),
            "activity-tail"
        );
        assert!(
            !activities.truncated,
            "this fetch finished inside the page bound"
        );
        first.assert_async().await;
        second.assert_async().await;
    }

    /// A buy consumes cash and a sell produces it. Getting the sign wrong inverts every pair's
    /// realized profit and loss, which is a number that looks entirely plausible either way.
    #[test]
    fn test_signed_cash_flow_follows_the_side() {
        let build = |side: OrderSide| {
            AccountActivity::new(
                "a".to_string(),
                ActivityType::Fill,
                Utc::now(),
                Some(ticker("AAPL")),
                Some(side),
                Some(Decimal::new(10, 0)),
                Some(Decimal::new(15050, 2)),
                None,
                None,
            )
        };
        assert_eq!(
            build(OrderSide::Buy).signed_cash_flow(),
            Some(Decimal::new(-150500, 2))
        );
        assert_eq!(
            build(OrderSide::Sell).signed_cash_flow(),
            Some(Decimal::new(150500, 2))
        );
        assert_eq!(
            build(OrderSide::SellShort).signed_cash_flow(),
            Some(Decimal::new(150500, 2))
        );
    }

    /// A fee has no side, quantity, or price. Attributing it to a pair as though it were a leg
    /// would move that pair's realized profit and loss by the fee amount.
    #[test]
    fn test_non_trade_activity_has_no_cash_flow() {
        let fee = AccountActivity::new(
            "f".to_string(),
            ActivityType::parse("FEE"),
            Utc::now(),
            None,
            None,
            None,
            None,
            Some(Decimal::new(-2, 2)),
            None,
        );
        assert_eq!(fee.signed_cash_flow(), None);
        assert_eq!(fee.net_amount(), Some(Decimal::new(-2, 2)));
    }

    /// `account_activities.transaction_time` is NOT NULL, so an activity with neither a time nor a
    /// date cannot be stored. Synthesizing one would put a fabricated timestamp into the record.
    #[tokio::test]
    async fn test_activity_without_a_time_or_date_is_dropped() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"timeless","activity_type":"FEE"},
                    {"id":"dated","activity_type":"DIV","date":"2026-07-30"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let activities = client
            .fetch_activities(&ActivityType::parse("FEE"), date(2026, 7, 30))
            .await
            .expect("activities must parse");

        assert_eq!(activities.activities.len(), 1);
        assert_eq!(activities.activities[0].activity_id(), "dated");
        assert_eq!(
            activities.undated, 1,
            "the dropped record is counted, not silently absent"
        );
        mock.assert_async().await;
    }

    /// A settlement date carries no time, and the instant synthesized for it decides which session
    /// the activity lands in. Midnight UTC is still the *previous* Eastern day for the whole
    /// evening, so a transfer dated the 15th would reconcile against the 14th's capital flows.
    #[tokio::test]
    async fn test_dated_activity_resolves_to_its_eastern_session() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"20260515000000000::journal","activity_type":"JNLC",
                     "date":"2026-05-15","net_amount":"20000","status":"executed"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let activities = client
            .fetch_activities(&ActivityType::CashJournal, date(2026, 5, 15))
            .await
            .expect("activities must parse");

        assert_eq!(
            activities.activities[0]
                .transaction_time()
                .with_timezone(&New_York)
                .date_naive(),
            date(2026, 5, 15),
            "a dated activity must land in the session Alpaca dated it"
        );
        mock.assert_async().await;
    }

    /// The trailing-window fetch asks the plural endpoint for every return type in one request,
    /// windowed on Alpaca's record time. Verbatim live payload, fees and all.
    ///
    /// The type list is matched whole rather than by a wildcard, so dropping one from the constant
    /// fails here instead of silently narrowing what a sync asks for.
    #[tokio::test]
    async fn test_return_activities_are_fetched_as_one_windowed_request() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/account/activities")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("after".into(), "2026-08-07".into()),
                mockito::Matcher::UrlEncoded(
                    "activity_types".into(),
                    "DIV,DIVCGL,DIVCGS,DIVFEE,DIVFT,DIVNRA,DIVROC,DIVTW,DIVTXEX,CGD,INT,INTNRA,\
                     INTTW,FEE,CSD,CSW,JNLC"
                        .into(),
                ),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"20260814000000000::taf","activity_type":"FEE","activity_sub_type":"TAF",
                     "date":"2026-08-14","net_amount":"-0.07","status":"executed"},
                    {"id":"20260814000000000::reg","activity_type":"FEE","activity_sub_type":"REG",
                     "date":"2026-08-14","net_amount":"-0.31","status":"executed"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let fetched = client
            .fetch_activities_since(&ActivityType::windowed(), date(2026, 8, 7))
            .await
            .expect("activities must parse");

        assert_eq!(fetched.activities.len(), 2);
        let total: Decimal = fetched
            .activities
            .iter()
            .filter_map(|activity| activity.net_amount())
            .sum();
        assert_eq!(
            total,
            Decimal::new(-38, 2),
            "the fee family nets to a cost, not income"
        );
        assert!(
            fetched.activities.iter().all(|activity| activity
                .transaction_time()
                .with_timezone(&New_York)
                .date_naive()
                == date(2026, 8, 14)),
            "each row files under its own date, not the window it was fetched by"
        );
        mock.assert_async().await;
    }

    /// A fee has no symbol, so it can never reach a pair through `attribute`. Worth pinning: it is
    /// why the sync reports these as a session cost rather than a per-pair adjustment.
    #[tokio::test]
    async fn test_a_fee_carries_no_ticker_and_no_cash_flow() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"20260814000000000::cat","activity_type":"FEE","activity_sub_type":"CAT",
                     "date":"2026-08-14","net_amount":"-0.01","status":"executed"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let fetched = client
            .fetch_activities_since(&[ActivityType::parse("FEE")], date(2026, 8, 7))
            .await
            .expect("activities must parse");

        let fee = &fetched.activities[0];
        assert!(fee.ticker().is_none(), "a fee names no security");
        assert!(
            fee.signed_cash_flow().is_none(),
            "and carries no quantity or price to derive one from"
        );
        assert_eq!(fee.net_amount(), Some(Decimal::new(-1, 2)));
    }

    /// A transfer carries neither quantity nor price, so `net_amount` is the only field saying how
    /// much moved. Dropping it would store the deposit with no amount at all.
    ///
    /// The `JNLC` fixture is the shape a real paper account returns; `CSD` is what a bank deposit
    /// into a live account books as, and has no live coverage until real money moves.
    #[tokio::test]
    async fn test_transfer_activities_retain_their_net_amount() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"20260515000000000::journal","activity_type":"JNLC",
                     "date":"2026-05-15","net_amount":"20000","description":"",
                     "status":"executed","currency":"USD"},
                    {"id":"20260515000000001::deposit","activity_type":"CSD",
                     "date":"2026-05-15","net_amount":"10000.50","status":"executed"},
                    {"id":"20260515000000002::withdrawal","activity_type":"CSW",
                     "date":"2026-05-15","net_amount":"-2500.25","status":"executed"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let activities = client
            .fetch_activities(&ActivityType::CashJournal, date(2026, 5, 15))
            .await
            .expect("activities must parse");

        assert_eq!(activities.activities.len(), 3);
        assert_eq!(
            activities.activities[0].net_amount(),
            Some(Decimal::new(20000, 0))
        );
        assert_eq!(
            activities.activities[1].net_amount(),
            Some(Decimal::new(1000050, 2))
        );
        assert_eq!(
            activities.activities[2].net_amount(),
            Some(Decimal::new(-250025, 2)),
            "a withdrawal must keep its sign"
        );
        for activity in &activities.activities {
            assert_eq!(
                activity.signed_cash_flow(),
                None,
                "a transfer is not a two-sided trade and must never be attributed to a pair"
            );
        }
        mock.assert_async().await;
    }

    /// The fallback must not disturb activities that carry a real time of their own.
    #[tokio::test]
    async fn test_timed_activity_keeps_its_transaction_time() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"[{"id":"fill","activity_type":"FILL","transaction_time":"2026-05-15T13:45:00Z",
                     "symbol":"AAPL","side":"buy","qty":"1","price":"150.00"}]"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let activities = client
            .fetch_activities(&ActivityType::Fill, date(2026, 5, 15))
            .await
            .expect("activities must parse");

        assert_eq!(
            activities.activities[0].transaction_time(),
            "2026-05-15T13:45:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        mock.assert_async().await;
    }

    fn client(base_url: String) -> MarketDataClient {
        MarketDataClient::with_base_url(credentials(), base_url, DataFeed::Iex)
    }

    const FULL_SNAPSHOT: &str = r#"{
        "AAPL": {
            "latestTrade": {"t":"2026-06-10T15:59:00Z","p":201.5},
            "latestQuote": {"t":"2026-06-10T15:59:30Z","bp":201.0,"ap":202.0,"bs":10,"as":12},
            "minuteBar": {"t":"2026-06-10T15:59:00Z","o":201.1,"h":201.9,"l":201.0,"c":201.5,"v":15000.0,"vw":201.4,"n":120},
            "dailyBar": {"t":"2026-06-10T04:00:00Z","o":199.0,"h":202.5,"l":198.5,"c":201.5,"v":2500000.0,"vw":200.9,"n":18000},
            "prevDailyBar": {"t":"2026-06-09T04:00:00Z","o":197.0,"h":199.5,"l":196.5,"c":199.0,"v":2100000.0,"vw":198.2,"n":16000}
        }
    }"#;

    /// The whole payload must survive, not just the quote. The previous daily bar in particular is
    /// a free end-of-day backfill that the earlier quote-only parse discarded.
    #[tokio::test]
    async fn test_fetch_snapshots_retains_every_component() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/snapshots?symbols=AAPL&feed=iex")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(FULL_SNAPSHOT)
            .create_async()
            .await;

        let snapshots = client(server.url())
            .fetch_snapshots(&[ticker("AAPL")])
            .await
            .expect("snapshot must parse");

        assert_eq!(snapshots.snapshots.len(), 1);
        let snapshot = &snapshots.snapshots[0];
        assert_eq!(snapshot.ticker().as_str(), "AAPL");
        assert_eq!(snapshot.latest_trade().unwrap().price(), 201.5);
        assert_eq!(snapshot.latest_quote().unwrap().bid_price(), 201.0);
        mock.assert_async().await;
    }

    /// The midpoint leads the last trade, because it says where the market will transact now rather
    /// than where it already did.
    #[tokio::test]
    async fn test_reference_price_prefers_midpoint_over_last_trade() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/snapshots?symbols=AAPL&feed=iex")
            .with_status(200)
            .with_body(FULL_SNAPSHOT)
            .create_async()
            .await;

        let snapshots = client(server.url())
            .fetch_snapshots(&[ticker("AAPL")])
            .await
            .unwrap();

        assert_eq!(
            snapshots.snapshots[0]
                .reference_price_checked(snapshot_read_at(), QuoteLimits::default())
                .map(|checked| checked.price()),
            Some(201.5)
        );
        mock.assert_async().await;
    }

    /// The instant the snapshot fixtures are read at, a few seconds after the quotes they carry so
    /// the staleness guard does not refuse them.
    fn snapshot_read_at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-10T16:00:05Z")
            .expect("valid instant")
            .with_timezone(&Utc)
    }

    /// A quote missing a side must be dropped, not defaulted to zero. The fallback to the last
    /// trade is what keeps the symbol priced at all.
    #[tokio::test]
    async fn test_partial_quote_is_dropped_and_falls_back_to_trade() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/snapshots?symbols=AAPL&feed=iex")
            .with_status(200)
            .with_body(
                r#"{"AAPL":{"latestTrade":{"t":"2026-06-10T15:59:00Z","p":150.0},
                    "latestQuote":{"t":"2026-06-10T15:59:30Z","bp":149.0,"bs":10,"as":12}}}"#,
            )
            .create_async()
            .await;

        let snapshots = client(server.url())
            .fetch_snapshots(&[ticker("AAPL")])
            .await
            .unwrap();

        assert!(
            snapshots.snapshots[0].latest_quote().is_none(),
            "half a book is no book"
        );
        assert_eq!(
            snapshots.snapshots[0]
                .reference_price_checked(snapshot_read_at(), QuoteLimits::default())
                .map(|checked| checked.price()),
            Some(150.0)
        );
        mock.assert_async().await;
    }

    /// The trade's `t` was the one timestamp the snapshot parse dropped, so a fallback price could
    /// be any age and the record could not say. Every other component already kept its own.
    #[tokio::test]
    async fn test_a_snapshot_keeps_when_the_last_trade_printed() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/snapshots?symbols=AAPL&feed=iex")
            .with_status(200)
            .with_body(FULL_SNAPSHOT)
            .create_async()
            .await;

        let snapshots = client(server.url())
            .fetch_snapshots(&[ticker("AAPL")])
            .await
            .unwrap();

        assert_eq!(
            snapshots.snapshots[0].latest_trade().unwrap().timestamp(),
            "2026-06-10T15:59:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        mock.assert_async().await;
    }

    /// An undated trade costs the symbol its fallback price rather than supplying an unjudgeable
    /// one, which is the same call [`QuotePayload::into_equity_quote`] makes on a partial book.
    #[tokio::test]
    async fn test_a_trade_that_cannot_be_dated_or_priced_is_dropped() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                mockito::Matcher::Regex(r"^/v2/stocks/snapshots".into()),
            )
            .with_status(200)
            .with_body(
                r#"{"AAPL":{"latestTrade":{"p":150.0}},
                    "MSFT":{"latestTrade":{"t":"2026-06-10T15:59:00Z","p":0.0}}}"#,
            )
            .create_async()
            .await;

        let snapshots = client(server.url())
            .fetch_snapshots(&[ticker("AAPL"), ticker("MSFT")])
            .await
            .unwrap();

        for snapshot in &snapshots.snapshots {
            assert!(snapshot.latest_trade().is_none());
            assert_eq!(
                snapshot
                    .reference_price_checked(snapshot_read_at(), QuoteLimits::default())
                    .map(|checked| checked.price()),
                None
            );
        }
        mock.assert_async().await;
    }

    /// The `AER` reading from 2026-08-12: a book wide enough that its midpoint sat seven percent
    /// below where the symbol filled all session. The last trade is one of that day's real fills.
    fn wide_book_snapshot(quoted_at: DateTime<Utc>) -> Snapshot {
        let ticker = Ticker::new("AER").unwrap();
        Snapshot {
            latest_quote: Some(
                EquityQuote::new(ticker.clone(), quoted_at, 128.0, 150.86, 10, 12).unwrap(),
            ),
            latest_trade: Some(EquityTrade::new(ticker.clone(), quoted_at, 150.60).unwrap()),
            ticker,
        }
    }

    fn sound_book_snapshot(quoted_at: DateTime<Utc>) -> Snapshot {
        let ticker = Ticker::new("AER").unwrap();
        Snapshot {
            latest_quote: Some(
                EquityQuote::new(ticker.clone(), quoted_at, 150.58, 150.62, 10, 12).unwrap(),
            ),
            latest_trade: Some(EquityTrade::new(ticker.clone(), quoted_at, 150.60).unwrap()),
            ticker,
        }
    }

    #[test]
    fn test_a_wide_book_is_refused_and_falls_back_to_the_last_trade() {
        let now = Utc.with_ymd_and_hms(2026, 8, 12, 14, 40, 0).unwrap();
        let checked = wide_book_snapshot(now)
            .reference_price_checked(now, QuoteLimits::default())
            .unwrap();

        assert_eq!(checked.price(), 150.60);
        assert_eq!(checked.source(), PriceSource::LastTrade);
        // The payload, not only the variant: these numbers are the entire reason the rejection is
        // recorded, and a wrong divisor or a swapped pair reads as a pass against `matches!`.
        let Some(QuoteRejection::Wide {
            relative_spread,
            limit,
        }) = checked.rejection()
        else {
            panic!("a wide book must be refused as wide");
        };
        assert!((relative_spread - (150.86 - 128.0) / 139.43).abs() < 1e-9);
        assert_eq!(limit, MAXIMUM_RELATIVE_QUOTE_SPREAD);
    }

    #[test]
    fn test_a_stale_quote_is_refused_however_tight_the_book() {
        let now = Utc.with_ymd_and_hms(2026, 8, 12, 14, 40, 0).unwrap();
        let quoted_at = now - chrono::Duration::seconds(MAXIMUM_QUOTE_AGE_SECONDS + 1);
        let checked = sound_book_snapshot(quoted_at)
            .reference_price_checked(now, QuoteLimits::default())
            .unwrap();

        assert_eq!(checked.source(), PriceSource::LastTrade);
        let Some(QuoteRejection::Stale {
            age_seconds,
            limit_seconds,
        }) = checked.rejection()
        else {
            panic!("a stale quote must be refused as stale");
        };
        assert_eq!(age_seconds, MAXIMUM_QUOTE_AGE_SECONDS + 1);
        assert_eq!(limit_seconds, MAXIMUM_QUOTE_AGE_SECONDS);
    }

    /// Age is signed, so a quote stamped after `now` reads as negative and would slip past a
    /// comparison against the limit alone. Feed clock skew is ordinary and a future-dated book
    /// describes the current market no better than a stale one.
    #[test]
    fn test_a_future_dated_quote_is_refused_as_stale() {
        let now = Utc.with_ymd_and_hms(2026, 8, 12, 14, 40, 0).unwrap();
        let quoted_at = now + chrono::Duration::seconds(MAXIMUM_QUOTE_AGE_SECONDS + 1);
        let checked = sound_book_snapshot(quoted_at)
            .reference_price_checked(now, QuoteLimits::default())
            .unwrap();

        assert_eq!(checked.source(), PriceSource::LastTrade);
        let Some(QuoteRejection::Stale { age_seconds, .. }) = checked.rejection() else {
            panic!("a future-dated quote must be refused as stale");
        };
        assert_eq!(
            age_seconds,
            -(MAXIMUM_QUOTE_AGE_SECONDS + 1),
            "the sign is kept so the log shows a future stamp rather than disguising it"
        );
    }

    /// The rendered forms reach the structured logs, where a swapped pair would be read as fact.
    #[test]
    fn test_a_rejection_renders_its_own_numbers() {
        let stale = QuoteRejection::Stale {
            age_seconds: 90,
            limit_seconds: 60,
        };
        assert_eq!(
            stale.to_string(),
            "the quote is 90 seconds old, past the 60 second limit"
        );
        assert_eq!(stale.as_str(), "stale_quote");

        let wide = QuoteRejection::Wide {
            relative_spread: 0.1639,
            limit: 0.01,
        };
        assert_eq!(
            wide.to_string(),
            "the book is 0.1639 wide, past the 0.0100 limit"
        );
        assert_eq!(wide.as_str(), "wide_quote");
    }

    #[test]
    fn test_a_sound_quote_is_taken_and_records_no_rejection() {
        let now = Utc.with_ymd_and_hms(2026, 8, 12, 14, 40, 0).unwrap();
        let checked = sound_book_snapshot(now)
            .reference_price_checked(now, QuoteLimits::default())
            .unwrap();

        assert!((checked.price() - 150.60).abs() < 1e-9);
        assert_eq!(checked.source(), PriceSource::QuoteMidpoint);
        assert_eq!(checked.rejection(), None);
    }

    /// The whole point of refusing a book is that the symbol goes unpriced rather than priced
    /// wrongly, so a refusal with nothing behind it must not fall through to the midpoint.
    #[test]
    fn test_a_refused_quote_with_no_last_trade_leaves_the_symbol_unpriced() {
        let now = Utc.with_ymd_and_hms(2026, 8, 12, 14, 40, 0).unwrap();
        let mut snapshot = wide_book_snapshot(now);
        snapshot.latest_trade = None;

        assert_eq!(
            snapshot.reference_price_checked(now, QuoteLimits::default()),
            None
        );
    }

    /// No quote at all is not a rejection — it is the ordinary fallback the unguarded path already
    /// took, and recording it as a refusal would overstate how often the guard fired.
    #[test]
    fn test_a_missing_quote_falls_back_without_a_rejection() {
        let now = Utc.with_ymd_and_hms(2026, 8, 12, 14, 40, 0).unwrap();
        let mut snapshot = wide_book_snapshot(now);
        snapshot.latest_quote = None;

        let checked = snapshot
            .reference_price_checked(now, QuoteLimits::default())
            .unwrap();
        assert_eq!(checked.source(), PriceSource::LastTrade);
        assert_eq!(checked.rejection(), None);
    }

    /// [`QuoteLimits`] describes a book and nothing describes a trade, so the fallback price is
    /// taken at any age. Pinned here so it reads as a decision rather than an oversight.
    #[test]
    fn test_an_hours_old_trade_is_still_taken_as_the_fallback_price() {
        let now = Utc.with_ymd_and_hms(2026, 8, 12, 14, 40, 0).unwrap();
        let mut snapshot = wide_book_snapshot(now);
        let ticker = snapshot.ticker().clone();
        snapshot.latest_trade =
            Some(EquityTrade::new(ticker, now - chrono::Duration::hours(6), 150.60).unwrap());

        let checked = snapshot
            .reference_price_checked(now, QuoteLimits::default())
            .unwrap();
        assert_eq!(checked.source(), PriceSource::LastTrade);
        assert!((checked.price() - 150.60).abs() < 1e-9);
    }

    #[test]
    fn test_quote_limits_reject_bounds_that_measure_nothing() {
        assert!(QuoteLimits::new(chrono::Duration::zero(), 0.01).is_none());
        assert!(QuoteLimits::new(chrono::Duration::seconds(-1), 0.01).is_none());
        assert!(QuoteLimits::new(chrono::Duration::seconds(60), 0.0).is_none());
        assert!(QuoteLimits::new(chrono::Duration::seconds(60), f64::NAN).is_none());
        assert!(QuoteLimits::new(chrono::Duration::seconds(60), 0.01).is_some());
    }

    #[tokio::test]
    async fn test_empty_symbol_list_makes_no_request() {
        let server = mockito::Server::new_async().await;
        let snapshots = client(server.url()).fetch_snapshots(&[]).await.unwrap();
        assert!(snapshots.snapshots.is_empty());
    }

    /// Field names and row shapes copied from live responses, including the placeholder rows. The
    /// splits feed taught that a payload which parses in a test and not against the feed is the
    /// failure mode worth spending a fixture on.
    const CORPORATE_ACTIONS_PAGE: &str = r#"{
        "corporate_actions": {
            "name_changes": [
                {"id":"n1","old_cusip":"03209R103","old_symbol":"RNA",
                 "new_cusip":"05370B107","new_symbol":"RNAM","process_date":"2026-02-26"},
                {"id":"n2","old_cusip":"185CNT011","old_symbol":"185CNT011",
                 "new_cusip":"18506U302","new_symbol":"18506U302","process_date":"2026-02-05"},
                {"id":"n3","old_cusip":"00846U101","old_symbol":"INDV",
                 "new_cusip":"00846U101","new_symbol":"INDV","process_date":"2026-01-26"}
            ],
            "spin_offs": [
                {"ex_date":"2026-06-01","id":"s1","new_cusip":"31428X106","new_rate":0.5,
                 "new_symbol":"FDXF","process_date":"2026-06-01","source_cusip":"31428X106",
                 "source_rate":1,"source_symbol":"FDX"},
                {"ex_date":"2026-01-27","id":"s2","new_cusip":"898920103","new_rate":0.072996,
                 "new_symbol":"HURA","process_date":"2026-01-27","source_cusip":"494ESC015",
                 "source_rate":1,"source_symbol":"494ESC015"}
            ],
            "rights_distributions": [
                {"ex_date":"2026-02-10","expiration_date":"2026-02-27","id":"r1",
                 "new_cusip":"009RGT010","new_symbol":"009RGT010","payable_date":"2026-02-19",
                 "process_date":"2026-02-19","rate":1,"record_date":"2026-02-10",
                 "source_cusip":"00901B303","source_symbol":"AIM"}
            ],
            "unit_splits": [
                {"alternate_cusip":"G0679A118","alternate_rate":0.1667,"alternate_symbol":"ACAAW",
                 "effective_date":"2026-04-10","id":"u1","new_cusip":"G0679A100","new_rate":1,
                 "new_symbol":"ACAA","old_cusip":"G0679A126","old_rate":1,"old_symbol":"ACAAU",
                 "process_date":"2026-04-10"}
            ],
            "reorganizations": [
                {"cash_rate":0.1,"cusip":"89531P105","effective_date":"2026-05-29","id":"o1",
                 "payable_date":"2026-06-02","process_date":"2026-06-02","symbol":"TRXO"},
                {"cash_rate":0.1,"cusip":"004ESC018","effective_date":"2026-05-06","id":"o2",
                 "payable_date":"2026-05-11","process_date":"2026-05-11","symbol":"004ESC018"}
            ]
        }
    }"#;

    async fn boundaries_from(body: &str) -> Vec<SeriesBoundary> {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await;

        let fetched = client(server.url())
            .fetch_corporate_actions(date(2026, 1, 1), date(2026, 8, 14))
            .await
            .expect("the page must parse");
        mock.assert_async().await;
        fetched
    }

    fn boundary_for<'a>(
        boundaries: &'a [SeriesBoundary],
        ticker: &str,
    ) -> Option<&'a SeriesBoundary> {
        boundaries
            .iter()
            .find(|boundary| boundary.ticker().as_str() == ticker)
    }

    #[tokio::test]
    async fn test_each_category_becomes_the_boundary_it_describes() {
        let boundaries = boundaries_from(CORPORATE_ACTIONS_PAGE).await;

        assert_eq!(
            boundary_for(&boundaries, "RNA").map(SeriesBoundary::reason),
            Some(&BoundaryReason::Renamed {
                to: Ticker::new("RNAM").unwrap()
            })
        );
        assert_eq!(
            boundary_for(&boundaries, "FDX").map(SeriesBoundary::reason),
            Some(&BoundaryReason::SpunOff {
                spin_off_company: Ticker::new("FDXF").unwrap()
            }),
            "a spinoff names the company whose shares were distributed"
        );
        assert_eq!(
            boundary_for(&boundaries, "AIM").map(SeriesBoundary::reason),
            Some(&BoundaryReason::RightsDistributed)
        );
        assert_eq!(
            boundary_for(&boundaries, "ACAAU").map(SeriesBoundary::reason),
            Some(&BoundaryReason::UnitSeparated),
            "a unit separation names no successor: its price steps, so history cannot follow it"
        );
        assert_eq!(
            boundary_for(&boundaries, "TRXO").map(SeriesBoundary::reason),
            Some(&BoundaryReason::Reorganized)
        );
    }

    /// A spinoff bounds the session its price steps in, which is the ex-date rather than the date
    /// the transfer agent processed it.
    #[tokio::test]
    async fn test_a_spinoff_is_bounded_at_its_ex_date() {
        let boundaries = boundaries_from(CORPORATE_ACTIONS_PAGE).await;

        assert_eq!(
            boundary_for(&boundaries, "FDX").map(SeriesBoundary::date),
            Some(SessionDate::from_date(date(2026, 6, 1)))
        );
    }

    /// The single most common row in the feed, and one that bounds nothing: the company's name or
    /// CUSIP changed under an unchanged ticker. Admitting it would truncate every window spanning
    /// a date on which nothing happened to the price.
    #[tokio::test]
    async fn test_a_rename_that_changes_no_symbol_is_not_a_boundary() {
        let boundaries = boundaries_from(CORPORATE_ACTIONS_PAGE).await;

        assert!(
            boundary_for(&boundaries, "INDV").is_none(),
            "INDV renamed to itself, so its series is continuous"
        );
    }

    /// The feed puts CUSIPs in symbol fields for entities that never traded under a ticker. They
    /// parse as JSON and mean nothing to a bar archive keyed by symbol.
    #[tokio::test]
    async fn test_placeholder_symbols_are_dropped() {
        let boundaries = boundaries_from(CORPORATE_ACTIONS_PAGE).await;

        assert_eq!(
            boundaries.len(),
            5,
            "RNA, FDX, AIM, ACAAU and TRXO survive; the three placeholder rows and INDV do not"
        );
        assert!(boundaries
            .iter()
            .all(|boundary| !boundary.ticker().as_str().contains(char::is_numeric)));
    }

    /// A page carrying only some categories is the normal case, not a malformed response.
    #[tokio::test]
    async fn test_a_page_missing_every_category_is_empty_rather_than_an_error() {
        assert!(boundaries_from(r#"{"corporate_actions":{}}"#)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn test_pagination_follows_the_token_to_the_last_page() {
        let mut server = mockito::Server::new_async().await;
        // Registered before the tokenless page, because mockito serves the first mock whose
        // matchers accept the request and the fallback below accepts any query at all.
        let second = server
            .mock("GET", mockito::Matcher::Any)
            .match_query(mockito::Matcher::UrlEncoded(
                "page_token".into(),
                "page-two".into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"corporate_actions":{"name_changes":[
                    {"id":"n2","old_symbol":"SPCX","new_symbol":"SPCK","process_date":"2026-04-07"}
                ]}}"#,
            )
            .create_async()
            .await;
        let first = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"corporate_actions":{"name_changes":[
                    {"id":"n1","old_symbol":"RNA","new_symbol":"RNAM","process_date":"2026-02-26"}
                ]},"next_page_token":"page-two"}"#,
            )
            .create_async()
            .await;

        let boundaries = client(server.url())
            .fetch_corporate_actions(date(2026, 1, 1), date(2026, 8, 14))
            .await
            .expect("both pages must parse");

        assert_eq!(boundaries.len(), 2, "rows from both pages are kept");
        assert!(boundary_for(&boundaries, "SPCX").is_some());
        first.assert_async().await;
        second.assert_async().await;
    }

    fn quote_window() -> (DateTime<Utc>, DateTime<Utc>) {
        (
            Utc.with_ymd_and_hms(2026, 8, 20, 13, 30, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 8, 20, 20, 0, 0).unwrap(),
        )
    }

    /// Collects what the fold would have seen, which no production caller does — the whole point of
    /// the streaming signature is that a name-session's ticks never exist all at once.
    async fn collect_quotes(base_url: String) -> (Vec<QuoteTick>, Result<QuoteFetch, ClientError>) {
        let (start, end) = quote_window();
        let mut ticks = Vec::new();
        let fetch = client(base_url)
            .fetch_quotes(&ticker("AAPL"), start, end, date(2026, 8, 20), |tick| {
                ticks.push(tick)
            })
            .await;
        (ticks, fetch)
    }

    #[tokio::test]
    async fn test_quote_pagination_folds_every_page_in_order() {
        let mut server = mockito::Server::new_async().await;
        let second = server
            .mock("GET", mockito::Matcher::Any)
            .match_query(mockito::Matcher::UrlEncoded(
                "page_token".into(),
                "page-two".into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"quotes":{"AAPL":[
                    {"t":"2026-08-20T13:30:02Z","bp":100.02,"ap":100.06,"bs":3,"as":4}
                ]}}"#,
            )
            .create_async()
            .await;
        let first = server
            .mock("GET", mockito::Matcher::Any)
            // Pinned because the fold weighs each quote by the time until the next one, so a
            // descending page would weigh every tick at zero and read as a session with no book.
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("sort".into(), "asc".into()),
                mockito::Matcher::UrlEncoded("feed".into(), "iex".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"quotes":{"AAPL":[
                    {"t":"2026-08-20T13:30:00Z","bp":100.00,"ap":100.05,"bs":1,"as":2}
                ]},"next_page_token":"page-two"}"#,
            )
            .create_async()
            .await;

        let (ticks, fetch) = collect_quotes(server.url()).await;
        let fetch = fetch.expect("both pages must parse");

        assert_eq!(fetch.received, 2);
        assert_eq!(fetch.rejected, 0);
        assert_eq!(fetch.pages, 2);
        assert_eq!(ticks.len(), 2);
        assert!(ticks[0].timestamp() < ticks[1].timestamp());
        assert_eq!(ticks[0].bid_size(), 1);
        first.assert_async().await;
        second.assert_async().await;
    }

    /// The default resolves a ticker through today's symbol table, so a name reassigned since the
    /// session would answer with the company holding it now rather than the one that traded.
    #[tokio::test]
    async fn test_quotes_are_resolved_as_of_the_session_rather_than_today() {
        let mut server = mockito::Server::new_async().await;
        let page = server
            .mock("GET", mockito::Matcher::Any)
            .match_query(mockito::Matcher::UrlEncoded(
                "asof".into(),
                "2026-08-20".into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"quotes":{"AAPL":[
                    {"t":"2026-08-20T13:30:00Z","bp":100.00,"ap":100.05,"bs":1,"as":2}
                ]}}"#,
            )
            .create_async()
            .await;

        let (ticks, fetch) = collect_quotes(server.url()).await;

        assert_eq!(fetch.expect("the page must parse").received, 1);
        assert_eq!(ticks.len(), 1);
        // The mock only answers a request carrying the session's own date, so reaching this line
        // is the assertion; `assert_async` names the failure when it does not.
        page.assert_async().await;
    }

    /// A crossed consolidated book is ordinary around the open — 20 of AAPL's first 30,126 quotes
    /// on 2026-08-20 were crossed — so these are counted rather than raised, and never reach a fold
    /// that would read a negative spread off them.
    #[tokio::test]
    async fn test_books_no_spread_can_be_read_off_are_counted_not_delivered() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"quotes":{"AAPL":[
                    {"t":"2026-08-20T13:30:00Z","bp":100.10,"ap":100.05,"bs":1,"as":2},
                    {"t":"2026-08-20T13:30:01Z","bp":0,"ap":100.05,"bs":1,"as":2},
                    {"t":"2026-08-20T13:30:02Z","bp":100.00,"ap":100.05,"bs":-1,"as":2},
                    {"bp":100.00,"ap":100.05,"bs":1,"as":2},
                    {"t":"2026-08-20T13:30:04Z","bp":100.00,"ap":100.05,"bs":1,"as":2}
                ]}}"#,
            )
            .create_async()
            .await;

        let (ticks, fetch) = collect_quotes(server.url()).await;
        let fetch = fetch.expect("a page of mostly unusable books still parses");

        assert_eq!(fetch.received, 5);
        assert_eq!(fetch.rejected, 4, "crossed, zero, signed size, undated");
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].spread(), 100.05 - 100.00);
        mock.assert_async().await;
    }

    /// The endpoint omits `quotes` entirely rather than returning it empty, which every session
    /// before a name listed produces.
    #[tokio::test]
    async fn test_a_window_with_no_quotes_is_an_empty_fold_rather_than_an_error() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"quotes":null,"next_page_token":null}"#)
            .create_async()
            .await;

        let (ticks, fetch) = collect_quotes(server.url()).await;
        let fetch = fetch.expect("an empty window is an answer");

        assert_eq!(
            fetch,
            QuoteFetch {
                received: 0,
                rejected: 0,
                pages: 1,
                retries: 0
            }
        );
        assert!(ticks.is_empty());
        mock.assert_async().await;
    }

    /// The first real run lost twelve names to `error decoding response body` — a connection
    /// dropped part-way through a page — because this arrived as [`ClientError::Parse`], which the
    /// archive treats as permanent and never retries. It is a transport failure and must say so.
    #[tokio::test]
    async fn test_a_body_that_does_not_arrive_intact_is_a_transport_failure() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"quotes":{"AAPL":[{"t":"2026-08-20T13:30:00Z","#)
            // Four, spelled out. Taking it from `QUOTES_PAGE_ATTEMPTS` would let the constant drop
            // to one and this test would still pass while proving no retry happened at all.
            .expect(4)
            .create_async()
            .await;

        let (_, fetch) = collect_quotes(server.url()).await;

        assert!(
            matches!(fetch, Err(ClientError::Request(_))),
            "a truncated body is transport, not content: {fetch:?}"
        );
        // Exhausting the page's attempts must still report the transport failure itself, so the
        // symbol-level retry above it can act on it.
        mock.assert_async().await;
    }

    #[test]
    fn test_only_a_throttle_or_a_server_fault_is_worth_asking_again_for() {
        let api = |status| ClientError::Api {
            status,
            body: String::new(),
        };
        assert!(api(429).is_transient(), "throttled");
        assert!(api(503).is_transient(), "server fault");
        assert!(!api(403).is_transient(), "an entitlement");
        assert!(!api(422).is_transient(), "a malformed window");
        assert!(!ClientError::Parse("page limit".to_string()).is_transient());
    }

    /// AAPL is 118 pages over one session, so restarting the symbol to recover one dropped
    /// connection discards 117 pages. The token names the page, so resuming at it is exact — and
    /// the ticks either side of the failure must both survive.
    #[tokio::test]
    async fn test_a_dropped_page_is_retried_without_losing_the_pages_before_it() {
        let mut server = mockito::Server::new_async().await;
        let second = server
            .mock("GET", mockito::Matcher::Any)
            .match_query(mockito::Matcher::UrlEncoded(
                "page_token".into(),
                "page-two".into(),
            ))
            .with_status(500)
            .with_body("upstream fell over")
            .expect(1)
            .create_async()
            .await;
        let recovered = server
            .mock("GET", mockito::Matcher::Any)
            .match_query(mockito::Matcher::UrlEncoded(
                "page_token".into(),
                "page-two".into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"quotes":{"AAPL":[
                    {"t":"2026-08-20T13:30:02Z","bp":100.02,"ap":100.06,"bs":3,"as":4}
                ]}}"#,
            )
            .create_async()
            .await;
        let first = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"quotes":{"AAPL":[
                    {"t":"2026-08-20T13:30:00Z","bp":100.00,"ap":100.05,"bs":1,"as":2}
                ]},"next_page_token":"page-two"}"#,
            )
            .create_async()
            .await;

        let (ticks, fetch) = collect_quotes(server.url()).await;
        let fetch = fetch.expect("a retried page must not fail the symbol");

        assert_eq!(fetch.retries, 1, "the 500 was retried");
        assert_eq!(fetch.pages, 2, "a retry is not a second page");
        assert_eq!(ticks.len(), 2, "page one's tick survived the failure");
        first.assert_async().await;
        second.assert_async().await;
        recovered.assert_async().await;
    }

    #[test]
    fn test_a_locked_book_is_usable_and_a_crossed_one_is_not() {
        let at = Utc.with_ymd_and_hms(2026, 8, 20, 13, 30, 0).unwrap();
        let locked = QuoteTick::new(at, 100.0, 100.0, 1, 1).expect("a locked book is legal");
        assert_eq!(locked.spread(), 0.0);
        assert_eq!(locked.mid_price(), 100.0);
        assert_eq!(QuoteTick::new(at, 100.1, 100.0, 1, 1), None, "crossed");
        assert_eq!(
            QuoteTick::new(at, f64::NAN, 100.0, 1, 1),
            None,
            "non-finite"
        );
    }
}
