//! The Alpaca integration: credentials, the trading API, and the market data API.
//!
//! Two services behind one set of credentials: the trading API answers what the market *is* — the
//! clock, calendar, assets, orders, positions — and the market data API what things *cost*. They
//! share authentication and an error type and nothing else, which is why one file holds both.
//!
//! Deliberately thin, since Alpaca is the source of truth for fills and balances. Deserialization
//! surfaces ambiguity rather than defaulting it — a snapshot with no quote, a calendar day with no
//! duration — because a missing value and a zero one are indistinguishable after the fact. What to
//! do about a missing value is left to the consumer: [`Snapshot::reference_price`], for one,
//! deliberately falls back to the last trade.

use std::collections::HashSet;
use std::num::NonZeroU32;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::common::types::{BarInterval, Dollars, EquityBar, EquityQuote, Ticker};

// --------------------------------------------------------------------------
// Shared configuration and errors
// --------------------------------------------------------------------------

/// Base URL for paper trading (sandbox environment).
const PAPER_BASE_URL: &str = "https://paper-api.alpaca.markets";

/// Base URL for live trading.
const LIVE_BASE_URL: &str = "https://api.alpaca.markets";

/// Base URL for the Alpaca market data API.
const DATA_BASE_URL: &str = "https://data.alpaca.markets";

/// Header name for the Alpaca API key ID.
const HEADER_KEY_ID: &str = "APCA-API-KEY-ID";

/// Header name for the Alpaca API secret key.
const HEADER_SECRET_KEY: &str = "APCA-API-SECRET-KEY";

/// Failures reaching or interpreting an Alpaca endpoint.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The request never produced a response: connection refused, timed out, TLS failure.
    #[error("Alpaca request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// Alpaca answered with a non-success status.
    #[error("Alpaca returned status {status}: {body}")]
    Api { status: u16, body: String },
    /// Alpaca answered successfully with something that could not be interpreted.
    #[error("Alpaca response could not be parsed: {0}")]
    Parse(String),
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

// --------------------------------------------------------------------------
// Credentials
// --------------------------------------------------------------------------

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
    secret: String,
}

impl AlpacaCredentials {
    /// Constructs `AlpacaCredentials` from explicit field values.
    ///
    /// Rejects empty values: an empty key reaches Alpaca as a 403, which reads like a permissions
    /// problem rather than the configuration one it is.
    pub fn new(key_id: String, secret: String) -> Result<Self, CredentialsError> {
        if key_id.is_empty() {
            return Err(CredentialsError::Empty { field: "key_id" });
        }
        if secret.is_empty() {
            return Err(CredentialsError::Empty { field: "secret" });
        }
        Ok(Self { key_id, secret })
    }

    /// Reads `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET` from the environment.
    pub fn from_env() -> Result<Self, CredentialsError> {
        let key_id = std::env::var("ALPACA_API_KEY_ID").map_err(|_| CredentialsError::Missing {
            variable: "ALPACA_API_KEY_ID",
        })?;
        let secret = std::env::var("ALPACA_API_SECRET").map_err(|_| CredentialsError::Missing {
            variable: "ALPACA_API_SECRET",
        })?;
        Self::new(key_id, secret)
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }
}

// --------------------------------------------------------------------------
// Trading API: clock, calendar, asset universe
// --------------------------------------------------------------------------

/// The trading session as Alpaca's clock reports it right now.
///
/// `next_close` reflects early-close days, so a caller gets the real session end without consulting
/// a local holiday table.
#[derive(Debug, Clone, PartialEq)]
pub struct ClockSnapshot {
    is_open: bool,
    next_open: DateTime<Utc>,
    next_close: DateTime<Utc>,
}

impl ClockSnapshot {
    pub fn new(is_open: bool, next_open: DateTime<Utc>, next_close: DateTime<Utc>) -> Self {
        Self {
            is_open,
            next_open,
            next_close,
        }
    }

    pub fn is_open(&self) -> bool {
        self.is_open
    }

    pub fn next_open(&self) -> DateTime<Utc> {
        self.next_open
    }

    pub fn next_close(&self) -> DateTime<Utc> {
        self.next_close
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
    tradable: HashSet<String>,
    shortable: HashSet<String>,
}

impl TradableAssets {
    /// Constructs from explicit sets, for tests and for the cache warm path.
    pub fn from_sets(tradable: HashSet<String>, shortable: HashSet<String>) -> Self {
        Self {
            tradable,
            shortable,
        }
    }

    pub fn is_tradable(&self, symbol: &str) -> bool {
        self.tradable.contains(symbol)
    }

    pub fn is_shortable(&self, symbol: &str) -> bool {
        self.shortable.contains(symbol)
    }

    pub fn tradable_count(&self) -> usize {
        self.tradable.len()
    }

    pub fn shortable_count(&self) -> usize {
        self.shortable.len()
    }

    /// Every symbol that can be traded, sorted so callers get a stable universe order.
    pub fn tradable_symbols(&self) -> Vec<String> {
        let mut symbols: Vec<String> = self.tradable.iter().cloned().collect();
        symbols.sort();
        symbols
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
            .header(HEADER_SECRET_KEY, self.credentials.secret())
    }

    /// Fetches the current trading session from the clock endpoint.
    pub async fn fetch_clock(&self) -> Result<ClockSnapshot, ClientError> {
        let url = format!("{}/v2/clock", self.base_url);
        let response = error_for_status(self.get(&url).send().await?).await?;
        let clock: ClockResponse = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse clock response: {error}"))
        })?;

        Ok(ClockSnapshot::new(
            clock.is_open,
            clock.next_open,
            clock.next_close,
        ))
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

        for asset in assets {
            // Checked here as well as in the query string. The `status=active` parameter already
            // filters server-side, so this rejects nothing today -- it exists so a change to the
            // URL cannot silently admit delisted or suspended symbols into the universe.
            if asset.status.as_deref() != Some("active") {
                inactive_rejected += 1;
                continue;
            }
            if asset.tradable.unwrap_or(false) {
                tradable.insert(asset.symbol.clone());
                if asset.shortable.unwrap_or(false) && asset.easy_to_borrow.unwrap_or(false) {
                    shortable.insert(asset.symbol);
                }
            }
        }

        info!(
            tradable = tradable.len(),
            shortable = shortable.len(),
            inactive_rejected,
            "Tradable asset universe fetched"
        );
        Ok(TradableAssets {
            tradable,
            shortable,
        })
    }
}

#[derive(Deserialize)]
struct ClockResponse {
    is_open: bool,
    next_open: DateTime<Utc>,
    next_close: DateTime<Utc>,
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

// --------------------------------------------------------------------------
// Trading API: orders, positions, account, activities
// --------------------------------------------------------------------------

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
    OpenShort { ticker: Ticker, shares: NonZeroU32 },
}

impl OrderIntent {
    /// The symbol the order is for.
    pub fn ticker(&self) -> &Ticker {
        match self {
            OrderIntent::OpenLong { ticker, .. } | OrderIntent::OpenShort { ticker, .. } => ticker,
        }
    }

    /// Which side of the book this order takes, as Alpaca names it.
    pub fn side(&self) -> &'static str {
        match self {
            OrderIntent::OpenLong { .. } => "buy",
            OrderIntent::OpenShort { .. } => "sell",
        }
    }

    /// Builds the request body Alpaca expects.
    ///
    /// `position_intent` is sent explicitly rather than left to Alpaca's inference. Without it a
    /// sell against an existing long is read as a close rather than a short, which for a strategy
    /// that holds both sides of a pair is the difference between opening a hedge and unwinding one.
    fn to_request(&self, client_order_id: &str) -> OrderRequest {
        match self {
            OrderIntent::OpenLong { ticker, notional } => OrderRequest {
                symbol: ticker.as_str().to_string(),
                side: "buy",
                order_type: "market",
                time_in_force: "day",
                notional: Some(format!("{:.2}", notional.value())),
                qty: None,
                position_intent: "buy_to_open",
                client_order_id: client_order_id.to_string(),
            },
            OrderIntent::OpenShort { ticker, shares } => OrderRequest {
                symbol: ticker.as_str().to_string(),
                side: "sell",
                order_type: "market",
                time_in_force: "day",
                notional: None,
                qty: Some(shares.get()),
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
#[derive(Debug, Clone, PartialEq)]
pub enum OrderState {
    /// Alpaca still has it: accepted, queued, or partially filled. Ask again.
    Working { filled_shares: f64 },
    /// Terminal and completely filled.
    Filled {
        filled_shares: f64,
        average_price: f64,
    },
    /// Terminal without a complete fill: canceled, expired, rejected, or done for the day.
    ///
    /// `filled_shares` is non-zero when a partial fill was terminated, which is the case that makes
    /// this more than a failure flag — those shares are held and have to be unwound.
    Abandoned { status: String, filled_shares: f64 },
}

impl OrderState {
    /// Whether Alpaca is finished with this order, however it ended.
    pub fn is_terminal(&self) -> bool {
        match self {
            OrderState::Working { .. } => false,
            OrderState::Filled { .. } | OrderState::Abandoned { .. } => true,
        }
    }

    /// Shares Alpaca reports as filled, whatever state the order reached.
    pub fn filled_shares(&self) -> f64 {
        match self {
            OrderState::Working { filled_shares }
            | OrderState::Filled { filled_shares, .. }
            | OrderState::Abandoned { filled_shares, .. } => *filled_shares,
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
    /// Parses Alpaca's `side` field. Returns `None` for anything else.
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "long" => Some(PositionSide::Long),
            "short" => Some(PositionSide::Short),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
        }
    }
}

/// One open position as Alpaca reports it.
///
/// `shares` is the absolute count; the direction lives in `side`. Alpaca signs the quantity for
/// shorts, and carrying a signed count alongside a side means two representations of the same fact
/// that can disagree.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    ticker: Ticker,
    side: PositionSide,
    shares: f64,
    market_value: f64,
    unrealized_profit_and_loss: f64,
}

impl Position {
    pub fn new(
        ticker: Ticker,
        side: PositionSide,
        shares: f64,
        market_value: f64,
        unrealized_profit_and_loss: f64,
    ) -> Self {
        Self {
            ticker,
            side,
            shares: shares.abs(),
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

    pub fn shares(&self) -> f64 {
        self.shares
    }

    pub fn market_value(&self) -> f64 {
        self.market_value
    }

    pub fn unrealized_profit_and_loss(&self) -> f64 {
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
    measured_at: DateTime<Utc>,
    equity: Decimal,
}

impl EquityPoint {
    pub fn new(measured_at: DateTime<Utc>, equity: Decimal) -> Self {
        Self {
            measured_at,
            equity,
        }
    }

    pub fn measured_at(&self) -> DateTime<Utc> {
        self.measured_at
    }

    pub fn equity(&self) -> Decimal {
        self.equity
    }
}

/// The activity type carrying trade fills.
pub const FILL_ACTIVITY_TYPE: &str = "FILL";

/// Activity types that move capital into or out of the account from outside it.
///
/// The distinction that matters is external flow versus return, not cash versus non-cash: `INT`,
/// `DIV`, and `FEE` also move the balance, but they are performance and must never be netted out of
/// a return. `CSD` and `CSW` are bank deposits and withdrawals; `JNLC` is cash journalled between
/// accounts on Alpaca's own books, which is how paper accounts are funded.
pub const TRANSFER_ACTIVITY_TYPES: [&str; 3] = ["CSD", "CSW", "JNLC"];

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
    id: String,
    activity_type: String,
    transaction_time: DateTime<Utc>,
    ticker: Option<Ticker>,
    side: Option<String>,
    shares: Option<Decimal>,
    price: Option<Decimal>,
    net_amount: Option<Decimal>,
    order_id: Option<String>,
}

impl AccountActivity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        activity_type: String,
        transaction_time: DateTime<Utc>,
        ticker: Option<Ticker>,
        side: Option<String>,
        shares: Option<Decimal>,
        price: Option<Decimal>,
        net_amount: Option<Decimal>,
        order_id: Option<String>,
    ) -> Self {
        Self {
            id,
            activity_type,
            transaction_time,
            ticker,
            side,
            shares,
            price,
            net_amount,
            order_id,
        }
    }

    /// Alpaca's own activity identifier, and the primary key of `account_activities`.
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn activity_type(&self) -> &str {
        &self.activity_type
    }

    pub fn transaction_time(&self) -> DateTime<Utc> {
        self.transaction_time
    }

    pub fn ticker(&self) -> Option<&Ticker> {
        self.ticker.as_ref()
    }

    pub fn side(&self) -> Option<&str> {
        self.side.as_deref()
    }

    pub fn shares(&self) -> Option<Decimal> {
        self.shares
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

    pub fn order_id(&self) -> Option<&str> {
        self.order_id.as_deref()
    }

    /// Signed cash effect of a fill: negative when shares were bought, positive when sold.
    ///
    /// `None` for any activity that is not a two-sided trade, which is what keeps a fee or a
    /// dividend from being attributed to a pair as though it were a leg.
    pub fn signed_cash_flow(&self) -> Option<Decimal> {
        let shares = self.shares?;
        let price = self.price?;
        match self.side.as_deref()? {
            "buy" => Some(-(shares * price)),
            "sell" | "sell_short" => Some(shares * price),
            _ => None,
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
    side: Option<String>,
    quantity: Option<f64>,
}

impl PositionClose {
    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn alpaca_order_id(&self) -> Option<&str> {
        self.alpaca_order_id.as_deref()
    }

    pub fn side(&self) -> Option<&str> {
        self.side.as_deref()
    }

    pub fn quantity(&self) -> Option<f64> {
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
    quantity: Option<f64>,
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
        quantity: Option<f64>,
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

    pub fn quantity(&self) -> Option<f64> {
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
            .header(HEADER_SECRET_KEY, self.credentials.secret())
    }

    fn delete(&self, url: &str) -> reqwest::RequestBuilder {
        self.http_client
            .delete(url)
            .header(HEADER_KEY_ID, self.credentials.key_id())
            .header(HEADER_SECRET_KEY, self.credentials.secret())
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
    pub async fn fetch_positions(&self) -> Result<Vec<Position>, ClientError> {
        let url = format!("{}/v2/positions", self.base_url);
        let response = error_for_status(self.get(&url).send().await?).await?;
        let payloads: Vec<PositionResponse> = response.json().await.map_err(|error| {
            ClientError::Parse(format!("Failed to parse positions response: {error}"))
        })?;

        let reported = payloads.len();
        let mut positions = Vec::with_capacity(reported);
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
                continue;
            };
            positions.push(Position::new(
                ticker,
                side,
                parse_f64(&payload.qty, "qty")?,
                parse_f64(&payload.market_value, "market_value")?,
                parse_f64(&payload.unrealized_pl, "unrealized_pl")?,
            ));
        }

        debug!(
            positions = positions.len(),
            reported, "Alpaca positions fetched"
        );
        Ok(positions)
    }

    /// Submits an order and returns Alpaca's order identifier.
    /// Sends an order under a caller-chosen `client_order_id`.
    ///
    /// The identifier is the caller's because it must exist before the request does, so a crash
    /// before Alpaca answers still leaves an order something can name.
    pub async fn submit_order(
        &self,
        intent: &OrderIntent,
        client_order_id: &str,
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
            order_id = %order.id,
            client_order_id,
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

    /// Closes the whole position in one symbol.
    ///
    /// Returns `false` when there was no position to close (`404`). That is the expected answer
    /// when a pair is being closed for the second time — after a retry, or after Alpaca liquidated
    /// the leg itself — and treating it as an error would turn a no-op into an incident.
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
            order_id = alpaca_order_id.as_deref().unwrap_or("unknown"),
            "Position close submitted"
        );
        Ok(Some(PositionClose {
            ticker: ticker.clone(),
            alpaca_order_id,
            side: order.as_ref().and_then(|order| order.side.clone()),
            quantity: order
                .as_ref()
                .and_then(|order| order.qty.as_ref())
                .and_then(|quantity| quantity.parse().ok()),
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
                    .and_then(|order| order.qty.as_ref())
                    .and_then(|quantity| quantity.parse().ok()),
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
    ///
    ///
    /// Activities with neither a `transaction_time` nor a `date` are dropped with a warning. They
    /// cannot be stored — `account_activities.transaction_time` is `NOT NULL` — and synthesizing a
    /// timestamp would put a fabricated time into the record the dashboard reads as fact.
    pub async fn fetch_activities(
        &self,
        activity_type: &str,
        date: NaiveDate,
    ) -> Result<ActivityFetch, ClientError> {
        let url = format!("{}/v2/account/activities/{activity_type}", self.base_url);
        let date_text = date.to_string();
        let page_size = ACTIVITIES_PAGE_SIZE.to_string();

        let mut activities: Vec<AccountActivity> = Vec::new();
        let mut page_token: Option<String> = None;
        let mut undated: usize = 0;
        let mut truncated = false;

        for page in 0..ACTIVITIES_MAXIMUM_PAGES {
            let mut query: Vec<(&str, &str)> = vec![
                ("date", date_text.as_str()),
                ("page_size", page_size.as_str()),
            ];
            if let Some(token) = page_token.as_deref() {
                query.push(("page_token", token));
            }

            let response = error_for_status(self.get(&url).query(&query).send().await?).await?;
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
                    payload.activity_type,
                    transaction_time,
                    payload.symbol.as_deref().and_then(Ticker::new),
                    payload.side,
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
                    activity_type,
                    %date,
                    pages = ACTIVITIES_MAXIMUM_PAGES,
                    "Activity pagination hit its page bound; the tail was not fetched"
                );
            }
        }

        if undated > 0 {
            warn!(
                activity_type,
                undated, "Dropped activities carrying neither a transaction time nor a date"
            );
        }
        debug!(activity_type, %date, activities = activities.len(), "Account activities fetched");
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
    let filled_shares = order
        .filled_qty
        .as_deref()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(0.0);

    match order.status.as_str() {
        "filled" => {
            let average_price = order
                .filled_avg_price
                .as_deref()
                .and_then(|raw| raw.parse::<f64>().ok())
                .ok_or_else(|| {
                    ClientError::Parse(format!(
                        "Alpaca reported order {} filled with no average fill price",
                        order.id
                    ))
                })?;
            Ok(OrderState::Filled {
                filled_shares,
                average_price,
            })
        }
        "canceled" | "expired" | "rejected" | "done_for_day" | "stopped" | "suspended" => {
            Ok(OrderState::Abandoned {
                status: order.status,
                filled_shares,
            })
        }
        _ => Ok(OrderState::Working { filled_shares }),
    }
}

fn parse_decimal(raw: &str, field: &'static str) -> Result<Decimal, ClientError> {
    raw.trim()
        .parse::<Decimal>()
        .map_err(|error| ClientError::Parse(format!("Failed to parse {field} '{raw}': {error}")))
}

fn parse_f64(raw: &str, field: &'static str) -> Result<f64, ClientError> {
    raw.trim()
        .parse::<f64>()
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
    /// This is what makes an order recoverable after a crash between the session log write and
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

// --------------------------------------------------------------------------
// Market data API: snapshots and bars
// --------------------------------------------------------------------------

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
/// the request carrying it failed. `failed_symbols` is what separates the two.
#[derive(Debug, Clone, Default)]
pub struct SnapshotFetch {
    pub snapshots: Vec<Snapshot>,
    pub failed_symbols: Vec<String>,
}

/// One symbol's point-in-time market state.
///
/// Every field is optional because Alpaca omits rather than zeroes: a symbol that has not traded
/// today has no daily bar, and one that is halted may have no quote. A missing value and a zero one
/// are indistinguishable after the fact, so the distinction is preserved here and resolved by the
/// caller.
#[derive(Debug, Clone)]
pub struct Snapshot {
    ticker: Ticker,
    latest_quote: Option<EquityQuote>,
    latest_trade_price: Option<f64>,
    minute_bar: Option<EquityBar>,
    daily_bar: Option<EquityBar>,
    previous_daily_bar: Option<EquityBar>,
}

impl Snapshot {
    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn latest_quote(&self) -> Option<&EquityQuote> {
        self.latest_quote.as_ref()
    }

    pub fn latest_trade_price(&self) -> Option<f64> {
        self.latest_trade_price
    }

    pub fn minute_bar(&self) -> Option<&EquityBar> {
        self.minute_bar.as_ref()
    }

    pub fn daily_bar(&self) -> Option<&EquityBar> {
        self.daily_bar.as_ref()
    }

    pub fn previous_daily_bar(&self) -> Option<&EquityBar> {
        self.previous_daily_bar.as_ref()
    }

    /// The most defensible current price, preferring the quote midpoint and falling back to the
    /// last trade.
    ///
    /// The midpoint leads because it reflects where the market currently is willing to transact,
    /// while the last trade reports where someone already did — possibly a long time ago in a thin
    /// name. Callers needing to reject a wide or stale book should read [`Snapshot::latest_quote`]
    /// directly; this is the convenience path, not the careful one.
    pub fn reference_price(&self) -> Option<f64> {
        self.reference_price_with_source().map(|(price, _)| price)
    }

    /// [`Snapshot::reference_price`] together with the field it came from.
    ///
    /// A midpoint and a last trade are not interchangeable readings, so a price without its source
    /// cannot be compared against the same symbol on the next pass.
    pub fn reference_price_with_source(&self) -> Option<(f64, PriceSource)> {
        self.latest_quote
            .as_ref()
            .map(|quote| (quote.mid_price(), PriceSource::QuoteMidpoint))
            .or_else(|| {
                self.latest_trade_price
                    .map(|price| (price, PriceSource::LastTrade))
            })
    }
}

/// Which field of a [`Snapshot`] a reference price was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceSource {
    QuoteMidpoint,
    LastTrade,
}

impl PriceSource {
    /// A stable short name for the session log and the structured logs.
    pub fn as_str(self) -> &'static str {
        match self {
            PriceSource::QuoteMidpoint => "quote_midpoint",
            PriceSource::LastTrade => "last_trade",
        }
    }
}

impl std::fmt::Display for PriceSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
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
            .header(HEADER_SECRET_KEY, self.credentials.secret())
    }

    /// Fetches point-in-time snapshots for `symbols`, in bounded chunks.
    ///
    /// A chunk that fails is logged and skipped; its symbols simply go unpriced. Partial pricing
    /// narrows the entry set and holds the exits it cannot price, both of which beat pricing
    /// nothing at all. Only a total failure — every chunk failing — is reported as an error, which
    /// keeps that report meaningful for the common single-chunk case.
    ///
    /// The failed chunk's symbols come back named. Downstream, a symbol Alpaca had no quote for and
    /// one whose request never completed are the same absence, and they are not the same problem.
    pub async fn fetch_snapshots(&self, symbols: &[String]) -> Result<SnapshotFetch, ClientError> {
        if symbols.is_empty() {
            return Ok(SnapshotFetch::default());
        }

        let mut snapshots: Vec<Snapshot> = Vec::new();
        let mut failed_symbols: Vec<String> = Vec::new();
        let mut failed_chunks: usize = 0;
        let mut requests: usize = 0;
        let mut last_error: Option<ClientError> = None;

        for chunk in symbols.chunks(SNAPSHOT_SYMBOLS_PER_REQUEST) {
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
                    failed_symbols.extend(chunk.iter().cloned());
                    last_error = Some(error);
                }
            }
        }

        if failed_chunks == requests {
            return Err(last_error.expect("a failed chunk records its error"));
        }

        info!(
            requested = symbols.len(),
            returned = snapshots.len(),
            requests,
            failed_chunks,
            "Snapshots fetched"
        );
        Ok(SnapshotFetch {
            snapshots,
            failed_symbols,
        })
    }

    async fn fetch_snapshot_chunk(&self, symbols: &[String]) -> Result<Vec<Snapshot>, ClientError> {
        let url = format!("{}/v2/stocks/snapshots", self.base_url);
        let response = error_for_status(
            self.get(&url)
                .query(&[
                    ("symbols", symbols.join(",").as_str()),
                    ("feed", self.feed.as_str()),
                ])
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
                    latest_trade_price: snapshot.latest_trade.and_then(|trade| trade.price),
                    minute_bar: snapshot
                        .minute_bar
                        .and_then(|bar| bar.into_equity_bar(&ticker, BarInterval::OneMinute)),
                    daily_bar: snapshot
                        .daily_bar
                        .and_then(|bar| bar.into_equity_bar(&ticker, BarInterval::OneDay)),
                    previous_daily_bar: snapshot
                        .previous_daily_bar
                        .and_then(|bar| bar.into_equity_bar(&ticker, BarInterval::OneDay)),
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
    #[serde(rename = "minuteBar")]
    minute_bar: Option<BarPayload>,
    #[serde(rename = "dailyBar")]
    daily_bar: Option<BarPayload>,
    #[serde(rename = "prevDailyBar")]
    previous_daily_bar: Option<BarPayload>,
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
    #[serde(rename = "p")]
    price: Option<f64>,
}

#[derive(Deserialize)]
struct BarPayload {
    #[serde(rename = "t")]
    timestamp: Option<DateTime<Utc>>,
    #[serde(rename = "o")]
    open_price: Option<f64>,
    #[serde(rename = "h")]
    high_price: Option<f64>,
    #[serde(rename = "l")]
    low_price: Option<f64>,
    #[serde(rename = "c")]
    close_price: Option<f64>,
    #[serde(rename = "v")]
    volume: Option<f64>,
    #[serde(rename = "vw")]
    vw: Option<f64>,
    #[serde(rename = "n")]
    transactions: Option<i64>,
}

impl BarPayload {
    /// Volume arrives as a float and is rounded to whole shares. Alpaca reports fractional volume
    /// for some feeds; the column is a `BIGINT` and a partial share is not a meaningful unit here.
    ///
    /// A bar whose prices do not form a coherent candle is dropped by the constructor, with the
    /// reason logged, rather than being stored for the screen to trip over later.
    fn into_equity_bar(self, ticker: &Ticker, bar_interval: BarInterval) -> Option<EquityBar> {
        EquityBar::new(
            ticker.clone(),
            bar_interval,
            self.timestamp?,
            self.open_price?,
            self.high_price?,
            self.low_price?,
            self.close_price?,
            self.volume?.round() as i64,
            self.vw,
            self.transactions,
        )
        .inspect_err(|error| {
            debug!(ticker = %ticker, error = %error, "Dropped an incoherent bar");
        })
        .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- credentials ---
    use serial_test::serial;

    #[test]
    fn test_new_stores_fields() {
        let credentials =
            AlpacaCredentials::new("key123".to_string(), "secret456".to_string()).unwrap();
        assert_eq!(credentials.key_id(), "key123");
        assert_eq!(credentials.secret(), "secret456");
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
    fn test_new_rejects_empty_secret() {
        assert!(matches!(
            AlpacaCredentials::new("key123".to_string(), String::new()),
            Err(CredentialsError::Empty { field: "secret" })
        ));
    }

    #[test]
    fn test_clone() {
        let credentials =
            AlpacaCredentials::new("key123".to_string(), "secret456".to_string()).unwrap();
        let cloned = credentials.clone();
        assert_eq!(cloned.key_id(), "key123");
        assert_eq!(cloned.secret(), "secret456");
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

    // --- trading API ---

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
            HashSet::from(["AAPL".to_string(), "MSFT".to_string()]),
            HashSet::from(["AAPL".to_string()]),
        );
        assert!(assets.is_tradable("AAPL"));
        assert!(assets.is_tradable("MSFT"));
        assert!(assets.is_shortable("AAPL"));
        assert!(!assets.is_shortable("MSFT"));
        assert!(!assets.is_tradable("NVDA"));
        assert_eq!(assets.tradable_count(), 2);
        assert_eq!(assets.shortable_count(), 1);
        assert_eq!(assets.tradable_symbols(), vec!["AAPL", "MSFT"]);
    }

    #[tokio::test]
    async fn test_fetch_clock_reads_session_boundaries() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/clock")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"timestamp":"2026-06-10T14:00:00Z","is_open":true,
                    "next_open":"2026-06-11T13:30:00Z","next_close":"2026-06-10T20:00:00Z"}"#,
            )
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let clock = client.fetch_clock().await.expect("clock must parse");

        assert!(clock.is_open());
        assert_eq!(clock.next_close().to_rfc3339(), "2026-06-10T20:00:00+00:00");
        mock.assert_async().await;
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
        assert_eq!(points[0].measured_at().timestamp(), 1_778_788_800);
        assert_eq!(points[0].equity(), Decimal::new(2010050, 2));
        assert_eq!(points[1].measured_at().timestamp(), 1_778_875_200);
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

        let eastern = points[0]
            .measured_at()
            .with_timezone(&New_York)
            .date_naive();
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

        assert!(assets.is_shortable("AAPL"));
        assert!(assets.is_tradable("MSFT"));
        assert!(
            !assets.is_shortable("MSFT"),
            "hard to borrow is not shortable"
        );
        assert!(!assets.is_tradable("XYZ"), "inactive assets are excluded");
        assert!(
            !assets.is_tradable("NOPE"),
            "non-tradable assets are excluded"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_api_error_carries_status_and_body() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/clock")
            .with_status(403)
            .with_body("forbidden")
            .create_async()
            .await;

        let client = TradingClient::with_base_url(credentials(), server.url());
        let error = client
            .fetch_clock()
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

    // --- trading API: orders, positions, account, activities ---

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
        .to_request("client-long");
        assert_eq!(long.side, "buy");
        assert_eq!(long.notional.as_deref(), Some("1234.56"));
        assert_eq!(long.qty, None);
        assert_eq!(long.position_intent, "buy_to_open");

        let short = OrderIntent::OpenShort {
            ticker: ticker("MSFT"),
            shares: shares(40),
        }
        .to_request("client-short");
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
                shares: shares(10),
            }
            .to_request("client-short"),
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
                filled_shares: 12.0,
                average_price: 101.25,
            }
        );
        for status in ["canceled", "expired", "rejected", "done_for_day"] {
            assert!(matches!(
                order_state_from(order_response(status, "3", "100")).unwrap(),
                OrderState::Abandoned {
                    filled_shares,
                    ..
                } if filled_shares == 3.0
            ));
        }
        for status in ["new", "accepted", "partially_filled", "pending_cancel"] {
            assert!(matches!(
                order_state_from(order_response(status, "0", "0")).unwrap(),
                OrderState::Working { .. }
            ));
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
        assert_eq!(state.filled_shares(), 7.5);
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

        assert_eq!(positions.len(), 2);
        assert_eq!(positions[1].side(), PositionSide::Short);
        assert_eq!(positions[1].shares(), 4.0);
        assert_eq!(positions[1].market_value(), -1480.0);
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

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].ticker(), &ticker("MSFT"));
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
                "client-long",
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
            .fetch_activities("FILL", date(2026, 7, 30))
            .await
            .expect("activities must parse");

        assert_eq!(activities.activities.len(), ACTIVITIES_PAGE_SIZE + 1);
        assert_eq!(activities.activities.last().unwrap().id(), "activity-tail");
        first.assert_async().await;
        second.assert_async().await;
    }

    /// A buy consumes cash and a sell produces it. Getting the sign wrong inverts every pair's
    /// realized profit and loss, which is a number that looks entirely plausible either way.
    #[test]
    fn test_signed_cash_flow_follows_the_side() {
        let build = |side: &str| {
            AccountActivity::new(
                "a".to_string(),
                "FILL".to_string(),
                Utc::now(),
                Some(ticker("AAPL")),
                Some(side.to_string()),
                Some(Decimal::new(10, 0)),
                Some(Decimal::new(15050, 2)),
                None,
                None,
            )
        };
        assert_eq!(
            build("buy").signed_cash_flow(),
            Some(Decimal::new(-150500, 2))
        );
        assert_eq!(
            build("sell").signed_cash_flow(),
            Some(Decimal::new(150500, 2))
        );
        assert_eq!(
            build("sell_short").signed_cash_flow(),
            Some(Decimal::new(150500, 2))
        );
    }

    /// A fee has no side, quantity, or price. Attributing it to a pair as though it were a leg
    /// would move that pair's realized profit and loss by the fee amount.
    #[test]
    fn test_non_trade_activity_has_no_cash_flow() {
        let fee = AccountActivity::new(
            "f".to_string(),
            "FEE".to_string(),
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
            .fetch_activities("FEE", date(2026, 7, 30))
            .await
            .expect("activities must parse");

        assert_eq!(activities.activities.len(), 1);
        assert_eq!(activities.activities[0].id(), "dated");
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
            .fetch_activities("JNLC", date(2026, 5, 15))
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
            .fetch_activities("JNLC", date(2026, 5, 15))
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
            .fetch_activities("FILL", date(2026, 5, 15))
            .await
            .expect("activities must parse");

        assert_eq!(
            activities.activities[0].transaction_time(),
            "2026-05-15T13:45:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        mock.assert_async().await;
    }

    // --- market data API ---

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
            .fetch_snapshots(&["AAPL".to_string()])
            .await
            .expect("snapshot must parse");

        assert_eq!(snapshots.snapshots.len(), 1);
        let snapshot = &snapshots.snapshots[0];
        assert_eq!(snapshot.ticker().as_str(), "AAPL");
        assert_eq!(snapshot.latest_trade_price(), Some(201.5));
        assert_eq!(snapshot.latest_quote().unwrap().bid_price(), 201.0);
        assert_eq!(snapshot.minute_bar().unwrap().close_price(), 201.5);
        assert_eq!(snapshot.daily_bar().unwrap().volume(), 2_500_000);
        assert_eq!(snapshot.previous_daily_bar().unwrap().close_price(), 199.0);
        assert_eq!(
            snapshot.previous_daily_bar().unwrap().bar_interval(),
            BarInterval::OneDay
        );
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
            .fetch_snapshots(&["AAPL".to_string()])
            .await
            .unwrap();

        assert_eq!(snapshots.snapshots[0].reference_price(), Some(201.5));
        mock.assert_async().await;
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
                r#"{"AAPL":{"latestTrade":{"p":150.0},
                    "latestQuote":{"t":"2026-06-10T15:59:30Z","bp":149.0,"bs":10,"as":12}}}"#,
            )
            .create_async()
            .await;

        let snapshots = client(server.url())
            .fetch_snapshots(&["AAPL".to_string()])
            .await
            .unwrap();

        assert!(
            snapshots.snapshots[0].latest_quote().is_none(),
            "half a book is no book"
        );
        assert_eq!(snapshots.snapshots[0].reference_price(), Some(150.0));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_empty_symbol_list_makes_no_request() {
        let server = mockito::Server::new_async().await;
        let snapshots = client(server.url()).fetch_snapshots(&[]).await.unwrap();
        assert!(snapshots.snapshots.is_empty());
    }
}
