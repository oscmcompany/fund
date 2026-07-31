//! The Alpaca integration: credentials, the trading API, and the market data API.
//!
//! Alpaca is two services behind one set of credentials. The trading API at `api.alpaca.markets`
//! answers what the market *is* — the clock, the published calendar, the asset universe, and in due
//! course orders and positions. The market data API at `data.alpaca.markets` answers what things
//! *cost* — snapshots and bars. They share authentication, an error type, and nothing else, which
//! is why one file holds both rather than a module per host.
//!
//! Alpaca is the source of truth for fills, balances, buying power, and positions, so this module
//! is deliberately thin: it deserializes what Alpaca reports and hands it on. Where a response is
//! ambiguous — a snapshot with no quote, a calendar day with no duration — the ambiguity is
//! surfaced rather than defaulted, because a missing value and a zero one are indistinguishable
//! after the fact.

use std::collections::HashSet;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::common::types::{BarInterval, EquityBar, EquityQuote, Ticker};

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

    /// Reads the paper/live choice from `ALPACA_PAPER_TRADING`, defaulting to paper.
    ///
    /// Defaulting to paper is deliberate: a missing or misspelled variable sends orders to the
    /// sandbox, not to a live account.
    pub fn from_env(credentials: AlpacaCredentials) -> Self {
        let is_paper = std::env::var("ALPACA_PAPER_TRADING")
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
// Market data API: snapshots and bars
// --------------------------------------------------------------------------

/// Symbols requested per snapshot call.
///
/// The endpoint imposes no documented symbol ceiling and has been measured accepting 2,000 symbols
/// in a 9,424-character URL. This cap is therefore not the API's limit but a bound on request-line
/// length, which intermediaries do restrict.
///
/// Chunking also limits the blast radius of a single failed request, but only because
/// [`MarketDataClient::fetch_snapshots`] keeps the chunks that succeeded. Propagating the first
/// error instead would make a smaller cap strictly worse, by giving one failure more chunks to take
/// down with it.
const SNAPSHOT_SYMBOLS_PER_REQUEST: usize = 1_000;

/// Which consolidated tape the market data API serves from.
///
/// `iex` covers a few percent of consolidated volume, so quoted spreads are wide and often stale
/// outside the largest names -- computing a pair spread from it introduces noise that looks exactly
/// like signal. `sip` is the full consolidated tape and requires a paid subscription.
///
/// An enum rather than a string so an unrecognized value is rejected at the edge, where it can be
/// reported, instead of reaching Alpaca as an unhelpful 400 on every request of the session.
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

/// Symbols requested per historical bars call.
///
/// Separate from the snapshot cap despite sharing its value today. The two endpoints have different
/// request shapes -- bars carries six more parameters and a page token -- so the request-line budget
/// they are each bounded by is not the same, and tuning one should not silently move the other.
const BARS_SYMBOLS_PER_REQUEST: usize = 1_000;

/// Rows requested per historical bars page.
///
/// 10,000 is the endpoint's documented maximum. Asking for fewer only increases the number of
/// round trips needed to walk a backfill.
const BARS_PAGE_LIMIT: usize = 10_000;

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
        self.latest_quote
            .as_ref()
            .map(EquityQuote::mid_price)
            .or(self.latest_trade_price)
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
    pub async fn fetch_snapshots(&self, symbols: &[String]) -> Result<Vec<Snapshot>, ClientError> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let mut snapshots: Vec<Snapshot> = Vec::new();
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
        Ok(snapshots)
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

    /// Fetches historical bars for `symbols` over an inclusive date range, following pagination
    /// until the feed is exhausted.
    ///
    /// The multi-symbol bars endpoint returns a map of symbol to bar list plus a `next_page_token`
    /// that spans symbols, so a page boundary can fall in the middle of one symbol's history.
    /// Accumulating across pages before returning is what keeps that invisible to callers.
    pub async fn fetch_bars(
        &self,
        symbols: &[String],
        bar_interval: BarInterval,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<EquityBar>, ClientError> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let mut bars: Vec<EquityBar> = Vec::new();
        let mut pages: usize = 0;

        let url = format!("{}/v2/stocks/bars", self.base_url);
        let page_limit = BARS_PAGE_LIMIT.to_string();
        let start_parameter = start.to_string();
        let end_parameter = end.to_string();

        for chunk in symbols.chunks(BARS_SYMBOLS_PER_REQUEST) {
            let symbols_parameter = chunk.join(",");
            let mut page_token: Option<String> = None;
            loop {
                // Built through the query serializer rather than interpolated into the URL.
                // `next_page_token` is base64, so it can contain `+`, `/`, and `=`; interpolated
                // raw, a `+` decodes server-side as a space and the token is rejected or silently
                // resolves to a different page. That surfaces as a truncated backfill, not an error.
                let mut parameters: Vec<(&str, &str)> = vec![
                    ("symbols", symbols_parameter.as_str()),
                    ("timeframe", bar_interval.alpaca_timeframe()),
                    ("start", start_parameter.as_str()),
                    ("end", end_parameter.as_str()),
                    ("limit", page_limit.as_str()),
                    ("feed", self.feed.as_str()),
                    ("adjustment", "all"),
                ];
                if let Some(token) = &page_token {
                    parameters.push(("page_token", token.as_str()));
                }

                let response =
                    error_for_status(self.get(&url).query(&parameters).send().await?).await?;
                let payload: BarsResponse = response.json().await.map_err(|error| {
                    ClientError::Parse(format!("Failed to parse bars response: {error}"))
                })?;
                pages += 1;

                for (symbol, symbol_bars) in payload.bars {
                    let Some(ticker) = Ticker::new(&symbol) else {
                        warn!(symbol = %symbol, "Bars returned for an unparseable symbol, skipping");
                        continue;
                    };
                    bars.extend(
                        symbol_bars
                            .into_iter()
                            .filter_map(|bar| bar.into_equity_bar(&ticker, bar_interval)),
                    );
                }

                match payload.next_page_token {
                    Some(token) if !token.is_empty() => page_token = Some(token),
                    _ => break,
                }
            }
        }

        info!(
            symbols = symbols.len(),
            bar_interval = bar_interval.as_str(),
            bars = bars.len(),
            pages,
            "Historical bars fetched"
        );
        Ok(bars)
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

#[derive(Deserialize)]
struct BarsResponse {
    #[serde(default)]
    bars: std::collections::HashMap<String, Vec<BarPayload>>,
    #[serde(default)]
    next_page_token: Option<String>,
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

        assert_eq!(snapshots.len(), 1);
        let snapshot = &snapshots[0];
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

        assert_eq!(snapshots[0].reference_price(), Some(201.5));
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
            snapshots[0].latest_quote().is_none(),
            "half a book is no book"
        );
        assert_eq!(snapshots[0].reference_price(), Some(150.0));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_empty_symbol_list_makes_no_request() {
        let server = mockito::Server::new_async().await;
        let snapshots = client(server.url()).fetch_snapshots(&[]).await.unwrap();
        assert!(snapshots.is_empty());
    }

    /// Bars must accumulate across pages. Stopping at the first page silently truncates a backfill,
    /// which then looks like missing market history rather than a client bug.
    #[tokio::test]
    async fn test_fetch_bars_follows_pagination() {
        let mut server = mockito::Server::new_async().await;
        let base = "/v2/stocks/bars?symbols=AAPL&timeframe=1Day&start=2026-06-08&end=2026-06-10&limit=10000&feed=iex&adjustment=all";
        let first = server
            .mock("GET", base)
            .with_status(200)
            .with_body(
                r#"{"bars":{"AAPL":[{"t":"2026-06-08T04:00:00Z","o":1.0,"h":2.0,"l":0.5,"c":1.5,"v":100.0}]},
                    "next_page_token":"page2"}"#,
            )
            .create_async()
            .await;
        let second = server
            .mock("GET", format!("{base}&page_token=page2").as_str())
            .with_status(200)
            .with_body(
                r#"{"bars":{"AAPL":[{"t":"2026-06-09T04:00:00Z","o":1.5,"h":2.5,"l":1.0,"c":2.0,"v":200.0}]},
                    "next_page_token":null}"#,
            )
            .create_async()
            .await;

        let bars = client(server.url())
            .fetch_bars(
                &["AAPL".to_string()],
                BarInterval::OneDay,
                NaiveDate::from_ymd_opt(2026, 6, 8).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
            )
            .await
            .expect("bars must parse");

        assert_eq!(bars.len(), 2, "both pages must be accumulated");
        first.assert_async().await;
        second.assert_async().await;
    }

    /// Page tokens are base64, so they contain `+`, `/`, and `=`. Interpolated into the URL raw, a
    /// `+` is decoded server-side as a space and the token names a different page or none at all —
    /// which surfaces as a silently truncated backfill rather than an error. The query serializer
    /// percent-encodes them, and this asserts the encoded form actually goes out on the wire.
    #[tokio::test]
    async fn test_fetch_bars_percent_encodes_the_page_token() {
        let mut server = mockito::Server::new_async().await;
        let base = "/v2/stocks/bars?symbols=AAPL&timeframe=1Day&start=2026-06-08&end=2026-06-10&limit=10000&feed=iex&adjustment=all";
        let first = server
            .mock("GET", base)
            .with_status(200)
            .with_body(r#"{"bars":{},"next_page_token":"a+b/c=="}"#)
            .create_async()
            .await;
        let second = server
            .mock("GET", format!("{base}&page_token=a%2Bb%2Fc%3D%3D").as_str())
            .with_status(200)
            .with_body(r#"{"bars":{},"next_page_token":null}"#)
            .create_async()
            .await;

        client(server.url())
            .fetch_bars(
                &["AAPL".to_string()],
                BarInterval::OneDay,
                NaiveDate::from_ymd_opt(2026, 6, 8).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
            )
            .await
            .expect("bars must parse");

        first.assert_async().await;
        second.assert_async().await;
    }

    /// The request must carry Alpaca's timeframe spelling, not the stored form. Sending `1day`
    /// returns an empty result rather than an error, so this is the test that catches it.
    #[tokio::test]
    async fn test_fetch_bars_sends_alpaca_timeframe_spelling() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/stocks/bars?symbols=AAPL&timeframe=1Hour&start=2026-06-10&end=2026-06-10&limit=10000&feed=iex&adjustment=all")
            .with_status(200)
            .with_body(r#"{"bars":{},"next_page_token":null}"#)
            .create_async()
            .await;

        client(server.url())
            .fetch_bars(
                &["AAPL".to_string()],
                BarInterval::SixtyMinute,
                NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 10).unwrap(),
            )
            .await
            .expect("bars must parse");

        mock.assert_async().await;
    }
}
