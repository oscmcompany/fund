//! Alpaca's SIP quotes and trades, a session's requested as of that session, its latest quotes, its published trading
//! calendar, and its asset listing's short-sale flags.

pub mod corporate_actions;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

use super::retry::{FetchError, send, with_retries};
use super::{
    Fetched, OneSidedQuote, ParseRefusal, PrintOrigin, QuoteOrigin, RefusedRow, RowRefusal, Secret,
    VariableRefusal, priced_side, variable,
};
use crate::common::market::borrow::BorrowFlags;
use crate::common::market::record::Quote;
use crate::common::market::trade_bars::{ConditionLetter, Correction, Print, Tape};
use crate::common::market::{Price, Shares, Symbol};
use crate::common::time::calendar::{TradingCalendar, TradingSession};
use crate::common::time::{SessionDate, SessionRange};

const QUOTES_URL: &str = "https://data.alpaca.markets/v2/stocks/quotes";
const TRADES_URL: &str = "https://data.alpaca.markets/v2/stocks/trades";
const LATEST_QUOTES_URL: &str = "https://data.alpaca.markets/v2/stocks/quotes/latest";

/// Rows per page, the endpoint's maximum.
const PAGE_LIMIT: &str = "10000";

pub struct Alpaca {
    http_client: reqwest::Client,
    credentials: Credentials,
    account: Account,
}

/// The account a key pair trades against; paper and live keys each work only against their own trading API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Account {
    Paper,
    Live,
}

impl Account {
    /// The trading API, which also serves the calendar.
    fn trading_url(self) -> &'static str {
        match self {
            Self::Paper => "https://paper-api.alpaca.markets",
            Self::Live => "https://api.alpaca.markets",
        }
    }
}

/// The key pair every Alpaca request carries, and the only reader of its keys.
#[derive(Debug)]
struct Credentials {
    key_id: Secret,
    secret: Secret,
}

impl Credentials {
    fn sign(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header("APCA-API-KEY-ID", self.key_id.expose())
            .header("APCA-API-SECRET-KEY", self.secret.expose())
    }
}

/// The one field every paginated Alpaca response shares.
#[derive(Deserialize)]
struct PageToken {
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct CalendarRow {
    date: NaiveDate,
    #[serde(deserialize_with = "hour_minute")]
    open: NaiveTime,
    #[serde(deserialize_with = "hour_minute")]
    close: NaiveTime,
}

/// Alpaca's Eastern wall-clock `HH:MM`, which chrono's default `HH:MM:SS` does not read.
fn hour_minute<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<NaiveTime, D::Error> {
    let text = String::deserialize(deserializer)?;
    NaiveTime::parse_from_str(&text, "%H:%M")
        .map_err(|error| serde::de::Error::custom(format!("`{text}`: {error}")))
}

#[derive(Deserialize)]
struct ErrorBody {
    message: String,
}

impl Alpaca {
    /// Reads `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET` and `ALPACA_IS_PAPER`, which is `true` or `false` in any case.
    pub fn from_environment(http_client: reqwest::Client) -> Result<Self, VariableRefusal> {
        Ok(Self {
            http_client,
            credentials: Credentials {
                key_id: Secret::new(variable("ALPACA_API_KEY_ID")?),
                secret: Secret::new(variable("ALPACA_API_SECRET")?),
            },
            account: account(variable("ALPACA_IS_PAPER")?)?,
        })
    }

    /// A client with no keys whose paper flag reads `is_paper`, for tests that never send a request.
    #[cfg(test)]
    pub(crate) fn unkeyed(is_paper: &str) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            credentials: Credentials {
                key_id: Secret::new(String::new()),
                secret: Secret::new(String::new()),
            },
            account: account(is_paper.to_string()).expect("a test passes a valid paper flag"),
        }
    }

    pub(crate) fn account(&self) -> Account {
        self.account
    }

    /// A request to the trading API at `path`, carrying the keys.
    pub(crate) fn trading(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.credentials.sign(
            self.http_client
                .request(method, format!("{}{path}", self.account.trading_url())),
        )
    }

    /// The published sessions over `range`. A row that does not read refuses the whole calendar, since dropping it
    /// would turn a trading day into a holiday that no heal ever owes.
    pub async fn calendar(&self, range: SessionRange) -> Result<TradingCalendar, FetchError> {
        let url = format!("{}/v2/calendar", self.account.trading_url());
        let (start, end) = (range.first().to_string(), range.last().to_string());
        let body = with_retries(|| {
            send(
                self.credentials
                    .sign(self.http_client.get(&url))
                    .query(&[("start", start.as_str()), ("end", end.as_str())]),
            )
        })
        .await?;
        parse_calendar(&body, range)
    }

    /// Every active US equity's short-sale flags as of now, from one response of the asset listing.
    pub async fn borrow_flags(&self) -> Fetched<BorrowFlagsRead> {
        let url = format!("{}/v2/assets", self.account.trading_url());
        let body = with_retries(|| {
            send(
                self.credentials
                    .sign(self.http_client.get(&url))
                    .query(&[("status", "active"), ("asset_class", "us_equity")]),
            )
        })
        .await;
        match body {
            Ok(body) => Fetched::new(parse_borrow_flags(&body), vec![body]),
            Err(error) => Fetched::new(Err(error), Vec::new()),
        }
    }
}

/// One active asset's short-sale flags; the listing carries more fields, which the archive keeps raw.
#[derive(Deserialize)]
struct AssetRow {
    symbol: String,
    shortable: bool,
    easy_to_borrow: bool,
}

/// The asset listing as read: each asset's flags, and every row whose symbol did not read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BorrowFlagsRead {
    pub flags: Vec<BorrowFlags>,
    pub refused: Vec<RefusedRow>,
}

#[derive(Deserialize)]
struct QuotesPage {
    /// `null` on a page with no quotes.
    quotes: Option<BTreeMap<String, Vec<AlpacaQuote>>>,
}

/// The latest-quotes response: one quote per ticker rather than a page of them.
#[derive(Deserialize)]
struct LatestQuotes {
    quotes: BTreeMap<String, AlpacaQuote>,
}

#[derive(Deserialize)]
struct AlpacaQuote {
    #[serde(rename = "t")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "bp")]
    bid_price: f64,
    #[serde(rename = "bs")]
    bid_size: f64,
    #[serde(rename = "ap")]
    ask_price: f64,
    #[serde(rename = "as")]
    ask_size: f64,
    #[serde(rename = "bx")]
    bid_exchange: Option<String>,
    #[serde(rename = "ax")]
    ask_exchange: Option<String>,
    #[serde(rename = "z")]
    tape: Option<String>,
    #[serde(rename = "c", default)]
    conditions: Vec<String>,
}

#[derive(Deserialize)]
struct TradesPage {
    /// `null` on a page with no trades.
    trades: Option<BTreeMap<String, Vec<AlpacaTrade>>>,
}

#[derive(Deserialize)]
struct AlpacaTrade {
    #[serde(rename = "t")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "p")]
    price: f64,
    #[serde(rename = "s")]
    size: f64,
    /// The tape's condition letters, one per element.
    #[serde(rename = "c", default)]
    conditions: Vec<String>,
    /// The tape letter: `A` and `B` report under CTA's letters, `C` under UTP's.
    #[serde(rename = "z")]
    tape: String,
    /// The print's correction status, absent on a regular print.
    #[serde(rename = "u")]
    update: Option<String>,
    #[serde(rename = "x")]
    exchange: Option<String>,
    #[serde(rename = "i")]
    id: Option<u64>,
}

/// The tape letter a trade is reported under, as Alpaca spells it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
enum TapeLetter {
    A,
    B,
    C,
}

/// What one Alpaca quote became.
#[derive(Debug, Clone, PartialEq)]
pub enum AlpacaQuoteOutcome {
    Quote {
        quote: Quote,
        origin: QuoteOrigin,
    },
    /// A side with no price, which is no top of book.
    OneSided {
        quote: OneSidedQuote,
        origin: QuoteOrigin,
    },
    Refused(RefusedRow),
}

/// What one Alpaca trade became.
#[derive(Debug, Clone, PartialEq)]
pub enum AlpacaTradeOutcome {
    Print {
        print: Print,
        tape: Tape,
        letters: Vec<ConditionLetter>,
        correction: Correction,
        origin: PrintOrigin,
    },
    Refused(RefusedRow),
}

/// Stands in for a condition element that is not one letter, which no condition spells, so the print reads as
/// unresolved rather than being dropped.
const UNSPELLABLE: ConditionLetter = ConditionLetter::of('\u{FFFD}');

/// One response page of a symbol's ticks: its bytes as served and the outcomes read from them.
#[derive(Debug)]
pub struct Page<Outcome> {
    body: Vec<u8>,
    outcomes: Vec<Outcome>,
}

impl<Outcome> Page<Outcome> {
    pub fn new(body: Vec<u8>, outcomes: Vec<Outcome>) -> Self {
        Self { body, outcomes }
    }

    pub fn into_parts(self) -> (Vec<u8>, Vec<Outcome>) {
        (self.body, self.outcomes)
    }
}

/// Reads one page of a symbol's ticks into outcomes, with whether any row came filed under that symbol.
type PageParser<Outcome> = fn(&Symbol, &[u8]) -> Result<(Vec<Outcome>, bool), FetchError>;

impl Alpaca {
    /// Sends every SIP quote for `symbol` over the whole Eastern day of `session`, resolved as of that session, a
    /// page at a time; answers whether any row came filed under the symbol asked for, which rows filed under another
    /// ticker do not show.
    pub async fn quotes(
        &self,
        symbol: &Symbol,
        session: SessionDate,
        pages: &Sender<Page<AlpacaQuoteOutcome>>,
    ) -> Result<bool, FetchError> {
        self.tick_pages(QUOTES_URL, symbol, session, quote_page, pages)
            .await
    }

    /// `quotes`, for SIP trades.
    pub async fn trades(
        &self,
        symbol: &Symbol,
        session: SessionDate,
        pages: &Sender<Page<AlpacaTradeOutcome>>,
    ) -> Result<bool, FetchError> {
        self.tick_pages(TRADES_URL, symbol, session, trade_page, pages)
            .await
    }

    async fn tick_pages<Outcome>(
        &self,
        url: &str,
        symbol: &Symbol,
        session: SessionDate,
        parse: PageParser<Outcome>,
        pages: &Sender<Page<Outcome>>,
    ) -> Result<bool, FetchError> {
        let (start, end) = session.bounds();
        // The endpoint's end is inclusive, so the next session's first nanosecond is excluded.
        let end = end - TimeDelta::nanoseconds(1);
        let as_of = session.to_string();
        let (start, end) = (start.to_rfc3339(), end.to_rfc3339());
        let mut tokens = PageTokens::default();
        let mut token: Option<String> = None;
        let mut answered = false;
        loop {
            let body = with_retries(|| {
                let mut query = vec![
                    ("symbols", symbol.as_str()),
                    ("start", start.as_str()),
                    ("end", end.as_str()),
                    ("feed", "sip"),
                    ("asof", as_of.as_str()),
                    ("sort", "asc"),
                    ("limit", PAGE_LIMIT),
                ];
                if let Some(token) = token.as_deref() {
                    query.push(("page_token", token));
                }
                send(
                    self.credentials
                        .sign(self.http_client.get(url))
                        .query(&query),
                )
            })
            .await?;
            let next = tokens.next(&body)?;
            let (outcomes, page_answered) = parse(symbol, &body)?;
            answered |= page_answered;
            // A closed channel means nothing folds these pages, so fetching more is wasted.
            match (pages.send(Page::new(body, outcomes)).await, next) {
                (Err(_), _) | (Ok(()), None) => return Ok(answered),
                (Ok(()), Some(next)) => token = Some(next),
            }
        }
    }

    /// Every SIP trade of `symbols` stamped in `[start, end)`, every page of it. The endpoint's end is inclusive, so the
    /// request ends a nanosecond before `end`.
    pub async fn trades_between(
        &self,
        symbols: &[Symbol],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<AlpacaTradeOutcome>, FetchError> {
        let names: Vec<&str> = symbols.iter().map(Symbol::as_str).collect();
        let names = names.join(",");
        let (start, end) = (
            start.to_rfc3339(),
            (end - TimeDelta::nanoseconds(1)).to_rfc3339(),
        );
        let (names, start, end) = (&names, &start, &end);
        let mut trades = Vec::new();
        paginate(
            |page_token| async move {
                with_retries(|| {
                    let mut query = vec![
                        ("symbols", names.as_str()),
                        ("start", start.as_str()),
                        ("end", end.as_str()),
                        ("feed", "sip"),
                        ("sort", "asc"),
                        ("limit", PAGE_LIMIT),
                    ];
                    if let Some(token) = page_token.as_deref() {
                        query.push(("page_token", token));
                    }
                    send(
                        self.credentials
                            .sign(self.http_client.get(TRADES_URL))
                            .query(&query),
                    )
                })
                .await
            },
            |page| {
                trades.extend(requested_trades(&page, symbols)?);
                Ok(())
            },
        )
        .await?;
        Ok(trades)
    }

    /// Each of `symbols`' latest SIP quote, read through transient failures; a symbol with no quote is absent.
    pub async fn latest_quotes(
        &self,
        symbols: &[Symbol],
    ) -> Result<Vec<AlpacaQuoteOutcome>, FetchError> {
        let names: Vec<&str> = symbols.iter().map(Symbol::as_str).collect();
        let names = names.join(",");
        let body = with_retries(|| {
            send(
                self.credentials
                    .sign(self.http_client.get(LATEST_QUOTES_URL))
                    .query(&[("symbols", names.as_str()), ("feed", "sip")]),
            )
        })
        .await?;
        latest_quotes(&body, symbols)
    }
}

/// The latest quotes for `requested`, a quote filed under any other ticker refused.
fn latest_quotes(body: &[u8], requested: &[Symbol]) -> Result<Vec<AlpacaQuoteOutcome>, FetchError> {
    let page: LatestQuotes =
        serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
            reason: error.to_string(),
        })?;
    Ok(page
        .quotes
        .into_iter()
        .map(|(ticker, row)| match Symbol::new(&ticker) {
            Ok(symbol) if requested.contains(&symbol) => quote_outcome(&symbol, &row),
            Ok(_) => AlpacaQuoteOutcome::Refused(RefusedRow {
                ticker,
                cause: RowRefusal::Unrequested,
            }),
            Err(cause) => AlpacaQuoteOutcome::Refused(RefusedRow {
                ticker,
                cause: RowRefusal::Symbol(cause),
            }),
        })
        .collect())
}

/// One page of trades for `requested`, a row filed under any other ticker refused.
fn requested_trades(
    body: &[u8],
    requested: &[Symbol],
) -> Result<Vec<AlpacaTradeOutcome>, FetchError> {
    let page: TradesPage = serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
        reason: error.to_string(),
    })?;
    let mut trades = Vec::new();
    for (ticker, rows) in page.trades.unwrap_or_default() {
        for row in rows {
            trades.push(match Symbol::new(&ticker) {
                Ok(symbol) if requested.contains(&symbol) => trade_outcome(&symbol, &row),
                Ok(_) => AlpacaTradeOutcome::Refused(RefusedRow {
                    ticker: ticker.clone(),
                    cause: RowRefusal::Unrequested,
                }),
                Err(cause) => AlpacaTradeOutcome::Refused(RefusedRow {
                    ticker: ticker.clone(),
                    cause: RowRefusal::Symbol(cause),
                }),
            });
        }
    }
    Ok(trades)
}

/// One page of `symbol`'s quotes, refusing any row filed under a ticker that was not asked for.
fn quote_page(symbol: &Symbol, body: &[u8]) -> Result<(Vec<AlpacaQuoteOutcome>, bool), FetchError> {
    let page: QuotesPage = serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
        reason: error.to_string(),
    })?;
    let mut outcomes = Vec::new();
    let mut answered = false;
    for (ticker, rows) in page.quotes.unwrap_or_default() {
        answered |= ticker == symbol.as_str() && !rows.is_empty();
        for row in rows {
            outcomes.push(match ticker == symbol.as_str() {
                true => quote_outcome(symbol, &row),
                false => AlpacaQuoteOutcome::Refused(RefusedRow {
                    ticker: ticker.clone(),
                    cause: RowRefusal::Unrequested,
                }),
            });
        }
    }
    Ok((outcomes, answered))
}

fn quote_outcome(symbol: &Symbol, row: &AlpacaQuote) -> AlpacaQuoteOutcome {
    let refused = |cause| {
        AlpacaQuoteOutcome::Refused(RefusedRow {
            ticker: symbol.as_str().to_string(),
            cause,
        })
    };
    let origin = QuoteOrigin {
        bid_exchange: row.bid_exchange.clone(),
        ask_exchange: row.ask_exchange.clone(),
        tape: row.tape.clone(),
        conditions: row.conditions.clone(),
        indicators: vec![],
        participant_timestamp: None,
        sequence: None,
    };
    match priced_side((row.bid_price, row.bid_size), (row.ask_price, row.ask_size)) {
        Some(Ok(priced)) => {
            return AlpacaQuoteOutcome::OneSided {
                quote: OneSidedQuote {
                    symbol: symbol.clone(),
                    timestamp: row.timestamp,
                    priced,
                },
                origin,
            };
        }
        Some(Err(cause)) => return refused(cause),
        None => {}
    }
    let parts = (|| {
        Ok::<_, RowRefusal>((
            Price::from_dollars(row.bid_price).map_err(RowRefusal::Price)?,
            Price::from_dollars(row.ask_price).map_err(RowRefusal::Price)?,
            Shares::from_float(row.bid_size).map_err(RowRefusal::Shares)?,
            Shares::from_float(row.ask_size).map_err(RowRefusal::Shares)?,
        ))
    })();
    match parts {
        Ok((bid, ask, bid_size, ask_size)) => {
            match Quote::new(symbol.clone(), row.timestamp, bid, ask, bid_size, ask_size) {
                Ok(quote) => AlpacaQuoteOutcome::Quote { quote, origin },
                Err(cause) => refused(RowRefusal::Quote(cause)),
            }
        }
        Err(cause) => refused(cause),
    }
}

/// One page of `symbol`'s trades, refusing any row filed under a ticker that was not asked for.
pub(crate) fn trade_page(
    symbol: &Symbol,
    body: &[u8],
) -> Result<(Vec<AlpacaTradeOutcome>, bool), FetchError> {
    let page: TradesPage = serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
        reason: error.to_string(),
    })?;
    let mut outcomes = Vec::new();
    let mut answered = false;
    for (ticker, rows) in page.trades.unwrap_or_default() {
        answered |= ticker == symbol.as_str() && !rows.is_empty();
        for row in rows {
            outcomes.push(match ticker == symbol.as_str() {
                true => trade_outcome(symbol, &row),
                false => AlpacaTradeOutcome::Refused(RefusedRow {
                    ticker: ticker.clone(),
                    cause: RowRefusal::Unrequested,
                }),
            });
        }
    }
    Ok((outcomes, answered))
}

fn trade_outcome(symbol: &Symbol, row: &AlpacaTrade) -> AlpacaTradeOutcome {
    let refused = |cause| {
        AlpacaTradeOutcome::Refused(RefusedRow {
            ticker: symbol.as_str().to_string(),
            cause,
        })
    };
    let tape = match tape_of(&row.tape) {
        Some(tape) => tape,
        None => {
            return refused(RowRefusal::Tape {
                raw: row.tape.clone(),
            });
        }
    };
    let letters = row
        .conditions
        .iter()
        .map(|element| ConditionLetter::new(element).unwrap_or(UNSPELLABLE))
        .collect();
    let price = match Price::from_dollars(row.price) {
        Ok(price) => price,
        Err(cause) => return refused(RowRefusal::Price(cause)),
    };
    let size = match Shares::from_float(row.size) {
        Ok(size) => size,
        Err(cause) => return refused(RowRefusal::Shares(cause)),
    };
    let correction = match correction(row.update.as_deref()) {
        Ok(correction) => correction,
        Err(cause) => return refused(cause),
    };
    let print = Print::new(symbol.clone(), row.timestamp, price, size);
    let origin = PrintOrigin {
        exchange: row.exchange.clone(),
        tape: row.tape.clone(),
        id: row.id.map(|id| id.to_string()),
        participant_timestamp: None,
        trade_reporting_facility_timestamp: None,
        sequence: None,
    };
    AlpacaTradeOutcome::Print {
        print,
        tape,
        letters,
        correction,
        origin,
    }
}

/// The tape a letter as Alpaca spells it reports under, or none for any other spelling.
pub fn tape_of(letter: &str) -> Option<Tape> {
    match letter.parse::<TapeLetter>() {
        Ok(TapeLetter::A | TapeLetter::B) => Some(Tape::ConsolidatedTape),
        Ok(TapeLetter::C) => Some(Tape::UnlistedTrading),
        Err(strum::ParseError::VariantNotFound) => None,
    }
}

/// The raw tee's lines from a gzipped object, one response page each, each paired with its line number from one.
fn raw_pages(
    gzipped: impl std::io::Read,
) -> impl Iterator<Item = Result<(u64, String), ParseRefusal>> {
    let decoded = flate2::read::MultiGzDecoder::new(gzipped);
    (1..)
        .zip(std::io::BufRead::lines(std::io::BufReader::new(decoded)))
        .map(|(line, text)| {
            text.map(|text| (line, text))
                .map_err(|error| ParseRefusal::Malformed {
                    line: Some(line),
                    reason: error.to_string(),
                })
        })
}

/// Reads a gzipped raw tee object of Alpaca quotes, handing each row's outcome to `each` in file order, read under the
/// ticker it is filed under.
pub fn read_raw_quotes(
    gzipped: impl std::io::Read,
    mut each: impl FnMut(AlpacaQuoteOutcome),
) -> Result<(), ParseRefusal> {
    for page in raw_pages(gzipped) {
        let (line, text) = page?;
        let page: QuotesPage =
            serde_json::from_str(&text).map_err(|error| ParseRefusal::Malformed {
                line: Some(line),
                reason: error.to_string(),
            })?;
        for (ticker, rows) in page.quotes.unwrap_or_default() {
            let symbol = Symbol::new(&ticker);
            for row in rows {
                each(match &symbol {
                    Ok(symbol) => quote_outcome(symbol, &row),
                    Err(cause) => AlpacaQuoteOutcome::Refused(RefusedRow {
                        ticker: ticker.clone(),
                        cause: RowRefusal::Symbol(cause.clone()),
                    }),
                });
            }
        }
    }
    Ok(())
}

/// Reads a gzipped raw tee object of Alpaca trades, handing each row's outcome to `each` in file order, read under the
/// ticker it is filed under.
pub fn read_raw_trades(
    gzipped: impl std::io::Read,
    mut each: impl FnMut(AlpacaTradeOutcome),
) -> Result<(), ParseRefusal> {
    for page in raw_pages(gzipped) {
        let (line, text) = page?;
        let page: TradesPage =
            serde_json::from_str(&text).map_err(|error| ParseRefusal::Malformed {
                line: Some(line),
                reason: error.to_string(),
            })?;
        for (ticker, rows) in page.trades.unwrap_or_default() {
            let symbol = Symbol::new(&ticker);
            for row in rows {
                each(match &symbol {
                    Ok(symbol) => trade_outcome(symbol, &row),
                    Err(cause) => AlpacaTradeOutcome::Refused(RefusedRow {
                        ticker: ticker.clone(),
                        cause: RowRefusal::Symbol(cause.clone()),
                    }),
                });
            }
        }
    }
    Ok(())
}

/// Reads Alpaca's update label: `incorrect` marks the record that replaces a corrected print, the one that stands, while
/// `corrected` marks the original it replaced and `canceled` a print withdrawn; any other label is refused with itself.
fn correction(update: Option<&str>) -> Result<Correction, RowRefusal> {
    match update {
        None | Some("incorrect") => Ok(Correction::Stands),
        Some("corrected" | "canceled") => Ok(Correction::Withdrawn),
        Some(label) => Err(RowRefusal::Correction {
            raw: label.to_string(),
        }),
    }
}

/// The account a paper flag names.
fn account(is_paper: String) -> Result<Account, VariableRefusal> {
    match is_paper.to_ascii_lowercase().parse::<bool>() {
        Ok(true) => Ok(Account::Paper),
        Ok(false) => Ok(Account::Live),
        Err(_) => Err(VariableRefusal::Malformed {
            name: "ALPACA_IS_PAPER",
            raw: is_paper,
        }),
    }
}

/// The listing's rows, a row whose symbol does not read refused rather than dropped; a body that is not the listing is
/// malformed.
fn parse_borrow_flags(body: &[u8]) -> Result<BorrowFlagsRead, FetchError> {
    let rows: Vec<AssetRow> =
        serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
            reason: error.to_string(),
        })?;
    let mut read = BorrowFlagsRead::default();
    for row in rows {
        match Symbol::new(&row.symbol) {
            Ok(symbol) => {
                read.flags
                    .push(BorrowFlags::new(symbol, row.shortable, row.easy_to_borrow))
            }
            Err(cause) => read.refused.push(RefusedRow {
                ticker: row.symbol,
                cause: RowRefusal::Symbol(cause),
            }),
        }
    }
    Ok(read)
}

fn parse_calendar(body: &[u8], range: SessionRange) -> Result<TradingCalendar, FetchError> {
    let malformed = |reason: String| FetchError::Malformed { reason };
    let rows: Vec<CalendarRow> =
        serde_json::from_slice(body).map_err(|error| malformed(error.to_string()))?;
    let sessions = rows
        .iter()
        .map(|row| {
            TradingSession::new(SessionDate::from_date(row.date), row.open, row.close)
                .map_err(|refusal| malformed(format!("{refusal:?}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    TradingCalendar::new(sessions, range).map_err(|refusal| malformed(format!("{refusal:?}")))
}

/// Follows `next_page_token` until it is null, handing each page to `read_page` before fetching the next.
async fn paginate<Fetch, Pending, Read>(
    mut fetch_page: Fetch,
    mut read_page: Read,
) -> Result<(), FetchError>
where
    Fetch: FnMut(Option<String>) -> Pending,
    Pending: std::future::Future<Output = Result<Vec<u8>, FetchError>>,
    Read: FnMut(Vec<u8>) -> Result<(), FetchError>,
{
    let mut tokens = PageTokens::default();
    let mut token = None;
    loop {
        let body = fetch_page(token).await?;
        let next = tokens.next(&body)?;
        read_page(body)?;
        match next {
            None => return Ok(()),
            Some(next) => token = Some(next),
        }
    }
}

/// The page tokens seen so far. A token seen before means the pages cycle, which would otherwise request forever, so
/// it is refused.
#[derive(Default)]
struct PageTokens {
    seen: BTreeSet<String>,
}

impl PageTokens {
    /// The token `body` names for the next page, `None` on the last.
    fn next(&mut self, body: &[u8]) -> Result<Option<String>, FetchError> {
        match next_page_token(body)? {
            Some(next) if !self.seen.insert(next.clone()) => Err(FetchError::Malformed {
                reason: format!("page token {next} repeated"),
            }),
            next => Ok(next),
        }
    }
}

fn next_page_token(body: &[u8]) -> Result<Option<String>, FetchError> {
    let page: PageToken = serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
        reason: error.to_string(),
    })?;
    Ok(page.next_page_token)
}

/// The symbol an Alpaca 400 names, from a body such as `{"message":"invalid symbol: BC-C"}`.
pub(crate) fn invalid_symbol(body: &str) -> Option<String> {
    let error: ErrorBody = serde_json::from_str(body).ok()?;
    error
        .message
        .strip_prefix("invalid symbol: ")
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;
    use crate::ingest::PricedSide;

    #[test]
    fn test_an_invalid_symbol_is_read_from_the_live_error_body() {
        assert_eq!(
            invalid_symbol(r#"{"message":"invalid symbol: BC-C"}"#),
            Some("BC-C".to_string())
        );
        assert_eq!(invalid_symbol(r#"{"message":"forbidden"}"#), None);
        assert_eq!(invalid_symbol("<html>"), None);
    }

    fn page(token: Option<&str>) -> Vec<u8> {
        let token = token.map_or("null".to_string(), |token| format!("\"{token}\""));
        format!(r#"{{"trades":{{}},"next_page_token":{token}}}"#).into_bytes()
    }

    #[test]
    fn test_a_page_names_the_next_token_until_the_last() {
        let mut tokens = PageTokens::default();
        assert_eq!(tokens.next(&page(Some("a"))), Ok(Some("a".to_string())));
        assert_eq!(tokens.next(&page(Some("b"))), Ok(Some("b".to_string())));
        assert_eq!(tokens.next(&page(None)), Ok(None));
        assert!(matches!(
            tokens.next(b"<html>"),
            Err(FetchError::Malformed { .. })
        ));
    }

    #[test]
    fn test_the_asset_listing_reads_its_borrow_flags_and_refuses_an_unreadable_symbol() {
        // Rows shaped as the live listing sends them (probed 2026-11-07), with synthetic names and values.
        let row = |symbol: &str, shortable: bool, easy: bool| {
            format!(
                r#"{{"id":"e3d28abe-4e29-408d-8cc1-356777ae3c8a","class":"us_equity","exchange":"NASDAQ","symbol":"{symbol}","name":"Example","status":"active","tradable":true,"marginable":true,"maintenance_margin_requirement":30,"margin_requirement_long":"50","margin_requirement_short":"50","shortable":{shortable},"easy_to_borrow":{easy},"borrow_status":"easy_to_borrow","fractionable":true,"attributes":[]}}"#
            )
        };
        let body = format!(
            "[{},{},{}]",
            row("AAA", true, true),
            row("BB.A", false, true),
            row("bad-1", true, false)
        );
        let read = parse_borrow_flags(body.as_bytes()).unwrap();
        assert_eq!(
            read.flags,
            [
                BorrowFlags::new(Symbol::new("AAA").unwrap(), true, true),
                BorrowFlags::new(Symbol::new("BB.A").unwrap(), false, true),
            ]
        );
        assert_eq!(
            read.refused,
            [RefusedRow {
                ticker: "bad-1".to_string(),
                cause: RowRefusal::Symbol(Symbol::new("bad-1").unwrap_err()),
            }]
        );
        assert!(matches!(
            parse_borrow_flags(b"{}"),
            Err(FetchError::Malformed { .. })
        ));
    }

    #[tokio::test]
    async fn test_pages_are_followed_until_the_token_is_null() {
        let requested = std::cell::RefCell::new(Vec::new());
        let mut pages = Vec::new();
        paginate(
            |token| {
                requested.borrow_mut().push(token.clone());
                async move {
                    Ok(match token.as_deref() {
                        None => page(Some("a")),
                        Some("a") => page(Some("b")),
                        _ => page(None),
                    })
                }
            },
            |body| {
                pages.push(body);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(pages, [page(Some("a")), page(Some("b")), page(None)]);
        assert_eq!(
            *requested.borrow(),
            [None, Some("a".to_string()), Some("b".to_string())]
        );
    }

    #[tokio::test]
    async fn test_a_cycling_page_token_is_refused() {
        let calls = std::cell::Cell::new(0);
        let result = paginate(
            |token| {
                calls.set(calls.get() + 1);
                // A cap, so a broken cycle check fails this test instead of hanging it.
                let exhausted = calls.get() > 10;
                async move {
                    if exhausted {
                        return Err(FetchError::Exhausted {
                            attempts: 10,
                            last: "cycle never detected".to_string(),
                        });
                    }
                    Ok(match token.as_deref() {
                        None | Some("b") => page(Some("a")),
                        _ => page(Some("b")),
                    })
                }
            },
            |_| Ok(()),
        )
        .await;
        assert_eq!(
            result,
            Err(FetchError::Malformed {
                reason: "page token a repeated".to_string()
            })
        );
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn test_a_page_that_cannot_be_read_stops_the_fetching() {
        let calls = std::cell::Cell::new(0);
        let result = paginate(
            |_| {
                calls.set(calls.get() + 1);
                async { Ok(page(Some("a"))) }
            },
            |_| {
                Err(FetchError::Malformed {
                    reason: "unreadable".to_string(),
                })
            },
        )
        .await;
        assert_eq!(
            result,
            Err(FetchError::Malformed {
                reason: "unreadable".to_string()
            })
        );
        assert_eq!(calls.get(), 1);
    }

    /// Alpaca's calendar over Thanksgiving 2026, whose hours are the exchange's published schedule.
    const CALENDAR: &[u8] = br#"[{"close":"16:00","date":"2026-11-25","open":"09:30","session_close":"2000","session_open":"0400","settlement_date":"2026-11-27"},{"close":"13:00","date":"2026-11-27","open":"09:30","session_close":"1700","session_open":"0400","settlement_date":"2026-11-30"},{"close":"16:00","date":"2026-11-30","open":"09:30","session_close":"2000","session_open":"0400","settlement_date":"2026-12-01"}]"#;

    fn day(text: &str) -> SessionDate {
        SessionDate::from_date(text.parse().unwrap())
    }

    /// The keys reach a request's headers and never a printed client.
    #[test]
    fn test_credentials_sign_a_request_and_print_no_key() {
        let credentials = Credentials {
            key_id: Secret::new("key-id-value".to_string()),
            secret: Secret::new("secret-value".to_string()),
        };
        assert_eq!(
            format!("{credentials:?}"),
            "Credentials { key_id: Secret(..), secret: Secret(..) }"
        );
        let request = credentials
            .sign(reqwest::Client::new().get("https://example.com"))
            .build()
            .unwrap();
        let headers: Vec<(&str, &str)> = request
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.to_str().unwrap()))
            .collect();
        assert_eq!(
            headers,
            [
                ("apca-api-key-id", "key-id-value"),
                ("apca-api-secret-key", "secret-value")
            ]
        );
    }

    #[test]
    fn test_the_paper_flag_is_read_in_any_case_and_nothing_else() {
        for raw in ["true", "TRUE", "True"] {
            assert_eq!(
                account(raw.to_string()).map(Account::trading_url),
                Ok("https://paper-api.alpaca.markets")
            );
        }
        for raw in ["false", "FALSE", "False"] {
            assert_eq!(
                account(raw.to_string()).map(Account::trading_url),
                Ok("https://api.alpaca.markets")
            );
        }
        for raw in ["", "yes", "1", " true"] {
            assert_eq!(
                account(raw.to_string()),
                Err(VariableRefusal::Malformed {
                    name: "ALPACA_IS_PAPER",
                    raw: raw.to_string()
                })
            );
        }
    }

    fn range(first: &str, last: &str) -> SessionRange {
        SessionRange::new(day(first), day(last)).unwrap()
    }

    #[test]
    fn test_the_calendar_reads_holidays_and_early_closes() {
        let calendar = parse_calendar(CALENDAR, range("2026-11-25", "2026-11-30")).unwrap();
        let trading: Vec<String> = calendar
            .trading_days_in_range(range("2026-11-25", "2026-11-30"))
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(trading, ["2026-11-25", "2026-11-27", "2026-11-30"]);
        let early: Vec<(String, String)> = calendar
            .early_closes()
            .map(|session| (session.date().to_string(), session.close().to_string()))
            .collect();
        assert_eq!(early, [("2026-11-27".to_string(), "13:00:00".to_string())]);
    }

    #[test]
    fn test_one_unreadable_calendar_row_refuses_the_calendar() {
        let unreadable = String::from_utf8(CALENDAR.to_vec())
            .unwrap()
            .replace(r#""close":"13:00""#, r#""close":"1300""#);
        assert!(matches!(
            parse_calendar(unreadable.as_bytes(), range("2026-11-25", "2026-11-30")),
            Err(FetchError::Malformed { .. })
        ));
        // A range narrower than the answer is a row outside the calendar, never a row silently dropped.
        assert!(matches!(
            parse_calendar(CALENDAR, range("2026-11-25", "2026-11-27")),
            Err(FetchError::Malformed { .. })
        ));
    }

    /// SIP quotes and trades for ABC on 2026-10-02 with invented values in Alpaca's shape: a one-sided quote, the
    /// closing cross, a canceled print, a zero-size print and a tape `E` to exercise the refusal.
    const QUOTES_PAGE: &str = r#"{"next_page_token": null, "quotes": {"ABC": [{"ap": 50.27, "as": 200, "ax": "Q", "bp": 50.25, "bs": 1530, "bx": "Q", "c": ["R"], "t": "2026-10-02T19:59:58.000000123Z", "z": "C"}, {"ap": 0, "as": 0, "ax": "Q", "bp": 50.25, "bs": 100, "bx": "Q", "c": ["R"], "t": "2026-10-02T19:59:58.0005Z", "z": "C"}]}}"#;
    const TRADES_PAGE: &str = r#"{"next_page_token": null, "trades": {"ABC": [{"c": ["@", "6", "X"], "i": 1, "p": 50.26, "s": 6000000, "t": "2026-10-02T20:00:00.125Z", "x": "Q", "z": "C"}, {"c": ["@", "T", "P"], "i": 300001, "p": 50.26, "s": 800000, "t": "2026-10-02T21:45:00.000000456Z", "u": "canceled", "x": "D", "z": "C"}, {"c": [" ", "9"], "i": 7, "p": 25.5, "s": 0, "t": "2026-10-02T20:10:00.001Z", "x": "N", "z": "A"}, {"c": ["@"], "i": 8, "p": 1.0, "s": 1, "t": "2026-10-02T20:10:00.001Z", "x": "N", "z": "E"}]}}"#;

    #[test]
    fn test_alpaca_quotes_keep_the_top_of_book_in_shares() {
        let symbol = Symbol::new("ABC").unwrap();
        let (outcomes, answered) = quote_page(&symbol, QUOTES_PAGE.as_bytes()).unwrap();
        assert!(answered);
        assert_eq!(outcomes.len(), 2);
        match &outcomes[0] {
            AlpacaQuoteOutcome::Quote { quote, origin } => {
                assert_eq!(quote.bid().ticks(), 50_250_000);
                assert_eq!(quote.bid_size().units(), 1_530_000_000);
                assert_eq!(
                    quote.timestamp().to_rfc3339(),
                    "2026-10-02T19:59:58.000000123+00:00"
                );
                assert_eq!(
                    origin,
                    &QuoteOrigin {
                        bid_exchange: Some("Q".to_string()),
                        ask_exchange: Some("Q".to_string()),
                        tape: Some("C".to_string()),
                        conditions: vec!["R".to_string()],
                        indicators: vec![],
                        participant_timestamp: None,
                        sequence: None,
                    }
                );
            }
            other @ (AlpacaQuoteOutcome::OneSided { .. } | AlpacaQuoteOutcome::Refused(_)) => {
                panic!("{other:?}")
            }
        }
        match &outcomes[1] {
            AlpacaQuoteOutcome::OneSided { quote, .. } => {
                assert_eq!(
                    quote.priced,
                    PricedSide::Bid(
                        Price::from_ticks(50_250_000).unwrap(),
                        Shares::from_units(100_000_000)
                    )
                );
            }
            other @ (AlpacaQuoteOutcome::Quote { .. } | AlpacaQuoteOutcome::Refused(_)) => {
                panic!("{other:?}")
            }
        }
    }

    /// The latest-quotes response as Alpaca served it on 2026-11-08, one object per ticker, with synthetic values.
    const LATEST_QUOTES: &str = r#"{"quotes":{"ABC":{"ap":50.27,"as":40,"ax":"K","bp":50.25,"bs":320,"bx":"P","c":["R"],"t":"2026-11-07T00:59:51.009613177Z","z":"C"},"XYZ":{"ap":20.01,"as":1,"ax":"P","bp":20.0,"bs":2,"bx":"P","c":["R"],"t":"2026-11-07T01:00:00.161965348Z","z":"B"}}}"#;

    #[test]
    fn test_latest_quotes_read_each_requested_ticker_and_refuse_another() {
        let abc = Symbol::new("ABC").unwrap();
        let outcomes = latest_quotes(LATEST_QUOTES.as_bytes(), std::slice::from_ref(&abc)).unwrap();
        assert_eq!(outcomes.len(), 2);
        match &outcomes[0] {
            AlpacaQuoteOutcome::Quote { quote, .. } => {
                assert_eq!(quote.symbol(), &abc);
                assert_eq!(quote.bid().ticks(), 50_250_000);
                assert_eq!(quote.ask().ticks(), 50_270_000);
                assert_eq!(
                    quote.timestamp().to_rfc3339(),
                    "2026-11-07T00:59:51.009613177+00:00"
                );
            }
            other @ (AlpacaQuoteOutcome::OneSided { .. } | AlpacaQuoteOutcome::Refused(_)) => {
                panic!("{other:?}")
            }
        }
        match &outcomes[1] {
            AlpacaQuoteOutcome::Refused(row) => {
                assert_eq!(row.ticker(), "XYZ");
                assert_eq!(*row.cause(), RowRefusal::Unrequested);
            }
            other @ (AlpacaQuoteOutcome::Quote { .. } | AlpacaQuoteOutcome::OneSided { .. }) => {
                panic!("{other:?}")
            }
        }
    }

    #[test]
    fn test_the_correction_record_stands_and_what_it_replaces_or_cancels_is_withdrawn() {
        let read = [
            None,
            Some("incorrect"),
            Some("corrected"),
            Some("canceled"),
            Some("unheard"),
        ]
        .map(correction);
        assert_eq!(
            read,
            [
                Ok(Correction::Stands),
                Ok(Correction::Stands),
                Ok(Correction::Withdrawn),
                Ok(Correction::Withdrawn),
                Err(RowRefusal::Correction {
                    raw: "unheard".to_string()
                }),
            ]
        );
    }

    #[test]
    fn test_a_tape_letter_is_spelled_as_alpaca_spells_it() {
        let spelled: Vec<&'static str> = TapeLetter::iter().map(<&'static str>::from).collect();
        assert_eq!(spelled, ["A", "B", "C"]);
        for letter in TapeLetter::iter() {
            assert_eq!(letter.to_string().parse::<TapeLetter>(), Ok(letter));
        }
        assert!("a".parse::<TapeLetter>().is_err());
    }

    /// The raw tee as `RawWriter` stores it: one gzip stream holding a line per page.
    fn tee(pages: &[&str]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        for page in pages {
            writeln!(encoder, "{page}").unwrap();
        }
        encoder.finish().unwrap()
    }

    /// A raw object holds many pages of many tickers, so each row reads under its own and a ticker that is no symbol is
    /// refused.
    #[test]
    fn test_a_raw_object_reads_every_ticker_of_every_page() {
        let trades = tee(&[
            r#"{"next_page_token": "x", "trades": {"ABC": [{"c": ["@"], "i": 1, "p": 50.26, "s": 100, "t": "2026-10-09T14:00:00Z", "x": "Q", "z": "C"}], "XYZ": [{"c": [" "], "i": 2, "p": 10.5, "s": 5, "t": "2026-10-09T14:00:01Z", "x": "N", "z": "A"}]}}"#,
            r#"{"next_page_token": null, "trades": {"abc1": [{"c": [" "], "i": 3, "p": 1.0, "s": 1, "t": "2026-10-09T14:00:02Z", "x": "N", "z": "A"}]}}"#,
        ]);
        let mut read = Vec::new();
        read_raw_trades(trades.as_slice(), |outcome| {
            read.push(match outcome {
                AlpacaTradeOutcome::Print { print, origin, .. } => {
                    format!("{} {:?}", print.symbol().as_str(), origin.exchange)
                }
                AlpacaTradeOutcome::Refused(row) => format!("refused {}", row.ticker()),
            });
        })
        .unwrap();
        assert_eq!(read, ["ABC Some(\"Q\")", "XYZ Some(\"N\")", "refused abc1"]);
        let quotes = tee(&[
            r#"{"next_page_token": null, "quotes": {"ABC": [{"ap": 50.27, "as": 200, "ax": "Q", "bp": 50.25, "bs": 100, "bx": "P", "c": ["R"], "t": "2026-10-09T14:00:00Z", "z": "C"}], "XYZ": [{"ap": 0, "as": 0, "ax": " ", "bp": 10.5, "bs": 100, "bx": "N", "c": ["R"], "t": "2026-10-09T14:00:01Z", "z": "A"}]}}"#,
        ]);
        let mut read = Vec::new();
        read_raw_quotes(quotes.as_slice(), |outcome| {
            read.push(match outcome {
                AlpacaQuoteOutcome::Quote { quote, origin } => format!(
                    "{} {}/{}",
                    quote.symbol().as_str(),
                    origin.bid_exchange.as_deref().unwrap_or("-"),
                    origin.ask_exchange.as_deref().unwrap_or("-")
                ),
                AlpacaQuoteOutcome::OneSided { quote, .. } => {
                    format!("{} one-sided", quote.symbol.as_str())
                }
                AlpacaQuoteOutcome::Refused(row) => format!("refused {}", row.ticker()),
            });
        })
        .unwrap();
        assert_eq!(read, ["ABC P/Q", "XYZ one-sided"]);
    }

    /// A page that is not JSON refuses the object at its line, after the pages before it were handed out.
    #[test]
    fn test_a_malformed_page_is_refused_at_its_line() {
        let trades = tee(&[
            r#"{"next_page_token": "x", "trades": {"ABC": [{"c": ["@"], "i": 1, "p": 50.26, "s": 100, "t": "2026-10-09T14:00:00Z", "x": "Q", "z": "C"}]}}"#,
            "not a page",
        ]);
        let mut rows = 0;
        let refusal = read_raw_trades(trades.as_slice(), |_| rows += 1).unwrap_err();
        assert_eq!(rows, 1);
        assert!(matches!(
            refusal,
            ParseRefusal::Malformed { line: Some(2), .. }
        ));
    }

    /// An object cut short in transfer is refused rather than read as a shorter session.
    #[test]
    fn test_a_truncated_object_is_refused() {
        let page = r#"{"next_page_token": null, "trades": {"ABC": [{"c": ["@"], "i": 1, "p": 50.26, "s": 100, "t": "2026-10-09T14:00:00Z", "x": "Q", "z": "C"}]}}"#;
        let whole = tee(&[page, page]);
        let refusal = read_raw_trades(&whole[..whole.len() - 4], |_| {}).unwrap_err();
        assert!(matches!(refusal, ParseRefusal::Malformed { .. }));
    }

    #[test]
    fn test_alpaca_trades_carry_their_tape_letters_and_corrections() {
        let symbol = Symbol::new("ABC").unwrap();
        let (outcomes, answered) = trade_page(&symbol, TRADES_PAGE.as_bytes()).unwrap();
        assert!(answered);
        let summary: Vec<String> = outcomes
            .iter()
            .map(|outcome| match outcome {
                AlpacaTradeOutcome::Print {
                    print,
                    tape,
                    letters,
                    correction,
                    origin,
                } => format!(
                    "{tape:?} {:?} {correction:?} unsized={} {}/{}/{}",
                    letters
                        .iter()
                        .map(|letter| letter.get())
                        .collect::<Vec<char>>(),
                    matches!(print, Print::Unsized { .. }),
                    origin.exchange.as_deref().unwrap_or("-"),
                    origin.tape,
                    origin.id.as_deref().unwrap_or("-")
                ),
                AlpacaTradeOutcome::Refused(row) => {
                    format!("refused {}", <&'static str>::from(row.cause().kind()))
                }
            })
            .collect();
        assert_eq!(
            summary,
            [
                "UnlistedTrading ['@', '6', 'X'] Stands unsized=false Q/C/1",
                "UnlistedTrading ['@', 'T', 'P'] Withdrawn unsized=false D/C/300001",
                "ConsolidatedTape [' ', '9'] Stands unsized=true N/A/7",
                "refused tape",
            ]
        );
    }

    /// A page of ABC trades for 2026-10-06 19:59:59 with invented values in the REST history's shape; the token
    /// encodes the next row as Alpaca's tokens do.
    const REQUESTED_PAGE: &str = r#"{"next_page_token":"QUJDfDE3OTEzMTY3OTkwMDAwMDA0MDB8UXw5MDA0","trades":{"ABC":[{"c":["@","F"],"i":9001,"p":50.255,"s":40,"t":"2026-10-06T19:59:59.000000100Z","x":"Q","z":"C"},{"c":["@","F"],"i":9002,"p":50.26,"s":100,"t":"2026-10-06T19:59:59.000000200Z","x":"Q","z":"C"},{"c":["@","F"],"i":9003,"p":50.26,"s":60,"t":"2026-10-06T19:59:59.000000300Z","x":"Q","z":"C"}]}}"#;

    /// The page's rows read as prints in order, an empty page reads as none, and a repeated page token is refused
    /// rather than followed forever.
    #[test]
    fn test_a_rest_page_reads_each_trade_of_the_requested_symbols() {
        let abc = [Symbol::new("ABC").unwrap()];
        let trades = requested_trades(REQUESTED_PAGE.as_bytes(), &abc).unwrap();
        let stamped: Vec<String> = trades
            .iter()
            .map(|outcome| match outcome {
                AlpacaTradeOutcome::Print { print, .. } => print.timestamp().to_rfc3339(),
                AlpacaTradeOutcome::Refused(row) => panic!("{row:?}"),
            })
            .collect();
        assert_eq!(
            stamped,
            [
                "2026-10-06T19:59:59.000000100+00:00",
                "2026-10-06T19:59:59.000000200+00:00",
                "2026-10-06T19:59:59.000000300+00:00"
            ]
        );
        assert_eq!(
            requested_trades(br#"{"trades":null,"next_page_token":null}"#, &abc).unwrap(),
            vec![]
        );
        let mut tokens = PageTokens::default();
        assert_eq!(
            tokens.next(REQUESTED_PAGE.as_bytes()).unwrap().as_deref(),
            Some("QUJDfDE3OTEzMTY3OTkwMDAwMDA0MDB8UXw5MDA0")
        );
        assert!(matches!(
            tokens.next(REQUESTED_PAGE.as_bytes()),
            Err(FetchError::Malformed { .. })
        ));
    }

    /// A row filed under a ticker the request did not name is refused, never handed out as a print.
    #[test]
    fn test_a_row_for_an_unrequested_ticker_is_refused() {
        let trades =
            requested_trades(REQUESTED_PAGE.as_bytes(), &[Symbol::new("SPY").unwrap()]).unwrap();
        assert_eq!(trades.len(), 3);
        assert!(trades.iter().all(|outcome| matches!(
            outcome,
            AlpacaTradeOutcome::Refused(row) if *row.cause() == RowRefusal::Unrequested
        )));
    }

    #[tokio::test]
    #[ignore = "reads Alpaca's asset listing; run deliberately under secretspec"]
    async fn live_the_asset_listing_reads_whole() {
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let (read, bodies) = alpaca.borrow_flags().await.into_parts();
        let read = read.unwrap();
        assert_eq!(bodies.len(), 1);
        // About 14,000 active assets on 2026-11-07, every symbol reading, a third of them borrowable.
        assert!(read.flags.len() > 10_000, "{}", read.flags.len());
        assert!(read.refused.is_empty(), "{:?}", read.refused);
        let available = read
            .flags
            .iter()
            .filter(|flags| {
                matches!(
                    flags.borrow(),
                    crate::common::market::borrow::Borrow::Available
                )
            })
            .count();
        assert!(
            available > 3_000 && available < read.flags.len(),
            "{available}"
        );
    }

    /// Two adjacent minutes of the 2026-10-12 open hold exactly the trades of the two-minute window, every field
    /// compared, and every print is inside the minute asked for.
    #[tokio::test]
    #[ignore = "reads Alpaca's REST trade history; run deliberately under secretspec"]
    async fn live_adjacent_minutes_partition_their_window() {
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let symbols = [Symbol::new("SPY").unwrap(), Symbol::new("QQQ").unwrap()];
        let at = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        let (open, minute, two) = (
            at("2026-10-12T13:30:00Z"),
            at("2026-10-12T13:31:00Z"),
            at("2026-10-12T13:32:00Z"),
        );
        let printed = |trades: Vec<AlpacaTradeOutcome>, start, end| -> Vec<String> {
            trades
                .into_iter()
                .map(|outcome| match outcome {
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        correction,
                        ..
                    } => {
                        assert!(
                            start <= print.timestamp() && print.timestamp() < end,
                            "{print:?}"
                        );
                        format!("{print:?} {tape:?} {letters:?} {correction:?}")
                    }
                    AlpacaTradeOutcome::Refused(row) => panic!("{row:?}"),
                })
                .collect()
        };
        let first = printed(
            alpaca.trades_between(&symbols, open, minute).await.unwrap(),
            open,
            minute,
        );
        let second = printed(
            alpaca.trades_between(&symbols, minute, two).await.unwrap(),
            minute,
            two,
        );
        let mut whole = printed(
            alpaca.trades_between(&symbols, open, two).await.unwrap(),
            open,
            two,
        );
        println!(
            "{} + {} = {} trades",
            first.len(),
            second.len(),
            whole.len()
        );
        assert!(!first.is_empty() && !second.is_empty());
        // Compared as multisets of every field, so a print dropped and another repeated cannot balance out.
        let mut split = [first, second].concat();
        split.sort();
        whole.sort();
        assert_eq!(split, whole);
    }

    #[test]
    fn test_alpaca_ticks_filed_under_another_ticker_are_refused_not_dropped() {
        let symbol = Symbol::new("ABD").unwrap();
        let unrequested = AlpacaQuoteOutcome::Refused(RefusedRow {
            ticker: "ABC".to_string(),
            cause: RowRefusal::Unrequested,
        });
        // Rows filed only under another ticker refuse each row and leave the symbol unanswered.
        assert_eq!(
            quote_page(&symbol, QUOTES_PAGE.as_bytes()).unwrap(),
            (vec![unrequested.clone(), unrequested], false)
        );
        let (trades, answered) = trade_page(&symbol, TRADES_PAGE.as_bytes()).unwrap();
        assert!(!answered);
        assert_eq!(trades.len(), 4);
        assert!(trades.iter().all(|outcome| matches!(
            outcome,
            AlpacaTradeOutcome::Refused(RefusedRow {
                cause: RowRefusal::Unrequested,
                ..
            })
        )));
        assert_eq!(
            quote_page(&symbol, br#"{"next_page_token": null, "quotes": null}"#).unwrap(),
            (vec![], false)
        );
    }
}
