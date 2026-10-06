//! Alpaca's historical SIP bars, fetched for many symbols at once and named as they were on the session, and its
//! published trading calendar.

pub mod stream;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

use super::retry::{FetchError, send, with_retries};
use super::{Accepted, RefusedRow, RowRefusal, VariableRefusal, variable};
use crate::common::market::record::{Bar, BarInterval, Ohlc, Quote, Trade};
use crate::common::market::trade_bars::{Print, Tape, condition_letter};
use crate::common::market::{DollarVolume, Price, Shares, Symbol, TradeCount};
use crate::common::monoid::Monoid;
use crate::common::time::SessionDate;
use crate::common::time::calendar::{TradingCalendar, TradingSession};

const BARS_URL: &str = "https://data.alpaca.markets/v2/stocks/bars";
const QUOTES_URL: &str = "https://data.alpaca.markets/v2/stocks/quotes";
const TRADES_URL: &str = "https://data.alpaca.markets/v2/stocks/trades";

/// The trading API, which serves the calendar; paper and live keys each work only against their own.
const PAPER_TRADING_URL: &str = "https://paper-api.alpaca.markets";
const LIVE_TRADING_URL: &str = "https://api.alpaca.markets";

/// Bars per page, the endpoint's maximum.
const PAGE_LIMIT: &str = "10000";

pub struct Alpaca {
    http_client: reqwest::Client,
    key_id: String,
    secret: String,
    trading_url: &'static str,
}

/// One batch's one-minute bars. Every symbol asked for is exactly one of answered (its rows are bars or refusals),
/// missing or invalid, and every row is a bar or a refusal.
#[derive(Debug, Clone, PartialEq)]
pub struct MinuteBars {
    bars: Vec<Bar>,
    missing: Vec<Symbol>,
    invalid: Vec<Symbol>,
    refused: Vec<RefusedRow>,
}

impl MinuteBars {
    /// Reads the pages answering for `requested`; `invalid` are the symbols dropped before they were fetched.
    fn from_pages(
        pages: &[Vec<u8>],
        requested: &[Symbol],
        invalid: Vec<Symbol>,
        session: SessionDate,
    ) -> Result<Self, FetchError> {
        let asked: BTreeSet<&str> = requested.iter().map(Symbol::as_str).collect();
        let mut answered = BTreeSet::new();
        let mut accepted = Accepted::new();
        for page in pages {
            let page: BarsPage =
                serde_json::from_slice(page).map_err(|error| FetchError::Malformed {
                    reason: error.to_string(),
                })?;
            for (ticker, rows) in page.bars.unwrap_or_default() {
                let was_asked = asked.contains(ticker.as_str());
                for row in rows {
                    let bar = match was_asked {
                        true => minute_bar(&ticker, &row, session),
                        false => Err(RowRefusal::Unrequested),
                    };
                    match bar {
                        Ok(bar) => accepted.offer(
                            (bar.symbol().clone(), bar.timestamp()),
                            ticker.clone(),
                            bar,
                        ),
                        Err(cause) => accepted.refuse(ticker.clone(), cause),
                    }
                }
                if was_asked {
                    answered.insert(ticker);
                }
            }
        }
        let missing = requested
            .iter()
            .filter(|symbol| !answered.contains(symbol.as_str()))
            .cloned()
            .collect();
        let (bars, refused) = accepted.finish();
        Ok(Self {
            bars,
            missing,
            invalid,
            refused,
        })
    }

    /// In symbol, then timestamp, order.
    pub fn bars(&self) -> &[Bar] {
        &self.bars
    }

    /// Asked for but absent from every page: Alpaca drops a name it does not know without saying so.
    pub fn missing(&self) -> &[Symbol] {
        &self.missing
    }

    /// Named invalid by Alpaca, which fails the whole batch, so dropped and the rest fetched again.
    pub fn invalid(&self) -> &[Symbol] {
        &self.invalid
    }

    pub fn refused(&self) -> &[RefusedRow] {
        &self.refused
    }
}

/// Batches over disjoint symbols concatenate in the order they were asked, which keeps the bars in symbol order when
/// the batches were cut from a sorted list.
impl Monoid for MinuteBars {
    fn empty() -> Self {
        Self {
            bars: Vec::new(),
            missing: Vec::new(),
            invalid: Vec::new(),
            refused: Vec::new(),
        }
    }

    fn combine(mut self, other: Self) -> Self {
        self.bars.extend(other.bars);
        self.missing.extend(other.missing);
        self.invalid.extend(other.invalid);
        self.refused.extend(other.refused);
        self
    }
}

#[derive(Deserialize)]
struct BarsPage {
    /// `null` on a page with no bars.
    bars: Option<BTreeMap<String, Vec<AlpacaBar>>>,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct AlpacaBar {
    #[serde(rename = "t")]
    timestamp: DateTime<Utc>,
    #[serde(rename = "o")]
    open: f64,
    #[serde(rename = "h")]
    high: f64,
    #[serde(rename = "l")]
    low: f64,
    #[serde(rename = "c")]
    close: f64,
    #[serde(rename = "v")]
    volume: u64,
    #[serde(rename = "n")]
    trade_count: Option<u64>,
    #[serde(rename = "vw")]
    volume_weighted_average_price: Option<f64>,
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
            key_id: variable("ALPACA_API_KEY_ID")?,
            secret: variable("ALPACA_API_SECRET")?,
            trading_url: trading_url(variable("ALPACA_IS_PAPER")?)?,
        })
    }

    /// A client with no keys whose paper flag reads `is_paper`, for tests that never send a request.
    #[cfg(test)]
    pub(crate) fn unkeyed(is_paper: &str) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            key_id: String::new(),
            secret: String::new(),
            trading_url: trading_url(is_paper.to_string())
                .expect("a test passes a valid paper flag"),
        }
    }

    /// Whether the keys trade against the paper account.
    pub(crate) fn is_paper(&self) -> bool {
        self.trading_url == PAPER_TRADING_URL
    }

    /// A request to the trading API at `path`, carrying the keys.
    pub(crate) fn trading(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http_client
            .request(method, format!("{}{path}", self.trading_url))
            .header("APCA-API-KEY-ID", &self.key_id)
            .header("APCA-API-SECRET-KEY", &self.secret)
    }

    /// The published sessions over `[first, last]`. A row that does not read refuses the whole calendar, since
    /// dropping it would turn a trading day into a holiday that no heal ever owes.
    pub async fn calendar(
        &self,
        first: SessionDate,
        last: SessionDate,
    ) -> Result<TradingCalendar, FetchError> {
        let url = format!("{}/v2/calendar", self.trading_url);
        let (start, end) = (first.to_string(), last.to_string());
        let body = with_retries(|| {
            send(
                self.http_client
                    .get(&url)
                    .header("APCA-API-KEY-ID", &self.key_id)
                    .header("APCA-API-SECRET-KEY", &self.secret)
                    .query(&[("start", start.as_str()), ("end", end.as_str())]),
            )
        })
        .await?;
        parse_calendar(&body, first, last)
    }

    /// Raw one-minute SIP bars across the whole Eastern day of `session`, with symbols resolved as of that session so
    /// a renamed or reused ticker reads its own history.
    pub async fn minute_bars(
        &self,
        symbols: &[Symbol],
        session: SessionDate,
    ) -> Result<MinuteBars, FetchError> {
        let (pages, requested, invalid) =
            dropping_invalid(symbols, |requested| self.pages(requested, session)).await?;
        MinuteBars::from_pages(&pages, &requested, invalid, session)
    }

    async fn pages(
        &self,
        symbols: Vec<Symbol>,
        session: SessionDate,
    ) -> Result<Vec<Vec<u8>>, FetchError> {
        let joined = symbols
            .iter()
            .map(Symbol::as_str)
            .collect::<Vec<_>>()
            .join(",");
        let (start, end) = session.bounds();
        // The endpoint's end is inclusive, so the next session's midnight bar is excluded by ending a second early.
        let end = end - TimeDelta::seconds(1);
        let as_of = session.to_string();
        let (start, end) = (start.to_rfc3339(), end.to_rfc3339());
        let (joined, start, end, as_of) = (&joined, &start, &end, &as_of);
        paginate(|page_token| async move {
            with_retries(|| {
                let mut query = vec![
                    ("symbols", joined.as_str()),
                    ("timeframe", "1Min"),
                    ("start", start.as_str()),
                    ("end", end.as_str()),
                    ("feed", "sip"),
                    ("adjustment", "raw"),
                    ("asof", as_of.as_str()),
                    ("sort", "asc"),
                    ("limit", PAGE_LIMIT),
                ];
                if let Some(token) = page_token.as_deref() {
                    query.push(("page_token", token));
                }
                send(
                    self.http_client
                        .get(BARS_URL)
                        .header("APCA-API-KEY-ID", &self.key_id)
                        .header("APCA-API-SECRET-KEY", &self.secret)
                        .query(&query),
                )
            })
            .await
        })
        .await
    }
}

#[derive(Deserialize)]
struct QuotesPage {
    /// `null` on a page with no quotes.
    quotes: Option<BTreeMap<String, Vec<AlpacaQuote>>>,
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
    /// Present on a print later corrected or canceled, whatever its value.
    #[serde(rename = "u")]
    update: Option<String>,
}

/// What one Alpaca quote became.
#[derive(Debug, Clone, PartialEq)]
pub enum AlpacaQuoteOutcome {
    Quote(Quote),
    /// A side with no price, which is no top of book.
    OneSided,
    Refused(RefusedRow),
}

/// What one Alpaca trade became.
#[derive(Debug, Clone, PartialEq)]
pub enum AlpacaTradeOutcome {
    Print {
        print: Print,
        tape: Tape,
        letters: Vec<char>,
        corrected: bool,
    },
    Refused(RefusedRow),
}

/// Stands in for a condition element longer than one letter, which no condition spells, so the print reads as
/// unresolved rather than being dropped.
const UNSPELLABLE: char = '\u{FFFD}';

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
        pages: &Sender<Vec<AlpacaQuoteOutcome>>,
    ) -> Result<bool, FetchError> {
        self.tick_pages(QUOTES_URL, symbol, session, quote_page, pages)
            .await
    }

    /// `quotes`, for SIP trades.
    pub async fn trades(
        &self,
        symbol: &Symbol,
        session: SessionDate,
        pages: &Sender<Vec<AlpacaTradeOutcome>>,
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
        pages: &Sender<Vec<Outcome>>,
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
                    self.http_client
                        .get(url)
                        .header("APCA-API-KEY-ID", &self.key_id)
                        .header("APCA-API-SECRET-KEY", &self.secret)
                        .query(&query),
                )
            })
            .await?;
            let next = tokens.next(&body)?;
            let (outcomes, page_answered) = parse(symbol, &body)?;
            answered |= page_answered;
            // A closed channel means nothing folds these pages, so fetching more is wasted.
            match (pages.send(outcomes).await, next) {
                (Err(_), _) | (Ok(()), None) => return Ok(answered),
                (Ok(()), Some(next)) => token = Some(next),
            }
        }
    }
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
    if row.bid_price == 0.0 || row.ask_price == 0.0 {
        return AlpacaQuoteOutcome::OneSided;
    }
    let refused = |cause| {
        AlpacaQuoteOutcome::Refused(RefusedRow {
            ticker: symbol.as_str().to_string(),
            cause,
        })
    };
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
                Ok(quote) => AlpacaQuoteOutcome::Quote(quote),
                Err(cause) => refused(RowRefusal::Quote(cause)),
            }
        }
        Err(cause) => refused(cause),
    }
}

/// One page of `symbol`'s trades, refusing any row filed under a ticker that was not asked for.
fn trade_page(symbol: &Symbol, body: &[u8]) -> Result<(Vec<AlpacaTradeOutcome>, bool), FetchError> {
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
    let tape = match row.tape.as_str() {
        "A" | "B" => Tape::ConsolidatedTape,
        "C" => Tape::UnlistedTrading,
        _ => {
            return refused(RowRefusal::Tape {
                raw: row.tape.clone(),
            });
        }
    };
    let letters = row
        .conditions
        .iter()
        .map(|element| condition_letter(element).unwrap_or(UNSPELLABLE))
        .collect();
    let price = match Price::from_dollars(row.price) {
        Ok(price) => price,
        Err(cause) => return refused(RowRefusal::Price(cause)),
    };
    let size = match Shares::from_float(row.size) {
        Ok(size) => size,
        Err(cause) => return refused(RowRefusal::Shares(cause)),
    };
    let print = match size.is_zero() {
        true => Print::Unsized {
            symbol: symbol.clone(),
            timestamp: row.timestamp,
            price,
        },
        false => match Trade::new(symbol.clone(), row.timestamp, price, size) {
            Ok(trade) => Print::Trade(trade),
            Err(cause) => return refused(RowRefusal::Trade(cause)),
        },
    };
    AlpacaTradeOutcome::Print {
        print,
        tape,
        letters,
        corrected: row.update.is_some(),
    }
}

/// The trading API a paper flag names.
fn trading_url(is_paper: String) -> Result<&'static str, VariableRefusal> {
    match is_paper.to_ascii_lowercase().parse::<bool>() {
        Ok(true) => Ok(PAPER_TRADING_URL),
        Ok(false) => Ok(LIVE_TRADING_URL),
        Err(_) => Err(VariableRefusal::Malformed {
            name: "ALPACA_IS_PAPER",
            raw: is_paper,
        }),
    }
}

fn parse_calendar(
    body: &[u8],
    first: SessionDate,
    last: SessionDate,
) -> Result<TradingCalendar, FetchError> {
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
    TradingCalendar::new(sessions, first, last).map_err(|refusal| malformed(format!("{refusal:?}")))
}

/// Fetches `symbols`, and whenever Alpaca names one invalid (which fails the whole request) drops it and fetches the
/// rest again. Returns the pages, the symbols they answer for, and the symbols dropped.
async fn dropping_invalid<Fetch, Pending>(
    symbols: &[Symbol],
    mut fetch: Fetch,
) -> Result<(Vec<Vec<u8>>, Vec<Symbol>, Vec<Symbol>), FetchError>
where
    Fetch: FnMut(Vec<Symbol>) -> Pending,
    Pending: std::future::Future<Output = Result<Vec<Vec<u8>>, FetchError>>,
{
    let mut requested = symbols.to_vec();
    let mut invalid = Vec::new();
    while !requested.is_empty() {
        match fetch(requested.clone()).await {
            Ok(pages) => return Ok((pages, requested, invalid)),
            Err(FetchError::Refused { status: 400, body }) => {
                let named = invalid_symbol(&body)
                    .and_then(|name| requested.iter().position(|symbol| symbol.as_str() == name));
                match named {
                    Some(index) => invalid.push(requested.remove(index)),
                    None => return Err(FetchError::Refused { status: 400, body }),
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok((Vec::new(), requested, invalid))
}

/// Follows `next_page_token` until it is null.
async fn paginate<Fetch, Pending>(mut fetch_page: Fetch) -> Result<Vec<Vec<u8>>, FetchError>
where
    Fetch: FnMut(Option<String>) -> Pending,
    Pending: std::future::Future<Output = Result<Vec<u8>, FetchError>>,
{
    let mut tokens = PageTokens::default();
    let mut pages = Vec::new();
    let mut token = None;
    loop {
        let body = fetch_page(token).await?;
        let next = tokens.next(&body)?;
        pages.push(body);
        match next {
            None => return Ok(pages),
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
    let page: BarsPage = serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
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

fn minute_bar(ticker: &str, row: &AlpacaBar, session: SessionDate) -> Result<Bar, RowRefusal> {
    let symbol = Symbol::new(ticker).map_err(RowRefusal::Symbol)?;
    if SessionDate::at(row.timestamp) != session {
        return Err(RowRefusal::Session {
            timestamp: row.timestamp.to_rfc3339(),
        });
    }
    let price = |dollars: f64| Price::from_dollars(dollars).map_err(RowRefusal::Price);
    let prices = Ohlc::new(
        price(row.open)?,
        price(row.high)?,
        price(row.low)?,
        price(row.close)?,
    )
    .map_err(RowRefusal::Prices)?;
    let volume = Shares::whole(row.volume).map_err(RowRefusal::Shares)?;
    let dollar_volume = row
        .volume_weighted_average_price
        .map(|average| DollarVolume::from_average(average, volume))
        .transpose()
        .map_err(RowRefusal::DollarVolume)?;
    Bar::new(
        symbol,
        BarInterval::OneMinute,
        row.timestamp,
        prices,
        volume,
        row.trade_count.map(TradeCount::new),
        dollar_volume,
    )
    .map_err(RowRefusal::Bar)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;
    use crate::common::monoid::laws;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
    }

    fn read(pages: &[Vec<u8>], requested: &[Symbol]) -> Result<MinuteBars, FetchError> {
        MinuteBars::from_pages(pages, requested, Vec::new(), session())
    }

    fn symbols(names: &[&str]) -> Vec<Symbol> {
        names
            .iter()
            .map(|name| Symbol::new(name).unwrap())
            .collect()
    }

    /// The live response for AAPL and BRK.B over 13:30-13:32Z on 2026-09-25, probed with the development key.
    const FIXTURE: &[u8] = br#"{"bars":{
        "AAPL":[
            {"c":335.81,"h":336.8,"l":335.5,"n":9642,"o":336.04,"t":"2026-09-25T13:30:00Z","v":436147,"vw":335.929638},
            {"c":334.79,"h":335.83,"l":334.53,"n":3622,"o":335.75,"t":"2026-09-25T13:31:00Z","v":151972,"vw":335.145391},
            {"c":335.1005,"h":335.3702,"l":334.6606,"n":2040,"o":334.765,"t":"2026-09-25T13:32:00Z","v":78018,"vw":335.036579}
        ],
        "BRK.B":[
            {"c":504.61,"h":505.48,"l":504.5,"n":1636,"o":505.25,"t":"2026-09-25T13:30:00Z","v":57776,"vw":505.147324},
            {"c":505.362,"h":505.362,"l":504.55,"n":454,"o":504.57,"t":"2026-09-25T13:31:00Z","v":15475,"vw":505.097352},
            {"c":505.1888,"h":505.6,"l":504.6101,"n":238,"o":505.49,"t":"2026-09-25T13:32:00Z","v":5026,"vw":505.025129}
        ]
    },"next_page_token":null}"#;

    #[test]
    fn test_a_page_becomes_minute_bars_and_names_what_is_missing() {
        let batch = read(&[FIXTURE.to_vec()], &symbols(&["AAPL", "BRK.B", "ZTST"])).unwrap();
        assert_eq!(batch.bars.len(), 6);
        assert_eq!(batch.missing, symbols(&["ZTST"]));
        assert!(batch.refused.is_empty());
        let first = &batch.bars[0];
        assert_eq!(first.symbol().as_str(), "AAPL");
        assert_eq!(first.timestamp().to_rfc3339(), "2026-09-25T13:30:00+00:00");
        assert_eq!(first.volume(), Shares::whole(436_147).unwrap());
        assert_eq!(first.trade_count(), Some(TradeCount::new(9642)));
        let average = first.volume_weighted_average_price().unwrap();
        assert!((average - 335.929_638).abs() < 1e-9, "{average}");
        // A four-decimal SIP print sits on the grid.
        assert_eq!(batch.bars[2].prices().close().to_string(), "335.1005");
    }

    #[test]
    fn test_pages_are_read_in_order_and_a_zero_volume_bar_has_no_average() {
        let first = br#"{"bars":{"AAC.WS":[{"t":"2026-09-25T14:00:00Z","o":0.7201,"h":0.7201,"l":0.7201,"c":0.7201,"v":0,"n":0,"vw":0}]},"next_page_token":"abc"}"#;
        let second = br#"{"bars":null,"next_page_token":null}"#;
        assert_eq!(next_page_token(first), Ok(Some("abc".to_string())));
        assert_eq!(next_page_token(second), Ok(None));
        let batch = read(&[first.to_vec(), second.to_vec()], &symbols(&["AAC.WS"])).unwrap();
        assert_eq!(batch.bars.len(), 1);
        assert_eq!(batch.bars[0].volume_weighted_average_price(), None);
        assert!(batch.missing.is_empty());
    }

    #[test]
    fn test_a_bar_from_another_session_is_refused() {
        let page = br#"{"bars":{"AAPL":[{"t":"2026-09-26T04:00:00Z","o":1,"h":1,"l":1,"c":1,"v":1}]},"next_page_token":null}"#;
        let batch = read(&[page.to_vec()], &symbols(&["AAPL"])).unwrap();
        assert_eq!(
            batch.refused,
            [RefusedRow {
                ticker: "AAPL".to_string(),
                cause: RowRefusal::Session {
                    timestamp: "2026-09-26T04:00:00+00:00".to_string()
                }
            }]
        );
    }

    /// Every symbol asked for lands in exactly one of answered, missing or invalid.
    #[test]
    fn test_every_requested_symbol_is_accounted_for_once() {
        let batch = MinuteBars::from_pages(
            &[FIXTURE.to_vec()],
            &symbols(&["AAPL", "BRK.B", "ZTST"]),
            symbols(&["BC.PRC"]),
            session(),
        )
        .unwrap();
        let answered: BTreeSet<&str> = batch
            .bars()
            .iter()
            .map(|bar| bar.symbol().as_str())
            .chain(batch.refused().iter().map(RefusedRow::ticker))
            .collect();
        let mut accounted: Vec<&str> = answered
            .into_iter()
            .chain(batch.missing().iter().map(Symbol::as_str))
            .chain(batch.invalid().iter().map(Symbol::as_str))
            .collect();
        accounted.sort();
        assert_eq!(accounted, ["AAPL", "BC.PRC", "BRK.B", "ZTST"]);
    }

    #[test]
    fn test_a_symbol_answered_but_not_asked_for_is_refused() {
        // What Alpaca sent back for `BCpC`: the unrelated common stock.
        let page = br#"{"bars":{"BCPC":[{"t":"2026-09-25T14:00:00Z","o":167,"h":168,"l":166,"c":167,"v":10}]},"next_page_token":null}"#;
        let batch = read(&[page.to_vec()], &symbols(&["BC.PRC"])).unwrap();
        assert!(batch.bars().is_empty());
        assert_eq!(batch.missing(), symbols(&["BC.PRC"]));
        assert_eq!(
            batch.refused(),
            [RefusedRow {
                ticker: "BCPC".to_string(),
                cause: RowRefusal::Unrequested
            }]
        );
    }

    #[test]
    fn test_a_minute_repeated_across_pages_keeps_neither_row() {
        let row = |close: u32| {
            format!(
                r#"{{"bars":{{"AAPL":[{{"t":"2026-09-25T14:00:00Z","o":1,"h":2,"l":1,"c":{close},"v":10}}]}},"next_page_token":null}}"#
            )
            .into_bytes()
        };
        let batch = read(&[row(1), row(2), row(2)], &symbols(&["AAPL"])).unwrap();
        assert!(batch.bars().is_empty());
        assert_eq!(
            batch
                .refused()
                .iter()
                .map(|row| row.cause().clone())
                .collect::<Vec<_>>(),
            [
                RowRefusal::Duplicate,
                RowRefusal::Duplicate,
                RowRefusal::Duplicate
            ]
        );
        assert!(batch.missing().is_empty());
    }

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
        format!(r#"{{"bars":{{}},"next_page_token":{token}}}"#).into_bytes()
    }

    #[tokio::test]
    async fn test_pages_are_followed_until_the_token_is_null() {
        let requested = std::cell::RefCell::new(Vec::new());
        let pages = paginate(|token| {
            requested.borrow_mut().push(token.clone());
            async move {
                Ok(match token.as_deref() {
                    None => page(Some("a")),
                    Some("a") => page(Some("b")),
                    _ => page(None),
                })
            }
        })
        .await
        .unwrap();
        assert_eq!(pages.len(), 3);
        assert_eq!(
            *requested.borrow(),
            [None, Some("a".to_string()), Some("b".to_string())]
        );
    }

    #[tokio::test]
    async fn test_a_cycling_page_token_is_refused() {
        let calls = std::cell::Cell::new(0);
        let result = paginate(|token| {
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
        })
        .await;
        assert_eq!(
            result,
            Err(FetchError::Malformed {
                reason: "page token a repeated".to_string()
            })
        );
        assert_eq!(calls.get(), 3);
    }

    fn refusal(name: &str) -> FetchError {
        FetchError::Refused {
            status: 400,
            body: format!(r#"{{"message":"invalid symbol: {name}"}}"#),
        }
    }

    #[tokio::test]
    async fn test_an_invalid_symbol_is_dropped_and_the_rest_fetched_again() {
        let calls = std::cell::RefCell::new(Vec::new());
        let result = dropping_invalid(&symbols(&["AAPL", "BRK.B", "MSFT"]), |requested| {
            let names: Vec<String> = requested.iter().map(Symbol::to_string).collect();
            calls.borrow_mut().push(names.clone());
            async move {
                if names.contains(&"BRK.B".to_string()) {
                    Err(refusal("BRK.B"))
                } else {
                    Ok(vec![b"page".to_vec()])
                }
            }
        })
        .await;
        assert_eq!(
            result,
            Ok((
                vec![b"page".to_vec()],
                symbols(&["AAPL", "MSFT"]),
                symbols(&["BRK.B"])
            ))
        );
        assert_eq!(calls.borrow().len(), 2);
    }

    #[tokio::test]
    async fn test_a_refusal_naming_no_requested_symbol_is_returned() {
        let result =
            dropping_invalid(&symbols(&["AAPL"]), |_| async { Err(refusal("BC-C")) }).await;
        assert_eq!(result, Err(refusal("BC-C")));
    }

    #[tokio::test]
    async fn test_a_batch_that_loses_every_symbol_stops() {
        let result =
            dropping_invalid(&symbols(&["AAPL"]), |_| async { Err(refusal("AAPL")) }).await;
        assert_eq!(result, Ok((Vec::new(), Vec::new(), symbols(&["AAPL"]))));
    }

    fn any_batch() -> impl Strategy<Value = MinuteBars> {
        let pool = read(&[FIXTURE.to_vec()], &symbols(&["AAPL", "BRK.B"]))
            .unwrap()
            .bars;
        let names = symbols(&["ZTST", "BC.PRC", "ABC", "XYZ"]);
        let causes = prop::sample::select(vec![RowRefusal::Unrequested, RowRefusal::Duplicate]);
        (
            prop::sample::subsequence(pool.clone(), 0..=pool.len()),
            prop::sample::subsequence(names.clone(), 0..=names.len()),
            prop::sample::subsequence(names, 0..=2),
            prop::collection::vec(("[A-Z]{1,4}", causes), 0..3),
        )
            .prop_map(|(bars, missing, invalid, refused)| MinuteBars {
                bars,
                missing,
                invalid,
                refused: refused
                    .into_iter()
                    .map(|(ticker, cause)| RefusedRow { ticker, cause })
                    .collect(),
            })
    }

    proptest! {
        #[test]
        fn property_batches_concatenate_as_a_monoid(
            first in any_batch(),
            second in any_batch(),
            third in any_batch(),
        ) {
            laws::check_ordered(first, second, third)?;
        }
    }

    /// The live calendar over Thanksgiving 2026, probed with the development key.
    const CALENDAR: &[u8] = br#"[{"close":"16:00","date":"2026-11-25","open":"09:30","session_close":"2000","session_open":"0400","settlement_date":"2026-11-27"},{"close":"13:00","date":"2026-11-27","open":"09:30","session_close":"1700","session_open":"0400","settlement_date":"2026-11-30"},{"close":"16:00","date":"2026-11-30","open":"09:30","session_close":"2000","session_open":"0400","settlement_date":"2026-12-01"}]"#;

    fn day(text: &str) -> SessionDate {
        SessionDate::from_date(text.parse().unwrap())
    }

    #[test]
    fn test_the_paper_flag_is_read_in_any_case_and_nothing_else() {
        for raw in ["true", "TRUE", "True"] {
            assert_eq!(
                trading_url(raw.to_string()),
                Ok("https://paper-api.alpaca.markets")
            );
        }
        for raw in ["false", "FALSE", "False"] {
            assert_eq!(
                trading_url(raw.to_string()),
                Ok("https://api.alpaca.markets")
            );
        }
        for raw in ["", "yes", "1", " true"] {
            assert_eq!(
                trading_url(raw.to_string()),
                Err(VariableRefusal::Malformed {
                    name: "ALPACA_IS_PAPER",
                    raw: raw.to_string()
                })
            );
        }
    }

    #[test]
    fn test_the_calendar_reads_holidays_and_early_closes() {
        let calendar = parse_calendar(CALENDAR, day("2026-11-25"), day("2026-11-30")).unwrap();
        let trading: Vec<String> = calendar
            .trading_days_in_range(day("2026-11-25"), day("2026-11-30"))
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
            parse_calendar(unreadable.as_bytes(), day("2026-11-25"), day("2026-11-30")),
            Err(FetchError::Malformed { .. })
        ));
        // A range narrower than the answer is a row outside the calendar, never a row silently dropped.
        assert!(matches!(
            parse_calendar(CALENDAR, day("2026-11-25"), day("2026-11-27")),
            Err(FetchError::Malformed { .. })
        ));
    }

    #[tokio::test]
    #[ignore = "reads the live Alpaca API; run under secretspec with --ignored"]
    async fn live_minute_bars_cover_the_session() {
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let batch = alpaca
            .minute_bars(&symbols(&["AAPL", "BRK.B", "BC.PRC", "ZTST"]), session())
            .await
            .unwrap();
        let per_symbol = batch.bars.iter().fold(BTreeMap::new(), |mut counts, bar| {
            *counts.entry(bar.symbol().to_string()).or_insert(0) += 1;
            counts
        });
        println!(
            "{per_symbol:?} missing {:?} refused {:?}",
            batch.missing, batch.refused
        );
        assert!(per_symbol["AAPL"] > 390, "{per_symbol:?}");
        assert_eq!(batch.missing, symbols(&["ZTST"]));
        assert!(batch.refused.is_empty(), "{:?}", batch.refused);
    }

    /// Rows from Alpaca's SIP quotes and trades for AAPL and PFE on 2026-10-02, probed with the production key and
    /// filed under AAPL; the last trade's tape `E` is invented to exercise the refusal.
    const QUOTES_PAGE: &str = r#"{"next_page_token": null, "quotes": {"AAPL": [{"ap": 333.67, "as": 200, "ax": "Q", "bp": 333.65, "bs": 1520, "bx": "Q", "c": ["R"], "t": "2026-10-02T19:59:58.001719242Z", "z": "C"}, {"ap": 0, "as": 0, "ax": "Q", "bp": 333.65, "bs": 100, "bx": "Q", "c": ["R"], "t": "2026-10-02T19:59:58.0018Z", "z": "C"}]}}"#;
    const TRADES_PAGE: &str = r#"{"next_page_token": null, "trades": {"AAPL": [{"c": ["@", "6", "X"], "i": 1, "p": 333.69, "s": 6224093, "t": "2026-10-02T20:00:00.118Z", "x": "Q", "z": "C"}, {"c": ["@", "T", "P"], "i": 335045, "p": 333.69, "s": 819560, "t": "2026-10-02T21:49:13.402977462Z", "u": "canceled", "x": "D", "z": "C"}, {"c": [" ", "9"], "i": 7, "p": 27.81, "s": 0, "t": "2026-10-02T20:10:00.001Z", "x": "N", "z": "A"}, {"c": ["@"], "i": 8, "p": 1.0, "s": 1, "t": "2026-10-02T20:10:00.001Z", "x": "N", "z": "E"}]}}"#;

    #[test]
    fn test_alpaca_quotes_keep_the_top_of_book_in_shares() {
        let symbol = Symbol::new("AAPL").unwrap();
        let (outcomes, answered) = quote_page(&symbol, QUOTES_PAGE.as_bytes()).unwrap();
        assert!(answered);
        assert_eq!(outcomes.len(), 2);
        match &outcomes[0] {
            AlpacaQuoteOutcome::Quote(quote) => {
                assert_eq!(quote.bid().ticks(), 333_650_000);
                assert_eq!(quote.bid_size().units(), 1_520_000_000);
                assert_eq!(
                    quote.timestamp().to_rfc3339(),
                    "2026-10-02T19:59:58.001719242+00:00"
                );
            }
            other @ (AlpacaQuoteOutcome::OneSided | AlpacaQuoteOutcome::Refused(_)) => {
                panic!("{other:?}")
            }
        }
        assert_eq!(outcomes[1], AlpacaQuoteOutcome::OneSided);
    }

    #[test]
    fn test_alpaca_trades_carry_their_tape_letters_and_corrections() {
        let symbol = Symbol::new("AAPL").unwrap();
        let (outcomes, answered) = trade_page(&symbol, TRADES_PAGE.as_bytes()).unwrap();
        assert!(answered);
        let summary: Vec<String> = outcomes
            .iter()
            .map(|outcome| match outcome {
                AlpacaTradeOutcome::Print {
                    print,
                    tape,
                    letters,
                    corrected,
                } => format!(
                    "{tape:?} {letters:?} corrected={corrected} unsized={}",
                    matches!(print, Print::Unsized { .. })
                ),
                AlpacaTradeOutcome::Refused(row) => {
                    format!("refused {}", <&'static str>::from(row.cause()))
                }
            })
            .collect();
        assert_eq!(
            summary,
            [
                "UnlistedTrading ['@', '6', 'X'] corrected=false unsized=false",
                "UnlistedTrading ['@', 'T', 'P'] corrected=true unsized=false",
                "ConsolidatedTape [' ', '9'] corrected=false unsized=true",
                "refused tape",
            ]
        );
    }

    #[test]
    fn test_alpaca_ticks_filed_under_another_ticker_are_refused_not_dropped() {
        let symbol = Symbol::new("PFE").unwrap();
        let unrequested = AlpacaQuoteOutcome::Refused(RefusedRow {
            ticker: "AAPL".to_string(),
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
