//! Alpaca's historical SIP bars, fetched for many symbols at once and named as they were on the session.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;

use super::retry::{FetchError, send, with_retries};
use super::{Accepted, MissingVariable, RefusedRow, RowRefusal, variable};
use crate::common::market::record::{Bar, BarInterval, Ohlc};
use crate::common::market::{DollarVolume, Price, Shares, Symbol, TradeCount};
use crate::common::time::SessionDate;

const BARS_URL: &str = "https://data.alpaca.markets/v2/stocks/bars";

/// Bars per page, the endpoint's maximum.
const PAGE_LIMIT: &str = "10000";

pub struct Alpaca {
    http_client: reqwest::Client,
    key_id: String,
    secret: String,
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
struct ErrorBody {
    message: String,
}

impl Alpaca {
    /// Reads `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET`.
    pub fn from_environment(http_client: reqwest::Client) -> Result<Self, MissingVariable> {
        Ok(Self {
            http_client,
            key_id: variable("ALPACA_API_KEY_ID")?,
            secret: variable("ALPACA_API_SECRET")?,
        })
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

/// Follows `next_page_token` until it is null. A token seen before means the pages cycle, which would otherwise
/// request forever while holding every page, so it is refused.
async fn paginate<Fetch, Pending>(mut fetch_page: Fetch) -> Result<Vec<Vec<u8>>, FetchError>
where
    Fetch: FnMut(Option<String>) -> Pending,
    Pending: std::future::Future<Output = Result<Vec<u8>, FetchError>>,
{
    let mut pages = Vec::new();
    let mut seen = BTreeSet::new();
    let mut token = None;
    loop {
        let body = fetch_page(token).await?;
        let next = next_page_token(&body)?;
        pages.push(body);
        match next {
            None => return Ok(pages),
            Some(next) if !seen.insert(next.clone()) => {
                return Err(FetchError::Malformed {
                    reason: format!("page token {next} repeated"),
                });
            }
            Some(next) => token = Some(next),
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
fn invalid_symbol(body: &str) -> Option<String> {
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

    use super::*;

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
}
