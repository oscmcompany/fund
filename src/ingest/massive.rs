//! Massive's grouped daily bars: every ticker that traded on a session, which is also the session's symbol list.

use chrono::DateTime;
use serde::Deserialize;

use super::retry::{FetchError, send, with_retries};
use super::{MissingVariable, RefusedRow, RowRefusal, variable};
use crate::common::market::record::{Bar, BarInterval, Ohlc};
use crate::common::market::{DollarVolume, Price, Shares, Symbol, SymbolRefusal, TradeCount};
use crate::common::time::SessionDate;

/// Exchange test tickers, which print in the grouped daily but are not securities. An exact list rather than a pattern,
/// because ZTS, ZBRA and CBOE are real.
const EXCHANGE_TEST_TICKERS: [&str; 34] = [
    "ATEST", "CBOA", "CBOJ", "CBOL", "CBOO", "CBOT", "CBOX", "CBOY", "CBXA", "CBXJ", "CBXL",
    "CBXO", "CBXY", "IBOT", "MTEST", "MTEST.A", "NTEST", "NTEST.H", "NTEST.I", "ZBZX", "ZEXIT",
    "ZIEXT", "ZJZZT", "ZTEST", "ZTST", "ZVZZT", "ZWZZT", "ZXIET", "ZXZZT", "ZZZTA", "ZZZTE",
    "ZZZTS", "ZZZTT", "ZZZTX",
];

pub struct Massive {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
}

/// One session's daily bars, with every ticker that did not become one.
#[derive(Debug, Clone, PartialEq)]
pub struct DailyBars {
    pub bars: Vec<Bar>,
    pub test_tickers: Vec<String>,
    pub refused: Vec<RefusedRow>,
}

#[derive(Deserialize)]
struct GroupedResponse {
    /// Absent on a session with no trading.
    #[serde(default)]
    results: Vec<GroupedRow>,
}

#[derive(Deserialize)]
struct GroupedRow {
    #[serde(rename = "T")]
    ticker: String,
    #[serde(rename = "o")]
    open: f64,
    #[serde(rename = "h")]
    high: f64,
    #[serde(rename = "l")]
    low: f64,
    #[serde(rename = "c")]
    close: f64,
    #[serde(rename = "v")]
    volume: f64,
    #[serde(rename = "vw")]
    volume_weighted_average_price: Option<f64>,
    #[serde(rename = "n")]
    trade_count: Option<u64>,
    /// Milliseconds since the epoch at the session's Eastern midnight.
    #[serde(rename = "t")]
    timestamp: i64,
}

impl Massive {
    /// Reads `MASSIVE_BASE_URL` and `MASSIVE_API_KEY`.
    pub fn from_environment(http: reqwest::Client) -> Result<Self, MissingVariable> {
        Ok(Self {
            http,
            base_url: variable("MASSIVE_BASE_URL")?,
            api_key: variable("MASSIVE_API_KEY")?,
        })
    }

    /// Unadjusted daily bars for every exchange-listed ticker that traded on `session`.
    pub async fn grouped_daily(&self, session: SessionDate) -> Result<DailyBars, FetchError> {
        let url = format!(
            "{}/v2/aggs/grouped/locale/us/market/stocks/{session}",
            self.base_url
        );
        let body = with_retries(|| {
            send(self.http.get(&url).query(&[
                ("adjusted", "false"),
                ("include_otc", "false"),
                ("apiKey", self.api_key.as_str()),
            ]))
        })
        .await?;
        parse_grouped_daily(&body, session)
    }
}

fn parse_grouped_daily(body: &[u8], session: SessionDate) -> Result<DailyBars, FetchError> {
    let response: GroupedResponse =
        serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
            reason: error.to_string(),
        })?;
    let mut daily = DailyBars {
        bars: Vec::new(),
        test_tickers: Vec::new(),
        refused: Vec::new(),
    };
    for row in response.results {
        if EXCHANGE_TEST_TICKERS.contains(&row.ticker.as_str()) {
            daily.test_tickers.push(row.ticker);
            continue;
        }
        match daily_bar(&row, session) {
            Ok(bar) => daily.bars.push(bar),
            Err(cause) => daily.refused.push(RefusedRow {
                ticker: row.ticker,
                cause,
            }),
        }
    }
    Ok(daily)
}

fn daily_bar(row: &GroupedRow, session: SessionDate) -> Result<Bar, RowRefusal> {
    let symbol = alpaca_symbol(&row.ticker).map_err(RowRefusal::Symbol)?;
    let stamped = DateTime::from_timestamp_millis(row.timestamp)
        .filter(|instant| SessionDate::at(*instant) == session);
    if stamped.is_none() {
        return Err(RowRefusal::Session {
            timestamp: row.timestamp.to_string(),
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
    let volume = Shares::from_float(row.volume).map_err(RowRefusal::Shares)?;
    let dollar_volume = row
        .volume_weighted_average_price
        .map(|average| DollarVolume::from_average(average, volume))
        .transpose()
        .map_err(RowRefusal::DollarVolume)?;
    Bar::new(
        symbol,
        BarInterval::OneDay,
        session.regular_close(),
        prices,
        volume,
        row.trade_count.map(TradeCount::new),
        dollar_volume,
    )
    .map_err(RowRefusal::Bar)
}

/// Massive's ticker in Alpaca's notation, which `Symbol` holds: preferred `BCpC` is `BC.PRC`, warrant `ABCw` is
/// `ABC.WS` and right `ABCr` is `ABC.RT`. Anything else goes to `Symbol::new` as written, so a lowercase form no rule
/// covers is refused rather than uppercased into another security (`BCpC` is not the common `BCPC`).
pub fn alpaca_symbol(ticker: &str) -> Result<Symbol, SymbolRefusal> {
    let suffixed = |root: &str, suffix: &str| format!("{root}.{suffix}");
    let translated = match ticker.char_indices().rev().nth(1) {
        Some((index, 'p'))
            if ticker[index + 1..]
                .bytes()
                .all(|byte| byte.is_ascii_uppercase()) =>
        {
            suffixed(&ticker[..index], &format!("PR{}", &ticker[index + 1..]))
        }
        _ => match ticker.strip_suffix('w') {
            Some(root) => suffixed(root, "WS"),
            None => match ticker.strip_suffix('r') {
                Some(root) => suffixed(root, "RT"),
                None => ticker.to_string(),
            },
        },
    };
    Symbol::new(&translated).map_err(|_| SymbolRefusal::Malformed {
        raw: ticker.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
    }

    /// Rows trimmed from the live grouped daily for 2026-09-25, probed with the development key.
    fn fixture() -> DailyBars {
        parse_grouped_daily(
            include_bytes!("fixtures/massive_grouped_daily_2026_09_25.json"),
            session(),
        )
        .unwrap()
    }

    #[test]
    fn test_massive_notation_maps_to_alpaca() {
        let cases = [
            ("BCpC", "BC.PRC"),
            ("PEBpF", "PEB.PRF"),
            ("AIIAr", "AIIA.RT"),
            ("ABCw", "ABC.WS"),
            ("BRK.B", "BRK.B"),
            ("SBXD.U", "SBXD.U"),
            ("AAPL", "AAPL"),
        ];
        for (massive, alpaca) in cases {
            assert_eq!(
                alpaca_symbol(massive).map(|symbol| symbol.to_string()),
                Ok(alpaca.to_string()),
                "{massive}"
            );
        }
        for ticker in ["PHXEp", "BCATrw", "NE.WS.A", "BCPc"] {
            assert_eq!(
                alpaca_symbol(ticker),
                Err(SymbolRefusal::Malformed {
                    raw: ticker.to_string()
                }),
                "{ticker}"
            );
        }
    }

    #[test]
    fn test_every_grouped_row_is_a_bar_a_test_ticker_or_a_refusal() {
        let daily = fixture();
        let bars: Vec<String> = daily
            .bars
            .iter()
            .map(|bar| bar.symbol().to_string())
            .collect();
        assert_eq!(
            bars,
            [
                "INEO", "PEB.PRF", "GTOQ", "CRVL", "BRK.B", "BC.PRC", "AAPL", "SBXD.U", "AIIA.RT"
            ]
        );
        assert_eq!(daily.test_tickers, ["ZBZX", "ZTST"]);
        let refused: Vec<(&str, &RowRefusal)> = daily
            .refused
            .iter()
            .map(|row| (row.ticker.as_str(), &row.cause))
            .collect();
        assert_eq!(refused.len(), 3);
        for (ticker, cause) in refused {
            assert_eq!(
                cause,
                &RowRefusal::Symbol(SymbolRefusal::Malformed {
                    raw: ticker.to_string()
                })
            );
        }
    }

    #[test]
    fn test_a_grouped_row_becomes_a_daily_bar_at_the_close() {
        let daily = fixture();
        let crvl = daily
            .bars
            .iter()
            .find(|bar| bar.symbol().as_str() == "CRVL")
            .unwrap();
        assert_eq!(crvl.timestamp().to_rfc3339(), "2026-09-25T20:00:00+00:00");
        assert_eq!(crvl.volume().to_string(), "213849.305802");
        assert_eq!(crvl.trade_count(), Some(TradeCount::new(7494)));
        let average = crvl.volume_weighted_average_price().unwrap();
        assert!((average - 74.6122).abs() < 1e-9, "{average}");
        assert_eq!(crvl.prices().open().to_string(), "73.73");
    }

    #[test]
    fn test_a_row_stamped_for_another_session_is_refused() {
        let body = br#"{"results":[{"T":"AAPL","o":1,"h":1,"l":1,"c":1,"v":1,"t":1790280000000}]}"#;
        let daily = parse_grouped_daily(body, session()).unwrap();
        assert_eq!(
            daily.refused,
            [RefusedRow {
                ticker: "AAPL".to_string(),
                cause: RowRefusal::Session {
                    timestamp: "1790280000000".to_string()
                }
            }]
        );
    }

    #[test]
    fn test_a_session_without_results_is_empty_and_a_bad_body_is_malformed() {
        let empty = parse_grouped_daily(br#"{"status":"OK","resultsCount":0}"#, session()).unwrap();
        assert!(empty.bars.is_empty() && empty.refused.is_empty());
        assert!(matches!(
            parse_grouped_daily(b"<html>", session()),
            Err(FetchError::Malformed { .. })
        ));
    }

    #[test]
    fn test_the_test_ticker_list_is_sorted_and_unique() {
        assert!(
            EXCHANGE_TEST_TICKERS
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
    }

    #[tokio::test]
    #[ignore = "reads the live Massive API; run under secretspec with --ignored"]
    async fn live_grouped_daily_refuses_only_unmapped_notation() {
        let massive = Massive::from_environment(reqwest::Client::new()).unwrap();
        let daily = massive.grouped_daily(session()).await.unwrap();
        let total = daily.bars.len() + daily.test_tickers.len() + daily.refused.len();
        println!(
            "rows {total}, bars {}, test tickers {}, refused {}",
            daily.bars.len(),
            daily.test_tickers.len(),
            daily.refused.len()
        );
        assert!(daily.bars.len() > 12_000, "{}", daily.bars.len());
        // Only notation no rule covers may be refused; any other cause is a mapping bug.
        for row in &daily.refused {
            assert!(
                matches!(row.cause, RowRefusal::Symbol(_)),
                "{} {:?}",
                row.ticker,
                row.cause
            );
        }
    }
}
