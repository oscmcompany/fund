//! Massive's grouped daily bars: every ticker that traded on a session, which is also the session's symbol list.

use chrono::DateTime;
use serde::Deserialize;

use super::retry::{FetchError, send, with_retries};
use super::{Accepted, MissingVariable, RefusedRow, RowRefusal, variable};
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
    http_client: reqwest::Client,
    base_url: String,
    api_key: String,
}

/// One session's daily bars, with every ticker that did not become one: each row of the response is exactly one of a
/// bar, a test ticker or a refusal.
#[derive(Debug, Clone, PartialEq)]
pub struct DailyBars {
    bars: Vec<Bar>,
    test_tickers: Vec<String>,
    refused: Vec<RefusedRow>,
}

impl DailyBars {
    /// One bar per symbol, in symbol order.
    pub fn bars(&self) -> &[Bar] {
        &self.bars
    }

    pub fn test_tickers(&self) -> &[String] {
        &self.test_tickers
    }

    pub fn refused(&self) -> &[RefusedRow] {
        &self.refused
    }
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
    pub fn from_environment(http_client: reqwest::Client) -> Result<Self, MissingVariable> {
        Ok(Self {
            http_client,
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
            send(
                self.http_client
                    .get(&url)
                    .bearer_auth(&self.api_key)
                    .query(&[("adjusted", "false"), ("include_otc", "false")]),
            )
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
    let mut test_tickers = Vec::new();
    // Keyed by symbol, so two tickers the notation map sends to one symbol (`ABCw` and `ABC.WS`) keep neither.
    let mut accepted = Accepted::new();
    for row in response.results {
        if EXCHANGE_TEST_TICKERS.contains(&row.ticker.as_str()) {
            test_tickers.push(row.ticker);
            continue;
        }
        match daily_bar(&row, session) {
            Ok(bar) => accepted.offer(bar.symbol().clone(), row.ticker, bar),
            Err(cause) => accepted.refuse(row.ticker, cause),
        }
    }
    let (bars, refused) = accepted.finish();
    Ok(DailyBars {
        bars,
        test_tickers,
        refused,
    })
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
    const FIXTURE: &str = r#"{"results":[
        {"T":"INEO","v":51938,"vw":0.6123,"o":0.59,"c":0.6356,"h":0.6536,"l":0.59,"t":1790366400000,"n":203},
        {"T":"PEBpF","v":16902.906,"vw":19.6913,"o":19.79,"c":19.66,"h":19.79,"l":19.48,"t":1790366400000,"n":175},
        {"T":"GTOQ","v":2577.8477,"vw":21.7639,"o":21.8,"c":21.775,"h":21.8,"l":21.7301,"t":1790366400000,"n":51},
        {"T":"CRVL","v":213849.305802,"vw":74.6122,"o":73.73,"c":74.33,"h":75,"l":73.73,"t":1790366400000,"n":7494},
        {"T":"PHXEp","v":16951.6951,"vw":28.1291,"o":28.5,"c":28.0713,"h":28.5,"l":27.8101,"t":1790366400000,"n":114},
        {"T":"BRK.B","v":2994434.093355,"vw":504.8007,"o":505.25,"c":505.48,"h":505.87,"l":502.24,"t":1790366400000,"n":88853},
        {"T":"BCpC","v":3328,"vw":23.5217,"o":23.45,"c":23.97,"h":23.97,"l":23.4001,"t":1790366400000,"n":43},
        {"T":"AAPL","v":30002507.354057,"vw":339.4037,"o":336.04,"c":341.07,"h":341.67,"l":334.53,"t":1790366400000,"n":606476},
        {"T":"NE.WS.A","v":389,"vw":21.3059,"o":21.36,"c":21.36,"h":21.36,"l":21.36,"t":1790366400000,"n":6},
        {"T":"SBXD.U","v":2117,"vw":11.3604,"o":11.13,"c":11.52,"h":11.52,"l":11.13,"t":1790366400000,"n":16},
        {"T":"BCATrw","v":1869.1711,"vw":0.01837,"o":0.02,"c":0.0158,"h":0.02,"l":0.0157,"t":1790366400000,"n":143},
        {"T":"AIIAr","v":744,"vw":0.155,"o":0.155,"c":0.155,"h":0.155,"l":0.155,"t":1790366400000,"n":6},
        {"T":"ZBZX","v":0,"o":25,"c":25,"h":25,"l":25,"t":1790366400000},
        {"T":"ZTST","v":0,"o":12345,"c":12345,"h":12345,"l":12345,"t":1790366400000}
    ],"queryCount":14,"resultsCount":14,"adjusted":false,"status":"OK","request_id":"34b9c2486d430c6996d16b5cc67afae0","count":14}"#;

    fn fixture() -> DailyBars {
        parse_grouped_daily(FIXTURE.as_bytes(), session()).unwrap()
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
                "AAPL", "AIIA.RT", "BC.PRC", "BRK.B", "CRVL", "GTOQ", "INEO", "PEB.PRF", "SBXD.U"
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

    /// Each of the fixture's 14 rows is exactly one of a bar, a test ticker or a refusal.
    #[test]
    fn test_every_grouped_row_is_accounted_for_once() {
        let daily = fixture();
        assert_eq!(
            (
                daily.bars().len(),
                daily.test_tickers().len(),
                daily.refused().len()
            ),
            (9, 2, 3)
        );
    }

    #[test]
    fn test_two_tickers_naming_one_symbol_keep_neither() {
        let body = br#"{"results":[
            {"T":"ABCw","o":1,"h":1,"l":1,"c":1,"v":1,"t":1790366400000},
            {"T":"ABC.WS","o":2,"h":2,"l":2,"c":2,"v":1,"t":1790366400000}
        ]}"#;
        let daily = parse_grouped_daily(body, session()).unwrap();
        assert!(daily.bars().is_empty());
        assert_eq!(
            daily.refused(),
            [
                RefusedRow {
                    ticker: "ABCw".to_string(),
                    cause: RowRefusal::Duplicate
                },
                RefusedRow {
                    ticker: "ABC.WS".to_string(),
                    cause: RowRefusal::Duplicate
                }
            ]
        );
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

    proptest::proptest! {
        /// The rules never send two Massive forms to one symbol; the one collision they cannot see, a warrant written
        /// both `ABCw` and `ABC.WS`, is refused at runtime as a duplicate.
        #[test]
        fn property_the_notation_map_is_injective_across_forms(first in "[A-Z]{1,4}", second in "[A-Z]{1,4}") {
            let tickers: std::collections::BTreeSet<String> = [&first, &second]
                .iter()
                .flat_map(|root| [root.to_string(), format!("{root}pA"), format!("{root}w"), format!("{root}r")])
                .collect();
            let symbols: std::collections::BTreeSet<Symbol> =
                tickers.iter().map(|ticker| alpaca_symbol(ticker).unwrap()).collect();
            proptest::prop_assert_eq!(symbols.len(), tickers.len());
        }
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
