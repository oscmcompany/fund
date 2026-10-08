//! Massive's grouped daily bars, which are also a session's symbol list, and its reference data: trade conditions,
//! splits, and each symbol's details as of a date.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate};
use serde::Deserialize;

use super::retry::{FetchError, send, with_retries};
use super::{Accepted, RefusedRow, RowRefusal, VariableRefusal, variable};
use crate::common::market::corporate_actions::{ActionId, Split, SplitRatio};
use crate::common::market::record::{Bar, BarInterval, Ohlc};
use crate::common::market::security_details::{
    CentralIndexKey, IndustryCode, MarketIdentifierCode, SecurityDetails, SecurityType,
};
use crate::common::market::trade_bars::{
    Condition, TradeConditions, UpdateRules, condition_letter,
};
use crate::common::market::{
    DollarVolume, Dollars, DollarsRefusal, Price, Shares, Symbol, SymbolRefusal, TradeCount,
};
use crate::common::time::SessionDate;

/// Exchange test tickers, which print in the grouped daily but are not securities. An exact list rather than a pattern,
/// because ZTS, ZBRA and CBOE are real.
const EXCHANGE_TEST_TICKERS: [&str; 34] = [
    "ATEST", "CBOA", "CBOJ", "CBOL", "CBOO", "CBOT", "CBOX", "CBOY", "CBXA", "CBXJ", "CBXL",
    "CBXO", "CBXY", "IBOT", "MTEST", "MTEST.A", "NTEST", "NTEST.H", "NTEST.I", "ZBZX", "ZEXIT",
    "ZIEXT", "ZJZZT", "ZTEST", "ZTST", "ZVZZT", "ZWZZT", "ZXIET", "ZXZZT", "ZZZTA", "ZZZTE",
    "ZZZTS", "ZZZTT", "ZZZTX",
];

/// Whether `ticker` is an exchange test ticker; the legacy reader shares it until archive task A6 deletes that reader.
pub(crate) fn is_exchange_test_ticker(ticker: &str) -> bool {
    EXCHANGE_TEST_TICKERS.contains(&ticker)
}

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
    pub fn from_environment(http_client: reqwest::Client) -> Result<Self, VariableRefusal> {
        Ok(Self {
            http_client,
            base_url: variable("MASSIVE_BASE_URL")?,
            api_key: variable("MASSIVE_API_KEY")?,
        })
    }

    /// The sale conditions a stock print can carry, with the consolidated tape's update rules for each.
    pub async fn trade_conditions(&self) -> Result<TradeConditions, FetchError> {
        let url = format!("{}/v3/reference/conditions", self.base_url);
        let body = with_retries(|| {
            send(
                self.http_client
                    .get(&url)
                    .bearer_auth(&self.api_key)
                    .query(&[("asset_class", "stocks"), ("limit", "1000")]),
            )
        })
        .await?;
        parse_trade_conditions(&body)
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

    /// Every split Massive has published, past and announced, as one table.
    pub async fn splits(&self) -> Result<Splits, FetchError> {
        let url = format!("{}/v3/reference/splits", self.base_url);
        let mut splits = Splits::default();
        let mut cursor: Option<String> = None;
        let mut seen = BTreeSet::new();
        for _ in 0..SPLITS_PAGES_AT_MOST {
            let body = with_retries(|| {
                let mut query = vec![("limit", SPLITS_PAGE_LIMIT)];
                if let Some(cursor) = cursor.as_deref() {
                    query.push(("cursor", cursor));
                }
                send(
                    self.http_client
                        .get(&url)
                        .bearer_auth(&self.api_key)
                        .query(&query),
                )
            })
            .await?;
            match parse_splits_page(&body, &mut splits)? {
                None => return Ok(splits.unique()),
                Some(next) if !seen.insert(next.clone()) => {
                    return Err(FetchError::Malformed {
                        reason: format!("splits cursor {next} repeated"),
                    });
                }
                Some(next) => cursor = Some(next),
            }
        }
        Err(FetchError::Malformed {
            reason: format!("splits did not end within {SPLITS_PAGES_AT_MOST} pages"),
        })
    }

    /// `symbol`'s details as Massive held them on `as_of`; a symbol not listed that day is `Missing`, not an error.
    pub async fn security_details(
        &self,
        symbol: &Symbol,
        as_of: SessionDate,
    ) -> Result<DetailsAnswer, FetchError> {
        let ticker = massive_ticker(symbol);
        let url = format!("{}/v3/reference/tickers/{ticker}", self.base_url);
        let date = as_of.to_string();
        let answer = with_retries(|| {
            send(
                self.http_client
                    .get(&url)
                    .bearer_auth(&self.api_key)
                    .query(&[("date", date.as_str())]),
            )
        })
        .await;
        match answer {
            Ok(body) => parse_security_details(&body, symbol, &ticker),
            Err(FetchError::Refused { status: 404, .. }) => Ok(DetailsAnswer::Missing),
            Err(error) => Err(error),
        }
    }
}

const SPLITS_PAGE_LIMIT: &str = "1000";
/// Far past the table's 29 pages, so a cursor that never ends is caught.
const SPLITS_PAGES_AT_MOST: usize = 200;

/// Massive's split table, with every row that did not become a split.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Splits {
    splits: Vec<Split>,
    refused: Vec<RefusedRow>,
    /// How often each identifier was listed, refused rows included.
    listed: BTreeMap<String, usize>,
}

impl Splits {
    /// The table with every action listed more than once refused, each copy, since nothing says which is true.
    fn unique(self) -> Self {
        let listed = self.listed;
        let (splits, repeated): (Vec<Split>, Vec<Split>) = self
            .splits
            .into_iter()
            .partition(|split| listed.get(split.id().as_str()) == Some(&1));
        let mut refused = self.refused;
        refused.extend(repeated.into_iter().map(|split| RefusedRow {
            ticker: split.symbol().as_str().to_string(),
            cause: RowRefusal::Duplicate,
        }));
        Self {
            splits,
            refused,
            listed,
        }
    }

    pub fn splits(&self) -> &[Split] {
        &self.splits
    }

    pub fn refused(&self) -> &[RefusedRow] {
        &self.refused
    }
}

#[derive(Deserialize)]
struct SplitsPage {
    #[serde(default)]
    results: Vec<SplitRow>,
    next_url: Option<String>,
}

#[derive(Deserialize)]
struct SplitRow {
    id: String,
    ticker: String,
    execution_date: NaiveDate,
    split_from: f64,
    split_to: f64,
}

/// Adds one page's splits to `splits` and answers the cursor for the next page. Only the cursor is taken from
/// `next_url`, so the key is never sent to a host the response named.
fn parse_splits_page(body: &[u8], splits: &mut Splits) -> Result<Option<String>, FetchError> {
    let page: SplitsPage = serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
        reason: error.to_string(),
    })?;
    for row in page.results {
        *splits.listed.entry(row.id.clone()).or_insert(0) += 1;
        let split = ActionId::new(&row.id)
            .map_err(RowRefusal::ActionId)
            .and_then(|id| {
                let symbol = alpaca_symbol(&row.ticker).map_err(RowRefusal::Symbol)?;
                let ratio = SplitRatio::from_floats(row.split_from, row.split_to)
                    .map_err(RowRefusal::SplitRatio)?;
                Ok(Split::new(
                    id,
                    symbol,
                    SessionDate::from_date(row.execution_date),
                    ratio,
                ))
            });
        match split {
            Ok(split) => splits.splits.push(split),
            Err(cause) => splits.refused.push(RefusedRow {
                ticker: row.ticker,
                cause,
            }),
        }
    }
    page.next_url
        .map(|next| {
            reqwest::Url::parse(&next)
                .ok()
                .and_then(|url| {
                    url.query_pairs()
                        .find(|(name, _)| name == "cursor")
                        .map(|(_, cursor)| cursor.into_owned())
                })
                .ok_or(FetchError::Malformed {
                    reason: format!("next_url without a cursor: {next}"),
                })
        })
        .transpose()
}

/// What Massive answered about one symbol's details.
#[derive(Debug, Clone, PartialEq)]
pub enum DetailsAnswer {
    Details(SecurityDetails),
    /// Not listed on the date asked.
    Missing,
    Refused(RefusedRow),
}

#[derive(Deserialize)]
struct DetailsResponse {
    results: Option<DetailsRow>,
}

#[derive(Deserialize)]
struct DetailsRow {
    ticker: String,
    #[serde(rename = "type")]
    security_type: Option<String>,
    sic_code: Option<String>,
    sic_description: Option<String>,
    share_class_shares_outstanding: Option<f64>,
    market_cap: Option<f64>,
    primary_exchange: Option<String>,
    cik: Option<String>,
}

fn parse_security_details(
    body: &[u8],
    symbol: &Symbol,
    ticker: &str,
) -> Result<DetailsAnswer, FetchError> {
    let response: DetailsResponse =
        serde_json::from_slice(body).map_err(|error| FetchError::Malformed {
            reason: error.to_string(),
        })?;
    let Some(row) = response.results else {
        return Ok(DetailsAnswer::Missing);
    };
    let refused = |cause| {
        Ok(DetailsAnswer::Refused(RefusedRow {
            ticker: ticker.to_string(),
            cause,
        }))
    };
    if row.ticker != ticker {
        return refused(RowRefusal::Unrequested);
    }
    let details = (|| {
        Ok::<_, RowRefusal>(SecurityDetails::new(
            symbol.clone(),
            row.security_type
                .map(|code| security_type(&code).ok_or(RowRefusal::SecurityType { raw: code }))
                .transpose()?,
            row.sic_code
                .as_deref()
                .map(IndustryCode::new)
                .transpose()
                .map_err(RowRefusal::IndustryCode)?,
            row.sic_description,
            row.share_class_shares_outstanding
                .map(Shares::from_float)
                .transpose()
                .map_err(RowRefusal::Shares)?,
            row.market_cap
                .map(capitalization_to_the_cent)
                .transpose()
                .map_err(RowRefusal::Dollars)?,
            row.primary_exchange
                .as_deref()
                .map(MarketIdentifierCode::new)
                .transpose()
                .map_err(RowRefusal::Exchange)?,
            row.cik
                .map(|raw| {
                    raw.parse::<u64>()
                        .map(CentralIndexKey::new)
                        .map_err(|_| RowRefusal::CentralIndexKey { raw })
                })
                .transpose()?,
        ))
    })();
    match details {
        Ok(details) => Ok(DetailsAnswer::Details(details)),
        Err(cause) => refused(cause),
    }
}

/// Massive's capitalization, its own float product a few hundred-millionths off the cent, rounded to the cent.
pub(crate) fn capitalization_to_the_cent(dollars: f64) -> Result<Dollars, DollarsRefusal> {
    Dollars::from_float((dollars * 100.0).round() / 100.0)
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
        if is_exchange_test_ticker(&row.ticker) {
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

#[derive(Deserialize)]
struct ConditionsResponse {
    results: Vec<ConditionRow>,
    /// Present only when the listing continues past this page.
    next_url: Option<String>,
}

#[derive(Deserialize)]
struct ConditionRow {
    id: u16,
    #[serde(rename = "type")]
    kind: String,
    update_rules: Option<ConditionUpdateRules>,
    /// The letter each tape spells the condition with, keyed by the vendor's name for the plan.
    #[serde(default)]
    sip_mapping: BTreeMap<String, String>,
    /// Kept for history and no longer printed.
    #[serde(default)]
    legacy: bool,
}

#[derive(Deserialize)]
struct ConditionUpdateRules {
    consolidated: ConditionRules,
}

#[derive(Deserialize)]
struct ConditionRules {
    updates_volume: bool,
    updates_high_low: bool,
    updates_open_close: bool,
}

/// The sale and trade-through-exempt conditions, the two kinds a print's `conditions` holds; a sale condition
/// without rules, or a listing that continues, refuses the whole table, since a missing code would read as unknown.
fn parse_trade_conditions(body: &[u8]) -> Result<TradeConditions, FetchError> {
    let malformed = |reason: String| FetchError::Malformed { reason };
    let response: ConditionsResponse =
        serde_json::from_slice(body).map_err(|error| malformed(error.to_string()))?;
    if let Some(next) = response.next_url {
        return Err(malformed(format!("the listing continues at {next}")));
    }
    let mut rules = BTreeMap::new();
    for row in response.results {
        match row.kind.as_str() {
            "sale_condition" | "trade_thru_exempt" => {
                let consolidated = row
                    .update_rules
                    .ok_or_else(|| malformed(format!("condition {} has no update rules", row.id)))?
                    .consolidated;
                let rule = UpdateRules::new(
                    consolidated.updates_volume,
                    consolidated.updates_high_low,
                    consolidated.updates_open_close,
                );
                let letter = |plan: &str| -> Result<Option<char>, FetchError> {
                    match row.sip_mapping.get(plan) {
                        None => Ok(None),
                        Some(spelled) => match condition_letter(spelled) {
                            Some(letter) => Ok(Some(letter)),
                            None => Err(malformed(format!(
                                "condition {} spells {plan} as `{spelled}`",
                                row.id
                            ))),
                        },
                    }
                };
                let condition = Condition::new(rule, letter("CTA")?, letter("UTP")?, row.legacy);
                if rules.insert(row.id, condition).is_some() {
                    return Err(malformed(format!("condition {} is listed twice", row.id)));
                }
            }
            _ => {}
        }
    }
    Ok(TradeConditions::new(rules))
}

/// Massive's security type code in our terms; `None` for a code no variant names.
pub fn security_type(code: &str) -> Option<SecurityType> {
    match code {
        "CS" => Some(SecurityType::CommonStock),
        "ETF" => Some(SecurityType::ExchangeTradedFund),
        "WARRANT" => Some(SecurityType::Warrant),
        "ADRC" => Some(SecurityType::DepositaryReceipt),
        "FUND" => Some(SecurityType::Fund),
        "UNIT" => Some(SecurityType::Unit),
        "SP" => Some(SecurityType::StructuredProduct),
        "PFD" => Some(SecurityType::PreferredStock),
        "ETS" => Some(SecurityType::ExchangeTradedSecurity),
        "ETN" => Some(SecurityType::ExchangeTradedNote),
        "ETV" => Some(SecurityType::ExchangeTradedVehicle),
        "RIGHT" => Some(SecurityType::Right),
        "INDEX" => Some(SecurityType::Index),
        _ => None,
    }
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

/// `symbol` in Massive's notation, the inverse of `alpaca_symbol`: `BC.PRC` is `BCpC`, `ABC.WS` is `ABCw` and `ABC.RT`
/// is `ABCr`, and any other suffix is written as Massive writes it, `BRK.B`.
pub fn massive_ticker(symbol: &Symbol) -> String {
    match symbol.as_str().split_once('.') {
        Some((root, "WS")) => format!("{root}w"),
        Some((root, "RT")) => format!("{root}r"),
        Some((root, suffix)) => match suffix.strip_prefix("PR") {
            Some(series) if !series.is_empty() => format!("{root}p{series}"),
            Some(_) | None => symbol.as_str().to_string(),
        },
        None => symbol.as_str().to_string(),
    }
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

    /// Rows trimmed from the live conditions listing of 2026-10-05: the retired CAP election, two sale conditions, the
    /// trade-through exempt flag, and a quote condition that must be left out.
    const CONDITIONS: &str = r#"{"results": [{"id": 6, "type": "sale_condition", "name": "CAP Election", "asset_class": "stocks", "sip_mapping": {"CTA": "I"}, "update_rules": {"consolidated": {"updates_high_low": true, "updates_open_close": true, "updates_volume": true}, "market_center": {"updates_high_low": true, "updates_open_close": true, "updates_volume": true}}, "data_types": ["trade"], "legacy": true}, {"id": 10, "type": "sale_condition", "name": "Derivatively Priced", "asset_class": "stocks", "sip_mapping": {"CTA": "4", "UTP": "4"}, "update_rules": {"consolidated": {"updates_high_low": true, "updates_open_close": false, "updates_volume": true}, "market_center": {"updates_high_low": true, "updates_open_close": false, "updates_volume": true}}, "data_types": ["trade"]}, {"id": 37, "type": "sale_condition", "name": "Odd Lot Trade", "asset_class": "stocks", "sip_mapping": {"CTA": "I", "UTP": "I", "FINRA_TDDS": "I"}, "update_rules": {"consolidated": {"updates_high_low": false, "updates_open_close": false, "updates_volume": true}, "market_center": {"updates_high_low": false, "updates_open_close": false, "updates_volume": true}}, "data_types": ["trade"]}, {"id": 41, "type": "trade_thru_exempt", "name": "Trade Thru Exempt", "asset_class": "stocks", "sip_mapping": {"CTA": "1", "UTP": "X"}, "update_rules": {"consolidated": {"updates_high_low": true, "updates_open_close": true, "updates_volume": true}, "market_center": {"updates_high_low": true, "updates_open_close": true, "updates_volume": true}}, "data_types": ["trade"]}, {"id": 41, "type": "settlement_condition", "name": "Cash Only Settlement", "asset_class": "stocks", "sip_mapping": {"CTA": "A"}, "data_types": ["bbo", "nbbo"]}, {"id": 1, "type": "quote_condition", "name": "Regular Two-Sided Open", "asset_class": "stocks", "sip_mapping": {"CTA": "R", "UTP": "R"}, "data_types": ["bbo", "nbbo"]}], "status": "OK", "request_id": "x", "count": 5}"#;

    #[test]
    fn test_conditions_keep_the_trade_kinds_with_their_consolidated_rules() {
        let conditions = parse_trade_conditions(CONDITIONS.as_bytes()).unwrap();
        let codes: Vec<u16> = conditions.conditions().keys().copied().collect();
        assert_eq!(codes, [6, 10, 37, 41]);
        let condition = |code: u16| conditions.conditions()[&code];
        assert_eq!(condition(10).rules(), UpdateRules::new(true, true, false));
        assert_eq!(condition(37).rules(), UpdateRules::new(true, false, false));
        assert_eq!(condition(41).rules(), UpdateRules::new(true, true, true));
        assert_eq!(
            (
                condition(41).consolidated_tape(),
                condition(41).unlisted_trading()
            ),
            (Some('1'), Some('X'))
        );
        assert!(!condition(37).retired());
        assert!(condition(6).retired());
    }

    #[test]
    fn test_every_security_type_code_seen_maps_to_one_of_ours() {
        // The thirteen codes the legacy snapshots hold, 2021-08-23 to 2026-10-01.
        let codes = [
            "CS", "ETF", "WARRANT", "ADRC", "FUND", "UNIT", "SP", "PFD", "ETS", "ETN", "ETV",
            "RIGHT", "INDEX",
        ];
        let mapped: std::collections::BTreeSet<_> = codes
            .iter()
            .filter_map(|code| security_type(code))
            .collect();
        assert_eq!(mapped.len(), 13);
        assert_eq!(security_type("OS"), None);
    }

    /// Massive's splits endpoint on 2026-10-07: its first row, a fractional ratio and the cursor, plus a zero side.
    const SPLITS_PAGE: &str = r#"{"results":[{"execution_date":"2026-12-17","id":"Ee311332e3f60ade13ccbf987b2d89b2085533e7db481fe8b311b5d167e949c8e","split_from":50,"split_to":1,"ticker":"DPU"},{"execution_date":"2026-10-23","id":"E3366d86694dc48ac06cd3a951a03c211a348540830849916066a920737090ee3","split_from":1,"split_to":0.7137,"ticker":"VSEAX"},{"execution_date":"2026-11-02","id":"Eabc","split_from":1,"split_to":0,"ticker":"ZERO"}],"status":"OK","request_id":"191409f29929687bb8cc5810c2fa7054","next_url":"https://api.massive.com/v3/reference/splits?cursor=YXA9MyZhcz0mbGltaXQ9MyZvcmRlcj1kZXNjJnNvcnQ9ZXhlY3V0aW9uX2RhdGU"}"#;

    #[test]
    fn test_a_splits_page_keeps_fractional_ratios_and_answers_only_its_cursor() {
        let mut splits = Splits::default();
        let cursor = parse_splits_page(SPLITS_PAGE.as_bytes(), &mut splits).unwrap();
        assert_eq!(
            cursor.as_deref(),
            Some("YXA9MyZhcz0mbGltaXQ9MyZvcmRlcj1kZXNjJnNvcnQ9ZXhlY3V0aW9uX2RhdGU")
        );
        let read: Vec<(String, String, u64, u64)> = splits
            .splits()
            .iter()
            .map(|split| {
                (
                    split.symbol().as_str().to_string(),
                    split.executed_on().to_string(),
                    split.ratio().from().units(),
                    split.ratio().to().units(),
                )
            })
            .collect();
        assert_eq!(
            read,
            [
                (
                    "DPU".to_string(),
                    "2026-12-17".to_string(),
                    50_000_000,
                    1_000_000
                ),
                (
                    "VSEAX".to_string(),
                    "2026-10-23".to_string(),
                    1_000_000,
                    713_700
                ),
            ]
        );
        assert_eq!(splits.refused().len(), 1);
        assert_eq!(<&str>::from(splits.refused()[0].cause()), "split_ratio");
        let last = SPLITS_PAGE.replace(
            r#""next_url":"https://api.massive.com/v3/reference/splits?cursor=YXA9MyZhcz0mbGltaXQ9MyZvcmRlcj1kZXNjJnNvcnQ9ZXhlY3V0aW9uX2RhdGU""#,
            r#""next_url":null"#,
        );
        assert_eq!(
            parse_splits_page(last.as_bytes(), &mut Splits::default()),
            Ok(None)
        );
        // The same page read twice, as a cursor revisiting rows would: every repeated action is refused.
        parse_splits_page(SPLITS_PAGE.as_bytes(), &mut splits).unwrap();
        let unique = splits.unique();
        assert!(unique.splits().is_empty());
        assert_eq!(
            unique
                .refused()
                .iter()
                .filter(|row| row.cause() == &RowRefusal::Duplicate)
                .count(),
            4
        );
    }

    #[test]
    fn test_an_action_repeated_beside_a_refused_copy_is_refused_too() {
        // DPU's action filed again under a ticker no symbol holds: the valid copy cannot be trusted either.
        let page = r#"{"results":[{"execution_date":"2026-12-17","id":"E1","split_from":50,"split_to":1,"ticker":"DPU"},{"execution_date":"2026-12-17","id":"E1","split_from":50,"split_to":1,"ticker":"DPU.WARRANTS"}],"next_url":null}"#;
        let mut splits = Splits::default();
        parse_splits_page(page.as_bytes(), &mut splits).unwrap();
        let unique = splits.unique();
        assert!(unique.splits().is_empty());
        let causes: Vec<&str> = unique
            .refused()
            .iter()
            .map(|row| row.cause().into())
            .collect();
        assert_eq!(causes, ["symbol", "duplicate"]);
    }

    #[test]
    fn test_details_read_every_field_and_refuse_an_answer_about_another_ticker() {
        // AAPL and BACpL as Massive answered for 2026-10-01, trimmed to the fields kept.
        let apple = r#"{"results":{"ticker":"AAPL","type":"CS","sic_code":"3571","sic_description":"ELECTRONIC COMPUTERS","share_class_shares_outstanding":14594180000,"market_cap":4820749537600.0,"primary_exchange":"XNAS","cik":"0000320193"},"status":"OK"}"#;
        let preferred = r#"{"results":{"ticker":"BACpL","type":"PFD","sic_code":"6021","sic_description":"NATIONAL COMMERCIAL BANKS","share_class_shares_outstanding":3080000,"primary_exchange":"XNYS","cik":"0000070858"},"status":"OK"}"#;
        let aapl = Symbol::new("AAPL").unwrap();
        let DetailsAnswer::Details(details) =
            parse_security_details(apple.as_bytes(), &aapl, "AAPL").unwrap()
        else {
            panic!("AAPL did not read");
        };
        assert_eq!(details.security_type(), Some(SecurityType::CommonStock));
        assert_eq!(details.industry_code().map(IndustryCode::code), Some(3571));
        assert_eq!(
            details.shares_outstanding().map(Shares::units),
            Some(14_594_180_000_000_000)
        );
        assert_eq!(
            details.market_capitalization().map(Dollars::millionths),
            Some(4_820_749_537_600_000_000)
        );
        assert_eq!(
            details.central_index_key().map(CentralIndexKey::value),
            Some(320_193)
        );
        let bac = Symbol::new("BAC.PRL").unwrap();
        assert_eq!(massive_ticker(&bac), "BACpL");
        let DetailsAnswer::Details(details) =
            parse_security_details(preferred.as_bytes(), &bac, "BACpL").unwrap()
        else {
            panic!("BACpL did not read");
        };
        assert_eq!(details.symbol(), &bac);
        assert_eq!(details.market_capitalization(), None);
        assert!(matches!(
            parse_security_details(apple.as_bytes(), &bac, "BACpL"),
            Ok(DetailsAnswer::Refused(row)) if row.cause() == &RowRefusal::Unrequested
        ));
        assert_eq!(
            parse_security_details(br#"{"status":"OK"}"#, &aapl, "AAPL"),
            Ok(DetailsAnswer::Missing)
        );
    }

    proptest::proptest! {
        /// Every Massive ticker the mapping translates comes back through `massive_ticker` as written.
        #[test]
        fn property_massive_notation_round_trips(
            root in "[A-Z]{1,4}",
            suffix in proptest::sample::select(vec!["", "pA", "pB", "w", "r", ".A", ".B", ".U"]),
        ) {
            let ticker = format!("{root}{suffix}");
            let symbol = alpaca_symbol(&ticker).unwrap();
            proptest::prop_assert_eq!(massive_ticker(&symbol), ticker);
        }
    }

    #[test]
    fn test_a_capitalization_off_the_cent_rounds_onto_it() {
        assert!(Dollars::from_float(1_234.567_891_2).is_err());
        assert_eq!(
            capitalization_to_the_cent(1_234.567_891_2).map(Dollars::millionths),
            Ok(1_234_570_000)
        );
        assert!(capitalization_to_the_cent(-1.0).is_err());
    }
}
