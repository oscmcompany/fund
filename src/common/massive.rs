//! Massive market data client: whole-market daily bars, one request per session.
//!
//! Deliberately narrow: Massive answers only what every US stock did on a date, and Alpaca answers
//! everything about our account and the current moment. Three properties of the grouped endpoint
//! make that split load-bearing rather than aesthetic.
//!
//! **It takes no symbol list.** A backfill through Alpaca's bars endpoint can only pass Alpaca's
//! *current* tradable set, so every symbol delisted since the start date is absent from its own
//! history and the model trains on a universe that survived by construction.
//!
//! **It returns the whole market, which keeps the universe open.** `load_liquidity` reads averages
//! out of `equity_bars`, so a sync that refreshed only the tickers already in the universe could
//! never admit a name that became liquid — it would ratchet closed `LIQUIDITY_LOOKBACK_DAYS` after
//! the last seed and shrink from there.
//!
//! **It is the consolidated tape, with no feed tiers.** Alpaca's `iex` feed carries a few percent
//! of consolidated volume — survivable for quotes, not for bars, since `volume` and `vw` would be
//! computed over that few percent while the liquidity thresholds assume the real numbers.

use chrono::{DateTime, NaiveDate};
use serde::Deserialize;
use tracing::{info, warn};

use crate::common::types::{BarInterval, EquityBar, EquitySplit, SessionDate, Ticker};

/// Why the client could not be constructed.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CredentialsError {
    #[error("{variable} environment variable is not set")]
    Missing { variable: &'static str },
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
}

/// Why a request failed.
#[derive(Debug, thiserror::Error)]
pub enum MassiveError {
    /// The request never produced a response: connection refused, timed out, TLS failure.
    #[error("Massive request failed: {0}")]
    Request(#[from] reqwest::Error),
    /// Massive answered with a non-success status.
    #[error("Massive returned status {status}: {body}")]
    Api { status: u16, body: String },
    /// Massive answered successfully with something that could not be interpreted.
    #[error("Massive response could not be parsed: {0}")]
    Parse(String),
}

/// Massive API credentials.
///
/// Deliberately does not derive `Debug`: it holds an API key, and a derived `Debug` puts that key
/// into any log line or panic message that formats a struct containing one.
#[derive(Clone)]
pub struct MassiveCredentials {
    base_url: String,
    api_key: String,
}

impl MassiveCredentials {
    /// Constructs from explicit values, rejecting empties.
    ///
    /// An empty key reaches Massive as a 401, which reads like a permissions problem rather than
    /// the configuration one it is.
    pub fn new(base_url: String, api_key: String) -> Result<Self, CredentialsError> {
        if base_url.is_empty() {
            return Err(CredentialsError::Empty { field: "base_url" });
        }
        if api_key.is_empty() {
            return Err(CredentialsError::Empty { field: "api_key" });
        }
        Ok(Self { base_url, api_key })
    }

    /// Reads `MASSIVE_BASE_URL` and `MASSIVE_API_KEY` from the environment.
    pub fn from_env() -> Result<Self, CredentialsError> {
        let base_url =
            std::env::var("MASSIVE_BASE_URL").map_err(|_| CredentialsError::Missing {
                variable: "MASSIVE_BASE_URL",
            })?;
        let api_key = std::env::var("MASSIVE_API_KEY").map_err(|_| CredentialsError::Missing {
            variable: "MASSIVE_API_KEY",
        })?;
        Self::new(base_url, api_key)
    }
}

/// Builds the grouped-daily URL for one date.
///
/// The configured base may carry a trailing slash — the `development/chris.addy` secret is
/// `https://api.massive.com/` — and joining it naively yields a `//` that the API answers with a
/// 404. Normalized here rather than at the call site, because there is only one right answer and a
/// 404 from a doubled slash reads like a wrong path.
fn grouped_bars_url(base: &str, date: NaiveDate) -> String {
    format!(
        "{}/v2/aggs/grouped/locale/us/market/stocks/{}",
        base.trim_end_matches('/'),
        date.format("%Y-%m-%d")
    )
}

/// Builds the first splits URL, on the same normalization as [`grouped_bars_url`].
///
/// Unfiltered by date. The whole table is 29 pages and three seconds, so bounding it would buy
/// nothing and cost a start date nobody could later justify.
fn splits_url(base: &str) -> String {
    format!(
        "{}/v3/reference/splits?limit={SPLITS_PAGE_SIZE}",
        base.trim_end_matches('/')
    )
}

/// Rows per splits page.
///
/// The endpoint's documented maximum is 1,000 and it answers a larger value with a validation error
/// rather than a clamp, so this is a ceiling rather than a preference.
const SPLITS_PAGE_SIZE: usize = 1_000;

/// Pages followed before a splits fetch gives up.
///
/// The full table is 29 pages; this bounds a cursor that never terminates, which would otherwise
/// spin against the API rather than fail. Generous enough that ordinary growth cannot reach it.
const SPLITS_PAGE_LIMIT: usize = 200;

/// One row of the splits response.
///
/// Massive publishes exactly these five fields — there is no adjustment factor to read, so the
/// ratio is the whole of what a split says.
///
/// Both ratio sides are `f64` because the feed sends fractional ones: 5,344 of 28,135 live rows are
/// mutual-fund reallocations like `1 -> 1.0056`. A whole-number type does not merely drop those
/// rows, it fails the whole page they are on, and they are on most pages.
#[derive(Deserialize, Debug)]
struct SplitRow {
    id: String,
    ticker: String,
    execution_date: NaiveDate,
    split_from: f64,
    split_to: f64,
}

/// The splits envelope. `next_url` is absent on the last page, which is how pagination ends.
#[derive(Deserialize)]
struct SplitsResponse {
    results: Option<Vec<SplitRow>>,
    next_url: Option<String>,
}

/// Converts an untrusted splits row into a validated [`EquitySplit`], or `None`.
///
/// Non-common-stock symbols are dropped for the reason [`is_common_stock_symbol`] gives, and a
/// zero-sided ratio by the constructor's own validation. The execution date is already an exchange
/// calendar date, so it is wrapped rather than derived from an instant.
fn parse_split(row: &SplitRow) -> Option<EquitySplit> {
    if !is_common_stock_symbol(&row.ticker) {
        return None;
    }
    EquitySplit::new(
        row.id.clone(),
        Ticker::new(&row.ticker)?,
        SessionDate::from_date(row.execution_date),
        row.split_from,
        row.split_to,
    )
    .ok()
}

/// One row of the grouped-daily response.
///
/// Every OHLCV field is optional because the API omits them for thinly traded or halted
/// instruments. `EquityBar::new` is what decides whether the result is usable.
#[derive(Deserialize, Debug)]
struct GroupedBarRow {
    #[serde(rename = "T")]
    ticker: String,
    c: Option<f64>,
    h: Option<f64>,
    l: Option<f64>,
    n: Option<u64>,
    o: Option<f64>,
    t: u64,
    v: Option<f64>,
    vw: Option<f64>,
}

/// The grouped-daily envelope. Unknown fields (`adjusted`, `queryCount`, `request_id`, `status`)
/// are ignored by serde's default behaviour.
#[derive(Deserialize)]
struct GroupedResponse {
    #[serde(rename = "resultsCount", default)]
    results_count: u64,
    results: Option<Vec<GroupedBarRow>>,
}

/// Whether a raw symbol is common stock rather than a preferred, warrant, unit, or right.
///
/// Massive encodes share class in the *case* of the symbol: `GSpD` is Goldman Sachs preferred
/// series D, `BCpC` a preferred, `TpC` another. `Ticker::new` uppercases before validating — which
/// is right for user input and wrong here, because it collapses `BCpC` onto `BCPC`, the common
/// stock of an entirely different company. The two then share a primary key of
/// `(ticker, bar_interval, timestamp)` and the upsert silently keeps whichever arrived last, so a
/// real mid-cap's bar can be replaced by a thinly traded preferred's.
///
/// Measured on one live session: 12,445 symbols, of which 2 collided this way. Small, silent, and
/// corrupting to exactly the names the strategy would trade — the preferred's volume is a rounding
/// error against the common's, so the liquidity screen, the correlation, and the model input all
/// take the wrong number.
///
/// Requiring the raw form to already be uppercase drops every one of them. That is not a
/// workaround: this strategy trades common stock, and a preferred share is not something it should
/// have been offered in the first place.
fn is_common_stock_symbol(raw: &str) -> bool {
    let trimmed = raw.trim();
    !trimmed.is_empty() && trimmed == trimmed.to_ascii_uppercase()
}

/// Converts an untrusted row into a validated [`EquityBar`], or `None`.
///
/// Returns `None` for a non-common-stock symbol, an unparseable ticker, a missing OHLCV field, a
/// volume that does not fit an `i64`, or prices that do not form a coherent candle. All of those
/// are conditions to drop the row over rather than fail the date: a grouped response covers the
/// whole market, so one malformed instrument would otherwise cost every other symbol that session.
fn parse_bar(row: &GroupedBarRow) -> Option<EquityBar> {
    if !is_common_stock_symbol(&row.ticker) {
        return None;
    }
    let ticker = Ticker::new(&row.ticker)?;
    let timestamp = DateTime::from_timestamp_millis(i64::try_from(row.t).ok()?)?;

    let volume = row
        .v
        .filter(|volume| volume.is_finite() && *volume >= 0.0)?;
    let rounded = volume.round();
    if rounded > i64::MAX as f64 {
        return None;
    }

    EquityBar::new(
        ticker,
        BarInterval::OneDay,
        timestamp,
        row.o?,
        row.h?,
        row.l?,
        row.c?,
        rounded as i64,
        row.vw,
        row.n.and_then(|count| i64::try_from(count).ok()),
    )
    .ok()
}

/// Fetches whole-market daily bars from Massive.
pub struct MassiveClient {
    http_client: reqwest::Client,
    credentials: MassiveCredentials,
}

impl MassiveClient {
    pub fn new(credentials: MassiveCredentials) -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                // Generous relative to Alpaca's, because a grouped response is the whole market:
                // ten thousand rows of JSON rather than a page of one symbol's history.
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .unwrap_or_default(),
            credentials,
        }
    }

    pub fn from_env() -> Result<Self, CredentialsError> {
        Ok(Self::new(MassiveCredentials::from_env()?))
    }

    /// Starts a request carrying the API key as a bearer token rather than a query parameter.
    ///
    /// Massive accepts either. The header is used because `reqwest` puts the full URL into the
    /// `Display` of a decode failure, so a key in the query string reaches every error message and
    /// log line that reports one — observed while building the splits fetch.
    fn authorized(&self, url: &str) -> reqwest::RequestBuilder {
        self.http_client
            .get(url)
            .bearer_auth(&self.credentials.api_key)
    }

    /// Constructs a client pointed at an explicit base, for tests against a local HTTP mock.
    #[cfg(test)]
    fn for_tests(base_url: &str) -> Self {
        Self::new(
            MassiveCredentials::new(base_url.to_string(), "test-key".to_string())
                .expect("test credentials must be valid"),
        )
    }

    /// Fetches every US stock's daily bar for `date`.
    ///
    /// Returns an empty vector for a weekend or holiday: the API answers those with a zero result
    /// count rather than an error, and so does this. A caller iterating calendar days therefore
    /// needs no trading-day filter of its own.
    ///
    /// Prices are split- and dividend-adjusted (`adjusted=true`), matching the `adjustment=all`
    /// the Alpaca path used, so the two produce comparable series.
    pub async fn fetch_grouped_daily(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<EquityBar>, MassiveError> {
        let url = grouped_bars_url(&self.credentials.base_url, date);

        let response = self
            .authorized(&url)
            .query(&[("adjusted", "true")])
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(MassiveError::Api { status, body });
        }

        let payload: GroupedResponse = response.json().await.map_err(|error| {
            MassiveError::Parse(format!("Failed to parse grouped bars: {error}"))
        })?;

        let Some(rows) = payload.results else {
            // A trading day with no results is worth a line; a weekend is not, but the two are
            // indistinguishable here and the caller has the calendar.
            info!(%date, "Massive returned no results for the date");
            return Ok(Vec::new());
        };

        let received = rows.len();
        let bars: Vec<EquityBar> = rows.iter().filter_map(parse_bar).collect();

        let dropped = received.saturating_sub(bars.len());
        if dropped > 0 {
            // Expected and routine -- warrants, units, and halted instruments all land here. Worth
            // counting so a sudden change in the share is visible.
            warn!(%date, dropped, received, "Dropped grouped rows that failed validation");
        }

        info!(
            %date,
            bars = bars.len(),
            reported = payload.results_count,
            "Grouped daily bars fetched"
        );
        Ok(bars)
    }

    /// Fetches every stock split Massive knows about, following the cursor to the last page.
    ///
    /// The response covers announced-but-unexecuted splits as well as historical ones, so a caller
    /// holding the result has the feed's whole current opinion rather than a window of it — which is
    /// what makes replacing the stored table safe when a split is cancelled and disappears.
    pub async fn fetch_splits(&self) -> Result<Vec<EquitySplit>, MassiveError> {
        let mut url = splits_url(&self.credentials.base_url);
        let mut splits: Vec<EquitySplit> = Vec::new();
        let mut received = 0usize;

        for page in 1..=SPLITS_PAGE_LIMIT {
            let response = self.authorized(&url).send().await?;

            if !response.status().is_success() {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();
                return Err(MassiveError::Api { status, body });
            }

            let payload: SplitsResponse = response
                .json()
                .await
                .map_err(|error| MassiveError::Parse(format!("Failed to parse splits: {error}")))?;

            let rows = payload.results.unwrap_or_default();
            received += rows.len();
            splits.extend(rows.iter().filter_map(parse_split));

            let Some(next_url) = payload.next_url else {
                let dropped = received.saturating_sub(splits.len());
                if dropped > 0 {
                    // Routine: the same preferreds, warrants, and units the bar path drops.
                    warn!(
                        dropped,
                        received, "Dropped splits rows that failed validation"
                    );
                }
                info!(splits = splits.len(), pages = page, "Splits fetched");
                return Ok(splits);
            };
            // The cursor URL is absolute and carries no credential, so the key is re-attached by the
            // `query` call above rather than being present in `next_url` already.
            url = next_url;
        }

        Err(MassiveError::Parse(format!(
            "splits pagination did not end within {SPLITS_PAGE_LIMIT} pages"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(value: &str) -> NaiveDate {
        NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("a valid test date")
    }

    /// One row of a realistic grouped response. `t` is the session's Eastern midnight in epoch
    /// milliseconds, which is how the API stamps a daily bar.
    fn body(rows: &str) -> String {
        format!(
            r#"{{"status":"OK","adjusted":true,"queryCount":1,"resultsCount":1,"results":[{rows}]}}"#
        )
    }

    const APPLE: &str = r#"{"T":"AAPL","v":52000000,"vw":189.5,"o":188.0,"c":190.0,"h":191.0,"l":187.5,"t":1751000000000,"n":450000}"#;

    #[test]
    fn test_the_base_url_is_normalized_before_the_path_is_appended() {
        let expected = "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2026-06-05";
        for base in [
            "https://api.massive.com",
            "https://api.massive.com/",
            "https://api.massive.com//",
        ] {
            assert_eq!(
                grouped_bars_url(base, date("2026-06-05")),
                expected,
                "{base}"
            );
        }
    }

    /// `matches!` rather than `assert_eq!`, because `MassiveCredentials` deliberately has no
    /// `Debug` — it holds an API key — and comparing a `Result` that contains it needs one.
    #[test]
    fn test_credentials_reject_empty_values() {
        assert!(matches!(
            MassiveCredentials::new(String::new(), "key".to_string()),
            Err(CredentialsError::Empty { field: "base_url" })
        ));
        assert!(matches!(
            MassiveCredentials::new("https://api.massive.com".to_string(), String::new()),
            Err(CredentialsError::Empty { field: "api_key" })
        ));
        assert!(
            MassiveCredentials::new("https://api.massive.com".to_string(), "key".to_string())
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_a_grouped_response_becomes_validated_bars() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v2/aggs/grouped/locale/us/market/stocks/2026-06-05")
            .match_query(mockito::Matcher::UrlEncoded(
                "adjusted".into(),
                "true".into(),
            ))
            .match_header("authorization", "Bearer test-key")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body(APPLE))
            .create_async()
            .await;

        let bars = MassiveClient::for_tests(&server.url())
            .fetch_grouped_daily(date("2026-06-05"))
            .await
            .expect("a successful fetch");

        mock.assert_async().await;
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].ticker().as_str(), "AAPL");
        assert_eq!(bars[0].bar_interval(), BarInterval::OneDay);
        assert_eq!(bars[0].close_price(), 190.0);
        assert_eq!(bars[0].volume(), 52_000_000);
        assert_eq!(bars[0].volume_weighted_average_price(), Some(189.5));
        assert_eq!(bars[0].transactions(), Some(450_000));
    }

    /// A weekend answers with a null `results`, not an error, and must not read as a failure.
    #[tokio::test]
    async fn test_a_date_with_no_results_yields_no_bars_rather_than_an_error() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":"OK","resultsCount":0,"queryCount":0}"#)
            .create_async()
            .await;

        let bars = MassiveClient::for_tests(&server.url())
            .fetch_grouped_daily(date("2026-06-06"))
            .await
            .expect("an empty date is not an error");
        assert!(bars.is_empty());
    }

    /// The whole point of dropping rows rather than failing the date: a grouped response covers
    /// the entire market, so one malformed instrument would otherwise cost every other symbol.
    #[tokio::test]
    async fn test_one_malformed_row_does_not_cost_the_rest_of_the_market() {
        let mut server = mockito::Server::new_async().await;
        // A ticker that fails format validation, a candle whose low exceeds its high, a row with a
        // missing close, and one good bar.
        let rows = format!(
            r#"{{"T":"not a ticker","v":1000,"o":1.0,"c":1.0,"h":1.0,"l":1.0,"t":1751000000000}},
               {{"T":"BAD","v":1000,"o":5.0,"c":5.0,"h":1.0,"l":9.0,"t":1751000000000}},
               {{"T":"GAPS","v":1000,"o":5.0,"h":6.0,"l":4.0,"t":1751000000000}},
               {APPLE}"#
        );
        server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body(&rows))
            .create_async()
            .await;

        let bars = MassiveClient::for_tests(&server.url())
            .fetch_grouped_daily(date("2026-06-05"))
            .await
            .expect("a successful fetch");

        assert_eq!(bars.len(), 1, "only the coherent bar survives");
        assert_eq!(bars[0].ticker().as_str(), "AAPL");
    }

    /// The collision this guards against, with the two real cases from a live session.
    ///
    /// `BCpC` and `TpC` are preferred shares; uppercased they become `BCPC` (Balchem) and `TPC`
    /// (Tutor Perini), both common stocks the strategy could genuinely hold. Sharing a primary key
    /// means the upsert keeps whichever row arrived last.
    #[test]
    fn test_preferred_share_symbols_are_not_mistaken_for_common_stock() {
        for preferred in ["BCpC", "TpC", "GSpD"] {
            assert!(
                !is_common_stock_symbol(preferred),
                "{preferred} must not be accepted as common stock"
            );
        }
        for common in ["BCPC", "TPC", "AAPL", "BRK.B", "GS"] {
            assert!(is_common_stock_symbol(common), "{common} must be accepted");
        }
        assert!(!is_common_stock_symbol(""));
        assert!(!is_common_stock_symbol("   "));
    }

    /// End to end through the parser: a preferred and its common namesake in one response must not
    /// collapse into one bar.
    #[tokio::test]
    async fn test_a_preferred_share_does_not_overwrite_its_common_namesake() {
        let mut server = mockito::Server::new_async().await;
        let rows = concat!(
            r#"{"T":"BCPC","v":500000,"o":180.0,"c":182.0,"h":183.0,"l":179.0,"t":1751000000000},"#,
            r#"{"T":"BCpC","v":900,"o":25.0,"c":25.1,"h":25.2,"l":24.9,"t":1751000000000}"#
        );
        server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body(rows))
            .create_async()
            .await;

        let bars = MassiveClient::for_tests(&server.url())
            .fetch_grouped_daily(date("2026-06-05"))
            .await
            .expect("a successful fetch");

        assert_eq!(bars.len(), 1, "only the common stock survives");
        assert_eq!(bars[0].ticker().as_str(), "BCPC");
        assert_eq!(bars[0].volume(), 500_000, "the common stock's own volume");
    }

    #[tokio::test]
    async fn test_a_non_success_status_is_an_error_carrying_the_body() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", mockito::Matcher::Any)
            .with_status(403)
            .with_body("not entitled")
            .create_async()
            .await;

        let error = MassiveClient::for_tests(&server.url())
            .fetch_grouped_daily(date("2026-06-05"))
            .await
            .expect_err("a 403 is an error");

        match error {
            MassiveError::Api { status, body } => {
                assert_eq!(status, 403);
                assert_eq!(body, "not entitled");
            }
            other => panic!("expected an Api error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_an_unparseable_body_is_reported_as_a_parse_failure() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body("{ not json")
            .create_async()
            .await;

        let error = MassiveClient::for_tests(&server.url())
            .fetch_grouped_daily(date("2026-06-05"))
            .await
            .expect_err("malformed JSON is an error");
        assert!(matches!(error, MassiveError::Parse(_)), "{error:?}");
    }

    // --- splits ---

    /// Two rows as the live endpoint returns them: a forward split and a reverse one, with the
    /// execution date already an Eastern calendar date and no adjustment factor to read.
    const SPLIT_ROWS: &str = r#"{"execution_date":"2026-07-06","id":"E1","split_from":1,"split_to":2,"ticker":"MNST"},
        {"execution_date":"2026-10-06","id":"E2","split_from":3,"split_to":1,"ticker":"ETHA"},
        {"execution_date":"2026-09-14","id":"E3","split_from":1,"split_to":1.0056,"ticker":"NSRSX"}"#;

    #[tokio::test]
    async fn test_a_splits_response_becomes_validated_splits() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v3/reference/splits")
            .match_query(mockito::Matcher::UrlEncoded("limit".into(), "1000".into()))
            .match_header("authorization", "Bearer test-key")
            .with_status(200)
            .with_body(format!(r#"{{"status":"OK","results":[{SPLIT_ROWS}]}}"#))
            .create_async()
            .await;

        let splits = MassiveClient::for_tests(&server.url())
            .fetch_splits()
            .await
            .expect("a successful fetch");

        mock.assert_async().await;
        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0].ticker().as_str(), "MNST");
        assert_eq!(
            splits[0].execution_date(),
            SessionDate::from_date(date("2026-07-06"))
        );
        assert_eq!((splits[0].split_from(), splits[0].split_to()), (1.0, 2.0));
        assert_eq!(
            (splits[1].split_from(), splits[1].split_to()),
            (3.0, 1.0),
            "a reverse split keeps its ratio the way round the feed reported it"
        );
        // Nineteen percent of the live feed looks like this. A whole-number ratio does not drop the
        // row, it fails the page, so the fetch returned nothing at all against the real endpoint.
        assert_eq!(
            (splits[2].split_from(), splits[2].split_to()),
            (1.0, 1.0056),
            "a fractional mutual-fund reallocation survives the parse"
        );
    }

    /// The whole table is 29 pages, so a fetch that stopped at the first would silently hold the
    /// most recent thousand splits and nothing older.
    #[tokio::test]
    async fn test_the_cursor_is_followed_to_the_last_page() {
        let mut server = mockito::Server::new_async().await;
        let cursor = format!("{}/v3/reference/splits?cursor=page-two", server.url());
        let first = server
            .mock("GET", "/v3/reference/splits")
            .match_query(mockito::Matcher::UrlEncoded("limit".into(), "1000".into()))
            .with_status(200)
            .with_body(format!(
                r#"{{"status":"OK","results":[{{"execution_date":"2026-07-06","id":"E1","split_from":1,"split_to":2,"ticker":"MNST"}}],"next_url":"{cursor}"}}"#
            ))
            .create_async()
            .await;
        // The cursor URL carries no credential of its own, so the second request must re-attach it.
        let second = server
            .mock("GET", "/v3/reference/splits")
            .match_query(mockito::Matcher::UrlEncoded("cursor".into(), "page-two".into()))
            .match_header("authorization", "Bearer test-key")
            .with_status(200)
            .with_body(
                r#"{"status":"OK","results":[{"execution_date":"2026-10-06","id":"E2","split_from":3,"split_to":1,"ticker":"ETHA"}]}"#,
            )
            .create_async()
            .await;

        let splits = MassiveClient::for_tests(&server.url())
            .fetch_splits()
            .await
            .expect("a successful fetch");

        first.assert_async().await;
        second.assert_async().await;
        assert_eq!(splits.len(), 2, "both pages reach the caller");
    }

    /// One unusable row must not cost the whole table, on the same terms the bar path drops a
    /// malformed instrument: a preferred's lowercase symbol, and a ratio that cannot be divided by.
    #[tokio::test]
    async fn test_unusable_split_rows_are_dropped_rather_than_failing_the_fetch() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v3/reference/splits")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                r#"{"status":"OK","results":[
                    {"execution_date":"2026-07-06","id":"E1","split_from":1,"split_to":2,"ticker":"MNST"},
                    {"execution_date":"2026-07-06","id":"E2","split_from":1,"split_to":2,"ticker":"GSpD"},
                    {"execution_date":"2026-07-06","id":"E3","split_from":0,"split_to":2,"ticker":"ZERO"}
                ]}"#,
            )
            .create_async()
            .await;

        let splits = MassiveClient::for_tests(&server.url())
            .fetch_splits()
            .await
            .expect("a bad row is dropped, not fatal");

        mock.assert_async().await;
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].ticker().as_str(), "MNST");
    }

    #[tokio::test]
    async fn test_a_failed_splits_request_is_an_error() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/v3/reference/splits")
            .match_query(mockito::Matcher::Any)
            .with_status(429)
            .with_body("slow down")
            .create_async()
            .await;

        let error = MassiveClient::for_tests(&server.url())
            .fetch_splits()
            .await
            .expect_err("a throttle is an error");

        mock.assert_async().await;
        assert!(
            matches!(error, MassiveError::Api { status: 429, .. }),
            "{error:?}"
        );
    }
}
