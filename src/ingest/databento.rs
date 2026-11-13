//! Databento's historical API. Rows are reachable only through `rows`, which prices the request against a budget first
//! and takes a session the dataset's condition reported available; they come back as served CSV, gzipped as they stream.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use super::retry::{Outcome, send, send_gzipped, with_retries};
use super::{FetchError, Secret, VariableRefusal, variable};
use crate::common::market::{Dollars, Symbol};
use crate::common::time::SessionDate;

const BASE_URL: &str = "https://hist.databento.com/v0";

/// A day of ten-level depth for the busiest names runs to gigabytes, so a body gets far longer than a metadata answer.
const ROWS_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Integer prices in billionths of a dollar and integer nanosecond instants, so no value passes through a float.
const ROW_FORMAT: [(&str, &str); 5] = [
    ("encoding", "csv"),
    ("compression", "none"),
    ("pretty_px", "false"),
    ("pretty_ts", "false"),
    ("map_symbols", "true"),
];

/// The served column holding Databento's receive instant, which is each row's `known_at`.
pub const KNOWN_AT_COLUMN: &str = "ts_recv";

/// A Databento dataset code such as `XNAS.ITCH`: upper-case letters and digits either side of one dot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Dataset(String);

impl TryFrom<String> for Dataset {
    type Error = DatasetRefusal;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::new(&raw)
    }
}

impl From<Dataset> for String {
    fn from(dataset: Dataset) -> Self {
        dataset.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DatasetRefusal {
    #[error("`{raw}` is not a dataset code")]
    Malformed { raw: String },
}

impl Dataset {
    pub fn new(raw: &str) -> Result<Self, DatasetRefusal> {
        let part = |text: &str| {
            !text.is_empty()
                && text
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        };
        match raw.split_once('.') {
            Some((venue, feed)) if part(venue) && part(feed) => Ok(Self(raw.to_string())),
            Some(_) | None => Err(DatasetRefusal::Malformed {
                raw: raw.to_string(),
            }),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Which book a request reads, named by the vendor's schema code.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
pub enum Schema {
    /// Every change to a venue's best bid and offer, and every trade with the book it met.
    #[strum(serialize = "mbp-1")]
    #[serde(rename = "mbp-1")]
    TopOfBook,
    /// Every trade on a venue with its best bid and offer just before the trade; a subset of `TopOfBook`'s rows.
    #[strum(serialize = "tbbo")]
    #[serde(rename = "tbbo")]
    TradeWithBook,
    /// Every change to a venue's ten best price levels.
    #[strum(serialize = "mbp-10")]
    #[serde(rename = "mbp-10")]
    TenLevels,
}

/// What a request would cost, or that none of its symbols resolved on the dataset over its range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priced {
    Cost(Dollars),
    Unresolved,
}

/// What a pull may still spend; every request is priced against it before it is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    remaining: Dollars,
}

impl Budget {
    pub fn new(ceiling: Dollars) -> Self {
        Self { remaining: ceiling }
    }

    pub fn remaining(self) -> Dollars {
        self.remaining
    }

    fn spend(&mut self, cost: Dollars) -> Result<(), PullRefusal> {
        let left = self.remaining.millionths().checked_sub(cost.millionths());
        match left {
            Some(left) => {
                self.remaining = Dollars::from_millionths(left);
                Ok(())
            }
            None => Err(PullRefusal::OverCeiling {
                cost,
                remaining: self.remaining,
            }),
        }
    }
}

/// Why a request for rows was not sent or not answered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PullRefusal {
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error("a request priced at ${cost} exceeds the ${remaining} left under the ceiling")]
    OverCeiling { cost: Dollars, remaining: Dollars },
}

/// What a priced request returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pulled {
    /// The CSV as served, gzipped, with what it was priced at.
    Rows { gzipped: Vec<u8>, cost: Dollars },
    /// The symbol did not resolve on the dataset that session, so nothing was asked for.
    Unresolved,
    /// Priced at zero, which is no rows, so nothing was asked for.
    Empty,
}

/// An ended session whose dataset condition Databento reported whole, as `split_by_condition` reads it; only
/// `available_sessions` makes one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableSession {
    dataset: Dataset,
    session: SessionDate,
}

impl AvailableSession {
    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    pub fn session(&self) -> SessionDate {
        self.session
    }
}

#[derive(Deserialize)]
struct ConditionRow {
    date: NaiveDate,
    condition: String,
}

pub struct Databento {
    http_client: reqwest::Client,
    key: Secret,
}

impl Databento {
    pub fn from_environment(http_client: reqwest::Client) -> Result<Self, VariableRefusal> {
        Ok(Self {
            http_client,
            key: Secret::new(variable("DATABENTO_API_KEY")?),
        })
    }

    fn post(&self, endpoint: &str, form: &[(&str, String)]) -> reqwest::RequestBuilder {
        self.http_client
            .post(format!("{BASE_URL}/{endpoint}"))
            .basic_auth(self.key.expose(), None::<&str>)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(encode_form(form))
    }

    /// The `sessions` the dataset holds whole as of `now`, and apart from them the sessions it does not.
    pub async fn available_sessions(
        &self,
        dataset: &Dataset,
        sessions: &[SessionDate],
        now: DateTime<Utc>,
    ) -> Result<(Vec<AvailableSession>, Vec<SessionDate>), FetchError> {
        let (Some(first), Some(last)) = (sessions.iter().min(), sessions.iter().max()) else {
            return Ok((Vec::new(), Vec::new()));
        };
        let query = [
            ("dataset", dataset.as_str().to_string()),
            ("start_date", first.to_string()),
            ("end_date", last.plus_calendar_days(1).to_string()),
        ];
        let url = format!("{BASE_URL}/metadata.get_dataset_condition");
        let body = with_retries(|| {
            send(
                self.http_client
                    .get(&url)
                    .basic_auth(self.key.expose(), None::<&str>)
                    .query(&query),
            )
        })
        .await?;
        let rows: Vec<ConditionRow> = serde_json::from_slice(&body).map_err(malformed)?;
        let (available, unavailable) = split_by_condition(&rows, sessions, now);
        let available = available
            .into_iter()
            .map(|session| AvailableSession {
                dataset: dataset.clone(),
                session,
            })
            .collect();
        Ok((available, unavailable))
    }

    /// What a request for `symbols` over `[start, end)` would cost; free, and asked before every request for rows.
    pub async fn cost(
        &self,
        dataset: &Dataset,
        schema: Schema,
        symbols: &[Symbol],
        (start, end): (DateTime<Utc>, DateTime<Utc>),
    ) -> Result<Priced, FetchError> {
        let form = request_form(dataset, schema, symbols, start, end);
        match with_retries(|| send(self.post("metadata.get_cost", &form))).await {
            Ok(body) => parse_cost(&body).map(Priced::Cost),
            Err(FetchError::Refused { status: 422, body })
                if body.contains("symbology_invalid_request") =>
            {
                Ok(Priced::Unresolved)
            }
            Err(error) => Err(error),
        }
    }

    /// One symbol's rows over one available session, priced first and refused when the price exceeds what the budget
    /// has left. Sent once and charged before it is sent, since a retried transfer may be billed again; a failed one
    /// still counts against the budget, and a rerun picks the session up.
    pub async fn rows(
        &self,
        available: &AvailableSession,
        schema: Schema,
        symbol: &Symbol,
        budget: &mut Budget,
    ) -> Result<Pulled, PullRefusal> {
        let bounds = available.session.bounds();
        let symbols = std::slice::from_ref(symbol);
        let cost = match self
            .cost(&available.dataset, schema, symbols, bounds)
            .await?
        {
            Priced::Unresolved => return Ok(Pulled::Unresolved),
            Priced::Cost(cost) if cost.millionths() == 0 => return Ok(Pulled::Empty),
            Priced::Cost(cost) => cost,
        };
        budget.spend(cost)?;
        let mut form = request_form(&available.dataset, schema, symbols, bounds.0, bounds.1);
        form.extend(ROW_FORMAT.map(|(name, value)| (name, value.to_string())));
        match send_gzipped(self.post("timeseries.get_range", &form), ROWS_TIMEOUT).await {
            Outcome::Body(gzipped) => Ok(Pulled::Rows { gzipped, cost }),
            Outcome::Refused { status, body } => {
                Err(PullRefusal::Fetch(FetchError::Refused { status, body }))
            }
            Outcome::Transient(last) => Err(PullRefusal::Fetch(FetchError::Exhausted {
                attempts: 1,
                last,
            })),
        }
    }
}

fn request_form(
    dataset: &Dataset,
    schema: Schema,
    symbols: &[Symbol],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Vec<(&'static str, String)> {
    let symbols: Vec<&str> = symbols.iter().map(Symbol::as_str).collect();
    vec![
        ("dataset", dataset.as_str().to_string()),
        ("schema", schema.to_string()),
        ("symbols", symbols.join(",")),
        ("stype_in", "raw_symbol".to_string()),
        ("start", start.to_rfc3339_opts(SecondsFormat::Secs, true)),
        ("end", end.to_rfc3339_opts(SecondsFormat::Secs, true)),
    ]
}

/// A form body, percent-encoded as a URL query is.
fn encode_form(form: &[(&str, String)]) -> String {
    let mut url = reqwest::Url::parse("https://form.invalid/").expect("a constant URL parses");
    url.query_pairs_mut()
        .extend_pairs(form.iter().map(|(name, value)| (*name, value.as_str())));
    url.query().unwrap_or_default().to_string()
}

/// A served decimal dollar amount, rounded up to the millionth so the ceiling is never undercounted.
fn parse_cost(body: &[u8]) -> Result<Dollars, FetchError> {
    let text = std::str::from_utf8(body).map_err(malformed)?.trim();
    let refused = || malformed(format!("`{text}` is not a cost"));
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) {
        return Err(refused());
    }
    let (kept, dropped) = fraction.split_at(fraction.len().min(6));
    let round_up = u64::from(dropped.bytes().any(|byte| byte != b'0'));
    whole
        .parse::<u64>()
        .ok()
        .and_then(|whole| whole.checked_mul(1_000_000))
        .zip(format!("{kept:0<6}").parse::<u64>().ok())
        .and_then(|(whole, kept)| whole.checked_add(kept)?.checked_add(round_up))
        .map(Dollars::from_millionths)
        .ok_or_else(refused)
}

/// A session is whole when it has ended by `now`, its dataset reported its Eastern date `available`, and reported the
/// next UTC day, into which the evening session spills, as `available` or not at all, since the report leaves out a day
/// with no data. The report calls a day still in progress `available`, so the end is checked against the clock.
fn split_by_condition(
    rows: &[ConditionRow],
    sessions: &[SessionDate],
    now: DateTime<Utc>,
) -> (Vec<SessionDate>, Vec<SessionDate>) {
    let reported: BTreeMap<NaiveDate, bool> = rows
        .iter()
        .map(|row| (row.date, row.condition == "available"))
        .collect();
    sessions.iter().copied().partition(|session| {
        let date = session.date();
        let next = date.succ_opt().and_then(|next| reported.get(&next));
        session.bounds().1 <= now
            && reported.get(&date) == Some(&true)
            && next.is_none_or(|available| *available)
    })
}

fn malformed(reason: impl std::fmt::Display) -> FetchError {
    FetchError::Malformed {
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn session(text: &str) -> SessionDate {
        SessionDate::from_date(text.parse().unwrap())
    }

    fn row(date: &str, condition: &str) -> ConditionRow {
        ConditionRow {
            date: date.parse().unwrap(),
            condition: condition.to_string(),
        }
    }

    /// The payload shape `get_cost` served on 2026-11-13: twelve decimal places, a dollar amount in plain text.
    #[test]
    fn test_a_cost_rounds_up_to_the_millionth() {
        let read = |body: &str| parse_cost(body.as_bytes()).map(Dollars::millionths);
        assert_eq!(read("0.051643252373"), Ok(51_644));
        assert_eq!(read("0.000000000000"), Ok(0));
        assert_eq!(read("0.000002980232"), Ok(3));
        assert_eq!(read("12.500000000000\n"), Ok(12_500_000));
        assert_eq!(read("7"), Ok(7_000_000));
        for body in ["", "-0.5", ".5", "1e-5", "{\"detail\":1}", "1.2.3"] {
            assert!(read(body).is_err(), "{body:?} read as a cost");
        }
    }

    proptest! {
        /// A served amount reads to the least millionth not below it: exact when it has six places, one more when a
        /// finer digit is set.
        #[test]
        fn test_a_cost_is_the_least_millionth_not_below_it(
            whole in 0u64..1_000_000,
            kept in 0u64..1_000_000,
            finer in 0u64..1_000_000,
        ) {
            let body = format!("{whole}.{kept:06}{finer:06}");
            let exact = whole * 1_000_000 + kept;
            let expected = exact + u64::from(finer > 0);
            prop_assert_eq!(parse_cost(body.as_bytes()).map(Dollars::millionths), Ok(expected));
        }
    }

    /// The budget refuses a request it cannot cover and is left unchanged, and spends exactly to zero.
    #[test]
    fn test_a_budget_refuses_what_it_cannot_cover() {
        let mut budget = Budget::new(Dollars::from_millionths(5_000_000));
        assert_eq!(budget.spend(Dollars::from_millionths(3_000_000)), Ok(()));
        assert_eq!(
            budget.spend(Dollars::from_millionths(2_000_001)),
            Err(PullRefusal::OverCeiling {
                cost: Dollars::from_millionths(2_000_001),
                remaining: Dollars::from_millionths(2_000_000),
            })
        );
        assert_eq!(budget.remaining().millionths(), 2_000_000);
        assert_eq!(budget.spend(Dollars::from_millionths(2_000_000)), Ok(()));
        assert_eq!(budget.remaining().millionths(), 0);
    }

    /// The shape `get_dataset_condition` served on 2026-11-13. A degraded or pending next UTC day keeps a session out,
    /// while a next day left out of the report, as most Saturdays are, does not; a session whose own date is degraded or
    /// left out is kept out.
    #[test]
    fn test_a_session_needs_its_date_and_no_bad_next_utc_day() {
        let rows = [
            row("2026-11-05", "available"),
            row("2026-11-06", "available"),
            row("2026-11-09", "available"),
            row("2026-11-10", "degraded"),
            row("2026-11-11", "available"),
            row("2026-11-12", "available"),
            row("2026-11-13", "pending"),
        ];
        let sessions = [
            "2026-11-04",
            "2026-11-05",
            "2026-11-06",
            "2026-11-09",
            "2026-11-10",
            "2026-11-11",
            "2026-11-12",
        ]
        .map(session);
        let after = session("2026-11-14").midnight();
        let (available, unavailable) = split_by_condition(&rows, &sessions, after);
        assert_eq!(
            available,
            ["2026-11-05", "2026-11-06", "2026-11-11"].map(session)
        );
        assert_eq!(
            unavailable,
            ["2026-11-04", "2026-11-09", "2026-11-10", "2026-11-12"].map(session)
        );
    }

    /// On 2026-11-13 at 21:53 UTC the report already called that day `available`, so a session still running is kept
    /// out until its Eastern day ends.
    #[test]
    fn test_a_session_still_running_is_not_whole() {
        let rows = [
            row("2026-11-12", "available"),
            row("2026-11-13", "available"),
        ];
        let running = [session("2026-11-13")];
        let end = session("2026-11-14").midnight();
        let before = end - chrono::TimeDelta::seconds(1);
        assert_eq!(
            split_by_condition(&rows, &running, before),
            (Vec::new(), running.to_vec())
        );
        assert_eq!(
            split_by_condition(&rows, &running, end),
            (running.to_vec(), Vec::new())
        );
    }

    #[test]
    fn test_a_dataset_code_is_two_upper_case_parts() {
        assert!(Dataset::new("XNAS.ITCH").is_ok());
        assert!(Dataset::new("EQUS.MINI").is_ok());
        for raw in [
            "XNAS",
            "xnas.itch",
            "XNAS.",
            ".ITCH",
            "XNAS.ITCH.X",
            "XNAS/ITCH",
            "",
        ] {
            assert!(Dataset::new(raw).is_err(), "{raw:?} read as a dataset");
        }
    }

    /// The schema codes the vendor names, written and read back.
    #[test]
    fn test_a_schema_round_trips_through_its_vendor_code() {
        use strum::IntoEnumIterator;
        let codes: Vec<String> = Schema::iter().map(|schema| schema.to_string()).collect();
        assert_eq!(codes, ["mbp-1", "tbbo", "mbp-10"]);
        for code in codes {
            let schema = code.parse::<Schema>().unwrap();
            assert_eq!(schema.to_string(), code);
            assert_eq!(
                serde_json::to_string(&schema).unwrap(),
                format!("\"{code}\"")
            );
            assert_eq!(
                serde_json::from_str::<Schema>(&format!("\"{code}\"")).unwrap(),
                schema
            );
        }
    }

    /// A dataset code is written as its text and refused on the way back in when malformed.
    #[test]
    fn test_a_dataset_round_trips_through_json_and_refuses_a_malformed_code() {
        let dataset = Dataset::new("XNAS.ITCH").unwrap();
        assert_eq!(serde_json::to_string(&dataset).unwrap(), "\"XNAS.ITCH\"");
        assert_eq!(
            serde_json::from_str::<Dataset>("\"XNAS.ITCH\"").unwrap(),
            dataset
        );
        assert!(serde_json::from_str::<Dataset>("\"xnas.itch\"").is_err());
    }

    /// The form a request sends, which `get_cost` accepted on 2026-11-13; the colons and commas are percent-encoded.
    #[test]
    fn test_a_request_form_encodes_its_symbols_and_instants() {
        let start = session("2026-11-10").bounds();
        let symbols = [Symbol::new("AAPL").unwrap(), Symbol::new("BRK.B").unwrap()];
        let form = request_form(
            &Dataset::new("XNAS.ITCH").unwrap(),
            Schema::TopOfBook,
            &symbols,
            start.0,
            start.1,
        );
        assert_eq!(
            encode_form(&form),
            "dataset=XNAS.ITCH&schema=mbp-1&symbols=AAPL%2CBRK.B&stype_in=raw_symbol\
             &start=2026-11-10T05%3A00%3A00Z&end=2026-11-11T05%3A00%3A00Z"
        );
    }

    /// Prices one live request and reads one dataset's conditions, both free; never fetches rows.
    #[tokio::test]
    #[ignore = "reaches Databento's metadata endpoints"]
    async fn live_databento_prices_a_request_and_reports_conditions() {
        let client = Databento::from_environment(reqwest::Client::new()).unwrap();
        let dataset = Dataset::new("XNAS.ITCH").unwrap();
        let day = session("2026-11-10");
        let priced = client
            .cost(
                &dataset,
                Schema::TopOfBook,
                &[Symbol::new("AAPL").unwrap()],
                day.bounds(),
            )
            .await
            .unwrap();
        assert!(matches!(priced, Priced::Cost(cost) if cost.millionths() > 0));
        let unresolved = client
            .cost(
                &dataset,
                Schema::TopOfBook,
                &[Symbol::new("ZZZZQ").unwrap()],
                day.bounds(),
            )
            .await
            .unwrap();
        assert_eq!(unresolved, Priced::Unresolved);
        let (available, unavailable) = client
            .available_sessions(&dataset, &[day], Utc::now())
            .await
            .unwrap();
        assert_eq!((available.len(), unavailable.len()), (1, 0));
    }
}
