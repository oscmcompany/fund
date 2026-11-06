//! Vendor clients: the only place a vendor's payload or notation appears. Each maps what it fetches into `common`
//! records and names every row it refused.

pub mod alpaca;
pub mod flat_files;
pub mod massive;
pub(crate) mod retry;
pub mod sec;

pub use retry::FetchError;

use std::collections::{BTreeMap, BTreeSet};

pub use crate::common::market::refusal::{RowRefusal, RowRefusalKind};

use chrono::{DateTime, Utc};

use crate::common::market::record::Bar;
use crate::common::market::{Price, Shares, Symbol};
use crate::common::monoid::Tally;

/// Why an environment variable a client needs was not used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VariableRefusal {
    #[error("{name} is not set")]
    Missing { name: &'static str },
    /// Set, but not one of the few values it may take.
    #[error("{name} is `{raw}`")]
    Malformed { name: &'static str, raw: String },
    /// Set, but not Unicode; the value is left out, since the variable may hold a secret.
    #[error("{name} holds {bytes} bytes that are not Unicode")]
    NotUnicode { name: &'static str, bytes: usize },
}

pub(crate) fn variable(name: &'static str) -> Result<String, VariableRefusal> {
    std::env::var(name).map_err(|error| variable_refusal(name, error))
}

fn variable_refusal(name: &'static str, error: std::env::VarError) -> VariableRefusal {
    match error {
        std::env::VarError::NotPresent => VariableRefusal::Missing { name },
        std::env::VarError::NotUnicode(raw) => VariableRefusal::NotUnicode {
            name,
            bytes: raw.len(),
        },
    }
}

/// A key a vendor authenticates with; its `Debug` prints none of it.
pub(crate) struct Secret(String);

impl Secret {
    pub(crate) fn new(key: String) -> Self {
        Self(key)
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Secret(..)")
    }
}

/// What a fetch read, or why it failed, beside every response body it received, so the bodies can be stored as served
/// even when reading them failed.
#[derive(Debug)]
pub struct Fetched<T> {
    value: Result<T, FetchError>,
    bodies: Vec<Vec<u8>>,
}

impl<T> Fetched<T> {
    pub fn new(value: Result<T, FetchError>, bodies: Vec<Vec<u8>>) -> Self {
        Self { value, bodies }
    }

    pub fn into_parts(self) -> (Result<T, FetchError>, Vec<Vec<u8>>) {
        (self.value, self.bodies)
    }
}

/// A vendor row that did not become a record, named as the vendor wrote it.
#[derive(Debug, Clone, PartialEq)]
pub struct RefusedRow {
    ticker: String,
    cause: RowRefusal,
}

impl RefusedRow {
    #[cfg(test)]
    pub(crate) fn new(ticker: &str, cause: RowRefusal) -> Self {
        Self {
            ticker: ticker.to_string(),
            cause,
        }
    }

    pub fn ticker(&self) -> &str {
        &self.ticker
    }

    pub fn cause(&self) -> &RowRefusal {
        &self.cause
    }
}

/// Why a raw file was not read at all.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseRefusal {
    /// A gzip, CSV or JSON error, with the line it stopped on where the reader knows it.
    #[error("malformed at line {line:?}: {reason}")]
    Malformed { line: Option<u64>, reason: String },
}

/// Where and when a print was reported, in the vendor's own codes; no fold reads it, so research can.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrintOrigin {
    /// Absent where the vendor served none.
    pub exchange: Option<String>,
    pub tape: String,
    /// Absent where the vendor served none.
    pub id: Option<String>,
    /// When the reporting venue stamped the print; Alpaca serves none.
    pub participant_timestamp: Option<DateTime<Utc>>,
    /// When a trade reporting facility stamped the print; Alpaca serves none.
    pub trade_reporting_facility_timestamp: Option<DateTime<Utc>>,
    /// Massive's sequence number, increasing within a ticker's session; Alpaca serves none.
    pub sequence: Option<u64>,
}

/// Where and when a quote was published, in the vendor's own codes; no fold reads it, so research can.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteOrigin {
    /// Each absent where the vendor served none.
    pub bid_exchange: Option<String>,
    pub ask_exchange: Option<String>,
    pub tape: Option<String>,
    pub conditions: Vec<String>,
    /// Massive's quote indicators; Alpaca serves none.
    pub indicators: Vec<String>,
    /// When the quoting venue stamped the quote; Alpaca serves none.
    pub participant_timestamp: Option<DateTime<Utc>>,
    /// Massive's sequence number, increasing within a ticker's session; Alpaca serves none.
    pub sequence: Option<u64>,
}

/// Which side of a one-sided quote is priced; both priced is a two-sided quote, so it is no variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PricedSide {
    Neither,
    Bid(Price, Shares),
    Ask(Price, Shares),
}

/// A quote with one side or both unpriced, which is no top of book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneSidedQuote {
    pub symbol: Symbol,
    pub timestamp: DateTime<Utc>,
    pub priced: PricedSide,
}

/// The priced side of a quote `one_sided` accepts, none for any other quote; the priced side is refused when its
/// price or size is.
pub(crate) fn priced_side(
    (bid_price, bid_size): (f64, f64),
    (ask_price, ask_size): (f64, f64),
) -> Option<Result<PricedSide, RowRefusal>> {
    if !one_sided(bid_price, ask_price) {
        return None;
    }
    let priced = |price: f64, size: f64| {
        Ok::<_, RowRefusal>((
            Price::from_dollars(price).map_err(RowRefusal::Price)?,
            Shares::from_float(size).map_err(RowRefusal::Shares)?,
        ))
    };
    match (bid_price == 0.0, ask_price == 0.0) {
        (true, true) => Some(Ok(PricedSide::Neither)),
        (false, true) => {
            Some(priced(bid_price, bid_size).map(|(price, size)| PricedSide::Bid(price, size)))
        }
        (true, false) => {
            Some(priced(ask_price, ask_size).map(|(price, size)| PricedSide::Ask(price, size)))
        }
        (false, false) => None,
    }
}

/// Refused rows counted by the kind of their cause.
pub fn refused_by_cause(rows: &[RefusedRow]) -> Tally<RowRefusalKind> {
    let mut tally = Tally::default();
    for row in rows {
        tally.add(row.cause().kind());
    }
    tally
}

/// One session's bars from a vendor's report, with every row that did not become one: each row is exactly one of a bar,
/// a test ticker or a refusal.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionBars {
    bars: Vec<Bar>,
    test_tickers: Vec<String>,
    refused: Vec<RefusedRow>,
}

impl SessionBars {
    fn new<Key: Ord + Clone>(accepted: Accepted<Key>, test_tickers: Vec<String>) -> Self {
        let (bars, refused) = accepted.finish();
        Self {
            bars,
            test_tickers,
            refused,
        }
    }

    /// In symbol, then timestamp, order.
    pub fn bars(&self) -> &[Bar] {
        &self.bars
    }

    pub fn into_bars(self) -> Vec<Bar> {
        self.bars
    }

    pub fn test_tickers(&self) -> &[String] {
        &self.test_tickers
    }

    pub fn refused(&self) -> &[RefusedRow] {
        &self.refused
    }
}

/// Collects a report's bars by the record each claims to be, so a key claimed twice keeps neither row.
struct Accepted<Key> {
    rows: BTreeMap<Key, (String, Bar)>,
    duplicated: BTreeSet<Key>,
    refused: Vec<RefusedRow>,
}

impl<Key: Ord + Clone> Accepted<Key> {
    fn new() -> Self {
        Self {
            rows: BTreeMap::new(),
            duplicated: BTreeSet::new(),
            refused: Vec::new(),
        }
    }

    fn offer(&mut self, key: Key, ticker: String, bar: Bar) {
        if self.duplicated.contains(&key) {
            self.refuse(ticker, RowRefusal::Duplicate);
        } else if let Some((first, _)) = self.rows.remove(&key) {
            self.duplicated.insert(key);
            self.refuse(first, RowRefusal::Duplicate);
            self.refuse(ticker, RowRefusal::Duplicate);
        } else {
            self.rows.insert(key, (ticker, bar));
        }
    }

    fn refuse(&mut self, ticker: String, cause: RowRefusal) {
        self.refused.push(RefusedRow { ticker, cause });
    }

    /// The bars in key order, and every refusal in the order it was made.
    fn finish(self) -> (Vec<Bar>, Vec<RefusedRow>) {
        let bars = self.rows.into_values().map(|(_, bar)| bar).collect();
        (bars, self.refused)
    }
}

/// A quote with a side priced at zero and no bad price, which is no top of book; a negative or non-finite price on
/// either side is a bad price and refused as one.
pub(crate) fn one_sided(bid: f64, ask: f64) -> bool {
    let priced = |price: f64| price.is_finite() && price >= 0.0;
    priced(bid) && priced(ask) && (bid == 0.0 || ask == 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each one-sided shape keeps only its priced side, whatever size the unpriced side carries; a priced side with a
    /// bad price or size is refused, and a two-sided quote is not one-sided at all.
    #[test]
    fn test_a_one_sided_quote_keeps_exactly_its_priced_side() {
        let side = |ticks, units| (Price::from_ticks(ticks).unwrap(), Shares::from_units(units));
        let read =
            |bid, ask| priced_side(bid, ask).map(|priced| priced.map_err(|cause| cause.kind()));
        assert_eq!(
            read((0.0, 300.0), (0.0, 0.0)),
            Some(Ok(PricedSide::Neither))
        );
        let (price, size) = side(10_500_000, 100_000_000);
        assert_eq!(
            read((10.5, 100.0), (0.0, 700.0)),
            Some(Ok(PricedSide::Bid(price, size)))
        );
        let (price, size) = side(50_270_000, 200_000_000);
        assert_eq!(
            read((0.0, 900.0), (50.27, 200.0)),
            Some(Ok(PricedSide::Ask(price, size)))
        );
        assert_eq!(
            read((0.0, 0.0), (50.27, -1.0)),
            Some(Err(RowRefusalKind::Shares))
        );
        assert_eq!(
            read((20_000_000.0, 100.0), (0.0, 0.0)),
            Some(Err(RowRefusalKind::Price))
        );
        assert_eq!(read((50.25, 100.0), (50.27, 200.0)), None);
    }

    /// A variable set to bytes that are not Unicode is set, not missing, and is refused without its value, which may
    /// be a secret.
    #[cfg(unix)]
    #[test]
    fn test_a_variable_that_is_not_unicode_is_refused_without_its_value() {
        use std::os::unix::ffi::OsStringExt;
        let raw = std::ffi::OsString::from_vec(b"paper\xff".to_vec());
        assert_eq!(
            variable_refusal("ALPACA_IS_PAPER", std::env::VarError::NotUnicode(raw)),
            VariableRefusal::NotUnicode {
                name: "ALPACA_IS_PAPER",
                bytes: 6,
            }
        );
        assert_eq!(
            variable_refusal("ALPACA_IS_PAPER", std::env::VarError::NotPresent),
            VariableRefusal::Missing {
                name: "ALPACA_IS_PAPER"
            }
        );
    }

    /// Only a zero side beside a good price is one-sided; a bad price on either side is left for the price refusal.
    #[test]
    fn test_a_quote_is_one_sided_only_beside_a_good_price() {
        let read: Vec<bool> = [
            (0.0, 10.0),
            (10.0, 0.0),
            (0.0, 0.0),
            (10.0, 10.01),
            (-1.0, 0.0),
            (0.0, -1.0),
            (0.0, f64::NAN),
            (f64::INFINITY, 0.0),
        ]
        .into_iter()
        .map(|(bid, ask)| one_sided(bid, ask))
        .collect();
        assert_eq!(read, [true, true, true, false, false, false, false, false]);
    }
}
