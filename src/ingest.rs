//! Vendor clients: the only place a vendor's payload or notation appears. Each maps what it fetches into `common`
//! records and names every row it refused.

pub mod alpaca;
pub mod flat_files;
pub mod massive;
pub(crate) mod retry;

pub use retry::FetchError;

use std::collections::{BTreeMap, BTreeSet};

pub use crate::common::market::refusal::{RowRefusal, RowRefusalKind};

use crate::common::market::record::Bar;
use crate::common::monoid::Tally;

/// Why an environment variable a client needs was not used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableRefusal {
    Missing {
        name: &'static str,
    },
    /// Set, but not one of the few values it may take.
    Malformed {
        name: &'static str,
        raw: String,
    },
    /// Set, but not Unicode; the value is left out, since the variable may hold a secret.
    NotUnicode {
        name: &'static str,
        bytes: usize,
    },
}

impl std::fmt::Display for VariableRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing { name } => write!(formatter, "{name} is not set"),
            Self::Malformed { name, raw } => write!(formatter, "{name} is `{raw}`"),
            Self::NotUnicode { name, bytes } => {
                write!(formatter, "{name} holds {bytes} bytes that are not Unicode")
            }
        }
    }
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
