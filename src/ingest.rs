//! Vendor clients: the only place a vendor's payload or notation appears. Each maps what it fetches into `common`
//! records and names every row it refused.

pub mod alpaca;
pub mod massive;
mod retry;

pub use retry::FetchError;

use std::collections::{BTreeMap, BTreeSet};

use crate::common::market::record::{Bar, BarRefusal, OhlcRefusal};
use crate::common::market::{DollarVolumeRefusal, PriceRefusal, SharesRefusal, SymbolRefusal};

/// An environment variable a client needs and did not find.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingVariable {
    name: &'static str,
}

impl MissingVariable {
    pub fn name(&self) -> &'static str {
        self.name
    }
}

pub(crate) fn variable(name: &'static str) -> Result<String, MissingVariable> {
    std::env::var(name).map_err(|_| MissingVariable { name })
}

/// A vendor row that did not become a record, named as the vendor wrote it.
#[derive(Debug, Clone, PartialEq)]
pub struct RefusedRow {
    ticker: String,
    cause: RowRefusal,
}

impl RefusedRow {
    pub fn ticker(&self) -> &str {
        &self.ticker
    }

    pub fn cause(&self) -> &RowRefusal {
        &self.cause
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RowRefusal {
    Symbol(SymbolRefusal),
    /// Stamped for a session other than the one requested.
    Session {
        timestamp: String,
    },
    Price(PriceRefusal),
    Prices(OhlcRefusal),
    Shares(SharesRefusal),
    DollarVolume(DollarVolumeRefusal),
    Bar(BarRefusal),
    /// Answered for a symbol that was not asked for, as when a vendor normalizes a name into another security's.
    Unrequested,
    /// One of several rows claiming the same record; none is kept, since nothing says which is true.
    Duplicate,
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
