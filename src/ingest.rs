//! Vendor clients: the only place a vendor's payload or notation appears. Each maps what it fetches into `common`
//! records and names every row it refused.

pub mod alpaca;
pub mod massive;
mod retry;

pub use retry::FetchError;

use crate::common::market::record::{BarRefusal, OhlcRefusal};
use crate::common::market::{DollarVolumeRefusal, PriceRefusal, SharesRefusal, SymbolRefusal};

/// An environment variable a client needs and did not find.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingVariable {
    pub name: &'static str,
}

fn variable(name: &'static str) -> Result<String, MissingVariable> {
    std::env::var(name).map_err(|_| MissingVariable { name })
}

/// A vendor row that did not become a record, named as the vendor wrote it.
#[derive(Debug, Clone, PartialEq)]
pub struct RefusedRow {
    pub ticker: String,
    pub cause: RowRefusal,
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
}
