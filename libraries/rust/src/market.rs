//! Raw ingest record types from market data providers (Alpaca, Massive).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::FromRow;

/// A normalized US equity ticker symbol.
///
/// Enforces the Alpaca US equity ticker format: 1–5 uppercase ASCII letters for
/// the base symbol, with an optional dot-separated suffix of 1–3 uppercase ASCII
/// letters for share class or warrant notation (e.g. `BRK.B`, `BRK.WS`).
///
/// The private field prevents construction without going through [`Ticker::new`],
/// which trims, uppercases, and validates the raw input. A `Ticker` in scope is
/// proof that the symbol passed format validation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, sqlx::Type)]
#[sqlx(transparent)]
pub struct Ticker(String);

impl Ticker {
    /// Constructs a `Ticker` from a raw string.
    ///
    /// Trims surrounding whitespace, uppercases, then validates the result against
    /// the US equity ticker format. Returns `None` if the normalized value does not
    /// match.
    pub fn new(raw: &str) -> Option<Self> {
        let normalized = raw.trim().to_ascii_uppercase();
        if is_valid_ticker_format(&normalized) {
            Some(Self(normalized))
        } else {
            None
        }
    }

    /// Returns the normalized ticker string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Ticker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl PartialEq<str> for Ticker {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Ticker {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_str() == *other
    }
}

impl PartialEq<String> for Ticker {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl<'de> Deserialize<'de> for Ticker {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ticker::new(&raw)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid ticker: {}", raw)))
    }
}

fn is_valid_ticker_format(normalized: &str) -> bool {
    match normalized.split_once('.') {
        Some((base, suffix)) => is_valid_base(base) && is_valid_suffix(suffix),
        None => is_valid_base(normalized),
    }
}

fn is_valid_base(s: &str) -> bool {
    !s.is_empty() && s.len() <= 5 && s.chars().all(|c| c.is_ascii_uppercase())
}

fn is_valid_suffix(s: &str) -> bool {
    !s.is_empty() && s.len() <= 3 && s.chars().all(|c| c.is_ascii_uppercase())
}

/// Daily OHLCV equity bar record.
///
/// Timestamps are stored as `TIMESTAMPTZ` in PostgreSQL. The `inserted_at` field
/// is set by the caller at ingest time and explicitly bound in the upsert query.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityBar {
    pub ticker: Ticker,
    /// UTC timestamp for the trading day this bar covers.
    pub timestamp: DateTime<Utc>,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    /// Whole share units. Fractional values from the source API are rounded on ingest.
    pub volume: i64,
    pub volume_weighted_average_price: Option<f64>,
    pub transactions: Option<i64>,
    /// Set by the database at insert time.
    pub inserted_at: DateTime<Utc>,
}

/// Intraday bid/ask quote record from the Alpaca WebSocket stream.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityQuote {
    pub timestamp: DateTime<Utc>,
    pub ticker: Ticker,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: i32,
    pub ask_size: i32,
}

/// Ticker metadata record seeded from the S3 details CSV.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityDetails {
    pub ticker: Ticker,
    pub sector: String,
    pub industry: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_ticker_new_valid_simple() {
        let ticker = Ticker::new("AAPL").unwrap();
        assert_eq!(ticker.as_str(), "AAPL");
    }

    #[test]
    fn test_ticker_new_valid_class_share() {
        let ticker = Ticker::new("BRK.B").unwrap();
        assert_eq!(ticker.as_str(), "BRK.B");
    }

    #[test]
    fn test_ticker_new_valid_warrant_suffix() {
        let ticker = Ticker::new("BRK.WS").unwrap();
        assert_eq!(ticker.as_str(), "BRK.WS");
    }

    #[test]
    fn test_ticker_new_normalizes_lowercase() {
        let ticker = Ticker::new("aapl").unwrap();
        assert_eq!(ticker.as_str(), "AAPL");
    }

    #[test]
    fn test_ticker_new_normalizes_whitespace() {
        let ticker = Ticker::new("  AAPL  ").unwrap();
        assert_eq!(ticker.as_str(), "AAPL");
    }

    #[test]
    fn test_ticker_new_valid_max_base_length() {
        let ticker = Ticker::new("ABCDE").unwrap();
        assert_eq!(ticker.as_str(), "ABCDE");
    }

    #[test]
    fn test_ticker_new_valid_max_suffix_length() {
        let ticker = Ticker::new("A.WSD").unwrap();
        assert_eq!(ticker.as_str(), "A.WSD");
    }

    #[test]
    fn test_ticker_new_rejects_empty() {
        assert!(Ticker::new("").is_none());
    }

    #[test]
    fn test_ticker_new_rejects_whitespace_only() {
        assert!(Ticker::new("   ").is_none());
    }

    #[test]
    fn test_ticker_new_rejects_base_too_long() {
        assert!(Ticker::new("ABCDEF").is_none());
    }

    #[test]
    fn test_ticker_new_rejects_suffix_too_long() {
        assert!(Ticker::new("BRK.ABCD").is_none());
    }

    #[test]
    fn test_ticker_new_rejects_empty_suffix() {
        assert!(Ticker::new("BRK.").is_none());
    }

    #[test]
    fn test_ticker_new_rejects_empty_base() {
        assert!(Ticker::new(".B").is_none());
    }

    #[test]
    fn test_ticker_new_rejects_numbers_in_base() {
        assert!(Ticker::new("A1B").is_none());
    }

    #[test]
    fn test_ticker_new_rejects_multiple_dots() {
        assert!(Ticker::new("A.B.C").is_none());
    }

    #[test]
    fn test_ticker_display() {
        let ticker = Ticker::new("AAPL").unwrap();
        assert_eq!(format!("{}", ticker), "AAPL");
    }

    #[test]
    fn test_ticker_partial_eq_str_ref() {
        let ticker = Ticker::new("AAPL").unwrap();
        assert_eq!(ticker, "AAPL");
    }

    #[test]
    fn test_ticker_partial_eq_string() {
        let ticker = Ticker::new("AAPL").unwrap();
        assert_eq!(ticker, String::from("AAPL"));
    }

    #[test]
    fn test_ticker_hash_and_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Ticker::new("AAPL").unwrap());
        set.insert(Ticker::new("AAPL").unwrap());
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_equity_bar_construction_with_all_fields() {
        let now = Utc::now();
        let bar = EquityBar {
            ticker: Ticker::new("AAPL").unwrap(),
            timestamp: now,
            open_price: 150.0,
            high_price: 155.0,
            low_price: 149.0,
            close_price: 153.0,
            volume: 1_000_000,
            volume_weighted_average_price: Some(152.0),
            transactions: Some(50_000),
            inserted_at: now,
        };
        assert_eq!(bar.ticker.as_str(), "AAPL");
        assert_eq!(bar.open_price, 150.0);
        assert_eq!(bar.volume, 1_000_000);
    }

    #[test]
    fn test_equity_bar_clone() {
        let now = Utc::now();
        let bar = EquityBar {
            ticker: Ticker::new("GOOG").unwrap(),
            timestamp: now,
            open_price: 100.0,
            high_price: 105.0,
            low_price: 99.0,
            close_price: 103.0,
            volume: 500_000,
            volume_weighted_average_price: Some(102.0),
            transactions: Some(25_000),
            inserted_at: now,
        };
        let cloned = bar.clone();
        assert_eq!(cloned.ticker.as_str(), "GOOG");
        assert_eq!(cloned.close_price, 103.0);
    }

    #[test]
    fn test_equity_quote_construction() {
        let quote = EquityQuote {
            timestamp: Utc::now(),
            ticker: Ticker::new("AAPL").unwrap(),
            bid_price: 150.50,
            ask_price: 150.55,
            bid_size: 10,
            ask_size: 5,
        };
        assert_eq!(quote.ticker.as_str(), "AAPL");
        assert_eq!(quote.bid_price, 150.50);
        assert_eq!(quote.ask_price, 150.55);
        assert_eq!(quote.bid_size, 10);
        assert_eq!(quote.ask_size, 5);
    }

    #[test]
    fn test_equity_quote_clone() {
        let quote = EquityQuote {
            timestamp: Utc::now(),
            ticker: Ticker::new("MSFT").unwrap(),
            bid_price: 420.10,
            ask_price: 420.20,
            bid_size: 2,
            ask_size: 4,
        };
        let cloned = quote.clone();
        assert_eq!(cloned.ticker.as_str(), "MSFT");
        assert_eq!(cloned.bid_price, 420.10);
    }

    #[test]
    fn test_equity_details_construction() {
        let details = EquityDetails {
            ticker: Ticker::new("AAPL").unwrap(),
            sector: "TECHNOLOGY".to_string(),
            industry: "SOFTWARE".to_string(),
        };
        assert_eq!(details.ticker.as_str(), "AAPL");
        assert_eq!(details.sector, "TECHNOLOGY");
        assert_eq!(details.industry, "SOFTWARE");
    }

    #[test]
    fn test_equity_details_clone() {
        let details = EquityDetails {
            ticker: Ticker::new("NVDA").unwrap(),
            sector: "TECHNOLOGY".to_string(),
            industry: "SEMICONDUCTORS".to_string(),
        };
        let cloned = details.clone();
        assert_eq!(cloned.ticker.as_str(), "NVDA");
    }
}
