//! Raw ingest record types from market data providers (Alpaca, Massive).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Daily OHLCV equity bar record.
///
/// Timestamps are stored as `TIMESTAMPTZ` in PostgreSQL. The `inserted_at` field
/// is set by the database on insert.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityBar {
    pub ticker: String,
    /// UTC timestamp for the trading day this bar covers.
    pub timestamp: DateTime<Utc>,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    /// Whole share units. Fractional values from the source API are rounded on ingest.
    pub volume: Option<i64>,
    pub volume_weighted_average_price: Option<f64>,
    pub transactions: Option<i64>,
    /// Set by the database at insert time.
    pub inserted_at: DateTime<Utc>,
}

/// Intraday bid/ask quote record from the Alpaca WebSocket stream.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityQuote {
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: i32,
    pub ask_size: i32,
}

/// Ticker metadata record seeded from the S3 details CSV.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityDetails {
    pub ticker: String,
    pub sector: String,
    pub industry: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_equity_bar_construction_with_all_fields() {
        let now = Utc::now();
        let bar = EquityBar {
            ticker: "AAPL".to_string(),
            timestamp: now,
            open_price: Some(150.0),
            high_price: Some(155.0),
            low_price: Some(149.0),
            close_price: Some(153.0),
            volume: Some(1_000_000),
            volume_weighted_average_price: Some(152.0),
            transactions: Some(50_000),
            inserted_at: now,
        };
        assert_eq!(bar.ticker, "AAPL");
        assert_eq!(bar.open_price, Some(150.0));
        assert_eq!(bar.volume, Some(1_000_000));
    }

    #[test]
    fn test_equity_bar_construction_with_nullable_fields() {
        let now = Utc::now();
        let bar = EquityBar {
            ticker: "MSFT".to_string(),
            timestamp: now,
            open_price: None,
            high_price: None,
            low_price: None,
            close_price: None,
            volume: None,
            volume_weighted_average_price: None,
            transactions: None,
            inserted_at: now,
        };
        assert_eq!(bar.ticker, "MSFT");
        assert!(bar.open_price.is_none());
    }

    #[test]
    fn test_equity_bar_clone() {
        let now = Utc::now();
        let bar = EquityBar {
            ticker: "GOOG".to_string(),
            timestamp: now,
            open_price: Some(100.0),
            high_price: Some(105.0),
            low_price: Some(99.0),
            close_price: Some(103.0),
            volume: Some(500_000),
            volume_weighted_average_price: Some(102.0),
            transactions: Some(25_000),
            inserted_at: now,
        };
        let cloned = bar.clone();
        assert_eq!(cloned.ticker, "GOOG");
        assert_eq!(cloned.close_price, Some(103.0));
    }

    #[test]
    fn test_equity_quote_construction() {
        let quote = EquityQuote {
            timestamp: Utc::now(),
            ticker: "AAPL".to_string(),
            bid_price: 150.50,
            ask_price: 150.55,
            bid_size: 10,
            ask_size: 5,
        };
        assert_eq!(quote.ticker, "AAPL");
        assert_eq!(quote.bid_price, 150.50);
        assert_eq!(quote.ask_price, 150.55);
        assert_eq!(quote.bid_size, 10);
        assert_eq!(quote.ask_size, 5);
    }

    #[test]
    fn test_equity_quote_clone() {
        let quote = EquityQuote {
            timestamp: Utc::now(),
            ticker: "MSFT".to_string(),
            bid_price: 420.10,
            ask_price: 420.20,
            bid_size: 2,
            ask_size: 4,
        };
        let cloned = quote.clone();
        assert_eq!(cloned.ticker, "MSFT");
        assert_eq!(cloned.bid_price, 420.10);
    }

    #[test]
    fn test_equity_details_construction() {
        let details = EquityDetails {
            ticker: "AAPL".to_string(),
            sector: "TECHNOLOGY".to_string(),
            industry: "SOFTWARE".to_string(),
        };
        assert_eq!(details.ticker, "AAPL");
        assert_eq!(details.sector, "TECHNOLOGY");
        assert_eq!(details.industry, "SOFTWARE");
    }

    #[test]
    fn test_equity_details_clone() {
        let details = EquityDetails {
            ticker: "NVDA".to_string(),
            sector: "TECHNOLOGY".to_string(),
            industry: "SEMICONDUCTORS".to_string(),
        };
        let cloned = details.clone();
        assert_eq!(cloned.ticker, "NVDA");
    }
}
