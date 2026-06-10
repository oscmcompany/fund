//! Raw ingest record types from market data providers (Alpaca, Massive).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::FromRow;

/// Liquidity thresholds defining the modeled and served equity universe.
///
/// Training applies them per row (`fit::filter_training_bars`) and inference
/// per ticker average (`predict::filter_equity_bars`); both sides must use the
/// same values so the model trains on the population it serves. They were
/// historically mismatched (training at 1.0 / 100k), which trained the scaler
/// and model on penny-stock dynamics the service never predicts.
pub const MINIMUM_CLOSE_PRICE: f64 = 10.0;
pub const MINIMUM_VOLUME: f64 = 1_000_000.0;

/// A normalized US equity ticker symbol.
///
/// Enforces the Alpaca US equity ticker format: 1–5 uppercase ASCII letters for
/// the base symbol, with an optional dot-separated suffix of 1–3 uppercase ASCII
/// letters for share class or warrant notation (e.g. `BRK.B`, `BRK.WS`).
///
/// The private field prevents construction without going through [`Ticker::new`],
/// which trims, uppercases, and validates the raw input. A `Ticker` in scope is
/// proof that the symbol passed format validation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
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

impl sqlx::Type<sqlx::Postgres> for Ticker {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <String as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for Ticker {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0.as_str(), buf)
    }
}

/// Decoding routes through [`Ticker::new`] so a `Ticker` read from the database
/// carries the same validation proof as one constructed in code; an invalid
/// stored value surfaces as a decode error instead of bypassing the format check.
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for Ticker {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <String as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Ticker::new(&raw)
            .ok_or_else(|| format!("invalid ticker decoded from database: {}", raw).into())
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
    ticker: Ticker,
    /// UTC timestamp for the trading day this bar covers.
    timestamp: DateTime<Utc>,
    open_price: f64,
    high_price: f64,
    low_price: f64,
    close_price: f64,
    /// Whole share units. Fractional values from the source API are rounded on ingest.
    volume: i64,
    volume_weighted_average_price: Option<f64>,
    transactions: Option<i64>,
    /// Set by the database at insert time.
    inserted_at: DateTime<Utc>,
}

impl EquityBar {
    /// Constructs an `EquityBar` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ticker: Ticker,
        timestamp: DateTime<Utc>,
        open_price: f64,
        high_price: f64,
        low_price: f64,
        close_price: f64,
        volume: i64,
        volume_weighted_average_price: Option<f64>,
        transactions: Option<i64>,
        inserted_at: DateTime<Utc>,
    ) -> Self {
        Self {
            ticker,
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            volume_weighted_average_price,
            transactions,
            inserted_at,
        }
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn open_price(&self) -> f64 {
        self.open_price
    }

    pub fn high_price(&self) -> f64 {
        self.high_price
    }

    pub fn low_price(&self) -> f64 {
        self.low_price
    }

    pub fn close_price(&self) -> f64 {
        self.close_price
    }

    pub fn volume(&self) -> i64 {
        self.volume
    }

    pub fn volume_weighted_average_price(&self) -> Option<f64> {
        self.volume_weighted_average_price
    }

    pub fn transactions(&self) -> Option<i64> {
        self.transactions
    }

    pub fn inserted_at(&self) -> DateTime<Utc> {
        self.inserted_at
    }
}

/// Intraday bid/ask quote record from the Alpaca WebSocket stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityQuote {
    timestamp: DateTime<Utc>,
    ticker: Ticker,
    bid_price: f64,
    ask_price: f64,
    bid_size: i32,
    ask_size: i32,
}

impl EquityQuote {
    /// Constructs an `EquityQuote` from validated field values.
    pub fn new(
        timestamp: DateTime<Utc>,
        ticker: Ticker,
        bid_price: f64,
        ask_price: f64,
        bid_size: i32,
        ask_size: i32,
    ) -> Self {
        Self {
            timestamp,
            ticker,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn bid_price(&self) -> f64 {
        self.bid_price
    }

    pub fn ask_price(&self) -> f64 {
        self.ask_price
    }

    pub fn bid_size(&self) -> i32 {
        self.bid_size
    }

    pub fn ask_size(&self) -> i32 {
        self.ask_size
    }
}

/// Ticker metadata record seeded from the S3 details CSV.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityDetail {
    ticker: Ticker,
    sector: String,
    industry: String,
}

impl EquityDetail {
    /// Constructs an `EquityDetail` from validated field values.
    pub fn new(ticker: Ticker, sector: String, industry: String) -> Self {
        Self {
            ticker,
            sector,
            industry,
        }
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn sector(&self) -> &str {
        &self.sector
    }

    pub fn industry(&self) -> &str {
        &self.industry
    }
}

/// A non-empty collection of [`EquityBar`] records.
///
/// The `Option`-returning constructor enforces that a value in scope always
/// contains at least one bar. Callers that receive `None` know immediately that
/// there is nothing to process or store.
#[derive(Debug, Clone)]
pub struct EquityBars(Vec<EquityBar>);

impl EquityBars {
    /// Returns `None` if `bars` is empty.
    pub fn new(bars: Vec<EquityBar>) -> Option<Self> {
        if bars.is_empty() {
            None
        } else {
            Some(Self(bars))
        }
    }

    pub fn as_slice(&self) -> &[EquityBar] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A non-empty collection of [`EquityQuote`] records.
#[derive(Debug, Clone)]
pub struct EquityQuotes(Vec<EquityQuote>);

impl EquityQuotes {
    /// Returns `None` if `quotes` is empty.
    pub fn new(quotes: Vec<EquityQuote>) -> Option<Self> {
        if quotes.is_empty() {
            None
        } else {
            Some(Self(quotes))
        }
    }

    pub fn as_slice(&self) -> &[EquityQuote] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A non-empty collection of [`EquityDetail`] records.
#[derive(Debug, Clone)]
pub struct EquityDetails(Vec<EquityDetail>);

impl EquityDetails {
    /// Returns `None` if `details` is empty.
    pub fn new(details: Vec<EquityDetail>) -> Option<Self> {
        if details.is_empty() {
            None
        } else {
            Some(Self(details))
        }
    }

    pub fn as_slice(&self) -> &[EquityDetail] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_universe_thresholds_match_served_population() {
        // Training and inference both consume these; the values are the served
        // universe (close > $10, volume > 1M on average), deliberately aligned
        // after the historical training/inference mismatch.
        assert_eq!(MINIMUM_CLOSE_PRICE, 10.0);
        assert_eq!(MINIMUM_VOLUME, 1_000_000.0);
    }

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
    fn test_ticker_serde_round_trip() {
        let ticker = Ticker::new("BRK.B").unwrap();
        let serialized = serde_json::to_string(&ticker).unwrap();
        assert_eq!(serialized, "\"BRK.B\"");
        let deserialized: Ticker = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, ticker);
    }

    #[test]
    fn test_ticker_deserialize_rejects_invalid() {
        let result: Result<Ticker, _> = serde_json::from_str("\"NOTAVALIDTICKER\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_equity_bar_construction_with_all_fields() {
        let now = Utc::now();
        let bar = EquityBar::new(
            Ticker::new("AAPL").unwrap(),
            now,
            150.0,
            155.0,
            149.0,
            153.0,
            1_000_000,
            Some(152.0),
            Some(50_000),
            now,
        );
        assert_eq!(bar.ticker().as_str(), "AAPL");
        assert_eq!(bar.open_price(), 150.0);
        assert_eq!(bar.volume(), 1_000_000);
    }

    #[test]
    fn test_equity_bar_clone() {
        let now = Utc::now();
        let bar = EquityBar::new(
            Ticker::new("GOOG").unwrap(),
            now,
            100.0,
            105.0,
            99.0,
            103.0,
            500_000,
            Some(102.0),
            Some(25_000),
            now,
        );
        let cloned = bar.clone();
        assert_eq!(cloned.ticker().as_str(), "GOOG");
        assert_eq!(cloned.close_price(), 103.0);
    }

    #[test]
    fn test_equity_quote_construction() {
        let quote = EquityQuote::new(
            Utc::now(),
            Ticker::new("AAPL").unwrap(),
            150.50,
            150.55,
            10,
            5,
        );
        assert_eq!(quote.ticker().as_str(), "AAPL");
        assert_eq!(quote.bid_price(), 150.50);
        assert_eq!(quote.ask_price(), 150.55);
        assert_eq!(quote.bid_size(), 10);
        assert_eq!(quote.ask_size(), 5);
    }

    #[test]
    fn test_equity_quote_clone() {
        let quote = EquityQuote::new(
            Utc::now(),
            Ticker::new("MSFT").unwrap(),
            420.10,
            420.20,
            2,
            4,
        );
        let cloned = quote.clone();
        assert_eq!(cloned.ticker().as_str(), "MSFT");
        assert_eq!(cloned.bid_price(), 420.10);
    }

    #[test]
    fn test_equity_detail_construction() {
        let detail = EquityDetail::new(
            Ticker::new("AAPL").unwrap(),
            "TECHNOLOGY".to_string(),
            "SOFTWARE".to_string(),
        );
        assert_eq!(detail.ticker().as_str(), "AAPL");
        assert_eq!(detail.sector(), "TECHNOLOGY");
        assert_eq!(detail.industry(), "SOFTWARE");
    }

    #[test]
    fn test_equity_detail_clone() {
        let detail = EquityDetail::new(
            Ticker::new("NVDA").unwrap(),
            "TECHNOLOGY".to_string(),
            "SEMICONDUCTORS".to_string(),
        );
        let cloned = detail.clone();
        assert_eq!(cloned.ticker().as_str(), "NVDA");
    }

    #[test]
    fn test_equity_bars_new_returns_some_for_nonempty() {
        let now = Utc::now();
        let bar = EquityBar::new(
            Ticker::new("AAPL").unwrap(),
            now,
            150.0,
            155.0,
            149.0,
            153.0,
            1_000_000,
            None,
            None,
            now,
        );
        let bars = EquityBars::new(vec![bar]);
        assert!(bars.is_some());
        let bars = bars.unwrap();
        assert_eq!(bars.len(), 1);
        assert!(!bars.is_empty());
        assert_eq!(bars.as_slice()[0].ticker().as_str(), "AAPL");
    }

    #[test]
    fn test_equity_bars_new_returns_none_for_empty() {
        assert!(EquityBars::new(vec![]).is_none());
    }

    #[test]
    fn test_equity_quotes_new_returns_some_for_nonempty() {
        let quote = EquityQuote::new(Utc::now(), Ticker::new("AAPL").unwrap(), 150.0, 150.5, 1, 1);
        let quotes = EquityQuotes::new(vec![quote]);
        assert!(quotes.is_some());
        let quotes = quotes.unwrap();
        assert_eq!(quotes.len(), 1);
        assert!(!quotes.is_empty());
        assert_eq!(quotes.as_slice()[0].ticker().as_str(), "AAPL");
    }

    #[test]
    fn test_equity_quotes_new_returns_none_for_empty() {
        assert!(EquityQuotes::new(vec![]).is_none());
    }

    #[test]
    fn test_equity_details_new_returns_some_for_nonempty() {
        let detail = EquityDetail::new(
            Ticker::new("AAPL").unwrap(),
            "TECHNOLOGY".to_string(),
            "SOFTWARE".to_string(),
        );
        let details = EquityDetails::new(vec![detail]);
        assert!(details.is_some());
        let details = details.unwrap();
        assert_eq!(details.len(), 1);
        assert!(!details.is_empty());
        assert_eq!(details.as_slice()[0].sector(), "TECHNOLOGY");
    }

    #[test]
    fn test_equity_details_new_returns_none_for_empty() {
        assert!(EquityDetails::new(vec![]).is_none());
    }
}
