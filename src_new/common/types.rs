//! The domain vocabulary every module speaks.
//!
//! Two kinds of type live here. **Validated primitives** wrap standard Rust types so unit-mismatch
//! and sign bugs are compile errors rather than silent runtime surprises. **Record types** mirror
//! the PostgreSQL tables and the Alpaca payloads they are built from.
//!
//! Both follow the same rule: fields are private and construction goes through a constructor that
//! validates. A value of one of these types in scope is proof that its invariants held at
//! construction, so downstream code never re-checks.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Liquidity thresholds defining the modeled and served equity universe.
///
/// Training applies them per row and inference per ticker average; both sides must use the same
/// values so the model trains on the population it serves. They were historically mismatched, which
/// trained the scaler and model on penny-stock dynamics the service never predicts.
pub const MINIMUM_CLOSE_PRICE: f64 = 10.0;
pub const MINIMUM_VOLUME: f64 = 1_000_000.0;

// ---------------------------------------------------------------------------
// Validated primitives
// ---------------------------------------------------------------------------

/// Error returned when constructing a validated primitive with an out-of-range value.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeError {
    pub value: f64,
    pub minimum: f64,
    pub maximum: f64,
}

impl std::fmt::Display for RangeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Value {} is not in range [{}, {}].",
            self.value, self.minimum, self.maximum
        )
    }
}

impl std::error::Error for RangeError {}

/// Error returned when constructing a `Shares` with a non-positive quantity.
#[derive(Debug, Clone, PartialEq)]
pub struct SharesError;

impl std::fmt::Display for SharesError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Share quantity must be positive.")
    }
}

impl std::error::Error for SharesError {}

/// Error returned when constructing a validated amount with a negative value.
#[derive(Debug, Clone, PartialEq)]
pub struct NegativeAmountError {
    pub amount: Decimal,
}

impl std::fmt::Display for NegativeAmountError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Amount {} must be non-negative.", self.amount)
    }
}

impl std::error::Error for NegativeAmountError {}

/// Whole share count (always positive).
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Shares(Decimal);

impl Shares {
    /// Creates a new `Shares`, returning an error if `quantity` is not positive.
    pub fn new(quantity: Decimal) -> Result<Self, SharesError> {
        if quantity <= Decimal::ZERO {
            return Err(SharesError);
        }
        Ok(Shares(quantity))
    }

    /// Returns the inner `Decimal` value.
    pub fn value(self) -> Decimal {
        self.0
    }
}

impl<'de> Deserialize<'de> for Shares {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <Decimal as Deserialize<'de>>::deserialize(deserializer)?;
        Shares::new(raw).map_err(de::Error::custom)
    }
}

/// Dollar amount (non-negative).
///
/// Every current use is a magnitude — fill notionals and limit prices — so the constructor rejects
/// negative values.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize)]
pub struct Dollars(Decimal);

impl Dollars {
    /// Creates a new `Dollars`, returning an error if `amount` is negative.
    pub fn new(amount: Decimal) -> Result<Self, NegativeAmountError> {
        if amount < Decimal::ZERO {
            return Err(NegativeAmountError { amount });
        }
        Ok(Dollars(amount))
    }

    /// Returns the inner `Decimal` value.
    pub fn value(self) -> Decimal {
        self.0
    }
}

impl<'de> Deserialize<'de> for Dollars {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <Decimal as Deserialize<'de>>::deserialize(deserializer)?;
        Dollars::new(raw).map_err(de::Error::custom)
    }
}

/// Percentage in the range `[0.0, 1.0]`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize)]
pub struct Percent(f64);

impl Percent {
    /// Creates a new `Percent`, returning an error if `value` is outside `[0.0, 1.0]`.
    pub fn new(value: f64) -> Result<Self, RangeError> {
        if !(0.0..=1.0).contains(&value) {
            return Err(RangeError {
                value,
                minimum: 0.0,
                maximum: 1.0,
            });
        }
        Ok(Percent(value))
    }

    /// Returns the inner `f64` value.
    pub fn value(self) -> f64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Percent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = f64::deserialize(deserializer)?;
        Percent::new(raw).map_err(de::Error::custom)
    }
}

/// Notional dollar amount.
///
/// Wraps an already-validated [`Dollars`], so construction is infallible: the `Dollars` in hand is
/// proof the amount is non-negative.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Notional(Dollars);

impl Notional {
    /// Creates a new `Notional` from a validated `Dollars` amount.
    pub fn new(amount: Dollars) -> Self {
        Notional(amount)
    }

    /// Returns the inner `Dollars` value.
    pub fn value(self) -> Dollars {
        self.0
    }
}

impl<'de> Deserialize<'de> for Notional {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let amount = Dollars::deserialize(deserializer)?;
        Ok(Notional::new(amount))
    }
}

// ---------------------------------------------------------------------------
// Ticker
// ---------------------------------------------------------------------------

/// A normalized US equity ticker symbol.
///
/// Enforces the Alpaca US equity ticker format: 1–5 uppercase ASCII letters for the base symbol,
/// with an optional dot-separated suffix of 1–3 uppercase ASCII letters for share class or warrant
/// notation (e.g. `BRK.B`, `BRK.WS`).
///
/// Alpaca asset reference: <https://docs.alpaca.markets/us/reference/get-v2-assets-1>
///
/// The private field prevents construction without going through [`Ticker::new`], which trims,
/// uppercases, and validates. A `Ticker` in scope is proof that the symbol passed format validation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct Ticker(String);

impl Ticker {
    /// Constructs a `Ticker` from a raw string.
    ///
    /// Trims surrounding whitespace, uppercases, then validates the result against the US equity
    /// ticker format. Returns `None` if the normalized value does not match.
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
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
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

impl<'de> Deserialize<'de> for Ticker {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ticker::new(&raw).ok_or_else(|| de::Error::custom(format!("invalid ticker: {}", raw)))
    }
}

impl sqlx::Type<sqlx::Postgres> for Ticker {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(type_info: &sqlx::postgres::PgTypeInfo) -> bool {
        <String as sqlx::Type<sqlx::Postgres>>::compatible(type_info)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for Ticker {
    fn encode_by_ref(
        &self,
        buffer: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0.as_str(), buffer)
    }
}

/// Decoding routes through [`Ticker::new`] so a `Ticker` read from the database carries the same
/// validation proof as one constructed in code; an invalid stored value surfaces as a decode error
/// instead of bypassing the format check.
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

fn is_valid_base(segment: &str) -> bool {
    !segment.is_empty() && segment.len() <= 5 && segment.chars().all(|c| c.is_ascii_uppercase())
}

fn is_valid_suffix(segment: &str) -> bool {
    !segment.is_empty() && segment.len() <= 3 && segment.chars().all(|c| c.is_ascii_uppercase())
}

// ---------------------------------------------------------------------------
// PairID
// ---------------------------------------------------------------------------

/// A canonical long-short equity pair identifier.
///
/// Combines two validated [`Ticker`] values into a `"LONG-SHORT"` formatted string, stored at
/// construction so [`PairID::as_str`] is a cheap borrow.
///
/// Splitting on the **first** dash only means tickers with dot-suffixes such as `BRK.B` round-trip
/// correctly: `"BRK.B-MSFT"` splits into `("BRK.B", "MSFT")`, not three fragments.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PairID {
    long: Ticker,
    short: Ticker,
    formatted: String,
}

impl PairID {
    /// Constructs a `PairID` from two validated `Ticker` values.
    pub fn new(long: Ticker, short: Ticker) -> Self {
        let formatted = format!("{}-{}", long.as_str(), short.as_str());
        Self {
            long,
            short,
            formatted,
        }
    }

    /// Parses a `"LONG-SHORT"` formatted string by splitting on the first dash only.
    ///
    /// Returns `None` if the string cannot be split on `'-'` or if either half fails
    /// [`Ticker::new`].
    pub fn parse(raw: &str) -> Option<Self> {
        let (long_raw, short_raw) = raw.split_once('-')?;
        let long = Ticker::new(long_raw)?;
        let short = Ticker::new(short_raw)?;
        Some(Self::new(long, short))
    }

    /// Returns the canonical `"LONG-SHORT"` formatted string.
    pub fn as_str(&self) -> &str {
        &self.formatted
    }

    /// Returns the long-leg ticker.
    pub fn long(&self) -> &Ticker {
        &self.long
    }

    /// Returns the short-leg ticker.
    pub fn short(&self) -> &Ticker {
        &self.short
    }
}

impl std::fmt::Display for PairID {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.formatted)
    }
}

impl Serialize for PairID {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.formatted)
    }
}

impl sqlx::Type<sqlx::Postgres> for PairID {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(type_info: &sqlx::postgres::PgTypeInfo) -> bool {
        <String as sqlx::Type<sqlx::Postgres>>::compatible(type_info)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PairID {
    fn encode_by_ref(
        &self,
        buffer: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.formatted.as_str(), buffer)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PairID {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <String as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        PairID::parse(&raw)
            .ok_or_else(|| format!("invalid pair id decoded from database: {}", raw).into())
    }
}

// ---------------------------------------------------------------------------
// BarInterval
// ---------------------------------------------------------------------------

/// The sampling interval of an OHLCV bar.
///
/// Part of the `equity_bars` primary key, so a single table carries daily and intraday history
/// together. Only [`BarInterval::OneDay`] is written today; the rest exist so the intraday
/// migration needs no table rewrite.
///
/// Two string forms, deliberately distinct. [`BarInterval::as_str`] is the canonical stored form
/// and must match the `bar_interval` CHECK constraint in `schema.sql` exactly.
/// [`BarInterval::alpaca_timeframe`] is what Alpaca's bars API expects, which differs in both
/// casing and vocabulary — Alpaca writes `1Min` and `1Hour` where the stored form is `1min` and
/// `60min`. Conflating them silently produces empty API responses, so the mapping lives here rather
/// than at each call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum BarInterval {
    OneMinute,
    FiveMinute,
    FifteenMinute,
    ThirtyMinute,
    SixtyMinute,
    OneDay,
}

impl BarInterval {
    /// Every variant, for exhaustive iteration in tests and validation.
    pub const ALL: [BarInterval; 6] = [
        BarInterval::OneMinute,
        BarInterval::FiveMinute,
        BarInterval::FifteenMinute,
        BarInterval::ThirtyMinute,
        BarInterval::SixtyMinute,
        BarInterval::OneDay,
    ];

    /// The canonical stored form, matching the `bar_interval` CHECK constraint.
    pub fn as_str(self) -> &'static str {
        match self {
            BarInterval::OneMinute => "1min",
            BarInterval::FiveMinute => "5min",
            BarInterval::FifteenMinute => "15min",
            BarInterval::ThirtyMinute => "30min",
            BarInterval::SixtyMinute => "60min",
            BarInterval::OneDay => "1day",
        }
    }

    /// The timeframe string Alpaca's bars API expects.
    ///
    /// Not the same as [`BarInterval::as_str`]: Alpaca capitalizes and expresses sixty minutes as
    /// an hour.
    pub fn alpaca_timeframe(self) -> &'static str {
        match self {
            BarInterval::OneMinute => "1Min",
            BarInterval::FiveMinute => "5Min",
            BarInterval::FifteenMinute => "15Min",
            BarInterval::ThirtyMinute => "30Min",
            BarInterval::SixtyMinute => "1Hour",
            BarInterval::OneDay => "1Day",
        }
    }

    /// Parses the canonical stored form. Returns `None` for anything else, including Alpaca's
    /// timeframe spelling — the two vocabularies are deliberately not interchangeable.
    pub fn parse(raw: &str) -> Option<Self> {
        BarInterval::ALL
            .into_iter()
            .find(|interval| interval.as_str() == raw)
    }
}

impl std::fmt::Display for BarInterval {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for BarInterval {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        BarInterval::parse(&raw)
            .ok_or_else(|| de::Error::custom(format!("invalid bar interval: {}", raw)))
    }
}

impl sqlx::Type<sqlx::Postgres> for BarInterval {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(type_info: &sqlx::postgres::PgTypeInfo) -> bool {
        <String as sqlx::Type<sqlx::Postgres>>::compatible(type_info)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for BarInterval {
    fn encode_by_ref(
        &self,
        buffer: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buffer)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for BarInterval {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <String as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        BarInterval::parse(&raw)
            .ok_or_else(|| format!("invalid bar interval decoded from database: {}", raw).into())
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// An OHLCV equity bar at a declared interval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityBar {
    ticker: Ticker,
    bar_interval: BarInterval,
    /// UTC timestamp of the period this bar opens.
    timestamp: DateTime<Utc>,
    open_price: f64,
    high_price: f64,
    low_price: f64,
    close_price: f64,
    /// Whole share units. Fractional values from the source API are rounded on ingest.
    volume: i64,
    volume_weighted_average_price: Option<f64>,
    transactions: Option<i64>,
}

impl EquityBar {
    /// Constructs an `EquityBar` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ticker: Ticker,
        bar_interval: BarInterval,
        timestamp: DateTime<Utc>,
        open_price: f64,
        high_price: f64,
        low_price: f64,
        close_price: f64,
        volume: i64,
        volume_weighted_average_price: Option<f64>,
        transactions: Option<i64>,
    ) -> Self {
        Self {
            ticker,
            bar_interval,
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            volume_weighted_average_price,
            transactions,
        }
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn bar_interval(&self) -> BarInterval {
        self.bar_interval
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
}

/// A two-sided bid/ask quote observed at a point in time.
///
/// Produced by the snapshot fetch, which is the only price source in the system — the streaming
/// path was removed with the rest of the WebSocket surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityQuote {
    ticker: Ticker,
    timestamp: DateTime<Utc>,
    bid_price: f64,
    ask_price: f64,
    bid_size: i32,
    ask_size: i32,
}

impl EquityQuote {
    /// Constructs an `EquityQuote` from validated field values.
    pub fn new(
        ticker: Ticker,
        timestamp: DateTime<Utc>,
        bid_price: f64,
        ask_price: f64,
        bid_size: i32,
        ask_size: i32,
    ) -> Self {
        Self {
            ticker,
            timestamp,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
        }
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
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

    /// The midpoint of the book.
    ///
    /// Callers are responsible for having established that the book is worth taking a midpoint of;
    /// this is arithmetic, not a judgment about quality.
    pub fn mid_price(&self) -> f64 {
        (self.bid_price + self.ask_price) / 2.0
    }
}

/// Ticker metadata used to constrain pair selection to cross-sector matches.
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

/// One ticker's quantile forecast from a single prediction batch.
///
/// The three quantiles are ordered by construction: [`EquityPrediction::new`] rejects a set where
/// `quantile_10 > quantile_50` or `quantile_50 > quantile_90`. A crossed quantile set is a model or
/// deserialization bug, and every downstream use — the confidence measure, the directional signal —
/// silently produces nonsense from one rather than failing.
#[derive(Debug, Clone, Serialize)]
pub struct EquityPrediction {
    correlation_id: Uuid,
    model_run_id: String,
    ticker: Ticker,
    timestamp: DateTime<Utc>,
    quantile_10: f64,
    quantile_50: f64,
    quantile_90: f64,
}

/// Error returned when constructing an [`EquityPrediction`] whose quantiles are not ordered.
#[derive(Debug, Clone, PartialEq)]
pub struct CrossedQuantilesError {
    pub quantile_10: f64,
    pub quantile_50: f64,
    pub quantile_90: f64,
}

impl std::fmt::Display for CrossedQuantilesError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Quantiles must be non-decreasing, got [{}, {}, {}].",
            self.quantile_10, self.quantile_50, self.quantile_90
        )
    }
}

impl std::error::Error for CrossedQuantilesError {}

impl EquityPrediction {
    /// Constructs an `EquityPrediction`, rejecting crossed or non-finite quantiles.
    pub fn new(
        correlation_id: Uuid,
        model_run_id: String,
        ticker: Ticker,
        timestamp: DateTime<Utc>,
        quantile_10: f64,
        quantile_50: f64,
        quantile_90: f64,
    ) -> Result<Self, CrossedQuantilesError> {
        let ordered = quantile_10 <= quantile_50 && quantile_50 <= quantile_90;
        let finite = quantile_10.is_finite() && quantile_50.is_finite() && quantile_90.is_finite();
        if !ordered || !finite {
            return Err(CrossedQuantilesError {
                quantile_10,
                quantile_50,
                quantile_90,
            });
        }
        Ok(Self {
            correlation_id,
            model_run_id,
            ticker,
            timestamp,
            quantile_10,
            quantile_50,
            quantile_90,
        })
    }

    pub fn correlation_id(&self) -> Uuid {
        self.correlation_id
    }

    pub fn model_run_id(&self) -> &str {
        &self.model_run_id
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn quantile_10(&self) -> f64 {
        self.quantile_10
    }

    pub fn quantile_50(&self) -> f64 {
        self.quantile_50
    }

    pub fn quantile_90(&self) -> f64 {
        self.quantile_90
    }

    /// The median forecast, which is the expected forward return the strategy trades on.
    pub fn expected_return(&self) -> f64 {
        self.quantile_50
    }

    /// Confidence derived from the width of the prediction interval, in `(0.0, 1.0]`.
    ///
    /// A tight interval means the model is sure; a wide one means it is not. The reciprocal form
    /// maps a zero-width interval to 1.0 and decays monotonically, and is finite for any
    /// non-negative width, which the ordering invariant guarantees.
    pub fn confidence(&self) -> f64 {
        1.0 / (1.0 + (self.quantile_90 - self.quantile_10))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).expect("test ticker must be valid")
    }

    #[test]
    fn test_ticker_normalizes_case_and_whitespace() {
        assert_eq!(ticker(" aapl ").as_str(), "AAPL");
    }

    #[test]
    fn test_ticker_accepts_dot_suffix() {
        assert_eq!(ticker("brk.b").as_str(), "BRK.B");
        assert_eq!(ticker("BRK.WS").as_str(), "BRK.WS");
    }

    #[test]
    fn test_ticker_rejects_malformed() {
        assert!(Ticker::new("").is_none());
        assert!(Ticker::new("TOOLONG").is_none());
        assert!(Ticker::new("AA1").is_none());
        assert!(Ticker::new("A.B.C").is_none());
        assert!(Ticker::new("A.").is_none());
        assert!(Ticker::new(".B").is_none());
        assert!(Ticker::new("A.TOOLONG").is_none());
    }

    #[test]
    fn test_ticker_deserialize_rejects_invalid() {
        let result: Result<Ticker, _> = serde_json::from_str("\"aa1\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_pair_id_round_trips_through_parse() {
        let pair = PairID::new(ticker("AAPL"), ticker("MSFT"));
        assert_eq!(pair.as_str(), "AAPL-MSFT");
        assert_eq!(PairID::parse("AAPL-MSFT"), Some(pair));
    }

    /// A dot-suffixed leg must survive the round trip: splitting on every dash rather than the
    /// first would turn `BRK.B-MSFT` into three fragments and fail to parse.
    #[test]
    fn test_pair_id_parse_splits_on_first_dash_only() {
        let parsed = PairID::parse("BRK.B-MSFT").expect("dot-suffixed leg must parse");
        assert_eq!(parsed.long().as_str(), "BRK.B");
        assert_eq!(parsed.short().as_str(), "MSFT");
    }

    #[test]
    fn test_pair_id_parse_rejects_malformed() {
        assert!(PairID::parse("AAPL").is_none());
        assert!(PairID::parse("AAPL-").is_none());
        assert!(PairID::parse("-MSFT").is_none());
    }

    #[test]
    fn test_bar_interval_round_trips_through_stored_form() {
        for interval in BarInterval::ALL {
            assert_eq!(BarInterval::parse(interval.as_str()), Some(interval));
        }
    }

    /// The stored form and the Alpaca timeframe are different vocabularies. If a refactor ever
    /// collapses them, this fails rather than silently sending `1day` to an API expecting `1Day`.
    #[test]
    fn test_bar_interval_alpaca_timeframe_differs_from_stored_form() {
        assert_eq!(BarInterval::OneDay.as_str(), "1day");
        assert_eq!(BarInterval::OneDay.alpaca_timeframe(), "1Day");
        assert_eq!(BarInterval::SixtyMinute.as_str(), "60min");
        assert_eq!(BarInterval::SixtyMinute.alpaca_timeframe(), "1Hour");
    }

    #[test]
    fn test_bar_interval_parse_rejects_alpaca_spelling() {
        assert!(BarInterval::parse("1Day").is_none());
        assert!(BarInterval::parse("1Hour").is_none());
        assert!(BarInterval::parse("7min").is_none());
    }

    #[test]
    fn test_percent_bounds() {
        assert_eq!(Percent::new(0.0).unwrap().value(), 0.0);
        assert_eq!(Percent::new(1.0).unwrap().value(), 1.0);
        assert!(Percent::new(-0.1).is_err());
        assert!(Percent::new(1.1).is_err());
    }

    #[test]
    fn test_shares_rejects_non_positive() {
        assert!(Shares::new(Decimal::ZERO).is_err());
        assert!(Shares::new(Decimal::from(-1)).is_err());
        assert_eq!(
            Shares::new(Decimal::from(100)).unwrap().value(),
            Decimal::from(100)
        );
    }

    #[test]
    fn test_dollars_rejects_negative_but_accepts_zero() {
        assert!(Dollars::new(Decimal::from(-1)).is_err());
        assert_eq!(Dollars::new(Decimal::ZERO).unwrap().value(), Decimal::ZERO);
    }

    #[test]
    fn test_dollars_deserialize_rejects_negative() {
        let result: Result<Dollars, _> = serde_json::from_str("\"-5\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_notional_wraps_validated_dollars() {
        let notional = Notional::new(Dollars::new(Decimal::from(10_000)).unwrap());
        assert_eq!(notional.value().value(), Decimal::from(10_000));
    }

    #[test]
    fn test_error_displays_name_their_cause() {
        assert!(format!("{}", SharesError).contains("positive"));
        assert!(format!(
            "{}",
            NegativeAmountError {
                amount: Decimal::from(-5)
            }
        )
        .contains("non-negative"));
        assert!(format!(
            "{}",
            RangeError {
                value: 1.5,
                minimum: 0.0,
                maximum: 1.0
            }
        )
        .contains("1.5"));
    }

    fn prediction(
        low: f64,
        mid: f64,
        high: f64,
    ) -> Result<EquityPrediction, CrossedQuantilesError> {
        EquityPrediction::new(
            Uuid::nil(),
            "run-1".to_string(),
            ticker("AAPL"),
            Utc::now(),
            low,
            mid,
            high,
        )
    }

    #[test]
    fn test_prediction_accepts_ordered_quantiles() {
        let prediction = prediction(-0.01, 0.0, 0.01).expect("ordered quantiles must construct");
        assert_eq!(prediction.expected_return(), 0.0);
    }

    #[test]
    fn test_prediction_rejects_crossed_quantiles() {
        assert!(prediction(0.02, 0.0, 0.01).is_err());
        assert!(prediction(-0.01, 0.05, 0.01).is_err());
    }

    #[test]
    fn test_prediction_rejects_non_finite_quantiles() {
        assert!(prediction(f64::NAN, 0.0, 0.01).is_err());
        assert!(prediction(-0.01, 0.0, f64::INFINITY).is_err());
    }

    /// Confidence must fall as the interval widens, and stay inside `(0.0, 1.0]`. A zero-width
    /// interval is maximal confidence.
    #[test]
    fn test_prediction_confidence_decreases_with_interval_width() {
        let tight = prediction(0.0, 0.0, 0.0).unwrap();
        let medium = prediction(-0.01, 0.0, 0.01).unwrap();
        let wide = prediction(-0.5, 0.0, 0.5).unwrap();

        assert_eq!(tight.confidence(), 1.0);
        assert!(medium.confidence() < tight.confidence());
        assert!(wide.confidence() < medium.confidence());
        assert!(wide.confidence() > 0.0);
    }
}
