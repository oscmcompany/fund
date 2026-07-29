//! Raw ingest record types from market data providers (Alpaca, Massive).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
/// Both Alpaca and Massive use dot-separated suffixes for class shares and other
/// security subtypes, though their conventions diverge for preferred shares
/// (Alpaca: `BAC.PRL`, Massive: `BACpL`). Common stocks and class shares use
/// identical formats across both platforms.
///
/// Alpaca asset reference: <https://docs.alpaca.markets/us/reference/get-v2-assets-1>
/// Massive ticker reference: <https://massive.com/docs/rest/stocks/tickers/all-tickers>
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

/// A canonical long-short equity pair identifier.
///
/// Combines two validated [`Ticker`] values into a `"LONG-SHORT"` formatted
/// string. The canonical form is stored at construction time so that [`as_str`]
/// is a cheap borrow. A `PairID` in scope is proof that both legs passed ticker
/// format validation.
///
/// Splitting on the **first** dash only (via [`str::split_once`]) means tickers
/// with dot-suffixes such as `BRK.B` round-trip correctly: `"BRK.B-MSFT"` splits
/// into `("BRK.B", "MSFT")`, not three fragments.
///
/// [`as_str`]: PairID::as_str
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PairID {
    long: Ticker,
    short: Ticker,
    formatted: String,
}

impl PairID {
    /// Constructs a `PairID` from two validated `Ticker` values.
    ///
    /// Stores the canonical `"LONG-SHORT"` formatted string at construction
    /// time so that [`as_str`] is a cheap borrow.
    ///
    /// [`as_str`]: PairID::as_str
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
    /// Returns `None` if the string cannot be split on `'-'` or if either half
    /// fails [`Ticker::new`].
    pub fn parse(raw: &str) -> Option<Self> {
        let (long_str, short_str) = raw.split_once('-')?;
        let long = Ticker::new(long_str)?;
        let short = Ticker::new(short_str)?;
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.formatted)
    }
}

impl Serialize for PairID {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.formatted)
    }
}

impl<'de> Deserialize<'de> for PairID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        PairID::parse(&raw)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid pair id: {}", raw)))
    }
}

impl sqlx::Type<sqlx::Postgres> for PairID {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <String as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PairID {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.formatted.as_str(), buf)
    }
}

/// Decoding parses the stored `"A-B"` format by splitting on the **first** dash
/// only, consistent with [`PairID::parse`]. A malformed stored value surfaces as
/// a decode error rather than silently constructing an invalid `PairID`.
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PairID {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <String as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        PairID::parse(&raw)
            .ok_or_else(|| format!("invalid pair id decoded from database: {}", raw).into())
    }
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

/// Widest book, as a fraction of the midpoint, whose mid is treated as a price.
///
/// Derived from trade economics rather than from the observed distribution. A
/// pair crosses the spread on both legs entering and both legs exiting, so the
/// round-trip cost is roughly the sum of the two legs' spreads: at 25 basis
/// points a leg that is about 50 basis points a round turn, which the strategy's
/// edge has to clear before anything is left. Measured liquid names quote 1–15
/// basis points, so this leaves generous room while excluding the several
/// hundred to several thousand basis point books seen on thin symbols.
pub const MAXIMUM_RELATIVE_SPREAD: f64 = 0.0025;

/// Smallest quoted size, per side, accepted as real liquidity.
///
/// One round lot. This excludes odd-lot-only books rather than expressing a view
/// on depth: the observed sample quoted exactly 100 on both sides often enough
/// that a higher floor would cut aggressively for reasons no measurement here
/// supports. Raising it should follow a study of the size distribution across
/// the traded universe, not intuition.
pub const MINIMUM_QUOTE_SIZE: i32 = 100;

/// Bounds a two-sided book must satisfy before its midpoint is treated as a price.
///
/// Both limits exist because a quote can be arithmetically valid and still
/// economically meaningless. The IEX feed carries a few percent of consolidated
/// volume, so a thinly quoted name routinely shows a book hundreds of basis
/// points wide whose midpoint sits nowhere near any price the symbol traded at.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BookQualityLimits {
    maximum_relative_spread: f64,
    minimum_size: i32,
}

/// Error returned when constructing [`BookQualityLimits`] with unusable bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidBookQualityLimits;

impl std::fmt::Display for InvalidBookQualityLimits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Maximum relative spread must be positive and finite, and minimum size non-negative."
        )
    }
}

impl std::error::Error for InvalidBookQualityLimits {}

impl BookQualityLimits {
    /// Creates limits, rejecting a non-positive or non-finite spread bound.
    pub fn new(
        maximum_relative_spread: f64,
        minimum_size: i32,
    ) -> Result<Self, InvalidBookQualityLimits> {
        if !maximum_relative_spread.is_finite() || maximum_relative_spread <= 0.0 {
            return Err(InvalidBookQualityLimits);
        }
        if minimum_size < 0 {
            return Err(InvalidBookQualityLimits);
        }
        Ok(Self {
            maximum_relative_spread,
            minimum_size,
        })
    }

    pub fn maximum_relative_spread(&self) -> f64 {
        self.maximum_relative_spread
    }

    pub fn minimum_size(&self) -> i32 {
        self.minimum_size
    }
}

impl Default for BookQualityLimits {
    fn default() -> Self {
        Self::new(MAXIMUM_RELATIVE_SPREAD, MINIMUM_QUOTE_SIZE)
            .expect("MAXIMUM_RELATIVE_SPREAD and MINIMUM_QUOTE_SIZE must be valid bounds")
    }
}

/// An [`EquityQuote`] proven to carry a book worth pricing against.
///
/// Holding one is proof that every check in [`UsableQuote::new`] passed, so
/// [`UsableQuote::mid_price`] is a price rather than a candidate for
/// re-validation downstream. Both the streamed and the REST-snapshot paths
/// construct through here, which is what keeps them from drifting apart: an
/// earlier arrangement validated `bid > 0 && ask > 0` on one path and
/// additionally rejected crossed books on the other, so the same quote could be
/// usable or not depending on which code reached it first.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UsableQuote {
    mid_price: f64,
    relative_spread: f64,
    observed_at: DateTime<Utc>,
}

impl UsableQuote {
    /// Returns the validated quote, or `None` when the book fails any check.
    ///
    /// Each side is validated independently before the midpoint is formed.
    /// Checking only the average would accept a one-sided book such as
    /// `bid = 0, ask = 200`, whose mean is a plausible-looking 100 but is really
    /// half a real price — and it would then feed every spread that leg appears
    /// in. A crossed book (`bid > ask`) is rejected for a related reason: it is
    /// a stale frame or a feed artifact, and its midpoint is not a price
    /// anything traded at.
    ///
    /// Size is checked because a tight spread quoted for a handful of shares is
    /// not liquidity a real position can cross.
    pub fn new(quote: &EquityQuote, limits: BookQualityLimits) -> Option<Self> {
        let bid_price = quote.bid_price();
        let ask_price = quote.ask_price();

        if !bid_price.is_finite() || bid_price <= 0.0 {
            return None;
        }
        if !ask_price.is_finite() || ask_price <= 0.0 {
            return None;
        }
        if bid_price > ask_price {
            return None;
        }
        if quote.bid_size() < limits.minimum_size() || quote.ask_size() < limits.minimum_size() {
            return None;
        }

        let mid_price = (bid_price + ask_price) / 2.0;
        let relative_spread = (ask_price - bid_price) / mid_price;
        if relative_spread > limits.maximum_relative_spread() {
            return None;
        }

        Some(Self {
            mid_price,
            relative_spread,
            observed_at: quote.timestamp(),
        })
    }

    pub fn mid_price(&self) -> f64 {
        self.mid_price
    }

    /// Spread as a fraction of the midpoint; multiply by 10,000 for basis points.
    pub fn relative_spread(&self) -> f64 {
        self.relative_spread
    }

    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
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

    // --- PairID tests ---

    #[test]
    fn test_pair_id_new_stores_canonical_format() {
        let pair_id = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        assert_eq!(pair_id.as_str(), "AAPL-MSFT");
    }

    #[test]
    fn test_pair_id_new_long_short_accessors() {
        let pair_id = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        assert_eq!(pair_id.long().as_str(), "AAPL");
        assert_eq!(pair_id.short().as_str(), "MSFT");
    }

    #[test]
    fn test_pair_id_display_delegates_to_as_str() {
        let pair_id = PairID::new(Ticker::new("GOOG").unwrap(), Ticker::new("META").unwrap());
        assert_eq!(format!("{}", pair_id), "GOOG-META");
    }

    #[test]
    fn test_pair_id_clone_equality() {
        let pair_id = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        let cloned = pair_id.clone();
        assert_eq!(pair_id, cloned);
    }

    #[test]
    fn test_pair_id_hash_and_eq() {
        use std::collections::HashSet;
        let a = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        let b = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_pair_id_different_order_not_equal() {
        let ab = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        let ba = PairID::new(Ticker::new("MSFT").unwrap(), Ticker::new("AAPL").unwrap());
        assert_ne!(ab, ba);
    }

    #[test]
    fn test_pair_id_parse_simple_tickers() {
        let pair_id = PairID::parse("AAPL-MSFT").unwrap();
        assert_eq!(pair_id.long().as_str(), "AAPL");
        assert_eq!(pair_id.short().as_str(), "MSFT");
        assert_eq!(pair_id.as_str(), "AAPL-MSFT");
    }

    #[test]
    fn test_pair_id_parse_dot_suffix_ticker_splits_on_first_dash_only() {
        // BRK.B contains a dot but no dash; the only dash is the pair separator.
        let pair_id = PairID::parse("BRK.B-MSFT").unwrap();
        assert_eq!(pair_id.long().as_str(), "BRK.B");
        assert_eq!(pair_id.short().as_str(), "MSFT");
        assert_eq!(pair_id.as_str(), "BRK.B-MSFT");
    }

    #[test]
    fn test_pair_id_parse_rejects_no_dash() {
        assert!(PairID::parse("AAPLMSFT").is_none());
    }

    #[test]
    fn test_pair_id_parse_rejects_invalid_long_ticker() {
        // "TOOLONG" has six characters — invalid ticker.
        assert!(PairID::parse("TOOLONG-MSFT").is_none());
    }

    #[test]
    fn test_pair_id_parse_rejects_invalid_short_ticker() {
        assert!(PairID::parse("AAPL-TOOLONG").is_none());
    }

    #[test]
    fn test_pair_id_parse_rejects_empty_string() {
        assert!(PairID::parse("").is_none());
    }

    #[test]
    fn test_pair_id_parse_rejects_only_dash() {
        assert!(PairID::parse("-").is_none());
    }

    #[test]
    fn test_pair_id_serde_round_trip() {
        let pair_id = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        let serialized = serde_json::to_string(&pair_id).unwrap();
        assert_eq!(serialized, "\"AAPL-MSFT\"");
        let deserialized: PairID = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, pair_id);
    }

    #[test]
    fn test_pair_id_deserialize_dot_suffix_round_trip() {
        let raw = "\"BRK.B-MSFT\"";
        let pair_id: PairID = serde_json::from_str(raw).unwrap();
        assert_eq!(pair_id.long().as_str(), "BRK.B");
        assert_eq!(pair_id.short().as_str(), "MSFT");
    }

    #[test]
    fn test_pair_id_deserialize_rejects_invalid() {
        let result: Result<PairID, _> = serde_json::from_str("\"AAPLMSFT\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_pair_id_deserialize_rejects_malformed_ticker() {
        let result: Result<PairID, _> = serde_json::from_str("\"TOOLONG-MSFT\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_pair_id_as_hash_map_key() {
        use std::collections::HashMap;
        let key = PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap());
        let mut map: HashMap<PairID, i32> = HashMap::new();
        map.insert(key.clone(), 42);
        assert_eq!(map.get(&key), Some(&42));
    }

    // --- BookQualityLimits / UsableQuote ---

    fn book(bid: f64, ask: f64, bid_size: i32, ask_size: i32) -> EquityQuote {
        EquityQuote::new(
            Utc::now(),
            Ticker::new("AAPL").expect("valid ticker"),
            bid,
            ask,
            bid_size,
            ask_size,
        )
    }

    /// Limits that admit any width, isolating the price and size rules.
    fn any_width() -> BookQualityLimits {
        BookQualityLimits::new(f64::MAX, 0).expect("valid limits")
    }

    #[test]
    fn test_book_quality_limits_reject_non_positive_spread() {
        assert_eq!(
            BookQualityLimits::new(0.0, 100).unwrap_err(),
            InvalidBookQualityLimits
        );
        assert_eq!(
            BookQualityLimits::new(-0.01, 100).unwrap_err(),
            InvalidBookQualityLimits
        );
        assert_eq!(
            BookQualityLimits::new(f64::NAN, 100).unwrap_err(),
            InvalidBookQualityLimits
        );
    }

    #[test]
    fn test_book_quality_limits_reject_negative_size() {
        assert_eq!(
            BookQualityLimits::new(0.0025, -1).unwrap_err(),
            InvalidBookQualityLimits
        );
    }

    #[test]
    fn test_book_quality_limits_default_matches_constants() {
        let limits = BookQualityLimits::default();
        assert_eq!(limits.maximum_relative_spread(), MAXIMUM_RELATIVE_SPREAD);
        assert_eq!(limits.minimum_size(), MINIMUM_QUOTE_SIZE);
    }

    #[test]
    fn test_usable_quote_accepts_tight_book() {
        let quote = UsableQuote::new(&book(180.0, 180.2, 100, 100), BookQualityLimits::default())
            .expect("tight book is usable");
        assert!((quote.mid_price() - 180.1).abs() < 1e-9);
        // (180.2 - 180.0) / 180.1 = 11.1 basis points.
        assert!((quote.relative_spread() * 10_000.0 - 11.105).abs() < 0.01);
    }

    #[test]
    fn test_usable_quote_accepts_touching_book() {
        // Bid equal to ask is valid, not crossed, and has zero spread.
        let quote = UsableQuote::new(&book(180.0, 180.0, 100, 100), BookQualityLimits::default())
            .expect("touching book is usable");
        assert_eq!(quote.mid_price(), 180.0);
        assert_eq!(quote.relative_spread(), 0.0);
    }

    #[test]
    fn test_usable_quote_validates_each_side_independently() {
        // A one-sided book averages to a plausible-looking positive number.
        // Validating only the midpoint would accept 100.0 for a symbol with no
        // bid, and that value would feed every spread the leg appears in.
        assert!(UsableQuote::new(&book(0.0, 200.0, 100, 100), any_width()).is_none());
        assert!(UsableQuote::new(&book(200.0, 0.0, 100, 100), any_width()).is_none());
        assert!(UsableQuote::new(&book(-1.0, 200.0, 100, 100), any_width()).is_none());
        assert!(UsableQuote::new(&book(f64::NAN, 180.0, 100, 100), any_width()).is_none());
        assert!(UsableQuote::new(&book(180.0, f64::INFINITY, 100, 100), any_width()).is_none());
    }

    #[test]
    fn test_usable_quote_rejects_crossed_book() {
        assert!(UsableQuote::new(&book(181.0, 180.0, 100, 100), any_width()).is_none());
    }

    #[test]
    fn test_usable_quote_rejects_book_wider_than_limit() {
        // 180.0 / 200.0 spans 1,053 basis points against a 25 basis point bound.
        assert!(
            UsableQuote::new(&book(180.0, 200.0, 100, 100), BookQualityLimits::default()).is_none()
        );
    }

    #[test]
    fn test_usable_quote_accepts_book_exactly_at_spread_limit() {
        // Boundary is inclusive: rejection requires exceeding the limit.
        let limits = BookQualityLimits::new(0.01, 0).expect("valid limits");
        let quote = UsableQuote::new(&book(99.5, 100.5, 0, 0), limits)
            .expect("a book exactly at the limit is usable");
        assert!((quote.relative_spread() - 0.01).abs() < 1e-12);
    }

    #[test]
    fn test_usable_quote_rejects_undersized_side() {
        let limits = BookQualityLimits::new(1.0, 100).expect("valid limits");
        assert!(UsableQuote::new(&book(180.0, 180.2, 1, 100), limits).is_none());
        assert!(UsableQuote::new(&book(180.0, 180.2, 100, 1), limits).is_none());
        assert!(UsableQuote::new(&book(180.0, 180.2, 100, 100), limits).is_some());
    }

    #[test]
    fn test_usable_quote_preserves_observation_time() {
        let observed_at = Utc::now();
        let quote = EquityQuote::new(
            observed_at,
            Ticker::new("AAPL").expect("valid ticker"),
            180.0,
            180.2,
            100,
            100,
        );
        let usable = UsableQuote::new(&quote, BookQualityLimits::default()).expect("usable");
        assert_eq!(usable.observed_at(), observed_at);
    }
}
