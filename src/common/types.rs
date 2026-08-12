//! The domain vocabulary every module speaks.
//!
//! **Validated primitives** wrap standard Rust types so a unit mismatch is a compile error and
//! sign and range constraints are enforced by the constructor. **Record types** mirror the
//! PostgreSQL tables and Alpaca payloads.
//!
//! Both keep fields private and validate in the constructor, so a value in scope is proof its
//! invariants held and downstream code never re-checks.

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Liquidity thresholds defining the modeled and served equity universe.
///
/// Training applies them per row and inference per ticker average; both sides must use the same
/// values, or the scaler and model learn dynamics the service never predicts.
///
/// The *comparison* has to match too, and matching values are not enough on their own. All three
/// readers — `filter_training_bars`, `filter_equity_bars`, and `LiquidityRow::is_liquid` — admit the
/// threshold itself, so a ticker averaging exactly $10.00 is in the universe, in the training set,
/// and predicted for. Each has a boundary test, because this is the kind of skew that costs one
/// character and shows up only at the edge of the population.
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
// SessionDate
// ---------------------------------------------------------------------------

/// A trading day, identified by its `America/New_York` calendar date.
///
/// The type exists to make a session and an instant impossible to confuse. Both used to be spelled
/// `NaiveDate`/`DateTime`, and every timekeeping bug this system has had was that confusion: a
/// session derived from `Utc::now().date_naive()`, a prediction stamped at UTC midnight, a session
/// and its label computed from two separate expressions. None of those is expressible here.
///
/// There are exactly two ways in. [`SessionDate::at`] converts an instant, which is the only
/// correct way to answer "what trading day is it now". [`SessionDate::from_date`] takes a date
/// already expressed in Eastern terms — parsed from a command-line argument, read from a `DATE`
/// column, or returned by an exchange API that publishes Eastern dates. A `NaiveDate` obtained any
/// other way, and a UTC date in particular, has no business becoming one of these.
///
/// **What it guarantees is the timezone, not tradability.** A `SessionDate` is a calendar date in
/// the frame the exchange keeps; it is not proof that the market opens that day. Weekends and
/// holidays are perfectly representable, and [`SessionDate::plus_calendar_days`] will land on them.
/// Whether a date trades is [`crate::data::calendar::TradingCalendar::is_trading_day`]'s question,
/// and it is the only thing that can answer it — the calendar is fetched from Alpaca, and a holiday
/// table in the type system would be a second source that can disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SessionDate(NaiveDate);

impl SessionDate {
    /// The trading day an instant falls in.
    ///
    /// The trading day rolls over at Eastern midnight, not UTC midnight, and the two differ for
    /// four or five hours of every day. Every cache key, session comparison, and "is this still
    /// today" check in the system goes through here.
    pub fn at(instant: DateTime<Utc>) -> Self {
        Self(instant.with_timezone(&New_York).date_naive())
    }

    /// Wraps a date that is already known to be a session.
    ///
    /// For values that arrive as session dates rather than being derived from a clock: a parsed
    /// argument, a `DATE` column, an exchange calendar entry. Deriving one from an instant is
    /// [`SessionDate::at`]'s job, and going through `date_naive()` to reach here is the bug this
    /// type prevents.
    pub fn from_date(date: NaiveDate) -> Self {
        Self(date)
    }

    /// The underlying calendar date, for formatting and for binding to a `DATE` column.
    pub fn date(self) -> NaiveDate {
        self.0
    }

    /// Midnight Eastern on this session, as the equivalent UTC instant.
    ///
    /// Eastern shifts at 02:00 so local midnight always exists, but `earliest()` with a UTC
    /// fallback is used anyway so a timezone database change cannot panic a caller.
    pub fn midnight(self) -> DateTime<Utc> {
        let local_midnight = self
            .0
            .and_hms_opt(0, 0, 0)
            .expect("midnight is a valid wall-clock time");
        New_York
            .from_local_datetime(&local_midnight)
            .earliest()
            .map(|zoned| zoned.with_timezone(&Utc))
            .unwrap_or_else(|| local_midnight.and_utc())
    }

    /// The half-open UTC interval `[start, end)` covering this session.
    ///
    /// The inverse of [`SessionDate::at`], so queries can bound a timestamp column directly. A
    /// predicate like `(created_at AT TIME ZONE 'America/New_York')::date = $1` hides the column
    /// behind an expression, defeating chunk exclusion and the index — a one-day export becomes a
    /// full scan. Resolving bounds here keeps `column >= $start AND column < $end` sargable.
    pub fn bounds(self) -> (DateTime<Utc>, DateTime<Utc>) {
        (self.midnight(), self.plus_calendar_days(1).midnight())
    }

    /// This session shifted by whole **calendar** days.
    ///
    /// Named for the distinction it must not lose: this steps over weekends and holidays, unlike
    /// [`crate::data::calendar::TradingCalendar::previous_trading_day`] and
    /// [`crate::data::calendar::TradingCalendar::next_trading_day`], which land on published
    /// sessions. Used to bound fetch ranges, where overshooting is free.
    pub fn plus_calendar_days(self, days: i64) -> Self {
        Self(self.0 + Duration::days(days))
    }

    /// Whether this date falls on a weekend.
    ///
    /// Not a substitute for [`crate::data::calendar::TradingCalendar::is_trading_day`] — it knows
    /// nothing about holidays — but useful for bounding a fetch range before the calendar is
    /// available.
    pub fn is_weekend(self) -> bool {
        matches!(
            self.0.weekday(),
            chrono::Weekday::Sat | chrono::Weekday::Sun
        )
    }
}

impl std::fmt::Display for SessionDate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

// ---------------------------------------------------------------------------
// Ticker
// ---------------------------------------------------------------------------

/// A normalized US equity ticker symbol.
///
/// Enforces the Alpaca US equity ticker format: 1–5 uppercase ASCII letters, with an optional
/// dot-separated suffix of 1–3 for share class or warrant notation (e.g. `BRK.B`, `BRK.WS`).
///
/// Reference: <https://docs.alpaca.markets/us/reference/get-v2-assets-1>
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
/// Part of the `equity_bars` primary key. The post-close sync writes only
/// [`BarInterval::OneDay`]; [`BarInterval::OneMinute`] is equally permitted by the CHECK constraint
/// and is what `fetch_snapshots` tags Alpaca's `minuteBar` with in memory, so "daily only" is a
/// property of the current writer rather than of the table.
///
/// [`BarInterval::as_str`] must match the `bar_interval` CHECK constraint exactly. It is the
/// snake_case of the variant name, which lets `rename_all` derive the same string for serde so what
/// is serialized and what is stored cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BarInterval {
    OneMinute,
    OneDay,
}

impl BarInterval {
    /// Every variant, for exhaustive iteration in tests and validation.
    pub const ALL: [BarInterval; 2] = [BarInterval::OneMinute, BarInterval::OneDay];

    /// The canonical stored form, matching the `bar_interval` CHECK constraint.
    ///
    /// Must stay identical to what the `rename_all` derive produces; the round-trip test below is
    /// what enforces that.
    pub fn as_str(self) -> &'static str {
        match self {
            BarInterval::OneMinute => "one_minute",
            BarInterval::OneDay => "one_day",
        }
    }

    /// Parses the canonical stored form. Returns `None` for anything else, including Alpaca's
    /// `1Day`/`1Min` timeframe spelling — that vocabulary belonged to a bars endpoint this
    /// codebase no longer calls, and accepting it here would let it back in unnoticed.
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

/// Error returned when a market data record's fields are not internally consistent.
///
/// Carries the reason rather than a bare unit so a rejected Alpaca payload can be logged with
/// enough detail to tell a provider problem from a parsing one.
#[derive(Debug, Clone, PartialEq)]
pub struct InconsistentRecordError {
    pub reason: String,
}

impl std::fmt::Display for InconsistentRecordError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Inconsistent market data record: {}.",
            self.reason
        )
    }
}

impl std::error::Error for InconsistentRecordError {}

fn reject(reason: impl Into<String>) -> InconsistentRecordError {
    InconsistentRecordError {
        reason: reason.into(),
    }
}

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
    /// Constructs an `EquityBar`, rejecting a bar whose prices do not form a coherent candle.
    ///
    /// The invariants are the ones every consumer silently assumes: prices are finite and positive,
    /// the low is the low, the high is the high, and the open and close fall between them. A bar
    /// violating any of these reaches the liquidity average, the correlation screen, and the model
    /// input without complaint, and produces plausible-looking nonsense rather than an error — so
    /// the boundary where it enters the system is the last cheap place to stop it.
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
    ) -> Result<Self, InconsistentRecordError> {
        for (name, price) in [
            ("open", open_price),
            ("high", high_price),
            ("low", low_price),
            ("close", close_price),
        ] {
            if !price.is_finite() || price <= 0.0 {
                return Err(reject(format!(
                    "{name} price {price} is not a positive number"
                )));
            }
        }
        if low_price > high_price {
            return Err(reject(format!(
                "low price {low_price} exceeds high price {high_price}"
            )));
        }
        if open_price < low_price || open_price > high_price {
            return Err(reject(format!(
                "open price {open_price} is outside [{low_price}, {high_price}]"
            )));
        }
        if close_price < low_price || close_price > high_price {
            return Err(reject(format!(
                "close price {close_price} is outside [{low_price}, {high_price}]"
            )));
        }
        if volume < 0 {
            return Err(reject(format!("volume {volume} is negative")));
        }

        Ok(Self {
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
        })
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
    /// Constructs an `EquityQuote`, rejecting a book that cannot be meaningfully averaged.
    ///
    /// [`EquityQuote::mid_price`] is arithmetic with no opinion about its inputs, so the opinion
    /// lives here. A crossed book or a zero side yields a midpoint that looks like a price and is
    /// not one — a zero bid against a hundred-dollar ask gives a fifty-dollar mid, and that reaches
    /// an order. A locked book (bid equal to ask) is legal and accepted.
    pub fn new(
        ticker: Ticker,
        timestamp: DateTime<Utc>,
        bid_price: f64,
        ask_price: f64,
        bid_size: i32,
        ask_size: i32,
    ) -> Result<Self, InconsistentRecordError> {
        for (name, price) in [("bid", bid_price), ("ask", ask_price)] {
            if !price.is_finite() || price <= 0.0 {
                return Err(reject(format!(
                    "{name} price {price} is not a positive number"
                )));
            }
        }
        if bid_price > ask_price {
            return Err(reject(format!(
                "book is crossed: bid {bid_price} exceeds ask {ask_price}"
            )));
        }
        if bid_size < 0 || ask_size < 0 {
            return Err(reject(format!(
                "quoted sizes must be non-negative, got bid {bid_size} and ask {ask_size}"
            )));
        }

        Ok(Self {
            ticker,
            timestamp,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
        })
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

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    /// The Eastern date must roll at Eastern midnight. 03:00 UTC is still the previous evening in
    /// New York, and a UTC-based key would put it on the wrong trading day.
    #[test]
    fn test_eastern_date_rolls_at_eastern_midnight() {
        let late_evening = "2026-06-11T03:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(SessionDate::at(late_evening), session(2026, 6, 10));

        let morning = "2026-06-11T13:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(SessionDate::at(morning), session(2026, 6, 11));
    }

    /// The bounds must be half-open and must span exactly 24 hours on an ordinary day. In summer
    /// Eastern is UTC-4, so the day runs 04:00 to 04:00 UTC.
    #[test]
    fn test_eastern_day_bounds_span_the_local_day_in_summer() {
        let (start, end) = session(2026, 6, 10).bounds();
        assert_eq!(start.to_rfc3339(), "2026-06-10T04:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2026-06-11T04:00:00+00:00");
        assert_eq!((end - start).num_hours(), 24);
    }

    /// In winter Eastern is UTC-5, so the same local day is offset by an hour. A fixed offset would
    /// get one of these two cases wrong.
    #[test]
    fn test_eastern_day_bounds_span_the_local_day_in_winter() {
        let (start, end) = session(2026, 1, 14).bounds();
        assert_eq!(start.to_rfc3339(), "2026-01-14T05:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2026-01-15T05:00:00+00:00");
    }

    /// The transition days are 23 and 25 hours long. Bounds computed by adding a fixed 24 hours
    /// would silently include or exclude an hour of rows on exactly these two days a year.
    #[test]
    fn test_eastern_day_bounds_handle_daylight_saving_transitions() {
        let (spring_start, spring_end) = session(2026, 3, 8).bounds();
        assert_eq!(
            (spring_end - spring_start).num_hours(),
            23,
            "spring forward is a 23-hour day"
        );

        let (autumn_start, autumn_end) = session(2026, 11, 1).bounds();
        assert_eq!(
            (autumn_end - autumn_start).num_hours(),
            25,
            "fall back is a 25-hour day"
        );
    }

    /// The bounds must round-trip against [`SessionDate::at`]: every instant inside them is that
    /// Eastern date, and the exclusive end already belongs to the next one.
    #[test]
    fn test_eastern_day_bounds_round_trip_against_eastern_date() {
        let day = session(2026, 6, 10);
        let (start, end) = day.bounds();
        assert_eq!(SessionDate::at(start), day);
        assert_eq!(SessionDate::at(end - Duration::seconds(1)), day);
        assert_eq!(SessionDate::at(end), day.plus_calendar_days(1));
    }

    #[test]
    fn test_is_weekend() {
        assert!(session(2026, 11, 28).is_weekend());
        assert!(session(2026, 11, 29).is_weekend());
        assert!(!session(2026, 11, 27).is_weekend());
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

    /// These exact strings are the `bar_interval` CHECK constraint in `schema.sql`. Changing one
    /// without the other makes every insert fail at runtime, which no compile-time check catches —
    /// the column is TEXT, so sqlx sees a string either way.
    #[test]
    fn test_bar_interval_stored_form_matches_the_check_constraint() {
        assert_eq!(BarInterval::OneDay.as_str(), "one_day");
        assert_eq!(BarInterval::OneMinute.as_str(), "one_minute");
    }

    /// The serde derive and the stored form must produce the same string. They did not before the
    /// `rename_all` attribute: the derive emitted `OneDay` while `parse` accepted only the stored
    /// form, so an `EquityBar` serialized to JSON could not be deserialized back. Nothing failed
    /// loudly because no live path did that round trip — this is what stops one being added.
    #[test]
    fn test_bar_interval_serde_agrees_with_the_stored_form() {
        for interval in BarInterval::ALL {
            let encoded = serde_json::to_string(&interval).expect("interval must serialize");
            assert_eq!(encoded, format!("\"{}\"", interval.as_str()));
            let decoded: BarInterval =
                serde_json::from_str(&encoded).expect("what serde writes, serde must read");
            assert_eq!(decoded, interval);
        }
    }

    /// Neither Alpaca's timeframe spelling nor either pre-rename stored form is accepted. The last
    /// two matter most: `1day` was the stored value before this vocabulary changed, and silently
    /// parsing it would let a stale writer put unreadable rows in the table.
    #[test]
    fn test_bar_interval_parse_rejects_foreign_spellings() {
        assert!(BarInterval::parse("1Day").is_none());
        assert!(BarInterval::parse("1Hour").is_none());
        assert!(BarInterval::parse("OneDay").is_none());
        assert!(BarInterval::parse("1day").is_none());
        assert!(BarInterval::parse("1_day").is_none());
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

    fn bar(
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: i64,
    ) -> Result<EquityBar, InconsistentRecordError> {
        EquityBar::new(
            ticker("AAPL"),
            BarInterval::OneDay,
            Utc::now(),
            open,
            high,
            low,
            close,
            volume,
            None,
            None,
        )
    }

    #[test]
    fn test_bar_accepts_a_coherent_candle() {
        let bar_value = bar(100.0, 105.0, 99.0, 103.0, 1_000).expect("coherent candle constructs");
        assert_eq!(bar_value.close_price(), 103.0);
        // A doji, where every price is equal, is legitimate.
        assert!(bar(100.0, 100.0, 100.0, 100.0, 0).is_ok());
    }

    /// Each invariant must bind on its own. An inverted range, an open or close outside it, and a
    /// negative volume are separate provider faults that all reach the screen looking plausible.
    #[test]
    fn test_bar_rejects_an_incoherent_candle() {
        assert!(
            bar(100.0, 99.0, 105.0, 100.0, 10).is_err(),
            "low above high"
        );
        assert!(
            bar(200.0, 105.0, 99.0, 103.0, 10).is_err(),
            "open above high"
        );
        assert!(
            bar(100.0, 105.0, 99.0, 50.0, 10).is_err(),
            "close below low"
        );
        assert!(
            bar(100.0, 105.0, 99.0, 103.0, -1).is_err(),
            "negative volume"
        );
    }

    #[test]
    fn test_bar_rejects_non_positive_and_non_finite_prices() {
        assert!(bar(0.0, 105.0, 0.0, 103.0, 10).is_err());
        assert!(bar(-1.0, 105.0, -2.0, 103.0, 10).is_err());
        assert!(bar(f64::NAN, 105.0, 99.0, 103.0, 10).is_err());
        assert!(bar(100.0, f64::INFINITY, 99.0, 103.0, 10).is_err());
    }

    fn quote(
        bid: f64,
        ask: f64,
        bid_size: i32,
        ask_size: i32,
    ) -> Result<EquityQuote, InconsistentRecordError> {
        EquityQuote::new(ticker("AAPL"), Utc::now(), bid, ask, bid_size, ask_size)
    }

    #[test]
    fn test_quote_accepts_a_normal_and_a_locked_book() {
        assert_eq!(quote(100.0, 102.0, 5, 5).unwrap().mid_price(), 101.0);
        assert!(quote(100.0, 100.0, 1, 1).is_ok(), "a locked book is legal");
    }

    /// This is why `mid_price` can be plain arithmetic. A zero bid against a real ask yields a
    /// midpoint that looks like a price and is not one, and that number would reach an order.
    #[test]
    fn test_quote_rejects_a_book_that_cannot_be_averaged() {
        assert!(quote(0.0, 100.0, 5, 5).is_err(), "zero bid");
        assert!(quote(103.0, 102.0, 5, 5).is_err(), "crossed book");
        assert!(quote(f64::NAN, 102.0, 5, 5).is_err(), "non-finite bid");
        assert!(quote(100.0, 102.0, -1, 5).is_err(), "negative size");
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
