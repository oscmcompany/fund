//! The domain vocabulary every module speaks.
//!
//! Fields are private and constructors validate, so a value in scope is proof its invariants held.

use std::num::NonZeroU32;

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use rust_decimal::Decimal;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Which bound of a [`LiquidityFloor`] a name failed, and the reading that failed it.
///
/// A bound can only be moved from the readings it refused, so a refusal that says only "excluded"
/// cannot be calibrated against anything.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LiquidityRefusal {
    ClosePrice { observed: f64, minimum: f64 },
    DollarVolume { observed: f64, minimum: f64 },
}

impl LiquidityRefusal {
    /// The stable name this refusal is recorded under.
    pub fn as_str(&self) -> &'static str {
        match self {
            LiquidityRefusal::ClosePrice { .. } => "close_price",
            LiquidityRefusal::DollarVolume { .. } => "dollar_volume",
        }
    }

    /// The reading and the bound it failed.
    pub fn detail(&self) -> String {
        match self {
            LiquidityRefusal::ClosePrice { observed, minimum } => {
                format!("close_price={observed:.4} minimum={minimum:.4}")
            }
            LiquidityRefusal::DollarVolume { observed, minimum } => {
                format!("dollar_volume={observed:.2} minimum={minimum:.2}")
            }
        }
    }
}

impl std::fmt::Display for LiquidityRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail())
    }
}

/// The boundary of the modeled and served equity universe: the price and daily notional a name
/// must clear.
///
/// One value rather than two arguments, because the pair is meaningless apart and every screen
/// compares against both. Volume is counted in dollars traded, never in shares: a share count is a
/// liquidity measure divided by price, so it excludes expensive names that trade freely.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct LiquidityFloor {
    minimum_close_price: f64,
    minimum_dollar_volume: f64,
}

impl<'de> Deserialize<'de> for LiquidityFloor {
    /// Through [`LiquidityFloor::new`], so a stored floor that could not have been constructed is
    /// refused rather than read back into a value that claims to be valid.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Stored {
            minimum_close_price: f64,
            minimum_dollar_volume: f64,
        }
        let stored = Stored::deserialize(deserializer)?;
        LiquidityFloor::new(stored.minimum_close_price, stored.minimum_dollar_volume)
            .ok_or_else(|| de::Error::custom("a liquidity floor bound is negative or not finite"))
    }
}

impl LiquidityFloor {
    /// The floor every path uses until a caller declares its own.
    ///
    /// Both figures are conventions rather than measurements, and the notional one drifts with the
    /// market it screens: it admitted 20.6% of priced names in 2022 and 23.0% in 2026.
    pub const CURRENT: Self = Self {
        minimum_close_price: 10.0,
        minimum_dollar_volume: 50_000_000.0,
    };

    /// Returns `None` unless both bounds are finite and non-negative; zero admits everything.
    pub fn new(minimum_close_price: f64, minimum_dollar_volume: f64) -> Option<Self> {
        let usable = |bound: f64| bound.is_finite() && bound >= 0.0;
        (usable(minimum_close_price) && usable(minimum_dollar_volume)).then_some(Self {
            minimum_close_price,
            minimum_dollar_volume,
        })
    }

    /// Whether a name trading at `close_price` on `dollar_volume` is inside the universe.
    ///
    /// Both bounds are inclusive, and this is the only place that comparison is written. Price is
    /// tested first, so a name failing both is reported on its price.
    pub fn admits(&self, close_price: f64, dollar_volume: f64) -> Result<(), LiquidityRefusal> {
        // The `is_nan` half is load-bearing: a bare `<` admits a reading that fails every
        // comparison, which is how an unpriceable name reaches a screen that thinks it is liquid.
        if close_price.is_nan() || close_price < self.minimum_close_price {
            return Err(LiquidityRefusal::ClosePrice {
                observed: close_price,
                minimum: self.minimum_close_price,
            });
        }
        if dollar_volume.is_nan() || dollar_volume < self.minimum_dollar_volume {
            return Err(LiquidityRefusal::DollarVolume {
                observed: dollar_volume,
                minimum: self.minimum_dollar_volume,
            });
        }
        Ok(())
    }

    pub fn minimum_close_price(&self) -> f64 {
        self.minimum_close_price
    }

    pub fn minimum_dollar_volume(&self) -> f64 {
        self.minimum_dollar_volume
    }
}

impl std::fmt::Display for LiquidityFloor {
    /// Both bounds unscaled, because a floor that renders ambiguously is worse than one that reads
    /// awkwardly: abbreviating the notional to millions would print any floor under $1M as `$0M`.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "${} close on ${} traded",
            self.minimum_close_price, self.minimum_dollar_volume
        )
    }
}

/// The stretch of history a [`LiquidityFloor`] is applied over before a name is admitted.
///
/// Beside the floor rather than with the screens, because the pair is one decision: the same bounds
/// over a trailing month and over two years admit different sets, and the tree currently holds both
/// and calls each "the universe".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum ScreenWindow {
    /// The trailing calendar days the traded universe screens over.
    ///
    /// Unsigned because a negative window is not a narrower one: it puts the lower bound *after* the
    /// upper and selects nothing, which both readers would report as a name that went quiet.
    Trailing(NonZeroU32),
    /// Every session the frame holds, so a name that dipped once anywhere in it is refused.
    WholeFrame,
    /// [`ScreenWindow::Trailing`] re-anchored on every session, admitting a name *per session*.
    ///
    /// The other two admit a set of names; this admits a set of `(name, session)` pairs, which is
    /// what the live book actually does — it screens each morning and trades what clears that
    /// morning. A name that goes quiet leaves and comes back, where a single anchor has to choose
    /// between refusing its whole history and admitting it throughout.
    PerSession(NonZeroU32),
}

impl ScreenWindow {
    /// Whether this window admits a name for some sessions and not others.
    ///
    /// The distinction a caller has to respect: a per-ticker window leaves every admitted name's
    /// history intact, and a per-session one punches holes in it.
    pub const fn is_per_session(&self) -> bool {
        matches!(self, ScreenWindow::PerSession(_))
    }
}

impl std::fmt::Display for ScreenWindow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScreenWindow::Trailing(days) => write!(formatter, "trailing {days} days"),
            ScreenWindow::WholeFrame => write!(formatter, "the whole frame"),
            ScreenWindow::PerSession(days) => {
                write!(formatter, "trailing {days} days, re-anchored each session")
            }
        }
    }
}

/// A liquidity screen: the bounds, and the stretch of history they are measured over.
///
/// One value rather than two arguments, for the reason [`LiquidityFloor`] is one value rather than
/// two bounds — the pair is meaningless apart. The same bounds over a trailing month and over two
/// years admit different sets of names, so a caller that supplies only a floor has not said which
/// population it means, and three callers that each supply their own window produce three.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Screen {
    floor: LiquidityFloor,
    window: ScreenWindow,
}

impl Screen {
    pub const fn new(floor: LiquidityFloor, window: ScreenWindow) -> Self {
        Self { floor, window }
    }

    pub const fn floor(&self) -> LiquidityFloor {
        self.floor
    }

    pub const fn window(&self) -> ScreenWindow {
        self.window
    }
}

impl std::fmt::Display for Screen {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} over {}", self.floor, self.window)
    }
}

/// Serializes a [`Decimal`] as a JSON number rather than a quoted string.
///
/// `Decimal`'s own `Serialize` writes a string, which a reader has to cast before it can do
/// arithmetic; used through `#[serde(with = "...")]` on every field that reaches JSON. Routes
/// through [`Decimal::as_f64`], which returns an `f64` rather than an `Option`, so there is no
/// failure to handle and no unwrap in the journal write path.
pub mod decimal_number {
    use super::Decimal;
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_f64(value.as_f64())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
        let raw = f64::deserialize(deserializer)?;
        Decimal::try_from(raw).map_err(de::Error::custom)
    }
}

/// [`decimal_number`] for an optional amount, which stays `null` when absent.
pub mod decimal_number_option {
    use super::Decimal;
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Decimal>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(amount) => serializer.serialize_f64(amount.as_f64()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Decimal>, D::Error> {
        match Option::<f64>::deserialize(deserializer)? {
            Some(raw) => Decimal::try_from(raw).map(Some).map_err(de::Error::custom),
            None => Ok(None),
        }
    }
}

/// A PostgreSQL table the nightly export ships to S3.
///
/// The stored name and the S3 prefix travel together because they are one decision: a dataset
/// written under another's prefix is a silent overwrite, and neither half is checkable from the
/// other. [`crate::data::purge`] deletes from a subset of these, which is what makes deleting safe — a table it
/// could name but the export could not would have no copy anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dataset {
    Events,
    EquityPairs,
    AccountSnapshots,
    AccountActivities,
}

impl Dataset {
    /// Every dataset, which is what a stored name is read back against.
    pub const ALL: [Dataset; 4] = [
        Dataset::Events,
        Dataset::EquityPairs,
        Dataset::AccountSnapshots,
        Dataset::AccountActivities,
    ];

    /// The PostgreSQL table name, which is also how the dataset is reported.
    pub fn as_str(self) -> &'static str {
        match self {
            Dataset::Events => "events",
            Dataset::EquityPairs => "equity_pairs",
            Dataset::AccountSnapshots => "account_snapshots",
            Dataset::AccountActivities => "account_activities",
        }
    }

    /// The S3 prefix this dataset is written under.
    pub fn prefix(self) -> &'static str {
        match self {
            Dataset::Events => "exports/events",
            Dataset::EquityPairs => "exports/equity/pairs",
            Dataset::AccountSnapshots => "exports/account/snapshots",
            Dataset::AccountActivities => "exports/account/activities",
        }
    }
}

impl std::fmt::Display for Dataset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl serde::Serialize for Dataset {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// Reads a closed vocabulary back from the name it was written under, refusing any other name.
///
/// One function rather than one per type, so every recorded enum refuses an unknown spelling the
/// same way instead of each defaulting it differently.
pub fn deserialize_named<'de, D, T>(
    deserializer: D,
    kind: &str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse(&raw).ok_or_else(|| de::Error::custom(format!("unknown {kind}: {raw:?}")))
}

impl<'de> Deserialize<'de> for Dataset {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_named(deserializer, "dataset", |raw| {
            Dataset::ALL
                .into_iter()
                .find(|dataset| dataset.as_str() == raw)
        })
    }
}

impl<'de> Deserialize<'de> for CloseReason {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_named(deserializer, "close reason", CloseReason::parse)
    }
}

impl<'de> Deserialize<'de> for PairID {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_named(deserializer, "pair identifier", PairID::parse)
    }
}

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
pub struct Shares(#[serde(with = "decimal_number")] Decimal);

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
pub struct Dollars(#[serde(with = "decimal_number")] Decimal);

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

/// A trading day, identified by its `America/New_York` calendar date.
///
/// There are exactly two ways in: [`SessionDate::at`] converts an instant, and
/// [`SessionDate::from_date`] takes a date already expressed in Eastern terms. What it guarantees
/// is the timezone, not tradability — only
/// [`crate::data::calendar::TradingCalendar::is_trading_day`] can answer whether a date trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    /// [`crate::data::calendar::TradingCalendar::previous_trading_day`], which lands on a published
    /// session. Used to bound fetch ranges, where overshooting is free.
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

/// Which side of a pair an order opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairLeg {
    Long,
    Short,
}

impl PairLeg {
    pub const fn as_str(self) -> &'static str {
        match self {
            PairLeg::Long => "long",
            PairLeg::Short => "short",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        [PairLeg::Long, PairLeg::Short]
            .into_iter()
            .find(|leg| leg.as_str() == raw)
    }
}

/// The identifier an order is sent to Alpaca under.
///
/// The UUID keeps it unique across a pair opened, closed and reopened; the pair and leg make the
/// broker's own order history say which trade an order belongs to. An order sent before the pair
/// was encoded carries a bare UUID and parses with no pair, which is exactly what it records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOrderId {
    uuid: Uuid,
    pair: Option<(PairID, PairLeg)>,
}

/// Alpaca's limit, measured against the paper account on 2026-09-25: 128 accepted, 129 refused.
pub const CLIENT_ORDER_ID_MAXIMUM_LENGTH: usize = 128;

/// The longest identifier the format can produce: two nine-character tickers, the longer leg name,
/// and a hyphenated UUID, which must fit Alpaca's limit for every pair the ticker format admits.
const _: () = assert!(
    "pair:".len() + 9 + "-".len() + 9 + ":short:".len() + 36 <= CLIENT_ORDER_ID_MAXIMUM_LENGTH
);

impl ClientOrderId {
    /// A fresh identifier for one leg of a pair.
    pub fn for_pair(pair: &PairID, leg: PairLeg) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            pair: Some((pair.clone(), leg)),
        }
    }

    /// Reads either form Alpaca may hand back; `None` for anything this system did not send.
    ///
    /// Only the exact spelling this system writes is accepted: the parsers beneath accept braced or
    /// uppercase UUIDs and lowercase tickers, and a hand-placed order in one of those must not read
    /// as ours.
    pub fn parse(raw: &str) -> Option<Self> {
        Self::parse_any_spelling(raw).filter(|parsed| parsed.to_string() == raw)
    }

    fn parse_any_spelling(raw: &str) -> Option<Self> {
        if let Ok(uuid) = Uuid::parse_str(raw) {
            return Some(Self { uuid, pair: None });
        }
        let mut fields = raw.split(':');
        let (Some("pair"), Some(pair), Some(leg), Some(uuid), None) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            return None;
        };
        Some(Self {
            uuid: Uuid::parse_str(uuid).ok()?,
            pair: Some((PairID::parse(pair)?, PairLeg::parse(leg)?)),
        })
    }

    /// The part the journal keys an order by.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// The pair and leg this order opens, where it was sent with them.
    pub fn pair(&self) -> Option<(&PairID, PairLeg)> {
        self.pair.as_ref().map(|(pair, leg)| (pair, *leg))
    }
}

impl std::fmt::Display for ClientOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.pair {
            Some((pair, leg)) => write!(formatter, "pair:{pair}:{}:{}", leg.as_str(), self.uuid),
            None => write!(formatter, "{}", self.uuid),
        }
    }
}

/// The sampling interval of an OHLCV bar.
///
/// Part of the `equity_bars` primary key, and every variant has a writer.
///
/// [`BarInterval::as_str`] must match the `bar_interval` CHECK constraint exactly. It is the
/// snake_case of the variant name, which lets `rename_all` derive the same string for serde so what
/// is serialized and what is stored cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BarInterval {
    OneMinute,
    FiveMinute,
    OneDay,
}

impl BarInterval {
    /// Every variant, for exhaustive iteration in tests and validation.
    pub const ALL: [BarInterval; 3] = [
        BarInterval::OneMinute,
        BarInterval::FiveMinute,
        BarInterval::OneDay,
    ];

    /// The canonical stored form, matching the `bar_interval` CHECK constraint.
    ///
    /// Must stay identical to what the `rename_all` derive produces; the round-trip test below is
    /// what enforces that.
    pub fn as_str(self) -> &'static str {
        match self {
            BarInterval::OneMinute => "one_minute",
            BarInterval::FiveMinute => "five_minute",
            BarInterval::OneDay => "one_day",
        }
    }

    /// The multiplier and timespan Massive's aggregates route spells this interval with.
    ///
    /// Separate from [`BarInterval::as_str`] because the two vocabularies answer to different
    /// owners: one is the stored form a CHECK constraint pins, the other is a vendor's URL.
    pub fn massive_timespan(self) -> (u32, &'static str) {
        match self {
            BarInterval::OneMinute => (1, "minute"),
            BarInterval::FiveMinute => (5, "minute"),
            BarInterval::OneDay => (1, "day"),
        }
    }

    /// Parses the canonical stored form. Returns `None` for anything else, including Alpaca's
    /// `1Day`/`1Min` timeframe spelling, which is not a form this table stores.
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

/// Which SIP disseminated a print, which is what decides how its conditions are spelled.
///
/// The same condition carries a different character on each tape — an average price trade is `B` on
/// CTA and `W` on UTP — so a character cannot be read without knowing which one published it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tape {
    /// Tapes A and B: NYSE and NYSE-listed issues.
    ConsolidatedTapeAssociation,
    /// Tape C: Nasdaq-listed issues.
    UnlistedTradingPrivileges,
    /// FINRA's off-exchange feed, which carries the TRF prints.
    TradeDataDissemination,
}

impl Tape {
    /// Every variant, for exhaustive iteration in tests and validation.
    pub const ALL: [Tape; 3] = [
        Tape::ConsolidatedTapeAssociation,
        Tape::UnlistedTradingPrivileges,
        Tape::TradeDataDissemination,
    ];

    /// The tape a provider's numeric marker names, or `None` for a value no SIP publishes.
    ///
    /// `1` and `2` are both CTA — tape A is NYSE-listed and tape B is regional, and they share a
    /// condition vocabulary, so the distinction does not survive into this type.
    pub fn from_marker(marker: u8) -> Option<Self> {
        match marker {
            1 | 2 => Some(Tape::ConsolidatedTapeAssociation),
            3 => Some(Tape::UnlistedTradingPrivileges),
            _ => None,
        }
    }

    /// The tape Alpaca's letter names, which is the same three SIPs spelled differently.
    ///
    /// `A` and `B` are both CTA for the reason `1` and `2` are. There is no letter for FINRA: a TRF
    /// print is disseminated on the tape of its listing venue, so it arrives as `A`, `B` or `C`.
    pub fn from_letter(letter: u8) -> Option<Self> {
        match letter {
            b'A' | b'B' => Some(Tape::ConsolidatedTapeAssociation),
            b'C' => Some(Tape::UnlistedTradingPrivileges),
            _ => None,
        }
    }
}

impl std::fmt::Display for Tape {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Tape::ConsolidatedTapeAssociation => "CTA",
            Tape::UnlistedTradingPrivileges => "UTP",
            Tape::TradeDataDissemination => "FINRA_TDDS",
        })
    }
}

/// How a provider spells the sale conditions on a print.
///
/// Massive publishes identifiers, which name a condition outright, and Alpaca publishes the SIP
/// characters, which name one only alongside the tape that carried them — the same condition is `B`
/// on CTA and `W` on UTP. Carrying the spelling beside the values is what stops a character being
/// read against the wrong table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeConditions {
    /// The provider's own identifiers.
    Identified(Vec<u32>),
    /// SIP characters, readable only against the tape that published them.
    Spelled { characters: Vec<u8>, tape: Tape },
}

impl TradeConditions {
    /// Stands in for a token no condition table can spell, so it resolves as unknown.
    ///
    /// A SIP condition is one character, and `0xFF` is not one: it is outside ASCII, so no row can
    /// claim it and every lookup answers empty. A malformed token therefore reads exactly like an
    /// unrecognized character rather than vanishing and leaving the print looking ordinary.
    pub const UNSPELLABLE: u8 = 0xFF;

    /// Whether anything is spelled here at all, which an unconditioned print is.
    pub fn is_empty(&self) -> bool {
        match self {
            TradeConditions::Identified(identifiers) => identifiers.is_empty(),
            TradeConditions::Spelled { characters, .. } => characters.is_empty(),
        }
    }
}

/// The grid a session is folded onto, which is the two [`BarInterval`] values that name a bucket.
///
/// Distinct from [`BarInterval`] so a fold cannot be opened at [`BarInterval::OneDay`], whose bucket
/// is the session itself: the daily row is the merge of the intraday ones, never a grid of one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IntradayCadence {
    OneMinute,
    FiveMinute,
}

impl IntradayCadence {
    /// Every variant, for exhaustive iteration in tests and validation.
    pub const ALL: [IntradayCadence; 2] = [IntradayCadence::OneMinute, IntradayCadence::FiveMinute];

    /// Seconds in one bucket.
    pub fn seconds(self) -> i64 {
        match self {
            IntradayCadence::OneMinute => 60,
            IntradayCadence::FiveMinute => 300,
        }
    }

    /// The interval the rows folded at this cadence are stored under.
    pub fn bar_interval(self) -> BarInterval {
        match self {
            IntradayCadence::OneMinute => BarInterval::OneMinute,
            IntradayCadence::FiveMinute => BarInterval::FiveMinute,
        }
    }

    /// The cadence an interval names, or `None` for [`BarInterval::OneDay`].
    pub fn from_bar_interval(interval: BarInterval) -> Option<Self> {
        match interval {
            BarInterval::OneMinute => Some(IntradayCadence::OneMinute),
            BarInterval::FiveMinute => Some(IntradayCadence::FiveMinute),
            BarInterval::OneDay => None,
        }
    }
}

impl std::fmt::Display for IntradayCadence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.bar_interval().as_str())
    }
}

/// Why an open pair was closed.
///
/// These four are the `close_reason` CHECK constraint in `schema.sql`, spelled as an enum so a
/// reason the database would reject cannot be constructed in the first place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// The spread returned through its mean. The trade worked.
    Convergence,
    /// The spread widened past the stop, against the position. The trade did not.
    StopLoss,
    /// The pre-close fail-safe flattened the book. No opinion either way.
    EndOfDay,
    /// Alpaca no longer reports a position for one or both legs, so there is nothing left to hold.
    PositionMissing,
}

impl CloseReason {
    pub const ALL: [CloseReason; 4] = [
        CloseReason::Convergence,
        CloseReason::StopLoss,
        CloseReason::EndOfDay,
        CloseReason::PositionMissing,
    ];

    /// The stored form, which must match the CHECK constraint exactly.
    pub fn as_str(self) -> &'static str {
        match self {
            CloseReason::Convergence => "convergence",
            CloseReason::StopLoss => "stop_loss",
            CloseReason::EndOfDay => "end_of_day",
            CloseReason::PositionMissing => "position_missing",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        CloseReason::ALL
            .into_iter()
            .find(|reason| reason.as_str() == raw)
    }

    /// Whether this exit reflects the strategy's own opinion about the spread.
    ///
    /// A book that only ever exits at [`CloseReason::EndOfDay`] is one whose holding period is
    /// shorter than the horizon it forecasts. The ratio of these two answers is the interim measure
    /// of whether the strategy is doing anything, so the distinction is worth naming rather than
    /// recomputing from a string at each call site.
    pub fn is_signal(self) -> bool {
        match self {
            CloseReason::Convergence | CloseReason::StopLoss => true,
            CloseReason::EndOfDay | CloseReason::PositionMissing => false,
        }
    }
}

impl std::fmt::Display for CloseReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for CloseReason {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

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

/// A rate in hundredths of a percent, which is what every spread and cost figure is quoted in.
///
/// A type rather than an `f64` because the same spread is `0.0001` as a fraction and `1.0` as basis
/// points, and nothing about a bare float says which was meant. Non-negative: the rates this carries
/// are widths and costs, and [`EquityQuote::new`] has already refused the crossed book that would
/// produce a negative one.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BasisPoints(f64);

impl BasisPoints {
    pub fn new(value: f64) -> Option<Self> {
        (value.is_finite() && value >= 0.0).then_some(Self(value))
    }

    /// The rate `numerator / denominator` expressed in basis points.
    ///
    /// `None` on a denominator of zero rather than an infinity, so a midpoint that should never
    /// have been zero fails where it is computed instead of downstream of it.
    pub fn from_ratio(numerator: f64, denominator: f64) -> Option<Self> {
        if denominator == 0.0 {
            return None;
        }
        Self::new(numerator / denominator * 10_000.0)
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

impl std::fmt::Display for BasisPoints {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:.2}bp", self.0)
    }
}

/// What the quoted book looked like for one name over one bar, folded from the ticks.
///
/// Every average here is weighted by how long the quote prevailed rather than by how many times it
/// printed, because quote traffic is dominated by flickering that no order ever interacts with. The
/// two denominators travel with the averages for that reason: `quote_count` is how many updates
/// arrived and `covered_seconds` is how much of the bar a quote was standing at all.
#[derive(Debug, Clone, PartialEq)]
pub struct QuoteSummary {
    ticker: Ticker,
    bar_interval: BarInterval,
    /// UTC timestamp of the period this summary opens, matching the bar it describes.
    timestamp: DateTime<Utc>,
    quoted_spread_mean: f64,
    quoted_spread_basis_points_mean: BasisPoints,
    quoted_spread_basis_points_median: BasisPoints,
    quoted_spread_basis_points_ninetieth_percentile: BasisPoints,
    /// Time-weighted top-of-book size, in the unit the session's feed used: **round lots before
    /// 2025-11-03, shares on or after**. A series crossing that date steps by about a hundred and
    /// the step is the unit rather than the liquidity, so convert on read, keyed on the session.
    bid_size_mean: f64,
    ask_size_mean: f64,
    quote_count: i64,
    covered_seconds: f64,
}

impl QuoteSummary {
    /// Constructs a `QuoteSummary`, rejecting a fold that cannot have come from real quotes.
    ///
    /// A bar nothing was quoted across has no summary rather than a zero-weighted one, so
    /// `covered_seconds` must be positive; `quote_count` may be zero, which is an illiquid name
    /// still showing the quote it posted in an earlier bar.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ticker: Ticker,
        bar_interval: BarInterval,
        timestamp: DateTime<Utc>,
        quoted_spread_mean: f64,
        quoted_spread_basis_points_mean: BasisPoints,
        quoted_spread_basis_points_median: BasisPoints,
        quoted_spread_basis_points_ninetieth_percentile: BasisPoints,
        bid_size_mean: f64,
        ask_size_mean: f64,
        quote_count: i64,
        covered_seconds: f64,
    ) -> Result<Self, InconsistentRecordError> {
        for (name, value) in [
            ("mean quoted spread", quoted_spread_mean),
            ("mean bid size", bid_size_mean),
            ("mean ask size", ask_size_mean),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(reject(format!(
                    "{name} {value} is not a non-negative number"
                )));
            }
        }
        if !covered_seconds.is_finite() || covered_seconds <= 0.0 {
            return Err(reject(format!(
                "covered seconds {covered_seconds} is not a positive number"
            )));
        }
        if quote_count < 0 {
            return Err(reject(format!("quote count {quote_count} is negative")));
        }
        if quoted_spread_basis_points_median > quoted_spread_basis_points_ninetieth_percentile {
            return Err(reject(format!(
                "median spread {quoted_spread_basis_points_median} exceeds the ninetieth percentile {quoted_spread_basis_points_ninetieth_percentile}"
            )));
        }

        Ok(Self {
            ticker,
            bar_interval,
            timestamp,
            quoted_spread_mean,
            quoted_spread_basis_points_mean,
            quoted_spread_basis_points_median,
            quoted_spread_basis_points_ninetieth_percentile,
            bid_size_mean,
            ask_size_mean,
            quote_count,
            covered_seconds,
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

    pub fn quoted_spread_mean(&self) -> f64 {
        self.quoted_spread_mean
    }

    pub fn quoted_spread_basis_points_mean(&self) -> BasisPoints {
        self.quoted_spread_basis_points_mean
    }

    pub fn quoted_spread_basis_points_median(&self) -> BasisPoints {
        self.quoted_spread_basis_points_median
    }

    pub fn quoted_spread_basis_points_ninetieth_percentile(&self) -> BasisPoints {
        self.quoted_spread_basis_points_ninetieth_percentile
    }

    pub fn bid_size_mean(&self) -> f64 {
        self.bid_size_mean
    }

    pub fn ask_size_mean(&self) -> f64 {
        self.ask_size_mean
    }

    pub fn quote_count(&self) -> i64 {
        self.quote_count
    }

    pub fn covered_seconds(&self) -> f64 {
        self.covered_seconds
    }
}

/// What a bar dropped, and under whose rule.
///
/// Carried beside every aggregate because the fold is lossy: once a bar is written, no reader can
/// recover how much of the tape it declined to count. Both counts and dollar volumes, because the
/// two disagree wildly — 246 auction prints were 0.03% of one session's trades and 14.1% of its
/// money.
/// Every field is private and every count moves with its dollars, which is the point: a rule
/// recorded in two statements can be half-recorded, and these columns are the audit trail.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TradeExclusions {
    /// Refused by the provider's own `updates_volume`, chiefly the official open and close.
    volume_ineligible_trades: i64,
    volume_ineligible_dollar_volume: f64,
    /// Marked corrected or busted by the provider.
    corrected_trades: i64,
    corrected_dollar_volume: f64,
    /// Priced away from the market — an average or derivatively priced print. Counted rather than
    /// applied: no aggregate here differences against a quote yet, and this is what it would cost.
    non_market_price_trades: i64,
    non_market_price_dollar_volume: f64,
    /// Carried a condition the baked table does not name, so its eligibility was assumed rather than
    /// read. Never disqualifying; a rising count is how a provider's new code becomes visible.
    unresolved_condition_trades: i64,
}

impl TradeExclusions {
    /// Records a print the provider's volume rule refused.
    pub fn record_volume_ineligible(&mut self, notional: f64) {
        self.volume_ineligible_trades += 1;
        self.volume_ineligible_dollar_volume += notional;
    }

    /// Records a print the provider marked corrected or busted.
    pub fn record_correction(&mut self, notional: f64) {
        self.corrected_trades += 1;
        self.corrected_dollar_volume += notional;
    }

    /// Records a print whose price is not a market price, which still counts as volume.
    pub fn record_non_market_price(&mut self, notional: f64) {
        self.non_market_price_trades += 1;
        self.non_market_price_dollar_volume += notional;
    }

    /// Records a print carrying a condition this table does not name. No dollars: the print is
    /// folded like any other, and this counts only that its eligibility was assumed.
    pub fn record_unresolved_condition(&mut self) {
        self.unresolved_condition_trades += 1;
    }

    pub fn volume_ineligible_trades(&self) -> i64 {
        self.volume_ineligible_trades
    }

    pub fn volume_ineligible_dollar_volume(&self) -> f64 {
        self.volume_ineligible_dollar_volume
    }

    pub fn corrected_trades(&self) -> i64 {
        self.corrected_trades
    }

    pub fn corrected_dollar_volume(&self) -> f64 {
        self.corrected_dollar_volume
    }

    pub fn non_market_price_trades(&self) -> i64 {
        self.non_market_price_trades
    }

    pub fn non_market_price_dollar_volume(&self) -> f64 {
        self.non_market_price_dollar_volume
    }

    pub fn unresolved_condition_trades(&self) -> i64 {
        self.unresolved_condition_trades
    }

    /// Takes everything `other` recorded, so a coarser bar is the merge of its finer ones.
    ///
    /// Destructured rather than field-by-field: a field added later stops compiling here instead of
    /// being silently omitted, which would make a daily row understate what its minutes reported.
    pub(crate) fn absorb(&mut self, other: Self) {
        let Self {
            volume_ineligible_trades,
            volume_ineligible_dollar_volume,
            corrected_trades,
            corrected_dollar_volume,
            non_market_price_trades,
            non_market_price_dollar_volume,
            unresolved_condition_trades,
        } = other;
        self.volume_ineligible_trades += volume_ineligible_trades;
        self.volume_ineligible_dollar_volume += volume_ineligible_dollar_volume;
        self.corrected_trades += corrected_trades;
        self.corrected_dollar_volume += corrected_dollar_volume;
        self.non_market_price_trades += non_market_price_trades;
        self.non_market_price_dollar_volume += non_market_price_dollar_volume;
        self.unresolved_condition_trades += unresolved_condition_trades;
    }
}

#[cfg(test)]
mod trade_exclusion_tests {
    use super::TradeExclusions;

    /// Every recorded exclusion moves its count and its dollars together.
    ///
    /// The reason the fields are private: a rule recorded as two separate statements can increment
    /// a count and forget its dollars, and these columns are the archive's audit trail. Pinned to
    /// literals rather than to a sum of the inputs.
    #[test]
    fn test_a_recorded_exclusion_moves_its_count_and_its_dollars_together() {
        let mut exclusions = TradeExclusions::default();
        exclusions.record_volume_ineligible(100_000.0);
        exclusions.record_volume_ineligible(50_000.0);
        exclusions.record_correction(7_500.0);
        exclusions.record_non_market_price(250.0);
        exclusions.record_unresolved_condition();

        assert_eq!(exclusions.volume_ineligible_trades(), 2);
        assert_eq!(exclusions.volume_ineligible_dollar_volume(), 150_000.0);
        assert_eq!(exclusions.corrected_trades(), 1);
        assert_eq!(exclusions.corrected_dollar_volume(), 7_500.0);
        assert_eq!(exclusions.non_market_price_trades(), 1);
        assert_eq!(exclusions.non_market_price_dollar_volume(), 250.0);
        // No dollars: the print is folded like any other, and this counts only that its
        // eligibility was assumed rather than read.
        assert_eq!(exclusions.unresolved_condition_trades(), 1);
    }

    /// A coarser bar is the merge of its finer ones, on every field at once.
    #[test]
    fn test_absorb_carries_every_rule_upward() {
        let mut minute = TradeExclusions::default();
        minute.record_correction(1_000.0);
        minute.record_unresolved_condition();

        let mut session = TradeExclusions::default();
        session.record_volume_ineligible(9_000.0);
        session.absorb(minute);

        assert_eq!(session.corrected_trades(), 1);
        assert_eq!(session.corrected_dollar_volume(), 1_000.0);
        assert_eq!(session.volume_ineligible_trades(), 1);
        assert_eq!(session.volume_ineligible_dollar_volume(), 9_000.0);
        assert_eq!(session.unresolved_condition_trades(), 1);
    }
}

/// One bar's worth of printed trades, counting only what the eligibility policy admits.
///
/// `volume` and `dollar_volume` are the eligible tape, not the whole of it; [`TradeSummary::
/// exclusions`] says what was left out. A reader that wants the raw total must add them back
/// deliberately rather than by default.
#[derive(Debug, Clone)]
pub struct TradeSummary {
    ticker: Ticker,
    bar_interval: BarInterval,
    /// UTC timestamp of the period this summary opens, matching the bar it describes.
    timestamp: DateTime<Utc>,
    trade_count: i64,
    /// Shares, fractional: 16.5% of a session's prints are not whole shares.
    volume: f64,
    dollar_volume: f64,
    /// `None` when no eligible share traded, which an exclusion-only bar is.
    volume_weighted_average_price: Option<f64>,
    /// `None` when no eligible print has a size to report, which an exclusion-only bar is.
    median_trade_size: Option<f64>,
    /// Measurable exactly when [`TradeSummary::median_trade_size`] is.
    ninetieth_percentile_trade_size: Option<f64>,
    /// Buys minus sells in shares, signed by the tick rule — a trade above the previous price is a
    /// buy, below is a sell, equal inherits the last non-zero direction.
    signed_volume: f64,
    exclusions: TradeExclusions,
}

impl TradeSummary {
    /// Constructs a `TradeSummary`, rejecting a bar that cannot have come from real prints.
    ///
    /// A bar nothing eligible traded in has no summary rather than a zero-volume one, so `volume`
    /// must be positive. The exclusions may still be non-zero on such a bar, which is why a bar of
    /// nothing but auction prints is absent here and visible in the daily row that absorbs it.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ticker: Ticker,
        bar_interval: BarInterval,
        timestamp: DateTime<Utc>,
        trade_count: i64,
        volume: f64,
        dollar_volume: f64,
        median_trade_size: Option<f64>,
        ninetieth_percentile_trade_size: Option<f64>,
        signed_volume: f64,
        exclusions: TradeExclusions,
    ) -> Result<Self, InconsistentRecordError> {
        // A bar that admitted nothing is still a row when it *excluded* something: an auction-only
        // bar is exactly where the exclusion matters, and dropping it would lose the only record
        // that the tape had anything in it at all. Zeroes are permitted on that condition alone, so
        // a fold that produced nothing for no reason is still refused.
        let excluded_something = exclusions != TradeExclusions::default();
        for (name, value) in [("volume", volume), ("dollar volume", dollar_volume)] {
            if !value.is_finite() || value < 0.0 {
                return Err(reject(format!(
                    "{name} {value} is not a non-negative number"
                )));
            }
            if value == 0.0 && !excluded_something {
                return Err(reject(format!(
                    "{name} is zero on a bar that excluded nothing, so it describes no trades"
                )));
            }
        }
        for (name, value) in [
            ("median trade size", median_trade_size),
            (
                "ninetieth percentile trade size",
                ninetieth_percentile_trade_size,
            ),
        ] {
            let Some(value) = value else { continue };
            if !value.is_finite() || value < 0.0 {
                return Err(reject(format!(
                    "{name} {value} is not a non-negative number"
                )));
            }
        }
        // Both are drawn from the same eligible prints, so a bar that could measure one could
        // measure the other; a disagreement means the two were folded over different populations.
        match (median_trade_size, ninetieth_percentile_trade_size) {
            (Some(median), Some(ninetieth)) if median > ninetieth => {
                return Err(reject(format!(
                    "median trade size {median} exceeds the ninetieth percentile {ninetieth}"
                )));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(reject(
                    "one trade size percentile is measured and the other is not".to_string(),
                ));
            }
            (Some(_), Some(_)) | (None, None) => {}
        }
        if trade_count < 0 {
            return Err(reject(format!("trade count {trade_count} is negative")));
        }
        if trade_count == 0 && !excluded_something {
            return Err(reject(
                "a bar with no trades and no exclusions describes nothing".to_string(),
            ));
        }
        // Signed volume is buys minus sells drawn from the same shares, so it cannot exceed them.
        // A violation means the tick rule and the volume accumulator saw different trades.
        if !signed_volume.is_finite() || signed_volume.abs() > volume {
            return Err(reject(format!(
                "signed volume {signed_volume} exceeds the {volume} shares it is drawn from"
            )));
        }
        Ok(Self {
            ticker,
            bar_interval,
            timestamp,
            trade_count,
            volume,
            dollar_volume,
            // Divided here rather than passed in, so the identity cannot be violated by a caller.
            // `None` on an exclusion-only bar: no eligible share traded, so there is no price it
            // traded at, and a zero would read as one.
            volume_weighted_average_price: (volume > 0.0).then(|| dollar_volume / volume),
            median_trade_size,
            ninetieth_percentile_trade_size,
            signed_volume,
            exclusions,
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

    pub fn trade_count(&self) -> i64 {
        self.trade_count
    }

    pub fn volume(&self) -> f64 {
        self.volume
    }

    pub fn dollar_volume(&self) -> f64 {
        self.dollar_volume
    }

    pub fn volume_weighted_average_price(&self) -> Option<f64> {
        self.volume_weighted_average_price
    }

    pub fn median_trade_size(&self) -> Option<f64> {
        self.median_trade_size
    }

    pub fn ninetieth_percentile_trade_size(&self) -> Option<f64> {
        self.ninetieth_percentile_trade_size
    }

    pub fn signed_volume(&self) -> f64 {
        self.signed_volume
    }

    pub fn exclusions(&self) -> TradeExclusions {
        self.exclusions
    }
}

/// The most recent trade in a symbol, as one snapshot reported it.
///
/// The timestamp is not optional because the last trade is what the pass prices on whenever the
/// book is refused, and an undated one cannot be judged for staleness the way [`EquityQuote`] can.
#[derive(Debug, Clone)]
pub struct EquityTrade {
    ticker: Ticker,
    timestamp: DateTime<Utc>,
    price: f64,
}

impl EquityTrade {
    /// Constructs an `EquityTrade`, rejecting a price that is not one.
    pub fn new(
        ticker: Ticker,
        timestamp: DateTime<Utc>,
        price: f64,
    ) -> Result<Self, InconsistentRecordError> {
        if !price.is_finite() || price <= 0.0 {
            return Err(reject(format!(
                "trade price {price} is not a positive number"
            )));
        }

        Ok(Self {
            ticker,
            timestamp,
            price,
        })
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn price(&self) -> f64 {
        self.price
    }
}

/// A Standard Industrial Classification code: four digits, leading zeros significant.
///
/// A validated value rather than a `String` because the whole code is what the sector and industry
/// lookups take, so a malformed one must be refused where it arrives — stored, it would read as a
/// name with no classification, which is indistinguishable from the 901 of 5,217 common stocks that
/// genuinely have none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SicCode(String);

impl SicCode {
    /// `None` unless the code is exactly four ASCII digits.
    pub fn new(code: &str) -> Option<Self> {
        (code.len() == 4 && code.chars().all(|digit| digit.is_ascii_digit()))
            .then(|| Self(code.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The SEC's Central Index Key: which company filed, independent of the ticker it trades under.
///
/// Stored zero-padded to ten digits, the width EDGAR's own paths use, so two spellings of one company
/// compare equal. A ticker can be reused by a different company; a CIK cannot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cik(String);

impl Cik {
    /// `None` unless the key is one to ten ASCII digits and not all zeros.
    pub fn new(key: &str) -> Option<Self> {
        let usable = (1..=10).contains(&key.len())
            && key.chars().all(|digit| digit.is_ascii_digit())
            && key.chars().any(|digit| digit != '0');
        usable.then(|| Self(format!("{key:0>10}")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What kind of instrument a symbol is, as the reference feed classifies it.
///
/// The variant that matters is [`SecurityType::CommonStock`]: a bar exists for every instrument that
/// traded, so without this the universe admits exchange-traded funds and warrants alongside the
/// companies a pairs book is supposed to hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityType {
    CommonStock,
    ExchangeTradedFund,
    ExchangeTradedNote,
    Warrant,
    Unit,
    Preferred,
    AmericanDepositaryReceipt,
    Fund,
    StructuredProduct,
    Right,
    Index,
    /// A code the archive does not model, kept verbatim rather than folded into a neighbour.
    ///
    /// Carrying the code means a type the feed adds later reads as unmodelled instead of silently
    /// becoming whichever variant a wildcard arm happened to name.
    Other(String),
}

impl SecurityType {
    /// Reads the feed's code. Every unrecognized code becomes [`SecurityType::Other`] rather than an
    /// error, because a new instrument class is not a malformed record.
    pub fn from_code(code: &str) -> Self {
        match code {
            "CS" => SecurityType::CommonStock,
            "ETF" => SecurityType::ExchangeTradedFund,
            "ETN" => SecurityType::ExchangeTradedNote,
            "WARRANT" => SecurityType::Warrant,
            "UNIT" => SecurityType::Unit,
            "PFD" => SecurityType::Preferred,
            "ADRC" => SecurityType::AmericanDepositaryReceipt,
            "FUND" => SecurityType::Fund,
            "SP" => SecurityType::StructuredProduct,
            "RIGHT" => SecurityType::Right,
            "INDEX" => SecurityType::Index,
            other => SecurityType::Other(other.to_string()),
        }
    }

    /// The stored form, which round-trips through [`SecurityType::from_code`].
    pub fn as_code(&self) -> &str {
        match self {
            SecurityType::CommonStock => "CS",
            SecurityType::ExchangeTradedFund => "ETF",
            SecurityType::ExchangeTradedNote => "ETN",
            SecurityType::Warrant => "WARRANT",
            SecurityType::Unit => "UNIT",
            SecurityType::Preferred => "PFD",
            SecurityType::AmericanDepositaryReceipt => "ADRC",
            SecurityType::Fund => "FUND",
            SecurityType::StructuredProduct => "SP",
            SecurityType::Right => "RIGHT",
            SecurityType::Index => "INDEX",
            SecurityType::Other(code) => code,
        }
    }

    /// Whether this is a share in a company, which is the only thing the equity universe admits.
    ///
    /// The one place that comparison is written, so widening the universe to another class is an
    /// edit here rather than a screen somewhere that forgot to.
    pub fn is_common_stock(&self) -> bool {
        matches!(self, SecurityType::CommonStock)
    }
}

/// What the reference feed said about one symbol as of one date.
///
/// Point-in-time by construction: `as_of` is the date the values were requested for, not the date
/// they were fetched, and the feed answers differently for different dates. Reading a 2021 session
/// against a 2026 snapshot is what made the universe survivorship-biased.
#[derive(Debug, Clone, PartialEq)]
pub struct EquityReference {
    ticker: Ticker,
    as_of: SessionDate,
    /// `None` where the feed classified nothing, which it does for a small tail of symbols.
    ///
    /// Absent rather than assumed: an unclassified symbol must not enter the universe as though it
    /// were common stock, and the count of them is worth reporting rather than hiding.
    security_type: Option<SecurityType>,
    /// `None` covers both a feed that reported no code and one that reported an unusable one.
    sic_code: Option<SicCode>,
    sic_description: Option<String>,
    /// Shares in issue on `as_of`, which is what makes a market capitalization point-in-time.
    ///
    /// `None` means the feed reported none, never zero — a company with no shares is not a thing,
    /// so a zero here would be a missing measurement wearing a number.
    shares_outstanding: Option<f64>,
    /// The feed's own capitalization, carried only as a cross-check against `shares x close`.
    reported_market_capitalization: Option<f64>,
    primary_exchange: Option<String>,
    /// The filer behind the ticker on `as_of`, which is what an SEC lookup is keyed by.
    cik: Option<Cik>,
}

impl EquityReference {
    /// Constructs a reference row, rejecting counts that cannot describe an instrument.
    ///
    /// A non-positive or non-finite share count would make a derived capitalization meaningless in a
    /// way no downstream reader could detect, so it is refused here rather than stored.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ticker: Ticker,
        as_of: SessionDate,
        security_type: Option<SecurityType>,
        sic_code: Option<SicCode>,
        sic_description: Option<String>,
        shares_outstanding: Option<f64>,
        reported_market_capitalization: Option<f64>,
        primary_exchange: Option<String>,
    ) -> Result<Self, InconsistentRecordError> {
        for (name, value) in [
            ("shares outstanding", shares_outstanding),
            (
                "reported market capitalization",
                reported_market_capitalization,
            ),
        ] {
            if let Some(value) = value {
                if !value.is_finite() || value <= 0.0 {
                    return Err(reject(format!(
                        "{name} must be finite and positive, got {value}"
                    )));
                }
            }
        }
        Ok(Self {
            ticker,
            as_of,
            security_type,
            sic_code,
            sic_description,
            shares_outstanding,
            reported_market_capitalization,
            primary_exchange,
            cik: None,
        })
    }

    /// Attaches the filer the feed named, which only the Massive route reports.
    pub fn with_cik(self, cik: Option<Cik>) -> Self {
        Self { cik, ..self }
    }

    pub fn cik(&self) -> Option<&Cik> {
        self.cik.as_ref()
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn as_of(&self) -> SessionDate {
        self.as_of
    }

    pub fn security_type(&self) -> Option<&SecurityType> {
        self.security_type.as_ref()
    }

    pub fn sic_code(&self) -> Option<&SicCode> {
        self.sic_code.as_ref()
    }

    pub fn sic_description(&self) -> Option<&str> {
        self.sic_description.as_deref()
    }

    pub fn shares_outstanding(&self) -> Option<f64> {
        self.shares_outstanding
    }

    pub fn reported_market_capitalization(&self) -> Option<f64> {
        self.reported_market_capitalization
    }

    pub fn primary_exchange(&self) -> Option<&str> {
        self.primary_exchange.as_deref()
    }

    /// Whether the universe admits this symbol, which requires the feed to have classified it.
    ///
    /// An unclassified symbol is refused rather than admitted: the screen's job is to say what a
    /// thing is, and `None` is the feed declining to.
    pub fn is_tradeable_equity(&self) -> bool {
        self.security_type
            .as_ref()
            .is_some_and(SecurityType::is_common_stock)
    }
}

/// One stock split as the corporate-actions feed reported it.
///
/// The ratio reads `split_from` shares becoming `split_to` shares, so a two-for-one forward split
/// is `1 -> 2` and a one-for-three reverse split is `3 -> 1`. Both sides are real rather than whole
/// numbers, because a fifth of the live feed is fractional mutual-fund reallocations.
///
/// `execution_date` may be in the future: the feed publishes splits once announced, and can later
/// revise or cancel one.
#[derive(Debug, Clone, PartialEq)]
pub struct EquitySplit {
    id: String,
    ticker: Ticker,
    execution_date: SessionDate,
    split_from: f64,
    split_to: f64,
}

impl EquitySplit {
    /// Constructs an `EquitySplit`, rejecting a ratio that cannot describe one.
    ///
    /// A non-positive or non-finite side would make the adjustment factor a division by zero, a
    /// sign flip, or a `NaN` that silently poisons every price it touches. An unidentified row
    /// cannot be merged against a later fetch of the same split.
    pub fn new(
        id: String,
        ticker: Ticker,
        execution_date: SessionDate,
        split_from: f64,
        split_to: f64,
    ) -> Result<Self, InconsistentRecordError> {
        if id.trim().is_empty() {
            return Err(reject("split identifier is empty"));
        }
        for (name, side) in [("from", split_from), ("to", split_to)] {
            if !side.is_finite() || side <= 0.0 {
                return Err(reject(format!(
                    "split {name} side {side} is not a positive number"
                )));
            }
        }

        Ok(Self {
            id,
            ticker,
            execution_date,
            split_from,
            split_to,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn execution_date(&self) -> SessionDate {
        self.execution_date
    }

    pub fn split_from(&self) -> f64 {
        self.split_from
    }

    pub fn split_to(&self) -> f64 {
        self.split_to
    }
}

/// Why a symbol's price series must not be read across a date.
///
/// Only `Renamed` carries history forward. The rest mean the symbol still denotes the same company
/// but its price stepped for a reason no return should be computed across, so their bars before the
/// date are unusable rather than relocatable.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundaryReason {
    /// The company kept trading under `to`, which is where its history continues.
    ///
    /// This one row also ends the old symbol's series: whatever trades under it afterwards is a
    /// different company, so reuse needs no variant of its own.
    Renamed { to: Ticker },
    /// The company distributed shares of `spin_off_company`; its price fell by their value.
    SpunOff { spin_off_company: Ticker },
    /// Rights were distributed. The price falls by their value and the bar feed never carries a
    /// price for them, so the step cannot be measured, only avoided.
    RightsDistributed,
    /// A unit separated into its components — a rename and a distribution at once, so unlike a
    /// rename the price steps and the history does not follow.
    UnitSeparated,
    /// A reorganization the feed does not classify further; truncating is the conservative reading.
    Reorganized,
}

impl BoundaryReason {
    /// The stored form, and the discriminant a reader matches on.
    pub fn as_str(&self) -> &'static str {
        match self {
            BoundaryReason::Renamed { .. } => "renamed",
            BoundaryReason::SpunOff { .. } => "spun_off",
            BoundaryReason::RightsDistributed => "rights_distributed",
            BoundaryReason::UnitSeparated => "unit_separated",
            BoundaryReason::Reorganized => "reorganized",
        }
    }

    /// The symbol this one's history continues under, when it continues at all.
    pub fn successor(&self) -> Option<&Ticker> {
        match self {
            BoundaryReason::Renamed { to } => Some(to),
            BoundaryReason::SpunOff { .. }
            | BoundaryReason::RightsDistributed
            | BoundaryReason::UnitSeparated
            | BoundaryReason::Reorganized => None,
        }
    }
}

/// A date a symbol's series may not be read across.
///
/// Held per symbol rather than per company on purpose: no identifier shared by our two providers
/// survives a rename, so a symbol bounded in time is the strongest identity available.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesBoundary {
    id: String,
    ticker: Ticker,
    date: SessionDate,
    process_date: SessionDate,
    reason: BoundaryReason,
}

impl SeriesBoundary {
    /// Constructs a `SeriesBoundary`, rejecting one that does not divide anything.
    ///
    /// A rename onto the same symbol is the case this exists for: the feed reports a company's name
    /// or CUSIP changing under an unchanged ticker far more often than a real symbol change, and
    /// such a row bounds nothing while truncating every window that spans it.
    ///
    /// The identifier is stored trimmed, because it is the key a refresh matches stored rows on and
    /// whitespace either side would make one boundary look like two.
    pub fn new(
        id: String,
        ticker: Ticker,
        date: SessionDate,
        process_date: SessionDate,
        reason: BoundaryReason,
    ) -> Result<Self, InconsistentRecordError> {
        let id = id.trim().to_string();
        if id.is_empty() {
            return Err(reject("boundary identifier is empty"));
        }
        if reason.successor() == Some(&ticker) {
            return Err(reject(format!(
                "boundary for {ticker} names itself as its successor"
            )));
        }

        Ok(Self {
            id,
            ticker,
            date,
            process_date,
            reason,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    /// The session the price moved: an ex-date, or an effective date.
    pub fn date(&self) -> SessionDate {
        self.date
    }

    /// The session the feed processed the action, which is what its date filter selects on.
    ///
    /// Carried because it is the only way a refresh can tell which stored rows it was in a position
    /// to re-report, and therefore which absences mean an action was cancelled.
    pub fn process_date(&self) -> SessionDate {
        self.process_date
    }

    pub fn reason(&self) -> &BoundaryReason {
        &self.reason
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

    /// Both bounds admit the threshold itself. Every screen defers to this comparison, so an
    /// exclusive test here would admit a name to training and then refuse to predict for it.
    #[test]
    fn test_a_floor_admits_a_name_sitting_exactly_on_it() {
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).unwrap();

        assert_eq!(floor.admits(10.0, 50_000_000.0), Ok(()));
        assert_eq!(
            floor.admits(9.99, 50_000_000.0),
            Err(LiquidityRefusal::ClosePrice {
                observed: 9.99,
                minimum: 10.0,
            })
        );
        assert_eq!(
            floor.admits(10.0, 49_999_999.0),
            Err(LiquidityRefusal::DollarVolume {
                observed: 49_999_999.0,
                minimum: 50_000_000.0,
            })
        );
    }

    /// The two bounds are independent, which is what makes an expensive thin name and a cheap heavy
    /// one each excluded for their own reason — and the refusal is what says which.
    #[test]
    fn test_each_bound_rejects_on_its_own_and_names_itself() {
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).unwrap();

        assert_eq!(
            floor
                .admits(500.0, 30_000_000.0)
                .expect_err("expensive but untraded")
                .as_str(),
            "dollar_volume"
        );
        assert_eq!(
            floor
                .admits(2.0, 900_000_000.0)
                .expect_err("heavily traded but cheap")
                .as_str(),
            "close_price"
        );
        assert_eq!(floor.admits(290.0, 261_000_000.0), Ok(()));

        // The whole string: the detail is aggregated out of the journal, so the format is contract.
        assert_eq!(
            floor
                .admits(2.0, 900_000_000.0)
                .expect_err("a cheap name reports its price")
                .detail(),
            "close_price=2.0000 minimum=10.0000"
        );
    }

    /// A reading that is not a number fails every comparison, so a bare `<` would admit it.
    #[test]
    fn test_a_floor_refuses_a_reading_that_is_not_a_number() {
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).unwrap();

        assert_eq!(
            floor
                .admits(f64::NAN, 900_000_000.0)
                .expect_err("an unpriceable name must be refused")
                .as_str(),
            "close_price"
        );
        assert_eq!(
            floor
                .admits(290.0, f64::NAN)
                .expect_err("an unmeasurable notional must be refused")
                .as_str(),
            "dollar_volume"
        );
    }

    #[test]
    fn test_a_floor_refuses_a_bound_that_is_not_a_usable_number() {
        assert!(LiquidityFloor::new(-1.0, 50_000_000.0).is_none());
        assert!(LiquidityFloor::new(10.0, f64::NAN).is_none());
        assert!(LiquidityFloor::new(10.0, f64::INFINITY).is_none());
        assert!(
            LiquidityFloor::new(0.0, 0.0).is_some(),
            "a floor of zero admits everything, which is a screen and not an error"
        );
    }

    /// The shipped floor is what every path screens against until a caller declares its own, so a
    /// change to it changes the traded universe and must be a deliberate edit rather than a drift.
    #[test]
    fn test_the_current_floor_is_ten_dollars_and_fifty_million() {
        assert_eq!(LiquidityFloor::CURRENT.minimum_close_price(), 10.0);
        assert_eq!(
            LiquidityFloor::CURRENT.minimum_dollar_volume(),
            50_000_000.0
        );
    }

    /// Both bounds have to appear, because this renders into log fields whose only job is telling
    /// two passes apart — one that named the screen without its floor could not.
    #[test]
    fn test_a_floor_renders_both_of_its_bounds() {
        assert_eq!(
            LiquidityFloor::CURRENT.to_string(),
            "$10 close on $50000000 traded"
        );
        assert_eq!(
            LiquidityFloor::new(2.5, 750_000.0).unwrap().to_string(),
            "$2.5 close on $750000 traded",
            "a floor under a million must not render as zero"
        );
    }

    #[derive(Serialize, Deserialize)]
    struct Amount {
        #[serde(with = "decimal_number")]
        value: Decimal,
        #[serde(with = "decimal_number_option")]
        maybe: Option<Decimal>,
    }

    /// Amounts reach JSON as numbers. A quoted decimal has to be cast before a reader can do
    /// arithmetic on it, and it shows its quotes on the dashboard's rendered payloads.
    #[test]
    fn test_an_amount_serializes_as_a_number() {
        let rendered = serde_json::to_string(&Amount {
            value: "104812.55".parse().expect("valid decimal"),
            maybe: Some("-50000.01".parse().expect("valid decimal")),
        })
        .expect("an amount must serialize");
        assert_eq!(rendered, r#"{"value":104812.55,"maybe":-50000.01}"#);
    }

    #[test]
    fn test_an_absent_amount_stays_null() {
        let rendered = serde_json::to_string(&Amount {
            value: Decimal::ZERO,
            maybe: None,
        })
        .expect("an amount must serialize");
        assert_eq!(rendered, r#"{"value":0.0,"maybe":null}"#);
    }

    /// The conversion has no failure case to handle, so no journal write can panic on it. Checked
    /// at the extremes of the type rather than on ordinary balances, which is where a fallible
    /// conversion would give way.
    #[test]
    fn test_no_decimal_fails_to_render_as_a_number() {
        for value in [
            Decimal::MAX,
            Decimal::MIN,
            Decimal::ZERO,
            Decimal::new(i64::MAX, 28),
            "0.0000000000000000000000000001"
                .parse()
                .expect("valid decimal"),
        ] {
            let rendered = serde_json::to_value(Amount {
                value,
                maybe: Some(value),
            })
            .expect("every decimal must serialize");
            assert!(
                rendered["value"].is_number() && rendered["maybe"].is_number(),
                "every amount renders as a number: {rendered}"
            );
        }
    }

    /// The `f64` wire form is lossless over the values this system actually holds, which is what
    /// makes it a fair trade for an unquoted JSON number. The bound is about seventeen significant
    /// digits; a nine-figure balance and a nine-decimal share count together use ten.
    #[test]
    fn test_a_decimal_survives_the_round_trip_to_json_and_back() {
        for literal in [
            "123456789.99",   // a balance larger than this fund will hold
            "0.000000001",    // the finest fractional share Alpaca quotes
            "-48250.75",      // a short market value, which is signed
            "0",              //
            "1234.567890123", // ten significant digits
        ] {
            let value: Decimal = literal.parse().expect("the literal must parse");
            let rendered = serde_json::to_string(&Amount {
                value,
                maybe: Some(value),
            })
            .expect("an amount must serialize");
            let restored: Amount =
                serde_json::from_str(&rendered).expect("an amount must deserialize");
            assert_eq!(
                restored.value, value,
                "round trip of {literal} via {rendered}"
            );
            assert_eq!(restored.maybe, Some(value));
        }
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

    /// These exact strings are the `close_reason` CHECK constraint in `schema.sql:105`, on the same
    /// terms as [`BarInterval`] below: the column is TEXT, so a drift between the enum and the
    /// constraint is invisible until every close fails at runtime.
    #[test]
    fn test_close_reason_stored_form_matches_the_check_constraint() {
        assert_eq!(CloseReason::Convergence.as_str(), "convergence");
        assert_eq!(CloseReason::StopLoss.as_str(), "stop_loss");
        assert_eq!(CloseReason::EndOfDay.as_str(), "end_of_day");
        assert_eq!(CloseReason::PositionMissing.as_str(), "position_missing");
    }

    #[test]
    fn test_close_reason_round_trips_through_stored_form() {
        for reason in CloseReason::ALL {
            assert_eq!(CloseReason::parse(reason.as_str()), Some(reason));
        }
        assert!(CloseReason::parse("convergence ").is_none());
        assert!(CloseReason::parse("Convergence").is_none());
    }

    /// The ratio of signal exits to end-of-day exits is the interim measure of whether the strategy
    /// does anything, so which side each reason falls on is worth pinning rather than re-deriving.
    #[test]
    fn test_only_a_spread_opinion_counts_as_a_signal_exit() {
        assert!(CloseReason::Convergence.is_signal());
        assert!(CloseReason::StopLoss.is_signal());
        assert!(!CloseReason::EndOfDay.is_signal());
        assert!(!CloseReason::PositionMissing.is_signal());
    }

    /// These exact strings are the `bar_interval` CHECK constraint in `schema.sql`. Changing one
    /// without the other makes every insert fail at runtime, which no compile-time check catches —
    /// the column is TEXT, so sqlx sees a string either way.
    #[test]
    fn test_bar_interval_stored_form_matches_the_check_constraint() {
        assert_eq!(BarInterval::OneDay.as_str(), "one_day");
        assert_eq!(BarInterval::OneMinute.as_str(), "one_minute");
        assert_eq!(BarInterval::FiveMinute.as_str(), "five_minute");
    }

    /// Massive's aggregates route spells an interval as a multiplier and a timespan, and the two
    /// minute cadences differ only in the multiplier — so a copied arm is invisible until every
    /// five-minute request silently returns one-minute bars.
    #[test]
    fn test_each_interval_maps_to_a_distinct_massive_timespan() {
        assert_eq!(BarInterval::OneMinute.massive_timespan(), (1, "minute"));
        assert_eq!(BarInterval::FiveMinute.massive_timespan(), (5, "minute"));
        assert_eq!(BarInterval::OneDay.massive_timespan(), (1, "day"));

        let spellings: std::collections::BTreeSet<(u32, &str)> = BarInterval::ALL
            .iter()
            .map(|i| i.massive_timespan())
            .collect();
        assert_eq!(spellings.len(), 3);
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

    /// Neither Alpaca's timeframe spelling nor any near-miss of the stored form is accepted;
    /// silently parsing one would let a stale writer put unreadable rows in the table.
    #[test]
    fn test_bar_interval_parse_rejects_foreign_spellings() {
        assert!(BarInterval::parse("1Day").is_none());
        assert!(BarInterval::parse("1Hour").is_none());
        assert!(BarInterval::parse("OneDay").is_none());
        assert!(BarInterval::parse("1day").is_none());
        assert!(BarInterval::parse("1_day").is_none());
    }

    /// Every cadence names a bucket, and every bucket-naming interval has a cadence.
    ///
    /// The `OneDay` arm is the point: a fold opened on the session would divide it into a grid of
    /// one, and the daily row is the merge of the intraday rows rather than a bucket of its own.
    #[test]
    fn test_intraday_cadence_covers_the_bucket_naming_intervals_and_no_others() {
        for cadence in IntradayCadence::ALL {
            assert_eq!(
                IntradayCadence::from_bar_interval(cadence.bar_interval()),
                Some(cadence)
            );
        }
        assert!(IntradayCadence::from_bar_interval(BarInterval::OneDay).is_none());
        assert_eq!(IntradayCadence::OneMinute.seconds(), 60);
        assert_eq!(IntradayCadence::FiveMinute.seconds(), 300);
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

    #[test]
    fn test_basis_points_converts_a_ratio_and_refuses_an_unusable_one() {
        // AAPL midday on 2026-08-20: a nine-cent book on a $317 midpoint.
        let measured = BasisPoints::from_ratio(0.09, 317.455).unwrap();
        assert!((measured.value() - 2.835).abs() < 0.001, "{measured}");
        assert_eq!(BasisPoints::from_ratio(0.01, 0.0), None, "zero midpoint");
        assert_eq!(BasisPoints::new(-0.5), None, "a width is not signed");
        assert_eq!(BasisPoints::new(f64::NAN), None, "non-finite");
    }

    fn summary(
        median: f64,
        ninetieth: f64,
        quote_count: i64,
        covered_seconds: f64,
    ) -> Result<QuoteSummary, InconsistentRecordError> {
        QuoteSummary::new(
            ticker("AAPL"),
            BarInterval::FiveMinute,
            Utc::now(),
            0.09,
            BasisPoints::new(2.84).unwrap(),
            BasisPoints::new(median).unwrap(),
            BasisPoints::new(ninetieth).unwrap(),
            480.0,
            80.0,
            quote_count,
            covered_seconds,
        )
    }

    /// Zero arrivals is an illiquid name still showing an earlier bar's quote, which is a real
    /// reading. Zero covered seconds is a bar nothing was quoted across, which is not a reading at
    /// all — every average in it would be a division by zero wearing a number's clothes.
    #[test]
    fn test_summary_separates_no_arrivals_from_no_coverage() {
        assert!(summary(2.5, 6.0, 0, 300.0).is_ok(), "no arrivals");
        assert!(summary(2.5, 6.0, 12, 0.0).is_err(), "no coverage");
        assert!(summary(2.5, 6.0, -1, 300.0).is_err(), "negative count");
    }

    #[test]
    fn test_summary_refuses_quantiles_out_of_order() {
        assert!(summary(6.0, 2.5, 12, 300.0).is_err());
        assert!(summary(2.5, 2.5, 12, 300.0).is_ok(), "a flat book ties");
    }

    #[test]
    fn test_trade_rejects_a_price_that_is_not_one() {
        let at = Utc::now();
        assert_eq!(
            EquityTrade::new(ticker("AAPL"), at, 201.5).unwrap().price(),
            201.5
        );
        assert!(EquityTrade::new(ticker("AAPL"), at, 0.0).is_err(), "zero");
        assert!(
            EquityTrade::new(ticker("AAPL"), at, -1.0).is_err(),
            "signed"
        );
        assert!(
            EquityTrade::new(ticker("AAPL"), at, f64::NAN).is_err(),
            "non-finite"
        );
    }

    fn split(id: &str, from: f64, to: f64) -> Result<EquitySplit, InconsistentRecordError> {
        EquitySplit::new(
            id.to_string(),
            ticker("MNST"),
            SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 7, 6).unwrap()),
            from,
            to,
        )
    }

    /// A non-positive side makes the adjustment factor a division by zero or a sign flip, and a
    /// `NaN` poisons every price it reaches. An unidentified row cannot be merged against a later
    /// fetch of the same split.
    #[test]
    fn test_split_rejects_a_ratio_or_identifier_it_cannot_use() {
        let forward = split("E1", 1.0, 2.0).expect("a two-for-one must construct");
        assert_eq!((forward.split_from(), forward.split_to()), (1.0, 2.0));
        assert!(split("E1", 3.0, 1.0).is_ok(), "a reverse split is ordinary");

        assert!(split("E1", 0.0, 2.0).is_err(), "zero from");
        assert!(split("E1", 1.0, 0.0).is_err(), "zero to");
        assert!(split("E1", -1.0, 2.0).is_err(), "negative from");
        assert!(split("E1", 1.0, f64::NAN).is_err(), "non-finite to");
        assert!(split("", 1.0, 2.0).is_err(), "empty identifier");
        assert!(split("   ", 1.0, 2.0).is_err(), "blank identifier");
    }

    /// Nineteen percent of the live feed is fractional, so a whole-number ratio would silently
    /// discard every mutual-fund reallocation in it.
    #[test]
    fn test_split_accepts_a_fractional_ratio() {
        let reallocation = split("E1", 1.0, 1.0056).expect("a fractional ratio must construct");
        assert_eq!(reallocation.split_to(), 1.0056);
    }

    fn reference(security_type: Option<SecurityType>, sic: Option<&str>) -> EquityReference {
        EquityReference::new(
            ticker("AAPL"),
            session(2021, 9, 15),
            security_type,
            sic.and_then(SicCode::new),
            None,
            Some(16_530_169_999.0),
            Some(2_445_045_344_910.0),
            Some("XNAS".to_string()),
        )
        .expect("the fixture must be constructible")
    }

    /// Every code the feed actually emitted on one session, counted from a full sweep on
    /// 2021-09-15. Pinned to literals so a code silently changing meaning fails here.
    #[test]
    fn test_every_observed_security_code_round_trips() {
        for code in [
            "CS", "ETF", "ETN", "WARRANT", "UNIT", "PFD", "ADRC", "FUND", "SP", "RIGHT", "INDEX",
        ] {
            let parsed = SecurityType::from_code(code);
            assert_eq!(parsed.as_code(), code, "{code} did not round trip");
            assert!(
                !matches!(parsed, SecurityType::Other(_)),
                "{code} was observed in the feed and must be modelled"
            );
        }
    }

    /// A class the archive does not model keeps its code rather than becoming a neighbour.
    #[test]
    fn test_an_unmodelled_code_is_carried_verbatim() {
        let parsed = SecurityType::from_code("BASKET");

        assert_eq!(parsed, SecurityType::Other("BASKET".to_string()));
        assert_eq!(parsed.as_code(), "BASKET");
        assert!(!parsed.is_common_stock());
    }

    /// The screen the whole dataset exists to enable: a bar is written for every instrument that
    /// traded, and only common stock may reach the universe.
    #[test]
    fn test_only_common_stock_is_a_tradeable_equity() {
        assert!(reference(Some(SecurityType::CommonStock), None).is_tradeable_equity());

        for excluded in [
            SecurityType::ExchangeTradedFund,
            SecurityType::Warrant,
            SecurityType::Unit,
            SecurityType::Preferred,
            SecurityType::Fund,
            SecurityType::Other("BASKET".to_string()),
        ] {
            assert!(
                !reference(Some(excluded.clone()), None).is_tradeable_equity(),
                "{} must not reach the universe",
                excluded.as_code()
            );
        }
    }

    /// 153 symbols carried no type at all on the session measured. An unclassified symbol is
    /// refused rather than admitted, because `None` is the feed declining to say what it is.
    #[test]
    fn test_an_unclassified_symbol_is_refused_rather_than_assumed() {
        let unclassified = reference(None, None);

        assert_eq!(unclassified.security_type(), None);
        assert!(!unclassified.is_tradeable_equity());
    }

    /// A malformed code is refused where it arrives rather than stored and re-checked downstream.
    #[test]
    fn test_a_code_that_is_not_four_digits_is_not_a_sic_code() {
        for malformed in ["357", "35710", "35A1", "", " 357"] {
            assert!(
                SicCode::new(malformed).is_none(),
                "{malformed:?} must not construct"
            );
            assert_eq!(
                reference(None, Some(malformed)).sic_code(),
                None,
                "{malformed:?} must not reach the classification lookups"
            );
        }
        assert_eq!(reference(None, None).sic_code(), None);
    }

    /// Leading zeros are significant, which is why the code is a string and not a number.
    #[test]
    fn test_a_leading_zero_survives_construction() {
        let agriculture = SicCode::new("0100").expect("four digits");

        assert_eq!(agriculture.as_str(), "0100");
    }

    /// Zero shares is a missing measurement wearing a number, so it is refused at construction.
    #[test]
    fn test_a_share_count_that_cannot_describe_a_company_is_refused() {
        let build = |shares: Option<f64>, capitalization: Option<f64>| {
            EquityReference::new(
                ticker("AAPL"),
                session(2021, 9, 15),
                Some(SecurityType::CommonStock),
                None,
                None,
                shares,
                capitalization,
                None,
            )
        };

        assert!(build(Some(16_530_169_999.0), Some(1.0)).is_ok());
        // Absent is fine; present-but-impossible is not.
        assert!(build(None, None).is_ok());
        assert!(build(Some(0.0), None).is_err());
        assert!(build(Some(-1.0), None).is_err());
        assert!(build(Some(f64::NAN), None).is_err());
        assert!(build(None, Some(0.0)).is_err());
    }

    /// The point-in-time claim, as two real readings: Apple bought back roughly 1.9 billion shares
    /// between these dates, so a 2026 snapshot cannot stand in for a 2021 one.
    #[test]
    fn test_two_dates_carry_their_own_share_counts() {
        let build = |as_of: SessionDate, shares: f64| {
            EquityReference::new(
                ticker("AAPL"),
                as_of,
                Some(SecurityType::CommonStock),
                SicCode::new("3571"),
                None,
                Some(shares),
                None,
                None,
            )
            .expect("the fixture must be constructible")
        };

        let then = build(session(2021, 9, 15), 16_530_169_999.0);
        let now = build(session(2026, 9, 16), 14_594_180_000.0);

        assert_ne!(then.shares_outstanding(), now.shares_outstanding());
        assert_eq!(then.as_of(), session(2021, 9, 15));
        assert!(then.shares_outstanding().unwrap() > now.shares_outstanding().unwrap());
    }

    #[test]
    fn test_a_pair_order_id_names_its_pair_and_round_trips() {
        let pair = PairID::new(Ticker::new("BRK.B").unwrap(), Ticker::new("XOM").unwrap());
        let sent = ClientOrderId::for_pair(&pair, PairLeg::Short);
        let text = sent.to_string();
        assert_eq!(text, format!("pair:BRK.B-XOM:short:{}", sent.uuid()));
        let read = ClientOrderId::parse(&text).expect("what was sent must parse");
        assert_eq!(read, sent);
        assert_eq!(read.pair(), Some((&pair, PairLeg::Short)));
    }

    #[test]
    fn test_an_order_sent_before_the_pair_was_encoded_parses_with_no_pair() {
        let read = ClientOrderId::parse("7885f50f-a68f-4b06-a359-fec24d64b30a")
            .expect("a bare UUID is the old form");
        assert_eq!(read.pair(), None);
        assert_eq!(
            read.uuid().to_string(),
            "7885f50f-a68f-4b06-a359-fec24d64b30a"
        );
    }

    #[test]
    fn test_an_identifier_this_system_did_not_send_is_refused() {
        for raw in [
            "",
            "manual-order-1",
            "pair:BRK.B-XOM:sideways:7885f50f-a68f-4b06-a359-fec24d64b30a",
            "pair:BRKB:long:7885f50f-a68f-4b06-a359-fec24d64b30a",
            "pair:BRK.B-XOM:long:not-a-uuid",
            "pair:BRK.B-XOM:long:7885f50f-a68f-4b06-a359-fec24d64b30a:extra",
            // Spellings the parsers beneath accept and this system never writes.
            "pair:brk.b-XOM:long:7885f50f-a68f-4b06-a359-fec24d64b30a",
            "pair:BRK.B-XOM:long:7885F50F-A68F-4B06-A359-FEC24D64B30A",
            "7885f50fa68f4b06a359fec24d64b30a",
            "{7885f50f-a68f-4b06-a359-fec24d64b30a}",
            "urn:uuid:7885f50f-a68f-4b06-a359-fec24d64b30a",
        ] {
            assert_eq!(ClientOrderId::parse(raw), None, "{raw:?} must not parse");
        }
    }

    #[test]
    fn test_the_longest_pair_order_id_fits_alpacas_limit() {
        let pair = PairID::new(
            Ticker::new("ABCDE.WSA").unwrap(),
            Ticker::new("VWXYZ.WSB").unwrap(),
        );
        let text = ClientOrderId::for_pair(&pair, PairLeg::Short).to_string();
        assert_eq!(text.len(), 67);
        assert!(text.len() <= CLIENT_ORDER_ID_MAXIMUM_LENGTH);
    }

    /// A stored floor goes back through the constructor, so one the constructor would refuse is
    /// refused on read rather than read back as a floor that screens nothing.
    #[test]
    fn test_a_stored_floor_the_constructor_would_refuse_is_refused() {
        let read: LiquidityFloor =
            serde_json::from_str(r#"{"minimum_close_price":10.0,"minimum_dollar_volume":5e7}"#)
                .expect("a valid floor reads");
        assert_eq!(read, LiquidityFloor::CURRENT);
        assert!(serde_json::from_str::<LiquidityFloor>(
            r#"{"minimum_close_price":-1.0,"minimum_dollar_volume":5e7}"#
        )
        .is_err());
    }
}
