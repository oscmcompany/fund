//! Market units: symbols, prices and share counts. Prices and share counts are whole numbers of millionths so that
//! every sum is exact; a float appears only when a value is presented.

pub mod aggregate;
pub mod corporate_actions;
pub mod quote_bars;
pub mod record;
pub mod security_details;
pub mod state;
pub mod trade_bars;

use crate::common::monoid::Monoid;

/// Millionths of a dollar per dollar: consolidated prints reach six decimals (midpoint and average-price trades), so
/// every stored price is an integer scaled by this.
pub const PRICE_SCALE: i64 = 1_000_000;

/// Ten million dollars, far above any listed share, keeping price × shares well inside `u128`.
const MAXIMUM_PRICE_TICKS: i64 = 10_000_000 * PRICE_SCALE;

/// `scaled` rounded to the nearest integer, or `None` when it misses by more than float noise explains: twice the
/// float's spacing at that magnitude, and never less than a thousandth of a unit.
fn snap(scaled: f64) -> Option<f64> {
    let nearest = scaled.round();
    let magnitude = scaled.abs();
    let spacing = f64::from_bits(magnitude.to_bits() + 1) - magnitude;
    ((scaled - nearest).abs() <= (2.0 * spacing).max(1e-3)).then_some(nearest)
}

/// `whole.fraction`, the fraction written to `digits` places and trimmed of trailing zeros down to `minimum`.
fn write_decimal(
    formatter: &mut std::fmt::Formatter<'_>,
    whole: u128,
    fraction: u128,
    digits: usize,
    minimum: usize,
) -> std::fmt::Result {
    let text = format!("{fraction:0digits$}");
    let kept = text.trim_end_matches('0').len().max(minimum);
    match kept {
        0 => write!(formatter, "{whole}"),
        kept => write!(formatter, "{whole}.{}", &text[..kept]),
    }
}

/// An exchange ticker: one to five letters, with an optional `.` and one to three letter class suffix.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct Symbol(String);

/// Why a symbol was refused.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolRefusal {
    Malformed { raw: String },
}

impl Symbol {
    /// Taken exactly as written: case carries meaning in vendor notation (Massive's preferred `BCpC` is not the
    /// common stock `BCPC`), so a reader maps its vendor's form before this and nothing here normalizes.
    pub fn new(raw: &str) -> Result<Self, SymbolRefusal> {
        let (root, suffix) = match raw.split_once('.') {
            Some((root, suffix)) => (root, Some(suffix)),
            None => (raw, None),
        };
        let letters = |part: &str, most: usize| {
            (1..=most).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_uppercase())
        };
        if letters(root, 5) && suffix.is_none_or(|suffix| letters(suffix, 3)) {
            Ok(Self(raw.to_string()))
        } else {
            Err(SymbolRefusal::Malformed {
                raw: raw.to_string(),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SymbolRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed { raw } => write!(formatter, "`{raw}` is not a ticker"),
        }
    }
}

impl std::error::Error for SymbolRefusal {}

impl TryFrom<String> for Symbol {
    type Error = SymbolRefusal;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::new(&raw)
    }
}

impl From<Symbol> for String {
    fn from(symbol: Symbol) -> Self {
        symbol.0
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A positive price held as a whole number of ticks, `PRICE_SCALE` ticks to the dollar.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "i64", into = "i64")]
pub struct Price(i64);

impl TryFrom<i64> for Price {
    type Error = PriceRefusal;

    fn try_from(ticks: i64) -> Result<Self, Self::Error> {
        Self::from_ticks(ticks)
    }
}

impl From<Price> for i64 {
    fn from(price: Price) -> Self {
        price.0
    }
}

/// Why a price was refused.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PriceRefusal {
    NotFinite {
        dollars: f64,
    },
    /// Not positive, or above ten million dollars.
    OutOfRange {
        ticks: i64,
    },
    /// Further from the millionth grid than float noise explains.
    OffGrid {
        dollars: f64,
    },
}

impl std::fmt::Display for PriceRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFinite { dollars } => {
                write!(formatter, "{dollars} dollars is not a finite price")
            }
            Self::OutOfRange { ticks } => write!(
                formatter,
                "{ticks} ticks is not above zero and at most ten million dollars"
            ),
            Self::OffGrid { dollars } => {
                write!(formatter, "{dollars} dollars is finer than a millionth")
            }
        }
    }
}

impl std::error::Error for PriceRefusal {}

impl Price {
    /// A price read back from its stored integer.
    pub fn from_ticks(ticks: i64) -> Result<Self, PriceRefusal> {
        if (1..=MAXIMUM_PRICE_TICKS).contains(&ticks) {
            Ok(Self(ticks))
        } else {
            Err(PriceRefusal::OutOfRange { ticks })
        }
    }

    /// A vendor's float price, snapped to the grid when it misses by float noise and refused when it misses by more.
    pub fn from_dollars(dollars: f64) -> Result<Self, PriceRefusal> {
        if !dollars.is_finite() {
            return Err(PriceRefusal::NotFinite { dollars });
        }
        let scaled = dollars * PRICE_SCALE as f64;
        // The cast saturates, so a huge value still reaches the range check as out of range.
        let price = Self::from_ticks(scaled.round() as i64)?;
        match snap(scaled) {
            Some(_) => Ok(price),
            None => Err(PriceRefusal::OffGrid { dollars }),
        }
    }

    pub fn ticks(self) -> i64 {
        self.0
    }

    /// The price in dollars, for presentation only: sums and comparisons belong on `ticks`.
    pub fn dollars(self) -> f64 {
        self.0 as f64 / PRICE_SCALE as f64
    }
}

impl std::fmt::Display for Price {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (whole, fraction) = (self.0 / PRICE_SCALE, self.0 % PRICE_SCALE);
        write_decimal(
            formatter,
            u128::from(whole.unsigned_abs()),
            u128::from(fraction.unsigned_abs()),
            6,
            2,
        )
    }
}

/// Millionths of a share per share: fractional-share trades report to six decimals, so every share count is an
/// integer scaled by this.
pub const SHARE_SCALE: u64 = 1_000_000;

/// A number of shares held in millionths, `SHARE_SCALE` to the share, where zero is a measurement: an empty book
/// side or a bar nobody traded in.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Shares(u64);

/// Why a share count was refused.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SharesRefusal {
    NotFinite {
        shares: f64,
    },
    /// Negative, or past `u64` once scaled.
    OutOfRange {
        shares: f64,
    },
    /// Finer than a millionth by more than float precision explains.
    OffGrid {
        shares: f64,
    },
}

impl std::fmt::Display for SharesRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFinite { shares } => {
                write!(formatter, "{shares} shares is not a finite count")
            }
            Self::OutOfRange { shares } => write!(
                formatter,
                "{shares} shares is negative or past what millionths in a u64 hold"
            ),
            Self::OffGrid { shares } => {
                write!(formatter, "{shares} shares is finer than a millionth")
            }
        }
    }
}

impl std::error::Error for SharesRefusal {}

impl Shares {
    /// Whole shares, as SIP quotes and trades report them.
    pub fn whole(count: u64) -> Result<Self, SharesRefusal> {
        count
            .checked_mul(SHARE_SCALE)
            .map(Self)
            .ok_or(SharesRefusal::OutOfRange {
                shares: count as f64,
            })
    }

    /// A share count read back from its stored integer.
    pub fn from_units(units: u64) -> Self {
        Self(units)
    }

    /// A vendor's float share count, snapped to the grid when it misses by float noise and refused when it misses by
    /// more. The tolerance is twice the float's spacing at that magnitude, so half a millionth off the grid is caught
    /// below about 1.1 billion shares; above that a float cannot place a millionth, and the count is rounded.
    pub fn from_float(shares: f64) -> Result<Self, SharesRefusal> {
        if !shares.is_finite() {
            return Err(SharesRefusal::NotFinite { shares });
        }
        let scaled = shares * SHARE_SCALE as f64;
        // Checked before rounding, so negative noise cannot round into a valid zero.
        if shares < 0.0 || scaled >= u64::MAX as f64 {
            return Err(SharesRefusal::OutOfRange { shares });
        }
        match snap(scaled) {
            Some(units) => Ok(Self(units as u64)),
            None => Err(SharesRefusal::OffGrid { shares }),
        }
    }

    pub fn units(self) -> u64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn plus(self, other: Self) -> Self {
        self.checked_plus(other).expect("share count fits u64")
    }

    /// The sum, `None` when it passes what a `u64` of millionths holds.
    pub fn checked_plus(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    /// The count in shares, for presentation only: sums belong on `units`.
    pub fn shares(self) -> f64 {
        self.0 as f64 / SHARE_SCALE as f64
    }
}

impl Monoid for Shares {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(self, other: Self) -> Self {
        self.plus(other)
    }
}

impl std::fmt::Display for Shares {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_decimal(
            formatter,
            u128::from(self.0 / SHARE_SCALE),
            u128::from(self.0 % SHARE_SCALE),
            6,
            0,
        )
    }
}

/// A number of trades, kept apart from `Shares` so the two counts cannot be swapped.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct TradeCount(u64);

impl TradeCount {
    pub fn new(count: u64) -> Self {
        Self(count)
    }

    pub fn count(self) -> u64 {
        self.0
    }

    pub fn plus(self, other: Self) -> Self {
        Self(self.0.checked_add(other.0).expect("trade count fits u64"))
    }
}

impl Monoid for TradeCount {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(self, other: Self) -> Self {
        self.plus(other)
    }
}

impl std::fmt::Display for TradeCount {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A sum of price × shares, held in ticks × millionths of a share: `PRICE_SCALE × SHARE_SCALE` to the dollar.
/// Unsigned, since a positive price times a share count cannot be negative.
// Written as a decimal string, since serde's buffer for a tagged journal record cannot hold a u128.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct DollarVolume(u128);

impl TryFrom<String> for DollarVolume {
    type Error = std::num::ParseIntError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse().map(Self)
    }
}

impl From<DollarVolume> for String {
    fn from(value: DollarVolume) -> Self {
        value.0.to_string()
    }
}

/// Why a vendor's average price was not turned into a dollar volume.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DollarVolumeRefusal {
    /// Not finite, or negative.
    Invalid { average: f64 },
    /// Implies a dollar volume past the integer's range.
    OutOfRange { average: f64 },
}

impl std::fmt::Display for DollarVolumeRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid { average } => write!(
                formatter,
                "an average price of {average} is not finite and non-negative"
            ),
            Self::OutOfRange { average } => write!(
                formatter,
                "an average price of {average} implies a dollar volume past its range"
            ),
        }
    }
}

impl std::error::Error for DollarVolumeRefusal {}

impl DollarVolume {
    pub fn of(price: Price, shares: Shares) -> Self {
        Self(u128::from(price.0.unsigned_abs()) * u128::from(shares.0))
    }

    /// The dollar volume a vendor's volume-weighted average price implies. It is exact to the float precision of the
    /// average, and from here on it sums exactly, so a derived bar's average is recovered from sums, not averages.
    pub fn from_average(average: f64, volume: Shares) -> Result<Self, DollarVolumeRefusal> {
        if !average.is_finite() || average < 0.0 {
            return Err(DollarVolumeRefusal::Invalid { average });
        }
        if volume.is_zero() {
            return Ok(Self::default());
        }
        let units = average * PRICE_SCALE as f64 * volume.0 as f64;
        // A cast would saturate silently, so a product past the range is refused instead.
        if !units.is_finite() || units >= u128::MAX as f64 {
            return Err(DollarVolumeRefusal::OutOfRange { average });
        }
        Ok(Self(units.round() as u128))
    }

    pub fn plus(self, other: Self) -> Self {
        Self(
            self.0
                .checked_add(other.0)
                .expect("dollar volume fits u128"),
        )
    }

    /// A dollar volume read back from its stored integer.
    pub const fn from_units(units: u128) -> Self {
        Self(units)
    }

    pub fn units(self) -> u128 {
        self.0
    }

    /// The amount in dollars, for presentation only: sums belong on `units`.
    pub fn dollars(self) -> f64 {
        self.0 as f64 / (PRICE_SCALE as f64 * SHARE_SCALE as f64)
    }

    /// The average price in dollars over `volume`, for presentation; `None` when nothing traded.
    pub fn average_over(self, volume: Shares) -> Option<f64> {
        match volume.0 {
            0 => None,
            units => Some(self.0 as f64 / PRICE_SCALE as f64 / units as f64),
        }
    }
}

impl Monoid for DollarVolume {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(self, other: Self) -> Self {
        self.plus(other)
    }
}

impl std::fmt::Display for DollarVolume {
    /// Rounded to the tick, which is as fine as a dollar amount is presented.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let per_tick = u128::from(SHARE_SCALE);
        let ticks = self.0 / per_tick + u128::from(self.0 % per_tick >= per_tick / 2);
        let scale = u128::from(PRICE_SCALE.unsigned_abs());
        write_decimal(formatter, ticks / scale, ticks % scale, 6, 2)
    }
}

/// A non-negative amount of money in millionths of a dollar, `PRICE_SCALE` to the dollar, such as a study's spend.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Dollars(u64);

/// Why an amount was refused.
#[derive(Debug, Clone, PartialEq)]
pub enum DollarsRefusal {
    /// Negative, not finite, too large, or finer than a millionth.
    Unrepresentable {
        dollars: f64,
    },
    Unparsable {
        raw: String,
    },
}

impl std::fmt::Display for DollarsRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unrepresentable { dollars } => {
                write!(
                    formatter,
                    "{dollars} is not a non-negative whole number of millionths of a dollar"
                )
            }
            Self::Unparsable { raw } => {
                write!(
                    formatter,
                    "`{raw}` is not dollars with at most six decimal places"
                )
            }
        }
    }
}

impl std::error::Error for DollarsRefusal {}

impl Dollars {
    pub fn from_millionths(millionths: u64) -> Self {
        Self(millionths)
    }

    pub fn from_float(dollars: f64) -> Result<Self, DollarsRefusal> {
        // Refused before snapping, which would round a tiny negative to zero.
        if dollars < 0.0 {
            return Err(DollarsRefusal::Unrepresentable { dollars });
        }
        let scaled = dollars * PRICE_SCALE as f64;
        snap(scaled)
            .filter(|nearest| (0.0..u64::MAX as f64).contains(nearest))
            .map(|nearest| Self(nearest as u64))
            .ok_or(DollarsRefusal::Unrepresentable { dollars })
    }

    pub fn millionths(self) -> u64 {
        self.0
    }
}

impl std::str::FromStr for Dollars {
    type Err = DollarsRefusal;

    /// Read digit by digit, so no amount passes through a float: up to six decimal places, no sign.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let refused = || DollarsRefusal::Unparsable {
            raw: raw.to_string(),
        };
        let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
        let digits =
            |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
        let fraction_allowed = match raw.contains('.') {
            true => digits(fraction) && fraction.len() <= 6,
            false => true,
        };
        if !digits(whole) || !fraction_allowed {
            return Err(refused());
        }
        let scale = PRICE_SCALE.unsigned_abs();
        let fraction = format!("{fraction:0<6}");
        whole
            .parse::<u64>()
            .ok()
            .and_then(|whole| whole.checked_mul(scale))
            .zip(fraction.parse::<u64>().ok())
            .and_then(|(whole, fraction)| whole.checked_add(fraction))
            .map(Self)
            .ok_or_else(refused)
    }
}

impl std::fmt::Display for Dollars {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scale = PRICE_SCALE.unsigned_abs();
        write_decimal(
            formatter,
            u128::from(self.0 / scale),
            u128::from(self.0 % scale),
            6,
            2,
        )
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::monoid::laws;

    #[test]
    fn test_dollars_are_exact_millionths() {
        let dollars = |raw: &str| raw.parse::<Dollars>();
        assert_eq!(dollars("1.49").map(Dollars::millionths), Ok(1_490_000));
        assert_eq!(dollars("0.001").map(Dollars::millionths), Ok(1_000));
        assert_eq!(dollars("0").map(Dollars::millionths), Ok(0));
        assert_eq!(Dollars::from_millionths(1_000).to_string(), "0.001");
        assert_eq!(Dollars::from_millionths(1_490_000).to_string(), "1.49");
        for refused in [
            "-0.01",
            "0.0000001",
            "NaN",
            "inf",
            "",
            "1e30",
            ".5",
            "5.",
            "1,000",
            "18446744073710",
        ] {
            assert!(dollars(refused).is_err(), "{refused}");
        }
        assert_eq!(
            Dollars::from_float(0.001).map(Dollars::millionths),
            Ok(1_000)
        );
        for refused in [-0.01, -1e-10, 1e-7, f64::NAN, f64::INFINITY] {
            assert!(Dollars::from_float(refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn test_a_symbol_is_taken_as_written() {
        assert_eq!(Symbol::new("BRK.B").unwrap().as_str(), "BRK.B");
        assert_eq!(Symbol::new("AAPL").unwrap().to_string(), "AAPL");
    }

    #[test]
    fn test_a_malformed_symbol_is_refused_with_its_text() {
        for raw in [
            "", "TOOLONG", "BRK.ABCD", "A1", ".B", "BRK.", "BCpC", "brk.b", " BRK.B ",
        ] {
            assert_eq!(
                Symbol::new(raw),
                Err(SymbolRefusal::Malformed {
                    raw: raw.to_string()
                }),
                "{raw}"
            );
        }
    }

    #[test]
    fn test_the_price_divisor_is_a_million() {
        let price = Price::from_dollars(123.4567).unwrap();
        assert_eq!(price.ticks(), 123_456_700);
        assert_eq!(price.to_string(), "123.4567");
        assert_eq!(price.dollars(), 123.4567);
        assert_eq!(Price::from_ticks(500).unwrap().to_string(), "0.0005");
        assert_eq!(Price::from_ticks(5).unwrap().to_string(), "0.000005");
        assert_eq!(Price::from_ticks(73_730_000).unwrap().to_string(), "73.73");
        // A midpoint print at half a sub-penny, from the 2026-09-25 grouped daily.
        assert_eq!(Price::from_dollars(0.18205).unwrap().ticks(), 182_050);
    }

    #[test]
    fn test_float_noise_snaps_to_the_grid() {
        assert_eq!(Price::from_dollars(0.1 + 0.2).unwrap().ticks(), 300_000);
    }

    #[test]
    fn test_a_price_off_the_grid_or_out_of_range_is_refused_with_its_value() {
        assert_eq!(
            Price::from_dollars(1.000_000_5),
            Err(PriceRefusal::OffGrid {
                dollars: 1.000_000_5
            })
        );
        assert_eq!(
            Price::from_dollars(0.0),
            Err(PriceRefusal::OutOfRange { ticks: 0 })
        );
        assert_eq!(
            Price::from_dollars(-1.0),
            Err(PriceRefusal::OutOfRange { ticks: -1_000_000 })
        );
        assert_eq!(
            Price::from_dollars(10_000_000.000_1),
            Err(PriceRefusal::OutOfRange {
                ticks: 10_000_000_000_100
            })
        );
        assert!(matches!(
            Price::from_dollars(f64::NAN),
            Err(PriceRefusal::NotFinite { .. })
        ));
        assert_eq!(
            Price::from_ticks(10_000_000_000_000).unwrap().to_string(),
            "10000000.00"
        );
    }

    #[test]
    fn test_a_stored_price_is_refused_with_its_typed_cause() {
        assert_eq!(
            Price::try_from(0),
            Err(PriceRefusal::OutOfRange { ticks: 0 })
        );
        assert_eq!(
            serde_json::from_str::<Price>("-5").unwrap_err().to_string(),
            "-5 ticks is not above zero and at most ten million dollars"
        );
    }

    #[test]
    fn test_dollar_volume_is_exact() {
        let volume = DollarVolume::of(Price::from_dollars(1.5).unwrap(), Shares::whole(3).unwrap());
        assert_eq!(volume.units(), 4_500_000_000_000);
        assert_eq!(volume.plus(volume).to_string(), "9.00");
        assert_eq!(volume.dollars(), 4.5);
        assert_eq!(volume.average_over(Shares::whole(3).unwrap()), Some(1.5));
        assert_eq!(volume.average_over(Shares::default()), None);
    }

    #[test]
    fn test_the_share_divisor_is_a_million() {
        let fractional = Shares::from_float(213_849.305_802).unwrap();
        assert_eq!(fractional.units(), 213_849_305_802);
        assert_eq!(fractional.to_string(), "213849.305802");
        assert_eq!(Shares::whole(100).unwrap().to_string(), "100");
        assert_eq!(Shares::from_units(1_500_000).to_string(), "1.5");
    }

    #[test]
    fn test_share_float_noise_snaps_and_finer_counts_are_refused() {
        // A summed float from the grouped daily, carrying sixteen decimals of noise.
        assert_eq!(
            Shares::from_float(16_902.906_000_000_003).unwrap().units(),
            16_902_906_000
        );
        assert_eq!(
            Shares::from_float(518_029_038.771_163).unwrap().units(),
            518_029_038_771_163
        );
        assert_eq!(
            Shares::from_float(1.000_000_5),
            Err(SharesRefusal::OffGrid {
                shares: 1.000_000_5
            })
        );
        assert_eq!(
            Shares::from_float(-1.0),
            Err(SharesRefusal::OutOfRange { shares: -1.0 })
        );
        assert_eq!(
            Shares::from_float(-1e-10),
            Err(SharesRefusal::OutOfRange { shares: -1e-10 })
        );
        // Half a millionth off the grid is still detectable at 600 million shares.
        assert_eq!(
            Shares::from_float(600_000_000.000_000_5),
            Err(SharesRefusal::OffGrid {
                shares: 600_000_000.000_000_5
            })
        );
        assert!(matches!(
            Shares::from_float(f64::NAN),
            Err(SharesRefusal::NotFinite { .. })
        ));
        assert_eq!(
            Shares::whole(u64::MAX),
            Err(SharesRefusal::OutOfRange {
                shares: u64::MAX as f64
            })
        );
    }

    #[test]
    fn test_a_vendor_average_becomes_a_dollar_volume_it_can_be_recovered_from() {
        let volume = Shares::whole(436_147).unwrap();
        let dollar_volume = DollarVolume::from_average(335.929_638, volume).unwrap();
        let recovered = dollar_volume.average_over(volume).unwrap();
        assert!((recovered - 335.929_638).abs() < 1e-9, "{recovered}");
        assert_eq!(
            DollarVolume::from_average(0.0, Shares::default()),
            Ok(DollarVolume::default())
        );
        assert_eq!(
            DollarVolume::from_average(-1.0, volume),
            Err(DollarVolumeRefusal::Invalid { average: -1.0 })
        );
        assert_eq!(
            DollarVolume::from_average(1e300, volume),
            Err(DollarVolumeRefusal::OutOfRange { average: 1e300 })
        );
        assert_eq!(
            DollarVolume::from_average(f64::MAX, Shares::default()),
            Ok(DollarVolume::default())
        );
    }

    proptest! {
        #[test]
        fn property_a_symbol_reads_back_as_itself(raw in "[A-Z]{1,5}(\\.[A-Z]{1,3})?") {
            let symbol = Symbol::new(&raw).unwrap();
            prop_assert_eq!(Symbol::new(symbol.as_str()), Ok(symbol));
        }

        /// Exact below a billion shares; past that the division by `SHARE_SCALE` rounds in the float.
        #[test]
        fn property_shares_survive_their_presentation_float(units in 0_u64..1_000_000_000_000_000) {
            let shares = Shares::from_units(units);
            prop_assert_eq!(Shares::from_float(shares.shares()), Ok(shares));
        }

        #[test]
        fn property_a_price_survives_its_presentation_float(ticks in 1..=MAXIMUM_PRICE_TICKS) {
            let price = Price::from_ticks(ticks).unwrap();
            prop_assert_eq!(Price::from_dollars(price.dollars()), Ok(price));
        }
    }

    proptest! {
        /// Bounded to a third of the range, so no sum of three overflows into the panic the laws do not cover.
        #[test]
        fn property_shares_are_a_commutative_monoid(units in prop::array::uniform3(0_u64..u64::MAX / 3)) {
            let [first, second, third] = units.map(Shares::from_units);
            laws::check(first, second, third)?;
        }

        #[test]
        fn property_trade_counts_are_a_commutative_monoid(counts in prop::array::uniform3(0_u64..u64::MAX / 3)) {
            let [first, second, third] = counts.map(TradeCount::new);
            laws::check(first, second, third)?;
        }

        #[test]
        fn property_dollar_volumes_are_a_commutative_monoid(units in prop::array::uniform3(0_u128..u128::MAX / 3)) {
            let [first, second, third] = units.map(DollarVolume::from_units);
            laws::check(first, second, third)?;
        }

        #[test]
        fn property_a_dollar_volume_round_trips_through_its_decimal_string(units in any::<u128>()) {
            let written = serde_json::to_string(&DollarVolume::from_units(units)).unwrap();
            prop_assert_eq!(&written, &format!("\"{units}\""));
            prop_assert_eq!(serde_json::from_str::<DollarVolume>(&written).unwrap(), DollarVolume::from_units(units));
        }
    }

    proptest! {
        #[test]
        fn property_dollars_read_back_from_their_display(millionths in any::<u64>()) {
            let dollars = Dollars::from_millionths(millionths);
            prop_assert_eq!(dollars.to_string().parse::<Dollars>(), Ok(dollars));
        }
    }
}
