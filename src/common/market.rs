//! Market units: symbols, prices and share counts. A price is a whole number of ten-thousandths of a dollar so
//! that every sum over prices is exact; a float appears only when a value is presented.

pub mod aggregate;
pub mod record;

/// Ten-thousandths of a dollar per dollar: every stored price and dollar amount is an integer scaled by this.
pub const PRICE_SCALE: i64 = 10_000;

/// Ten million dollars, far above any listed share, keeping price × shares well inside `i128`.
const MAXIMUM_PRICE_TICKS: i64 = 10_000_000 * PRICE_SCALE;

/// How far a vendor float may sit from the grid, in ticks, and still be read as float noise.
const OFF_GRID_TOLERANCE_TICKS: f64 = 1e-3;

/// An exchange ticker: one to five letters, with an optional `.` and one to three letter class suffix.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(String);

/// Why a symbol was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolRefusal {
    Malformed { raw: String },
}

impl Symbol {
    /// Trims and uppercases, so `" brk.b "` reads as `BRK.B`.
    pub fn new(raw: &str) -> Result<Self, SymbolRefusal> {
        let symbol = raw.trim().to_ascii_uppercase();
        let (root, suffix) = match symbol.split_once('.') {
            Some((root, suffix)) => (root, Some(suffix)),
            None => (symbol.as_str(), None),
        };
        let letters = |part: &str, most: usize| {
            (1..=most).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_uppercase())
        };
        if letters(root, 5) && suffix.is_none_or(|suffix| letters(suffix, 3)) {
            Ok(Self(symbol))
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

impl std::fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A positive price held as a whole number of ticks, `PRICE_SCALE` ticks to the dollar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(i64);

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
    /// Further from the ten-thousandth grid than float noise explains.
    OffGrid {
        dollars: f64,
    },
}

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
        let ticks = scaled.round();
        // The cast saturates, so a huge value still reaches the range check as out of range.
        let price = Self::from_ticks(ticks as i64)?;
        if (scaled - ticks).abs() > OFF_GRID_TOLERANCE_TICKS {
            return Err(PriceRefusal::OffGrid { dollars });
        }
        Ok(price)
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
        write!(
            formatter,
            "{}.{:04}",
            self.0 / PRICE_SCALE,
            self.0 % PRICE_SCALE
        )
    }
}

/// A whole number of shares, where zero is a measurement: an empty book side or a bar nobody traded in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Shares(u64);

impl Shares {
    pub fn new(count: u64) -> Self {
        Self(count)
    }

    pub fn count(self) -> u64 {
        self.0
    }

    pub fn plus(self, other: Self) -> Self {
        Self(self.0.checked_add(other.0).expect("share count fits u64"))
    }
}

impl std::fmt::Display for Shares {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A sum of price × shares in ticks, `PRICE_SCALE` ticks to the dollar.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DollarVolume(i128);

impl DollarVolume {
    pub fn of(price: Price, shares: Shares) -> Self {
        Self(i128::from(price.0) * i128::from(shares.0))
    }

    pub fn plus(self, other: Self) -> Self {
        Self(
            self.0
                .checked_add(other.0)
                .expect("dollar volume fits i128"),
        )
    }

    pub fn ticks(self) -> i128 {
        self.0
    }

    /// The amount in dollars, for presentation only: sums belong on `ticks`.
    pub fn dollars(self) -> f64 {
        self.0 as f64 / PRICE_SCALE as f64
    }
}

impl std::fmt::Display for DollarVolume {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scale = i128::from(PRICE_SCALE);
        write!(formatter, "{}.{:04}", self.0 / scale, self.0 % scale)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_a_symbol_is_trimmed_and_uppercased() {
        assert_eq!(Symbol::new(" brk.b ").unwrap().as_str(), "BRK.B");
        assert_eq!(Symbol::new("AAPL").unwrap().to_string(), "AAPL");
    }

    #[test]
    fn test_a_malformed_symbol_is_refused_with_its_text() {
        for raw in ["", "TOOLONG", "BRK.ABCD", "A1", ".B", "BRK."] {
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
    fn test_the_divisor_is_ten_thousand() {
        let price = Price::from_dollars(123.4567).unwrap();
        assert_eq!(price.ticks(), 1_234_567);
        assert_eq!(price.to_string(), "123.4567");
        assert_eq!(price.dollars(), 123.4567);
        assert_eq!(Price::from_ticks(5).unwrap().to_string(), "0.0005");
    }

    #[test]
    fn test_float_noise_snaps_to_the_grid() {
        assert_eq!(Price::from_dollars(0.1 + 0.2).unwrap().ticks(), 3_000);
    }

    #[test]
    fn test_a_price_off_the_grid_or_out_of_range_is_refused_with_its_value() {
        assert_eq!(
            Price::from_dollars(1.00005),
            Err(PriceRefusal::OffGrid { dollars: 1.00005 })
        );
        assert_eq!(
            Price::from_dollars(0.0),
            Err(PriceRefusal::OutOfRange { ticks: 0 })
        );
        assert_eq!(
            Price::from_dollars(-1.0),
            Err(PriceRefusal::OutOfRange { ticks: -10_000 })
        );
        assert_eq!(
            Price::from_dollars(10_000_000.000_1),
            Err(PriceRefusal::OutOfRange {
                ticks: 100_000_000_001
            })
        );
        assert!(matches!(
            Price::from_dollars(f64::NAN),
            Err(PriceRefusal::NotFinite { .. })
        ));
        assert_eq!(
            Price::from_ticks(100_000_000_000).unwrap().to_string(),
            "10000000.0000"
        );
    }

    #[test]
    fn test_dollar_volume_is_exact() {
        let volume = DollarVolume::of(Price::from_dollars(1.5).unwrap(), Shares::new(3));
        assert_eq!(volume.ticks(), 45_000);
        assert_eq!(volume.plus(volume).to_string(), "9.0000");
        assert_eq!(volume.dollars(), 4.5);
    }

    proptest! {
        #[test]
        fn property_a_symbol_reads_back_as_itself(raw in "[A-Z]{1,5}(\\.[A-Z]{1,3})?") {
            let symbol = Symbol::new(&raw).unwrap();
            prop_assert_eq!(Symbol::new(symbol.as_str()), Ok(symbol));
        }

        #[test]
        fn property_a_price_survives_its_presentation_float(ticks in 1..=MAXIMUM_PRICE_TICKS) {
            let price = Price::from_ticks(ticks).unwrap();
            prop_assert_eq!(Price::from_dollars(price.dollars()), Ok(price));
        }
    }
}
