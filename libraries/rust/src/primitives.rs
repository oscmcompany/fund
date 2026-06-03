//! Zero-cost financial primitive newtypes.
//!
//! These types wrap standard Rust primitives to make unit-mismatch bugs compile
//! errors rather than silent runtime surprises.

use rust_decimal::Decimal;
use serde::{de, Deserialize, Deserializer, Serialize};

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

/// Whole share count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Shares(pub Decimal);

/// Dollar amount.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Dollars(pub Decimal);

/// Percentage in the range `[0.0, 1.0]`.
///
/// The inner field is private; use `Percent::new()` to construct and `value()` to read.
/// Deserialization validates the range, preventing out-of-range values from serde.
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

/// Long position size (always a positive magnitude).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Long(pub Decimal);

/// Short position size (stored as a positive magnitude).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Short(pub Decimal);

/// Notional dollar amount.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notional(pub Dollars);

/// Portfolio allocation expressed as a fraction of total capital.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Allocation(pub Percent);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percent_new_accepts_zero() {
        let percent = Percent::new(0.0).unwrap();
        assert_eq!(percent.value(), 0.0);
    }

    #[test]
    fn test_percent_new_accepts_one() {
        let percent = Percent::new(1.0).unwrap();
        assert_eq!(percent.0, 1.0);
    }

    #[test]
    fn test_percent_new_accepts_midpoint() {
        let percent = Percent::new(0.5).unwrap();
        assert_eq!(percent.0, 0.5);
    }

    #[test]
    fn test_percent_new_rejects_negative() {
        let error = Percent::new(-0.1).unwrap_err();
        assert_eq!(error.value, -0.1);
        assert_eq!(error.minimum, 0.0);
        assert_eq!(error.maximum, 1.0);
    }

    #[test]
    fn test_percent_new_rejects_above_one() {
        let error = Percent::new(1.1).unwrap_err();
        assert_eq!(error.value, 1.1);
    }

    #[test]
    fn test_range_error_display() {
        let error = RangeError {
            value: 1.5,
            minimum: 0.0,
            maximum: 1.0,
        };
        let message = format!("{}", error);
        assert!(message.contains("1.5"));
        assert!(message.contains("0"));
        assert!(message.contains("1"));
    }

    #[test]
    fn test_percent_ordering() {
        let low = Percent::new(0.2).unwrap();
        let high = Percent::new(0.8).unwrap();
        assert!(low < high);
        assert!(high > low);
    }

    #[test]
    fn test_percent_copy() {
        let original = Percent::new(0.5).unwrap();
        let copy = original;
        assert_eq!(original, copy);
    }

    #[test]
    fn test_shares_construction() {
        let shares = Shares(Decimal::from(100));
        assert_eq!(shares.0, Decimal::from(100));
    }

    #[test]
    fn test_dollars_construction() {
        let dollars = Dollars(Decimal::from(5000));
        assert_eq!(dollars.0, Decimal::from(5000));
    }

    #[test]
    fn test_long_construction() {
        let long = Long(Decimal::from(50));
        assert_eq!(long.0, Decimal::from(50));
    }

    #[test]
    fn test_short_construction() {
        let short = Short(Decimal::from(50));
        assert_eq!(short.0, Decimal::from(50));
    }

    #[test]
    fn test_notional_construction() {
        let notional = Notional(Dollars(Decimal::from(10_000)));
        assert_eq!(notional.0 .0, Decimal::from(10_000));
    }

    #[test]
    fn test_allocation_construction() {
        let allocation = Allocation(Percent::new(0.25).unwrap());
        assert_eq!(allocation.0.value(), 0.25);
    }
}
