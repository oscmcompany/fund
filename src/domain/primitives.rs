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
///
/// The inner field is private; use `Shares::new()` to construct and `value()` to read.
/// Deserialization validates positivity, preventing zero or negative quantities from serde.
#[derive(Debug, Clone, PartialEq, Serialize)]
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
/// Every current use is a magnitude — fill notionals and limit prices — so the
/// constructor rejects negative values. The inner field is private; use
/// `Dollars::new()` to construct and `value()` to read. Deserialization
/// validates non-negativity, preventing negative amounts from serde.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
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

/// Long position size (a non-negative magnitude).
///
/// The inner field is private; use `Long::new()` to construct and `value()` to read.
/// Deserialization validates non-negativity, preventing negative sizes from serde.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Long(Decimal);

impl Long {
    /// Creates a new `Long`, returning an error if `size` is negative.
    pub fn new(size: Decimal) -> Result<Self, NegativeAmountError> {
        if size < Decimal::ZERO {
            return Err(NegativeAmountError { amount: size });
        }
        Ok(Long(size))
    }

    /// Returns the inner `Decimal` value.
    pub fn value(self) -> Decimal {
        self.0
    }
}

impl<'de> Deserialize<'de> for Long {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <Decimal as Deserialize<'de>>::deserialize(deserializer)?;
        Long::new(raw).map_err(de::Error::custom)
    }
}

/// Short position size (stored as a non-negative magnitude).
///
/// The inner field is private; use `Short::new()` to construct and `value()` to read.
/// Deserialization validates non-negativity, preventing negative sizes from serde.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Short(Decimal);

impl Short {
    /// Creates a new `Short`, returning an error if `size` is negative.
    pub fn new(size: Decimal) -> Result<Self, NegativeAmountError> {
        if size < Decimal::ZERO {
            return Err(NegativeAmountError { amount: size });
        }
        Ok(Short(size))
    }

    /// Returns the inner `Decimal` value.
    pub fn value(self) -> Decimal {
        self.0
    }
}

impl<'de> Deserialize<'de> for Short {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <Decimal as Deserialize<'de>>::deserialize(deserializer)?;
        Short::new(raw).map_err(de::Error::custom)
    }
}

/// Notional dollar amount.
///
/// Wraps an already-validated [`Dollars`], so construction is infallible: the
/// `Dollars` in hand is proof the amount is non-negative.
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

/// Portfolio allocation expressed as a fraction of total capital.
///
/// Wraps an already-validated [`Percent`], so construction is infallible: the
/// `Percent` in hand is proof the fraction is in `[0.0, 1.0]`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Allocation(Percent);

impl Allocation {
    /// Creates a new `Allocation` from a validated `Percent` fraction.
    pub fn new(fraction: Percent) -> Self {
        Allocation(fraction)
    }

    /// Returns the inner `Percent` value.
    pub fn value(self) -> Percent {
        self.0
    }
}

impl<'de> Deserialize<'de> for Allocation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fraction = Percent::deserialize(deserializer)?;
        Ok(Allocation::new(fraction))
    }
}

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
        assert_eq!(percent.value(), 1.0);
    }

    #[test]
    fn test_percent_new_accepts_midpoint() {
        let percent = Percent::new(0.5).unwrap();
        assert_eq!(percent.value(), 0.5);
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
    fn test_shares_new_accepts_positive() {
        let shares = Shares::new(Decimal::from(100)).unwrap();
        assert_eq!(shares.value(), Decimal::from(100));
    }

    #[test]
    fn test_shares_new_rejects_zero() {
        let error = Shares::new(Decimal::ZERO).unwrap_err();
        assert_eq!(error, SharesError);
    }

    #[test]
    fn test_shares_new_rejects_negative() {
        let error = Shares::new(Decimal::from(-1)).unwrap_err();
        assert_eq!(error, SharesError);
    }

    #[test]
    fn test_shares_error_display() {
        assert!(format!("{}", SharesError).contains("positive"));
    }

    #[test]
    fn test_dollars_new_accepts_zero() {
        let dollars = Dollars::new(Decimal::ZERO).unwrap();
        assert_eq!(dollars.value(), Decimal::ZERO);
    }

    #[test]
    fn test_dollars_new_accepts_positive() {
        let dollars = Dollars::new(Decimal::from(5000)).unwrap();
        assert_eq!(dollars.value(), Decimal::from(5000));
    }

    #[test]
    fn test_dollars_new_rejects_negative() {
        let error = Dollars::new(Decimal::from(-1)).unwrap_err();
        assert_eq!(error.amount, Decimal::from(-1));
    }

    #[test]
    fn test_dollars_deserialize_rejects_negative() {
        let result: Result<Dollars, _> = serde_json::from_str("\"-5\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_negative_amount_error_display() {
        let error = NegativeAmountError {
            amount: Decimal::from(-5),
        };
        let message = format!("{}", error);
        assert!(message.contains("-5"));
        assert!(message.contains("non-negative"));
    }

    #[test]
    fn test_long_new_accepts_non_negative() {
        let long = Long::new(Decimal::from(50)).unwrap();
        assert_eq!(long.value(), Decimal::from(50));
        assert!(Long::new(Decimal::ZERO).is_ok());
    }

    #[test]
    fn test_long_new_rejects_negative() {
        assert!(Long::new(Decimal::from(-50)).is_err());
    }

    #[test]
    fn test_short_new_accepts_non_negative() {
        let short = Short::new(Decimal::from(50)).unwrap();
        assert_eq!(short.value(), Decimal::from(50));
        assert!(Short::new(Decimal::ZERO).is_ok());
    }

    #[test]
    fn test_short_new_rejects_negative() {
        assert!(Short::new(Decimal::from(-50)).is_err());
    }

    #[test]
    fn test_notional_wraps_validated_dollars() {
        let notional = Notional::new(Dollars::new(Decimal::from(10_000)).unwrap());
        assert_eq!(notional.value().value(), Decimal::from(10_000));
    }

    #[test]
    fn test_notional_deserialize_rejects_negative() {
        let result: Result<Notional, _> = serde_json::from_str("\"-1\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_allocation_wraps_validated_percent() {
        let allocation = Allocation::new(Percent::new(0.25).unwrap());
        assert_eq!(allocation.value().value(), 0.25);
    }

    #[test]
    fn test_allocation_deserialize_rejects_out_of_range() {
        let result: Result<Allocation, _> = serde_json::from_str("1.5");
        assert!(result.is_err());
    }
}
