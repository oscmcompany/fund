//! What a fill costs: a round trip read off a quoted spread in basis points for a bar replay, and per share with its
//! error under a fee regime (`model`, `fees`), with basis points only to report.

pub mod fees;
pub mod model;

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::common::book::Side;
use crate::common::market::{MAXIMUM_PRICE_TICKS, PRICE_SCALE, Price};

/// The bound on a price, so a sum of a few amounts never overflows.
const MAXIMUM_PER_SHARE: i64 = MAXIMUM_PRICE_TICKS;

/// A signed amount of money per share in millionths of a dollar (`PRICE_SCALE`): a cost when charged, an edge when
/// earned, and a credit when negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PerShare(i64);

/// An amount per share refused for lying beyond ten million dollars either side of zero, with the amount offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{millionths} millionths of a dollar per share is beyond ten million dollars")]
pub struct PerShareRefusal {
    pub millionths: i64,
}

impl PerShare {
    pub const ZERO: Self = Self(0);

    pub fn from_millionths(millionths: i64) -> Result<Self, PerShareRefusal> {
        match (-MAXIMUM_PER_SHARE..=MAXIMUM_PER_SHARE).contains(&millionths) {
            true => Ok(Self(millionths)),
            false => Err(PerShareRefusal { millionths }),
        }
    }

    pub fn millionths(self) -> i64 {
        self.0
    }

    /// The amount in cents, for presentation only.
    pub fn cents(self) -> f64 {
        self.0 as f64 / (PRICE_SCALE / 100) as f64
    }

    /// The amount in basis points of `price`, for presentation only: a per-share charge is compared in cents.
    pub fn basis_points(self, price: Price) -> f64 {
        10_000.0 * self.0 as f64 / price.ticks() as f64
    }

    fn plus(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }

    fn minus(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl std::fmt::Display for PerShare {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}c/share", self.cents())
    }
}

/// Which print fills a resting order: the two ends of the fill bracket a resting result must hold under both of.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FillBound {
    /// A print at the limit fills it, as though the order stood first in the queue.
    Optimistic,
    /// Only a print through the limit fills it, which no place in the queue could have missed.
    Conservative,
}

impl FillBound {
    /// Whether `print` fills a resting `side` order at `limit` under this bound.
    pub fn fills(self, side: Side, limit: Price, print: Price) -> bool {
        match (self, side) {
            (Self::Optimistic, Side::Buy) => print <= limit,
            (Self::Optimistic, Side::Sell) => print >= limit,
            (Self::Conservative, Side::Buy) => print < limit,
            (Self::Conservative, Side::Sell) => print > limit,
        }
    }
}

/// A non-negative, finite width or charge in basis points, with a negative zero stored as zero.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64")]
pub struct BasisPoints(f64);

/// A basis-point reading refused for being negative or not finite, with the value read.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("{value} is not a finite, non-negative basis-point reading")]
pub struct BasisPointsRefusal {
    pub value: f64,
}

impl BasisPoints {
    pub fn new(value: f64) -> Result<Self, BasisPointsRefusal> {
        match value.is_finite() && value >= 0.0 {
            true => Ok(Self(value + 0.0)),
            false => Err(BasisPointsRefusal { value }),
        }
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for BasisPoints {
    type Error = BasisPointsRefusal;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl std::fmt::Display for BasisPoints {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}bp", self.0)
    }
}

/// How an order reaches the book; only a crossing order's cost can be read off a quoted spread.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FillStyle {
    /// Crosses the book, paying half the quoted spread on each crossing.
    Aggressive,
    /// Rests at the touch, filling only when the market comes to it.
    Passive,
    /// Prices at the midpoint, filling only against a willing counterparty.
    Midpoint,
}

/// Why a cost could not be quoted, with the spread that was offered.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum CostRefusal {
    /// The style pays no spread but turns on a fill rate the archive does not measure; zero would flatter it.
    #[error(
        "a {style} fill turns on a fill rate the archive does not measure; the book quoted {quoted_spread}"
    )]
    FillRateUnmeasured {
        style: FillStyle,
        quoted_spread: BasisPoints,
    },
    /// The charge overflowed what a basis-point reading holds, or a fill model's crossing would cost more than its
    /// notional.
    #[error("a quoted spread of {quoted_spread} over {names} name(s) is not representable")]
    Unrepresentable {
        quoted_spread: BasisPoints,
        names: NonZeroU32,
    },
}

/// A round trip's charge read off a quoted spread, the assumption a bar replay reports net of. It counts names, each
/// crossed in and out, so half a round trip cannot be expressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoundTrip {
    fill_style: FillStyle,
    names: NonZeroU32,
}

impl RoundTrip {
    pub const fn new(fill_style: FillStyle, names: NonZeroU32) -> Self {
        Self { fill_style, names }
    }

    pub fn fill_style(self) -> FillStyle {
        self.fill_style
    }

    pub fn names(self) -> NonZeroU32 {
        self.names
    }

    /// One round trip's charge at `quoted_spread`. The quoted width is conservative: price improvement only
    /// narrows the effective spread.
    pub fn cost(self, quoted_spread: BasisPoints) -> Result<BasisPoints, CostRefusal> {
        match self.fill_style {
            // Half a spread per crossing and two crossings per name cancel.
            FillStyle::Aggressive => BasisPoints::new(
                quoted_spread.value() * f64::from(self.names.get()),
            )
            .map_err(|_| CostRefusal::Unrepresentable {
                quoted_spread,
                names: self.names,
            }),
            FillStyle::Passive | FillStyle::Midpoint => Err(CostRefusal::FillRateUnmeasured {
                style: self.fill_style,
                quoted_spread,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    use strum::IntoEnumIterator;

    fn names(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).unwrap()
    }

    fn basis_points(value: f64) -> BasisPoints {
        BasisPoints::new(value).unwrap()
    }

    #[test]
    fn test_an_aggressive_round_trip_pays_the_spread_once_per_name() {
        let pair = RoundTrip::new(FillStyle::Aggressive, names(2));
        assert_eq!(pair.cost(basis_points(10.0)), Ok(basis_points(20.0)));
        assert_eq!(pair.cost(basis_points(18.04)), Ok(basis_points(36.08)));
        let single = RoundTrip::new(FillStyle::Aggressive, names(1));
        assert_eq!(single.cost(basis_points(10.0)), Ok(basis_points(10.0)));
        assert_eq!(single.cost(basis_points(0.0)), Ok(basis_points(0.0)));
    }

    #[test]
    fn test_the_styles_that_pay_no_spread_refuse_rather_than_return_zero() {
        for style in [FillStyle::Passive, FillStyle::Midpoint] {
            assert_eq!(
                RoundTrip::new(style, names(2)).cost(basis_points(7.5)),
                Err(CostRefusal::FillRateUnmeasured {
                    style,
                    quoted_spread: basis_points(7.5)
                })
            );
        }
        let rendered = RoundTrip::new(FillStyle::Passive, names(1))
            .cost(basis_points(3.25))
            .unwrap_err()
            .to_string();
        assert!(
            rendered.contains("passive") && rendered.contains("3.25bp"),
            "{rendered}"
        );
    }

    #[test]
    fn test_an_overflowing_charge_names_arithmetic_rather_than_a_fill_rate() {
        assert_eq!(
            RoundTrip::new(FillStyle::Aggressive, names(2)).cost(basis_points(f64::MAX)),
            Err(CostRefusal::Unrepresentable {
                quoted_spread: basis_points(f64::MAX),
                names: names(2)
            })
        );
    }

    #[test]
    fn test_basis_points_refuse_negative_or_non_finite_readings_even_when_stored() {
        for refused in [-0.01, f64::NAN, f64::INFINITY] {
            let value = BasisPoints::new(refused).unwrap_err().value;
            assert_eq!(value.to_bits(), refused.to_bits(), "{refused}");
        }
        assert_eq!(
            serde_json::from_str::<BasisPoints>("2.5").unwrap(),
            basis_points(2.5)
        );
        assert!(serde_json::from_str::<BasisPoints>("-1").is_err());
        assert!(
            serde_json::from_str::<RoundTrip>(r#"{"fill_style":"aggressive","names":0}"#).is_err()
        );
    }

    #[test]
    fn test_a_negative_zero_reading_is_stored_and_named_as_zero() {
        let negative = basis_points(-0.0);
        assert!(negative.value().is_sign_positive());
        assert_eq!(negative.to_string(), "0bp");
    }

    #[test]
    fn test_fill_style_names_agree_between_strum_and_serde() {
        for style in FillStyle::iter() {
            let serialized = serde_json::to_string(&style).unwrap();
            assert_eq!(serialized, format!("\"{style}\""));
            assert_eq!(FillStyle::from_str(&style.to_string()), Ok(style));
        }
        assert_eq!(FillStyle::Midpoint.to_string(), "midpoint");
    }

    fn price(ticks: i64) -> Price {
        Price::from_ticks(ticks).unwrap()
    }

    #[test]
    fn test_an_amount_per_share_is_bounded_and_presents_in_cents_and_basis_points() {
        let bound = 10_000_000_000_000;
        assert!(PerShare::from_millionths(bound).is_ok());
        assert!(PerShare::from_millionths(-bound).is_ok());
        assert_eq!(
            PerShare::from_millionths(bound + 1),
            Err(PerShareRefusal {
                millionths: bound + 1
            })
        );
        let quarter_cent = PerShare::from_millionths(2_500).unwrap();
        assert_eq!(quarter_cent.cents(), 0.25);
        assert_eq!(quarter_cent.to_string(), "0.25c/share");
        // A quarter cent is five times more basis points at $5 than at $25.
        assert_eq!(quarter_cent.basis_points(price(5_000_000)), 5.0);
        assert_eq!(quarter_cent.basis_points(price(25_000_000)), 1.0);
    }

    #[test]
    fn test_the_bracket_parts_only_on_a_print_at_the_limit() {
        let limit = price(10_000_000);
        let cases = [
            (Side::Buy, 9_999_999, true, true),
            (Side::Buy, 10_000_000, true, false),
            (Side::Buy, 10_000_001, false, false),
            (Side::Sell, 10_000_001, true, true),
            (Side::Sell, 10_000_000, true, false),
            (Side::Sell, 9_999_999, false, false),
        ];
        for (side, print, optimistic, conservative) in cases {
            let read = |bound: FillBound| bound.fills(side, limit, price(print));
            assert_eq!(
                (read(FillBound::Optimistic), read(FillBound::Conservative)),
                (optimistic, conservative),
                "{side} at {print}"
            );
        }
    }

    #[test]
    fn test_fill_bound_names_agree_between_strum_and_serde() {
        for bound in FillBound::iter() {
            let serialized = serde_json::to_string(&bound).unwrap();
            assert_eq!(serialized, format!("\"{bound}\""));
            assert_eq!(FillBound::from_str(&bound.to_string()), Ok(bound));
        }
        assert_eq!(FillBound::Conservative.to_string(), "conservative");
    }

    proptest::proptest! {
        /// Whatever fills the conservative bound fills the optimistic one.
        #[test]
        fn test_a_conservative_fill_is_an_optimistic_fill(
            limit in 1i64..1_000_000_000,
            print in 1i64..1_000_000_000,
            sell in proptest::prelude::any::<bool>(),
        ) {
            let side = match sell { true => Side::Sell, false => Side::Buy };
            let (limit, print) = (price(limit), price(print));
            proptest::prop_assert!(
                !FillBound::Conservative.fills(side, limit, print) || FillBound::Optimistic.fills(side, limit, print)
            );
        }
    }
}
