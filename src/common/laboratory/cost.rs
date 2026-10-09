//! What a round trip costs, as a value, so changing the fill assumption re-scores every study that used it.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// A non-negative, finite width or charge in basis points.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64")]
pub struct BasisPoints(f64);

/// A basis-point reading refused for being negative or not finite, with the value read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BasisPointsRefusal {
    pub value: f64,
}

impl std::fmt::Display for BasisPointsRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} is not a finite, non-negative basis-point reading",
            self.value
        )
    }
}

impl std::error::Error for BasisPointsRefusal {}

impl BasisPoints {
    pub fn new(value: f64) -> Result<Self, BasisPointsRefusal> {
        match value.is_finite() && value >= 0.0 {
            true => Ok(Self(value)),
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
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum CostRefusal {
    /// The style pays no spread but turns on a fill rate the archive does not measure; zero would flatter it.
    FillRateUnmeasured {
        style: FillStyle,
        quoted_spread: BasisPoints,
    },
    /// The charge overflowed what a basis-point reading holds, or a fill model's crossing would cost more than its
    /// notional.
    Unrepresentable {
        quoted_spread: BasisPoints,
        names: NonZeroU32,
    },
}

impl std::fmt::Display for CostRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CostRefusal::FillRateUnmeasured {
                style,
                quoted_spread,
            } => write!(
                formatter,
                "a {style} fill turns on a fill rate the archive does not measure; the book quoted {quoted_spread}"
            ),
            CostRefusal::Unrepresentable {
                quoted_spread,
                names,
            } => write!(
                formatter,
                "a quoted spread of {quoted_spread} over {names} name(s) is not representable"
            ),
        }
    }
}

/// The cost assumption a study reports net of. A round trip counts names, each crossed in and out, so half a
/// round trip cannot be expressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostModel {
    fill_style: FillStyle,
    names: NonZeroU32,
}

impl CostModel {
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
        let pair = CostModel::new(FillStyle::Aggressive, names(2));
        assert_eq!(pair.cost(basis_points(10.0)), Ok(basis_points(20.0)));
        assert_eq!(pair.cost(basis_points(18.04)), Ok(basis_points(36.08)));
        let single = CostModel::new(FillStyle::Aggressive, names(1));
        assert_eq!(single.cost(basis_points(10.0)), Ok(basis_points(10.0)));
        assert_eq!(single.cost(basis_points(0.0)), Ok(basis_points(0.0)));
    }

    #[test]
    fn test_the_styles_that_pay_no_spread_refuse_rather_than_return_zero() {
        for style in [FillStyle::Passive, FillStyle::Midpoint] {
            assert_eq!(
                CostModel::new(style, names(2)).cost(basis_points(7.5)),
                Err(CostRefusal::FillRateUnmeasured {
                    style,
                    quoted_spread: basis_points(7.5)
                })
            );
        }
        let rendered = CostModel::new(FillStyle::Passive, names(1))
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
            CostModel::new(FillStyle::Aggressive, names(2)).cost(basis_points(f64::MAX)),
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
            serde_json::from_str::<CostModel>(r#"{"fill_style":"aggressive","names":0}"#).is_err()
        );
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
}
