//! What a round trip costs, as a value rather than a literal.
//!
//! Every study prices its turnover through this type, so changing the fill assumption re-scores the
//! studies that used it instead of requiring each to be re-derived by hand.

use crate::common::types::BasisPoints;

/// How an order is assumed to reach the book.
///
/// The variants differ in what they can be costed from and not only in what they pay: a crossing
/// order's cost is readable off a quoted spread, and the other two turn on a fill rate the archive
/// does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillStyle {
    /// Crosses the book, paying half the quoted spread on each crossing.
    Aggressive,
    /// Rests at the touch, paying no spread and filling only when the market comes to it.
    Passive,
    /// Prices at the midpoint, paying no spread and filling only against a willing counterparty.
    Midpoint,
}

/// Touches a strategy pays on one round trip: two for each name it holds, in and out.
///
/// A count rather than a bare number because the halving in [`CostModel::round_trip`] is only
/// correct against a count of crossings, and a stray factor of two is invisible in the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Crossings(u32);

impl Crossings {
    /// One name, in and out.
    pub const SINGLE_NAME_ROUND_TRIP: Self = Self(2);

    /// Both legs of a pair, in and out.
    pub const PAIR_ROUND_TRIP: Self = Self(4);

    /// `None` on zero, which describes a position that is never opened rather than a free one.
    pub fn new(count: u32) -> Option<Self> {
        (count > 0).then_some(Self(count))
    }

    pub fn count(self) -> u32 {
        self.0
    }
}

/// Why a cost could not be quoted, carrying the reading that produced the refusal.
///
/// A refusal rather than a zero, because the styles that pay no spread are not free: they pay in
/// unfilled orders and in adverse selection, and both are measured against data the archive does
/// not yet hold. Returning zero would make the cheapest assumption look like the best one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CostRefusal {
    /// The style's cost turns on a fill rate, which no stored field measures.
    FillRateUnmeasured {
        style: FillStyle,
        /// What the name did quote, so the refusal still says how wide the book was.
        quoted_spread: BasisPoints,
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
                "a {style:?} fill costs no spread but turns on a fill rate the archive does not \
                 measure; the book quoted {quoted_spread}"
            ),
        }
    }
}

/// The cost assumption a study is reporting net of.
///
/// Constructed rather than assembled from literals at each call site, so two studies quoting a net
/// figure are quoting it on the same terms or visibly not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostModel {
    fill_style: FillStyle,
    crossings: Crossings,
}

impl CostModel {
    pub const fn new(fill_style: FillStyle, crossings: Crossings) -> Self {
        Self {
            fill_style,
            crossings,
        }
    }

    pub const fn fill_style(self) -> FillStyle {
        self.fill_style
    }

    pub const fn crossings(self) -> Crossings {
        self.crossings
    }

    /// What one round trip pays, given the name's quoted spread over the period being traded.
    ///
    /// The spread is the **quoted** width, which is what the archive folds. Substituting it for an
    /// effective spread is deliberately conservative: price improvement makes an effective spread no
    /// wider than the quoted one, so a strategy that survives this cost survives the real one.
    pub fn round_trip(self, quoted_spread: BasisPoints) -> Result<BasisPoints, CostRefusal> {
        match self.fill_style {
            // Half per crossing, because a spread is the full width and a crossing pays one side.
            FillStyle::Aggressive => {
                BasisPoints::new(quoted_spread.value() * f64::from(self.crossings.count()) / 2.0)
                    .ok_or(CostRefusal::FillRateUnmeasured {
                        style: FillStyle::Aggressive,
                        quoted_spread,
                    })
            }
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

    fn basis_points(value: f64) -> BasisPoints {
        BasisPoints::new(value).expect("the fixture must be a usable reading")
    }

    #[test]
    fn test_a_pair_round_trip_pays_twice_the_single_name_spread() {
        let model = CostModel::new(FillStyle::Aggressive, Crossings::PAIR_ROUND_TRIP);

        let paid = model
            .round_trip(basis_points(10.0))
            .expect("an aggressive fill is costable from a quoted spread");

        // Pinned to the literal the old `EFFECTIVE_SPREAD_BASIS_POINTS = 10.0` produced through
        // `pair_round_trip_basis_points`, so this fails if the arithmetic moves rather than tracking it.
        assert!((paid.value() - 20.0).abs() < 1e-12, "got {paid}");
    }

    #[test]
    fn test_a_single_name_round_trip_pays_the_spread_once() {
        let model = CostModel::new(FillStyle::Aggressive, Crossings::SINGLE_NAME_ROUND_TRIP);

        let paid = model
            .round_trip(basis_points(10.0))
            .expect("an aggressive fill is costable from a quoted spread");

        assert!((paid.value() - 10.0).abs() < 1e-12, "got {paid}");
    }

    #[test]
    fn test_the_measured_spreads_price_differently_from_the_retired_literal() {
        let model = CostModel::new(FillStyle::Aggressive, Crossings::PAIR_ROUND_TRIP);

        // SPY and CBOE as measured in the archive, against the 10.0 bp every prior study assumed.
        let tight = model.round_trip(basis_points(0.26)).expect("costable");
        let wide = model.round_trip(basis_points(18.04)).expect("costable");

        assert!((tight.value() - 0.52).abs() < 1e-12, "got {tight}");
        assert!((wide.value() - 36.08).abs() < 1e-12, "got {wide}");
        // The whole argument for the type: one literal cannot stand for both of these.
        assert!(wide.value() > 60.0 * tight.value());
    }

    #[test]
    fn test_the_styles_that_pay_no_spread_refuse_rather_than_return_zero() {
        for style in [FillStyle::Passive, FillStyle::Midpoint] {
            let model = CostModel::new(style, Crossings::PAIR_ROUND_TRIP);

            let refusal = model
                .round_trip(basis_points(7.5))
                .expect_err("a fill rate the archive does not hold cannot be costed");

            match refusal {
                CostRefusal::FillRateUnmeasured {
                    style: refused,
                    quoted_spread,
                } => {
                    assert_eq!(refused, style);
                    // The refusal carries the number that produced it.
                    assert!((quoted_spread.value() - 7.5).abs() < 1e-12);
                }
            }
        }
    }

    #[test]
    fn test_a_refusal_says_which_style_it_refused_and_how_wide_the_book_was() {
        let model = CostModel::new(FillStyle::Passive, Crossings::SINGLE_NAME_ROUND_TRIP);

        let rendered = model
            .round_trip(basis_points(3.25))
            .expect_err("passive is not costable")
            .to_string();

        assert!(rendered.contains("Passive"), "got {rendered}");
        assert!(rendered.contains("3.25bp"), "got {rendered}");
    }

    #[test]
    fn test_a_crossing_count_of_zero_is_refused() {
        assert!(Crossings::new(0).is_none());
        assert_eq!(Crossings::new(2), Some(Crossings::SINGLE_NAME_ROUND_TRIP));
        assert_eq!(Crossings::new(4), Some(Crossings::PAIR_ROUND_TRIP));
    }

    #[test]
    fn test_the_named_counts_are_two_and_four() {
        // Literals rather than the constants, so a change to either has to be made deliberately.
        assert_eq!(Crossings::SINGLE_NAME_ROUND_TRIP.count(), 2);
        assert_eq!(Crossings::PAIR_ROUND_TRIP.count(), 4);
    }

    #[test]
    fn test_a_zero_spread_costs_zero_rather_than_refusing() {
        let model = CostModel::new(FillStyle::Aggressive, Crossings::PAIR_ROUND_TRIP);

        // Zero is a measurement here, not an absence: a locked book quotes no width.
        let paid = model.round_trip(basis_points(0.0)).expect("costable");

        assert!((paid.value() - 0.0).abs() < 1e-12, "got {paid}");
    }
}
