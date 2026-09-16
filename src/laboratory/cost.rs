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

/// The names one round trip touches, each of them crossed twice — in and out.
///
/// Counted in names rather than crossings so that a half-finished round trip cannot be expressed:
/// a count of crossings admits odd numbers, and three crossings would price one and a half spreads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoundTrip(u32);

impl RoundTrip {
    /// One name, bought and sold.
    pub const SINGLE_NAME: Self = Self(1);

    /// Both legs of a pair.
    pub const PAIR: Self = Self(2);

    /// `None` on zero, which describes a position that is never opened rather than a free one.
    pub fn new(names: u32) -> Option<Self> {
        (names > 0).then_some(Self(names))
    }

    pub fn names(self) -> u32 {
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
    /// The product left the range a basis-point reading can hold.
    ///
    /// Distinct from the above because the cause is arithmetic rather than absent data: a spread
    /// near `f64::MAX` passes `BasisPoints::new` and still overflows here, and reporting that as an
    /// unmeasured fill rate would send a caller looking for data that was never the problem.
    Unrepresentable {
        quoted_spread: BasisPoints,
        names: u32,
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
            CostRefusal::Unrepresentable {
                quoted_spread,
                names,
            } => write!(
                formatter,
                "a quoted spread of {quoted_spread} over {names} name(s) is not representable as a \
                 basis-point cost"
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
    round_trip: RoundTrip,
}

impl CostModel {
    pub const fn new(fill_style: FillStyle, round_trip: RoundTrip) -> Self {
        Self {
            fill_style,
            round_trip,
        }
    }

    pub const fn fill_style(self) -> FillStyle {
        self.fill_style
    }

    pub const fn round_trip(self) -> RoundTrip {
        self.round_trip
    }

    /// What one round trip pays, given the name's quoted spread over the period being traded.
    ///
    /// The spread is the **quoted** width, which is what the archive folds. Substituting it for an
    /// effective spread is deliberately conservative: price improvement makes an effective spread no
    /// wider than the quoted one, so a strategy that survives this cost survives the real one.
    pub fn cost(self, quoted_spread: BasisPoints) -> Result<BasisPoints, CostRefusal> {
        match self.fill_style {
            // Spread times names, because a crossing pays half a spread and each name is crossed
            // twice; the two factors of two cancel and there is no halving left to get wrong.
            FillStyle::Aggressive => {
                let names = self.round_trip.names();
                BasisPoints::new(quoted_spread.value() * f64::from(names)).ok_or(
                    CostRefusal::Unrepresentable {
                        quoted_spread,
                        names,
                    },
                )
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
        let model = CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR);

        let paid = model
            .cost(basis_points(10.0))
            .expect("an aggressive fill is costable from a quoted spread");

        // Pinned to the literal the retired `EFFECTIVE_SPREAD_BASIS_POINTS = 10.0` produced through
        // `pair_round_trip_basis_points`, so this fails if the arithmetic moves rather than tracking it.
        assert!((paid.value() - 20.0).abs() < 1e-12, "got {paid}");
    }

    #[test]
    fn test_a_single_name_round_trip_pays_the_spread_once() {
        let model = CostModel::new(FillStyle::Aggressive, RoundTrip::SINGLE_NAME);

        let paid = model
            .cost(basis_points(10.0))
            .expect("an aggressive fill is costable from a quoted spread");

        assert!((paid.value() - 10.0).abs() < 1e-12, "got {paid}");
    }

    #[test]
    fn test_the_measured_spreads_price_differently_from_the_retired_literal() {
        let model = CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR);

        // SPY and CBOE as measured in the archive, against the 10.0 bp every prior study assumed.
        let tight = model.cost(basis_points(0.26)).expect("costable");
        let wide = model.cost(basis_points(18.04)).expect("costable");

        assert!((tight.value() - 0.52).abs() < 1e-12, "got {tight}");
        assert!((wide.value() - 36.08).abs() < 1e-12, "got {wide}");
        // The whole argument for the type: one literal cannot stand for both of these.
        assert!(wide.value() > 60.0 * tight.value());
    }

    #[test]
    fn test_the_styles_that_pay_no_spread_refuse_rather_than_return_zero() {
        for style in [FillStyle::Passive, FillStyle::Midpoint] {
            let model = CostModel::new(style, RoundTrip::PAIR);

            let refusal = model
                .cost(basis_points(7.5))
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
                CostRefusal::Unrepresentable { .. } => {
                    panic!("a 7.5bp spread is representable; the cause is the fill rate")
                }
            }
        }
    }

    /// An overflowing product must not borrow the fill-rate refusal.
    ///
    /// `BasisPoints::new` admits any finite non-negative reading, so `f64::MAX` reaches the
    /// multiplication and leaves the representable range. Reporting that as an unmeasured fill rate
    /// would send a caller looking for data that was never the problem.
    #[test]
    fn test_an_overflowing_product_names_arithmetic_rather_than_a_fill_rate() {
        let model = CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR);

        let refusal = model
            .cost(basis_points(f64::MAX))
            .expect_err("the product leaves the representable range");

        match refusal {
            CostRefusal::Unrepresentable {
                quoted_spread,
                names,
            } => {
                assert_eq!(quoted_spread.value(), f64::MAX);
                assert_eq!(names, 2);
            }
            CostRefusal::FillRateUnmeasured { .. } => {
                panic!("an aggressive fill's cost is measured; the cause is arithmetic")
            }
        }
    }

    #[test]
    fn test_a_refusal_says_which_style_it_refused_and_how_wide_the_book_was() {
        let model = CostModel::new(FillStyle::Passive, RoundTrip::SINGLE_NAME);

        let rendered = model
            .cost(basis_points(3.25))
            .expect_err("passive is not costable")
            .to_string();

        assert!(rendered.contains("Passive"), "got {rendered}");
        assert!(rendered.contains("3.25bp"), "got {rendered}");
    }

    #[test]
    fn test_a_round_trip_over_no_names_is_refused() {
        assert!(RoundTrip::new(0).is_none());
        assert_eq!(RoundTrip::new(1), Some(RoundTrip::SINGLE_NAME));
        assert_eq!(RoundTrip::new(2), Some(RoundTrip::PAIR));
    }

    /// Counting names rather than crossings is what makes a half round trip unrepresentable.
    ///
    /// The retired `Crossings` type admitted three, which priced one and a half spreads. Every
    /// count here is a whole number of names and therefore an even number of crossings.
    #[test]
    fn test_every_admissible_round_trip_costs_a_whole_number_of_spreads() {
        let spread = basis_points(1.0);

        for names in 1..=5u32 {
            let model = CostModel::new(
                FillStyle::Aggressive,
                RoundTrip::new(names).expect("a positive count of names"),
            );

            let paid = model.cost(spread).expect("costable");

            assert!(
                (paid.value() - paid.value().round()).abs() < 1e-12,
                "{names} names priced {paid}, which is not a whole number of spreads"
            );
        }
    }

    #[test]
    fn test_the_named_round_trips_are_one_and_two_names() {
        // Literals rather than the constants, so a change to either has to be made deliberately.
        assert_eq!(RoundTrip::SINGLE_NAME.names(), 1);
        assert_eq!(RoundTrip::PAIR.names(), 2);
    }

    #[test]
    fn test_a_zero_spread_costs_zero_rather_than_refusing() {
        let model = CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR);

        // Zero is a measurement here, not an absence: a locked book quotes no width.
        let paid = model.cost(basis_points(0.0)).expect("costable");

        assert!((paid.value() - 0.0).abs() < 1e-12, "got {paid}");
    }
}
