//! What a fill is expected to cost per share and how well that is known, per name or per spread decile, under a fee
//! regime; the error is the floor a net edge must clear before a candidate is admitted.

use std::collections::BTreeMap;

use super::fees::{ChargeRefusal, FeeRegime, Liquidity};
use super::{PerShare, PerShareRefusal};
use crate::common::book::Side;
use crate::common::market::quote_bars::Spread;
use crate::common::market::{Price, Symbol};
use crate::common::monoid::Monoid;

/// The fewest fills whose error is read as a floor, since fewer leave the error itself noisy.
pub const MINIMUM_FILLS: u64 = 20;

/// A measured execution cost per share folded from fills: their count and, in millionths, their sum and sum of
/// squares, so estimates from any grouping of fills combine exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostEstimate {
    /// Saturates rather than wraps, and a count that would wrap marks the sums overflowed.
    fills: u64,
    sums: Sums,
}

/// The sums a cost estimate holds, or their loss to overflow, which no further fill undoes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sums {
    Exact { sum: i128, sum_of_squares: u128 },
    Overflowed,
}

/// Why an estimate has no point or error yet, with the fills it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EstimateRefusal {
    #[error(
        "{fills} fill(s) cannot measure a cost: a point takes one and an error {MINIMUM_FILLS}"
    )]
    TooFewFills { fills: u64 },
    #[error("the cost of {fills} fills is not representable per share")]
    Unrepresentable { fills: u64 },
    #[error("the sums of {fills} fills overflowed")]
    Overflowed { fills: u64 },
}

impl CostEstimate {
    /// One fill's execution cost per share.
    pub fn of(cost: PerShare) -> Self {
        let millionths = cost.millionths();
        Self {
            fills: 1,
            sums: Sums::Exact {
                sum: i128::from(millionths),
                sum_of_squares: u128::from(millionths.unsigned_abs()).pow(2),
            },
        }
    }

    pub fn fills(self) -> u64 {
        self.fills
    }

    fn exact(self) -> Result<(i128, u128), EstimateRefusal> {
        match self.sums {
            Sums::Exact {
                sum,
                sum_of_squares,
            } => Ok((sum, sum_of_squares)),
            Sums::Overflowed => Err(EstimateRefusal::Overflowed { fills: self.fills }),
        }
    }

    /// The mean cost, rounded up so rounding never flatters.
    pub fn point(self) -> Result<PerShare, EstimateRefusal> {
        let (sum, _) = self.exact()?;
        let fills = i128::from(self.fills);
        match fills {
            0 => Err(EstimateRefusal::TooFewFills { fills: self.fills }),
            _ => i64::try_from(-(-sum).div_euclid(fills))
                .ok()
                .and_then(|mean| PerShare::from_millionths(mean).ok())
                .ok_or(EstimateRefusal::Unrepresentable { fills: self.fills }),
        }
    }

    /// The standard error of the mean, `sqrt((n * squares - sum^2) / (n^2 * (n - 1)))`, computed in integers and
    /// rounded up, refused below `MINIMUM_FILLS`.
    pub fn error(self) -> Result<PerShare, EstimateRefusal> {
        let (sum, sum_of_squares) = self.exact()?;
        if self.fills < MINIMUM_FILLS {
            return Err(EstimateRefusal::TooFewFills { fills: self.fills });
        }
        let fills = u128::from(self.fills);
        let unrepresentable = EstimateRefusal::Unrepresentable { fills: self.fills };
        let spread = fills
            .checked_mul(sum_of_squares)
            .and_then(|scaled| scaled.checked_sub(sum.unsigned_abs().checked_pow(2)?))
            .ok_or(unrepresentable)?;
        // Successive ceilings equal the ceiling of the whole quotient, and the root of a ceiling rounds up the same.
        let variance = spread.div_ceil(fills).div_ceil(fills).div_ceil(fills - 1);
        let root = variance.isqrt();
        let error = root + u128::from(root * root < variance);
        i64::try_from(error)
            .ok()
            .and_then(|error| PerShare::from_millionths(error).ok())
            .ok_or(unrepresentable)
    }
}

/// Overflow absorbs, so the laws hold through it: the count and squares only grow, and a sum of fills built by `of`
/// cannot overflow an `i128` before the count overflows a `u64`, so whether a combination overflows is independent
/// of its grouping.
impl Monoid for CostEstimate {
    fn empty() -> Self {
        Self {
            fills: 0,
            sums: Sums::Exact {
                sum: 0,
                sum_of_squares: 0,
            },
        }
    }

    fn combine(self, other: Self) -> Self {
        let sums = match (self.sums, other.sums) {
            (
                Sums::Exact {
                    sum,
                    sum_of_squares,
                },
                Sums::Exact {
                    sum: other_sum,
                    sum_of_squares: other_sum_of_squares,
                },
            ) => self
                .fills
                .checked_add(other.fills)
                .and(sum.checked_add(other_sum))
                .zip(sum_of_squares.checked_add(other_sum_of_squares))
                .map_or(Sums::Overflowed, |(sum, sum_of_squares)| Sums::Exact {
                    sum,
                    sum_of_squares,
                }),
            (Sums::Overflowed, Sums::Exact { .. } | Sums::Overflowed)
            | (Sums::Exact { .. }, Sums::Overflowed) => Sums::Overflowed,
        };
        Self {
            fills: self.fills.saturating_add(other.fills),
            sums,
        }
    }
}

/// One of ten spread deciles, numbered 1 (narrowest) to 10 (widest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decile(u8);

impl Decile {
    pub fn number(self) -> u8 {
        self.0
    }
}

/// Ten cost estimates keyed by the quoted spread in ticks, the unit a per-share cost is compared in, with the nine
/// spreads that open deciles 2 to 10; a spread at a bound falls in the wider decile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpreadDeciles {
    lower_bounds: [Spread; 9],
    estimates: [CostEstimate; 10],
}

/// Deciles refused because a bound sits below the one before it, with both bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("decile {} opens at {} ticks, below the {} ticks before it", .decile.number(), .lower.ticks(), .previous.ticks())]
pub struct SpreadDecilesRefusal {
    pub decile: Decile,
    pub previous: Spread,
    pub lower: Spread,
}

impl SpreadDeciles {
    pub fn new(
        lower_bounds: [Spread; 9],
        estimates: [CostEstimate; 10],
    ) -> Result<Self, SpreadDecilesRefusal> {
        for (index, pair) in lower_bounds.windows(2).enumerate() {
            if pair[1] < pair[0] {
                return Err(SpreadDecilesRefusal {
                    // The second bound of the pair at `index` opens decile `index + 3`.
                    decile: Decile(index as u8 + 3),
                    previous: pair[0],
                    lower: pair[1],
                });
            }
        }
        Ok(Self {
            lower_bounds,
            estimates,
        })
    }

    pub fn decile(&self, spread: Spread) -> Decile {
        let opened = self
            .lower_bounds
            .iter()
            .filter(|bound| **bound <= spread)
            .count();
        Decile(opened as u8 + 1)
    }

    pub fn estimate(&self, spread: Spread) -> CostEstimate {
        self.estimates[usize::from(self.decile(spread).number() - 1)]
    }
}

/// What a cost estimate is keyed by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CostBasis {
    /// Measured per name; a name outside the map is unpriced rather than given another name's cost.
    ByName(BTreeMap<Symbol, CostEstimate>),
    BySpreadDecile(Box<SpreadDeciles>),
}

/// Why a candidate could not be priced.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PricingRefusal {
    #[error("{symbol} has no measured cost")]
    Unpriced { symbol: Symbol },
    #[error("the cost is unmeasured: {0}")]
    Unmeasured(#[from] EstimateRefusal),
    #[error("the fees could not be charged: {0}")]
    Uncharged(#[from] ChargeRefusal),
    #[error("the cost with its fees is out of range: {0}")]
    OutOfRange(#[from] PerShareRefusal),
}

/// The expected cost of a fill per share, its execution estimate plus its fees, and the floor its error sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PricedCost {
    point: PerShare,
    floor: PerShare,
}

/// Why a candidate was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionRefusal {
    /// Its edge net of cost does not clear the cost's error, with both.
    #[error("a net edge of {net} is inside the floor of {floor}")]
    InsideFloor { net: PerShare, floor: PerShare },
    #[error("the net edge is out of range: {0}")]
    OutOfRange(#[from] PerShareRefusal),
}

impl PricedCost {
    pub fn point(self) -> PerShare {
        self.point
    }

    /// The cost's error: a net edge at or below it cannot be told from a mispriced cost. Fees add none, since their
    /// schedule is known.
    pub fn floor(self) -> PerShare {
        self.floor
    }

    /// The edge per share net of the cost, admitted only when it lies strictly above the floor.
    pub fn admit(self, gross_edge: PerShare) -> Result<PerShare, AdmissionRefusal> {
        let net = gross_edge.minus(self.point)?;
        match net > self.floor {
            true => Ok(net),
            false => Err(AdmissionRefusal::InsideFloor {
                net,
                floor: self.floor,
            }),
        }
    }
}

/// The cost of a fill under `fees`, its execution estimate read from `basis`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostModel {
    basis: CostBasis,
    fees: FeeRegime,
}

impl CostModel {
    pub fn new(basis: CostBasis, fees: FeeRegime) -> Self {
        Self { basis, fees }
    }

    pub fn fees(&self) -> FeeRegime {
        self.fees
    }

    /// A share of `symbol` filled on `side` at `price`, taking or resting per `liquidity`, while the book quotes
    /// `spread`.
    pub fn price(
        &self,
        symbol: &Symbol,
        spread: Spread,
        price: Price,
        side: Side,
        liquidity: Liquidity,
    ) -> Result<PricedCost, PricingRefusal> {
        let estimate = match &self.basis {
            CostBasis::ByName(estimates) => {
                estimates
                    .get(symbol)
                    .copied()
                    .ok_or_else(|| PricingRefusal::Unpriced {
                        symbol: symbol.clone(),
                    })?
            }
            CostBasis::BySpreadDecile(deciles) => deciles.estimate(spread),
        };
        Ok(PricedCost {
            point: estimate
                .point()?
                .plus(self.fees.per_share(liquidity, side, price)?)?,
            floor: estimate.error()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    use crate::common::cost::fees::{PassThroughSchedule, RegulatoryFees};
    use crate::common::market::{Dollars, PRICE_SCALE};
    use crate::common::monoid::{concatenate, laws};

    fn per_share(millionths: i64) -> PerShare {
        PerShare::from_millionths(millionths).unwrap()
    }

    /// Thirteen fills either side of `point` by five times `error`, whose mean is `point` and whose standard error
    /// is exactly `error`.
    fn estimate(point: i64, error: i64) -> CostEstimate {
        let pair = CostEstimate::of(per_share(point - 5 * error))
            .combine(CostEstimate::of(per_share(point + 5 * error)));
        concatenate(std::iter::repeat_n(pair, 13))
    }

    fn exact(fills: u64, sum: i128, sum_of_squares: u128) -> CostEstimate {
        CostEstimate {
            fills,
            sums: Sums::Exact {
                sum,
                sum_of_squares,
            },
        }
    }

    fn fills(costs: &[i64]) -> CostEstimate {
        concatenate(costs.iter().map(|cost| CostEstimate::of(per_share(*cost))))
    }

    fn spread(ticks: u64) -> Spread {
        Spread::from_ticks(ticks)
    }

    /// Bounds at 1 to 9 cents, each decile's point its number in cents and its error a tenth of that.
    fn deciles() -> SpreadDeciles {
        SpreadDeciles::new(
            std::array::from_fn(|index| spread(10_000 * (index as u64 + 1))),
            std::array::from_fn(|index| {
                estimate(10_000 * (index as i64 + 1), 1_000 * (index as i64 + 1))
            }),
        )
        .unwrap()
    }

    fn free() -> FeeRegime {
        FeeRegime::CommissionFree {
            regulatory: RegulatoryFees::new(Dollars::from_millionths(0), per_share(0)).unwrap(),
        }
    }

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn price() -> Price {
        Price::from_ticks(50 * PRICE_SCALE).unwrap()
    }

    #[test]
    fn test_a_spread_at_a_bound_falls_in_the_wider_decile() {
        let deciles = deciles();
        let numbers = [0, 9_999, 10_000, 10_001, 89_999, 90_000, u64::MAX]
            .map(|ticks| deciles.decile(spread(ticks)).number());
        assert_eq!(numbers, [1, 1, 2, 2, 9, 10, 10]);
        assert_eq!(deciles.estimate(spread(25_000)), estimate(30_000, 3_000));
    }

    #[test]
    fn test_descending_bounds_are_refused_naming_the_decile_they_open() {
        let mut bounds: [Spread; 9] = std::array::from_fn(|index| spread(index as u64 * 10));
        bounds[4] = spread(5);
        assert_eq!(
            SpreadDeciles::new(bounds, [estimate(0, 0); 10]),
            Err(SpreadDecilesRefusal {
                decile: Decile(6),
                previous: spread(30),
                lower: spread(5),
            })
        );
        assert!(SpreadDeciles::new([spread(7); 9], [estimate(0, 0); 10]).is_ok());
    }

    #[test]
    fn test_an_estimate_needs_a_fill_for_its_point_and_twenty_for_its_error() {
        let empty = CostEstimate::empty();
        assert_eq!(
            empty.point(),
            Err(EstimateRefusal::TooFewFills { fills: 0 })
        );
        assert_eq!(
            empty.error(),
            Err(EstimateRefusal::TooFewFills { fills: 0 })
        );
        let single = fills(&[4_000]);
        assert_eq!(single.point(), Ok(per_share(4_000)));
        assert_eq!(
            single.error(),
            Err(EstimateRefusal::TooFewFills { fills: 1 })
        );
        let nineteen = fills(&[4_000; 19]);
        assert_eq!(nineteen.point(), Ok(per_share(4_000)));
        assert_eq!(
            nineteen.error(),
            Err(EstimateRefusal::TooFewFills { fills: 19 })
        );
        assert_eq!(fills(&[4_000; 20]).error(), Ok(per_share(0)));
        assert_eq!(estimate(4_000, 500).point(), Ok(per_share(4_000)));
        assert_eq!(estimate(4_000, 500).error(), Ok(per_share(500)));
    }

    #[test]
    fn test_two_equal_fills_cannot_admit_a_positive_edge() {
        let model = CostModel::new(
            CostBasis::ByName(BTreeMap::from([(symbol("AAPL"), fills(&[3_000, 3_000]))])),
            free(),
        );
        assert_eq!(
            model.price(
                &symbol("AAPL"),
                spread(10_000),
                price(),
                Side::Buy,
                Liquidity::Removing
            ),
            Err(PricingRefusal::Unmeasured(EstimateRefusal::TooFewFills {
                fills: 2
            }))
        );
    }

    #[test]
    fn test_overflowing_squares_are_refused_rather_than_panicking() {
        let half = exact(10, 0, u128::MAX / 2 + 1);
        let overflowed = half.combine(half);
        assert_eq!(overflowed.fills(), 20);
        assert_eq!(
            overflowed.point(),
            Err(EstimateRefusal::Overflowed { fills: 20 })
        );
        assert_eq!(
            overflowed.error(),
            Err(EstimateRefusal::Overflowed { fills: 20 })
        );
        let absorbed = overflowed.combine(fills(&[1_000; 30]));
        assert_eq!(
            absorbed.error(),
            Err(EstimateRefusal::Overflowed { fills: 50 })
        );
        let counted = exact(u64::MAX, 0, 0).combine(fills(&[1_000]));
        assert_eq!(
            counted.point(),
            Err(EstimateRefusal::Overflowed { fills: u64::MAX })
        );
    }

    #[test]
    fn test_the_point_and_error_round_up() {
        // Variance of the mean 25,000,000 / 380 = 65,789.5, whose root 256.5 rounds up.
        let measured = fills(&[1_000, 2_000, 3_000, 4_000].repeat(5));
        assert_eq!(
            (measured.fills(), measured.point(), measured.error()),
            (20, Ok(per_share(2_500)), Ok(per_share(257)))
        );
        assert_eq!(fills(&[-1, -2]).point(), Ok(per_share(-1)));
        assert_eq!(fills(&[1, 2]).point(), Ok(per_share(2)));
        assert_eq!(fills(&[7; 20]).error(), Ok(per_share(0)));
    }

    #[test]
    fn test_a_spread_past_an_i128_is_refused_rather_than_wrapped() {
        // A million fills at each end of the bound.
        let wide = exact(2_000_000, 0, 2_000_000 * 10_000_000_000_000u128.pow(2));
        assert_eq!(wide.point(), Ok(PerShare::ZERO));
        assert_eq!(
            wide.error(),
            Err(EstimateRefusal::Unrepresentable { fills: 2_000_000 })
        );
    }

    #[test]
    fn test_repeating_a_batch_of_fills_lowers_the_floor() {
        let model = |estimate: CostEstimate| {
            CostModel::new(
                CostBasis::ByName(BTreeMap::from([(symbol("AAPL"), estimate)])),
                free(),
            )
            .price(
                &symbol("AAPL"),
                spread(10_000),
                price(),
                Side::Buy,
                Liquidity::Removing,
            )
        };
        let batch = fills(&[3_000, 5_000]);
        assert_eq!(
            model(CostEstimate::of(per_share(3_000))),
            Err(PricingRefusal::Unmeasured(EstimateRefusal::TooFewFills {
                fills: 1
            }))
        );
        let floors = [10, 20, 40, 160].map(|copies| {
            model(concatenate(std::iter::repeat_n(batch, copies)))
                .unwrap()
                .floor()
                .millionths()
        });
        // sqrt(1,000,000 / (2 * copies - 1)), rounded up.
        assert_eq!(floors, [230, 161, 113, 56]);
    }

    #[test]
    fn test_a_name_outside_the_map_is_unpriced_rather_than_free() {
        let model = CostModel::new(
            CostBasis::ByName(BTreeMap::from([(symbol("AAPL"), estimate(4_000, 500))])),
            free(),
        );
        let priced = model
            .price(
                &symbol("AAPL"),
                spread(10_000),
                price(),
                Side::Buy,
                Liquidity::Removing,
            )
            .unwrap();
        assert_eq!(
            (priced.point(), priced.floor()),
            (per_share(4_000), per_share(500))
        );
        assert_eq!(
            model.price(
                &symbol("MSFT"),
                spread(10_000),
                price(),
                Side::Buy,
                Liquidity::Removing
            ),
            Err(PricingRefusal::Unpriced {
                symbol: symbol("MSFT")
            })
        );
    }

    #[test]
    fn test_fees_add_to_the_point_and_never_to_the_floor() {
        let schedule =
            PassThroughSchedule::new(per_share(2_000), per_share(3_000), per_share(2_500)).unwrap();
        let regulatory =
            RegulatoryFees::new(Dollars::from_millionths(10_000_000), per_share(100)).unwrap();
        let fees = FeeRegime::PassThrough {
            regulatory,
            schedule,
        };
        let model = CostModel::new(CostBasis::BySpreadDecile(Box::new(deciles())), fees);
        // Decile 2's 2 cents, plus 5,000 removing or less 500 adding, plus a $50 sale's 600 levy.
        let sold = |liquidity: Liquidity| {
            model
                .price(
                    &symbol("AAPL"),
                    spread(15_000),
                    price(),
                    Side::Sell,
                    liquidity,
                )
                .unwrap()
        };
        assert_eq!(sold(Liquidity::Removing).point(), per_share(25_600));
        assert_eq!(sold(Liquidity::Adding).point(), per_share(20_100));
        assert_eq!(sold(Liquidity::Removing).floor(), per_share(2_000));
        assert_eq!(sold(Liquidity::Adding).floor(), per_share(2_000));
    }

    #[test]
    fn test_an_edge_at_the_floor_is_refused_and_one_above_admitted() {
        let priced = PricedCost {
            point: per_share(3_000),
            floor: per_share(500),
        };
        assert_eq!(
            priced.admit(per_share(3_500)),
            Err(AdmissionRefusal::InsideFloor {
                net: per_share(500),
                floor: per_share(500)
            })
        );
        assert_eq!(priced.admit(per_share(3_501)), Ok(per_share(501)));
        assert_eq!(
            priced.admit(per_share(-1_000)),
            Err(AdmissionRefusal::InsideFloor {
                net: per_share(-4_000),
                floor: per_share(500)
            })
        );
    }

    #[test]
    fn test_a_cost_or_net_edge_past_the_bound_is_refused() {
        let bound = 10_000_000_000_000;
        let model = CostModel::new(
            CostBasis::ByName(BTreeMap::from([(symbol("AAPL"), estimate(bound, 0))])),
            FeeRegime::CommissionFree {
                regulatory: RegulatoryFees::new(Dollars::from_millionths(0), per_share(1)).unwrap(),
            },
        );
        assert_eq!(
            model.price(
                &symbol("AAPL"),
                spread(10_000),
                price(),
                Side::Sell,
                Liquidity::Removing
            ),
            Err(PricingRefusal::OutOfRange(PerShareRefusal {
                millionths: bound + 1
            }))
        );
        let priced = PricedCost {
            point: per_share(1),
            floor: per_share(0),
        };
        assert_eq!(
            priced.admit(per_share(-bound)),
            Err(AdmissionRefusal::OutOfRange(PerShareRefusal {
                millionths: -bound - 1
            }))
        );
    }

    /// Built from sums directly rather than through `combine`, so the law test does not lean on what it checks;
    /// counts and squares near half their range overflow when combined, so the laws are read through overflow too.
    fn any_estimate() -> impl Strategy<Value = CostEstimate> {
        let fills = prop_oneof![0u64..1_000, u64::MAX / 3..=u64::MAX / 2 + 1];
        let sum_of_squares = prop_oneof![
            0u128..1_000_000_000_000_000_000,
            u128::MAX / 3..=u128::MAX / 2 + 1,
        ];
        prop_oneof![
            (
                fills.clone(),
                -1_000_000_000i128..1_000_000_000,
                sum_of_squares
            )
                .prop_map(|(fills, sum, sum_of_squares)| exact(
                    fills,
                    sum,
                    sum_of_squares
                )),
            fills.prop_map(|fills| CostEstimate {
                fills,
                sums: Sums::Overflowed,
            }),
        ]
    }

    proptest! {
        #[test]
        fn property_a_cost_estimate_is_a_commutative_monoid(
            first in any_estimate(),
            second in any_estimate(),
            third in any_estimate(),
        ) {
            laws::check(first, second, third)?;
        }

        /// The point lies within the fills, and the error is the floating-point standard error rounded up.
        #[test]
        fn test_the_estimate_agrees_with_the_textbook_formulas(
            costs in prop::collection::vec(-10_000_000i64..10_000_000, 20..60),
        ) {
            let estimate = fills(&costs);
            let point = estimate.point().unwrap().millionths();
            prop_assert!(*costs.iter().min().unwrap() <= point && point <= *costs.iter().max().unwrap());
            let count = costs.len() as f64;
            let mean = costs.iter().sum::<i64>() as f64 / count;
            let variance = costs.iter().map(|cost| (*cost as f64 - mean).powi(2)).sum::<f64>() / (count - 1.0);
            let expected = (variance / count).sqrt();
            let error = estimate.error().unwrap().millionths() as f64;
            prop_assert!(error >= expected - 1e-6 && error < expected + 1.0 + 1e-6, "{error} against {expected}");
        }

        /// Repeating a batch of fills never raises the error.
        #[test]
        fn test_more_fills_of_the_same_costs_never_raise_the_error(
            costs in prop::collection::vec(-10_000_000i64..10_000_000, 20..40),
            copies in 1usize..20,
        ) {
            let batch = fills(&costs);
            let fewer = concatenate(std::iter::repeat_n(batch, copies)).error().unwrap();
            let more = concatenate(std::iter::repeat_n(batch, copies + 1)).error().unwrap();
            prop_assert!(more <= fewer);
        }

        /// A wider spread never lands in a narrower decile, and every spread lands in one of the ten.
        #[test]
        fn test_deciles_are_monotone_in_the_spread(
            mut bounds in prop::array::uniform9(0u64..1_000_000),
            narrower in 0u64..2_000_000,
            widening in 0u64..2_000_000,
        ) {
            bounds.sort_unstable();
            let deciles = SpreadDeciles::new(bounds.map(Spread::from_ticks), [estimate(0, 0); 10]).unwrap();
            let low = deciles.decile(spread(narrower));
            let high = deciles.decile(spread(narrower + widening));
            prop_assert!(low <= high);
            prop_assert!((1..=10).contains(&low.number()) && (1..=10).contains(&high.number()));
        }

        /// Raising the floor by any amount never admits a candidate it refused.
        #[test]
        fn test_a_higher_floor_admits_no_more(
            point in -100_000i64..100_000,
            floor in 0i64..100_000,
            raise in 0i64..100_000,
            edge in -1_000_000i64..1_000_000,
        ) {
            let lower = PricedCost { point: per_share(point), floor: per_share(floor) };
            let higher = PricedCost { point: per_share(point), floor: per_share(floor + raise) };
            prop_assert!(higher.admit(per_share(edge)).is_err() || lower.admit(per_share(edge)).is_ok());
        }
    }
}
