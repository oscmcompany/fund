//! What a fill is expected to cost per share and how well that is known, per name or per spread decile, under a fee
//! regime; the error is the floor a net edge must clear before a candidate is admitted.

use std::collections::BTreeMap;

use super::fees::{FeeRegime, Liquidity};
use super::{PerShare, PerShareRefusal};
use crate::common::book::Side;
use crate::common::market::quote_bars::Spread;
use crate::common::market::{Price, Symbol};

/// A measured execution cost per share and its error, both fitted outside the code and passed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostEstimate {
    point: PerShare,
    error: PerShare,
}

/// An estimate refused for a negative error, with the error offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("an error of {error} is negative")]
pub struct CostEstimateRefusal {
    pub error: PerShare,
}

impl CostEstimate {
    pub fn new(point: PerShare, error: PerShare) -> Result<Self, CostEstimateRefusal> {
        match error >= PerShare::ZERO {
            true => Ok(Self { point, error }),
            false => Err(CostEstimateRefusal { error }),
        }
    }

    pub fn point(self) -> PerShare {
        self.point
    }

    pub fn error(self) -> PerShare {
        self.error
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
                .point
                .plus(self.fees.per_share(liquidity, side, price)?)?,
            floor: estimate.error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    use crate::common::cost::fees::{PassThroughSchedule, RegulatoryFees};
    use crate::common::market::{Dollars, PRICE_SCALE};

    fn per_share(millionths: i64) -> PerShare {
        PerShare::from_millionths(millionths).unwrap()
    }

    fn estimate(point: i64, error: i64) -> CostEstimate {
        CostEstimate::new(per_share(point), per_share(error)).unwrap()
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
    fn test_a_negative_error_is_refused() {
        assert_eq!(
            CostEstimate::new(per_share(5), per_share(-1)),
            Err(CostEstimateRefusal {
                error: per_share(-1)
            })
        );
        assert!(CostEstimate::new(per_share(-5), per_share(0)).is_ok());
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

    proptest! {
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
