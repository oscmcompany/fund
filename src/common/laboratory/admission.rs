//! What a candidate is expected to earn net of its priced cost, and which candidates each of a descending set of
//! thresholds admits, swept from tight to loose.

use std::num::NonZeroU32;

use crate::common::book::Side;
use crate::common::cost::fees::Liquidity;
use crate::common::cost::model::{AdmissionRefusal, CostModel, PricedCost, PricingRefusal};
use crate::common::cost::{PerShare, PerShareRefusal};
use crate::common::market::quote_bars::{Spread, StandingQuote};
use crate::common::market::{Price, Symbol};

/// A bar on the expected net per share, in signed hundredths of a basis point of the mid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Threshold(i32);

impl Threshold {
    pub fn from_hundredths(hundredths: i32) -> Self {
        Self(hundredths)
    }

    pub fn hundredths(self) -> i32 {
        self.0
    }

    /// Whether `net` lies strictly above the threshold's share of `mid`, compared exactly in integers.
    pub fn clears(self, net: PerShare, mid: Price) -> bool {
        // A hundredth of a basis point is a millionth of the mid, and both amounts are in millionths of a dollar.
        i128::from(net.millionths()) * 1_000_000 > i128::from(self.0) * i128::from(mid.ticks())
    }
}

/// The thresholds a sweep reads, strictly descending from tight to loose and never empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Thresholds(Vec<Threshold>);

/// Why a set of thresholds was refused, with the pair out of order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ThresholdsRefusal {
    #[error("a sweep needs at least one threshold")]
    Empty,
    #[error("{} hundredths of a basis point is not above the {} after it", .earlier.hundredths(), .later.hundredths())]
    NotDescending {
        earlier: Threshold,
        later: Threshold,
    },
}

impl Thresholds {
    pub fn new(thresholds: Vec<Threshold>) -> Result<Self, ThresholdsRefusal> {
        if thresholds.is_empty() {
            return Err(ThresholdsRefusal::Empty);
        }
        match thresholds.windows(2).find(|pair| pair[0] <= pair[1]) {
            Some(pair) => Err(ThresholdsRefusal::NotDescending {
                earlier: pair[0],
                later: pair[1],
            }),
            None => Ok(Self(thresholds)),
        }
    }

    pub fn thresholds(&self) -> &[Threshold] {
        &self.0
    }

    /// The haircut family size: one test per threshold, saturating at `u32::MAX`.
    pub fn tests(&self) -> NonZeroU32 {
        let count = u32::try_from(self.0.len()).unwrap_or(u32::MAX);
        NonZeroU32::new(count).expect("thresholds are never empty")
    }
}

/// A candidate's trade: its entry side, the mid of the touch it was read at, its edge per share net of the entry,
/// and the priced cost of its exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expected {
    side: Side,
    mid: Price,
    edge: PerShare,
    priced: PricedCost,
}

/// Why a candidate has no expectation, with the forecast or the refusal beneath.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExpectRefusal {
    #[error("a forecast of {forecast} names no side")]
    Flat { forecast: PerShare },
    #[error("the candidate could not be priced: {0}")]
    Pricing(#[from] PricingRefusal),
    #[error("the expectation is out of range: {0}")]
    OutOfRange(#[from] PerShareRefusal),
}

impl Expected {
    pub fn side(self) -> Side {
        self.side
    }

    /// The touch's midpoint, rounded down to the millionth; a threshold is read against it.
    pub fn mid(self) -> Price {
        self.mid
    }

    /// The forecast's magnitude less the entry's half spread, rounded up, and its fees.
    pub fn edge(self) -> PerShare {
        self.edge
    }

    pub fn priced(self) -> PricedCost {
        self.priced
    }

    /// The edge net of the exit's priced cost.
    pub fn net(self) -> PerShare {
        self.edge
            .minus(self.priced.point())
            .expect("the net is checked in range when expected")
    }

    /// The net, admitted only when it clears the exit cost's floor.
    pub fn admit(self) -> Result<PerShare, AdmissionRefusal> {
        self.priced.admit(self.edge)
    }

    /// Whether the net is admitted and lies strictly above `threshold`'s share of the mid.
    pub fn clears(self, threshold: Threshold) -> bool {
        self.admit()
            .is_ok_and(|net| threshold.clears(net, self.mid))
    }
}

/// The trade `forecast` per share implies for `symbol`: entered by taking `touch`, exited by taking its other side,
/// the exit priced by `model` at the `trailing` spread.
pub fn expect(
    model: &CostModel,
    symbol: &Symbol,
    touch: &StandingQuote,
    trailing: Spread,
    forecast: PerShare,
) -> Result<Expected, ExpectRefusal> {
    let (side, entry, exit_side, exit) = match forecast.cmp(&PerShare::ZERO) {
        std::cmp::Ordering::Greater => (Side::Buy, touch.ask(), Side::Sell, touch.bid()),
        std::cmp::Ordering::Less => (Side::Sell, touch.bid(), Side::Buy, touch.ask()),
        std::cmp::Ordering::Equal => return Err(ExpectRefusal::Flat { forecast }),
    };
    // An uncrossed quote's difference is nonnegative, so adding one before halving rounds up.
    let half_spread =
        PerShare::from_millionths((touch.ask().ticks() - touch.bid().ticks() + 1) / 2)?;
    let entry_fees = model
        .fees()
        .per_share(Liquidity::Removing, side, entry)
        .map_err(PricingRefusal::from)?;
    let edge = PerShare::from_millionths(forecast.millionths().abs())?
        .minus(half_spread)?
        .minus(entry_fees)?;
    let priced = model.price(symbol, trailing, exit, exit_side, Liquidity::Removing)?;
    edge.minus(priced.point())?;
    let mid = Price::from_ticks((touch.bid().ticks() + touch.ask().ticks()) / 2)
        .expect("a midpoint lies between two prices");
    Ok(Expected {
        side,
        mid,
        edge,
        priced,
    })
}

/// What one threshold admits: every admitted row whose net clears it, and the band of those no tighter threshold admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted<'a, T> {
    threshold: Threshold,
    cumulative: Vec<&'a (Expected, T)>,
    band: Vec<&'a (Expected, T)>,
}

impl<'a, T> Admitted<'a, T> {
    pub fn threshold(&self) -> Threshold {
        self.threshold
    }

    pub fn cumulative(&self) -> &[&'a (Expected, T)] {
        &self.cumulative
    }

    pub fn band(&self) -> &[&'a (Expected, T)] {
        &self.band
    }
}

/// Each threshold's admissions from tight to loose, each row admitted past its cost floor and its net read against
/// the bar at its own mid.
pub fn sweep<'a, T>(thresholds: &Thresholds, rows: &'a [(Expected, T)]) -> Vec<Admitted<'a, T>> {
    let mut tighter: Option<Threshold> = None;
    thresholds
        .thresholds()
        .iter()
        .map(|threshold| {
            let cumulative: Vec<&'a (Expected, T)> = rows
                .iter()
                .filter(|(expected, _)| expected.clears(*threshold))
                .collect();
            let band = cumulative
                .iter()
                .copied()
                .filter(|(expected, _)| tighter.is_none_or(|bar| !expected.clears(bar)))
                .collect();
            tighter = Some(*threshold);
            Admitted {
                threshold: *threshold,
                cumulative,
                band,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, BTreeSet};

    use chrono::DateTime;
    use proptest::prelude::*;

    use crate::common::cost::fees::{FeeRegime, RegulatoryFees};
    use crate::common::cost::model::{CostBasis, CostEstimate, EstimateRefusal};
    use crate::common::market::{Dollars, Shares};
    use crate::common::monoid::{Monoid, concatenate};

    fn per_share(millionths: i64) -> PerShare {
        PerShare::from_millionths(millionths).unwrap()
    }

    fn price(millionths: i64) -> Price {
        Price::from_ticks(millionths).unwrap()
    }

    fn threshold(hundredths: i32) -> Threshold {
        Threshold::from_hundredths(hundredths)
    }

    fn symbol() -> Symbol {
        Symbol::new("AAA").unwrap()
    }

    fn quote(bid: i64, ask: i64) -> StandingQuote {
        let size = Shares::from_units(100);
        StandingQuote::new(DateTime::UNIX_EPOCH, price(bid), price(ask), size, size).unwrap()
    }

    /// Twenty-six fills whose mean is `point` and whose standard error is exactly `error`.
    fn estimate(point: i64, error: i64) -> CostEstimate {
        let pair = CostEstimate::of(per_share(point - 5 * error))
            .combine(CostEstimate::of(per_share(point + 5 * error)));
        concatenate(std::iter::repeat_n(pair, 13))
    }

    /// `AAA` at a point of `point` and an error of 300, under synthetic sale levies of ten dollars a million and a
    /// hundred millionths a share.
    fn model(point: i64) -> CostModel {
        let regulatory =
            RegulatoryFees::new(Dollars::from_millionths(10_000_000), per_share(100)).unwrap();
        CostModel::new(
            CostBasis::ByName(BTreeMap::from([(symbol(), estimate(point, 300))])),
            FeeRegime::CommissionFree { regulatory },
        )
    }

    fn expected(forecast: i64, touch: &StandingQuote) -> Result<Expected, ExpectRefusal> {
        expect(
            &model(3_000),
            &symbol(),
            touch,
            Spread::from_ticks(1),
            per_share(forecast),
        )
    }

    #[test]
    fn test_a_threshold_is_cleared_strictly_above_its_share_of_the_mid() {
        let mid = price(50_000_000);
        let cleared = [
            (100, 5_000),
            (100, 5_001),
            (-200, -10_000),
            (-200, -9_999),
            (0, 0),
            (0, 1),
        ]
        .map(|(hundredths, net)| threshold(hundredths).clears(per_share(net), mid));
        assert_eq!(cleared, [false, true, false, true, false, true]);
    }

    #[test]
    fn test_thresholds_must_be_nonempty_and_strictly_descending() {
        let thresholds =
            |values: &[i32]| Thresholds::new(values.iter().copied().map(threshold).collect());
        assert_eq!(thresholds(&[]), Err(ThresholdsRefusal::Empty));
        let refused = |earlier, later| {
            Err(ThresholdsRefusal::NotDescending {
                earlier: threshold(earlier),
                later: threshold(later),
            })
        };
        assert_eq!(thresholds(&[100, 100]), refused(100, 100));
        assert_eq!(thresholds(&[300, 100, 200]), refused(100, 200));
        assert_eq!(thresholds(&[300, 200, -100]).unwrap().tests().get(), 3);
    }

    #[test]
    fn test_a_long_and_a_short_net_the_entry_half_spread_its_fees_and_the_exit_cost() {
        let touch = quote(49_990_000, 50_010_000);
        let long = expected(50_000, &touch).unwrap();
        // A buy at the ask is levied nothing; the exit sells at the bid, levied 500 on the notional and 100 a share.
        assert_eq!(
            (long.side(), long.edge(), long.priced().point()),
            (Side::Buy, per_share(40_000), per_share(3_600))
        );
        let short = expected(-50_000, &touch).unwrap();
        assert_eq!(
            (short.side(), short.edge(), short.priced().point()),
            (Side::Sell, per_share(39_400), per_share(3_000))
        );
        assert_eq!(
            (long.net(), short.net()),
            (per_share(36_400), per_share(36_400))
        );
        assert_eq!(
            (long.admit(), long.priced().floor()),
            (Ok(per_share(36_400)), per_share(300))
        );
        // An odd spread's half rounds up.
        let odd = expected(50_000, &quote(49_990_000, 50_000_001)).unwrap();
        assert_eq!(
            (odd.edge(), odd.mid()),
            (per_share(44_999), price(49_995_000))
        );
        assert_eq!(long.mid(), price(50_000_000));
    }

    #[test]
    fn test_a_candidate_is_refused_with_its_cause() {
        let touch = quote(49_990_000, 50_010_000);
        let spread = Spread::from_ticks(1);
        assert_eq!(
            expected(0, &touch),
            Err(ExpectRefusal::Flat {
                forecast: PerShare::ZERO
            })
        );
        let unpriced = Symbol::new("BBB").unwrap();
        assert_eq!(
            expect(&model(3_000), &unpriced, &touch, spread, per_share(50_000)),
            Err(ExpectRefusal::Pricing(PricingRefusal::Unpriced {
                symbol: unpriced
            }))
        );
        let unmeasured = CostModel::new(
            CostBasis::ByName(BTreeMap::from([(symbol(), CostEstimate::empty())])),
            model(0).fees(),
        );
        assert_eq!(
            expect(&unmeasured, &symbol(), &touch, spread, per_share(50_000)),
            Err(ExpectRefusal::Pricing(PricingRefusal::Unmeasured(
                EstimateRefusal::TooFewFills { fills: 0 }
            )))
        );
        // A credit of ten million dollars a share pushes the net past the bound.
        let bound = 10_000_000_000_000;
        let credited = CostModel::new(
            CostBasis::ByName(BTreeMap::from([(symbol(), estimate(-bound, 0))])),
            model(0).fees(),
        );
        assert!(matches!(
            expect(&credited, &symbol(), &touch, spread, per_share(bound)),
            Err(ExpectRefusal::OutOfRange(_))
        ));
    }

    fn positions(rows: &[(Expected, Price)], admissions: &[&(Expected, Price)]) -> Vec<usize> {
        admissions
            .iter()
            .map(|admission| {
                rows.iter()
                    .position(|row| std::ptr::eq(row, *admission))
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn test_a_sweep_admits_cumulatively_and_bands_the_margin() {
        let touch = quote(49_990_000, 50_010_000);
        let rows: Vec<(Expected, Price)> = [50_000, 20_000, 14_000, -60_000]
            .map(|forecast| (expected(forecast, &touch).unwrap(), price(50_000_000)))
            .into();
        // Nets of 36,400, 6,400, 400 and 46,400 millionths against bars of 10,000, 500 and zero.
        let thresholds =
            Thresholds::new(vec![threshold(200), threshold(10), threshold(0)]).unwrap();
        let read: Vec<(i32, Vec<usize>, Vec<usize>)> = sweep(&thresholds, &rows)
            .iter()
            .map(|admission| {
                (
                    admission.threshold().hundredths(),
                    positions(&rows, admission.cumulative()),
                    positions(&rows, admission.band()),
                )
            })
            .collect();
        assert_eq!(
            read,
            [
                (200, vec![0, 3], vec![0, 3]),
                (10, vec![0, 1, 3], vec![1]),
                (0, vec![0, 1, 2, 3], vec![2]),
            ]
        );
    }

    #[test]
    fn test_a_sweep_admits_nothing_inside_the_cost_floor() {
        let touch = quote(49_990_000, 50_010_000);
        let rows: Vec<(Expected, Price)> = [13_601, 13_900, 13_901]
            .map(|forecast| (expected(forecast, &touch).unwrap(), price(50_000_000)))
            .into();
        // Nets of 1, 300 and 301 millionths against the model's floor of 300, all above a bar of zero.
        let thresholds = Thresholds::new(vec![threshold(0), threshold(-100)]).unwrap();
        let read: Vec<Vec<usize>> = sweep(&thresholds, &rows)
            .iter()
            .map(|admission| positions(&rows, admission.cumulative()))
            .collect();
        assert_eq!(read, [vec![2], vec![2]]);
    }

    fn rows() -> impl Strategy<Value = Vec<(Expected, Price)>> {
        let row = (
            (-200_000i64..200_000)
                .prop_filter("a forecast names a side", |forecast| *forecast != 0),
            1i64..=20_000,
            1_000_000i64..=500_000_000,
        );
        prop::collection::vec(row, 0..40).prop_map(|rows| {
            rows.into_iter()
                .map(|(forecast, half_spread, mid)| {
                    let touch = quote(mid - half_spread, mid + half_spread);
                    (expected(forecast, &touch).unwrap(), price(mid))
                })
                .collect()
        })
    }

    fn thresholds() -> impl Strategy<Value = Thresholds> {
        prop::collection::btree_set(-5_000i32..5_000, 1..10).prop_map(|values| {
            Thresholds::new(values.into_iter().rev().map(threshold).collect()).unwrap()
        })
    }

    proptest! {
        #[test]
        fn property_a_sweep_grows_by_its_bands_and_admits_exactly_what_clears_past_the_floor(
            rows in rows(),
            thresholds in thresholds(),
        ) {
            // Each row carries the mid of the symmetric touch it was expected at, read independently of `expect`.
            for (expected, mid) in &rows {
                prop_assert_eq!(expected.mid(), *mid);
            }
            let admitted = sweep(&thresholds, &rows);
            prop_assert_eq!(
                admitted.iter().map(Admitted::threshold).collect::<Vec<_>>(),
                thresholds.thresholds().to_vec()
            );
            let mut union = BTreeSet::new();
            for admission in &admitted {
                let cumulative: BTreeSet<usize> = positions(&rows, admission.cumulative()).into_iter().collect();
                let band: BTreeSet<usize> = positions(&rows, admission.band()).into_iter().collect();
                prop_assert!(union.is_subset(&cumulative));
                prop_assert!(union.is_disjoint(&band));
                union.extend(band);
                prop_assert_eq!(&union, &cumulative);
                let clearing: BTreeSet<usize> = rows
                    .iter()
                    .enumerate()
                    .filter(|(_, (expected, mid))| {
                        let net = expected.net().millionths();
                        net > expected.priced().floor().millionths()
                            && i128::from(net) * 1_000_000
                                > i128::from(admission.threshold().hundredths()) * i128::from(mid.ticks())
                    })
                    .map(|(index, _)| index)
                    .collect();
                prop_assert_eq!(cumulative, clearing);
            }
        }
    }
}
