//! Whether a forecast means what it says: a quantile band's coverage overall and by decile of width, the mean outcome
//! by decile of the forecast, and the slope that rescales a forecast to the outcomes it was fit against.

use std::num::NonZeroU64;

use crate::common::laboratory::score::{Forecast, ForecastRefusal, Outcome, Pair};
use crate::common::laboratory::share::Share;
use crate::common::learning::component::{Component, Diagnostics};
use crate::common::learning::loss::Level;
use crate::common::monoid::{Monoid, concatenate};

/// Deciles a calibration is read in.
pub const DECILES: usize = 10;

/// The two quantile levels a band forecasts, the lower strictly below the upper.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Levels {
    lower: Level,
    upper: Level,
}

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("a band's lower level lies below its upper, and {lower} does not lie below {upper}")]
pub struct LevelsRefusal {
    pub lower: f64,
    pub upper: f64,
}

impl Levels {
    pub fn new(lower: Level, upper: Level) -> Result<Self, LevelsRefusal> {
        let (low, high) = (f64::from(lower), f64::from(upper));
        match low < high {
            true => Ok(Self { lower, upper }),
            false => Err(LevelsRefusal {
                lower: low,
                upper: high,
            }),
        }
    }

    /// The share of outcomes a calibrated band leaves below its lower forecast.
    pub fn expected_below(self) -> Share {
        Share::from(self.lower)
    }

    /// The share of outcomes a calibrated band leaves above its upper forecast.
    pub fn expected_above(self) -> Share {
        Share::from(self.upper).complement()
    }
}

/// One forecast band and the outcome that followed; a crossed band, its upper below its lower, is kept and counted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Band {
    lower: Forecast,
    upper: Forecast,
    outcome: Outcome,
}

impl Band {
    pub fn new(lower: Forecast, upper: Forecast, outcome: Outcome) -> Self {
        Self {
            lower,
            upper,
            outcome,
        }
    }

    pub fn width(self) -> f64 {
        self.upper.get() - self.lower.get()
    }
}

/// Counts of bands read, of outcomes below and above them, and of crossed bands; a monoid under `combine`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coverage {
    bands: u64,
    below: u64,
    above: u64,
    crossed: u64,
}

impl Monoid for Coverage {
    fn empty() -> Self {
        Self {
            bands: 0,
            below: 0,
            above: 0,
            crossed: 0,
        }
    }

    fn combine(self, other: Self) -> Self {
        let sum = |left: u64, right: u64| {
            left.checked_add(right)
                .expect("a coverage counts fewer than u64::MAX bands")
        };
        Self {
            bands: sum(self.bands, other.bands),
            below: sum(self.below, other.below),
            above: sum(self.above, other.above),
            crossed: sum(self.crossed, other.crossed),
        }
    }
}

impl Coverage {
    fn of(band: Band) -> Self {
        Self {
            bands: 1,
            below: u64::from(band.outcome.get() < band.lower.get()),
            above: u64::from(band.outcome.get() > band.upper.get()),
            crossed: u64::from(band.upper < band.lower),
        }
    }

    pub fn bands(self) -> u64 {
        self.bands
    }

    /// The share of outcomes below the lower forecast, `None` over no bands.
    pub fn below(self) -> Option<Share> {
        Share::of(self.below, self.bands)
    }

    pub fn above(self) -> Option<Share> {
        Share::of(self.above, self.bands)
    }

    pub fn crossed(self) -> Option<Share> {
        Share::of(self.crossed, self.bands)
    }
}

/// A band forecast's coverage overall and in each decile of width, narrowest first.
#[derive(Debug, Clone, PartialEq)]
pub struct Calibration {
    levels: Levels,
    by_width: [Coverage; DECILES],
}

impl Calibration {
    /// The levels its coverage is read against.
    pub fn levels(&self) -> Levels {
        self.levels
    }

    pub fn overall(&self) -> Coverage {
        concatenate(self.by_width)
    }

    pub fn by_width(&self) -> &[Coverage; DECILES] {
        &self.by_width
    }
}

/// Reads `bands` overall and in width deciles whose sizes differ by at most one, the larger first; bands of equal
/// width at a decile boundary are split in the order given.
pub fn calibrate(levels: Levels, bands: &[Band]) -> Calibration {
    let order = ordered(bands, |band| band.width());
    let mut by_width = [Coverage::empty(); DECILES];
    for (coverage, decile) in by_width.iter_mut().zip(deciles(&order)) {
        *coverage = concatenate(decile.iter().map(|index| Coverage::of(bands[*index])));
    }
    Calibration { levels, by_width }
}

/// The indices of `items` sorted by `key`, ties kept in the order given.
fn ordered<T: Copy>(items: &[T], key: impl Fn(T) -> f64) -> Vec<usize> {
    let mut order: Vec<usize> = (0..items.len()).collect();
    order.sort_by(|left, right| key(items[*left]).total_cmp(&key(items[*right])));
    order
}

/// `order` cut into deciles whose sizes differ by at most one, the larger first.
fn deciles(order: &[usize]) -> [&[usize]; DECILES] {
    let (size, larger) = (order.len() / DECILES, order.len() % DECILES);
    let mut start = 0;
    std::array::from_fn(|decile| {
        let end = start + size + usize::from(decile < larger);
        let slice = &order[start..end];
        start = end;
        slice
    })
}

/// One forecast decile's pairs and their mean forecast and mean outcome.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecileMeans {
    pub pairs: NonZeroU64,
    pub forecast: f64,
    pub outcome: f64,
}

/// The mean forecast beside the mean outcome in each decile of the forecast, lowest first, `None` for a decile
/// fewer than ten pairs leave empty; a calibrated forecast's two means agree in every decile.
pub fn by_forecast_decile(pairs: &[Pair]) -> [Option<DecileMeans>; DECILES] {
    let order = ordered(pairs, |pair| pair.forecast().get());
    deciles(&order).map(|decile| {
        let count = NonZeroU64::new(decile.len() as u64)?;
        let mean = |value: fn(Pair) -> f64| {
            decile
                .iter()
                .map(|index| value(pairs[*index]) / decile.len() as f64)
                .sum()
        };
        Some(DecileMeans {
            pairs: count,
            forecast: mean(|pair| pair.forecast().get()),
            outcome: mean(|pair| pair.outcome().get()),
        })
    })
}

/// The least-squares slope, with an intercept, of outcome on forecast over the pairs it was fit on; a forecast is
/// rescaled by the slope floored at zero, so a forecast that points the wrong way calibrates to no forecast.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Slope {
    raw: f64,
    pairs: u64,
}

/// Why no slope was fit, with the pairs read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SlopeRefusal {
    #[error("a slope needs two pairs, and {pairs} were given")]
    TooFewPairs { pairs: u64 },
    #[error("all {pairs} forecasts were equal")]
    ForecastConstant { pairs: u64 },
    #[error("the slope over {pairs} pairs is not finite")]
    NotFinite { pairs: u64 },
}

impl Slope {
    pub fn fit(pairs: &[Pair]) -> Result<Self, SlopeRefusal> {
        let count = pairs.len() as u64;
        if pairs.len() < 2 {
            return Err(SlopeRefusal::TooFewPairs { pairs: count });
        }
        let mean = |value: fn(Pair) -> f64| -> f64 {
            pairs
                .iter()
                .map(|pair| value(*pair) / pairs.len() as f64)
                .sum()
        };
        let (forecast_mean, outcome_mean) = (
            mean(|pair| pair.forecast().get()),
            mean(|pair| pair.outcome().get()),
        );
        let (covariance, variance) =
            pairs
                .iter()
                .fold((0.0, 0.0), |(covariance, variance), pair| {
                    let forecast = pair.forecast().get() - forecast_mean;
                    (
                        covariance + forecast * (pair.outcome().get() - outcome_mean),
                        variance + forecast * forecast,
                    )
                });
        let raw = covariance / variance;
        // A rounded mean leaves equal forecasts a variance just above zero, so constancy is read off the forecasts.
        let constant = pairs
            .iter()
            .all(|pair| pair.forecast() == pairs[0].forecast());
        match (constant, variance.is_finite() && raw.is_finite()) {
            (true, _) => Err(SlopeRefusal::ForecastConstant { pairs: count }),
            (false, true) => Ok(Self { raw, pairs: count }),
            (false, false) => Err(SlopeRefusal::NotFinite { pairs: count }),
        }
    }

    /// The fitted slope before flooring, negative when the forecast pointed the wrong way.
    pub fn raw(self) -> f64 {
        self.raw
    }

    pub fn pairs(self) -> u64 {
        self.pairs
    }

    pub fn calibrate(self, forecast: Forecast) -> Result<Forecast, ForecastRefusal> {
        Forecast::new(self.raw.max(0.0) * forecast.get())
    }
}

impl Component<Vec<Forecast>> for Slope {
    type Output = Vec<Forecast>;
    type Refusal = ForecastRefusal;

    /// Records the raw slope and the pairs it was fit on.
    fn apply(
        &self,
        input: Vec<Forecast>,
        diagnostics: &mut Diagnostics,
    ) -> Result<Vec<Forecast>, ForecastRefusal> {
        diagnostics.record("slope", self.raw);
        diagnostics.record("slope_pairs", self.pairs as f64);
        input
            .into_iter()
            .map(|forecast| self.calibrate(forecast))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::laboratory::permutation::Generator;
    use crate::common::monoid::laws;

    fn band(lower: f64, upper: f64, outcome: f64) -> Band {
        Band::new(
            Forecast::new(lower).unwrap(),
            Forecast::new(upper).unwrap(),
            Outcome::new(outcome).unwrap(),
        )
    }

    fn levels() -> Levels {
        Levels::new(Level::try_from(0.1).unwrap(), Level::try_from(0.9).unwrap()).unwrap()
    }

    #[test]
    fn test_levels_and_bands_refuse_what_cannot_be_read() {
        let half = Level::try_from(0.5).unwrap();
        assert_eq!(
            Levels::new(half, half).map_err(|refusal| refusal.to_string()),
            Err(
                "a band's lower level lies below its upper, and 0.5 does not lie below 0.5"
                    .to_string()
            )
        );
        assert_eq!(levels().expected_below().get(), 0.1);
        assert!((levels().expected_above().get() - 0.1).abs() < 1e-15);
    }

    /// Outcome 5 against [0, 4], [6, 9], [2, 8], the crossed [7, 3] and the empty [5, 5]: a crossed band counts on both sides.
    #[test]
    fn test_coverage_counts_each_side_and_crossed_bands() {
        let bands = [
            band(0.0, 4.0, 5.0),
            band(6.0, 9.0, 5.0),
            band(2.0, 8.0, 5.0),
            band(7.0, 3.0, 5.0),
            band(5.0, 5.0, 5.0),
        ];
        let overall = calibrate(levels(), &bands).overall();
        assert_eq!(
            overall,
            Coverage {
                bands: 5,
                below: 2,
                above: 2,
                crossed: 1
            }
        );
        assert_eq!(
            (
                overall.below().map(Share::get),
                overall.crossed().map(Share::get)
            ),
            (Some(0.4), Some(0.2))
        );
        assert_eq!(Coverage::empty().below(), None);
    }

    /// 23 bands of widths 0 to 22 fill deciles of 3, 3, 3 and then 2, narrowest first, with outcomes above only in
    /// the widest two.
    #[test]
    fn test_deciles_split_by_width_with_the_larger_first() {
        let bands: Vec<Band> = (0..23)
            .rev()
            .map(|width| {
                let outcome = if width >= 21 { 100.0 } else { 0.0 };
                band(0.0, f64::from(width), outcome)
            })
            .collect();
        let calibration = calibrate(levels(), &bands);
        let sizes: Vec<u64> = calibration
            .by_width()
            .iter()
            .map(|coverage| coverage.bands())
            .collect();
        assert_eq!(sizes, [3, 3, 3, 2, 2, 2, 2, 2, 2, 2]);
        let above: Vec<u64> = calibration
            .by_width()
            .iter()
            .map(|coverage| coverage.above)
            .collect();
        assert_eq!(above, [0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert_eq!(calibration.overall().bands(), 23);
    }

    /// Outcomes drawn uniform on [0, 1) against the band [0.1, 0.9] land within four standard errors of the levels
    /// in every decile; against a band shifted up, the machinery reports the miss.
    #[test]
    fn test_a_calibrated_band_reads_its_levels_and_a_shifted_one_does_not() {
        let mut generator = Generator::new(29);
        let outcomes: Vec<f64> = (0..20_000)
            .map(|_| (generator.next_u64() >> 11) as f64 / (1u64 << 53) as f64)
            .collect();
        let read = |lower: f64, upper: f64| {
            let bands: Vec<Band> = outcomes
                .iter()
                .map(|outcome| band(lower, upper, *outcome))
                .collect();
            calibrate(levels(), &bands)
        };
        let calibrated = read(0.1, 0.9);
        let standard_error = (0.1 * 0.9 / 2_000.0f64).sqrt();
        for coverage in calibrated.by_width() {
            assert!((coverage.below().unwrap().get() - 0.1).abs() < 4.0 * standard_error);
            assert!((coverage.above().unwrap().get() - 0.1).abs() < 4.0 * standard_error);
        }
        let shifted = read(0.3, 1.1).overall();
        assert!(
            (shifted.below().unwrap().get() - 0.3).abs() < 0.02,
            "{shifted:?}"
        );
        assert_eq!(shifted.above().map(Share::get), Some(0.0));
    }

    fn pair(forecast: f64, outcome: f64) -> Pair {
        Pair::new(
            Forecast::new(forecast).unwrap(),
            Outcome::new(outcome).unwrap(),
        )
    }

    /// Outcomes on the line 3 + 2x read slope 2, a forecast pointing the wrong way calibrates to zero, and the
    /// component records the slope it applied.
    #[test]
    fn test_a_slope_rescales_a_forecast_and_floors_at_zero() {
        let line: Vec<Pair> = (0..5)
            .map(|x| pair(f64::from(x), 3.0 + 2.0 * f64::from(x)))
            .collect();
        let slope = Slope::fit(&line).unwrap();
        assert_eq!((slope.raw(), slope.pairs()), (2.0, 5));
        let mut diagnostics = Diagnostics::default();
        let calibrated = slope
            .apply(
                vec![Forecast::new(1.5).unwrap(), Forecast::new(-4.0).unwrap()],
                &mut diagnostics,
            )
            .unwrap();
        assert_eq!(
            calibrated
                .iter()
                .map(|forecast| forecast.get())
                .collect::<Vec<_>>(),
            [3.0, -8.0]
        );
        assert_eq!(
            diagnostics.readings(),
            [("slope".to_string(), 2.0), ("slope_pairs".to_string(), 5.0)]
        );
        let backwards = Slope::fit(&[pair(0.0, 1.0), pair(1.0, 0.0)]).unwrap();
        assert_eq!(backwards.raw(), -1.0);
        assert_eq!(
            backwards
                .calibrate(Forecast::new(7.0).unwrap())
                .map(Forecast::get),
            Ok(0.0)
        );
        assert_eq!(
            Slope::fit(&[pair(1e200, 1e200), pair(-1e200, -1e200)]),
            Err(SlopeRefusal::NotFinite { pairs: 2 })
        );
    }

    #[test]
    fn test_a_slope_refuses_what_has_none() {
        let refused = |pairs: &[Pair]| Slope::fit(pairs).map_err(|refusal| refusal.to_string());
        assert_eq!(
            refused(&[pair(1.0, 2.0)]),
            Err("a slope needs two pairs, and 1 were given".to_string())
        );
        assert_eq!(
            refused(&[pair(1.0, 2.0), pair(1.0, 5.0)]),
            Err("all 2 forecasts were equal".to_string())
        );
        let constant: Vec<Pair> = (0..11).map(|x| pair(0.1, f64::from(x))).collect();
        assert_eq!(
            Slope::fit(&constant),
            Err(SlopeRefusal::ForecastConstant { pairs: 11 })
        );
    }

    /// Forecasts 0 to 22 against outcomes twice them fill deciles of 3, 3, 3 and then 2, each reading its own means;
    /// five pairs leave the last five deciles unmeasured.
    #[test]
    fn test_forecast_deciles_read_each_decile_mean() {
        let pairs: Vec<Pair> = (0..23)
            .rev()
            .map(|x| pair(f64::from(x), 2.0 * f64::from(x)))
            .collect();
        let deciles = by_forecast_decile(&pairs);
        let read: Vec<(u64, f64, f64)> = deciles
            .iter()
            .map(|decile| {
                let decile = decile.unwrap();
                (
                    decile.pairs.get(),
                    (decile.forecast * 1e9).round() / 1e9,
                    (decile.outcome * 1e9).round() / 1e9,
                )
            })
            .collect();
        assert_eq!(
            &read[..4],
            [(3, 1.0, 2.0), (3, 4.0, 8.0), (3, 7.0, 14.0), (2, 9.5, 19.0)]
        );
        assert_eq!(read[9], (2, 21.5, 43.0));
        let few = by_forecast_decile(&pairs[..5]);
        assert_eq!(few.iter().filter(|decile| decile.is_none()).count(), 5);
        assert!(
            few[..5]
                .iter()
                .all(|decile| decile.is_some_and(|decile| decile.pairs.get() == 1))
        );
    }

    fn pairs() -> impl Strategy<Value = Vec<Pair>> {
        prop::collection::vec((-1e3_f64..1e3, -1e3_f64..1e3), 2..60).prop_map(|values| {
            values
                .into_iter()
                .map(|(forecast, outcome)| pair(forecast, outcome))
                .collect()
        })
    }

    fn coverage() -> impl Strategy<Value = Coverage> {
        (0..1_000u64, 0..1_000u64, 0..1_000u64, 0..1_000u64).prop_map(
            |(bands, below, above, crossed)| Coverage {
                bands,
                below,
                above,
                crossed,
            },
        )
    }

    fn bands() -> impl Strategy<Value = Vec<Band>> {
        prop::collection::vec((-50i32..50, -50i32..50, -50i32..50), 0..80).prop_map(|values| {
            values
                .into_iter()
                .map(|(lower, upper, outcome)| {
                    band(f64::from(lower), f64::from(upper), f64::from(outcome))
                })
                .collect()
        })
    }

    proptest! {
        #[test]
        fn test_coverage_is_a_monoid(first in coverage(), second in coverage(), third in coverage()) {
            laws::check(first, second, third)?;
        }

        /// Every band lands in exactly one decile, and the deciles differ in size by at most one, the larger first.
        #[test]
        fn test_deciles_partition_the_bands_in_width_order(bands in bands()) {
            let calibration = calibrate(levels(), &bands);
            let sizes: Vec<u64> = calibration.by_width().iter().map(|coverage| coverage.bands()).collect();
            prop_assert_eq!(sizes.iter().sum::<u64>(), bands.len() as u64);
            prop_assert_eq!(calibration.overall().bands(), bands.len() as u64);
            prop_assert!(sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1);
            prop_assert!(sizes.windows(2).all(|pair| pair[0] >= pair[1]));
            let below: u64 = bands.iter().map(|band| u64::from(band.outcome.get() < band.lower.get())).sum();
            prop_assert_eq!(calibration.overall().below, below);
        }

        /// Scaling outcomes scales the slope, shifting either side leaves it, and a forecast calibrated by a positive
        /// slope refits to a slope of one.
        #[test]
        fn test_a_slope_is_equivariant_and_calibration_is_idempotent(
            pairs in pairs(),
            scale in 0.1_f64..10.0,
            shift in -1e3_f64..1e3,
        ) {
            let Ok(slope) = Slope::fit(&pairs) else {
                return Ok(());
            };
            let tolerance = |value: f64| 1e-6 * value.abs().max(1.0);
            let moved: Vec<Pair> = pairs
                .iter()
                .map(|each| pair(each.forecast().get() + shift, scale * each.outcome().get() - shift))
                .collect();
            let refit = Slope::fit(&moved).unwrap().raw();
            prop_assert!((refit - scale * slope.raw()).abs() < tolerance(refit), "{refit} {}", slope.raw());
            if slope.raw() > 1e-3 {
                let calibrated: Vec<Pair> = pairs
                    .iter()
                    .map(|each| Pair::new(slope.calibrate(each.forecast()).unwrap(), each.outcome()))
                    .collect();
                let unit = Slope::fit(&calibrated).unwrap().raw();
                prop_assert!((unit - 1.0).abs() < 1e-6, "{unit}");
            }
        }

        /// Every pair lands in one decile, and decile mean forecasts never fall.
        #[test]
        fn test_forecast_deciles_partition_the_pairs_in_forecast_order(pairs in pairs()) {
            let deciles = by_forecast_decile(&pairs);
            let counted: u64 = deciles.iter().flatten().map(|decile| decile.pairs.get()).sum();
            prop_assert_eq!(counted, pairs.len() as u64);
            let forecasts: Vec<f64> = deciles.iter().flatten().map(|decile| decile.forecast).collect();
            prop_assert!(forecasts.windows(2).all(|two| two[0] <= two[1] + 1e-9), "{forecasts:?}");
        }
    }
}
