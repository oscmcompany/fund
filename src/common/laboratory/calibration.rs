//! Whether a forecast's quantile band holds the share of outcomes its levels promise, overall and by decile of the
//! band's width, since a band can be right on average and wrong at every width.

use crate::common::laboratory::score::{Forecast, Outcome};
use crate::common::laboratory::share::Share;
use crate::common::learning::loss::Level;
use crate::common::monoid::{Monoid, concatenate};

/// Width deciles a calibration is read in.
pub const WIDTH_DECILES: usize = 10;

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
    by_width: [Coverage; WIDTH_DECILES],
}

impl Calibration {
    pub fn overall(&self) -> Coverage {
        concatenate(self.by_width)
    }

    pub fn by_width(&self) -> &[Coverage; WIDTH_DECILES] {
        &self.by_width
    }
}

/// Reads `bands` overall and in width deciles whose sizes differ by at most one, the larger first; bands of equal
/// width at a decile boundary are split in the order given.
pub fn calibrate(bands: &[Band]) -> Calibration {
    let mut order: Vec<usize> = (0..bands.len()).collect();
    order.sort_by(|left, right| bands[*left].width().total_cmp(&bands[*right].width()));
    let (size, larger) = (bands.len() / WIDTH_DECILES, bands.len() % WIDTH_DECILES);
    let mut by_width = [Coverage::empty(); WIDTH_DECILES];
    let mut start = 0;
    for (decile, coverage) in by_width.iter_mut().enumerate() {
        let end = start + size + usize::from(decile < larger);
        *coverage = concatenate(
            order[start..end]
                .iter()
                .map(|index| Coverage::of(bands[*index])),
        );
        start = end;
    }
    Calibration { by_width }
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
        let overall = calibrate(&bands).overall();
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
        let calibration = calibrate(&bands);
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
            calibrate(&bands)
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
            let calibration = calibrate(&bands);
            let sizes: Vec<u64> = calibration.by_width().iter().map(|coverage| coverage.bands()).collect();
            prop_assert_eq!(sizes.iter().sum::<u64>(), bands.len() as u64);
            prop_assert_eq!(calibration.overall().bands(), bands.len() as u64);
            prop_assert!(sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1);
            prop_assert!(sizes.windows(2).all(|pair| pair[0] >= pair[1]));
            let below: u64 = bands.iter().map(|band| u64::from(band.outcome.get() < band.lower.get())).sum();
            prop_assert_eq!(calibration.overall().below, below);
        }
    }
}
