//! The seeds and distributions the synthetic market draws from, written here rather than taken from a distributions
//! crate so a seed and a commit name every draw.

use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::common::laboratory::permutation::Generator;

/// The fewest readings a fit takes, since a dispersion needs two.
const MINIMUM_FIT_READINGS: usize = 2;

/// The value every draw of a synthetic run descends from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seed(u64);

impl Seed {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    /// The seed of `stream`: the first draw of the permutation generator started `stream` steps along from this seed,
    /// so neighbouring streams start from unrelated states.
    pub(super) fn child(self, stream: u64) -> Self {
        Self(
            Generator::new(
                self.0
                    .wrapping_add(stream.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
            )
            .next_u64(),
        )
    }

    pub(super) fn generator(self) -> StdRng {
        StdRng::seed_from_u64(self.0)
    }
}

/// A positive quantity whose logarithm is normal: `median` is its median and `dispersion` the standard deviation of
/// its logarithm.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LogNormal {
    median: f64,
    dispersion: f64,
}

/// Why a log-normal was refused, with the value read.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum LogNormalRefusal {
    #[error("a median of {value} is not finite and positive")]
    Median { value: f64 },
    #[error("a dispersion of {value} is not finite and non-negative")]
    Dispersion { value: f64 },
    #[error("{count} readings is fewer than the {minimum} a fit needs")]
    TooFewReadings { count: usize, minimum: usize },
    #[error("a reading of {value} is not finite and positive")]
    Reading { value: f64 },
}

impl LogNormal {
    pub fn new(median: f64, dispersion: f64) -> Result<Self, LogNormalRefusal> {
        if !(median.is_finite() && median > 0.0) {
            return Err(LogNormalRefusal::Median { value: median });
        }
        if !(dispersion.is_finite() && dispersion >= 0.0) {
            return Err(LogNormalRefusal::Dispersion { value: dispersion });
        }
        Ok(Self { median, dispersion })
    }

    /// The log-normal whose logarithm has the readings' mean and sample standard deviation.
    pub fn fit(readings: &[f64]) -> Result<Self, LogNormalRefusal> {
        if readings.len() < MINIMUM_FIT_READINGS {
            return Err(LogNormalRefusal::TooFewReadings {
                count: readings.len(),
                minimum: MINIMUM_FIT_READINGS,
            });
        }
        if let Some(value) = readings
            .iter()
            .find(|value| !(value.is_finite() && **value > 0.0))
        {
            return Err(LogNormalRefusal::Reading { value: *value });
        }
        let logarithms: Vec<f64> = readings.iter().map(|value| value.ln()).collect();
        let count = logarithms.len() as f64;
        let mean = logarithms.iter().sum::<f64>() / count;
        let variance = logarithms
            .iter()
            .map(|logarithm| (logarithm - mean).powi(2))
            .sum::<f64>()
            / (count - 1.0);
        Self::new(mean.exp(), variance.sqrt())
    }

    pub fn median(self) -> f64 {
        self.median
    }

    pub fn dispersion(self) -> f64 {
        self.dispersion
    }

    pub(super) fn draw(self, generator: &mut impl Rng) -> f64 {
        self.median * (self.dispersion * standard_normal(generator)).exp()
    }
}

/// A uniform draw on `(0, 1]`, so its logarithm is finite.
fn open_unit(generator: &mut impl Rng) -> f64 {
    1.0 - generator.random::<f64>()
}

/// A standard normal draw by Box-Muller, keeping one of the pair.
pub(super) fn standard_normal(generator: &mut impl Rng) -> f64 {
    let radius = (-2.0 * open_unit(generator).ln()).sqrt();
    radius * (std::f64::consts::TAU * generator.random::<f64>()).cos()
}

/// An exponential draw with `mean`.
pub(super) fn exponential(generator: &mut impl Rng, mean: f64) -> f64 {
    -mean * open_unit(generator).ln()
}

/// A uniform draw on `[0, 1)`.
pub(super) fn uniform(generator: &mut impl Rng) -> f64 {
    generator.random::<f64>()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_a_fit_recovers_the_log_normal_it_was_drawn_from() {
        let truth = LogNormal::new(12.0, 0.8).unwrap();
        let mut generator = Seed::new(3).generator();
        let readings: Vec<f64> = (0..20_000).map(|_| truth.draw(&mut generator)).collect();
        let fitted = LogNormal::fit(&readings).unwrap();
        assert!((fitted.median() / 12.0 - 1.0).abs() < 0.03, "{fitted:?}");
        assert!((fitted.dispersion() - 0.8).abs() < 0.02, "{fitted:?}");
    }

    #[test]
    fn test_a_fit_refuses_too_few_or_non_positive_readings() {
        assert_eq!(
            LogNormal::fit(&[4.0]),
            Err(LogNormalRefusal::TooFewReadings {
                count: 1,
                minimum: 2
            })
        );
        assert_eq!(
            LogNormal::fit(&[4.0, 0.0]),
            Err(LogNormalRefusal::Reading { value: 0.0 })
        );
    }

    #[test]
    fn test_a_fit_of_equal_readings_has_no_dispersion() {
        let fitted = LogNormal::fit(&[5.0, 5.0, 5.0]).unwrap();
        assert!((fitted.median() - 5.0).abs() < 1e-12);
        assert_eq!(fitted.dispersion(), 0.0);
    }

    #[test]
    fn test_draws_have_the_stated_moments() {
        let mut generator = Seed::new(11).generator();
        let normals: Vec<f64> = (0..40_000)
            .map(|_| standard_normal(&mut generator))
            .collect();
        let mean = normals.iter().sum::<f64>() / 40_000.0;
        let variance = normals.iter().map(|value| value.powi(2)).sum::<f64>() / 40_000.0;
        assert!(mean.abs() < 0.02, "{mean}");
        assert!((variance - 1.0).abs() < 0.03, "{variance}");
        let waits: Vec<f64> = (0..40_000)
            .map(|_| exponential(&mut generator, 3.0))
            .collect();
        assert!(waits.iter().all(|wait| *wait >= 0.0));
        let mean_wait = waits.iter().sum::<f64>() / 40_000.0;
        assert!((mean_wait - 3.0).abs() < 0.06, "{mean_wait}");
    }

    proptest! {
        #[test]
        fn test_children_differ_from_their_parent_and_each_other(parent in any::<u64>(), stream in 0u64..1_000) {
            let seed = Seed::new(parent);
            prop_assert_ne!(seed.child(stream), seed.child(stream + 1));
            prop_assert_eq!(seed.child(stream), seed.child(stream));
        }
    }
}
