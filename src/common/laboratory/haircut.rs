//! How many standard errors a reading must clear, given how many tests its family has run.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// The share of families in which at least one reading may clear by chance.
pub const FAMILY_WISE_ERROR_RATE: f64 = 0.05;

/// The Bonferroni bar for a family of `tests`, two-sided; Bonferroni rather than Šidák because readings in one
/// family share a universe and a window, and only Bonferroni's bound holds without independence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Haircut {
    tests: NonZeroU32,
}

impl Haircut {
    pub fn new(tests: NonZeroU32) -> Self {
        Self { tests }
    }

    pub fn tests(self) -> NonZeroU32 {
        self.tests
    }

    pub fn required_standard_errors(self) -> f64 {
        required_standard_errors(self.tests, FAMILY_WISE_ERROR_RATE)
            .expect("the shipped rate is a probability")
    }

    /// Whether `standard_errors` from zero, in either direction, clears the bar.
    pub fn clears(self, standard_errors: f64) -> bool {
        standard_errors.abs() >= self.required_standard_errors()
    }
}

fn required_standard_errors(tests: NonZeroU32, rate: f64) -> Option<f64> {
    if !(rate > 0.0 && rate < 1.0) {
        return None;
    }
    inverse_standard_normal(1.0 - rate / (2.0 * f64::from(tests.get())))
}

/// Acklam's rational approximation, polished by one Halley step against the evaluated normal integral.
fn inverse_standard_normal(probability: f64) -> Option<f64> {
    if !(probability > 0.0 && probability < 1.0) {
        return None;
    }
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239e0,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838e0,
        -2.549_732_539_343_734e0,
        4.374_664_141_464_968e0,
        2.938_163_982_698_783e0,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996e0,
        3.754_408_661_907_416e0,
    ];
    const BREAK: f64 = 0.02425;
    let tail = |q: f64| {
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    let mut z = if probability < BREAK {
        tail((-2.0 * probability.ln()).sqrt())
    } else if probability <= 1.0 - BREAK {
        let q = probability - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        -tail((-2.0 * (1.0 - probability).ln()).sqrt())
    };
    let error = 0.5 * erfc(-z / std::f64::consts::SQRT_2) - probability;
    let density = (-0.5 * z * z).exp() / (2.0 * std::f64::consts::PI).sqrt();
    if density > 0.0 {
        let step = error / density;
        z -= step / (1.0 + 0.5 * z * step);
    }
    Some(z)
}

/// The complementary error function: a series below one, a continued fraction above it.
fn erfc(x: f64) -> f64 {
    let magnitude = x.abs();
    let tail = match magnitude < 1.0 {
        true => erfc_by_series(magnitude),
        false => erfc_by_continued_fraction(magnitude),
    };
    match x >= 0.0 {
        true => tail,
        false => 2.0 - tail,
    }
}

/// The Taylor series for erf, subtracted from one; for non-negative arguments.
fn erfc_by_series(x: f64) -> f64 {
    let mut term = x;
    let mut sum = x;
    for index in 1..200 {
        term *= -x * x / index as f64;
        let addition = term / (2.0 * index as f64 + 1.0);
        sum += addition;
        if addition.abs() < 1e-18 * sum.abs() {
            break;
        }
    }
    1.0 - 2.0 / std::f64::consts::PI.sqrt() * sum
}

/// Lentz's method on the continued fraction for the upper incomplete gamma at a half; for non-negative arguments.
fn erfc_by_continued_fraction(x: f64) -> f64 {
    let squared = x * x;
    let mut f = 1e-300;
    let mut c = f;
    let mut d = 0.0;
    for index in 0..300 {
        let (a, b) = match index {
            0 => (1.0, squared + 0.5),
            _ => {
                let step = index as f64;
                (-step * (step - 0.5), squared + 2.0 * step + 0.5)
            }
        };
        d = b + a * d;
        if d.abs() < 1e-300 {
            d = 1e-300;
        }
        c = b + a / c;
        if c.abs() < 1e-300 {
            c = 1e-300;
        }
        d = 1.0 / d;
        let delta = c * d;
        f *= delta;
        if (delta - 1.0).abs() < 1e-17 {
            break;
        }
    }
    x * (-squared).exp() / std::f64::consts::PI.sqrt() * f
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tests(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).unwrap()
    }

    fn bar(count: u32) -> f64 {
        Haircut::new(tests(count)).required_standard_errors()
    }

    #[test]
    fn test_one_test_is_the_conventional_two_standard_errors() {
        assert!((bar(1) - 1.959_963_984_540_054).abs() < 1e-10, "{}", bar(1));
    }

    #[test]
    fn test_forty_tests_demand_three_and_a_quarter_standard_errors() {
        assert!(
            (bar(40) - 3.227_218_425_963_163).abs() < 1e-9,
            "{}",
            bar(40)
        );
    }

    #[test]
    fn test_five_tests_at_five_percent_is_one_test_at_one_percent() {
        let spread = required_standard_errors(tests(5), 0.05).unwrap();
        let single = required_standard_errors(tests(1), 0.01).unwrap();
        assert!((spread - single).abs() < 1e-12, "{spread} against {single}");
        assert!((spread - 2.575_829_303_548_9).abs() < 1e-9);
    }

    #[test]
    fn test_the_bar_rises_with_every_test_added() {
        let bars: Vec<f64> = [1, 2, 5, 10, 40].map(bar).to_vec();
        assert!(bars.windows(2).all(|pair| pair[1] > pair[0]), "{bars:?}");
    }

    #[test]
    fn test_clearing_is_two_sided_and_inclusive() {
        let haircut = Haircut::new(tests(1));
        assert!(haircut.clears(-2.0));
        assert!(haircut.clears(2.0));
        assert!(!haircut.clears(1.9));
        assert!(haircut.clears(haircut.required_standard_errors()));
    }

    #[test]
    fn test_a_rate_that_is_not_a_probability_is_refused() {
        for rate in [0.0, 1.0, -0.1, 1.5, f64::NAN, f64::INFINITY] {
            assert_eq!(required_standard_errors(tests(3), rate), None, "{rate}");
        }
    }

    #[test]
    fn test_the_quantile_is_symmetric_and_holds_in_the_far_tail() {
        for probability in [0.001, 0.01, 0.2, 0.4, 0.49] {
            let low = inverse_standard_normal(probability).unwrap();
            let high = inverse_standard_normal(1.0 - probability).unwrap();
            assert!((low + high).abs() < 1e-11, "{low} against {high}");
        }
        assert!(inverse_standard_normal(0.5).unwrap().abs() < 1e-12);
        let far = inverse_standard_normal(1.0 - 0.001 / 2.0).unwrap();
        assert!((far - 3.290_526_731_491_925).abs() < 1e-9, "{far}");
        for refused in [0.0, 1.0, -0.5, 2.0, f64::NAN] {
            assert_eq!(inverse_standard_normal(refused), None, "{refused}");
        }
    }

    /// Reference values computed independently in 50-digit decimal arithmetic.
    #[test]
    fn test_each_branch_of_erfc_holds_across_the_crossover() {
        for (argument, expected) in [
            (0.8_f64, 2.578_990_352_923_395_4e-1_f64),
            (1.0, 1.572_992_070_502_851_3e-1),
            (1.5, 3.389_485_352_468_927_4e-2),
        ] {
            let series = erfc_by_series(argument);
            let fraction = erfc_by_continued_fraction(argument);
            assert!(
                (series / expected - 1.0).abs() < 1e-13,
                "series at {argument}: {series}"
            );
            assert!(
                (fraction / expected - 1.0).abs() < 1e-13,
                "fraction at {argument}: {fraction}"
            );
        }
    }

    #[test]
    fn test_erfc_matches_its_published_values_on_both_sides_of_zero() {
        for (argument, expected) in [
            (0.5_f64, 4.795_001_221_869_535e-1_f64),
            (1.0, 1.572_992_070_502_851_3e-1),
            (2.0, 4.677_734_981_047_266e-3),
            (3.0, 2.209_049_699_858_544e-5),
        ] {
            assert!(
                (erfc(argument) / expected - 1.0).abs() < 1e-14,
                "at {argument}"
            );
            let mirrored = erfc(-argument);
            assert!(
                ((mirrored - (2.0 - expected)) / (2.0 - expected)).abs() < 1e-14,
                "at -{argument}"
            );
        }
    }
}
