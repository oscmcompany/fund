//! How many standard errors a reading must clear, given how many tests its family has run and how many degrees of
//! freedom its error was estimated with.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// The share of families in which at least one reading may clear by chance.
pub const FAMILY_WISE_ERROR_RATE: f64 = 0.05;

/// The degrees of freedom a standard error was estimated with: finite and at least one, fractional for Welch.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64")]
pub struct DegreesOfFreedom(f64);

impl DegreesOfFreedom {
    pub fn new(value: f64) -> Option<Self> {
        (value.is_finite() && value >= 1.0).then_some(Self(value))
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for DegreesOfFreedom {
    type Error = String;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
            .ok_or_else(|| format!("{value} is not a usable count of degrees of freedom"))
    }
}

/// The Bonferroni bar for a family of `tests`, two-sided, against Student's t; Bonferroni rather than Šidák because
/// readings in one family share a universe and a window, and only Bonferroni's bound holds without independence.
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

    pub fn required_standard_errors(self, degrees_of_freedom: DegreesOfFreedom) -> f64 {
        student_quantile(
            FAMILY_WISE_ERROR_RATE / f64::from(self.tests.get()),
            degrees_of_freedom,
        )
    }

    /// Whether `standard_errors` from zero, in either direction, clears the bar.
    pub fn clears(self, standard_errors: f64, degrees_of_freedom: DegreesOfFreedom) -> bool {
        standard_errors.abs() >= self.required_standard_errors(degrees_of_freedom)
    }
}

/// The t beyond which Student's distribution puts `two_sided_tail` of its mass, split evenly between the tails.
fn student_quantile(two_sided_tail: f64, degrees_of_freedom: DegreesOfFreedom) -> f64 {
    let tail = |statistic: f64| {
        let freedom = degrees_of_freedom.value();
        regularized_incomplete_beta(
            freedom / (freedom + statistic * statistic),
            freedom / 2.0,
            0.5,
        )
    };
    let mut upper = 1.0;
    while tail(upper) > two_sided_tail {
        upper *= 2.0;
    }
    let mut lower = 0.0;
    for _ in 0..200 {
        let middle = 0.5 * (lower + upper);
        match tail(middle) > two_sided_tail {
            true => lower = middle,
            false => upper = middle,
        }
    }
    0.5 * (lower + upper)
}

/// I_x(a, b) by its continued fraction, taken on whichever side of the mean converges.
fn regularized_incomplete_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let front =
        (a * x.ln() + b * (1.0 - x).ln() + log_gamma(a + b) - log_gamma(a) - log_gamma(b)).exp();
    match x < (a + 1.0) / (a + b + 2.0) {
        true => front * beta_continued_fraction(x, a, b) / a,
        false => 1.0 - front * beta_continued_fraction(1.0 - x, b, a) / b,
    }
}

/// Lentz's method on the incomplete beta's continued fraction.
fn beta_continued_fraction(x: f64, a: f64, b: f64) -> f64 {
    const TINY: f64 = 1e-300;
    let guard = |value: f64| match value.abs() < TINY {
        true => TINY,
        false => value,
    };
    let mut numerator = 1.0;
    let mut denominator = 1.0 / guard(1.0 - (a + b) * x / (a + 1.0));
    let mut fraction = denominator;
    for step in 1..500 {
        let step = f64::from(step);
        let even = step * (b - step) * x / ((a + 2.0 * step - 1.0) * (a + 2.0 * step));
        denominator = 1.0 / guard(1.0 + even * denominator);
        numerator = guard(1.0 + even / numerator);
        fraction *= denominator * numerator;
        let odd = -(a + step) * (a + b + step) * x / ((a + 2.0 * step) * (a + 2.0 * step + 1.0));
        denominator = 1.0 / guard(1.0 + odd * denominator);
        numerator = guard(1.0 + odd / numerator);
        let change = denominator * numerator;
        fraction *= change;
        if (change - 1.0).abs() < 1e-16 {
            break;
        }
    }
    fraction
}

/// The Lanczos approximation (g = 7), for arguments of at least one half.
fn log_gamma(argument: f64) -> f64 {
    const COEFFICIENTS: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    let shifted = argument - 1.0;
    let series = COEFFICIENTS[1..]
        .iter()
        .enumerate()
        .fold(COEFFICIENTS[0], |sum, (index, coefficient)| {
            sum + coefficient / (shifted + index as f64 + 1.0)
        });
    let base = shifted + 7.5;
    0.5 * (2.0 * std::f64::consts::PI).ln() + (shifted + 0.5) * base.ln() - base + series.ln()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn freedom(value: f64) -> DegreesOfFreedom {
        DegreesOfFreedom::new(value).unwrap()
    }

    fn bar(tests: u32, degrees_of_freedom: f64) -> f64 {
        Haircut::new(NonZeroU32::new(tests).unwrap())
            .required_standard_errors(freedom(degrees_of_freedom))
    }

    /// One and two degrees of freedom have closed-form quantiles, which pin the numerics without a table.
    #[test]
    fn test_the_quantile_matches_its_closed_forms() {
        for tail in [0.05, 0.01, 0.001, 0.05 / 40.0] {
            let probability = 1.0 - tail / 2.0;
            let one = (std::f64::consts::PI * (probability - 0.5)).tan();
            let two = (2.0 * probability - 1.0) / (2.0 * probability * (1.0 - probability)).sqrt();
            for (degrees_of_freedom, expected) in [(1.0, one), (2.0, two)] {
                let measured = student_quantile(tail, freedom(degrees_of_freedom));
                assert!(
                    (measured / expected - 1.0).abs() < 1e-9,
                    "{tail} at {degrees_of_freedom}: {measured}"
                );
            }
        }
        assert!(
            (bar(1, 1.0) - 12.706_204_736).abs() < 1e-6,
            "{}",
            bar(1, 1.0)
        );
    }

    #[test]
    fn test_the_quantile_matches_published_tables_and_tends_to_the_normal() {
        assert!(
            (bar(1, 10.0) - 2.228_138_852).abs() < 1e-6,
            "{}",
            bar(1, 10.0)
        );
        assert!(
            (bar(1, 29.0) - 2.045_229_642).abs() < 1e-6,
            "{}",
            bar(1, 29.0)
        );
        assert!(
            (bar(1, 1e9) - 1.959_963_984_540_054).abs() < 1e-6,
            "{}",
            bar(1, 1e9)
        );
        assert!(
            (bar(40, 1e9) - 3.227_218_425_963_163).abs() < 1e-6,
            "{}",
            bar(40, 1e9)
        );
    }

    #[test]
    fn test_the_bar_rises_with_tests_and_falls_with_degrees_of_freedom() {
        let by_tests: Vec<f64> = [1, 2, 5, 10, 40].map(|tests| bar(tests, 20.0)).to_vec();
        assert!(
            by_tests.windows(2).all(|pair| pair[1] > pair[0]),
            "{by_tests:?}"
        );
        let by_freedom: Vec<f64> = [1.0, 1.5, 3.0, 30.0, 300.0]
            .map(|freedom| bar(5, freedom))
            .to_vec();
        assert!(
            by_freedom.windows(2).all(|pair| pair[1] < pair[0]),
            "{by_freedom:?}"
        );
    }

    #[test]
    fn test_clearing_is_two_sided_and_inclusive() {
        let haircut = Haircut::new(NonZeroU32::MIN);
        let required = haircut.required_standard_errors(freedom(10.0));
        assert!(haircut.clears(-required, freedom(10.0)));
        assert!(haircut.clears(required, freedom(10.0)));
        assert!(!haircut.clears(2.2, freedom(10.0)));
        assert!(haircut.clears(2.2, freedom(1e9)));
    }

    #[test]
    fn test_degrees_of_freedom_below_one_or_non_finite_are_refused_even_when_stored() {
        for refused in [0.5, 0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(DegreesOfFreedom::new(refused), None, "{refused}");
        }
        assert_eq!(
            serde_json::from_str::<DegreesOfFreedom>("4.5").unwrap(),
            freedom(4.5)
        );
        assert!(serde_json::from_str::<DegreesOfFreedom>("0.5").is_err());
    }

    #[test]
    fn test_log_gamma_matches_factorials_and_the_half() {
        for (argument, expected) in [
            (0.5, std::f64::consts::PI.sqrt().ln()),
            (1.0, 0.0),
            (5.0, 24f64.ln()),
            (11.0, 3_628_800f64.ln()),
        ] {
            assert!(
                (log_gamma(argument) - expected).abs() < 1e-12,
                "{argument}: {}",
                log_gamma(argument)
            );
        }
    }
}
