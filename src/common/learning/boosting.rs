//! Gradient-boosted trees: inputs quantized once into bins, histograms of derivatives folded as a monoid, trees
//! grown leaf-wise from them, and an ensemble whose score is the sum of its trees'.

pub mod bins;
pub mod ensemble;
pub mod histogram;
pub mod tree;

/// A share of a population, in (0, 1].
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Fraction(f64);

impl Fraction {
    pub fn new(value: f64) -> Option<Self> {
        (value > 0.0 && value <= 1.0).then_some(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }

    /// The share of `population` this fraction keeps, rounded up so a nonempty population keeps at least one.
    pub fn of(self, population: usize) -> usize {
        ((self.0 * population as f64).ceil() as usize).min(population)
    }
}

/// The L2 penalty on a fitted weight, a leaf's value or a ridge coefficient; positive and finite, so a leaf's
/// denominator never vanishes.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Penalty(f64);

impl Penalty {
    pub fn new(value: f64) -> Option<Self> {
        (value > 0.0 && value.is_finite()).then_some(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_fraction_keeps_at_least_one_of_a_nonempty_population() {
        assert_eq!(Fraction::new(0.0), None);
        assert_eq!(Fraction::new(1.5), None);
        assert_eq!(Fraction::new(f64::NAN), None);
        let fraction = Fraction::new(0.8).unwrap();
        assert_eq!(
            (fraction.of(75), fraction.of(1), fraction.of(0)),
            (60, 1, 0)
        );
        assert_eq!(Fraction::new(1.0).unwrap().of(7), 7);
        assert_eq!(Penalty::new(0.0), None);
        assert_eq!(Penalty::new(f64::INFINITY), None);
        assert_eq!(Penalty::new(10.0).unwrap().get(), 10.0);
    }
}
