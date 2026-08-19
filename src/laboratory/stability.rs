//! Whether a per-session reading carries into the sessions after it.
//!
//! The cross-sectional statistics say nothing about time; this asks whether their sign holds.

use serde::Serialize;

use crate::laboratory::metrics::pearson_correlation;

/// Lags reported by default. Ten sessions is two trading weeks, far enough for a decay to show.
pub const DEFAULT_LAGS: usize = 10;

/// How a session's reading relates to the reading `lag` sessions later.
///
/// The error is taken under the null that the series has no memory, which is the hypothesis being
/// tested — so a correlation inside twice its error is a series that forgets.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Autocorrelation {
    pub lag: usize,
    pub correlation: f64,
    pub standard_error: f64,
    pub pairs: usize,
}

/// How often a session's reading shares a sign with the reading `lag` sessions later.
///
/// The error is that of a coin rather than of the observed rate, for the same reason: one half is
/// the claim being tested.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SignAgreement {
    pub lag: usize,
    pub rate: f64,
    pub standard_error: f64,
    pub pairs: usize,
}

/// Pairs each session's reading with the one `lag` sessions later, where both exist.
///
/// A session the statistic could not measure breaks the run rather than closing it: pairing across
/// the gap would call a longer step a shorter one, which is what session contiguity already forbids
/// on the other side of the measurement.
fn paired_by_lag(values: &[Option<f64>], lag: usize) -> (Vec<f64>, Vec<f64>) {
    if lag == 0 || lag >= values.len() {
        return (Vec::new(), Vec::new());
    }
    values
        .iter()
        .zip(&values[lag..])
        .filter_map(|(current, later)| current.zip(*later))
        .filter(|(current, later)| current.is_finite() && later.is_finite())
        .unzip()
}

/// Correlation between each session's reading and the reading `lag` sessions later.
pub fn autocorrelation(values: &[Option<f64>], lag: usize) -> Option<Autocorrelation> {
    let (current, later) = paired_by_lag(values, lag);
    let correlation = pearson_correlation(&current, &later)?;
    Some(Autocorrelation {
        lag,
        correlation,
        standard_error: 1.0 / (current.len() as f64).sqrt(),
        pairs: current.len(),
    })
}

/// Share of pairs whose two readings share a sign.
///
/// The blunt form of the same question and the one a book acts on: above a half is a relationship
/// that held, below is one that flipped. A reading of exactly zero points nowhere and takes its
/// pair with it.
pub fn sign_agreement(values: &[Option<f64>], lag: usize) -> Option<SignAgreement> {
    let (current, later) = paired_by_lag(values, lag);
    let agreed: Vec<bool> = current
        .iter()
        .zip(&later)
        .filter(|(first, second)| **first != 0.0 && **second != 0.0)
        .map(|(first, second)| first.is_sign_positive() == second.is_sign_positive())
        .collect();
    if agreed.len() < 2 {
        return None;
    }
    Some(SignAgreement {
        lag,
        rate: agreed.iter().filter(|agreement| **agreement).count() as f64 / agreed.len() as f64,
        standard_error: 0.5 / (agreed.len() as f64).sqrt(),
        pairs: agreed.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readings(values: impl IntoIterator<Item = f64>) -> Vec<Option<f64>> {
        values.into_iter().map(Some).collect()
    }

    /// A series that flips every session is as far from memoryless as one that repeats — and the
    /// difference is the sign. Reading only "is it positive" would call this nothing, when it is a
    /// relationship a book could act on by flipping with it.
    #[test]
    fn test_an_alternating_series_is_measured_as_strongly_negative() {
        let series = readings((0..200).map(|index| if index % 2 == 0 { 0.1 } else { -0.1 }));

        let first = autocorrelation(&series, 1).unwrap();
        assert!((first.correlation + 1.0).abs() < 1e-9, "{first:?}");
        assert_eq!(sign_agreement(&series, 1).unwrap().rate, 0.0);

        // And two sessions on it is back where it started.
        let second = autocorrelation(&series, 2).unwrap();
        assert!((second.correlation - 1.0).abs() < 1e-9, "{second:?}");
        assert_eq!(sign_agreement(&series, 2).unwrap().rate, 1.0);
    }

    /// A relationship that holds its sign reads as a whole agreement, whatever its magnitudes do.
    #[test]
    fn test_a_series_that_keeps_its_sign_agrees_throughout() {
        let series = readings((0..200).map(|index| 0.01 * ((index % 7) + 1) as f64));
        assert_eq!(sign_agreement(&series, 1).unwrap().rate, 1.0);
    }

    /// The control the whole measurement is read against: a series with no memory must land inside
    /// its own error, or the error is wrong and every other row is unreadable.
    #[test]
    fn test_a_memoryless_series_lands_inside_its_error() {
        use rand::{rngs::StdRng, RngExt, SeedableRng};
        let mut generator = StdRng::seed_from_u64(0x5EED);
        let series = readings((0..2000).map(|_| generator.random::<f64>() - 0.5));

        let measured = autocorrelation(&series, 1).unwrap();
        assert!(
            measured.correlation.abs() < 2.0 * measured.standard_error,
            "{measured:?}"
        );
        let agreement = sign_agreement(&series, 1).unwrap();
        assert!(
            (agreement.rate - 0.5).abs() < 2.0 * agreement.standard_error,
            "{agreement:?}"
        );
    }

    /// A session that could not be measured is a hole, not a join. Closing it would pair readings
    /// two sessions apart and report the answer as a one-session step.
    #[test]
    fn test_a_gap_is_not_paired_across() {
        let series = vec![Some(1.0), None, Some(2.0), None, Some(3.0), None, Some(4.0)];
        assert_eq!(autocorrelation(&series, 1), None, "no adjacent pair exists");

        // Two sessions apart every pair straddles a hole, and those are real.
        let measured = autocorrelation(&series, 2).unwrap();
        assert_eq!(measured.pairs, 3);
    }

    /// Only the pairs that survive the holes count toward the error, or a series measured on a
    /// third of its sessions would claim the precision of one measured throughout.
    #[test]
    fn test_the_error_counts_the_pairs_and_not_the_sessions() {
        let mut series = readings((0..101).map(|index| index as f64));
        for index in (1..101).step_by(2) {
            series[index] = None;
        }

        let measured = autocorrelation(&series, 2).unwrap();
        assert_eq!(measured.pairs, 50);
        assert!(
            (measured.standard_error - 1.0 / 50.0_f64.sqrt()).abs() < 1e-12,
            "{measured:?}"
        );
    }

    /// A reading of exactly zero agrees with nothing. Counting it as positive would put a thumb on
    /// the scale in whichever direction `is_sign_positive` happens to answer.
    #[test]
    fn test_a_zero_reading_carries_no_sign() {
        let series = readings([0.1, 0.0, 0.1, 0.1, 0.1]);
        let agreement = sign_agreement(&series, 1).unwrap();
        assert_eq!(agreement.pairs, 2, "the two pairs the zero is not in");
        assert_eq!(agreement.rate, 1.0);
    }

    #[test]
    fn test_an_unusable_lag_or_series_is_refused() {
        let series = readings([0.1, 0.2, 0.3]);
        assert_eq!(autocorrelation(&series, 0), None);
        assert_eq!(autocorrelation(&series, 3), None);
        assert_eq!(autocorrelation(&series, 9), None);
        assert_eq!(sign_agreement(&series, 0), None);
        assert_eq!(sign_agreement(&series, 3), None);
        assert_eq!(autocorrelation(&[], 1), None);
        assert_eq!(autocorrelation(&[Some(f64::NAN); 20], 1), None);
    }
}
