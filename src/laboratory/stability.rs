//! Whether a per-session reading is anticipated by something `lag` sessions before it.
//!
//! The cross-sectional statistics say nothing about time; this asks what a reading follows from.

use serde::Serialize;

use crate::laboratory::metrics::pearson_correlation;

/// Lags reported by default. Ten sessions is two trading weeks, far enough for a decay to show.
pub const DEFAULT_LAGS: usize = 10;

/// How a session's reading relates to a value `lag` sessions before it.
///
/// The error is taken under the null that the two are unrelated, which is the hypothesis being
/// tested — so a correlation inside twice its error is nothing.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Association {
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

/// Pairs each session's value in `earlier` with the value `lag` sessions later in `later`.
///
/// A session either side could not measure breaks the run rather than closing it: pairing across
/// the gap would call a longer step a shorter one, which is what session contiguity already forbids
/// on the other side of the measurement.
fn paired_by_lag(
    earlier: &[Option<f64>],
    later: &[Option<f64>],
    lag: usize,
) -> (Vec<f64>, Vec<f64>) {
    if earlier.len() != later.len() || lag >= later.len() {
        return (Vec::new(), Vec::new());
    }
    earlier
        .iter()
        .zip(&later[lag..])
        .filter_map(|(before, after)| before.zip(*after))
        .filter(|(before, after)| before.is_finite() && after.is_finite())
        .unzip()
}

/// Correlation between each session's reading and a value `lag` sessions before it.
///
/// At a lag of zero the two describe the same session, which explains a reading without being able
/// to anticipate one — only a positive lag names something known before the session it speaks about.
pub fn association(
    earlier: &[Option<f64>],
    readings: &[Option<f64>],
    lag: usize,
) -> Option<Association> {
    let (before, after) = paired_by_lag(earlier, readings, lag);
    let correlation = pearson_correlation(&before, &after)?;
    Some(Association {
        lag,
        correlation,
        standard_error: 1.0 / (before.len() as f64).sqrt(),
        pairs: before.len(),
    })
}

/// Correlation between each session's reading and the reading `lag` sessions later.
///
/// A series against itself, so a lag of zero is refused: every series correlates with itself
/// perfectly and reports nothing.
pub fn autocorrelation(values: &[Option<f64>], lag: usize) -> Option<Association> {
    if lag == 0 {
        return None;
    }
    association(values, values, lag)
}

/// Share of pairs whose two readings share a sign.
///
/// The blunt form of the same question and the one a book acts on: above a half is a relationship
/// that held, below is one that flipped. A reading of exactly zero points nowhere and takes its
/// pair with it.
pub fn sign_agreement(values: &[Option<f64>], lag: usize) -> Option<SignAgreement> {
    if lag == 0 {
        return None;
    }
    let (current, later) = paired_by_lag(values, values, lag);
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

    /// The lag runs from the state to the reading, not the other way. Reversed, the measurement
    /// would report a reading explained by a session that had not happened yet and read as a gate.
    #[test]
    fn test_the_lag_reaches_back_from_the_reading_to_the_state() {
        // Scrambled rather than counting, so a shift of one is not still a straight line.
        let state = readings((0..200).map(|index| ((index * 37) % 200) as f64));
        let mut following = vec![None];
        following.extend(state.iter().take(199).copied());

        // Each reading is the state of the session before it, so a lag of one is exact.
        let anticipated = association(&state, &following, 1).unwrap();
        assert!(
            (anticipated.correlation - 1.0).abs() < 1e-9,
            "{anticipated:?}"
        );

        // And the same session says nothing, which is what makes the two columns worth printing.
        let same = association(&state, &following, 0).unwrap();
        assert!(same.correlation.abs() < 0.2, "{same:?}");
    }

    /// A state describing the session it is read against is a legitimate question — it explains
    /// without anticipating — so unlike an autocorrelation it must not be refused.
    #[test]
    fn test_a_state_may_describe_the_session_it_explains() {
        let state = readings((0..100).map(|index| index as f64));
        let doubled = readings((0..100).map(|index| 2.0 * index as f64));

        let same = association(&state, &doubled, 0).unwrap();
        assert!((same.correlation - 1.0).abs() < 1e-9, "{same:?}");
        assert_eq!(same.pairs, 100);

        // But a series against itself at no lag is the trivial answer, and is refused.
        assert_eq!(autocorrelation(&state, 0), None);
        assert_eq!(sign_agreement(&state, 0), None);
    }

    /// Two series measured over different windows cannot be aligned by index, and quietly pairing
    /// the overlap would put each reading against the wrong session's state.
    #[test]
    fn test_series_of_different_lengths_are_refused() {
        let state = readings((0..100).map(|index| index as f64));
        let shorter = readings((0..40).map(|index| index as f64));
        assert_eq!(association(&state, &shorter, 0), None);
        assert_eq!(association(&shorter, &state, 1), None);
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
