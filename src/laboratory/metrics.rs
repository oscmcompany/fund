//! Forecast quality, measured across one session's names and then across sessions.
//!
//! Cross-sectional throughout: pooling sessions measures time variation a neutral book cannot trade.

use serde::Serialize;

/// Names a cross-section needs before a decile means anything.
pub const MINIMUM_CROSS_SECTION: usize = 10;

/// What one session's forecasts were worth.
///
/// Every field is optional because a degenerate cross-section has no answer rather than a zero —
/// scores that are all equal cannot rank, and a rank correlation over them is undefined, not nil.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize)]
pub struct SessionMetrics {
    pub information_coefficient: Option<f64>,
    pub decile_spread: Option<f64>,
    pub directional_accuracy: Option<f64>,
}

/// A statistic's mean across sessions and how well that mean is pinned down.
///
/// Reported together because an information coefficient without its standard error cannot be told
/// apart from noise: across a few hundred sessions, 0.01 and zero are the same claim.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Distribution {
    pub mean: f64,
    pub standard_error: f64,
    pub sessions: usize,
}

/// Measures one session's cross-section.
pub fn measure_session(scores: &[f64], realized: &[f64]) -> SessionMetrics {
    SessionMetrics {
        information_coefficient: information_coefficient(scores, realized),
        decile_spread: decile_spread(scores, realized),
        directional_accuracy: directional_accuracy(scores, realized),
    }
}

/// Rank correlation between the forecast order and the realized order, over one session.
///
/// Ranks rather than values because the book acts on the ordering: a forecast that gets every
/// magnitude wrong and every ordering right is worth everything, and the reverse is worth nothing.
pub fn information_coefficient(scores: &[f64], realized: &[f64]) -> Option<f64> {
    if scores.len() != realized.len() {
        return None;
    }
    pearson_correlation(&average_ranks(scores)?, &average_ranks(realized)?)
}

/// Mean realized return of the best-scored tenth less than that of the worst-scored tenth.
///
/// The rank correlation says whether the ordering is right; this says whether acting on it pays,
/// which is not the same question when the signal lives only in the extremes.
pub fn decile_spread(scores: &[f64], realized: &[f64]) -> Option<f64> {
    if scores.len() != realized.len() || scores.len() < MINIMUM_CROSS_SECTION {
        return None;
    }
    if !finite(scores) || !finite(realized) {
        return None;
    }

    let mut order: Vec<usize> = (0..scores.len()).collect();
    order.sort_by(|left, right| {
        scores[*left]
            .partial_cmp(&scores[*right])
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.cmp(right))
    });

    let decile = scores.len() / 10;
    // Both boundaries have to fall between distinct scores. A name tied with the one just outside
    // its tenth is in it only because of where the sort put it, and a cross-section of equal scores
    // would otherwise report the gap between two alphabetical slices as forecast performance.
    let boundary_is_arbitrary = |edge: usize| scores[order[edge]] == scores[order[edge + 1]];
    if boundary_is_arbitrary(decile - 1) || boundary_is_arbitrary(order.len() - decile - 1) {
        return None;
    }

    let bottom: f64 = order[..decile].iter().map(|index| realized[*index]).sum();
    let top: f64 = order[order.len() - decile..]
        .iter()
        .map(|index| realized[*index])
        .sum();
    Some((top - bottom) / decile as f64)
}

/// Share of names whose forecast sign matched the realized sign.
///
/// Only meaningful where the score carries return units; a ranking score has no sign to agree with.
/// Names where either side is exactly zero have no direction and are left out.
pub fn directional_accuracy(scores: &[f64], realized: &[f64]) -> Option<f64> {
    if scores.len() != realized.len() || !finite(scores) || !finite(realized) {
        return None;
    }
    let directional: Vec<bool> = scores
        .iter()
        .zip(realized)
        .filter(|(score, outcome)| **score != 0.0 && **outcome != 0.0)
        .map(|(score, outcome)| score.is_sign_positive() == outcome.is_sign_positive())
        .collect();
    if directional.is_empty() {
        return None;
    }
    Some(directional.iter().filter(|agreed| **agreed).count() as f64 / directional.len() as f64)
}

/// Mean and standard error of one statistic over the sessions that produced it.
///
/// Sessions without a value are skipped rather than counted as zero: a session whose scores could
/// not rank is one the measurement says nothing about, and averaging a zero in asserts otherwise.
pub fn summarize(values: impl IntoIterator<Item = Option<f64>>) -> Option<Distribution> {
    let observed: Vec<f64> = values
        .into_iter()
        .flatten()
        .filter(|v| v.is_finite())
        .collect();
    if observed.len() < 2 {
        return None;
    }
    let sessions = observed.len();
    let mean = observed.iter().sum::<f64>() / sessions as f64;
    let variance = observed
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (sessions - 1) as f64;
    Some(Distribution {
        mean,
        standard_error: variance.sqrt() / (sessions as f64).sqrt(),
        sessions,
    })
}

/// Ranks smallest to largest, giving tied values the average of the ranks they span.
///
/// Ties have to share a rank or a cross-section full of equal scores would rank as if it were
/// ordered, and the correlation would report structure that is not there.
fn average_ranks(values: &[f64]) -> Option<Vec<f64>> {
    if values.is_empty() || !finite(values) {
        return None;
    }
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|left, right| {
        values[*left]
            .partial_cmp(&values[*right])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut ranks = vec![0.0; values.len()];
    let mut start = 0;
    while start < order.len() {
        let mut end = start;
        while end + 1 < order.len() && values[order[end + 1]] == values[order[start]] {
            end += 1;
        }
        let shared = (start + end) as f64 / 2.0 + 1.0;
        for position in &order[start..=end] {
            ranks[*position] = shared;
        }
        start = end + 1;
    }
    Some(ranks)
}

/// Correlation of two equal-length series, or `None` where either cannot vary.
fn pearson_correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.len() < 2 {
        return None;
    }
    let left_mean = left.iter().sum::<f64>() / left.len() as f64;
    let right_mean = right.iter().sum::<f64>() / right.len() as f64;

    let mut covariance = 0.0;
    let mut left_variance = 0.0;
    let mut right_variance = 0.0;
    for (first, second) in left.iter().zip(right) {
        let left_deviation = first - left_mean;
        let right_deviation = second - right_mean;
        covariance += left_deviation * right_deviation;
        left_variance += left_deviation.powi(2);
        right_variance += right_deviation.powi(2);
    }

    // A cross-section that cannot vary cannot rank, which is a different answer from no correlation.
    let denominator = (left_variance * right_variance).sqrt();
    (denominator > 0.0).then(|| covariance / denominator)
}

fn finite(values: &[f64]) -> bool {
    values.iter().all(|value| value.is_finite())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_perfectly_ordered_forecast_scores_one() {
        let scores = [1.0, 2.0, 3.0, 4.0];
        let realized = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(information_coefficient(&scores, &realized), Some(1.0));
    }

    #[test]
    fn test_a_perfectly_inverted_forecast_scores_minus_one() {
        let scores = [1.0, 2.0, 3.0, 4.0];
        let realized = [40.0, 30.0, 20.0, 10.0];
        assert_eq!(information_coefficient(&scores, &realized), Some(-1.0));
    }

    /// Ranks [1,2,3,4] against [1,3,2,4]: one adjacent transposition out of four names.
    #[test]
    fn test_one_transposition_scores_its_hand_computed_value() {
        let scores = [1.0, 2.0, 3.0, 4.0];
        let realized = [10.0, 30.0, 20.0, 40.0];
        let measured = information_coefficient(&scores, &realized).unwrap();
        assert!(
            (measured - 0.8).abs() < 1e-12,
            "expected 0.8, measured {measured}"
        );
    }

    /// Two tied scores share rank 1.5, so the ranks are [1.5, 1.5, 3, 4] against [1, 2, 3, 4].
    /// Without the tie correction the ranks would read [1, 2, 3, 4] and the answer would be 1.0.
    #[test]
    fn test_tied_scores_share_a_rank() {
        let scores = [1.0, 1.0, 2.0, 3.0];
        let realized = [10.0, 20.0, 30.0, 40.0];
        let measured = information_coefficient(&scores, &realized).unwrap();
        assert!(
            (measured - 0.948_683_298_050_513_8).abs() < 1e-12,
            "expected sqrt(0.9), measured {measured}"
        );
    }

    /// The signature of a forecast that cannot rank, and the reason every field is optional: this
    /// is what a cross-sectional-mean baseline produces, and what TiDE produces if it has learned
    /// only the drift.
    #[test]
    fn test_a_cross_section_of_equal_scores_has_no_coefficient() {
        let scores = [5.0, 5.0, 5.0, 5.0];
        let realized = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(information_coefficient(&scores, &realized), None);
    }

    /// Two sides of different lengths are not a cross-section, so nothing is ranked.
    #[test]
    fn test_sides_of_different_lengths_are_refused() {
        assert_eq!(information_coefficient(&[1.0, 2.0], &[1.0, 2.0, 3.0]), None);
        assert_eq!(decile_spread(&[1.0, 2.0], &[1.0, 2.0, 3.0]), None);
        assert_eq!(directional_accuracy(&[1.0, 2.0], &[1.0, 2.0, 3.0]), None);
        assert_eq!(
            measure_session(&[1.0, 2.0], &[1.0, 2.0, 3.0]),
            SessionMetrics::default()
        );
    }

    #[test]
    fn test_a_non_finite_reading_is_refused_rather_than_ranked() {
        let realized = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(
            information_coefficient(&[1.0, f64::NAN, 3.0, 4.0], &realized),
            None
        );
        assert_eq!(
            information_coefficient(&[1.0, f64::INFINITY, 3.0, 4.0], &realized),
            None
        );
    }

    /// Twenty names split into deciles of two: the top two realize 19 and 20, the bottom two 1 and
    /// 2, so the spread is 19.5 - 1.5.
    #[test]
    fn test_the_decile_spread_is_the_gap_between_the_extreme_tenths() {
        let scores: Vec<f64> = (1..=20).map(f64::from).collect();
        let realized = scores.clone();
        assert_eq!(decile_spread(&scores, &realized), Some(18.0));
    }

    /// The same failure the rank correlation already refuses. Equal scores leave the sort to break
    /// ties by position, so the two tenths are alphabetical slices of an unordered set and the gap
    /// between their outcomes is not forecast performance.
    #[test]
    fn test_a_cross_section_of_equal_scores_has_no_spread() {
        let scores = [1.0_f64; 20];
        let realized: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(decile_spread(&scores, &realized), None);
    }

    /// A tie straddling a boundary is arbitrary for the same reason, even though the rest of the
    /// cross-section is ordered: which of the equal names lands in the tenth is down to the sort.
    #[test]
    fn test_a_tie_across_a_decile_boundary_has_no_spread() {
        // Twenty names, deciles of two. The two lowest scores tie with the third.
        let mut scores: Vec<f64> = (1..=20).map(f64::from).collect();
        scores[2] = scores[1];
        let realized: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(decile_spread(&scores, &realized), None);
    }

    #[test]
    fn test_a_cross_section_too_small_to_have_a_decile_has_no_spread() {
        let scores: Vec<f64> = (1..=9).map(f64::from).collect();
        let realized = scores.clone();
        assert_eq!(decile_spread(&scores, &realized), None);
    }

    #[test]
    fn test_directional_accuracy_counts_sign_agreement() {
        let scores = [1.0, -1.0, 1.0, -1.0];
        let realized = [2.0, -2.0, -2.0, 2.0];
        assert_eq!(directional_accuracy(&scores, &realized), Some(0.5));
    }

    /// A zero has no direction to agree with, so it is left out rather than counted as a miss.
    #[test]
    fn test_a_zero_on_either_side_is_not_a_direction() {
        assert_eq!(
            directional_accuracy(&[1.0, 0.0, 1.0], &[1.0, 1.0, 1.0]),
            Some(1.0)
        );
        assert_eq!(directional_accuracy(&[0.0, 0.0], &[1.0, 1.0]), None);
    }

    /// Mean 0.2 with a sample standard deviation of 0.1 over three sessions, so the standard error
    /// is 0.1/sqrt(3).
    #[test]
    fn test_a_summary_reports_the_mean_and_its_standard_error() {
        let summary = summarize([Some(0.1), Some(0.2), Some(0.3)]).unwrap();
        assert_eq!(summary.sessions, 3);
        assert!((summary.mean - 0.2).abs() < 1e-12, "mean {}", summary.mean);
        assert!(
            (summary.standard_error - 0.057_735_026_918_962_58).abs() < 1e-12,
            "standard error {}",
            summary.standard_error
        );
    }

    /// A session that could not rank is skipped, not counted as zero — averaging a zero in would
    /// claim the forecast was measured and found worthless there.
    #[test]
    fn test_sessions_without_a_value_are_skipped_not_zeroed() {
        let summary = summarize([Some(0.1), None, Some(0.3), None]).unwrap();
        assert_eq!(summary.sessions, 2);
        assert!((summary.mean - 0.2).abs() < 1e-12, "mean {}", summary.mean);
    }

    #[test]
    fn test_one_session_is_not_a_distribution() {
        assert_eq!(summarize([Some(0.1)]), None);
        assert_eq!(summarize([None, None]), None);
    }
}
