//! Model drift detection: compare a training run's CRPS against the baseline of recent prior runs.
//!
//! Drift is logged, never used to block an artifact upload.

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftStatus {
    InsufficientHistory,
    NoDrift,
    DriftDetected,
}

#[derive(Debug, Clone, Serialize)]
pub struct DriftResult {
    pub status: DriftStatus,
    pub message: String,
    pub current_continuous_ranked_probability_score: f64,
    pub baseline_continuous_ranked_probability_score: Option<f64>,
}

/// Check whether `current_continuous_ranked_probability_score` has degraded relative to the mean of
/// `prior_continuous_ranked_probability_scores` by more than `degradation_threshold` (a fraction, e.g. 0.20).
/// Fewer than `minimum_runs` prior values yields `InsufficientHistory`; the
/// baseline is floored at 1e-8 so a near-zero history cannot flag noise.
pub fn check_drift(
    current_continuous_ranked_probability_score: f64,
    prior_continuous_ranked_probability_scores: &[f64],
    minimum_runs: usize,
    degradation_threshold: f64,
) -> DriftResult {
    // The emptiness check is separate from the `minimum_runs` check on purpose. With
    // `minimum_runs` of 0 the comparison below is false for an empty slice, so the function would
    // continue and divide by zero: `baseline_continuous_ranked_probability_score` becomes NaN, `NaN.max(1e-8)` collapses the limit
    // to the floor so almost any current value reports drift, and the NaN then fails JSON
    // serialization at the call site.
    if prior_continuous_ranked_probability_scores.is_empty()
        || prior_continuous_ranked_probability_scores.len() < minimum_runs
    {
        let message = format!(
            "Insufficient evaluation history: {} run(s) recorded, {} required for baseline.",
            prior_continuous_ranked_probability_scores.len(),
            minimum_runs
        );
        return DriftResult {
            status: DriftStatus::InsufficientHistory,
            message,
            current_continuous_ranked_probability_score,
            baseline_continuous_ranked_probability_score: None,
        };
    }

    let baseline_continuous_ranked_probability_score = prior_continuous_ranked_probability_scores
        .iter()
        .sum::<f64>()
        / prior_continuous_ranked_probability_scores.len() as f64;
    let degradation_limit =
        baseline_continuous_ranked_probability_score.max(1e-8) * (1.0 + degradation_threshold);

    if current_continuous_ranked_probability_score > degradation_limit {
        let message = format!(
            "Drift detected: current CRPS {current_continuous_ranked_probability_score:.6} exceeds baseline \
             {baseline_continuous_ranked_probability_score:.6} by more than {:.0}%.",
            degradation_threshold * 100.0
        );
        return DriftResult {
            status: DriftStatus::DriftDetected,
            message,
            current_continuous_ranked_probability_score,
            baseline_continuous_ranked_probability_score: Some(
                baseline_continuous_ranked_probability_score,
            ),
        };
    }

    let message = format!(
        "No drift detected: current CRPS {current_continuous_ranked_probability_score:.6} is within {:.0}% of \
         baseline {baseline_continuous_ranked_probability_score:.6}.",
        degradation_threshold * 100.0
    );
    DriftResult {
        status: DriftStatus::NoDrift,
        message,
        current_continuous_ranked_probability_score,
        baseline_continuous_ranked_probability_score: Some(
            baseline_continuous_ranked_probability_score,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With `minimum_runs` of zero the length comparison passes for an empty slice, and the mean
    /// then divides by zero. The resulting NaN collapses the drift limit to its floor, so almost
    /// any current value reports drift, and it cannot be serialized to JSON afterwards.
    #[test]
    fn test_empty_history_is_insufficient_regardless_of_minimum_runs() {
        let result = check_drift(0.5, &[], 0, 0.2);
        assert_eq!(result.status, DriftStatus::InsufficientHistory);
        assert_eq!(result.baseline_continuous_ranked_probability_score, None);
    }

    #[test]
    fn test_insufficient_history_below_minimum_runs() {
        let result = check_drift(0.5, &[0.3, 0.3], 3, 0.20);
        assert_eq!(result.status, DriftStatus::InsufficientHistory);
        assert_eq!(result.baseline_continuous_ranked_probability_score, None);
        assert_eq!(result.current_continuous_ranked_probability_score, 0.5);
    }

    #[test]
    fn test_no_drift_at_exact_degradation_limit() {
        // baseline = 0.3, limit = 0.36. The check is strictly greater than, so a value
        // exactly at the limit is not drift.
        let result = check_drift(0.36, &[0.3, 0.3, 0.3], 3, 0.20);
        assert_eq!(result.status, DriftStatus::NoDrift);
        assert_eq!(
            result.baseline_continuous_ranked_probability_score,
            Some(0.3)
        );
    }

    #[test]
    fn test_drift_detected_just_over_limit() {
        let result = check_drift(0.361, &[0.3, 0.3, 0.3], 3, 0.20);
        assert_eq!(result.status, DriftStatus::DriftDetected);
        assert_eq!(
            result.baseline_continuous_ranked_probability_score,
            Some(0.3)
        );
        assert!(result.message.contains("Drift detected"));
    }

    #[test]
    fn test_baseline_uses_mean_of_priors() {
        // mean(0.2, 0.3, 0.4) = 0.3 -> limit 0.36.
        let result = check_drift(0.35, &[0.2, 0.3, 0.4], 3, 0.20);
        assert_eq!(result.status, DriftStatus::NoDrift);
        assert!((result.baseline_continuous_ranked_probability_score.unwrap() - 0.3).abs() < 1e-12);
    }

    #[test]
    fn test_near_zero_baseline_is_floored() {
        // baseline mean is ~0, but the floor of 1e-8 keeps the limit positive,
        // so a tiny current value does not flag drift.
        let result = check_drift(1e-9, &[0.0, 0.0, 0.0], 3, 0.20);
        assert_eq!(result.status, DriftStatus::NoDrift);
    }

    #[test]
    fn test_status_serializes_to_snake_case() {
        assert_eq!(
            serde_json::to_value(DriftStatus::InsufficientHistory).unwrap(),
            "insufficient_history"
        );
        assert_eq!(
            serde_json::to_value(DriftStatus::NoDrift).unwrap(),
            "no_drift"
        );
        assert_eq!(
            serde_json::to_value(DriftStatus::DriftDetected).unwrap(),
            "drift_detected"
        );
    }
}
