//! Model drift detection: compare a training run's CRPS against the baseline
//! of recent prior runs. A direct port of the retired Python trainer's
//! `check_drift` — drift is reported (logged and recorded in `model_runs`),
//! never used to block an artifact upload.

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
    pub current_crps: f64,
    pub baseline_crps: Option<f64>,
}

/// Check whether `current_crps` has degraded relative to the mean of
/// `prior_crps` by more than `degradation_threshold` (a fraction, e.g. 0.20).
/// Fewer than `minimum_runs` prior values yields `InsufficientHistory`; the
/// baseline is floored at 1e-8 so a near-zero history cannot flag noise.
pub fn check_drift(
    current_crps: f64,
    prior_crps: &[f64],
    minimum_runs: usize,
    degradation_threshold: f64,
) -> DriftResult {
    if prior_crps.len() < minimum_runs {
        let message = format!(
            "Insufficient evaluation history: {} run(s) recorded, {} required for baseline.",
            prior_crps.len(),
            minimum_runs
        );
        return DriftResult {
            status: DriftStatus::InsufficientHistory,
            message,
            current_crps,
            baseline_crps: None,
        };
    }

    let baseline_crps = prior_crps.iter().sum::<f64>() / prior_crps.len() as f64;
    let degradation_limit = baseline_crps.max(1e-8) * (1.0 + degradation_threshold);

    if current_crps > degradation_limit {
        let message = format!(
            "Drift detected: current CRPS {current_crps:.6} exceeds baseline \
             {baseline_crps:.6} by more than {:.0}%.",
            degradation_threshold * 100.0
        );
        return DriftResult {
            status: DriftStatus::DriftDetected,
            message,
            current_crps,
            baseline_crps: Some(baseline_crps),
        };
    }

    let message = format!(
        "No drift detected: current CRPS {current_crps:.6} is within {:.0}% of \
         baseline {baseline_crps:.6}.",
        degradation_threshold * 100.0
    );
    DriftResult {
        status: DriftStatus::NoDrift,
        message,
        current_crps,
        baseline_crps: Some(baseline_crps),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insufficient_history_below_minimum_runs() {
        let result = check_drift(0.5, &[0.3, 0.3], 3, 0.20);
        assert_eq!(result.status, DriftStatus::InsufficientHistory);
        assert_eq!(result.baseline_crps, None);
        assert_eq!(result.current_crps, 0.5);
    }

    #[test]
    fn test_no_drift_at_exact_degradation_limit() {
        // baseline = 0.3, limit = 0.36; the Python check is strictly greater
        // than, so a value exactly at the limit is not drift.
        let result = check_drift(0.36, &[0.3, 0.3, 0.3], 3, 0.20);
        assert_eq!(result.status, DriftStatus::NoDrift);
        assert_eq!(result.baseline_crps, Some(0.3));
    }

    #[test]
    fn test_drift_detected_just_over_limit() {
        let result = check_drift(0.361, &[0.3, 0.3, 0.3], 3, 0.20);
        assert_eq!(result.status, DriftStatus::DriftDetected);
        assert_eq!(result.baseline_crps, Some(0.3));
        assert!(result.message.contains("Drift detected"));
    }

    #[test]
    fn test_baseline_uses_mean_of_priors() {
        // mean(0.2, 0.3, 0.4) = 0.3 -> limit 0.36.
        let result = check_drift(0.35, &[0.2, 0.3, 0.4], 3, 0.20);
        assert_eq!(result.status, DriftStatus::NoDrift);
        assert!((result.baseline_crps.unwrap() - 0.3).abs() < 1e-12);
    }

    #[test]
    fn test_near_zero_baseline_is_floored() {
        // baseline mean is ~0, but the floor of 1e-8 keeps the limit positive,
        // so a tiny current value does not flag drift.
        let result = check_drift(1e-9, &[0.0, 0.0, 0.0], 3, 0.20);
        assert_eq!(result.status, DriftStatus::NoDrift);
    }

    #[test]
    fn test_status_serializes_to_python_strings() {
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
