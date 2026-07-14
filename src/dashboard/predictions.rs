//! Prediction formatting helpers used by the HTML renderer.

use chrono::Utc;

use crate::dashboard::cache::ModelRunInformation;

/// Age threshold in hours above which model run data is considered stale.
const MODEL_RUN_STALE_HOURS: i64 = 36;

/// Age threshold in hours above which model run data is approaching stale (warning).
const MODEL_RUN_WARNING_HOURS: i64 = 25;

/// Formats the age of a timestamp as a human-readable string.
///
/// Returns `"0m"` when the timestamp is in the future (e.g. due to clock skew
/// between the database host and the dashboard host) to avoid rendering negative ages.
pub fn format_age(timestamp: chrono::DateTime<Utc>) -> String {
    let age = Utc::now() - timestamp;
    if age < chrono::Duration::zero() {
        return "0m".to_string();
    }
    if age.num_days() > 0 {
        format!("{}d {}h", age.num_days(), age.num_hours() % 24)
    } else if age.num_hours() > 0 {
        format!("{}h {}m", age.num_hours(), age.num_minutes() % 60)
    } else {
        format!("{}m", age.num_minutes())
    }
}

/// Returns the CSS class for a model run age based on staleness thresholds.
///
/// `"stale"` when older than [`MODEL_RUN_STALE_HOURS`], `"warning"` when older
/// than [`MODEL_RUN_WARNING_HOURS`], `"fresh"` otherwise.
pub fn model_run_age_css_class(info: &ModelRunInformation) -> &'static str {
    let age = Utc::now() - info.completed_at();
    if age > chrono::Duration::hours(MODEL_RUN_STALE_HOURS) {
        "stale"
    } else if age > chrono::Duration::hours(MODEL_RUN_WARNING_HOURS) {
        "warning"
    } else {
        "fresh"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate};

    #[test]
    fn test_format_age_minutes() {
        let timestamp = Utc::now() - Duration::minutes(45);
        assert_eq!(format_age(timestamp), "45m");
    }

    #[test]
    fn test_format_age_hours_and_minutes() {
        let timestamp = Utc::now() - Duration::hours(3) - Duration::minutes(20);
        let result = format_age(timestamp);
        assert!(
            result.starts_with("3h"),
            "expected '3h ...', got '{result}'"
        );
    }

    #[test]
    fn test_format_age_days_and_hours() {
        let timestamp = Utc::now() - Duration::days(2) - Duration::hours(5);
        let result = format_age(timestamp);
        assert!(
            result.starts_with("2d"),
            "expected '2d ...', got '{result}'"
        );
    }

    #[test]
    fn test_format_age_future_timestamp_returns_zero() {
        let timestamp = Utc::now() + Duration::minutes(5);
        assert_eq!(format_age(timestamp), "0m");
    }

    fn make_model_run_information(hours_ago: i64) -> ModelRunInformation {
        ModelRunInformation::new(
            Utc::now() - Duration::hours(hours_ago),
            Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
            Some(0.123),
            Some(0.725),
        )
        .unwrap()
    }

    #[test]
    fn test_model_run_age_css_class_fresh() {
        let info = make_model_run_information(1);
        assert_eq!(model_run_age_css_class(&info), "fresh");
    }

    #[test]
    fn test_model_run_age_css_class_warning() {
        let info = make_model_run_information(30);
        assert_eq!(model_run_age_css_class(&info), "warning");
    }

    #[test]
    fn test_model_run_age_css_class_stale() {
        let info = make_model_run_information(40);
        assert_eq!(model_run_age_css_class(&info), "stale");
    }
}
