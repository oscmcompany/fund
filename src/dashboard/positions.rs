//! Position formatting helpers used by the HTML renderer.

use chrono::{DateTime, Utc};

/// Age threshold in minutes above which no rebalance is flagged as stale (red).
const REBALANCE_STALE_MINUTES: i64 = 30;

/// Age threshold in minutes above which no rebalance triggers a warning (yellow).
const REBALANCE_WARNING_MINUTES: i64 = 10;

/// Formats a `Decimal` value as a dollar string with two decimal places (e.g. `"$1000.50"`).
///
/// Formats directly in decimal space to preserve cent-level precision without
/// converting through floating point.
pub fn format_dollars(decimal: rust_decimal::Decimal) -> String {
    format!("${:.2}", decimal)
}

/// Returns the CSS class for a rebalance age based on staleness thresholds.
///
/// `"stale"` when older than [`REBALANCE_STALE_MINUTES`], `"warning"` when older
/// than [`REBALANCE_WARNING_MINUTES`], `"fresh"` otherwise.
pub fn rebalance_age_css_class(completed_at: DateTime<Utc>) -> &'static str {
    let age = Utc::now() - completed_at;
    if age > chrono::Duration::minutes(REBALANCE_STALE_MINUTES) {
        "stale"
    } else if age > chrono::Duration::minutes(REBALANCE_WARNING_MINUTES) {
        "warning"
    } else {
        "fresh"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_decimal::Decimal;

    #[test]
    fn test_format_dollars_positive() {
        assert_eq!(format_dollars(Decimal::new(1000, 0)), "$1000.00");
    }

    #[test]
    fn test_format_dollars_zero() {
        assert_eq!(format_dollars(Decimal::ZERO), "$0.00");
    }

    #[test]
    fn test_format_dollars_fractional() {
        assert_eq!(format_dollars(Decimal::new(10050, 2)), "$100.50");
    }

    #[test]
    fn test_rebalance_age_css_class_fresh() {
        let completed_at = Utc::now() - Duration::minutes(3);
        assert_eq!(rebalance_age_css_class(completed_at), "fresh");
    }

    #[test]
    fn test_rebalance_age_css_class_warning() {
        let completed_at = Utc::now() - Duration::minutes(20);
        assert_eq!(rebalance_age_css_class(completed_at), "warning");
    }

    #[test]
    fn test_rebalance_age_css_class_stale() {
        let completed_at = Utc::now() - Duration::minutes(45);
        assert_eq!(rebalance_age_css_class(completed_at), "stale");
    }
}
