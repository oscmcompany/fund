//! Trade formatting helpers used by the HTML renderer.

/// Formats a holding duration in seconds to a human-readable string.
pub fn format_holding_duration(seconds: i64) -> String {
    if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86400 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else {
        format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_holding_duration_minutes() {
        assert_eq!(format_holding_duration(300), "5m");
    }

    #[test]
    fn test_format_holding_duration_hours_and_minutes() {
        assert_eq!(format_holding_duration(3900), "1h 5m");
    }

    #[test]
    fn test_format_holding_duration_days_and_hours() {
        assert_eq!(format_holding_duration(90000), "1d 1h");
    }

    #[test]
    fn test_format_holding_duration_zero() {
        assert_eq!(format_holding_duration(0), "0m");
    }

    #[test]
    fn test_format_holding_duration_exactly_one_hour() {
        assert_eq!(format_holding_duration(3600), "1h 0m");
    }

    #[test]
    fn test_format_holding_duration_exactly_one_day() {
        assert_eq!(format_holding_duration(86400), "1d 0h");
    }
}
