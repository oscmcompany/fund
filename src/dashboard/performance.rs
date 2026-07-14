//! Performance formatting helpers used by the HTML renderer.

/// Formats an optional return percentage with sign and two decimal places.
pub fn format_return(value: Option<f64>) -> String {
    match value {
        Some(return_value) => format!("{:+.2}%", return_value),
        None => "\u{2014}".to_string(),
    }
}

/// Returns the CSS class for a return value: `"positive"`, `"negative"`, or `"muted"`.
pub fn return_css_class(value: Option<f64>) -> &'static str {
    match value {
        Some(return_value) if return_value > 0.0 => "positive",
        Some(return_value) if return_value < 0.0 => "negative",
        Some(_) => "",
        None => "muted",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_return_positive() {
        assert_eq!(format_return(Some(5.25)), "+5.25%");
    }

    #[test]
    fn test_format_return_negative() {
        assert_eq!(format_return(Some(-3.10)), "-3.10%");
    }

    #[test]
    fn test_format_return_none() {
        assert_eq!(format_return(None), "\u{2014}");
    }

    #[test]
    fn test_return_css_class_positive() {
        assert_eq!(return_css_class(Some(1.0)), "positive");
    }

    #[test]
    fn test_return_css_class_negative() {
        assert_eq!(return_css_class(Some(-1.0)), "negative");
    }

    #[test]
    fn test_return_css_class_none() {
        assert_eq!(return_css_class(None), "muted");
    }

    #[test]
    fn test_return_css_class_zero() {
        assert_eq!(return_css_class(Some(0.0)), "");
    }
}
