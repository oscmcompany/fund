//! Event formatting helpers used by the HTML renderer.

/// Returns the CSS class for an event type based on its outcome suffix.
pub fn event_type_css_class(event_type: &str) -> &'static str {
    if event_type.ends_with("errored") {
        "event-errored"
    } else if event_type.ends_with("completed") {
        "event-completed"
    } else if event_type.ends_with("started") || event_type.ends_with("requested") {
        "event-started"
    } else {
        ""
    }
}

/// Truncates a JSON payload to a short summary string.
///
/// Truncation is performed on Unicode codepoint boundaries (not byte boundaries)
/// to avoid panicking on multibyte characters such as CJK, emoji, or accented text.
pub fn truncate_payload(payload: &serde_json::Value) -> String {
    let serialized = payload.to_string();
    if serialized.chars().count() > 58 {
        let truncated: String = serialized.chars().take(57).collect();
        format!("{truncated}\u{2026}")
    } else {
        serialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_css_class_errored() {
        assert_eq!(
            event_type_css_class("equity_bars_sync_errored"),
            "event-errored"
        );
    }

    #[test]
    fn test_event_type_css_class_completed() {
        assert_eq!(
            event_type_css_class("portfolio_rebalance_completed"),
            "event-completed"
        );
    }

    #[test]
    fn test_event_type_css_class_started() {
        assert_eq!(
            event_type_css_class("equity_bars_sync_started"),
            "event-started"
        );
    }

    #[test]
    fn test_event_type_css_class_unknown() {
        assert_eq!(event_type_css_class("something_else"), "");
    }

    #[test]
    fn test_truncate_payload_short_value() {
        let payload = serde_json::json!({"id": 1});
        let result = truncate_payload(&payload);
        assert!(!result.contains('\u{2026}'));
    }

    #[test]
    fn test_truncate_payload_long_value() {
        let payload = serde_json::json!({"key": "a very long value that exceeds the display limit for dashboard rendering"});
        let result = truncate_payload(&payload);
        assert!(result.contains('\u{2026}'));
    }

    #[test]
    fn test_truncate_payload_multibyte_does_not_panic() {
        let long_cjk = "\u{65E5}".repeat(60);
        let payload = serde_json::json!({"key": long_cjk});
        let result = truncate_payload(&payload);
        assert!(result.contains('\u{2026}'));
        assert!(result.is_char_boundary(result.len()));
    }
}
