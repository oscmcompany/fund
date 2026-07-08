//! US equity market session helpers.
//!
//! Provides timezone-aware checks for whether the current time falls within
//! the regular US equity trading session (09:30–16:00 Eastern) or the wider
//! quote-stream window used by `data_manager` to capture quotes around open
//! and close.

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::US::Eastern;

/// Returns `true` when `now` falls within the regular US equity trading session
/// (09:30–16:00 Eastern, weekdays only). DST-safe.
pub fn is_within_trading_session_at(now: DateTime<Utc>) -> bool {
    is_weekday_minutes_in_range(now, 9 * 60 + 30, 16 * 60)
}

/// Returns `true` when the market is currently open.
pub fn is_within_trading_session() -> bool {
    is_within_trading_session_at(Utc::now())
}

/// Returns `true` when `now` falls within the quote-stream window
/// (09:25–16:05 Eastern, weekdays only). DST-safe.
///
/// The window opens 5 minutes before market open so quotes are already flowing
/// when the first rebalance cycle runs, and closes 5 minutes after market close
/// to capture any late fills and the liquidation event.
pub fn is_within_quote_stream_window_at(now: DateTime<Utc>) -> bool {
    is_weekday_minutes_in_range(now, 9 * 60 + 25, 16 * 60 + 5)
}

/// Returns `true` when the current time is within the quote-stream window.
pub fn is_within_quote_stream_window() -> bool {
    is_within_quote_stream_window_at(Utc::now())
}

/// Checks whether `now` is a weekday in Eastern Time and the time-of-day
/// (in minutes since midnight) falls within `[start_minutes, end_minutes)`.
fn is_weekday_minutes_in_range(now: DateTime<Utc>, start_minutes: u32, end_minutes: u32) -> bool {
    let eastern = now.with_timezone(&Eastern);
    if matches!(eastern.weekday(), Weekday::Sat | Weekday::Sun) {
        return false;
    }
    let minutes = eastern.hour() * 60 + eastern.minute();
    (start_minutes..end_minutes).contains(&minutes)
}

/// Returns the duration from `now` until the next quote-stream window opens
/// (09:25 Eastern on the next weekday). Returns `Duration::ZERO` if the window
/// is already open.
pub fn duration_until_quote_stream_window(now: DateTime<Utc>) -> std::time::Duration {
    use chrono::Duration as ChronoDuration;

    if is_within_quote_stream_window_at(now) {
        return std::time::Duration::ZERO;
    }

    let eastern = now.with_timezone(&Eastern);

    // Target: 09:25 Eastern today or the next weekday.
    let target_today = eastern
        .date_naive()
        .and_hms_opt(9, 25, 0)
        .expect("09:25:00 is a valid time");
    let target_today = Eastern
        .from_local_datetime(&target_today)
        .single()
        .expect("09:25 Eastern is unambiguous");

    let mut target = target_today;

    // If we're past the window close today (or it's a weekend), advance to next weekday.
    if eastern >= target_today && !is_within_quote_stream_window_at(now) {
        target += ChronoDuration::days(1);
    }

    // Skip weekends.
    while matches!(target.weekday(), Weekday::Sat | Weekday::Sun) {
        target += ChronoDuration::days(1);
    }

    let delta = target.signed_duration_since(eastern);
    if delta <= ChronoDuration::zero() {
        std::time::Duration::ZERO
    } else {
        delta.to_std().unwrap_or(std::time::Duration::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn utc(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    // --- is_within_trading_session_at ---

    #[test]
    fn test_trading_session_open_during_edt() {
        // 2024-07-15 14:00 UTC = 10:00 EDT (UTC-4): open.
        assert!(is_within_trading_session_at(utc("2024-07-15T14:00:00Z")));
    }

    #[test]
    fn test_trading_session_closed_after_close_est() {
        // 2024-01-15 21:30 UTC = 16:30 EST (UTC-5): closed.
        assert!(!is_within_trading_session_at(utc("2024-01-15T21:30:00Z")));
    }

    #[test]
    fn test_trading_session_closed_on_weekend() {
        // 2024-07-13 15:00 UTC = Saturday 11:00 EDT: closed.
        assert!(!is_within_trading_session_at(utc("2024-07-13T15:00:00Z")));
    }

    #[test]
    fn test_trading_session_closed_before_open() {
        // 2024-07-15 13:00 UTC = 09:00 EDT: before 09:30 open.
        assert!(!is_within_trading_session_at(utc("2024-07-15T13:00:00Z")));
    }

    #[test]
    fn test_trading_session_exact_open() {
        // 2024-07-15 13:30 UTC = 09:30 EDT: exactly at open.
        assert!(is_within_trading_session_at(utc("2024-07-15T13:30:00Z")));
    }

    #[test]
    fn test_trading_session_exact_close() {
        // 2024-07-15 20:00 UTC = 16:00 EDT: exactly at close (exclusive).
        assert!(!is_within_trading_session_at(utc("2024-07-15T20:00:00Z")));
    }

    // --- is_within_quote_stream_window_at ---

    #[test]
    fn test_quote_window_open_before_market_open() {
        // 2024-07-15 13:26 UTC = 09:26 EDT: within quote window but before trading.
        assert!(is_within_quote_stream_window_at(utc(
            "2024-07-15T13:26:00Z"
        )));
        assert!(!is_within_trading_session_at(utc("2024-07-15T13:26:00Z")));
    }

    #[test]
    fn test_quote_window_open_after_market_close() {
        // 2024-07-15 20:03 UTC = 16:03 EDT: within quote window but after trading.
        assert!(is_within_quote_stream_window_at(utc(
            "2024-07-15T20:03:00Z"
        )));
        assert!(!is_within_trading_session_at(utc("2024-07-15T20:03:00Z")));
    }

    #[test]
    fn test_quote_window_closed_before_window() {
        // 2024-07-15 13:24 UTC = 09:24 EDT: one minute before quote window.
        assert!(!is_within_quote_stream_window_at(utc(
            "2024-07-15T13:24:00Z"
        )));
    }

    #[test]
    fn test_quote_window_closed_after_window() {
        // 2024-07-15 20:06 UTC = 16:06 EDT: one minute after quote window.
        assert!(!is_within_quote_stream_window_at(utc(
            "2024-07-15T20:06:00Z"
        )));
    }

    #[test]
    fn test_quote_window_closed_on_weekend() {
        // 2024-07-13 14:00 UTC = Saturday 10:00 EDT: weekend.
        assert!(!is_within_quote_stream_window_at(utc(
            "2024-07-13T14:00:00Z"
        )));
    }

    // --- duration_until_quote_stream_window ---

    #[test]
    fn test_duration_zero_when_window_open() {
        // Already inside the window.
        let now = utc("2024-07-15T14:00:00Z"); // 10:00 EDT
        assert_eq!(
            duration_until_quote_stream_window(now),
            std::time::Duration::ZERO
        );
    }

    #[test]
    fn test_duration_before_window_same_day() {
        // 2024-07-15 12:00 UTC = 08:00 EDT: 1h25m until 09:25 EDT.
        let now = utc("2024-07-15T12:00:00Z");
        let duration = duration_until_quote_stream_window(now);
        assert_eq!(duration.as_secs(), 85 * 60);
    }

    #[test]
    fn test_duration_after_window_advances_to_next_day() {
        // 2024-07-15 21:00 UTC = 17:00 EDT: past close, should advance to 2024-07-16 09:25 EDT.
        let now = utc("2024-07-15T21:00:00Z");
        let duration = duration_until_quote_stream_window(now);
        // 09:25 EDT = 13:25 UTC on 2024-07-16, so ~16h25m from 21:00 UTC.
        assert_eq!(duration.as_secs(), 16 * 3600 + 25 * 60);
    }

    #[test]
    fn test_duration_friday_evening_skips_weekend() {
        // 2024-07-12 21:00 UTC = Friday 17:00 EDT: should skip Sat+Sun to Mon 09:25 EDT.
        let now = utc("2024-07-12T21:00:00Z");
        let duration = duration_until_quote_stream_window(now);
        // Mon 2024-07-15 09:25 EDT = 13:25 UTC. From Fri 21:00 UTC = 2d16h25m.
        let expected = 2 * 24 * 3600 + 16 * 3600 + 25 * 60;
        assert_eq!(duration.as_secs(), expected);
    }

    #[test]
    fn test_duration_saturday_skips_to_monday() {
        // 2024-07-13 15:00 UTC = Saturday 11:00 EDT.
        let now = utc("2024-07-13T15:00:00Z");
        let duration = duration_until_quote_stream_window(now);
        // Mon 2024-07-15 09:25 EDT = 13:25 UTC. From Sat 15:00 UTC = 1d22h25m.
        let expected = 24 * 3600 + 22 * 3600 + 25 * 60;
        assert_eq!(duration.as_secs(), expected);
    }
}
