//! US equity market (NYSE) holiday calendar.
//!
//! Provides a static table of observed NYSE holidays and helpers to determine
//! whether a given date is a trading day. The holiday table covers 2024–2027;
//! dates outside that range fall back to weekday-only validation.

use chrono::{Datelike, NaiveDate, Weekday};

/// NYSE observed holidays for 2024–2027.
///
/// Each entry is the date the exchange is *closed*, which may differ from the
/// calendar holiday when the holiday falls on a weekend (Saturday → preceding
/// Friday, Sunday → following Monday).
const NYSE_HOLIDAYS: &[(i32, u32, u32)] = &[
    // 2024
    (2024, 1, 1),   // New Year's Day
    (2024, 1, 15),  // Martin Luther King Jr. Day
    (2024, 2, 19),  // Presidents' Day
    (2024, 3, 29),  // Good Friday
    (2024, 5, 27),  // Memorial Day
    (2024, 6, 19),  // Juneteenth
    (2024, 7, 4),   // Independence Day
    (2024, 9, 2),   // Labor Day
    (2024, 11, 28), // Thanksgiving Day
    (2024, 12, 25), // Christmas Day
    // 2025
    (2025, 1, 1),   // New Year's Day
    (2025, 1, 9),   // National Day of Mourning (Jimmy Carter)
    (2025, 1, 20),  // Martin Luther King Jr. Day
    (2025, 2, 17),  // Presidents' Day
    (2025, 4, 18),  // Good Friday
    (2025, 5, 26),  // Memorial Day
    (2025, 6, 19),  // Juneteenth
    (2025, 7, 4),   // Independence Day
    (2025, 9, 1),   // Labor Day
    (2025, 11, 27), // Thanksgiving Day
    (2025, 12, 25), // Christmas Day
    // 2026
    (2026, 1, 1),   // New Year's Day
    (2026, 1, 19),  // Martin Luther King Jr. Day
    (2026, 2, 16),  // Presidents' Day
    (2026, 4, 3),   // Good Friday
    (2026, 5, 25),  // Memorial Day
    (2026, 6, 19),  // Juneteenth
    (2026, 7, 3),   // Independence Day (observed; July 4 is Saturday)
    (2026, 9, 7),   // Labor Day
    (2026, 11, 26), // Thanksgiving Day
    (2026, 12, 25), // Christmas Day
    // 2027
    (2027, 1, 1),   // New Year's Day
    (2027, 1, 18),  // Martin Luther King Jr. Day
    (2027, 2, 15),  // Presidents' Day
    (2027, 3, 26),  // Good Friday
    (2027, 5, 31),  // Memorial Day
    (2027, 6, 18),  // Juneteenth (observed; June 19 is Saturday)
    (2027, 7, 5),   // Independence Day (observed; July 4 is Sunday)
    (2027, 9, 6),   // Labor Day
    (2027, 11, 25), // Thanksgiving Day
    (2027, 12, 24), // Christmas Day (observed; December 25 is Saturday)
];

/// The first and last years covered by the holiday table.
const COVERAGE_START_YEAR: i32 = 2024;
const COVERAGE_END_YEAR: i32 = 2027;

/// Returns `true` when `year` has holiday data in the table.
pub fn has_holiday_coverage(year: i32) -> bool {
    (COVERAGE_START_YEAR..=COVERAGE_END_YEAR).contains(&year)
}

/// Returns `true` when `date` is a known NYSE holiday.
///
/// For dates outside the covered year range, returns `false` (no holiday
/// data). Callers should check [`has_holiday_coverage`] and warn when
/// operating on uncovered years.
pub fn is_market_holiday(date: NaiveDate) -> bool {
    let year = date.year();
    let month = date.month();
    let day = date.day();
    NYSE_HOLIDAYS
        .iter()
        .any(|&(holiday_year, holiday_month, holiday_day)| {
            holiday_year == year && holiday_month == month && holiday_day == day
        })
}

/// Returns `true` when `date` is a trading day (weekday and not a holiday).
pub fn is_trading_day(date: NaiveDate) -> bool {
    !matches!(date.weekday(), Weekday::Sat | Weekday::Sun) && !is_market_holiday(date)
}

/// Returns all trading days in the inclusive range `[start, end]`.
pub fn trading_days_in_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut days = Vec::new();
    let mut date = start;
    while date <= end {
        if is_trading_day(date) {
            days.push(date);
        }
        date = match date.succ_opt() {
            Some(next) => next,
            None => break,
        };
    }
    days
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    // --- is_market_holiday ---

    #[test]
    fn test_christmas_2024_is_holiday() {
        assert!(is_market_holiday(date(2024, 12, 25)));
    }

    #[test]
    fn test_carter_mourning_day_2025_is_holiday() {
        assert!(is_market_holiday(date(2025, 1, 9)));
    }

    #[test]
    fn test_independence_day_observed_2026_is_holiday() {
        // July 4, 2026 is Saturday; observed Friday July 3.
        assert!(is_market_holiday(date(2026, 7, 3)));
    }

    #[test]
    fn test_independence_day_actual_2026_is_not_holiday() {
        // The actual July 4 is Saturday — not in the holiday table because
        // the exchange is already closed on weekends.
        assert!(!is_market_holiday(date(2026, 7, 4)));
    }

    #[test]
    fn test_christmas_observed_2027_is_holiday() {
        // December 25, 2027 is Saturday; observed Friday December 24.
        assert!(is_market_holiday(date(2027, 12, 24)));
    }

    #[test]
    fn test_regular_weekday_is_not_holiday() {
        assert!(!is_market_holiday(date(2026, 6, 15)));
    }

    #[test]
    fn test_date_outside_range_is_not_holiday() {
        // 2023 is before our table range.
        assert!(!is_market_holiday(date(2023, 12, 25)));
    }

    // --- is_trading_day ---

    #[test]
    fn test_regular_monday_is_trading_day() {
        assert!(is_trading_day(date(2026, 6, 15)));
    }

    #[test]
    fn test_saturday_is_not_trading_day() {
        assert!(!is_trading_day(date(2026, 6, 13)));
    }

    #[test]
    fn test_sunday_is_not_trading_day() {
        assert!(!is_trading_day(date(2026, 6, 14)));
    }

    #[test]
    fn test_holiday_weekday_is_not_trading_day() {
        // MLK Day 2026 is Monday January 19.
        assert!(!is_trading_day(date(2026, 1, 19)));
    }

    #[test]
    fn test_good_friday_2026_is_not_trading_day() {
        assert!(!is_trading_day(date(2026, 4, 3)));
    }

    // --- trading_days_in_range ---

    #[test]
    fn test_trading_days_excludes_weekends() {
        // Mon June 8 to Sun June 14, 2026 — no holidays, should yield Mon-Fri (5 days).
        let days = trading_days_in_range(date(2026, 6, 8), date(2026, 6, 14));
        assert_eq!(days.len(), 5);
        assert_eq!(days[0], date(2026, 6, 8));
        assert_eq!(days[4], date(2026, 6, 12));
    }

    #[test]
    fn test_trading_days_excludes_holidays() {
        // Thanksgiving week 2026: Nov 23 (Mon) to Nov 27 (Fri).
        // Nov 26 is Thanksgiving — only 4 trading days.
        let days = trading_days_in_range(date(2026, 11, 23), date(2026, 11, 27));
        assert_eq!(days.len(), 4);
        assert!(!days.contains(&date(2026, 11, 26)));
    }

    #[test]
    fn test_trading_days_empty_when_start_after_end() {
        let days = trading_days_in_range(date(2026, 6, 20), date(2026, 6, 15));
        assert!(days.is_empty());
    }

    #[test]
    fn test_trading_days_single_trading_day() {
        let days = trading_days_in_range(date(2026, 6, 15), date(2026, 6, 15));
        assert_eq!(days, vec![date(2026, 6, 15)]);
    }

    #[test]
    fn test_trading_days_single_weekend_day() {
        let days = trading_days_in_range(date(2026, 6, 13), date(2026, 6, 13));
        assert!(days.is_empty());
    }

    #[test]
    fn test_trading_days_single_holiday() {
        let days = trading_days_in_range(date(2026, 12, 25), date(2026, 12, 25));
        assert!(days.is_empty());
    }

    #[test]
    fn test_all_2026_holidays_are_excluded() {
        let days = trading_days_in_range(date(2026, 1, 1), date(2026, 12, 31));
        for &(year, month, day) in NYSE_HOLIDAYS.iter() {
            if year == 2026 {
                let holiday = date(year, month, day);
                assert!(
                    !days.contains(&holiday),
                    "Holiday {} should be excluded",
                    holiday
                );
            }
        }
    }

    #[test]
    fn test_trading_days_count_for_full_week() {
        // A normal week (no holidays) should have exactly 5 trading days.
        // June 8-14, 2026 (Mon-Sun).
        let days = trading_days_in_range(date(2026, 6, 8), date(2026, 6, 14));
        assert_eq!(days.len(), 5);
    }

    #[test]
    fn test_juneteenth_observed_2027() {
        // June 19, 2027 is Saturday; observed Friday June 18.
        assert!(!is_trading_day(date(2027, 6, 18)));
        // June 19 itself is Saturday, so also not a trading day (weekend).
        assert!(!is_trading_day(date(2027, 6, 19)));
    }

    #[test]
    fn test_independence_day_observed_2027() {
        // July 4, 2027 is Sunday; observed Monday July 5.
        assert!(!is_trading_day(date(2027, 7, 5)));
    }

    // --- has_holiday_coverage ---

    #[test]
    fn test_has_coverage_for_known_years() {
        assert!(has_holiday_coverage(2024));
        assert!(has_holiday_coverage(2025));
        assert!(has_holiday_coverage(2026));
        assert!(has_holiday_coverage(2027));
    }

    #[test]
    fn test_no_coverage_for_unknown_years() {
        assert!(!has_holiday_coverage(2023));
        assert!(!has_holiday_coverage(2028));
    }

    /// Ensures the holiday table covers the current calendar year.
    ///
    /// This test fails when the table needs to be extended, serving as a
    /// built-in reminder to add the next year's holidays before they're needed.
    #[test]
    fn test_current_year_has_holiday_coverage() {
        let current_year = chrono::Utc::now()
            .with_timezone(&chrono_tz::US::Eastern)
            .date_naive()
            .year();
        assert!(
            has_holiday_coverage(current_year),
            "NYSE_HOLIDAYS table does not cover {} — add holidays for this year",
            current_year
        );
    }
}
