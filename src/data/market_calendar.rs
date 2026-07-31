//! US equity market (NYSE) trading calendar.
//!
//! Two sources answer "does the market trade on this date, and between what
//! times", in that order of preference:
//!
//! 1. **The published calendar**, fetched from Alpaca and persisted to the
//!    `market_calendar` table. A row exists for every trading day and carries
//!    the real open and close, so half-days are represented as the shortened
//!    sessions they are. This is the only source that knows about them.
//! 2. **The static holiday table below**, covering 2024–2027. Weekday-and-not-a-
//!    holiday only: it has no concept of a shortened session, and it needs
//!    editing by hand each year.
//!
//! The static table survives as a cold-start fallback rather than a peer. Before
//! the first sync completes — a fresh database, a failed fetch, a date outside
//! the synced horizon — trading-day arithmetic still has to answer, and
//! answering from a slightly stale holiday list beats answering weekday-only.
//! Dates outside both fall back to weekday-only.
//!
//! [`install`] publishes a loaded calendar for the whole process. Lookups stay
//! synchronous and infallible, which is what lets `TradingDate::from_naive_date`
//! and the gap-detection arithmetic keep their shape; the alternative was
//! threading an async, fallible calendar handle through a dozen pure functions.

use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

use chrono::{Datelike, NaiveDate, NaiveTime, Weekday};

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

/// The open and close of one trading session, in Eastern local time.
///
/// A regular session is 09:30–16:00. A half-day closes at 13:00 and is the
/// reason this type carries times at all rather than being a set of dates: the
/// static holiday table cannot express one, and every early close in the year is
/// a day the system would otherwise treat as running until 16:00.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionHours {
    open: NaiveTime,
    close: NaiveTime,
}

impl SessionHours {
    /// Constructs session hours, rejecting a close at or before the open.
    pub fn new(open: NaiveTime, close: NaiveTime) -> Option<Self> {
        if close <= open {
            return None;
        }
        Some(Self { open, close })
    }

    /// Returns the session open, in Eastern local time.
    pub fn open(&self) -> NaiveTime {
        self.open
    }

    /// Returns the session close, in Eastern local time.
    pub fn close(&self) -> NaiveTime {
        self.close
    }

    /// Returns `true` when this session closes earlier than a regular one.
    pub fn is_shortened(&self) -> bool {
        self.close < regular_close()
    }
}

/// The close of a regular, unshortened session: 16:00 Eastern.
fn regular_close() -> NaiveTime {
    NaiveTime::from_hms_opt(16, 0, 0).expect("16:00:00 is a valid time")
}

/// A published trading calendar: one entry per trading day, with its real hours.
///
/// Absence of a date inside [`MarketCalendar::horizon`] is meaningful — it means
/// the market does not trade that day. Absence outside the horizon means only
/// that the calendar was not asked about it, which is why the horizon is carried
/// alongside the entries rather than inferred from them.
#[derive(Debug, Clone, Default)]
pub struct MarketCalendar {
    sessions: BTreeMap<NaiveDate, SessionHours>,
}

impl MarketCalendar {
    /// Builds a calendar from published sessions.
    pub fn new(sessions: BTreeMap<NaiveDate, SessionHours>) -> Self {
        Self { sessions }
    }

    /// Returns the first and last dates the calendar can speak to.
    ///
    /// `None` when empty. Derived from the entries: a synced range always has a
    /// trading day at each end, because a range ending on a weekend or holiday
    /// carries no row for it either way.
    pub fn horizon(&self) -> Option<(NaiveDate, NaiveDate)> {
        let first = *self.sessions.keys().next()?;
        let last = *self.sessions.keys().next_back()?;
        Some((first, last))
    }

    /// Returns `true` when the calendar has coverage spanning `date`.
    pub fn covers(&self, date: NaiveDate) -> bool {
        matches!(self.horizon(), Some((first, last)) if date >= first && date <= last)
    }

    /// Returns the session hours for `date`, or `None` when it does not trade.
    pub fn session_hours(&self, date: NaiveDate) -> Option<SessionHours> {
        self.sessions.get(&date).copied()
    }

    /// Returns the number of sessions held.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Returns whether the calendar holds no sessions.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Returns the next trading day at or after `date`, within the horizon.
    pub fn next_session_on_or_after(&self, date: NaiveDate) -> Option<(NaiveDate, SessionHours)> {
        self.sessions
            .range(date..)
            .next()
            .map(|(date, hours)| (*date, *hours))
    }
}

/// Process-wide published calendar, installed once the first sync has loaded.
static PUBLISHED_CALENDAR: OnceLock<RwLock<MarketCalendar>> = OnceLock::new();

fn published_calendar() -> &'static RwLock<MarketCalendar> {
    PUBLISHED_CALENDAR.get_or_init(|| RwLock::new(MarketCalendar::default()))
}

/// Publishes `calendar` for the whole process, replacing any previous one.
///
/// Called once at startup after loading from the `market_calendar` table, and
/// again whenever a sync refreshes it. Until then every lookup falls back to the
/// static holiday table, so an uninstalled calendar degrades rather than fails.
pub fn install(calendar: MarketCalendar) {
    match published_calendar().write() {
        Ok(mut published) => *published = calendar,
        // A poisoned lock means a writer panicked mid-install. The calendar is
        // reference data with a total fallback, so continuing on the previous
        // one is better than propagating the panic into a trading path.
        Err(poisoned) => *poisoned.into_inner() = calendar,
    }
}

/// Returns the session hours for `date` from the published calendar.
///
/// `None` when the date does not trade, or when the calendar has no coverage
/// for it. Callers that need to tell those apart use [`calendar_covers`].
pub fn session_hours(date: NaiveDate) -> Option<SessionHours> {
    with_published(|calendar| calendar.session_hours(date))
}

/// Returns `true` when the published calendar spans `date`.
pub fn calendar_covers(date: NaiveDate) -> bool {
    with_published(|calendar| calendar.covers(date))
}

/// Returns the next published session at or after `date`.
///
/// This is the "impending session" lookup: it answers whether the market opens,
/// and between what times, for a date the system has not reached yet. The
/// `/v2/clock` endpoint cannot answer it — it reports one session, the next
/// close, so a question about next Tuesday is unanswerable until Tuesday.
pub fn next_session_on_or_after(date: NaiveDate) -> Option<(NaiveDate, SessionHours)> {
    with_published(|calendar| calendar.next_session_on_or_after(date))
}

/// Returns the published horizon, for reporting how far ahead the calendar sees.
pub fn published_horizon() -> Option<(NaiveDate, NaiveDate)> {
    with_published(|calendar| calendar.horizon())
}

/// Reads the published calendar, tolerating a poisoned lock.
fn with_published<T>(read: impl FnOnce(&MarketCalendar) -> T) -> T {
    match published_calendar().read() {
        Ok(published) => read(&published),
        Err(poisoned) => read(&poisoned.into_inner()),
    }
}

/// Returns `true` when `date` is a known NYSE holiday.
///
/// Consults the static table only. For dates outside the covered year range,
/// returns `false` (no holiday data). Prefer [`is_trading_day`], which reaches
/// for the published calendar first.
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

/// Returns `true` when the market trades on `date`.
///
/// Answers from the published calendar when it covers the date — where presence
/// of a row *is* the answer — and from the static holiday table otherwise. Both
/// paths agree on regular closures; they differ only in that the published one
/// stays correct without anyone editing a list each year.
pub fn is_trading_day(date: NaiveDate) -> bool {
    if calendar_covers(date) {
        return session_hours(date).is_some();
    }
    is_weekday_and_not_a_known_holiday(date)
}

/// The static fallback: a weekday that is not in the hardcoded holiday table.
fn is_weekday_and_not_a_known_holiday(date: NaiveDate) -> bool {
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

    // --- Published calendar ---
    //
    // These tests install into process-wide state, so they run serially and
    // restore an empty calendar afterwards.
    //
    // Fixtures deliberately sit in 2035, outside every date any other test in
    // the crate asserts on. A published calendar only overrides inside its own
    // horizon, so even mid-test — when an install is briefly live and an
    // unmarked test elsewhere reads `is_trading_day` — no other test can observe
    // one of these. The two override tests below have to use a date the static
    // holiday table knows, and use 2027-12-24, which nothing else reads through
    // `is_trading_day`.

    fn calendar_with(sessions: &[(NaiveDate, NaiveTime, NaiveTime)]) -> MarketCalendar {
        let mut published = BTreeMap::new();
        for &(date, open, close) in sessions {
            published.insert(date, SessionHours::new(open, close).expect("valid hours"));
        }
        MarketCalendar::new(published)
    }

    fn published_date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    fn time(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    fn regular(date_text: &str) -> (NaiveDate, NaiveTime, NaiveTime) {
        (published_date(date_text), time(9, 30), time(16, 0))
    }

    #[test]
    fn test_session_hours_reject_a_close_at_or_before_the_open() {
        assert!(SessionHours::new(time(9, 30), time(9, 30)).is_none());
        assert!(SessionHours::new(time(16, 0), time(9, 30)).is_none());
        assert!(SessionHours::new(time(9, 30), time(13, 0)).is_some());
    }

    #[test]
    fn test_a_shortened_session_is_recognised() {
        let regular_hours = SessionHours::new(time(9, 30), time(16, 0)).unwrap();
        let half_day = SessionHours::new(time(9, 30), time(13, 0)).unwrap();
        assert!(!regular_hours.is_shortened());
        assert!(half_day.is_shortened());
    }

    #[test]
    fn test_an_empty_calendar_has_no_horizon_and_covers_nothing() {
        let empty = MarketCalendar::default();
        assert!(empty.horizon().is_none());
        assert!(!empty.covers(published_date("2035-06-12")));
        assert!(empty.is_empty());
    }

    #[test]
    fn test_absence_inside_the_horizon_means_the_market_is_closed() {
        // 2026-07-03 is the observed Independence Day holiday.
        let calendar = calendar_with(&[regular("2035-07-03"), regular("2035-07-06")]);
        assert!(calendar.covers(published_date("2035-07-04")));
        assert!(calendar
            .session_hours(published_date("2035-07-04"))
            .is_none());
        // Outside the horizon, absence says nothing at all.
        assert!(!calendar.covers(published_date("2035-08-01")));
    }

    #[test]
    #[serial_test::serial]
    fn test_published_calendar_overrides_the_static_holiday_table() {
        // A date the static table calls a holiday, published as trading. The
        // published calendar is authoritative where it has coverage — that is
        // the whole reason it exists.
        let christmas = published_date("2027-12-24");
        assert!(is_market_holiday(christmas));
        assert!(!is_trading_day(christmas));

        install(calendar_with(&[
            regular("2027-12-23"),
            regular("2027-12-24"),
            regular("2027-12-27"),
        ]));
        assert!(is_trading_day(christmas));

        install(MarketCalendar::default());
        assert!(!is_trading_day(christmas));
    }

    #[test]
    #[serial_test::serial]
    fn test_published_calendar_reports_a_closure_the_static_table_misses() {
        // A hypothetical unscheduled closure — a day of mourning — is a date
        // with no published row. The static table has no way to learn about one
        // without an edit and a redeploy.
        let closed = published_date("2035-06-12");
        assert!(is_trading_day(closed));

        install(calendar_with(&[
            regular("2035-06-11"),
            regular("2035-06-13"),
        ]));
        assert!(!is_trading_day(closed));

        install(MarketCalendar::default());
        assert!(is_trading_day(closed));
    }

    #[test]
    #[serial_test::serial]
    fn test_dates_outside_the_horizon_fall_back_to_the_static_table() {
        install(calendar_with(&[
            regular("2035-06-11"),
            regular("2035-06-13"),
        ]));

        // Inside the horizon the calendar answers; outside it the static table
        // does, so a synced range narrower than the questions asked degrades
        // rather than reporting every uncovered date as closed.
        assert!(!calendar_covers(published_date("2027-12-24")));
        assert!(!is_trading_day(published_date("2027-12-24")));
        assert!(is_trading_day(published_date("2027-12-23")));

        install(MarketCalendar::default());
    }

    #[test]
    #[serial_test::serial]
    fn test_half_day_hours_are_published() {
        install(calendar_with(&[
            regular("2035-11-22"),
            (published_date("2035-11-23"), time(9, 30), time(13, 0)),
        ]));

        let hours = session_hours(published_date("2035-11-23")).expect("the half day is published");
        assert_eq!(hours.close(), time(13, 0));
        assert!(hours.is_shortened());

        install(MarketCalendar::default());
    }

    #[test]
    #[serial_test::serial]
    fn test_next_session_answers_an_impending_date() {
        // The lookup `/v2/clock` cannot serve: what happens on a date the
        // system has not reached yet.
        install(calendar_with(&[
            regular("2035-07-03"),
            (published_date("2035-07-06"), time(9, 30), time(13, 0)),
        ]));

        let (next_date, hours) = next_session_on_or_after(published_date("2035-07-04"))
            .expect("a later session is published");
        assert_eq!(next_date, published_date("2035-07-06"));
        assert!(hours.is_shortened());

        install(MarketCalendar::default());
    }

    #[test]
    #[serial_test::serial]
    fn test_published_horizon_is_reported() {
        install(calendar_with(&[
            regular("2035-06-11"),
            regular("2035-06-13"),
        ]));
        assert_eq!(
            published_horizon(),
            Some((published_date("2035-06-11"), published_date("2035-06-13")))
        );
        install(MarketCalendar::default());
        assert_eq!(published_horizon(), None);
    }
}
