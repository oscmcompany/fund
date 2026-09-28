//! Time has two kinds that never mix: an instant is a `DateTime<Utc>`, and a session is an
//! `America/New_York` calendar date. Every conversion between them goes through this module.

pub mod calendar;

use chrono::{
    DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Utc, Weekday,
};
use chrono_tz::America::New_York;

/// The usual 16:00 Eastern bell, which a daily bar is stamped at and an early close ends before.
const REGULAR_CLOSE: NaiveTime = match NaiveTime::from_hms_opt(16, 0, 0) {
    Some(time) => time,
    None => panic!("16:00 is a valid wall-clock time"),
};

/// A trading day, identified by its `America/New_York` calendar date.
///
/// It guarantees the timezone, not tradability: weekends and holidays are representable, and only
/// [`calendar::TradingCalendar::is_trading_day`] answers whether a date trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionDate(NaiveDate);

impl SessionDate {
    /// The session an instant falls in, which rolls over at Eastern midnight rather than UTC midnight.
    pub fn at(instant: DateTime<Utc>) -> Self {
        Self(instant.with_timezone(&New_York).date_naive())
    }

    /// Wraps a date already expressed in Eastern terms, such as a provider's calendar row.
    ///
    /// Deriving one from an instant is [`SessionDate::at`]'s job; reaching here through
    /// `date_naive()` is the bug this type exists to prevent.
    pub fn from_date(date: NaiveDate) -> Self {
        Self(date)
    }

    /// The underlying Eastern calendar date, for formatting and storage.
    pub fn date(self) -> NaiveDate {
        self.0
    }

    /// Midnight Eastern on this date, as a UTC instant; for date-only values, never a daily bar.
    pub fn midnight(self) -> DateTime<Utc> {
        eastern_instant(self.0, NaiveTime::MIN)
    }

    /// The 16:00 Eastern close on this date, as the UTC instant a daily bar is stamped at.
    pub fn regular_close(self) -> DateTime<Utc> {
        eastern_instant(self.0, REGULAR_CLOSE)
    }

    /// The half-open UTC interval `[start, end)` of instants whose session is this one.
    ///
    /// It spans 23 or 25 hours on the two daylight saving transition days.
    pub fn bounds(self) -> (DateTime<Utc>, DateTime<Utc>) {
        (self.midnight(), self.plus_calendar_days(1).midnight())
    }

    /// This date shifted by whole calendar days, landing on weekends and holidays alike.
    pub fn plus_calendar_days(self, days: i64) -> Self {
        Self(self.0 + TimeDelta::days(days))
    }

    /// Whether this date is a Saturday or Sunday; it knows nothing of holidays.
    pub fn is_weekend(self) -> bool {
        match self.0.weekday() {
            Weekday::Sat | Weekday::Sun => true,
            Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri => false,
        }
    }
}

impl std::fmt::Display for SessionDate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The Eastern wall-clock date and time at an instant, from one conversion so the halves agree.
pub fn eastern_datetime(instant: DateTime<Utc>) -> NaiveDateTime {
    instant.with_timezone(&New_York).naive_local()
}

/// The Eastern wall-clock time at an instant.
fn eastern_time(instant: DateTime<Utc>) -> NaiveTime {
    eastern_datetime(instant).time()
}

/// The UTC instant of an Eastern wall-clock time on an Eastern date.
///
/// Every time this module builds exists year-round because the zone shifts at 02:00; `earliest()`
/// with a UTC fallback keeps a timezone database change from panicking a caller.
fn eastern_instant(date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
    let local = date.and_time(time);
    New_York
        .from_local_datetime(&local)
        .earliest()
        .map(|zoned| zoned.with_timezone(&Utc))
        .unwrap_or_else(|| local.and_utc())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    /// 03:00 UTC is still the previous evening in New York, so a UTC-keyed date is a day late.
    #[test]
    fn test_eastern_date_rolls_at_eastern_midnight() {
        assert_eq!(
            SessionDate::at(instant("2026-06-11T03:00:00Z")),
            session(2026, 6, 10)
        );
        assert_eq!(
            SessionDate::at(instant("2026-06-11T13:00:00Z")),
            session(2026, 6, 11)
        );
    }

    /// In summer Eastern is UTC-4, so the day runs 04:00 to 04:00 UTC.
    #[test]
    fn test_eastern_day_bounds_span_the_local_day_in_summer() {
        let (start, end) = session(2026, 6, 10).bounds();
        assert_eq!(start.to_rfc3339(), "2026-06-10T04:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2026-06-11T04:00:00+00:00");
    }

    /// In winter Eastern is UTC-5, so a fixed offset gets one of the two seasons wrong.
    #[test]
    fn test_eastern_day_bounds_span_the_local_day_in_winter() {
        let (start, end) = session(2026, 1, 14).bounds();
        assert_eq!(start.to_rfc3339(), "2026-01-14T05:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2026-01-15T05:00:00+00:00");
    }

    /// Adding a fixed 24 hours would include or exclude an hour on exactly these two days.
    #[test]
    fn test_eastern_day_bounds_handle_daylight_saving_transitions() {
        let (spring_start, spring_end) = session(2026, 3, 8).bounds();
        assert_eq!((spring_end - spring_start).num_hours(), 23);
        let (autumn_start, autumn_end) = session(2026, 11, 1).bounds();
        assert_eq!((autumn_end - autumn_start).num_hours(), 25);
    }

    #[test]
    fn test_eastern_day_bounds_round_trip_against_eastern_date() {
        let day = session(2026, 6, 10);
        let (start, end) = day.bounds();
        assert_eq!(SessionDate::at(start), day);
        assert_eq!(SessionDate::at(end - TimeDelta::seconds(1)), day);
        assert_eq!(SessionDate::at(end), session(2026, 6, 11));
    }

    /// A daily bar is stamped at the close, which is 20:00 UTC in summer and 21:00 UTC in winter.
    #[test]
    fn test_regular_close_is_the_eastern_bell_in_both_seasons() {
        assert_eq!(
            session(2026, 6, 10).regular_close().to_rfc3339(),
            "2026-06-10T20:00:00+00:00"
        );
        assert_eq!(
            session(2026, 1, 14).regular_close().to_rfc3339(),
            "2026-01-14T21:00:00+00:00"
        );
    }

    #[test]
    fn test_is_weekend() {
        assert!(session(2026, 11, 28).is_weekend());
        assert!(session(2026, 11, 29).is_weekend());
        assert!(!session(2026, 11, 27).is_weekend());
    }

    #[test]
    fn test_display_is_the_iso_date() {
        assert_eq!(session(2026, 7, 6).to_string(), "2026-07-06");
    }

    /// 00:30 UTC on 1 August is 20:30 on 31 July in New York, so a UTC-named run names the wrong session.
    #[test]
    fn test_eastern_datetime_names_the_session_the_instant_belongs_to() {
        let after_utc_midnight = instant("2026-08-01T00:30:00Z");
        assert_eq!(
            after_utc_midnight.date_naive(),
            NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()
        );
        assert_eq!(
            eastern_datetime(after_utc_midnight)
                .format("%Y-%m-%d-%H-%M-%S")
                .to_string(),
            "2026-07-31-20-30-00"
        );
    }

    /// 13:30 UTC is 09:30 Eastern in summer and 08:30 in winter.
    #[test]
    fn test_eastern_time_follows_daylight_saving() {
        assert_eq!(
            eastern_time(instant("2026-06-10T13:30:00Z")),
            NaiveTime::from_hms_opt(9, 30, 0).unwrap()
        );
        assert_eq!(
            eastern_time(instant("2026-01-14T13:30:00Z")),
            NaiveTime::from_hms_opt(8, 30, 0).unwrap()
        );
    }

    /// Instants from 1970 to 2100, the span the timezone database carries rules for.
    fn any_instant() -> impl Strategy<Value = DateTime<Utc>> {
        (0_i64..4_102_444_800).prop_map(|seconds| DateTime::from_timestamp(seconds, 0).unwrap())
    }

    /// Dates from 1970 to 2099.
    fn any_session() -> impl Strategy<Value = SessionDate> {
        (0_i64..47_000).prop_map(|days| {
            SessionDate::from_date(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + TimeDelta::days(days),
            )
        })
    }

    proptest! {
        #[test]
        fn property_bounds_contain_the_instant_they_were_derived_from(instant in any_instant()) {
            let (start, end) = SessionDate::at(instant).bounds();
            prop_assert!(start <= instant && instant < end);
        }

        #[test]
        fn property_midnight_round_trips_to_its_own_date(session in any_session()) {
            prop_assert_eq!(SessionDate::at(session.midnight()), session);
            prop_assert_eq!(SessionDate::from_date(session.date()), session);
        }

        #[test]
        fn property_regular_close_falls_inside_its_own_session(session in any_session()) {
            let (start, end) = session.bounds();
            let close = session.regular_close();
            prop_assert!(start < close && close < end);
            prop_assert_eq!(eastern_time(close), REGULAR_CLOSE);
        }

        #[test]
        fn property_consecutive_bounds_tile_the_timeline(session in any_session()) {
            let (start, end) = session.bounds();
            prop_assert_eq!(end, session.plus_calendar_days(1).bounds().0);
            prop_assert!([23, 24, 25].contains(&(end - start).num_hours()));
        }

        #[test]
        fn property_ordering_follows_the_underlying_date(left in any_session(), right in any_session()) {
            prop_assert_eq!(left.cmp(&right), left.date().cmp(&right.date()));
        }

        #[test]
        fn property_at_is_monotone(left in any_instant(), right in any_instant()) {
            let (earlier, later) = if left <= right { (left, right) } else { (right, left) };
            prop_assert!(SessionDate::at(earlier) <= SessionDate::at(later));
        }

        #[test]
        fn property_calendar_day_shifts_compose(
            session in any_session(),
            first in -400_i64..400,
            second in -400_i64..400,
        ) {
            prop_assert_eq!(
                session.plus_calendar_days(first).plus_calendar_days(second),
                session.plus_calendar_days(first + second)
            );
            prop_assert_eq!(session.plus_calendar_days(0), session);
        }
    }
}
