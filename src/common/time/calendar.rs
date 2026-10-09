//! The published trading calendar over a bounded range, in Eastern wall-clock time throughout.
//! Built from plain session rows, so any provider can feed it; fetching them is not this module's job.

use std::collections::BTreeMap;
use std::ops::Bound;

use chrono::{DateTime, NaiveTime, TimeDelta, Utc};

use super::{REGULAR_CLOSE, SessionDate, SessionRange, eastern_time};

/// One published trading day with the Eastern wall-clock hours it actually keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradingSession {
    date: SessionDate,
    open: NaiveTime,
    close: NaiveTime,
}

/// Why a session row was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRefusal {
    CloseNotAfterOpen {
        date: SessionDate,
        open: NaiveTime,
        close: NaiveTime,
    },
}

impl TradingSession {
    /// A session whose close is strictly after its open.
    pub fn new(
        date: SessionDate,
        open: NaiveTime,
        close: NaiveTime,
    ) -> Result<Self, SessionRefusal> {
        if close <= open {
            return Err(SessionRefusal::CloseNotAfterOpen { date, open, close });
        }
        Ok(Self { date, open, close })
    }

    pub fn date(&self) -> SessionDate {
        self.date
    }

    /// The Eastern wall-clock open.
    pub fn open(&self) -> NaiveTime {
        self.open
    }

    /// The Eastern wall-clock close, which is 13:00 rather than 16:00 on a half-day.
    pub fn close(&self) -> NaiveTime {
        self.close
    }

    /// The regular session as the UTC instants `[open, close)`.
    pub fn hours(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        (
            super::eastern_instant(self.date.date(), self.open),
            super::eastern_instant(self.date.date(), self.close),
        )
    }
}

/// Where an instant falls relative to the published session on its Eastern date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    /// No session is published for the date, which covers both a holiday and a date outside the range.
    NoPublishedSession,
    BeforeOpen {
        #[serde(with = "nanoseconds")]
        until_open: TimeDelta,
    },
    Open {
        #[serde(with = "nanoseconds")]
        until_close: TimeDelta,
    },
    AfterClose,
}

/// A duration as whole nanoseconds, so a journaled phase reads back exactly.
mod nanoseconds {
    use chrono::TimeDelta;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        duration: &TimeDelta,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let nanoseconds = duration
            .num_nanoseconds()
            .ok_or_else(|| serde::ser::Error::custom("a duration past 292 years"))?;
        serializer.serialize_i64(nanoseconds)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<TimeDelta, D::Error> {
        i64::deserialize(deserializer).map(TimeDelta::nanoseconds)
    }
}

/// Why a calendar was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalendarRefusal {
    SessionOutsideRange {
        date: SessionDate,
        range: SessionRange,
    },
    DuplicateSession {
        date: SessionDate,
    },
}

/// The sessions a provider published over an inclusive range.
///
/// The range is carried rather than inferred, because a range opening on a holiday has no session
/// at its own start. A date without a session is treated as not trading, never as unknown-so-open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingCalendar {
    sessions: BTreeMap<SessionDate, TradingSession>,
    range: SessionRange,
}

impl TradingCalendar {
    /// A calendar over `range`, refusing a duplicated date or a session outside the range.
    pub fn new(
        sessions: Vec<TradingSession>,
        range: SessionRange,
    ) -> Result<Self, CalendarRefusal> {
        let mut indexed = BTreeMap::new();
        for session in sessions {
            let date = session.date();
            if !range.contains(date) {
                return Err(CalendarRefusal::SessionOutsideRange { date, range });
            }
            if indexed.insert(date, session).is_some() {
                return Err(CalendarRefusal::DuplicateSession { date });
            }
        }
        Ok(Self {
            sessions: indexed,
            range,
        })
    }

    /// The range the provider published over.
    pub fn range(&self) -> SessionRange {
        self.range
    }

    /// Whether the published range spans the whole of `range`.
    pub fn covers(&self, range: SessionRange) -> bool {
        self.range.encloses(range)
    }

    /// Whether the market trades on `date`; a date outside the range answers `false`.
    pub fn is_trading_day(&self, date: SessionDate) -> bool {
        self.sessions.contains_key(&date)
    }

    /// The published session on `date`, if it trades.
    pub fn session(&self, date: SessionDate) -> Option<&TradingSession> {
        self.sessions.get(&date)
    }

    /// Every session that closes before the regular 16:00 Eastern bell.
    pub fn early_closes(&self) -> impl Iterator<Item = &TradingSession> {
        self.sessions
            .values()
            .filter(|session| session.close() < REGULAR_CLOSE)
    }

    /// The number of published sessions.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// The latest trading day strictly before `date`, or `None` when the range holds none.
    pub fn previous_trading_day(&self, date: SessionDate) -> Option<SessionDate> {
        self.sessions.range(..date).next_back().map(|(day, _)| *day)
    }

    /// The earliest trading day strictly after `date`, or `None` when the range holds none.
    pub fn next_trading_day(&self, date: SessionDate) -> Option<SessionDate> {
        self.sessions
            .range((Bound::Excluded(date), Bound::Unbounded))
            .next()
            .map(|(day, _)| *day)
    }

    /// Every trading day in `range`.
    pub fn trading_days_in_range(&self, range: SessionRange) -> Vec<SessionDate> {
        self.sessions
            .range(range.first()..=range.last())
            .map(|(day, _)| *day)
            .collect()
    }

    /// Where `instant` falls in its Eastern date's session, read from the published hours.
    pub fn phase_at(&self, instant: DateTime<Utc>) -> SessionPhase {
        let Some(session) = self.session(SessionDate::at(instant)) else {
            return SessionPhase::NoPublishedSession;
        };
        let now = eastern_time(instant);
        if now < session.open() {
            SessionPhase::BeforeOpen {
                until_open: session.open() - now,
            }
        } else if now < session.close() {
            SessionPhase::Open {
                until_close: session.close() - now,
            }
        } else {
            SessionPhase::AfterClose
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;

    fn date(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    fn range(first: SessionDate, last: SessionDate) -> SessionRange {
        SessionRange::new(first, last).unwrap()
    }

    fn time(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn day(session_date: SessionDate, open: (u32, u32), close: (u32, u32)) -> TradingSession {
        TradingSession::new(session_date, time(open.0, open.1), time(close.0, close.1)).unwrap()
    }

    /// Thanksgiving week 2026: a holiday on the 26th, a half-day on the 27th, then the weekend.
    fn calendar() -> TradingCalendar {
        TradingCalendar::new(
            vec![
                day(date(2026, 11, 24), (9, 30), (16, 0)),
                day(date(2026, 11, 25), (9, 30), (16, 0)),
                day(date(2026, 11, 27), (9, 30), (13, 0)),
                day(date(2026, 11, 30), (9, 30), (16, 0)),
            ],
            range(date(2026, 11, 23), date(2026, 11, 30)),
        )
        .unwrap()
    }

    #[test]
    fn test_a_session_refuses_a_close_not_after_its_open() {
        assert_eq!(
            TradingSession::new(date(2026, 11, 24), time(16, 0), time(16, 0)),
            Err(SessionRefusal::CloseNotAfterOpen {
                date: date(2026, 11, 24),
                open: time(16, 0),
                close: time(16, 0),
            })
        );
    }

    #[test]
    fn test_a_calendar_refuses_a_session_outside_its_range() {
        assert_eq!(
            TradingCalendar::new(
                vec![day(date(2026, 12, 1), (9, 30), (16, 0))],
                range(date(2026, 11, 23), date(2026, 11, 30)),
            ),
            Err(CalendarRefusal::SessionOutsideRange {
                date: date(2026, 12, 1),
                range: range(date(2026, 11, 23), date(2026, 11, 30)),
            })
        );
    }

    /// A map silently keeps the last of two rows for one date, so a disagreeing provider would go unseen.
    #[test]
    fn test_a_calendar_refuses_a_duplicated_date() {
        assert_eq!(
            TradingCalendar::new(
                vec![
                    day(date(2026, 11, 27), (9, 30), (16, 0)),
                    day(date(2026, 11, 27), (9, 30), (13, 0)),
                ],
                range(date(2026, 11, 23), date(2026, 11, 30)),
            ),
            Err(CalendarRefusal::DuplicateSession {
                date: date(2026, 11, 27)
            })
        );
    }

    #[test]
    fn test_covers_only_ranges_inside_the_published_one() {
        let calendar = calendar();
        assert!(calendar.covers(range(date(2026, 11, 23), date(2026, 11, 30))));
        assert!(calendar.covers(range(date(2026, 11, 25), date(2026, 11, 25))));
        assert!(!calendar.covers(range(date(2026, 11, 22), date(2026, 11, 30))));
        assert!(!calendar.covers(range(date(2026, 11, 23), date(2026, 12, 1))));
    }

    #[test]
    fn test_next_trading_day_after_the_last_representable_date_is_none() {
        let last = SessionDate::from_date(NaiveDate::MAX);
        assert_eq!(calendar().next_trading_day(last), None);
    }

    #[test]
    fn test_holiday_is_not_a_trading_day() {
        assert!(!calendar().is_trading_day(date(2026, 11, 26)));
        assert!(calendar().is_trading_day(date(2026, 11, 27)));
    }

    #[test]
    fn test_unknown_date_does_not_trade() {
        assert!(!calendar().is_trading_day(date(2030, 1, 2)));
    }

    #[test]
    fn test_half_day_reports_its_real_close() {
        let calendar = calendar();
        let session = calendar.session(date(2026, 11, 27)).unwrap();
        assert_eq!(session.date(), date(2026, 11, 27));
        assert_eq!(session.open(), time(9, 30));
        assert_eq!(session.close(), time(13, 0));
    }

    #[test]
    fn test_early_closes_names_only_the_half_day() {
        let dates: Vec<SessionDate> = calendar()
            .early_closes()
            .map(TradingSession::date)
            .collect();
        assert_eq!(dates, vec![date(2026, 11, 27)]);
    }

    #[test]
    fn test_a_half_day_closes_at_one_eastern_in_utc() {
        let half_day = calendar().session(date(2026, 11, 27)).copied().unwrap();
        let (open, close) = half_day.hours();
        assert_eq!(open.to_rfc3339(), "2026-11-27T14:30:00+00:00");
        assert_eq!(close.to_rfc3339(), "2026-11-27T18:00:00+00:00");
    }

    #[test]
    fn test_empty_calendar_refuses_everything() {
        let empty =
            TradingCalendar::new(vec![], range(date(2026, 11, 28), date(2026, 11, 29))).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert!(!empty.is_trading_day(date(2026, 11, 24)));
        assert_eq!(
            empty.phase_at(instant("2026-11-24T16:00:00Z")),
            SessionPhase::NoPublishedSession
        );
        assert_eq!(empty.previous_trading_day(date(2026, 11, 24)), None);
        assert_eq!(empty.next_trading_day(date(2026, 11, 24)), None);
    }

    #[test]
    fn test_previous_trading_day_skips_holiday_and_weekend() {
        let calendar = calendar();
        assert_eq!(calendar.len(), 4);
        assert_eq!(
            calendar.previous_trading_day(date(2026, 11, 27)),
            Some(date(2026, 11, 25))
        );
        assert_eq!(
            calendar.previous_trading_day(date(2026, 11, 30)),
            Some(date(2026, 11, 27))
        );
        assert_eq!(calendar.previous_trading_day(date(2026, 11, 24)), None);
    }

    #[test]
    fn test_next_trading_day_skips_holiday_and_weekend() {
        let calendar = calendar();
        assert_eq!(
            calendar.next_trading_day(date(2026, 11, 25)),
            Some(date(2026, 11, 27))
        );
        assert_eq!(
            calendar.next_trading_day(date(2026, 11, 27)),
            Some(date(2026, 11, 30))
        );
        assert_eq!(calendar.next_trading_day(date(2026, 11, 30)), None);
    }

    #[test]
    fn test_trading_days_in_range_excludes_non_sessions() {
        assert_eq!(
            calendar().trading_days_in_range(range(date(2026, 11, 24), date(2026, 11, 30))),
            vec![
                date(2026, 11, 24),
                date(2026, 11, 25),
                date(2026, 11, 27),
                date(2026, 11, 30),
            ]
        );
    }

    /// 14:00 UTC is 09:00 Eastern in November, half an hour before the open.
    #[test]
    fn test_phase_respects_eastern_session_hours() {
        let calendar = calendar();
        assert_eq!(
            calendar.phase_at(instant("2026-11-24T14:00:00Z")),
            SessionPhase::BeforeOpen {
                until_open: TimeDelta::minutes(30)
            }
        );
        assert_eq!(
            calendar.phase_at(instant("2026-11-24T14:30:00Z")),
            SessionPhase::Open {
                until_close: TimeDelta::minutes(390)
            }
        );
        assert_eq!(
            calendar.phase_at(instant("2026-11-24T21:00:00Z")),
            SessionPhase::AfterClose
        );
        assert_eq!(
            calendar.phase_at(instant("2026-11-26T17:00:00Z")),
            SessionPhase::NoPublishedSession
        );
    }

    /// At noon Eastern the half-day has an hour left, not four; assuming 16:00 trades past the bell.
    #[test]
    fn test_phase_counts_down_to_the_published_close() {
        let calendar = calendar();
        assert_eq!(
            calendar.phase_at(instant("2026-11-27T17:00:00Z")),
            SessionPhase::Open {
                until_close: TimeDelta::minutes(60)
            }
        );
        assert_eq!(
            calendar.phase_at(instant("2026-11-24T17:00:00Z")),
            SessionPhase::Open {
                until_close: TimeDelta::minutes(240)
            }
        );
        assert_eq!(
            calendar.phase_at(instant("2026-11-27T18:00:00Z")),
            SessionPhase::AfterClose
        );
    }

    /// A calendar of up to 59 days from a 2020s start, with random sessions closing 10:00 to 16:00.
    fn any_calendar() -> impl Strategy<Value = TradingCalendar> {
        (
            0_i64..3_650,
            prop::collection::vec((any::<bool>(), 600_u32..=960), 1..60),
        )
            .prop_map(|(offset, days)| {
                let first = date(2020, 1, 1).plus_calendar_days(offset);
                let last = first.plus_calendar_days(days.len() as i64 - 1);
                let sessions = days
                    .iter()
                    .enumerate()
                    .filter(|(_, (trades, _))| *trades)
                    .map(|(index, (_, close_minute))| {
                        TradingSession::new(
                            first.plus_calendar_days(index as i64),
                            time(9, 30),
                            time(close_minute / 60, close_minute % 60),
                        )
                        .unwrap()
                    })
                    .collect();
                TradingCalendar::new(sessions, range(first, last)).unwrap()
            })
    }

    proptest! {
        #[test]
        fn property_next_and_previous_are_inverses_on_trading_days(calendar in any_calendar()) {
            let trading_days = calendar.trading_days_in_range(calendar.range());
            prop_assert_eq!(trading_days.len(), calendar.len());
            for day in trading_days {
                if let Some(next) = calendar.next_trading_day(day) {
                    prop_assert_eq!(calendar.previous_trading_day(next), Some(day));
                }
                if let Some(previous) = calendar.previous_trading_day(day) {
                    prop_assert_eq!(calendar.next_trading_day(previous), Some(day));
                }
            }
        }

        #[test]
        fn property_stepping_lands_on_the_adjacent_trading_day(
            calendar in any_calendar(),
            offset in -5_i64..65,
        ) {
            let start = calendar.range().first().plus_calendar_days(offset);
            if let Some(next) = calendar.next_trading_day(start) {
                prop_assert!(calendar.is_trading_day(next) && start < next);
                // Adjacent days leave an inverted range, which holds nothing between them.
                let between = SessionRange::new(start.plus_calendar_days(1), next.plus_calendar_days(-1))
                    .map(|between| calendar.trading_days_in_range(between))
                    .unwrap_or_default();
                prop_assert!(between.is_empty());
            }
            if let Some(previous) = calendar.previous_trading_day(start) {
                prop_assert!(calendar.is_trading_day(previous) && previous < start);
                let between = SessionRange::new(previous.plus_calendar_days(1), start.plus_calendar_days(-1))
                    .map(|between| calendar.trading_days_in_range(between))
                    .unwrap_or_default();
                prop_assert!(between.is_empty());
            }
        }

        #[test]
        fn property_phase_is_open_exactly_between_the_published_hours(
            calendar in any_calendar(),
            offset in 0_i64..60,
            minute in 0_i64..1_440,
        ) {
            let day = calendar.range().first().plus_calendar_days(offset);
            let at = day.midnight() + TimeDelta::minutes(minute);
            prop_assume!(SessionDate::at(at) == day);
            let phase = calendar.phase_at(at);
            match (calendar.session(day), phase) {
                (None, SessionPhase::NoPublishedSession) => {}
                (Some(session), SessionPhase::BeforeOpen { until_open }) => {
                    prop_assert!(until_open > TimeDelta::zero());
                    prop_assert_eq!(eastern_time(at) + until_open, session.open());
                }
                (Some(session), SessionPhase::Open { until_close }) => {
                    prop_assert!(session.open() <= eastern_time(at));
                    prop_assert!(until_close > TimeDelta::zero());
                    prop_assert_eq!(eastern_time(at) + until_close, session.close());
                }
                (Some(session), SessionPhase::AfterClose) => {
                    prop_assert!(session.close() <= eastern_time(at));
                }
                (None, SessionPhase::BeforeOpen { .. } | SessionPhase::Open { .. } | SessionPhase::AfterClose)
                | (Some(_), SessionPhase::NoPublishedSession) => {
                    prop_assert!(false, "{day} reported {phase:?}");
                }
            }
        }
    }
}
