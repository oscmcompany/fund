//! What a nightly run owes the archive: the calendar's sessions in a trailing window, less those already held.
//! A missed night and a week's outage heal by the same difference, so nothing remembers what failed.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::common::market::record::BarInterval;
use crate::common::storage::{Key, Origin, Provider};
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

/// One series the archiver keeps whole, in the order a run works them: the daily bars first, since they are the
/// one-minute leg's symbol list.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Leg {
    MassiveDailyBars,
    AlpacaMinuteBars,
}

impl Leg {
    pub fn key(self, session: SessionDate) -> Key {
        let (provider, interval) = match self {
            Self::MassiveDailyBars => (Provider::Massive, BarInterval::OneDay),
            Self::AlpacaMinuteBars => (Provider::Alpaca, BarInterval::OneMinute),
        };
        Key::Bars {
            provider,
            origin: Origin::Fetched,
            interval,
            session,
        }
    }
}

/// How one owed session ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum SessionOutcome {
    Written,
    Failed {
        cause: String,
    },
    /// The time budget ran out before the session was started.
    Unreached,
}

/// Why no window was drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowRefusal {
    NotCovered {
        first: SessionDate,
        last: SessionDate,
    },
    TooFewSessions {
        wanted: usize,
        found: usize,
    },
}

impl std::fmt::Display for WindowRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCovered { first, last } => {
                write!(formatter, "the calendar does not cover {first} to {last}")
            }
            Self::TooFewSessions { wanted, found } => write!(
                formatter,
                "the calendar has {found} trading days where {wanted} were wanted"
            ),
        }
    }
}

/// Calendar days drawn per trading day wanted, which with the padding holds the window through any run of holidays.
const CALENDAR_DAYS_PER_SESSION: i64 = 2;
const HOLIDAY_PADDING_DAYS: i64 = 7;

/// The calendar days a window of `sessions` trading days before `today` is drawn from.
pub fn calendar_range(today: SessionDate, sessions: NonZeroUsize) -> (SessionDate, SessionDate) {
    let sessions = i64::try_from(sessions.get()).unwrap_or(i64::MAX / 4);
    let days = sessions * CALENDAR_DAYS_PER_SESSION + HOLIDAY_PADDING_DAYS;
    (
        today.plus_calendar_days(-days),
        today.plus_calendar_days(-1),
    )
}

/// The last `sessions` trading days strictly before `today`, oldest first; today is never owed, since its session
/// may not have closed.
pub fn window(
    calendar: &TradingCalendar,
    today: SessionDate,
    sessions: NonZeroUsize,
) -> Result<Vec<SessionDate>, WindowRefusal> {
    let (first, last) = calendar_range(today, sessions);
    if !calendar.covers(first, last) {
        return Err(WindowRefusal::NotCovered { first, last });
    }
    let trading = calendar.trading_days_in_range(first, last);
    let skip = trading
        .len()
        .checked_sub(sessions.get())
        .ok_or(WindowRefusal::TooFewSessions {
            wanted: sessions.get(),
            found: trading.len(),
        })?;
    Ok(trading[skip..].to_vec())
}

/// The sessions of `window` a series does not hold, oldest first.
pub fn owed(window: &[SessionDate], held: &BTreeSet<SessionDate>) -> Vec<SessionDate> {
    window
        .iter()
        .filter(|session| !held.contains(session))
        .copied()
        .collect()
}

/// What a listing under `leg`'s series holds: the sessions of its own keys, and every path that is not one of them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Held {
    sessions: BTreeSet<SessionDate>,
    unrecognized: Vec<String>,
}

impl Held {
    /// Reads a listing, so an object a sibling series or a stray write left under the prefix is never taken as held.
    pub fn of(leg: Leg, paths: impl IntoIterator<Item = String>) -> Self {
        let mut held = Self::default();
        for path in paths {
            match Key::parse(&path) {
                Ok(key) if key == leg.key(key.session()) => {
                    held.sessions.insert(key.session());
                }
                Ok(_) | Err(_) => held.unrecognized.push(path),
            }
        }
        held
    }

    pub fn sessions(&self) -> &BTreeSet<SessionDate> {
        &self.sessions
    }

    pub fn unrecognized(&self) -> &[String] {
        &self.unrecognized
    }
}

/// Every owed session of every leg with how it ended; complete only when each was written.
pub fn is_complete(outcomes: &BTreeMap<Leg, BTreeMap<SessionDate, SessionOutcome>>) -> bool {
    outcomes
        .values()
        .flat_map(BTreeMap::values)
        .all(|outcome| *outcome == SessionOutcome::Written)
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;
    use crate::common::time::calendar::TradingSession;

    fn date(text: &str) -> SessionDate {
        SessionDate::from_date(text.parse::<NaiveDate>().unwrap())
    }

    /// Weekdays over `[first, last]`, less `holidays`.
    fn calendar(first: &str, last: &str, holidays: &[&str]) -> TradingCalendar {
        let (first, last) = (date(first), date(last));
        let mut sessions = Vec::new();
        let mut day = first;
        while day <= last {
            if !day.is_weekend() && !holidays.contains(&day.to_string().as_str()) {
                sessions.push(
                    TradingSession::new(
                        day,
                        NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                        NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
                    )
                    .unwrap(),
                );
            }
            day = day.plus_calendar_days(1);
        }
        TradingCalendar::new(sessions, first, last).unwrap()
    }

    fn sessions(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).unwrap()
    }

    #[test]
    fn test_the_window_ends_before_today_and_skips_thanksgiving() {
        let calendar = calendar("2026-11-01", "2026-12-31", &["2026-11-26"]);
        let window = window(&calendar, date("2026-11-30"), sessions(5)).unwrap();
        let dates: Vec<String> = window.iter().map(ToString::to_string).collect();
        assert_eq!(
            dates,
            [
                "2026-11-20",
                "2026-11-23",
                "2026-11-24",
                "2026-11-25",
                "2026-11-27"
            ]
        );
    }

    #[test]
    fn test_a_calendar_short_of_the_range_draws_no_window() {
        let calendar = calendar("2026-11-20", "2026-12-31", &[]);
        assert_eq!(
            window(&calendar, date("2026-11-30"), sessions(5)),
            Err(WindowRefusal::NotCovered {
                first: date("2026-11-13"),
                last: date("2026-11-29"),
            })
        );
    }

    #[test]
    fn test_a_range_of_holidays_leaves_too_few_sessions() {
        let closed: Vec<String> = (13..=27).map(|day| format!("2026-11-{day}")).collect();
        let closed: Vec<&str> = closed.iter().map(String::as_str).collect();
        let calendar = calendar("2026-11-01", "2026-12-31", &closed);
        assert_eq!(
            window(&calendar, date("2026-11-30"), sessions(5)),
            Err(WindowRefusal::TooFewSessions {
                wanted: 5,
                found: 0
            })
        );
    }

    #[test]
    fn test_a_listing_holds_only_its_own_series() {
        let paths = [
            "data/equity/bars/provider=massive/origin=fetched/interval=one_day/year=2026/month=09/day=28/data.parquet",
            "data/equity/bars/provider=massive/origin=fetched/interval=one_day/year=2026/month=09/day=29/data.parquet",
            // Another provider's daily bars and a derived series are not this leg's.
            "data/equity/bars/provider=alpaca/origin=fetched/interval=one_day/year=2026/month=09/day=30/data.parquet",
            "data/equity/bars/provider=massive/origin=derived/interval=one_day/year=2026/month=10/day=01/data.parquet",
            "data/equity/bars/provider=massive/origin=fetched/interval=one_day/year=2026/month=10/day=02/data.parquet.tmp",
        ]
        .map(String::from);
        let held = Held::of(Leg::MassiveDailyBars, paths.clone());
        assert_eq!(
            held.sessions(),
            &BTreeSet::from([date("2026-09-28"), date("2026-09-29")])
        );
        assert_eq!(held.unrecognized(), &paths[2..]);
    }

    #[test]
    fn test_only_a_night_of_written_sessions_is_complete() {
        let night = |outcome: SessionOutcome| {
            BTreeMap::from([
                (
                    Leg::MassiveDailyBars,
                    BTreeMap::from([(date("2026-09-29"), SessionOutcome::Written)]),
                ),
                (
                    Leg::AlpacaMinuteBars,
                    BTreeMap::from([
                        (date("2026-09-28"), SessionOutcome::Written),
                        (date("2026-09-29"), outcome),
                    ]),
                ),
            ])
        };
        assert!(is_complete(&night(SessionOutcome::Written)));
        assert!(!is_complete(&night(SessionOutcome::Unreached)));
        assert!(!is_complete(&night(SessionOutcome::Failed {
            cause: "refused".to_string()
        })));
        assert!(is_complete(&BTreeMap::new()));
    }

    #[test]
    fn test_serde_and_strum_agree_on_every_leg() {
        for leg in Leg::iter() {
            let json = serde_json::to_string(&leg).unwrap();
            assert_eq!(json, format!("\"{leg}\""));
            assert_eq!(serde_json::from_str::<Leg>(&json).unwrap(), leg);
        }
    }

    #[test]
    fn test_each_leg_has_its_series() {
        let series: Vec<String> = Leg::iter()
            .map(|leg| leg.key(date("2026-09-29")).series())
            .collect();
        assert_eq!(
            series,
            [
                "data/equity/bars/provider=massive/origin=fetched/interval=one_day/",
                "data/equity/bars/provider=alpaca/origin=fetched/interval=one_minute/",
            ]
        );
    }

    fn any_session() -> impl Strategy<Value = SessionDate> {
        (0_i64..400).prop_map(|days| date("2026-01-01").plus_calendar_days(days))
    }

    proptest! {
        /// The window is exactly the trading days it names: that many, each trading, ascending, all before today,
        /// and no trading day skipped between its first and today.
        #[test]
        fn property_the_window_is_the_last_trading_days_before_today(
            today in any_session(),
            count in 1_usize..40,
            holidays in prop::collection::btree_set(any_session(), 0..30),
        ) {
            let holidays: Vec<String> = holidays.iter().map(ToString::to_string).collect();
            let holidays: Vec<&str> = holidays.iter().map(String::as_str).collect();
            let calendar = calendar("2025-09-01", "2027-03-01", &holidays);
            let window = window(&calendar, today, sessions(count));
            prop_assume!(window.is_ok());
            let window = window.unwrap();
            prop_assert_eq!(window.len(), count);
            prop_assert!(window.windows(2).all(|pair| pair[0] < pair[1]));
            prop_assert!(window.iter().all(|day| calendar.is_trading_day(*day) && *day < today));
            prop_assert_eq!(
                calendar.trading_days_in_range(window[0], today.plus_calendar_days(-1)),
                window
            );
        }

        /// The owed and the held partition the window.
        #[test]
        fn property_owed_and_held_partition_the_window(
            window in prop::collection::btree_set(any_session(), 0..20),
            held in prop::collection::btree_set(any_session(), 0..20),
        ) {
            let window: Vec<SessionDate> = window.into_iter().collect();
            let owed = owed(&window, &held);
            let covered: BTreeSet<SessionDate> = owed
                .iter()
                .copied()
                .chain(window.iter().copied().filter(|session| held.contains(session)))
                .collect();
            prop_assert_eq!(covered, window.iter().copied().collect::<BTreeSet<_>>());
            prop_assert!(owed.iter().all(|session| !held.contains(session)));
            prop_assert!(owed.windows(2).all(|pair| pair[0] < pair[1]));
        }
    }
}
