//! What a nightly run owes the archive: the calendar's sessions in a trailing window, less those already held.
//! A missed night and a week's outage heal by the same difference, so nothing remembers what failed.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::common::market::record::BarInterval;
use crate::common::storage::{Key, KeyRefusal, Origin, Provider, ReferenceTable};
use crate::common::time::calendar::TradingCalendar;
use crate::common::time::{SessionDate, SessionRange};

/// One series the archiver keeps whole, in the order a run works them: the daily bars first, since they are the
/// other legs' symbol list, then the reference snapshots, which are quick, before the tick legs. A tick leg writes
/// one-minute, five-minute and daily bars, and is held by its daily file, which it writes last.
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
    MassiveSecurityDetails,
    MassiveSplits,
    AlpacaSeriesBoundaries,
    AlpacaMinuteBars,
    AlpacaQuotes,
    AlpacaTrades,
}

impl Leg {
    /// The key whose presence means the session is held.
    pub fn key(self, session: SessionDate) -> Key {
        match self {
            Self::MassiveDailyBars => Key::Bars {
                provider: Provider::Massive,
                origin: Origin::Vendor,
                interval: BarInterval::OneDay,
                session,
            },
            Self::AlpacaMinuteBars => Key::Bars {
                provider: Provider::Alpaca,
                origin: Origin::Vendor,
                interval: BarInterval::OneMinute,
                session,
            },
            Self::AlpacaQuotes => Key::Quotes {
                provider: Provider::Alpaca,
                origin: Origin::Derived,
                interval: BarInterval::OneDay,
                session,
            },
            Self::AlpacaTrades => Key::Trades {
                provider: Provider::Alpaca,
                origin: Origin::Derived,
                interval: BarInterval::OneDay,
                session,
            },
            Self::MassiveSecurityDetails => Key::Reference {
                provider: Provider::Massive,
                table: ReferenceTable::SecurityDetails,
                as_of: session,
            },
            Self::MassiveSplits => Key::Reference {
                provider: Provider::Massive,
                table: ReferenceTable::Splits,
                as_of: session,
            },
            Self::AlpacaSeriesBoundaries => Key::Reference {
                provider: Provider::Alpaca,
                table: ReferenceTable::SeriesBoundaries,
                as_of: session,
            },
        }
    }

    /// The sessions this leg writes. A snapshot of a whole table fetched later cannot stand for an earlier day, so a
    /// snapshot leg keeps only the window's last session; security details are kept once a quarter, on the session that
    /// opens the quarter the window ends in, which the vendor answers as of that date however late it is asked, so a
    /// missed one stays owed for the rest of the quarter. `calendar` must reach back to that quarter's first day.
    pub fn keeps(
        self,
        window: &Window,
        calendar: &TradingCalendar,
    ) -> Result<Vec<SessionDate>, KeepsRefusal> {
        match self {
            Self::MassiveDailyBars
            | Self::AlpacaMinuteBars
            | Self::AlpacaQuotes
            | Self::AlpacaTrades => Ok(window.sessions().to_vec()),
            Self::MassiveSplits | Self::AlpacaSeriesBoundaries => Ok(vec![window.last()]),
            Self::MassiveSecurityDetails => {
                quarter_opening(calendar, window.last()).map(|opening| vec![opening])
            }
        }
    }
}

/// Why a leg's sessions could not be read off the calendar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepsRefusal {
    CalendarShort {
        quarter: SessionRange,
        covered: SessionRange,
    },
    NoTradingDay {
        quarter: SessionRange,
    },
}

impl std::fmt::Display for KeepsRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CalendarShort { quarter, covered } => write!(
                formatter,
                "the calendar covers {} to {}, short of the quarter's {} to {}",
                covered.first(),
                covered.last(),
                quarter.first(),
                quarter.last()
            ),
            Self::NoTradingDay { quarter } => write!(
                formatter,
                "the calendar has no trading day from {} to {}",
                quarter.first(),
                quarter.last()
            ),
        }
    }
}

/// The first day of the calendar quarter `session` falls in.
pub fn quarter_start(session: SessionDate) -> SessionDate {
    let date = session.date();
    SessionDate::from_date(
        NaiveDate::from_ymd_opt(date.year(), date.month0() / 3 * 3 + 1, 1)
            .expect("a quarter starts on a real date"),
    )
}

/// The first trading day of `session`'s quarter on or before `session`, refused when the calendar cannot say.
pub fn quarter_opening(
    calendar: &TradingCalendar,
    session: SessionDate,
) -> Result<SessionDate, KeepsRefusal> {
    let quarter = SessionRange::single(session).reaching_back_to(quarter_start(session));
    match calendar.covers(quarter) {
        true => calendar
            .trading_days_in_range(quarter)
            .first()
            .copied()
            .ok_or(KeepsRefusal::NoTradingDay { quarter }),
        false => Err(KeepsRefusal::CalendarShort {
            quarter,
            covered: calendar.range(),
        }),
    }
}

/// What kind of failure kept an owed partition unwritten.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PartitionFailureKind {
    Fetch,
    NoRows,
    NotInCalendar,
    Encode,
    Decode,
    Archive,
    Vanished,
    NoSymbols,
    Fold,
    /// A write the partition needed went unrecorded, which stops the run.
    Journal,
}

/// How one owed session ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum SessionOutcome {
    Written,
    /// `failure` is the kind to group by; `cause` is the failure's text.
    Failed {
        failure: PartitionFailureKind,
        cause: String,
    },
    /// The time budget ran out before the session was started.
    Unreached,
}

/// Why no window was drawn, or read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowRefusal {
    NotCovered {
        range: SessionRange,
    },
    TooFewSessions {
        wanted: usize,
        found: usize,
    },
    Empty,
    NotAscending {
        earlier: SessionDate,
        later: SessionDate,
    },
}

impl std::fmt::Display for WindowRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotCovered { range } => write!(
                formatter,
                "the calendar does not cover {} to {}",
                range.first(),
                range.last()
            ),
            Self::TooFewSessions { wanted, found } => write!(
                formatter,
                "the calendar has {found} trading days where {wanted} were wanted"
            ),
            Self::Empty => write!(formatter, "the window holds no session"),
            Self::NotAscending { earlier, later } => {
                write!(formatter, "the window lists {earlier} after {later}")
            }
        }
    }
}

/// The trading days a heal covers, oldest first: ascending and never empty, and each a trading day before today when
/// drawn by [`window`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<SessionDate>", into = "Vec<SessionDate>")]
pub struct Window(Vec<SessionDate>);

impl TryFrom<Vec<SessionDate>> for Window {
    type Error = WindowRefusal;

    fn try_from(sessions: Vec<SessionDate>) -> Result<Self, Self::Error> {
        if sessions.is_empty() {
            return Err(WindowRefusal::Empty);
        }
        if let Some(pair) = sessions.windows(2).find(|pair| pair[0] >= pair[1]) {
            return Err(WindowRefusal::NotAscending {
                earlier: pair[1],
                later: pair[0],
            });
        }
        Ok(Self(sessions))
    }
}

impl From<Window> for Vec<SessionDate> {
    fn from(window: Window) -> Self {
        window.0
    }
}

impl Window {
    pub fn sessions(&self) -> &[SessionDate] {
        &self.0
    }

    /// The latest session.
    pub fn last(&self) -> SessionDate {
        *self.0.last().expect("a window is never empty")
    }
}

/// Calendar days drawn per trading day wanted, which with the padding holds the window through any run of holidays.
const CALENDAR_DAYS_PER_SESSION: i64 = 2;
const HOLIDAY_PADDING_DAYS: i64 = 7;

/// The calendar days a window of `sessions` trading days before `today` is drawn from.
pub fn calendar_range(today: SessionDate, sessions: NonZeroUsize) -> SessionRange {
    let sessions = i64::try_from(sessions.get()).unwrap_or(i64::MAX / 4);
    let days = sessions * CALENDAR_DAYS_PER_SESSION + HOLIDAY_PADDING_DAYS;
    SessionRange::single(today.plus_calendar_days(-1))
        .reaching_back_to(today.plus_calendar_days(-days))
}

/// The calendar days a run fetches: `calendar_range` reaching back to its first day's quarter start, which no window
/// drawn from it can end before, so [`Leg::keeps`] can always find that quarter's opening.
pub fn fetched_range(today: SessionDate, sessions: NonZeroUsize) -> SessionRange {
    let range = calendar_range(today, sessions);
    range.reaching_back_to(quarter_start(range.first()))
}

/// The last `sessions` trading days strictly before `today`, oldest first; today is never owed, since its session
/// may not have closed.
pub fn window(
    calendar: &TradingCalendar,
    today: SessionDate,
    sessions: NonZeroUsize,
) -> Result<Window, WindowRefusal> {
    let range = calendar_range(today, sessions);
    if !calendar.covers(range) {
        return Err(WindowRefusal::NotCovered { range });
    }
    let trading = calendar.trading_days_in_range(range);
    let skip = trading
        .len()
        .checked_sub(sessions.get())
        .ok_or(WindowRefusal::TooFewSessions {
            wanted: sessions.get(),
            found: trading.len(),
        })?;
    Window::try_from(trading[skip..].to_vec())
}

/// The sessions of `window` a series does not hold, oldest first.
pub fn owed(window: &[SessionDate], held: &BTreeSet<SessionDate>) -> Vec<SessionDate> {
    window
        .iter()
        .filter(|session| !held.contains(session))
        .copied()
        .collect()
}

/// Why a path under a leg's series was not taken as one of its sessions.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Unrecognized {
    /// Not a path any key writes.
    NotAKey,
    /// A key, but of another series, as a sibling sharing the prefix writes.
    AnotherSeries,
}

/// What a listing under `leg`'s series holds: the sessions of its own keys, and every path that is not one of them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Held {
    sessions: BTreeSet<SessionDate>,
    unrecognized: BTreeMap<String, Unrecognized>,
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
                Ok(_) => {
                    held.unrecognized.insert(path, Unrecognized::AnotherSeries);
                }
                Err(KeyRefusal::Unrecognized { .. }) => {
                    held.unrecognized.insert(path, Unrecognized::NotAKey);
                }
            }
        }
        held
    }

    pub fn sessions(&self) -> &BTreeSet<SessionDate> {
        &self.sessions
    }

    pub fn unrecognized(&self) -> &BTreeMap<String, Unrecognized> {
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
        TradingCalendar::new(sessions, SessionRange::new(first, last).unwrap()).unwrap()
    }

    fn sessions(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).unwrap()
    }

    #[test]
    fn test_the_window_ends_before_today_and_skips_thanksgiving() {
        let calendar = calendar("2026-11-01", "2026-12-31", &["2026-11-26"]);
        let window = window(&calendar, date("2026-11-30"), sessions(5)).unwrap();
        let dates: Vec<String> = window.sessions().iter().map(ToString::to_string).collect();
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
                range: SessionRange::new(date("2026-11-13"), date("2026-11-29")).unwrap(),
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
            "data/equity/stage=parsed/bars/provider=massive/origin=vendor/interval=one_day/year=2026/month=09/day=28/data.parquet",
            "data/equity/stage=parsed/bars/provider=massive/origin=vendor/interval=one_day/year=2026/month=09/day=29/data.parquet",
            // Another provider's daily bars and a derived series are not this leg's.
            "data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_day/year=2026/month=09/day=30/data.parquet",
            "data/equity/stage=parsed/bars/provider=massive/origin=derived/interval=one_day/year=2026/month=10/day=01/data.parquet",
            "data/equity/stage=parsed/bars/provider=massive/origin=vendor/interval=one_day/year=2026/month=10/day=02/data.parquet.tmp",
        ]
        .map(String::from);
        let held = Held::of(Leg::MassiveDailyBars, paths.clone());
        assert_eq!(
            held.sessions(),
            &BTreeSet::from([date("2026-09-28"), date("2026-09-29")])
        );
        assert_eq!(
            held.unrecognized(),
            &BTreeMap::from([
                (paths[2].clone(), Unrecognized::AnotherSeries),
                (paths[3].clone(), Unrecognized::AnotherSeries),
                (paths[4].clone(), Unrecognized::NotAKey),
            ])
        );
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
            failure: PartitionFailureKind::Fetch,
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
    fn test_serde_and_strum_agree_on_every_failure_kind() {
        let names: Vec<String> = PartitionFailureKind::iter()
            .map(|kind| {
                let json = serde_json::to_string(&kind).unwrap();
                assert_eq!(json, format!("\"{kind}\""));
                assert_eq!(kind.to_string().parse::<PartitionFailureKind>(), Ok(kind));
                assert_eq!(
                    serde_json::from_str::<PartitionFailureKind>(&json).unwrap(),
                    kind
                );
                json
            })
            .collect();
        assert_eq!(
            names,
            [
                r#""fetch""#,
                r#""no_rows""#,
                r#""not_in_calendar""#,
                r#""encode""#,
                r#""decode""#,
                r#""archive""#,
                r#""vanished""#,
                r#""no_symbols""#,
                r#""fold""#,
                r#""journal""#,
            ]
        );
    }

    #[test]
    fn test_serde_and_strum_agree_on_every_unrecognized_cause() {
        let names: Vec<String> = Unrecognized::iter()
            .map(|cause| {
                let json = serde_json::to_string(&cause).unwrap();
                assert_eq!(json, format!("\"{cause}\""));
                assert_eq!(cause.to_string().parse::<Unrecognized>(), Ok(cause));
                assert_eq!(serde_json::from_str::<Unrecognized>(&json).unwrap(), cause);
                json
            })
            .collect();
        assert_eq!(names, [r#""not_a_key""#, r#""another_series""#]);
    }

    #[test]
    fn test_each_leg_has_its_series() {
        let series: Vec<String> = Leg::iter()
            .map(|leg| leg.key(date("2026-09-29")).series())
            .collect();
        assert_eq!(
            series,
            [
                "data/equity/stage=parsed/bars/provider=massive/origin=vendor/interval=one_day/",
                "data/equity/stage=parsed/reference/provider=massive/table=security_details/",
                "data/equity/stage=parsed/reference/provider=massive/table=splits/",
                "data/equity/stage=parsed/reference/provider=alpaca/table=series_boundaries/",
                "data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_minute/",
                "data/equity/stage=parsed/quotes/provider=alpaca/origin=derived/interval=one_day/",
                "data/equity/stage=parsed/trades/provider=alpaca/origin=derived/interval=one_day/",
            ]
        );
    }

    #[test]
    fn test_each_leg_keeps_its_own_sessions_of_the_window() {
        // New Year's Day 2027 is a Friday holiday, so the first quarter opens on Monday the 4th.
        let winter = calendar("2026-12-01", "2027-01-31", &["2026-12-25", "2027-01-01"]);
        let opening = window(&winter, date("2027-01-07"), sessions(5)).unwrap();
        let kept = |leg: Leg| -> Vec<String> {
            leg.keeps(&opening, &winter)
                .unwrap()
                .iter()
                .map(ToString::to_string)
                .collect()
        };
        assert_eq!(
            kept(Leg::AlpacaTrades),
            [
                "2026-12-30",
                "2026-12-31",
                "2027-01-04",
                "2027-01-05",
                "2027-01-06"
            ]
        );
        assert_eq!(kept(Leg::MassiveSplits), ["2027-01-06"]);
        assert_eq!(kept(Leg::AlpacaSeriesBoundaries), ["2027-01-06"]);
        assert_eq!(kept(Leg::MassiveSecurityDetails), ["2027-01-04"]);
        // Weeks later the quarter's opening is long out of the window and still owed until it is held.
        let later = window(&winter, date("2027-01-28"), sessions(5)).unwrap();
        assert_eq!(
            Leg::MassiveSecurityDetails.keeps(&later, &winter),
            Ok(vec![date("2027-01-04")])
        );
        assert_eq!(quarter_start(date("2027-03-31")), date("2027-01-01"));
    }

    /// On the first trading day of a quarter that opens on a holiday, the window still ends in the quarter before, so
    /// the fetched calendar must reach back to that quarter's start or the run is refused.
    #[test]
    fn test_the_fetched_range_reaches_the_quarter_the_window_ends_in() {
        let today = date("2027-01-04");
        let range = fetched_range(today, sessions(5));
        assert_eq!(
            range,
            SessionRange::new(date("2026-10-01"), date("2027-01-03")).unwrap()
        );
        let fetched = calendar("2026-10-01", "2027-01-03", &["2026-12-25", "2027-01-01"]);
        let drawn = window(&fetched, today, sessions(5)).unwrap();
        assert_eq!(drawn.last(), date("2026-12-31"));
        assert_eq!(
            Leg::MassiveSecurityDetails.keeps(&drawn, &fetched),
            Ok(vec![date("2026-10-01")])
        );
    }

    /// A calendar that starts after the quarter does cannot name its opening, so the leg is refused rather than owed
    /// nothing.
    #[test]
    fn test_a_calendar_short_of_the_quarter_refuses_the_security_details_leg() {
        let short = calendar("2027-01-02", "2027-01-31", &[]);
        let window = window(&short, date("2027-01-28"), sessions(5)).unwrap();
        assert_eq!(
            Leg::MassiveSecurityDetails.keeps(&window, &short),
            Err(KeepsRefusal::CalendarShort {
                quarter: SessionRange::new(date("2027-01-01"), date("2027-01-27")).unwrap(),
                covered: SessionRange::new(date("2027-01-02"), date("2027-01-31")).unwrap(),
            })
        );
        assert_eq!(
            Leg::MassiveSplits.keeps(&window, &short),
            Ok(vec![date("2027-01-27")])
        );
        // A quarter the calendar covers but where nothing trades has no opening either.
        let closed = calendar("2027-01-01", "2027-01-31", &["2027-01-01"]);
        assert_eq!(
            quarter_opening(&closed, date("2027-01-01")),
            Err(KeepsRefusal::NoTradingDay {
                quarter: SessionRange::single(date("2027-01-01")),
            })
        );
    }

    #[test]
    fn test_a_window_is_ascending_and_never_empty() {
        assert_eq!(Window::try_from(vec![]), Err(WindowRefusal::Empty));
        for repeated_or_reversed in ["2026-09-29", "2026-09-28"] {
            assert_eq!(
                Window::try_from(vec![date("2026-09-29"), date(repeated_or_reversed)]),
                Err(WindowRefusal::NotAscending {
                    earlier: date(repeated_or_reversed),
                    later: date("2026-09-29"),
                })
            );
        }
        let window = Window::try_from(vec![date("2026-09-28"), date("2026-09-29")]).unwrap();
        assert_eq!(window.last(), date("2026-09-29"));
        let json = serde_json::to_string(&window).unwrap();
        assert_eq!(json, r#"["2026-09-28","2026-09-29"]"#);
        assert_eq!(serde_json::from_str::<Window>(&json).unwrap(), window);
        assert!(serde_json::from_str::<Window>(r#"["2026-09-29","2026-09-28"]"#).is_err());
        assert!(serde_json::from_str::<Window>("[]").is_err());
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
            let window = window.unwrap().sessions().to_vec();
            prop_assert_eq!(window.len(), count);
            prop_assert!(window.windows(2).all(|pair| pair[0] < pair[1]));
            prop_assert!(window.iter().all(|day| calendar.is_trading_day(*day) && *day < today));
            prop_assert_eq!(
                calendar.trading_days_in_range(SessionRange::new(window[0], today.plus_calendar_days(-1)).unwrap()),
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
