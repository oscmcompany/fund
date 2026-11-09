//! What a nightly run owes the archive: the calendar's sessions in a trailing window, less those already held.
//! A missed night and a week's outage heal by the same difference, so nothing remembers what failed.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::common::market::record::BarInterval;
use crate::common::storage::{
    Key, KeyRefusal, Provider, QuotesKey, ReferenceKey, ReferenceTable, TradesKey,
};
use crate::common::time::calendar::TradingCalendar;
use crate::common::time::{SessionDate, SessionRange};

/// One series the archiver keeps whole, in the order a run works them: the traded symbols and reference snapshots,
/// which are quick, before the tick legs. A tick leg writes raw pages, then one-minute, five-minute and daily bars, and
/// is held by its raw pages and its daily file.
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
    /// A snapshot only of the night it runs, so a missed night is lost; first, so a run that exhausts its budget still
    /// takes it.
    AlpacaBorrowFlags,
    /// The session's grouped daily response stored raw, from the endpoint the tick legs take their symbols from.
    MassiveTradedSymbols,
    MassiveSecurityDetails,
    /// After the security details it fills, since it reads every snapshot of them up to its own session.
    SecFiledIndustryCodes,
    MassiveSplits,
    AlpacaSeriesBoundaries,
    AlpacaQuotes,
    AlpacaTrades,
}

impl Leg {
    /// The keys whose presence together means the session is held.
    pub fn keys(self, session: SessionDate) -> Vec<Key> {
        match self {
            Self::MassiveTradedSymbols => vec![Key::RawReference(massive_traded_symbols(session))],
            Self::AlpacaQuotes => vec![
                alpaca_quotes(session).into(),
                Key::RawQuotes {
                    provider: Provider::Alpaca,
                    session,
                },
            ],
            Self::AlpacaTrades => vec![
                alpaca_trades(session).into(),
                Key::RawTrades {
                    provider: Provider::Alpaca,
                    session,
                },
            ],
            Self::MassiveSecurityDetails => snapshot(massive_security_details(session)),
            Self::SecFiledIndustryCodes => snapshot(sec_filed_industry_codes(session)),
            Self::MassiveSplits => snapshot(massive_splits(session)),
            Self::AlpacaSeriesBoundaries => snapshot(alpaca_series_boundaries(session)),
            Self::AlpacaBorrowFlags => snapshot(alpaca_borrow_flags(session)),
        }
    }

    /// The sessions this leg writes: every window session for a tick leg or the traded symbols, the window's last for
    /// a snapshot, and the opening session of the last session's quarter for security details and filed industry codes.
    /// A snapshot of a whole table stands only for the day it was fetched; `calendar` must reach back to that quarter's
    /// first day.
    pub fn keeps(
        self,
        window: &Window,
        calendar: &TradingCalendar,
    ) -> Result<Vec<SessionDate>, KeepsRefusal> {
        match self {
            Self::MassiveTradedSymbols | Self::AlpacaQuotes | Self::AlpacaTrades => {
                Ok(window.sessions().to_vec())
            }
            Self::MassiveSplits | Self::AlpacaSeriesBoundaries | Self::AlpacaBorrowFlags => {
                Ok(vec![window.last()])
            }
            Self::MassiveSecurityDetails | Self::SecFiledIndustryCodes => {
                quarter_opening(calendar, window.last()).map(|opening| vec![opening])
            }
        }
    }
}

/// A reference snapshot and the raw responses fetched for it.
fn snapshot(key: ReferenceKey) -> Vec<Key> {
    vec![key.into(), Key::RawReference(key)]
}

/// Alpaca's daily quote bars; the leg writes the series' other intervals beside them.
pub fn alpaca_quotes(session: SessionDate) -> QuotesKey {
    QuotesKey::new(Provider::Alpaca, BarInterval::OneDay, session)
}

/// Alpaca's daily trade bars; the leg writes the series' other intervals beside them.
pub fn alpaca_trades(session: SessionDate) -> TradesKey {
    TradesKey::new(Provider::Alpaca, BarInterval::OneDay, session)
}

pub fn massive_security_details(session: SessionDate) -> ReferenceKey {
    ReferenceKey::new(Provider::Massive, ReferenceTable::SecurityDetails, session)
}

pub fn sec_filed_industry_codes(session: SessionDate) -> ReferenceKey {
    ReferenceKey::new(Provider::Sec, ReferenceTable::FiledIndustryCodes, session)
}

/// The grouped daily response kept only for the symbols it names; it has no parsed snapshot.
pub fn massive_traded_symbols(session: SessionDate) -> ReferenceKey {
    ReferenceKey::new(Provider::Massive, ReferenceTable::TradedSymbols, session)
}

pub fn massive_splits(session: SessionDate) -> ReferenceKey {
    ReferenceKey::new(Provider::Massive, ReferenceTable::Splits, session)
}

pub fn alpaca_series_boundaries(session: SessionDate) -> ReferenceKey {
    ReferenceKey::new(Provider::Alpaca, ReferenceTable::SeriesBoundaries, session)
}

pub fn alpaca_borrow_flags(session: SessionDate) -> ReferenceKey {
    ReferenceKey::new(Provider::Alpaca, ReferenceTable::BorrowFlags, session)
}

/// Why a listing that answers only for now cannot be dated by a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Lapse {
    /// The fetch reached the next session's open.
    #[error("the next session opened at {next_open}")]
    Opened { next_open: DateTime<Utc> },
    /// The calendar names no session after it, so whether one has opened cannot be told.
    #[error("the calendar names no later session through {covered_through}")]
    Unscheduled { covered_through: SessionDate },
}

/// Refuses a listing fetched at `fetched_at` as `session`'s once the next session has opened, or when the calendar
/// cannot say when that is.
pub fn lapsed(
    calendar: &TradingCalendar,
    session: SessionDate,
    fetched_at: DateTime<Utc>,
) -> Result<(), Lapse> {
    let next_open = calendar
        .next_trading_day(session)
        .and_then(|next| calendar.session(next))
        .map(|next| next.hours().0)
        .ok_or(Lapse::Unscheduled {
            covered_through: calendar.range().last(),
        })?;
    match fetched_at >= next_open {
        true => Err(Lapse::Opened { next_open }),
        false => Ok(()),
    }
}

/// Why a leg's sessions could not be read off the calendar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeepsRefusal {
    #[error("the calendar covers {} to {}, short of the quarter's {} to {}", .covered.first(), .covered.last(), .quarter.first(), .quarter.last())]
    CalendarShort {
        quarter: SessionRange,
        covered: SessionRange,
    },
    #[error("the calendar has no trading day from {} to {}", .quarter.first(), .quarter.last())]
    NoTradingDay { quarter: SessionRange },
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
    Bucket,
    Vanished,
    NoSymbols,
    /// A partition the leg reads was not yet written.
    Unwritten,
    /// A client the leg needs was not configured.
    Unconfigured,
    Fold,
    /// The fetched pages could not be stored raw.
    Tee,
    /// A write the partition needed went unrecorded, which stops the run.
    Journal,
    /// A listing that answers only for now was fetched after the next session opened, so it no longer describes the
    /// close it would be dated by.
    Lapsed,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WindowRefusal {
    #[error("the calendar does not cover {} to {}", .range.first(), .range.last())]
    NotCovered { range: SessionRange },
    #[error("the calendar has {found} trading days where {wanted} were wanted")]
    TooFewSessions { wanted: usize, found: usize },
    #[error("the window holds no session")]
    Empty,
    #[error("the window lists {earlier} after {later}")]
    NotAscending {
        earlier: SessionDate,
        later: SessionDate,
    },
}

/// The trading days a heal covers, oldest first: ascending and never empty, and each a trading day before today when
/// drawn by [`window`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<SessionDate>", into = "Vec<SessionDate>")]
pub struct Window(Vec<SessionDate>);

impl TryFrom<Vec<SessionDate>> for Window {
    type Error = WindowRefusal;

    fn try_from(sessions: Vec<SessionDate>) -> Result<Self, Self::Error> {
        Self::new(sessions)
    }
}

impl From<Window> for Vec<SessionDate> {
    fn from(window: Window) -> Self {
        window.0
    }
}

impl Window {
    pub fn new(sessions: Vec<SessionDate>) -> Result<Self, WindowRefusal> {
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

/// Calendar days past `today` a run fetches, so [`lapsed`] can find the session after the window's last.
const NEXT_SESSION_REACH_DAYS: i64 = 10;

/// The calendar days a run fetches: from the start of the quarter `calendar_range` begins in, so [`Leg::keeps`] can
/// always find a window's quarter opening, through `NEXT_SESSION_REACH_DAYS` past `today`.
pub fn fetched_range(today: SessionDate, sessions: NonZeroUsize) -> SessionRange {
    let range = calendar_range(today, sessions);
    SessionRange::single(today.plus_calendar_days(NEXT_SESSION_REACH_DAYS))
        .reaching_back_to(quarter_start(range.first()))
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
    Window::new(trading[skip..].to_vec())
}

/// The sessions of `window` a series does not hold, oldest first.
pub fn owed(window: &[SessionDate], held: &BTreeSet<SessionDate>) -> Vec<SessionDate> {
    window
        .iter()
        .filter(|session| !held.contains(session))
        .copied()
        .collect()
}

/// One archive series' sessions against the trading calendar: the span it holds, how many sessions, and every
/// trading session inside the span it lacks or held session the calendar does not trade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coverage {
    first: SessionDate,
    last: SessionDate,
    held: usize,
    gaps: Vec<SessionDate>,
    off_calendar: Vec<SessionDate>,
}

impl Coverage {
    /// Refused when nothing is held or `calendar` does not reach across what is.
    pub fn of(
        held: &BTreeSet<SessionDate>,
        calendar: &TradingCalendar,
    ) -> Result<Self, CoverageRefusal> {
        let (Some(first), Some(last)) = (held.first(), held.last()) else {
            return Err(CoverageRefusal::Empty);
        };
        let range = SessionRange::new(*first, *last).expect("a set's first is not after its last");
        if !calendar.covers(range) {
            return Err(CoverageRefusal::CalendarShort { range });
        }
        let trading = calendar.trading_days_in_range(range);
        Ok(Self {
            first: *first,
            last: *last,
            held: held.len(),
            gaps: owed(&trading, held),
            off_calendar: held
                .iter()
                .filter(|session| !calendar.is_trading_day(**session))
                .copied()
                .collect(),
        })
    }

    pub fn last(&self) -> SessionDate {
        self.last
    }

    /// Whether every trading session in the span is held and nothing else is.
    pub fn is_whole(&self) -> bool {
        self.gaps.is_empty() && self.off_calendar.is_empty()
    }
}

/// Why a series' coverage was not measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CoverageRefusal {
    #[error("the series holds nothing")]
    Empty,
    #[error("the calendar does not cover {} to {}", .range.first(), .range.last())]
    CalendarShort { range: SessionRange },
}

/// The market-data archive's paths grouped by dated series, reference snapshots set aside and anything not a key
/// counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesListing {
    series: BTreeMap<String, BTreeSet<SessionDate>>,
    parsed_by_raw: BTreeMap<String, BTreeSet<SessionDate>>,
    unrecognized: u64,
}

impl SeriesListing {
    pub fn of<'a>(paths: impl IntoIterator<Item = &'a str>) -> Self {
        let mut series: BTreeMap<String, BTreeSet<SessionDate>> = BTreeMap::new();
        let mut parsed_by_raw: BTreeMap<String, BTreeSet<SessionDate>> = BTreeMap::new();
        let mut unrecognized = 0_u64;
        for path in paths {
            let key = match path.parse::<Key>() {
                Ok(key) => key,
                Err(_) => {
                    unrecognized += 1;
                    continue;
                }
            };
            let raw = match &key {
                Key::Reference(_) | Key::RawReference(_) => continue,
                Key::Quotes(parsed) => Some(Key::RawQuotes {
                    provider: parsed.provider(),
                    session: parsed.session(),
                }),
                Key::Trades(parsed) => Some(Key::RawTrades {
                    provider: parsed.provider(),
                    session: parsed.session(),
                }),
                Key::RawQuotes { .. } | Key::RawTrades { .. } | Key::Journal(_) | Key::Logs(_) => {
                    None
                }
            };
            if let Some(raw) = raw {
                parsed_by_raw
                    .entry(raw.series().to_string())
                    .or_default()
                    .insert(raw.session());
            }
            series
                .entry(key.series().to_string())
                .or_default()
                .insert(key.session());
        }
        Self {
            series,
            parsed_by_raw,
            unrecognized,
        }
    }

    pub fn series(&self) -> &BTreeMap<String, BTreeSet<SessionDate>> {
        &self.series
    }

    /// Each raw series' missing sessions that a parsed series of its family and provider holds, at any interval;
    /// a raw series missing none is left out.
    pub fn raw_uncovered(&self) -> BTreeMap<String, Vec<SessionDate>> {
        self.parsed_by_raw
            .iter()
            .filter_map(|(raw, parsed)| {
                let missing: Vec<SessionDate> = match self.series.get(raw) {
                    Some(held) => parsed.difference(held).copied().collect(),
                    None => parsed.iter().copied().collect(),
                };
                (!missing.is_empty()).then(|| (raw.clone(), missing))
            })
            .collect()
    }

    pub fn unrecognized(&self) -> u64 {
        self.unrecognized
    }

    /// The sessions a calendar must cover, from the earliest held to `today`; `None` when nothing is held. A session
    /// held after `today` lies outside it, for `Coverage::of` to refuse.
    pub fn reach(&self, today: SessionDate) -> Option<SessionRange> {
        let first = *self.series.values().filter_map(BTreeSet::first).min()?;
        Some(
            SessionRange::new(first, first.max(today))
                .expect("the reach ends no earlier than it starts"),
        )
    }
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
    NotAKey,
    /// A key, but of another series, as a sibling sharing the prefix writes.
    AnotherSeries,
}

/// What a listing under `leg`'s series holds: the sessions with every one of its keys, and every path that is none of
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Held {
    sessions: BTreeSet<SessionDate>,
    unrecognized: BTreeMap<String, Unrecognized>,
}

impl Held {
    /// Reads a listing of the leg's series, so an object a sibling series or a stray write left under a prefix is
    /// never taken as held, nor a session holding only some of its keys.
    pub fn of(leg: Leg, paths: impl IntoIterator<Item = String>) -> Self {
        let mut held = Self::default();
        let mut found: BTreeMap<SessionDate, Vec<Key>> = BTreeMap::new();
        for path in paths {
            match path.parse::<Key>() {
                Ok(key) if leg.keys(key.session()).contains(&key) => {
                    found.entry(key.session()).or_default().push(key);
                }
                Ok(_) => {
                    held.unrecognized.insert(path, Unrecognized::AnotherSeries);
                }
                Err(KeyRefusal::Unrecognized { .. }) => {
                    held.unrecognized.insert(path, Unrecognized::NotAKey);
                }
            }
        }
        held.sessions = found
            .into_iter()
            .filter(|(session, keys)| leg.keys(*session).iter().all(|key| keys.contains(key)))
            .map(|(session, _)| session)
            .collect();
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

    /// Parsed and raw paths group under their series, a reference snapshot is set aside and a stray path is counted,
    /// and the calendar must reach from the earliest held session to today.
    #[test]
    fn test_a_listing_groups_dated_series_and_reaches_from_the_earliest() {
        let minutes = "data/equity/stage=parsed/trades/provider=alpaca/interval=one_minute/";
        let raw_quotes = "data/equity/stage=raw/quotes/provider=massive/";
        let listing = SeriesListing::of([
            &format!("{minutes}year=2026/month=10/day=08/data.parquet") as &str,
            &format!("{minutes}year=2026/month=10/day=06/data.parquet"),
            &format!("{raw_quotes}year=2026/month=10/day=05/data.csv.gz"),
            "data/equity/stage=parsed/reference/provider=massive/table=splits/as_of=2026-10-01/data.parquet",
            "data/equity/stage=parsed/bars/notes.txt",
        ]);
        assert_eq!(
            listing.series().keys().collect::<Vec<_>>(),
            [minutes, raw_quotes]
        );
        assert_eq!(
            listing.series()[minutes],
            BTreeSet::from([date("2026-10-06"), date("2026-10-08")])
        );
        assert_eq!(
            listing.series()[raw_quotes],
            BTreeSet::from([date("2026-10-05")])
        );
        assert_eq!(listing.unrecognized(), 1);
        let reach = |today| {
            listing
                .reach(date(today))
                .map(|range| (range.first(), range.last()))
        };
        assert_eq!(
            reach("2026-10-09"),
            Some((date("2026-10-05"), date("2026-10-09")))
        );
        assert_eq!(
            reach("2026-10-01"),
            Some((date("2026-10-05"), date("2026-10-05")))
        );
        assert_eq!(SeriesListing::of([]).reach(date("2026-10-09")), None);
    }

    /// A raw series lacks the sessions its parsed counterpart holds at any interval and no others: Alpaca trades are
    /// missing 10-06 (one-minute only) and 10-07 (five-minute only), Alpaca quotes are whole, and Massive trades
    /// hold no raw series at all, so every parsed session of theirs is missing.
    #[test]
    fn test_a_listing_names_the_parsed_sessions_its_raw_series_lacks() {
        let parsed = "data/equity/stage=parsed";
        let raw = "data/equity/stage=raw";
        let day = |series: &str, day: &str, file: &str| {
            format!("{series}year=2026/month=10/day={day}/{file}")
        };
        let paths = [
            day(
                &format!("{parsed}/trades/provider=alpaca/interval=one_minute/"),
                "06",
                "data.parquet",
            ),
            day(
                &format!("{parsed}/trades/provider=alpaca/interval=one_minute/"),
                "08",
                "data.parquet",
            ),
            day(
                &format!("{parsed}/trades/provider=alpaca/interval=five_minute/"),
                "07",
                "data.parquet",
            ),
            day(
                &format!("{raw}/trades/provider=alpaca/"),
                "08",
                "data.jsonl.gz",
            ),
            day(
                &format!("{raw}/trades/provider=alpaca/"),
                "05",
                "data.jsonl.gz",
            ),
            day(
                &format!("{parsed}/quotes/provider=alpaca/interval=one_minute/"),
                "08",
                "data.parquet",
            ),
            day(
                &format!("{raw}/quotes/provider=alpaca/"),
                "08",
                "data.jsonl.gz",
            ),
            day(
                &format!("{parsed}/trades/provider=massive/interval=one_minute/"),
                "02",
                "data.parquet",
            ),
        ];
        let listing = SeriesListing::of(paths.iter().map(String::as_str));
        assert_eq!(
            listing.raw_uncovered(),
            BTreeMap::from([
                (
                    "data/equity/stage=raw/trades/provider=alpaca/".to_string(),
                    vec![date("2026-10-06"), date("2026-10-07")]
                ),
                (
                    "data/equity/stage=raw/trades/provider=massive/".to_string(),
                    vec![date("2026-10-02")]
                ),
            ])
        );
    }

    /// Over 2026-11-23 to 11-30 with Thanksgiving off: a held span missing the 24th has that one gap, a Saturday held
    /// is off the calendar, and a span the calendar does not reach is refused.
    #[test]
    fn test_coverage_names_each_gap_inside_the_held_span() {
        let calendar = calendar("2026-11-23", "2026-11-30", &["2026-11-26"]);
        let held = |dates: &[&str]| dates.iter().map(|text| date(text)).collect::<BTreeSet<_>>();
        let coverage = Coverage::of(
            &held(&["2026-11-23", "2026-11-25", "2026-11-27", "2026-11-28"]),
            &calendar,
        )
        .unwrap();
        assert_eq!(
            coverage,
            Coverage {
                first: date("2026-11-23"),
                last: date("2026-11-28"),
                held: 4,
                gaps: vec![date("2026-11-24")],
                off_calendar: vec![date("2026-11-28")],
            }
        );
        assert!(!coverage.is_whole());
        let whole = Coverage::of(
            &held(&["2026-11-24", "2026-11-25", "2026-11-27"]),
            &calendar,
        )
        .unwrap();
        assert!(whole.is_whole());
        assert_eq!(
            Coverage::of(&BTreeSet::new(), &calendar),
            Err(CoverageRefusal::Empty)
        );
        assert!(matches!(
            Coverage::of(&held(&["2026-11-27", "2026-12-01"]), &calendar),
            Err(CoverageRefusal::CalendarShort { .. })
        ));
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
            "data/equity/stage=raw/reference/provider=massive/table=traded_symbols/as_of=2026-09-28/data.jsonl.gz",
            "data/equity/stage=raw/reference/provider=massive/table=traded_symbols/as_of=2026-09-29/data.jsonl.gz",
            // Another table's raw responses and another provider's are not this leg's.
            "data/equity/stage=raw/reference/provider=massive/table=splits/as_of=2026-09-30/data.jsonl.gz",
            "data/equity/stage=raw/reference/provider=alpaca/table=traded_symbols/as_of=2026-10-01/data.jsonl.gz",
            "data/equity/stage=raw/reference/provider=massive/table=traded_symbols/as_of=2026-10-02/data.jsonl.gz.tmp",
        ]
        .map(String::from);
        let held = Held::of(Leg::MassiveTradedSymbols, paths.clone());
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
    fn test_a_tick_session_is_held_only_beside_its_raw_pages() {
        let parsed = |day: &str| {
            format!(
                "data/equity/stage=parsed/quotes/provider=alpaca/interval=one_day/year=2026/month=10/day={day}/data.parquet"
            )
        };
        let raw = |day: &str| {
            format!(
                "data/equity/stage=raw/quotes/provider=alpaca/year=2026/month=10/day={day}/data.jsonl.gz"
            )
        };
        let held = Held::of(
            Leg::AlpacaQuotes,
            [parsed("05"), raw("05"), parsed("06"), raw("07")],
        );
        assert_eq!(held.sessions(), &BTreeSet::from([date("2026-10-05")]));
        assert!(held.unrecognized().is_empty());
    }

    #[test]
    fn test_only_a_night_of_written_sessions_is_complete() {
        let night = |outcome: SessionOutcome| {
            BTreeMap::from([
                (
                    Leg::MassiveTradedSymbols,
                    BTreeMap::from([(date("2026-09-29"), SessionOutcome::Written)]),
                ),
                (
                    Leg::AlpacaTrades,
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
        let names: Vec<String> = Leg::iter()
            .map(|leg| {
                let json = serde_json::to_string(&leg).unwrap();
                assert_eq!(json, format!("\"{leg}\""));
                assert_eq!(leg.to_string().parse::<Leg>(), Ok(leg));
                assert_eq!(serde_json::from_str::<Leg>(&json).unwrap(), leg);
                json
            })
            .collect();
        assert_eq!(
            names,
            [
                "\"alpaca_borrow_flags\"",
                "\"massive_traded_symbols\"",
                "\"massive_security_details\"",
                "\"sec_filed_industry_codes\"",
                "\"massive_splits\"",
                "\"alpaca_series_boundaries\"",
                "\"alpaca_quotes\"",
                "\"alpaca_trades\"",
            ]
        );
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
                r#""bucket""#,
                r#""vanished""#,
                r#""no_symbols""#,
                r#""unwritten""#,
                r#""unconfigured""#,
                r#""fold""#,
                r#""tee""#,
                r#""journal""#,
                r#""lapsed""#,
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
    fn test_a_listing_lapses_when_the_next_session_opens() {
        // Friday 2026-11-06 and Monday 2026-11-09; the Monday open is 14:30 UTC, after the change back from daylight time.
        let sessions = ["2026-11-06", "2026-11-09"]
            .map(|day| {
                TradingSession::new(
                    date(day),
                    "09:30:00".parse().unwrap(),
                    "16:00:00".parse().unwrap(),
                )
                .unwrap()
            })
            .to_vec();
        let range = SessionRange::new(date("2026-11-06"), date("2026-11-09")).unwrap();
        let calendar = TradingCalendar::new(sessions, range).unwrap();
        let at = |instant: &str| instant.parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            lapsed(&calendar, date("2026-11-06"), at("2026-11-07T03:00:00Z")),
            Ok(())
        );
        assert_eq!(
            lapsed(&calendar, date("2026-11-06"), at("2026-11-09T14:29:59Z")),
            Ok(())
        );
        assert_eq!(
            lapsed(&calendar, date("2026-11-06"), at("2026-11-09T14:30:00Z")),
            Err(Lapse::Opened {
                next_open: at("2026-11-09T14:30:00Z")
            })
        );
        // With no later session in the calendar, whether one has opened cannot be told, so the listing is refused.
        assert_eq!(
            lapsed(&calendar, date("2026-11-09"), at("2026-11-10T03:00:00Z")),
            Err(Lapse::Unscheduled {
                covered_through: date("2026-11-09")
            })
        );
    }

    #[test]
    fn test_each_leg_has_its_series() {
        let series: Vec<String> = Leg::iter()
            .flat_map(|leg| leg.keys(date("2026-09-29")))
            .map(|key| key.series().to_string())
            .collect();
        assert_eq!(
            series,
            [
                "data/equity/stage=parsed/reference/provider=alpaca/table=borrow_flags/",
                "data/equity/stage=raw/reference/provider=alpaca/table=borrow_flags/",
                "data/equity/stage=raw/reference/provider=massive/table=traded_symbols/",
                "data/equity/stage=parsed/reference/provider=massive/table=security_details/",
                "data/equity/stage=raw/reference/provider=massive/table=security_details/",
                "data/equity/stage=parsed/reference/provider=sec/table=filed_industry_codes/",
                "data/equity/stage=raw/reference/provider=sec/table=filed_industry_codes/",
                "data/equity/stage=parsed/reference/provider=massive/table=splits/",
                "data/equity/stage=raw/reference/provider=massive/table=splits/",
                "data/equity/stage=parsed/reference/provider=alpaca/table=series_boundaries/",
                "data/equity/stage=raw/reference/provider=alpaca/table=series_boundaries/",
                "data/equity/stage=parsed/quotes/provider=alpaca/interval=one_day/",
                "data/equity/stage=raw/quotes/provider=alpaca/",
                "data/equity/stage=parsed/trades/provider=alpaca/interval=one_day/",
                "data/equity/stage=raw/trades/provider=alpaca/",
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
        assert_eq!(kept(Leg::SecFiledIndustryCodes), ["2027-01-04"]);
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
            SessionRange::new(date("2026-10-01"), date("2027-01-14")).unwrap()
        );
        let fetched = calendar("2026-10-01", "2027-01-14", &["2026-12-25", "2027-01-01"]);
        let drawn = window(&fetched, today, sessions(5)).unwrap();
        assert_eq!(drawn.last(), date("2026-12-31"));
        // The window's last session finds its successor, so a run on today's session after the open is refused.
        let at = |instant: &str| instant.parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            lapsed(&fetched, drawn.last(), at("2027-01-04T08:00:00Z")),
            Ok(())
        );
        assert_eq!(
            lapsed(&fetched, drawn.last(), at("2027-01-04T15:00:00Z")),
            Err(Lapse::Opened {
                next_open: at("2027-01-04T14:30:00Z")
            })
        );
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
        assert_eq!(Window::new(vec![]), Err(WindowRefusal::Empty));
        for repeated_or_reversed in ["2026-09-29", "2026-09-28"] {
            assert_eq!(
                Window::new(vec![date("2026-09-29"), date(repeated_or_reversed)]),
                Err(WindowRefusal::NotAscending {
                    earlier: date(repeated_or_reversed),
                    later: date("2026-09-29"),
                })
            );
        }
        let window = Window::new(vec![date("2026-09-28"), date("2026-09-29")]).unwrap();
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
