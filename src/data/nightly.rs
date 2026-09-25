//! What one nightly archive run is responsible for, decided before anything is fetched.
//!
//! The decision is pure so it can be tested across the hours and dates where the Eastern and UTC
//! calendars disagree; `seed` owns the fetching half.

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use chrono::{Datelike, NaiveDate};
use tokio::time::Instant;

use crate::common::types::{BarInterval, IntradayCadence, SessionDate};
use crate::data::calendar::TradingCalendar;

/// One family-and-cadence a nightly run writes.
///
/// A leg rather than a family because the two quote cadences are separate passes over the tape and
/// either can be the one a budget runs out on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Leg {
    DailyBars,
    IntradayBars(IntradayCadence),
    Quotes(IntradayCadence),
    Trades,
}

impl Leg {
    /// Every leg, ordered cheapest first so a short budget still closes the daily gap.
    ///
    /// Daily bars are one request per session; quotes are the whole tape. A run that dies partway
    /// should leave the coarse partitions present rather than the expensive ones half-written.
    pub const ALL: [Leg; 6] = [
        Leg::DailyBars,
        Leg::IntradayBars(IntradayCadence::FiveMinute),
        Leg::IntradayBars(IntradayCadence::OneMinute),
        Leg::Trades,
        Leg::Quotes(IntradayCadence::FiveMinute),
        Leg::Quotes(IntradayCadence::OneMinute),
    ];

    /// The bar interval this leg's partitions are keyed at.
    pub fn interval(self) -> BarInterval {
        match self {
            Leg::DailyBars | Leg::Trades => BarInterval::OneDay,
            Leg::IntradayBars(cadence) | Leg::Quotes(cadence) => match cadence {
                IntradayCadence::OneMinute => BarInterval::OneMinute,
                IntradayCadence::FiveMinute => BarInterval::FiveMinute,
            },
        }
    }
}

impl fmt::Display for Leg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Leg::DailyBars => write!(formatter, "bars/one_day"),
            Leg::IntradayBars(cadence) => write!(formatter, "bars/{cadence}"),
            Leg::Quotes(cadence) => write!(formatter, "quotes/{cadence}"),
            Leg::Trades => write!(formatter, "trades"),
        }
    }
}

/// Why a run had nothing to plan.
///
/// An absence carries its cause: each variant names the window that produced it, because "no
/// sessions" from a calendar that does not reach back far enough is a different fault from a run
/// on a weekend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanRefusal {
    /// The calendar published no trading day in the window at all.
    NoTradingDay {
        window_start: SessionDate,
        window_end: SessionDate,
    },
    /// The calendar does not span the window, so absence cannot be distinguished from ignorance.
    CalendarTooShort {
        window_start: SessionDate,
        window_end: SessionDate,
    },
}

impl fmt::Display for PlanRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanRefusal::NoTradingDay {
                window_start,
                window_end,
            } => write!(
                formatter,
                "no published trading day between {window_start} and {window_end}"
            ),
            PlanRefusal::CalendarTooShort {
                window_start,
                window_end,
            } => write!(
                formatter,
                "the calendar does not span {window_start} to {window_end}"
            ),
        }
    }
}

/// The sessions a run will look at, and the legs it will look at them through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NightlyPlan {
    window_start: SessionDate,
    window_end: SessionDate,
    sessions: Vec<SessionDate>,
}

impl NightlyPlan {
    /// The oldest session the run will repair.
    pub fn window_start(&self) -> SessionDate {
        self.window_start
    }

    /// The newest session the run will repair, which is the last one to have closed.
    pub fn window_end(&self) -> SessionDate {
        self.window_end
    }

    /// Every published trading day in the window, oldest first.
    pub fn sessions(&self) -> &[SessionDate] {
        &self.sessions
    }
}

/// Decides which sessions tonight's run covers.
///
/// The window ends at the last trading day strictly before `today`, so a run started after
/// midnight Eastern folds the session that closed rather than the one about to open. It reaches
/// back `lookback_sessions` trading days because the run repairs by set difference: a night missed
/// for any reason is healed by the next one rather than needing anyone to notice.
pub fn plan(
    today: SessionDate,
    lookback_sessions: u32,
    calendar: &TradingCalendar,
) -> Result<NightlyPlan, PlanRefusal> {
    let window_start = today.plus_calendar_days(-(i64::from(lookback_sessions) * 2 + 7));

    if !calendar.covers(window_start, today) {
        return Err(PlanRefusal::CalendarTooShort {
            window_start,
            window_end: today,
        });
    }

    let Some(window_end) = calendar.previous_trading_day(today) else {
        return Err(PlanRefusal::NoTradingDay {
            window_start,
            window_end: today,
        });
    };

    // Taken off the tail of the published days rather than by date arithmetic: counting back a
    // fixed number of calendar days lands on a different number of sessions across a holiday week.
    let published = calendar.trading_days_in_range(window_start, window_end);
    let sessions: Vec<SessionDate> = published
        .iter()
        .rev()
        .take(lookback_sessions as usize)
        .rev()
        .copied()
        .collect();

    let Some(&first) = sessions.first() else {
        return Err(PlanRefusal::NoTradingDay {
            window_start,
            window_end,
        });
    };

    Ok(NightlyPlan {
        window_start: first,
        window_end,
        sessions,
    })
}

/// A calendar quarter, which is the grid the point-in-time reference is observed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Quarter {
    year: i32,
    index: u32,
}

impl Quarter {
    /// The quarter a date falls in.
    fn of(date: SessionDate) -> Self {
        Quarter {
            year: date.date().year(),
            index: (date.date().month() - 1) / 3,
        }
    }

    /// The quarter's first calendar day, which is not necessarily a trading day.
    fn opens(self) -> NaiveDate {
        NaiveDate::from_ymd_opt(self.year, self.index * 3 + 1, 1)
            .expect("a quarter index of 0 to 3 names a real month")
    }

    /// The quarter's last calendar day.
    fn closes(self) -> NaiveDate {
        self.next()
            .opens()
            .pred_opt()
            .expect("the day before a quarter start is in range")
    }

    fn next(self) -> Self {
        match self.index {
            3 => Quarter {
                year: self.year + 1,
                index: 0,
            },
            index => Quarter {
                year: self.year,
                index: index + 1,
            },
        }
    }
}

/// Why a reference sweep had nothing it could plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferenceRefusal {
    /// The archive holds no observation, so there is no anchor and no grid.
    NoObservations,
    /// The calendar does not span the grid, so a quarter's first trading day cannot be found.
    CalendarTooShort {
        window_start: SessionDate,
        window_end: SessionDate,
    },
    /// No session has closed before today, so no grid point is observable yet.
    NoClosedSession { window_end: SessionDate },
    /// The calendar published no trading day inside a quarter it claims to cover, so that quarter's
    /// grid point is unknowable and its absence cannot be told from a quarter with nothing owed.
    NoPublishedQuarter {
        window_start: SessionDate,
        window_end: SessionDate,
    },
}

impl fmt::Display for ReferenceRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReferenceRefusal::NoObservations => write!(
                formatter,
                "the reference archive holds no observation to anchor the grid to"
            ),
            ReferenceRefusal::CalendarTooShort {
                window_start,
                window_end,
            } => write!(
                formatter,
                "the calendar does not span {window_start} to {window_end}"
            ),
            ReferenceRefusal::NoPublishedQuarter {
                window_start,
                window_end,
            } => write!(
                formatter,
                "the calendar published no trading day between {window_start} and {window_end}"
            ),
            ReferenceRefusal::NoClosedSession { window_end } => write!(
                formatter,
                "no published trading day has closed before {window_end}"
            ),
        }
    }
}

/// Which quarterly observations the archive owes, taken as a set difference rather than a date.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferencePlan {
    anchor: SessionDate,
    grid: Vec<SessionDate>,
    owed: Vec<SessionDate>,
}

impl ReferencePlan {
    /// The oldest observation the archive holds, which is where the grid starts.
    pub fn anchor(&self) -> SessionDate {
        self.anchor
    }

    /// Every grid point from the quarter *after* the anchor's to today's, ascending.
    ///
    /// The anchor's own quarter is excluded, so this is one shorter than the number of quarters
    /// spanned. [`plan_reference`] says why.
    pub fn grid(&self) -> &[SessionDate] {
        &self.grid
    }

    /// The grid points whose quarter holds no observation, ascending.
    pub fn owed(&self) -> &[SessionDate] {
        &self.owed
    }
}

/// Where the grid starts, which is the archive's oldest observation.
///
/// Public because the caller has to span the same range when it fetches a calendar, and an anchor
/// decided in two places is two rules that agree by coincidence.
pub fn reference_anchor(present: &[SessionDate]) -> Option<SessionDate> {
    present.iter().copied().min()
}

/// Decides which quarterly reference observations are missing, as of `today`.
///
/// The grid starts after the quarter of the archive's own oldest observation, and quarters are
/// compared at quarter granularity rather than by date. Observations dated after `today` are not
/// read, so the answer is the one that night would have given.
pub fn plan_reference(
    today: SessionDate,
    present: &[SessionDate],
    calendar: &TradingCalendar,
) -> Result<ReferencePlan, ReferenceRefusal> {
    // An observation the night could not have seen must not answer for a quarter on its behalf.
    let observable: Vec<SessionDate> = present
        .iter()
        .copied()
        .filter(|observed| *observed <= today)
        .collect();

    let Some(anchor) = reference_anchor(&observable) else {
        return Err(ReferenceRefusal::NoObservations);
    };
    if !calendar.covers(anchor, today) {
        return Err(ReferenceRefusal::CalendarTooShort {
            window_start: anchor,
            window_end: today,
        });
    }

    // The same bound the session window uses: a grid point has to be a session that has closed.
    let Some(last_closed) = calendar.previous_trading_day(today) else {
        return Err(ReferenceRefusal::NoClosedSession { window_end: today });
    };

    let observed: BTreeSet<Quarter> = observable.iter().copied().map(Quarter::of).collect();
    let current = Quarter::of(today);

    // The anchor's own quarter holds the anchor by construction, so it can never be owed.
    let mut grid = Vec::new();
    let mut owed = Vec::new();
    let mut quarter = Quarter::of(anchor).next();
    while quarter <= current {
        let opens = SessionDate::from_date(quarter.opens());
        let closes = SessionDate::from_date(quarter.closes()).min(last_closed);
        // Tested before the range is built: the calendar indexes a `BTreeMap`, which panics on a
        // range whose start is past its end.
        if closes < opens {
            quarter = quarter.next();
            continue;
        }
        // Refused rather than skipped: a quarter the calendar cannot answer for is indistinguishable
        // from one with nothing owed, and `covers` tests the declared bounds rather than the days.
        let Some(&first) = calendar.trading_days_in_range(opens, closes).first() else {
            return Err(ReferenceRefusal::NoPublishedQuarter {
                window_start: opens,
                window_end: closes,
            });
        };
        grid.push(first);
        if !observed.contains(&quarter) {
            owed.push(first);
        }
        quarter = quarter.next();
    }

    Ok(ReferencePlan { anchor, grid, owed })
}

/// What became of the reference sweep.
///
/// The unwritten half carries its cause per grid point: the ordinary reason is that the session's
/// bar partition is not there yet, which is a different fact from the feed refusing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReferenceOutcome {
    /// Every owed grid point was attempted.
    Swept {
        written: Vec<SessionDate>,
        unwritten: Vec<(SessionDate, String)>,
    },
    /// The budget was spent before the sweep started.
    Skipped,
    /// The sweep could not be planned or run at all.
    Failed(String),
}

impl fmt::Display for ReferenceOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReferenceOutcome::Swept { written, unwritten } => {
                write!(formatter, "{} written", written.len())?;
                for (as_of, cause) in unwritten {
                    write!(formatter, ", {as_of} unwritten ({cause})")?;
                }
                Ok(())
            }
            ReferenceOutcome::Skipped => write!(formatter, "skipped for budget"),
            ReferenceOutcome::Failed(cause) => write!(formatter, "failed: {cause}"),
        }
    }
}

/// What a reference table's `--check` reported when the nightly ran it.
///
/// Drift is a fact about the provider rather than a failure of the run, so it is recorded and the
/// run's exit status is left alone. `Failed` is kept apart from `Drifted` because a check that could
/// not reach the provider says nothing about whether the provider moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReferenceCheck {
    Matches,
    Drifted,
    Failed { exit_status: i32 },
}

impl ReferenceCheck {
    /// Reads the tools' contract: 0 matches, 3 drifted, and anything else is a check not made.
    pub fn from_exit_status(exit_status: i32) -> Self {
        match exit_status {
            0 => ReferenceCheck::Matches,
            3 => ReferenceCheck::Drifted,
            exit_status => ReferenceCheck::Failed { exit_status },
        }
    }
}

/// What `tools/check-views` reported: whether every view over the buckets answers something.
///
/// Its own enum rather than a [`ReferenceCheck`], because a view that reads nothing is our defect
/// and not the provider moving. `Failed` is a check that could not reach S3 and so saw nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ViewCheck {
    AllRead,
    SomeReadNothing,
    Failed { exit_status: i32 },
}

impl ViewCheck {
    /// Reads the tool's contract: 0 every view read rows, 3 some did not, anything else no check.
    pub fn from_exit_status(exit_status: i32) -> Self {
        match exit_status {
            0 => ViewCheck::AllRead,
            3 => ViewCheck::SomeReadNothing,
            exit_status => ViewCheck::Failed { exit_status },
        }
    }
}

/// What refreshing one whole-table dataset did.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TableRefresh {
    Refreshed {
        rows: usize,
    },
    /// The table was left as it was; the error is the vendor's or the bucket's.
    Failed(String),
}

impl TableRefresh {
    pub fn of<E: std::fmt::Display>(result: Result<usize, E>) -> Self {
        match result {
            Ok(rows) => TableRefresh::Refreshed { rows },
            Err(error) => TableRefresh::Failed(error.to_string()),
        }
    }
}

/// A count and the population it was taken over, so a rate carries its denominator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Share {
    pub count: u64,
    pub population: u64,
}

impl Share {
    /// `None` over an empty population, which measured nothing rather than zero.
    pub fn rate(self) -> Option<f64> {
        (self.population > 0).then(|| self.count as f64 / self.population as f64)
    }
}

impl std::ops::Add for Share {
    type Output = Share;

    fn add(self, other: Share) -> Share {
        Share {
            count: self.count + other.count,
            population: self.population + other.population,
        }
    }
}

/// What was wrong with a partition a nightly run rewrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Defect {
    /// No partition for a session older than the newest one, so an earlier night owed it and missed.
    SessionMissed,
    /// A partition was present and short `missing` names, `still_missing` of which the repair could
    /// not fetch either.
    NamesMissing {
        missing: usize,
        still_missing: usize,
    },
}

/// One partition a run rewrote, named by its site and the defect it had.
///
/// The site is the leg and the session, so the same site across nights is a recurring defect rather
/// than routine upkeep. Daily bars never appear: that leg re-fetches its correction window by design.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Repair {
    pub leg: Leg,
    pub session: SessionDate,
    pub defect: Defect,
}

/// What became of one leg.
///
/// Skipped and failed are separate variants rather than one "did not finish", because a night that
/// ran out of budget is healed by the next one and a night that errored is not.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LegOutcome {
    /// Reached every session it was given. Carries whether the passes considered themselves
    /// complete.
    Folded { complete: bool, written: usize },
    /// The budget ran out partway, so some of this leg's sessions were never reached.
    CutShort { written: usize, unreached: usize },
    /// The budget was spent before this leg started.
    Skipped,
    /// The leg returned an error, which the run stepped over to reach the next one.
    Failed(String),
}

/// What a nightly run did, leg by leg, at session granularity.
///
/// Every leg appears whatever happened to it: a report that listed only the legs that ran would
/// read as a clean night when the budget cut it in half. Symbol-level gaps are invisible here --
/// a partition missing one name reads as present, and the scan is what answers that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NightlyReport {
    window_start: SessionDate,
    window_end: SessionDate,
    sessions: usize,
    legs: Vec<(Leg, LegOutcome)>,
    reference: Option<ReferenceOutcome>,
    /// Set only by a trades leg that ran, because folding no tape is not folding under no rules.
    conditions_as_of: Option<chrono::NaiveDate>,
    repairs: Vec<Repair>,
    /// The splits and boundary refreshes, or `None` where the budget ran out before them.
    corporate_actions: Option<(TableRefresh, TableRefresh)>,
}

impl NightlyReport {
    /// Starts a report over the plan's window.
    pub fn over(plan: &NightlyPlan) -> Self {
        NightlyReport {
            window_start: plan.window_start(),
            window_end: plan.window_end(),
            sessions: plan.sessions().len(),
            legs: Vec::new(),
            reference: None,
            conditions_as_of: None,
            repairs: Vec::new(),
            corporate_actions: None,
        }
    }

    /// Records what the splits and boundary refreshes did.
    pub fn record_corporate_actions(&mut self, splits: TableRefresh, boundaries: TableRefresh) {
        self.corporate_actions = Some((splits, boundaries));
    }

    /// The splits and boundary refreshes, or `None` if they never ran.
    pub fn corporate_actions(&self) -> Option<&(TableRefresh, TableRefresh)> {
        self.corporate_actions.as_ref()
    }

    /// Records one partition the run rewrote and what was wrong with it.
    pub fn record_repair(&mut self, repair: Repair) {
        self.repairs.push(repair);
    }

    /// Every repair, in the order the run made them.
    pub fn repairs(&self) -> &[Repair] {
        &self.repairs
    }

    /// Records what became of one leg.
    pub fn record(&mut self, leg: Leg, outcome: LegOutcome) {
        self.legs.push((leg, outcome));
    }

    /// Records what became of the quarterly reference sweep.
    pub fn record_reference(&mut self, outcome: ReferenceOutcome) {
        self.reference = Some(outcome);
    }

    /// What the reference sweep did, or `None` if it never ran.
    pub fn reference(&self) -> Option<&ReferenceOutcome> {
        self.reference.as_ref()
    }

    /// Records which published conditions table the tape was folded under.
    pub fn record_conditions(&mut self, as_of: chrono::NaiveDate) {
        self.conditions_as_of = Some(as_of);
    }

    /// The conditions table the trades leg ran under, or `None` if no tape was folded.
    ///
    /// The fold cannot be undone, so this is the only account of which eligibility rules produced
    /// the partitions this run wrote.
    pub fn conditions_as_of(&self) -> Option<chrono::NaiveDate> {
        self.conditions_as_of
    }

    /// Legs that errored.
    pub fn failed(&self) -> Vec<&Leg> {
        self.legs
            .iter()
            .filter(|(_, outcome)| matches!(outcome, LegOutcome::Failed(_)))
            .map(|(leg, _)| leg)
            .collect()
    }

    /// Legs the budget did not reach, or reached and then ran out on.
    pub fn skipped(&self) -> Vec<&Leg> {
        self.legs
            .iter()
            .filter(|(_, outcome)| {
                matches!(outcome, LegOutcome::Skipped | LegOutcome::CutShort { .. })
            })
            .map(|(leg, _)| leg)
            .collect()
    }

    /// Legs that ran but did not consider themselves complete.
    pub fn incomplete(&self) -> Vec<&Leg> {
        self.legs
            .iter()
            .filter(|(_, outcome)| {
                matches!(
                    outcome,
                    LegOutcome::Folded {
                        complete: false,
                        ..
                    }
                )
            })
            .map(|(leg, _)| leg)
            .collect()
    }

    /// Whether the night owes the archive nothing further, at session granularity.
    ///
    /// Session granularity is the whole claim: a partition written while one name's fetch failed
    /// reads exactly like a complete one, which is what `SessionSelection::Present` exists for.
    ///
    /// Every leg must have been recorded, and so must the reference sweep. The emptiness tests
    /// below are all vacuously true over a report where nothing ran, and this predicate is what the
    /// exit code is taken from.
    pub fn is_complete(&self) -> bool {
        let recorded: Vec<Leg> = self.legs.iter().map(|(leg, _)| *leg).collect();
        let swept = matches!(
            self.reference,
            Some(ReferenceOutcome::Swept { ref unwritten, .. }) if unwritten.is_empty()
        );
        // A stale table fails the night the way a failed leg does, or it goes unnoticed for weeks.
        let refreshed = matches!(
            self.corporate_actions,
            Some((
                TableRefresh::Refreshed { .. },
                TableRefresh::Refreshed { .. }
            ))
        );
        Leg::ALL.iter().all(|leg| recorded.contains(leg))
            && swept
            && refreshed
            && self.failed().is_empty()
            && self.skipped().is_empty()
            && self.incomplete().is_empty()
    }

    /// The window this report covers, and how many sessions were planned inside it.
    ///
    /// Exposed so the journal record can carry the plan as well as its result: "nothing written" and
    /// "nothing owed" are different nights and the count is what separates them.
    pub fn window(&self) -> (SessionDate, SessionDate) {
        (self.window_start, self.window_end)
    }

    /// How many sessions the plan held.
    pub fn sessions_planned(&self) -> usize {
        self.sessions
    }

    /// Every leg recorded, in the order it ran.
    pub fn legs(&self) -> &[(Leg, LegOutcome)] {
        &self.legs
    }

    /// Total partitions written across every leg.
    pub fn written(&self) -> usize {
        self.legs
            .iter()
            .map(|(_, outcome)| match outcome {
                LegOutcome::Folded { written, .. } | LegOutcome::CutShort { written, .. } => {
                    *written
                }
                LegOutcome::Skipped | LegOutcome::Failed(_) => 0,
            })
            .sum()
    }
}

impl fmt::Display for NightlyReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} sessions {} to {}, {} written",
            self.sessions,
            self.window_start,
            self.window_end,
            self.written()
        )?;
        for (label, legs) in [
            ("failed", self.failed()),
            ("skipped for budget", self.skipped()),
            ("incomplete", self.incomplete()),
        ] {
            if !legs.is_empty() {
                let names: Vec<String> = legs.iter().map(|leg| leg.to_string()).collect();
                write!(formatter, "; {label}: {}", names.join(", "))?;
            }
        }
        match &self.reference {
            Some(outcome) => write!(formatter, "; reference: {outcome}"),
            None => write!(formatter, "; reference: not run"),
        }
    }
}

/// How much wall clock a run may still spend starting work.
///
/// Measured on a monotonic clock, because a run spans hours and a wall clock can step.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    started: Instant,
    total: Duration,
}

impl Budget {
    /// Starts a budget running now.
    pub fn starting_now(total: Duration) -> Self {
        Budget {
            started: Instant::now(),
            total,
        }
    }

    /// How long is left, saturating at zero rather than going negative.
    pub fn remaining(&self) -> Duration {
        self.total.saturating_sub(self.started.elapsed())
    }

    /// Whether there is time to start another leg.
    ///
    /// Asked between legs and never inside one: a fold that is halfway through a session would
    /// leave a partition written at one cadence and absent at another.
    pub fn may_start_another(&self) -> bool {
        !self.remaining().is_zero()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::alpaca::CalendarDay;

    /// Published days over a range, skipping weekends and any date in `holidays`.
    fn calendar_over(start: &str, end: &str, holidays: &[&str]) -> TradingCalendar {
        use chrono::{Datelike, NaiveDate, NaiveTime, Weekday};
        let open = NaiveTime::from_hms_opt(9, 30, 0).expect("a time");
        let close = NaiveTime::from_hms_opt(16, 0, 0).expect("a time");
        let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").expect("a date");
        let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").expect("a date");
        let mut days = Vec::new();
        let mut date = start;
        while date <= end {
            let weekend = matches!(date.weekday(), Weekday::Sat | Weekday::Sun);
            let holiday = holidays.contains(&date.format("%Y-%m-%d").to_string().as_str());
            if !weekend && !holiday {
                days.push(CalendarDay::new(date, open, close).expect("a session"));
            }
            date = date.succ_opt().expect("a next day");
        }
        TradingCalendar::covering(
            days,
            SessionDate::from_date(start),
            SessionDate::from_date(end),
        )
    }

    fn session(text: &str) -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("a date"))
    }

    #[test]
    fn test_a_table_refresh_keeps_the_row_count_or_the_error() {
        assert_eq!(
            TableRefresh::of::<String>(Ok(12)),
            TableRefresh::Refreshed { rows: 12 }
        );
        assert_eq!(
            TableRefresh::of(Err("throttled")),
            TableRefresh::Failed("throttled".to_string())
        );
    }

    #[test]
    fn test_a_share_over_nothing_measured_nothing() {
        assert_eq!(
            Share {
                count: 0,
                population: 0
            }
            .rate(),
            None
        );
        let summed = Share {
            count: 3,
            population: 100,
        } + Share {
            count: 1,
            population: 300,
        };
        assert_eq!(
            summed,
            Share {
                count: 4,
                population: 400
            }
        );
        assert_eq!(summed.rate(), Some(0.01));
    }

    #[test]
    fn test_a_report_keeps_its_repairs_in_order() {
        let calendar = calendar_over("2026-09-01", "2026-09-30", &[]);
        let plan = plan(session("2026-09-23"), 2, &calendar).expect("a plan");
        let mut report = NightlyReport::over(&plan);
        let first = Repair {
            leg: Leg::Trades,
            session: session("2026-09-21"),
            defect: Defect::SessionMissed,
        };
        let second = Repair {
            leg: Leg::Quotes(IntradayCadence::OneMinute),
            session: session("2026-09-22"),
            defect: Defect::NamesMissing {
                missing: 2,
                still_missing: 1,
            },
        };
        report.record_repair(first);
        report.record_repair(second);
        assert_eq!(report.repairs(), &[first, second]);
    }

    #[test]
    fn test_the_window_ends_on_the_session_that_closed_not_today() {
        // 2026-09-18 is a Friday; a run on Saturday folds Friday, not Saturday.
        let calendar = calendar_over("2026-08-01", "2026-09-30", &[]);
        let plan = plan(session("2026-09-19"), 5, &calendar).expect("a plan");
        assert_eq!(plan.window_end(), session("2026-09-18"));
    }

    #[test]
    fn test_a_weekend_run_still_reaches_the_weeks_sessions() {
        // The gate is not "was yesterday a trading day" -- a Sunday run must still heal a hole,
        // which is the whole reason the window is a set difference rather than one session.
        let calendar = calendar_over("2026-08-01", "2026-09-30", &[]);
        let plan = plan(session("2026-09-20"), 5, &calendar).expect("a plan");
        assert_eq!(plan.window_end(), session("2026-09-18"));
        assert_eq!(plan.sessions().len(), 5);
        assert_eq!(
            plan.sessions().first().copied(),
            Some(session("2026-09-14"))
        );
    }

    #[test]
    fn test_the_lookback_counts_sessions_not_calendar_days() {
        // Thanksgiving week: 2026-11-26 is the holiday, so five sessions back from Friday the 27th
        // reaches Monday the 23rd only if the count skips it.
        let calendar = calendar_over("2026-11-01", "2026-12-05", &["2026-11-26"]);
        let plan = plan(session("2026-11-28"), 5, &calendar).expect("a plan");
        assert_eq!(plan.window_end(), session("2026-11-27"));
        assert_eq!(
            plan.sessions(),
            [
                session("2026-11-20"),
                session("2026-11-23"),
                session("2026-11-24"),
                session("2026-11-25"),
                session("2026-11-27"),
            ]
        );
    }

    #[test]
    fn test_a_calendar_that_does_not_span_the_window_is_refused() {
        let calendar = calendar_over("2026-09-15", "2026-09-30", &[]);
        let refusal = plan(session("2026-09-20"), 5, &calendar).expect_err("a refusal");
        assert!(matches!(refusal, PlanRefusal::CalendarTooShort { .. }));
    }

    #[test]
    fn test_a_budget_stops_admitting_legs_once_spent() {
        let spent = Budget {
            started: Instant::now() - Duration::from_secs(120),
            total: Duration::from_secs(60),
        };
        assert!(!spent.may_start_another());
        assert_eq!(spent.remaining(), Duration::ZERO);

        let fresh = Budget::starting_now(Duration::from_secs(60));
        assert!(fresh.may_start_another());
    }

    /// Quarterly grid points, as the archive actually holds them.
    ///
    /// Anchored at 2021-08-23, which is the session the bar archive begins on and is mid-quarter.
    fn archived_grid() -> Vec<SessionDate> {
        [
            "2021-08-23",
            "2021-10-01",
            "2022-01-03",
            "2022-04-01",
            "2022-07-01",
            "2022-10-03",
            "2023-01-03",
            "2023-04-03",
            "2023-07-03",
            "2023-10-02",
            "2024-01-02",
            "2024-04-01",
            "2024-07-01",
            "2024-10-01",
            "2025-01-02",
            "2025-04-01",
            "2025-07-01",
            "2025-10-01",
            "2026-01-02",
            "2026-04-01",
            "2026-07-01",
        ]
        .iter()
        .map(|text| session(text))
        .collect()
    }

    #[test]
    fn test_the_anchor_quarter_is_not_on_the_grid() {
        // 2021-08-23 is the session the bar archive begins on, and it lands mid-quarter. Its
        // quarter holds it by construction, and the calendar cannot see that quarter's true open.
        let calendar = calendar_over("2021-08-23", "2021-12-31", &[]);
        let plan = plan_reference(session("2021-11-01"), &[session("2021-08-23")], &calendar)
            .expect("a plan");
        assert_eq!(plan.anchor(), session("2021-08-23"));
        assert_eq!(plan.grid(), [session("2021-10-01")]);
        assert_eq!(plan.owed(), [session("2021-10-01")]);
    }

    #[test]
    fn test_a_quarter_is_satisfied_by_any_observation_inside_it() {
        // An observation written mid-quarter by a repair answers for its quarter. Taken on dates
        // this would be owed forever, asking for a date the archive already answered with another.
        let calendar = calendar_over("2021-08-23", "2021-12-31", &[]);
        let plan = plan_reference(
            session("2021-11-01"),
            &[session("2021-08-23"), session("2021-10-14")],
            &calendar,
        )
        .expect("a plan");
        assert_eq!(plan.grid(), [session("2021-10-01")]);
        assert!(plan.owed().is_empty());
    }

    #[test]
    fn test_a_new_quarter_is_owed_once_its_first_session_has_traded() {
        // The case this job exists for: 2026-10-01 is the next grid point after the archive's
        // newest, and nothing writes it today.
        let calendar = calendar_over("2021-07-01", "2026-10-31", &[]);
        let plan =
            plan_reference(session("2026-10-02"), &archived_grid(), &calendar).expect("a plan");
        assert_eq!(plan.owed(), [session("2026-10-01")]);
    }

    #[test]
    fn test_a_quarter_that_has_not_opened_yet_is_not_owed() {
        // Run the same archive through a date inside the quarter it already holds. Nothing is owed,
        // which is what makes this safe to run every night rather than once a quarter.
        let calendar = calendar_over("2021-07-01", "2026-09-30", &[]);
        let plan =
            plan_reference(session("2026-09-20"), &archived_grid(), &calendar).expect("a plan");
        // Twenty, not twenty-one: the anchor's own quarter is not on the grid. Pinned to the number
        // `seed equity-reference grid` prints against the live bucket.
        assert_eq!(plan.grid().len(), 20);
        assert_eq!(plan.grid().first().copied(), Some(session("2021-10-01")));
        assert!(plan.owed().is_empty());
    }

    #[test]
    fn test_a_hole_in_the_middle_of_the_grid_is_owed() {
        // Self-healing is not only about the newest quarter: a quarter that failed two years ago is
        // owed on the same terms, because the difference is taken over the whole grid.
        let calendar = calendar_over("2021-07-01", "2026-09-30", &[]);
        let holed: Vec<SessionDate> = archived_grid()
            .into_iter()
            .filter(|date| *date != session("2024-04-01"))
            .collect();
        let plan = plan_reference(session("2026-09-20"), &holed, &calendar).expect("a plan");
        assert_eq!(plan.owed(), [session("2024-04-01")]);
    }

    #[test]
    fn test_the_grid_point_is_the_first_trading_day_not_the_first_of_the_month() {
        // 2023-01-01 is a Sunday and the holiday is observed on the Monday, so the quarter opens on
        // the third. Pinned to the date the archive holds rather than to the calendar's own answer.
        let calendar = calendar_over("2022-10-01", "2023-03-31", &["2023-01-02"]);
        let plan = plan_reference(session("2023-02-01"), &[session("2022-10-03")], &calendar)
            .expect("a plan");
        assert_eq!(plan.grid(), [session("2023-01-03")]);
        assert_eq!(plan.owed(), [session("2023-01-03")]);
    }

    #[test]
    fn test_a_quarters_opening_day_is_not_owed_until_it_has_closed() {
        // The nightly fires at three in the morning Eastern, so on 2026-10-01 the quarter has
        // opened and has no bars yet. Owing it is a non-zero exit once a quarter, for nothing.
        let calendar = calendar_over("2026-06-01", "2026-10-31", &[]);
        let present = [session("2026-07-01")];

        let opening = plan_reference(session("2026-10-01"), &present, &calendar).expect("a plan");
        assert!(
            opening.owed().is_empty(),
            "the quarter's first session has not closed yet"
        );

        let after = plan_reference(session("2026-10-02"), &present, &calendar).expect("a plan");
        assert_eq!(after.owed(), [session("2026-10-01")]);
    }

    #[test]
    fn test_a_calendar_with_nothing_closed_before_today_is_refused() {
        // Reachable rather than defensive: the bounds a calendar claims to cover and the days it
        // actually holds are separate, so a calendar can span today and publish nothing before it.
        let calendar = TradingCalendar::covering(
            Vec::new(),
            SessionDate::from_date(
                chrono::NaiveDate::parse_from_str("2026-09-01", "%Y-%m-%d").expect("a date"),
            ),
            SessionDate::from_date(
                chrono::NaiveDate::parse_from_str("2026-09-30", "%Y-%m-%d").expect("a date"),
            ),
        );
        let refusal = plan_reference(session("2026-09-20"), &[session("2026-09-02")], &calendar)
            .expect_err("a refusal");
        assert_eq!(
            refusal,
            ReferenceRefusal::NoClosedSession {
                window_end: session("2026-09-20")
            }
        );
    }

    #[test]
    fn test_an_observation_after_the_as_of_date_does_not_answer_for_its_quarter() {
        // The flag says "as of", so a partition written later in the quarter than the date being
        // asked about must not make that quarter present on the night that had not seen it.
        let calendar = calendar_over("2026-01-01", "2026-12-31", &[]);
        let repaired_later = [session("2026-01-05"), session("2026-08-14")];

        let blind =
            plan_reference(session("2026-07-20"), &repaired_later, &calendar).expect("a plan");
        assert_eq!(
            blind.owed(),
            [session("2026-04-01"), session("2026-07-01")],
            "the 2026-08-14 observation had not been written yet"
        );

        let sighted =
            plan_reference(session("2026-09-20"), &repaired_later, &calendar).expect("a plan");
        assert_eq!(
            sighted.owed(),
            [session("2026-04-01")],
            "once it is in the past it answers for the third quarter"
        );
    }

    #[test]
    fn test_a_quarter_the_calendar_cannot_answer_for_is_refused() {
        // `covers` tests the bounds the calendar was handed, not the days it holds, so a calendar
        // can claim a span and publish nothing inside part of it.
        let published = |from: &str, to: &str| {
            calendar_over(from, to, &[])
                .trading_days_in_range(session(from), session(to))
                .into_iter()
                .map(|date| {
                    CalendarDay::new(
                        date.date(),
                        chrono::NaiveTime::from_hms_opt(9, 30, 0).expect("a time"),
                        chrono::NaiveTime::from_hms_opt(16, 0, 0).expect("a time"),
                    )
                    .expect("a session")
                })
                .collect::<Vec<_>>()
        };
        // The whole span except the second quarter, which is the hole.
        let mut days = published("2026-01-01", "2026-03-31");
        days.extend(published("2026-07-01", "2026-09-30"));
        let sparse = TradingCalendar::covering(days, session("2026-01-01"), session("2026-09-30"));

        let refusal = plan_reference(session("2026-09-20"), &[session("2026-01-05")], &sparse)
            .expect_err("a refusal");
        assert!(
            matches!(refusal, ReferenceRefusal::NoPublishedQuarter { .. }),
            "the second quarter published no day, so its grid point is unknowable: {refusal}"
        );
    }

    #[test]
    fn test_an_archive_with_no_observation_is_refused_rather_than_backfilled() {
        // A nightly job must not decide to fetch five years of history because a bucket looked
        // empty. The grid has no anchor, so there is no answer to give.
        let calendar = calendar_over("2026-09-01", "2026-09-30", &[]);
        let refusal = plan_reference(session("2026-09-20"), &[], &calendar).expect_err("a refusal");
        assert_eq!(refusal, ReferenceRefusal::NoObservations);
    }

    #[test]
    fn test_a_calendar_that_does_not_span_the_grid_is_refused() {
        let calendar = calendar_over("2026-09-01", "2026-09-30", &[]);
        let refusal = plan_reference(session("2026-09-20"), &archived_grid(), &calendar)
            .expect_err("a refusal");
        assert!(matches!(refusal, ReferenceRefusal::CalendarTooShort { .. }));
    }

    #[test]
    fn test_legs_are_ordered_cheapest_first() {
        // Pinned to the literal order rather than derived from ALL, so a reordering that put the
        // whole tape before the daily bars has to be made here too.
        assert_eq!(Leg::ALL[0], Leg::DailyBars);
        assert_eq!(Leg::ALL[5], Leg::Quotes(IntradayCadence::OneMinute));
        assert_eq!(Leg::ALL.len(), 6);
    }
}

#[cfg(test)]
mod report_tests {
    use super::*;

    fn plan_over(sessions: &[&str]) -> NightlyPlan {
        let dates: Vec<SessionDate> = sessions
            .iter()
            .map(|text| {
                SessionDate::from_date(
                    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("a date"),
                )
            })
            .collect();
        NightlyPlan {
            window_start: *dates.first().expect("a session"),
            window_end: *dates.last().expect("a session"),
            sessions: dates,
        }
    }

    /// A sweep that found the grid complete, which is the ordinary night.
    fn swept() -> ReferenceOutcome {
        ReferenceOutcome::Swept {
            written: Vec::new(),
            unwritten: Vec::new(),
        }
    }

    #[test]
    fn test_a_night_that_ran_out_of_budget_does_not_report_as_complete() {
        // A pass can report success over an incomplete result: nothing downstream separates a leg
        // the budget never reached from one that had nothing to write.
        let plan = plan_over(&["2026-09-14", "2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        report.record(
            Leg::DailyBars,
            LegOutcome::Folded {
                complete: true,
                written: 3,
            },
        );
        report.record(Leg::Quotes(IntradayCadence::OneMinute), LegOutcome::Skipped);
        report.record_reference(swept());

        assert!(!report.is_complete());
        assert_eq!(report.written(), 3);
        assert_eq!(
            report.to_string(),
            "2 sessions 2026-09-14 to 2026-09-18, 3 written; skipped for budget: quotes/one_minute; reference: 0 written"
        );
    }

    #[test]
    fn test_a_failed_leg_is_named_separately_from_a_skipped_one() {
        let plan = plan_over(&["2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        report.record(Leg::Trades, LegOutcome::Failed("no credentials".into()));
        report.record(Leg::DailyBars, LegOutcome::Skipped);

        assert_eq!(report.failed(), vec![&Leg::Trades]);
        assert_eq!(report.skipped(), vec![&Leg::DailyBars]);
        assert!(report.to_string().contains("failed: trades"));
        assert!(report
            .to_string()
            .contains("skipped for budget: bars/one_day"));
    }

    #[test]
    fn test_a_leg_cut_off_partway_is_not_a_clean_fold() {
        // The distinction that matters: this leg wrote real partitions, and everything downstream
        // reads those as finished sessions. Only the exit code says the rest were never reached.
        let plan = plan_over(&["2026-09-14", "2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        report.record(
            Leg::Quotes(IntradayCadence::FiveMinute),
            LegOutcome::CutShort {
                written: 2,
                unreached: 3,
            },
        );
        assert!(!report.is_complete());
        assert_eq!(report.written(), 2);
        assert_eq!(
            report.skipped(),
            vec![&Leg::Quotes(IntradayCadence::FiveMinute)]
        );
    }

    #[test]
    fn test_a_report_with_no_legs_recorded_is_not_complete() {
        // Every emptiness test in `is_complete` is vacuously true here, and this predicate is what
        // the exit code is taken from -- a run that never started must not exit zero.
        let plan = plan_over(&["2026-09-18"]);
        let report = NightlyReport::over(&plan);
        assert!(report.failed().is_empty());
        assert!(report.skipped().is_empty());
        assert!(report.incomplete().is_empty());
        assert!(!report.is_complete());
    }

    #[test]
    fn test_a_report_missing_one_leg_is_not_complete() {
        let plan = plan_over(&["2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        for leg in Leg::ALL.iter().take(5) {
            report.record(
                *leg,
                LegOutcome::Folded {
                    complete: true,
                    written: 0,
                },
            );
        }
        assert!(
            !report.is_complete(),
            "five of six legs is not a whole night"
        );
    }

    #[test]
    fn test_a_night_with_nothing_to_do_is_complete() {
        // Every leg ran and found no gap. Distinct from the case above: zero written here is the
        // archive already being current, not work that never started.
        let plan = plan_over(&["2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        for leg in Leg::ALL {
            report.record(
                leg,
                LegOutcome::Folded {
                    complete: true,
                    written: 0,
                },
            );
        }
        report.record_reference(swept());
        assert!(
            !report.is_complete(),
            "a night that never refreshed its tables is not complete"
        );
        report.record_corporate_actions(
            TableRefresh::Refreshed { rows: 3_120 },
            TableRefresh::Failed("throttled".to_string()),
        );
        assert!(!report.is_complete(), "a failed refresh fails the night");
        report.record_corporate_actions(
            TableRefresh::Refreshed { rows: 3_120 },
            TableRefresh::Refreshed { rows: 41 },
        );
        assert!(report.is_complete());
        assert_eq!(report.written(), 0);
        assert_eq!(
            report.to_string(),
            "1 sessions 2026-09-18 to 2026-09-18, 0 written; reference: 0 written"
        );
    }

    #[test]
    fn test_a_night_that_never_reached_the_reference_sweep_is_not_complete() {
        // Six clean legs and no sweep. Without this the quarter the job exists to catch is missed
        // by a run that exits zero -- which is how 2026-10-01 would pass unnoticed.
        let plan = plan_over(&["2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        for leg in Leg::ALL {
            report.record(
                leg,
                LegOutcome::Folded {
                    complete: true,
                    written: 0,
                },
            );
        }
        assert!(report.reference().is_none());
        assert!(!report.is_complete());
        assert!(report.to_string().ends_with("reference: not run"));
    }

    #[test]
    fn test_a_sweep_that_left_a_quarter_unwritten_is_not_complete() {
        // The grid point was owed, attempted, and not written. The cause travels with it, because
        // a missing bar partition is tomorrow's problem and a refusing feed is tonight's.
        let plan = plan_over(&["2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        for leg in Leg::ALL {
            report.record(
                leg,
                LegOutcome::Folded {
                    complete: true,
                    written: 0,
                },
            );
        }
        report.record_reference(ReferenceOutcome::Swept {
            written: Vec::new(),
            unwritten: vec![(
                SessionDate::from_date(
                    chrono::NaiveDate::from_ymd_opt(2026, 10, 1).expect("a date"),
                ),
                "no bar partition".to_string(),
            )],
        });
        assert!(!report.is_complete());
        assert!(report
            .to_string()
            .contains("2026-10-01 unwritten (no bar partition)"));
    }

    #[test]
    fn test_a_sweep_that_failed_outright_is_not_complete() {
        let plan = plan_over(&["2026-09-18"]);
        let mut report = NightlyReport::over(&plan);
        for leg in Leg::ALL {
            report.record(
                leg,
                LegOutcome::Folded {
                    complete: true,
                    written: 0,
                },
            );
        }
        report.record_reference(ReferenceOutcome::Failed("no credentials".to_string()));
        assert!(!report.is_complete());
    }

    /// The contract both `--check` tools state in their headers, pinned to literals so a change on
    /// `tools/check-views` exits 3 for a view that reads nothing, and 124 when `timeout` cut it off.
    #[test]
    fn test_a_view_check_status_reads_as_the_tool_defines_it() {
        assert_eq!(ViewCheck::from_exit_status(0), ViewCheck::AllRead);
        assert_eq!(ViewCheck::from_exit_status(3), ViewCheck::SomeReadNothing);
        assert_eq!(
            ViewCheck::from_exit_status(124),
            ViewCheck::Failed { exit_status: 124 }
        );
    }

    /// either side fails here rather than recording drift as a failed check or the reverse.
    #[test]
    fn test_a_check_status_reads_as_the_tools_define_it() {
        assert_eq!(ReferenceCheck::from_exit_status(0), ReferenceCheck::Matches);
        assert_eq!(ReferenceCheck::from_exit_status(3), ReferenceCheck::Drifted);
        assert_eq!(
            ReferenceCheck::from_exit_status(1),
            ReferenceCheck::Failed { exit_status: 1 }
        );
        assert_eq!(
            ReferenceCheck::from_exit_status(127),
            ReferenceCheck::Failed { exit_status: 127 },
            "a tool that was never found is a check not made, not a provider that moved"
        );
    }
}
