//! What one nightly archive run is responsible for, decided before anything is fetched.
//!
//! The decision is pure so it can be tested across the hours and dates where the Eastern and UTC
//! calendars disagree; `seed` owns the fetching half.

use std::fmt;
use std::time::Duration;

use tokio::time::Instant;

use crate::common::types::{BarInterval, IntradayCadence, SessionDate};
use crate::data::calendar::TradingCalendar;

/// One family-and-cadence a nightly run writes.
///
/// A leg rather than a family because the two quote cadences are separate passes over the tape and
/// either can be the one a budget runs out on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// What became of one leg.
///
/// Skipped and failed are separate variants rather than one "did not finish", because a night that
/// ran out of budget is healed by the next one and a night that errored is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegOutcome {
    /// Ran to its own end. Carries whether the pass considered itself complete.
    Folded { complete: bool, written: usize },
    /// The budget was spent before this leg started.
    Skipped,
    /// The leg returned an error, which the run stepped over to reach the next one.
    Failed(String),
}

/// What a nightly run did, leg by leg.
///
/// Every leg appears whatever happened to it: a report that listed only the legs that ran would
/// read as a clean night when the budget cut it in half.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NightlyReport {
    window_start: SessionDate,
    window_end: SessionDate,
    sessions: usize,
    legs: Vec<(Leg, LegOutcome)>,
}

impl NightlyReport {
    /// Starts a report over the plan's window.
    pub fn over(plan: &NightlyPlan) -> Self {
        NightlyReport {
            window_start: plan.window_start(),
            window_end: plan.window_end(),
            sessions: plan.sessions().len(),
            legs: Vec::new(),
        }
    }

    /// Records what became of one leg.
    pub fn record(&mut self, leg: Leg, outcome: LegOutcome) {
        self.legs.push((leg, outcome));
    }

    /// Legs that errored.
    pub fn failed(&self) -> Vec<&Leg> {
        self.legs
            .iter()
            .filter(|(_, outcome)| matches!(outcome, LegOutcome::Failed(_)))
            .map(|(leg, _)| leg)
            .collect()
    }

    /// Legs the budget did not reach.
    pub fn skipped(&self) -> Vec<&Leg> {
        self.legs
            .iter()
            .filter(|(_, outcome)| matches!(outcome, LegOutcome::Skipped))
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

    /// Whether the night owes the archive nothing further.
    ///
    /// A skipped leg counts against this. The next run heals it, but the exit code is the only
    /// thing automation reads and a half-finished night must not report as a whole one.
    pub fn is_complete(&self) -> bool {
        self.failed().is_empty() && self.skipped().is_empty() && self.incomplete().is_empty()
    }

    /// Total partitions written across every leg.
    pub fn written(&self) -> usize {
        self.legs
            .iter()
            .map(|(_, outcome)| match outcome {
                LegOutcome::Folded { written, .. } => *written,
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
        Ok(())
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

    #[test]
    fn test_a_night_that_ran_out_of_budget_does_not_report_as_complete() {
        // The lesson this encodes: a pass can report success over an incomplete result. A leg the
        // budget never reached wrote nothing, and nothing downstream can tell that from a leg that
        // had nothing to write.
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

        assert!(!report.is_complete());
        assert_eq!(report.written(), 3);
        assert_eq!(
            report.to_string(),
            "2 sessions 2026-09-14 to 2026-09-18, 3 written; skipped for budget: quotes/one_minute"
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
        assert!(report.is_complete());
        assert_eq!(report.written(), 0);
        assert_eq!(
            report.to_string(),
            "1 sessions 2026-09-18 to 2026-09-18, 0 written"
        );
    }
}
