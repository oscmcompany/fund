//! What the Register says about how we test: whether bids are calibrated, whether the bar still binds as tests
//! accumulate, and what a test costs.

use std::collections::BTreeMap;
use std::fmt::Write;

use strum::IntoEnumIterator;

use crate::common::market::Dollars;
use crate::common::register::{
    Accession, AccessionNumber, Bid, Family, Measured, Status, Unit, Verdict,
};
use crate::common::time::SessionDate;

/// Below this many scored bids, coverage and error are read as noise.
pub const MINIMUM_SCORED_BIDS: usize = 20;

/// Bids against what was measured, with every accession that could not be scored counted by cause.
#[derive(Debug, Clone, PartialEq, Default)]
struct BidCalibration {
    open: usize,
    /// Closed with a bid that is prose or absent.
    bid_absent: usize,
    /// Closed with an interval bid but no measured value.
    measurement_absent: usize,
    scored: usize,
    covered: usize,
    /// Summed in whole percents, so the mean is exact.
    declared_coverage_percent: u64,
    /// Signed error and width only average within one unit.
    by_units: BTreeMap<Unit, Errors>,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct Errors {
    scored: usize,
    signed_error: f64,
    width: f64,
}

impl BidCalibration {
    fn of<'a>(accessions: impl IntoIterator<Item = &'a Accession>) -> Self {
        let mut calibration = Self::default();
        for accession in accessions {
            let closing = match accession.status() {
                Status::Open => {
                    calibration.open += 1;
                    continue;
                }
                Status::Closed(closing) => closing,
            };
            match (accession.opening().bid(), closing.measured()) {
                (Bid::Written(_) | Bid::Unrecorded, _) => calibration.bid_absent += 1,
                (Bid::Interval(_), Measured::NotMeasured | Measured::Unrecorded) => {
                    calibration.measurement_absent += 1;
                }
                (Bid::Interval(interval), Measured::Value(measured)) => {
                    calibration.scored += 1;
                    calibration.covered +=
                        usize::from((interval.low()..=interval.high()).contains(&measured));
                    calibration.declared_coverage_percent += u64::from(interval.coverage_percent());
                    let errors = calibration
                        .by_units
                        .entry(interval.units().clone())
                        .or_default();
                    errors.scored += 1;
                    errors.signed_error += measured - interval.estimate();
                    errors.width += interval.high() - interval.low();
                }
            }
        }
        calibration
    }

    fn closed(&self) -> usize {
        self.bid_absent + self.measurement_absent + self.scored
    }
}

/// A calendar quarter, which `opened` dates are counted in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Quarter {
    year: i32,
    quarter: u32,
}

impl Quarter {
    fn of(session: SessionDate) -> Self {
        use chrono::Datelike;
        Self {
            year: session.date().year(),
            quarter: session.date().month0() / 3 + 1,
        }
    }
}

impl std::fmt::Display for Quarter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}Q{}", self.year, self.quarter)
    }
}

/// Accessions opened in one quarter and how they closed. A rising count against a flat acceptance share is healthy;
/// both rising means the bar stopped binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Tests {
    open: usize,
    closed: usize,
    accepted: usize,
}

fn tests_by_quarter(accessions: &[Accession]) -> BTreeMap<Quarter, Tests> {
    let mut quarters: BTreeMap<Quarter, Tests> = BTreeMap::new();
    for accession in accessions {
        let tests = quarters
            .entry(Quarter::of(accession.opening().opened()))
            .or_default();
        match accession.status() {
            Status::Open => tests.open += 1,
            Status::Closed(closing) => {
                tests.closed += 1;
                tests.accepted += usize::from(closing.verdict() == Verdict::Accept);
            }
        }
    }
    quarters
}

/// The spread of one recorded cost over closed accessions, with how many never recorded it.
#[derive(Debug, Clone, PartialEq)]
struct Spread {
    unrecorded: usize,
    /// Sorted ascending.
    recorded: Vec<u64>,
}

impl Spread {
    fn of(values: impl IntoIterator<Item = Option<u64>>) -> Self {
        let (recorded, unrecorded): (Vec<Option<u64>>, Vec<Option<u64>>) =
            values.into_iter().partition(Option::is_some);
        let mut recorded: Vec<u64> = recorded.into_iter().flatten().collect();
        recorded.sort_unstable();
        Self {
            unrecorded: unrecorded.len(),
            recorded,
        }
    }

    /// Minimum, lower median and maximum; `None` when nothing was recorded. The lower middle of an even count rather
    /// than an average of two, so it is always a recorded value and never a half-unit of seconds or millionths.
    fn bounds(&self) -> Option<(u64, u64, u64)> {
        let (first, last) = (self.recorded.first()?, self.recorded.last()?);
        Some((*first, self.recorded[(self.recorded.len() - 1) / 2], *last))
    }
}

/// The refutation that took longest by the wall clock, which says how ambitious a test was allowed to be.
fn most_expensive_refutation(accessions: &[Accession]) -> Option<(AccessionNumber, u64)> {
    accessions
        .iter()
        .filter_map(|accession| match accession.status() {
            Status::Closed(closing) if closing.verdict() == Verdict::Refute => {
                Some((accession.number(), closing.cost().wall_clock_seconds?))
            }
            Status::Open | Status::Closed(_) => None,
        })
        .max_by_key(|(_, seconds)| *seconds)
}

/// The Register's readings as text, every share printed beside its population.
pub fn report(accessions: &[Accession]) -> String {
    let mut text = String::new();
    let overall = BidCalibration::of(accessions);
    let _ = writeln!(text, "Bids: {}", population(&overall));
    write_calibration(&mut text, "  ", &overall);
    for family in Family::iter() {
        let calibration = BidCalibration::of(
            accessions
                .iter()
                .filter(|accession| accession.opening().family() == family),
        );
        if calibration.open + calibration.closed() > 0 {
            let _ = writeln!(text, "  {family}: {}", population(&calibration));
            write_calibration(&mut text, "    ", &calibration);
        }
    }
    let _ = writeln!(text, "Tests by quarter opened:");
    for (quarter, tests) in tests_by_quarter(accessions) {
        let acceptance = match tests.closed {
            0 => "no verdicts yet".to_string(),
            closed => format!("{:.0}%", 100.0 * tests.accepted as f64 / closed as f64),
        };
        let _ = writeln!(
            text,
            "  {quarter}: {} opened, {} closed, {} accepted ({acceptance})",
            tests.open + tests.closed,
            tests.closed,
            tests.accepted
        );
    }
    let closings: Vec<_> = accessions
        .iter()
        .filter_map(|accession| match accession.status() {
            Status::Closed(closing) => Some(closing.cost()),
            Status::Open => None,
        })
        .collect();
    let _ = writeln!(text, "Study cost over {} closed:", closings.len());
    write_spread(
        &mut text,
        "wall clock",
        &Spread::of(closings.iter().map(|cost| cost.wall_clock_seconds)),
        |seconds| format!("{seconds}s"),
    );
    write_spread(
        &mut text,
        "bytes read",
        &Spread::of(closings.iter().map(|cost| cost.bytes_read)),
        |bytes| bytes.to_string(),
    );
    write_spread(
        &mut text,
        "dollars",
        &Spread::of(
            closings
                .iter()
                .map(|cost| cost.dollars.map(Dollars::millionths)),
        ),
        |millionths| format!("${}", Dollars::from_millionths(millionths)),
    );
    let _ = match most_expensive_refutation(accessions) {
        Some((number, seconds)) => {
            writeln!(text, "  most expensive refutation: {number}, {seconds}s")
        }
        None => writeln!(
            text,
            "  most expensive refutation: no refutation recorded its wall clock"
        ),
    };
    text
}

fn write_spread(text: &mut String, name: &str, spread: &Spread, show: impl Fn(u64) -> String) {
    let _ = match spread.bounds() {
        Some((minimum, median, maximum)) => writeln!(
            text,
            "  {name}: {} recorded, {} unrecorded; minimum {}, lower median {}, maximum {}",
            spread.recorded.len(),
            spread.unrecorded,
            show(minimum),
            show(median),
            show(maximum)
        ),
        None => writeln!(text, "  {name}: none recorded of {}", spread.unrecorded),
    };
}

/// Every accession counted once, as scored or by why it could not be.
fn population(calibration: &BidCalibration) -> String {
    format!(
        "{} closed, {} open; {} scored, {} without an interval bid, {} without a measured value",
        calibration.closed(),
        calibration.open,
        calibration.scored,
        calibration.bid_absent,
        calibration.measurement_absent
    )
}

/// Coverage and error, written only when something was scored; the population line already says when nothing was.
fn write_calibration(text: &mut String, indent: &str, calibration: &BidCalibration) {
    if calibration.scored == 0 {
        return;
    }
    let caveat = match calibration.scored < MINIMUM_SCORED_BIDS {
        true => format!("; fewer than {MINIMUM_SCORED_BIDS} scored, so not yet meaningful"),
        false => String::new(),
    };
    let _ = writeln!(
        text,
        "{indent}covered {} of {} ({:.0}%) against a declared {:.0}%{caveat}",
        calibration.covered,
        calibration.scored,
        100.0 * calibration.covered as f64 / calibration.scored as f64,
        calibration.declared_coverage_percent as f64 / calibration.scored as f64
    );
    for (units, errors) in &calibration.by_units {
        let scored = errors.scored as f64;
        let _ = writeln!(
            text,
            "{indent}{}: {} scored, mean signed error {:+.2}, mean width {:.2}",
            units.as_str(),
            errors.scored,
            errors.signed_error / scored,
            errors.width / scored
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::NaiveDate;

    use crate::common::register::{Closing, Opening, Sample, StudyCost};

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    fn opened(number: u32, family: Family, bid: Bid, on: SessionDate) -> Accession {
        let opening = Opening::new(
            family,
            "liquid-common@1".parse().unwrap(),
            "1 sessions".parse().unwrap(),
            "the gap persists".to_string(),
            bid,
            on,
            None,
            None,
        )
        .unwrap();
        Accession::open(AccessionNumber::new(number).unwrap(), opening)
    }

    fn closed(
        accession: Accession,
        verdict: Verdict,
        measured: Measured,
        cost: StudyCost,
    ) -> Accession {
        let notes = match verdict {
            Verdict::Inconclusive | Verdict::LandedNotAdopted => Some("noted".to_string()),
            Verdict::Accept | Verdict::Refute => None,
        };
        accession
            .close(
                Closing::new(
                    verdict,
                    "read".to_string(),
                    measured,
                    Sample::Unrecorded,
                    Vec::new(),
                    session(2026, 9, 30),
                    notes,
                    cost,
                )
                .unwrap(),
            )
            .unwrap()
    }

    fn interval(raw: &str) -> Bid {
        Bid::Interval(raw.parse().unwrap())
    }

    fn seconds(wall_clock_seconds: u64) -> StudyCost {
        StudyCost {
            wall_clock_seconds: Some(wall_clock_seconds),
            ..StudyCost::default()
        }
    }

    fn register() -> Vec<Accession> {
        let on = session(2026, 9, 25);
        vec![
            // Covered at its upper bound, which is inside.
            closed(
                opened(1, Family::Overnight, interval("4 [0, 9] 80% net-bp"), on),
                Verdict::Accept,
                Measured::Value(9.0),
                seconds(60),
            ),
            // Missed below.
            closed(
                opened(2, Family::Overnight, interval("4 [0, 9] 90% net-bp"), on),
                Verdict::Refute,
                Measured::Value(-2.0),
                seconds(600),
            ),
            // Another unit, kept apart.
            closed(
                opened(
                    3,
                    Family::Execution,
                    interval("0.5 [0.3, 0.7] 80% fill-share"),
                    on,
                ),
                Verdict::Refute,
                Measured::Value(0.4),
                seconds(30),
            ),
            closed(
                opened(4, Family::Execution, interval("1 [0, 2] 80% net-bp"), on),
                Verdict::Inconclusive,
                Measured::NotMeasured,
                StudyCost::default(),
            ),
            closed(
                opened(5, Family::Baselines, Bid::Unrecorded, on),
                Verdict::Refute,
                Measured::Unrecorded,
                seconds(6000),
            ),
            opened(
                6,
                Family::Overnight,
                interval("4 [0, 9] 80% net-bp"),
                session(2026, 10, 1),
            ),
        ]
    }

    #[test]
    fn test_every_accession_is_scored_or_counted_by_its_cause() {
        let calibration = BidCalibration::of(&register());
        assert_eq!(
            (
                calibration.open,
                calibration.bid_absent,
                calibration.measurement_absent,
                calibration.scored,
                calibration.closed()
            ),
            (1, 1, 1, 3, 5)
        );
        assert_eq!(
            (calibration.covered, calibration.declared_coverage_percent),
            (2, 250)
        );
        assert_eq!(
            calibration
                .by_units
                .keys()
                .map(Unit::as_str)
                .collect::<Vec<_>>(),
            ["fill-share", "net-bp"]
        );
        let net = &calibration.by_units[&"net-bp".parse::<Unit>().unwrap()];
        // Errors +5 and -6; widths 9 and 9.
        assert_eq!((net.scored, net.signed_error, net.width), (2, -1.0, 18.0));
        let share = &calibration.by_units[&"fill-share".parse::<Unit>().unwrap()];
        assert_eq!(share.scored, 1);
        assert!((share.signed_error + 0.1).abs() < 1e-12 && (share.width - 0.4).abs() < 1e-12);
    }

    #[test]
    fn test_tests_are_counted_in_the_quarter_they_opened() {
        let register = vec![
            opened(1, Family::Overnight, Bid::Unrecorded, session(2026, 3, 31)),
            closed(
                opened(2, Family::Overnight, Bid::Unrecorded, session(2026, 4, 1)),
                Verdict::Accept,
                Measured::NotMeasured,
                StudyCost::default(),
            ),
            closed(
                opened(3, Family::Overnight, Bid::Unrecorded, session(2026, 6, 30)),
                Verdict::Refute,
                Measured::NotMeasured,
                StudyCost::default(),
            ),
        ];
        let quarters: Vec<(String, Tests)> = tests_by_quarter(&register)
            .into_iter()
            .map(|(quarter, tests)| (quarter.to_string(), tests))
            .collect();
        assert_eq!(
            quarters,
            [
                (
                    "2026Q1".to_string(),
                    Tests {
                        open: 1,
                        closed: 0,
                        accepted: 0
                    }
                ),
                (
                    "2026Q2".to_string(),
                    Tests {
                        open: 0,
                        closed: 2,
                        accepted: 1
                    }
                )
            ]
        );
    }

    #[test]
    fn test_a_spread_reports_recorded_values_beside_the_unrecorded() {
        let even = Spread::of([Some(30), None, Some(10), Some(40), Some(20)]);
        assert_eq!((even.unrecorded, even.bounds()), (1, Some((10, 20, 40))));
        assert_eq!(
            Spread::of([Some(5), Some(1), Some(3)]).bounds(),
            Some((1, 3, 5))
        );
        let none = Spread::of([None, None]);
        assert_eq!((none.unrecorded, none.bounds()), (2, None));
    }

    #[test]
    fn test_the_most_expensive_refutation_ignores_other_verdicts() {
        assert_eq!(
            most_expensive_refutation(&register()),
            Some((AccessionNumber::new(5).unwrap(), 6000))
        );
        assert_eq!(most_expensive_refutation(&register()[..1]), None);
    }

    #[test]
    fn test_the_report_prints_each_share_beside_its_population() {
        let report = report(&register());
        for line in [
            "Bids: 5 closed, 1 open; 3 scored, 1 without an interval bid, 1 without a measured value",
            "  covered 2 of 3 (67%) against a declared 83%; fewer than 20 scored, so not yet meaningful",
            "  net-bp: 2 scored, mean signed error -0.50, mean width 9.00",
            "  overnight: 2 closed, 1 open; 2 scored, 0 without an interval bid, 0 without a measured value",
            "  baselines: 1 closed, 0 open; 0 scored, 1 without an interval bid, 0 without a measured value",
            "  2026Q3: 5 opened, 5 closed, 1 accepted (20%)",
            "  2026Q4: 1 opened, 0 closed, 0 accepted (no verdicts yet)",
            "  wall clock: 4 recorded, 1 unrecorded; minimum 30s, lower median 60s, maximum 6000s",
            "  dollars: none recorded of 5",
            "  most expensive refutation: 000005, 6000s",
        ] {
            assert!(
                report.lines().any(|candidate| candidate == line),
                "missing {line:?} in\n{report}"
            );
        }
        // A family with no accessions has no section.
        assert!(!report.contains("forecast-model:"), "{report}");
    }

    #[test]
    fn test_the_caveat_lifts_at_twenty_scored_bids() {
        let on = session(2026, 9, 25);
        let register: Vec<Accession> = (1..=20)
            .map(|number| {
                closed(
                    opened(
                        number,
                        Family::Overnight,
                        interval("4 [0, 9] 80% net-bp"),
                        on,
                    ),
                    Verdict::Refute,
                    Measured::Value(1.0),
                    StudyCost::default(),
                )
            })
            .collect();
        assert!(!report(&register).contains("not yet meaningful"));
        assert!(report(&register[..19]).contains("not yet meaningful"));
        assert!(report(&[]).starts_with(
            "Bids: 0 closed, 0 open; 0 scored, 0 without an interval bid, 0 without a measured value\nTests"
        ));
    }
}
