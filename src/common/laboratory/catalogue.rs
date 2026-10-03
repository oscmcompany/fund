//! Reads the catalogue back: which journaled experiments a query selects, and one line per experiment for a reader
//! to scan before starting the next.

use crate::common::heal::Leg;
use crate::common::journal::{Observation, Record};
use crate::common::laboratory::experiment::ExperimentRan;
use crate::common::time::SessionDate;

/// Which experiments to show; every filter left out selects everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// Lowercased, so a label matches whatever its case.
    label: Option<String>,
    leg: Option<Leg>,
    since: Option<SessionDate>,
    until: Option<SessionDate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryRefusal {
    Inverted {
        since: SessionDate,
        until: SessionDate,
    },
}

impl std::fmt::Display for QueryRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inverted { since, until } => {
                write!(formatter, "since {since} is after until {until}")
            }
        }
    }
}

impl std::error::Error for QueryRefusal {}

impl Query {
    /// `label` matches any experiment whose label contains it; `since` and `until` bound the session it ran in.
    pub fn new(
        label: Option<&str>,
        leg: Option<Leg>,
        since: Option<SessionDate>,
        until: Option<SessionDate>,
    ) -> Result<Self, QueryRefusal> {
        if let (Some(since), Some(until)) = (since, until)
            && until < since
        {
            return Err(QueryRefusal::Inverted { since, until });
        }
        Ok(Self {
            label: label.map(str::to_lowercase),
            leg,
            since,
            until,
        })
    }

    /// Whether a session's journal can hold a selected experiment, so an object outside the window is never fetched.
    pub fn selects_session(&self, session: SessionDate) -> bool {
        self.since.is_none_or(|since| since <= session)
            && self.until.is_none_or(|until| session <= until)
    }

    /// The experiment `record` holds when the query selects it.
    pub fn selects<'record>(&self, record: &'record Record) -> Option<&'record ExperimentRan> {
        let experiment = match record.observation() {
            Observation::ExperimentRan(experiment) => experiment,
            Observation::ConfigurationResolved(_)
            | Observation::PartitionWritten(_)
            | Observation::HealFinished(_)
            | Observation::DatasetRead(_) => return None,
        };
        let labelled = self.label.as_ref().is_none_or(|label| {
            experiment
                .label()
                .as_str()
                .to_lowercase()
                .contains(label.as_str())
        });
        let on_leg = self.leg.is_none_or(|leg| {
            experiment
                .fingerprints()
                .iter()
                .any(|fingerprint| fingerprint.leg() == leg)
        });
        (labelled && on_leg && self.selects_session(record.session())).then_some(experiment)
    }
}

/// One experiment as a line: when, what, which settings, over which window, what it measured, and which build, host
/// and run produced it. Setting values are escaped, so a line break inside one cannot split the line.
pub fn line(record: &Record, experiment: &ExperimentRan) -> String {
    let parameters = experiment
        .parameters()
        .settings()
        .iter()
        .map(|(name, value)| format!("{}={}", name.as_str(), value.escape_debug()))
        .collect::<Vec<_>>();
    let mut legs = experiment
        .fingerprints()
        .iter()
        .map(|fingerprint| fingerprint.leg().to_string())
        .collect::<Vec<_>>();
    legs.sort();
    legs.dedup();
    let first = experiment
        .fingerprints()
        .iter()
        .map(|fingerprint| fingerprint.first())
        .min();
    let last = experiment
        .fingerprints()
        .iter()
        .map(|fingerprint| fingerprint.last())
        .max();
    let window = match (first, last) {
        (Some(first), Some(last)) => format!("{} {first}..{last}", legs.join("+")),
        (None, _) | (_, None) => "no datasets".to_string(),
    };
    let estimates = experiment.estimates().iter().map(|(name, estimate)| {
        let t = estimate
            .t()
            .map_or("none".to_string(), |t| format!("{t:.2}"));
        format!(
            "{}={}±{} t={t} n={} undefined={}",
            name.as_str(),
            number(estimate.mean()),
            number(estimate.standard_error()),
            estimate.sessions(),
            estimate.undefined(),
        )
    });
    let metrics = experiment
        .metrics()
        .iter()
        .map(|(name, value)| format!("{}={}", name.as_str(), number(*value)));
    let outputs = estimates.chain(metrics).collect::<Vec<_>>();
    let commit = record.commit().map_or("no-commit".to_string(), |commit| {
        let dirty = if commit.is_dirty() { "-dirty" } else { "" };
        format!("{}{dirty}", &commit.as_str()[..8])
    });
    let machine = experiment.machine();
    format!(
        "{}  {}  [{}]  {window}  {}  {commit}  {} {}/{} {}c  run {}",
        record.timestamp().format("%Y-%m-%dT%H:%M:%SZ"),
        experiment.label().as_str(),
        parameters.join(" "),
        if outputs.is_empty() {
            "no outputs".to_string()
        } else {
            outputs.join("  ")
        },
        machine.hostname(),
        machine.architecture(),
        machine.operating_system(),
        machine.cores(),
        record.run_id(),
    )
}

/// Four decimals where that reads well, scientific notation where it would round to zero or run long.
fn number(value: f64) -> String {
    if value == 0.0 || (1e-3..1e6).contains(&value.abs()) {
        format!("{value:.4}")
    } else {
        format!("{value:.3e}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroU64};

    use chrono::{NaiveDate, NaiveTime};
    use uuid::Uuid;

    use super::*;

    use crate::common::journal::{Commit, RunId};
    use crate::common::laboratory::dataset::Fingerprint;
    use crate::common::laboratory::estimate::{Estimate, summarize};
    use crate::common::laboratory::experiment::{
        DatasetRead, Elapsed, Label, Machine, Outputs, Parameters,
    };
    use crate::common::laboratory::series::Series;
    use crate::common::time::calendar::{TradingCalendar, TradingSession};

    fn session(day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, day).unwrap())
    }

    /// 2026-09-21 to 2026-09-25, Monday to Friday.
    fn fingerprint(leg: Leg, first: u32, last: u32) -> Fingerprint {
        let open = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
        let close = NaiveTime::from_hms_opt(16, 0, 0).unwrap();
        let calendar = TradingCalendar::new(
            (21..=25)
                .map(|day| TradingSession::new(session(day), open, close).unwrap())
                .collect(),
            session(21),
            session(27),
        )
        .unwrap();
        Fingerprint::new(
            leg,
            session(first),
            session(last),
            &calendar,
            BTreeMap::new(),
        )
        .unwrap()
    }

    fn machine() -> Machine {
        Machine::new("laptop", "aarch64", "macos", NonZeroU32::new(8).unwrap()).unwrap()
    }

    fn experiment_named(
        label: &str,
        fingerprints: Vec<Fingerprint>,
        outputs: Outputs,
    ) -> ExperimentRan {
        ExperimentRan::new(
            Label::new(label).unwrap(),
            machine(),
            Parameters::new([("lookback", "20"), ("cost", "10bp")]).unwrap(),
            fingerprints,
            outputs,
            Elapsed::from_milliseconds(1500),
        )
    }

    fn record(run: u128, day: u32, observation: Observation) -> Record {
        Record::new(
            RunId::new(Uuid::from_u128(run)),
            NonZeroU64::MIN,
            format!("2026-09-{day}T15:00:00Z").parse().unwrap(),
            Some(Commit::new("0123456789abcdef0123456789abcdef01234567-dirty").unwrap()),
            observation,
        )
    }

    fn ran(run: u128, day: u32, label: &str, leg: Leg) -> Record {
        let experiment =
            experiment_named(label, vec![fingerprint(leg, 21, 25)], Outputs::default());
        record(run, day, Observation::ExperimentRan(Box::new(experiment)))
    }

    fn selected(query: &Query, records: &[Record]) -> Vec<RunId> {
        records
            .iter()
            .filter(|record| query.selects(record).is_some())
            .map(Record::run_id)
            .collect()
    }

    #[test]
    fn test_a_query_selects_by_label_leg_and_session() {
        let read = record(
            9,
            22,
            Observation::DatasetRead(Box::new(DatasetRead::new(
                Label::new("Overnight drift").unwrap(),
                machine(),
                fingerprint(Leg::MassiveDailyBars, 21, 25),
            ))),
        );
        let records = [
            ran(1, 22, "Overnight drift", Leg::MassiveDailyBars),
            ran(2, 24, "overnight drift, screened", Leg::AlpacaMinuteBars),
            ran(3, 26, "Intraday reversal", Leg::AlpacaMinuteBars),
            read,
        ];
        let run = |id| RunId::new(Uuid::from_u128(id));
        let everything = Query::new(None, None, None, None).unwrap();
        assert_eq!(selected(&everything, &records), [run(1), run(2), run(3)]);
        let drift = Query::new(Some("OVERNIGHT"), None, None, None).unwrap();
        assert_eq!(selected(&drift, &records), [run(1), run(2)]);
        let minute = Query::new(None, Some(Leg::AlpacaMinuteBars), None, None).unwrap();
        assert_eq!(selected(&minute, &records), [run(2), run(3)]);
        let window = Query::new(None, None, Some(session(23)), Some(session(24))).unwrap();
        assert_eq!(selected(&window, &records), [run(2)]);
        assert!(window.selects_session(session(23)) && window.selects_session(session(24)));
        assert!(!window.selects_session(session(22)) && !window.selects_session(session(25)));
    }

    #[test]
    fn test_a_setting_with_line_breaks_stays_on_one_line() {
        let experiment = ExperimentRan::new(
            Label::new("Breaks").unwrap(),
            machine(),
            Parameters::new([("note", "first\nsecond\rthird")]).unwrap(),
            Vec::new(),
            Outputs::default(),
            Elapsed::from_milliseconds(0),
        );
        let record = record(
            1,
            22,
            Observation::ExperimentRan(Box::new(experiment.clone())),
        );
        let text = line(&record, &experiment);
        assert!(!text.contains(['\n', '\r']), "{text}");
        assert!(text.contains(r"[note=first\nsecond\rthird]"), "{text}");
    }

    #[test]
    fn test_a_query_refuses_an_inverted_window() {
        assert_eq!(
            Query::new(None, None, Some(session(24)), Some(session(23))),
            Err(QueryRefusal::Inverted {
                since: session(24),
                until: session(23),
            })
        );
    }

    #[test]
    fn test_an_experiment_reads_as_one_line() {
        let series = Series::new(BTreeMap::from([
            (session(21), Some(1.0)),
            (session(22), Some(3.0)),
            (session(23), None),
        ]))
        .unwrap();
        let flat = Series::new(BTreeMap::from([
            (session(21), Some(2.0)),
            (session(22), Some(2.0)),
        ]))
        .unwrap();
        let outputs = Outputs::default()
            .estimate("drift", Estimate::try_from(summarize(&series)).unwrap())
            .unwrap()
            .estimate("flat", Estimate::try_from(summarize(&flat)).unwrap())
            .unwrap()
            .metric("hit_rate", 0.000_012_5)
            .unwrap();
        let experiment = experiment_named(
            "Overnight drift",
            vec![
                fingerprint(Leg::MassiveDailyBars, 22, 25),
                fingerprint(Leg::AlpacaMinuteBars, 21, 23),
                fingerprint(Leg::MassiveDailyBars, 21, 24),
            ],
            outputs,
        );
        let record = record(
            1,
            22,
            Observation::ExperimentRan(Box::new(experiment.clone())),
        );
        assert_eq!(
            line(&record, &experiment),
            "2026-09-22T15:00:00Z  Overnight drift  [cost=10bp lookback=20]  \
             alpaca_minute_bars+massive_daily_bars 2026-09-21..2026-09-25  \
             drift=2.0000±1.0000 t=2.00 n=2 undefined=1  flat=2.0000±0.0000 t=none n=2 undefined=0  \
             hit_rate=1.250e-5  01234567-dirty  laptop aarch64/macos 8c  \
             run 00000000-0000-0000-0000-000000000001"
        );
        let bare = experiment_named("Bare", Vec::new(), Outputs::default());
        let bare_record = Record::new(
            RunId::new(Uuid::from_u128(2)),
            NonZeroU64::MIN,
            "2026-09-22T15:00:00Z".parse().unwrap(),
            None,
            Observation::ExperimentRan(Box::new(bare.clone())),
        );
        assert_eq!(
            line(&bare_record, &bare),
            "2026-09-22T15:00:00Z  Bare  [cost=10bp lookback=20]  no datasets  no outputs  no-commit  \
             laptop aarch64/macos 8c  run 00000000-0000-0000-0000-000000000002"
        );
    }
}
