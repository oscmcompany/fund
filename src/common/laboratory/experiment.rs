//! What the catalog records about a study: each dataset it read and each experiment it ran, with the inputs,
//! outputs and machine behind them, so past work can be found and read back rather than redone blind.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::common::laboratory::dataset::Fingerprint;
use crate::common::laboratory::estimate::Estimate;

/// Free text naming what a study is about, for searching the catalog; it gates nothing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Label(String);

/// The machine a study ran on: its hostname, so a laptop run and a researcher-host run read apart, and what can
/// change how a result reads, the architecture (floating-point results can differ across them) and the cores a
/// duration was measured on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MachineFields")]
pub struct Machine {
    hostname: String,
    architecture: String,
    operating_system: String,
    cores: NonZeroU32,
}

#[derive(Deserialize)]
struct MachineFields {
    hostname: String,
    architecture: String,
    operating_system: String,
    cores: NonZeroU32,
}

impl TryFrom<MachineFields> for Machine {
    type Error = ExperimentRefusal;

    fn try_from(fields: MachineFields) -> Result<Self, Self::Error> {
        Self::new(
            fields.hostname,
            fields.architecture,
            fields.operating_system,
            fields.cores,
        )
    }
}

/// Milliseconds since a study opened, measured on the study's monotonic clock and stored as a count so a pure module
/// holds no clock type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Elapsed(u64);

impl Elapsed {
    pub fn from_milliseconds(milliseconds: u64) -> Self {
        Self(milliseconds)
    }

    pub fn milliseconds(self) -> u64 {
        self.0
    }
}

/// The name of a setting, estimate or metric; validated wherever it is built, deserialization included.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Name(String);

/// One experiment's settings, by name.
///
/// Strings to strings on purpose: whether two runs tried the same variant is answered by equality, and strings
/// compare exactly where floats do not (0.1 written two ways, NaN); untyped values also let every study name its
/// own settings without a schema change. Typing the values would make float equality decide what counts as a repeat.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Parameters(BTreeMap<Name, String>);

#[derive(Debug, Clone, PartialEq)]
pub enum ExperimentRefusal {
    BlankLabel,
    BlankMachine,
    /// A label or name with a line break would split a catalog line.
    LineBreak {
        text: String,
    },
    BlankName,
    /// A parameter, estimate or metric named twice, where keeping either value would hide the other.
    Duplicate {
        name: String,
    },
    /// JSON has no NaN or infinity, so a metric that is neither finite nor absent cannot be journaled.
    NotFinite {
        metric: String,
        value: f64,
    },
}

impl std::fmt::Display for ExperimentRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlankLabel => write!(formatter, "a study needs a label"),
            Self::BlankMachine => write!(
                formatter,
                "a machine needs a hostname, an architecture and an operating system"
            ),
            Self::LineBreak { text } => write!(formatter, "{text:?} holds a line break"),
            Self::BlankName => write!(formatter, "a parameter, estimate or metric needs a name"),
            Self::Duplicate { name } => write!(formatter, "{name} is named twice"),
            Self::NotFinite { metric, value } => {
                write!(formatter, "metric {metric} is {value}, which is not finite")
            }
        }
    }
}

impl std::error::Error for ExperimentRefusal {}

fn text(raw: String, blank: ExperimentRefusal) -> Result<String, ExperimentRefusal> {
    match (raw.trim().is_empty(), raw.contains(['\n', '\r'])) {
        (true, _) => Err(blank),
        (false, true) => Err(ExperimentRefusal::LineBreak { text: raw }),
        (false, false) => Ok(raw),
    }
}

impl Label {
    pub fn new(raw: impl Into<String>) -> Result<Self, ExperimentRefusal> {
        text(raw.into(), ExperimentRefusal::BlankLabel).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Machine {
    pub fn new(
        hostname: impl Into<String>,
        architecture: impl Into<String>,
        operating_system: impl Into<String>,
        cores: NonZeroU32,
    ) -> Result<Self, ExperimentRefusal> {
        let field = |raw: String| text(raw.trim().to_string(), ExperimentRefusal::BlankMachine);
        Ok(Self {
            hostname: field(hostname.into())?,
            architecture: field(architecture.into())?,
            operating_system: field(operating_system.into())?,
            cores,
        })
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn architecture(&self) -> &str {
        &self.architecture
    }

    pub fn operating_system(&self) -> &str {
        &self.operating_system
    }

    pub fn cores(&self) -> NonZeroU32 {
        self.cores
    }
}

impl Name {
    pub fn new(raw: impl Into<String>) -> Result<Self, ExperimentRefusal> {
        text(raw.into(), ExperimentRefusal::BlankName).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// So a map keyed by `Name` is indexed by a plain `&str`.
impl Borrow<str> for Name {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Parameters {
    pub fn new<Setting: Into<String>, Value: Into<String>>(
        settings: impl IntoIterator<Item = (Setting, Value)>,
    ) -> Result<Self, ExperimentRefusal> {
        let mut held = BTreeMap::new();
        for (name, value) in settings {
            let name = Name::new(name)?;
            if held.contains_key(&name) {
                return Err(ExperimentRefusal::Duplicate { name: name.0 });
            }
            held.insert(name, value.into());
        }
        Ok(Self(held))
    }

    pub fn settings(&self) -> &BTreeMap<Name, String> {
        &self.0
    }
}

macro_rules! string_conversions {
    ($type:ty, $inner:ty) => {
        impl TryFrom<$inner> for $type {
            type Error = ExperimentRefusal;

            fn try_from(raw: $inner) -> Result<Self, Self::Error> {
                Self::new(raw)
            }
        }

        impl From<$type> for $inner {
            fn from(value: $type) -> Self {
                value.0
            }
        }
    };
}

string_conversions!(Label, String);
string_conversions!(Name, String);

/// A loader read `fingerprint` for the study `label` names, on `machine`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetRead {
    label: Label,
    machine: Machine,
    fingerprint: Fingerprint,
}

impl DatasetRead {
    pub fn new(label: Label, machine: Machine, fingerprint: Fingerprint) -> Self {
        Self {
            label,
            machine,
            fingerprint,
        }
    }

    pub fn label(&self) -> &Label {
        &self.label
    }

    pub fn machine(&self) -> &Machine {
        &self.machine
    }

    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }
}

/// One experiment: its inputs (settings and the data read), its outputs (named estimates and metrics), and where
/// and how long into its study it ran. The run and commit are on the record that carries it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentRan {
    label: Label,
    machine: Machine,
    parameters: Parameters,
    fingerprints: Vec<Fingerprint>,
    estimates: BTreeMap<Name, Estimate>,
    metrics: BTreeMap<Name, f64>,
    /// Cumulative from the study's opening, not this experiment's own time, which is the difference from the
    /// experiment recorded before it.
    since_opened: Elapsed,
}

/// The outputs an experiment reports, kept apart from its inputs so a caller names each once.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Outputs {
    estimates: BTreeMap<Name, Estimate>,
    metrics: BTreeMap<Name, f64>,
}

impl Outputs {
    pub fn estimate(
        mut self,
        name: impl Into<String>,
        estimate: Estimate,
    ) -> Result<Self, ExperimentRefusal> {
        let name = Name::new(name)?;
        if self.estimates.contains_key(&name) {
            return Err(ExperimentRefusal::Duplicate { name: name.0 });
        }
        self.estimates.insert(name, estimate);
        Ok(self)
    }

    pub fn metric(
        mut self,
        name: impl Into<String>,
        value: f64,
    ) -> Result<Self, ExperimentRefusal> {
        let name = Name::new(name)?;
        if !value.is_finite() {
            return Err(ExperimentRefusal::NotFinite {
                metric: name.0,
                value,
            });
        }
        if self.metrics.contains_key(&name) {
            return Err(ExperimentRefusal::Duplicate { name: name.0 });
        }
        self.metrics.insert(name, value);
        Ok(self)
    }
}

impl ExperimentRan {
    pub fn new(
        label: Label,
        machine: Machine,
        parameters: Parameters,
        fingerprints: Vec<Fingerprint>,
        outputs: Outputs,
        since_opened: Elapsed,
    ) -> Self {
        Self {
            label,
            machine,
            parameters,
            fingerprints,
            estimates: outputs.estimates,
            metrics: outputs.metrics,
            since_opened,
        }
    }

    pub fn label(&self) -> &Label {
        &self.label
    }

    pub fn machine(&self) -> &Machine {
        &self.machine
    }

    pub fn parameters(&self) -> &Parameters {
        &self.parameters
    }

    pub fn fingerprints(&self) -> &[Fingerprint] {
        &self.fingerprints
    }

    pub fn estimates(&self) -> &BTreeMap<Name, Estimate> {
        &self.estimates
    }

    pub fn metrics(&self) -> &BTreeMap<Name, f64> {
        &self.metrics
    }

    pub fn since_opened(&self) -> Elapsed {
        self.since_opened
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;
    use crate::common::laboratory::dataset::DatasetLeg;
    use crate::common::laboratory::estimate::summarize;
    use crate::common::laboratory::series::Series;
    use crate::common::time::calendar::{TradingCalendar, TradingSession};
    use crate::common::time::{SessionDate, SessionRange};

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    fn fingerprint() -> Fingerprint {
        let (open, close) = (
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        );
        let calendar = TradingCalendar::new(
            (0..3)
                .map(|day| TradingSession::new(session(day), open, close).unwrap())
                .collect(),
            SessionRange::new(session(0), session(2)).unwrap(),
        )
        .unwrap();
        let read = [0, 2]
            .map(|day| (session(day), format!("\"tag-{day}\"")))
            .into();
        Fingerprint::new(
            DatasetLeg::MassiveDailyBars,
            SessionRange::new(session(0), session(2)).unwrap(),
            &calendar,
            read,
        )
        .unwrap()
    }

    #[test]
    fn test_catalog_text_refuses_what_would_not_search_or_split_a_line() {
        assert_eq!(Label::new("  "), Err(ExperimentRefusal::BlankLabel));
        assert_eq!(
            Label::new("gap\npersists"),
            Err(ExperimentRefusal::LineBreak {
                text: "gap\npersists".to_string()
            })
        );
        let cores = NonZeroU32::new(8).unwrap();
        assert_eq!(
            Machine::new("\n", "aarch64", "macos", cores),
            Err(ExperimentRefusal::BlankMachine)
        );
        assert_eq!(
            Machine::new("laptop", "", "macos", cores),
            Err(ExperimentRefusal::BlankMachine)
        );
        assert_eq!(
            Machine::new("ip-10-0-0-1\n", "x86_64", "linux", cores)
                .unwrap()
                .hostname(),
            "ip-10-0-0-1"
        );
        assert!(
            serde_json::from_str::<Machine>(
                r#"{"hostname":"laptop","architecture":" ","operating_system":"macos","cores":8}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<Machine>(
                r#"{"hostname":"laptop","architecture":"aarch64","operating_system":"macos","cores":0}"#
            )
            .is_err()
        );
        assert_eq!(
            Parameters::new([("", "1")]),
            Err(ExperimentRefusal::BlankName)
        );
        assert_eq!(
            Outputs::default()
                .metric("hit rate", f64::NAN)
                .map_err(|refusal| refusal.to_string()),
            Err("metric hit rate is NaN, which is not finite".to_string())
        );
        assert!(matches!(
            Outputs::default().metric("hit rate", f64::INFINITY),
            Err(ExperimentRefusal::NotFinite { .. })
        ));
        assert!(serde_json::from_str::<Label>("\" \"").is_err());
        assert!(serde_json::from_str::<Parameters>(r#"{"":"1"}"#).is_err());
        assert!(serde_json::from_str::<Name>("\"net\\nreturn\"").is_err());
    }

    /// A name given twice is refused with itself, for a setting, an estimate and a metric alike, rather than the
    /// later value quietly replacing the earlier.
    #[test]
    fn test_a_name_given_twice_is_refused() {
        let duplicate = Err(ExperimentRefusal::Duplicate {
            name: "side".to_string(),
        });
        assert_eq!(
            Parameters::new([("side", "long"), ("side", "short")]).map(|_| ()),
            duplicate
        );
        assert_eq!(
            Outputs::default()
                .metric("side", 1.0)
                .and_then(|outputs| outputs.metric("side", 2.0))
                .map(|_| ()),
            duplicate
        );
        let estimate = Estimate::try_from(summarize(
            &Series::new([(session(0), Some(1.0)), (session(1), Some(2.0))]).unwrap(),
        ))
        .unwrap();
        assert_eq!(
            Outputs::default()
                .estimate("side", estimate)
                .and_then(|outputs| outputs.estimate("side", estimate))
                .map(|_| ()),
            duplicate
        );
    }

    #[test]
    fn test_parameters_compare_as_written() {
        let written = Parameters::new([("lookback", "0.1"), ("side", "long")]).unwrap();
        assert_ne!(
            written,
            Parameters::new([("lookback", "0.10"), ("side", "long")]).unwrap()
        );
        assert_eq!(
            serde_json::to_string(&written).unwrap(),
            r#"{"lookback":"0.1","side":"long"}"#
        );
    }

    proptest! {
        /// An experiment reads back from the journal as written, estimates and all.
        #[test]
        fn test_an_experiment_round_trips(
            readings in prop::collection::vec(-1000.0..1000.0f64, 2..20),
            settings in prop::collection::btree_map("[a-z]{1,8}", "[ -~]{0,8}", 0..4),
            metric in -1e6..1e6f64,
            milliseconds in 0..100_000u64,
        ) {
            let series = Series::new(
                readings.iter().enumerate().map(|(day, value)| (session(day as i64), Some(*value))),
            )
            .unwrap();
            let estimate = Estimate::try_from(summarize(&series)).unwrap();
            let ran = ExperimentRan::new(
                Label::new("overnight gap").unwrap(),
                Machine::new("laptop", "aarch64", "macos", NonZeroU32::new(8).unwrap()).unwrap(),
                Parameters::new(settings).unwrap(),
                vec![fingerprint()],
                Outputs::default()
                    .estimate("net", estimate)
                    .unwrap()
                    .metric("hit rate", metric)
                    .unwrap(),
                Elapsed::from_milliseconds(milliseconds),
            );
            let encoded = serde_json::to_string(&ran).unwrap();
            prop_assert_eq!(serde_json::from_str::<ExperimentRan>(&encoded).unwrap(), ran);
        }
    }
}
