//! What a study asked and what it concluded: the question declared with its baseline and bar before any reading, the
//! checks beside each experiment, and the verdict that closes the question with its scope and what would reopen it.

use serde::{Deserialize, Serialize};

use crate::common::journal::RunId;
use crate::common::laboratory::experiment::{
    ExperimentRefusal, Label, Metric, Name, Parameters, text,
};
use crate::common::laboratory::share::Share;
use crate::common::time::SessionDate;

macro_rules! statement {
    ($(#[$documentation:meta])* $type:ident) => {
        $(#[$documentation])*
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $type(String);

        impl $type {
            pub fn new(raw: impl Into<String>) -> Result<Self, ExperimentRefusal> {
                text(raw.into(), ExperimentRefusal::BlankStatement).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $type {
            type Error = ExperimentRefusal;

            fn try_from(raw: String) -> Result<Self, Self::Error> {
                Self::new(raw)
            }
        }

        impl From<$type> for String {
            fn from(value: $type) -> Self {
                value.0
            }
        }
    };
}

statement!(
    /// What a study set out to answer, in one line; a verdict names the question it closes by this text.
    Question
);
statement!(
    /// Exactly what a refutation covers: the universe, sessions, costs and horizon it was measured under.
    Scope
);
statement!(
    /// What change in capability or data would reopen a refutation: a cost level, a data source, a latency.
    Reopen
);
statement!(
    /// Why something was not measured or not decided.
    Cause
);

/// Where a metric must land for the question's answer to be yes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bar {
    Above(Metric),
    Below(Metric),
}

/// A question declared before reading: the trivial baseline skill is quoted against, the metric the bar reads, and
/// the session after which later data is the "as of" confirm read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuestionDeclared {
    label: Label,
    question: Question,
    baseline: Name,
    metric: Name,
    bar: Bar,
    as_of: SessionDate,
    parameters: Parameters,
}

impl QuestionDeclared {
    pub fn new(
        label: Label,
        question: Question,
        baseline: Name,
        metric: Name,
        bar: Bar,
        as_of: SessionDate,
        parameters: Parameters,
    ) -> Self {
        Self {
            label,
            question,
            baseline,
            metric,
            bar,
            as_of,
            parameters,
        }
    }

    pub fn label(&self) -> &Label {
        &self.label
    }

    pub fn question(&self) -> &Question {
        &self.question
    }

    pub fn baseline(&self) -> &Name {
        &self.baseline
    }

    pub fn metric(&self) -> &Name {
        &self.metric
    }

    pub fn bar(&self) -> Bar {
        self.bar
    }

    pub fn as_of(&self) -> SessionDate {
        self.as_of
    }

    pub fn parameters(&self) -> &Parameters {
        &self.parameters
    }
}

/// The permutation null's reading of the experiment's metric, or why none was taken.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullReading {
    Measured(Metric),
    Unmeasured { cause: Cause },
}

/// What an experiment's estimate stands on: the population it read, how many of those left it undefined, and the
/// null it must clear.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ChecksFields")]
pub struct Checks {
    population: u64,
    undefined: u64,
    null: NullReading,
}

#[derive(Deserialize)]
struct ChecksFields {
    population: u64,
    undefined: u64,
    null: NullReading,
}

impl TryFrom<ChecksFields> for Checks {
    type Error = ExperimentRefusal;

    fn try_from(fields: ChecksFields) -> Result<Self, Self::Error> {
        Self::new(fields.population, fields.undefined, fields.null)
    }
}

impl Checks {
    pub fn new(
        population: u64,
        undefined: u64,
        null: NullReading,
    ) -> Result<Self, ExperimentRefusal> {
        if undefined > population {
            return Err(ExperimentRefusal::UndefinedExceedsPopulation {
                undefined,
                population,
            });
        }
        Ok(Self {
            population,
            undefined,
            null,
        })
    }

    pub fn population(&self) -> u64 {
        self.population
    }

    pub fn undefined(&self) -> u64 {
        self.undefined
    }

    /// The undefined count over the population, or `None` over an empty population.
    pub fn undefined_share(&self) -> Option<Share> {
        Share::of(self.undefined, self.population)
    }

    pub fn null(&self) -> &NullReading {
        &self.null
    }
}

/// How a question closed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, strum::IntoStaticStr)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Outcome {
    /// Cleared its bar with `reading`, until the "as of" read says otherwise.
    Live {
        reading: Metric,
    },
    Refuted {
        scope: Scope,
        reopen: Reopen,
    },
    Inconclusive {
        cause: Cause,
    },
}

/// A question closed, with the runs it rests on; `runs` is empty for a finding recorded after its runs' journals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerdictReached {
    label: Label,
    question: Question,
    outcome: Outcome,
    runs: Vec<RunId>,
}

impl VerdictReached {
    pub fn new(label: Label, question: Question, outcome: Outcome, runs: Vec<RunId>) -> Self {
        Self {
            label,
            question,
            outcome,
            runs,
        }
    }

    pub fn label(&self) -> &Label {
        &self.label
    }

    pub fn question(&self) -> &Question {
        &self.question
    }

    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub fn runs(&self) -> &[RunId] {
        &self.runs
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_a_statement_refuses_blank_text_and_line_breaks() {
        assert_eq!(Question::new(" "), Err(ExperimentRefusal::BlankStatement));
        assert_eq!(
            Scope::new("a\nb"),
            Err(ExperimentRefusal::LineBreak {
                text: "a\nb".to_string()
            })
        );
        assert_eq!(
            Reopen::new("a cost of 1bp").unwrap().as_str(),
            "a cost of 1bp"
        );
        assert!(serde_json::from_str::<Cause>(r#""""#).is_err());
    }

    #[test]
    fn test_checks_refuse_more_undefined_than_the_population() {
        let null = || NullReading::Measured(Metric::new(0.0).unwrap());
        assert_eq!(
            Checks::new(4, 5, null()),
            Err(ExperimentRefusal::UndefinedExceedsPopulation {
                undefined: 5,
                population: 4
            })
        );
        assert_eq!(
            Checks::new(4, 1, null())
                .unwrap()
                .undefined_share()
                .map(Share::get),
            Some(0.25)
        );
        assert_eq!(Checks::new(0, 0, null()).unwrap().undefined_share(), None);
        let decoded = serde_json::from_str::<Checks>(
            r#"{"population":4,"undefined":5,"null":{"measured":0.0}}"#,
        );
        assert!(decoded.is_err(), "{decoded:?}");
    }

    #[test]
    fn test_the_research_records_encode_to_their_wire_format() {
        let declared = QuestionDeclared::new(
            Label::new("study").unwrap(),
            Question::new("does the arm beat its baseline").unwrap(),
            Name::new("null_strategy").unwrap(),
            Name::new("net_bp").unwrap(),
            Bar::Above(Metric::new(1.5).unwrap()),
            SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 30).unwrap()),
            Parameters::new([("horizon", "15m")]).unwrap(),
        );
        let encoded = serde_json::to_string(&declared).unwrap();
        assert_eq!(
            encoded,
            r#"{"label":"study","question":"does the arm beat its baseline","baseline":"null_strategy","#.to_string()
                + r#""metric":"net_bp","bar":{"above":1.5},"as_of":"2026-10-30","parameters":{"horizon":"15m"}}"#
        );
        assert_eq!(
            serde_json::from_str::<QuestionDeclared>(&encoded).unwrap(),
            declared
        );
        let checks = Checks::new(
            1254,
            3,
            NullReading::Unmeasured {
                cause: Cause::new("no permutation run").unwrap(),
            },
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&checks).unwrap(),
            r#"{"population":1254,"undefined":3,"null":{"unmeasured":{"cause":"no permutation run"}}}"#
        );
        let run = RunId::new(Uuid::nil());
        let verdicts = [
            (
                Outcome::Live {
                    reading: Metric::new(2.0).unwrap(),
                },
                r#"{"kind":"live","reading":2.0}"#,
            ),
            (
                Outcome::Refuted {
                    scope: Scope::new("liquid names, 15 minutes").unwrap(),
                    reopen: Reopen::new("a cost under 1bp").unwrap(),
                },
                r#"{"kind":"refuted","scope":"liquid names, 15 minutes","reopen":"a cost under 1bp"}"#,
            ),
            (
                Outcome::Inconclusive {
                    cause: Cause::new("too few sessions").unwrap(),
                },
                r#"{"kind":"inconclusive","cause":"too few sessions"}"#,
            ),
        ];
        for (outcome, encoded) in verdicts {
            let kind: &'static str = (&outcome).into();
            let reached = VerdictReached::new(
                Label::new("study").unwrap(),
                Question::new("q").unwrap(),
                outcome,
                vec![run],
            );
            let wire = serde_json::to_string(&reached).unwrap();
            assert_eq!(
                wire,
                format!(
                    r#"{{"label":"study","question":"q","outcome":{encoded},"runs":["00000000-0000-0000-0000-000000000000"]}}"#
                )
            );
            assert_eq!(
                serde_json::from_str::<VerdictReached>(&wire).unwrap(),
                reached
            );
            assert!(encoded.contains(&format!(r#""kind":"{kind}""#)));
        }
    }

    proptest! {
        #[test]
        fn test_checks_round_trip_through_json(population in 0u64..1_000_000, part in 0.0f64..=1.0, null in -1e6f64..1e6) {
            let undefined = (population as f64 * part) as u64;
            let checks = Checks::new(population, undefined, NullReading::Measured(Metric::new(null).unwrap())).unwrap();
            prop_assert_eq!(serde_json::from_str::<Checks>(&serde_json::to_string(&checks).unwrap()).unwrap(), checks);
        }
    }
}
