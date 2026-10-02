//! A study compares two arms read per session and is scored in the lane it was declared in: a registered study
//! quotes standard errors against its family's haircut, and an exploratory one reports only an effect against a
//! kill line, so a number that was never registered cannot be quoted as if it had been.

pub mod cost;
pub mod haircut;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::common::laboratory::cost::{BasisPoints, CostModel, CostRefusal};
use crate::common::laboratory::haircut::Haircut;
use crate::common::register::{AccessionNumber, Family, Horizon, OpenAccession, Unit, Universe};
use crate::common::time::SessionDate;

/// What a reading measures, and therefore whether a round trip is charged against it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "quantity", rename_all = "snake_case")]
pub enum Quantity {
    /// Signed basis points of return per round trip, charged at a declared spread rather than one inherited from the
    /// archive.
    ReturnPerRoundTrip {
        cost_model: CostModel,
        quoted_spread: BasisPoints,
    },
    /// A reading with no round trip behind it: a correlation, a share, a rate.
    Unpriced { units: Unit },
}

/// How the arms relate, which decides how their errors combine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Pairing {
    /// Both arms read the same sessions, so the difference is taken per session and shared variation cancels.
    Matched,
    /// The arms read no session in common, so their errors add in quadrature.
    Disjoint,
}

/// One side of a comparison, read per session because sessions are what vary; rows within a session share their
/// legs, so a per-event or per-bar arm folds to the session and `observations` carries the row count.
#[derive(Debug, Clone, PartialEq)]
pub struct Arm {
    name: String,
    readings: BTreeMap<SessionDate, Option<f64>>,
    observations: u64,
}

impl Arm {
    /// A `None` reading is a session the arm read and could not measure. The name should carry the arm's parameters.
    pub fn new(
        name: impl Into<String>,
        readings: impl IntoIterator<Item = (SessionDate, Option<f64>)>,
        observations: u64,
    ) -> Result<Self, StudyRefusal> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(StudyRefusal::BlankName);
        }
        let mut held = BTreeMap::new();
        for (session, reading) in readings {
            if reading.is_some_and(|value| !value.is_finite()) {
                return Err(StudyRefusal::NotFinite { arm: name, session });
            }
            if held.insert(session, reading).is_some() {
                return Err(StudyRefusal::ReadTwice { arm: name, session });
            }
        }
        let measured = held.values().flatten().count() as u64;
        match (held.is_empty(), observations < measured) {
            (true, _) => Err(StudyRefusal::NoReadings { arm: name }),
            (false, true) => Err(StudyRefusal::FewerObservationsThanReadings {
                arm: name,
                observations,
                measured,
            }),
            (false, false) => Ok(Self {
                name,
                readings: held,
                observations,
            }),
        }
    }
}

/// What an exploratory study commits to before it reads: the question, where it looks, and the effect below which
/// the direction is dropped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exploration {
    family: Family,
    universe: Universe,
    horizon: Horizon,
    question: String,
    /// In the effect's own units: net basis points for a return, the declared units otherwise.
    kill_line: f64,
}

impl Exploration {
    pub fn new(
        family: Family,
        universe: Universe,
        horizon: Horizon,
        question: impl Into<String>,
        kill_line: f64,
    ) -> Result<Self, StudyRefusal> {
        let question = question.into();
        match (question.trim().is_empty(), kill_line.is_finite()) {
            (true, _) => Err(StudyRefusal::BlankQuestion),
            (false, false) => Err(StudyRefusal::KillLineNotFinite { kill_line }),
            (false, true) => Ok(Self {
                family,
                universe,
                horizon,
                question,
                kill_line,
            }),
        }
    }

    pub fn family(&self) -> Family {
        self.family
    }

    pub fn kill_line(&self) -> f64 {
        self.kill_line
    }
}

/// Which questions a study may answer: only a registered one has a bar to clear.
#[derive(Debug, Clone, PartialEq)]
pub enum Lane {
    Registered(OpenAccession),
    Exploratory(Exploration),
}

/// Why a study could not be assembled, with the values that disagreed.
#[derive(Debug, Clone, PartialEq)]
pub enum StudyRefusal {
    BlankName,
    BlankQuestion,
    NoReadings {
        arm: String,
    },
    ReadTwice {
        arm: String,
        session: SessionDate,
    },
    NotFinite {
        arm: String,
        session: SessionDate,
    },
    /// A reading folds from rows, so more readings than rows counted something twice.
    FewerObservationsThanReadings {
        arm: String,
        observations: u64,
        measured: u64,
    },
    KillLineNotFinite {
        kill_line: f64,
    },
    /// Two arms under one name are one arm counted twice.
    ArmsNotDistinct {
        name: String,
    },
    /// Matched arms must read the same sessions; `session` is the first only one of them read.
    MatchedArmsDiverge {
        session: SessionDate,
    },
    /// Disjoint arms' errors add in quadrature only where they share no session.
    DisjointArmsOverlap {
        shared: usize,
        first: SessionDate,
    },
}

impl std::fmt::Display for StudyRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlankName => write!(formatter, "an arm has no name"),
            Self::BlankQuestion => write!(formatter, "the exploration asks no question"),
            Self::NoReadings { arm } => write!(formatter, "arm {arm} read no sessions"),
            Self::ReadTwice { arm, session } => write!(formatter, "arm {arm} read {session} twice"),
            Self::NotFinite { arm, session } => {
                write!(formatter, "arm {arm} read a non-finite value on {session}")
            }
            Self::FewerObservationsThanReadings {
                arm,
                observations,
                measured,
            } => write!(
                formatter,
                "arm {arm} claims {measured} readings from {observations} observations"
            ),
            Self::KillLineNotFinite { kill_line } => {
                write!(formatter, "a kill line of {kill_line} is not a number")
            }
            Self::ArmsNotDistinct { name } => {
                write!(
                    formatter,
                    "both arms are named {name}, so there is no control"
                )
            }
            Self::MatchedArmsDiverge { session } => {
                write!(
                    formatter,
                    "matched arms diverge: only one of them read {session}"
                )
            }
            Self::DisjointArmsOverlap { shared, first } => write!(
                formatter,
                "disjoint arms share {shared} session(s), the first {first}"
            ),
        }
    }
}

/// A declared comparison with both arms attached; it is measured only through `laboratory::run`, which journals it.
#[derive(Debug, Clone, PartialEq)]
pub struct Study {
    lane: Lane,
    quantity: Quantity,
    pairing: Pairing,
    treatment: Arm,
    control: Arm,
}

impl Study {
    /// Refuses arms whose difference would not mean what it says: one arm twice, matched arms over different
    /// sessions, or disjoint arms that share one.
    pub fn new(
        lane: Lane,
        quantity: Quantity,
        pairing: Pairing,
        treatment: Arm,
        control: Arm,
    ) -> Result<Self, StudyRefusal> {
        if treatment.name == control.name {
            return Err(StudyRefusal::ArmsNotDistinct {
                name: treatment.name,
            });
        }
        let only_one = |left: &Arm, right: &Arm| {
            left.readings
                .keys()
                .find(|session| !right.readings.contains_key(session))
                .copied()
        };
        match pairing {
            Pairing::Matched => {
                let divergence = [
                    only_one(&treatment, &control),
                    only_one(&control, &treatment),
                ];
                if let Some(session) = divergence.into_iter().flatten().min() {
                    return Err(StudyRefusal::MatchedArmsDiverge { session });
                }
            }
            Pairing::Disjoint => {
                let shared: Vec<SessionDate> = treatment
                    .readings
                    .keys()
                    .filter(|session| control.readings.contains_key(session))
                    .copied()
                    .collect();
                if let Some(first) = shared.first() {
                    return Err(StudyRefusal::DisjointArmsOverlap {
                        shared: shared.len(),
                        first: *first,
                    });
                }
            }
        }
        Ok(Self {
            lane,
            quantity,
            pairing,
            treatment,
            control,
        })
    }

    /// Scores the study, consuming it so the declaration travels with the number it produced.
    pub(crate) fn measure(self) -> StudyRan {
        let difference = match self.pairing {
            Pairing::Matched => summarize(
                self.treatment
                    .readings
                    .values()
                    .zip(self.control.readings.values())
                    .map(|(treatment, control)| Some((*treatment)? - (*control)?)),
            ),
            Pairing::Disjoint => {
                let treatment = summarize(self.treatment.readings.values().copied());
                let control = summarize(self.control.readings.values().copied());
                Summary {
                    sessions: treatment.sessions + control.sessions,
                    undefined: treatment.undefined + control.undefined,
                    distribution: treatment.distribution.zip(control.distribution).map(
                        |(treatment, control)| Distribution {
                            mean: treatment.mean - control.mean,
                            standard_error: treatment.standard_error.hypot(control.standard_error),
                        },
                    ),
                }
            }
        };
        let effect = difference
            .distribution
            .map(|distribution| Effect::of(&self.quantity, distribution.mean));
        let finding = match self.lane {
            Lane::Registered(accession) => Finding::Registered {
                accession: accession.number(),
                haircut: Haircut::new(accession.family_tests()),
                effect,
                standard_error: difference
                    .distribution
                    .map(|distribution| distribution.standard_error),
            },
            Lane::Exploratory(exploration) => Finding::Exploratory {
                exploration,
                effect,
            },
        };
        StudyRan {
            pairing: self.pairing,
            quantity: self.quantity,
            treatment: ArmRead::of(&self.treatment),
            control: ArmRead::of(&self.control),
            sessions: difference.sessions,
            undefined: difference.undefined,
            finding,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Distribution {
    mean: f64,
    standard_error: f64,
}

/// A mean over the measured sessions, with the unmeasured ones counted beside it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Summary {
    sessions: usize,
    undefined: usize,
    /// `None` below two measured sessions, where there is no spread to take an error from.
    distribution: Option<Distribution>,
}

fn summarize(readings: impl IntoIterator<Item = Option<f64>>) -> Summary {
    let (measured, undefined): (Vec<Option<f64>>, Vec<Option<f64>>) =
        readings.into_iter().partition(Option::is_some);
    let measured: Vec<f64> = measured.into_iter().flatten().collect();
    let count = measured.len() as f64;
    let distribution = (measured.len() > 1).then(|| {
        let mean = measured.iter().sum::<f64>() / count;
        let variance = measured
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (count - 1.0);
        Distribution {
            mean,
            standard_error: (variance / count).sqrt(),
        }
    });
    Summary {
        sessions: measured.len(),
        undefined: undefined.len(),
        distribution,
    }
}

/// What the difference is worth, so a gross return never appears without its charge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Effect {
    Net {
        gross: f64,
        cost: BasisPoints,
    },
    /// A return the declared fill style cannot be costed from.
    Refused {
        gross: f64,
        refusal: CostRefusal,
    },
    NotAReturn {
        value: f64,
        units: Unit,
    },
}

impl Effect {
    fn of(quantity: &Quantity, mean: f64) -> Self {
        match quantity {
            Quantity::Unpriced { units } => Self::NotAReturn {
                value: mean,
                units: units.clone(),
            },
            Quantity::ReturnPerRoundTrip {
                cost_model,
                quoted_spread,
            } => match cost_model.cost(*quoted_spread) {
                Ok(cost) => Self::Net { gross: mean, cost },
                Err(refusal) => Self::Refused {
                    gross: mean,
                    refusal,
                },
            },
        }
    }

    /// Treatment less control before any charge.
    pub fn difference(&self) -> f64 {
        match self {
            Self::Net { gross, .. } | Self::Refused { gross, .. } => *gross,
            Self::NotAReturn { value, .. } => *value,
        }
    }

    /// The number a decision rests on: net of cost for a return, the value otherwise, and nothing for a return that
    /// could not be costed.
    pub fn headline(&self) -> Option<f64> {
        match self {
            // Subtracted, not signed toward zero: a negative gross is not paid the spread for being wrong.
            Self::Net { gross, cost } => Some(gross - cost.value()),
            Self::Refused { .. } => None,
            Self::NotAReturn { value, .. } => Some(*value),
        }
    }
}

/// What one arm read, without its error: the population and the boundary sessions beside the mean.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArmRead {
    name: String,
    first: SessionDate,
    last: SessionDate,
    sessions: usize,
    undefined: usize,
    observations: u64,
    mean: Option<f64>,
}

impl ArmRead {
    fn of(arm: &Arm) -> Self {
        let summary = summarize(arm.readings.values().copied());
        let sessions = arm.readings.keys();
        Self {
            name: arm.name.clone(),
            first: *sessions
                .clone()
                .next()
                .expect("an arm reads at least one session"),
            last: *sessions.last().expect("an arm reads at least one session"),
            sessions: summary.sessions,
            undefined: summary.undefined,
            observations: arm.observations,
            mean: summary.distribution.map(|distribution| distribution.mean),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sessions(&self) -> usize {
        self.sessions
    }

    pub fn undefined(&self) -> usize {
        self.undefined
    }
}

/// What a study found, by lane; only the registered variant holds a standard error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "lane", rename_all = "snake_case")]
pub enum Finding {
    Registered {
        accession: AccessionNumber,
        haircut: Haircut,
        effect: Option<Effect>,
        standard_error: Option<f64>,
    },
    Exploratory {
        exploration: Exploration,
        effect: Option<Effect>,
    },
}

/// One study's run, journaled whole: the declaration, both arms' populations, and the finding in its lane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StudyRan {
    pairing: Pairing,
    quantity: Quantity,
    treatment: ArmRead,
    control: ArmRead,
    /// Sessions the difference was measured on, and those where either arm could not be.
    sessions: usize,
    undefined: usize,
    finding: Finding,
}

impl StudyRan {
    pub fn treatment(&self) -> &ArmRead {
        &self.treatment
    }

    pub fn control(&self) -> &ArmRead {
        &self.control
    }

    pub fn sessions(&self) -> usize {
        self.sessions
    }

    pub fn undefined(&self) -> usize {
        self.undefined
    }

    pub fn finding(&self) -> &Finding {
        &self.finding
    }

    pub fn effect(&self) -> Option<&Effect> {
        match &self.finding {
            Finding::Registered { effect, .. } | Finding::Exploratory { effect, .. } => {
                effect.as_ref()
            }
        }
    }

    /// The difference over its error, quotable only for a registered study. `None` also where the error is zero: a
    /// reading that never varied is pinned by the data's shape, not by an effect.
    pub fn standard_errors(&self) -> Option<f64> {
        match &self.finding {
            Finding::Registered {
                effect,
                standard_error,
                ..
            } => {
                let difference = effect.as_ref()?.difference();
                standard_error
                    .filter(|error| *error > 0.0)
                    .map(|error| difference / error)
            }
            Finding::Exploratory { .. } => None,
        }
    }

    /// Whether a registered reading clears its family's haircut; `None` where nothing could be measured, which has
    /// not failed the bar but failed to reach it.
    pub fn clears_haircut(&self) -> Option<bool> {
        match &self.finding {
            Finding::Registered { haircut, .. } => Some(haircut.clears(self.standard_errors()?)),
            Finding::Exploratory { .. } => None,
        }
    }

    /// Whether an exploratory effect reaches its kill line in the declared direction.
    pub fn survives_kill_line(&self) -> Option<bool> {
        match &self.finding {
            Finding::Exploratory {
                exploration,
                effect,
            } => Some(effect.as_ref()?.headline()? >= exploration.kill_line),
            Finding::Registered { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::NonZeroU32;

    use chrono::NaiveDate;
    use proptest::prelude::*;

    use crate::common::laboratory::cost::FillStyle;
    use crate::common::register::{Accession, Bid, Opening};

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    fn arm(name: &str, readings: &[Option<f64>]) -> Arm {
        Arm::new(
            name,
            readings
                .iter()
                .enumerate()
                .map(|(day, reading)| (session(day as i64), *reading)),
            readings.len() as u64,
        )
        .unwrap()
    }

    fn opening() -> Opening {
        Opening::new(
            Family::Overnight,
            "liquid-common@1".parse().unwrap(),
            "1 sessions".parse().unwrap(),
            "close-to-open returns persist".to_string(),
            Bid::Unrecorded,
            session(0),
            None,
            None,
        )
        .unwrap()
    }

    /// An open accession in a family that has opened `tests` accessions.
    fn registered(tests: u32) -> Lane {
        let register: Vec<Accession> = (1..=tests)
            .map(|number| Accession::open(AccessionNumber::new(number).unwrap(), opening()))
            .collect();
        Lane::Registered(register[0].study(&register).unwrap())
    }

    fn exploratory(kill_line: f64) -> Lane {
        Lane::Exploratory(
            Exploration::new(
                Family::Overnight,
                "liquid-common@1".parse().unwrap(),
                "1 sessions".parse().unwrap(),
                "does the gap persist",
                kill_line,
            )
            .unwrap(),
        )
    }

    fn unpriced() -> Quantity {
        Quantity::Unpriced {
            units: "correlation".parse().unwrap(),
        }
    }

    fn priced(style: FillStyle, spread: f64) -> Quantity {
        Quantity::ReturnPerRoundTrip {
            cost_model: CostModel::new(style, NonZeroU32::MIN),
            quoted_spread: BasisPoints::new(spread).unwrap(),
        }
    }

    fn measure(
        lane: Lane,
        quantity: Quantity,
        pairing: Pairing,
        treatment: Arm,
        control: Arm,
    ) -> StudyRan {
        Study::new(lane, quantity, pairing, treatment, control)
            .unwrap()
            .measure()
    }

    #[test]
    fn test_an_arm_refuses_what_it_could_not_have_read() {
        let twice = [(session(0), Some(1.0)), (session(0), Some(2.0))];
        assert_eq!(
            Arm::new("a", twice, 2),
            Err(StudyRefusal::ReadTwice {
                arm: "a".to_string(),
                session: session(0)
            })
        );
        assert_eq!(
            Arm::new("a", [(session(1), Some(f64::NAN))], 1),
            Err(StudyRefusal::NotFinite {
                arm: "a".to_string(),
                session: session(1)
            })
        );
        assert_eq!(
            Arm::new(
                "a",
                [
                    (session(0), Some(1.0)),
                    (session(1), Some(1.0)),
                    (session(2), None)
                ],
                1
            ),
            Err(StudyRefusal::FewerObservationsThanReadings {
                arm: "a".to_string(),
                observations: 1,
                measured: 2
            })
        );
        assert_eq!(
            Arm::new(" ", [(session(0), Some(1.0))], 1),
            Err(StudyRefusal::BlankName)
        );
        assert_eq!(
            Arm::new("a", [], 0),
            Err(StudyRefusal::NoReadings {
                arm: "a".to_string()
            })
        );
        assert!(Arm::new("a", [(session(0), Some(1.0)), (session(1), None)], 1).is_ok());
    }

    #[test]
    fn test_a_study_refuses_arms_whose_difference_would_mean_nothing() {
        let study = |pairing, treatment, control| {
            Study::new(exploratory(0.0), unpriced(), pairing, treatment, control)
        };
        assert_eq!(
            study(
                Pairing::Matched,
                arm("a", &[Some(1.0)]),
                arm("a", &[Some(1.0)])
            ),
            Err(StudyRefusal::ArmsNotDistinct {
                name: "a".to_string()
            })
        );
        let offset = Arm::new(
            "control",
            [(session(1), Some(1.0)), (session(2), Some(1.0))],
            2,
        )
        .unwrap();
        for (treatment, control) in [
            (arm("treatment", &[Some(1.0), Some(1.0)]), offset.clone()),
            (
                offset.clone(),
                Arm::new(
                    "treatment",
                    [(session(0), Some(1.0)), (session(1), Some(1.0))],
                    2,
                )
                .unwrap(),
            ),
        ] {
            assert_eq!(
                study(Pairing::Matched, treatment, control),
                Err(StudyRefusal::MatchedArmsDiverge {
                    session: session(0)
                })
            );
        }
        assert_eq!(
            study(
                Pairing::Disjoint,
                arm("treatment", &[Some(1.0), Some(1.0), Some(1.0)]),
                offset.clone()
            ),
            Err(StudyRefusal::DisjointArmsOverlap {
                shared: 2,
                first: session(1)
            })
        );
        assert!(study(Pairing::Disjoint, arm("treatment", &[Some(1.0)]), offset).is_ok());
    }

    #[test]
    fn test_a_matched_difference_removes_the_variation_both_arms_share() {
        let ran = measure(
            registered(1),
            unpriced(),
            Pairing::Matched,
            arm(
                "treatment",
                &[Some(11.0), Some(-19.0), Some(32.0), Some(-8.0)],
            ),
            arm(
                "control",
                &[Some(10.0), Some(-20.0), Some(30.0), Some(-10.0)],
            ),
        );
        assert_eq!(ran.effect().map(Effect::difference), Some(1.5));
        // Differences 1, 1, 2, 2: standard deviation 1/√3, over √4.
        let standard_errors = ran.standard_errors().unwrap();
        assert!(
            (standard_errors - 1.5 / (1.0 / 3f64.sqrt() / 2.0)).abs() < 1e-12,
            "{standard_errors}"
        );
        assert_eq!((ran.sessions(), ran.undefined()), (4, 0));
    }

    #[test]
    fn test_disjoint_arms_add_their_errors_in_quadrature() {
        let treatment = arm("treatment", &[Some(1.0), Some(3.0)]);
        let control = Arm::new(
            "control",
            [
                (session(5), Some(0.0)),
                (session(6), Some(0.0)),
                (session(7), Some(3.0)),
            ],
            3,
        )
        .unwrap();
        let ran = measure(
            registered(1),
            unpriced(),
            Pairing::Disjoint,
            treatment,
            control,
        );
        assert_eq!(ran.effect().map(Effect::difference), Some(1.0));
        // Treatment error 1, control error 1: √2 together.
        assert!((ran.standard_errors().unwrap() - 1.0 / 2f64.sqrt()).abs() < 1e-12);
        assert_eq!((ran.sessions(), ran.undefined()), (5, 0));
        assert_eq!(
            (ran.control().sessions(), ran.control().name()),
            (3, "control")
        );
    }

    #[test]
    fn test_an_unmeasured_session_is_counted_rather_than_zeroed() {
        let ran = measure(
            registered(1),
            unpriced(),
            Pairing::Matched,
            arm("treatment", &[Some(2.0), None, Some(4.0), Some(6.0)]),
            arm("control", &[Some(1.0), Some(1.0), None, Some(1.0)]),
        );
        assert_eq!((ran.sessions(), ran.undefined()), (2, 2));
        assert_eq!(ran.effect().map(Effect::difference), Some(3.0));
        assert_eq!(
            (ran.treatment().sessions(), ran.treatment().undefined()),
            (3, 1)
        );
    }

    #[test]
    fn test_a_difference_that_never_varied_has_no_standard_errors() {
        let ran = measure(
            registered(1),
            unpriced(),
            Pairing::Matched,
            arm("treatment", &[Some(2.0), Some(3.0)]),
            arm("control", &[Some(1.0), Some(2.0)]),
        );
        assert_eq!(ran.effect().map(Effect::difference), Some(1.0));
        assert_eq!((ran.standard_errors(), ran.clears_haircut()), (None, None));
        let single = measure(
            registered(1),
            unpriced(),
            Pairing::Matched,
            arm("t", &[Some(2.0)]),
            arm("c", &[Some(1.0)]),
        );
        assert_eq!((single.effect(), single.clears_haircut()), (None, None));
    }

    /// 2.5 standard errors clears a family of one and fails a family of forty.
    #[test]
    fn test_a_reading_that_clears_alone_fails_once_its_family_is_counted() {
        let reading = |tests| {
            measure(
                registered(tests),
                unpriced(),
                Pairing::Disjoint,
                arm("treatment", &[Some(2.5 - 1.0), Some(2.5 + 1.0)]),
                Arm::new(
                    "control",
                    [(session(9), Some(0.0)), (session(10), Some(0.0))],
                    2,
                )
                .unwrap(),
            )
        };
        assert!((reading(1).standard_errors().unwrap() - 2.5).abs() < 1e-12);
        assert_eq!(reading(1).clears_haircut(), Some(true));
        assert_eq!(reading(40).clears_haircut(), Some(false));
        match reading(40).finding() {
            Finding::Registered {
                accession, haircut, ..
            } => {
                assert_eq!(
                    (accession.to_string().as_str(), haircut.tests().get()),
                    ("000001", 40)
                );
            }
            Finding::Exploratory { .. } => panic!("registered"),
        }
    }

    #[test]
    fn test_an_exploratory_study_has_no_quotable_statistic_only_a_kill_line() {
        let ran = |kill_line, quantity| {
            measure(
                exploratory(kill_line),
                quantity,
                Pairing::Matched,
                arm("treatment", &[Some(10.0), Some(14.0)]),
                arm("control", &[Some(0.0), Some(0.0)]),
            )
        };
        let net = ran(5.0, priced(FillStyle::Aggressive, 6.0));
        assert_eq!((net.standard_errors(), net.clears_haircut()), (None, None));
        assert_eq!(net.effect().and_then(Effect::headline), Some(6.0));
        assert_eq!(net.survives_kill_line(), Some(true));
        assert_eq!(
            ran(6.5, priced(FillStyle::Aggressive, 6.0)).survives_kill_line(),
            Some(false)
        );
        assert_eq!(
            ran(6.0, priced(FillStyle::Aggressive, 6.0)).survives_kill_line(),
            Some(true)
        );
        assert_eq!(ran(11.0, unpriced()).survives_kill_line(), Some(true));
        let refused = ran(-100.0, priced(FillStyle::Passive, 6.0));
        assert_eq!(refused.effect().map(Effect::difference), Some(12.0));
        assert_eq!(refused.survives_kill_line(), None);
        match serde_json::to_value(net.finding()).unwrap() {
            serde_json::Value::Object(fields) => {
                assert_eq!(
                    fields.keys().collect::<Vec<_>>(),
                    ["effect", "exploration", "lane"]
                );
            }
            other => panic!("{other}"),
        }
    }

    #[test]
    fn test_an_exploration_commits_to_a_question_and_a_finite_kill_line() {
        let exploration = |question, kill_line| {
            Exploration::new(
                Family::Overnight,
                "liquid-common@1".parse().unwrap(),
                "1 sessions".parse().unwrap(),
                question,
                kill_line,
            )
        };
        assert_eq!(exploration(" ", 1.0), Err(StudyRefusal::BlankQuestion));
        assert_eq!(
            exploration("q", f64::INFINITY),
            Err(StudyRefusal::KillLineNotFinite {
                kill_line: f64::INFINITY
            })
        );
        assert_eq!(exploration("q", -2.0).unwrap().kill_line(), -2.0);
    }

    #[test]
    fn test_a_negative_gross_pays_the_spread_rather_than_being_paid_it() {
        let effect = Effect::of(&priced(FillStyle::Aggressive, 3.0), -2.0);
        assert_eq!(effect.headline(), Some(-5.0));
        assert_eq!(effect.difference(), -2.0);
    }

    fn readings() -> impl Strategy<Value = Vec<(f64, f64, f64)>> {
        prop::collection::vec((-100.0..100.0, -100.0..100.0, -1000.0..1000.0), 2..40)
    }

    proptest! {
        /// Adding the same per-session shock to both matched arms moves neither the difference nor its error.
        #[test]
        fn test_a_shared_shock_cancels_in_a_matched_difference(rows in readings()) {
            let read = |shocked: bool| {
                let treatment: Vec<Option<f64>> = rows.iter().map(|(t, _, s)| Some(t + if shocked { *s } else { 0.0 })).collect();
                let control: Vec<Option<f64>> = rows.iter().map(|(_, c, s)| Some(c + if shocked { *s } else { 0.0 })).collect();
                let ran = measure(registered(1), unpriced(), Pairing::Matched, arm("t", &treatment), arm("c", &control));
                (ran.effect().map(Effect::difference).unwrap(), ran.standard_errors())
            };
            let (plain, shocked) = (read(false), read(true));
            prop_assert!((plain.0 - shocked.0).abs() < 1e-6);
            match (plain.1, shocked.1) {
                (Some(left), Some(right)) => prop_assert!((left - right).abs() < 1e-4 * left.abs().max(1.0)),
                (left, right) => prop_assert_eq!(left.is_some(), right.is_some()),
            }
        }

        /// Swapping disjoint arms negates the difference and keeps its error.
        #[test]
        fn test_swapping_disjoint_arms_negates_the_difference(rows in readings()) {
            let treatment = Arm::new("t", rows.iter().enumerate().map(|(day, (t, _, _))| (session(day as i64), Some(*t))), rows.len() as u64).unwrap();
            let control = Arm::new("c", rows.iter().enumerate().map(|(day, (_, c, _))| (session(100 + day as i64), Some(*c))), rows.len() as u64).unwrap();
            let forward = measure(registered(1), unpriced(), Pairing::Disjoint, treatment.clone(), control.clone());
            let backward = measure(registered(1), unpriced(), Pairing::Disjoint, control, treatment);
            prop_assert_eq!(
                forward.effect().map(Effect::difference).map(|value| -value),
                backward.effect().map(Effect::difference)
            );
            prop_assert_eq!(forward.standard_errors().map(|value| -value), backward.standard_errors());
        }

        /// A study's record reads back as the record written, which is what lets the journal stand in for the run.
        #[test]
        fn test_a_study_ran_record_round_trips(rows in readings(), spread in 0.0..50.0f64, explore in any::<bool>()) {
            let treatment: Vec<Option<f64>> = rows.iter().map(|(t, _, _)| Some(*t)).collect();
            let control: Vec<Option<f64>> = rows.iter().map(|(_, c, _)| Some(*c)).collect();
            let lane = if explore { exploratory(spread) } else { registered(3) };
            let ran = measure(lane, priced(FillStyle::Aggressive, spread), Pairing::Matched, arm("t", &treatment), arm("c", &control));
            let encoded = serde_json::to_string(&ran).unwrap();
            prop_assert_eq!(serde_json::from_str::<StudyRan>(&encoded).unwrap(), ran);
        }
    }
}
