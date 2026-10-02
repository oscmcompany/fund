//! A study compares two arms read per session and is scored in the lane it was declared in: a registered study
//! quotes standard errors against its family's haircut, and an exploratory one reports only an effect against a
//! kill line, so a number that was never registered cannot be quoted as if it had been.

pub mod cost;
pub mod haircut;

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::common::laboratory::cost::{BasisPoints, CostModel, CostRefusal};
use crate::common::laboratory::haircut::{DegreesOfFreedom, Haircut};
use crate::common::register::{
    Accession, AccessionNumber, Family, Horizon, OpenAccession, Unit, Universe,
};
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

/// The largest reading magnitude an arm admits; no measured quantity comes near it, and below it every sum, square
/// and Welch term stays finite.
pub const READING_BOUND: f64 = 1e50;

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
            if let Some(value) =
                reading.filter(|value| value.is_nan() || value.abs() > READING_BOUND)
            {
                return Err(StudyRefusal::OutOfRange {
                    arm: name,
                    session,
                    value,
                });
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

/// Which side of a kill line an effect must land on to survive.
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
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Direction {
    Higher,
    Lower,
}

/// The effect beyond which an exploratory direction is worth pursuing, in the effect's own units: net basis points
/// for a return, the declared units otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "KillLineFields")]
pub struct KillLine {
    threshold: f64,
    direction: Direction,
}

#[derive(Deserialize)]
struct KillLineFields {
    threshold: f64,
    direction: Direction,
}

impl TryFrom<KillLineFields> for KillLine {
    type Error = StudyRefusal;

    fn try_from(fields: KillLineFields) -> Result<Self, Self::Error> {
        Self::new(fields.threshold, fields.direction)
    }
}

impl KillLine {
    pub fn new(threshold: f64, direction: Direction) -> Result<Self, StudyRefusal> {
        match threshold.is_finite() {
            true => Ok(Self {
                threshold,
                direction,
            }),
            false => Err(StudyRefusal::KillLineNotFinite { threshold }),
        }
    }

    /// Whether `effect` reaches the line, inclusively, on the declared side.
    pub fn admits(self, effect: f64) -> bool {
        match self.direction {
            Direction::Higher => effect >= self.threshold,
            Direction::Lower => effect <= self.threshold,
        }
    }
}

/// What an exploratory study commits to before it reads: the question, where it looks, and its kill line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ExplorationFields")]
pub struct Exploration {
    family: Family,
    universe: Universe,
    horizon: Horizon,
    question: String,
    kill_line: KillLine,
}

#[derive(Deserialize)]
struct ExplorationFields {
    family: Family,
    universe: Universe,
    horizon: Horizon,
    question: String,
    kill_line: KillLine,
}

impl TryFrom<ExplorationFields> for Exploration {
    type Error = StudyRefusal;

    fn try_from(fields: ExplorationFields) -> Result<Self, Self::Error> {
        Self::new(
            fields.family,
            fields.universe,
            fields.horizon,
            fields.question,
            fields.kill_line,
        )
    }
}

impl Exploration {
    pub fn new(
        family: Family,
        universe: Universe,
        horizon: Horizon,
        question: impl Into<String>,
        kill_line: KillLine,
    ) -> Result<Self, StudyRefusal> {
        let question = question.into();
        match question.trim().is_empty() {
            true => Err(StudyRefusal::BlankQuestion),
            false => Ok(Self {
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

    pub fn kill_line(&self) -> KillLine {
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
    /// Beyond `READING_BOUND`, or not a number.
    OutOfRange {
        arm: String,
        session: SessionDate,
        value: f64,
    },
    /// A reading folds from rows, so more readings than rows counted something twice.
    FewerObservationsThanReadings {
        arm: String,
        observations: u64,
        measured: u64,
    },
    KillLineNotFinite {
        threshold: f64,
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
            Self::OutOfRange {
                arm,
                session,
                value,
            } => write!(
                formatter,
                "arm {arm} read {value} on {session}, beyond {READING_BOUND:e}"
            ),
            Self::FewerObservationsThanReadings {
                arm,
                observations,
                measured,
            } => write!(
                formatter,
                "arm {arm} claims {measured} readings from {observations} observations"
            ),
            Self::KillLineNotFinite { threshold } => {
                write!(formatter, "a kill line of {threshold} is not a number")
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
                    distribution: treatment
                        .distribution
                        .zip(control.distribution)
                        .map(|(treatment, control)| welch(treatment, control)),
                }
            }
        };
        let effect = difference
            .distribution
            .map(|distribution| Effect::of(&self.quantity, distribution.mean));
        let finding = match self.lane {
            Lane::Registered(accession) => Finding::Registered {
                accession: accession.number(),
                family: accession.family(),
                effect,
                standard_error: difference
                    .distribution
                    .map(|distribution| distribution.standard_error),
                degrees_of_freedom: difference
                    .distribution
                    .and_then(|distribution| distribution.degrees_of_freedom),
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
    /// `None` where no spread was seen to estimate the error from.
    degrees_of_freedom: Option<DegreesOfFreedom>,
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
            degrees_of_freedom: DegreesOfFreedom::new(count - 1.0),
        }
    });
    Summary {
        sessions: measured.len(),
        undefined: undefined.len(),
        distribution,
    }
}

/// Treatment less control over disjoint sessions: errors in quadrature, with Welch–Satterthwaite degrees of freedom
/// because the arms' variances need not agree.
fn welch(treatment: Distribution, control: Distribution) -> Distribution {
    let freedom =
        |distribution: Distribution| distribution.degrees_of_freedom.map(DegreesOfFreedom::value);
    let (treatment_variance, control_variance) = (
        treatment.standard_error.powi(2),
        control.standard_error.powi(2),
    );
    let degrees_of_freedom = freedom(treatment).zip(freedom(control)).and_then(
        |(treatment_freedom, control_freedom)| {
            DegreesOfFreedom::new(
                (treatment_variance + control_variance).powi(2)
                    / (treatment_variance.powi(2) / treatment_freedom
                        + control_variance.powi(2) / control_freedom),
            )
        },
    );
    Distribution {
        mean: treatment.mean - control.mean,
        standard_error: (treatment_variance + control_variance).sqrt(),
        degrees_of_freedom,
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
        family: Family,
        effect: Option<Effect>,
        standard_error: Option<f64>,
        degrees_of_freedom: Option<DegreesOfFreedom>,
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

    /// Whether a registered reading clears its family's haircut as `register` counts it, so every result is judged
    /// at the family's current size; `None` where nothing could be measured, which has not failed the bar but failed
    /// to reach it.
    pub fn clears_haircut(&self, register: &[Accession]) -> Option<bool> {
        match &self.finding {
            Finding::Registered {
                accession,
                family,
                degrees_of_freedom,
                ..
            } => {
                let others = register
                    .iter()
                    .filter(|other| {
                        other.number() != *accession && other.opening().family() == *family
                    })
                    .count();
                let tests = u32::try_from(others)
                    .ok()
                    .and_then(|others| NonZeroU32::MIN.checked_add(others))
                    .expect("a register numbers fewer than u32::MAX accessions");
                Some(Haircut::new(tests).clears(self.standard_errors()?, (*degrees_of_freedom)?))
            }
            Finding::Exploratory { .. } => None,
        }
    }

    /// Whether an exploratory effect reaches its kill line in the declared direction.
    pub fn survives_kill_line(&self) -> Option<bool> {
        match &self.finding {
            Finding::Exploratory {
                exploration,
                effect,
            } => Some(exploration.kill_line.admits(effect.as_ref()?.headline()?)),
            Finding::Registered { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::NaiveDate;
    use proptest::prelude::*;

    use crate::common::laboratory::cost::FillStyle;
    use crate::common::register::{Bid, Closing, Measured, Opening, Sample, StudyCost, Verdict};

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    fn arm(name: &str, readings: &[Option<f64>]) -> Arm {
        arm_from(name, 0, readings)
    }

    fn arm_from(name: &str, first_day: i64, readings: &[Option<f64>]) -> Arm {
        Arm::new(
            name,
            readings
                .iter()
                .enumerate()
                .map(|(day, reading)| (session(first_day + day as i64), *reading)),
            readings.len() as u64,
        )
        .unwrap()
    }

    fn opening(family: Family) -> Opening {
        Opening::new(
            family,
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

    fn accession(number: u32, family: Family) -> Accession {
        Accession::open(AccessionNumber::new(number).unwrap(), opening(family))
    }

    /// Accession 1, open in the overnight family.
    fn registered() -> Lane {
        Lane::Registered(accession(1, Family::Overnight).study().unwrap())
    }

    /// A register whose overnight family has opened `tests` accessions, accession 1 among them.
    fn family(tests: u32) -> Vec<Accession> {
        (1..=tests)
            .map(|number| accession(number, Family::Overnight))
            .collect()
    }

    fn exploratory(threshold: f64, direction: Direction) -> Lane {
        Lane::Exploratory(
            Exploration::new(
                Family::Overnight,
                "liquid-common@1".parse().unwrap(),
                "1 sessions".parse().unwrap(),
                "does the gap persist",
                KillLine::new(threshold, direction).unwrap(),
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

    fn degrees_of_freedom(ran: &StudyRan) -> Option<f64> {
        match ran.finding() {
            Finding::Registered {
                degrees_of_freedom, ..
            } => degrees_of_freedom.map(DegreesOfFreedom::value),
            Finding::Exploratory { .. } => panic!("registered"),
        }
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
        for value in [1e308, -1e51, f64::INFINITY] {
            assert_eq!(
                Arm::new("a", [(session(1), Some(value))], 1),
                Err(StudyRefusal::OutOfRange {
                    arm: "a".to_string(),
                    session: session(1),
                    value
                })
            );
        }
        assert!(matches!(
            Arm::new("a", [(session(1), Some(f64::NAN))], 1),
            Err(StudyRefusal::OutOfRange { .. })
        ));
        assert!(Arm::new("a", [(session(1), Some(-1e50))], 1).is_ok());
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
            Study::new(
                exploratory(0.0, Direction::Higher),
                unpriced(),
                pairing,
                treatment,
                control,
            )
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
        let offset = arm_from("control", 1, &[Some(1.0), Some(1.0)]);
        for (treatment, control) in [
            (arm("treatment", &[Some(1.0), Some(1.0)]), offset.clone()),
            (offset.clone(), arm("treatment", &[Some(1.0), Some(1.0)])),
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
            registered(),
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
        assert_eq!(degrees_of_freedom(&ran), Some(3.0));
    }

    /// Treatment 1, 3 (squared error 1, one degree of freedom) against control 0, 0, 3 (squared error 1, two).
    #[test]
    fn test_disjoint_arms_add_their_errors_in_quadrature_with_welch_freedom() {
        let ran = measure(
            registered(),
            unpriced(),
            Pairing::Disjoint,
            arm("treatment", &[Some(1.0), Some(3.0)]),
            arm_from("control", 5, &[Some(0.0), Some(0.0), Some(3.0)]),
        );
        assert_eq!(ran.effect().map(Effect::difference), Some(1.0));
        assert!((ran.standard_errors().unwrap() - 1.0 / 2f64.sqrt()).abs() < 1e-12);
        let welch = degrees_of_freedom(&ran).unwrap();
        assert!((welch - 4.0 / 1.5).abs() < 1e-12, "{welch}");
        assert_eq!((ran.sessions(), ran.undefined()), (5, 0));
        assert_eq!(
            (ran.control().sessions(), ran.control().name()),
            (3, "control")
        );
    }

    #[test]
    fn test_an_unmeasured_session_is_counted_rather_than_zeroed() {
        let ran = measure(
            registered(),
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
            registered(),
            unpriced(),
            Pairing::Matched,
            arm("treatment", &[Some(2.0), Some(3.0)]),
            arm("control", &[Some(1.0), Some(2.0)]),
        );
        assert_eq!(ran.effect().map(Effect::difference), Some(1.0));
        assert_eq!(
            (ran.standard_errors(), ran.clears_haircut(&family(1))),
            (None, None)
        );
        let single = measure(
            registered(),
            unpriced(),
            Pairing::Matched,
            arm("treatment", &[Some(2.0)]),
            arm("control", &[Some(1.0)]),
        );
        assert_eq!(
            (single.effect(), single.clears_haircut(&family(1))),
            (None, None)
        );
    }

    /// Thirty-two matched differences at `standard_errors`, so thirty-one degrees of freedom.
    fn thirty_two_sessions_at(standard_errors: f64) -> StudyRan {
        let mean = standard_errors / 31f64.sqrt();
        let differences: Vec<Option<f64>> = (0..32)
            .map(|day| Some(mean + if day % 2 == 0 { 1.0 } else { -1.0 }))
            .collect();
        measure(
            registered(),
            unpriced(),
            Pairing::Matched,
            arm("treatment", &differences),
            arm("control", &[Some(0.0); 32]),
        )
    }

    /// At 31 degrees of freedom one test needs 2.04 and two need 2.36, so 2.2 tells a family of one from two.
    #[test]
    fn test_a_reading_is_judged_against_its_family_as_the_register_counts_it() {
        let borderline = thirty_two_sessions_at(2.2);
        assert_eq!(borderline.clears_haircut(&family(1)), Some(true));
        assert_eq!(borderline.clears_haircut(&family(2)), Some(false));
        let ran = thirty_two_sessions_at(2.5);
        assert!((ran.standard_errors().unwrap() - 2.5).abs() < 1e-9);
        assert_eq!(degrees_of_freedom(&ran), Some(31.0));
        assert_eq!(ran.clears_haircut(&family(1)), Some(true));
        assert_eq!(ran.clears_haircut(&family(40)), Some(false));
        // Other families, and the accession itself, do not add tests; a closed one in the family does.
        let mut register = vec![
            accession(2, Family::Execution),
            accession(3, Family::Execution),
        ];
        assert_eq!(ran.clears_haircut(&register), Some(true));
        register.extend((4..=42).map(|number| {
            accession(number, Family::Overnight)
                .close(
                    Closing::new(
                        Verdict::Refute,
                        "no effect".to_string(),
                        Measured::NotMeasured,
                        Sample::Unrecorded,
                        Vec::new(),
                        session(1),
                        None,
                        StudyCost::default(),
                    )
                    .unwrap(),
                )
                .unwrap()
        }));
        assert_eq!(ran.clears_haircut(&register), Some(false));
    }

    /// 2.5 standard errors clears the normal's 1.96 but not Student's 12.7 at one degree of freedom.
    #[test]
    fn test_a_small_sample_is_judged_at_its_own_degrees_of_freedom() {
        let ran = measure(
            registered(),
            unpriced(),
            Pairing::Disjoint,
            arm("treatment", &[Some(2.5 - 1.0), Some(2.5 + 1.0)]),
            arm_from("control", 9, &[Some(0.0), Some(0.0)]),
        );
        assert!((ran.standard_errors().unwrap() - 2.5).abs() < 1e-12);
        assert_eq!(degrees_of_freedom(&ran), Some(1.0));
        assert_eq!(ran.clears_haircut(&family(1)), Some(false));
    }

    #[test]
    fn test_an_exploratory_study_has_no_quotable_statistic_only_a_kill_line() {
        let ran = |threshold, direction, quantity| {
            measure(
                exploratory(threshold, direction),
                quantity,
                Pairing::Matched,
                arm("treatment", &[Some(10.0), Some(14.0)]),
                arm("control", &[Some(0.0), Some(0.0)]),
            )
        };
        let net = ran(5.0, Direction::Higher, priced(FillStyle::Aggressive, 6.0));
        assert_eq!(
            (net.standard_errors(), net.clears_haircut(&family(1))),
            (None, None)
        );
        assert_eq!(net.effect().and_then(Effect::headline), Some(6.0));
        assert_eq!(net.survives_kill_line(), Some(true));
        for (threshold, direction, survives) in [
            (6.5, Direction::Higher, false),
            (6.0, Direction::Higher, true),
            (6.0, Direction::Lower, true),
            (5.0, Direction::Lower, false),
            (7.0, Direction::Lower, true),
        ] {
            assert_eq!(
                ran(threshold, direction, priced(FillStyle::Aggressive, 6.0)).survives_kill_line(),
                Some(survives),
                "{threshold} {direction}"
            );
        }
        let refused = ran(-100.0, Direction::Higher, priced(FillStyle::Passive, 6.0));
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

    /// A reduction study survives by landing below its line: −8 against −5 survives, −3 does not.
    #[test]
    fn test_a_lower_kill_line_admits_the_larger_reduction() {
        let line = KillLine::new(-5.0, Direction::Lower).unwrap();
        assert!(line.admits(-8.0));
        assert!(line.admits(-5.0));
        assert!(!line.admits(-3.0));
        for refused in [f64::NAN, f64::INFINITY] {
            assert!(matches!(
                KillLine::new(refused, Direction::Lower),
                Err(StudyRefusal::KillLineNotFinite { .. })
            ));
        }
    }

    #[test]
    fn test_an_exploration_commits_to_a_question_even_when_stored() {
        let line = KillLine::new(1.0, Direction::Higher).unwrap();
        let exploration = |question| {
            Exploration::new(
                Family::Overnight,
                "liquid-common@1".parse().unwrap(),
                "1 sessions".parse().unwrap(),
                question,
                line,
            )
        };
        assert_eq!(exploration(" "), Err(StudyRefusal::BlankQuestion));
        let stored = serde_json::to_value(exploration("q").unwrap()).unwrap();
        assert_eq!(
            serde_json::from_value::<Exploration>(stored.clone()).unwrap(),
            exploration("q").unwrap()
        );
        let mut blank = stored.clone();
        blank["question"] = serde_json::json!("  ");
        assert!(serde_json::from_value::<Exploration>(blank).is_err());
        let mut sideways = stored;
        sideways["kill_line"]["direction"] = serde_json::json!("sideways");
        assert!(serde_json::from_value::<Exploration>(sideways).is_err());
    }

    #[test]
    fn test_direction_names_agree_between_strum_and_serde() {
        use strum::IntoEnumIterator;
        for direction in Direction::iter() {
            assert_eq!(
                serde_json::to_string(&direction).unwrap(),
                format!("\"{direction}\"")
            );
            assert_eq!(direction.to_string().parse::<Direction>(), Ok(direction));
        }
        assert_eq!(Direction::Lower.to_string(), "lower");
    }

    #[test]
    fn test_a_negative_gross_pays_the_spread_rather_than_being_paid_it() {
        let effect = Effect::of(&priced(FillStyle::Aggressive, 3.0), -2.0);
        assert_eq!(effect.headline(), Some(-5.0));
        assert_eq!(effect.difference(), -2.0);
    }

    /// Readings at the bound measure finite and read back from the journal as written.
    #[test]
    fn test_readings_at_the_bound_measure_finite() {
        for pairing in [Pairing::Matched, Pairing::Disjoint] {
            let control_first_day = match pairing {
                Pairing::Matched => 0,
                Pairing::Disjoint => 10,
            };
            let ran = measure(
                registered(),
                unpriced(),
                pairing,
                arm("treatment", &[Some(1e50), Some(-1e50), Some(1e50)]),
                arm_from(
                    "control",
                    control_first_day,
                    &[Some(-1e50), Some(1e50), Some(-1e50)],
                ),
            );
            let standard_errors = ran.standard_errors().unwrap();
            assert!(standard_errors.is_finite(), "{pairing}: {standard_errors}");
            assert!(degrees_of_freedom(&ran).is_some(), "{pairing}");
            let encoded = serde_json::to_string(&ran).unwrap();
            assert_eq!(serde_json::from_str::<StudyRan>(&encoded).unwrap(), ran);
        }
    }

    fn readings() -> impl Strategy<Value = Vec<(f64, f64, f64)>> {
        prop::collection::vec((-100.0..100.0, -100.0..100.0, -1000.0..1000.0), 2..40)
    }

    proptest! {
        /// Adding the same per-session shock to both matched arms moves neither the difference nor its error.
        #[test]
        fn test_a_shared_shock_cancels_in_a_matched_difference(rows in readings()) {
            let read = |shocked: bool| {
                let shock = |value: f64| if shocked { value } else { 0.0 };
                let treatment: Vec<Option<f64>> =
                    rows.iter().map(|(treatment, _, common)| Some(treatment + shock(*common))).collect();
                let control: Vec<Option<f64>> =
                    rows.iter().map(|(_, control, common)| Some(control + shock(*common))).collect();
                let ran = measure(registered(), unpriced(), Pairing::Matched, arm("treatment", &treatment), arm("control", &control));
                (ran.effect().map(Effect::difference).unwrap(), ran.standard_errors())
            };
            let (plain, shocked) = (read(false), read(true));
            prop_assert!((plain.0 - shocked.0).abs() < 1e-6);
            match (plain.1, shocked.1) {
                (Some(left), Some(right)) => prop_assert!((left - right).abs() < 1e-4 * left.abs().max(1.0)),
                (left, right) => prop_assert_eq!(left.is_some(), right.is_some()),
            }
        }

        /// Swapping disjoint arms negates the difference and keeps its error and its degrees of freedom.
        #[test]
        fn test_swapping_disjoint_arms_negates_the_difference(rows in readings()) {
            let treatment = arm_from("treatment", 0, &rows.iter().map(|(treatment, _, _)| Some(*treatment)).collect::<Vec<_>>());
            let control = arm_from("control", 100, &rows.iter().map(|(_, control, _)| Some(*control)).collect::<Vec<_>>());
            let forward = measure(registered(), unpriced(), Pairing::Disjoint, treatment.clone(), control.clone());
            let backward = measure(registered(), unpriced(), Pairing::Disjoint, control, treatment);
            prop_assert_eq!(
                forward.effect().map(Effect::difference).map(|value| -value),
                backward.effect().map(Effect::difference)
            );
            prop_assert_eq!(forward.standard_errors().map(|value| -value), backward.standard_errors());
            prop_assert_eq!(degrees_of_freedom(&forward), degrees_of_freedom(&backward));
        }

        /// A study's record reads back as the record written, which is what lets the journal stand in for the run.
        #[test]
        fn test_a_study_ran_record_round_trips(rows in readings(), spread in 0.0..50.0f64, explore in any::<bool>()) {
            let treatment: Vec<Option<f64>> = rows.iter().map(|(treatment, _, _)| Some(*treatment)).collect();
            let control: Vec<Option<f64>> = rows.iter().map(|(_, control, _)| Some(*control)).collect();
            let lane = if explore { exploratory(spread, Direction::Lower) } else { registered() };
            let ran = measure(lane, priced(FillStyle::Aggressive, spread), Pairing::Matched, arm("treatment", &treatment), arm("control", &control));
            let encoded = serde_json::to_string(&ran).unwrap();
            prop_assert_eq!(serde_json::from_str::<StudyRan>(&encoded).unwrap(), ran);
        }
    }
}
