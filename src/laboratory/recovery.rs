//! Runs a model arm on a synthetic market planting its mechanism and on the null market planting nothing, journals
//! what it recovered, and lets the arm read any other dataset only once it has recovered what was planted.

use std::collections::BTreeSet;

use crate::common::laboratory::estimate::{Control, EstimateRefusal, Treatment};
use crate::common::laboratory::experiment::{ExperimentRefusal, Parameters};
use crate::common::laboratory::recovery::Recovery;
use crate::common::laboratory::series::Series;
use crate::common::laboratory::synthetic::generate::GenerationRefusal;
use crate::common::laboratory::synthetic::{MechanismKind, Specification};
use crate::common::market::Symbol;
use crate::common::time::calendar::TradingSession;
use crate::laboratory::dataset::{Dataset, synthetic};
use crate::laboratory::{Study, StudyError};

/// Proof that a read runs through `recover` or `Recovered::read`, the only places one is built.
pub struct Harness(());

/// A model read for the strength of one mechanism `synthetic` can plant.
pub trait ModelArm {
    type Refusal;

    /// The mechanism whose strength the arm reads.
    fn mechanism(&self) -> MechanismKind;

    /// The arm's own settings, journaled under `arm.`.
    fn parameters(&self) -> Parameters;

    /// The arm's reading of the mechanism's strength in basis points, one per session of `dataset`.
    fn read(&self, dataset: &Dataset, harness: &Harness) -> Result<Series, Self::Refusal>;
}

/// An arm that recovered what was planted, with the recovery that admitted it.
#[derive(Debug)]
pub struct Recovered<A> {
    arm: A,
    recovery: Recovery,
}

impl<A: ModelArm> Recovered<A> {
    pub fn arm(&self) -> &A {
        &self.arm
    }

    pub fn recovery(&self) -> &Recovery {
        &self.recovery
    }

    /// The arm's reading of `dataset`, real sessions included.
    pub fn read(&self, dataset: &Dataset) -> Result<Series, A::Refusal> {
        self.arm.read(dataset, &Harness(()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError<Refusal> {
    #[error("the specification plants no {mechanism} for the arm to recover")]
    NotPlanted { mechanism: MechanismKind },
    /// Several of the arm's mechanism leave no single stated strength to recover.
    #[error("the specification plants {count} {mechanism} mechanisms, not one")]
    PlantedMoreThanOnce {
        mechanism: MechanismKind,
        count: usize,
    },
    #[error("{0}")]
    Generation(GenerationRefusal),
    #[error("the arm refused to read")]
    Arm(Refusal),
    #[error("{0}")]
    Estimate(EstimateRefusal),
    #[error("{0}")]
    Experiment(ExperimentRefusal),
    #[error("{0}")]
    Study(StudyError),
    /// The recovery was journaled, but what the arm recovered did not clear its bar.
    #[error(
        "the arm recovered {} of a stated {}, which does not clear its bar",
        .0.recovered().mean(),
        .0.stated()
    )]
    Unrecovered(Box<Recovery>),
}

/// Reads `arm` over the market `specification` generates for `universe` over `sessions` and over its null, journals
/// the recovery as one experiment of `study`, and returns the arm admitted only if it recovered what was planted.
pub fn recover<A: ModelArm>(
    study: &mut Study,
    arm: A,
    specification: &Specification,
    universe: &BTreeSet<Symbol>,
    sessions: &[TradingSession],
) -> Result<Recovered<A>, RecoveryError<A::Refusal>> {
    let mechanism = arm.mechanism();
    let stated = match specification
        .mechanisms
        .iter()
        .filter(|planted| planted.kind() == mechanism)
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => return Err(RecoveryError::NotPlanted { mechanism }),
        [only] => only.flow().strength,
        several => {
            return Err(RecoveryError::PlantedMoreThanOnce {
                mechanism,
                count: several.len(),
            });
        }
    };
    let generate = |specification: &Specification| {
        synthetic(study, specification, universe, sessions).map_err(RecoveryError::Generation)
    };
    let (planted, events) = generate(specification)?;
    let (null, _) = generate(&specification.null())?;
    let harness = Harness(());
    let planted_readings = arm.read(&planted, &harness).map_err(RecoveryError::Arm)?;
    let null_readings = arm.read(&null, &harness).map_err(RecoveryError::Arm)?;
    let recovery = Recovery::new(
        stated,
        events
            .iter()
            .filter(|event| event.kind == mechanism)
            .count() as u64,
        Treatment(&planted_readings),
        Control(&null_readings),
    )
    .map_err(RecoveryError::Estimate)?;
    let parameters = Parameters::new(
        arm.parameters()
            .settings()
            .iter()
            .map(|(name, value)| (format!("arm.{}", name.as_str()), value.clone()))
            .chain([("recovering".to_string(), mechanism.to_string())]),
    )
    .map_err(RecoveryError::Experiment)?;
    let outputs = recovery.outputs().map_err(RecoveryError::Experiment)?;
    study
        .experiment(parameters, &[planted.lineage(), null.lineage()], outputs)
        .map_err(RecoveryError::Study)?;
    match recovery.recovers() {
        true => Ok(Recovered { arm, recovery }),
        false => Err(RecoveryError::Unrecovered(Box::new(recovery))),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::num::NonZeroU32;

    use chrono::{NaiveDate, TimeDelta};

    use super::*;
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::cost::BasisPoints;
    use crate::common::laboratory::experiment::Label;
    use crate::common::laboratory::synthetic::draw::{LogNormal, Seed};
    use crate::common::laboratory::synthetic::{
        Flow, Mechanism, Prevalence, Shapes, Span, Volatility, Window,
    };
    use crate::common::time::SessionDate;
    use crate::laboratory::source::Source;

    /// Reads a permanent impact as each session's mean absolute open-to-close log return across names.
    #[derive(Debug)]
    struct SessionMove;

    impl ModelArm for SessionMove {
        type Refusal = Infallible;

        fn mechanism(&self) -> MechanismKind {
            MechanismKind::WorkedParent
        }

        fn parameters(&self) -> Parameters {
            Parameters::new([("reading", "mean_absolute_session_return")]).unwrap()
        }

        fn read(&self, dataset: &Dataset, _: &Harness) -> Result<Series, Infallible> {
            let readings = dataset.bars().iter().map(|(session, partition)| {
                let mut moves = std::collections::BTreeMap::new();
                for bar in partition.rows() {
                    let Some(open_close) = bar.sums().open_close() else {
                        continue;
                    };
                    let (open, close) = (open_close.open().price(), open_close.close().price());
                    let entry = moves.entry(bar.symbol().clone()).or_insert((open, close));
                    entry.1 = close;
                }
                let returns: Vec<f64> = moves
                    .values()
                    .map(|(open, close)| (close.dollars() / open.dollars()).ln().abs() * 10_000.0)
                    .collect();
                let mean = returns.iter().sum::<f64>() / returns.len() as f64;
                (*session, (!returns.is_empty()).then_some(mean))
            });
            Ok(Series::new(readings).unwrap())
        }
    }

    fn sessions() -> Vec<TradingSession> {
        let first = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap());
        [0, 1, 2, 3, 4, 7, 8, 9]
            .map(|day| {
                TradingSession::new(
                    first.plus_calendar_days(day),
                    "09:30:00".parse().unwrap(),
                    "16:00:00".parse().unwrap(),
                )
                .unwrap()
            })
            .to_vec()
    }

    fn universe() -> BTreeSet<Symbol> {
        (0..10)
            .map(|index| Symbol::new(&format!("NAME{}", char::from(b'A' + index))).unwrap())
            .collect()
    }

    /// Quiet names, so a session's move is mostly what was planted.
    fn specification(strength: f64) -> Specification {
        let fixed = |median| LogNormal::new(median, 0.0).unwrap();
        Specification {
            seed: Seed::new(11),
            shapes: Shapes {
                price: fixed(100.0),
                volatility: fixed(0.0002),
                beta: fixed(1.0),
                trades_per_minute: fixed(2.0),
                quotes_per_minute: fixed(2.0),
                spread_ticks: fixed(1.0),
                trade_size: fixed(100.0),
                quote_size: fixed(300.0),
                market_volatility: Volatility::new(0.0).unwrap(),
            },
            mechanisms: vec![Mechanism::WorkedParent {
                flow: Flow {
                    strength: BasisPoints::new(strength).unwrap(),
                    prevalence: Prevalence::new(1.0).unwrap(),
                    pieces: NonZeroU32::new(5).unwrap(),
                    spacing: Span::new(TimeDelta::seconds(60)).unwrap(),
                },
                window: Window::new("10:00:00".parse().unwrap(), "15:00:00".parse().unwrap())
                    .unwrap(),
            }],
        }
    }

    fn study(directory: &std::path::Path) -> Study {
        Study::open(
            Label::new("recovery").unwrap(),
            directory,
            Source::new("test.rs", ""),
        )
        .unwrap()
    }

    fn directory() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("fund-recovery-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn test_a_planted_impact_is_recovered_at_its_stated_strength() {
        let directory = directory();
        let mut study = study(&directory);
        let recovered = recover(
            &mut study,
            SessionMove,
            &specification(40.0),
            &universe(),
            &sessions(),
        )
        .unwrap();
        let recovery = recovered.recovery();
        assert_eq!(recovery.planted_events(), 80);
        assert_eq!(recovery.recovered().sessions(), 8);
        let share = recovery.share().unwrap();
        assert!((0.8..1.2).contains(&share), "{recovery:?}");
        assert!(recovery.null().mean() < 4.0, "{recovery:?}");
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_nothing_is_recovered_where_nothing_moved_the_price() {
        let directory = directory();
        let mut study = study(&directory);
        let refused = recover(
            &mut study,
            SessionMove,
            &specification(0.0),
            &universe(),
            &sessions(),
        );
        match refused {
            Err(RecoveryError::Unrecovered(recovery)) => {
                assert_eq!(recovery.planted_events(), 80);
                assert!(recovery.recovered().mean().abs() < 1.0, "{recovery:?}");
            }
            other => panic!("{other:?}"),
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_an_arm_needs_its_mechanism_planted_exactly_once() {
        let directory = directory();
        let mut study = study(&directory);
        let mut twice = specification(40.0);
        twice.mechanisms.push(twice.mechanisms[0]);
        assert!(matches!(
            recover(&mut study, SessionMove, &twice, &universe(), &sessions()),
            Err(RecoveryError::PlantedMoreThanOnce {
                mechanism: MechanismKind::WorkedParent,
                count: 2
            })
        ));
        assert!(matches!(
            recover(
                &mut study,
                SessionMove,
                &twice.null(),
                &universe(),
                &sessions()
            ),
            Err(RecoveryError::NotPlanted {
                mechanism: MechanismKind::WorkedParent
            })
        ));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// The journal names both markets' specifications and the arm's settings, and records no archive read.
    #[test]
    fn test_the_recovery_journals_both_specifications_and_no_fingerprint() {
        let directory = directory();
        let mut study = study(&directory);
        recover(
            &mut study,
            SessionMove,
            &specification(40.0),
            &universe(),
            &sessions(),
        )
        .unwrap();
        let file = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        let lines = read(&std::fs::read_to_string(&file).unwrap());
        let observations: Vec<Observation> = lines
            .iter()
            .map(|line| match line {
                ReadLine::Read(read) => read.record().observation().clone(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect();
        let ran = match observations.as_slice() {
            [Observation::ExperimentRan(ran)] => ran,
            other => panic!("{other:?}"),
        };
        assert_eq!(ran.fingerprints(), []);
        let setting = |name: &str| ran.parameters().settings().get(name).cloned();
        assert_eq!(setting("recovering").as_deref(), Some("worked_parent"));
        assert_eq!(
            setting("arm.reading").as_deref(),
            Some("mean_absolute_session_return")
        );
        assert_eq!(setting("synthetic.0.mechanisms").as_deref(), Some("1"));
        assert_eq!(
            setting("synthetic.0.mechanisms.0.strength_basis_points").as_deref(),
            Some("40")
        );
        assert_eq!(setting("synthetic.1.mechanisms").as_deref(), Some("0"));
        assert_eq!(setting("synthetic.0.seed"), setting("synthetic.1.seed"));
        assert_eq!(
            setting("synthetic.1.universe").as_deref(),
            Some("NAMEA,NAMEB,NAMEC,NAMED,NAMEE,NAMEF,NAMEG,NAMEH,NAMEI,NAMEJ")
        );
        let journaled_sessions = setting("synthetic.0.sessions").unwrap();
        let journaled_sessions: Vec<&str> = journaled_sessions.split(',').collect();
        assert_eq!(journaled_sessions.len(), 8);
        assert_eq!(journaled_sessions[0], "2026-03-02 09:30:00-16:00:00");
        assert_eq!(journaled_sessions[7], "2026-03-11 09:30:00-16:00:00");
        assert!(ran.checks().is_some());
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A study's own parameter named as a synthetic dataset's would hide one of the two, so the experiment is refused.
    #[test]
    fn test_a_parameter_named_as_a_synthetic_setting_is_refused() {
        let directory = directory();
        let mut study = study(&directory);
        let (dataset, _) =
            synthetic(&study, &specification(40.0), &universe(), &sessions()).unwrap();
        let refused = study.experiment(
            Parameters::new([("synthetic.0.seed", "12")]).unwrap(),
            &[dataset.lineage()],
            Default::default(),
        );
        match refused {
            Err(StudyError::Parameters(ExperimentRefusal::Duplicate { name })) => {
                assert_eq!(name, "synthetic.0.seed");
            }
            other => panic!("{other:?}"),
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
