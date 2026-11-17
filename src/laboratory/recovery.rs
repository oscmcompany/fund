//! Runs a model arm on a synthetic market planting its mechanism and on the null market planting nothing, journals
//! what it recovered, and lets the arm read any other dataset only once it has recovered what was planted.

use std::collections::{BTreeMap, BTreeSet};

use crate::common::laboratory::estimate::{Control, EstimateRefusal, Treatment};
use crate::common::laboratory::experiment::{ExperimentRefusal, Parameters};
use crate::common::laboratory::recovery::{Attempt, Direction, Recovery, Shortfall};
use crate::common::laboratory::series::Series;
use crate::common::laboratory::synthetic::generate::GenerationRefusal;
use crate::common::laboratory::synthetic::{Mechanism, MechanismKind, Specification};
use crate::common::market::Symbol;
use crate::common::market::record::Partition;
use crate::common::market::trade_bars::TradeBar;
use crate::common::time::SessionDate;
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

    /// Which way the arm's reading moves when its mechanism is planted.
    fn direction(&self) -> Direction;

    /// The arm's own settings, journaled under `arm.`.
    fn parameters(&self) -> Parameters;

    /// The arm's reading of the mechanism's strength in basis points, one per session of `bars`, which carry nothing
    /// of how they were generated.
    fn read(
        &self,
        bars: &BTreeMap<SessionDate, Partition<TradeBar>>,
        harness: &Harness,
    ) -> Result<Series, Self::Refusal>;
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
        self.arm.read(dataset.bars(), &Harness(()))
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
    /// Removing every mechanism for the null would credit the arm with what the others moved.
    #[error("the specification plants {others} mechanisms besides {mechanism}")]
    PlantedOthers {
        mechanism: MechanismKind,
        others: usize,
    },
    #[error("{0}")]
    Generation(GenerationRefusal),
    #[error("the arm refused to read")]
    Arm(Refusal),
    #[error("the arm read {session} of the {market} market, which was not requested")]
    Unrequested {
        market: SyntheticMarket,
        session: SessionDate,
    },
    #[error("{0}")]
    Estimate(EstimateRefusal),
    #[error("{0}")]
    Experiment(ExperimentRefusal),
    #[error("{0}")]
    Study(StudyError),
    /// The recovery was journaled, but it does not admit the arm.
    #[error("the arm recovered {} of an expected {}: {shortfall}", .recovery.recovered().mean(), .recovery.expected())]
    Unrecovered {
        recovery: Box<Recovery>,
        shortfall: Shortfall,
    },
}

/// Which of the two generated markets an arm read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum SyntheticMarket {
    Planted,
    Null,
}

/// `readings` over exactly the requested `sessions`, a session the arm left out counted as unmeasured.
fn covering<Refusal>(
    readings: Series,
    sessions: &[TradingSession],
    market: SyntheticMarket,
) -> Result<Series, RecoveryError<Refusal>> {
    let requested: BTreeSet<SessionDate> = sessions.iter().map(|session| session.date()).collect();
    if let Some(session) = readings
        .readings()
        .keys()
        .find(|session| !requested.contains(session))
    {
        return Err(RecoveryError::Unrequested {
            market,
            session: *session,
        });
    }
    Ok(Series::new(requested.into_iter().map(|session| {
        (
            session,
            readings.readings().get(&session).copied().flatten(),
        )
    }))
    .expect("readings already admitted, each requested session once"))
}

/// Reads `arm` over the market `specification` generates for `universe` over `sessions` and over its null, journals
/// the recovery as one experiment of `study`, and returns the arm admitted only if it recovered what was planted at
/// the bar of its `attempt`, which the caller counts and the journal records.
pub fn recover<A: ModelArm>(
    study: &mut Study,
    arm: A,
    specification: &Specification,
    universe: &BTreeSet<Symbol>,
    sessions: &[TradingSession],
    attempt: Attempt,
) -> Result<Recovered<A>, RecoveryError<A::Refusal>> {
    let mechanism = arm.mechanism();
    let (own, others): (Vec<&Mechanism>, Vec<&Mechanism>) = specification
        .mechanisms
        .iter()
        .partition(|planted| planted.kind() == mechanism);
    if !others.is_empty() {
        return Err(RecoveryError::PlantedOthers {
            mechanism,
            others: others.len(),
        });
    }
    let stated = match own.as_slice() {
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
    let planted_readings = covering(
        arm.read(planted.bars(), &harness)
            .map_err(RecoveryError::Arm)?,
        sessions,
        SyntheticMarket::Planted,
    )?;
    let null_readings = covering(
        arm.read(null.bars(), &harness)
            .map_err(RecoveryError::Arm)?,
        sessions,
        SyntheticMarket::Null,
    )?;
    let direction = arm.direction();
    let recovery = Recovery::new(
        stated,
        direction,
        attempt,
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
            .chain([
                ("recovering".to_string(), mechanism.to_string()),
                ("direction".to_string(), direction.to_string()),
                ("attempt".to_string(), attempt.get().to_string()),
            ]),
    )
    .map_err(RecoveryError::Experiment)?;
    let outputs = recovery.outputs().map_err(RecoveryError::Experiment)?;
    study
        .experiment(parameters, &[planted.lineage(), null.lineage()], outputs)
        .map_err(RecoveryError::Study)?;
    match recovery.admits() {
        Ok(()) => Ok(Recovered { arm, recovery }),
        Err(shortfall) => Err(RecoveryError::Unrecovered {
            recovery: Box::new(recovery),
            shortfall,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::num::NonZeroU32;

    use chrono::{NaiveDate, TimeDelta};

    use super::*;
    use crate::common::cost::BasisPoints;
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::experiment::{ExperimentRan, Label};
    use crate::common::laboratory::synthetic::draw::{LogNormal, Seed};
    use crate::common::laboratory::synthetic::{
        Flow, Prevalence, Shapes, Span, Volatility, Window,
    };
    use crate::laboratory::source::Source;

    /// Reads a permanent impact as each session's mean across names of the open-to-close log return signed by the
    /// bar-to-bar jumps wider than the bid-ask bounce, zero for a name with none; an absolute return would read the
    /// noise's magnitude on the null and recover short of the plant.
    #[derive(Debug)]
    struct SessionMove;

    /// Wider than a one-tick bounce on the quiet names `specification` draws, narrower than one planted piece.
    const JUMP_BASIS_POINTS: f64 = 4.0;

    /// One name's session so far: its open, its last close, and the signed sum of its jumps.
    struct NameSession {
        open: f64,
        close: f64,
        jumps: f64,
    }

    impl ModelArm for SessionMove {
        type Refusal = Infallible;

        fn mechanism(&self) -> MechanismKind {
            MechanismKind::WorkedParent
        }

        fn direction(&self) -> Direction {
            Direction::Rises
        }

        fn parameters(&self) -> Parameters {
            Parameters::new([
                ("reading", "jump_signed_session_return".to_string()),
                ("jump_basis_points", JUMP_BASIS_POINTS.to_string()),
            ])
            .unwrap()
        }

        fn read(
            &self,
            bars: &BTreeMap<SessionDate, Partition<TradeBar>>,
            _: &Harness,
        ) -> Result<Series, Infallible> {
            let readings = bars.iter().map(|(session, partition)| {
                let mut names = BTreeMap::new();
                for bar in partition.rows() {
                    let Some(open_close) = bar.sums().open_close() else {
                        continue;
                    };
                    let (open, close) = (
                        open_close.open().price().dollars(),
                        open_close.close().price().dollars(),
                    );
                    let name = names.entry(bar.symbol().clone()).or_insert(NameSession {
                        open,
                        close: open,
                        jumps: 0.0,
                    });
                    let step = (close / name.close).ln() * 10_000.0;
                    if step.abs() > JUMP_BASIS_POINTS {
                        name.jumps += step;
                    }
                    name.close = close;
                }
                let returns: Vec<f64> = names
                    .values()
                    .map(|name| {
                        let sign = match name.jumps {
                            0.0 => 0.0,
                            jumps => jumps.signum(),
                        };
                        sign * (name.close / name.open).ln() * 10_000.0
                    })
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

    /// The one experiment journaled in `directory`.
    fn journaled(directory: &std::path::Path) -> ExperimentRan {
        let file = std::fs::read_dir(directory)
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
        match observations.as_slice() {
            [Observation::ExperimentRan(ran)] => (**ran).clone(),
            other => panic!("{other:?}"),
        }
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
            Attempt::FIRST,
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
            Attempt::FIRST,
        );
        match refused {
            Err(RecoveryError::Unrecovered { recovery, .. }) => {
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
            recover(
                &mut study,
                SessionMove,
                &twice,
                &universe(),
                &sessions(),
                Attempt::FIRST
            ),
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
                &sessions(),
                Attempt::FIRST
            ),
            Err(RecoveryError::NotPlanted {
                mechanism: MechanismKind::WorkedParent
            })
        ));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A second mechanism would move the planted market but not the null, crediting the arm with what it moved.
    #[test]
    fn test_an_arm_is_refused_a_specification_planting_another_mechanism() {
        let directory = directory();
        let mut study = study(&directory);
        let mut mixed = specification(0.0);
        mixed.mechanisms.push(Mechanism::ClosingFlow {
            flow: Flow {
                strength: BasisPoints::new(40.0).unwrap(),
                prevalence: Prevalence::new(1.0).unwrap(),
                pieces: NonZeroU32::new(5).unwrap(),
                spacing: Span::new(TimeDelta::seconds(60)).unwrap(),
            },
            half_life: Span::new(TimeDelta::hours(10)).unwrap(),
        });
        assert!(matches!(
            recover(
                &mut study,
                SessionMove,
                &mixed,
                &universe(),
                &sessions(),
                Attempt::FIRST
            ),
            Err(RecoveryError::PlantedOthers {
                mechanism: MechanismKind::WorkedParent,
                others: 1
            })
        ));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Reads as `SessionMove` does, then alters the readings it returns from read `from` on, `recover` reading the
    /// planted market first and the null second.
    #[derive(Debug)]
    struct Altered {
        alter: fn(Series) -> Series,
        from: usize,
        direction: Direction,
        reads: std::cell::Cell<usize>,
    }

    impl Altered {
        fn new(alter: fn(Series) -> Series, from: usize) -> Self {
            Self {
                alter,
                from,
                direction: SessionMove.direction(),
                reads: std::cell::Cell::new(0),
            }
        }

        fn falling(alter: fn(Series) -> Series) -> Self {
            Self {
                direction: Direction::Falls,
                ..Self::new(alter, 0)
            }
        }
    }

    /// Every measured reading multiplied by `factor`.
    fn scaled(series: Series, factor: f64) -> Series {
        Series::new(
            series
                .readings()
                .iter()
                .map(|(session, reading)| (*session, reading.map(|value| value * factor))),
        )
        .unwrap()
    }

    impl ModelArm for Altered {
        type Refusal = Infallible;

        fn mechanism(&self) -> MechanismKind {
            SessionMove.mechanism()
        }

        fn direction(&self) -> Direction {
            self.direction
        }

        fn parameters(&self) -> Parameters {
            SessionMove.parameters()
        }

        fn read(
            &self,
            bars: &BTreeMap<SessionDate, Partition<TradeBar>>,
            harness: &Harness,
        ) -> Result<Series, Infallible> {
            let read = self.reads.replace(self.reads.get() + 1);
            SessionMove
                .read(bars, harness)
                .map(|series| match read >= self.from {
                    true => (self.alter)(series),
                    false => series,
                })
        }
    }

    /// An arm reading a tenth of the move detects the plant far beyond its error, but has not measured it.
    #[test]
    fn test_an_arm_recovering_a_tenth_of_the_plant_is_refused() {
        let directory = directory();
        let mut study = study(&directory);
        let refused = recover(
            &mut study,
            Altered::new(|series| scaled(series, 0.1), 0),
            &specification(40.0),
            &universe(),
            &sessions(),
            Attempt::FIRST,
        );
        match refused {
            Err(RecoveryError::Unrecovered {
                recovery,
                shortfall: Shortfall::Inaccurate { expected, .. },
            }) => {
                assert_eq!(expected, 40.0);
                let share = recovery.share().unwrap();
                assert!((0.08..0.12).contains(&share), "{recovery:?}");
            }
            other => panic!("{other:?}"),
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_an_arm_whose_reading_falls_is_admitted_on_a_fall_and_journals_its_direction() {
        let directory = directory();
        let mut study = study(&directory);
        let recovered = recover(
            &mut study,
            Altered::falling(|series| scaled(series, -1.0)),
            &specification(40.0),
            &universe(),
            &sessions(),
            Attempt::new(NonZeroU32::new(3).unwrap()),
        )
        .unwrap();
        assert_eq!(recovered.recovery().expected(), -40.0);
        assert!(recovered.recovery().recovered().mean() < -30.0);
        let ran = journaled(&directory);
        let setting = |name: &str| ran.parameters().settings().get(name).cloned();
        assert_eq!(setting("direction").as_deref(), Some("falls"));
        assert_eq!(setting("attempt").as_deref(), Some("3"));
        let refused = recover(
            &mut study,
            Altered::new(|series| scaled(series, -1.0), 0),
            &specification(40.0),
            &universe(),
            &sessions(),
            Attempt::FIRST,
        );
        assert!(matches!(
            refused,
            Err(RecoveryError::Unrecovered {
                shortfall: Shortfall::WrongDirection { .. },
                ..
            })
        ));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_a_session_the_arm_left_out_counts_as_unmeasured() {
        let directory = directory();
        let mut study = study(&directory);
        let recovered = recover(
            &mut study,
            Altered::new(
                |series| {
                    Series::new(
                        series
                            .readings()
                            .iter()
                            .skip(1)
                            .map(|(session, reading)| (*session, *reading)),
                    )
                    .unwrap()
                },
                0,
            ),
            &specification(40.0),
            &universe(),
            &sessions(),
            Attempt::FIRST,
        )
        .unwrap();
        let recovery = recovered.recovery();
        assert_eq!(
            (
                recovery.recovered().sessions(),
                recovery.recovered().undefined()
            ),
            (7, 1)
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Each market's readings are checked, the null's after the planted market's pass.
    #[test]
    fn test_a_session_that_was_not_requested_is_refused() {
        for (from, expected) in [(0, SyntheticMarket::Planted), (1, SyntheticMarket::Null)] {
            let directory = directory();
            let mut study = study(&directory);
            let refused = recover(
                &mut study,
                Altered::new(
                    |series| {
                        let extra =
                            SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 4, 1).unwrap());
                        Series::new(
                            series
                                .readings()
                                .iter()
                                .map(|(session, reading)| (*session, *reading))
                                .chain([(extra, Some(1.0))]),
                        )
                        .unwrap()
                    },
                    from,
                ),
                &specification(40.0),
                &universe(),
                &sessions(),
                Attempt::FIRST,
            );
            match refused {
                Err(RecoveryError::Unrequested { market, session }) => {
                    assert_eq!(market, expected);
                    assert_eq!(session.to_string(), "2026-04-01");
                }
                other => panic!("{other:?}"),
            }
            std::fs::remove_dir_all(&directory).unwrap();
        }
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
            Attempt::FIRST,
        )
        .unwrap();
        let ran = journaled(&directory);
        assert_eq!(ran.fingerprints(), []);
        let setting = |name: &str| ran.parameters().settings().get(name).cloned();
        assert_eq!(setting("recovering").as_deref(), Some("worked_parent"));
        assert_eq!(setting("direction").as_deref(), Some("rises"));
        assert_eq!(setting("attempt").as_deref(), Some("1"));
        assert_eq!(
            setting("arm.reading").as_deref(),
            Some("jump_signed_session_return")
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
