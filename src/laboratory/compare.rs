//! Replays a candidate strategy and its baseline over the same sessions and fill model and journals them as one
//! experiment: the paired difference of their session returns with its checks and block errors, and the candidate
//! set beside the index over the same sessions, none of which a study can leave out.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};

use crate::common::book::{Cash, ValuationRefusal};
use crate::common::laboratory::blocks::{Coarsening, Observations, coarsen};
use crate::common::laboratory::estimate::{
    Control, Estimate, Treatment, flipped_share, paired, summarize,
};
use crate::common::laboratory::experiment::{ExperimentRefusal, Metric, Name, Outputs, Parameters};
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::laboratory::sharpe::{IndexReading, beside_index, index_returns};
use crate::common::laboratory::verdict::{Cause, Checks, NullReading};
use crate::common::market::Symbol;
use crate::common::market::record::{BarInterval, Partition};
use crate::common::replay::{Controls, FillModel, Replay, Replayer};
use crate::common::strategy::Strategy;
use crate::common::time::calendar::TradingCalendar;
use crate::laboratory::Study;
use crate::laboratory::dataset::Dataset;
use crate::laboratory::replay::{Opening, ReplayStudyError, beside, metrics, run, settings};
use crate::laboratory::risk_free::RiskFree;

/// The estimate a comparison's paired difference is journaled under.
const DIFFERENCE: &str = "session_return_difference";

/// The coarsening of the arms' difference in gain at each decision, journaled beside the session difference.
const DECISION_DIFFERENCE: &str = "decision_gain_difference";

/// The setting the index's symbol is journaled under.
const INDEX: &str = "index";

/// The seed of the sign-flip null, fixed so a rerun reads the same null.
const NULL_SEED: u64 = 0;

/// How many sign flips the null draws, so the smallest share it can report is about one in ten thousand.
const NULL_PERMUTATIONS: NonZeroU32 = NonZeroU32::new(9_999).expect("nonzero");

/// Which arm of a comparison a setting or output belongs to, by the name it is journaled under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum ArmSide {
    Candidate,
    Baseline,
}

/// One side of a comparison: a strategy and the name that says what it is and the settings that make it so.
pub struct Arm<S> {
    strategy: S,
    name: Name,
}

impl<S: Strategy> Arm<S> {
    /// Named `kind("setting"="value",...)`, each quoted and escaped, so two arms of one kind are told apart by their settings in the journal.
    pub fn new(
        strategy: S,
        kind: &str,
        parameters: &Parameters,
    ) -> Result<Self, ExperimentRefusal> {
        let settings: Vec<String> = parameters
            .settings()
            .iter()
            .map(|(name, value)| format!("{:?}={value:?}", name.as_str()))
            .collect();
        Ok(Self {
            strategy,
            name: Name::new(format!("{kind}({})", settings.join(",")))?,
        })
    }

    pub fn name(&self) -> &Name {
        &self.name
    }
}

/// What a comparison's candidate is set beside: an index's daily returns from a dataset the study read, and the
/// risk-free rate both Sharpes take off.
pub struct Benchmark<'a> {
    dataset: &'a Dataset,
    symbol: Symbol,
    returns: Series,
    risk_free: &'a RiskFree,
}

/// Why an index could not be read as a benchmark.
#[derive(Debug, thiserror::Error)]
pub enum BenchmarkRefusal {
    /// No bar of the index at all, which would read as every session missing its return.
    #[error("the dataset holds no bar of {symbol}")]
    Absent { symbol: Symbol },
    /// A bar of the index at another interval, whose close the daily returns would read as a session's.
    #[error("the index has a {interval} bar, where only daily bars carry a session's close")]
    NotDaily { interval: BarInterval },
    #[error("the index returns were refused: {0}")]
    Series(SeriesRefusal),
}

impl<'a> Benchmark<'a> {
    /// `symbol`'s close-to-close returns from `dataset`'s daily bars, a session counting only when `calendar` puts the
    /// dataset's previous session on the trading day before it.
    pub fn new(
        dataset: &'a Dataset,
        symbol: Symbol,
        calendar: &TradingCalendar,
        risk_free: &'a RiskFree,
    ) -> Result<Self, BenchmarkRefusal> {
        let mut held = dataset
            .bars()
            .values()
            .flat_map(Partition::rows)
            .filter(|bar| bar.symbol() == &symbol)
            .peekable();
        if held.peek().is_none() {
            return Err(BenchmarkRefusal::Absent { symbol });
        }
        if let Some(bar) = held.find(|bar| bar.interval() != BarInterval::OneDay) {
            return Err(BenchmarkRefusal::NotDaily {
                interval: bar.interval(),
            });
        }
        let returns =
            index_returns(dataset.bars(), &symbol, calendar).map_err(BenchmarkRefusal::Series)?;
        Ok(Self {
            dataset,
            symbol,
            returns,
            risk_free,
        })
    }
}

/// Both arms' replays and what the comparison journaled from them.
#[derive(Debug)]
pub struct Comparison {
    candidate: Replay,
    baseline: Replay,
    difference: Estimate,
    checks: Checks,
    coarsening: Coarsening,
    index: IndexReading,
}

impl Comparison {
    pub fn candidate(&self) -> &Replay {
        &self.candidate
    }

    pub fn baseline(&self) -> &Replay {
        &self.baseline
    }

    /// The candidate's session return less the baseline's.
    pub fn difference(&self) -> Estimate {
        self.difference
    }

    /// The sessions the difference read, those it could not, and the sign-flip null's share at least as far from zero.
    pub fn checks(&self) -> &Checks {
        &self.checks
    }

    /// The difference in gain over the opening cash at each decision, its error read under each block grouping.
    pub fn coarsening(&self) -> &Coarsening {
        &self.coarsening
    }

    /// The candidate's Sharpe beside the index's over the candidate's measured sessions, or why it could not be read.
    pub fn index(&self) -> IndexReading {
        self.index
    }
}

/// Replays both arms over `dataset` with one fill model, decision interval, opening and controls, so they differ only
/// in the strategy, and journals one experiment naming both arms, with the candidate set beside `benchmark`.
#[expect(
    clippy::too_many_arguments,
    reason = "each is one condition both arms share, named at the call so none is mistaken for another"
)]
pub fn compare<C: Strategy, B: Strategy>(
    study: &mut Study,
    dataset: &Dataset,
    candidate: Arm<C>,
    baseline: Arm<B>,
    fill_model: FillModel,
    decision: BarInterval,
    opening: Opening,
    controls: Controls,
    benchmark: &Benchmark,
) -> Result<Comparison, ReplayStudyError> {
    let parameters = beside(
        [
            (
                ArmSide::Candidate.into(),
                candidate.name.as_str().to_string(),
            ),
            (ArmSide::Baseline.into(), baseline.name.as_str().to_string()),
            (INDEX, benchmark.symbol.as_str().to_string()),
        ]
        .into_iter()
        .chain(
            benchmark
                .risk_free
                .settings()
                .iter()
                .map(|(name, value)| (name.as_str(), value.clone())),
        ),
        settings(fill_model, decision, &controls, opening),
    )?;
    let candidate_replay = run(
        dataset,
        &Replayer::controlled(candidate.strategy, fill_model, decision, controls.clone())
            .map_err(ReplayStudyError::Controls)?,
        opening,
    )?;
    let baseline_replay = run(
        dataset,
        &Replayer::controlled(baseline.strategy, fill_model, decision, controls.clone())
            .map_err(ReplayStudyError::Controls)?,
        opening,
    )?;
    let returns = |replay: &Replay| {
        replay
            .session_returns(opening.cash())
            .map_err(ReplayStudyError::Series)
    };
    let (candidate_returns, baseline_returns) =
        (returns(&candidate_replay)?, returns(&baseline_replay)?);
    let difference = paired(Treatment(&candidate_returns), Control(&baseline_returns))
        .map_err(ReplayStudyError::Estimate)?;
    let checks = checks(&candidate_returns, &baseline_returns, difference)?;
    let coarsening = coarsen(
        &Observations::book(decision_differences(
            candidate_replay.marks(),
            baseline_replay.marks(),
            opening.cash(),
        ))
        .map_err(ReplayStudyError::Blocks)?,
    )
    .map_err(ReplayStudyError::Blocks)?;
    let index = IndexReading::from(beside_index(
        &candidate_returns,
        &benchmark.returns,
        benchmark.risk_free.rates(),
    ));
    let outputs = outputs(
        [
            (ArmSide::Candidate, &candidate_replay, &candidate_returns),
            (ArmSide::Baseline, &baseline_replay, &baseline_returns),
        ],
        &controls,
        opening,
        difference,
    )?
    .checks(checks.clone())
    .and_then(|outputs| outputs.coarsening(DECISION_DIFFERENCE, coarsening.clone()))
    .and_then(|outputs| outputs.index(index))
    .map_err(ReplayStudyError::Experiment)?;
    let (read, benchmark_read) = (dataset.lineage(), benchmark.dataset.lineage());
    let lineages = match read.fingerprint() == benchmark_read.fingerprint()
        && read.run() == benchmark_read.run()
    {
        true => vec![read],
        false => vec![read, benchmark_read],
    };
    study
        .experiment(parameters, &lineages, outputs)
        .map_err(ReplayStudyError::Study)?;
    Ok(Comparison {
        candidate: candidate_replay,
        baseline: baseline_replay,
        difference,
        checks,
        coarsening,
        index,
    })
}

/// The difference's sessions as its population, those it could not read as undefined, and the share of sign flips of
/// its session readings at least as far from zero as its own, unmeasured with its cause below two readings.
fn checks(
    candidate: &Series,
    baseline: &Series,
    difference: Estimate,
) -> Result<Checks, ReplayStudyError> {
    let differences = candidate
        .zip_with(baseline, |candidate, baseline| candidate - baseline)
        .map_err(ReplayStudyError::Series)?;
    let null = match flipped_share(&differences, NULL_SEED, NULL_PERMUTATIONS) {
        Ok(share) => {
            NullReading::Measured(Metric::new(share).expect("a share of permutations is finite"))
        }
        Err(refusal) => NullReading::Unmeasured {
            cause: Cause::new(refusal.to_string()).map_err(ReplayStudyError::Experiment)?,
        },
    };
    Checks::new(
        difference.sessions() + difference.undefined(),
        difference.undefined(),
        null,
    )
    .map_err(ReplayStudyError::Experiment)
}

/// Each arm's gain at each decision mark over the opening cash, from the mark before it or the opening before the
/// first, candidate less baseline; `None` where either arm's mark, or the one before it, was unpriced or absent.
fn decision_differences(
    candidate: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>,
    baseline: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>,
    opening: Cash,
) -> Vec<(DateTime<Utc>, Option<f64>)> {
    let gains = |marks: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>| {
        let mut previous = Some(opening);
        marks
            .iter()
            .map(|(at, mark)| {
                let mark = mark.as_ref().ok().copied();
                // Taken in exact units first, so only the ratio rounds.
                let gain = previous.zip(mark).map(|(previous, mark)| {
                    (mark.units() - previous.units()) as f64 / opening.units() as f64
                });
                previous = mark;
                (*at, gain)
            })
            .collect::<BTreeMap<DateTime<Utc>, Option<f64>>>()
    };
    let (candidate, baseline) = (gains(candidate), gains(baseline));
    let instants: std::collections::BTreeSet<DateTime<Utc>> =
        candidate.keys().chain(baseline.keys()).copied().collect();
    instants
        .into_iter()
        .map(|at| {
            let gain =
                |gains: &BTreeMap<DateTime<Utc>, Option<f64>>| gains.get(&at).copied().flatten();
            (
                at,
                gain(&candidate)
                    .zip(gain(&baseline))
                    .map(|(candidate, baseline)| candidate - baseline),
            )
        })
        .collect()
}

/// The difference, each arm's own session return, and each arm's replay metrics under its side's prefix.
fn outputs(
    arms: [(ArmSide, &Replay, &Series); 2],
    controls: &Controls,
    opening: Opening,
    difference: Estimate,
) -> Result<Outputs, ReplayStudyError> {
    let mut outputs = Outputs::default()
        .estimate(DIFFERENCE, difference)
        .map_err(ReplayStudyError::Experiment)?;
    for (side, replay, returns) in arms {
        let own = Estimate::from_summary(summarize(returns)).map_err(ReplayStudyError::Estimate)?;
        outputs = outputs
            .estimate(format!("{side}_session_return"), own)
            .map_err(ReplayStudyError::Experiment)?;
        for (metric, value) in metrics(replay, controls, opening) {
            outputs = outputs
                .metric(format!("{side}_{metric}"), value)
                .map_err(ReplayStudyError::Experiment)?;
        }
    }
    Ok(outputs)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::book::{Book, Cash};
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::blocks::{Grouping, GroupingRefusal};
    use crate::common::laboratory::cost::{BasisPoints, FillStyle};
    use crate::common::laboratory::experiment::{ExperimentRan, Label};
    use crate::common::laboratory::sharpe::{BesideIndexRefusal, SharpeRefusal};
    use crate::common::market::state::MarketState;
    use crate::common::market::{Shares, Symbol};
    use crate::common::strategy::Target;
    use crate::common::time::SessionDate;
    use crate::laboratory::dataset::tests::{
        calendar, calendar_between, daily_dataset, dataset, session,
    };
    use crate::laboratory::risk_free::tests::zero_rate;
    use crate::laboratory::source::Source;

    /// Wants one share of AAPL, whatever it has seen.
    #[derive(Clone)]
    pub(crate) struct OneShare;

    impl Strategy for OneShare {
        fn decide(&self, _: &MarketState, _: &Book) -> Target {
            Target::new(BTreeMap::from([(
                Symbol::new("AAPL").unwrap(),
                Shares::whole(1).unwrap(),
            )]))
        }
    }

    /// Wants nothing.
    #[derive(Clone)]
    pub(crate) struct Flat;

    impl Strategy for Flat {
        fn decide(&self, _: &MarketState, _: &Book) -> Target {
            Target::default()
        }
    }

    pub(crate) fn fill_model() -> FillModel {
        FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap()
    }

    pub(crate) fn opening() -> Opening {
        Opening::new(Cash::from_units(100 * 1_000_000_000_000)).unwrap()
    }

    fn study() -> (Study, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!("fund-compare-{}", uuid::Uuid::new_v4()));
        let study = Study::open(
            Label::new("compare check").unwrap(),
            &directory,
            Source::new("test.rs", ""),
        )
        .unwrap();
        (study, directory)
    }

    fn sessions(dataset: &Dataset) -> Vec<SessionDate> {
        dataset.bars().keys().copied().collect()
    }

    /// The one `experiment_ran` the study in `directory` journaled.
    pub(crate) fn journaled(directory: &std::path::Path) -> ExperimentRan {
        let mut ran = Vec::new();
        let files = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file());
        for file in files {
            for line in read(&std::fs::read_to_string(file).unwrap()) {
                match line {
                    ReadLine::Read(read) => match read.record().observation() {
                        Observation::ExperimentRan(experiment) => ran.push((**experiment).clone()),
                        Observation::ConfigurationResolved(_)
                        | Observation::PartitionWritten(_)
                        | Observation::PartitionFailed(_)
                        | Observation::ConditionsWritten(_)
                        | Observation::HealFinished(_)
                        | Observation::ArchiveSurveyed(_)
                        | Observation::ViewsChecked(_)
                        | Observation::DatasetRead(_)
                        | Observation::QuestionDeclared(_)
                        | Observation::VerdictReached(_)
                        | Observation::OrderSubmitted(_)
                        | Observation::OrderClosed(_)
                        | Observation::OrderRefused(_)
                        | Observation::OrderUnresolved(_)
                        | Observation::OrderGuarded(_)
                        | Observation::TradabilityUnread(_)
                        | Observation::BookReconciled(_)
                        | Observation::TargetDecided(_)
                        | Observation::SessionOpened(_)
                        | Observation::BarBuilt(_)
                        | Observation::TradabilityRead(_)
                        | Observation::SessionHalted(_)
                        | Observation::SessionClosed(_)
                        | Observation::PlaybookRead(_)
                        | Observation::PlaybookUploaded(_) => {}
                    },
                    ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
                }
            }
        }
        assert_eq!(ran.len(), 1);
        ran.remove(0)
    }

    /// One share bought at session 1's $10 open for 5bp and marked at the $11 closes gains 0.995% in session 1 and
    /// nothing in sessions 0 and 3 against a flat baseline: a mean of a third of that, with an error equal to it.
    #[test]
    fn test_a_comparison_is_journaled_as_one_experiment_naming_both_arms() {
        let (mut study, directory) = study();
        let dataset = dataset(study.run_id());
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("AAPL").unwrap(),
            &calendar(),
            &risk_free,
        )
        .unwrap();
        let candidate = Arm::new(
            OneShare,
            "one_share",
            &Parameters::new([("symbol", "AAPL")]).unwrap(),
        )
        .unwrap();
        let baseline = Arm::new(Flat, "flat", &Parameters::default()).unwrap();
        let comparison = compare(
            &mut study,
            &dataset,
            candidate,
            baseline,
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &benchmark,
        )
        .unwrap();
        assert_eq!(comparison.candidate().fills().len(), 1);
        assert_eq!(comparison.baseline().fills().len(), 0);
        let ran = journaled(&directory);
        std::fs::remove_dir_all(&directory).unwrap();
        let settings: Vec<_> = ran
            .parameters()
            .settings()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            settings,
            [
                ("baseline", "flat()"),
                ("candidate", r#"one_share("symbol"="AAPL")"#),
                ("controls", "unrestrained"),
                ("decision_interval", "one_day"),
                ("fill_style", "aggressive"),
                ("index", "AAPL"),
                ("opening_cash_units", "100000000000000"),
                ("quoted_spread_basis_points", "10"),
                (
                    "risk_free.digest",
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                ),
                ("risk_free.source", "massive_treasury_yield_3_month"),
            ]
        );
        let estimates: Vec<_> = ran
            .estimates()
            .iter()
            .map(|(name, estimate)| {
                (
                    name.as_str(),
                    estimate.mean(),
                    estimate.standard_error(),
                    estimate.sessions(),
                    estimate.undefined(),
                )
            })
            .collect();
        let third = 0.00995 / 3.0;
        assert_eq!(estimates.len(), 3);
        for ((name, mean, error, sessions, undefined), expected) in estimates.iter().zip([
            ("baseline_session_return", 0.0, 0.0),
            ("candidate_session_return", third, third),
            ("session_return_difference", third, third),
        ]) {
            assert_eq!(*name, expected.0);
            assert!((mean - expected.1).abs() < 1e-15, "{name} mean {mean}");
            assert!((error - expected.2).abs() < 1e-15, "{name} error {error}");
            assert_eq!((*sessions, *undefined), (3, 0), "{name}");
        }
        let metrics: Vec<_> = ran
            .metrics()
            .iter()
            .filter(|(name, _)| name.as_str().ends_with("_return"))
            .map(|(name, value)| (name.as_str(), value.value()))
            .collect();
        assert_eq!(
            metrics,
            [
                ("baseline_gross_return", 0.0),
                ("baseline_net_return", 0.0),
                ("candidate_gross_return", 0.01),
                ("candidate_net_return", 0.00995),
            ]
        );
        // One reading of 0.995% among three: every sign flip reaches it, so the null finds the whole share.
        let checks = ran.checks().unwrap();
        assert_eq!(
            (checks.population(), checks.undefined(), checks.null()),
            (3, 0, &NullReading::Measured(Metric::new(1.0).unwrap()))
        );
        assert_eq!(checks, comparison.checks());
        let coarsenings: Vec<_> = ran.coarsenings().keys().map(Name::as_str).collect();
        assert_eq!(coarsenings, [DECISION_DIFFERENCE]);
        let coarsening = &ran.coarsenings()[DECISION_DIFFERENCE];
        assert_eq!((coarsening.measured(), coarsening.undefined()), (3, 0));
        assert!((coarsening.mean() - third).abs() < 1e-15);
        let groupings: Vec<_> = coarsening
            .groupings()
            .iter()
            .map(|(grouping, reading)| {
                (
                    *grouping,
                    reading.as_ref().map(|clustered| clustered.blocks()),
                )
            })
            .collect();
        assert_eq!(
            groupings,
            [
                (Grouping::Minute, Ok(3)),
                (Grouping::FiveMinutes, Ok(3)),
                (Grouping::Hour, Ok(3)),
                (Grouping::Session, Ok(3)),
                (Grouping::Week, Err(&GroupingRefusal::OneBlock)),
                (Grouping::Name, Err(&GroupingRefusal::OneBlock)),
                (Grouping::NameSession, Ok(3)),
                (Grouping::SessionAndName, Err(&GroupingRefusal::OneBlock)),
            ]
        );
        assert_eq!(
            ran.index(),
            Some(&IndexReading::Refused {
                cause: BesideIndexRefusal::Result(SharpeRefusal::TooFewSessions {
                    count: 3,
                    minimum: 60
                })
            })
        );
    }

    /// AAPL gains $0.20 every other session over 70 sessions while the index alternates $100 and $102; the index is read
    /// over exactly the candidate's 70 sessions, from a dataset reaching one session earlier, and the sign-flip null
    /// finds no flip as far from zero as a candidate that never loses after its entry.
    #[test]
    fn test_the_candidate_is_read_beside_the_index_over_its_own_sessions() {
        let (mut study, directory) = study();
        let mut apple = vec![10.0];
        for day in 1..70 {
            apple.push(apple[day - 1] + if day % 2 == 0 { 0.2 } else { 0.0 });
        }
        let dataset = daily_dataset(study.run_id(), 0, &[("AAPL", apple)]);
        let index = daily_dataset(
            study.run_id(),
            -1,
            &[(
                "SPY",
                (0..71)
                    .map(|day| if day % 2 == 0 { 100.0 } else { 102.0 })
                    .collect(),
            )],
        );
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &index,
            Symbol::new("SPY").unwrap(),
            &calendar_between(-1, 69),
            &risk_free,
        )
        .unwrap();
        let comparison = compare(
            &mut study,
            &dataset,
            Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &benchmark,
        )
        .unwrap();
        let ran = journaled(&directory);
        std::fs::remove_dir_all(&directory).unwrap();
        assert_eq!(
            ran.fingerprints(),
            [dataset.fingerprint().clone(), index.fingerprint().clone()]
        );
        let IndexReading::Read(beside) = comparison.index() else {
            panic!("{:?}", comparison.index());
        };
        assert_eq!(ran.index(), Some(&IndexReading::Read(beside)));
        // Pinned by an independent computation of each Sharpe from the closes, the 5bp entry cost and a zero rate.
        assert_eq!(
            (beside.result().sessions(), beside.index().sessions()),
            (70, 70)
        );
        assert!(
            (beside.result().annualized() - 15.2884195763199).abs() < 1e-9,
            "{beside:?}"
        );
        assert!(
            (beside.index().annualized() - 0.15604664003837096).abs() < 1e-9,
            "{beside:?}"
        );
        assert_eq!(
            comparison.checks().null(),
            &NullReading::Measured(Metric::new(1.0 / 10_000.0).unwrap())
        );
    }

    /// An index read from the comparison's own sessions has no return for the first of them, so the candidate is not
    /// set beside it at all, and a benchmark with no bar of its symbol, or from minute bars, is refused outright.
    #[test]
    fn test_an_index_missing_a_candidate_session_is_journaled_as_refused() {
        let (mut study, directory) = study();
        let closes: Vec<f64> = (0..70).map(|day| 10.0 + day as f64).collect();
        let dataset = daily_dataset(
            study.run_id(),
            0,
            &[("AAPL", closes.clone()), ("SPY", closes)],
        );
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("SPY").unwrap(),
            &calendar_between(0, 69),
            &risk_free,
        )
        .unwrap();
        let comparison = compare(
            &mut study,
            &dataset,
            Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &benchmark,
        )
        .unwrap();
        let ran = journaled(&directory);
        assert_eq!(ran.fingerprints(), [dataset.fingerprint().clone()]);
        assert_eq!(
            comparison.index(),
            IndexReading::Refused {
                cause: BesideIndexRefusal::IndexMissing {
                    session: session(0)
                }
            }
        );
        assert!(matches!(
            Benchmark::new(
                &dataset,
                Symbol::new("QQQ").unwrap(),
                &calendar_between(0, 69),
                &risk_free
            ),
            Err(BenchmarkRefusal::Absent { symbol }) if symbol.as_str() == "QQQ"
        ));
        let minutes = crate::laboratory::dataset::tests::minute_dataset(study.run_id());
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(matches!(
            Benchmark::new(
                &minutes,
                Symbol::new("AAPL").unwrap(),
                &calendar(),
                &risk_free
            ),
            Err(BenchmarkRefusal::NotDaily {
                interval: BarInterval::OneMinute
            })
        ));
    }

    /// Each arm's gain is taken from its own previous mark: an unpriced mark leaves it and the next undefined, an
    /// instant only one arm marked is undefined, and the baseline's gain at the last instant runs from its own mark
    /// before it rather than the candidate's.
    #[test]
    fn test_decision_differences_read_each_arm_from_its_own_previous_mark() {
        let at = |minute: u32| {
            chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 3, 2, 15, minute, 0).unwrap()
        };
        let cash = |dollars: i128| Ok(Cash::from_units(dollars * 1_000));
        let unpriced = Err(ValuationRefusal::Unpriced {
            symbol: Symbol::new("AAPL").unwrap(),
        });
        let candidate = BTreeMap::from([
            (at(1), cash(101)),
            (at(2), unpriced),
            (at(3), cash(103)),
            (at(4), cash(106)),
            (at(6), cash(110)),
        ]);
        let baseline = BTreeMap::from([
            (at(1), cash(100)),
            (at(2), cash(100)),
            (at(3), cash(100)),
            (at(5), cash(102)),
            (at(6), cash(101)),
        ]);
        let differences = decision_differences(&candidate, &baseline, Cash::from_units(100_000));
        let instants: Vec<_> = differences.iter().map(|(at, _)| *at).collect();
        assert_eq!(instants, [at(1), at(2), at(3), at(4), at(5), at(6)]);
        let readings: Vec<_> = differences.iter().map(|(_, reading)| *reading).collect();
        // Candidate 110 less its 106 is 4%; baseline 101 less its own 102 is -1%, not 1% from the candidate's 100.
        for (reading, expected) in
            readings
                .iter()
                .zip([Some(0.01), None, None, None, None, Some(0.05)])
        {
            match (reading, expected) {
                (Some(reading), Some(expected)) => {
                    assert!((reading - expected).abs() < 1e-15, "{readings:?}")
                }
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => panic!("{readings:?}"),
            }
        }
    }

    /// Settings whose names hold the separators still name a different arm than the settings they mimic.
    #[test]
    fn test_settings_that_mimic_others_name_different_arms() {
        let name = |settings: &[(&str, &str)]| {
            Arm::new(
                Flat,
                "flat",
                &Parameters::new(settings.iter().copied()).unwrap(),
            )
            .unwrap()
            .name()
            .as_str()
            .to_string()
        };
        let two = name(&[("a", "x"), ("b", "y")]);
        let one = name(&[(r#"a="x",b"#, "y")]);
        assert_eq!(two, r#"flat("a"="x","b"="y")"#);
        assert_ne!(one, two);
    }

    /// A strategy compared with itself differs by exactly nothing in every session and at every decision, whatever it
    /// trades, and every sign flip of nothing ties it, so the null finds the whole share.
    #[test]
    fn test_a_strategy_against_itself_differs_by_nothing() {
        let (mut study, directory) = study();
        let dataset = dataset(study.run_id());
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("AAPL").unwrap(),
            &calendar(),
            &risk_free,
        )
        .unwrap();
        let arm = || Arm::new(OneShare, "one_share", &Parameters::default()).unwrap();
        let comparison = compare(
            &mut study,
            &dataset,
            arm(),
            arm(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &benchmark,
        )
        .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        let difference = comparison.difference();
        assert_eq!(
            (
                difference.mean(),
                difference.standard_error(),
                difference.sessions()
            ),
            (0.0, 0.0, 3)
        );
        assert_eq!(comparison.candidate(), comparison.baseline());
        assert_eq!(
            (
                comparison.coarsening().mean(),
                comparison.coarsening().independent_standard_error()
            ),
            (0.0, 0.0)
        );
        assert_eq!(
            comparison.checks().null(),
            &NullReading::Measured(Metric::new(1.0).unwrap())
        );
    }

    /// Both arms replay under the one set of controls: the guard holds each of the candidate's six buys, and the flat
    /// baseline is judged by risk at each of its six decisions too.
    #[test]
    fn test_a_restrained_comparison_restrains_both_arms() {
        let (mut study, directory) = study();
        let index = dataset(study.run_id());
        let risk_free = zero_rate(&sessions(&index));
        let benchmark = Benchmark::new(
            &index,
            Symbol::new("AAPL").unwrap(),
            &calendar(),
            &risk_free,
        )
        .unwrap();
        let dataset = crate::laboratory::dataset::tests::minute_dataset(study.run_id());
        let untradable = BTreeMap::from([(
            Symbol::new("AAPL").unwrap(),
            crate::common::guard::Tradability::Untradable,
        )]);
        let comparison = compare(
            &mut study,
            &dataset,
            Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneMinute,
            opening(),
            crate::laboratory::replay::tests::restrained(15, untradable),
            &benchmark,
        )
        .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        let (candidate, baseline) = (comparison.candidate(), comparison.baseline());
        assert_eq!(
            [
                candidate.fills().len(),
                candidate.held().len(),
                candidate.restraints().len()
            ],
            [0, 6, 6]
        );
        assert_eq!(
            [
                baseline.fills().len(),
                baseline.held().len(),
                baseline.restraints().len()
            ],
            [0, 0, 6]
        );
        // Six minute decisions, two in each session, each one a reading of the difference.
        assert_eq!(comparison.coarsening().measured(), 6);
        assert_eq!(
            comparison.coarsening().groupings()[&Grouping::Session]
                .as_ref()
                .map(|clustered| clustered.blocks()),
            Ok(3)
        );
    }

    /// A benchmark over the same partitions that another run read is refused rather than folded into this run's read.
    #[test]
    fn test_a_benchmark_another_run_read_is_refused() {
        let (mut study, directory) = study();
        let elsewhere = crate::common::journal::RunId::new(uuid::Uuid::new_v4());
        let other = dataset(elsewhere);
        let dataset = dataset(study.run_id());
        assert_eq!(dataset.fingerprint(), other.fingerprint());
        let risk_free = zero_rate(&sessions(&other));
        let benchmark = Benchmark::new(
            &other,
            Symbol::new("AAPL").unwrap(),
            &calendar(),
            &risk_free,
        )
        .unwrap();
        let refused = compare(
            &mut study,
            &dataset,
            Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
            Controls::Unrestrained,
            &benchmark,
        );
        let _ = std::fs::remove_dir_all(&directory);
        assert!(matches!(
            refused,
            Err(ReplayStudyError::Study(crate::laboratory::StudyError::ReadByAnotherRun { run }))
                if run == elsewhere
        ));
    }

    /// A comparison with no bar to decide on is refused before anything is journaled.
    #[test]
    fn test_a_comparison_with_nothing_to_decide_on_is_refused() {
        let (mut study, directory) = study();
        let dataset = dataset(study.run_id());
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("AAPL").unwrap(),
            &calendar(),
            &risk_free,
        )
        .unwrap();
        let refused = compare(
            &mut study,
            &dataset,
            Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneMinute,
            opening(),
            Controls::Unrestrained,
            &benchmark,
        );
        assert!(matches!(
            refused,
            Err(ReplayStudyError::NoDecisionBars {
                decision: BarInterval::OneMinute
            })
        ));
        assert!(
            !directory.exists()
                || std::fs::read_dir(&directory)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.is_file())
                    .all(|file| std::fs::read_to_string(file).unwrap().is_empty())
        );
        let _ = std::fs::remove_dir_all(&directory);
    }
}
