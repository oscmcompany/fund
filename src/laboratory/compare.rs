//! Replays a candidate strategy and its baseline over the same sessions and fill model and journals them as one
//! experiment, with the paired difference of their session returns, so a shock both arms share cancels.

use crate::common::laboratory::estimate::{Control, Estimate, Treatment, paired, summarize};
use crate::common::laboratory::experiment::{ExperimentRefusal, Name, Outputs, Parameters};
use crate::common::laboratory::series::Series;
use crate::common::market::record::BarInterval;
use crate::common::replay::{FillModel, Replay, Replayer};
use crate::common::strategy::Strategy;
use crate::laboratory::Study;
use crate::laboratory::dataset::Dataset;
use crate::laboratory::replay::{Opening, ReplayStudyError, beside, metrics, run, settings};

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

/// Both arms' replays and the candidate's session return less the baseline's.
#[derive(Debug)]
pub struct Comparison {
    candidate: Replay,
    baseline: Replay,
    difference: Estimate,
}

impl Comparison {
    pub fn candidate(&self) -> &Replay {
        &self.candidate
    }

    pub fn baseline(&self) -> &Replay {
        &self.baseline
    }

    pub fn difference(&self) -> Estimate {
        self.difference
    }
}

/// Replays both arms over `dataset` with one fill model, decision interval and opening, so they differ only in the
/// strategy, and journals one experiment naming both arms.
pub fn compare<C: Strategy, B: Strategy>(
    study: &mut Study,
    dataset: &Dataset,
    candidate: Arm<C>,
    baseline: Arm<B>,
    fill_model: FillModel,
    decision: BarInterval,
    opening: Opening,
) -> Result<Comparison, ReplayStudyError> {
    let parameters = beside(
        [
            ("candidate", candidate.name.as_str().to_string()),
            ("baseline", baseline.name.as_str().to_string()),
        ],
        settings(fill_model, decision, opening),
    )?;
    let candidate = run(
        dataset,
        &Replayer::new(candidate.strategy, fill_model, decision),
        opening,
    )?;
    let baseline = run(
        dataset,
        &Replayer::new(baseline.strategy, fill_model, decision),
        opening,
    )?;
    let returns = |replay: &Replay| {
        replay
            .session_returns(opening.cash())
            .map_err(ReplayStudyError::Series)
    };
    let (candidate_returns, baseline_returns) = (returns(&candidate)?, returns(&baseline)?);
    let difference = paired(Treatment(&candidate_returns), Control(&baseline_returns))
        .map_err(ReplayStudyError::Estimate)?;
    let outputs = outputs(
        [
            ("candidate", &candidate, &candidate_returns),
            ("baseline", &baseline, &baseline_returns),
        ],
        opening,
        difference,
    )?;
    study
        .experiment(parameters, &[dataset], outputs)
        .map_err(ReplayStudyError::Study)?;
    Ok(Comparison {
        candidate,
        baseline,
        difference,
    })
}

/// The difference, each arm's own session return, and each arm's replay metrics under its side's prefix.
fn outputs(
    arms: [(&str, &Replay, &Series); 2],
    opening: Opening,
    difference: Estimate,
) -> Result<Outputs, ReplayStudyError> {
    let mut outputs = Outputs::default()
        .estimate("session_return_difference", difference)
        .map_err(ReplayStudyError::Experiment)?;
    for (side, replay, returns) in arms {
        let own = Estimate::try_from(summarize(returns)).map_err(ReplayStudyError::Estimate)?;
        outputs = outputs
            .estimate(format!("{side}_session_return"), own)
            .map_err(ReplayStudyError::Experiment)?;
        for (name, value) in metrics(replay, opening) {
            outputs = outputs
                .metric(format!("{side}_{name}"), value)
                .map_err(ReplayStudyError::Experiment)?;
        }
    }
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::book::{Book, Cash};
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::cost::{BasisPoints, FillStyle};
    use crate::common::laboratory::experiment::{ExperimentRan, Label};
    use crate::common::market::state::MarketState;
    use crate::common::market::{Shares, Symbol};
    use crate::common::strategy::Target;
    use crate::laboratory::dataset::tests::dataset;

    /// Wants one share of AAPL, whatever it has seen.
    struct OneShare;

    impl Strategy for OneShare {
        fn decide(&self, _: &MarketState, _: &Book) -> Target {
            Target::new(BTreeMap::from([(
                Symbol::new("AAPL").unwrap(),
                Shares::whole(1).unwrap(),
            )]))
        }
    }

    /// Wants nothing.
    struct Flat;

    impl Strategy for Flat {
        fn decide(&self, _: &MarketState, _: &Book) -> Target {
            Target::default()
        }
    }

    fn fill_model() -> FillModel {
        FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap()
    }

    fn opening() -> Opening {
        Opening::new(Cash::from_units(100 * 1_000_000_000_000)).unwrap()
    }

    /// The one `experiment_ran` the study in `directory` journaled.
    fn journaled(directory: &std::path::Path) -> ExperimentRan {
        let mut ran = Vec::new();
        for entry in std::fs::read_dir(directory).unwrap() {
            for line in read(&std::fs::read_to_string(entry.unwrap().path()).unwrap()) {
                match line {
                    ReadLine::Read(record) => match record.observation() {
                        Observation::ExperimentRan(experiment) => ran.push((**experiment).clone()),
                        Observation::ConfigurationResolved(_)
                        | Observation::PartitionWritten(_)
                        | Observation::PartitionFailed(_)
                        | Observation::ConditionsWritten(_)
                        | Observation::HealFinished(_)
                        | Observation::DatasetRead(_)
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
                        | Observation::FeedChanged(_)
                        | Observation::SessionHalted(_)
                        | Observation::SessionClosed(_)
                        | Observation::PlaybookRead(_) => {}
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
        let directory = std::env::temp_dir().join(format!("fund-compare-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("compare check").unwrap(), &directory).unwrap();
        let dataset = dataset(study.run_id());
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
                ("decision_interval", "one_day"),
                ("fill_style", "aggressive"),
                ("opening_cash_units", "100000000000000"),
                ("quoted_spread_basis_points", "10"),
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

    /// A strategy compared with itself differs by exactly nothing in every session, whatever it trades.
    #[test]
    fn test_a_strategy_against_itself_differs_by_nothing() {
        let directory = std::env::temp_dir().join(format!("fund-compare-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("compare check").unwrap(), &directory).unwrap();
        let dataset = dataset(study.run_id());
        let arm = || Arm::new(OneShare, "one_share", &Parameters::default()).unwrap();
        let comparison = compare(
            &mut study,
            &dataset,
            arm(),
            arm(),
            fill_model(),
            BarInterval::OneDay,
            opening(),
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
    }

    /// A comparison with no bar to decide on is refused before anything is journaled.
    #[test]
    fn test_a_comparison_with_nothing_to_decide_on_is_refused() {
        let directory = std::env::temp_dir().join(format!("fund-compare-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("compare check").unwrap(), &directory).unwrap();
        let dataset = dataset(study.run_id());
        let refused = compare(
            &mut study,
            &dataset,
            Arm::new(OneShare, "one_share", &Parameters::default()).unwrap(),
            Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneMinute,
            opening(),
        );
        assert!(matches!(
            refused,
            Err(ReplayStudyError::NoDecisionBars {
                decision: BarInterval::OneMinute
            })
        ));
        assert!(
            !directory.exists()
                || std::fs::read_dir(&directory).unwrap().all(|entry| {
                    std::fs::read_to_string(entry.unwrap().path())
                        .unwrap()
                        .is_empty()
                })
        );
        let _ = std::fs::remove_dir_all(&directory);
    }
}
