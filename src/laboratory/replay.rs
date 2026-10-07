//! Replays a strategy over a study's dataset and journals the run as one experiment, with the fill model, the
//! decision interval and the opening cash among its parameters.

use crate::common::book::{Book, Cash};
use crate::common::laboratory::experiment::{ExperimentRefusal, Outputs, Parameters};
use crate::common::market::DollarVolume;
use crate::common::market::record::BarInterval;
use crate::common::monoid::Monoid;
use crate::common::replay::{Replay, ReplayRefusal, Replayer};
use crate::common::strategy::Strategy;
use crate::laboratory::dataset::Dataset;
use crate::laboratory::{Study, StudyError};

#[derive(Debug)]
pub enum ReplayStudyError {
    /// The dataset holds no bar of the interval the strategy decides on, so it would never decide.
    NoDecisionBars {
        decision: BarInterval,
    },
    /// The opening holds no cash, so neither a return nor a turnover is measurable against it.
    Unfunded {
        opening: Cash,
    },
    /// A strategy parameter names a setting the replay journals itself.
    ReservedParameter {
        name: String,
    },
    Replay(ReplayRefusal),
    Experiment(ExperimentRefusal),
    Study(StudyError),
}

impl std::fmt::Display for ReplayStudyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoDecisionBars { decision } => {
                write!(
                    formatter,
                    "the dataset holds no {decision} bar to decide on"
                )
            }
            Self::Unfunded { opening } => {
                write!(
                    formatter,
                    "an opening of {} dollars funds nothing",
                    opening.dollars()
                )
            }
            Self::ReservedParameter { name } => {
                write!(
                    formatter,
                    "the parameter {name} is one the replay journals itself"
                )
            }
            Self::Replay(refusal) => write!(formatter, "the replay was refused: {refusal:?}"),
            Self::Experiment(refusal) => write!(formatter, "the experiment was refused: {refusal}"),
            Self::Study(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ReplayStudyError {}

/// Replays `dataset` from a book funded with `opening` and journals it beside the strategy's own `parameters`.
pub fn replay<S: Strategy>(
    study: &mut Study,
    dataset: &Dataset,
    replayer: &Replayer<S>,
    opening: Cash,
    parameters: &Parameters,
) -> Result<Replay, ReplayStudyError> {
    if opening.units() <= 0 {
        return Err(ReplayStudyError::Unfunded { opening });
    }
    let decision = replayer.decision();
    let bars = dataset.bars().values().flatten();
    if !bars.clone().any(|bar| bar.interval() == decision) {
        return Err(ReplayStudyError::NoDecisionBars { decision });
    }
    let fill_model = replayer.fill_model();
    let own = [
        ("decision_interval", decision.to_string()),
        ("fill_style", fill_model.style().to_string()),
        ("opening_cash_units", opening.units().to_string()),
        (
            "quoted_spread_basis_points",
            fill_model.quoted_spread().value().to_string(),
        ),
    ];
    if let Some((name, _)) = own
        .iter()
        .find(|(name, _)| parameters.settings().contains_key(*name))
    {
        return Err(ReplayStudyError::ReservedParameter {
            name: name.to_string(),
        });
    }
    let settings = parameters
        .settings()
        .iter()
        .map(|(name, value)| (name.as_str().to_string(), value.clone()))
        .chain(own.map(|(name, value)| (name.to_string(), value)));
    let parameters = Parameters::new(settings).map_err(ReplayStudyError::Experiment)?;
    let replay = replayer
        .act(Replay::open(Book::funded(opening)), bars.cloned())
        .map_err(ReplayStudyError::Replay)?
        .finish();
    let outputs = outputs(&replay, opening).map_err(ReplayStudyError::Experiment)?;
    study
        .experiment(parameters, &[dataset], outputs)
        .map_err(ReplayStudyError::Study)?;
    Ok(replay)
}

/// Counts, dollars traded and paid, and turnover; the final mark's return net and gross of costs only when that mark
/// was priced, every fill having landed before it.
fn outputs(replay: &Replay, opening: Cash) -> Result<Outputs, ExperimentRefusal> {
    let sum = |amounts: Vec<DollarVolume>| {
        amounts
            .into_iter()
            .fold(DollarVolume::empty(), Monoid::combine)
    };
    let costs = sum(replay.fills().iter().map(|fill| fill.cost()).collect());
    let traded = sum(replay.fills().iter().map(|fill| fill.notional()).collect());
    let unpriced = replay.marks().values().filter(|mark| mark.is_err()).count();
    let mut outputs = Outputs::default()
        .metric("fills", replay.fills().len() as f64)?
        .metric("unfilled", replay.unfilled().len() as f64)?
        .metric("marks", replay.marks().len() as f64)?
        .metric("marks_unpriced", unpriced as f64)?
        .metric("costs_dollars", costs.dollars())?
        .metric("traded_dollars", traded.dollars())?;
    outputs = outputs.metric("turnover", traded.dollars() / opening.dollars())?;
    match replay.marks().last_key_value() {
        Some((_, Ok(last))) => {
            // The gain is taken in exact units first, so only the ratio rounds.
            let gain = last.units() - opening.units();
            let paid = i128::try_from(costs.units()).expect("costs fit i128");
            outputs
                .metric("net_return", gain as f64 / opening.units() as f64)?
                .metric(
                    "gross_return",
                    (gain + paid) as f64 / opening.units() as f64,
                )
        }
        Some((_, Err(_))) | None => Ok(outputs),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::cost::{BasisPoints, FillStyle};
    use crate::common::laboratory::experiment::Label;
    use crate::common::market::record::BarInterval;
    use crate::common::market::state::MarketState;
    use crate::common::market::{Shares, Symbol};
    use crate::common::replay::FillModel;
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

    /// One share bought at session 1's $10 open for a 5bp charge, marked at the $11 closes: the journal names the
    /// fill model beside the strategy's settings and reports the return net of the charge.
    #[test]
    fn test_a_replay_is_journaled_as_one_experiment() {
        let directory = std::env::temp_dir().join(format!("fund-replay-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("replay check").unwrap(), &directory).unwrap();
        let dataset = dataset(study.run_id());
        let replayer = Replayer::new(
            OneShare,
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap(),
            BarInterval::OneDay,
        );
        let opening = Cash::from_units(100 * 1_000_000_000_000);
        let parameters = Parameters::new([("strategy", "one_share")]).unwrap();
        let replay = replay(&mut study, &dataset, &replayer, opening, &parameters).unwrap();
        assert_eq!(replay.fills().len(), 1);
        let file = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        let ran = read(&std::fs::read_to_string(&file).unwrap())
            .into_iter()
            .find_map(|line| match line {
                ReadLine::Read(record) => match record.observation() {
                    Observation::ExperimentRan(ran) => Some(ran.clone()),
                    Observation::ConfigurationResolved(_)
                    | Observation::PartitionWritten(_)
                    | Observation::HealFinished(_)
                    | Observation::DatasetRead(_)
                    | Observation::OrderSubmitted(_)
                    | Observation::OrderClosed(_)
                    | Observation::OrderRefused(_)
                    | Observation::OrderUnresolved(_)
                    | Observation::OrderGuarded(_)
                    | Observation::TradabilityUnread(_)
                    | Observation::BookReconciled(_)
                    | Observation::TargetDecided(_) => None,
                },
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .unwrap();
        let settings: Vec<_> = ran
            .parameters()
            .settings()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            settings,
            [
                ("decision_interval", "one_day"),
                ("fill_style", "aggressive"),
                ("opening_cash_units", "100000000000000"),
                ("quoted_spread_basis_points", "10"),
                ("strategy", "one_share"),
            ]
        );
        let metrics: Vec<_> = ran
            .metrics()
            .iter()
            .map(|(name, value)| (name.as_str(), *value))
            .collect();
        assert_eq!(
            metrics,
            [
                ("costs_dollars", 0.005),
                ("fills", 1.0),
                ("gross_return", 0.01),
                ("marks", 3.0),
                ("marks_unpriced", 0.0),
                ("net_return", 0.00995),
                ("traded_dollars", 10.0),
                ("turnover", 0.1),
                ("unfilled", 0.0),
            ]
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A minute strategy over daily bars, an unfunded opening, or a strategy parameter named like a replay setting is
    /// refused before anything is journaled.
    #[test]
    fn test_a_replay_that_would_misreport_is_refused() {
        let directory = std::env::temp_dir().join(format!("fund-replay-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(Label::new("replay check").unwrap(), &directory).unwrap();
        let dataset = dataset(study.run_id());
        let fill_model =
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap();
        let opening = Cash::from_units(1);
        let daily = Replayer::new(OneShare, fill_model, BarInterval::OneDay);
        let none = Parameters::new([("strategy", "one_share")]).unwrap();
        let minute = Replayer::new(OneShare, fill_model, BarInterval::OneMinute);
        assert!(matches!(
            replay(&mut study, &dataset, &minute, opening, &none),
            Err(ReplayStudyError::NoDecisionBars {
                decision: BarInterval::OneMinute
            })
        ));
        assert!(matches!(
            replay(&mut study, &dataset, &daily, Cash::from_units(0), &none),
            Err(ReplayStudyError::Unfunded { .. })
        ));
        let clashing = Parameters::new([("fill_style", "mine")]).unwrap();
        assert!(matches!(
            replay(&mut study, &dataset, &daily, opening, &clashing),
            Err(ReplayStudyError::ReservedParameter { name }) if name == "fill_style"
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
