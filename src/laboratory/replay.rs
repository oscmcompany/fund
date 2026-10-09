//! Replays a strategy over a study's dataset and journals the run as one experiment, with the fill model, the
//! decision interval and the opening cash among its parameters.

use crate::common::book::{Book, Cash};
use crate::common::laboratory::estimate::EstimateRefusal;
use crate::common::laboratory::experiment::{ExperimentRefusal, Outputs, Parameters};
use crate::common::laboratory::series::SeriesRefusal;
use crate::common::market::record::BarInterval;
use crate::common::monoid::concatenate;
use crate::common::replay::{FillModel, Replay, ReplayRefusal, Replayer};
use crate::common::strategy::Strategy;
use crate::laboratory::dataset::Dataset;
use crate::laboratory::{Study, StudyError};

#[derive(Debug)]
pub enum ReplayStudyError {
    /// The dataset holds no bar of the interval the strategy decides on, so it would never decide.
    NoDecisionBars {
        decision: BarInterval,
    },
    /// A strategy parameter names a setting the replay journals itself.
    ReservedParameter {
        name: String,
    },
    Replay(ReplayRefusal),
    Series(SeriesRefusal),
    Estimate(EstimateRefusal),
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
            Self::ReservedParameter { name } => {
                write!(
                    formatter,
                    "the parameter {name} is one the replay journals itself"
                )
            }
            Self::Replay(refusal) => write!(formatter, "the replay was refused: {refusal:?}"),
            Self::Series(refusal) => write!(formatter, "the returns were refused: {refusal}"),
            Self::Estimate(refusal) => write!(formatter, "the estimate was refused: {refusal}"),
            Self::Experiment(refusal) => write!(formatter, "the experiment was refused: {refusal}"),
            Self::Study(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ReplayStudyError {}

/// The cash a replay opens with, positive so a return and a turnover are measurable against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Opening(Cash);

/// An opening that holds no cash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unfunded {
    cash: Cash,
}

impl std::fmt::Display for Unfunded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "an opening of {} dollars funds nothing",
            self.cash.dollars()
        )
    }
}

impl std::error::Error for Unfunded {}

impl Opening {
    pub fn new(cash: Cash) -> Result<Self, Unfunded> {
        match cash.units() > 0 {
            true => Ok(Self(cash)),
            false => Err(Unfunded { cash }),
        }
    }

    pub fn cash(self) -> Cash {
        self.0
    }
}

/// Replays `dataset` from a book funded with `opening` and journals it beside the strategy's own `parameters`.
pub fn replay<S: Strategy>(
    study: &mut Study,
    dataset: &Dataset,
    replayer: &Replayer<S>,
    opening: Opening,
    parameters: &Parameters,
) -> Result<Replay, ReplayStudyError> {
    let own = settings(replayer.fill_model(), replayer.decision(), opening);
    let parameters = beside(
        parameters
            .settings()
            .iter()
            .map(|(name, value)| (name.as_str(), value.clone())),
        own,
    )?;
    let replay = run(dataset, replayer, opening)?;
    let outputs = metrics(&replay, opening)
        .into_iter()
        .try_fold(Outputs::default(), |outputs, (name, value)| {
            outputs.metric(name, value)
        })
        .map_err(ReplayStudyError::Experiment)?;
    study
        .experiment(parameters, &[dataset], outputs)
        .map_err(ReplayStudyError::Study)?;
    Ok(replay)
}

/// The settings every replay journals itself: the decision interval, the fill model and the opening cash.
pub(crate) fn settings(
    fill_model: FillModel,
    decision: BarInterval,
    opening: Opening,
) -> [(&'static str, String); 4] {
    [
        ("decision_interval", decision.to_string()),
        ("fill_style", fill_model.style().to_string()),
        ("opening_cash_units", opening.cash().units().to_string()),
        (
            "quoted_spread_basis_points",
            fill_model.quoted_spread().value().to_string(),
        ),
    ]
}

/// `settings` with the replay's `own`, refused where one of them names an `own` setting.
pub(crate) fn beside<'a>(
    settings: impl IntoIterator<Item = (&'a str, String)>,
    own: [(&'static str, String); 4],
) -> Result<Parameters, ReplayStudyError> {
    let settings: Vec<(&str, String)> = settings.into_iter().collect();
    if let Some((name, _)) = own
        .iter()
        .find(|(name, _)| settings.iter().any(|(setting, _)| setting == name))
    {
        return Err(ReplayStudyError::ReservedParameter {
            name: name.to_string(),
        });
    }
    Parameters::new(settings.into_iter().chain(own)).map_err(ReplayStudyError::Experiment)
}

/// Replays the whole of `dataset` from a book funded with `opening`, refused where it could only misreport.
pub(crate) fn run<S: Strategy>(
    dataset: &Dataset,
    replayer: &Replayer<S>,
    opening: Opening,
) -> Result<Replay, ReplayStudyError> {
    let decision = replayer.decision();
    let bars = dataset.bars().values().flatten();
    if !bars.clone().any(|bar| bar.interval() == decision) {
        return Err(ReplayStudyError::NoDecisionBars { decision });
    }
    Ok(replayer
        .act(Replay::open(Book::funded(opening.cash())), bars.cloned())
        .map_err(ReplayStudyError::Replay)?
        .finish())
}

/// Counts, dollars traded and paid, and turnover; the final mark's return net and gross of costs only when that mark
/// was priced, every fill having landed before it.
pub(crate) fn metrics(replay: &Replay, opening: Opening) -> Vec<(&'static str, f64)> {
    let opening = opening.cash();
    let costs = concatenate(replay.fills().iter().map(|fill| fill.cost()));
    let traded = concatenate(replay.fills().iter().map(|fill| fill.notional()));
    let unpriced = replay.marks().values().filter(|mark| mark.is_err()).count();
    let mut metrics = vec![
        ("fills", replay.fills().len() as f64),
        ("unfilled", replay.unfilled().len() as f64),
        ("marks", replay.marks().len() as f64),
        ("marks_unpriced", unpriced as f64),
        ("costs_dollars", costs.dollars()),
        ("traded_dollars", traded.dollars()),
        ("turnover", traded.dollars() / opening.dollars()),
    ];
    match replay.marks().last_key_value() {
        Some((_, Ok(last))) => {
            // The gain is taken in exact units first, so only the ratio rounds.
            let gain = last.units() - opening.units();
            let paid = i128::try_from(costs.units()).expect("costs fit i128");
            metrics.extend([
                ("net_return", gain as f64 / opening.units() as f64),
                (
                    "gross_return",
                    (gain + paid) as f64 / opening.units() as f64,
                ),
            ]);
        }
        Some((_, Err(_))) | None => {}
    }
    metrics
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
        let opening = Opening::new(Cash::from_units(100 * 1_000_000_000_000)).unwrap();
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
                    | Observation::PlaybookRead(_) => None,
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
            .map(|(name, value)| (name.as_str(), value.value()))
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
        let opening = Opening::new(Cash::from_units(1)).unwrap();
        let daily = Replayer::new(OneShare, fill_model, BarInterval::OneDay);
        let none = Parameters::new([("strategy", "one_share")]).unwrap();
        let minute = Replayer::new(OneShare, fill_model, BarInterval::OneMinute);
        assert!(matches!(
            replay(&mut study, &dataset, &minute, opening, &none),
            Err(ReplayStudyError::NoDecisionBars {
                decision: BarInterval::OneMinute
            })
        ));
        for (unfunded, refused) in [
            (0, "an opening of 0 dollars funds nothing"),
            (-1, "an opening of -0.000000000001 dollars funds nothing"),
        ] {
            assert_eq!(
                Opening::new(Cash::from_units(unfunded)).map_err(|refusal| refusal.to_string()),
                Err(refused.to_string())
            );
        }
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
