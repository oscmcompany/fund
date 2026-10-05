//! Replays a strategy over a study's dataset and journals the run as one experiment, with the fill model, the
//! decision interval and the opening cash among its parameters.

use crate::common::book::Book;
use crate::common::laboratory::experiment::{ExperimentRefusal, Outputs, Parameters};
use crate::common::market::DollarVolume;
use crate::common::monoid::Monoid;
use crate::common::replay::{Replay, ReplayRefusal, Replayer};
use crate::common::strategy::Strategy;
use crate::laboratory::dataset::Dataset;
use crate::laboratory::{Study, StudyError};

#[derive(Debug)]
pub enum ReplayStudyError {
    Replay(ReplayRefusal),
    Experiment(ExperimentRefusal),
    Study(StudyError),
}

impl std::fmt::Display for ReplayStudyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
    opening: DollarVolume,
    parameters: &Parameters,
) -> Result<Replay, ReplayStudyError> {
    let bars = dataset.bars().values().flatten().cloned();
    let replay = replayer
        .act(Replay::open(Book::funded(opening)), bars)
        .map_err(ReplayStudyError::Replay)?
        .finish();
    let fills = replayer.fills();
    let settings = parameters
        .settings()
        .iter()
        .map(|(name, value)| (name.as_str().to_string(), value.clone()))
        .chain([
            ("fill_style".to_string(), fills.style().to_string()),
            (
                "quoted_spread_basis_points".to_string(),
                fills.quoted_spread().value().to_string(),
            ),
            (
                "decision_interval".to_string(),
                replayer.decision().to_string(),
            ),
            (
                "opening_cash_units".to_string(),
                opening.units().to_string(),
            ),
        ]);
    let parameters = Parameters::new(settings).map_err(ReplayStudyError::Experiment)?;
    let outputs = outputs(&replay, opening).map_err(ReplayStudyError::Experiment)?;
    study
        .experiment(parameters, &[dataset], outputs)
        .map_err(ReplayStudyError::Study)?;
    Ok(replay)
}

/// Counts and dollars traded and paid; turnover and the final mark's return only over a funded opening, and the
/// return only when the final mark was priced.
fn outputs(replay: &Replay, opening: DollarVolume) -> Result<Outputs, ExperimentRefusal> {
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
    if opening.units() == 0 {
        return Ok(outputs);
    }
    outputs = outputs.metric("turnover", traded.dollars() / opening.dollars())?;
    match replay.marks().last_key_value() {
        Some((_, Ok(last))) => {
            // The gain is taken in exact units first, so only the ratio rounds.
            let opening_units = i128::try_from(opening.units()).expect("a dollar volume fits i128");
            let gain = last.units() - opening_units;
            outputs.metric("net_return", gain as f64 / opening_units as f64)
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
        let opening = DollarVolume::from_units(100 * 1_000_000_000_000);
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
                    | Observation::DatasetRead(_) => None,
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
}
