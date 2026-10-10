//! Replays a strategy over a study's dataset and journals the run as one experiment, with the fill model, the
//! decision interval and the opening cash among its parameters.

use crate::common::book::{Book, Cash};
use crate::common::laboratory::estimate::EstimateRefusal;
use crate::common::laboratory::experiment::{ExperimentRefusal, Outputs, Parameters};
use crate::common::laboratory::series::SeriesRefusal;
use crate::common::market::record::{BarInterval, BarPartition};
use crate::common::monoid::concatenate;
use crate::common::replay::{
    Controls, ControlsRefusal, FillModel, Replay, ReplayRefusal, Replayer,
};
use crate::common::risk::Limit;
use crate::common::strategy::Strategy;
use crate::laboratory::dataset::Dataset;
use crate::laboratory::{Study, StudyError};

#[derive(Debug, thiserror::Error)]
pub enum ReplayStudyError {
    /// The dataset holds no bar of the interval the strategy decides on, so it would never decide.
    #[error("the dataset holds no {decision} bar to decide on")]
    NoDecisionBars { decision: BarInterval },
    /// A strategy parameter names a setting the replay journals itself.
    #[error("the parameter {setting} is one the replay journals itself")]
    ReservedParameter { setting: ReplaySetting },
    #[error("the replay was refused: {0:?}")]
    Replay(ReplayRefusal),
    #[error("the controls were refused: {0}")]
    Controls(ControlsRefusal),
    #[error("the returns were refused: {0}")]
    Series(SeriesRefusal),
    #[error("the estimate was refused: {0}")]
    Estimate(EstimateRefusal),
    #[error("the experiment was refused: {0}")]
    Experiment(ExperimentRefusal),
    #[error("{0}")]
    Study(StudyError),
}

/// A setting every replay journals itself, by the name it is journaled under.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::EnumString, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum ReplaySetting {
    DecisionInterval,
    FillStyle,
    OpeningCashUnits,
    QuotedSpreadBasisPoints,
    Controls,
    GrossLimitUnits,
    PerNameLimitUnits,
    DailyLossLimitUnits,
    FlatBeforeCloseSeconds,
    Tradability,
}

/// A measurement every replay journals, by the name it is journaled under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum ReplayMetric {
    Fills,
    Unfilled,
    Marks,
    MarksUnpriced,
    CostsDollars,
    TradedDollars,
    Turnover,
    NetReturn,
    GrossReturn,
    RiskCuts,
    RiskRefusals,
    OrdersHeld,
}

/// The cash a replay opens with, positive so a return and a turnover are measurable against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Opening(Cash);

/// An opening that holds no cash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("an opening of {} dollars funds nothing", .cash.dollars())]
pub struct Unfunded {
    cash: Cash,
}

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
    let own = settings(
        replayer.fill_model(),
        replayer.decision(),
        replayer.controls(),
        opening,
    );
    let parameters = beside(
        parameters
            .settings()
            .iter()
            .map(|(name, value)| (name.as_str(), value.clone())),
        own,
    )?;
    let replay = run(dataset, replayer, opening)?;
    let outputs = metrics(&replay, replayer.controls(), opening)
        .into_iter()
        .try_fold(Outputs::default(), |outputs, (metric, value)| {
            outputs.metric(<&str>::from(metric), value)
        })
        .map_err(ReplayStudyError::Experiment)?;
    study
        .experiment(parameters, &[dataset], outputs)
        .map_err(ReplayStudyError::Study)?;
    Ok(replay)
}

/// The settings every replay journals itself: the decision interval, the fill model, the opening cash and its
/// controls, with the limits and tradability a restrained replay applies.
pub(crate) fn settings(
    fill_model: FillModel,
    decision: BarInterval,
    controls: &Controls,
    opening: Opening,
) -> Vec<(ReplaySetting, String)> {
    let mut settings = vec![
        (ReplaySetting::DecisionInterval, decision.to_string()),
        (ReplaySetting::FillStyle, fill_model.style().to_string()),
        (
            ReplaySetting::OpeningCashUnits,
            opening.cash().units().to_string(),
        ),
        (
            ReplaySetting::QuotedSpreadBasisPoints,
            fill_model.quoted_spread().value().to_string(),
        ),
    ];
    settings.push((ReplaySetting::Controls, <&str>::from(controls).to_string()));
    match controls {
        Controls::Unrestrained => {}
        Controls::Restrained(restraint) => {
            let limits = restraint.limits();
            let units = |limit| limits.dollars(limit).units().to_string();
            settings.extend([
                (ReplaySetting::GrossLimitUnits, units(Limit::Gross)),
                (ReplaySetting::PerNameLimitUnits, units(Limit::PerName)),
                (ReplaySetting::DailyLossLimitUnits, units(Limit::DailyLoss)),
                (
                    ReplaySetting::FlatBeforeCloseSeconds,
                    limits.flat_before_close().num_seconds().to_string(),
                ),
                (
                    ReplaySetting::Tradability,
                    serde_json::to_string(restraint.tradability())
                        .expect("a tradability map has only string keys"),
                ),
            ]);
        }
    }
    settings
}

/// `settings` with the replay's `own`, refused where one of them names a `ReplaySetting`.
pub(crate) fn beside<'a>(
    settings: impl IntoIterator<Item = (&'a str, String)>,
    own: Vec<(ReplaySetting, String)>,
) -> Result<Parameters, ReplayStudyError> {
    let settings: Vec<(&str, String)> = settings.into_iter().collect();
    if let Some(setting) = settings.iter().find_map(|(name, _)| name.parse().ok()) {
        return Err(ReplayStudyError::ReservedParameter { setting });
    }
    let own = own
        .into_iter()
        .map(|(setting, value)| (<&str>::from(setting), value));
    Parameters::new(settings.into_iter().chain(own)).map_err(ReplayStudyError::Experiment)
}

/// Replays the whole of `dataset` from a book funded with `opening`, refused where it could only misreport.
pub(crate) fn run<S: Strategy>(
    dataset: &Dataset,
    replayer: &Replayer<S>,
    opening: Opening,
) -> Result<Replay, ReplayStudyError> {
    let decision = replayer.decision();
    let bars = dataset.bars().values().flat_map(BarPartition::bars);
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
pub(crate) fn metrics(
    replay: &Replay,
    controls: &Controls,
    opening: Opening,
) -> Vec<(ReplayMetric, f64)> {
    let opening = opening.cash();
    let costs = concatenate(replay.fills().iter().map(|fill| fill.cost()));
    let traded = concatenate(replay.fills().iter().map(|fill| fill.notional()));
    let unpriced = replay.marks().values().filter(|mark| mark.is_err()).count();
    let mut metrics = vec![
        (ReplayMetric::Fills, replay.fills().len() as f64),
        (ReplayMetric::Unfilled, replay.unfilled().len() as f64),
        (ReplayMetric::Marks, replay.marks().len() as f64),
        (ReplayMetric::MarksUnpriced, unpriced as f64),
        (ReplayMetric::CostsDollars, costs.dollars()),
        (ReplayMetric::TradedDollars, traded.dollars()),
        (ReplayMetric::Turnover, traded.dollars() / opening.dollars()),
    ];
    match replay.marks().last_key_value() {
        Some((_, Ok(last))) => {
            // The gain is taken in exact units first, so only the ratio rounds.
            let gain = last.units() - opening.units();
            let paid = i128::try_from(costs.units()).expect("costs fit i128");
            metrics.extend([
                (
                    ReplayMetric::NetReturn,
                    gain as f64 / opening.units() as f64,
                ),
                (
                    ReplayMetric::GrossReturn,
                    (gain + paid) as f64 / opening.units() as f64,
                ),
            ]);
        }
        Some((_, Err(_))) | None => {}
    }
    match controls {
        Controls::Unrestrained => {}
        Controls::Restrained(_) => {
            let cuts: usize = replay.restraints().values().flatten().map(Vec::len).sum();
            let refusals = replay
                .restraints()
                .values()
                .filter(|judged| judged.is_err())
                .count();
            metrics.extend([
                (ReplayMetric::RiskCuts, cuts as f64),
                (ReplayMetric::RiskRefusals, refusals as f64),
                (ReplayMetric::OrdersHeld, replay.held().len() as f64),
            ]);
        }
    }
    metrics
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::guard::Tradability;
    use crate::common::journal::{Observation, ReadLine, RunId, read};
    use crate::common::laboratory::cost::{BasisPoints, FillStyle};
    use crate::common::laboratory::experiment::Label;
    use crate::common::market::record::BarInterval;
    use crate::common::market::state::MarketState;
    use crate::common::market::{Shares, Symbol};
    use crate::common::replay::Restraint;
    use crate::common::risk::Limits;
    use crate::common::strategy::Target;
    use crate::laboratory::dataset::tests::{calendar, dataset, minute_dataset};

    /// $1,000 on every dollar limit over the test dataset's calendar, flat `flat_before_close_minutes` before the close.
    pub(crate) fn restrained(
        flat_before_close_minutes: i64,
        tradability: BTreeMap<Symbol, Tradability>,
    ) -> Controls {
        let thousand = Cash::from_units(1_000 * 1_000_000_000_000);
        let limits = Limits::new(
            thousand,
            thousand,
            thousand,
            chrono::TimeDelta::minutes(flat_before_close_minutes),
        )
        .unwrap();
        Controls::Restrained(Restraint::new(limits, calendar(), tradability))
    }

    /// Flat all session, risk cuts each of the six decisions; with AAPL untradable, the guard holds each buy instead.
    #[test]
    fn test_the_restraint_metrics_count_what_risk_and_the_guard_did() {
        let dataset = minute_dataset(RunId::new(uuid::Uuid::new_v4()));
        let opening = Opening::new(Cash::from_units(100 * 1_000_000_000_000)).unwrap();
        let fill_model =
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap();
        let measured = |controls: Controls| {
            let replayer = Replayer::controlled(
                OneShare,
                fill_model,
                BarInterval::OneMinute,
                controls.clone(),
            )
            .unwrap();
            let replay = run(&dataset, &replayer, opening).unwrap();
            metrics(&replay, &controls, opening)
                .into_iter()
                .map(|(metric, value)| (<&str>::from(metric), value))
                .filter(|(name, _)| ["risk_cuts", "risk_refusals", "orders_held"].contains(name))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            measured(restrained(390, BTreeMap::new())),
            [
                ("risk_cuts", 6.0),
                ("risk_refusals", 0.0),
                ("orders_held", 0.0)
            ]
        );
        let untradable = BTreeMap::from([(Symbol::new("AAPL").unwrap(), Tradability::Untradable)]);
        assert_eq!(
            measured(restrained(15, untradable)),
            [
                ("risk_cuts", 0.0),
                ("risk_refusals", 0.0),
                ("orders_held", 6.0)
            ]
        );
    }

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

    /// A restrained replay journals its limits and tradability as settings and its restraints as metrics.
    #[test]
    fn test_a_restrained_replay_journals_its_controls() {
        use crate::common::book::{Book, Cash};
        use crate::common::guard::Tradability;
        use crate::common::replay::Restraint;
        use crate::common::risk::Limits;
        use crate::common::time::calendar::{TradingCalendar, TradingSession};
        use crate::common::time::{SessionDate, SessionRange};
        let monday = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 9, 21).unwrap());
        let session = TradingSession::new(
            monday,
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        )
        .unwrap();
        let calendar =
            TradingCalendar::new(vec![session], SessionRange::new(monday, monday).unwrap())
                .unwrap();
        let limits = Limits::new(
            Cash::from_units(3),
            Cash::from_units(2),
            Cash::from_units(1),
            chrono::TimeDelta::minutes(15),
        )
        .unwrap();
        let tradability = BTreeMap::from([(Symbol::new("AAPL").unwrap(), Tradability::Untradable)]);
        let controls = Controls::Restrained(Restraint::new(limits, calendar, tradability));
        let fill_model =
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap();
        let opening = Opening::new(Cash::from_units(1)).unwrap();
        let settings: Vec<(&str, String)> =
            settings(fill_model, BarInterval::OneMinute, &controls, opening)
                .into_iter()
                .filter(|(setting, _)| {
                    !matches!(
                        setting,
                        ReplaySetting::DecisionInterval
                            | ReplaySetting::FillStyle
                            | ReplaySetting::OpeningCashUnits
                            | ReplaySetting::QuotedSpreadBasisPoints
                    )
                })
                .map(|(setting, value)| (<&str>::from(setting), value))
                .collect();
        assert_eq!(
            settings,
            [
                ("controls", "restrained".to_string()),
                ("gross_limit_units", "3".to_string()),
                ("per_name_limit_units", "2".to_string()),
                ("daily_loss_limit_units", "1".to_string()),
                ("flat_before_close_seconds", "900".to_string()),
                ("tradability", r#"{"AAPL":"untradable"}"#.to_string()),
            ]
        );
        let names: Vec<&str> = metrics(
            &Replay::open(Book::funded(Cash::from_units(1))),
            &controls,
            opening,
        )
        .into_iter()
        .map(|(metric, _)| <&str>::from(metric))
        .filter(|name| ["risk_cuts", "risk_refusals", "orders_held"].contains(name))
        .collect();
        assert_eq!(names, ["risk_cuts", "risk_refusals", "orders_held"]);
    }

    #[test]
    fn test_a_replay_setting_reads_back_from_the_name_it_is_journaled_under() {
        for name in [
            "decision_interval",
            "fill_style",
            "opening_cash_units",
            "quoted_spread_basis_points",
            "controls",
            "gross_limit_units",
            "per_name_limit_units",
            "daily_loss_limit_units",
            "flat_before_close_seconds",
            "tradability",
        ] {
            assert_eq!(name.parse::<ReplaySetting>().unwrap().to_string(), name);
        }
        assert!("FillStyle".parse::<ReplaySetting>().is_err());
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
                    | Observation::ObjectWritten(_)
                    | Observation::ObjectDeleted(_)
                    | Observation::HealFinished(_)
                    | Observation::ArchiveSurveyed(_)
                    | Observation::ViewsChecked(_)
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
                ("controls", "unrestrained"),
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
            Err(ReplayStudyError::ReservedParameter {
                setting: ReplaySetting::FillStyle
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
