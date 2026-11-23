//! Replays a candidate strategy and its baseline over the same sessions and fill model and journals them as one
//! experiment: the paired difference of their session returns with its checks and block errors, and the candidate
//! set beside the index over the same sessions, none of which a study can leave out.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use chrono::{DateTime, Utc};

use crate::common::book::{Cash, ValuationRefusal};
use crate::common::laboratory::blocks::{BlocksRefusal, Coarsening, Observations, coarsen};
use crate::common::laboratory::estimate::{
    Control, Estimate, EstimateRefusal, Treatment, flipped_share, paired, summarize,
};
use crate::common::laboratory::experiment::{ExperimentRefusal, Metric, Name, Outputs, Parameters};
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::laboratory::sharpe::{
    IndexReading, IndexUnread, Targets, beside_index, index_returns,
};
use crate::common::laboratory::trades::{Booked, BookingError, Borrows, Terms, Trade, book};
use crate::common::laboratory::verdict::{Cause, Checks, NullReading};
use crate::common::market::Symbol;
use crate::common::market::record::{BarInterval, Partition};
use crate::common::monoid::Monoid;
use crate::common::replay::{Controls, FillModel, Replay, Replayer};
use crate::common::strategy::Strategy;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;
use crate::laboratory::dataset::Dataset;
use crate::laboratory::replay::{Opening, ReplayStudyError, beside, metrics, run, settings};
use crate::laboratory::risk_free::RiskFree;
use crate::laboratory::{Study, StudyError};

/// The estimate a comparison's paired difference is journaled under.
const DIFFERENCE: &str = "session_return_difference";

/// The coarsening of the arms' difference in each name's gain at each decision, journaled beside the session difference.
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

/// One side of a comparison: what it runs (a strategy, or a list of trades) and the name that says what it is and
/// the settings that make it so.
pub struct Arm<S> {
    payload: S,
    name: Name,
}

impl<S> Arm<S> {
    /// Named `kind("setting"="value",...)`, each quoted and escaped, so two arms of one kind are told apart by their settings in the journal.
    pub fn new(payload: S, kind: &str, parameters: &Parameters) -> Result<Self, ExperimentRefusal> {
        let settings: Vec<String> = parameters
            .settings()
            .iter()
            .map(|(name, value)| format!("{:?}={value:?}", name.as_str()))
            .collect();
        Ok(Self {
            payload,
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

/// Both arms' runs and what the comparison journaled from them: `R` is a `Replay` when the arms are strategies and
/// a `Booked` when they are trade lists.
#[derive(Debug)]
pub struct Comparison<R> {
    candidate: R,
    baseline: R,
    difference: Estimate,
    checks: Checks,
    coarsening: Coarsening,
    index: IndexReading,
}

impl<R> Comparison<R> {
    pub fn candidate(&self) -> &R {
        &self.candidate
    }

    pub fn baseline(&self) -> &R {
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

    /// The difference in each traded name's gain over the opening cash at each decision, or in the book's when neither
    /// arm traded, its error read under each block grouping and quoted at the largest well-supported one.
    pub fn coarsening(&self) -> &Coarsening {
        &self.coarsening
    }

    /// The candidate's Sharpe beside the index's over the candidate's measured sessions, or why it could not be read.
    pub fn index(&self) -> IndexReading {
        self.index
    }

    /// The candidate against the Sharpe-over-index and daily-return targets, or why it was never set beside the index.
    pub fn against_targets(&self) -> Result<Targets, IndexUnread> {
        self.index.against_targets()
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
) -> Result<Comparison<Replay>, ReplayStudyError> {
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
        &Replayer::controlled(candidate.payload, fill_model, decision, controls.clone())
            .map_err(ReplayStudyError::Controls)?,
        opening,
    )?;
    let baseline_replay = run(
        dataset,
        &Replayer::controlled(baseline.payload, fill_model, decision, controls.clone())
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
    let by_name = name_differences(
        candidate_replay.name_marks(),
        baseline_replay.name_marks(),
        opening.cash(),
    );
    let observations = match by_name.is_empty() {
        true => Observations::book(decision_differences(
            candidate_replay.marks(),
            baseline_replay.marks(),
            opening.cash(),
        )),
        false => Observations::new(by_name),
    }
    .map_err(ReplayStudyError::Blocks)?;
    let coarsening = coarsen(&observations).map_err(ReplayStudyError::Blocks)?;
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
    let lineages =
        match read.origin() == benchmark_read.origin() && read.run() == benchmark_read.run() {
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

/// Why a comparison's checks could not be assembled.
#[derive(Debug, thiserror::Error)]
pub enum CheckRefusal {
    #[error("the difference series was refused: {0}")]
    Series(SeriesRefusal),
    #[error("the checks were refused: {0}")]
    Experiment(ExperimentRefusal),
}

impl From<CheckRefusal> for ReplayStudyError {
    fn from(refusal: CheckRefusal) -> Self {
        match refusal {
            CheckRefusal::Series(refusal) => Self::Series(refusal),
            CheckRefusal::Experiment(refusal) => Self::Experiment(refusal),
        }
    }
}

/// The difference's sessions as its population, those it could not read as undefined, and the share of sign flips of
/// its session readings at least as far from zero as its own, unmeasured with its cause below two readings.
fn checks(
    candidate: &Series,
    baseline: &Series,
    difference: Estimate,
) -> Result<Checks, CheckRefusal> {
    let differences = candidate
        .zip_with(baseline, |candidate, baseline| candidate - baseline)
        .map_err(CheckRefusal::Series)?;
    let null = match flipped_share(&differences, NULL_SEED, NULL_PERMUTATIONS) {
        Ok(share) => {
            NullReading::Measured(Metric::new(share).expect("a share of permutations is finite"))
        }
        Err(refusal) => NullReading::Unmeasured {
            cause: Cause::new(refusal.to_string()).map_err(CheckRefusal::Experiment)?,
        },
    };
    Checks::new(
        difference.sessions() + difference.undefined(),
        difference.undefined(),
        null,
    )
    .map_err(CheckRefusal::Experiment)
}

/// The coarsening a trade comparison's paired gains are journaled under.
const TRADE_DIFFERENCE: &str = "trade_gain_difference";

/// Why a trade comparison was not read.
#[derive(Debug, thiserror::Error)]
pub enum CompareTradesError {
    #[error("a trade comparison needs at least one session to read")]
    NoSessions,
    #[error("the {arm} arm could not be booked: {cause}")]
    Booking {
        arm: &'static str,
        cause: BookingError,
    },
    #[error("neither arm took a trade, so there is nothing to coarsen")]
    NothingTaken,
    #[error("the returns were refused: {0}")]
    Series(SeriesRefusal),
    #[error("the estimate was refused: {0}")]
    Estimate(EstimateRefusal),
    #[error("the block errors were refused: {0}")]
    Blocks(BlocksRefusal),
    #[error("{0}")]
    Checks(#[from] CheckRefusal),
    #[error("the experiment was refused: {0}")]
    Experiment(ExperimentRefusal),
    #[error("{0}")]
    Study(StudyError),
}

/// Candidate less baseline in each trade's gain over the opening, at every instant and name either arm entered; an
/// arm without a trade there gained nothing, since a trade arm trades its list and nothing else.
fn trade_differences(
    candidate: &Booked,
    baseline: &Booked,
) -> Vec<(DateTime<Utc>, Symbol, Option<f64>)> {
    let keyed = |booked: &Booked| {
        let mut gains: BTreeMap<(DateTime<Utc>, Symbol), f64> = BTreeMap::new();
        for (at, symbol, gain) in booked.gains() {
            if let Some(gain) = gain {
                *gains.entry((at, symbol)).or_insert(0.0) += gain;
            }
        }
        gains
    };
    let (candidate, baseline) = (keyed(candidate), keyed(baseline));
    let keys: BTreeSet<(DateTime<Utc>, Symbol)> =
        candidate.keys().chain(baseline.keys()).cloned().collect();
    keys.into_iter()
        .map(|(at, symbol)| {
            let gain = |gains: &BTreeMap<(DateTime<Utc>, Symbol), f64>| {
                gains.get(&(at, symbol.clone())).copied().unwrap_or(0.0)
            };
            let difference = gain(&candidate) - gain(&baseline);
            (at, symbol, Some(difference))
        })
        .collect()
}

/// Books both trade lists through the one account `terms` describes, so the arms differ only in their trades, and
/// journals them as one experiment naming both arms, the borrow provenance and the terms, with the candidate set
/// beside `benchmark`. Both arms close every one of `sessions`, the study's own population, so a session neither
/// arm traded still reads as a session of nothing and the paired difference reads on one series.
#[expect(
    clippy::too_many_arguments,
    reason = "each is one condition both arms share, named at the call so none is mistaken for another"
)]
pub fn compare_trades(
    study: &mut Study,
    candidate: Arm<Vec<Trade>>,
    baseline: Arm<Vec<Trade>>,
    terms: &Terms,
    calendar: &TradingCalendar,
    borrows: &Borrows,
    sessions: &BTreeSet<SessionDate>,
    benchmark: &Benchmark,
) -> Result<Comparison<Booked>, CompareTradesError> {
    if sessions.is_empty() {
        return Err(CompareTradesError::NoSessions);
    }
    let parameters = Parameters::new(
        [
            (
                <&str>::from(ArmSide::Candidate),
                candidate.name.as_str().to_string(),
            ),
            (
                <&str>::from(ArmSide::Baseline),
                baseline.name.as_str().to_string(),
            ),
            (INDEX, benchmark.symbol.as_str().to_string()),
            ("borrow", borrows.provenance().flag()),
        ]
        .into_iter()
        .chain(
            benchmark
                .risk_free
                .settings()
                .iter()
                .map(|(name, value)| (name.as_str(), value.clone())),
        )
        .chain(terms.settings()),
    )
    .map_err(CompareTradesError::Experiment)?;
    let booked = |arm: Arm<Vec<Trade>>, side: ArmSide| {
        book(terms, calendar, borrows, sessions, arm.payload).map_err(|cause| {
            CompareTradesError::Booking {
                arm: side.into(),
                cause,
            }
        })
    };
    let candidate_booked = booked(candidate, ArmSide::Candidate)?;
    let baseline_booked = booked(baseline, ArmSide::Baseline)?;
    let returns = |booked: &Booked| booked.returns().map_err(CompareTradesError::Series);
    let (candidate_returns, baseline_returns) =
        (returns(&candidate_booked)?, returns(&baseline_booked)?);
    let difference = paired(Treatment(&candidate_returns), Control(&baseline_returns))
        .map_err(CompareTradesError::Estimate)?;
    let checks = checks(&candidate_returns, &baseline_returns, difference)?;
    let differences = trade_differences(&candidate_booked, &baseline_booked);
    if differences.is_empty() {
        return Err(CompareTradesError::NothingTaken);
    }
    let observations = Observations::new(differences).map_err(CompareTradesError::Blocks)?;
    let coarsening = coarsen(&observations).map_err(CompareTradesError::Blocks)?;
    let index = IndexReading::from(beside_index(
        &candidate_returns,
        &benchmark.returns,
        benchmark.risk_free.rates(),
    ));
    let mut outputs = Outputs::default()
        .estimate(DIFFERENCE, difference)
        .map_err(CompareTradesError::Experiment)?;
    for (side, booked, returns) in [
        (ArmSide::Candidate, &candidate_booked, &candidate_returns),
        (ArmSide::Baseline, &baseline_booked, &baseline_returns),
    ] {
        let own =
            Estimate::from_summary(summarize(returns)).map_err(CompareTradesError::Estimate)?;
        outputs = outputs
            .estimate(format!("{side}_session_return"), own)
            .map_err(CompareTradesError::Experiment)?;
        let (taken, trimmed, refused) = booked.counts();
        for (metric, value) in [
            ("trades_taken", taken as f64),
            ("trades_trimmed", trimmed as f64),
            ("trades_refused", refused as f64),
            ("net_return", booked.net_return()),
        ] {
            outputs = outputs
                .metric(format!("{side}_{metric}"), value)
                .map_err(CompareTradesError::Experiment)?;
        }
    }
    let outputs = outputs
        .checks(checks.clone())
        .and_then(|outputs| outputs.coarsening(TRADE_DIFFERENCE, coarsening.clone()))
        .and_then(|outputs| outputs.index(index))
        .map_err(CompareTradesError::Experiment)?;
    study
        .experiment(parameters, &[benchmark.dataset.lineage()], outputs)
        .map_err(CompareTradesError::Study)?;
    Ok(Comparison {
        candidate: candidate_booked,
        baseline: baseline_booked,
        difference,
        checks,
        coarsening,
        index,
    })
}

/// Each arm's gain at each decision mark over `opening`, from the mark before it or `start` before the first.
fn gains(
    marks: impl Iterator<Item = (DateTime<Utc>, Option<Cash>)>,
    start: Cash,
    opening: Cash,
) -> BTreeMap<DateTime<Utc>, Option<f64>> {
    let mut previous = Some(start);
    marks
        .map(|(at, mark)| {
            // Taken in exact units first, so only the ratio rounds.
            let gain = previous.zip(mark).map(|(previous, mark)| {
                (mark.units() - previous.units()) as f64 / opening.units() as f64
            });
            previous = mark;
            (at, gain)
        })
        .collect()
}

/// Candidate less baseline at every instant either arm marked; `None` where only one did or either gain is.
fn paired_gains(
    candidate: &BTreeMap<DateTime<Utc>, Option<f64>>,
    baseline: &BTreeMap<DateTime<Utc>, Option<f64>>,
) -> impl Iterator<Item = (DateTime<Utc>, Option<f64>)> {
    let instants: BTreeSet<DateTime<Utc>> =
        candidate.keys().chain(baseline.keys()).copied().collect();
    instants.into_iter().map(|at| {
        let gain = |gains: &BTreeMap<DateTime<Utc>, Option<f64>>| gains.get(&at).copied().flatten();
        (
            at,
            gain(candidate)
                .zip(gain(baseline))
                .map(|(candidate, baseline)| candidate - baseline),
        )
    })
}

/// Each arm's book gain at each decision mark over the opening cash, candidate less baseline; `None` where either
/// arm's mark, or the one before it, was unpriced or absent.
fn decision_differences(
    candidate: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>,
    baseline: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>,
    opening: Cash,
) -> Vec<(DateTime<Utc>, Option<f64>)> {
    let book = |marks: &BTreeMap<DateTime<Utc>, Result<Cash, ValuationRefusal>>| {
        gains(
            marks
                .iter()
                .map(|(at, mark)| (*at, mark.as_ref().ok().copied())),
            opening,
            opening,
        )
    };
    paired_gains(&book(candidate), &book(baseline)).collect()
}

/// Each name either arm traded, its gain at each decision mark over the opening cash, candidate less baseline; a
/// name is worth nothing to an arm until it trades it, since a comparison opens on cash alone.
fn name_differences(
    candidate: &BTreeMap<DateTime<Utc>, BTreeMap<Symbol, Result<Cash, ValuationRefusal>>>,
    baseline: &BTreeMap<DateTime<Utc>, BTreeMap<Symbol, Result<Cash, ValuationRefusal>>>,
    opening: Cash,
) -> Vec<(DateTime<Utc>, Symbol, Option<f64>)> {
    let names: BTreeSet<&Symbol> = candidate
        .values()
        .chain(baseline.values())
        .flat_map(BTreeMap::keys)
        .collect();
    let name =
        |marks: &BTreeMap<DateTime<Utc>, BTreeMap<Symbol, Result<Cash, ValuationRefusal>>>,
         symbol: &Symbol| {
            gains(
                marks.iter().map(|(at, names)| {
                    let mark = match names.get(symbol) {
                        Some(Ok(mark)) => Some(*mark),
                        Some(Err(_)) => None,
                        None => Some(Cash::empty()),
                    };
                    (*at, mark)
                }),
                Cash::empty(),
                opening,
            )
        };
    names
        .into_iter()
        .flat_map(|symbol| {
            paired_gains(&name(candidate, symbol), &name(baseline, symbol))
                .map(|(at, gain)| (at, symbol.clone(), gain))
                .collect::<Vec<_>>()
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

    use strum::IntoEnumIterator;

    use super::*;
    use crate::common::book::{Book, Cash};
    use crate::common::cost::{BasisPoints, FillStyle};
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::blocks::{Grouping, GroupingRefusal};
    use crate::common::laboratory::experiment::{ExperimentRan, Label};
    use crate::common::laboratory::sharpe::{BesideIndexRefusal, SharpeRefusal, Verdict};
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
        let mut ran = experiments(directory);
        assert_eq!(ran.len(), 1);
        ran.remove(0)
    }

    /// Every `experiment_ran` the study in `directory` journaled.
    fn experiments(directory: &std::path::Path) -> Vec<ExperimentRan> {
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
                        | Observation::BookFlattened(_)
                        | Observation::SessionClosed(_)
                        | Observation::PlaybookRead(_)
                        | Observation::PlaybookUploaded(_) => {}
                    },
                    ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
                }
            }
        }
        ran
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
            [
                dataset.fingerprint().unwrap().clone(),
                index.fingerprint().unwrap().clone()
            ]
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
        let targets = comparison.against_targets().unwrap();
        let (margin, daily_return) = (targets.sharpe_margin(), targets.daily_return());
        assert_eq!((margin.target(), daily_return.target()), (2.0, 8.0));
        assert!(
            (margin.reading() - 15.132372936280528).abs() < 1e-9,
            "{targets:?}"
        );
        // Each session's gain over the equity before it, from $100 with the $10 share and its 5bp entry.
        assert!(
            (daily_return.reading() - 9.400663127295928).abs() < 1e-9,
            "{targets:?}"
        );
        assert_eq!(
            (margin.verdict(), daily_return.verdict()),
            (Verdict::Clears, Verdict::Clears)
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
        assert_eq!(ran.fingerprints(), [dataset.fingerprint().unwrap().clone()]);
        assert_eq!(
            comparison.index(),
            IndexReading::Refused {
                cause: BesideIndexRefusal::IndexMissing {
                    session: session(0)
                }
            }
        );
        assert_eq!(
            comparison.against_targets(),
            Err(IndexUnread {
                cause: BesideIndexRefusal::IndexMissing {
                    session: session(0)
                }
            })
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

    /// Wants one share of each of its names, whatever it has seen.
    #[derive(Clone)]
    struct OneEach(Vec<Symbol>);

    impl Strategy for OneEach {
        fn decide(&self, _: &MarketState, _: &Book) -> Target {
            Target::new(
                self.0
                    .iter()
                    .map(|symbol| (symbol.clone(), Shares::whole(1).unwrap()))
                    .collect::<BTreeMap<_, _>>(),
            )
        }
    }

    /// One share of each of 24 names held over 30 sessions against a flat baseline, each name moving $1 a session up
    /// or down: by name alone when `persistent`, so each name drifts one way throughout, or by name and session
    /// together otherwise; half the names rise in every session either way. Returns the journaled coarsening and the
    /// book-level one over the same replays.
    fn name_shocked(persistent: bool) -> (Coarsening, Coarsening) {
        let (mut study, directory) = study();
        let names: Vec<String> = (0..24u8)
            .map(|name| format!("A{}", (b'A' + name) as char))
            .collect();
        let closes: Vec<(&str, Vec<f64>)> = names
            .iter()
            .enumerate()
            .map(|(name, symbol)| {
                let mut closes = vec![100.0];
                for session in 1..30 {
                    let rises = match persistent {
                        true => name % 2 == 0,
                        false => (name + session) % 2 == 0,
                    };
                    closes.push(closes[session - 1] + if rises { 1.0 } else { -1.0 });
                }
                (symbol.as_str(), closes)
            })
            .collect();
        let dataset = daily_dataset(study.run_id(), 0, &closes);
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("AA").unwrap(),
            &calendar_between(0, 29),
            &risk_free,
        )
        .unwrap();
        let opening = Opening::new(Cash::from_units(10_000 * 1_000_000_000_000)).unwrap();
        let comparison = compare(
            &mut study,
            &dataset,
            Arm::new(
                OneEach(
                    names
                        .iter()
                        .map(|name| Symbol::new(name).unwrap())
                        .collect(),
                ),
                "one_each",
                &Parameters::default(),
            )
            .unwrap(),
            Arm::new(Flat, "flat", &Parameters::default()).unwrap(),
            fill_model(),
            BarInterval::OneDay,
            opening,
            Controls::Unrestrained,
            &benchmark,
        )
        .unwrap();
        let ran = journaled(&directory);
        std::fs::remove_dir_all(&directory).unwrap();
        let book = coarsen(
            &Observations::book(decision_differences(
                comparison.candidate().marks(),
                comparison.baseline().marks(),
                opening.cash(),
            ))
            .unwrap(),
        )
        .unwrap();
        (ran.coarsenings()[DECISION_DIFFERENCE].clone(), book)
    }

    /// A shock that follows each name across sessions shows only in the name groupings: it widens the name error well
    /// past the same names moving by name and session together, and the journaled coarsening quotes the name
    /// grouping, where the unshocked one quotes name within session.
    #[test]
    fn test_a_name_shock_widens_the_name_error_and_is_quoted() {
        let (shocked, _) = name_shocked(true);
        let (control, _) = name_shocked(false);
        for coarsening in [&shocked, &control] {
            assert_eq!((coarsening.measured(), coarsening.undefined()), (720, 0));
            assert!(coarsening.groupings().keys().copied().eq(Grouping::iter()));
            let blocks: Vec<_> = coarsening
                .groupings()
                .values()
                .map(|reading| reading.as_ref().map(|clustered| clustered.blocks()).ok())
                .collect();
            assert_eq!(
                blocks,
                [
                    Some(30),
                    Some(30),
                    Some(30),
                    Some(30),
                    Some(5),
                    Some(24),
                    Some(720),
                    Some(24)
                ]
            );
        }
        let name_error = |coarsening: &Coarsening| {
            coarsening.groupings()[&Grouping::Name]
                .as_ref()
                .unwrap()
                .standard_error()
        };
        assert!(
            name_error(&shocked) > 10.0 * name_error(&control),
            "{} against {}",
            name_error(&shocked),
            name_error(&control)
        );
        let quoted = |coarsening: &Coarsening| {
            let quoted = coarsening.quoted().unwrap();
            (quoted.grouping(), quoted.blocks())
        };
        assert_eq!(quoted(&shocked), (Grouping::Name, 24));
        assert_eq!(quoted(&control), (Grouping::NameSession, 720));
        assert_eq!(
            shocked.quoted().unwrap().standard_error(),
            name_error(&shocked)
        );
    }

    /// With every traded name read at every decision, each time grouping's error over the names is the book's own
    /// over its 24 names, since a time block's name residuals sum to the book's residual.
    #[test]
    fn test_time_groupings_over_names_are_the_books_over_the_name_count() {
        for persistent in [true, false] {
            let (names, book) = name_shocked(persistent);
            assert_eq!(book.measured(), 30);
            assert!((names.mean() * 24.0 - book.mean()).abs() < 1e-15);
            for grouping in [
                Grouping::Minute,
                Grouping::FiveMinutes,
                Grouping::Hour,
                Grouping::Session,
                Grouping::Week,
            ] {
                let error = |coarsening: &Coarsening| {
                    coarsening.groupings()[&grouping]
                        .as_ref()
                        .unwrap()
                        .standard_error()
                };
                assert!(
                    (error(&names) * 24.0 - error(&book)).abs() <= 1e-12 * error(&book).max(1e-12),
                    "{grouping}: {} against {}",
                    error(&names) * 24.0,
                    error(&book)
                );
            }
        }
    }

    /// A name only the candidate traded gains nothing for the baseline, an unpriced name mark leaves that name's gain
    /// and the next undefined while the other name reads on, and a name is worth nothing before its first mark.
    #[test]
    fn test_name_differences_read_each_name_from_its_own_previous_mark() {
        let at = |minute: u32| {
            chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 3, 2, 15, minute, 0).unwrap()
        };
        let (apple, microsoft) = (Symbol::new("AAPL").unwrap(), Symbol::new("MSFT").unwrap());
        let cash = |dollars: i128| Ok(Cash::from_units(dollars * 1_000));
        let unpriced = || {
            Err(ValuationRefusal::Unpriced {
                symbol: Symbol::new("AAPL").unwrap(),
            })
        };
        let candidate = BTreeMap::from([
            (at(1), BTreeMap::new()),
            (
                at(2),
                BTreeMap::from([(apple.clone(), cash(2)), (microsoft.clone(), cash(-1))]),
            ),
            (
                at(3),
                BTreeMap::from([(apple.clone(), unpriced()), (microsoft.clone(), cash(3))]),
            ),
            (
                at(4),
                BTreeMap::from([(apple.clone(), cash(5)), (microsoft.clone(), cash(4))]),
            ),
            (
                at(5),
                BTreeMap::from([(apple.clone(), cash(9)), (microsoft.clone(), cash(4))]),
            ),
        ]);
        let baseline = BTreeMap::from([
            (at(1), BTreeMap::new()),
            (at(2), BTreeMap::new()),
            (at(3), BTreeMap::from([(microsoft.clone(), cash(1))])),
            (at(4), BTreeMap::from([(microsoft.clone(), cash(1))])),
            (at(5), BTreeMap::from([(microsoft.clone(), cash(2))])),
        ]);
        let differences = name_differences(&candidate, &baseline, Cash::from_units(100_000));
        let keys: Vec<_> = differences
            .iter()
            .map(|(at, symbol, _)| (*at, symbol.as_str()))
            .collect();
        let expected_keys: Vec<_> = ["AAPL", "MSFT"]
            .into_iter()
            .flat_map(|symbol| (1..=5).map(move |minute| (at(minute), symbol)))
            .collect();
        assert_eq!(keys, expected_keys);
        let readings: Vec<_> = differences.iter().map(|(.., reading)| *reading).collect();
        // AAPL: nothing, 2%, unpriced, after unpriced, 4%; MSFT: nothing, -1%, 4% less 1%, 1% less 0%, 0% less 1%.
        let expected = [
            Some(0.0),
            Some(0.02),
            None,
            None,
            Some(0.04),
            Some(0.0),
            Some(-0.01),
            Some(0.03),
            Some(0.01),
            Some(-0.01),
        ];
        assert_eq!(readings.len(), expected.len());
        for (reading, expected) in readings.iter().zip(expected) {
            match (reading, expected) {
                (Some(reading), Some(expected)) => {
                    assert!((reading - expected).abs() < 1e-15, "{readings:?}")
                }
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => panic!("{readings:?}"),
            }
        }
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

    use crate::common::laboratory::entry::{
        Entry as EventEntry, EventRow, ExitUnread, UnknownCause, enter,
    };
    use crate::common::laboratory::ledger::Leverage;
    use crate::common::laboratory::trades::BorrowProvenance;
    use crate::common::market::borrow::Borrow;
    use crate::common::market::quote_bars::{Spread, StandingQuote};
    use crate::common::market::{Price, SHARE_SCALE};

    /// An instant `minutes` past 10:00 Eastern on the dataset fixtures' session `day`, all in early March, so no
    /// daylight transition sits between midnight and the open.
    fn trade_at(day: i64, minutes: i64) -> DateTime<Utc> {
        crate::laboratory::dataset::tests::session(day).midnight()
            + chrono::TimeDelta::hours(10)
            + chrono::TimeDelta::minutes(minutes)
    }

    /// A crossed round trip of `whole` shares at a $49.99 by $50.01 touch, exiting at `exit` dollars 5 minutes in.
    fn round_trip(name: &str, whole: f64, exit: f64, day: i64, minutes: i64) -> Trade {
        let at = trade_at(day, minutes);
        let price = |dollars: f64| Price::from_ticks((dollars * 1e6).round() as i64).unwrap();
        let size = Shares::whole(1_000).unwrap();
        let touch = StandingQuote::new(
            at - chrono::TimeDelta::seconds(1),
            price(49.99),
            price(50.01),
            size,
            size,
        )
        .unwrap();
        let row = EventRow::new(
            Symbol::new(name).unwrap(),
            at,
            touch,
            Spread::from_ticks(10_000),
            Err(ExitUnread::PastLastRead),
            Err(UnknownCause::WindowPastLastRead),
        )
        .unwrap();
        let entered = enter(&row, crate::common::book::Side::Buy, EventEntry::Cross).unwrap();
        Trade::new(
            Symbol::new(name).unwrap(),
            crate::common::book::Side::Buy,
            Shares::from_units((whole * SHARE_SCALE as f64).round() as u64),
            at,
            entered,
            at + chrono::TimeDelta::minutes(5),
            price(exit),
        )
        .unwrap()
    }

    fn terms() -> Terms {
        let dollar = 1_000_000_000_000;
        let limits = crate::common::risk::Limits::new(
            Cash::from_units(40_000 * dollar),
            Cash::from_units(5_000 * dollar),
            Cash::from_units(100_000 * dollar),
            Cash::from_units(100_000 * dollar),
            chrono::TimeDelta::minutes(10),
        )
        .unwrap();
        Terms::new(
            Cash::from_units(20_000 * dollar),
            limits,
            Leverage::new(std::num::NonZeroU8::new(2).unwrap()),
            crate::common::cost::fees::FeeRegime::CommissionFree {
                regulatory: crate::common::cost::fees::RegulatoryFees::new(
                    crate::common::market::Dollars::from_millionths(0),
                    crate::common::cost::PerShare::ZERO,
                )
                .unwrap(),
            },
        )
    }

    fn snapshot_borrows() -> Borrows {
        Borrows::new(
            BorrowProvenance::Snapshot {
                fetched: crate::laboratory::dataset::tests::session(0),
            },
            BTreeMap::from([(Symbol::new("AAA").unwrap(), Borrow::Available)]),
        )
    }

    fn trade_arm(name: &str, trades: Vec<Trade>) -> Arm<Vec<Trade>> {
        Arm::new(trades, name, &Parameters::default()).unwrap()
    }

    fn fit_sessions() -> BTreeSet<SessionDate> {
        (0..3)
            .map(crate::laboratory::dataset::tests::session)
            .collect()
    }

    /// A $45 candidate round trip against a $5 baseline one on the first of three sessions reads a difference of
    /// exactly 0.002 on the $20,000 opening over the one traded session, journaled with both arms' names, the
    /// borrow flag and the account terms.
    #[test]
    fn test_a_trade_comparison_is_journaled_with_the_borrow_flag() {
        let (mut study, directory) = study();
        let closes: Vec<f64> = (0..3).map(|day| 100.0 + day as f64).collect();
        let dataset = daily_dataset(study.run_id(), 0, &[("SPY", closes)]);
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("SPY").unwrap(),
            &calendar_between(0, 2),
            &risk_free,
        )
        .unwrap();
        let comparison = compare_trades(
            &mut study,
            trade_arm(
                "candidate_trades",
                vec![round_trip("AAA", 90.0, 50.51, 0, 0)],
            ),
            trade_arm(
                "baseline_trades",
                vec![round_trip("BBB", 10.0, 50.51, 0, 10)],
            ),
            &terms(),
            &calendar_between(0, 2),
            &snapshot_borrows(),
            &fit_sessions(),
            &benchmark,
        )
        .unwrap();
        let ran = journaled(&directory);
        std::fs::remove_dir_all(&directory).unwrap();
        let difference = comparison.difference();
        assert!(
            (difference.mean() - 0.002 / 3.0).abs() < 1e-15,
            "{difference:?}"
        );
        assert_eq!((difference.sessions(), difference.undefined()), (3, 0));
        let settings: BTreeMap<&str, &str> = ran
            .parameters()
            .settings()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(settings["candidate"], "candidate_trades()");
        assert_eq!(settings["baseline"], "baseline_trades()");
        assert_eq!(
            settings["borrow"],
            "snapshot(fetched=2026-03-02, not point in time)"
        );
        assert_eq!(settings["opening_cash_units"], "20000000000000000");
        assert_eq!(settings["leverage"], "2");
        assert_eq!(settings["gross_limit_units"], "40000000000000000");
        assert_eq!(settings["per_name_limit_units"], "5000000000000000");
        assert_eq!(settings["daily_loss_limit_units"], "100000000000000000");
        assert_eq!(settings["per_name_loss_limit_units"], "100000000000000000");
        assert_eq!(
            settings["flat_before_close"],
            "TimeDelta { secs: 600, nanos: 0 }"
        );
        assert_eq!(settings["index"], "SPY");
        assert!(settings.contains_key("fees"));
        let metrics: BTreeMap<&str, f64> = ran
            .metrics()
            .iter()
            .map(|(name, value)| (name.as_str(), value.value()))
            .collect();
        assert_eq!(metrics["candidate_trades_taken"], 1.0);
        assert_eq!(metrics["baseline_trades_taken"], 1.0);
        assert_eq!(metrics["candidate_trades_refused"], 0.0);
        assert!((metrics["candidate_net_return"] - 45.0 / 20_000.0).abs() < 1e-15);
        // The candidate's $45 gain at its entry and the baseline's $5 at its own, each paired against nothing.
        let coarsening = &ran.coarsenings()[TRADE_DIFFERENCE];
        assert_eq!((coarsening.measured(), coarsening.undefined()), (2, 0));
        assert!((coarsening.mean() - (0.00225 - 0.00025) / 2.0).abs() < 1e-15);
        assert_eq!(ran.fingerprints(), [dataset.fingerprint().unwrap().clone()]);
    }

    /// A trade arm against itself differs by nothing in every session and at every trade.
    #[test]
    fn test_a_trade_arm_against_itself_differs_by_nothing() {
        let (mut study, directory) = study();
        let closes: Vec<f64> = (0..3).map(|day| 100.0 + day as f64).collect();
        let dataset = daily_dataset(study.run_id(), 0, &[("SPY", closes)]);
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("SPY").unwrap(),
            &calendar_between(0, 2),
            &risk_free,
        )
        .unwrap();
        let trades = || {
            vec![
                round_trip("AAA", 50.0, 50.51, 0, 0),
                round_trip("AAA", 20.0, 49.51, 2, 0),
            ]
        };
        let comparison = compare_trades(
            &mut study,
            trade_arm("same", trades()),
            trade_arm("same", trades()),
            &terms(),
            &calendar_between(0, 2),
            &snapshot_borrows(),
            &fit_sessions(),
            &benchmark,
        )
        .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        let difference = comparison.difference();
        // Three sessions close: the two traded ones and the eventless one between them.
        assert_eq!(
            (
                difference.mean(),
                difference.standard_error(),
                difference.sessions()
            ),
            (0.0, 0.0, 3)
        );
        assert_eq!(comparison.coarsening().mean(), 0.0);
        assert_eq!(comparison.candidate(), comparison.baseline());
    }

    /// Two empty arms have no session to read, and two arms whose every trade is refused have nothing to coarsen;
    /// neither journals an experiment.
    #[test]
    fn test_trade_arms_with_nothing_to_read_are_refused() {
        let (mut study, directory) = study();
        let closes: Vec<f64> = (0..3).map(|day| 100.0 + day as f64).collect();
        let dataset = daily_dataset(study.run_id(), 0, &[("SPY", closes)]);
        let risk_free = zero_rate(&sessions(&dataset));
        let benchmark = Benchmark::new(
            &dataset,
            Symbol::new("SPY").unwrap(),
            &calendar_between(0, 2),
            &risk_free,
        )
        .unwrap();
        assert!(matches!(
            compare_trades(
                &mut study,
                trade_arm("candidate_trades", vec![]),
                trade_arm("baseline_trades", vec![]),
                &terms(),
                &calendar_between(0, 2),
                &snapshot_borrows(),
                &BTreeSet::new(),
                &benchmark,
            ),
            Err(CompareTradesError::NoSessions)
        ));
        // A half-share short opens nothing, so both arms book but neither takes a trade.
        let refused = || {
            let built = round_trip("AAA", 90.0, 50.51, 0, 0);
            vec![
                Trade::new(
                    built.symbol().clone(),
                    crate::common::book::Side::Sell,
                    Shares::from_units(SHARE_SCALE / 2),
                    built.entered_at(),
                    built.entered(),
                    built.exited_at(),
                    built.exit(),
                )
                .unwrap(),
            ]
        };
        assert!(matches!(
            compare_trades(
                &mut study,
                trade_arm("candidate_trades", refused()),
                trade_arm("baseline_trades", refused()),
                &terms(),
                &calendar_between(0, 2),
                &snapshot_borrows(),
                &fit_sessions(),
                &benchmark,
            ),
            Err(CompareTradesError::NothingTaken)
        ));
        assert!(experiments(&directory).is_empty());
        std::fs::remove_dir_all(&directory).unwrap();
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
