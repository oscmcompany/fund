//! Trains several models and scores them against the baselines, over one universe of names.
//!
//! Publishes nothing: the question is what the model orders, not what it should trade.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use burn::module::AutodiffModule;
use burn::tensor::backend::Backend;
use chrono::Utc;
use polars::prelude::*;
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use tracing::{error, info, warn};

use fund::common::log::init_tracing;
use fund::common::types::SessionDate;
use fund::laboratory::journal as laboratory;
use fund::laboratory::metrics::{self, Distribution};
use fund::laboratory::predictor::{
    evaluate, CrossSectionalMean, Evaluation, Momentum, Panel, Persistence, Predictor,
    RandomRanking,
};
use fund::laboratory::{dataset, forecast};
use fund::models::tide::configuration::ModelParameters;
use fund::models::tide::data::{
    input_feature_size, DatasetKind, FeatureMappings, Target, TrainingDataset, TrainingFraction,
    STATIC_CATEGORICAL_COLUMNS,
};
use fund::models::tide::model::TiDEModel;
use fund::models::tide::train::{train, TrainBackend, TrainConfiguration};
use fund::models::tide::TideError;

const USAGE: &str =
    "Usage: laboratory_tide [LOOKBACK_DAYS] [EPOCHS] [SEED] [TARGET] [SEED_COUNT]\n\
     TARGET is raw (default) or demeaned";

const INPUT_LENGTH: usize = 35;
const OUTPUT_LENGTH: usize = 1;
const TRAINING_FRACTION: f64 = 0.8;

/// The trainer's own window, so the model being scored is the model the trainer would publish.
const DEFAULT_LOOKBACK_DAYS: i64 = 365;

/// Fewer than the trainer runs. The question is what the model orders, not its best score, and a
/// rehearsal that takes an hour is one nobody repeats.
const DEFAULT_EPOCHS: i64 = 5;

const MOMENTUM_SESSIONS: usize = 20;
const RANDOM_SEED: u64 = 0x5EED;

/// Seeds the weight initialiser, so two runs of one configuration produce one model.
///
/// `train` already seeds its own batch shuffle; the weights go through the backend's global
/// generator, and initialisation alone moves the reported coefficient by more than the tradeable
/// threshold. This names the first of [`Parameters::seed_count`] consecutive seeds.
const DEFAULT_TRAINING_SEED: i64 = 0x7A1D;

/// How many seeds one invocation trains, so the run reports a spread rather than a point.
///
/// Each seed costs a full fit of both arms, so this is the smallest count that has a spread at all
/// plus one. A single seed reports a number with no error bar on the thing that is being varied.
const DEFAULT_SEED_COUNT: i64 = 3;

/// Mixed into the training seed to draw the permutation, so the null's shuffle and the null model's
/// weights are not the same draw.
const PERMUTATION_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Below this the seeds produced one model rather than several.
///
/// A tight spread is a warning and never a quality signal: it means initialisation did not reach the
/// weights, so the single number the run reports has no error bar behind it after all.
const SEED_DISPERSION_FLOOR: f64 = 1e-9;

struct Parameters {
    lookback_days: i64,
    epochs: usize,
    seed: u64,
    target: Target,
    seed_count: usize,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (lookback_days, epochs, seed, seed_count) = match arguments {
            [] => (
                DEFAULT_LOOKBACK_DAYS,
                DEFAULT_EPOCHS,
                DEFAULT_TRAINING_SEED,
                DEFAULT_SEED_COUNT,
            ),
            [lookback] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                DEFAULT_EPOCHS,
                DEFAULT_TRAINING_SEED,
                DEFAULT_SEED_COUNT,
            ),
            [lookback, epochs] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(epochs, "EPOCHS")?,
                DEFAULT_TRAINING_SEED,
                DEFAULT_SEED_COUNT,
            ),
            [lookback, epochs, seed] | [lookback, epochs, seed, _] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(epochs, "EPOCHS")?,
                positive(seed, "SEED")?,
                DEFAULT_SEED_COUNT,
            ),
            [lookback, epochs, seed, _, seed_count] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(epochs, "EPOCHS")?,
                positive(seed, "SEED")?,
                positive(seed_count, "SEED_COUNT")?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };
        let target = match arguments {
            [_, _, _, target] | [_, _, _, target, _] => match target.trim() {
                "raw" => Target::Raw,
                "demeaned" => Target::CrossSectionallyDemeaned,
                other => {
                    return Err(format!(
                        "TARGET must be raw or demeaned, got {other:?}\n{USAGE}"
                    ));
                }
            },
            _ => Target::Raw,
        };
        Ok(Self {
            lookback_days,
            epochs: usize::try_from(epochs)
                .map_err(|_| format!("EPOCHS is larger than this platform can index\n{USAGE}"))?,
            seed: seed as u64,
            target,
            seed_count: usize::try_from(seed_count).map_err(|_| {
                format!("SEED_COUNT is larger than this platform can index\n{USAGE}")
            })?,
        })
    }
}

fn positive(raw: &str, name: &str) -> Result<i64, String> {
    let value: i64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("{name} must be a positive integer, got {raw:?}\n{USAGE}"))?;
    if value <= 0 {
        return Err(format!(
            "{name} must be greater than zero, got {value}\n{USAGE}"
        ));
    }
    Ok(value)
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing("laboratory-tide.log", Some("info"), "laboratory-tide");

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let parameters = match Parameters::parse(&arguments) {
        Ok(parameters) => parameters,
        Err(message) => {
            eprintln!("{message}");
            drop(tracing_guard);
            std::process::exit(2);
        }
    };

    let code = match run(&parameters).await {
        Ok(report) => {
            println!("{}", render(&report));
            0
        }
        Err(error) => {
            error!(%error, "Scoring the model failed");
            eprintln!("Scoring the model failed: {error}");
            1
        }
    };

    drop(tracing_guard);
    std::process::exit(code);
}

/// The cross-section every arm in one run was measured over.
///
/// Reported beside the scores because the windowing emits samples only for names with a contiguous
/// window, so a baseline read over the whole panel would differ from the model in two respects at
/// once and neither could be blamed for the gap.
#[derive(Debug, Clone, Copy)]
struct Universe {
    tickers: usize,
    sessions: usize,
}

/// How far one statistic moved across seeds that differ in nothing else.
///
/// `standard_deviation` is `None` for a single seed, which measures no spread rather than a spread of
/// zero. A spread far below the per-session standard error is a warning, not a stable architecture.
#[derive(Debug, Clone)]
struct SeedDispersion {
    arm: String,
    seeds: usize,
    mean: f64,
    standard_deviation: Option<f64>,
    minimum: f64,
    maximum: f64,
}

/// What one invocation produced: the universe, every arm's score, and the spread across seeds.
struct Report {
    universe: Universe,
    records: Vec<laboratory::ForecastScored>,
    dispersions: Vec<SeedDispersion>,
}

async fn run(parameters: &Parameters) -> Result<Report, Box<dyn std::error::Error>> {
    let bucket = fund::common::aws::archive_bucket()?;
    let s3_client = fund::common::aws::s3_client().await;

    let now = Utc::now();
    let session = SessionDate::at(now);
    let run_id = uuid::Uuid::new_v4();
    let journal = match laboratory::Journal::from_env() {
        Ok(journal) => Some(journal),
        Err(error) => {
            warn!(%error, "No laboratory journal; this run is not recorded");
            None
        }
    };

    info!(
        bucket,
        lookback_days = parameters.lookback_days,
        epochs = parameters.epochs,
        seed = parameters.seed,
        target = format!("{:?}", parameters.target),
        seed_count = parameters.seed_count,
        %session,
        %run_id,
        "Training models to score them"
    );

    let training_fraction = TrainingFraction::new(TRAINING_FRACTION)?;
    let prepared = dataset::build(
        &s3_client,
        &bucket,
        parameters.lookback_days,
        session,
        dataset::RESEARCH_SCREEN,
        training_fraction,
        parameters.target,
    )
    .await?;
    let fingerprint = prepared.fingerprint;
    if let Some(journal) = journal.as_ref() {
        journal
            .record(
                run_id,
                Utc::now(),
                laboratory::Observation::DatasetBuilt(laboratory::DatasetBuilt {
                    fingerprint: fingerprint.clone(),
                    revision: std::env::var("FUND_REVISION").ok(),
                }),
            )
            .await;
    }

    let training_data = prepared.fit.data.get_dataset(
        DatasetKind::Train(training_fraction),
        INPUT_LENGTH,
        OUTPUT_LENGTH,
    )?;
    let validation_data = prepared.fit.data.get_dataset(
        DatasetKind::Validate(training_fraction),
        INPUT_LENGTH,
        OUTPUT_LENGTH,
    )?;
    if training_data.is_empty() || validation_data.is_empty() {
        return Err("the lookback window produced no training or validation samples".into());
    }
    info!(
        train_samples = training_data.len(),
        validation_samples = validation_data.len(),
        "Built windowed datasets"
    );

    let input_size = input_feature_size(INPUT_LENGTH, OUTPUT_LENGTH);
    let model_parameters = ModelParameters::new(input_size, INPUT_LENGTH, OUTPUT_LENGTH);
    let device = <TrainBackend as Backend>::Device::default();

    // Through the validated constructor rather than by assigning a field, as the trainer does: an
    // epoch count of zero would otherwise run no epochs and score an untrained model.
    let defaults = TrainConfiguration::default();
    let configuration = TrainConfiguration::new(
        defaults.learning_rate(),
        parameters.epochs,
        defaults.batch_size(),
        defaults.early_stopping_patience(),
        defaults.min_delta(),
    )?;

    let arm = match parameters.target {
        Target::Raw => "tide",
        Target::CrossSectionallyDemeaned => "tide_demeaned",
    };
    let null_arm = format!("{arm}_permuted");

    let mut model_evaluations: Vec<Evaluation> = Vec::with_capacity(parameters.seed_count);
    let mut null_evaluations: Vec<Evaluation> = Vec::with_capacity(parameters.seed_count);
    for offset in 0..parameters.seed_count {
        let seed = parameters.seed.wrapping_add(offset as u64);

        model_evaluations.push(fit_and_score(
            &arm_name(arm, seed),
            seed,
            &training_data,
            &validation_data,
            &model_parameters,
            &configuration,
            &device,
            &prepared.fit.scaler,
        )?);

        // The identical pipeline over a target that cannot be forecast, so whatever it still scores
        // is what fitting and scoring manufacture. Both splits are permuted: a model fitted on noise
        // and scored against real outcomes measures something else again.
        let permutation_seed = seed ^ PERMUTATION_SEED;
        let permuted_training = permute_targets(&training_data, permutation_seed)?;
        let permuted_validation = permute_targets(&validation_data, permutation_seed ^ 1)?;
        null_evaluations.push(fit_and_score(
            &arm_name(&null_arm, seed),
            seed,
            &permuted_training,
            &permuted_validation,
            &model_parameters,
            &configuration,
            &device,
            &prepared.fit.scaler,
        )?);
    }

    // The exact cells the model was measured over. Every baseline is cut to the same ones: a
    // forecast scored on fifty sessions against one scored on five hundred compares two stretches of
    // calendar, and a name scored on every session against one scored only where its window was
    // contiguous compares two universes — in which case the difference reported as model quality is
    // partly a difference in population.
    let measured = measured_pairs(&validation_data, &prepared.fit.mappings)?;
    let measured_sessions: BTreeSet<i64> = measured.iter().map(|(session, _)| *session).collect();
    let universe_tickers: BTreeSet<String> =
        measured.iter().map(|(_, ticker)| ticker.clone()).collect();
    let universe = Universe {
        tickers: universe_tickers.len(),
        sessions: measured_sessions.len(),
    };
    info!(
        tickers = universe.tickers,
        sessions = universe.sessions,
        "Scored the models over their validation universe"
    );

    // A second read of the same window, because `build` consumed the first into the fit. The
    // archive is written nightly, so a partition landing between the two would measure the
    // baselines over a snapshot the model never saw — which is the one comparison this binary is for.
    let returns = dataset::returns(
        &s3_client,
        &bucket,
        parameters.lookback_days,
        session,
        dataset::RESEARCH_SCREEN,
    )
    .await?;
    if returns.fingerprint != fingerprint {
        return Err(format!(
            "the archive moved between the two reads of this window: the model was fitted on {} \
             rows over {} tickers and the baselines would be measured on {} over {}",
            fingerprint.rows,
            fingerprint.tickers,
            returns.fingerprint.rows,
            returns.fingerprint.tickers
        )
        .into());
    }
    // Cut by name before the panel is built, so every session of a measured name's history survives
    // — momentum needs twenty sessions of it, and cutting by session as well would starve it. The
    // mask then holds the grading to the model's own cells without touching that history.
    let panel = Panel::from_frame(&restrict_to_tickers(&returns.returns, &universe_tickers)?)?
        .scoring_restricted_to(&measured);
    let baselines: Vec<Box<dyn Predictor>> = vec![
        Box::new(CrossSectionalMean),
        Box::new(Persistence),
        Box::new(Momentum {
            sessions: MOMENTUM_SESSIONS,
        }),
        Box::new(RandomRanking { seed: RANDOM_SEED }),
    ];

    let mut scored: Vec<Evaluation> = Vec::new();
    scored.extend(model_evaluations.iter().cloned());
    scored.extend(null_evaluations.iter().cloned());
    for baseline in &baselines {
        // Evaluated over the whole panel and cut afterwards, so each baseline still reads the
        // history before its first measured session.
        scored.push(restrict(
            evaluate(baseline.as_ref(), &panel),
            &panel,
            &measured_sessions,
        ));
    }

    let mut records = Vec::with_capacity(scored.len());
    for evaluation in &scored {
        let record = laboratory::ForecastScored::from(evaluation);
        info!(
            predictor = record.predictor,
            sessions = record.sessions,
            universe = universe.tickers,
            information_coefficient = record
                .information_coefficient
                .map(|distribution| distribution.mean),
            "Scored a forecast"
        );
        if let Some(journal) = journal.as_ref() {
            journal
                .record(
                    run_id,
                    Utc::now(),
                    laboratory::Observation::ForecastScored(record.clone()),
                )
                .await;
        }
        records.push(record);
    }

    let dispersions: Vec<SeedDispersion> = [
        dispersion(arm, &model_evaluations),
        dispersion(&null_arm, &null_evaluations),
    ]
    .into_iter()
    .flatten()
    .collect();
    for spread in &dispersions {
        match spread.standard_deviation {
            Some(standard_deviation) if standard_deviation < SEED_DISPERSION_FLOOR => warn!(
                arm = spread.arm,
                seeds = spread.seeds,
                standard_deviation,
                "Seeds that differ produced one model; initialisation is not reaching the weights"
            ),
            Some(standard_deviation) => info!(
                arm = spread.arm,
                seeds = spread.seeds,
                mean = spread.mean,
                standard_deviation,
                "Information coefficient across seeds"
            ),
            None => info!(
                arm = spread.arm,
                seeds = spread.seeds,
                "One seed measures one model, so this run has no spread to report"
            ),
        }
    }

    Ok(Report {
        universe,
        records,
        dispersions,
    })
}

/// Seeds the backend, trains one model, and scores the ordering it produces.
///
/// The seed is set immediately before the weights are drawn rather than after, which is the only
/// point at which it reaches them.
#[allow(clippy::too_many_arguments)]
fn fit_and_score(
    name: &str,
    seed: u64,
    training_data: &TrainingDataset,
    validation_data: &TrainingDataset,
    model_parameters: &ModelParameters,
    configuration: &TrainConfiguration,
    device: &<TrainBackend as Backend>::Device,
    scaler: &fund::models::tide::data::Scaler,
) -> Result<Evaluation, Box<dyn std::error::Error>> {
    <TrainBackend as Backend>::seed(seed);
    let model = TiDEModel::<TrainBackend>::new(
        device,
        model_parameters.input_size(),
        model_parameters.hidden_size(),
        model_parameters.encoder_layer_count(),
        model_parameters.decoder_layer_count(),
        model_parameters.output_length(),
        model_parameters.quantiles().len(),
        model_parameters.dropout_rate(),
    );

    let started = tokio::time::Instant::now();
    let (best_model, losses) = train(
        model,
        training_data,
        Some(validation_data),
        model_parameters,
        configuration,
        device,
    );
    info!(
        arm = name,
        seed,
        epochs = losses.len(),
        final_train_loss = losses.last().copied().unwrap_or_default(),
        seconds = started.elapsed().as_secs(),
        "Training complete"
    );

    Ok(forecast::score(
        name,
        &best_model.valid(),
        validation_data,
        model_parameters,
        scaler,
    )?)
}

/// The name a scored forecast is journaled under.
///
/// The seed is part of the name because [`laboratory::ForecastScored`] carries no seed field, and two
/// records differing only by initialisation would otherwise be indistinguishable in the journal.
fn arm_name(base: &str, seed: u64) -> String {
    format!("{base}_seed_{seed}")
}

/// The same samples with each session's targets dealt out among the names in that session.
///
/// Shuffling inside the session leaves the feature panel and the return distribution alone and breaks
/// only the pairing, so whatever the pipeline still scores over it is what the pipeline manufactures.
fn permute_targets(dataset: &TrainingDataset, seed: u64) -> Result<TrainingDataset, TideError> {
    let targets = dataset.targets().ok_or_else(|| {
        TideError::Data("a permutation null needs the targets it is permuting".to_string())
    })?;
    let steps = targets.shape()[1];
    let mut permuted = targets.clone();
    let mut generator = StdRng::seed_from_u64(seed);

    let mut by_session: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for (sample, session) in dataset.forecast_sessions().iter().enumerate() {
        by_session.entry(*session).or_default().push(sample);
    }
    for samples in by_session.values() {
        let mut shuffled = samples.clone();
        shuffled.shuffle(&mut generator);
        for (source, destination) in samples.iter().zip(&shuffled) {
            for step in 0..steps {
                permuted[[*destination, step, 0]] = targets[[*source, step, 0]];
            }
        }
    }

    TrainingDataset::new(
        dataset.past_continuous().clone(),
        dataset.past_categorical().clone(),
        dataset.future_categorical().clone(),
        dataset.static_categorical().clone(),
        Some(permuted),
        dataset.forecast_sessions().to_vec(),
    )
}

/// The `(session, ticker)` cells the model's samples exist for, decoded through the fit's mappings.
///
/// Pairs rather than names: windowing drops a name from the sessions its history is not contiguous
/// over, so the set of names it was measured on does not say which sessions each was measured in.
fn measured_pairs(
    dataset: &TrainingDataset,
    mappings: &FeatureMappings,
) -> Result<BTreeSet<(i64, String)>, Box<dyn std::error::Error>> {
    let mapping = mappings.get("ticker").ok_or(
        "the fitted mappings carry no `ticker` column, so the model's names cannot be read",
    )?;
    let names: HashMap<i32, &str> = mapping
        .iter()
        .map(|(name, id)| (*id, name.as_str()))
        .collect();
    // Derived rather than assumed to be zero: reordering the constant would otherwise decode a
    // sector code through the ticker mapping and build a plausible, wrong universe.
    let column = STATIC_CATEGORICAL_COLUMNS
        .iter()
        .position(|name| *name == "ticker")
        .ok_or("the static categorical columns carry no `ticker`, so a sample names no symbol")?;

    let encoded = dataset.static_categorical();
    let sessions = dataset.forecast_sessions();
    let mut pairs = BTreeSet::new();
    for sample in 0..dataset.len() {
        let id = encoded[[sample, 0, column]];
        let name = names.get(&id).ok_or_else(|| {
            format!("sample {sample} carries encoded ticker {id}, which the mapping does not name")
        })?;
        pairs.insert((sessions[sample], (*name).to_string()));
    }
    Ok(pairs)
}

/// Cuts the returns frame to the names the model was measured on, keeping their whole history.
fn restrict_to_tickers(
    frame: &DataFrame,
    tickers: &BTreeSet<String>,
) -> Result<DataFrame, PolarsError> {
    let names: Vec<String> = tickers.iter().cloned().collect();
    let measured = Series::new("measured_ticker".into(), &names);
    frame
        .clone()
        .lazy()
        .filter(col("ticker").is_in(lit(measured), false))
        .collect()
}

/// The spread of the information coefficient across seeds that differ in nothing else.
///
/// Returns `None` for an arm whose seeds all abstained, because an unmeasurable statistic has no
/// spread; a single seed yields a `standard_deviation` of `None` for the same reason.
fn dispersion(arm: &str, evaluations: &[Evaluation]) -> Option<SeedDispersion> {
    let observed: Vec<f64> = evaluations
        .iter()
        .filter_map(|evaluation| evaluation.information_coefficient)
        .map(|distribution| distribution.mean)
        .filter(|value| value.is_finite())
        .collect();
    if observed.is_empty() {
        return None;
    }

    let seeds = observed.len();
    let mean = observed.iter().sum::<f64>() / seeds as f64;
    let standard_deviation = (seeds > 1).then(|| {
        (observed
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (seeds - 1) as f64)
            .sqrt()
    });
    Some(SeedDispersion {
        arm: arm.to_string(),
        seeds,
        mean,
        standard_deviation,
        minimum: observed.iter().copied().fold(f64::INFINITY, f64::min),
        maximum: observed.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    })
}

/// Keeps only the sessions in `measured`, then re-summarizes what is left.
fn restrict(evaluation: Evaluation, panel: &Panel, measured: &BTreeSet<i64>) -> Evaluation {
    let sessions: Vec<metrics::SessionMetrics> = evaluation
        .sessions
        .iter()
        .enumerate()
        .filter(|(index, _)| measured.contains(&panel.session_at(*index)))
        .map(|(_, session)| *session)
        .collect();

    Evaluation {
        predictor: evaluation.predictor,
        information_coefficient: metrics::summarize(
            sessions
                .iter()
                .map(|session| session.information_coefficient),
        ),
        decile_spread: metrics::summarize(sessions.iter().map(|session| session.decile_spread)),
        directional_accuracy: metrics::summarize(
            sessions.iter().map(|session| session.directional_accuracy),
        ),
        sessions,
    }
}

fn render(report: &Report) -> String {
    let mut rendered = format!(
        "universe: {} names over {} sessions, every arm cut to both\n",
        report.universe.tickers, report.universe.sessions
    );
    rendered.push_str(&format!(
        "{:<36}{:>10}{:>30}{:>30}{:>30}\n",
        "predictor", "sessions", "information_coefficient", "decile_spread", "directional_accuracy"
    ));
    for record in &report.records {
        rendered.push_str(&format!(
            "{:<36}{:>10}{:>30}{:>30}{:>30}\n",
            record.predictor,
            record.sessions,
            distribution(record.information_coefficient),
            distribution(record.decile_spread),
            distribution(record.directional_accuracy),
        ));
    }

    if !report.dispersions.is_empty() {
        rendered.push_str(
            "\ninformation coefficient across seeds — a tight spread is a warning, not a result\n",
        );
        for spread in &report.dispersions {
            rendered.push_str(&format!(
                "{:<36}{:>10}{:>16.6}{:>16}{:>16.6}{:>16.6}\n",
                spread.arm,
                spread.seeds,
                spread.mean,
                spread
                    .standard_deviation
                    .map_or_else(|| "unmeasurable".to_string(), |value| format!("{value:.6}")),
                spread.minimum,
                spread.maximum,
            ));
        }
    }
    rendered
}

fn distribution(value: Option<Distribution>) -> String {
    value.map_or_else(
        || "unmeasurable".to_string(),
        |distribution| {
            format!(
                "{:+.6} ± {:.6} ({})",
                distribution.mean, distribution.standard_error, distribution.sessions
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    /// An evaluation carrying one information coefficient and nothing else, which is all
    /// [`dispersion`] reads.
    fn evaluation_scoring(information_coefficient: f64) -> Evaluation {
        Evaluation {
            predictor: "tide".to_string(),
            sessions: Vec::new(),
            information_coefficient: Some(Distribution {
                mean: information_coefficient,
                standard_error: 0.0,
                sessions: 2,
            }),
            decile_spread: None,
            directional_accuracy: None,
        }
    }

    /// Samples whose only content is their target and the session they forecast.
    fn dataset_of(sessions: &[i64], values: &[f32]) -> TrainingDataset {
        let samples = values.len();
        let mut targets = ndarray::Array3::<f32>::zeros((samples, 1, 1));
        for (sample, value) in values.iter().enumerate() {
            targets[[sample, 0, 0]] = *value;
        }
        TrainingDataset::new(
            ndarray::Array3::zeros((samples, 2, 7)),
            ndarray::Array3::zeros((samples, 2, 5)),
            ndarray::Array3::zeros((samples, 1, 5)),
            ndarray::Array3::zeros((samples, 1, 3)),
            Some(targets),
            sessions.to_vec(),
        )
        .unwrap()
    }

    fn targets_of(dataset: &TrainingDataset) -> Vec<f32> {
        let targets = dataset.targets().unwrap();
        (0..dataset.len())
            .map(|sample| targets[[sample, 0, 0]])
            .collect()
    }

    /// The null must be the same panel with the pairing broken, not a different panel. A target that
    /// crossed a session would put a return in a cross-section it never belonged to, and the arm
    /// would then differ from the treatment in two respects.
    #[test]
    fn test_the_permutation_leaves_every_session_holding_its_own_returns() {
        let sessions: Vec<i64> = (0..12).map(|_| 0).chain((0..12).map(|_| DAY)).collect();
        let values: Vec<f32> = (0..12)
            .map(|name| name as f32)
            .chain((0..12).map(|name| 100.0 + name as f32))
            .collect();

        let permuted = permute_targets(&dataset_of(&sessions, &values), 0x51D).unwrap();
        assert_eq!(permuted.forecast_sessions(), sessions.as_slice());

        let permuted_targets = targets_of(&permuted);
        let mut first = permuted_targets[..12].to_vec();
        let mut second = permuted_targets[12..].to_vec();
        first.sort_by(|left, right| left.partial_cmp(right).unwrap());
        second.sort_by(|left, right| left.partial_cmp(right).unwrap());

        assert_eq!(first, (0..12).map(|name| name as f32).collect::<Vec<f32>>());
        assert_eq!(
            second,
            (0..12)
                .map(|name| 100.0 + name as f32)
                .collect::<Vec<f32>>()
        );
    }

    /// A null that left the pairing intact would fit and score the treatment a second time and
    /// report it as the floor the treatment must clear.
    #[test]
    fn test_the_permutation_moves_the_targets_it_was_given() {
        let sessions: Vec<i64> = (0..12).map(|_| 0).collect();
        let values: Vec<f32> = (0..12).map(|name| name as f32).collect();
        let dataset = dataset_of(&sessions, &values);

        let permuted = permute_targets(&dataset, 0x51D).unwrap();

        assert_ne!(
            targets_of(&permuted),
            targets_of(&dataset),
            "the shuffle returned the order it was given"
        );
    }

    /// A dataset with no outcomes cannot have them permuted, and inventing zeros would score the
    /// null against a target nobody realized.
    #[test]
    fn test_a_dataset_without_targets_cannot_be_permuted() {
        let bare = TrainingDataset::new(
            ndarray::Array3::zeros((4, 2, 7)),
            ndarray::Array3::zeros((4, 2, 5)),
            ndarray::Array3::zeros((4, 1, 5)),
            ndarray::Array3::zeros((4, 1, 3)),
            None,
            vec![0; 4],
        )
        .unwrap();
        assert!(permute_targets(&bare, 1).is_err());
    }

    #[test]
    fn test_arguments_default_from_the_right() {
        let parameters = Parameters::parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.epochs, 5);
        assert_eq!(parameters.seed, DEFAULT_TRAINING_SEED as u64);
        assert_eq!(parameters.seed_count, 3);

        let parameters = Parameters::parse(&arguments(&["400", "2"])).unwrap();
        assert_eq!(parameters.lookback_days, 400);
        assert_eq!(parameters.epochs, 2);
        assert_eq!(parameters.seed, DEFAULT_TRAINING_SEED as u64);

        let parameters = Parameters::parse(&arguments(&["400", "2", "9"])).unwrap();
        assert_eq!(parameters.seed, 9);
        assert_eq!(parameters.target, Target::Raw);

        let parameters = Parameters::parse(&arguments(&["400", "2", "9", "raw", "7"])).unwrap();
        assert_eq!(parameters.seed_count, 7);
    }

    /// A run that trains one seed reports a coefficient with no error bar on the thing it varied,
    /// which is the reading two runs of one configuration disagreed about.
    #[test]
    fn test_the_seed_count_is_read_and_refused_like_the_others() {
        assert_eq!(
            Parameters::parse(&arguments(&["400", "2", "9", "demeaned", "5"]))
                .unwrap()
                .seed_count,
            5
        );
        assert!(Parameters::parse(&arguments(&["400", "2", "9", "raw", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["400", "2", "9", "raw", "x"])).is_err());
        assert!(Parameters::parse(&arguments(&["400", "2", "9", "raw", "1", "1"])).is_err());
    }

    /// The two arms must be distinguishable in a journal record that carries no seed field.
    #[test]
    fn test_each_seed_is_journaled_under_its_own_name() {
        assert_eq!(arm_name("tide", 31_261), "tide_seed_31261");
        assert_ne!(arm_name("tide", 31_261), arm_name("tide", 31_262));
        assert_ne!(arm_name("tide", 31_261), arm_name("tide_permuted", 31_261));
    }

    /// A spread of zero and no spread at all are different claims, and only one of them is a fact
    /// about the architecture.
    #[test]
    fn test_one_seed_measures_no_spread_rather_than_a_spread_of_zero() {
        let single = dispersion("tide", &[evaluation_scoring(0.02)]).unwrap();
        assert_eq!(single.seeds, 1);
        assert_eq!(single.mean, 0.02);
        assert_eq!(single.standard_deviation, None);

        let identical = dispersion(
            "tide",
            &[evaluation_scoring(0.02), evaluation_scoring(0.02)],
        )
        .unwrap();
        assert_eq!(identical.standard_deviation, Some(0.0));
        assert!(
            identical.standard_deviation.unwrap() < SEED_DISPERSION_FLOOR,
            "two seeds landing on one number is what the floor exists to catch"
        );
    }

    /// The spread must be the sample standard deviation of the per-seed means, pinned to a literal
    /// rather than to the arithmetic under test.
    #[test]
    fn test_the_spread_across_seeds_is_the_sample_standard_deviation() {
        let spread = dispersion(
            "tide",
            &[
                evaluation_scoring(0.01),
                evaluation_scoring(0.02),
                evaluation_scoring(0.03),
            ],
        )
        .unwrap();

        assert_eq!(spread.seeds, 3);
        assert!((spread.mean - 0.02).abs() < 1e-12);
        // Deviations of -0.01, 0, +0.01: variance 0.0002 / 2 = 0.0001, so the deviation is 0.01.
        assert!(
            (spread.standard_deviation.unwrap() - 0.01).abs() < 1e-12,
            "got {:?}",
            spread.standard_deviation
        );
        assert_eq!(spread.minimum, 0.01);
        assert_eq!(spread.maximum, 0.03);
    }

    /// An arm whose seeds all abstained has no spread, and reporting one would assert a reading
    /// nobody took.
    #[test]
    fn test_an_arm_that_never_ranked_reports_no_spread() {
        let mut silent = evaluation_scoring(0.0);
        silent.information_coefficient = None;
        assert!(dispersion("tide", &[silent]).is_none());
        assert!(dispersion("tide", &[]).is_none());
    }

    /// The argument that selects the arm, so a regression here would silently compare one target
    /// against itself — and comparing the two arms is the whole point of running this.
    #[test]
    fn test_both_targets_are_accepted_and_raw_is_the_default() {
        assert_eq!(Parameters::parse(&[]).unwrap().target, Target::Raw);
        assert_eq!(
            Parameters::parse(&arguments(&["400", "2", "9", "raw"]))
                .unwrap()
                .target,
            Target::Raw
        );
        assert_eq!(
            Parameters::parse(&arguments(&["400", "2", "9", "demeaned"]))
                .unwrap()
                .target,
            Target::CrossSectionallyDemeaned
        );
        assert!(Parameters::parse(&arguments(&["400", "2", "9", "Demeaned"])).is_err());
    }

    /// The seed is the whole reason a run is repeatable: initialisation moves the reported
    /// coefficient by more than the tradeable threshold, so a run that did not name its seed would
    /// report a number nobody could get back.
    #[test]
    fn test_the_seed_is_read_and_refused_like_the_others() {
        assert!(Parameters::parse(&arguments(&["365", "5", "0"])).is_err());
        assert!(Parameters::parse(&arguments(&["365", "5", "x"])).is_err());
        assert!(Parameters::parse(&arguments(&["365", "5", "9", "1"])).is_err());
    }

    #[test]
    fn test_an_unusable_argument_is_refused() {
        for value in ["1o", "0", "-1", ""] {
            assert!(
                Parameters::parse(&arguments(&[value])).is_err(),
                "{value:?} must be refused"
            );
        }
        assert!(Parameters::parse(&arguments(&["365", "0"])).is_err());
    }
}
