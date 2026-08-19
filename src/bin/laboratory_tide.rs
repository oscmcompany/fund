//! Trains one model and scores it against the baselines, over the same sessions.
//!
//! Publishes nothing: the question is what the model orders, not what it should trade.

use std::collections::BTreeSet;

use burn::module::AutodiffModule;
use burn::tensor::backend::Backend;
use chrono::Utc;
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
use fund::models::tide::data::{input_feature_size, DatasetKind, TrainingFraction};
use fund::models::tide::model::TiDEModel;
use fund::models::tide::train::{train, TrainBackend, TrainConfiguration};

const USAGE: &str = "Usage: laboratory_tide [LOOKBACK_DAYS] [EPOCHS] [SEED]";

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
/// `train` already seeds its own batch shuffle; the weights went through the backend's global
/// generator, and two runs of the same configuration scored 0.008 apart — a whole standard error.
/// Settable, because one seed measures one model and the question is about the architecture.
const DEFAULT_TRAINING_SEED: i64 = 0x7A1D;

struct Parameters {
    lookback_days: i64,
    epochs: usize,
    seed: u64,
}

impl Parameters {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let (lookback_days, epochs, seed) = match arguments {
            [] => (DEFAULT_LOOKBACK_DAYS, DEFAULT_EPOCHS, DEFAULT_TRAINING_SEED),
            [lookback] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                DEFAULT_EPOCHS,
                DEFAULT_TRAINING_SEED,
            ),
            [lookback, epochs] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(epochs, "EPOCHS")?,
                DEFAULT_TRAINING_SEED,
            ),
            [lookback, epochs, seed] => (
                positive(lookback, "LOOKBACK_DAYS")?,
                positive(epochs, "EPOCHS")?,
                positive(seed, "SEED")?,
            ),
            _ => return Err(format!("Too many arguments\n{USAGE}")),
        };
        Ok(Self {
            lookback_days,
            epochs: usize::try_from(epochs)
                .map_err(|_| format!("EPOCHS is larger than this platform can index\n{USAGE}"))?,
            seed: seed as u64,
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
        Ok(scored) => {
            println!("{}", render(&scored));
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

async fn run(
    parameters: &Parameters,
) -> Result<Vec<laboratory::ForecastScored>, Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
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
        %session,
        %run_id,
        "Training a model to score it"
    );

    let training_fraction = TrainingFraction::new(TRAINING_FRACTION)?;
    let prepared = dataset::build(
        &s3_client,
        &bucket,
        parameters.lookback_days,
        session,
        training_fraction,
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
    // Before the weights are drawn, not after.
    <TrainBackend as Backend>::seed(parameters.seed);
    let model = TiDEModel::<TrainBackend>::new(
        &device,
        input_size,
        model_parameters.hidden_size(),
        model_parameters.encoder_layer_count(),
        model_parameters.decoder_layer_count(),
        model_parameters.output_length(),
        model_parameters.quantiles().len(),
        model_parameters.dropout_rate(),
    );

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
    let started = tokio::time::Instant::now();
    let (best_model, losses) = train(
        model,
        &training_data,
        Some(&validation_data),
        &model_parameters,
        &configuration,
        &device,
    );
    info!(
        epochs = losses.len(),
        final_train_loss = losses.last().copied().unwrap_or_default(),
        seconds = started.elapsed().as_secs(),
        "Training complete"
    );

    let scored_model = forecast::score(
        "tide",
        &best_model.valid(),
        &validation_data,
        &model_parameters,
        &prepared.fit.scaler,
    )?;

    // The sessions the model was actually measured over. Every baseline is then cut to the same
    // set, because comparing a forecast scored on fifty sessions against one scored on five hundred
    // compares two stretches of calendar rather than two forecasts.
    let measured: BTreeSet<i64> = validation_data
        .forecast_sessions()
        .iter()
        .copied()
        .collect();
    info!(
        sessions = measured.len(),
        "Scored the model over its validation window"
    );

    // A second read of the same window, because `build` consumed the first into the fit. The
    // archive is written nightly, so a partition landing between the two would measure the
    // baselines over a snapshot the model never saw — which is the one comparison this binary is for.
    let returns = dataset::returns(&s3_client, &bucket, parameters.lookback_days, session).await?;
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
    let panel = Panel::from_frame(&returns.returns)?;
    let baselines: Vec<Box<dyn Predictor>> = vec![
        Box::new(CrossSectionalMean),
        Box::new(Persistence),
        Box::new(Momentum {
            sessions: MOMENTUM_SESSIONS,
        }),
        Box::new(RandomRanking { seed: RANDOM_SEED }),
    ];

    let mut scored = vec![scored_model];
    for baseline in &baselines {
        // Evaluated over the whole panel and cut afterwards, so each baseline still reads the
        // history before its first measured session — momentum needs twenty sessions of it.
        scored.push(restrict(
            evaluate(baseline.as_ref(), &panel),
            &panel,
            &measured,
        ));
    }

    let mut records = Vec::with_capacity(scored.len());
    for evaluation in &scored {
        let record = laboratory::ForecastScored::from(evaluation);
        info!(
            predictor = record.predictor,
            sessions = record.sessions,
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

    Ok(records)
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

fn render(scored: &[laboratory::ForecastScored]) -> String {
    let mut rendered = format!(
        "{:<22}{:>10}{:>30}{:>30}{:>30}\n",
        "predictor", "sessions", "information_coefficient", "decile_spread", "directional_accuracy"
    );
    for record in scored {
        rendered.push_str(&format!(
            "{:<22}{:>10}{:>30}{:>30}{:>30}\n",
            record.predictor,
            record.sessions,
            distribution(record.information_coefficient),
            distribution(record.decile_spread),
            distribution(record.directional_accuracy),
        ));
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

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn test_arguments_default_from_the_right() {
        let parameters = Parameters::parse(&[]).unwrap();
        assert_eq!(parameters.lookback_days, 365);
        assert_eq!(parameters.epochs, 5);
        assert_eq!(parameters.seed, DEFAULT_TRAINING_SEED as u64);

        let parameters = Parameters::parse(&arguments(&["400", "2"])).unwrap();
        assert_eq!(parameters.lookback_days, 400);
        assert_eq!(parameters.epochs, 2);
        assert_eq!(parameters.seed, DEFAULT_TRAINING_SEED as u64);

        let parameters = Parameters::parse(&arguments(&["400", "2", "9"])).unwrap();
        assert_eq!(parameters.seed, 9);
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
