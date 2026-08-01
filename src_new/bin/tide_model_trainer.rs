//! The trainer: fetch, archive, train, publish. Runs on its own machine with no database.
//!
//! Four stages, and the first one is new. The trainer used to read a dataset the application's
//! nightly export had written, which coupled two machines through a bucket and made a failed export
//! cost the next day's model. It now fetches its own bars from Alpaca and writes its own parquet,
//! so the only thing crossing the boundary between the two VMs is the finished artifact.
//!
//! Both sides fetch through [`fund::data::bars`] and build frames through
//! [`fund::data::bars::bars_to_dataframe`]. That shared path is the point: if the trainer's frames
//! and the application's ever diverged, the model would train on columns the inference path does
//! not produce, and it would surface as bad predictions rather than as a build error.
//!
//! Nothing here writes to a database, and nothing here reads one.

use std::collections::HashSet;
use std::io::Cursor;

use burn::module::AutodiffModule;
use burn::tensor::backend::Backend;
use chrono::{Duration, NaiveDate, Utc};
use polars::prelude::*;
use tracing::{error, info, warn};

use fund::common::alpaca::{AlpacaCredentials, MarketDataClient, TradingClient};
use fund::common::aws::date_partitioned_key;
use fund::common::observability::init_tracing;
use fund::common::types::{BarInterval, MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME};
use fund::data::bars;
use fund::data::calendar::{eastern_date, is_weekend};
use fund::data::details;
use fund::models::artifact::{
    candidate_folders_descending, list_run_folders, package_dir_to_tar_gz, upload_artifact,
};
use fund::models::predict::consolidate_data;
use fund::models::tide::config::ModelParameters;
use fund::models::tide::data::input_feature_size;
use fund::models::tide::drift::{check_drift, DriftStatus};
use fund::models::tide::evaluate::evaluate;
use fund::models::tide::fit::{filter_training_bars, fit, write_artifact_json};
use fund::models::tide::model::TideModel;
use fund::models::tide::train::{train, TrainBackend, TrainConfig};

const INPUT_LENGTH: usize = 35;
const OUTPUT_LENGTH: usize = 5;
const VALIDATION_SPLIT: f64 = 0.8;

/// S3 prefix for the trainer's own bar archive.
///
/// Deliberately not under `exports/`, which is where the application's nightly database export
/// lands. The two datasets live in one bucket and describe overlapping facts, and giving them one
/// prefix would make whichever job ran second the one that mattered.
const BAR_ARCHIVE_PREFIX: &str = "data/equity/bars";

/// S3 key for the ticker metadata that accompanies the archive.
const DETAILS_ARCHIVE_KEY: &str = "data/equity/details/details.csv";

/// Sessions the fetch stage re-requests each night.
///
/// Three rather than one, for the same reason the application's sync uses three: a night the
/// trainer did not run leaves a hole in the archive that nothing else fills, and the overlap is one
/// extra request against a partition that is simply overwritten.
const FETCH_LOOKBACK_SESSIONS: i64 = 3;

// Drift baseline: mean CRPS of the most recent prior runs. Reported, never used to block an
// artifact -- a model that has degraded is still better than no model at all, and the decision to
// stop trading on it is not one a training job should be making at 4pm unattended.
const DRIFT_PRIOR_RUN_COUNT: usize = 7;
const DRIFT_MINIMUM_RUNS: usize = 3;
const DRIFT_DEGRADATION_THRESHOLD: f64 = 0.20;

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let _tracing_guard = init_tracing("tide-model-trainer.log", Some("info"), "tide-model-trainer");
    if let Err(error) = run().await {
        error!(%error, "Training failed");
        eprintln!("Training failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let artifact_prefix =
        std::env::var("AWS_S3_MODEL_ARTIFACT_PATH").unwrap_or_else(|_| "models/tide/".to_string());
    let lookback_days: i64 = std::env::var("FUND_LOOKBACK_DAYS")
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok())
        .unwrap_or(365);

    let s3_client = fund::common::aws::s3_client().await;

    info!(
        bucket,
        artifact_prefix, lookback_days, "Starting tide training"
    );

    // --- stage one: fetch and archive ---
    //
    // A failure here is logged and stepped over rather than fatal. The archive already holds a
    // year; a night with no new partition trains on 364 days instead of 365, which is a far better
    // outcome than publishing no model because Alpaca was briefly unreachable.
    match fetch_and_archive(&s3_client, &bucket).await {
        Ok(rows) => info!(rows, "Session bars archived"),
        Err(error) => warn!(%error, "Fetch stage failed; training on the existing archive"),
    }

    // --- stage two: load the accumulated window ---

    let equity_bars = load_archived_bars(&s3_client, &bucket, lookback_days).await?;
    info!(rows = equity_bars.height(), "Loaded equity bars from S3");

    let equity_details = details::details_to_dataframe(&details::parse_embedded_details()?)?;
    info!(rows = equity_details.height(), "Loaded equity details");

    let consolidated = consolidate_data(equity_bars, equity_details)?;
    let filtered = filter_training_bars(consolidated, MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME)?;
    info!(rows = filtered.height(), "Consolidated and filtered");

    // --- stage three: train and evaluate ---

    let fit_result = fit(filtered)?;

    let train_dataset =
        fit_result
            .data
            .get_dataset("train", VALIDATION_SPLIT, INPUT_LENGTH, OUTPUT_LENGTH)?;
    let valid_dataset =
        fit_result
            .data
            .get_dataset("validate", VALIDATION_SPLIT, INPUT_LENGTH, OUTPUT_LENGTH)?;
    info!(
        train_samples = train_dataset.len(),
        validation_samples = valid_dataset.len(),
        "Built windowed datasets"
    );
    if train_dataset.is_empty() {
        return Err("No training samples produced from the lookback window".into());
    }

    let input_size = input_feature_size(INPUT_LENGTH, OUTPUT_LENGTH);
    let parameters = ModelParameters::new(input_size, INPUT_LENGTH, OUTPUT_LENGTH);

    let device = <TrainBackend as Backend>::Device::default();
    let model = TideModel::<TrainBackend>::new(
        &device,
        input_size,
        parameters.hidden_size(),
        parameters.num_encoder_layers(),
        parameters.num_decoder_layers(),
        parameters.output_length(),
        parameters.quantiles().len(),
        parameters.dropout_rate(),
    );

    let config = training_config()?;
    let (best_model, losses) = train(
        model,
        &train_dataset,
        Some(&valid_dataset),
        &parameters,
        &config,
        &device,
    );
    info!(
        epochs = losses.len(),
        final_train_loss = losses.last().copied().unwrap_or_default(),
        "Training complete"
    );

    let inner_model = best_model.valid();
    let metrics = evaluate(&inner_model, &valid_dataset, &parameters)?;
    info!(
        crps = metrics.crps,
        directional_accuracy = metrics.directional_accuracy,
        quantile_coverage = metrics.quantile_coverage,
        "Evaluation metrics"
    );

    // --- stage four: publish ---

    let staging = tempfile::tempdir()?;
    write_artifact_json(
        staging.path(),
        &fit_result.scaler,
        &fit_result.mappings,
        &parameters,
    )?;
    inner_model.save(staging.path())?;

    // The run identifier is a sortable timestamp, and the application reads its date prefix to
    // report how stale the model it is serving is. A different format would silently turn that
    // staleness field into "unknown" on every session.
    let timestamp = Utc::now().format("%Y-%m-%d-%H-%M-%S-%3f").to_string();
    let model_key = format!("{artifact_prefix}{timestamp}/output/model.tar.gz");
    upload_artifact(
        &s3_client,
        &bucket,
        &model_key,
        package_dir_to_tar_gz(staging.path())?,
        "application/gzip",
    )
    .await?;
    info!(key = model_key, "Uploaded model artifact");

    let current_folder = format!("{artifact_prefix}{timestamp}/");
    let prior_crps = fetch_prior_crps(
        &s3_client,
        &bucket,
        &artifact_prefix,
        &current_folder,
        DRIFT_PRIOR_RUN_COUNT,
    )
    .await;
    let drift = check_drift(
        metrics.crps,
        &prior_crps,
        DRIFT_MINIMUM_RUNS,
        DRIFT_DEGRADATION_THRESHOLD,
    );
    match drift.status {
        DriftStatus::DriftDetected => warn!(
            current_crps = drift.current_crps,
            baseline_crps = drift.baseline_crps,
            "Model drift detected"
        ),
        _ => info!(
            current_crps = drift.current_crps,
            baseline_crps = drift.baseline_crps,
            prior_runs = prior_crps.len(),
            message = drift.message,
            "Drift check complete"
        ),
    }

    let end_date = Utc::now().date_naive();
    let start_date = end_date - Duration::days(lookback_days);
    let metadata = serde_json::json!({
        "artifact_timestamp": timestamp,
        "input_size": input_size,
        "input_length": INPUT_LENGTH,
        "output_length": OUTPUT_LENGTH,
        "lookback_days": lookback_days,
        "start_date": start_date.format("%Y-%m-%d").to_string(),
        "end_date": end_date.format("%Y-%m-%d").to_string(),
        "epochs_run": losses.len(),
        "final_train_loss": losses.last().copied().unwrap_or_default(),
        "metrics": metrics,
        "train_samples": train_dataset.len(),
        "validation_samples": valid_dataset.len(),
        "drift": {
            "status": drift.status,
            "message": drift.message,
            "baseline_crps": drift.baseline_crps,
            "prior_runs": prior_crps.len(),
        },
    });
    upload_artifact(
        &s3_client,
        &bucket,
        &format!("{current_folder}run_metadata.json"),
        serde_json::to_vec_pretty(&metadata)?,
        "application/json",
    )
    .await?;

    println!("Training complete: artifact s3://{bucket}/{model_key}");
    println!(
        "Metrics: CRPS={:.6} directional_accuracy={:.4} quantile_coverage={:.4}",
        metrics.crps, metrics.directional_accuracy, metrics.quantile_coverage
    );
    Ok(())
}

/// The training configuration, with `FUND_EPOCHS` applied over the defaults.
///
/// Rebuilt through the validated constructor rather than by assigning a field, so an epoch count of
/// zero is rejected here with a message rather than reaching the training loop, which would run no
/// epochs and publish an untrained model that looks exactly like a trained one.
fn training_config() -> Result<TrainConfig, Box<dyn std::error::Error>> {
    let defaults = TrainConfig::default();
    let epoch_count = std::env::var("FUND_EPOCHS")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or_else(|| defaults.epoch_count());

    Ok(TrainConfig::new(
        defaults.learning_rate(),
        epoch_count,
        defaults.batch_size(),
        defaults.early_stopping_patience(),
        defaults.min_delta(),
    )?)
}

/// Fetches the most recent sessions' bars from Alpaca and writes them to the archive.
///
/// Returns the number of rows written. The universe is Alpaca's tradable set intersected with the
/// embedded ticker list — the trainer has no database, so it cannot compute the application's
/// liquidity-filtered universe, and it does not need to: `filter_training_bars` applies the same
/// price and volume thresholds to the rows themselves further down.
async fn fetch_and_archive(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let credentials = AlpacaCredentials::from_env()?;
    let trading = TradingClient::from_env(credentials.clone());
    let market_data = MarketDataClient::from_env(credentials);

    let assets = trading.fetch_tradable_assets().await?;
    let known: HashSet<String> = details::parse_embedded_details()?
        .into_iter()
        .map(|detail| detail.ticker().as_str().to_string())
        .collect();
    let symbols: Vec<String> = assets
        .tradable_symbols()
        .into_iter()
        .filter(|symbol| known.contains(symbol))
        .collect();

    let end = most_recent_weekday(eastern_date(Utc::now()));
    let start = end - Duration::days(FETCH_LOOKBACK_SESSIONS * 2);
    info!(
        symbols = symbols.len(),
        %start,
        %end,
        "Fetching session bars from Alpaca"
    );

    let fetched = bars::fetch_bars(&market_data, &symbols, BarInterval::OneDay, start, end).await?;
    if fetched.is_empty() {
        return Ok(0);
    }

    // One partition per session date, so the archive stays date-partitioned and the loader below
    // can walk it a day at a time.
    let mut by_date: std::collections::BTreeMap<NaiveDate, Vec<_>> =
        std::collections::BTreeMap::new();
    for bar in fetched {
        by_date
            .entry(eastern_date(bar.timestamp()))
            .or_default()
            .push(bar);
    }

    let mut written = 0;
    for (date, bars_for_date) in by_date {
        let mut frame = bars::bars_to_dataframe(&bars_for_date)?;
        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer).finish(&mut frame)?;

        upload_artifact(
            s3_client,
            bucket,
            &date_partitioned_key(BAR_ARCHIVE_PREFIX, date),
            buffer,
            "application/octet-stream",
        )
        .await?;
        written += frame.height();
    }

    upload_artifact(
        s3_client,
        bucket,
        DETAILS_ARCHIVE_KEY,
        details::embedded_csv().as_bytes().to_vec(),
        "text/csv",
    )
    .await?;

    Ok(written)
}

/// The most recent weekday at or before `date`.
///
/// A weekday check rather than a calendar lookup. The trainer would otherwise need the published
/// calendar for one date arithmetic, and a request for a holiday's partition simply returns nothing
/// — which the loader below already steps over.
fn most_recent_weekday(date: NaiveDate) -> NaiveDate {
    let mut candidate = date;
    while is_weekend(candidate) {
        candidate = candidate.pred_opt().unwrap_or(candidate);
    }
    candidate
}

/// Reads every available daily partition over the lookback window and concatenates them.
///
/// Missing days — weekends, holidays, and nights the fetch stage did not run — are skipped rather
/// than treated as errors, which is what lets a single failed fetch cost one session of history
/// instead of the whole run.
async fn load_archived_bars(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    lookback_days: i64,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let end_date = Utc::now().date_naive();
    let start_date = end_date - Duration::days(lookback_days);

    let mut frames: Vec<LazyFrame> = Vec::new();
    let mut date = start_date;
    while date <= end_date {
        let key = date_partitioned_key(BAR_ARCHIVE_PREFIX, date);
        if let Ok(response) = s3_client.get_object().bucket(bucket).key(&key).send().await {
            let bytes = response.body.collect().await?.into_bytes();
            frames.push(ParquetReader::new(Cursor::new(bytes)).finish()?.lazy());
        }
        date = match date.succ_opt() {
            Some(next_date) => next_date,
            None => break,
        };
    }

    if frames.is_empty() {
        return Err("No equity-bar parquet files found in the lookback window".into());
    }
    Ok(concat(frames, UnionArgs::default())?.collect()?)
}

/// CRPS from the most recent prior runs' `run_metadata.json`, newest first.
///
/// Read from the standalone metadata object rather than from inside the artifact tarball, which is
/// what the deleted `model_runs` lineage table used to make possible. Failures degrade to an empty
/// history: a drift baseline that cannot be read is not a reason to fail a training run.
async fn fetch_prior_crps(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    current_folder: &str,
    run_count: usize,
) -> Vec<f64> {
    let folders = match list_run_folders(s3_client, bucket, prefix).await {
        Ok(folders) => folders,
        Err(error) => {
            warn!(%error, "Failed to list prior runs for the drift check");
            return Vec::new();
        }
    };

    let mut prior_crps = Vec::new();
    for folder in candidate_folders_descending(folders) {
        if prior_crps.len() >= run_count {
            break;
        }
        if folder == current_folder {
            continue;
        }
        let Ok(response) = s3_client
            .get_object()
            .bucket(bucket)
            .key(format!("{folder}run_metadata.json"))
            .send()
            .await
        else {
            continue;
        };
        let Ok(bytes) = response.body.collect().await else {
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&bytes.into_bytes()) else {
            continue;
        };
        if let Some(crps) = metadata["metrics"]["crps"].as_f64() {
            prior_crps.push(crps);
        }
    }
    prior_crps
}
