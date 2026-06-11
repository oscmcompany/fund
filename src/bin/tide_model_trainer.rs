//! Rust-native TiDE training entrypoint.
//!
//! Loads equity bars and category details from S3 over a lookback window, fits
//! the scaler and categorical mappings, trains the Burn `TideModel` on the
//! `Autodiff<NdArray>` backend, evaluates on a held-out date split, then packages
//! and uploads a `model.tar.gz` the inference service loads directly. Replaces
//! the former Python/tinygrad `tide.workflow`.

use std::io::Cursor;

use burn::module::AutodiffModule;
use burn::tensor::backend::Backend;
use chrono::{Duration, Utc};
use polars::prelude::*;
use tracing::{error, info, warn};

use fund::common::observability::init_tracing;
use fund::domain::market::{MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME};
use fund::ensemble_manager::artifact::{
    candidate_folders_descending, fetch_run_metadata, list_run_folders,
};
use fund::ensemble_manager::predict::consolidate_data;
use fund::models::tide::artifact::{package_dir_to_tar_gz, upload_artifact};
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

// Drift baseline: mean CRPS of the most recent prior runs (the Python trainer
// compared against 7, required at least 3, and flagged >20% degradation).
const DRIFT_PRIOR_RUN_COUNT: usize = 7;
const DRIFT_MINIMUM_RUNS: usize = 3;
const DRIFT_DEGRADATION_THRESHOLD: f64 = 0.20;

#[tokio::main]
async fn main() {
    let _tracing_guard = init_tracing("tide-model-trainer.log", Some("info"));
    if let Err(error) = run().await {
        error!("Training failed: {}", error);
        eprintln!("Training failed: {}", error);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let data_bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    // Write artifacts where the inference service reads them. In production these
    // env vars are set explicitly; in dev they fall back to the data bucket under
    // the models/tide/ prefix (where the prior pipeline wrote).
    let artifact_bucket =
        std::env::var("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME").unwrap_or_else(|_| data_bucket.clone());
    let artifact_prefix =
        std::env::var("AWS_S3_MODEL_ARTIFACT_PATH").unwrap_or_else(|_| "models/tide/".to_string());
    let lookback_days: i64 = std::env::var("FUND_LOOKBACK_DAYS")
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok())
        .unwrap_or(365);

    let s3_client = fund::common::aws::s3_client().await;

    info!(
        data_bucket = data_bucket,
        artifact_bucket = artifact_bucket,
        artifact_prefix = artifact_prefix,
        lookback_days = lookback_days,
        "Starting tide training"
    );

    let equity_bars = load_equity_bars(&s3_client, &data_bucket, lookback_days).await?;
    info!(rows = equity_bars.height(), "Loaded equity bars from S3");

    let equity_details = load_equity_details(&s3_client, &data_bucket).await?;
    info!(
        rows = equity_details.height(),
        "Loaded equity details from S3"
    );

    let consolidated = consolidate_data(equity_bars, equity_details)?;
    let filtered = filter_training_bars(consolidated, MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME)?;
    info!(rows = filtered.height(), "Consolidated and filtered");

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

    let mut config = TrainConfig::default();
    if let Some(epochs) = std::env::var("FUND_EPOCHS")
        .ok()
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse().ok())
    {
        config.epoch_count = epochs;
    }
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

    let staging = tempfile::tempdir()?;
    write_artifact_json(
        staging.path(),
        &fit_result.scaler,
        &fit_result.mappings,
        &parameters,
    )?;
    inner_model.save(staging.path())?;

    let timestamp = Utc::now().format("%Y-%m-%d-%H-%M-%S-%3f").to_string();
    let model_key = format!("{artifact_prefix}{timestamp}/output/model.tar.gz");
    let tar_gz = package_dir_to_tar_gz(staging.path())?;
    upload_artifact(
        &s3_client,
        &artifact_bucket,
        &model_key,
        tar_gz,
        "application/gzip",
    )
    .await?;
    info!(key = model_key, "Uploaded model artifact");

    // Compare this run's CRPS against the recent-run baseline. Drift is
    // reported, never used to block the artifact (matching the Python trainer).
    let current_folder = format!("{artifact_prefix}{timestamp}/");
    let prior_crps = fetch_prior_crps(
        &s3_client,
        &artifact_bucket,
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

    // Date range covered by the lookback window, for model_runs lineage.
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
    let metadata_key = format!("{artifact_prefix}{timestamp}/run_metadata.json");
    upload_artifact(
        &s3_client,
        &artifact_bucket,
        &metadata_key,
        serde_json::to_vec_pretty(&metadata)?,
        "application/json",
    )
    .await?;

    println!("Training complete: artifact s3://{artifact_bucket}/{model_key}");
    println!(
        "Metrics: CRPS={:.6} directional_accuracy={:.4} quantile_coverage={:.4}",
        metrics.crps, metrics.directional_accuracy, metrics.quantile_coverage
    );
    Ok(())
}

/// CRPS values from the most recent prior runs' `run_metadata.json`, newest
/// first. The current run's folder and runs without recorded metrics (e.g.
/// retired tinygrad-era artifacts) are skipped. Failures degrade to an empty
/// history so training never fails because the drift baseline is unavailable.
async fn fetch_prior_crps(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    current_folder: &str,
    run_count: usize,
) -> Vec<f64> {
    let folders = match list_run_folders(s3_client, bucket, prefix).await {
        Ok(folders) => folders,
        Err(listing_error) => {
            warn!("Failed to list prior runs for drift check: {listing_error}");
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
        let artifact_key = format!("{folder}output/model.tar.gz");
        let Some(metadata) = fetch_run_metadata(s3_client, bucket, &artifact_key, None).await
        else {
            continue;
        };
        if let Some(crps) = metadata["metrics"]["crps"].as_f64() {
            prior_crps.push(crps);
        }
    }
    prior_crps
}

/// Read every available daily equity-bar parquet over the lookback window and
/// concatenate them. Missing days (weekends, holidays, gaps) are skipped.
async fn load_equity_bars(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    lookback_days: i64,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let end_date = Utc::now().date_naive();
    let start_date = end_date - Duration::days(lookback_days);

    let mut frames: Vec<LazyFrame> = Vec::new();
    let mut date = start_date;
    while date <= end_date {
        let key = fund::common::aws::date_partitioned_key("data/equity/bars", date);
        if let Ok(response) = s3_client.get_object().bucket(bucket).key(&key).send().await {
            let bytes = response.body.collect().await?.into_bytes();
            let frame = ParquetReader::new(Cursor::new(bytes)).finish()?;
            frames.push(frame.lazy());
        }
        date = match date.succ_opt() {
            Some(next_date) => next_date,
            None => break,
        };
    }

    if frames.is_empty() {
        return Err("No equity-bar parquet files found in the lookback window".into());
    }

    let combined = concat(frames, UnionArgs::default())?.collect()?;
    Ok(combined)
}

async fn load_equity_details(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let key = "data/equity/details/details.csv";
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let bytes = response.body.collect().await?.into_bytes();
    let frame = CsvReader::new(Cursor::new(bytes)).finish()?;
    Ok(frame)
}
