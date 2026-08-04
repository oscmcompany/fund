//! The trainer: fetch, archive, train, publish. Runs on its own machine with no database.
//!
//! It fetches its own bars from Massive and writes its own parquet rather than reading the
//! application's nightly export, so the only thing crossing between the two VMs is the finished
//! artifact and a failed export cannot cost the next day's model.
//!
//! Frames are built through [`fund::data::bars::bars_to_dataframe`], shared with the application:
//! if the two diverged, the model would train on columns the inference path does not produce.
//!
//! Nothing here touches a database or Alpaca — Massive answers by date, so there is no symbol list
//! to build and no broker to ask for one.

use std::io::Cursor;

use aws_sdk_s3::operation::get_object::GetObjectError;
use burn::module::AutodiffModule;
use burn::tensor::backend::Backend;
use chrono::{Duration, NaiveDate, Utc};
use polars::prelude::*;
use tracing::{error, info, warn};

use fund::common::aws::date_partitioned_key;
use fund::common::massive::MassiveClient;
use fund::common::observability::init_tracing;
use fund::common::types::{MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME};
use fund::data::bars;
use fund::data::calendar::{eastern_date, is_weekend};
use fund::data::details;
use fund::models::tide::artifact::{
    candidate_folders_descending, list_run_folders, package_dir_to_tar_gz, upload_artifact,
};
use fund::models::tide::configuration::ModelParameters;
use fund::models::tide::data::input_feature_size;
use fund::models::tide::drift::{check_drift, DriftStatus};
use fund::models::tide::evaluate::evaluate;
use fund::models::tide::fit::{filter_training_bars, fit, write_artifact_json};
use fund::models::tide::model::TiDEModel;
use fund::models::tide::predict::consolidate_data;
use fund::models::tide::train::{train, TrainBackend, TrainConfiguration};

const INPUT_LENGTH: usize = 35;
const OUTPUT_LENGTH: usize = 1;
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
/// Three rather than one: a night the trainer did not run leaves a hole nothing else fills. The
/// merge below makes the overlap cheap, and guards against a later response omitting rows an
/// earlier one had.
const FETCH_LOOKBACK_SESSIONS: i64 = 3;

/// Calendar days of archive the training window spans by default.
const DEFAULT_LOOKBACK_DAYS: i64 = 365;

/// Attempts to publish `run_metadata.json` before giving up.
const METADATA_UPLOAD_ATTEMPTS: usize = 3;

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
        // `std::process::exit` runs no destructors, so the non-blocking appender's guard would
        // never drop and its buffered lines would be lost -- exactly when the failure log matters.
        drop(_tracing_guard);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (the equity-bar data bucket)")?;
    let artifact_prefix =
        std::env::var("AWS_S3_MODEL_ARTIFACT_PATH").unwrap_or_else(|_| "models/tide/".to_string());
    let lookback_days = read_positive_env("FUND_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS)?;

    let s3_client = fund::common::aws::s3_client().await;

    info!(
        bucket,
        artifact_prefix, lookback_days, "Starting tide training"
    );

    // --- stage one: fetch and archive ---
    //
    // A failure here is logged and stepped over rather than fatal. The archive already holds a
    // year; a night with no new partition trains on 364 days instead of 365, which is a far better
    // outcome than publishing no model because Massive was briefly unreachable.
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
    let model = TiDEModel::<TrainBackend>::new(
        &device,
        input_size,
        parameters.hidden_size(),
        parameters.encoder_layer_count(),
        parameters.decoder_layer_count(),
        parameters.output_length(),
        parameters.quantiles().len(),
        parameters.dropout_rate(),
    );

    let configuration = training_configuration()?;
    let (best_model, losses) = train(
        model,
        &train_dataset,
        Some(&valid_dataset),
        &parameters,
        &configuration,
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
    // Every variant named. A wildcard would send a future `DriftStatus` down the informational path
    // with no compiler error, which is the case where a new signal is most likely to be missed.
    match drift.status {
        DriftStatus::DriftDetected => warn!(
            current_crps = drift.current_crps,
            baseline_crps = drift.baseline_crps,
            "Model drift detected"
        ),
        DriftStatus::NoDrift | DriftStatus::InsufficientHistory => info!(
            status = ?drift.status,
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
    // Retried, because the model tarball is already published by this point. A folder holding a
    // model and no metadata is skipped by `fetch_prior_crps`, so the run vanishes from the drift
    // baseline for the next DRIFT_PRIOR_RUN_COUNT executions -- and with DRIFT_MINIMUM_RUNS at three,
    // repeated partial publishes suppress drift reporting entirely.
    let metadata_key = format!("{current_folder}run_metadata.json");
    let metadata_body = serde_json::to_vec_pretty(&metadata)?;
    let mut published = false;
    for attempt in 1..=METADATA_UPLOAD_ATTEMPTS {
        match upload_artifact(
            &s3_client,
            &bucket,
            &metadata_key,
            metadata_body.clone(),
            "application/json",
        )
        .await
        {
            Ok(()) => {
                published = true;
                break;
            }
            Err(error) => warn!(
                attempt,
                attempts = METADATA_UPLOAD_ATTEMPTS,
                %error,
                "Publishing run metadata failed"
            ),
        }
    }
    if !published {
        error!(
            folder = current_folder,
            "Run metadata could not be published; this folder holds a model with no metadata and \
             will be skipped by the drift baseline"
        );
        return Err(format!("failed to publish run metadata for {current_folder}").into());
    }

    println!("Training complete: artifact s3://{bucket}/{model_key}");
    println!(
        "Metrics: CRPS={:.6} directional_accuracy={:.4} quantile_coverage={:.4}",
        metrics.crps, metrics.directional_accuracy, metrics.quantile_coverage
    );
    Ok(())
}

/// Reads a positive integer from the environment, falling back only when the variable is absent.
///
/// A present-but-unparsable value is an error rather than a fallback. This is a batch job an
/// operator starts: a typo in a deployment variable should stop it, not silently run it with a
/// default nobody chose. That is the opposite of `SizingParameters::from_env`, which warns and
/// continues -- and deliberately so, because the service has to keep running.
fn read_positive_env(variable: &str, default: i64) -> Result<i64, Box<dyn std::error::Error>> {
    let Some(raw) = std::env::var(variable)
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(default);
    };
    let value: i64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("{variable} must be a positive integer, got {raw:?}"))?;
    if value <= 0 {
        return Err(format!("{variable} must be greater than zero, got {value}").into());
    }
    Ok(value)
}

/// The training configuration, with `FUND_EPOCHS` applied over the defaults.
///
/// Rebuilt through the validated constructor rather than by assigning a field, so an epoch count of
/// zero is rejected here with a message rather than reaching the training loop, which would run no
/// epochs and publish an untrained model that looks exactly like a trained one.
fn training_configuration() -> Result<TrainConfiguration, Box<dyn std::error::Error>> {
    let defaults = TrainConfiguration::default();
    let epoch_count = read_positive_env("FUND_EPOCHS", defaults.epoch_count() as i64)? as usize;

    Ok(TrainConfiguration::new(
        defaults.learning_rate(),
        epoch_count,
        defaults.batch_size(),
        defaults.early_stopping_patience(),
        defaults.min_delta(),
    )?)
}

/// Fetches the most recent sessions' bars from Massive and writes them to the archive.
///
/// Returns rows written. No universe and no symbol list: the endpoint is asked for a date and
/// answers with every stock that traded. `filter_training_bars` applies the thresholds further
/// down, so pre-filtering here would only decide which names the model may ever have seen — and
/// building a list from `fetch_tradable_assets` would pin the archive to whatever Alpaca lists
/// *today*, which is the survivorship problem.
async fn fetch_and_archive(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let client = MassiveClient::from_env()?;

    let end = most_recent_weekday(eastern_date(Utc::now()));
    let start = end - Duration::days(FETCH_LOOKBACK_SESSIONS * 2);
    let dates: Vec<NaiveDate> = (0..=(end - start).num_days())
        .map(|offset| start + Duration::days(offset))
        .collect();
    info!(%start, %end, dates = dates.len(), "Fetching session bars from Massive");

    let fetched = bars::fetch_daily_bars(&client, &dates).await;
    if !fetched.dates_failed.is_empty() {
        // Logged rather than fatal, as before: a failed session costs one day of history, and the
        // next run's overlap window covers it.
        warn!(
            dates_failed = ?fetched.dates_failed,
            "Some sessions could not be fetched; the archive will have gaps until the next run"
        );
    }
    if fetched.bars.is_empty() {
        return Ok(0);
    }

    // One partition per session date, so the archive stays date-partitioned and the loader below
    // can walk it a day at a time.
    let mut by_date: std::collections::BTreeMap<NaiveDate, Vec<_>> =
        std::collections::BTreeMap::new();
    for bar in fetched.bars {
        by_date
            .entry(eastern_date(bar.timestamp()))
            .or_default()
            .push(bar);
    }

    let mut written = 0;
    for (date, bars_for_date) in by_date {
        let key = date_partitioned_key(BAR_ARCHIVE_PREFIX, date);
        let fetched_frame = bars::bars_to_dataframe(&bars_for_date)?;

        // Merged with whatever the partition already holds, not written over it. This matters less
        // than it did — the grouped endpoint answers by date, so a symbol delisted today still
        // comes back for the days it was trading — but a plain overwrite would still discard
        // anything a later response happens to omit, and merging costs nothing.
        let mut frame = match read_partition(s3_client, bucket, &key).await? {
            Some(existing) => merge_partitions(existing, fetched_frame)?,
            None => fetched_frame,
        };

        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer).finish(&mut frame)?;

        upload_artifact(s3_client, bucket, &key, buffer, "application/octet-stream").await?;
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

/// Reads one archived partition, distinguishing a missing object from a failed request.
///
/// `Ok(None)` means the partition genuinely does not exist yet — the documented skip. Every other
/// failure propagates: a credential error, a throttle, or a network fault silently treated as
/// "missing" would shorten the training window without any signal, and the model would just be
/// slightly worse for reasons nothing recorded.
async fn read_partition(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<Option<DataFrame>, Box<dyn std::error::Error>> {
    let response = match s3_client.get_object().bucket(bucket).key(key).send().await {
        Ok(response) => response,
        Err(error) => {
            return match error.into_service_error() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                other => Err(other.into()),
            }
        }
    };
    let bytes = response.body.collect().await?.into_bytes();
    Ok(Some(ParquetReader::new(Cursor::new(bytes)).finish()?))
}

/// Combines an existing partition with a freshly fetched one, newest row winning per key.
///
/// The key is `(ticker, bar_interval, timestamp)` — the same primary key `equity_bars` uses, so the
/// archive and the table agree about what constitutes a duplicate. The fetched rows are appended
/// last and `UniqueKeepStrategy::Last` keeps them, which makes a re-fetch a correction rather than a
/// duplicate.
fn merge_partitions(
    existing: DataFrame,
    fetched: DataFrame,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let combined = concat([existing.lazy(), fetched.lazy()], UnionArgs::default())?
        .unique_stable(
            Some(polars::prelude::Selector::ByName {
                names: vec![
                    PlSmallStr::from("ticker"),
                    PlSmallStr::from("bar_interval"),
                    PlSmallStr::from("timestamp"),
                ]
                .into(),
                strict: false,
            }),
            UniqueKeepStrategy::Last,
        )
        .collect()?;
    Ok(combined)
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
        if let Some(frame) = read_partition(s3_client, bucket, &key).await? {
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
    Ok(concat(frames, UnionArgs::default())?.collect()?)
}

/// CRPS from the most recent prior runs' `run_metadata.json`, newest first.
///
/// Read from the standalone metadata object rather than from inside the artifact tarball, so a
/// baseline can be built without downloading every prior run. An unreadable run is skipped and the
/// rest are kept; only an unlistable prefix yields nothing. Either way a drift baseline that cannot
/// be read is not a reason to fail a training run.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    /// RAII guard restoring one environment variable on drop, so an assertion failure cannot leave
    /// a value in place for the next `#[serial]` test in the file.
    struct EnvironmentVariableGuard {
        key: &'static str,
        original: Option<String>,
    }

    impl EnvironmentVariableGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, original }
        }

        fn unset(key: &'static str) -> Self {
            let original = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, original }
        }
    }

    impl Drop for EnvironmentVariableGuard {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("test date must be valid")
    }

    #[test]
    #[serial]
    fn test_an_absent_variable_uses_the_default() {
        let _guard = EnvironmentVariableGuard::unset("FUND_LOOKBACK_DAYS");
        assert_eq!(
            read_positive_env("FUND_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS).unwrap(),
            DEFAULT_LOOKBACK_DAYS
        );
    }

    /// An empty value is a variable that was set to nothing, which reads the same as unset. That is
    /// the one fallback worth keeping, because deployment tooling writes empty strings routinely.
    #[test]
    #[serial]
    fn test_an_empty_variable_uses_the_default() {
        let _guard = EnvironmentVariableGuard::set("FUND_LOOKBACK_DAYS", "   ");
        assert_eq!(read_positive_env("FUND_LOOKBACK_DAYS", 365).unwrap(), 365);
    }

    /// A typo must stop the run. Falling back would make a misconfigured deployment
    /// indistinguishable from an unconfigured one, and the model would train on a window nobody
    /// chose.
    #[test]
    #[serial]
    fn test_an_unparsable_variable_is_an_error() {
        let _guard = EnvironmentVariableGuard::set("FUND_LOOKBACK_DAYS", "3o5");
        assert!(read_positive_env("FUND_LOOKBACK_DAYS", 365).is_err());
    }

    #[test]
    #[serial]
    fn test_a_non_positive_variable_is_an_error() {
        for value in ["0", "-5"] {
            let _guard = EnvironmentVariableGuard::set("FUND_LOOKBACK_DAYS", value);
            assert!(
                read_positive_env("FUND_LOOKBACK_DAYS", 365).is_err(),
                "{value} must be rejected"
            );
        }
    }

    /// The docstring on `training_configuration` claims a zero epoch count is rejected with a message.
    /// Nothing proved that until now — and an accepted zero would publish an untrained model that
    /// looks exactly like a trained one.
    #[test]
    #[serial]
    fn test_a_zero_epoch_count_is_rejected() {
        let _guard = EnvironmentVariableGuard::set("FUND_EPOCHS", "0");
        assert!(training_configuration().is_err());
    }

    #[test]
    #[serial]
    fn test_a_malformed_epoch_count_is_rejected() {
        let _guard = EnvironmentVariableGuard::set("FUND_EPOCHS", "twenty");
        assert!(training_configuration().is_err());
    }

    #[test]
    #[serial]
    fn test_an_absent_epoch_count_keeps_the_default() {
        let _guard = EnvironmentVariableGuard::unset("FUND_EPOCHS");
        assert_eq!(
            training_configuration().unwrap().epoch_count(),
            TrainConfiguration::default().epoch_count()
        );
    }

    /// Alpaca publishes no bars on a weekend, so the fetch window has to end on a weekday or the
    /// last partition of the run is empty.
    #[test]
    fn test_most_recent_weekday_walks_back_over_the_weekend() {
        // 2026-08-01 is a Saturday, 2026-08-02 a Sunday, 2026-08-03 a Monday.
        assert_eq!(most_recent_weekday(date(2026, 8, 1)), date(2026, 7, 31));
        assert_eq!(most_recent_weekday(date(2026, 8, 2)), date(2026, 7, 31));
        assert_eq!(most_recent_weekday(date(2026, 8, 3)), date(2026, 8, 3));
        assert_eq!(most_recent_weekday(date(2026, 7, 31)), date(2026, 7, 31));
    }
}
