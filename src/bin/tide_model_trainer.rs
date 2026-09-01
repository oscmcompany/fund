//! The trainer: repair, load, train, publish. Runs on its own machine with no database.
//!
//! It fetches its own bars from Massive and writes its own parquet rather than reading the
//! application's nightly export, so the only thing crossing between the two VMs is the finished
//! artifact. The archive it owns is the long-term record of those bars; the application exports
//! only its own tables.
//!
//! Frames are built through [`fund::data::bars::bars_to_dataframe`], shared with the application:
//! if the two diverged, the model would train on columns the inference path does not produce.
//!
//! Nothing here touches a database, and Alpaca is optional — Massive answers bars by date, so there
//! is no symbol list to build. The trading calendar and the boundary table do need a broker key, and
//! both warn and carry on without one: a repair that also requests holidays beats no repair.

use burn::module::AutodiffModule;
use burn::tensor::backend::Backend;
use chrono::Utc;
use tracing::{error, info, warn};

use fund::common::alpaca::{AlpacaCredentials, DataFeed, MarketDataClient, TradingClient};
use fund::common::log::init_tracing;
use fund::common::massive::MassiveClient;
use fund::common::types::SessionDate;
use fund::data::archive;
use fund::data::calendar::TradingCalendar;
use fund::data::details;
use fund::laboratory::dataset;
use fund::laboratory::journal as laboratory;
use fund::models::tide::artifact::{
    candidate_folders_descending, list_run_folders, package_dir_to_tar_gz, upload_artifact,
};
use fund::models::tide::configuration::ModelParameters;
use fund::models::tide::data::{input_feature_size, DatasetKind, Target, TrainingFraction};
use fund::models::tide::drift::{check_drift, DriftStatus};
use fund::models::tide::evaluate::evaluate;
use fund::models::tide::fit::write_artifact_json;
use fund::models::tide::model::TiDEModel;
use fund::models::tide::train::{train, TrainBackend, TrainConfiguration};

const INPUT_LENGTH: usize = 35;
const OUTPUT_LENGTH: usize = 1;
const TRAINING_FRACTION: f64 = 0.8;

/// Calendar days of archive the training window spans by default.
///
/// Also the window stage one repairs, because there is no point archiving a session stage two will
/// never load.
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
    // Two buckets: `data/**` is the shared corpus this reads and repairs, and the model artifact
    // below belongs to the one instance that trained it.
    let archive_bucket = std::env::var("AWS_S3_ARCHIVE_BUCKET_NAME")
        .map_err(|_| "AWS_S3_ARCHIVE_BUCKET_NAME must be set (the shared data/** archive)")?;
    let bucket = std::env::var("AWS_S3_BUCKET_NAME")
        .map_err(|_| "AWS_S3_BUCKET_NAME must be set (this instance's records)")?;
    let artifact_prefix =
        std::env::var("AWS_S3_MODEL_ARTIFACT_PATH").unwrap_or_else(|_| "models/tide/".to_string());
    let lookback_days = read_positive_env("FUND_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS)?;

    let s3_client = fund::common::aws::s3_client().await;

    // One instant for the whole run, resolved once to its Eastern session.
    //
    // The stages disagreed before: the fetch keyed archive partitions by the Eastern date while the
    // load walked them by the UTC date, and a run that crossed UTC midnight -- which one starting at
    // 23:00 UTC does whenever training takes over an hour -- read a window shifted a day off the one
    // it had just written, silently dropping its oldest session. Deriving every date below from this
    // one value is what makes the stages agree regardless of how long training takes.
    let now = Utc::now();
    let session = SessionDate::at(now);

    // One identity for every record this run emits, so a later result can be joined back to the
    // frame it was measured on. A journal that will not open is warned about, never fatal.
    let run_id = uuid::Uuid::new_v4();
    let laboratory_journal = match laboratory::Journal::from_env() {
        Ok(journal) => Some(journal),
        Err(error) => {
            warn!(%error, "No laboratory journal; this run is not recorded");
            None
        }
    };

    info!(
        bucket,
        artifact_prefix,
        lookback_days,
        %session,
        %run_id,
        "Starting tide training"
    );

    // Before the archive is touched, because everything that would diagnose the window costs
    // network round trips first and reports the failure as a sample count.
    validate_lookback_window(lookback_days, session)?;

    //
    // Scanned rather than topped up: every weekday in the window with no partition is fetched, so a
    // night this did not run, a week of downtime, and an empty bucket are one case. The window is
    // the same one stage two reads, because archiving a session training will never load buys
    // nothing. An outage older than that window is a `data:seed:s3` run, which floors at two years.
    //
    // A failure here is logged and stepped over rather than fatal. The archive already holds a
    // year; a night with no new partition trains on 364 days instead of 365, which is a far better
    // outcome than publishing no model because Massive was briefly unreachable.
    let window_start = session.plus_calendar_days(-lookback_days);
    // Fetched here rather than required, unlike the seed binaries. Without it the pass requests
    // holidays and reports them as sessions without data; with a broker key absent entirely that is
    // still a better trade than skipping the repair, which is the same call the boundary stage makes.
    let calendar = match AlpacaCredentials::from_env() {
        Ok(credentials) => match TradingClient::from_env(credentials)
            .fetch_calendar(window_start.date(), session.date())
            .await
        {
            Ok(days) => Some(TradingCalendar::covering(days, window_start, session)),
            Err(error) => {
                warn!(%error, "Trading calendar unavailable; the repair will request holidays too");
                None
            }
        },
        Err(error) => {
            warn!(%error, "No Alpaca credentials; the repair will request holidays too");
            None
        }
    };
    match MassiveClient::from_env() {
        Ok(massive) => {
            match archive::archive_missing_sessions(
                &s3_client,
                &massive,
                &archive_bucket,
                window_start,
                session,
                calendar.as_ref(),
            )
            .await
            {
                Ok(summary) => info!(
                    sessions_written = summary.sessions_written(),
                    sessions_without_data = summary.sessions_without_data(),
                    sessions_failed = summary.sessions_failed().len(),
                    bars_written = summary.output().rows_written(),
                    "Archive repaired"
                ),
                Err(error) => {
                    warn!(%error, "Archive stage failed; training on the existing archive")
                }
            }

            // Refreshed whole rather than topped up, and stepped over on failure like the bars
            // above. Stage two reads it, and refuses to train if it is absent entirely.
            match archive::archive_splits(&s3_client, &massive, &archive_bucket, now).await {
                Ok(splits) => info!(splits, "Splits table refreshed"),
                Err(error) => warn!(%error, "Splits refresh failed; the archive keeps its copy"),
            }
        }
        Err(error) => warn!(%error, "No Massive client; training on the existing archive"),
    }

    // Outside the arm above, because the metadata comes from a CSV compiled into this binary and
    // reaching S3 with it needs no Massive credential. Gating it on one meant a bad key left
    // DuckDB's `training_details` view resolving a stale copy for a reason that had nothing to do
    // with it.
    if let Err(error) =
        archive::archive_details(&s3_client, &archive_bucket, details::embedded_csv()).await
    {
        warn!(%error, "Ticker metadata upload failed; the archive keeps the previous copy");
    }

    // The one thing here that needs a broker rather than a data vendor: Massive reports splits and
    // nothing else, so renames, spinoffs, rights and unit separations come from Alpaca. Refreshed
    // over the training window only — rows outside it survive the merge, so a nightly pass costs
    // one windowed request rather than a full history.
    //
    // Absent credentials warn and continue, like the Massive arm above. The boundary table is not
    // read yet, and a trainer that refused to run without a broker key would be a worse trade than
    // one whose boundary table is a night stale.
    match AlpacaCredentials::from_env() {
        Ok(credentials) => {
            let market_data = MarketDataClient::new(credentials, DataFeed::Sip);
            match archive::archive_boundaries(
                &s3_client,
                &market_data,
                &archive_bucket,
                window_start,
                session,
                now,
            )
            .await
            {
                Ok(boundaries) => info!(boundaries, "Boundary table refreshed"),
                Err(error) => {
                    warn!(%error, "Boundary refresh failed; the archive keeps its copy")
                }
            }
        }
        Err(error) => warn!(%error, "No Alpaca credentials; the boundary table is not refreshed"),
    }

    // The same fraction reaches preprocessing and windowing, because the scaler is fitted on the
    // rows at or before its cutoff. Passing one value here and another below would fit statistics
    // over a window other than the one the model trains on.
    let training_fraction = TrainingFraction::new(TRAINING_FRACTION)?;

    let prepared = dataset::build(
        &s3_client,
        &archive_bucket,
        lookback_days,
        session,
        training_fraction,
        // The published artifact keeps the raw target; the demeaned one is a laboratory experiment
        // until it is shown to produce a model that ranks.
        Target::Raw,
    )
    .await?;
    let fingerprint = prepared.fingerprint;
    let fit_result = prepared.fit;
    info!(
        rows = fingerprint.rows,
        tickers = fingerprint.tickers,
        "Prepared the training frame"
    );

    if let Some(journal) = laboratory_journal.as_ref() {
        // Stamped when it is recorded, not when the run began. Preparation takes minutes, and a run
        // that crosses UTC midnight would otherwise file this into a day the exporter has sealed.
        journal
            .record(
                run_id,
                Utc::now(),
                laboratory::Observation::DatasetBuilt(laboratory::DatasetBuilt {
                    fingerprint,
                    revision: std::env::var("FUND_REVISION").ok(),
                }),
            )
            .await;
    }

    let train_dataset = fit_result.data.get_dataset(
        DatasetKind::Train(training_fraction),
        INPUT_LENGTH,
        OUTPUT_LENGTH,
    )?;
    let validation_dataset = fit_result.data.get_dataset(
        DatasetKind::Validate(training_fraction),
        INPUT_LENGTH,
        OUTPUT_LENGTH,
    )?;
    info!(
        train_samples = train_dataset.len(),
        validation_samples = validation_dataset.len(),
        "Built windowed datasets"
    );
    if train_dataset.is_empty() {
        return Err("No training samples produced from the lookback window".into());
    }
    // Failing here rather than training anyway, because every downstream consequence of an empty
    // validation split is silent. Early stopping falls back to the training loss, `evaluate`
    // short-circuits to `EvaluationMetrics::zero()`, and the metadata below publishes `crps: 0.0`
    // -- the best score possible. That zero then sits in the drift baseline for
    // DRIFT_PRIOR_RUN_COUNT runs, dragging down the mean every later run is measured against. A
    // run that produced no validation data reads as a flawless one.
    if validation_dataset.is_empty() {
        return Err("No validation samples produced from the lookback window".into());
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
        Some(&validation_dataset),
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
    let metrics = evaluate(&inner_model, &validation_dataset, &parameters)?;
    info!(
        continuous_ranked_probability_score = metrics.continuous_ranked_probability_score,
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

    // The run identifier is a sortable timestamp, and the application reads its date prefix to
    // report how stale the model it is serving is. A different format would silently turn that
    // staleness field into "unknown" on every session.
    //
    // Eastern, not UTC, because that date prefix is read back as a *session*. Stamped in UTC it
    // named the session after next whenever training crossed midnight, so the same artifact
    // reported a different staleness depending only on how long it had taken to build.
    //
    // Still sortable: `resolve_artifact_key` picks the lexicographically greatest folder, and
    // Eastern local time is monotonic except across the hour that repeats on the autumn transition
    // -- 01:00 Eastern on a Sunday, which a weekday 23:00 UTC schedule cannot reach.
    let timestamp = fund::data::calendar::eastern_datetime(now)
        .format("%Y-%m-%d-%H-%M-%S-%3f")
        .to_string();
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
    let prior_continuous_ranked_probability_scores =
        fetch_prior_continuous_ranked_probability_scores(
            &s3_client,
            &bucket,
            &artifact_prefix,
            &current_folder,
            DRIFT_PRIOR_RUN_COUNT,
        )
        .await;
    let drift = check_drift(
        metrics.continuous_ranked_probability_score,
        &prior_continuous_ranked_probability_scores,
        DRIFT_MINIMUM_RUNS,
        DRIFT_DEGRADATION_THRESHOLD,
    );
    // Every variant named. A wildcard would send a future `DriftStatus` down the informational path
    // with no compiler error, which is the case where a new signal is most likely to be missed.
    match drift.status {
        DriftStatus::DriftDetected => warn!(
            current_continuous_ranked_probability_score =
                drift.current_continuous_ranked_probability_score,
            baseline_continuous_ranked_probability_score =
                drift.baseline_continuous_ranked_probability_score,
            "Model drift detected"
        ),
        DriftStatus::NoDrift | DriftStatus::InsufficientHistory => info!(
            status = ?drift.status,
            current_continuous_ranked_probability_score = drift.current_continuous_ranked_probability_score,
            baseline_continuous_ranked_probability_score = drift.baseline_continuous_ranked_probability_score,
            prior_runs = prior_continuous_ranked_probability_scores.len(),
            message = drift.message,
            "Drift check complete"
        ),
    }

    let end_date = session;
    let start_date = end_date.plus_calendar_days(-lookback_days);
    let metadata = serde_json::json!({
        "artifact_timestamp": timestamp,
        "input_size": input_size,
        "input_length": INPUT_LENGTH,
        "output_length": OUTPUT_LENGTH,
        "lookback_days": lookback_days,
        "start_date": start_date.to_string(),
        "end_date": end_date.to_string(),
        "epochs_run": losses.len(),
        "final_train_loss": losses.last().copied().unwrap_or_default(),
        "metrics": metrics,
        "train_samples": train_dataset.len(),
        "validation_samples": validation_dataset.len(),
        // `crps` keys are the published spelling and stay that way; see `EvaluationMetrics`. Renaming
        // them would orphan every artifact already in the bucket.
        "drift": {
            "status": drift.status,
            "message": drift.message,
            "baseline_crps": drift.baseline_continuous_ranked_probability_score,
            "prior_runs": prior_continuous_ranked_probability_scores.len(),
        },
    });
    // Retried, because the model tarball is already published by this point. A folder holding a
    // model and no metadata is skipped by `fetch_prior_continuous_ranked_probability_scores`, so the run vanishes from the drift
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
        metrics.continuous_ranked_probability_score,
        metrics.directional_accuracy,
        metrics.quantile_coverage
    );
    Ok(())
}

/// Refuses a lookback window too short for the validation split to hold one window.
///
/// `FUND_LOOKBACK_DAYS` has a floor near 250 that its name does not suggest: the split is a cutoff
/// at [`TRAINING_FRACTION`] of the window's span, and windowing needs `INPUT_LENGTH + OUTPUT_LENGTH`
/// sessions on the far side of it. Sessions are counted from weekends alone, as the archive scan
/// does, so holidays make the count optimistic and the exact guards after windowing still stand.
fn validate_lookback_window(
    lookback_days: i64,
    session: SessionDate,
) -> Result<(), Box<dyn std::error::Error>> {
    let required_sessions = (INPUT_LENGTH + OUTPUT_LENGTH) as i64;
    let validation_days = lookback_days - (lookback_days as f64 * TRAINING_FRACTION) as i64;

    let mut available_sessions = 0;
    let mut date = session.plus_calendar_days(-validation_days);
    while date <= session {
        if !date.is_weekend() {
            available_sessions += 1;
        }
        date = date.plus_calendar_days(1);
    }

    if available_sessions < required_sessions {
        // Weekdays are five days in seven, so this inverts the session count back into the calendar
        // days the whole window needs to leave that many after the cutoff.
        let suggested =
            (required_sessions as f64 * 7.0 / 5.0 / (1.0 - TRAINING_FRACTION)).ceil() as i64;
        return Err(format!(
            "FUND_LOOKBACK_DAYS={lookback_days} leaves about {available_sessions} sessions after \
             the training cutoff, and windowing needs {required_sessions}. Use at least \
             {suggested}."
        )
        .into());
    }
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

/// Reads every available daily partition over the lookback window and concatenates them.
///
/// Missing days — holidays and sessions Massive has never answered for — are skipped rather than
/// treated as errors, which is what lets one unavailable session cost a day of history instead of
/// the whole run. Stage one has already tried to fill everything else.
///
/// Weekends are stepped over rather than requested. Stage one never writes a weekend partition, so
/// a request for one is a guaranteed 404 — about a hundred of them per run. Skipping them here uses
/// the same predicate the archive scan does, which is what keeps reader and writer agreeing about
/// which days the archive can hold at all.
/// The bar columns training consumes, in the types [`fund::data::bars::bars_to_dataframe`] writes.
///
/// Deliberately a subset of what the archive holds: `bar_interval` and `transactions` are written
/// but never trained on, so projecting them away costs nothing.
/// CRPS from the most recent prior runs' `run_metadata.json`, newest first.
///
/// Read from the standalone metadata object rather than from inside the artifact tarball, so a
/// baseline can be built without downloading every prior run. An unreadable run is skipped and the
/// rest are kept; only an unlistable prefix yields nothing. Either way a drift baseline that cannot
/// be read is not a reason to fail a training run.
async fn fetch_prior_continuous_ranked_probability_scores(
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

    let mut prior_continuous_ranked_probability_scores = Vec::new();
    for folder in candidate_folders_descending(folders) {
        if prior_continuous_ranked_probability_scores.len() >= run_count {
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
        if let Some(continuous_ranked_probability_score) = metadata["metrics"]["crps"].as_f64() {
            prior_continuous_ranked_probability_scores.push(continuous_ranked_probability_score);
        }
    }
    prior_continuous_ranked_probability_scores
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

    fn session_on(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    /// The value in the operator notes for a quick local run. It reaches stage three and fails there
    /// on a sample count, which is the failure this guard exists to move to the front of the run.
    #[test]
    fn test_a_short_lookback_is_rejected() {
        let error = validate_lookback_window(10, session_on(2026, 8, 11)).unwrap_err();
        assert!(
            error.to_string().contains("Use at least"),
            "the message must name a workable window, got {error}"
        );
    }

    #[test]
    fn test_the_default_lookback_is_accepted() {
        assert!(validate_lookback_window(DEFAULT_LOOKBACK_DAYS, session_on(2026, 8, 11)).is_ok());
    }

    /// The suggestion the rejection prints has to be a window the same guard then accepts, or it
    /// sends the operator round the loop a second time.
    #[test]
    fn test_the_suggested_lookback_is_accepted() {
        let session = session_on(2026, 8, 11);
        let message = validate_lookback_window(10, session)
            .unwrap_err()
            .to_string();
        let suggested: i64 = message
            .rsplit_once("Use at least ")
            .and_then(|(_, tail)| tail.trim_end_matches('.').parse().ok())
            .expect("the rejection must end with a suggested day count");
        assert!(validate_lookback_window(suggested, session).is_ok());
    }
}
