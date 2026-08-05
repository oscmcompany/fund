//! TiDE model artifacts: written on the trainer, read on the application.
//!
//! Nothing orders the write against the read — the trainer has no database. Running a session
//! against yesterday's model is normal, reported as the artifact age in the `predictions_completed`
//! payload rather than as a failure.
//!
//! The tar is flat: one entry per file, no directory prefix. Both sides live here because that
//! format is the only contract between them.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use burn::backend::NdArray;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::{debug, info, warn};

use crate::models::tide::configuration::ModelParameters;
use crate::models::tide::data::{FeatureMappings, Scaler};
use crate::models::tide::model::TiDEModel;

// --------------------------------------------------------------------------
// The loaded artifact
// --------------------------------------------------------------------------

pub struct ModelState {
    /// The loaded weights, behind a mutex because the forward pass mutates them.
    ///
    /// Burn stores each parameter as a `core::cell::OnceCell`, and `Param::val` initializes it
    /// through `get_or_init` — so a forward pass writes through a shared reference and
    /// `TiDEModel<NdArray>` is therefore `!Sync`. The mutex is not here to arbitrate contention;
    /// there is exactly one caller. It is here so this type is `Sync` by construction, which the
    /// prediction handler needs: it holds a `&ModelState` across an await and `JoinSet::spawn`
    /// requires the resulting future to be `Send`.
    model: Mutex<TiDEModel<NdArray>>,
    parameters: ModelParameters,
    scaler: Scaler,
    mappings: FeatureMappings,
    artifact_key: String,
    /// Training run id: the timestamp segment of the artifact key. Written to
    /// `equity_predictions.model_run_id`, which is how a prediction is traced back to the artifact
    /// that produced it.
    run_id: String,
    load_timestamp: i64,
}

impl ModelState {
    /// Constructs a `ModelState` from a fully loaded artifact.
    ///
    /// The column lists the artifact was fitted with are no longer carried here. They are checked
    /// against this build's constants inside [`crate::models::tide::data::Scaler::load`] and then
    /// dropped: verifying them is what they were for, nothing read them afterwards, and keeping a
    /// copy that is provably equal to the constants invited a future caller to trust the copy.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        model: TiDEModel<NdArray>,
        parameters: ModelParameters,
        scaler: Scaler,
        mappings: FeatureMappings,
        artifact_key: String,
        run_id: String,
        load_timestamp: i64,
    ) -> Self {
        Self {
            model: Mutex::new(model),
            parameters,
            scaler,
            mappings,
            artifact_key,
            run_id,
            load_timestamp,
        }
    }

    /// Borrows the model for a forward pass.
    ///
    /// A poisoned lock is recovered from rather than propagated. Poisoning here means a previous
    /// forward pass panicked, but the weights carry no invariant that a panic could leave half
    /// written: the only mutation is `OnceCell::get_or_init` caching a materialized tensor, and a
    /// panic during initialization leaves the cell empty rather than partly filled. Failing every
    /// later session because one earlier one panicked would be the worse outcome.
    pub fn model(&self) -> MutexGuard<'_, TiDEModel<NdArray>> {
        self.model.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn parameters(&self) -> &ModelParameters {
        &self.parameters
    }

    pub fn scaler(&self) -> &Scaler {
        &self.scaler
    }

    pub fn mappings(&self) -> &FeatureMappings {
        &self.mappings
    }

    pub fn artifact_key(&self) -> &str {
        &self.artifact_key
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn load_timestamp(&self) -> i64 {
        self.load_timestamp
    }
}

/// `ModelState` must stay `Send + Sync` — the prediction handler holds a `&ModelState` across an
/// await inside a future that `JoinSet::spawn` requires to be `Send`. That used to be asserted with
/// `unsafe impl`; it is now a property the compiler derives from the `Mutex` around the model, and
/// this is what makes the difference visible. Removing the mutex fails here rather than in
/// `bin/fund.rs`, several call layers from the cause.
#[allow(dead_code)]
fn model_state_is_send_and_sync() {
    fn require<T: Send + Sync>() {}
    require::<ModelState>();
}

// --------------------------------------------------------------------------
// Reading: resolve, download, load
// --------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("No artifacts found")]
    NoArtifacts,
    #[error("S3 error: {0}")]
    S3(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Model load error: {0}")]
    ModelLoad(String),
}

/// Derive the training run id from an artifact key. For the canonical
/// `<prefix>/<run_id>/output/model.tar.gz` layout this returns `<run_id>`;
/// otherwise it falls back to the last path segment.
pub fn run_id_from_artifact_key(artifact_key: &str) -> String {
    if let Some(prefix) = artifact_key.strip_suffix("/output/model.tar.gz") {
        return prefix.rsplit('/').next().unwrap_or(prefix).to_string();
    }
    artifact_key
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(artifact_key)
        .to_string()
}

/// Order run folders newest-first for artifact resolution. The trainer's
/// timestamped folder names sort lexicographically by recency, and callers try
/// each candidate in turn so an incomplete newest folder (trainer crashed
/// before uploading `output/model.tar.gz`) falls back to the previous run.
pub fn candidate_folders_descending(prefixes: Vec<String>) -> Vec<String> {
    let mut folders = prefixes;
    folders.sort();
    folders.reverse();
    folders
}

/// List the training-run folders (S3 common prefixes) under `prefix`, e.g.
/// `models/tide/2026-06-10-01-00-07-377/`. Paginates so more than 1000 runs
/// are still all visible. Used by artifact resolution and by the trainer's
/// drift check, which compares against recent runs' metadata.
pub async fn list_run_folders(
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>, ArtifactError> {
    let mut folders: Vec<String> = Vec::new();
    let mut pages = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let page = page.map_err(|e| ArtifactError::S3(e.to_string()))?;
        folders.extend(
            page.common_prefixes()
                .iter()
                .filter_map(|p| p.prefix().map(String::from)),
        );
    }
    Ok(folders)
}

pub async fn resolve_artifact_key(
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
    version: &str,
    local_dir: Option<&Path>,
) -> Result<String, ArtifactError> {
    if let Some(local_dir) = local_dir {
        return resolve_local_artifact_key(local_dir, prefix, version);
    }

    if version != "latest" {
        return Ok(format!("{prefix}{version}/output/model.tar.gz"));
    }

    let folders = list_run_folders(s3_client, bucket, prefix).await?;

    // Try folders newest-first and verify the model object actually exists, so
    // an incomplete run (trainer crashed before uploading) falls back to the
    // previous good artifact instead of being retried forever.
    for folder in candidate_folders_descending(folders) {
        let key = format!("{folder}output/model.tar.gz");
        match s3_client
            .head_object()
            .bucket(bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => {
                debug!(key = key, "Resolved latest artifact key");
                return Ok(key);
            }
            Err(error) => {
                // Only a genuine absence justifies reaching for an older artifact. Treating every
                // failure as "not there" means an expired credential or a transient network fault
                // silently resolves yesterday's model and the day trades on it, reported as a
                // normal run. A 404 is the trainer not having finished; anything else is a problem
                // with this process, and it is raised.
                let is_missing = error
                    .as_service_error()
                    .is_some_and(|service_error| service_error.is_not_found());
                if !is_missing {
                    return Err(ArtifactError::S3(format!("failed to check {key}: {error}")));
                }
                debug!(key = key, "Run folder has no model artifact, trying older");
            }
        }
    }

    Err(ArtifactError::NoArtifacts)
}

fn resolve_local_artifact_key(
    local_dir: &Path,
    prefix: &str,
    version: &str,
) -> Result<String, ArtifactError> {
    if version != "latest" {
        return Ok(format!("{prefix}{version}/output/model.tar.gz"));
    }

    let mut entries: Vec<PathBuf> = std::fs::read_dir(local_dir)
        .map_err(|_| ArtifactError::NoArtifacts)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();

    entries.sort();

    // Scan newest → oldest, returning the first directory that contains a
    // valid artifact. This ensures that a partially-uploaded newer run does
    // not shadow an older run that is fully available.
    for entry in entries.iter().rev() {
        let model_path = entry.join("output").join("model.tar.gz");
        if model_path.exists() {
            return Ok(model_path.to_string_lossy().to_string());
        }
        let params_path = entry.join("tide_parameters.json");
        if params_path.exists() {
            return Ok(entry.to_string_lossy().to_string());
        }
    }

    Err(ArtifactError::NoArtifacts)
}

pub async fn download_and_load_model(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    local_dir: Option<&Path>,
) -> Result<ModelState, ArtifactError> {
    let extract_dir = tempfile::tempdir()?;
    let extract_path = extract_dir.path();

    if let Some(local_dir) = local_dir {
        let local_path = Path::new(key);
        if local_path.is_dir() {
            // Already extracted directory
            return load_model_from_directory(local_path, key);
        }

        let tar_path = if local_path.exists() {
            local_path.to_path_buf()
        } else {
            local_dir.join(key)
        };

        if tar_path.exists() {
            extract_tar_gz(&tar_path, extract_path)?;
            return load_model_from_directory(extract_path, key);
        }
    }

    info!(
        bucket = bucket,
        key = key,
        "Downloading model artifact from S3"
    );

    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| ArtifactError::S3(e.to_string()))?;

    let bytes = response
        .body
        .collect()
        .await
        .map_err(|e| ArtifactError::S3(e.to_string()))?
        .into_bytes();

    let tmp_file = extract_path.join("model.tar.gz");
    std::fs::write(&tmp_file, &bytes)?;
    extract_tar_gz(&tmp_file, extract_path)?;

    load_model_from_directory(extract_path, key)
}

fn extract_tar_gz(tar_path: &Path, dest: &Path) -> Result<(), ArtifactError> {
    let file = std::fs::File::open(tar_path)?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();

        // Every component must be a plain name. Rejecting only `..` was not enough: `Path::join`
        // discards its base when the argument is absolute, so an entry named `/etc/cron.d/anything`
        // would have been written to that absolute path rather than inside the extraction
        // directory. Requiring `Normal` components rejects absolute paths, drive prefixes, `.` and
        // `..` in one condition, and the archive this reads is flat by construction so nothing
        // legitimate is excluded.
        if !path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
        {
            warn!(
                entry = %path.display(),
                "Rejected an artifact entry whose path is not a plain relative name"
            );
            continue;
        }

        let dest_path = dest.join(&path);
        if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&dest_path)?;
        } else {
            if let Some(parent) = dest_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut output = std::fs::File::create(&dest_path)?;
            std::io::copy(&mut entry, &mut output)?;
        }
    }

    Ok(())
}

fn load_model_from_directory(dir: &Path, artifact_key: &str) -> Result<ModelState, ArtifactError> {
    let parameters_path = dir.join("tide_parameters.json");
    let parameters = crate::models::tide::configuration::ModelParameters::load(&parameters_path)
        .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;

    let scaler_path = dir.join("tide_data_scaler.json");
    let scaler = crate::models::tide::data::Scaler::load(&scaler_path)
        .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;

    let mappings_path = dir.join("tide_data_mappings.json");
    let mappings_content = std::fs::read_to_string(&mappings_path)
        .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;
    let mappings: crate::models::tide::data::FeatureMappings =
        serde_json::from_str(&mappings_content)
            .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;

    let quantile_count = parameters.quantiles().len();
    let model = crate::models::tide::model::TiDEModel::load(
        dir,
        parameters.input_size(),
        parameters.hidden_size(),
        parameters.encoder_layer_count(),
        parameters.decoder_layer_count(),
        parameters.output_length(),
        quantile_count,
        parameters.dropout_rate(),
    )
    .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;

    let load_timestamp = Utc::now().timestamp();

    info!(
        artifact_key = artifact_key,
        input_size = parameters.input_size(),
        hidden_size = parameters.hidden_size(),
        "Model loaded successfully"
    );

    Ok(ModelState::new(
        model,
        parameters,
        scaler,
        mappings,
        artifact_key.to_string(),
        run_id_from_artifact_key(artifact_key),
        load_timestamp,
    ))
}

// --------------------------------------------------------------------------
// Writing: package and upload
// --------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ArtifactWriteError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("S3 error: {0}")]
    S3(String),
}

/// Gzip-tar every file directly under `directory` into an in-memory buffer.
/// Entries use bare file names so the archive is flat.
pub fn package_dir_to_tar_gz(directory: &Path) -> Result<Vec<u8>, ArtifactWriteError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    {
        let mut builder = tar::Builder::new(&mut encoder);
        // Propagate entry errors: silently skipping an unreadable entry would
        // package an incomplete artifact that only fails much later, at load.
        let mut files: Vec<std::path::PathBuf> = Vec::new();
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_file() {
                files.push(path);
            }
        }
        files.sort();
        for path in files {
            let name = path
                .file_name()
                .ok_or_else(|| std::io::Error::other("artifact file has no name"))?;
            builder.append_path_with_name(&path, name)?;
        }
        builder.finish()?;
    }
    Ok(encoder.finish()?)
}

/// Upload a byte payload to `s3://{bucket}/{key}`.
pub async fn upload_artifact(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    content_type: &str,
) -> Result<(), ArtifactWriteError> {
    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .content_type(content_type)
        .send()
        .await
        .map_err(|e| ArtifactWriteError::S3(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Path::join` discards its base when given an absolute path, so an entry named `/tmp/...`
    /// would escape the extraction directory. Rejecting non-`Normal` components is what stops it.
    ///
    /// The header name is written directly because the `tar` crate refuses to *produce* an absolute
    /// entry path through its safe API — which is precisely why the reading side has to defend
    /// itself against archives produced by something else.
    #[test]
    fn test_extract_rejects_an_absolute_entry_path() {
        let escape_target = std::env::temp_dir().join("fund-artifact-escape-probe.txt");
        let _ = std::fs::remove_file(&escape_target);

        let staging = tempfile::tempdir().unwrap();
        let archive_path = staging.path().join("evil.tar.gz");
        {
            let file = std::fs::File::create(&archive_path).unwrap();
            let encoder = GzEncoder::new(file, Compression::default());
            let mut builder = tar::Builder::new(encoder);

            let contents = b"owned";
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            {
                let absolute = escape_target.to_string_lossy().into_owned();
                let name_field = &mut header.as_gnu_mut().unwrap().name;
                let bytes = absolute.as_bytes();
                assert!(
                    bytes.len() < name_field.len(),
                    "probe path must fit the name field"
                );
                name_field[..bytes.len()].copy_from_slice(bytes);
            }
            header.set_cksum();
            builder.append(&header, &contents[..]).unwrap();
            builder.into_inner().unwrap().finish().unwrap();
        }

        let destination = tempfile::tempdir().unwrap();
        extract_tar_gz(&archive_path, destination.path()).expect("extraction must not fail");

        let escaped = escape_target.exists();
        let _ = std::fs::remove_file(&escape_target);
        assert!(
            !escaped,
            "an absolute entry path must not be written outside the destination"
        );
    }

    /// The flat, well-formed archive the trainer produces must still extract.
    #[test]
    fn test_extract_accepts_a_flat_archive() {
        let staging = tempfile::tempdir().unwrap();
        std::fs::write(staging.path().join("tide_parameters.json"), b"{}").unwrap();
        let bytes = package_dir_to_tar_gz(staging.path()).unwrap();

        let archive_path = staging.path().join("model.tar.gz");
        std::fs::write(&archive_path, &bytes).unwrap();

        let destination = tempfile::tempdir().unwrap();
        extract_tar_gz(&archive_path, destination.path()).unwrap();

        assert!(destination.path().join("tide_parameters.json").exists());
    }

    #[test]
    fn test_candidate_folders_descending_orders_newest_first() {
        let folders = candidate_folders_descending(vec![
            "models/tide/2026-06-01-00-00-00-000/".to_string(),
            "models/tide/2026-06-09-16-21-25-195/".to_string(),
            "models/tide/2026-06-05-12-00-00-000/".to_string(),
        ]);
        assert_eq!(
            folders,
            vec![
                "models/tide/2026-06-09-16-21-25-195/",
                "models/tide/2026-06-05-12-00-00-000/",
                "models/tide/2026-06-01-00-00-00-000/",
            ]
        );
    }

    #[test]
    fn test_resolve_local_no_dir() {
        let result =
            resolve_local_artifact_key(Path::new("/nonexistent"), "artifacts/tide/", "latest");
        assert!(result.is_err());
    }

    #[test]
    fn test_run_id_from_artifact_key_canonical() {
        assert_eq!(
            run_id_from_artifact_key("models/tide/2026-06-09-16-21-25-195/output/model.tar.gz"),
            "2026-06-09-16-21-25-195"
        );
    }

    #[test]
    fn test_run_id_from_artifact_key_fallback() {
        assert_eq!(run_id_from_artifact_key("some/dir/run-x"), "run-x");
        assert_eq!(run_id_from_artifact_key("run-y"), "run-y");
    }

    #[test]
    fn test_resolve_explicit_version() {
        let result = resolve_local_artifact_key(Path::new("/tmp"), "artifacts/tide/", "2024-01-01");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "artifacts/tide/2024-01-01/output/model.tar.gz"
        );
    }

    #[test]
    fn test_run_id_from_artifact_key_empty_string() {
        // An empty input should not panic; it falls back to the full string.
        let result = run_id_from_artifact_key("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_run_id_from_artifact_key_trailing_slash() {
        // A plain directory name with a trailing slash must trim the slash.
        let result = run_id_from_artifact_key("some/run-folder/");
        assert_eq!(result, "run-folder");
    }

    #[test]
    fn test_run_id_from_artifact_key_no_slash() {
        // A bare filename with no slashes must return the whole string.
        let result = run_id_from_artifact_key("run-2026-01-01");
        assert_eq!(result, "run-2026-01-01");
    }

    #[test]
    fn test_candidate_folders_descending_single_element() {
        let folders = candidate_folders_descending(vec!["models/tide/2026-06-01/".to_string()]);
        assert_eq!(folders, vec!["models/tide/2026-06-01/"]);
    }

    #[test]
    fn test_candidate_folders_descending_empty() {
        let folders = candidate_folders_descending(vec![]);
        assert!(folders.is_empty());
    }

    #[test]
    fn test_candidate_folders_descending_already_sorted_descending() {
        // Providing folders newest-first must not change the order.
        let input = vec![
            "models/tide/2026-06-09/".to_string(),
            "models/tide/2026-06-05/".to_string(),
            "models/tide/2026-06-01/".to_string(),
        ];
        let result = candidate_folders_descending(input.clone());
        assert_eq!(result, input);
    }

    #[test]
    fn test_resolve_local_artifact_key_explicit_version_ignores_dir_contents() {
        // With an explicit version the directory is not even opened.
        let result = resolve_local_artifact_key(
            Path::new("/nonexistent"),
            "models/tide/",
            "2026-06-10-01-00-07",
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "models/tide/2026-06-10-01-00-07/output/model.tar.gz"
        );
    }

    #[test]
    fn test_resolve_local_artifact_key_latest_with_empty_dir() {
        // A temporary directory with no subdirectories must return NoArtifacts.
        let temp_dir = tempfile::tempdir().unwrap();
        let result = resolve_local_artifact_key(temp_dir.path(), "models/tide/", "latest");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArtifactError::NoArtifacts));
    }

    #[test]
    fn test_resolve_local_artifact_key_latest_with_dir_but_no_model() {
        // A subdirectory that has no model.tar.gz and no tide_parameters.json
        // must return NoArtifacts.
        let temp_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp_dir.path().join("2026-06-01-00-00-00-000")).unwrap();
        let result = resolve_local_artifact_key(temp_dir.path(), "models/tide/", "latest");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArtifactError::NoArtifacts));
    }

    #[test]
    fn test_resolve_local_artifact_key_latest_prefers_most_recent_run_with_params() {
        // If the newest subdirectory contains tide_parameters.json the path to
        // that directory is returned (the extracted-files fallback branch).
        let temp_dir = tempfile::tempdir().unwrap();
        let run_dir = temp_dir.path().join("2026-06-10-00-00-00-000");
        std::fs::create_dir(&run_dir).unwrap();
        std::fs::write(run_dir.join("tide_parameters.json"), b"{}").unwrap();
        let result = resolve_local_artifact_key(temp_dir.path(), "models/tide/", "latest");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), run_dir.to_string_lossy().to_string());
    }

    #[test]
    fn test_resolve_local_artifact_key_latest_prefers_model_tar_gz() {
        // When output/model.tar.gz exists it takes precedence over the
        // tide_parameters.json fallback.
        let temp_dir = tempfile::tempdir().unwrap();
        let run_dir = temp_dir.path().join("2026-06-10-00-00-00-000");
        let output_dir = run_dir.join("output");
        std::fs::create_dir_all(&output_dir).unwrap();
        std::fs::write(output_dir.join("model.tar.gz"), b"fake").unwrap();
        let result = resolve_local_artifact_key(temp_dir.path(), "models/tide/", "latest");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            output_dir
                .join("model.tar.gz")
                .to_string_lossy()
                .to_string()
        );
    }

    #[test]
    fn test_artifact_error_display() {
        // Verify the Display impl for each variant so the error messages are
        // stable and the thiserror derive is wired up correctly.
        assert_eq!(ArtifactError::NoArtifacts.to_string(), "No artifacts found");
        assert_eq!(
            ArtifactError::S3("timeout".to_string()).to_string(),
            "S3 error: timeout"
        );
        assert_eq!(
            ArtifactError::ModelLoad("bad file".to_string()).to_string(),
            "Model load error: bad file"
        );
    }

    #[test]
    fn test_artifact_error_io_variant_display() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let artifact_error = ArtifactError::Io(io_error);
        let display = artifact_error.to_string();
        assert!(display.contains("IO error"));
    }

    #[test]
    fn test_run_id_from_artifact_key_only_slash() {
        // A single slash with nothing after it: trim gives empty, last segment is "".
        let result = run_id_from_artifact_key("/");
        assert_eq!(result, "");
    }

    #[test]
    fn test_run_id_from_artifact_key_double_trailing_slash() {
        // Trailing slashes are stripped one at a time; the last non-empty segment
        // should be returned.
        let result = run_id_from_artifact_key("models/tide/run-2026//");
        // trim_end_matches('/') strips all trailing slashes then rsplit gives "run-2026"
        assert_eq!(result, "run-2026");
    }

    #[test]
    fn test_run_id_from_artifact_key_exactly_output_suffix_prefix() {
        // A key whose entire suffix matches the canonical form but has no leading
        // prefix — i.e., "2026-06-09/output/model.tar.gz".
        let result = run_id_from_artifact_key("2026-06-09/output/model.tar.gz");
        assert_eq!(result, "2026-06-09");
    }

    #[test]
    fn test_candidate_folders_descending_duplicates_preserve_all() {
        // Duplicate entries are not deduplicated — the caller is responsible for
        // deduplication; the sort+reverse must still work correctly.
        let folders = candidate_folders_descending(vec![
            "models/tide/2026-06-05/".to_string(),
            "models/tide/2026-06-05/".to_string(),
        ]);
        assert_eq!(folders.len(), 2);
        assert_eq!(folders[0], "models/tide/2026-06-05/");
    }

    #[test]
    fn test_resolve_local_artifact_key_latest_with_multiple_dirs_picks_lexicographically_last() {
        // When multiple subdirectories exist the lexicographically last one
        // (i.e., the newest timestamped run) must be selected.
        let temp_dir = tempfile::tempdir().unwrap();

        let older_dir = temp_dir.path().join("2026-06-08-00-00-00-000");
        let newer_dir = temp_dir.path().join("2026-06-10-00-00-00-000");
        std::fs::create_dir(&older_dir).unwrap();
        std::fs::create_dir(&newer_dir).unwrap();

        // Only the newer directory has a tide_parameters.json.
        std::fs::write(newer_dir.join("tide_parameters.json"), b"{}").unwrap();

        let result = resolve_local_artifact_key(temp_dir.path(), "models/tide/", "latest");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), newer_dir.to_string_lossy().to_string());
    }

    #[test]
    fn test_resolve_local_artifact_key_latest_older_dir_has_params_newer_has_nothing() {
        // When the newest directory has no valid artifact but an older directory
        // has tide_parameters.json, the resolver scans newest → oldest and returns
        // the older directory rather than giving up with NoArtifacts.
        let temp_dir = tempfile::tempdir().unwrap();

        let older_dir = temp_dir.path().join("2026-06-08-00-00-00-000");
        let newer_dir = temp_dir.path().join("2026-06-10-00-00-00-000");
        std::fs::create_dir(&older_dir).unwrap();
        std::fs::create_dir(&newer_dir).unwrap();

        // Only the older directory has a parameters file.
        std::fs::write(older_dir.join("tide_parameters.json"), b"{}").unwrap();

        let result = resolve_local_artifact_key(temp_dir.path(), "models/tide/", "latest");
        // The newer dir is skipped (no artifact); the older dir is returned.
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), older_dir.to_string_lossy().to_string());
    }

    #[test]
    fn test_resolve_local_artifact_key_only_files_no_subdirs() {
        // If the local directory contains only files (not subdirectories) the
        // directory list after filtering by is_dir() is empty → NoArtifacts.
        let temp_dir = tempfile::tempdir().unwrap();
        std::fs::write(temp_dir.path().join("some_file.txt"), b"data").unwrap();

        let result = resolve_local_artifact_key(temp_dir.path(), "models/tide/", "latest");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ArtifactError::NoArtifacts));
    }

    use std::io::Read;

    #[test]
    fn test_package_dir_to_tar_gz_is_flat_and_readable() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("tide_parameters.json"), b"{}").unwrap();
        std::fs::write(dir.path().join("tide_states.mpk"), b"weights").unwrap();

        let bytes = package_dir_to_tar_gz(dir.path()).unwrap();

        let decoder = flate2::read::GzDecoder::new(bytes.as_slice());
        let mut archive = tar::Archive::new(decoder);
        let mut names: Vec<String> = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            names.push(path);
            // Flat: no directory components.
            assert!(!names.last().unwrap().contains('/'));
            let mut contents = String::new();
            entry.read_to_string(&mut contents).ok();
        }
        names.sort();
        assert_eq!(names, vec!["tide_parameters.json", "tide_states.mpk"]);
    }
}
