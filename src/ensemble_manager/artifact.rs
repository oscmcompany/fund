use std::path::{Path, PathBuf};

use aws_sdk_s3::Client as S3Client;
use chrono::Utc;
use tracing::{debug, info};

use crate::ensemble_manager::state::ModelState;

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

/// Best-effort fetch of the sibling `run_metadata.json` next to an artifact
/// (`<prefix>/<run_id>/run_metadata.json`). Returns `None` on any failure so the
/// caller can proceed without lineage metadata.
pub async fn fetch_run_metadata(
    s3_client: &S3Client,
    bucket: &str,
    artifact_key: &str,
    local_dir: Option<&Path>,
) -> Option<serde_json::Value> {
    let metadata_key = artifact_key
        .strip_suffix("output/model.tar.gz")
        .map(|base| format!("{base}run_metadata.json"));

    if local_dir.is_some() {
        let candidate = match &metadata_key {
            Some(key) => PathBuf::from(key),
            None => Path::new(artifact_key).join("run_metadata.json"),
        };
        return std::fs::read_to_string(&candidate)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok());
    }

    let key = metadata_key?;
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await
        .ok()?;
    let bytes = response.body.collect().await.ok()?.into_bytes();
    serde_json::from_slice(&bytes).ok()
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
                debug!(key = key, error = %error, "Run folder has no model artifact, trying older");
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

        // Prevent path traversal
        if path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
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
    let parameters = crate::models::tide::config::ModelParameters::load(&parameters_path)
        .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;

    let scaler_path = dir.join("tide_data_scaler.json");
    let (scaler, continuous_columns, categorical_columns, static_categorical_columns) =
        crate::models::tide::data::Scaler::load(&scaler_path)
            .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;

    let mappings_path = dir.join("tide_data_mappings.json");
    let mappings_content = std::fs::read_to_string(&mappings_path)
        .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;
    let mappings: crate::models::tide::data::FeatureMappings =
        serde_json::from_str(&mappings_content)
            .map_err(|e| ArtifactError::ModelLoad(e.to_string()))?;

    let num_quantiles = parameters.quantiles().len();
    let model = crate::models::tide::model::TideModel::load(
        dir,
        parameters.input_size(),
        parameters.hidden_size(),
        parameters.num_encoder_layers(),
        parameters.num_decoder_layers(),
        parameters.output_length(),
        num_quantiles,
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
        continuous_columns,
        categorical_columns,
        static_categorical_columns,
        artifact_key.to_string(),
        run_id_from_artifact_key(artifact_key),
        load_timestamp,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn test_fetch_run_metadata_local_dir_canonical_key_returns_metadata() {
        // With local_dir set and a canonical artifact key
        // (<prefix>/<run_id>/output/model.tar.gz), the function derives the
        // sibling metadata key (<prefix>/<run_id>/run_metadata.json) and reads
        // it from the local filesystem.
        let temp_dir = tempfile::tempdir().unwrap();
        let run_dir = temp_dir.path().join("2026-06-10-00-00-00-000");
        let output_dir = run_dir.join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let metadata = serde_json::json!({"run_id": "2026-06-10-00-00-00-000", "epochs": 50});
        let metadata_path = run_dir.join("run_metadata.json");
        std::fs::write(&metadata_path, metadata.to_string().as_bytes()).unwrap();

        // Build a dummy S3 client — it will not be called because local_dir is set.
        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        // The artifact key uses the canonical layout so the metadata path is
        // derived by stripping "output/model.tar.gz" and appending "run_metadata.json".
        let artifact_key = format!("{}/output/model.tar.gz", run_dir.to_string_lossy());

        let result = fetch_run_metadata(
            &s3_client,
            "test-bucket",
            &artifact_key,
            Some(temp_dir.path()),
        )
        .await;

        assert!(result.is_some(), "expected metadata to be returned");
        let value = result.unwrap();
        assert_eq!(value["run_id"], "2026-06-10-00-00-00-000");
        assert_eq!(value["epochs"], 50);
    }

    #[tokio::test]
    async fn test_fetch_run_metadata_local_dir_non_canonical_key_joins_run_metadata() {
        // With a non-canonical artifact key (no output/model.tar.gz suffix) the
        // function falls back to joining artifact_key with "run_metadata.json".
        let temp_dir = tempfile::tempdir().unwrap();

        // Write the metadata file at <temp_dir>/run_metadata.json.
        let metadata = serde_json::json!({"status": "ok"});
        std::fs::write(
            temp_dir.path().join("run_metadata.json"),
            metadata.to_string().as_bytes(),
        )
        .unwrap();

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        // Use the temp directory path itself as the artifact key (non-canonical).
        let artifact_key = temp_dir.path().to_string_lossy().to_string();

        let result = fetch_run_metadata(
            &s3_client,
            "test-bucket",
            &artifact_key,
            Some(temp_dir.path()),
        )
        .await;

        assert!(
            result.is_some(),
            "expected metadata from non-canonical key path"
        );
        assert_eq!(result.unwrap()["status"], "ok");
    }

    #[tokio::test]
    async fn test_fetch_run_metadata_local_dir_file_missing_returns_none() {
        // When the metadata file does not exist on disk the function must return
        // None rather than propagating an error.
        let temp_dir = tempfile::tempdir().unwrap();

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let artifact_key = format!(
            "{}/2026-06-10/output/model.tar.gz",
            temp_dir.path().to_string_lossy()
        );

        let result = fetch_run_metadata(
            &s3_client,
            "test-bucket",
            &artifact_key,
            Some(temp_dir.path()),
        )
        .await;

        assert!(result.is_none(), "missing metadata file must return None");
    }

    #[tokio::test]
    async fn test_fetch_run_metadata_local_dir_invalid_json_returns_none() {
        // A metadata file that contains invalid JSON must cause the function to
        // return None rather than panicking or propagating a parse error.
        let temp_dir = tempfile::tempdir().unwrap();
        let run_dir = temp_dir.path().join("2026-06-11-00-00-00-000");
        let output_dir = run_dir.join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        // Write invalid JSON to the metadata file.
        std::fs::write(run_dir.join("run_metadata.json"), b"not valid json { }]").unwrap();

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let artifact_key = format!("{}/output/model.tar.gz", run_dir.to_string_lossy());

        let result = fetch_run_metadata(
            &s3_client,
            "test-bucket",
            &artifact_key,
            Some(temp_dir.path()),
        )
        .await;

        assert!(result.is_none(), "invalid JSON metadata must return None");
    }
}
