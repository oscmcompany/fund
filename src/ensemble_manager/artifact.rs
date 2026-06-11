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

    let latest = entries.last().ok_or(ArtifactError::NoArtifacts)?;

    let model_path = latest.join("output").join("model.tar.gz");
    if model_path.exists() {
        Ok(model_path.to_string_lossy().to_string())
    } else {
        // Check for extracted files directly
        let params_path = latest.join("tide_parameters.json");
        if params_path.exists() {
            Ok(latest.to_string_lossy().to_string())
        } else {
            Err(ArtifactError::NoArtifacts)
        }
    }
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
}
