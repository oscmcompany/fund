//! Write side of TiDE model artifacts: package a directory of trained files into
//! a gzipped tar and upload it (plus run metadata) to S3.
//!
//! The tar is flat (one entry per file, no directory prefix) so it matches the
//! extraction the inference loader performs in `inference::artifact`.

use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use flate2::write::GzEncoder;
use flate2::Compression;

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
