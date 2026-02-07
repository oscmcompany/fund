use aws_credential_types::provider::error::CredentialsError;
use duckdb::Error as DuckError;
use polars::prelude::PolarsError;
use thiserror::Error as ThisError;

/// Substrings in DuckDB or S3 error messages that indicate missing data rather
/// than an unexpected failure. These typically appear on first run before any
/// parquet files have been written.
const NOT_FOUND_PATTERNS: &[&str] = &[
    "No files found",
    "Could not find",
    "does not exist",
    "Invalid Input",
];

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("DuckDB error: {0}")]
    DuckDB(#[from] DuckError),
    #[error("Credentials error: {0}")]
    Credentials(#[from] CredentialsError),
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("Other error: {0}")]
    Other(String),
}

impl Error {
    /// Returns true when the error indicates that requested data was not found,
    /// which is expected on first run before any files exist in S3.
    pub fn is_not_found(&self) -> bool {
        let message = self.to_string();
        NOT_FOUND_PATTERNS
            .iter()
            .any(|pattern| message.contains(pattern))
    }
}
