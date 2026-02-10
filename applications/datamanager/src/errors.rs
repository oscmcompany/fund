use aws_credential_types::provider::error::CredentialsError;
use duckdb::Error as DuckError;
use polars::prelude::PolarsError;
use thiserror::Error as ThisError;

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
