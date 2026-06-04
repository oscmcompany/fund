use polars::prelude::PolarsError;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("No data found")]
    NoData,
    #[error("Other error: {0}")]
    Other(String),
}
