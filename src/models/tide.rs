//! TiDE: the model, its training, its artifact, and its inference.
//!
//! [`artifact`] is the only thing crossing between the trainer VM and the application VM.

/// What went wrong building, fitting, or loading a TiDE model.
///
/// [`TideError::Artifact`] and [`TideError::Data`] split by which machine is at fault, and carry
/// prose because no caller branches on the reason — only an operator reads it.
#[derive(Debug, thiserror::Error)]
pub enum TideError {
    #[error("dataframe operation failed: {0}")]
    Frame(#[from] polars::prelude::PolarsError),
    #[error("array shape is inconsistent: {0}")]
    Shape(#[from] ndarray::ShapeError),
    #[error("failed to read or write a model file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse a model file: {0}")]
    Json(#[from] serde_json::Error),
    /// An artifact file this build cannot trust: the trainer's output disagrees with this binary.
    #[error("{0}")]
    Artifact(String),
    /// Rows that cannot be turned into model input: the market data, not the artifact.
    #[error("{0}")]
    Data(String),
}

pub mod artifact;
pub mod batch;
pub mod configuration;
pub mod data;
pub mod drift;
pub mod evaluate;
pub mod fit;
pub mod loss;
pub mod model;
pub mod predict;
pub mod train;
