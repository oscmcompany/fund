//! TiDE model: configuration, data handling, and the Burn model definition.

pub mod batch;
pub mod config;
pub mod data;
pub mod model;

// Rust-native training pipeline (the `train` feature). Inference builds skip these.
#[cfg(feature = "train")]
pub mod artifact;
#[cfg(feature = "train")]
pub mod evaluate;
#[cfg(feature = "train")]
pub mod fit;
#[cfg(feature = "train")]
pub mod loss;
#[cfg(feature = "train")]
pub mod train;
