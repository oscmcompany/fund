//! Inference service: loads trained TiDE artifacts from S3 and consumes
//! `equity_predictions_requested` events from the Postgres event bus. Prediction
//! pipeline and artifact polling are driven by the consolidated `fund` binary.

pub mod artifact;
pub mod consumer;
pub mod database;
pub mod pipeline;
pub mod predict;
pub mod state;
