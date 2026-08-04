//! TiDE: the model, its training, its artifact, and its inference.
//!
//! [`artifact`] is the only thing crossing between the trainer VM and the application VM.

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
