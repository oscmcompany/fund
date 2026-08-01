//! The TiDE model: its definition, its training, its artifact, and its inference.
//!
//! Training and inference used to live apart — a `models/` module holding the architecture and an
//! `inference/` module holding everything needed to run it — which meant the two sides of the same
//! model were maintained in different places and drifted. They are one thing here.
//!
//! [`tide`] is the model itself: configuration, feature engineering, the Burn network, the loss,
//! and the training loop. [`artifact`] is how a trained model crosses the machine boundary between
//! the trainer VM and the application VM, as a `model.tar.gz` in S3. [`predict`] is what the
//! application does with it.

pub mod artifact;
pub mod predict;
pub mod tide;
