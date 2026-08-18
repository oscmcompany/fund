//! Where models, features, and strategies are tried: stateful, offline, and never on the trading path.
//!
//! Everything here reads the archive and writes its own journal. Nothing here decides a trade.

pub mod dataset;
pub mod export;
pub mod journal;
pub mod metrics;
pub mod predictor;
