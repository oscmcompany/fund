//! Where models, features, and strategies are tried: stateful, offline, and never on the trading path.
//!
//! Everything here reads the archive and writes its own journal. Nothing here decides a trade.

pub mod convergence;
pub mod dataset;
pub mod export;
pub mod forecast;
pub mod information;
pub mod intraday;
pub mod intraday_convergence;
pub mod journal;
pub mod metrics;
pub mod predictor;
pub mod regime;
pub mod stability;
