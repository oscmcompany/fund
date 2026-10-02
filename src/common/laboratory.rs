//! The pieces a study composes: a dataset folds to a `Series`, a series summarizes to an `Estimate`, and an estimate is
//! judged against a cost, a haircut and a permutation null.

pub mod cost;
pub mod dataset;
pub mod estimate;
pub mod haircut;
pub mod permutation;
pub mod series;

/// The largest reading magnitude a series admits; no measured quantity comes near it, and below it every sum, square
/// and Welch term stays finite.
pub const READING_BOUND: f64 = 1e50;
