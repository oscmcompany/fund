//! TiDE: configuration, feature engineering, the Burn model, and the training loop.
//!
//! There are no feature gates here. The training modules used to sit behind a `train` feature so an
//! inference-only build could skip the Autodiff backend, but Burn's `autodiff` feature adds no
//! runtime cost to the NdArray inference path and was always compiled in regardless. All the gate
//! actually excluded was `rand`, used to shuffle epochs. Carrying a whole feature flag — and a
//! second build configuration to keep working — to avoid linking a random number generator was not
//! a trade worth making.
//!
//! Artifact packaging lives one level up in [`crate::models::artifact`], alongside the reading side
//! it is the mirror of.

pub mod batch;
pub mod config;
pub mod data;
pub mod drift;
pub mod evaluate;
pub mod fit;
pub mod loss;
pub mod model;
pub mod train;
