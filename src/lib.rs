//! Fund platform: a single crate hosting the platform's Rust services as
//! feature-gated modules, each exposed as a thin binary under `src/bin/`.
//!
//! - `domain`: schema authority for records shared across services (always built).
//! - `models`: model definitions and inference (TiDE), used by `ensemble_model`.
//! - `data_manager` / `ensemble_model`: the HTTP services, gated by features of
//!   the same name so a slim build links only the deps it needs.

pub mod common;
pub mod domain;

#[cfg(feature = "ensemble_model")]
pub mod models;

#[cfg(feature = "data_manager")]
pub mod data_manager;

#[cfg(feature = "ensemble_model")]
pub mod ensemble_model;
