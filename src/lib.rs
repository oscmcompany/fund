//! Fund platform: a single crate hosting the platform's Rust services as
//! feature-gated modules, driven by the consolidated `fund` binary.
//!
//! - `domain`: schema authority for records shared across services (always built).
//! - `models`: model definitions and inference (TiDE), used by `inference`.
//! - `data` / `inference` / `portfolio`: event-driven services, gated by features
//!   of the same name so a slim build links only the deps it needs.

pub mod common;
pub mod domain;

#[cfg(feature = "inference")]
pub mod models;

#[cfg(feature = "data")]
pub mod data;

#[cfg(feature = "inference")]
pub mod inference;

#[cfg(feature = "portfolio")]
pub mod portfolio;

#[cfg(feature = "stream")]
pub mod stream;

#[cfg(feature = "dashboard")]
pub mod dashboard;
