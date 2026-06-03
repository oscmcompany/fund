//! Internal domain types shared across fund platform services.
//!
//! This crate is the schema authority: struct definitions are the single source
//! of truth for what a data record looks like across PostgreSQL, S3, and
//! application code.

pub mod freshness;
pub mod market;
pub mod orders;
pub mod portfolio;
pub mod predictions;
pub mod primitives;
pub mod signals;
pub mod trading;
