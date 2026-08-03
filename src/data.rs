//! Market data and storage.
//!
//! [`calendar`] and [`universe`] are fetched once and held for the Eastern date; neither changes
//! intraday. [`bars`] is shared with the trainer, which has no database and writes the same frames
//! to S3 — that shared path keeps the training and inference datasets structurally identical.
//! [`export`] and [`purge`] run in that order and never the reverse.

pub mod bars;
pub mod calendar;
pub mod details;
pub mod export;
pub mod purge;
pub mod universe;
