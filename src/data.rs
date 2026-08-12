//! Market data and storage.
//!
//! [`calendar`] and [`universe`] are fetched once and held for the Eastern date; neither changes
//! intraday. [`bars`] is shared with the trainer, which has no database and writes the same frames
//! to S3 — that shared path keeps the training and inference datasets structurally identical.
//! [`export`] and [`purge`] run in that order and never the reverse.
//!
//! Two S3 prefixes with one owner each. [`archive`] owns `data/`, the trainer's bars and the long-
//! term record of them; [`export`] owns `exports/`, the application's own tables, which the purge
//! deletes from PostgreSQL only once they are safely written. Neither writes the other's prefix.
//!
//! [`export`] also seals and ships [`crate::common::session_log`], the local original that both
//! PostgreSQL and S3 are derived from.

pub mod archive;
pub mod bars;
pub mod calendar;
pub mod details;
pub mod export;
pub mod purge;
pub mod universe;
