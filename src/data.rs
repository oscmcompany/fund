//! Market data and storage: what the market is, what it costs, and where the record goes.
//!
//! [`calendar`] and [`universe`] answer the two questions every session opens with — does the
//! market trade today and until when, and which symbols are eligible. Both are fetched once and
//! held for the Eastern date, because neither changes intraday.
//!
//! [`bars`] and [`details`] are the market history the model and the pair screen read from.
//! [`bars`] is shared with the trainer, which runs on a different machine with no database and
//! writes the same frames to S3 — that shared code path is what keeps the training and inference
//! datasets structurally identical.
//!
//! [`export`] and [`purge`] are the nightly archival pair, in that order and never the reverse.

pub mod bars;
pub mod calendar;
pub mod details;
pub mod export;
pub mod purge;
pub mod universe;
