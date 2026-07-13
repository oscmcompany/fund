//! Data: syncs equity data from the Massive API, backed by S3 and
//! PostgreSQL. Driven by the Postgres event bus and a sync scheduler.

pub mod database;
pub mod equity_bars;
pub mod equity_details;
pub mod equity_quotes;
pub mod errors;
pub mod export;
pub mod scheduler;
pub mod state;
pub mod types;
