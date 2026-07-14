//! Shared service infrastructure: observability, AWS clients, database pools,
//! and the PostgreSQL event bus. Deduplicates the bootstrap code common to
//! every module.

pub mod alpaca;
pub mod aws;
pub mod crypto;
pub mod database;
pub mod events;
#[cfg(any(feature = "data", feature = "portfolio"))]
pub mod market_hours;
pub mod observability;
