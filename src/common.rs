//! Shared service infrastructure: observability, AWS clients, database pools,
//! and HTTP serving. Deduplicates the bootstrap code common to every service.

pub mod aws;
pub mod database;
pub mod observability;
pub mod server;
