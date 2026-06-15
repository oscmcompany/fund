//! Shared service infrastructure: observability, AWS clients, database pools,
//! HTTP serving, and the PostgreSQL event bus. Deduplicates the bootstrap code
//! common to every service.

pub mod alpaca;
pub mod aws;
pub mod database;
pub mod events;
pub mod observability;
pub mod server;
