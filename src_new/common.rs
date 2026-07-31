//! Shared vocabulary and service infrastructure.
//!
//! [`types`] holds the domain vocabulary every other module speaks — tickers, bars, quotes,
//! predictions, and the validated financial primitives. [`events`] is the coordination mechanism:
//! six commands, three outcomes, and the scan that recovers work missed while the process was down.
//! The remainder is bootstrap shared by every entry point.

pub mod alpaca;
pub mod aws;
pub mod crypto;
pub mod database;
pub mod events;
pub mod observability;
pub mod types;
