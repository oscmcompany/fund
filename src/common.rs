//! Shared vocabulary and service infrastructure.
//!
//! Two market data providers, split by question rather than preference. [`alpaca`] is the venue,
//! so it answers everything about our account and the current moment. [`massive`] answers only what
//! the whole market did on a given date — its grouped endpoint takes no symbol list, which is what
//! keeps historical bars free of survivorship bias.

pub mod alpaca;
pub mod aws;
pub mod crypto;
pub mod database;
pub mod events;
pub mod massive;
pub mod observability;
pub mod types;
