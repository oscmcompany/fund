//! Shared vocabulary and service infrastructure.
//!
//! [`types`] holds the domain vocabulary every other module speaks — tickers, bars, quotes,
//! predictions, and the validated financial primitives. [`events`] is the coordination mechanism:
//! six commands, three outcomes, and the scan that recovers work missed while the process was down.
//! The remainder is bootstrap shared by every entry point.
//!
//! Two market data providers, split by question rather than by preference. [`alpaca`] is the venue,
//! so it answers everything about our account and about the current moment: the clock, the calendar,
//! the tradable set, intraday snapshots, orders, positions, balances. [`massive`] answers one
//! question — what did the whole market do on this date — because its grouped endpoint takes no
//! symbol list, which is what keeps historical bars free of survivorship bias.

pub mod alpaca;
pub mod aws;
pub mod crypto;
pub mod database;
pub mod events;
pub mod massive;
pub mod observability;
pub mod types;
