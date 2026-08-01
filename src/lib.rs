//! Fund platform: a statistical arbitrage service driven entirely by scheduled events.
//!
//! One process, woken by pg_cron. Pre-open it runs model inference; every five minutes through the
//! session it prices the book from a point-in-time Alpaca snapshot and opens or closes pairs;
//! before the close it liquidates; after the close it syncs the account, the market history, and
//! the database export. The book is flat overnight without exception.
//!
//! Alpaca is the source of truth for fills, balances, buying power, and positions. PostgreSQL holds
//! model output, the long/short pair mapping, market history, and an audit trail of every command
//! issued and outcome reached.
//!
//! - [`common`] — shared vocabulary and infrastructure: types, the event bus, the Alpaca client,
//!   database pools, AWS clients, tracing.
//! - [`data`] — market data and storage: the trading calendar, the tradable universe, bar and
//!   detail syncing, S3 export, and purging.
//! - [`models`] — the TiDE model: artifact handling, inference, and training.
//! - [`portfolio`] — the strategy: pair selection, sizing, the risk gate, execution, and the
//!   five-minute evaluation pass.
//! - [`handlers`] — one function per command, and the state they share. The only module that knows
//!   how an event name turns into work.
//! - [`dashboard`] — a read-only page describing all of the above. Its own process, its own
//!   read-only database role, and no path back into the strategy.

pub mod common;
pub mod dashboard;
pub mod data;
pub mod handlers;
pub mod models;
pub mod portfolio;
