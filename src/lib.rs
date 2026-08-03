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

pub mod common;
pub mod dashboard;
pub mod data;
pub mod handlers;
pub mod models;
pub mod portfolio;
