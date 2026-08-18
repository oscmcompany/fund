//! Fund platform: a statistical arbitrage service driven entirely by scheduled events.
//!
//! One process, woken by pg_cron, running the commands in [`common::events::Command`]. The book is
//! flat overnight without exception, and Alpaca is the source of truth for everything it holds.
//!

pub mod common;
pub mod dashboard;
pub mod data;
pub mod handlers;
pub mod models;
pub mod portfolio;
