//! Fund platform: a statistical arbitrage service driven entirely by scheduled events.
//!
//! One process, woken by pg_cron, running the commands in [`common::events::Command`]. It flattens
//! the book before every close, and treats Alpaca as the source of truth for what it holds.
//!

pub mod common;
pub mod dashboard;
pub mod data;
pub mod handlers;
pub mod models;
pub mod portfolio;
