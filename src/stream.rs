//! Live market data streaming infrastructure.
//!
//! Provides a generic WebSocket connection manager and an in-memory broadcast
//! buffer for real-time market data. Raw data stays exclusively in the
//! broadcast channel and is never written to PostgreSQL — this keeps the hot
//! path decoupled from database I/O throughput.
//!
//! Data crosses the persistence boundary only when a downstream consumer
//! produces a derived signal or trading decision. Those are written to the
//! `events` table via [`crate::common::events::emit_event`] and become
//! durable, replayable events visible to all consumers.

pub mod buffer;
pub mod connection;
pub mod data_boundary;
