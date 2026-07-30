//! Live market data streaming infrastructure.
//!
//! Provides a generic WebSocket connection manager and an in-memory broadcast
//! buffer for real-time market data. Raw data stays exclusively in the
//! broadcast channel and is never written to PostgreSQL — this keeps the hot
//! path decoupled from database I/O throughput.
//!
//! That separation is structural rather than enforced by a runtime check:
//! [`buffer::MarketDataBuffer`] is generic over its message type and has no
//! database dependency, so there is no code path from a streamed quote to a
//! `INSERT`. Nothing needs to police the boundary because nothing can cross it
//! by accident.
//!
//! Data reaches durable storage only when a downstream consumer turns it into a
//! derived signal or trading decision. Those are written to the `events` table
//! via [`crate::common::events::emit_event`] and become durable, replayable
//! events visible to all consumers. For live quotes there is exactly one such
//! crossing: the live-quote evaluator emitting `portfolio_evaluation_requested`
//! on a threshold crossing.

pub mod alpaca_equities;
pub mod buffer;
pub mod connection;
