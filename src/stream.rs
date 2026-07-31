//! Live market data streaming infrastructure.
//!
//! Provides a generic WebSocket connection manager and an in-memory broadcast
//! buffer for real-time market data. Raw data stays exclusively in the
//! broadcast channel and is never written to PostgreSQL — this keeps the hot
//! path decoupled from database I/O throughput.
//!
//! The guarantee is about the buffer, not about everything downstream of it:
//! [`buffer::MarketDataBuffer`] is generic over its message type and has no
//! database dependency, so the transport itself cannot persist what passes
//! through it. A consumer holding a subscriber could of course write what it
//! receives; the convention is that it does not, and that durable state is
//! reached only through the event bus.
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
