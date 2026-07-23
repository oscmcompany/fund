//! Event boundary contract classifying live market data by persistence
//! requirements.
//!
//! The two-tier [`DataBoundary`] enum enforces the architectural separation
//! between ephemeral in-memory data and durable persisted events. Code at
//! the boundary uses this enum to route data: [`DataBoundary::Ephemeral`]
//! values stay in the broadcast channel, while [`DataBoundary::Durable`]
//! values cross the event boundary and are written to PostgreSQL.

/// Classifies live market data by persistence requirements.
///
/// This enum encodes the architectural boundary between the in-memory
/// broadcast channel and the PostgreSQL events table. Routing decisions
/// at the boundary are driven by this classification — ephemeral data
/// is never written to the database, and durable data is always persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataBoundary {
    /// Tick-level quotes, bid/ask updates, and raw price data.
    ///
    /// Lives only in the broadcast channel during market hours. Lost on
    /// process restart and repopulated from the WebSocket stream. Never
    /// written to PostgreSQL.
    Ephemeral,

    /// Derived signals and trading decisions that must survive process
    /// restarts.
    ///
    /// Persisted to the `events` table via [`crate::common::events::emit_event`]
    /// for durability and consumer replay. Examples include z-score threshold
    /// breaches, IV surface dislocations, and order submissions.
    Durable,
}

impl DataBoundary {
    /// Returns `true` if this data should be persisted to PostgreSQL.
    pub fn is_durable(self) -> bool {
        matches!(self, Self::Durable)
    }

    /// Returns `true` if this data lives only in the broadcast channel.
    pub fn is_ephemeral(self) -> bool {
        matches!(self, Self::Ephemeral)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ephemeral_is_not_durable() {
        assert!(DataBoundary::Ephemeral.is_ephemeral());
        assert!(!DataBoundary::Ephemeral.is_durable());
    }

    #[test]
    fn test_durable_is_not_ephemeral() {
        assert!(DataBoundary::Durable.is_durable());
        assert!(!DataBoundary::Durable.is_ephemeral());
    }

    #[test]
    fn test_variants_are_distinct() {
        assert_ne!(DataBoundary::Ephemeral, DataBoundary::Durable);
    }

    #[test]
    fn test_copy_semantics() {
        let original = DataBoundary::Ephemeral;
        let copied = original;
        assert_eq!(original, copied);
    }
}
