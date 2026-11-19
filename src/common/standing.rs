//! What a trading session journals of its own standing: why it halted, how it went flat, and how it ended.

use std::num::NonZeroU32;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::common::order::{BrokerFailure, ClientOrderId};
use crate::common::time::SessionDate;

/// Why a session stopped deciding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaltCause {
    /// The journal refused a write; the halt is still journaled, best effort.
    JournalRefused { error: String },
    /// The broker's book could not be read to reconcile against.
    ReconcileUnread(BrokerFailure),
    /// The broker's book diverged from the journal's; the `book_reconciled` record says where.
    Diverged,
    /// An order was left unresolved and may still be open at the broker.
    Unresolved { client_order_id: ClientOrderId },
}

/// The first halt of a session, journaled once with its cause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHalted {
    cause: HaltCause,
}

impl SessionHalted {
    pub fn new(cause: HaltCause) -> Self {
        Self { cause }
    }
}

/// Whether a session still decides, or the cause of its first halt, after which it decides nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Standing {
    Trading,
    Halted(HaltCause),
}

/// How long before the close a session closes whatever it still holds through the broker, decisions or none.
pub const FLAT_BACKSTOP: TimeDelta = TimeDelta::minutes(2);

/// The instant a session closing at `close` falls due for its backstop.
pub fn backstop_at(close: DateTime<Utc>) -> DateTime<Utc> {
    close - FLAT_BACKSTOP
}

/// Why a session standing at `standing` closes every position at `now`, or `None` once it has, before the backstop
/// when trading, or at or past the close, where orders would rest a session.
pub fn backstop_due(
    standing: &Standing,
    flattened: bool,
    close: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<FlattenCause> {
    match (
        flattened || now >= close,
        standing,
        now >= backstop_at(close),
    ) {
        (true, _, _) | (false, Standing::Trading, false) => None,
        (false, Standing::Halted(_), _) => Some(FlattenCause::Halted),
        (false, Standing::Trading, true) => Some(FlattenCause::ClosingBackstop),
    }
}

/// Why a session closed its positions through the broker rather than by a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlattenCause {
    /// The session halted, after which it decides nothing more.
    Halted,
    /// The close was near, whatever the flat window's decisions left.
    ClosingBackstop,
}

/// What the broker reported once a close request had its wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlatReading {
    Flat,
    Held { positions: usize },
    Unread(BrokerFailure),
}

/// One reading of the book after closing every position through the broker: why, how many close requests the session
/// had sent by then, at least the one that began it, and what the broker held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookFlattened {
    cause: FlattenCause,
    closes: NonZeroU32,
    reading: FlatReading,
}

impl BookFlattened {
    pub fn new(cause: FlattenCause, closes: NonZeroU32, reading: FlatReading) -> Self {
        Self {
            cause,
            closes,
            reading,
        }
    }

    pub fn cause(&self) -> FlattenCause {
        self.cause
    }

    pub fn closes(&self) -> NonZeroU32 {
        self.closes
    }

    pub fn reading(&self) -> &FlatReading {
        &self.reading
    }
}

/// How a trading session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEnding {
    NoSession,
    /// Ran to the close and the account held nothing there.
    ToTheClose,
    /// Ran to the close but the account still held `positions` positions.
    HeldAtTheClose {
        positions: usize,
    },
    /// Halted before the close, its cause journaled as `session_halted` when the journal took it.
    Halted,
    /// Refused before the open.
    StoppedBeforeTheOpen,
    /// Stopped by a failed step while trading.
    StoppedTrading,
}

/// The session a trader run was for and how the run ended, journaled whether or not `session_opened` was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionClosed {
    session: SessionDate,
    ending: SessionEnding,
}

impl SessionClosed {
    pub fn new(session: SessionDate, ending: SessionEnding) -> Self {
        Self { session, ending }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    /// Trading falls due two minutes before the close, a halt at once, and neither once flattened or at the close.
    #[test]
    fn test_the_backstop_falls_due_before_the_close_or_at_a_halt() {
        let close = at("2026-10-07T20:00:00Z");
        let halted = Standing::Halted(HaltCause::Diverged);
        let due = |standing: &Standing, flattened: bool, now: &str| {
            backstop_due(standing, flattened, close, at(now))
        };
        assert_eq!(due(&Standing::Trading, false, "2026-10-07T19:57:59Z"), None);
        assert_eq!(
            due(&Standing::Trading, false, "2026-10-07T19:58:00Z"),
            Some(FlattenCause::ClosingBackstop)
        );
        assert_eq!(
            due(&halted, false, "2026-10-07T14:05:00Z"),
            Some(FlattenCause::Halted)
        );
        assert_eq!(due(&Standing::Trading, true, "2026-10-07T19:59:00Z"), None);
        assert_eq!(due(&halted, true, "2026-10-07T14:05:00Z"), None);
        assert_eq!(due(&Standing::Trading, false, "2026-10-07T20:00:00Z"), None);
        assert_eq!(due(&halted, false, "2026-10-07T20:00:00Z"), None);
    }

    /// A record of no close request at all does not read back.
    #[test]
    fn test_a_flattening_without_a_close_request_is_refused() {
        let line = r#"{"cause":"halted","closes":0,"reading":"flat"}"#;
        assert!(serde_json::from_str::<BookFlattened>(line).is_err());
    }
}
