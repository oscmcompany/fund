//! What a trading session journals of its own standing: whether its tape is whole, why it halted, and how it ended.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::common::order::{BrokerFailure, ClientOrderId};
use crate::common::time::SessionDate;

/// Whether the trader's tape is whole, which the trader requires before it drains or decides on bars.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FeedContinuity {
    Whole,
    /// The stream was lost; prints may be missing until a reopen and a backfill behind it.
    Interrupted,
    /// The stream is open but the tape has a gap before it, at startup or after a reopen; the next backfill closes it.
    Reopened,
}

/// A feed event that can move the continuity, with what the feed reported of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedTransition {
    Lost,
    Reopened { attempts: u32 },
    Backfilled { since: DateTime<Utc>, fresh: usize },
}

impl FeedContinuity {
    pub fn after(self, transition: FeedTransition) -> Self {
        match (self, transition) {
            (Self::Whole | Self::Interrupted | Self::Reopened, FeedTransition::Lost) => {
                Self::Interrupted
            }
            (Self::Interrupted | Self::Reopened, FeedTransition::Reopened { .. }) => Self::Reopened,
            (Self::Reopened, FeedTransition::Backfilled { .. }) => Self::Whole,
            (Self::Whole, FeedTransition::Reopened { .. })
            | (Self::Whole | Self::Interrupted, FeedTransition::Backfilled { .. }) => self,
        }
    }
}

/// A transition that changed the feed's continuity, from `from` to what `transition` leads to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedChanged {
    from: FeedContinuity,
    transition: FeedTransition,
}

impl FeedChanged {
    /// The change `transition` makes from `from`, or `None` when it leaves the continuity as it stands.
    pub fn of(from: FeedContinuity, transition: FeedTransition) -> Option<Self> {
        (from.after(transition) != from).then_some(Self { from, transition })
    }

    pub fn to(&self) -> FeedContinuity {
        self.from.after(self.transition)
    }
}

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
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_feed_continuity_names_agree_between_strum_and_serde() {
        let names: Vec<&str> = FeedContinuity::iter().map(Into::into).collect();
        assert_eq!(names, ["whole", "interrupted", "reopened"]);
        for continuity in FeedContinuity::iter() {
            assert_eq!(continuity.to_string().parse(), Ok(continuity));
            assert_eq!(
                serde_json::to_string(&continuity).unwrap(),
                format!("\"{continuity}\"")
            );
        }
    }

    /// Only a transition that moves the continuity is a change: a loss from anywhere but an interruption, a reopen
    /// after one, and a backfill behind a reopen.
    #[test]
    fn test_only_a_moving_transition_is_a_change() {
        let reopened = FeedTransition::Reopened { attempts: 1 };
        let backfilled = FeedTransition::Backfilled {
            since: "2026-10-07T14:00:00Z".parse().unwrap(),
            fresh: 0,
        };
        let changes: Vec<(&str, &str, Option<&str>)> = FeedContinuity::iter()
            .flat_map(|from| {
                [FeedTransition::Lost, reopened, backfilled]
                    .into_iter()
                    .map(move |transition| (from, transition))
            })
            .map(|(from, transition)| {
                let name = match transition {
                    FeedTransition::Lost => "lost",
                    FeedTransition::Reopened { .. } => "reopened",
                    FeedTransition::Backfilled { .. } => "backfilled",
                };
                let to = FeedChanged::of(from, transition).map(|changed| changed.to().into());
                (from.into(), name, to)
            })
            .collect();
        assert_eq!(
            changes,
            [
                ("whole", "lost", Some("interrupted")),
                ("whole", "reopened", None),
                ("whole", "backfilled", None),
                ("interrupted", "lost", None),
                ("interrupted", "reopened", Some("reopened")),
                ("interrupted", "backfilled", None),
                ("reopened", "lost", Some("interrupted")),
                ("reopened", "reopened", None),
                ("reopened", "backfilled", Some("whole")),
            ]
        );
    }
}
