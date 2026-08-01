//! The event bus: six commands, three outcomes, and the scan that recovers missed work.
//!
//! pg_cron writes `<command>_requested` rows into the `events` table. A trigger fires `pg_notify`,
//! the service's listener wakes, dispatches, and writes back `<command>_completed` or
//! `<command>_errored` with a payload describing what happened. That is the entire coordination
//! mechanism — there is no queue, no offset table, and no in-memory scheduling.
//!
//! There is deliberately no `started` outcome. It existed to say a handler had picked up the work,
//! nothing read it, and the structured logs already answer the question.
//!
//! The `events` table doubles as the restart recovery mechanism. A `_requested` row with no
//! matching terminal outcome is work that was issued and never finished, which is exactly what a
//! process that died mid-handler leaves behind. See [`recover_missed_commands`].

use serde_json::Value;
use sqlx::PgPool;
use tracing::{debug, warn};

/// A unit of work the service can be asked to perform.
///
/// Five arrive from pg_cron on a schedule; [`Command::DatabaseExport`] is chained by the service
/// itself when the market data sync completes, so the export can never run against a half-synced
/// database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Command {
    /// Pre-open: resolve the newest model artifact, run inference, write predictions, warm the
    /// calendar and universe caches.
    Predictions,
    /// Every five minutes through the session: price the book, close what should close, open into
    /// vacant slots if conditions allow.
    PortfolioEvaluation,
    /// Pre-close: flatten the book so nothing is held overnight.
    PortfolioLiquidation,
    /// Post-close: pull balances and activities from Alpaca, attribute realized profit and loss
    /// back to the session's closed pairs.
    AccountSync,
    /// Post-close: pull the session's whole-market bars from Massive, and detail changes, into
    /// PostgreSQL.
    MarketDataSync,
    /// Chained from a completed market data sync: export to S3 parquet, then purge.
    DatabaseExport,
}

/// How a command reached its end state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// Issued, not yet handled.
    Requested,
    /// Handled successfully. The payload carries the summary of what happened.
    Completed,
    /// Handling failed. The payload carries the error.
    Errored,
}

/// What to do with a command found unfinished at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// Re-run it. The work still needs doing and nothing else will do it today.
    Replay,
    /// Drop it. Another firing is imminent and would do the same work against fresher inputs.
    Skip,
}

impl Command {
    /// Every variant, for exhaustive iteration in tests and in the recovery scan.
    pub const ALL: [Command; 6] = [
        Command::Predictions,
        Command::PortfolioEvaluation,
        Command::PortfolioLiquidation,
        Command::AccountSync,
        Command::MarketDataSync,
        Command::DatabaseExport,
    ];

    /// The command's stable name, used as the prefix of every event type it appears in.
    pub fn as_str(self) -> &'static str {
        match self {
            Command::Predictions => "predictions",
            Command::PortfolioEvaluation => "portfolio_evaluation",
            Command::PortfolioLiquidation => "portfolio_liquidation",
            Command::AccountSync => "account_sync",
            Command::MarketDataSync => "market_data_sync",
            Command::DatabaseExport => "database_export",
        }
    }

    /// Whether an unfinished instance of this command should be re-run at startup.
    ///
    /// Only the five-minute evaluation is skipped, and for a specific reason: replaying it would
    /// re-run a pass whose inputs are already stale, and the next firing is at most five minutes
    /// away and will price the book against a current snapshot. Every other command happens once a
    /// day, so a missed one is a hole nothing else fills — a missed liquidation in particular must
    /// be replayed, because the alternative is carrying positions overnight.
    pub fn recovery(self) -> Recovery {
        match self {
            Command::PortfolioEvaluation => Recovery::Skip,
            Command::Predictions
            | Command::PortfolioLiquidation
            | Command::AccountSync
            | Command::MarketDataSync
            | Command::DatabaseExport => Recovery::Replay,
        }
    }
}

impl Outcome {
    /// The outcome's stable name, used as the suffix of every event type it appears in.
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Requested => "requested",
            Outcome::Completed => "completed",
            Outcome::Errored => "errored",
        }
    }

    /// Whether this outcome ends a command's lifecycle.
    ///
    /// The recovery scan looks for requests with no terminal outcome; this is what "terminal"
    /// means.
    pub fn is_terminal(self) -> bool {
        match self {
            Outcome::Requested => false,
            Outcome::Completed | Outcome::Errored => true,
        }
    }
}

/// A command paired with the outcome it reached: one row in the `events` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventType {
    command: Command,
    outcome: Outcome,
}

impl EventType {
    /// Pairs a command with an outcome.
    pub fn new(command: Command, outcome: Outcome) -> Self {
        Self { command, outcome }
    }

    pub fn command(self) -> Command {
        self.command
    }

    pub fn outcome(self) -> Outcome {
        self.outcome
    }

    /// The wire name, as stored in `events.event_type` and emitted by pg_cron.
    ///
    /// Spelled out per variant rather than assembled from the two halves so every name that exists
    /// in the schema is greppable in this file.
    pub fn as_str(self) -> &'static str {
        match (self.command, self.outcome) {
            (Command::Predictions, Outcome::Requested) => "predictions_requested",
            (Command::Predictions, Outcome::Completed) => "predictions_completed",
            (Command::Predictions, Outcome::Errored) => "predictions_errored",
            (Command::PortfolioEvaluation, Outcome::Requested) => "portfolio_evaluation_requested",
            (Command::PortfolioEvaluation, Outcome::Completed) => "portfolio_evaluation_completed",
            (Command::PortfolioEvaluation, Outcome::Errored) => "portfolio_evaluation_errored",
            (Command::PortfolioLiquidation, Outcome::Requested) => {
                "portfolio_liquidation_requested"
            }
            (Command::PortfolioLiquidation, Outcome::Completed) => {
                "portfolio_liquidation_completed"
            }
            (Command::PortfolioLiquidation, Outcome::Errored) => "portfolio_liquidation_errored",
            (Command::AccountSync, Outcome::Requested) => "account_sync_requested",
            (Command::AccountSync, Outcome::Completed) => "account_sync_completed",
            (Command::AccountSync, Outcome::Errored) => "account_sync_errored",
            (Command::MarketDataSync, Outcome::Requested) => "market_data_sync_requested",
            (Command::MarketDataSync, Outcome::Completed) => "market_data_sync_completed",
            (Command::MarketDataSync, Outcome::Errored) => "market_data_sync_errored",
            (Command::DatabaseExport, Outcome::Requested) => "database_export_requested",
            (Command::DatabaseExport, Outcome::Completed) => "database_export_completed",
            (Command::DatabaseExport, Outcome::Errored) => "database_export_errored",
        }
    }

    /// Parses a wire name back into a typed event.
    ///
    /// Returns `None` for anything unrecognized. An unknown name is not an error the service should
    /// die on: a stale cron job or a hand-issued event from an old vocabulary should be logged and
    /// ignored, not fatal.
    pub fn parse(raw: &str) -> Option<Self> {
        Command::ALL
            .into_iter()
            .flat_map(|command| {
                [Outcome::Requested, Outcome::Completed, Outcome::Errored]
                    .into_iter()
                    .map(move |outcome| EventType::new(command, outcome))
            })
            .find(|event_type| event_type.as_str() == raw)
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Errors from writing to or reading the event table.
#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("event database access failed: {0}")]
    Database(#[from] sqlx::Error),
}

/// Writes an event row. The insert trigger fires `pg_notify`, so no separate publish is needed.
pub async fn emit(pool: &PgPool, event_type: EventType, payload: Value) -> Result<(), EventError> {
    sqlx::query!(
        "INSERT INTO events (event_type, payload) VALUES ($1, $2)",
        event_type.as_str(),
        payload
    )
    .execute(pool)
    .await?;
    debug!(event_type = event_type.as_str(), "Event emitted");
    Ok(())
}

/// Records a command as completed, with a payload summarizing what it did.
///
/// The summary is the point. A completed row that says only "it finished" makes the nightly export
/// of this table worthless; one that carries the pairs opened and closed, the rows synced, or the
/// artifact used makes it the record of the trading day.
pub async fn emit_completed(
    pool: &PgPool,
    command: Command,
    summary: Value,
) -> Result<(), EventError> {
    emit(pool, EventType::new(command, Outcome::Completed), summary).await
}

/// Records a command as failed, with the error rendered into the payload.
pub async fn emit_errored(pool: &PgPool, command: Command, error: &str) -> Result<(), EventError> {
    emit(
        pool,
        EventType::new(command, Outcome::Errored),
        serde_json::json!({ "error": error }),
    )
    .await
}

/// Finds commands requested during the current Eastern trading date that never reached a terminal
/// outcome, and which should be re-run.
///
/// This is what replaces a consumer offset table. A `_requested` row with no later `_completed` or
/// `_errored` row is, by construction, work that was issued and never finished — which is what a
/// process killed mid-handler leaves behind, and also what a process that was simply not running
/// when cron fired leaves behind. Both want the same response.
///
/// Commands whose [`Command::recovery`] is [`Recovery::Skip`] are found and then dropped, so the
/// caller receives only work worth doing.
///
/// The Eastern date rather than the UTC date because that is the boundary the trading day actually
/// has. The additional two-day bound on `created_at` keeps the hypertable from scanning every
/// chunk to answer a question about today.
pub async fn recover_missed_commands(pool: &PgPool) -> Result<Vec<Command>, EventError> {
    let rows = sqlx::query!(
        r#"
        SELECT request.event_type AS "event_type!"
        FROM events AS request
        WHERE request.created_at >= now() - INTERVAL '2 days'
          AND (request.created_at AT TIME ZONE 'America/New_York')::date
              = (now() AT TIME ZONE 'America/New_York')::date
          AND request.event_type LIKE '%\_requested'
          AND NOT EXISTS (
              SELECT 1
              FROM events AS terminal
              WHERE terminal.created_at >= now() - INTERVAL '2 days'
                AND terminal.id > request.id
                AND terminal.event_type IN (
                    replace(request.event_type, '_requested', '_completed'),
                    replace(request.event_type, '_requested', '_errored')
                )
          )
        ORDER BY request.id
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut replayable = Vec::new();
    for row in rows {
        let Some(event_type) = EventType::parse(&row.event_type) else {
            warn!(
                event_type = %row.event_type,
                "Unrecognized event type in recovery scan, ignoring"
            );
            continue;
        };
        let command = event_type.command();
        match command.recovery() {
            Recovery::Replay => {
                if !replayable.contains(&command) {
                    replayable.push(command);
                }
            }
            Recovery::Skip => debug!(
                command = command.as_str(),
                "Unfinished command skipped in recovery; a fresher firing is imminent"
            ),
        }
    }
    Ok(replayable)
}

/// A parsed `pg_notify` payload from the `events` channel.
#[derive(Debug, Clone, PartialEq)]
pub struct Notification {
    pub event_id: i64,
    pub event_type: EventType,
    pub payload: Value,
    /// Whether the trigger omitted the payload because the notification would have exceeded
    /// `pg_notify`'s 8000-byte limit.
    ///
    /// When set, `payload` is an empty object and the real payload must be read from the row with
    /// [`fetch_payload`]. Ignoring this flag would silently treat a large summary as an empty one.
    pub payload_truncated: bool,
}

impl Notification {
    /// Parses the JSON body the `notify_event` trigger builds.
    ///
    /// Returns `None` when the body is malformed or names an event type outside the current
    /// vocabulary. Both are conditions to log and ignore rather than crash on: the listener must
    /// survive a stray notification.
    pub fn parse(raw: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(raw).ok()?;
        let event_id = value.get("event_id")?.as_i64()?;
        let event_type = EventType::parse(value.get("event_type")?.as_str()?)?;
        let payload_truncated = value
            .get("payload_truncated")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let payload = value
            .get("payload")
            .cloned()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        Some(Self {
            event_id,
            event_type,
            payload,
            payload_truncated,
        })
    }
}

/// Reads one event's payload from the row, for notifications that arrived truncated.
pub async fn fetch_payload(pool: &PgPool, event_id: i64) -> Result<Value, EventError> {
    let row = sqlx::query!(
        r#"SELECT payload AS "payload!" FROM events WHERE id = $1"#,
        event_id
    )
    .fetch_one(pool)
    .await?;
    Ok(row.payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every command-outcome pair must round-trip through its wire name. This is the guard against
    /// a typo in the 18-arm match: a name that does not parse back is a name no listener can
    /// dispatch.
    #[test]
    fn test_every_event_type_round_trips() {
        for command in Command::ALL {
            for outcome in [Outcome::Requested, Outcome::Completed, Outcome::Errored] {
                let event_type = EventType::new(command, outcome);
                assert_eq!(
                    EventType::parse(event_type.as_str()),
                    Some(event_type),
                    "{} must round-trip",
                    event_type
                );
            }
        }
    }

    /// Wire names must be unique. Two pairs sharing a name would make dispatch ambiguous and the
    /// round-trip test above would still pass for one of them.
    #[test]
    fn test_wire_names_are_unique() {
        let mut names = Vec::new();
        for command in Command::ALL {
            for outcome in [Outcome::Requested, Outcome::Completed, Outcome::Errored] {
                names.push(EventType::new(command, outcome).as_str());
            }
        }
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len(), "wire names must be unique");
        assert_eq!(names.len(), 18, "six commands times three outcomes");
    }

    /// The wire name must be exactly the command prefix and outcome suffix joined by an underscore.
    /// The recovery scan's SQL rewrites `_requested` into `_completed` textually, so a name that
    /// does not follow this shape would silently never match its own terminal outcome.
    #[test]
    fn test_wire_names_match_the_pattern_the_recovery_scan_assumes() {
        for command in Command::ALL {
            for outcome in [Outcome::Requested, Outcome::Completed, Outcome::Errored] {
                let event_type = EventType::new(command, outcome);
                assert_eq!(
                    event_type.as_str(),
                    format!("{}_{}", command.as_str(), outcome.as_str()),
                    "{} must be prefix_suffix",
                    event_type
                );
            }
        }
    }

    #[test]
    fn test_parse_rejects_unknown_names() {
        assert!(EventType::parse("trading_session_started").is_none());
        assert!(EventType::parse("predictions_started").is_none());
        assert!(EventType::parse("").is_none());
    }

    #[test]
    fn test_only_terminal_outcomes_are_terminal() {
        assert!(!Outcome::Requested.is_terminal());
        assert!(Outcome::Completed.is_terminal());
        assert!(Outcome::Errored.is_terminal());
    }

    /// Liquidation must replay. A process down at 15:45 that comes up at 15:50 has to flatten the
    /// book; skipping it means carrying positions overnight, which the design forbids outright.
    #[test]
    fn test_liquidation_replays_and_evaluation_does_not() {
        assert_eq!(Command::PortfolioLiquidation.recovery(), Recovery::Replay);
        assert_eq!(Command::PortfolioEvaluation.recovery(), Recovery::Skip);
    }

    #[test]
    fn test_notification_parses_trigger_payload() {
        let raw = r#"{"event_id":42,"event_type":"predictions_requested","payload":{"reason":"pre_open"}}"#;
        let notification = Notification::parse(raw).expect("well-formed payload must parse");
        assert_eq!(notification.event_id, 42);
        assert_eq!(
            notification.event_type,
            EventType::new(Command::Predictions, Outcome::Requested)
        );
        assert_eq!(notification.payload["reason"], "pre_open");
    }

    #[test]
    fn test_notification_defaults_absent_payload_to_empty_object() {
        let raw = r#"{"event_id":1,"event_type":"account_sync_requested"}"#;
        let notification = Notification::parse(raw).expect("absent payload is allowed");
        assert_eq!(notification.payload, Value::Object(serde_json::Map::new()));
        assert!(!notification.payload_truncated);
    }

    /// A payload too large for `pg_notify` arrives with the flag set and no payload. Treating that
    /// as an empty summary would silently discard exactly the largest, most interesting runs, so
    /// the flag has to survive parsing.
    #[test]
    fn test_notification_reports_a_truncated_payload() {
        let raw = r#"{"event_id":7,"event_type":"portfolio_evaluation_completed","payload_truncated":true}"#;
        let notification = Notification::parse(raw).expect("truncated form must parse");
        assert!(notification.payload_truncated);
        assert_eq!(notification.payload, Value::Object(serde_json::Map::new()));
        assert_eq!(notification.event_id, 7);
    }

    /// A malformed or unknown notification must not be an error the listener dies on.
    #[test]
    fn test_notification_rejects_malformed_input() {
        assert!(Notification::parse("not json").is_none());
        assert!(Notification::parse(r#"{"event_id":1}"#).is_none());
        assert!(Notification::parse(r#"{"event_type":"predictions_requested"}"#).is_none());
        assert!(
            Notification::parse(r#"{"event_id":1,"event_type":"legacy_thing_requested"}"#)
                .is_none()
        );
    }
}
