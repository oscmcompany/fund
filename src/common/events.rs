//! PostgreSQL event bus shared by all services.
//!
//! Provides the canonical [`EventType`] enumeration, consumer name constants,
//! and database helper functions for emitting events and tracking consumer
//! offsets. All services must use these in place of local copies.

use std::fmt;
use std::future::Future;

use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Lifecycle stage of an event family.
///
/// Only [`Outcome::Requested`] ever drives behavior; the other three are the
/// audit trail a family writes as it runs. The single exception is
/// [`EventType::EquityPredictions`]`(Completed)`, which starts a rebalance —
/// [`EventType::is_control`] is the authoritative list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The work has been asked for. Emitted by a pg_cron job, by a chain step,
    /// or by a consumer re-requesting work it found missing.
    Requested,
    /// The work has begun.
    Started,
    /// The work finished successfully.
    Completed,
    /// The work failed, or finished with a partial failure.
    Errored,
}

/// All event types published on the `events` PostgreSQL NOTIFY channel.
///
/// Each variant maps to a canonical snake_case string via [`EventType::as_str`].
/// That string is stored in the `events` table `event_type` column and carried
/// in NOTIFY payloads so consumers can match without an extra database round-trip.
///
/// Most variants are a family carrying an [`Outcome`], because the
/// requested/started/completed/errored quadruplet is the shape nearly every
/// event follows. The singletons are genuinely lone events, not families with
/// three unemitted siblings.
///
/// Only a minority of these drive behavior. [`EventType::is_control`] is the
/// authoritative list; everything else is written for the operator dashboard and
/// the nightly export and is never dispatched on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// Nightly equity bar sync. `Requested` comes from pg_cron; the data service
    /// emits the rest as it runs.
    EquityBarsSync(Outcome),

    /// Nightly database export to S3 Parquet, the first leg of the post-market
    /// chain. `Requested` comes from pg_cron; export success chains to
    /// [`EventType::DatabaseBackup`].
    DatabaseExport(Outcome),

    /// Nightly database backup, chained from export completion.
    DatabaseBackup(Outcome),

    /// Nightly retention purge, chained from backup completion.
    ///
    /// `Errored` is a partial outcome rather than an abort: a failing table is
    /// skipped so the rest of the purge still runs, and the payload carries the
    /// failed table names alongside the rows that were deleted.
    DatabasePurge(Outcome),

    /// Daily equity prediction run.
    ///
    /// Predictions are derived from daily bars, so one run per session produces
    /// every distinct value the day will have. `Requested` comes from pg_cron,
    /// and the portfolio consumer re-emits it when a session begins with no
    /// predictions recorded, which covers a failed or missed pre-market run.
    ///
    /// `Completed` is the one non-`Requested` control event: it is what drives
    /// the portfolio rebalance once a fresh prediction set exists.
    EquityPredictions(Outcome),

    /// The trading session is about to begin. Control.
    ///
    /// Emitted once by pg_cron, shortly before the regular open. Starts the
    /// session: the portfolio consumer reconciles against the broker, confirms
    /// the market actually trades today, builds the initial portfolio, and arms
    /// the liquidation timer from the real session close.
    ///
    /// This replaced a five-minute evaluation heartbeat that ran a full
    /// rebalance whether or not anything had changed. Intraday work is driven by
    /// the live-quote evaluator instead, which emits
    /// [`EventType::PortfolioEvaluationRequested`] only when a spread actually
    /// crosses a close threshold.
    TradingSessionStarted,

    /// Evaluate open positions for exits and idle capital for entries. Control.
    ///
    /// Emitted by the live-quote evaluator on a threshold crossing. No longer
    /// emitted on a timer: a pass that finds nothing changed is pure cost, and
    /// the events that genuinely need one now emit it directly.
    ///
    /// A singleton rather than a `Requested` outcome of
    /// [`EventType::PortfolioRebalance`] because it is a request to *decide*
    /// whether to rebalance, which is not the same as the rebalance itself.
    PortfolioEvaluationRequested,

    /// A rebalance pass. Audit only.
    ///
    /// Nothing emits `Requested` — a pass is started by
    /// [`EventType::PortfolioEvaluationRequested`],
    /// [`EventType::TradingSessionStarted`], or
    /// [`EventType::EquityPredictions`]`(Completed)`, never by a rebalance
    /// request of its own. The combination is representable so the family keeps
    /// the uniform shape, and round-trips correctly should an emitter ever
    /// appear.
    PortfolioRebalance(Outcome),

    /// End-of-day liquidation of all open positions.
    ///
    /// `Requested` has two emitters, deliberately: the in-process timer armed
    /// from the real session close, and a pg_cron fail-safe on the wall clock.
    /// Liquidation is idempotent, so both firing is harmless.
    PortfolioLiquidation(Outcome),

    /// Dedicated variant for the event bus stress test binary.
    StressTest,
}

impl EventType {
    /// Returns the canonical snake_case string stored in the `events` table.
    ///
    /// These strings are a stable external interface: they sit in existing
    /// `events` rows, in the nightly S3 exports, and in the dashboard's parsing
    /// path. Changing one orphans stored data.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EquityBarsSync(Outcome::Requested) => "equity_bars_sync_requested",
            Self::EquityBarsSync(Outcome::Started) => "equity_bars_sync_started",
            Self::EquityBarsSync(Outcome::Completed) => "equity_bars_sync_completed",
            Self::EquityBarsSync(Outcome::Errored) => "equity_bars_sync_errored",
            Self::DatabaseExport(Outcome::Requested) => "database_export_requested",
            Self::DatabaseExport(Outcome::Started) => "database_export_started",
            Self::DatabaseExport(Outcome::Completed) => "database_export_completed",
            Self::DatabaseExport(Outcome::Errored) => "database_export_errored",
            Self::DatabaseBackup(Outcome::Requested) => "database_backup_requested",
            Self::DatabaseBackup(Outcome::Started) => "database_backup_started",
            Self::DatabaseBackup(Outcome::Completed) => "database_backup_completed",
            Self::DatabaseBackup(Outcome::Errored) => "database_backup_errored",
            Self::DatabasePurge(Outcome::Requested) => "database_purge_requested",
            Self::DatabasePurge(Outcome::Started) => "database_purge_started",
            Self::DatabasePurge(Outcome::Completed) => "database_purge_completed",
            Self::DatabasePurge(Outcome::Errored) => "database_purge_errored",
            Self::EquityPredictions(Outcome::Requested) => "equity_predictions_requested",
            Self::EquityPredictions(Outcome::Started) => "equity_predictions_started",
            Self::EquityPredictions(Outcome::Completed) => "equity_predictions_completed",
            Self::EquityPredictions(Outcome::Errored) => "equity_predictions_errored",
            Self::TradingSessionStarted => "trading_session_started",
            Self::PortfolioEvaluationRequested => "portfolio_evaluation_requested",
            Self::PortfolioRebalance(Outcome::Requested) => "portfolio_rebalance_requested",
            Self::PortfolioRebalance(Outcome::Started) => "portfolio_rebalance_started",
            Self::PortfolioRebalance(Outcome::Completed) => "portfolio_rebalance_completed",
            Self::PortfolioRebalance(Outcome::Errored) => "portfolio_rebalance_errored",
            Self::PortfolioLiquidation(Outcome::Requested) => "portfolio_liquidation_requested",
            Self::PortfolioLiquidation(Outcome::Started) => "portfolio_liquidation_started",
            Self::PortfolioLiquidation(Outcome::Completed) => "portfolio_liquidation_completed",
            Self::PortfolioLiquidation(Outcome::Errored) => "portfolio_liquidation_errored",
            Self::StressTest => "stress_test",
        }
    }

    /// Parses a stored event type string. Returns `None` for unknown values.
    ///
    /// Unknown values are expected in practice: the `events` table retains rows
    /// written by earlier builds, including event types that have since been
    /// removed. Callers skip what they cannot parse rather than treating it as
    /// an error.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "equity_bars_sync_requested" => Some(Self::EquityBarsSync(Outcome::Requested)),
            "equity_bars_sync_started" => Some(Self::EquityBarsSync(Outcome::Started)),
            "equity_bars_sync_completed" => Some(Self::EquityBarsSync(Outcome::Completed)),
            "equity_bars_sync_errored" => Some(Self::EquityBarsSync(Outcome::Errored)),
            "database_export_requested" => Some(Self::DatabaseExport(Outcome::Requested)),
            "database_export_started" => Some(Self::DatabaseExport(Outcome::Started)),
            "database_export_completed" => Some(Self::DatabaseExport(Outcome::Completed)),
            "database_export_errored" => Some(Self::DatabaseExport(Outcome::Errored)),
            "database_backup_requested" => Some(Self::DatabaseBackup(Outcome::Requested)),
            "database_backup_started" => Some(Self::DatabaseBackup(Outcome::Started)),
            "database_backup_completed" => Some(Self::DatabaseBackup(Outcome::Completed)),
            "database_backup_errored" => Some(Self::DatabaseBackup(Outcome::Errored)),
            "database_purge_requested" => Some(Self::DatabasePurge(Outcome::Requested)),
            "database_purge_started" => Some(Self::DatabasePurge(Outcome::Started)),
            "database_purge_completed" => Some(Self::DatabasePurge(Outcome::Completed)),
            "database_purge_errored" => Some(Self::DatabasePurge(Outcome::Errored)),
            "equity_predictions_requested" => Some(Self::EquityPredictions(Outcome::Requested)),
            "equity_predictions_started" => Some(Self::EquityPredictions(Outcome::Started)),
            "equity_predictions_completed" => Some(Self::EquityPredictions(Outcome::Completed)),
            "equity_predictions_errored" => Some(Self::EquityPredictions(Outcome::Errored)),
            "trading_session_started" => Some(Self::TradingSessionStarted),
            "portfolio_evaluation_requested" => Some(Self::PortfolioEvaluationRequested),
            "portfolio_rebalance_requested" => Some(Self::PortfolioRebalance(Outcome::Requested)),
            "portfolio_rebalance_started" => Some(Self::PortfolioRebalance(Outcome::Started)),
            "portfolio_rebalance_completed" => Some(Self::PortfolioRebalance(Outcome::Completed)),
            "portfolio_rebalance_errored" => Some(Self::PortfolioRebalance(Outcome::Errored)),
            "portfolio_liquidation_requested" => {
                Some(Self::PortfolioLiquidation(Outcome::Requested))
            }
            "portfolio_liquidation_started" => Some(Self::PortfolioLiquidation(Outcome::Started)),
            "portfolio_liquidation_completed" => {
                Some(Self::PortfolioLiquidation(Outcome::Completed))
            }
            "portfolio_liquidation_errored" => Some(Self::PortfolioLiquidation(Outcome::Errored)),
            "stress_test" => Some(Self::StressTest),
            _ => None,
        }
    }

    /// Returns `true` when a consumer acts on this event.
    ///
    /// Everything else is an audit record: written for the operator dashboard
    /// and the nightly export, never dispatched on. This list is the definition
    /// — if a consumer starts handling an event, it belongs here, and if one
    /// stops, it does not.
    pub fn is_control(self) -> bool {
        matches!(
            self,
            Self::EquityBarsSync(Outcome::Requested)
                | Self::DatabaseExport(Outcome::Requested)
                | Self::DatabaseBackup(Outcome::Requested)
                | Self::DatabasePurge(Outcome::Requested)
                | Self::EquityPredictions(Outcome::Requested)
                | Self::EquityPredictions(Outcome::Completed)
                | Self::TradingSessionStarted
                | Self::PortfolioEvaluationRequested
                | Self::PortfolioLiquidation(Outcome::Requested)
        )
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

// --- Notification parsing ---

/// A parsed notification from the `events` channel.
///
/// Holding one is proof the payload was well-formed: valid JSON, a positive
/// `event_id`, and an `event_type` this build recognises. Consumers used to
/// re-derive those guarantees themselves, and two of them checked for an
/// `event_id` of zero by hand because their parse helpers returned a sentinel.
///
/// The payload shape is fixed by the `notify_event` trigger in `schema.sql`.
#[derive(Debug, Clone, PartialEq)]
pub struct EventNotification {
    event_id: i64,
    event_type: EventType,
    payload: serde_json::Value,
}

/// Why a NOTIFY payload could not be turned into an [`EventNotification`].
///
/// The two cases carry different severities, which is the reason this is an enum
/// rather than a bare `Option`. A malformed payload means the trigger or its
/// caller is broken and nothing downstream will see the event. An unrecognised
/// event type is routine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationParseFailure {
    /// The payload is not valid JSON, or `event_id` / `event_type` is absent,
    /// the wrong JSON type, or (for `event_id`) not positive.
    ///
    /// A producer bug: the `notify_event` trigger always supplies all three
    /// fields, so seeing this means something else is writing to the channel.
    Malformed,
    /// The payload is well-formed but names an event type this build does not
    /// know.
    ///
    /// Expected rather than exceptional: the `events` table retains rows written
    /// by earlier builds, including types that have since been retired.
    UnknownEventType(String),
}

impl fmt::Display for NotificationParseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => formatter.write_str("malformed notification payload"),
            Self::UnknownEventType(event_type) => {
                write!(formatter, "unknown event type '{event_type}'")
            }
        }
    }
}

impl EventNotification {
    /// Parses a NOTIFY payload emitted by the `notify_event` trigger.
    ///
    /// See [`NotificationParseFailure`] for the two ways this fails and why they
    /// are distinguished.
    pub fn parse(payload: &str) -> Result<Self, NotificationParseFailure> {
        let parsed: serde_json::Value =
            serde_json::from_str(payload).map_err(|_| NotificationParseFailure::Malformed)?;
        let event_id = parsed
            .get("event_id")
            .and_then(serde_json::Value::as_i64)
            .ok_or(NotificationParseFailure::Malformed)?;
        if event_id <= 0 {
            return Err(NotificationParseFailure::Malformed);
        }
        let event_type_string = parsed
            .get("event_type")
            .and_then(serde_json::Value::as_str)
            .ok_or(NotificationParseFailure::Malformed)?;
        let event_type = EventType::parse(event_type_string).ok_or_else(|| {
            NotificationParseFailure::UnknownEventType(event_type_string.to_string())
        })?;
        let payload = parsed
            .get("payload")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
        Ok(Self {
            event_id,
            event_type,
            payload,
        })
    }

    /// The `events` row id this notification refers to. Always positive.
    pub fn event_id(&self) -> i64 {
        self.event_id
    }

    /// The event type, already resolved to a variant.
    pub fn event_type(&self) -> EventType {
        self.event_type
    }

    /// The event's JSONB payload, or an empty object when the row carried none.
    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }

    /// Consumes the notification and returns the payload by value.
    pub fn into_payload(self) -> serde_json::Value {
        self.payload
    }
}

/// Connects to the `events` channel and dispatches notifications until shutdown.
///
/// Replaces the connect / listen / catch-up / select / dispatch scaffolding that
/// each consumer used to carry its own copy of. Returning `Err` hands control
/// back to the caller's supervisor loop, which owns the reconnect backoff.
///
/// `on_connect` runs once per successful connection, before the first
/// notification is read. It carries the consumer's startup catch-up, which
/// deliberately differs between consumers — some replay every missed event,
/// some collapse to the latest, and some intentionally skip catch-up entirely.
/// It must run on every reconnect, not once per process, so that a Postgres blip
/// does not silently skip the reconciliation and offset replay a consumer needs
/// after any gap in delivery.
///
/// Cancellation is checked before `on_connect` and while waiting for the next
/// notification, but not during either. Catch-up can submit orders — the
/// portfolio consumer's runs a full rebalance — so abandoning it midway would
/// leave the broker and the database disagreeing, which is the state startup
/// reconciliation exists to repair. Waiting for it is the safer failure mode.
///
/// Notifications that fail to parse are skipped, at the severity their failure
/// warrants. See [`NotificationParseFailure`].
pub async fn run_event_listener<Connect, ConnectFuture, Handle, HandleFuture>(
    pool: &PgPool,
    shutdown_token: &CancellationToken,
    // Names the service in connect, shutdown, and skipped-notification logs.
    // Deliberately not a `CONSUMER_*` constant: those are offset-tracking keys,
    // and a listener may own several of them, so reusing one would name only a
    // fraction of what the log line covers.
    service_label: &str,
    on_connect: Connect,
    handle: Handle,
) -> Result<(), sqlx::Error>
where
    Connect: Fn() -> ConnectFuture,
    ConnectFuture: Future<Output = Result<(), sqlx::Error>>,
    Handle: Fn(EventNotification) -> HandleFuture,
    HandleFuture: Future<Output = ()>,
{
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen("events").await?;
    info!(
        service = service_label,
        "Event consumer connected, listening on channel 'events'"
    );

    if shutdown_token.is_cancelled() {
        return Ok(());
    }

    on_connect().await?;

    loop {
        let notification = tokio::select! {
            result = listener.recv() => result?,
            _ = shutdown_token.cancelled() => {
                info!(service = service_label, "Shutdown signal received, draining");
                break;
            }
        };

        match EventNotification::parse(notification.payload()) {
            Ok(notification) => handle(notification).await,
            Err(NotificationParseFailure::Malformed) => {
                warn!(
                    service = service_label,
                    payload = notification.payload(),
                    "Skipping malformed event notification"
                );
            }
            Err(NotificationParseFailure::UnknownEventType(event_type)) => {
                debug!(
                    service = service_label,
                    event_type, "Skipping event type this build does not know"
                );
            }
        }
    }

    Ok(())
}

// --- Consumer name constants ---

/// Consumer name for the inference event consumer.
pub const CONSUMER_INFERENCE: &str = "inference";

/// Consumer name for the portfolio predictions consumer.
/// Tracks the last processed `equity_predictions_completed` event.
pub const CONSUMER_PORTFOLIO: &str = "portfolio";

/// Consumer name for the portfolio liquidation consumer.
/// Tracks the last processed `portfolio_liquidation_requested` event separately
/// so the predictions offset cannot mask a missed end-of-day liquidation.
pub const CONSUMER_PORTFOLIO_LIQUIDATION: &str = "portfolio-liquidation";

/// Consumer name for the portfolio session-start consumer.
/// Tracks the last processed `trading_session_started` event so a process that
/// was down at the open can still build the portfolio when it comes back, for
/// as long as the session is still trading.
pub const CONSUMER_PORTFOLIO_SESSION: &str = "portfolio-session";

/// Consumer name for the data equity bars sync consumer.
pub const CONSUMER_DATA_EQUITY_BARS_SYNC: &str = "data-equity-bars-sync";

/// Consumer name for the data database export consumer.
pub const CONSUMER_DATA_DATABASE_EXPORT: &str = "data-database-export";

/// Consumer name for the data database backup consumer.
pub const CONSUMER_DATA_DATABASE_BACKUP: &str = "data-database-backup";

/// Consumer name for the data database purge consumer.
pub const CONSUMER_DATA_DATABASE_PURGE: &str = "data-database-purge";

// --- Database helpers ---

/// Inserts an event row by calling the `emit_event` PostgreSQL stored procedure.
/// The `events_notify` trigger fires `pg_notify` on the `events` channel automatically.
///
/// Accepts any sqlx executor (`&PgPool`, `&mut Transaction`, etc.) so callers
/// can include event emission inside a transaction when atomicity is needed.
pub async fn emit_event<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Postgres>,
    event_type: EventType,
    payload: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "SELECT emit_event($1, $2::jsonb)",
        event_type.as_str(),
        payload
    )
    .execute(executor)
    .await?;
    info!(event_type = event_type.as_str(), "Emitted event");
    Ok(())
}

/// Returns the last processed event id for a consumer, or 0 if not yet recorded.
pub async fn get_consumer_offset(pool: &PgPool, consumer_name: &str) -> Result<i64, sqlx::Error> {
    let row = sqlx::query!(
        "SELECT last_event_id FROM event_consumer_offsets WHERE consumer_name = $1",
        consumer_name
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|record| record.last_event_id).unwrap_or(0))
}

/// Upserts the last processed event id for a consumer.
///
/// `GREATEST` guards against moving the offset backwards under concurrent updates.
pub async fn update_consumer_offset(
    pool: &PgPool,
    consumer_name: &str,
    last_event_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "INSERT INTO event_consumer_offsets (consumer_name, last_event_id, updated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (consumer_name) DO UPDATE SET \
           last_event_id = GREATEST(event_consumer_offsets.last_event_id, EXCLUDED.last_event_id), \
           updated_at = EXCLUDED.updated_at",
        consumer_name,
        last_event_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Returns the id of the most recent event of `event_type` with id greater than
/// `after_id`, used to catch up on events that arrived while a consumer was down.
pub async fn latest_event_after(
    pool: &PgPool,
    event_type: EventType,
    after_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let row = sqlx::query!(
        "SELECT id FROM events WHERE event_type = $1 AND id > $2 ORDER BY id DESC LIMIT 1",
        event_type.as_str(),
        after_id
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|record| record.id))
}

/// Returns all events of `event_type` with id greater than `after_id` in ascending order,
/// paired with their JSONB payloads. Used during startup catch-up to replay every missed
/// event when skipping intermediate occurrences would lose date-specific payload data
/// (e.g. nightly export events where each carries a distinct export date).
pub async fn events_after(
    pool: &PgPool,
    event_type: EventType,
    after_id: i64,
) -> Result<Vec<(i64, serde_json::Value)>, sqlx::Error> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT id, payload FROM events WHERE event_type = $1 AND id > $2 ORDER BY id ASC",
    )
    .bind(event_type.as_str())
    .bind(after_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let event_id: i64 = row.get("id");
            let payload: serde_json::Value = row
                .try_get("payload")
                .unwrap_or_else(|_| serde_json::json!({}));
            (event_id, payload)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lazy_pool() -> PgPool {
        PgPool::connect_lazy("postgresql://localhost:5432/fund_test_nonexistent")
            .expect("lazy pool creation should not fail")
    }

    fn make_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// Every variant paired with the exact string stored in the `events` table.
    ///
    /// This table is the contract with existing rows, the nightly S3 exports,
    /// and the dashboard. A mismatch here means stored data has been orphaned.
    const ALL_EVENT_TYPES: &[(EventType, &str)] = &[
        (
            EventType::EquityBarsSync(Outcome::Requested),
            "equity_bars_sync_requested",
        ),
        (
            EventType::EquityBarsSync(Outcome::Started),
            "equity_bars_sync_started",
        ),
        (
            EventType::EquityBarsSync(Outcome::Completed),
            "equity_bars_sync_completed",
        ),
        (
            EventType::EquityBarsSync(Outcome::Errored),
            "equity_bars_sync_errored",
        ),
        (
            EventType::DatabaseExport(Outcome::Requested),
            "database_export_requested",
        ),
        (
            EventType::DatabaseExport(Outcome::Started),
            "database_export_started",
        ),
        (
            EventType::DatabaseExport(Outcome::Completed),
            "database_export_completed",
        ),
        (
            EventType::DatabaseExport(Outcome::Errored),
            "database_export_errored",
        ),
        (
            EventType::DatabaseBackup(Outcome::Requested),
            "database_backup_requested",
        ),
        (
            EventType::DatabaseBackup(Outcome::Started),
            "database_backup_started",
        ),
        (
            EventType::DatabaseBackup(Outcome::Completed),
            "database_backup_completed",
        ),
        (
            EventType::DatabaseBackup(Outcome::Errored),
            "database_backup_errored",
        ),
        (
            EventType::DatabasePurge(Outcome::Requested),
            "database_purge_requested",
        ),
        (
            EventType::DatabasePurge(Outcome::Started),
            "database_purge_started",
        ),
        (
            EventType::DatabasePurge(Outcome::Completed),
            "database_purge_completed",
        ),
        (
            EventType::DatabasePurge(Outcome::Errored),
            "database_purge_errored",
        ),
        (
            EventType::EquityPredictions(Outcome::Requested),
            "equity_predictions_requested",
        ),
        (
            EventType::EquityPredictions(Outcome::Started),
            "equity_predictions_started",
        ),
        (
            EventType::EquityPredictions(Outcome::Completed),
            "equity_predictions_completed",
        ),
        (
            EventType::EquityPredictions(Outcome::Errored),
            "equity_predictions_errored",
        ),
        (EventType::TradingSessionStarted, "trading_session_started"),
        (
            EventType::PortfolioEvaluationRequested,
            "portfolio_evaluation_requested",
        ),
        (
            EventType::PortfolioRebalance(Outcome::Requested),
            "portfolio_rebalance_requested",
        ),
        (
            EventType::PortfolioRebalance(Outcome::Started),
            "portfolio_rebalance_started",
        ),
        (
            EventType::PortfolioRebalance(Outcome::Completed),
            "portfolio_rebalance_completed",
        ),
        (
            EventType::PortfolioRebalance(Outcome::Errored),
            "portfolio_rebalance_errored",
        ),
        (
            EventType::PortfolioLiquidation(Outcome::Requested),
            "portfolio_liquidation_requested",
        ),
        (
            EventType::PortfolioLiquidation(Outcome::Started),
            "portfolio_liquidation_started",
        ),
        (
            EventType::PortfolioLiquidation(Outcome::Completed),
            "portfolio_liquidation_completed",
        ),
        (
            EventType::PortfolioLiquidation(Outcome::Errored),
            "portfolio_liquidation_errored",
        ),
        (EventType::StressTest, "stress_test"),
    ];

    /// Every event a consumer acts on. The complement is audit-only.
    const CONTROL_EVENT_TYPES: &[EventType] = &[
        EventType::EquityBarsSync(Outcome::Requested),
        EventType::DatabaseExport(Outcome::Requested),
        EventType::DatabaseBackup(Outcome::Requested),
        EventType::DatabasePurge(Outcome::Requested),
        EventType::EquityPredictions(Outcome::Requested),
        EventType::EquityPredictions(Outcome::Completed),
        EventType::TradingSessionStarted,
        EventType::PortfolioEvaluationRequested,
        EventType::PortfolioLiquidation(Outcome::Requested),
    ];

    #[test]
    fn test_event_type_maps_to_exact_stored_string() {
        // Guards the strings themselves, not just their consistency: a rename
        // that updated both as_str and parse would still orphan stored rows.
        for &(event_type, expected) in ALL_EVENT_TYPES {
            assert_eq!(
                event_type.as_str(),
                expected,
                "wrong stored string for {:?}",
                event_type
            );
        }
    }

    #[test]
    fn test_event_type_parse_round_trips_all_variants() {
        for &(event_type, _) in ALL_EVENT_TYPES {
            assert_eq!(
                EventType::parse(event_type.as_str()),
                Some(event_type),
                "parse round-trip failed for {:?}",
                event_type
            );
        }
    }

    #[test]
    fn test_event_type_table_covers_every_variant() {
        // The tests above only check the variants the table already lists, so a
        // variant added to EventType but forgotten here would slip past them.
        // Generate the full cross product instead of trusting a hand-written
        // count: every family paired with every outcome, plus the singletons.
        //
        // Adding a *family* still needs a line here, which the exhaustive match
        // below turns into a compile error rather than a silent gap.
        let families: [fn(Outcome) -> EventType; 7] = [
            EventType::EquityBarsSync,
            EventType::DatabaseExport,
            EventType::DatabaseBackup,
            EventType::DatabasePurge,
            EventType::EquityPredictions,
            EventType::PortfolioRebalance,
            EventType::PortfolioLiquidation,
        ];
        let outcomes = [
            Outcome::Requested,
            Outcome::Started,
            Outcome::Completed,
            Outcome::Errored,
        ];
        let singletons = [
            EventType::TradingSessionStarted,
            EventType::PortfolioEvaluationRequested,
            EventType::StressTest,
        ];

        let mut expected = Vec::new();
        for family in families {
            for outcome in outcomes {
                expected.push(family(outcome));
            }
        }
        expected.extend_from_slice(&singletons);

        for event_type in &expected {
            assert!(
                ALL_EVENT_TYPES
                    .iter()
                    .any(|(listed, _)| listed == event_type),
                "ALL_EVENT_TYPES is missing {:?}",
                event_type
            );
        }
        assert_eq!(
            ALL_EVENT_TYPES.len(),
            expected.len(),
            "ALL_EVENT_TYPES lists a variant the cross product does not produce"
        );
    }

    #[test]
    fn test_every_family_appears_in_the_coverage_cross_product() {
        // Companion to the test above: an exhaustive match, so adding a variant
        // to EventType fails to compile until the cross product learns about it.
        fn assert_accounted_for(event_type: EventType) {
            match event_type {
                EventType::EquityBarsSync(_)
                | EventType::DatabaseExport(_)
                | EventType::DatabaseBackup(_)
                | EventType::DatabasePurge(_)
                | EventType::EquityPredictions(_)
                | EventType::PortfolioRebalance(_)
                | EventType::PortfolioLiquidation(_)
                | EventType::TradingSessionStarted
                | EventType::PortfolioEvaluationRequested
                | EventType::StressTest => {}
            }
        }
        for &(event_type, _) in ALL_EVENT_TYPES {
            assert_accounted_for(event_type);
        }
    }

    /// Builds a payload in the shape the `notify_event` trigger emits.
    fn trigger_payload(event_type: &str, event_id: i64) -> String {
        serde_json::json!({
            "event_id": event_id,
            "event_type": event_type,
            "payload": {"date": "2026-07-30"},
        })
        .to_string()
    }

    #[test]
    fn test_event_notification_parses_a_well_formed_payload() {
        let raw = trigger_payload("equity_predictions_requested", 42);
        let notification =
            EventNotification::parse(&raw).expect("well-formed payload should parse");
        assert_eq!(notification.event_id(), 42);
        assert_eq!(
            notification.event_type(),
            EventType::EquityPredictions(Outcome::Requested)
        );
        assert_eq!(notification.payload()["date"], "2026-07-30");
    }

    #[test]
    fn test_event_notification_defaults_a_missing_payload_to_an_empty_object() {
        let raw = serde_json::json!({
            "event_id": 7,
            "event_type": "equity_bars_sync_completed",
        })
        .to_string();
        let notification = EventNotification::parse(&raw).expect("payload field is optional");
        assert_eq!(
            notification.event_type(),
            EventType::EquityBarsSync(Outcome::Completed)
        );
        assert_eq!(notification.payload(), &serde_json::json!({}));
    }

    #[test]
    fn test_event_notification_rejects_invalid_json_as_malformed() {
        for raw in ["not-json", "", "{unclosed"] {
            assert_eq!(
                EventNotification::parse(raw),
                Err(NotificationParseFailure::Malformed),
                "{raw:?} should be rejected as malformed"
            );
        }
    }

    #[test]
    fn test_event_notification_rejects_missing_or_malformed_fields() {
        let cases = [
            ("missing event_type", serde_json::json!({"event_id": 1})),
            (
                "missing event_id",
                serde_json::json!({"event_type": "stress_test"}),
            ),
            (
                "non-string event_type",
                serde_json::json!({"event_id": 1, "event_type": 99}),
            ),
            (
                "non-integer event_id",
                serde_json::json!({"event_id": "not-a-number", "event_type": "stress_test"}),
            ),
        ];
        for (description, raw) in cases {
            assert_eq!(
                EventNotification::parse(&raw.to_string()),
                Err(NotificationParseFailure::Malformed),
                "{description} should be rejected as malformed"
            );
        }
    }

    #[test]
    fn test_event_notification_rejects_non_positive_event_id() {
        // Two consumers used to check for a zero event_id by hand because their
        // parse helper returned it as a sentinel. Rejecting it here is what lets
        // event_id() promise a real row.
        for event_id in [0, -1] {
            let raw = trigger_payload("stress_test", event_id);
            assert_eq!(
                EventNotification::parse(&raw),
                Err(NotificationParseFailure::Malformed),
                "event_id {event_id} should be rejected"
            );
        }
    }

    #[test]
    fn test_event_notification_reports_an_unknown_event_type_separately() {
        // Retired event types still present in older `events` rows are routine,
        // not a producer bug, and the two are logged at different severities.
        let raw = trigger_payload("equity_bars_export_requested", 5);
        assert_eq!(
            EventNotification::parse(&raw),
            Err(NotificationParseFailure::UnknownEventType(
                "equity_bars_export_requested".to_string()
            ))
        );
    }

    #[test]
    fn test_event_type_strings_are_unique() {
        // Two variants sharing a string would make parse lossy in one direction.
        let mut seen = std::collections::HashSet::new();
        for &(_, stored) in ALL_EVENT_TYPES {
            assert!(seen.insert(stored), "duplicate stored string {stored}");
        }
    }

    #[test]
    fn test_event_type_parse_rejects_unknown() {
        assert_eq!(EventType::parse("unknown_event"), None);
        assert_eq!(EventType::parse(""), None);
        assert_eq!(EventType::parse("EQUITY_BARS_SYNC_COMPLETED"), None);
        // Retired event types still present in older `events` rows.
        assert_eq!(EventType::parse("equity_bars_export_requested"), None);
        assert_eq!(EventType::parse("trading_history_export_requested"), None);
    }

    #[test]
    fn test_is_control_is_true_for_exactly_the_control_events() {
        for &event_type in CONTROL_EVENT_TYPES {
            assert!(
                event_type.is_control(),
                "{:?} should be a control event",
                event_type
            );
        }
        for &(event_type, _) in ALL_EVENT_TYPES {
            let expected = CONTROL_EVENT_TYPES.contains(&event_type);
            assert_eq!(
                event_type.is_control(),
                expected,
                "is_control disagrees with the control list for {:?}",
                event_type
            );
        }
    }

    #[test]
    fn test_event_type_display_matches_as_str() {
        for &(event_type, stored) in ALL_EVENT_TYPES {
            assert_eq!(event_type.to_string(), stored);
        }
    }

    #[test]
    fn test_consumer_name_constants_are_stable() {
        assert_eq!(CONSUMER_INFERENCE, "inference");
        assert_eq!(CONSUMER_PORTFOLIO, "portfolio");
        assert_eq!(CONSUMER_PORTFOLIO_LIQUIDATION, "portfolio-liquidation");
        assert_eq!(CONSUMER_PORTFOLIO_SESSION, "portfolio-session");
        assert_eq!(CONSUMER_DATA_EQUITY_BARS_SYNC, "data-equity-bars-sync");
        assert_eq!(CONSUMER_DATA_DATABASE_EXPORT, "data-database-export");
        assert_eq!(CONSUMER_DATA_DATABASE_BACKUP, "data-database-backup");
        assert_eq!(CONSUMER_DATA_DATABASE_PURGE, "data-database-purge");
    }

    #[test]
    fn test_emit_event_compiles() {
        make_runtime().block_on(async {
            let result = emit_event(
                &lazy_pool(),
                EventType::EquityBarsSync(Outcome::Completed),
                &serde_json::json!({}),
            )
            .await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_get_consumer_offset_compiles() {
        make_runtime().block_on(async {
            assert!(get_consumer_offset(&lazy_pool(), CONSUMER_INFERENCE)
                .await
                .is_err());
        });
    }

    #[test]
    fn test_update_consumer_offset_compiles() {
        make_runtime().block_on(async {
            assert!(update_consumer_offset(&lazy_pool(), CONSUMER_PORTFOLIO, 42)
                .await
                .is_err());
        });
    }

    #[test]
    fn test_latest_event_after_compiles() {
        make_runtime().block_on(async {
            assert!(latest_event_after(
                &lazy_pool(),
                EventType::EquityPredictions(Outcome::Completed),
                0
            )
            .await
            .is_err());
        });
    }

    #[test]
    fn test_events_after_compiles() {
        make_runtime().block_on(async {
            assert!(events_after(
                &lazy_pool(),
                EventType::DatabaseExport(Outcome::Requested),
                0
            )
            .await
            .is_err());
        });
    }

    #[test]
    fn test_event_type_equality() {
        assert_eq!(
            EventType::PortfolioEvaluationRequested,
            EventType::PortfolioEvaluationRequested
        );
        assert_ne!(
            EventType::PortfolioEvaluationRequested,
            EventType::EquityPredictions(Outcome::Started)
        );
        assert_ne!(
            EventType::TradingSessionStarted,
            EventType::PortfolioEvaluationRequested
        );
    }

    #[test]
    fn test_event_type_copy() {
        // EventType derives Copy so it can be passed by value freely.
        let original = EventType::PortfolioRebalance(Outcome::Started);
        let copied = original;
        assert_eq!(original, copied);
    }
}
