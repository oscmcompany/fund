//! PostgreSQL event bus shared by all services.
//!
//! Provides the canonical [`EventType`] enumeration, consumer name constants,
//! and database helper functions for emitting events and tracking consumer
//! offsets. All services must use these in place of local copies.

use std::fmt;

use sqlx::PgPool;
use tracing::info;

/// All event types published on the `events` PostgreSQL NOTIFY channel.
///
/// Each variant maps to a canonical snake_case string via [`EventType::as_str`].
/// That string is stored in the `events` table `event_type` column and carried
/// in NOTIFY payloads so consumers can match without an extra database round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    // --- Market data sync ---
    /// pg_cron trigger: nightly equity bar sync requested.
    EquityBarsSyncRequested,
    /// data: equity bar sync has started.
    EquityBarsSyncStarted,
    /// data: equity bar sync completed successfully.
    EquityBarsSyncCompleted,
    /// data: equity bar sync encountered an error.
    EquityBarsSyncErrored,

    // --- Nightly post-market chain: export → backup → purge ---
    /// pg_cron trigger: nightly database export to S3 Parquet requested.
    DatabaseExportRequested,
    /// data: database export has started.
    DatabaseExportStarted,
    /// data: database export completed successfully.
    DatabaseExportCompleted,
    /// data: database export encountered an error.
    DatabaseExportErrored,

    /// data: database backup requested (chained from export completion).
    DatabaseBackupRequested,
    /// data: database backup has started.
    DatabaseBackupStarted,
    /// data: database backup completed successfully.
    DatabaseBackupCompleted,
    /// data: database backup encountered an error.
    DatabaseBackupErrored,

    /// data: database purge requested (chained from backup completion).
    DatabasePurgeRequested,
    /// data: database purge has started.
    DatabasePurgeStarted,
    /// data: database purge completed successfully.
    DatabasePurgeCompleted,
    /// data: database purge encountered an error.
    DatabasePurgeErrored,

    // --- Prediction pipeline ---
    /// pg_cron periodic trigger: intraday market session check.
    MarketSessionCheck,
    /// portfolio: equity prediction run requested in response to a market session check.
    EquityPredictionsRequested,
    /// inference: equity prediction run has started.
    EquityPredictionsStarted,
    /// inference: equity prediction run completed successfully.
    EquityPredictionsCompleted,
    /// inference: equity prediction run encountered an error.
    EquityPredictionsErrored,

    // --- Portfolio rebalance ---
    /// portfolio: rebalance has started.
    PortfolioRebalanceStarted,
    /// portfolio: rebalance completed successfully.
    PortfolioRebalanceCompleted,
    /// portfolio: rebalance encountered an error.
    PortfolioRebalanceErrored,

    // --- End-of-day liquidation ---
    /// pg_cron trigger: close all open positions before market close.
    PortfolioLiquidationRequested,
    /// portfolio: liquidation has started.
    PortfolioLiquidationStarted,
    /// portfolio: liquidation completed successfully.
    PortfolioLiquidationCompleted,
    /// portfolio: liquidation encountered an error.
    PortfolioLiquidationErrored,
}

impl EventType {
    /// Returns the canonical snake_case string stored in the `events` table.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EquityBarsSyncRequested => "equity_bars_sync_requested",
            Self::EquityBarsSyncStarted => "equity_bars_sync_started",
            Self::EquityBarsSyncCompleted => "equity_bars_sync_completed",
            Self::EquityBarsSyncErrored => "equity_bars_sync_errored",
            Self::DatabaseExportRequested => "database_export_requested",
            Self::DatabaseExportStarted => "database_export_started",
            Self::DatabaseExportCompleted => "database_export_completed",
            Self::DatabaseExportErrored => "database_export_errored",
            Self::DatabaseBackupRequested => "database_backup_requested",
            Self::DatabaseBackupStarted => "database_backup_started",
            Self::DatabaseBackupCompleted => "database_backup_completed",
            Self::DatabaseBackupErrored => "database_backup_errored",
            Self::DatabasePurgeRequested => "database_purge_requested",
            Self::DatabasePurgeStarted => "database_purge_started",
            Self::DatabasePurgeCompleted => "database_purge_completed",
            Self::DatabasePurgeErrored => "database_purge_errored",
            Self::MarketSessionCheck => "market_session_check",
            Self::EquityPredictionsRequested => "equity_predictions_requested",
            Self::EquityPredictionsStarted => "equity_predictions_started",
            Self::EquityPredictionsCompleted => "equity_predictions_completed",
            Self::EquityPredictionsErrored => "equity_predictions_errored",
            Self::PortfolioRebalanceStarted => "portfolio_rebalance_started",
            Self::PortfolioRebalanceCompleted => "portfolio_rebalance_completed",
            Self::PortfolioRebalanceErrored => "portfolio_rebalance_errored",
            Self::PortfolioLiquidationRequested => "portfolio_liquidation_requested",
            Self::PortfolioLiquidationStarted => "portfolio_liquidation_started",
            Self::PortfolioLiquidationCompleted => "portfolio_liquidation_completed",
            Self::PortfolioLiquidationErrored => "portfolio_liquidation_errored",
        }
    }

    /// Parses a stored event type string. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "equity_bars_sync_requested" => Some(Self::EquityBarsSyncRequested),
            "equity_bars_sync_started" => Some(Self::EquityBarsSyncStarted),
            "equity_bars_sync_completed" => Some(Self::EquityBarsSyncCompleted),
            "equity_bars_sync_errored" => Some(Self::EquityBarsSyncErrored),
            "database_export_requested" => Some(Self::DatabaseExportRequested),
            "database_export_started" => Some(Self::DatabaseExportStarted),
            "database_export_completed" => Some(Self::DatabaseExportCompleted),
            "database_export_errored" => Some(Self::DatabaseExportErrored),
            "database_backup_requested" => Some(Self::DatabaseBackupRequested),
            "database_backup_started" => Some(Self::DatabaseBackupStarted),
            "database_backup_completed" => Some(Self::DatabaseBackupCompleted),
            "database_backup_errored" => Some(Self::DatabaseBackupErrored),
            "database_purge_requested" => Some(Self::DatabasePurgeRequested),
            "database_purge_started" => Some(Self::DatabasePurgeStarted),
            "database_purge_completed" => Some(Self::DatabasePurgeCompleted),
            "database_purge_errored" => Some(Self::DatabasePurgeErrored),
            "market_session_check" => Some(Self::MarketSessionCheck),
            "equity_predictions_requested" => Some(Self::EquityPredictionsRequested),
            "equity_predictions_started" => Some(Self::EquityPredictionsStarted),
            "equity_predictions_completed" => Some(Self::EquityPredictionsCompleted),
            "equity_predictions_errored" => Some(Self::EquityPredictionsErrored),
            "portfolio_rebalance_started" => Some(Self::PortfolioRebalanceStarted),
            "portfolio_rebalance_completed" => Some(Self::PortfolioRebalanceCompleted),
            "portfolio_rebalance_errored" => Some(Self::PortfolioRebalanceErrored),
            "portfolio_liquidation_requested" => Some(Self::PortfolioLiquidationRequested),
            "portfolio_liquidation_started" => Some(Self::PortfolioLiquidationStarted),
            "portfolio_liquidation_completed" => Some(Self::PortfolioLiquidationCompleted),
            "portfolio_liquidation_errored" => Some(Self::PortfolioLiquidationErrored),
            _ => None,
        }
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
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

/// Returns the payload of a specific event by type and id.
///
/// Used during startup catchup to retrieve the JSONB payload (e.g. export date)
/// for an event that was missed while the consumer was down. Returns an empty
/// object when the event is not found.
pub async fn query_event_payload(
    pool: &PgPool,
    event_type: EventType,
    event_id: i64,
) -> Result<serde_json::Value, sqlx::Error> {
    use sqlx::Row;
    let row = sqlx::query("SELECT payload FROM events WHERE event_type = $1 AND id = $2 LIMIT 1")
        .bind(event_type.as_str())
        .bind(event_id)
        .fetch_optional(pool)
        .await?;
    Ok(row
        .and_then(|row| row.try_get::<serde_json::Value, _>("payload").ok())
        .unwrap_or_else(|| serde_json::json!({})))
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

    #[test]
    fn test_event_type_parse_round_trips_all_variants() {
        // Every as_str() output must parse back to the same variant.
        let all: &[EventType] = &[
            EventType::EquityBarsSyncRequested,
            EventType::EquityBarsSyncStarted,
            EventType::EquityBarsSyncCompleted,
            EventType::EquityBarsSyncErrored,
            EventType::DatabaseExportRequested,
            EventType::DatabaseExportStarted,
            EventType::DatabaseExportCompleted,
            EventType::DatabaseExportErrored,
            EventType::DatabaseBackupRequested,
            EventType::DatabaseBackupStarted,
            EventType::DatabaseBackupCompleted,
            EventType::DatabaseBackupErrored,
            EventType::DatabasePurgeRequested,
            EventType::DatabasePurgeStarted,
            EventType::DatabasePurgeCompleted,
            EventType::DatabasePurgeErrored,
            EventType::MarketSessionCheck,
            EventType::EquityPredictionsRequested,
            EventType::EquityPredictionsStarted,
            EventType::EquityPredictionsCompleted,
            EventType::EquityPredictionsErrored,
            EventType::PortfolioRebalanceStarted,
            EventType::PortfolioRebalanceCompleted,
            EventType::PortfolioRebalanceErrored,
            EventType::PortfolioLiquidationRequested,
            EventType::PortfolioLiquidationStarted,
            EventType::PortfolioLiquidationCompleted,
            EventType::PortfolioLiquidationErrored,
        ];
        for &event_type in all {
            assert_eq!(
                EventType::parse(event_type.as_str()),
                Some(event_type),
                "parse round-trip failed for {:?}",
                event_type
            );
        }
    }

    #[test]
    fn test_event_type_parse_rejects_unknown() {
        assert_eq!(EventType::parse("unknown_event"), None);
        assert_eq!(EventType::parse(""), None);
        assert_eq!(EventType::parse("EQUITY_BARS_SYNC_COMPLETED"), None);
    }

    #[test]
    fn test_event_type_as_str_is_snake_case() {
        assert_eq!(
            EventType::EquityBarsSyncRequested.as_str(),
            "equity_bars_sync_requested"
        );
        assert_eq!(
            EventType::EquityPredictionsCompleted.as_str(),
            "equity_predictions_completed"
        );
        assert_eq!(
            EventType::PortfolioRebalanceCompleted.as_str(),
            "portfolio_rebalance_completed"
        );
        assert_eq!(
            EventType::PortfolioLiquidationCompleted.as_str(),
            "portfolio_liquidation_completed"
        );
        assert_eq!(
            EventType::MarketSessionCheck.as_str(),
            "market_session_check"
        );
        assert_eq!(
            EventType::EquityPredictionsErrored.as_str(),
            "equity_predictions_errored"
        );
        assert_eq!(
            EventType::PortfolioLiquidationErrored.as_str(),
            "portfolio_liquidation_errored"
        );
    }

    #[test]
    fn test_event_type_display_matches_as_str() {
        for event_type in [
            EventType::EquityBarsSyncCompleted,
            EventType::EquityPredictionsRequested,
            EventType::PortfolioRebalanceCompleted,
            EventType::DatabaseBackupCompleted,
        ] {
            assert_eq!(event_type.to_string(), event_type.as_str());
        }
    }

    #[test]
    fn test_consumer_name_constants_are_stable() {
        assert_eq!(CONSUMER_INFERENCE, "inference");
        assert_eq!(CONSUMER_PORTFOLIO, "portfolio");
        assert_eq!(CONSUMER_PORTFOLIO_LIQUIDATION, "portfolio-liquidation");
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
                EventType::EquityBarsSyncCompleted,
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
            assert!(
                latest_event_after(&lazy_pool(), EventType::EquityPredictionsCompleted, 0)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn test_events_after_compiles() {
        make_runtime().block_on(async {
            assert!(
                events_after(&lazy_pool(), EventType::DatabaseExportRequested, 0)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn test_query_event_payload_compiles() {
        make_runtime().block_on(async {
            assert!(
                query_event_payload(&lazy_pool(), EventType::DatabaseExportRequested, 1)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn test_event_type_as_str_all_variants() {
        // Exhaustively verify every variant maps to its expected snake_case string.
        let cases: &[(EventType, &str)] = &[
            (
                EventType::EquityBarsSyncRequested,
                "equity_bars_sync_requested",
            ),
            (EventType::EquityBarsSyncStarted, "equity_bars_sync_started"),
            (
                EventType::EquityBarsSyncCompleted,
                "equity_bars_sync_completed",
            ),
            (EventType::EquityBarsSyncErrored, "equity_bars_sync_errored"),
            (
                EventType::DatabaseExportRequested,
                "database_export_requested",
            ),
            (EventType::DatabaseExportStarted, "database_export_started"),
            (
                EventType::DatabaseExportCompleted,
                "database_export_completed",
            ),
            (EventType::DatabaseExportErrored, "database_export_errored"),
            (
                EventType::DatabaseBackupRequested,
                "database_backup_requested",
            ),
            (EventType::DatabaseBackupStarted, "database_backup_started"),
            (
                EventType::DatabaseBackupCompleted,
                "database_backup_completed",
            ),
            (EventType::DatabaseBackupErrored, "database_backup_errored"),
            (
                EventType::DatabasePurgeRequested,
                "database_purge_requested",
            ),
            (EventType::DatabasePurgeStarted, "database_purge_started"),
            (
                EventType::DatabasePurgeCompleted,
                "database_purge_completed",
            ),
            (EventType::DatabasePurgeErrored, "database_purge_errored"),
            (EventType::MarketSessionCheck, "market_session_check"),
            (
                EventType::EquityPredictionsRequested,
                "equity_predictions_requested",
            ),
            (
                EventType::EquityPredictionsStarted,
                "equity_predictions_started",
            ),
            (
                EventType::EquityPredictionsCompleted,
                "equity_predictions_completed",
            ),
            (
                EventType::EquityPredictionsErrored,
                "equity_predictions_errored",
            ),
            (
                EventType::PortfolioRebalanceStarted,
                "portfolio_rebalance_started",
            ),
            (
                EventType::PortfolioRebalanceCompleted,
                "portfolio_rebalance_completed",
            ),
            (
                EventType::PortfolioRebalanceErrored,
                "portfolio_rebalance_errored",
            ),
            (
                EventType::PortfolioLiquidationRequested,
                "portfolio_liquidation_requested",
            ),
            (
                EventType::PortfolioLiquidationStarted,
                "portfolio_liquidation_started",
            ),
            (
                EventType::PortfolioLiquidationCompleted,
                "portfolio_liquidation_completed",
            ),
            (
                EventType::PortfolioLiquidationErrored,
                "portfolio_liquidation_errored",
            ),
        ];
        for (event_type, expected) in cases {
            assert_eq!(
                event_type.as_str(),
                *expected,
                "as_str mismatch for {:?}",
                event_type
            );
        }
    }

    #[test]
    fn test_event_type_display_matches_as_str_all_variants() {
        // Display must equal as_str() for every variant.
        let all: &[EventType] = &[
            EventType::EquityBarsSyncRequested,
            EventType::EquityBarsSyncStarted,
            EventType::EquityBarsSyncCompleted,
            EventType::EquityBarsSyncErrored,
            EventType::DatabaseExportRequested,
            EventType::DatabaseExportStarted,
            EventType::DatabaseExportCompleted,
            EventType::DatabaseExportErrored,
            EventType::DatabaseBackupRequested,
            EventType::DatabaseBackupStarted,
            EventType::DatabaseBackupCompleted,
            EventType::DatabaseBackupErrored,
            EventType::DatabasePurgeRequested,
            EventType::DatabasePurgeStarted,
            EventType::DatabasePurgeCompleted,
            EventType::DatabasePurgeErrored,
            EventType::MarketSessionCheck,
            EventType::EquityPredictionsRequested,
            EventType::EquityPredictionsStarted,
            EventType::EquityPredictionsCompleted,
            EventType::EquityPredictionsErrored,
            EventType::PortfolioRebalanceStarted,
            EventType::PortfolioRebalanceCompleted,
            EventType::PortfolioRebalanceErrored,
            EventType::PortfolioLiquidationRequested,
            EventType::PortfolioLiquidationStarted,
            EventType::PortfolioLiquidationCompleted,
            EventType::PortfolioLiquidationErrored,
        ];
        for event_type in all {
            assert_eq!(
                event_type.to_string(),
                event_type.as_str(),
                "Display != as_str for {:?}",
                event_type
            );
        }
    }

    #[test]
    fn test_event_type_equality() {
        assert_eq!(EventType::MarketSessionCheck, EventType::MarketSessionCheck);
        assert_ne!(
            EventType::MarketSessionCheck,
            EventType::EquityPredictionsStarted
        );
    }

    #[test]
    fn test_event_type_copy() {
        // EventType derives Copy so it can be passed by value freely.
        let original = EventType::PortfolioRebalanceStarted;
        let copied = original;
        assert_eq!(original, copied);
    }
}
