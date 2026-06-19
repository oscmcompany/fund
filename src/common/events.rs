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
    /// data_manager: equity bar sync has started.
    EquityBarsSyncStarted,
    /// data_manager: equity bar sync completed successfully.
    EquityBarsSyncCompleted,
    /// data_manager: equity bar sync encountered an error.
    EquityBarsSyncErrored,

    // --- Nightly exports ---
    /// pg_cron trigger: equity bars S3 export requested.
    EquityBarsExportRequested,
    /// data_manager: equity bars export has started.
    EquityBarsExportStarted,
    /// data_manager: equity bars export completed successfully.
    EquityBarsExportCompleted,
    /// data_manager: equity bars export encountered an error.
    EquityBarsExportErrored,

    /// pg_cron trigger: trading history S3 export requested.
    TradingHistoryExportRequested,
    /// data_manager: trading history export has started.
    TradingHistoryExportStarted,
    /// data_manager: trading history export completed successfully.
    TradingHistoryExportCompleted,
    /// data_manager: trading history export encountered an error.
    TradingHistoryExportErrored,

    /// pg_cron trigger: database backup requested.
    DatabaseBackupRequested,
    /// data_manager: database backup has started.
    DatabaseBackupStarted,
    /// data_manager: database backup completed successfully.
    DatabaseBackupCompleted,
    /// data_manager: database backup encountered an error.
    DatabaseBackupErrored,

    // --- Prediction pipeline ---
    /// pg_cron periodic trigger: intraday market session check.
    MarketSessionCheck,
    /// portfolio_manager: equity prediction run requested in response to a market session check.
    EquityPredictionsRequested,
    /// ensemble_manager: equity prediction run has started.
    EquityPredictionsStarted,
    /// ensemble_manager: equity prediction run completed successfully.
    EquityPredictionsCompleted,
    /// ensemble_manager: equity prediction run encountered an error.
    EquityPredictionsErrored,

    // --- Portfolio rebalance ---
    /// portfolio_manager: rebalance has started.
    PortfolioRebalanceStarted,
    /// portfolio_manager: rebalance completed successfully.
    PortfolioRebalanceCompleted,
    /// portfolio_manager: rebalance encountered an error.
    PortfolioRebalanceErrored,

    // --- End-of-day liquidation ---
    /// pg_cron trigger: close all open positions before market close.
    PortfolioLiquidationRequested,
    /// portfolio_manager: liquidation has started.
    PortfolioLiquidationStarted,
    /// portfolio_manager: liquidation completed successfully.
    PortfolioLiquidationCompleted,
    /// portfolio_manager: liquidation encountered an error.
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
            Self::EquityBarsExportRequested => "equity_bars_export_requested",
            Self::EquityBarsExportStarted => "equity_bars_export_started",
            Self::EquityBarsExportCompleted => "equity_bars_export_completed",
            Self::EquityBarsExportErrored => "equity_bars_export_errored",
            Self::TradingHistoryExportRequested => "trading_history_export_requested",
            Self::TradingHistoryExportStarted => "trading_history_export_started",
            Self::TradingHistoryExportCompleted => "trading_history_export_completed",
            Self::TradingHistoryExportErrored => "trading_history_export_errored",
            Self::DatabaseBackupRequested => "database_backup_requested",
            Self::DatabaseBackupStarted => "database_backup_started",
            Self::DatabaseBackupCompleted => "database_backup_completed",
            Self::DatabaseBackupErrored => "database_backup_errored",
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
}

impl fmt::Display for EventType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

// --- Consumer name constants ---

/// Consumer name for the ensemble_manager event consumer.
pub const CONSUMER_ENSEMBLE_MANAGER: &str = "ensemble-manager";

/// Consumer name for the portfolio_manager predictions consumer.
/// Tracks the last processed `equity_predictions_completed` event.
pub const CONSUMER_PORTFOLIO_MANAGER: &str = "portfolio-manager";

/// Consumer name for the portfolio_manager liquidation consumer.
/// Tracks the last processed `portfolio_liquidation_requested` event separately
/// so the predictions offset cannot mask a missed end-of-day liquidation.
pub const CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION: &str = "portfolio-manager-liquidation";

/// Consumer name for the data_manager equity bars sync consumer.
pub const CONSUMER_DATA_MANAGER_EQUITY_BARS_SYNC: &str = "data-manager-equity-bars-sync";

/// Consumer name for the data_manager equity bars export consumer.
pub const CONSUMER_DATA_MANAGER_EQUITY_BARS_EXPORT: &str = "data-manager-equity-bars-export";

/// Consumer name for the data_manager trading history export consumer.
pub const CONSUMER_DATA_MANAGER_TRADING_HISTORY_EXPORT: &str =
    "data-manager-trading-history-export";

/// Consumer name for the data_manager database backup consumer.
pub const CONSUMER_DATA_MANAGER_DATABASE_BACKUP: &str = "data-manager-database-backup";

// --- Database helpers ---

/// Inserts an event row by calling the `emit_event` PostgreSQL stored procedure.
/// The `events_notify` trigger fires `pg_notify` on the `events` channel automatically.
pub async fn emit_event(
    pool: &PgPool,
    event_type: EventType,
    payload: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "SELECT emit_event($1, $2::jsonb)",
        event_type.as_str(),
        payload
    )
    .execute(pool)
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
        assert_eq!(CONSUMER_ENSEMBLE_MANAGER, "ensemble-manager");
        assert_eq!(CONSUMER_PORTFOLIO_MANAGER, "portfolio-manager");
        assert_eq!(
            CONSUMER_PORTFOLIO_MANAGER_LIQUIDATION,
            "portfolio-manager-liquidation"
        );
        assert_eq!(
            CONSUMER_DATA_MANAGER_EQUITY_BARS_SYNC,
            "data-manager-equity-bars-sync"
        );
        assert_eq!(
            CONSUMER_DATA_MANAGER_EQUITY_BARS_EXPORT,
            "data-manager-equity-bars-export"
        );
        assert_eq!(
            CONSUMER_DATA_MANAGER_TRADING_HISTORY_EXPORT,
            "data-manager-trading-history-export"
        );
        assert_eq!(
            CONSUMER_DATA_MANAGER_DATABASE_BACKUP,
            "data-manager-database-backup"
        );
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
            assert!(get_consumer_offset(&lazy_pool(), CONSUMER_ENSEMBLE_MANAGER)
                .await
                .is_err());
        });
    }

    #[test]
    fn test_update_consumer_offset_compiles() {
        make_runtime().block_on(async {
            assert!(
                update_consumer_offset(&lazy_pool(), CONSUMER_PORTFOLIO_MANAGER, 42)
                    .await
                    .is_err()
            );
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
                events_after(&lazy_pool(), EventType::EquityBarsExportRequested, 0)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn test_query_event_payload_compiles() {
        make_runtime().block_on(async {
            assert!(
                query_event_payload(&lazy_pool(), EventType::EquityBarsExportRequested, 1)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn test_event_type_as_str_all_variants() {
        // Exhaustively verify every variant maps to its expected snake_case string.
        // This catches any future variant added without updating as_str().
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
                EventType::EquityBarsExportRequested,
                "equity_bars_export_requested",
            ),
            (
                EventType::EquityBarsExportStarted,
                "equity_bars_export_started",
            ),
            (
                EventType::EquityBarsExportCompleted,
                "equity_bars_export_completed",
            ),
            (
                EventType::EquityBarsExportErrored,
                "equity_bars_export_errored",
            ),
            (
                EventType::TradingHistoryExportRequested,
                "trading_history_export_requested",
            ),
            (
                EventType::TradingHistoryExportStarted,
                "trading_history_export_started",
            ),
            (
                EventType::TradingHistoryExportCompleted,
                "trading_history_export_completed",
            ),
            (
                EventType::TradingHistoryExportErrored,
                "trading_history_export_errored",
            ),
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
            EventType::EquityBarsExportRequested,
            EventType::EquityBarsExportStarted,
            EventType::EquityBarsExportCompleted,
            EventType::EquityBarsExportErrored,
            EventType::TradingHistoryExportRequested,
            EventType::TradingHistoryExportStarted,
            EventType::TradingHistoryExportCompleted,
            EventType::TradingHistoryExportErrored,
            EventType::DatabaseBackupRequested,
            EventType::DatabaseBackupStarted,
            EventType::DatabaseBackupCompleted,
            EventType::DatabaseBackupErrored,
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
