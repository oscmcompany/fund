//! Nightly export of the application's own tables to S3 as Parquet.
//!
//! Chained from a completed market data sync rather than scheduled, so it can never run against a
//! half-synced database. Everything it writes is archival: the trainer fetches its own data from
//! Massive, so a failed export costs a backup rather than the next day's model.
//!
//! **Bars and ticker metadata are deliberately absent.** Both used to be exported here and both are
//! written by the trainer under `data/`, from the same Massive grouped endpoint this application
//! syncs from — the same rows by two paths, and two writers of one fact is one too many. The
//! archive under [`crate::data::archive`] owns them and is their long-term record; what remains
//! here is what only this application knows.
//!
//! Two shapes. **Incremental** tables — events, predictions, account activities — are written per
//! session date under a Hive-partitioned key. **Snapshot** tables — pairs, account state — are
//! written whole each night, because a row can change after the day it was created (a pair opened
//! Monday closes Tuesday, and the closing is the interesting part).
//!
//! A failure on one table is logged and the rest continue; the caller receives the list so the
//! completion event can report it. That list is also the purge's gate: [`crate::data::purge`] runs
//! only when every dataset here wrote cleanly.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::aws::date_partitioned_key;
use crate::data::calendar::SessionDate;

/// What one nightly export accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExportSummary {
    /// `(dataset, rows)` for each table that exported cleanly.
    pub exported: Vec<(String, usize)>,
    /// `(dataset, error)` for each table that failed.
    pub failed: Vec<(String, String)>,
}

impl ExportSummary {
    /// Total rows written across every dataset that succeeded.
    pub fn total_rows(&self) -> usize {
        self.exported.iter().map(|(_, rows)| rows).sum()
    }

    /// Whether every dataset exported without error.
    pub fn is_clean(&self) -> bool {
        self.failed.is_empty()
    }
}

/// Exports every table to S3 for `date`.
///
/// Never returns `Err`: a per-table failure belongs in the summary, where the completion event can
/// report it, rather than aborting the remaining tables and the purge that follows.
pub async fn export_database(
    pool: &PgPool,
    s3_client: &S3Client,
    bucket: &str,
    date: SessionDate,
) -> ExportSummary {
    let mut summary = ExportSummary::default();

    macro_rules! export {
        ($dataset:expr, $prefix:expr, $frame:expr) => {
            match $frame.await {
                Ok(mut frame) => {
                    let key = date_partitioned_key($prefix, date.date());
                    match write_frame(s3_client, bucket, &key, &mut frame).await {
                        Ok(()) => summary
                            .exported
                            .push(($dataset.to_string(), frame.height())),
                        Err(error) => summary.failed.push(($dataset.to_string(), error)),
                    }
                }
                Err(error) => summary
                    .failed
                    .push(($dataset.to_string(), error.to_string())),
            }
        };
    }

    // Resolved once and passed to every incremental query. Bounding the timestamp column directly
    // keeps the predicate sargable; see `eastern_day_bounds`.
    let (start, end) = date.bounds();

    export!("events", "exports/events", events_frame(pool, start, end));
    export!(
        "equity_predictions",
        "exports/equity/predictions",
        predictions_frame(pool, start, end)
    );
    export!("equity_pairs", "exports/equity/pairs", pairs_frame(pool));
    export!(
        "account_snapshots",
        "exports/account/snapshots",
        account_snapshots_frame(pool)
    );
    export!(
        "account_activities",
        "exports/account/activities",
        account_activities_frame(pool, start, end)
    );

    info!(
        datasets = summary.exported.len(),
        rows = summary.total_rows(),
        failed = summary.failed.len(),
        date = %date,
        "Database export finished"
    );
    for (dataset, error) in &summary.failed {
        warn!(dataset, error, "Dataset failed to export");
    }
    summary
}

/// Serializes a frame to Parquet and puts it at `key`.
async fn write_frame(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    frame: &mut DataFrame,
) -> Result<(), String> {
    let mut buffer: Vec<u8> = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(frame)
        .map_err(|error| format!("failed to serialize Parquet: {error}"))?;

    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(buffer))
        .content_type("application/vnd.apache.parquet")
        .send()
        .await
        .map_err(|error| format!("failed to write s3://{bucket}/{key}: {error}"))?;

    info!(key, rows = frame.height(), "Dataset exported");
    Ok(())
}

async fn events_frame(
    pool: &PgPool,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<DataFrame, PolarsError> {
    let rows = sqlx::query!(
        r#"SELECT id AS "id!", event_type AS "event_type!", payload AS "payload!",
                  created_at AS "created_at!"
           FROM events
           WHERE created_at >= $1 AND created_at < $2
           ORDER BY id"#,
        start,
        end
    )
    .fetch_all(pool)
    .await
    .map_err(to_polars_error)?;

    DataFrame::new(vec![
        Column::new("id".into(), collect(&rows, |row| row.id)),
        Column::new(
            "event_type".into(),
            collect(&rows, |row| row.event_type.clone()),
        ),
        Column::new(
            "payload".into(),
            collect(&rows, |row| row.payload.to_string()),
        ),
        Column::new(
            "created_at".into(),
            collect(&rows, |row| row.created_at.timestamp_millis()),
        ),
    ])
}

async fn predictions_frame(
    pool: &PgPool,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<DataFrame, PolarsError> {
    let rows = sqlx::query!(
        r#"SELECT correlation_id AS "correlation_id!", model_run_id AS "model_run_id!",
                  ticker AS "ticker!", timestamp AS "timestamp!",
                  quantile_10 AS "quantile_10!", quantile_50 AS "quantile_50!",
                  quantile_90 AS "quantile_90!"
           FROM equity_predictions
           WHERE timestamp >= $1 AND timestamp < $2
           ORDER BY ticker"#,
        start,
        end
    )
    .fetch_all(pool)
    .await
    .map_err(to_polars_error)?;

    DataFrame::new(vec![
        Column::new(
            "correlation_id".into(),
            collect(&rows, |row| row.correlation_id.to_string()),
        ),
        Column::new(
            "model_run_id".into(),
            collect(&rows, |row| row.model_run_id.clone()),
        ),
        Column::new("ticker".into(), collect(&rows, |row| row.ticker.clone())),
        Column::new(
            "timestamp".into(),
            collect(&rows, |row| row.timestamp.timestamp_millis()),
        ),
        Column::new("quantile_10".into(), collect(&rows, |row| row.quantile_10)),
        Column::new("quantile_50".into(), collect(&rows, |row| row.quantile_50)),
        Column::new("quantile_90".into(), collect(&rows, |row| row.quantile_90)),
    ])
}

async fn pairs_frame(pool: &PgPool) -> Result<DataFrame, PolarsError> {
    let rows = sqlx::query!(
        r#"SELECT id AS "id!", pair_id AS "pair_id!", long_ticker AS "long_ticker!",
                  short_ticker AS "short_ticker!",
                  hedge_ratio::double precision AS "hedge_ratio!",
                  entry_z_score::double precision AS "entry_z_score!",
                  signal_strength::double precision AS "signal_strength!",
                  model_run_id, status AS "status!", opened_at AS "opened_at!", closed_at,
                  close_reason,
                  realized_profit_and_loss::double precision AS realized_profit_and_loss
           FROM equity_pairs
           ORDER BY opened_at"#
    )
    .fetch_all(pool)
    .await
    .map_err(to_polars_error)?;

    DataFrame::new(vec![
        Column::new("id".into(), collect(&rows, |row| row.id.to_string())),
        Column::new("pair_id".into(), collect(&rows, |row| row.pair_id.clone())),
        Column::new(
            "long_ticker".into(),
            collect(&rows, |row| row.long_ticker.clone()),
        ),
        Column::new(
            "short_ticker".into(),
            collect(&rows, |row| row.short_ticker.clone()),
        ),
        Column::new("hedge_ratio".into(), collect(&rows, |row| row.hedge_ratio)),
        Column::new(
            "entry_z_score".into(),
            collect(&rows, |row| row.entry_z_score),
        ),
        Column::new(
            "signal_strength".into(),
            collect(&rows, |row| row.signal_strength),
        ),
        Column::new(
            "model_run_id".into(),
            collect(&rows, |row| row.model_run_id.clone()),
        ),
        Column::new("status".into(), collect(&rows, |row| row.status.clone())),
        Column::new(
            "opened_at".into(),
            collect(&rows, |row| row.opened_at.timestamp_millis()),
        ),
        Column::new(
            "closed_at".into(),
            collect(&rows, |row| {
                row.closed_at.map(|value| value.timestamp_millis())
            }),
        ),
        Column::new(
            "close_reason".into(),
            collect(&rows, |row| row.close_reason.clone()),
        ),
        Column::new(
            "realized_profit_and_loss".into(),
            collect(&rows, |row| row.realized_profit_and_loss),
        ),
    ])
}

async fn account_snapshots_frame(pool: &PgPool) -> Result<DataFrame, PolarsError> {
    let rows = sqlx::query!(
        // The balances carry no `!`: a session reconstructed from portfolio history records equity
        // alone. Asserting them non-null would compile and then fail the whole nightly export at
        // runtime, on the first backfilled row it met.
        r#"SELECT session_date AS "session_date!",
                  equity::double precision AS "equity!",
                  cash::double precision,
                  buying_power::double precision,
                  long_market_value::double precision,
                  short_market_value::double precision
           FROM account_snapshots
           ORDER BY session_date"#
    )
    .fetch_all(pool)
    .await
    .map_err(to_polars_error)?;

    DataFrame::new(vec![
        Column::new(
            "session_date".into(),
            collect(&rows, |row| row.session_date.to_string()),
        ),
        Column::new("equity".into(), collect(&rows, |row| row.equity)),
        // The return types are spelled out to hold the query above to its word. Re-adding a `!` to
        // any of these columns would make the field an `f64` again and fail here at compile time,
        // rather than in production on the first reconstructed row.
        Column::new(
            "cash".into(),
            collect(&rows, |row| -> Option<f64> { row.cash }),
        ),
        Column::new(
            "buying_power".into(),
            collect(&rows, |row| -> Option<f64> { row.buying_power }),
        ),
        Column::new(
            "long_market_value".into(),
            collect(&rows, |row| -> Option<f64> { row.long_market_value }),
        ),
        Column::new(
            "short_market_value".into(),
            collect(&rows, |row| -> Option<f64> { row.short_market_value }),
        ),
    ])
}

async fn account_activities_frame(
    pool: &PgPool,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<DataFrame, PolarsError> {
    let rows = sqlx::query!(
        r#"SELECT id AS "id!", activity_type AS "activity_type!",
                  transaction_time AS "transaction_time!", ticker, side,
                  quantity::double precision AS quantity,
                  price::double precision AS price,
                  order_id
           FROM account_activities
           WHERE transaction_time >= $1 AND transaction_time < $2
           ORDER BY transaction_time"#,
        start,
        end
    )
    .fetch_all(pool)
    .await
    .map_err(to_polars_error)?;

    DataFrame::new(vec![
        Column::new("id".into(), collect(&rows, |row| row.id.clone())),
        Column::new(
            "activity_type".into(),
            collect(&rows, |row| row.activity_type.clone()),
        ),
        Column::new(
            "transaction_time".into(),
            collect(&rows, |row| row.transaction_time.timestamp_millis()),
        ),
        Column::new("ticker".into(), collect(&rows, |row| row.ticker.clone())),
        Column::new("side".into(), collect(&rows, |row| row.side.clone())),
        Column::new("quantity".into(), collect(&rows, |row| row.quantity)),
        Column::new("price".into(), collect(&rows, |row| row.price)),
        Column::new(
            "order_id".into(),
            collect(&rows, |row| row.order_id.clone()),
        ),
    ])
}

/// Projects one column out of a row slice.
fn collect<Row, Value>(rows: &[Row], extract: impl Fn(&Row) -> Value) -> Vec<Value> {
    rows.iter().map(extract).collect()
}

fn to_polars_error(error: sqlx::Error) -> PolarsError {
    PolarsError::ComputeError(error.to_string().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_totals_only_successful_datasets() {
        let summary = ExportSummary {
            exported: vec![("events".into(), 12), ("equity_pairs".into(), 3)],
            failed: vec![("account_activities".into(), "boom".into())],
        };
        assert_eq!(summary.total_rows(), 15);
        assert!(!summary.is_clean());
    }

    #[test]
    fn test_empty_summary_is_clean() {
        let summary = ExportSummary::default();
        assert_eq!(summary.total_rows(), 0);
        assert!(summary.is_clean());
    }

    #[test]
    fn test_collect_projects_a_column() {
        let rows = vec![(1_i64, "a"), (2, "b")];
        assert_eq!(collect(&rows, |row| row.0), vec![1, 2]);
        assert_eq!(collect(&rows, |row| row.1.to_string()), vec!["a", "b"]);
    }
}
