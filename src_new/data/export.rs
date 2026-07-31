//! Nightly export of the database to S3 as Parquet.
//!
//! Chained from a completed market data sync rather than scheduled, so it can never run against a
//! half-synced database. Everything it writes is archival: the trainer fetches its own data from
//! Alpaca on its own machine, so a failed export costs a backup rather than the next day's model.
//!
//! Two export shapes, because the tables have two shapes. **Incremental** tables — events,
//! predictions, bars — are written for one session date under a Hive-partitioned key, so a day's
//! rows land in their own object and the history accumulates. **Snapshot** tables — pairs,
//! account state, ticker metadata — are written whole each night, because they are small and
//! because a row can change after the day it was created (a pair opened on Monday closes on
//! Tuesday, and the closing is the interesting part).
//!
//! A failure on one table is logged and the rest continue. Exporting six of seven tables is
//! strictly better than exporting none, and the caller receives the list of failures so the
//! completion event can report them.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::aws::date_partitioned_key;
use crate::data::calendar::eastern_day_bounds;

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
    date: NaiveDate,
) -> ExportSummary {
    let mut summary = ExportSummary::default();

    macro_rules! export {
        ($dataset:expr, $prefix:expr, $frame:expr) => {
            match $frame.await {
                Ok(mut frame) => {
                    let key = date_partitioned_key($prefix, date);
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
    let (start, end) = eastern_day_bounds(date);

    export!("events", "exports/events", events_frame(pool, start, end));
    export!(
        "equity_predictions",
        "exports/equity/predictions",
        predictions_frame(pool, start, end)
    );
    export!(
        "equity_bars",
        "exports/equity/bars",
        bars_frame(pool, start, end)
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
    export!(
        "equity_details",
        "exports/equity/details",
        crate::data::details::load_details_dataframe(pool)
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

/// Every interval present for the session, not just daily.
///
/// Filtering to one interval here would leave intraday rows with no S3 archive at all, and
/// TimescaleDB's retention policy deletes them on the same 90-day window regardless. The interval
/// travels as a column so one object still holds the session.
async fn bars_frame(
    pool: &PgPool,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<DataFrame, PolarsError> {
    let rows = sqlx::query!(
        r#"SELECT ticker AS "ticker!", bar_interval AS "bar_interval!",
                  timestamp AS "timestamp!", open_price AS "open_price!",
                  high_price AS "high_price!", low_price AS "low_price!",
                  close_price AS "close_price!", volume AS "volume!",
                  volume_weighted_average_price, transactions
           FROM equity_bars
           WHERE timestamp >= $1 AND timestamp < $2
           ORDER BY bar_interval, ticker"#,
        start,
        end
    )
    .fetch_all(pool)
    .await
    .map_err(to_polars_error)?;

    DataFrame::new(vec![
        Column::new("ticker".into(), collect(&rows, |row| row.ticker.clone())),
        Column::new(
            "bar_interval".into(),
            collect(&rows, |row| row.bar_interval.clone()),
        ),
        Column::new(
            "timestamp".into(),
            collect(&rows, |row| row.timestamp.timestamp_millis()),
        ),
        Column::new("open_price".into(), collect(&rows, |row| row.open_price)),
        Column::new("high_price".into(), collect(&rows, |row| row.high_price)),
        Column::new("low_price".into(), collect(&rows, |row| row.low_price)),
        Column::new("close_price".into(), collect(&rows, |row| row.close_price)),
        Column::new("volume".into(), collect(&rows, |row| row.volume)),
        Column::new(
            "volume_weighted_average_price".into(),
            collect(&rows, |row| row.volume_weighted_average_price),
        ),
        Column::new(
            "transactions".into(),
            collect(&rows, |row| row.transactions),
        ),
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
        r#"SELECT session_date AS "session_date!",
                  equity::double precision AS "equity!",
                  last_equity::double precision AS "last_equity!",
                  cash::double precision AS "cash!",
                  buying_power::double precision AS "buying_power!",
                  long_market_value::double precision AS "long_market_value!",
                  short_market_value::double precision AS "short_market_value!"
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
        Column::new("last_equity".into(), collect(&rows, |row| row.last_equity)),
        Column::new("cash".into(), collect(&rows, |row| row.cash)),
        Column::new(
            "buying_power".into(),
            collect(&rows, |row| row.buying_power),
        ),
        Column::new(
            "long_market_value".into(),
            collect(&rows, |row| row.long_market_value),
        ),
        Column::new(
            "short_market_value".into(),
            collect(&rows, |row| row.short_market_value),
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
            failed: vec![("equity_bars".into(), "boom".into())],
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
