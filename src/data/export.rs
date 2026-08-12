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
//!
//! [`export_session_logs`] ships a third shape: whole local JSONL files, one object per session,
//! independent of the database entirely.

use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use serde_json::Value;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::aws::date_partitioned_key;
use crate::common::session_log::{file_name, session_from_file_name, SessionLog};
use crate::common::types::SessionDate;

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
        // No `!` on the balances: asserting them non-null compiles and then fails at runtime, on
        // the first reconstructed row.
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
        // Return types spelled out so that re-adding a `!` above fails here at compile time.
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

// ---------------------------------------------------------------------------
// Session log
// ---------------------------------------------------------------------------

/// S3 prefix the sealed sessions are written under.
pub const SESSION_LOG_PREFIX: &str = "exports/session_log";

/// Calendar days of sealed sessions kept on local disk after a clean export.
///
/// The window in which a bad Parquet conversion can still be repaired from the original bytes.
pub const SESSION_LOG_RETENTION_DAYS: i64 = 7;

/// What one export run accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionLogExportSummary {
    /// `(session_date, records)` for each session written to S3.
    pub exported: Vec<(NaiveDate, usize)>,
    /// `(session_date, error)` for each session that failed.
    pub failed: Vec<(NaiveDate, String)>,
    /// Sessions whose local file was deleted after the retention window.
    pub deleted: Vec<NaiveDate>,
    /// Lines skipped as unreadable, across every session.
    ///
    /// A torn final line per crashed session is expected; more than that means something else is
    /// wrong.
    pub unparsable_lines: usize,
}

impl SessionLogExportSummary {
    pub fn total_records(&self) -> usize {
        self.exported.iter().map(|(_, records)| records).sum()
    }

    pub fn is_clean(&self) -> bool {
        self.failed.is_empty()
    }
}

/// Converts every sealed session on disk to Parquet in S3, then deletes what has aged out.
///
/// Every file present is exported, not just the retention window: one whole file at a deterministic
/// key makes a repeat a byte-identical overwrite, so a failed run repairs itself. Local files are
/// deleted only after a clean run that skipped nothing.
pub async fn export_session_logs(
    log: &SessionLog,
    s3_client: &S3Client,
    bucket: &str,
    today: SessionDate,
) -> SessionLogExportSummary {
    let mut summary = SessionLogExportSummary::default();

    let mut sessions = match sealed_sessions(log.directory(), today) {
        Ok(sessions) => sessions,
        Err(error) => {
            warn!(%error, "Session log directory could not be read; nothing exported");
            return summary;
        }
    };
    sessions.sort();

    // The seal covers the reads and nothing else. Holding it across the uploads would leave a
    // concurrent `record` waiting on S3, and a log write that blocks on the network is the one
    // thing this module promises never to be.
    let frames = {
        let _sealed = log.seal().await;
        sessions
            .iter()
            .map(|session_date| {
                (
                    *session_date,
                    read_session_frame(&log.directory().join(file_name(*session_date))),
                )
            })
            .collect::<Vec<_>>()
    };

    // Only the sessions whose Parquet holds everything their original did. A session is deletable
    // when it uploaded cleanly *and* skipped nothing, and both are per-session questions: a run-wide
    // gate would let one bad line in one file stop every other file from ever aging out.
    let mut deletable: Vec<SessionDate> = Vec::with_capacity(sessions.len());
    let mut held_back: Vec<NaiveDate> = Vec::new();
    for (session_date, frame) in frames {
        match frame {
            Ok((mut frame, unparsable)) => {
                summary.unparsable_lines += unparsable;
                let key = date_partitioned_key(SESSION_LOG_PREFIX, session_date.date());
                match write_frame(s3_client, bucket, &key, &mut frame).await {
                    Ok(()) => {
                        summary.exported.push((session_date.date(), frame.height()));
                        // A skipped line is in the original and not in the Parquet, which makes the
                        // original its only copy. Keeping one file costs kilobytes; deleting it
                        // makes a merely unreadable record permanently gone.
                        match unparsable {
                            0 => deletable.push(session_date),
                            _ => held_back.push(session_date.date()),
                        }
                    }
                    Err(error) => summary.failed.push((session_date.date(), error)),
                }
            }
            Err(error) => summary.failed.push((session_date.date(), error)),
        }
    }

    summary.deleted = delete_aged_out(log.directory(), &deletable, today);
    for session_date in &held_back {
        warn!(
            %session_date,
            "Session log held back from deletion: its original holds lines the Parquet does not"
        );
    }

    info!(
        sessions = summary.exported.len(),
        records = summary.total_records(),
        failed = summary.failed.len(),
        deleted = summary.deleted.len(),
        unparsable_lines = summary.unparsable_lines,
        "Session log export finished"
    );
    for (session_date, error) in &summary.failed {
        warn!(%session_date, error, "Session log failed to export");
    }
    summary
}

/// Sessions on disk whose trading day has finished.
///
/// Today counts, because this runs after the close; a future-dated file does not, and exporting one
/// would seal a session still being written.
fn sealed_sessions(
    directory: &Path,
    today: SessionDate,
) -> Result<Vec<SessionDate>, std::io::Error> {
    let mut sessions = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(session_date) = session_from_file_name(&name) else {
            continue;
        };
        if session_date > today {
            warn!(%session_date, %today, "Session log is dated ahead of today; not sealed");
            continue;
        }
        sessions.push(session_date);
    }
    Ok(sessions)
}

/// Removes the local copies of sessions older than the retention window.
///
/// Called only after a clean run, so a session cannot age out without reaching S3. A file that will
/// not delete is a warning: a stale local copy costs disk and nothing else.
fn delete_aged_out(
    directory: &Path,
    sessions: &[SessionDate],
    today: SessionDate,
) -> Vec<NaiveDate> {
    let oldest_kept = today.plus_calendar_days(-SESSION_LOG_RETENTION_DAYS);
    let mut deleted = Vec::new();
    for session_date in sessions.iter().filter(|session| **session < oldest_kept) {
        let path = directory.join(file_name(*session_date));
        match std::fs::remove_file(&path) {
            Ok(()) => deleted.push(session_date.date()),
            Err(error) => warn!(
                path = %path.display(),
                %error,
                "Aged-out session log could not be deleted"
            ),
        }
    }
    deleted
}

/// Reads one session file into a frame, returning it with the number of lines skipped.
///
/// Discarding a torn final line is why the original is JSONL rather than Parquet.
fn read_session_frame(path: &Path) -> Result<(DataFrame, usize), String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;

    let mut schema_versions: Vec<Option<i64>> = Vec::new();
    let mut event_ids: Vec<Option<String>> = Vec::new();
    let mut correlation_ids: Vec<Option<String>> = Vec::new();
    let mut event_types: Vec<Option<String>> = Vec::new();
    let mut session_dates: Vec<Option<String>> = Vec::new();
    let mut created_ats: Vec<Option<i64>> = Vec::new();
    let mut payloads: Vec<Option<String>> = Vec::new();
    let mut unparsable = 0usize;

    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(Value::Object(record)) = serde_json::from_str::<Value>(line) else {
            unparsable += 1;
            continue;
        };
        let text = |key: &str| record.get(key).and_then(Value::as_str).map(str::to_string);

        // Every envelope column or none of the row. All six carry a query: correlation_id is the
        // join, session_date the partition filter, schema_version the shape. A row admitted with
        // any of them null is unreachable by the queries this archive exists to answer, and is
        // indistinguishable from a good one until someone counts.
        let (
            Some(schema_version),
            Some(event_id),
            Some(correlation_id),
            Some(event_type),
            Some(session_date),
            Some(created_at),
        ) = (
            record.get("schema_version").and_then(Value::as_i64),
            text("event_id"),
            text("correlation_id"),
            text("event_type"),
            text("session_date"),
            text("created_at").and_then(|stamp| {
                DateTime::parse_from_rfc3339(&stamp)
                    .ok()
                    .map(|instant| instant.timestamp_millis())
            }),
        )
        else {
            unparsable += 1;
            continue;
        };

        schema_versions.push(Some(schema_version));
        event_ids.push(Some(event_id));
        correlation_ids.push(Some(correlation_id));
        event_types.push(Some(event_type));
        session_dates.push(Some(session_date));
        created_ats.push(Some(created_at));
        payloads.push(record.get("payload").map(Value::to_string));
    }

    if unparsable > 0 {
        warn!(
            path = %path.display(),
            unparsable,
            "Skipped session log lines that would not parse"
        );
    }

    let frame = DataFrame::new(vec![
        Column::new("schema_version".into(), schema_versions),
        Column::new("event_id".into(), event_ids),
        Column::new("correlation_id".into(), correlation_ids),
        Column::new("event_type".into(), event_types),
        Column::new("session_date".into(), session_dates),
        Column::new("created_at".into(), created_ats),
        Column::new("payload".into(), payloads),
    ])
    .map_err(|error| format!("failed to build frame for {}: {error}", path.display()))?;

    Ok((frame, unparsable))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::*;
    use crate::common::session_log::{AccountObserved, Observation, Record};

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).expect("valid date"))
    }

    fn instant(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid instant")
            .with_timezone(&Utc)
    }

    fn account_observation() -> Observation {
        Observation::AccountObserved(AccountObserved {
            session_date: session(2026, 8, 11),
            equity: Some(104_812.55),
            cash: Some(12_000.0),
            buying_power: Some(200_000.0),
            long_market_value: Some(50_000.0),
            short_market_value: Some(-50_000.0),
        })
    }

    /// A directory unique to this run.
    ///
    /// The process and counter suffix stop two overlapping `cargo test` invocations deleting and
    /// recreating one path underneath each other.
    fn temporary_directory(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "fund-session-export-{name}-{}-{unique}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        directory
    }

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

    /// A crash mid-append leaves a partial final line. Discarding it is the whole reason the
    /// original is JSONL: the same truncation in a Parquet file loses the entire session.
    #[test]
    fn test_a_torn_final_line_is_skipped_rather_than_failing_the_session() {
        let directory = temporary_directory("torn");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let complete = serde_json::to_string(&Record::new(
            Uuid::nil(),
            instant("2026-08-11T14:35:00Z"),
            account_observation(),
        ))
        .expect("the record must serialize");
        std::fs::write(&path, format!("{complete}\n{{\"schema_version\":1,\"eve"))
            .expect("the file must be writable");

        let (frame, unparsable) = read_session_frame(&path).expect("the session must still read");
        assert_eq!(frame.height(), 1);
        assert_eq!(unparsable, 1);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Valid JSON is not enough: a row without an addressable envelope would reach Parquet with
    /// nulls no query could tell apart from a good record.
    #[test]
    fn test_a_record_missing_its_envelope_is_counted_unparsable() {
        let directory = temporary_directory("envelope");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let complete = serde_json::to_string(&Record::new(
            Uuid::nil(),
            instant("2026-08-11T20:15:00Z"),
            account_observation(),
        ))
        .expect("the record must serialize");
        let lines = [
            complete.as_str(),
            r#"{"schema_version":1,"event_type":"account_observed","created_at":"2026-08-11T20:15:00Z"}"#,
            r#"{"schema_version":1,"event_id":"a","event_type":"account_observed","created_at":"not a time"}"#,
        ]
        .join("\n");
        std::fs::write(&path, lines).expect("the file must be writable");

        let (frame, unparsable) = read_session_frame(&path).expect("the session must read");
        assert_eq!(
            frame.height(),
            1,
            "only the complete record reaches the frame"
        );
        assert_eq!(
            unparsable, 2,
            "a missing event_id and an unparseable instant"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn test_a_frame_carries_the_envelope_as_columns_and_the_payload_as_json() {
        let directory = temporary_directory("frame");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let record = Record::new(
            Uuid::nil(),
            instant("2026-08-11T20:15:00Z"),
            account_observation(),
        );
        std::fs::write(
            &path,
            format!(
                "{}\n",
                serde_json::to_string(&record).expect("the record must serialize")
            ),
        )
        .expect("the file must be writable");

        let (frame, _) = read_session_frame(&path).expect("the session must read");
        assert_eq!(
            frame.get_column_names(),
            [
                "schema_version",
                "event_id",
                "correlation_id",
                "event_type",
                "session_date",
                "created_at",
                "payload"
            ]
        );
        let payload = frame
            .column("payload")
            .expect("payload column")
            .str()
            .expect("payload is text")
            .get(0)
            .expect("one row");
        let parsed: Value = serde_json::from_str(payload).expect("the payload stays valid JSON");
        assert_eq!(parsed["equity"], 104_812.55);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Today's file is sealed because the export runs after the close; a future-dated one is not,
    /// and exporting it would seal a session still being written.
    #[test]
    fn test_only_sessions_up_to_today_are_sealed() {
        let directory = temporary_directory("sealed");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        for name in [
            "session-2026-08-09.jsonl",
            "session-2026-08-11.jsonl",
            "session-2026-08-12.jsonl",
            "notes.txt",
        ] {
            std::fs::write(directory.join(name), "").expect("the file must be writable");
        }

        let mut sealed = sealed_sessions(&directory, session(2026, 8, 11)).expect("readable");
        sealed.sort();
        assert_eq!(sealed, vec![session(2026, 8, 9), session(2026, 8, 11)]);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The frame survives a Parquet round trip with its envelope typed and its payload intact.
    ///
    /// This is the artifact DuckDB reads, so a serialization that silently dropped a column or
    /// widened the payload out of shape would take the whole archive with it.
    #[test]
    fn test_a_session_frame_round_trips_through_parquet() {
        let directory = temporary_directory("parquet");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let mut lines = String::new();
        for equity in [104_812.55_f64, 105_000.0] {
            let record = Record::new(
                Uuid::new_v4(),
                instant("2026-08-11T20:15:00Z"),
                Observation::AccountObserved(AccountObserved {
                    session_date: session(2026, 8, 11),
                    equity: Some(equity),
                    cash: Some(12_000.0),
                    buying_power: Some(200_000.0),
                    long_market_value: Some(50_000.0),
                    short_market_value: Some(-50_000.0),
                }),
            );
            lines.push_str(&serde_json::to_string(&record).expect("the record must serialize"));
            lines.push('\n');
        }
        std::fs::write(&path, lines).expect("the file must be writable");

        let (mut frame, _) = read_session_frame(&path).expect("the session must read");
        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer)
            .finish(&mut frame)
            .expect("the frame must serialize to Parquet");

        let restored = ParquetReader::new(std::io::Cursor::new(buffer))
            .finish()
            .expect("the Parquet must read back");
        assert_eq!(restored.height(), 2);
        assert_eq!(restored.get_column_names(), frame.get_column_names());
        // The instant survives as milliseconds, which is what lets a reader recover 16:15 Eastern.
        assert_eq!(
            restored
                .column("created_at")
                .expect("created_at column")
                .i64()
                .expect("created_at is an integer")
                .get(0),
            Some(instant("2026-08-11T20:15:00Z").timestamp_millis())
        );
        let payload = restored
            .column("payload")
            .expect("payload column")
            .str()
            .expect("payload is text")
            .get(0)
            .expect("one row");
        let parsed: Value = serde_json::from_str(payload).expect("the payload stays valid JSON");
        assert_eq!(parsed["equity"], 104_812.55);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// A row missing any envelope column is unreachable by the queries the archive exists for: no
    /// correlation join, no partition filter, no way to tell which shape it is. Admitting it with
    /// nulls would make it indistinguishable from a good row until someone counted.
    #[test]
    fn test_a_record_missing_any_envelope_column_is_counted_unparsable() {
        let directory = temporary_directory("envelope-columns");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        let complete = serde_json::to_string(&Record::new(
            Uuid::nil(),
            instant("2026-08-11T20:15:00Z"),
            account_observation(),
        ))
        .expect("the record must serialize");

        // One line per envelope column, each complete but for that column.
        let mut lines = vec![complete.clone()];
        for column in [
            "schema_version",
            "event_id",
            "correlation_id",
            "event_type",
            "session_date",
            "created_at",
        ] {
            let mut record: serde_json::Map<String, Value> =
                serde_json::from_str(&complete).expect("the record must parse");
            record.remove(column);
            lines.push(Value::Object(record).to_string());
        }
        std::fs::write(&path, lines.join("\n")).expect("the file must be writable");

        let (frame, unparsable) = read_session_frame(&path).expect("the session must read");
        assert_eq!(
            frame.height(),
            1,
            "only the complete record reaches Parquet"
        );
        assert_eq!(unparsable, 6, "one per missing envelope column");
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Holding a file back must not hold every other file back with it.
    ///
    /// The gate is per session because the reason for it is: one session's original holds a line
    /// its Parquet does not. A run-wide gate would let a single bad line stop the whole directory
    /// from ever ageing out, which on a 25 GB disk is a leak rather than a safeguard.
    #[test]
    fn test_one_session_held_back_does_not_hold_back_the_rest() {
        let directory = temporary_directory("held-back-is-per-session");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");

        let today = session(2026, 8, 11);
        let aged_out = [
            today.plus_calendar_days(-SESSION_LOG_RETENTION_DAYS - 2),
            today.plus_calendar_days(-SESSION_LOG_RETENTION_DAYS - 1),
        ];
        for session_date in aged_out {
            std::fs::write(directory.join(file_name(session_date)), "")
                .expect("the file must be writable");
        }

        // Only the first is deletable; the second skipped a line and is held back.
        let deleted = delete_aged_out(&directory, &aged_out[..1], today);

        assert_eq!(deleted, vec![aged_out[0].date()]);
        assert!(!directory.join(file_name(aged_out[0])).exists());
        assert!(
            directory.join(file_name(aged_out[1])).exists(),
            "the held-back session keeps its original"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// A file written across a mid-session deploy holds both shapes. Every v1 record must survive
    /// the stricter envelope check, or the deploy silently drops the morning from the archive.
    #[test]
    fn test_a_file_written_across_a_deploy_keeps_both_schema_versions() {
        let directory = temporary_directory("mixed-versions");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("session-2026-08-12.jsonl");

        // Verbatim v1 shapes, as the currently deployed build writes them.
        let v1 = [
            r#"{"schema_version":1,"event_id":"11111111-1111-1111-1111-111111111111","correlation_id":"22222222-2222-2222-2222-222222222222","session_date":"2026-08-12","created_at":"2026-08-12T14:35:00Z","event_type":"evaluation_pass","payload":{"universe_size":7000,"prices":[{"ticker":"AAAA","price":100.0,"price_source":"last_trade"}],"open_pairs":[],"screen_inputs":[],"excluded":[],"candidates":[]}}"#,
            r#"{"schema_version":1,"event_id":"33333333-3333-3333-3333-333333333333","correlation_id":"22222222-2222-2222-2222-222222222222","session_date":"2026-08-12","created_at":"2026-08-12T14:35:01Z","event_type":"order_submitted","payload":{"client_order_id":"k-long","ticker":"AAAA","side":"buy","shares":null,"notional":1000.0}}"#,
            r#"{"schema_version":1,"event_id":"44444444-4444-4444-4444-444444444444","correlation_id":"22222222-2222-2222-2222-222222222222","session_date":"2026-08-12","created_at":"2026-08-12T14:35:02Z","event_type":"liquidation_run","payload":{"pairs_closed":1,"positions_refused":0,"pairs_still_open":[]}}"#,
        ];
        // A v2 record appended after the restart, into the same file.
        let v2 = serde_json::to_string(&Record::new(
            Uuid::nil(),
            DateTime::parse_from_rfc3339("2026-08-12T18:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            Observation::PricesObserved(Default::default()),
        ))
        .unwrap();

        let mut lines = v1.join("\n");
        lines.push('\n');
        lines.push_str(&v2);
        std::fs::write(&path, lines).unwrap();

        let (frame, unparsable) = read_session_frame(&path).unwrap();
        assert_eq!(
            unparsable, 0,
            "no v1 record may be dropped by the new reader"
        );
        assert_eq!(frame.height(), 4, "three v1 records plus one v2");
        let versions = frame.column("schema_version").unwrap().i64().unwrap();
        assert_eq!(
            (versions.get(0), versions.get(3)),
            (Some(1), Some(2)),
            "both versions coexist in one file, each row carrying its own"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// Only what has aged out goes, and the window's own edge stays. The local file is the
    /// crash-exact original, so deleting one still inside the window would remove the only thing a
    /// bad Parquet conversion could be repaired from.
    #[test]
    fn test_only_sessions_past_the_retention_window_are_deleted() {
        let directory = temporary_directory("retention");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");

        let today = session(2026, 8, 11);
        let sessions = [
            today.plus_calendar_days(-SESSION_LOG_RETENTION_DAYS - 1),
            today.plus_calendar_days(-SESSION_LOG_RETENTION_DAYS),
            today.plus_calendar_days(-1),
            today,
        ];
        for session_date in sessions {
            std::fs::write(directory.join(file_name(session_date)), "")
                .expect("the file must be writable");
        }

        let deleted = delete_aged_out(&directory, &sessions, today);

        assert_eq!(deleted, vec![sessions[0].date()]);
        assert!(!directory.join(file_name(sessions[0])).exists());
        for session_date in &sessions[1..] {
            assert!(
                directory.join(file_name(*session_date)).exists(),
                "{session_date} is still inside the window"
            );
        }
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The key is a pure function of the session, which is what makes a repeat export an overwrite
    /// rather than a duplicate — and why there is no cursor to keep in sync.
    #[test]
    fn test_the_export_key_is_determined_by_the_session_alone() {
        let key = date_partitioned_key(SESSION_LOG_PREFIX, session(2026, 8, 11).date());
        assert_eq!(
            key,
            "exports/session_log/year=2026/month=08/day=11/data.parquet"
        );
        assert_eq!(
            key,
            date_partitioned_key(SESSION_LOG_PREFIX, session(2026, 8, 11).date())
        );
    }
}
