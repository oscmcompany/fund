//! Nightly export of what only this application knows: its own tables, its journal, and its logs.
//!
//! Chained from a completed market data sync rather than scheduled, so it never runs mid-sync.

use std::path::{Path, PathBuf};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use serde_json::Value;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::aws::date_partitioned_key;
use crate::common::journal::{file_name, session_from_file_name, Journal};
use crate::common::types::{Dataset, SessionDate};

/// What one nightly export accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExportSummary {
    /// `(dataset, rows)` for each table that exported cleanly.
    pub exported: Vec<(Dataset, usize)>,
    /// `(dataset, error)` for each table that failed.
    pub failed: Vec<(Dataset, String)>,
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
/// Two shapes: the incremental tables are written per session date under a Hive-partitioned key,
/// while pairs and account state are written whole each night, because a row can change after the
/// day it was created — a pair opened Monday closes Tuesday, and the closing is the interesting
/// part. Never returns `Err`: a per-table failure belongs in the summary, which is also the purge's
/// gate, rather than aborting the tables behind it.
pub async fn export_database(
    pool: &PgPool,
    s3_client: &S3Client,
    bucket: &str,
    date: SessionDate,
) -> ExportSummary {
    let mut summary = ExportSummary::default();

    macro_rules! export {
        ($dataset:expr, $frame:expr) => {
            match $frame.await {
                Ok(mut frame) => {
                    let key = date_partitioned_key($dataset.prefix(), date.date());
                    match write_frame(s3_client, bucket, &key, &mut frame).await {
                        Ok(()) => summary.exported.push(($dataset, frame.height())),
                        Err(error) => summary.failed.push(($dataset, error)),
                    }
                }
                Err(error) => summary.failed.push(($dataset, error.to_string())),
            }
        };
    }

    // Resolved once and passed to every incremental query. Bounding the timestamp column directly
    // keeps the predicate sargable; see `eastern_day_bounds`.
    let (start, end) = date.bounds();

    export!(Dataset::Events, events_frame(pool, start, end));
    export!(
        Dataset::EquityPredictions,
        predictions_frame(pool, start, end)
    );
    export!(Dataset::EquityPairs, pairs_frame(pool));
    export!(Dataset::AccountSnapshots, account_snapshots_frame(pool));
    export!(
        Dataset::AccountActivities,
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
        warn!(%dataset, error, "Dataset failed to export");
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

/// S3 prefix the sealed sessions are written under.
pub const JOURNAL_PREFIX: &str = "exports/journal";

/// Calendar days of sealed sessions kept on local disk after a clean export.
///
/// The window in which a bad Parquet conversion can still be repaired from the original bytes.
pub const JOURNAL_RETENTION_DAYS: i64 = 7;

/// What one export run accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JournalExportSummary {
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

impl JournalExportSummary {
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
pub async fn export_journals(
    journal: &Journal,
    s3_client: &S3Client,
    bucket: &str,
    today: SessionDate,
) -> JournalExportSummary {
    let mut summary = JournalExportSummary::default();

    let mut sessions = match sealed_sessions(journal.directory(), today) {
        Ok(sessions) => sessions,
        Err(error) => {
            warn!(%error, "Journal directory could not be read; nothing exported");
            return summary;
        }
    };
    sessions.sort();

    // The seal covers the reads and nothing else. Holding it across the uploads would leave a
    // concurrent `record` waiting on S3, and a journal write that blocks on the network is the one
    // thing this module promises never to be.
    let frames = {
        let _sealed = journal.seal().await;
        sessions
            .iter()
            .map(|session_date| {
                (
                    *session_date,
                    read_journal_frame(&journal.directory().join(file_name(*session_date))),
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
                // A file that yielded no rows at all has nothing to ship, and the key is
                // deterministic: uploading the empty frame would replace a good object from an
                // earlier run with one holding none of it.
                if frame.height() == 0 && unparsable > 0 {
                    summary.failed.push((
                        session_date.date(),
                        format!("every one of {unparsable} lines was unparsable"),
                    ));
                    continue;
                }
                let key = date_partitioned_key(JOURNAL_PREFIX, session_date.date());
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

    summary.deleted = delete_aged_out(journal.directory(), &deletable, today);
    for session_date in &held_back {
        warn!(
            %session_date,
            "Journal held back from deletion: its original holds lines the Parquet does not"
        );
    }

    info!(
        sessions = summary.exported.len(),
        records = summary.total_records(),
        failed = summary.failed.len(),
        deleted = summary.deleted.len(),
        unparsable_lines = summary.unparsable_lines,
        "Journal export finished"
    );
    for (session_date, error) in &summary.failed {
        warn!(%session_date, error, "Journal failed to export");
    }
    summary
}

/// S3 prefix the diagnostic logs are written under, partitioned by service.
pub const LOG_PREFIX: &str = "exports/logs";

/// Calendar days of exported logs kept on local disk.
///
/// The same window the journal keeps, for the same reason: long enough that a bad conversion can
/// still be repaired from the original bytes.
pub const LOG_RETENTION_DAYS: i64 = 7;

/// What one log export run accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogExportSummary {
    /// `(date, service, lines)` for each file written to S3.
    pub exported: Vec<(NaiveDate, String, usize)>,
    /// `(date, service, error)` for each file that did not write.
    pub failed: Vec<(NaiveDate, String, String)>,
    /// Files deleted from local disk, which had uploaded cleanly and aged out.
    pub deleted: Vec<NaiveDate>,
    /// Lines the Parquet does not hold, across every file this run read.
    pub unparsable_lines: usize,
}

impl LogExportSummary {
    pub fn total_lines(&self) -> usize {
        self.exported.iter().map(|(_, _, lines)| lines).sum()
    }
}

/// Converts every rolled log file on disk to Parquet in S3, then deletes what has aged out.
///
/// Follows [`export_journals`] — one whole file at a deterministic key, so a repeat is an
/// overwrite and a failed run repairs itself — with one difference it cannot avoid. The journal is
/// sealed across its reads and the log has no seal: the appender owns the file and keeps writing.
/// Today's object is therefore a snapshot that the next run replaces, and a line torn mid-write is
/// counted unparsable exactly as a torn journal line is.
pub async fn export_logs(
    directory: &Path,
    s3_client: &S3Client,
    bucket: &str,
    today: SessionDate,
) -> LogExportSummary {
    let mut summary = LogExportSummary::default();

    let mut files = match rolled_log_files(directory) {
        Ok(files) => files,
        Err(error) => {
            warn!(%error, "Log directory could not be read; nothing exported");
            return summary;
        }
    };
    files.sort();

    let mut deletable: Vec<NaiveDate> = Vec::new();
    for (date, service, path) in files {
        let (mut frame, unparsable) = match read_log_frame(&path) {
            Ok(read) => read,
            Err(error) => {
                summary.failed.push((date, service, error));
                continue;
            }
        };
        summary.unparsable_lines += unparsable;

        // Nothing to ship and nothing readable, so uploading would replace a good object from an
        // earlier run with one holding none of it -- the guard `export_journals` carries.
        if frame.height() == 0 && unparsable > 0 {
            summary.failed.push((
                date,
                service,
                format!("every one of {unparsable} lines was unparsable"),
            ));
            continue;
        }

        let key = date_partitioned_key(&format!("{LOG_PREFIX}/service={service}"), date);
        match write_frame(s3_client, bucket, &key, &mut frame).await {
            Ok(()) => {
                summary.exported.push((date, service, frame.height()));
                if unparsable == 0 {
                    deletable.push(date);
                }
            }
            Err(error) => summary.failed.push((date, service, error)),
        }
    }

    let oldest_kept = today.plus_calendar_days(-LOG_RETENTION_DAYS).date();
    summary.deleted = delete_aged_out_logs(directory, &deletable, oldest_kept);

    info!(
        files = summary.exported.len(),
        lines = summary.total_lines(),
        failed = summary.failed.len(),
        deleted = summary.deleted.len(),
        unparsable_lines = summary.unparsable_lines,
        "Log export finished"
    );
    for (date, service, error) in &summary.failed {
        warn!(%date, service, error, "Log failed to export");
    }
    summary
}

/// Every `<date>.<service>.log` in the directory, as its date, service, and path.
///
/// A name that does not parse is skipped rather than failing the run: the directory is shared with
/// whatever else writes there, and an unrecognised file is not this function's to interpret.
fn rolled_log_files(directory: &Path) -> Result<Vec<(NaiveDate, String, PathBuf)>, String> {
    let mut files = Vec::new();
    let entries = std::fs::read_dir(directory)
        .map_err(|error| format!("failed to read {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("failed to read a directory entry: {error}"))?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some((date, service)) = split_log_file_name(&name) else {
            continue;
        };
        files.push((date, service, entry.path()));
    }
    Ok(files)
}

/// Splits `2026-08-17.fund.log` into its date and service.
fn split_log_file_name(name: &str) -> Option<(NaiveDate, String)> {
    let remainder = name.strip_suffix(".log")?;
    let (date, service) = remainder.split_once('.')?;
    let date = date.parse::<NaiveDate>().ok()?;
    match service.is_empty() {
        true => None,
        false => Some((date, service.to_string())),
    }
}

/// Reads one log file into a frame, returning it with the number of lines skipped.
///
/// `fields` stays a JSON string rather than becoming columns: every call site puts different keys
/// in it, so a fixed schema would either lose them or grow a column per message.
fn read_log_frame(path: &Path) -> Result<(DataFrame, usize), String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;

    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut levels: Vec<Option<String>> = Vec::new();
    let mut targets: Vec<Option<String>> = Vec::new();
    let mut messages: Vec<Option<String>> = Vec::new();
    let mut field_blobs: Vec<Option<String>> = Vec::new();
    let mut unparsable = 0usize;

    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(Value::Object(record)) = serde_json::from_str::<Value>(line) else {
            unparsable += 1;
            continue;
        };
        let text = |key: &str| record.get(key).and_then(Value::as_str).map(str::to_string);

        // The timestamp and the level are what every query filters on, so a line missing either is
        // unreachable rather than merely thin.
        let (Some(timestamp), Some(level)) = (
            text("timestamp").and_then(|stamp| {
                DateTime::parse_from_rfc3339(&stamp)
                    .ok()
                    .map(|instant| instant.timestamp_millis())
            }),
            text("level"),
        ) else {
            unparsable += 1;
            continue;
        };

        timestamps.push(Some(timestamp));
        levels.push(Some(level));
        targets.push(text("target"));
        messages.push(
            record
                .get("fields")
                .and_then(|fields| fields.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string),
        );
        field_blobs.push(record.get("fields").map(Value::to_string));
    }

    if unparsable > 0 {
        warn!(
            path = %path.display(),
            unparsable,
            "Skipped log lines that would not parse"
        );
    }

    let frame = DataFrame::new(vec![
        Column::new("timestamp".into(), timestamps),
        Column::new("level".into(), levels),
        Column::new("target".into(), targets),
        Column::new("message".into(), messages),
        Column::new("fields".into(), field_blobs),
    ])
    .map_err(|error| format!("failed to build a log frame: {error}"))?;
    Ok((frame, unparsable))
}

/// Removes local log files older than the retention window.
///
/// The window is what keeps the appender's own file safe: `oldest_kept` is a whole retention period
/// behind today, so a file still open for writing is never old enough to qualify.
fn delete_aged_out_logs(
    directory: &Path,
    dates: &[NaiveDate],
    oldest_kept: NaiveDate,
) -> Vec<NaiveDate> {
    let mut deleted = Vec::new();
    let aged_out: std::collections::BTreeSet<NaiveDate> = dates
        .iter()
        .filter(|date| **date < oldest_kept)
        .copied()
        .collect();
    let Ok(files) = rolled_log_files(directory) else {
        return deleted;
    };
    for (date, _, path) in files
        .into_iter()
        .filter(|(date, _, _)| aged_out.contains(date))
    {
        match std::fs::remove_file(&path) {
            Ok(()) => deleted.push(date),
            Err(error) => warn!(
                path = %path.display(),
                %error,
                "Aged-out log could not be deleted"
            ),
        }
    }
    deleted.sort();
    deleted.dedup();
    deleted
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
            warn!(%session_date, %today, "Journal is dated ahead of today; not sealed");
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
    let oldest_kept = today.plus_calendar_days(-JOURNAL_RETENTION_DAYS);
    let mut deleted = Vec::new();
    for session_date in sessions.iter().filter(|session| **session < oldest_kept) {
        let path = directory.join(file_name(*session_date));
        match std::fs::remove_file(&path) {
            Ok(()) => deleted.push(session_date.date()),
            Err(error) => warn!(
                path = %path.display(),
                %error,
                "Aged-out journal could not be deleted"
            ),
        }
    }
    deleted
}

/// Reads one session file into a frame, returning it with the number of lines skipped.
///
/// Discarding a torn final line is why the original is JSONL rather than Parquet.
fn read_journal_frame(path: &Path) -> Result<(DataFrame, usize), String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;

    let mut schema_versions: Vec<Option<i64>> = Vec::new();
    let mut event_ids: Vec<Option<String>> = Vec::new();
    let mut correlation_ids: Vec<Option<String>> = Vec::new();
    let mut event_types: Vec<Option<String>> = Vec::new();
    let mut session_dates: Vec<Option<String>> = Vec::new();
    let mut timestamps: Vec<Option<i64>> = Vec::new();
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
            Some(timestamp),
        ) = (
            record.get("schema_version").and_then(Value::as_i64),
            text("event_id"),
            text("correlation_id"),
            text("event_type"),
            text("session_date"),
            text("timestamp").and_then(|stamp| {
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
        timestamps.push(Some(timestamp));
        payloads.push(record.get("payload").map(Value::to_string));
    }

    if unparsable > 0 {
        warn!(
            path = %path.display(),
            unparsable,
            "Skipped journal lines that would not parse"
        );
    }

    let frame = DataFrame::new(vec![
        Column::new("schema_version".into(), schema_versions),
        Column::new("event_id".into(), event_ids),
        Column::new("correlation_id".into(), correlation_ids),
        Column::new("event_type".into(), event_types),
        Column::new("session_date".into(), session_dates),
        Column::new("timestamp".into(), timestamps),
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
    use crate::common::journal::{AccountObserved, Observation, Record};

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
            equity: "104812.55".parse().expect("valid decimal"),
            cash: "12000.07".parse().expect("valid decimal"),
            buying_power: "200000.00".parse().expect("valid decimal"),
            long_market_value: "50000.00".parse().expect("valid decimal"),
            short_market_value: "-50000.01".parse().expect("valid decimal"),
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
            "fund-journal-export-{name}-{}-{unique}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        directory
    }

    #[test]
    fn test_summary_totals_only_successful_datasets() {
        let summary = ExportSummary {
            exported: vec![(Dataset::Events, 12), (Dataset::EquityPairs, 3)],
            failed: vec![(Dataset::AccountActivities, "boom".into())],
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

        let (frame, unparsable) = read_journal_frame(&path).expect("the session must still read");
        assert_eq!(frame.height(), 1);
        assert_eq!(unparsable, 1);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The appender names files `<date>.<service>.log`, and the service is what keys the S3
    /// partition — two services on one date would otherwise write the same object.
    #[test]
    fn test_a_log_file_name_splits_into_its_date_and_service() {
        assert_eq!(
            split_log_file_name("2026-08-17.fund.log"),
            Some((
                "2026-08-17".parse::<NaiveDate>().expect("a valid date"),
                "fund".to_string()
            ))
        );
        assert_eq!(
            split_log_file_name("2026-08-17.tide-model-trainer.log"),
            Some((
                "2026-08-17".parse::<NaiveDate>().expect("a valid date"),
                "tide-model-trainer".to_string()
            ))
        );
        // Anything else in the directory belongs to somebody else.
        assert_eq!(split_log_file_name("fund.log"), None);
        assert_eq!(split_log_file_name("2026-08-17.log"), None);
        assert_eq!(split_log_file_name("not-a-date.fund.log"), None);
        assert_eq!(split_log_file_name("2026-08-17.fund.txt"), None);
    }

    /// The log directory is shared with whatever else writes there, so a name this does not
    /// recognise is skipped rather than guessed at or fatal.
    #[test]
    fn test_only_recognisable_log_names_are_collected() {
        let directory = temporary_directory("logs-foreign");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        for name in [
            "2026-08-17.fund.log",
            "portfolio-manager-errors.log",
            "sessions",
            "2026-08-17.fund.log.gz",
        ] {
            std::fs::write(directory.join(name), "{}\n").expect("the file must be writable");
        }

        let mut collected: Vec<String> = rolled_log_files(&directory)
            .expect("the directory must read")
            .into_iter()
            .map(|(date, service, _)| format!("{date}.{service}"))
            .collect();
        collected.sort();
        assert_eq!(collected, vec!["2026-08-17.fund"]);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The log has no seal, so the file is live while it is read: the last line can be half-written.
    #[test]
    fn test_a_torn_log_line_is_skipped_rather_than_failing_the_file() {
        let directory = temporary_directory("logs-torn");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("2026-08-17.fund.log");
        std::fs::write(
            &path,
            "{\"timestamp\":\"2026-08-17T20:15:00Z\",\"level\":\"INFO\",\"target\":\"fund\",\
             \"fields\":{\"message\":\"Starting data sync\"}}\n{\"timestamp\":\"2026-08-1",
        )
        .expect("the file must be writable");

        let (frame, unparsable) = read_log_frame(&path).expect("the file must read");
        assert_eq!(frame.height(), 1);
        assert_eq!(unparsable, 1);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// A line with no timestamp or no level cannot be filtered on, so it is unreachable by every
    /// query the archive exists to answer rather than merely thin.
    #[test]
    fn test_a_log_line_without_a_timestamp_or_level_is_counted_unparsable() {
        let directory = temporary_directory("logs-envelope");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("2026-08-17.fund.log");
        std::fs::write(
            &path,
            "{\"timestamp\":\"2026-08-17T20:15:00Z\",\"level\":\"WARN\",\"fields\":{}}\n\
             {\"level\":\"INFO\",\"fields\":{}}\n\
             {\"timestamp\":\"2026-08-17T20:16:00Z\",\"fields\":{}}\n",
        )
        .expect("the file must be writable");

        let (frame, unparsable) = read_log_frame(&path).expect("the file must read");
        assert_eq!(frame.height(), 1);
        assert_eq!(
            unparsable, 2,
            "one missing a level, one missing a timestamp"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The message is lifted out of `fields` into its own column because it is what a human scans,
    /// while the rest of `fields` stays a JSON string — every call site puts different keys there.
    #[test]
    fn test_the_log_message_is_lifted_out_of_its_fields() {
        let directory = temporary_directory("logs-message");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("2026-08-17.fund.log");
        std::fs::write(
            &path,
            "{\"timestamp\":\"2026-08-17T20:15:00Z\",\"level\":\"INFO\",\"target\":\"fund::data\",\
             \"fields\":{\"message\":\"Journal export finished\",\"sessions\":3}}\n",
        )
        .expect("the file must be writable");

        let (frame, _) = read_log_frame(&path).expect("the file must read");
        let message = frame.column("message").expect("the column must exist");
        assert_eq!(
            message.str().expect("a string column").get(0),
            Some("Journal export finished")
        );
        let fields = frame.column("fields").expect("the column must exist");
        assert!(
            fields
                .str()
                .expect("a string column")
                .get(0)
                .expect("a value")
                .contains("\"sessions\":3"),
            "the rest of the fields survive as JSON"
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The file the appender still holds open must survive. The retention window is what protects
    /// it — `oldest_kept` is a whole period behind today, so today can never be old enough — which
    /// is why there is no separate guard against deleting it.
    #[test]
    fn test_the_retention_window_leaves_the_open_log_alone() {
        let directory = temporary_directory("logs-retention");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let today = session(2026, 8, 17);
        let oldest_kept = today.plus_calendar_days(-LOG_RETENTION_DAYS).date();
        let stale = today.plus_calendar_days(-LOG_RETENTION_DAYS - 1).date();
        // The boundary date itself is kept, so the window is half-open: `<=` would take a file the
        // retention period still covers.
        let dates = [today.date(), oldest_kept, stale];
        for date in dates {
            std::fs::write(directory.join(format!("{date}.fund.log")), "{}\n")
                .expect("the file must be writable");
        }
        assert!(
            oldest_kept < today.date(),
            "the window is what does the work"
        );

        let deleted = delete_aged_out_logs(&directory, &dates, oldest_kept);
        assert_eq!(
            deleted,
            vec![stale],
            "only the file past the window ages out"
        );
        assert!(directory
            .join(format!("{}.fund.log", today.date()))
            .exists());
        assert!(directory.join(format!("{oldest_kept}.fund.log")).exists());
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// The state `export_journals` refuses to upload. A file whose every line is unparsable still
    /// reads as `Ok`, and the session key is deterministic, so shipping this frame would replace a
    /// good object from an earlier run with one holding none of it.
    #[test]
    fn test_a_wholly_unparsable_session_yields_an_empty_frame() {
        let directory = temporary_directory("unreadable");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        let path = directory.join("session-2026-08-11.jsonl");
        std::fs::write(&path, "not json at all\n{\"schema_version\":3}\n")
            .expect("the file must be writable");

        let (frame, unparsable) = read_journal_frame(&path).expect("the session must read");
        assert_eq!(frame.height(), 0);
        assert_eq!(unparsable, 2);
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
            r#"{"schema_version":3,"event_type":"account_observed","timestamp":"2026-08-11T20:15:00Z"}"#,
            r#"{"schema_version":3,"event_id":"a","event_type":"account_observed","timestamp":"not a time"}"#,
        ]
        .join("\n");
        std::fs::write(&path, lines).expect("the file must be writable");

        let (frame, unparsable) = read_journal_frame(&path).expect("the session must read");
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

        let (frame, _) = read_journal_frame(&path).expect("the session must read");
        assert_eq!(
            frame.get_column_names(),
            [
                "schema_version",
                "event_id",
                "correlation_id",
                "event_type",
                "session_date",
                "timestamp",
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
        for equity in ["104812.55", "105000.00"] {
            let record = Record::new(
                Uuid::new_v4(),
                instant("2026-08-11T20:15:00Z"),
                Observation::AccountObserved(AccountObserved {
                    session_date: session(2026, 8, 11),
                    equity: equity.parse().expect("valid decimal"),
                    cash: "12000.07".parse().expect("valid decimal"),
                    buying_power: "200000.00".parse().expect("valid decimal"),
                    long_market_value: "50000.00".parse().expect("valid decimal"),
                    short_market_value: "-50000.01".parse().expect("valid decimal"),
                }),
            );
            lines.push_str(&serde_json::to_string(&record).expect("the record must serialize"));
            lines.push('\n');
        }
        std::fs::write(&path, lines).expect("the file must be writable");

        let (mut frame, _) = read_journal_frame(&path).expect("the session must read");
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
                .column("timestamp")
                .expect("timestamp column")
                .i64()
                .expect("the timestamp is an integer")
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
            "timestamp",
        ] {
            let mut record: serde_json::Map<String, Value> =
                serde_json::from_str(&complete).expect("the record must parse");
            record.remove(column);
            lines.push(Value::Object(record).to_string());
        }
        std::fs::write(&path, lines.join("\n")).expect("the file must be writable");

        let (frame, unparsable) = read_journal_frame(&path).expect("the session must read");
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
            today.plus_calendar_days(-JOURNAL_RETENTION_DAYS - 2),
            today.plus_calendar_days(-JOURNAL_RETENTION_DAYS - 1),
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

    /// Only what has aged out goes, and the window's own edge stays. The local file is the
    /// crash-exact original, so deleting one still inside the window would remove the only thing a
    /// bad Parquet conversion could be repaired from.
    #[test]
    fn test_only_sessions_past_the_retention_window_are_deleted() {
        let directory = temporary_directory("retention");
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");

        let today = session(2026, 8, 11);
        let sessions = [
            today.plus_calendar_days(-JOURNAL_RETENTION_DAYS - 1),
            today.plus_calendar_days(-JOURNAL_RETENTION_DAYS),
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
        let key = date_partitioned_key(JOURNAL_PREFIX, session(2026, 8, 11).date());
        assert_eq!(
            key,
            "exports/journal/year=2026/month=08/day=11/data.parquet"
        );
        assert_eq!(
            key,
            date_partitioned_key(JOURNAL_PREFIX, session(2026, 8, 11).date())
        );
    }
}
