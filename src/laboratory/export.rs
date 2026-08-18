//! Ships sealed laboratory journal days to S3, one object per experiment type per UTC date.
//!
//! Triggered explicitly: the laboratory has no scheduled pass to hang this off.

use std::collections::BTreeMap;
use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate};
use polars::prelude::*;
use serde_json::Value;
use tracing::{info, warn};

use crate::common::aws::date_partitioned_key;
use crate::laboratory::journal::{date_from_file_name, file_name, Journal};

/// S3 prefix the laboratory writes under.
///
/// Its own, and not the application's `exports/journal`: both partition by date, so one prefix
/// would mean two producers overwriting each other's object for the same day.
pub const EXPERIMENT_PREFIX: &str = "exports/laboratory/experiments";

/// Age, in days, past which a shipped file is deleted.
///
/// A file exactly this old is kept, matching `data::export::JOURNAL_RETENTION_DAYS`.
pub const RETENTION_DAYS: i64 = 7;

/// What one export run shipped, and what it could not.
#[derive(Debug, Default, PartialEq)]
pub struct ExportSummary {
    /// `(experiment_type, date, rows)` for each object written.
    pub written: Vec<(String, NaiveDate, usize)>,
    pub failed: Vec<String>,
    pub files_deleted: usize,
    /// Lines the Parquet does not hold, which keep their file from being deleted.
    pub unparsable_lines: usize,
}

/// Writes every sealed day to S3, then deletes the local files that have aged out.
///
/// A day is sealed once `today` has moved past it. The key is derived from the record, so a repeat
/// run overwrites byte-identically and a failed run repairs itself.
pub async fn export_journals(
    journal: &Journal,
    s3_client: &S3Client,
    bucket: &str,
    today: NaiveDate,
) -> ExportSummary {
    let mut summary = ExportSummary::default();

    let dates = match sealed_dates(journal.directory(), today) {
        Ok(dates) => dates,
        Err(error) => {
            summary.failed.push(format!(
                "could not list {}: {error}",
                journal.directory().display()
            ));
            return summary;
        }
    };

    let mut shipped: Vec<NaiveDate> = Vec::new();
    for date in dates {
        // Held only for the read, so a long upload does not block an experiment mid-run.
        let frames = {
            let _sealed = journal.seal().await;
            read_day(&journal.directory().join(file_name(date)))
        };
        let (frames, unparsable) = match frames {
            Ok(read) => read,
            Err(error) => {
                summary.failed.push(error);
                continue;
            }
        };
        summary.unparsable_lines += unparsable;

        let mut day_written = true;
        for (experiment_type, mut frame) in frames {
            // Every run re-exports the sealed days inside the retention window, so writing a frame
            // whose records were all rejected would replace a complete object with an empty one.
            if frame.is_empty() {
                day_written = false;
                continue;
            }
            let prefix = format!("{EXPERIMENT_PREFIX}/experiment_type={experiment_type}");
            let key = date_partitioned_key(&prefix, date);
            match write_frame(s3_client, bucket, &key, &mut frame).await {
                Ok(()) => summary
                    .written
                    .push((experiment_type, date, frame.height())),
                Err(error) => {
                    summary.failed.push(error);
                    day_written = false;
                }
            }
        }
        // A file with unparsable lines is kept whole: those lines reach no object, and deleting the
        // file would destroy the only copy of them.
        if day_written && unparsable == 0 {
            shipped.push(date);
        }
    }

    summary.files_deleted = delete_aged_out(journal.directory(), &shipped, today);
    summary
}

/// Every UTC date in the directory that `today` has moved past.
fn sealed_dates(directory: &Path, today: NaiveDate) -> Result<Vec<NaiveDate>, std::io::Error> {
    let mut dates = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let Some(name) = entry?.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(date) = date_from_file_name(&name) else {
            continue;
        };
        if date >= today {
            continue;
        }
        dates.push(date);
    }
    dates.sort_unstable();
    Ok(dates)
}

/// One day's records as a frame per experiment type, keyed by that type.
///
/// Grouped here rather than at the query surface because the type is a partition, so a day holding
/// three kinds of record writes three objects.
fn read_day(path: &Path) -> Result<(BTreeMap<String, DataFrame>, usize), String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;

    let mut grouped: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut unparsable = 0usize;
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(Value::Object(record)) = serde_json::from_str::<Value>(line) else {
            unparsable += 1;
            continue;
        };
        match record.get("experiment_type").and_then(Value::as_str) {
            Some(experiment_type) => grouped
                .entry(experiment_type.to_string())
                .or_default()
                .push(Value::Object(record)),
            None => unparsable += 1,
        }
    }

    if unparsable > 0 {
        warn!(path = %path.display(), unparsable, "Skipped laboratory lines that would not parse");
    }

    let mut frames = BTreeMap::new();
    for (experiment_type, records) in grouped {
        let (frame, rejected) = records_to_frame(&records, path)?;
        unparsable += rejected;
        frames.insert(experiment_type, frame);
    }
    Ok((frames, unparsable))
}

/// The envelope as columns and the payload as one JSON string.
///
/// Every envelope column or no row at all: `run_id` is the join and `timestamp` orders the runs, so
/// a row missing either is unreachable by the queries this archive exists to answer.
fn records_to_frame(records: &[Value], path: &Path) -> Result<(DataFrame, usize), String> {
    let mut schema_versions: Vec<Option<i64>> = Vec::new();
    let mut event_ids: Vec<Option<String>> = Vec::new();
    let mut run_ids: Vec<Option<String>> = Vec::new();
    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut payloads: Vec<Option<String>> = Vec::new();
    let mut rejected = 0usize;

    for record in records {
        let text = |key: &str| record.get(key).and_then(Value::as_str).map(str::to_string);
        let (Some(schema_version), Some(event_id), Some(run_id), Some(timestamp)) = (
            record.get("schema_version").and_then(Value::as_i64),
            text("event_id"),
            text("run_id"),
            text("timestamp").and_then(|stamp| {
                DateTime::parse_from_rfc3339(&stamp)
                    .ok()
                    .map(|instant| instant.timestamp_millis())
            }),
        ) else {
            rejected += 1;
            continue;
        };

        schema_versions.push(Some(schema_version));
        event_ids.push(Some(event_id));
        run_ids.push(Some(run_id));
        timestamps.push(Some(timestamp));
        payloads.push(record.get("payload").map(Value::to_string));
    }

    let frame = DataFrame::new(vec![
        Column::new("schema_version".into(), schema_versions),
        Column::new("event_id".into(), event_ids),
        Column::new("run_id".into(), run_ids),
        Column::new("timestamp".into(), timestamps),
        Column::new("payload".into(), payloads),
    ])
    .map_err(|error| format!("failed to build frame for {}: {error}", path.display()))?;

    Ok((frame, rejected))
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

    info!(key, rows = frame.height(), "Experiment records exported");
    Ok(())
}

/// Deletes shipped files older than the retention window.
fn delete_aged_out(directory: &Path, shipped: &[NaiveDate], today: NaiveDate) -> usize {
    let oldest_kept = today - chrono::Duration::days(RETENTION_DAYS);
    let mut deleted = 0usize;
    for date in shipped {
        if *date >= oldest_kept {
            continue;
        }
        let path = directory.join(file_name(*date));
        match std::fs::remove_file(&path) {
            Ok(()) => deleted += 1,
            Err(error) => warn!(path = %path.display(), %error, "Could not delete a shipped file"),
        }
    }
    deleted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::SessionDate;
    use crate::laboratory::dataset::DatasetFingerprint;
    use crate::laboratory::journal::{DatasetBuilt, Observation, Record};
    use uuid::Uuid;

    fn record(run_id: Uuid, milliseconds: i64) -> Record {
        Record::new(
            run_id,
            DateTime::from_timestamp_millis(milliseconds).unwrap(),
            Observation::DatasetBuilt(DatasetBuilt {
                fingerprint: DatasetFingerprint {
                    session: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()),
                    lookback_days: 365,
                    rows: 10,
                    tickers: 2,
                    first_timestamp: DateTime::from_timestamp_millis(0),
                    last_timestamp: DateTime::from_timestamp_millis(86_400_000),
                    splits_digest: 0xAB,
                    boundaries_digest: 0xCD,
                },
                revision: None,
            }),
        )
    }

    fn write_day(directory: &Path, date: NaiveDate, lines: &[String]) {
        std::fs::write(directory.join(file_name(date)), lines.join("\n") + "\n").unwrap();
    }

    #[test]
    fn test_today_is_not_yet_sealed() {
        let directory = tempfile::tempdir().unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
        let yesterday = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        for date in [today, yesterday] {
            write_day(directory.path(), date, &[]);
        }

        let sealed = sealed_dates(directory.path(), today).unwrap();

        assert_eq!(
            sealed,
            vec![yesterday],
            "a day still being written to is not sealed"
        );
    }

    /// Experiment type is a partition, so a day holding two kinds writes two objects rather than
    /// one frame with a mostly-null union schema.
    #[test]
    fn test_a_day_groups_into_one_frame_per_experiment_type() {
        let directory = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let run_id = Uuid::new_v4();

        let mut lines: Vec<String> = (0..2)
            .map(|index| serde_json::to_string(&record(run_id, 1_755_400_000_000 + index)).unwrap())
            .collect();
        // A second type, spelled directly because only one variant exists so far.
        let mut other: Value = serde_json::from_str(&lines[0]).unwrap();
        other["experiment_type"] = Value::String("model_trained".to_string());
        lines.push(other.to_string());
        write_day(directory.path(), date, &lines);

        let (frames, unparsable) = read_day(&directory.path().join(file_name(date))).unwrap();

        assert_eq!(unparsable, 0);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames["dataset_built"].height(), 2);
        assert_eq!(frames["model_trained"].height(), 1);

        // The reader rebuilds the envelope from JSON keys the producer names, so this is what
        // catches a field renamed on `Record` — without it the failure is only a row count.
        for column in [
            "schema_version",
            "event_id",
            "run_id",
            "timestamp",
            "payload",
        ] {
            assert_eq!(
                frames["dataset_built"].column(column).unwrap().null_count(),
                0,
                "{column} did not survive the round trip from Record"
            );
        }
    }

    /// Pinned to literal dates rather than to `RETENTION_DAYS`: an expectation derived from the
    /// constant under test moves with it and can never fail.
    #[test]
    fn test_only_files_older_than_the_retention_window_are_deleted() {
        let directory = tempfile::tempdir().unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 8, 18).unwrap();
        let too_old = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let exactly_at_the_edge = NaiveDate::from_ymd_opt(2026, 8, 11).unwrap();
        let recent = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        for date in [too_old, exactly_at_the_edge, recent] {
            write_day(directory.path(), date, &[]);
        }

        let deleted = delete_aged_out(
            directory.path(),
            &[too_old, exactly_at_the_edge, recent],
            today,
        );

        assert_eq!(deleted, 1);
        assert!(!directory.path().join(file_name(too_old)).exists());
        assert!(directory
            .path()
            .join(file_name(exactly_at_the_edge))
            .exists());
        assert!(directory.path().join(file_name(recent)).exists());
    }

    /// A day whose records were all rejected must not ship. Every run re-exports the sealed days in
    /// the window, so writing that frame would replace a complete object with an empty one.
    #[test]
    fn test_a_type_whose_records_were_all_rejected_yields_an_empty_frame() {
        let directory = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        // A well-formed experiment_type over an envelope missing run_id: grouped, then rejected.
        let mut broken: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&record(Uuid::new_v4(), 1)).unwrap())
                .unwrap();
        broken.as_object_mut().unwrap().remove("run_id");
        write_day(directory.path(), date, &[broken.to_string()]);

        let (frames, unparsable) = read_day(&directory.path().join(file_name(date))).unwrap();

        assert_eq!(unparsable, 1);
        assert!(frames["dataset_built"].is_empty());
    }

    #[test]
    fn test_a_line_without_an_experiment_type_is_counted_not_filed() {
        let directory = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        write_day(
            directory.path(),
            date,
            &[
                serde_json::to_string(&record(Uuid::new_v4(), 1_755_400_000_000)).unwrap(),
                "{\"run_id\":\"nope\"}".to_string(),
                "not json at all".to_string(),
            ],
        );

        let (frames, unparsable) = read_day(&directory.path().join(file_name(date))).unwrap();

        assert_eq!(unparsable, 2);
        assert_eq!(frames["dataset_built"].height(), 1);
    }

    /// The whole reason for a separate prefix: the application's journal key for the same date must
    /// not be the one this produces.
    #[test]
    fn test_the_key_does_not_collide_with_the_application_journal() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let laboratory = date_partitioned_key(
            &format!("{EXPERIMENT_PREFIX}/experiment_type=dataset_built"),
            date,
        );
        let application = date_partitioned_key(crate::data::export::JOURNAL_PREFIX, date);

        assert_ne!(laboratory, application);
        assert_eq!(
            laboratory,
            "exports/laboratory/experiments/experiment_type=dataset_built/year=2026/month=08/day=17/data.parquet"
        );
    }
}
