//! Ships laboratory journal days to S3, one object per experiment type per session.
//!
//! Called by `laboratory_export`, which `tools/run-researcher` runs as its last leg.

use std::collections::BTreeMap;
use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::DateTime;
use polars::prelude::*;
use serde_json::Value;
use tracing::{info, warn};

use crate::common::aws::{date_partitioned_key, producer_prefix, Producer};
use crate::common::types::SessionDate;
use crate::data::export::JOURNAL_PREFIX;
use crate::laboratory::journal::{file_name, session_from_file_name, Journal};

/// The producer this export writes as.
///
/// The host, not this module: `researcher` runs every `laboratory_*` binary and would run model
/// training beside them, so the key survives either moving. Shares `exports/journal` with the
/// trader, which is safe only because `producer` distinguishes them — both write one object a
/// session and a date alone would collide.
const PRODUCER: Producer = Producer::Researcher;

/// Age, in days, past which a shipped file is deleted.
///
/// A file exactly this old is kept, matching `data::export::JOURNAL_RETENTION_DAYS`.
pub const RETENTION_DAYS: i64 = 7;

/// What one export run shipped, and what it could not.
#[derive(Debug, Default, PartialEq)]
pub struct ExportSummary {
    /// `(experiment_type, session, rows)` for each object written.
    pub written: Vec<(String, SessionDate, usize)>,
    pub failed: Vec<String>,
    pub files_deleted: usize,
    /// Lines the Parquet does not hold, which keep their file from being deleted.
    pub unparsable_lines: usize,
}

/// Writes every day up to and including `today` to S3, then deletes the local files that have aged
/// out.
///
/// The key is derived from the session, so a repeat run overwrites what it wrote before and a failed
/// run repairs itself. Today's file is still open, which is why the seal is taken around each read.
pub async fn export_journals(
    journal: &Journal,
    s3_client: &S3Client,
    bucket: &str,
    today: SessionDate,
) -> ExportSummary {
    let mut summary = ExportSummary::default();

    let sessions = match sealed_sessions(journal.directory(), today) {
        Ok(sessions) => sessions,
        Err(error) => {
            summary.failed.push(format!(
                "could not list {}: {error}",
                journal.directory().display()
            ));
            return summary;
        }
    };

    let mut shipped: Vec<SessionDate> = Vec::new();
    for session in sessions {
        // Held only for the read, so a long upload does not block an experiment mid-run.
        let frames = {
            let _sealed = journal.seal().await;
            read_day(&journal.directory().join(file_name(session)))
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
            // `experiment_type` sits beneath `producer`, so one session is many objects here and
            // exactly one for the trader.
            let prefix = producer_prefix(JOURNAL_PREFIX, PRODUCER);
            let key = date_partitioned_key(
                &format!("{prefix}/experiment_type={experiment_type}"),
                session.date(),
            );
            match write_frame(s3_client, bucket, &key, &mut frame).await {
                Ok(()) => summary
                    .written
                    .push((experiment_type, session, frame.height())),
                Err(error) => {
                    summary.failed.push(error);
                    day_written = false;
                }
            }
        }
        // A file with unparsable lines is kept whole: those lines reach no object, and deleting the
        // file would destroy the only copy of them.
        if day_written && unparsable == 0 {
            shipped.push(session);
        }
    }

    summary.files_deleted = delete_aged_out(journal.directory(), &shipped, today);
    summary
}

/// Every session in the directory up to and including `today`.
///
/// Today is included, matching `data::export::sealed_sessions`: the seal below covers the read, the
/// key is derived from the session, and a later run overwrites it with the same rows plus whatever
/// arrived after. Excluding today meant a study could not ship on the day it ran, which on a box
/// that is usually off left a day's work waiting on the next boot.
fn sealed_sessions(
    directory: &Path,
    today: SessionDate,
) -> Result<Vec<SessionDate>, std::io::Error> {
    let mut sessions = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let Some(name) = entry?.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(session) = session_from_file_name(&name) else {
            continue;
        };
        // A file dated ahead of today is a clock that disagrees with this one, not a sealed day.
        if session > today {
            continue;
        }
        sessions.push(session);
    }
    sessions.sort_unstable();
    Ok(sessions)
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
fn delete_aged_out(directory: &Path, shipped: &[SessionDate], today: SessionDate) -> usize {
    let oldest_kept = today.plus_calendar_days(-RETENTION_DAYS);
    let mut deleted = 0usize;
    for session in shipped {
        if *session >= oldest_kept {
            continue;
        }
        let path = directory.join(file_name(*session));
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
    use aws_sdk_s3::Client as S3Client;
    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;
    use percent_encoding::percent_decode_str;
    use uuid::Uuid;

    fn record(run_id: Uuid, milliseconds: i64) -> Record {
        Record::new(
            run_id,
            DateTime::from_timestamp_millis(milliseconds).unwrap(),
            Observation::DatasetBuilt(DatasetBuilt::new(DatasetFingerprint {
                session: session(2026, 8, 17),
                lookback_days: 365,
                liquidity_floor: None,
                screen_window: None,
                rows: 10,
                tickers: 2,
                first_timestamp: DateTime::from_timestamp_millis(0),
                last_timestamp: DateTime::from_timestamp_millis(86_400_000),
                splits_digest: 0xAB,
                boundaries_digest: 0xCD,
                reference_digest: Some(0xEF),
                factor_specification: None,
                microstructure: crate::laboratory::dataset::Microstructure::Omitted,
                quote_summary_digest: None,
                trade_summary_digest: None,
            })),
        )
    }

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    fn write_day(directory: &Path, session: SessionDate, lines: &[String]) {
        std::fs::write(directory.join(file_name(session)), lines.join("\n") + "\n").unwrap();
    }

    /// The property the whole trigger rests on: a study run today ships tonight rather than on the
    /// next boot of a box that is usually off.
    #[test]
    fn test_today_ships_and_a_day_dated_ahead_does_not() {
        let directory = tempfile::tempdir().unwrap();
        let tomorrow = session(2026, 8, 19);
        let today = session(2026, 8, 18);
        let yesterday = session(2026, 8, 17);
        for date in [tomorrow, today, yesterday] {
            write_day(directory.path(), date, &[]);
        }

        let sealed = sealed_sessions(directory.path(), today).unwrap();

        assert_eq!(
            sealed,
            vec![yesterday, today],
            "today is shipped and a file dated ahead of it is not"
        );
    }

    /// Experiment type is a partition, so a day holding two kinds writes two objects rather than
    /// one frame with a mostly-null union schema.
    #[test]
    fn test_a_day_groups_into_one_frame_per_experiment_type() {
        let directory = tempfile::tempdir().unwrap();
        let date = session(2026, 8, 17);
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
        let today = session(2026, 8, 18);
        let too_old = session(2026, 8, 10);
        let exactly_at_the_edge = session(2026, 8, 11);
        let recent = session(2026, 8, 17);
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
        let date = session(2026, 8, 17);
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
        let date = session(2026, 8, 17);
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

    /// The separate prefix is gone, so `producer` is now the only thing keeping two writers of the
    /// same date apart. It is the reason the shared prefix is safe rather than a collision.
    #[test]
    fn test_the_key_does_not_collide_with_the_traders_journal() {
        let date = session(2026, 8, 17).date();
        let trainer = date_partitioned_key(
            &format!(
                "{}/experiment_type=dataset_built",
                producer_prefix(JOURNAL_PREFIX, PRODUCER)
            ),
            date,
        );
        let trader = date_partitioned_key(&producer_prefix(JOURNAL_PREFIX, Producer::Trader), date);

        assert_ne!(trainer, trader);
        assert_eq!(
            trainer,
            "exports/journal/producer=researcher/experiment_type=dataset_built/year=2026/month=08/day=17/data.parquet"
        );
        assert_eq!(
            trader,
            "exports/journal/producer=trader/year=2026/month=08/day=17/data.parquet"
        );
    }

    /// All three share the prefix, so a reader listing `exports/journal/` sees every producer and
    /// must be able to tell them apart from the key alone.
    #[test]
    fn test_every_producer_writes_a_distinct_key_under_one_prefix() {
        let date = session(2026, 8, 17).date();
        // Spelled out rather than derived from `as_str`, which is the thing under test: a producer
        // whose name came back empty would satisfy any assertion built from it.
        let expected = [
            (
                Producer::Trader,
                "exports/journal/producer=trader/year=2026/month=08/day=17/data.parquet",
            ),
            (
                Producer::Researcher,
                "exports/journal/producer=researcher/year=2026/month=08/day=17/data.parquet",
            ),
            (
                Producer::Archiver,
                "exports/journal/producer=archiver/year=2026/month=08/day=17/data.parquet",
            ),
        ];

        for (producer, key) in expected {
            assert_eq!(
                date_partitioned_key(&producer_prefix(JOURNAL_PREFIX, producer), date),
                key
            );
        }

        let distinct: BTreeMap<&str, ()> = expected.iter().map(|(_, key)| (*key, ())).collect();
        assert_eq!(distinct.len(), expected.len());
    }

    /// An S3 client that answers every request from memory and keeps what it was handed.
    ///
    /// The key is decoded because S3 percent-encodes the `=` in every hive segment, so a key built
    /// by `date_partitioned_key` never matches the wire form.
    fn capturing_s3_client(
        captured: std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>,
    ) -> S3Client {
        let http_client = infallible_client_fn(move |request| {
            let key = percent_decode_str(request.uri().path())
                .decode_utf8_lossy()
                .trim_start_matches('/')
                .trim_start_matches("test-bucket/")
                .to_string();
            let body = request.body().bytes().unwrap_or_default().to_vec();
            captured
                .lock()
                .expect("the recorder must not be poisoned")
                .push((key, body));
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .expect("a canned response must build")
        });
        S3Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test-key",
                    "test-secret",
                    None,
                    None,
                    "test",
                ))
                .http_client(http_client)
                .build(),
        )
    }

    /// The whole leg, end to end: records appended through the real journal, exported, and the
    /// bytes read back as Parquet.
    ///
    /// The three unit tests above each cover one step. This is the seam between them, which is
    /// where the export spent its whole life broken — every piece worked and nothing called them.
    #[tokio::test]
    async fn test_a_study_run_today_reaches_s3_as_parquet() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::new(directory.path()).expect("the journal must open");
        let today = session(2026, 8, 18);
        let run_id = Uuid::new_v4();
        // Appended through the writer rather than handwritten, so the file under test is the one a
        // study would actually leave behind.
        let stamp = |date: SessionDate, hour: u32| {
            date.date()
                .and_hms_opt(hour, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis()
        };
        for (date, hour) in [(session(2026, 8, 17), 20), (today, 20)] {
            journal
                .append(&record(run_id, stamp(date, hour)))
                .await
                .expect("the record must append");
        }

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let summary = export_journals(
            &journal,
            &capturing_s3_client(captured.clone()),
            "test-bucket",
            today,
        )
        .await;

        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        let written = captured.lock().expect("the recorder must not be poisoned");
        let keys: Vec<&str> = written.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "exports/journal/producer=researcher/experiment_type=dataset_built/year=2026/month=08/day=17/data.parquet",
                "exports/journal/producer=researcher/experiment_type=dataset_built/year=2026/month=08/day=18/data.parquet",
            ],
            "yesterday and today, each under its own partition"
        );

        let (_, bytes) = written.last().expect("today's object must have been put");
        let frame = ParquetReader::new(std::io::Cursor::new(bytes.clone()))
            .finish()
            .expect("the body must be Parquet");
        assert_eq!(frame.height(), 1, "the one record appended for today");
        let columns: Vec<String> = frame
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        assert_eq!(
            columns,
            [
                "schema_version",
                "event_id",
                "run_id",
                "timestamp",
                "payload"
            ],
            "the envelope the DuckDB view selects"
        );
    }
}
