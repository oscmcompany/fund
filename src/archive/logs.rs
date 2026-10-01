//! A service's log file as Parquet: one row per line, what every line carries as typed columns and the rest of its
//! fields as JSON text. A file holds the runs that started in its session, so a line may be stamped past midnight.

use std::sync::Arc;

use arrow_array::builder::{StringBuilder, TimestampNanosecondBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use tracing::Level;
use uuid::Uuid;

use super::parquet::{self, ReadRefusal};
use crate::common::journal::{Commit, RunId};
use crate::common::storage::Key;

const LAYOUT_VERSION: &str = "1";

/// One line of a log file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogLine {
    Read {
        line: u64,
        timestamp: DateTime<Utc>,
        level: Level,
        target: String,
        message: String,
        /// Absent on a line logged outside a run.
        run_id: Option<RunId>,
        commit: Option<Commit>,
        /// The line's other fields as a JSON object, keys sorted.
        fields: String,
    },
    /// Not a line this service writes, kept as written.
    Unreadable { line: u64, text: String },
}

/// Why lines were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    NotALogsKey,
    /// A timestamp past what nanoseconds since the epoch hold, the year 2262.
    Unrepresentable {
        line: u64,
    },
    Parquet {
        reason: String,
    },
}

/// Why a file was not read as a log.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeRefusal {
    NotALogsKey,
    File(ReadRefusal),
    Row { line: u64, reason: String },
}

impl From<ReadRefusal> for DecodeRefusal {
    fn from(refusal: ReadRefusal) -> Self {
        Self::File(refusal)
    }
}

fn is_logs_key(key: &Key) -> bool {
    match key {
        Key::Logs { .. } => true,
        Key::Bars { .. }
        | Key::Quotes { .. }
        | Key::Trades { .. }
        | Key::Reference { .. }
        | Key::Journal { .. }
        | Key::Register { .. } => false,
    }
}

/// Every line in the order written, numbered from one.
pub fn parse(contents: &str) -> Vec<LogLine> {
    contents
        .lines()
        .zip(1_u64..)
        .map(|(text, line)| {
            parse_line(line, text).unwrap_or_else(|| LogLine::Unreadable {
                line,
                text: text.to_string(),
            })
        })
        .collect()
}

/// A line as `tracing-subscriber`'s JSON formatter writes it, with the run's span current.
fn parse_line(line: u64, text: &str) -> Option<LogLine> {
    let Value::Object(mut object) = serde_json::from_str(text).ok()? else {
        return None;
    };
    let text_of = |value: Option<Value>| match value {
        Some(Value::String(text)) => Some(text),
        Some(
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_),
        )
        | None => None,
    };
    // `Some(None)` when absent, which is no value; `None` when present but not text, which is not our line.
    let optional_text = |value: Option<Value>| match value {
        None => Some(None),
        Some(Value::String(text)) => Some(Some(text)),
        Some(
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_),
        ) => None,
    };
    let timestamp = text_of(object.remove("timestamp"))?.parse().ok()?;
    let level = text_of(object.remove("level"))?.parse().ok()?;
    let target = text_of(object.remove("target"))?;
    let Some(Value::Object(mut fields)) = object.remove("fields") else {
        return None;
    };
    let message = text_of(fields.remove("message"))?;
    // A span other than the run's, such as a library's, carries neither field.
    let (run_id, commit) = match object.remove("span") {
        None => (None, None),
        Some(Value::Object(mut span)) => {
            let run_id = match optional_text(span.remove("run_id"))? {
                None => None,
                Some(raw) => Some(RunId::new(Uuid::parse_str(&raw).ok()?)),
            };
            let commit = match optional_text(span.remove("commit"))? {
                None => None,
                Some(unknown) if unknown == "unknown" => None,
                Some(sha) => Some(Commit::new(&sha).ok()?),
            };
            (run_id, commit)
        }
        Some(
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_),
        ) => {
            return None;
        }
    };
    Some(LogLine::Read {
        line,
        timestamp,
        level,
        target,
        message,
        run_id,
        commit,
        fields: Value::Object(fields).to_string(),
    })
}

/// Every column but `line` is null on an unreadable line, which keeps its text instead.
fn schema() -> Schema {
    Schema::new(vec![
        Field::new("line", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new("level", DataType::Utf8, true),
        Field::new("target", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("commit", DataType::Utf8, true),
        Field::new("fields", DataType::Utf8, true),
        Field::new("unreadable", DataType::Utf8, true),
    ])
}

pub fn encode(key: &Key, lines: &[LogLine]) -> Result<Vec<u8>, EncodeRefusal> {
    if !is_logs_key(key) {
        return Err(EncodeRefusal::NotALogsKey);
    }
    let mut numbers = UInt64Builder::new();
    let mut timestamps = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut texts: [StringBuilder; 7] = std::array::from_fn(|_| StringBuilder::new());
    for entry in lines {
        match entry {
            LogLine::Read {
                line,
                timestamp,
                level,
                target,
                message,
                run_id,
                commit,
                fields,
            } => {
                numbers.append_value(*line);
                timestamps.append_value(
                    timestamp
                        .timestamp_nanos_opt()
                        .ok_or(EncodeRefusal::Unrepresentable { line: *line })?,
                );
                let values = [
                    Some(level.to_string()),
                    Some(target.clone()),
                    Some(message.clone()),
                    run_id.map(|run_id| run_id.to_string()),
                    commit.as_ref().map(|commit| commit.as_str().to_string()),
                    Some(fields.clone()),
                    None,
                ];
                for (builder, value) in texts.iter_mut().zip(values) {
                    builder.append_option(value);
                }
            }
            LogLine::Unreadable { line, text } => {
                numbers.append_value(*line);
                timestamps.append_null();
                for builder in texts.iter_mut().take(6) {
                    builder.append_null();
                }
                texts[6].append_value(text);
            }
        }
    }
    let mut columns: Vec<ArrayRef> =
        vec![Arc::new(numbers.finish()), Arc::new(timestamps.finish())];
    columns.extend(
        texts
            .iter_mut()
            .map(|builder| Arc::new(builder.finish()) as ArrayRef),
    );
    parquet::write(schema(), columns, LAYOUT_VERSION, Vec::new())
        .map_err(|reason| EncodeRefusal::Parquet { reason })
}

pub fn decode(key: &Key, bytes: Vec<u8>) -> Result<Vec<LogLine>, DecodeRefusal> {
    if !is_logs_key(key) {
        return Err(DecodeRefusal::NotALogsKey);
    }
    let (batches, _) = parquet::read(bytes, &schema(), LAYOUT_VERSION)?;
    let mut lines = Vec::new();
    for batch in batches {
        let numbers = parquet::downcast::<UInt64Array>(batch.column(0))?;
        let timestamps = parquet::downcast::<TimestampNanosecondArray>(batch.column(1))?;
        let texts: Vec<&StringArray> = (2..9)
            .map(|index| parquet::downcast::<StringArray>(batch.column(index)))
            .collect::<Result<_, _>>()?;
        for row in 0..batch.num_rows() {
            let line = numbers.value(row);
            let refused = |reason: &str| DecodeRefusal::Row {
                line,
                reason: reason.to_string(),
            };
            let text = |index: usize| texts[index].is_valid(row).then(|| texts[index].value(row));
            if let Some(unreadable) = text(6) {
                lines.push(LogLine::Unreadable {
                    line,
                    text: unreadable.to_string(),
                });
                continue;
            }
            let required = |index: usize| text(index).ok_or_else(|| refused("a column is null"));
            if !timestamps.is_valid(row) {
                return Err(refused("the timestamp is null"));
            }
            let fields = required(5)?;
            if !matches!(serde_json::from_str(fields), Ok(Value::Object(Map { .. }))) {
                return Err(refused("the fields are not a JSON object"));
            }
            lines.push(LogLine::Read {
                line,
                timestamp: DateTime::from_timestamp_nanos(timestamps.value(row)),
                level: required(0)?.parse().map_err(|_| refused("unknown level"))?,
                target: required(1)?.to_string(),
                message: required(2)?.to_string(),
                run_id: text(3)
                    .map(|raw| Uuid::parse_str(raw).map(RunId::new))
                    .transpose()
                    .map_err(|_| refused("run id is not a uuid"))?,
                commit: text(4)
                    .map(Commit::new)
                    .transpose()
                    .map_err(|_| refused("commit is not a sha"))?,
                fields: fields.to_string(),
            });
        }
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;
    use crate::common::storage::{Host, Service};
    use crate::common::time::SessionDate;

    fn key() -> Key {
        Key::Logs {
            host: Host::Archiver,
            service: Service::new("archive_nightly").unwrap(),
            session: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 30).unwrap()),
        }
    }

    /// Lines the binary wrote on 2026-09-30: a run's partition line, a startup refusal and a torn tail.
    const LOG: &str = concat!(
        r#"{"timestamp":"2026-09-30T18:45:07.440905Z","level":"INFO","fields":{"message":"Partition written","leg":"massive_daily_bars","session":"2026-09-28","bars":12533,"refused":"{\"symbol\": 7}","unanswered":0},"target":"fund::heal","span":{"commit":"2a31d587cd48a6d5a77307f8b3ad5a4ce32873fa-dirty","run_id":"16d2b586-9c4a-4651-a3a9-65d3eb91e073","name":"run"}}"#,
        "\n",
        r#"{"timestamp":"2026-09-30T20:48:16.570992Z","level":"ERROR","fields":{"message":"Parameters refused","refusal":"FUND_LOOKBACK_SESSIONS is `0`"},"target":"archive_nightly","span":{"commit":"unknown","run_id":"ed3f2289-28ce-43e8-a21a-2f2e97514d91","name":"run"}}"#,
        "\n",
        r#"{"timestamp":"2026-09-30T20:49"#,
    );

    #[test]
    fn test_a_log_line_reads_into_its_columns() {
        let lines = parse(LOG);
        assert_eq!(lines.len(), 3);
        match &lines[0] {
            LogLine::Read {
                line,
                timestamp,
                level,
                target,
                message,
                run_id,
                commit,
                fields,
            } => {
                assert_eq!(*line, 1);
                assert_eq!(timestamp.to_rfc3339(), "2026-09-30T18:45:07.440905+00:00");
                assert_eq!(*level, Level::INFO);
                assert_eq!(target, "fund::heal");
                assert_eq!(message, "Partition written");
                assert_eq!(
                    run_id.map(|run_id| run_id.to_string()).as_deref(),
                    Some("16d2b586-9c4a-4651-a3a9-65d3eb91e073")
                );
                assert!(commit.as_ref().is_some_and(Commit::is_dirty));
                assert_eq!(
                    fields,
                    r#"{"bars":12533,"leg":"massive_daily_bars","refused":"{\"symbol\": 7}","session":"2026-09-28","unanswered":0}"#
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(
            matches!(&lines[1], LogLine::Read { commit: None, level, .. } if *level == Level::ERROR)
        );
        assert!(matches!(&lines[2], LogLine::Unreadable { line: 3, .. }));
    }

    #[test]
    fn test_a_span_without_the_runs_fields_still_reads() {
        let lines = parse(
            r#"{"timestamp":"2026-09-30T18:45:05Z","level":"INFO","fields":{"message":"Sent"},"target":"aws_smithy","span":{"name":"send"}}"#,
        );
        assert!(
            matches!(
                &lines[..],
                [LogLine::Read {
                    run_id: None,
                    commit: None,
                    ..
                }]
            ),
            "{lines:?}"
        );
        let malformed = parse(
            r#"{"timestamp":"2026-09-30T18:45:05Z","level":"INFO","fields":{"message":"Sent"},"target":"archive_nightly","span":{"run_id":"not-a-uuid"}}"#,
        );
        assert!(matches!(&malformed[..], [LogLine::Unreadable { .. }]));
    }

    #[test]
    fn test_a_log_file_reads_back_line_for_line() {
        let lines = parse(LOG);
        let bytes = encode(&key(), &lines).unwrap();
        assert_eq!(decode(&key(), bytes).unwrap(), lines);
    }

    #[test]
    fn test_a_journal_key_is_refused_both_ways() {
        let journal = Key::Journal {
            host: Host::Archiver,
            session: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 30).unwrap()),
        };
        assert_eq!(
            encode(&journal, &[]).map(|_| ()),
            Err(EncodeRefusal::NotALogsKey)
        );
        assert_eq!(
            decode(&journal, encode(&key(), &[]).unwrap()),
            Err(DecodeRefusal::NotALogsKey)
        );
    }

    proptest! {
        /// Whatever the file holds, it reads back as the lines parsed from it.
        #[test]
        fn property_lines_round_trip(
            picks in prop::collection::vec(0_usize..3, 0..8),
            noise in prop::collection::vec("[ -~]{1,30}", 0..3),
        ) {
            let known: Vec<&str> = LOG.lines().collect();
            let mut texts: Vec<String> = picks.iter().map(|pick| known[*pick].to_string()).collect();
            texts.extend(noise);
            let lines = parse(&texts.join("\n"));
            prop_assert_eq!(lines.len(), texts.len());
            let bytes = encode(&key(), &lines).unwrap();
            prop_assert_eq!(decode(&key(), bytes).unwrap(), lines);
        }
    }
}
