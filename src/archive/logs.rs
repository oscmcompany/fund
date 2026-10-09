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
use crate::journal::UNKNOWN_COMMIT;

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
        fields: Fields,
    },
    /// Not a line this service writes, kept as written.
    Unreadable {
        line: u64,
        text: String,
        cause: LogUnreadableCause,
    },
}

/// A line's fields other than the ones `LogLine::Read` types, as one JSON object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fields(Map<String, Value>);

/// The object as JSON text, keys sorted.
impl std::fmt::Display for Fields {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = serde_json::to_string(&self.0).map_err(|_| std::fmt::Error)?;
        formatter.write_str(&text)
    }
}

/// Why a line was not read as one this service writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogUnreadableCause {
    NotJson {
        reason: String,
    },
    NotAnObject,
    Missing {
        field: LogField,
    },
    /// Present, but not what the formatter writes there.
    Malformed {
        field: LogField,
    },
}

/// A field `tracing-subscriber`'s JSON formatter writes, by the name it writes it under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum LogField {
    Timestamp,
    Level,
    Target,
    Fields,
    Message,
    Span,
    RunId,
    Commit,
}

/// Why lines were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
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
    File(ReadRefusal),
    Row { line: u64, reason: String },
}

impl From<ReadRefusal> for DecodeRefusal {
    fn from(refusal: ReadRefusal) -> Self {
        Self::File(refusal)
    }
}

/// Every line in the order written, numbered from one.
pub fn parse(contents: &str) -> Vec<LogLine> {
    contents
        .lines()
        .zip(1_u64..)
        .map(|(text, line)| read_one(line, text))
        .collect()
}

/// Line `line` of a log file, as `parse` would give it.
fn read_one(line: u64, text: &str) -> LogLine {
    parse_line(line, text).unwrap_or_else(|cause| LogLine::Unreadable {
        line,
        text: text.to_string(),
        cause,
    })
}

/// A line as `tracing-subscriber`'s JSON formatter writes it, with the run's span current.
fn parse_line(line: u64, text: &str) -> Result<LogLine, LogUnreadableCause> {
    use LogUnreadableCause::Malformed;
    let value = serde_json::from_str(text).map_err(|error| LogUnreadableCause::NotJson {
        reason: error.to_string(),
    })?;
    let Value::Object(mut object) = value else {
        return Err(LogUnreadableCause::NotAnObject);
    };
    let timestamp = parsed(&mut object, LogField::Timestamp)?;
    let level = parsed(&mut object, LogField::Level)?;
    let target = text_of(&mut object, LogField::Target)?;
    let mut fields =
        optional_object_of(&mut object, LogField::Fields)?.ok_or(LogUnreadableCause::Missing {
            field: LogField::Fields,
        })?;
    let message = text_of(&mut fields, LogField::Message)?;
    // A span other than the run's, such as a library's, carries neither field.
    let (run_id, commit) = match optional_object_of(&mut object, LogField::Span)? {
        None => (None, None),
        Some(mut span) => {
            let run_id = optional_text_of(&mut span, LogField::RunId)?
                .map(|raw| Uuid::parse_str(&raw).map(RunId::new))
                .transpose()
                .map_err(|_| Malformed {
                    field: LogField::RunId,
                })?;
            let commit = match optional_text_of(&mut span, LogField::Commit)? {
                None => None,
                Some(raw) if raw == UNKNOWN_COMMIT => None,
                Some(raw) => Some(Commit::new(&raw).map_err(|_| Malformed {
                    field: LogField::Commit,
                })?),
            };
            (run_id, commit)
        }
    };
    Ok(LogLine::Read {
        line,
        timestamp,
        level,
        target,
        message,
        run_id,
        commit,
        fields: Fields(fields),
    })
}

/// The text under `field` parsed as what the formatter writes there.
fn parsed<Parsed: std::str::FromStr>(
    object: &mut Map<String, Value>,
    field: LogField,
) -> Result<Parsed, LogUnreadableCause> {
    text_of(object, field)?
        .parse()
        .map_err(|_| LogUnreadableCause::Malformed { field })
}

/// The text under `field`, which the formatter always writes.
fn text_of(object: &mut Map<String, Value>, field: LogField) -> Result<String, LogUnreadableCause> {
    optional_text_of(object, field)?.ok_or(LogUnreadableCause::Missing { field })
}

/// The object under `field`, `None` when absent; present as anything but an object, it is not our line.
fn optional_object_of(
    object: &mut Map<String, Value>,
    field: LogField,
) -> Result<Option<Map<String, Value>>, LogUnreadableCause> {
    match object.remove(<&str>::from(field)) {
        None => Ok(None),
        Some(Value::Object(inner)) => Ok(Some(inner)),
        Some(
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_),
        ) => Err(LogUnreadableCause::Malformed { field }),
    }
}

/// The text under `field`, `None` when absent; present as anything but text, it is not our line.
fn optional_text_of(
    object: &mut Map<String, Value>,
    field: LogField,
) -> Result<Option<String>, LogUnreadableCause> {
    match object.remove(<&str>::from(field)) {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_),
        ) => Err(LogUnreadableCause::Malformed { field }),
    }
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

pub fn encode(lines: &[LogLine]) -> Result<Vec<u8>, EncodeRefusal> {
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
                    commit.as_ref().map(Commit::to_string),
                    Some(fields.to_string()),
                    None,
                ];
                for (builder, value) in texts.iter_mut().zip(values) {
                    builder.append_option(value);
                }
            }
            LogLine::Unreadable { line, text, .. } => {
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

pub fn decode(bytes: Vec<u8>) -> Result<Vec<LogLine>, DecodeRefusal> {
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
            // Read again, so the cause is this build's and a row edited out of band reads as what it now says.
            if let Some(unreadable) = text(6) {
                lines.push(read_one(line, unreadable));
                continue;
            }
            let required = |index: usize| text(index).ok_or_else(|| refused("a column is null"));
            if !timestamps.is_valid(row) {
                return Err(refused("the timestamp is null"));
            }
            let fields = match serde_json::from_str(required(5)?) {
                Ok(Value::Object(fields)) => Fields(fields),
                Ok(
                    Value::Null
                    | Value::Bool(_)
                    | Value::Number(_)
                    | Value::String(_)
                    | Value::Array(_),
                )
                | Err(_) => return Err(refused("the fields are not a JSON object")),
            };
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
                fields,
            });
        }
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

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
                    fields.to_string(),
                    r#"{"bars":12533,"leg":"massive_daily_bars","refused":"{\"symbol\": 7}","session":"2026-09-28","unanswered":0}"#
                );
            }
            other @ LogLine::Unreadable { .. } => panic!("{other:?}"),
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
        assert!(matches!(
            &malformed[..],
            [LogLine::Unreadable {
                cause: LogUnreadableCause::Malformed {
                    field: LogField::RunId
                },
                ..
            }]
        ));
    }

    #[test]
    fn test_an_unreadable_line_names_why() {
        let cause = |text: &str| match parse(text).as_slice() {
            [LogLine::Unreadable { cause, .. }] => cause.clone(),
            other => panic!("{other:?}"),
        };
        assert!(matches!(
            cause(r#"{"timestamp":"2026-09-30T20:49"#),
            LogUnreadableCause::NotJson { .. }
        ));
        assert_eq!(cause("[1]"), LogUnreadableCause::NotAnObject);
        assert_eq!(
            cause(r#"{"timestamp":"2026-09-30T18:45:05Z","level":"INFO","target":"t"}"#),
            LogUnreadableCause::Missing {
                field: LogField::Fields
            }
        );
        assert_eq!(
            cause(
                r#"{"timestamp":"2026-09-30T18:45:05Z","level":"LOUD","fields":{"message":"m"},"target":"t"}"#
            ),
            LogUnreadableCause::Malformed {
                field: LogField::Level
            }
        );
        assert_eq!(
            cause(
                r#"{"timestamp":"2026-09-30T18:45:05Z","level":"INFO","fields":{},"target":"t"}"#
            ),
            LogUnreadableCause::Missing {
                field: LogField::Message
            }
        );
    }

    #[test]
    fn test_a_log_file_reads_back_line_for_line() {
        let lines = parse(LOG);
        let bytes = encode(&lines).unwrap();
        assert_eq!(decode(bytes).unwrap(), lines);
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
            let bytes = encode(&lines).unwrap();
            prop_assert_eq!(decode(bytes).unwrap(), lines);
        }
    }
}
