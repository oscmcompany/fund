//! A session's journal as Parquet: one row per line of the session file, the envelope as typed columns and the
//! observation's payload as JSON text, so a reader filters by event and run and a new observation needs no new column.

use std::sync::Arc;

use arrow_array::builder::{StringBuilder, TimestampNanosecondBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::DateTime;

use super::parquet::{self, ReadRefusal};
use crate::common::journal::{ReadLine, read_one};
use crate::common::storage::Key;
use crate::common::time::SessionDate;

const LAYOUT_VERSION: &str = "1";

/// Why lines were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    NotAJournalKey,
    /// A record from another session than the key's.
    OutsideKey {
        line: usize,
        session: SessionDate,
    },
    /// A timestamp past what nanoseconds since the epoch hold, the year 2262.
    Unrepresentable {
        line: usize,
    },
    Parquet {
        reason: String,
    },
}

/// Why a file was not read as a journal.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeRefusal {
    NotAJournalKey,
    File(ReadRefusal),
    /// A row that does not read back as the line it was written from.
    Row {
        line: u64,
        reason: String,
    },
}

impl From<ReadRefusal> for DecodeRefusal {
    fn from(refusal: ReadRefusal) -> Self {
        Self::File(refusal)
    }
}

fn session_of(key: &Key) -> Option<SessionDate> {
    match key {
        Key::Journal { session, .. } => Some(*session),
        Key::Bars { .. }
        | Key::Quotes { .. }
        | Key::Trades { .. }
        | Key::Reference { .. }
        | Key::Logs { .. } => None,
    }
}

/// Every envelope column is null on an unreadable line, which keeps its text instead.
fn schema() -> Schema {
    Schema::new(vec![
        Field::new("line", DataType::UInt64, false),
        Field::new("schema_version", DataType::UInt64, true),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("sequence", DataType::UInt64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new("commit", DataType::Utf8, true),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
        Field::new("unreadable", DataType::Utf8, true),
    ])
}

/// The file for `key` from its session's lines in file order, unreadable lines kept so the export is never a shorter
/// run than the one that happened.
pub fn encode(key: &Key, lines: &[ReadLine]) -> Result<Vec<u8>, EncodeRefusal> {
    let session = session_of(key).ok_or(EncodeRefusal::NotAJournalKey)?;
    let mut numbers = UInt64Builder::new();
    let mut versions = UInt64Builder::new();
    let mut run_ids = StringBuilder::new();
    let mut sequences = UInt64Builder::new();
    let mut timestamps = TimestampNanosecondBuilder::new().with_timezone("UTC");
    let mut commits = StringBuilder::new();
    let mut event_types = StringBuilder::new();
    let mut payloads = StringBuilder::new();
    let mut unreadables = StringBuilder::new();
    for (index, line) in lines.iter().enumerate() {
        match line {
            ReadLine::Read(record) => {
                let number = index + 1;
                if record.session() != session {
                    return Err(EncodeRefusal::OutsideKey {
                        line: number,
                        session: record.session(),
                    });
                }
                let nanoseconds = record
                    .timestamp()
                    .timestamp_nanos_opt()
                    .ok_or(EncodeRefusal::Unrepresentable { line: number })?;
                let tagged = serde_json::to_value(record.observation())
                    .expect("an observation has only string keys, so it serializes");
                numbers.append_value(number as u64);
                versions.append_value(record.schema_version());
                run_ids.append_value(record.run_id().to_string());
                sequences.append_value(record.sequence());
                timestamps.append_value(nanoseconds);
                commits.append_option(record.commit().map(|commit| commit.as_str()));
                event_types.append_value(record.observation().event_type());
                payloads.append_value(tagged["payload"].to_string());
                unreadables.append_null();
            }
            ReadLine::Unreadable { line, text, .. } => {
                numbers.append_value(*line as u64);
                versions.append_null();
                run_ids.append_null();
                sequences.append_null();
                timestamps.append_null();
                commits.append_null();
                event_types.append_null();
                payloads.append_null();
                unreadables.append_value(text);
            }
        }
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(numbers.finish()),
        Arc::new(versions.finish()),
        Arc::new(run_ids.finish()),
        Arc::new(sequences.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(commits.finish()),
        Arc::new(event_types.finish()),
        Arc::new(payloads.finish()),
        Arc::new(unreadables.finish()),
    ];
    parquet::write(schema(), columns, LAYOUT_VERSION, Vec::new())
        .map_err(|reason| EncodeRefusal::Parquet { reason })
}

/// The lines a file written by `encode` under `key` holds, each read again through the journal's own reader, so a
/// row edited out of band reads back as what it now says rather than what it was.
pub fn decode(key: &Key, bytes: Vec<u8>) -> Result<Vec<ReadLine>, DecodeRefusal> {
    let session = session_of(key).ok_or(DecodeRefusal::NotAJournalKey)?;
    let (batches, _) = parquet::read(bytes, &schema(), LAYOUT_VERSION)?;
    let mut lines = Vec::new();
    for batch in batches {
        let column = |index: usize| batch.column(index);
        let numbers = parquet::downcast::<UInt64Array>(column(0))?;
        let versions = parquet::downcast::<UInt64Array>(column(1))?;
        let run_ids = parquet::downcast::<StringArray>(column(2))?;
        let sequences = parquet::downcast::<UInt64Array>(column(3))?;
        let timestamps = parquet::downcast::<TimestampNanosecondArray>(column(4))?;
        let commits = parquet::downcast::<StringArray>(column(5))?;
        let event_types = parquet::downcast::<StringArray>(column(6))?;
        let payloads = parquet::downcast::<StringArray>(column(7))?;
        let unreadables = parquet::downcast::<StringArray>(column(8))?;
        for row in 0..batch.num_rows() {
            let line = numbers.value(row);
            let refused = |reason: &str| DecodeRefusal::Row {
                line,
                reason: reason.to_string(),
            };
            let number = usize::try_from(line).map_err(|_| refused("line number overflows"))?;
            // A record from another session contradicts the key, however the row came to hold it.
            let in_session = |read: ReadLine| match read {
                ReadLine::Read(record) if record.session() != session => Err(refused(&format!(
                    "a record from {} under {session}",
                    record.session()
                ))),
                ReadLine::Read(_) | ReadLine::Unreadable { .. } => Ok(read),
            };
            if unreadables.is_valid(row) {
                lines.push(in_session(read_one(number, unreadables.value(row)))?);
                continue;
            }
            let present = [versions.is_valid(row), run_ids.is_valid(row)]
                .into_iter()
                .chain([sequences.is_valid(row), timestamps.is_valid(row)])
                .chain([event_types.is_valid(row), payloads.is_valid(row)])
                .all(|valid| valid);
            if !present {
                return Err(refused("an envelope column is null on a readable line"));
            }
            let timestamp = DateTime::from_timestamp_nanos(timestamps.value(row));
            let payload: serde_json::Value = serde_json::from_str(payloads.value(row))
                .map_err(|error| refused(&error.to_string()))?;
            let text = serde_json::json!({
                "schema_version": versions.value(row),
                "run_id": run_ids.value(row),
                "sequence": sequences.value(row),
                "timestamp": timestamp,
                "commit": commits.is_valid(row).then(|| commits.value(row)),
                "event_type": event_types.value(row),
                "payload": payload,
            })
            .to_string();
            match read_one(number, &text) {
                ReadLine::Read(record) => lines.push(in_session(ReadLine::Read(record))?),
                ReadLine::Unreadable { cause, .. } => {
                    return Err(refused(&format!("{cause:?}")));
                }
            }
        }
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use chrono::{NaiveDate, Utc};
    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;
    use crate::common::heal::{Leg, SessionOutcome};
    use crate::common::journal::{
        Commit, ConfigurationResolved, HealFinished, Observation, Record, RunId, read,
    };
    use crate::common::storage::Host;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 30).unwrap())
    }

    fn key() -> Key {
        Key::Journal {
            host: Host::Archiver,
            session: session(),
        }
    }

    fn record(sequence: u64, timestamp: &str, observation: Observation) -> String {
        Record::new(
            RunId::new(Uuid::from_u128(7)),
            NonZeroU64::new(sequence).unwrap(),
            timestamp.parse().unwrap(),
            Some(Commit::new("0123456789abcdef0123456789abcdef01234567-dirty").unwrap()),
            observation,
        )
        .encode()
    }

    /// A session file as the journal writes one, with a torn line a crash left behind.
    fn session_file() -> String {
        let finished = HealFinished::new(
            vec![session().plus_calendar_days(-1)],
            BTreeMap::from([(
                Leg::MassiveDailyBars,
                BTreeMap::from([(session().plus_calendar_days(-1), SessionOutcome::Unreached)]),
            )]),
        );
        [
            record(
                1,
                "2026-09-30T07:00:00.123456789Z",
                Observation::ConfigurationResolved(ConfigurationResolved::new(BTreeMap::new())),
            ),
            r#"{"schema_version":1,"run"#.to_string(),
            record(
                2,
                "2026-09-30T07:01:00Z",
                Observation::HealFinished(finished),
            ),
        ]
        .join("\n")
    }

    #[test]
    fn test_a_session_file_reads_back_line_for_line() {
        let lines = read(&session_file());
        assert!(matches!(
            lines.as_slice(),
            [
                ReadLine::Read(_),
                ReadLine::Unreadable { line: 2, .. },
                ReadLine::Read(_)
            ]
        ));
        let bytes = encode(&key(), &lines).unwrap();
        assert_eq!(decode(&key(), bytes).unwrap(), lines);
    }

    #[test]
    fn test_a_file_read_under_another_session_is_refused() {
        let bytes = encode(&key(), &read(&session_file())).unwrap();
        let next = Key::Journal {
            host: Host::Archiver,
            session: session().plus_calendar_days(1),
        };
        assert!(matches!(
            decode(&next, bytes),
            Err(DecodeRefusal::Row { line: 1, .. })
        ));
    }

    #[test]
    fn test_a_record_from_another_session_is_refused() {
        let lines = read(&record(
            1,
            // 03:30 UTC on September 30 is 23:30 Eastern on the 29th.
            "2026-09-30T03:30:00Z",
            Observation::ConfigurationResolved(ConfigurationResolved::new(BTreeMap::new())),
        ));
        assert_eq!(
            encode(&key(), &lines).map(|_| ()),
            Err(EncodeRefusal::OutsideKey {
                line: 1,
                session: session().plus_calendar_days(-1)
            })
        );
    }

    #[test]
    fn test_a_bars_key_is_refused_both_ways() {
        let bars = Leg::MassiveDailyBars.key(session());
        assert_eq!(
            encode(&bars, &[]).map(|_| ()),
            Err(EncodeRefusal::NotAJournalKey)
        );
        assert_eq!(
            decode(&bars, encode(&key(), &[]).unwrap()),
            Err(DecodeRefusal::NotAJournalKey)
        );
    }

    #[test]
    fn test_a_bars_file_is_refused_by_its_schema() {
        let bars = crate::archive::bars::encode(
            &Leg::MassiveDailyBars.key(session()),
            &[],
            &crate::archive::bars::Provenance::new(
                crate::archive::bars::Subscription::StocksStarter,
                Utc::now(),
                RunId::new(Uuid::from_u128(7)),
                None,
            ),
        )
        .unwrap();
        assert!(matches!(
            decode(&key(), bars),
            Err(DecodeRefusal::File(ReadRefusal::Schema { .. }))
        ));
    }

    proptest! {
        /// Whatever the lines, readable or not, the file gives back exactly the lines it was written from.
        #[test]
        fn property_lines_round_trip(
            seconds in prop::collection::vec(0_i64..86_399, 0..6),
            torn in prop::collection::vec("[ -~]{0,20}", 0..3),
        ) {
            let start = session().midnight().timestamp();
            let mut texts: Vec<String> = seconds
                .iter()
                .enumerate()
                .map(|(index, second)| {
                    let timestamp = DateTime::from_timestamp(start + second, 0).unwrap().to_rfc3339();
                    record(
                        index as u64 + 1,
                        &timestamp,
                        Observation::ConfigurationResolved(ConfigurationResolved::new(BTreeMap::new())),
                    )
                })
                .collect();
            texts.extend(torn);
            let lines = read(&texts.join("\n"));
            let bytes = encode(&key(), &lines).unwrap();
            prop_assert_eq!(decode(&key(), bytes).unwrap(), lines);
        }
    }
}
