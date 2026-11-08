//! A session's journal as Parquet: one row per line of the session file, the envelope as typed columns and the
//! observation's payload as JSON text, so a reader filters by event and run and a new observation needs no new column.

use std::sync::Arc;

use arrow_array::builder::{StringBuilder, TimestampNanosecondBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::DateTime;

use super::parquet::{self, ReadRefusal};
use crate::common::journal::{Commit, ReadLine, read_one};
use crate::common::storage::JournalKey;
use crate::common::time::SessionDate;

const LAYOUT_VERSION: &str = "1";

/// Why lines were not written under a key.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeRefusal {
    /// A record from another session than the key's.
    OutsideKey {
        line: usize,
        session: SessionDate,
    },
    /// A timestamp past what nanoseconds since the epoch hold, the year 2262.
    Unrepresentable {
        line: usize,
    },
    Parquet(parquet::WriteRefusal),
}

/// Why a file was not read as a journal.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeRefusal {
    File(ReadRefusal),
    /// A row no line could have been written as, such as a record from another session than the key's.
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

/// A line's envelope as its columns hold it.
struct Envelope {
    schema_version: u64,
    run_id: String,
    sequence: u64,
    timestamp: chrono::DateTime<chrono::Utc>,
    commit: Option<String>,
    event_type: String,
    payload: String,
}

/// The envelope a read line carries, or an unreadable line's number and text, which has none.
fn envelope(line: &ReadLine) -> Result<Envelope, (usize, &str)> {
    match line {
        ReadLine::Read(read) => Ok(Envelope {
            schema_version: read.schema_version(),
            run_id: read.run_id().to_string(),
            sequence: read.sequence(),
            timestamp: read.timestamp(),
            commit: read.commit().map(Commit::to_string),
            event_type: read.event_type().to_string(),
            payload: read.payload().to_string(),
        }),
        ReadLine::Unreadable { line, text, .. } => Err((*line, text)),
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
pub fn encode(key: &JournalKey, lines: &[ReadLine]) -> Result<Vec<u8>, EncodeRefusal> {
    let session = key.session();
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
        match envelope(line) {
            Ok(envelope) => {
                let number = index + 1;
                let stamped = SessionDate::at(envelope.timestamp);
                if stamped != session {
                    return Err(EncodeRefusal::OutsideKey {
                        line: number,
                        session: stamped,
                    });
                }
                let nanoseconds = envelope
                    .timestamp
                    .timestamp_nanos_opt()
                    .ok_or(EncodeRefusal::Unrepresentable { line: number })?;
                numbers.append_value(number as u64);
                versions.append_value(envelope.schema_version);
                run_ids.append_value(envelope.run_id);
                sequences.append_value(envelope.sequence);
                timestamps.append_value(nanoseconds);
                commits.append_option(envelope.commit);
                event_types.append_value(envelope.event_type);
                payloads.append_value(envelope.payload);
                unreadables.append_null();
            }
            Err((line, text)) => {
                numbers.append_value(line as u64);
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
    parquet::write(schema(), columns, LAYOUT_VERSION, Vec::new()).map_err(EncodeRefusal::Parquet)
}

/// The lines a file written by `encode` under `key` holds, each read again through the journal's own reader, so a
/// row edited out of band reads back as what it now says rather than what it was.
pub fn decode(key: &JournalKey, bytes: Vec<u8>) -> Result<Vec<ReadLine>, DecodeRefusal> {
    let session = key.session();
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
            let stamped_in_session = |stamped: SessionDate| match stamped == session {
                true => Ok(()),
                false => Err(refused(&format!("a record from {stamped} under {session}"))),
            };
            if unreadables.is_valid(row) {
                let read = read_one(number, unreadables.value(row));
                match &read {
                    ReadLine::Read(read) => stamped_in_session(read.session())?,
                    ReadLine::Unreadable { .. } => {}
                }
                lines.push(read);
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
            // Checked on the typed column, so a row whose line would not read is held to the key too.
            let timestamp = DateTime::from_timestamp_nanos(timestamps.value(row));
            stamped_in_session(SessionDate::at(timestamp))?;
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
            lines.push(read_one(number, &text));
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
    use crate::common::heal::{Leg, SessionOutcome, Window};
    use crate::common::journal::{
        Commit, ConfigurationResolved, HealFinished, Observation, Record, RunId, read,
    };
    use crate::common::storage::Host;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 30).unwrap())
    }

    fn key() -> JournalKey {
        JournalKey::new(Host::Archiver, session())
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
            Window::new(vec![session().plus_calendar_days(-1)]).unwrap(),
            BTreeMap::from([(
                Leg::MassiveSplits,
                BTreeMap::from([(session().plus_calendar_days(-1), SessionOutcome::Unreached)]),
            )]),
            BTreeMap::new(),
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
    fn test_a_row_missing_its_envelope_or_holding_a_non_json_payload_refuses_the_file() {
        let reason = |bytes| match decode(&key(), bytes) {
            Err(DecodeRefusal::Row { line, reason }) => (line, reason),
            other => panic!("not refused by row: {other:?}"),
        };
        assert_eq!(
            reason(file_with_row(None, "{}")),
            (
                1,
                "an envelope column is null on a readable line".to_string()
            )
        );
        assert_eq!(
            reason(file_with_row(Some("retired_event"), "not json")).0,
            1
        );
    }

    #[test]
    fn test_a_file_read_under_another_session_is_refused() {
        let bytes = encode(&key(), &read(&session_file())).unwrap();
        let next = JournalKey::new(Host::Archiver, session().plus_calendar_days(1));
        assert!(matches!(
            decode(&next, bytes),
            Err(DecodeRefusal::Row { line: 1, .. })
        ));
    }

    /// A one-row file in typed columns, as an older build may have written it.
    fn file_with_row(event_type: Option<&str>, payload: &str) -> Vec<u8> {
        file_with_row_on(session(), event_type, payload)
    }

    /// `file_with_row` stamped at the start of `stamped`.
    fn file_with_row_on(stamped: SessionDate, event_type: Option<&str>, payload: &str) -> Vec<u8> {
        let nanoseconds = stamped.midnight().timestamp_nanos_opt().unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![1_u64])),
            Arc::new(UInt64Array::from(vec![1_u64])),
            Arc::new(StringArray::from(vec![Uuid::from_u128(7).to_string()])),
            Arc::new(UInt64Array::from(vec![1_u64])),
            Arc::new(TimestampNanosecondArray::from(vec![nanoseconds]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![event_type])),
            Arc::new(StringArray::from(vec![payload])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ];
        parquet::write(schema(), columns, LAYOUT_VERSION, Vec::new()).unwrap()
    }

    /// A row naming a retired event reads back with its envelope, and is written back into the envelope's columns
    /// rather than as text, so it stays filterable by run and event.
    #[test]
    fn test_a_row_naming_a_retired_event_reads_back_and_keeps_its_columns() {
        let lines = decode(
            &key(),
            file_with_row(Some("retired_event"), r#"{"kept":true}"#),
        )
        .unwrap();
        let [ReadLine::Read(read)] = lines.as_slice() else {
            panic!("not read: {lines:?}");
        };
        assert_eq!(read.event_type(), "retired_event");
        assert_eq!(read.run_id(), RunId::new(Uuid::from_u128(7)));
        assert_eq!(read.payload(), &serde_json::json!({"kept": true}));
        let bytes = encode(&key(), &lines).unwrap();
        assert_eq!(
            bytes,
            file_with_row(Some("retired_event"), r#"{"kept":true}"#)
        );
        assert_eq!(decode(&key(), bytes).unwrap(), lines);
    }

    #[test]
    fn test_a_retired_row_from_another_session_refuses_the_file() {
        let next = session().plus_calendar_days(1);
        assert_eq!(
            decode(&key(), file_with_row_on(next, Some("retired_event"), "{}")),
            Err(DecodeRefusal::Row {
                line: 1,
                reason: "a record from 2026-10-01 under 2026-09-30".to_string(),
            })
        );
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
    fn test_a_trades_file_is_refused_by_its_schema() {
        use crate::common::market::aggregate::TradeTotals;
        use crate::common::market::record::{BarInterval, Partition, Trade};
        use crate::common::market::trade_bars::{TradeBar, TradeSums};
        use crate::common::market::{Price, Shares, Symbol};
        use crate::storage::provenance::{Provenance, Subscription};
        let trade = Trade::new(
            Symbol::new("AAPL").unwrap(),
            session().regular_close(),
            Price::from_dollars(10.0).unwrap(),
            Shares::whole(1).unwrap(),
        )
        .unwrap();
        let bar = TradeBar::new(
            Symbol::new("AAPL").unwrap(),
            BarInterval::OneDay,
            session().regular_close(),
            TradeSums::new(TradeTotals::of(&trade), None, None),
        )
        .unwrap();
        let bars = crate::storage::trade_bars::encode(
            &crate::common::heal::alpaca_trades(session()),
            &Partition::try_from(vec![bar]).unwrap(),
            &Provenance::new(
                Subscription::AlgoTraderPlus,
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
