//! The S3 bar archive: the trainer's data, repaired to a window rather than topped up by a night.
//!
//! One partition per session under `data/equity/bars/year=/month=/day=/data.parquet`, written by
//! whoever holds Massive credentials — the seeder on a cold bucket, the trainer every night. Both
//! call [`archive_missing_sessions`], because two implementations of this would drift the way the
//! trainer's fetch and load stages once did over Eastern versus UTC dates.
//!
//! **What gets fetched is a set difference, not a lookback.** The archive is asked what it already
//! has; everything expected and absent is fetched. A missed night, a week of downtime, and an empty
//! bucket are then the same case handled the same way, where a fixed lookback repairs only the
//! first of the three and leaves the others as permanent holes.
//!
//! **Expected means "worth requesting", not "the market traded".** [`TradingCalendar`] is fetched
//! from Alpaca, and the trainer deliberately holds no broker credentials, so this uses
//! [`SessionDate::is_weekend`] — which its own documentation blesses for exactly this, bounding a
//! fetch range before a calendar is available. Nothing here concludes a date traded: a holiday is
//! requested, Massive answers with nothing, no partition is written, and it is requested again next
//! run. The partition that exists is the evidence; the request set is only a guess about where to
//! look. The cost is roughly ten empty requests a year, which [`ArchiveSummary`] reports rather
//! than hides.
//!
//! [`TradingCalendar`]: crate::data::calendar::TradingCalendar

use std::collections::BTreeSet;
use std::io::Cursor;

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use tracing::{info, warn};

use crate::common::aws::{date_from_partitioned_key, date_partitioned_key};
use crate::common::massive::MassiveClient;
use crate::common::types::SessionDate;
use crate::data::{bars, splits};

/// S3 prefix for the bar archive.
///
/// Deliberately not under `exports/`, which is where the application's nightly database export
/// lands. The two datasets live in one bucket and describe overlapping facts, and giving them one
/// prefix would make whichever job ran second the one that mattered.
pub const BAR_ARCHIVE_PREFIX: &str = "data/equity/bars";

/// S3 key for the ticker metadata that accompanies the archive.
///
/// Written for external readers — DuckDB's `training_details` view resolves here. Training does not
/// read it: the trainer parses the CSV compiled into its own binary, so this copy can be absent
/// without a model run noticing.
pub const DETAILS_ARCHIVE_KEY: &str = "data/equity/details/details.csv";

/// S3 key for the stock splits the bars will be adjusted against; nothing reads it yet.
///
/// One object rather than a partition per session, unlike the bars beside it. A split belongs to
/// its execution date, but the feed revises and cancels announced ones, so a per-date layout would
/// leave a cancelled split sitting in a partition nothing revisits.
///
/// Under `corporate_actions/` rather than a directory of its own, because spinoffs and symbol
/// changes are the same kind of fact read the same way and belong beside it as sibling files.
pub const SPLITS_ARCHIVE_KEY: &str = "data/equity/corporate_actions/splits.parquet";

/// Trailing sessions re-fetched even when a partition already exists.
///
/// Gap-filling alone never revisits a day it has, but a later response can *correct* an earlier one
/// — a bar restated after the close, a symbol that arrived late. That is what [`merge_partitions`]
/// and its last-write-wins strategy are for, and without a deliberate overlap they would have
/// nothing to do. Kept separate from the gap scan because the two answer different questions, and
/// the single fixed lookback that used to serve both could not do either well.
///
/// Counted in sessions, and it has to be taken from `expected` rather than measured backwards from
/// `end` in calendar days. The two disagree across a weekend: a Monday run with a two-*day* floor
/// lands on Saturday, so only Monday is above it and the preceding Friday is never revisited. The
/// trainer runs weekdays, which made that every Monday, on the session a weekend gives the most
/// time to be restated.
const CORRECTION_WINDOW_SESSIONS: usize = 2;

/// Sessions fetched before their partitions are written and the buffer released.
///
/// A grouped response is the whole market — on the order of ten thousand rows per session — so a
/// cold seed of five hundred weekdays fetched in one call would hold several million bars before
/// the first write and lose all of them to one failure. Thirty is a couple of hundred thousand
/// rows, a bounded amount of work to repeat, and the same figure `seed_equity_bars_postgres` picked
/// for the same reason.
const CHUNK_SESSIONS: usize = 30;

/// Read-merge-write cycles attempted before a contended partition is left for the next pass.
///
/// Contention is another archiving pass touching the same session — the nightly repair and an
/// operator's seed overlapping. Two writers converge quickly, so a small bound is enough; giving up
/// costs nothing permanent, because the session is reported failed and the next scan repairs it.
const CONTENDED_WRITE_ATTEMPTS: usize = 3;

/// Errors archiving bars.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("failed to list s3://{bucket}/{prefix}: {message}")]
    List {
        bucket: String,
        prefix: String,
        message: String,
    },
    #[error("failed to read s3://{bucket}/{key}: {message}")]
    Read {
        bucket: String,
        key: String,
        message: String,
    },
    #[error("failed to write s3://{bucket}/{key}: {message}")]
    Write {
        bucket: String,
        key: String,
        message: String,
    },
    #[error("gave up on {key} after {attempts} concurrent writes by another pass")]
    Contended { key: String, attempts: usize },
    /// The upstream feed failed, before any bucket was touched.
    #[error("failed to fetch from Massive: {0}")]
    Feed(String),
    #[error("failed to build a bar frame: {0}")]
    Frame(#[from] PolarsError),
}

/// What a partition write must be true of the object already at the key.
///
/// The archive's guard against two passes clobbering each other. Expressed as a type rather than an
/// `Option<String>` so the two cases cannot be confused at the call site: "the object I read" and
/// "no object at all" map to different S3 headers, and sending the wrong one turns the check off
/// without failing.
enum Precondition {
    /// The object carried this ETag when it was read.
    Match(String),
    /// There was no object at the key.
    Absent,
}

/// The three outcomes of a conditional write, which are not all errors.
///
/// Contention is an expected outcome on a shared bucket, not a fault, so it is separated from a
/// genuine write failure — the caller retries one and propagates the other.
enum WriteOutcome {
    Written,
    Contended,
    Failed(String),
}

/// What one archiving pass accomplished.
///
/// `sessions_without_data` is reported rather than swallowed because it is how a caller tells a
/// healthy run from a broken one. A steady handful is the holidays this deliberately re-requests; a
/// sudden window of them is Massive answering empty, which looks identical in the archive and
/// nothing else would surface.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArchiveSummary {
    /// Sessions that were absent (or inside the correction window) and therefore requested.
    pub sessions_requested: usize,
    /// Sessions that came back with bars and were written.
    pub sessions_written: usize,
    /// Sessions that were requested and returned no bars — holidays, and anything Massive lacks.
    pub sessions_without_data: usize,
    /// Sessions whose request failed outright, carried rather than fatal.
    pub sessions_failed: Vec<SessionDate>,
    /// Bars written across every partition this pass touched.
    pub bars_written: usize,
}

/// Every weekday in `[start, end]` that the archive should be able to answer for.
///
/// Weekends are excluded because they are never sessions anywhere; holidays are not, because
/// knowing them requires the calendar this deliberately does without.
fn expected_sessions(start: SessionDate, end: SessionDate) -> Vec<SessionDate> {
    let mut expected = Vec::new();
    let mut date = start;
    while date <= end {
        if !date.is_weekend() {
            expected.push(date);
        }
        date = date.plus_calendar_days(1);
    }
    expected
}

/// The sessions to request: those with no partition, plus the correction window.
///
/// Split out from the S3 call so the decision itself is testable without a bucket.
fn sessions_to_request(
    expected: &[SessionDate],
    present: &BTreeSet<SessionDate>,
) -> Vec<SessionDate> {
    // The trailing entries of `expected`, which already excludes weekends, so the window is
    // measured in sessions rather than in calendar days that a weekend can swallow.
    let correction_window: BTreeSet<SessionDate> = expected
        .iter()
        .rev()
        .take(CORRECTION_WINDOW_SESSIONS)
        .copied()
        .collect();
    expected
        .iter()
        .copied()
        .filter(|session| !present.contains(session) || correction_window.contains(session))
        .collect()
}

/// Sessions the archive already holds within `[start, end]`.
async fn present_sessions(
    s3_client: &S3Client,
    bucket: &str,
    start: SessionDate,
    end: SessionDate,
) -> Result<BTreeSet<SessionDate>, ArchiveError> {
    let mut present = BTreeSet::new();
    let mut pages = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(format!("{BAR_ARCHIVE_PREFIX}/"))
        .into_paginator()
        .send();

    while let Some(page) = pages.next().await {
        let page = page.map_err(|error| ArchiveError::List {
            bucket: bucket.to_string(),
            prefix: BAR_ARCHIVE_PREFIX.to_string(),
            message: error.to_string(),
        })?;
        for object in page.contents() {
            let Some(date) = object.key().and_then(date_from_partitioned_key) else {
                continue;
            };
            let session = SessionDate::from_date(date);
            if session >= start && session <= end {
                present.insert(session);
            }
        }
    }
    Ok(present)
}

/// Fetches and writes every session in `[window_start, window_end]` the archive is missing.
///
/// Idempotent: a second pass over an unchanged window requests only the correction window and
/// writes the same rows back. Safe to interrupt, because partitions are written as each chunk
/// completes and no state outside the bucket records progress — an interrupted seed keeps
/// everything it had already written, and the next pass sees the rest as gaps.
///
/// Safe to run concurrently with another pass over the same bucket, which the trainer's nightly
/// repair and an operator's seed can be. Each partition is written under a precondition on what was
/// read, so a racing writer is detected rather than overwritten; see [`write_partition`].
pub async fn archive_missing_sessions(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    window_start: SessionDate,
    window_end: SessionDate,
) -> Result<ArchiveSummary, ArchiveError> {
    let expected = expected_sessions(window_start, window_end);
    let present = present_sessions(s3_client, bucket, window_start, window_end).await?;
    let requested = sessions_to_request(&expected, &present);

    info!(
        %window_start,
        %window_end,
        expected = expected.len(),
        present = present.len(),
        requested = requested.len(),
        "Scanned the bar archive for gaps"
    );

    let mut summary = ArchiveSummary {
        sessions_requested: requested.len(),
        ..Default::default()
    };

    // Fetched and written a chunk at a time rather than all at once. A grouped response is the whole
    // market -- on the order of ten thousand rows per session -- so a cold seed of five hundred
    // weekdays held every bar for all of them in memory before the first write. The same reasoning
    // and the same size as `seed_equity_bars_postgres`, whose chunking predates this.
    for chunk in requested.chunks(CHUNK_SESSIONS) {
        archive_chunk(s3_client, massive, bucket, chunk, &mut summary).await?;
    }

    if !summary.sessions_failed.is_empty() {
        // Logged rather than fatal: a failed session costs one partition, and the next pass finds
        // it missing again and retries it. That is the whole point of scanning rather than counting
        // back from today.
        warn!(
            sessions_failed = ?summary.sessions_failed,
            "Some sessions could not be fetched; the next pass will retry them"
        );
    }

    info!(
        sessions_requested = summary.sessions_requested,
        sessions_written = summary.sessions_written,
        sessions_without_data = summary.sessions_without_data,
        sessions_failed = summary.sessions_failed.len(),
        bars_written = summary.bars_written,
        "Bar archive updated"
    );
    Ok(summary)
}

/// Requested sessions that neither answered with bars nor failed outright.
///
/// Counted over the requested sessions rather than by subtracting the number of answers. Responses
/// are grouped by each bar's own timestamp, so a response can carry a session nobody asked for, and
/// subtracting counts would let that extra one stand in for a session that genuinely came back
/// empty — concealing exactly the signal [`ArchiveSummary::sessions_without_data`] exists to carry.
/// Taken together with the written and failed counts, this partitions the requested set.
fn count_sessions_without_data(
    requested: &[SessionDate],
    answered: &BTreeSet<SessionDate>,
    failed: &BTreeSet<SessionDate>,
) -> usize {
    requested
        .iter()
        .filter(|session| !answered.contains(session) && !failed.contains(session))
        .count()
}

/// Fetches one chunk of sessions and writes their partitions, accumulating into `summary`.
async fn archive_chunk(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    chunk: &[SessionDate],
    summary: &mut ArchiveSummary,
) -> Result<(), ArchiveError> {
    let fetched = bars::fetch_daily_bars(massive, chunk).await;
    let failed: BTreeSet<SessionDate> = fetched.dates_failed.iter().copied().collect();
    summary.sessions_failed.extend(fetched.dates_failed);

    // One partition per session date, keyed by the bar's own timestamp rather than by the date that
    // was requested, so a response that answers for a neighbouring session cannot land under the
    // wrong key.
    let mut by_date: std::collections::BTreeMap<SessionDate, Vec<_>> =
        std::collections::BTreeMap::new();
    for bar in fetched.bars {
        by_date
            .entry(SessionDate::at(bar.timestamp()))
            .or_default()
            .push(bar);
    }

    let answered: BTreeSet<SessionDate> = by_date.keys().copied().collect();
    summary.sessions_without_data += count_sessions_without_data(chunk, &answered, &failed);

    for (session, bars_for_session) in by_date {
        let fetched_frame = bars::bars_to_dataframe(&bars_for_session)?;
        let fetched_rows = fetched_frame.height();

        match write_partition(s3_client, bucket, session, fetched_frame).await {
            Ok(()) => {
                // The rows this pass contributed, not the partition's height. Counting the merged
                // total reported the archive's size as though every pass had just written it.
                summary.sessions_written += 1;
                summary.bars_written += fetched_rows;
            }
            Err(ArchiveError::Contended { key, attempts }) => {
                // Another pass kept winning the partition. Recorded as failed rather than retried
                // forever: the next scan finds this session and repairs it, and the writer that did
                // win wrote the same Massive response this one was holding.
                warn!(key, attempts, %session, "Partition contended; the next pass will retry it");
                summary.sessions_failed.push(session);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Merges `fetched` into the partition for `session` and writes it back, conditional on what it read.
///
/// The read-merge-write is a compare-and-swap. S3 returns the object's `ETag` on read, and the write
/// carries `If-Match` on it -- or `If-None-Match: *` when the partition did not exist -- so a pass
/// that raced another one is rejected with `412` instead of silently discarding the other's rows.
/// The whole cycle is retried against the now-current object, which is why the merge is redone
/// rather than the buffer resent.
///
/// This matters more than it would have before bars left the nightly export: `data/` is now their
/// only copy, so a lost write has no second source to recover from.
async fn write_partition(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
    fetched: DataFrame,
) -> Result<(), ArchiveError> {
    let key = date_partitioned_key(BAR_ARCHIVE_PREFIX, session.date());
    write_merged(s3_client, bucket, key, fetched, |existing, fetched, key| {
        merge_or_replace(existing, fetched, key)
    })
    .await
}

/// The read-merge-write cycle itself, over any key and any way of combining the two frames.
///
/// Shared because the bar partitions and the splits table differ only in how they merge — the
/// compare-and-swap around it, and the reasoning for it, are the same either way.
async fn write_merged<F>(
    s3_client: &S3Client,
    bucket: &str,
    key: String,
    fetched: DataFrame,
    merge: F,
) -> Result<(), ArchiveError>
where
    F: Fn(DataFrame, DataFrame, &str) -> Result<DataFrame, ArchiveError>,
{
    for attempt in 1..=CONTENDED_WRITE_ATTEMPTS {
        // Merged with whatever the partition already holds rather than written over it. A plain
        // overwrite would discard anything a later response happens to omit, and merging costs one
        // read of an object this pass is about to replace anyway.
        let existing = read_partition_with_etag(s3_client, bucket, &key).await?;
        let (mut frame, precondition) = match existing {
            Some((existing_frame, etag)) => (
                merge(existing_frame, fetched.clone(), &key)?,
                Precondition::Match(etag),
            ),
            None => (fetched.clone(), Precondition::Absent),
        };

        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer).finish(&mut frame)?;

        match put_partition(s3_client, bucket, &key, buffer, &precondition).await {
            WriteOutcome::Written => return Ok(()),
            // Someone else wrote between this read and this write. Go round again so the merge is
            // redone against what they left, rather than resending a buffer built from stale rows.
            WriteOutcome::Contended => {
                warn!(
                    key,
                    attempt, "Partition changed under a write; merging again"
                )
            }
            WriteOutcome::Failed(message) => {
                return Err(ArchiveError::Write {
                    bucket: bucket.to_string(),
                    key,
                    message,
                })
            }
        }
    }

    Err(ArchiveError::Contended {
        key,
        attempts: CONTENDED_WRITE_ATTEMPTS,
    })
}

/// Merges, or falls back to the fetched frame when the two schemas cannot be combined.
///
/// A partition written before a column was added or renamed cannot be concatenated with a current
/// one, and propagating that would cost every session after it in the pass. The fetched frame is
/// authoritative and current-schema, so replacing is the recoverable outcome; what is lost is
/// whatever the stale partition held that the fresh response omits, which is worth a warning rather
/// than an aborted run.
fn merge_or_replace(
    existing: DataFrame,
    fetched: DataFrame,
    key: &str,
) -> Result<DataFrame, ArchiveError> {
    match merge_partitions(existing, fetched.clone()) {
        Ok(merged) => Ok(merged),
        Err(error) => {
            warn!(
                key,
                %error,
                "Could not merge the existing partition; replacing it with the fetched rows"
            );
            Ok(fetched)
        }
    }
}

/// Fetches the whole splits table and writes it, keeping each row's earliest `first_seen`.
///
/// Accumulates the history that makes the switch to unadjusted bars possible; nothing reads the
/// result yet.
///
/// Not a gap scan like [`archive_missing_sessions`], because there are no gaps to find: the feed
/// answers with its entire current opinion in a few seconds, and that opinion is the answer. Rows
/// it has stopped reporting are dropped rather than kept, which is the case a per-session layout
/// could not express.
pub async fn archive_splits(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    fetched_at: DateTime<Utc>,
) -> Result<usize, ArchiveError> {
    let splits = massive
        .fetch_splits()
        .await
        .map_err(|error| ArchiveError::Feed(error.to_string()))?;

    // Refused before the write rather than merged away inside it, so a cold bucket does not get an
    // empty object either. A feed that answers success with nothing is an outage, not an emptied
    // table.
    if splits.is_empty() {
        warn!(
            key = SPLITS_ARCHIVE_KEY,
            "Splits fetch returned nothing; keeping the stored table"
        );
        return Ok(0);
    }

    let frame = splits::splits_to_dataframe(&splits, fetched_at)?;
    write_merged(
        s3_client,
        bucket,
        SPLITS_ARCHIVE_KEY.to_string(),
        frame,
        // Falls back rather than propagating, for the reason `merge_or_replace` does: a stored
        // object whose schema stopped matching would otherwise fail every future refresh too.
        |existing, fetched, key| match splits::merge_splits(existing, fetched.clone()) {
            Ok(merged) => Ok(merged),
            Err(error) => {
                warn!(
                    key,
                    %error,
                    "Could not merge the stored splits table; replacing it with the fetched rows"
                );
                Ok(fetched)
            }
        },
    )
    .await?;

    info!(
        key = SPLITS_ARCHIVE_KEY,
        splits = splits.len(),
        "Splits table archived"
    );
    Ok(splits.len())
}

/// Writes the ticker metadata that accompanies the archive.
pub async fn archive_details(
    s3_client: &S3Client,
    bucket: &str,
    csv: &str,
) -> Result<(), ArchiveError> {
    put_bytes(
        s3_client,
        bucket,
        DETAILS_ARCHIVE_KEY,
        csv.as_bytes().to_vec(),
        "text/csv",
    )
    .await?;
    info!(key = DETAILS_ARCHIVE_KEY, "Ticker metadata archived");
    Ok(())
}

/// Reads one archived partition, distinguishing a missing object from a failed request.
///
/// `Ok(None)` means the partition genuinely does not exist yet. Every other failure propagates: a
/// credential error, a throttle, or a network fault silently treated as "missing" would shorten the
/// training window without any signal, and the model would just be slightly worse for reasons
/// nothing recorded.
///
/// For readers that only want the data. A writer wants [`read_partition_with_etag`], because the
/// ETag is what makes its write conditional on the version it merged from.
pub async fn read_partition(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Option<DataFrame>, ArchiveError> {
    Ok(read_partition_with_etag(s3_client, bucket, key)
        .await?
        .map(|(frame, _etag)| frame))
}

/// Reads one archived partition together with the ETag it carried.
///
/// The ETag is the version identity a conditional write compares against. Read here rather than
/// through a separate `HeadObject` so it describes the very bytes that were merged; fetching it
/// independently would leave a window in which the object changed between the two calls, which is
/// the race the precondition exists to close.
async fn read_partition_with_etag(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Option<(DataFrame, String)>, ArchiveError> {
    let response = match s3_client.get_object().bucket(bucket).key(key).send().await {
        Ok(response) => response,
        Err(error) => {
            return match error.into_service_error() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                other => Err(ArchiveError::Read {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    message: other.to_string(),
                }),
            }
        }
    };
    let etag = response.e_tag().unwrap_or_default().to_string();
    let bytes = response
        .body
        .collect()
        .await
        .map_err(|error| ArchiveError::Read {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: error.to_string(),
        })?
        .into_bytes();
    Ok(Some((
        ParquetReader::new(Cursor::new(bytes)).finish()?,
        etag,
    )))
}

/// Writes a partition only if the object at `key` still matches `precondition`.
///
/// S3 answers a failed precondition with `412 Precondition Failed`, and `If-None-Match: *` against
/// an object that has appeared since the read with `409 Conflict`. Both mean the same thing here —
/// another pass got there first — so both become [`WriteOutcome::Contended`] rather than an error.
async fn put_partition(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    precondition: &Precondition,
) -> WriteOutcome {
    let request = s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .content_type("application/vnd.apache.parquet");

    let request = match precondition {
        Precondition::Match(etag) => request.if_match(etag),
        Precondition::Absent => request.if_none_match("*"),
    };

    match request.send().await {
        Ok(_) => WriteOutcome::Written,
        Err(error) => {
            let status = error
                .raw_response()
                .map(|response| response.status().as_u16());
            match status {
                Some(412) | Some(409) => WriteOutcome::Contended,
                _ => WriteOutcome::Failed(error.to_string()),
            }
        }
    }
}

/// Combines an existing partition with a freshly fetched one, newest row winning per key.
///
/// The key is `(ticker, bar_interval, timestamp)` — the same primary key `equity_bars` uses, so the
/// archive and the table agree about what constitutes a duplicate. The fetched rows are appended
/// last and `UniqueKeepStrategy::Last` keeps them, which makes a re-fetch a correction rather than a
/// duplicate.
fn merge_partitions(existing: DataFrame, fetched: DataFrame) -> Result<DataFrame, ArchiveError> {
    let combined = concat([existing.lazy(), fetched.lazy()], UnionArgs::default())?
        .unique_stable(
            Some(polars::prelude::Selector::ByName {
                names: vec![
                    PlSmallStr::from("ticker"),
                    PlSmallStr::from("bar_interval"),
                    PlSmallStr::from("timestamp"),
                ]
                .into(),
                strict: false,
            }),
            UniqueKeepStrategy::Last,
        )
        .collect()?;
    Ok(combined)
}

/// Puts bytes at `key`, matching the pattern `export::write_frame` uses for the other prefix.
async fn put_bytes(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    content_type: &str,
) -> Result<(), ArchiveError> {
    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .content_type(content_type)
        .send()
        .await
        .map_err(|error| ArchiveError::Write {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: error.to_string(),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::from_ymd_opt(year, month, day).expect("test date must be valid"),
        )
    }

    /// 2026-06-01 is a Monday, so the week runs Mon-Fri 1..=5 and the weekend is 6-7.
    #[test]
    fn test_expected_sessions_excludes_weekends() {
        let expected = expected_sessions(session(2026, 6, 1), session(2026, 6, 7));
        assert_eq!(
            expected,
            vec![
                session(2026, 6, 1),
                session(2026, 6, 2),
                session(2026, 6, 3),
                session(2026, 6, 4),
                session(2026, 6, 5),
            ]
        );
    }

    #[test]
    fn test_expected_sessions_is_inclusive_of_both_ends() {
        let expected = expected_sessions(session(2026, 6, 3), session(2026, 6, 3));
        assert_eq!(expected, vec![session(2026, 6, 3)]);
    }

    #[test]
    fn test_expected_sessions_is_empty_when_the_window_is_a_weekend() {
        assert!(expected_sessions(session(2026, 6, 6), session(2026, 6, 7)).is_empty());
    }

    /// The cold-start case: nothing present means everything expected is requested.
    #[test]
    fn test_an_empty_archive_requests_the_whole_window() {
        let expected = expected_sessions(session(2026, 6, 1), session(2026, 6, 5));
        let requested = sessions_to_request(&expected, &BTreeSet::new());
        assert_eq!(requested, expected);
    }

    /// The self-heal property: one interior hole is the only thing refetched, aside from the
    /// correction window. This is what the previous fixed lookback could not do.
    #[test]
    fn test_only_missing_sessions_and_the_correction_window_are_requested() {
        let end = session(2026, 6, 19);
        let expected = expected_sessions(session(2026, 6, 1), end);
        let present: BTreeSet<SessionDate> = expected
            .iter()
            .copied()
            .filter(|date| *date != session(2026, 6, 3))
            .collect();

        let requested = sessions_to_request(&expected, &present);

        // The hole, plus the trailing sessions after the correction floor (2026-06-17).
        assert_eq!(
            requested,
            vec![
                session(2026, 6, 3),
                session(2026, 6, 18),
                session(2026, 6, 19),
            ]
        );
    }

    /// A fully populated window still refetches its tail, which is what gives `merge_partitions`
    /// something to correct.
    #[test]
    fn test_a_complete_archive_still_requests_the_correction_window() {
        let end = session(2026, 6, 19);
        let expected = expected_sessions(session(2026, 6, 1), end);
        let present: BTreeSet<SessionDate> = expected.iter().copied().collect();

        let requested = sessions_to_request(&expected, &present);

        assert_eq!(requested, vec![session(2026, 6, 18), session(2026, 6, 19)]);
    }

    /// The correction window is sessions, not calendar days, and a weekend is where the two part
    /// company.
    ///
    /// Measured backwards from a Monday in calendar days, a two-day floor lands on Saturday and
    /// only Monday clears it -- so the previous Friday, which a weekend gives the most time to be
    /// restated, was never revisited. The trainer runs weekdays, so that was every Monday. The
    /// earlier tests all ended on a Friday and never saw it.
    #[test]
    fn test_the_correction_window_reaches_back_over_a_weekend() {
        // 2026-06-22 is a Monday; the preceding session is Friday 2026-06-19.
        let end = session(2026, 6, 22);
        let expected = expected_sessions(session(2026, 6, 1), end);
        let present: BTreeSet<SessionDate> = expected.iter().copied().collect();

        let requested = sessions_to_request(&expected, &present);

        assert_eq!(
            requested,
            vec![session(2026, 6, 19), session(2026, 6, 22)],
            "a Monday run must still correct the Friday before it"
        );
    }

    /// The window is bounded by the sessions that exist, not by its nominal length.
    #[test]
    fn test_the_correction_window_cannot_exceed_the_expected_sessions() {
        let expected = expected_sessions(session(2026, 6, 1), session(2026, 6, 1));
        let present: BTreeSet<SessionDate> = expected.iter().copied().collect();

        assert_eq!(
            sessions_to_request(&expected, &present),
            vec![session(2026, 6, 1)]
        );
    }

    /// An empty window requests nothing rather than panicking on the trailing take.
    #[test]
    fn test_an_empty_window_requests_nothing() {
        assert!(sessions_to_request(&[], &BTreeSet::new()).is_empty());
    }

    /// A session Massive has no data for is never written, so the next scan finds it absent and
    /// requests it again. Holidays live here, and so does anything genuinely missing upstream.
    #[test]
    fn test_a_session_that_returned_no_data_is_requested_again() {
        let end = session(2026, 6, 5);
        let expected = expected_sessions(session(2026, 6, 1), end);
        let holiday = session(2026, 6, 3);

        // First pass: everything requested, everything but the holiday comes back and is written.
        let present: BTreeSet<SessionDate> = expected
            .iter()
            .copied()
            .filter(|date| *date != holiday)
            .collect();

        assert!(sessions_to_request(&expected, &present).contains(&holiday));
    }

    /// The three counts partition the requested set, which is what makes the summary readable as
    /// "everything asked for is accounted for".
    #[test]
    fn test_the_counts_partition_the_requested_sessions() {
        let requested = vec![
            session(2026, 6, 1),
            session(2026, 6, 2),
            session(2026, 6, 3),
            session(2026, 6, 4),
        ];
        let answered: BTreeSet<SessionDate> = [session(2026, 6, 1), session(2026, 6, 2)].into();
        let failed: BTreeSet<SessionDate> = [session(2026, 6, 3)].into();

        let without_data = count_sessions_without_data(&requested, &answered, &failed);

        assert_eq!(without_data, 1);
        assert_eq!(
            answered.len() + failed.len() + without_data,
            requested.len()
        );
    }

    /// A response can be grouped under a session that was never requested — the bar's own timestamp
    /// decides the key. That extra entry must not cancel out a session that really came back empty.
    #[test]
    fn test_an_unrequested_answer_does_not_mask_an_empty_session() {
        let requested = vec![session(2026, 6, 1), session(2026, 6, 2)];
        // 06-03 was never asked for; 06-02 answered with nothing.
        let answered: BTreeSet<SessionDate> = [session(2026, 6, 1), session(2026, 6, 3)].into();

        let without_data = count_sessions_without_data(&requested, &answered, &BTreeSet::new());

        assert_eq!(
            without_data, 1,
            "the unrequested 06-03 must not stand in for the empty 06-02"
        );
    }

    /// Schema drift must cost one partition's history, not the whole pass. The fetched rows are
    /// current-schema and authoritative, so replacing is the recoverable outcome.
    #[test]
    fn test_a_partition_that_cannot_be_merged_is_replaced_by_the_fetched_rows() {
        let existing = df![
            "ticker" => ["AAPL"],
            "a_retired_column" => [1_i64],
        ]
        .unwrap();
        let fetched = df![
            "ticker" => ["AAPL"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_i64],
            "close_price" => [101.0_f64],
        ]
        .unwrap();

        let result = merge_or_replace(existing, fetched.clone(), "some/key").unwrap();

        assert_eq!(result.get_column_names(), fetched.get_column_names());
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn test_merge_keeps_the_fetched_row_for_a_repeated_key() {
        let existing = df![
            "ticker" => ["AAPL"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_i64],
            "close_price" => [100.0_f64],
        ]
        .unwrap();
        let fetched = df![
            "ticker" => ["AAPL"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_i64],
            "close_price" => [101.0_f64],
        ]
        .unwrap();

        let merged = merge_partitions(existing, fetched).unwrap();

        assert_eq!(merged.height(), 1);
        assert_eq!(
            merged
                .column("close_price")
                .unwrap()
                .f64()
                .unwrap()
                .get(0)
                .unwrap(),
            101.0,
            "the fetched row must win, so a re-fetch is a correction rather than a duplicate"
        );
    }

    #[test]
    fn test_merge_keeps_rows_the_fetched_partition_omits() {
        let existing = df![
            "ticker" => ["AAPL", "MSFT"],
            "bar_interval" => ["one_day", "one_day"],
            "timestamp" => [1_i64, 1_i64],
            "close_price" => [100.0_f64, 200.0_f64],
        ]
        .unwrap();
        let fetched = df![
            "ticker" => ["AAPL"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_i64],
            "close_price" => [101.0_f64],
        ]
        .unwrap();

        let merged = merge_partitions(existing, fetched).unwrap();

        assert_eq!(
            merged.height(),
            2,
            "a symbol absent from a later response must survive the merge"
        );
    }
}
