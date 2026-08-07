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
use polars::prelude::*;
use tracing::{info, warn};

use crate::common::aws::{date_from_partitioned_key, date_partitioned_key};
use crate::common::massive::MassiveClient;
use crate::data::bars;
use crate::data::calendar::SessionDate;

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

/// Trailing sessions re-fetched even when a partition already exists.
///
/// Gap-filling alone never revisits a day it has, but a later response can *correct* an earlier one
/// — a bar restated after the close, a symbol that arrived late. That is what [`merge_partitions`]
/// and its last-write-wins strategy are for, and without a deliberate overlap they would have
/// nothing to do. Kept separate from the gap scan because the two answer different questions, and
/// the single fixed lookback that used to serve both could not do either well.
const CORRECTION_WINDOW_SESSIONS: i64 = 2;

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
    #[error("failed to build a bar frame: {0}")]
    Frame(#[from] PolarsError),
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
    end: SessionDate,
) -> Vec<SessionDate> {
    let correction_floor = end.plus_calendar_days(-CORRECTION_WINDOW_SESSIONS);
    expected
        .iter()
        .copied()
        .filter(|session| !present.contains(session) || *session > correction_floor)
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
/// writes the same rows back. Safe to interrupt, because each partition is written whole and no
/// state outside the bucket records progress.
pub async fn archive_missing_sessions(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    window_start: SessionDate,
    window_end: SessionDate,
) -> Result<ArchiveSummary, ArchiveError> {
    let expected = expected_sessions(window_start, window_end);
    let present = present_sessions(s3_client, bucket, window_start, window_end).await?;
    let requested = sessions_to_request(&expected, &present, window_end);

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
    if requested.is_empty() {
        return Ok(summary);
    }

    let fetched = bars::fetch_daily_bars(massive, &requested).await;
    summary.sessions_failed = fetched.dates_failed;
    if !summary.sessions_failed.is_empty() {
        // Logged rather than fatal: a failed session costs one partition, and the next pass finds
        // it missing again and retries it. That is the whole point of scanning rather than counting
        // back from today.
        warn!(
            sessions_failed = ?summary.sessions_failed,
            "Some sessions could not be fetched; the next pass will retry them"
        );
    }

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

    summary.sessions_without_data = summary
        .sessions_requested
        .saturating_sub(by_date.len() + summary.sessions_failed.len());

    for (session, bars_for_session) in by_date {
        let key = date_partitioned_key(BAR_ARCHIVE_PREFIX, session.date());
        let fetched_frame = bars::bars_to_dataframe(&bars_for_session)?;
        let fetched_rows = fetched_frame.height();

        // Merged with whatever the partition already holds rather than written over it. A plain
        // overwrite would discard anything a later response happens to omit, and merging costs one
        // read of an object this pass is about to replace anyway.
        let mut frame = match read_partition(s3_client, bucket, &key).await? {
            Some(existing) => merge_partitions(existing, fetched_frame)?,
            None => fetched_frame,
        };

        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer).finish(&mut frame)?;
        put_bytes(
            s3_client,
            bucket,
            &key,
            buffer,
            "application/vnd.apache.parquet",
        )
        .await?;

        // The rows this pass contributed, not the partition's height. Counting the merged total
        // reported the archive's size as though every pass had just written it.
        summary.sessions_written += 1;
        summary.bars_written += fetched_rows;
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
pub async fn read_partition(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Option<DataFrame>, ArchiveError> {
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
    Ok(Some(ParquetReader::new(Cursor::new(bytes)).finish()?))
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
        let requested = sessions_to_request(&expected, &BTreeSet::new(), session(2026, 6, 5));
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

        let requested = sessions_to_request(&expected, &present, end);

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

        let requested = sessions_to_request(&expected, &present, end);

        assert_eq!(requested, vec![session(2026, 6, 18), session(2026, 6, 19)]);
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

        assert!(sessions_to_request(&expected, &present, end).contains(&holiday));
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
