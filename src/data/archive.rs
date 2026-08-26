//! The S3 bar archive: the trainer's data, repaired to a window rather than topped up by a night.
//!
//! One partition per session and cadence under `data/equity/bars/interval=/year=/month=/day=/`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use tracing::{info, warn};

use crate::common::alpaca::MarketDataClient;
use crate::common::aws::{date_from_partitioned_key, date_partitioned_key};
use crate::common::flatfiles::{FlatFileClient, FlatFileError};
use crate::common::massive::MassiveClient;
use crate::common::types::{
    BarInterval, EquityBar, LiquidityFloor, QuoteSummary, SessionDate, Ticker,
};
use crate::data::calendar::TradingCalendar;
use crate::data::{bars, boundaries, quotes, splits};

/// Root of the bar archive, never a partition prefix on its own — [`bar_archive_prefix`] adds the
/// cadence, and a key built without one collides with every other cadence of the same session.
///
/// Deliberately not under `exports/`, which is where the application's nightly database export
/// lands. The two datasets live in one bucket and describe overlapping facts, and giving them one
/// prefix would make whichever job ran second the one that mattered.
pub const BAR_ARCHIVE_PREFIX: &str = "data/equity/bars";

/// The archive prefix for one bar cadence.
///
/// Hive-partitioned on the interval, so a reader that scans the tree gets the cadence as a column
/// and a second cadence costs one more value rather than a parallel tree. Daily and intraday bars
/// describe overlapping facts — a daily bar is the aggregate of its own intraday bars — and one
/// partition holding both would make whichever job wrote last the one that mattered.
pub fn bar_archive_prefix(interval: BarInterval) -> String {
    format!("{BAR_ARCHIVE_PREFIX}/interval={interval}")
}

/// S3 key for the ticker metadata that accompanies the archive.
///
/// Written for external readers — DuckDB's `training_details` view resolves here. Training does not
/// read it: the trainer parses the CSV compiled into its own binary, so this copy can be absent
/// without a model run noticing.
pub const DETAILS_ARCHIVE_KEY: &str = "data/equity/details/details.csv";

/// S3 key for the stock splits the bars are adjusted against.
///
/// Read by [`crate::data::adjust::SplitTableCache`] at read time and by the trainer before it
/// builds a dataset, so both see the same basis.
///
/// One object rather than a partition per session, unlike the bars beside it. A split belongs to
/// its execution date, but the feed revises and cancels announced ones, so a per-date layout would
/// leave a cancelled split sitting in a partition nothing revisits.
///
/// Under `corporate_actions/` rather than a directory of its own, because spinoffs and symbol
/// changes are the same kind of fact read the same way and belong beside it as sibling files.
pub const SPLITS_ARCHIVE_KEY: &str = "data/equity/corporate_actions/splits.parquet";

/// Object holding every date a symbol's price series may not be read across.
///
/// Beside the splits table rather than merged into it, because the two are read for opposite
/// purposes: a split says how to restate a price across a date, a boundary says not to.
pub const BOUNDARIES_ARCHIVE_KEY: &str = "data/equity/corporate_actions/boundaries.parquet";

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
/// rows, a bounded amount of work to repeat, and the same figure `seed equity-bars daily postgres` picked
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
    /// The trading calendar does not span the window, so which dates trade is unanswerable.
    ///
    /// Fatal rather than carried, unlike a failed session: a calendar short of the window drops real
    /// sessions from the request and the pass would report a complete run over a window it never
    /// covered — the one failure the summary itself cannot show.
    #[error("the trading calendar does not cover {start} to {end}")]
    Calendar {
        start: SessionDate,
        end: SessionDate,
    },
    /// The upstream feed failed, before any bucket was touched.
    ///
    /// The vendor is carried because two of them supply this archive, and an operator reading the
    /// message during an incident needs to know which one to look at.
    #[error("failed to fetch from {vendor}: {message}")]
    Feed {
        vendor: &'static str,
        message: String,
    },
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

/// What a pass produced, whose units differ where it matters.
///
/// `quotes_folded` counts ticks fetched and discarded, which is what decides whether a backfill is
/// affordable: a session yields roughly 79 summaries per name and hundreds of thousands of quotes to
/// produce them. Per variant rather than beside the shared counts, so a bar pass cannot report one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassOutput {
    /// Bars written across every partition the pass touched.
    Bars { bars_written: usize },
    /// Summary rows written across both cadences, and the ticks folded to produce them.
    Quotes {
        summaries_written: usize,
        quotes_folded: usize,
    },
}

impl PassOutput {
    /// Rows this pass wrote to the archive, whichever kind it writes.
    ///
    /// For a caller that wants the one number rather than the variant — the trainer logs it, and a
    /// bar pass and a quote pass both mean "rows that landed in a partition" by it.
    pub fn rows_written(&self) -> usize {
        match self {
            PassOutput::Bars { bars_written } => *bars_written,
            PassOutput::Quotes {
                summaries_written, ..
            } => *summaries_written,
        }
    }
}

/// How the sessions a pass requested were chosen, which decides whether an empty answer is a fault.
///
/// Not a label for which pass ran: [`expected_sessions`] excludes weekends but not holidays, because
/// knowing them needs the calendar it deliberately does without. A holiday requested that way
/// answers empty forever and is not a defect; one requested from a calendar-filtered list is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionSource {
    /// Every weekday in the window, holidays included.
    Weekdays,
    /// Already filtered against the exchange calendar, so no holiday survives to answer empty.
    TradingCalendar,
}

impl SessionSource {
    /// Read off the calendar a pass actually held, so nothing can claim a filtering it never did.
    fn of(calendar: Option<&TradingCalendar>) -> Self {
        match calendar {
            Some(_) => SessionSource::TradingCalendar,
            None => SessionSource::Weekdays,
        }
    }
}

/// What one archiving pass accomplished.
///
/// Fields are private and read through accessors: every one is a result, except the session source,
/// which is the policy that interprets them — a caller able to set that could call a broken pass
/// clean, and the counts carry a partition invariant nothing outside should be able to break.
#[derive(Debug, Clone, PartialEq)]
pub struct PassSummary {
    sessions_requested: usize,
    sessions_written: usize,
    sessions_without_data: usize,
    sessions_failed: Vec<SessionDate>,
    symbols_failed: usize,
    output: PassOutput,
    sessions: SessionSource,
}

impl PassSummary {
    /// Sessions that were absent (or inside the correction window) and therefore requested.
    pub fn sessions_requested(&self) -> usize {
        self.sessions_requested
    }

    /// Sessions that came back with data and were written.
    pub fn sessions_written(&self) -> usize {
        self.sessions_written
    }

    /// Sessions that were requested and returned nothing.
    ///
    /// Reported rather than swallowed because it is how a caller tells a healthy run from a broken
    /// one. Whether it is a fault depends on how the session list was built, which is what
    /// [`PassSummary::is_complete`] settles.
    pub fn sessions_without_data(&self) -> usize {
        self.sessions_without_data
    }

    /// Sessions whose fetch or write failed, carried rather than fatal.
    ///
    /// A non-empty list is an incomplete pass: `Ok` means the pass ran to the end, not that every
    /// requested session was written.
    pub fn sessions_failed(&self) -> &[SessionDate] {
        &self.sessions_failed
    }

    /// Symbols the pass could not fetch after every attempt.
    ///
    /// Reported because nothing downstream can detect one: a session-level gap scan sees the
    /// partition and moves on, so a symbol missing from it stays missing until someone re-runs the
    /// window. Always zero on the daily path, which fetches the market in one call.
    pub fn symbols_failed(&self) -> usize {
        self.symbols_failed
    }

    /// What landed in the archive, in the units of the pass that wrote it.
    pub fn output(&self) -> &PassOutput {
        &self.output
    }

    /// Whether every session this pass requested is now in the archive.
    ///
    /// The one rule every binary's exit code derives from. Deciding it per binary was wrong twice —
    /// bars exiting 0 on failure, quotes counting holidays as failure — and it is silent when wrong,
    /// because a partition an incomplete pass wrote reads as complete to everything downstream.
    pub fn is_complete(&self) -> bool {
        self.sessions_failed.is_empty()
            && self.symbols_failed == 0
            && match self.sessions {
                // A pass given no calendar still requested holidays, and a holiday is
                // indistinguishable from a session the vendor lacks, so this cannot be called a fault.
                SessionSource::Weekdays => true,
                SessionSource::TradingCalendar => self.sessions_without_data == 0,
            }
    }
}

impl std::fmt::Display for PassSummary {
    /// The operator's one line, so no two callers can render the same counts differently.
    ///
    /// The tail comes off [`PassOutput`], which is why a quote pass reports the ticks it folded and a
    /// bar pass does not have to print a zero it could never fill.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "requested {} sessions, wrote {}, {} without data, {} failed, {} symbols missing",
            self.sessions_requested,
            self.sessions_written,
            self.sessions_without_data,
            self.sessions_failed.len(),
            self.symbols_failed
        )?;
        match &self.output {
            PassOutput::Bars { bars_written } => write!(formatter, ", {bars_written} bars"),
            PassOutput::Quotes {
                summaries_written,
                quotes_folded,
            } => write!(
                formatter,
                ", {summaries_written} summaries, {quotes_folded} quotes folded"
            ),
        }
    }
}

/// What a pass accumulates while it runs, before it is finished into a [`PassSummary`].
///
/// Separate so the published type can carry [`PassOutput`] as an enum without any code matching on a
/// variant it might not have: the variant is built once, by the finisher, where it is not in doubt.
#[derive(Debug, Default)]
struct PassProgress {
    sessions_requested: usize,
    sessions_written: usize,
    sessions_without_data: usize,
    sessions_failed: Vec<SessionDate>,
    symbols_failed: usize,
    /// Bars on a bar pass, summary rows on a quote pass — one quantity, named at the finish.
    rows_written: usize,
}

impl PassProgress {
    fn into_bar_summary(self, calendar: Option<&TradingCalendar>) -> PassSummary {
        PassSummary {
            sessions_requested: self.sessions_requested,
            sessions_written: self.sessions_written,
            sessions_without_data: self.sessions_without_data,
            sessions_failed: self.sessions_failed,
            symbols_failed: self.symbols_failed,
            output: PassOutput::Bars {
                bars_written: self.rows_written,
            },
            sessions: SessionSource::of(calendar),
        }
    }

    /// `quotes_folded` arrives by return rather than through this accumulator, so no bar pass ever
    /// holds a counter it cannot fill.
    fn into_quote_summary(self, quotes_folded: usize) -> PassSummary {
        PassSummary {
            sessions_requested: self.sessions_requested,
            sessions_written: self.sessions_written,
            sessions_without_data: self.sessions_without_data,
            sessions_failed: self.sessions_failed,
            symbols_failed: self.symbols_failed,
            output: PassOutput::Quotes {
                summaries_written: self.rows_written,
                quotes_folded,
            },
            sessions: SessionSource::TradingCalendar,
        }
    }
}

/// Every weekday in `[start, end]` that the archive should be able to answer for.
///
/// Weekends are excluded because they are never sessions anywhere; holidays are not, because
/// knowing them requires a calendar this deliberately does without — see [`sessions_in_window`],
/// which applies one when the caller has it.
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

/// The sessions a pass should be able to answer for, filtered by `calendar` when it has one.
///
/// Refused rather than narrowed when the calendar does not span the window: `is_trading_day` answers
/// `false` outside its published horizon, so a short calendar would drop real sessions, shrink
/// `sessions_requested`, and leave the pass reporting a complete run over a window it never covered.
fn sessions_in_window(
    start: SessionDate,
    end: SessionDate,
    calendar: Option<&TradingCalendar>,
) -> Result<Vec<SessionDate>, ArchiveError> {
    let weekdays = expected_sessions(start, end);
    let Some(calendar) = calendar else {
        return Ok(weekdays);
    };
    if !calendar.covers(start, end) {
        return Err(ArchiveError::Calendar { start, end });
    }
    Ok(weekdays
        .into_iter()
        .filter(|session| calendar.is_trading_day(*session))
        .collect())
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

/// Sessions the bar archive already holds within `[start, end]`.
async fn present_sessions(
    s3_client: &S3Client,
    bucket: &str,
    interval: BarInterval,
    start: SessionDate,
    end: SessionDate,
) -> Result<BTreeSet<SessionDate>, ArchiveError> {
    // Scoped to the cadence being repaired. Listing the whole bar tree would count an intraday
    // partition as a daily session already present, and the gap scan would stop fetching it.
    present_partitions(s3_client, bucket, &bar_archive_prefix(interval), start, end).await
}

/// Sessions that have a partition under `prefix` within `[start, end]`.
async fn present_partitions(
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
    start: SessionDate,
    end: SessionDate,
) -> Result<BTreeSet<SessionDate>, ArchiveError> {
    let prefix = prefix.to_string();
    let mut present = BTreeSet::new();
    let mut pages = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(format!("{prefix}/"))
        .into_paginator()
        .send();

    while let Some(page) = pages.next().await {
        let page = page.map_err(|error| ArchiveError::List {
            bucket: bucket.to_string(),
            prefix: prefix.clone(),
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
/// **A set difference, not a lookback**, so a missed night, a week of downtime, and an empty bucket
/// are one case where a fixed lookback repairs only the first. Expected means "worth requesting"
/// rather than "the market traded", since the trainer holds no broker credentials to consult a
/// calendar with: a holiday is requested, answered with nothing, and requested again, at a cost of
/// roughly ten empty requests a year that [`PassSummary`] reports rather than hides.
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
    calendar: Option<&TradingCalendar>,
) -> Result<PassSummary, ArchiveError> {
    let expected = sessions_in_window(window_start, window_end, calendar)?;
    let present = present_sessions(
        s3_client,
        bucket,
        BarInterval::OneDay,
        window_start,
        window_end,
    )
    .await?;
    let requested = sessions_to_request(&expected, &present);

    info!(
        %window_start,
        %window_end,
        expected = expected.len(),
        present = present.len(),
        requested = requested.len(),
        "Scanned the bar archive for gaps"
    );

    let mut progress = PassProgress {
        sessions_requested: requested.len(),
        ..Default::default()
    };

    // Fetched and written a chunk at a time rather than all at once. A grouped response is the whole
    // market -- on the order of ten thousand rows per session -- so a cold seed of five hundred
    // weekdays held every bar for all of them in memory before the first write. The same reasoning
    // and the same size as `seed equity-bars daily postgres`, whose chunking predates this.
    for chunk in requested.chunks(CHUNK_SESSIONS) {
        archive_chunk(s3_client, massive, bucket, chunk, &mut progress).await?;
    }

    if !progress.sessions_failed.is_empty() {
        // Logged rather than fatal: a failed session costs one partition, and the next pass finds
        // it missing again and retries it. That is the whole point of scanning rather than counting
        // back from today.
        warn!(
            sessions_failed = ?progress.sessions_failed,
            "Some sessions could not be fetched; the next pass will retry them"
        );
    }

    info!(
        sessions_requested = progress.sessions_requested,
        sessions_written = progress.sessions_written,
        sessions_without_data = progress.sessions_without_data,
        sessions_failed = progress.sessions_failed.len(),
        bars_written = progress.rows_written,
        "Bar archive updated"
    );
    Ok(progress.into_bar_summary(calendar))
}

/// Requested sessions that neither answered with bars nor failed outright.
///
/// Counted over the requested sessions rather than by subtracting the number of answers. Responses
/// are grouped by each bar's own timestamp, so a response can carry a session nobody asked for, and
/// subtracting counts would let that extra one stand in for a session that genuinely came back
/// empty — concealing exactly the signal [`PassSummary::sessions_without_data`] exists to carry.
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

/// Chunk sessions that will leave no partition behind, counted once each.
///
/// A session is unwritten when the daily archive cannot describe it or the vendor returns no bars,
/// and an undescribed session is skipped before any fetch, so the two conditions overlap rather
/// than partition. Taken with the written and failed counts, this partitions the chunk.
fn count_intraday_sessions_without_data(
    chunk: &[SessionDate],
    described: &BTreeSet<SessionDate>,
    answered: &BTreeSet<SessionDate>,
) -> usize {
    chunk
        .iter()
        .filter(|session| !described.contains(session) || !answered.contains(session))
        .count()
}

/// Fetches one chunk of sessions and writes their partitions, accumulating into `summary`.
async fn archive_chunk(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    chunk: &[SessionDate],
    progress: &mut PassProgress,
) -> Result<(), ArchiveError> {
    let fetched = bars::fetch_daily_bars(massive, chunk).await;
    let failed: BTreeSet<SessionDate> = fetched.dates_failed.iter().copied().collect();
    progress.sessions_failed.extend(fetched.dates_failed);

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
    progress.sessions_without_data += count_sessions_without_data(chunk, &answered, &failed);

    for (session, bars_for_session) in by_date {
        let fetched_frame = bars::bars_to_dataframe(&bars_for_session)?;
        let fetched_rows = fetched_frame.height();

        match write_partition(
            s3_client,
            bucket,
            BarInterval::OneDay,
            session,
            fetched_frame,
        )
        .await
        {
            Ok(()) => {
                // The rows this pass contributed, not the partition's height. Counting the merged
                // total reported the archive's size as though every pass had just written it.
                progress.sessions_written += 1;
                progress.rows_written += fetched_rows;
            }
            Err(ArchiveError::Contended { key, attempts }) => {
                // Another pass kept winning the partition. Recorded as failed rather than retried
                // forever: the next scan finds this session and repairs it, and the writer that did
                // win wrote the same Massive response this one was holding.
                warn!(key, attempts, %session, "Partition contended; the next pass will retry it");
                progress.sessions_failed.push(session);
            }
            // Carried like contention above, so one session's fault costs that session rather than
            // every session after it. The pass is only complete if `sessions_failed` is empty.
            Err(error) => {
                warn!(%error, %session, "Partition write failed; this session was not archived");
                progress.sessions_failed.push(session);
            }
        }
    }
    Ok(())
}

/// Sessions held in memory before an intraday pass writes its partitions and releases the buffer.
///
/// A month, because requests are ticker-major and partitions session-major: a chunk holds every name
/// before any one session can be written, and a quarter of five-minute bars is several hundred
/// megabytes where a month is a fifth of that. Smaller chunks trade requests for memory.
const INTRADAY_CHUNK_SESSIONS: usize = 21;

/// Attempts per symbol before a chunk gives up on it.
///
/// Load-bearing rather than defensive: a session-level gap scan cannot see a *symbol*-level hole, so
/// one dropped response becomes a name permanently missing from that month. A single transient
/// failure appeared in the first twenty-two sessions and succeeded immediately on retry.
const INTRADAY_SYMBOL_ATTEMPTS: usize = 3;

/// Pause before a symbol's next attempt, growing with the attempt number.
///
/// Eight tasks retrying without one turns a vendor throttle into twenty-four immediate requests at
/// an endpoint that is already refusing.
fn retry_delay(attempt: usize) -> std::time::Duration {
    std::time::Duration::from_millis(250 << attempt.min(4))
}

/// Whether a failure is worth another attempt.
///
/// A 404 for a delisted symbol can never succeed, and a survivorship-free universe is full of them,
/// so retrying every refusal scales the wasted requests with the delisted tail. Throttling and
/// server faults are the transient statuses; a transport error carries no status and is transient by
/// nature.
fn is_transient(error: &crate::common::massive::MassiveError) -> bool {
    match error {
        crate::common::massive::MassiveError::Api { status, .. } => {
            *status == 429 || (500..600).contains(status)
        }
        crate::common::massive::MassiveError::Request(_)
        | crate::common::massive::MassiveError::Parse(_) => true,
        crate::common::massive::MassiveError::Cursor { .. } => false,
    }
}

/// Which names a pass folds, and with it whether that pass may create a partition.
///
/// Creation is not an axis of its own: only [`NameSelection::Named`] cannot create one, because a
/// partition holding a named handful reads as complete to every later pass. The other two derive
/// their universe from the daily partition for the session, so what they write is whole.
#[derive(Debug, Clone, PartialEq)]
pub enum NameSelection {
    /// Every name the daily partition holds, unscreened.
    ///
    /// The only way a name outside the screen reaches the archive at all: the screen decides what
    /// gets fetched, so a spread it excluded cannot be read back off what was written.
    WholeMarket,
    /// The names in the daily partition that clear a liquidity floor.
    Screened(LiquidityFloor),
    /// An explicit set, taken as given rather than intersected with the daily partition.
    ///
    /// The partition merge keys on `(ticker, bar_interval, timestamp)`, so folding into a session
    /// that already holds other names adds these and leaves those untouched.
    Named(BTreeSet<Ticker>),
}

/// Which of the offered sessions a pass touches, judged on whether a partition is already there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSelection {
    /// Only the sessions with no partition, which is what seeding wants.
    ///
    /// No correction window, unlike [`sessions_to_request`] on the daily path: an intraday bar is
    /// not restated after the close the way a daily one is, and re-fetching a month to learn that
    /// costs the whole universe in requests rather than one grouped call.
    Absent,
    /// Only the sessions that already have one, which is what repairing wants.
    ///
    /// Presence cannot answer whether a *symbol* is there: a partition written while one name's
    /// fetch failed reads exactly like a complete one, so this is the set a repair works over.
    Present,
    /// Every offered session, which is the only way a partition already written gets widened.
    Every,
}

/// What a pass is for: which names, across which sessions.
///
/// Private fields because the two axes are not free of each other — [`Scope::new`] is the one place
/// the pair is judged, and a value in scope is proof the pair was allowed.
#[derive(Debug, Clone, PartialEq)]
pub struct Scope {
    names: NameSelection,
    sessions: SessionSelection,
}

/// Why a pair of axes is not a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScopeError {
    /// A named set anywhere but [`SessionSelection::Present`].
    #[error("a named symbol set may only touch sessions that already have a partition")]
    NamedSetWouldCreate,
}

impl Scope {
    /// Refuses a named set outside [`SessionSelection::Present`].
    ///
    /// A named pass fetches only the names it was given, so a session it answered where no
    /// partition existed would hold that handful and read as complete to every later pass.
    pub fn new(names: NameSelection, sessions: SessionSelection) -> Result<Self, ScopeError> {
        if matches!(names, NameSelection::Named(_)) && sessions != SessionSelection::Present {
            return Err(ScopeError::NamedSetWouldCreate);
        }
        Ok(Self { names, sessions })
    }

    /// Whether this pass may write a partition where none exists.
    ///
    /// Derived rather than stored, which is what stops it disagreeing with [`Scope::new`].
    fn may_create(&self) -> bool {
        !matches!(self.names, NameSelection::Named(_))
    }
}

/// Names printed before a scope elides the rest.
///
/// A scan-driven repair names thousands of symbols, and printing the whole list produced a
/// multi-kilobyte log line that buried the fields beside it.
const SCOPE_NAMES_SHOWN: usize = 12;

impl std::fmt::Display for Scope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.names {
            NameSelection::WholeMarket => write!(formatter, "every name")?,
            // The floor, not just the word: two passes run at different floors are different passes
            // and a log field that cannot tell them apart is the reason this exists.
            NameSelection::Screened(floor) => write!(formatter, "the screened universe ({floor})")?,
            NameSelection::Named(symbols) => {
                let shown: Vec<&str> = symbols
                    .iter()
                    .take(SCOPE_NAMES_SHOWN)
                    .map(Ticker::as_str)
                    .collect();
                write!(formatter, "{}", shown.join(","))?;
                let elided = symbols.len().saturating_sub(shown.len());
                if elided > 0 {
                    write!(formatter, " and {elided} more")?;
                }
            }
        }
        let sessions = match self.sessions {
            SessionSelection::Absent => "absent sessions only",
            SessionSelection::Present => "sessions already present",
            SessionSelection::Every => "every session",
        };
        write!(formatter, ", {sessions}")
    }
}

/// The sessions a pass touches, which [`SessionSelection`] decides.
///
/// Split out from the S3 listing so the rule is testable without a bucket. Seeding and repairing
/// want opposite sets, and getting that backwards is the defect #1102 was written for.
fn sessions_for(
    selection: SessionSelection,
    offered: &[SessionDate],
    present: &BTreeSet<SessionDate>,
) -> Vec<SessionDate> {
    offered
        .iter()
        .copied()
        .filter(|session| match selection {
            SessionSelection::Absent => !present.contains(session),
            SessionSelection::Present => present.contains(session),
            SessionSelection::Every => true,
        })
        .collect()
}

/// The names one daily partition contributes to a pass's universe.
///
/// A named set comes back as given rather than intersected: a name that did not trade answers with
/// no data and produces nothing, which is the same outcome as excluding it here.
fn names_from_partition(
    names: &NameSelection,
    daily: DataFrame,
) -> Result<BTreeSet<Ticker>, ArchiveError> {
    match names {
        NameSelection::WholeMarket => partition_tickers(&daily),
        NameSelection::Screened(floor) => screen_partition(daily, *floor),
        NameSelection::Named(symbols) => Ok(symbols.clone()),
    }
}

/// Symbols fetched at once.
///
/// The vendor imposes no rate limit worth pacing against — 25 sequential requests measured at
/// roughly twelve a second with no throttling — so this bounds our own concurrency rather than
/// respecting theirs, and keeps a failure to a handful of symbols rather than the whole chunk.
const INTRADAY_CONCURRENCY: usize = 8;

/// Fetches and writes intraday partitions across `[window_start, window_end]`, per `scope`.
///
/// Scoped to `interval` throughout, so a five-minute pass neither sees nor writes the daily
/// partitions beside it.
pub async fn archive_intraday_sessions(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    interval: BarInterval,
    window_start: SessionDate,
    window_end: SessionDate,
    scope: &Scope,
    calendar: Option<&TradingCalendar>,
) -> Result<PassSummary, ArchiveError> {
    let expected = sessions_in_window(window_start, window_end, calendar)?;
    let present = present_sessions(s3_client, bucket, interval, window_start, window_end).await?;
    let requested = sessions_for(scope.sessions, &expected, &present);

    info!(
        %window_start,
        %window_end,
        interval = %interval,
        %scope,
        expected = expected.len(),
        present = present.len(),
        requested = requested.len(),
        "Planned an intraday pass"
    );

    let mut progress = PassProgress {
        sessions_requested: requested.len(),
        ..Default::default()
    };
    for chunk in requested.chunks(INTRADAY_CHUNK_SESSIONS) {
        archive_intraday_chunk(
            s3_client,
            massive,
            bucket,
            interval,
            chunk,
            scope,
            &present,
            &mut progress,
        )
        .await?;
    }

    if progress.symbols_failed > 0 {
        // Warned separately from the summary below, because this is the only outcome here that
        // needs a person: nothing re-requests a session that was written without one of its names.
        warn!(
            symbols_failed = progress.symbols_failed,
            "Some symbols are absent from the partitions this pass wrote; re-run the window to repair them"
        );
    }
    info!(
        sessions_requested = progress.sessions_requested,
        sessions_written = progress.sessions_written,
        sessions_without_data = progress.sessions_without_data,
        sessions_failed = progress.sessions_failed.len(),
        symbols_failed = progress.symbols_failed,
        bars_written = progress.rows_written,
        "Intraday archive updated"
    );
    Ok(progress.into_bar_summary(calendar))
}

/// One chunk: derive the universe, fan out over it, then write a partition per session.
///
/// `present` is the sessions the archive already holds a partition for, which a named repair needs
/// in order *not* to write one — see [`writable_session`].
#[allow(clippy::too_many_arguments)]
async fn archive_intraday_chunk(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    interval: BarInterval,
    chunk: &[SessionDate],
    scope: &Scope,
    present: &BTreeSet<SessionDate>,
    progress: &mut PassProgress,
) -> Result<(), ArchiveError> {
    let (Some(first), Some(last)) = (chunk.first(), chunk.last()) else {
        return Ok(());
    };
    let universe = universe_over(s3_client, bucket, chunk, scope).await?;
    let undescribed = chunk.len() - universe.described.len();
    if undescribed > 0 {
        // Left absent so the next pass retries, rather than written from a universe that never
        // included whatever traded only in the sessions the daily archive is missing.
        warn!(
            undescribed,
            %first, %last,
            "Some sessions have no daily partition to screen against; leaving them unwritten"
        );
    }
    if universe.symbols.is_empty() {
        // Nothing to fetch, so nothing in the chunk gets written.
        progress.sessions_without_data += chunk.len();
        return Ok(());
    }
    info!(
        %first,
        %last,
        universe = universe.symbols.len(),
        described = universe.described.len(),
        "Fetching an intraday chunk"
    );

    let mut pending: Vec<Ticker> = universe.symbols.iter().cloned().collect();
    let mut tasks = tokio::task::JoinSet::new();
    let mut bars: Vec<EquityBar> = Vec::new();
    let mut symbols_failed = 0usize;

    loop {
        while tasks.len() < INTRADAY_CONCURRENCY {
            let Some(ticker) = pending.pop() else { break };
            let client = massive.clone();
            let (from, to) = (first.date(), last.date());
            tasks.spawn(async move {
                let mut last_error = None;
                for attempt in 0..INTRADAY_SYMBOL_ATTEMPTS {
                    match client.fetch_intraday(&ticker, interval, from, to).await {
                        Ok(bars) => return Ok(bars),
                        Err(error) => {
                            // A refusal the vendor will repeat is not worth repeating at it. A
                            // survivorship-free universe is full of delisted names, so retrying
                            // every 404 three times scales the waste with the delisted tail.
                            if !is_transient(&error) {
                                return Err((ticker, error));
                            }
                            last_error = Some(error);
                        }
                    }
                    // Backed off, because eight tasks retrying without one turns a vendor throttle
                    // into twenty-four immediate requests at an endpoint already refusing.
                    tokio::time::sleep(retry_delay(attempt)).await;
                }
                Err((
                    ticker,
                    last_error.expect("a failed attempt records its error"),
                ))
            });
        }
        let Some(finished) = tasks.join_next().await else {
            break;
        };
        match finished {
            Ok(Ok(fetched)) => bars.extend(fetched),
            // One symbol's failure costs that symbol, not the chunk. The session stays absent from
            // the archive only if every symbol in it failed, and the next pass requests it again.
            Ok(Err((ticker, error))) => {
                symbols_failed += 1;
                // Named, because this is the one outcome nothing downstream can detect and an
                // operator cannot repair a symbol they have not been told about.
                warn!(%ticker, %error, "A symbol's intraday fetch failed; continuing the chunk");
            }
            Err(error) => {
                symbols_failed += 1;
                warn!(%error, "An intraday fetch task did not complete");
            }
        }
    }

    // Keyed by the bar's own timestamp, not the session requested, so a response answering for a
    // neighbour cannot land under the wrong key. Extended hours are still their own Eastern date.
    let mut by_session: std::collections::BTreeMap<SessionDate, Vec<_>> =
        std::collections::BTreeMap::new();
    for bar in bars {
        by_session
            .entry(SessionDate::at(bar.timestamp()))
            .or_default()
            .push(bar);
    }

    let answered: BTreeSet<SessionDate> = by_session.keys().copied().collect();
    progress.sessions_without_data +=
        count_intraday_sessions_without_data(chunk, &universe.described, &answered);
    if symbols_failed > 0 {
        progress.symbols_failed += symbols_failed;
        // Loud, because the partitions below are about to be written *without* these names and
        // nothing downstream can tell that from a complete one.
        warn!(
            symbols_failed,
            attempts = INTRADAY_SYMBOL_ATTEMPTS,
            %first,
            %last,
            "Some symbols could not be fetched; their bars are absent from this chunk's partitions"
        );
    }

    let mut writable = Vec::new();
    let mut skipped = Vec::new();
    for (session, bars_for_session) in by_session {
        if writable_session(session, scope, &universe.described, present) {
            writable.push((session, bars_for_session));
        } else {
            skipped.push(session);
        }
    }
    if !skipped.is_empty() {
        // These were fetched and are about to be discarded, and they land in no counter, so the
        // summary alone cannot tell an operator a whole-universe pass is still owed for them.
        warn!(
            sessions = ?skipped,
            "Bars were fetched for sessions this pass may not write; run a missing-sessions pass for them"
        );
    }

    write_partitions(s3_client, bucket, interval, writable, progress).await
}

/// Whether this pass may write the partition for `session`.
///
/// An undescribed session is refused whatever the scope, because nothing can say what a complete
/// partition for it would hold. A named set is refused an absent one too, which the chunk fetch can
/// still reach: it spans its chunk's whole range, so bars arrive for sessions never requested.
fn writable_session(
    session: SessionDate,
    scope: &Scope,
    described: &BTreeSet<SessionDate>,
    present: &BTreeSet<SessionDate>,
) -> bool {
    described.contains(&session) && (scope.may_create() || present.contains(&session))
}

/// Partitions written at once.
///
/// Concurrency is safe by construction rather than by scheduling: each write is a compare-and-swap
/// on the object's ETag, so a racing pass is rejected with a `412` and retried instead of being
/// silently clobbered.
const INTRADAY_WRITE_CONCURRENCY: usize = 8;

/// Writes one partition per session, several at a time, folding the outcomes into `summary`.
///
/// Takes bars rather than frames and converts as a slot opens, so the frames alive at once are the
/// ones in flight rather than the whole chunk — converting up front peaked at
/// [`INTRADAY_CHUNK_SESSIONS`] frames beside the bars they were built from.
async fn write_partitions(
    s3_client: &S3Client,
    bucket: &str,
    interval: BarInterval,
    partitions: Vec<(SessionDate, Vec<EquityBar>)>,
    progress: &mut PassProgress,
) -> Result<(), ArchiveError> {
    let mut queued = partitions.into_iter();
    let mut writes = tokio::task::JoinSet::new();

    loop {
        while writes.len() < INTRADAY_WRITE_CONCURRENCY {
            let Some((session, bars_for_session)) = queued.next() else {
                break;
            };
            let frame = bars::bars_to_dataframe(&bars_for_session)?;
            let client = s3_client.clone();
            let bucket = bucket.to_string();
            let rows = frame.height();
            writes.spawn(async move {
                let written = write_partition(&client, &bucket, interval, session, frame).await;
                (session, rows, written)
            });
        }
        let Some(finished) = writes.join_next().await else {
            break;
        };
        match finished {
            Ok((_, rows, Ok(()))) => {
                progress.sessions_written += 1;
                progress.rows_written += rows;
            }
            Ok((session, _, Err(ArchiveError::Contended { key, attempts }))) => {
                warn!(key, attempts, %session, "Partition contended; the next pass will retry it");
                progress.sessions_failed.push(session);
            }
            // Carried for the same reason contention is: one session's write says nothing about the
            // next one's. A fault that breaks writes but not reads is carried too and costs the rest
            // of the pass, which `sessions_failed` reports rather than hides.
            Ok((session, _, Err(error))) => {
                warn!(%error, %session, "Partition write failed; this session was not archived");
                progress.sessions_failed.push(session);
            }
            Err(error) => {
                return Err(ArchiveError::Write {
                    bucket: bucket.to_string(),
                    key: bar_archive_prefix(interval),
                    message: format!("a partition write task did not complete: {error}"),
                })
            }
        }
    }
    Ok(())
}

/// Every name the daily archive holds across `sessions`, unscreened.
///
/// Survivorship-free by construction: the partitions are whole-market and were written on the day,
/// so a name that has since delisted is still there in the sessions it traded. Taking the list from
/// today's market instead would sample only the survivors.
async fn universe_over(
    s3_client: &S3Client,
    bucket: &str,
    sessions: &[SessionDate],
    scope: &Scope,
) -> Result<Universe, ArchiveError> {
    let daily = bar_archive_prefix(BarInterval::OneDay);
    let mut universe = Universe::default();

    for session in sessions {
        let key = date_partitioned_key(&daily, session.date());
        let Some(frame) = read_partition(s3_client, bucket, &key).await? else {
            // The session this chunk would be screened against is absent, so there is no universe
            // for it and no way to call an intraday partition for it complete.
            continue;
        };
        universe.described.insert(*session);
        universe
            .symbols
            .extend(names_from_partition(&scope.names, frame)?);
    }
    Ok(universe)
}

/// The names in one daily partition that clear the liquidity thresholds.
///
/// Notional is the per-row product, not a trailing average, because a partition is one session and
/// there is no window to average over — the runtime screen in [`crate::data::universe`] is the one
/// that smooths.
fn screen_partition(
    frame: DataFrame,
    floor: LiquidityFloor,
) -> Result<BTreeSet<Ticker>, ArchiveError> {
    let screened = frame
        .lazy()
        .filter(
            col("close_price")
                .gt_eq(lit(floor.minimum_close_price()))
                .and(
                    (col("close_price") * col("volume").cast(DataType::Float64))
                        .gt_eq(lit(floor.minimum_dollar_volume())),
                ),
        )
        .select([col("ticker")])
        .collect()?;
    let tickers = screened.column("ticker")?.str()?;
    Ok(tickers
        .into_iter()
        .flatten()
        .filter_map(Ticker::new)
        .collect())
}

/// Sessions scanned at once.
///
/// Two reads a session against S3, and the scan is read-only, so this bounds our own concurrency
/// rather than protecting anything. A full archive sweep is ~2,500 reads and sequential it is
/// twenty minutes of latency for no work.
const SCAN_CONCURRENCY: usize = 16;

/// Which names each intraday partition lacks against the daily universe for its own session.
///
/// The difference [`SessionSelection::Absent`] cannot express: a partition written while one
/// symbol's fetch failed is present, non-empty, and short a name. `floor` is a parameter because
/// this reads and never writes, so scanning wider than the archive was ingested at yields a
/// backfill list rather than a fault.
pub async fn scan_intraday_symbols(
    s3_client: &S3Client,
    bucket: &str,
    interval: BarInterval,
    window_start: SessionDate,
    window_end: SessionDate,
    floor: LiquidityFloor,
) -> Result<SymbolScan, ArchiveError> {
    let sessions = expected_sessions(window_start, window_end);
    info!(
        %window_start,
        %window_end,
        interval = %interval,
        sessions = sessions.len(),
        "Scanning intraday partitions for symbol-level gaps"
    );

    let mut queued = sessions.into_iter();
    let mut scans = tokio::task::JoinSet::new();
    let mut coverage = BTreeMap::new();
    let mut failed = BTreeSet::new();

    loop {
        while scans.len() < SCAN_CONCURRENCY {
            let Some(session) = queued.next() else { break };
            let client = s3_client.clone();
            let bucket = bucket.to_string();
            scans.spawn(async move {
                let coverage = session_coverage(&client, &bucket, interval, session, floor).await;
                (session, coverage)
            });
        }
        let Some(finished) = scans.join_next().await else {
            break;
        };
        match finished {
            Ok((session, Ok(result))) => {
                coverage.insert(session, result);
            }
            // Carried, not fatal. A missing object already reads as `Ok(None)`, so what arrives here
            // is a transient S3 fault, and propagating it would discard every session behind it.
            Ok((session, Err(error))) => {
                warn!(%session, %error, "A session could not be scanned; carrying it as failed");
                failed.insert(session);
            }
            Err(error) => {
                return Err(ArchiveError::Read {
                    bucket: bucket.to_string(),
                    key: bar_archive_prefix(interval),
                    message: format!("a scan task did not complete: {error}"),
                })
            }
        }
    }

    if !failed.is_empty() {
        warn!(
            failed = failed.len(),
            "Some sessions could not be scanned; anything driven by this scan is acting on a partial picture"
        );
    }
    Ok(SymbolScan { coverage, failed })
}

/// One session's coverage: read the daily partition, screen it, and difference the intraday one.
async fn session_coverage(
    s3_client: &S3Client,
    bucket: &str,
    interval: BarInterval,
    session: SessionDate,
    floor: LiquidityFloor,
) -> Result<SessionCoverage, ArchiveError> {
    let daily_key = date_partitioned_key(&bar_archive_prefix(BarInterval::OneDay), session.date());
    let Some(daily) = read_partition(s3_client, bucket, &daily_key).await? else {
        return Ok(SessionCoverage::Undescribed);
    };
    let expected = screen_partition(daily, floor)?;

    let intraday_key = date_partitioned_key(&bar_archive_prefix(interval), session.date());
    let Some(intraday) = read_partition(s3_client, bucket, &intraday_key).await? else {
        return Ok(SessionCoverage::Absent);
    };
    Ok(coverage_of(&expected, &partition_tickers(&intraday)?))
}

/// Classifies a session from the two ticker sets, so the comparison is testable without S3.
///
/// One-directional: a partition holding *more* than its own session screened in is complete, not
/// wrong. The archive unions the screened universe across a chunk, so that is every partition.
fn coverage_of(expected: &BTreeSet<Ticker>, present: &BTreeSet<Ticker>) -> SessionCoverage {
    let missing: BTreeSet<Ticker> = expected.difference(present).cloned().collect();
    if missing.is_empty() {
        SessionCoverage::Complete
    } else {
        SessionCoverage::Partial(missing)
    }
}

/// The distinct tickers a partition holds, unscreened.
///
/// Unparseable names are dropped rather than refused, and that is safe only because
/// [`screen_partition`] builds the other side of the comparison through the same [`Ticker::new`] —
/// anything this cannot read is absent from both sets, so it can never read as a gap.
fn partition_tickers(frame: &DataFrame) -> Result<BTreeSet<Ticker>, ArchiveError> {
    let tickers = frame.column("ticker")?.str()?;
    Ok(tickers
        .into_iter()
        .flatten()
        .filter_map(Ticker::new)
        .collect())
}

/// Whether one session's intraday partition holds every name the daily archive screens in.
///
/// Four states rather than a boolean, because the repairs differ and only the first two are visible
/// to a scan that works by set difference over sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionCoverage {
    /// The daily archive has no partition for this session, so completeness is unanswerable.
    Undescribed,
    /// No intraday partition at all — what [`SessionSelection::Absent`] already finds.
    Absent,
    /// Every screened name is present.
    Complete,
    /// A partition exists and is short these names, which is the hole nothing downstream can see.
    Partial(BTreeSet<Ticker>),
}

/// How many sessions fell into each coverage state.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ScanCounts {
    pub undescribed: usize,
    pub absent: usize,
    pub complete: usize,
    pub partial: usize,
}

/// Per-session coverage across a window, and the symbols a repair pass should be given.
///
/// `failed` is the sessions whose partitions could not be read. They are carried rather than fatal,
/// so one throttled response does not discard a sweep, but a repair driven by a scan that has any
/// is acting on an incomplete picture.
#[derive(Debug, Default)]
pub struct SymbolScan {
    coverage: BTreeMap<SessionDate, SessionCoverage>,
    failed: BTreeSet<SessionDate>,
}

impl SymbolScan {
    pub fn coverage(&self) -> &BTreeMap<SessionDate, SessionCoverage> {
        &self.coverage
    }

    pub fn failed(&self) -> &BTreeSet<SessionDate> {
        &self.failed
    }

    /// Every name missing from at least one partition, which is what [`NameSelection::Named`] takes.
    ///
    /// A union rather than a per-session list because the repair requests its symbols across every
    /// session it touches anyway — a name missing from one costs nothing extra to ask for on the
    /// others, and the merge leaves what is already there untouched.
    pub fn missing_symbols(&self) -> BTreeSet<Ticker> {
        self.coverage
            .values()
            .filter_map(|coverage| match coverage {
                SessionCoverage::Partial(missing) => Some(missing),
                SessionCoverage::Undescribed
                | SessionCoverage::Absent
                | SessionCoverage::Complete => None,
            })
            .flatten()
            .cloned()
            .collect()
    }

    pub fn counts(&self) -> ScanCounts {
        let mut counts = ScanCounts::default();
        for coverage in self.coverage.values() {
            match coverage {
                SessionCoverage::Undescribed => counts.undescribed += 1,
                SessionCoverage::Absent => counts.absent += 1,
                SessionCoverage::Complete => counts.complete += 1,
                SessionCoverage::Partial(_) => counts.partial += 1,
            }
        }
        counts
    }
}

/// The names to fetch, and the sessions the daily archive could actually describe.
///
/// The two travel together because a session the daily archive lacks contributes no names, so an
/// intraday partition written for it would look complete while missing whatever traded only then.
#[derive(Default)]
struct Universe {
    symbols: BTreeSet<Ticker>,
    described: BTreeSet<SessionDate>,
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
    interval: BarInterval,
    session: SessionDate,
    fetched: DataFrame,
) -> Result<(), ArchiveError> {
    let key = date_partitioned_key(&bar_archive_prefix(interval), session.date());
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

/// Root of the quote-summary archive, beside the bars rather than under them.
///
/// `data/equity/bars` is Massive's and this is Alpaca's. One prefix for both would put two vendors'
/// opinions of the same session under one key, and make whichever job ran second the one that
/// mattered.
pub const QUOTE_ARCHIVE_PREFIX: &str = "data/equity/quotes";

/// The archive prefix for one summary cadence, hive-partitioned like the bars.
pub fn quote_archive_prefix(interval: BarInterval) -> String {
    format!("{QUOTE_ARCHIVE_PREFIX}/interval={interval}")
}

/// Names folded at once.
///
/// Eight because the endpoint's throughput is the ceiling rather than ours: measured at roughly
/// 100,000 quotes a second on 2026-08-20, and thirty-two concurrent fetches moved no more than
/// eight did. More concurrency buys only memory, since each fold holds its session's observations.
const QUOTE_CONCURRENCY: usize = 8;

/// Attempts per symbol before a session gives up on it.
///
/// The second line of defence, not the first: [`MarketDataClient::fetch_quotes`] already retries
/// the individual page, so what reaches here has failed a page four times running. Load-bearing for
/// the same reason the intraday one is — nothing downstream can tell a session summarized without
/// one of its names from a complete one.
const QUOTE_SYMBOL_ATTEMPTS: usize = 3;

/// Folds the quoted book across `sessions`, per `scope`.
///
/// Session-major rather than ticker-major, unlike the intraday bar pass: a quote request is already
/// bounded to one session's hours, so there is nothing to gain by holding a chunk of them. That is
/// also why the never-create rule costs nothing here: a session-major pass never fetches outside the
/// sessions it selected, while a bar chunk spans a range and needs [`writable_session`] at the write.
///
/// Regular hours only, taken from the calendar so an early close is 3.5 hours rather than 6.5. The
/// overnight book is an order of magnitude wider and would swamp any session mean it entered.
///
/// `sessions` must already be calendar-filtered, which the returned summary assumes when it reports
/// Where a quote pass gets its ticks.
///
/// Two providers with opposite shapes: Alpaca answers one name at a time and is what a repair wants,
/// while a Massive flat file is whole-market by construction and is the only affordable way to reach
/// five years. The scope decides the universe either way; only the fetching differs.
pub enum QuoteSource<'a> {
    /// One request per name, retried per name.
    PerName(&'a MarketDataClient),
    /// One file per session, folded whole.
    WholeSession(&'a FlatFileClient),
}

impl std::fmt::Display for QuoteSource<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            QuoteSource::PerName(_) => "per-name",
            QuoteSource::WholeSession(_) => "whole-session",
        })
    }
}

/// a session that answered with nothing as a fault rather than as a holiday.
pub async fn archive_quote_sessions(
    s3_client: &S3Client,
    source: &QuoteSource<'_>,
    calendar: &TradingCalendar,
    bucket: &str,
    sessions: &[SessionDate],
    scope: &Scope,
) -> Result<PassSummary, ArchiveError> {
    let (Some(first), Some(last)) = (sessions.first(), sessions.last()) else {
        return Ok(PassProgress::default().into_quote_summary(0));
    };
    let present = present_partitions(
        s3_client,
        bucket,
        &quote_archive_prefix(BarInterval::OneDay),
        *first,
        *last,
    )
    .await?;
    let requested = sessions_for(scope.sessions, sessions, &present);

    info!(
        window_start = %first,
        window_end = %last,
        %scope,
        offered = sessions.len(),
        already_summarized = present.len(),
        requested = requested.len(),
        "Planned a quote pass"
    );
    if let NameSelection::Named(symbols) = &scope.names {
        let unsummarized: Vec<SessionDate> = sessions
            .iter()
            .copied()
            .filter(|session| !present.contains(session))
            .collect();
        if !unsummarized.is_empty() {
            // Dated, not counted: a repair that skips most of its window looks identical to one
            // that had nothing to do, and a count cannot say which sessions to seed.
            warn!(
                sessions = ?unsummarized,
                symbols = symbols.len(),
                "Some offered sessions have no quote partition to repair; seed them first"
            );
        }
    }

    let mut progress = PassProgress {
        sessions_requested: requested.len(),
        ..Default::default()
    };
    // Summed here rather than accumulated through `progress`, which is what lets the published
    // summary carry it per variant without a bar pass holding a counter it can never fill.
    let mut quotes_folded = 0usize;
    for session in requested {
        quotes_folded += archive_quote_session(
            s3_client,
            source,
            calendar,
            bucket,
            session,
            scope,
            &mut progress,
        )
        .await?;
    }

    if progress.symbols_failed > 0 {
        // Warned separately, because this is the outcome nothing re-requests: the partition exists,
        // so the next pass reads the session as present and never looks inside it.
        warn!(
            symbols_failed = progress.symbols_failed,
            "Some symbols are absent from the summaries this pass wrote; re-run those sessions to repair them"
        );
    }
    info!(
        sessions_requested = progress.sessions_requested,
        sessions_written = progress.sessions_written,
        sessions_without_data = progress.sessions_without_data,
        sessions_failed = progress.sessions_failed.len(),
        summaries_written = progress.rows_written,
        symbols_failed = progress.symbols_failed,
        quotes_folded,
        "Quote archive updated"
    );
    Ok(progress.into_quote_summary(quotes_folded))
}

/// One session: derive the universe from the scope, fold every name's book, write both cadences.
///
/// Returns the quotes folded to produce them, which is the pass's real cost and the only figure that
/// travels by return rather than through `progress`.
#[allow(clippy::too_many_arguments)]
async fn archive_quote_session(
    s3_client: &S3Client,
    source: &QuoteSource<'_>,
    calendar: &TradingCalendar,
    bucket: &str,
    session: SessionDate,
    scope: &Scope,
    progress: &mut PassProgress,
) -> Result<usize, ArchiveError> {
    let Some((open, close)) = quotes::trading_hours(calendar, session) else {
        // A date the calendar does not publish. Counted rather than fatal, so one unusable session
        // does not cost the rest of the window.
        progress.sessions_without_data += 1;
        return Ok(0);
    };

    let universe = match &scope.names {
        // The partition is not read at all: it would only say which names are there, and this scope
        // has already answered that.
        NameSelection::Named(symbols) => symbols.clone(),
        NameSelection::WholeMarket | NameSelection::Screened(_) => {
            let key =
                date_partitioned_key(&bar_archive_prefix(BarInterval::OneDay), session.date());
            let Some(daily) = read_partition(s3_client, bucket, &key).await? else {
                // Left unwritten so a later pass retries, rather than summarized against a universe
                // that never included whatever traded only on the session the daily archive lacks.
                warn!(%session, "No daily partition to read a universe from; leaving the session unsummarized");
                progress.sessions_without_data += 1;
                return Ok(0);
            };
            names_from_partition(&scope.names, daily)?
        }
    };
    if universe.is_empty() {
        progress.sessions_without_data += 1;
        return Ok(0);
    }

    info!(%session, %open, %close, %source, universe = universe.len(), "Folding a session's quoted book");
    let (folded, quotes_folded) = match source {
        QuoteSource::PerName(market_data) => {
            fold_universe(market_data, session, open, close, &universe, progress).await
        }
        QuoteSource::WholeSession(flat_files) => {
            match fold_whole_session(flat_files, session, open, close, universe, progress).await {
                Ok(folded) => folded,
                Err(error) => {
                    // The file is the session: nothing partial survives it, so this is the session
                    // failing rather than a name within it.
                    warn!(%session, %error, "A session's flat file could not be folded");
                    progress.sessions_failed.push(session);
                    return Ok(0);
                }
            }
        }
    };
    if folded.is_empty() {
        // The quotes still cost what they cost, so the count is returned even though nothing of
        // this session reaches the archive.
        progress.sessions_without_data += 1;
        return Ok(quotes_folded);
    }
    write_quote_partitions(s3_client, bucket, session, folded, progress).await?;
    Ok(quotes_folded)
}

/// Fans out over the universe, folding each name's session and keeping the summaries and the cost.
async fn fold_universe(
    market_data: &MarketDataClient,
    session: SessionDate,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    universe: &BTreeSet<Ticker>,
    progress: &mut PassProgress,
) -> (Vec<QuoteSummary>, usize) {
    let mut pending: Vec<Ticker> = universe.iter().cloned().collect();
    let mut tasks = tokio::task::JoinSet::new();
    let mut folded: Vec<QuoteSummary> = Vec::new();
    let mut quotes_folded = 0usize;

    loop {
        while tasks.len() < QUOTE_CONCURRENCY {
            let Some(ticker) = pending.pop() else { break };
            let client = market_data.clone();
            tasks.spawn(async move {
                let mut last_error = None;
                for attempt in 0..QUOTE_SYMBOL_ATTEMPTS {
                    match quotes::fold_session(&client, &ticker, session, open, close).await {
                        Ok(folded) => return Ok(folded),
                        Err(error) => {
                            if !error.is_transient() {
                                return Err((ticker, error));
                            }
                            last_error = Some(error);
                        }
                    }
                    tokio::time::sleep(retry_delay(attempt)).await;
                }
                Err((
                    ticker,
                    last_error.expect("a failed attempt records its error"),
                ))
            });
        }
        let Some(finished) = tasks.join_next().await else {
            break;
        };
        match finished {
            Ok(Ok((summaries, fetch))) => {
                quotes_folded += fetch.received;
                folded.extend(summaries);
            }
            // One symbol's failure costs that symbol, not the session — the other names are already
            // fetched, and discarding them would mean paying for them twice.
            Ok(Err((ticker, error))) => {
                progress.symbols_failed += 1;
                warn!(%ticker, %session, %error, "A symbol's quote fetch failed; continuing the session");
            }
            Err(error) => {
                progress.symbols_failed += 1;
                warn!(%error, %session, "A quote fold task did not complete");
            }
        }
    }
    (folded, quotes_folded)
}

/// Folds a whole session out of one flat file, keeping only the names the scope asked for.
///
/// Every fold is held until the file ends rather than released at the ticker switch. Two names a
/// session arrive in two runs, and a released fold cannot take the second: folding the runs apart
/// extends each one's last quote to the close, so their weights overlap and sum past the session.
async fn fold_whole_session(
    flat_files: &FlatFileClient,
    session: SessionDate,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    universe: BTreeSet<Ticker>,
    progress: &mut PassProgress,
) -> Result<(Vec<QuoteSummary>, usize), FlatFileError> {
    let requested = universe.len();
    let fold = quotes::MarketFold::new(session, open, close, universe);
    let (_, fold) = flat_files.fold_quotes(session.date(), fold).await?;

    let quotes_folded = fold.folded();
    let (summaries, resumed) = fold.finish();
    let summarized: BTreeSet<&Ticker> = summaries.iter().map(QuoteSummary::ticker).collect();
    // A name the file never carried, which the per-name path would have recorded as a failed fetch.
    progress.symbols_failed += requested.saturating_sub(summarized.len());
    if !resumed.is_empty() {
        info!(%session, resumed = resumed.len(), "Names whose rows arrived in more than one run");
    }
    Ok((summaries, quotes_folded))
}

/// Writes a session's summaries, one partition per cadence, five-minute first.
///
/// The order is the recovery rule, not a preference. Presence is read off the daily prefix, so a
/// pass that dies between the two writes leaves the session looking absent and the next pass redoes
/// both — where the reverse order would mark it done with its intraday half missing.
async fn write_quote_partitions(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
    folded: Vec<QuoteSummary>,
    progress: &mut PassProgress,
) -> Result<(), ArchiveError> {
    let mut intraday: Vec<QuoteSummary> = Vec::new();
    let mut daily: Vec<QuoteSummary> = Vec::new();
    let mut unexpected = 0usize;
    for row in folded {
        match row.bar_interval() {
            BarInterval::FiveMinute => intraday.push(row),
            BarInterval::OneDay => daily.push(row),
            // The fold emits exactly the two cadences above; a third is this module's own bug.
            BarInterval::OneMinute => unexpected += 1,
        }
    }
    if unexpected > 0 {
        warn!(unexpected, %session, "Discarded quote summaries at a cadence the archive has no prefix for");
    }

    let mut written = 0usize;
    for (interval, rows) in [
        (BarInterval::FiveMinute, intraday),
        (BarInterval::OneDay, daily),
    ] {
        if rows.is_empty() {
            continue;
        }
        let frame = quotes::summaries_to_dataframe(&rows)?;
        let key = date_partitioned_key(&quote_archive_prefix(interval), session.date());
        match write_merged(s3_client, bucket, key, frame, |existing, fetched, key| {
            merge_or_replace(existing, fetched, key)
        })
        .await
        {
            Ok(()) => written += rows.len(),
            // Both arms leave the session unsummarized at this cadence. A scope that skips sessions
            // already present will not return to it, so re-running is the operator's to decide.
            Err(ArchiveError::Contended { key, attempts }) => {
                warn!(key, attempts, %session, "Quote partition contended; this session was not summarized");
                progress.sessions_failed.push(session);
                return Ok(());
            }
            Err(error) => {
                warn!(%error, %session, "Quote partition write failed; this session was not summarized");
                progress.sessions_failed.push(session);
                return Ok(());
            }
        }
    }

    progress.sessions_written += 1;
    progress.rows_written += written;
    Ok(())
}

/// Fetches the whole splits table and writes it, keeping each row's earliest `first_seen`.
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
        .map_err(|error| ArchiveError::Feed {
            vendor: "Massive",
            message: error.to_string(),
        })?;

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

/// Refreshes the series-boundary table over `start..=end`, merging it with what is stored.
///
/// Windowed rather than whole, unlike [`archive_splits`]: the endpoint has no all-time form, so
/// rows outside the range are left as they were. An empty fetch is written rather than refused,
/// because a range with no corporate action in it is an ordinary answer here.
pub async fn archive_boundaries(
    s3_client: &S3Client,
    market_data: &MarketDataClient,
    bucket: &str,
    start: SessionDate,
    end: SessionDate,
    fetched_at: DateTime<Utc>,
) -> Result<usize, ArchiveError> {
    let fetched = market_data
        .fetch_corporate_actions(start.date(), end.date())
        .await
        .map_err(|error| ArchiveError::Feed {
            vendor: "Alpaca",
            message: error.to_string(),
        })?;

    let frame = boundaries::boundaries_to_dataframe(&fetched, fetched_at)?;
    write_merged(
        s3_client,
        bucket,
        BOUNDARIES_ARCHIVE_KEY.to_string(),
        frame,
        // Fails rather than falling back, which is the opposite of `archive_splits`. Replacing with
        // the fetch would drop every boundary outside the window, and quietly keeping the stored
        // table would report a refresh that did not happen — a schema mismatch would then look like
        // success on every run forever. Nothing is written, so the stored table survives either way.
        |existing, fetched, key| {
            boundaries::merge_boundaries(existing, fetched, start, end).map_err(|error| {
                ArchiveError::Frame(PolarsError::ComputeError(
                    format!("could not merge the stored boundary table at {key}: {error}").into(),
                ))
            })
        },
    )
    .await?;

    info!(
        key = BOUNDARIES_ARCHIVE_KEY,
        boundaries = fetched.len(),
        %start,
        %end,
        "Boundary table archived"
    );
    Ok(fetched.len())
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
    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;
    use chrono::NaiveDate;
    use percent_encoding::percent_decode_str;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::from_ymd_opt(year, month, day).expect("test date must be valid"),
        )
    }

    /// An S3 client answering from `respond` instead of the network, given the method and the key.
    ///
    /// Dispatched on the request rather than replayed in order, because `write_partitions` runs
    /// [`INTRADAY_WRITE_CONCURRENCY`] writes at once and an ordered script would race them.
    fn scripted_s3_client(
        respond: impl Fn(&http::Method, &str) -> http::Response<SdkBody> + Send + Sync + 'static,
    ) -> S3Client {
        let http_client = infallible_client_fn(move |request| {
            let method = request.method().clone();
            // Decoded rather than matched against: S3 percent-encodes the `=` in every hive
            // segment, so a key built by `date_partitioned_key` never matches the wire form.
            let key = percent_decode_str(request.uri().path()).decode_utf8_lossy();
            respond(&method, &key)
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

    /// Absent, so `write_merged` takes its `Precondition::Absent` path and writes without a merge.
    fn no_such_key() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(404)
            .body(SdkBody::from(
                "<Error><Code>NoSuchKey</Code><Message>absent</Message></Error>",
            ))
            .expect("a canned response must build")
    }

    fn one_bar(ticker_symbol: &str, at: SessionDate) -> EquityBar {
        EquityBar::new(
            Ticker::new(ticker_symbol).expect("a test ticker must parse"),
            BarInterval::FiveMinute,
            at.midnight(),
            100.0,
            101.0,
            99.0,
            100.5,
            1_000,
            None,
            None,
        )
        .expect("a coherent candle must construct")
    }

    /// One session's write failing must cost that session and no other.
    ///
    /// The regression test #1106 could not have: before it, any non-contention error returned from
    /// `write_partitions`, so a single transient fault discarded every session after it — which is
    /// what ended a five-hour whole-market pass four fifths of the way through.
    #[tokio::test]
    async fn test_a_failed_partition_write_costs_only_its_own_session() {
        let doomed = session(2026, 9, 3);
        let doomed_key =
            date_partitioned_key(&bar_archive_prefix(BarInterval::FiveMinute), doomed.date());
        let client = scripted_s3_client(move |method, key| {
            if method == http::Method::PUT && key.ends_with(&doomed_key) {
                http::Response::builder()
                    .status(500)
                    .body(SdkBody::from("<Error><Code>InternalError</Code></Error>"))
                    .expect("a canned response must build")
            } else if method == http::Method::PUT {
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .expect("a canned response must build")
            } else {
                no_such_key()
            }
        });

        // More partitions than [`INTRADAY_WRITE_CONCURRENCY`], so work is still queued when the
        // failure lands — a defect that stopped scheduling the rest would leave the count short.
        let partitions: Vec<(SessionDate, Vec<EquityBar>)> = (1..=20)
            .map(|day| session(2026, 9, day))
            .map(|at| (at, vec![one_bar("AAPL", at)]))
            .collect();

        let mut progress = PassProgress::default();
        let outcome = write_partitions(
            &client,
            "test-bucket",
            BarInterval::FiveMinute,
            partitions,
            &mut progress,
        )
        .await;

        assert!(outcome.is_ok(), "one bad session must not end the pass");
        assert_eq!(progress.sessions_written, 19);
        // The literal, not `doomed`: an expectation carried from the fixture moves with it, so
        // relocating the scripted failure would keep this passing without meaning to.
        assert_eq!(progress.sessions_failed, vec![session(2026, 9, 3)]);
    }

    /// The stored layout, pinned to literals. Everything already written lives at these keys, and a
    /// silent change to either the segment name or its position orphans the whole archive — the gap
    /// scan would report every session missing and refetch five years over an intact bucket.
    #[test]
    fn test_the_partition_key_carries_its_cadence() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 19).expect("test date must be valid");

        assert_eq!(
            date_partitioned_key(&bar_archive_prefix(BarInterval::OneDay), date),
            "data/equity/bars/interval=one_day/year=2026/month=08/day=19/data.parquet"
        );
        assert_eq!(
            date_partitioned_key(&bar_archive_prefix(BarInterval::OneMinute), date),
            "data/equity/bars/interval=one_minute/year=2026/month=08/day=19/data.parquet"
        );
        assert_eq!(
            date_partitioned_key(&bar_archive_prefix(BarInterval::FiveMinute), date),
            "data/equity/bars/interval=five_minute/year=2026/month=08/day=19/data.parquet"
        );
    }

    /// Quotes are Alpaca's opinion and bars are Massive's. A shared key would put two vendors'
    /// accounts of one session at one address, and the date inverse must still read the tail so a
    /// listing recovers the session.
    #[test]
    fn test_quote_partitions_sit_beside_the_bars_rather_than_among_them() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 19).expect("test date must be valid");

        assert_eq!(
            date_partitioned_key(&quote_archive_prefix(BarInterval::OneDay), date),
            "data/equity/quotes/interval=one_day/year=2026/month=08/day=19/data.parquet"
        );
        assert_eq!(
            date_partitioned_key(&quote_archive_prefix(BarInterval::FiveMinute), date),
            "data/equity/quotes/interval=five_minute/year=2026/month=08/day=19/data.parquet"
        );
        for interval in BarInterval::ALL {
            let quotes = date_partitioned_key(&quote_archive_prefix(interval), date);
            assert_ne!(
                quotes,
                date_partitioned_key(&bar_archive_prefix(interval), date)
            );
            assert_eq!(date_from_partitioned_key(&quotes), Some(date));
        }
    }

    /// Each fault fails the pass on its own, and a clean pass passes. This is the rule all three
    /// binaries' exit codes now derive from, and deciding it per binary was wrong twice.
    #[test]
    fn test_each_fault_alone_makes_a_pass_incomplete() {
        let clean = PassProgress::default().into_bar_summary(None);
        assert!(clean.is_complete());

        let failed_session = PassProgress {
            sessions_failed: vec![session(2026, 8, 19)],
            ..Default::default()
        }
        .into_bar_summary(None);
        assert!(!failed_session.is_complete(), "a failed session is a fault");

        let failed_symbol = PassProgress {
            symbols_failed: 1,
            ..Default::default()
        }
        .into_bar_summary(None);
        assert!(
            !failed_symbol.is_complete(),
            "a symbol missing from a written partition is a fault nothing downstream can see"
        );
    }

    /// The bit that has been wrong twice, in both directions: bars exited 0 on failure and quotes
    /// counted holidays as failure. The rule follows the calendar the pass actually held, so it
    /// cannot claim a filtering it never did.
    #[test]
    fn test_an_empty_session_is_a_fault_only_when_a_calendar_filtered_the_window() {
        let counts = || PassProgress {
            sessions_requested: 5,
            sessions_written: 4,
            sessions_without_data: 1,
            ..Default::default()
        };
        let calendar = calendar_of(
            &[session(2026, 8, 17)],
            session(2026, 8, 17),
            session(2026, 8, 21),
        );

        assert!(
            counts().into_bar_summary(None).is_complete(),
            "with no calendar the list still holds holidays, which answer empty forever"
        );
        assert!(
            !counts().into_bar_summary(Some(&calendar)).is_complete(),
            "a calendar-filtered window has no holiday left to explain an empty answer"
        );
        assert!(
            !counts().into_quote_summary(0).is_complete(),
            "the quote pass is always calendar-filtered"
        );
    }

    /// Holidays are what the weekday sweep cannot exclude on its own, and requesting them is what
    /// made `sessions_without_data` ambiguous. 2026-07-03 is the observed Independence Day.
    #[test]
    fn test_a_calendar_drops_the_holidays_a_weekday_sweep_keeps() {
        let (start, end) = (session(2026, 7, 1), session(2026, 7, 6));

        assert_eq!(
            sessions_in_window(start, end, None).unwrap(),
            vec![
                session(2026, 7, 1),
                session(2026, 7, 2),
                session(2026, 7, 3),
                session(2026, 7, 6),
            ],
            "the weekday sweep keeps the holiday and drops only the weekend"
        );

        let calendar = calendar_of(
            &[
                session(2026, 7, 1),
                session(2026, 7, 2),
                session(2026, 7, 6),
            ],
            start,
            end,
        );
        assert_eq!(
            sessions_in_window(start, end, Some(&calendar)).unwrap(),
            vec![
                session(2026, 7, 1),
                session(2026, 7, 2),
                session(2026, 7, 6)
            ],
            "the calendar drops 07-03, which would answer empty forever"
        );
    }

    /// The hole the first version of this only half-covered: it warned when the filter left nothing
    /// at all, which misses the truncation that matters. A calendar short at either end drops real
    /// sessions, shrinks `sessions_requested`, and leaves `is_complete()` reporting a clean run over
    /// a window it never saw — the one failure the summary itself cannot show.
    #[test]
    fn test_a_calendar_that_does_not_span_the_window_is_refused() {
        let (start, end) = (session(2026, 7, 1), session(2026, 7, 31));
        let trading = [session(2026, 7, 1), session(2026, 7, 2)];

        for (covered_start, covered_end, why) in [
            (session(2026, 7, 6), end, "short at the start"),
            (start, session(2026, 7, 20), "short at the end"),
        ] {
            let calendar = calendar_of(&trading, covered_start, covered_end);
            assert!(
                matches!(
                    sessions_in_window(start, end, Some(&calendar)).unwrap_err(),
                    ArchiveError::Calendar { start: refused_start, end: refused_end }
                        if refused_start == start && refused_end == end
                ),
                "a calendar {why} must be refused, not silently narrow the request"
            );
        }

        let spanning = calendar_of(&trading, start, end);
        assert!(sessions_in_window(start, end, Some(&spanning)).is_ok());
    }

    /// A calendar built without recording its range cannot prove it covers anything, so it is
    /// refused rather than trusted — the same conservatism `is_trading_day` already applies outside
    /// its horizon, where it answers `false` rather than guessing.
    #[test]
    fn test_a_calendar_of_unknown_range_covers_nothing() {
        let (start, end) = (session(2026, 7, 1), session(2026, 7, 2));
        let unknown = TradingCalendar::from_days(vec![]);

        assert!(!unknown.covers(start, end));
        assert!(matches!(
            sessions_in_window(start, end, Some(&unknown)).unwrap_err(),
            ArchiveError::Calendar { .. }
        ));
    }

    /// Every count has to survive the accumulator becoming a summary. Both finishers copy field by
    /// field, so a transposed pair would be silent — the counts would still be plausible, just
    /// attached to the wrong name.
    #[test]
    fn test_the_finish_carries_every_count_through() {
        let progress = || PassProgress {
            sessions_requested: 11,
            sessions_written: 7,
            sessions_without_data: 3,
            sessions_failed: vec![session(2026, 8, 19)],
            symbols_failed: 5,
            rows_written: 2_048,
        };

        for summary in [
            progress().into_bar_summary(None),
            progress().into_quote_summary(0),
        ] {
            assert_eq!(summary.sessions_requested(), 11);
            assert_eq!(summary.sessions_written(), 7);
            assert_eq!(summary.sessions_without_data(), 3);
            assert_eq!(summary.sessions_failed(), [session(2026, 8, 19)]);
            assert_eq!(summary.symbols_failed(), 5);
            assert_eq!(summary.output().rows_written(), 2_048);
        }
    }

    /// The tail is what the two passes disagree about, so the rendered line has to differ exactly
    /// there and nowhere else. A bar pass printing a folded-quote count it can never fill was the
    /// hole the per-variant output exists to close, and rendering is the last place it could reopen.
    #[test]
    fn test_the_rendered_line_names_what_the_pass_actually_wrote() {
        let progress = || PassProgress {
            sessions_requested: 11,
            sessions_written: 7,
            sessions_without_data: 3,
            sessions_failed: vec![session(2026, 8, 19)],
            symbols_failed: 5,
            rows_written: 2_048,
        };
        let shared = "requested 11 sessions, wrote 7, 3 without data, 1 failed, 5 symbols missing";

        assert_eq!(
            progress().into_bar_summary(None).to_string(),
            format!("{shared}, 2048 bars")
        );
        assert_eq!(
            progress().into_quote_summary(412_000).to_string(),
            format!("{shared}, 2048 summaries, 412000 quotes folded")
        );
    }

    /// The one figure that travels by return rather than through the accumulator, which is what
    /// lets a bar pass hold no counter it could never fill.
    #[test]
    fn test_a_quote_pass_carries_what_it_cost() {
        let summary = PassProgress {
            rows_written: 158,
            ..Default::default()
        }
        .into_quote_summary(412_000);

        assert_eq!(
            summary.output,
            PassOutput::Quotes {
                summaries_written: 158,
                quotes_folded: 412_000
            }
        );
        assert_eq!(
            summary.output.rows_written(),
            158,
            "the shared accessor reads the rows, not the ticks folded to get them"
        );
    }

    /// The rule the product exists to enforce, and the reason "may it create" is not a third axis.
    /// A named pass fetches only its own names, so a session it answered where no partition existed
    /// would hold that handful and read as complete to every later pass.
    #[test]
    fn test_a_named_set_may_only_touch_sessions_that_already_exist() {
        for refused in [SessionSelection::Absent, SessionSelection::Every] {
            assert_eq!(
                Scope::new(NameSelection::Named(tickers(&["AAPL"])), refused),
                Err(ScopeError::NamedSetWouldCreate),
                "{refused:?} would let a named set answer a session nothing has written"
            );
        }
        assert!(Scope::new(
            NameSelection::Named(tickers(&["AAPL"])),
            SessionSelection::Present
        )
        .is_ok());

        // The other two derive their universe from the daily partition, so every pairing is a pass.
        for names in [
            NameSelection::WholeMarket,
            NameSelection::Screened(LiquidityFloor::CURRENT),
        ] {
            for sessions in [
                SessionSelection::Absent,
                SessionSelection::Present,
                SessionSelection::Every,
            ] {
                assert!(Scope::new(names.clone(), sessions).is_ok(), "{names:?}");
            }
        }
    }

    /// Both binaries log the scope through this rather than each phrasing it, and the elision is
    /// the part that can be wrong: a scan-driven repair names thousands, and the whole list once
    /// produced a multi-kilobyte line that buried the fields beside it.
    #[test]
    fn test_a_scope_says_which_names_across_which_sessions() {
        assert_eq!(
            scope(NameSelection::WholeMarket, SessionSelection::Absent).to_string(),
            "every name, absent sessions only"
        );
        assert_eq!(
            scope(
                NameSelection::Screened(LiquidityFloor::CURRENT),
                SessionSelection::Every
            )
            .to_string(),
            "the screened universe ($10 close on $50000000 traded), every session"
        );
        assert_eq!(
            scope(
                NameSelection::Named(tickers(&["CBOE", "AAPL"])),
                SessionSelection::Present
            )
            .to_string(),
            "AAPL,CBOE, sessions already present"
        );

        // Thirteen names, so exactly one is elided. The literal, not the bound: an expectation read
        // off the constant it is checking moves with it and can never fail.
        let many = tickers(&[
            "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM",
        ]);
        assert_eq!(
            scope(NameSelection::Named(many), SessionSelection::Present).to_string(),
            "AA,AB,AC,AD,AE,AF,AG,AH,AI,AJ,AK,AL and 1 more, sessions already present"
        );
    }

    /// A whole-market pass exists to widen partitions that already exist, so unlike the other two
    /// selections it filters on nothing. Skipping the present ones would make it a no-op against a
    /// seeded archive, which is exactly the state it is run against.
    #[test]
    fn test_every_session_selection_filters_on_nothing() {
        let sessions = [
            session(2026, 8, 17),
            session(2026, 8, 18),
            session(2026, 8, 19),
        ];
        let present: BTreeSet<SessionDate> = [session(2026, 8, 18)].into_iter().collect();

        // The literals, not `sessions`: an expectation read off the fixture under test moves with
        // it, so a fourth session added here would keep passing without meaning to.
        assert_eq!(
            sessions_for(SessionSelection::Every, &sessions, &present),
            vec![
                session(2026, 8, 17),
                session(2026, 8, 18),
                session(2026, 8, 19)
            ]
        );
    }

    /// Both passes now read this off one function, where each carried its own answer and the bar
    /// side's was a third one — request everything, refuse at the write — which fetched sessions it
    /// was never allowed to keep. The sets are opposite because a partition holding some of its
    /// names is indistinguishable from a complete one, so the difference that repairs a *session*
    /// sees nothing to do; that is what left CBOE absent from 202 partitions that all existed.
    #[test]
    fn test_seeding_and_repairing_take_opposite_session_sets() {
        let sessions = [
            session(2026, 8, 17),
            session(2026, 8, 18),
            session(2026, 8, 19),
        ];
        let present: BTreeSet<SessionDate> = [session(2026, 8, 18)].into_iter().collect();

        assert_eq!(
            sessions_for(SessionSelection::Absent, &sessions, &present),
            vec![session(2026, 8, 17), session(2026, 8, 19)],
            "seeding takes the sessions with nothing in them"
        );
        assert_eq!(
            sessions_for(SessionSelection::Present, &sessions, &present),
            vec![session(2026, 8, 18)],
            "repairing takes exactly the sessions that already have one"
        );
    }

    /// The same asymmetry at the bar write, which is where it still has to be enforced: a chunk is
    /// fetched over its whole range, so bars arrive for sessions the selection never requested.
    #[test]
    fn test_a_whole_market_bar_pass_may_create_a_partition_where_a_repair_may_not() {
        let described: BTreeSet<SessionDate> = [session(2026, 8, 19)].into_iter().collect();
        let absent = BTreeSet::new();
        let whole_market = scope(NameSelection::WholeMarket, SessionSelection::Every);

        assert!(writable_session(
            session(2026, 8, 19),
            &whole_market,
            &described,
            &absent
        ));
        assert!(!writable_session(
            session(2026, 8, 19),
            &scope(
                NameSelection::Named(tickers(&["AAPL"])),
                SessionSelection::Present
            ),
            &described,
            &absent
        ));
        // Undescribed refuses both: nothing can say what a complete partition would hold.
        assert!(!writable_session(
            session(2026, 8, 20),
            &whole_market,
            &described,
            &absent
        ));
    }

    /// Two cadences of one session must not collide, which is the whole reason the segment exists.
    /// A shared key would make whichever job wrote last the one that mattered.
    #[test]
    fn test_two_cadences_of_one_session_do_not_share_a_key() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 19).expect("test date must be valid");
        let keys: std::collections::BTreeSet<String> = BarInterval::ALL
            .iter()
            .map(|interval| date_partitioned_key(&bar_archive_prefix(*interval), date))
            .collect();

        // Three, the cadences that exist today. Pinned rather than taken from `BarInterval::ALL`,
        // so adding a variant has to come here and say what its key is rather than passing
        // silently — which is what caught `five_minute` arriving.
        assert_eq!(keys.len(), 3);
    }

    /// The cadence segment sits before the date partition, so the date inverse still reads the tail
    /// of the key. Without this the gap scan cannot recover a session from a listing.
    #[test]
    fn test_the_cadence_segment_does_not_break_the_date_inverse() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 19).expect("test date must be valid");
        for interval in BarInterval::ALL {
            let key = date_partitioned_key(&bar_archive_prefix(interval), date);
            assert_eq!(date_from_partitioned_key(&key), Some(date), "key: {key}");
        }
    }

    /// The intraday pass deliberately has no correction window, unlike the daily one. A daily bar
    /// gets restated after the close; an intraday bar does not, and re-requesting a month to learn
    /// that would cost the whole universe in requests rather than one grouped call.
    #[test]
    fn test_the_intraday_scan_requests_only_what_is_absent() {
        let expected = expected_sessions(session(2026, 6, 1), session(2026, 6, 5));
        // The two most recent sessions are held, which is where the daily correction window bites.
        let present: BTreeSet<SessionDate> = [session(2026, 6, 4), session(2026, 6, 5)]
            .into_iter()
            .collect();

        let requested = sessions_for(SessionSelection::Absent, &expected, &present);
        assert_eq!(
            requested,
            vec![
                session(2026, 6, 1),
                session(2026, 6, 2),
                session(2026, 6, 3)
            ],
            "nothing already held is re-requested"
        );

        // The daily path re-requests the two held sessions on top, to pick up restatements.
        let daily = sessions_to_request(&expected, &present);
        assert_eq!(daily.len(), 5, "{daily:?}");
        assert!(daily.contains(&session(2026, 6, 5)), "{daily:?}");
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

    /// A session with no daily universe to screen against is skipped before any fetch, so it is
    /// also unanswered. Counting both reasons inflated the five-year backfill's figure to exactly
    /// twice the sessions it left unwritten.
    #[test]
    fn test_a_session_missing_its_universe_is_counted_once_not_twice() {
        let chunk = vec![
            session(2026, 6, 1),
            session(2026, 6, 2),
            session(2026, 6, 3),
        ];
        // 06-01 has no daily partition, so it is never fetched and never answers.
        let described: BTreeSet<SessionDate> = [session(2026, 6, 2), session(2026, 6, 3)].into();
        let answered: BTreeSet<SessionDate> = [session(2026, 6, 2)].into();

        let without_data = count_intraday_sessions_without_data(&chunk, &described, &answered);

        assert_eq!(
            without_data, 2,
            "06-01 lacks a universe and 06-03 lacks bars; neither may be counted twice"
        );
        // One written session (06-02) plus the two above accounts for the whole chunk.
        assert_eq!(without_data + 1, 3);
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

    /// The intraday repair universe is screened on notional, at the three names' measured figures.
    /// CBOE moves 900,000 shares and is in it; OBDC moves nearly five times as many and is not. The
    /// retired share-count floor answered both the other way round, and OBDC is the one the archive
    /// spent its five-minute fetches on.
    #[test]
    fn test_the_partition_screen_counts_dollars_not_shares() {
        let partition = df![
            "ticker" => ["CBOE", "OBDC", "SNDL"],
            "close_price" => [290.0_f64, 11.3, 1.50],
            "volume" => [900_000_i64, 4_350_000, 40_000_000],
        ]
        .unwrap();

        let screened =
            screen_partition(partition, LiquidityFloor::new(10.0, 50_000_000.0).unwrap()).unwrap();

        assert_eq!(
            screened,
            BTreeSet::from([Ticker::new("CBOE").unwrap()]),
            "OBDC turns over $49.2M, just under the floor; SNDL clears $60M and fails on price"
        );
    }

    /// The bug this cost a backfill to find. A symbol repair fetches only the named symbols, so
    /// writing where no partition existed leaves one holding those names and nothing else — which
    /// then reads as present and is never filled. Repairing 2026-08-21 turned an absent session into
    /// one short 1,335 names.
    #[test]
    fn test_a_symbol_repair_never_creates_a_partition() {
        let session = session(2026, 8, 21);
        let described = BTreeSet::from([session]);
        let absent = BTreeSet::new();
        let repair = scope(
            NameSelection::Named(tickers(&["CBOE", "NVDA"])),
            SessionSelection::Present,
        );

        assert!(
            !writable_session(session, &repair, &described, &absent),
            "a repair must leave an absent session absent, so the session scan still fetches it"
        );
        assert!(
            writable_session(
                session,
                &scope(NameSelection::WholeMarket, SessionSelection::Absent),
                &described,
                &absent
            ),
            "the whole-universe pass is the one that may create it"
        );
    }

    /// The repair still writes where a partition exists — that is the merge it is for.
    #[test]
    fn test_a_symbol_repair_writes_into_a_partition_that_exists() {
        let session = session(2026, 8, 20);
        let described = BTreeSet::from([session]);
        let present = BTreeSet::from([session]);
        let repair = scope(
            NameSelection::Named(tickers(&["CBOE"])),
            SessionSelection::Present,
        );

        assert!(writable_session(session, &repair, &described, &present));
    }

    /// An undescribed session is refused whatever the scope, because nothing can say what a
    /// complete partition for it would hold.
    #[test]
    fn test_an_undescribed_session_is_never_written() {
        let session = session(2026, 8, 20);
        let described = BTreeSet::new();
        let present = BTreeSet::from([session]);

        assert!(!writable_session(
            session,
            &scope(NameSelection::WholeMarket, SessionSelection::Absent),
            &described,
            &present
        ));
        assert!(!writable_session(
            session,
            &scope(
                NameSelection::Named(tickers(&["CBOE"])),
                SessionSelection::Present
            ),
            &described,
            &present
        ));
    }

    fn scan_of(entries: &[(SessionDate, SessionCoverage)]) -> SymbolScan {
        SymbolScan {
            coverage: entries.iter().cloned().collect(),
            failed: BTreeSet::new(),
        }
    }

    fn tickers(names: &[&str]) -> BTreeSet<Ticker> {
        names
            .iter()
            .map(|name| Ticker::new(name).expect("a valid test ticker"))
            .collect()
    }

    fn scope(names: NameSelection, sessions: SessionSelection) -> Scope {
        Scope::new(names, sessions).expect("a valid test scope")
    }

    /// A calendar publishing exactly these sessions, at the usual bell, over `[start, end]`.
    fn calendar_of(
        sessions: &[SessionDate],
        start: SessionDate,
        end: SessionDate,
    ) -> TradingCalendar {
        TradingCalendar::covering(
            sessions
                .iter()
                .map(|at| {
                    crate::common::alpaca::CalendarDay::new(
                        at.date(),
                        chrono::NaiveTime::from_hms_opt(9, 30, 0).expect("a valid open"),
                        chrono::NaiveTime::from_hms_opt(16, 0, 0).expect("a valid close"),
                    )
                    .expect("a session with duration")
                })
                .collect(),
            start,
            end,
        )
    }

    /// The whole point of the task: a partition that exists and is short two names reads as
    /// `Partial`, where the session-level scan sees only that a partition is there.
    #[test]
    fn test_a_partition_short_two_names_is_partial_not_complete() {
        let coverage = coverage_of(
            &tickers(&["AAPL", "CBOE", "MSFT", "NVDA"]),
            &tickers(&["AAPL", "MSFT"]),
        );

        assert_eq!(
            coverage,
            SessionCoverage::Partial(tickers(&["CBOE", "NVDA"]))
        );
    }

    /// An extra name is not a gap. The archive over-fetches by unioning the screened universe
    /// across a chunk, so every partition holds names its own session did not screen in — treating
    /// that as a difference in either direction would report the whole archive as broken.
    #[test]
    fn test_a_partition_holding_more_than_the_screen_expects_is_complete() {
        let coverage = coverage_of(&tickers(&["AAPL"]), &tickers(&["AAPL", "MSFT", "NVDA"]));

        assert_eq!(coverage, SessionCoverage::Complete);
    }

    /// A repair is given the union across the window, and only from partitions that exist. An
    /// absent session contributes nothing: it is repaired by fetching the session, not the symbol.
    #[test]
    fn test_missing_symbols_unions_partials_and_ignores_every_other_state() {
        let scan = scan_of(&[
            (session(2026, 8, 17), SessionCoverage::Undescribed),
            (session(2026, 8, 18), SessionCoverage::Absent),
            (session(2026, 8, 19), SessionCoverage::Complete),
            (
                session(2026, 8, 20),
                SessionCoverage::Partial(tickers(&["CBOE", "NVDA"])),
            ),
            (
                session(2026, 8, 21),
                SessionCoverage::Partial(tickers(&["NVDA", "TW"])),
            ),
        ]);

        assert_eq!(scan.missing_symbols(), tickers(&["CBOE", "NVDA", "TW"]));
        assert_eq!(
            scan.counts(),
            ScanCounts {
                undescribed: 1,
                absent: 1,
                complete: 1,
                partial: 2,
            }
        );
    }

    /// A clean archive asks for no repair at all, so the pass it would drive is skipped rather than
    /// requesting every session in the window for an empty symbol set.
    #[test]
    fn test_a_clean_scan_names_no_symbols() {
        let scan = scan_of(&[
            (session(2026, 8, 20), SessionCoverage::Complete),
            (session(2026, 8, 21), SessionCoverage::Complete),
        ]);

        assert!(scan.missing_symbols().is_empty());
        assert_eq!(scan.counts().complete, 2);
    }

    /// `Absent` and `Partial` must stay distinguishable: the first is a session the existing scan
    /// already repairs, the second is one it reports as complete forever.
    #[test]
    fn test_an_absent_partition_is_not_an_empty_partial() {
        let scan = scan_of(&[
            (session(2026, 8, 20), SessionCoverage::Absent),
            (
                session(2026, 8, 21),
                SessionCoverage::Partial(tickers(&["CBOE"])),
            ),
        ]);

        let counts = scan.counts();
        assert_eq!(counts.absent, 1);
        assert_eq!(counts.partial, 1);
        assert_eq!(scan.missing_symbols(), tickers(&["CBOE"]));
    }

    #[test]
    fn test_partition_tickers_reads_distinct_names_without_screening_them() {
        let partition = df![
            "ticker" => ["CBOE", "CBOE", "OBDC", "SNDL"],
            "close_price" => [290.0_f64, 291.0, 11.3, 1.50],
            "volume" => [900_000_i64, 800_000, 4_350_000, 40_000_000],
        ]
        .unwrap();

        let held = partition_tickers(&partition).unwrap();

        assert_eq!(
            held,
            tickers(&["CBOE", "OBDC", "SNDL"]),
            "the screen belongs to the daily side of the comparison, not this one"
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
