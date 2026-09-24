//! The S3 bar archive: the trainer's data, repaired to a window rather than topped up by a night.
//!
//! One partition per session and cadence under `data/derived/equity/bars/interval=/year=/month=/day=/`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use tracing::{info, warn};

use crate::common::alpaca::MarketDataClient;
use crate::common::aws::{date_from_partitioned_key, date_partitioned_key};
use crate::common::edgar;
use crate::common::flatfiles::{FlatFileClient, FlatFileError, RawDataset};
use crate::common::massive::MassiveClient;
use crate::common::provenance::{
    AlpacaPlan, MassivePlan, MassiveTransport, PartitionProvenance, Provenance,
};
use crate::common::types::{
    BarInterval, Cik, EquityBar, EquityReference, IntradayCadence, LiquidityFloor, QuoteSummary,
    SecurityType, SessionDate, SicCode, Ticker, TradeSummary,
};
use crate::data::attribution::{Attribution, Declaration};
use crate::data::cadence::{CadenceCheck, CadenceError, CadenceTotals, SessionOutcome};
use crate::data::calendar::TradingCalendar;
use crate::data::classification::ClassificationTable;
use crate::data::conditions::ConditionsTable;
use crate::data::industry_codes::{IndustryCode, IndustryCodesTable};
use crate::data::{bars, boundaries, quotes, reference, splits, trades};

/// Root of the bar archive, never a partition prefix on its own — [`bar_archive_prefix`] adds the
/// cadence, and a key built without one collides with every other cadence of the same session.
///
/// Deliberately not under `exports/`, which is where the application's nightly database export
/// lands. The two datasets live in one bucket and describe overlapping facts, and giving them one
/// prefix would make whichever job ran second the one that mattered.
pub const BAR_ARCHIVE_PREFIX: &str = "data/derived/equity/bars";

/// The archive prefix for one bar cadence.
///
/// Hive-partitioned on the interval, so a reader that scans the tree gets the cadence as a column
/// and a second cadence costs one more value rather than a parallel tree. Daily and intraday bars
/// describe overlapping facts — a daily bar is the aggregate of its own intraday bars — and one
/// partition holding both would make whichever job wrote last the one that mattered.
pub fn bar_archive_prefix(interval: BarInterval) -> String {
    format!("{BAR_ARCHIVE_PREFIX}/interval={interval}")
}

/// S3 key for the stock splits the bars are adjusted against.
///
/// One object rather than a partition per session, unlike the bars beside it: the feed revises and
/// cancels announced splits, so a per-date layout would leave a cancelled one sitting in a
/// partition nothing revisits.
pub const SPLITS_ARCHIVE_KEY: &str = "data/derived/equity/corporate_actions/splits.parquet";

/// Object holding every date a symbol's price series may not be read across.
///
/// Beside the splits table rather than merged into it, because the two are read for opposite
/// purposes: a split says how to restate a price across a date, a boundary says not to.
pub const BOUNDARIES_ARCHIVE_KEY: &str = "data/derived/equity/corporate_actions/boundaries.parquet";

/// Trailing sessions re-fetched even when a partition already exists, so a bar restated after the
/// close reaches [`merge_partitions`] rather than being skipped as a day already held.
///
/// Counted in sessions and taken from `expected`, never measured backwards from `end` in calendar
/// days: a Monday run with a two-*day* floor lands on Saturday, leaving the preceding Friday — the
/// session a weekend gives the most time to be restated — never revisited.
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
    #[error("the conditions table could not be loaded: {message}")]
    Conditions { message: String },
    #[error("the classification mapping could not be loaded: {message}")]
    Classification { message: String },
    #[error("the SEC industry codes could not be loaded or published: {message}")]
    IndustryCodes { message: String },
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
    /// A second provider tried to write into a partition a different one already built.
    ///
    /// Refused rather than merged: once two providers' rows are concatenated the partition cannot
    /// say which row came from which. A partition answers for one provider so that a reader
    /// comparing two of them is comparing the vendors rather than the write order.
    #[error(
        "{key} was built by {existing} and {incoming} tried to write into it; a partition answers \
         for one provider. Re-fold the whole partition from one source instead of merging."
    )]
    MixedProvenance {
        key: String,
        existing: String,
        incoming: String,
    },
    /// A stored partition's schema no longer combines with the one being written.
    ///
    /// Refused rather than replaced, on the same grounds as `MixedProvenance`: the fetched frame is
    /// current-schema but it is not a superset, so overwriting silently drops whatever the stored
    /// partition held that this response omits. Replacing is a repair an operator asks for.
    #[error(
        "{key} holds rows this write cannot be combined with ({message}); replacing it would \
         discard whatever it holds that this response omits. Re-fold the partition deliberately."
    )]
    SchemaConflict { key: String, message: String },
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
    /// A cross-cadence comparison could not be made at all, which is separate from one that ran and
    /// disagreed — a disagreement is a finding the pass reports and carries on past.
    #[error("failed to compare cadences: {0}")]
    Cadence(#[from] CadenceError),
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
    /// Summary rows written across all three cadences, and the prints folded to produce them.
    Trades {
        summaries_written: usize,
        trades_folded: usize,
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
            }
            | PassOutput::Trades {
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
            PassOutput::Trades {
                summaries_written,
                trades_folded,
            } => write!(
                formatter,
                ", {summaries_written} summaries, {trades_folded} trades folded"
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

    /// The trade counterpart, separate so neither pass can render the other's units.
    fn into_trade_summary(self, trades_folded: usize) -> PassSummary {
        PassSummary {
            sessions_requested: self.sessions_requested,
            sessions_written: self.sessions_written,
            sessions_without_data: self.sessions_without_data,
            sessions_failed: self.sessions_failed,
            symbols_failed: self.symbols_failed,
            output: PassOutput::Trades {
                summaries_written: self.rows_written,
                trades_folded,
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

/// What a provenance pass did, counted so a run can be believed rather than assumed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ProvenanceOutcome {
    pub objects_seen: usize,
    pub sidecars_present: usize,
    /// What a stamp would write. Counted apart from `sidecars_written` so a reporting run cannot
    /// claim writes it did not make.
    pub sidecars_planned: usize,
    pub sidecars_written: usize,
    /// Objects no source could attribute. Reported rather than guessed at.
    pub unattributed: Vec<String>,
    /// Objects carrying no sidecar, which is what a sweep finds. Distinct from `unattributed`,
    /// because a sweep consults no attribution and so cannot say anything failed to attribute.
    pub sidecars_missing: Vec<String>,
    /// Sidecars a stamp meant to write and could not. One fault costs one object, not the pass.
    pub write_failures: Vec<String>,
}

/// A dataset the archive writes, named once so a prefix and a provenance record cannot disagree.
///
/// An enum rather than the `&str` this began as: the same name reaches `write_merged`, the stamp and
/// the sweep, and nothing linked those lists. A dataset added at one and forgotten at the others was
/// skipped in silence; now it does not compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedDataset {
    Bars,
    Quotes,
    Trades,
    Splits,
    Boundaries,
    Reference,
}

impl DerivedDataset {
    /// Every dataset, so a pass cannot iterate a subset by accident.
    pub const ALL: [DerivedDataset; 6] = [
        DerivedDataset::Bars,
        DerivedDataset::Quotes,
        DerivedDataset::Trades,
        DerivedDataset::Splits,
        DerivedDataset::Boundaries,
        DerivedDataset::Reference,
    ];

    /// The name a provenance record carries, and the name a declaration matches on.
    pub const fn as_str(self) -> &'static str {
        match self {
            DerivedDataset::Bars => "equity_bars",
            DerivedDataset::Quotes => "equity_quotes",
            DerivedDataset::Trades => "equity_trades",
            DerivedDataset::Splits => "equity_corporate_action_splits",
            DerivedDataset::Boundaries => "equity_corporate_action_boundaries",
            DerivedDataset::Reference => "equity_reference",
        }
    }
}

/// One prefix a provenance pass covers, and what its objects are called.
///
/// Named rather than derived from a listing, so a dataset that stops being written still reports its
/// objects instead of silently leaving the sweep.
struct StampablePrefix {
    dataset: String,
    /// `None` for a prefix that has no cadence: every raw dataset, and both corporate-action tables.
    interval: Option<BarInterval>,
    prefix: String,
    /// Derived partitions are parquet, the raw tee keeps the vendor's gzip, and the corporate-action
    /// tables are a single object rather than a partition.
    object: &'static str,
}

/// Every prefix a provenance pass covers: the derived cadences, the whole-table objects, and the raw
/// tee beside them.
fn stampable_prefixes() -> Vec<StampablePrefix> {
    let mut found = Vec::new();
    for dataset in DerivedDataset::ALL {
        match dataset {
            DerivedDataset::Bars => found.extend(session_partitions(dataset, bar_archive_prefix)),
            DerivedDataset::Quotes => {
                found.extend(session_partitions(dataset, quote_archive_prefix))
            }
            DerivedDataset::Trades => {
                found.extend(session_partitions(dataset, trade_archive_prefix))
            }
            // One object, not a partition tree, so the prefix is its parent and the object its name.
            DerivedDataset::Splits => found.push(whole_table(dataset, SPLITS_ARCHIVE_KEY)),
            DerivedDataset::Boundaries => found.push(whole_table(dataset, BOUNDARIES_ARCHIVE_KEY)),
            // Date-partitioned like the folds but without a cadence: a reference row describes a
            // symbol on a date, and there is no intraday version of that.
            DerivedDataset::Reference => found.push(StampablePrefix {
                dataset: dataset.as_str().to_string(),
                interval: None,
                prefix: REFERENCE_ARCHIVE_PREFIX.to_string(),
                object: "data.parquet",
            }),
        }
    }
    for raw in [
        RawDataset::Quotes,
        RawDataset::Trades,
        RawDataset::MinuteAggregates,
    ] {
        found.push(StampablePrefix {
            dataset: format!("raw_equity_{}", raw.as_str()),
            interval: None,
            prefix: format!("data/raw/massive/equity/{}", raw.as_str()),
            object: "data.csv.gz",
        });
    }
    found
}

/// One session-partitioned prefix per cadence, which `build` names.
fn session_partitions(
    dataset: DerivedDataset,
    build: fn(BarInterval) -> String,
) -> Vec<StampablePrefix> {
    BarInterval::ALL
        .into_iter()
        .map(|interval| StampablePrefix {
            dataset: dataset.as_str().to_string(),
            interval: Some(interval),
            prefix: build(interval),
            object: "data.parquet",
        })
        .collect()
}

/// A whole-table object described as a prefix and a filename, so the sweep can list it like any other.
fn whole_table(dataset: DerivedDataset, key: &'static str) -> StampablePrefix {
    let (prefix, object) = key
        .rsplit_once('/')
        .expect("an archive key has a parent prefix");
    StampablePrefix {
        dataset: dataset.as_str().to_string(),
        interval: None,
        prefix: prefix.to_string(),
        object,
    }
}

/// The session a partition or raw key belongs to, whatever its object is called.
///
/// The partition path is written in Eastern terms, so this is the boundary the wrap happens at.
fn session_from_key(key: &str, object: &str) -> Option<SessionDate> {
    let mut segments = key.rsplit('/');
    if segments.next()? != object {
        return None;
    }
    let day: u32 = segments.next()?.strip_prefix("day=")?.parse().ok()?;
    let month: u32 = segments.next()?.strip_prefix("month=")?.parse().ok()?;
    let year: i32 = segments.next()?.strip_prefix("year=")?.parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day).map(SessionDate::from_date)
}

/// Every partition key under `prefix`, with whether it already carries a sidecar.
async fn partitions_and_sidecars(
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
    object: &str,
) -> Result<(Vec<String>, BTreeSet<String>), ArchiveError> {
    let mut partitions = Vec::new();
    let mut sidecars = BTreeSet::new();
    let mut pages = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(format!("{prefix}/"))
        .into_paginator()
        .send();

    while let Some(page) = pages.next().await {
        let page = page.map_err(|error| ArchiveError::List {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            message: error.to_string(),
        })?;
        for listed in page.contents() {
            let Some(key) = listed.key() else { continue };
            if key.ends_with(&format!("/{object}")) {
                partitions.push(key.to_string());
            } else if key.ends_with(".provenance.json") {
                sidecars.insert(key.to_string());
            }
        }
    }
    Ok((partitions, sidecars))
}

/// Writes provenance sidecars for partitions that have none, from an attribution built elsewhere.
///
/// Only ever adds: a partition that already carries a sidecar is left alone, because the record
/// written at the time of the write is better evidence than one reconstructed afterwards.
pub async fn stamp_partition_provenance(
    s3_client: &S3Client,
    bucket: &str,
    attribution: &Attribution,
    declarations: &[Declaration],
    apply: bool,
) -> Result<ProvenanceOutcome, ArchiveError> {
    let mut outcome = ProvenanceOutcome::default();
    for target in stampable_prefixes() {
        let (objects, sidecars) =
            partitions_and_sidecars(s3_client, bucket, &target.prefix, target.object).await?;
        for key in objects {
            outcome.objects_seen += 1;
            let sidecar = PartitionProvenance::sidecar_key(&key);
            if sidecars.contains(&sidecar) {
                outcome.sidecars_present += 1;
                continue;
            }
            let session = session_from_key(&key, target.object);
            // Observed beats declared, and cadence-specific beats dataset-wide. A source that named
            // one cadence must never speak for the others written on the same session.
            let routes = session
                .and_then(|date| {
                    attribution
                        .get(&(target.dataset.clone(), target.interval, date))
                        .or_else(|| attribution.get(&(target.dataset.clone(), None, date)))
                })
                .cloned()
                .or_else(|| declared(declarations, &target).map(|route| vec![route]));
            let Some(routes) = routes else {
                outcome.unattributed.push(key);
                continue;
            };
            outcome.sidecars_planned += 1;
            if !apply {
                continue;
            }
            let record = PartitionProvenance {
                dataset: target.dataset.clone(),
                session: session.map(|at| at.to_string()),
                routes,
                written_at: Utc::now(),
            };
            let body = match serde_json::to_vec(&record) {
                Ok(body) => body,
                Err(error) => {
                    warn!(key = sidecar, %error, "Provenance did not serialize");
                    outcome.write_failures.push(sidecar);
                    continue;
                }
            };
            // One fault costs one object rather than the pass, matching `write_partitions`. A run
            // that dies on a transient S3 error would otherwise discard everything it had done.
            match put_object_with_precondition(
                s3_client,
                bucket,
                &sidecar,
                body,
                "application/json",
                &Precondition::Absent,
            )
            .await
            {
                WriteOutcome::Written => outcome.sidecars_written += 1,
                WriteOutcome::Contended => outcome.sidecars_present += 1,
                WriteOutcome::Failed(message) => {
                    warn!(key = sidecar, message, "Provenance sidecar was not written");
                    outcome.write_failures.push(sidecar);
                }
            }
        }
    }
    Ok(outcome)
}

/// The declared route for a prefix, preferring one that names its cadence.
fn declared(declarations: &[Declaration], target: &StampablePrefix) -> Option<Provenance> {
    let matching = |wanted: Option<BarInterval>| {
        declarations
            .iter()
            .find(|entry| entry.dataset == target.dataset && entry.interval == wanted)
            .map(|entry| entry.provenance)
    };
    matching(target.interval).or_else(|| matching(None))
}

/// Reports partitions carrying no provenance sidecar, which is the check the best-effort write
/// trades against.
pub async fn sweep_partition_provenance(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<ProvenanceOutcome, ArchiveError> {
    let mut outcome = ProvenanceOutcome::default();
    for target in stampable_prefixes() {
        let (objects, sidecars) =
            partitions_and_sidecars(s3_client, bucket, &target.prefix, target.object).await?;
        for key in objects {
            outcome.objects_seen += 1;
            if sidecars.contains(&PartitionProvenance::sidecar_key(&key)) {
                outcome.sidecars_present += 1;
            } else {
                // Missing, not unattributable: a sweep reads no attribution and so cannot say
                // whether anything could have named this object's route.
                outcome.sidecars_missing.push(key);
            }
        }
    }
    Ok(outcome)
}

/// Fetches and writes every session in `[window_start, window_end]` the archive is missing.
///
/// **A set difference, not a lookback**, so a missed night, a week of downtime and an empty bucket
/// are one case. Expected means "worth requesting" rather than "the market traded", so a holiday is
/// requested, answered with nothing, and requested again. Idempotent, safe to interrupt, and safe
/// to run concurrently over the same bucket — [`write_partition`] writes under a precondition on
/// what it read, so a racing writer is rejected rather than overwritten.
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
            Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest),
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
/// want opposite sets.
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
/// The exchanges' test symbols, which the daily aggregate carries and no tick file ever does.
///
/// Listed by name rather than matched by pattern: `ZTS` and `ZBRA` are real securities and any
/// prefix rule wide enough to catch `ZVZZT` catches them too. A test symbol the exchanges add later
/// is absent from here and reads as a gap, which is the safe direction to be wrong in — a false
/// alarm rather than a real name silently excused.
///
/// Sorted, because the lookup binary-searches it.
const EXCHANGE_TEST_SYMBOLS: [&str; 28] = [
    "CBOA", "CBOJ", "CBOL", "CBOO", "CBOT", "CBOX", "CBOY", "CBXA", "CBXJ", "CBXL", "CBXO", "CBXY",
    "IBOT", "MTEST", "MTEST.A", "NTEST", "NTEST.H", "NTEST.I", "ZBZX", "ZEXIT", "ZIEXT", "ZJZZT",
    "ZTEST", "ZTST", "ZVZZT", "ZWZZT", "ZXIET", "ZXZZT",
];

/// Whether this name is an exchange test symbol rather than a security.
fn is_exchange_test_symbol(ticker: &Ticker) -> bool {
    EXCHANGE_TEST_SYMBOLS
        .binary_search(&ticker.as_str())
        .is_ok()
}

/// The names a pass over `daily` should expect to find ticks for.
///
/// Test symbols are dropped here rather than at each call site, because this is the one place a
/// universe is derived from a daily partition and both the folds and the scan read it. Leaving them
/// in made every whole-market pass end `complete: false` on roughly 25 names a session that can
/// never print — a permanent non-zero exit, which is an alert nobody can act on and everybody
/// learns to ignore.
fn names_from_partition(
    names: &NameSelection,
    daily: DataFrame,
) -> Result<BTreeSet<Ticker>, ArchiveError> {
    let derived = match names {
        NameSelection::WholeMarket => partition_tickers(&daily)?,
        NameSelection::Screened(floor) => screen_partition(daily, *floor)?,
        // Taken as given: naming a symbol is a deliberate act, so an operator who asks for a test
        // symbol gets it rather than being silently given nothing.
        NameSelection::Named(symbols) => return Ok(symbols.clone()),
    };
    let expected: BTreeSet<Ticker> = derived
        .into_iter()
        .filter(|ticker| !is_exchange_test_symbol(ticker))
        .collect();
    Ok(expected)
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

    // Massive's per-ticker aggregate route, which the Starter tier answers within its rolling
    // five-year window.
    write_partitions(
        s3_client,
        bucket,
        interval,
        writable,
        progress,
        Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest),
    )
    .await
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
    provenance: Provenance,
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
                let written =
                    write_partition(&client, &bucket, interval, session, frame, provenance).await;
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

/// A family stored as one partition per session, and therefore one that can be short a name.
///
/// Narrower than [`DerivedDataset`] deliberately: splits and boundaries are whole-table objects with
/// no session to be short a name on, so scanning one is not a question that can be asked rather than
/// one that returns nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionFamily {
    Bars,
    Quotes,
    Trades,
}

impl SessionFamily {
    /// The archive prefix this family stores `interval` under.
    pub fn prefix(self, interval: BarInterval) -> String {
        match self {
            SessionFamily::Bars => bar_archive_prefix(interval),
            SessionFamily::Quotes => quote_archive_prefix(interval),
            SessionFamily::Trades => trade_archive_prefix(interval),
        }
    }

    /// The universe a scan of this family compares against, which is the one its own fold fills.
    ///
    /// Carried by the family rather than chosen at the call site because the two selections are not
    /// interchangeable: bars are ingested behind the liquidity screen and the tick folds take every
    /// name the daily partition holds. `floor` stays a parameter because the scan writes nothing, so
    /// scanning wider than the archive was ingested at yields a backfill list rather than a fault.
    pub fn universe(self, floor: LiquidityFloor) -> NameSelection {
        match self {
            SessionFamily::Bars => NameSelection::Screened(floor),
            SessionFamily::Quotes | SessionFamily::Trades => NameSelection::WholeMarket,
        }
    }
}

impl std::fmt::Display for SessionFamily {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            SessionFamily::Bars => "bars",
            SessionFamily::Quotes => "quotes",
            SessionFamily::Trades => "trades",
        };
        formatter.write_str(name)
    }
}

/// Sessions scanned at once.
///
/// Two reads a session against S3, and the scan is read-only, so this bounds our own concurrency
/// rather than protecting anything. A full archive sweep is ~2,500 reads and sequential it is
/// twenty minutes of latency for no work.
const SCAN_CONCURRENCY: usize = 16;

/// Which names each session partition lacks against the daily universe for its own session.
///
/// The difference [`SessionSelection::Absent`] cannot express: a partition written while one
/// symbol's fetch failed is present, non-empty, and short a name. The selection comes from `family`
/// rather than from the caller, because the two are not interchangeable and picking the wrong one is
/// silent — only `floor` is open, and only because a scan writes nothing, so scanning wider than the
/// archive was ingested at yields a backfill list rather than a fault.
pub async fn scan_session_symbols(
    s3_client: &S3Client,
    bucket: &str,
    family: SessionFamily,
    interval: BarInterval,
    floor: LiquidityFloor,
    window_start: SessionDate,
    window_end: SessionDate,
) -> Result<SymbolScan, ArchiveError> {
    let names = family.universe(floor);
    let sessions = expected_sessions(window_start, window_end);
    info!(
        %window_start,
        %window_end,
        %family,
        interval = %interval,
        sessions = sessions.len(),
        "Scanning session partitions for symbol-level gaps"
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
            let names = names.clone();
            scans.spawn(async move {
                let coverage =
                    session_coverage(&client, &bucket, family, interval, &names, session).await;
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
                    key: family.prefix(interval),
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
    family: SessionFamily,
    interval: BarInterval,
    names: &NameSelection,
    session: SessionDate,
) -> Result<SessionCoverage, ArchiveError> {
    let daily_key = date_partitioned_key(&bar_archive_prefix(BarInterval::OneDay), session.date());
    let Some(daily) = read_partition(s3_client, bucket, &daily_key).await? else {
        return Ok(SessionCoverage::Undescribed);
    };
    let expected = names_from_partition(names, daily)?;

    let scanned_key = date_partitioned_key(&family.prefix(interval), session.date());
    let Some(scanned) = read_partition(s3_client, bucket, &scanned_key).await? else {
        return Ok(SessionCoverage::Absent);
    };
    Ok(coverage_of(&expected, &partition_tickers(&scanned)?))
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
/// The read-merge-write is a compare-and-swap on the object's `ETag`: the write carries `If-Match`
/// on it -- or `If-None-Match: *` when the partition did not exist -- so a pass that raced another
/// is rejected with `412` instead of silently discarding the other's rows. The whole cycle is
/// retried against the now-current object, which is why the merge is redone rather than the buffer
/// resent.
async fn write_partition(
    s3_client: &S3Client,
    bucket: &str,
    interval: BarInterval,
    session: SessionDate,
    fetched: DataFrame,
    provenance: Provenance,
) -> Result<(), ArchiveError> {
    let key = date_partitioned_key(&bar_archive_prefix(interval), session.date());
    write_merged(
        s3_client,
        bucket,
        key,
        fetched,
        |existing, fetched, key| merge_or_refuse(existing, fetched, key),
        DerivedDataset::Bars,
        Authorship::refusing(provenance),
    )
    .await
}

/// What a write does about a provider the partition it is entering already names.
///
/// [`ForeignProvider::Refuse`] is the standing rule and the default everywhere: a partition answers
/// for one vendor. [`ForeignProvider::Claim`] is the re-fold's, and it is the only way a partition
/// that already mixes two vendors becomes single-source — merging would keep precisely the rows a
/// re-fold exists to remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignProvider {
    Refuse,
    /// Takes the partition over. Must not run beside a repair on the same session: the parquet and
    /// its sidecar are two conditional writes, and a repair landing between them merges into the
    /// replacement, which the claim then records as single-source.
    Claim,
}

/// Who a write is attributed to, and what it does about a provider already named.
///
/// The two travel together because the second is only meaningful against the first: a claim is the
/// statement that *this* provider owns the partition from here on, so the pair cannot be separated
/// without letting a caller claim a partition for nobody in particular. Named for the byline rather
/// than the record, since [`crate::data::attribution::Attribution`] is already the stored one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Authorship {
    provenance: Provenance,
    foreign: ForeignProvider,
}

impl Authorship {
    /// A write that refuses to enter a partition another provider built.
    pub const fn refusing(provenance: Provenance) -> Self {
        Self {
            provenance,
            foreign: ForeignProvider::Refuse,
        }
    }

    /// A write that takes the partition over from whatever built it.
    pub const fn claiming(provenance: Provenance) -> Self {
        Self {
            provenance,
            foreign: ForeignProvider::Claim,
        }
    }

    pub const fn provenance(self) -> Provenance {
        self.provenance
    }

    const fn claims(self) -> bool {
        matches!(self.foreign, ForeignProvider::Claim)
    }
}

/// The read-merge-write cycle itself, over any key and any way of combining the two frames.
///
/// Shared because the bar partitions and the splits table differ only in how they merge — the
/// compare-and-swap around it, and the reasoning for it, are the same either way. A claiming
/// attribution does not call `merge` at all: its whole purpose is that the stored rows do not
/// survive.
async fn write_merged<F>(
    s3_client: &S3Client,
    bucket: &str,
    key: String,
    fetched: DataFrame,
    merge: F,
    dataset: DerivedDataset,
    authorship: Authorship,
) -> Result<(), ArchiveError>
where
    F: Fn(DataFrame, DataFrame, &str) -> Result<DataFrame, ArchiveError>,
{
    for attempt in 1..=CONTENDED_WRITE_ATTEMPTS {
        // Merged with whatever the partition already holds rather than written over it. A plain
        // overwrite would discard anything a later response happens to omit, and merging costs one
        // read of an object this pass is about to replace anyway.
        let existing = read_partition_with_etag(s3_client, bucket, &key).await?;
        // Checked before the merge, not after: the sidecar records a set of routes rather than a
        // route per row, so this is the last point at which the two providers are still separable.
        if existing.is_some() && !authorship.claims() {
            refuse_a_second_provider(s3_client, bucket, &key, authorship.provenance).await?;
        }
        let (mut frame, precondition) = match existing {
            // Still conditional on the ETag: a claim overwrites the object this pass read, so a
            // failed re-fold cannot leave a hole where a wrong-but-present partition was.
            Some((_, etag)) if authorship.claims() => (fetched.clone(), Precondition::Match(etag)),
            Some((existing_frame, etag)) => (
                merge(existing_frame, fetched.clone(), &key)?,
                Precondition::Match(etag),
            ),
            None => (fetched.clone(), Precondition::Absent),
        };

        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer).finish(&mut frame)?;

        match put_partition(s3_client, bucket, &key, buffer, &precondition).await {
            WriteOutcome::Written => {
                return match write_sidecar(s3_client, bucket, &key, dataset, authorship).await {
                    Ok(()) => Ok(()),
                    // A claim's record is the write, not a footnote to it. Rows that changed hands
                    // under a record still naming the old provider read as a re-fold that did
                    // nothing, and every provenance query would agree with the record.
                    Err(message) if authorship.claims() => Err(ArchiveError::Write {
                        bucket: bucket.to_string(),
                        key: PartitionProvenance::sidecar_key(&key),
                        message,
                    }),
                    Err(message) => {
                        warn!(key, message, "Provenance sidecar was not written");
                        Ok(())
                    }
                };
            }
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

/// Refuses a write into a partition a different provider already built.
///
/// A missing sidecar is allowed through because the archive predates provenance, while an unreadable
/// one is refused, since "could not tell" and "safe" are different answers. This stops a later pass
/// rather than a simultaneous one: a writer landing between another's partition and its sidecar sees
/// no record and passes, a window no ordering of two objects closes.
async fn refuse_a_second_provider(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    incoming: Provenance,
) -> Result<(), ArchiveError> {
    let sidecar = PartitionProvenance::sidecar_key(key);
    let record = match read_sidecar(s3_client, bucket, &sidecar).await? {
        SidecarRead::Found(record, _) => record,
        SidecarRead::Absent => return Ok(()),
        SidecarRead::Unreadable => {
            return Err(ArchiveError::Read {
                bucket: bucket.to_string(),
                key: sidecar,
                message: "the provenance record did not parse, so the provider it names is unknown"
                    .to_string(),
            })
        }
    };
    let foreign: Vec<&str> = record
        .routes
        .iter()
        .map(|route| route.provider_name())
        .filter(|provider| *provider != incoming.provider_name())
        .collect();
    if foreign.is_empty() {
        return Ok(());
    }
    Err(ArchiveError::MixedProvenance {
        key: key.to_string(),
        existing: foreign.join(" and "),
        incoming: incoming.provider_name().to_string(),
    })
}

/// Merges, or refuses when the stored partition's schema will not combine with the fetched one.
///
/// The caller decides what to do with one refused partition; what it must not do is decide for the
/// rows, which is what replacing them silently did.
fn merge_or_refuse(
    existing: DataFrame,
    fetched: DataFrame,
    key: &str,
) -> Result<DataFrame, ArchiveError> {
    let disagreement = schema_disagreement(&existing, &fetched);
    merge_partitions(existing, fetched)
        .map_err(|error| classify_merge_failure(error, disagreement, key))
}

/// The same refusal for the splits table, which merges on its own rule rather than the bar key.
///
/// Separate from the write it guards so it can be tested; inline in an async S3 function, the
/// fallback this replaces was unreachable from any test.
fn merge_splits_or_refuse(
    existing: DataFrame,
    fetched: DataFrame,
    key: &str,
) -> Result<DataFrame, ArchiveError> {
    let disagreement = schema_disagreement(&existing, &fetched);
    splits::merge_splits(existing, fetched)
        .map_err(|error| classify_merge_failure(error, disagreement, key))
}

/// Calls a failed merge a schema conflict only when the two frames actually disagreed.
///
/// The disagreement is established before the merge rather than read off the failure: polars
/// reports a mismatched `concat` as `InvalidOperation`, the same variant it raises for faults that
/// have nothing to do with schema, so the error alone cannot tell the two apart. A merge that fails
/// while the columns agree keeps its own error, because telling an operator to re-fold a partition
/// that is not malformed sends them to repair the wrong thing.
fn classify_merge_failure(
    error: PolarsError,
    disagreement: Option<String>,
    key: &str,
) -> ArchiveError {
    match disagreement {
        Some(message) => ArchiveError::SchemaConflict {
            key: key.to_string(),
            message,
        },
        None => ArchiveError::Frame(error),
    }
}

/// How a stored frame's columns differ from the ones being written, or `None` when they agree.
fn schema_disagreement(existing: &DataFrame, fetched: &DataFrame) -> Option<String> {
    let stored = existing.schema();
    let incoming = fetched.schema();
    if stored == incoming {
        return None;
    }

    let missing: Vec<&str> = stored
        .iter_names()
        .filter(|name| !incoming.contains(name))
        .map(|name| name.as_str())
        .collect();
    let added: Vec<&str> = incoming
        .iter_names()
        .filter(|name| !stored.contains(name))
        .map(|name| name.as_str())
        .collect();

    // Neither list is populated when the columns match by name but differ in type or in order,
    // which `concat` rejects just as firmly.
    if missing.is_empty() && added.is_empty() {
        return Some(
            "the stored columns match by name but differ in type or order from this write"
                .to_string(),
        );
    }
    Some(format!(
        "the stored partition holds [{}] that this write does not, and this write holds [{}] that \
         it does not",
        missing.join(", "),
        added.join(", ")
    ))
}

/// Root of the quote-summary archive, beside the bars rather than under them.
///
/// Keyed by what the rows describe, not by who supplied them: the five-year backfill folded these
/// from Massive's flat files and a repair folds the same names from Alpaca, so one session's
/// partition routinely holds both. Splitting by vendor would put one session under two keys.
pub const QUOTE_ARCHIVE_PREFIX: &str = "data/derived/equity/quotes";

/// The archive prefix for one summary cadence, hive-partitioned like the bars.
pub fn quote_archive_prefix(interval: BarInterval) -> String {
    format!("{QUOTE_ARCHIVE_PREFIX}/interval={interval}")
}

/// Root of the trade-summary archive, beside the quotes it will eventually be differenced against.
pub const TRADE_ARCHIVE_PREFIX: &str = "data/derived/equity/trades";

/// Root of the point-in-time symbol reference, one partition per `as_of` date.
///
/// Partitioned rather than a whole table, unlike the corporate-action objects: those are one table
/// the feed revises in place, and these are dated observations that only ever accumulate. An append
/// is a new key, so a failed backfill cannot damage the quarters that already landed.
pub const REFERENCE_ARCHIVE_PREFIX: &str = "data/derived/equity/reference";

/// The archive prefix for one trade cadence, hive-partitioned like the quotes.
pub fn trade_archive_prefix(interval: BarInterval) -> String {
    format!("{TRADE_ARCHIVE_PREFIX}/interval={interval}")
}

/// Where the published SIC-to-bucket mapping lives, one partition per `as_of`.
///
/// A dataset rather than a table compiled into the binary, so republished definitions need no
/// deploy. Partitioned by `as_of` and written only when the rows change, which makes the partition
/// list the change history rather than a log of when someone last looked.
pub const CLASSIFICATION_ARCHIVE_PREFIX: &str = "data/reference/classification";

/// The key one `as_of` of the mapping is written to.
pub fn classification_key(as_of: chrono::NaiveDate) -> String {
    format!("{CLASSIFICATION_ARCHIVE_PREFIX}/as_of={as_of}/data.parquet")
}

/// Every `as_of` the mapping holds a partition for, ascending.
pub async fn classification_as_of_dates(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<Vec<chrono::NaiveDate>, ArchiveError> {
    as_of_partition_dates(s3_client, bucket, CLASSIFICATION_ARCHIVE_PREFIX).await
}

/// The key one `as_of` of a reference dataset under `prefix` is written to.
fn as_of_key(prefix: &str, as_of: chrono::NaiveDate) -> String {
    format!("{prefix}/as_of={as_of}/data.parquet")
}

/// Every `as_of` a reference dataset under `prefix` holds a partition for, ascending.
///
/// One listing for every such dataset, so they cannot come to disagree about which keys count.
async fn as_of_partition_dates(
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<chrono::NaiveDate>, ArchiveError> {
    let (partitions, _sidecars) =
        partitions_and_sidecars(s3_client, bucket, prefix, "data.parquet").await?;

    // Kept only when the date round-trips to the very key it was read from. Finding `as_of=` in any
    // segment would accept `as_of=2099-01-01/backup/data.parquet` and then send the reader to a
    // canonical key that does not exist, failing the load while a valid earlier partition sits there.
    let mut dates: Vec<chrono::NaiveDate> = partitions
        .iter()
        .filter_map(|key| {
            let date = key
                .split('/')
                .find_map(|segment| segment.strip_prefix("as_of="))
                .and_then(|value| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())?;
            (as_of_key(prefix, date) == *key).then_some(date)
        })
        .collect();
    dates.sort_unstable();
    dates.dedup();
    Ok(dates)
}

/// Loads the newest published mapping.
///
/// Refuses when the dataset is absent or empty rather than falling back to a default: an empty
/// mapping classifies every name as the fallback bucket, which is a plausible-looking answer no
/// caller could distinguish from a market where nothing is classifiable.
pub async fn read_newest_classification(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<ClassificationTable, ArchiveError> {
    let newest = classification_as_of_dates(s3_client, bucket)
        .await?
        .pop()
        .ok_or_else(|| ArchiveError::Classification {
            message: format!(
                "no mapping partition under s3://{bucket}/{CLASSIFICATION_ARCHIVE_PREFIX}"
            ),
        })?;
    let key = classification_key(newest);
    let frame = read_partition(s3_client, bucket, &key)
        .await?
        .ok_or_else(|| ArchiveError::Classification {
            message: format!("s3://{bucket}/{key} was listed and then could not be read"),
        })?;
    ClassificationTable::from_dataframe(newest, &frame).map_err(|error| {
        ArchiveError::Classification {
            message: format!("s3://{bucket}/{key} is not a usable mapping: {error}"),
        }
    })
}

/// Where the provider's sale-condition table is published, one partition per `as_of`.
pub const CONDITIONS_ARCHIVE_PREFIX: &str = "data/reference/conditions";

/// The key one `as_of` of the conditions table is written to.
pub fn conditions_key(as_of: chrono::NaiveDate) -> String {
    format!("{CONDITIONS_ARCHIVE_PREFIX}/as_of={as_of}/data.parquet")
}

/// Every `as_of` the conditions table holds a partition for, ascending.
pub async fn conditions_as_of_dates(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<Vec<chrono::NaiveDate>, ArchiveError> {
    as_of_partition_dates(s3_client, bucket, CONDITIONS_ARCHIVE_PREFIX).await
}

/// Loads the newest published conditions table.
///
/// Refuses when the dataset is absent rather than folding under no rules: an empty table reports
/// every print as unresolved, which is indistinguishable from a tape nobody can read.
pub async fn read_newest_conditions(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<ConditionsTable, ArchiveError> {
    let newest = conditions_as_of_dates(s3_client, bucket)
        .await?
        .pop()
        .ok_or_else(|| ArchiveError::Conditions {
            message: format!(
                "no conditions partition under s3://{bucket}/{CONDITIONS_ARCHIVE_PREFIX}"
            ),
        })?;
    let key = conditions_key(newest);
    let frame = read_partition(s3_client, bucket, &key)
        .await?
        .ok_or_else(|| ArchiveError::Conditions {
            message: format!("s3://{bucket}/{key} was listed and then could not be read"),
        })?;
    ConditionsTable::from_dataframe(newest, &frame).map_err(|error| ArchiveError::Conditions {
        message: format!("s3://{bucket}/{key} is not a usable conditions table: {error}"),
    })
}

/// Where the SEC's industry codes are published, one partition per `as_of`.
pub const INDUSTRY_CODES_ARCHIVE_PREFIX: &str = "data/reference/sec_industry_codes";

/// The key one `as_of` of the SEC industry codes is written to.
pub fn industry_codes_key(as_of: chrono::NaiveDate) -> String {
    as_of_key(INDUSTRY_CODES_ARCHIVE_PREFIX, as_of)
}

/// Loads the newest published SEC industry codes, or `None` when none has been published.
///
/// Absence is an answer here, unlike for conditions: without the table every name keeps whatever code
/// Massive gave it, which is exactly the state before this dataset existed.
pub async fn read_newest_industry_codes(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<Option<IndustryCodesTable>, ArchiveError> {
    let Some(newest) = as_of_partition_dates(s3_client, bucket, INDUSTRY_CODES_ARCHIVE_PREFIX)
        .await?
        .pop()
    else {
        return Ok(None);
    };
    let key = industry_codes_key(newest);
    let frame = read_partition(s3_client, bucket, &key)
        .await?
        .ok_or_else(|| ArchiveError::IndustryCodes {
            message: format!("s3://{bucket}/{key} was listed and then could not be read"),
        })?;
    IndustryCodesTable::from_dataframe(newest, &frame)
        .map(Some)
        .map_err(|error| ArchiveError::IndustryCodes {
            message: format!("s3://{bucket}/{key} is not a usable table: {error}"),
        })
}

/// Every filer of common stock the reference archive holds with no Massive code, across all of it.
///
/// All partitions rather than the newest, because a name that delisted in 2023 still classifies the
/// 2023 bars it printed. A partition written before the CIK was recorded contributes nothing, and is
/// counted so a caller can say how much of the archive the lookup could not reach.
pub async fn filers_without_a_code(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<(BTreeSet<Cik>, usize), ArchiveError> {
    let mut filers = BTreeSet::new();
    let mut without_a_cik_column = 0usize;
    for as_of in reference_partition_dates(s3_client, bucket).await? {
        let frame = read_reference_partition(s3_client, bucket, as_of).await?;
        let Ok(ciks) = frame.column("cik").and_then(|column| column.str().cloned()) else {
            without_a_cik_column += 1;
            continue;
        };
        let security_types = frame.column("security_type")?.str()?;
        let sic_codes = frame.column("sic_code")?.str()?;
        for row in 0..frame.height() {
            let common_stock = security_types.get(row) == Some(SecurityType::CommonStock.as_code());
            if common_stock && sic_codes.get(row).and_then(SicCode::new).is_none() {
                filers.extend(ciks.get(row).and_then(Cik::new));
            }
        }
    }
    Ok((filers, without_a_cik_column))
}

/// What one refresh of the SEC industry codes did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndustryCodesOutcome {
    /// Nothing in the archive lacked a code, so EDGAR was not asked.
    NothingToLookUp,
    /// Every filer was asked and the answers match the newest published table.
    Unchanged { filers: usize, coded: usize },
    /// Every filer was asked and a new `as_of` was written.
    Published {
        as_of: chrono::NaiveDate,
        filers: usize,
        coded: usize,
    },
    /// Some filers could not be asked, so nothing was published: a partial table would silently
    /// drop codes the stored one holds.
    Incomplete { filers: usize, failed: usize },
}

/// Asks EDGAR for every filer the archive lacks a code for, and publishes when the answers changed.
///
/// Sequential at [`edgar::REQUESTS_PER_SECOND`]: roughly a thousand filers take two minutes, and the
/// SEC answers a burst by blocking the address rather than slowing it.
pub async fn refresh_industry_codes(
    s3_client: &S3Client,
    edgar_client: &edgar::EdgarClient,
    bucket: &str,
    today: SessionDate,
) -> Result<IndustryCodesOutcome, ArchiveError> {
    let (filers, without_a_cik_column) = filers_without_a_code(s3_client, bucket).await?;
    if without_a_cik_column > 0 {
        warn!(
            partitions = without_a_cik_column,
            "Reference partitions carry no CIK; their filers cannot be looked up until re-swept"
        );
    }
    if filers.is_empty() {
        return Ok(IndustryCodesOutcome::NothingToLookUp);
    }

    let mut pace = tokio::time::interval(std::time::Duration::from_millis(
        1000 / u64::from(edgar::REQUESTS_PER_SECOND),
    ));
    let mut rows: Vec<IndustryCode> = Vec::new();
    let mut failed = 0usize;
    for cik in &filers {
        pace.tick().await;
        match edgar_client.registration(cik).await {
            Ok(Some(registration)) => rows.push(IndustryCode {
                cik: cik.clone(),
                sic_code: registration.sic_code,
                sic_description: registration.sic_description,
            }),
            Ok(None) => {}
            Err(error) => {
                warn!(cik = cik.as_str(), %error, "EDGAR lookup failed");
                failed += 1;
            }
        }
    }
    if failed > 0 {
        return Ok(IndustryCodesOutcome::Incomplete {
            filers: filers.len(),
            failed,
        });
    }

    let coded = rows.len();
    let fetched = IndustryCodesTable::new(today.date(), rows).map_err(|error| {
        ArchiveError::IndustryCodes {
            message: format!("EDGAR coded none of {} filers: {error}", filers.len()),
        }
    })?;
    if let Some(stored) = read_newest_industry_codes(s3_client, bucket).await? {
        if stored.same_rows_as(&fetched) {
            return Ok(IndustryCodesOutcome::Unchanged {
                filers: filers.len(),
                coded,
            });
        }
    }

    let key = industry_codes_key(today.date());
    let mut frame = fetched.to_dataframe()?;
    let mut buffer: Vec<u8> = Vec::new();
    ParquetWriter::new(&mut buffer).finish(&mut frame)?;
    // Absent-only, so a second change on one day refuses rather than replacing what a run that day
    // already read -- the `fetch-trade-conditions` rule, for the same reason.
    match put_object_with_precondition(
        s3_client,
        bucket,
        &key,
        buffer,
        "application/vnd.apache.parquet",
        &Precondition::Absent,
    )
    .await
    {
        WriteOutcome::Written => Ok(IndustryCodesOutcome::Published {
            as_of: today.date(),
            filers: filers.len(),
            coded,
        }),
        WriteOutcome::Contended => Err(ArchiveError::IndustryCodes {
            message: format!("s3://{bucket}/{key} already holds a different table"),
        }),
        WriteOutcome::Failed(message) => Err(ArchiveError::IndustryCodes { message }),
    }
}

/// Every `as_of` the reference dataset holds a partition for, ascending.
///
/// Listed rather than derived from the quarterly grid, because the grid is a policy about what
/// *should* be fetched and a reader has to know what is actually there. A key whose hive components
/// do not parse is skipped with a warning rather than failing the read.
pub async fn reference_partition_dates(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<Vec<SessionDate>, ArchiveError> {
    let (partitions, _sidecars) =
        partitions_and_sidecars(s3_client, bucket, REFERENCE_ARCHIVE_PREFIX, "data.parquet")
            .await?;

    let mut dates: Vec<SessionDate> = Vec::with_capacity(partitions.len());
    for key in &partitions {
        match session_from_key(key, "data.parquet") {
            Some(date) => dates.push(date),
            None => warn!(
                key,
                "Skipped a reference partition whose key carries no date"
            ),
        }
    }
    dates.sort_unstable();
    Ok(dates)
}

/// The newest point-in-time universe the archive holds.
///
/// The newest observation rather than one chosen per session, for readers that classify the current
/// session only: the screen's sector cap is a question about what a name is now. A study reading a
/// historical window wants [`read_reference_window`] instead, and the two must not be confused.
pub async fn current_universe(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<reference::Universe, ArchiveError> {
    let dates = reference_partition_dates(s3_client, bucket).await?;
    let newest = dates.last().copied().ok_or_else(|| ArchiveError::Read {
        bucket: bucket.to_string(),
        key: REFERENCE_ARCHIVE_PREFIX.to_string(),
        message: "the reference dataset holds no partitions".to_string(),
    })?;
    // Read directly rather than through `read_reference_window`, which would list a second time: if
    // the newest key vanished between the two listings its nearest-prior rule returns an older one.
    let frame = read_reference_partition(s3_client, bucket, newest).await?;
    let table = read_newest_classification(s3_client, bucket).await?;
    let industry_codes = read_newest_industry_codes(s3_client, bucket).await?;
    let universe = reference::universe_of(&[(newest, frame)], &table, industry_codes.as_ref())?;
    // Journalled because the grouping is re-derived on every read: two runs agree about which names
    // share a sector only if they used the same mapping and the same SEC codes beneath it.
    tracing::info!(
        classification_as_of = %table.as_of(),
        industry_codes_as_of = ?industry_codes.as_ref().map(IndustryCodesTable::as_of),
        coded_by_sec = universe.coded_by_sec(),
        reference_as_of = %newest,
        "Built the current universe"
    );
    Ok(universe)
}

/// Which of the `available` observations answer for the sessions in `[start, end]`.
///
/// The window opens at the nearest `as_of` at or before `start`, not the first one inside it. Where
/// nothing precedes the window the earliest available is used instead, which leaves the opening
/// sessions unclassified rather than classified by an observation that postdates them.
fn covering_partitions(
    available: &[SessionDate],
    start: SessionDate,
    end: SessionDate,
) -> Vec<SessionDate> {
    let Some(opening) = available
        .iter()
        .rev()
        .find(|date| **date <= start)
        .or_else(|| available.first())
    else {
        return Vec::new();
    };
    available
        .iter()
        .copied()
        .filter(|date| date >= opening && *date <= end)
        .collect()
}

/// Every reference partition that answers for a session in `[start, end]`, ascending by `as_of`.
///
/// The first is the nearest `as_of` at or before `start` rather than the first one inside the
/// window: a session is answered for by the most recent observation preceding it, so a window
/// opening mid-quarter would otherwise lose every name until the next quarter began.
pub async fn read_reference_window(
    s3_client: &S3Client,
    bucket: &str,
    start: SessionDate,
    end: SessionDate,
) -> Result<Vec<(SessionDate, DataFrame)>, ArchiveError> {
    let available = reference_partition_dates(s3_client, bucket).await?;
    let wanted = covering_partitions(&available, start, end);

    let mut partitions = Vec::with_capacity(wanted.len());
    for as_of in wanted {
        partitions.push((
            as_of,
            read_reference_partition(s3_client, bucket, as_of).await?,
        ));
    }
    Ok(partitions)
}

/// Reads one `as_of` partition, refusing a key that was listed and is no longer there.
///
/// A skip would narrow the sequence silently, and the carry-forward in
/// [`crate::data::reference::universe_of`] would hold the previous observation across the hole --
/// reclassifying a quarter of bars by a stale one with nothing recording that it happened.
async fn read_reference_partition(
    s3_client: &S3Client,
    bucket: &str,
    as_of: SessionDate,
) -> Result<DataFrame, ArchiveError> {
    let key = date_partitioned_key(REFERENCE_ARCHIVE_PREFIX, as_of.date());
    read_partition(s3_client, bucket, &key)
        .await?
        .ok_or_else(|| ArchiveError::Read {
            bucket: bucket.to_string(),
            key,
            message: "a listed reference partition was gone before it could be read".to_string(),
        })
}

/// Names folded at once.
///
/// Thirty-two moving no more than eight is a straggler and not the endpoint's ceiling: a name is a
/// serial pagination chain, so a short symbol list is bounded by its longest member. Across the
/// whole-market universe that amortises, making this times the per-stream rate the real bound.
const QUOTE_CONCURRENCY: usize = 8;

/// Attempts per symbol before a session gives up on it.
///
/// The second line of defence, not the first: [`MarketDataClient::fetch_quotes`] already retries
/// the individual page, so what reaches here has failed a page four times running. Load-bearing for
/// the same reason the intraday one is — nothing downstream can tell a session summarized without
/// one of its names from a complete one.
const QUOTE_SYMBOL_ATTEMPTS: usize = 3;

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

impl QuoteSource<'_> {
    /// Where this route's ticks come from.
    ///
    /// Derived from the variant rather than passed alongside it, so a session folded from Alpaca
    /// cannot be filed as having come from a flat file.
    pub const fn provenance(&self) -> Provenance {
        match self {
            QuoteSource::PerName(_) => Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
            QuoteSource::WholeSession(_) => RawDataset::Quotes.provenance(),
        }
    }
}

impl std::fmt::Display for QuoteSource<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            QuoteSource::PerName(_) => "per-name",
            QuoteSource::WholeSession(_) => "whole-session",
        })
    }
}

/// Folds the quoted book across `sessions`, per `scope`, taking its ticks from `source`.
///
/// Regular hours only, taken from the calendar so an early close is 3.5 hours rather than 6.5: the
/// overnight book is an order of magnitude wider and would swamp any session mean it entered.
/// `sessions` must already be calendar-filtered, which the returned summary assumes when it reports
/// a session that answered with nothing as a fault rather than as a holiday.
#[allow(clippy::too_many_arguments)]
pub async fn archive_quote_sessions(
    s3_client: &S3Client,
    source: &QuoteSource<'_>,
    calendar: &TradingCalendar,
    bucket: &str,
    sessions: &[SessionDate],
    scope: &Scope,
    cadence: IntradayCadence,
    foreign: ForeignProvider,
) -> Result<PassSummary, ArchiveError> {
    let (Some(first), Some(last)) = (sessions.first(), sessions.last()) else {
        return Ok(PassProgress::default().into_quote_summary(0));
    };
    let present = present_partitions(
        s3_client,
        bucket,
        &quote_archive_prefix(quote_presence_interval(cadence)),
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
    let mut agreement = CadenceTotals::default();
    for session in requested {
        quotes_folded += archive_quote_session(
            s3_client,
            source,
            calendar,
            bucket,
            session,
            scope,
            cadence,
            foreign,
            &mut progress,
            &mut agreement,
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
    if agreement.sessions() > 0 {
        // Reported whichever way it went. A cadence that only speaks up when it disagrees leaves a
        // clean run indistinguishable from one where the check never ran.
        let (folded_only, stored_only) = agreement.one_sided();
        info!(
            sessions_checked = agreement.sessions(),
            sessions_agreeing = agreement.sessions_agreeing(),
            rows_compared = agreement.compared(),
            disagreements = agreement.disagreements(),
            folded_only,
            stored_only,
            unresolved = ?agreement.unresolved(),
            "Checked the folded session rows against the stored ones"
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
    cadence: IntradayCadence,
    foreign: ForeignProvider,
    progress: &mut PassProgress,
    agreement: &mut CadenceTotals,
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

    info!(%session, %open, %close, %source, %cadence, universe = universe.len(), "Folding a session's quoted book");
    let (folded, quotes_folded) = match source {
        QuoteSource::PerName(market_data) => {
            fold_universe(
                market_data,
                session,
                cadence,
                open,
                close,
                &universe,
                progress,
            )
            .await
        }
        QuoteSource::WholeSession(flat_files) => {
            match fold_whole_session(
                flat_files, session, cadence, open, close, universe, progress,
            )
            .await
            {
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
    write_quote_partitions(
        s3_client,
        bucket,
        session,
        folded,
        cadence,
        progress,
        agreement,
        Authorship {
            provenance: source.provenance(),
            foreign,
        },
    )
    .await?;
    Ok(quotes_folded)
}

/// Fans out over the universe, folding each name's session and keeping the summaries and the cost.
#[allow(clippy::too_many_arguments)]
async fn fold_universe(
    market_data: &MarketDataClient,
    session: SessionDate,
    cadence: IntradayCadence,
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
                    match quotes::fold_session(&client, &ticker, session, cadence, open, close)
                        .await
                    {
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
#[allow(clippy::too_many_arguments)]
async fn fold_whole_session(
    flat_files: &FlatFileClient,
    session: SessionDate,
    cadence: IntradayCadence,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    universe: BTreeSet<Ticker>,
    progress: &mut PassProgress,
) -> Result<(Vec<QuoteSummary>, usize), FlatFileError> {
    let requested = universe.len();
    let fold = quotes::MarketFold::new(session, cadence, open, close, universe);
    let (_, fold) = flat_files.fold_quotes(session.date(), fold).await?;

    let quotes_folded = fold.folded();
    let folded = fold.finish();
    // Counted against what the file carried rather than against what produced a summary: a name that
    // was there and stayed quiet through regular hours is not a failure, and the per-name path counts
    // only a failed fetch. Counting it would make almost every session of a whole-market pass exit
    // non-zero and invite a re-run that cannot change the result.
    progress.symbols_failed += requested.saturating_sub(folded.seen);
    if !folded.resumed.is_empty() {
        info!(%session, resumed = folded.resumed.len(), "Names whose rows arrived in more than one run");
    }
    Ok((folded.summaries, quotes_folded))
}

/// The prefix a quote pass reads presence from, which must be the last partition it writes.
///
/// Presence has to name something the pass is certain to write, or `archive` skips sessions it only
/// half-wrote. The five-minute pass writes the session row last and is keyed on it; a one-minute
/// pass does not author that row at all, so it is keyed on its own cadence — without which every
/// session already reads present and a one-minute `archive` silently does nothing.
fn quote_presence_interval(cadence: IntradayCadence) -> BarInterval {
    match cadence {
        IntradayCadence::FiveMinute => BarInterval::OneDay,
        IntradayCadence::OneMinute => BarInterval::OneMinute,
    }
}

/// The cadence that authors the archive's daily quote row.
///
/// A finer pass derives the same row from its own buckets, which is evidence about that row rather
/// than a replacement for it. Re-emitting it would put an unverified figure over a stored one.
const DAILY_QUOTE_AUTHOR: IntradayCadence = IntradayCadence::FiveMinute;

/// The prefixes a quote pass writes, in the order it writes them.
///
/// Stated as an order rather than derived from [`quote_presence_interval`], so a test can pin both
/// to literals and still assert they agree — the failure this guards is silent, and a derivation
/// would move with whichever of the two it was derived from.
fn quote_write_order(cadence: IntradayCadence) -> [BarInterval; 2] {
    match cadence {
        IntradayCadence::FiveMinute => [BarInterval::FiveMinute, BarInterval::OneDay],
        IntradayCadence::OneMinute => [BarInterval::OneDay, BarInterval::OneMinute],
    }
}

/// Writes a session's summaries, the prefix presence is read off last.
///
/// The order is the recovery rule rather than a preference, and it flips between cadences because
/// which prefix carries presence does: a pass that dies partway has to leave the session looking
/// absent, or the next `archive` skips what it only half-wrote.
#[allow(clippy::too_many_arguments)]
async fn write_quote_partitions(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
    folded: Vec<QuoteSummary>,
    cadence: IntradayCadence,
    progress: &mut PassProgress,
    agreement: &mut CadenceTotals,
    authorship: Authorship,
) -> Result<(), ArchiveError> {
    let mut intraday: Vec<QuoteSummary> = Vec::new();
    let mut daily: Vec<QuoteSummary> = Vec::new();
    for row in folded {
        match row.bar_interval() {
            BarInterval::OneDay => daily.push(row),
            BarInterval::OneMinute | BarInterval::FiveMinute => intraday.push(row),
        }
    }

    let mut written = 0usize;
    for prefix in quote_write_order(cadence) {
        let rows = match prefix {
            BarInterval::OneDay => {
                settle_session_row(
                    s3_client, bucket, session, &daily, cadence, progress, agreement, authorship,
                )
                .await?
            }
            intraday_prefix => {
                write_intraday_partition(
                    s3_client,
                    bucket,
                    session,
                    &intraday,
                    intraday_prefix,
                    progress,
                    authorship,
                )
                .await?
            }
        };
        let Some(rows) = rows else {
            return Ok(());
        };
        written += rows;
    }

    progress.sessions_written += 1;
    progress.rows_written += written;
    Ok(())
}

/// Writes the session's intraday rows, returning how many landed.
///
/// `None` means the session failed and the caller should stop.
async fn write_intraday_partition(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
    intraday: &[QuoteSummary],
    interval: BarInterval,
    progress: &mut PassProgress,
    authorship: Authorship,
) -> Result<Option<usize>, ArchiveError> {
    if intraday.is_empty() {
        return Ok(Some(0));
    }
    let frame = quotes::summaries_to_dataframe(intraday)?;
    let key = date_partitioned_key(&quote_archive_prefix(interval), session.date());
    let survived =
        write_quote_partition(s3_client, bucket, session, key, frame, progress, authorship).await?;
    Ok(survived.then_some(intraday.len()))
}

/// Writes the session row, or checks it where one is already stored, returning the rows written.
///
/// `None` means the session failed and the caller should stop. A pass at a cadence that does not
/// author this row writes it only where none exists, since there is nothing to overwrite.
#[allow(clippy::too_many_arguments)]
async fn settle_session_row(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
    daily: &[QuoteSummary],
    cadence: IntradayCadence,
    progress: &mut PassProgress,
    agreement: &mut CadenceTotals,
    authorship: Authorship,
) -> Result<Option<usize>, ArchiveError> {
    if daily.is_empty() {
        return Ok(Some(0));
    }
    let key = date_partitioned_key(&quote_archive_prefix(BarInterval::OneDay), session.date());
    let stored = match cadence {
        cadence if cadence == DAILY_QUOTE_AUTHOR => None,
        _ => read_partition(s3_client, bucket, &key).await?,
    };
    let Some(stored) = stored else {
        let frame = quotes::summaries_to_dataframe(daily)?;
        let survived =
            write_quote_partition(s3_client, bucket, session, key, frame, progress, authorship)
                .await?;
        return Ok(survived.then_some(daily.len()));
    };

    check_session_row(session, daily, &stored, agreement, progress)?;
    Ok(Some(0))
}

/// Writes one quote partition, reporting whether the session survived it.
///
/// `false` means the session is left unsummarized at this cadence. A scope that skips sessions
/// already present will not return to it, so re-running is the operator's to decide.
async fn write_quote_partition(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
    key: String,
    frame: DataFrame,
    progress: &mut PassProgress,
    authorship: Authorship,
) -> Result<bool, ArchiveError> {
    match write_merged(
        s3_client,
        bucket,
        key,
        frame,
        |existing, fetched, key| merge_or_refuse(existing, fetched, key),
        DerivedDataset::Quotes,
        authorship,
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(ArchiveError::Contended { key, attempts }) => {
            warn!(key, attempts, %session, "Quote partition contended; this session was not summarized");
            progress.sessions_failed.push(session);
            Ok(false)
        }
        Err(error) => {
            warn!(%error, %session, "Quote partition write failed; this session was not summarized");
            progress.sessions_failed.push(session);
            Ok(false)
        }
    }
}

/// Compares a derived session row against the stored one, writing nothing either way.
///
/// Neither a disagreement nor an uncomparable pair stops the pass: the stored row belongs to another
/// cadence, and a multi-day backfill must not end over a figure it deliberately does not own.
fn check_session_row(
    session: SessionDate,
    derived: &[QuoteSummary],
    stored: &DataFrame,
    totals: &mut CadenceTotals,
    progress: &mut PassProgress,
) -> Result<(), ArchiveError> {
    let derived = quotes::summaries_to_dataframe(derived)?;
    let outcome =
        match CadenceCheck::quotes().compare(&derived, stored, BarInterval::OneDay, session) {
            Ok(outcome) => outcome,
            // Carried like a failed fold rather than raised. A stored row this pass cannot read is one
            // session's problem, and the intraday rows it did fold are still worth keeping.
            Err(error) => {
                warn!(%session, %error, "The stored session row could not be compared");
                totals.note(session, SessionOutcome::Unusable);
                progress.sessions_failed.push(session);
                return Ok(());
            }
        };
    if outcome.agrees() {
        info!(%outcome, "The folded session row agrees with the stored one");
    } else {
        warn!(%outcome, worst = ?outcome.worst().map(|(column, worst)| format!("{column}: {worst}")), "The folded session row disagrees with the stored one");
    }
    totals.absorb(&outcome);
    Ok(())
}

/// A summary family, which is a prefix and the column rules stored under it.
///
/// The two families differ in where they live and what reconstructs, and in nothing else — which is
/// what lets one check answer for both instead of being written twice and kept in step by hand.
#[derive(Clone, Copy, Debug)]
pub enum SummaryFamily {
    Quotes,
    Trades,
}

impl SummaryFamily {
    /// The same family seen as a session partition, which is where its prefix and name come from.
    ///
    /// Every summary family is stored per session; not every session family reconstructs from finer
    /// rows, which is why bars have no variant here. One mapping rather than two so the prefixes
    /// cannot drift apart.
    fn session_family(self) -> SessionFamily {
        match self {
            SummaryFamily::Quotes => SessionFamily::Quotes,
            SummaryFamily::Trades => SessionFamily::Trades,
        }
    }

    /// The archive prefix this family stores `interval` under.
    fn prefix(self, interval: BarInterval) -> String {
        self.session_family().prefix(interval)
    }

    /// Which columns a coarser row of this family reconstructs, and how.
    fn check(self) -> CadenceCheck {
        match self {
            SummaryFamily::Quotes => CadenceCheck::quotes(),
            SummaryFamily::Trades => CadenceCheck::trades(),
        }
    }

    /// The columns of this family no coarser row reconstructs, for a report to name.
    pub fn opaque_columns(self) -> &'static [&'static str] {
        self.check().opaque()
    }
}

impl std::fmt::Display for SummaryFamily {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.session_family().fmt(formatter)
    }
}

/// Folds each session's `finer` partition up to `coarser` and compares it to what is stored there.
///
/// Reads only, and needs no vendor credential: the population is what the finer prefix holds rather
/// than what a calendar publishes, so the check speaks for the archive as it is. One session at a
/// time, because a one-minute quote partition is millions of rows and several will not fit at once.
#[allow(clippy::too_many_arguments)]
pub async fn check_cadence_agreement(
    s3_client: &S3Client,
    bucket: &str,
    family: SummaryFamily,
    finer: BarInterval,
    coarser: BarInterval,
    window_start: SessionDate,
    window_end: SessionDate,
    stride: usize,
) -> Result<CadenceTotals, ArchiveError> {
    let check = family.check();
    let sessions: Vec<SessionDate> = present_partitions(
        s3_client,
        bucket,
        &family.prefix(finer),
        window_start,
        window_end,
    )
    .await?
    .into_iter()
    .step_by(stride.max(1))
    .collect();
    info!(
        %family,
        %finer,
        %coarser,
        window_start = %window_start,
        window_end = %window_end,
        stride,
        sessions = sessions.len(),
        "Planned a cross-cadence check"
    );

    let mut totals = CadenceTotals::default();
    for &session in &sessions {
        let finer_key = date_partitioned_key(&family.prefix(finer), session.date());
        let coarser_key = date_partitioned_key(&family.prefix(coarser), session.date());
        // The coarse read is what can be absent: the session list came from the finer prefix.
        let Some(coarse) = read_partition(s3_client, bucket, &coarser_key).await? else {
            // Absent, not disagreeing. A session missing a cadence is a coverage gap for the
            // backfill to answer, and counting it as a failed comparison would make a hole in one
            // prefix read as evidence that the other prefix is wrong. It is still not agreement,
            // so it is carried in the totals rather than logged beside them.
            totals.note(session, SessionOutcome::Absent);
            continue;
        };
        let Some(fine) = read_partition(s3_client, bucket, &finer_key).await? else {
            totals.note(session, SessionOutcome::Absent);
            continue;
        };
        let outcome = match check.compare(&fine, &coarse, coarser, session) {
            Ok(outcome) => outcome,
            // One unusable session, not the window. Ending here would discard every session already
            // compared and report nothing about the ones after it.
            Err(error) => {
                warn!(%session, %error, "A session could not be compared");
                totals.note(session, SessionOutcome::Unusable);
                continue;
            }
        };
        if outcome.agrees() {
            info!(%outcome, "Cadences agree");
        } else {
            warn!(%outcome, worst = ?outcome.worst().map(|(column, worst)| format!("{column}: {worst}")), "Cadences disagree");
        }
        totals.absorb(&outcome);
    }

    let (folded_only, stored_only) = totals.one_sided();
    info!(
        %family,
        %finer,
        %coarser,
        sessions_listed = sessions.len(),
        sessions_reached = totals.sessions(),
        sessions_agreeing = totals.sessions_agreeing(),
        rows_compared = totals.compared(),
        disagreements = totals.disagreements(),
        folded_only,
        stored_only,
        // Dated and reasoned, not counted: a count says a re-fold is needed and cannot say which
        // sessions to run it over, nor which of the four ways they failed to agree.
        unresolved = ?totals.unresolved(),
        opaque = ?check.opaque(),
        "Cross-cadence check complete"
    );
    Ok(totals)
}

/// Where a trade pass gets its prints.
///
/// The same split as [`QuoteSource`], and load-bearing for the same reason: a Massive flat file is
/// whole-market and the only affordable route to five years, while Alpaca answers one name at a time
/// and is the only route reaching past Massive's five-year window.
pub enum TradeSource<'a> {
    /// One request per name, retried per name.
    PerName(&'a MarketDataClient),
    /// One file per session, folded whole.
    WholeSession(&'a FlatFileClient),
}

/// The earliest session Alpaca's trade history reproduces faithfully.
///
/// Before this, Alpaca folds the opening auction print that the SIP also publishes as Market Center
/// Official Open, which our conditions table marks volume-ineligible, so a fold from that route
/// carries 0.3–2.2% of session volume the archive correctly excludes. Measured 2026-09-06: the
/// disagreement ends 2022-11-07 on CTA-tape names but runs intermittently into June 2023 on
/// Nasdaq-listed ones, and 2023-07-05 is the first session sampled clean across both.
const ALPACA_TRADES_FAITHFUL_FROM: (i32, u32, u32) = (2023, 7, 5);

impl TradeSource<'_> {
    /// Where this route's prints came from.
    ///
    /// Derived from the variant rather than passed beside it, so a session folded from Alpaca cannot
    /// be filed as having come from a flat file.
    pub const fn provenance(&self) -> Provenance {
        match self {
            TradeSource::PerName(_) => Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
            TradeSource::WholeSession(_) => RawDataset::Trades.provenance(),
        }
    }

    /// The earliest session this route may be written into the archive from.
    ///
    /// `None` for the flat-file route, which is where the archive's trade partitions came from and
    /// so cannot disagree with them. Some for Alpaca, whose early history differs — see
    /// [`ALPACA_TRADES_FAITHFUL_FROM`]. Carried on the source rather than checked at the call site
    /// because the constraint is a property of the provider, not of any one command.
    pub fn faithful_from(&self) -> Option<SessionDate> {
        match self {
            TradeSource::PerName(_) => {
                let (year, month, day) = ALPACA_TRADES_FAITHFUL_FROM;
                Some(SessionDate::from_date(
                    NaiveDate::from_ymd_opt(year, month, day)
                        .expect("the faithful-from date is a real calendar date"),
                ))
            }
            TradeSource::WholeSession(_) => None,
        }
    }

    /// The sessions in `sessions` this route must not be written into, earliest first.
    ///
    /// Returned rather than logged so the caller refuses before fetching anything. A silent partial
    /// skip would be worse than either refusing or proceeding: the pass would report success over a
    /// window it only half covered.
    pub fn unfaithful_sessions(&self, sessions: &[SessionDate]) -> Vec<SessionDate> {
        let Some(floor) = self.faithful_from() else {
            return Vec::new();
        };
        let mut refused: Vec<SessionDate> = sessions
            .iter()
            .copied()
            .filter(|&day| day < floor)
            .collect();
        refused.sort();
        refused
    }
}

impl std::fmt::Display for TradeSource<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            TradeSource::PerName(_) => "per-name",
            TradeSource::WholeSession(_) => "whole-session",
        })
    }
}

/// Folds the printed tape across `sessions`, per `scope`, from whichever route `source` names.
///
/// `sessions` must already be calendar-filtered on the same terms as the quote pass. A whole-session
/// fold treats a name absent from the file as absent from the tape, while a per-name fold treats it
/// as a fetch that can be retried — which is why the two report a failed symbol differently.
#[allow(clippy::too_many_arguments)]
pub async fn archive_trade_sessions(
    s3_client: &S3Client,
    source: &TradeSource<'_>,
    calendar: &TradingCalendar,
    bucket: &str,
    sessions: &[SessionDate],
    scope: &Scope,
    conditions: Arc<ConditionsTable>,
) -> Result<PassSummary, ArchiveError> {
    let (Some(first), Some(last)) = (sessions.first(), sessions.last()) else {
        return Ok(PassProgress::default().into_trade_summary(0));
    };
    // Presence is read off the daily prefix, which is the last one written, so a pass that died
    // partway reads as absent and is redone rather than left with its intraday half missing.
    let present = present_partitions(
        s3_client,
        bucket,
        &trade_archive_prefix(BarInterval::OneDay),
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
        "Planned a trade pass"
    );

    let mut progress = PassProgress {
        sessions_requested: requested.len(),
        ..Default::default()
    };
    // Taken rather than loaded here, so the caller that records the night names the very table the
    // fold ran under. A pass that loaded its own would leave the record asserting a fact about a
    // read it did not make.
    info!(conditions_as_of = %conditions.as_of(), "Folding the tape under the published conditions");

    let mut trades_folded = 0usize;
    for session in requested {
        trades_folded += archive_trade_session(
            s3_client,
            source,
            calendar,
            bucket,
            session,
            scope,
            Arc::clone(&conditions),
            &mut progress,
        )
        .await?;
    }

    if progress.symbols_failed > 0 {
        // The two routes fail differently, and the remedy differs with them: a flat file that does
        // not carry a name is answering, so re-running reads the same absence, while a per-name
        // fetch that failed is a request to make again.
        match source {
            TradeSource::WholeSession(_) => warn!(
                symbols_failed = progress.symbols_failed,
                "Some symbols in the universe never printed in these sessions' trade files; only a widen pass revisits them"
            ),
            TradeSource::PerName(_) => warn!(
                symbols_failed = progress.symbols_failed,
                "Some symbols' trade fetches failed; a repair pass naming them fetches them again"
            ),
        }
    }
    info!(
        sessions_requested = progress.sessions_requested,
        sessions_written = progress.sessions_written,
        sessions_without_data = progress.sessions_without_data,
        sessions_failed = progress.sessions_failed.len(),
        summaries_written = progress.rows_written,
        symbols_failed = progress.symbols_failed,
        trades_folded,
        "Trade archive updated"
    );
    Ok(progress.into_trade_summary(trades_folded))
}

/// One session: derive the universe from the scope, fold the tape, write all three cadences.
#[allow(clippy::too_many_arguments)]
async fn archive_trade_session(
    s3_client: &S3Client,
    source: &TradeSource<'_>,
    calendar: &TradingCalendar,
    bucket: &str,
    session: SessionDate,
    scope: &Scope,
    conditions: Arc<ConditionsTable>,
    progress: &mut PassProgress,
) -> Result<usize, ArchiveError> {
    let Some((open, close)) = quotes::trading_hours(calendar, session) else {
        progress.sessions_without_data += 1;
        return Ok(0);
    };

    let universe = match &scope.names {
        NameSelection::Named(symbols) => symbols.clone(),
        NameSelection::WholeMarket | NameSelection::Screened(_) => {
            let key =
                date_partitioned_key(&bar_archive_prefix(BarInterval::OneDay), session.date());
            let Some(daily) = read_partition(s3_client, bucket, &key).await? else {
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

    info!(%session, %open, %close, %source, universe = universe.len(), "Folding a session's printed tape");
    let requested = universe.len();
    // A tuple rather than a `SessionFolded` from both arms: `seen` is what the *file* carried, and
    // the per-name route has no equivalent -- it counts failed fetches directly, so a value invented
    // for it here would be read later as though a file had reported it.
    let (summaries, trades_folded) = match source {
        TradeSource::PerName(market_data) => {
            fold_trade_universe(
                market_data,
                session,
                open,
                close,
                &universe,
                conditions,
                progress,
            )
            .await
        }
        TradeSource::WholeSession(flat_files) => {
            let Some(fold) = trades::MarketFold::new(session, open, close, universe, conditions)
            else {
                // Unreachable while `trading_hours` returns a published session, and cheap to say so
                // rather than fold nothing and report the whole universe as its cost.
                warn!(%session, %open, %close, "The session spans no time; leaving it unsummarized");
                progress.sessions_without_data += 1;
                return Ok(0);
            };
            match flat_files.fold_trades(session.date(), fold).await {
                Ok((_, fold)) => {
                    let folded = fold.finish();
                    // Counted against what the file carried: a name that was there and never printed
                    // is not a failure, and counting it would make almost every whole-market session
                    // exit non-zero.
                    progress.symbols_failed += requested.saturating_sub(folded.seen);
                    if !folded.resumed.is_empty() {
                        info!(%session, resumed = folded.resumed.len(), "Names whose trades arrived in more than one run");
                    }
                    (folded.summaries, folded.folded)
                }
                Err(error) => {
                    // The file is the session: nothing partial survives it.
                    warn!(%session, %error, "A session's trade file could not be folded");
                    progress.sessions_failed.push(session);
                    return Ok(0);
                }
            }
        }
    };
    if summaries.is_empty() {
        progress.sessions_without_data += 1;
        return Ok(trades_folded);
    }
    write_trade_partitions(
        s3_client,
        bucket,
        session,
        summaries,
        progress,
        source.provenance(),
    )
    .await?;
    Ok(trades_folded)
}

/// Fans out over the universe, folding each name's prints and keeping the summaries and the cost.
///
/// A symbol's failure costs that symbol rather than the session, as on the quote path: the other
/// names are already fetched and discarding them would mean paying for them twice.
#[allow(clippy::too_many_arguments)]
async fn fold_trade_universe(
    market_data: &MarketDataClient,
    session: SessionDate,
    open: DateTime<Utc>,
    close: DateTime<Utc>,
    universe: &BTreeSet<Ticker>,
    conditions: Arc<ConditionsTable>,
    progress: &mut PassProgress,
) -> (Vec<TradeSummary>, usize) {
    let mut pending: Vec<Ticker> = universe.iter().cloned().collect();
    let mut tasks = tokio::task::JoinSet::new();
    let mut folded: Vec<TradeSummary> = Vec::new();
    let mut trades_folded = 0usize;

    loop {
        while tasks.len() < QUOTE_CONCURRENCY {
            let Some(ticker) = pending.pop() else { break };
            let client = market_data.clone();
            let conditions = Arc::clone(&conditions);
            tasks.spawn(async move {
                let mut last_error = None;
                for attempt in 0..QUOTE_SYMBOL_ATTEMPTS {
                    match trades::fold_session(
                        &client,
                        &ticker,
                        session,
                        open,
                        close,
                        Arc::clone(&conditions),
                    )
                    .await
                    {
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
                trades_folded += fetch.received;
                folded.extend(summaries);
            }
            Ok(Err((ticker, error))) => {
                progress.symbols_failed += 1;
                warn!(%ticker, %session, %error, "A symbol's trade fetch failed; continuing the session");
            }
            Err(error) => {
                progress.symbols_failed += 1;
                warn!(%error, %session, "A trade fold task did not complete");
            }
        }
    }
    (folded, trades_folded)
}

/// Writes one-minute bar partitions across `sessions`, per `scope`.
///
/// The flat-file counterpart to [`archive_intraday_sessions`], and simpler in one way that matters:
/// a bar row is already an output row, so there is no universe to derive and no fold to seed. The
/// file is the whole market, which is the reason to read it rather than 12,000 per-symbol requests.
pub async fn archive_bar_flat_file_sessions(
    s3_client: &S3Client,
    flat_files: &FlatFileClient,
    bucket: &str,
    sessions: &[SessionDate],
    scope: &Scope,
    calendar: Option<&TradingCalendar>,
) -> Result<PassSummary, ArchiveError> {
    let (Some(first), Some(last)) = (sessions.first(), sessions.last()) else {
        return Ok(PassProgress::default().into_bar_summary(calendar));
    };
    // One cadence, so presence is read off the prefix this pass writes. The trade and quote passes
    // read theirs off the daily prefix only because they write several and need the last one.
    let present = present_partitions(
        s3_client,
        bucket,
        &bar_archive_prefix(BarInterval::OneMinute),
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
        already_written = present.len(),
        requested = requested.len(),
        "Planned a one-minute bar pass"
    );

    let mut progress = PassProgress {
        sessions_requested: requested.len(),
        ..Default::default()
    };
    for session in requested {
        archive_bar_flat_file_session(s3_client, flat_files, bucket, session, &mut progress)
            .await?;
    }

    info!(
        sessions_requested = progress.sessions_requested,
        sessions_written = progress.sessions_written,
        sessions_without_data = progress.sessions_without_data,
        sessions_failed = progress.sessions_failed.len(),
        bars_written = progress.rows_written,
        "One-minute bar archive updated"
    );
    Ok(progress.into_bar_summary(calendar))
}

/// One session: fold the file into bars and write the single partition they form.
async fn archive_bar_flat_file_session(
    s3_client: &S3Client,
    flat_files: &FlatFileClient,
    bucket: &str,
    session: SessionDate,
    progress: &mut PassProgress,
) -> Result<(), ArchiveError> {
    let bars: Vec<EquityBar> = match flat_files.fold_bars(session.date(), Vec::new()).await {
        Ok((_, bars)) => bars,
        Err(error) => {
            // The file is the session: nothing partial survives it, exactly as for trades.
            warn!(%session, %error, "A session's bar file could not be folded");
            progress.sessions_failed.push(session);
            return Ok(());
        }
    };
    if bars.is_empty() {
        progress.sessions_without_data += 1;
        return Ok(());
    }

    // Through `write_partitions` rather than `write_partition`, for one session at a time: the
    // plural form is where contention and write faults are carried instead of ending the pass.
    write_partitions(
        s3_client,
        bucket,
        BarInterval::OneMinute,
        vec![(session, bars)],
        progress,
        RawDataset::MinuteAggregates.provenance(),
    )
    .await
}

/// Writes a session's trade summaries, one partition per cadence, daily last.
///
/// The order is the recovery rule, matching the quote pass: presence is read off the daily prefix,
/// so a pass that dies partway must leave the session looking absent.
async fn write_trade_partitions(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
    folded: Vec<TradeSummary>,
    progress: &mut PassProgress,
    provenance: Provenance,
) -> Result<(), ArchiveError> {
    let mut one_minute: Vec<TradeSummary> = Vec::new();
    let mut five_minute: Vec<TradeSummary> = Vec::new();
    let mut daily: Vec<TradeSummary> = Vec::new();
    for row in folded {
        match row.bar_interval() {
            BarInterval::OneMinute => one_minute.push(row),
            BarInterval::FiveMinute => five_minute.push(row),
            BarInterval::OneDay => daily.push(row),
        }
    }

    let mut written = 0usize;
    for (interval, rows) in [
        (BarInterval::OneMinute, one_minute),
        (BarInterval::FiveMinute, five_minute),
        (BarInterval::OneDay, daily),
    ] {
        if rows.is_empty() {
            continue;
        }
        let frame = trades::summaries_to_dataframe(&rows)?;
        let key = date_partitioned_key(&trade_archive_prefix(interval), session.date());
        match write_merged(
            s3_client,
            bucket,
            key,
            frame,
            |existing, fetched, key| merge_or_refuse(existing, fetched, key),
            DerivedDataset::Trades,
            // Carried from the source rather than named here: a repair folds Alpaca into a partition
            // a flat file built, and filing that as Massive-only would hide the provider seam in the
            // one record that exists to expose it.
            Authorship::refusing(provenance),
        )
        .await
        {
            Ok(()) => written += rows.len(),
            Err(ArchiveError::Contended { key, attempts }) => {
                warn!(key, attempts, %session, "Trade partition contended; this session was not summarized");
                progress.sessions_failed.push(session);
                return Ok(());
            }
            Err(error) => {
                warn!(%error, %session, "Trade partition write failed; this session was not summarized");
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
        |existing, fetched, key| merge_splits_or_refuse(existing, fetched, key),
        DerivedDataset::Splits,
        Authorship::refusing(Provenance::massive(
            MassivePlan::StocksStarter,
            MassiveTransport::Rest,
        )),
    )
    .await?;

    info!(
        key = SPLITS_ARCHIVE_KEY,
        splits = splits.len(),
        "Splits table archived"
    );
    Ok(splits.len())
}

/// Every symbol that traded on `session`, read from the daily bar partition.
///
/// This is the archive's own membership list and the reason the reference sweep needs no separate
/// membership fetch: a bar exists for an instrument that traded, which is the operational form of
/// "was listed". Measured on 2021-09-15 it agreed with the vendor's listing on 10,199 of 10,207
/// symbols, the residual being the exchange test tickers.
pub async fn session_symbols(
    s3_client: &S3Client,
    bucket: &str,
    session: SessionDate,
) -> Result<Vec<Ticker>, ArchiveError> {
    let key = date_partitioned_key(&bar_archive_prefix(BarInterval::OneDay), session.date());
    let Some(frame) = read_partition(s3_client, bucket, &key).await? else {
        return Ok(Vec::new());
    };

    let column = frame.column("ticker")?.str()?;
    let mut symbols: BTreeSet<Ticker> = BTreeSet::new();
    let mut unreadable = 0usize;
    for row in 0..frame.height() {
        match column.get(row).and_then(Ticker::new) {
            Some(ticker) => {
                symbols.insert(ticker);
            }
            None => unreadable += 1,
        }
    }
    if unreadable > 0 {
        // Counted rather than silent: a partition holding names no validator accepts is a finding
        // about the fold, not a routine skip.
        warn!(
            key,
            unreadable,
            rows = frame.height(),
            "Bar partition holds symbols that are not usable tickers"
        );
    }
    Ok(symbols.into_iter().collect())
}

/// Symbols fetched at once when sweeping a reference date.
///
/// Measured against the live endpoint 2026-09-16: sequential is 18 requests a second and the feed
/// does not throttle, giving 157 at eight, 278 at sixteen and 504 at thirty-two, all successful.
/// Thirty-two turns a ten-thousand-symbol sweep into about twenty seconds; higher was not probed,
/// so this is the fastest rate observed rather than the fastest available.
const REFERENCE_FETCH_CONCURRENCY: usize = 32;

/// Fetches the reference record for every symbol in `tickers` as of `as_of` and writes one partition.
///
/// The symbol list belongs to the caller because the archive's own bar index is the membership list:
/// a name with a bar traded that session, so the feed having no record for it is a finding rather
/// than a routine miss. Both kinds of silence are counted and returned rather than logged away.
pub async fn archive_reference(
    s3_client: &S3Client,
    massive: &MassiveClient,
    bucket: &str,
    as_of: SessionDate,
    tickers: &[Ticker],
) -> Result<reference::ReferenceSweep, ArchiveError> {
    let mut sweep = reference::ReferenceSweep {
        requested: tickers.len(),
        ..Default::default()
    };
    let mut references: Vec<EquityReference> = Vec::with_capacity(tickers.len());
    let mut in_flight = tokio::task::JoinSet::new();
    let mut queued = tickers.iter();

    loop {
        while in_flight.len() < REFERENCE_FETCH_CONCURRENCY {
            let Some(ticker) = queued.next() else { break };
            let (client, ticker) = (massive.clone(), ticker.clone());
            in_flight.spawn(async move {
                let outcome = client.fetch_reference(&ticker, as_of).await;
                (ticker, outcome)
            });
        }
        let Some(joined) = in_flight.join_next().await else {
            break;
        };

        // A panicked task is neither an absence nor a feed failure, so it is not folded into either.
        let (ticker, outcome) = joined.map_err(|error| ArchiveError::Feed {
            vendor: "Massive",
            message: format!("a reference fetch task did not complete: {error}"),
        })?;

        match outcome {
            Ok(Some(found)) => {
                sweep.found += 1;
                references.push(found);
            }
            Ok(None) => sweep.absent.push(ticker.as_str().to_string()),
            Err(error) => {
                warn!(%ticker, %as_of, %error, "Reference fetch failed for a symbol");
                sweep.failed.push(reference::ReferenceFailure {
                    ticker: ticker.as_str().to_string(),
                    reason: error.to_string(),
                });
            }
        }
    }

    // A claim replaces the partition, so a sweep that could not ask every symbol would overwrite a
    // complete stored answer with a shorter one. Refused rather than merged: a retry is cheap.
    if !sweep.failed.is_empty() {
        warn!(
            %as_of,
            requested = sweep.requested,
            found = sweep.found,
            failed = sweep.failed.len(),
            "Reference sweep left symbols unanswered; keeping the stored partition"
        );
        return Ok(sweep);
    }

    // Refused before the write for the reason `archive_splits` gives: a feed answering success with
    // nothing is an outage, and an empty partition would record it as a date with no instruments.
    if references.is_empty() {
        warn!(
            %as_of,
            requested = sweep.requested,
            "Reference sweep found nothing; writing no partition"
        );
        return Ok(sweep);
    }

    // Sorted so two sweeps of the same date produce byte-comparable partitions, which is what makes
    // a re-run checkable against its predecessor.
    references.sort_by(|left, right| left.ticker().as_str().cmp(right.ticker().as_str()));

    let key = date_partitioned_key(REFERENCE_ARCHIVE_PREFIX, as_of.date());
    let frame = reference::reference_to_dataframe(&references)?;
    write_merged(
        s3_client,
        bucket,
        key.clone(),
        frame,
        // A claim rather than a merge, which the guard above is what licenses: every symbol was
        // answered for, so these rows are the whole answer and a stored row absent from them is stale.
        |_existing, fetched, _key| Ok(fetched),
        DerivedDataset::Reference,
        Authorship::claiming(Provenance::massive(
            MassivePlan::StocksStarter,
            MassiveTransport::Rest,
        )),
    )
    .await?;

    info!(
        key,
        %as_of,
        requested = sweep.requested,
        found = sweep.found,
        absent = sweep.absent.len(),
        failed = sweep.failed.len(),
        "Reference partition archived"
    );
    Ok(sweep)
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
        DerivedDataset::Boundaries,
        Authorship::refusing(Provenance::alpaca(AlpacaPlan::AlgoTraderPlus)),
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

/// Reads one archived partition, distinguishing a missing object from a failed request.
///
/// `Ok(None)` means the partition genuinely does not exist yet; every other failure propagates,
/// because a throttle silently treated as "missing" would shorten the training window with no
/// signal. A writer wants [`read_partition_with_etag`], whose ETag makes its write conditional.
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

/// What was found at a sidecar key.
///
/// Three-way rather than an `Option` because absent and unreadable pull the two callers opposite
/// ways: the guard must refuse a record it cannot interpret, and the writer must not spend its
/// retries on one.
enum SidecarRead {
    /// The record parsed, and carried this ETag when it was read.
    Found(PartitionProvenance, String),
    /// No object at the key.
    Absent,
    /// An object is there and did not parse, so it can neither be trusted nor replaced.
    Unreadable,
}

/// Reads a partition's provenance and the ETag it carried.
///
/// A failed request is neither absent nor unreadable and is propagated: treating a throttle as "no
/// record" is what lets a single-route write replace a multi-route one.
async fn read_sidecar(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<SidecarRead, ArchiveError> {
    let response = match s3_client.get_object().bucket(bucket).key(key).send().await {
        Ok(response) => response,
        Err(error) => {
            return match error.into_service_error() {
                GetObjectError::NoSuchKey(_) => Ok(SidecarRead::Absent),
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
    Ok(match serde_json::from_slice(&bytes) {
        Ok(record) => SidecarRead::Found(record, etag),
        Err(_) => SidecarRead::Unreadable,
    })
}

/// Records where an object's bytes came from, beside the object itself.
///
/// Returns why it gave up rather than swallowing it, because what a failure costs depends on the
/// write: an ordinary fold loses a record the sweep rebuilds, and a claim loses the only statement
/// that the partition changed hands. The write is conditional, so a repair racing a bulk walk
/// retries against what the other wrote instead of discarding it.
async fn write_sidecar(
    s3_client: &S3Client,
    bucket: &str,
    object_key: &str,
    dataset: DerivedDataset,
    authorship: Authorship,
) -> Result<(), String> {
    let key = PartitionProvenance::sidecar_key(object_key);
    let session = session_from_key(object_key, "data.parquet").map(|at| at.to_string());
    let provenance = authorship.provenance;
    let fresh = || PartitionProvenance::new(dataset.as_str(), session.as_deref(), provenance);

    for attempt in 1..=CONTENDED_WRITE_ATTEMPTS {
        let existing = match read_sidecar(s3_client, bucket, &key).await {
            Ok(existing) => existing,
            // Never overwrite a record that could not be read: guessing "absent" loses a route.
            Err(error) => return Err(format!("the record could not be read: {error}")),
        };
        let (record, precondition) = match existing {
            // A claim rewrites the record rather than adding to it: the other route's rows are
            // gone, and the sidecar is what every provenance query reads.
            SidecarRead::Found(_, etag) if authorship.claims() => {
                (fresh(), Precondition::Match(etag))
            }
            SidecarRead::Found(record, etag) => {
                (record.contributed(provenance), Precondition::Match(etag))
            }
            SidecarRead::Absent => (fresh(), Precondition::Absent),
            // An `Absent` precondition cannot replace an object that is really there, so retrying
            // would 409 until the attempts ran out under a warning naming the wrong cause.
            SidecarRead::Unreadable => {
                return Err("the record did not parse; the sweep must rewrite it".to_string())
            }
        };
        let body = match serde_json::to_vec(&record) {
            Ok(body) => body,
            Err(error) => return Err(format!("the record did not serialize: {error}")),
        };
        match put_object_with_precondition(
            s3_client,
            bucket,
            &key,
            body,
            "application/json",
            &precondition,
        )
        .await
        {
            WriteOutcome::Written => return Ok(()),
            WriteOutcome::Contended => {
                warn!(
                    key,
                    attempt, "Provenance changed under a write; merging again"
                )
            }
            WriteOutcome::Failed(message) => return Err(message),
        }
    }
    Err(format!("the record lost {CONTENDED_WRITE_ATTEMPTS} races"))
}

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
    put_object_with_precondition(
        s3_client,
        bucket,
        key,
        body,
        "application/vnd.apache.parquet",
        precondition,
    )
    .await
}

/// The conditional write itself, over any content type.
///
/// Shared with the provenance sidecar, whose contention window is the same one partitions have: two
/// routes touching a session at once must merge rather than overwrite.
async fn put_object_with_precondition(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    content_type: &str,
    precondition: &Precondition,
) -> WriteOutcome {
    let request = s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .content_type(content_type);

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
fn merge_partitions(existing: DataFrame, fetched: DataFrame) -> Result<DataFrame, PolarsError> {
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    fn as_of(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).expect("a valid date"))
    }

    /// The quarterly grid the backfill actually wrote, so the selection is exercised against real
    /// spacing rather than a convenient one.
    fn grid() -> Vec<SessionDate> {
        vec![
            as_of(2021, 8, 23),
            as_of(2021, 10, 1),
            as_of(2022, 1, 3),
            as_of(2022, 4, 1),
            as_of(2022, 7, 1),
        ]
    }

    /// A window opening mid-quarter must reach back for the observation that answers for its first
    /// sessions. Taking only the observations inside the window leaves every name unclassified
    /// until the next quarter begins, which reads as a thin archive rather than a bad read.
    #[test]
    fn test_the_window_opens_at_the_nearest_observation_at_or_before_it() {
        let covering = covering_partitions(&grid(), as_of(2021, 11, 15), as_of(2022, 2, 10));

        assert_eq!(covering, vec![as_of(2021, 10, 1), as_of(2022, 1, 3)]);
    }

    #[test]
    fn test_an_observation_landing_exactly_on_the_start_is_the_opening_one() {
        let covering = covering_partitions(&grid(), as_of(2022, 1, 3), as_of(2022, 4, 1));

        assert_eq!(covering, vec![as_of(2022, 1, 3), as_of(2022, 4, 1)]);
    }

    /// Nothing precedes the window, so the opening sessions genuinely cannot be classified. The
    /// earliest available is used rather than none, and the bars before it are dropped by the join.
    #[test]
    fn test_a_window_starting_before_every_observation_takes_the_earliest() {
        let covering = covering_partitions(&grid(), as_of(2020, 1, 2), as_of(2021, 10, 1));

        assert_eq!(covering, vec![as_of(2021, 8, 23), as_of(2021, 10, 1)]);
    }

    #[test]
    fn test_a_window_after_every_observation_takes_only_the_last() {
        let covering = covering_partitions(&grid(), as_of(2026, 9, 1), as_of(2026, 9, 17));

        assert_eq!(covering, vec![as_of(2022, 7, 1)]);
    }

    #[test]
    fn test_an_empty_archive_covers_nothing() {
        assert!(covering_partitions(&[], as_of(2022, 1, 3), as_of(2022, 4, 1)).is_empty());
    }

    use crate::common::alpaca::TradeTick;
    use crate::common::types::TradeConditions;
    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;
    use chrono::NaiveDate;
    use percent_encoding::percent_decode_str;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::from_ymd_opt(year, month, day).expect("test date must be valid"),
        )
    }

    /// Builds an Alpaca route with throwaway credentials, for the checks that never make a request.
    fn per_name_credentials() -> crate::common::alpaca::MarketDataClient {
        let credentials = crate::common::alpaca::AlpacaCredentials::new(
            "test-key".to_string(),
            "test-secret".to_string(),
        )
        .expect("test credentials must construct");
        MarketDataClient::new(credentials, crate::common::alpaca::DataFeed::Sip)
    }

    /// A second provider writing into a partition the first built is what produced quote cadences
    /// that disagree about the same name-session by a factor of thirty.
    ///
    /// Refused rather than merged, because after the concatenation nothing can say which row came
    /// from which vendor: the sidecar records a set of routes for the partition, never a route per
    /// row.
    #[tokio::test]
    async fn test_a_second_provider_is_refused_rather_than_merged() {
        let record = PartitionProvenance::new(
            "equity_quotes",
            Some("2022-01-04"),
            Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile),
        );
        let body = serde_json::to_string(&record).expect("the record must serialize");
        let client = scripted_s3_client(move |_method, _key| {
            http::Response::builder()
                .status(200)
                .header("etag", "\"an-etag\"")
                .body(SdkBody::from(body.clone()))
                .expect("a canned response must build")
        });

        let refused = refuse_a_second_provider(
            &client,
            "test-bucket",
            "data/derived/equity/quotes/interval=one_day/year=2022/month=01/day=04/data.parquet",
            Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
        )
        .await;

        match refused {
            Err(ArchiveError::MixedProvenance {
                existing, incoming, ..
            }) => {
                assert_eq!(existing, "massive");
                assert_eq!(incoming, "alpaca");
            }
            other => panic!("a second provider must be refused, got {other:?}"),
        }
    }

    /// A record that does not parse names no provider, and "could not tell" must not be spent as
    /// "safe" — the write is refused so the sweep can rewrite the sidecar first.
    #[tokio::test]
    async fn test_an_unreadable_provenance_record_is_refused_rather_than_assumed_absent() {
        let client = scripted_s3_client(move |_method, _key| {
            http::Response::builder()
                .status(200)
                .header("etag", "\"an-etag\"")
                .body(SdkBody::from("{ this is not the record it claims to be"))
                .expect("a canned response must build")
        });

        let refused = refuse_a_second_provider(
            &client,
            "test-bucket",
            "data/derived/equity/quotes/interval=one_day/year=2022/month=01/day=04/data.parquet",
            Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
        )
        .await;

        match refused {
            Err(ArchiveError::Read { key, .. }) => assert_eq!(
                key,
                "data/derived/equity/quotes/interval=one_day/year=2022/month=01/day=04/\
                 data.parquet.provenance.json"
            ),
            other => panic!("an unreadable record must be refused, got {other:?}"),
        }
    }

    /// The same provider re-folding its own partition is ordinary and must stay allowed — that is
    /// what the re-fold from the raw tee will be.
    #[tokio::test]
    async fn test_the_same_provider_may_rewrite_its_own_partition() {
        let record = PartitionProvenance::new(
            "equity_quotes",
            Some("2022-01-04"),
            Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile),
        );
        let body = serde_json::to_string(&record).expect("the record must serialize");
        let client = scripted_s3_client(move |_method, _key| {
            http::Response::builder()
                .status(200)
                .header("etag", "\"an-etag\"")
                .body(SdkBody::from(body.clone()))
                .expect("a canned response must build")
        });

        // A different Massive plan and transport, deliberately: what must match is the vendor, not
        // the route, so a Starter REST re-fold over an Advanced flat-file partition is allowed.
        refuse_a_second_provider(
            &client,
            "test-bucket",
            "data/derived/equity/quotes/interval=one_day/year=2022/month=01/day=04/data.parquet",
            Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest),
        )
        .await
        .expect("one vendor rewriting its own partition is not a mix");
    }

    /// Alpaca's early history carries the opening auction print the archive excludes, so a repair
    /// reaching back that far would add 0.3-2.2% of session volume, silently and per name.
    ///
    /// Pinned to the literal date rather than read off `ALPACA_TRADES_FAITHFUL_FROM`, so moving the
    /// constant has to move this too and cannot quietly widen what a repair may overwrite.
    #[test]
    fn test_the_alpaca_trade_route_refuses_sessions_before_its_floor() {
        let market_data = per_name_credentials();
        let source = TradeSource::PerName(&market_data);

        assert_eq!(source.faithful_from(), Some(session(2023, 7, 5)));

        let refused = source.unfaithful_sessions(&[
            session(2022, 1, 4),
            session(2023, 6, 15),
            session(2023, 7, 5),
            session(2026, 8, 21),
        ]);
        assert_eq!(
            refused,
            vec![session(2022, 1, 4), session(2023, 6, 15)],
            "every session before the floor is refused, and the floor itself is not"
        );
    }

    /// The flat file is where the archive's trade partitions came from, so it cannot disagree with
    /// them and must never be refused — a floor on this route would block the only repair that
    /// survives the Massive Advanced lapse.
    #[test]
    fn test_the_flat_file_trade_route_has_no_floor() {
        let credentials = crate::common::flatfiles::FlatFileCredentials::new(
            "https://example.invalid".to_string(),
            "test-key".to_string(),
            "test-secret".to_string(),
        )
        .expect("test credentials must construct");
        let flat_files = FlatFileClient::new(credentials);
        let source = TradeSource::WholeSession(&flat_files);

        assert_eq!(source.faithful_from(), None);
        assert!(source
            .unfaithful_sessions(&[session(2021, 8, 26), session(2022, 1, 4)])
            .is_empty());
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

    /// As [`scripted_s3_client`], but the responder also sees the request body.
    ///
    /// Needed where the assertion is about what was *written* rather than where: a provenance record
    /// is only checkable by reading the bytes it put.
    fn scripted_s3_client_capturing(
        respond: impl Fn(&http::Method, &str, &str) -> http::Response<SdkBody> + Send + Sync + 'static,
    ) -> S3Client {
        let http_client = infallible_client_fn(move |request| {
            let method = request.method().clone();
            let key = percent_decode_str(request.uri().path()).decode_utf8_lossy();
            let body = request
                .body()
                .bytes()
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .unwrap_or_default();
            respond(&method, &key, &body)
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

    /// A one-row frame carrying a name, which is all a replace test needs to tell two apart.
    fn one_frame(ticker_symbol: &str) -> DataFrame {
        crate::data::bars::bars_to_dataframe(&[one_bar(ticker_symbol, session(2024, 1, 25))])
            .expect("a single bar must frame")
    }

    /// One name's session, folded from a single ordinary print.
    fn one_trade_summary(ticker_symbol: &str, at: SessionDate) -> Vec<TradeSummary> {
        let open = at.midnight();
        let close = open + chrono::Duration::minutes(5);
        let mut fold = crate::data::trades::SessionFold::new(
            Ticker::new(ticker_symbol).expect("a test ticker must parse"),
            at,
            open,
            close,
            std::sync::Arc::new(crate::data::conditions::fixture::table()),
        )
        .expect("a positive session must open");
        fold.push(
            TradeTick::new(
                open + chrono::Duration::seconds(1),
                100.0,
                10.0,
                TradeConditions::Identified(Vec::new()),
                false,
            )
            .expect("an ordinary print must construct"),
        );
        fold.finish()
    }

    /// A partition records the route that actually built it, not a hard-coded one.
    ///
    /// Trades have two routes, the flat-file fold and the Alpaca per-name repair, so provenance has
    /// to come from [`TradeSource::provenance`]: filing an `equity-trades repair` under Massive
    /// would hide the provider seam in the one record kept to expose it.
    #[tokio::test]
    async fn test_a_trade_partition_is_attributed_to_the_route_that_built_it() {
        let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&bodies);
        let client = scripted_s3_client_capturing(move |method, key, body| {
            if method == http::Method::PUT && key.ends_with(".provenance.json") {
                recorder
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(body.to_string());
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
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

        let at = session(2026, 9, 3);
        let mut progress = PassProgress::default();
        let credentials = crate::common::alpaca::AlpacaCredentials::new(
            "test-key".to_string(),
            "test-secret".to_string(),
        )
        .expect("test credentials must construct");
        let market_data = MarketDataClient::new(credentials, crate::common::alpaca::DataFeed::Sip);
        write_trade_partitions(
            &client,
            "test-bucket",
            at,
            one_trade_summary("AAPL", at),
            &mut progress,
            TradeSource::PerName(&market_data).provenance(),
        )
        .await
        .expect("the partition must be written");

        let written = bodies
            .lock()
            .expect("the recorder must not be poisoned")
            .clone();
        assert!(!written.is_empty(), "a sidecar must have been written");
        // Pinned to the literal strings the record carries on the wire, not to `Provenance`'s own
        // accessors, so a rename that moved both together would still fail here.
        for record in &written {
            assert!(
                record.contains("\"provider\":\"alpaca\""),
                "a per-name fold is Alpaca's; wrote {record}"
            );
            assert!(
                !record.contains("\"provider\":\"massive\""),
                "a per-name fold must not be filed as a flat file; wrote {record}"
            );
        }
    }

    /// Each cadence lands under its own hive partition, and trades never collide with quotes.
    ///
    /// Pinned to literal keys because these are the archive's wire layout: a reader scanning the
    /// tree gets `interval` as a column, and two cadences sharing a key would make whichever job
    /// wrote last the one that mattered.
    #[test]
    fn test_every_trade_cadence_has_its_own_partition_prefix() {
        let session = NaiveDate::from_ymd_opt(2026, 8, 21).expect("a real date");
        let key = |interval| date_partitioned_key(&trade_archive_prefix(interval), session);

        assert_eq!(
            key(BarInterval::OneMinute),
            "data/derived/equity/trades/interval=one_minute/year=2026/month=08/day=21/data.parquet"
        );
        assert_eq!(
            key(BarInterval::FiveMinute),
            "data/derived/equity/trades/interval=five_minute/year=2026/month=08/day=21/data.parquet"
        );
        assert_eq!(
            key(BarInterval::OneDay),
            "data/derived/equity/trades/interval=one_day/year=2026/month=08/day=21/data.parquet"
        );

        // Disjoint from the quote archive at every cadence, so one session's two datasets cannot
        // overwrite each other.
        for interval in BarInterval::ALL {
            assert_ne!(
                trade_archive_prefix(interval),
                quote_archive_prefix(interval),
                "{interval} must not share a prefix with the quote archive"
            );
        }
    }

    /// A written partition must leave a provenance sidecar beside it, naming the route.
    ///
    /// The sidecar is the only durable answer to "where did this session come from": the prefix
    /// rewrite of 2026-09-03 reset `LastModified` across the whole archive, so object metadata
    /// cannot date or attribute a partition, and the summary itself carries no vendor.
    #[tokio::test]
    async fn test_a_written_partition_records_where_its_bytes_came_from() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let client = scripted_s3_client(move |method, key| {
            if method == http::Method::PUT {
                recorder
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(key.to_string());
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .expect("a canned response must build")
            } else {
                no_such_key()
            }
        });

        let at = session(2026, 9, 3);
        let mut progress = PassProgress::default();
        write_partitions(
            &client,
            "test-bucket",
            BarInterval::FiveMinute,
            vec![(at, vec![one_bar("AAPL", at)])],
            &mut progress,
            Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest),
        )
        .await
        .expect("the partition must be written");

        let written = seen
            .lock()
            .expect("the recorder must not be poisoned")
            .clone();
        // Pinned to the literal rather than derived from the constant under test.
        assert!(
            written.iter().any(|key| key.ends_with(
                "data/derived/equity/bars/interval=five_minute/year=2026/month=09/day=03/data.parquet.provenance.json"
            )),
            "a sidecar must sit beside the partition; wrote {written:?}"
        );
    }

    /// A claim rewrites the provenance record instead of adding to it.
    ///
    /// The union is right for every other write and wrong for this one: the rows the other route
    /// produced are gone, so a record still naming it describes a partition that no longer exists.
    /// It is the sidecar rather than the parquet that every provenance query reads, so a stale one
    /// leaves the re-fold looking like it did nothing.
    #[tokio::test]
    async fn test_a_claim_rewrites_the_provenance_record_rather_than_joining_it() {
        let mixed = PartitionProvenance::new(
            "equity_quotes",
            Some("2024-01-25"),
            Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile),
        )
        .contributed(Provenance::alpaca(AlpacaPlan::AlgoTraderPlus));
        let body = serde_json::to_string(&mixed).expect("the record must serialize");
        let written: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&written);
        let client = scripted_s3_client_capturing(move |method, _key, put| {
            if method == http::Method::PUT {
                recorder
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(put.to_string());
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .expect("a canned response must build")
            } else {
                http::Response::builder()
                    .status(200)
                    .header("etag", "\"an-etag\"")
                    .body(SdkBody::from(body.clone()))
                    .expect("a canned response must build")
            }
        });

        let key =
            "data/derived/equity/quotes/interval=one_day/year=2024/month=01/day=25/data.parquet";
        write_sidecar(
            &client,
            "test-bucket",
            key,
            DerivedDataset::Quotes,
            Authorship::claiming(Provenance::massive(
                MassivePlan::StocksAdvanced,
                MassiveTransport::FlatFile,
            )),
        )
        .await;

        let records = written
            .lock()
            .expect("the recorder must not be poisoned")
            .clone();
        assert_eq!(records.len(), 1, "one record must be written: {records:?}");
        let rewritten: PartitionProvenance =
            serde_json::from_str(&records[0]).expect("the record must parse");
        let providers: Vec<&str> = rewritten
            .routes
            .iter()
            .map(|route| route.provider_name())
            .collect();
        assert_eq!(
            providers,
            vec!["massive"],
            "alpaca must not survive a claim"
        );
    }

    /// A claim discards the stored rows, and does so without asking the merge how to combine them.
    ///
    /// The merge is what a `widen` uses to keep everything either side holds, which is precisely
    /// wrong here -- it would preserve the rows the re-fold exists to remove. Asserted by handing in
    /// a merge that panics, so "not called" is proven rather than inferred from the output.
    #[tokio::test]
    async fn test_a_claim_replaces_the_stored_rows_without_merging_them() {
        let mut stored = one_frame("OLD");
        let mut existing: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut existing)
            .finish(&mut stored)
            .expect("the stored partition must serialize");

        let conditions: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&conditions);
        let client = scripted_s3_client(move |method, key| {
            if method == http::Method::PUT {
                recorder
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(key.to_string());
                return http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .expect("a canned response must build");
            }
            if key.ends_with("provenance.json") {
                return no_such_key();
            }
            http::Response::builder()
                .status(200)
                .header("etag", "\"an-etag\"")
                .body(SdkBody::from(existing.clone()))
                .expect("a canned response must build")
        });

        write_merged(
            &client,
            "test-bucket",
            "data/derived/equity/quotes/interval=one_day/year=2024/month=01/day=25/data.parquet"
                .to_string(),
            one_frame("NEW"),
            |_existing, _fetched, _key| panic!("a claim must not consult the merge"),
            DerivedDataset::Quotes,
            Authorship::claiming(Provenance::massive(
                MassivePlan::StocksAdvanced,
                MassiveTransport::FlatFile,
            )),
        )
        .await
        .expect("the claim must be written");

        assert_eq!(
            conditions
                .lock()
                .expect("the recorder must not be poisoned")
                .len(),
            2,
            "the partition and its sidecar"
        );
    }

    /// A claim whose record cannot be written must not report success; an ordinary write still may.
    ///
    /// The asymmetry is the point. An ordinary fold that loses its sidecar loses a note the sweep
    /// rebuilds, while a claim that loses its sidecar leaves rows that changed hands under a record
    /// still naming the provider they came from — which reads downstream as a re-fold that never ran.
    #[tokio::test]
    async fn test_only_a_claim_fails_when_its_provenance_record_cannot_be_written() {
        let outcome = |authorship: Authorship| async move {
            let client = scripted_s3_client(move |method, key| {
                // The sidecar alone refuses, so the partition lands and only the record fails.
                if method == http::Method::PUT && key.ends_with("provenance.json") {
                    return http::Response::builder()
                        .status(500)
                        .body(SdkBody::from("<Error><Code>InternalError</Code></Error>"))
                        .expect("a canned response must build");
                }
                if method == http::Method::PUT {
                    return http::Response::builder()
                        .status(200)
                        .body(SdkBody::empty())
                        .expect("a canned response must build");
                }
                no_such_key()
            });
            write_merged(
                &client,
                "test-bucket",
                "data/derived/equity/quotes/interval=one_day/year=2024/month=01/day=25/data.parquet"
                    .to_string(),
                one_frame("NEW"),
                |_existing, fetched, _key| Ok(fetched),
                DerivedDataset::Quotes,
                authorship,
            )
            .await
        };

        let massive = Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile);
        assert!(
            outcome(Authorship::claiming(massive)).await.is_err(),
            "a claim must not report success over an unwritten record"
        );
        assert!(
            outcome(Authorship::refusing(massive)).await.is_ok(),
            "an ordinary fold must survive a lost sidecar"
        );
    }

    /// A failed read must never become an overwrite.
    ///
    /// `read_sidecar` once mapped every error to "absent", so a throttle or a 503 made the next write
    /// build a fresh single-route record over a multi-route one -- the exact loss the set exists to
    /// prevent. The record is only ever replaced by one that read the old one first.
    #[tokio::test]
    async fn test_a_sidecar_that_cannot_be_read_is_not_overwritten() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let client = scripted_s3_client(move |method, key| {
            if method == http::Method::PUT {
                recorder
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(key.to_string());
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .expect("a canned response must build")
            } else {
                // Throttled, not absent. A 503 is the vendor saying "ask again", not "there is
                // nothing here".
                http::Response::builder()
                    .status(503)
                    .body(SdkBody::from("<Error><Code>SlowDown</Code></Error>"))
                    .expect("a canned response must build")
            }
        });

        write_sidecar(
            &client,
            "test-bucket",
            SPLITS_ARCHIVE_KEY,
            DerivedDataset::Splits,
            Authorship::refusing(Provenance::massive(
                MassivePlan::StocksStarter,
                MassiveTransport::Rest,
            )),
        )
        .await;

        let written = seen
            .lock()
            .expect("the recorder must not be poisoned")
            .clone();
        assert!(
            written.is_empty(),
            "a sidecar that could not be read must be left alone; wrote {written:?}"
        );
    }

    /// A record that does not parse is left for the sweep rather than retried against.
    ///
    /// An `Absent` precondition cannot replace an object that is really there, so a lenient read
    /// spends every attempt on writes that can only 409, then warns about losing a race.
    #[tokio::test]
    async fn test_a_sidecar_that_does_not_parse_is_left_for_the_sweep() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let client = scripted_s3_client(move |method, key| {
            if method == http::Method::PUT {
                recorder
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(key.to_string());
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .expect("a canned response must build")
            } else {
                http::Response::builder()
                    .status(200)
                    .header("etag", "\"an-etag\"")
                    .body(SdkBody::from("{ this is not the record it claims to be"))
                    .expect("a canned response must build")
            }
        });

        write_sidecar(
            &client,
            "test-bucket",
            SPLITS_ARCHIVE_KEY,
            DerivedDataset::Splits,
            Authorship::refusing(Provenance::massive(
                MassivePlan::StocksStarter,
                MassiveTransport::Rest,
            )),
        )
        .await;

        let written = seen
            .lock()
            .expect("the recorder must not be poisoned")
            .clone();
        assert!(
            written.is_empty(),
            "a record that did not parse must not be written over; wrote {written:?}"
        );
    }

    /// A whole-table object must carry provenance too, and must not be reported as sessionless.
    ///
    /// The splits and boundaries tables are one object rather than a partition tree, so a stamp that
    /// insisted on a date-partitioned key wrote nothing for them and warned on every single write.
    #[tokio::test]
    async fn test_a_whole_table_object_records_its_route() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let client = scripted_s3_client(move |method, key| {
            if method == http::Method::PUT {
                recorder
                    .lock()
                    .expect("the recorder must not be poisoned")
                    .push(key.to_string());
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .expect("a canned response must build")
            } else {
                no_such_key()
            }
        });

        write_sidecar(
            &client,
            "test-bucket",
            SPLITS_ARCHIVE_KEY,
            DerivedDataset::Splits,
            Authorship::refusing(Provenance::massive(
                MassivePlan::StocksStarter,
                MassiveTransport::Rest,
            )),
        )
        .await;

        let written = seen
            .lock()
            .expect("the recorder must not be poisoned")
            .clone();
        assert!(
            written.iter().any(|key| key
                .ends_with("data/derived/equity/corporate_actions/splits.parquet.provenance.json")),
            "a whole-table object needs a sidecar; wrote {written:?}"
        );
    }

    /// One session's write failing must cost that session and no other.
    ///
    /// A non-contention error returned from `write_partitions` would discard every session after
    /// it, which on a five-hour whole-market pass costs whatever is left of the run.
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
            Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest),
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
            "data/derived/equity/bars/interval=one_day/year=2026/month=08/day=19/data.parquet"
        );
        assert_eq!(
            date_partitioned_key(&bar_archive_prefix(BarInterval::OneMinute), date),
            "data/derived/equity/bars/interval=one_minute/year=2026/month=08/day=19/data.parquet"
        );
        assert_eq!(
            date_partitioned_key(&bar_archive_prefix(BarInterval::FiveMinute), date),
            "data/derived/equity/bars/interval=five_minute/year=2026/month=08/day=19/data.parquet"
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
            "data/derived/equity/quotes/interval=one_day/year=2026/month=08/day=19/data.parquet"
        );
        assert_eq!(
            date_partitioned_key(&quote_archive_prefix(BarInterval::FiveMinute), date),
            "data/derived/equity/quotes/interval=five_minute/year=2026/month=08/day=19/data.parquet"
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

    /// A pass has to read presence off something it is certain to write, or `archive` skips the
    /// sessions it half-wrote. Pinned to literals because the failure is silent both ways: keyed on
    /// a prefix the pass does not write, every session reads present and the pass does nothing at
    /// all -- which is what a one-minute quote fold would have done against the daily prefix.
    #[test]
    fn test_a_pass_reads_presence_off_a_prefix_it_writes() {
        assert_eq!(
            quote_presence_interval(IntradayCadence::FiveMinute),
            BarInterval::OneDay
        );
        assert_eq!(
            quote_presence_interval(IntradayCadence::OneMinute),
            BarInterval::OneMinute
        );

        // Literals, not a pairing read off `DAILY_QUOTE_AUTHOR`: an expectation derived from the
        // constant under test moves with it and can never fail.
        assert_eq!(DAILY_QUOTE_AUTHOR, IntradayCadence::FiveMinute);
        for (cadence, presence) in [
            (IntradayCadence::FiveMinute, BarInterval::OneDay),
            (IntradayCadence::OneMinute, BarInterval::OneMinute),
        ] {
            assert_eq!(quote_presence_interval(cadence), presence, "{cadence:?}");
        }
    }

    /// The prefix presence is read off has to be the last one written, or a pass that dies partway
    /// marks the session done with half of it missing -- the `1106` partition-write-loss shape.
    ///
    /// Both sides are pinned to literals and then asserted to agree. Deriving either from the other
    /// would make the assertion tautological, and the failure it guards against is silent: the
    /// session simply never comes back.
    #[test]
    fn test_the_prefix_presence_is_read_off_is_written_last() {
        assert_eq!(
            quote_write_order(IntradayCadence::FiveMinute),
            [BarInterval::FiveMinute, BarInterval::OneDay]
        );
        assert_eq!(
            quote_write_order(IntradayCadence::OneMinute),
            [BarInterval::OneDay, BarInterval::OneMinute]
        );

        for cadence in IntradayCadence::ALL {
            let order = quote_write_order(cadence);
            assert_eq!(
                order.last().copied(),
                Some(quote_presence_interval(cadence)),
                "{cadence:?} reads presence off a prefix it does not write last"
            );
            // Both partitions, never one twice: a pass that wrote its session row in place of its
            // intraday one would still satisfy the rule above.
            assert_ne!(order[0], order[1], "{cadence:?}");
        }
    }

    /// The two families' cadence checks must not be able to name each other's columns: a quote
    /// prefix read with trade rules would ask for `volume` in a partition that has never held one.
    #[test]
    fn test_each_summary_family_names_its_own_prefix_and_its_own_opaque_columns() {
        assert_eq!(
            SummaryFamily::Quotes.prefix(BarInterval::OneMinute),
            "data/derived/equity/quotes/interval=one_minute"
        );
        assert_eq!(
            SummaryFamily::Trades.prefix(BarInterval::OneMinute),
            "data/derived/equity/trades/interval=one_minute"
        );
        assert_eq!(
            SummaryFamily::Quotes.opaque_columns(),
            [
                "quoted_spread_basis_points_median",
                "quoted_spread_basis_points_ninetieth_percentile"
            ]
        );
        assert_eq!(
            SummaryFamily::Trades.opaque_columns(),
            ["median_trade_size", "ninetieth_percentile_trade_size"]
        );
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
        assert_eq!(
            progress().into_trade_summary(871_159).to_string(),
            format!("{shared}, 2048 summaries, 871159 trades folded")
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

    /// Same shape on the trade path, which shares the `rows_written` arm with quotes: a variant
    /// folded into that arm by mistake would report the prints it folded as rows it wrote.
    #[test]
    fn test_a_trade_pass_carries_what_it_cost() {
        let summary = PassProgress {
            rows_written: 158,
            ..Default::default()
        }
        .into_trade_summary(871_159);

        assert_eq!(
            summary.output,
            PassOutput::Trades {
                summaries_written: 158,
                trades_folded: 871_159
            }
        );
        assert_eq!(summary.output.rows_written(), 158);
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

    /// `binary_search` is only correct on a sorted slice, and an entry appended in the wrong place
    /// fails by silently not matching — the one way this list can be wrong without looking wrong.
    #[test]
    fn test_the_test_symbol_list_is_sorted_because_the_lookup_assumes_it() {
        let mut sorted = EXCHANGE_TEST_SYMBOLS;
        sorted.sort_unstable();

        assert_eq!(EXCHANGE_TEST_SYMBOLS, sorted);
        for symbol in EXCHANGE_TEST_SYMBOLS {
            let ticker = Ticker::new(symbol).expect("every listed test symbol must parse");
            assert!(is_exchange_test_symbol(&ticker), "{symbol} must be found");
        }
    }

    /// The reason the list is spelled out instead of pattern-matched. Every one of these is a real
    /// security whose name sits inside the shape of a test symbol, and a prefix rule wide enough to
    /// catch `ZVZZT` or the `CBO` series would drop them from every universe in the archive.
    #[test]
    fn test_real_securities_shaped_like_test_symbols_survive() {
        for real in ["ZTS", "ZBRA", "CBOE", "IBM", "NTES", "ZIM", "MTN"] {
            let ticker = Ticker::new(real).expect("a real ticker must parse");
            assert!(
                !is_exchange_test_symbol(&ticker),
                "{real} is a listed security and must never be filtered"
            );
        }
    }

    /// Measured over every September 2026 session: these 28 names appear in the daily aggregate and
    /// are quoted on zero sessions and traded on zero sessions. Counting them made a whole-market
    /// pass end `complete: false` forever, so they are not the universe a fold can be held to.
    #[test]
    fn test_a_derived_universe_drops_test_symbols_and_keeps_securities() {
        let daily = df![
            "ticker" => ["AAPL", "ZVZZT", "CBOE", "CBOX", "NTEST.H", "ZTS"],
            "close_price" => [230.0_f64, 27.43, 101.66, 101.66, 25.03, 160.0],
            "volume" => [50_000_000_i64, 73_771, 900_000, 15_287, 1_800, 2_000_000],
        ]
        .unwrap();

        let universe = names_from_partition(&NameSelection::WholeMarket, daily)
            .expect("a whole-market universe");

        assert_eq!(universe, tickers(&["AAPL", "CBOE", "ZTS"]));
    }

    /// A named set is an operator's instruction, not a universe that drifted, so it is taken as
    /// given — otherwise probing a test symbol deliberately would silently fold nothing.
    #[test]
    fn test_a_named_selection_is_never_filtered() {
        let daily = df![
            "ticker" => ["AAPL"],
            "close_price" => [230.0_f64],
            "volume" => [50_000_000_i64],
        ]
        .unwrap();
        let named = tickers(&["ZVZZT"]);

        let universe = names_from_partition(&NameSelection::Named(named.clone()), daily)
            .expect("a named universe");

        assert_eq!(universe, named);
    }

    /// The three families must not share a prefix, or a scan reports one family's gaps against
    /// another's partitions and the answer looks plausible.
    #[test]
    fn test_each_session_family_scans_its_own_prefix() {
        let interval = BarInterval::OneMinute;

        assert_eq!(
            SessionFamily::Bars.prefix(interval),
            "data/derived/equity/bars/interval=one_minute"
        );
        assert_eq!(
            SessionFamily::Quotes.prefix(interval),
            "data/derived/equity/quotes/interval=one_minute"
        );
        assert_eq!(
            SessionFamily::Trades.prefix(interval),
            "data/derived/equity/trades/interval=one_minute"
        );
    }

    /// The cadence check and the symbol scan must read the same bytes for the same family. They
    /// held separate copies of this mapping until they were joined, which is two places for one
    /// prefix to be wrong in.
    #[test]
    fn test_the_cadence_check_and_the_scan_agree_on_where_a_family_lives() {
        for interval in [BarInterval::OneMinute, BarInterval::OneDay] {
            assert_eq!(
                SummaryFamily::Quotes.prefix(interval),
                SessionFamily::Quotes.prefix(interval)
            );
            assert_eq!(
                SummaryFamily::Trades.prefix(interval),
                SessionFamily::Trades.prefix(interval)
            );
        }
        assert_eq!(SummaryFamily::Quotes.to_string(), "quotes");
        assert_eq!(SummaryFamily::Trades.to_string(), "trades");
    }

    /// The distinction the scan turns on, and the two opposite ways it goes wrong. Scan a tick fold
    /// against the screen and every gap below the floor is **hidden**, because a narrower expectation
    /// can only report fewer differences. Scan bars against the whole market and the mirror happens —
    /// every below-floor name reads as a gap, because a bars partition holds only screened names.
    #[test]
    fn test_the_tick_folds_are_scanned_against_every_name_and_bars_against_the_screen() {
        let daily = || {
            df![
                "ticker" => ["CBOE", "OBDC", "SNDL"],
                "close_price" => [290.0_f64, 11.3, 1.50],
                "volume" => [900_000_i64, 4_350_000, 40_000_000],
            ]
            .unwrap()
        };
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).unwrap();

        let screened = names_from_partition(&SessionFamily::Bars.universe(floor), daily())
            .expect("a screened read");
        let whole = names_from_partition(&SessionFamily::Quotes.universe(floor), daily())
            .expect("a whole-market read");
        assert_eq!(
            SessionFamily::Trades.universe(floor),
            SessionFamily::Quotes.universe(floor),
            "both tick folds take the same universe"
        );

        assert_eq!(screened, tickers(&["CBOE"]));
        assert_eq!(whole, tickers(&["CBOE", "OBDC", "SNDL"]));

        // The consequence, in the direction it actually runs: a partition holding only CBOE is two
        // names short, and the screen calls that complete.
        assert_eq!(
            coverage_of(&whole, &tickers(&["CBOE"])),
            SessionCoverage::Partial(tickers(&["OBDC", "SNDL"])),
            "the tick folds' universe reports the gap"
        );
        assert_eq!(
            coverage_of(&screened, &tickers(&["CBOE"])),
            SessionCoverage::Complete,
            "the screen hides it, which is why the selection is not the caller's to choose"
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

    /// A partition that will not combine is refused, because the fetched frame is current-schema
    /// without being a superset: writing it drops whatever the stored rows held that it omits.
    #[test]
    fn test_a_partition_that_cannot_be_merged_is_refused() {
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

        let error = merge_or_refuse(existing, fetched, "some/key")
            .expect_err("a schema that will not combine is a refusal, not a replacement");

        match error {
            ArchiveError::SchemaConflict { key, message } => {
                assert_eq!(key, "some/key");
                // Asserted, not just the key: a constant message would satisfy a key-only check
                // while telling an operator nothing about which column moved.
                assert!(message.contains("a_retired_column"), "{message}");
                assert!(message.contains("close_price"), "{message}");
            }
            other => panic!("expected a schema conflict, got {other}"),
        }
    }

    /// A merge can fail for reasons that are not the two frames disagreeing, and those must keep
    /// their own error — a refusal that names the wrong cause sends the repair to the wrong place.
    #[test]
    fn test_frames_that_agree_on_their_columns_report_no_disagreement() {
        let stored = df![
            "ticker" => ["AAPL"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_i64],
        ]
        .unwrap();
        let incoming = df![
            "ticker" => ["MSFT"],
            "bar_interval" => ["one_day"],
            "timestamp" => [2_i64],
        ]
        .unwrap();

        assert_eq!(schema_disagreement(&stored, &incoming), None);
        assert_eq!(
            classify_merge_failure(
                PolarsError::ComputeError("ran out of memory".into()),
                None,
                "some/key"
            )
            .to_string(),
            ArchiveError::Frame(PolarsError::ComputeError("ran out of memory".into())).to_string(),
        );
    }

    /// Columns that match by name but not by type are still a disagreement `concat` refuses, and
    /// the name-difference lists are both empty there — so that branch needs its own reading.
    #[test]
    fn test_a_type_change_is_a_disagreement_even_though_no_column_moved() {
        let stored = df!["ticker" => ["AAPL"], "timestamp" => [1_i64]].unwrap();
        let incoming = df!["ticker" => ["AAPL"], "timestamp" => [1.0_f64]].unwrap();

        let disagreement =
            schema_disagreement(&stored, &incoming).expect("a changed type is a disagreement");

        assert!(disagreement.contains("type or order"), "{disagreement}");
    }

    /// The refusal has to name the object, or an operator reading a failed nightly cannot tell
    /// which of a session's partitions to re-fold.
    #[test]
    fn test_a_schema_conflict_names_the_key_and_the_cause() {
        let error = ArchiveError::SchemaConflict {
            key: "data/derived/equity/bars/interval=one_day/date=2026-09-21/part.parquet"
                .to_string(),
            message: "lengths don't match".to_string(),
        };

        let rendered = error.to_string();

        assert!(rendered.contains("date=2026-09-21"), "{rendered}");
        assert!(rendered.contains("lengths don't match"), "{rendered}");
    }

    /// The splits table is cumulative and the feed answers with a window, so a replacement drops
    /// every split older than the current page — the most expensive instance of this defect.
    #[test]
    fn test_a_splits_table_that_cannot_be_merged_is_refused() {
        let existing = df![
            "ticker" => ["AAPL"],
            "a_retired_column" => [1_i64],
        ]
        .unwrap();
        let fetched = df![
            "id" => ["some-id"],
            "ticker" => ["AAPL"],
            "execution_date" => ["2026-09-21"],
            "split_from" => [1_i64],
            "split_to" => [4_i64],
            "first_seen" => [1_i64],
        ]
        .unwrap();

        let error = merge_splits_or_refuse(existing, fetched, SPLITS_ARCHIVE_KEY)
            .expect_err("a stored table that will not combine is a refusal");

        match error {
            ArchiveError::SchemaConflict { key, message } => {
                assert_eq!(key, SPLITS_ARCHIVE_KEY);
                assert!(message.contains("a_retired_column"), "{message}");
            }
            other => panic!("expected a schema conflict, got {other}"),
        }
    }

    /// A merge that succeeds must still succeed; the refusal is the exceptional path, not the
    /// normal one, and a guard that refuses everything would pass the test above.
    #[test]
    fn test_a_partition_that_combines_is_merged_rather_than_refused() {
        let existing = df![
            "ticker" => ["AAPL"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_i64],
            "close_price" => [100.0_f64],
        ]
        .unwrap();
        let fetched = df![
            "ticker" => ["MSFT"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_i64],
            "close_price" => [101.0_f64],
        ]
        .unwrap();

        let merged = merge_or_refuse(existing, fetched, "some/key")
            .expect("two frames of the same schema combine");

        assert_eq!(merged.height(), 2);
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

    /// A `ListObjectsV2` body naming exactly `keys`.
    fn listing_body(keys: &[&str]) -> String {
        let contents: String = keys
            .iter()
            .map(|key| {
                format!(
                    "<Contents><Key>{key}</Key><Size>1</Size>\
<LastModified>2026-09-22T00:00:00.000Z</LastModified></Contents>"
                )
            })
            .collect();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
<Name>test-bucket</Name><KeyCount>{}</KeyCount><IsTruncated>false</IsTruncated>{contents}\
</ListBucketResult>",
            keys.len()
        )
    }

    /// A mapping parquet holding one run per granularity, naming `sector_bucket` verbatim.
    fn mapping_parquet(sector_bucket: &str) -> Vec<u8> {
        let mut frame = DataFrame::new(vec![
            Column::new("granularity".into(), vec!["sector", "industry"]),
            Column::new("low".into(), vec![100_u32, 100]),
            Column::new("high".into(), vec![999_u32, 999]),
            Column::new("bucket".into(), vec![sector_bucket, "Agriculture"]),
        ])
        .expect("the fixture frame builds");
        let mut buffer: Vec<u8> = Vec::new();
        ParquetWriter::new(&mut buffer)
            .finish(&mut frame)
            .expect("the fixture frame serializes");
        buffer
    }

    /// Answers a listing with `keys` and every object read with `object`.
    fn classification_client(keys: Vec<String>, object: Vec<u8>) -> S3Client {
        scripted_s3_client(move |_method, key| {
            // The listing is the only request whose path carries no hive segment.
            let payload = if key.contains("as_of=") {
                SdkBody::from(object.clone())
            } else {
                let borrowed: Vec<&str> = keys.iter().map(String::as_str).collect();
                SdkBody::from(listing_body(&borrowed))
            };
            http::Response::builder()
                .status(200)
                .header("etag", "\"an-etag\"")
                .body(payload)
                .expect("a canned response must build")
        })
    }

    /// The newest `as_of` is the one loaded, not the first or last listed.
    #[tokio::test]
    async fn test_the_newest_classification_partition_is_the_one_read() {
        let client = classification_client(
            vec![
                classification_key(date_of(2026, 6, 1)),
                classification_key(date_of(2026, 9, 22)),
                classification_key(date_of(2026, 7, 15)),
            ],
            mapping_parquet("ConsumerNondurables"),
        );

        let table = read_newest_classification(&client, "test-bucket")
            .await
            .expect("the mapping must load");

        assert_eq!(table.as_of(), date_of(2026, 9, 22));
    }

    /// A key that merely contains `as_of=` is not a partition.
    ///
    /// Without the round-trip check the 2099 key wins, the reader is sent to a canonical object that
    /// does not exist, and the load fails while a valid partition is sitting in the same prefix.
    #[tokio::test]
    async fn test_a_non_canonical_key_is_not_mistaken_for_a_partition() {
        let client = classification_client(
            vec![
                "data/reference/classification/as_of=2099-01-01/backup/data.parquet".to_string(),
                classification_key(date_of(2026, 9, 22)),
            ],
            mapping_parquet("ConsumerNondurables"),
        );

        let table = read_newest_classification(&client, "test-bucket")
            .await
            .expect("the valid partition must still load");

        assert_eq!(table.as_of(), date_of(2026, 9, 22));
    }

    /// An absent dataset refuses rather than classifying every name as the fallback, which is the
    /// failure with no visible symptom.
    #[tokio::test]
    async fn test_an_absent_classification_dataset_is_refused() {
        let client = classification_client(Vec::new(), mapping_parquet("ConsumerNondurables"));

        let error = read_newest_classification(&client, "test-bucket")
            .await
            .expect_err("an absent mapping must be refused");

        assert!(
            matches!(error, ArchiveError::Classification { .. }),
            "got {error:?}"
        );
    }

    /// A bucket name this build cannot place is refused at the read, not folded into the catch-all.
    #[tokio::test]
    async fn test_a_stored_mapping_naming_an_unknown_bucket_is_refused() {
        let client = classification_client(
            vec![classification_key(date_of(2026, 9, 22))],
            mapping_parquet("CryptoMining"),
        );

        let error = read_newest_classification(&client, "test-bucket")
            .await
            .expect_err("an unknown bucket must be refused");

        assert!(
            error.to_string().contains("CryptoMining"),
            "the refusal must name the bucket it could not place: {error}"
        );
    }

    fn date_of(year: i32, month: u32, day: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(year, month, day).expect("a real date")
    }
}
