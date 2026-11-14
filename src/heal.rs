//! The nightly heal: each leg's owed sessions fetched and written to the archive, oldest first, until done or out of
//! time, with each partition journaled once it has been read back.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use strum::IntoEnumIterator;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::common::heal::{
    Coverage, CoverageRefusal, Held, KeepsRefusal, Lapse, Leg, PartitionFailureKind, SeriesListing,
    SessionOutcome, WindowRefusal, alpaca_borrow_flags, alpaca_quotes, alpaca_series_boundaries,
    alpaca_trades, fetched_range, lapsed, massive_security_details, massive_splits,
    massive_traded_symbols, owed, sec_filed_industry_codes, window,
};
use crate::common::journal::{
    ArchiveSurveyed, ConditionsWritten, ConfigurationResolved, HealFinished, Observation,
    PartitionFailed, PartitionWritten, Unanswered, quotes_folded, trades_folded,
};
use crate::common::market::aggregate::{RollsUp, session_bars};
use crate::common::market::corporate_actions::refresh_boundaries;
use crate::common::market::industry::{AccessionNumber, FiledIndustryCode};
use crate::common::market::quote_bars::{QuoteFold, QuoteFoldRefusal};
use crate::common::market::record::{Bar, BarInterval, Partition, PartitionRefusal, Quote};
use crate::common::market::restatements::Restatements;
use crate::common::market::security_details::{
    CentralIndexKey, IndustryCode, SecurityDetails, SecurityType,
};
use crate::common::market::trade_bars::{TradeConditions, TradeFold};
use crate::common::market::{Price, Shares, Symbol};
use crate::common::monoid::{Monoid, Tally, concatenate};
use crate::common::parameter::{Parameter, ParameterRefusal, at_most, record};
use crate::common::storage::{Key, Provider, ReferenceKey, ReferenceTable, SeriesPrefix};
use crate::common::time::calendar::TradingCalendar;
use crate::common::time::{SessionDate, SessionRange};
use crate::ingest::alpaca::corporate_actions::SeriesBoundaries;
use crate::ingest::alpaca::{
    Alpaca, AlpacaQuoteOutcome, AlpacaTradeOutcome, Page, invalid_symbol, tape_of,
};
use crate::ingest::massive::{DetailsAnswer, Massive};
use crate::ingest::sec::{AnnualReport, Sec};
use crate::ingest::{
    FetchError, Fetched, QuoteOrigin, RefusedRow, RowRefusal, SessionBars, VariableRefusal,
    refused_by_cause,
};
use crate::journal::Journal;
use crate::parameter::{Directories, environment_variable};
use crate::storage::provenance::{Provenance, Subscription};
use crate::storage::raw::TeeError;
use crate::storage::reference::{
    conditions_key, decode_filed_industry_codes, decode_security_details, decode_series_boundaries,
    encode_borrow_flags, encode_conditions, encode_filed_industry_codes, encode_security_details,
    encode_series_boundaries, encode_splits, latest_conditions, latest_snapshot,
};
use crate::storage::{self, Bucket, BucketError};
use crate::storage::{quote_bars, trade_bars};

const DEFAULT_LOOKBACK_SESSIONS: NonZeroUsize = NonZeroUsize::new(5).expect("5 is not zero");
const DEFAULT_BUDGET_MINUTES: NonZeroU64 = NonZeroU64::new(240).expect("240 is not zero");
/// Each symbol's ticks are one serial chain of pages, so a session is bounded by its longest names.
const DEFAULT_TICK_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(16).expect("16 is not zero");
/// Five years of sessions, as far back as Massive Starter reaches.
const MAXIMUM_LOOKBACK_SESSIONS: NonZeroUsize =
    NonZeroUsize::new(1_260).expect("1,260 is not zero");
/// One day, past which one night's run would meet the next.
const MAXIMUM_BUDGET_MINUTES: NonZeroU64 = NonZeroU64::new(1_440).expect("1,440 is not zero");

/// The heal's settings, each resolved once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameters {
    lookback_sessions: NonZeroUsize,
    budget: Duration,
    directories: Directories,
    tick_concurrency: NonZeroUsize,
}

impl Parameters {
    /// Reads each parameter's variable, returning the configuration the journal records for them.
    pub fn from_environment() -> Result<(Self, ConfigurationResolved), ParameterRefusal> {
        Self::resolved(&environment_variable)
    }

    /// Resolves each parameter from what `supplied` returns for it, or its default when that is nothing.
    fn resolved(
        supplied: &impl Fn(Parameter) -> Result<Option<String>, ParameterRefusal>,
    ) -> Result<(Self, ConfigurationResolved), ParameterRefusal> {
        let mut resolved = BTreeMap::new();
        let read = |parameter| Ok::<_, ParameterRefusal>((parameter, supplied(parameter)?));
        let budget_minutes = at_most(
            Parameter::BudgetMinutes,
            record(
                read(Parameter::BudgetMinutes)?,
                DEFAULT_BUDGET_MINUTES,
                &mut resolved,
            )?,
            MAXIMUM_BUDGET_MINUTES,
        )?;
        let parameters = Self {
            lookback_sessions: at_most(
                Parameter::LookbackSessions,
                record(
                    read(Parameter::LookbackSessions)?,
                    DEFAULT_LOOKBACK_SESSIONS,
                    &mut resolved,
                )?,
                MAXIMUM_LOOKBACK_SESSIONS,
            )?,
            budget: Duration::from_secs(budget_minutes.get() * 60),
            directories: Directories::resolved(supplied, &mut resolved)?,
            tick_concurrency: record(
                read(Parameter::TickConcurrency)?,
                DEFAULT_TICK_CONCURRENCY,
                &mut resolved,
            )?,
        };
        Ok((parameters, ConfigurationResolved::new(resolved)))
    }

    pub fn journal_directory(&self) -> &Path {
        self.directories.journal()
    }

    pub fn log_directory(&self) -> &Path {
        self.directories.log()
    }
}

/// Why the heal could not decide what it owes, so wrote nothing.
#[derive(Debug, thiserror::Error)]
pub enum HealError {
    #[error("fetching the calendar failed: {0}")]
    Calendar(FetchError),
    #[error("{0}")]
    Window(WindowRefusal),
    /// A leg's sessions could not be read off the calendar, which the run fetched to reach back far enough.
    #[error("the {leg} leg: {refusal}")]
    Keeps { leg: Leg, refusal: KeepsRefusal },
    #[error("{0}")]
    List(BucketError),
    /// A series spans sessions the fetched calendar does not reach.
    #[error("the {series} series: {refusal}")]
    Coverage {
        series: String,
        refusal: CoverageRefusal,
    },
    /// A write or failure went unrecorded, so the run stops rather than go on with what it cannot record.
    #[error("journaling the heal failed: {0}")]
    Journal(std::io::Error),
}

/// Why one owed partition was not written.
#[derive(Debug, thiserror::Error)]
pub enum PartitionFailure {
    #[error("{0}")]
    Fetch(FetchError),
    /// The vendor answered a trading day with nothing, which written would mark the session held for good.
    #[error("the vendor answered with no rows")]
    NoRows,
    #[error("{session} is not in the calendar")]
    NotInCalendar { session: SessionDate },
    #[error("{0}")]
    Encode(storage::EncodeRefusal),
    #[error("{0}")]
    Decode(storage::DecodeRefusal),
    #[error("{0}")]
    Bucket(BucketError),
    /// Listed, then gone when read.
    #[error("{} vanished", .key.path())]
    Vanished { key: Key },
    /// The session's grouped daily, which names its symbols, listed none; `key` is its traded-symbols partition.
    #[error("no symbols listed for {}", .key.path())]
    NoSymbols { key: Key },
    /// A partition the leg reads was not yet written for its session.
    #[error("{} is not yet written", .key.path())]
    Unwritten { key: Key },
    #[error("{0}")]
    Unconfigured(VariableRefusal),
    #[error("{0}")]
    Fold(QuoteFoldRefusal),
    #[error("{0}")]
    Tee(TeeError),
    /// The listing answers only for now, and could no longer be dated by `session` when it arrived.
    #[error("{session}'s listing lapsed: {lapse}")]
    Lapsed { session: SessionDate, lapse: Lapse },
    /// A write the partition needed could not be journaled, which stops the run.
    #[error("journaling a write failed: {0}")]
    Journal(std::io::Error),
}

impl PartitionFailure {
    pub fn kind(&self) -> PartitionFailureKind {
        match self {
            Self::Fetch(_) => PartitionFailureKind::Fetch,
            Self::NoRows => PartitionFailureKind::NoRows,
            Self::NotInCalendar { .. } => PartitionFailureKind::NotInCalendar,
            Self::Encode(_) => PartitionFailureKind::Encode,
            Self::Decode(_) => PartitionFailureKind::Decode,
            Self::Bucket(_) => PartitionFailureKind::Bucket,
            Self::Vanished { .. } => PartitionFailureKind::Vanished,
            Self::NoSymbols { .. } => PartitionFailureKind::NoSymbols,
            Self::Unwritten { .. } => PartitionFailureKind::Unwritten,
            Self::Unconfigured(_) => PartitionFailureKind::Unconfigured,
            Self::Fold(_) => PartitionFailureKind::Fold,
            Self::Tee(_) => PartitionFailureKind::Tee,
            Self::Journal(_) => PartitionFailureKind::Journal,
            Self::Lapsed { .. } => PartitionFailureKind::Lapsed,
        }
    }

    /// The record of this failure, journaled as it happens.
    fn failed(&self, leg: Leg, session: SessionDate) -> PartitionFailed {
        PartitionFailed::new(leg, session, self.kind(), self.to_string())
    }

    /// The outcome the journal records, its kind beside its text.
    fn outcome(&self) -> SessionOutcome {
        SessionOutcome::Failed {
            failure: self.kind(),
            cause: self.to_string(),
        }
    }
}

impl From<FetchError> for PartitionFailure {
    fn from(error: FetchError) -> Self {
        Self::Fetch(error)
    }
}

impl From<BucketError> for PartitionFailure {
    fn from(error: BucketError) -> Self {
        Self::Bucket(error)
    }
}

impl From<storage::EncodeRefusal> for PartitionFailure {
    fn from(refusal: storage::EncodeRefusal) -> Self {
        Self::Encode(refusal)
    }
}

impl From<storage::DecodeRefusal> for PartitionFailure {
    fn from(refusal: storage::DecodeRefusal) -> Self {
        Self::Decode(refusal)
    }
}

/// The clients the heal reads from and writes to.
pub struct Clients {
    archive: Bucket,
    /// Shared by the concurrent security details requests.
    massive: Arc<Massive>,
    /// Shared by the concurrent per-symbol tick fetches, so its secret is held once rather than copied into each.
    alpaca: Arc<Alpaca>,
    /// Refused only where its contact is unset, which fails the one leg that reads it rather than the run.
    sec: Result<Sec, VariableRefusal>,
}

impl Clients {
    pub fn new(
        archive: Bucket,
        massive: Massive,
        alpaca: Alpaca,
        sec: Result<Sec, VariableRefusal>,
    ) -> Self {
        Self {
            archive,
            massive: Arc::new(massive),
            alpaca: Arc::new(alpaca),
            sec,
        }
    }

    /// Writes one object to the archive, as `publish` and `write_conditions` take it.
    fn put(&self) -> impl AsyncFn(&Key, Vec<u8>) -> Result<(), BucketError> {
        async |key: &Key, body| self.archive.put(key, body).await
    }

    /// Stores a snapshot's response bodies as served under its raw key, deleting the parsed snapshot first so one that
    /// fails before it is published again reads as owed.
    fn store(
        &self,
    ) -> impl AsyncFn(ReferenceKey, &Provenance, Vec<Vec<u8>>) -> Result<(), PartitionFailure> {
        async |key: ReferenceKey, provenance: &Provenance, bodies: Vec<Vec<u8>>| {
            self.archive.delete(&Key::from(key)).await?;
            self.archive
                .store_responses(key, provenance, &bodies)
                .await
                .map_err(PartitionFailure::Tee)?;
            Ok(())
        }
    }
}

/// Heals the window of trading days before `today`. Every owed session of every leg ends with an outcome; the budget
/// is checked before each session starts, so a session under way always finishes.
pub async fn run(
    parameters: &Parameters,
    clients: &Clients,
    journal: &mut Journal,
    today: SessionDate,
) -> Result<HealFinished, HealError> {
    let deadline = Instant::now() + parameters.budget;
    let calendar = clients
        .alpaca
        .calendar(fetched_range(today, parameters.lookback_sessions))
        .await
        .map_err(HealError::Calendar)?;
    let window =
        window(&calendar, today, parameters.lookback_sessions).map_err(HealError::Window)?;
    let mut outcomes = BTreeMap::new();
    let mut unrecognized = BTreeMap::new();
    for leg in Leg::iter() {
        let mut paths = Vec::new();
        for key in leg.keys(today) {
            paths.extend(
                clients
                    .archive
                    .list(&key.series())
                    .await
                    .map_err(HealError::List)?,
            );
        }
        let held = Held::of(leg, paths);
        if !held.unrecognized().is_empty() {
            tracing::warn!(%leg, unrecognized = ?held.unrecognized(), "Objects outside the series were not counted as held");
            unrecognized.insert(leg, held.unrecognized().clone());
        }
        let kept = leg
            .keeps(&window, &calendar)
            .map_err(|refusal| HealError::Keeps { leg, refusal })?;
        let sessions = heal_leg(
            leg,
            owed(&kept, held.sessions()),
            deadline,
            journal,
            async |session, journal: &mut Journal| {
                write(leg, session, &calendar, parameters, clients, journal).await
            },
        )
        .await?;
        outcomes.insert(leg, sessions);
    }
    Ok(HealFinished::new(window, outcomes, unrecognized))
}

/// Measures every dated series of market data, raw and parsed, against the trading calendar from its first session,
/// and each raw series against the sessions its parsed counterpart holds. Reference snapshots are left out, since a
/// snapshot leg keeps only some sessions by design.
pub async fn survey(clients: &Clients, today: SessionDate) -> Result<ArchiveSurveyed, HealError> {
    let paths = clients
        .archive
        .list(&SeriesPrefix::market_data())
        .await
        .map_err(HealError::List)?;
    let listing = SeriesListing::of(paths.iter().map(String::as_str));
    let Some(reach) = listing.reach(today) else {
        return Ok(ArchiveSurveyed::new(
            BTreeMap::new(),
            BTreeMap::new(),
            listing.unrecognized(),
        ));
    };
    let calendar = clients
        .alpaca
        .calendar(reach)
        .await
        .map_err(HealError::Calendar)?;
    let coverage = listing
        .series()
        .iter()
        .map(|(series, held)| match Coverage::of(held, &calendar) {
            Ok(coverage) => Ok((series.clone(), coverage)),
            Err(refusal) => Err(HealError::Coverage {
                series: series.clone(),
                refusal,
            }),
        })
        .collect::<Result<_, _>>()?;
    Ok(ArchiveSurveyed::new(
        coverage,
        listing.raw_uncovered(),
        listing.unrecognized(),
    ))
}

/// Writes each of `leg`'s owed sessions with `write` until `deadline`, journaling each outcome as it lands; a journal
/// error, from the append or from inside `write`, stops the run before another session starts.
async fn heal_leg(
    leg: Leg,
    owed: impl IntoIterator<Item = SessionDate>,
    deadline: Instant,
    journal: &mut Journal,
    mut write: impl AsyncFnMut(SessionDate, &mut Journal) -> Result<PartitionWritten, PartitionFailure>,
) -> Result<BTreeMap<SessionDate, SessionOutcome>, HealError> {
    let mut sessions = BTreeMap::new();
    for session in owed {
        let outcome = if Instant::now() >= deadline {
            SessionOutcome::Unreached
        } else {
            match write(session, journal).await {
                Ok(written) => {
                    tracing::info!(%leg, %session, rows = written.rows(), refused = %written.refused(), unanswered = written.unanswered().len(), "Partition written");
                    journal
                        .append(Utc::now(), Observation::PartitionWritten(written))
                        .map_err(HealError::Journal)?;
                    SessionOutcome::Written
                }
                Err(PartitionFailure::Journal(error)) => return Err(HealError::Journal(error)),
                Err(failure) => {
                    tracing::warn!(%leg, %session, %failure, "Partition not written");
                    journal
                        .append(
                            Utc::now(),
                            Observation::PartitionFailed(failure.failed(leg, session)),
                        )
                        .map_err(HealError::Journal)?;
                    failure.outcome()
                }
            }
        };
        sessions.insert(session, outcome);
    }
    Ok(sessions)
}

/// Fetches one leg's session and writes it, returning its record, or why it was not written.
async fn write(
    leg: Leg,
    session: SessionDate,
    calendar: &TradingCalendar,
    parameters: &Parameters,
    clients: &Clients,
    journal: &mut Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    match leg {
        Leg::MassiveTradedSymbols => write_traded_symbols(session, clients, journal).await,
        Leg::AlpacaQuotes => {
            let hours = calendar
                .session(session)
                .map(|trading| trading.hours())
                .ok_or(PartitionFailure::NotInCalendar { session })?;
            write_quotes(session, hours, parameters, clients, journal).await
        }
        Leg::AlpacaTrades => write_trades(session, parameters, clients, journal).await,
        Leg::MassiveSplits => write_splits(session, clients, journal).await,
        Leg::AlpacaSeriesBoundaries => write_series_boundaries(session, clients, journal).await,
        Leg::AlpacaBorrowFlags => write_borrow_flags(session, calendar, clients, journal).await,
        Leg::MassiveSecurityDetails => {
            write_security_details(session, parameters, clients, journal).await
        }
        Leg::SecFiledIndustryCodes => write_filed_industry_codes(session, clients, journal).await,
    }
}

/// The provenance of rows `subscription` answered just now, written by this run.
fn fetched_now(subscription: Subscription, journal: &Journal) -> Provenance {
    Provenance::new(
        subscription,
        Utc::now(),
        journal.run_id(),
        journal.commit().cloned(),
    )
}

/// Encodes `rows` under `key` and writes them with `put`, returning how many were written. An empty answer is refused,
/// since for a trading day it is a vendor gap that written would mark the session held for good.
async fn publish<K: Into<Key>, Row, Refusal: Into<storage::EncodeRefusal>>(
    put: impl AsyncFnOnce(&Key, Vec<u8>) -> Result<(), BucketError>,
    key: K,
    rows: &[Row],
    provenance: &Provenance,
    encode: impl FnOnce(&K, &[Row], &Provenance) -> Result<Vec<u8>, Refusal>,
) -> Result<u64, PartitionFailure> {
    if rows.is_empty() {
        return Err(PartitionFailure::NoRows);
    }
    let body = encode(&key, rows, provenance).map_err(Into::into)?;
    put(&key.into(), body).await?;
    Ok(u64::try_from(rows.len()).expect("a partition holds fewer than u64::MAX rows"))
}

/// `rows` as a partition, an empty answer refused as `publish` refuses one.
fn partition<Row>(rows: Vec<Row>) -> Result<Partition<Row>, PartitionFailure> {
    Partition::try_from(rows).map_err(|refusal| match refusal {
        PartitionRefusal::Empty => PartitionFailure::NoRows,
    })
}

/// Encodes `rows` under `key` and writes them with `put`, returning how many were written.
async fn publish_partition<K: Into<Key>, Row, Refusal: Into<storage::EncodeRefusal>>(
    put: impl AsyncFnOnce(&Key, Vec<u8>) -> Result<(), BucketError>,
    key: K,
    rows: &Partition<Row>,
    provenance: &Provenance,
    encode: impl FnOnce(&K, &Partition<Row>, &Provenance) -> Result<Vec<u8>, Refusal>,
) -> Result<u64, PartitionFailure> {
    let body = encode(&key, rows, provenance).map_err(Into::into)?;
    put(&key.into(), body).await?;
    Ok(u64::try_from(rows.rows().len()).expect("a partition holds fewer than u64::MAX rows"))
}

/// Publishes a fold's minute bars and their rollups under `key` at each interval, the daily last so a session reads as
/// held only once all three are, returning how many minute bars were written.
async fn publish_ticks<K: Into<Key>, B: RollsUp, Refusal: Into<storage::EncodeRefusal>>(
    put: impl AsyncFn(&Key, Vec<u8>) -> Result<(), BucketError>,
    minutes: Vec<B>,
    key: impl Fn(BarInterval) -> K,
    provenance: &Provenance,
    encode: impl Fn(&K, &Partition<B>, &Provenance) -> Result<Vec<u8>, Refusal>,
) -> Result<u64, PartitionFailure> {
    let [(interval, minute_bars), five_minute, daily] = session_bars(partition(minutes)?);
    let written = publish_partition(&put, key(interval), &minute_bars, provenance, &encode).await?;
    for (interval, bars) in [five_minute, daily] {
        publish_partition(&put, key(interval), &bars, provenance, &encode).await?;
    }
    Ok(written)
}

/// Stores the session's grouped daily response raw as the list of symbols that traded, fetched apart from the one
/// `symbol_list` gives the tick legs; nothing is parsed from it into the archive.
async fn write_traded_symbols(
    session: SessionDate,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let fetched = clients.massive.grouped_daily(session).await;
    let provenance = fetched_now(Subscription::StocksStarter, journal);
    let daily = record_traded_symbols(session, fetched, &provenance, clients.store()).await?;
    Ok(PartitionWritten::new(
        Leg::MassiveTradedSymbols,
        session,
        u64::try_from(daily.bars().len()).expect("a session lists fewer than u64::MAX symbols"),
        refused_by_cause(daily.refused()),
        BTreeMap::new(),
        BTreeMap::new(),
    ))
}

/// Stores `fetched` with `store` only once it parses and lists a symbol: the stored object alone marks the session held,
/// so a body stored on failure would never be fetched again.
async fn record_traded_symbols(
    session: SessionDate,
    fetched: Fetched<SessionBars>,
    provenance: &Provenance,
    store: impl AsyncFnOnce(ReferenceKey, &Provenance, Vec<Vec<u8>>) -> Result<(), PartitionFailure>,
) -> Result<SessionBars, PartitionFailure> {
    let (daily, bodies) = fetched.into_parts();
    let daily = listed(session, daily)?;
    store(massive_traded_symbols(session), provenance, bodies).await?;
    Ok(daily)
}

/// A session's grouped daily as the symbols that traded, refused when it lists none.
fn listed(
    session: SessionDate,
    daily: Result<SessionBars, FetchError>,
) -> Result<SessionBars, PartitionFailure> {
    let daily = daily?;
    match daily.bars().is_empty() {
        true => Err(PartitionFailure::NoSymbols {
            key: Key::RawReference(massive_traded_symbols(session)),
        }),
        false => Ok(daily),
    }
}

/// Writes Massive's whole split table as the session's snapshot.
async fn write_splits(
    session: SessionDate,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let fetched = clients.massive.splits().await;
    let provenance = fetched_now(Subscription::StocksStarter, journal);
    let splits = kept(
        clients.store(),
        massive_splits(session),
        &provenance,
        fetched,
    )
    .await?;
    let rows = publish(
        clients.put(),
        massive_splits(session),
        splits.splits(),
        &provenance,
        encode_splits,
    )
    .await?;
    Ok(PartitionWritten::new(
        Leg::MassiveSplits,
        session,
        rows,
        refused_by_cause(splits.refused()),
        BTreeMap::new(),
        BTreeMap::new(),
    ))
}

/// Stores every body `fetched` received with `store`, even when reading them failed, and returns what was read; a fetch
/// that received nothing fails as it failed, with nothing stored.
async fn kept<T>(
    store: impl AsyncFnOnce(ReferenceKey, &Provenance, Vec<Vec<u8>>) -> Result<(), PartitionFailure>,
    key: ReferenceKey,
    provenance: &Provenance,
    fetched: Fetched<T>,
) -> Result<T, PartitionFailure> {
    let (value, bodies) = fetched.into_parts();
    let value = match (value, bodies.is_empty()) {
        (Err(error), true) => return Err(error.into()),
        (value, true | false) => value,
    };
    store(key, provenance, bodies).await?;
    Ok(value?)
}

/// How far before the previous snapshot a refresh reaches back, and the span of one request.
const BOUNDARY_REFRESH_DAYS: NonZeroU16 = match NonZeroU16::new(365) {
    Some(days) => days,
    None => panic!("a year is more than no days"),
};

/// Where the first snapshot's history starts, fetched a refresh window at a time.
const BOUNDARIES_SINCE: (i32, u32, u32) = (2015, 1, 1);

/// Writes the session's series boundaries: the previous snapshot refreshed from a year before it was taken, so a
/// missed night leaves no gap, or, with none to refresh, the feed's history; either is read a year at a time.
async fn write_series_boundaries(
    session: SessionDate,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let previous = latest_snapshot(
        &clients.archive,
        Provider::Alpaca,
        ReferenceTable::SeriesBoundaries,
        Some(session),
    )
    .await?;
    let (held, first) = match previous {
        Some(key) => {
            let bytes = clients
                .archive
                .get(&Key::from(key))
                .await?
                .ok_or(PartitionFailure::Vanished { key: key.into() })?;
            let (held, _) =
                decode_series_boundaries(&key, bytes).map_err(storage::DecodeRefusal::Reference)?;
            (
                held,
                key.as_of()
                    .plus_calendar_days(-i64::from(BOUNDARY_REFRESH_DAYS.get())),
            )
        }
        None => {
            let (year, month, day) = BOUNDARIES_SINCE;
            let since = chrono::NaiveDate::from_ymd_opt(year, month, day)
                .expect("the boundaries' first day is a date");
            (Vec::new(), SessionDate::from_date(since))
        }
    };
    let window = SessionRange::single(session).reaching_back_to(first);
    let mut read = SeriesBoundaries::empty();
    let mut bodies = Vec::new();
    let mut failed = None;
    for span in window.spans(BOUNDARY_REFRESH_DAYS) {
        let (boundaries, pages) = clients.alpaca.series_boundaries(span).await.into_parts();
        bodies.extend(pages);
        match boundaries {
            Ok(boundaries) => read = read.combine(boundaries),
            Err(error) => {
                failed = Some(error);
                break;
            }
        }
    }
    let provenance = fetched_now(Subscription::AlgoTraderPlus, journal);
    let read = kept(
        clients.store(),
        alpaca_series_boundaries(session),
        &provenance,
        Fetched::new(failed.map_or(Ok(read), Err), bodies),
    )
    .await?;
    let boundaries = refresh_boundaries(&held, read.boundaries(), window);
    let rows = publish(
        clients.put(),
        alpaca_series_boundaries(session),
        &boundaries,
        &provenance,
        encode_series_boundaries,
    )
    .await?;
    Ok(PartitionWritten::new(
        Leg::AlpacaSeriesBoundaries,
        session,
        rows,
        refused_by_cause(read.refused()),
        BTreeMap::new(),
        BTreeMap::new(),
    ))
}

/// Writes the session's borrow flags from the asset listing as it stands now, refused once the next session has
/// opened; the instant is taken after the response, so a slow fetch cannot carry it past the open.
async fn write_borrow_flags(
    session: SessionDate,
    calendar: &TradingCalendar,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let fetched = clients.alpaca.borrow_flags().await;
    let provenance = fetched_now(Subscription::BrokerageAccount, journal);
    lapsed(calendar, session, provenance.fetched_at())
        .map_err(|lapse| PartitionFailure::Lapsed { session, lapse })?;
    let read = kept(
        clients.store(),
        alpaca_borrow_flags(session),
        &provenance,
        fetched,
    )
    .await?;
    let rows = publish(
        clients.put(),
        alpaca_borrow_flags(session),
        &read.flags,
        &provenance,
        encode_borrow_flags,
    )
    .await?;
    Ok(PartitionWritten::new(
        Leg::AlpacaBorrowFlags,
        session,
        rows,
        refused_by_cause(&read.refused),
        BTreeMap::new(),
        BTreeMap::new(),
    ))
}

/// What the details requests for a session answered, gathered across symbols.
#[derive(Debug, Clone, Default, PartialEq)]
struct DetailsGathered {
    details: Vec<SecurityDetails>,
    missing: Vec<Symbol>,
    refused: Vec<RefusedRow>,
    bodies: Vec<Vec<u8>>,
    /// The first request that failed, kept rather than returned so the bodies already received are still stored.
    failed: Option<FetchError>,
}

impl Monoid for DetailsGathered {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(mut self, other: Self) -> Self {
        self.details.extend(other.details);
        self.missing.extend(other.missing);
        self.refused.extend(other.refused);
        self.bodies.extend(other.bodies);
        self.failed = self.failed.or(other.failed);
        self
    }
}

/// Writes the details of every symbol that traded on `session`, as Massive held them on that date.
async fn write_security_details(
    session: SessionDate,
    parameters: &Parameters,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let symbols = symbol_list(clients, session).await?;
    let stopped = Arc::new(AtomicBool::new(false));
    let mut gathered: DetailsGathered =
        each_symbol(&symbols, parameters.tick_concurrency, |symbol| {
            let massive = Arc::clone(&clients.massive);
            let stopped = Arc::clone(&stopped);
            async move {
                let mut gathered = DetailsGathered::default();
                // A failed request ends the leg, so the symbols after it are not requested.
                if stopped.load(Ordering::Relaxed) {
                    return Ok(gathered);
                }
                let (answer, bodies) = massive
                    .security_details(&symbol, session)
                    .await
                    .into_parts();
                gathered.bodies.extend(bodies);
                match answer {
                    Ok(DetailsAnswer::Details(details)) => gathered.details.push(details),
                    Ok(DetailsAnswer::Missing) => gathered.missing.push(symbol),
                    Ok(DetailsAnswer::Refused(row)) => gathered.refused.push(row),
                    Err(error) => {
                        stopped.store(true, Ordering::Relaxed);
                        gathered.failed = Some(error);
                    }
                }
                Ok(gathered)
            }
        })
        .await?;
    let failed = gathered.failed.take();
    let provenance = fetched_now(Subscription::StocksStarter, journal);
    kept(
        clients.store(),
        massive_security_details(session),
        &provenance,
        Fetched::new(
            failed.map_or(Ok(()), Err),
            std::mem::take(&mut gathered.bodies),
        ),
    )
    .await?;
    let rows = publish(
        clients.put(),
        massive_security_details(session),
        &gathered.details,
        &provenance,
        encode_security_details,
    )
    .await?;
    Ok(PartitionWritten::new(
        Leg::MassiveSecurityDetails,
        session,
        rows,
        refused_by_cause(&gathered.refused),
        gathered
            .missing
            .into_iter()
            .map(|symbol| (symbol, Unanswered::Missing))
            .collect(),
        BTreeMap::new(),
    ))
}

/// How far back a filer's annual reports are read: the year before the archive's first security details snapshot.
const FILINGS_SINCE: (i32, u32, u32) = (2020, 8, 1);

/// The filer of a common stock or depositary receipt Massive gives no industry code, which the SEC's filings can fill.
fn uncoded_filer(details: &SecurityDetails) -> Option<CentralIndexKey> {
    let fillable = matches!(
        details.security_type(),
        Some(SecurityType::CommonStock | SecurityType::DepositaryReceipt)
    ) && details.industry_code().is_none();
    details.central_index_key().filter(|_| fillable)
}

/// Every filer of common stock or a depositary receipt that a security details snapshot up to `session` names without
/// an industry code, beside the symbol its latest such snapshot names it under; refused until `session`'s own snapshot
/// is written.
async fn uncoded_filers(
    archive: &Bucket,
    session: SessionDate,
) -> Result<BTreeMap<CentralIndexKey, Symbol>, PartitionFailure> {
    let mut snapshots: Vec<ReferenceKey> = archive
        .list(&SeriesPrefix::reference(
            Provider::Massive,
            ReferenceTable::SecurityDetails,
        ))
        .await?
        .iter()
        .filter_map(|path| path.parse::<Key>().ok())
        .filter_map(|key| ReferenceKey::try_from(key).ok())
        .filter(|key| key.as_of() <= session)
        .collect();
    snapshots.sort_by_key(ReferenceKey::as_of);
    if snapshots.last().map(ReferenceKey::as_of) != Some(session) {
        return Err(PartitionFailure::Unwritten {
            key: massive_security_details(session).into(),
        });
    }
    let mut filers = BTreeMap::new();
    for key in snapshots {
        let bytes = archive
            .get(&Key::from(key))
            .await?
            .ok_or(PartitionFailure::Vanished { key: key.into() })?;
        let (details, _) =
            decode_security_details(&key, bytes).map_err(storage::DecodeRefusal::Reference)?;
        filers.extend(
            details
                .iter()
                .filter_map(|row| uncoded_filer(row).map(|filer| (filer, row.symbol().clone()))),
        );
    }
    Ok(filers)
}

/// Writes the industry codes the SEC's annual reports assigned every filer `uncoded_filers` names, keeping the
/// previous snapshot's rows and reading only the reports it lacks; a filer the SEC does not know is unanswered.
async fn write_filed_industry_codes(
    session: SessionDate,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let sec = clients
        .sec
        .as_ref()
        .map_err(|refusal| PartitionFailure::Unconfigured(refusal.clone()))?;
    let filers = uncoded_filers(&clients.archive, session).await?;
    let mut codes = match latest_snapshot(
        &clients.archive,
        Provider::Sec,
        ReferenceTable::FiledIndustryCodes,
        Some(session),
    )
    .await?
    {
        Some(key) => {
            let bytes = clients
                .archive
                .get(&Key::from(key))
                .await?
                .ok_or(PartitionFailure::Vanished { key: key.into() })?;
            decode_filed_industry_codes(&key, bytes)
                .map_err(storage::DecodeRefusal::Reference)?
                .0
        }
        None => Vec::new(),
    };
    let held: BTreeSet<(CentralIndexKey, AccessionNumber)> = codes
        .iter()
        .map(|code| (code.filer(), code.accession().clone()))
        .collect();
    let (year, month, day) = FILINGS_SINCE;
    let since = Utc
        .with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .expect("the filings' first day is an instant");
    let (filed, unanswered, fetched) = read_filed_codes(
        &filers,
        &held,
        async |filer| sec.annual_reports(filer, since).await,
        async |filer, report| sec.assigned_code(filer, report).await,
    )
    .await;
    codes.extend(filed);
    let provenance = fetched_now(Subscription::EdgarPublic, journal);
    kept(
        clients.store(),
        sec_filed_industry_codes(session),
        &provenance,
        fetched,
    )
    .await?;
    let rows = publish(
        clients.put(),
        sec_filed_industry_codes(session),
        &codes,
        &provenance,
        encode_filed_industry_codes,
    )
    .await?;
    Ok(PartitionWritten::new(
        Leg::SecFiledIndustryCodes,
        session,
        rows,
        Tally::empty(),
        unanswered,
        BTreeMap::new(),
    ))
}

/// The codes each new annual report of `filers` assigned, reading only the filer's reports `held` lacks, beside
/// the filers the SEC does not know and every body fetched; the first other failure stops the reading.
async fn read_filed_codes(
    filers: &BTreeMap<CentralIndexKey, Symbol>,
    held: &BTreeSet<(CentralIndexKey, AccessionNumber)>,
    annual_reports: impl AsyncFn(CentralIndexKey) -> Fetched<Vec<AnnualReport>>,
    assigned_code: impl AsyncFn(CentralIndexKey, &AnnualReport) -> Fetched<Option<IndustryCode>>,
) -> (
    Vec<FiledIndustryCode>,
    BTreeMap<Symbol, Unanswered>,
    Fetched<()>,
) {
    let mut codes = Vec::new();
    let mut bodies = Vec::new();
    let mut unanswered = BTreeMap::new();
    let mut failed = None;
    'filers: for (filer, symbol) in filers {
        let (reports, pages) = annual_reports(*filer).await.into_parts();
        bodies.extend(pages);
        let reports = match reports {
            Ok(reports) => reports,
            Err(FetchError::Refused { status: 404, .. }) => {
                unanswered.insert(symbol.clone(), Unanswered::Missing);
                continue;
            }
            Err(error) => {
                failed = Some(error);
                break;
            }
        };
        for report in reports
            .iter()
            .filter(|report| !held.contains(&(*filer, report.accession().clone())))
        {
            let (code, pages) = assigned_code(*filer, report).await.into_parts();
            bodies.extend(pages);
            match code {
                Ok(code) => codes.push(FiledIndustryCode::new(
                    *filer,
                    report.accession().clone(),
                    code,
                    report.accepted_at(),
                )),
                // A report listed without a header at its address is read again next quarter rather than stopping
                // every filer after it.
                Err(FetchError::Refused { status: 404, .. }) => {}
                Err(error) => {
                    failed = Some(error);
                    break 'filers;
                }
            }
        }
    }
    (
        codes,
        unanswered,
        Fetched::new(failed.map_or(Ok(()), Err), bodies),
    )
}

/// Folds Alpaca's quotes for every symbol of the session and writes its quote bars.
async fn write_quotes(
    session: SessionDate,
    (open, close): (DateTime<Utc>, DateTime<Utc>),
    parameters: &Parameters,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let symbols = symbol_list(clients, session).await?;
    let mut fold = QuoteFold::new(open, close).map_err(PartitionFailure::Fold)?;
    let mut refused = Vec::new();
    let mut one_sided = 0_u64;
    let mut restatements = Restatements::default();
    let mut restated = 0_u64;
    let (unanswered, provenance) = per_symbol_raw(
        &clients.archive,
        alpaca_quotes(session).into(),
        Key::RawQuotes {
            provider: Provider::Alpaca,
            session,
        },
        (Subscription::AlgoTraderPlus, journal),
        &symbols,
        parameters.tick_concurrency,
        |symbol, pages| {
            let alpaca = Arc::clone(&clients.alpaca);
            async move { alpaca.quotes(&symbol, session, &pages).await }
        },
        |outcome| match outcome {
            AlpacaQuoteOutcome::Quote { quote, origin } => {
                match restates(&mut restatements, &quote, origin) {
                    true => restated += 1,
                    false => fold.push(&quote),
                }
            }
            AlpacaQuoteOutcome::OneSided { quote, .. } => {
                restatements.interrupt(&quote.symbol);
                one_sided += 1;
            }
            AlpacaQuoteOutcome::Refused(row) => {
                interrupt_refused(&mut restatements, &row);
                refused.push(row);
            }
        },
    )
    .await?;
    let (minutes, counts) = fold.finish();
    let minute_bars = publish_ticks(
        clients.put(),
        minutes,
        |interval| alpaca_quotes(session).at_interval(interval),
        &provenance,
        quote_bars::encode,
    )
    .await?;
    tracing::info!(%session, quotes = counts.accepted(), out_of_order = counts.out_of_order(), one_sided, restated, "Alpaca quotes folded");
    Ok(PartitionWritten::new(
        Leg::AlpacaQuotes,
        session,
        minute_bars,
        refused_by_cause(&refused),
        unanswered,
        quotes_folded(counts, one_sided, restated),
    ))
}

/// Everything Alpaca serves of a quote but its symbol and timestamp.
#[derive(Debug, Clone, PartialEq)]
struct AlpacaQuoteRow {
    bid: Price,
    ask: Price,
    bid_size: Shares,
    ask_size: Shares,
    origin: QuoteOrigin,
}

/// Whether Alpaca's `quote` restates its symbol's previous row, which `restatements` then holds.
fn restates(
    restatements: &mut Restatements<AlpacaQuoteRow>,
    quote: &Quote,
    origin: QuoteOrigin,
) -> bool {
    let tape = origin.tape.as_deref().and_then(tape_of);
    let row = AlpacaQuoteRow {
        bid: quote.bid(),
        ask: quote.ask(),
        bid_size: quote.bid_size(),
        ask_size: quote.ask_size(),
        origin,
    };
    restatements.restates(quote.symbol(), quote.timestamp(), tape, row)
}

/// Forgets the previous row of the symbol whose row Alpaca refused; a row filed under an unrequested ticker leaves
/// every symbol's previous row in place.
fn interrupt_refused(restatements: &mut Restatements<AlpacaQuoteRow>, row: &RefusedRow) {
    let interrupts = match row.cause() {
        RowRefusal::Unrequested => false,
        RowRefusal::Symbol(_)
        | RowRefusal::Session { .. }
        | RowRefusal::Price(_)
        | RowRefusal::Prices(_)
        | RowRefusal::Shares(_)
        | RowRefusal::DollarVolume(_)
        | RowRefusal::Bar(_)
        | RowRefusal::Quote(_)
        | RowRefusal::Tape { .. }
        | RowRefusal::Conditions { .. }
        | RowRefusal::Correction { .. }
        | RowRefusal::Duplicate
        | RowRefusal::ActionId(_)
        | RowRefusal::SplitRatio(_)
        | RowRefusal::Boundary(_)
        | RowRefusal::Undated
        | RowRefusal::SecurityType { .. }
        | RowRefusal::IndustryCode(_)
        | RowRefusal::Exchange(_)
        | RowRefusal::Dollars(_)
        | RowRefusal::CentralIndexKey { .. } => true,
    };
    if interrupts && let Ok(symbol) = Symbol::new(row.ticker()) {
        restatements.interrupt(&symbol);
    }
}

/// Folds Alpaca's trades for every symbol of the session under the newest conditions table and writes its trade bars.
async fn write_trades(
    session: SessionDate,
    parameters: &Parameters,
    clients: &Clients,
    journal: &mut Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let symbols = symbol_list(clients, session).await?;
    let conditions = trade_conditions(clients, journal).await?;
    let mut fold = TradeFold::new(session, conditions);
    let mut refused = Vec::new();
    let (unanswered, provenance) = per_symbol_raw(
        &clients.archive,
        alpaca_trades(session).into(),
        Key::RawTrades {
            provider: Provider::Alpaca,
            session,
        },
        (Subscription::AlgoTraderPlus, journal),
        &symbols,
        parameters.tick_concurrency,
        |symbol, pages| {
            let alpaca = Arc::clone(&clients.alpaca);
            async move { alpaca.trades(&symbol, session, &pages).await }
        },
        |outcome| match outcome {
            AlpacaTradeOutcome::Print {
                print,
                tape,
                letters,
                correction,
                ..
            } => fold.push_lettered(&print, tape, &letters, correction),
            AlpacaTradeOutcome::Refused(row) => refused.push(row),
        },
    )
    .await?;
    let (minutes, counts) = fold.finish();
    let minute_bars = publish_ticks(
        clients.put(),
        minutes,
        |interval| alpaca_trades(session).at_interval(interval),
        &provenance,
        trade_bars::encode,
    )
    .await?;
    tracing::info!(%session, folded = counts.folded(), withdrawn = counts.withdrawn(), unresolved = counts.unresolved().total(), unresolved_by_cause = %counts.unresolved(), unsized_prints = counts.unsized_prints(), "Alpaca trades folded");
    Ok(PartitionWritten::new(
        Leg::AlpacaTrades,
        session,
        minute_bars,
        refused_by_cause(&refused),
        unanswered,
        trades_folded(&counts),
    ))
}

/// The newest conditions snapshot, or, when none reads under this build's layout, today's fetched from Massive and
/// written and journaled first, so the trade leg never waits on an operator.
async fn trade_conditions(
    clients: &Clients,
    journal: &mut Journal,
) -> Result<TradeConditions, PartitionFailure> {
    match latest_conditions(&clients.archive).await {
        Ok((_, conditions)) => Ok(conditions),
        Err(error) => {
            tracing::warn!(%error, "No readable conditions snapshot; fetching today's");
            write_conditions(
                journal,
                async || clients.massive.trade_conditions().await,
                clients.store(),
                clients.put(),
            )
            .await
        }
    }
}

/// Fetches today's conditions with `fetch`, stores the response with `store`, writes them with `put` and journals the
/// write before returning them.
async fn write_conditions(
    journal: &mut Journal,
    fetch: impl AsyncFnOnce() -> Fetched<TradeConditions>,
    store: impl AsyncFnOnce(ReferenceKey, &Provenance, Vec<Vec<u8>>) -> Result<(), PartitionFailure>,
    put: impl AsyncFnOnce(&Key, Vec<u8>) -> Result<(), BucketError>,
) -> Result<TradeConditions, PartitionFailure> {
    let fetched = fetch().await;
    let provenance = fetched_now(Subscription::StocksStarter, journal);
    let key = conditions_key(SessionDate::at(provenance.fetched_at()));
    let conditions = kept(store, key, &provenance, fetched).await?;
    let body =
        encode_conditions(&key, &conditions, &provenance).map_err(storage::EncodeRefusal::from)?;
    put(&key.into(), body).await?;
    let written = ConditionsWritten::new(
        key.as_of(),
        u64::try_from(conditions.conditions().len())
            .expect("a table holds fewer than u64::MAX rows"),
    );
    journal
        .append(Utc::now(), Observation::ConditionsWritten(written))
        .map_err(PartitionFailure::Journal)?;
    Ok(conditions)
}

/// `per_symbol` from `subscription`, storing every page under the raw `key`, the object complete before this returns
/// so nothing folded from the pages is published without them; a failed fetch abandons the upload. The object is
/// stamped when its upload opens, since S3 fixes its metadata there, and `held_by` is deleted first, so a session that
/// fails after its new raw object is written reads as owed rather than held.
#[allow(clippy::too_many_arguments)]
async fn per_symbol_raw<Fetch, Pending, Row>(
    archive: &Bucket,
    held_by: Key,
    key: Key,
    (subscription, journal): (Subscription, &Journal),
    symbols: &[Symbol],
    concurrency: NonZeroUsize,
    fetch: Fetch,
    each: impl FnMut(Row),
) -> Result<(BTreeMap<Symbol, Unanswered>, Provenance), PartitionFailure>
where
    Fetch: Fn(Symbol, Sender<Page<Row>>) -> Pending,
    Pending: Future<Output = Result<bool, FetchError>> + Send + 'static,
    Row: Send + 'static,
{
    archive.delete(&held_by).await?;
    let mut writer = archive
        .begin_raw(key, &fetched_now(subscription, journal))
        .await
        .map_err(PartitionFailure::Tee)?;
    let fetched = per_symbol(
        symbols,
        concurrency,
        fetch,
        async |body: Vec<u8>| writer.push_line(&body).await,
        each,
        || fetched_now(subscription, journal),
    )
    .await;
    match fetched {
        Ok(answer) => {
            writer.finish().await.map_err(PartitionFailure::Tee)?;
            Ok(answer)
        }
        Err(failure) => {
            writer.abort().await;
            Err(failure)
        }
    }
}

/// Fetches each symbol's rows with at most `concurrency` in flight, handing every page's bytes to `tee` and its rows to
/// `each` as pages arrive, in order within a symbol but interleaved across symbols. A symbol Alpaca names invalid, or
/// answers with no row filed under it, is returned as unanswered rather than failing the session; any other failure
/// fails it. The provenance `answered` gives is taken once the last symbol has answered.
async fn per_symbol<Fetch, Pending, Row>(
    symbols: &[Symbol],
    concurrency: NonZeroUsize,
    fetch: Fetch,
    mut tee: impl AsyncFnMut(Vec<u8>) -> Result<(), TeeError>,
    mut each: impl FnMut(Row),
    answered: impl FnOnce() -> Provenance,
) -> Result<(BTreeMap<Symbol, Unanswered>, Provenance), PartitionFailure>
where
    Fetch: Fn(Symbol, Sender<Page<Row>>) -> Pending,
    Pending: Future<Output = Result<bool, FetchError>> + Send + 'static,
    Row: Send + 'static,
{
    let (sender, mut pages) = mpsc::channel(concurrency.get());
    let mut pending = symbols.iter().cloned();
    let mut in_flight = JoinSet::new();
    let mut unanswered = BTreeMap::new();
    loop {
        while in_flight.len() < concurrency.get() {
            let Some(symbol) = pending.next() else {
                break;
            };
            let answer = fetch(symbol.clone(), sender.clone());
            in_flight.spawn(async move { (symbol, answer.await) });
        }
        if in_flight.is_empty() {
            drop(sender);
            while let Some(page) = pages.recv().await {
                let (body, rows) = page.into_parts();
                tee(body).await.map_err(PartitionFailure::Tee)?;
                rows.into_iter().for_each(&mut each);
            }
            return Ok((unanswered, answered()));
        }
        tokio::select! {
            Some(page) = pages.recv() => {
                let (body, rows) = page.into_parts();
                tee(body).await.map_err(PartitionFailure::Tee)?;
                rows.into_iter().for_each(&mut each);
            }
            Some(joined) = in_flight.join_next() => {
                let (symbol, answer) =
                    joined.unwrap_or_else(|error| std::panic::resume_unwind(error.into_panic()));
                match answer {
                    Ok(true) => {}
                    Ok(false) => {
                        unanswered.insert(symbol, Unanswered::Missing);
                    }
                    Err(FetchError::Refused { status: 400, body })
                        if invalid_symbol(&body).as_deref() == Some(symbol.as_str()) =>
                    {
                        unanswered.insert(symbol, Unanswered::Invalid);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
}

/// The symbols that traded on `session`, which the tick legs must name since Alpaca's endpoints take a symbol list.
async fn symbol_list(
    clients: &Clients,
    session: SessionDate,
) -> Result<Vec<Symbol>, PartitionFailure> {
    let daily = listed(
        session,
        clients.massive.grouped_daily(session).await.into_parts().0,
    )?;
    Ok(daily.bars().iter().map(Bar::symbol).cloned().collect())
}

/// Fetches each of `symbols` with at most `concurrency` in flight, concatenating the answers in the order of
/// `symbols`, whatever order they finish in; the first fetch to fail fails them all and cancels the rest.
async fn each_symbol<Fetch, Pending, Answer>(
    symbols: &[Symbol],
    concurrency: NonZeroUsize,
    fetch: Fetch,
) -> Result<Answer, FetchError>
where
    Fetch: Fn(Symbol) -> Pending,
    Pending: Future<Output = Result<Answer, FetchError>> + Send + 'static,
    Answer: Monoid + Send + 'static,
{
    let mut pending_symbols = symbols.iter().cloned().enumerate();
    let mut answered = BTreeMap::new();
    let mut in_flight = JoinSet::new();
    loop {
        while in_flight.len() < concurrency.get() {
            let Some((index, symbol)) = pending_symbols.next() else {
                break;
            };
            let pending = fetch(symbol);
            in_flight.spawn(async move { (index, pending.await) });
        }
        match in_flight.join_next().await {
            None => return Ok(concatenate(answered.into_values())),
            Some(joined) => {
                let (index, answer) =
                    joined.unwrap_or_else(|error| std::panic::resume_unwind(error.into_panic()));
                answered.insert(index, answer?);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::common::journal::{ParameterSource, RunId};
    use crate::common::market::trade_bars::{
        Condition, ConditionCode, ConditionStatus, UpdateRules,
    };
    use crate::common::monoid::{Sequence, Tally, laws};

    fn symbols(count: usize) -> Vec<Symbol> {
        (0..count)
            .map(|index| {
                let letters: String = [index / 26, index % 26]
                    .iter()
                    .map(|digit| char::from(b'A' + u8::try_from(*digit).unwrap()))
                    .collect();
                Symbol::new(&letters).unwrap()
            })
            .collect()
    }

    fn size(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).unwrap()
    }

    fn stamp() -> Provenance {
        Provenance::new(
            Subscription::AlgoTraderPlus,
            Utc::now(),
            RunId::new(uuid::Uuid::new_v4()),
            None,
        )
    }

    /// The journal keeps a failure's kind beside its text, so a night's failures group without matching on prose.
    #[test]
    fn test_a_failure_is_journaled_with_its_kind_and_its_text() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 9, 29).unwrap());
        let outcomes = [
            PartitionFailure::NotInCalendar { session },
            PartitionFailure::NoRows,
            PartitionFailure::Fetch(FetchError::Refused {
                status: 403,
                body: "forbidden".to_string(),
            }),
            PartitionFailure::NoSymbols {
                key: Key::RawReference(massive_traded_symbols(session)),
            },
        ]
        .map(|failure| failure.outcome());
        let cause = |failure, cause: &str| SessionOutcome::Failed {
            failure,
            cause: cause.to_string(),
        };
        assert_eq!(
            outcomes,
            [
                cause(
                    PartitionFailureKind::NotInCalendar,
                    "2026-09-29 is not in the calendar"
                ),
                cause(
                    PartitionFailureKind::NoRows,
                    "the vendor answered with no rows"
                ),
                cause(PartitionFailureKind::Fetch, "refused with 403: forbidden"),
                cause(
                    PartitionFailureKind::NoSymbols,
                    "no symbols listed for data/equity/stage=raw/reference/provider=massive/table=traded_symbols/\
                     as_of=2026-09-29/data.jsonl.gz"
                ),
            ]
        );
    }

    /// The record journaled when a partition fails carries the same kind and text its run's outcome does.
    #[test]
    fn test_a_failure_is_journaled_as_it_happens_with_its_outcome() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 9, 29).unwrap());
        let failure = PartitionFailure::Fetch(FetchError::Refused {
            status: 403,
            body: "forbidden".to_string(),
        });
        assert_eq!(
            failure.failed(Leg::AlpacaTrades, session),
            PartitionFailed::new(
                Leg::AlpacaTrades,
                session,
                PartitionFailureKind::Fetch,
                "refused with 403: forbidden".to_string()
            )
        );
        assert_eq!(
            PartitionFailure::Journal(std::io::Error::other("disk full")).kind(),
            PartitionFailureKind::Journal
        );
    }

    /// A journal of its own in a fresh directory, which the caller removes.
    fn temporary_journal() -> (Journal, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!("fund-heal-{}", uuid::Uuid::new_v4()));
        let journal = Journal::open(&directory, RunId::new(uuid::Uuid::new_v4())).unwrap();
        (journal, directory)
    }

    fn event_types(journal: &Journal) -> Vec<&'static str> {
        journal
            .history()
            .iter()
            .map(|record| record.observation().event_type())
            .collect()
    }

    fn sessions() -> [SessionDate; 2] {
        [29, 30].map(|day| {
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 9, day).unwrap())
        })
    }

    fn far_deadline() -> Instant {
        Instant::now() + Duration::from_secs(3_600)
    }

    /// A failed partition is in the journal before the leg starts its next session, not gathered for the end.
    #[tokio::test]
    async fn test_a_failed_partition_is_journaled_before_the_next_session_starts() {
        let [first, second] = sessions();
        let (mut journal, directory) = temporary_journal();
        let mut seen = Vec::new();
        let outcomes = heal_leg(
            Leg::MassiveSplits,
            [first, second],
            far_deadline(),
            &mut journal,
            async |session, journal: &mut Journal| {
                seen.push((session, event_types(journal)));
                if session == first {
                    Err(PartitionFailure::NoRows)
                } else {
                    Ok(PartitionWritten::new(
                        Leg::MassiveSplits,
                        session,
                        1,
                        Tally::default(),
                        BTreeMap::new(),
                        BTreeMap::new(),
                    ))
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(seen, [(first, vec![]), (second, vec!["partition_failed"])]);
        assert_eq!(
            event_types(&journal),
            ["partition_failed", "partition_written"]
        );
        assert_eq!(
            outcomes,
            BTreeMap::from([
                (
                    first,
                    SessionOutcome::Failed {
                        failure: PartitionFailureKind::NoRows,
                        cause: "the vendor answered with no rows".to_string(),
                    }
                ),
                (second, SessionOutcome::Written),
            ])
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// An outcome the journal refuses, or a write the partition could not journal, stops the leg before its next
    /// session.
    #[tokio::test]
    async fn test_a_refused_journal_append_stops_further_partition_work() {
        let [first, second] = sessions();
        let (mut refusing, directory) = temporary_journal();
        std::fs::remove_dir_all(&directory).unwrap();
        let mut attempted = Vec::new();
        let refused = heal_leg(
            Leg::MassiveSplits,
            [first, second],
            far_deadline(),
            &mut refusing,
            async |session, _journal: &mut Journal| {
                attempted.push(session);
                Err(PartitionFailure::NoRows)
            },
        )
        .await;
        assert!(matches!(refused, Err(HealError::Journal(_))));
        assert_eq!(attempted, [first]);

        let (mut journal, directory) = temporary_journal();
        let mut attempted = Vec::new();
        let refused = heal_leg(
            Leg::AlpacaTrades,
            [first, second],
            far_deadline(),
            &mut journal,
            async |session, _journal: &mut Journal| {
                attempted.push(session);
                Err(PartitionFailure::Journal(std::io::Error::other(
                    "disk full",
                )))
            },
        )
        .await;
        assert!(matches!(refused, Err(HealError::Journal(_))));
        assert_eq!(attempted, [first]);
        assert_eq!(event_types(&journal), Vec::<&str>::new());
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Sessions owed past the deadline are marked unreached without a write or a journal record.
    #[tokio::test]
    async fn test_sessions_past_the_deadline_are_unreached_and_unjournaled() {
        let [first, second] = sessions();
        let (mut journal, directory) = temporary_journal();
        let mut attempted = Vec::new();
        let outcomes = heal_leg(
            Leg::MassiveSplits,
            [first, second],
            Instant::now(),
            &mut journal,
            async |session, _journal: &mut Journal| {
                attempted.push(session);
                Err(PartitionFailure::NoRows)
            },
        )
        .await
        .unwrap();
        assert_eq!(attempted, Vec::<SessionDate>::new());
        assert_eq!(
            outcomes,
            BTreeMap::from([
                (first, SessionOutcome::Unreached),
                (second, SessionOutcome::Unreached),
            ])
        );
        assert_eq!(event_types(&journal), Vec::<&str>::new());
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A conditions snapshot is stamped as fetched once its response is in, never when the request starts.
    #[tokio::test]
    async fn test_a_conditions_snapshot_is_stamped_after_its_fetch() {
        let (mut journal, directory) = temporary_journal();
        let answered = std::sync::Mutex::new(None);
        let stamped = std::sync::Mutex::new(None);
        write_conditions(
            &mut journal,
            async || {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                *answered.lock().unwrap() = Some(Utc::now());
                Fetched::new(
                    Ok(TradeConditions::new(BTreeMap::from([(
                        ConditionCode::new(37),
                        Condition::new(
                            UpdateRules::VOLUME_ONLY,
                            None,
                            None,
                            ConditionStatus::Current,
                        ),
                    )]))),
                    vec![b"{}".to_vec()],
                )
            },
            async |_key: ReferenceKey, provenance: &Provenance, _bodies: Vec<Vec<u8>>| {
                *stamped.lock().unwrap() = Some(provenance.fetched_at());
                Ok(())
            },
            async |_key: &Key, _body| Ok(()),
        )
        .await
        .unwrap();
        let answered = answered.into_inner().unwrap().unwrap();
        let stamped = stamped.into_inner().unwrap().unwrap();
        assert!(
            stamped >= answered,
            "stamped {stamped}, answered {answered}"
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A fetched conditions snapshot is stored raw, then written, then journaled, and one never fetched is none of them.
    #[tokio::test]
    async fn test_a_conditions_snapshot_is_journaled_once_it_is_written() {
        let (mut journal, directory) = temporary_journal();
        let order = std::sync::Mutex::new(Vec::new());
        let mut written = Vec::new();
        let conditions = write_conditions(
            &mut journal,
            async || {
                Fetched::new(
                    Ok(TradeConditions::new(BTreeMap::from([(
                        ConditionCode::new(37),
                        Condition::new(
                            UpdateRules::VOLUME_ONLY,
                            None,
                            None,
                            ConditionStatus::Current,
                        ),
                    )]))),
                    vec![b"{}".to_vec()],
                )
            },
            async |key: ReferenceKey, _provenance: &Provenance, bodies: Vec<Vec<u8>>| {
                order
                    .lock()
                    .unwrap()
                    .push((Key::RawReference(key).path(), bodies));
                Ok(())
            },
            async |key: &Key, _body| {
                order.lock().unwrap().push((key.path(), Vec::new()));
                written.push(key.clone());
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(conditions.conditions().len(), 1);
        assert_eq!(written.len(), 1);
        let as_of = written[0].session();
        assert_eq!(
            order.into_inner().unwrap(),
            [
                (
                    format!(
                        "data/equity/stage=raw/reference/provider=massive/table=conditions/as_of={as_of}/data.jsonl.gz"
                    ),
                    vec![b"{}".to_vec()],
                ),
                (
                    format!(
                        "data/equity/stage=parsed/reference/provider=massive/table=conditions/as_of={as_of}/data.parquet"
                    ),
                    Vec::new(),
                ),
            ]
        );
        let observations: Vec<Observation> = journal
            .history()
            .iter()
            .map(|record| record.observation().clone())
            .collect();
        assert_eq!(
            observations,
            [Observation::ConditionsWritten(ConditionsWritten::new(
                written[0].session(),
                1
            ))]
        );

        let (mut journal, unfetched_directory) = temporary_journal();
        let mut written = Vec::new();
        let refused = write_conditions(
            &mut journal,
            async || {
                Fetched::new(
                    Err(FetchError::Refused {
                        status: 403,
                        body: "forbidden".to_string(),
                    }),
                    Vec::new(),
                )
            },
            async |_: ReferenceKey, _: &Provenance, _: Vec<Vec<u8>>| {
                panic!("nothing fetched is stored")
            },
            async |key: &Key, _body| {
                written.push(key.clone());
                Ok(())
            },
        )
        .await;
        assert!(matches!(refused, Err(PartitionFailure::Fetch(_))));
        assert_eq!(written, Vec::<Key>::new());
        assert_eq!(event_types(&journal), Vec::<&str>::new());
        std::fs::remove_dir_all(&directory).unwrap();
        std::fs::remove_dir_all(&unfetched_directory).unwrap();
    }

    /// A grouped daily is stored only once it parses and lists a symbol, since the stored object alone marks the
    /// traded-symbols session held; a failed or empty one is left for the next heal to fetch again.
    #[tokio::test]
    async fn test_traded_symbols_are_stored_only_when_they_list_a_symbol() {
        let [session, _] = sessions();
        let provenance = Provenance::new(
            Subscription::StocksStarter,
            "2026-09-30T07:00:00Z".parse().unwrap(),
            RunId::new(uuid::Uuid::from_u128(1)),
            None,
        );
        let fetched = |body: &[u8]| {
            Fetched::new(
                crate::ingest::massive::parse_grouped_daily(body, session),
                vec![body.to_vec()],
            )
        };
        let record = async |fetched: Fetched<SessionBars>| {
            let mut stored = Vec::new();
            let recorded = record_traded_symbols(
                session,
                fetched,
                &provenance,
                async |key: ReferenceKey, _: &Provenance, bodies: Vec<Vec<u8>>| {
                    stored.push((Key::RawReference(key).path(), bodies.len()));
                    Ok(())
                },
            )
            .await;
            (recorded.map(|daily| daily.bars().len()), stored)
        };

        let (malformed, stored) = record(fetched(b"{\"results\":")).await;
        assert!(matches!(
            malformed,
            Err(PartitionFailure::Fetch(FetchError::Malformed { .. }))
        ));
        assert_eq!(stored, []);

        let (empty, stored) = record(fetched(br#"{"results":[]}"#)).await;
        assert!(matches!(empty, Err(PartitionFailure::NoSymbols { .. })));
        assert_eq!(stored, []);

        let (listed, stored) = record(fetched(
            br#"{"results":[{"T":"AAPL","o":1,"h":1,"l":1,"c":1,"v":1,"t":1790712000000}]}"#,
        ))
        .await;
        assert_eq!(listed.unwrap(), 1);
        assert_eq!(
            stored,
            [(
                format!(
                    "data/equity/stage=raw/reference/provider=massive/table=traded_symbols/as_of={session}/data.jsonl.gz"
                ),
                1
            )]
        );
    }

    /// A conditions response that fails to parse is still stored as served, and nothing parsed is written.
    #[tokio::test]
    async fn test_a_conditions_response_that_fails_to_parse_is_still_stored() {
        let (mut journal, directory) = temporary_journal();
        let mut stored = Vec::new();
        let failed = write_conditions(
            &mut journal,
            async || {
                Fetched::new(
                    Err(FetchError::Malformed {
                        reason: "no update rules".to_string(),
                    }),
                    vec![b"{\"results\":[{}]}".to_vec()],
                )
            },
            async |key: ReferenceKey, _provenance: &Provenance, bodies: Vec<Vec<u8>>| {
                stored.push((Key::RawReference(key).series().to_string(), bodies));
                Ok(())
            },
            async |_: &Key, _body| -> Result<(), BucketError> {
                panic!("nothing parsed is written")
            },
        )
        .await;
        assert!(matches!(
            failed,
            Err(PartitionFailure::Fetch(FetchError::Malformed { .. }))
        ));
        assert_eq!(
            stored,
            [(
                "data/equity/stage=raw/reference/provider=massive/table=conditions/".to_string(),
                vec![b"{\"results\":[{}]}".to_vec()],
            )]
        );
        assert_eq!(event_types(&journal), Vec::<&str>::new());
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Two prints a minute apart, folded into the minute bars a trade leg publishes.
    fn trade_minutes(session: SessionDate) -> Vec<crate::common::market::trade_bars::TradeBar> {
        use crate::common::market::record::Trade;
        use crate::common::market::trade_bars::{Correction, Print};
        let mut fold = TradeFold::new(session, TradeConditions::new(BTreeMap::new()));
        for at in ["2026-09-29T13:31:00Z", "2026-09-29T13:32:00Z"] {
            let print = Trade::new(
                Symbol::new("AAPL").unwrap(),
                at.parse().unwrap(),
                Price::from_dollars(100.0).unwrap(),
                Shares::whole(10).unwrap(),
            )
            .unwrap();
            fold.push(&Print::Trade(print), &[], Correction::Stands);
        }
        fold.finish().0
    }

    /// A session's three tick files land minute first and daily last, and the count journaled is the minute bars'.
    #[tokio::test]
    async fn test_ticks_publish_each_interval_with_the_daily_last() {
        let [session, _] = sessions();
        let provenance = Provenance::new(
            Subscription::AlgoTraderPlus,
            "2026-09-30T07:00:00Z".parse().unwrap(),
            RunId::new(uuid::Uuid::from_u128(1)),
            None,
        );
        let written = std::sync::Mutex::new(Vec::new());
        let put = async |key: &Key, _body| {
            written.lock().unwrap().push(key.path());
            Ok(())
        };
        let minute_bars = publish_ticks(
            &put,
            trade_minutes(session),
            |interval| alpaca_trades(session).at_interval(interval),
            &provenance,
            trade_bars::encode,
        )
        .await
        .unwrap();
        assert_eq!(minute_bars, 2);
        let prefix = "data/equity/stage=parsed/trades/provider=alpaca";
        let suffix = "year=2026/month=09/day=29/data.parquet";
        assert_eq!(
            *written.lock().unwrap(),
            ["one_minute", "five_minute", "one_day"]
                .map(|interval| format!("{prefix}/interval={interval}/{suffix}"))
        );

        written.lock().unwrap().clear();
        let refused = publish_ticks(
            &put,
            Vec::new(),
            |interval| alpaca_trades(session).at_interval(interval),
            &provenance,
            trade_bars::encode,
        )
        .await;
        assert!(matches!(refused, Err(PartitionFailure::NoRows)));
        assert_eq!(*written.lock().unwrap(), Vec::<String>::new());
    }

    #[tokio::test(start_paused = true)]
    async fn test_answers_concatenate_in_symbol_order_whatever_order_they_finish() {
        let names = symbols(25);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let answer = each_symbol(&names, size(3), |symbol| {
            let (in_flight, most) = (Arc::clone(&in_flight), Arc::clone(&most));
            async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                // Later symbols finish first.
                let letter = u64::from(symbol.as_str().as_bytes()[1]);
                tokio::time::sleep(Duration::from_millis(1_000 - letter)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(Sequence(vec![symbol.to_string()]))
            }
        })
        .await
        .unwrap();
        let expected: Vec<String> = names.iter().map(ToString::to_string).collect();
        assert_eq!(answer, Sequence(expected));
        assert_eq!(most.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_failed_fetch_fails_the_session_and_cancels_the_rest() {
        let started = Arc::new(AtomicUsize::new(0));
        let answer = each_symbol(&symbols(40), size(2), |symbol| {
            let started = Arc::clone(&started);
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                if symbol.as_str() == "AB" {
                    return Err(FetchError::Malformed {
                        reason: "broken page".to_string(),
                    });
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(Sequence::<String>(Vec::new()))
            }
        })
        .await;
        assert_eq!(
            answer,
            Err(FetchError::Malformed {
                reason: "broken page".to_string()
            })
        );
        // The second symbol fails at once, so only the two first in flight ever started.
        assert_eq!(started.load(Ordering::SeqCst), 2);
    }

    fn supplied(
        values: &[(Parameter, &str)],
    ) -> impl Fn(Parameter) -> Result<Option<String>, ParameterRefusal> {
        let values: BTreeMap<Parameter, String> = values
            .iter()
            .map(|(parameter, raw)| (*parameter, raw.to_string()))
            .collect();
        move |parameter| Ok(values.get(&parameter).cloned())
    }

    #[test]
    fn test_every_parameter_is_journaled_with_its_source() {
        let (parameters, configuration) =
            Parameters::resolved(&supplied(&[(Parameter::LookbackSessions, "10")])).unwrap();
        assert_eq!(parameters.lookback_sessions.get(), 10);
        assert_eq!(parameters.budget, Duration::from_secs(240 * 60));
        let sources: Vec<(Parameter, &str, ParameterSource)> = configuration
            .parameters()
            .iter()
            .map(|(parameter, resolved)| (*parameter, resolved.value(), resolved.source()))
            .collect();
        assert_eq!(
            sources,
            [
                (
                    Parameter::LookbackSessions,
                    "10",
                    ParameterSource::Environment
                ),
                (Parameter::BudgetMinutes, "240", ParameterSource::Default),
                (
                    Parameter::JournalDirectory,
                    "/var/journal/fund",
                    ParameterSource::Default
                ),
                (
                    Parameter::LogDirectory,
                    "/var/log/fund",
                    ParameterSource::Default
                ),
                (Parameter::TickConcurrency, "16", ParameterSource::Default),
            ]
        );
    }

    /// Past these, the deadline and the calendar range overflow and the run would panic rather than refuse.
    #[test]
    fn test_a_lookback_or_budget_past_its_most_refuses_to_start() {
        for (parameter, most, past) in [
            (Parameter::LookbackSessions, "1260", "1261"),
            (Parameter::BudgetMinutes, "1440", "1441"),
        ] {
            assert!(Parameters::resolved(&supplied(&[(parameter, most)])).is_ok());
            assert_eq!(
                Parameters::resolved(&supplied(&[(parameter, past)])).map(|_| ()),
                Err(ParameterRefusal::OutOfRange {
                    parameter,
                    value: past.to_string(),
                    most: most.to_string(),
                })
            );
        }
        let today = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 9, 30).unwrap());
        let range = fetched_range(today, MAXIMUM_LOOKBACK_SESSIONS);
        assert_eq!(range.first().to_string(), "2019-10-01");
        assert!(
            Instant::now()
                .checked_add(Duration::from_secs(1_440 * 60))
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_an_empty_or_invalid_symbol_is_unanswered_and_the_rest_fold() {
        let symbols: Vec<Symbol> = ["AAA", "BBB", "CCC"]
            .iter()
            .map(|name| Symbol::new(name).unwrap())
            .collect();
        let mut rows = Vec::new();
        let mut bodies = Vec::new();
        let unanswered = per_symbol(
            &symbols,
            size(2),
            |symbol, pages| async move {
                match symbol.as_str() {
                    "AAA" => {
                        for (body, page) in [("a1", vec![1, 2]), ("a2", vec![3])] {
                            pages.send(Page::new(body.into(), page)).await.unwrap();
                        }
                        Ok(true)
                    }
                    // Only rows filed under another ticker: the symbol asked for did not answer.
                    "BBB" => {
                        pages
                            .send(Page::new(b"b1".to_vec(), vec![4]))
                            .await
                            .unwrap();
                        Ok(false)
                    }
                    _ => Err(FetchError::Refused {
                        status: 400,
                        body: format!(r#"{{"message":"invalid symbol: {symbol}"}}"#),
                    }),
                }
            },
            async |body| {
                bodies.push(body);
                Ok(())
            },
            |row: i32| rows.push(row),
            stamp,
        )
        .await
        .unwrap()
        .0;
        rows.sort();
        assert_eq!(rows, [1, 2, 3, 4]);
        bodies.sort();
        assert_eq!(bodies, [b"a1".to_vec(), b"a2".to_vec(), b"b1".to_vec()]);
        assert_eq!(
            unanswered,
            BTreeMap::from([
                (Symbol::new("BBB").unwrap(), Unanswered::Missing),
                (Symbol::new("CCC").unwrap(), Unanswered::Invalid),
            ])
        );
    }

    #[tokio::test]
    async fn test_pages_fold_while_their_symbol_is_still_fetching() {
        let symbols = [Symbol::new("AAA").unwrap()];
        let mut rows = Vec::new();
        // One page fits the channel, so the fetch finishes only if pages are folded as they arrive.
        let answer = per_symbol(
            &symbols,
            size(1),
            |_, pages| async move {
                for page in 0..5 {
                    pages.send(Page::new(Vec::new(), vec![page])).await.unwrap();
                }
                Ok(true)
            },
            async |_| Ok(()),
            |row: i32| rows.push(row),
            stamp,
        );
        let unanswered = tokio::time::timeout(Duration::from_secs(5), answer)
            .await
            .expect("the fold drains pages while the fetch runs")
            .unwrap()
            .0;
        assert_eq!(rows, [0, 1, 2, 3, 4]);
        assert!(unanswered.is_empty());
    }

    /// Folded ticks are stamped as fetched once the last symbol has answered, never when the first request starts.
    #[tokio::test]
    async fn test_folded_ticks_are_stamped_after_the_last_symbol_answers() {
        let symbols = [Symbol::new("AAA").unwrap(), Symbol::new("BBB").unwrap()];
        let answered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (unanswered, provenance) = per_symbol(
            &symbols,
            size(1),
            |_, pages| {
                let answered = Arc::clone(&answered);
                async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    pages.send(Page::new(Vec::new(), vec![1])).await.unwrap();
                    answered.lock().unwrap().push(Utc::now());
                    Ok(true)
                }
            },
            async |_| Ok(()),
            |_: i32| {},
            stamp,
        )
        .await
        .unwrap();
        let answered = answered.lock().unwrap().clone();
        assert!(unanswered.is_empty());
        assert_eq!(answered.len(), 2);
        let last = answered.into_iter().max().unwrap();
        let stamped = provenance.fetched_at();
        assert!(stamped >= last, "stamped {stamped}, answered {last}");
    }

    #[tokio::test]
    async fn test_a_page_not_stored_raw_fails_the_session_before_its_rows_fold() {
        let symbols = [Symbol::new("AAA").unwrap()];
        let mut rows = Vec::new();
        let answer = per_symbol(
            &symbols,
            size(1),
            |_, pages| async move {
                let _ = pages.send(Page::new(b"a1".to_vec(), vec![1])).await;
                Ok(true)
            },
            async |_| {
                Err(TeeError::LineBreak {
                    path: "raw".to_string(),
                })
            },
            |row: i32| rows.push(row),
            stamp,
        )
        .await;
        assert!(matches!(
            answer,
            Err(PartitionFailure::Tee(TeeError::LineBreak { .. }))
        ));
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn test_filed_codes_skip_held_reports_and_stop_at_the_first_failure_keeping_bodies() {
        let filer = |value: u64| CentralIndexKey::new(value).unwrap();
        let report = |accession: &str, accepted_at: &str| {
            AnnualReport::new(
                AccessionNumber::new(accession).unwrap(),
                accepted_at.parse().unwrap(),
            )
        };
        let filers: BTreeMap<CentralIndexKey, Symbol> =
            [(1, "AAA"), (2, "BBB"), (3, "CCC"), (4, "DDD")]
                .into_iter()
                .map(|(value, symbol)| (filer(value), Symbol::new(symbol).unwrap()))
                .collect();
        let held = BTreeSet::from([(
            filer(1),
            AccessionNumber::new("0000000001-24-000001").unwrap(),
        )]);
        let headers = std::cell::RefCell::new(Vec::new());
        let (codes, unanswered, fetched) = read_filed_codes(
            &filers,
            &held,
            async |asked: CentralIndexKey| match asked.value() {
                1 => Fetched::new(
                    Ok(vec![
                        report("0000000001-25-000002", "2025-02-18T21:06:30Z"),
                        report("0000000001-24-000001", "2024-02-20T21:32:15Z"),
                        report("0000000001-23-000003", "2023-02-21T11:00:00Z"),
                        report("0000000001-22-000004", "2022-02-22T11:00:00Z"),
                    ]),
                    vec![b"submissions 1".to_vec()],
                ),
                2 => Fetched::new(
                    Err(FetchError::Refused {
                        status: 404,
                        body: String::new(),
                    }),
                    Vec::new(),
                ),
                3 => Fetched::new(
                    Ok(vec![report("0000000003-25-000001", "2025-03-01T21:00:00Z")]),
                    vec![b"submissions 3".to_vec()],
                ),
                _ => panic!("filer {} asked after the failure", asked.value()),
            },
            async |asked: CentralIndexKey, report: &AnnualReport| {
                headers
                    .borrow_mut()
                    .push(report.accession().as_str().to_string());
                match (asked.value(), report.accession().as_str()) {
                    (1, "0000000001-25-000002") => Fetched::new(
                        Ok(Some(IndustryCode::new("2834").unwrap())),
                        vec![b"header 1-25".to_vec()],
                    ),
                    (1, "0000000001-23-000003") => Fetched::new(
                        Err(FetchError::Refused {
                            status: 404,
                            body: String::new(),
                        }),
                        Vec::new(),
                    ),
                    (1, _) => Fetched::new(Ok(None), vec![b"header 1-22".to_vec()]),
                    (_, _) => Fetched::new(
                        Err(FetchError::Refused {
                            status: 403,
                            body: String::new(),
                        }),
                        Vec::new(),
                    ),
                }
            },
        )
        .await;
        assert_eq!(
            *headers.borrow(),
            [
                "0000000001-25-000002",
                "0000000001-23-000003",
                "0000000001-22-000004",
                "0000000003-25-000001"
            ]
        );
        assert_eq!(
            codes,
            [
                FiledIndustryCode::new(
                    filer(1),
                    AccessionNumber::new("0000000001-25-000002").unwrap(),
                    Some(IndustryCode::new("2834").unwrap()),
                    "2025-02-18T21:06:30Z".parse().unwrap(),
                ),
                FiledIndustryCode::new(
                    filer(1),
                    AccessionNumber::new("0000000001-22-000004").unwrap(),
                    None,
                    "2022-02-22T11:00:00Z".parse().unwrap(),
                ),
            ]
        );
        assert_eq!(
            unanswered,
            BTreeMap::from([(Symbol::new("BBB").unwrap(), Unanswered::Missing)])
        );
        let (outcome, bodies) = fetched.into_parts();
        assert!(matches!(
            outcome,
            Err(FetchError::Refused { status: 403, .. })
        ));
        assert_eq!(
            bodies,
            [
                b"submissions 1".to_vec(),
                b"header 1-25".to_vec(),
                b"header 1-22".to_vec(),
                b"submissions 3".to_vec(),
            ]
        );
    }

    #[test]
    fn test_only_uncoded_stock_and_receipts_are_filled() {
        let filer = CentralIndexKey::new(1_046_179).unwrap();
        let details = |kind: SecurityType, code: Option<&str>, filer: Option<CentralIndexKey>| {
            SecurityDetails::new(
                Symbol::new("AAA").unwrap(),
                Some(kind),
                code.map(|code| IndustryCode::new(code).unwrap()),
                None,
                None,
                None,
                None,
                filer,
            )
        };
        let read = |kind, code, filer| uncoded_filer(&details(kind, code, filer));
        assert_eq!(
            read(SecurityType::CommonStock, None, Some(filer)),
            Some(filer)
        );
        assert_eq!(
            read(SecurityType::DepositaryReceipt, None, Some(filer)),
            Some(filer)
        );
        assert_eq!(
            read(SecurityType::DepositaryReceipt, Some("3674"), Some(filer)),
            None
        );
        assert_eq!(
            read(SecurityType::ExchangeTradedFund, None, Some(filer)),
            None
        );
        assert_eq!(read(SecurityType::CommonStock, None, None), None);
    }

    /// Reads every security details snapshot in the archive and names the filers Massive left without a code.
    #[tokio::test]
    #[ignore = "reads every security details snapshot from the archive bucket"]
    async fn live_uncoded_filers_name_the_foreign_issuers() {
        let configuration = aws_config::load_from_env().await;
        let archive = Bucket::archive(&configuration).unwrap();
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap());
        let filers = uncoded_filers(&archive, session).await.unwrap();
        println!("{} uncoded filers", filers.len());
        let astrazeneca = CentralIndexKey::new(901_832).unwrap();
        assert_eq!(filers.get(&astrazeneca).map(Symbol::as_str), Some("AZN"));
        assert!(filers.len() > 800);
    }

    /// Stores a session's grouped daily raw in the profile bucket and reads its symbols from Massive.
    #[tokio::test]
    #[ignore = "fetches from Massive and writes one Deep Archive object to the profile bucket, then deletes it"]
    async fn live_traded_symbols_store_raw_and_name_the_session() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 9).unwrap());
        let configuration = aws_config::load_from_env().await;
        let clients = Clients::new(
            Bucket::profile(&configuration).unwrap(),
            Massive::from_environment(reqwest::Client::new()).unwrap(),
            Alpaca::from_environment(reqwest::Client::new()).unwrap(),
            Sec::from_environment(reqwest::Client::new()),
        );
        let provenance = Provenance::new(
            Subscription::StocksStarter,
            Utc::now(),
            RunId::new(uuid::Uuid::new_v4()),
            None,
        );
        let key = massive_traded_symbols(session);
        let daily = record_traded_symbols(
            session,
            clients.massive.grouped_daily(session).await,
            &provenance,
            clients.store(),
        )
        .await
        .unwrap();
        let symbols = symbol_list(&clients, session).await.unwrap();
        let raw = Key::RawReference(key);
        let stored = clients.archive.stored(&raw).await.unwrap().unwrap();
        println!(
            "{} symbols, {} bytes, {}",
            symbols.len(),
            stored.length(),
            raw.path()
        );
        clients.archive.delete(&raw).await.unwrap();
        assert_eq!(symbols.len(), daily.bars().len());
        assert!(symbols.len() > 10_000);
    }

    /// Stores Massive's split pages raw in the profile bucket, clearing the parsed snapshot first.
    #[tokio::test]
    #[ignore = "fetches from Massive and writes one Deep Archive object to the profile bucket, then deletes it"]
    async fn live_split_pages_store_raw_after_clearing_the_snapshot() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 9).unwrap());
        let configuration = aws_config::load_from_env().await;
        let clients = Clients::new(
            Bucket::profile(&configuration).unwrap(),
            Massive::from_environment(reqwest::Client::new()).unwrap(),
            Alpaca::from_environment(reqwest::Client::new()).unwrap(),
            Sec::from_environment(reqwest::Client::new()),
        );
        let key = massive_splits(session);
        clients
            .archive
            .put(&Key::from(key), b"an earlier fetch".to_vec())
            .await
            .unwrap();
        let provenance = Provenance::new(
            Subscription::StocksStarter,
            Utc::now(),
            RunId::new(uuid::Uuid::new_v4()),
            None,
        );
        let (splits, bodies) = clients.massive.splits().await.into_parts();
        let splits = splits.unwrap();
        let pages = bodies.len();
        clients.store()(key, &provenance, bodies).await.unwrap();
        let raw = Key::RawReference(key);
        let stored = clients.archive.stored(&raw).await.unwrap().unwrap();
        println!(
            "{} splits, {pages} pages, {} bytes, {}",
            splits.splits().len(),
            stored.length(),
            raw.path()
        );
        clients.archive.delete(&raw).await.unwrap();
        assert_eq!(clients.archive.get(&Key::from(key)).await.unwrap(), None);
        assert!(pages > 1);
        assert_eq!(stored.fetched_at(), Some(provenance.fetched_at()));
    }

    /// A two-sided quote of `symbol` at `at`, its prices and sizes, and Alpaca's origin for it on tape A.
    fn alpaca_quote(
        symbol: &str,
        at: &str,
        (bid, ask): (f64, f64),
        (bid_size, ask_size): (u64, u64),
    ) -> (Quote, QuoteOrigin) {
        let quote = Quote::new(
            Symbol::new(symbol).unwrap(),
            at.parse().unwrap(),
            Price::from_dollars(bid).unwrap(),
            Price::from_dollars(ask).unwrap(),
            Shares::whole(bid_size).unwrap(),
            Shares::whole(ask_size).unwrap(),
        )
        .unwrap();
        let origin = QuoteOrigin {
            bid_exchange: Some("N".to_string()),
            ask_exchange: Some("N".to_string()),
            tape: Some("A".to_string()),
            conditions: vec!["R".to_string()],
        };
        (quote, origin)
    }

    #[test]
    fn test_an_alpaca_quote_restates_only_when_every_served_field_repeats() {
        let (first, base) = alpaca_quote(
            "UTL",
            "2026-10-01T14:00:29.990Z",
            (50.00, 50.10),
            (100, 200),
        );
        let tape_b = QuoteOrigin {
            tape: Some("B".to_string()),
            ..base.clone()
        };
        let bid_exchange = QuoteOrigin {
            bid_exchange: Some("P".to_string()),
            ..base.clone()
        };
        let ask_exchange = QuoteOrigin {
            ask_exchange: Some("P".to_string()),
            ..base.clone()
        };
        let conditions = QuoteOrigin {
            conditions: vec!["O".to_string()],
            ..base.clone()
        };
        let at = "2026-10-01T14:00:30.005Z";
        for (name, (quote, origin), restated) in [
            (
                "repeat",
                alpaca_quote("UTL", at, (50.00, 50.10), (100, 200)),
                true,
            ),
            (
                "bid",
                alpaca_quote("UTL", at, (50.01, 50.10), (100, 200)),
                false,
            ),
            (
                "ask",
                alpaca_quote("UTL", at, (50.00, 50.11), (100, 200)),
                false,
            ),
            (
                "bid size",
                alpaca_quote("UTL", at, (50.00, 50.10), (101, 200)),
                false,
            ),
            (
                "ask size",
                alpaca_quote("UTL", at, (50.00, 50.10), (100, 201)),
                false,
            ),
            (
                "tape",
                (
                    alpaca_quote("UTL", at, (50.00, 50.10), (100, 200)).0,
                    tape_b,
                ),
                false,
            ),
            (
                "bid exchange",
                (
                    alpaca_quote("UTL", at, (50.00, 50.10), (100, 200)).0,
                    bid_exchange,
                ),
                false,
            ),
            (
                "ask exchange",
                (
                    alpaca_quote("UTL", at, (50.00, 50.10), (100, 200)).0,
                    ask_exchange,
                ),
                false,
            ),
            (
                "conditions",
                (
                    alpaca_quote("UTL", at, (50.00, 50.10), (100, 200)).0,
                    conditions,
                ),
                false,
            ),
            (
                "off boundary",
                alpaca_quote(
                    "UTL",
                    "2026-10-01T14:00:30.100Z",
                    (50.00, 50.10),
                    (100, 200),
                ),
                false,
            ),
        ] {
            let mut restatements = Restatements::default();
            assert!(!restates(&mut restatements, &first, base.clone()), "{name}");
            assert_eq!(
                restates(&mut restatements, &quote, origin),
                restated,
                "{name}"
            );
        }
    }

    #[test]
    fn test_only_a_refused_row_of_the_requested_symbol_interrupts_its_restatements() {
        let (first, origin) = alpaca_quote(
            "UTL",
            "2026-10-01T14:00:29.990Z",
            (50.00, 50.10),
            (100, 200),
        );
        let (repeat, _) = alpaca_quote(
            "UTL",
            "2026-10-01T14:00:30.005Z",
            (50.00, 50.10),
            (100, 200),
        );
        for (refusal, restated) in [
            (RefusedRow::new("UTL", RowRefusal::Unrequested), true),
            (RefusedRow::new("UTL", RowRefusal::Duplicate), false),
        ] {
            let mut restatements = Restatements::default();
            assert!(!restates(&mut restatements, &first, origin.clone()));
            interrupt_refused(&mut restatements, &refusal);
            assert_eq!(
                restates(&mut restatements, &repeat, origin.clone()),
                restated,
                "{refusal:?}"
            );
        }
    }

    /// A quiet tape-A name's session restates its quote at most once per half-minute boundary, and does so at all.
    #[tokio::test]
    #[ignore = "reads Alpaca's REST quote history; run deliberately under secretspec"]
    async fn live_a_quiet_tape_a_session_restates_once_per_boundary() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap());
        let symbol = Symbol::new("UTL").unwrap();
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let (sender, mut receiver) = mpsc::channel(4);
        let fetch = async move {
            let answered = alpaca.quotes(&symbol, session, &sender).await;
            drop(sender);
            answered
        };
        let mut restatements = Restatements::default();
        let (mut quotes, mut boundaries) = (0_u64, BTreeMap::<i64, u64>::new());
        let drain = async {
            while let Some(page) = receiver.recv().await {
                for outcome in page.into_parts().1 {
                    match outcome {
                        AlpacaQuoteOutcome::Quote { quote, origin } => {
                            quotes += 1;
                            if restates(&mut restatements, &quote, origin) {
                                let nearest = (quote.timestamp().timestamp_millis() + 15_000)
                                    .div_euclid(30_000);
                                *boundaries.entry(nearest).or_insert(0) += 1;
                            }
                        }
                        AlpacaQuoteOutcome::OneSided { quote, .. } => {
                            restatements.interrupt(&quote.symbol);
                        }
                        AlpacaQuoteOutcome::Refused(row) => panic!("{row:?}"),
                    }
                }
            }
        };
        let (answered, ()) = tokio::join!(fetch, drain);
        let restated: u64 = boundaries.values().sum();
        println!(
            "{quotes} quotes, {restated} restated over {} boundaries",
            boundaries.len()
        );
        assert!(answered.unwrap());
        assert!(restated > 0);
        assert_eq!(restated, u64::try_from(boundaries.len()).unwrap());
    }

    /// Stores five busy symbols' quotes for one session, enough to span several parts, in the profile bucket,
    /// clearing the earlier fetch's daily fold first.
    #[tokio::test]
    #[ignore = "fetches from Alpaca and writes one Deep Archive object to the profile bucket, then deletes it"]
    async fn live_raw_quote_pages_store_across_parts() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 9).unwrap());
        let symbols: Vec<Symbol> = ["SPY", "QQQ", "AAPL", "NVDA", "TSLA"]
            .iter()
            .map(|name| Symbol::new(name).unwrap())
            .collect();
        let configuration = aws_config::load_from_env().await;
        let profile = Bucket::profile(&configuration).unwrap();
        let alpaca = Arc::new(Alpaca::from_environment(reqwest::Client::new()).unwrap());
        let key = Key::RawQuotes {
            provider: Provider::Alpaca,
            session,
        };
        let held_by: Key = alpaca_quotes(session).into();
        profile
            .put(&held_by, b"an earlier fetch".to_vec())
            .await
            .unwrap();
        let mut quotes = 0_u64;
        let (journal, directory) = temporary_journal();
        let started = Utc::now();
        let (unanswered, folded) = per_symbol_raw(
            &profile,
            held_by.clone(),
            key.clone(),
            (Subscription::AlgoTraderPlus, &journal),
            &symbols,
            size(5),
            |symbol, pages| {
                let alpaca = Arc::clone(&alpaca);
                async move { alpaca.quotes(&symbol, session, &pages).await }
            },
            |_| quotes += 1,
        )
        .await
        .unwrap();
        let stored = profile.stored(&key).await.unwrap().unwrap();
        println!(
            "{quotes} quotes, {} bytes, {:?}, {}",
            stored.length(),
            stored.checksum(),
            key.path()
        );
        profile.delete(&key).await.unwrap();
        assert_eq!(profile.get(&held_by).await.unwrap(), None);
        assert!(unanswered.is_empty());
        assert!(stored.length() > 64 * 1024 * 1024);
        assert!(matches!(
            stored.checksum(),
            crate::storage::raw::Checksum::FullObject(_)
        ));
        assert!(
            stored.fetched_at().is_some_and(
                |fetched_at| started <= fetched_at && fetched_at <= folded.fetched_at()
            )
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Built field by field rather than through `combine`, so the law test does not lean on what it checks.
    fn any_gathered() -> impl proptest::strategy::Strategy<Value = DetailsGathered> {
        use proptest::prelude::*;
        let symbol = || {
            prop::sample::select(vec!["AAA", "BBB", "CCC"])
                .prop_map(|name| Symbol::new(name).unwrap())
        };
        let refusal = prop::sample::select(vec![RowRefusal::Duplicate, RowRefusal::Unrequested]);
        (
            prop::collection::vec(symbol(), 0..4),
            prop::collection::vec(symbol(), 0..4),
            prop::collection::vec(("[A-Z]{1,3}", refusal), 0..4),
            prop::collection::vec(prop::collection::vec(any::<u8>(), 0..4), 0..3),
            prop::option::of("[a-z]{1,8}".prop_map(|reason| FetchError::Malformed { reason })),
        )
            .prop_map(
                |(details, missing, refused, bodies, failed)| DetailsGathered {
                    details: details
                        .into_iter()
                        .map(|symbol| {
                            SecurityDetails::new(symbol, None, None, None, None, None, None, None)
                        })
                        .collect(),
                    missing,
                    bodies,
                    failed,
                    refused: refused
                        .into_iter()
                        .map(|(ticker, cause)| RefusedRow::new(&ticker, cause))
                        .collect(),
                },
            )
    }

    proptest::proptest! {
        /// Answers gather in the order they were asked, whatever grouping the concurrency happened to merge them in.
        #[test]
        fn property_gathered_details_concatenate_as_a_monoid(
            first in any_gathered(),
            second in any_gathered(),
            third in any_gathered(),
        ) {
            laws::check_ordered(first, second, third)?;
        }
    }

    #[tokio::test]
    async fn test_no_symbols_is_the_empty_answer() {
        let answer = each_symbol(&[], size(2), |_| async {
            Err::<Sequence<String>, _>(FetchError::Malformed {
                reason: "fetched nothing".to_string(),
            })
        })
        .await;
        assert_eq!(answer, Ok(Sequence(Vec::new())));
    }
}
