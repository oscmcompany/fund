//! The nightly heal: each leg's owed sessions fetched and written to the archive, oldest first, until done or out of
//! time, with each partition journaled once it has been read back.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use strum::IntoEnumIterator;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::archive::bars::{Provenance, Subscription, decode, encode};
use crate::archive::reference::{
    conditions_key, decode_series_boundaries, encode_conditions, encode_security_details,
    encode_series_boundaries, encode_splits, latest_conditions, latest_snapshot,
};
use crate::archive::{Archive, ArchiveError, DecodeRefusal, EncodeRefusal};
use crate::archive::{quote_bars, trade_bars};
use crate::common::heal::{
    Held, KeepsRefusal, Leg, PartitionFailureKind, SessionOutcome, WindowRefusal,
    alpaca_minute_bars, alpaca_quotes, alpaca_series_boundaries, alpaca_trades, fetched_range,
    massive_daily_bars, massive_security_details, massive_splits, owed, window,
};
use crate::common::journal::{
    ConditionsWritten, ConfigurationResolved, HealFinished, Observation, PartitionFailed,
    PartitionWritten, Unanswered, quotes_folded, trades_folded,
};
use crate::common::market::Symbol;
use crate::common::market::aggregate::{RollsUp, session_bars};
use crate::common::market::corporate_actions::refresh_boundaries;
use crate::common::market::quote_bars::{QuoteFold, QuoteFoldRefusal};
use crate::common::market::record::{Bar, BarInterval};
use crate::common::market::security_details::SecurityDetails;
use crate::common::market::trade_bars::{TradeConditions, TradeFold};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::parameter::{Parameter, ParameterRefusal, at_most, record};
use crate::common::storage::{Key, Provider, ReferenceTable};
use crate::common::time::calendar::TradingCalendar;
use crate::common::time::{SessionDate, SessionRange};
use crate::ingest::alpaca::corporate_actions::SeriesBoundaries;
use crate::ingest::alpaca::{
    Alpaca, AlpacaQuoteOutcome, AlpacaTradeOutcome, MinuteBars, invalid_symbol,
};
use crate::ingest::massive::{DetailsAnswer, Massive};
use crate::ingest::{FetchError, RefusedRow, refused_by_cause};
use crate::journal::Journal;
use crate::parameter::{DEFAULT_JOURNAL_DIRECTORY, DEFAULT_LOG_DIRECTORY, environment_variable};

const DEFAULT_LOOKBACK_SESSIONS: NonZeroUsize = NonZeroUsize::new(5).expect("5 is not zero");
const DEFAULT_BUDGET_MINUTES: NonZeroU64 = NonZeroU64::new(240).expect("240 is not zero");
/// A whole-market session measured 2026-09-30 at about 33 s with these two.
const DEFAULT_MINUTE_BATCH_SYMBOLS: NonZeroUsize = NonZeroUsize::new(200).expect("200 is not zero");
const DEFAULT_MINUTE_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(8).expect("8 is not zero");
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
    journal_directory: PathBuf,
    log_directory: PathBuf,
    minute_batch_symbols: NonZeroUsize,
    minute_concurrency: NonZeroUsize,
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
            journal_directory: PathBuf::from(record(
                read(Parameter::JournalDirectory)?,
                DEFAULT_JOURNAL_DIRECTORY.to_string(),
                &mut resolved,
            )?),
            log_directory: PathBuf::from(record(
                read(Parameter::LogDirectory)?,
                DEFAULT_LOG_DIRECTORY.to_string(),
                &mut resolved,
            )?),
            minute_batch_symbols: record(
                read(Parameter::MinuteBatchSymbols)?,
                DEFAULT_MINUTE_BATCH_SYMBOLS,
                &mut resolved,
            )?,
            minute_concurrency: record(
                read(Parameter::MinuteConcurrency)?,
                DEFAULT_MINUTE_CONCURRENCY,
                &mut resolved,
            )?,
            tick_concurrency: record(
                read(Parameter::TickConcurrency)?,
                DEFAULT_TICK_CONCURRENCY,
                &mut resolved,
            )?,
        };
        Ok((parameters, ConfigurationResolved::new(resolved)))
    }

    pub fn journal_directory(&self) -> &PathBuf {
        &self.journal_directory
    }

    pub fn log_directory(&self) -> &PathBuf {
        &self.log_directory
    }
}

/// Why the heal could not decide what it owes, so wrote nothing.
#[derive(Debug)]
pub enum HealError {
    Calendar(FetchError),
    Window(WindowRefusal),
    /// A leg's sessions could not be read off the calendar, which the run fetched to reach back far enough.
    Keeps {
        leg: Leg,
        refusal: KeepsRefusal,
    },
    List(ArchiveError),
    /// A write or failure went unrecorded, so the run stops rather than go on with what it cannot record.
    Journal(std::io::Error),
}

impl std::fmt::Display for HealError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Calendar(error) => write!(formatter, "fetching the calendar failed: {error}"),
            Self::Window(refusal) => write!(formatter, "{refusal}"),
            Self::Keeps { leg, refusal } => write!(formatter, "the {leg} leg: {refusal}"),
            Self::List(error) => write!(formatter, "{error}"),
            Self::Journal(error) => {
                write!(formatter, "journaling the heal failed: {error}")
            }
        }
    }
}

/// Why one owed partition was not written.
#[derive(Debug)]
pub enum PartitionFailure {
    Fetch(FetchError),
    /// The vendor answered a trading day with nothing, which written would mark the session held for good.
    NoRows,
    NotInCalendar {
        session: SessionDate,
    },
    Encode(EncodeRefusal),
    Decode(DecodeRefusal),
    Archive(ArchiveError),
    /// Listed, then gone when read.
    Vanished {
        key: Key,
    },
    /// The session's daily bars, which name its symbols, are not written.
    NoSymbols {
        key: Key,
    },
    Fold(QuoteFoldRefusal),
    /// A write the partition needed could not be journaled, which stops the run.
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
            Self::Archive(_) => PartitionFailureKind::Archive,
            Self::Vanished { .. } => PartitionFailureKind::Vanished,
            Self::NoSymbols { .. } => PartitionFailureKind::NoSymbols,
            Self::Fold(_) => PartitionFailureKind::Fold,
            Self::Journal(_) => PartitionFailureKind::Journal,
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

impl std::fmt::Display for PartitionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fetch(error) => write!(formatter, "{error}"),
            Self::NoRows => write!(formatter, "the vendor answered with no rows"),
            Self::NotInCalendar { session } => {
                write!(formatter, "{session} is not in the calendar")
            }
            Self::Encode(refusal) => write!(formatter, "{refusal}"),
            Self::Decode(refusal) => write!(formatter, "{refusal}"),
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::Vanished { key } => write!(formatter, "{} vanished", key.path()),
            Self::NoSymbols { key } => {
                write!(
                    formatter,
                    "no daily bars at {} to take the symbols from",
                    key.path()
                )
            }
            Self::Fold(refusal) => write!(formatter, "{refusal}"),
            Self::Journal(error) => write!(formatter, "journaling a write failed: {error}"),
        }
    }
}

impl std::error::Error for PartitionFailure {}

impl From<FetchError> for PartitionFailure {
    fn from(error: FetchError) -> Self {
        Self::Fetch(error)
    }
}

impl From<ArchiveError> for PartitionFailure {
    fn from(error: ArchiveError) -> Self {
        Self::Archive(error)
    }
}

impl From<EncodeRefusal> for PartitionFailure {
    fn from(refusal: EncodeRefusal) -> Self {
        Self::Encode(refusal)
    }
}

impl From<DecodeRefusal> for PartitionFailure {
    fn from(refusal: DecodeRefusal) -> Self {
        Self::Decode(refusal)
    }
}

/// The clients the heal reads from and writes to.
pub struct Clients {
    archive: Archive,
    /// Shared by the concurrent security details requests.
    massive: Arc<Massive>,
    /// Shared by the concurrent one-minute batches, so its secret is held once rather than copied into each.
    alpaca: Arc<Alpaca>,
}

impl Clients {
    pub fn new(archive: Archive, massive: Massive, alpaca: Alpaca) -> Self {
        Self {
            archive,
            massive: Arc::new(massive),
            alpaca: Arc::new(alpaca),
        }
    }

    /// Writes one object to the archive, as `publish` and `write_conditions` take it.
    fn put(&self) -> impl AsyncFn(&Key, Vec<u8>) -> Result<(), ArchiveError> {
        async |key: &Key, body| self.archive.put(key, body).await
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
        let series = leg.key(today).series();
        let held = Held::of(
            leg,
            clients
                .archive
                .list(&series)
                .await
                .map_err(HealError::List)?,
        );
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
                    tracing::info!(%leg, %session, bars = written.bars(), refused = %written.refused(), unanswered = written.unanswered().len(), "Partition written");
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
        Leg::MassiveDailyBars => write_daily_bars(session, clients, journal).await,
        Leg::AlpacaMinuteBars => write_minute_bars(session, parameters, clients, journal).await,
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
        Leg::MassiveSecurityDetails => {
            write_security_details(session, parameters, clients, journal).await
        }
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
async fn publish<K: Into<Key>, Row, Refusal: Into<EncodeRefusal>>(
    put: impl AsyncFnOnce(&Key, Vec<u8>) -> Result<(), ArchiveError>,
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

/// Publishes a fold's minute bars and their rollups under `key` at each interval, the daily last so a session reads as
/// held only once all three are, returning how many minute bars were written.
async fn publish_ticks<K: Into<Key>, B: RollsUp, Refusal: Into<EncodeRefusal>>(
    put: impl AsyncFn(&Key, Vec<u8>) -> Result<(), ArchiveError>,
    minutes: Vec<B>,
    key: impl Fn(BarInterval) -> K,
    provenance: &Provenance,
    encode: impl Fn(&K, &[B], &Provenance) -> Result<Vec<u8>, Refusal>,
) -> Result<u64, PartitionFailure> {
    let [(interval, minute_bars), five_minute, daily] = session_bars(minutes);
    let written = publish(&put, key(interval), &minute_bars, provenance, &encode).await?;
    for (interval, bars) in [five_minute, daily] {
        publish(&put, key(interval), &bars, provenance, &encode).await?;
    }
    Ok(written)
}

/// Writes Massive's grouped daily bars for the session.
async fn write_daily_bars(
    session: SessionDate,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let daily = clients.massive.grouped_daily(session).await?;
    if !daily.test_tickers().is_empty() {
        tracing::info!(%session, test_tickers = daily.test_tickers().len(), "Exchange test tickers left out");
    }
    let provenance = fetched_now(Subscription::StocksStarter, journal);
    let bars = publish(
        clients.put(),
        massive_daily_bars(session),
        daily.bars(),
        &provenance,
        encode,
    )
    .await?;
    Ok(PartitionWritten::new(
        Leg::MassiveDailyBars,
        session,
        bars,
        refused_by_cause(daily.refused()),
        BTreeMap::new(),
        BTreeMap::new(),
    ))
}

/// Writes Alpaca's one-minute bars for every symbol of the session, fetched in concurrent batches.
async fn write_minute_bars(
    session: SessionDate,
    parameters: &Parameters,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let symbols = symbol_list(clients, session).await?;
    let minute: MinuteBars = in_batches(
        &symbols,
        parameters.minute_batch_symbols,
        parameters.minute_concurrency,
        |batch| {
            let alpaca = Arc::clone(&clients.alpaca);
            async move { alpaca.minute_bars(&batch, session).await }
        },
    )
    .await?;
    let unanswered = minute
        .missing()
        .iter()
        .map(|symbol| (symbol.clone(), Unanswered::Missing))
        .chain(
            minute
                .invalid()
                .iter()
                .map(|symbol| (symbol.clone(), Unanswered::Invalid)),
        )
        .collect();
    let provenance = fetched_now(Subscription::AlgoTraderPlus, journal);
    let bars = publish(
        clients.put(),
        alpaca_minute_bars(session),
        minute.bars(),
        &provenance,
        encode,
    )
    .await?;
    Ok(PartitionWritten::new(
        Leg::AlpacaMinuteBars,
        session,
        bars,
        refused_by_cause(minute.refused()),
        unanswered,
        BTreeMap::new(),
    ))
}

/// Writes Massive's whole split table as the session's snapshot.
async fn write_splits(
    session: SessionDate,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, PartitionFailure> {
    let splits = clients.massive.splits().await?;
    let provenance = fetched_now(Subscription::StocksStarter, journal);
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
        session,
    )
    .await?;
    let (held, first) = match previous {
        Some(key) => {
            let bytes = clients
                .archive
                .get(&key.into())
                .await?
                .ok_or(PartitionFailure::Vanished { key: key.into() })?;
            let (held, _) =
                decode_series_boundaries(&key, bytes).map_err(DecodeRefusal::Reference)?;
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
    for span in window.spans(BOUNDARY_REFRESH_DAYS) {
        read = read.combine(clients.alpaca.series_boundaries(span).await?);
    }
    let boundaries = refresh_boundaries(&held, read.boundaries(), window);
    let provenance = fetched_now(Subscription::AlgoTraderPlus, journal);
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

/// What the details requests for a session answered, gathered across symbols.
#[derive(Debug, Clone, Default, PartialEq)]
struct DetailsGathered {
    details: Vec<SecurityDetails>,
    missing: Vec<Symbol>,
    refused: Vec<RefusedRow>,
}

impl Monoid for DetailsGathered {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(mut self, other: Self) -> Self {
        self.details.extend(other.details);
        self.missing.extend(other.missing);
        self.refused.extend(other.refused);
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
    let one = NonZeroUsize::new(1).expect("one is not zero");
    let gathered: DetailsGathered =
        in_batches(&symbols, one, parameters.tick_concurrency, |batch| {
            let massive = Arc::clone(&clients.massive);
            async move {
                let mut gathered = DetailsGathered::default();
                for symbol in batch {
                    match massive.security_details(&symbol, session).await? {
                        DetailsAnswer::Details(details) => gathered.details.push(details),
                        DetailsAnswer::Missing => gathered.missing.push(symbol),
                        DetailsAnswer::Refused(row) => gathered.refused.push(row),
                    }
                }
                Ok(gathered)
            }
        })
        .await?;
    let provenance = fetched_now(Subscription::StocksStarter, journal);
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
    let unanswered = per_symbol(
        &symbols,
        parameters.tick_concurrency,
        |symbol, pages| {
            let alpaca = Arc::clone(&clients.alpaca);
            async move { alpaca.quotes(&symbol, session, &pages).await }
        },
        |outcome| match outcome {
            AlpacaQuoteOutcome::Quote(quote) => fold.push(&quote),
            AlpacaQuoteOutcome::OneSided => one_sided += 1,
            AlpacaQuoteOutcome::Refused(row) => refused.push(row),
        },
    )
    .await?;
    let (minutes, counts) = fold.finish();
    let provenance = fetched_now(Subscription::AlgoTraderPlus, journal);
    let minute_bars = publish_ticks(
        clients.put(),
        minutes,
        |interval| alpaca_quotes(session).at_interval(interval),
        &provenance,
        quote_bars::encode,
    )
    .await?;
    tracing::info!(%session, quotes = counts.accepted(), out_of_order = counts.out_of_order(), one_sided, "Alpaca quotes folded");
    Ok(PartitionWritten::new(
        Leg::AlpacaQuotes,
        session,
        minute_bars,
        refused_by_cause(&refused),
        unanswered,
        quotes_folded(counts, one_sided),
    ))
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
    let unanswered = per_symbol(
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
            } => fold.push_lettered(&print, tape, &letters, correction),
            AlpacaTradeOutcome::Refused(row) => refused.push(row),
        },
    )
    .await?;
    let (minutes, counts) = fold.finish();
    let provenance = fetched_now(Subscription::AlgoTraderPlus, journal);
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
                clients.put(),
            )
            .await
        }
    }
}

/// Fetches today's conditions with `fetch`, writes them with `put` and journals the write before returning them.
async fn write_conditions(
    journal: &mut Journal,
    fetch: impl AsyncFnOnce() -> Result<TradeConditions, FetchError>,
    put: impl AsyncFnOnce(&Key, Vec<u8>) -> Result<(), ArchiveError>,
) -> Result<TradeConditions, PartitionFailure> {
    let fetched_at = Utc::now();
    let conditions = fetch().await?;
    let key = conditions_key(SessionDate::at(fetched_at));
    let provenance = Provenance::new(
        Subscription::StocksStarter,
        fetched_at,
        journal.run_id(),
        journal.commit().cloned(),
    );
    let body = encode_conditions(&key, &conditions, &provenance).map_err(EncodeRefusal::from)?;
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

/// Fetches each symbol's rows with at most `concurrency` in flight, handing every row to `each` a page at a time as
/// pages arrive, in order within a symbol but interleaved across symbols. A symbol Alpaca names invalid, or answers
/// with no row filed under it, is returned as unanswered rather than failing the session; any other failure fails it.
async fn per_symbol<Fetch, Pending, Row>(
    symbols: &[Symbol],
    concurrency: NonZeroUsize,
    fetch: Fetch,
    mut each: impl FnMut(Row),
) -> Result<BTreeMap<Symbol, Unanswered>, FetchError>
where
    Fetch: Fn(Symbol, Sender<Vec<Row>>) -> Pending,
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
                page.into_iter().for_each(&mut each);
            }
            return Ok(unanswered);
        }
        tokio::select! {
            Some(page) = pages.recv() => page.into_iter().for_each(&mut each),
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
                    Err(error) => return Err(error),
                }
            }
        }
    }
}

/// The session's symbols, read from its written daily bars, since those name what traded that day.
async fn symbol_list(
    clients: &Clients,
    session: SessionDate,
) -> Result<Vec<Symbol>, PartitionFailure> {
    let key = massive_daily_bars(session);
    let body = clients
        .archive
        .get(&key.into())
        .await?
        .ok_or(PartitionFailure::NoSymbols { key: key.into() })?;
    let (bars, _) = decode(&key, body).map_err(DecodeRefusal::from)?;
    Ok(bars.iter().map(Bar::symbol).cloned().collect())
}

/// Fetches `symbols` in batches with at most `concurrency` in flight, concatenating the answers in the order the
/// batches were cut, whatever order they finish in; the first batch to fail fails them all and cancels the rest.
async fn in_batches<Fetch, Pending, Answer>(
    symbols: &[Symbol],
    batch_symbols: NonZeroUsize,
    concurrency: NonZeroUsize,
    fetch: Fetch,
) -> Result<Answer, FetchError>
where
    Fetch: Fn(Vec<Symbol>) -> Pending,
    Pending: Future<Output = Result<Answer, FetchError>> + Send + 'static,
    Answer: Monoid + Send + 'static,
{
    let mut batches = symbols
        .chunks(batch_symbols.get())
        .map(<[Symbol]>::to_vec)
        .enumerate();
    let mut answered = BTreeMap::new();
    let mut in_flight = JoinSet::new();
    loop {
        while in_flight.len() < concurrency.get() {
            let Some((index, batch)) = batches.next() else {
                break;
            };
            let pending = fetch(batch);
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
    use crate::common::monoid::{Tally, laws};
    use crate::ingest::RowRefusal;

    /// The batches' symbols in the order they were concatenated.
    #[derive(Debug, Clone, PartialEq)]
    struct Names(Vec<String>);

    impl Monoid for Names {
        fn empty() -> Self {
            Self(Vec::new())
        }

        fn combine(mut self, other: Self) -> Self {
            self.0.extend(other.0);
            self
        }
    }

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
                key: Leg::MassiveDailyBars.key(session),
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
                    "no daily bars at data/equity/stage=parsed/bars/provider=massive/origin=vendor/interval=one_day/\
                     year=2026/month=09/day=29/data.parquet to take the symbols from"
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
    fn temporary_journal() -> (Journal, PathBuf) {
        let directory = std::env::temp_dir().join(format!("fund-heal-{}", uuid::Uuid::new_v4()));
        let journal = Journal::open(&directory, RunId::new(uuid::Uuid::new_v4())).unwrap();
        (journal, directory)
    }

    fn event_types(journal: &Journal) -> Vec<&'static str> {
        journal
            .history()
            .unwrap()
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
            Leg::MassiveDailyBars,
            [first, second],
            far_deadline(),
            &mut journal,
            async |session, journal: &mut Journal| {
                seen.push((session, event_types(journal)));
                if session == first {
                    Err(PartitionFailure::NoRows)
                } else {
                    Ok(PartitionWritten::new(
                        Leg::MassiveDailyBars,
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
            Leg::MassiveDailyBars,
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
            Leg::MassiveDailyBars,
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

    /// A fetched conditions snapshot is journaled once written, and one never fetched is neither written nor journaled.
    #[tokio::test]
    async fn test_a_conditions_snapshot_is_journaled_once_it_is_written() {
        let (mut journal, directory) = temporary_journal();
        let mut written = Vec::new();
        let conditions = write_conditions(
            &mut journal,
            async || {
                Ok(TradeConditions::new(BTreeMap::from([(
                    ConditionCode::new(37),
                    Condition::new(
                        UpdateRules::VOLUME_ONLY,
                        None,
                        None,
                        ConditionStatus::Current,
                    ),
                )])))
            },
            async |key: &Key, _body| {
                written.push(key.clone());
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(conditions.conditions().len(), 1);
        assert_eq!(written.len(), 1);
        let observations: Vec<Observation> = journal
            .history()
            .unwrap()
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
                Err(FetchError::Refused {
                    status: 403,
                    body: "forbidden".to_string(),
                })
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

    /// Two prints a minute apart, folded into the minute bars a trade leg publishes.
    fn trade_minutes(session: SessionDate) -> Vec<crate::common::market::trade_bars::TradeBar> {
        use crate::common::market::record::Trade;
        use crate::common::market::trade_bars::{Correction, Print};
        use crate::common::market::{Price, Shares};
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
        let prefix = "data/equity/stage=parsed/trades/provider=alpaca/origin=derived";
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
    async fn test_batches_concatenate_in_the_order_cut_whatever_order_they_finish() {
        let names = symbols(25);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let answer = in_batches(&names, size(4), size(3), |batch| {
            let (in_flight, most) = (Arc::clone(&in_flight), Arc::clone(&most));
            async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                // Later batches finish first.
                let first = u64::from(batch[0].as_str().as_bytes()[1]);
                tokio::time::sleep(Duration::from_millis(1_000 - first)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(Names(batch.iter().map(ToString::to_string).collect()))
            }
        })
        .await
        .unwrap();
        let expected: Vec<String> = names.iter().map(ToString::to_string).collect();
        assert_eq!(answer, Names(expected));
        assert_eq!(most.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_failed_batch_fails_the_session_and_cancels_the_rest() {
        let started = Arc::new(AtomicUsize::new(0));
        let answer = in_batches(&symbols(40), size(4), size(2), |batch| {
            let started = Arc::clone(&started);
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                if batch[0].as_str() == "AE" {
                    return Err(FetchError::Malformed {
                        reason: "broken page".to_string(),
                    });
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(Names(Vec::new()))
            }
        })
        .await;
        assert_eq!(
            answer,
            Err(FetchError::Malformed {
                reason: "broken page".to_string()
            })
        );
        // The second batch fails at once, so only the two first in flight ever started.
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
                (
                    Parameter::MinuteBatchSymbols,
                    "200",
                    ParameterSource::Default
                ),
                (Parameter::MinuteConcurrency, "8", ParameterSource::Default),
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
        let unanswered = per_symbol(
            &symbols,
            size(2),
            |symbol, pages| async move {
                match symbol.as_str() {
                    "AAA" => {
                        for page in [vec![1, 2], vec![3]] {
                            pages.send(page).await.unwrap();
                        }
                        Ok(true)
                    }
                    // Only rows filed under another ticker: the symbol asked for did not answer.
                    "BBB" => {
                        pages.send(vec![4]).await.unwrap();
                        Ok(false)
                    }
                    _ => Err(FetchError::Refused {
                        status: 400,
                        body: format!(r#"{{"message":"invalid symbol: {symbol}"}}"#),
                    }),
                }
            },
            |row: i32| rows.push(row),
        )
        .await
        .unwrap();
        rows.sort();
        assert_eq!(rows, [1, 2, 3, 4]);
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
                    pages.send(vec![page]).await.unwrap();
                }
                Ok(true)
            },
            |row: i32| rows.push(row),
        );
        let unanswered = tokio::time::timeout(Duration::from_secs(5), answer)
            .await
            .expect("the fold drains pages while the fetch runs")
            .unwrap();
        assert_eq!(rows, [0, 1, 2, 3, 4]);
        assert!(unanswered.is_empty());
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
        )
            .prop_map(|(details, missing, refused)| DetailsGathered {
                details: details
                    .into_iter()
                    .map(|symbol| {
                        SecurityDetails::new(symbol, None, None, None, None, None, None, None)
                    })
                    .collect(),
                missing,
                refused: refused
                    .into_iter()
                    .map(|(ticker, cause)| RefusedRow::new(&ticker, cause))
                    .collect(),
            })
    }

    proptest::proptest! {
        /// Batches gather in the order they were asked, whatever grouping the concurrency happened to merge them in.
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
        let answer = in_batches(&[], size(4), size(2), |_| async {
            Err::<Names, _>(FetchError::Malformed {
                reason: "fetched nothing".to_string(),
            })
        })
        .await;
        assert_eq!(answer, Ok(Names(Vec::new())));
    }
}
