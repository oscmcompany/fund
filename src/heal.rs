//! The nightly heal: each leg's owed sessions fetched and written to the archive, oldest first, until done or out of
//! time, with each partition journaled once it has been read back.

use std::collections::BTreeMap;
use std::env::VarError;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use strum::IntoEnumIterator;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::archive::Archive;
use crate::archive::bars::{Provenance, Subscription, decode, encode};
use crate::archive::reference::{conditions_key, encode_conditions, latest_conditions};
use crate::archive::{quote_bars, trade_bars};
use crate::common::heal::{Held, Leg, SessionOutcome, WindowRefusal, calendar_range, owed, window};
use crate::common::journal::{
    ConfigurationResolved, HealFinished, Observation, PartitionWritten, Unanswered,
};
use crate::common::market::Symbol;
use crate::common::market::quote_bars::{QuoteFold, QuoteRollup};
use crate::common::market::record::{Bar, BarInterval};
use crate::common::market::trade_bars::{TradeConditions, TradeFold, TradeRollup};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::parameter::{Parameter, ParameterRefusal, at_most, resolve};
use crate::common::storage::Key;
use crate::common::time::SessionDate;
use crate::ingest::alpaca::{
    Alpaca, AlpacaQuoteOutcome, AlpacaTradeOutcome, MinuteBars, TickAnswer, invalid_symbol,
};
use crate::ingest::massive::Massive;
use crate::ingest::{FetchError, refused_by_cause};
use crate::journal::Journal;

const DEFAULT_LOOKBACK_SESSIONS: NonZeroUsize = NonZeroUsize::new(5).expect("5 is not zero");
const DEFAULT_BUDGET_MINUTES: NonZeroU64 = NonZeroU64::new(240).expect("240 is not zero");
const DEFAULT_JOURNAL_DIRECTORY: &str = "/var/journal/fund";
const DEFAULT_LOG_DIRECTORY: &str = "/var/log/fund";
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

    /// Only the log directory, resolved on its own so a refusal of any other parameter still reaches the log file.
    pub fn log_directory_from_environment() -> Result<PathBuf, ParameterRefusal> {
        let (parameter, supplied) = (
            Parameter::LogDirectory,
            environment_variable(Parameter::LogDirectory)?,
        );
        record(
            (parameter, supplied),
            DEFAULT_LOG_DIRECTORY.to_string(),
            &mut BTreeMap::new(),
        )
        .map(PathBuf::from)
    }

    pub fn journal_directory(&self) -> &PathBuf {
        &self.journal_directory
    }

    pub fn log_directory(&self) -> &PathBuf {
        &self.log_directory
    }
}

fn environment_variable(parameter: Parameter) -> Result<Option<String>, ParameterRefusal> {
    match std::env::var(parameter.variable()) {
        Ok(raw) => Ok(Some(raw)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(raw)) => Err(ParameterRefusal::Unparsable {
            parameter,
            raw: raw.to_string_lossy().into_owned(),
            reason: "not unicode".to_string(),
        }),
    }
}

/// Parses one parameter's supplied value, keeping what the journal records for it.
fn record<Value>(
    (parameter, supplied): (Parameter, Option<String>),
    default: Value,
    resolved: &mut BTreeMap<Parameter, crate::common::journal::ResolvedParameter>,
) -> Result<Value, ParameterRefusal>
where
    Value: std::str::FromStr + std::fmt::Display,
    Value::Err: std::fmt::Display,
{
    let (value, parameter_resolved) = resolve(parameter, supplied.as_deref(), default)?;
    resolved.insert(parameter, parameter_resolved);
    Ok(value)
}

/// Why a run did not start: another holds the lock, or the lock could not be taken at all.
#[derive(Debug)]
pub enum LockRefusal {
    Held {
        path: PathBuf,
    },
    Unavailable {
        path: PathBuf,
        error: std::io::Error,
    },
}

impl std::fmt::Display for LockRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Held { path } => write!(formatter, "another run holds {}", path.display()),
            Self::Unavailable { path, error } => {
                write!(formatter, "locking {} failed: {error}", path.display())
            }
        }
    }
}

/// Takes the lock every run must hold, so two runs never write the same sessions at once; the operating system
/// releases it when the returned file closes, including when the process dies.
pub fn lock(directory: &std::path::Path) -> Result<std::fs::File, LockRefusal> {
    let path = directory.join("heal.lock");
    let unavailable = |error| LockRefusal::Unavailable {
        path: path.clone(),
        error,
    };
    let file = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(unavailable)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(LockRefusal::Held { path }),
        Err(std::fs::TryLockError::Error(error)) => Err(unavailable(error)),
    }
}

/// Why the heal could not decide what it owes, so wrote nothing.
#[derive(Debug)]
pub enum HealError {
    Calendar(FetchError),
    Window(WindowRefusal),
    List(crate::archive::ArchiveError),
    /// A partition was written but its record was not, so the run stops rather than write what it cannot record.
    Journal(std::io::Error),
}

impl std::fmt::Display for HealError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Calendar(error) => write!(formatter, "fetching the calendar failed: {error}"),
            Self::Window(refusal) => write!(formatter, "{refusal}"),
            Self::List(error) => write!(formatter, "{error}"),
            Self::Journal(error) => {
                write!(formatter, "journaling a written partition failed: {error}")
            }
        }
    }
}

/// The clients the heal reads from and writes to.
pub struct Clients {
    archive: Archive,
    massive: Massive,
    /// Shared by the concurrent one-minute batches, so its secret is held once rather than copied into each.
    alpaca: Arc<Alpaca>,
}

impl Clients {
    pub fn new(archive: Archive, massive: Massive, alpaca: Alpaca) -> Self {
        Self {
            archive,
            massive,
            alpaca: Arc::new(alpaca),
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
    let (first, last) = calendar_range(today, parameters.lookback_sessions);
    let calendar = clients
        .alpaca
        .calendar(first, last)
        .await
        .map_err(HealError::Calendar)?;
    let window =
        window(&calendar, today, parameters.lookback_sessions).map_err(HealError::Window)?;
    let mut outcomes = BTreeMap::new();
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
        }
        let mut sessions = BTreeMap::new();
        for session in owed(&window, held.sessions()) {
            let outcome = if Instant::now() >= deadline {
                SessionOutcome::Unreached
            } else {
                let hours = calendar
                    .session(session)
                    .map(|trading| trading.hours())
                    .ok_or_else(|| format!("{session} is not in the calendar"));
                match write(leg, session, hours, parameters, clients, journal).await {
                    Ok(written) => {
                        tracing::info!(%leg, %session, bars = written.bars(), refused = ?written.refused(), unanswered = written.unanswered().len(), "Partition written");
                        journal
                            .append(Utc::now(), Observation::PartitionWritten(written))
                            .map_err(HealError::Journal)?;
                        SessionOutcome::Written
                    }
                    Err(cause) => {
                        tracing::warn!(%leg, %session, %cause, "Partition not written");
                        SessionOutcome::Failed { cause }
                    }
                }
            };
            sessions.insert(session, outcome);
        }
        outcomes.insert(leg, sessions);
    }
    Ok(HealFinished::new(window, outcomes))
}

/// Fetches one leg's session and writes it, returning its record, or why it was not written.
async fn write(
    leg: Leg,
    session: SessionDate,
    hours: Result<(DateTime<Utc>, DateTime<Utc>), String>,
    parameters: &Parameters,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, String> {
    let key = leg.key(session);
    let (bars, refused, unanswered, subscription) = match leg {
        Leg::AlpacaQuotes => {
            return write_quotes(session, hours?, parameters, clients, journal).await;
        }
        Leg::AlpacaTrades => return write_trades(session, parameters, clients, journal).await,
        Leg::MassiveDailyBars => {
            let daily = clients
                .massive
                .grouped_daily(session)
                .await
                .map_err(|error| error.to_string())?;
            if !daily.test_tickers().is_empty() {
                tracing::info!(%session, test_tickers = daily.test_tickers().len(), "Exchange test tickers left out");
            }
            let refused = refused_by_cause(daily.refused());
            (
                daily.bars().to_vec(),
                refused,
                BTreeMap::new(),
                Subscription::StocksStarter,
            )
        }
        Leg::AlpacaMinuteBars => {
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
            .await
            .map_err(|error| error.to_string())?;
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
            let refused = refused_by_cause(minute.refused());
            (
                minute.bars().to_vec(),
                refused,
                unanswered,
                Subscription::AlgoTraderPlus,
            )
        }
    };
    // An empty answer for a trading day is a vendor gap, and writing it would mark the session held for good.
    if bars.is_empty() {
        return Err("the vendor answered with no bars".to_string());
    }
    let provenance = Provenance::new(
        subscription,
        Utc::now(),
        journal.run_id(),
        journal.commit().cloned(),
    );
    let body = encode(&key, &bars, &provenance).map_err(|refusal| format!("{refusal:?}"))?;
    clients
        .archive
        .put(&key, body)
        .await
        .map_err(|error| error.to_string())?;
    Ok(PartitionWritten::new(
        leg,
        session,
        u64::try_from(bars.len()).expect("a partition holds fewer than u64::MAX bars"),
        refused,
        unanswered,
    ))
}

/// Keys for a tick leg's bars at every interval, minutes first and the daily, which marks the session held, last.
fn tick_keys(leg_key: &Key) -> [Key; 3] {
    let intervals = [
        BarInterval::OneMinute,
        BarInterval::FiveMinute,
        BarInterval::OneDay,
    ];
    match leg_key {
        Key::Quotes {
            provider,
            origin,
            session,
            ..
        } => intervals.map(|interval| Key::Quotes {
            provider: *provider,
            origin: *origin,
            interval,
            session: *session,
        }),
        Key::Trades {
            provider,
            origin,
            session,
            ..
        } => intervals.map(|interval| Key::Trades {
            provider: *provider,
            origin: *origin,
            interval,
            session: *session,
        }),
        Key::Bars { .. }
        | Key::Reference { .. }
        | Key::RawBars { .. }
        | Key::RawQuotes { .. }
        | Key::RawTrades { .. }
        | Key::Journal { .. }
        | Key::Logs { .. } => unreachable!("a tick leg's key is a quotes or trades key"),
    }
}

/// Folds Alpaca's quotes for every symbol of the session and writes its quote bars.
async fn write_quotes(
    session: SessionDate,
    (open, close): (DateTime<Utc>, DateTime<Utc>),
    parameters: &Parameters,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, String> {
    let symbols = symbol_list(clients, session).await?;
    let mut fold = QuoteFold::new(open, close).map_err(|refusal| format!("{refusal:?}"))?;
    let mut refused = Vec::new();
    let mut one_sided = 0_u64;
    let unanswered = per_symbol(
        &symbols,
        parameters.tick_concurrency,
        |symbol| {
            let alpaca = Arc::clone(&clients.alpaca);
            async move { alpaca.quotes(&symbol, session).await }
        },
        |outcome| match outcome {
            AlpacaQuoteOutcome::Quote(quote) => fold.push(&quote),
            AlpacaQuoteOutcome::OneSided => one_sided += 1,
            AlpacaQuoteOutcome::Refused(row) => refused.push(row),
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let (minutes, counts) = fold.finish();
    if minutes.is_empty() {
        return Err("the vendor answered with no quotes".to_string());
    }
    let provenance = tick_provenance(journal);
    let [minute_key, five_minute_key, daily_key] = tick_keys(&Leg::AlpacaQuotes.key(session));
    let rollup =
        |interval| {
            concatenate(minutes.iter().map(|bar| {
                QuoteRollup::of(bar, interval).expect("minutes roll up to coarser bars")
            }))
            .into_bars()
        };
    for (key, bars) in [
        (minute_key, minutes.clone()),
        (five_minute_key, rollup(BarInterval::FiveMinute)),
        (daily_key, rollup(BarInterval::OneDay)),
    ] {
        let body = quote_bars::encode(&key, &bars, &provenance)
            .map_err(|refusal| format!("{refusal:?}"))?;
        clients
            .archive
            .put(&key, body)
            .await
            .map_err(|error| error.to_string())?;
    }
    tracing::info!(%session, quotes = counts.accepted(), out_of_order = counts.out_of_order(), one_sided, "Alpaca quotes folded");
    Ok(PartitionWritten::new(
        Leg::AlpacaQuotes,
        session,
        u64::try_from(minutes.len()).expect("a partition holds fewer than u64::MAX bars"),
        refused_by_cause(&refused),
        unanswered,
    ))
}

/// Folds Alpaca's trades for every symbol of the session under the newest conditions table and writes its trade bars.
async fn write_trades(
    session: SessionDate,
    parameters: &Parameters,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, String> {
    let symbols = symbol_list(clients, session).await?;
    let conditions = trade_conditions(clients, journal).await?;
    let mut fold = TradeFold::new(session, conditions);
    let mut refused = Vec::new();
    let unanswered = per_symbol(
        &symbols,
        parameters.tick_concurrency,
        |symbol| {
            let alpaca = Arc::clone(&clients.alpaca);
            async move { alpaca.trades(&symbol, session).await }
        },
        |outcome| match outcome {
            AlpacaTradeOutcome::Print {
                print,
                tape,
                letters,
                corrected,
            } => fold.push_lettered(&print, tape, &letters, corrected),
            AlpacaTradeOutcome::Refused(row) => refused.push(row),
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let (minutes, counts) = fold.finish();
    if minutes.is_empty() {
        return Err("the vendor answered with no trades".to_string());
    }
    let provenance = tick_provenance(journal);
    let [minute_key, five_minute_key, daily_key] = tick_keys(&Leg::AlpacaTrades.key(session));
    let rollup =
        |interval| {
            concatenate(minutes.iter().map(|bar| {
                TradeRollup::of(bar, interval).expect("minutes roll up to coarser bars")
            }))
            .into_bars()
        };
    for (key, bars) in [
        (minute_key, minutes.clone()),
        (five_minute_key, rollup(BarInterval::FiveMinute)),
        (daily_key, rollup(BarInterval::OneDay)),
    ] {
        let body = trade_bars::encode(&key, &bars, &provenance)
            .map_err(|refusal| format!("{refusal:?}"))?;
        clients
            .archive
            .put(&key, body)
            .await
            .map_err(|error| error.to_string())?;
    }
    tracing::info!(%session, folded = counts.folded(), corrected = counts.corrected(), unresolved = counts.unresolved(), unsized_prints = counts.unsized_prints(), "Alpaca trades folded");
    Ok(PartitionWritten::new(
        Leg::AlpacaTrades,
        session,
        u64::try_from(minutes.len()).expect("a partition holds fewer than u64::MAX bars"),
        refused_by_cause(&refused),
        unanswered,
    ))
}

/// The newest conditions snapshot, or, when none reads under this build's layout, today's fetched from Massive and
/// written first, so the trade leg never waits on an operator.
async fn trade_conditions(clients: &Clients, journal: &Journal) -> Result<TradeConditions, String> {
    match latest_conditions(&clients.archive).await {
        Ok((_, conditions)) => Ok(conditions),
        Err(reason) => {
            tracing::warn!(reason, "No readable conditions snapshot; fetching today's");
            let fetched_at = Utc::now();
            let conditions = clients
                .massive
                .trade_conditions()
                .await
                .map_err(|error| error.to_string())?;
            let key = conditions_key(SessionDate::at(fetched_at));
            let provenance = Provenance::new(
                Subscription::StocksStarter,
                fetched_at,
                journal.run_id(),
                journal.commit().cloned(),
            );
            let body = encode_conditions(&key, &conditions, &provenance)
                .map_err(|refusal| format!("{refusal:?}"))?;
            clients
                .archive
                .put(&key, body)
                .await
                .map_err(|error| error.to_string())?;
            Ok(conditions)
        }
    }
}

fn tick_provenance(journal: &Journal) -> Provenance {
    Provenance::new(
        Subscription::AlgoTraderPlus,
        Utc::now(),
        journal.run_id(),
        journal.commit().cloned(),
    )
}

/// Fetches each symbol's rows with at most `concurrency` in flight, handing every row to `each` as its symbol
/// finishes. A symbol Alpaca names invalid, or answers with no row filed under it, is returned as unanswered rather
/// than failing the session; any other failure fails it.
async fn per_symbol<Fetch, Pending, Row>(
    symbols: &[Symbol],
    concurrency: NonZeroUsize,
    fetch: Fetch,
    mut each: impl FnMut(Row),
) -> Result<BTreeMap<Symbol, Unanswered>, FetchError>
where
    Fetch: Fn(Symbol) -> Pending,
    Pending: Future<Output = Result<TickAnswer<Row>, FetchError>> + Send + 'static,
    Row: Send + 'static,
{
    let mut pending = symbols.iter().cloned();
    let mut in_flight = JoinSet::new();
    let mut unanswered = BTreeMap::new();
    loop {
        while in_flight.len() < concurrency.get() {
            let Some(symbol) = pending.next() else {
                break;
            };
            let answer = fetch(symbol.clone());
            in_flight.spawn(async move { (symbol, answer.await) });
        }
        match in_flight.join_next().await {
            None => return Ok(unanswered),
            Some(joined) => {
                let (symbol, answer) =
                    joined.unwrap_or_else(|error| std::panic::resume_unwind(error.into_panic()));
                match answer {
                    Ok(answer) => {
                        if !answer.answered {
                            unanswered.insert(symbol, Unanswered::Missing);
                        }
                        answer.outcomes.into_iter().for_each(&mut each);
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
async fn symbol_list(clients: &Clients, session: SessionDate) -> Result<Vec<Symbol>, String> {
    let key = Leg::MassiveDailyBars.key(session);
    let body = clients
        .archive
        .get(&key)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("no daily bars at {} to take the symbols from", key.path()))?;
    let (bars, _) = decode(&key, body).map_err(|refusal| format!("{refusal:?}"))?;
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
    use crate::common::journal::ParameterSource;

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
        let (first, _) = calendar_range(today, MAXIMUM_LOOKBACK_SESSIONS);
        assert_eq!(first.to_string(), "2019-10-30");
        assert!(
            Instant::now()
                .checked_add(Duration::from_secs(1_440 * 60))
                .is_some()
        );
    }

    #[test]
    fn test_a_second_run_is_refused_until_the_first_releases_the_lock() {
        let directory = std::env::temp_dir().join(format!("fund-heal-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let first = lock(&directory).unwrap();
        assert!(matches!(lock(&directory), Err(LockRefusal::Held { .. })));
        drop(first);
        assert!(lock(&directory).is_ok());
        std::fs::remove_dir_all(&directory).unwrap();
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
            |symbol| async move {
                match symbol.as_str() {
                    "AAA" => Ok(TickAnswer {
                        outcomes: vec![1, 2],
                        answered: true,
                    }),
                    // Only rows filed under another ticker: the symbol asked for did not answer.
                    "BBB" => Ok(TickAnswer {
                        outcomes: vec![3],
                        answered: false,
                    }),
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
        assert_eq!(rows, [1, 2, 3]);
        assert_eq!(
            unanswered,
            BTreeMap::from([
                (Symbol::new("BBB").unwrap(), Unanswered::Missing),
                (Symbol::new("CCC").unwrap(), Unanswered::Invalid),
            ])
        );
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
