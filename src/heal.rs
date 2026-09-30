//! The nightly heal: each leg's owed sessions fetched and written to the archive, oldest first, until done or out of
//! time, with each partition journaled once it has been read back.

use std::collections::BTreeMap;
use std::env::VarError;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use strum::IntoEnumIterator;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::archive::Archive;
use crate::archive::bars::{Provenance, Subscription, decode, encode};
use crate::common::heal::{Held, Leg, SessionOutcome, WindowRefusal, calendar_range, owed, window};
use crate::common::journal::{
    ConfigurationResolved, HealFinished, Observation, PartitionWritten, Unanswered,
};
use crate::common::market::Symbol;
use crate::common::market::record::Bar;
use crate::common::monoid::{Monoid, concatenate};
use crate::common::parameter::{Parameter, ParameterRefusal, resolve};
use crate::common::time::SessionDate;
use crate::ingest::alpaca::{Alpaca, MinuteBars};
use crate::ingest::massive::Massive;
use crate::ingest::{FetchError, refused_by_cause};
use crate::journal::Journal;

const DEFAULT_LOOKBACK_SESSIONS: NonZeroUsize = NonZeroUsize::new(5).expect("5 is not zero");
const DEFAULT_BUDGET_MINUTES: NonZeroU64 = NonZeroU64::new(240).expect("240 is not zero");
const DEFAULT_JOURNAL_DIRECTORY: &str = "/var/journal/fund";
/// A whole-market session measured 2026-09-30 at about 33 s with these two.
const DEFAULT_MINUTE_BATCH_SYMBOLS: NonZeroUsize = NonZeroUsize::new(200).expect("200 is not zero");
const DEFAULT_MINUTE_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(8).expect("8 is not zero");

/// The heal's settings, each resolved once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameters {
    lookback_sessions: NonZeroUsize,
    budget: Duration,
    journal_directory: PathBuf,
    minute_batch_symbols: NonZeroUsize,
    minute_concurrency: NonZeroUsize,
}

impl Parameters {
    /// Reads each parameter's variable, returning the configuration the journal records for them.
    pub fn from_environment() -> Result<(Self, ConfigurationResolved), ParameterRefusal> {
        let mut resolved = BTreeMap::new();
        let budget_minutes: NonZeroU64 = read(
            Parameter::BudgetMinutes,
            DEFAULT_BUDGET_MINUTES,
            &mut resolved,
        )?;
        let parameters = Self {
            lookback_sessions: read(
                Parameter::LookbackSessions,
                DEFAULT_LOOKBACK_SESSIONS,
                &mut resolved,
            )?,
            budget: Duration::from_secs(budget_minutes.get().saturating_mul(60)),
            journal_directory: PathBuf::from(read(
                Parameter::JournalDirectory,
                DEFAULT_JOURNAL_DIRECTORY.to_string(),
                &mut resolved,
            )?),
            minute_batch_symbols: read(
                Parameter::MinuteBatchSymbols,
                DEFAULT_MINUTE_BATCH_SYMBOLS,
                &mut resolved,
            )?,
            minute_concurrency: read(
                Parameter::MinuteConcurrency,
                DEFAULT_MINUTE_CONCURRENCY,
                &mut resolved,
            )?,
        };
        Ok((parameters, ConfigurationResolved::new(resolved)))
    }

    pub fn journal_directory(&self) -> &PathBuf {
        &self.journal_directory
    }
}

fn read<Value>(
    parameter: Parameter,
    default: Value,
    resolved: &mut BTreeMap<Parameter, crate::common::journal::ResolvedParameter>,
) -> Result<Value, ParameterRefusal>
where
    Value: std::str::FromStr + std::fmt::Display,
    Value::Err: std::fmt::Display,
{
    let supplied = match std::env::var(parameter.variable()) {
        Ok(raw) => Some(raw),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(raw)) => {
            return Err(ParameterRefusal::Unparsable {
                parameter,
                raw: raw.to_string_lossy().into_owned(),
                reason: "not unicode".to_string(),
            });
        }
    };
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
                match write(leg, session, parameters, clients, journal).await {
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
    parameters: &Parameters,
    clients: &Clients,
    journal: &Journal,
) -> Result<PartitionWritten, String> {
    let key = leg.key(session);
    let (bars, refused, unanswered, subscription) = match leg {
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
