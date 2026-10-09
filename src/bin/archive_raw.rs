//! Massive's flat files into the archive while the Advanced keys last: `copy` fetches each raw session not held, `parse`,
//! `fold-quotes`, `fold-trades` and `roll-up` derive bars from them, `fetch-conditions` stores the trade conditions and
//! `delete` removes keyed objects. Exits 0 when everything succeeded, 1 when not, 2 on bad usage.

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::Instrument;
use uuid::Uuid;

use fund::archive::bars;
use fund::archive::bars::{Provenance, Subscription, encode};
use fund::archive::raw::CopyError;
use fund::archive::{Archive, ArchiveError, DecodeRefusal, EncodeRefusal};
use fund::archive::{quote_bars, reference, trade_bars};
use fund::common::journal::{Commit, RunId};
use fund::common::market::aggregate::{self, session_bars};
use fund::common::market::quote_bars::QuoteFold;
use fund::common::market::record::BarInterval;
use fund::common::market::trade_bars::{TradeConditions, TradeFold};
use fund::common::monoid::{Monoid, Tally};
use fund::common::storage::{Key, Origin, Provider};
use fund::common::time::calendar::TradingCalendar;
use fund::common::time::{SessionDate, SessionRange};
use fund::ingest::RowRefusalKind;
use fund::ingest::alpaca::Alpaca;
use fund::ingest::flat_files::{
    BarFile, FlatFileDataset, FlatFileStream, FlatFiles, ParseRefusal, QuoteRowOutcome,
    TradeRowOutcome, read_quotes, read_trades,
};
use fund::ingest::massive::Massive;
use fund::ingest::refused_by_cause;
use fund::journal::built_commit;

const REFUSED_TO_START: u8 = 2;

/// The first session the nightly heal writes Massive's REST daily bars for, which carry the dollar volume a flat
/// file lacks; a create-only parse written first would hold that session against them.
const HEAL_DAILY_BARS_FROM: NaiveDate = match NaiveDate::from_ymd_opt(2026, 9, 28) {
    Some(date) => date,
    None => panic!("2026-09-28 is a date"),
};

/// Sessions named in one log line; the count beside them says how many there are.
const LISTED_SESSIONS: usize = 50;

/// Ranged reads queued ahead of the parser per streamed file; at sixteen mebibytes each, roughly the bytes held ahead of it.
const STREAM_AHEAD: usize = 8;

enum Command {
    Copy {
        dataset: FlatFileDataset,
        range: SessionRange,
        concurrency: usize,
    },
    Parse {
        file: BarFile,
        range: SessionRange,
        concurrency: usize,
    },
    FoldQuotes {
        range: SessionRange,
        concurrency: usize,
    },
    FetchConditions,
    /// Deletes the objects at paths that each parse as a key.
    Delete {
        keys: Vec<Key>,
    },
    RollUp {
        range: SessionRange,
        concurrency: usize,
    },
    FoldTrades {
        range: SessionRange,
        concurrency: usize,
    },
}

fn parse(arguments: &[String]) -> Option<Command> {
    let date = |raw: &str| {
        NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .ok()
            .map(SessionDate::from_date)
    };
    let range = |first: &str, last: &str| SessionRange::new(date(first)?, date(last)?).ok();
    match arguments {
        [command, dataset, first, last, concurrency] if command == "copy" => Some(Command::Copy {
            dataset: dataset.parse().ok()?,
            range: range(first, last)?,
            concurrency: concurrency
                .parse()
                .ok()
                .filter(|count: &usize| *count > 0)?,
        }),
        [command, dataset, first, last, concurrency] if command == "parse" => {
            Some(Command::Parse {
                file: dataset.parse().ok()?,
                range: range(first, last)?,
                concurrency: concurrency
                    .parse()
                    .ok()
                    .filter(|count: &usize| *count > 0)?,
            })
        }
        [command] if command == "fetch-conditions" => Some(Command::FetchConditions),
        [command, paths @ ..] if command == "delete" && !paths.is_empty() => {
            Some(Command::Delete {
                keys: paths
                    .iter()
                    .map(|path| Key::parse(path).ok())
                    .collect::<Option<_>>()?,
            })
        }
        [command, first, last, concurrency] if command == "roll-up" => Some(Command::RollUp {
            range: range(first, last)?,
            concurrency: concurrency
                .parse()
                .ok()
                .filter(|count: &usize| *count > 0)?,
        }),
        [command, first, last, concurrency] if command == "fold-trades" => {
            Some(Command::FoldTrades {
                range: range(first, last)?,
                concurrency: concurrency
                    .parse()
                    .ok()
                    .filter(|count: &usize| *count > 0)?,
            })
        }
        [command, first, last, concurrency] if command == "fold-quotes" => {
            Some(Command::FoldQuotes {
                range: range(first, last)?,
                concurrency: concurrency
                    .parse()
                    .ok()
                    .filter(|count: &usize| *count > 0)?,
            })
        }
        _ => None,
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .init();
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = parse(&arguments) else {
        tracing::error!(
            ?arguments,
            "Usage: copy <dataset> <first> <last> <concurrency> | parse <dataset> <first> <last> <concurrency> | fold-quotes <first> <last> <concurrency> | fetch-conditions | delete <path>... | roll-up <first> <last> <concurrency> | fold-trades <first> <last> <concurrency>"
        );
        return ExitCode::from(REFUSED_TO_START);
    };
    let run_id = RunId::new(Uuid::new_v4());
    let commit = built_commit();
    let span = tracing::info_span!(
        "run",
        %run_id,
        commit = commit.as_ref().map_or("unknown", Commit::as_str),
    );
    async move {
        let configuration = aws_config::load_from_env().await;
        let archive = match Archive::market_data(&configuration) {
            Ok(archive) => archive,
            Err(refusal) => {
                tracing::error!(%refusal, "Archive configuration refused");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        match command {
            Command::Copy {
                dataset,
                range,
                concurrency,
            } => {
                let flat_files = match FlatFiles::from_environment() {
                    Ok(flat_files) => flat_files,
                    Err(refusal) => {
                        tracing::error!(%refusal, "Flat-file configuration refused");
                        return ExitCode::from(REFUSED_TO_START);
                    }
                };
                copy(
                    &archive,
                    &flat_files,
                    dataset,
                    range,
                    concurrency,
                    run_id,
                    commit,
                )
                .await
            }
            Command::RollUp { range, concurrency } => roll_up(archive, range, concurrency).await,
            Command::Delete { keys } => {
                let mut failed = 0;
                for key in &keys {
                    match archive.delete(key).await {
                        Ok(()) => tracing::info!(path = key.path(), "Deleted an object"),
                        Err(error) => {
                            failed += 1;
                            tracing::error!(%error, "Object not deleted");
                        }
                    }
                }
                if failed == 0 {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            Command::FetchConditions => match Massive::from_environment(reqwest::Client::new()) {
                Ok(massive) => fetch_conditions(&archive, &massive, run_id, commit).await,
                Err(refusal) => {
                    tracing::error!(%refusal, "Client configuration refused");
                    ExitCode::from(REFUSED_TO_START)
                }
            },
            Command::FoldTrades { range, concurrency } => {
                let Some((flat_files, alpaca)) = fold_clients() else {
                    return ExitCode::from(REFUSED_TO_START);
                };
                fold_trades(
                    &archive,
                    &flat_files,
                    &alpaca,
                    range,
                    concurrency,
                    run_id,
                    commit,
                )
                .await
            }
            Command::FoldQuotes { range, concurrency } => {
                let Some((flat_files, alpaca)) = fold_clients() else {
                    return ExitCode::from(REFUSED_TO_START);
                };
                fold_quotes(
                    &archive,
                    &flat_files,
                    &alpaca,
                    range,
                    concurrency,
                    run_id,
                    commit,
                )
                .await
            }
            Command::Parse {
                file,
                range,
                concurrency,
            } => parse_bars(archive, file, range, concurrency, run_id, commit).await,
        }
    }
    .instrument(span)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn copy(
    archive: &Archive,
    flat_files: &FlatFiles,
    dataset: FlatFileDataset,
    range: SessionRange,
    concurrency: usize,
    run_id: RunId,
    commit: Option<Commit>,
) -> ExitCode {
    let listing = match flat_files.listing(dataset).await {
        Ok(listing) => listing,
        Err(error) => {
            tracing::error!(%error, "Flat files not listed");
            return ExitCode::FAILURE;
        }
    };
    let held = match held(archive, dataset).await {
        Ok(held) => held,
        Err(error) => {
            tracing::error!(%error, "Archive not listed");
            return ExitCode::FAILURE;
        }
    };
    let offered: Vec<_> = listing
        .into_iter()
        .filter(|listed| range.contains(listed.session()))
        .collect();
    let owed: Vec<_> = offered
        .iter()
        .filter(|listed| !held.contains(&listed.session()))
        .cloned()
        .collect();
    tracing::info!(
        %dataset,
        first = %range.first(),
        last = %range.last(),
        offered = offered.len(),
        offered_first = ?offered.first().map(|listed| listed.session().to_string()),
        offered_last = ?offered.last().map(|listed| listed.session().to_string()),
        held = offered.len() - owed.len(),
        owed = owed.len(),
        owed_bytes = owed.iter().map(|listed| listed.length()).sum::<u64>(),
        concurrency,
        series = dataset.key(range.first()).series(),
        "Planned a raw copy"
    );
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();
    let mut outcomes = BTreeMap::new();
    let mut panicked = 0;
    for listed in owed {
        // At most `concurrency` sessions are in flight, so small files overlap while a large one fills every permit.
        while tasks.len() >= concurrency {
            if let Some(joined) = tasks.join_next().await {
                record(joined, &mut outcomes, &mut panicked);
            }
        }
        let archive = archive.clone();
        let flat_files = flat_files.clone();
        let permits = Arc::clone(&permits);
        let commit = commit.clone();
        tasks.spawn(
            async move {
                let started = tokio::time::Instant::now();
                let provenance =
                    Provenance::new(Subscription::StocksAdvanced, Utc::now(), run_id, commit);
                let outcome = archive
                    .copy_flat_file(&flat_files, dataset, &listed, &provenance, permits)
                    .await;
                let seconds = started.elapsed().as_secs_f64();
                match &outcome {
                    Ok(stored) => tracing::info!(
                        session = %listed.session(),
                        bytes = stored.length(),
                        seconds,
                        megabytes_per_second = stored.length() as f64 / 1e6 / seconds,
                        "Copied a raw file"
                    ),
                    Err(error) => {
                        tracing::error!(session = %listed.session(), %error, "Raw file not copied")
                    }
                }
                (listed.session(), outcome)
            }
            .in_current_span(),
        );
    }
    while let Some(joined) = tasks.join_next().await {
        record(joined, &mut outcomes, &mut panicked);
    }
    let failed: Vec<String> = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.is_err())
        .map(|(session, _)| session.to_string())
        .collect();
    let contended = outcomes
        .values()
        .filter(|outcome| {
            matches!(
                outcome,
                Err(CopyError::Archive(ArchiveError::Contended { .. }))
            )
        })
        .count();
    tracing::info!(
        %dataset,
        copied = outcomes.len() - failed.len(),
        failed = failed.len(),
        contended,
        panicked,
        failed_sessions = failed.join(","),
        "Finished a raw copy"
    );
    if failed.is_empty() && panicked == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// The sessions the raw stage holds for `dataset`.
async fn held(
    archive: &Archive,
    dataset: FlatFileDataset,
) -> Result<BTreeSet<SessionDate>, ArchiveError> {
    let series = dataset.key(SessionDate::from_date(NaiveDate::MIN)).series();
    Ok(archive
        .list(&series)
        .await?
        .iter()
        .filter_map(|path| Key::parse(path).ok())
        .map(|key| key.session())
        .collect())
}

/// Why a held raw bar file was not written as bars.
#[derive(Debug)]
enum ParseFailure {
    Archive(ArchiveError),
    /// Listed a moment ago, gone now.
    Missing,
    /// The raw copy records no fetch time, which the bars' provenance needs.
    Unstamped,
    Parse(ParseRefusal),
    /// No row became a bar, so writing the session would mark it done with nothing in it.
    Empty {
        test_tickers: usize,
        refused: usize,
    },
    Encode(bars::EncodeRefusal),
    /// The blocking parse did not finish.
    Interrupted(String),
}

impl std::fmt::Display for ParseFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::Missing => write!(formatter, "the raw file is gone"),
            Self::Unstamped => write!(formatter, "the raw file records no fetch time"),
            Self::Parse(refusal) => write!(formatter, "{refusal}"),
            Self::Empty {
                test_tickers,
                refused,
            } => write!(
                formatter,
                "no bars: {test_tickers} test tickers and {refused} refused rows"
            ),
            Self::Interrupted(reason) => write!(formatter, "the parse did not finish: {reason}"),
            Self::Encode(refusal) => write!(formatter, "{refusal:?}"),
        }
    }
}

async fn parse_bars(
    archive: Archive,
    file: BarFile,
    range: SessionRange,
    concurrency: usize,
    run_id: RunId,
    commit: Option<Commit>,
) -> ExitCode {
    let (dataset, interval) = (file.dataset(), file.interval());
    let heal_start = SessionDate::from_date(HEAL_DAILY_BARS_FROM);
    match file {
        BarFile::Daily if range.last() >= heal_start => {
            tracing::error!(last = %range.last(), %heal_start, "Daily bars from this session on are the nightly heal's to write");
            return ExitCode::from(REFUSED_TO_START);
        }
        BarFile::Daily | BarFile::Minute => {}
    }
    let bars_key = move |session| Key::Bars {
        provider: Provider::Massive,
        origin: Origin::Vendor,
        interval,
        session,
    };
    let series = bars_key(range.first()).series();
    let (raw, parsed) = match (held(&archive, dataset).await, archive.list(&series).await) {
        (Ok(raw), Ok(parsed)) => (
            raw,
            parsed
                .iter()
                .filter_map(|path| Key::parse(path).ok())
                .map(|key| key.session())
                .collect::<BTreeSet<_>>(),
        ),
        (Err(error), _) | (_, Err(error)) => {
            tracing::error!(%error, "Archive not listed");
            return ExitCode::FAILURE;
        }
    };
    let offered: Vec<SessionDate> = raw
        .into_iter()
        .filter(|session| range.contains(*session))
        .collect();
    let (already, owed): (Vec<SessionDate>, Vec<SessionDate>) =
        offered.iter().partition(|session| parsed.contains(session));
    tracing::info!(
        %dataset,
        first = %range.first(),
        last = %range.last(),
        offered = offered.len(),
        offered_first = ?offered.first().map(ToString::to_string),
        offered_last = ?offered.last().map(ToString::to_string),
        already_held = already.len(),
        already_held_sessions = listed(&already),
        owed = owed.len(),
        concurrency,
        series,
        "Planned a raw parse"
    );
    let mut tasks = JoinSet::new();
    let mut outcomes = BTreeMap::new();
    let mut panicked = 0;
    for session in owed {
        while tasks.len() >= concurrency {
            if let Some(joined) = tasks.join_next().await {
                record(joined, &mut outcomes, &mut panicked);
            }
        }
        let archive = archive.clone();
        let commit = commit.clone();
        let key = bars_key(session);
        tasks.spawn(
            async move {
                let outcome = parse_one(&archive, file, &key, run_id, commit).await;
                match &outcome {
                    Ok(()) => {}
                    Err(failure) => {
                        tracing::error!(session = %session, %failure, "Raw file not parsed")
                    }
                }
                (session, outcome)
            }
            .in_current_span(),
        );
    }
    while let Some(joined) = tasks.join_next().await {
        record(joined, &mut outcomes, &mut panicked);
    }
    let failed: Vec<SessionDate> = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.is_err())
        .map(|(session, _)| *session)
        .collect();
    let contended: Vec<SessionDate> = outcomes
        .iter()
        .filter(|(_, outcome)| {
            matches!(
                outcome,
                Err(ParseFailure::Archive(ArchiveError::Contended { .. }))
            )
        })
        .map(|(session, _)| *session)
        .collect();
    tracing::info!(
        %dataset,
        written = outcomes.len() - failed.len(),
        failed = failed.len(),
        failed_sessions = listed(&failed),
        contended = contended.len(),
        contended_sessions = listed(&contended),
        panicked,
        "Finished a raw parse"
    );
    if failed.is_empty() && panicked == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn record<T>(
    joined: Result<(SessionDate, T), tokio::task::JoinError>,
    outcomes: &mut BTreeMap<SessionDate, T>,
    panicked: &mut usize,
) {
    match joined {
        Ok((session, outcome)) => {
            outcomes.insert(session, outcome);
        }
        Err(error) => {
            tracing::error!(%error, "Task failed");
            *panicked += 1;
        }
    }
}

/// The first sessions of `sessions`, comma-joined for one log field.
fn listed(sessions: &[SessionDate]) -> String {
    sessions
        .iter()
        .take(LISTED_SESSIONS)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Reads one held raw file, parses it, and creates its bars under `key`, which must be unwritten.
async fn parse_one(
    archive: &Archive,
    file: BarFile,
    key: &Key,
    run_id: RunId,
    commit: Option<Commit>,
) -> Result<(), ParseFailure> {
    let session = key.session();
    let raw_key = file.dataset().key(session);
    let fetched_at = archive
        .stored(&raw_key)
        .await
        .map_err(ParseFailure::Archive)?
        .ok_or(ParseFailure::Missing)?
        .fetched_at()
        .ok_or(ParseFailure::Unstamped)?;
    let gzipped = archive
        .get(&raw_key)
        .await
        .map_err(ParseFailure::Archive)?
        .ok_or(ParseFailure::Missing)?;
    let provenance = Provenance::new(Subscription::StocksAdvanced, fetched_at, run_id, commit);
    let encoded_key = key.clone();
    // Decompressing, parsing and encoding a minute file is seconds of CPU, which belongs off the async workers.
    let (parsed, body) = tokio::task::spawn_blocking(move || {
        let parsed = file
            .parse_bars(&gzipped, session)
            .map_err(ParseFailure::Parse)?;
        if parsed.bars().is_empty() {
            return Err(ParseFailure::Empty {
                test_tickers: parsed.test_tickers().len(),
                refused: parsed.refused().len(),
            });
        }
        let body =
            encode(&encoded_key, parsed.bars(), &provenance).map_err(ParseFailure::Encode)?;
        Ok((parsed, body))
    })
    .await
    .map_err(|error| ParseFailure::Interrupted(error.to_string()))??;
    archive
        .create(key, body)
        .await
        .map_err(ParseFailure::Archive)?;
    tracing::info!(
        session = %session,
        bars = parsed.bars().len(),
        test_tickers = parsed.test_tickers().len(),
        refused = parsed.refused().len(),
        refused_by_cause = %refused_by_cause(parsed.refused()),
        "Parsed a raw file"
    );
    Ok(())
}

/// Where a session's quote bars at `interval` are written.
fn quote_key(interval: BarInterval, session: SessionDate) -> Key {
    Key::Quotes {
        provider: Provider::Massive,
        origin: Origin::Derived,
        interval,
        session,
    }
}

/// The flat files to stream and the calendar to plan by, or `None` once the refusal is logged.
fn fold_clients() -> Option<(FlatFiles, Alpaca)> {
    match (
        FlatFiles::from_environment(),
        Alpaca::from_environment(reqwest::Client::new()),
    ) {
        (Ok(flat_files), Ok(alpaca)) => Some((flat_files, alpaca)),
        (Err(refusal), _) | (_, Err(refusal)) => {
            tracing::error!(%refusal, "Client configuration refused");
            None
        }
    }
}

/// Why a session's quotes or trades were not folded and written.
#[derive(Debug)]
enum FoldFailure {
    Parse(ParseRefusal),
    Encode(EncodeRefusal),
    Archive(ArchiveError),
    Interrupted(String),
    Empty,
    /// An earlier run wrote different bars under the key, which this run will not replace.
    Held {
        key: Key,
    },
    /// An earlier run wrote the key, and what it holds does not read back as bars.
    Unreadable {
        key: Key,
        refusal: Box<DecodeRefusal>,
    },
}

impl std::fmt::Display for FoldFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(refusal) => write!(formatter, "{refusal}"),
            Self::Encode(refusal) => write!(formatter, "{refusal}"),
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::Interrupted(reason) => write!(formatter, "the fold did not finish: {reason}"),
            Self::Empty => write!(formatter, "nothing the fold could keep fell in the session"),
            Self::Held { key } => write!(formatter, "{} already holds different bars", key.path()),
            Self::Unreadable { key, refusal } => {
                write!(formatter, "{} held but unreadable: {refusal}", key.path())
            }
        }
    }
}

/// Streams each listed `dataset` file of a trading session in `range` whose daily file under `daily` is not
/// yet written, and hands it to `fold`; `noun` names the bars in the log.
#[allow(clippy::too_many_arguments)]
async fn fold_sessions<Fold, Folding>(
    archive: &Archive,
    flat_files: &FlatFiles,
    calendar: &TradingCalendar,
    dataset: FlatFileDataset,
    daily: Key,
    range: SessionRange,
    concurrency: usize,
    noun: &'static str,
    mut fold: Fold,
) -> ExitCode
where
    Fold: FnMut(SessionDate, FlatFileStream) -> Folding,
    Folding: Future<Output = Result<(), FoldFailure>> + Send + 'static,
{
    let (listing, written) = match (
        flat_files.listing(dataset).await,
        archive.list(&daily.series()).await,
    ) {
        (Ok(listing), Ok(written)) => (listing, written),
        (Err(error), _) => {
            tracing::error!(%error, "Flat files not listed");
            return ExitCode::FAILURE;
        }
        (_, Err(error)) => {
            tracing::error!(%error, "Archive not listed");
            return ExitCode::FAILURE;
        }
    };
    // The daily file is written last, so a session holding it holds all three.
    let done: BTreeSet<SessionDate> = written
        .iter()
        .filter_map(|path| Key::parse(path).ok())
        .map(|key| key.session())
        .collect();
    let offered: Vec<_> = listing
        .into_iter()
        .filter(|listed| range.contains(listed.session()))
        .collect();
    let untraded: Vec<SessionDate> = offered
        .iter()
        .map(|listed| listed.session())
        .filter(|session| !calendar.is_trading_day(*session))
        .collect();
    let owed: Vec<_> = offered
        .iter()
        .filter(|listed| {
            calendar.is_trading_day(listed.session()) && !done.contains(&listed.session())
        })
        .cloned()
        .collect();
    tracing::info!(
        first = %range.first(),
        last = %range.last(),
        offered = offered.len(),
        already_folded = offered.len() - owed.len() - untraded.len(),
        not_trading_days = listed(&untraded),
        owed = owed.len(),
        owed_bytes = owed.iter().map(|listed| listed.length()).sum::<u64>(),
        concurrency,
        "Planned a {noun} fold"
    );
    let mut tasks = JoinSet::new();
    let mut outcomes = BTreeMap::new();
    let mut panicked = 0;
    for listed_file in owed {
        while tasks.len() >= concurrency {
            if let Some(joined) = tasks.join_next().await {
                record(joined, &mut outcomes, &mut panicked);
            }
        }
        let session = listed_file.session();
        let folding = fold(
            session,
            flat_files.stream(dataset, &listed_file, STREAM_AHEAD),
        );
        tasks.spawn(
            async move {
                let started = tokio::time::Instant::now();
                let outcome = folding.await;
                match &outcome {
                    Ok(()) => tracing::info!(
                        session = %session,
                        seconds = started.elapsed().as_secs_f64(),
                        "Folded a {noun} file"
                    ),
                    Err(failure) => {
                        tracing::error!(session = %session, %failure, bars = noun, "File not folded")
                    }
                }
                (session, outcome)
            }
            .in_current_span(),
        );
    }
    while let Some(joined) = tasks.join_next().await {
        record(joined, &mut outcomes, &mut panicked);
    }
    let failed: Vec<SessionDate> = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.is_err())
        .map(|(session, _)| *session)
        .collect();
    tracing::info!(
        written = outcomes.len() - failed.len(),
        failed = failed.len(),
        failed_sessions = listed(&failed),
        panicked,
        "Finished a {noun} fold"
    );
    if failed.is_empty() && panicked == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Streams each listed quote file not yet folded from Massive and writes its one-minute, five-minute and daily bars.
#[allow(clippy::too_many_arguments)]
async fn fold_quotes(
    archive: &Archive,
    flat_files: &FlatFiles,
    alpaca: &Alpaca,
    range: SessionRange,
    concurrency: usize,
    run_id: RunId,
    commit: Option<Commit>,
) -> ExitCode {
    let calendar = match alpaca.calendar(range).await {
        Ok(calendar) => calendar,
        Err(error) => {
            tracing::error!(%error, "Calendar not fetched");
            return ExitCode::FAILURE;
        }
    };
    let daily = quote_key(BarInterval::OneDay, range.first());
    fold_sessions(
        archive,
        flat_files,
        &calendar,
        FlatFileDataset::Quotes,
        daily,
        range,
        concurrency,
        "quote",
        |session, stream| {
            let hours = calendar
                .session(session)
                .map(|trading| trading.hours())
                .expect("owed sessions are trading days");
            let archive = archive.clone();
            let provenance = Provenance::new(
                Subscription::StocksAdvanced,
                Utc::now(),
                run_id,
                commit.clone(),
            );
            async move { fold_quotes_one(&archive, stream, session, hours, &provenance).await }
        },
    )
    .await
}
/// Folds one streamed quote file and creates its three files, the daily last.
async fn fold_quotes_one(
    archive: &Archive,
    stream: FlatFileStream,
    session: SessionDate,
    (open, close): (DateTime<Utc>, DateTime<Utc>),
    provenance: &Provenance,
) -> Result<(), FoldFailure> {
    let folded = tokio::task::spawn_blocking(move || {
        let mut fold =
            QuoteFold::new(open, close).expect("a trading session closes after it opens");
        let mut rows = QuoteRowCounts::default();
        read_quotes(stream, |outcome| match outcome {
            QuoteRowOutcome::Quote(quote) => fold.push(&quote),
            QuoteRowOutcome::TestTicker => rows.test_tickers += 1,
            QuoteRowOutcome::OneSided => rows.one_sided += 1,
            QuoteRowOutcome::Refused(row) => rows.refused.add(row.cause().kind()),
        })
        .map_err(FoldFailure::Parse)?;
        let (minutes, counts) = fold.finish();
        Ok::<_, FoldFailure>((minutes, counts, rows))
    })
    .await
    .map_err(|error| FoldFailure::Interrupted(error.to_string()))??;
    let (minutes, counts, rows) = folded;
    if minutes.is_empty() {
        return Err(FoldFailure::Empty);
    }
    let minute_bars = minutes.len();
    let files = session_bars(minutes);
    let symbols = files[2].1.len();
    for (interval, bars) in files {
        let key = quote_key(interval, session);
        let body = quote_bars::encode(&key, &bars, provenance)
            .map_err(|refusal| FoldFailure::Encode(refusal.into()))?;
        create_or_confirm(archive, &key, body, |held| {
            quote_bars::decode(&key, held).map(|(held, _)| held == bars)
        })
        .await?;
    }
    tracing::info!(
        session = %session,
        symbols,
        minute_bars,
        quotes = counts.accepted(),
        out_of_order = counts.out_of_order(),
        one_sided = rows.one_sided,
        test_tickers = rows.test_tickers,
        refused = rows.refused.total(),
        refused_by_cause = %rows.refused,
        "Wrote quote bars"
    );
    Ok(())
}

/// Creates `key`, or, when an interrupted run already wrote it, accepts it only if it holds the same bars, so a
/// rerun finishes a session's missing files without replacing what it cannot tell is identical.
async fn create_or_confirm<Refusal: Into<DecodeRefusal>>(
    archive: &Archive,
    key: &Key,
    body: Vec<u8>,
    same_bars: impl FnOnce(Vec<u8>) -> Result<bool, Refusal>,
) -> Result<(), FoldFailure> {
    match archive.create(key, body).await {
        Ok(()) => Ok(()),
        Err(ArchiveError::Contended { path }) => {
            let held = archive
                .get(key)
                .await
                .map_err(FoldFailure::Archive)?
                .ok_or_else(|| {
                    FoldFailure::Archive(ArchiveError::Contended { path: path.clone() })
                })?;
            match same_bars(held) {
                Ok(true) => {
                    tracing::info!(path, "Kept bars an earlier run wrote identically");
                    Ok(())
                }
                Ok(false) => Err(FoldFailure::Held { key: key.clone() }),
                Err(refusal) => Err(FoldFailure::Unreadable {
                    key: key.clone(),
                    refusal: Box::new(refusal.into()),
                }),
            }
        }
        Err(error) => Err(FoldFailure::Archive(error)),
    }
}

/// Rows of a quote file that did not become a quote, by what they were.
#[derive(Default)]
struct QuoteRowCounts {
    test_tickers: u64,
    one_sided: u64,
    /// Counted by cause rather than kept, since a session refuses tens of thousands of rows.
    refused: Tally<RowRefusalKind>,
}

/// Fetches Massive's condition table and writes it as today's snapshot.
async fn fetch_conditions(
    archive: &Archive,
    massive: &Massive,
    run_id: RunId,
    commit: Option<Commit>,
) -> ExitCode {
    let fetched_at = Utc::now();
    let conditions = match massive.trade_conditions().await {
        Ok(conditions) => conditions,
        Err(error) => {
            tracing::error!(%error, "Conditions not fetched");
            return ExitCode::FAILURE;
        }
    };
    let key = reference::conditions_key(SessionDate::at(fetched_at));
    let provenance = Provenance::new(Subscription::StocksStarter, fetched_at, run_id, commit);
    let written = reference::encode_conditions(&key, &conditions, &provenance)
        .map_err(EncodeRefusal::Reference)
        .map(|body| archive.create(&key, body));
    match written {
        Ok(write) => match write.await {
            Ok(()) => {
                tracing::info!(
                    path = key.path(),
                    codes = conditions.conditions().len(),
                    "Wrote the conditions table"
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                tracing::error!(%error, "Conditions table not written");
                ExitCode::FAILURE
            }
        },
        Err(refusal) => {
            tracing::error!(%refusal, "Conditions table not encoded");
            ExitCode::FAILURE
        }
    }
}

/// Where a session's trade bars at `interval` are written.
fn trade_key(interval: BarInterval, session: SessionDate) -> Key {
    Key::Trades {
        provider: Provider::Massive,
        origin: Origin::Derived,
        interval,
        session,
    }
}

/// Streams each listed trade file not yet folded from Massive and writes its one-minute, five-minute and daily bars.
#[allow(clippy::too_many_arguments)]
async fn fold_trades(
    archive: &Archive,
    flat_files: &FlatFiles,
    alpaca: &Alpaca,
    range: SessionRange,
    concurrency: usize,
    run_id: RunId,
    commit: Option<Commit>,
) -> ExitCode {
    let (conditions_key, conditions) = match reference::latest_conditions(archive).await {
        Ok(latest) => latest,
        Err(error) => {
            tracing::error!(%error, "Conditions table not read");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(
        conditions = conditions_key.path(),
        codes = conditions.conditions().len(),
        "Read the conditions table"
    );
    let calendar = match alpaca.calendar(range).await {
        Ok(calendar) => calendar,
        Err(error) => {
            tracing::error!(%error, "Calendar not fetched");
            return ExitCode::FAILURE;
        }
    };
    let daily = trade_key(BarInterval::OneDay, range.first());
    fold_sessions(
        archive,
        flat_files,
        &calendar,
        FlatFileDataset::Trades,
        daily,
        range,
        concurrency,
        "trade",
        |session, stream| {
            let archive = archive.clone();
            let conditions = conditions.clone();
            let provenance = Provenance::new(
                Subscription::StocksAdvanced,
                Utc::now(),
                run_id,
                commit.clone(),
            );
            async move { fold_trades_one(&archive, stream, session, conditions, &provenance).await }
        },
    )
    .await
}

/// Folds one streamed trade file and creates its three files, the daily last.
async fn fold_trades_one(
    archive: &Archive,
    stream: FlatFileStream,
    session: SessionDate,
    conditions: TradeConditions,
    provenance: &Provenance,
) -> Result<(), FoldFailure> {
    let folded = tokio::task::spawn_blocking(move || {
        let mut fold = TradeFold::new(session, conditions);
        let mut test_tickers = 0_u64;
        let mut refused = Tally::empty();
        read_trades(stream, |outcome| match outcome {
            TradeRowOutcome::Print {
                print,
                conditions,
                correction,
            } => fold.push(&print, &conditions, correction),
            TradeRowOutcome::TestTicker => test_tickers += 1,
            TradeRowOutcome::Refused(row) => refused.add(row.cause().kind()),
        })
        .map_err(FoldFailure::Parse)?;
        let (minutes, counts) = fold.finish();
        Ok::<_, FoldFailure>((minutes, counts, test_tickers, refused))
    })
    .await
    .map_err(|error| FoldFailure::Interrupted(error.to_string()))??;
    let (minutes, counts, test_tickers, refused) = folded;
    if minutes.is_empty() {
        return Err(FoldFailure::Empty);
    }
    let minute_bars = minutes.len();
    let files = session_bars(minutes);
    let symbols = files[2].1.len();
    for (interval, bars) in files {
        let key = trade_key(interval, session);
        let body = trade_bars::encode(&key, &bars, provenance)
            .map_err(|refusal| FoldFailure::Encode(refusal.into()))?;
        create_or_confirm(archive, &key, body, |held| {
            trade_bars::decode(&key, held).map(|(held, _)| held == bars)
        })
        .await?;
    }
    tracing::info!(
        session = %session,
        symbols,
        minute_bars,
        folded = counts.folded(),
        other_session = counts.other_session(),
        withdrawn = counts.withdrawn(),
        volume_ineligible = counts.volume_ineligible(),
        unsized_prints = counts.unsized_prints(),
        unresolved = counts.unresolved().total(),
        unresolved_by_cause = %counts.unresolved(),
        test_tickers,
        refused = refused.total(),
        refused_by_cause = %refused,
        "Wrote trade bars"
    );
    Ok(())
}

/// Massive's bars at `interval` and `origin` for a session.
fn massive_bars_key(origin: Origin, interval: BarInterval, session: SessionDate) -> Key {
    Key::Bars {
        provider: Provider::Massive,
        origin,
        interval,
        session,
    }
}

/// Why a session's minutes were not rolled up.
#[derive(Debug)]
enum RollUpFailure {
    Archive(ArchiveError),
    Missing,
    Decode(bars::DecodeRefusal),
    Encode(bars::EncodeRefusal),
    /// The roll-up's task panicked or was canceled before it finished.
    Interrupted(String),
}

impl std::fmt::Display for RollUpFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::Missing => write!(formatter, "the minute bars are gone"),
            Self::Decode(refusal) => write!(formatter, "minute bars not read: {refusal:?}"),
            Self::Encode(refusal) => write!(formatter, "five-minute bars not encoded: {refusal:?}"),
            Self::Interrupted(reason) => write!(formatter, "the roll-up did not finish: {reason}"),
        }
    }
}

/// Writes derived five-minute bars for each session whose Massive minute bars are held and five-minute bars are not.
async fn roll_up(archive: Archive, range: SessionRange, concurrency: usize) -> ExitCode {
    let sessions = |origin, interval| {
        let archive = archive.clone();
        async move {
            archive
                .list(&massive_bars_key(origin, interval, range.first()).series())
                .await
                .map(|paths| {
                    paths
                        .iter()
                        .filter_map(|path| Key::parse(path).ok())
                        .map(|key| key.session())
                        .filter(|session| range.contains(*session))
                        .collect::<BTreeSet<SessionDate>>()
                })
        }
    };
    let (minutes, rolled) = match (
        sessions(Origin::Vendor, BarInterval::OneMinute).await,
        sessions(Origin::Derived, BarInterval::FiveMinute).await,
    ) {
        (Ok(minutes), Ok(rolled)) => (minutes, rolled),
        (Err(error), _) | (_, Err(error)) => {
            tracing::error!(%error, "Archive not listed");
            return ExitCode::FAILURE;
        }
    };
    let owed: Vec<SessionDate> = minutes.difference(&rolled).copied().collect();
    tracing::info!(
        first = %range.first(),
        last = %range.last(),
        minute_sessions = minutes.len(),
        already_rolled = minutes.len() - owed.len(),
        owed = owed.len(),
        owed_first = ?owed.first().map(ToString::to_string),
        owed_last = ?owed.last().map(ToString::to_string),
        "Planned a five-minute roll-up"
    );
    let mut tasks = JoinSet::new();
    let mut outcomes = BTreeMap::new();
    let mut panicked = 0;
    for session in owed {
        while tasks.len() >= concurrency {
            if let Some(joined) = tasks.join_next().await {
                record(joined, &mut outcomes, &mut panicked);
            }
        }
        let archive = archive.clone();
        tasks.spawn(
            async move {
                let outcome = roll_up_one(&archive, session).await;
                match &outcome {
                    Ok(bars) => tracing::info!(session = %session, bars, "Rolled up a session"),
                    Err(failure) => {
                        tracing::error!(session = %session, %failure, "Session not rolled up")
                    }
                }
                (session, outcome)
            }
            .in_current_span(),
        );
    }
    while let Some(joined) = tasks.join_next().await {
        record(joined, &mut outcomes, &mut panicked);
    }
    let failed: Vec<SessionDate> = outcomes
        .iter()
        .filter(|(_, outcome)| outcome.is_err())
        .map(|(session, _)| *session)
        .collect();
    tracing::info!(
        written = outcomes.len() - failed.len(),
        failed = failed.len(),
        failed_sessions = listed(&failed),
        panicked,
        "Finished a five-minute roll-up"
    );
    if failed.is_empty() && panicked == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Rolls one session's minute bars up to five-minute bars under the minutes' own provenance.
async fn roll_up_one(archive: &Archive, session: SessionDate) -> Result<usize, RollUpFailure> {
    let minute_key = massive_bars_key(Origin::Vendor, BarInterval::OneMinute, session);
    let bytes = archive
        .get(&minute_key)
        .await
        .map_err(RollUpFailure::Archive)?
        .ok_or(RollUpFailure::Missing)?;
    let key = massive_bars_key(Origin::Derived, BarInterval::FiveMinute, session);
    let encoded_key = key.clone();
    let (count, body) = tokio::task::spawn_blocking(move || {
        let (minutes, provenance) =
            bars::decode(&minute_key, bytes).map_err(RollUpFailure::Decode)?;
        let five_minutes = aggregate::roll_up(&minutes, BarInterval::FiveMinute)
            .expect("minutes roll up to five minutes");
        let body = bars::encode(&encoded_key, &five_minutes, &provenance)
            .map_err(RollUpFailure::Encode)?;
        Ok::<_, RollUpFailure>((five_minutes.len(), body))
    })
    .await
    .map_err(|error| RollUpFailure::Interrupted(error.to_string()))??;
    archive
        .create(&key, body)
        .await
        .map_err(RollUpFailure::Archive)?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_copy_whose_last_session_precedes_its_first_is_bad_usage() {
        let arguments = |first: &str, last: &str| {
            ["copy", "quotes", first, last, "4"]
                .map(String::from)
                .to_vec()
        };
        assert!(parse(&arguments("2021-08-23", "2021-08-23")).is_some());
        assert!(parse(&arguments("2021-08-24", "2021-08-23")).is_none());
        let parsing = ["parse", "daily_bars", "2021-08-24", "2021-08-23", "4"].map(String::from);
        assert!(parse(&parsing).is_none());
        let parse_file =
            |file: &str| parse(&["parse", file, "2021-08-23", "2021-08-24", "4"].map(String::from));
        assert!(matches!(
            parse_file("minute_bars"),
            Some(Command::Parse {
                file: BarFile::Minute,
                ..
            })
        ));
        assert!(parse_file("quotes").is_none());
        let folding = |first: &str, last: &str, concurrency: &str| {
            parse(&["fold-quotes", first, last, concurrency].map(String::from))
        };
        assert!(folding("2021-08-23", "2021-08-23", "9").is_some());
        assert!(folding("2021-08-24", "2021-08-23", "9").is_none());
        assert!(folding("2021-08-23", "2021-08-24", "0").is_none());
        let day = |text: &str| SessionDate::from_date(text.parse().unwrap());
        for command in ["roll-up", "fold-trades", "fold-quotes"] {
            let ranged = |first: &str, last: &str| {
                parse(&[command, first, last, "2"].map(String::from)).map(|command| match command {
                    Command::RollUp { range, .. }
                    | Command::FoldTrades { range, .. }
                    | Command::FoldQuotes { range, .. }
                    | Command::Copy { range, .. }
                    | Command::Parse { range, .. } => Some((range.first(), range.last())),
                    Command::FetchConditions | Command::Delete { .. } => None,
                })
            };
            assert_eq!(
                ranged("2021-08-23", "2021-08-27"),
                Some(Some((day("2021-08-23"), day("2021-08-27")))),
                "{command}"
            );
            assert_eq!(ranged("2021-08-27", "2021-08-23"), None, "{command}");
        }
    }

    #[test]
    fn test_a_delete_names_only_paths_that_parse_as_keys() {
        let key = "data/equity/stage=parsed/trades/provider=massive/origin=derived/interval=one_day/year=2021/month=08/day=23/data.parquet";
        assert!(parse(&["delete".to_string(), key.to_string()]).is_some());
        let legacy =
            "data/derived/equity/trades/interval=one_day/year=2021/month=08/day=23/data.parquet";
        assert!(parse(&["delete".to_string(), key.to_string(), legacy.to_string()]).is_none());
        assert!(parse(&["delete".to_string()]).is_none());
    }
}
