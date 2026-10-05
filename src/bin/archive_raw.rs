//! Copies Massive's flat files into the archive's raw stage, and verifies each copy against the legacy archiver's.
//! `archive_raw copy <dataset> <first> <last> <concurrency>` copies every listed session not yet held;
//! `archive_raw verify <dataset>` compares checksums. Exits 0 when everything succeeded, 1 when not, 2 on bad usage.

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::Arc;

use chrono::{NaiveDate, Utc};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::Instrument;
use uuid::Uuid;

use fund::archive::bars::{Provenance, Subscription};
use fund::archive::raw::{CopyError, Stored};
use fund::archive::{Archive, ArchiveError};
use fund::common::journal::{Commit, RunId};
use fund::common::storage::Key;
use fund::common::time::SessionDate;
use fund::ingest::flat_files::{FlatFileDataset, FlatFiles};
use fund::journal::built_commit;

const REFUSED_TO_START: u8 = 2;

/// Sessions named in one log line; the count beside them says how many there are.
const LISTED_SESSIONS: usize = 50;

/// Metadata reads in flight during a verify.
const VERIFY_CONCURRENCY: usize = 64;

enum Command {
    Copy {
        dataset: FlatFileDataset,
        first: SessionDate,
        last: SessionDate,
        concurrency: usize,
    },
    Verify {
        dataset: FlatFileDataset,
    },
}

fn parse(arguments: &[String]) -> Option<Command> {
    let date = |raw: &str| {
        NaiveDate::parse_from_str(raw, "%Y-%m-%d")
            .ok()
            .map(SessionDate::from_date)
    };
    match arguments {
        [command, dataset, first, last, concurrency] if command == "copy" => Some(Command::Copy {
            dataset: dataset.parse().ok()?,
            first: date(first)?,
            last: date(last)?,
            concurrency: concurrency
                .parse()
                .ok()
                .filter(|count: &usize| *count > 0)?,
        }),
        [command, dataset] if command == "verify" => Some(Command::Verify {
            dataset: dataset.parse().ok()?,
        }),
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
            "Usage: copy <dataset> <first> <last> <concurrency> | verify <dataset>"
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
                first,
                last,
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
                    first,
                    last,
                    concurrency,
                    run_id,
                    commit,
                )
                .await
            }
            Command::Verify { dataset } => verify(archive, dataset).await,
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
    first: SessionDate,
    last: SessionDate,
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
        .filter(|listed| (first..=last).contains(&listed.session()))
        .collect();
    let owed: Vec<_> = offered
        .iter()
        .filter(|listed| !held.contains(&listed.session()))
        .cloned()
        .collect();
    tracing::info!(
        %dataset,
        %first,
        %last,
        offered = offered.len(),
        offered_first = ?offered.first().map(|listed| listed.session().to_string()),
        offered_last = ?offered.last().map(|listed| listed.session().to_string()),
        held = offered.len() - owed.len(),
        owed = owed.len(),
        owed_bytes = owed.iter().map(|listed| listed.length()).sum::<u64>(),
        concurrency,
        series = dataset.key(first).series(),
        "Planned a raw copy"
    );
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();
    let mut outcomes = BTreeMap::new();
    let mut panicked = 0;
    let mut record = |joined: Result<(SessionDate, Result<Stored, CopyError>), _>| match joined {
        Ok((session, outcome)) => {
            outcomes.insert(session, outcome);
        }
        Err(error) => {
            tracing::error!(%error, "Copy task failed");
            panicked += 1;
        }
    };
    for listed in owed {
        // At most `concurrency` sessions are in flight, so small files overlap while a large one fills every permit.
        while tasks.len() >= concurrency {
            if let Some(joined) = tasks.join_next().await {
                record(joined);
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
        record(joined);
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

/// How one session's two copies compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Comparison {
    Equal,
    Differ,
    /// Either copy lacks a full-object checksum, so only lengths could be compared.
    Unchecksummed,
    NewOnly,
    LegacyOnly,
    Unreadable,
}

async fn verify(archive: Archive, dataset: FlatFileDataset) -> ExitCode {
    let legacy_prefix = dataset.legacy_prefix();
    let (new_sessions, legacy_paths) = match (
        held(&archive, dataset).await,
        archive.list(&legacy_prefix).await,
    ) {
        (Ok(new_sessions), Ok(legacy_paths)) => (new_sessions, legacy_paths),
        (Err(error), _) | (_, Err(error)) => {
            tracing::error!(%error, "Archive not listed");
            return ExitCode::FAILURE;
        }
    };
    let legacy_sessions: BTreeSet<SessionDate> = legacy_paths
        .iter()
        .filter_map(|path| legacy_session(dataset, path))
        .collect();
    let sessions: BTreeSet<SessionDate> = new_sessions.union(&legacy_sessions).copied().collect();
    let permits = Arc::new(Semaphore::new(VERIFY_CONCURRENCY));
    let mut tasks = JoinSet::new();
    for session in sessions {
        let archive = archive.clone();
        let permits = Arc::clone(&permits);
        tasks.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .expect("the semaphore is never closed");
            let new = archive.stored(&dataset.key(session)).await;
            let legacy = archive.stored_at(dataset.legacy_path(session)).await;
            (session, compare(new, legacy))
        });
    }
    let mut by_comparison: BTreeMap<Comparison, Vec<SessionDate>> = BTreeMap::new();
    let mut panicked = 0;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((session, comparison)) => by_comparison.entry(comparison).or_default().push(session),
            Err(error) => {
                tracing::error!(%error, "Comparison task failed");
                panicked += 1;
            }
        }
    }
    for (comparison, sessions) in &mut by_comparison {
        sessions.sort();
        tracing::info!(
            %dataset,
            comparison = ?comparison,
            count = sessions.len(),
            first = ?sessions.first().map(ToString::to_string),
            last = ?sessions.last().map(ToString::to_string),
            "Compared raw copies"
        );
        if !matches!(comparison, Comparison::Equal | Comparison::NewOnly) {
            let listed: Vec<String> = sessions
                .iter()
                .take(LISTED_SESSIONS)
                .map(ToString::to_string)
                .collect();
            tracing::warn!(%dataset, comparison = ?comparison, sessions = listed.join(","), "Sessions needing attention");
        }
    }
    let count = |comparison| by_comparison.get(&comparison).map_or(0, Vec::len);
    let clean = count(Comparison::Differ)
        + count(Comparison::Unchecksummed)
        + count(Comparison::LegacyOnly)
        + count(Comparison::Unreadable)
        + panicked
        == 0;
    tracing::info!(
        %dataset,
        new = new_sessions.len(),
        legacy = legacy_sessions.len(),
        equal = count(Comparison::Equal),
        differ = count(Comparison::Differ),
        unchecksummed = count(Comparison::Unchecksummed),
        new_only = count(Comparison::NewOnly),
        legacy_only = count(Comparison::LegacyOnly),
        unreadable = count(Comparison::Unreadable),
        panicked,
        clean,
        "Verified raw copies"
    );
    if clean {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn compare(
    new: Result<Option<Stored>, ArchiveError>,
    legacy: Result<Option<Stored>, ArchiveError>,
) -> Comparison {
    match (new, legacy) {
        (Err(_), _) | (_, Err(_)) => Comparison::Unreadable,
        (Ok(None), Ok(None)) => Comparison::Unreadable,
        (Ok(Some(_)), Ok(None)) => Comparison::NewOnly,
        (Ok(None), Ok(Some(_))) => Comparison::LegacyOnly,
        (Ok(Some(new)), Ok(Some(legacy))) => match (new.checksum(), legacy.checksum()) {
            _ if new.length() != legacy.length() => Comparison::Differ,
            (Some(new), Some(legacy)) if new == legacy => Comparison::Equal,
            (Some(_), Some(_)) => Comparison::Differ,
            (None, _) | (_, None) => Comparison::Unchecksummed,
        },
    }
}

/// The session a legacy raw path is for, when it is that dataset's data file rather than a sidecar.
fn legacy_session(dataset: FlatFileDataset, path: &str) -> Option<SessionDate> {
    let value = |name: &str| {
        path.split('/')
            .find_map(|segment| segment.strip_prefix(name)?.strip_prefix('='))
    };
    let session = SessionDate::from_date(NaiveDate::from_ymd_opt(
        value("year")?.parse().ok()?,
        value("month")?.parse().ok()?,
        value("day")?.parse().ok()?,
    )?);
    (dataset.legacy_path(session) == path).then_some(session)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(length: u64, checksum: Option<&str>) -> Result<Option<Stored>, ArchiveError> {
        Ok(Some(Stored::new(length, checksum.map(String::from))))
    }

    #[test]
    fn test_each_pair_of_copies_compares_as_one_outcome() {
        let absent = || Ok(None);
        let unreadable = || {
            Err(ArchiveError::Get {
                path: "p".to_string(),
                reason: "r".to_string(),
            })
        };
        let cases = [
            (
                stored(5, Some("a")),
                stored(5, Some("a")),
                Comparison::Equal,
            ),
            (
                stored(5, Some("a")),
                stored(5, Some("b")),
                Comparison::Differ,
            ),
            (
                stored(5, Some("a")),
                stored(6, Some("a")),
                Comparison::Differ,
            ),
            (stored(5, None), stored(6, Some("a")), Comparison::Differ),
            (
                stored(5, Some("a")),
                stored(5, None),
                Comparison::Unchecksummed,
            ),
            (stored(5, Some("a")), absent(), Comparison::NewOnly),
            (absent(), stored(5, Some("a")), Comparison::LegacyOnly),
            (absent(), absent(), Comparison::Unreadable),
            (unreadable(), stored(5, Some("a")), Comparison::Unreadable),
        ];
        for (new, legacy, expected) in cases {
            assert_eq!(compare(new, legacy), expected);
        }
    }

    #[test]
    fn test_only_a_datasets_own_data_file_names_a_legacy_session() {
        let path = "data/raw/massive/equity/quotes/schema=v1/year=2021/month=08/day=23/data.csv.gz";
        assert_eq!(
            legacy_session(FlatFileDataset::Quotes, path),
            Some(SessionDate::from_date(
                NaiveDate::from_ymd_opt(2021, 8, 23).unwrap()
            ))
        );
        assert_eq!(legacy_session(FlatFileDataset::Trades, path), None);
        let sidecar = format!("{path}.provenance.json");
        assert_eq!(legacy_session(FlatFileDataset::Quotes, &sidecar), None);
    }
}
