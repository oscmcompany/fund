//! Appends journal records to one JSONL file per session. A record is on disk when `append` returns: the line is
//! written and `sync_data` has run, and every file or directory the journal created has its parent's entry synced.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::common::journal::{Commit, Observation, Record, RunId};
use crate::common::time::SessionDate;

/// Writes one run's records.
pub struct Journal {
    directory: PathBuf,
    run_id: RunId,
    commit: Option<Commit>,
    next_sequence: NonZeroU64,
    open: Option<(SessionDate, File)>,
}

impl Journal {
    /// Starts a run writing into `directory`, creating it if needed.
    pub fn open(directory: impl Into<PathBuf>) -> io::Result<Self> {
        let directory = directory.into();
        let missing: Vec<PathBuf> = directory
            .ancestors()
            .take_while(|path| !path.as_os_str().is_empty() && !path.exists())
            .map(Path::to_path_buf)
            .collect();
        std::fs::create_dir_all(&directory)?;
        // Outermost first, so each entry lands in a parent whose own entry is already durable.
        for created in missing.iter().rev() {
            sync_parent(created)?;
        }
        Ok(Self {
            directory,
            run_id: RunId::new(Uuid::new_v4()),
            commit: built_commit(),
            next_sequence: NonZeroU64::MIN,
            open: None,
        })
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn commit(&self) -> Option<&Commit> {
        self.commit.as_ref()
    }

    /// Returns once the record is durable, so a caller that waits before acting knows the observation survives the
    /// crash the action might cause. Every append consumes a sequence, so a failure leaves a gap, never a duplicate.
    pub fn append(&mut self, timestamp: DateTime<Utc>, observation: Observation) -> io::Result<()> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .expect("a run appends fewer than u64::MAX records");
        let record = Record::new(
            self.run_id,
            sequence,
            timestamp,
            self.commit.clone(),
            observation,
        );
        let written = self.write(&record);
        if written.is_err() {
            self.open = None;
        }
        written
    }

    fn write(&mut self, record: &Record) -> io::Result<()> {
        let session = record.session();
        if self
            .open
            .as_ref()
            .is_none_or(|(open_session, _)| *open_session != session)
        {
            self.open = None;
            let file = open_session_file(&self.directory.join(file_name(session)))?;
            self.open = Some((session, file));
        }
        let (_, file) = self.open.as_mut().expect("a session file was opened above");
        let mut line = record.encode();
        line.push('\n');
        file.write_all(line.as_bytes())?;
        file.sync_data()
    }
}

/// Opens a session file for appending, ending a torn last line so the next record starts on a line of its own.
///
/// The directory is synced on every open, not only on creation, so a sync that failed once is not skipped on retry.
fn open_session_file(path: &Path) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    sync_parent(path)?;
    if ends_mid_line(&mut file)? {
        file.write_all(b"\n")?;
    }
    Ok(file)
}

fn ends_mid_line(file: &mut File) -> io::Result<bool> {
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok(false);
    }
    let mut last = [0_u8];
    file.seek(SeekFrom::Start(length - 1))?;
    file.read_exact(&mut last)?;
    Ok(last != *b"\n")
}

/// Syncs the directory holding `path`, which is what makes a newly created entry survive a power cut.
fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

/// The file one session's records live in.
pub fn file_name(session: SessionDate) -> String {
    format!("session-{}.jsonl", session.date())
}

/// The commit `build.rs` stamped, or `None` when the build could not ask git.
fn built_commit() -> Option<Commit> {
    option_env!("FUND_COMMIT")
        .map(|raw| Commit::new(raw).expect("build.rs stamps a 40-character sha"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::journal::{ConfigurationResolved, ReadLine, UnreadableCause, read};

    fn observation() -> Observation {
        Observation::ConfigurationResolved(ConfigurationResolved::new(BTreeMap::new()))
    }

    #[test]
    fn test_records_land_in_their_session_file_in_sequence() {
        let directory = std::env::temp_dir().join(format!("fund-journal-{}", Uuid::new_v4()));
        let mut journal = Journal::open(&directory).unwrap();
        for timestamp in [
            "2026-07-31T14:30:00Z",
            // 23:00 Eastern, still July 31.
            "2026-08-01T03:00:00Z",
            "2026-08-03T14:30:00Z",
        ] {
            journal
                .append(timestamp.parse().unwrap(), observation())
                .unwrap();
        }
        let mut files: Vec<String> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        files.sort();
        assert_eq!(
            files,
            ["session-2026-07-31.jsonl", "session-2026-08-03.jsonl"]
        );
        let sequences = |file: &str| -> Vec<(u64, RunId, Option<Commit>)> {
            read(&std::fs::read_to_string(directory.join(file)).unwrap())
                .into_iter()
                .map(|line| match line {
                    ReadLine::Read(record) => {
                        (record.sequence(), record.run_id(), record.commit().cloned())
                    }
                    ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
                })
                .collect()
        };
        let run = journal.run_id();
        assert_eq!(
            sequences("session-2026-07-31.jsonl"),
            [(1, run, built_commit()), (2, run, built_commit())]
        );
        assert_eq!(
            sequences("session-2026-08-03.jsonl"),
            [(3, run, built_commit())]
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    fn temporary_directory() -> PathBuf {
        std::env::temp_dir().join(format!("fund-journal-{}", Uuid::new_v4()))
    }

    fn lines(directory: &std::path::Path) -> Vec<ReadLine> {
        read(&std::fs::read_to_string(directory.join("session-2026-07-31.jsonl")).unwrap())
    }

    #[test]
    fn test_a_torn_line_stays_its_own_line() {
        let directory = temporary_directory();
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("session-2026-07-31.jsonl"),
            r#"{"schema_version":1,"run"#,
        )
        .unwrap();
        let mut journal = Journal::open(&directory).unwrap();
        journal
            .append("2026-07-31T14:30:00Z".parse().unwrap(), observation())
            .unwrap();
        let lines = lines(&directory);
        assert!(
            matches!(
                lines.as_slice(),
                [
                    ReadLine::Unreadable {
                        line: 1,
                        cause: UnreadableCause::NotJson { .. },
                        ..
                    },
                    ReadLine::Read(record)
                ] if record.sequence() == 1
            ),
            "{lines:?}"
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_a_failed_append_consumes_its_sequence() {
        let directory = temporary_directory();
        let mut journal = Journal::open(&directory).unwrap();
        let blocker = directory.join("session-2026-07-31.jsonl");
        std::fs::create_dir(&blocker).unwrap();
        assert!(
            journal
                .append("2026-07-31T14:30:00Z".parse().unwrap(), observation())
                .is_err()
        );
        std::fs::remove_dir(&blocker).unwrap();
        journal
            .append("2026-07-31T14:31:00Z".parse().unwrap(), observation())
            .unwrap();
        let sequences: Vec<u64> = lines(&directory)
            .iter()
            .map(|line| match line {
                ReadLine::Read(record) => record.sequence(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect();
        assert_eq!(sequences, [2]);
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
