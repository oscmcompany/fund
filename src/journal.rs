//! Appends journal records to one JSONL file per session. A record is on disk when `append` returns: the line is
//! written and `sync_data` has run, and a newly created session file's directory entry is synced as well.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::common::journal::{Commit, Observation, Record, RunId};
use crate::common::time::SessionDate;

/// Writes one run's records.
pub struct Journal {
    directory: PathBuf,
    run_id: RunId,
    commit: Option<Commit>,
    next_sequence: u64,
    open: Option<(SessionDate, File)>,
}

impl Journal {
    /// Starts a run writing into `directory`, creating it if needed.
    pub fn open(directory: impl Into<PathBuf>) -> io::Result<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory)?;
        Ok(Self {
            directory,
            run_id: RunId::new(Uuid::new_v4()),
            commit: built_commit(),
            next_sequence: 1,
            open: None,
        })
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns once the record is durable, so a caller that waits before acting knows the observation survives the
    /// crash the action might cause. A failed append leaves the sequence where it was.
    pub fn append(&mut self, timestamp: DateTime<Utc>, observation: Observation) -> io::Result<()> {
        let record = Record::new(
            self.run_id,
            self.next_sequence,
            timestamp,
            self.commit.clone(),
            observation,
        );
        let session = record.session();
        if self
            .open
            .as_ref()
            .is_none_or(|(open_session, _)| *open_session != session)
        {
            let path = self.directory.join(file_name(session));
            let created = !path.exists();
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            if created {
                File::open(&self.directory)?.sync_all()?;
            }
            self.open = Some((session, file));
        }
        let (_, file) = self.open.as_mut().expect("a session file was opened above");
        let mut line = record.encode();
        line.push('\n');
        file.write_all(line.as_bytes())?;
        file.sync_data()?;
        self.next_sequence += 1;
        Ok(())
    }
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
    use crate::common::journal::{ConfigurationResolved, ReadLine, read};

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
                    ReadLine::Unreadable { line, cause } => panic!("line {line}: {cause:?}"),
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
        assert!(built_commit().is_some());
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
