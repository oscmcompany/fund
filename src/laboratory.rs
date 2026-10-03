//! Loads what studies read and catalogues what they do: a `Study` journals every dataset its loaders read and every
//! experiment it reports, then ships the journal to the records bucket.

pub mod dataset;

use std::io;
use std::path::PathBuf;
use std::time::Instant;

use chrono::Utc;
use uuid::Uuid;

use crate::archive::Archive;
use crate::common::journal::{Observation, RunId};
use crate::common::laboratory::dataset::Fingerprint;
use crate::common::laboratory::experiment::{
    DatasetRead, Elapsed, ExperimentRan, Label, Machine, Outputs, Parameters,
};
use crate::common::storage::{Host, Key, Service};
use crate::common::time::SessionDate;
use crate::journal::Journal;
use crate::laboratory::dataset::Dataset;
use crate::records;

/// One run of study code: its journal, what it is about, the machine it runs on, and when it opened.
pub struct Study {
    journal: Journal,
    label: Label,
    machine: Machine,
    opened: Instant,
}

impl Study {
    /// Opens a run journaling into `directory`, named for this machine's hostname.
    pub fn open(label: Label, directory: impl Into<PathBuf>) -> io::Result<Self> {
        let output = std::process::Command::new("hostname").output()?;
        let machine = Machine::new(String::from_utf8_lossy(&output.stdout))
            .map_err(|refusal| io::Error::new(io::ErrorKind::InvalidData, refusal.to_string()))?;
        Ok(Self {
            journal: Journal::open(directory, RunId::new(Uuid::new_v4()))?,
            label,
            machine,
            opened: Instant::now(),
        })
    }

    pub fn run_id(&self) -> RunId {
        self.journal.run_id()
    }

    /// Journals a read; only loaders call it, so every dataset a study holds was catalogued.
    pub(crate) fn read(&mut self, fingerprint: &Fingerprint) -> io::Result<()> {
        let read = DatasetRead::new(
            self.label.clone(),
            self.machine.clone(),
            fingerprint.clone(),
        );
        self.journal
            .append(Utc::now(), Observation::DatasetRead(Box::new(read)))
    }

    /// Journals one experiment over `datasets`, whose fingerprints are taken from the datasets themselves so the
    /// record names exactly what was read.
    pub fn experiment(
        &mut self,
        parameters: Parameters,
        datasets: &[&Dataset],
        outputs: Outputs,
    ) -> io::Result<()> {
        let ran = ExperimentRan::new(
            self.label.clone(),
            self.machine.clone(),
            parameters,
            datasets
                .iter()
                .map(|dataset| dataset.fingerprint().clone())
                .collect(),
            outputs,
            Elapsed::from_milliseconds(
                u64::try_from(self.opened.elapsed().as_millis()).unwrap_or(u64::MAX),
            ),
        );
        self.journal
            .append(Utc::now(), Observation::ExperimentRan(Box::new(ran)))
    }

    /// Ships the journal directory's recent files to `records` as the researcher's, returning each key's outcome.
    pub async fn finish(self, records: &Archive) -> Vec<(Key, Result<(), String>)> {
        let directory = self.journal.directory().to_path_buf();
        records::ship(
            records,
            Host::Researcher,
            &Service::new("laboratory").expect("a fixed service name is valid"),
            &directory,
            &directory,
            SessionDate::at(Utc::now()),
        )
        .await
    }
}
