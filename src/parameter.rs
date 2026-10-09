//! Parameters read from the process environment, by the variable each `Parameter` names.

use std::collections::BTreeMap;
use std::env::VarError;
use std::path::{Path, PathBuf};

use crate::common::journal::{ConfigurationResolved, ResolvedParameter};
use crate::common::parameter::{Parameter, ParameterRefusal, record};

pub const DEFAULT_JOURNAL_DIRECTORY: &str = "/var/journal/fund";
pub const DEFAULT_LOG_DIRECTORY: &str = "/var/log/fund";

/// The parameter as its variable supplies it, `None` when the variable is unset.
pub fn environment_variable(parameter: Parameter) -> Result<Option<String>, ParameterRefusal> {
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

/// Only the log directory, resolved on its own so a refusal of any other parameter still reaches the log file.
pub fn log_directory_from_environment() -> Result<PathBuf, ParameterRefusal> {
    directory(
        (Parameter::LogDirectory, DEFAULT_LOG_DIRECTORY),
        &environment_variable,
        &mut BTreeMap::new(),
    )
}

/// Where a run's journal and log files go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Directories {
    journal: PathBuf,
    log: PathBuf,
}

impl Directories {
    /// Both directories from their variables, with the configuration the journal records for them.
    pub fn from_environment() -> Result<(Self, ConfigurationResolved), ParameterRefusal> {
        let mut resolved = BTreeMap::new();
        let directories = Self::resolved(&environment_variable, &mut resolved)?;
        Ok((directories, ConfigurationResolved::new(resolved)))
    }

    /// Each directory from what `supplied` returns for it, or its default when that is nothing, recorded in `resolved`.
    pub fn resolved(
        supplied: &impl Fn(Parameter) -> Result<Option<String>, ParameterRefusal>,
        resolved: &mut BTreeMap<Parameter, ResolvedParameter>,
    ) -> Result<Self, ParameterRefusal> {
        Ok(Self {
            journal: directory(
                (Parameter::JournalDirectory, DEFAULT_JOURNAL_DIRECTORY),
                supplied,
                resolved,
            )?,
            log: directory(
                (Parameter::LogDirectory, DEFAULT_LOG_DIRECTORY),
                supplied,
                resolved,
            )?,
        })
    }

    pub fn journal(&self) -> &Path {
        &self.journal
    }

    pub fn log(&self) -> &Path {
        &self.log
    }
}

/// The directory `parameter` names, or `default` when it is not supplied.
fn directory(
    (parameter, default): (Parameter, &str),
    supplied: &impl Fn(Parameter) -> Result<Option<String>, ParameterRefusal>,
    resolved: &mut BTreeMap<Parameter, ResolvedParameter>,
) -> Result<PathBuf, ParameterRefusal> {
    record(
        (parameter, supplied(parameter)?),
        default.to_string(),
        resolved,
    )
    .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_each_directory_is_read_from_its_own_parameter() {
        let values = BTreeMap::from([(Parameter::LogDirectory, "/tmp/logs".to_string())]);
        let supplied = |parameter| Ok(values.get(&parameter).cloned());
        let mut resolved = BTreeMap::new();
        let directories = Directories::resolved(&supplied, &mut resolved).unwrap();
        assert_eq!(directories.journal(), Path::new("/var/journal/fund"));
        assert_eq!(directories.log(), Path::new("/tmp/logs"));
        let recorded: Vec<(Parameter, &str)> = resolved
            .iter()
            .map(|(parameter, resolved)| (*parameter, resolved.value()))
            .collect();
        assert_eq!(
            recorded,
            [
                (Parameter::JournalDirectory, "/var/journal/fund"),
                (Parameter::LogDirectory, "/tmp/logs"),
            ]
        );
    }
}
