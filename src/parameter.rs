//! Parameters read from the process environment, by the variable each `Parameter` names.

use std::collections::BTreeMap;
use std::env::VarError;
use std::path::PathBuf;

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
    let supplied = environment_variable(Parameter::LogDirectory)?;
    record(
        (Parameter::LogDirectory, supplied),
        DEFAULT_LOG_DIRECTORY.to_string(),
        &mut BTreeMap::new(),
    )
    .map(PathBuf::from)
}
