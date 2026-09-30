//! Every parameter a binary reads, named by one enum so a journaled name cannot drift from the parameter it names.

use std::fmt::Display;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::common::journal::{ParameterSource, ResolvedParameter};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Parameter {
    /// Trading days before today the archive heal looks back over.
    LookbackSessions,
    /// Minutes the archive heal may start new work within.
    BudgetMinutes,
    JournalDirectory,
    /// Symbols per one-minute bars request.
    MinuteBatchSymbols,
    /// One-minute bars requests in flight at once.
    MinuteConcurrency,
}

impl Parameter {
    /// The environment variable that sets it: its name in capitals behind `FUND_`.
    pub fn variable(self) -> String {
        format!("FUND_{}", self.to_string().to_ascii_uppercase())
    }
}

/// Why a supplied value was not used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParameterRefusal {
    Unparsable {
        parameter: Parameter,
        raw: String,
        reason: String,
    },
}

impl Display for ParameterRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unparsable {
                parameter,
                raw,
                reason,
            } => write!(
                formatter,
                "{} is `{raw}`, which is not a valid {parameter}: {reason}",
                parameter.variable()
            ),
        }
    }
}

/// The supplied value parsed, or `default` when none was supplied, with the text the journal keeps for it.
pub fn resolve<Value>(
    parameter: Parameter,
    supplied: Option<&str>,
    default: Value,
) -> Result<(Value, ResolvedParameter), ParameterRefusal>
where
    Value: FromStr + Display,
    Value::Err: Display,
{
    let (value, source) = match supplied {
        None => (default, ParameterSource::Default),
        Some(raw) => {
            let value = raw
                .parse()
                .map_err(|error: Value::Err| ParameterRefusal::Unparsable {
                    parameter,
                    raw: raw.to_string(),
                    reason: error.to_string(),
                })?;
            (value, ParameterSource::Environment)
        }
    };
    let resolved = ResolvedParameter::new(value.to_string(), source);
    Ok((value, resolved))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_each_parameter_has_its_variable() {
        let variables: Vec<String> = Parameter::iter().map(Parameter::variable).collect();
        assert_eq!(
            variables,
            [
                "FUND_LOOKBACK_SESSIONS",
                "FUND_BUDGET_MINUTES",
                "FUND_JOURNAL_DIRECTORY",
                "FUND_MINUTE_BATCH_SYMBOLS",
                "FUND_MINUTE_CONCURRENCY",
            ]
        );
    }

    #[test]
    fn test_serde_and_strum_agree_on_every_name() {
        for parameter in Parameter::iter() {
            let json = serde_json::to_string(&parameter).unwrap();
            assert_eq!(json, format!("\"{parameter}\""));
            assert_eq!(serde_json::from_str::<Parameter>(&json).unwrap(), parameter);
            assert_eq!(parameter.to_string().parse::<Parameter>(), Ok(parameter));
        }
    }

    #[test]
    fn test_a_supplied_value_is_parsed_and_an_absent_one_defaults() {
        let five = NonZeroUsize::new(5).unwrap();
        let (value, resolved) = resolve(Parameter::LookbackSessions, Some("10"), five).unwrap();
        assert_eq!(value.get(), 10);
        assert_eq!(
            resolved,
            ResolvedParameter::new("10".to_string(), ParameterSource::Environment)
        );
        let (value, resolved) = resolve(Parameter::LookbackSessions, None, five).unwrap();
        assert_eq!(value.get(), 5);
        assert_eq!(
            resolved,
            ResolvedParameter::new("5".to_string(), ParameterSource::Default)
        );
    }

    #[test]
    fn test_a_value_that_does_not_parse_is_refused_with_itself() {
        let five = NonZeroUsize::new(5).unwrap();
        for raw in ["0", "-1", "", "five"] {
            match resolve(Parameter::MinuteConcurrency, Some(raw), five) {
                Err(ParameterRefusal::Unparsable {
                    parameter: Parameter::MinuteConcurrency,
                    raw: refused,
                    ..
                }) => assert_eq!(refused, raw),
                other => panic!("{raw}: {other:?}"),
            }
        }
    }
}
