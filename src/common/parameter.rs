//! Every parameter a binary reads, named by one enum so a journaled name cannot drift from the parameter it names.

use std::collections::BTreeMap;
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
/// A variant is never removed while a journal names it, since the record that names it would stop reading; a retired
/// parameter keeps its variant.
pub enum Parameter {
    /// Trading days before today the archive heal looks back over.
    LookbackSessions,
    /// Minutes the archive heal may start new work within.
    BudgetMinutes,
    JournalDirectory,
    LogDirectory,
    /// Symbols per one-minute bars request.
    MinuteBatchSymbols,
    /// One-minute bars requests in flight at once.
    MinuteConcurrency,
    /// Symbols whose quotes or trades are paged at once.
    TickConcurrency,
    /// The symbols the trader trades, comma-separated.
    Universe,
    /// How often the trader decides: `one_minute` or `five_minute`.
    DecisionInterval,
    /// Whole shares the noise strategy holds of each symbol it draws.
    NoiseShares,
    NoiseSeed,
    /// Dollars the trader may hold across every name at once.
    GrossLimit,
    /// Dollars the trader may hold in any one name.
    PerNameLimit,
    /// Dollars the trader may lose in a session before it goes flat.
    DailyLossLimit,
    /// Minutes before the close from which the trader holds nothing.
    FlatBeforeCloseMinutes,
    /// Seconds a price may age before the trader treats its symbol as unpriced.
    StaleAfterSeconds,
    /// Milliseconds between reads of an open order.
    OrderPollMilliseconds,
    /// Seconds an order may stay open before it is canceled.
    OrderOpenSeconds,
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
    /// Not supplied, and the parameter has no default.
    Missing { parameter: Parameter },
    Unparsable {
        parameter: Parameter,
        raw: String,
        reason: String,
    },
    /// Parsed, but past the most the parameter may be.
    OutOfRange {
        parameter: Parameter,
        value: String,
        most: String,
    },
}

impl Display for ParameterRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing { parameter } => {
                write!(
                    formatter,
                    "{} is not set and has no default",
                    parameter.variable()
                )
            }
            Self::Unparsable {
                parameter,
                raw,
                reason,
            } => write!(
                formatter,
                "{} is `{raw}`, which is not a valid {parameter}: {reason}",
                parameter.variable()
            ),
            Self::OutOfRange {
                parameter,
                value,
                most,
            } => write!(
                formatter,
                "{} is {value}, past its most of {most}",
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
        Some(raw) => (parse(parameter, raw)?, ParameterSource::Environment),
    };
    let resolved = ResolvedParameter::new(value.to_string(), source);
    Ok((value, resolved))
}

fn parse<Value>(parameter: Parameter, raw: &str) -> Result<Value, ParameterRefusal>
where
    Value: FromStr,
    Value::Err: Display,
{
    raw.parse()
        .map_err(|error: Value::Err| ParameterRefusal::Unparsable {
            parameter,
            raw: raw.to_string(),
            reason: error.to_string(),
        })
}

/// `value` when it is no more than `most`.
pub fn at_most<Value: PartialOrd + Display>(
    parameter: Parameter,
    value: Value,
    most: Value,
) -> Result<Value, ParameterRefusal> {
    if value <= most {
        Ok(value)
    } else {
        Err(ParameterRefusal::OutOfRange {
            parameter,
            value: value.to_string(),
            most: most.to_string(),
        })
    }
}

/// Parses one parameter's supplied value, or `default` when none was supplied, keeping what the journal records for it.
pub fn record<Value>(
    (parameter, supplied): (Parameter, Option<String>),
    default: Value,
    resolved: &mut BTreeMap<Parameter, ResolvedParameter>,
) -> Result<Value, ParameterRefusal>
where
    Value: FromStr + Display,
    Value::Err: Display,
{
    let (value, parameter_resolved) = resolve(parameter, supplied.as_deref(), default)?;
    resolved.insert(parameter, parameter_resolved);
    Ok(value)
}

/// `record` for a parameter with no default, refused when it was not supplied.
pub fn record_required<Value>(
    (parameter, supplied): (Parameter, Option<String>),
    resolved: &mut BTreeMap<Parameter, ResolvedParameter>,
) -> Result<Value, ParameterRefusal>
where
    Value: FromStr + Display,
    Value::Err: Display,
{
    let Some(raw) = supplied else {
        return Err(ParameterRefusal::Missing { parameter });
    };
    let value: Value = parse(parameter, &raw)?;
    resolved.insert(
        parameter,
        ResolvedParameter::new(value.to_string(), ParameterSource::Environment),
    );
    Ok(value)
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
                "FUND_LOG_DIRECTORY",
                "FUND_MINUTE_BATCH_SYMBOLS",
                "FUND_MINUTE_CONCURRENCY",
                "FUND_TICK_CONCURRENCY",
                "FUND_UNIVERSE",
                "FUND_DECISION_INTERVAL",
                "FUND_NOISE_SHARES",
                "FUND_NOISE_SEED",
                "FUND_GROSS_LIMIT",
                "FUND_PER_NAME_LIMIT",
                "FUND_DAILY_LOSS_LIMIT",
                "FUND_FLAT_BEFORE_CLOSE_MINUTES",
                "FUND_STALE_AFTER_SECONDS",
                "FUND_ORDER_POLL_MILLISECONDS",
                "FUND_ORDER_OPEN_SECONDS",
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
    fn test_a_value_past_its_most_is_refused_with_both() {
        assert_eq!(at_most(Parameter::BudgetMinutes, 1_440, 1_440), Ok(1_440));
        assert_eq!(
            at_most(Parameter::BudgetMinutes, 1_441, 1_440),
            Err(ParameterRefusal::OutOfRange {
                parameter: Parameter::BudgetMinutes,
                value: "1441".to_string(),
                most: "1440".to_string(),
            })
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

    #[test]
    fn test_a_required_value_is_refused_when_absent_and_journaled_as_parsed() {
        let mut resolved = BTreeMap::new();
        assert_eq!(
            record_required::<u64>((Parameter::NoiseSeed, None), &mut resolved),
            Err(ParameterRefusal::Missing {
                parameter: Parameter::NoiseSeed
            })
        );
        assert!(resolved.is_empty());
        assert_eq!(
            ParameterRefusal::Missing {
                parameter: Parameter::NoiseSeed
            }
            .to_string(),
            "FUND_NOISE_SEED is not set and has no default"
        );
        assert_eq!(
            record_required::<u64>(
                (Parameter::NoiseSeed, Some("07".to_string())),
                &mut resolved
            ),
            Ok(7)
        );
        assert_eq!(
            resolved,
            BTreeMap::from([(
                Parameter::NoiseSeed,
                ResolvedParameter::new("7".to_string(), ParameterSource::Environment)
            )])
        );
        assert!(matches!(
            record_required::<u64>((Parameter::NoiseSeed, Some("seven".to_string())), &mut resolved),
            Err(ParameterRefusal::Unparsable { raw, .. }) if raw == "seven"
        ));
    }
}
