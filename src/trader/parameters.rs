//! The trader's settings, each resolved once at startup. The universe, the strategy's settings and the limits have no
//! default, so a run trades only what was deliberately configured.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use chrono::TimeDelta;

use crate::common::book::Cash;
use crate::common::journal::ConfigurationResolved;
use crate::common::market::{Dollars, SHARE_SCALE, Shares, Symbol};
use crate::common::parameter::{Parameter, ParameterRefusal, at_most, record, record_required};
use crate::common::risk::{Limits, LimitsRefusal};
use crate::common::strategy::noise::Noise;
use crate::execution::Patience;
use crate::parameter::{DEFAULT_JOURNAL_DIRECTORY, DEFAULT_LOG_DIRECTORY, environment_variable};
use crate::trader::{DecisionInterval, SessionSettings, SettingsRefusal};

const DEFAULT_DECISION_INTERVAL: DecisionInterval = DecisionInterval::FiveMinute;
const DEFAULT_STALE_AFTER_SECONDS: u64 = 120;
const DEFAULT_ORDER_POLL_MILLISECONDS: NonZeroU64 = NonZeroU64::new(500).expect("500 is not zero");
const DEFAULT_ORDER_OPEN_SECONDS: NonZeroU64 = NonZeroU64::new(30).expect("30 is not zero");
/// A whole regular session.
const MAXIMUM_FLAT_BEFORE_CLOSE_MINUTES: u64 = 390;
const MAXIMUM_STALE_AFTER_SECONDS: u64 = 3_600;
const MAXIMUM_ORDER_POLL_MILLISECONDS: NonZeroU64 =
    NonZeroU64::new(60_000).expect("60,000 is not zero");
const MAXIMUM_ORDER_OPEN_SECONDS: NonZeroU64 = NonZeroU64::new(3_600).expect("3,600 is not zero");
/// The most whole shares a `Shares` holds.
const MAXIMUM_NOISE_SHARES: NonZeroU64 =
    NonZeroU64::new(u64::MAX / SHARE_SCALE).expect("u64::MAX / SHARE_SCALE is not zero");

/// The symbols a run trades: at least one, written comma-separated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Universe(BTreeSet<Symbol>);

impl Universe {
    pub fn symbols(&self) -> &BTreeSet<Symbol> {
        &self.0
    }
}

impl FromStr for Universe {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let symbols = raw
            .split(',')
            .map(|symbol| Symbol::new(symbol.trim()).map_err(|refusal| refusal.to_string()))
            .collect::<Result<BTreeSet<_>, _>>()?;
        Ok(Self(symbols))
    }
}

impl Display for Universe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let symbols: Vec<&str> = self.0.iter().map(Symbol::as_str).collect();
        formatter.write_str(&symbols.join(","))
    }
}

/// Why the trader's settings were refused: one parameter, or limits that do not hold together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParametersRefusal {
    Parameter(ParameterRefusal),
    Limits(LimitsRefusal),
    Settings(SettingsRefusal),
}

impl From<ParameterRefusal> for ParametersRefusal {
    fn from(refusal: ParameterRefusal) -> Self {
        Self::Parameter(refusal)
    }
}

impl Display for ParametersRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parameter(refusal) => write!(formatter, "{refusal}"),
            Self::Limits(refusal) => write!(formatter, "the limits are refused: {refusal:?}"),
            Self::Settings(refusal) => write!(formatter, "the settings are refused: {refusal:?}"),
        }
    }
}

/// The trader's settings.
#[derive(Debug, Clone)]
pub struct Parameters {
    universe: Universe,
    strategy: Noise,
    settings: SessionSettings,
    journal_directory: PathBuf,
    log_directory: PathBuf,
}

impl Parameters {
    /// Reads each parameter's variable, returning the configuration the journal records for them.
    pub fn from_environment() -> Result<(Self, ConfigurationResolved), ParametersRefusal> {
        Self::resolved(&environment_variable)
    }

    fn resolved(
        supplied: &impl Fn(Parameter) -> Result<Option<String>, ParameterRefusal>,
    ) -> Result<(Self, ConfigurationResolved), ParametersRefusal> {
        let mut resolved = BTreeMap::new();
        let read = |parameter| Ok::<_, ParameterRefusal>((parameter, supplied(parameter)?));
        let universe: Universe = record_required(read(Parameter::Universe)?, &mut resolved)?;
        let shares = at_most(
            Parameter::NoiseShares,
            record_required::<NonZeroU64>(read(Parameter::NoiseShares)?, &mut resolved)?,
            MAXIMUM_NOISE_SHARES,
        )?;
        let seed: u64 = record_required(read(Parameter::NoiseSeed)?, &mut resolved)?;
        let mut limit =
            |parameter| record_required::<Dollars>(read(parameter)?, &mut resolved).map(Cash::from);
        let (gross, per_name, daily_loss) = (
            limit(Parameter::GrossLimit)?,
            limit(Parameter::PerNameLimit)?,
            limit(Parameter::DailyLossLimit)?,
        );
        let flat_before_close = at_most(
            Parameter::FlatBeforeCloseMinutes,
            record_required(read(Parameter::FlatBeforeCloseMinutes)?, &mut resolved)?,
            MAXIMUM_FLAT_BEFORE_CLOSE_MINUTES,
        )?;
        let limits = Limits::new(gross, per_name, daily_loss, minutes(flat_before_close))
            .map_err(ParametersRefusal::Limits)?;
        let decision = record(
            read(Parameter::DecisionInterval)?,
            DEFAULT_DECISION_INTERVAL,
            &mut resolved,
        )?;
        let stale_after = at_most(
            Parameter::StaleAfterSeconds,
            record(
                read(Parameter::StaleAfterSeconds)?,
                DEFAULT_STALE_AFTER_SECONDS,
                &mut resolved,
            )?,
            MAXIMUM_STALE_AFTER_SECONDS,
        )?;
        let poll = at_most(
            Parameter::OrderPollMilliseconds,
            record(
                read(Parameter::OrderPollMilliseconds)?,
                DEFAULT_ORDER_POLL_MILLISECONDS,
                &mut resolved,
            )?,
            MAXIMUM_ORDER_POLL_MILLISECONDS,
        )?;
        let open_for = at_most(
            Parameter::OrderOpenSeconds,
            record(
                read(Parameter::OrderOpenSeconds)?,
                DEFAULT_ORDER_OPEN_SECONDS,
                &mut resolved,
            )?,
            MAXIMUM_ORDER_OPEN_SECONDS,
        )?;
        let settings = SessionSettings::new(
            decision,
            limits,
            Patience {
                poll: Duration::from_millis(poll.get()),
                open_for: Duration::from_secs(open_for.get()),
            },
            TimeDelta::seconds(i64::try_from(stale_after).expect("at most an hour of seconds")),
        )
        .map_err(ParametersRefusal::Settings)?;
        let parameters = Self {
            strategy: Noise::new(
                universe.symbols().clone(),
                Shares::whole(shares.get()).expect("at most the whole shares a `Shares` holds"),
                seed,
            ),
            universe,
            settings,
            journal_directory: PathBuf::from(record(
                read(Parameter::JournalDirectory)?,
                DEFAULT_JOURNAL_DIRECTORY.to_string(),
                &mut resolved,
            )?),
            log_directory: PathBuf::from(record(
                read(Parameter::LogDirectory)?,
                DEFAULT_LOG_DIRECTORY.to_string(),
                &mut resolved,
            )?),
        };
        Ok((parameters, ConfigurationResolved::new(resolved)))
    }

    pub fn universe(&self) -> &Universe {
        &self.universe
    }

    pub fn strategy(&self) -> &Noise {
        &self.strategy
    }

    pub fn settings(&self) -> SessionSettings {
        self.settings
    }

    pub fn journal_directory(&self) -> &PathBuf {
        &self.journal_directory
    }

    pub fn log_directory(&self) -> &PathBuf {
        &self.log_directory
    }
}

fn minutes(count: u64) -> TimeDelta {
    TimeDelta::minutes(i64::try_from(count).expect("at most a session of minutes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::journal::ParameterSource;

    const REQUIRED: [(Parameter, &str); 7] = [
        (Parameter::Universe, "SPY, QQQ"),
        (Parameter::NoiseShares, "2"),
        (Parameter::NoiseSeed, "11"),
        (Parameter::GrossLimit, "1000"),
        (Parameter::PerNameLimit, "500.5"),
        (Parameter::DailyLossLimit, "50"),
        (Parameter::FlatBeforeCloseMinutes, "15"),
    ];

    fn supplied(
        values: &[(Parameter, &str)],
    ) -> impl Fn(Parameter) -> Result<Option<String>, ParameterRefusal> {
        let values: BTreeMap<Parameter, String> = values
            .iter()
            .map(|(parameter, raw)| (*parameter, (*raw).to_string()))
            .collect();
        move |parameter| Ok(values.get(&parameter).cloned())
    }

    #[test]
    fn test_the_required_parameters_resolve_and_the_rest_default() {
        let (parameters, configuration) = Parameters::resolved(&supplied(&REQUIRED)).unwrap();
        assert_eq!(parameters.universe().to_string(), "QQQ,SPY");
        assert_eq!(
            parameters.strategy(),
            &Noise::new(
                ["QQQ", "SPY"]
                    .into_iter()
                    .map(|raw| Symbol::new(raw).unwrap())
                    .collect(),
                Shares::whole(2).unwrap(),
                11
            )
        );
        let journaled: Vec<(&str, &str, ParameterSource)> = configuration
            .parameters()
            .iter()
            .map(|(parameter, resolved)| (parameter.into(), resolved.value(), resolved.source()))
            .collect();
        assert_eq!(
            journaled,
            [
                (
                    "journal_directory",
                    "/var/journal/fund",
                    ParameterSource::Default
                ),
                ("log_directory", "/var/log/fund", ParameterSource::Default),
                ("universe", "QQQ,SPY", ParameterSource::Environment),
                ("decision_interval", "five_minute", ParameterSource::Default),
                ("noise_shares", "2", ParameterSource::Environment),
                ("noise_seed", "11", ParameterSource::Environment),
                ("gross_limit", "1000.00", ParameterSource::Environment),
                ("per_name_limit", "500.50", ParameterSource::Environment),
                ("daily_loss_limit", "50.00", ParameterSource::Environment),
                (
                    "flat_before_close_minutes",
                    "15",
                    ParameterSource::Environment
                ),
                ("stale_after_seconds", "120", ParameterSource::Default),
                ("order_poll_milliseconds", "500", ParameterSource::Default),
                ("order_open_seconds", "30", ParameterSource::Default),
            ]
        );
    }

    #[test]
    fn test_each_required_parameter_is_refused_when_absent() {
        for (index, (missing, _)) in REQUIRED.iter().enumerate() {
            let mut values = REQUIRED.to_vec();
            values.remove(index);
            assert_eq!(
                Parameters::resolved(&supplied(&values)).map(|_| ()),
                Err(ParametersRefusal::Parameter(ParameterRefusal::Missing {
                    parameter: *missing
                })),
                "{missing}"
            );
        }
    }

    #[test]
    fn test_an_empty_or_invalid_universe_is_refused() {
        for raw in ["", "SPY,", "SPY,not a symbol"] {
            let mut values = REQUIRED.to_vec();
            values[0] = (Parameter::Universe, raw);
            assert!(
                matches!(
                    Parameters::resolved(&supplied(&values)),
                    Err(ParametersRefusal::Parameter(ParameterRefusal::Unparsable {
                        parameter: Parameter::Universe,
                        ..
                    }))
                ),
                "{raw}"
            );
        }
    }

    #[test]
    fn test_a_zero_limit_is_refused_by_the_limits() {
        let mut values = REQUIRED.to_vec();
        values[5] = (Parameter::DailyLossLimit, "0");
        assert!(matches!(
            Parameters::resolved(&supplied(&values)),
            Err(ParametersRefusal::Limits(LimitsRefusal::NotPositive { .. }))
        ));
    }

    #[test]
    fn test_a_flat_window_past_a_session_is_refused() {
        let mut values = REQUIRED.to_vec();
        values[6] = (Parameter::FlatBeforeCloseMinutes, "391");
        assert_eq!(
            Parameters::resolved(&supplied(&values)).map(|_| ()),
            Err(ParametersRefusal::Parameter(ParameterRefusal::OutOfRange {
                parameter: Parameter::FlatBeforeCloseMinutes,
                value: "391".to_string(),
                most: "390".to_string(),
            }))
        );
    }

    #[test]
    fn test_a_staleness_under_a_minute_is_refused_by_the_settings() {
        let mut values = REQUIRED.to_vec();
        values.push((Parameter::StaleAfterSeconds, "59"));
        assert_eq!(
            Parameters::resolved(&supplied(&values)).map(|_| ()),
            Err(ParametersRefusal::Settings(
                SettingsRefusal::StalenessUnderAMinute {
                    stale_after: TimeDelta::seconds(59)
                }
            ))
        );
    }
}
