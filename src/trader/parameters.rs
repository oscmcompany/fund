//! The trader's settings, each resolved once at startup. The universe, the playbook and the limits have no default, so a
//! run trades only what was deliberately configured.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::num::{NonZeroU16, ParseIntError};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use chrono::TimeDelta;

use crate::common::book::Cash;
use crate::common::journal::ConfigurationResolved;
use crate::common::market::{Dollars, Symbol, SymbolRefusal};
use crate::common::parameter::{
    Parameter, ParameterRefusal, record, record_required, refuse_retired,
};
use crate::common::risk::{Limits, LimitsRefusal};
use crate::execution::{Patience, PatienceRefusal};
use crate::parameter::{Directories, environment_variable};
use crate::trader::{DecisionInterval, SessionSettings, SettingsRefusal};

const DEFAULT_DECISION_INTERVAL: DecisionInterval = DecisionInterval::FiveMinute;
const DEFAULT_STALE_AFTER: StaleAfter = StaleAfter(120);
const DEFAULT_ORDER_POLL: OrderPoll = OrderPoll(NonZeroU16::new(500).expect("500 is not zero"));
const DEFAULT_ORDER_OPEN: OrderOpen = OrderOpen(NonZeroU16::new(30).expect("30 is not zero"));

/// The symbols a run trades: at least one, written comma-separated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Universe(BTreeSet<Symbol>);

impl Universe {
    pub fn symbols(&self) -> &BTreeSet<Symbol> {
        &self.0
    }
}

/// Why a universe was refused: it names no symbol, or one that is not a ticker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniverseRefusal {
    Empty,
    Symbol(SymbolRefusal),
}

impl Display for UniverseRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(formatter, "the universe names no symbol"),
            Self::Symbol(refusal) => write!(formatter, "{refusal}"),
        }
    }
}

impl FromStr for Universe {
    type Err = UniverseRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.trim().is_empty() {
            return Err(UniverseRefusal::Empty);
        }
        let symbols = raw
            .split(',')
            .map(|symbol| Symbol::new(symbol.trim()).map_err(UniverseRefusal::Symbol))
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

/// Why a bounded count was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CountRefusal {
    NotACount(ParseIntError),
    Zero,
    PastMost { most: u16 },
}

impl Display for CountRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotACount(error) => write!(formatter, "not a whole count: {error}"),
            Self::Zero => write!(formatter, "zero is not allowed"),
            Self::PastMost { most } => write!(formatter, "past its most of {most}"),
        }
    }
}

fn count(raw: &str, most: u16) -> Result<u16, CountRefusal> {
    let value: u64 = raw.parse().map_err(CountRefusal::NotACount)?;
    u16::try_from(value)
        .ok()
        .filter(|value| *value <= most)
        .ok_or(CountRefusal::PastMost { most })
}

fn positive_count(raw: &str, most: u16) -> Result<NonZeroU16, CountRefusal> {
    NonZeroU16::new(count(raw, most)?).ok_or(CountRefusal::Zero)
}

/// Minutes before the close from which the trader holds nothing, at most a whole regular session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlatBeforeClose(u16);

impl FromStr for FlatBeforeClose {
    type Err = CountRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        count(raw, 390).map(Self)
    }
}

impl From<FlatBeforeClose> for TimeDelta {
    fn from(minutes: FlatBeforeClose) -> Self {
        TimeDelta::minutes(i64::from(minutes.0))
    }
}

/// Seconds a price may age before the trader treats its symbol as unpriced, at most an hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StaleAfter(u16);

impl FromStr for StaleAfter {
    type Err = CountRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        count(raw, 3_600).map(Self)
    }
}

impl From<StaleAfter> for TimeDelta {
    fn from(seconds: StaleAfter) -> Self {
        TimeDelta::seconds(i64::from(seconds.0))
    }
}

/// Milliseconds between reads of an open order, positive and at most a minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderPoll(NonZeroU16);

impl FromStr for OrderPoll {
    type Err = CountRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        positive_count(raw, 60_000).map(Self)
    }
}

impl From<OrderPoll> for Duration {
    fn from(milliseconds: OrderPoll) -> Self {
        Duration::from_millis(u64::from(milliseconds.0.get()))
    }
}

/// Seconds an order may stay open before it is canceled, positive and at most an hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderOpen(NonZeroU16);

impl FromStr for OrderOpen {
    type Err = CountRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        positive_count(raw, 3_600).map(Self)
    }
}

impl From<OrderOpen> for Duration {
    fn from(seconds: OrderOpen) -> Self {
        Duration::from_secs(u64::from(seconds.0.get()))
    }
}

macro_rules! display_count {
    ($($type:ty),*) => {$(
        impl Display for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{}", self.0)
            }
        }
    )*};
}

display_count!(FlatBeforeClose, StaleAfter, OrderPoll, OrderOpen);

/// Why the trader's settings were refused: one parameter, or settings that do not hold together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParametersRefusal {
    Parameter(ParameterRefusal),
    Limits(LimitsRefusal),
    Patience(PatienceRefusal),
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
            Self::Limits(refusal) => write!(formatter, "the limits are refused: {refusal}"),
            Self::Patience(refusal) => write!(formatter, "the patience is refused: {refusal}"),
            Self::Settings(refusal) => write!(formatter, "the settings are refused: {refusal}"),
        }
    }
}

/// The trader's settings.
#[derive(Debug, Clone)]
pub struct Parameters {
    universe: Universe,
    playbook: PathBuf,
    settings: SessionSettings,
    directories: Directories,
}

impl Parameters {
    /// Reads each parameter's variable, returning the configuration the journal records for them.
    pub fn from_environment() -> Result<(Self, ConfigurationResolved), ParametersRefusal> {
        Self::resolved(&environment_variable)
    }

    fn resolved(
        supplied: &impl Fn(Parameter) -> Result<Option<String>, ParameterRefusal>,
    ) -> Result<(Self, ConfigurationResolved), ParametersRefusal> {
        refuse_retired(supplied)?;
        let mut resolved = BTreeMap::new();
        let read = |parameter| Ok::<_, ParameterRefusal>((parameter, supplied(parameter)?));
        let universe: Universe = record_required(read(Parameter::Universe)?, &mut resolved)?;
        let playbook = PathBuf::from(record_required::<String>(
            read(Parameter::Playbook)?,
            &mut resolved,
        )?);
        let mut limit =
            |parameter| record_required::<Dollars>(read(parameter)?, &mut resolved).map(Cash::from);
        let (gross, per_name, daily_loss) = (
            limit(Parameter::GrossLimit)?,
            limit(Parameter::PerNameLimit)?,
            limit(Parameter::DailyLossLimit)?,
        );
        let flat_before_close: FlatBeforeClose =
            record_required(read(Parameter::FlatBeforeCloseMinutes)?, &mut resolved)?;
        let limits = Limits::new(gross, per_name, daily_loss, flat_before_close.into())
            .map_err(ParametersRefusal::Limits)?;
        let decision = record(
            read(Parameter::DecisionInterval)?,
            DEFAULT_DECISION_INTERVAL,
            &mut resolved,
        )?;
        let stale_after = record(
            read(Parameter::StaleAfterSeconds)?,
            DEFAULT_STALE_AFTER,
            &mut resolved,
        )?;
        let poll = record(
            read(Parameter::OrderPollMilliseconds)?,
            DEFAULT_ORDER_POLL,
            &mut resolved,
        )?;
        let open_for = record(
            read(Parameter::OrderOpenSeconds)?,
            DEFAULT_ORDER_OPEN,
            &mut resolved,
        )?;
        let patience =
            Patience::new(poll.into(), open_for.into()).map_err(ParametersRefusal::Patience)?;
        let settings = SessionSettings::new(decision, limits, patience, stale_after.into())
            .map_err(ParametersRefusal::Settings)?;
        let parameters = Self {
            universe,
            playbook,
            settings,
            directories: Directories::resolved(supplied, &mut resolved)?,
        };
        Ok((parameters, ConfigurationResolved::new(resolved)))
    }

    pub fn universe(&self) -> &Universe {
        &self.universe
    }

    pub fn playbook(&self) -> &Path {
        &self.playbook
    }

    pub fn settings(&self) -> SessionSettings {
        self.settings
    }

    pub fn journal_directory(&self) -> &Path {
        self.directories.journal()
    }

    pub fn log_directory(&self) -> &Path {
        self.directories.log()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::journal::ParameterSource;

    const REQUIRED: [(Parameter, &str); 6] = [
        (Parameter::Universe, "SPY, QQQ"),
        (Parameter::Playbook, "/etc/fund/playbook.toml"),
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
        assert_eq!(parameters.playbook(), Path::new("/etc/fund/playbook.toml"));
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
                (
                    "playbook",
                    "/etc/fund/playbook.toml",
                    ParameterSource::Environment
                ),
            ]
        );
    }

    #[test]
    fn test_each_directory_is_read_from_its_own_variable() {
        let directories = [
            ("FUND_JOURNAL_DIRECTORY", "/tmp/journal"),
            ("FUND_LOG_DIRECTORY", "/tmp/logs"),
        ];
        let supplied = |parameter: Parameter| {
            let directory = directories
                .iter()
                .find(|(variable, _)| *variable == parameter.variable())
                .map(|(_, raw)| *raw);
            let required = REQUIRED
                .iter()
                .find(|(required, _)| *required == parameter)
                .map(|(_, raw)| *raw);
            Ok::<_, ParameterRefusal>(directory.or(required).map(str::to_string))
        };
        let (parameters, _) = Parameters::resolved(&supplied).unwrap();
        assert_eq!(parameters.journal_directory(), Path::new("/tmp/journal"));
        assert_eq!(parameters.log_directory(), Path::new("/tmp/logs"));
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
    fn test_a_supplied_retired_parameter_refuses_to_start() {
        let mut values = REQUIRED.to_vec();
        values.push((Parameter::NoiseShares, "1"));
        assert_eq!(
            Parameters::resolved(&supplied(&values)).map(|_| ()),
            Err(ParametersRefusal::Parameter(ParameterRefusal::Retired {
                parameter: Parameter::NoiseShares
            }))
        );
    }

    #[test]
    fn test_an_empty_or_invalid_universe_is_refused_with_its_cause() {
        let refused = |raw: &str| Universe::from_str(raw).unwrap_err();
        assert_eq!(refused(""), UniverseRefusal::Empty);
        assert_eq!(refused(" "), UniverseRefusal::Empty);
        assert_eq!(
            refused("SPY,"),
            UniverseRefusal::Symbol(SymbolRefusal::Malformed { raw: String::new() })
        );
        assert_eq!(
            refused("SPY,not a symbol").to_string(),
            "`not a symbol` is not a ticker"
        );
        let mut values = REQUIRED.to_vec();
        values[0] = (Parameter::Universe, "");
        assert_eq!(
            Parameters::resolved(&supplied(&values)).map(|_| ()),
            Err(ParametersRefusal::Parameter(ParameterRefusal::Unparsable {
                parameter: Parameter::Universe,
                raw: String::new(),
                reason: "the universe names no symbol".to_string(),
            }))
        );
    }

    proptest! {
        /// The text the journal records for a universe reads back as the same universe.
        #[test]
        fn property_a_universe_round_trips_through_its_text(
            symbols in prop::collection::btree_set("[A-Z]{1,5}(\\.[A-Z]{1,3})?", 1..6),
        ) {
            let universe: Universe = symbols.iter().cloned().collect::<Vec<_>>().join(",").parse().unwrap();
            prop_assert_eq!(universe.symbols().len(), symbols.len());
            prop_assert_eq!(universe.to_string().parse::<Universe>(), Ok(universe));
        }
    }

    #[test]
    fn test_each_default_count_reads_back_from_its_text() {
        assert_eq!(
            DEFAULT_STALE_AFTER.to_string().parse(),
            Ok(DEFAULT_STALE_AFTER)
        );
        assert_eq!(
            DEFAULT_ORDER_POLL.to_string().parse(),
            Ok(DEFAULT_ORDER_POLL)
        );
        assert_eq!(
            DEFAULT_ORDER_OPEN.to_string().parse(),
            Ok(DEFAULT_ORDER_OPEN)
        );
    }

    #[test]
    fn test_a_count_is_refused_past_its_most_at_zero_or_unread() {
        assert_eq!(
            FlatBeforeClose::from_str("390").map(TimeDelta::from),
            Ok(TimeDelta::minutes(390))
        );
        assert_eq!(
            FlatBeforeClose::from_str("391"),
            Err(CountRefusal::PastMost { most: 390 })
        );
        assert_eq!(
            StaleAfter::from_str("3600").map(TimeDelta::from),
            Ok(TimeDelta::hours(1))
        );
        assert_eq!(
            StaleAfter::from_str("65536"),
            Err(CountRefusal::PastMost { most: 3_600 })
        );
        assert_eq!(
            OrderPoll::from_str("60000").map(Duration::from),
            Ok(Duration::from_secs(60))
        );
        assert_eq!(
            OrderPoll::from_str("60001"),
            Err(CountRefusal::PastMost { most: 60_000 })
        );
        assert_eq!(
            OrderOpen::from_str("3600").map(Duration::from),
            Ok(Duration::from_secs(3_600))
        );
        assert_eq!(OrderOpen::from_str("0"), Err(CountRefusal::Zero));
        assert!(matches!(
            StaleAfter::from_str("-1"),
            Err(CountRefusal::NotACount(_))
        ));
    }

    #[test]
    fn test_a_poll_longer_than_the_open_window_is_refused_by_the_patience() {
        let mut values = REQUIRED.to_vec();
        values.push((Parameter::OrderPollMilliseconds, "2000"));
        values.push((Parameter::OrderOpenSeconds, "1"));
        assert_eq!(
            Parameters::resolved(&supplied(&values)).map(|_| ()),
            Err(ParametersRefusal::Patience(PatienceRefusal::PollPastOpen {
                poll: Duration::from_secs(2),
                open_for: Duration::from_secs(1),
            }))
        );
    }

    #[test]
    fn test_a_zero_limit_is_refused_by_the_limits() {
        let mut values = REQUIRED.to_vec();
        values[4] = (Parameter::DailyLossLimit, "0");
        assert!(matches!(
            Parameters::resolved(&supplied(&values)),
            Err(ParametersRefusal::Limits(LimitsRefusal::NotPositive { .. }))
        ));
    }

    #[test]
    fn test_a_flat_window_past_a_session_is_refused() {
        let mut values = REQUIRED.to_vec();
        values[5] = (Parameter::FlatBeforeCloseMinutes, "391");
        assert_eq!(
            Parameters::resolved(&supplied(&values)).map(|_| ()),
            Err(ParametersRefusal::Parameter(ParameterRefusal::Unparsable {
                parameter: Parameter::FlatBeforeCloseMinutes,
                raw: "391".to_string(),
                reason: "past its most of 390".to_string(),
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
