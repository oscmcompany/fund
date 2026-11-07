//! A seeded market of quotes and prints with mechanisms planted at stated strengths, so a model change is read on real
//! sessions only after it recovers what was planted here; nothing is stored, since a seed and a commit rebuild it.

pub mod draw;
pub mod generate;

use std::num::NonZeroU32;

use chrono::{NaiveTime, TimeDelta};

use crate::common::laboratory::cost::BasisPoints;
use crate::common::laboratory::experiment::Parameters;
use draw::{LogNormal, Seed};

/// The probability that a name's session carries a mechanism, which plants it at most once per name-session.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Prevalence(f64);

/// A daily standard deviation of log returns, finite and non-negative.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Volatility(f64);

/// A positive span of time, as the interval between a flow's pieces or the half-life of its impact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Span(TimeDelta);

/// The Eastern wall-clock hours a flow may start and finish within.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    from: NaiveTime,
    to: NaiveTime,
}

/// Why a specification value was refused, with the value read.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum SpecificationRefusal {
    #[error("a prevalence of {value} is not a probability")]
    Prevalence { value: f64 },
    #[error("a volatility of {value} is not finite and non-negative")]
    Volatility { value: f64 },
    #[error("a span of {span} is not positive")]
    Span { span: TimeDelta },
    #[error("a window from {from} to {to} is empty")]
    EmptyWindow { from: NaiveTime, to: NaiveTime },
}

impl Prevalence {
    pub fn new(value: f64) -> Result<Self, SpecificationRefusal> {
        match (0.0..=1.0).contains(&value) {
            true => Ok(Self(value)),
            false => Err(SpecificationRefusal::Prevalence { value }),
        }
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

impl Volatility {
    pub fn new(value: f64) -> Result<Self, SpecificationRefusal> {
        match value.is_finite() && value >= 0.0 {
            true => Ok(Self(value + 0.0)),
            false => Err(SpecificationRefusal::Volatility { value }),
        }
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

impl Span {
    pub fn new(span: TimeDelta) -> Result<Self, SpecificationRefusal> {
        match span > TimeDelta::zero() {
            true => Ok(Self(span)),
            false => Err(SpecificationRefusal::Span { span }),
        }
    }

    pub fn get(self) -> TimeDelta {
        self.0
    }

    fn seconds(self) -> f64 {
        self.0.as_seconds_f64()
    }
}

impl Window {
    pub fn new(from: NaiveTime, to: NaiveTime) -> Result<Self, SpecificationRefusal> {
        match from < to {
            true => Ok(Self { from, to }),
            false => Err(SpecificationRefusal::EmptyWindow { from, to }),
        }
    }

    pub fn from(self) -> NaiveTime {
        self.from
    }

    pub fn to(self) -> NaiveTime {
        self.to
    }
}

/// A run of same-side market orders: `pieces` prints `spacing` apart, each moving the log price by its equal share of
/// `strength`, so a permanent impact sums to `strength` while a fading one peaks below it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Flow {
    pub strength: BasisPoints,
    pub prevalence: Prevalence,
    pub pieces: NonZeroU32,
    pub spacing: Span,
}

impl Flow {
    /// The pieces as the multiplier a `TimeDelta` takes; `Market::new` refuses a flow it cannot schedule.
    fn piece_count(self) -> i32 {
        i32::try_from(self.pieces.get()).expect("Market::new refuses a flow it cannot schedule")
    }

    /// From the first piece to the last, or `None` when the pieces or the span are more than a schedule can count.
    fn checked_duration(self) -> Option<TimeDelta> {
        let pieces = i32::try_from(self.pieces.get()).ok()?;
        // chrono's `checked_mul` can return a delta past `TimeDelta::MAX`, so the bound is checked here.
        self.spacing
            .get()
            .checked_mul(pieces - 1)
            .filter(|duration| *duration <= TimeDelta::MAX)
    }

    /// From the first piece to the last.
    fn duration(self) -> TimeDelta {
        self.checked_duration()
            .expect("Market::new refuses a flow it cannot schedule")
    }
}

/// One mechanism a specification plants, each with its strength, prevalence, decay and timing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mechanism {
    /// Impatient flow starting anywhere in `window`, whose impact fades with `half_life`.
    ImpatientFlow {
        flow: Flow,
        half_life: Span,
        window: Window,
    },
    /// A parent order worked in pieces anywhere in `window`, whose impact is permanent.
    WorkedParent { flow: Flow, window: Window },
    /// Forced flow whose last piece lands one spacing before the close, its impact fading with `half_life`.
    ClosingFlow { flow: Flow, half_life: Span },
}

/// A mechanism's name, as `Planted` records and the arm's parameters spell it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum MechanismKind {
    ImpatientFlow,
    WorkedParent,
    ClosingFlow,
}

impl Mechanism {
    pub fn kind(&self) -> MechanismKind {
        match self {
            Self::ImpatientFlow { .. } => MechanismKind::ImpatientFlow,
            Self::WorkedParent { .. } => MechanismKind::WorkedParent,
            Self::ClosingFlow { .. } => MechanismKind::ClosingFlow,
        }
    }

    pub fn flow(&self) -> Flow {
        match self {
            Self::ImpatientFlow { flow, .. }
            | Self::WorkedParent { flow, .. }
            | Self::ClosingFlow { flow, .. } => *flow,
        }
    }

    /// The fade of the impact, or `None` for an impact that is permanent.
    fn half_life(&self) -> Option<Span> {
        match self {
            Self::ImpatientFlow { half_life, .. } | Self::ClosingFlow { half_life, .. } => {
                Some(*half_life)
            }
            Self::WorkedParent { .. } => None,
        }
    }

    fn settings(&self, prefix: &str) -> Vec<(String, String)> {
        let flow = self.flow();
        let mut settings = vec![
            (format!("{prefix}.kind"), self.kind().to_string()),
            (
                format!("{prefix}.strength_basis_points"),
                flow.strength.value().to_string(),
            ),
            (
                format!("{prefix}.prevalence"),
                flow.prevalence.value().to_string(),
            ),
            (format!("{prefix}.pieces"), flow.pieces.to_string()),
            (
                format!("{prefix}.spacing_seconds"),
                flow.spacing.seconds().to_string(),
            ),
        ];
        if let Some(half_life) = self.half_life() {
            settings.push((
                format!("{prefix}.half_life_seconds"),
                half_life.seconds().to_string(),
            ));
        }
        match self {
            Self::ImpatientFlow { window, .. } | Self::WorkedParent { window, .. } => {
                settings.push((
                    format!("{prefix}.window"),
                    format!("{}-{}", window.from, window.to),
                ));
            }
            Self::ClosingFlow { .. } => {}
        }
        settings
    }
}

/// The marginal shapes names are drawn from: each name draws one value of each, fitted across real names, and its
/// prints, quotes and spreads vary around that value within a session.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Shapes {
    /// The opening price in dollars.
    pub price: LogNormal,
    /// The daily standard deviation of the name's own log return.
    pub volatility: LogNormal,
    /// The name's loading on the market's log return.
    pub beta: LogNormal,
    pub trades_per_minute: LogNormal,
    pub quotes_per_minute: LogNormal,
    /// The mean quoted spread in ticks, at least one.
    pub spread_ticks: LogNormal,
    /// The mean shares per print.
    pub trade_size: LogNormal,
    /// The mean shares quoted on each side.
    pub quote_size: LogNormal,
    pub market_volatility: Volatility,
}

impl Shapes {
    fn settings(&self) -> Vec<(String, String)> {
        let shapes = [
            ("price", self.price),
            ("volatility", self.volatility),
            ("beta", self.beta),
            ("trades_per_minute", self.trades_per_minute),
            ("quotes_per_minute", self.quotes_per_minute),
            ("spread_ticks", self.spread_ticks),
            ("trade_size", self.trade_size),
            ("quote_size", self.quote_size),
        ];
        let mut settings: Vec<(String, String)> = shapes
            .iter()
            .flat_map(|(name, shape)| {
                [
                    (format!("shapes.{name}.median"), shape.median().to_string()),
                    (
                        format!("shapes.{name}.dispersion"),
                        shape.dispersion().to_string(),
                    ),
                ]
            })
            .collect();
        settings.push((
            "shapes.market_volatility".to_string(),
            self.market_volatility.value().to_string(),
        ));
        settings
    }
}

/// One arm of the synthetic market: its seed, its shapes and what it plants.
#[derive(Debug, Clone, PartialEq)]
pub struct Specification {
    pub seed: Seed,
    pub shapes: Shapes,
    pub mechanisms: Vec<Mechanism>,
}

impl Specification {
    /// The null arm: the same seed and shapes, planting nothing.
    pub fn null(&self) -> Self {
        Self {
            seed: self.seed,
            shapes: self.shapes,
            mechanisms: Vec::new(),
        }
    }

    /// Every field of the arm by name, so the journal names what was planted and at what strength.
    pub fn parameters(&self) -> Parameters {
        let mut settings = vec![
            ("seed".to_string(), self.seed.value().to_string()),
            ("mechanisms".to_string(), self.mechanisms.len().to_string()),
        ];
        settings.extend(self.shapes.settings());
        for (index, mechanism) in self.mechanisms.iter().enumerate() {
            settings.extend(mechanism.settings(&format!("mechanisms.{index}")));
        }
        Parameters::new(settings).expect("each setting's name is distinct and on one line")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn shapes() -> Shapes {
        let fixed = |median| LogNormal::new(median, 0.0).unwrap();
        Shapes {
            price: fixed(100.0),
            volatility: fixed(0.02),
            beta: fixed(1.0),
            trades_per_minute: fixed(6.0),
            quotes_per_minute: fixed(12.0),
            spread_ticks: fixed(2.0),
            trade_size: fixed(100.0),
            quote_size: fixed(300.0),
            market_volatility: Volatility::new(0.01).unwrap(),
        }
    }

    pub(super) fn flow(strength: f64, prevalence: f64, pieces: u32, spacing_seconds: i64) -> Flow {
        Flow {
            strength: BasisPoints::new(strength).unwrap(),
            prevalence: Prevalence::new(prevalence).unwrap(),
            pieces: NonZeroU32::new(pieces).unwrap(),
            spacing: Span::new(TimeDelta::seconds(spacing_seconds)).unwrap(),
        }
    }

    pub(super) fn window(from: &str, to: &str) -> Window {
        Window::new(from.parse().unwrap(), to.parse().unwrap()).unwrap()
    }

    #[test]
    fn test_values_outside_their_domain_are_refused() {
        assert_eq!(
            Prevalence::new(1.5),
            Err(SpecificationRefusal::Prevalence { value: 1.5 })
        );
        assert_eq!(
            Volatility::new(-0.1),
            Err(SpecificationRefusal::Volatility { value: -0.1 })
        );
        assert_eq!(
            Span::new(TimeDelta::zero()),
            Err(SpecificationRefusal::Span {
                span: TimeDelta::zero()
            })
        );
        let noon: NaiveTime = "12:00:00".parse().unwrap();
        assert_eq!(
            Window::new(noon, noon),
            Err(SpecificationRefusal::EmptyWindow {
                from: noon,
                to: noon
            })
        );
    }

    #[test]
    fn test_the_parameters_name_every_field_of_the_arm() {
        let specification = Specification {
            seed: Seed::new(7),
            shapes: shapes(),
            mechanisms: vec![
                Mechanism::ImpatientFlow {
                    flow: flow(25.0, 0.5, 4, 2),
                    half_life: Span::new(TimeDelta::seconds(300)).unwrap(),
                    window: window("10:00:00", "15:00:00"),
                },
                Mechanism::ClosingFlow {
                    flow: flow(40.0, 0.1, 10, 30),
                    half_life: Span::new(TimeDelta::seconds(3600)).unwrap(),
                },
            ],
        };
        let parameters = specification.parameters();
        let settings = parameters.settings();
        assert_eq!(settings.len(), 2 + 17 + 7 + 6);
        let setting = |name: &str| settings.get(name).map(String::as_str);
        assert_eq!(setting("seed"), Some("7"));
        assert_eq!(setting("mechanisms"), Some("2"));
        assert_eq!(setting("shapes.spread_ticks.median"), Some("2"));
        assert_eq!(setting("mechanisms.0.kind"), Some("impatient_flow"));
        assert_eq!(setting("mechanisms.0.window"), Some("10:00:00-15:00:00"));
        assert_eq!(setting("mechanisms.1.strength_basis_points"), Some("40"));
        assert_eq!(setting("mechanisms.1.half_life_seconds"), Some("3600"));
        assert_eq!(setting("mechanisms.1.window"), None);
    }

    #[test]
    fn test_the_null_arm_differs_only_in_planting_nothing() {
        let specification = Specification {
            seed: Seed::new(7),
            shapes: shapes(),
            mechanisms: vec![Mechanism::WorkedParent {
                flow: flow(10.0, 1.0, 5, 60),
                window: window("10:00:00", "15:00:00"),
            }],
        };
        let null = specification.null();
        assert_eq!(null.seed, specification.seed);
        assert_eq!(null.shapes, specification.shapes);
        assert!(null.mechanisms.is_empty());
        assert_eq!(null.parameters().settings().get("mechanisms").unwrap(), "0");
    }
}
