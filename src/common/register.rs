//! The Register: every test against the substrate, opened before it runs and closed with a verdict, so its count is
//! the denominator the multiple-testing haircut reads. Fields legacy wrote as prose keep a variant saying so, and are
//! never reconstructed into the structured form.

use std::fmt::Display;
use std::num::NonZeroU32;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::common::journal::Commit;
use crate::common::market::Dollars;
use crate::common::market::record::BarInterval;
use crate::common::time::SessionDate;

/// Assigned once, in order, and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccessionNumber(NonZeroU32);

impl AccessionNumber {
    pub const FIRST: Self = Self(NonZeroU32::MIN);

    pub fn new(number: u32) -> Option<Self> {
        NonZeroU32::new(number).map(Self)
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Six digits at least, so a listing sorts in order through 999,999; past that the number only grows wider.
impl Display for AccessionNumber {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:06}", self.0)
    }
}

impl FromStr for AccessionNumber {
    type Err = RegisterRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse()
            .ok()
            .and_then(Self::new)
            .ok_or_else(|| malformed("accession number", raw))
    }
}

/// The number after the highest held, compared as numbers rather than read off a listing's order; `None` once
/// `u32` is spent.
pub fn next_number(held: impl IntoIterator<Item = AccessionNumber>) -> Option<AccessionNumber> {
    match held.into_iter().max() {
        None => Some(AccessionNumber::FIRST),
        Some(highest) => highest.get().checked_add(1).and_then(AccessionNumber::new),
    }
}

/// The multiple-testing bucket a haircut is taken over: lowercase letters, digits and `-`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Family(String);

impl Family {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Family {
    type Err = RegisterRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        is_slug(raw)
            .then(|| Self(raw.to_string()))
            .ok_or_else(|| malformed("family", raw))
    }
}

impl TryFrom<String> for Family {
    type Error = RegisterRefusal;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        raw.parse()
    }
}

impl From<Family> for String {
    fn from(family: Family) -> Self {
        family.0
    }
}

fn is_slug(raw: &str) -> bool {
    !raw.is_empty()
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// The instruments a test measures over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Universe {
    /// Written `name@version`; a changed membership rule is a new version, never an edit.
    Versioned { name: String, version: NonZeroU32 },
    /// Named in prose before universes were versioned.
    Legacy(String),
}

impl FromStr for Universe {
    type Err = RegisterRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.split_once('@')
            .filter(|(name, _)| is_slug(name))
            .and_then(|(name, version)| {
                Some(Self::Versioned {
                    name: name.to_string(),
                    version: version.parse().ok()?,
                })
            })
            .ok_or_else(|| malformed("universe (name@version)", raw))
    }
}

/// How far ahead a test reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Horizon {
    Sessions(NonZeroU32),
    Bars {
        interval: BarInterval,
        count: NonZeroU32,
    },
    /// Described in prose before horizons were typed.
    Described(String),
}

impl FromStr for Horizon {
    type Err = RegisterRefusal;

    /// `<count> sessions` or `<count> <interval> bars`, as in `12 one_minute bars`.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let words: Vec<&str> = raw.split_whitespace().collect();
        let parsed = match words.as_slice() {
            [count, "sessions"] => count.parse().ok().map(Self::Sessions),
            [count, interval, "bars"] => count
                .parse()
                .ok()
                .zip(interval.parse().ok())
                .map(|(count, interval)| Self::Bars { interval, count }),
            _ => None,
        };
        parsed.ok_or_else(|| malformed("horizon", raw))
    }
}

/// The expected effect committed at opening, in the units the verdict is read in. Scored, never enforced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bid {
    Interval(Interval),
    /// Written as prose before bids were structured; kept as written.
    Written(String),
    /// Opened before bids existed.
    Unrecorded,
}

/// An estimate inside an interval expected to cover the truth at `coverage_percent`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "IntervalFields")]
pub struct Interval {
    estimate: f64,
    low: f64,
    high: f64,
    coverage_percent: u8,
    units: String,
}

#[derive(Deserialize)]
struct IntervalFields {
    estimate: f64,
    low: f64,
    high: f64,
    coverage_percent: u8,
    units: String,
}

impl TryFrom<IntervalFields> for Interval {
    type Error = RegisterRefusal;

    fn try_from(fields: IntervalFields) -> Result<Self, Self::Error> {
        Self::new(
            fields.estimate,
            fields.low,
            fields.high,
            fields.coverage_percent,
            fields.units,
        )
    }
}

impl Interval {
    /// Finite bounds around the estimate, coverage strictly between 0 and 100, and named units.
    pub fn new(
        estimate: f64,
        low: f64,
        high: f64,
        coverage_percent: u8,
        units: String,
    ) -> Result<Self, RegisterRefusal> {
        let ordered = [low, estimate, high].iter().all(|value| value.is_finite())
            && low <= estimate
            && estimate <= high;
        if !ordered || !(1..=99).contains(&coverage_percent) || !is_slug(&units) {
            return Err(RegisterRefusal::Interval {
                estimate,
                low,
                high,
                coverage_percent,
                units,
            });
        }
        Ok(Self {
            estimate,
            low,
            high,
            coverage_percent,
            units,
        })
    }

    pub fn estimate(&self) -> f64 {
        self.estimate
    }

    pub fn low(&self) -> f64 {
        self.low
    }

    pub fn high(&self) -> f64 {
        self.high
    }

    pub fn coverage_percent(&self) -> u8 {
        self.coverage_percent
    }

    pub fn units(&self) -> &str {
        &self.units
    }
}

impl FromStr for Interval {
    type Err = RegisterRefusal;

    /// `<estimate> [<low>, <high>] <coverage>% <units>`, as in `4 [0, 9] 80% net-bp`.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let parsed = (|| {
            let (estimate, rest) = raw.split_once('[')?;
            let (bounds, rest) = rest.split_once(']')?;
            let (low, high) = bounds.split_once(',')?;
            let (coverage, units) = rest.trim().split_once("% ")?;
            Some((
                estimate.trim().parse().ok()?,
                low.trim().parse().ok()?,
                high.trim().parse().ok()?,
                coverage.parse().ok()?,
                units.trim().to_string(),
            ))
        })();
        let (estimate, low, high, coverage, units) =
            parsed.ok_or_else(|| malformed("bid (estimate [low, high] coverage% units)", raw))?;
        Self::new(estimate, low, high, coverage, units)
    }
}

/// What a closed accession found.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Verdict {
    /// Cleared the named baseline, the split, cost and the family haircut; frozen.
    Accept,
    /// This hypothesis, universe and horizon, and nothing wider.
    Refute,
    /// Unmeasurable, underpowered, or the control fired too; still counts in the family.
    Inconclusive,
    /// Built and measured, kept available, but not earning default use.
    LandedNotAdopted,
}

/// The number the verdict rests on, in the bid's units.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Measured {
    Value(f64),
    /// The study produced no single number, as an unmeasurable one does.
    NotMeasured,
    /// Closed before the number was recorded beside the prose.
    Unrecorded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sessions {
    Counted(u32),
    Unrecorded,
}

/// What a test cost, so a two-week pass counts differently from a minute's check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StudyCost {
    pub wall_clock_seconds: Option<u64>,
    pub bytes_read: Option<u64>,
    /// Spent on a metered source.
    pub dollars: Option<Dollars>,
}

/// Everything committed when an accession opens, before its study runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "OpeningFields")]
pub struct Opening {
    family: Family,
    universe: Universe,
    horizon: Horizon,
    hypothesis: String,
    bid: Bid,
    opened: SessionDate,
    supersedes: Option<AccessionNumber>,
    /// Why a successor to an accepted accession is a new test rather than a re-measure.
    substrate_change: Option<String>,
}

#[derive(Deserialize)]
struct OpeningFields {
    family: Family,
    universe: Universe,
    horizon: Horizon,
    hypothesis: String,
    bid: Bid,
    opened: SessionDate,
    supersedes: Option<AccessionNumber>,
    substrate_change: Option<String>,
}

impl TryFrom<OpeningFields> for Opening {
    type Error = RegisterRefusal;

    fn try_from(fields: OpeningFields) -> Result<Self, Self::Error> {
        Self::new(
            fields.family,
            fields.universe,
            fields.horizon,
            fields.hypothesis,
            fields.bid,
            fields.opened,
            fields.supersedes,
            fields.substrate_change,
        )
    }
}

impl Opening {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        family: Family,
        universe: Universe,
        horizon: Horizon,
        hypothesis: String,
        bid: Bid,
        opened: SessionDate,
        supersedes: Option<AccessionNumber>,
        substrate_change: Option<String>,
    ) -> Result<Self, RegisterRefusal> {
        let hypothesis = stated(Some(hypothesis)).ok_or(RegisterRefusal::Blank {
            field: "hypothesis",
        })?;
        match &universe {
            Universe::Versioned { name, .. } if !is_slug(name) => {
                return Err(malformed("universe name", name));
            }
            Universe::Versioned { .. } | Universe::Legacy(_) => {}
        }
        Ok(Self {
            family,
            universe,
            horizon,
            hypothesis,
            bid,
            opened,
            supersedes,
            substrate_change: stated(substrate_change),
        })
    }

    pub fn family(&self) -> &Family {
        &self.family
    }

    pub fn universe(&self) -> &Universe {
        &self.universe
    }

    pub fn horizon(&self) -> &Horizon {
        &self.horizon
    }

    pub fn hypothesis(&self) -> &str {
        &self.hypothesis
    }

    pub fn bid(&self) -> &Bid {
        &self.bid
    }

    pub fn opened(&self) -> SessionDate {
        self.opened
    }

    pub fn supersedes(&self) -> Option<AccessionNumber> {
        self.supersedes
    }

    pub fn substrate_change(&self) -> Option<&str> {
        self.substrate_change.as_deref()
    }
}

/// Everything recorded when an accession closes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ClosingFields")]
pub struct Closing {
    verdict: Verdict,
    /// The finding in a sentence, beside the number in `measured`.
    statistic: String,
    measured: Measured,
    sessions: Sessions,
    commits: Vec<Commit>,
    closed: SessionDate,
    /// For an inconclusive verdict, the one change its successor makes; for landed-not-adopted, what would earn
    /// adoption.
    notes: Option<String>,
    cost: StudyCost,
}

#[derive(Deserialize)]
struct ClosingFields {
    verdict: Verdict,
    statistic: String,
    measured: Measured,
    sessions: Sessions,
    commits: Vec<Commit>,
    closed: SessionDate,
    notes: Option<String>,
    cost: StudyCost,
}

impl TryFrom<ClosingFields> for Closing {
    type Error = RegisterRefusal;

    fn try_from(fields: ClosingFields) -> Result<Self, Self::Error> {
        Self::new(
            fields.verdict,
            fields.statistic,
            fields.measured,
            fields.sessions,
            fields.commits,
            fields.closed,
            fields.notes,
            fields.cost,
        )
    }
}

impl Closing {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        verdict: Verdict,
        statistic: String,
        measured: Measured,
        sessions: Sessions,
        commits: Vec<Commit>,
        closed: SessionDate,
        notes: Option<String>,
        cost: StudyCost,
    ) -> Result<Self, RegisterRefusal> {
        let statistic =
            stated(Some(statistic)).ok_or(RegisterRefusal::Blank { field: "statistic" })?;
        match measured {
            Measured::Value(value) if !value.is_finite() => {
                return Err(RegisterRefusal::NotFinite { value });
            }
            Measured::Value(_) | Measured::NotMeasured | Measured::Unrecorded => {}
        }
        let notes = stated(notes);
        match (verdict, &notes) {
            (Verdict::Inconclusive, None) => Err(RegisterRefusal::InconclusiveWithoutNotes),
            (Verdict::LandedNotAdopted, None) => {
                Err(RegisterRefusal::LandedWithoutAdoptionCondition)
            }
            (
                Verdict::Accept
                | Verdict::Refute
                | Verdict::Inconclusive
                | Verdict::LandedNotAdopted,
                _,
            ) => Ok(Self {
                verdict,
                statistic,
                measured,
                sessions,
                commits,
                closed,
                notes,
                cost,
            }),
        }
    }

    pub fn verdict(&self) -> Verdict {
        self.verdict
    }

    pub fn statistic(&self) -> &str {
        &self.statistic
    }

    pub fn measured(&self) -> Measured {
        self.measured
    }

    pub fn sessions(&self) -> Sessions {
        self.sessions
    }

    pub fn commits(&self) -> &[Commit] {
        &self.commits
    }

    pub fn closed(&self) -> SessionDate {
        self.closed
    }

    pub fn notes(&self) -> Option<&str> {
        self.notes.as_deref()
    }

    pub fn cost(&self) -> StudyCost {
        self.cost
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Closed(Closing),
}

/// One test against the substrate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Accession {
    number: AccessionNumber,
    opening: Opening,
    status: Status,
}

/// Proof that a study names an accession open at the moment it was read; only the Register hands one out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAccession {
    number: AccessionNumber,
    family: Family,
}

impl OpenAccession {
    pub fn number(&self) -> AccessionNumber {
        self.number
    }

    pub fn family(&self) -> &Family {
        &self.family
    }
}

/// Why a change to the Register was refused.
#[derive(Debug, Clone, PartialEq)]
pub enum RegisterRefusal {
    Malformed {
        what: &'static str,
        raw: String,
    },
    Blank {
        field: &'static str,
    },
    Interval {
        estimate: f64,
        low: f64,
        high: f64,
        coverage_percent: u8,
        units: String,
    },
    InconclusiveWithoutNotes,
    LandedWithoutAdoptionCondition,
    AlreadyClosed {
        number: AccessionNumber,
    },
    /// A study may only run under an open accession; a closed one is a new test.
    NotOpen {
        number: AccessionNumber,
    },
    StillOpen {
        number: AccessionNumber,
    },
    AcceptedIsFrozen {
        number: AccessionNumber,
    },
    /// JSON has no NaN or infinity, so such a measurement would not read back.
    NotFinite {
        value: f64,
    },
}

impl Display for RegisterRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed { what, raw } => write!(formatter, "`{raw}` is not a {what}"),
            Self::Blank { field } => write!(formatter, "the {field} is blank"),
            Self::Interval {
                estimate,
                low,
                high,
                coverage_percent,
                units,
            } => write!(
                formatter,
                "{estimate} [{low}, {high}] {coverage_percent}% {units} is not an estimate inside its interval, \
                 with coverage between 1 and 99 and named units"
            ),
            Self::InconclusiveWithoutNotes => {
                write!(
                    formatter,
                    "an inconclusive verdict must name the one change its successor makes"
                )
            }
            Self::LandedWithoutAdoptionCondition => {
                write!(
                    formatter,
                    "a landed-not-adopted verdict must name what would earn it adoption"
                )
            }
            Self::AlreadyClosed { number } => {
                write!(formatter, "accession {number} is already closed")
            }
            Self::NotOpen { number } => {
                write!(
                    formatter,
                    "accession {number} is closed, so a study cannot run under it"
                )
            }
            Self::StillOpen { number } => {
                write!(
                    formatter,
                    "accession {number} is open, so it has no verdict to supersede"
                )
            }
            Self::AcceptedIsFrozen { number } => write!(
                formatter,
                "accession {number} was accepted and is frozen; its successor must name the substrate change"
            ),
            Self::NotFinite { value } => write!(formatter, "the measurement {value} is not finite"),
        }
    }
}

impl std::error::Error for RegisterRefusal {}

fn malformed(what: &'static str, raw: &str) -> RegisterRefusal {
    RegisterRefusal::Malformed {
        what,
        raw: raw.to_string(),
    }
}

/// Text that says something, so a blank value cannot stand in for one.
fn stated(text: Option<String>) -> Option<String> {
    text.map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

impl Accession {
    pub fn open(number: AccessionNumber, opening: Opening) -> Self {
        Self {
            number,
            opening,
            status: Status::Open,
        }
    }

    pub fn number(&self) -> AccessionNumber {
        self.number
    }

    pub fn opening(&self) -> &Opening {
        &self.opening
    }

    pub fn status(&self) -> &Status {
        &self.status
    }

    /// Records the verdict. An accession closes once; a second reading is a new accession.
    pub fn close(self, closing: Closing) -> Result<Self, RegisterRefusal> {
        match self.status {
            Status::Closed(_) => Err(RegisterRefusal::AlreadyClosed {
                number: self.number,
            }),
            Status::Open => Ok(Self {
                status: Status::Closed(closing),
                ..self
            }),
        }
    }

    /// The proof a study needs to run, given only while this accession is open.
    pub fn study(&self) -> Result<OpenAccession, RegisterRefusal> {
        match self.status {
            Status::Open => Ok(OpenAccession {
                number: self.number,
                family: self.opening.family.clone(),
            }),
            Status::Closed(_) => Err(RegisterRefusal::NotOpen {
                number: self.number,
            }),
        }
    }

    /// Refuses `successor` unless this accession has a verdict; an accepted one is frozen, so its successor must name
    /// what changed underneath it. Any number of successors may re-test one accession, each a test of its own.
    pub fn admit_successor(&self, successor: &Opening) -> Result<(), RegisterRefusal> {
        match &self.status {
            Status::Open => Err(RegisterRefusal::StillOpen {
                number: self.number,
            }),
            Status::Closed(closing) => match (closing.verdict, successor.substrate_change()) {
                (Verdict::Accept, None) => Err(RegisterRefusal::AcceptedIsFrozen {
                    number: self.number,
                }),
                (Verdict::Accept, Some(_))
                | (Verdict::Refute | Verdict::Inconclusive | Verdict::LandedNotAdopted, _) => {
                    Ok(())
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 1).unwrap())
    }

    fn number(raw: u32) -> AccessionNumber {
        AccessionNumber::new(raw).unwrap()
    }

    fn opening(substrate_change: Option<&str>) -> Opening {
        Opening::new(
            "overnight".parse().unwrap(),
            "liquid-common@1".parse().unwrap(),
            "1 sessions".parse().unwrap(),
            "close-to-open returns persist net of cost".to_string(),
            Bid::Interval("4 [0, 9] 80% net-bp".parse().unwrap()),
            session(),
            None,
            substrate_change.map(str::to_string),
        )
        .unwrap()
    }

    fn closing(verdict: Verdict, notes: Option<&str>) -> Result<Closing, RegisterRefusal> {
        Closing::new(
            verdict,
            "-5.7bp net".to_string(),
            Measured::Value(-5.7),
            Sessions::Counted(1253),
            Vec::new(),
            session(),
            notes.map(str::to_string),
            StudyCost::default(),
        )
    }

    fn closed(verdict: Verdict) -> Accession {
        Accession::open(number(5), opening(None))
            .close(closing(verdict, Some("one change")).unwrap())
            .unwrap()
    }

    #[test]
    fn test_the_next_number_follows_the_highest_as_a_number() {
        assert_eq!(next_number([]), Some(number(1)));
        assert_eq!(
            next_number([number(3), number(9_999), number(12)]),
            Some(number(10_000))
        );
        assert_eq!(next_number([number(u32::MAX)]), None);
        assert_eq!(number(7).to_string(), "000007");
        assert_eq!(number(1_000_000).to_string(), "1000000");
        assert_eq!("000007".parse(), Ok(number(7)));
        assert!("0".parse::<AccessionNumber>().is_err());
    }

    #[test]
    fn test_typed_fields_read_their_written_forms() {
        assert_eq!(
            "liquid-common@3".parse(),
            Ok(Universe::Versioned {
                name: "liquid-common".to_string(),
                version: NonZeroU32::new(3).unwrap()
            })
        );
        assert_eq!(
            "12 one_minute bars".parse(),
            Ok(Horizon::Bars {
                interval: BarInterval::OneMinute,
                count: NonZeroU32::new(12).unwrap()
            })
        );
        let interval: Interval = "+4 [0, 9.5] 80% net-bp".parse().unwrap();
        assert_eq!(
            (
                interval.estimate(),
                interval.low(),
                interval.high(),
                interval.coverage_percent(),
                interval.units()
            ),
            (4.0, 0.0, 9.5, 80, "net-bp")
        );
        for refused in ["liquid-common", "Liquid@1", "x@0"] {
            assert!(refused.parse::<Universe>().is_err(), "{refused}");
        }
        for refused in ["0 sessions", "5 days", "3 one_week bars"] {
            assert!(refused.parse::<Horizon>().is_err(), "{refused}");
        }
        for refused in [
            "10 [0, 9] 80% net-bp",
            "4 [0, 9] 100% net-bp",
            "4 [0, 9] 80% ",
            "4 [0 9] 80% bp",
            "NaN [0, 9] 80% bp",
        ] {
            assert!(refused.parse::<Interval>().is_err(), "{refused}");
        }
        for refused in ["", "Overnight", "over night"] {
            assert!(refused.parse::<Family>().is_err(), "{refused:?}");
        }
    }

    #[test]
    fn test_a_blank_hypothesis_or_statistic_is_refused() {
        let blank = Opening::new(
            "overnight".parse().unwrap(),
            Universe::Legacy("prose".to_string()),
            Horizon::Described("1 session".to_string()),
            "   ".to_string(),
            Bid::Unrecorded,
            session(),
            None,
            None,
        );
        assert_eq!(
            blank,
            Err(RegisterRefusal::Blank {
                field: "hypothesis"
            })
        );
        let blank = Closing::new(
            Verdict::Refute,
            " ".to_string(),
            Measured::NotMeasured,
            Sessions::Unrecorded,
            Vec::new(),
            session(),
            None,
            StudyCost::default(),
        );
        assert_eq!(blank, Err(RegisterRefusal::Blank { field: "statistic" }));
    }

    #[test]
    fn test_an_inconclusive_or_landed_verdict_needs_its_notes() {
        assert_eq!(
            closing(Verdict::Inconclusive, Some("  ")),
            Err(RegisterRefusal::InconclusiveWithoutNotes)
        );
        assert_eq!(
            closing(Verdict::LandedNotAdopted, None),
            Err(RegisterRefusal::LandedWithoutAdoptionCondition)
        );
        assert!(closing(Verdict::Refute, None).is_ok());
        assert!(closing(Verdict::Accept, None).is_ok());
    }

    #[test]
    fn test_an_accession_closes_once_and_studies_run_only_while_open() {
        let open = Accession::open(number(5), opening(None));
        assert_eq!(
            open.study()
                .map(|proof| (proof.number(), proof.family().as_str().to_string())),
            Ok((number(5), "overnight".to_string()))
        );
        let closed = open.close(closing(Verdict::Refute, None).unwrap()).unwrap();
        assert_eq!(
            closed.study(),
            Err(RegisterRefusal::NotOpen { number: number(5) })
        );
        assert_eq!(
            closed.close(closing(Verdict::Accept, None).unwrap()),
            Err(RegisterRefusal::AlreadyClosed { number: number(5) })
        );
    }

    #[test]
    fn test_a_successor_needs_a_verdict_and_an_accepted_one_a_substrate_change() {
        let open = Accession::open(number(5), opening(None));
        assert_eq!(
            open.admit_successor(&opening(None)),
            Err(RegisterRefusal::StillOpen { number: number(5) })
        );
        assert_eq!(
            closed(Verdict::Accept).admit_successor(&opening(Some(" "))),
            Err(RegisterRefusal::AcceptedIsFrozen { number: number(5) })
        );
        assert!(
            closed(Verdict::Accept)
                .admit_successor(&opening(Some("re-fold of 2026-10")))
                .is_ok()
        );
        for verdict in [
            Verdict::Refute,
            Verdict::Inconclusive,
            Verdict::LandedNotAdopted,
        ] {
            assert!(
                closed(verdict).admit_successor(&opening(None)).is_ok(),
                "{verdict}"
            );
        }
    }

    #[test]
    fn test_a_versioned_universe_or_a_measurement_cannot_bypass_its_rule() {
        let wire = serde_json::to_string(&closed(Verdict::Refute)).unwrap();
        let named = wire.replace(r#""name":"liquid-common""#, r#""name":"Bad Name""#);
        assert!(serde_json::from_str::<Accession>(&named).is_err());
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let refused = Closing::new(
                Verdict::Refute,
                "s".to_string(),
                Measured::Value(value),
                Sessions::Unrecorded,
                Vec::new(),
                session(),
                None,
                StudyCost::default(),
            );
            assert!(
                matches!(refused, Err(RegisterRefusal::NotFinite { .. })),
                "{value}"
            );
        }
    }

    #[test]
    fn test_serde_and_strum_agree_on_every_verdict() {
        for verdict in Verdict::iter() {
            let json = serde_json::to_string(&verdict).unwrap();
            assert_eq!(json, format!("\"{verdict}\""));
            assert_eq!(verdict.to_string().parse::<Verdict>(), Ok(verdict));
        }
    }

    #[test]
    fn test_an_accession_encodes_to_its_wire_format() {
        let accession = closed(Verdict::Refute);
        assert_eq!(
            serde_json::to_string(&accession).unwrap(),
            concat!(
                r#"{"number":5,"opening":{"family":"overnight","universe":{"versioned":{"name":"liquid-common","version":1}},"#,
                r#""horizon":{"sessions":1},"hypothesis":"close-to-open returns persist net of cost","#,
                r#""bid":{"interval":{"estimate":4.0,"low":0.0,"high":9.0,"coverage_percent":80,"units":"net-bp"}},"#,
                r#""opened":"2026-10-01","supersedes":null,"substrate_change":null},"#,
                r#""status":{"closed":{"verdict":"refute","statistic":"-5.7bp net","measured":{"value":-5.7},"#,
                r#""sessions":{"counted":1253},"commits":[],"closed":"2026-10-01","notes":"one change","#,
                r#""cost":{"wall_clock_seconds":null,"bytes_read":null,"dollars":null}}}}"#,
            )
        );
    }

    /// Stored JSON goes through the same constructors, so an edited object cannot hand back an invalid value.
    #[test]
    fn test_invalid_stored_values_do_not_read_back() {
        let interval =
            r#"{"estimate":10.0,"low":0.0,"high":9.0,"coverage_percent":80,"units":"net-bp"}"#;
        assert!(serde_json::from_str::<Interval>(interval).is_err());
        let wire = serde_json::to_string(&closed(Verdict::Refute)).unwrap();
        let blank = wire.replace("close-to-open returns persist net of cost", " ");
        assert!(serde_json::from_str::<Accession>(&blank).is_err());
        let unexplained = wire
            .replace(r#""verdict":"refute""#, r#""verdict":"inconclusive""#)
            .replace(r#""notes":"one change""#, r#""notes":null"#);
        assert!(serde_json::from_str::<Accession>(&unexplained).is_err());
        assert!(
            serde_json::from_str::<Accession>(&wire.replace(r#""number":5"#, r#""number":0"#))
                .is_err()
        );
    }

    fn any_accession() -> impl Strategy<Value = Accession> {
        let universe = prop_oneof![
            ("[a-z][a-z-]{0,10}", 1_u32..50).prop_map(|(name, version)| Universe::Versioned {
                name,
                version: NonZeroU32::new(version).unwrap()
            }),
            ".{1,20}".prop_map(Universe::Legacy),
        ];
        let horizon = prop_oneof![
            (1_u32..100).prop_map(|count| Horizon::Sessions(NonZeroU32::new(count).unwrap())),
            ".{1,20}".prop_map(Horizon::Described),
        ];
        let bid = prop_oneof![
            Just(Bid::Unrecorded),
            ".{1,30}".prop_map(Bid::Written),
            (-50_i32..50, 0_i32..20, 1_u8..100).prop_map(|(estimate, width, coverage)| {
                Bid::Interval(
                    Interval::new(
                        f64::from(estimate),
                        f64::from(estimate - width),
                        f64::from(estimate + width),
                        coverage,
                        "net-bp".to_string(),
                    )
                    .unwrap(),
                )
            }),
        ];
        let closing = prop::option::of((
            prop::sample::select(Verdict::iter().collect::<Vec<_>>()),
            prop_oneof![
                (-1_000_i32..1_000).prop_map(|value| Measured::Value(f64::from(value) / 10.0)),
                Just(Measured::NotMeasured),
                Just(Measured::Unrecorded)
            ],
            prop::option::of(0_u32..2_000),
        ));
        (
            1_u32..1_000_000,
            universe,
            horizon,
            bid,
            prop::option::of(1_u32..100),
            closing,
        )
            .prop_map(|(raw, universe, horizon, bid, supersedes, closing)| {
                let opening = Opening::new(
                    "overnight".parse().unwrap(),
                    universe,
                    horizon,
                    "a hypothesis".to_string(),
                    bid,
                    session(),
                    supersedes.and_then(AccessionNumber::new),
                    None,
                )
                .unwrap();
                let accession = Accession::open(number(raw), opening);
                match closing {
                    None => accession,
                    Some((verdict, measured, sessions)) => {
                        let closing = Closing::new(
                            verdict,
                            "a finding".to_string(),
                            measured,
                            sessions.map_or(Sessions::Unrecorded, Sessions::Counted),
                            Vec::new(),
                            session(),
                            Some("the one change".to_string()),
                            StudyCost::default(),
                        )
                        .unwrap();
                        accession.close(closing).unwrap()
                    }
                }
            })
    }

    proptest! {
        #[test]
        fn property_an_accession_reads_back_as_itself(accession in any_accession()) {
            let json = serde_json::to_string(&accession).unwrap();
            prop_assert_eq!(serde_json::from_str::<Accession>(&json).unwrap(), accession);
        }
    }
}
