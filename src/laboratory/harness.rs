//! One comparison, declared before it is measured and scored against the tests sharing its family.
//!
//! Lowering the cost of testing without raising the bar for believing generates confident nonsense
//! at scale, so the multiple-testing count is a constructor argument rather than a convention.

use std::num::NonZeroUsize;

use serde::Serialize;

use crate::common::types::{BarInterval, BasisPoints, LiquidityFloor, ScreenWindow};
use crate::laboratory::cost::{CostModel, CostRefusal};
use crate::laboratory::dataset::DatasetFingerprint;
use crate::laboratory::metrics::{summarize, Distribution};

/// The share of families in which at least one reading is allowed to clear by chance.
///
/// Methodology rather than position-taking, so it is public and stays public: a reader who learns
/// this learns how carefully a claim is tested and nothing about what is traded.
pub const FAMILY_WISE_ERROR_RATE: f64 = 0.05;

/// The multiple-testing bucket a study belongs to, and how many tests that bucket holds.
///
/// `tests` is not defaulted and cannot be zero. A default would let "nobody counted" render exactly
/// like "this was the only test", which is the failure the count exists to make impossible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Family {
    name: String,
    tests: NonZeroUsize,
}

impl Family {
    pub fn new(name: impl Into<String>, tests: NonZeroUsize) -> Self {
        Self {
            name: name.into(),
            tests,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tests(&self) -> NonZeroUsize {
        self.tests
    }
}

/// How many standard errors a reading has to clear, given how many chances its hypothesis had.
///
/// Bonferroni rather than Šidák: readings inside a family share a universe and a window and are
/// therefore correlated, and Bonferroni is the only one of the two whose bound holds without an
/// independence assumption nobody has checked.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Haircut {
    tests: NonZeroUsize,
    family_wise_error_rate: f64,
    required_standard_errors: f64,
}

impl Haircut {
    /// `None` unless the rate is a probability strictly between zero and one.
    ///
    /// Zero would demand infinitely many standard errors and one would demand none, and both are
    /// ways of declining to test rather than settings of a test.
    pub fn new(tests: NonZeroUsize, family_wise_error_rate: f64) -> Option<Self> {
        if !(family_wise_error_rate.is_finite()
            && family_wise_error_rate > 0.0
            && family_wise_error_rate < 1.0)
        {
            return None;
        }
        // Two-sided: a study that only ever looked for a positive effect would still have accepted
        // a large negative one as a finding, so both tails are spent.
        let tail = family_wise_error_rate / (2.0 * tests.get() as f64);
        Some(Self {
            tests,
            family_wise_error_rate,
            required_standard_errors: inverse_standard_normal(1.0 - tail)?,
        })
    }

    pub fn required_standard_errors(&self) -> f64 {
        self.required_standard_errors
    }

    pub fn family_wise_error_rate(&self) -> f64 {
        self.family_wise_error_rate
    }
}

/// How far ahead the reading looks.
///
/// Two variants because the archive holds two clocks, and a horizon of "5" means a week in one and
/// twenty-five minutes in the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Horizon {
    Sessions(NonZeroUsize),
    Bars {
        interval: BarInterval,
        count: NonZeroUsize,
    },
}

impl std::fmt::Display for Horizon {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Horizon::Sessions(count) => write!(formatter, "{count} sessions"),
            Horizon::Bars { interval, count } => write!(formatter, "{count} {interval} bars"),
        }
    }
}

/// The population a study measured over, named rather than described by a bare threshold.
///
/// `Unscreened` is a variant rather than an absent floor because it is a real and common answer —
/// the intraday datasets apply no screen at all — and a study over the whole market must not be
/// readable as a study over the traded book.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum DeclaredUniverse {
    Screened {
        name: String,
        floor: LiquidityFloor,
        window: ScreenWindow,
    },
    Unscreened,
}

impl DeclaredUniverse {
    /// The screen this declaration claims the dataset was built under.
    ///
    /// The name is deliberately not part of it: a name is a label the caller picks, and no property
    /// of a frame can confirm or deny it. The floor and the window are facts about the rows and are
    /// checked against the fingerprint; the name is recorded on the caller's word alone.
    fn screen(&self) -> Option<(LiquidityFloor, ScreenWindow)> {
        match self {
            DeclaredUniverse::Screened { floor, window, .. } => Some((*floor, *window)),
            DeclaredUniverse::Unscreened => None,
        }
    }
}

impl std::fmt::Display for DeclaredUniverse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeclaredUniverse::Screened {
                name,
                floor,
                window,
            } => write!(formatter, "{name} ({floor}, over {window})"),
            DeclaredUniverse::Unscreened => write!(formatter, "unscreened"),
        }
    }
}

/// What a reading measures, and therefore whether a round trip can be charged against it.
///
/// The cost model travels inside the priced variant so that netting a spread off a variance share
/// or a count of bits is unrepresentable rather than merely discouraged.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum Quantity {
    /// Basis points of return per round trip, signed, costable through the declared model.
    ReturnPerRoundTrip {
        cost_model: CostModel,
        /// The width the cost is charged at. Declared rather than derived, because the archive's
        /// whole-market spread distribution is contaminated and no study should quietly inherit it.
        quoted_spread: BasisPoints,
    },
    /// A reading with no round trip behind it: a correlation, a share, a rate, a count of bits.
    Unpriced { units: &'static str },
}

/// How the two arms relate, which is what decides how their errors combine.
///
/// Matched is the better arm whenever it is available — differencing session by session removes the
/// variation both arms share — and it is the only one of the two that requires aligned readings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Pairing {
    /// Both arms read the same sessions, so the difference is taken per session.
    Matched,
    /// The arms read disjoint sessions, so their errors add in quadrature.
    Disjoint,
}

/// One side of a comparison: what it was called, and what it read on each session.
///
/// Readings are per session because sessions are the level that actually varies. Entries inside one
/// session share legs and a universe, so an error taken over entries divides by far more
/// independence than is present; `observations` carries that row count beside the session count so
/// the two can never be mistaken for each other.
#[derive(Debug, Clone, PartialEq)]
pub struct Arm {
    name: String,
    readings: Vec<(SessionKey, Option<f64>)>,
    observations: usize,
}

/// A session's epoch milliseconds, used here only as an identity.
///
/// A bare `i64` rather than a newtype, because that is how the laboratory already keys a session —
/// [`crate::laboratory::predictor::Panel`] and [`crate::laboratory::convergence::Closes`] both do —
/// and what makes the pairing safe is the comparison below, not a wrapper around the number.
type SessionKey = i64;

impl Arm {
    /// `None` on an unnamed arm, on no readings at all, on a session read twice, or on fewer
    /// observations than readings.
    ///
    /// The observation bound is the one worth having: a reading is folded from observations, so an
    /// arm claiming more readings than rows has counted something twice.
    pub fn new(
        name: impl Into<String>,
        readings: Vec<(SessionKey, Option<f64>)>,
        observations: usize,
    ) -> Option<Self> {
        let name = name.into();
        let measured = readings.iter().filter(|(_, value)| value.is_some()).count();
        let distinct: std::collections::HashSet<SessionKey> =
            readings.iter().map(|(session, _)| *session).collect();

        (!name.trim().is_empty()
            && !readings.is_empty()
            && distinct.len() == readings.len()
            && observations >= measured)
            .then_some(Self {
                name,
                readings,
                observations,
            })
    }

    /// The sessions this arm read, in the order it read them.
    fn sessions(&self) -> impl Iterator<Item = SessionKey> + '_ {
        self.readings.iter().map(|(session, _)| *session)
    }

    /// What it read on each of them, in the same order.
    fn values(&self) -> impl Iterator<Item = Option<f64>> + '_ {
        self.readings.iter().map(|(_, value)| *value)
    }
}

/// Everything fixed before the numbers are looked at.
///
/// Held together in one value so that a study cannot be constructed having declared some of it: the
/// free choices are recorded with the reading they produced, which is the only order in which they
/// constrain anything.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Declaration {
    question: String,
    family: Family,
    horizon: Horizon,
    universe: DeclaredUniverse,
    quantity: Quantity,
}

impl Declaration {
    pub fn new(
        question: impl Into<String>,
        family: Family,
        horizon: Horizon,
        universe: DeclaredUniverse,
        quantity: Quantity,
    ) -> Self {
        Self {
            question: question.into(),
            family,
            horizon,
            universe,
            quantity,
        }
    }

    pub fn question(&self) -> &str {
        &self.question
    }

    pub fn family(&self) -> &Family {
        &self.family
    }

    pub fn horizon(&self) -> Horizon {
        self.horizon
    }

    pub fn universe(&self) -> &DeclaredUniverse {
        &self.universe
    }

    pub fn quantity(&self) -> Quantity {
        self.quantity
    }
}

/// Why a study could not be assembled, carrying the two values that disagreed.
#[derive(Debug, Clone, PartialEq)]
pub enum StudyRefusal {
    /// The declared universe is not the one the dataset was actually screened by.
    ///
    /// Both halves of the screen travel in the refusal, because a study declaring the right floor
    /// over the wrong window is the case a floor-only comparison cannot see.
    UniverseDisagrees {
        declared: Option<(LiquidityFloor, ScreenWindow)>,
        measured: Option<(LiquidityFloor, ScreenWindow)>,
    },
    /// Matched arms must read the same number of sessions.
    ArmsNotAligned { treatment: usize, control: usize },
    /// Matched arms read the same count of sessions, but not the same sessions.
    ///
    /// Separate from the length case because the remedy differs: a length mismatch is a caller that
    /// built one arm over a different window, where this is one that built both over the same window
    /// and lost their alignment. `measure` zips the two, so this would subtract unrelated readings.
    ArmsReadDifferentSessions {
        index: usize,
        treatment: SessionKey,
        control: SessionKey,
    },
    /// Disjoint arms must not read any session twice between them.
    ///
    /// Their errors are added in quadrature, which is only right where the two are independent.
    ArmsOverlap { shared: usize },
    /// Two arms under one name are one arm counted twice.
    ArmsNotDistinct { name: String },
    /// The family-wise error rate is not a probability.
    RateUnusable { family_wise_error_rate: f64 },
}

impl std::fmt::Display for StudyRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let screen = |value: &Option<(LiquidityFloor, ScreenWindow)>| match value {
            Some((floor, window)) => format!("{floor} over {window}"),
            None => "unscreened".to_string(),
        };
        match self {
            StudyRefusal::UniverseDisagrees { declared, measured } => write!(
                formatter,
                "the study declares {} but the dataset was screened by {}",
                screen(declared),
                screen(measured)
            ),
            StudyRefusal::ArmsNotAligned { treatment, control } => write!(
                formatter,
                "matched arms must read the same sessions, but the treatment read {treatment} and \
                 the control {control}"
            ),
            StudyRefusal::ArmsReadDifferentSessions {
                index,
                treatment,
                control,
            } => write!(
                formatter,
                "matched arms diverge at reading {index}: the treatment read session {treatment} \
                 where the control read {control}"
            ),
            StudyRefusal::ArmsOverlap { shared } => write!(
                formatter,
                "disjoint arms share {shared} session(s), so their errors do not add in quadrature"
            ),
            StudyRefusal::ArmsNotDistinct { name } => {
                write!(
                    formatter,
                    "both arms are named {name}, so there is no control"
                )
            }
            StudyRefusal::RateUnusable {
                family_wise_error_rate,
            } => write!(
                formatter,
                "a family-wise error rate of {family_wise_error_rate} is not a probability"
            ),
        }
    }
}

/// A declared comparison with both its arms attached, ready to be scored.
#[derive(Debug, Clone, PartialEq)]
pub struct Study {
    declaration: Declaration,
    pairing: Pairing,
    treatment: Arm,
    control: Arm,
    haircut: Haircut,
}

impl Study {
    /// Assembles a study, refusing the shapes whose readings would not mean what they say.
    ///
    /// The fingerprint is required rather than optional: the declared **floor and window** are
    /// checked against the screen the dataset was actually built under, so a study cannot claim to
    /// have measured the traded book while reading a frame screened by something else.
    ///
    /// The universe's *name* is not checked and cannot be — it is a label the caller chooses, and no
    /// property of a frame confirms it. It reaches the journal on the caller's word.
    pub fn new(
        declaration: Declaration,
        pairing: Pairing,
        treatment: Arm,
        control: Arm,
        fingerprint: &DatasetFingerprint,
    ) -> Result<Self, StudyRefusal> {
        if treatment.name == control.name {
            return Err(StudyRefusal::ArmsNotDistinct {
                name: treatment.name,
            });
        }
        let declared = declaration.universe.screen();
        if declared != fingerprint.screen() {
            return Err(StudyRefusal::UniverseDisagrees {
                declared,
                measured: fingerprint.screen(),
            });
        }
        // Each variant of `Pairing` is a claim about how the arms relate, and each is checked here
        // rather than trusted: `measure` reads the claim and cannot tell a false one from a true one.
        match pairing {
            Pairing::Matched => {
                if treatment.readings.len() != control.readings.len() {
                    return Err(StudyRefusal::ArmsNotAligned {
                        treatment: treatment.readings.len(),
                        control: control.readings.len(),
                    });
                }
                // Equal lengths are not equal sessions, and `measure` zips: two arms built over the
                // same window but offset by a session would subtract unrelated readings and report
                // a difference with nothing wrong on its face.
                let divergence = treatment
                    .sessions()
                    .zip(control.sessions())
                    .enumerate()
                    .find(|(_, (arm, control))| arm != control);
                if let Some((index, (arm, control))) = divergence {
                    return Err(StudyRefusal::ArmsReadDifferentSessions {
                        index,
                        treatment: arm,
                        control,
                    });
                }
            }
            Pairing::Disjoint => {
                let read: std::collections::HashSet<SessionKey> = treatment.sessions().collect();
                let shared = control
                    .sessions()
                    .filter(|session| read.contains(session))
                    .count();
                if shared > 0 {
                    return Err(StudyRefusal::ArmsOverlap { shared });
                }
            }
        }
        let haircut = Haircut::new(declaration.family.tests, FAMILY_WISE_ERROR_RATE).ok_or(
            StudyRefusal::RateUnusable {
                family_wise_error_rate: FAMILY_WISE_ERROR_RATE,
            },
        )?;

        Ok(Self {
            declaration,
            pairing,
            treatment,
            control,
            haircut,
        })
    }

    /// Scores the study, consuming it so the declaration travels with the number it produced.
    pub fn measure(self) -> StudyResult {
        let treatment = summarize(self.treatment.values());
        let control = summarize(self.control.values());
        let difference = match self.pairing {
            // Per session, so the variation both arms share cancels instead of being counted twice.
            // The zip is sound because `new` refused any matched pair whose sessions disagree.
            Pairing::Matched => summarize(
                self.treatment
                    .values()
                    .zip(self.control.values())
                    .map(|(arm, control)| Some(arm? - control?)),
            ),
            Pairing::Disjoint => disjoint_difference(treatment, control),
        };

        let net_of_cost = net_of_cost(self.declaration.quantity, difference);
        StudyResult {
            declaration: self.declaration,
            pairing: self.pairing,
            treatment_name: self.treatment.name,
            control_name: self.control.name,
            observations: match self.pairing {
                // Matched arms read one row set, so summing would count every row twice. Where the
                // two counts differ, the larger is the population either arm could have read.
                Pairing::Matched => self.treatment.observations.max(self.control.observations),
                Pairing::Disjoint => self.treatment.observations + self.control.observations,
            },
            treatment,
            control,
            difference,
            net_of_cost,
            haircut: self.haircut,
        }
    }
}

/// Treatment less control where the two read different sessions, errors added in quadrature.
///
/// The count is the sum rather than either arm's, because both arms' sessions inform the gap and
/// neither alone does.
fn disjoint_difference(
    treatment: Option<Distribution>,
    control: Option<Distribution>,
) -> Option<Distribution> {
    let (treatment, control) = (treatment?, control?);
    Some(Distribution {
        mean: treatment.mean - control.mean,
        standard_error: (treatment.standard_error.powi(2) + control.standard_error.powi(2)).sqrt(),
        sessions: treatment.sessions + control.sessions,
    })
}

/// What the difference is worth once the round trip it implies has been paid for.
///
/// Four outcomes rather than a number and a flag, because "no round trip to charge for" and "a
/// round trip nobody can price" are different answers and only one of them is about the data.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum NetOfCost {
    Net {
        gross_basis_points: f64,
        cost_basis_points: f64,
        net_basis_points: f64,
    },
    /// The readings are not a return, so no spread is owed on them.
    NotAReturn { units: &'static str },
    /// A return the declared fill style cannot be costed from.
    Refused(CostRefusal),
    /// No difference was measurable, so there is nothing to charge against.
    Unmeasured,
}

fn net_of_cost(quantity: Quantity, difference: Option<Distribution>) -> NetOfCost {
    match quantity {
        Quantity::Unpriced { units } => NetOfCost::NotAReturn { units },
        Quantity::ReturnPerRoundTrip {
            cost_model,
            quoted_spread,
        } => match difference {
            None => NetOfCost::Unmeasured,
            Some(difference) => match cost_model.cost(quoted_spread) {
                Err(refusal) => NetOfCost::Refused(refusal),
                Ok(cost) => NetOfCost::Net {
                    gross_basis_points: difference.mean,
                    cost_basis_points: cost.value(),
                    // Subtracted rather than signed-toward-zero: a strategy that earns a negative
                    // gross does not get paid the spread for being wrong.
                    net_basis_points: difference.mean - cost.value(),
                },
            },
        },
    }
}

/// What one study read, alongside everything that was fixed before it read it.
#[derive(Debug, Clone, PartialEq)]
pub struct StudyResult {
    declaration: Declaration,
    pairing: Pairing,
    treatment_name: String,
    control_name: String,
    /// Rows both arms were folded from, reported beside the session counts inside each
    /// distribution so a reading over 1,254 sessions and 600,000 rows cannot be read as 600,000
    /// independent observations.
    observations: usize,
    treatment: Option<Distribution>,
    control: Option<Distribution>,
    difference: Option<Distribution>,
    net_of_cost: NetOfCost,
    haircut: Haircut,
}

impl StudyResult {
    pub fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    pub fn pairing(&self) -> Pairing {
        self.pairing
    }

    pub fn treatment_name(&self) -> &str {
        &self.treatment_name
    }

    pub fn control_name(&self) -> &str {
        &self.control_name
    }

    pub fn observations(&self) -> usize {
        self.observations
    }

    pub fn treatment(&self) -> Option<Distribution> {
        self.treatment
    }

    pub fn control(&self) -> Option<Distribution> {
        self.control
    }

    pub fn difference(&self) -> Option<Distribution> {
        self.difference
    }

    pub fn net_of_cost(&self) -> NetOfCost {
        self.net_of_cost
    }

    pub fn haircut(&self) -> Haircut {
        self.haircut
    }

    /// How many standard errors the difference stands from zero.
    ///
    /// `None` where the difference could not be measured, and where its error is zero — a reading
    /// that never varied is pinned by the shape of the data rather than by an effect, so it has no
    /// number of errors rather than infinitely many.
    pub fn standard_errors(&self) -> Option<f64> {
        let difference = self.difference?;
        (difference.standard_error > 0.0).then(|| difference.mean / difference.standard_error)
    }

    /// Whether the difference clears what its family demands, in the direction it was measured.
    ///
    /// `None` rather than `false` where nothing could be measured: an unmeasurable study has not
    /// failed its threshold, it has failed to reach one.
    pub fn clears_haircut(&self) -> Option<bool> {
        Some(self.standard_errors()?.abs() >= self.haircut.required_standard_errors)
    }
}

/// One table per family, with the threshold that family's readings are judged against above it.
///
/// The haircut is rendered once per header rather than per row because it is a property of the
/// family: printing it beside each reading invites comparing a row against its own threshold, which
/// is the habit the count exists to break.
///
/// Results are **grouped** rather than assumed homogeneous. The header describes a family, a
/// universe and a horizon, so a slice spanning two of any of those has no single header — taking one
/// from the first row would file every later reading under a bar and a population that are not its
/// own. Grouping removes the invariant instead of checking it, and a slice that was already
/// homogeneous renders exactly as it did before.
pub fn render(results: &[StudyResult]) -> String {
    if results.is_empty() {
        return "No studies were measured.\n".to_string();
    }

    let mut rendered = String::new();
    for (index, group) in group_by_header(results).into_iter().enumerate() {
        if index > 0 {
            rendered.push('\n');
        }
        rendered.push_str(&render_group(&group));
    }
    rendered
}

/// Splits results into runs sharing a header, preserving the order they were first seen in.
///
/// Linear scan: a family holds a handful of studies, and preserving caller order matters more than
/// the lookup, because a table whose rows are reordered is a table a reader cannot check against the
/// binary that produced it.
fn group_by_header(results: &[StudyResult]) -> Vec<Vec<&StudyResult>> {
    let mut groups: Vec<Vec<&StudyResult>> = Vec::new();
    for result in results {
        let matching = groups.iter_mut().find(|group| {
            let first: &StudyResult = group[0];
            first.declaration.family == result.declaration.family
                && first.declaration.universe == result.declaration.universe
                && first.declaration.horizon == result.declaration.horizon
        });
        match matching {
            Some(group) => group.push(result),
            None => groups.push(vec![result]),
        }
    }
    groups
}

/// One family's table. Every row here shares the header above it by construction.
fn render_group(results: &[&StudyResult]) -> String {
    let first = results[0];
    let mut rendered = format!(
        "Family {}\n  {:.2} standard errors required at a {:.0}% family-wise error rate, \
         Bonferroni over {} tests\n  universe {}, horizon {}\n\n",
        first.declaration.family.name,
        first.haircut.required_standard_errors,
        first.haircut.family_wise_error_rate * 100.0,
        first.haircut.tests,
        first.declaration.universe,
        first.declaration.horizon,
    );
    rendered.push_str(&format!(
        "{:<40}{:<18}{:>28}{:>28}{:>28}{:>9}{:>8}{:>24}\n",
        "question",
        "against",
        "treatment",
        "control",
        "difference",
        "errors",
        "clears",
        "net of cost",
    ));
    for result in results {
        rendered.push_str(&format!(
            "{:<40}{:<18}{:>28}{:>28}{:>28}{:>9}{:>8}{:>24}\n",
            truncated(&result.declaration.question, 38),
            truncated(&result.control_name, 16),
            distribution(result.treatment),
            distribution(result.control),
            distribution(result.difference),
            result
                .standard_errors()
                .map_or_else(|| "none".to_string(), |errors| format!("{errors:+.2}")),
            match result.clears_haircut() {
                Some(true) => "yes",
                Some(false) => "no",
                None => "-",
            },
            net_of_cost_cell(result.net_of_cost),
        ));
    }

    // Per study, not per family: two studies in one family can be folded from different row counts,
    // and quoting the first row's number for all of them overstates how well the rest are supported.
    let counts: Vec<usize> = results.iter().map(|result| result.observations).collect();
    let (fewest, most) = (
        counts.iter().min().copied().unwrap_or(0),
        counts.iter().max().copied().unwrap_or(0),
    );
    let folded = if fewest == most {
        most.to_string()
    } else {
        format!("{fewest} to {most}")
    };
    rendered.push_str(&format!("\n  rows folded into these readings: {folded}\n"));
    rendered
}

/// A statistic with its standard error and the sessions behind it, or why there is none.
///
/// The one rendering of "mean ± error (n)" in the tree; three binaries previously carried
/// byte-identical private copies of it and three more open-coded it at different precisions.
pub fn distribution(value: Option<Distribution>) -> String {
    value.map_or_else(
        || "unmeasurable".to_string(),
        |distribution| {
            format!(
                "{:+.6} ± {:.6} ({})",
                distribution.mean, distribution.standard_error, distribution.sessions
            )
        },
    )
}

fn net_of_cost_cell(net_of_cost: NetOfCost) -> String {
    match net_of_cost {
        NetOfCost::Net {
            cost_basis_points,
            net_basis_points,
            ..
        } => format!("{net_basis_points:+.2} after {cost_basis_points:.2}bp"),
        NetOfCost::NotAReturn { units } => units.to_string(),
        NetOfCost::Refused(_) => "uncostable".to_string(),
        NetOfCost::Unmeasured => "unmeasurable".to_string(),
    }
}

/// Trims to `width` so one long question cannot shift every column on the rows beneath it.
fn truncated(value: &str, width: usize) -> String {
    match value.char_indices().nth(width) {
        Some((boundary, _)) => format!("{}…", &value[..boundary]),
        None => value.to_string(),
    }
}

/// The standard normal quantile: the `z` with `probability` of the mass below it.
///
/// Acklam's rational approximation, refined by one Halley step against a series evaluation of the
/// normal integral. The approximation alone is good to about 1.15e-9 relative and the refinement
/// takes it to the limits of the representation; the threshold this feeds is compared against a
/// t-ratio, so being right in the far tail is the whole point.
///
/// `None` outside the open unit interval, where no finite quantile exists.
fn inverse_standard_normal(probability: f64) -> Option<f64> {
    if !(probability.is_finite() && probability > 0.0 && probability < 1.0) {
        return None;
    }

    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239e0,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838e0,
        -2.549_732_539_343_734e0,
        4.374_664_141_464_968e0,
        2.938_163_982_698_783e0,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996e0,
        3.754_408_661_907_416e0,
    ];
    /// Where the central rational branch gives way to the tail one.
    const BREAK: f64 = 0.02425;

    let mut z = if probability < BREAK {
        let q = (-2.0 * probability.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if probability <= 1.0 - BREAK {
        let q = probability - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - probability).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };

    // One Halley step. The error function is evaluated rather than approximated, so this corrects
    // the rational fit against the integral it is fitting rather than against a second fit.
    let error = standard_normal_cumulative(z) - probability;
    let density = (-0.5 * z * z).exp() / (2.0 * std::f64::consts::PI).sqrt();
    if density > 0.0 {
        let step = error / density;
        z -= step / (1.0 + 0.5 * z * step);
    }
    Some(z)
}

/// The standard normal integral from negative infinity to `z`.
///
/// Written as a series rather than pulled from a crate: the only consumer is the Halley step above,
/// which needs one evaluation per threshold and no speed at all.
fn standard_normal_cumulative(z: f64) -> f64 {
    0.5 * erfc(-z / std::f64::consts::SQRT_2)
}

/// The complementary error function, by series below one and by continued fraction above it.
///
/// The two branches are separate functions rather than arms of an `if`, so each can be evaluated at
/// the crossover and checked against the published value there. Comparing the composed function on
/// either side of the crossover instead would compare two different arguments, and erfc moves by
/// more across that gap than the agreement being asserted.
fn erfc(x: f64) -> f64 {
    let magnitude = x.abs();
    let tail = if magnitude < 1.0 {
        erfc_by_series(magnitude)
    } else {
        erfc_by_continued_fraction(magnitude)
    };

    if x >= 0.0 {
        tail
    } else {
        2.0 - tail
    }
}

/// The Taylor series for erf, subtracted from one. For non-negative arguments only.
///
/// Used below one, where erf is far from one and the subtraction costs no significant figures; the
/// continued fraction converges too slowly to be worth it here.
fn erfc_by_series(x: f64) -> f64 {
    let mut term = x;
    let mut sum = x;
    for index in 1..200 {
        term *= -x * x / index as f64;
        let addition = term / (2.0 * index as f64 + 1.0);
        sum += addition;
        if addition.abs() < 1e-18 * sum.abs() {
            break;
        }
    }
    1.0 - 2.0 / std::f64::consts::PI.sqrt() * sum
}

/// Lentz's method on the continued fraction for the upper incomplete gamma at a half.
///
/// For non-negative arguments only, and the branch the far tail is read from — which is where the
/// multiple-testing threshold lives once a family holds more than a handful of tests.
fn erfc_by_continued_fraction(x: f64) -> f64 {
    let squared = x * x;
    let mut f = 1e-300;
    let mut c = f;
    let mut d = 0.0;
    for index in 0..300 {
        let (a, b) = if index == 0 {
            (1.0, squared + 0.5)
        } else {
            let step = index as f64;
            (-step * (step - 0.5), squared + 2.0 * step + 0.5)
        };
        d = b + a * d;
        if d.abs() < 1e-300 {
            d = 1e-300;
        }
        c = b + a / c;
        if c.abs() < 1e-300 {
            c = 1e-300;
        }
        d = 1.0 / d;
        let delta = c * d;
        f *= delta;
        if (delta - 1.0).abs() < 1e-17 {
            break;
        }
    }
    x * (-squared).exp() / std::f64::consts::PI.sqrt() * f
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::NonZeroI64;

    use chrono::{TimeZone, Utc};

    use crate::common::types::SessionDate;
    use crate::laboratory::cost::{FillStyle, RoundTrip};

    fn tests(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).expect("the fixture must declare at least one test")
    }

    fn fingerprint(screen: Option<(LiquidityFloor, ScreenWindow)>) -> DatasetFingerprint {
        DatasetFingerprint {
            session: SessionDate::at(Utc.with_ymd_and_hms(2026, 9, 17, 20, 0, 0).unwrap()),
            lookback_days: 730,
            liquidity_floor: screen.map(|(floor, _)| floor),
            screen_window: screen.map(|(_, window)| window),
            rows: 578_581,
            tickers: 1_253,
            first_timestamp: None,
            last_timestamp: None,
            splits_digest: 0,
            boundaries_digest: 0,
            reference_digest: None,
            factor_specification: None,
        }
    }

    fn declaration(quantity: Quantity) -> Declaration {
        Declaration::new(
            "does the sector factor explain anything",
            Family::new("residual-panel", tests(1)),
            Horizon::Sessions(tests(1)),
            DeclaredUniverse::Unscreened,
            quantity,
        )
    }

    fn unpriced() -> Quantity {
        Quantity::Unpriced { units: "share" }
    }

    /// One session a day, so every fixture arm reads the same sessions in the same order.
    const DAY: i64 = 86_400_000;

    /// Attaches sequential session keys to a list of readings.
    fn keyed(values: &[Option<f64>]) -> Vec<(SessionKey, Option<f64>)> {
        values
            .iter()
            .enumerate()
            .map(|(index, value)| (index as i64 * DAY, *value))
            .collect()
    }

    /// The same, from a slice of readings that were all measurable.
    fn measurable(values: &[f64]) -> Vec<(SessionKey, Option<f64>)> {
        keyed(&values.iter().copied().map(Some).collect::<Vec<_>>())
    }

    fn arm(name: &str, sessions: &[f64]) -> Arm {
        Arm::new(name, measurable(sessions), sessions.len())
            .expect("the fixture must be a usable arm")
    }

    /// An arm reading the same count of sessions as `arm` does, but a different set of them.
    fn shifted(name: &str, sessions: &[f64], by: i64) -> Arm {
        let readings: Vec<(SessionKey, Option<f64>)> = measurable(sessions)
            .into_iter()
            .map(|(session, value)| (session + by * DAY, value))
            .collect();
        Arm::new(name, readings, sessions.len()).expect("the fixture must be a usable arm")
    }

    fn study(pairing: Pairing, treatment: Arm, control: Arm) -> Study {
        Study::new(
            declaration(unpriced()),
            pairing,
            treatment,
            control,
            &fingerprint(None),
        )
        .expect("the fixture must assemble")
    }

    // --- the multiple-testing threshold ------------------------------------

    /// Pinned to the published two-sided normal quantiles, not to the function under test.
    #[test]
    fn test_one_test_at_five_percent_is_the_conventional_two_standard_errors() {
        let haircut = Haircut::new(tests(1), 0.05).expect("five percent is a probability");
        assert!(
            (haircut.required_standard_errors() - 1.959_963_984_540_054).abs() < 1e-10,
            "got {}",
            haircut.required_standard_errors()
        );
    }

    /// The plan's own worked example: forty cheap tests at the conventional threshold produce two
    /// false positives by construction, and this is the bar that stops them.
    #[test]
    fn test_forty_tests_demand_three_and_a_quarter_standard_errors() {
        let haircut = Haircut::new(tests(40), 0.05).expect("five percent is a probability");
        assert!(
            (haircut.required_standard_errors() - 3.227_218_425_963_163).abs() < 1e-9,
            "got {}",
            haircut.required_standard_errors()
        );
    }

    /// Five tests at 5% is the same bar as one test at 1%, which is what Bonferroni means.
    #[test]
    fn test_spending_the_rate_across_five_tests_matches_one_test_at_a_fifth_of_it() {
        let spread = Haircut::new(tests(5), 0.05).expect("a probability");
        let single = Haircut::new(tests(1), 0.01).expect("a probability");
        assert!(
            (spread.required_standard_errors() - single.required_standard_errors()).abs() < 1e-12,
            "{} against {}",
            spread.required_standard_errors(),
            single.required_standard_errors()
        );
        assert!((spread.required_standard_errors() - 2.575_829_303_548_9).abs() < 1e-9);
    }

    #[test]
    fn test_the_bar_rises_with_every_test_added_to_the_family() {
        let bars: Vec<f64> = [1usize, 2, 5, 10, 40]
            .into_iter()
            .map(|count| {
                Haircut::new(tests(count), 0.05)
                    .expect("a probability")
                    .required_standard_errors()
            })
            .collect();
        assert!(
            bars.windows(2).all(|pair| pair[1] > pair[0]),
            "the bar must rise: {bars:?}"
        );
    }

    /// The shipped rate has to be usable, or every study refuses.
    ///
    /// Asserted as a property rather than pinned to 0.05: the rate is methodology and is meant to
    /// be arguable, but a value outside the open unit interval stops the harness entirely and the
    /// refusal that would report it is otherwise unreachable.
    #[test]
    fn test_the_shipped_family_wise_error_rate_is_a_probability() {
        assert!(Haircut::new(tests(1), FAMILY_WISE_ERROR_RATE).is_some());
    }

    /// Zero demands infinitely many errors and one demands none; both decline to test.
    #[test]
    fn test_a_rate_that_is_not_a_probability_is_refused() {
        for rate in [0.0, 1.0, -0.1, 1.5, f64::NAN, f64::INFINITY] {
            assert!(
                Haircut::new(tests(3), rate).is_none(),
                "{rate} must be refused"
            );
        }
    }

    #[test]
    fn test_the_quantile_is_symmetric_about_a_half() {
        for probability in [0.001, 0.01, 0.2, 0.4, 0.49] {
            let low = inverse_standard_normal(probability).expect("inside the interval");
            let high = inverse_standard_normal(1.0 - probability).expect("inside the interval");
            assert!((low + high).abs() < 1e-11, "{low} against {high}");
        }
        assert!(inverse_standard_normal(0.5).expect("a half").abs() < 1e-12);
    }

    /// Deep in the tail is exactly where the threshold lives once a family holds many tests, and it
    /// is where a rational fit is weakest — so the published quantile is the pin.
    #[test]
    fn test_the_quantile_holds_in_the_far_tail() {
        let z = inverse_standard_normal(1.0 - 0.001 / 2.0).expect("inside the interval");
        assert!((z - 3.290_526_731_491_925).abs() < 1e-9, "got {z}");
    }

    #[test]
    fn test_the_quantile_is_refused_outside_the_open_unit_interval() {
        for probability in [0.0, 1.0, -0.5, 2.0, f64::NAN] {
            assert!(
                inverse_standard_normal(probability).is_none(),
                "{probability} must be refused"
            );
        }
    }

    /// The two expansions are joined at one, and a step there would move the threshold.
    ///
    /// Both are evaluated at the crossover itself and checked against the published `erfc(1)`.
    /// Comparing `erfc(1 - ε)` against `erfc(1 + ε)` would not do: erfc moves by 8e-13 across that
    /// gap, which is larger than the agreement worth asserting, so such a test can only pass by
    /// being loose enough to miss a real step.
    #[test]
    fn test_the_two_branches_of_erfc_agree_where_they_meet() {
        // erfc(1) to sixteen figures, computed independently in 40-digit decimal arithmetic.
        const ERFC_AT_ONE: f64 = 0.157_299_207_050_285_13;

        let series = erfc_by_series(1.0);
        let fraction = erfc_by_continued_fraction(1.0);

        assert!((series - ERFC_AT_ONE).abs() < 1e-16, "series {series}");
        assert!(
            (fraction - ERFC_AT_ONE).abs() < 1e-16,
            "fraction {fraction}"
        );
        assert!(
            (series - fraction).abs() < 1e-16,
            "{series} against {fraction}"
        );
    }

    /// Each branch must hold across a margin either side of the crossover, not merely at it.
    ///
    /// Only a margin: the series suffers catastrophic cancellation well before two — its largest
    /// intermediate term at three is about 3,200 against a sum of 0.886 — and the continued
    /// fraction converges slowly well before a half. Each is checked where it is used plus enough
    /// overlap that a crossover placed a little wrong would show.
    #[test]
    fn test_each_branch_of_erfc_holds_across_the_crossover() {
        // Reference values computed independently in 50-digit decimal arithmetic.
        for (argument, expected) in [
            (0.8_f64, 2.578_990_352_923_395_4e-1_f64),
            (1.0, 1.572_992_070_502_851_3e-1),
            (1.5, 3.389_485_352_468_927_4e-2),
        ] {
            let series = erfc_by_series(argument);
            let fraction = erfc_by_continued_fraction(argument);
            assert!(
                (series / expected - 1.0).abs() < 1e-13,
                "series at {argument}: {series} against {expected}"
            );
            assert!(
                (fraction / expected - 1.0).abs() < 1e-13,
                "fraction at {argument}: {fraction} against {expected}"
            );
        }
    }

    /// The composed function across the range the threshold is actually read over.
    #[test]
    fn test_erfc_matches_its_published_values() {
        for (argument, expected) in [
            (0.5_f64, 4.795_001_221_869_535e-1_f64),
            (1.0, 1.572_992_070_502_851_3e-1),
            (2.0, 4.677_734_981_047_266e-3),
            (3.0, 2.209_049_699_858_544e-5),
        ] {
            let measured = erfc(argument);
            assert!(
                (measured / expected - 1.0).abs() < 1e-14,
                "at {argument}: {measured} against {expected}"
            );
            // erfc(-x) = 2 - erfc(x), which is what carries the negative half of the normal integral.
            let mirrored = erfc(-argument);
            assert!(
                ((mirrored - (2.0 - expected)) / (2.0 - expected)).abs() < 1e-14,
                "at {}: {mirrored}",
                -argument
            );
        }
    }

    // --- what the declaration says -----------------------------------------

    /// Every variant renders, including the two no binary constructs yet.
    ///
    /// The intraday datasets apply no liquidity screen and count their horizon in bars, and the
    /// traded universe screens a trailing window rather than the whole frame — so both of those
    /// variants are what the next study to use this type needs. An unrendered variant is one whose
    /// wording nobody has read.
    #[test]
    fn test_every_part_of_a_declaration_renders() {
        let five = NonZeroUsize::new(5).expect("a positive count");
        assert_eq!(Horizon::Sessions(five).to_string(), "5 sessions");
        assert_eq!(
            Horizon::Bars {
                interval: BarInterval::FiveMinute,
                count: five,
            }
            .to_string(),
            "5 five_minute bars"
        );

        let thirty = NonZeroI64::new(30).expect("a positive window");
        assert_eq!(
            ScreenWindow::Trailing(thirty).to_string(),
            "trailing 30 days"
        );
        assert_eq!(ScreenWindow::WholeFrame.to_string(), "the whole frame");

        assert_eq!(DeclaredUniverse::Unscreened.to_string(), "unscreened");
        assert_eq!(
            DeclaredUniverse::Screened {
                name: "traded-v1".to_string(),
                floor: LiquidityFloor::new(10.0, 50_000_000.0).expect("a usable floor"),
                window: ScreenWindow::Trailing(thirty),
            }
            .to_string(),
            "traded-v1 ($10 close on $50000000 traded, over trailing 30 days)"
        );
    }

    /// A trailing-window declaration must also reach a study, not merely render.
    #[test]
    fn test_a_trailing_window_universe_assembles_against_its_own_floor() {
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).expect("a usable floor");
        let trailing =
            ScreenWindow::Trailing(NonZeroI64::new(30).expect("a positive trailing window"));
        let result = Study::new(
            Declaration::new(
                "does the traded book behave differently",
                Family::new("traded", tests(1)),
                Horizon::Sessions(tests(1)),
                DeclaredUniverse::Screened {
                    name: "traded-v1".to_string(),
                    floor,
                    window: trailing,
                },
                unpriced(),
            ),
            Pairing::Matched,
            arm("treatment", &[0.3, 0.1]),
            arm("control", &[0.1, 0.1]),
            &fingerprint(Some((floor, trailing))),
        )
        .expect("the declaration matches the screen the dataset was built under")
        .measure();

        assert!(render(&[result]).contains("trailing 30 days"));
    }

    /// The case a floor-only comparison cannot see: the right bounds over the wrong window.
    ///
    /// The whole point of carrying the window in the fingerprint, and the half of the universe
    /// check that was missing. A study screened over two years is not a study screened over the
    /// trailing month, whatever the two bounds say.
    #[test]
    fn test_the_right_floor_over_the_wrong_window_is_refused() {
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).expect("a usable floor");
        let trailing =
            ScreenWindow::Trailing(NonZeroI64::new(30).expect("a positive trailing window"));

        let refusal = Study::new(
            Declaration::new(
                "does the traded book behave differently",
                Family::new("traded", tests(1)),
                Horizon::Sessions(tests(1)),
                DeclaredUniverse::Screened {
                    name: "traded-v1".to_string(),
                    floor,
                    window: trailing,
                },
                unpriced(),
            ),
            Pairing::Matched,
            arm("treatment", &[0.3, 0.1]),
            arm("control", &[0.1, 0.1]),
            // The same floor, applied across every session the frame held.
            &fingerprint(Some((floor, ScreenWindow::WholeFrame))),
        )
        .expect_err("the same bounds over a different window are a different population");

        assert_eq!(
            refusal,
            StudyRefusal::UniverseDisagrees {
                declared: Some((floor, trailing)),
                measured: Some((floor, ScreenWindow::WholeFrame)),
            }
        );
        // The refusal has to say which window, or a reader sees two identical-looking floors.
        let rendered = refusal.to_string();
        assert!(rendered.contains("trailing 30 days"), "{rendered}");
        assert!(rendered.contains("the whole frame"), "{rendered}");
    }

    // --- the arms ----------------------------------------------------------

    #[test]
    fn test_an_arm_claiming_more_readings_than_rows_is_refused() {
        assert!(Arm::new("momentum", keyed(&[Some(0.1), Some(0.2)]), 1).is_none());
        assert!(Arm::new("momentum", keyed(&[Some(0.1), Some(0.2)]), 2).is_some());
        // A session that could not be read does not need a row behind it.
        assert!(Arm::new("momentum", keyed(&[Some(0.1), None]), 1).is_some());
    }

    #[test]
    fn test_an_unnamed_or_empty_arm_is_refused() {
        assert!(Arm::new("", keyed(&[Some(0.1)]), 1).is_none());
        assert!(Arm::new("   ", keyed(&[Some(0.1)]), 1).is_none());
        assert!(Arm::new("momentum", Vec::new(), 0).is_none());
        // One session read twice is one session counted twice.
        assert!(Arm::new("momentum", vec![(0, Some(0.1)), (0, Some(0.2))], 2).is_none());
    }

    // --- assembling a study ------------------------------------------------

    /// The invariant the whole type exists for: a study cannot claim a population it did not read.
    #[test]
    fn test_a_declared_universe_that_the_dataset_was_not_screened_by_is_refused() {
        let declaration = Declaration::new(
            "does momentum pay",
            Family::new("momentum", tests(1)),
            Horizon::Sessions(tests(20)),
            DeclaredUniverse::Screened {
                name: "liquid".to_string(),
                floor: LiquidityFloor::new(10.0, 50_000_000.0).expect("a usable floor"),
                window: ScreenWindow::Trailing(NonZeroI64::new(30).expect("a positive window")),
            },
            unpriced(),
        );

        let refusal = Study::new(
            declaration,
            Pairing::Matched,
            arm("momentum", &[0.1, 0.2]),
            arm("random", &[0.0, 0.0]),
            &fingerprint(None),
        )
        .expect_err("a screened declaration over an unscreened dataset must be refused");

        match refusal {
            StudyRefusal::UniverseDisagrees { declared, measured } => {
                assert!(declared.is_some());
                assert_eq!(measured, None);
            }
            other => panic!("the cause must name the universe, got {other:?}"),
        }
    }

    /// The reverse direction, which is the one that would otherwise pass silently: a study that
    /// says "unscreened" while reading a screened frame is quoting the traded book as the market.
    #[test]
    fn test_an_unscreened_declaration_over_a_screened_dataset_is_refused() {
        let refusal = Study::new(
            declaration(unpriced()),
            Pairing::Matched,
            arm("sector", &[0.1, 0.2]),
            arm("permuted", &[0.0, 0.0]),
            &fingerprint(Some((LiquidityFloor::CURRENT, ScreenWindow::WholeFrame))),
        )
        .expect_err("an unscreened declaration over a screened dataset must be refused");

        assert!(matches!(
            refusal,
            StudyRefusal::UniverseDisagrees { declared: None, .. }
        ));
    }

    #[test]
    fn test_matched_arms_reading_different_session_counts_are_refused() {
        let refusal = Study::new(
            declaration(unpriced()),
            Pairing::Matched,
            arm("sector", &[0.1, 0.2, 0.3]),
            arm("permuted", &[0.0, 0.0]),
            &fingerprint(None),
        )
        .expect_err("matched arms must align");

        assert_eq!(
            refusal,
            StudyRefusal::ArmsNotAligned {
                treatment: 3,
                control: 2
            }
        );
    }

    /// Disjoint arms are allowed to differ in length, which is the point of the variant.
    #[test]
    fn test_disjoint_arms_may_read_different_session_counts() {
        assert!(Study::new(
            declaration(unpriced()),
            Pairing::Disjoint,
            arm("first half", &[0.1, 0.2, 0.3]),
            // Sessions 3 and 4, where the first half read 0 through 2: disjoint in fact and not
            // only in the label.
            shifted("second half", &[0.0, 0.0], 3),
            &fingerprint(None),
        )
        .is_ok());
    }

    /// Equal lengths are not equal sessions, and this is the case the length check cannot see.
    ///
    /// `measure` zips the two arms, so an offset pair would subtract a session's treatment from a
    /// different session's control and report a difference with nothing wrong on its face. Both
    /// arms read three sessions; they are simply not the same three.
    #[test]
    fn test_matched_arms_reading_different_sessions_are_refused() {
        let refusal = Study::new(
            declaration(unpriced()),
            Pairing::Matched,
            arm("sector", &[0.1, 0.2, 0.3]),
            shifted("permuted", &[0.0, 0.0, 0.0], 1),
            &fingerprint(None),
        )
        .expect_err("matched arms must read the same sessions, not merely as many");

        assert_eq!(
            refusal,
            StudyRefusal::ArmsReadDifferentSessions {
                // The very first reading already disagrees, and the refusal says which sessions.
                index: 0,
                treatment: 0,
                control: DAY,
            }
        );
    }

    /// One session read twice is one session counted twice, whatever the other arm holds.
    #[test]
    fn test_an_arm_reading_one_session_twice_is_refused() {
        assert!(Arm::new("momentum", vec![(0, Some(0.1)), (DAY, Some(0.2))], 2).is_some());
        assert!(Arm::new("momentum", vec![(0, Some(0.1)), (0, Some(0.2))], 2).is_none());
    }

    /// `Disjoint` adds the arms' errors in quadrature, which is only right where they are
    /// independent. Two arms sharing sessions are not, so the claim is refused rather than priced.
    #[test]
    fn test_disjoint_arms_that_share_sessions_are_refused() {
        let refusal = Study::new(
            declaration(unpriced()),
            Pairing::Disjoint,
            arm("first half", &[0.1, 0.2, 0.3]),
            // Overlaps on two of the treatment's three sessions.
            shifted("second half", &[0.0, 0.0], 1),
            &fingerprint(None),
        )
        .expect_err("arms that share sessions are not disjoint");

        assert_eq!(refusal, StudyRefusal::ArmsOverlap { shared: 2 });
    }

    #[test]
    fn test_two_arms_under_one_name_are_not_a_comparison() {
        let refusal = Study::new(
            declaration(unpriced()),
            Pairing::Matched,
            arm("sector", &[0.1, 0.2]),
            arm("sector", &[0.0, 0.0]),
            &fingerprint(None),
        )
        .expect_err("one name is one arm");

        assert_eq!(
            refusal,
            StudyRefusal::ArmsNotDistinct {
                name: "sector".to_string()
            }
        );
    }

    // --- measuring ---------------------------------------------------------

    /// Matched differencing removes what both arms share, which is the whole reason to pair.
    ///
    /// Both arms swing by ±1.0 session to session and differ by exactly 0.1 throughout. Measured
    /// separately each has an enormous error; differenced per session the error is zero.
    #[test]
    fn test_a_matched_difference_removes_the_variation_both_arms_share() {
        let common = [1.0, -1.0, 1.0, -1.0, 1.0, -1.0];
        let treatment: Vec<f64> = common.iter().map(|value| value + 0.1).collect();

        let result = study(
            Pairing::Matched,
            arm("treatment", &treatment),
            arm("control", &common),
        )
        .measure();

        let difference = result
            .difference()
            .expect("the arms differ on every session");
        assert!((difference.mean - 0.1).abs() < 1e-12, "{difference:?}");
        assert!(difference.standard_error < 1e-12, "{difference:?}");
        assert_eq!(difference.sessions, 6);

        // Each arm on its own is indistinguishable from nothing, which is what pairing rescues.
        let treatment = result.treatment().expect("measurable");
        assert!(treatment.standard_error > 0.4, "{treatment:?}");
    }

    /// The same readings over *different* sessions: the errors add instead of cancelling, so the
    /// identical gap is no longer significant. The pairing is a claim about the data, not a style.
    ///
    /// The control is shifted six sessions clear of the treatment, because the same readings over
    /// the *same* sessions are not disjoint and `Study::new` now refuses to call them so — which is
    /// the whole reason the two arms cannot simply be relabelled from the matched test above.
    #[test]
    fn test_the_same_gap_read_over_separate_sessions_carries_both_arms_errors() {
        let common = [1.0, -1.0, 1.0, -1.0, 1.0, -1.0];
        let treatment: Vec<f64> = common.iter().map(|value| value + 0.1).collect();

        let result = study(
            Pairing::Disjoint,
            arm("treatment", &treatment),
            shifted("control", &common, 6),
        )
        .measure();

        let difference = result.difference().expect("both arms are measurable");
        assert!((difference.mean - 0.1).abs() < 1e-12, "{difference:?}");
        assert!(difference.standard_error > 0.6, "{difference:?}");
        // Both arms' sessions inform the gap, and neither alone does.
        assert_eq!(difference.sessions, 12);
        assert_eq!(result.clears_haircut(), Some(false));
    }

    /// A session either arm could not read is not a difference, and must not be folded in as zero.
    #[test]
    fn test_a_session_only_one_arm_read_is_skipped_rather_than_zeroed() {
        let treatment = Arm::new(
            "treatment",
            keyed(&[Some(0.3), None, Some(0.5), Some(0.4)]),
            4,
        )
        .expect("a usable arm");
        let control = Arm::new(
            "control",
            keyed(&[Some(0.1), Some(0.9), None, Some(0.2)]),
            4,
        )
        .expect("a usable arm");

        let result = study(Pairing::Matched, treatment, control).measure();

        let difference = result.difference().expect("two sessions are shared");
        assert_eq!(difference.sessions, 2);
        assert!((difference.mean - 0.2).abs() < 1e-12, "{difference:?}");
    }

    /// Matched arms read one row set, so the population is that set and not twice it.
    ///
    /// Caught on the first real run: the residual panel reported 435,382 rows folded when both arms
    /// had read the same 217,691. A doubled population makes a reading look twice as well supported
    /// as it is, and it is the one number a reader uses to judge that.
    #[test]
    fn test_matched_arms_are_not_counted_twice() {
        let treatment =
            Arm::new("treatment", measurable(&[0.3, 0.1, 0.2]), 217_691).expect("a usable arm");
        let control =
            Arm::new("control", measurable(&[0.1, 0.1, 0.1]), 217_691).expect("a usable arm");

        let matched = study(Pairing::Matched, treatment.clone(), control.clone()).measure();
        assert_eq!(matched.observations(), 217_691);

        // Disjoint arms read different rows, so there the total genuinely is the sum — and they
        // have to read different sessions too, which is what the shift supplies.
        let elsewhere = Arm::new(
            "control",
            shifted("control", &[0.1, 0.1, 0.1], 3).readings,
            217_691,
        )
        .expect("a usable arm");
        let disjoint = study(Pairing::Disjoint, treatment, elsewhere).measure();
        assert_eq!(disjoint.observations(), 435_382);
    }

    /// A reading pinned by the shape of the data has no number of standard errors, not an infinite
    /// one — and therefore has not cleared anything.
    #[test]
    fn test_a_difference_that_never_varied_has_no_standard_errors() {
        let result = study(
            Pairing::Matched,
            arm("treatment", &[0.5, 0.5, 0.5, 0.5]),
            arm("control", &[0.1, 0.1, 0.1, 0.1]),
        )
        .measure();

        let difference = result.difference().expect("measurable");
        assert!((difference.mean - 0.4).abs() < 1e-12);
        assert_eq!(difference.standard_error, 0.0);
        assert_eq!(result.standard_errors(), None);
        assert_eq!(result.clears_haircut(), None);
    }

    #[test]
    fn test_an_unmeasurable_study_has_not_failed_its_threshold() {
        let treatment = Arm::new("treatment", keyed(&[None, None]), 0).expect("a usable arm");
        let control = Arm::new("control", keyed(&[Some(0.1), Some(0.2)]), 2).expect("a usable arm");

        let result = study(Pairing::Matched, treatment, control).measure();

        assert_eq!(result.difference(), None);
        assert_eq!(result.standard_errors(), None);
        assert_eq!(result.clears_haircut(), None);
        assert_eq!(
            result.net_of_cost(),
            NetOfCost::NotAReturn { units: "share" }
        );
    }

    /// The reading that clears alone and fails once its family is counted. This is the entire
    /// argument for putting the count in the type.
    #[test]
    fn test_a_reading_that_clears_alone_fails_once_its_family_is_counted() {
        // Six sessions whose mean is about 2.6 standard errors from zero.
        let readings = [0.30, 0.10, 0.25, 0.15, 0.20, 0.20];
        let control = [0.0; 6];

        let measure = |count: usize| {
            Study::new(
                Declaration::new(
                    "does it pay",
                    Family::new("intraday", tests(count)),
                    Horizon::Sessions(tests(1)),
                    DeclaredUniverse::Unscreened,
                    unpriced(),
                ),
                Pairing::Matched,
                arm("treatment", &readings),
                arm("control", &control),
                &fingerprint(None),
            )
            .expect("the fixture must assemble")
            .measure()
        };

        let alone = measure(1);
        let errors = alone.standard_errors().expect("measurable");
        assert!((6.0..8.0).contains(&errors), "got {errors}");
        assert_eq!(alone.clears_haircut(), Some(true));

        // The identical reading, declared as one of forty. Nothing about the data changed.
        let counted = measure(40);
        assert_eq!(counted.standard_errors(), alone.standard_errors());
        assert_eq!(counted.clears_haircut(), Some(true));

        // And one where the count is what decides it: 2.22 errors clears the conventional two and
        // fails the moment a second test is declared beside it. Nothing about the data changed.
        let thin = [0.30, -0.10, 0.25, 0.05, 0.20, 0.10];
        let measure_thin = |count: usize| {
            Study::new(
                Declaration::new(
                    "does it pay",
                    Family::new("intraday", tests(count)),
                    Horizon::Sessions(tests(1)),
                    DeclaredUniverse::Unscreened,
                    unpriced(),
                ),
                Pairing::Matched,
                arm("treatment", &thin),
                arm("control", &control),
                &fingerprint(None),
            )
            .expect("the fixture must assemble")
            .measure()
        };
        let thin_errors = measure_thin(1).standard_errors().expect("measurable");
        assert!((2.0..2.4).contains(&thin_errors), "got {thin_errors}");
        assert_eq!(measure_thin(1).clears_haircut(), Some(true));
        assert_eq!(measure_thin(2).clears_haircut(), Some(false));
        assert_eq!(measure_thin(5).clears_haircut(), Some(false));
    }

    // --- cost --------------------------------------------------------------

    #[test]
    fn test_a_return_is_netted_of_the_declared_round_trip() {
        let priced = Quantity::ReturnPerRoundTrip {
            cost_model: CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR),
            quoted_spread: BasisPoints::new(10.0).expect("a usable reading"),
        };
        let result = Study::new(
            declaration(priced),
            Pairing::Matched,
            arm("treatment", &[30.0, 10.0, 25.0, 15.0]),
            arm("control", &[0.0, 0.0, 0.0, 0.0]),
            &fingerprint(None),
        )
        .expect("the fixture must assemble")
        .measure();

        match result.net_of_cost() {
            NetOfCost::Net {
                gross_basis_points,
                cost_basis_points,
                net_basis_points,
            } => {
                assert!((gross_basis_points - 20.0).abs() < 1e-12);
                // A pair round trip at a 10bp quoted spread, per the cost model's own arithmetic.
                assert!((cost_basis_points - 20.0).abs() < 1e-12);
                assert!(net_basis_points.abs() < 1e-12);
            }
            other => panic!("an aggressive fill is costable, got {other:?}"),
        }
    }

    /// A gross of zero after cost is what a real refutation looks like, and it must not read as a
    /// study that could not be costed.
    #[test]
    fn test_a_style_the_archive_cannot_cost_is_refused_rather_than_charged_nothing() {
        let priced = Quantity::ReturnPerRoundTrip {
            cost_model: CostModel::new(FillStyle::Passive, RoundTrip::PAIR),
            quoted_spread: BasisPoints::new(10.0).expect("a usable reading"),
        };
        let result = Study::new(
            declaration(priced),
            Pairing::Matched,
            arm("treatment", &[30.0, 10.0, 25.0, 15.0]),
            arm("control", &[0.0, 0.0, 0.0, 0.0]),
            &fingerprint(None),
        )
        .expect("the fixture must assemble")
        .measure();

        assert!(matches!(result.net_of_cost(), NetOfCost::Refused(_)));
    }

    /// A variance share has no round trip behind it, so charging one would be inventing a trade.
    #[test]
    fn test_an_unpriced_reading_is_not_charged_a_spread() {
        let result = study(
            Pairing::Matched,
            arm("treatment", &[0.3, 0.1]),
            arm("control", &[0.1, 0.1]),
        )
        .measure();

        assert_eq!(
            result.net_of_cost(),
            NetOfCost::NotAReturn { units: "share" }
        );
    }

    // --- rendering ---------------------------------------------------------

    #[test]
    fn test_the_header_states_the_threshold_and_the_count_that_set_it() {
        let result = Study::new(
            Declaration::new(
                "does it pay",
                Family::new("intraday", tests(40)),
                Horizon::Sessions(tests(1)),
                DeclaredUniverse::Unscreened,
                unpriced(),
            ),
            Pairing::Matched,
            arm("treatment", &[0.3, 0.1, 0.2]),
            arm("control", &[0.1, 0.1, 0.1]),
            &fingerprint(None),
        )
        .expect("the fixture must assemble")
        .measure();

        let rendered = render(&[result]);
        assert!(
            rendered.contains("3.23 standard errors required"),
            "{rendered}"
        );
        assert!(rendered.contains("Bonferroni over 40 tests"), "{rendered}");
        assert!(rendered.contains("unscreened"), "{rendered}");
    }

    /// The cell shows what is left after the cost, not what was there before it.
    ///
    /// A gross that pays and a net that does not is the most common shape a refutation takes here,
    /// so the column must not be readable as the gross with a cost printed beside it.
    #[test]
    fn test_the_cost_column_shows_the_net_rather_than_the_gross() {
        let priced = Quantity::ReturnPerRoundTrip {
            cost_model: CostModel::new(FillStyle::Aggressive, RoundTrip::PAIR),
            quoted_spread: BasisPoints::new(10.0).expect("a usable reading"),
        };
        let result = Study::new(
            declaration(priced),
            Pairing::Matched,
            arm("treatment", &[10.0, 6.0, 8.0, 4.0]),
            arm("control", &[0.0, 0.0, 0.0, 0.0]),
            &fingerprint(None),
        )
        .expect("the fixture must assemble")
        .measure();

        // Gross is +7.00bp and the pair round trip charges 20.00bp, so the book loses 13.00bp.
        assert_eq!(
            result.difference().expect("measurable").mean.round(),
            7.0_f64
        );
        let rendered = render(&[result]);
        assert!(rendered.contains("-13.00 after 20.00bp"), "{rendered}");
        assert!(!rendered.contains("+7.00 after"), "{rendered}");
    }

    /// The sign is what a reader acts on, and a negative difference must not render as a stray
    /// minus in a column of positives.
    #[test]
    fn test_a_rendered_statistic_carries_its_sign_and_its_error() {
        assert_eq!(
            distribution(Some(Distribution {
                mean: -0.013489,
                standard_error: 0.007973,
                sessions: 502,
            })),
            "-0.013489 ± 0.007973 (502)"
        );
        assert_eq!(distribution(None), "unmeasurable");
    }

    /// An unmeasurable reading rendered as zero would read as a measurement that found nothing.
    #[test]
    fn test_an_unmeasurable_reading_is_not_rendered_as_zero() {
        let treatment = Arm::new("treatment", keyed(&[None, None]), 0).expect("a usable arm");
        let control = Arm::new("control", keyed(&[Some(0.1), Some(0.2)]), 2).expect("a usable arm");
        let rendered = render(&[study(Pairing::Matched, treatment, control).measure()]);

        assert!(rendered.contains("unmeasurable"), "{rendered}");
        assert!(!rendered.contains("0.000000"), "{rendered}");
    }

    #[test]
    fn test_a_long_question_does_not_shift_the_columns_beneath_it() {
        // Both in one family, so they land in one table and their rows can be compared at all.
        let asked = |question: &str, treatment: &str| {
            Study::new(
                Declaration::new(
                    question,
                    Family::new("intraday", tests(2)),
                    Horizon::Sessions(tests(1)),
                    DeclaredUniverse::Unscreened,
                    unpriced(),
                ),
                Pairing::Matched,
                arm(treatment, &[0.3, 0.1, 0.2]),
                arm("control", &[0.1, 0.1, 0.1]),
                &fingerprint(None),
            )
            .expect("the fixture must assemble")
            .measure()
        };
        let long = asked(
            "a question far longer than the column it has been given to sit in",
            "treatment",
        );
        let short = asked("short", "second");

        let rendered = render(&[long, short]);
        // Columns, not bytes: `±` is two bytes and the ellipsis is three, so a byte count would
        // report a misalignment on rows that line up perfectly in a terminal.
        let widths: Vec<usize> = rendered
            .lines()
            .filter(|line| line.starts_with("question") || line.contains(" ± "))
            .map(|line| line.chars().count())
            .collect();
        assert_eq!(widths.len(), 3, "a header and two rows: {rendered}");
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "rows must align: {widths:?}\n{rendered}"
        );
    }

    #[test]
    fn test_no_studies_renders_as_no_studies_rather_than_an_empty_table() {
        assert_eq!(render(&[]), "No studies were measured.\n");
    }

    /// Studies from two families get two headers, because one header cannot describe both.
    ///
    /// The defect this closes: the header states a family, a threshold, a universe and a horizon,
    /// and taking them from the first row filed every later reading under a bar that was not its
    /// own. A reading needing 3.23 errors rendered under a 1.96 header reads as comfortably clear.
    #[test]
    fn test_studies_from_two_families_are_not_rendered_under_one_header() {
        let measured = |family: &str, count: usize| {
            Study::new(
                Declaration::new(
                    format!("does {family} pay"),
                    Family::new(family, tests(count)),
                    Horizon::Sessions(tests(1)),
                    DeclaredUniverse::Unscreened,
                    unpriced(),
                ),
                Pairing::Matched,
                arm("treatment", &[0.30, 0.10, 0.25, 0.15, 0.20, 0.20]),
                arm("control", &[0.0; 6]),
                &fingerprint(None),
            )
            .expect("the fixture must assemble")
            .measure()
        };

        let rendered = render(&[measured("alone", 1), measured("crowded", 40)]);

        assert_eq!(rendered.matches("Family ").count(), 2, "{rendered}");
        assert!(rendered.contains("Family alone"), "{rendered}");
        assert!(rendered.contains("Family crowded"), "{rendered}");
        // Each family's own bar, not the first one's applied to both.
        assert!(
            rendered.contains("1.96 standard errors required"),
            "{rendered}"
        );
        assert!(
            rendered.contains("3.23 standard errors required"),
            "{rendered}"
        );
        assert!(rendered.contains("Bonferroni over 1 tests"), "{rendered}");
        assert!(rendered.contains("Bonferroni over 40 tests"), "{rendered}");
    }

    /// Two universes are two populations, so they do not share a header either.
    #[test]
    fn test_studies_over_two_universes_are_not_rendered_under_one_header() {
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).expect("a usable floor");
        let screened = Study::new(
            Declaration::new(
                "does the traded book pay",
                Family::new("one", tests(1)),
                Horizon::Sessions(tests(1)),
                DeclaredUniverse::Screened {
                    name: "traded-v1".to_string(),
                    floor,
                    window: ScreenWindow::WholeFrame,
                },
                unpriced(),
            ),
            Pairing::Matched,
            arm("treatment", &[0.3, 0.1]),
            arm("control", &[0.1, 0.1]),
            &fingerprint(Some((floor, ScreenWindow::WholeFrame))),
        )
        .expect("the fixture must assemble")
        .measure();
        let unscreened = study(
            Pairing::Matched,
            arm("treatment", &[0.3, 0.1]),
            arm("control", &[0.1, 0.1]),
        )
        .measure();

        let rendered = render(&[screened, unscreened]);

        assert_eq!(rendered.matches("Family ").count(), 2, "{rendered}");
        assert!(rendered.contains("universe traded-v1"), "{rendered}");
        assert!(rendered.contains("universe unscreened"), "{rendered}");
    }

    /// A homogeneous slice renders exactly one table, which is what both binaries pass.
    #[test]
    fn test_one_family_still_renders_as_one_table() {
        let rendered = render(&[
            study(
                Pairing::Matched,
                arm("treatment", &[0.3, 0.1, 0.2]),
                arm("control", &[0.1, 0.1, 0.1]),
            )
            .measure(),
            study(
                Pairing::Matched,
                arm("second", &[0.4, 0.2, 0.3]),
                arm("control", &[0.1, 0.1, 0.1]),
            )
            .measure(),
        ]);

        assert_eq!(rendered.matches("Family ").count(), 1, "{rendered}");
        assert_eq!(rendered.matches("question ").count(), 1, "{rendered}");
    }

    /// The footer counts rows per study, and two studies in a family need not share a population.
    ///
    /// Quoting the first row's count for every row is what the footer used to do, and it overstates
    /// how well the rest of the table is supported.
    #[test]
    fn test_the_footer_reports_the_range_when_the_studies_read_different_row_counts() {
        let build = |name: &str, observations: usize| {
            Study::new(
                declaration(unpriced()),
                Pairing::Matched,
                Arm::new(name, measurable(&[0.3, 0.1, 0.2]), observations).expect("a usable arm"),
                Arm::new("control", measurable(&[0.1, 0.1, 0.1]), observations)
                    .expect("a usable arm"),
                &fingerprint(None),
            )
            .expect("the fixture must assemble")
            .measure()
        };

        let same = render(&[build("first", 500), build("second", 500)]);
        assert!(
            same.contains("rows folded into these readings: 500"),
            "{same}"
        );

        let differing = render(&[build("first", 500), build("second", 1_200)]);
        assert!(
            differing.contains("rows folded into these readings: 500 to 1200"),
            "{differing}"
        );
    }
}
