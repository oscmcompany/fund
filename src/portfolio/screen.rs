//! Pair selection: which two symbols, which way round, and how strong the signal.
//!
//! Quadratic in the supplied universe, so the cheap tests come first.

use std::collections::{HashMap, HashSet};

use tracing::debug;

use crate::common::journal::ExclusionReason;
use crate::common::types::{PairID, Ticker};

/// Sessions of daily closes the correlation and the spread distribution are fitted over.
pub const CORRELATION_WINDOW_SESSIONS: usize = 60;

/// Correlation band a pair's log returns must fall inside.
///
/// The band is on signed correlation, not magnitude: an anti-correlated pair fits a negative hedge
/// ratio, turning `ln(short) - hedge_ratio * ln(long)` into a sum that hedges nothing while sizing
/// stays dollar-neutral. Read it through [`admits_correlation`] rather than spelling it out.
pub const CORRELATION_MINIMUM: f64 = 0.5;
pub const CORRELATION_MAXIMUM: f64 = 0.95;

/// Spread z-score at which a pair is worth opening.
pub const ENTRY_Z_SCORE: f64 = 2.0;

/// Spread z-score at which an open pair has converged and is closed at a profit.
///
/// Zero, not a band around zero: the spread is entered above [`ENTRY_Z_SCORE`] and closed when it
/// crosses back through its own mean, which is the move the position was taken to capture.
pub const CONVERGENCE_Z_SCORE: f64 = 0.0;

/// How much further a spread must widen beyond its own entry before the pair is stopped out.
///
/// Expressed in z units, so it is already normalized per pair: one unit is one standard deviation
/// of *that* pair's own spread. Read it through [`stop_at`] rather than adding it at a call site.
pub const STOP_LOSS_WIDENING: f64 = 1.5;

/// Upper bound on the entry z-score.
///
/// A data-quality guard, not a strategy rule: a spread this far out is more often an unadjusted
/// corporate action or a regime break than an opportunity, and neither reverts.
pub const ENTRY_Z_SCORE_CAP: f64 = 5.0;

/// Legs the book may hold in one sector at once.
///
/// A property of the selection across the book rather than of any candidate: a reservoir of
/// same-sector spreads ranks by the same industry factor at the top, so ten selected pairs can be
/// one bet held ten times. Counted in legs so held and candidate positions measure the same way.
pub const MAXIMUM_LEGS_PER_SECTOR: usize = 6;

/// Tickers needed before a screen can produce anything.
const MINIMUM_ELIGIBLE_TICKERS: usize = 2;

/// Observations needed before a spread distribution can be fitted at all.
///
/// Two, because the sample standard deviation removes one degree of freedom. Separate from
/// [`MINIMUM_ELIGIBLE_TICKERS`] despite sharing its value: the two are unrelated quantities.
const MINIMUM_SPREAD_OBSERVATIONS: usize = 2;

/// Largest single-session log return a fit window may contain and still be screened.
///
/// A window holding a larger move is not one distribution, so the hedge ratio fitted across it does
/// not hedge. The cause is deliberately not consulted: a split artifact and a real collapse damage
/// the fit identically, and only one of them is fixable by re-fetching.
pub const MAXIMUM_SESSION_LOGARITHMIC_RETURN: f64 = 0.40;

/// Whether `z_score` is inside the entry admission band `[ENTRY_Z_SCORE, ENTRY_Z_SCORE_CAP]`.
///
/// The single expression of the band. A caller spelling the comparison out itself is free to differ
/// on a boundary, and a study that differs from the screen is a study of another strategy.
pub fn admits_entry_z_score(z_score: f64) -> bool {
    (ENTRY_Z_SCORE..=ENTRY_Z_SCORE_CAP).contains(&z_score)
}

/// Whether `correlation` is inside `[CORRELATION_MINIMUM, CORRELATION_MAXIMUM]`.
///
/// The single expression of the band, on the same terms as [`admits_entry_z_score`]. Signed, so an
/// anti-correlated pair is refused rather than mirrored.
pub fn admits_correlation(correlation: f64) -> bool {
    (CORRELATION_MINIMUM..=CORRELATION_MAXIMUM).contains(&correlation)
}

/// The z-score at which a pair entered at `entry_z_score` is stopped out.
///
/// The single expression of the stop. It is relative because an absolute line silently forbids
/// entries above itself, closing a pair on the same reading that opened it.
pub fn stop_at(entry_z_score: f64) -> f64 {
    entry_z_score + STOP_LOSS_WIDENING
}

/// Why a symbol could not be screened.
///
/// Carried out of [`ScreenInput::new`] rather than collapsed into `None`, so the journal can say
/// which test a ticker failed and — where the test is a number — by how much.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScreenRejection {
    /// Too short a history, or a close, live price, or prediction that is not a usable number.
    UnusableInput,
    /// One session's move is large enough to dominate the window it sits in.
    StructuralBreak { logarithmic_return: f64, limit: f64 },
}

impl ScreenRejection {
    /// The stable name this rejection is recorded under.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScreenRejection::UnusableInput => "unusable_input",
            ScreenRejection::StructuralBreak { .. } => "structural_break",
        }
    }

    /// Which funnel exit this rejection is, for the journal.
    pub fn exclusion_reason(&self) -> ExclusionReason {
        match self {
            ScreenRejection::UnusableInput => ExclusionReason::UnusableInput,
            ScreenRejection::StructuralBreak { .. } => ExclusionReason::StructuralBreak,
        }
    }

    /// The numbers behind the rejection, for the ones that have any.
    ///
    /// A bound can only be moved from readings it refused, so the reading is recorded and not just
    /// the verdict. `UnusableInput` has nothing to report beyond its own name.
    pub fn detail(&self) -> Option<String> {
        match self {
            ScreenRejection::UnusableInput => None,
            ScreenRejection::StructuralBreak {
                logarithmic_return,
                limit,
            } => Some(format!(
                "logarithmic_return={logarithmic_return:.4} limit={limit:.4}"
            )),
        }
    }
}

/// One symbol's inputs to the screen.
///
/// `closes` must be aligned across every input in a batch — position `i` the same session in each
/// — which is what [`crate::data::bars::load_aligned_closes`] guarantees.
#[derive(Debug, Clone)]
pub struct ScreenInput {
    ticker: Ticker,
    closes: Vec<f64>,
    price: f64,
    expected_return: f64,
    confidence: f64,
    is_shortable: bool,
}

impl ScreenInput {
    /// Builds an input, rejecting one that cannot be screened.
    ///
    /// A non-positive close or live price is rejected rather than skipped later: the spread is
    /// built from logarithms, and `ln` of a non-positive price is `NaN` or `-inf`, which propagates
    /// through the correlation and the regression into a candidate that looks like any other.
    pub fn new(
        ticker: Ticker,
        closes: Vec<f64>,
        price: f64,
        expected_return: f64,
        confidence: f64,
        is_shortable: bool,
    ) -> Result<Self, ScreenRejection> {
        if closes.len() < CORRELATION_WINDOW_SESSIONS {
            return Err(ScreenRejection::UnusableInput);
        }
        if !price.is_finite() || price <= 0.0 {
            return Err(ScreenRejection::UnusableInput);
        }
        if closes
            .iter()
            .any(|close| !close.is_finite() || *close <= 0.0)
        {
            return Err(ScreenRejection::UnusableInput);
        }
        if !expected_return.is_finite() || !confidence.is_finite() {
            return Err(ScreenRejection::UnusableInput);
        }
        // The fitted window and not the whole series: a break older than the window cannot reach
        // the spread model, so excluding on it would refuse a name whose history has since settled.
        let window = &closes[closes.len() - CORRELATION_WINDOW_SESSIONS..];
        match worst_session_move(window) {
            Some(logarithmic_return)
                if logarithmic_return.abs() > MAXIMUM_SESSION_LOGARITHMIC_RETURN =>
            {
                return Err(ScreenRejection::StructuralBreak {
                    logarithmic_return,
                    limit: MAXIMUM_SESSION_LOGARITHMIC_RETURN,
                });
            }
            Some(_) | None => {}
        }
        Ok(Self {
            ticker,
            closes,
            price,
            expected_return,
            confidence,
            is_shortable,
        })
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn price(&self) -> f64 {
        self.price
    }

    pub fn expected_return(&self) -> f64 {
        self.expected_return
    }

    pub fn confidence(&self) -> f64 {
        self.confidence
    }

    pub fn is_shortable(&self) -> bool {
        self.is_shortable
    }

    /// The trailing window, oldest first.
    fn window(&self) -> &[f64] {
        &self.closes[self.closes.len() - CORRELATION_WINDOW_SESSIONS..]
    }
}

/// Why a spread distribution could not be fitted.
///
/// Carried out of the fit rather than collapsed into `None`, so an open pair held without a signal
/// says which of the six causes it was and — where the cause is a number — what that number was.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SpreadFitFailure {
    /// The two close series do not describe the same sessions.
    MisalignedSeries {
        long_sessions: usize,
        short_sessions: usize,
    },
    /// Fewer observations than a distribution can be drawn from.
    WindowTooShort { sessions: usize, minimum: usize },
    /// A close with no logarithm: non-positive, or not a number.
    UnusableClose,
    /// The long leg's log price does not move, so no slope exists to hedge with.
    HedgeRatioUnfittable,
    /// The hedge ratio handed in is not a usable number.
    HedgeRatioUnusable { hedge_ratio: f64 },
    /// The spread does not move, which makes every z-score infinite.
    NoDispersion { standard_deviation: f64 },
}

impl SpreadFitFailure {
    /// The stable name this failure is recorded under.
    pub fn as_str(&self) -> &'static str {
        match self {
            SpreadFitFailure::MisalignedSeries { .. } => "misaligned_series",
            SpreadFitFailure::WindowTooShort { .. } => "window_too_short",
            SpreadFitFailure::UnusableClose => "unusable_close",
            SpreadFitFailure::HedgeRatioUnfittable => "hedge_ratio_unfittable",
            SpreadFitFailure::HedgeRatioUnusable { .. } => "hedge_ratio_unusable",
            SpreadFitFailure::NoDispersion { .. } => "no_dispersion",
        }
    }

    /// The numbers behind the failure, for the ones that have any.
    pub fn detail(&self) -> Option<String> {
        match self {
            SpreadFitFailure::MisalignedSeries {
                long_sessions,
                short_sessions,
            } => Some(format!(
                "long_sessions={long_sessions} short_sessions={short_sessions}"
            )),
            SpreadFitFailure::WindowTooShort { sessions, minimum } => {
                Some(format!("sessions={sessions} minimum={minimum}"))
            }
            SpreadFitFailure::UnusableClose | SpreadFitFailure::HedgeRatioUnfittable => None,
            SpreadFitFailure::HedgeRatioUnusable { hedge_ratio } => {
                Some(format!("hedge_ratio={hedge_ratio}"))
            }
            SpreadFitFailure::NoDispersion { standard_deviation } => {
                Some(format!("standard_deviation={standard_deviation:e}"))
            }
        }
    }
}

/// The log-price spread of an oriented pair, and the distribution it is measured against.
///
/// The spread is `ln(short) - hedge_ratio * ln(long)`, with `hedge_ratio` the ordinary least
/// squares slope of the short leg's log price on the long leg's. A pair is opened only when the
/// short leg is the expensive one, so an entry z-score is always positive and no call site
/// downstream has to reason about which sign means what.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpreadModel {
    hedge_ratio: f64,
    mean: f64,
    standard_deviation: f64,
}

impl SpreadModel {
    /// Fits both the hedge ratio and the distribution from aligned close histories.
    pub fn fit(long_closes: &[f64], short_closes: &[f64]) -> Result<Self, SpreadFitFailure> {
        let (long_logs, short_logs) = aligned_logs(long_closes, short_closes)?;
        let hedge_ratio = ordinary_least_squares_slope(&long_logs, &short_logs)
            .ok_or(SpreadFitFailure::HedgeRatioUnfittable)?;
        Self::build(hedge_ratio, &long_logs, &short_logs)
    }

    /// Rebuilds the distribution around a hedge ratio that was already decided.
    ///
    /// This is the exit path. Refitting here would measure a different spread from the one the
    /// entry was taken on, and a series shorter than the window would draw its mean and deviation
    /// from a different sample, crossing a threshold for a spread that has not moved.
    pub fn with_hedge_ratio(
        hedge_ratio: f64,
        long_closes: &[f64],
        short_closes: &[f64],
    ) -> Result<Self, SpreadFitFailure> {
        if !hedge_ratio.is_finite() {
            return Err(SpreadFitFailure::HedgeRatioUnusable { hedge_ratio });
        }
        for sessions in [long_closes.len(), short_closes.len()] {
            if sessions < CORRELATION_WINDOW_SESSIONS {
                return Err(SpreadFitFailure::WindowTooShort {
                    sessions,
                    minimum: CORRELATION_WINDOW_SESSIONS,
                });
            }
        }
        let long_window = &long_closes[long_closes.len() - CORRELATION_WINDOW_SESSIONS..];
        let short_window = &short_closes[short_closes.len() - CORRELATION_WINDOW_SESSIONS..];

        let (long_logs, short_logs) = aligned_logs(long_window, short_window)?;
        Self::build(hedge_ratio, &long_logs, &short_logs)
    }

    fn build(
        hedge_ratio: f64,
        long_logs: &[f64],
        short_logs: &[f64],
    ) -> Result<Self, SpreadFitFailure> {
        if !hedge_ratio.is_finite() {
            return Err(SpreadFitFailure::HedgeRatioUnusable { hedge_ratio });
        }
        let spread: Vec<f64> = short_logs
            .iter()
            .zip(long_logs.iter())
            .map(|(short, long)| short - hedge_ratio * long)
            .collect();

        // `aligned_logs` already refused a short or non-finite series, so anything unreadable here
        // is a spread with no dispersion by another name.
        let mean = mean(&spread).ok_or(SpreadFitFailure::NoDispersion {
            standard_deviation: f64::NAN,
        })?;
        let standard_deviation =
            standard_deviation(&spread, mean).ok_or(SpreadFitFailure::NoDispersion {
                standard_deviation: f64::NAN,
            })?;
        if standard_deviation <= f64::EPSILON {
            return Err(SpreadFitFailure::NoDispersion { standard_deviation });
        }
        Ok(Self {
            hedge_ratio,
            mean,
            standard_deviation,
        })
    }

    pub fn hedge_ratio(&self) -> f64 {
        self.hedge_ratio
    }

    /// The fitted spread mean and dispersion, for the exit path's diagnostic log.
    ///
    /// Exposed because a z-score alone cannot be checked against anything: two runs reporting
    /// different z for one pair are indistinguishable from a price move without these.
    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn standard_deviation(&self) -> f64 {
        self.standard_deviation
    }

    /// Standardizes a live observation of the spread against the fitted distribution.
    ///
    /// The observation is deliberately not part of the distribution it is measured against — the
    /// window is closed daily bars, the observation is intraday. A z-score taken against a
    /// distribution containing the scored point is bounded by the sample size.
    pub fn z_score(&self, long_price: f64, short_price: f64) -> Option<f64> {
        if !long_price.is_finite() || long_price <= 0.0 {
            return None;
        }
        if !short_price.is_finite() || short_price <= 0.0 {
            return None;
        }
        let spread = short_price.ln() - self.hedge_ratio * long_price.ln();
        let z_score = (spread - self.mean) / self.standard_deviation;
        z_score.is_finite().then_some(z_score)
    }
}

/// Why a candidate could not describe a position worth taking.
///
/// Carried out of [`PairCandidate::new`] rather than collapsed into `None`, so the five refusals
/// stay five facts and each reports the number that produced it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CandidateRejection {
    HedgeRatioUnusable {
        hedge_ratio: f64,
    },
    /// The legs are oriented the wrong way round: the short leg is the cheap one.
    EntryZScoreNotPositive {
        entry_z_score: f64,
    },
    /// The model expects the short leg to out-return the long, contradicting the orientation.
    SignalStrengthNotPositive {
        signal_strength: f64,
    },
    LongPriceUnusable {
        long_price: f64,
    },
    ShortPriceUnusable {
        short_price: f64,
    },
}

impl CandidateRejection {
    /// The stable name this rejection is recorded under.
    pub fn as_str(&self) -> &'static str {
        match self {
            CandidateRejection::HedgeRatioUnusable { .. } => "hedge_ratio_unusable",
            CandidateRejection::EntryZScoreNotPositive { .. } => "entry_z_score_not_positive",
            CandidateRejection::SignalStrengthNotPositive { .. } => "signal_strength_not_positive",
            CandidateRejection::LongPriceUnusable { .. } => "long_price_unusable",
            CandidateRejection::ShortPriceUnusable { .. } => "short_price_unusable",
        }
    }

    /// The number behind the rejection. Every variant has one.
    pub fn detail(&self) -> String {
        match self {
            CandidateRejection::HedgeRatioUnusable { hedge_ratio } => {
                format!("hedge_ratio={hedge_ratio}")
            }
            CandidateRejection::EntryZScoreNotPositive { entry_z_score } => {
                format!("entry_z_score={entry_z_score}")
            }
            CandidateRejection::SignalStrengthNotPositive { signal_strength } => {
                format!("signal_strength={signal_strength}")
            }
            CandidateRejection::LongPriceUnusable { long_price } => {
                format!("long_price={long_price}")
            }
            CandidateRejection::ShortPriceUnusable { short_price } => {
                format!("short_price={short_price}")
            }
        }
    }
}

/// A pair worth opening, oriented and scored.
#[derive(Debug, Clone, PartialEq)]
pub struct PairCandidate {
    pair_id: PairID,
    hedge_ratio: f64,
    entry_z_score: f64,
    signal_strength: f64,
    long_price: f64,
    short_price: f64,
}

impl PairCandidate {
    /// Constructs a candidate, rejecting one that cannot describe a position worth taking.
    ///
    /// The two positivity requirements are the module's invariants made enforceable rather than
    /// merely documented: a non-positive `entry_z_score` means the legs were oriented the wrong way
    /// round, and a non-positive `signal_strength` means the model contradicts that orientation.
    pub fn new(
        pair_id: PairID,
        hedge_ratio: f64,
        entry_z_score: f64,
        signal_strength: f64,
        long_price: f64,
        short_price: f64,
    ) -> Result<Self, CandidateRejection> {
        if !hedge_ratio.is_finite() {
            return Err(CandidateRejection::HedgeRatioUnusable { hedge_ratio });
        }
        if !entry_z_score.is_finite() || entry_z_score <= 0.0 {
            return Err(CandidateRejection::EntryZScoreNotPositive { entry_z_score });
        }
        if !signal_strength.is_finite() || signal_strength <= 0.0 {
            return Err(CandidateRejection::SignalStrengthNotPositive { signal_strength });
        }
        if !long_price.is_finite() || long_price <= 0.0 {
            return Err(CandidateRejection::LongPriceUnusable { long_price });
        }
        if !short_price.is_finite() || short_price <= 0.0 {
            return Err(CandidateRejection::ShortPriceUnusable { short_price });
        }
        Ok(Self {
            pair_id,
            hedge_ratio,
            entry_z_score,
            signal_strength,
            long_price,
            short_price,
        })
    }

    pub fn pair_id(&self) -> &PairID {
        &self.pair_id
    }

    pub fn long_ticker(&self) -> &Ticker {
        self.pair_id.long()
    }

    pub fn short_ticker(&self) -> &Ticker {
        self.pair_id.short()
    }

    pub fn hedge_ratio(&self) -> f64 {
        self.hedge_ratio
    }

    pub fn entry_z_score(&self) -> f64 {
        self.entry_z_score
    }

    /// How much more the model expects the long leg to return than the short leg.
    ///
    /// Positive by construction: a candidate whose model disagrees with the spread's orientation is
    /// not produced at all.
    pub fn signal_strength(&self) -> f64 {
        self.signal_strength
    }

    pub fn long_price(&self) -> f64 {
        self.long_price
    }

    pub fn short_price(&self) -> f64 {
        self.short_price
    }

    /// Rank score: how stretched the spread is, scaled by how strongly the model agrees.
    pub fn rank_score(&self) -> f64 {
        self.entry_z_score * self.signal_strength
    }
}

/// Why one orientation of a pair did not become a candidate.
///
/// Every exit from [`orient_one`] is a variant here, so a pair that produced nothing says which
/// test it failed rather than only that it failed one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrientationRejection {
    /// The leg that would be sold cannot be borrowed.
    ShortLegNotShortable,
    SpreadUnfitted(SpreadFitFailure),
    /// The live prices do not standardize against the fitted distribution.
    SpreadUnreadable,
    EntryBelowThreshold {
        z_score: f64,
        threshold: f64,
    },
    EntryBeyondCap {
        z_score: f64,
        cap: f64,
    },
    /// The model expects the short leg to out-return the long.
    ModelDisagrees {
        signal_strength: f64,
    },
    Candidate(CandidateRejection),
}

impl OrientationRejection {
    /// The stable name this rejection is counted under.
    pub fn as_str(&self) -> &'static str {
        match self {
            OrientationRejection::ShortLegNotShortable => "short_leg_not_shortable",
            OrientationRejection::SpreadUnfitted(failure) => failure.as_str(),
            OrientationRejection::SpreadUnreadable => "spread_unreadable",
            OrientationRejection::EntryBelowThreshold { .. } => "entry_below_threshold",
            OrientationRejection::EntryBeyondCap { .. } => "entry_beyond_cap",
            OrientationRejection::ModelDisagrees { .. } => "model_disagrees",
            OrientationRejection::Candidate(rejection) => rejection.as_str(),
        }
    }

    /// The numbers behind the rejection, for the ones that have any.
    pub fn detail(&self) -> Option<String> {
        match self {
            OrientationRejection::ShortLegNotShortable | OrientationRejection::SpreadUnreadable => {
                None
            }
            OrientationRejection::SpreadUnfitted(failure) => failure.detail(),
            OrientationRejection::EntryBelowThreshold { z_score, threshold } => {
                Some(format!("z_score={z_score:.4} threshold={threshold:.4}"))
            }
            OrientationRejection::EntryBeyondCap { z_score, cap } => {
                Some(format!("z_score={z_score:.4} cap={cap:.4}"))
            }
            OrientationRejection::ModelDisagrees { signal_strength } => {
                Some(format!("signal_strength={signal_strength:.6}"))
            }
            OrientationRejection::Candidate(rejection) => Some(rejection.detail()),
        }
    }
}

/// Screens every combination and returns the candidates worth opening, best first.
///
/// Sector is not tested here: a same-sector spread is the canonical statistical arbitrage trade, so
/// concentration is bounded across the book in [`select_disjoint`] instead. No disjointness applies
/// either — the returned list is the full reservoir and pairs may share tickers.
pub fn score_candidates(inputs: &[ScreenInput]) -> Vec<PairCandidate> {
    if inputs.len() < MINIMUM_ELIGIBLE_TICKERS {
        debug!(
            supplied = inputs.len(),
            required = MINIMUM_ELIGIBLE_TICKERS,
            "Too few tickers were supplied to screen any pair"
        );
        return Vec::new();
    }

    let mut candidates: Vec<PairCandidate> = Vec::new();
    // Tallied rather than logged per pair: the loop is quadratic in the universe, so a line each
    // would be millions of them.
    let mut rejections: HashMap<&'static str, usize> = HashMap::new();
    for first_index in 0..inputs.len() {
        for second_index in (first_index + 1)..inputs.len() {
            let first = &inputs[first_index];
            let second = &inputs[second_index];

            // At least one leg has to be shortable or there is no orientation to take.
            if !first.is_shortable && !second.is_shortable {
                *rejections.entry("no_shortable_leg").or_default() += 1;
                continue;
            }

            let correlation = pearson_correlation(
                &logarithmic_returns(first.window()),
                &logarithmic_returns(second.window()),
            );
            let Some(correlation) = correlation else {
                *rejections.entry("correlation_unreadable").or_default() += 1;
                continue;
            };
            if !admits_correlation(correlation) {
                *rejections.entry("correlation_outside_band").or_default() += 1;
                continue;
            }

            match orient(first, second) {
                Ok(candidate) => candidates.push(candidate),
                Err(orientations) => {
                    for rejection in orientations {
                        *rejections.entry(rejection.as_str()).or_default() += 1;
                        // The cap is the one rejection whose reading is worth seeing per pair: it
                        // is how an unadjusted corporate action announces itself.
                        match rejection {
                            OrientationRejection::EntryBeyondCap { z_score, cap } => debug!(
                                first = %first.ticker,
                                second = %second.ticker,
                                z_score,
                                cap,
                                "Rejected a candidate whose entry spread is beyond the cap"
                            ),
                            OrientationRejection::ShortLegNotShortable
                            | OrientationRejection::SpreadUnfitted(_)
                            | OrientationRejection::SpreadUnreadable
                            | OrientationRejection::EntryBelowThreshold { .. }
                            | OrientationRejection::ModelDisagrees { .. }
                            | OrientationRejection::Candidate(_) => {}
                        }
                    }
                }
            }
        }
    }

    candidates.sort_by(|left, right| {
        right
            .rank_score()
            .partial_cmp(&left.rank_score())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut tally: Vec<(&str, usize)> = rejections.into_iter().collect();
    tally.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    debug!(
        supplied = inputs.len(),
        candidates = candidates.len(),
        rejections = ?tally,
        "Pair screen complete"
    );
    candidates
}

/// Decides which leg is long and which is short, and applies the entry and agreement tests.
///
/// Both orientations are tried because ordinary least squares is not symmetric: the slope of `a` on
/// `b` is not the reciprocal of the slope of `b` on `a`. On refusal both causes are returned, in
/// the order tried, because a pair that fails one test each way failed for two different reasons.
fn orient(
    first: &ScreenInput,
    second: &ScreenInput,
) -> Result<PairCandidate, [OrientationRejection; 2]> {
    // `first` short, `second` long; then the reverse. At most one can clear a positive entry
    // threshold, since the two spreads move in opposite directions.
    let forward = match orient_one(second, first) {
        Ok(candidate) => return Ok(candidate),
        Err(rejection) => rejection,
    };
    let reverse = match orient_one(first, second) {
        Ok(candidate) => return Ok(candidate),
        Err(rejection) => rejection,
    };
    Err([forward, reverse])
}

/// Applies every entry test to one fixed orientation.
fn orient_one(
    long: &ScreenInput,
    short: &ScreenInput,
) -> Result<PairCandidate, OrientationRejection> {
    if !short.is_shortable {
        return Err(OrientationRejection::ShortLegNotShortable);
    }
    let model = SpreadModel::fit(long.window(), short.window())
        .map_err(OrientationRejection::SpreadUnfitted)?;
    let z_score = model
        .z_score(long.price, short.price)
        .ok_or(OrientationRejection::SpreadUnreadable)?;
    if !admits_entry_z_score(z_score) {
        return Err(if z_score < ENTRY_Z_SCORE {
            OrientationRejection::EntryBelowThreshold {
                z_score,
                threshold: ENTRY_Z_SCORE,
            }
        } else {
            OrientationRejection::EntryBeyondCap {
                z_score,
                cap: ENTRY_Z_SCORE_CAP,
            }
        });
    }

    // The model has to agree that the cheap leg is the one to own. Checked here as well as in
    // `PairCandidate::new` so the caller can tell a model disagreement from a spread rejection.
    let signal_strength = long.expected_return - short.expected_return;
    if signal_strength <= 0.0 {
        return Err(OrientationRejection::ModelDisagrees { signal_strength });
    }

    PairCandidate::new(
        PairID::new(long.ticker.clone(), short.ticker.clone()),
        model.hedge_ratio(),
        z_score,
        signal_strength,
        long.price,
        short.price,
    )
    .map_err(OrientationRejection::Candidate)
}

/// Greedily takes up to `limit` candidates that share no ticker with each other or with `held`,
/// and that keep every sector within [`MAXIMUM_LEGS_PER_SECTOR`].
///
/// The sector cap is seeded from the book, not from this pass, so a sector at its limit stays there
/// rather than being handed a fresh allowance every five minutes. A held ticker whose sector is
/// unknown contributes to no sector rather than to a fabricated one, and still blocks re-entry.
pub fn select_disjoint(
    candidates: &[PairCandidate],
    limit: usize,
    held: &HashSet<Ticker>,
    sectors: &HashMap<Ticker, String>,
) -> Vec<PairCandidate> {
    if limit == 0 {
        return Vec::new();
    }

    let mut used: HashSet<Ticker> = held.clone();
    let mut selected: Vec<PairCandidate> = Vec::with_capacity(limit);

    let mut legs_per_sector: HashMap<&str, usize> = HashMap::new();
    for ticker in held {
        if let Some(sector) = sectors.get(ticker) {
            *legs_per_sector.entry(sector.as_str()).or_default() += 1;
        }
    }

    for candidate in candidates {
        if selected.len() == limit {
            break;
        }
        if used.contains(candidate.long_ticker()) || used.contains(candidate.short_ticker()) {
            continue;
        }

        let long_sector = sectors.get(candidate.long_ticker()).map(String::as_str);
        let short_sector = sectors.get(candidate.short_ticker()).map(String::as_str);
        let taken = |sector: &str| legs_per_sector.get(sector).copied().unwrap_or(0);

        // Both legs are weighed together, and the same-sector case is why: asking twice for one
        // allowance, a leg at a time, would let such a pair take its sector one past the cap.
        let fits = match (long_sector, short_sector) {
            (Some(long), Some(short)) if long == short => {
                taken(long) + 2 <= MAXIMUM_LEGS_PER_SECTOR
            }
            (Some(long), Some(short)) => {
                taken(long) < MAXIMUM_LEGS_PER_SECTOR && taken(short) < MAXIMUM_LEGS_PER_SECTOR
            }
            (Some(sector), None) | (None, Some(sector)) => taken(sector) < MAXIMUM_LEGS_PER_SECTOR,
            (None, None) => true,
        };
        if !fits {
            continue;
        }
        for sector in [long_sector, short_sector].into_iter().flatten() {
            *legs_per_sector.entry(sector).or_default() += 1;
        }
        used.insert(candidate.long_ticker().clone());
        used.insert(candidate.short_ticker().clone());
        selected.push(candidate.clone());
    }
    selected
}

/// Why an open pair has no spread model this pass.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitModelFailure {
    /// One or both legs are absent from the session's close history.
    MissingCloseHistory,
    /// The history is present and the distribution could not be rebuilt from it.
    Unfitted(SpreadFitFailure),
}

impl ExitModelFailure {
    /// The stable name this failure is recorded under.
    pub fn as_str(&self) -> &'static str {
        match self {
            ExitModelFailure::MissingCloseHistory => "missing_close_history",
            ExitModelFailure::Unfitted(failure) => failure.as_str(),
        }
    }

    /// The name and, where there is one, the reading behind it.
    pub fn detail(&self) -> String {
        match self {
            ExitModelFailure::MissingCloseHistory => "missing_close_history".to_string(),
            ExitModelFailure::Unfitted(failure) => match failure.detail() {
                Some(detail) => format!("{}: {detail}", failure.as_str()),
                None => failure.as_str().to_string(),
            },
        }
    }
}

/// Builds the exit models for open pairs, keyed by pair identifier.
///
/// Every supplied pair gets an entry, successful or not: "held with no signal" and "held because
/// its history went missing" are different operational facts, and a pair absent from the map would
/// collapse them. The caller never reads a failure as "hold forever" — the pre-close liquidation
/// closes it regardless.
pub fn exit_models<'a>(
    pairs: impl IntoIterator<Item = (&'a PairID, f64)>,
    closes: &HashMap<Ticker, Vec<f64>>,
) -> HashMap<PairID, Result<SpreadModel, ExitModelFailure>> {
    let mut models = HashMap::new();
    for (pair_id, hedge_ratio) in pairs {
        let built = match (closes.get(pair_id.long()), closes.get(pair_id.short())) {
            (Some(long_closes), Some(short_closes)) => {
                SpreadModel::with_hedge_ratio(hedge_ratio, long_closes, short_closes)
                    .map_err(ExitModelFailure::Unfitted)
            }
            (None, _) | (_, None) => Err(ExitModelFailure::MissingCloseHistory),
        };
        models.insert(pair_id.clone(), built);
    }
    models
}

/// Takes the natural logarithm of two series, refusing unless both are the same usable length with
/// no non-positive value.
fn aligned_logs(
    long_closes: &[f64],
    short_closes: &[f64],
) -> Result<(Vec<f64>, Vec<f64>), SpreadFitFailure> {
    if long_closes.len() != short_closes.len() {
        return Err(SpreadFitFailure::MisalignedSeries {
            long_sessions: long_closes.len(),
            short_sessions: short_closes.len(),
        });
    }
    if long_closes.len() < MINIMUM_SPREAD_OBSERVATIONS {
        return Err(SpreadFitFailure::WindowTooShort {
            sessions: long_closes.len(),
            minimum: MINIMUM_SPREAD_OBSERVATIONS,
        });
    }
    let to_logs = |closes: &[f64]| -> Option<Vec<f64>> {
        closes
            .iter()
            .map(|close| (close.is_finite() && *close > 0.0).then(|| close.ln()))
            .collect()
    };
    match (to_logs(long_closes), to_logs(short_closes)) {
        (Some(long_logs), Some(short_logs)) => Ok((long_logs, short_logs)),
        (None, _) | (_, None) => Err(SpreadFitFailure::UnusableClose),
    }
}

/// The window's largest single-session move, by magnitude, as a signed log return.
///
/// Signed so the recorded detail says which way the series jumped, and largest rather than first so
/// the number a bound gets calibrated against is the worst one present. Iterates rather than reusing
/// [`logarithmic_returns`], which would collect a vector per ticker on a pass that screens over a thousand.
pub(crate) fn worst_session_move(window: &[f64]) -> Option<f64> {
    window
        .windows(2)
        .filter(|pair| pair[0] > 0.0 && pair[1] > 0.0)
        .map(|pair| (pair[1] / pair[0]).ln())
        .max_by(|left, right| left.abs().total_cmp(&right.abs()))
}

/// Period-over-period log returns. One shorter than its input.
pub(crate) fn logarithmic_returns(prices: &[f64]) -> Vec<f64> {
    prices
        .windows(2)
        .filter(|window| window[0] > 0.0 && window[1] > 0.0)
        .map(|window| (window[1] / window[0]).ln())
        .collect()
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    mean.is_finite().then_some(mean)
}

/// Sample standard deviation, with one degree of freedom removed.
fn standard_deviation(values: &[f64], mean: f64) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let sum_of_squares: f64 = values.iter().map(|value| (value - mean).powi(2)).sum();
    let deviation = (sum_of_squares / (values.len() - 1) as f64).sqrt();
    deviation.is_finite().then_some(deviation)
}

/// Pearson correlation. `None` when either series has no dispersion to correlate.
pub(crate) fn pearson_correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    let count = left.len().min(right.len());
    if count < 2 {
        return None;
    }
    let left = &left[left.len() - count..];
    let right = &right[right.len() - count..];

    let left_mean = mean(left)?;
    let right_mean = mean(right)?;

    let mut covariance = 0.0;
    let mut left_variance = 0.0;
    let mut right_variance = 0.0;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_deviation = left_value - left_mean;
        let right_deviation = right_value - right_mean;
        covariance += left_deviation * right_deviation;
        left_variance += left_deviation.powi(2);
        right_variance += right_deviation.powi(2);
    }

    let denominator = (left_variance * right_variance).sqrt();
    if denominator <= f64::EPSILON {
        return None;
    }
    let correlation = covariance / denominator;
    correlation.is_finite().then_some(correlation)
}

/// Ordinary least squares slope of `y` on `x`, without an intercept term on the slope itself.
///
/// `None` when `x` has no dispersion, which would otherwise divide by zero and produce an infinite
/// hedge ratio that every later comparison silently accepts.
fn ordinary_least_squares_slope(x_values: &[f64], y_values: &[f64]) -> Option<f64> {
    if x_values.len() != y_values.len() || x_values.len() < 2 {
        return None;
    }
    let x_mean = mean(x_values)?;
    let y_mean = mean(y_values)?;

    let mut covariance = 0.0;
    let mut x_variance = 0.0;
    for (x_value, y_value) in x_values.iter().zip(y_values.iter()) {
        let x_deviation = x_value - x_mean;
        covariance += x_deviation * (y_value - y_mean);
        x_variance += x_deviation.powi(2);
    }

    if x_variance <= f64::EPSILON {
        return None;
    }
    let slope = covariance / x_variance;
    slope.is_finite().then_some(slope)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).expect("test ticker must be valid")
    }

    /// A cointegrated pair whose correlation lands inside the screen's band.
    ///
    /// Both legs share a common factor; the follower's idiosyncratic component puts the correlation
    /// near 0.8 rather than 1.0 and gives the spread dispersion to revert within.
    ///
    /// A series with no idiosyncratic component correlates at 1.0 and one whose spread has no
    /// variance will not fit; either produces zero candidates and every test below asserts nothing.
    fn cointegrated_series(sessions: usize) -> (Vec<f64>, Vec<f64>) {
        let mut leader = Vec::with_capacity(sessions);
        let mut follower = Vec::with_capacity(sessions);
        let mut leader_price = 100.0_f64;
        let mut follower_price = 80.0_f64;
        for session in 0..sessions {
            let step = session as f64;
            let common = 0.012 * (step * 0.7).sin();
            let idiosyncratic = 0.012 * (step * 1.9 + 1.0).sin();
            leader_price *= common.exp();
            follower_price *= (0.8 * common + 0.6 * idiosyncratic).exp();
            leader.push(leader_price);
            follower.push(follower_price);
        }
        (leader, follower)
    }

    /// The fixture has to sit inside the screen's own correlation band, or the tests below it are
    /// asserting against a pair the screen would never see.
    #[test]
    fn test_the_fixture_correlates_within_the_screened_band() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let correlation = pearson_correlation(
            &logarithmic_returns(&leader),
            &logarithmic_returns(&follower),
        )
        .expect("the fixture must correlate");
        assert!(
            (CORRELATION_MINIMUM..=CORRELATION_MAXIMUM).contains(&correlation),
            "fixture correlation {correlation} is outside [{CORRELATION_MINIMUM}, {CORRELATION_MAXIMUM}]"
        );
    }

    fn input(name: &str, closes: Vec<f64>, price: f64, expected_return: f64) -> ScreenInput {
        ScreenInput::new(ticker(name), closes, price, expected_return, 0.9, true)
            .expect("test input must be constructible")
    }

    /// The whole point of the type. An open pair's exit is judged against the hedge ratio it was
    /// entered on, so the spread being measured now is the one the entry threshold was applied to.
    /// Refitting instead would judge the position against a line it was never above.
    #[test]
    fn test_with_hedge_ratio_reuses_the_stored_ratio_rather_than_refitting() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let fitted = SpreadModel::fit(&long_closes, &short_closes).expect("the fit must succeed");
        let rebuilt = SpreadModel::with_hedge_ratio(0.5, &long_closes, &short_closes)
            .expect("the rebuild must succeed");

        assert_eq!(rebuilt.hedge_ratio(), 0.5);
        assert_ne!(fitted.hedge_ratio(), rebuilt.hedge_ratio());
    }

    /// A flat spread has zero standard deviation, which makes every z-score infinite — so every
    /// pair reads as a screaming entry and every open pair reads as an instant stop-out.
    #[test]
    fn test_fit_rejects_a_spread_with_no_dispersion() {
        let closes = vec![100.0; CORRELATION_WINDOW_SESSIONS];
        // The cause, not merely the refusal: a flat series is refused by two different tests on the
        // way through, and only one of them is the degenerate distribution.
        assert_eq!(
            SpreadModel::fit(&closes, &closes).expect_err("a flat spread must be refused"),
            SpreadFitFailure::HedgeRatioUnfittable
        );

        let (long_closes, _) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let failure = SpreadModel::with_hedge_ratio(1.0, &long_closes, &long_closes)
            .expect_err("a spread against itself has no dispersion");
        let SpreadFitFailure::NoDispersion { standard_deviation } = failure else {
            panic!("expected no dispersion, got {failure:?}");
        };
        assert_eq!(standard_deviation, 0.0);
    }

    #[test]
    fn test_fit_rejects_misaligned_or_non_positive_series() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        assert_eq!(
            SpreadModel::fit(&long_closes[1..], &short_closes)
                .expect_err("misaligned series must be refused"),
            SpreadFitFailure::MisalignedSeries {
                long_sessions: 59,
                short_sessions: 60,
            }
        );

        let mut negative = long_closes.clone();
        negative[10] = -1.0;
        assert_eq!(
            SpreadModel::fit(&negative, &short_closes)
                .expect_err("a non-positive close must be refused"),
            SpreadFitFailure::UnusableClose
        );
    }

    #[test]
    fn test_every_spread_failure_has_a_stable_name_and_the_numbered_ones_carry_a_reading() {
        assert_eq!(
            SpreadFitFailure::MisalignedSeries {
                long_sessions: 60,
                short_sessions: 59,
            }
            .detail()
            .expect("a misalignment reports both lengths"),
            "long_sessions=60 short_sessions=59"
        );
        assert_eq!(
            SpreadFitFailure::WindowTooShort {
                sessions: 12,
                minimum: 60,
            }
            .as_str(),
            "window_too_short"
        );
        assert_eq!(SpreadFitFailure::UnusableClose.detail(), None);
        assert_eq!(SpreadFitFailure::HedgeRatioUnfittable.detail(), None);
        assert_eq!(
            SpreadFitFailure::HedgeRatioUnusable {
                hedge_ratio: f64::NAN,
            }
            .as_str(),
            "hedge_ratio_unusable"
        );
        assert_eq!(
            SpreadFitFailure::NoDispersion {
                standard_deviation: 0.0,
            }
            .as_str(),
            "no_dispersion"
        );
    }

    /// The observation is intraday and the window is closed daily bars, so the point being scored
    /// is not in the distribution it is scored against. A z-score taken against a distribution
    /// containing the observation is bounded by the sample size and can never reach the threshold.
    #[test]
    fn test_z_score_is_unbounded_by_the_fitted_sample_size() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let model = SpreadModel::fit(&long_closes, &short_closes).expect("the fit must succeed");

        let long_price = *long_closes.last().unwrap();
        let stretched = short_closes.last().unwrap() * 1.5;
        let z_score = model
            .z_score(long_price, stretched)
            .expect("a live reading must standardize");

        let sample_bound = (CORRELATION_WINDOW_SESSIONS as f64 - 1.0).sqrt();
        assert!(
            z_score > sample_bound,
            "a live observation must be able to exceed the in-sample maximum of {sample_bound}, got {z_score}"
        );
    }

    /// The exit path must be fitted over the same window as the entry. A shorter series produces a
    /// mean and standard deviation from a different sample, so the z-score can cross a threshold for
    /// a spread that has not moved — which is the asymmetry this type exists to prevent, one level
    /// down from the hedge ratio.
    #[test]
    fn test_with_hedge_ratio_refuses_a_series_shorter_than_the_window() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let short_history = CORRELATION_WINDOW_SESSIONS - 1;

        assert_eq!(
            SpreadModel::with_hedge_ratio(
                1.0,
                &long_closes[..short_history],
                &short_closes[..short_history],
            )
            .expect_err("a short series must be refused"),
            SpreadFitFailure::WindowTooShort {
                sessions: 59,
                minimum: 60,
            }
        );
        assert!(SpreadModel::with_hedge_ratio(1.0, &long_closes, &short_closes).is_ok());
    }

    /// A longer history is trimmed to the window rather than fitted over all of it, so an exit
    /// measured today uses the same number of observations as the entry did.
    #[test]
    fn test_with_hedge_ratio_trims_a_longer_series_to_the_window() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS * 2);
        let trimmed = SpreadModel::with_hedge_ratio(
            1.0,
            &long_closes[long_closes.len() - CORRELATION_WINDOW_SESSIONS..],
            &short_closes[short_closes.len() - CORRELATION_WINDOW_SESSIONS..],
        );
        assert_eq!(
            SpreadModel::with_hedge_ratio(1.0, &long_closes, &short_closes),
            trimmed
        );
    }

    #[test]
    fn test_z_score_rejects_a_non_positive_price() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let model = SpreadModel::fit(&long_closes, &short_closes).expect("the fit must succeed");
        assert_eq!(model.z_score(0.0, 100.0), None);
        assert_eq!(model.z_score(100.0, f64::NAN), None);
    }

    #[test]
    fn test_ordinary_least_squares_recovers_a_known_slope() {
        let x_values: Vec<f64> = (0..20).map(|index| index as f64).collect();
        let y_values: Vec<f64> = x_values.iter().map(|x| 3.0 * x + 7.0).collect();
        let slope = ordinary_least_squares_slope(&x_values, &y_values).unwrap();
        assert!((slope - 3.0).abs() < 1e-9, "expected 3.0, got {slope}");
    }

    #[test]
    fn test_ordinary_least_squares_rejects_a_constant_predictor() {
        assert_eq!(
            ordinary_least_squares_slope(
                &[5.0; 10],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
            ),
            None
        );
    }

    #[test]
    fn test_pearson_correlation_recovers_perfect_agreement_and_opposition() {
        let ascending: Vec<f64> = (0..20).map(|index| index as f64).collect();
        let descending: Vec<f64> = ascending.iter().map(|value| -value).collect();
        assert!((pearson_correlation(&ascending, &ascending).unwrap() - 1.0).abs() < 1e-12);
        assert!((pearson_correlation(&ascending, &descending).unwrap() + 1.0).abs() < 1e-12);
        assert_eq!(pearson_correlation(&ascending, &[3.0; 20]), None);
    }

    #[test]
    fn test_logarithmic_returns_is_one_shorter_than_its_input() {
        let returns = logarithmic_returns(&[100.0, 110.0, 121.0]);
        assert_eq!(returns.len(), 2);
        assert!((returns[0] - returns[1]).abs() < 1e-12);
    }

    /// The fixture has to actually produce pairs, or every assertion below it passes vacuously.
    /// This is the trap recorded in `statistical_arbitrage_test_fixtures`.
    #[test]
    fn test_the_fixture_yields_at_least_one_candidate() {
        assert!(!score_candidates(&screenable_inputs()).is_empty());
    }

    /// Two cointegrated names with the short-leg candidate stretched 1.2% away from its partner.
    ///
    /// The generated series is near-deterministic — its spread deviation is about 0.3% — so a
    /// larger stretch scores a z the screen would never admit and every test built on it would
    /// assert against a candidate that cannot exist. This lands at z ~ 3.0.
    fn screenable_inputs() -> Vec<ScreenInput> {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.012;
        vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), 0.03),
            input("BBBB", follower.clone(), stretched, -0.02),
        ]
    }

    /// Every candidate the screen emits must survive its own entry reading.
    ///
    /// Composing the screen and the exit rule is the only place a candidate born closable is
    /// visible: each half is internally consistent about a threshold the other never sees.
    #[test]
    fn test_no_candidate_is_closable_at_its_own_entry() {
        let candidates = score_candidates(&screenable_inputs());
        // The count before the readings. A loop over an empty reservoir asserts nothing, and the
        // fixture produces exactly one pair from two names.
        assert_eq!(candidates.len(), 1, "the fixture must produce a candidate");

        for candidate in candidates {
            let entry = candidate.entry_z_score();
            assert!(
                entry <= ENTRY_Z_SCORE_CAP,
                "the screen emitted z={entry}, beyond the cap of {ENTRY_Z_SCORE_CAP}"
            );
            assert_eq!(
                crate::portfolio::evaluate::exit_reason(entry, entry),
                None,
                "a candidate entered at z={entry} would close on its own entry reading"
            );
        }
    }

    /// The cap is an upper bound on what the screen will emit, not advice.
    ///
    /// The emptiness is the assertion, so it has to be attributed: the same two names produce a
    /// candidate at a stretch inside the band, and the orientation names the cap as what refused
    /// them. An empty reservoir on its own is equally consistent with a broken fixture.
    #[test]
    fn test_a_spread_beyond_the_cap_is_not_a_candidate() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        // Far enough out that the fitted spread cannot place it inside the cap.
        let dislocated = follower.last().unwrap() * 10.0;
        let inputs = vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), 0.03),
            input("BBBB", follower.clone(), dislocated, -0.02),
        ];

        assert_eq!(score_candidates(&screenable_inputs()).len(), 1);
        assert_eq!(
            score_candidates(&inputs).len(),
            0,
            "a dislocated spread is not a candidate"
        );

        // In the order tried: BBBB long is the mirror of the dislocation and reads below the
        // floor; AAAA long is the stretched orientation, and the cap is what turns it away.
        let [bbbb_long, aaaa_long] =
            orient(&inputs[0], &inputs[1]).expect_err("the dislocation must be refused");
        assert_eq!(bbbb_long.as_str(), "entry_below_threshold");

        let OrientationRejection::EntryBeyondCap { z_score, cap } = aaaa_long else {
            panic!("expected the cap to refuse it, got {aaaa_long:?}");
        };
        assert!(z_score > 5.0, "the fixture must sit beyond the cap");
        assert_eq!(cap, 5.0, "the cap reported is the one in schema terms");
    }

    /// The spread decides which leg is expensive; the short leg is the expensive one, always. An
    /// orientation that flips means the position is exactly backwards while looking correct.
    #[test]
    fn test_the_expensive_leg_becomes_the_short_leg() {
        let candidates = score_candidates(&screenable_inputs());
        let candidate = candidates.first().expect("the fixture must produce a pair");
        assert_eq!(candidate.short_ticker().as_str(), "BBBB");
        assert_eq!(candidate.long_ticker().as_str(), "AAAA");
        assert!(candidate.entry_z_score() >= ENTRY_Z_SCORE);
    }

    /// Every entry score is positive by construction, which is what lets convergence be a fall to
    /// zero with no sign handling anywhere downstream.
    #[test]
    fn test_every_candidate_carries_a_positive_entry_score() {
        let candidates = score_candidates(&screenable_inputs());
        assert_eq!(candidates.len(), 1, "the fixture must produce a candidate");

        for candidate in candidates {
            assert!(candidate.entry_z_score() > 0.0);
            assert!(candidate.signal_strength() > 0.0);
        }
    }

    /// The spread says buy AAAA and sell BBBB; the prediction says the opposite. Opening on that is a
    /// position whose two justifications cancel.
    #[test]
    fn test_a_pair_the_model_disagrees_with_is_not_opened() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.5;
        let inputs = vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), -0.02),
            input("BBBB", follower, stretched, 0.03),
        ];
        assert!(score_candidates(&inputs).is_empty());
    }

    /// An anti-correlated pair fits a negative hedge ratio, which turns the spread into a sum and
    /// hedges nothing. Sizing is dollar-neutral regardless, so admitting one produces a directional
    /// bet wearing the name of a market-neutral pair.
    #[test]
    fn test_an_anti_correlated_pair_is_rejected() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        // Mirror the follower's returns around its starting level to invert the correlation while
        // keeping every price positive and the dispersion intact.
        let first = follower[0];
        let mirrored: Vec<f64> = follower.iter().map(|price| first * first / price).collect();

        let correlation = pearson_correlation(
            &logarithmic_returns(&leader),
            &logarithmic_returns(&mirrored),
        )
        .expect("the mirrored fixture must correlate");
        assert!(
            correlation < -CORRELATION_MINIMUM,
            "the fixture must be anti-correlated inside the band's magnitude, got {correlation}"
        );

        let stretched = mirrored.last().unwrap() * 1.5;
        let inputs = vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), 0.03),
            input("BBBB", mirrored, stretched, -0.02),
        ];
        assert!(score_candidates(&inputs).is_empty());
    }

    /// Two cointegrated names are a candidate, whatever sectors they are in.
    ///
    /// Sector cannot even be expressed here: `ScreenInput` does not carry one, because nothing in
    /// scoring reads it. Concentration is bounded in [`select_disjoint`] instead.
    #[test]
    fn test_scoring_does_not_consider_sector() {
        // The shared fixture rather than a second hand-rolled dislocation, so the entry score stays
        // inside the band the screen admits when the fixture is retuned.
        let candidates = score_candidates(&screenable_inputs());

        assert_eq!(
            candidates.len(),
            1,
            "two cointegrated names are a candidate"
        );
        assert_eq!(candidates[0].long_ticker().as_str(), "AAAA");
        assert_eq!(candidates[0].short_ticker().as_str(), "BBBB");
    }

    /// Without a shortable short leg there is no position to take, whatever the spread says.
    #[test]
    fn test_a_pair_whose_expensive_leg_cannot_be_shorted_is_rejected() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.5;
        let mut inputs = screenable_inputs();
        inputs[1] =
            ScreenInput::new(ticker("BBBB"), follower, stretched, -0.02, 0.9, false).unwrap();
        let _ = leader;
        assert!(score_candidates(&inputs).is_empty());
    }

    /// Confidence is carried and journaled but no longer screened on, so a leg that would have
    /// failed the retired 0.5 floor now reaches the screen like any other.
    ///
    /// Built on the `screenable_inputs` stretch, which is the only one in this module that yields a
    /// candidate: the retired test used 1.5, which clears `ENTRY_Z_SCORE_CAP` and returned nothing
    /// at *any* confidence, so it never tested the floor it was named for.
    #[test]
    fn test_a_low_confidence_leg_still_reaches_the_screen() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.012;
        let inputs = vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), 0.03),
            // 0.01, far below the 0.5 both legs used to have to clear.
            ScreenInput::new(ticker("BBBB"), follower, stretched, -0.02, 0.01, true).unwrap(),
        ];

        let candidates = score_candidates(&inputs);

        // One pair from two tickers, where the retired floor would have left none.
        assert_eq!(
            candidates.len(),
            1,
            "a dislocated pair must still be scored"
        );
        let tickers = [
            candidates[0].long_ticker().as_str(),
            candidates[0].short_ticker().as_str(),
        ];
        assert!(
            tickers.contains(&"BBBB"),
            "the 0.01 leg must be in the pair"
        );
    }

    #[test]
    fn test_screen_input_rejects_a_short_or_unusable_history() {
        assert_eq!(
            ScreenInput::new(
                ticker("AAAA"),
                vec![100.0; CORRELATION_WINDOW_SESSIONS - 1],
                100.0,
                0.01,
                0.9,
                true
            )
            .expect_err("too short a history must be refused"),
            ScreenRejection::UnusableInput
        );

        let mut with_zero = vec![100.0; CORRELATION_WINDOW_SESSIONS];
        with_zero[3] = 0.0;
        assert_eq!(
            ScreenInput::new(ticker("AAAA"), with_zero, 100.0, 0.01, 0.9, true)
                .expect_err("a non-positive close must be refused"),
            ScreenRejection::UnusableInput
        );
    }

    /// A window holding one persistent level change, the shape a collapse or a split leaves behind.
    fn window_with_one_move(logarithmic_return: f64) -> Vec<f64> {
        let mut closes = vec![100.0; CORRELATION_WINDOW_SESSIONS];
        for close in closes.iter_mut().skip(CORRELATION_WINDOW_SESSIONS / 2) {
            *close = 100.0 * logarithmic_return.exp();
        }
        closes
    }

    /// The reason the guard exists. A collapse partway through leaves a series that is not one
    /// distribution, and nothing about the fit says so — it succeeds, and the dispersion it reports
    /// describes the level change rather than the spread it is supposed to measure.
    #[test]
    fn test_a_broken_window_silently_inflates_the_fitted_dispersion() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let clean = SpreadModel::fit(&leader, &follower).expect("the clean fit must succeed");

        let mut broken = follower.clone();
        for close in broken.iter_mut().skip(CORRELATION_WINDOW_SESSIONS / 2) {
            *close *= 0.1;
        }
        let damaged = SpreadModel::fit(&leader, &broken).expect("the broken fit still succeeds");

        assert!(
            damaged.standard_deviation() > clean.standard_deviation() * 5.0,
            "broken standard deviation {} should dwarf the clean {}",
            damaged.standard_deviation(),
            clean.standard_deviation()
        );
    }

    #[test]
    fn test_screen_input_rejects_a_window_containing_a_structural_break() {
        let breached = MAXIMUM_SESSION_LOGARITHMIC_RETURN + 0.01;
        let rejection = ScreenInput::new(
            ticker("AAAA"),
            window_with_one_move(-breached),
            100.0,
            0.01,
            0.9,
            true,
        )
        .expect_err("a window with a break must be refused");

        // The payload, not only the variant: the recorded number is the entire reason the rejection
        // is written down, and a sign flip or a swapped pair reads as a pass against `matches!`.
        let ScreenRejection::StructuralBreak {
            logarithmic_return,
            limit,
        } = rejection
        else {
            panic!("expected a structural break, got {rejection:?}");
        };
        assert!(
            (logarithmic_return + breached).abs() < 1e-9,
            "got {logarithmic_return}"
        );
        assert_eq!(limit, 0.40, "the limit reported is the one in schema terms");
    }

    /// At the limit exactly, not merely inside it. The comparison is strict, so a move equal to the
    /// bound is admitted, and testing below the bound would let a change to `>=` pass unnoticed.
    #[test]
    fn test_screen_input_accepts_a_move_at_the_limit() {
        assert!(ScreenInput::new(
            ticker("AAAA"),
            window_with_one_move(MAXIMUM_SESSION_LOGARITHMIC_RETURN),
            100.0,
            0.01,
            0.9,
            true
        )
        .is_ok());
    }

    /// Only the fitted window can reach the spread model, so a name whose history has since settled
    /// is screenable again rather than excluded for as long as the break stays in its series.
    #[test]
    fn test_a_break_older_than_the_fitted_window_is_not_rejected() {
        let mut closes = vec![10.0; 5];
        closes.extend(vec![100.0; CORRELATION_WINDOW_SESSIONS]);
        assert!(ScreenInput::new(ticker("AAAA"), closes, 100.0, 0.01, 0.9, true).is_ok());
    }

    /// Entry only, deliberately. Refusing to measure a pair already on the book would leave it with
    /// no signal to close on, and a close reduces risk — the asymmetry runs one way.
    #[test]
    fn test_the_exit_path_still_measures_a_broken_window() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let mut broken = follower.clone();
        for close in broken.iter_mut().skip(CORRELATION_WINDOW_SESSIONS / 2) {
            *close *= 0.1;
        }
        assert!(SpreadModel::with_hedge_ratio(0.9, &leader, &broken).is_ok());
    }

    #[test]
    fn test_every_rejection_has_a_stable_name_and_only_breaks_carry_detail() {
        assert_eq!(ScreenRejection::UnusableInput.as_str(), "unusable_input");
        assert_eq!(ScreenRejection::UnusableInput.detail(), None);

        let broken = ScreenRejection::StructuralBreak {
            logarithmic_return: -2.28,
            limit: MAXIMUM_SESSION_LOGARITHMIC_RETURN,
        };
        assert_eq!(broken.as_str(), "structural_break");
        // The whole string: the detail exists to be aggregated out of the journal, so the format
        // is the contract and a `contains` check would not notice it drifting.
        assert_eq!(
            broken.detail().expect("a break reports its reading"),
            format!("logarithmic_return=-2.2800 limit={MAXIMUM_SESSION_LOGARITHMIC_RETURN:.4}")
        );
    }

    #[test]
    fn test_worst_session_move_reports_the_largest_by_magnitude_with_its_sign() {
        assert_eq!(worst_session_move(&[100.0]), None);

        let move_down = worst_session_move(&[100.0, 110.0, 55.0]).expect("a move must be found");
        assert!((move_down - (55.0_f64 / 110.0).ln()).abs() < 1e-12);
    }

    /// Assigns every named ticker to one sector, for the selection tests.
    fn sector_map(assignments: &[(&str, &str)]) -> HashMap<Ticker, String> {
        assignments
            .iter()
            .map(|(symbol, sector)| (ticker(symbol), (*sector).to_string()))
            .collect()
    }

    /// The nth distinct pair of symbols: `("LAAA", "SAAA")`, `("LBBB", "SBBB")`, and so on.
    /// `Ticker` admits letters only, so the index is spelled rather than numbered.
    fn pair_for_index(index: usize) -> (String, String) {
        let letter = (b'A' + index as u8) as char;
        (
            format!("L{letter}{letter}{letter}"),
            format!("S{letter}{letter}{letter}"),
        )
    }

    /// `count` distinct pairs, both legs of each in `sector`, ranked best first.
    fn same_sector_candidates(
        count: usize,
        sector: &str,
    ) -> (Vec<PairCandidate>, HashMap<Ticker, String>) {
        let symbols: Vec<(String, String)> = (0..count).map(pair_for_index).collect();
        let candidates = symbols
            .iter()
            .enumerate()
            .map(|(index, (long, short))| candidate(long, short, (count - index) as f64))
            .collect();
        let assignments: Vec<(&str, &str)> = symbols
            .iter()
            .flat_map(|(long, short)| [(long.as_str(), sector), (short.as_str(), sector)])
            .collect();
        (candidates, sector_map(&assignments))
    }

    /// The cap is what stops a reservoir of same-sector spreads becoming one bet held ten times.
    /// Both legs sit in the sector, so each pair spends two of the six legs on offer.
    #[test]
    fn test_selection_stops_at_the_sector_cap() {
        let (candidates, sectors) = same_sector_candidates(6, "Technology");

        let selected = select_disjoint(&candidates, 10, &HashSet::new(), &sectors);

        assert_eq!(
            selected.len(),
            3,
            "six legs allows three same-sector pairs, not six"
        );
        assert_eq!(
            selected[0].long_ticker().as_str(),
            "LAAA",
            "the best candidates are the ones kept"
        );
    }

    /// A cross-sector pair spends one leg in each sector, so the same allowance goes twice as far.
    #[test]
    fn test_a_cross_sector_pair_costs_one_leg_in_each_sector() {
        let symbols: Vec<(String, String)> = (0..6).map(pair_for_index).collect();
        let candidates: Vec<PairCandidate> = symbols
            .iter()
            .enumerate()
            .map(|(index, (long, short))| candidate(long, short, (6 - index) as f64))
            .collect();
        let assignments: Vec<(&str, &str)> = symbols
            .iter()
            .flat_map(|(long, short)| {
                [
                    (long.as_str(), "Technology"),
                    (short.as_str(), "Healthcare"),
                ]
            })
            .collect();
        let sectors = sector_map(&assignments);

        let selected = select_disjoint(&candidates, 10, &HashSet::new(), &sectors);

        assert_eq!(
            selected.len(),
            6,
            "one leg per sector per pair, so six pairs fit inside a six-leg cap"
        );
    }

    /// Seeded from the book, not from the pass. Otherwise a sector at its limit would be handed a
    /// fresh allowance every five minutes.
    #[test]
    fn test_the_cap_counts_legs_already_held() {
        let (candidates, mut sectors) = same_sector_candidates(3, "Technology");
        let held: HashSet<Ticker> = ["HELDA", "HELDB", "HELDC", "HELDD"]
            .iter()
            .map(|symbol| ticker(symbol))
            .collect();
        for symbol in ["HELDA", "HELDB", "HELDC", "HELDD"] {
            sectors.insert(ticker(symbol), "Technology".to_string());
        }

        let selected = select_disjoint(&candidates, 10, &held, &sectors);

        assert_eq!(
            selected.len(),
            1,
            "four legs held leaves room for one more pair, not three"
        );
    }

    /// A held ticker with no sector row contributes to no sector rather than to a fabricated one,
    /// and still blocks re-entry through the disjointness check.
    #[test]
    fn test_a_held_ticker_without_a_sector_does_not_consume_an_allowance() {
        let (candidates, sectors) = same_sector_candidates(3, "Technology");
        let held: HashSet<Ticker> = ["ZZZZ"].iter().map(|symbol| ticker(symbol)).collect();

        let selected = select_disjoint(&candidates, 10, &held, &sectors);

        assert_eq!(selected.len(), 3);
    }

    /// An empty sector map caps nothing.
    ///
    /// A property of this function, not a claim about the system: `build_screen_inputs` refuses a
    /// ticker with no sector upstream, so what this pins is that the cap is the only thing capping.
    #[test]
    fn test_an_empty_sector_map_constrains_nothing() {
        let (candidates, _) = same_sector_candidates(5, "Technology");

        let selected = select_disjoint(&candidates, 10, &HashSet::new(), &HashMap::new());

        assert_eq!(selected.len(), 5);
    }

    fn candidate(long: &str, short: &str, rank: f64) -> PairCandidate {
        PairCandidate::new(
            PairID::new(ticker(long), ticker(short)),
            1.0,
            rank,
            1.0,
            100.0,
            100.0,
        )
        .expect("the test candidate must be constructible")
    }

    /// Five distinct refusals, each naming itself and the number it refused. Collapsed into one
    /// answer they are a candidate that "did not construct", which no bound can be moved from.
    #[test]
    fn test_candidate_rejects_a_backwards_orientation_or_a_contradicting_model() {
        let pair_id = PairID::new(ticker("AAAA"), ticker("BBBB"));
        let refuse = |hedge_ratio, entry_z_score, signal_strength, long_price, short_price| {
            PairCandidate::new(
                pair_id.clone(),
                hedge_ratio,
                entry_z_score,
                signal_strength,
                long_price,
                short_price,
            )
            .expect_err("the candidate must be refused")
        };

        assert_eq!(
            refuse(f64::NAN, 2.5, 0.02, 100.0, 100.0).as_str(),
            "hedge_ratio_unusable"
        );
        assert_eq!(
            refuse(1.0, -2.5, 0.02, 100.0, 100.0),
            CandidateRejection::EntryZScoreNotPositive {
                entry_z_score: -2.5
            }
        );
        assert_eq!(
            refuse(1.0, 2.5, -0.02, 100.0, 100.0),
            CandidateRejection::SignalStrengthNotPositive {
                signal_strength: -0.02
            }
        );
        assert_eq!(
            refuse(1.0, 2.5, 0.02, 0.0, 100.0),
            CandidateRejection::LongPriceUnusable { long_price: 0.0 }
        );
        assert_eq!(
            refuse(1.0, 2.5, 0.02, 100.0, 0.0),
            CandidateRejection::ShortPriceUnusable { short_price: 0.0 }
        );
        // The whole string: the detail is aggregated out of the journal, so the format is contract.
        assert_eq!(
            refuse(1.0, -2.5, 0.02, 100.0, 100.0).detail(),
            "entry_z_score=-2.5"
        );
    }

    #[test]
    fn test_select_disjoint_takes_the_best_and_skips_ticker_collisions() {
        let candidates = vec![
            candidate("AAAA", "BBBB", 4.0),
            candidate("AAAA", "CCCC", 3.0),
            candidate("DDDD", "EEEE", 2.0),
        ];
        let selected = select_disjoint(&candidates, 3, &HashSet::new(), &HashMap::new());
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].short_ticker().as_str(), "BBBB");
        assert_eq!(selected[1].long_ticker().as_str(), "DDDD");
    }

    /// A ticker already on the book cannot appear in a new pair. Opening a second position in a
    /// symbol already held turns two market-neutral pairs into one directional bet.
    #[test]
    fn test_select_disjoint_excludes_tickers_already_held() {
        let candidates = vec![
            candidate("AAAA", "BBBB", 4.0),
            candidate("CCCC", "DDDD", 3.0),
        ];
        let held: HashSet<Ticker> = [ticker("BBBB")].into_iter().collect();
        let selected = select_disjoint(&candidates, 3, &held, &HashMap::new());
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].long_ticker().as_str(), "CCCC");
    }

    #[test]
    fn test_select_disjoint_respects_the_limit() {
        let candidates = vec![
            candidate("AAAA", "BBBB", 4.0),
            candidate("CCCC", "DDDD", 3.0),
        ];
        assert_eq!(
            select_disjoint(&candidates, 1, &HashSet::new(), &HashMap::new()).len(),
            1
        );
        assert!(select_disjoint(&candidates, 0, &HashSet::new(), &HashMap::new()).is_empty());
    }

    /// A pair with no history is named rather than dropped: absent from the map it is
    /// indistinguishable from a pair whose distribution genuinely would not fit.
    #[test]
    fn test_exit_models_names_a_pair_whose_history_is_missing() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let mut closes = HashMap::new();
        closes.insert(ticker("AAAA"), long_closes);
        closes.insert(ticker("BBBB"), short_closes);
        closes.insert(ticker("CCCC"), vec![100.0; CORRELATION_WINDOW_SESSIONS]);

        let present = PairID::new(ticker("AAAA"), ticker("BBBB"));
        let absent = PairID::new(ticker("AAAA"), ticker("ZZZZ"));
        let flat = PairID::new(ticker("CCCC"), ticker("CCCC"));
        let models = exit_models([(&present, 1.0), (&absent, 1.0), (&flat, 1.0)], &closes);

        assert_eq!(models.len(), 3, "every supplied pair gets an entry");
        assert!(models[&present].is_ok());
        assert_eq!(
            models[&absent].expect_err("a pair with no history must say so"),
            ExitModelFailure::MissingCloseHistory
        );
        assert_eq!(
            models[&flat]
                .expect_err("a flat spread must say so")
                .as_str(),
            "no_dispersion"
        );
    }
}
