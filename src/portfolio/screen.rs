//! Pair selection: which two symbols, which way round, and how strong the signal.
//!
//! The screen is quadratic in the eligible universe, so everything cheap runs first: the confidence
//! floor and the shortable check shrink the input before a single correlation is computed.
//!
//! Sector is not one of those tests. It constrains the *book* rather than the pair, so it is applied
//! during selection — see [`MAXIMUM_LEGS_PER_SECTOR`].
//!
//! [`SpreadModel`] is load-bearing. An open pair's model is rebuilt from its *stored* hedge ratio
//! rather than refitted, so entry and exit measure the same spread.

use std::collections::{HashMap, HashSet};

use tracing::debug;

use crate::common::types::{PairID, Ticker};

/// Sessions of daily closes the correlation and the spread distribution are fitted over.
pub const CORRELATION_WINDOW_SESSIONS: usize = 60;

/// Correlation band a pair's log returns must fall inside.
///
/// The floor rejects pairs with nothing to mean-revert to. The ceiling rejects pairs so alike the
/// spread is mostly microstructure noise — two share classes of one issuer — where a two-sigma move
/// is inside the bid-ask spread.
///
/// **The band is on signed correlation, not magnitude.** An anti-correlated pair fits a negative
/// hedge ratio, turning `ln(short) - hedge_ratio * ln(long)` into a sum that hedges nothing, and
/// sizing is dollar-neutral regardless — so admitting one is a directional bet wearing a
/// market-neutral name.
const CORRELATION_MINIMUM: f64 = 0.5;
const CORRELATION_MAXIMUM: f64 = 0.95;

/// Spread z-score at which a pair is worth opening.
pub const ENTRY_Z_SCORE: f64 = 2.0;

/// Spread z-score at which an open pair has converged and is closed at a profit.
///
/// Zero, not a band around zero: the spread is entered above `ENTRY_Z_SCORE` and closed when it
/// crosses back through its own mean, which is the move the position was taken to capture.
pub const CONVERGENCE_Z_SCORE: f64 = 0.0;

/// How much further a spread must widen *beyond its own entry* before the pair is stopped out.
///
/// Relative rather than absolute, because an absolute line cannot be right for every pair at once:
/// with entry admitting anything at or above [`ENTRY_Z_SCORE`], a pair entered above the old fixed
/// stop of 4.0 was already closable the moment it opened, and three of the first ten pairs opened in
/// production were stopped on the next pass without the spread ever moving against them. Measuring
/// from entry is what makes the stop describe adverse movement rather than absolute position.
///
/// Expressed in z units, so it is already normalized per pair: one unit is one standard deviation of
/// *that* pair's own spread.
pub const STOP_LOSS_WIDENING: f64 = 1.5;

/// Upper bound on the entry z-score.
///
/// A data-quality guard, not a strategy rule. Mean reversion is the premise of the whole position,
/// and a spread this far out is more often an unadjusted corporate action or a regime break than an
/// opportunity — neither of which reverts. Rejections are counted so the rate is visible rather than
/// inferred.
pub const ENTRY_Z_SCORE_CAP: f64 = 5.0;

/// Minimum model confidence for a ticker to be eligible for either leg.
pub const CONFIDENCE_FLOOR: f64 = 0.5;

/// Legs the book may hold in one sector at once.
///
/// This is the constraint the different-sector rule was reaching for, applied where it belongs. A
/// reservoir of same-sector spreads ranks by the same industry factor at the top, so ten selected
/// pairs can be one bet held ten times. That is a property of the *selection across the book*, not
/// of any individual candidate — a single same-sector pair is a perfectly good trade, and is in
/// fact the classic one.
///
/// Counted in legs rather than pairs so held and candidate positions measure the same way from a
/// flat ticker set, and because a same-sector pair genuinely sits entirely inside one sector while
/// a cross-sector pair only half does. Against a ten-pair book, six legs is a little under a third
/// of the twenty on offer: enough for three same-sector pairs in one industry, not enough for the
/// book to become an industry bet. Note it is absolute, so lowering
/// [`crate::portfolio::size::MAXIMUM_CONCURRENT_PAIRS`] far enough would make it inert.
pub const MAXIMUM_LEGS_PER_SECTOR: usize = 6;

/// Tickers needed before a screen can produce anything.
const MINIMUM_ELIGIBLE_TICKERS: usize = 2;

/// Observations needed before a spread distribution can be fitted at all.
///
/// Two, because the sample standard deviation removes one degree of freedom. Deliberately a separate
/// constant from [`MINIMUM_ELIGIBLE_TICKERS`] despite sharing its value: the two are unrelated
/// quantities, and a change to the ticker threshold must not silently move the statistical floor.
const MINIMUM_SPREAD_OBSERVATIONS: usize = 2;

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
    ) -> Option<Self> {
        if closes.len() < CORRELATION_WINDOW_SESSIONS {
            return None;
        }
        if !price.is_finite() || price <= 0.0 {
            return None;
        }
        if closes
            .iter()
            .any(|close| !close.is_finite() || *close <= 0.0)
        {
            return None;
        }
        if !expected_return.is_finite() || !confidence.is_finite() {
            return None;
        }
        Some(Self {
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

/// The log-price spread of an oriented pair, and the distribution it is measured against.
///
/// The spread is `ln(short) - hedge_ratio * ln(long)`, with `hedge_ratio` the ordinary least
/// squares slope of the short leg's log price on the long leg's. One consequence is worth stating
/// because everything downstream leans on it: a pair is opened only when the short leg is the
/// expensive one, so **an entry z-score is always positive**, convergence is a fall toward zero,
/// and a stop is a rise away from it. No call site has to reason about which sign means what.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpreadModel {
    hedge_ratio: f64,
    mean: f64,
    standard_deviation: f64,
}

impl SpreadModel {
    /// Fits both the hedge ratio and the distribution from aligned close histories.
    ///
    /// Returns `None` when the series are unusable: different lengths, too short, or a spread with
    /// no dispersion. A zero standard deviation is the degenerate case that matters — it makes
    /// every z-score infinite, so every pair looks like a screaming entry.
    pub fn fit(long_closes: &[f64], short_closes: &[f64]) -> Option<Self> {
        let (long_logs, short_logs) = aligned_logs(long_closes, short_closes)?;
        let hedge_ratio = ordinary_least_squares_slope(&long_logs, &short_logs)?;
        Self::build(hedge_ratio, &long_logs, &short_logs)
    }

    /// Rebuilds the distribution around a hedge ratio that was already decided.
    ///
    /// This is the exit path. Refitting here would measure a different spread from the one the
    /// entry was taken on, judging the position against a line it was never above.
    ///
    /// The window length is enforced for the same reason: [`SpreadModel::fit`] is only ever called
    /// with exactly `CORRELATION_WINDOW_SESSIONS` closes, so a shorter series draws its mean and
    /// standard deviation from a different sample and can cross a threshold for a spread that has
    /// not moved.
    pub fn with_hedge_ratio(
        hedge_ratio: f64,
        long_closes: &[f64],
        short_closes: &[f64],
    ) -> Option<Self> {
        if !hedge_ratio.is_finite() {
            return None;
        }
        if long_closes.len() < CORRELATION_WINDOW_SESSIONS
            || short_closes.len() < CORRELATION_WINDOW_SESSIONS
        {
            return None;
        }
        let long_window = &long_closes[long_closes.len() - CORRELATION_WINDOW_SESSIONS..];
        let short_window = &short_closes[short_closes.len() - CORRELATION_WINDOW_SESSIONS..];

        let (long_logs, short_logs) = aligned_logs(long_window, short_window)?;
        Self::build(hedge_ratio, &long_logs, &short_logs)
    }

    fn build(hedge_ratio: f64, long_logs: &[f64], short_logs: &[f64]) -> Option<Self> {
        if !hedge_ratio.is_finite() {
            return None;
        }
        let spread: Vec<f64> = short_logs
            .iter()
            .zip(long_logs.iter())
            .map(|(short, long)| short - hedge_ratio * long)
            .collect();

        let mean = mean(&spread)?;
        let standard_deviation = standard_deviation(&spread, mean)?;
        if standard_deviation <= f64::EPSILON {
            return None;
        }
        Some(Self {
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
    /// The observation is a pair of current prices and is deliberately not part of the distribution
    /// it is measured against — the window is closed daily bars, the observation is intraday. A
    /// z-score taken against a distribution containing the point being scored is bounded by the
    /// sample size and cannot exceed the threshold it is compared to, which is the failure recorded
    /// in `dual_path_signal_agreement`.
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
    ) -> Option<Self> {
        if !hedge_ratio.is_finite() {
            return None;
        }
        if !entry_z_score.is_finite() || entry_z_score <= 0.0 {
            return None;
        }
        if !signal_strength.is_finite() || signal_strength <= 0.0 {
            return None;
        }
        if !long_price.is_finite() || long_price <= 0.0 {
            return None;
        }
        if !short_price.is_finite() || short_price <= 0.0 {
            return None;
        }
        Some(Self {
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

/// Screens every combination and returns the candidates worth opening, best first.
///
/// A pair survives four tests, in increasing order of cost:
///
/// 1. Both legs clear the confidence floor and at least one is shortable.
/// 2. Their log returns correlate *positively*, within `[CORRELATION_MINIMUM, CORRELATION_MAXIMUM]`.
/// 3. The spread, oriented so the short leg is the expensive one, is at or above `ENTRY_Z_SCORE`.
/// 4. The model agrees with that orientation — it expects the long leg to out-return the short.
///
/// Test four is the model doing more than gating eligibility: without it a pair can open whose
/// spread says buy A and sell B while the forecast says the opposite.
///
/// **Sector is not tested here.** A same-sector spread is the canonical statistical arbitrage
/// trade — two companies facing the same demand, the same input costs, and the same regulator,
/// whose relative price has something to mean-revert toward — and refusing those removes exactly
/// the pairs most likely to cointegrate, which is the property the whole strategy rests on. The
/// real concern was never the pair but the book, so it is answered in [`select_disjoint`] by
/// [`MAXIMUM_LEGS_PER_SECTOR`].
///
/// No disjointness constraint is applied — the returned list is the full reservoir and pairs may
/// share tickers. [`select_disjoint`] is the cheap second half.
pub fn score_candidates(inputs: &[ScreenInput]) -> Vec<PairCandidate> {
    let eligible: Vec<&ScreenInput> = inputs
        .iter()
        .filter(|input| input.confidence >= CONFIDENCE_FLOOR)
        .collect();

    if eligible.len() < MINIMUM_ELIGIBLE_TICKERS {
        debug!(
            eligible = eligible.len(),
            supplied = inputs.len(),
            "Too few tickers cleared the confidence floor to screen any pair"
        );
        return Vec::new();
    }

    let mut candidates: Vec<PairCandidate> = Vec::new();
    for first_index in 0..eligible.len() {
        for second_index in (first_index + 1)..eligible.len() {
            let first = eligible[first_index];
            let second = eligible[second_index];

            // At least one leg has to be shortable or there is no orientation to take.
            if !first.is_shortable && !second.is_shortable {
                continue;
            }

            let correlation =
                pearson_correlation(&log_returns(first.window()), &log_returns(second.window()));
            let Some(correlation) = correlation else {
                continue;
            };
            if !(CORRELATION_MINIMUM..=CORRELATION_MAXIMUM).contains(&correlation) {
                continue;
            }

            if let Some(candidate) = orient(first, second) {
                candidates.push(candidate);
            }
        }
    }

    candidates.sort_by(|left, right| {
        right
            .rank_score()
            .partial_cmp(&left.rank_score())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    debug!(
        eligible = eligible.len(),
        candidates = candidates.len(),
        "Pair screen complete"
    );
    candidates
}

/// Decides which leg is long and which is short, and applies the entry and agreement tests.
///
/// Both orientations are tried because ordinary least squares is not symmetric: the slope of `a` on
/// `b` is not the reciprocal of the slope of `b` on `a`, so the spread has to be fitted in the
/// orientation it will be held in rather than negated from the other one.
fn orient(first: &ScreenInput, second: &ScreenInput) -> Option<PairCandidate> {
    // `first` short, `second` long; then the reverse. At most one can clear a positive entry
    // threshold, since the two spreads move in opposite directions.
    for (long, short) in [(second, first), (first, second)] {
        if !short.is_shortable {
            continue;
        }
        let Some(model) = SpreadModel::fit(long.window(), short.window()) else {
            continue;
        };
        let Some(z_score) = model.z_score(long.price, short.price) else {
            continue;
        };
        if z_score < ENTRY_Z_SCORE {
            continue;
        }
        // Bounded above as well as below. Mean reversion is the premise of the position, and a
        // spread this stretched is more often an unadjusted corporate action or a regime break than
        // an opportunity.
        if z_score > ENTRY_Z_SCORE_CAP {
            debug!(
                long = %long.ticker,
                short = %short.ticker,
                z_score,
                cap = ENTRY_Z_SCORE_CAP,
                "Rejected a candidate whose entry spread is beyond the cap"
            );
            continue;
        }

        // The model has to agree that the cheap leg is the one to own. `PairCandidate::new`
        // enforces this too; checking here as well keeps the loop from silently falling through to
        // the other orientation on a rejection that is about the model rather than the spread.
        let signal_strength = long.expected_return - short.expected_return;
        if signal_strength <= 0.0 {
            continue;
        }

        return PairCandidate::new(
            PairID::new(long.ticker.clone(), short.ticker.clone()),
            model.hedge_ratio(),
            z_score,
            signal_strength,
            long.price,
            short.price,
        );
    }
    None
}

/// Greedily takes up to `limit` candidates that share no ticker with each other or with `held`,
/// and that keep every sector within [`MAXIMUM_LEGS_PER_SECTOR`].
///
/// Disjointness is why rejecting a candidate changes what is available below it: excluding one pair
/// frees both of its tickers, so a pair further down that was skipped for a collision becomes
/// selectable. Re-selecting is therefore not the same as taking the next item off the list. The
/// sector cap behaves the same way and for the same reason.
///
/// **The cap is seeded from the book, not from this pass.** `held` carries the legs already open,
/// so a sector at its limit stays at its limit rather than admitting a fresh allocation every time
/// the evaluator runs. A held ticker whose sector is unknown — a name whose `equity_details` row
/// went away, say — contributes to no sector rather than to a fabricated one; it still blocks
/// re-entry through `used`.
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

/// Builds the exit models for open pairs, keyed by pair identifier.
///
/// A pair whose legs are missing from `closes` gets no model and therefore no spread reading. The
/// caller treats that as "no exit signal", not as "hold forever" — the pre-close liquidation closes
/// it regardless, so the worst case is that it is held until 15:45 rather than exited on a signal.
pub fn exit_models<'a>(
    pairs: impl IntoIterator<Item = (&'a PairID, f64)>,
    closes: &HashMap<Ticker, Vec<f64>>,
) -> HashMap<PairID, SpreadModel> {
    let mut models = HashMap::new();
    for (pair_id, hedge_ratio) in pairs {
        let (Some(long_closes), Some(short_closes)) =
            (closes.get(pair_id.long()), closes.get(pair_id.short()))
        else {
            continue;
        };
        if let Some(model) = SpreadModel::with_hedge_ratio(hedge_ratio, long_closes, short_closes) {
            models.insert(pair_id.clone(), model);
        }
    }
    models
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Takes the natural logarithm of two series, returning `None` unless both are the same usable
/// length with no non-positive value.
fn aligned_logs(long_closes: &[f64], short_closes: &[f64]) -> Option<(Vec<f64>, Vec<f64>)> {
    if long_closes.len() != short_closes.len() || long_closes.len() < MINIMUM_SPREAD_OBSERVATIONS {
        return None;
    }
    let to_logs = |closes: &[f64]| -> Option<Vec<f64>> {
        closes
            .iter()
            .map(|close| (close.is_finite() && *close > 0.0).then(|| close.ln()))
            .collect()
    };
    Some((to_logs(long_closes)?, to_logs(short_closes)?))
}

/// Period-over-period log returns. One shorter than its input.
fn log_returns(prices: &[f64]) -> Vec<f64> {
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
fn pearson_correlation(left: &[f64], right: &[f64]) -> Option<f64> {
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
    /// Two failure modes are avoided deliberately. A series with no idiosyncratic component
    /// correlates at 1.0 and is rejected by `CORRELATION_MAXIMUM`; one whose spread has no variance
    /// is rejected by [`SpreadModel::build`]. Either produces zero candidates and every test built
    /// on it then asserts nothing, which is what
    /// `test_the_fixture_yields_at_least_one_candidate` guards.
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
        let correlation = pearson_correlation(&log_returns(&leader), &log_returns(&follower))
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

    // --- the spread model ---

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
        assert_eq!(SpreadModel::fit(&closes, &closes), None);
    }

    #[test]
    fn test_fit_rejects_misaligned_or_non_positive_series() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        assert_eq!(SpreadModel::fit(&long_closes[1..], &short_closes), None);

        let mut negative = long_closes.clone();
        negative[10] = -1.0;
        assert_eq!(SpreadModel::fit(&negative, &short_closes), None);
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
            ),
            None
        );
        assert!(SpreadModel::with_hedge_ratio(1.0, &long_closes, &short_closes).is_some());
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

    // --- statistics ---

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
    fn test_log_returns_is_one_shorter_than_its_input() {
        let returns = log_returns(&[100.0, 110.0, 121.0]);
        assert_eq!(returns.len(), 2);
        assert!((returns[0] - returns[1]).abs() < 1e-12);
    }

    // --- the screen ---

    /// The fixture has to actually produce pairs, or every assertion below it passes vacuously.
    /// This is the trap recorded in `statistical_arbitrage_test_fixtures`.
    #[test]
    fn test_the_fixture_yields_at_least_one_candidate() {
        assert!(!score_candidates(&screenable_inputs()).is_empty());
    }

    /// Three tickers in two sectors, with the short-leg candidate stretched away from its partner
    /// so the spread reads above the entry threshold.
    fn screenable_inputs() -> Vec<ScreenInput> {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        // 1.2%, not the 50% this used to stretch by. The generated series is near-deterministic —
        // its spread deviation is about 0.3%, two orders below a real pair's — so a 50% dislocation
        // scored z = 128, a value the screen would never see and the exit rule would close
        // instantly. Every test built on the fixture was therefore asserting against a candidate
        // that could not exist. This lands at z ~ 3.0, inside the band the screen actually admits.
        let stretched = follower.last().unwrap() * 1.012;
        vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), 0.03),
            input("BBBB", follower.clone(), stretched, -0.02),
        ]
    }

    /// Every candidate the screen emits must survive its own entry reading.
    ///
    /// The screen and the exit rule were each internally consistent and never checked against one
    /// another, which is how entries above the old absolute stop of 4.0 were admitted and then
    /// closed by the next pass. Composing the two here is the only place that gap is visible.
    #[test]
    fn test_no_candidate_is_closable_at_its_own_entry() {
        for candidate in score_candidates(&screenable_inputs()) {
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
    #[test]
    fn test_a_spread_beyond_the_cap_is_not_a_candidate() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        // Far enough out that the fitted spread cannot place it inside the cap.
        let dislocated = follower.last().unwrap() * 10.0;
        let inputs = vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), 0.03),
            input("BBBB", follower.clone(), dislocated, -0.02),
        ];
        for candidate in score_candidates(&inputs) {
            assert!(
                candidate.entry_z_score() <= ENTRY_Z_SCORE_CAP,
                "a dislocated spread at z={} cleared the cap",
                candidate.entry_z_score()
            );
        }
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

    /// Every entry score is positive by construction, which is what lets convergence be "falls to
    /// zero" and a stop be "rises past four" with no sign handling anywhere downstream.
    #[test]
    fn test_every_candidate_carries_a_positive_entry_score() {
        for candidate in score_candidates(&screenable_inputs()) {
            assert!(candidate.entry_z_score() > 0.0);
            assert!(candidate.signal_strength() > 0.0);
        }
    }

    /// The spread says buy AAAA and sell BBBB; the forecast says the opposite. Opening on that is a
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

        let correlation = pearson_correlation(&log_returns(&leader), &log_returns(&mirrored))
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
    /// This replaces a test asserting the reverse. A same-sector spread is the canonical statistical
    /// arbitrage trade, and within-sector correlation is where the `[0.5, 0.95]` band is most
    /// densely populated, so the old rule removed disproportionately many of the best candidates.
    ///
    /// Sector cannot even be *expressed* here any more: `ScreenInput` no longer carries one, because
    /// nothing in scoring reads it. That is the strongest statement of the change — the screen has
    /// no sector to consider. Concentration is bounded in `select_disjoint` instead, and the tests
    /// for it are below.
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

    #[test]
    fn test_a_leg_below_the_confidence_floor_is_ineligible() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.5;
        let inputs = vec![
            input("AAAA", leader.clone(), *leader.last().unwrap(), 0.03),
            ScreenInput::new(
                ticker("BBBB"),
                follower,
                stretched,
                -0.02,
                CONFIDENCE_FLOOR - 0.01,
                true,
            )
            .unwrap(),
        ];
        assert!(score_candidates(&inputs).is_empty());
    }

    #[test]
    fn test_screen_input_rejects_a_short_or_unusable_history() {
        assert!(ScreenInput::new(
            ticker("AAAA"),
            vec![100.0; CORRELATION_WINDOW_SESSIONS - 1],
            100.0,
            0.01,
            0.9,
            true
        )
        .is_none());

        let mut with_zero = vec![100.0; CORRELATION_WINDOW_SESSIONS];
        with_zero[3] = 0.0;
        assert!(ScreenInput::new(ticker("AAAA"), with_zero, 100.0, 0.01, 0.9, true).is_none());
    }

    // --- selection ---

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
            MAXIMUM_LEGS_PER_SECTOR / 2,
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
            MAXIMUM_LEGS_PER_SECTOR,
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

        assert_eq!(selected.len(), MAXIMUM_LEGS_PER_SECTOR / 2);
    }

    /// An empty sector map caps nothing.
    ///
    /// Recorded as a property of this function, not as a claim about the system: `build_screen_inputs`
    /// refuses a ticker with no sector before it can become a candidate, precisely so an unmeasurable
    /// name cannot slip past the cap. What this pins is that the *cap* is the only thing doing the
    /// capping — remove the upstream filter and concentration becomes unbounded, silently.
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

    #[test]
    fn test_candidate_rejects_a_backwards_orientation_or_a_contradicting_model() {
        let pair_id = PairID::new(ticker("AAAA"), ticker("BBBB"));
        assert_eq!(
            PairCandidate::new(pair_id.clone(), 1.0, -2.5, 0.02, 100.0, 100.0),
            None
        );
        assert_eq!(
            PairCandidate::new(pair_id.clone(), 1.0, 2.5, -0.02, 100.0, 100.0),
            None
        );
        assert_eq!(
            PairCandidate::new(pair_id, 1.0, 2.5, 0.02, 100.0, 0.0),
            None
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

    #[test]
    fn test_exit_models_skips_a_pair_whose_history_is_missing() {
        let (long_closes, short_closes) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let mut closes = HashMap::new();
        closes.insert(ticker("AAAA"), long_closes);
        closes.insert(ticker("BBBB"), short_closes);

        let present = PairID::new(ticker("AAAA"), ticker("BBBB"));
        let absent = PairID::new(ticker("AAAA"), ticker("ZZZZ"));
        let models = exit_models([(&present, 1.0), (&absent, 1.0)], &closes);

        assert!(models.contains_key(&present));
        assert!(!models.contains_key(&absent));
    }
}
