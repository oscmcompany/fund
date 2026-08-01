//! Pair selection: which two symbols, which way round, and how strong the signal.
//!
//! The screen is quadratic in the eligible universe, so everything cheap happens first: the
//! prediction confidence floor, the shortable check, and the different-sector rule all reduce the
//! input before a single correlation is computed.
//!
//! [`SpreadModel`] is the load-bearing type. Entry and exit both measure the spread through it, and
//! an open pair's model is rebuilt from its *stored* hedge ratio rather than refitted, so the
//! spread a pass compares against the exit threshold is the same spread the entry threshold was
//! applied to. Two code paths measuring the same quantity differently is the failure recorded in
//! `dual_path_signal_agreement`, and this is where it would recur.

use std::collections::{HashMap, HashSet};

use tracing::debug;

use crate::common::types::{PairID, Ticker};

/// Sessions of daily closes the correlation and the spread distribution are fitted over.
pub const CORRELATION_WINDOW_SESSIONS: usize = 60;

/// Correlation band a pair's log returns must fall inside.
///
/// The floor rejects pairs with no relationship to mean-revert to. The ceiling rejects pairs so
/// alike that the spread is mostly microstructure noise — two share classes of one issuer, or an
/// index fund and its largest holding — where the spread's standard deviation is small enough that
/// a two-sigma move is inside the bid-ask spread.
///
/// **The band is on the signed correlation, not its magnitude.** An anti-correlated pair fits a
/// negative hedge ratio, which turns `ln(short) - hedge_ratio * ln(long)` into a sum rather than a
/// difference — a quantity that hedges nothing. Sizing is dollar-neutral regardless of the ratio, so
/// admitting one produces a directional bet wearing the name of a market-neutral pair.
const CORRELATION_MINIMUM: f64 = 0.5;
const CORRELATION_MAXIMUM: f64 = 0.95;

/// Spread z-score at which a pair is worth opening.
pub const ENTRY_Z_SCORE: f64 = 2.0;

/// Spread z-score at which an open pair has converged and is closed at a profit.
///
/// Zero, not a band around zero: the spread is entered above `ENTRY_Z_SCORE` and closed when it
/// crosses back through its own mean, which is the move the position was taken to capture.
pub const CONVERGENCE_Z_SCORE: f64 = 0.0;

/// Spread z-score at which an open pair is stopped out.
///
/// Above the entry threshold, in the same direction, so this fires when the spread widened further
/// against the position rather than reverting.
pub const STOP_LOSS_Z_SCORE: f64 = 4.0;

/// Minimum model confidence for a ticker to be eligible for either leg.
pub const CONFIDENCE_FLOOR: f64 = 0.5;

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
    sector: String,
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
        sector: String,
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
            sector,
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

    pub fn sector(&self) -> &str {
        &self.sector
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
    /// This is the exit path. Refitting the hedge ratio here instead would mean the pass measures a
    /// different spread from the one the entry was taken on, and the position would be judged
    /// against a line it was never above.
    ///
    /// The window length is enforced here as well as the hedge ratio, and for the same reason.
    /// [`SpreadModel::fit`] is only ever called with exactly `CORRELATION_WINDOW_SESSIONS` closes,
    /// so a shorter series here yields a mean and standard deviation drawn from a different sample
    /// than the entry was measured against — and a z-score computed from it can cross the
    /// convergence or stop threshold for a spread that has not moved. Reusing the hedge ratio but
    /// not the window would reintroduce the asymmetry this type exists to prevent, one level down.
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
/// 1. Both legs clear the confidence floor, the short leg is shortable, and the two come from
///    different sectors.
/// 2. Their log returns correlate *positively*, within `[CORRELATION_MINIMUM, CORRELATION_MAXIMUM]`.
/// 3. The spread, oriented so the short leg is the expensive one, is at or above `ENTRY_Z_SCORE`.
/// 4. The model agrees with that orientation — it expects the long leg to out-return the short.
///
/// Test four is the model doing more than gating eligibility. Without it a pair can be opened whose
/// spread says buy A and sell B while the forecast says the opposite, and the two disagreements
/// cancel into a position with no thesis at all.
///
/// No disjointness constraint is applied: the returned list is the full reservoir and pairs within
/// it may share tickers. [`select_disjoint`] is the cheap second half, so a pass that rejects a
/// candidate can re-select without paying for the ranking again.
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

            // Different sectors. This is a constraint on the book rather than on the pair: a
            // reservoir of same-sector spreads ranks by the same industry factor at the top, and
            // ten such pairs is one bet held ten times.
            if first.sector == second.sector {
                continue;
            }
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

/// Greedily takes up to `limit` candidates that share no ticker with each other or with `held`.
///
/// Disjointness is why rejecting a candidate changes what is available below it: excluding one pair
/// frees both of its tickers, so a pair further down that was skipped for a collision becomes
/// selectable. Re-selecting is therefore not the same as taking the next item off the list.
pub fn select_disjoint(
    candidates: &[PairCandidate],
    limit: usize,
    held: &HashSet<Ticker>,
) -> Vec<PairCandidate> {
    if limit == 0 {
        return Vec::new();
    }

    let mut used: HashSet<Ticker> = held.clone();
    let mut selected: Vec<PairCandidate> = Vec::with_capacity(limit);

    for candidate in candidates {
        if selected.len() == limit {
            break;
        }
        if used.contains(candidate.long_ticker()) || used.contains(candidate.short_ticker()) {
            continue;
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
    /// Both legs are driven by a common factor; the follower carries an idiosyncratic component at
    /// a different frequency, which is what puts the correlation near 0.8 rather than at 1.0 and
    /// what gives the spread dispersion to revert within.
    ///
    /// Two failure modes are being avoided deliberately, both recorded in
    /// `statistical_arbitrage_test_fixtures`. A series with no idiosyncratic component correlates
    /// at 1.0 and is rejected by `CORRELATION_MAXIMUM`; one whose spread has no variance yields an
    /// infinite z-score. Either produces zero candidates, and every test built on it then passes
    /// while asserting nothing — which is why `test_the_fixture_yields_at_least_one_candidate`
    /// exists as a guard above the rest.
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

    fn input(
        name: &str,
        closes: Vec<f64>,
        price: f64,
        sector: &str,
        expected_return: f64,
    ) -> ScreenInput {
        ScreenInput::new(
            ticker(name),
            closes,
            price,
            sector.to_string(),
            expected_return,
            0.9,
            true,
        )
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
        let stretched = follower.last().unwrap() * 1.5;
        vec![
            input(
                "AAAA",
                leader.clone(),
                *leader.last().unwrap(),
                "Technology",
                0.03,
            ),
            input("BBBB", follower.clone(), stretched, "Utilities", -0.02),
        ]
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
            input(
                "AAAA",
                leader.clone(),
                *leader.last().unwrap(),
                "Technology",
                -0.02,
            ),
            input("BBBB", follower, stretched, "Utilities", 0.03),
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
            input(
                "AAAA",
                leader.clone(),
                *leader.last().unwrap(),
                "Technology",
                0.03,
            ),
            input("BBBB", mirrored, stretched, "Utilities", -0.02),
        ];
        assert!(score_candidates(&inputs).is_empty());
    }

    #[test]
    fn test_same_sector_pairs_are_rejected() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.5;
        let inputs = vec![
            input(
                "AAAA",
                leader.clone(),
                *leader.last().unwrap(),
                "Technology",
                0.03,
            ),
            input("BBBB", follower, stretched, "Technology", -0.02),
        ];
        assert!(score_candidates(&inputs).is_empty());
    }

    /// Without a shortable short leg there is no position to take, whatever the spread says.
    #[test]
    fn test_a_pair_whose_expensive_leg_cannot_be_shorted_is_rejected() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.5;
        let mut inputs = screenable_inputs();
        inputs[1] = ScreenInput::new(
            ticker("BBBB"),
            follower,
            stretched,
            "Utilities".to_string(),
            -0.02,
            0.9,
            false,
        )
        .unwrap();
        let _ = leader;
        assert!(score_candidates(&inputs).is_empty());
    }

    #[test]
    fn test_a_leg_below_the_confidence_floor_is_ineligible() {
        let (leader, follower) = cointegrated_series(CORRELATION_WINDOW_SESSIONS);
        let stretched = follower.last().unwrap() * 1.5;
        let inputs = vec![
            input(
                "AAAA",
                leader.clone(),
                *leader.last().unwrap(),
                "Technology",
                0.03,
            ),
            ScreenInput::new(
                ticker("BBBB"),
                follower,
                stretched,
                "Utilities".to_string(),
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
            "Technology".to_string(),
            0.01,
            0.9,
            true,
        )
        .is_none());

        let mut with_zero = vec![100.0; CORRELATION_WINDOW_SESSIONS];
        with_zero[3] = 0.0;
        assert!(ScreenInput::new(
            ticker("AAAA"),
            with_zero,
            100.0,
            "Technology".to_string(),
            0.01,
            0.9,
            true,
        )
        .is_none());
    }

    // --- selection ---

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
        let selected = select_disjoint(&candidates, 3, &HashSet::new());
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
        let selected = select_disjoint(&candidates, 3, &held);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].long_ticker().as_str(), "CCCC");
    }

    #[test]
    fn test_select_disjoint_respects_the_limit() {
        let candidates = vec![
            candidate("AAAA", "BBBB", 4.0),
            candidate("CCCC", "DDDD", 3.0),
        ];
        assert_eq!(select_disjoint(&candidates, 1, &HashSet::new()).len(), 1);
        assert!(select_disjoint(&candidates, 0, &HashSet::new()).is_empty());
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
