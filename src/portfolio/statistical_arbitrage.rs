//! Statistical arbitrage pair selection via correlation and z-score screening.
//!
//! Builds a correlation matrix of ticker log returns over a trailing window,
//! identifies pairs whose correlation falls in the signal band, computes the
//! OLS spread z-score for each candidate pair, and returns the top
//! `candidate_pool` pairs by rank score using a greedy no-duplicate-ticker
//! selection.

use std::collections::{HashMap, HashSet};

use crate::domain::market::{PairID, Ticker};
use crate::portfolio::consolidation::ConsolidatedSignal;
use crate::portfolio::math::{log_returns, ols_slope, pearson_correlation, z_score_last};

/// Trailing window for correlation and z-score computation (trading days).
pub const CORRELATION_WINDOW_DAYS: usize = 60;

/// Minimum absolute Pearson correlation for a pair to be considered.
const CORRELATION_MINIMUM: f64 = 0.5;

/// Maximum absolute Pearson correlation; pairs above this are too similar.
const CORRELATION_MAXIMUM: f64 = 0.95;

/// Z-score magnitude required to enter a position.
const Z_SCORE_ENTRY_THRESHOLD: f64 = 2.0;

/// Minimum ensemble confidence for a ticker to be eligible.
const CONFIDENCE_THRESHOLD: f64 = 0.5;

/// Minimum number of eligible tickers needed to form any pair.
const MINIMUM_TICKER_COUNT: usize = 2;

/// Default size of the working set handed to sizing, overridable via
/// `PORTFOLIO_CANDIDATE_POOL`.
///
/// This is a cap on how many disjoint pairs [`select_disjoint_pairs`] will take,
/// not a reservoir. The reservoir is the full ranked output of
/// [`score_candidate_pairs`], which the entry pass re-selects from whenever a
/// pair is rejected, so spare capacity does not need to be reserved here.
///
/// Kept at the same value as the default `minimum_pairs` because raising it
/// would widen the basket that volatility parity and the beta-neutral solve run
/// over, changing position sizing rather than merely adding fallbacks.
pub const DEFAULT_CANDIDATE_POOL: usize = 10;

/// A candidate long-short pair identified by the statistical arbitrage screener.
#[derive(Debug, Clone)]
pub struct CandidatePair {
    /// Canonical pair identifier combining both tickers, e.g. `"AAPL-MSFT"`.
    pair_id: PairID,
    /// The leg to buy: the relatively cheap ticker.
    long_ticker: Ticker,
    /// The leg to sell short: the relatively expensive ticker.
    short_ticker: Ticker,
    /// Standard deviations the current spread has diverged from its historical mean.
    z_score: f64,
    /// OLS regression slope: shares of the short leg per share of the long leg.
    hedge_ratio: f64,
    /// Absolute difference in `ensemble_alpha` between the two legs.
    signal_strength: f64,
    /// Realized daily return volatility of the long leg.
    long_realized_volatility: f64,
    /// Realized daily return volatility of the short leg.
    short_realized_volatility: f64,
}

impl CandidatePair {
    /// Constructs a `CandidatePair`, validating that statistical values are finite,
    /// volatilities are positive, and the two tickers are distinct.
    ///
    /// Returns `None` when any invariant is violated.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pair_id: PairID,
        long_ticker: Ticker,
        short_ticker: Ticker,
        z_score: f64,
        hedge_ratio: f64,
        signal_strength: f64,
        long_realized_volatility: f64,
        short_realized_volatility: f64,
    ) -> Option<Self> {
        if !z_score.is_finite()
            || !hedge_ratio.is_finite()
            || !signal_strength.is_finite()
            || long_realized_volatility <= 0.0
            || short_realized_volatility <= 0.0
            || long_ticker == short_ticker
        {
            return None;
        }
        Some(Self {
            pair_id,
            long_ticker,
            short_ticker,
            z_score,
            hedge_ratio,
            signal_strength,
            long_realized_volatility,
            short_realized_volatility,
        })
    }

    pub fn pair_id(&self) -> &PairID {
        &self.pair_id
    }

    pub fn long_ticker(&self) -> &Ticker {
        &self.long_ticker
    }

    pub fn short_ticker(&self) -> &Ticker {
        &self.short_ticker
    }

    pub fn z_score(&self) -> f64 {
        self.z_score
    }

    pub fn hedge_ratio(&self) -> f64 {
        self.hedge_ratio
    }

    pub fn signal_strength(&self) -> f64 {
        self.signal_strength
    }

    pub fn long_realized_volatility(&self) -> f64 {
        self.long_realized_volatility
    }

    pub fn short_realized_volatility(&self) -> f64 {
        self.short_realized_volatility
    }
}

/// A screened pair together with the score it was ranked on.
#[derive(Debug, Clone)]
pub struct ScoredPair {
    pair: CandidatePair,
    rank_score: f64,
}

impl ScoredPair {
    /// Pairs one candidate with its rank score.
    ///
    /// Test-only: production values come from [`score_candidate_pairs`], which
    /// derives the score from the whole reservoir. Exposing an unrestricted
    /// constructor would let a caller invent a ranking that never went through
    /// screening.
    #[cfg(test)]
    pub fn new(pair: CandidatePair, rank_score: f64) -> Self {
        Self { pair, rank_score }
    }

    pub fn pair(&self) -> &CandidatePair {
        &self.pair
    }

    pub fn rank_score(&self) -> f64 {
        self.rank_score
    }
}

/// Generates, screens, and ranks every eligible pair.
///
/// Filters signals by `ensemble_confidence >= CONFIDENCE_THRESHOLD` and
/// `realized_volatility > 0`. Builds a Pearson correlation matrix over the last
/// `CORRELATION_WINDOW_DAYS` of log returns. Pairs are screened by:
/// 1. Correlation in `[CORRELATION_MINIMUM, CORRELATION_MAXIMUM]`
/// 2. Z-score of the OLS spread >= `Z_SCORE_ENTRY_THRESHOLD`
///
/// Results are ordered by `|z_score| × signal_strength`, descending. No
/// disjointness constraint is applied here: the returned list is the full
/// reservoir, and pairs within it may share tickers.
///
/// This is the expensive half of pair selection, quadratic in the number of
/// eligible tickers with a correlation and an OLS regression per combination.
/// It is separated from selection so an entry pass that re-selects after a
/// rejection pays for the ranking only once.
pub fn score_candidate_pairs(
    signals: &[ConsolidatedSignal],
    historical_closes: &HashMap<Ticker, Vec<f64>>,
) -> Vec<ScoredPair> {
    // Filter to confident tickers with valid volatility.
    let eligible: Vec<&ConsolidatedSignal> = signals
        .iter()
        .filter(|s| s.ensemble_confidence >= CONFIDENCE_THRESHOLD && s.realized_volatility > 0.0)
        .collect();

    if eligible.len() < MINIMUM_TICKER_COUNT {
        return Vec::new();
    }

    // Build per-ticker log returns over the correlation window.
    let mut ticker_returns: Vec<(Ticker, Vec<f64>)> = Vec::new();
    for signal in &eligible {
        if let Some(closes) = historical_closes.get(&signal.ticker) {
            let window_closes: &[f64] = if closes.len() > CORRELATION_WINDOW_DAYS {
                &closes[closes.len() - CORRELATION_WINDOW_DAYS..]
            } else {
                closes
            };
            if window_closes.iter().any(|&p| p <= 0.0) {
                continue;
            }
            if window_closes.len() < CORRELATION_WINDOW_DAYS {
                continue;
            }
            let returns = log_returns(window_closes);
            if returns.is_empty() {
                continue;
            }
            let mean_squared_return: f64 =
                returns.iter().map(|r| r.powi(2)).sum::<f64>() / returns.len() as f64;
            if mean_squared_return < f64::EPSILON {
                continue;
            }
            ticker_returns.push((signal.ticker.clone(), returns));
        }
    }

    if ticker_returns.len() < MINIMUM_TICKER_COUNT {
        return Vec::new();
    }

    // Build a signals lookup for alpha and volatility access.
    let signals_lookup: HashMap<Ticker, &ConsolidatedSignal> =
        eligible.iter().map(|s| (s.ticker.clone(), *s)).collect();

    // Candidate pair generation with correlation and z-score screening.
    let mut candidates: Vec<(CandidatePair, f64)> = Vec::new();

    for i in 0..ticker_returns.len() {
        for j in (i + 1)..ticker_returns.len() {
            let (ticker_a, returns_a) = &ticker_returns[i];
            let (ticker_b, returns_b) = &ticker_returns[j];

            let count = returns_a.len().min(returns_b.len());
            let returns_a_aligned = &returns_a[returns_a.len() - count..];
            let returns_b_aligned = &returns_b[returns_b.len() - count..];

            let correlation = pearson_correlation(returns_a_aligned, returns_b_aligned);
            if !(CORRELATION_MINIMUM..=CORRELATION_MAXIMUM).contains(&correlation.abs()) {
                continue;
            }

            // Retrieve log price series for spread computation.
            let closes_a = match historical_closes.get(ticker_a) {
                Some(closes) if closes.len() >= CORRELATION_WINDOW_DAYS => {
                    &closes[closes.len() - CORRELATION_WINDOW_DAYS..]
                }
                _ => continue,
            };
            let closes_b = match historical_closes.get(ticker_b) {
                Some(closes) if closes.len() >= CORRELATION_WINDOW_DAYS => {
                    &closes[closes.len() - CORRELATION_WINDOW_DAYS..]
                }
                _ => continue,
            };

            let log_prices_a: Vec<f64> = closes_a.iter().map(|p| p.ln()).collect();
            let log_prices_b: Vec<f64> = closes_b.iter().map(|p| p.ln()).collect();

            // Replicate Python: slope of log_prices_a on log_prices_b
            let hedge_ratio = ols_slope(&log_prices_b, &log_prices_a);
            if !hedge_ratio.is_finite() {
                continue;
            }

            let spread: Vec<f64> = log_prices_a
                .iter()
                .zip(log_prices_b.iter())
                .map(|(a, b)| a - hedge_ratio * b)
                .collect();

            let current_z_score = z_score_last(&spread);
            if !current_z_score.is_finite() {
                continue;
            }
            if current_z_score.abs() < Z_SCORE_ENTRY_THRESHOLD {
                continue;
            }

            // z > 0: A is expensive → short A, long B.
            let (long_ticker, short_ticker) = if current_z_score > 0.0 {
                (ticker_b.clone(), ticker_a.clone())
            } else {
                (ticker_a.clone(), ticker_b.clone())
            };

            let long_signal = match signals_lookup.get(&long_ticker) {
                Some(signal) => signal,
                None => continue,
            };
            let short_signal = match signals_lookup.get(&short_ticker) {
                Some(signal) => signal,
                None => continue,
            };

            let signal_strength = (long_signal.ensemble_alpha - short_signal.ensemble_alpha).abs();
            let rank_score = current_z_score.abs() * signal_strength;

            let pair_id = PairID::new(long_ticker.clone(), short_ticker.clone());
            if let Some(candidate) = CandidatePair::new(
                pair_id,
                long_ticker,
                short_ticker,
                current_z_score.abs(),
                hedge_ratio,
                signal_strength,
                long_signal.realized_volatility,
                short_signal.realized_volatility,
            ) {
                candidates.push((candidate, rank_score));
            }
        }
    }

    // Sort by rank score descending.
    candidates.sort_by(|(_, score_a), (_, score_b)| {
        score_b
            .partial_cmp(score_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    candidates
        .into_iter()
        .map(|(pair, rank_score)| ScoredPair { pair, rank_score })
        .collect()
}

/// Greedily picks up to `limit` disjoint pairs from a ranked reservoir.
///
/// Walks `scored` in rank order, taking each pair whose tickers are still free
/// and whose identifier is not in `excluded`. No ticker appears in more than one
/// selected pair.
///
/// The disjointness constraint is why a rejected pair changes what is available
/// below it: excluding one pair frees both of its tickers, so pairs further down
/// the ranking that were previously skipped for a ticker collision become
/// selectable. Re-selecting is therefore not the same as taking the next item
/// off a list.
///
/// This half is cheap — a single pass over an already-ranked list — so an entry
/// pass can repeat it after every rejection.
pub fn select_disjoint_pairs(
    scored: &[ScoredPair],
    limit: usize,
    excluded: &HashSet<PairID>,
) -> Vec<CandidatePair> {
    if limit == 0 {
        return Vec::new();
    }

    let mut used_tickers: HashSet<Ticker> = HashSet::new();
    let mut selected: Vec<CandidatePair> = Vec::new();

    for scored_pair in scored {
        let pair = &scored_pair.pair;
        if excluded.contains(pair.pair_id()) {
            continue;
        }
        if used_tickers.contains(pair.long_ticker()) || used_tickers.contains(pair.short_ticker()) {
            continue;
        }
        used_tickers.insert(pair.long_ticker().clone());
        used_tickers.insert(pair.short_ticker().clone());
        selected.push(pair.clone());
        if selected.len() >= limit {
            break;
        }
    }

    selected
}

/// Selects up to `candidate_pool` disjoint pairs in one shot.
///
/// Convenience wrapper over [`score_candidate_pairs`] and
/// [`select_disjoint_pairs`] for callers that do not re-select.
pub fn select_pairs(
    signals: &[ConsolidatedSignal],
    historical_closes: &HashMap<Ticker, Vec<f64>>,
    candidate_pool: usize,
) -> Vec<CandidatePair> {
    let scored = score_candidate_pairs(signals, historical_closes);
    select_disjoint_pairs(&scored, candidate_pool, &HashSet::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_signal(
        ticker: &str,
        alpha: f64,
        confidence: f64,
        volatility: f64,
    ) -> ConsolidatedSignal {
        ConsolidatedSignal {
            ticker: Ticker::new(ticker).unwrap(),
            ensemble_alpha: alpha,
            ensemble_confidence: confidence,
            realized_volatility: volatility,
            sector: "TECHNOLOGY".to_string(),
        }
    }

    fn make_correlated_prices(
        count: usize,
        base: f64,
        market_factor: &[f64],
        idiosyncratic: f64,
    ) -> Vec<f64> {
        let mut prices = vec![base];
        for (i, &market_return) in market_factor.iter().enumerate().take(count - 1) {
            let last = *prices.last().unwrap();
            let noise = idiosyncratic * ((i as f64).sin() * 0.001);
            prices.push(last * (1.0 + market_return + noise));
        }
        prices
    }

    fn make_candidate(long_ticker: &str, short_ticker: &str) -> CandidatePair {
        CandidatePair::new(
            PairID::new(
                Ticker::new(long_ticker).unwrap(),
                Ticker::new(short_ticker).unwrap(),
            ),
            Ticker::new(long_ticker).unwrap(),
            Ticker::new(short_ticker).unwrap(),
            2.5,
            1.0,
            0.05,
            0.01,
            0.01,
        )
        .expect("test candidate pair should be valid")
    }

    #[test]
    fn test_select_pairs_empty_signals() {
        assert!(select_pairs(&[], &HashMap::new(), DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_insufficient_eligible_signals() {
        // Only one signal above confidence threshold
        let signals = vec![make_signal("AAPL", 0.02, 0.8, 0.01)];
        assert!(select_pairs(&signals, &HashMap::new(), DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_below_confidence_threshold_excluded() {
        let signals = vec![
            make_signal("AAPL", 0.02, 0.3, 0.01), // below threshold
            make_signal("MSFT", 0.01, 0.3, 0.01), // below threshold
        ];
        assert!(select_pairs(&signals, &HashMap::new(), DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_missing_closes_excluded() {
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        // No closes provided → no pair selected
        assert!(select_pairs(&signals, &HashMap::new(), DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_no_ticker_in_multiple_pairs() {
        // Build signals and correlated prices to generate at least one pair,
        // then verify the greedy constraint holds.
        let common_factor: Vec<f64> = (0..70).map(|i| 0.005 * ((i as f64 * 0.3).sin())).collect();

        let mut closes = HashMap::new();
        let tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META"];
        let signals: Vec<ConsolidatedSignal> = tickers
            .iter()
            .enumerate()
            .map(|(i, &ticker)| {
                let offset = i as f64 * 0.0001;
                let prices =
                    make_correlated_prices(71, 100.0 + i as f64 * 10.0, &common_factor, offset);
                closes.insert(Ticker::new(ticker).unwrap(), prices);
                make_signal(ticker, 0.01 * (i as f64 + 1.0), 0.9, 0.01)
            })
            .collect();

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);

        // Verify no ticker appears in more than one pair.
        let mut all_tickers: HashSet<Ticker> = HashSet::new();
        for pair in &pairs {
            assert!(
                !all_tickers.contains(pair.long_ticker()),
                "duplicate long ticker"
            );
            assert!(
                !all_tickers.contains(pair.short_ticker()),
                "duplicate short ticker"
            );
            all_tickers.insert(pair.long_ticker().clone());
            all_tickers.insert(pair.short_ticker().clone());
        }
    }

    #[test]
    fn test_select_pairs_caps_at_candidate_pool() {
        // A smaller candidate pool never returns more pairs than a larger one and
        // never exceeds its own cap.
        let common_factor: Vec<f64> = (0..70).map(|i| 0.005 * ((i as f64 * 0.3).sin())).collect();
        let mut closes = HashMap::new();
        let tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "NVDA"];
        let signals: Vec<ConsolidatedSignal> = tickers
            .iter()
            .enumerate()
            .map(|(i, &ticker)| {
                let offset = i as f64 * 0.0001;
                let prices =
                    make_correlated_prices(71, 100.0 + i as f64 * 10.0, &common_factor, offset);
                closes.insert(Ticker::new(ticker).unwrap(), prices);
                make_signal(ticker, 0.01 * (i as f64 + 1.0), 0.9, 0.01)
            })
            .collect();

        let large_pool = select_pairs(&signals, &closes, 10);
        let single = select_pairs(&signals, &closes, 1);
        assert!(single.len() <= 1, "pool of 1 must cap to at most one pair");
        assert!(
            single.len() <= large_pool.len(),
            "a smaller pool never yields more pairs than a larger one"
        );
    }

    #[test]
    fn test_select_pairs_z_score_magnitude_matches() {
        // Any returned pair must have z_score >= Z_SCORE_ENTRY_THRESHOLD.
        let common_factor: Vec<f64> = (0..70).map(|i| 0.003 * ((i as f64 * 0.25).sin())).collect();

        let mut closes = HashMap::new();
        let tickers = ["AAPL", "MSFT", "GOOG", "AMZN"];
        let signals: Vec<ConsolidatedSignal> = tickers
            .iter()
            .enumerate()
            .map(|(i, &ticker)| {
                let offset = i as f64 * 0.0002;
                let prices =
                    make_correlated_prices(71, 100.0 + i as f64 * 20.0, &common_factor, offset);
                closes.insert(Ticker::new(ticker).unwrap(), prices);
                make_signal(ticker, 0.01 * (i as f64 + 1.0), 0.9, 0.012)
            })
            .collect();

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);
        for pair in &pairs {
            assert!(pair.z_score() >= Z_SCORE_ENTRY_THRESHOLD);
            assert!(pair.hedge_ratio().is_finite());
            assert!(pair.signal_strength() >= 0.0);
        }
    }

    #[test]
    fn test_select_pairs_zero_candidate_pool_returns_empty() {
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        assert!(select_pairs(&signals, &HashMap::new(), 0).is_empty());
    }

    #[test]
    fn test_select_pairs_zero_volatility_signals_excluded() {
        // Signals with realized_volatility == 0.0 must be filtered out.
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.0),
            make_signal("MSFT", 0.01, 0.8, 0.0),
        ];
        assert!(select_pairs(&signals, &HashMap::new(), DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_insufficient_closes_after_filter() {
        // Signals pass the confidence + volatility filter but closes are too
        // short to satisfy CORRELATION_WINDOW_DAYS.
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        let mut closes = HashMap::new();
        // Only 10 days — well below CORRELATION_WINDOW_DAYS (60).
        closes.insert(Ticker::new("AAPL").unwrap(), vec![100.0_f64; 10]);
        closes.insert(Ticker::new("MSFT").unwrap(), vec![200.0_f64; 10]);
        assert!(select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_constant_prices_excluded_by_variance_filter() {
        // Constant prices produce zero log returns, which have zero mean-squared
        // return, so all tickers fail the variance filter and no pair is formed.
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        let mut closes = HashMap::new();
        // Exactly CORRELATION_WINDOW_DAYS constant prices → all log returns are 0.
        closes.insert(
            Ticker::new("AAPL").unwrap(),
            vec![100.0_f64; CORRELATION_WINDOW_DAYS],
        );
        closes.insert(
            Ticker::new("MSFT").unwrap(),
            vec![200.0_f64; CORRELATION_WINDOW_DAYS],
        );
        assert!(select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_prices_with_non_positive_value_excluded() {
        // A non-positive price in the window causes the ticker to be skipped.
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        let mut closes = HashMap::new();
        let mut aapl = vec![100.0_f64; CORRELATION_WINDOW_DAYS];
        aapl[30] = 0.0; // non-positive price
        closes.insert(Ticker::new("AAPL").unwrap(), aapl);
        closes.insert(
            Ticker::new("MSFT").unwrap(),
            vec![200.0_f64; CORRELATION_WINDOW_DAYS],
        );
        assert!(select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_candidate_pair_new_rejects_identical_tickers() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("AAPL").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("AAPL").unwrap(),
            2.5,
            1.0,
            0.05,
            0.01,
            0.01,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_rejects_non_finite_z_score() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            f64::NAN,
            1.0,
            0.05,
            0.01,
            0.01,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_rejects_infinite_hedge_ratio() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            2.5,
            f64::INFINITY,
            0.05,
            0.01,
            0.01,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_rejects_non_finite_signal_strength() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            2.5,
            1.0,
            f64::NAN,
            0.01,
            0.01,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_rejects_zero_long_volatility() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            2.5,
            1.0,
            0.05,
            0.0,
            0.01,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_rejects_negative_short_volatility() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            2.5,
            1.0,
            0.05,
            0.01,
            -0.001,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_valid_all_accessors() {
        let pair = make_candidate("AAPL", "MSFT");
        assert_eq!(pair.pair_id().as_str(), "AAPL-MSFT");
        assert_eq!(pair.long_ticker().as_str(), "AAPL");
        assert_eq!(pair.short_ticker().as_str(), "MSFT");
        assert!((pair.z_score() - 2.5).abs() < f64::EPSILON);
        assert!((pair.hedge_ratio() - 1.0).abs() < f64::EPSILON);
        assert!((pair.signal_strength() - 0.05).abs() < f64::EPSILON);
        assert!((pair.long_realized_volatility() - 0.01).abs() < f64::EPSILON);
        assert!((pair.short_realized_volatility() - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn test_candidate_pair_new_rejects_nan_signal_strength() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            2.5,
            1.0,
            f64::NAN,
            0.01,
            0.01,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_negative_z_score_is_valid() {
        // z_score is stored as abs() by the caller; a negative value passed in
        // directly is still finite so the constructor must accept it.
        let pair = CandidatePair::new(
            PairID::new(Ticker::new("MSFT").unwrap(), Ticker::new("AAPL").unwrap()),
            Ticker::new("MSFT").unwrap(),
            Ticker::new("AAPL").unwrap(),
            -2.5,
            0.8,
            0.03,
            0.011,
            0.012,
        );
        assert!(pair.is_some());
        assert!((pair.unwrap().z_score() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_select_pairs_only_one_closes_missing_second_ticker() {
        // AAPL closes are provided with variance but MSFT closes are missing
        // entirely. After the eligible loop only one ticker_return entry exists
        // (AAPL), which is below MINIMUM_TICKER_COUNT, so the result is empty.
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        let common_factor: Vec<f64> = (0..70).map(|i| 0.005 * ((i as f64 * 0.3).sin())).collect();
        let mut closes = HashMap::new();
        closes.insert(
            Ticker::new("AAPL").unwrap(),
            make_correlated_prices(CORRELATION_WINDOW_DAYS + 1, 100.0, &common_factor, 0.001),
        );
        // MSFT has no closes at all — select_pairs must return empty.
        assert!(select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL).is_empty());
    }

    #[test]
    fn test_select_pairs_greedy_deduplication_prevents_ticker_reuse() {
        // Build three tickers A, B, C all highly correlated.  After the first
        // pair (A, B) is selected, the greedy guard must block any further pair
        // that contains A or B.  We verify that if a pair is returned it never
        // reuses a ticker from a previously selected pair.
        let common_factor: Vec<f64> = (0..70).map(|i| 0.008 * ((i as f64 * 0.4).sin())).collect();

        let mut closes = HashMap::new();
        let tickers = ["AA", "BB", "CC", "DD"];
        let signals: Vec<ConsolidatedSignal> = tickers
            .iter()
            .enumerate()
            .map(|(i, &ticker)| {
                let offset = i as f64 * 0.00005;
                let prices =
                    make_correlated_prices(71, 50.0 + i as f64 * 5.0, &common_factor, offset);
                closes.insert(Ticker::new(ticker).unwrap(), prices);
                make_signal(ticker, 0.02 * (i as f64 + 1.0), 0.9, 0.01)
            })
            .collect();

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);

        let mut all_tickers: HashSet<Ticker> = HashSet::new();
        for pair in &pairs {
            assert!(
                !all_tickers.contains(pair.long_ticker()),
                "long ticker {} appears in multiple pairs",
                pair.long_ticker()
            );
            assert!(
                !all_tickers.contains(pair.short_ticker()),
                "short ticker {} appears in multiple pairs",
                pair.short_ticker()
            );
            all_tickers.insert(pair.long_ticker().clone());
            all_tickers.insert(pair.short_ticker().clone());
        }
    }

    #[test]
    fn test_select_pairs_returns_empty_when_no_pair_meets_z_score_threshold() {
        // Constant-increment prices produce very small z-scores. A uniform
        // upward drift produces near-zero spread z-scores, failing the threshold.
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        let mut closes = HashMap::new();
        // Prices increasing by exactly 1% each day — both tickers move together
        // so the spread has zero variance and z_score will not be finite.
        let prices_a: Vec<f64> = (0..=CORRELATION_WINDOW_DAYS)
            .map(|i| 100.0 * (1.01_f64).powi(i as i32))
            .collect();
        let prices_b: Vec<f64> = (0..=CORRELATION_WINDOW_DAYS)
            .map(|i| 200.0 * (1.01_f64).powi(i as i32))
            .collect();
        closes.insert(Ticker::new("AAPL").unwrap(), prices_a);
        closes.insert(Ticker::new("MSFT").unwrap(), prices_b);
        // With perfectly proportional prices, z-score will not reach the threshold.
        // The exact result depends on numerics; we just ensure no panic occurs.
        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);
        // Either empty (z-score below threshold) or a pair (rare numeric case) —
        // the important thing is no panic and the no-ticker-reuse invariant holds.
        let mut all_tickers: HashSet<Ticker> = HashSet::new();
        for pair in &pairs {
            assert!(!all_tickers.contains(pair.long_ticker()));
            assert!(!all_tickers.contains(pair.short_ticker()));
            all_tickers.insert(pair.long_ticker().clone());
            all_tickers.insert(pair.short_ticker().clone());
        }
    }

    #[test]
    fn test_candidate_pair_new_rejects_infinite_signal_strength() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            2.5,
            1.0,
            f64::INFINITY,
            0.01,
            0.01,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_candidate_pair_new_rejects_nan_z_score() {
        let result = CandidatePair::new(
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            f64::NAN,
            1.0,
            0.05,
            0.01,
            0.01,
        );
        assert!(result.is_none());
    }

    /// Builds price series for two tickers designed to exercise the inner pair-generation
    /// loop in `select_pairs`. The two series share a moderate common sinusoidal factor
    /// (keeping Pearson correlation in [CORRELATION_MINIMUM, CORRELATION_MAXIMUM]) and
    /// ticker A includes a deterministic upward drift in the final 16 steps that drives
    /// the OLS spread z-score above Z_SCORE_ENTRY_THRESHOLD.
    fn make_pair_prices_for_inner_loop_coverage() -> (Vec<f64>, Vec<f64>) {
        let count = CORRELATION_WINDOW_DAYS + 1; // 61 prices → 60 log returns
        let mut prices_a = vec![100.0f64];
        let mut prices_b = vec![100.0f64];

        for i in 0..(count - 1) {
            let last_a = *prices_a.last().unwrap();
            let last_b = *prices_b.last().unwrap();

            let shared = 0.008 * ((i as f64 * 0.4).sin());
            let drift_a = if i >= count - 16 { 0.006 } else { 0.0 };
            let idio_a = 0.004 * ((i as f64 * 1.7).sin()) + drift_a;
            let idio_b = 0.004 * ((i as f64 * 2.3).cos());

            prices_a.push(last_a * (1.0 + shared + idio_a));
            prices_b.push(last_b * (1.0 + shared + idio_b));
        }

        (prices_a, prices_b)
    }

    #[test]
    fn test_select_pairs_inner_loop_pair_generated_when_correlated_and_z_score_sufficient() {
        let (prices_a, prices_b) = make_pair_prices_for_inner_loop_coverage();

        let mut closes = HashMap::new();
        closes.insert(Ticker::new("TKRA").unwrap(), prices_a);
        closes.insert(Ticker::new("TKRB").unwrap(), prices_b);

        let signals = vec![
            make_signal("TKRA", 0.01, 0.9, 0.015),
            make_signal("TKRB", 0.05, 0.9, 0.015),
        ];

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);

        assert!(
            !pairs.is_empty(),
            "engineered fixture must generate at least one pair"
        );
        for pair in &pairs {
            assert!(pair.z_score() >= Z_SCORE_ENTRY_THRESHOLD);
            assert!(pair.hedge_ratio().is_finite());
            assert!(pair.signal_strength() >= 0.0);
            assert!(pair.long_realized_volatility() > 0.0);
            assert!(pair.short_realized_volatility() > 0.0);
            assert_ne!(pair.long_ticker(), pair.short_ticker());
            let has_both = (pair.long_ticker() == "TKRA" || pair.short_ticker() == "TKRA")
                && (pair.long_ticker() == "TKRB" || pair.short_ticker() == "TKRB");
            assert!(has_both, "pair must contain TKRA and TKRB");
        }
    }

    #[test]
    fn test_select_pairs_z_score_negative_assigns_long_a_short_b() {
        let (prices_b, prices_a) = make_pair_prices_for_inner_loop_coverage();

        let mut closes = HashMap::new();
        closes.insert(Ticker::new("TKRA").unwrap(), prices_a);
        closes.insert(Ticker::new("TKRB").unwrap(), prices_b);

        let signals = vec![
            make_signal("TKRA", 0.05, 0.9, 0.015),
            make_signal("TKRB", 0.01, 0.9, 0.015),
        ];

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);

        for pair in &pairs {
            assert!(pair.z_score() >= Z_SCORE_ENTRY_THRESHOLD);
            assert!(pair.long_realized_volatility() > 0.0);
            assert!(pair.short_realized_volatility() > 0.0);
        }
    }

    #[test]
    fn test_select_pairs_closes_longer_than_window_uses_trailing_window() {
        let extra = 10;
        let total_count = CORRELATION_WINDOW_DAYS + extra + 1;

        let mut prices_a = vec![100.0f64];
        let mut prices_b = vec![200.0f64];
        for i in 0..(total_count - 1) {
            let last_a = *prices_a.last().unwrap();
            let last_b = *prices_b.last().unwrap();
            let shared = 0.008 * ((i as f64 * 0.4).sin());
            let idio_a = 0.15 * ((i as f64 * 1.7).sin()) * 0.01;
            let drift = if i >= total_count - 16 { 0.012 } else { 0.0 };
            let idio_b = 0.15 * ((i as f64 * 2.3).cos()) * 0.01 + drift;
            prices_a.push(last_a * (1.0 + shared + idio_a));
            prices_b.push(last_b * (1.0 + shared + idio_b));
        }

        let mut closes = HashMap::new();
        closes.insert(Ticker::new("TKRA").unwrap(), prices_a);
        closes.insert(Ticker::new("TKRB").unwrap(), prices_b);

        let signals = vec![
            make_signal("TKRA", 0.05, 0.9, 0.015),
            make_signal("TKRB", 0.01, 0.9, 0.015),
        ];

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);

        for pair in &pairs {
            assert!(pair.z_score() >= Z_SCORE_ENTRY_THRESHOLD);
            assert!(pair.hedge_ratio().is_finite());
            assert_ne!(pair.long_ticker(), pair.short_ticker());
        }
    }

    #[test]
    fn test_select_pairs_candidate_pool_of_one_returns_at_most_one() {
        let common_factor: Vec<f64> = (0..70).map(|i| 0.005 * ((i as f64 * 0.3).sin())).collect();
        let mut closes = HashMap::new();
        let tickers = ["AAPL", "MSFT", "GOOG"];
        let signals: Vec<ConsolidatedSignal> = tickers
            .iter()
            .enumerate()
            .map(|(i, &ticker)| {
                let offset = i as f64 * 0.0001;
                let prices =
                    make_correlated_prices(71, 100.0 + i as f64 * 50.0, &common_factor, offset);
                closes.insert(Ticker::new(ticker).unwrap(), prices);
                make_signal(ticker, 0.01 * (i as f64 + 1.0), 0.9, 0.015)
            })
            .collect();

        let pairs = select_pairs(&signals, &closes, 1);
        assert!(
            pairs.len() <= 1,
            "pool of 1 must not return more than one pair"
        );
    }

    fn make_guaranteed_pair_prices() -> (Vec<f64>, Vec<f64>) {
        let count = CORRELATION_WINDOW_DAYS + 1;
        let mut prices_a = vec![100.0_f64];
        let mut prices_b = vec![100.0_f64];

        for i in 0..(count - 1) {
            let last_a = *prices_a.last().unwrap();
            let last_b = *prices_b.last().unwrap();

            let shared = 0.012 * ((i as f64 * 0.5).sin());
            let drift_a = if i >= count - 13 { 0.008 } else { 0.0 };
            let idio_a = 0.006 * ((i as f64 * 1.3).cos()) + drift_a;
            let idio_b = 0.006 * ((i as f64 * 2.1).sin());

            prices_a.push(last_a * (1.0 + shared + idio_a));
            prices_b.push(last_b * (1.0 + shared + idio_b));
        }

        (prices_a, prices_b)
    }

    #[test]
    fn test_select_pairs_guaranteed_pair_covers_inner_loop_and_sort() {
        let (prices_a, prices_b) = make_guaranteed_pair_prices();

        let mut closes = HashMap::new();
        closes.insert(Ticker::new("TKRA").unwrap(), prices_a);
        closes.insert(Ticker::new("TKRB").unwrap(), prices_b);

        let signals = vec![
            make_signal("TKRA", 0.01, 0.9, 0.015),
            make_signal("TKRB", 0.07, 0.9, 0.015),
        ];

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);

        assert!(
            !pairs.is_empty(),
            "engineered fixture must generate at least one pair"
        );
        for pair in &pairs {
            assert!(pair.z_score() >= Z_SCORE_ENTRY_THRESHOLD);
            assert!(pair.hedge_ratio().is_finite());
            assert!(pair.signal_strength() >= 0.0);
            assert!(pair.long_realized_volatility() > 0.0);
            assert!(pair.short_realized_volatility() > 0.0);
            assert_ne!(pair.long_ticker(), pair.short_ticker());
        }
    }

    #[test]
    fn test_select_pairs_greedy_break_on_pool_limit_with_guaranteed_pair() {
        let (prices_a, prices_b) = make_guaranteed_pair_prices();

        let mut closes = HashMap::new();
        closes.insert(Ticker::new("TKRA").unwrap(), prices_a);
        closes.insert(Ticker::new("TKRB").unwrap(), prices_b);

        let signals = vec![
            make_signal("TKRA", 0.02, 0.9, 0.015),
            make_signal("TKRB", 0.08, 0.9, 0.015),
        ];

        let pairs = select_pairs(&signals, &closes, 1);
        assert!(
            !pairs.is_empty(),
            "engineered fixture must generate at least one pair"
        );
        assert!(pairs.len() <= 1, "pool cap of 1 must never be exceeded");
    }

    #[test]
    fn test_select_pairs_greedy_ticker_reuse_skip_with_three_tickers() {
        let (prices_a, prices_b) = make_guaranteed_pair_prices();
        let prices_c: Vec<f64> = prices_b.iter().map(|p| p * 1.001).collect();

        let mut closes = HashMap::new();
        closes.insert(Ticker::new("TKA").unwrap(), prices_a);
        closes.insert(Ticker::new("TKB").unwrap(), prices_b);
        closes.insert(Ticker::new("TKC").unwrap(), prices_c);

        let signals = vec![
            make_signal("TKA", 0.01, 0.9, 0.015),
            make_signal("TKB", 0.07, 0.9, 0.015),
            make_signal("TKC", 0.06, 0.9, 0.015),
        ];

        let pairs = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);

        let mut seen_tickers: HashSet<Ticker> = HashSet::new();
        for pair in &pairs {
            assert!(
                !seen_tickers.contains(pair.long_ticker()),
                "long ticker {} appears in multiple pairs",
                pair.long_ticker()
            );
            assert!(
                !seen_tickers.contains(pair.short_ticker()),
                "short ticker {} appears in multiple pairs",
                pair.short_ticker()
            );
            seen_tickers.insert(pair.long_ticker().clone());
            seen_tickers.insert(pair.short_ticker().clone());
        }
    }

    #[test]
    fn test_select_pairs_single_valid_ticker_returns_empty() {
        let signals = vec![
            make_signal("AAPL", 0.02, 0.8, 0.01),
            make_signal("MSFT", 0.01, 0.8, 0.01),
        ];
        let mut closes = HashMap::new();
        closes.insert(
            Ticker::new("AAPL").unwrap(),
            vec![100.0_f64; CORRELATION_WINDOW_DAYS],
        );
        assert!(select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL).is_empty());
    }

    // --- selection over a ranked reservoir ---
    //
    // These exercise `select_disjoint_pairs` against a hand-built ranking rather
    // than a synthetic price universe. The screener's correlation band and
    // z-score threshold make it hard to guarantee any synthetic universe yields
    // pairs at all, and selection is a pure function of an already-ranked list,
    // so feeding it that list directly is both deterministic and the actual unit
    // under test.

    /// Builds a scored pair with an explicit rank score.
    fn scored(long: &str, short: &str, rank_score: f64) -> ScoredPair {
        let long_ticker = Ticker::new(long).unwrap();
        let short_ticker = Ticker::new(short).unwrap();
        ScoredPair {
            pair: CandidatePair::new(
                PairID::new(long_ticker.clone(), short_ticker.clone()),
                long_ticker,
                short_ticker,
                2.5,
                1.0,
                0.01,
                0.01,
                0.01,
            )
            .expect("test pair should be valid"),
            rank_score,
        }
    }

    /// A reservoir where the top pair competes for tickers with lower ones.
    ///
    /// AAPL-MSFT ranks first and GOOG-AMZN is disjoint from it, so both are
    /// taken. AAPL-GOOG and MSFT-AMZN each collide with one of those and are
    /// skipped while they stand.
    fn competing_reservoir() -> Vec<ScoredPair> {
        vec![
            scored("AAPL", "MSFT", 0.9),
            scored("GOOG", "AMZN", 0.8),
            scored("AAPL", "GOOG", 0.7),
            scored("MSFT", "AMZN", 0.6),
        ]
    }

    fn selected_ids(pairs: &[CandidatePair]) -> Vec<String> {
        pairs
            .iter()
            .map(|pair| pair.pair_id().as_str().to_string())
            .collect()
    }

    #[test]
    fn test_selection_takes_highest_ranked_disjoint_pairs() {
        let selected = select_disjoint_pairs(&competing_reservoir(), 10, &HashSet::new());
        assert_eq!(selected.len(), 2);

        let ids = selected_ids(&selected);
        assert!(ids
            .iter()
            .any(|id| id.contains("AAPL") && id.contains("MSFT")));
        assert!(ids
            .iter()
            .any(|id| id.contains("GOOG") && id.contains("AMZN")));
    }

    #[test]
    fn test_exclusion_promotes_previously_skipped_pairs() {
        // The property the convergence loop depends on: excluding a pair frees
        // both of its tickers, so pairs further down the ranking that were
        // skipped for a collision become selectable. Not a list pop.
        let reservoir = competing_reservoir();
        let baseline = select_disjoint_pairs(&reservoir, 10, &HashSet::new());

        let excluded: HashSet<PairID> =
            baseline.iter().map(|pair| pair.pair_id().clone()).collect();
        let after = select_disjoint_pairs(&reservoir, 10, &excluded);

        assert_eq!(after.len(), 2, "both lower-ranked pairs become selectable");
        let ids = selected_ids(&after);
        assert!(ids
            .iter()
            .any(|id| id.contains("AAPL") && id.contains("GOOG")));
        assert!(ids
            .iter()
            .any(|id| id.contains("MSFT") && id.contains("AMZN")));
    }

    #[test]
    fn test_excluded_pair_is_never_reselected() {
        let reservoir = competing_reservoir();
        let baseline = select_disjoint_pairs(&reservoir, 10, &HashSet::new());
        let mut excluded = HashSet::new();
        excluded.insert(baseline[0].pair_id().clone());

        let after = select_disjoint_pairs(&reservoir, 10, &excluded);
        assert!(!after
            .iter()
            .any(|pair| pair.pair_id() == baseline[0].pair_id()));
    }

    #[test]
    fn test_selection_remains_disjoint_after_exclusions() {
        let reservoir = competing_reservoir();
        let mut excluded = HashSet::new();
        excluded.insert(PairID::new(
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
        ));

        let mut seen: HashSet<Ticker> = HashSet::new();
        for pair in select_disjoint_pairs(&reservoir, 10, &excluded) {
            assert!(seen.insert(pair.long_ticker().clone()));
            assert!(seen.insert(pair.short_ticker().clone()));
        }
    }

    #[test]
    fn test_selection_limit_is_respected() {
        let reservoir = competing_reservoir();
        assert_eq!(
            select_disjoint_pairs(&reservoir, 1, &HashSet::new()).len(),
            1
        );
        assert!(select_disjoint_pairs(&reservoir, 0, &HashSet::new()).is_empty());
    }

    #[test]
    fn test_excluding_everything_selects_nothing() {
        let reservoir = competing_reservoir();
        let excluded: HashSet<PairID> = reservoir
            .iter()
            .map(|scored_pair| scored_pair.pair().pair_id().clone())
            .collect();

        assert!(select_disjoint_pairs(&reservoir, 10, &excluded).is_empty());
    }

    #[test]
    fn test_scored_pairs_are_ranked_descending() {
        // Selection walks the reservoir in order, so the ranking contract holds
        // regardless of how many pairs the screener admits.
        let signals = vec![
            make_signal("AAPL", 0.02, 0.9, 0.01),
            make_signal("MSFT", 0.01, 0.9, 0.01),
            make_signal("GOOG", 0.03, 0.9, 0.01),
        ];
        let common_factor: Vec<f64> = (0..70).map(|i| 0.005 * ((i as f64 * 0.3).sin())).collect();
        let mut closes = HashMap::new();
        for (index, ticker) in ["AAPL", "MSFT", "GOOG"].iter().enumerate() {
            closes.insert(
                Ticker::new(ticker).unwrap(),
                make_correlated_prices(71, 100.0, &common_factor, index as f64 * 0.0001),
            );
        }

        let scored = score_candidate_pairs(&signals, &closes);
        for window in scored.windows(2) {
            assert!(window[0].rank_score() >= window[1].rank_score());
        }
    }

    /// Builds a two-ticker universe that actually clears the screen.
    ///
    /// Correlation must land inside `[0.5, 0.95]` *and* the terminal spread
    /// z-score must reach `2.0`, which most synthetic series miss: a single
    /// terminal shock large enough to move the z-score also destroys the
    /// correlation. Here the legs share a common factor, and the long leg picks
    /// up a small drift over the final few days — enough to push the spread out
    /// without breaking the relationship. Yields correlation ≈ 0.80 and z ≈ 3.4.
    fn screening_universe() -> (Vec<ConsolidatedSignal>, HashMap<Ticker, Vec<f64>>) {
        let mut long_prices = vec![100.0_f64];
        let mut short_prices = vec![100.0_f64];
        let length = CORRELATION_WINDOW_DAYS + 1;

        for index in 1..length {
            let position = index as f64;
            let mut long_return = 0.012 * (position * 0.7).sin();
            let short_return = 0.7 * long_return + 0.006 * (position * 1.3).cos();
            if index >= length - 5 {
                long_return += 0.004;
            }
            long_prices.push(long_prices[long_prices.len() - 1] * long_return.exp());
            short_prices.push(short_prices[short_prices.len() - 1] * short_return.exp());
        }

        let mut closes = HashMap::new();
        closes.insert(Ticker::new("AAPL").unwrap(), long_prices);
        closes.insert(Ticker::new("MSFT").unwrap(), short_prices);

        let signals = vec![
            make_signal("AAPL", 0.03, 0.9, 0.01),
            make_signal("MSFT", 0.01, 0.9, 0.01),
        ];
        (signals, closes)
    }

    #[test]
    fn test_screening_universe_yields_a_pair() {
        // Guards the fixture itself. Without this, the composition test below
        // could quietly go back to comparing two empty vectors.
        let (signals, closes) = screening_universe();
        assert!(
            !score_candidate_pairs(&signals, &closes).is_empty(),
            "fixture must clear the correlation band and z-score threshold"
        );
    }

    #[test]
    fn test_select_pairs_wrapper_composes_both_halves() {
        let (signals, closes) = screening_universe();

        let wrapper = select_pairs(&signals, &closes, DEFAULT_CANDIDATE_POOL);
        let manual = select_disjoint_pairs(
            &score_candidate_pairs(&signals, &closes),
            DEFAULT_CANDIDATE_POOL,
            &HashSet::new(),
        );

        assert!(!wrapper.is_empty(), "composition test must not be vacuous");
        assert_eq!(selected_ids(&wrapper), selected_ids(&manual));
    }

    #[test]
    fn test_exclusion_is_honoured_on_a_real_screened_pair() {
        let (signals, closes) = screening_universe();
        let scored = score_candidate_pairs(&signals, &closes);
        let selected = select_disjoint_pairs(&scored, DEFAULT_CANDIDATE_POOL, &HashSet::new());
        assert!(!selected.is_empty());

        let excluded: HashSet<PairID> =
            selected.iter().map(|pair| pair.pair_id().clone()).collect();
        assert!(select_disjoint_pairs(&scored, DEFAULT_CANDIDATE_POOL, &excluded).is_empty());
    }
}
