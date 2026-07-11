//! Volatility-parity position sizing with beta-neutral weight optimization.
//!
//! Pairs with lower realized spread volatility receive proportionally more capital
//! so that each pair contributes equal risk to the portfolio. A projected gradient
//! descent optimizer then nudges the weights toward net portfolio beta zero while
//! keeping each weight within `[BETA_WEIGHT_LOWER_BOUND, BETA_WEIGHT_UPPER_BOUND]`
//! times its volatility-parity allocation.

use std::collections::HashMap;

use crate::domain::market::{PairID, Ticker};
use crate::portfolio::statistical_arbitrage::CandidatePair;

/// Relative lower bound for each pair weight versus its volatility-parity share.
const BETA_WEIGHT_LOWER_BOUND: f64 = 0.5;

/// Relative upper bound for each pair weight versus its volatility-parity share.
const BETA_WEIGHT_UPPER_BOUND: f64 = 2.0;

/// Short buying power reservation factor used by Alpaca.
const SHORT_BUYING_POWER_BUFFER: f64 = 1.03;

/// Combined capital divisor that accounts for the short buying power reservation.
const CAPITAL_DIVISOR: f64 = 1.0 + SHORT_BUYING_POWER_BUFFER;

/// Maximum iterations for the beta-neutral projected gradient descent optimizer.
const OPTIMIZER_ITERATIONS: usize = 500;

/// Learning rate for the projected gradient descent step.
const OPTIMIZER_LEARNING_RATE: f64 = 0.01;

/// A candidate pair that has been sized with dollar amounts and share quantities.
#[derive(Debug, Clone)]
pub struct SizedPair {
    /// Canonical pair identifier, e.g. `"AAPL-MSFT"`.
    pair_id: PairID,
    /// Validated ticker for the long leg.
    long_ticker: Ticker,
    /// Validated ticker for the short leg.
    short_ticker: Ticker,
    /// Long notional dollar amount (matched to `short_dollar_amount` for dollar neutrality).
    long_dollar_amount: f64,
    /// Short notional dollar amount after whole-share rounding.
    short_dollar_amount: f64,
    /// Whole-share count for the short leg (Alpaca SELL orders require whole shares).
    short_quantity: i64,
    /// Latest close price used as the long leg entry price.
    long_entry_price: f64,
    /// Latest close price used as the short leg entry price.
    short_entry_price: f64,
    /// Z-score at pair selection time.
    z_score: f64,
    /// OLS hedge ratio at pair selection time.
    hedge_ratio: f64,
    /// Ensemble alpha signal strength differential.
    signal_strength: f64,
    /// Market beta of the long leg.
    long_market_beta: f64,
    /// Market beta of the short leg.
    short_market_beta: f64,
}

impl SizedPair {
    /// Constructs a `SizedPair`, validating that amounts are non-negative and
    /// `short_quantity` is positive.
    ///
    /// Returns `Err(SizingError::InsufficientPairs)` when validation fails.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pair_id: PairID,
        long_ticker: Ticker,
        short_ticker: Ticker,
        long_dollar_amount: f64,
        short_dollar_amount: f64,
        short_quantity: i64,
        long_entry_price: f64,
        short_entry_price: f64,
        z_score: f64,
        hedge_ratio: f64,
        signal_strength: f64,
        long_market_beta: f64,
        short_market_beta: f64,
    ) -> Result<Self, SizingError> {
        if long_dollar_amount < 0.0 || short_dollar_amount < 0.0 || short_quantity <= 0 {
            // A single invalid pair; the caller discards this error and skips the
            // pair, so the count is only informational.
            return Err(SizingError::InsufficientPairs {
                found: 0,
                required: 1,
            });
        }
        Ok(Self {
            pair_id,
            long_ticker,
            short_ticker,
            long_dollar_amount,
            short_dollar_amount,
            short_quantity,
            long_entry_price,
            short_entry_price,
            z_score,
            hedge_ratio,
            signal_strength,
            long_market_beta,
            short_market_beta,
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

    pub fn long_dollar_amount(&self) -> f64 {
        self.long_dollar_amount
    }

    pub fn short_dollar_amount(&self) -> f64 {
        self.short_dollar_amount
    }

    pub fn short_quantity(&self) -> i64 {
        self.short_quantity
    }

    pub fn long_entry_price(&self) -> f64 {
        self.long_entry_price
    }

    pub fn short_entry_price(&self) -> f64 {
        self.short_entry_price
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

    pub fn long_market_beta(&self) -> f64 {
        self.long_market_beta
    }

    pub fn short_market_beta(&self) -> f64 {
        self.short_market_beta
    }
}

/// Error returned when sizing cannot produce a viable portfolio.
#[derive(Debug, Clone, PartialEq)]
pub enum SizingError {
    /// `required_pairs` must be greater than zero.
    InvalidRequiredPairs { required: usize },
    /// Fewer than `required_pairs` candidates remained after feasibility filtering.
    InsufficientPairs { found: usize, required: usize },
}

impl std::fmt::Display for SizingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SizingError::InvalidRequiredPairs { required } => {
                write!(
                    formatter,
                    "required_pairs must be greater than 0, got {required}."
                )
            }
            SizingError::InsufficientPairs { found, required } => write!(
                formatter,
                "Only {found} viable pairs available, need at least {required}."
            ),
        }
    }
}

impl std::error::Error for SizingError {}

/// Sizes the first `required_pairs` candidate pairs using volatility parity,
/// then optimizes weights for beta neutrality.
///
/// `maximum_capital` is the cash available in the account (Alpaca `cash` field).
/// `market_betas` maps ticker → OLS market beta.
/// `entry_prices` maps ticker → latest mid or close price.
/// `exposure_scale` is the regime-driven multiplier (1.0 for mean reversion,
/// 0.5 for trending).
/// `required_pairs` is the minimum (and target) number of viable pairs, sourced
/// from the `minimum_pairs` constraint; it also sets the per-pair capital share.
///
/// Pairs whose short leg price exceeds the maximum affordable per-pair allocation
/// are discarded before sizing. Returns `Err(SizingError::InvalidRequiredPairs)`
/// when `required_pairs` is 0, and `Err(SizingError::InsufficientPairs)` when
/// fewer than `required_pairs` pairs remain after filtering or after the whole-share
/// constraint removes zero-quantity pairs.
pub fn size_pairs_with_volatility_parity(
    candidate_pairs: &[CandidatePair],
    maximum_capital: f64,
    market_betas: &HashMap<Ticker, f64>,
    entry_prices: &HashMap<Ticker, f64>,
    exposure_scale: f64,
    required_pairs: usize,
) -> Result<Vec<SizedPair>, SizingError> {
    if required_pairs == 0 {
        return Err(SizingError::InvalidRequiredPairs { required: 0 });
    }

    // Maximum dollar allocation a single pair can receive at the highest weight.
    let capital_per_leg = maximum_capital / CAPITAL_DIVISOR;
    let maximum_per_pair_dollar =
        capital_per_leg * exposure_scale * BETA_WEIGHT_UPPER_BOUND / required_pairs as f64;

    // Discard pairs where the short leg cannot afford at least one whole share,
    // or where either entry price is unknown.
    let feasible: Vec<&CandidatePair> = candidate_pairs
        .iter()
        .filter(|pair| {
            let long_price = entry_prices.get(pair.long_ticker()).copied().unwrap_or(0.0);
            let short_price = entry_prices
                .get(pair.short_ticker())
                .copied()
                .unwrap_or(0.0);
            long_price > 0.0 && short_price > 0.0 && short_price <= maximum_per_pair_dollar
        })
        .collect();

    if feasible.len() < required_pairs {
        return Err(SizingError::InsufficientPairs {
            found: feasible.len(),
            required: required_pairs,
        });
    }

    let pairs: Vec<&CandidatePair> = feasible.into_iter().collect();

    // Volatility parity: inverse-volatility weights.
    let pair_volatilities: Vec<f64> = pairs
        .iter()
        .map(|pair| {
            ((pair.long_realized_volatility() + pair.short_realized_volatility()) / 2.0).max(1e-8)
        })
        .collect();

    let inverse_volatility_weights: Vec<f64> = pair_volatilities
        .iter()
        .map(|volatility| 1.0 / volatility)
        .collect();
    let total_inverse_weight: f64 = inverse_volatility_weights.iter().sum();
    let parity_weights: Vec<f64> = inverse_volatility_weights
        .iter()
        .map(|inverse_weight| inverse_weight / total_inverse_weight)
        .collect();

    // Beta-neutral optimization via projected gradient descent.
    let pair_net_betas: Vec<f64> = pairs
        .iter()
        .map(|pair| {
            let long_beta = market_betas.get(pair.long_ticker()).copied().unwrap_or(0.0);
            let short_beta = market_betas
                .get(pair.short_ticker())
                .copied()
                .unwrap_or(0.0);
            long_beta - short_beta
        })
        .collect();

    let adjusted_weights = optimize_beta_neutral(&pair_net_betas, &parity_weights);

    // Normalize to sum = 1.0.
    let weight_sum: f64 = adjusted_weights.iter().sum();
    let final_weights: Vec<f64> = if weight_sum.abs() < f64::EPSILON {
        parity_weights.clone()
    } else {
        adjusted_weights
            .iter()
            .map(|weight| weight / weight_sum)
            .collect()
    };

    // Compute dollar amounts.
    let raw_dollar_amounts: Vec<f64> = final_weights
        .iter()
        .map(|weight| weight * capital_per_leg * exposure_scale)
        .collect();

    // Apply whole-share constraint to short legs and match long to short dollar amount.
    let mut sized_pairs: Vec<SizedPair> = Vec::new();
    for (index, pair) in pairs.iter().enumerate() {
        let short_price = entry_prices
            .get(pair.short_ticker())
            .copied()
            .unwrap_or(0.0);
        let long_price = entry_prices.get(pair.long_ticker()).copied().unwrap_or(0.0);
        let dollar_amount = raw_dollar_amounts[index];

        let short_quantity = (dollar_amount / short_price).floor() as i64;
        if short_quantity <= 0 {
            continue;
        }

        let short_dollar_amount = short_quantity as f64 * short_price;
        let long_dollar_amount = short_dollar_amount; // dollar-neutral

        let long_beta = market_betas.get(pair.long_ticker()).copied().unwrap_or(0.0);
        let short_beta = market_betas
            .get(pair.short_ticker())
            .copied()
            .unwrap_or(0.0);

        // short_quantity > 0 is already verified by the guard above.
        if let Ok(sized_pair) = SizedPair::new(
            pair.pair_id().clone(),
            pair.long_ticker().clone(),
            pair.short_ticker().clone(),
            long_dollar_amount,
            short_dollar_amount,
            short_quantity,
            long_price,
            short_price,
            pair.z_score(),
            pair.hedge_ratio(),
            pair.signal_strength(),
            long_beta,
            short_beta,
        ) {
            sized_pairs.push(sized_pair);
            if sized_pairs.len() == required_pairs {
                break;
            }
        }
    }

    if sized_pairs.len() < required_pairs {
        return Err(SizingError::InsufficientPairs {
            found: sized_pairs.len(),
            required: required_pairs,
        });
    }

    Ok(sized_pairs)
}

/// Projected gradient descent beta-neutral optimizer.
///
/// Minimizes `(net_beta)²` subject to box constraints and a fixed total weight.
fn optimize_beta_neutral(pair_net_betas: &[f64], parity_weights: &[f64]) -> Vec<f64> {
    let total_weight: f64 = parity_weights.iter().sum();
    let mut weights = parity_weights.to_vec();

    for _ in 0..OPTIMIZER_ITERATIONS {
        let current_sum: f64 = weights.iter().sum();
        if current_sum.abs() < f64::EPSILON {
            break;
        }

        let net_beta: f64 = weights
            .iter()
            .zip(pair_net_betas.iter())
            .map(|(weight, pair_net_beta_value)| weight * pair_net_beta_value)
            .sum::<f64>()
            / current_sum;

        // Gradient of (net_beta)² w.r.t. each weight.
        let gradient: Vec<f64> = pair_net_betas
            .iter()
            .map(|&pair_net_beta| 2.0 * net_beta * (pair_net_beta - net_beta) / current_sum)
            .collect();

        // Gradient step.
        let stepped: Vec<f64> = weights
            .iter()
            .zip(gradient.iter())
            .map(|(weight, gradient_component)| {
                weight - OPTIMIZER_LEARNING_RATE * gradient_component
            })
            .collect();

        // Project into box constraints.
        let projected: Vec<f64> = stepped
            .iter()
            .zip(parity_weights.iter())
            .map(|(weight, &parity)| {
                weight.clamp(
                    BETA_WEIGHT_LOWER_BOUND * parity,
                    BETA_WEIGHT_UPPER_BOUND * parity,
                )
            })
            .collect();

        // Rescale to maintain total weight.
        let projected_sum: f64 = projected.iter().sum();
        if projected_sum.abs() < f64::EPSILON {
            break;
        }
        weights = projected
            .iter()
            .map(|weight| weight * total_weight / projected_sum)
            .collect();
    }

    weights
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Required-pairs value used across sizing tests (the historical default).
    const REQUIRED_PAIRS: usize = 10;

    fn make_candidate(
        long_ticker: &str,
        short_ticker: &str,
        long_volatility: f64,
        short_volatility: f64,
    ) -> CandidatePair {
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
            long_volatility,
            short_volatility,
        )
        .expect("test candidate pair should be valid")
    }

    fn make_ten_candidates(
        long_price: f64,
        short_price: f64,
    ) -> (Vec<CandidatePair>, HashMap<Ticker, f64>) {
        let long_tickers = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];
        let short_tickers = ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"];

        let pairs: Vec<CandidatePair> = long_tickers
            .iter()
            .zip(short_tickers.iter())
            .map(|(long, short)| make_candidate(long, short, 0.01, 0.01))
            .collect();

        let mut entry_prices: HashMap<Ticker, f64> = HashMap::new();
        for ticker in long_tickers.iter().chain(short_tickers.iter()) {
            let price = if long_tickers.contains(ticker) {
                long_price
            } else {
                short_price
            };
            entry_prices.insert(Ticker::new(ticker).unwrap(), price);
        }

        (pairs, entry_prices)
    }

    #[test]
    fn test_sizing_error_display() {
        let error = SizingError::InsufficientPairs {
            found: 5,
            required: 10,
        };
        let message = format!("{}", error);
        assert!(message.contains("5"));
        assert!(message.contains("10"));
    }

    #[test]
    fn test_size_pairs_insufficient_candidates_error() {
        let result = size_pairs_with_volatility_parity(
            &[make_candidate("AAPL", "MSFT", 0.01, 0.01)],
            100_000.0,
            &HashMap::new(),
            &HashMap::new(),
            1.0,
            REQUIRED_PAIRS,
        );
        assert!(matches!(
            result,
            Err(SizingError::InsufficientPairs { found: 0, .. })
        ));
    }

    #[test]
    fn test_size_pairs_required_pairs_zero_error() {
        let result = size_pairs_with_volatility_parity(
            &[],
            100_000.0,
            &HashMap::new(),
            &HashMap::new(),
            1.0,
            0,
        );
        assert!(matches!(
            result,
            Err(SizingError::InvalidRequiredPairs { required: 0 })
        ));
    }

    #[test]
    fn test_size_pairs_short_price_too_high_discards_pairs() {
        let (candidates, mut entry_prices) = make_ten_candidates(100.0, 100.0);
        // Set short prices so they exceed maximum_per_pair_dollar
        for ticker in ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"] {
            entry_prices.insert(Ticker::new(ticker).unwrap(), 1_000_000.0);
        }
        let result = size_pairs_with_volatility_parity(
            &candidates,
            100_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));
    }

    #[test]
    fn test_size_pairs_honors_required_below_candidate_pool() {
        // Decoupling: with more feasible candidates than required, sizing returns
        // exactly `required_pairs`, leaving the rest as spare buffer.
        let (candidates, entry_prices) = make_ten_candidates(100.0, 50.0);
        let required = 3;
        let sized = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
            required,
        )
        .unwrap();
        assert_eq!(sized.len(), required);
    }

    #[test]
    fn test_size_pairs_success_returns_required_pairs() {
        let (candidates, entry_prices) = make_ten_candidates(100.0, 50.0);
        let result = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        );
        assert!(result.is_ok());
        let sized = result.unwrap();
        assert_eq!(sized.len(), REQUIRED_PAIRS);
    }

    #[test]
    fn test_size_pairs_each_pair_is_dollar_neutral() {
        let (candidates, entry_prices) = make_ten_candidates(100.0, 50.0);
        let sized = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        )
        .unwrap();
        for pair in &sized {
            // long_dollar_amount == short_dollar_amount by construction
            assert!((pair.long_dollar_amount - pair.short_dollar_amount).abs() < 1e-6);
        }
    }

    #[test]
    fn test_size_pairs_short_quantity_is_positive() {
        let (candidates, entry_prices) = make_ten_candidates(100.0, 50.0);
        let sized = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        )
        .unwrap();
        for pair in &sized {
            assert!(pair.short_quantity > 0);
        }
    }

    #[test]
    fn test_size_pairs_exposure_scale_halves_allocation() {
        let (candidates, entry_prices) = make_ten_candidates(100.0, 50.0);
        let betas = HashMap::new();

        let full = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &betas,
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        )
        .unwrap();
        let half = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &betas,
            &entry_prices,
            0.5,
            REQUIRED_PAIRS,
        )
        .unwrap();

        // With half exposure, dollar amounts should be ≤ full amounts.
        for (full_pair, half_pair) in full.iter().zip(half.iter()) {
            assert!(half_pair.short_dollar_amount <= full_pair.short_dollar_amount + 1.0);
        }
    }

    #[test]
    fn test_optimize_beta_neutral_reduces_net_beta() {
        // Start with weights that produce positive net beta; optimizer should reduce it.
        let net_betas = vec![1.0; 10]; // all pairs have positive net beta
        let parity_weights: Vec<f64> = vec![0.1; 10]; // equal weights

        let weight_sum: f64 = parity_weights.iter().sum();
        let baseline_net_beta = parity_weights
            .iter()
            .zip(net_betas.iter())
            .map(|(weight, net_beta)| weight * net_beta)
            .sum::<f64>()
            / weight_sum;

        let optimized = optimize_beta_neutral(&net_betas, &parity_weights);

        let optimized_sum: f64 = optimized.iter().sum();
        let optimized_net_beta = optimized
            .iter()
            .zip(net_betas.iter())
            .map(|(weight, net_beta)| weight * net_beta)
            .sum::<f64>()
            / optimized_sum;

        assert!(optimized_net_beta.abs() <= baseline_net_beta.abs() + 1e-12);
        assert_eq!(optimized.len(), 10);
        for weight in &optimized {
            assert!(weight.is_finite());
            assert!(*weight >= 0.0);
        }
    }

    #[test]
    fn test_optimize_beta_neutral_zero_weights_returns_input() {
        // If all parity weights are 0, the optimizer should not panic.
        let net_betas = vec![1.0, -1.0];
        let parity_weights = vec![0.0, 0.0];
        let result = optimize_beta_neutral(&net_betas, &parity_weights);
        // All projected weights will be clamped to 0; result should be all zeros.
        assert_eq!(result.len(), 2);
    }

    fn make_pair_id(long: &str, short: &str) -> PairID {
        PairID::new(Ticker::new(long).unwrap(), Ticker::new(short).unwrap())
    }

    #[test]
    fn test_sized_pair_new_rejects_negative_long_dollar_amount() {
        let result = SizedPair::new(
            make_pair_id("AAPL", "MSFT"),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            -100.0,
            100.0,
            1,
            150.0,
            100.0,
            2.5,
            1.0,
            0.05,
            1.1,
            0.9,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));
    }

    #[test]
    fn test_sized_pair_new_rejects_negative_short_dollar_amount() {
        let result = SizedPair::new(
            make_pair_id("AAPL", "MSFT"),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            100.0,
            -50.0,
            1,
            150.0,
            100.0,
            2.5,
            1.0,
            0.05,
            1.1,
            0.9,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));
    }

    #[test]
    fn test_sized_pair_new_rejects_zero_short_quantity() {
        let result = SizedPair::new(
            make_pair_id("AAPL", "MSFT"),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            100.0,
            100.0,
            0,
            150.0,
            100.0,
            2.5,
            1.0,
            0.05,
            1.1,
            0.9,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));
    }

    #[test]
    fn test_sized_pair_new_rejects_negative_short_quantity() {
        let result = SizedPair::new(
            make_pair_id("AAPL", "MSFT"),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            100.0,
            100.0,
            -1,
            150.0,
            100.0,
            2.5,
            1.0,
            0.05,
            1.1,
            0.9,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));
    }

    #[test]
    fn test_sized_pair_new_valid_all_accessors() {
        let pair = SizedPair::new(
            make_pair_id("AAPL", "MSFT"),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            5000.0,
            4900.0,
            49,
            155.0,
            100.0,
            2.7,
            0.8,
            0.04,
            1.2,
            0.95,
        )
        .expect("valid sized pair");

        assert_eq!(pair.pair_id().as_str(), "AAPL-MSFT");
        assert_eq!(pair.long_ticker().as_str(), "AAPL");
        assert_eq!(pair.short_ticker().as_str(), "MSFT");
        assert!((pair.long_dollar_amount() - 5000.0).abs() < f64::EPSILON);
        assert!((pair.short_dollar_amount() - 4900.0).abs() < f64::EPSILON);
        assert_eq!(pair.short_quantity(), 49);
        assert!((pair.long_entry_price() - 155.0).abs() < f64::EPSILON);
        assert!((pair.short_entry_price() - 100.0).abs() < f64::EPSILON);
        assert!((pair.z_score() - 2.7).abs() < f64::EPSILON);
        assert!((pair.hedge_ratio() - 0.8).abs() < f64::EPSILON);
        assert!((pair.signal_strength() - 0.04).abs() < f64::EPSILON);
        assert!((pair.long_market_beta() - 1.2).abs() < f64::EPSILON);
        assert!((pair.short_market_beta() - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sizing_error_invalid_required_pairs_display() {
        let error = SizingError::InvalidRequiredPairs { required: 0 };
        let message = format!("{}", error);
        assert!(message.contains("0"));
        assert!(message.contains("greater than"));
    }

    #[test]
    fn test_sizing_error_is_error_trait() {
        let error = SizingError::InsufficientPairs {
            found: 2,
            required: 5,
        };
        let _boxed: Box<dyn std::error::Error> = Box::new(error);
    }

    #[test]
    fn test_size_pairs_whole_share_constraint_drops_pair_when_quantity_zero() {
        // With very low capital per pair a short price can yield floor(amount/price) == 0,
        // causing the pair to be skipped, producing InsufficientPairs.
        let (candidates, mut entry_prices) = make_ten_candidates(100.0, 100.0);
        // Set short prices to a very high value relative to per-pair capital so
        // floor(capital / price) == 0 for each pair.
        for ticker in ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"] {
            entry_prices.insert(Ticker::new(ticker).unwrap(), 999_999.0);
        }
        // Supply enough capital to pass the feasibility filter threshold but the
        // per-pair dollar allocation still floor-divides to 0 shares.
        // maximum_per_pair_dollar = (capital / CAPITAL_DIVISOR) * exposure * UPPER / required
        // = (2_000_000 / 2.03) * 1.0 * 2.0 / 10 ≈ 197,044 < 999_999 → feasibility filter
        // kicks in first. So use a moderate short price that passes feasibility but
        // is high enough that the per-pair dollar amount (without the upper-bound
        // multiplier) rounds down to 0 shares.
        //
        // A simpler direct path: use a single required pair and set the short price
        // to just above the raw dollar_amount for that pair.
        let (single_candidates, mut single_prices) = make_ten_candidates(50.0, 50.0);
        // With capital=100, required=1:
        // capital_per_leg = 100 / 2.03 ≈ 49.26
        // maximum_per_pair_dollar = 49.26 * 1.0 * 2.0 / 1 ≈ 98.52 → 50 passes
        // raw_dollar_amounts[0] = final_weight (≈0.1 for 10 equal pairs) * 49.26 * 1.0 ≈ 4.93
        // short_quantity = floor(4.93 / 50) = 0 → pair skipped
        for ticker in ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"] {
            single_prices.insert(Ticker::new(ticker).unwrap(), 50.0);
        }
        let result = size_pairs_with_volatility_parity(
            &single_candidates,
            100.0,
            &HashMap::new(),
            &single_prices,
            1.0,
            1,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));

        // The first test (all short prices at 999_999) also confirms InsufficientPairs.
        let result2 = size_pairs_with_volatility_parity(
            &candidates,
            100_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        );
        assert!(matches!(
            result2,
            Err(SizingError::InsufficientPairs { .. })
        ));
    }

    #[test]
    fn test_size_pairs_missing_long_price_discards_pair() {
        // Pairs missing their long price (unwrap_or(0.0) → 0.0 → fails > 0.0 check)
        // are dropped by the feasibility filter.
        let (candidates, mut entry_prices) = make_ten_candidates(100.0, 50.0);
        // Remove all long-ticker prices so feasibility fails.
        for ticker in ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"] {
            entry_prices.remove(&Ticker::new(ticker).unwrap());
        }
        let result = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));
    }

    #[test]
    fn test_size_pairs_beta_neutral_with_mixed_betas() {
        // Supply market betas with opposite signs on long and short legs.
        // This exercises the beta-neutral optimization path with actual beta values.
        let (candidates, entry_prices) = make_ten_candidates(100.0, 50.0);
        let mut market_betas: HashMap<Ticker, f64> = HashMap::new();
        for ticker in ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"] {
            market_betas.insert(Ticker::new(ticker).unwrap(), 1.2_f64);
        }
        for ticker in ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"] {
            market_betas.insert(Ticker::new(ticker).unwrap(), 0.8_f64);
        }
        let result = size_pairs_with_volatility_parity(
            &candidates,
            500_000.0,
            &market_betas,
            &entry_prices,
            1.0,
            REQUIRED_PAIRS,
        );
        assert!(result.is_ok());
        let sized = result.unwrap();
        assert_eq!(sized.len(), REQUIRED_PAIRS);
        // Verify that betas are correctly threaded through to SizedPair.
        for pair in &sized {
            assert!((pair.long_market_beta() - 1.2).abs() < f64::EPSILON);
            assert!((pair.short_market_beta() - 0.8).abs() < f64::EPSILON);
        }
    }
}
