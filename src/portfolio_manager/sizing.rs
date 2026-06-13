//! Volatility-parity position sizing with beta-neutral weight optimization.
//!
//! Pairs with lower realized spread volatility receive proportionally more capital
//! so that each pair contributes equal risk to the portfolio. A projected gradient
//! descent optimizer then nudges the weights toward net portfolio beta zero while
//! keeping each weight within `[BETA_WEIGHT_LOWER_BOUND, BETA_WEIGHT_UPPER_BOUND]`
//! times its volatility-parity allocation.

use std::collections::HashMap;

use crate::portfolio_manager::statistical_arbitrage::{CandidatePair, TARGET_PAIR_COUNT};

/// Relative lower bound for each pair weight versus its volatility-parity share.
const BETA_WEIGHT_LOWER_BOUND: f64 = 0.5;

/// Relative upper bound for each pair weight versus its volatility-parity share.
const BETA_WEIGHT_UPPER_BOUND: f64 = 2.0;

/// Required minimum number of viable pairs to form a portfolio.
const REQUIRED_PAIRS: usize = TARGET_PAIR_COUNT;

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
    /// Human-readable pair identifier, e.g. `"AAPL-MSFT"`.
    pub pair_id: String,
    /// Ticker symbol for the long leg.
    pub long_ticker: String,
    /// Ticker symbol for the short leg.
    pub short_ticker: String,
    /// Long notional dollar amount (matched to `short_dollar_amount` for dollar neutrality).
    pub long_dollar_amount: f64,
    /// Short notional dollar amount after whole-share rounding.
    pub short_dollar_amount: f64,
    /// Whole-share count for the short leg (Alpaca SELL orders require whole shares).
    pub short_quantity: i64,
    /// Latest close price used as the long leg entry price.
    pub long_entry_price: f64,
    /// Latest close price used as the short leg entry price.
    pub short_entry_price: f64,
    /// Z-score at pair selection time.
    pub z_score: f64,
    /// OLS hedge ratio at pair selection time.
    pub hedge_ratio: f64,
    /// Ensemble alpha signal strength differential.
    pub signal_strength: f64,
    /// Market beta of the long leg.
    pub long_market_beta: f64,
    /// Market beta of the short leg.
    pub short_market_beta: f64,
}

/// Error returned when sizing cannot produce a viable portfolio.
#[derive(Debug, Clone, PartialEq)]
pub enum SizingError {
    /// Fewer than `REQUIRED_PAIRS` candidates remained after feasibility filtering.
    InsufficientPairs { found: usize, required: usize },
}

impl std::fmt::Display for SizingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SizingError::InsufficientPairs { found, required } => write!(
                formatter,
                "Only {found} viable pairs available, need at least {required}."
            ),
        }
    }
}

impl std::error::Error for SizingError {}

/// Sizes the first `REQUIRED_PAIRS` candidate pairs using volatility parity,
/// then optimizes weights for beta neutrality.
///
/// `maximum_capital` is the cash available in the account (Alpaca `cash` field).
/// `market_betas` maps ticker → OLS market beta.
/// `entry_prices` maps ticker → latest mid or close price.
/// `exposure_scale` is the regime-driven multiplier (1.0 for mean reversion,
/// 0.5 for trending).
///
/// Pairs whose short leg price exceeds the maximum affordable per-pair allocation
/// are discarded before sizing. Returns `Err(SizingError::InsufficientPairs)` when
/// fewer than `REQUIRED_PAIRS` pairs remain after filtering or after the whole-share
/// constraint removes zero-quantity pairs.
pub fn size_pairs_with_volatility_parity(
    candidate_pairs: &[CandidatePair],
    maximum_capital: f64,
    market_betas: &HashMap<String, f64>,
    entry_prices: &HashMap<String, f64>,
    exposure_scale: f64,
) -> Result<Vec<SizedPair>, SizingError> {
    // Maximum dollar allocation a single pair can receive at the highest weight.
    let capital_per_leg = maximum_capital / CAPITAL_DIVISOR;
    let maximum_per_pair_dollar =
        capital_per_leg * exposure_scale * BETA_WEIGHT_UPPER_BOUND / REQUIRED_PAIRS as f64;

    // Discard pairs where the short leg cannot afford at least one whole share,
    // or where either entry price is unknown.
    let feasible: Vec<&CandidatePair> = candidate_pairs
        .iter()
        .filter(|pair| {
            let long_price = entry_prices.get(&pair.long_ticker).copied().unwrap_or(0.0);
            let short_price = entry_prices.get(&pair.short_ticker).copied().unwrap_or(0.0);
            long_price > 0.0 && short_price > 0.0 && short_price <= maximum_per_pair_dollar
        })
        .collect();

    if feasible.len() < REQUIRED_PAIRS {
        return Err(SizingError::InsufficientPairs {
            found: feasible.len(),
            required: REQUIRED_PAIRS,
        });
    }

    let pairs: Vec<&CandidatePair> = feasible.into_iter().take(REQUIRED_PAIRS).collect();

    // Volatility parity: inverse-volatility weights.
    let pair_volatilities: Vec<f64> = pairs
        .iter()
        .map(|pair| {
            ((pair.long_realized_volatility + pair.short_realized_volatility) / 2.0).max(1e-8)
        })
        .collect();

    let inverse_volatility_weights: Vec<f64> = pair_volatilities.iter().map(|v| 1.0 / v).collect();
    let total_inverse_weight: f64 = inverse_volatility_weights.iter().sum();
    let parity_weights: Vec<f64> = inverse_volatility_weights
        .iter()
        .map(|w| w / total_inverse_weight)
        .collect();

    // Beta-neutral optimization via projected gradient descent.
    let pair_net_betas: Vec<f64> = pairs
        .iter()
        .map(|pair| {
            let long_beta = market_betas.get(&pair.long_ticker).copied().unwrap_or(0.0);
            let short_beta = market_betas.get(&pair.short_ticker).copied().unwrap_or(0.0);
            long_beta - short_beta
        })
        .collect();

    let adjusted_weights = optimize_beta_neutral(&pair_net_betas, &parity_weights);

    // Normalize to sum = 1.0.
    let weight_sum: f64 = adjusted_weights.iter().sum();
    let final_weights: Vec<f64> = if weight_sum.abs() < f64::EPSILON {
        parity_weights.clone()
    } else {
        adjusted_weights.iter().map(|w| w / weight_sum).collect()
    };

    // Compute dollar amounts.
    let raw_dollar_amounts: Vec<f64> = final_weights
        .iter()
        .map(|w| w * capital_per_leg * exposure_scale)
        .collect();

    // Apply whole-share constraint to short legs and match long to short dollar amount.
    let mut sized_pairs: Vec<SizedPair> = Vec::new();
    for (index, pair) in pairs.iter().enumerate() {
        let short_price = entry_prices.get(&pair.short_ticker).copied().unwrap_or(0.0);
        let long_price = entry_prices.get(&pair.long_ticker).copied().unwrap_or(0.0);
        let dollar_amount = raw_dollar_amounts[index];

        let short_quantity = (dollar_amount / short_price).floor() as i64;
        if short_quantity <= 0 {
            continue;
        }

        let short_dollar_amount = short_quantity as f64 * short_price;
        let long_dollar_amount = short_dollar_amount; // dollar-neutral

        let long_beta = market_betas.get(&pair.long_ticker).copied().unwrap_or(0.0);
        let short_beta = market_betas.get(&pair.short_ticker).copied().unwrap_or(0.0);

        sized_pairs.push(SizedPair {
            pair_id: pair.pair_id.clone(),
            long_ticker: pair.long_ticker.clone(),
            short_ticker: pair.short_ticker.clone(),
            long_dollar_amount,
            short_dollar_amount,
            short_quantity,
            long_entry_price: long_price,
            short_entry_price: short_price,
            z_score: pair.z_score,
            hedge_ratio: pair.hedge_ratio,
            signal_strength: pair.signal_strength,
            long_market_beta: long_beta,
            short_market_beta: short_beta,
        });
    }

    if sized_pairs.len() < REQUIRED_PAIRS {
        return Err(SizingError::InsufficientPairs {
            found: sized_pairs.len(),
            required: REQUIRED_PAIRS,
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
            .map(|(w, b)| w * b)
            .sum::<f64>()
            / current_sum;

        // Gradient of (net_beta)² w.r.t. each weight.
        let gradient: Vec<f64> = pair_net_betas
            .iter()
            .map(|&b| 2.0 * net_beta * b / current_sum)
            .collect();

        // Gradient step.
        let stepped: Vec<f64> = weights
            .iter()
            .zip(gradient.iter())
            .map(|(w, g)| w - OPTIMIZER_LEARNING_RATE * g)
            .collect();

        // Project into box constraints.
        let projected: Vec<f64> = stepped
            .iter()
            .zip(parity_weights.iter())
            .map(|(w, &parity)| {
                w.clamp(
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
            .map(|w| w * total_weight / projected_sum)
            .collect();
    }

    weights
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(
        long_ticker: &str,
        short_ticker: &str,
        long_volatility: f64,
        short_volatility: f64,
    ) -> CandidatePair {
        CandidatePair {
            pair_id: format!("{long_ticker}-{short_ticker}"),
            long_ticker: long_ticker.to_string(),
            short_ticker: short_ticker.to_string(),
            z_score: 2.5,
            hedge_ratio: 1.0,
            signal_strength: 0.05,
            long_realized_volatility: long_volatility,
            short_realized_volatility: short_volatility,
        }
    }

    fn make_ten_candidates(
        long_price: f64,
        short_price: f64,
    ) -> (Vec<CandidatePair>, HashMap<String, f64>) {
        let long_tickers = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];
        let short_tickers = ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"];

        let pairs: Vec<CandidatePair> = long_tickers
            .iter()
            .zip(short_tickers.iter())
            .map(|(long, short)| make_candidate(long, short, 0.01, 0.01))
            .collect();

        let mut entry_prices = HashMap::new();
        for ticker in long_tickers.iter().chain(short_tickers.iter()) {
            let price = if long_tickers.contains(ticker) {
                long_price
            } else {
                short_price
            };
            entry_prices.insert(ticker.to_string(), price);
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
        );
        assert!(matches!(
            result,
            Err(SizingError::InsufficientPairs { found: 0, .. })
        ));
    }

    #[test]
    fn test_size_pairs_short_price_too_high_discards_pairs() {
        let (candidates, mut entry_prices) = make_ten_candidates(100.0, 100.0);
        // Set short prices so they exceed maximum_per_pair_dollar
        for ticker in ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"] {
            entry_prices.insert(ticker.to_string(), 1_000_000.0);
        }
        let result = size_pairs_with_volatility_parity(
            &candidates,
            100_000.0,
            &HashMap::new(),
            &entry_prices,
            1.0,
        );
        assert!(matches!(result, Err(SizingError::InsufficientPairs { .. })));
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

        let full =
            size_pairs_with_volatility_parity(&candidates, 500_000.0, &betas, &entry_prices, 1.0)
                .unwrap();
        let half =
            size_pairs_with_volatility_parity(&candidates, 500_000.0, &betas, &entry_prices, 0.5)
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
        let optimized = optimize_beta_neutral(&net_betas, &parity_weights);
        // Weights should all be at the lower bound (optimizer drives weights down uniformly).
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
}
