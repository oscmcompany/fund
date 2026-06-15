//! Portfolio construction with validated invariants.
//!
//! The only way to obtain a `Portfolio` value is through `Portfolio::new()`.
//! A `Portfolio` in scope is proof that all structural constraints passed at
//! construction time, eliminating the need for defensive re-checks downstream.

use std::collections::HashMap;
use std::num::NonZeroU8;

use num_traits::ToPrimitive;

use crate::domain::orders::FilledPair;
use crate::domain::primitives::Percent;

/// Required minimum number of pairs in a valid portfolio.
pub const REQUIRED_PAIRS: usize = 10;

/// Maximum allowed fractional imbalance between long and short notional per pair.
const DOLLAR_NEUTRAL_TOLERANCE: f64 = 0.05;

/// Drawdown threshold as a percentage of capital; triggers a trading halt when breached.
#[derive(Debug, Clone, Copy)]
pub struct DrawdownThreshold(pub Percent);

/// Maximum allowed notional fraction for any single ticker across the portfolio.
#[derive(Debug, Clone, Copy)]
pub struct ConcentrationCap(pub Percent);

/// Minimum number of pairs required for a valid portfolio.
#[derive(Debug, Clone, Copy)]
pub struct MinimumPairs(pub NonZeroU8);

/// Maximum allowed absolute deviation of net portfolio beta from zero.
#[derive(Debug, Clone, Copy)]
pub struct BetaTolerance(f64);

impl BetaTolerance {
    /// Constructs a `BetaTolerance` from a positive tolerance value.
    ///
    /// Returns `Err` if `value` is not positive.
    pub fn new(value: f64) -> Result<Self, String> {
        if value <= 0.0 {
            return Err(format!("BetaTolerance must be positive, got {value}"));
        }
        Ok(Self(value))
    }

    /// Returns the tolerance value.
    pub fn value(self) -> f64 {
        self.0
    }
}

/// Portfolio construction constraints.
#[derive(Debug, Clone)]
pub struct Constraints {
    drawdown_threshold: DrawdownThreshold,
    concentration_cap: ConcentrationCap,
    minimum_pairs: MinimumPairs,
    beta_tolerance: BetaTolerance,
}

impl Constraints {
    /// Constructs a `Constraints` bundle from already-validated component types.
    pub fn new(
        drawdown_threshold: DrawdownThreshold,
        concentration_cap: ConcentrationCap,
        minimum_pairs: MinimumPairs,
        beta_tolerance: BetaTolerance,
    ) -> Self {
        Self {
            drawdown_threshold,
            concentration_cap,
            minimum_pairs,
            beta_tolerance,
        }
    }

    /// Returns the drawdown threshold.
    pub fn drawdown_threshold(&self) -> DrawdownThreshold {
        self.drawdown_threshold
    }

    /// Returns the concentration cap.
    pub fn concentration_cap(&self) -> ConcentrationCap {
        self.concentration_cap
    }

    /// Returns the minimum pairs requirement.
    pub fn minimum_pairs(&self) -> MinimumPairs {
        self.minimum_pairs
    }

    /// Returns the beta tolerance.
    pub fn beta_tolerance(&self) -> BetaTolerance {
        self.beta_tolerance
    }
}

/// Error returned when `Portfolio::new()` rejects the candidate pairs.
#[derive(Debug, Clone, PartialEq)]
pub enum PortfolioError {
    /// Fewer pairs than the minimum floor.
    InsufficientPairs { required: usize, found: usize },
    /// A single ticker's notional fraction exceeds the concentration cap.
    ConcentrationCapExceeded { ticker: String },
    /// A pair's long and short notionals deviate beyond the dollar-neutral tolerance.
    DollarNeutralityViolation { pair_index: usize },
    /// Net portfolio beta exceeds the configured tolerance.
    BetaNeutralityViolation { net_beta: f64, tolerance: f64 },
}

impl std::fmt::Display for PortfolioError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortfolioError::InsufficientPairs { required, found } => write!(
                formatter,
                "Portfolio has {found} pairs but requires {required}."
            ),
            PortfolioError::ConcentrationCapExceeded { ticker } => {
                write!(formatter, "Ticker {ticker} exceeds the concentration cap.")
            }
            PortfolioError::DollarNeutralityViolation { pair_index } => write!(
                formatter,
                "Pair at index {pair_index} violates dollar neutrality."
            ),
            PortfolioError::BetaNeutralityViolation {
                net_beta,
                tolerance,
            } => write!(
                formatter,
                "Net portfolio beta {net_beta:.4} exceeds tolerance {tolerance:.4}."
            ),
        }
    }
}

impl std::error::Error for PortfolioError {}

/// A validated portfolio of filled long-short pairs.
///
/// Constructed only via `::new()`. A value of this type is proof that:
/// - the pair count meets or exceeds `minimum_pairs`
/// - no single ticker exceeds `concentration_cap` of total gross notional
/// - each pair is dollar-neutral within `DOLLAR_NEUTRAL_TOLERANCE`
/// - net portfolio beta is within `beta_tolerance` of zero
///
/// The inner field is private; use `pairs()` to access the validated pairs.
#[derive(Debug)]
pub struct Portfolio(Vec<FilledPair>);

impl Portfolio {
    /// Returns the validated pairs in this portfolio.
    pub fn pairs(&self) -> &[FilledPair] {
        &self.0
    }

    /// Constructs a `Portfolio`, enforcing all invariants from `constraints`.
    ///
    /// Returns `Err` with the first violated constraint.
    pub fn new(pairs: Vec<FilledPair>, constraints: &Constraints) -> Result<Self, PortfolioError> {
        let minimum = constraints.minimum_pairs().0.get() as usize;
        if pairs.len() < minimum {
            return Err(PortfolioError::InsufficientPairs {
                required: minimum,
                found: pairs.len(),
            });
        }

        // Compute total gross notional as f64 for ratio checks.
        let total_gross_notional: f64 = pairs
            .iter()
            .flat_map(|pair| [pair.long_notional.value(), pair.short_notional.value()])
            .map(|decimal| decimal.to_f64().unwrap_or(0.0))
            .sum();

        if total_gross_notional > 0.0 {
            // Concentration cap: no single ticker may exceed the cap fraction of total gross notional.
            let mut ticker_notionals: HashMap<String, f64> = HashMap::new();
            for pair in &pairs {
                let long_notional = pair.long_notional.value().to_f64().unwrap_or(0.0);
                let short_notional = pair.short_notional.value().to_f64().unwrap_or(0.0);
                *ticker_notionals
                    .entry(pair.long.ticker.clone())
                    .or_insert(0.0) += long_notional;
                *ticker_notionals
                    .entry(pair.short.ticker.clone())
                    .or_insert(0.0) += short_notional;
            }

            let cap = constraints.concentration_cap().0.value();
            for (ticker, notional) in &ticker_notionals {
                let fraction = notional / total_gross_notional;
                if fraction > cap {
                    return Err(PortfolioError::ConcentrationCapExceeded {
                        ticker: ticker.clone(),
                    });
                }
            }
        }

        // Dollar-neutral pairing: each pair's long and short notionals must be within tolerance.
        for (index, pair) in pairs.iter().enumerate() {
            let long_notional = pair.long_notional.value().to_f64().unwrap_or(0.0);
            let short_notional = pair.short_notional.value().to_f64().unwrap_or(0.0);
            let average = (long_notional + short_notional) / 2.0;
            if average > 0.0 {
                let imbalance = (long_notional - short_notional).abs() / average;
                if imbalance > DOLLAR_NEUTRAL_TOLERANCE {
                    return Err(PortfolioError::DollarNeutralityViolation { pair_index: index });
                }
            }
        }

        // Beta neutrality: net portfolio beta must be within the configured tolerance.
        let net_beta: f64 = pairs
            .iter()
            .map(|pair| pair.long_beta - pair.short_beta)
            .sum();
        let tolerance = constraints.beta_tolerance().value();
        if net_beta.abs() > tolerance {
            return Err(PortfolioError::BetaNeutralityViolation {
                net_beta,
                tolerance,
            });
        }

        Ok(Portfolio(pairs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::orders::{FilledOrder, Order, OrderSide, PendingPair};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn make_constraints() -> Constraints {
        Constraints::new(
            DrawdownThreshold(Percent::new(0.10).unwrap()),
            ConcentrationCap(Percent::new(0.20).unwrap()),
            MinimumPairs(NonZeroU8::new(10).unwrap()),
            BetaTolerance::new(0.1).unwrap(),
        )
    }

    fn make_filled_pair(
        long_ticker: &str,
        short_ticker: &str,
        long_beta: f64,
        short_beta: f64,
    ) -> FilledPair {
        let long_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            long_ticker.to_string(),
            OrderSide::Long,
            Decimal::from(100),
            "market".to_string(),
            None,
            format!("alpaca-long-{}", long_ticker.to_lowercase()),
            Utc::now(),
        );
        let short_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            short_ticker.to_string(),
            OrderSide::Short,
            Decimal::from(100),
            "market".to_string(),
            None,
            format!("alpaca-short-{}", short_ticker.to_lowercase()),
            Utc::now(),
        );
        let pending_pair = PendingPair::new(long_order, short_order, long_beta, short_beta);
        // price=100, qty=100 → notional=10,000 per leg
        pending_pair
            .confirm(
                Some(FilledOrder {
                    alpaca_order_id: "fill-long".to_string(),
                    fill_price: Decimal::from(100),
                    filled_quantity: Decimal::from(100),
                }),
                Some(FilledOrder {
                    alpaca_order_id: "fill-short".to_string(),
                    fill_price: Decimal::from(100),
                    filled_quantity: Decimal::from(100),
                }),
            )
            .unwrap()
    }

    fn make_balanced_pairs(count: usize) -> Vec<FilledPair> {
        let long_tickers = [
            "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "NFLX", "AMD", "INTC", "CRM",
            "ORCL", "IBM", "QCOM", "TXN",
        ];
        let short_tickers = [
            "WMT", "TGT", "KO", "PEP", "JNJ", "PFE", "MRK", "ABT", "UNH", "CVS", "WBA", "RAD",
            "DG", "DLTR", "COST",
        ];
        (0..count)
            .map(|index| {
                make_filled_pair(
                    long_tickers[index % long_tickers.len()],
                    short_tickers[index % short_tickers.len()],
                    1.0,
                    1.0,
                )
            })
            .collect()
    }

    #[test]
    fn test_portfolio_new_accepts_valid_pairs() {
        let pairs = make_balanced_pairs(10);
        let constraints = make_constraints();
        assert!(Portfolio::new(pairs, &constraints).is_ok());
    }

    #[test]
    fn test_portfolio_new_rejects_insufficient_pairs() {
        let pairs = make_balanced_pairs(9);
        let constraints = make_constraints();
        let error = Portfolio::new(pairs, &constraints).unwrap_err();
        assert_eq!(
            error,
            PortfolioError::InsufficientPairs {
                required: 10,
                found: 9
            }
        );
    }

    #[test]
    fn test_portfolio_new_rejects_empty_pairs() {
        let constraints = make_constraints();
        let error = Portfolio::new(vec![], &constraints).unwrap_err();
        assert!(matches!(error, PortfolioError::InsufficientPairs { .. }));
    }

    #[test]
    fn test_portfolio_new_rejects_concentration_cap_exceeded() {
        // 10 pairs all with the same long ticker → exceeds 20% cap on a 20-ticker portfolio
        let long_tickers = ["AAPL"; 10];
        let short_tickers = [
            "WMT", "TGT", "KO", "PEP", "JNJ", "PFE", "MRK", "ABT", "UNH", "CVS",
        ];
        let pairs: Vec<FilledPair> = (0..10)
            .map(|index| make_filled_pair(long_tickers[index], short_tickers[index], 1.0, 1.0))
            .collect();
        let constraints = make_constraints();
        let error = Portfolio::new(pairs, &constraints).unwrap_err();
        assert!(matches!(
            error,
            PortfolioError::ConcentrationCapExceeded { ticker } if ticker == "AAPL"
        ));
    }

    #[test]
    fn test_portfolio_new_rejects_dollar_neutrality_violation() {
        let long_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            "AAPL".to_string(),
            OrderSide::Long,
            Decimal::from(100),
            "market".to_string(),
            None,
            "alpaca-long-aapl".to_string(),
            Utc::now(),
        );
        let short_order = Order::<crate::domain::orders::Pending>::new(
            Uuid::new_v4(),
            "WMT".to_string(),
            OrderSide::Short,
            Decimal::from(100),
            "market".to_string(),
            None,
            "alpaca-short-wmt".to_string(),
            Utc::now(),
        );
        let pending = PendingPair::new(long_order, short_order, 1.0, 1.0);
        // long: 100*100=10,000; short: 50*100=5,000 → imbalance ≫ 5%
        let imbalanced = pending
            .confirm(
                Some(FilledOrder {
                    alpaca_order_id: "fill-long".to_string(),
                    fill_price: Decimal::from(100),
                    filled_quantity: Decimal::from(100),
                }),
                Some(FilledOrder {
                    alpaca_order_id: "fill-short".to_string(),
                    fill_price: Decimal::from(50),
                    filled_quantity: Decimal::from(100),
                }),
            )
            .unwrap();

        // Build 10 valid pairs plus the one imbalanced pair (which becomes index 0 after replacing)
        let mut pairs = make_balanced_pairs(9);
        pairs.insert(0, imbalanced);

        let constraints = make_constraints();
        let error = Portfolio::new(pairs, &constraints).unwrap_err();
        assert!(matches!(
            error,
            PortfolioError::DollarNeutralityViolation { .. }
        ));
    }

    #[test]
    fn test_portfolio_new_rejects_beta_neutrality_violation() {
        // All pairs have long_beta=2.0 and short_beta=1.0 → net_beta = 10*1.0 = 10.0 >> 0.1
        let pairs: Vec<FilledPair> = (0..10)
            .map(|index| {
                let long_tickers = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"];
                let short_tickers = ["K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"];
                make_filled_pair(long_tickers[index], short_tickers[index], 2.0, 1.0)
            })
            .collect();
        let constraints = make_constraints();
        let error = Portfolio::new(pairs, &constraints).unwrap_err();
        assert!(matches!(
            error,
            PortfolioError::BetaNeutralityViolation { .. }
        ));
    }

    #[test]
    fn test_portfolio_error_display() {
        let error = PortfolioError::InsufficientPairs {
            required: 10,
            found: 5,
        };
        assert!(format!("{}", error).contains("10"));
        assert!(format!("{}", error).contains("5"));

        let error = PortfolioError::ConcentrationCapExceeded {
            ticker: "AAPL".to_string(),
        };
        assert!(format!("{}", error).contains("AAPL"));

        let error = PortfolioError::DollarNeutralityViolation { pair_index: 3 };
        assert!(format!("{}", error).contains("3"));

        let error = PortfolioError::BetaNeutralityViolation {
            net_beta: 0.5,
            tolerance: 0.1,
        };
        assert!(format!("{}", error).contains("0.5000"));
    }

    #[test]
    fn test_required_pairs_constant() {
        assert_eq!(REQUIRED_PAIRS, 10);
    }

    #[test]
    fn test_constraints_construction() {
        let constraints = make_constraints();
        assert_eq!(constraints.drawdown_threshold().0.value(), 0.10);
        assert_eq!(constraints.concentration_cap().0.value(), 0.20);
        assert_eq!(constraints.minimum_pairs().0.get(), 10);
        assert_eq!(constraints.beta_tolerance().value(), 0.1);
    }

    #[test]
    fn test_beta_tolerance_new_rejects_nonpositive() {
        assert!(BetaTolerance::new(0.0).is_err());
        assert!(BetaTolerance::new(-0.1).is_err());
    }

    #[test]
    fn test_beta_tolerance_new_accepts_positive() {
        let tolerance = BetaTolerance::new(0.1).unwrap();
        assert_eq!(tolerance.value(), 0.1);
    }
}
