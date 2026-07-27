//! Pre-trade risk gate for position request validation.
//!
//! Every position request must pass through the risk gate before execution,
//! regardless of the originating strategy. The gate evaluates margin
//! utilization, position concentration, strategy budget compliance, and
//! end-of-day exit feasibility. All checks are pure functions of current
//! portfolio state and the proposed position — no side effects, no I/O.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::common::market_hours::MarketSession;
use crate::domain::portfolio::ConcentrationCap;
use crate::domain::primitives::Percent;

/// Day-trade buying power multiplier for Pattern Day Trader margin accounts.
///
/// Under Reg T with PDT status, buying power = 4 × (equity − initial_margin).
/// All fund positions are intraday (EOD liquidation), so the 4× multiplier applies.
const BUYING_POWER_MULTIPLIER: f64 = 4.0;

/// Identifies a trading strategy for budget allocation and attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyId {
    StatisticalArbitrage,
}

impl std::fmt::Display for StrategyId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrategyId::StatisticalArbitrage => write!(formatter, "statistical_arbitrage"),
        }
    }
}

/// Asset type for a position request.
///
/// The `Option` variant exists so the risk gate interface supports options
/// positions without modification when volatility arbitrage is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetType {
    Equity,
    Option,
}

impl std::fmt::Display for AssetType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssetType::Equity => write!(formatter, "equity"),
            AssetType::Option => write!(formatter, "option"),
        }
    }
}

/// Maximum allowed fraction of account equity consumed by margin.
///
/// Wraps a validated [`Percent`]; construction is infallible given a valid percent.
#[derive(Debug, Clone, Copy)]
pub struct MarginUtilizationLimit(Percent);

impl MarginUtilizationLimit {
    /// Constructs a margin utilization limit from a validated percentage.
    pub fn new(limit: Percent) -> Self {
        Self(limit)
    }

    /// Returns the limit as a fraction in `[0.0, 1.0]`.
    pub fn value(self) -> f64 {
        self.0.value()
    }
}

/// Maximum allowed participation rate in remaining session volume for exit feasibility.
///
/// Wraps a validated [`Percent`]; construction is infallible given a valid percent.
#[derive(Debug, Clone, Copy)]
pub struct MaximumParticipationRate(Percent);

impl MaximumParticipationRate {
    /// Constructs a maximum participation rate from a validated percentage.
    pub fn new(rate: Percent) -> Self {
        Self(rate)
    }

    /// Returns the rate as a fraction in `[0.0, 1.0]`.
    pub fn value(self) -> f64 {
        self.0.value()
    }
}

/// A strategy's proposal to enter a new position.
#[derive(Debug, Clone)]
pub struct PositionRequest {
    /// Ticker symbol of the underlying.
    pub ticker: String,
    /// Type of asset being traded.
    pub asset_type: AssetType,
    /// Dollar notional of the proposed position.
    pub notional: f64,
    /// Strategy originating the request.
    pub strategy: StrategyId,
}

/// A single position in the current portfolio snapshot.
#[derive(Debug, Clone)]
pub struct PositionSnapshot {
    /// Ticker symbol.
    pub ticker: String,
    /// Absolute market value (always positive, regardless of side).
    pub market_value_absolute: f64,
    /// Strategy that owns this position.
    pub strategy: StrategyId,
}

/// Current portfolio state for risk gate evaluation.
///
/// The caller constructs this from live Alpaca account and position data.
/// The risk gate operates on the snapshot without performing any I/O.
#[derive(Debug, Clone)]
pub struct PortfolioSnapshot {
    /// Net account value (cash + long_market_value + short_market_value).
    pub account_equity: f64,
    /// Remaining buying power under current margin usage.
    pub buying_power: f64,
    /// All open positions with strategy attribution.
    pub positions: Vec<PositionSnapshot>,
}

/// Liquidity metrics for exit feasibility assessment.
#[derive(Debug, Clone)]
pub struct LiquidityMetrics {
    /// Average daily traded volume in dollar terms.
    pub average_daily_volume_dollars: f64,
}

/// Configuration for the risk gate.
///
/// All thresholds are validated at construction via their wrapped types.
#[derive(Debug, Clone)]
pub struct RiskGateConfiguration {
    /// Maximum fraction of account equity that may be consumed by margin.
    pub margin_utilization_limit: MarginUtilizationLimit,
    /// Maximum fraction of account equity for any single underlying.
    pub concentration_cap: ConcentrationCap,
    /// Capital allocation per strategy as a fraction of total equity.
    pub strategy_budgets: HashMap<StrategyId, Percent>,
    /// Maximum participation in remaining session volume for exit feasibility.
    pub maximum_participation_rate: MaximumParticipationRate,
}

/// Reason a position request was rejected by the risk gate.
#[derive(Debug, Clone, PartialEq)]
pub enum RejectionReason {
    /// Proposed position would push margin utilization above the limit.
    MarginUtilizationExceeded {
        current_utilization: f64,
        projected_utilization: f64,
        limit: f64,
    },
    /// Proposed position would push single-underlying exposure above the cap.
    ConcentrationCapExceeded {
        ticker: String,
        projected_fraction: f64,
        cap: f64,
    },
    /// Proposed position would push strategy capital usage above its budget.
    StrategyBudgetExceeded {
        strategy: StrategyId,
        projected_usage: f64,
        budget: f64,
    },
    /// Insufficient remaining session volume to exit the position before close.
    ExitFeasibilityInsufficient {
        participation_rate: f64,
        maximum_participation_rate: f64,
        minutes_remaining: u32,
    },
    /// Position request submitted outside trading hours.
    OutsideTradingSession,
    /// Strategy has no allocated budget.
    StrategyNotAllocated { strategy: StrategyId },
    /// Position request contains invalid values (negative, zero, or non-finite notional).
    InvalidRequest { reason: String },
}

impl std::fmt::Display for RejectionReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectionReason::MarginUtilizationExceeded {
                current_utilization,
                projected_utilization,
                limit,
            } => write!(
                formatter,
                "Margin utilization would increase from {:.2}% to {:.2}%, \
                 exceeding limit of {:.2}%.",
                current_utilization * 100.0,
                projected_utilization * 100.0,
                limit * 100.0,
            ),
            RejectionReason::ConcentrationCapExceeded {
                ticker,
                projected_fraction,
                cap,
            } => write!(
                formatter,
                "Ticker {ticker} would reach {:.2}% of equity, exceeding cap of {:.2}%.",
                projected_fraction * 100.0,
                cap * 100.0,
            ),
            RejectionReason::StrategyBudgetExceeded {
                strategy,
                projected_usage,
                budget,
            } => write!(
                formatter,
                "Strategy {strategy} would use {:.2}% of equity, \
                 exceeding budget of {:.2}%.",
                projected_usage * 100.0,
                budget * 100.0,
            ),
            RejectionReason::ExitFeasibilityInsufficient {
                participation_rate,
                maximum_participation_rate,
                minutes_remaining,
            } => write!(
                formatter,
                "Exit participation rate would be {:.2}% of remaining volume \
                 ({minutes_remaining} minutes left), exceeding maximum of {:.2}%.",
                participation_rate * 100.0,
                maximum_participation_rate * 100.0,
            ),
            RejectionReason::OutsideTradingSession => {
                write!(
                    formatter,
                    "Position request submitted outside trading hours."
                )
            }
            RejectionReason::StrategyNotAllocated { strategy } => {
                write!(formatter, "Strategy {strategy} has no allocated budget.")
            }
            RejectionReason::InvalidRequest { reason } => {
                write!(formatter, "Invalid position request: {reason}.")
            }
        }
    }
}

impl std::error::Error for RejectionReason {}

/// Result of the risk gate evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum RiskGateDecision {
    /// All checks passed; the position may proceed to execution.
    Approved,
    /// One or more checks failed; the position must not be executed.
    Rejected { reasons: Vec<RejectionReason> },
}

impl RiskGateDecision {
    /// Returns `true` if the position request was approved.
    pub fn is_approved(&self) -> bool {
        matches!(self, RiskGateDecision::Approved)
    }
}

/// Computes margin utilization as a fraction of account equity.
///
/// Returns a value in `[0.0, 1.0]` where 0.0 means fully cash and 1.0 means
/// all buying power is consumed. Returns 1.0 for zero or negative equity.
fn margin_utilization(equity: f64, buying_power: f64) -> f64 {
    if equity <= 0.0 {
        return 1.0;
    }
    (1.0 - buying_power / (BUYING_POWER_MULTIPLIER * equity)).clamp(0.0, 1.0)
}

/// Checks whether the proposed position would push margin utilization above the limit.
///
/// Under Reg T, each dollar of equity notional consumes approximately 50% as
/// initial margin, which translates to 2× the notional in buying power consumed
/// (due to the 4× day-trade multiplier).
fn check_margin_utilization(
    snapshot: &PortfolioSnapshot,
    request: &PositionRequest,
    limit: MarginUtilizationLimit,
) -> Option<RejectionReason> {
    let equity = snapshot.account_equity;
    if equity <= 0.0 {
        return Some(RejectionReason::MarginUtilizationExceeded {
            current_utilization: 1.0,
            projected_utilization: 1.0,
            limit: limit.value(),
        });
    }

    let current_utilization = margin_utilization(equity, snapshot.buying_power);
    let estimated_buying_power_consumed = 2.0 * request.notional;
    let projected_buying_power = snapshot.buying_power - estimated_buying_power_consumed;
    let projected_utilization = margin_utilization(equity, projected_buying_power);

    if projected_utilization > limit.value() {
        Some(RejectionReason::MarginUtilizationExceeded {
            current_utilization,
            projected_utilization,
            limit: limit.value(),
        })
    } else {
        None
    }
}

/// Checks whether the proposed position would push single-underlying concentration above the cap.
///
/// Concentration is measured as the proposed ticker's total exposure (existing +
/// proposed) divided by account equity, not by total deployed notional. This
/// gives meaningful results for partially-deployed portfolios where total
/// notional is much less than equity.
fn check_concentration(
    snapshot: &PortfolioSnapshot,
    request: &PositionRequest,
    cap: ConcentrationCap,
) -> Option<RejectionReason> {
    let equity = snapshot.account_equity;
    if equity <= 0.0 {
        return Some(RejectionReason::ConcentrationCapExceeded {
            ticker: request.ticker.clone(),
            projected_fraction: 1.0,
            cap: cap.0.value(),
        });
    }

    let existing_ticker_exposure: f64 = snapshot
        .positions
        .iter()
        .filter(|position| position.ticker == request.ticker)
        .map(|position| position.market_value_absolute)
        .sum();

    let projected_exposure = existing_ticker_exposure + request.notional;
    let projected_fraction = projected_exposure / equity;

    if projected_fraction > cap.0.value() {
        Some(RejectionReason::ConcentrationCapExceeded {
            ticker: request.ticker.clone(),
            projected_fraction,
            cap: cap.0.value(),
        })
    } else {
        None
    }
}

/// Checks whether the proposed position would push strategy capital usage above its budget.
///
/// Capital usage is the sum of absolute market values of all positions
/// attributed to the requesting strategy, plus the proposed position's notional,
/// divided by account equity.
fn check_strategy_budget(
    snapshot: &PortfolioSnapshot,
    request: &PositionRequest,
    strategy_budgets: &HashMap<StrategyId, Percent>,
) -> Option<RejectionReason> {
    let budget = match strategy_budgets.get(&request.strategy) {
        Some(allocation) => allocation,
        None => {
            return Some(RejectionReason::StrategyNotAllocated {
                strategy: request.strategy,
            });
        }
    };

    let equity = snapshot.account_equity;
    if equity <= 0.0 {
        return Some(RejectionReason::StrategyBudgetExceeded {
            strategy: request.strategy,
            projected_usage: 1.0,
            budget: budget.value(),
        });
    }

    let current_usage: f64 = snapshot
        .positions
        .iter()
        .filter(|position| position.strategy == request.strategy)
        .map(|position| position.market_value_absolute)
        .sum();

    let projected_usage = (current_usage + request.notional) / equity;

    if projected_usage > budget.value() {
        Some(RejectionReason::StrategyBudgetExceeded {
            strategy: request.strategy,
            projected_usage,
            budget: budget.value(),
        })
    } else {
        None
    }
}

/// Checks whether the proposed position can be exited before market close.
///
/// Uses a linear volume distribution model: remaining session volume is
/// estimated as `average_daily_volume × (minutes_remaining / total_minutes)`.
/// The position's participation rate (notional / estimated remaining volume)
/// must be below the configured maximum.
///
/// The check becomes naturally stricter as the session progresses because the
/// remaining volume shrinks while the position size stays constant. Both the
/// remaining and total minutes come from `session`, so an early close tightens
/// the gate for the whole day rather than letting it approve entries against
/// hours of liquidity that will never arrive.
fn check_exit_feasibility(
    session: &MarketSession,
    now: DateTime<Utc>,
    request: &PositionRequest,
    liquidity: &LiquidityMetrics,
    maximum_participation_rate: MaximumParticipationRate,
) -> Option<RejectionReason> {
    if !session.contains(now) {
        return Some(RejectionReason::OutsideTradingSession);
    }

    let total_minutes = session.total_minutes();
    if total_minutes == 0 {
        return Some(RejectionReason::OutsideTradingSession);
    }

    // minutes_remaining is always >= 1 here because `contains` uses the
    // end-exclusive range [open, close), so passing it guarantees time remains.
    let minutes_remaining = session.minutes_remaining(now);
    let remaining_fraction = minutes_remaining as f64 / total_minutes as f64;
    let estimated_remaining_volume = liquidity.average_daily_volume_dollars * remaining_fraction;

    if estimated_remaining_volume <= 0.0 {
        return Some(RejectionReason::ExitFeasibilityInsufficient {
            participation_rate: f64::INFINITY,
            maximum_participation_rate: maximum_participation_rate.value(),
            minutes_remaining,
        });
    }

    let participation_rate = request.notional / estimated_remaining_volume;

    if participation_rate > maximum_participation_rate.value() {
        Some(RejectionReason::ExitFeasibilityInsufficient {
            participation_rate,
            maximum_participation_rate: maximum_participation_rate.value(),
            minutes_remaining,
        })
    } else {
        None
    }
}

/// Evaluates a position request against all risk gate checks.
///
/// All checks run regardless of individual failures, so the caller receives
/// the complete set of violations for logging and diagnostics.
pub fn evaluate(
    config: &RiskGateConfiguration,
    snapshot: &PortfolioSnapshot,
    request: &PositionRequest,
    liquidity: &LiquidityMetrics,
    session: &MarketSession,
    now: DateTime<Utc>,
) -> RiskGateDecision {
    if !request.notional.is_finite() || request.notional <= 0.0 {
        return RiskGateDecision::Rejected {
            reasons: vec![RejectionReason::InvalidRequest {
                reason: format!(
                    "Notional must be positive and finite, got {}",
                    request.notional
                ),
            }],
        };
    }

    let mut reasons = Vec::new();

    if let Some(reason) =
        check_margin_utilization(snapshot, request, config.margin_utilization_limit)
    {
        reasons.push(reason);
    }
    if let Some(reason) = check_concentration(snapshot, request, config.concentration_cap) {
        reasons.push(reason);
    }
    if let Some(reason) = check_strategy_budget(snapshot, request, &config.strategy_budgets) {
        reasons.push(reason);
    }
    if let Some(reason) = check_exit_feasibility(
        session,
        now,
        request,
        liquidity,
        config.maximum_participation_rate,
    ) {
        reasons.push(reason);
    }

    if reasons.is_empty() {
        RiskGateDecision::Approved
    } else {
        RiskGateDecision::Rejected { reasons }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> RiskGateConfiguration {
        let mut strategy_budgets = HashMap::new();
        strategy_budgets.insert(StrategyId::StatisticalArbitrage, Percent::new(1.0).unwrap());
        RiskGateConfiguration {
            margin_utilization_limit: MarginUtilizationLimit::new(Percent::new(0.80).unwrap()),
            concentration_cap: ConcentrationCap(Percent::new(0.20).unwrap()),
            strategy_budgets,
            maximum_participation_rate: MaximumParticipationRate::new(Percent::new(0.10).unwrap()),
        }
    }

    fn empty_snapshot(equity: f64) -> PortfolioSnapshot {
        PortfolioSnapshot {
            account_equity: equity,
            buying_power: BUYING_POWER_MULTIPLIER * equity,
            positions: vec![],
        }
    }

    fn default_request() -> PositionRequest {
        PositionRequest {
            ticker: "AAPL".to_string(),
            asset_type: AssetType::Equity,
            notional: 10_000.0,
            strategy: StrategyId::StatisticalArbitrage,
        }
    }

    fn default_liquidity() -> LiquidityMetrics {
        LiquidityMetrics {
            average_daily_volume_dollars: 10_000_000.0,
        }
    }

    /// 10:00 AM EDT on a Monday (2024-07-15), 360 minutes until close.
    fn trading_hours() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2024-07-15T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    // ---- margin_utilization helper ----

    #[test]
    fn test_margin_utilization_fully_cash() {
        let result = margin_utilization(100_000.0, 400_000.0);
        assert!((result - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_margin_utilization_half_deployed() {
        let result = margin_utilization(100_000.0, 200_000.0);
        assert!((result - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_margin_utilization_fully_deployed() {
        let result = margin_utilization(100_000.0, 0.0);
        assert!((result - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_margin_utilization_zero_equity() {
        assert!((margin_utilization(0.0, 0.0) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_margin_utilization_clamps_negative() {
        // Negative buying power (margin call territory) should clamp to 1.0.
        let result = margin_utilization(100_000.0, -50_000.0);
        assert!((result - 1.0).abs() < f64::EPSILON);
    }

    // ---- check_margin_utilization ----

    #[test]
    fn test_margin_check_empty_portfolio_small_position_passes() {
        let snapshot = empty_snapshot(100_000.0);
        let request = default_request();
        let limit = MarginUtilizationLimit::new(Percent::new(0.80).unwrap());
        assert!(check_margin_utilization(&snapshot, &request, limit).is_none());
    }

    #[test]
    fn test_margin_check_near_limit_rejects() {
        let snapshot = PortfolioSnapshot {
            account_equity: 100_000.0,
            // 80% utilized: buying_power = 4 × 100k × (1 − 0.80) = 80k
            buying_power: 80_000.0,
            positions: vec![],
        };
        let request = PositionRequest {
            notional: 10_000.0,
            ..default_request()
        };
        let limit = MarginUtilizationLimit::new(Percent::new(0.80).unwrap());
        let result = check_margin_utilization(&snapshot, &request, limit);
        assert!(result.is_some());
        match result.unwrap() {
            RejectionReason::MarginUtilizationExceeded {
                projected_utilization,
                limit: limit_value,
                ..
            } => {
                assert!(projected_utilization > limit_value);
            }
            other => panic!("Expected MarginUtilizationExceeded, got {other:?}"),
        }
    }

    #[test]
    fn test_margin_check_exactly_at_limit_passes() {
        // Construct a scenario where projected utilization equals the limit exactly.
        // equity = 100k, limit = 0.50.
        // We want projected_utilization = 0.50, meaning projected_buying_power = 200k.
        // projected_buying_power = buying_power − 2 × notional.
        // If notional = 10k and buying_power = 220k:
        // projected_buying_power = 220k − 20k = 200k.
        // projected_utilization = 1 − 200k/400k = 0.50. Exactly at limit → passes.
        let snapshot = PortfolioSnapshot {
            account_equity: 100_000.0,
            buying_power: 220_000.0,
            positions: vec![],
        };
        let request = PositionRequest {
            notional: 10_000.0,
            ..default_request()
        };
        let limit = MarginUtilizationLimit::new(Percent::new(0.50).unwrap());
        assert!(check_margin_utilization(&snapshot, &request, limit).is_none());
    }

    #[test]
    fn test_margin_check_zero_equity_rejects() {
        let snapshot = PortfolioSnapshot {
            account_equity: 0.0,
            buying_power: 0.0,
            positions: vec![],
        };
        let request = default_request();
        let limit = MarginUtilizationLimit::new(Percent::new(0.80).unwrap());
        assert!(check_margin_utilization(&snapshot, &request, limit).is_some());
    }

    // ---- check_concentration ----

    #[test]
    fn test_concentration_new_ticker_under_cap_passes() {
        let snapshot = empty_snapshot(100_000.0);
        let request = PositionRequest {
            notional: 15_000.0, // 15% of equity
            ..default_request()
        };
        let cap = ConcentrationCap(Percent::new(0.20).unwrap());
        assert!(check_concentration(&snapshot, &request, cap).is_none());
    }

    #[test]
    fn test_concentration_exceeds_cap_rejects() {
        let snapshot = empty_snapshot(100_000.0);
        let request = PositionRequest {
            notional: 25_000.0, // 25% of equity
            ..default_request()
        };
        let cap = ConcentrationCap(Percent::new(0.20).unwrap());
        let result = check_concentration(&snapshot, &request, cap);
        assert!(result.is_some());
        match result.unwrap() {
            RejectionReason::ConcentrationCapExceeded {
                ticker,
                projected_fraction,
                cap: cap_value,
            } => {
                assert_eq!(ticker, "AAPL");
                assert!((projected_fraction - 0.25).abs() < 1e-10);
                assert!((cap_value - 0.20).abs() < 1e-10);
            }
            other => panic!("Expected ConcentrationCapExceeded, got {other:?}"),
        }
    }

    #[test]
    fn test_concentration_cumulative_with_existing_position() {
        let snapshot = PortfolioSnapshot {
            account_equity: 100_000.0,
            buying_power: 300_000.0,
            positions: vec![PositionSnapshot {
                ticker: "AAPL".to_string(),
                market_value_absolute: 15_000.0,
                strategy: StrategyId::StatisticalArbitrage,
            }],
        };
        // 15k existing + 10k proposed = 25k = 25% > 20%
        let request = PositionRequest {
            ticker: "AAPL".to_string(),
            notional: 10_000.0,
            ..default_request()
        };
        let cap = ConcentrationCap(Percent::new(0.20).unwrap());
        assert!(check_concentration(&snapshot, &request, cap).is_some());
    }

    #[test]
    fn test_concentration_different_ticker_no_cumulation() {
        let snapshot = PortfolioSnapshot {
            account_equity: 100_000.0,
            buying_power: 300_000.0,
            positions: vec![PositionSnapshot {
                ticker: "AAPL".to_string(),
                market_value_absolute: 19_000.0,
                strategy: StrategyId::StatisticalArbitrage,
            }],
        };
        let request = PositionRequest {
            ticker: "MSFT".to_string(),
            notional: 15_000.0, // 15% for MSFT, under cap
            ..default_request()
        };
        let cap = ConcentrationCap(Percent::new(0.20).unwrap());
        assert!(check_concentration(&snapshot, &request, cap).is_none());
    }

    #[test]
    fn test_concentration_zero_equity_rejects() {
        let snapshot = PortfolioSnapshot {
            account_equity: 0.0,
            buying_power: 0.0,
            positions: vec![],
        };
        let request = default_request();
        let cap = ConcentrationCap(Percent::new(0.20).unwrap());
        assert!(check_concentration(&snapshot, &request, cap).is_some());
    }

    // ---- check_strategy_budget ----

    #[test]
    fn test_strategy_budget_under_limit_passes() {
        let snapshot = PortfolioSnapshot {
            account_equity: 100_000.0,
            buying_power: 300_000.0,
            positions: vec![PositionSnapshot {
                ticker: "AAPL".to_string(),
                market_value_absolute: 10_000.0,
                strategy: StrategyId::StatisticalArbitrage,
            }],
        };
        let request = PositionRequest {
            notional: 10_000.0, // 10k + 10k = 20k = 20% < 100%
            ..default_request()
        };
        let mut budgets = HashMap::new();
        budgets.insert(StrategyId::StatisticalArbitrage, Percent::new(1.0).unwrap());
        assert!(check_strategy_budget(&snapshot, &request, &budgets).is_none());
    }

    #[test]
    fn test_strategy_budget_exceeds_limit_rejects() {
        let snapshot = PortfolioSnapshot {
            account_equity: 100_000.0,
            buying_power: 200_000.0,
            positions: vec![PositionSnapshot {
                ticker: "AAPL".to_string(),
                market_value_absolute: 55_000.0,
                strategy: StrategyId::StatisticalArbitrage,
            }],
        };
        let request = PositionRequest {
            notional: 10_000.0, // 55k + 10k = 65k = 65% > 60%
            ..default_request()
        };
        let mut budgets = HashMap::new();
        budgets.insert(
            StrategyId::StatisticalArbitrage,
            Percent::new(0.60).unwrap(),
        );
        let result = check_strategy_budget(&snapshot, &request, &budgets);
        assert!(result.is_some());
        match result.unwrap() {
            RejectionReason::StrategyBudgetExceeded {
                strategy,
                projected_usage,
                budget,
            } => {
                assert_eq!(strategy, StrategyId::StatisticalArbitrage);
                assert!((projected_usage - 0.65).abs() < 1e-10);
                assert!((budget - 0.60).abs() < 1e-10);
            }
            other => panic!("Expected StrategyBudgetExceeded, got {other:?}"),
        }
    }

    #[test]
    fn test_strategy_budget_unallocated_strategy_rejects() {
        let snapshot = empty_snapshot(100_000.0);
        let request = default_request();
        let budgets = HashMap::new();
        let result = check_strategy_budget(&snapshot, &request, &budgets);
        assert!(result.is_some());
        assert!(matches!(
            result.unwrap(),
            RejectionReason::StrategyNotAllocated { .. }
        ));
    }

    #[test]
    fn test_strategy_budget_zero_equity_rejects() {
        let snapshot = PortfolioSnapshot {
            account_equity: 0.0,
            buying_power: 0.0,
            positions: vec![],
        };
        let request = default_request();
        let mut budgets = HashMap::new();
        budgets.insert(StrategyId::StatisticalArbitrage, Percent::new(1.0).unwrap());
        assert!(check_strategy_budget(&snapshot, &request, &budgets).is_some());
    }

    // ---- session-derived minutes ----

    /// Regular session on Monday 2024-07-15: 09:30–16:00 EDT.
    fn regular_session() -> MarketSession {
        MarketSession::new(true, utc("2024-07-15T20:00:00Z"))
            .expect("regular session should construct")
    }

    /// Early-close session on the same date: 09:30–13:00 EDT, 210 minutes.
    fn early_close_session() -> MarketSession {
        MarketSession::new(true, utc("2024-07-15T17:00:00Z"))
            .expect("early close session should construct")
    }

    fn utc(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("valid RFC3339 timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn test_minutes_remaining_mid_session() {
        // 10:00 AM EDT = 14:00 UTC → 360 minutes until 16:00 ET
        assert_eq!(regular_session().minutes_remaining(trading_hours()), 360);
    }

    #[test]
    fn test_minutes_remaining_near_close() {
        // 3:55 PM EDT = 19:55 UTC → 5 minutes
        assert_eq!(
            regular_session().minutes_remaining(utc("2024-07-15T19:55:00Z")),
            5
        );
    }

    #[test]
    fn test_minutes_remaining_at_close() {
        // 4:00 PM EDT = 20:00 UTC → 0 minutes
        assert_eq!(
            regular_session().minutes_remaining(utc("2024-07-15T20:00:00Z")),
            0
        );
    }

    #[test]
    fn test_minutes_remaining_at_open() {
        // 9:30 AM EDT = 13:30 UTC → 390 minutes
        assert_eq!(
            regular_session().minutes_remaining(utc("2024-07-15T13:30:00Z")),
            390
        );
    }

    #[test]
    fn test_early_close_session_is_shorter() {
        let session = early_close_session();
        assert_eq!(session.total_minutes(), 210);
        // 12:50 PM EDT = 16:50 UTC → only 10 minutes left, not 190.
        assert_eq!(session.minutes_remaining(utc("2024-07-15T16:50:00Z")), 10);
    }

    // ---- check_exit_feasibility ----

    #[test]
    fn test_exit_feasibility_early_session_small_position_passes() {
        let now = trading_hours();
        let request = PositionRequest {
            notional: 100_000.0,
            ..default_request()
        };
        let liquidity = LiquidityMetrics {
            average_daily_volume_dollars: 50_000_000.0,
        };
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());
        assert!(
            check_exit_feasibility(&regular_session(), now, &request, &liquidity, max_rate)
                .is_none()
        );
    }

    #[test]
    fn test_exit_feasibility_late_session_large_position_rejects() {
        // 3:30 PM EDT = 19:30 UTC, 30 minutes remaining
        let now = utc("2024-07-15T19:30:00Z");
        let request = PositionRequest {
            notional: 1_000_000.0,
            ..default_request()
        };
        let liquidity = LiquidityMetrics {
            average_daily_volume_dollars: 5_000_000.0,
        };
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());
        let result =
            check_exit_feasibility(&regular_session(), now, &request, &liquidity, max_rate);
        assert!(result.is_some());
        match result.unwrap() {
            RejectionReason::ExitFeasibilityInsufficient {
                participation_rate,
                minutes_remaining,
                ..
            } => {
                assert_eq!(minutes_remaining, 30);
                // remaining_fraction = 30/390 ≈ 0.077
                // remaining_volume ≈ 5M × 0.077 ≈ 384.6k
                // participation = 1M / 384.6k ≈ 2.6
                assert!(participation_rate > 0.10);
            }
            other => panic!("Expected ExitFeasibilityInsufficient, got {other:?}"),
        }
    }

    #[test]
    fn test_exit_feasibility_outside_trading_hours_rejects() {
        // 8:00 AM EDT = 12:00 UTC, before market open
        let now = utc("2024-07-15T12:00:00Z");
        let request = default_request();
        let liquidity = default_liquidity();
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());
        let result =
            check_exit_feasibility(&regular_session(), now, &request, &liquidity, max_rate);
        assert!(result.is_some());
        assert!(matches!(
            result.unwrap(),
            RejectionReason::OutsideTradingSession
        ));
    }

    #[test]
    fn test_exit_feasibility_before_session_date_rejects() {
        // Saturday 10:00 AM EDT, two days before the session being evaluated.
        let now = utc("2024-07-13T14:00:00Z");
        let request = default_request();
        let liquidity = default_liquidity();
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());
        let result =
            check_exit_feasibility(&regular_session(), now, &request, &liquidity, max_rate);
        assert!(result.is_some());
        assert!(matches!(
            result.unwrap(),
            RejectionReason::OutsideTradingSession
        ));
    }

    #[test]
    fn test_exit_feasibility_zero_adv_rejects() {
        let now = trading_hours();
        let request = default_request();
        let liquidity = LiquidityMetrics {
            average_daily_volume_dollars: 0.0,
        };
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());
        assert!(
            check_exit_feasibility(&regular_session(), now, &request, &liquidity, max_rate)
                .is_some()
        );
    }

    #[test]
    fn test_exit_feasibility_naturally_stricter_late_session() {
        // Same position and ADV, but later in the session should be rejected
        // while early session passes.
        let request = PositionRequest {
            notional: 500_000.0,
            ..default_request()
        };
        let liquidity = LiquidityMetrics {
            average_daily_volume_dollars: 10_000_000.0,
        };
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());
        let session = regular_session();

        // 10:00 AM EDT: 360 min remaining, est volume = 10M × 360/390 ≈ 9.23M
        // participation = 500k / 9.23M ≈ 5.4% < 10% → pass
        let early = trading_hours();
        assert!(check_exit_feasibility(&session, early, &request, &liquidity, max_rate).is_none());

        // 3:50 PM EDT: 10 min remaining, est volume = 10M × 10/390 ≈ 25.6k
        // participation = 500k / 25.6k ≈ 19.5 > 10% → fail
        let late = utc("2024-07-15T19:50:00Z");
        assert!(check_exit_feasibility(&session, late, &request, &liquidity, max_rate).is_some());
    }

    #[test]
    fn test_exit_feasibility_early_close_rejects_what_regular_close_allows() {
        // 12:50 PM EDT. Under a regular close 190 minutes remain and the position
        // is comfortably feasible; under a 13:00 close only 10 minutes remain and
        // it is not. This is the case the old hardcoded 390-minute session got
        // wrong: it approved entries against liquidity that would never arrive.
        let now = utc("2024-07-15T16:50:00Z");
        let request = PositionRequest {
            notional: 400_000.0,
            ..default_request()
        };
        let liquidity = LiquidityMetrics {
            average_daily_volume_dollars: 10_000_000.0,
        };
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());

        assert!(
            check_exit_feasibility(&regular_session(), now, &request, &liquidity, max_rate)
                .is_none()
        );
        assert!(check_exit_feasibility(
            &early_close_session(),
            now,
            &request,
            &liquidity,
            max_rate
        )
        .is_some());
    }

    #[test]
    fn test_exit_feasibility_after_early_close_rejects() {
        // 2:00 PM EDT is inside a regular session but past a 13:00 close.
        let now = utc("2024-07-15T18:00:00Z");
        let request = default_request();
        let liquidity = default_liquidity();
        let max_rate = MaximumParticipationRate::new(Percent::new(0.10).unwrap());

        assert!(matches!(
            check_exit_feasibility(&early_close_session(), now, &request, &liquidity, max_rate),
            Some(RejectionReason::OutsideTradingSession)
        ));
    }

    // ---- evaluate (full gate) ----

    #[test]
    fn test_evaluate_all_checks_pass() {
        let config = default_config();
        let snapshot = empty_snapshot(100_000.0);
        let request = default_request();
        let liquidity = default_liquidity();
        let now = trading_hours();
        let decision = evaluate(
            &config,
            &snapshot,
            &request,
            &liquidity,
            &regular_session(),
            now,
        );
        assert!(decision.is_approved());
    }

    #[test]
    fn test_evaluate_collects_all_rejections() {
        let mut strategy_budgets = HashMap::new();
        strategy_budgets.insert(
            StrategyId::StatisticalArbitrage,
            Percent::new(0.05).unwrap(),
        );
        let config = RiskGateConfiguration {
            margin_utilization_limit: MarginUtilizationLimit::new(Percent::new(0.01).unwrap()),
            concentration_cap: ConcentrationCap(Percent::new(0.01).unwrap()),
            strategy_budgets,
            maximum_participation_rate: MaximumParticipationRate::new(Percent::new(0.10).unwrap()),
        };
        let snapshot = empty_snapshot(100_000.0);
        let request = PositionRequest {
            notional: 50_000.0,
            ..default_request()
        };
        let liquidity = default_liquidity();
        let now = trading_hours();
        let decision = evaluate(
            &config,
            &snapshot,
            &request,
            &liquidity,
            &regular_session(),
            now,
        );
        match decision {
            RiskGateDecision::Rejected { reasons } => {
                // Margin (projected 25% > 1%), concentration (50% > 1%), budget (50% > 5%)
                assert_eq!(reasons.len(), 3);
            }
            RiskGateDecision::Approved => panic!("Expected rejection"),
        }
    }

    #[test]
    fn test_evaluate_outside_trading_hours_includes_session_rejection() {
        let config = default_config();
        let snapshot = empty_snapshot(100_000.0);
        let request = default_request();
        let liquidity = default_liquidity();
        // Before market open
        let now = DateTime::parse_from_rfc3339("2024-07-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let decision = evaluate(
            &config,
            &snapshot,
            &request,
            &liquidity,
            &regular_session(),
            now,
        );
        match decision {
            RiskGateDecision::Rejected { reasons } => {
                assert!(reasons
                    .iter()
                    .any(|reason| matches!(reason, RejectionReason::OutsideTradingSession)));
            }
            RiskGateDecision::Approved => panic!("Expected rejection"),
        }
    }

    // ---- Display tests ----

    #[test]
    fn test_rejection_reason_margin_display() {
        let reason = RejectionReason::MarginUtilizationExceeded {
            current_utilization: 0.75,
            projected_utilization: 0.85,
            limit: 0.80,
        };
        let message = format!("{reason}");
        assert!(message.contains("75.00%"));
        assert!(message.contains("85.00%"));
        assert!(message.contains("80.00%"));
    }

    #[test]
    fn test_rejection_reason_concentration_display() {
        let reason = RejectionReason::ConcentrationCapExceeded {
            ticker: "AAPL".to_string(),
            projected_fraction: 0.25,
            cap: 0.20,
        };
        let message = format!("{reason}");
        assert!(message.contains("AAPL"));
        assert!(message.contains("25.00%"));
        assert!(message.contains("20.00%"));
    }

    #[test]
    fn test_rejection_reason_budget_display() {
        let reason = RejectionReason::StrategyBudgetExceeded {
            strategy: StrategyId::StatisticalArbitrage,
            projected_usage: 0.65,
            budget: 0.60,
        };
        let message = format!("{reason}");
        assert!(message.contains("statistical_arbitrage"));
        assert!(message.contains("65.00%"));
        assert!(message.contains("60.00%"));
    }

    #[test]
    fn test_rejection_reason_exit_feasibility_display() {
        let reason = RejectionReason::ExitFeasibilityInsufficient {
            participation_rate: 0.25,
            maximum_participation_rate: 0.10,
            minutes_remaining: 30,
        };
        let message = format!("{reason}");
        assert!(message.contains("25.00%"));
        assert!(message.contains("10.00%"));
        assert!(message.contains("30 minutes"));
    }

    #[test]
    fn test_rejection_reason_outside_session_display() {
        let message = format!("{}", RejectionReason::OutsideTradingSession);
        assert!(message.contains("outside trading hours"));
    }

    #[test]
    fn test_rejection_reason_not_allocated_display() {
        let reason = RejectionReason::StrategyNotAllocated {
            strategy: StrategyId::StatisticalArbitrage,
        };
        let message = format!("{reason}");
        assert!(message.contains("statistical_arbitrage"));
        assert!(message.contains("no allocated budget"));
    }

    #[test]
    fn test_strategy_id_display() {
        assert_eq!(
            format!("{}", StrategyId::StatisticalArbitrage),
            "statistical_arbitrage"
        );
    }

    #[test]
    fn test_asset_type_display() {
        assert_eq!(format!("{}", AssetType::Equity), "equity");
        assert_eq!(format!("{}", AssetType::Option), "option");
    }

    #[test]
    fn test_evaluate_rejects_negative_notional() {
        let config = default_config();
        let snapshot = empty_snapshot(100_000.0);
        let request = PositionRequest {
            notional: -10_000.0,
            ..default_request()
        };
        let liquidity = default_liquidity();
        let now = trading_hours();
        let decision = evaluate(
            &config,
            &snapshot,
            &request,
            &liquidity,
            &regular_session(),
            now,
        );
        match decision {
            RiskGateDecision::Rejected { reasons } => {
                assert_eq!(reasons.len(), 1);
                assert!(matches!(reasons[0], RejectionReason::InvalidRequest { .. }));
            }
            RiskGateDecision::Approved => panic!("Expected rejection for negative notional"),
        }
    }

    #[test]
    fn test_evaluate_rejects_zero_notional() {
        let config = default_config();
        let snapshot = empty_snapshot(100_000.0);
        let request = PositionRequest {
            notional: 0.0,
            ..default_request()
        };
        let liquidity = default_liquidity();
        let now = trading_hours();
        let decision = evaluate(
            &config,
            &snapshot,
            &request,
            &liquidity,
            &regular_session(),
            now,
        );
        assert!(!decision.is_approved());
    }

    #[test]
    fn test_evaluate_rejects_nan_notional() {
        let config = default_config();
        let snapshot = empty_snapshot(100_000.0);
        let request = PositionRequest {
            notional: f64::NAN,
            ..default_request()
        };
        let liquidity = default_liquidity();
        let now = trading_hours();
        let decision = evaluate(
            &config,
            &snapshot,
            &request,
            &liquidity,
            &regular_session(),
            now,
        );
        assert!(!decision.is_approved());
    }

    #[test]
    fn test_evaluate_rejects_infinite_notional() {
        let config = default_config();
        let snapshot = empty_snapshot(100_000.0);
        let request = PositionRequest {
            notional: f64::INFINITY,
            ..default_request()
        };
        let liquidity = default_liquidity();
        let now = trading_hours();
        let decision = evaluate(
            &config,
            &snapshot,
            &request,
            &liquidity,
            &regular_session(),
            now,
        );
        assert!(!decision.is_approved());
    }

    #[test]
    fn test_rejection_reason_invalid_request_display() {
        let reason = RejectionReason::InvalidRequest {
            reason: "Notional must be positive and finite, got -100".to_string(),
        };
        let message = format!("{reason}");
        assert!(message.contains("Invalid position request"));
        assert!(message.contains("-100"));
    }

    #[test]
    fn test_risk_gate_decision_is_approved() {
        assert!(RiskGateDecision::Approved.is_approved());
        assert!(!RiskGateDecision::Rejected {
            reasons: vec![RejectionReason::OutsideTradingSession]
        }
        .is_approved());
    }
}
