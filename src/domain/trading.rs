//! Trading lifecycle record types mirroring the PostgreSQL trading tables.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Groups one full rebalance cycle from allocation through order submission.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityRebalanceSession {
    pub id: Uuid,
    pub triggered_at: DateTime<Utc>,
    pub trigger_reason: String,
    /// References `model_runs.run_id`; nullable when the model run is unavailable.
    pub model_run_id: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: String,
}

/// One cointegrated long-short pair within a rebalance session.
///
/// Entry signal fields (`z_score`, `hedge_ratio`, `signal_strength`) are recorded
/// at the time the pair is opened.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityPair {
    pub id: Uuid,
    pub rebalance_id: Uuid,
    pub pair_id: String,
    pub long_ticker: String,
    pub short_ticker: String,
    pub z_score: Decimal,
    pub hedge_ratio: Decimal,
    pub signal_strength: Decimal,
    /// Either `"open"` or `"closed"`.
    pub status: String,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub realized_profit_and_loss: Option<Decimal>,
    pub return_percent: Option<Decimal>,
    pub holding_days: Option<i32>,
}

/// One ticker leg of an allocation within a rebalance session.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityAllocation {
    pub id: Uuid,
    pub rebalance_id: Uuid,
    pub equity_pair_id: Uuid,
    pub generated_at: DateTime<Utc>,
    /// References `model_runs.run_id`; nullable when unavailable.
    pub model_run_id: Option<String>,
    pub ticker: String,
    /// Either `"LONG"` or `"SHORT"`.
    pub side: String,
    /// Either `"OPEN_POSITION"`, `"CLOSE_POSITION"`, or `"UNSPECIFIED"`.
    pub action: String,
    pub dollar_amount: Decimal,
    pub entry_price: Option<Decimal>,
    /// Non-null for `SHORT` legs (whole-share count for Alpaca SELL).
    pub quantity: Option<Decimal>,
    /// Non-null for `LONG` legs (dollar amount for Alpaca BUY).
    pub notional: Option<Decimal>,
}

/// An order submitted to Alpaca, linked to an allocation.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityOrder {
    pub id: Uuid,
    pub allocation_id: Uuid,
    pub submitted_at: DateTime<Utc>,
    pub ticker: String,
    pub side: String,
    pub quantity: Decimal,
    pub order_type: String,
    pub limit_price: Option<Decimal>,
    pub alpaca_order_id: String,
}

/// Per-rebalance portfolio state snapshot.
///
/// `"intraday"` rows are recorded after each live rebalance; `gross_return` and
/// `net_return` are `None`. `"eod"` rows are recorded once per trading day at
/// market close; all columns are populated.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityPortfolioSnapshot {
    pub id: i64,
    pub snapshot_timestamp: DateTime<Utc>,
    /// Either `"intraday"` or `"eod"`.
    pub snapshot_type: String,
    pub net_asset_value: Decimal,
    pub gross_return: Option<Decimal>,
    pub net_return: Option<Decimal>,
    pub total_slippage_cost: Decimal,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    #[test]
    fn test_equity_rebalance_session_construction() {
        let session = EquityRebalanceSession {
            id: Uuid::new_v4(),
            triggered_at: Utc::now(),
            trigger_reason: "intraday_check".to_string(),
            model_run_id: Some("run-abc123".to_string()),
            completed_at: None,
            status: "completed".to_string(),
        };
        assert_eq!(session.trigger_reason, "intraday_check");
        assert_eq!(session.status, "completed");
        assert!(session.completed_at.is_none());
    }

    #[test]
    fn test_equity_rebalance_session_clone() {
        let session = EquityRebalanceSession {
            id: Uuid::new_v4(),
            triggered_at: Utc::now(),
            trigger_reason: "eod_snapshot_requested".to_string(),
            model_run_id: None,
            completed_at: Some(Utc::now()),
            status: "completed".to_string(),
        };
        let cloned = session.clone();
        assert_eq!(cloned.trigger_reason, "eod_snapshot_requested");
    }

    #[test]
    fn test_equity_pair_construction() {
        let pair = EquityPair {
            id: Uuid::new_v4(),
            rebalance_id: Uuid::new_v4(),
            pair_id: "AAPL-MSFT".to_string(),
            long_ticker: "AAPL".to_string(),
            short_ticker: "MSFT".to_string(),
            z_score: Decimal::from(2),
            hedge_ratio: Decimal::from(1),
            signal_strength: Decimal::new(75, 2),
            status: "open".to_string(),
            opened_at: Utc::now(),
            closed_at: None,
            realized_profit_and_loss: None,
            return_percent: None,
            holding_days: None,
        };
        assert_eq!(pair.long_ticker, "AAPL");
        assert_eq!(pair.short_ticker, "MSFT");
        assert_eq!(pair.status, "open");
    }

    #[test]
    fn test_equity_allocation_construction() {
        let allocation = EquityAllocation {
            id: Uuid::new_v4(),
            rebalance_id: Uuid::new_v4(),
            equity_pair_id: Uuid::new_v4(),
            generated_at: Utc::now(),
            model_run_id: None,
            ticker: "AAPL".to_string(),
            side: "LONG".to_string(),
            action: "OPEN_POSITION".to_string(),
            dollar_amount: Decimal::from(10_000),
            entry_price: Some(Decimal::from(150)),
            quantity: None,
            notional: Some(Decimal::from(10_000)),
        };
        assert_eq!(allocation.ticker, "AAPL");
        assert_eq!(allocation.side, "LONG");
        assert_eq!(allocation.dollar_amount, Decimal::from(10_000));
    }

    #[test]
    fn test_equity_order_construction() {
        let order = EquityOrder {
            id: Uuid::new_v4(),
            allocation_id: Uuid::new_v4(),
            submitted_at: Utc::now(),
            ticker: "MSFT".to_string(),
            side: "SHORT".to_string(),
            quantity: Decimal::from(25),
            order_type: "market".to_string(),
            limit_price: None,
            alpaca_order_id: "alpaca-order-xyz".to_string(),
        };
        assert_eq!(order.ticker, "MSFT");
        assert_eq!(order.side, "SHORT");
        assert_eq!(order.quantity, Decimal::from(25));
    }

    #[test]
    fn test_equity_portfolio_snapshot_construction() {
        let snapshot = EquityPortfolioSnapshot {
            id: 1,
            snapshot_timestamp: Utc::now(),
            snapshot_type: "intraday".to_string(),
            net_asset_value: Decimal::from(100_000),
            gross_return: None,
            net_return: None,
            total_slippage_cost: Decimal::from(50),
            created_at: Utc::now(),
        };
        assert_eq!(snapshot.snapshot_type, "intraday");
        assert_eq!(snapshot.net_asset_value, Decimal::from(100_000));
        assert!(snapshot.gross_return.is_none());
    }

    #[test]
    fn test_equity_portfolio_snapshot_eod() {
        let snapshot = EquityPortfolioSnapshot {
            id: 2,
            snapshot_timestamp: Utc::now(),
            snapshot_type: "eod".to_string(),
            net_asset_value: Decimal::from(102_000),
            gross_return: Some(Decimal::new(2, 2)),
            net_return: Some(Decimal::new(18, 3)),
            total_slippage_cost: Decimal::from(75),
            created_at: Utc::now(),
        };
        assert_eq!(snapshot.snapshot_type, "eod");
        assert!(snapshot.gross_return.is_some());
    }
}
