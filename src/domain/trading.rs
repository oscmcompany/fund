//! Trading lifecycle record types mirroring the PostgreSQL trading tables.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::market::{PairID, Ticker};

/// Status of an equity rebalance session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum RebalanceSessionStatus {
    Completed,
    Failed,
}

impl RebalanceSessionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    /// Parses a stored database value. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Open/closed status of an equity pair position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum EquityPairStatus {
    Open,
    Closed,
}

impl EquityPairStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }

    /// Parses a stored database value. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }
}

/// Long or short side of an allocation or order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AllocationSide {
    Long,
    Short,
}

impl AllocationSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
        }
    }

    /// Parses a stored database value. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "LONG" => Some(Self::Long),
            "SHORT" => Some(Self::Short),
            _ => None,
        }
    }
}

/// Intended action for an allocation leg.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AllocationAction {
    OpenPosition,
    ClosePosition,
    Unspecified,
}

impl AllocationAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OpenPosition => "OPEN_POSITION",
            Self::ClosePosition => "CLOSE_POSITION",
            Self::Unspecified => "UNSPECIFIED",
        }
    }

    /// Parses a stored database value. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "OPEN_POSITION" => Some(Self::OpenPosition),
            "CLOSE_POSITION" => Some(Self::ClosePosition),
            "UNSPECIFIED" => Some(Self::Unspecified),
            _ => None,
        }
    }
}

/// Intraday or end-of-day portfolio snapshot type.
///
/// The `EndOfDay` variant maps to the database value `"end_of_day"` which
/// matches the `snapshot_type` CHECK constraint in `equity_portfolio_snapshots`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum SnapshotType {
    Intraday,
    EndOfDay,
}

impl SnapshotType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Intraday => "intraday",
            Self::EndOfDay => "end_of_day",
        }
    }

    /// Parses a stored database value. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "intraday" => Some(Self::Intraday),
            "end_of_day" => Some(Self::EndOfDay),
            _ => None,
        }
    }
}

/// Reason a long-short pair position was closed.
///
/// Mirrors the `CHECK` constraint on `equity_pairs.close_reason`:
/// `('profit_taken', 'stop_loss', 'end_of_day')`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    /// The pair spread converged back through zero; the trade thesis played out.
    ProfitTaken,
    /// The pair spread diverged further against the position past the stop-loss threshold.
    StopLoss,
    /// All positions closed before market close (go to cash).
    EndOfDay,
}

impl CloseReason {
    /// Returns the database string identifier for this close reason.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ProfitTaken => "profit_taken",
            Self::StopLoss => "stop_loss",
            Self::EndOfDay => "end_of_day",
        }
    }

    /// Parses a stored database value. Returns `None` for unknown values.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "profit_taken" => Some(Self::ProfitTaken),
            "stop_loss" => Some(Self::StopLoss),
            "end_of_day" => Some(Self::EndOfDay),
            _ => None,
        }
    }
}

impl std::fmt::Display for CloseReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Groups one full rebalance cycle from allocation through order submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityRebalanceSession {
    id: Uuid,
    triggered_at: DateTime<Utc>,
    trigger_reason: String,
    /// References `model_runs.run_id`; nullable when the model run is unavailable.
    model_run_id: Option<String>,
    completed_at: Option<DateTime<Utc>>,
    status: RebalanceSessionStatus,
}

impl EquityRebalanceSession {
    /// Constructs an `EquityRebalanceSession` from validated field values.
    pub fn new(
        id: Uuid,
        triggered_at: DateTime<Utc>,
        trigger_reason: String,
        model_run_id: Option<String>,
        completed_at: Option<DateTime<Utc>>,
        status: RebalanceSessionStatus,
    ) -> Self {
        Self {
            id,
            triggered_at,
            trigger_reason,
            model_run_id,
            completed_at,
            status,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn triggered_at(&self) -> DateTime<Utc> {
        self.triggered_at
    }

    pub fn trigger_reason(&self) -> &str {
        &self.trigger_reason
    }

    pub fn model_run_id(&self) -> Option<&str> {
        self.model_run_id.as_deref()
    }

    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    pub fn status(&self) -> &RebalanceSessionStatus {
        &self.status
    }
}

/// One cointegrated long-short pair within a rebalance session.
///
/// Entry signal fields (`z_score`, `hedge_ratio`, `signal_strength`) are recorded
/// at the time the pair is opened.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPair {
    id: Uuid,
    rebalance_id: Uuid,
    pair_id: PairID,
    long_ticker: Ticker,
    short_ticker: Ticker,
    z_score: Decimal,
    hedge_ratio: Decimal,
    signal_strength: Decimal,
    status: EquityPairStatus,
    opened_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
    realized_profit_and_loss: Option<Decimal>,
    return_percent: Option<Decimal>,
}

impl EquityPair {
    /// Constructs an `EquityPair` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        rebalance_id: Uuid,
        pair_id: PairID,
        long_ticker: Ticker,
        short_ticker: Ticker,
        z_score: Decimal,
        hedge_ratio: Decimal,
        signal_strength: Decimal,
        status: EquityPairStatus,
        opened_at: DateTime<Utc>,
        closed_at: Option<DateTime<Utc>>,
        realized_profit_and_loss: Option<Decimal>,
        return_percent: Option<Decimal>,
    ) -> Self {
        Self {
            id,
            rebalance_id,
            pair_id,
            long_ticker,
            short_ticker,
            z_score,
            hedge_ratio,
            signal_strength,
            status,
            opened_at,
            closed_at,
            realized_profit_and_loss,
            return_percent,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn rebalance_id(&self) -> Uuid {
        self.rebalance_id
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

    pub fn z_score(&self) -> &Decimal {
        &self.z_score
    }

    pub fn hedge_ratio(&self) -> &Decimal {
        &self.hedge_ratio
    }

    pub fn signal_strength(&self) -> &Decimal {
        &self.signal_strength
    }

    pub fn status(&self) -> &EquityPairStatus {
        &self.status
    }

    pub fn opened_at(&self) -> DateTime<Utc> {
        self.opened_at
    }

    pub fn closed_at(&self) -> Option<DateTime<Utc>> {
        self.closed_at
    }

    pub fn realized_profit_and_loss(&self) -> Option<&Decimal> {
        self.realized_profit_and_loss.as_ref()
    }

    pub fn return_percent(&self) -> Option<&Decimal> {
        self.return_percent.as_ref()
    }
}

/// One ticker leg of an allocation within a rebalance session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityAllocation {
    id: Uuid,
    rebalance_id: Uuid,
    equity_pair_id: Uuid,
    generated_at: DateTime<Utc>,
    /// References `model_runs.run_id`; nullable when unavailable.
    model_run_id: Option<String>,
    ticker: Ticker,
    side: AllocationSide,
    action: AllocationAction,
    dollar_amount: Decimal,
    entry_price: Option<Decimal>,
    /// Non-null for `SHORT` legs (whole-share count for Alpaca SELL).
    quantity: Option<Decimal>,
    /// Non-null for `LONG` legs (dollar amount for Alpaca BUY).
    notional: Option<Decimal>,
}

impl EquityAllocation {
    /// Constructs an `EquityAllocation` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        rebalance_id: Uuid,
        equity_pair_id: Uuid,
        generated_at: DateTime<Utc>,
        model_run_id: Option<String>,
        ticker: Ticker,
        side: AllocationSide,
        action: AllocationAction,
        dollar_amount: Decimal,
        entry_price: Option<Decimal>,
        quantity: Option<Decimal>,
        notional: Option<Decimal>,
    ) -> Self {
        Self {
            id,
            rebalance_id,
            equity_pair_id,
            generated_at,
            model_run_id,
            ticker,
            side,
            action,
            dollar_amount,
            entry_price,
            quantity,
            notional,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn rebalance_id(&self) -> Uuid {
        self.rebalance_id
    }

    pub fn equity_pair_id(&self) -> Uuid {
        self.equity_pair_id
    }

    pub fn generated_at(&self) -> DateTime<Utc> {
        self.generated_at
    }

    pub fn model_run_id(&self) -> Option<&str> {
        self.model_run_id.as_deref()
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn side(&self) -> &AllocationSide {
        &self.side
    }

    pub fn action(&self) -> &AllocationAction {
        &self.action
    }

    pub fn dollar_amount(&self) -> &Decimal {
        &self.dollar_amount
    }

    pub fn entry_price(&self) -> Option<&Decimal> {
        self.entry_price.as_ref()
    }

    pub fn quantity(&self) -> Option<&Decimal> {
        self.quantity.as_ref()
    }

    pub fn notional(&self) -> Option<&Decimal> {
        self.notional.as_ref()
    }
}

/// An order submitted to Alpaca, linked to an allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityOrder {
    id: Uuid,
    allocation_id: Uuid,
    submitted_at: DateTime<Utc>,
    ticker: Ticker,
    side: AllocationSide,
    quantity: Decimal,
    order_type: String,
    limit_price: Option<Decimal>,
    alpaca_order_id: String,
}

impl EquityOrder {
    /// Constructs an `EquityOrder` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        allocation_id: Uuid,
        submitted_at: DateTime<Utc>,
        ticker: Ticker,
        side: AllocationSide,
        quantity: Decimal,
        order_type: String,
        limit_price: Option<Decimal>,
        alpaca_order_id: String,
    ) -> Self {
        Self {
            id,
            allocation_id,
            submitted_at,
            ticker,
            side,
            quantity,
            order_type,
            limit_price,
            alpaca_order_id,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn allocation_id(&self) -> Uuid {
        self.allocation_id
    }

    pub fn submitted_at(&self) -> DateTime<Utc> {
        self.submitted_at
    }

    pub fn ticker(&self) -> &Ticker {
        &self.ticker
    }

    pub fn side(&self) -> &AllocationSide {
        &self.side
    }

    pub fn quantity(&self) -> &Decimal {
        &self.quantity
    }

    pub fn order_type(&self) -> &str {
        &self.order_type
    }

    pub fn limit_price(&self) -> Option<&Decimal> {
        self.limit_price.as_ref()
    }

    pub fn alpaca_order_id(&self) -> &str {
        &self.alpaca_order_id
    }
}

/// Per-rebalance portfolio state snapshot.
///
/// `Intraday` rows are recorded after each live rebalance; `gross_return` and
/// `net_return` are `None`. `EndOfDay` rows are recorded once per trading day
/// at market close; all columns are populated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPortfolioSnapshot {
    id: i64,
    snapshot_timestamp: DateTime<Utc>,
    snapshot_type: SnapshotType,
    net_asset_value: Decimal,
    gross_return: Option<Decimal>,
    net_return: Option<Decimal>,
    total_slippage_cost: Decimal,
    created_at: DateTime<Utc>,
}

impl EquityPortfolioSnapshot {
    /// Constructs an `EquityPortfolioSnapshot` from validated field values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i64,
        snapshot_timestamp: DateTime<Utc>,
        snapshot_type: SnapshotType,
        net_asset_value: Decimal,
        gross_return: Option<Decimal>,
        net_return: Option<Decimal>,
        total_slippage_cost: Decimal,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            snapshot_timestamp,
            snapshot_type,
            net_asset_value,
            gross_return,
            net_return,
            total_slippage_cost,
            created_at,
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn snapshot_timestamp(&self) -> DateTime<Utc> {
        self.snapshot_timestamp
    }

    pub fn snapshot_type(&self) -> &SnapshotType {
        &self.snapshot_type
    }

    pub fn net_asset_value(&self) -> &Decimal {
        &self.net_asset_value
    }

    pub fn gross_return(&self) -> Option<&Decimal> {
        self.gross_return.as_ref()
    }

    pub fn net_return(&self) -> Option<&Decimal> {
        self.net_return.as_ref()
    }

    pub fn total_slippage_cost(&self) -> &Decimal {
        &self.total_slippage_cost
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

/// A non-empty collection of [`EquityRebalanceSession`] records.
#[derive(Debug, Clone)]
pub struct RebalanceSessions(Vec<EquityRebalanceSession>);

impl RebalanceSessions {
    /// Returns `None` if `sessions` is empty.
    pub fn new(sessions: Vec<EquityRebalanceSession>) -> Option<Self> {
        if sessions.is_empty() {
            None
        } else {
            Some(Self(sessions))
        }
    }

    pub fn as_slice(&self) -> &[EquityRebalanceSession] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A non-empty collection of [`EquityPair`] records.
#[derive(Debug, Clone)]
pub struct EquityPairs(Vec<EquityPair>);

impl EquityPairs {
    /// Returns `None` if `pairs` is empty.
    pub fn new(pairs: Vec<EquityPair>) -> Option<Self> {
        if pairs.is_empty() {
            None
        } else {
            Some(Self(pairs))
        }
    }

    pub fn as_slice(&self) -> &[EquityPair] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A non-empty collection of [`EquityAllocation`] records.
#[derive(Debug, Clone)]
pub struct EquityAllocations(Vec<EquityAllocation>);

impl EquityAllocations {
    /// Returns `None` if `allocations` is empty.
    pub fn new(allocations: Vec<EquityAllocation>) -> Option<Self> {
        if allocations.is_empty() {
            None
        } else {
            Some(Self(allocations))
        }
    }

    pub fn as_slice(&self) -> &[EquityAllocation] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A non-empty collection of [`EquityOrder`] records.
#[derive(Debug, Clone)]
pub struct EquityOrders(Vec<EquityOrder>);

impl EquityOrders {
    /// Returns `None` if `orders` is empty.
    pub fn new(orders: Vec<EquityOrder>) -> Option<Self> {
        if orders.is_empty() {
            None
        } else {
            Some(Self(orders))
        }
    }

    pub fn as_slice(&self) -> &[EquityOrder] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A non-empty collection of [`EquityPortfolioSnapshot`] records.
#[derive(Debug, Clone)]
pub struct PortfolioSnapshots(Vec<EquityPortfolioSnapshot>);

impl PortfolioSnapshots {
    /// Returns `None` if `snapshots` is empty.
    pub fn new(snapshots: Vec<EquityPortfolioSnapshot>) -> Option<Self> {
        if snapshots.is_empty() {
            None
        } else {
            Some(Self(snapshots))
        }
    }

    pub fn as_slice(&self) -> &[EquityPortfolioSnapshot] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn make_pair_id() -> PairID {
        PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap())
    }

    #[test]
    fn test_close_reason_round_trip() {
        for reason in [
            CloseReason::ProfitTaken,
            CloseReason::StopLoss,
            CloseReason::EndOfDay,
        ] {
            assert_eq!(CloseReason::parse(reason.as_str()), Some(reason.clone()));
            let serialized = serde_json::to_string(&reason).unwrap();
            assert_eq!(serialized, format!("\"{}\"", reason.as_str()));
            let deserialized: CloseReason = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, reason);
        }
    }

    #[test]
    fn test_close_reason_parse_rejects_unknown() {
        assert_eq!(CloseReason::parse("expired"), None);
        assert_eq!(CloseReason::parse("PROFIT_TAKEN"), None);
        assert_eq!(CloseReason::parse("rebalance"), None);
    }

    #[test]
    fn test_close_reason_display() {
        assert_eq!(CloseReason::ProfitTaken.to_string(), "profit_taken");
        assert_eq!(CloseReason::StopLoss.to_string(), "stop_loss");
        assert_eq!(CloseReason::EndOfDay.to_string(), "end_of_day");
    }

    #[test]
    fn test_close_reason_matches_schema_check_constraint() {
        // Values must exactly match the CHECK constraint in schema.sql.
        assert_eq!(CloseReason::ProfitTaken.as_str(), "profit_taken");
        assert_eq!(CloseReason::StopLoss.as_str(), "stop_loss");
        assert_eq!(CloseReason::EndOfDay.as_str(), "end_of_day");
    }

    #[test]
    fn test_rebalance_session_status_round_trip() {
        for status in [
            RebalanceSessionStatus::Completed,
            RebalanceSessionStatus::Failed,
        ] {
            assert_eq!(
                RebalanceSessionStatus::parse(status.as_str()),
                Some(status.clone())
            );
            let serialized = serde_json::to_string(&status).unwrap();
            assert_eq!(serialized, format!("\"{}\"", status.as_str()));
            let deserialized: RebalanceSessionStatus = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    #[test]
    fn test_rebalance_session_status_parse_rejects_unknown() {
        assert_eq!(RebalanceSessionStatus::parse("pending"), None);
    }

    #[test]
    fn test_equity_pair_status_round_trip() {
        for status in [EquityPairStatus::Open, EquityPairStatus::Closed] {
            assert_eq!(
                EquityPairStatus::parse(status.as_str()),
                Some(status.clone())
            );
            let serialized = serde_json::to_string(&status).unwrap();
            assert_eq!(serialized, format!("\"{}\"", status.as_str()));
            let deserialized: EquityPairStatus = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, status);
        }
    }

    #[test]
    fn test_equity_pair_status_parse_rejects_unknown() {
        assert_eq!(EquityPairStatus::parse("OPEN"), None);
    }

    #[test]
    fn test_allocation_side_round_trip() {
        for side in [AllocationSide::Long, AllocationSide::Short] {
            assert_eq!(AllocationSide::parse(side.as_str()), Some(side.clone()));
            let serialized = serde_json::to_string(&side).unwrap();
            assert_eq!(serialized, format!("\"{}\"", side.as_str()));
            let deserialized: AllocationSide = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, side);
        }
    }

    #[test]
    fn test_allocation_side_parse_rejects_unknown() {
        assert_eq!(AllocationSide::parse("long"), None);
    }

    #[test]
    fn test_allocation_action_round_trip() {
        for action in [
            AllocationAction::OpenPosition,
            AllocationAction::ClosePosition,
            AllocationAction::Unspecified,
        ] {
            assert_eq!(
                AllocationAction::parse(action.as_str()),
                Some(action.clone())
            );
            let serialized = serde_json::to_string(&action).unwrap();
            assert_eq!(serialized, format!("\"{}\"", action.as_str()));
            let deserialized: AllocationAction = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, action);
        }
    }

    #[test]
    fn test_allocation_action_parse_rejects_unknown() {
        assert_eq!(AllocationAction::parse("open_position"), None);
    }

    #[test]
    fn test_snapshot_type_round_trip() {
        for snapshot_type in [SnapshotType::Intraday, SnapshotType::EndOfDay] {
            assert_eq!(
                SnapshotType::parse(snapshot_type.as_str()),
                Some(snapshot_type.clone())
            );
            let serialized = serde_json::to_string(&snapshot_type).unwrap();
            assert_eq!(serialized, format!("\"{}\"", snapshot_type.as_str()));
            let deserialized: SnapshotType = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, snapshot_type);
        }
    }

    #[test]
    fn test_snapshot_type_end_of_day_matches_check_constraint() {
        // The schema CHECK constraint allows 'intraday' and 'end_of_day'; the
        // historical "eod" shorthand was never a legal stored value.
        assert_eq!(SnapshotType::EndOfDay.as_str(), "end_of_day");
        assert_eq!(SnapshotType::parse("eod"), None);
    }

    #[test]
    fn test_equity_rebalance_session_construction() {
        let session = EquityRebalanceSession::new(
            Uuid::new_v4(),
            Utc::now(),
            "market_session_check".to_string(),
            Some("run-abc123".to_string()),
            None,
            RebalanceSessionStatus::Completed,
        );
        assert_eq!(session.trigger_reason(), "market_session_check");
        assert_eq!(session.status(), &RebalanceSessionStatus::Completed);
        assert!(session.completed_at().is_none());
    }

    #[test]
    fn test_equity_rebalance_session_clone() {
        let session = EquityRebalanceSession::new(
            Uuid::new_v4(),
            Utc::now(),
            "eod_snapshot_requested".to_string(),
            None,
            Some(Utc::now()),
            RebalanceSessionStatus::Completed,
        );
        let cloned = session.clone();
        assert_eq!(cloned.trigger_reason(), "eod_snapshot_requested");
    }

    #[test]
    fn test_equity_pair_construction() {
        let pair = EquityPair::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            make_pair_id(),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            Decimal::from(2),
            Decimal::from(1),
            Decimal::new(75, 2),
            EquityPairStatus::Open,
            Utc::now(),
            None,
            None,
            None,
        );
        assert_eq!(pair.long_ticker().as_str(), "AAPL");
        assert_eq!(pair.short_ticker().as_str(), "MSFT");
        assert_eq!(pair.status(), &EquityPairStatus::Open);
    }

    #[test]
    fn test_equity_allocation_construction() {
        let allocation = EquityAllocation::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Utc::now(),
            None,
            Ticker::new("AAPL").unwrap(),
            AllocationSide::Long,
            AllocationAction::OpenPosition,
            Decimal::from(10_000),
            Some(Decimal::from(150)),
            None,
            Some(Decimal::from(10_000)),
        );
        assert_eq!(allocation.ticker().as_str(), "AAPL");
        assert_eq!(allocation.side(), &AllocationSide::Long);
        assert_eq!(allocation.dollar_amount(), &Decimal::from(10_000));
    }

    #[test]
    fn test_equity_order_construction() {
        let order = EquityOrder::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Utc::now(),
            Ticker::new("MSFT").unwrap(),
            AllocationSide::Short,
            Decimal::from(25),
            "market".to_string(),
            None,
            "alpaca-order-xyz".to_string(),
        );
        assert_eq!(order.ticker().as_str(), "MSFT");
        assert_eq!(order.side(), &AllocationSide::Short);
        assert_eq!(order.quantity(), &Decimal::from(25));
    }

    #[test]
    fn test_equity_portfolio_snapshot_construction() {
        let snapshot = EquityPortfolioSnapshot::new(
            1,
            Utc::now(),
            SnapshotType::Intraday,
            Decimal::from(100_000),
            None,
            None,
            Decimal::from(50),
            Utc::now(),
        );
        assert_eq!(snapshot.snapshot_type(), &SnapshotType::Intraday);
        assert_eq!(snapshot.net_asset_value(), &Decimal::from(100_000));
        assert!(snapshot.gross_return().is_none());
    }

    #[test]
    fn test_equity_portfolio_snapshot_end_of_day() {
        let snapshot = EquityPortfolioSnapshot::new(
            2,
            Utc::now(),
            SnapshotType::EndOfDay,
            Decimal::from(102_000),
            Some(Decimal::new(2, 2)),
            Some(Decimal::new(18, 3)),
            Decimal::from(75),
            Utc::now(),
        );
        assert_eq!(snapshot.snapshot_type(), &SnapshotType::EndOfDay);
        assert!(snapshot.gross_return().is_some());
    }

    fn sample_session() -> EquityRebalanceSession {
        EquityRebalanceSession::new(
            Uuid::new_v4(),
            Utc::now(),
            "market_session_check".to_string(),
            None,
            None,
            RebalanceSessionStatus::Completed,
        )
    }

    fn sample_pair() -> EquityPair {
        EquityPair::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            make_pair_id(),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            Decimal::from(2),
            Decimal::from(1),
            Decimal::new(75, 2),
            EquityPairStatus::Open,
            Utc::now(),
            None,
            None,
            None,
        )
    }

    fn sample_allocation() -> EquityAllocation {
        EquityAllocation::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Utc::now(),
            None,
            Ticker::new("AAPL").unwrap(),
            AllocationSide::Long,
            AllocationAction::OpenPosition,
            Decimal::from(10_000),
            None,
            None,
            Some(Decimal::from(10_000)),
        )
    }

    fn sample_order() -> EquityOrder {
        EquityOrder::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Utc::now(),
            Ticker::new("MSFT").unwrap(),
            AllocationSide::Short,
            Decimal::from(25),
            "market".to_string(),
            None,
            "alpaca-order-xyz".to_string(),
        )
    }

    fn sample_snapshot() -> EquityPortfolioSnapshot {
        EquityPortfolioSnapshot::new(
            1,
            Utc::now(),
            SnapshotType::EndOfDay,
            Decimal::from(100_000),
            None,
            None,
            Decimal::from(50),
            Utc::now(),
        )
    }

    #[test]
    fn test_rebalance_sessions_non_empty_constructor() {
        assert!(RebalanceSessions::new(vec![]).is_none());
        let sessions = RebalanceSessions::new(vec![sample_session()]).unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(!sessions.is_empty());
        assert_eq!(
            sessions.as_slice()[0].trigger_reason(),
            "market_session_check"
        );
    }

    #[test]
    fn test_equity_pairs_non_empty_constructor() {
        assert!(EquityPairs::new(vec![]).is_none());
        let pairs = EquityPairs::new(vec![sample_pair()]).unwrap();
        assert_eq!(pairs.len(), 1);
        assert!(!pairs.is_empty());
        assert_eq!(pairs.as_slice()[0].pair_id().as_str(), "AAPL-MSFT");
    }

    #[test]
    fn test_equity_allocations_non_empty_constructor() {
        assert!(EquityAllocations::new(vec![]).is_none());
        let allocations = EquityAllocations::new(vec![sample_allocation()]).unwrap();
        assert_eq!(allocations.len(), 1);
        assert!(!allocations.is_empty());
        assert_eq!(allocations.as_slice()[0].ticker().as_str(), "AAPL");
    }

    #[test]
    fn test_equity_orders_non_empty_constructor() {
        assert!(EquityOrders::new(vec![]).is_none());
        let orders = EquityOrders::new(vec![sample_order()]).unwrap();
        assert_eq!(orders.len(), 1);
        assert!(!orders.is_empty());
        assert_eq!(orders.as_slice()[0].alpaca_order_id(), "alpaca-order-xyz");
    }

    #[test]
    fn test_portfolio_snapshots_non_empty_constructor() {
        assert!(PortfolioSnapshots::new(vec![]).is_none());
        let snapshots = PortfolioSnapshots::new(vec![sample_snapshot()]).unwrap();
        assert_eq!(snapshots.len(), 1);
        assert!(!snapshots.is_empty());
        assert_eq!(snapshots.as_slice()[0].id(), 1);
    }

    #[test]
    fn test_equity_rebalance_session_all_accessors() {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let session = EquityRebalanceSession::new(
            id,
            now,
            "market_session_check".to_string(),
            Some("run-xyz".to_string()),
            Some(now),
            RebalanceSessionStatus::Failed,
        );
        assert_eq!(session.id(), id);
        assert_eq!(session.triggered_at(), now);
        assert_eq!(session.model_run_id(), Some("run-xyz"));
        assert!(session.completed_at().is_some());
        assert_eq!(session.status(), &RebalanceSessionStatus::Failed);
    }

    #[test]
    fn test_equity_pair_all_accessors() {
        let id = Uuid::new_v4();
        let rebalance_id = Uuid::new_v4();
        let now = Utc::now();
        let pair = EquityPair::new(
            id,
            rebalance_id,
            make_pair_id(),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            Decimal::from(2),
            Decimal::new(85, 2),
            Decimal::new(75, 2),
            EquityPairStatus::Closed,
            now,
            Some(now),
            Some(Decimal::from(500)),
            Some(Decimal::new(5, 2)),
        );
        assert_eq!(pair.id(), id);
        assert_eq!(pair.rebalance_id(), rebalance_id);
        assert_eq!(pair.z_score(), &Decimal::from(2));
        assert_eq!(pair.hedge_ratio(), &Decimal::new(85, 2));
        assert_eq!(pair.signal_strength(), &Decimal::new(75, 2));
        assert!(pair.opened_at() <= Utc::now());
        assert!(pair.closed_at().is_some());
        assert!(pair.realized_profit_and_loss().is_some());
        assert!(pair.return_percent().is_some());
    }

    #[test]
    fn test_equity_allocation_all_accessors() {
        let id = Uuid::new_v4();
        let rebalance_id = Uuid::new_v4();
        let pair_id = Uuid::new_v4();
        let now = Utc::now();
        let allocation = EquityAllocation::new(
            id,
            rebalance_id,
            pair_id,
            now,
            Some("run-001".to_string()),
            Ticker::new("AAPL").unwrap(),
            AllocationSide::Short,
            AllocationAction::ClosePosition,
            Decimal::from(5_000),
            Some(Decimal::from(150)),
            Some(Decimal::from(33)),
            None,
        );
        assert_eq!(allocation.id(), id);
        assert_eq!(allocation.rebalance_id(), rebalance_id);
        assert_eq!(allocation.equity_pair_id(), pair_id);
        assert!(allocation.generated_at() <= Utc::now());
        assert_eq!(allocation.model_run_id(), Some("run-001"));
        assert_eq!(allocation.action(), &AllocationAction::ClosePosition);
        assert!(allocation.entry_price().is_some());
        assert!(allocation.quantity().is_some());
        assert!(allocation.notional().is_none());
    }

    #[test]
    fn test_equity_order_all_accessors() {
        let id = Uuid::new_v4();
        let allocation_id = Uuid::new_v4();
        let now = Utc::now();
        let order = EquityOrder::new(
            id,
            allocation_id,
            now,
            Ticker::new("AAPL").unwrap(),
            AllocationSide::Long,
            Decimal::from(10),
            "limit".to_string(),
            Some(Decimal::from(150)),
            "alpaca-001".to_string(),
        );
        assert_eq!(order.id(), id);
        assert_eq!(order.allocation_id(), allocation_id);
        assert!(order.submitted_at() <= Utc::now());
        assert_eq!(order.order_type(), "limit");
        assert!(order.limit_price().is_some());
        assert_eq!(order.alpaca_order_id(), "alpaca-001");
    }

    #[test]
    fn test_equity_portfolio_snapshot_all_accessors() {
        let now = Utc::now();
        let snapshot = EquityPortfolioSnapshot::new(
            42,
            now,
            SnapshotType::EndOfDay,
            Decimal::from(105_000),
            Some(Decimal::new(5, 2)),
            Some(Decimal::new(45, 3)),
            Decimal::from(100),
            now,
        );
        assert!(snapshot.snapshot_timestamp() <= Utc::now());
        assert!(snapshot.net_return().is_some());
        assert_eq!(snapshot.total_slippage_cost(), &Decimal::from(100));
        assert!(snapshot.created_at() <= Utc::now());
    }
}
