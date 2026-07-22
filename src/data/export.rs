//! Parquet export tasks for database tables.
//!
//! Each task reads rows from PostgreSQL into typed structs using explicit
//! column lists, serializes to Parquet with deterministic column ordering,
//! and writes to S3. Failures are surfaced as structured log entries.

use crate::common::aws::date_partitioned_key;
use crate::data::{database, state::State};
use crate::domain::market::EquityQuote;
use crate::domain::predictions::{EquityPrediction, ModelRun};
use crate::domain::trading::{
    EquityAllocation, EquityOrder, EquityPair, EquityPortfolioSnapshot, EquityRebalanceSession,
    EquityReconciliationEvent,
};
use aws_sdk_s3::primitives::ByteStream;
use chrono::NaiveDate;
use polars::prelude::*;
use tracing::info;

/// Exports all database tables to S3 Parquet.
///
/// Exports equity_quotes, equity_predictions, equity_rebalance_sessions, equity_pairs,
/// equity_allocations, equity_orders, equity_portfolio_snapshots, model_runs, and
/// equity_reconciliation_events. Rows are not deleted here — the unified purge handler
/// cleans up old data after backup completes.
pub async fn export_database(state: &State, date: NaiveDate) -> Result<usize, String> {
    let pool = state
        .database
        .pool()
        .ok_or_else(|| "database not connected".to_string())?;

    let quotes = database::query_equity_quotes_for_date(pool, date)
        .await
        .map_err(|error| format!("Failed to query equity quotes: {}", error))?;
    let quote_count = quotes.len();
    if quote_count > 0 {
        let mut quote_dataframe = create_equity_quote_dataframe(&quotes)?;
        write_dataframe_to_s3(
            state,
            &mut quote_dataframe,
            &date_partitioned_key("data/equity/quotes", date),
        )
        .await?;
    } else {
        info!(date = %date, "No equity quotes to export");
    }

    let predictions = database::query_equity_predictions_for_date(pool, date)
        .await
        .map_err(|error| format!("Failed to query equity predictions: {}", error))?;
    let prediction_count = predictions.len();
    if prediction_count > 0 {
        let mut prediction_dataframe = create_equity_prediction_dataframe(&predictions)?;
        write_dataframe_to_s3(
            state,
            &mut prediction_dataframe,
            &date_partitioned_key("exports/equity/predictions", date),
        )
        .await?;
    } else {
        info!(date = %date, "No equity predictions to export");
    }

    let sessions = database::query_equity_rebalance_sessions(pool)
        .await
        .map_err(|error| format!("Failed to query equity rebalance sessions: {}", error))?;
    let session_count = sessions.len();
    let mut session_dataframe = create_equity_rebalance_session_dataframe(&sessions)?;
    write_dataframe_to_s3(
        state,
        &mut session_dataframe,
        &date_partitioned_key("exports/equity/rebalance-sessions", date),
    )
    .await?;

    let pairs = database::query_equity_pairs(pool)
        .await
        .map_err(|error| format!("Failed to query equity pairs: {}", error))?;
    let pair_count = pairs.len();
    let mut pair_dataframe = create_equity_pair_dataframe(&pairs)?;
    write_dataframe_to_s3(
        state,
        &mut pair_dataframe,
        &date_partitioned_key("exports/equity/pairs", date),
    )
    .await?;

    let allocations = database::query_equity_allocations(pool)
        .await
        .map_err(|error| format!("Failed to query equity allocations: {}", error))?;
    let allocation_count = allocations.len();
    let mut allocation_dataframe = create_equity_allocation_dataframe(&allocations)?;
    write_dataframe_to_s3(
        state,
        &mut allocation_dataframe,
        &date_partitioned_key("exports/equity/allocations", date),
    )
    .await?;

    let orders = database::query_equity_orders(pool)
        .await
        .map_err(|error| format!("Failed to query equity orders: {}", error))?;
    let order_count = orders.len();
    let mut order_dataframe = create_equity_order_dataframe(&orders)?;
    write_dataframe_to_s3(
        state,
        &mut order_dataframe,
        &date_partitioned_key("exports/equity/orders", date),
    )
    .await?;

    let snapshots = database::query_equity_portfolio_snapshots(pool)
        .await
        .map_err(|error| format!("Failed to query equity portfolio snapshots: {}", error))?;
    let snapshot_count = snapshots.len();
    let mut snapshot_dataframe = create_equity_portfolio_snapshot_dataframe(&snapshots)?;
    write_dataframe_to_s3(
        state,
        &mut snapshot_dataframe,
        &date_partitioned_key("exports/equity/portfolio-snapshots", date),
    )
    .await?;

    let model_runs = database::query_model_runs(pool)
        .await
        .map_err(|error| format!("Failed to query model runs: {}", error))?;
    let model_run_count = model_runs.len();
    let mut model_run_dataframe = create_model_run_dataframe(&model_runs)?;
    write_dataframe_to_s3(
        state,
        &mut model_run_dataframe,
        &date_partitioned_key("exports/model-runs", date),
    )
    .await?;

    let reconciliation_events = database::query_equity_reconciliation_events_for_date(pool, date)
        .await
        .map_err(|error| format!("Failed to query equity reconciliation events: {}", error))?;
    let reconciliation_event_count = reconciliation_events.len();
    if reconciliation_event_count > 0 {
        let mut reconciliation_dataframe =
            create_equity_reconciliation_event_dataframe(&reconciliation_events)?;
        write_dataframe_to_s3(
            state,
            &mut reconciliation_dataframe,
            &date_partitioned_key("exports/equity/reconciliation-events", date),
        )
        .await?;
    } else {
        info!(date = %date, "No equity reconciliation events to export");
    }

    info!(
        "Exported database to S3: {} quotes, {} predictions, {} sessions, {} pairs, {} allocations, {} orders, {} snapshots, {} model runs, {} reconciliation events",
        quote_count, prediction_count, session_count, pair_count, allocation_count, order_count, snapshot_count, model_run_count, reconciliation_event_count
    );

    Ok(quote_count
        + prediction_count
        + session_count
        + pair_count
        + allocation_count
        + order_count
        + snapshot_count
        + model_run_count
        + reconciliation_event_count)
}

async fn write_dataframe_to_s3(
    state: &State,
    dataframe: &mut DataFrame,
    key: &str,
) -> Result<(), String> {
    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(dataframe)
        .map_err(|error| format!("Failed to serialize Parquet for {}: {}", key, error))?;

    state
        .s3_client
        .put_object()
        .bucket(&state.bucket_name)
        .key(key)
        .body(ByteStream::from(buffer))
        .send()
        .await
        .map_err(|error| format!("Failed to upload to S3 {}: {}", key, error))?;

    Ok(())
}

fn create_equity_quote_dataframe(quotes: &[EquityQuote]) -> Result<DataFrame, String> {
    df!(
        "timestamp" => quotes.iter().map(|quote| quote.timestamp().timestamp_millis()).collect::<Vec<i64>>(),
        "ticker" => quotes.iter().map(|quote| quote.ticker().as_str()).collect::<Vec<&str>>(),
        "bid_price" => quotes.iter().map(|quote| quote.bid_price()).collect::<Vec<f64>>(),
        "ask_price" => quotes.iter().map(|quote| quote.ask_price()).collect::<Vec<f64>>(),
        "bid_size" => quotes.iter().map(|quote| quote.bid_size()).collect::<Vec<i32>>(),
        "ask_size" => quotes.iter().map(|quote| quote.ask_size()).collect::<Vec<i32>>(),
    )
    .map_err(|error| format!("Failed to create equity quote DataFrame: {}", error))
}

fn create_equity_rebalance_session_dataframe(
    sessions: &[EquityRebalanceSession],
) -> Result<DataFrame, String> {
    df!(
        "id" => sessions.iter().map(|session| session.id().to_string()).collect::<Vec<String>>(),
        "triggered_at" => sessions.iter().map(|session| session.triggered_at().timestamp_millis()).collect::<Vec<i64>>(),
        "trigger_reason" => sessions.iter().map(|session| session.trigger_reason()).collect::<Vec<&str>>(),
        "model_run_id" => sessions.iter().map(|session| session.model_run_id()).collect::<Vec<Option<&str>>>(),
        "completed_at" => sessions.iter().map(|session| session.completed_at().map(|timestamp| timestamp.timestamp_millis())).collect::<Vec<Option<i64>>>(),
        "status" => sessions.iter().map(|session| session.status().as_str()).collect::<Vec<&str>>(),
    )
    .map_err(|error| {
        format!(
            "Failed to create equity rebalance session DataFrame: {}",
            error
        )
    })
}

fn create_equity_pair_dataframe(pairs: &[EquityPair]) -> Result<DataFrame, String> {
    df!(
        "id" => pairs.iter().map(|pair| pair.id().to_string()).collect::<Vec<String>>(),
        "rebalance_id" => pairs.iter().map(|pair| pair.rebalance_id().to_string()).collect::<Vec<String>>(),
        "pair_id" => pairs.iter().map(|pair| pair.pair_id().as_str()).collect::<Vec<&str>>(),
        "long_ticker" => pairs.iter().map(|pair| pair.long_ticker().as_str()).collect::<Vec<&str>>(),
        "short_ticker" => pairs.iter().map(|pair| pair.short_ticker().as_str()).collect::<Vec<&str>>(),
        "z_score" => pairs.iter().map(|pair| pair.z_score().to_string()).collect::<Vec<String>>(),
        "hedge_ratio" => pairs.iter().map(|pair| pair.hedge_ratio().to_string()).collect::<Vec<String>>(),
        "signal_strength" => pairs.iter().map(|pair| pair.signal_strength().to_string()).collect::<Vec<String>>(),
        "status" => pairs.iter().map(|pair| pair.status().as_str()).collect::<Vec<&str>>(),
        "opened_at" => pairs.iter().map(|pair| pair.opened_at().timestamp_millis()).collect::<Vec<i64>>(),
        "closed_at" => pairs.iter().map(|pair| pair.closed_at().map(|timestamp| timestamp.timestamp_millis())).collect::<Vec<Option<i64>>>(),
        "realized_profit_and_loss" => pairs.iter().map(|pair| pair.realized_profit_and_loss().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "return_percent" => pairs.iter().map(|pair| pair.return_percent().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
    )
    .map_err(|error| format!("Failed to create equity pair DataFrame: {}", error))
}

fn create_equity_allocation_dataframe(
    allocations: &[EquityAllocation],
) -> Result<DataFrame, String> {
    df!(
        "id" => allocations.iter().map(|allocation| allocation.id().to_string()).collect::<Vec<String>>(),
        "rebalance_id" => allocations.iter().map(|allocation| allocation.rebalance_id().to_string()).collect::<Vec<String>>(),
        "equity_pair_id" => allocations.iter().map(|allocation| allocation.equity_pair_id().to_string()).collect::<Vec<String>>(),
        "generated_at" => allocations.iter().map(|allocation| allocation.generated_at().timestamp_millis()).collect::<Vec<i64>>(),
        "model_run_id" => allocations.iter().map(|allocation| allocation.model_run_id()).collect::<Vec<Option<&str>>>(),
        "ticker" => allocations.iter().map(|allocation| allocation.ticker().as_str()).collect::<Vec<&str>>(),
        "side" => allocations.iter().map(|allocation| allocation.side().as_str()).collect::<Vec<&str>>(),
        "action" => allocations.iter().map(|allocation| allocation.action().as_str()).collect::<Vec<&str>>(),
        "dollar_amount" => allocations.iter().map(|allocation| allocation.dollar_amount().to_string()).collect::<Vec<String>>(),
        "entry_price" => allocations.iter().map(|allocation| allocation.entry_price().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "quantity" => allocations.iter().map(|allocation| allocation.quantity().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "notional" => allocations.iter().map(|allocation| allocation.notional().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
    )
    .map_err(|error| format!("Failed to create equity allocation DataFrame: {}", error))
}

fn create_equity_order_dataframe(orders: &[EquityOrder]) -> Result<DataFrame, String> {
    df!(
        "id" => orders.iter().map(|order| order.id().to_string()).collect::<Vec<String>>(),
        "allocation_id" => orders.iter().map(|order| order.allocation_id().to_string()).collect::<Vec<String>>(),
        "submitted_at" => orders.iter().map(|order| order.submitted_at().timestamp_millis()).collect::<Vec<i64>>(),
        "ticker" => orders.iter().map(|order| order.ticker().as_str()).collect::<Vec<&str>>(),
        "side" => orders.iter().map(|order| order.side().as_str()).collect::<Vec<&str>>(),
        "quantity" => orders.iter().map(|order| order.quantity().to_string()).collect::<Vec<String>>(),
        "order_type" => orders.iter().map(|order| order.order_type()).collect::<Vec<&str>>(),
        "limit_price" => orders.iter().map(|order| order.limit_price().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "alpaca_order_id" => orders.iter().map(|order| order.alpaca_order_id()).collect::<Vec<&str>>(),
    )
    .map_err(|error| format!("Failed to create equity order DataFrame: {}", error))
}

fn create_equity_portfolio_snapshot_dataframe(
    snapshots: &[EquityPortfolioSnapshot],
) -> Result<DataFrame, String> {
    df!(
        "id" => snapshots.iter().map(|snapshot| snapshot.id()).collect::<Vec<i64>>(),
        "snapshot_timestamp" => snapshots.iter().map(|snapshot| snapshot.snapshot_timestamp().timestamp_millis()).collect::<Vec<i64>>(),
        "snapshot_type" => snapshots.iter().map(|snapshot| snapshot.snapshot_type().as_str()).collect::<Vec<&str>>(),
        "net_asset_value" => snapshots.iter().map(|snapshot| snapshot.net_asset_value().to_string()).collect::<Vec<String>>(),
        "gross_return" => snapshots.iter().map(|snapshot| snapshot.gross_return().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "net_return" => snapshots.iter().map(|snapshot| snapshot.net_return().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "total_slippage_cost" => snapshots.iter().map(|snapshot| snapshot.total_slippage_cost().to_string()).collect::<Vec<String>>(),
        "created_at" => snapshots.iter().map(|snapshot| snapshot.created_at().timestamp_millis()).collect::<Vec<i64>>(),
    )
    .map_err(|error| {
        format!(
            "Failed to create equity portfolio snapshot DataFrame: {}",
            error
        )
    })
}

fn create_equity_prediction_dataframe(
    predictions: &[EquityPrediction],
) -> Result<DataFrame, String> {
    df!(
        "correlation_id" => predictions.iter().map(|prediction| prediction.correlation_id().to_string()).collect::<Vec<String>>(),
        "model_run_id" => predictions.iter().map(|prediction| prediction.model_run_id()).collect::<Vec<&str>>(),
        "ticker" => predictions.iter().map(|prediction| prediction.ticker().as_str()).collect::<Vec<&str>>(),
        "timestamp" => predictions.iter().map(|prediction| prediction.timestamp().timestamp_millis()).collect::<Vec<i64>>(),
        "quantile_10" => predictions.iter().map(|prediction| prediction.quantile_10()).collect::<Vec<f64>>(),
        "quantile_50" => predictions.iter().map(|prediction| prediction.quantile_50()).collect::<Vec<f64>>(),
        "quantile_90" => predictions.iter().map(|prediction| prediction.quantile_90()).collect::<Vec<f64>>(),
        "created_at" => predictions.iter().map(|prediction| prediction.created_at().timestamp_millis()).collect::<Vec<i64>>(),
    )
    .map_err(|error| format!("Failed to create equity prediction DataFrame: {}", error))
}

fn create_model_run_dataframe(model_runs: &[ModelRun]) -> Result<DataFrame, String> {
    df!(
        "id" => model_runs.iter().map(|model_run| model_run.id()).collect::<Vec<i64>>(),
        "run_id" => model_runs.iter().map(|model_run| model_run.run_id()).collect::<Vec<&str>>(),
        "model_name" => model_runs.iter().map(|model_run| model_run.model_name()).collect::<Vec<&str>>(),
        "artifact_key" => model_runs.iter().map(|model_run| model_run.artifact_key()).collect::<Vec<Option<&str>>>(),
        "training_data_key" => model_runs.iter().map(|model_run| model_run.training_data_key()).collect::<Vec<Option<&str>>>(),
        "start_date" => model_runs.iter().map(|model_run| model_run.start_date().map(|date| date.to_string())).collect::<Vec<Option<String>>>(),
        "end_date" => model_runs.iter().map(|model_run| model_run.end_date().map(|date| date.to_string())).collect::<Vec<Option<String>>>(),
        "lookback_days" => model_runs.iter().map(|model_run| model_run.lookback_days()).collect::<Vec<Option<i32>>>(),
        "status" => model_runs.iter().map(|model_run| model_run.status().as_str()).collect::<Vec<&str>>(),
        "continuous_ranked_probability_score" => model_runs.iter().map(|model_run| model_run.continuous_ranked_probability_score()).collect::<Vec<Option<f64>>>(),
        "directional_accuracy" => model_runs.iter().map(|model_run| model_run.directional_accuracy()).collect::<Vec<Option<f64>>>(),
        "quantile_coverage" => model_runs.iter().map(|model_run| model_run.quantile_coverage()).collect::<Vec<Option<f64>>>(),
        "drift_status" => model_runs.iter().map(|model_run| model_run.drift_status()).collect::<Vec<Option<&str>>>(),
        "stage_counts" => model_runs.iter().map(|model_run| model_run.stage_counts().map(|value| value.to_string())).collect::<Vec<Option<String>>>(),
        "started_at" => model_runs.iter().map(|model_run| model_run.started_at().timestamp_millis()).collect::<Vec<i64>>(),
        "completed_at" => model_runs.iter().map(|model_run| model_run.completed_at().map(|timestamp| timestamp.timestamp_millis())).collect::<Vec<Option<i64>>>(),
    )
    .map_err(|error| format!("Failed to create model run DataFrame: {}", error))
}

fn create_equity_reconciliation_event_dataframe(
    events: &[EquityReconciliationEvent],
) -> Result<DataFrame, String> {
    df!(
        "id" => events.iter().map(|event| event.id()).collect::<Vec<i64>>(),
        "detected_at" => events.iter().map(|event| event.detected_at().timestamp_millis()).collect::<Vec<i64>>(),
        "event_type" => events.iter().map(|event| event.event_type()).collect::<Vec<&str>>(),
        "ticker" => events.iter().map(|event| event.ticker()).collect::<Vec<&str>>(),
        "expected_quantity" => events.iter().map(|event| event.expected_quantity().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "actual_quantity" => events.iter().map(|event| event.actual_quantity().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "equity_pair_id" => events.iter().map(|event| event.equity_pair_id().map(|uuid| uuid.to_string())).collect::<Vec<Option<String>>>(),
        "alpaca_order_id" => events.iter().map(|event| event.alpaca_order_id()).collect::<Vec<Option<&str>>>(),
        "action_taken" => events.iter().map(|event| event.action_taken()).collect::<Vec<&str>>(),
        "resolved_at" => events.iter().map(|event| event.resolved_at().map(|timestamp| timestamp.timestamp_millis())).collect::<Vec<Option<i64>>>(),
    )
    .map_err(|error| format!("Failed to create equity reconciliation event DataFrame: {}", error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::market::{PairID, Ticker};
    use crate::domain::predictions::ModelRunStatus;
    use crate::domain::trading::{
        AllocationAction, AllocationSide, EquityPairStatus, RebalanceSessionStatus, SnapshotType,
    };
    use chrono::Utc;
    use serde_json::json;

    fn sample_quotes() -> Vec<EquityQuote> {
        let now = Utc::now();
        vec![
            EquityQuote::new(now, Ticker::new("AAPL").unwrap(), 150.50, 150.55, 10, 5),
            EquityQuote::new(now, Ticker::new("MSFT").unwrap(), 420.10, 420.20, 2, 4),
        ]
    }

    fn sample_sessions() -> Vec<EquityRebalanceSession> {
        vec![EquityRebalanceSession::new(
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            Utc::now(),
            "market_session_check".to_string(),
            Some("run-abc123".to_string()),
            None,
            RebalanceSessionStatus::Completed,
        )]
    }

    fn sample_pairs() -> Vec<EquityPair> {
        vec![EquityPair::new(
            "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap()),
            Ticker::new("AAPL").unwrap(),
            Ticker::new("MSFT").unwrap(),
            "2".parse().unwrap(),
            "1".parse().unwrap(),
            "0.75".parse().unwrap(),
            EquityPairStatus::Open,
            Utc::now(),
            None,
            None,
            None,
        )]
    }

    fn sample_allocations() -> Vec<EquityAllocation> {
        vec![EquityAllocation::new(
            "550e8400-e29b-41d4-a716-446655440003".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            Utc::now(),
            None,
            Ticker::new("AAPL").unwrap(),
            AllocationSide::Long,
            AllocationAction::OpenPosition,
            "10000".parse().unwrap(),
            Some("150".parse().unwrap()),
            None,
            Some("10000".parse().unwrap()),
        )]
    }

    fn sample_orders() -> Vec<EquityOrder> {
        vec![EquityOrder::new(
            "550e8400-e29b-41d4-a716-446655440004".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440003".parse().unwrap(),
            Utc::now(),
            Ticker::new("MSFT").unwrap(),
            AllocationSide::Short,
            "25".parse().unwrap(),
            "market".to_string(),
            None,
            "alpaca-order-xyz".to_string(),
        )]
    }

    fn sample_snapshots() -> Vec<EquityPortfolioSnapshot> {
        vec![EquityPortfolioSnapshot::new(
            1,
            Utc::now(),
            SnapshotType::EndOfDay,
            "100000".parse().unwrap(),
            Some("0.02".parse().unwrap()),
            Some("0.018".parse().unwrap()),
            "50".parse().unwrap(),
            Utc::now(),
        )]
    }

    #[test]
    fn test_create_equity_quote_dataframe_columns_and_rows() {
        let quotes = sample_quotes();
        let dataframe = create_equity_quote_dataframe(&quotes).unwrap();
        assert_eq!(dataframe.height(), 2);
        assert_eq!(dataframe.width(), 6);
        assert!(dataframe.column("timestamp").is_ok());
        assert!(dataframe.column("ticker").is_ok());
        assert!(dataframe.column("bid_price").is_ok());
        assert!(dataframe.column("ask_price").is_ok());
        assert!(dataframe.column("bid_size").is_ok());
        assert!(dataframe.column("ask_size").is_ok());
    }

    #[test]
    fn test_create_equity_quote_dataframe_empty() {
        let dataframe = create_equity_quote_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 6);
    }

    #[test]
    fn test_create_equity_rebalance_session_dataframe_columns_and_rows() {
        let sessions = sample_sessions();
        let dataframe = create_equity_rebalance_session_dataframe(&sessions).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.width(), 6);
        assert!(dataframe.column("id").is_ok());
        assert!(dataframe.column("trigger_reason").is_ok());
        assert!(dataframe.column("status").is_ok());
    }

    #[test]
    fn test_create_equity_rebalance_session_dataframe_empty() {
        let dataframe = create_equity_rebalance_session_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 6);
    }

    #[test]
    fn test_create_equity_pair_dataframe_columns_and_rows() {
        let pairs = sample_pairs();
        let dataframe = create_equity_pair_dataframe(&pairs).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.width(), 13);
        assert!(dataframe.column("id").is_ok());
        assert!(dataframe.column("z_score").is_ok());
        assert!(dataframe.column("return_percent").is_ok());
    }

    #[test]
    fn test_create_equity_pair_dataframe_empty() {
        let dataframe = create_equity_pair_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 13);
    }

    #[test]
    fn test_create_equity_allocation_dataframe_columns_and_rows() {
        let allocations = sample_allocations();
        let dataframe = create_equity_allocation_dataframe(&allocations).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.width(), 12);
        assert!(dataframe.column("ticker").is_ok());
        assert!(dataframe.column("dollar_amount").is_ok());
        assert!(dataframe.column("notional").is_ok());
    }

    #[test]
    fn test_create_equity_allocation_dataframe_empty() {
        let dataframe = create_equity_allocation_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 12);
    }

    #[test]
    fn test_create_equity_order_dataframe_columns_and_rows() {
        let orders = sample_orders();
        let dataframe = create_equity_order_dataframe(&orders).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.width(), 9);
        assert!(dataframe.column("ticker").is_ok());
        assert!(dataframe.column("quantity").is_ok());
        assert!(dataframe.column("alpaca_order_id").is_ok());
    }

    #[test]
    fn test_create_equity_order_dataframe_empty() {
        let dataframe = create_equity_order_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 9);
    }

    #[test]
    fn test_create_equity_portfolio_snapshot_dataframe_columns_and_rows() {
        let snapshots = sample_snapshots();
        let dataframe = create_equity_portfolio_snapshot_dataframe(&snapshots).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.width(), 8);
        assert!(dataframe.column("id").is_ok());
        assert!(dataframe.column("net_asset_value").is_ok());
        assert!(dataframe.column("snapshot_type").is_ok());
    }

    #[test]
    fn test_create_equity_portfolio_snapshot_dataframe_empty() {
        let dataframe = create_equity_portfolio_snapshot_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 8);
    }

    #[test]
    fn test_create_equity_pair_dataframe_serializes_decimal_as_string() {
        let pairs = sample_pairs();
        let dataframe = create_equity_pair_dataframe(&pairs).unwrap();
        let z_score_series = dataframe.column("z_score").unwrap();
        assert_eq!(z_score_series.dtype(), &DataType::String);
    }

    #[test]
    fn test_create_equity_allocation_dataframe_serializes_decimal_as_string() {
        let allocations = sample_allocations();
        let dataframe = create_equity_allocation_dataframe(&allocations).unwrap();
        let amount_series = dataframe.column("dollar_amount").unwrap();
        assert_eq!(amount_series.dtype(), &DataType::String);
    }

    fn sample_predictions() -> Vec<EquityPrediction> {
        let now = Utc::now();
        vec![
            EquityPrediction::new(
                "550e8400-e29b-41d4-a716-446655440010".parse().unwrap(),
                "run-tide-001".to_string(),
                Ticker::new("AAPL").unwrap(),
                now,
                -0.02,
                0.01,
                0.04,
                now,
            ),
            EquityPrediction::new(
                "550e8400-e29b-41d4-a716-446655440011".parse().unwrap(),
                "run-tide-001".to_string(),
                Ticker::new("MSFT").unwrap(),
                now,
                -0.01,
                0.005,
                0.02,
                now,
            ),
        ]
    }

    #[test]
    fn test_create_equity_prediction_dataframe_columns_and_rows() {
        let predictions = sample_predictions();
        let dataframe = create_equity_prediction_dataframe(&predictions).unwrap();
        assert_eq!(dataframe.height(), 2);
        assert_eq!(dataframe.width(), 8);
        assert!(dataframe.column("correlation_id").is_ok());
        assert!(dataframe.column("model_run_id").is_ok());
        assert!(dataframe.column("ticker").is_ok());
        assert!(dataframe.column("timestamp").is_ok());
        assert!(dataframe.column("quantile_10").is_ok());
        assert!(dataframe.column("quantile_50").is_ok());
        assert!(dataframe.column("quantile_90").is_ok());
        assert!(dataframe.column("created_at").is_ok());
    }

    #[test]
    fn test_create_equity_prediction_dataframe_empty() {
        let dataframe = create_equity_prediction_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 8);
    }

    fn sample_model_runs() -> Vec<ModelRun> {
        vec![
            ModelRun::new(
                1,
                "run-tide-001".to_string(),
                "tide".to_string(),
                Some("artifacts/tide/run-001/weights.safetensors".to_string()),
                Some("data/equity/bars/training.parquet".to_string()),
                Some(chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()),
                Some(chrono::NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()),
                Some(70),
                ModelRunStatus::Completed,
                Some(0.42),
                Some(0.55),
                Some(0.88),
                Some("stable".to_string()),
                Some(json!({"stage_1": 100})),
                Utc::now(),
                Some(Utc::now()),
            ),
            ModelRun::new(
                2,
                "run-tide-002".to_string(),
                "tide".to_string(),
                None,
                None,
                None,
                None,
                None,
                ModelRunStatus::Started,
                None,
                None,
                None,
                None,
                None,
                Utc::now(),
                None,
            ),
        ]
    }

    #[test]
    fn test_create_model_run_dataframe_columns_and_rows() {
        let model_runs = sample_model_runs();
        let dataframe = create_model_run_dataframe(&model_runs).unwrap();
        assert_eq!(dataframe.height(), 2);
        assert_eq!(dataframe.width(), 16);
        assert!(dataframe.column("id").is_ok());
        assert!(dataframe.column("run_id").is_ok());
        assert!(dataframe.column("model_name").is_ok());
        assert!(dataframe.column("artifact_key").is_ok());
        assert!(dataframe.column("training_data_key").is_ok());
        assert!(dataframe.column("start_date").is_ok());
        assert!(dataframe.column("end_date").is_ok());
        assert!(dataframe.column("lookback_days").is_ok());
        assert!(dataframe.column("status").is_ok());
        assert!(dataframe
            .column("continuous_ranked_probability_score")
            .is_ok());
        assert!(dataframe.column("directional_accuracy").is_ok());
        assert!(dataframe.column("quantile_coverage").is_ok());
        assert!(dataframe.column("drift_status").is_ok());
        assert!(dataframe.column("stage_counts").is_ok());
        assert!(dataframe.column("started_at").is_ok());
        assert!(dataframe.column("completed_at").is_ok());
    }

    #[test]
    fn test_create_model_run_dataframe_empty() {
        let dataframe = create_model_run_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 16);
    }

    #[test]
    fn test_create_model_run_dataframe_serializes_stage_counts_as_string() {
        let model_runs = sample_model_runs();
        let dataframe = create_model_run_dataframe(&model_runs).unwrap();
        let stage_counts_series = dataframe.column("stage_counts").unwrap();
        assert_eq!(stage_counts_series.dtype(), &DataType::String);
    }

    #[test]
    fn test_create_equity_quote_dataframe_values_are_correct() {
        let now = chrono::Utc::now();
        let quotes = vec![crate::domain::market::EquityQuote::new(
            now,
            Ticker::new("GOOG").unwrap(),
            180.10,
            180.20,
            3,
            7,
        )];
        let dataframe = create_equity_quote_dataframe(&quotes).unwrap();
        assert_eq!(dataframe.height(), 1);
        // Confirm bid and ask price columns carry Float64 values
        assert_eq!(
            dataframe.column("bid_price").unwrap().dtype(),
            &DataType::Float64
        );
        assert_eq!(
            dataframe.column("ask_price").unwrap().dtype(),
            &DataType::Float64
        );
        // bid_size and ask_size are Int32
        assert_eq!(
            dataframe.column("bid_size").unwrap().dtype(),
            &DataType::Int32
        );
        assert_eq!(
            dataframe.column("ask_size").unwrap().dtype(),
            &DataType::Int32
        );
    }

    #[test]
    fn test_create_equity_rebalance_session_dataframe_null_optional_fields() {
        // completed_at and model_run_id are Optional — test the None path
        let sessions = vec![crate::domain::trading::EquityRebalanceSession::new(
            "550e8400-e29b-41d4-a716-446655440099".parse().unwrap(),
            chrono::Utc::now(),
            "market_session_check".to_string(),
            None,
            None,
            crate::domain::trading::RebalanceSessionStatus::Completed,
        )];
        let dataframe = create_equity_rebalance_session_dataframe(&sessions).unwrap();
        assert_eq!(dataframe.height(), 1);
        // model_run_id column must exist and contain a null
        let model_run_id_series = dataframe.column("model_run_id").unwrap();
        assert!(model_run_id_series.null_count() == 1);
        // completed_at column must exist and contain a null
        let completed_at_series = dataframe.column("completed_at").unwrap();
        assert!(completed_at_series.null_count() == 1);
    }

    #[test]
    fn test_create_equity_pair_dataframe_null_optional_fields() {
        // closed_at, realized_profit_and_loss, return_percent are Optional
        let pairs = vec![crate::domain::trading::EquityPair::new(
            "550e8400-e29b-41d4-a716-446655440020".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            PairID::new(Ticker::new("GOOG").unwrap(), Ticker::new("META").unwrap()),
            Ticker::new("GOOG").unwrap(),
            Ticker::new("META").unwrap(),
            "1.5".parse().unwrap(),
            "0.9".parse().unwrap(),
            "0.8".parse().unwrap(),
            crate::domain::trading::EquityPairStatus::Closed,
            chrono::Utc::now(),
            None,
            None,
            None,
        )];
        let dataframe = create_equity_pair_dataframe(&pairs).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.column("closed_at").unwrap().null_count(), 1);
        assert_eq!(
            dataframe
                .column("realized_profit_and_loss")
                .unwrap()
                .null_count(),
            1
        );
        assert_eq!(dataframe.column("return_percent").unwrap().null_count(), 1);
    }

    #[test]
    fn test_create_equity_allocation_dataframe_null_optional_fields() {
        // entry_price, quantity, notional are Optional; model_run_id is Optional
        let allocations = vec![crate::domain::trading::EquityAllocation::new(
            "550e8400-e29b-41d4-a716-446655440030".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            chrono::Utc::now(),
            None,
            Ticker::new("NVDA").unwrap(),
            crate::domain::trading::AllocationSide::Short,
            crate::domain::trading::AllocationAction::ClosePosition,
            "5000".parse().unwrap(),
            None,
            None,
            None,
        )];
        let dataframe = create_equity_allocation_dataframe(&allocations).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.column("model_run_id").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("entry_price").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("quantity").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("notional").unwrap().null_count(), 1);
    }

    #[test]
    fn test_create_equity_order_dataframe_null_limit_price() {
        // limit_price is Optional — test the None (market order) path
        let orders = vec![crate::domain::trading::EquityOrder::new(
            "550e8400-e29b-41d4-a716-446655440040".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440003".parse().unwrap(),
            chrono::Utc::now(),
            Ticker::new("TSLA").unwrap(),
            crate::domain::trading::AllocationSide::Long,
            "10".parse().unwrap(),
            "market".to_string(),
            None,
            "alpaca-order-market-001".to_string(),
        )];
        let dataframe = create_equity_order_dataframe(&orders).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.column("limit_price").unwrap().null_count(), 1);
        // order_type column is a String
        assert_eq!(
            dataframe.column("order_type").unwrap().dtype(),
            &DataType::String
        );
    }

    #[test]
    fn test_create_equity_portfolio_snapshot_dataframe_null_optional_fields() {
        // gross_return and net_return are Optional
        let snapshots = vec![crate::domain::trading::EquityPortfolioSnapshot::new(
            2,
            chrono::Utc::now(),
            crate::domain::trading::SnapshotType::Intraday,
            "50000".parse().unwrap(),
            None,
            None,
            "0".parse().unwrap(),
            chrono::Utc::now(),
        )];
        let dataframe = create_equity_portfolio_snapshot_dataframe(&snapshots).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.column("gross_return").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("net_return").unwrap().null_count(), 1);
    }

    #[test]
    fn test_create_model_run_dataframe_null_optional_fields() {
        // artifact_key, training_data_key, start_date, end_date, lookback_days,
        // crps, directional_accuracy, quantile_coverage, drift_status, stage_counts,
        // completed_at are all Optional — test the all-None path
        let model_runs = vec![crate::domain::predictions::ModelRun::new(
            3,
            "run-tide-003".to_string(),
            "tide".to_string(),
            None,
            None,
            None,
            None,
            None,
            crate::domain::predictions::ModelRunStatus::Failed,
            None,
            None,
            None,
            None,
            None,
            chrono::Utc::now(),
            None,
        )];
        let dataframe = create_model_run_dataframe(&model_runs).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.column("artifact_key").unwrap().null_count(), 1);
        assert_eq!(
            dataframe.column("training_data_key").unwrap().null_count(),
            1
        );
        assert_eq!(dataframe.column("start_date").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("end_date").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("lookback_days").unwrap().null_count(), 1);
        assert_eq!(
            dataframe
                .column("continuous_ranked_probability_score")
                .unwrap()
                .null_count(),
            1
        );
        assert_eq!(
            dataframe
                .column("directional_accuracy")
                .unwrap()
                .null_count(),
            1
        );
        assert_eq!(
            dataframe.column("quantile_coverage").unwrap().null_count(),
            1
        );
        assert_eq!(dataframe.column("drift_status").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("stage_counts").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("completed_at").unwrap().null_count(), 1);
    }

    #[test]
    fn test_create_equity_prediction_dataframe_timestamp_is_int64() {
        let predictions = sample_predictions();
        let dataframe = create_equity_prediction_dataframe(&predictions).unwrap();
        assert_eq!(
            dataframe.column("timestamp").unwrap().dtype(),
            &DataType::Int64
        );
        assert_eq!(
            dataframe.column("created_at").unwrap().dtype(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_create_equity_rebalance_session_dataframe_timestamp_is_int64() {
        let sessions = sample_sessions();
        let dataframe = create_equity_rebalance_session_dataframe(&sessions).unwrap();
        assert_eq!(
            dataframe.column("triggered_at").unwrap().dtype(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_create_equity_order_dataframe_quantity_is_string() {
        let orders = sample_orders();
        let dataframe = create_equity_order_dataframe(&orders).unwrap();
        // quantity is a Decimal serialized to String
        assert_eq!(
            dataframe.column("quantity").unwrap().dtype(),
            &DataType::String
        );
    }

    #[test]
    fn test_create_equity_portfolio_snapshot_dataframe_id_is_int64() {
        let snapshots = sample_snapshots();
        let dataframe = create_equity_portfolio_snapshot_dataframe(&snapshots).unwrap();
        assert_eq!(dataframe.column("id").unwrap().dtype(), &DataType::Int64);
    }

    #[test]
    fn test_export_database_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data::state::{DatabaseState, MassiveSecrets, State};
            use aws_credential_types::Credentials;
            use aws_sdk_s3::config::Region;
            use chrono::NaiveDate;

            let credentials =
                Credentials::new("test-access-key", "test-secret-key", None, None, "tests");
            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(credentials)
                .endpoint_url("http://127.0.0.1:9")
                .load()
                .await;
            let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
                .force_path_style(true)
                .build();
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
            let state = State::new(
                reqwest::Client::new(),
                MassiveSecrets {
                    base: "http://127.0.0.1:1".to_string(),
                    key: "test-api-key".to_string(),
                },
                s3_client,
                "test-bucket".to_string(),
            );
            assert!(matches!(state.database, DatabaseState::NotConfigured));

            let date = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
            let result = export_database(&state, date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("database not connected"));
        });
    }

    #[test]
    fn test_create_equity_quote_dataframe_timestamp_is_int64() {
        let quotes = sample_quotes();
        let dataframe = create_equity_quote_dataframe(&quotes).unwrap();
        assert_eq!(
            dataframe.column("timestamp").unwrap().dtype(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_create_equity_quote_dataframe_ticker_is_string() {
        let quotes = sample_quotes();
        let dataframe = create_equity_quote_dataframe(&quotes).unwrap();
        assert_eq!(
            dataframe.column("ticker").unwrap().dtype(),
            &DataType::String
        );
    }

    #[test]
    fn test_create_equity_rebalance_session_dataframe_status_values() {
        let sessions = sample_sessions();
        let dataframe = create_equity_rebalance_session_dataframe(&sessions).unwrap();
        let status_series = dataframe.column("status").unwrap();
        let status_values: Vec<&str> = status_series.str().unwrap().into_no_null_iter().collect();
        assert_eq!(status_values, vec!["completed"]);
    }

    #[test]
    fn test_create_equity_pair_dataframe_status_values() {
        let pairs = sample_pairs();
        let dataframe = create_equity_pair_dataframe(&pairs).unwrap();
        let status_series = dataframe.column("status").unwrap();
        let status_values: Vec<&str> = status_series.str().unwrap().into_no_null_iter().collect();
        assert_eq!(status_values, vec!["open"]);
    }

    #[test]
    fn test_create_equity_allocation_dataframe_side_and_action_columns() {
        let allocations = sample_allocations();
        let dataframe = create_equity_allocation_dataframe(&allocations).unwrap();
        let side_values: Vec<&str> = dataframe
            .column("side")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let action_values: Vec<&str> = dataframe
            .column("action")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(side_values, vec!["LONG"]);
        assert_eq!(action_values, vec!["OPEN_POSITION"]);
    }

    #[test]
    fn test_create_equity_order_dataframe_side_values() {
        let orders = sample_orders();
        let dataframe = create_equity_order_dataframe(&orders).unwrap();
        let side_values: Vec<&str> = dataframe
            .column("side")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(side_values, vec!["SHORT"]);
    }

    #[test]
    fn test_create_equity_portfolio_snapshot_dataframe_net_asset_value_is_string() {
        let snapshots = sample_snapshots();
        let dataframe = create_equity_portfolio_snapshot_dataframe(&snapshots).unwrap();
        assert_eq!(
            dataframe.column("net_asset_value").unwrap().dtype(),
            &DataType::String
        );
    }

    #[test]
    fn test_create_equity_portfolio_snapshot_dataframe_snapshot_type_values() {
        let snapshots = sample_snapshots();
        let dataframe = create_equity_portfolio_snapshot_dataframe(&snapshots).unwrap();
        let type_values: Vec<&str> = dataframe
            .column("snapshot_type")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(type_values, vec!["end_of_day"]);
    }

    #[test]
    fn test_create_model_run_dataframe_status_values() {
        let model_runs = sample_model_runs();
        let dataframe = create_model_run_dataframe(&model_runs).unwrap();
        let status_values: Vec<&str> = dataframe
            .column("status")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(status_values, vec!["completed", "started"]);
    }

    #[test]
    fn test_create_equity_prediction_dataframe_quantile_values() {
        let predictions = sample_predictions();
        let dataframe = create_equity_prediction_dataframe(&predictions).unwrap();
        let q10: Vec<f64> = dataframe
            .column("quantile_10")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!((q10[0] - (-0.02)).abs() < f64::EPSILON);
        assert!((q10[1] - (-0.01)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_create_equity_rebalance_session_dataframe_with_failed_status() {
        let sessions = vec![EquityRebalanceSession::new(
            "550e8400-e29b-41d4-a716-446655440088".parse().unwrap(),
            Utc::now(),
            "manual".to_string(),
            None,
            None,
            RebalanceSessionStatus::Failed,
        )];
        let dataframe = create_equity_rebalance_session_dataframe(&sessions).unwrap();
        let status_values: Vec<&str> = dataframe
            .column("status")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(status_values, vec!["failed"]);
    }

    #[test]
    fn test_create_equity_pair_dataframe_closed_status() {
        let pairs = vec![EquityPair::new(
            "550e8400-e29b-41d4-a716-446655440099".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            PairID::new(Ticker::new("TSLA").unwrap(), Ticker::new("NVDA").unwrap()),
            Ticker::new("TSLA").unwrap(),
            Ticker::new("NVDA").unwrap(),
            "3.5".parse().unwrap(),
            "1.2".parse().unwrap(),
            "0.5".parse().unwrap(),
            EquityPairStatus::Closed,
            Utc::now(),
            Some(Utc::now()),
            Some("500".parse().unwrap()),
            Some("0.05".parse().unwrap()),
        )];
        let dataframe = create_equity_pair_dataframe(&pairs).unwrap();
        assert_eq!(dataframe.height(), 1);
        let status_values: Vec<&str> = dataframe
            .column("status")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(status_values, vec!["closed"]);
        // closed_at, realized_profit_and_loss, return_percent all non-null
        assert_eq!(dataframe.column("closed_at").unwrap().null_count(), 0);
        assert_eq!(
            dataframe
                .column("realized_profit_and_loss")
                .unwrap()
                .null_count(),
            0
        );
    }

    #[test]
    fn test_create_model_run_dataframe_id_is_int64() {
        let model_runs = sample_model_runs();
        let dataframe = create_model_run_dataframe(&model_runs).unwrap();
        assert_eq!(dataframe.column("id").unwrap().dtype(), &DataType::Int64);
    }

    #[test]
    fn test_create_equity_prediction_dataframe_model_run_id_is_string() {
        let predictions = sample_predictions();
        let dataframe = create_equity_prediction_dataframe(&predictions).unwrap();
        assert_eq!(
            dataframe.column("model_run_id").unwrap().dtype(),
            &DataType::String
        );
    }

    fn sample_reconciliation_events() -> Vec<EquityReconciliationEvent> {
        vec![EquityReconciliationEvent::new(
            1,
            Utc::now(),
            "quantity_mismatch".to_string(),
            "AAPL".to_string(),
            Some("100".parse().unwrap()),
            Some("95".parse().unwrap()),
            Some("550e8400-e29b-41d4-a716-446655440001".parse().unwrap()),
            Some("alpaca-order-123".to_string()),
            "logged_only".to_string(),
            None,
        )]
    }

    #[test]
    fn test_create_equity_reconciliation_event_dataframe_columns_and_rows() {
        let events = sample_reconciliation_events();
        let dataframe = create_equity_reconciliation_event_dataframe(&events).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.width(), 10);
        assert!(dataframe.column("id").is_ok());
        assert!(dataframe.column("detected_at").is_ok());
        assert!(dataframe.column("event_type").is_ok());
        assert!(dataframe.column("ticker").is_ok());
        assert!(dataframe.column("expected_quantity").is_ok());
        assert!(dataframe.column("actual_quantity").is_ok());
        assert!(dataframe.column("equity_pair_id").is_ok());
        assert!(dataframe.column("alpaca_order_id").is_ok());
        assert!(dataframe.column("action_taken").is_ok());
        assert!(dataframe.column("resolved_at").is_ok());
    }

    #[test]
    fn test_create_equity_reconciliation_event_dataframe_empty() {
        let dataframe = create_equity_reconciliation_event_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 10);
    }
}
