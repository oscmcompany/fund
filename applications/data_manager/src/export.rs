//! Parquet export tasks for equity market data and trading history.
//!
//! Each task reads rows from PostgreSQL into typed structs using explicit
//! column lists, serializes to Parquet with deterministic column ordering,
//! and writes to S3. Failures are surfaced as structured log entries.

use crate::{database, state::State};
use aws_sdk_s3::primitives::ByteStream;
use chrono::{Datelike, NaiveDate};
use internal::market::{EquityBar, EquityQuote};
use internal::predictions::{EquityPrediction, ModelRun};
use internal::trading::{
    EquityAllocation, EquityOrder, EquityPair, EquityPortfolioSnapshot, EquityRebalanceSession,
};
use polars::prelude::*;
use tracing::info;

/// Exports equity bars for the given date to S3 Parquet.
pub async fn export_equity_bars(state: &State, date: NaiveDate) -> Result<usize, String> {
    let pool = state
        .database
        .pool()
        .ok_or_else(|| "database not connected".to_string())?;

    let bars = database::query_equity_bars_for_date(pool, date)
        .await
        .map_err(|error| format!("Failed to query equity bars: {}", error))?;

    let count = bars.len();

    if count == 0 {
        info!("No equity bars to export for {}", date);
        return Ok(0);
    }

    let mut dataframe = create_equity_bar_export_dataframe(&bars)?;
    let key = format!(
        "data/equity/bars/year={}/month={:02}/day={:02}/data.parquet",
        date.year(),
        date.month(),
        date.day()
    );
    write_dataframe_to_s3(state, &mut dataframe, &key).await?;
    info!("Exported {} equity bars to S3: {}", count, key);

    Ok(count)
}

/// Exports all trading history tables to S3 Parquet.
///
/// Exports equity_quotes, equity_predictions, equity_rebalance_sessions, equity_pairs,
/// equity_allocations, equity_orders, equity_portfolio_snapshots, and model_runs. Deletes the
/// exported equity_quotes rows from the database after a successful S3 write.
pub async fn export_trading_history(state: &State, date: NaiveDate) -> Result<usize, String> {
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
            &format!(
                "data/equity/quotes/year={}/month={:02}/day={:02}/data.parquet",
                date.year(),
                date.month(),
                date.day()
            ),
        )
        .await?;
        database::delete_equity_quotes_for_date(pool, date)
            .await
            .map_err(|error| format!("Failed to delete equity quotes: {}", error))?;
    } else {
        info!("No equity quotes to export for {}", date);
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
            &format!(
                "exports/equity/predictions/year={}/month={:02}/day={:02}/data.parquet",
                date.year(),
                date.month(),
                date.day()
            ),
        )
        .await?;
    } else {
        info!("No equity predictions to export for {}", date);
    }

    let sessions = database::query_equity_rebalance_sessions(pool)
        .await
        .map_err(|error| format!("Failed to query equity rebalance sessions: {}", error))?;
    let session_count = sessions.len();
    let mut session_dataframe = create_equity_rebalance_session_dataframe(&sessions)?;
    write_dataframe_to_s3(
        state,
        &mut session_dataframe,
        &format!(
            "exports/equity/rebalance-sessions/year={}/month={:02}/day={:02}/data.parquet",
            date.year(),
            date.month(),
            date.day()
        ),
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
        &format!(
            "exports/equity/pairs/year={}/month={:02}/day={:02}/data.parquet",
            date.year(),
            date.month(),
            date.day()
        ),
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
        &format!(
            "exports/equity/allocations/year={}/month={:02}/day={:02}/data.parquet",
            date.year(),
            date.month(),
            date.day()
        ),
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
        &format!(
            "exports/equity/orders/year={}/month={:02}/day={:02}/data.parquet",
            date.year(),
            date.month(),
            date.day()
        ),
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
        &format!(
            "exports/equity/portfolio-snapshots/year={}/month={:02}/day={:02}/data.parquet",
            date.year(),
            date.month(),
            date.day()
        ),
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
        &format!(
            "exports/model-runs/year={}/month={:02}/day={:02}/data.parquet",
            date.year(),
            date.month(),
            date.day()
        ),
    )
    .await?;

    info!(
        "Exported trading history to S3: {} quotes, {} predictions, {} sessions, {} pairs, {} allocations, {} orders, {} snapshots, {} model runs",
        quote_count, prediction_count, session_count, pair_count, allocation_count, order_count, snapshot_count, model_run_count
    );

    Ok(quote_count
        + prediction_count
        + session_count
        + pair_count
        + allocation_count
        + order_count
        + snapshot_count
        + model_run_count)
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
        "timestamp" => quotes.iter().map(|quote| quote.timestamp.timestamp_millis()).collect::<Vec<i64>>(),
        "ticker" => quotes.iter().map(|quote| quote.ticker.as_str()).collect::<Vec<&str>>(),
        "bid_price" => quotes.iter().map(|quote| quote.bid_price).collect::<Vec<f64>>(),
        "ask_price" => quotes.iter().map(|quote| quote.ask_price).collect::<Vec<f64>>(),
        "bid_size" => quotes.iter().map(|quote| quote.bid_size).collect::<Vec<i32>>(),
        "ask_size" => quotes.iter().map(|quote| quote.ask_size).collect::<Vec<i32>>(),
    )
    .map_err(|error| format!("Failed to create equity quote DataFrame: {}", error))
}

fn create_equity_bar_export_dataframe(bars: &[EquityBar]) -> Result<DataFrame, String> {
    df!(
        "ticker" => bars.iter().map(|bar| bar.ticker.as_str()).collect::<Vec<&str>>(),
        "timestamp" => bars.iter().map(|bar| bar.timestamp.timestamp_millis()).collect::<Vec<i64>>(),
        "open_price" => bars.iter().map(|bar| bar.open_price).collect::<Vec<f64>>(),
        "high_price" => bars.iter().map(|bar| bar.high_price).collect::<Vec<f64>>(),
        "low_price" => bars.iter().map(|bar| bar.low_price).collect::<Vec<f64>>(),
        "close_price" => bars.iter().map(|bar| bar.close_price).collect::<Vec<f64>>(),
        "volume" => bars.iter().map(|bar| bar.volume).collect::<Vec<i64>>(),
        "volume_weighted_average_price" => bars.iter().map(|bar| bar.volume_weighted_average_price).collect::<Vec<Option<f64>>>(),
        "transactions" => bars.iter().map(|bar| bar.transactions).collect::<Vec<Option<i64>>>(),
        "inserted_at" => bars.iter().map(|bar| bar.inserted_at.timestamp_millis()).collect::<Vec<i64>>(),
    )
    .map_err(|error| format!("Failed to create equity bar export DataFrame: {}", error))
}

fn create_equity_rebalance_session_dataframe(
    sessions: &[EquityRebalanceSession],
) -> Result<DataFrame, String> {
    df!(
        "id" => sessions.iter().map(|session| session.id.to_string()).collect::<Vec<String>>(),
        "triggered_at" => sessions.iter().map(|session| session.triggered_at.timestamp_millis()).collect::<Vec<i64>>(),
        "trigger_reason" => sessions.iter().map(|session| session.trigger_reason.as_str()).collect::<Vec<&str>>(),
        "model_run_id" => sessions.iter().map(|session| session.model_run_id.as_deref()).collect::<Vec<Option<&str>>>(),
        "completed_at" => sessions.iter().map(|session| session.completed_at.map(|timestamp| timestamp.timestamp_millis())).collect::<Vec<Option<i64>>>(),
        "status" => sessions.iter().map(|session| session.status.as_str()).collect::<Vec<&str>>(),
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
        "id" => pairs.iter().map(|pair| pair.id.to_string()).collect::<Vec<String>>(),
        "rebalance_id" => pairs.iter().map(|pair| pair.rebalance_id.to_string()).collect::<Vec<String>>(),
        "pair_id" => pairs.iter().map(|pair| pair.pair_id.as_str()).collect::<Vec<&str>>(),
        "long_ticker" => pairs.iter().map(|pair| pair.long_ticker.as_str()).collect::<Vec<&str>>(),
        "short_ticker" => pairs.iter().map(|pair| pair.short_ticker.as_str()).collect::<Vec<&str>>(),
        "z_score" => pairs.iter().map(|pair| pair.z_score.to_string()).collect::<Vec<String>>(),
        "hedge_ratio" => pairs.iter().map(|pair| pair.hedge_ratio.to_string()).collect::<Vec<String>>(),
        "signal_strength" => pairs.iter().map(|pair| pair.signal_strength.to_string()).collect::<Vec<String>>(),
        "status" => pairs.iter().map(|pair| pair.status.as_str()).collect::<Vec<&str>>(),
        "opened_at" => pairs.iter().map(|pair| pair.opened_at.timestamp_millis()).collect::<Vec<i64>>(),
        "closed_at" => pairs.iter().map(|pair| pair.closed_at.map(|timestamp| timestamp.timestamp_millis())).collect::<Vec<Option<i64>>>(),
        "realized_profit_and_loss" => pairs.iter().map(|pair| pair.realized_profit_and_loss.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "return_percent" => pairs.iter().map(|pair| pair.return_percent.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "holding_days" => pairs.iter().map(|pair| pair.holding_days).collect::<Vec<Option<i32>>>(),
    )
    .map_err(|error| format!("Failed to create equity pair DataFrame: {}", error))
}

fn create_equity_allocation_dataframe(
    allocations: &[EquityAllocation],
) -> Result<DataFrame, String> {
    df!(
        "id" => allocations.iter().map(|allocation| allocation.id.to_string()).collect::<Vec<String>>(),
        "rebalance_id" => allocations.iter().map(|allocation| allocation.rebalance_id.to_string()).collect::<Vec<String>>(),
        "equity_pair_id" => allocations.iter().map(|allocation| allocation.equity_pair_id.to_string()).collect::<Vec<String>>(),
        "generated_at" => allocations.iter().map(|allocation| allocation.generated_at.timestamp_millis()).collect::<Vec<i64>>(),
        "model_run_id" => allocations.iter().map(|allocation| allocation.model_run_id.as_deref()).collect::<Vec<Option<&str>>>(),
        "ticker" => allocations.iter().map(|allocation| allocation.ticker.as_str()).collect::<Vec<&str>>(),
        "side" => allocations.iter().map(|allocation| allocation.side.as_str()).collect::<Vec<&str>>(),
        "action" => allocations.iter().map(|allocation| allocation.action.as_str()).collect::<Vec<&str>>(),
        "dollar_amount" => allocations.iter().map(|allocation| allocation.dollar_amount.to_string()).collect::<Vec<String>>(),
        "entry_price" => allocations.iter().map(|allocation| allocation.entry_price.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "quantity" => allocations.iter().map(|allocation| allocation.quantity.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "notional" => allocations.iter().map(|allocation| allocation.notional.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
    )
    .map_err(|error| format!("Failed to create equity allocation DataFrame: {}", error))
}

fn create_equity_order_dataframe(orders: &[EquityOrder]) -> Result<DataFrame, String> {
    df!(
        "id" => orders.iter().map(|order| order.id.to_string()).collect::<Vec<String>>(),
        "allocation_id" => orders.iter().map(|order| order.allocation_id.to_string()).collect::<Vec<String>>(),
        "submitted_at" => orders.iter().map(|order| order.submitted_at.timestamp_millis()).collect::<Vec<i64>>(),
        "ticker" => orders.iter().map(|order| order.ticker.as_str()).collect::<Vec<&str>>(),
        "side" => orders.iter().map(|order| order.side.as_str()).collect::<Vec<&str>>(),
        "quantity" => orders.iter().map(|order| order.quantity.to_string()).collect::<Vec<String>>(),
        "order_type" => orders.iter().map(|order| order.order_type.as_str()).collect::<Vec<&str>>(),
        "limit_price" => orders.iter().map(|order| order.limit_price.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "alpaca_order_id" => orders.iter().map(|order| order.alpaca_order_id.as_str()).collect::<Vec<&str>>(),
    )
    .map_err(|error| format!("Failed to create equity order DataFrame: {}", error))
}

fn create_equity_portfolio_snapshot_dataframe(
    snapshots: &[EquityPortfolioSnapshot],
) -> Result<DataFrame, String> {
    df!(
        "id" => snapshots.iter().map(|snapshot| snapshot.id).collect::<Vec<i64>>(),
        "snapshot_timestamp" => snapshots.iter().map(|snapshot| snapshot.snapshot_timestamp.timestamp_millis()).collect::<Vec<i64>>(),
        "snapshot_type" => snapshots.iter().map(|snapshot| snapshot.snapshot_type.as_str()).collect::<Vec<&str>>(),
        "net_asset_value" => snapshots.iter().map(|snapshot| snapshot.net_asset_value.to_string()).collect::<Vec<String>>(),
        "gross_return" => snapshots.iter().map(|snapshot| snapshot.gross_return.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "net_return" => snapshots.iter().map(|snapshot| snapshot.net_return.as_ref().map(|decimal| decimal.to_string())).collect::<Vec<Option<String>>>(),
        "total_slippage_cost" => snapshots.iter().map(|snapshot| snapshot.total_slippage_cost.to_string()).collect::<Vec<String>>(),
        "created_at" => snapshots.iter().map(|snapshot| snapshot.created_at.timestamp_millis()).collect::<Vec<i64>>(),
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
        "correlation_id" => predictions.iter().map(|prediction| prediction.correlation_id.to_string()).collect::<Vec<String>>(),
        "model_run_id" => predictions.iter().map(|prediction| prediction.model_run_id.as_str()).collect::<Vec<&str>>(),
        "ticker" => predictions.iter().map(|prediction| prediction.ticker.as_str()).collect::<Vec<&str>>(),
        "timestamp" => predictions.iter().map(|prediction| prediction.timestamp.timestamp_millis()).collect::<Vec<i64>>(),
        "quantile_10" => predictions.iter().map(|prediction| prediction.quantile_10).collect::<Vec<f64>>(),
        "quantile_50" => predictions.iter().map(|prediction| prediction.quantile_50).collect::<Vec<f64>>(),
        "quantile_90" => predictions.iter().map(|prediction| prediction.quantile_90).collect::<Vec<f64>>(),
        "created_at" => predictions.iter().map(|prediction| prediction.created_at.timestamp_millis()).collect::<Vec<i64>>(),
    )
    .map_err(|error| format!("Failed to create equity prediction DataFrame: {}", error))
}

fn create_model_run_dataframe(model_runs: &[ModelRun]) -> Result<DataFrame, String> {
    df!(
        "id" => model_runs.iter().map(|model_run| model_run.id).collect::<Vec<i64>>(),
        "run_id" => model_runs.iter().map(|model_run| model_run.run_id.as_str()).collect::<Vec<&str>>(),
        "model_name" => model_runs.iter().map(|model_run| model_run.model_name.as_str()).collect::<Vec<&str>>(),
        "artifact_key" => model_runs.iter().map(|model_run| model_run.artifact_key.as_deref()).collect::<Vec<Option<&str>>>(),
        "training_data_key" => model_runs.iter().map(|model_run| model_run.training_data_key.as_deref()).collect::<Vec<Option<&str>>>(),
        "start_date" => model_runs.iter().map(|model_run| model_run.start_date.map(|date| date.to_string())).collect::<Vec<Option<String>>>(),
        "end_date" => model_runs.iter().map(|model_run| model_run.end_date.map(|date| date.to_string())).collect::<Vec<Option<String>>>(),
        "lookback_days" => model_runs.iter().map(|model_run| model_run.lookback_days).collect::<Vec<Option<i32>>>(),
        "status" => model_runs.iter().map(|model_run| model_run.status.as_str()).collect::<Vec<&str>>(),
        "continuous_ranked_probability_score" => model_runs.iter().map(|model_run| model_run.continuous_ranked_probability_score).collect::<Vec<Option<f64>>>(),
        "directional_accuracy" => model_runs.iter().map(|model_run| model_run.directional_accuracy).collect::<Vec<Option<f64>>>(),
        "quantile_coverage" => model_runs.iter().map(|model_run| model_run.quantile_coverage).collect::<Vec<Option<f64>>>(),
        "drift_status" => model_runs.iter().map(|model_run| model_run.drift_status.as_deref()).collect::<Vec<Option<&str>>>(),
        "stage_counts" => model_runs.iter().map(|model_run| model_run.stage_counts.as_ref().map(|value| value.to_string())).collect::<Vec<Option<String>>>(),
        "started_at" => model_runs.iter().map(|model_run| model_run.started_at.timestamp_millis()).collect::<Vec<i64>>(),
        "completed_at" => model_runs.iter().map(|model_run| model_run.completed_at.map(|timestamp| timestamp.timestamp_millis())).collect::<Vec<Option<i64>>>(),
    )
    .map_err(|error| format!("Failed to create model run DataFrame: {}", error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use internal::market::Ticker;
    use serde_json::json;

    fn sample_quotes() -> Vec<EquityQuote> {
        let now = Utc::now();
        vec![
            EquityQuote {
                timestamp: now,
                ticker: Ticker::new("AAPL").unwrap(),
                bid_price: 150.50,
                ask_price: 150.55,
                bid_size: 10,
                ask_size: 5,
            },
            EquityQuote {
                timestamp: now,
                ticker: Ticker::new("MSFT").unwrap(),
                bid_price: 420.10,
                ask_price: 420.20,
                bid_size: 2,
                ask_size: 4,
            },
        ]
    }

    fn sample_bars() -> Vec<EquityBar> {
        let now = Utc::now();
        vec![EquityBar {
            ticker: Ticker::new("AAPL").unwrap(),
            timestamp: now,
            open_price: 150.0,
            high_price: 155.0,
            low_price: 149.0,
            close_price: 153.0,
            volume: 1_000_000,
            volume_weighted_average_price: Some(152.0),
            transactions: Some(50_000),
            inserted_at: now,
        }]
    }

    // Construct EquityRebalanceSession without importing uuid directly;
    // the id field type (Uuid) is inferred from the struct definition.
    fn sample_sessions() -> Vec<EquityRebalanceSession> {
        vec![EquityRebalanceSession {
            id: "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            triggered_at: Utc::now(),
            trigger_reason: "intraday_check".to_string(),
            model_run_id: Some("run-abc123".to_string()),
            completed_at: None,
            status: "completed".to_string(),
        }]
    }

    // Construct EquityPair without importing rust_decimal directly;
    // Decimal fields are inferred from the struct definition.
    fn sample_pairs() -> Vec<EquityPair> {
        vec![EquityPair {
            id: "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            rebalance_id: "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            pair_id: "AAPL-MSFT".to_string(),
            long_ticker: "AAPL".to_string(),
            short_ticker: "MSFT".to_string(),
            z_score: "2".parse().unwrap(),
            hedge_ratio: "1".parse().unwrap(),
            signal_strength: "0.75".parse().unwrap(),
            status: "open".to_string(),
            opened_at: Utc::now(),
            closed_at: None,
            realized_profit_and_loss: None,
            return_percent: None,
            holding_days: None,
        }]
    }

    fn sample_allocations() -> Vec<EquityAllocation> {
        vec![EquityAllocation {
            id: "550e8400-e29b-41d4-a716-446655440003".parse().unwrap(),
            rebalance_id: "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            equity_pair_id: "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            generated_at: Utc::now(),
            model_run_id: None,
            ticker: "AAPL".to_string(),
            side: "LONG".to_string(),
            action: "OPEN_POSITION".to_string(),
            dollar_amount: "10000".parse().unwrap(),
            entry_price: Some("150".parse().unwrap()),
            quantity: None,
            notional: Some("10000".parse().unwrap()),
        }]
    }

    fn sample_orders() -> Vec<EquityOrder> {
        vec![EquityOrder {
            id: "550e8400-e29b-41d4-a716-446655440004".parse().unwrap(),
            allocation_id: "550e8400-e29b-41d4-a716-446655440003".parse().unwrap(),
            submitted_at: Utc::now(),
            ticker: "MSFT".to_string(),
            side: "SHORT".to_string(),
            quantity: "25".parse().unwrap(),
            order_type: "market".to_string(),
            limit_price: None,
            alpaca_order_id: "alpaca-order-xyz".to_string(),
        }]
    }

    fn sample_snapshots() -> Vec<EquityPortfolioSnapshot> {
        vec![EquityPortfolioSnapshot {
            id: 1,
            snapshot_timestamp: Utc::now(),
            snapshot_type: "end_of_day".to_string(),
            net_asset_value: "100000".parse().unwrap(),
            gross_return: Some("0.02".parse().unwrap()),
            net_return: Some("0.018".parse().unwrap()),
            total_slippage_cost: "50".parse().unwrap(),
            created_at: Utc::now(),
        }]
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
    fn test_create_equity_bar_export_dataframe_columns_and_rows() {
        let bars = sample_bars();
        let dataframe = create_equity_bar_export_dataframe(&bars).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(dataframe.width(), 10);
        assert!(dataframe.column("ticker").is_ok());
        assert!(dataframe.column("open_price").is_ok());
        assert!(dataframe.column("volume").is_ok());
        assert!(dataframe.column("inserted_at").is_ok());
    }

    #[test]
    fn test_create_equity_bar_export_dataframe_empty() {
        let dataframe = create_equity_bar_export_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 10);
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
        assert_eq!(dataframe.width(), 14);
        assert!(dataframe.column("id").is_ok());
        assert!(dataframe.column("z_score").is_ok());
        assert!(dataframe.column("holding_days").is_ok());
    }

    #[test]
    fn test_create_equity_pair_dataframe_empty() {
        let dataframe = create_equity_pair_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 14);
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
            EquityPrediction {
                correlation_id: "550e8400-e29b-41d4-a716-446655440010".parse().unwrap(),
                model_run_id: "run-tide-001".to_string(),
                ticker: "AAPL".to_string(),
                timestamp: now,
                quantile_10: -0.02,
                quantile_50: 0.01,
                quantile_90: 0.04,
                created_at: now,
            },
            EquityPrediction {
                correlation_id: "550e8400-e29b-41d4-a716-446655440011".parse().unwrap(),
                model_run_id: "run-tide-001".to_string(),
                ticker: "MSFT".to_string(),
                timestamp: now,
                quantile_10: -0.01,
                quantile_50: 0.005,
                quantile_90: 0.02,
                created_at: now,
            },
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
            ModelRun {
                id: 1,
                run_id: "run-tide-001".to_string(),
                model_name: "tide".to_string(),
                artifact_key: Some("artifacts/tide/run-001/weights.safetensors".to_string()),
                training_data_key: Some("data/equity/bars/training.parquet".to_string()),
                start_date: Some(chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()),
                end_date: Some(chrono::NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()),
                lookback_days: Some(70),
                status: "completed".to_string(),
                continuous_ranked_probability_score: Some(0.42),
                directional_accuracy: Some(0.55),
                quantile_coverage: Some(0.88),
                drift_status: Some("stable".to_string()),
                stage_counts: Some(json!({"stage_1": 100})),
                started_at: Utc::now(),
                completed_at: Some(Utc::now()),
            },
            ModelRun {
                id: 2,
                run_id: "run-tide-002".to_string(),
                model_name: "tide".to_string(),
                artifact_key: None,
                training_data_key: None,
                start_date: None,
                end_date: None,
                lookback_days: None,
                status: "started".to_string(),
                continuous_ranked_probability_score: None,
                directional_accuracy: None,
                quantile_coverage: None,
                drift_status: None,
                stage_counts: None,
                started_at: Utc::now(),
                completed_at: None,
            },
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
    fn test_export_equity_bars_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::state::{DatabaseState, MassiveSecrets, State};
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
            let result = export_equity_bars(&state, date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("database not connected"));
        });
    }

    #[test]
    fn test_export_trading_history_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::state::{DatabaseState, MassiveSecrets, State};
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
            let result = export_trading_history(&state, date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("database not connected"));
        });
    }
}
