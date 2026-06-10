//! Parquet export tasks for equity market data and trading history.
//!
//! Each task reads rows from PostgreSQL into typed structs using explicit
//! column lists, serializes to Parquet with deterministic column ordering,
//! and writes to S3. Failures are surfaced as structured log entries.

use crate::data_manager::{database, state::State};
use crate::domain::market::{EquityBar, EquityQuote};
use crate::domain::trading::{
    EquityAllocation, EquityOrder, EquityPair, EquityPortfolioSnapshot, EquityRebalanceSession,
};
use aws_sdk_s3::primitives::ByteStream;
use chrono::{Datelike, NaiveDate};
use polars::prelude::*;
use tracing::info;

/// Exports equity quotes for the given date to S3 Parquet and deletes
/// the exported rows from the database.
pub async fn export_equity_quotes(state: &State, date: NaiveDate) -> Result<usize, String> {
    let pool = state
        .database
        .pool()
        .ok_or_else(|| "database not connected".to_string())?;

    let quotes = database::query_equity_quotes_for_date(pool, date)
        .await
        .map_err(|error| format!("Failed to query equity quotes: {}", error))?;

    let count = quotes.len();

    if count == 0 {
        info!("No equity quotes to export for {}", date);
        return Ok(0);
    }

    let mut dataframe = create_equity_quote_dataframe(&quotes)?;
    let key = date_partitioned_key("data/equity/quotes", date);
    write_dataframe_to_s3(state, &mut dataframe, &key).await?;
    info!("Exported {} equity quotes to S3: {}", count, key);

    database::delete_equity_quotes_for_date(pool, date)
        .await
        .map_err(|error| format!("Failed to delete equity quotes: {}", error))?;

    Ok(count)
}

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
    let key = date_partitioned_key("data/equity/bars", date);
    write_dataframe_to_s3(state, &mut dataframe, &key).await?;
    info!("Exported {} equity bars to S3: {}", count, key);

    Ok(count)
}

/// Exports all trading history tables to S3 Parquet.
pub async fn export_equity_trading_history(
    state: &State,
    date: NaiveDate,
) -> Result<usize, String> {
    let pool = state
        .database
        .pool()
        .ok_or_else(|| "database not connected".to_string())?;

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

    info!(
        "Exported trading history to S3: {} sessions, {} pairs, {} allocations, {} orders, {} snapshots",
        session_count, pair_count, allocation_count, order_count, snapshot_count
    );

    Ok(session_count + pair_count + allocation_count + order_count + snapshot_count)
}

fn date_partitioned_key(prefix: &str, date: NaiveDate) -> String {
    format!(
        "{}/year={}/month={:02}/day={:02}/data.parquet",
        prefix,
        date.year(),
        date.month(),
        date.day()
    )
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

fn create_equity_bar_export_dataframe(bars: &[EquityBar]) -> Result<DataFrame, String> {
    // The exported parquet must match the equity_bars_schema pandera contract
    // (9 columns, Int64 millisecond timestamp) and the backfill writer
    // (data::create_equity_bar_dataframe): the tide trainer concatenates the
    // daily exports with backfilled files, so a schema drift breaks training.
    // inserted_at stays on the EquityBar row for PostgreSQL only.
    df!(
        "ticker" => bars.iter().map(|bar| bar.ticker().as_str()).collect::<Vec<&str>>(),
        "timestamp" => bars.iter().map(|bar| bar.timestamp().timestamp_millis()).collect::<Vec<i64>>(),
        "open_price" => bars.iter().map(|bar| bar.open_price()).collect::<Vec<f64>>(),
        "high_price" => bars.iter().map(|bar| bar.high_price()).collect::<Vec<f64>>(),
        "low_price" => bars.iter().map(|bar| bar.low_price()).collect::<Vec<f64>>(),
        "close_price" => bars.iter().map(|bar| bar.close_price()).collect::<Vec<f64>>(),
        "volume" => bars.iter().map(|bar| bar.volume()).collect::<Vec<i64>>(),
        "volume_weighted_average_price" => bars.iter().map(|bar| bar.volume_weighted_average_price()).collect::<Vec<Option<f64>>>(),
        "transactions" => bars.iter().map(|bar| bar.transactions()).collect::<Vec<Option<i64>>>(),
    )
    .map_err(|error| format!("Failed to create equity bar export DataFrame: {}", error))
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
        "pair_id" => pairs.iter().map(|pair| pair.pair_id()).collect::<Vec<&str>>(),
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
        "holding_days" => pairs.iter().map(|pair| pair.holding_days()).collect::<Vec<Option<i32>>>(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::market::Ticker;
    use crate::domain::trading::{
        AllocationAction, AllocationSide, EquityPairStatus, RebalanceSessionStatus, SnapshotType,
    };
    use chrono::Utc;

    fn sample_quotes() -> Vec<EquityQuote> {
        let now = Utc::now();
        vec![
            EquityQuote::new(now, Ticker::new("AAPL").unwrap(), 150.50, 150.55, 10, 5),
            EquityQuote::new(now, Ticker::new("MSFT").unwrap(), 420.10, 420.20, 2, 4),
        ]
    }

    fn sample_bars() -> Vec<EquityBar> {
        let now = Utc::now();
        vec![EquityBar::new(
            Ticker::new("AAPL").unwrap(),
            now,
            150.0,
            155.0,
            149.0,
            153.0,
            1_000_000,
            Some(152.0),
            Some(50_000),
            now,
        )]
    }

    fn sample_sessions() -> Vec<EquityRebalanceSession> {
        vec![EquityRebalanceSession::new(
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            Utc::now(),
            "intraday_check".to_string(),
            Some("run-abc123".to_string()),
            None,
            RebalanceSessionStatus::Completed,
        )]
    }

    fn sample_pairs() -> Vec<EquityPair> {
        vec![EquityPair::new(
            "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
            "AAPL-MSFT".to_string(),
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
    fn test_create_equity_bar_export_dataframe_matches_pandera_contract() {
        // The S3 parquet schema is the equity_bars_schema pandera contract:
        // 9 columns, Int64 millisecond timestamp, no inserted_at. The tide
        // trainer concatenates these daily exports with the backfill writer's
        // files (create_equity_bar_dataframe), so the schemas must agree.
        let bars = sample_bars();
        let dataframe = create_equity_bar_export_dataframe(&bars).unwrap();
        assert_eq!(dataframe.height(), 1);
        assert_eq!(
            dataframe.get_column_names_str(),
            vec![
                "ticker",
                "timestamp",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "volume_weighted_average_price",
                "transactions",
            ]
        );
        assert!(dataframe.column("inserted_at").is_err());
        assert_eq!(
            dataframe.column("timestamp").unwrap().dtype(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_create_equity_bar_export_dataframe_empty() {
        let dataframe = create_equity_bar_export_dataframe(&[]).unwrap();
        assert_eq!(dataframe.height(), 0);
        assert_eq!(dataframe.width(), 9);
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

    #[test]
    fn test_export_equity_quotes_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data_manager::state::{DatabaseState, MassiveSecrets, State};
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
            let result = export_equity_quotes(&state, date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("database not connected"));
        });
    }

    #[test]
    fn test_export_equity_bars_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data_manager::state::{DatabaseState, MassiveSecrets, State};
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
    fn test_export_equity_trading_history_returns_error_when_no_database() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use crate::data_manager::state::{DatabaseState, MassiveSecrets, State};
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
            let result = export_equity_trading_history(&state, date).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("database not connected"));
        });
    }
}
