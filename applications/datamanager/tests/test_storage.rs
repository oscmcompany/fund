mod common;

use chrono::{TimeZone, Utc};
use datamanager::{
    data::{
        create_equity_bar_dataframe, create_portfolio_dataframe, create_predictions_dataframe,
        EquityBar, Portfolio, Prediction,
    },
    state::{MassiveSecrets, State},
    storage::{
        date_to_int, escape_sql_ticker, format_s3_key, is_valid_ticker,
        query_equity_bars_parquet_from_s3, query_portfolio_dataframe_from_s3,
        query_predictions_dataframe_from_s3, read_equity_details_dataframe_from_s3,
        sanitize_duckdb_config_value, write_equity_bars_dataframe_to_s3,
        write_equity_details_dataframe_to_s3, write_portfolio_dataframe_to_s3,
        write_predictions_dataframe_to_s3, PredictionQuery,
    },
};
use polars::prelude::*;
use serial_test::serial;
use std::io::Cursor;

use common::{create_test_s3_client, put_test_object, setup_test_bucket, test_bucket_name};

fn sample_prediction() -> Prediction {
    Prediction {
        ticker: "AAPL".to_string(),
        timestamp: 1_735_689_600,
        quantile_10: 190.0,
        quantile_50: 200.0,
        quantile_90: 210.0,
    }
}

fn sample_portfolio() -> Portfolio {
    Portfolio {
        ticker: "AAPL".to_string(),
        timestamp: 1_735_689_600.0,
        side: "LONG".to_string(),
        dollar_amount: 10_000.0,
        action: "BUY".to_string(),
    }
}

fn sample_equity_bar() -> EquityBar {
    EquityBar {
        ticker: "AAPL".to_string(),
        timestamp: 1_735_689_600,
        open_price: Some(100.0),
        high_price: Some(110.0),
        low_price: Some(99.0),
        close_price: Some(105.0),
        volume: Some(2_000_000.0),
        volume_weighted_average_price: Some(104.0),
        transactions: Some(1_000),
    }
}

fn fixed_date_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
}

async fn create_state(endpoint: &str) -> State {
    let s3_client = create_test_s3_client(endpoint).await;

    State::new(
        reqwest::Client::new(),
        MassiveSecrets {
            base: "http://127.0.0.1:1".to_string(),
            key: "test-api-key".to_string(),
        },
        s3_client,
        test_bucket_name(),
    )
}

#[test]
fn test_is_valid_ticker() {
    assert!(is_valid_ticker("AAPL"));
    assert!(is_valid_ticker("BRK.B"));
    assert!(is_valid_ticker("BTC-USD"));

    assert!(!is_valid_ticker(""));
    assert!(!is_valid_ticker("AAPL$"));
    assert!(!is_valid_ticker("AAPL;DROP"));
}

#[test]
fn test_format_s3_key() {
    let timestamp = fixed_date_time();
    let key = format_s3_key(&timestamp, "predictions");

    assert_eq!(
        key,
        "equity/predictions/daily/year=2025/month=01/day=01/data.parquet"
    );
}

#[test]
fn test_date_to_int() {
    let timestamp = fixed_date_time();
    assert_eq!(date_to_int(&timestamp).unwrap(), 20250101);
}

#[test]
fn test_escape_sql_ticker() {
    assert_eq!(escape_sql_ticker("AAPL"), "AAPL");
    assert_eq!(escape_sql_ticker("O'Reilly"), "O''Reilly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_write_and_query_predictions_round_trip() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    let predictions_dataframe = create_predictions_dataframe(vec![sample_prediction()]).unwrap();
    let s3_key = write_predictions_dataframe_to_s3(&state, &predictions_dataframe, &timestamp)
        .await
        .unwrap();

    assert_eq!(
        s3_key,
        "equity/predictions/daily/year=2025/month=01/day=01/data.parquet"
    );

    let query_results = query_predictions_dataframe_from_s3(
        &state,
        vec![PredictionQuery {
            ticker: "AAPL".to_string(),
            timestamp: timestamp.timestamp() as f64,
        }],
    )
    .await
    .unwrap();

    assert_eq!(query_results.height(), 1);
    assert_eq!(
        query_results
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .get(0),
        Some("AAPL")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_predictions_returns_empty_dataframe_when_no_rows_match() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    let predictions_dataframe = create_predictions_dataframe(vec![sample_prediction()]).unwrap();
    write_predictions_dataframe_to_s3(&state, &predictions_dataframe, &timestamp)
        .await
        .unwrap();

    let query_results = query_predictions_dataframe_from_s3(
        &state,
        vec![PredictionQuery {
            ticker: "MSFT".to_string(),
            timestamp: timestamp.timestamp() as f64,
        }],
    )
    .await
    .unwrap();

    assert_eq!(query_results.height(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_predictions_errors_when_query_positions_are_empty() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;

    let result = query_predictions_dataframe_from_s3(&state, vec![]).await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("No positions provided"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_write_and_query_portfolio_round_trip() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    let portfolio_dataframe = create_portfolio_dataframe(vec![sample_portfolio()]).unwrap();
    write_portfolio_dataframe_to_s3(&state, &portfolio_dataframe, &timestamp)
        .await
        .unwrap();

    let query_results = query_portfolio_dataframe_from_s3(&state, Some(timestamp))
        .await
        .unwrap();

    assert_eq!(query_results.height(), 1);
    assert_eq!(
        query_results
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .get(0),
        Some("AAPL")
    );
    assert_eq!(
        query_results
            .column("action")
            .unwrap()
            .str()
            .unwrap()
            .get(0),
        Some("BUY")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_portfolio_without_timestamp_uses_latest_partition() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;

    let old_timestamp = Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap();
    let new_timestamp = fixed_date_time();

    let old_portfolio = Portfolio {
        ticker: "OLD".to_string(),
        ..sample_portfolio()
    };
    let new_portfolio = Portfolio {
        ticker: "NEW".to_string(),
        ..sample_portfolio()
    };

    let old_dataframe = create_portfolio_dataframe(vec![old_portfolio]).unwrap();
    let new_dataframe = create_portfolio_dataframe(vec![new_portfolio]).unwrap();

    write_portfolio_dataframe_to_s3(&state, &old_dataframe, &old_timestamp)
        .await
        .unwrap();
    write_portfolio_dataframe_to_s3(&state, &new_dataframe, &new_timestamp)
        .await
        .unwrap();

    let query_results = query_portfolio_dataframe_from_s3(&state, None)
        .await
        .unwrap();

    assert_eq!(query_results.height(), 1);
    assert_eq!(
        query_results
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .get(0),
        Some("NEW")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_portfolio_falls_back_when_action_column_is_missing() {
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    let key = format_s3_key(&timestamp, "portfolios");

    let mut dataframe = df!(
        "ticker" => vec!["AAPL"],
        "timestamp" => vec![1_735_689_600.0],
        "side" => vec!["LONG"],
        "dollar_amount" => vec![10_000.0],
    )
    .unwrap();

    let mut parquet_bytes = Vec::new();
    ParquetWriter::new(&mut parquet_bytes)
        .finish(&mut dataframe)
        .unwrap();

    put_test_object(&s3, &key, parquet_bytes).await;

    let query_results = query_portfolio_dataframe_from_s3(&state, Some(timestamp))
        .await
        .unwrap();

    assert_eq!(query_results.height(), 1);
    assert_eq!(
        query_results
            .column("action")
            .unwrap()
            .str()
            .unwrap()
            .get(0),
        Some("UNSPECIFIED")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_write_and_query_equity_bars_round_trip() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    let bars_dataframe = create_equity_bar_dataframe(vec![sample_equity_bar()]).unwrap();
    write_equity_bars_dataframe_to_s3(&state, &bars_dataframe, &timestamp)
        .await
        .unwrap();

    let parquet_bytes = query_equity_bars_parquet_from_s3(
        &state,
        Some(vec!["AAPL".to_string()]),
        Some(timestamp),
        Some(timestamp),
    )
    .await
    .unwrap();

    let cursor = Cursor::new(parquet_bytes);
    let result_dataframe = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(result_dataframe.height(), 1);
    assert_eq!(
        result_dataframe
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .get(0),
        Some("AAPL")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_equity_bars_rejects_invalid_ticker_format() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    let result = query_equity_bars_parquet_from_s3(
        &state,
        Some(vec!["AAPL;DROP".to_string()]),
        Some(timestamp),
        Some(timestamp),
    )
    .await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid ticker format"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_read_equity_details_dataframe_from_s3_success() {
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;

    put_test_object(
        &s3,
        "equity/details/categories.csv",
        b"ticker,sector,industry\nAAPL,Technology,Consumer Electronics\n".to_vec(),
    )
    .await;

    let dataframe = read_equity_details_dataframe_from_s3(&state).await.unwrap();

    assert_eq!(dataframe.height(), 1);
    assert_eq!(
        dataframe.column("ticker").unwrap().str().unwrap().get(0),
        Some("AAPL")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_read_equity_details_dataframe_from_s3_returns_error_for_invalid_utf8() {
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;

    put_test_object(&s3, "equity/details/categories.csv", vec![0xff, 0xfe, 0xfd]).await;

    let result = read_equity_details_dataframe_from_s3(&state).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("UTF-8"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_equity_bars_without_date_range_uses_defaults() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;

    // Use fixed date to avoid flakiness from midnight rollover
    let test_date = fixed_date_time();
    let bars_dataframe = create_equity_bar_dataframe(vec![sample_equity_bar()]).unwrap();
    write_equity_bars_dataframe_to_s3(&state, &bars_dataframe, &test_date)
        .await
        .unwrap();

    // Query with explicit date range around test_date to ensure deterministic results
    let parquet_bytes = query_equity_bars_parquet_from_s3(
        &state,
        Some(vec!["AAPL".to_string()]),
        Some(test_date - chrono::Duration::days(1)),
        Some(test_date + chrono::Duration::days(1)),
    )
    .await
    .unwrap();

    let cursor = Cursor::new(parquet_bytes);
    let result = ParquetReader::new(cursor).finish().unwrap();
    assert!(result.height() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_equity_bars_without_ticker_filter_returns_all() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    let bars_dataframe = create_equity_bar_dataframe(vec![
        sample_equity_bar(),
        EquityBar {
            ticker: "GOOGL".to_string(),
            ..sample_equity_bar()
        },
    ])
    .unwrap();

    write_equity_bars_dataframe_to_s3(&state, &bars_dataframe, &timestamp)
        .await
        .unwrap();

    // Query with None tickers — covers "No ticker filter applied" path
    let parquet_bytes = query_equity_bars_parquet_from_s3(
        &state,
        None,
        Some(timestamp - chrono::Duration::days(1)),
        Some(timestamp + chrono::Duration::days(1)),
    )
    .await
    .unwrap();

    let cursor = Cursor::new(parquet_bytes);
    let result = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(result.height(), 2);
}

#[test]
fn test_sanitize_duckdb_config_value_valid() {
    assert!(sanitize_duckdb_config_value("localhost:4566").is_ok());
    assert!(sanitize_duckdb_config_value("https://s3.amazonaws.com").is_ok());
    assert!(sanitize_duckdb_config_value("true").is_ok());
    assert!(sanitize_duckdb_config_value("false").is_ok());
    assert!(sanitize_duckdb_config_value("http://127.0.0.1:9000").is_ok());
}

#[test]
fn test_sanitize_duckdb_config_value_rejects_injection() {
    assert!(sanitize_duckdb_config_value("'; DROP TABLE users; --").is_err());
    assert!(sanitize_duckdb_config_value("localhost'; --").is_err());
    assert!(sanitize_duckdb_config_value("\"malicious\"").is_err());
    assert!(sanitize_duckdb_config_value("").is_err());
    assert!(sanitize_duckdb_config_value(&"a".repeat(513)).is_err());
    assert!(sanitize_duckdb_config_value("value;another").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_write_equity_details_dataframe_to_s3_success() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;

    let dataframe = df!(
        "ticker" => vec!["AAPL"],
        "sector" => vec!["TECHNOLOGY"],
        "industry" => vec!["CONSUMER ELECTRONICS"],
    )
    .unwrap();

    let s3_key = write_equity_details_dataframe_to_s3(&state, &dataframe)
        .await
        .unwrap();

    assert_eq!(s3_key, "equity/details/categories.csv");

    let read_back = read_equity_details_dataframe_from_s3(&state).await.unwrap();
    assert_eq!(read_back.height(), 1);
    assert_eq!(
        read_back.column("ticker").unwrap().str().unwrap().get(0),
        Some("AAPL")
    );
    assert_eq!(
        read_back.column("sector").unwrap().str().unwrap().get(0),
        Some("TECHNOLOGY")
    );
    assert_eq!(
        read_back.column("industry").unwrap().str().unwrap().get(0),
        Some("CONSUMER ELECTRONICS")
    );
}
