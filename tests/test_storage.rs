mod common;

use chrono::{NaiveDate, TimeZone, Utc};
use fund::data_manager::{
    data::{create_equity_bar_dataframe, EquityBar},
    state::{MassiveSecrets, State},
    storage::{
        date_to_int, escape_sql_ticker, is_valid_ticker, query_equity_bars_parquet_from_s3,
        read_equity_details_dataframe_from_s3, sanitize_duckdb_config_value,
        write_equity_bars_to_s3, DUCKDB_CONFIG_VALUE_MAX_LENGTH,
    },
};
use polars::prelude::*;
use serial_test::serial;
use std::io::Cursor;

use common::{create_test_s3_client, put_test_object, setup_test_bucket, test_bucket_name};

fn sample_equity_bar() -> EquityBar {
    EquityBar {
        ticker: "AAPL".to_string(),
        timestamp: 1_735_689_600_000,
        open_price: Some(100.0),
        high_price: Some(110.0),
        low_price: Some(99.0),
        close_price: Some(105.0),
        volume: Some(2_000_000),
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

async fn seed_equity_bars_parquet(
    s3: &aws_sdk_s3::Client,
    bars: Vec<EquityBar>,
    timestamp: &chrono::DateTime<Utc>,
) {
    let mut dataframe = create_equity_bar_dataframe(bars).unwrap();
    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut dataframe)
        .unwrap();
    let key = format!(
        "data/equity/bars/year={}/month={}/day={}/data.parquet",
        timestamp.format("%Y"),
        timestamp.format("%m"),
        timestamp.format("%d")
    );
    put_test_object(s3, &key, buffer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_write_equity_bars_to_s3_schema_and_key() {
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let date = NaiveDate::from_ymd_opt(2025, 3, 7).unwrap();

    write_equity_bars_to_s3(&state, date, &[sample_equity_bar()])
        .await
        .unwrap();

    // Key must be zero-padded to match the pg_parquet export layout.
    let object = s3
        .get_object()
        .bucket(test_bucket_name())
        .key("data/equity/bars/year=2025/month=03/day=07/data.parquet")
        .send()
        .await
        .expect("partition object should exist");
    let bytes = object.body.collect().await.unwrap().into_bytes();
    let dataframe = ParquetReader::new(Cursor::new(bytes.to_vec()))
        .finish()
        .unwrap();

    // Column order and timestamp dtype must match the equity_bars_schema contract.
    let column_names: Vec<String> = dataframe
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    assert_eq!(
        column_names,
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
    assert_eq!(
        dataframe.column("timestamp").unwrap().dtype(),
        &DataType::Int64
    );
    assert_eq!(
        dataframe.column("timestamp").unwrap().i64().unwrap().get(0),
        Some(1_735_689_600_000)
    );
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
async fn test_write_and_query_equity_bars_round_trip() {
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    seed_equity_bars_parquet(&s3, vec![sample_equity_bar()], &timestamp).await;

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

    // Prove timestamp survives the round-trip as millisecond Int64.
    // A regression to Float64 or seconds would cause downstream consumers
    // to silently receive wrong values.
    assert_eq!(
        *result_dataframe.column("timestamp").unwrap().dtype(),
        DataType::Int64
    );
    assert_eq!(
        result_dataframe
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .get(0),
        Some(1_735_689_600_000i64)
    );

    // Prove volume survives as whole-share Int64.
    assert_eq!(
        *result_dataframe.column("volume").unwrap().dtype(),
        DataType::Int64
    );
    assert_eq!(
        result_dataframe
            .column("volume")
            .unwrap()
            .i64()
            .unwrap()
            .get(0),
        Some(2_000_000i64)
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
        "data/equity/details/details.csv",
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

    put_test_object(
        &s3,
        "data/equity/details/details.csv",
        vec![0xff, 0xfe, 0xfd],
    )
    .await;

    let result = read_equity_details_dataframe_from_s3(&state).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("UTF-8"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_query_equity_bars_without_date_range_uses_defaults() {
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;

    let test_date = fixed_date_time();
    seed_equity_bars_parquet(&s3, vec![sample_equity_bar()], &test_date).await;

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
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;
    let state = create_state(&endpoint).await;
    let timestamp = fixed_date_time();

    seed_equity_bars_parquet(
        &s3,
        vec![
            sample_equity_bar(),
            EquityBar {
                ticker: "GOOGL".to_string(),
                ..sample_equity_bar()
            },
        ],
        &timestamp,
    )
    .await;

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
    // AWS ECS session tokens can exceed 1000 characters
    assert!(sanitize_duckdb_config_value(&"a".repeat(1052)).is_ok());
}

#[test]
fn test_sanitize_duckdb_config_value_rejects_injection() {
    assert!(sanitize_duckdb_config_value("'; DROP TABLE users; --").is_err());
    assert!(sanitize_duckdb_config_value("localhost'; --").is_err());
    assert!(sanitize_duckdb_config_value("\"malicious\"").is_err());
    assert!(sanitize_duckdb_config_value("").is_err());
    assert!(sanitize_duckdb_config_value(&"a".repeat(DUCKDB_CONFIG_VALUE_MAX_LENGTH + 1)).is_err());
    assert!(sanitize_duckdb_config_value("value;another").is_err());
}
