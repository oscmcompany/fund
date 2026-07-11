mod common;

use common::initialize_test_tracing;
use fund::data::types::{create_equity_bar_dataframe, EquityBar, Ticker};
use polars::prelude::*;

fn sample_equity_bar() -> EquityBar {
    let timestamp = chrono::DateTime::from_timestamp(1_234_567_890, 0).unwrap();
    EquityBar::new(
        Ticker::new("AAPL").unwrap(),
        timestamp,
        100.0,
        105.0,
        99.0,
        103.0,
        1_000_000,
        Some(102.0),
        Some(5_000),
        timestamp,
    )
}

fn sample_equity_bar_lowercase() -> EquityBar {
    let timestamp = chrono::DateTime::from_timestamp(1_234_567_890, 0).unwrap();
    EquityBar::new(
        Ticker::new("googl").unwrap(),
        timestamp,
        2000.0,
        2050.0,
        1990.0,
        2030.0,
        500_000,
        Some(2020.0),
        Some(2_500),
        timestamp,
    )
}

#[test]
fn test_create_equity_bar_dataframe_valid_data() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar()];

    let dataframe = create_equity_bar_dataframe(&bars).unwrap();

    assert_eq!(dataframe.height(), 1);
    assert_eq!(dataframe.width(), 9);
    assert!(dataframe.column("ticker").is_ok());
    assert!(dataframe.column("timestamp").is_ok());
    assert!(dataframe.column("open_price").is_ok());
    assert!(dataframe.column("high_price").is_ok());
    assert!(dataframe.column("low_price").is_ok());
    assert!(dataframe.column("close_price").is_ok());
    assert!(dataframe.column("volume").is_ok());
    assert!(dataframe.column("volume_weighted_average_price").is_ok());
    assert!(dataframe.column("transactions").is_ok());
    // inserted_at is deliberately absent: the S3 parquet schema is the
    // 9-column equity_bars_schema pandera contract.
    assert!(dataframe.column("inserted_at").is_err());
}

#[test]
fn test_create_equity_bar_dataframe_uppercase_normalization() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar_lowercase()];

    let dataframe = create_equity_bar_dataframe(&bars).unwrap();

    let ticker = dataframe
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();

    assert_eq!(ticker, "GOOGL");
}

#[test]
fn test_create_equity_bar_dataframe_whitespace_trimming() {
    initialize_test_tracing();
    let timestamp = chrono::DateTime::from_timestamp(1_234_567_890, 0).unwrap();
    let bars = vec![EquityBar::new(
        Ticker::new("  ECC           ").unwrap(),
        timestamp,
        10.0,
        11.0,
        9.0,
        10.5,
        100_000,
        Some(10.2),
        Some(500),
        timestamp,
    )];

    let dataframe = create_equity_bar_dataframe(&bars).unwrap();

    let ticker = dataframe
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(ticker, "ECC");
}

#[test]
fn test_create_equity_bar_dataframe_mixed_case_tickers() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar(), sample_equity_bar_lowercase()];

    let dataframe = create_equity_bar_dataframe(&bars).unwrap();

    assert_eq!(dataframe.height(), 2);

    let tickers = dataframe
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|t| t.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(tickers, vec!["AAPL", "GOOGL"]);
}

#[test]
fn test_create_equity_bar_dataframe_empty_vec() {
    initialize_test_tracing();
    let bars: Vec<EquityBar> = vec![];

    let dataframe = create_equity_bar_dataframe(&bars).unwrap();

    assert_eq!(dataframe.height(), 0);
    assert_eq!(dataframe.width(), 9);
}

#[test]
fn test_create_equity_bar_dataframe_multiple_rows() {
    initialize_test_tracing();
    let bars = vec![
        sample_equity_bar(),
        sample_equity_bar(),
        sample_equity_bar(),
    ];

    let dataframe = create_equity_bar_dataframe(&bars).unwrap();

    assert_eq!(dataframe.height(), 3);
    assert_eq!(dataframe.width(), 9);
}

#[test]
fn test_equity_bar_dataframe_parquet_roundtrip() {
    initialize_test_tracing();
    use std::io::Cursor;

    let original_bars = vec![sample_equity_bar()];
    let original_df = create_equity_bar_dataframe(&original_bars).unwrap();

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut original_df.clone())
        .unwrap();

    let cursor = Cursor::new(buffer);
    let deserialized_df = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(deserialized_df.width(), 10);
    assert_eq!(deserialized_df.height(), 1);

    assert!(deserialized_df.column("ticker").is_ok());
    assert!(deserialized_df.column("timestamp").is_ok());
    assert!(deserialized_df.column("open_price").is_ok());
    assert!(deserialized_df.column("high_price").is_ok());
    assert!(deserialized_df.column("low_price").is_ok());
    assert!(deserialized_df.column("close_price").is_ok());
    assert!(deserialized_df.column("volume").is_ok());
    assert!(deserialized_df
        .column("volume_weighted_average_price")
        .is_ok());
    assert!(deserialized_df.column("transactions").is_ok());
    assert!(deserialized_df.column("inserted_at").is_err());

    let ticker_series = deserialized_df.column("ticker").unwrap();
    assert_eq!(ticker_series.str().unwrap().get(0).unwrap(), "AAPL");
}

#[test]
fn test_parquet_empty_dataframe_roundtrip() {
    initialize_test_tracing();
    use std::io::Cursor;

    let empty_bars: Vec<EquityBar> = vec![];
    let original_df = create_equity_bar_dataframe(&empty_bars).unwrap();

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut original_df.clone())
        .unwrap();

    let cursor = Cursor::new(buffer);
    let deserialized_df = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(deserialized_df.width(), 10);
    assert_eq!(deserialized_df.height(), 0);
}
