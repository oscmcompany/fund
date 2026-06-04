mod common;

use common::initialize_test_tracing;
use data_manager::data::{create_equity_bar_dataframe, EquityBar, Ticker};
use polars::prelude::*;

fn sample_equity_bar() -> EquityBar {
    let timestamp = chrono::DateTime::from_timestamp(1_234_567_890, 0).unwrap();
    EquityBar {
        ticker: Ticker::new("AAPL").unwrap(),
        timestamp,
        open_price: 100.0,
        high_price: 105.0,
        low_price: 99.0,
        close_price: 103.0,
        volume: 1_000_000,
        volume_weighted_average_price: Some(102.0),
        transactions: Some(5_000),
        inserted_at: timestamp,
    }
}

fn sample_equity_bar_lowercase() -> EquityBar {
    let timestamp = chrono::DateTime::from_timestamp(1_234_567_890, 0).unwrap();
    EquityBar {
        ticker: Ticker::new("googl").unwrap(),
        timestamp,
        open_price: 2000.0,
        high_price: 2050.0,
        low_price: 1990.0,
        close_price: 2030.0,
        volume: 500_000,
        volume_weighted_average_price: Some(2020.0),
        transactions: Some(2_500),
        inserted_at: timestamp,
    }
}

#[test]
fn test_create_equity_bar_dataframe_valid_data() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar()];

    let dataframe = create_equity_bar_dataframe(&bars).unwrap();

    assert_eq!(dataframe.height(), 1);
    assert_eq!(dataframe.width(), 10);
    assert!(dataframe.column("ticker").is_ok());
    assert!(dataframe.column("timestamp").is_ok());
    assert!(dataframe.column("open_price").is_ok());
    assert!(dataframe.column("high_price").is_ok());
    assert!(dataframe.column("low_price").is_ok());
    assert!(dataframe.column("close_price").is_ok());
    assert!(dataframe.column("volume").is_ok());
    assert!(dataframe.column("volume_weighted_average_price").is_ok());
    assert!(dataframe.column("transactions").is_ok());
    assert!(dataframe.column("inserted_at").is_ok());
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
    let bars = vec![EquityBar {
        ticker: Ticker::new("  ECC           ").unwrap(),
        timestamp,
        open_price: 10.0,
        high_price: 11.0,
        low_price: 9.0,
        close_price: 10.5,
        volume: 100_000,
        volume_weighted_average_price: Some(10.2),
        transactions: Some(500),
        inserted_at: timestamp,
    }];

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
    assert_eq!(dataframe.width(), 10);
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
    assert_eq!(dataframe.width(), 10);
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
    assert!(deserialized_df.column("inserted_at").is_ok());

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
