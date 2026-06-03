mod common;

use common::initialize_test_tracing;
use data_manager::data::{create_equity_bar_dataframe, create_equity_details_dataframe, EquityBar};
use polars::prelude::*;

#[allow(dead_code)]
fn sample_equity_bar() -> EquityBar {
    EquityBar {
        ticker: "AAPL".to_string(),
        timestamp: 1234567890000,
        open_price: Some(100.0),
        high_price: Some(105.0),
        low_price: Some(99.0),
        close_price: Some(103.0),
        volume: Some(1000000),
        volume_weighted_average_price: Some(102.0),
        transactions: Some(5000),
    }
}

#[allow(dead_code)]
fn sample_equity_bar_lowercase() -> EquityBar {
    EquityBar {
        ticker: "googl".to_string(),
        timestamp: 1234567890000,
        open_price: Some(2000.0),
        high_price: Some(2050.0),
        low_price: Some(1990.0),
        close_price: Some(2030.0),
        volume: Some(500000),
        volume_weighted_average_price: Some(2020.0),
        transactions: Some(2500),
    }
}

#[test]
fn test_create_equity_bar_dataframe_valid_data() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar()];

    let dataframe = create_equity_bar_dataframe(bars).unwrap();

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
}

#[test]
fn test_create_equity_bar_dataframe_uppercase_normalization() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar_lowercase()];

    let dataframe = create_equity_bar_dataframe(bars).unwrap();

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
    let bars = vec![EquityBar {
        ticker: "  ECC           ".to_string(),
        timestamp: 1234567890000,
        open_price: Some(10.0),
        high_price: Some(11.0),
        low_price: Some(9.0),
        close_price: Some(10.5),
        volume: Some(100000),
        volume_weighted_average_price: Some(10.2),
        transactions: Some(500),
    }];

    let dataframe = create_equity_bar_dataframe(bars).unwrap();

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

    let dataframe = create_equity_bar_dataframe(bars).unwrap();

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

    let dataframe = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(dataframe.height(), 0);
    assert_eq!(dataframe.width(), 9);
}

#[test]
fn test_create_equity_bar_dataframe_with_none_prices() {
    initialize_test_tracing();
    let bars = vec![EquityBar {
        ticker: "TEST".to_string(),
        timestamp: 1234567890000,
        open_price: None,
        high_price: None,
        low_price: None,
        close_price: None,
        volume: None,
        volume_weighted_average_price: None,
        transactions: None,
    }];

    let dataframe = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(dataframe.height(), 1);

    let close_price = dataframe.column("close_price").unwrap();
    assert_eq!(close_price.len(), 1);
}

#[test]
fn test_create_equity_bar_dataframe_multiple_rows() {
    initialize_test_tracing();
    let bars = vec![
        sample_equity_bar(),
        sample_equity_bar(),
        sample_equity_bar(),
    ];

    let dataframe = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(dataframe.height(), 3);
    assert_eq!(dataframe.width(), 9);
}

// Tests for create_equity_details_dataframe

#[test]
fn test_create_equity_details_dataframe_valid_csv() {
    initialize_test_tracing();
    let csv_content =
        "ticker,sector,industry\nAAPL,Technology,Consumer Electronics\nGOOGL,Technology,Internet Services\n";

    let dataframe = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(dataframe.height(), 2);
    assert_eq!(dataframe.width(), 3);
    assert!(dataframe.column("ticker").is_ok());
    assert!(dataframe.column("sector").is_ok());
    assert!(dataframe.column("industry").is_ok());
}

#[test]
fn test_create_equity_details_dataframe_whitespace_trimming() {
    initialize_test_tracing();
    let csv_content =
        "ticker,sector,industry\nECC           ,  Technology  ,  Consumer Electronics  \n";

    let dataframe = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    let ticker = dataframe
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(ticker, "ECC");

    let sector = dataframe
        .column("sector")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(sector, "TECHNOLOGY");

    let industry = dataframe
        .column("industry")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(industry, "CONSUMER ELECTRONICS");
}

#[test]
fn test_create_equity_details_dataframe_uppercase_normalization() {
    initialize_test_tracing();
    let csv_content = "ticker,sector,industry\naapl,technology,consumer electronics\n";

    let dataframe = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    let ticker = dataframe
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(ticker, "AAPL");

    let sector = dataframe
        .column("sector")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(sector, "TECHNOLOGY");

    let industry = dataframe
        .column("industry")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(industry, "CONSUMER ELECTRONICS");
}

#[test]
fn test_create_equity_details_dataframe_with_nulls() {
    initialize_test_tracing();
    let csv_content = "ticker,sector,industry\nAAPL,,\n";

    let dataframe = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(dataframe.height(), 1);

    let sector = dataframe
        .column("sector")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(sector, "NOT AVAILABLE");

    let industry = dataframe
        .column("industry")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(industry, "NOT AVAILABLE");
}

#[test]
fn test_create_equity_details_dataframe_extra_columns() {
    initialize_test_tracing();
    let csv_content =
        "ticker,sector,industry,extra_column\nAAPL,Technology,Consumer Electronics,Extra\n";

    let dataframe = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(dataframe.height(), 1);
    assert_eq!(dataframe.width(), 3);
    assert!(dataframe.column("extra_column").is_err());
}

#[test]
fn test_create_equity_details_dataframe_missing_ticker_column() {
    initialize_test_tracing();
    let csv_content = "sector,industry\nTechnology,Consumer Electronics\n";

    let result = create_equity_details_dataframe(csv_content.to_string());

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("missing required column"));
}

#[test]
fn test_create_equity_details_dataframe_missing_sector_column() {
    initialize_test_tracing();
    let csv_content = "ticker,industry\nAAPL,Consumer Electronics\n";

    let result = create_equity_details_dataframe(csv_content.to_string());

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("missing required column"));
}

#[test]
fn test_create_equity_details_dataframe_missing_industry_column() {
    initialize_test_tracing();
    let csv_content = "ticker,sector\nAAPL,Technology\n";

    let result = create_equity_details_dataframe(csv_content.to_string());

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("missing required column"));
}

#[test]
fn test_create_equity_details_dataframe_empty_csv() {
    initialize_test_tracing();
    let csv_content = "ticker,sector,industry\n";

    let dataframe = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(dataframe.height(), 0);
    assert_eq!(dataframe.width(), 3);
}

#[test]
fn test_create_equity_details_dataframe_malformed_csv() {
    initialize_test_tracing();
    let csv_content =
        "ticker,sector,industry\nAAPL,Technology\nGOOGL,Technology,Internet Services,Extra\n";

    let result = create_equity_details_dataframe(csv_content.to_string());

    assert!(result.is_err());
    let error_message = result.unwrap_err().to_string();
    assert!(
        error_message.contains("Polars")
            || error_message.contains("parse")
            || error_message.contains("column"),
        "Expected parse error but got: {}",
        error_message
    );
}

#[test]
fn test_equity_bar_dataframe_parquet_roundtrip() {
    initialize_test_tracing();
    use std::io::Cursor;

    let original_bars = vec![sample_equity_bar()];
    let original_df = create_equity_bar_dataframe(original_bars.clone()).unwrap();

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut original_df.clone())
        .unwrap();

    let cursor = Cursor::new(buffer);
    let deserialized_df = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(deserialized_df.width(), 9);
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

    let ticker_series = deserialized_df.column("ticker").unwrap();
    assert_eq!(ticker_series.str().unwrap().get(0).unwrap(), "AAPL");
}

#[test]
fn test_parquet_empty_dataframe_roundtrip() {
    initialize_test_tracing();
    use std::io::Cursor;

    let empty_bars: Vec<EquityBar> = vec![];
    let original_df = create_equity_bar_dataframe(empty_bars).unwrap();

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut original_df.clone())
        .unwrap();

    let cursor = Cursor::new(buffer);
    let deserialized_df = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(deserialized_df.width(), 9);
    assert_eq!(deserialized_df.height(), 0);
}
