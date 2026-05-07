mod common;

use common::initialize_test_tracing;
use data_manager::data::{
    create_closed_pair_dataframe, create_equity_bar_dataframe, create_equity_details_dataframe,
    create_performance_snapshot_dataframe, create_portfolio_dataframe,
    create_predictions_dataframe, ClosedPair, EquityBar, PerformanceSnapshot, Portfolio,
    Prediction,
};
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

#[allow(dead_code)]
fn sample_prediction() -> Prediction {
    Prediction {
        ticker: "AAPL".to_string(),
        timestamp: 1234567890000,
        quantile_10: 95.0,
        quantile_50: 100.0,
        quantile_90: 105.0,
    }
}

#[allow(dead_code)]
fn sample_prediction_with_timestamp(timestamp: i64) -> Prediction {
    Prediction {
        ticker: "AAPL".to_string(),
        timestamp,
        quantile_10: 95.0,
        quantile_50: 100.0,
        quantile_90: 105.0,
    }
}

#[allow(dead_code)]
fn sample_portfolio() -> Portfolio {
    Portfolio {
        ticker: "AAPL".to_string(),
        timestamp: 1234567890000,
        side: "long".to_string(),
        dollar_amount: 10000.0,
        action: "hold".to_string(),
        pair_id: "AAPL-GOOGL".to_string(),
        entry_price: Some(150.0),
    }
}

#[allow(dead_code)]
fn sample_portfolio_lowercase() -> Portfolio {
    Portfolio {
        ticker: "aapl".to_string(),
        timestamp: 1234567890000,
        side: "short".to_string(),
        dollar_amount: 5000.0,
        action: "sell".to_string(),
        pair_id: "aapl-googl".to_string(),
        entry_price: None,
    }
}

#[test]
fn test_create_equity_bar_dataframe_valid_data() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar()];

    let df = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 9);
    assert!(df.column("ticker").is_ok());
    assert!(df.column("timestamp").is_ok());
    assert!(df.column("open_price").is_ok());
    assert!(df.column("high_price").is_ok());
    assert!(df.column("low_price").is_ok());
    assert!(df.column("close_price").is_ok());
    assert!(df.column("volume").is_ok());
    assert!(df.column("volume_weighted_average_price").is_ok());
    assert!(df.column("transactions").is_ok());
}

#[test]
fn test_create_equity_bar_dataframe_uppercase_normalization() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar_lowercase()];

    let df = create_equity_bar_dataframe(bars).unwrap();

    let ticker = df.column("ticker").unwrap().str().unwrap().get(0).unwrap();

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

    let df = create_equity_bar_dataframe(bars).unwrap();

    let ticker = df.column("ticker").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(ticker, "ECC");
}

#[test]
fn test_create_equity_bar_dataframe_mixed_case_tickers() {
    initialize_test_tracing();
    let bars = vec![sample_equity_bar(), sample_equity_bar_lowercase()];

    let df = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(df.height(), 2);

    let tickers = df
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

    let df = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 9);
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

    let df = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(df.height(), 1);

    let close_price = df.column("close_price").unwrap();
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

    let df = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 9);
}

#[test]
fn test_create_predictions_dataframe_valid_data() {
    initialize_test_tracing();
    let predictions = vec![sample_prediction()];

    let df = create_predictions_dataframe(predictions).unwrap();

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 5);
    assert!(df.column("ticker").is_ok());
    assert!(df.column("timestamp").is_ok());
    assert!(df.column("quantile_10").is_ok());
    assert!(df.column("quantile_50").is_ok());
    assert!(df.column("quantile_90").is_ok());
}

#[test]
fn test_create_predictions_dataframe_uppercase_normalization() {
    initialize_test_tracing();
    let predictions = vec![Prediction {
        ticker: "aapl".to_string(),
        timestamp: 1234567890000,
        quantile_10: 95.0,
        quantile_50: 100.0,
        quantile_90: 105.0,
    }];

    let df = create_predictions_dataframe(predictions).unwrap();

    let ticker = df.column("ticker").unwrap().str().unwrap().get(0).unwrap();

    assert_eq!(ticker, "AAPL");
}

#[test]
fn test_create_predictions_dataframe_deduplication() {
    initialize_test_tracing();
    let predictions = vec![
        sample_prediction_with_timestamp(1000),
        sample_prediction_with_timestamp(2000),
        sample_prediction_with_timestamp(3000),
    ];

    let df = create_predictions_dataframe(predictions).unwrap();

    assert_eq!(df.height(), 1);

    let timestamp = df.column("timestamp").unwrap().i64().unwrap().get(0);
    assert_eq!(timestamp, Some(3000));
}

#[test]
fn test_create_predictions_dataframe_keeps_most_recent_per_ticker() {
    initialize_test_tracing();
    let predictions = vec![
        Prediction {
            ticker: "AAPL".to_string(),
            timestamp: 1000,
            quantile_10: 90.0,
            quantile_50: 95.0,
            quantile_90: 100.0,
        },
        Prediction {
            ticker: "AAPL".to_string(),
            timestamp: 2000,
            quantile_10: 95.0,
            quantile_50: 100.0,
            quantile_90: 105.0,
        },
        Prediction {
            ticker: "GOOGL".to_string(),
            timestamp: 1500,
            quantile_10: 1990.0,
            quantile_50: 2000.0,
            quantile_90: 2010.0,
        },
    ];

    let df = create_predictions_dataframe(predictions).unwrap();

    assert_eq!(df.height(), 2);

    let aapl_rows = df
        .clone()
        .lazy()
        .filter(polars::prelude::col("ticker").eq(polars::prelude::lit("AAPL")))
        .collect()
        .unwrap();
    assert_eq!(aapl_rows.height(), 1);

    let aapl_timestamp = aapl_rows.column("timestamp").unwrap().i64().unwrap().get(0);
    assert_eq!(aapl_timestamp, Some(2000));
}

#[test]
fn test_create_predictions_dataframe_empty_vec() {
    initialize_test_tracing();
    let predictions: Vec<Prediction> = vec![];

    let df = create_predictions_dataframe(predictions).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 5);
}

#[test]
fn test_create_predictions_dataframe_multiple_different_tickers() {
    initialize_test_tracing();
    let predictions = vec![
        Prediction {
            ticker: "AAPL".to_string(),
            timestamp: 1000,
            quantile_10: 95.0,
            quantile_50: 100.0,
            quantile_90: 105.0,
        },
        Prediction {
            ticker: "GOOGL".to_string(),
            timestamp: 1000,
            quantile_10: 1995.0,
            quantile_50: 2000.0,
            quantile_90: 2005.0,
        },
        Prediction {
            ticker: "MSFT".to_string(),
            timestamp: 1000,
            quantile_10: 295.0,
            quantile_50: 300.0,
            quantile_90: 305.0,
        },
    ];

    let df = create_predictions_dataframe(predictions).unwrap();

    assert_eq!(df.height(), 3);
}

#[test]
fn test_create_portfolio_dataframe_valid_data() {
    initialize_test_tracing();
    let portfolios = vec![sample_portfolio()];

    let df = create_portfolio_dataframe(portfolios).unwrap();

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 7);
    assert!(df.column("ticker").is_ok());
    assert!(df.column("timestamp").is_ok());
    assert!(df.column("side").is_ok());
    assert!(df.column("dollar_amount").is_ok());
    assert!(df.column("action").is_ok());
    assert!(df.column("pair_id").is_ok());
    assert!(df.column("entry_price").is_ok());
}

#[test]
fn test_create_portfolio_dataframe_uppercase_normalization() {
    initialize_test_tracing();
    let portfolios = vec![sample_portfolio_lowercase()];

    let df = create_portfolio_dataframe(portfolios).unwrap();

    let ticker = df.column("ticker").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(ticker, "AAPL");

    let side = df.column("side").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(side, "SHORT");

    let action = df.column("action").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(action, "SELL");
}

#[test]
fn test_create_portfolio_dataframe_mixed_case() {
    initialize_test_tracing();
    let portfolios = vec![
        Portfolio {
            ticker: "aapl".to_string(),
            timestamp: 1234567890000,
            side: "long".to_string(),
            dollar_amount: 10000.0,
            action: "buy".to_string(),
            pair_id: "AAPL-GOOGL".to_string(),
            entry_price: Some(150.0),
        },
        Portfolio {
            ticker: "GOOGL".to_string(),
            timestamp: 1234567890000,
            side: "SHORT".to_string(),
            dollar_amount: 5000.0,
            action: "Sell".to_string(),
            pair_id: "AAPL-GOOGL".to_string(),
            entry_price: Some(200.0),
        },
    ];

    let df = create_portfolio_dataframe(portfolios).unwrap();

    assert_eq!(df.height(), 2);

    let tickers = df
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|t| t.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(tickers, vec!["AAPL", "GOOGL"]);

    let sides = df
        .column("side")
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|s| s.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(sides, vec!["LONG", "SHORT"]);

    let actions = df
        .column("action")
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|a| a.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actions, vec!["BUY", "SELL"]);
}

#[test]
fn test_create_portfolio_dataframe_empty_vec() {
    initialize_test_tracing();
    let portfolios: Vec<Portfolio> = vec![];

    let df = create_portfolio_dataframe(portfolios).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 7);
}

// Tests for create_equity_details_dataframe

#[test]
fn test_create_equity_details_dataframe_valid_csv() {
    initialize_test_tracing();
    let csv_content = "ticker,sector,industry\nAAPL,Technology,Consumer Electronics\nGOOGL,Technology,Internet Services\n";

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 3);
    assert!(df.column("ticker").is_ok());
    assert!(df.column("sector").is_ok());
    assert!(df.column("industry").is_ok());
}

#[test]
fn test_create_equity_details_dataframe_whitespace_trimming() {
    initialize_test_tracing();
    let csv_content =
        "ticker,sector,industry\nECC           ,  Technology  ,  Consumer Electronics  \n";

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    let ticker = df.column("ticker").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(ticker, "ECC");

    let sector = df.column("sector").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(sector, "TECHNOLOGY");

    let industry = df
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

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    let ticker = df.column("ticker").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(ticker, "AAPL");

    let sector = df.column("sector").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(sector, "TECHNOLOGY");

    let industry = df
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

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(df.height(), 1);

    let sector = df.column("sector").unwrap().str().unwrap().get(0).unwrap();
    assert_eq!(sector, "NOT AVAILABLE");

    let industry = df
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

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 3);
    assert!(df.column("extra_column").is_err());
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

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 3);
}

#[test]
fn test_create_equity_details_dataframe_malformed_csv() {
    initialize_test_tracing();
    let csv_content =
        "ticker,sector,industry\nAAPL,Technology\nGOOGL,Technology,Internet Services,Extra\n";

    let result = create_equity_details_dataframe(csv_content.to_string());

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Polars") || err_msg.contains("parse") || err_msg.contains("column"),
        "Expected parse error but got: {}",
        err_msg
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
fn test_predictions_dataframe_parquet_roundtrip() {
    initialize_test_tracing();
    use std::io::Cursor;

    let original_predictions = vec![sample_prediction()];
    let original_df = create_predictions_dataframe(original_predictions.clone()).unwrap();

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut original_df.clone())
        .unwrap();

    let cursor = Cursor::new(buffer);
    let deserialized_df = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(deserialized_df.width(), 5);
    assert_eq!(deserialized_df.height(), 1);

    assert!(deserialized_df.column("ticker").is_ok());
    assert!(deserialized_df.column("timestamp").is_ok());
    assert!(deserialized_df.column("quantile_10").is_ok());
    assert!(deserialized_df.column("quantile_50").is_ok());
    assert!(deserialized_df.column("quantile_90").is_ok());

    let ticker_series = deserialized_df.column("ticker").unwrap();
    assert_eq!(ticker_series.str().unwrap().get(0).unwrap(), "AAPL");
}

#[test]
fn test_portfolio_dataframe_parquet_roundtrip() {
    initialize_test_tracing();
    use std::io::Cursor;

    let original_portfolios = vec![sample_portfolio()];
    let original_df = create_portfolio_dataframe(original_portfolios.clone()).unwrap();

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut original_df.clone())
        .unwrap();

    let cursor = Cursor::new(buffer);
    let deserialized_df = ParquetReader::new(cursor).finish().unwrap();

    assert_eq!(deserialized_df.width(), 7);
    assert_eq!(deserialized_df.height(), 1);

    assert!(deserialized_df.column("ticker").is_ok());
    assert!(deserialized_df.column("timestamp").is_ok());
    assert!(deserialized_df.column("side").is_ok());
    assert!(deserialized_df.column("dollar_amount").is_ok());
    assert!(deserialized_df.column("action").is_ok());
    assert!(deserialized_df.column("pair_id").is_ok());
    assert!(deserialized_df.column("entry_price").is_ok());

    let ticker_series = deserialized_df.column("ticker").unwrap();
    assert_eq!(ticker_series.str().unwrap().get(0).unwrap(), "AAPL");

    let pair_id_series = deserialized_df.column("pair_id").unwrap();
    assert_eq!(pair_id_series.str().unwrap().get(0).unwrap(), "AAPL-GOOGL");
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

#[test]
fn test_create_performance_snapshot_dataframe_single_row() {
    initialize_test_tracing();
    let snapshot = PerformanceSnapshot {
        timestamp: 1735689600000,
        portfolio_value: 100000.0,
        cash_balance: 25000.0,
        spy_close: 590.0,
        period_return_percent: 0.05,
        open_pair_count: 3,
    };

    let dataframe = create_performance_snapshot_dataframe(vec![snapshot]).unwrap();

    assert_eq!(dataframe.height(), 1);
    assert!(dataframe.column("timestamp").is_ok());
    assert!(dataframe.column("portfolio_value").is_ok());
    assert!(dataframe.column("cash_balance").is_ok());
    assert!(dataframe.column("spy_close").is_ok());
    assert!(dataframe.column("period_return_percent").is_ok());
    assert!(dataframe.column("open_pair_count").is_ok());

    let timestamp_value = dataframe.column("timestamp").unwrap().i64().unwrap().get(0);
    assert_eq!(timestamp_value, Some(1735689600000i64));
}

#[test]
fn test_create_performance_snapshot_dataframe_empty() {
    initialize_test_tracing();
    let snapshots: Vec<PerformanceSnapshot> = vec![];

    let dataframe = create_performance_snapshot_dataframe(snapshots).unwrap();

    assert_eq!(dataframe.height(), 0);
    assert_eq!(dataframe.width(), 6);
}

#[test]
fn test_create_closed_pair_dataframe_uppercase() {
    initialize_test_tracing();
    let closed_pair = ClosedPair {
        closed_timestamp: 1735689600000,
        pair_id: "aapl-msft".to_string(),
        long_ticker: "aapl".to_string(),
        short_ticker: "msft".to_string(),
        entry_timestamp: 1735000000000,
        dollar_amount: 10000.0,
        realized_profit_and_loss: 250.0,
        return_percent: 0.025,
        holding_days: 5,
    };

    let dataframe = create_closed_pair_dataframe(vec![closed_pair]).unwrap();

    assert_eq!(dataframe.height(), 1);

    let pair_id = dataframe
        .column("pair_id")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(pair_id, "AAPL-MSFT");

    let long_ticker = dataframe
        .column("long_ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(long_ticker, "AAPL");

    let short_ticker = dataframe
        .column("short_ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(short_ticker, "MSFT");
}

#[test]
fn test_create_closed_pair_dataframe_whitespace_trimming() {
    initialize_test_tracing();
    let closed_pair = ClosedPair {
        closed_timestamp: 1735689600000,
        pair_id: "AAPL-MSFT".to_string(),
        long_ticker: "  AAPL  ".to_string(),
        short_ticker: "MSFT   ".to_string(),
        entry_timestamp: 1735000000000,
        dollar_amount: 10000.0,
        realized_profit_and_loss: 250.0,
        return_percent: 0.025,
        holding_days: 5,
    };

    let dataframe = create_closed_pair_dataframe(vec![closed_pair]).unwrap();

    let long_ticker = dataframe
        .column("long_ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(long_ticker, "AAPL");

    let short_ticker = dataframe
        .column("short_ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(short_ticker, "MSFT");
}

#[test]
fn test_create_closed_pair_dataframe_empty() {
    initialize_test_tracing();
    let closed_pairs: Vec<ClosedPair> = vec![];

    let dataframe = create_closed_pair_dataframe(closed_pairs).unwrap();

    assert_eq!(dataframe.height(), 0);
    assert_eq!(dataframe.width(), 9);
}
