mod common;

use datamanager::data::{
    create_equity_bar_dataframe, create_equity_details_dataframe, create_portfolio_dataframe,
    create_predictions_dataframe, EquityBar, Portfolio, Prediction,
};
use polars::prelude::*;

#[test]
fn test_create_equity_bar_dataframe_valid_data() {
    let bars = vec![common::sample_equity_bar()];

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
    let bars = vec![common::sample_equity_bar_lowercase()];

    let df = create_equity_bar_dataframe(bars).unwrap();

    let ticker = df.column("ticker").unwrap().str().unwrap().get(0).unwrap();

    assert_eq!(ticker, "GOOGL");
}

#[test]
fn test_create_equity_bar_dataframe_mixed_case_tickers() {
    let bars = vec![
        common::sample_equity_bar(),
        common::sample_equity_bar_lowercase(),
    ];

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
    let bars: Vec<EquityBar> = vec![];

    let df = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 9);
}

#[test]
fn test_create_equity_bar_dataframe_with_none_prices() {
    let bars = vec![EquityBar {
        ticker: "TEST".to_string(),
        timestamp: 1234567890,
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
    let bars = vec![
        common::sample_equity_bar(),
        common::sample_equity_bar(),
        common::sample_equity_bar(),
    ];

    let df = create_equity_bar_dataframe(bars).unwrap();

    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 9);
}

#[test]
fn test_create_predictions_dataframe_valid_data() {
    let predictions = vec![common::sample_prediction()];

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
    let predictions = vec![Prediction {
        ticker: "aapl".to_string(),
        timestamp: 1234567890,
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
    let predictions = vec![
        common::sample_prediction_with_timestamp(1000),
        common::sample_prediction_with_timestamp(2000),
        common::sample_prediction_with_timestamp(3000),
    ];

    let df = create_predictions_dataframe(predictions).unwrap();

    assert_eq!(df.height(), 1);

    let timestamp = df.column("timestamp").unwrap().i64().unwrap().get(0);
    assert_eq!(timestamp, Some(3000));
}

#[test]
fn test_create_predictions_dataframe_keeps_most_recent_per_ticker() {
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
    let predictions: Vec<Prediction> = vec![];

    let df = create_predictions_dataframe(predictions).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 5);
}

#[test]
fn test_create_predictions_dataframe_multiple_different_tickers() {
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
    let portfolios = vec![common::sample_portfolio()];

    let df = create_portfolio_dataframe(portfolios).unwrap();

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 5);
    assert!(df.column("ticker").is_ok());
    assert!(df.column("timestamp").is_ok());
    assert!(df.column("side").is_ok());
    assert!(df.column("dollar_amount").is_ok());
    assert!(df.column("action").is_ok());
}

#[test]
fn test_create_portfolio_dataframe_uppercase_normalization() {
    let portfolios = vec![common::sample_portfolio_lowercase()];

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
    let portfolios = vec![
        Portfolio {
            ticker: "aapl".to_string(),
            timestamp: 1234567890.0,
            side: "long".to_string(),
            dollar_amount: 10000.0,
            action: "buy".to_string(),
        },
        Portfolio {
            ticker: "GOOGL".to_string(),
            timestamp: 1234567890.0,
            side: "SHORT".to_string(),
            dollar_amount: 5000.0,
            action: "Sell".to_string(),
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
    let portfolios: Vec<Portfolio> = vec![];

    let df = create_portfolio_dataframe(portfolios).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 5);
}

// Tests for create_equity_details_dataframe

#[test]
fn test_create_equity_details_dataframe_valid_csv() {
    let csv_content = "ticker,sector,industry\nAAPL,Technology,Consumer Electronics\nGOOGL,Technology,Internet Services\n";

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 3);
    assert!(df.column("ticker").is_ok());
    assert!(df.column("sector").is_ok());
    assert!(df.column("industry").is_ok());
}

#[test]
fn test_create_equity_details_dataframe_uppercase_normalization() {
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
    let csv_content =
        "ticker,sector,industry,extra_column\nAAPL,Technology,Consumer Electronics,Extra\n";

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 3);
    assert!(df.column("extra_column").is_err());
}

#[test]
fn test_create_equity_details_dataframe_missing_ticker_column() {
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
    let csv_content = "ticker,sector,industry\n";

    let df = create_equity_details_dataframe(csv_content.to_string()).unwrap();

    assert_eq!(df.height(), 0);
    assert_eq!(df.width(), 3);
}

#[test]
fn test_create_equity_details_dataframe_malformed_csv() {
    let csv_content =
        "ticker,sector,industry\nAAPL,Technology\nGOOGL,Technology,Internet Services,Extra\n";

    let result = create_equity_details_dataframe(csv_content.to_string());

    assert!(result.is_err());
}
