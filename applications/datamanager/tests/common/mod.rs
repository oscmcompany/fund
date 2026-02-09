use datamanager::data::{EquityBar, Portfolio, Prediction};

/// Create a sample EquityBar for testing
#[allow(dead_code)]
pub fn sample_equity_bar() -> EquityBar {
    EquityBar {
        ticker: "AAPL".to_string(),
        timestamp: 1234567890,
        open_price: Some(100.0),
        high_price: Some(105.0),
        low_price: Some(99.0),
        close_price: Some(103.0),
        volume: Some(1000000.0),
        volume_weighted_average_price: Some(102.0),
        transactions: Some(5000),
    }
}

/// Create a sample EquityBar with lowercase ticker for testing normalization
#[allow(dead_code)]
pub fn sample_equity_bar_lowercase() -> EquityBar {
    EquityBar {
        ticker: "googl".to_string(),
        timestamp: 1234567890,
        open_price: Some(2000.0),
        high_price: Some(2050.0),
        low_price: Some(1990.0),
        close_price: Some(2030.0),
        volume: Some(500000.0),
        volume_weighted_average_price: Some(2020.0),
        transactions: Some(2500),
    }
}

/// Create a sample Prediction for testing
#[allow(dead_code)]
pub fn sample_prediction() -> Prediction {
    Prediction {
        ticker: "AAPL".to_string(),
        timestamp: 1234567890,
        quantile_10: 95.0,
        quantile_50: 100.0,
        quantile_90: 105.0,
    }
}

/// Create a sample Prediction with a different timestamp
#[allow(dead_code)]
pub fn sample_prediction_with_timestamp(timestamp: i64) -> Prediction {
    Prediction {
        ticker: "AAPL".to_string(),
        timestamp,
        quantile_10: 95.0,
        quantile_50: 100.0,
        quantile_90: 105.0,
    }
}

/// Create a sample Portfolio position for testing
#[allow(dead_code)]
pub fn sample_portfolio() -> Portfolio {
    Portfolio {
        ticker: "AAPL".to_string(),
        timestamp: 1234567890.0,
        side: "long".to_string(),
        dollar_amount: 10000.0,
        action: "hold".to_string(),
    }
}

/// Create a sample Portfolio with lowercase fields for testing normalization
#[allow(dead_code)]
pub fn sample_portfolio_lowercase() -> Portfolio {
    Portfolio {
        ticker: "aapl".to_string(),
        timestamp: 1234567890.0,
        side: "short".to_string(),
        dollar_amount: 5000.0,
        action: "sell".to_string(),
    }
}
