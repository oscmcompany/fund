use crate::errors::Error;
use polars::prelude::*;
use serde::Deserialize;
use std::io::Cursor;
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize)]
pub struct EquityBar {
    pub ticker: String,
    pub timestamp: i64,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    pub volume: Option<f64>,
    pub volume_weighted_average_price: Option<f64>,
    pub transactions: Option<u64>,
}

pub fn create_equity_bar_dataframe(equity_bars_rows: Vec<EquityBar>) -> Result<DataFrame, Error> {
    debug!(
        "Creating equity bar DataFrame from {} rows",
        equity_bars_rows.len()
    );

    let equity_bars_dataframe = df!(
        "ticker" => equity_bars_rows.iter().map(|b| b.ticker.as_str()).collect::<Vec<_>>(),
        "timestamp" => equity_bars_rows.iter().map(|b| b.timestamp).collect::<Vec<_>>(),
        "open_price" => equity_bars_rows.iter().map(|b| b.open_price).collect::<Vec<_>>(),
        "high_price" => equity_bars_rows.iter().map(|b| b.high_price).collect::<Vec<_>>(),
        "low_price" => equity_bars_rows.iter().map(|b| b.low_price).collect::<Vec<_>>(),
        "close_price" => equity_bars_rows.iter().map(|b| b.close_price).collect::<Vec<_>>(),
        "volume" => equity_bars_rows.iter().map(|b| b.volume).collect::<Vec<_>>(),
        "volume_weighted_average_price" => equity_bars_rows.iter().map(|b| b.volume_weighted_average_price).collect::<Vec<_>>(),
        "transactions" => equity_bars_rows.iter().map(|b| b.transactions).collect::<Vec<_>>(),
    )
    .map_err(|e| {
        warn!("Failed to create equity bar DataFrame: {}", e);
        Error::Other(format!("Failed to create DataFrame: {}", e))
    })?;

    debug!("Normalizing ticker column to uppercase");
    let equity_bars_dataframe = equity_bars_dataframe
        .lazy()
        .with_columns([col("ticker").str().to_uppercase().alias("ticker")])
        .collect()?;

    info!(
        "Created equity bar DataFrame: {} rows x {} columns",
        equity_bars_dataframe.height(),
        equity_bars_dataframe.width()
    );

    Ok(equity_bars_dataframe)
}

#[derive(Debug, Deserialize)]
pub struct Prediction {
    pub ticker: String,
    pub timestamp: i64,
    pub quantile_10: f64,
    pub quantile_50: f64,
    pub quantile_90: f64,
}

pub fn create_predictions_dataframe(prediction_rows: Vec<Prediction>) -> Result<DataFrame, Error> {
    debug!(
        "Creating predictions DataFrame from {} rows",
        prediction_rows.len()
    );

    let prediction_dataframe = df!(
        "ticker" => prediction_rows.iter().map(|p| p.ticker.as_str()).collect::<Vec<_>>(),
        "timestamp" => prediction_rows.iter().map(|p| p.timestamp).collect::<Vec<_>>(),
        "quantile_10" => prediction_rows.iter().map(|p| p.quantile_10).collect::<Vec<_>>(),
        "quantile_50" => prediction_rows.iter().map(|p| p.quantile_50).collect::<Vec<_>>(),
        "quantile_90" => prediction_rows.iter().map(|p| p.quantile_90).collect::<Vec<_>>(),
    )
    .map_err(|e| {
        warn!("Failed to create predictions DataFrame: {}", e);
        Error::Other(format!("Failed to create DataFrame: {}", e))
    })?;

    debug!("Normalizing ticker column to uppercase");
    let unfiltered_prediction_dataframe = prediction_dataframe
        .lazy()
        .with_columns([col("ticker").str().to_uppercase().alias("ticker")])
        .collect()?;

    debug!(
        "Unfiltered predictions DataFrame has {} rows",
        unfiltered_prediction_dataframe.height()
    );

    // filtering necessary due to potentially overlapping tickers in predictions parquet files
    debug!("Filtering to keep only most recent prediction per ticker");
    let filtered_prediction_dataframe = unfiltered_prediction_dataframe
        .lazy()
        .with_columns([col("timestamp")
            .max()
            .over([col("ticker")])
            .alias("max_timestamp")])
        .filter(col("timestamp").eq(col("max_timestamp")))
        .select([
            col("ticker"),
            col("timestamp"),
            col("quantile_10"),
            col("quantile_50"),
            col("quantile_90"),
        ])
        .collect()?;

    info!(
        "Created predictions DataFrame: {} rows x {} columns (filtered from {} input rows)",
        filtered_prediction_dataframe.height(),
        filtered_prediction_dataframe.width(),
        prediction_rows.len()
    );

    Ok(filtered_prediction_dataframe)
}

#[derive(Debug, Deserialize)]
pub struct Portfolio {
    pub ticker: String,
    pub timestamp: f64,
    pub side: String,
    pub dollar_amount: f64,
    pub action: String,
}

pub fn create_portfolio_dataframe(portfolio_rows: Vec<Portfolio>) -> Result<DataFrame, Error> {
    debug!(
        "Creating portfolio DataFrame from {} rows",
        portfolio_rows.len()
    );

    let portfolio_dataframe = df!(
        "ticker" => portfolio_rows.iter().map(|p| p.ticker.as_str()).collect::<Vec<&str>>(),
        "timestamp" => portfolio_rows.iter().map(|p| p.timestamp).collect::<Vec<f64>>(),
        "side" => portfolio_rows.iter().map(|p| p.side.as_str()).collect::<Vec<&str>>(),
        "dollar_amount" => portfolio_rows.iter().map(|p| p.dollar_amount).collect::<Vec<f64>>(),
        "action" => portfolio_rows.iter().map(|p| p.action.as_str()).collect::<Vec<&str>>(),
    )
    .map_err(|e| {
        warn!("Failed to create portfolio DataFrame: {}", e);
        Error::Other(format!("Failed to create DataFrame: {}", e))
    })?;

    debug!("Normalizing ticker, side, and action columns to uppercase");
    let portfolio_dataframe = portfolio_dataframe
        .lazy()
        .with_columns([col("ticker").str().to_uppercase().alias("ticker")])
        .with_columns([col("side").str().to_uppercase().alias("side")])
        .with_columns([col("action").str().to_uppercase().alias("action")])
        .collect()?;

    info!(
        "Created portfolio DataFrame: {} rows x {} columns",
        portfolio_dataframe.height(),
        portfolio_dataframe.width()
    );

    Ok(portfolio_dataframe)
}

pub fn create_equity_details_dataframe(csv_content: String) -> Result<DataFrame, Error> {
    debug!(
        "Creating equity details DataFrame from CSV ({} bytes)",
        csv_content.len()
    );

    let cursor = Cursor::new(csv_content.as_bytes());
    let mut dataframe = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()
        .map_err(|e| {
            warn!("Failed to parse CSV: {}", e);
            Error::Other(format!("Failed to parse CSV: {}", e))
        })?;

    debug!(
        "Parsed CSV into DataFrame: {} rows x {} columns",
        dataframe.height(),
        dataframe.width()
    );

    let required_columns = vec!["ticker", "sector", "industry"];
    let column_names = dataframe.get_column_names();

    debug!("Available columns: {:?}", column_names);
    debug!("Required columns: {:?}", required_columns);

    for column in &required_columns {
        if !column_names.iter().any(|c| c.as_str() == *column) {
            let message = format!("CSV missing required column: {}", column);
            warn!("{}", message);
            return Err(Error::Other(message));
        }
    }

    debug!("All required columns present, selecting subset");
    dataframe = dataframe.select(required_columns).map_err(|e| {
        warn!("Failed to select columns: {}", e);
        Error::Other(format!("Failed to select columns: {}", e))
    })?;

    debug!("Normalizing ticker, sector, and industry columns to uppercase and filling nulls");
    let equity_details_dataframe = dataframe
        .lazy()
        .with_columns([
            col("ticker").str().to_uppercase().alias("ticker"),
            col("sector")
                .str()
                .to_uppercase()
                .fill_null(lit("NOT AVAILABLE"))
                .alias("sector"),
            col("industry")
                .str()
                .to_uppercase()
                .fill_null(lit("NOT AVAILABLE"))
                .alias("industry"),
        ])
        .collect()
        .map_err(|e| {
            warn!("Failed to transform columns: {}", e);
            Error::Other(format!("Failed to transform columns: {}", e))
        })?;

    info!(
        "Created equity details DataFrame: {} rows x {} columns",
        equity_details_dataframe.height(),
        equity_details_dataframe.width()
    );

    Ok(equity_details_dataframe)
}
