use crate::errors::Error;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::Deserialize;
use std::io::Cursor;
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize, Clone)]
pub struct EquityBar {
    pub ticker: String,
    /// Unix timestamp in milliseconds. Massive sends bar timestamps natively
    /// in milliseconds; storing them as-is avoids any conversion loss. Alpaca
    /// uses RFC-3339 strings which also resolve to millisecond precision for
    /// OHLCV bars. Use `chrono::DateTime::timestamp_millis()` to produce this
    /// value and `DateTime::from_timestamp_millis()` to reconstruct it.
    pub timestamp: i64,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    /// Whole share units. Massive sends volume as a floating-point number but
    /// bar volumes are always whole shares; fractional values are rounded on
    /// ingestion. Stored as i64 to match the Int64 Polars/Parquet column type.
    pub volume: Option<i64>,
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
    .map_err(|e| Error::Other(format!("Failed to create equity bar DataFrame: {}", e)))?;

    debug!("Normalizing ticker column: trimming whitespace and converting to uppercase");
    let equity_bars_dataframe = equity_bars_dataframe
        .lazy()
        .with_columns([col("ticker")
            .str()
            .strip_chars(lit(NULL))
            .str()
            .to_uppercase()
            .alias("ticker")])
        .collect()
        .map_err(|e| Error::Other(format!("Failed to normalize ticker column: {}", e)))?;

    info!(
        "Created equity bar DataFrame: {} rows x {} columns",
        equity_bars_dataframe.height(),
        equity_bars_dataframe.width()
    );

    Ok(equity_bars_dataframe)
}

#[derive(Debug, Clone)]
pub struct EquityQuote {
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: i32,
    pub ask_size: i32,
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
    dataframe = dataframe.select(required_columns)?;

    debug!("Normalizing ticker, sector, and industry columns: trimming whitespace, converting to uppercase, and filling nulls");
    let equity_details_dataframe = dataframe
        .lazy()
        .with_columns([
            col("ticker")
                .str()
                .strip_chars(lit(NULL))
                .str()
                .to_uppercase()
                .alias("ticker"),
            col("sector")
                .str()
                .strip_chars(lit(NULL))
                .str()
                .to_uppercase()
                .fill_null(lit("NOT AVAILABLE"))
                .alias("sector"),
            col("industry")
                .str()
                .strip_chars(lit(NULL))
                .str()
                .to_uppercase()
                .fill_null(lit("NOT AVAILABLE"))
                .alias("industry"),
        ])
        .collect()?;

    info!(
        "Created equity details DataFrame: {} rows x {} columns",
        equity_details_dataframe.height(),
        equity_details_dataframe.width()
    );

    Ok(equity_details_dataframe)
}
