use crate::data_manager::errors::Error;
use chrono::{Datelike, NaiveDate, Weekday};
use polars::prelude::*;
use tracing::{debug, info};

pub use crate::domain::market::{EquityBar, EquityDetails, EquityQuote, Ticker};

/// A validated US market trading date (Monday through Friday).
///
/// The private field prevents construction without going through
/// [`TradingDate::from_naive_date`], which rejects weekend dates.
/// A `TradingDate` in scope is proof the date is a weekday.
#[derive(Debug, Clone, Copy)]
pub struct TradingDate(NaiveDate);

impl TradingDate {
    /// Constructs a `TradingDate` from a `NaiveDate`.
    ///
    /// Returns `None` if the date falls on a Saturday or Sunday.
    pub fn from_naive_date(date: NaiveDate) -> Option<Self> {
        match date.weekday() {
            Weekday::Sat | Weekday::Sun => None,
            _ => Some(Self(date)),
        }
    }

    /// Returns the underlying `NaiveDate`.
    pub fn as_naive_date(&self) -> NaiveDate {
        self.0
    }
}

pub fn create_equity_bar_dataframe(equity_bars_rows: &[EquityBar]) -> Result<DataFrame, Error> {
    debug!(
        "Creating equity bar DataFrame from {} rows",
        equity_bars_rows.len()
    );

    // Ticker values are already normalized (trimmed and uppercased) by Ticker::new.
    let equity_bars_dataframe = df!(
        "ticker" => equity_bars_rows.iter().map(|b| b.ticker.as_str()).collect::<Vec<_>>(),
        "timestamp" => equity_bars_rows.iter().map(|b| b.timestamp.timestamp_millis()).collect::<Vec<_>>(),
        "open_price" => equity_bars_rows.iter().map(|b| b.open_price).collect::<Vec<f64>>(),
        "high_price" => equity_bars_rows.iter().map(|b| b.high_price).collect::<Vec<f64>>(),
        "low_price" => equity_bars_rows.iter().map(|b| b.low_price).collect::<Vec<f64>>(),
        "close_price" => equity_bars_rows.iter().map(|b| b.close_price).collect::<Vec<f64>>(),
        "volume" => equity_bars_rows.iter().map(|b| b.volume).collect::<Vec<i64>>(),
        "volume_weighted_average_price" => equity_bars_rows.iter().map(|b| b.volume_weighted_average_price).collect::<Vec<_>>(),
        "transactions" => equity_bars_rows.iter().map(|b| b.transactions).collect::<Vec<_>>(),
        // `inserted_at` is deliberately excluded: the S3 parquet schema is the
        // equity_bars_schema pandera contract (9 columns, Int64 timestamp),
        // which the nightly pg_parquet export (export_equity_bars in schema.sql)
        // also targets. Including inserted_at made backfilled files 10 columns
        // wide and broke the tide training reader's per-day concat. inserted_at
        // remains on the EquityBar row for the PostgreSQL insert path only.
    )
    .map_err(|e| Error::Other(format!("Failed to create equity bar DataFrame: {}", e)))?;

    info!(
        "Created equity bar DataFrame: {} rows x {} columns",
        equity_bars_dataframe.height(),
        equity_bars_dataframe.width()
    );

    Ok(equity_bars_dataframe)
}

#[cfg(test)]
mod tests {
    use super::{create_equity_bar_dataframe, EquityBar, Ticker, TradingDate};
    use chrono::{NaiveDate, Utc};
    use polars::prelude::DataType;

    fn sample_bar() -> EquityBar {
        EquityBar {
            ticker: Ticker::new("AAPL").unwrap(),
            timestamp: Utc::now(),
            open_price: 100.0,
            high_price: 110.0,
            low_price: 99.0,
            close_price: 105.0,
            volume: 2_000_000,
            volume_weighted_average_price: Some(104.0),
            transactions: Some(1_000),
            inserted_at: Utc::now(),
        }
    }

    #[test]
    fn test_equity_bar_dataframe_matches_pandera_contract() {
        // The S3 parquet schema is the equity_bars_schema pandera contract:
        // exactly these 9 columns in order, Int64 timestamp/transactions, and
        // NO inserted_at. The nightly pg_parquet export targets the same shape,
        // so backfilled and nightly files concat uniformly in the tide reader.
        let dataframe = create_equity_bar_dataframe(&[sample_bar()]).unwrap();
        assert_eq!(
            dataframe.get_column_names_str(),
            [
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
            dataframe.column("transactions").unwrap().dtype(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_trading_date_accepts_monday() {
        let monday = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        assert!(TradingDate::from_naive_date(monday).is_some());
    }

    #[test]
    fn test_trading_date_accepts_friday() {
        let friday = NaiveDate::from_ymd_opt(2026, 5, 1).unwrap();
        assert!(TradingDate::from_naive_date(friday).is_some());
    }

    #[test]
    fn test_trading_date_rejects_saturday() {
        let saturday = NaiveDate::from_ymd_opt(2026, 5, 2).unwrap();
        assert!(TradingDate::from_naive_date(saturday).is_none());
    }

    #[test]
    fn test_trading_date_rejects_sunday() {
        let sunday = NaiveDate::from_ymd_opt(2026, 5, 3).unwrap();
        assert!(TradingDate::from_naive_date(sunday).is_none());
    }

    #[test]
    fn test_trading_date_as_naive_date_roundtrips() {
        let date = NaiveDate::from_ymd_opt(2026, 4, 28).unwrap();
        let trading_date = TradingDate::from_naive_date(date).unwrap();
        assert_eq!(trading_date.as_naive_date(), date);
    }
}
