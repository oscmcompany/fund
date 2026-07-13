use crate::data::database;
use crate::data::state::State;
use crate::data::types::{create_equity_bar_dataframe, EquityBar, TradingDate};
use crate::domain::market::Ticker;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::{ParquetReader, ParquetWriter, SerReader};
use serde::Deserialize;
use std::io::Cursor;
use tracing::{debug, info, warn};

#[derive(Deserialize)]
pub struct DailySync {
    pub date: DateTime<Utc>,
}

/// Raw equity bar record as received from the Massive grouped-daily API.
/// All OHLCV fields are optional because the API may omit them for thinly
/// traded or halted instruments. The boundary morphism `parse_equity_bar`
/// converts this into a validated `EquityBar`.
#[derive(Deserialize, Debug)]
struct EquityBarResult {
    #[serde(rename = "T")]
    ticker: String,
    c: Option<f64>,
    h: Option<f64>,
    l: Option<f64>,
    n: Option<u64>,
    o: Option<f64>,
    t: u64,
    v: Option<f64>,
    vw: Option<f64>,
}

/// Minimal deserialization target for the Massive grouped-daily response.
/// Unknown fields (adjusted, queryCount, request_id, status) are ignored
/// by serde's default behaviour.
#[derive(Deserialize)]
struct MassiveResponse {
    #[serde(rename = "resultsCount", default)]
    results_count: u64,
    results: Option<Vec<EquityBarResult>>,
}

/// Boundary morphism: converts an untrusted `EquityBarResult` into a validated
/// `EquityBar`. Returns `None` for any record that fails ticker format
/// validation, has missing OHLCV fields, or has an unrepresentable volume.
fn parse_equity_bar(result: &EquityBarResult, inserted_at: DateTime<Utc>) -> Option<EquityBar> {
    let ticker = Ticker::new(&result.ticker)?;
    let timestamp = DateTime::from_timestamp_millis(i64::try_from(result.t).ok()?)?;
    let open_price = result.o?;
    let high_price = result.h?;
    let low_price = result.l?;
    let close_price = result.c?;
    let volume = result
        .v
        .filter(|v| v.is_finite() && *v >= 0.0)
        .and_then(|v| {
            let rounded = v.round();
            if rounded <= i64::MAX as f64 {
                Some(rounded as i64)
            } else {
                None
            }
        })?;

    Some(EquityBar::new(
        ticker,
        timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        result.vw,
        result.n.and_then(|n| i64::try_from(n).ok()),
        inserted_at,
    ))
}

/// S3 key for a day's equity bars.
///
/// This must stay byte-for-byte aligned with the nightly pg_parquet export
/// (`export_equity_bars` in `schema.sql`) and the tide training reader so that
/// backfilled and nightly files are read uniformly from one prefix; an earlier
/// stray `daily/` segment here diverged from both and hid backfilled data from
/// training.
fn equity_bars_key(date: NaiveDate) -> String {
    crate::common::aws::date_partitioned_key("data/equity/bars", date)
}

async fn write_equity_bars_to_s3(
    state: &State,
    trading_date: &TradingDate,
    bars: &[EquityBar],
) -> Result<(), String> {
    let mut dataframe = create_equity_bar_dataframe(bars)
        .map_err(|error| format!("Failed to create DataFrame: {}", error))?;

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut dataframe)
        .map_err(|error| format!("Failed to serialize Parquet: {}", error))?;

    let key = equity_bars_key(trading_date.as_naive_date());

    state
        .s3_client
        .put_object()
        .bucket(&state.bucket_name)
        .key(&key)
        .body(ByteStream::from(buffer))
        .send()
        .await
        .map_err(|error| format!("Failed to upload to S3: {}", error))?;

    info!("Wrote equity bars Parquet to S3: {}", key);
    Ok(())
}

/// Build the Massive grouped-daily bars URL for a date.
///
/// The configured `MASSIVE_BASE_URL` may carry a trailing slash (the
/// `development/chris.addy` secret is `https://api.massive.com/`); joining it
/// naively yields a `//` that the API answers with 404, so the base is
/// normalized here before the path is appended.
fn grouped_bars_url(base: &str, date: &str) -> String {
    format!(
        "{}/v2/aggs/grouped/locale/us/market/stocks/{}",
        base.trim_end_matches('/'),
        date
    )
}

async fn fetch_equity_bars_for_date(
    state: &State,
    trading_date: &TradingDate,
) -> Result<Option<Vec<EquityBar>>, String> {
    let massive_api_key = state.massive.key.clone();

    let date_str = trading_date.as_naive_date().format("%Y-%m-%d").to_string();
    let url = grouped_bars_url(&state.massive.base, &date_str);

    info!("Sending request to Massive API");
    let response = state
        .http_client
        .get(&url)
        .header("accept", "application/json")
        .query(&[("adjusted", "true"), ("apiKey", massive_api_key.as_str())])
        .send()
        .await
        .map_err(|err| {
            warn!(
                "Failed to send request to Massive API: {}",
                err.without_url()
            );
            "Failed to send API request".to_string()
        })?;

    info!(
        "Received response from Massive API, status: {}",
        response.status()
    );

    let text_content = response
        .error_for_status()
        .map_err(|err| {
            warn!(
                "API request failed with status code {:?}: {}",
                err.status(),
                err.without_url()
            );
            "API request failed".to_string()
        })?
        .text()
        .await
        .map_err(|err| {
            warn!("Failed to read response text: {}", err);
            "Failed to read API response".to_string()
        })?;

    info!(
        "Received response body, length: {} bytes",
        text_content.len()
    );

    let massive_response: MassiveResponse = serde_json::from_str(&text_content).map_err(|err| {
        warn!("Failed to parse JSON response: {}", err);
        let truncated: String = text_content.chars().take(500).collect();
        warn!("Raw response (first 500 chars): {}", truncated);
        "Invalid JSON response from API".to_string()
    })?;

    info!("API results count: {}", massive_response.results_count);

    let Some(results) = massive_response.results else {
        warn!("No results field in API response");
        return Ok(None);
    };

    if results.is_empty() {
        return Ok(None);
    }

    let raw_count = results.len();
    let inserted_at = Utc::now();

    let equity_bars: Vec<EquityBar> = results
        .iter()
        .filter_map(|result| parse_equity_bar(result, inserted_at))
        .collect();

    debug!(
        "Converted {}/{} results to valid equity bars",
        equity_bars.len(),
        raw_count
    );

    Ok(Some(equity_bars))
}

/// Fetch a day's grouped-daily bars and persist them to PostgreSQL (when a pool
/// is configured) and S3. Used by the on-demand `sync` handler.
pub async fn fetch_and_store_equity_bars(
    state: &State,
    trading_date: &TradingDate,
) -> Result<Option<usize>, String> {
    let Some(equity_bars) = fetch_equity_bars_for_date(state, trading_date).await? else {
        return Ok(None);
    };

    if let Some(pool) = state.database.pool() {
        database::insert_equity_bars(pool, &equity_bars)
            .await
            .map_err(|error| {
                warn!("Failed to write equity bars to PostgreSQL: {}", error);
                format!("Failed to insert equity bars: {}", error)
            })?;
    }

    if let Err(error) = write_equity_bars_to_s3(state, trading_date, &equity_bars).await {
        warn!("Failed to write equity bars to S3: {}", error);
    }

    Ok(Some(equity_bars.len()))
}

/// Result of a seed (or backfill) run over a date range.
#[derive(Debug, Default)]
pub struct SeedSummary {
    pub days_processed: usize,
    pub days_skipped_weekend: usize,
    pub days_failed: usize,
    pub total_bars: usize,
}

/// Where equity bar data is read from.
#[derive(Debug)]
pub enum SeedSource {
    /// Fetch from the Massive grouped-daily API.
    Massive,
    /// Read existing Hive-partitioned Parquet files from S3.
    S3,
}

/// Where equity bar data is written to.
#[derive(Debug)]
pub enum SeedTarget {
    /// Write Hive-partitioned Parquet to S3.
    S3,
    /// Insert into the PostgreSQL `equity_bars` table.
    PostgreSQL,
    /// Write to both S3 and PostgreSQL.
    All,
}

/// Read one day's equity bar Parquet from S3 and parse rows into validated
/// `EquityBar` values. Uses the same `Ticker::new` boundary validation as the
/// Massive API ingest path so both sources produce identical domain objects.
async fn read_equity_bars_from_s3(
    state: &State,
    date: NaiveDate,
) -> Result<Option<Vec<EquityBar>>, String> {
    let key = equity_bars_key(date);

    let response = match state
        .s3_client
        .get_object()
        .bucket(&state.bucket_name)
        .key(&key)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let error_string = error.to_string();
            if error_string.contains("NoSuchKey") || error_string.contains("not found") {
                debug!("No S3 object for {}: {}", key, error_string);
                return Ok(None);
            }
            return Err(format!(
                "Failed to read S3 object {}: {}",
                key, error_string
            ));
        }
    };

    let bytes = response
        .body
        .collect()
        .await
        .map_err(|error| format!("Failed to read S3 body for {}: {}", key, error))?
        .into_bytes();

    let dataframe = ParquetReader::new(Cursor::new(bytes))
        .finish()
        .map_err(|error| format!("Failed to parse Parquet from {}: {}", key, error))?;

    if dataframe.height() == 0 {
        return Ok(None);
    }

    let inserted_at = Utc::now();
    let mut bars = Vec::with_capacity(dataframe.height());
    let mut rejected = 0usize;

    let tickers = dataframe
        .column("ticker")
        .map_err(|error| format!("Missing ticker column: {}", error))?
        .str()
        .map_err(|error| format!("ticker column is not string: {}", error))?;
    let timestamps = dataframe
        .column("timestamp")
        .map_err(|error| format!("Missing timestamp column: {}", error))?
        .i64()
        .map_err(|error| format!("timestamp column is not i64: {}", error))?;
    let open_prices = dataframe
        .column("open_price")
        .map_err(|error| format!("Missing open_price column: {}", error))?
        .f64()
        .map_err(|error| format!("open_price column is not f64: {}", error))?;
    let high_prices = dataframe
        .column("high_price")
        .map_err(|error| format!("Missing high_price column: {}", error))?
        .f64()
        .map_err(|error| format!("high_price column is not f64: {}", error))?;
    let low_prices = dataframe
        .column("low_price")
        .map_err(|error| format!("Missing low_price column: {}", error))?
        .f64()
        .map_err(|error| format!("low_price column is not f64: {}", error))?;
    let close_prices = dataframe
        .column("close_price")
        .map_err(|error| format!("Missing close_price column: {}", error))?
        .f64()
        .map_err(|error| format!("close_price column is not f64: {}", error))?;
    let volumes = dataframe
        .column("volume")
        .map_err(|error| format!("Missing volume column: {}", error))?
        .i64()
        .map_err(|error| format!("volume column is not i64: {}", error))?;
    let volume_weighted_average_prices = dataframe
        .column("volume_weighted_average_price")
        .map_err(|error| format!("Missing volume_weighted_average_price column: {}", error))?
        .f64()
        .map_err(|error| format!("volume_weighted_average_price column is not f64: {}", error))?;
    let transactions = dataframe
        .column("transactions")
        .map_err(|error| format!("Missing transactions column: {}", error))?
        .i64()
        .map_err(|error| format!("transactions column is not i64: {}", error))?;

    for row_index in 0..dataframe.height() {
        let ticker = match tickers.get(row_index).and_then(Ticker::new) {
            Some(ticker) => ticker,
            None => {
                rejected += 1;
                continue;
            }
        };
        let timestamp = match timestamps
            .get(row_index)
            .and_then(DateTime::from_timestamp_millis)
        {
            Some(timestamp) => timestamp,
            None => {
                rejected += 1;
                continue;
            }
        };
        let (Some(open_price), Some(high_price), Some(low_price), Some(close_price)) = (
            open_prices.get(row_index),
            high_prices.get(row_index),
            low_prices.get(row_index),
            close_prices.get(row_index),
        ) else {
            rejected += 1;
            continue;
        };
        let Some(volume) = volumes.get(row_index) else {
            rejected += 1;
            continue;
        };

        bars.push(EquityBar::new(
            ticker,
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            volume_weighted_average_prices.get(row_index),
            transactions.get(row_index),
            inserted_at,
        ));
    }

    if rejected > 0 {
        warn!(
            "Rejected {} of {} rows from S3 Parquet {}",
            rejected,
            dataframe.height(),
            key
        );
    }

    if bars.is_empty() {
        Ok(None)
    } else {
        Ok(Some(bars))
    }
}

/// Seed one trading day's equity bars from the specified source to the
/// specified target.
async fn seed_one_day(
    state: &State,
    trading_date: &TradingDate,
    source: &SeedSource,
    target: &SeedTarget,
) -> Result<usize, String> {
    let equity_bars = match source {
        SeedSource::Massive => fetch_equity_bars_for_date(state, trading_date).await?,
        SeedSource::S3 => read_equity_bars_from_s3(state, trading_date.as_naive_date()).await?,
    };

    let Some(equity_bars) = equity_bars else {
        return Ok(0);
    };

    let bar_count = equity_bars.len();

    match target {
        SeedTarget::S3 => {
            write_equity_bars_to_s3(state, trading_date, &equity_bars).await?;
        }
        SeedTarget::PostgreSQL => {
            let pool = state
                .database
                .pool()
                .ok_or("PostgreSQL not configured but target is postgresql")?;
            database::insert_equity_bars(pool, &equity_bars)
                .await
                .map_err(|error| format!("Failed to insert equity bars: {}", error))?;
        }
        SeedTarget::All => {
            let pool = state
                .database
                .pool()
                .ok_or("PostgreSQL not configured but target is all")?;
            database::insert_equity_bars(pool, &equity_bars)
                .await
                .map_err(|error| format!("Failed to insert equity bars: {}", error))?;
            write_equity_bars_to_s3(state, trading_date, &equity_bars).await?;
        }
    }

    Ok(bar_count)
}

/// Seed equity bars over an inclusive date range from the given source to
/// the given target. Weekends are skipped; days with no data count as
/// processed with zero bars.
pub async fn seed(
    state: &State,
    start: NaiveDate,
    end: NaiveDate,
    source: SeedSource,
    target: SeedTarget,
) -> Result<SeedSummary, String> {
    if start > end {
        let message = format!("Seed start date {} is after end date {}", start, end);
        warn!("{}", message);
        return Err(message);
    }

    info!(
        "Starting equity bar seed from {} to {}",
        start.format("%Y-%m-%d"),
        end.format("%Y-%m-%d")
    );

    let mut summary = SeedSummary::default();
    let mut date = start;
    while date <= end {
        match TradingDate::from_naive_date(date) {
            None => {
                debug!("Skipping weekend date: {}", date.format("%Y-%m-%d"));
                summary.days_skipped_weekend += 1;
            }
            Some(trading_date) => {
                match seed_one_day(state, &trading_date, &source, &target).await {
                    Ok(bar_count) => {
                        summary.days_processed += 1;
                        summary.total_bars += bar_count;
                        info!("Seeded {} bars for {}", bar_count, date.format("%Y-%m-%d"));
                    }
                    Err(error) => {
                        summary.days_failed += 1;
                        warn!("Failed to seed {}: {}", date.format("%Y-%m-%d"), error);
                    }
                }
            }
        }
        date = match date.succ_opt() {
            Some(next_date) => next_date,
            None => break,
        };
    }

    info!(
        "Seed complete: {} days processed, {} weekends skipped, {} days failed, {} total bars",
        summary.days_processed,
        summary.days_skipped_weekend,
        summary.days_failed,
        summary.total_bars
    );

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::{equity_bars_key, grouped_bars_url, parse_equity_bar, EquityBarResult};
    use chrono::{DateTime, NaiveDate, Utc};

    #[test]
    fn test_equity_bars_key_matches_export_convention() {
        // Must match `export_equity_bars` in schema.sql and the tide reader:
        // `data/equity/bars/year=YYYY/month=MM/day=DD/data.parquet` (no `daily/`).
        let date = NaiveDate::from_ymd_opt(2026, 6, 5).unwrap();
        assert_eq!(
            equity_bars_key(date),
            "data/equity/bars/year=2026/month=06/day=05/data.parquet"
        );
    }

    #[test]
    fn test_grouped_bars_url_trims_trailing_slash() {
        assert_eq!(
            grouped_bars_url("https://api.massive.com/", "2026-06-05"),
            "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2026-06-05"
        );
    }

    #[test]
    fn test_grouped_bars_url_without_trailing_slash() {
        assert_eq!(
            grouped_bars_url("https://api.massive.com", "2026-06-05"),
            "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2026-06-05"
        );
    }

    fn make_valid_result() -> EquityBarResult {
        EquityBarResult {
            ticker: "AAPL".to_string(),
            c: Some(105.0),
            h: Some(110.0),
            l: Some(99.0),
            n: Some(1_000),
            o: Some(100.0),
            t: 1_735_689_600_000,
            v: Some(2_000_000.0),
            vw: Some(104.0),
        }
    }

    #[test]
    fn test_parse_equity_bar_valid() {
        let result = make_valid_result();
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.ticker(), "AAPL");
        assert_eq!(bar.open_price(), 100.0);
        assert_eq!(bar.close_price(), 105.0);
        assert_eq!(bar.volume(), 2_000_000);
        let expected_timestamp = DateTime::from_timestamp_millis(1_735_689_600_000).unwrap();
        assert_eq!(bar.timestamp(), expected_timestamp);
    }

    #[test]
    fn test_parse_equity_bar_normalizes_ticker() {
        let mut result = make_valid_result();
        result.ticker = "  aapl  ".to_string();
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.ticker(), "AAPL");
    }

    #[test]
    fn test_parse_equity_bar_rejects_invalid_ticker() {
        let mut result = make_valid_result();
        result.ticker = "TOOLONG".to_string();
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_missing_open_price() {
        let mut result = make_valid_result();
        result.o = None;
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_missing_high_price() {
        let mut result = make_valid_result();
        result.h = None;
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_missing_low_price() {
        let mut result = make_valid_result();
        result.l = None;
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_missing_close_price() {
        let mut result = make_valid_result();
        result.c = None;
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_nan_volume() {
        let mut result = make_valid_result();
        result.v = Some(f64::NAN);
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_negative_volume() {
        let mut result = make_valid_result();
        result.v = Some(-1.0);
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_volume_overflow() {
        let mut result = make_valid_result();
        result.v = Some(f64::MAX);
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_optional_fields_can_be_none() {
        let mut result = make_valid_result();
        result.vw = None;
        result.n = None;
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert!(bar.volume_weighted_average_price().is_none());
        assert!(bar.transactions().is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_class_share_ticker_with_valid_format() {
        // BRK.B is a valid Alpaca ticker format and should parse successfully.
        let mut result = make_valid_result();
        result.ticker = "BRK.B".to_string();
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.ticker(), "BRK.B");
    }

    #[test]
    fn test_parse_equity_bar_rejects_infinite_volume() {
        let mut result = make_valid_result();
        result.v = Some(f64::INFINITY);
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_missing_volume() {
        let mut result = make_valid_result();
        result.v = None;
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_volume_zero_is_accepted() {
        // Zero volume is valid (e.g., a halted instrument that still reports OHLC).
        let mut result = make_valid_result();
        result.v = Some(0.0);
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.volume(), 0);
    }

    #[test]
    fn test_parse_equity_bar_volume_fractional_rounds() {
        // Fractional volumes from the API are rounded to the nearest integer.
        let mut result = make_valid_result();
        result.v = Some(1_000_500.7);
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.volume(), 1_000_501);
    }

    #[test]
    fn test_parse_equity_bar_transactions_overflow_u64_becomes_none() {
        // n values that exceed i64::MAX cannot be represented and must be discarded
        // (the transactions field becomes None rather than corrupting the value).
        let mut result = make_valid_result();
        result.n = Some(u64::MAX);
        // Transactions should become None because i64::try_from(u64::MAX) fails,
        // but the bar itself is still valid.
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert!(bar.transactions().is_none());
    }

    #[test]
    fn test_equity_bars_key_sunday() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 7).unwrap(); // Sunday
        let key = equity_bars_key(date);
        assert_eq!(
            key,
            "data/equity/bars/year=2026/month=06/day=07/data.parquet"
        );
    }

    #[test]
    fn test_equity_bars_key_single_digit_month_and_day() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 3).unwrap();
        let key = equity_bars_key(date);
        assert_eq!(
            key,
            "data/equity/bars/year=2026/month=01/day=03/data.parquet"
        );
    }

    #[test]
    fn test_grouped_bars_url_multiple_trailing_slashes() {
        // Only the trailing slash(es) on the base are stripped; the path is appended once.
        assert_eq!(
            grouped_bars_url("https://api.massive.com//", "2026-01-02"),
            "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2026-01-02"
        );
    }

    #[test]
    fn test_seed_summary_default_is_all_zeros() {
        use super::SeedSummary;
        let summary = SeedSummary::default();
        assert_eq!(summary.days_processed, 0);
        assert_eq!(summary.days_skipped_weekend, 0);
        assert_eq!(summary.days_failed, 0);
        assert_eq!(summary.total_bars, 0);
    }

    #[test]
    fn test_parse_equity_bar_rejects_negative_infinity_volume() {
        let mut result = make_valid_result();
        result.v = Some(f64::NEG_INFINITY);
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_volume_exactly_at_i64_max_boundary() {
        // i64::MAX as f64 is representable (it rounds to 2^63 = 9.223372036854776e18).
        // The guard checks `rounded <= i64::MAX as f64`, which is exactly equal,
        // so the cast succeeds (Rust saturates the f64 cast to i64::MAX).
        // The bar must be accepted with a saturated volume value.
        let mut result = make_valid_result();
        result.v = Some(i64::MAX as f64);
        let bar = parse_equity_bar(&result, Utc::now());
        assert!(bar.is_some());
    }

    #[test]
    fn test_parse_equity_bar_timestamp_overflow_u64_to_i64_fails() {
        // A timestamp u64 value larger than i64::MAX cannot be converted
        // via i64::try_from and must cause the bar to be dropped.
        let mut result = make_valid_result();
        result.t = u64::MAX;
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_timestamp_out_of_range_for_datetime() {
        // A u64 timestamp that fits in i64 but is outside the valid DateTime
        // range causes from_timestamp_millis to return None, rejecting the bar.
        // (i64::MAX / 2) milliseconds is roughly 4.6 × 10^15 seconds ≈ far future
        // beyond chrono's supported range.
        let mut result = make_valid_result();
        result.t = (i64::MAX as u64) / 2;
        // from_timestamp_millis returns None for timestamps outside chrono's range
        // so the bar must be dropped.
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_volume_small_positive_rounds_to_zero() {
        // A very small positive volume (0 < v < 0.5) rounds to 0, which passes
        // the >= 0 filter and should produce a bar with volume 0.
        let mut result = make_valid_result();
        result.v = Some(0.3);
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.volume(), 0);
    }

    #[test]
    fn test_parse_equity_bar_transactions_zero_is_accepted() {
        let mut result = make_valid_result();
        result.n = Some(0);
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.transactions(), Some(0));
    }

    #[test]
    fn test_grouped_bars_url_empty_date_string() {
        // Date is caller-controlled; verify the template is still applied.
        let url = grouped_bars_url("https://api.massive.com", "");
        assert_eq!(
            url,
            "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/"
        );
    }

    #[test]
    fn test_equity_bars_key_year_boundary() {
        let date = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        let key = equity_bars_key(date);
        assert_eq!(
            key,
            "data/equity/bars/year=2025/month=12/day=31/data.parquet"
        );
    }

    #[test]
    fn test_parse_equity_bar_empty_ticker_is_rejected() {
        let mut result = make_valid_result();
        result.ticker = String::new();
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_parse_equity_bar_whitespace_only_ticker_is_rejected() {
        let mut result = make_valid_result();
        result.ticker = "   ".to_string();
        assert!(parse_equity_bar(&result, Utc::now()).is_none());
    }

    #[test]
    fn test_seed_summary_debug_is_implemented() {
        use super::SeedSummary;
        let summary = SeedSummary {
            days_processed: 5,
            days_skipped_weekend: 2,
            days_failed: 1,
            total_bars: 1000,
        };
        let debug_str = format!("{:?}", summary);
        assert!(debug_str.contains("SeedSummary"));
        assert!(debug_str.contains("days_processed"));
        assert!(debug_str.contains("total_bars"));
    }
}
