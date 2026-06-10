use crate::data_manager::data::{create_equity_bar_dataframe, EquityBar, TradingDate};
use crate::data_manager::database;
use crate::data_manager::state::State;
use crate::domain::market::Ticker;
use aws_sdk_s3::primitives::ByteStream;
use axum::{
    extract::{Json, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use polars::prelude::ParquetWriter;
use serde::Deserialize;
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

    Some(EquityBar {
        ticker,
        timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        volume_weighted_average_price: result.vw,
        transactions: result.n.and_then(|n| i64::try_from(n).ok()),
        inserted_at,
    })
}

/// S3 key for a day's equity bars.
///
/// This must stay byte-for-byte aligned with the nightly pg_parquet export
/// (`export_equity_bars` in `schema.sql`) and the tide training reader so that
/// backfilled and nightly files are read uniformly from one prefix; an earlier
/// stray `daily/` segment here diverged from both and hid backfilled data from
/// training.
fn equity_bars_key(date: NaiveDate) -> String {
    format!(
        "data/equity/bars/year={}/month={:02}/day={:02}/data.parquet",
        date.year(),
        date.month(),
        date.day()
    )
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
pub async fn fetch_and_store(
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

/// Result of a historical backfill run.
#[derive(Debug, Default)]
pub struct BackfillSummary {
    pub days_processed: usize,
    pub days_skipped_weekend: usize,
    pub days_failed: usize,
    pub total_bars: usize,
}

/// Fetch one trading day's bars and write them to S3 only.
///
/// Historical backfill targets S3 (the model-training source); PostgreSQL is a
/// 90-day rolling buffer, so backfill intentionally bypasses it.
async fn backfill_one_day(state: &State, trading_date: &TradingDate) -> Result<usize, String> {
    let Some(equity_bars) = fetch_equity_bars_for_date(state, trading_date).await? else {
        return Ok(0);
    };
    write_equity_bars_to_s3(state, trading_date, &equity_bars)
        .await
        .map_err(|error| format!("Failed to write equity bars to S3: {}", error))?;
    Ok(equity_bars.len())
}

/// Backfill historical equity bars over an inclusive date range, writing each
/// trading day's bars to S3. Weekends are skipped; days with no market data
/// (holidays) count as processed with zero bars.
pub async fn backfill(
    state: &State,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<BackfillSummary, String> {
    if start > end {
        let message = format!("Backfill start date {} is after end date {}", start, end);
        warn!("{}", message);
        return Err(message);
    }

    info!(
        "Starting equity bar backfill from {} to {}",
        start.format("%Y-%m-%d"),
        end.format("%Y-%m-%d")
    );

    let mut summary = BackfillSummary::default();
    let mut date = start;
    while date <= end {
        match TradingDate::from_naive_date(date) {
            None => {
                debug!("Skipping weekend date: {}", date.format("%Y-%m-%d"));
                summary.days_skipped_weekend += 1;
            }
            Some(trading_date) => match backfill_one_day(state, &trading_date).await {
                Ok(bar_count) => {
                    summary.days_processed += 1;
                    summary.total_bars += bar_count;
                    info!(
                        "Backfilled {} bars for {}",
                        bar_count,
                        date.format("%Y-%m-%d")
                    );
                }
                Err(error) => {
                    summary.days_failed += 1;
                    warn!("Failed to backfill {}: {}", date.format("%Y-%m-%d"), error);
                }
            },
        }
        date = match date.succ_opt() {
            Some(next_date) => next_date,
            None => break,
        };
    }

    info!(
        "Backfill complete: {} days processed, {} weekends skipped, {} days failed, {} total bars",
        summary.days_processed,
        summary.days_skipped_weekend,
        summary.days_failed,
        summary.total_bars
    );

    Ok(summary)
}

pub async fn sync(
    AxumState(state): AxumState<State>,
    Json(payload): Json<DailySync>,
) -> impl IntoResponse {
    info!("Sync date: {}", payload.date);

    let Some(trading_date) = TradingDate::from_naive_date(payload.date.date_naive()) else {
        info!("Skipping weekend date: {}", payload.date.format("%Y-%m-%d"));
        return (
            StatusCode::OK,
            "Skipping weekend, no trading data available",
        )
            .into_response();
    };

    match fetch_and_store(&state, &trading_date).await {
        Ok(Some(bar_count)) => {
            let response_message = format!("Data synced: {} bars stored", bar_count);
            (StatusCode::OK, response_message).into_response()
        }
        Ok(None) => (
            StatusCode::NO_CONTENT,
            "No market data available for this date",
        )
            .into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
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
        assert_eq!(bar.ticker, "AAPL");
        assert_eq!(bar.open_price, 100.0);
        assert_eq!(bar.close_price, 105.0);
        assert_eq!(bar.volume, 2_000_000);
        let expected_timestamp = DateTime::from_timestamp_millis(1_735_689_600_000).unwrap();
        assert_eq!(bar.timestamp, expected_timestamp);
    }

    #[test]
    fn test_parse_equity_bar_normalizes_ticker() {
        let mut result = make_valid_result();
        result.ticker = "  aapl  ".to_string();
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.ticker, "AAPL");
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
        assert!(bar.volume_weighted_average_price.is_none());
        assert!(bar.transactions.is_none());
    }

    #[test]
    fn test_parse_equity_bar_rejects_class_share_ticker_with_valid_format() {
        // BRK.B is a valid Alpaca ticker format and should parse successfully.
        let mut result = make_valid_result();
        result.ticker = "BRK.B".to_string();
        let bar = parse_equity_bar(&result, Utc::now()).unwrap();
        assert_eq!(bar.ticker, "BRK.B");
    }
}
