use crate::data::{create_equity_bar_dataframe, EquityBar, TradingDate};
use crate::database;
use crate::state::State;
use aws_sdk_s3::primitives::ByteStream;
use axum::{
    extract::{Json, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Datelike, Utc};
use internal::market::Ticker;
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
    let timestamp = DateTime::from_timestamp(result.t as i64, 0)?;
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

async fn write_equity_bars_to_s3(
    state: &State,
    trading_date: &TradingDate,
    bars: &[EquityBar],
) -> Result<(), String> {
    let mut dataframe = create_equity_bar_dataframe(bars.to_vec())
        .map_err(|error| format!("Failed to create DataFrame: {}", error))?;

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer)
        .finish(&mut dataframe)
        .map_err(|error| format!("Failed to serialize Parquet: {}", error))?;

    let date = trading_date.as_naive_date();
    let key = format!(
        "data/equity/bars/daily/year={}/month={:02}/day={:02}/data.parquet",
        date.year(),
        date.month(),
        date.day()
    );

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

pub async fn fetch_and_store(
    state: &State,
    trading_date: &TradingDate,
) -> Result<Option<usize>, String> {
    let massive_api_key = state.massive.key.clone();

    let date_str = trading_date.as_naive_date().format("%Y-%m-%d").to_string();
    let url = format!(
        "{}/v2/aggs/grouped/locale/us/market/stocks/{}",
        state.massive.base, date_str
    );

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

    Ok(Some(raw_count))
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
    use super::{parse_equity_bar, EquityBarResult};
    use chrono::Utc;

    fn make_valid_result() -> EquityBarResult {
        EquityBarResult {
            ticker: "AAPL".to_string(),
            c: Some(105.0),
            h: Some(110.0),
            l: Some(99.0),
            n: Some(1_000),
            o: Some(100.0),
            t: 1_735_689_600,
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
