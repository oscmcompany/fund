use crate::data_manager::data::EquityBar;
use crate::data_manager::database;
use crate::data_manager::state::State;
use crate::data_manager::storage::write_equity_bars_to_s3;
use axum::{
    body::Body,
    extract::{Json, Query, State as AxumState},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc, Weekday};
use chrono_tz::US::Eastern;
use polars::prelude::*;
use serde::Deserialize;
use tracing::{debug, info, warn};

#[derive(Deserialize)]
pub struct DailySync {
    pub date: DateTime<Utc>,
}

#[derive(Deserialize, Debug)]
struct BarResult {
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

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct MassiveResponse {
    adjusted: bool,
    #[serde(rename = "queryCount")]
    query_count: u64,
    request_id: String,
    #[serde(rename = "resultsCount")]
    results_count: u64,
    status: String,
    results: Option<Vec<BarResult>>,
}

/// Builds the Massive grouped-daily-bars URL for a date, tolerating a trailing
/// slash on the configured base URL (a trailing slash would otherwise produce a
/// double slash and a 404 from the API).
fn grouped_bars_url(base: &str, date: &str) -> String {
    format!(
        "{}/v2/aggs/grouped/locale/us/market/stocks/{}",
        base.trim_end_matches('/'),
        date
    )
}

/// Fetches grouped daily equity bars for `date` from the Massive API.
///
/// Returns `Ok(None)` when the API response contains no `results` field (for
/// example a market holiday with no trading data), and `Ok(Some(bars))`
/// otherwise. This performs no persistence; callers decide where to store the
/// returned bars (Postgres, S3, or both).
pub async fn fetch_equity_bars(
    state: &State,
    date: &DateTime<Utc>,
) -> Result<Option<Vec<EquityBar>>, String> {
    let massive_api_key = state.massive.key.clone();

    let date_str = date.with_timezone(&Eastern).format("%Y-%m-%d").to_string();
    let url = grouped_bars_url(&state.massive.base, &date_str);

    info!("url: {}", url);
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
    info!("Parsing JSON response");

    let json_content: serde_json::Value = serde_json::from_str(&text_content).map_err(|err| {
        warn!("Failed to parse JSON response: {}", err);
        let truncated: String = text_content.chars().take(500).collect();
        warn!("Raw response (first 500 chars): {}", truncated);
        "Invalid JSON response from API".to_string()
    })?;

    debug!("JSON parsed successfully");

    if let Some(status) = json_content.get("status") {
        info!("API response status field: {}", status);
    }
    if let Some(results_count) = json_content.get("resultsCount") {
        info!("API response resultsCount: {}", results_count);
    }

    let results = match json_content.get("results") {
        Some(results) => {
            info!("Found results field in response");
            results
        }
        None => {
            warn!("No results field found in response");
            debug!(
                "Response keys: {:?}",
                json_content
                    .as_object()
                    .map(|o| o.keys().collect::<Vec<_>>())
            );
            return Ok(None);
        }
    };

    info!("Parsing results into BarResult structs");
    let bars: Vec<BarResult> =
        serde_json::from_value::<Vec<BarResult>>(results.clone()).map_err(|err| {
            warn!("Failed to parse results into BarResult structs: {}", err);
            warn!("Results type: {:?}", results.as_array().map(|a| a.len()));
            if let Some(first_result) = results.as_array().and_then(|a| a.first()) {
                warn!("First result sample: {}", first_result);
            }
            "Failed to parse equity bar results".to_string()
        })?;

    info!("Successfully parsed {} bar results", bars.len());

    let equity_bars: Vec<EquityBar> = bars
        .iter()
        .map(|b| EquityBar {
            ticker: b.ticker.clone(),
            timestamp: b.t as i64,
            open_price: b.o,
            high_price: b.h,
            low_price: b.l,
            close_price: b.c,
            volume: b.v.and_then(|v| {
                if v.is_finite() && v >= 0.0 {
                    let rounded = v.round();
                    if rounded <= i64::MAX as f64 {
                        Some(rounded as i64)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }),
            volume_weighted_average_price: b.vw,
            transactions: b.n,
        })
        .collect();

    Ok(Some(equity_bars))
}

/// Fetches grouped daily equity bars for `date` and, when a Postgres pool is
/// configured, upserts them into the `equity_bars` table. Returns the number of
/// bars fetched, or `Ok(None)` when the API has no data for the date.
pub async fn fetch_and_store(state: &State, date: &DateTime<Utc>) -> Result<Option<usize>, String> {
    let equity_bars = match fetch_equity_bars(state, date).await? {
        Some(equity_bars) => equity_bars,
        None => return Ok(None),
    };

    let bar_count = equity_bars.len();

    if let Some(pool) = &state.pool {
        database::insert_equity_bars(pool, &equity_bars)
            .await
            .map_err(|error| {
                warn!("Failed to write equity bars to PostgreSQL: {}", error);
                format!("Failed to insert equity bars: {}", error)
            })?;
    }

    Ok(Some(bar_count))
}

/// Converts a calendar date to the instant of noon US/Eastern on that date, in
/// UTC. Noon avoids any date-boundary ambiguity when the Eastern-keyed Massive
/// grouped endpoint maps a UTC instant back to a trading day.
pub fn noon_eastern_utc(date: NaiveDate) -> DateTime<Utc> {
    Eastern
        .from_local_datetime(&date.and_hms_opt(12, 0, 0).unwrap())
        .earliest()
        .unwrap()
        .with_timezone(&Utc)
}

/// Outcome of a [`backfill`] run.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BackfillSummary {
    pub days_processed: usize,
    pub days_skipped_weekend: usize,
    pub days_failed: usize,
    pub total_bars: usize,
}

/// Backfills equity bars for every trading day in `[start, end]` (inclusive) by
/// fetching from the Massive API and writing Hive-partitioned Parquet directly
/// to S3. Postgres is intentionally bypassed: `equity_bars` is a 90-day rolling
/// buffer and the consumer of historical bars (model training) reads from S3.
///
/// Weekends are skipped. A failure on a single day is logged and counted but
/// does not abort the range. Days are processed sequentially to stay gentle on
/// the Massive API.
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
        if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
            debug!("Skipping weekend date: {}", date.format("%Y-%m-%d"));
            summary.days_skipped_weekend += 1;
            date = date.succ_opt().unwrap();
            continue;
        }

        match backfill_one_day(state, date).await {
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
        }

        date = date.succ_opt().unwrap();
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

/// Fetches one trading day and writes it to S3, returning the bar count.
async fn backfill_one_day(state: &State, date: NaiveDate) -> Result<usize, String> {
    let equity_bars = match fetch_equity_bars(state, &noon_eastern_utc(date)).await? {
        Some(equity_bars) if !equity_bars.is_empty() => equity_bars,
        _ => {
            info!("No market data for {}", date.format("%Y-%m-%d"));
            return Ok(0);
        }
    };

    let bar_count = equity_bars.len();
    write_equity_bars_to_s3(state, date, &equity_bars)
        .await
        .map_err(|error| format!("Failed to write equity bars to S3: {}", error))?;

    Ok(bar_count)
}

#[derive(Deserialize)]
pub struct RecentQueryParameters {
    tickers: Option<String>,
    days: Option<i32>,
}

pub async fn query_recent(
    AxumState(state): AxumState<State>,
    Query(parameters): Query<RecentQueryParameters>,
) -> impl IntoResponse {
    let days_back = parameters.days.unwrap_or(10);
    if !(1..=30).contains(&days_back) {
        return (StatusCode::BAD_REQUEST, "days must be between 1 and 30").into_response();
    }

    let pool = match &state.pool {
        Some(pool) => pool,
        None => {
            return (StatusCode::SERVICE_UNAVAILABLE, "PostgreSQL not available").into_response();
        }
    };

    let tickers: Option<Vec<String>> = parameters.tickers.as_ref().and_then(|tickers_str| {
        let parsed: Vec<String> = tickers_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_uppercase())
            .collect();
        if parsed.is_empty() {
            None
        } else {
            Some(parsed)
        }
    });

    match database::query_recent_equity_bars(pool, tickers.as_deref(), days_back).await {
        Ok(bars) => {
            let dataframe = crate::data_manager::data::create_equity_bar_dataframe(bars);
            match dataframe {
                Ok(dataframe) => {
                    let mut buffer = Vec::new();
                    let mut dataframe = dataframe;
                    if let Err(error) = ParquetWriter::new(&mut buffer).finish(&mut dataframe) {
                        warn!("Failed to serialize parquet: {}", error);
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to serialize response",
                        )
                            .into_response();
                    }
                    let mut response = Response::new(Body::from(buffer));
                    response.headers_mut().insert(
                        header::CONTENT_TYPE,
                        "application/octet-stream".parse().unwrap(),
                    );
                    response.headers_mut().insert(
                        "Content-Disposition",
                        "attachment; filename=\"equity_data.parquet\""
                            .parse()
                            .unwrap(),
                    );
                    *response.status_mut() = StatusCode::OK;
                    response
                }
                Err(error) => {
                    warn!("Failed to create DataFrame from cache: {}", error);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Query failed: {}", error),
                    )
                        .into_response()
                }
            }
        }
        Err(error) => {
            warn!("Failed to query PostgreSQL cache: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Cache query failed: {}", error),
            )
                .into_response()
        }
    }
}

pub async fn sync(
    AxumState(state): AxumState<State>,
    Json(payload): Json<DailySync>,
) -> impl IntoResponse {
    info!("Sync date: {}", payload.date);

    let weekday = payload.date.weekday();
    if weekday == Weekday::Sat || weekday == Weekday::Sun {
        info!("Skipping weekend date: {}", payload.date.format("%Y-%m-%d"));
        return (
            StatusCode::OK,
            "Skipping weekend, no trading data available",
        )
            .into_response();
    }

    match fetch_and_store(&state, &payload.date).await {
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
    use super::grouped_bars_url;

    #[test]
    fn test_grouped_bars_url_without_trailing_slash() {
        assert_eq!(
            grouped_bars_url("https://api.massive.com", "2023-01-03"),
            "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2023-01-03"
        );
    }

    #[test]
    fn test_grouped_bars_url_trims_trailing_slash() {
        // A trailing slash on the base URL must not produce a double slash,
        // which the Massive API rejects with a 404.
        assert_eq!(
            grouped_bars_url("https://api.massive.com/", "2023-01-03"),
            "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2023-01-03"
        );
    }
}
