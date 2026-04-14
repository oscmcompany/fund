use crate::state::State;
use crate::storage::{query_equity_bars_parquet_from_s3, write_equity_bars_dataframe_to_s3};
use axum::{
    body::Body,
    extract::{Json, Query, State as AxumState},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Datelike, Utc, Weekday};
use chrono_tz::US::Eastern;
use polars::prelude::*;
use serde::Deserialize;
use tracing::{debug, info, warn};

#[derive(Deserialize)]
pub struct DailySync {
    pub date: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct QueryParameters {
    tickers: Option<String>,
    start_timestamp: Option<DateTime<Utc>>,
    end_timestamp: Option<DateTime<Utc>>,
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

pub async fn query(
    AxumState(state): AxumState<State>,
    Query(parameters): Query<QueryParameters>,
) -> impl IntoResponse {
    info!(
        "Querying equity data from S3 partitioned files, tickers: {:?}, start: {:?}, end: {:?}",
        parameters.tickers, parameters.start_timestamp, parameters.end_timestamp
    );

    let tickers: Option<Vec<String>> = match &parameters.tickers {
        Some(tickers_str) if !tickers_str.is_empty() => {
            let vec: Vec<String> = tickers_str
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .collect();
            if vec.is_empty() {
                debug!("Ticker list was empty after parsing");
                None
            } else {
                debug!("Parsed {} tickers: {:?}", vec.len(), vec);
                Some(vec)
            }
        }
        _ => {
            debug!("No tickers specified, querying all");
            None
        }
    };

    match query_equity_bars_parquet_from_s3(
        &state,
        tickers,
        parameters.start_timestamp,
        parameters.end_timestamp,
    )
    .await
    {
        Ok(parquet_data) => {
            info!(
                "Query successful, returning {} bytes of parquet data",
                parquet_data.len()
            );
            let mut response = Response::new(Body::from(parquet_data));
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
        Err(err) => {
            warn!("Failed to query S3 data: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Query failed: {}", err),
            )
                .into_response()
        }
    }
}

pub async fn fetch_and_store(
    state: &State,
    date: &DateTime<Utc>,
) -> Result<Option<String>, String> {
    let massive_api_key = state.massive.key.clone();

    let date_str = date.with_timezone(&Eastern).format("%Y-%m-%d").to_string();
    let url = format!(
        "{}/v2/aggs/grouped/locale/us/market/stocks/{}",
        state.massive.base, date_str
    );

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
            warn!("API request failed with error status: {}", err);
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

    let tickers: Vec<String> = bars.iter().map(|b| b.ticker.clone()).collect();
    let volumes: Vec<Option<i64>> = bars
        .iter()
        .map(|b| {
            b.v.and_then(|v| {
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
            })
        })
        .collect();
    let volume_weighted_average_prices: Vec<Option<f64>> = bars.iter().map(|b| b.vw).collect();
    let open_prices: Vec<Option<f64>> = bars.iter().map(|b| b.o).collect();
    let close_prices: Vec<Option<f64>> = bars.iter().map(|b| b.c).collect();
    let high_prices: Vec<Option<f64>> = bars.iter().map(|b| b.h).collect();
    let low_prices: Vec<Option<f64>> = bars.iter().map(|b| b.l).collect();
    let timestamps: Vec<i64> = bars.iter().map(|b| b.t as i64).collect();
    let transactions: Vec<Option<u64>> = bars.iter().map(|b| b.n).collect();

    info!("Creating DataFrame from bar data");
    let data = df! {
        "ticker" => tickers,
        "timestamp" => timestamps,
        "open_price" => open_prices,
        "high_price" => high_prices,
        "low_price" => low_prices,
        "close_price" => close_prices,
        "volume" => volumes,
        "volume_weighted_average_price" => volume_weighted_average_prices,
        "transactions" => transactions,
    }
    .map_err(|err| {
        warn!("Failed to create DataFrame: {}", err);
        "Failed to create equity bars DataFrame".to_string()
    })?;

    info!(
        "Created DataFrame with {} rows and {} columns",
        data.height(),
        data.width()
    );
    debug!("DataFrame schema: {:?}", data.schema());

    info!("Uploading DataFrame to S3");
    let s3_key = write_equity_bars_dataframe_to_s3(state, &data, date)
        .await
        .map_err(|err| {
            warn!(
                "Failed to upload to S3: {}, rows: {}, columns: {}, date: {}",
                err,
                data.height(),
                data.width(),
                date.with_timezone(&Eastern).format("%Y-%m-%d")
            );
            format!(
                "Failed to upload equity bars to storage for date {}",
                date.with_timezone(&Eastern).format("%Y-%m-%d")
            )
        })?;

    info!("Successfully uploaded DataFrame to S3 at key: {}", s3_key);
    Ok(Some(s3_key))
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
        Ok(Some(s3_key)) => {
            let response_message = format!("Data fetched and uploaded to S3: {}", s3_key);
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
