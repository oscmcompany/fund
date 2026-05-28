use crate::data::EquityBar;
use crate::database;
use crate::state::State;
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

pub async fn fetch_and_store(state: &State, date: &DateTime<Utc>) -> Result<Option<usize>, String> {
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

    let bar_count = bars.len();

    if let Some(pool) = &state.pool {
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

        database::insert_equity_bars(pool, &equity_bars)
            .await
            .map_err(|error| {
                warn!("Failed to write equity bars to PostgreSQL: {}", error);
                format!("Failed to insert equity bars: {}", error)
            })?;
    }

    Ok(Some(bar_count))
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
            let dataframe = crate::data::create_equity_bar_dataframe(bars);
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
