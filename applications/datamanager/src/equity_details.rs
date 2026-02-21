use crate::state::State;
use crate::storage::{read_equity_details_dataframe_from_s3, write_equity_details_dataframe_to_s3};
use axum::{
    extract::State as AxumState,
    http::{header, StatusCode},
    response::IntoResponse,
};
use polars::prelude::*;
use serde::Deserialize;
use tracing::{debug, info, warn};

const EQUITY_TYPES: &[&str] = &["CS", "ADRC", "ADRP", "ADRS"];

#[derive(Deserialize, Debug)]
struct TickerResult {
    ticker: Option<String>,
    #[serde(rename = "type")]
    ticker_type: Option<String>,
    sector: Option<String>,
    industry: Option<String>,
}

#[derive(Deserialize, Debug)]
struct TickerResponse {
    results: Option<Vec<TickerResult>>,
    next_url: Option<String>,
}

pub async fn get(AxumState(state): AxumState<State>) -> impl IntoResponse {
    info!("Fetching equity details CSV from S3");

    match read_equity_details_dataframe_from_s3(&state).await {
        Ok(dataframe) => {
            let mut buffer = Vec::new();
            let mut writer = CsvWriter::new(&mut buffer);
            match writer.finish(&mut dataframe.clone()) {
                Ok(_) => {}
                Err(err) => {
                    info!("Failed to write CSV: {}", err);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to write CSV: {}", err),
                    )
                        .into_response();
                }
            }

            let csv_content = match String::from_utf8(buffer) {
                Ok(content) => content,
                Err(err) => {
                    info!("Failed to convert CSV to UTF-8: {}", err);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to convert CSV to UTF-8: {}", err),
                    )
                        .into_response();
                }
            };
            let mut response = csv_content.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/csv; charset=utf-8"),
            );
            *response.status_mut() = StatusCode::OK;
            response
        }
        Err(err) => {
            info!("Failed to fetch equity details from S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch equity details: {}", err),
            )
                .into_response()
        }
    }
}

pub async fn sync(AxumState(state): AxumState<State>) -> impl IntoResponse {
    info!("Syncing equity details from Massive API");

    let massive_api_key = state.massive.key.clone();
    let base_url = format!("{}/v3/reference/tickers", state.massive.base);

    let mut all_tickers: Vec<TickerResult> = Vec::new();
    let mut current_url = base_url;
    let mut is_first_page = true;
    let mut page_count: usize = 0;
    const MAX_PAGES: usize = 1000;

    loop {
        if page_count >= MAX_PAGES {
            warn!(
                "Reached maximum page limit of {}, stopping pagination",
                MAX_PAGES
            );
            break;
        }
        page_count += 1;
        debug!("Fetching ticker page, url: {}", current_url);

        let mut request = state
            .http_client
            .get(&current_url)
            .header("accept", "application/json");

        if is_first_page {
            request = request.query(&[
                ("market", "stocks"),
                ("active", "true"),
                ("limit", "1000"),
                ("apiKey", massive_api_key.as_str()),
            ]);
        } else {
            request = request.query(&[("apiKey", massive_api_key.as_str())]);
        }

        let response = match request.send().await {
            Ok(response) => {
                info!(
                    "Received response from Massive API, status: {}",
                    response.status()
                );
                response
            }
            Err(err) => {
                warn!("Failed to send request to Massive API: {}", err);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to send API request",
                )
                    .into_response();
            }
        };

        let text_content = match response.error_for_status() {
            Ok(response) => match response.text().await {
                Ok(text) => {
                    info!("Received response body, length: {} bytes", text.len());
                    text
                }
                Err(err) => {
                    warn!("Failed to read response text: {}", err);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to read API response",
                    )
                        .into_response();
                }
            },
            Err(err) => {
                warn!("API request failed with error status: {}", err);
                return (StatusCode::INTERNAL_SERVER_ERROR, "API request failed").into_response();
            }
        };

        let page: TickerResponse = match serde_json::from_str(&text_content) {
            Ok(value) => {
                debug!("JSON parsed successfully");
                value
            }
            Err(err) => {
                warn!("Failed to parse JSON response: {}", err);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Invalid JSON response from API",
                )
                    .into_response();
            }
        };

        let results = page.results.unwrap_or_default();
        info!("Fetched {} tickers on this page", results.len());
        all_tickers.extend(results);

        match page.next_url {
            Some(next_url) if !next_url.is_empty() => {
                current_url = next_url;
                is_first_page = false;
                tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
            }
            _ => break,
        }
    }

    info!(
        "Fetched {} total tickers from Massive API",
        all_tickers.len()
    );

    let mut tickers: Vec<String> = Vec::new();
    let mut sectors: Vec<String> = Vec::new();
    let mut industries: Vec<String> = Vec::new();

    for result in all_tickers {
        let ticker = match result.ticker {
            Some(value) if !value.is_empty() => value,
            _ => continue,
        };

        let ticker_type = result.ticker_type.unwrap_or_default();
        if !EQUITY_TYPES.contains(&ticker_type.as_str()) {
            continue;
        }

        let sector = match result.sector {
            Some(value) if !value.is_empty() => value.to_uppercase(),
            _ => "NOT AVAILABLE".to_string(),
        };

        let industry = match result.industry {
            Some(value) if !value.is_empty() => value.to_uppercase(),
            _ => "NOT AVAILABLE".to_string(),
        };

        tickers.push(ticker.to_uppercase());
        sectors.push(sector);
        industries.push(industry);
    }

    info!("Filtered to {} equity tickers", tickers.len());

    if tickers.is_empty() {
        return (StatusCode::OK, "No equity ticker data available").into_response();
    }

    let details_data = df! {
        "ticker" => tickers,
        "sector" => sectors,
        "industry" => industries,
    };

    info!("Creating DataFrame from ticker data");
    match details_data {
        Ok(data) => {
            info!(
                "Created DataFrame with {} rows and {} columns",
                data.height(),
                data.width()
            );

            info!("Uploading DataFrame to S3");
            match write_equity_details_dataframe_to_s3(&state, &data).await {
                Ok(s3_key) => {
                    info!("Successfully uploaded DataFrame to S3 at key: {}", s3_key);
                    let response_message = format!(
                        "DataFrame created with {} rows and uploaded to S3: {}",
                        data.height(),
                        s3_key
                    );
                    (StatusCode::OK, response_message).into_response()
                }
                Err(err) => {
                    warn!("Failed to upload to S3: {}", err);
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("DataFrame created but S3 upload failed: {}", err),
                    )
                        .into_response()
                }
            }
        }
        Err(err) => {
            warn!("Failed to create DataFrame: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create DataFrame",
            )
                .into_response()
        }
    }
}
