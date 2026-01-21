use crate::data::{create_portfolio_dataframe, Portfolio};
use crate::state::State;
use crate::storage::{query_portfolio_dataframe_from_s3, write_portfolio_dataframe_to_s3};
use axum::{
    extract::{Json, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::Deserialize;
use std::io::Cursor;
use tracing::{info, warn};

#[derive(Deserialize)]
pub struct SavePortfolioPayload {
    pub data: Vec<Portfolio>,
    pub timestamp: DateTime<Utc>,
}

pub async fn save(
    AxumState(state): AxumState<State>,
    Json(payload): Json<SavePortfolioPayload>,
) -> impl IntoResponse {
    let portfolio = match create_portfolio_dataframe(payload.data) {
        Ok(df) => df,
        Err(err) => {
            warn!("Failed to create portfolio DataFrame: {}", err);
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid portfolio data: {}", err),
            )
                .into_response();
        }
    };

    let timestamp = payload.timestamp;

    match write_portfolio_dataframe_to_s3(&state, &portfolio, &timestamp).await {
        Ok(s3_key) => {
            info!("Successfully uploaded DataFrame to S3 at key: {}", s3_key);
            let response_message = format!(
                "DataFrame created with {} rows and uploaded to S3: {}",
                portfolio.height(),
                s3_key
            );

            (StatusCode::OK, response_message).into_response()
        }
        Err(err) => {
            info!("Failed to upload to S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 upload failed: {}", err),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct QueryParameters {
    timestamp: Option<DateTime<Utc>>,
}

pub async fn get(
    AxumState(state): AxumState<State>,
    Query(parameters): Query<QueryParameters>,
) -> impl IntoResponse {
    info!("Fetching portfolio from S3");

    let timestamp: Option<DateTime<Utc>> = parameters.timestamp;

    match query_portfolio_dataframe_from_s3(&state, timestamp).await {
        Ok(dataframe) => {
            if dataframe.height() == 0 {
                warn!("No portfolio data found - this is expected on first run");
                return (StatusCode::NOT_FOUND, "No portfolio data found").into_response();
            }

            // Convert DataFrame to JSON array
            let mut buffer = Cursor::new(Vec::new());
            match JsonWriter::new(&mut buffer)
                .with_json_format(JsonFormat::Json)
                .finish(&mut dataframe.clone())
            {
                Ok(_) => {
                    let json_bytes = buffer.into_inner();
                    let json_string = String::from_utf8_lossy(&json_bytes).to_string();
                    info!(
                        "Returning portfolio as JSON with {} rows",
                        dataframe.height()
                    );
                    (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        json_string,
                    )
                        .into_response()
                }
                Err(e) => {
                    warn!("Failed to serialize portfolio to JSON: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to serialize portfolio: {}", e),
                    )
                        .into_response()
                }
            }
        }
        Err(err) => {
            let err_str = err.to_string();
            // Check if error indicates no files found (expected on first run)
            if err_str.contains("No files found")
                || err_str.contains("Could not find")
                || err_str.contains("does not exist")
                || err_str.contains("Invalid Input")
            {
                warn!(
                    "No portfolio files in S3 - this is expected on first run: {}",
                    err
                );
                return (StatusCode::NOT_FOUND, "No portfolio data found - first run")
                    .into_response();
            }
            warn!("Failed to fetch portfolio from S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch portfolio: {}", err),
            )
                .into_response()
        }
    }
}
