use crate::data::{create_predictions_dataframe, Prediction};
use crate::state::State;
use crate::storage::{
    query_predictions_dataframe_from_s3, write_predictions_dataframe_to_s3, PredictionQuery,
};
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
use urlencoding::decode;

#[derive(Deserialize)]
pub struct SavePayload {
    pub data: Vec<Prediction>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct QueryParameters {
    pub tickers_and_timestamps: String, // URL-encoded JSON string
}

pub async fn save(
    AxumState(state): AxumState<State>,
    Json(payload): Json<SavePayload>,
) -> impl IntoResponse {
    let predictions = match create_predictions_dataframe(payload.data) {
        Ok(df) => df,
        Err(err) => {
            warn!("Failed to create predictions DataFrame: {}", err);
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid prediction data: {}", err),
            )
                .into_response();
        }
    };

    let timestamp = payload.timestamp;

    match write_predictions_dataframe_to_s3(&state, &predictions, &timestamp).await {
        Ok(s3_key) => {
            info!("Successfully uploaded DataFrame to S3 at key: {}", s3_key);
            let response_message = format!(
                "DataFrame created with {} rows and uploaded to S3: {}",
                predictions.height(),
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

pub async fn query(
    AxumState(state): AxumState<State>,
    Query(parameters): Query<QueryParameters>,
) -> impl IntoResponse {
    info!("Fetching predictions from S3");

    let decoded = match decode(&parameters.tickers_and_timestamps) {
        Ok(decoded) => decoded.into_owned(),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to decode query parameter: {}", e),
            )
                .into_response();
        }
    };

    let predictions_query: Vec<PredictionQuery> = match serde_json::from_str(&decoded) {
        Ok(query) => query,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse JSON: {}", e),
            )
                .into_response();
        }
    };

    match query_predictions_dataframe_from_s3(&state, predictions_query).await {
        Ok(dataframe) => {
            if dataframe.height() == 0 {
                warn!("No predictions found for the requested tickers and timestamps");
                return (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    "[]".to_string(),
                )
                    .into_response();
            }

            let mut buffer = Cursor::new(Vec::new());
            match JsonWriter::new(&mut buffer)
                .with_json_format(JsonFormat::Json)
                .finish(&mut dataframe.clone())
            {
                Ok(_) => {
                    let json_bytes = buffer.into_inner();
                    let json_string = String::from_utf8_lossy(&json_bytes).to_string();
                    info!(
                        "Returning predictions as JSON with {} rows",
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
                    warn!("Failed to serialize predictions to JSON: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to serialize predictions: {}", e),
                    )
                        .into_response()
                }
            }
        }
        Err(err) => {
            info!("Failed to query S3 data: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Query failed: {}", err),
            )
                .into_response()
        }
    }
}
