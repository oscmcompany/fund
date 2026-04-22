use crate::data::{
    create_closed_pair_dataframe, create_performance_snapshot_dataframe, ClosedPair,
    PerformanceSnapshot,
};
use crate::state::State;
use crate::storage::{
    query_closed_pairs_from_s3, query_performance_snapshots_from_s3, write_closed_pair_to_s3,
    write_performance_snapshot_to_s3,
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

#[derive(Deserialize)]
pub struct SaveSnapshotPayload {
    pub data: PerformanceSnapshot,
    pub timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct SnapshotQueryParameters {
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct SaveClosedPairPayload {
    pub data: ClosedPair,
    pub timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct ClosedPairQueryParameters {
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
}

pub async fn save_snapshot(
    AxumState(state): AxumState<State>,
    Json(payload): Json<SaveSnapshotPayload>,
) -> impl IntoResponse {
    let dataframe = match create_performance_snapshot_dataframe(vec![payload.data]) {
        Ok(dataframe) => dataframe,
        Err(err) => {
            warn!("Failed to create performance snapshot DataFrame: {}", err);
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid performance snapshot data: {}", err),
            )
                .into_response();
        }
    };

    let timestamp = payload.timestamp;

    match write_performance_snapshot_to_s3(&state, &dataframe, &timestamp).await {
        Ok(s3_key) => {
            info!(
                "Successfully uploaded performance snapshot to S3 at key: {}",
                s3_key
            );
            let response_message = format!(
                "Performance snapshot created with {} rows and uploaded to S3: {}",
                dataframe.height(),
                s3_key
            );
            (StatusCode::OK, response_message).into_response()
        }
        Err(err) => {
            info!("Failed to upload performance snapshot to S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 upload failed: {}", err),
            )
                .into_response()
        }
    }
}

pub async fn query_snapshots(
    AxumState(state): AxumState<State>,
    Query(parameters): Query<SnapshotQueryParameters>,
) -> impl IntoResponse {
    info!("Fetching performance snapshots from S3");

    match query_performance_snapshots_from_s3(
        &state,
        &parameters.start_timestamp,
        &parameters.end_timestamp,
    )
    .await
    {
        Ok(parquet_bytes) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            parquet_bytes,
        )
            .into_response(),
        Err(err) => {
            let err_str = err.to_string();
            if err_str.contains("No files found")
                || err_str.contains("Could not find")
                || err_str.contains("does not exist")
                || err_str.contains("Invalid Input")
            {
                info!("No performance snapshot files in S3, returning empty parquet");
                let empty_dataframe = df!(
                    "timestamp" => Vec::<i64>::new(),
                    "portfolio_value" => Vec::<f64>::new(),
                    "cash_balance" => Vec::<f64>::new(),
                    "spy_close" => Vec::<f64>::new(),
                    "period_return_pct" => Vec::<f64>::new(),
                    "open_pair_count" => Vec::<i64>::new(),
                )
                .unwrap();
                let mut buffer = Vec::new();
                let cursor = Cursor::new(&mut buffer);
                ParquetWriter::new(cursor)
                    .finish(&mut empty_dataframe.clone())
                    .unwrap();
                return (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                    buffer,
                )
                    .into_response();
            }
            warn!("Failed to fetch performance snapshots from S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch performance snapshots: {}", err),
            )
                .into_response()
        }
    }
}

pub async fn save_closed_pair(
    AxumState(state): AxumState<State>,
    Json(payload): Json<SaveClosedPairPayload>,
) -> impl IntoResponse {
    let dataframe = match create_closed_pair_dataframe(vec![payload.data]) {
        Ok(dataframe) => dataframe,
        Err(err) => {
            warn!("Failed to create closed pair DataFrame: {}", err);
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid closed pair data: {}", err),
            )
                .into_response();
        }
    };

    let timestamp = payload.timestamp;

    match write_closed_pair_to_s3(&state, &dataframe, &timestamp).await {
        Ok(s3_key) => {
            info!("Successfully uploaded closed pair to S3 at key: {}", s3_key);
            let response_message = format!(
                "Closed pair created with {} rows and uploaded to S3: {}",
                dataframe.height(),
                s3_key
            );
            (StatusCode::OK, response_message).into_response()
        }
        Err(err) => {
            info!("Failed to upload closed pair to S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 upload failed: {}", err),
            )
                .into_response()
        }
    }
}

pub async fn query_closed_pairs(
    AxumState(state): AxumState<State>,
    Query(parameters): Query<ClosedPairQueryParameters>,
) -> impl IntoResponse {
    info!("Fetching closed pairs from S3");

    match query_closed_pairs_from_s3(
        &state,
        &parameters.start_timestamp,
        &parameters.end_timestamp,
    )
    .await
    {
        Ok(parquet_bytes) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            parquet_bytes,
        )
            .into_response(),
        Err(err) => {
            let err_str = err.to_string();
            if err_str.contains("No files found")
                || err_str.contains("Could not find")
                || err_str.contains("does not exist")
                || err_str.contains("Invalid Input")
            {
                info!("No closed pair files in S3, returning empty parquet");
                let empty_dataframe = df!(
                    "closed_timestamp" => Vec::<i64>::new(),
                    "pair_id" => Vec::<String>::new(),
                    "long_ticker" => Vec::<String>::new(),
                    "short_ticker" => Vec::<String>::new(),
                    "entry_timestamp" => Vec::<i64>::new(),
                    "dollar_amount" => Vec::<f64>::new(),
                    "realized_pnl" => Vec::<f64>::new(),
                    "return_pct" => Vec::<f64>::new(),
                    "holding_days" => Vec::<i64>::new(),
                )
                .unwrap();
                let mut buffer = Vec::new();
                let cursor = Cursor::new(&mut buffer);
                ParquetWriter::new(cursor)
                    .finish(&mut empty_dataframe.clone())
                    .unwrap();
                return (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                    buffer,
                )
                    .into_response();
            }
            warn!("Failed to fetch closed pairs from S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch closed pairs: {}", err),
            )
                .into_response()
        }
    }
}
