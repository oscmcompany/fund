use crate::data_manager::data::create_equity_details_dataframe;
use crate::data_manager::state::State;
use crate::data_manager::storage::read_equity_details_csv_from_s3;
use axum::{
    extract::State as AxumState,
    http::{header, StatusCode},
    response::IntoResponse,
};
use polars::prelude::*;
use tracing::{info, warn};

pub async fn get(AxumState(state): AxumState<State>) -> impl IntoResponse {
    info!("Fetching equity details CSV from S3");

    match read_equity_details_csv_from_s3(&state).await {
        Ok(csv_content) => {
            let mut dataframe = match create_equity_details_dataframe(csv_content) {
                Ok(parsed_dataframe) => parsed_dataframe,
                Err(err) => {
                    warn!("Failed to parse equity details CSV: {}", err);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to parse equity details: {}", err),
                    )
                        .into_response();
                }
            };

            let mut buffer = Vec::new();
            if let Err(err) = CsvWriter::new(&mut buffer).finish(&mut dataframe) {
                warn!("Failed to serialize equity details DataFrame: {}", err);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to serialize equity details: {}", err),
                )
                    .into_response();
            }

            let csv_output = match String::from_utf8(buffer) {
                Ok(s) => s,
                Err(err) => {
                    warn!("Equity details CSV output is not valid UTF-8: {}", err);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to serialize equity details: invalid UTF-8",
                    )
                        .into_response();
                }
            };
            let mut response = csv_output.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/csv; charset=utf-8"),
            );
            *response.status_mut() = StatusCode::OK;
            response
        }
        Err(err) => {
            warn!("Failed to fetch equity details from S3: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to fetch equity details: {}", err),
            )
                .into_response()
        }
    }
}

pub async fn sync(_state: AxumState<State>) -> impl IntoResponse {
    warn!("Equity details sync is not implemented");
    (StatusCode::NOT_IMPLEMENTED, "Sync is not implemented").into_response()
}
