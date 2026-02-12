use crate::state::State;
use crate::storage::read_equity_details_dataframe_from_s3;
use axum::{
    extract::State as AxumState,
    http::{header, StatusCode},
    response::IntoResponse,
};
use polars::prelude::*;
use tracing::info;

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
