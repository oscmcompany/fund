use crate::state::State;
use crate::storage::read_equity_details_csv_from_s3;
use axum::{
    extract::State as AxumState,
    http::{header, StatusCode},
    response::IntoResponse,
};
use tracing::{info, warn};

pub async fn get(AxumState(state): AxumState<State>) -> impl IntoResponse {
    info!("Fetching equity details CSV from S3");

    match read_equity_details_csv_from_s3(&state).await {
        Ok(csv_content) => {
            let mut response = csv_content.into_response();
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
