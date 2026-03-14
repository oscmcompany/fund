use axum::{http::StatusCode, response::IntoResponse};
use tracing::debug;

pub async fn get_health() -> impl IntoResponse {
    debug!("Health check endpoint called");
    (StatusCode::OK).into_response()
}
