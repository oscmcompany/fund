use axum::{extract::State as AxumState, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use tracing::debug;

use crate::state::State;

pub async fn get_health(AxumState(state): AxumState<State>) -> impl IntoResponse {
    debug!("Health check endpoint called");

    let s3_ok = state
        .s3_client
        .head_bucket()
        .bucket(&state.bucket_name)
        .send()
        .await
        .is_ok();

    if s3_ok {
        (
            StatusCode::OK,
            Json(json!({"status": "ok", "checks": {"s3": "ok"}})),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "degraded", "checks": {"s3": "error"}})),
        )
    }
}
