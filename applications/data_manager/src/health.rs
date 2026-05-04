use axum::{extract::State as AxumState, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;
use tracing::debug;

use crate::state::State;

const S3_HEALTH_TTL_SECS: u64 = 60;

pub async fn get_health(AxumState(state): AxumState<State>) -> impl IntoResponse {
    debug!("Health check endpoint called");

    let s3_ok = if state.s3_ok_recently(S3_HEALTH_TTL_SECS) {
        true
    } else {
        let ok = timeout(
            Duration::from_secs(3),
            state
                .s3_client
                .head_bucket()
                .bucket(&state.bucket_name)
                .send(),
        )
        .await
        .map(|result| result.is_ok())
        .unwrap_or(false);

        if ok {
            state.mark_s3_ok();
        }
        ok
    };

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
