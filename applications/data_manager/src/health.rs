use axum::{extract::State as AxumState, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;
use tracing::debug;

use crate::state::{DatabaseState, State};

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

    let postgres_status = match &state.database {
        DatabaseState::Connected(_) => "ok",
        DatabaseState::ConnectFailed => "error",
        DatabaseState::NotConfigured => "disabled",
    };

    let degraded = !s3_ok || postgres_status == "error";
    let overall_status = if degraded { "degraded" } else { "ok" };
    let status_code = if degraded {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        status_code,
        Json(json!({
            "status": overall_status,
            "checks": {
                "s3": if s3_ok { "ok" } else { "error" },
                "postgres": postgres_status,
            }
        })),
    )
}
