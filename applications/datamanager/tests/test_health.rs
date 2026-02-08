use axum::http::StatusCode;
use axum::response::IntoResponse;
use datamanager::health::get_health;

#[tokio::test]
async fn test_health_endpoint_returns_ok() {
    let response = get_health().await.into_response();
    assert_eq!(response.status(), StatusCode::OK);
}
