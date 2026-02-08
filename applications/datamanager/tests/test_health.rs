use axum::{body::Body, http::Request};
use datamanager::router::create_app;
use hyper::http::StatusCode;
use tower::ServiceExt;

#[tokio::test]
async fn test_health_endpoint_returns_ok() {
    let app = create_app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
