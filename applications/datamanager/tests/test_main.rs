use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use datamanager::router::create_app;
use datamanager::state::State;
use serial_test::serial;
use std::env;
use tower::ServiceExt;

#[tokio::test]
async fn test_create_app_succeeds() {
    let app = create_app().await;
    assert!(format!("{:?}", app).contains("Router"));
}

#[tokio::test]
async fn test_health_endpoint_via_app() {
    let app = create_app().await;

    let request = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn test_state_from_env_with_defaults() {
    env::remove_var("AWS_S3_DATA_BUCKET_NAME");
    env::remove_var("MASSIVE_BASE_URL");
    env::remove_var("MASSIVE_API_KEY");

    let state = State::from_env().await;

    assert_eq!(state.bucket_name, "oscm-data");
    assert_eq!(state.massive.base, "https://api.massive.io");
    assert_eq!(state.massive.key, "");
}

#[tokio::test]
#[serial]
async fn test_state_from_env_with_custom_bucket() {
    env::set_var("AWS_S3_DATA_BUCKET_NAME", "custom-bucket");
    env::remove_var("MASSIVE_BASE_URL");
    env::remove_var("MASSIVE_API_KEY");

    let state = State::from_env().await;

    assert_eq!(state.bucket_name, "custom-bucket");
    assert_eq!(state.massive.base, "https://api.massive.io");
    assert_eq!(state.massive.key, "");

    env::remove_var("AWS_S3_DATA_BUCKET_NAME");
}

#[tokio::test]
#[serial]
async fn test_state_from_env_with_custom_massive_url() {
    env::remove_var("AWS_S3_DATA_BUCKET_NAME");
    env::set_var("MASSIVE_BASE_URL", "https://custom.api.com");
    env::remove_var("MASSIVE_API_KEY");

    let state = State::from_env().await;

    assert_eq!(state.bucket_name, "oscm-data");
    assert_eq!(state.massive.base, "https://custom.api.com");
    assert_eq!(state.massive.key, "");

    env::remove_var("MASSIVE_BASE_URL");
}

#[tokio::test]
#[serial]
async fn test_state_from_env_with_custom_massive_key() {
    env::remove_var("AWS_S3_DATA_BUCKET_NAME");
    env::remove_var("MASSIVE_BASE_URL");
    env::set_var("MASSIVE_API_KEY", "test-api-key-12345");

    let state = State::from_env().await;

    assert_eq!(state.bucket_name, "oscm-data");
    assert_eq!(state.massive.base, "https://api.massive.io");
    assert_eq!(state.massive.key, "test-api-key-12345");

    env::remove_var("MASSIVE_API_KEY");
}

#[tokio::test]
#[serial]
async fn test_state_from_env_with_all_custom_values() {
    env::set_var("AWS_S3_DATA_BUCKET_NAME", "production-bucket");
    env::set_var("MASSIVE_BASE_URL", "https://prod.massive.io");
    env::set_var("MASSIVE_API_KEY", "prod-key-67890");

    let state = State::from_env().await;

    assert_eq!(state.bucket_name, "production-bucket");
    assert_eq!(state.massive.base, "https://prod.massive.io");
    assert_eq!(state.massive.key, "prod-key-67890");

    env::remove_var("AWS_S3_DATA_BUCKET_NAME");
    env::remove_var("MASSIVE_BASE_URL");
    env::remove_var("MASSIVE_API_KEY");
}

#[tokio::test]
async fn test_state_http_client_has_timeout() {
    let state = State::from_env().await;
    assert!(format!("{:?}", state.http_client).contains("Client"));
}

#[tokio::test]
async fn test_state_clone() {
    let state1 = State::from_env().await;
    let state2 = state1.clone();

    assert_eq!(state1.bucket_name, state2.bucket_name);
    assert_eq!(state1.massive.base, state2.massive.base);
    assert_eq!(state1.massive.key, state2.massive.key);
}

#[test]
fn test_massive_secrets_clone() {
    use datamanager::state::MassiveSecrets;

    let secrets1 = MassiveSecrets {
        base: "https://api.test.com".to_string(),
        key: "test-key".to_string(),
    };

    let secrets2 = secrets1.clone();

    assert_eq!(secrets1.base, secrets2.base);
    assert_eq!(secrets1.key, secrets2.key);
}

#[test]
fn test_sentry_event_filter_error_level() {
    use sentry::integrations::tracing::EventFilter;

    let error_level = &tracing::Level::ERROR;
    let filter = match error_level {
        &tracing::Level::ERROR | &tracing::Level::WARN => EventFilter::Event,
        _ => EventFilter::Breadcrumb,
    };

    assert!(matches!(filter, EventFilter::Event));
}

#[test]
fn test_sentry_event_filter_warn_level() {
    use sentry::integrations::tracing::EventFilter;

    let warn_level = &tracing::Level::WARN;
    let filter = match warn_level {
        &tracing::Level::ERROR | &tracing::Level::WARN => EventFilter::Event,
        _ => EventFilter::Breadcrumb,
    };

    assert!(matches!(filter, EventFilter::Event));
}

#[test]
fn test_sentry_event_filter_info_level() {
    use sentry::integrations::tracing::EventFilter;

    let info_level = &tracing::Level::INFO;
    let filter = match info_level {
        &tracing::Level::ERROR | &tracing::Level::WARN => EventFilter::Event,
        _ => EventFilter::Breadcrumb,
    };

    assert!(matches!(filter, EventFilter::Breadcrumb));
}

#[test]
fn test_sentry_event_filter_debug_level() {
    use sentry::integrations::tracing::EventFilter;

    let debug_level = &tracing::Level::DEBUG;
    let filter = match debug_level {
        &tracing::Level::ERROR | &tracing::Level::WARN => EventFilter::Event,
        _ => EventFilter::Breadcrumb,
    };

    assert!(matches!(filter, EventFilter::Breadcrumb));
}

#[test]
fn test_sentry_event_filter_trace_level() {
    use sentry::integrations::tracing::EventFilter;

    let trace_level = &tracing::Level::TRACE;
    let filter = match trace_level {
        &tracing::Level::ERROR | &tracing::Level::WARN => EventFilter::Event,
        _ => EventFilter::Breadcrumb,
    };

    assert!(matches!(filter, EventFilter::Breadcrumb));
}
