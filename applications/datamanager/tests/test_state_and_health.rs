mod common;

use datamanager::{
    router::create_app_with_state,
    state::{MassiveSecrets, State},
};
use reqwest::StatusCode;
use serial_test::serial;

use common::{
    create_test_s3_client, setup_test_bucket, test_bucket_name, EnvironmentVariableGuard,
    SpawnedAppServer,
};

async fn create_state_for_endpoint(endpoint: &str, bucket_name: &str) -> State {
    let s3_client = create_test_s3_client(endpoint).await;

    State::new(
        reqwest::Client::new(),
        MassiveSecrets {
            base: "http://127.0.0.1:1".to_string(),
            key: "test-key".to_string(),
        },
        s3_client,
        bucket_name.to_string(),
    )
}

async fn spawn_server_for_state(state: State) -> SpawnedAppServer {
    let app = create_app_with_state(state);
    SpawnedAppServer::start(app).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_health_route_returns_ok() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state_for_endpoint(&endpoint, &test_bucket_name()).await;
    let app_server = spawn_server_for_state(state).await;

    let response = reqwest::Client::new()
        .get(app_server.url("/health"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_router_returns_not_found_for_unknown_route() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state_for_endpoint(&endpoint, &test_bucket_name()).await;
    let app_server = spawn_server_for_state(state).await;

    let response = reqwest::Client::new()
        .get(app_server.url("/missing"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_state_new_sets_all_fields() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let state = create_state_for_endpoint(&endpoint, "custom-bucket").await;

    assert_eq!(state.massive.base, "http://127.0.0.1:1");
    assert_eq!(state.massive.key, "test-key");
    assert_eq!(state.bucket_name, "custom-bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_state_from_env_uses_defaults_when_variables_are_missing() {
    let _aws_bucket_guard = EnvironmentVariableGuard::remove("AWS_S3_DATA_BUCKET_NAME");
    let _massive_base_guard = EnvironmentVariableGuard::remove("MASSIVE_BASE_URL");
    let _massive_key_guard = EnvironmentVariableGuard::remove("MASSIVE_API_KEY");
    let _region_guard = EnvironmentVariableGuard::set("AWS_REGION", "us-east-1");
    let _metadata_guard = EnvironmentVariableGuard::set("AWS_EC2_METADATA_DISABLED", "true");

    let state = State::from_env().await;

    assert_eq!(state.bucket_name, "fund-data");
    assert_eq!(state.massive.base, "https://api.massive.io");
    assert!(state.massive.key.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_state_from_env_uses_environment_values() {
    let _aws_bucket_guard = EnvironmentVariableGuard::set("AWS_S3_DATA_BUCKET_NAME", "env-bucket");
    let _massive_base_guard =
        EnvironmentVariableGuard::set("MASSIVE_BASE_URL", "https://massive.example");
    let _massive_key_guard = EnvironmentVariableGuard::set("MASSIVE_API_KEY", "env-api-key");
    let _region_guard = EnvironmentVariableGuard::set("AWS_REGION", "us-east-1");
    let _metadata_guard = EnvironmentVariableGuard::set("AWS_EC2_METADATA_DISABLED", "true");

    let state = State::from_env().await;

    assert_eq!(state.bucket_name, "env-bucket");
    assert_eq!(state.massive.base, "https://massive.example");
    assert_eq!(state.massive.key, "env-api-key");
}
