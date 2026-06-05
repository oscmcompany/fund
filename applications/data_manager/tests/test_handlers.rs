mod common;

use data_manager::{
    router::create_app_with_state,
    state::{MassiveSecrets, State},
};
use mockito::{Matcher, Server};
use polars::prelude::*;
use reqwest::StatusCode;
use serial_test::serial;

use common::{
    create_test_s3_client, put_test_object, setup_test_bucket, test_bucket_name,
    EnvironmentVariableGuard, SpawnedAppServer,
};

async fn spawn_app(
    endpoint: &str,
    massive_base: String,
) -> (SpawnedAppServer, EnvironmentVariableGuard) {
    let env_guard = EnvironmentVariableGuard::set("MASSIVE_API_KEY", "test-api-key");

    let s3_client = create_test_s3_client(endpoint).await;
    let state = State::new(
        reqwest::Client::new(),
        MassiveSecrets {
            base: massive_base,
            key: std::env::var("MASSIVE_API_KEY").unwrap(),
        },
        s3_client,
        test_bucket_name(),
    );
    let app = create_app_with_state(state);
    (SpawnedAppServer::start(app).await, env_guard)
}

async fn spawn_app_with_unreachable_s3(massive_base: String) -> SpawnedAppServer {
    let unreachable_s3_client = create_test_s3_client("http://127.0.0.1:9").await;
    let state = State::new(
        reqwest::Client::new(),
        MassiveSecrets {
            base: massive_base,
            key: "test-api-key".to_string(),
        },
        unreachable_s3_client,
        "test-bucket".to_string(),
    );
    let app = create_app_with_state(state);
    SpawnedAppServer::start(app).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_no_content_when_api_has_no_results() {
    let (endpoint, _s3) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v2/aggs/grouped/locale/us/market/stocks/2025-01-01")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("adjusted".into(), "true".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body(r#"{"status":"OK","resultsCount":0}"#)
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T12:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_internal_server_error_for_invalid_json() {
    let (endpoint, _s3) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v2/aggs/grouped/locale/us/market/stocks/2025-01-01")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("adjusted".into(), "true".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body("not-json")
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_internal_server_error_for_unparseable_results() {
    let (endpoint, _s3) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v2/aggs/grouped/locale/us/market/stocks/2025-01-01")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("adjusted".into(), "true".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body(
            r#"{
                "status": "OK",
                "results": [{"T":"AAPL"}]
            }"#,
        )
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_internal_server_error_when_api_request_fails() {
    let (endpoint, _s3) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_internal_server_error_for_api_error_status() {
    let (endpoint, _s3) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v2/aggs/grouped/locale/us/market/stocks/2025-01-01")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("adjusted".into(), "true".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(500)
        .with_body("Internal Server Error")
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_succeeds_when_s3_upload_fails() {
    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v2/aggs/grouped/locale/us/market/stocks/2025-01-01")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("adjusted".into(), "true".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body(r#"{"adjusted":true,"queryCount":1,"request_id":"t","resultsCount":1,"status":"OK","results":[{"T":"AAPL","c":105.0,"h":110.0,"l":99.0,"n":1000,"o":100.0,"t":1735689600,"v":2000000.0,"vw":104.0}]}"#)
        .create_async()
        .await;

    // Working Massive API but broken S3 — parse succeeds, S3 upload fails non-fatally
    let app = spawn_app_with_unreachable_s3(massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_ok_for_weekend_date() {
    // 2025-01-04 is a Saturday, 2025-01-05 is a Sunday — no API or S3 calls expected
    let app = spawn_app_with_unreachable_s3("http://127.0.0.1:1".to_string()).await;
    let client = reqwest::Client::new();

    let response = client
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-04T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-05T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_writes_parquet_to_s3() {
    let (endpoint, s3) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v2/aggs/grouped/locale/us/market/stocks/2025-01-01")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("adjusted".into(), "true".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body(r#"{"adjusted":true,"queryCount":1,"request_id":"t","resultsCount":1,"status":"OK","results":[{"T":"AAPL","c":105.0,"h":110.0,"l":99.0,"n":1000,"o":100.0,"t":1735689600,"v":2000000.0,"vw":104.0}]}"#)
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T12:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let result = s3
        .get_object()
        .bucket(test_bucket_name())
        .key("data/equity/bars/daily/year=2025/month=01/day=01/data.parquet")
        .send()
        .await;
    assert!(result.is_ok(), "Expected Parquet file to be written to S3");

    let bytes = result.unwrap().body.collect().await.unwrap().into_bytes();
    let dataframe = ParquetReader::new(std::io::Cursor::new(bytes.to_vec()))
        .finish()
        .unwrap();
    assert_eq!(dataframe.height(), 1);
    assert_eq!(
        dataframe.column("ticker").unwrap().str().unwrap().get(0),
        Some("AAPL")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_csv_can_be_read_from_s3() {
    let (endpoint, s3) = setup_test_bucket().await;

    put_test_object(
        &s3,
        "data/equity/details/details.csv",
        b"ticker,sector,industry\nAAPL,Technology,Consumer Electronics\n".to_vec(),
    )
    .await;

    let s3_client = create_test_s3_client(&endpoint).await;
    let state = data_manager::state::State::new(
        reqwest::Client::new(),
        data_manager::state::MassiveSecrets {
            base: "http://127.0.0.1:1".to_string(),
            key: "test-api-key".to_string(),
        },
        s3_client,
        test_bucket_name(),
    );

    let result = data_manager::equity_details::read_equity_details_from_s3(&state).await;
    assert!(result.is_ok());
    let details = result.unwrap();
    assert!(details.iter().any(|d| d.ticker().as_str() == "AAPL"));
}
