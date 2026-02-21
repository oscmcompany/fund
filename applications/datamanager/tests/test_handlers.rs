mod common;

use datamanager::{
    router::create_app_with_state,
    state::{MassiveSecrets, State},
};
use mockito::{Matcher, Server};
use polars::prelude::*;
use reqwest::StatusCode;
use serial_test::serial;

use common::{
    create_test_s3_client, put_test_object, setup_test_bucket, test_bucket_name,
    DuckDbEnvironmentGuard, EnvironmentVariableGuard, SpawnedAppServer,
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

async fn spawn_app_with_unreachable_s3(
    massive_base: String,
) -> (SpawnedAppServer, DuckDbEnvironmentGuard) {
    // Point DuckDB env vars to the same unreachable endpoint so that
    // DuckDB's httpfs also fails (DuckDB reads credentials and endpoint from env).
    let env_guard = DuckDbEnvironmentGuard::new("127.0.0.1:9");

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
    (SpawnedAppServer::start(app).await, env_guard)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_predictions_save_and_query_round_trip() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;
    let client = reqwest::Client::new();

    let save_payload = r#"{
        "data": [{
            "ticker": "AAPL",
            "timestamp": 1735689600,
            "quantile_10": 190.0,
            "quantile_50": 200.0,
            "quantile_90": 210.0
        }],
        "timestamp": "2025-01-01T00:00:00Z"
    }"#;

    let response = client
        .post(app.url("/predictions"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(save_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let encoded_query = urlencoding::encode("[{\"ticker\":\"AAPL\",\"timestamp\":1735689600.0}]");
    let response = client
        .get(app.url(&format!(
            "/predictions?tickers_and_timestamps={}",
            encoded_query
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.text().await.unwrap();
    assert!(body.contains("AAPL"));
    assert!(body.contains("quantile_50"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_predictions_save_returns_internal_server_error_when_s3_upload_fails() {
    let (app, _env_guard) = spawn_app_with_unreachable_s3("http://127.0.0.1:1".to_string()).await;

    let save_payload = r#"{
        "data": [{
            "ticker": "AAPL",
            "timestamp": 1735689600,
            "quantile_10": 190.0,
            "quantile_50": 200.0,
            "quantile_90": 210.0
        }],
        "timestamp": "2025-01-01T00:00:00Z"
    }"#;

    let response = reqwest::Client::new()
        .post(app.url("/predictions"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(save_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_predictions_query_returns_bad_request_for_invalid_url_encoding() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .get(app.url("/predictions?tickers_and_timestamps=%"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_predictions_query_returns_bad_request_for_invalid_json() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let encoded = urlencoding::encode("not-json");
    let response = reqwest::Client::new()
        .get(app.url(&format!("/predictions?tickers_and_timestamps={}", encoded)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_predictions_query_returns_empty_json_array_when_no_rows_match() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;
    let client = reqwest::Client::new();

    let save_payload = r#"{
        "data": [{
            "ticker": "AAPL",
            "timestamp": 1735689600,
            "quantile_10": 190.0,
            "quantile_50": 200.0,
            "quantile_90": 210.0
        }],
        "timestamp": "2025-01-01T00:00:00Z"
    }"#;

    let response = client
        .post(app.url("/predictions"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(save_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let encoded = urlencoding::encode("[{\"ticker\":\"MSFT\",\"timestamp\":1735689600.0}]");
    let response = client
        .get(app.url(&format!("/predictions?tickers_and_timestamps={}", encoded)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "[]");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_predictions_query_returns_internal_server_error_when_storage_query_fails() {
    let (app, _env_guard) = spawn_app_with_unreachable_s3("http://127.0.0.1:1".to_string()).await;

    let encoded = urlencoding::encode("[{\"ticker\":\"AAPL\",\"timestamp\":1735689600.0}]");
    let response = reqwest::Client::new()
        .get(app.url(&format!("/predictions?tickers_and_timestamps={}", encoded)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_portfolios_save_and_get_round_trip() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;
    let client = reqwest::Client::new();

    let save_payload = r#"{
        "data": [{
            "ticker": "AAPL",
            "timestamp": 1735689600.0,
            "side": "long",
            "dollar_amount": 10000.0,
            "action": "buy"
        }],
        "timestamp": "2025-01-01T00:00:00Z"
    }"#;

    let response = client
        .post(app.url("/portfolios"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(save_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .get(app.url("/portfolios?timestamp=2025-01-01T00:00:00Z"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.text().await.unwrap();
    assert!(body.contains("AAPL"));
    assert!(body.contains("BUY"));

    let response = client.get(app.url("/portfolios")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_portfolios_save_returns_internal_server_error_when_s3_upload_fails() {
    let (app, _env_guard) = spawn_app_with_unreachable_s3("http://127.0.0.1:1".to_string()).await;

    let save_payload = r#"{
        "data": [{
            "ticker": "AAPL",
            "timestamp": 1735689600.0,
            "side": "long",
            "dollar_amount": 10000.0,
            "action": "buy"
        }],
        "timestamp": "2025-01-01T00:00:00Z"
    }"#;

    let response = reqwest::Client::new()
        .post(app.url("/portfolios"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(save_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_portfolios_get_returns_not_found_for_first_run_without_files() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .get(app.url("/portfolios"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_portfolios_get_returns_not_found_when_portfolio_file_is_empty() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;
    let client = reqwest::Client::new();

    let empty_save_payload = r#"{
        "data": [],
        "timestamp": "2025-01-01T00:00:00Z"
    }"#;

    let response = client
        .post(app.url("/portfolios"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(empty_save_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .get(app.url("/portfolios?timestamp=2025-01-01T00:00:00Z"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_get_returns_csv_content() {
    let (endpoint, s3, _env_guard) = setup_test_bucket().await;

    put_test_object(
        &s3,
        "equity/details/categories.csv",
        b"ticker,sector,industry\nAAPL,Technology,Consumer Electronics\n".to_vec(),
    )
    .await;

    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .get(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("text/csv"));

    let body = response.text().await.unwrap();
    assert!(body.contains("AAPL"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_get_returns_internal_server_error_when_csv_is_missing() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .get(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_and_query_round_trip() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

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
                "adjusted": true,
                "queryCount": 1,
                "request_id": "test",
                "resultsCount": 1,
                "status": "OK",
                "results": [{
                    "T": "AAPL",
                    "c": 105.0,
                    "h": 110.0,
                    "l": 99.0,
                    "n": 1000,
                    "o": 100.0,
                    "t": 1735689600,
                    "v": 2000000.0,
                    "vw": 104.0
                }]
            }"#,
        )
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;
    let client = reqwest::Client::new();

    let response = client
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .get(app.url(
            "/equity-bars?tickers=AAPL&start_timestamp=2025-01-01T00:00:00Z&end_timestamp=2025-01-01T00:00:00Z",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(content_type, "application/octet-stream");

    let body = response.bytes().await.unwrap();
    let dataframe = ParquetReader::new(std::io::Cursor::new(body.to_vec()))
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
async fn test_equity_bars_sync_returns_no_content_when_api_has_no_results() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

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
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_internal_server_error_for_invalid_json() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

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
async fn test_equity_bars_sync_returns_bad_gateway_for_unparseable_results() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

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
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_internal_server_error_when_api_request_fails() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
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
async fn test_equity_bars_query_returns_internal_server_error_for_invalid_ticker() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .get(app.url(
            "/equity-bars?tickers=AAPL;DROP&start_timestamp=2025-01-01T00:00:00Z&end_timestamp=2025-01-01T00:00:00Z",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_query_without_ticker_filter_returns_data() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

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
    let client = reqwest::Client::new();

    let response = client
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Query without tickers — covers "No tickers specified" branch
    let response = client
        .get(app.url(
            "/equity-bars?start_timestamp=2025-01-01T00:00:00Z&end_timestamp=2025-01-01T00:00:00Z",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.bytes().await.unwrap();
    let dataframe = ParquetReader::new(std::io::Cursor::new(body.to_vec()))
        .finish()
        .unwrap();
    assert_eq!(dataframe.height(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_query_with_empty_tickers_param_returns_data() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

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
    let client = reqwest::Client::new();

    let response = client
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Query with empty tickers — covers "Ticker list was empty" branch
    let response = client
        .get(app.url(
            "/equity-bars?tickers=&start_timestamp=2025-01-01T00:00:00Z&end_timestamp=2025-01-01T00:00:00Z",
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_bars_sync_returns_internal_server_error_for_api_error_status() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

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
async fn test_equity_bars_sync_returns_bad_gateway_when_s3_upload_fails() {
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

    // Working Massive API but broken S3 → parse succeeds, upload fails
    let (app, _env_guard) = spawn_app_with_unreachable_s3(massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-bars"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"date":"2025-01-01T00:00:00Z"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_portfolios_get_returns_internal_server_error_when_storage_query_fails() {
    // DuckDB connects to unreachable S3 → connection error (not "not found")
    let (app, _env_guard) = spawn_app_with_unreachable_s3("http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .get(app.url("/portfolios"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_and_get_round_trip() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v3/reference/tickers")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("market".into(), "stocks".into()),
            Matcher::UrlEncoded("active".into(), "true".into()),
            Matcher::UrlEncoded("limit".into(), "1000".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body(
            r#"{
                "results": [{
                    "ticker": "AAPL",
                    "type": "CS",
                    "sector": "Technology",
                    "industry": "Consumer Electronics"
                }],
                "status": "OK"
            }"#,
        )
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;
    let client = reqwest::Client::new();

    let response = client
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client.get(app.url("/equity-details")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.text().await.unwrap();
    assert!(body.contains("AAPL"));
    assert!(body.contains("TECHNOLOGY"));
    assert!(body.contains("CONSUMER ELECTRONICS"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_with_pagination() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let next_url = format!("{}/v3/reference/tickers/next", massive_server.url());

    let first_body = format!(
        r#"{{
            "results": [{{"ticker": "AAPL", "type": "CS", "sector": "Technology", "industry": "Hardware"}}],
            "next_url": "{}",
            "status": "OK"
        }}"#,
        next_url
    );

    let _mock_page1 = massive_server
        .mock("GET", "/v3/reference/tickers")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("market".into(), "stocks".into()),
            Matcher::UrlEncoded("active".into(), "true".into()),
            Matcher::UrlEncoded("limit".into(), "1000".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body(first_body)
        .create_async()
        .await;

    let _mock_page2 = massive_server
        .mock("GET", "/v3/reference/tickers/next")
        .match_query(Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()))
        .with_status(200)
        .with_body(
            r#"{
                "results": [{"ticker": "MSFT", "type": "CS", "sector": "Technology", "industry": "Software"}],
                "status": "OK"
            }"#,
        )
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;
    let client = reqwest::Client::new();

    let response = client
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client.get(app.url("/equity-details")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.text().await.unwrap();
    assert!(body.contains("AAPL"));
    assert!(body.contains("MSFT"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_returns_no_content_when_api_has_no_results() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v3/reference/tickers")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("market".into(), "stocks".into()),
            Matcher::UrlEncoded("active".into(), "true".into()),
            Matcher::UrlEncoded("limit".into(), "1000".into()),
            Matcher::UrlEncoded("apiKey".into(), "test-api-key".into()),
        ]))
        .with_status(200)
        .with_body(r#"{"results": [], "status": "OK"}"#)
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_returns_internal_server_error_when_api_request_fails() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;
    let (app, _env_guard) = spawn_app(&endpoint, "http://127.0.0.1:1".to_string()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_returns_bad_gateway_when_s3_upload_fails() {
    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v3/reference/tickers")
        .match_query(Matcher::Any)
        .with_status(200)
        .with_body(
            r#"{
                "results": [{"ticker": "AAPL", "type": "CS", "sector": "Technology", "industry": "Hardware"}],
                "status": "OK"
            }"#,
        )
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app_with_unreachable_s3(massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_returns_internal_server_error_for_api_error_status() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v3/reference/tickers")
        .match_query(Matcher::Any)
        .with_status(500)
        .with_body("Internal Server Error")
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_returns_internal_server_error_for_invalid_json() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v3/reference/tickers")
        .match_query(Matcher::Any)
        .with_status(200)
        .with_body("not-json")
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;

    let response = reqwest::Client::new()
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_equity_details_sync_filters_non_equity_types() {
    let (endpoint, _s3, _env_guard) = setup_test_bucket().await;

    let mut massive_server = Server::new_async().await;
    let _mock = massive_server
        .mock("GET", "/v3/reference/tickers")
        .match_query(Matcher::Any)
        .with_status(200)
        .with_body(
            r#"{
                "results": [
                    {"ticker": "AAPL", "type": "CS", "sector": "Technology", "industry": "Hardware"},
                    {"ticker": "XYZ", "type": "WARRANT", "sector": "Finance", "industry": "Banking"},
                    {"ticker": "DEF", "type": "ETF", "sector": "Finance", "industry": "Funds"},
                    {"ticker": "GHI", "type": "ADRC", "sector": null, "industry": null}
                ],
                "status": "OK"
            }"#,
        )
        .create_async()
        .await;

    let (app, _env_guard) = spawn_app(&endpoint, massive_server.url()).await;
    let client = reqwest::Client::new();

    let response = client
        .post(app.url("/equity-details"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client.get(app.url("/equity-details")).send().await.unwrap();
    let body = response.text().await.unwrap();
    assert!(body.contains("AAPL"));
    assert!(!body.contains("XYZ"));
    assert!(!body.contains("DEF"));
    assert!(body.contains("GHI"));
    assert!(body.contains("NOT AVAILABLE"));
}
