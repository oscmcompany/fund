mod common;

use chrono::NaiveDate;
use fund::data::{
    equity_bars::{seed, SeedSource, SeedTarget},
    state::{MassiveSecrets, State},
};
use mockito::{Matcher, Server};
use polars::prelude::*;
use serial_test::serial;
use std::io::Cursor;

use common::{create_test_s3_client, setup_test_bucket, test_bucket_name};

const SINGLE_BAR_BODY: &str = r#"{
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
        "t": 1735689600000,
        "v": 2000000.0,
        "vw": 104.0
    }]
}"#;

async fn create_state(massive_base: String, s3_endpoint: &str) -> State {
    let s3_client = create_test_s3_client(s3_endpoint).await;
    State::new(
        reqwest::Client::new(),
        MassiveSecrets {
            base: massive_base,
            key: "test-api-key".to_string(),
        },
        s3_client,
        test_bucket_name(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_seed_start_after_end_returns_error() {
    let (endpoint, _s3) = setup_test_bucket().await;
    let state = create_state("http://127.0.0.1:1".to_string(), &endpoint).await;

    let result = seed(
        &state,
        NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        SeedSource::Massive,
        SeedTarget::S3,
    )
    .await;

    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn test_seed_writes_s3_and_skips_weekends() {
    let (endpoint, s3) = setup_test_bucket().await;

    // 2025-01-03 is Friday and 2025-01-06 is Monday; 2025-01-04/05 are weekend.
    let mut massive_server = Server::new_async().await;
    for date in ["2025-01-03", "2025-01-06"] {
        massive_server
            .mock(
                "GET",
                format!("/v2/aggs/grouped/locale/us/market/stocks/{date}").as_str(),
            )
            .match_query(Matcher::Any)
            .with_status(200)
            .with_body(SINGLE_BAR_BODY)
            .create_async()
            .await;
    }

    let state = create_state(massive_server.url(), &endpoint).await;

    let summary = seed(
        &state,
        NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
        SeedSource::Massive,
        SeedTarget::S3,
    )
    .await
    .unwrap();

    assert_eq!(summary.days_processed, 2);
    assert_eq!(summary.days_skipped_non_trading, 2);
    assert_eq!(summary.days_failed, 0);
    assert_eq!(summary.total_bars, 2);

    // The Friday partition must exist with an Int64 millisecond timestamp.
    let object = s3
        .get_object()
        .bucket(test_bucket_name())
        .key("data/equity/bars/year=2025/month=01/day=03/data.parquet")
        .send()
        .await
        .expect("Friday partition should exist");
    let bytes = object.body.collect().await.unwrap().into_bytes();
    let dataframe = ParquetReader::new(Cursor::new(bytes.to_vec()))
        .finish()
        .unwrap();

    assert_eq!(dataframe.height(), 1);
    assert_eq!(
        dataframe.column("ticker").unwrap().str().unwrap().get(0),
        Some("AAPL")
    );
    assert_eq!(
        dataframe.column("timestamp").unwrap().dtype(),
        &DataType::Int64
    );
}
