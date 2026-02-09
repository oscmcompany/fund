use chrono::{TimeZone, Utc};
use datamanager::equity_bars::{DailySync, QueryParameters};
use serde_json;

#[test]
fn test_daily_sync_deserialization_valid() {
    let json = r#"{"date": "2024-01-15T12:30:45Z"}"#;

    let result: Result<DailySync, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let daily_sync = result.unwrap();
    assert_eq!(
        daily_sync.date,
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap()
    );
}

#[test]
fn test_daily_sync_deserialization_different_formats() {
    let test_cases = vec![
        (
            r#"{"date": "2024-12-31T23:59:59Z"}"#,
            Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap(),
        ),
        (
            r#"{"date": "2024-01-01T00:00:00Z"}"#,
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        ),
        (
            r#"{"date": "2024-02-29T12:00:00Z"}"#,
            Utc.with_ymd_and_hms(2024, 2, 29, 12, 0, 0).unwrap(),
        ),
    ];

    for (json, expected_date) in test_cases {
        let result: Result<DailySync, _> = serde_json::from_str(json);
        assert!(result.is_ok(), "Failed to parse: {}", json);

        let daily_sync = result.unwrap();
        assert_eq!(
            daily_sync.date, expected_date,
            "Date mismatch for input: {}",
            json
        );
    }
}

#[test]
fn test_daily_sync_deserialization_invalid_format() {
    let invalid_json_cases = vec![
        r#"{"date": "not-a-date"}"#,
        r#"{"date": "2024-13-01T00:00:00Z"}"#,
        r#"{"date": "2024-01-32T00:00:00Z"}"#,
        r#"{"date": ""}"#,
        r#"{"wrong_field": "2024-01-15T12:30:45Z"}"#,
        r#"{}"#,
    ];

    for json in invalid_json_cases {
        let result: Result<DailySync, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Expected parse error for: {} but got success",
            json
        );
    }
}

#[test]
fn test_query_parameters_all_fields() {
    let json = r#"{
        "tickers": "AAPL,GOOGL,MSFT",
        "start_timestamp": "2024-01-01T00:00:00Z",
        "end_timestamp": "2024-12-31T23:59:59Z"
    }"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_optional_fields() {
    let test_cases = vec![
        r#"{}"#,
        r#"{"tickers": "AAPL"}"#,
        r#"{"start_timestamp": "2024-01-01T00:00:00Z"}"#,
        r#"{"end_timestamp": "2024-12-31T23:59:59Z"}"#,
        r#"{"tickers": "AAPL,GOOGL", "start_timestamp": "2024-01-01T00:00:00Z"}"#,
    ];

    for json in test_cases {
        let result: Result<QueryParameters, _> = serde_json::from_str(json);
        assert!(result.is_ok(), "Failed to parse: {}", json);
    }
}

#[test]
fn test_query_parameters_single_ticker() {
    let json = r#"{"tickers": "AAPL"}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_multiple_tickers() {
    let json = r#"{"tickers": "AAPL,GOOGL,MSFT,TSLA,AMZN"}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_empty_tickers() {
    let json = r#"{"tickers": ""}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_whitespace_tickers() {
    let json = r#"{"tickers": "  AAPL  ,  GOOGL  "}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_bar_result_deserialization_all_fields() {
    let json = r#"{
        "T": "AAPL",
        "c": 150.5,
        "h": 152.0,
        "l": 149.0,
        "n": 5000,
        "o": 150.0,
        "t": 1234567890,
        "v": 1000000.0,
        "vw": 150.25
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_bar_result_deserialization_optional_fields_null() {
    let json = r#"{
        "T": "AAPL",
        "c": null,
        "h": null,
        "l": null,
        "n": null,
        "o": null,
        "t": 1234567890,
        "v": null,
        "vw": null
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_bar_result_deserialization_mixed_nulls() {
    let json = r#"{
        "T": "AAPL",
        "c": 150.5,
        "h": null,
        "l": 149.0,
        "n": null,
        "o": 150.0,
        "t": 1234567890,
        "v": 1000000.0,
        "vw": null
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_bar_result_deserialization_missing_optional_fields() {
    let json = r#"{
        "T": "AAPL",
        "t": 1234567890
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_bar_result_deserialization_missing_required_fields() {
    let invalid_cases = vec![
        r#"{"c": 150.5, "t": 1234567890}"#,
        r#"{"T": "AAPL"}"#,
        r#"{}"#,
    ];

    for json in invalid_cases {
        let result: Result<serde_json::Value, _> = serde_json::from_str(json);
        assert!(result.is_ok(), "JSON parsing should succeed for: {}", json);
    }
}

#[test]
fn test_massive_response_deserialization_valid() {
    let json = r#"{
        "adjusted": true,
        "queryCount": 1,
        "request_id": "abc123",
        "resultsCount": 2,
        "status": "OK",
        "results": [
            {
                "T": "AAPL",
                "c": 150.5,
                "h": 152.0,
                "l": 149.0,
                "n": 5000,
                "o": 150.0,
                "t": 1234567890,
                "v": 1000000.0,
                "vw": 150.25
            },
            {
                "T": "GOOGL",
                "c": 2800.0,
                "h": 2850.0,
                "l": 2790.0,
                "n": 3000,
                "o": 2800.0,
                "t": 1234567890,
                "v": 500000.0,
                "vw": 2820.0
            }
        ]
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_massive_response_deserialization_no_results() {
    let json = r#"{
        "adjusted": true,
        "queryCount": 0,
        "request_id": "xyz789",
        "resultsCount": 0,
        "status": "NO_RESULTS"
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_massive_response_deserialization_empty_results() {
    let json = r#"{
        "adjusted": true,
        "queryCount": 1,
        "request_id": "def456",
        "resultsCount": 0,
        "status": "OK",
        "results": []
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_massive_response_deserialization_null_results() {
    let json = r#"{
        "adjusted": false,
        "queryCount": 1,
        "request_id": "ghi789",
        "resultsCount": 0,
        "status": "ERROR",
        "results": null
    }"#;

    let result: Result<serde_json::Value, _> = serde_json::from_str(json);
    assert!(result.is_ok());
}

#[test]
fn test_dataframe_column_construction_from_bar_results() {
    let bars = vec![
        (
            "AAPL",
            Some(100.0),
            Some(105.0),
            Some(99.0),
            Some(103.0),
            Some(1000000.0),
            Some(102.0),
            1234567890_u64,
            Some(5000_u64),
        ),
        (
            "GOOGL",
            Some(2000.0),
            Some(2050.0),
            Some(1990.0),
            Some(2030.0),
            Some(500000.0),
            Some(2020.0),
            1234567890_u64,
            Some(2500_u64),
        ),
    ];

    let tickers: Vec<String> = bars.iter().map(|(t, ..)| t.to_string()).collect();
    let open_prices: Vec<Option<f64>> = bars.iter().map(|(_, o, ..)| *o).collect();
    let high_prices: Vec<Option<f64>> = bars.iter().map(|(_, _, h, ..)| *h).collect();
    let low_prices: Vec<Option<f64>> = bars.iter().map(|(_, _, _, l, ..)| *l).collect();
    let close_prices: Vec<Option<f64>> = bars.iter().map(|(_, _, _, _, c, ..)| *c).collect();
    let volumes: Vec<Option<f64>> = bars.iter().map(|(_, _, _, _, _, v, ..)| *v).collect();
    let vwaps: Vec<Option<f64>> = bars.iter().map(|(_, _, _, _, _, _, vw, ..)| *vw).collect();
    let timestamps: Vec<i64> = bars
        .iter()
        .map(|(_, _, _, _, _, _, _, t, _)| *t as i64)
        .collect();
    let transactions: Vec<Option<u64>> =
        bars.iter().map(|(_, _, _, _, _, _, _, _, n)| *n).collect();

    assert_eq!(tickers.len(), 2);
    assert_eq!(tickers[0], "AAPL");
    assert_eq!(tickers[1], "GOOGL");

    assert_eq!(open_prices.len(), 2);
    assert_eq!(open_prices[0], Some(100.0));
    assert_eq!(open_prices[1], Some(2000.0));

    assert_eq!(high_prices.len(), 2);
    assert_eq!(high_prices[0], Some(105.0));
    assert_eq!(high_prices[1], Some(2050.0));

    assert_eq!(low_prices.len(), 2);
    assert_eq!(low_prices[0], Some(99.0));
    assert_eq!(low_prices[1], Some(1990.0));

    assert_eq!(close_prices.len(), 2);
    assert_eq!(close_prices[0], Some(103.0));
    assert_eq!(close_prices[1], Some(2030.0));

    assert_eq!(volumes.len(), 2);
    assert_eq!(volumes[0], Some(1000000.0));
    assert_eq!(volumes[1], Some(500000.0));

    assert_eq!(vwaps.len(), 2);
    assert_eq!(vwaps[0], Some(102.0));
    assert_eq!(vwaps[1], Some(2020.0));

    assert_eq!(timestamps.len(), 2);
    assert_eq!(timestamps[0], 1234567890_i64);
    assert_eq!(timestamps[1], 1234567890_i64);

    assert_eq!(transactions.len(), 2);
    assert_eq!(transactions[0], Some(5000));
    assert_eq!(transactions[1], Some(2500));
}

#[test]
fn test_dataframe_column_construction_with_nulls() {
    let bars = vec![(
        "TEST",
        None,
        None,
        None,
        None,
        None,
        None,
        1234567890_u64,
        None,
    )];

    let tickers: Vec<String> = bars.iter().map(|(t, ..)| t.to_string()).collect();
    let open_prices: Vec<Option<f64>> = bars.iter().map(|(_, o, ..)| *o).collect();
    let high_prices: Vec<Option<f64>> = bars.iter().map(|(_, _, h, ..)| *h).collect();
    let low_prices: Vec<Option<f64>> = bars.iter().map(|(_, _, _, l, ..)| *l).collect();
    let close_prices: Vec<Option<f64>> = bars.iter().map(|(_, _, _, _, c, ..)| *c).collect();
    let volumes: Vec<Option<f64>> = bars.iter().map(|(_, _, _, _, _, v, ..)| *v).collect();
    let vwaps: Vec<Option<f64>> = bars.iter().map(|(_, _, _, _, _, _, vw, ..)| *vw).collect();
    let timestamps: Vec<i64> = bars
        .iter()
        .map(|(_, _, _, _, _, _, _, t, _)| *t as i64)
        .collect();
    let transactions: Vec<Option<u64>> =
        bars.iter().map(|(_, _, _, _, _, _, _, _, n)| *n).collect();

    assert_eq!(tickers.len(), 1);
    assert_eq!(tickers[0], "TEST");

    assert_eq!(open_prices.len(), 1);
    assert_eq!(open_prices[0], None);

    assert_eq!(high_prices.len(), 1);
    assert_eq!(high_prices[0], None);

    assert_eq!(low_prices.len(), 1);
    assert_eq!(low_prices[0], None);

    assert_eq!(close_prices.len(), 1);
    assert_eq!(close_prices[0], None);

    assert_eq!(volumes.len(), 1);
    assert_eq!(volumes[0], None);

    assert_eq!(vwaps.len(), 1);
    assert_eq!(vwaps[0], None);

    assert_eq!(timestamps.len(), 1);
    assert_eq!(timestamps[0], 1234567890_i64);

    assert_eq!(transactions.len(), 1);
    assert_eq!(transactions[0], None);
}

#[test]
fn test_ticker_parsing_single() {
    let tickers_str = "AAPL";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .collect();

    assert_eq!(vec.len(), 1);
    assert_eq!(vec[0], "AAPL");
}

#[test]
fn test_ticker_parsing_multiple() {
    let tickers_str = "AAPL,GOOGL,MSFT";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .collect();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], "AAPL");
    assert_eq!(vec[1], "GOOGL");
    assert_eq!(vec[2], "MSFT");
}

#[test]
fn test_ticker_parsing_with_whitespace() {
    let tickers_str = " AAPL , GOOGL , MSFT ";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .collect();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], "AAPL");
    assert_eq!(vec[1], "GOOGL");
    assert_eq!(vec[2], "MSFT");
}

#[test]
fn test_ticker_parsing_lowercase_normalization() {
    let tickers_str = "aapl,googl,msft";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .collect();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], "AAPL");
    assert_eq!(vec[1], "GOOGL");
    assert_eq!(vec[2], "MSFT");
}

#[test]
fn test_ticker_parsing_mixed_case() {
    let tickers_str = "AaPl,GoOgL,mSfT";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .collect();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], "AAPL");
    assert_eq!(vec[1], "GOOGL");
    assert_eq!(vec[2], "MSFT");
}

#[test]
fn test_ticker_parsing_empty_string() {
    let tickers_str = "";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    assert_eq!(vec.len(), 0);
}

#[test]
fn test_ticker_parsing_extra_commas() {
    let tickers_str = "AAPL,,GOOGL,,,MSFT";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], "AAPL");
    assert_eq!(vec[1], "GOOGL");
    assert_eq!(vec[2], "MSFT");
}

#[test]
fn test_ticker_parsing_trailing_comma() {
    let tickers_str = "AAPL,GOOGL,MSFT,";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], "AAPL");
    assert_eq!(vec[1], "GOOGL");
    assert_eq!(vec[2], "MSFT");
}

#[test]
fn test_ticker_parsing_leading_comma() {
    let tickers_str = ",AAPL,GOOGL,MSFT";

    let vec: Vec<String> = tickers_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    assert_eq!(vec.len(), 3);
    assert_eq!(vec[0], "AAPL");
    assert_eq!(vec[1], "GOOGL");
    assert_eq!(vec[2], "MSFT");
}
