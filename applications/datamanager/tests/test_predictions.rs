mod common;

use chrono::{TimeZone, Utc};
use datamanager::data::Prediction;
use datamanager::predictions::{QueryParameters, SavePayload};
use datamanager::storage::PredictionQuery;
use polars::prelude::*;
use serde_json;
use std::io::Cursor;
use urlencoding::{decode, encode};

#[test]
fn test_save_payload_deserialization_valid() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890,
                "quantile_10": 95.0,
                "quantile_50": 100.0,
                "quantile_90": 105.0
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data.len(), 1);
    assert_eq!(payload.data[0].ticker, "AAPL");
    assert_eq!(payload.data[0].timestamp, 1234567890);
    assert_eq!(payload.data[0].quantile_10, 95.0);
    assert_eq!(payload.data[0].quantile_50, 100.0);
    assert_eq!(payload.data[0].quantile_90, 105.0);
    assert_eq!(
        payload.timestamp,
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap()
    );
}

#[test]
fn test_save_payload_deserialization_multiple_predictions() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890,
                "quantile_10": 95.0,
                "quantile_50": 100.0,
                "quantile_90": 105.0
            },
            {
                "ticker": "GOOGL",
                "timestamp": 1234567890,
                "quantile_10": 1995.0,
                "quantile_50": 2000.0,
                "quantile_90": 2005.0
            },
            {
                "ticker": "MSFT",
                "timestamp": 1234567890,
                "quantile_10": 295.0,
                "quantile_50": 300.0,
                "quantile_90": 305.0
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data.len(), 3);
    assert_eq!(payload.data[0].ticker, "AAPL");
    assert_eq!(payload.data[1].ticker, "GOOGL");
    assert_eq!(payload.data[2].ticker, "MSFT");
}

#[test]
fn test_save_payload_deserialization_empty_data() {
    let json = r#"{
        "data": [],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data.len(), 0);
}

#[test]
fn test_save_payload_deserialization_missing_data_field() {
    let json = r#"{
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_save_payload_deserialization_missing_timestamp_field() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890,
                "quantile_10": 95.0,
                "quantile_50": 100.0,
                "quantile_90": 105.0
            }
        ]
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_save_payload_deserialization_invalid_timestamp() {
    let json = r#"{
        "data": [],
        "timestamp": "not-a-date"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_save_payload_deserialization_different_timestamps() {
    let test_cases = vec![
        (
            r#"{"data": [], "timestamp": "2024-12-31T23:59:59Z"}"#,
            Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap(),
        ),
        (
            r#"{"data": [], "timestamp": "2024-01-01T00:00:00Z"}"#,
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        ),
        (
            r#"{"data": [], "timestamp": "2024-02-29T12:00:00Z"}"#,
            Utc.with_ymd_and_hms(2024, 2, 29, 12, 0, 0).unwrap(),
        ),
    ];

    for (json, expected_timestamp) in test_cases {
        let result: Result<SavePayload, _> = serde_json::from_str(json);
        assert!(result.is_ok(), "Failed to parse: {}", json);

        let payload = result.unwrap();
        assert_eq!(
            payload.timestamp, expected_timestamp,
            "Timestamp mismatch for: {}",
            json
        );
    }
}

#[test]
fn test_save_payload_deserialization_quantile_values() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890,
                "quantile_10": 0.0,
                "quantile_50": 50.5,
                "quantile_90": 100.0
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data[0].quantile_10, 0.0);
    assert_eq!(payload.data[0].quantile_50, 50.5);
    assert_eq!(payload.data[0].quantile_90, 100.0);
}

#[test]
fn test_save_payload_deserialization_negative_quantiles() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890,
                "quantile_10": -10.0,
                "quantile_50": -5.0,
                "quantile_90": 0.0
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data[0].quantile_10, -10.0);
    assert_eq!(payload.data[0].quantile_50, -5.0);
    assert_eq!(payload.data[0].quantile_90, 0.0);
}

#[test]
fn test_save_payload_deserialization_large_quantile_values() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890,
                "quantile_10": 999999.99,
                "quantile_50": 1000000.00,
                "quantile_90": 1000000.01
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data[0].quantile_10, 999999.99);
    assert_eq!(payload.data[0].quantile_50, 1000000.00);
    assert_eq!(payload.data[0].quantile_90, 1000000.01);
}

#[test]
fn test_query_parameters_deserialization_valid_url_encoded() {
    let json_str = r#"[{"ticker":"AAPL","timestamp":1234567890.0}]"#;
    let encoded = encode(json_str);
    let json = format!(r#"{{"tickers_and_timestamps":"{}"}}"#, encoded);

    let result: Result<QueryParameters, _> = serde_json::from_str(&json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_deserialization_plain_string() {
    let json = r#"{"tickers_and_timestamps":"test"}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let params = result.unwrap();
    assert_eq!(params.tickers_and_timestamps, "test");
}

#[test]
fn test_query_parameters_deserialization_empty_string() {
    let json = r#"{"tickers_and_timestamps":""}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let params = result.unwrap();
    assert_eq!(params.tickers_and_timestamps, "");
}

#[test]
fn test_query_parameters_deserialization_missing_field() {
    let json = r#"{}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_prediction_query_deserialization_valid() {
    let json = r#"{
        "ticker": "AAPL",
        "timestamp": 1234567890.0
    }"#;

    let result: Result<PredictionQuery, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.ticker, "AAPL");
    assert_eq!(query.timestamp, 1234567890.0);
}

#[test]
fn test_prediction_query_deserialization_array() {
    let json = r#"[
        {
            "ticker": "AAPL",
            "timestamp": 1234567890.0
        },
        {
            "ticker": "GOOGL",
            "timestamp": 1234567891.0
        }
    ]"#;

    let result: Result<Vec<PredictionQuery>, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let queries = result.unwrap();
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0].ticker, "AAPL");
    assert_eq!(queries[0].timestamp, 1234567890.0);
    assert_eq!(queries[1].ticker, "GOOGL");
    assert_eq!(queries[1].timestamp, 1234567891.0);
}

#[test]
fn test_prediction_query_deserialization_empty_array() {
    let json = r#"[]"#;

    let result: Result<Vec<PredictionQuery>, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let queries = result.unwrap();
    assert_eq!(queries.len(), 0);
}

#[test]
fn test_prediction_query_deserialization_missing_ticker() {
    let json = r#"{
        "timestamp": 1234567890.0
    }"#;

    let result: Result<PredictionQuery, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_prediction_query_deserialization_missing_timestamp() {
    let json = r#"{
        "ticker": "AAPL"
    }"#;

    let result: Result<PredictionQuery, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_prediction_query_deserialization_integer_timestamp() {
    let json = r#"{
        "ticker": "AAPL",
        "timestamp": 1234567890
    }"#;

    let result: Result<PredictionQuery, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.timestamp, 1234567890.0);
}

#[test]
fn test_url_decoding_simple_string() {
    let encoded = encode("test");
    let decoded = decode(&encoded);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), "test");
}

#[test]
fn test_url_decoding_json_string() {
    let json_str = r#"[{"ticker":"AAPL","timestamp":1234567890.0}]"#;
    let encoded = encode(json_str);
    let decoded = decode(&encoded);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), json_str);
}

#[test]
fn test_url_decoding_special_characters() {
    let test_str = "ticker=AAPL&timestamp=123";
    let encoded = encode(test_str);
    let decoded = decode(&encoded);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), test_str);
}

#[test]
fn test_url_decoding_spaces() {
    let test_str = "hello world";
    let encoded = encode(test_str);
    let decoded = decode(&encoded);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), test_str);
}

#[test]
fn test_url_decoding_empty_string() {
    let encoded = encode("");
    let decoded = decode(&encoded);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), "");
}

#[test]
fn test_url_decoding_already_decoded() {
    let test_str = "AAPL";
    let decoded = decode(test_str);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), test_str);
}

#[test]
fn test_url_encoding_then_decoding_roundtrip() {
    let original = r#"[{"ticker":"AAPL","timestamp":1234567890.0},{"ticker":"GOOGL","timestamp":1234567890.0}]"#;
    let encoded = encode(original);
    let decoded = decode(&encoded);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), original);
}

#[test]
fn test_json_parsing_after_url_decoding() {
    let json_str = r#"[{"ticker":"AAPL","timestamp":1234567890.0}]"#;
    let encoded = encode(json_str);
    let decoded = decode(&encoded).unwrap().into_owned();

    let result: Result<Vec<PredictionQuery>, _> = serde_json::from_str(&decoded);

    assert!(result.is_ok());

    let queries = result.unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].ticker, "AAPL");
}

#[test]
fn test_json_parsing_multiple_queries_after_decoding() {
    let json_str = r#"[{"ticker":"AAPL","timestamp":1234567890.0},{"ticker":"GOOGL","timestamp":1234567891.0}]"#;
    let encoded = encode(json_str);
    let decoded = decode(&encoded).unwrap().into_owned();

    let result: Result<Vec<PredictionQuery>, _> = serde_json::from_str(&decoded);

    assert!(result.is_ok());

    let queries = result.unwrap();
    assert_eq!(queries.len(), 2);
}

#[test]
fn test_json_parsing_empty_array_after_decoding() {
    let json_str = r#"[]"#;
    let encoded = encode(json_str);
    let decoded = decode(&encoded).unwrap().into_owned();

    let result: Result<Vec<PredictionQuery>, _> = serde_json::from_str(&decoded);

    assert!(result.is_ok());

    let queries = result.unwrap();
    assert_eq!(queries.len(), 0);
}

#[test]
fn test_dataframe_to_json_valid_predictions() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "timestamp" => &[1234567890_i64, 1234567890_i64],
        "quantile_10" => &[95.0, 1995.0],
        "quantile_50" => &[100.0, 2000.0],
        "quantile_90" => &[105.0, 2005.0],
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    let result = JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone());

    assert!(result.is_ok());

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    assert!(json_string.contains("AAPL"));
    assert!(json_string.contains("GOOGL"));
    assert!(json_string.contains("95"));
    assert!(json_string.contains("100"));
    assert!(json_string.contains("105"));
}

#[test]
fn test_dataframe_to_json_single_prediction() {
    let df = df! {
        "ticker" => &["AAPL"],
        "timestamp" => &[1234567890_i64],
        "quantile_10" => &[95.0],
        "quantile_50" => &[100.0],
        "quantile_90" => &[105.0],
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    let result = JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone());

    assert!(result.is_ok());

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    assert!(json_string.contains("AAPL"));
}

#[test]
fn test_dataframe_to_json_empty_dataframe() {
    let df = df! {
        "ticker" => Vec::<&str>::new(),
        "timestamp" => Vec::<i64>::new(),
        "quantile_10" => Vec::<f64>::new(),
        "quantile_50" => Vec::<f64>::new(),
        "quantile_90" => Vec::<f64>::new(),
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    let result = JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone());

    assert!(result.is_ok());

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    assert!(json_string.contains("["));
    assert!(json_string.contains("]"));
}

#[test]
fn test_dataframe_to_json_multiple_predictions() {
    let tickers = vec!["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"];
    let timestamps = vec![1234567890_i64; 5];
    let q10 = vec![95.0, 1995.0, 295.0, 195.0, 3095.0];
    let q50 = vec![100.0, 2000.0, 300.0, 200.0, 3100.0];
    let q90 = vec![105.0, 2005.0, 305.0, 205.0, 3105.0];

    let df = df! {
        "ticker" => tickers,
        "timestamp" => timestamps,
        "quantile_10" => q10,
        "quantile_50" => q50,
        "quantile_90" => q90,
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    let result = JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone());

    assert!(result.is_ok());

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    assert!(json_string.contains("AAPL"));
    assert!(json_string.contains("GOOGL"));
    assert!(json_string.contains("MSFT"));
    assert!(json_string.contains("TSLA"));
    assert!(json_string.contains("AMZN"));
}

#[test]
fn test_dataframe_to_json_preserves_numeric_precision() {
    let df = df! {
        "ticker" => &["AAPL"],
        "timestamp" => &[1234567890_i64],
        "quantile_10" => &[95.123456],
        "quantile_50" => &[100.654321],
        "quantile_90" => &[105.999999],
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    let result = JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone());

    assert!(result.is_ok());

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    assert!(json_string.contains("95.123456"));
    assert!(json_string.contains("100.654321"));
    assert!(json_string.contains("105.999999"));
}

#[test]
fn test_empty_result_handling() {
    let df = df! {
        "ticker" => Vec::<&str>::new(),
        "timestamp" => Vec::<i64>::new(),
        "quantile_10" => Vec::<f64>::new(),
        "quantile_50" => Vec::<f64>::new(),
        "quantile_90" => Vec::<f64>::new(),
    }
    .unwrap();

    assert_eq!(df.height(), 0);

    let empty_json = "[]";
    assert!(empty_json.starts_with('['));
    assert!(empty_json.ends_with(']'));
    assert_eq!(empty_json.len(), 2);
}

#[test]
fn test_prediction_struct_fields() {
    let prediction = Prediction {
        ticker: "TEST".to_string(),
        timestamp: 9999999999,
        quantile_10: 50.0,
        quantile_50: 75.0,
        quantile_90: 100.0,
    };

    assert_eq!(prediction.ticker, "TEST");
    assert_eq!(prediction.timestamp, 9999999999);
    assert_eq!(prediction.quantile_10, 50.0);
    assert_eq!(prediction.quantile_50, 75.0);
    assert_eq!(prediction.quantile_90, 100.0);
}

#[test]
fn test_prediction_data_creation() {
    let predictions = vec![
        common::sample_prediction(),
        Prediction {
            ticker: "GOOGL".to_string(),
            timestamp: 1234567890,
            quantile_10: 1995.0,
            quantile_50: 2000.0,
            quantile_90: 2005.0,
        },
    ];

    assert_eq!(predictions.len(), 2);
    assert_eq!(predictions[0].ticker, "AAPL");
    assert_eq!(predictions[1].ticker, "GOOGL");
}

#[test]
fn test_dataframe_height_check_for_empty_result() {
    let df_empty = df! {
        "ticker" => Vec::<&str>::new(),
        "timestamp" => Vec::<i64>::new(),
        "quantile_10" => Vec::<f64>::new(),
        "quantile_50" => Vec::<f64>::new(),
        "quantile_90" => Vec::<f64>::new(),
    }
    .unwrap();

    assert_eq!(df_empty.height(), 0);

    let df_with_data = df! {
        "ticker" => &["AAPL"],
        "timestamp" => &[1234567890_i64],
        "quantile_10" => &[95.0],
        "quantile_50" => &[100.0],
        "quantile_90" => &[105.0],
    }
    .unwrap();

    assert_eq!(df_with_data.height(), 1);
}

#[test]
fn test_json_format_output_is_valid() {
    let df = df! {
        "ticker" => &["AAPL"],
        "timestamp" => &[1234567890_i64],
        "quantile_10" => &[95.0],
        "quantile_50" => &[100.0],
        "quantile_90" => &[105.0],
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone())
        .unwrap();

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    let parsed: Result<serde_json::Value, _> = serde_json::from_str(&json_string);
    assert!(parsed.is_ok(), "JSON output should be valid JSON");
}

#[test]
fn test_url_decoding_with_plus_signs() {
    let test_str = "hello+world";
    let decoded = decode(test_str);

    assert!(decoded.is_ok());
}

#[test]
fn test_url_decoding_with_percent_encoding() {
    let test_str = "hello%20world";
    let decoded = decode(test_str);

    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), "hello world");
}

#[test]
fn test_prediction_query_with_special_ticker_characters() {
    let json = r#"{
        "ticker": "BRK.A",
        "timestamp": 1234567890.0
    }"#;

    let result: Result<PredictionQuery, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let query = result.unwrap();
    assert_eq!(query.ticker, "BRK.A");
}

#[test]
fn test_complex_url_encode_decode_json_workflow() {
    let json_str = r#"[{"ticker":"AAPL","timestamp":1234567890.0},{"ticker":"GOOGL","timestamp":1234567891.0}]"#;
    let encoded = encode(json_str);
    let decoded = decode(&encoded).unwrap().into_owned();
    let parsed: Vec<PredictionQuery> = serde_json::from_str(&decoded).unwrap();

    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].ticker, "AAPL");
    assert_eq!(parsed[1].ticker, "GOOGL");
}
