mod common;

use chrono::{TimeZone, Utc};
use datamanager::data::Portfolio;
use datamanager::portfolios::{QueryParameters, SavePortfolioPayload};
use polars::prelude::*;
use serde_json;
use std::io::Cursor;

#[test]
fn test_save_portfolio_payload_deserialization_valid() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890.0,
                "side": "long",
                "dollar_amount": 10000.0,
                "action": "buy"
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data.len(), 1);
    assert_eq!(payload.data[0].ticker, "AAPL");
    assert_eq!(payload.data[0].timestamp, 1234567890.0);
    assert_eq!(payload.data[0].side, "long");
    assert_eq!(payload.data[0].dollar_amount, 10000.0);
    assert_eq!(payload.data[0].action, "buy");
    assert_eq!(
        payload.timestamp,
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap()
    );
}

#[test]
fn test_save_portfolio_payload_deserialization_multiple_positions() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890.0,
                "side": "long",
                "dollar_amount": 10000.0,
                "action": "buy"
            },
            {
                "ticker": "GOOGL",
                "timestamp": 1234567890.0,
                "side": "short",
                "dollar_amount": 5000.0,
                "action": "sell"
            },
            {
                "ticker": "MSFT",
                "timestamp": 1234567890.0,
                "side": "long",
                "dollar_amount": 7500.0,
                "action": "hold"
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data.len(), 3);
    assert_eq!(payload.data[0].ticker, "AAPL");
    assert_eq!(payload.data[1].ticker, "GOOGL");
    assert_eq!(payload.data[2].ticker, "MSFT");
}

#[test]
fn test_save_portfolio_payload_deserialization_empty_data() {
    let json = r#"{
        "data": [],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data.len(), 0);
}

#[test]
fn test_save_portfolio_payload_deserialization_missing_data_field() {
    let json = r#"{
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_save_portfolio_payload_deserialization_missing_timestamp_field() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890.0,
                "side": "long",
                "dollar_amount": 10000.0,
                "action": "buy"
            }
        ]
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_save_portfolio_payload_deserialization_invalid_timestamp() {
    let json = r#"{
        "data": [],
        "timestamp": "not-a-date"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_save_portfolio_payload_deserialization_different_timestamps() {
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
        let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);
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
fn test_save_portfolio_payload_deserialization_various_sides() {
    let sides = vec!["long", "short", "LONG", "SHORT", "Long", "Short"];

    for side in sides {
        let json = format!(
            r#"{{
                "data": [
                    {{
                        "ticker": "AAPL",
                        "timestamp": 1234567890.0,
                        "side": "{}",
                        "dollar_amount": 10000.0,
                        "action": "buy"
                    }}
                ],
                "timestamp": "2024-01-15T12:30:45Z"
            }}"#,
            side
        );

        let result: Result<SavePortfolioPayload, _> = serde_json::from_str(&json);
        assert!(result.is_ok(), "Failed to parse side: {}", side);

        let payload = result.unwrap();
        assert_eq!(payload.data[0].side, side);
    }
}

#[test]
fn test_save_portfolio_payload_deserialization_various_actions() {
    let actions = vec!["buy", "sell", "hold", "BUY", "SELL", "HOLD"];

    for action in actions {
        let json = format!(
            r#"{{
                "data": [
                    {{
                        "ticker": "AAPL",
                        "timestamp": 1234567890.0,
                        "side": "long",
                        "dollar_amount": 10000.0,
                        "action": "{}"
                    }}
                ],
                "timestamp": "2024-01-15T12:30:45Z"
            }}"#,
            action
        );

        let result: Result<SavePortfolioPayload, _> = serde_json::from_str(&json);
        assert!(result.is_ok(), "Failed to parse action: {}", action);

        let payload = result.unwrap();
        assert_eq!(payload.data[0].action, action);
    }
}

#[test]
fn test_save_portfolio_payload_deserialization_negative_dollar_amount() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890.0,
                "side": "long",
                "dollar_amount": -5000.0,
                "action": "buy"
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data[0].dollar_amount, -5000.0);
}

#[test]
fn test_save_portfolio_payload_deserialization_zero_dollar_amount() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890.0,
                "side": "long",
                "dollar_amount": 0.0,
                "action": "buy"
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data[0].dollar_amount, 0.0);
}

#[test]
fn test_save_portfolio_payload_deserialization_large_dollar_amount() {
    let json = r#"{
        "data": [
            {
                "ticker": "AAPL",
                "timestamp": 1234567890.0,
                "side": "long",
                "dollar_amount": 1000000000.0,
                "action": "buy"
            }
        ],
        "timestamp": "2024-01-15T12:30:45Z"
    }"#;

    let result: Result<SavePortfolioPayload, _> = serde_json::from_str(json);

    assert!(result.is_ok());

    let payload = result.unwrap();
    assert_eq!(payload.data[0].dollar_amount, 1000000000.0);
}

#[test]
fn test_query_parameters_deserialization_with_timestamp() {
    let json = r#"{"timestamp": "2024-01-15T12:30:45Z"}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_deserialization_without_timestamp() {
    let json = r#"{}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_deserialization_null_timestamp() {
    let json = r#"{"timestamp": null}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_ok());
}

#[test]
fn test_query_parameters_deserialization_invalid_timestamp() {
    let json = r#"{"timestamp": "not-a-date"}"#;

    let result: Result<QueryParameters, _> = serde_json::from_str(json);

    assert!(result.is_err());
}

#[test]
fn test_query_parameters_deserialization_various_timestamps() {
    let test_cases = vec![
        r#"{"timestamp": "2024-12-31T23:59:59Z"}"#,
        r#"{"timestamp": "2024-01-01T00:00:00Z"}"#,
        r#"{"timestamp": "2024-06-15T14:22:33Z"}"#,
    ];

    for json in test_cases {
        let result: Result<QueryParameters, _> = serde_json::from_str(json);
        assert!(result.is_ok(), "Failed to parse: {}", json);
    }
}

#[test]
fn test_dataframe_to_json_valid_portfolio_data() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "timestamp" => &[1234567890.0, 1234567890.0],
        "side" => &["LONG", "SHORT"],
        "dollar_amount" => &[10000.0, 5000.0],
        "action" => &["BUY", "SELL"],
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
    assert!(json_string.contains("LONG"));
    assert!(json_string.contains("SHORT"));
    assert!(json_string.contains("BUY"));
    assert!(json_string.contains("SELL"));
}

#[test]
fn test_dataframe_to_json_single_position() {
    let df = df! {
        "ticker" => &["AAPL"],
        "timestamp" => &[1234567890.0],
        "side" => &["LONG"],
        "dollar_amount" => &[10000.0],
        "action" => &["BUY"],
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
    assert!(json_string.contains("10000"));
}

#[test]
fn test_dataframe_to_json_empty_dataframe() {
    let df = df! {
        "ticker" => Vec::<&str>::new(),
        "timestamp" => Vec::<f64>::new(),
        "side" => Vec::<&str>::new(),
        "dollar_amount" => Vec::<f64>::new(),
        "action" => Vec::<&str>::new(),
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
fn test_dataframe_to_json_multiple_positions() {
    let tickers = vec!["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"];
    let timestamps = vec![1234567890.0; 5];
    let sides = vec!["LONG", "SHORT", "LONG", "SHORT", "LONG"];
    let amounts = vec![10000.0, 5000.0, 7500.0, 3000.0, 12000.0];
    let actions = vec!["BUY", "SELL", "HOLD", "BUY", "SELL"];

    let df = df! {
        "ticker" => tickers,
        "timestamp" => timestamps,
        "side" => sides,
        "dollar_amount" => amounts,
        "action" => actions,
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
        "timestamp" => &[1234567890.123456],
        "side" => &["LONG"],
        "dollar_amount" => &[10000.99],
        "action" => &["BUY"],
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    let result = JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone());

    assert!(result.is_ok());

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    assert!(json_string.contains("10000.99"));
}

#[test]
fn test_dataframe_to_json_handles_special_characters() {
    let df = df! {
        "ticker" => &["BRK.A", "TEST-B"],
        "timestamp" => &[1234567890.0, 1234567890.0],
        "side" => &["LONG", "SHORT"],
        "dollar_amount" => &[10000.0, 5000.0],
        "action" => &["BUY", "SELL"],
    }
    .unwrap();

    let mut buffer = Cursor::new(Vec::new());
    let result = JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone());

    assert!(result.is_ok());

    let json_bytes = buffer.into_inner();
    let json_string = String::from_utf8_lossy(&json_bytes).to_string();

    assert!(json_string.contains("BRK.A"));
    assert!(json_string.contains("TEST-B"));
}

#[test]
fn test_error_message_detection_no_files_found() {
    let error_messages = vec![
        "No files found in S3",
        "Could not find the file",
        "The specified path does not exist",
        "Invalid Input: no files found",
    ];

    for error_msg in error_messages {
        assert!(
            error_msg.contains("No files found")
                || error_msg.contains("Could not find")
                || error_msg.contains("does not exist")
                || error_msg.contains("Invalid Input"),
            "Error detection failed for: {}",
            error_msg
        );
    }
}

#[test]
fn test_error_message_detection_other_errors() {
    let error_messages = vec![
        "Permission denied",
        "Connection timeout",
        "Internal server error",
    ];

    for error_msg in error_messages {
        assert!(
            !(error_msg.contains("No files found")
                || error_msg.contains("Could not find")
                || error_msg.contains("does not exist")
                || error_msg.contains("Invalid Input")),
            "Error should not be detected as 'no files': {}",
            error_msg
        );
    }
}

#[test]
fn test_portfolio_data_creation() {
    let portfolios = vec![
        common::sample_portfolio(),
        Portfolio {
            ticker: "GOOGL".to_string(),
            timestamp: 1234567890.0,
            side: "short".to_string(),
            dollar_amount: 5000.0,
            action: "sell".to_string(),
        },
    ];

    assert_eq!(portfolios.len(), 2);
    assert_eq!(portfolios[0].ticker, "AAPL");
    assert_eq!(portfolios[1].ticker, "GOOGL");
}

#[test]
fn test_portfolio_struct_fields() {
    let portfolio = Portfolio {
        ticker: "TEST".to_string(),
        timestamp: 9999999999.0,
        side: "long".to_string(),
        dollar_amount: 12345.67,
        action: "buy".to_string(),
    };

    assert_eq!(portfolio.ticker, "TEST");
    assert_eq!(portfolio.timestamp, 9999999999.0);
    assert_eq!(portfolio.side, "long");
    assert_eq!(portfolio.dollar_amount, 12345.67);
    assert_eq!(portfolio.action, "buy");
}

#[test]
fn test_json_array_parsing() {
    let json = r#"[
        {
            "ticker": "AAPL",
            "timestamp": 1234567890.0,
            "side": "LONG",
            "dollar_amount": 10000.0,
            "action": "BUY"
        }
    ]"#;

    let result: Result<Vec<Portfolio>, _> = serde_json::from_str(json);
    assert!(result.is_ok());

    let portfolios = result.unwrap();
    assert_eq!(portfolios.len(), 1);
}

#[test]
fn test_dataframe_height_check() {
    let df_empty = df! {
        "ticker" => Vec::<&str>::new(),
        "timestamp" => Vec::<f64>::new(),
        "side" => Vec::<&str>::new(),
        "dollar_amount" => Vec::<f64>::new(),
        "action" => Vec::<&str>::new(),
    }
    .unwrap();

    assert_eq!(df_empty.height(), 0);

    let df_with_data = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "timestamp" => &[1234567890.0, 1234567890.0],
        "side" => &["LONG", "SHORT"],
        "dollar_amount" => &[10000.0, 5000.0],
        "action" => &["BUY", "SELL"],
    }
    .unwrap();

    assert_eq!(df_with_data.height(), 2);
}

#[test]
fn test_json_format_output_is_valid() {
    let df = df! {
        "ticker" => &["AAPL"],
        "timestamp" => &[1234567890.0],
        "side" => &["LONG"],
        "dollar_amount" => &[10000.0],
        "action" => &["BUY"],
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
