// Integration tests for storage module
// Note: Full S3/DuckDB tests require AWS credentials and are tested via handler integration tests

use datamanager::storage::{date_to_int, escape_sql_ticker, format_s3_key, is_valid_ticker};

#[test]
fn test_ticker_validation_valid_tickers() {
    let valid_tickers = vec!["AAPL", "GOOGL", "BRK.A", "BRK.B", "TEST-A", "A", "ABCD1234"];

    for ticker in valid_tickers {
        assert!(
            is_valid_ticker(ticker),
            "Ticker '{}' should be valid but validation failed",
            ticker
        );
    }
}

#[test]
fn test_ticker_validation_rejects_invalid_characters() {
    let invalid_tickers = vec![
        "AAPL'; DROP TABLE--;",
        "AAPL OR 1=1",
        "AAPL<script>",
        "AAPL;",
        "AAPL'",
        "AAPL\"",
        "AAPL\\",
        "AAPL/",
        "AAPL*",
        "AAPL%",
        "AAPL$",
        "AAPL#",
        "AAPL@",
        "AAPL!",
        "AAPL&",
        "AAPL|",
        "AAPL~",
        "AAPL`",
        "AAPL(",
        "AAPL)",
        "AAPL[",
        "AAPL]",
        "AAPL{",
        "AAPL}",
        "AAPL<",
        "AAPL>",
        "AAPL,",
        "AAPL ",
        " AAPL",
        "AA PL",
    ];

    for ticker in invalid_tickers {
        assert!(
            !is_valid_ticker(ticker),
            "Ticker '{}' should be invalid but passed validation",
            ticker
        );
    }
}

#[test]
fn test_ticker_validation_edge_cases() {
    let empty = "";
    assert!(!is_valid_ticker(empty), "Empty string should be invalid");

    let dots = "...";
    assert!(!is_valid_ticker(dots), "Dots-only ticker should be invalid");

    let dashes = "---";
    assert!(
        !is_valid_ticker(dashes),
        "Dashes-only ticker should be invalid"
    );
}

#[test]
fn test_s3_key_format_generation() {
    use chrono::{TimeZone, Utc};

    let test_cases = vec![
        (
            Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap(),
            "bars",
            "equity/bars/daily/year=2024/month=01/day=15/data.parquet",
        ),
        (
            Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap(),
            "predictions",
            "equity/predictions/daily/year=2024/month=12/day=31/data.parquet",
        ),
        (
            Utc.with_ymd_and_hms(2024, 2, 29, 0, 0, 0).unwrap(),
            "portfolios",
            "equity/portfolios/daily/year=2024/month=02/day=29/data.parquet",
        ),
    ];

    for (timestamp, dataframe_type, expected_key) in test_cases {
        let key = format_s3_key(&timestamp, dataframe_type);

        assert_eq!(
            key, expected_key,
            "S3 key format mismatch for timestamp {}",
            timestamp
        );
    }
}

#[test]
fn test_date_range_integer_conversion() {
    use chrono::{TimeZone, Utc};

    let test_cases = vec![
        (Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(), 20240101),
        (
            Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap(),
            20241231,
        ),
        (
            Utc.with_ymd_and_hms(2024, 2, 29, 12, 30, 45).unwrap(),
            20240229,
        ),
        (
            Utc.with_ymd_and_hms(2023, 7, 15, 10, 20, 30).unwrap(),
            20230715,
        ),
    ];

    for (timestamp, expected_int) in test_cases {
        let date_int = date_to_int(&timestamp);

        assert_eq!(
            date_int, expected_int,
            "Date integer conversion failed for {}",
            timestamp
        );
    }
}

#[test]
fn test_date_range_comparison_logic() {
    let test_cases = vec![
        (2024, 1, 15, 20240101, 20241231, true),
        (2024, 1, 1, 20240101, 20241231, true),
        (2024, 12, 31, 20240101, 20241231, true),
        (2023, 12, 31, 20240101, 20241231, false),
        (2025, 1, 1, 20240101, 20241231, false),
        (2024, 6, 15, 20240601, 20240630, true),
        (2024, 5, 31, 20240601, 20240630, false),
        (2024, 7, 1, 20240601, 20240630, false),
    ];

    for (year, month, day, start_range, end_range, should_match) in test_cases {
        let date_int = year * 10000 + month * 100 + day;
        let matches = date_int >= start_range && date_int <= end_range;

        assert_eq!(
            matches, should_match,
            "Date comparison failed for {}/{}/{} (int: {}) in range {} to {}",
            year, month, day, date_int, start_range, end_range
        );
    }
}

#[test]
fn test_ticker_sql_escaping() {
    let ticker_with_quote = "TEST'TICKER";
    let escaped = escape_sql_ticker(ticker_with_quote);

    assert_eq!(escaped, "TEST''TICKER");

    let multiple_quotes = "A'B'C";
    let escaped_multiple = escape_sql_ticker(multiple_quotes);

    assert_eq!(escaped_multiple, "A''B''C");
}
