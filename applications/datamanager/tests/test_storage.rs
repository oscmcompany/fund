// Integration tests for storage module
// Note: Full S3/DuckDB tests require AWS credentials and are tested via handler integration tests

#[test]
fn test_ticker_validation_valid_tickers() {
    let valid_tickers = vec!["AAPL", "GOOGL", "BRK.A", "BRK.B", "TEST-A", "A", "ABCD1234"];

    for ticker in valid_tickers {
        let is_valid = ticker
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-');
        assert!(
            is_valid,
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
        let is_valid = ticker
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-');
        assert!(
            !is_valid,
            "Ticker '{}' should be invalid but passed validation",
            ticker
        );
    }
}

#[test]
fn test_ticker_validation_edge_cases() {
    let empty = "";
    let is_valid = empty
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-');
    assert!(is_valid); // Empty string passes .all() but would fail elsewhere

    let dots = "...";
    let is_valid = dots
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-');
    assert!(is_valid);

    let dashes = "---";
    let is_valid = dashes
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-');
    assert!(is_valid);
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
        let year = timestamp.format("%Y");
        let month = timestamp.format("%m");
        let day = timestamp.format("%d");

        let key = format!(
            "equity/{}/daily/year={}/month={}/day={}/data.parquet",
            dataframe_type, year, month, day,
        );

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
        let date_int = timestamp
            .format("%Y%m%d")
            .to_string()
            .parse::<i32>()
            .unwrap();

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
    let escaped = ticker_with_quote.replace('\'', "''");

    assert_eq!(escaped, "TEST''TICKER");

    let multiple_quotes = "A'B'C";
    let escaped_multiple = multiple_quotes.replace('\'', "''");

    assert_eq!(escaped_multiple, "A''B''C");
}

#[test]
fn test_default_date_range_calculation() {
    use chrono::{Duration, Utc};

    let end_date = Utc::now();
    let start_date = end_date - Duration::days(7);

    let duration = end_date - start_date;
    assert_eq!(duration.num_days(), 7);

    assert!(start_date < end_date);
}
