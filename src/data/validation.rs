//! Data quality validation for DataFrames before S3 writes.
//!
//! Provides schema conformance, null detection, value range, and consistency
//! checks. Error-severity issues (schema mismatches, wrong column types) block
//! S3 writes by returning an error from [`validate_equity_bars_or_reject`].
//! Warning-severity issues (price anomalies, OHLC inconsistencies) are logged
//! for observability without blocking writes.

use chrono::NaiveDate;
use polars::prelude::*;
use tracing::{error, info, warn};

/// Severity level for a validation issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Data is structurally invalid (wrong schema, missing columns).
    Error,
    /// Data is present but suspicious (outlier values, unexpected nulls).
    Warning,
}

/// A single validation issue found during quality checks.
#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: Severity,
    pub check: String,
    pub message: String,
}

/// Aggregated result of all validation checks on a DataFrame.
#[derive(Debug)]
pub struct ValidationReport {
    pub dataset: String,
    pub issues: Vec<ValidationIssue>,
}

impl ValidationReport {
    fn new(dataset: &str) -> Self {
        Self {
            dataset: dataset.to_string(),
            issues: Vec::new(),
        }
    }

    fn add(&mut self, severity: Severity, check: &str, message: String) {
        self.issues.push(ValidationIssue {
            severity,
            check: check.to_string(),
            message,
        });
    }

    /// Returns `true` when no issues were found.
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }

    /// Returns the number of error-level issues.
    pub fn error_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|issue| issue.severity == Severity::Error)
            .count()
    }

    /// Returns a one-line summary of all error-level issues.
    pub fn error_summary(&self) -> String {
        let messages: Vec<&str> = self
            .issues
            .iter()
            .filter(|issue| issue.severity == Severity::Error)
            .map(|issue| issue.message.as_str())
            .collect();
        messages.join("; ")
    }

    /// Log all issues as structured tracing events.
    pub fn log(&self) {
        if self.is_clean() {
            info!(dataset = %self.dataset, "Data quality validation passed");
            return;
        }
        for issue in &self.issues {
            match issue.severity {
                Severity::Error => {
                    error!(
                        dataset = %self.dataset,
                        check = %issue.check,
                        "Data quality error: {}",
                        issue.message
                    );
                }
                Severity::Warning => {
                    warn!(
                        dataset = %self.dataset,
                        check = %issue.check,
                        "Data quality warning: {}",
                        issue.message
                    );
                }
            }
        }
    }
}

/// Expected column schema for equity bars Parquet files.
///
/// Each entry is `(column_name, polars_data_type, nullable)`. This is the
/// single source of truth for the S3 parquet shape — validation and manifest
/// generation both reference it.
pub const EQUITY_BARS_COLUMNS: &[(&str, DataType, bool)] = &[
    ("ticker", DataType::String, false),
    ("timestamp", DataType::Int64, false),
    ("open_price", DataType::Float64, false),
    ("high_price", DataType::Float64, false),
    ("low_price", DataType::Float64, false),
    ("close_price", DataType::Float64, false),
    ("volume", DataType::Int64, false),
    ("volume_weighted_average_price", DataType::Float64, true),
    ("transactions", DataType::Int64, true),
];

/// Validates an equity bars DataFrame against the expected schema and value
/// constraints.
///
/// The `date` parameter is the expected trading date for all rows in the
/// DataFrame, used for timestamp range validation.
pub fn validate_equity_bars(dataframe: &DataFrame, date: NaiveDate) -> ValidationReport {
    let mut report = ValidationReport::new("equity_bars");

    check_row_count(dataframe, &mut report);
    check_schema(dataframe, EQUITY_BARS_COLUMNS, &mut report);
    check_nulls(dataframe, EQUITY_BARS_COLUMNS, &mut report);
    check_positive_prices(dataframe, &mut report);
    check_high_low_consistency(dataframe, &mut report);
    check_non_negative_volume(dataframe, &mut report);
    check_timestamp_range(dataframe, date, &mut report);

    report
}

/// Validates an equity bars DataFrame and returns an error if any error-level
/// issues are found. Warning-level issues are logged but do not cause rejection.
pub fn validate_equity_bars_or_reject(
    dataframe: &DataFrame,
    date: NaiveDate,
) -> Result<(), String> {
    let report = validate_equity_bars(dataframe, date);
    report.log();
    if report.error_count() > 0 {
        return Err(format!("Data quality errors: {}", report.error_summary()));
    }
    Ok(())
}

/// Check that the DataFrame has at least one row.
fn check_row_count(dataframe: &DataFrame, report: &mut ValidationReport) {
    if dataframe.height() == 0 {
        report.add(
            Severity::Warning,
            "row_count",
            "DataFrame is empty".to_string(),
        );
    }
}

/// Check that columns match the expected names, types, and order.
fn check_schema(
    dataframe: &DataFrame,
    expected: &[(&str, DataType, bool)],
    report: &mut ValidationReport,
) {
    let actual_names = dataframe.get_column_names_str();
    let expected_names: Vec<&str> = expected.iter().map(|(name, _, _)| *name).collect();

    if actual_names.len() != expected_names.len() {
        report.add(
            Severity::Error,
            "schema_column_count",
            format!(
                "Expected {} columns, found {}",
                expected_names.len(),
                actual_names.len()
            ),
        );
        return;
    }

    for (index, (expected_name, expected_type, _)) in expected.iter().enumerate() {
        if index >= actual_names.len() {
            break;
        }
        if actual_names[index] != *expected_name {
            report.add(
                Severity::Error,
                "schema_column_name",
                format!(
                    "Column {} expected '{}', found '{}'",
                    index, expected_name, actual_names[index]
                ),
            );
        }
        if let Ok(column) = dataframe.column(expected_name) {
            if column.dtype() != expected_type {
                report.add(
                    Severity::Error,
                    "schema_column_type",
                    format!(
                        "Column '{}' expected type {:?}, found {:?}",
                        expected_name,
                        expected_type,
                        column.dtype()
                    ),
                );
            }
        }
    }
}

/// Check that non-nullable columns contain no null values.
fn check_nulls(
    dataframe: &DataFrame,
    expected: &[(&str, DataType, bool)],
    report: &mut ValidationReport,
) {
    for (name, _, nullable) in expected {
        if *nullable {
            continue;
        }
        if let Ok(column) = dataframe.column(name) {
            let null_count = column.null_count();
            if null_count > 0 {
                report.add(
                    Severity::Error,
                    "null_check",
                    format!(
                        "Non-nullable column '{}' has {} null values",
                        name, null_count
                    ),
                );
            }
        }
    }
}

/// Check that price columns are all finite and positive.
fn check_positive_prices(dataframe: &DataFrame, report: &mut ValidationReport) {
    for column_name in ["open_price", "high_price", "low_price", "close_price"] {
        let Ok(column) = dataframe.column(column_name) else {
            continue;
        };
        let Ok(values) = column.f64() else {
            continue;
        };
        let invalid = values
            .into_iter()
            .filter(|v| matches!(v, Some(p) if !p.is_finite() || *p <= 0.0))
            .count();
        if invalid > 0 {
            report.add(
                Severity::Warning,
                "positive_prices",
                format!(
                    "Column '{}' has {} non-positive or non-finite values",
                    column_name, invalid
                ),
            );
        }
    }
}

/// Check that high >= low for every row.
fn check_high_low_consistency(dataframe: &DataFrame, report: &mut ValidationReport) {
    let (Ok(high_column), Ok(low_column)) = (
        dataframe.column("high_price"),
        dataframe.column("low_price"),
    ) else {
        return;
    };
    let (Ok(high_values), Ok(low_values)) = (high_column.f64(), low_column.f64()) else {
        return;
    };
    let violations: usize = high_values
        .into_iter()
        .zip(low_values)
        .filter(|(high, low)| matches!((high, low), (Some(h), Some(l)) if h < l))
        .count();
    if violations > 0 {
        report.add(
            Severity::Warning,
            "high_low_consistency",
            format!("{} rows have high_price < low_price", violations),
        );
    }
}

/// Check that volume is non-negative.
fn check_non_negative_volume(dataframe: &DataFrame, report: &mut ValidationReport) {
    let Ok(column) = dataframe.column("volume") else {
        return;
    };
    let Ok(values) = column.i64() else {
        return;
    };
    let negative = values
        .into_iter()
        .filter(|v| matches!(v, Some(n) if *n < 0))
        .count();
    if negative > 0 {
        report.add(
            Severity::Warning,
            "non_negative_volume",
            format!("{} rows have negative volume", negative),
        );
    }
}

/// Check that all timestamps fall within the expected trading date (midnight to
/// midnight UTC of the given date).
fn check_timestamp_range(dataframe: &DataFrame, date: NaiveDate, report: &mut ValidationReport) {
    let Ok(column) = dataframe.column("timestamp") else {
        return;
    };
    let Ok(values) = column.i64() else {
        return;
    };

    let day_start_millis = date
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        .timestamp_millis();
    let day_end_millis = date
        .succ_opt()
        .expect("valid successor date")
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        .timestamp_millis();

    let out_of_range = values
        .into_iter()
        .filter(|v| matches!(v, Some(ts) if *ts < day_start_millis || *ts >= day_end_millis))
        .count();

    if out_of_range > 0 {
        report.add(
            Severity::Warning,
            "timestamp_range",
            format!(
                "{} rows have timestamps outside expected date {}",
                out_of_range, date
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    /// Builds a valid equity bars DataFrame with the given number of rows.
    fn valid_equity_bars_dataframe(trading_date: NaiveDate, row_count: usize) -> DataFrame {
        let timestamp_millis = trading_date
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        df!(
            "ticker" => (0..row_count).map(|_| "AAPL").collect::<Vec<_>>(),
            "timestamp" => vec![timestamp_millis; row_count],
            "open_price" => vec![150.0_f64; row_count],
            "high_price" => vec![155.0_f64; row_count],
            "low_price" => vec![148.0_f64; row_count],
            "close_price" => vec![152.0_f64; row_count],
            "volume" => vec![1_000_000_i64; row_count],
            "volume_weighted_average_price" => vec![Some(151.5_f64); row_count],
            "transactions" => vec![Some(5000_i64); row_count],
        )
        .unwrap()
    }

    #[test]
    fn test_valid_dataframe_passes() {
        let dataframe = valid_equity_bars_dataframe(date(2026, 6, 15), 5);
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(
            report.is_clean(),
            "Expected clean report, got: {:?}",
            report.issues
        );
    }

    #[test]
    fn test_empty_dataframe_flagged() {
        let dataframe = valid_equity_bars_dataframe(date(2026, 6, 15), 0);
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(!report.is_clean());
        assert!(report.issues.iter().any(|issue| issue.check == "row_count"));
    }

    #[test]
    fn test_wrong_column_count_flagged() {
        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [1_000_000_i64],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report.error_count() > 0);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "schema_column_count"));
    }

    #[test]
    fn test_wrong_column_type_flagged() {
        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => ["not_a_number"],
            "open_price" => [150.0_f64],
            "high_price" => [155.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [1_000_000_i64],
            "volume_weighted_average_price" => [Some(151.5_f64)],
            "transactions" => [Some(5000_i64)],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "schema_column_type"));
    }

    #[test]
    fn test_null_in_non_nullable_column_flagged() {
        let ticker: Vec<Option<&str>> = vec![Some("AAPL"), None];
        let timestamp_millis = date(2026, 6, 15)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => &ticker,
            "timestamp" => vec![timestamp_millis; 2],
            "open_price" => vec![150.0_f64; 2],
            "high_price" => vec![155.0_f64; 2],
            "low_price" => vec![148.0_f64; 2],
            "close_price" => vec![152.0_f64; 2],
            "volume" => vec![1_000_000_i64; 2],
            "volume_weighted_average_price" => vec![Some(151.5_f64); 2],
            "transactions" => vec![Some(5000_i64); 2],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "null_check"));
    }

    #[test]
    fn test_negative_price_flagged() {
        let timestamp_millis = date(2026, 6, 15)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [timestamp_millis],
            "open_price" => [-1.0_f64],
            "high_price" => [155.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [1_000_000_i64],
            "volume_weighted_average_price" => [Some(151.5_f64)],
            "transactions" => [Some(5000_i64)],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "positive_prices"));
    }

    #[test]
    fn test_nan_and_infinity_prices_flagged() {
        let timestamp_millis = date(2026, 6, 15)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => ["AAPL", "MSFT"],
            "timestamp" => vec![timestamp_millis; 2],
            "open_price" => [f64::NAN, f64::INFINITY],
            "high_price" => [155.0_f64, 155.0],
            "low_price" => [148.0_f64, 148.0],
            "close_price" => [152.0_f64, 152.0],
            "volume" => vec![1_000_000_i64; 2],
            "volume_weighted_average_price" => vec![Some(151.5_f64); 2],
            "transactions" => vec![Some(5000_i64); 2],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        let price_issues: Vec<_> = report
            .issues
            .iter()
            .filter(|issue| issue.check == "positive_prices")
            .collect();
        assert_eq!(price_issues.len(), 1);
        assert!(price_issues[0].message.contains("non-finite"));
    }

    #[test]
    fn test_high_less_than_low_flagged() {
        let timestamp_millis = date(2026, 6, 15)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [timestamp_millis],
            "open_price" => [150.0_f64],
            "high_price" => [140.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [1_000_000_i64],
            "volume_weighted_average_price" => [Some(151.5_f64)],
            "transactions" => [Some(5000_i64)],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "high_low_consistency"));
    }

    #[test]
    fn test_negative_volume_flagged() {
        let timestamp_millis = date(2026, 6, 15)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [timestamp_millis],
            "open_price" => [150.0_f64],
            "high_price" => [155.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [-100_i64],
            "volume_weighted_average_price" => [Some(151.5_f64)],
            "transactions" => [Some(5000_i64)],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "non_negative_volume"));
    }

    #[test]
    fn test_timestamp_out_of_range_flagged() {
        // Timestamp from a different day.
        let wrong_day_millis = date(2026, 6, 14)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [wrong_day_millis],
            "open_price" => [150.0_f64],
            "high_price" => [155.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [1_000_000_i64],
            "volume_weighted_average_price" => [Some(151.5_f64)],
            "transactions" => [Some(5000_i64)],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "timestamp_range"));
    }

    #[test]
    fn test_nullable_columns_allow_nulls() {
        let timestamp_millis = date(2026, 6, 15)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [timestamp_millis],
            "open_price" => [150.0_f64],
            "high_price" => [155.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [1_000_000_i64],
            "volume_weighted_average_price" => [None::<f64>],
            "transactions" => [None::<i64>],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(
            report.is_clean(),
            "Nullable columns with None should not trigger issues: {:?}",
            report.issues
        );
    }

    #[test]
    fn test_wrong_column_name_flagged() {
        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [1_000_000_i64],
            "open" => [150.0_f64],
            "high_price" => [155.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [1_000_000_i64],
            "volume_weighted_average_price" => [Some(151.5_f64)],
            "transactions" => [Some(5000_i64)],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.check == "schema_column_name"));
    }

    #[test]
    fn test_error_count() {
        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [1_000_000_i64],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        assert!(report.error_count() > 0);
    }

    #[test]
    fn test_error_summary_joins_messages() {
        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [1_000_000_i64],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        let summary = report.error_summary();
        assert!(summary.contains("Expected 9 columns, found 2"));
    }

    #[test]
    fn test_warnings_do_not_appear_in_error_summary() {
        let timestamp_millis = date(2026, 6, 15)
            .and_hms_opt(16, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        let dataframe = df!(
            "ticker" => ["AAPL"],
            "timestamp" => [timestamp_millis],
            "open_price" => [-1.0_f64],
            "high_price" => [155.0_f64],
            "low_price" => [148.0_f64],
            "close_price" => [152.0_f64],
            "volume" => [1_000_000_i64],
            "volume_weighted_average_price" => [Some(151.5_f64)],
            "transactions" => [Some(5000_i64)],
        )
        .unwrap();
        let report = validate_equity_bars(&dataframe, date(2026, 6, 15));
        // Negative price is a warning, not an error.
        assert_eq!(report.error_count(), 0);
        assert!(!report.is_clean());
        assert!(report.error_summary().is_empty());
    }
}
