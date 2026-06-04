use data_manager::errors::Error;

#[test]
fn test_error_display_other_variant() {
    let error = Error::Other("example message".to_string());
    assert_eq!(error.to_string(), "Other error: example message");
}

#[test]
fn test_error_display_no_data_variant() {
    let error = Error::NoData;
    assert_eq!(error.to_string(), "No data found");
}

#[test]
fn test_error_display_polars_variant() {
    use polars::prelude::*;
    let polars_error = PolarsError::ColumnNotFound("missing_col".into());
    let error = Error::Polars(polars_error);
    assert!(error.to_string().contains("Polars error"));
}

#[test]
fn test_error_from_polars_conversion() {
    use polars::prelude::*;
    let polars_error = PolarsError::ColumnNotFound("col".into());
    let error: Error = polars_error.into();
    assert!(error.to_string().contains("Polars error"));
}
