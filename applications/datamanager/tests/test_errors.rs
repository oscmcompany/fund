use datamanager::errors::Error;

#[test]
fn test_error_display_formats_messages() {
    let other_error = Error::Other("example message".to_string());
    assert_eq!(other_error.to_string(), "Other error: example message");

    let connection = duckdb::Connection::open_in_memory().unwrap();
    let duckdb_error = connection
        .execute_batch("SELECT * FROM missing_table")
        .unwrap_err();
    let wrapped_error = Error::DuckDB(duckdb_error);

    assert!(wrapped_error.to_string().contains("DuckDB error"));
}
