mod common;

#[test]
fn test_embedded_equity_details_csv_parses_successfully() {
    let result = fund::data::equity_details::parse_embedded_equity_details();
    assert!(result.is_ok());
    let details = result.unwrap();
    assert!(!details.is_empty());
    assert!(details.iter().any(|d| d.ticker().as_str() == "AAPL"));
}
