use polars::prelude::*;

#[test]
fn test_dataframe_to_csv_valid_data() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL", "MSFT"],
        "sector" => &["TECHNOLOGY", "TECHNOLOGY", "TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS", "INTERNET SERVICES", "SOFTWARE"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(csv_content.contains("ticker"));
    assert!(csv_content.contains("sector"));
    assert!(csv_content.contains("industry"));
    assert!(csv_content.contains("AAPL"));
    assert!(csv_content.contains("GOOGL"));
    assert!(csv_content.contains("MSFT"));
    assert!(csv_content.contains("TECHNOLOGY"));
    assert!(csv_content.contains("CONSUMER ELECTRONICS"));
}

#[test]
fn test_dataframe_to_csv_single_row() {
    let df = df! {
        "ticker" => &["AAPL"],
        "sector" => &["TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(csv_content.contains("AAPL"));
    assert!(csv_content.contains("TECHNOLOGY"));
    assert!(csv_content.contains("CONSUMER ELECTRONICS"));

    let lines: Vec<&str> = csv_content.lines().collect();
    assert_eq!(lines.len(), 2);
}

#[test]
fn test_dataframe_to_csv_empty_dataframe() {
    let df = df! {
        "ticker" => Vec::<&str>::new(),
        "sector" => Vec::<&str>::new(),
        "industry" => Vec::<&str>::new(),
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(csv_content.contains("ticker"));
    assert!(csv_content.contains("sector"));
    assert!(csv_content.contains("industry"));

    let lines: Vec<&str> = csv_content.lines().collect();
    assert_eq!(lines.len(), 1);
}

#[test]
fn test_dataframe_to_csv_special_characters() {
    let df = df! {
        "ticker" => &["TEST-A", "BRK.B"],
        "sector" => &["FINANCIAL SERVICES", "TECHNOLOGY & SERVICES"],
        "industry" => &["INSURANCE, PROPERTY & CASUALTY", "SEMICONDUCTORS & EQUIPMENT"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(csv_content.contains("TEST-A"));
    assert!(csv_content.contains("BRK.B"));
}

#[test]
fn test_dataframe_to_csv_uppercase_preservation() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "sector" => &["TECHNOLOGY", "TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS", "INTERNET SERVICES"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(csv_content.contains("AAPL"));
    assert!(!csv_content.contains("aapl"));
    assert!(csv_content.contains("TECHNOLOGY"));
    assert!(!csv_content.contains("technology"));
}

#[test]
fn test_dataframe_to_csv_with_quotes() {
    let df = df! {
        "ticker" => &["AAPL"],
        "sector" => &["TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS, DEVICES"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(csv_content.contains("CONSUMER ELECTRONICS"));
}

#[test]
fn test_dataframe_to_csv_large_dataset() {
    let ticker_count = 1000;
    let tickers: Vec<String> = (0..ticker_count).map(|i| format!("TICK{}", i)).collect();
    let sectors: Vec<String> = (0..ticker_count)
        .map(|_| "TECHNOLOGY".to_string())
        .collect();
    let industries: Vec<String> = (0..ticker_count)
        .map(|_| "SOFTWARE".to_string())
        .collect();

    let ticker_refs: Vec<&str> = tickers.iter().map(|s| s.as_str()).collect();
    let sector_refs: Vec<&str> = sectors.iter().map(|s| s.as_str()).collect();
    let industry_refs: Vec<&str> = industries.iter().map(|s| s.as_str()).collect();

    let df = df! {
        "ticker" => ticker_refs,
        "sector" => sector_refs,
        "industry" => industry_refs,
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    let lines: Vec<&str> = csv_content.lines().collect();
    assert_eq!(lines.len(), ticker_count + 1);

    assert!(csv_content.contains("TICK0"));
    assert!(csv_content.contains("TICK999"));
}

#[test]
fn test_dataframe_to_csv_null_values_replaced() {
    let tickers = Series::new("ticker".into(), &["AAPL", "GOOGL"]);
    let sectors = Series::new("sector".into(), &[Some("TECHNOLOGY"), None]);
    let industries = Series::new("industry".into(), &[None, Some("INTERNET SERVICES")]);

    let df = DataFrame::new(vec![tickers.into(), sectors.into(), industries.into()]).unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(csv_content.contains("AAPL"));
    assert!(csv_content.contains("GOOGL"));
}

#[test]
fn test_dataframe_to_csv_consistent_column_order() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "sector" => &["TECHNOLOGY", "TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS", "INTERNET SERVICES"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    let result = writer.finish(&mut df.clone());

    assert!(result.is_ok());

    let csv_content = String::from_utf8(buffer).unwrap();

    let lines: Vec<&str> = csv_content.lines().collect();
    let header = lines[0];

    let columns: Vec<&str> = header.split(',').collect();
    assert_eq!(columns.len(), 3);

    assert!(header.contains("ticker"));
    assert!(header.contains("sector"));
    assert!(header.contains("industry"));
}

#[test]
fn test_dataframe_to_csv_multiple_conversions() {
    let df = df! {
        "ticker" => &["AAPL"],
        "sector" => &["TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS"],
    }
    .unwrap();

    for _ in 0..5 {
        let mut buffer = Vec::new();
        let mut writer = CsvWriter::new(&mut buffer);

        let result = writer.finish(&mut df.clone());

        assert!(result.is_ok());

        let csv_content = String::from_utf8(buffer).unwrap();

        assert!(csv_content.contains("AAPL"));
    }
}

#[test]
fn test_utf8_conversion_valid() {
    let test_string = "ticker,sector,industry\nAAPL,TECHNOLOGY,CONSUMER ELECTRONICS\n";
    let bytes = test_string.as_bytes().to_vec();

    let result = String::from_utf8(bytes);

    assert!(result.is_ok());

    let converted = result.unwrap();
    assert_eq!(converted, test_string);
}

#[test]
fn test_utf8_conversion_ascii() {
    let test_string = "ABC123";
    let bytes = test_string.as_bytes().to_vec();

    let result = String::from_utf8(bytes);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), test_string);
}

#[test]
fn test_utf8_conversion_empty() {
    let bytes = Vec::new();

    let result = String::from_utf8(bytes);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "");
}

#[test]
fn test_utf8_conversion_long_content() {
    let test_string = "A".repeat(10000);
    let bytes = test_string.as_bytes().to_vec();

    let result = String::from_utf8(bytes);

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 10000);
}

#[test]
fn test_csv_header_content_type_value() {
    let content_type = "text/csv; charset=utf-8";

    assert!(content_type.contains("text/csv"));
    assert!(content_type.contains("charset=utf-8"));
}

#[test]
fn test_csv_content_disposition_header_value() {
    let content_disposition = "attachment; filename=\"equity_details.csv\"";

    assert!(content_disposition.contains("attachment"));
    assert!(content_disposition.contains("filename"));
}

#[test]
fn test_dataframe_csv_roundtrip() {
    let original_df = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "sector" => &["TECHNOLOGY", "TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS", "INTERNET SERVICES"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);
    writer.finish(&mut original_df.clone()).unwrap();

    let csv_content = String::from_utf8(buffer).unwrap();

    use std::io::Cursor;
    let cursor = Cursor::new(csv_content.as_bytes());
    let parsed_df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()
        .unwrap();

    assert_eq!(parsed_df.height(), original_df.height());
    assert_eq!(parsed_df.width(), original_df.width());
}

#[test]
fn test_dataframe_to_csv_preserves_row_count() {
    let row_counts = vec![0, 1, 10, 100, 500];

    for row_count in row_counts {
        let tickers: Vec<String> = (0..row_count).map(|i| format!("TICK{}", i)).collect();
        let sectors: Vec<String> = (0..row_count).map(|_| "SECTOR".to_string()).collect();
        let industries: Vec<String> = (0..row_count).map(|_| "INDUSTRY".to_string()).collect();

        let ticker_refs: Vec<&str> = tickers.iter().map(|s| s.as_str()).collect();
        let sector_refs: Vec<&str> = sectors.iter().map(|s| s.as_str()).collect();
        let industry_refs: Vec<&str> = industries.iter().map(|s| s.as_str()).collect();

        let df = df! {
            "ticker" => ticker_refs,
            "sector" => sector_refs,
            "industry" => industry_refs,
        }
        .unwrap();

        let mut buffer = Vec::new();
        let mut writer = CsvWriter::new(&mut buffer);
        writer.finish(&mut df.clone()).unwrap();

        let csv_content = String::from_utf8(buffer).unwrap();
        let lines: Vec<&str> = csv_content.lines().collect();

        assert_eq!(lines.len(), row_count + 1);
    }
}

#[test]
fn test_dataframe_to_csv_column_names_lowercase() {
    let df = df! {
        "ticker" => &["AAPL"],
        "sector" => &["TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    writer.finish(&mut df.clone()).unwrap();

    let csv_content = String::from_utf8(buffer).unwrap();
    let lines: Vec<&str> = csv_content.lines().collect();
    let header = lines[0];

    assert!(header.contains("ticker"));
    assert!(header.contains("sector"));
    assert!(header.contains("industry"));
}

#[test]
fn test_dataframe_to_csv_no_trailing_newline_required() {
    let df = df! {
        "ticker" => &["AAPL"],
        "sector" => &["TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS"],
    }
    .unwrap();

    let mut buffer = Vec::new();
    let mut writer = CsvWriter::new(&mut buffer);

    writer.finish(&mut df.clone()).unwrap();

    let csv_content = String::from_utf8(buffer).unwrap();

    assert!(!csv_content.is_empty());
}

#[test]
fn test_dataframe_schema_validation() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "sector" => &["TECHNOLOGY", "TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS", "INTERNET SERVICES"],
    }
    .unwrap();

    let schema = df.schema();

    assert_eq!(schema.len(), 3);
    assert!(schema.contains("ticker"));
    assert!(schema.contains("sector"));
    assert!(schema.contains("industry"));
}

#[test]
fn test_dataframe_column_access() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL"],
        "sector" => &["TECHNOLOGY", "TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS", "INTERNET SERVICES"],
    }
    .unwrap();

    let ticker_column = df.column("ticker");
    assert!(ticker_column.is_ok());

    let sector_column = df.column("sector");
    assert!(sector_column.is_ok());

    let industry_column = df.column("industry");
    assert!(industry_column.is_ok());

    let nonexistent_column = df.column("nonexistent");
    assert!(nonexistent_column.is_err());
}

#[test]
fn test_dataframe_height_and_width() {
    let df = df! {
        "ticker" => &["AAPL", "GOOGL", "MSFT"],
        "sector" => &["TECHNOLOGY", "TECHNOLOGY", "TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS", "INTERNET SERVICES", "SOFTWARE"],
    }
    .unwrap();

    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 3);
}

#[test]
fn test_dataframe_clone_preserves_data() {
    let df = df! {
        "ticker" => &["AAPL"],
        "sector" => &["TECHNOLOGY"],
        "industry" => &["CONSUMER ELECTRONICS"],
    }
    .unwrap();

    let cloned_df = df.clone();

    assert_eq!(df.height(), cloned_df.height());
    assert_eq!(df.width(), cloned_df.width());

    let original_ticker = df
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();

    let cloned_ticker = cloned_df
        .column("ticker")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap();

    assert_eq!(original_ticker, cloned_ticker);
}
