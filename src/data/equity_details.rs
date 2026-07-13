use crate::data::errors::Error;
use crate::domain::market::{EquityDetail, Ticker};
use tracing::{info, warn};

/// Equity details CSV embedded at compile time.
///
/// The source file lives at `data/equity_details.csv` in the repository root
/// and is checked into version control. Changes require a rebuild and redeploy.
const EQUITY_DETAILS_CSV: &str = include_str!("../../data/equity_details.csv");

/// Returns the raw embedded equity details CSV content for uploading to S3.
pub fn embedded_csv() -> &'static str {
    EQUITY_DETAILS_CSV
}

fn parse_equity_details_csv(csv_content: &str) -> Result<Vec<EquityDetail>, Error> {
    let mut lines = csv_content.lines();

    let header_line = match lines.next() {
        Some(line) => line,
        None => return Ok(Vec::new()),
    };

    let headers: Vec<&str> = header_line.split(',').map(|h| h.trim()).collect();

    for column in &["ticker", "sector", "industry"] {
        if !headers.iter().any(|h| h == column) {
            let message = format!("CSV missing required column: {}", column);
            return Err(Error::Other(message));
        }
    }

    let ticker_index = headers.iter().position(|h| *h == "ticker").unwrap();
    let sector_index = headers.iter().position(|h| *h == "sector").unwrap();
    let industry_index = headers.iter().position(|h| *h == "industry").unwrap();

    let mut details = Vec::new();
    let mut rejected_rows: usize = 0;

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() != headers.len() {
            let message = format!(
                "Malformed CSV row: expected {} fields, got {}",
                headers.len(),
                fields.len()
            );
            return Err(Error::Other(message));
        }

        let Some(ticker) = Ticker::new(fields[ticker_index]) else {
            rejected_rows += 1;
            continue;
        };

        let sector_raw = fields[sector_index].trim().to_uppercase();
        let sector = if sector_raw.is_empty() {
            "NOT AVAILABLE".to_string()
        } else {
            sector_raw
        };

        let industry_raw = fields[industry_index].trim().to_uppercase();
        let industry = if industry_raw.is_empty() {
            "NOT AVAILABLE".to_string()
        } else {
            industry_raw
        };

        details.push(EquityDetail::new(ticker, sector, industry));
    }

    if rejected_rows > 0 {
        warn!(
            "Discarded {} row(s) with invalid ticker symbols while parsing equity details CSV",
            rejected_rows
        );
    }

    Ok(details)
}

/// Parses the compile-time-embedded equity details CSV.
pub fn parse_embedded_equity_details() -> Result<Vec<EquityDetail>, Error> {
    let details = parse_equity_details_csv(EQUITY_DETAILS_CSV)?;
    info!("Parsed {} equity details from embedded CSV", details.len());
    Ok(details)
}

#[cfg(test)]
mod tests {
    use super::parse_equity_details_csv;

    #[test]
    fn test_parse_equity_details_csv_valid() {
        let csv = "ticker,sector,industry\nAAPL,Technology,Consumer Electronics\nGOOGL,Technology,Internet Services\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].ticker(), "AAPL");
        assert_eq!(details[0].sector(), "TECHNOLOGY");
        assert_eq!(details[0].industry(), "CONSUMER ELECTRONICS");
        assert_eq!(details[1].ticker(), "GOOGL");
    }

    #[test]
    fn test_parse_equity_details_csv_whitespace_trimming() {
        let csv =
            "ticker,sector,industry\nECC           ,  Technology  ,  Consumer Electronics  \n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker(), "ECC");
        assert_eq!(details[0].sector(), "TECHNOLOGY");
        assert_eq!(details[0].industry(), "CONSUMER ELECTRONICS");
    }

    #[test]
    fn test_parse_equity_details_csv_uppercase_normalization() {
        let csv = "ticker,sector,industry\naapl,technology,consumer electronics\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker(), "AAPL");
        assert_eq!(details[0].sector(), "TECHNOLOGY");
        assert_eq!(details[0].industry(), "CONSUMER ELECTRONICS");
    }

    #[test]
    fn test_parse_equity_details_csv_empty_sector_and_industry_filled() {
        let csv = "ticker,sector,industry\nAAPL,,\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].sector(), "NOT AVAILABLE");
        assert_eq!(details[0].industry(), "NOT AVAILABLE");
    }

    #[test]
    fn test_parse_equity_details_csv_extra_columns_ignored() {
        let csv =
            "ticker,sector,industry,extra_column\nAAPL,Technology,Consumer Electronics,Extra\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker(), "AAPL");
    }

    #[test]
    fn test_parse_equity_details_csv_missing_ticker_column() {
        let csv = "sector,industry\nTechnology,Consumer Electronics\n";
        let result = parse_equity_details_csv(csv);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing required column"));
    }

    #[test]
    fn test_parse_equity_details_csv_missing_sector_column() {
        let csv = "ticker,industry\nAAPL,Consumer Electronics\n";
        let result = parse_equity_details_csv(csv);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing required column"));
    }

    #[test]
    fn test_parse_equity_details_csv_missing_industry_column() {
        let csv = "ticker,sector\nAAPL,Technology\n";
        let result = parse_equity_details_csv(csv);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing required column"));
    }

    #[test]
    fn test_parse_equity_details_csv_empty_header_only() {
        let csv = "ticker,sector,industry\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 0);
    }

    #[test]
    fn test_parse_equity_details_csv_empty_input() {
        let details = parse_equity_details_csv("").unwrap();
        assert_eq!(details.len(), 0);
    }

    #[test]
    fn test_parse_equity_details_csv_malformed_row_too_few_fields() {
        let csv = "ticker,sector,industry\nAAPL,Technology\n";
        let result = parse_equity_details_csv(csv);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Malformed CSV row"));
    }

    #[test]
    fn test_parse_equity_details_csv_malformed_row_too_many_fields() {
        let csv = "ticker,sector,industry\nGOOGL,Technology,Internet Services,Extra\n";
        let result = parse_equity_details_csv(csv);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Malformed CSV row"));
    }

    #[test]
    fn test_parse_equity_details_csv_blank_lines_are_skipped() {
        // Blank lines between data rows must be silently ignored.
        let csv = "ticker,sector,industry\nAAPL,Technology,Consumer Electronics\n\n\nMSFT,Technology,Software\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].ticker(), "AAPL");
        assert_eq!(details[1].ticker(), "MSFT");
    }

    #[test]
    fn test_parse_equity_details_csv_invalid_ticker_is_skipped_not_errored() {
        // A row whose ticker field fails Ticker::new should be silently discarded
        // (rejected_rows counter), not cause the whole parse to fail.
        let csv = "ticker,sector,industry\nTOOLONG_SYMBOL,Technology,Software\nMSFT,Technology,Software\n";
        let details = parse_equity_details_csv(csv).unwrap();
        // TOOLONG_SYMBOL is rejected; MSFT passes
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker(), "MSFT");
    }

    #[test]
    fn test_parse_equity_details_csv_columns_in_different_order() {
        // Column position is determined by header lookup, not fixed index.
        let csv = "industry,ticker,sector\nConsumer Electronics,AAPL,Technology\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker(), "AAPL");
        assert_eq!(details[0].sector(), "TECHNOLOGY");
        assert_eq!(details[0].industry(), "CONSUMER ELECTRONICS");
    }

    #[test]
    fn test_parse_equity_details_csv_multiple_invalid_tickers_counted() {
        // Multiple rows with invalid tickers should all be skipped, leaving only
        // valid rows in the output.
        let csv = "ticker,sector,industry\nBADTICKER1,Tech,SW\nBADTICKER2,Tech,SW\nNVDA,Technology,Semiconductors\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker(), "NVDA");
    }

    #[test]
    fn test_parse_equity_details_csv_only_invalid_tickers_returns_empty() {
        let csv = "ticker,sector,industry\nBADTICKER1,Tech,SW\nBADTICKER2,Tech,SW\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 0);
    }

    #[test]
    fn test_parse_equity_details_csv_empty_sector_only_uses_not_available() {
        let csv = "ticker,sector,industry\nAAPL,,Software\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].sector(), "NOT AVAILABLE");
        assert_eq!(details[0].industry(), "SOFTWARE");
    }

    #[test]
    fn test_parse_equity_details_csv_empty_industry_only_uses_not_available() {
        let csv = "ticker,sector,industry\nAAPL,Technology,\n";
        let details = parse_equity_details_csv(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].sector(), "TECHNOLOGY");
        assert_eq!(details[0].industry(), "NOT AVAILABLE");
    }
}
