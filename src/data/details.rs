//! Ticker metadata: sector and industry.
//!
//! The pair screen caps how many legs the book may hold in one sector, so without this every ticker
//! is uncategorized, the cap binds on nothing, and ten pairs can be one industry bet held ten times.
//!
//! Seeded from `data/equity_details.csv`, embedded at compile time so a fresh database needs no
//! network. Alpaca does not publish sector or industry, so the post-close sync refreshes only
//! tickers that have appeared or been delisted.

use std::collections::HashMap;

use polars::prelude::*;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::types::{EquityDetail, Ticker};

/// Value stored when the source has no sector or industry for a ticker.
///
/// Spelled rather than left null so the screen's different-sector comparison has something total to
/// work with. Two tickers both marked unavailable are treated as the same sector, which is the
/// conservative reading: unknown sectors should not be assumed to diversify.
pub const UNKNOWN: &str = "NOT AVAILABLE";

/// Rows per insert chunk. Three columns, so a thousand rows is well inside the bind parameter limit.
const INSERT_CHUNK_ROWS: usize = 1_000;

/// The seed CSV, embedded at compile time.
const EMBEDDED_CSV: &str = include_str!("../../data/equity_details.csv");

/// Errors syncing or reading ticker metadata.
#[derive(Debug, thiserror::Error)]
pub enum DetailsError {
    #[error("failed to parse the embedded equity details CSV: {0}")]
    Parse(String),
    #[error("failed to persist equity details: {0}")]
    Database(#[from] sqlx::Error),
    #[error("failed to build an equity detail frame: {0}")]
    Frame(#[from] PolarsError),
}

/// Returns the embedded CSV text.
pub fn embedded_csv() -> &'static str {
    EMBEDDED_CSV
}

/// Parses the embedded CSV into validated details.
///
/// Rows whose ticker fails format validation are skipped rather than failing the batch: the source
/// is a public listing export that includes test issues and non-equity instruments, and one bad row
/// should not cost the other seven thousand.
pub fn parse_embedded_details() -> Result<Vec<EquityDetail>, DetailsError> {
    parse_details(EMBEDDED_CSV)
}

fn parse_details(csv: &str) -> Result<Vec<EquityDetail>, DetailsError> {
    let mut lines = csv.lines();
    let header = lines
        .next()
        .ok_or_else(|| DetailsError::Parse("the CSV is empty".to_string()))?;

    let header_fields = split_csv_line(header);

    let index_of = |name: &str| -> Result<usize, DetailsError> {
        header_fields
            .iter()
            .position(|field| field.trim() == name)
            .ok_or_else(|| DetailsError::Parse(format!("the CSV has no '{name}' column")))
    };
    let ticker_index = index_of("ticker")?;
    let sector_index = index_of("sector")?;
    let industry_index = index_of("industry")?;

    let mut details = Vec::new();
    let mut skipped: usize = 0;

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields = split_csv_line(line);
        let Some(raw_ticker) = fields.get(ticker_index) else {
            skipped += 1;
            continue;
        };
        let Some(ticker) = Ticker::new(raw_ticker) else {
            skipped += 1;
            continue;
        };
        let value_at = |index: usize| -> String {
            fields
                .get(index)
                .map(|field| field.trim())
                .filter(|field| !field.is_empty())
                .unwrap_or(UNKNOWN)
                .to_string()
        };
        details.push(EquityDetail::new(
            ticker,
            value_at(sector_index),
            value_at(industry_index),
        ));
    }

    if skipped > 0 {
        warn!(
            skipped,
            parsed = details.len(),
            "Skipped equity detail rows with unusable tickers"
        );
    }
    Ok(details)
}

/// Splits one CSV line, honouring double-quoted fields containing commas.
///
/// The source has company names like `Alcoa Corporation, Common Stock`, so splitting on every comma
/// shifts every later column by one and silently reads the wrong field as the sector.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for character in line.chars() {
        match (character, in_quotes) {
            ('"', _) => in_quotes = !in_quotes,
            (',', false) => fields.push(std::mem::take(&mut current)),
            _ => current.push(character),
        }
    }
    fields.push(current);
    fields
}

/// Upserts details into `equity_details`.
pub async fn store_details(pool: &PgPool, details: &[EquityDetail]) -> Result<u64, DetailsError> {
    if details.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;
    let mut transaction = pool.begin().await?;

    for chunk in details.chunks(INSERT_CHUNK_ROWS) {
        let mut query_builder =
            sqlx::QueryBuilder::new("INSERT INTO equity_details (ticker, sector, industry) ");

        query_builder.push_values(chunk, |mut builder, detail| {
            builder
                .push_bind(detail.ticker())
                .push_bind(detail.sector().to_string())
                .push_bind(detail.industry().to_string());
        });

        query_builder.push(
            " ON CONFLICT (ticker) DO UPDATE SET \
             sector = EXCLUDED.sector, \
             industry = EXCLUDED.industry",
        );

        rows_affected += query_builder
            .build()
            .execute(&mut *transaction)
            .await?
            .rows_affected();
    }

    transaction.commit().await?;
    info!(rows = rows_affected, "Equity details stored");
    Ok(rows_affected)
}

/// Loads the sector of every ticker, for the pair screen's different-sector rule.
///
/// A map rather than a frame because the screen looks a ticker up per candidate combination, and a
/// frame join per lookup in a quadratic loop is the wrong shape entirely.
pub async fn load_sectors(pool: &PgPool) -> Result<HashMap<Ticker, String>, DetailsError> {
    let rows =
        sqlx::query!(r#"SELECT ticker AS "ticker!", sector AS "sector!" FROM equity_details"#)
            .fetch_all(pool)
            .await?;

    let supplied = rows.len();
    let sectors: HashMap<Ticker, String> = rows
        .into_iter()
        .filter_map(|row| Ticker::new(&row.ticker).map(|ticker| (ticker, row.sector)))
        .collect();

    // Counted and reported, as `parse_details` does for the CSV path. A dropped ticker removes a
    // symbol from the screen, and an `info!` reporting only the survivors leaves no trace of it.
    let skipped = supplied - sectors.len();
    if skipped > 0 {
        warn!(skipped, "Skipped equity sector rows with unusable tickers");
    }
    info!(tickers = sectors.len(), "Equity sectors loaded");
    Ok(sectors)
}

/// Builds the detail frame from validated details.
///
/// Shared by the application, which reads them from PostgreSQL, and the trainer, which has no
/// database and parses the embedded CSV. One builder because the two paths feed the same
/// `consolidate_data`, and a column name or order that differed between them would surface as a
/// model trained on features the inference path does not produce.
pub fn details_to_dataframe(details: &[EquityDetail]) -> Result<DataFrame, PolarsError> {
    let mut tickers: Vec<String> = Vec::with_capacity(details.len());
    let mut sectors: Vec<String> = Vec::with_capacity(details.len());
    let mut industries: Vec<String> = Vec::with_capacity(details.len());
    for detail in details {
        tickers.push(detail.ticker().as_str().to_string());
        sectors.push(detail.sector().to_string());
        industries.push(detail.industry().to_string());
    }

    DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("sector".into(), sectors),
        Column::new("industry".into(), industries),
    ])
}

/// Loads all ticker metadata as a frame, for joining onto bars in the prediction pipeline.
pub async fn load_details_dataframe(pool: &PgPool) -> Result<DataFrame, DetailsError> {
    let rows = sqlx::query!(
        r#"SELECT ticker AS "ticker!", sector AS "sector!", industry AS "industry!"
           FROM equity_details
           ORDER BY ticker"#
    )
    .fetch_all(pool)
    .await?;

    let supplied = rows.len();
    let details: Vec<EquityDetail> = rows
        .into_iter()
        .filter_map(|row| {
            Ticker::new(&row.ticker)
                .map(|ticker| EquityDetail::new(ticker, row.sector, row.industry))
        })
        .collect();

    let skipped = supplied - details.len();
    if skipped > 0 {
        warn!(skipped, "Skipped equity detail rows with unusable tickers");
    }
    let dataframe = details_to_dataframe(&details)?;
    info!(rows = dataframe.height(), "Equity details loaded");
    Ok(dataframe)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A quoted company name containing a comma must not shift the later columns. Splitting on
    /// every comma reads the country as the sector, which is wrong in a way nothing downstream can
    /// detect.
    #[test]
    fn test_split_honours_quoted_commas() {
        let fields = split_csv_line(r#"AA,"Alcoa Corporation, Common Stock",$29.33,Industrials"#);
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[1], "Alcoa Corporation, Common Stock");
        assert_eq!(fields[3], "Industrials");
    }

    #[test]
    fn test_split_handles_empty_fields() {
        assert_eq!(split_csv_line("A,,C"), vec!["A", "", "C"]);
    }

    #[test]
    fn test_parse_reads_sector_and_industry_by_header_name() {
        let csv = "ticker,name,sector,industry\n\
                   AAPL,Apple Inc.,Technology,Consumer Electronics\n";
        let details = parse_details(csv).unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker().as_str(), "AAPL");
        assert_eq!(details[0].sector(), "Technology");
        assert_eq!(details[0].industry(), "Consumer Electronics");
    }

    /// Column order must not matter — the source is a third-party export that can reorder columns
    /// between refreshes.
    #[test]
    fn test_parse_is_insensitive_to_column_order() {
        let csv = "industry,ticker,name,sector\n\
                   Software,MSFT,Microsoft,Technology\n";
        let details = parse_details(csv).unwrap();
        assert_eq!(details[0].sector(), "Technology");
        assert_eq!(details[0].industry(), "Software");
    }

    #[test]
    fn test_parse_fills_blank_values_with_unknown() {
        let csv = "ticker,sector,industry\nAAPL,,\n";
        let details = parse_details(csv).unwrap();
        assert_eq!(details[0].sector(), UNKNOWN);
        assert_eq!(details[0].industry(), UNKNOWN);
    }

    /// The source listing includes test issues and non-equity instruments. One unusable ticker must
    /// not cost the rest of the file.
    #[test]
    fn test_parse_skips_unusable_tickers_without_failing() {
        let csv = "ticker,sector,industry\n\
                   AAPL,Technology,Consumer Electronics\n\
                   NOTATICKER123,Finance,Banks\n\
                   MSFT,Technology,Software\n";
        let details = parse_details(csv).unwrap();
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].ticker().as_str(), "AAPL");
        assert_eq!(details[1].ticker().as_str(), "MSFT");
    }

    #[test]
    fn test_parse_rejects_a_csv_without_the_required_columns() {
        assert!(parse_details("ticker,name\nAAPL,Apple\n").is_err());
        assert!(parse_details("").is_err());
    }

    #[test]
    fn test_parse_ignores_blank_lines() {
        let csv = "ticker,sector,industry\nAAPL,Technology,Hardware\n\n";
        assert_eq!(parse_details(csv).unwrap().len(), 1);
    }

    /// The embedded file is what seeds a fresh database, so it has to parse and it has to be
    /// substantial. A silently truncated file would leave the screen with almost no sector data.
    #[test]
    fn test_embedded_csv_parses_into_a_usable_universe() {
        let details = parse_embedded_details().expect("the embedded CSV must parse");
        assert!(
            details.len() > 5_000,
            "expected a full listing, got {} rows",
            details.len()
        );
        let with_sector = details
            .iter()
            .filter(|detail| detail.sector() != UNKNOWN)
            .count();
        assert!(
            with_sector > 3_000,
            "expected most rows to carry a sector, got {with_sector}"
        );
    }
}
