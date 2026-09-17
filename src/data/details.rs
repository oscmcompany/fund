//! Ticker metadata: sector and industry, which is what makes the screen's sector cap bind.
//!
//! Seeded from the archive's point-in-time reference dataset, which is the only thing that knows
//! what a symbol was rather than what today's ticker table says it is. The buckets themselves come
//! from `industry`, and the stored spelling is this module's.

use std::collections::HashMap;

use polars::prelude::*;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::types::{EquityDetail, Ticker};
use crate::data::industry;
use crate::data::industry_table::{Industry, Sector};

/// Value stored when the source has no SIC code, and so no sector or industry, for a ticker.
///
/// A storage encoding and not a bucket. Spelled rather than left null because the prediction path
/// filters null classifications out of the join and would drop these names from the universe
/// entirely; in memory the same absence is an `Option`, and each caller decides what it means. The
/// screen counts them as one allowance, the residual panel refuses them, and neither reading is
/// available to a caller handed a bare string.
///
/// Distinct from `Sector::Other`, which is a real group the definitions assign names to.
pub const UNKNOWN: &str = "NOT AVAILABLE";

/// The stored spelling of a sector, which round-trips through [`sector_of_stored`].
pub fn sector_code(sector: Option<Sector>) -> String {
    sector.map_or(UNKNOWN, |sector| sector.as_str()).to_string()
}

/// The stored spelling of an industry.
pub fn industry_code(industry: Option<Industry>) -> String {
    industry
        .map_or(UNKNOWN, |industry| industry.as_str())
        .to_string()
}

/// Reads a stored sector back into the domain type.
///
/// Unrecognised text reads as absent, which is the same answer the retired spelling gives: a value
/// this cannot place names no group, and inventing one would put a name into a factor it has no
/// claim to. The count of them is what says whether the table moved under the rows, so callers that
/// can report it should.
pub fn sector_of_stored(stored: &str) -> Option<Sector> {
    industry::sector_from_code(stored)
}

/// Rows per insert chunk. Three columns, so a thousand rows is well inside the bind parameter limit.
const INSERT_CHUNK_ROWS: usize = 1_000;

/// Errors syncing or reading ticker metadata.
#[derive(Debug, thiserror::Error)]
pub enum DetailsError {
    #[error("failed to read the reference universe: {0}")]
    Reference(String),
    #[error("failed to persist equity details: {0}")]
    Database(#[from] sqlx::Error),
    #[error("failed to build an equity detail frame: {0}")]
    Frame(#[from] PolarsError),
}

/// Reads validated details out of a point-in-time universe frame.
///
/// Rows whose ticker fails format validation are skipped rather than failing the batch: the feed
/// classifies test issues the archive's bar index still carries, and one bad row should not cost the
/// other seven thousand.
pub fn details_from_universe(universe: &DataFrame) -> Result<Vec<EquityDetail>, DetailsError> {
    let column = |name: &str| -> Result<&StringChunked, DetailsError> {
        universe
            .column(name)
            .and_then(|column| column.str())
            .map_err(|error| DetailsError::Reference(error.to_string()))
    };
    let tickers = column("ticker")?;
    let sectors = column("sector")?;
    let industries = column("industry")?;

    let mut details = Vec::with_capacity(universe.height());
    let mut skipped: usize = 0;
    for index in 0..universe.height() {
        let Some(ticker) = tickers.get(index).and_then(Ticker::new) else {
            skipped += 1;
            continue;
        };
        details.push(EquityDetail::new(
            ticker,
            sectors.get(index).unwrap_or(UNKNOWN).to_string(),
            industries.get(index).unwrap_or(UNKNOWN).to_string(),
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

/// Replaces `equity_details` with the supplied snapshot, inside one transaction.
///
/// A replace rather than an upsert, because the source is a point-in-time universe that genuinely
/// shrinks: a name that delists or reclassifies out of common stock leaves it, and an upsert would
/// keep admitting that name through the screen's inner join forever.
///
/// An empty snapshot writes nothing and leaves the stored table alone, because a sweep that answered
/// for nothing is a failed read rather than an empty market.
pub async fn store_details(pool: &PgPool, details: &[EquityDetail]) -> Result<u64, DetailsError> {
    if details.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;
    let mut transaction = pool.begin().await?;

    // Inside the transaction that refills it, so a failed insert leaves the previous universe in
    // place rather than an empty table the screen would read as a market with no names in it.
    sqlx::query!("DELETE FROM equity_details")
        .execute(&mut *transaction)
        .await?;

    for chunk in details.chunks(INSERT_CHUNK_ROWS) {
        let mut query_builder =
            sqlx::QueryBuilder::new("INSERT INTO equity_details (ticker, sector, industry) ");

        query_builder.push_values(chunk, |mut builder, detail| {
            builder
                .push_bind(detail.ticker())
                .push_bind(detail.sector().to_string())
                .push_bind(detail.industry().to_string());
        });

        // No `ON CONFLICT`: the delete above means a collision can only be a duplicate inside the
        // snapshot, and failing the transaction keeps the previous universe rather than picking one.
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
///
/// `None` against a ticker is the feed having given it no SIC code; a ticker absent from the map is
/// one the universe does not carry at all, and the two reach different arms of the cap.
pub async fn load_sectors(pool: &PgPool) -> Result<HashMap<Ticker, Option<Sector>>, DetailsError> {
    let rows =
        sqlx::query!(r#"SELECT ticker AS "ticker!", sector AS "sector!" FROM equity_details"#)
            .fetch_all(pool)
            .await?;

    let supplied = rows.len();
    let mut unclassified: usize = 0;
    let mut unrecognised: usize = 0;
    let sectors: HashMap<Ticker, Option<Sector>> = rows
        .into_iter()
        .filter_map(|row| {
            let ticker = Ticker::new(&row.ticker)?;
            let sector = sector_of_stored(&row.sector);
            // Separated because they send an operator to different places: one is the feed
            // declining to classify a name, and the other is a stored value this build cannot
            // place, which only happens when the table moved under rows written by an older one.
            match (&sector, row.sector.as_str()) {
                (None, UNKNOWN) => unclassified += 1,
                (None, _) => unrecognised += 1,
                (Some(_), _) => {}
            }
            Some((ticker, sector))
        })
        .collect();

    // Counted and reported, as `details_from_universe` does. A dropped ticker removes a symbol from
    // the screen, and an `info!` reporting only the survivors leaves no trace of it.
    let skipped = supplied - sectors.len();
    if skipped > 0 {
        warn!(skipped, "Skipped equity sector rows with unusable tickers");
    }
    if unrecognised > 0 {
        warn!(
            unrecognised,
            "Stored sectors name no known group; re-run the details refresh"
        );
    }
    info!(
        tickers = sectors.len(),
        unclassified, "Equity sectors loaded"
    );
    Ok(sectors)
}

/// Builds the detail frame from validated details.
///
/// One builder because the application's PostgreSQL read and any other caller feed the same
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

    fn universe(rows: &[(&str, Option<&str>, Option<&str>)]) -> DataFrame {
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "sector".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "industry".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
        ])
        .expect("the fixture must build")
    }

    #[test]
    fn test_details_are_read_by_column_name() {
        let details =
            details_from_universe(&universe(&[("AAPL", Some("35"), Some("3571"))])).unwrap();

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].ticker().as_str(), "AAPL");
        assert_eq!(details[0].sector(), "35");
        assert_eq!(details[0].industry(), "3571");
    }

    /// The bar index carries exchange test issues the feed still classifies. One unusable ticker
    /// must not cost the rest of the sweep.
    #[test]
    fn test_unusable_tickers_are_skipped_without_failing() {
        let details = details_from_universe(&universe(&[
            ("AAPL", Some("35"), Some("3571")),
            ("NOTATICKER123", Some("60"), Some("6021")),
            ("MSFT", Some("73"), Some("7372")),
        ]))
        .unwrap();

        assert_eq!(details.len(), 2);
        assert_eq!(details[0].ticker().as_str(), "AAPL");
        assert_eq!(details[1].ticker().as_str(), "MSFT");
    }

    /// A null must reach the screen as the sentinel, not as an empty string. Two names both marked
    /// unavailable count as one sector, which is the conservative reading the cap depends on.
    #[test]
    fn test_a_null_classification_becomes_the_sentinel() {
        let details = details_from_universe(&universe(&[("ZZZZ", None, None)])).unwrap();

        assert_eq!(details[0].sector(), "NOT AVAILABLE");
        assert_eq!(details[0].industry(), "NOT AVAILABLE");
    }

    #[test]
    fn test_a_frame_without_the_expected_columns_is_refused() {
        let wrong = DataFrame::new(vec![Column::new("symbol".into(), vec!["AAPL"])]).unwrap();

        assert!(details_from_universe(&wrong).is_err());
    }
}
