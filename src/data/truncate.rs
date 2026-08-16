//! Series truncation as a read-time filter over raw bars and the boundary table.
//!
//! Applied by the loaders, so a series spanning two companies is not something a caller can hold.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use tracing::warn;

use crate::common::types::SessionDate;
use crate::data::archive::{read_partition, ArchiveError, BOUNDARIES_ARCHIVE_KEY};

/// Column the join threshold is carried on, dropped before the frame is returned.
const THRESHOLD_COLUMN: &str = "earliest_usable_millis";

/// The boundary dates indexed for lookup, as read from the archive.
///
/// Holds only ticker and date: the reason explains a boundary but does not change what a reader
/// does with one, which is to stop.
#[derive(Debug, Clone, Default)]
pub struct BoundaryTable {
    by_ticker: HashMap<String, Vec<SessionDate>>,
}

impl BoundaryTable {
    /// Builds the index from the stored frame, ignoring rows it cannot read.
    ///
    /// A row that fails to parse is skipped rather than fatal, for the reason
    /// [`crate::data::adjust::SplitTable::from_dataframe`] skips one: the table covers the whole
    /// market, and one unreadable row should not cost every other ticker its guard.
    pub fn from_dataframe(frame: &DataFrame) -> Result<Self, PolarsError> {
        let tickers = frame.column("ticker")?.str()?;
        let dates = frame.column("date")?.str()?;

        let mut by_ticker: HashMap<String, Vec<SessionDate>> = HashMap::new();
        for row in 0..frame.height() {
            let (Some(ticker), Some(date)) = (tickers.get(row), dates.get(row)) else {
                continue;
            };
            let Ok(date) = NaiveDate::parse_from_str(date, "%Y-%m-%d") else {
                continue;
            };
            by_ticker
                .entry(ticker.to_string())
                .or_default()
                .push(SessionDate::from_date(date));
        }

        Ok(Self { by_ticker })
    }

    /// The earliest session of `ticker` describing the same company as `as_of` does.
    ///
    /// The latest boundary at or before `as_of`, because that is where the current run of sessions
    /// begins. A boundary dated after `as_of` has not happened yet from this read's point of view,
    /// and one long before it constrains nothing a lookback would reach.
    pub fn earliest_usable_session(&self, ticker: &str, as_of: SessionDate) -> Option<SessionDate> {
        self.by_ticker
            .get(ticker)?
            .iter()
            .filter(|date| **date <= as_of)
            .max()
            .copied()
    }

    /// Whether any boundary is known, which is what makes skipping the whole filter safe.
    pub fn is_empty(&self) -> bool {
        self.by_ticker.is_empty()
    }
}

/// The boundary table, reloaded when the cached copy is from an earlier Eastern date.
///
/// A daily table behind a daily cache, like [`crate::data::adjust::SplitTableCache`]: a boundary
/// falls on a session, so it cannot start applying part-way through one.
#[derive(Default)]
pub struct BoundaryTableCache {
    inner: tokio::sync::Mutex<Option<(SessionDate, Arc<BoundaryTable>)>>,
}

impl BoundaryTableCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns today's table, or `None` when the archive holds none.
    ///
    /// Absent is reported rather than flattened to an empty table, because the two mean opposite
    /// things: empty says no symbol changed hands, missing says nothing is known about whether one
    /// did. An absent object is not cached, so the next pass retries it.
    pub async fn get(
        &self,
        s3_client: &aws_sdk_s3::Client,
        bucket: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<Arc<BoundaryTable>>, ArchiveError> {
        let today = SessionDate::at(now);

        if let Some((cached_date, table)) = self.inner.lock().await.as_ref() {
            if *cached_date == today {
                return Ok(Some(Arc::clone(table)));
            }
        }

        let Some(frame) = read_partition(s3_client, bucket, BOUNDARIES_ARCHIVE_KEY).await? else {
            warn!(
                key = BOUNDARIES_ARCHIVE_KEY,
                "No boundary table in the archive; series cannot be bounded"
            );
            return Ok(None);
        };

        let table = Arc::new(BoundaryTable::from_dataframe(&frame)?);
        *self.inner.lock().await = Some((today, Arc::clone(&table)));
        Ok(Some(table))
    }
}

/// Drops the bars of each ticker that predate its most recent boundary.
///
/// Returns the frame untouched when no boundary is known, which is the ordinary case: fifteen of
/// the market's liquid names carry one in a year.
pub fn truncate_bars(
    frame: DataFrame,
    boundaries: &BoundaryTable,
    as_of: SessionDate,
) -> Result<DataFrame, PolarsError> {
    if boundaries.is_empty() || frame.height() == 0 {
        return Ok(frame);
    }

    let tickers = frame.column("ticker")?.str()?;
    let mut bounded: Vec<String> = Vec::new();
    let mut thresholds: Vec<i64> = Vec::new();
    let mut seen: HashMap<&str, ()> = HashMap::new();
    for row in 0..frame.height() {
        let Some(ticker) = tickers.get(row) else {
            continue;
        };
        if seen.insert(ticker, ()).is_some() {
            continue;
        }
        if let Some(earliest) = boundaries.earliest_usable_session(ticker, as_of) {
            bounded.push(ticker.to_string());
            thresholds.push(earliest.midnight().timestamp_millis());
        }
    }
    if bounded.is_empty() {
        return Ok(frame);
    }

    let cuts = DataFrame::new(vec![
        Column::new("ticker".into(), bounded),
        Column::new(THRESHOLD_COLUMN.into(), thresholds),
    ])?;

    frame
        .lazy()
        .join(
            cuts.lazy(),
            [col("ticker")],
            [col("ticker")],
            JoinArgs::new(JoinType::Left),
        )
        // A ticker with no boundary joins to null and keeps every bar; one with a boundary keeps the
        // sessions from it onward, the run that describes the company trading under the symbol now.
        .filter(
            col(THRESHOLD_COLUMN)
                .is_null()
                .or(col("timestamp").gt_eq(col(THRESHOLD_COLUMN))),
        )
        .drop(Selector::ByName {
            names: vec![PlSmallStr::from(THRESHOLD_COLUMN)].into(),
            strict: false,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(value: &str) -> SessionDate {
        SessionDate::from_date(value.parse().expect("a valid session date"))
    }

    fn table(rows: &[(&str, &str)]) -> BoundaryTable {
        let tickers: Vec<String> = rows.iter().map(|(ticker, _)| ticker.to_string()).collect();
        let dates: Vec<String> = rows.iter().map(|(_, date)| date.to_string()).collect();
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("date".into(), dates),
        ])
        .expect("a frame must build");
        BoundaryTable::from_dataframe(&frame).expect("the table must build")
    }

    fn bars(rows: &[(&str, &str, f64)]) -> DataFrame {
        let tickers: Vec<String> = rows
            .iter()
            .map(|(ticker, _, _)| ticker.to_string())
            .collect();
        let timestamps: Vec<i64> = rows
            .iter()
            .map(|(_, date, _)| session(date).midnight().timestamp_millis())
            .collect();
        let closes: Vec<f64> = rows.iter().map(|(_, _, close)| *close).collect();
        DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("timestamp".into(), timestamps),
            Column::new("close_price".into(), closes),
        ])
        .expect("a frame must build")
    }

    #[test]
    fn test_the_latest_boundary_at_or_before_as_of_is_the_one_that_applies() {
        let table = table(&[("RNA", "2022-06-09"), ("RNA", "2026-02-26")]);

        assert_eq!(
            table.earliest_usable_session("RNA", session("2026-08-15")),
            Some(session("2026-02-26"))
        );
    }

    /// The feed carries actions ahead of their date, exactly as the splits table does. One that has
    /// not happened yet must not shorten a window that ends before it.
    #[test]
    fn test_a_boundary_after_as_of_does_not_apply() {
        let table = table(&[("RNA", "2026-02-26")]);

        assert_eq!(
            table.earliest_usable_session("RNA", session("2026-01-05")),
            None
        );
    }

    #[test]
    fn test_a_ticker_with_no_boundary_is_unconstrained() {
        let table = table(&[("RNA", "2026-02-26")]);

        assert_eq!(
            table.earliest_usable_session("AAPL", session("2026-08-15")),
            None
        );
    }

    /// The case the table exists for. `RNA` was Avidity Biosciences until 2026-02-26 and Atrium
    /// Therapeutics after it, so a window spanning that date holds two companies' prices.
    #[test]
    fn test_bars_before_a_boundary_are_dropped() {
        let frame = bars(&[
            ("RNA", "2026-02-24", 72.87),
            ("RNA", "2026-02-26", 14.10),
            ("RNA", "2026-02-27", 14.35),
        ]);

        let truncated = truncate_bars(
            frame,
            &table(&[("RNA", "2026-02-26")]),
            session("2026-08-15"),
        )
        .expect("the truncation must succeed");

        assert_eq!(truncated.height(), 2, "only the sessions from the boundary");
        let closes = truncated.column("close_price").unwrap().f64().unwrap();
        assert_eq!(closes.get(0), Some(14.10));
    }

    #[test]
    fn test_a_ticker_without_a_boundary_keeps_every_bar() {
        let frame = bars(&[
            ("AAPL", "2026-02-24", 190.0),
            ("AAPL", "2026-02-26", 191.0),
            ("RNA", "2026-02-24", 72.87),
        ]);

        let truncated = truncate_bars(
            frame,
            &table(&[("RNA", "2026-02-26")]),
            session("2026-08-15"),
        )
        .expect("the truncation must succeed");

        let tickers = truncated.column("ticker").unwrap().str().unwrap();
        let kept: Vec<&str> = (0..truncated.height())
            .filter_map(|row| tickers.get(row))
            .collect();
        assert_eq!(kept, vec!["AAPL", "AAPL"], "RNA loses its pre-boundary bar");
    }

    #[test]
    fn test_an_empty_table_leaves_the_frame_alone() {
        let frame = bars(&[("RNA", "2026-02-24", 72.87)]);

        let truncated =
            truncate_bars(frame, &BoundaryTable::default(), session("2026-08-15")).unwrap();

        assert_eq!(truncated.height(), 1);
    }
}
