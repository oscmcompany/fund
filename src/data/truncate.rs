//! Series truncation as a read-time filter over raw bars and the boundary table.
//!
//! Applied by the loaders, so a series spanning two companies is not something a caller can hold.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use tracing::warn;

use crate::common::types::{SessionDate, Ticker};
use crate::data::archive::{read_partition, ArchiveError, BOUNDARIES_ARCHIVE_KEY};

/// Column the join threshold is carried on, dropped before the frame is returned.
const THRESHOLD_COLUMN: &str = "earliest_usable_millis";

/// Renames followed before a chain is declared circular.
///
/// A symbol changing hands eight times inside one lookback is not something the feed describes; a
/// table that pointed a symbol back at itself is, and would otherwise loop forever.
const RENAME_CHAIN_LIMIT: usize = 8;

/// The boundary dates indexed for lookup, as read from the archive.
///
/// Renames are held separately from the rest because only they answer a second question: every
/// boundary says where a series stops, and a rename also says where it continues.
#[derive(Debug, Clone, Default)]
pub struct BoundaryTable {
    by_ticker: HashMap<String, Vec<SessionDate>>,
    renames: HashMap<String, Vec<(SessionDate, String)>>,
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
        let reasons = frame.column("reason")?.str()?;
        let related = frame.column("related_ticker")?.str()?;

        let mut by_ticker: HashMap<String, Vec<SessionDate>> = HashMap::new();
        let mut renames: HashMap<String, Vec<(SessionDate, String)>> = HashMap::new();
        for row in 0..frame.height() {
            let (Some(ticker), Some(date)) = (tickers.get(row), dates.get(row)) else {
                continue;
            };
            // Validated here rather than on every lookup: the index is keyed by string because
            // `current_symbol` runs per row over millions of them, as `SplitTable` is for the fold.
            let Some(ticker) = Ticker::new(ticker) else {
                continue;
            };
            let ticker = ticker.as_str();
            let Ok(date) = NaiveDate::parse_from_str(date, "%Y-%m-%d") else {
                continue;
            };
            let date = SessionDate::from_date(date);
            by_ticker.entry(ticker.to_string()).or_default().push(date);

            if reasons.get(row) == Some("renamed") {
                if let Some(successor) = related.get(row) {
                    renames
                        .entry(ticker.to_string())
                        .or_default()
                        .push((date, successor.to_string()));
                }
            }
        }

        Ok(Self { by_ticker, renames })
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

    /// The symbol a fact about `ticker` dated `session` belongs to now, if it moved.
    ///
    /// Follows every rename dated after the session, so a company renamed twice is reached in two
    /// steps. A rename dated at or before the session is somebody else's: the symbol was free by
    /// then, and whoever took it is who `ticker` means from that date on.
    ///
    /// Used for splits as well as bars, which is what keeps the fold correct across a rename —
    /// otherwise a stitched bar takes the successor's splits and misses its own, or the reverse.
    /// A chain that revisits a symbol stops there rather than going round again, keeping the last
    /// symbol reached: the table is malformed at that point, and the walk so far is still the best
    /// answer available.
    pub fn current_symbol(&self, ticker: &str, session: SessionDate) -> Option<String> {
        let mut symbol = ticker.to_string();
        let mut visited: HashSet<String> = HashSet::from([symbol.clone()]);
        for _ in 0..RENAME_CHAIN_LIMIT {
            let Some(successor) = self
                .renames
                .get(&symbol)
                .and_then(|renames| {
                    renames
                        .iter()
                        .filter(|(date, _)| *date > session)
                        .min_by_key(|(date, _)| *date)
                })
                .map(|(_, successor)| successor.clone())
            else {
                break;
            };
            if !visited.insert(successor.clone()) {
                break;
            }
            symbol = successor;
        }

        (symbol != ticker).then_some(symbol)
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

/// Re-files each bar under the symbol its company trades as now.
///
/// A renamed company's history sits under a symbol that stopped trading, which leaves the successor
/// too short to screen while the predecessor is unusable. Relabelling joins them into one series.
///
/// Runs before [`truncate_bars`], because the bars it moves are exactly the ones truncation would
/// otherwise drop, and before the fold, because the fold keys on the symbol a bar now carries.
pub fn stitch_bars(frame: DataFrame, boundaries: &BoundaryTable) -> Result<DataFrame, PolarsError> {
    if boundaries.is_empty() || frame.height() == 0 {
        return Ok(frame);
    }

    let tickers = frame.column("ticker")?.str()?;
    let timestamps = frame.column("timestamp")?.i64()?;
    let mut relabelled: Vec<String> = Vec::with_capacity(frame.height());
    let mut moved = 0usize;
    for row in 0..frame.height() {
        let (Some(ticker), Some(timestamp)) = (tickers.get(row), timestamps.get(row)) else {
            relabelled.push(String::new());
            continue;
        };
        let session =
            SessionDate::at(DateTime::from_timestamp_millis(timestamp).unwrap_or_default());
        match boundaries.current_symbol(ticker, session) {
            Some(successor) => {
                moved += 1;
                relabelled.push(successor);
            }
            None => relabelled.push(ticker.to_string()),
        }
    }

    if moved == 0 {
        return Ok(frame);
    }

    let mut frame = frame;
    frame.with_column(Column::new("ticker".into(), relabelled))?;
    Ok(frame)
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

    /// Boundaries that stop a series without continuing it, like a spinoff.
    fn table(rows: &[(&str, &str)]) -> BoundaryTable {
        let renames: Vec<(&str, &str, Option<&str>)> = rows
            .iter()
            .map(|(ticker, date)| (*ticker, *date, None))
            .collect();
        renamed_table(&renames)
    }

    /// `None` in the third position is a spinoff; `Some` is a rename onto that symbol.
    fn renamed_table(rows: &[(&str, &str, Option<&str>)]) -> BoundaryTable {
        let tickers: Vec<String> = rows
            .iter()
            .map(|(ticker, _, _)| ticker.to_string())
            .collect();
        let dates: Vec<String> = rows.iter().map(|(_, date, _)| date.to_string()).collect();
        let reasons: Vec<String> = rows
            .iter()
            .map(|(_, _, to)| match to {
                Some(_) => "renamed".to_string(),
                None => "spun_off".to_string(),
            })
            .collect();
        let related: Vec<Option<String>> = rows
            .iter()
            .map(|(_, _, to)| to.map(str::to_string))
            .collect();
        let frame = DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("date".into(), dates),
            Column::new("reason".into(), reasons),
            Column::new("related_ticker".into(), related),
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

    /// A bar predating a rename belongs to the symbol the company trades as now; one from after it
    /// belongs to whoever took the vacated symbol.
    #[test]
    fn test_a_session_resolves_to_the_symbol_its_company_trades_as_now() {
        let table = renamed_table(&[("RNA", "2026-02-26", Some("RNAM"))]);

        assert_eq!(
            table.current_symbol("RNA", session("2026-02-24")),
            Some("RNAM".to_string())
        );
        assert_eq!(table.current_symbol("RNA", session("2026-02-26")), None);
        assert_eq!(table.current_symbol("RNA", session("2026-03-10")), None);
    }

    #[test]
    fn test_a_chain_of_renames_is_followed_to_the_end() {
        let table = renamed_table(&[
            ("AAA", "2026-03-01", Some("BBB")),
            ("BBB", "2026-05-01", Some("CCC")),
        ]);

        assert_eq!(
            table.current_symbol("AAA", session("2026-01-05")),
            Some("CCC".to_string())
        );
        assert_eq!(
            table.current_symbol("AAA", session("2026-04-01")),
            None,
            "the symbol was already free, so a later bar under it is somebody else's"
        );
    }

    /// A table pointing a symbol back at itself is malformed rather than impossible, and would
    /// otherwise be followed forever.
    #[test]
    fn test_a_circular_rename_terminates() {
        let table = renamed_table(&[
            ("AAA", "2026-03-01", Some("BBB")),
            ("BBB", "2026-05-01", Some("AAA")),
        ]);

        assert_eq!(
            table.current_symbol("AAA", session("2026-01-05")),
            Some("BBB".to_string()),
            "the walk stops where the cycle closes rather than going round again"
        );
    }

    #[test]
    fn test_bars_from_before_a_rename_move_to_the_successor() {
        let frame = bars(&[("RNA", "2026-02-24", 72.87), ("RNAM", "2026-02-26", 73.10)]);

        let stitched = stitch_bars(
            frame,
            &renamed_table(&[("RNA", "2026-02-26", Some("RNAM"))]),
        )
        .unwrap();

        let tickers = stitched.column("ticker").unwrap().str().unwrap();
        let labels: Vec<&str> = (0..stitched.height())
            .filter_map(|row| tickers.get(row))
            .collect();
        assert_eq!(labels, vec!["RNAM", "RNAM"], "one company, one symbol");
    }

    /// The reason the fold has to follow renames too. Without it a stitched bar takes whatever
    /// splits are filed under its new symbol and misses the ones filed under its old one.
    #[test]
    fn test_a_split_moves_with_the_company_that_declared_it() {
        let boundaries = renamed_table(&[("RNA", "2026-02-26", Some("RNAM"))]);
        let splits = crate::data::adjust::SplitTable::from_dataframe(
            &DataFrame::new(vec![
                Column::new("ticker".into(), vec!["RNA".to_string(), "RNA".to_string()]),
                Column::new(
                    "execution_date".into(),
                    vec!["2026-02-10".to_string(), "2026-04-01".to_string()],
                ),
                Column::new("split_from".into(), vec![1.0, 1.0]),
                Column::new("split_to".into(), vec![2.0, 4.0]),
            ])
            .unwrap(),
        )
        .unwrap();

        let followed = splits.following_renames(&boundaries);

        assert_eq!(
            followed.factor_at("RNAM", session("2026-02-01"), session("2026-08-15")),
            0.5,
            "the pre-rename split follows the company onto its new symbol"
        );
        assert_eq!(
            followed.factor_at("RNA", session("2026-03-01"), session("2026-08-15")),
            0.25,
            "the post-rename split stays with whoever holds the old symbol now"
        );
    }

    #[test]
    fn test_an_empty_table_leaves_the_frame_alone() {
        let frame = bars(&[("RNA", "2026-02-24", 72.87)]);

        let truncated =
            truncate_bars(frame, &BoundaryTable::default(), session("2026-08-15")).unwrap();

        assert_eq!(truncated.height(), 1);
    }
}
