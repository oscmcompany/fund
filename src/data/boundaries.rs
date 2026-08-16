//! The dates a symbol's price series may not be read across; nothing reads them yet.
//!
//! One S3 object, refreshed over a date range and merged with what is stored.

use chrono::{DateTime, Utc};
use polars::prelude::*;

use crate::common::types::{BoundaryReason, SeriesBoundary, SessionDate};

/// Builds the frame written to the archive, stamping every row with when this pass saw it.
///
/// `related_ticker` holds whichever symbol the reason names — where a rename continues, or which
/// company a spinoff distributed — and is null for the reasons that name neither.
pub fn boundaries_to_dataframe(
    boundaries: &[SeriesBoundary],
    fetched_at: DateTime<Utc>,
) -> Result<DataFrame, PolarsError> {
    let mut identifiers: Vec<String> = Vec::with_capacity(boundaries.len());
    let mut tickers: Vec<String> = Vec::with_capacity(boundaries.len());
    let mut dates: Vec<String> = Vec::with_capacity(boundaries.len());
    let mut process_dates: Vec<String> = Vec::with_capacity(boundaries.len());
    let mut reasons: Vec<String> = Vec::with_capacity(boundaries.len());
    let mut related: Vec<Option<String>> = Vec::with_capacity(boundaries.len());

    for boundary in boundaries {
        identifiers.push(boundary.id().to_string());
        tickers.push(boundary.ticker().as_str().to_string());
        dates.push(boundary.date().date().format("%Y-%m-%d").to_string());
        process_dates.push(
            boundary
                .process_date()
                .date()
                .format("%Y-%m-%d")
                .to_string(),
        );
        reasons.push(boundary.reason().as_str().to_string());
        related.push(match boundary.reason() {
            BoundaryReason::Renamed { to } => Some(to.as_str().to_string()),
            BoundaryReason::SpunOff { spin_off_company } => {
                Some(spin_off_company.as_str().to_string())
            }
            BoundaryReason::RightsDistributed
            | BoundaryReason::UnitSeparated
            | BoundaryReason::Reorganized => None,
        });
    }
    let first_seen = vec![fetched_at.timestamp_millis(); boundaries.len()];

    DataFrame::new(vec![
        Column::new("id".into(), identifiers),
        Column::new("ticker".into(), tickers),
        Column::new("date".into(), dates),
        Column::new("process_date".into(), process_dates),
        Column::new("reason".into(), reasons),
        Column::new("related_ticker".into(), related),
        Column::new("first_seen".into(), first_seen),
    ])
}

/// Merges a stored boundary table with one fetched over `start..=end`.
///
/// Unlike [`crate::data::splits::merge_splits`], which replaces the row set outright because that
/// feed answers with its whole history, this one answers only for a range — so stored rows the
/// refresh could not have re-reported survive, and absence within it means cancelled.
///
/// The range is matched on `process_date`, never on `date`. Alpaca filters a request by when it
/// processed an action, while the boundary is stamped with when the price moved, and those differ
/// by years: filtering survivors on the wrong one leaves a cancelled action archived forever.
pub fn merge_boundaries(
    existing: DataFrame,
    fetched: DataFrame,
    start: SessionDate,
    end: SessionDate,
) -> Result<DataFrame, PolarsError> {
    let start_text = start.date().format("%Y-%m-%d").to_string();
    let end_text = end.date().format("%Y-%m-%d").to_string();

    // Compared as text: the column is written `%Y-%m-%d`, which orders lexicographically exactly as
    // it orders chronologically.
    let outside_window = existing
        .clone()
        .lazy()
        .join(
            fetched.clone().lazy().select([col("id")]),
            [col("id")],
            [col("id")],
            JoinArgs::new(JoinType::Anti),
        )
        .filter(
            col("process_date")
                .lt(lit(start_text.clone()))
                .or(col("process_date").gt(lit(end_text.clone()))),
        );

    let previously_seen = existing
        .lazy()
        .select([col("id"), col("first_seen").alias("previous_first_seen")])
        .group_by([col("id")])
        .agg([col("previous_first_seen").min()]);

    let refreshed = fetched
        .lazy()
        // One row per identifier before the join, so a repeat across pages cannot become two rows.
        .unique_stable(
            Some(polars::prelude::Selector::ByName {
                names: vec![PlSmallStr::from("id")].into(),
                strict: false,
            }),
            UniqueKeepStrategy::First,
        )
        .join(
            previously_seen,
            [col("id")],
            [col("id")],
            JoinArgs::new(JoinType::Left),
        )
        // Defaulted before it is compared, because a row absent from the stored table joins to null
        // and a null comparison would propagate rather than choose a branch.
        .with_column(
            coalesce(&[col("previous_first_seen"), col("first_seen")]).alias("previous_first_seen"),
        )
        .with_column(
            when(col("previous_first_seen").lt(col("first_seen")))
                .then(col("previous_first_seen"))
                .otherwise(col("first_seen"))
                .alias("first_seen"),
        )
        .select([
            col("id"),
            col("ticker"),
            col("date"),
            col("process_date"),
            col("reason"),
            col("related_ticker"),
            col("first_seen"),
        ]);

    concat(
        [outside_window, refreshed],
        UnionArgs {
            rechunk: true,
            ..Default::default()
        },
    )?
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Ticker;

    fn instant(value: &str) -> DateTime<Utc> {
        value.parse().expect("a valid test instant")
    }

    fn session(value: &str) -> SessionDate {
        SessionDate::from_date(value.parse().expect("a valid session date"))
    }

    /// Processed on the day the price moved, which is the ordinary case. The tests that turn on the
    /// two dates differing use `renamed_processed` instead.
    fn renamed(id: &str, ticker: &str, date: &str, to: &str) -> SeriesBoundary {
        renamed_processed(id, ticker, date, date, to)
    }

    fn renamed_processed(
        id: &str,
        ticker: &str,
        date: &str,
        process_date: &str,
        to: &str,
    ) -> SeriesBoundary {
        SeriesBoundary::new(
            id.to_string(),
            Ticker::new(ticker).expect("a valid ticker"),
            session(date),
            session(process_date),
            BoundaryReason::Renamed {
                to: Ticker::new(to).expect("a valid ticker"),
            },
        )
        .expect("a valid boundary")
    }

    fn rights(id: &str, ticker: &str, date: &str) -> SeriesBoundary {
        SeriesBoundary::new(
            id.to_string(),
            Ticker::new(ticker).expect("a valid ticker"),
            session(date),
            session(date),
            BoundaryReason::RightsDistributed,
        )
        .expect("a valid boundary")
    }

    fn identifiers(frame: &DataFrame) -> Vec<String> {
        let column = frame.column("id").unwrap().str().unwrap();
        let mut found: Vec<String> = (0..frame.height())
            .filter_map(|row| column.get(row).map(str::to_string))
            .collect();
        found.sort();
        found
    }

    #[test]
    fn test_the_frame_carries_the_reason_and_the_symbol_it_names() {
        let frame = boundaries_to_dataframe(
            &[
                renamed("n1", "RNA", "2026-02-26", "RNAM"),
                rights("r1", "AIM", "2026-02-10"),
            ],
            instant("2026-08-15T02:00:00Z"),
        )
        .expect("a frame must build");

        let reasons = frame.column("reason").unwrap().str().unwrap();
        let related = frame.column("related_ticker").unwrap().str().unwrap();

        assert_eq!(reasons.get(0), Some("renamed"));
        assert_eq!(related.get(0), Some("RNAM"));
        assert_eq!(reasons.get(1), Some("rights_distributed"));
        assert_eq!(
            related.get(1),
            None,
            "a reason that names no symbol stores none"
        );
    }

    /// The cancellation this table exists to record, in the shape that is easiest to miss. The
    /// action was processed inside the refreshed window but is stamped years outside it, so
    /// matching survivors on the stored date would keep it forever and truncate real history at a
    /// date nothing happened on.
    #[test]
    fn test_a_cancelled_action_processed_inside_the_window_is_dropped() {
        let stored = boundaries_to_dataframe(
            &[
                renamed_processed("cancelled", "CNCL", "2017-03-01", "2026-06-10", "OTHR"),
                renamed_processed("kept", "FB", "2022-06-09", "2022-06-09", "META"),
            ],
            instant("2026-08-01T02:00:00Z"),
        )
        .unwrap();
        let nothing = boundaries_to_dataframe(&[], instant("2026-08-15T02:00:00Z")).unwrap();

        let merged = merge_boundaries(
            stored,
            nothing,
            session("2026-06-01"),
            session("2026-06-30"),
        )
        .expect("the merge must succeed");

        assert_eq!(
            identifiers(&merged),
            vec!["kept"],
            "the refresh covered the cancelled action's processing, so its absence is authoritative"
        );
    }

    /// The request window filters a different date than the one stored, so the feed returns rows
    /// stamped well outside it — four of them on the first live refresh. Splitting the stored table
    /// on date alone put those in both halves and wrote them twice.
    #[test]
    fn test_a_row_returned_from_outside_the_window_is_kept_once() {
        let stored = boundaries_to_dataframe(
            &[renamed("old", "FB", "2022-06-09", "META")],
            instant("2026-08-01T02:00:00Z"),
        )
        .unwrap();
        let fetched = boundaries_to_dataframe(
            &[renamed("old", "FB", "2022-06-09", "META")],
            instant("2026-08-15T02:00:00Z"),
        )
        .unwrap();

        let merged = merge_boundaries(
            stored,
            fetched,
            session("2026-06-01"),
            session("2026-06-30"),
        )
        .expect("the merge must succeed");

        assert_eq!(
            identifiers(&merged),
            vec!["old"],
            "a stored row the fetch returned again is replaced, not duplicated"
        );
    }

    /// The difference from the splits merge, and the reason this one takes a window at all. A
    /// refresh covering this year must not erase a boundary recorded for a previous one.
    #[test]
    fn test_a_boundary_outside_the_refreshed_window_survives() {
        let stored = boundaries_to_dataframe(
            &[
                renamed("old", "FB", "2022-06-09", "META"),
                renamed("n1", "RNA", "2026-02-26", "RNAM"),
            ],
            instant("2026-08-01T02:00:00Z"),
        )
        .unwrap();
        let fetched = boundaries_to_dataframe(
            &[renamed("n1", "RNA", "2026-02-26", "RNAM")],
            instant("2026-08-15T02:00:00Z"),
        )
        .unwrap();

        let merged = merge_boundaries(
            stored,
            fetched,
            session("2026-01-01"),
            session("2026-08-14"),
        )
        .expect("the merge must succeed");

        assert_eq!(identifiers(&merged), vec!["n1", "old"]);
    }

    /// Inside the window the fetch is authoritative, so an action the feed stopped reporting is
    /// dropped rather than kept — the same rule the splits table applies to its whole history.
    #[test]
    fn test_a_boundary_the_feed_no_longer_reports_inside_the_window_is_dropped() {
        let stored = boundaries_to_dataframe(
            &[
                renamed("n1", "RNA", "2026-02-26", "RNAM"),
                renamed("gone", "CNCL", "2026-03-01", "OTHR"),
            ],
            instant("2026-08-01T02:00:00Z"),
        )
        .unwrap();
        let fetched = boundaries_to_dataframe(
            &[renamed("n1", "RNA", "2026-02-26", "RNAM")],
            instant("2026-08-15T02:00:00Z"),
        )
        .unwrap();

        let merged = merge_boundaries(
            stored,
            fetched,
            session("2026-01-01"),
            session("2026-08-14"),
        )
        .expect("the merge must succeed");

        assert_eq!(identifiers(&merged), vec!["n1"]);
    }

    #[test]
    fn test_a_surviving_boundary_keeps_the_earlier_first_seen() {
        let first = instant("2026-08-01T02:00:00Z");
        let stored =
            boundaries_to_dataframe(&[renamed("n1", "RNA", "2026-02-26", "RNAM")], first).unwrap();
        let fetched = boundaries_to_dataframe(
            &[renamed("n1", "RNA", "2026-02-26", "RNAM")],
            instant("2026-08-15T02:00:00Z"),
        )
        .unwrap();

        let merged = merge_boundaries(
            stored,
            fetched,
            session("2026-01-01"),
            session("2026-08-14"),
        )
        .expect("the merge must succeed");

        assert_eq!(
            merged.column("first_seen").unwrap().i64().unwrap().get(0),
            Some(first.timestamp_millis())
        );
    }

    /// A window with no corporate actions in it is ordinary, and clears what the window held.
    #[test]
    fn test_an_empty_window_clears_only_that_window() {
        let stored = boundaries_to_dataframe(
            &[
                renamed("old", "FB", "2022-06-09", "META"),
                renamed("n1", "RNA", "2026-02-26", "RNAM"),
            ],
            instant("2026-08-01T02:00:00Z"),
        )
        .unwrap();
        let nothing = boundaries_to_dataframe(&[], instant("2026-08-15T02:00:00Z")).unwrap();

        let merged = merge_boundaries(
            stored,
            nothing,
            session("2026-01-01"),
            session("2026-08-14"),
        )
        .expect("the merge must succeed");

        assert_eq!(identifiers(&merged), vec!["old"]);
    }
}
