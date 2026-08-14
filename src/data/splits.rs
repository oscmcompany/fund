//! The stock splits the bar archive will be adjusted against; nothing reads them yet.
//!
//! One S3 object rather than a partition per session: a split belongs to its execution date, but
//! the feed revises and cancels announced ones, so only the whole table is ever authoritative.

use chrono::{DateTime, Utc};
use polars::prelude::*;

use crate::common::types::EquitySplit;

/// Builds the frame written to the archive, stamping every row with when this pass saw it.
///
/// `first_seen` is provenance rather than input: it answers whether a split was known to us when a
/// given bar partition was written, which is the question an adjustment disagreement turns into.
pub fn splits_to_dataframe(
    splits: &[EquitySplit],
    fetched_at: DateTime<Utc>,
) -> Result<DataFrame, PolarsError> {
    let mut identifiers: Vec<String> = Vec::with_capacity(splits.len());
    let mut tickers: Vec<String> = Vec::with_capacity(splits.len());
    let mut execution_dates: Vec<String> = Vec::with_capacity(splits.len());
    let mut splits_from: Vec<f64> = Vec::with_capacity(splits.len());
    let mut splits_to: Vec<f64> = Vec::with_capacity(splits.len());

    for split in splits {
        identifiers.push(split.id().to_string());
        tickers.push(split.ticker().to_string());
        execution_dates.push(split.execution_date().to_string());
        splits_from.push(split.split_from());
        splits_to.push(split.split_to());
    }
    let first_seen = vec![fetched_at.timestamp_millis(); splits.len()];

    DataFrame::new(vec![
        Column::new("id".into(), identifiers),
        Column::new("ticker".into(), tickers),
        Column::new("execution_date".into(), execution_dates),
        Column::new("split_from".into(), splits_from),
        Column::new("split_to".into(), splits_to),
        Column::new("first_seen".into(), first_seen),
    ])
}

/// Merges a stored splits table with a freshly fetched one.
///
/// The fetched rows are the row set: a split the feed no longer reports has been cancelled, and
/// keeping it would adjust prices across a split that never happened. Only `first_seen` survives
/// from the stored copy, taking the earlier of the two so a row keeps the date we first saw it
/// rather than the date of the latest fetch.
///
/// An empty fetch is the exception, and keeps the stored table. Replacing means an upstream answering
/// success with nothing would erase every corporate action we hold, and the feed has never held
/// nothing — it goes back to 1978.
pub fn merge_splits(existing: DataFrame, fetched: DataFrame) -> Result<DataFrame, PolarsError> {
    if fetched.height() == 0 {
        return Ok(existing);
    }

    let previously_seen = existing
        .lazy()
        .select([col("id"), col("first_seen").alias("previous_first_seen")])
        .group_by([col("id")])
        .agg([col("previous_first_seen").min()]);

    fetched
        .lazy()
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
        // Named rather than dropped, so the merged frame is written back with the same schema
        // `splits_to_dataframe` produces and the next read finds what it expects.
        .select([
            col("id"),
            col("ticker"),
            col("execution_date"),
            col("split_from"),
            col("split_to"),
            col("first_seen"),
        ])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{SessionDate, Ticker};

    fn instant(value: &str) -> DateTime<Utc> {
        value.parse().expect("a valid test instant")
    }

    fn split(id: &str, ticker: &str, execution_date: &str, from: f64, to: f64) -> EquitySplit {
        EquitySplit::new(
            id.to_string(),
            Ticker::new(ticker).expect("a valid ticker"),
            SessionDate::from_date(execution_date.parse().expect("a valid execution date")),
            from,
            to,
        )
        .expect("a valid split")
    }

    fn first_seen_by_id(frame: &DataFrame, id: &str) -> Option<i64> {
        let identifiers = frame.column("id").unwrap().str().unwrap();
        let seen = frame.column("first_seen").unwrap().i64().unwrap();
        (0..frame.height()).find_map(|row| (identifiers.get(row)? == id).then(|| seen.get(row))?)
    }

    #[test]
    fn test_the_frame_carries_every_field_a_split_has() {
        let frame = splits_to_dataframe(
            &[split("S1", "MNST", "2026-07-06", 1.0, 2.0)],
            instant("2026-08-14T02:00:00Z"),
        )
        .expect("a frame must build");

        assert_eq!(frame.height(), 1);
        assert_eq!(
            frame.column("ticker").unwrap().str().unwrap().get(0),
            Some("MNST")
        );
        assert_eq!(
            frame
                .column("execution_date")
                .unwrap()
                .str()
                .unwrap()
                .get(0),
            Some("2026-07-06")
        );
        assert_eq!(
            frame.column("split_from").unwrap().f64().unwrap().get(0),
            Some(1.0)
        );
        assert_eq!(
            frame.column("split_to").unwrap().f64().unwrap().get(0),
            Some(2.0)
        );
    }

    /// The reason the merge is not a union. A cancelled split disappears from the feed, and a table
    /// that only ever grew would keep adjusting prices across one that never executed.
    #[test]
    fn test_a_split_the_feed_no_longer_reports_is_dropped() {
        let stored = splits_to_dataframe(
            &[
                split("S1", "MNST", "2026-07-06", 1.0, 2.0),
                split("S2", "GONE", "2026-09-01", 1.0, 3.0),
            ],
            instant("2026-08-01T02:00:00Z"),
        )
        .unwrap();
        let fetched = splits_to_dataframe(
            &[split("S1", "MNST", "2026-07-06", 1.0, 2.0)],
            instant("2026-08-14T02:00:00Z"),
        )
        .unwrap();

        let merged = merge_splits(stored, fetched).expect("the merge must succeed");

        assert_eq!(merged.height(), 1);
        assert_eq!(
            merged.column("id").unwrap().str().unwrap().get(0),
            Some("S1")
        );
    }

    /// A row that survives keeps the date we first saw it, not the date of the latest fetch, so the
    /// column stays an answer about when a split became known rather than when this job last ran.
    #[test]
    fn test_a_surviving_split_keeps_the_earlier_first_seen() {
        let first = instant("2026-08-01T02:00:00Z");
        let stored =
            splits_to_dataframe(&[split("S1", "MNST", "2026-07-06", 1.0, 2.0)], first).unwrap();
        let fetched = splits_to_dataframe(
            &[
                split("S1", "MNST", "2026-07-06", 1.0, 2.0),
                split("S2", "NEW", "2026-12-17", 50.0, 1.0),
            ],
            instant("2026-08-14T02:00:00Z"),
        )
        .unwrap();

        let merged = merge_splits(stored, fetched).expect("the merge must succeed");

        assert_eq!(merged.height(), 2);
        assert_eq!(
            first_seen_by_id(&merged, "S1"),
            Some(first.timestamp_millis()),
            "a split already stored keeps when it was first seen"
        );
        assert_eq!(
            first_seen_by_id(&merged, "S2"),
            Some(instant("2026-08-14T02:00:00Z").timestamp_millis()),
            "a split seen for the first time is stamped with this fetch"
        );
    }

    /// The replace-the-row-set rule inverted: an upstream answering success with an empty page
    /// would otherwise erase every corporate action we hold, and log it as a refresh.
    #[test]
    fn test_an_empty_fetch_keeps_the_stored_table() {
        let stored = splits_to_dataframe(
            &[split("S1", "MNST", "2026-07-06", 1.0, 2.0)],
            instant("2026-08-01T02:00:00Z"),
        )
        .unwrap();
        let nothing = splits_to_dataframe(&[], instant("2026-08-14T02:00:00Z")).unwrap();

        let merged = merge_splits(stored, nothing).expect("the merge must succeed");

        assert_eq!(merged.height(), 1, "the stored table survives");
        assert_eq!(
            merged.column("id").unwrap().str().unwrap().get(0),
            Some("S1")
        );
    }

    /// The cold-bucket case: nothing stored, so every row is new and none of them lose their stamp
    /// to a null from the join.
    #[test]
    fn test_merging_into_an_empty_table_keeps_every_row_stamped() {
        let fetched_at = instant("2026-08-14T02:00:00Z");
        let empty = splits_to_dataframe(&[], fetched_at).unwrap();
        let fetched =
            splits_to_dataframe(&[split("S1", "MNST", "2026-07-06", 1.0, 2.0)], fetched_at)
                .unwrap();

        let merged = merge_splits(empty, fetched).expect("the merge must succeed");

        assert_eq!(merged.height(), 1);
        assert_eq!(
            first_seen_by_id(&merged, "S1"),
            Some(fetched_at.timestamp_millis())
        );
    }
}
