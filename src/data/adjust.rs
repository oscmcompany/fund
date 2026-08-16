//! Split adjustment as a read-time fold over raw bars and the splits table.
//!
//! Applied by the loaders themselves, so an unadjusted series is not something a caller can hold.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use polars::prelude::*;
use tracing::warn;

use crate::common::types::SessionDate;
use crate::data::archive::{read_partition, ArchiveError, SPLITS_ARCHIVE_KEY};
use crate::data::truncate::BoundaryTable;

/// The splits table indexed for lookup, as read from the archive.
///
/// Holds only the ratio and the date, because that is all an adjustment needs — the identifier and
/// provenance answer other questions.
#[derive(Debug, Clone, Default)]
pub struct SplitTable {
    by_ticker: HashMap<String, Vec<(SessionDate, f64)>>,
}

impl SplitTable {
    /// Builds the index from the stored frame, ignoring rows it cannot read.
    ///
    /// A row that fails to parse is skipped rather than fatal: the table covers the whole market
    /// back to 1978, and one unreadable row should not cost the adjustment of every other ticker.
    pub fn from_dataframe(frame: &DataFrame) -> Result<Self, PolarsError> {
        let tickers = frame.column("ticker")?.str()?;
        let execution_dates = frame.column("execution_date")?.str()?;
        let splits_from = frame.column("split_from")?.f64()?;
        let splits_to = frame.column("split_to")?.f64()?;

        let mut by_ticker: HashMap<String, Vec<(SessionDate, f64)>> = HashMap::new();
        for row in 0..frame.height() {
            let (Some(ticker), Some(execution_date), Some(split_from), Some(split_to)) = (
                tickers.get(row),
                execution_dates.get(row),
                splits_from.get(row),
                splits_to.get(row),
            ) else {
                continue;
            };
            let Ok(execution_date) = NaiveDate::parse_from_str(execution_date, "%Y-%m-%d") else {
                continue;
            };
            // Guarded rather than assumed, because a frame can be read from an object this build
            // did not write. Both sides *and* the quotient: two negatives divide to a plausible
            // ratio, and two positives can still overflow or underflow to one that is not.
            if !split_from.is_finite()
                || !split_to.is_finite()
                || split_from <= 0.0
                || split_to <= 0.0
            {
                continue;
            }
            let ratio = split_from / split_to;
            if !ratio.is_finite() || ratio <= 0.0 {
                continue;
            }
            by_ticker
                .entry(ticker.to_string())
                .or_default()
                .push((SessionDate::from_date(execution_date), ratio));
        }

        Ok(Self { by_ticker })
    }

    /// The factor putting a bar of `ticker` on `session` onto `as_of`'s share basis.
    ///
    /// Splits are applied when they execute strictly after the bar and no later than `as_of`. The
    /// lower bound is exclusive because a split executes at the open, so the bar stamped that day
    /// already reflects it. The upper bound is what keeps an *announced* split out: the table
    /// carries them months ahead, and applying one would restate today's history onto a basis the
    /// market has not moved to, leaving it incomparable with the live quote it gets screened against.
    ///
    /// Always a usable factor. Each ratio is finite and positive, but a product of them need not
    /// be, and an unusable one falls back to 1.0 — the same answer an unknown ticker gets — so no
    /// caller has to re-check a number it is about to divide by.
    pub fn factor_at(&self, ticker: &str, session: SessionDate, as_of: SessionDate) -> f64 {
        let factor: f64 = self
            .by_ticker
            .get(ticker)
            .map(|splits| {
                splits
                    .iter()
                    .filter(|(execution_date, _)| {
                        *execution_date > session && *execution_date <= as_of
                    })
                    .map(|(_, ratio)| ratio)
                    .product()
            })
            .unwrap_or(1.0);

        if factor.is_finite() && factor > 0.0 {
            factor
        } else {
            1.0
        }
    }

    /// Re-files each split under the symbol its company trades as now.
    ///
    /// Needed because [`crate::data::truncate::stitch_bars`] moves a bar to its company's current
    /// symbol while the split stays filed under the one in force when it executed. Left unresolved,
    /// a stitched bar would take the successor's splits and miss its own — and 18 of 330 renames in
    /// a year have a split on one side of them, so this is not a theoretical gap.
    pub fn following_renames(&self, boundaries: &BoundaryTable) -> Self {
        if boundaries.is_empty() {
            return self.clone();
        }

        let mut by_ticker: HashMap<String, Vec<(SessionDate, f64)>> = HashMap::new();
        for (ticker, splits) in &self.by_ticker {
            for (execution_date, ratio) in splits {
                let symbol = boundaries
                    .current_symbol(ticker, *execution_date)
                    .unwrap_or_else(|| ticker.clone());
                by_ticker
                    .entry(symbol)
                    .or_default()
                    .push((*execution_date, *ratio));
            }
        }

        Self { by_ticker }
    }

    /// Whether any split is known, which is what makes skipping the whole fold safe.
    pub fn is_empty(&self) -> bool {
        self.by_ticker.is_empty()
    }
}

/// The splits table, reloaded when the cached copy is from an earlier Eastern date.
///
/// A daily table behind a daily cache: the feed publishes an execution date, so a split cannot
/// start applying part-way through a session.
#[derive(Default)]
pub struct SplitTableCache {
    inner: tokio::sync::Mutex<Option<(SessionDate, Arc<SplitTable>)>>,
}

impl SplitTableCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns today's table, or `None` when the archive holds none.
    ///
    /// Absent is reported rather than flattened to an empty table, because the two mean opposite
    /// things: empty says no split affects these prices, missing says nothing is known about whether
    /// one does. An absent object is also not cached, so the next pass retries it.
    ///
    /// The lock is released before the S3 read and re-taken to store, like
    /// [`crate::data::bars::CloseHistoryCache`], so two cold callers may both read; both reads are
    /// deterministic, and the second store is a harmless overwrite.
    pub async fn get(
        &self,
        s3_client: &aws_sdk_s3::Client,
        bucket: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<Arc<SplitTable>>, ArchiveError> {
        let today = SessionDate::at(now);

        if let Some((cached_date, table)) = self.inner.lock().await.as_ref() {
            if *cached_date == today {
                return Ok(Some(Arc::clone(table)));
            }
        }

        let Some(frame) = read_partition(s3_client, bucket, SPLITS_ARCHIVE_KEY).await? else {
            warn!(
                key = SPLITS_ARCHIVE_KEY,
                "No splits table in the archive; prices cannot be adjusted"
            );
            return Ok(None);
        };

        let table = Arc::new(SplitTable::from_dataframe(&frame)?);
        *self.inner.lock().await = Some((today, Arc::clone(&table)));
        Ok(Some(table))
    }
}

/// Columns holding a price, which scale with the factor.
const PRICE_COLUMNS: [&str; 5] = [
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume_weighted_average_price",
];

/// Restates raw bars onto `as_of`'s share basis.
///
/// Prices scale by the factor and volume by its inverse, because a two-for-one split halves the
/// price and doubles the share count. Returns the frame untouched when no split is known, which is
/// the ordinary case for most of the market.
pub fn adjust_bars(
    frame: DataFrame,
    table: &SplitTable,
    as_of: SessionDate,
) -> Result<DataFrame, PolarsError> {
    if table.is_empty() {
        return Ok(frame);
    }

    // The factor depends on a per-ticker date range rather than a key, so it is not a join. Computed
    // once here and reused across the columns instead of rescanning the table for each of them.
    let factors = bar_factors(&frame, table, as_of)?;

    let mut adjusted = frame;
    for name in PRICE_COLUMNS {
        let Ok(column) = adjusted.column(name) else {
            continue;
        };
        let scaled: Float64Chunked = column
            .f64()?
            .into_iter()
            .zip(&factors)
            .map(|(price, factor)| price.map(|price| price * factor))
            .collect();
        adjusted.with_column(scaled.into_series().with_name(name.into()))?;
    }

    if let Ok(column) = adjusted.column("volume") {
        // Against the factor, not with it: a two-for-one halves the price and doubles the shares.
        let scaled: Int64Chunked = column
            .i64()?
            .into_iter()
            .zip(&factors)
            .map(|(volume, factor)| volume.map(|volume| (volume as f64 / factor).round() as i64))
            .collect();
        adjusted.with_column(scaled.into_series().with_name("volume".into()))?;
    }

    Ok(adjusted)
}

/// One factor per row, in frame order.
fn bar_factors(
    frame: &DataFrame,
    table: &SplitTable,
    as_of: SessionDate,
) -> Result<Vec<f64>, PolarsError> {
    let tickers = frame.column("ticker")?.str()?;
    let timestamps = frame.column("timestamp")?.i64()?;

    Ok((0..frame.height())
        .map(|row| {
            let (Some(ticker), Some(timestamp)) = (tickers.get(row), timestamps.get(row)) else {
                return 1.0;
            };
            let Some(instant) = chrono::DateTime::from_timestamp_millis(timestamp) else {
                return 1.0;
            };
            table.factor_at(ticker, SessionDate::at(instant), as_of)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{EquitySplit, Ticker};
    use crate::data::splits::splits_to_dataframe;

    fn session(value: &str) -> SessionDate {
        SessionDate::from_date(value.parse().expect("a valid session date"))
    }

    fn split(ticker: &str, execution_date: &str, from: f64, to: f64) -> EquitySplit {
        EquitySplit::new(
            format!("{ticker}-{execution_date}"),
            Ticker::new(ticker).expect("a valid ticker"),
            session(execution_date),
            from,
            to,
        )
        .expect("a valid split")
    }

    fn table(splits: &[EquitySplit]) -> SplitTable {
        let frame = splits_to_dataframe(splits, "2026-08-14T02:00:00Z".parse().unwrap())
            .expect("a frame must build");
        SplitTable::from_dataframe(&frame).expect("the table must index")
    }

    /// The live case that started this, with the numbers taken from the feed: `MNST` closed a raw
    /// 96.38 on 2026-06-26 and split two-for-one on 2026-08-11, and Massive's own `adjusted=true`
    /// answers 48.19 for that session.
    #[test]
    fn test_a_bar_before_a_split_is_restated_onto_the_current_basis() {
        let table = table(&[split("MNST", "2026-08-11", 1.0, 2.0)]);

        let factor = table.factor_at("MNST", session("2026-06-26"), session("2026-08-14"));

        assert!((96.38 * factor - 48.19).abs() < 1e-9, "factor was {factor}");
    }

    /// `NVDA`'s ten-for-one, the largest recent factor, checked against the same feed. A ratio this
    /// far from one is where an inverted factor stops being subtle.
    #[test]
    fn test_a_ten_for_one_scales_by_a_tenth_and_not_by_ten() {
        let table = table(&[split("NVDA", "2024-06-10", 1.0, 10.0)]);

        let factor = table.factor_at("NVDA", session("2024-06-07"), session("2024-08-14"));

        assert!((factor - 0.1).abs() < 1e-12, "factor was {factor}");
    }

    /// A split executes at the open, so the bar stamped that day already reflects it and applying
    /// the ratio again would halve a price that was never doubled.
    #[test]
    fn test_the_bar_on_the_execution_date_is_already_post_split() {
        let table = table(&[split("MNST", "2026-08-11", 1.0, 2.0)]);

        assert_eq!(
            table.factor_at("MNST", session("2026-08-11"), session("2026-08-14")),
            1.0
        );
        assert_eq!(
            table.factor_at("MNST", session("2026-08-12"), session("2026-08-14")),
            1.0,
            "a bar after the split needs no restating either"
        );
    }

    /// The trap the upper bound exists for. The feed carries announced splits months ahead, and
    /// applying one would restate today's history onto a basis the market has not moved to — leaving
    /// it incomparable with the live quote the screen measures it against.
    #[test]
    fn test_an_announced_split_does_not_restate_todays_history() {
        let table = table(&[split("DPU", "2026-12-17", 50.0, 1.0)]);

        assert_eq!(
            table.factor_at("DPU", session("2026-08-01"), session("2026-08-13")),
            1.0,
            "a split that has not executed yet must not touch anything"
        );
        assert_eq!(
            table.factor_at("DPU", session("2026-08-01"), session("2026-12-18")),
            50.0,
            "and must apply once it has"
        );
    }

    /// Two splits between the bar and today compound rather than replacing one another.
    #[test]
    fn test_successive_splits_compound() {
        let table = table(&[
            split("AAAA", "2026-03-02", 1.0, 2.0),
            split("AAAA", "2026-06-01", 1.0, 3.0),
        ]);

        let factor = table.factor_at("AAAA", session("2026-01-05"), session("2026-08-13"));

        assert!((factor - 1.0 / 6.0).abs() < 1e-12, "factor was {factor}");
    }

    #[test]
    fn test_a_reverse_split_raises_the_earlier_price() {
        let table = table(&[split("TGOSY", "2026-10-05", 5.0, 1.0)]);

        let factor = table.factor_at("TGOSY", session("2026-09-01"), session("2026-10-06"));

        assert_eq!(factor, 5.0);
    }

    #[test]
    fn test_a_ticker_with_no_splits_is_left_alone() {
        let table = table(&[split("MNST", "2026-08-11", 1.0, 2.0)]);

        assert_eq!(
            table.factor_at("AAPL", session("2026-06-26"), session("2026-08-14")),
            1.0
        );
    }

    fn bars(ticker: &str, session_date: &str, close: f64, volume: i64) -> DataFrame {
        df![
            "ticker" => [ticker],
            "bar_interval" => ["1Day"],
            "timestamp" => [session(session_date).midnight().timestamp_millis()],
            "open_price" => [close],
            "high_price" => [close],
            "low_price" => [close],
            "close_price" => [close],
            "volume" => [volume],
            "volume_weighted_average_price" => [Some(close)],
            "transactions" => [Some(1_000i64)],
        ]
        .expect("a frame must build")
    }

    /// Prices scale with the factor and volume against it: a two-for-one halves the price and
    /// doubles the shares, and a frame that moved only one of them is internally inconsistent.
    #[test]
    fn test_the_fold_scales_prices_down_and_volume_up() {
        let table = table(&[split("MNST", "2026-08-11", 1.0, 2.0)]);

        let adjusted = adjust_bars(
            bars("MNST", "2026-06-26", 96.38, 1_000_000),
            &table,
            session("2026-08-14"),
        )
        .expect("the fold must apply");

        let close = adjusted.column("close_price").unwrap().f64().unwrap();
        assert!((close.get(0).unwrap() - 48.19).abs() < 1e-9);
        assert_eq!(
            adjusted.column("volume").unwrap().i64().unwrap().get(0),
            Some(2_000_000)
        );
        assert!(
            (adjusted
                .column("volume_weighted_average_price")
                .unwrap()
                .f64()
                .unwrap()
                .get(0)
                .unwrap()
                - 48.19)
                .abs()
                < 1e-9,
            "the volume-weighted price is a price too"
        );
    }

    /// The output has to stay loadable by everything that reads a bar frame, so the fold must not
    /// leave its working column behind or drop one it did not touch.
    #[test]
    fn test_the_fold_preserves_the_frame_schema() {
        let raw = bars("MNST", "2026-06-26", 96.38, 1_000_000);
        let expected: Vec<String> = raw
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();

        let adjusted = adjust_bars(
            raw,
            &table(&[split("MNST", "2026-07-06", 1.0, 2.0)]),
            session("2026-08-13"),
        )
        .expect("the fold must apply");

        let actual: Vec<String> = adjusted
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_an_empty_table_returns_the_frame_untouched() {
        let raw = bars("MNST", "2026-06-26", 96.38, 1_000_000);

        let adjusted = adjust_bars(raw.clone(), &SplitTable::default(), session("2026-08-14"))
            .expect("the fold must apply");

        assert!(adjusted.equals(&raw));
    }

    /// A stored object this build did not write can carry anything, and every one of these reaches
    /// a divisor: a malformed date lands the split in the wrong window, and a factor of zero or
    /// infinity saturates volume to `i64::MAX` and sends every price to infinity.
    ///
    /// `DDDD` and `EEEE` are why the sides and the quotient are both checked. Two positives divide
    /// to a factor that is neither, and two negatives divide to one that looks entirely ordinary —
    /// so neither guard catches what the other does.
    #[test]
    fn test_unreadable_rows_are_skipped_rather_than_indexed() {
        let frame = df![
            "id" => ["E1", "E2", "E3", "E4", "E5", "E6"],
            "ticker" => ["AAAA", "BBBB", "CCCC", "DDDD", "EEEE", "FFFF"],
            "execution_date" => [
                "2026-07-06", "not a date", "2026-07-06", "2026-07-06", "2026-07-06", "2026-07-06",
            ],
            "split_from" => [1.0, 1.0, 0.0, 1e308, -1.0, 1e-308],
            "split_to" => [2.0, 2.0, 2.0, 1e-308, -2.0, 1e308],
            "first_seen" => [0i64, 0, 0, 0, 0, 0],
        ]
        .unwrap();

        let table = SplitTable::from_dataframe(&frame).expect("the table must index");

        assert_eq!(
            table.factor_at("AAAA", session("2026-07-02"), session("2026-08-13")),
            0.5,
            "an ordinary ratio still indexes"
        );
        for skipped in ["BBBB", "CCCC", "DDDD", "EEEE", "FFFF"] {
            assert_eq!(
                table.factor_at(skipped, session("2026-07-02"), session("2026-08-13")),
                1.0,
                "{skipped}"
            );
        }
    }

    /// The contract `adjust_bars` divides by. Each stored ratio is finite and positive, but their
    /// product need not be, and a caller that had to re-check would be re-checking everywhere.
    #[test]
    fn test_a_factor_that_compounds_out_of_range_falls_back_to_one() {
        let overflowing = table(&[
            split("AAAA", "2026-07-06", 1e200, 1.0),
            split("AAAA", "2026-07-07", 1e200, 1.0),
        ]);

        assert_eq!(
            overflowing.factor_at("AAAA", session("2026-07-02"), session("2026-08-13")),
            1.0,
            "a product past f64 is not a factor to scale prices by"
        );
    }
}
