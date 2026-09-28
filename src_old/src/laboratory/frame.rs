//! The daily frame every study reads: archive bars, deduplicated, screened, with one-session returns.
//!
//! Built here rather than beside a model, because a dataset builder must not depend on one.

use std::collections::HashMap;

use polars::prelude::*;
use tracing::warn;

use crate::common::types::{Screen, SessionDate};
use crate::data::reference::UNKNOWN;
use crate::data::universe::filter_liquid_bars;

/// What went wrong shaping bars into a study frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("dataframe operation failed: {0}")]
    Frame(#[from] PolarsError),
    /// Rows that cannot be turned into a frame a study can read: the market data, not the code.
    #[error("{0}")]
    Data(String),
}

/// The columns a row must hold a finite value in to be measured at all.
pub const CONTINUOUS_COLUMNS: &[&str] = &[
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "volume_weighted_average_price",
    "daily_return",
];

/// The one-session return, which is the quantity every study's target is built from.
pub const TARGET_COLUMN: &str = "daily_return";

/// The screen, plus a drop of tickers containing lowercase letters, which are distinct instruments
/// that would collide with the uppercase ticker after cleaning.
///
/// Liquidity is delegated to [`filter_liquid_bars`], so a study measures the population the universe
/// trades: the screen judges a name whole rather than admitting its good days.
pub fn filter_study_bars(
    data: DataFrame,
    screen: Screen,
    as_of: SessionDate,
) -> Result<DataFrame, FrameError> {
    let tickers = data.column("ticker")?.str()?;
    let mask: BooleanChunked = tickers
        .into_iter()
        .map(|ticker| {
            ticker.is_some_and(|value| !value.chars().any(|character| character.is_lowercase()))
        })
        .collect();

    Ok(filter_liquid_bars(data.filter(&mask)?, screen, as_of)?)
}

/// Collapses duplicate bars and drops the non-positive ones, before any classification is joined.
pub fn prepare_bars(equity_bars: DataFrame) -> Result<DataFrame, FrameError> {
    Ok(resolve_duplicate_bars(equity_bars)?
        .lazy()
        .filter(
            col("open_price")
                .gt(lit(0.0))
                .and(col("high_price").gt(lit(0.0)))
                .and(col("low_price").gt(lit(0.0)))
                .and(col("close_price").gt(lit(0.0))),
        )
        .collect()?)
}

/// Collapses repeated `(ticker, timestamp)` bars, keeping the highest-volume row of each pair.
///
/// A provider that serves two instruments under one symbol yields two bars for one session, and
/// volume separates them where price cannot: the two TPC series opened fifty cents apart while their
/// volumes differed five-fold. Every collapse is logged, because a silent tie-break leaves nothing
/// recording that a choice between two instruments was made at all.
fn resolve_duplicate_bars(bars: DataFrame) -> Result<DataFrame, FrameError> {
    let before = bars.height();

    // Ascending volume puts the row worth keeping last, which is the one `Last` then takes. Sorting
    // the frame is free downstream: `add_daily_returns` re-sorts by (ticker, timestamp) anyway.
    let deduplicated = bars
        .clone()
        .lazy()
        .sort(
            ["volume"],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .unique_stable(
            Some(polars::prelude::Selector::ByName {
                names: vec![PlSmallStr::from("ticker"), PlSmallStr::from("timestamp")].into(),
                strict: false,
            }),
            UniqueKeepStrategy::Last,
        )
        .collect()?;

    let collapsed = before.saturating_sub(deduplicated.height());
    if collapsed > 0 {
        // Only walked when a collision actually happened, which on clean data is never.
        let affected = duplicated_tickers(&bars)?;
        warn!(
            collapsed,
            tickers = ?affected,
            "Duplicate bars for one session; kept the highest-volume row of each"
        );
    }

    Ok(deduplicated)
}

/// The tickers carrying more than one bar for the same session, sorted, for the log line above.
///
/// Sorted because `UniqueKeepStrategy::Any` promises no ordering, and a log line that names the same
/// two symbols in a different order each session cannot be diffed or alerted on.
fn duplicated_tickers(bars: &DataFrame) -> Result<Vec<String>, FrameError> {
    let frame = bars
        .clone()
        .lazy()
        .group_by([col("ticker"), col("timestamp")])
        .agg([len().alias("bar_count")])
        .filter(col("bar_count").gt(lit(1u32)))
        .select([col("ticker")])
        .unique(None, UniqueKeepStrategy::Any)
        .collect()?;

    // Propagated rather than defaulted to empty: the caller logs this list as the record that a
    // tie-break happened, so swallowing the error here would report a collapse naming no ticker,
    // which is the silence this function exists to break.
    let tickers = frame.column("ticker")?.str()?;
    let mut names: Vec<String> = tickers.into_iter().flatten().map(str::to_string).collect();
    names.sort();
    Ok(names)
}

/// Maps every session the frame holds to its position in the sorted set of them.
///
/// Adjacency is therefore relative to the sessions this frame contains, not to a trading calendar:
/// a session missing for some tickers is a gap, and one missing across the whole universe is
/// invisible here and belongs to whatever checks the archive's completeness.
pub fn session_ranks(data: &DataFrame) -> Result<HashMap<i64, usize>, FrameError> {
    let timestamps = data.column("timestamp")?.i64()?;
    // Skipping these would leave fewer sessions than rows, and the windows are counted from the
    // row height.
    if timestamps.null_count() > 0 {
        return Err(FrameError::Data(format!(
            "Equity bars contain {} null timestamp values out of {} rows",
            timestamps.null_count(),
            data.height()
        )));
    }
    let mut sessions: Vec<i64> = timestamps
        .into_no_null_iter()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    sessions.sort_unstable();
    Ok(sessions
        .into_iter()
        .enumerate()
        .map(|(rank, timestamp)| (timestamp, rank))
        .collect())
}

/// Adds `daily_return`, the one-session return each study measures, to a frame of daily bars.
///
/// A return across a gap in a name's own history is left null rather than labelled one session.
pub fn add_daily_returns(data: DataFrame) -> Result<DataFrame, FrameError> {
    // Sort by [ticker, timestamp] so the return below is chronological whatever order rows arrived
    // in. Chronological is not contiguous, which is why it is measured against `session_ranks`.
    let data = data.sort(
        ["ticker", "timestamp"],
        SortMultipleOptions::default().with_maintain_order(true),
    )?;
    let ranks = session_ranks(&data)?;

    let timestamps = data.column("timestamp")?;
    let height = data.height();

    let mut daily_return: Vec<Option<f32>> = Vec::with_capacity(height);

    let close_prices: Vec<f64> = data
        .column("close_price")?
        .f64()?
        .into_no_null_iter()
        .collect();

    let tickers: Vec<String> = data
        .column("ticker")?
        .str()?
        .into_no_null_iter()
        .map(|name| name.to_string())
        .collect();

    let timestamp_values: Vec<i64> = timestamps.i64()?.into_no_null_iter().collect();

    // The no-null iterators above silently skip nulls, which would misalign
    // the per-row zip below; fail fast with a clear message instead.
    if close_prices.len() != height || tickers.len() != height || timestamp_values.len() != height {
        let message = format!(
            "Equity bars contain null ticker, timestamp, or close_price values \
             ({} rows; {} tickers, {} timestamps, {} close prices)",
            height,
            tickers.len(),
            timestamp_values.len(),
            close_prices.len()
        );
        return Err(FrameError::Data(message));
    }

    for (index, &timestamp_milliseconds) in timestamp_values.iter().enumerate() {
        // A *daily* return or nothing. Measured across a gap it is a multi-session return wearing
        // a one-session label.
        let follows_previous_session = index > 0
            && tickers[index] == tickers[index - 1]
            && ranks
                .get(&timestamp_milliseconds)
                .zip(ranks.get(&timestamp_values[index - 1]))
                .is_some_and(|(current, previous)| *current == previous + 1);
        if follows_previous_session && close_prices[index - 1] != 0.0 {
            daily_return.push(Some(
                ((close_prices[index] / close_prices[index - 1]) - 1.0) as f32,
            ));
        } else {
            daily_return.push(None);
        }
    }

    let mut new_data = data.clone();
    new_data.with_column(Column::new(TARGET_COLUMN.into(), daily_return))?;

    Ok(new_data)
}

/// Uppercases the names and their classification, and drops any row a study could not measure.
///
/// A row goes when any [`CONTINUOUS_COLUMNS`] value is null or non-finite: each name's first
/// session, a missing vendor VWAP, a division artifact. A null share count does not cost the row.
pub fn clean_frame(mut data: DataFrame) -> Result<DataFrame, FrameError> {
    // A row with no ticker cannot be attributed to an instrument, and substituting a placeholder
    // would make it indistinguishable from a real symbol of the same spelling.
    let tickers = data.column("ticker")?.str()?;
    if tickers.null_count() > 0 {
        return Err(FrameError::Data(format!(
            "Equity bars contain {} null ticker values out of {} rows",
            tickers.null_count(),
            data.height()
        )));
    }

    // Uppercase ticker, sector, industry columns in-place
    let ticker_upper: Vec<String> = tickers
        .into_no_null_iter()
        .map(|value| value.to_uppercase())
        .collect();

    let sector_upper: Vec<String> = data
        .column("sector")?
        .str()?
        .into_iter()
        .map(|value| value.unwrap_or(UNKNOWN).to_uppercase())
        .collect();

    let industry_upper: Vec<String> = data
        .column("industry")?
        .str()?
        .into_iter()
        .map(|value| value.unwrap_or(UNKNOWN).to_uppercase())
        .collect();

    data.with_column(Column::new("ticker".into(), ticker_upper))?;
    data.with_column(Column::new("sector".into(), sector_upper))?;
    data.with_column(Column::new("industry".into(), industry_upper))?;

    let cleaned = data;

    let mut keep_row = vec![true; cleaned.height()];
    for column in CONTINUOUS_COLUMNS {
        let values = cleaned.column(column)?.cast(&DataType::Float64)?;
        let values = values.f64()?;
        for (index, value) in values.into_iter().enumerate() {
            if !value.is_some_and(f64::is_finite) {
                keep_row[index] = false;
            }
        }
    }
    let finite_mask: BooleanChunked = keep_row.into_iter().collect();
    let cleaned = cleaned.filter(&finite_mask)?;
    Ok(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The instant a daily bar carries: the 16:00 Eastern close, where ingestion stamps it.
    fn session_close(session: SessionDate) -> i64 {
        use chrono::TimeZone;
        chrono_tz::America::New_York
            .from_local_datetime(&session.date().and_hms_opt(16, 0, 0).unwrap())
            .earliest()
            .unwrap()
            .with_timezone(&chrono::Utc)
            .timestamp_millis()
    }

    fn raw_gapped_frame() -> DataFrame {
        const DAY: i64 = 86_400_000;
        let gapped_days: Vec<i64> = vec![0, 1, 2, 3, 8, 9, 10];
        let dense_days: Vec<i64> = (0..=10).collect();

        let mut tickers: Vec<&str> = vec!["GAPPY"; gapped_days.len()];
        tickers.extend(vec!["DENSE"; dense_days.len()]);

        let mut timestamps: Vec<i64> = gapped_days.iter().map(|day| day * DAY).collect();
        timestamps.extend(dense_days.iter().map(|day| day * DAY));

        let closes: Vec<f64> = (0..timestamps.len())
            .map(|row| 100.0 + row as f64)
            .collect();
        let height = timestamps.len();

        DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("timestamp".into(), timestamps),
            Column::new("open_price".into(), vec![1.0_f64; height]),
            Column::new("high_price".into(), vec![1.0_f64; height]),
            Column::new("low_price".into(), vec![1.0_f64; height]),
            Column::new("close_price".into(), closes),
            Column::new("volume".into(), vec![1.0_f64; height]),
            Column::new(
                "volume_weighted_average_price".into(),
                vec![1.0_f64; height],
            ),
            Column::new("sector".into(), vec!["S"; height]),
            Column::new("industry".into(), vec!["I"; height]),
        ])
        .unwrap()
    }

    /// Two names over two consecutive sessions, stamped the way ingestion stamps a daily bar.
    fn raw_two_ticker_frame() -> DataFrame {
        let first = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap());
        let second = first.plus_calendar_days(1);
        DataFrame::new(vec![
            Column::new("ticker".into(), vec!["BBB", "AAA", "BBB", "AAA"]),
            Column::new(
                "timestamp".into(),
                vec![
                    session_close(first),
                    session_close(first),
                    session_close(second),
                    session_close(second),
                ],
            ),
            Column::new("open_price".into(), vec![1.0_f64; 4]),
            Column::new("high_price".into(), vec![1.0_f64; 4]),
            Column::new("low_price".into(), vec![1.0_f64; 4]),
            Column::new("close_price".into(), vec![10.0_f64, 20.0, 11.0, 22.0]),
            Column::new("volume".into(), vec![1.0_f64; 4]),
            Column::new("volume_weighted_average_price".into(), vec![1.0_f64; 4]),
            Column::new("sector".into(), vec!["S", "S", "S", "S"]),
            Column::new("industry".into(), vec!["I", "I", "I", "I"]),
        ])
        .unwrap()
    }

    fn returns(frame: &DataFrame) -> Vec<Option<f32>> {
        frame
            .column(TARGET_COLUMN)
            .unwrap()
            .f32()
            .unwrap()
            .into_iter()
            .collect()
    }

    #[test]
    fn test_a_null_timestamp_is_refused_rather_than_skipped() {
        let mut frame = raw_gapped_frame();
        let mut timestamps: Vec<Option<i64>> = frame
            .column("timestamp")
            .unwrap()
            .i64()
            .unwrap()
            .into_iter()
            .collect();
        let last = timestamps.len() - 1;
        timestamps[last] = None;
        frame
            .with_column(Column::new("timestamp".into(), timestamps))
            .unwrap();

        assert!(
            matches!(session_ranks(&frame), Err(FrameError::Data(_))),
            "a null timestamp must be an error, not a silently shorter session list"
        );
    }

    /// A return measured across a gap is a multi-session return wearing a one-session label.
    #[test]
    fn test_a_return_measured_across_a_session_gap_is_null() {
        let engineered = add_daily_returns(raw_gapped_frame()).unwrap();
        let tickers: Vec<String> = engineered
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(str::to_string)
            .collect();
        let returns = returns(&engineered);

        // Sorted by [ticker, timestamp]: DENSE days 0..10 occupy rows 0..10, then GAPPY days
        // 0, 1, 2, 3, 8, 9, 10 occupy rows 11..17. GAPPY's day 8 is row 15.
        assert_eq!(tickers[15], "GAPPY");
        assert_eq!(
            returns[15], None,
            "GAPPY's day 8 follows its day 3, so its return spans five sessions"
        );
        assert!(returns[14].is_some(), "GAPPY day 3 follows day 2");
        assert!(returns[16].is_some(), "GAPPY day 9 follows day 8");
    }

    #[test]
    fn test_each_names_first_row_has_no_return() {
        let engineered = add_daily_returns(raw_two_ticker_frame()).unwrap();
        // Sorted by [ticker, timestamp]: AAA@0, AAA@1, BBB@0, BBB@1.
        let returns = returns(&engineered);
        assert_eq!(returns[0], None);
        assert!((returns[1].unwrap() - 0.1).abs() < 1e-6); // 22/20 - 1
        assert_eq!(returns[2], None);
        assert!((returns[3].unwrap() - 0.1).abs() < 1e-6); // 11/10 - 1
    }

    #[test]
    fn test_only_the_return_is_added() {
        let raw = raw_two_ticker_frame();
        let mut expected: Vec<String> = raw
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        expected.push(TARGET_COLUMN.to_string());
        let engineered = add_daily_returns(raw).unwrap();
        let columns: Vec<String> = engineered
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        assert_eq!(columns, expected);
    }

    #[test]
    fn test_cleaning_drops_the_rows_with_no_return() {
        let cleaned = clean_frame(add_daily_returns(raw_two_ticker_frame()).unwrap()).unwrap();
        assert_eq!(cleaned.height(), 2);
        assert!(returns(&cleaned)
            .iter()
            .all(|daily_return| (daily_return.unwrap() - 0.1).abs() < 1e-6));
    }

    #[test]
    fn test_a_null_classification_is_stored_as_the_unknown_spelling() {
        let mut engineered = add_daily_returns(raw_two_ticker_frame()).unwrap();
        for column in ["sector", "industry"] {
            engineered
                .with_column(Column::new(column.into(), vec![None::<&str>; 4]))
                .unwrap();
        }

        let cleaned = clean_frame(engineered).unwrap();
        for column in ["sector", "industry"] {
            let values: Vec<&str> = cleaned
                .column(column)
                .unwrap()
                .str()
                .unwrap()
                .into_no_null_iter()
                .collect();
            assert!(!values.is_empty());
            assert!(
                values.iter().all(|value| *value == "NOT AVAILABLE"),
                "{column} fell back to {values:?}"
            );
        }
    }

    #[test]
    fn test_a_null_ticker_is_refused() {
        let mut engineered = add_daily_returns(raw_two_ticker_frame()).unwrap();
        engineered
            .with_column(Column::new(
                "ticker".into(),
                vec![Some("AAA"), None::<&str>, Some("BBB"), Some("BBB")],
            ))
            .unwrap();
        let error = clean_frame(engineered).unwrap_err().to_string();
        assert!(error.contains("null ticker"), "got: {error}");
    }

    #[test]
    fn test_a_null_continuous_value_drops_its_row() {
        let mut engineered = add_daily_returns(raw_two_ticker_frame()).unwrap();
        engineered
            .with_column(Column::new(
                "volume_weighted_average_price".into(),
                vec![Some(1.0_f64), None, Some(1.0), Some(1.0)],
            ))
            .unwrap();
        // Two first rows drop for their null returns, one more for the null price.
        assert_eq!(clean_frame(engineered).unwrap().height(), 1);
    }

    /// `shares_outstanding` is deliberately absent from [`CONTINUOUS_COLUMNS`], which is what keeps
    /// the names the feed stops reporting shares for inside the frame at all.
    #[test]
    fn test_a_null_share_count_does_not_drop_the_row() {
        let mut engineered = add_daily_returns(raw_two_ticker_frame()).unwrap();
        let baseline = clean_frame(engineered.clone()).unwrap().height();
        engineered
            .with_column(Column::new(
                "shares_outstanding".into(),
                vec![None::<f64>; 4],
            ))
            .unwrap();
        assert!(baseline > 0);
        assert_eq!(clean_frame(engineered).unwrap().height(), baseline);
    }

    fn bars(rows: &[(&str, i64, f64, f64)]) -> DataFrame {
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "open_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "high_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "low_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "close_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "volume".into(),
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            ),
        ])
        .unwrap()
    }

    #[test]
    fn test_a_duplicate_bar_keeps_the_higher_volume_row() {
        let prepared = prepare_bars(bars(&[
            ("TPC", 0, 10.0, 1_000.0),
            ("TPC", 0, 10.5, 5_000.0),
            ("AAA", 0, 20.0, 100.0),
        ]))
        .unwrap()
        .sort(["ticker"], SortMultipleOptions::default())
        .unwrap();
        let closes: Vec<f64> = prepared
            .column("close_price")
            .unwrap()
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(closes, vec![20.0, 10.5]);
    }

    #[test]
    fn test_a_non_positive_price_drops_its_bar() {
        let prepared =
            prepare_bars(bars(&[("ZERO", 0, 0.0, 100.0), ("KEEP", 0, 1.0, 100.0)])).unwrap();
        assert_eq!(prepared.height(), 1);
    }

    #[test]
    fn test_only_the_colliding_symbols_are_named() {
        let frame = bars(&[
            ("BBB", 0, 1.0, 1.0),
            ("BBB", 0, 1.0, 2.0),
            ("AAA", 0, 1.0, 1.0),
            ("AAA", 0, 1.0, 2.0),
            ("CCC", 0, 1.0, 1.0),
        ]);
        assert_eq!(duplicated_tickers(&frame).unwrap(), vec!["AAA", "BBB"]);
    }

    /// A whole-frame screen never reads the anchor.
    fn unread_anchor() -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 30).unwrap())
    }

    fn floor(minimum_close_price: f64, minimum_dollar_volume: f64) -> Screen {
        Screen::new(
            crate::common::types::LiquidityFloor::new(minimum_close_price, minimum_dollar_volume)
                .expect("test floor must be valid"),
            crate::common::types::ScreenWindow::WholeFrame,
        )
    }

    fn tickers_of(frame: &DataFrame) -> Vec<String> {
        frame
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn test_the_screen_judges_a_ticker_whole() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["DIPS", "DIPS", "KEEP", "KEEP"]),
            Column::new("timestamp".into(), vec![0_i64, 86_400_000, 0, 86_400_000]),
            Column::new("close_price".into(), vec![0.5_f64, 250.0, 1.0, 250.0]),
            Column::new("volume".into(), vec![100_000.0_f64; 4]),
        ])
        .unwrap();
        let filtered = filter_study_bars(data, floor(1.0, 100_000.0), unread_anchor()).unwrap();
        // DIPS dipped to 0.50 once, so both its rows go; KEEP sits exactly on the bound.
        assert_eq!(tickers_of(&filtered), vec!["KEEP", "KEEP"]);
    }

    #[test]
    fn test_the_screen_is_on_dollars_not_shares() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["HIGH", "LOW"]),
            Column::new("timestamp".into(), vec![0_i64, 0]),
            Column::new("close_price".into(), vec![250.0_f64, 12.0]),
            Column::new("volume".into(), vec![200.0_f64, 4_000.0]),
        ])
        .unwrap();
        let filtered = filter_study_bars(data, floor(1.0, 48_001.0), unread_anchor()).unwrap();
        assert_eq!(tickers_of(&filtered), vec!["HIGH"]);
    }

    #[test]
    fn test_a_lowercase_ticker_is_dropped() {
        let data = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL", "AAPLw", "brk.a"]),
            Column::new("timestamp".into(), vec![0_i64, 0, 0]),
            Column::new("close_price".into(), vec![150.0_f64; 3]),
            Column::new("volume".into(), vec![1_000_000.0_f64; 3]),
        ])
        .unwrap();
        let filtered = filter_study_bars(data, floor(1.0, 100_000.0), unread_anchor()).unwrap();
        assert_eq!(tickers_of(&filtered), vec!["AAPL"]);
    }
}
