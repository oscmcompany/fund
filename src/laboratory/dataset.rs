//! The frame every laboratory experiment reads, and the identity that makes two runs comparable.
//!
//! Lifted out of the trainer binary because baselines need this frame and no model.

use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::Serialize;
use tracing::warn;

use crate::common::aws::date_partitioned_key;
use crate::common::types::{BarInterval, SessionDate, MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME};
use crate::data::{adjust, archive, bars, details, truncate};
use crate::models::tide::data::{clean_data, engineer_features, Target, TrainingFraction};
use crate::models::tide::fit::{filter_training_bars, fit, FitResult};
use crate::models::tide::predict::consolidate_data;
use crate::models::tide::TideError;

/// Errors building a dataset.
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("failed to read the archive: {0}")]
    Archive(#[from] archive::ArchiveError),
    #[error("dataframe operation failed: {0}")]
    Frame(#[from] PolarsError),
    #[error("preprocessing failed: {0}")]
    Tide(#[from] TideError),
    #[error("failed to read the embedded ticker details: {0}")]
    Details(#[from] crate::data::details::DetailsError),
    #[error("failed to consolidate bars with details: {0}")]
    Consolidation(#[from] crate::models::tide::predict::PredictionError),
    /// A window that cannot produce a dataset, named rather than returned empty.
    #[error("{0}")]
    Window(String),
}

/// What a dataset was built from, recorded so two runs can be told apart.
///
/// Counts and spans catch a different window; the two digests catch the case they cannot, where the
/// archive holds the same raw bars and a table revised in place restates them at read time.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatasetFingerprint {
    pub session: SessionDate,
    pub lookback_days: i64,
    pub rows: usize,
    pub tickers: usize,
    pub first_timestamp: Option<DateTime<Utc>>,
    pub last_timestamp: Option<DateTime<Utc>>,
    /// Content of the splits table the prices were folded against.
    pub splits_digest: u64,
    /// Content of the boundary table the series were stitched and bounded against.
    pub boundaries_digest: u64,
}

/// Folds a frame's contents into one value that changes when any cell does.
///
/// FNV-1a over the sorted rows rather than Polars' own row hash, which is seeded randomly per call
/// and so cannot answer whether two runs read the same table. Sorting makes the row order irrelevant.
fn digest_of(frame: &DataFrame) -> Result<u64, DatasetError> {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let columns: Vec<String> = frame
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let sorted = frame.sort(&columns, SortMultipleOptions::default())?;

    let mut digest = OFFSET_BASIS;
    let mut fold = |bytes: &[u8]| {
        for byte in bytes {
            digest = (digest ^ u64::from(*byte)).wrapping_mul(PRIME);
        }
    };
    // Names as well as values: a column renamed in place changes what the table means.
    for name in &columns {
        fold(name.as_bytes());
        fold(b"\x1f");
    }
    let mut rendered = String::new();
    for index in 0..sorted.height() {
        rendered.clear();
        for value in &sorted.get_row(index)?.0 {
            use std::fmt::Write;
            let _ = write!(rendered, "{value}\u{1f}");
        }
        fold(rendered.as_bytes());
    }
    Ok(digest)
}

/// A prepared dataset and the identity of what it was prepared from.
pub struct PreparedDataset {
    pub fit: FitResult,
    pub fingerprint: DatasetFingerprint,
}

/// One session's returns per name, and the identity of the window they came from.
pub struct ReturnsDataset {
    /// Every engineered feature alongside `ticker` and `timestamp`, cleaned and unscaled.
    pub returns: DataFrame,
    pub fingerprint: DatasetFingerprint,
}

/// Reads the archive for `lookback_days` back from `session` and prepares it for windowing.
///
/// The splits table is fatal where the boundary table is not: stored prices are raw, so training
/// without splits fits a two-for-one as a genuine fifty percent fall, while an absent boundary
/// table costs a guard on a handful of names.
pub async fn build(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
    training_fraction: TrainingFraction,
    target: Target,
) -> Result<PreparedDataset, DatasetError> {
    let (filtered, fingerprint) = read_window(s3_client, bucket, lookback_days, session).await?;
    let fit = fit(filtered, training_fraction, target)?;

    Ok(PreparedDataset { fit, fingerprint })
}

/// Reads the same window as [`build`] and engineers returns from it, fitting nothing.
///
/// The rows are the rows the model would train on, because a baseline measured over a wider
/// universe is not a baseline for it. Returns stay unscaled: standardizing is monotone, so it would
/// leave the rank correlation alone and denominate the decile spread in units nobody earns.
pub async fn returns(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
) -> Result<ReturnsDataset, DatasetError> {
    let (filtered, fingerprint) = read_window(s3_client, bucket, lookback_days, session).await?;
    // The model's own two steps, not just the first: `clean_data` drops any row holding a null or
    // non-finite value in any continuous column, so skipping it would measure names the model never
    // sees — a missing vendor VWAP costs a row there and none here.
    let cleaned = clean_data(engineer_features(filtered)?)?;

    Ok(ReturnsDataset {
        returns: cleaned,
        fingerprint,
    })
}

/// Everything both paths share: the archive read, the folds applied to it, and its identity.
///
/// The fingerprint is taken here rather than in either caller, so a baseline run and a training run
/// over the same window report the same one and their journal records join.
async fn read_window(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
) -> Result<(DataFrame, DatasetFingerprint), DatasetError> {
    let splits_frame = archive::read_partition(s3_client, bucket, archive::SPLITS_ARCHIVE_KEY)
        .await?
        .ok_or_else(|| {
            DatasetError::Window(format!(
                "no splits table at {}; refusing to build on unadjusted prices",
                archive::SPLITS_ARCHIVE_KEY
            ))
        })?;
    let splits_digest = digest_of(&splits_frame)?;
    let splits = adjust::SplitTable::from_dataframe(&splits_frame)?;

    let boundaries_frame =
        archive::read_partition(s3_client, bucket, archive::BOUNDARIES_ARCHIVE_KEY).await?;
    let boundaries_digest = boundaries_frame
        .as_ref()
        .map(digest_of)
        .transpose()?
        .unwrap_or(0);
    let boundaries = match boundaries_frame {
        Some(frame) => truncate::BoundaryTable::from_dataframe(&frame)?,
        None => {
            warn!(
                key = archive::BOUNDARIES_ARCHIVE_KEY,
                "No boundary table in the archive; building on unbounded series"
            );
            truncate::BoundaryTable::default()
        }
    };

    let equity_bars = load_archived_bars(
        s3_client,
        bucket,
        lookback_days,
        session,
        &splits,
        &boundaries,
    )
    .await?;

    let equity_details = details::details_to_dataframe(&details::parse_embedded_details()?)?;
    let consolidated = consolidate_data(equity_bars, equity_details)?;
    let filtered = filter_training_bars(consolidated, MINIMUM_CLOSE_PRICE, MINIMUM_VOLUME)?;

    let fingerprint = fingerprint_of(
        &filtered,
        session,
        lookback_days,
        splits_digest,
        boundaries_digest,
    )?;

    Ok((filtered, fingerprint))
}

/// Describes the frame the model will be fitted on, before fitting consumes it.
fn fingerprint_of(
    frame: &DataFrame,
    session: SessionDate,
    lookback_days: i64,
    splits_digest: u64,
    boundaries_digest: u64,
) -> Result<DatasetFingerprint, DatasetError> {
    let timestamps = frame.column("timestamp")?.i64()?;
    let tickers = frame.column("ticker")?.str()?;

    Ok(DatasetFingerprint {
        session,
        lookback_days,
        rows: frame.height(),
        tickers: tickers
            .into_no_null_iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        first_timestamp: timestamps.min().and_then(DateTime::from_timestamp_millis),
        last_timestamp: timestamps.max().and_then(DateTime::from_timestamp_millis),
        splits_digest,
        boundaries_digest,
    })
}

/// Reads every weekday partition in the window, then stitches, bounds, and folds it once.
///
/// Folded over the concatenated window rather than per partition, because the factor depends on
/// where a bar sits relative to `session` and not on which file it came out of.
async fn load_archived_bars(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
    splits: &adjust::SplitTable,
    boundaries: &truncate::BoundaryTable,
) -> Result<DataFrame, DatasetError> {
    let daily_prefix = archive::bar_archive_prefix(BarInterval::OneDay);
    let mut frames: Vec<LazyFrame> = Vec::new();
    let mut unreadable = 0usize;
    let mut date = session.plus_calendar_days(-lookback_days);
    while date <= session {
        if date.is_weekend() {
            date = date.plus_calendar_days(1);
            continue;
        }
        let key = date_partitioned_key(&daily_prefix, date.date());
        if let Some(frame) = archive::read_partition(s3_client, bucket, &key).await? {
            match bars::project_bar_frame(frame) {
                Ok(projected) => frames.push(projected.lazy()),
                Err(error) => {
                    unreadable += 1;
                    warn!(key, %error, "Skipping a partition the training schema cannot read");
                }
            }
        }
        date = date.plus_calendar_days(1);
    }

    // Two different failures wearing one message otherwise: an empty archive sends the operator to
    // the fetch stage, where a window of partitions that all failed projection is a schema problem.
    if frames.is_empty() {
        return Err(DatasetError::Window(if unreadable > 0 {
            format!(
                "all {unreadable} partitions in the lookback window failed projection; none could \
                 be read"
            )
        } else {
            "No equity-bar parquet files found in the lookback window".to_string()
        }));
    }
    if unreadable > 0 {
        warn!(
            unreadable,
            loaded = frames.len(),
            "Some partitions were skipped; building on the rest"
        );
    }
    // Stepping over a bad partition is the point of the projection; stepping over most of them is a
    // different event wearing the same clothes, which the downstream sample counts cannot see.
    if unreadable > frames.len() {
        return Err(DatasetError::Window(format!(
            "{unreadable} partitions could not be read against {} that could; refusing to build on \
             the remainder",
            frames.len()
        )));
    }

    // Stitched, bounded, then folded, in the order the PostgreSQL loader uses: the stitch rescues
    // the bars truncation would drop, and the fold keys on the symbol a bar carries after both.
    let stitched =
        truncate::stitch_bars(concat(frames, UnionArgs::default())?.collect()?, boundaries)?;
    let bounded = truncate::truncate_bars(stitched, boundaries, session)?;
    Ok(adjust::adjust_bars(
        bounded,
        &splits.following_renames(boundaries),
        session,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A partition written before `bar_interval` joined the frame, and one written after. Both must
    /// project to the same schema, because `concat` rejects the window when one member differs.
    #[test]
    fn test_partitions_from_either_writer_project_to_one_schema() {
        let legacy = df![
            "ticker" => ["AAPL"],
            "timestamp" => [1_724_000_000_000i64],
            "open_price" => [100.0],
            "high_price" => [101.0],
            "low_price" => [99.0],
            "close_price" => [100.5],
            "volume" => [1_000i64],
            "volume_weighted_average_price" => [100.2],
        ]
        .unwrap();
        let current = df![
            "ticker" => ["AAPL"],
            "bar_interval" => ["one_day"],
            "timestamp" => [1_724_086_400_000i64],
            "open_price" => [100.5],
            "high_price" => [102.0],
            "low_price" => [100.0],
            "close_price" => [101.5],
            "volume" => [1_100i64],
            "volume_weighted_average_price" => [101.0],
            "transactions" => [42i64],
        ]
        .unwrap();

        let legacy = bars::project_bar_frame(legacy).unwrap();
        let current = bars::project_bar_frame(current).unwrap();
        assert_eq!(legacy.schema(), current.schema());

        let combined = concat([legacy.lazy(), current.lazy()], UnionArgs::default())
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(combined.height(), 2);
    }

    /// A column of the right name but the wrong type is refused rather than nulled.
    ///
    /// The non-strict cast would accept this and leave `clean_data` to drop the rows much later,
    /// reporting a thin session instead of a corrupt partition.
    #[test]
    fn test_a_partition_with_an_uncastable_column_is_refused() {
        let frame = df![
            "ticker" => ["AAPL"],
            "timestamp" => [1_724_000_000_000i64],
            "open_price" => ["not a number"],
            "high_price" => [101.0],
            "low_price" => [99.0],
            "close_price" => [100.5],
            "volume" => [1_000i64],
            "volume_weighted_average_price" => [100.2],
        ]
        .unwrap();
        assert!(bars::project_bar_frame(frame).is_err());
    }

    /// A partition genuinely missing a price column is skipped, not silently null-filled — the
    /// caller counts the skip and trains on the rest.
    #[test]
    fn test_a_partition_missing_a_price_column_is_refused() {
        let frame = df![
            "ticker" => ["AAPL"],
            "timestamp" => [1_724_000_000_000i64],
            "open_price" => [100.0],
        ]
        .unwrap();
        assert!(bars::project_bar_frame(frame).is_err());
    }

    fn frame(tickers: Vec<&str>, timestamps: Vec<i64>) -> DataFrame {
        DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new("timestamp".into(), timestamps),
        ])
        .unwrap()
    }

    #[test]
    fn test_fingerprint_counts_distinct_tickers_not_rows() {
        const DAY: i64 = 86_400_000;
        let fingerprint = fingerprint_of(
            &frame(vec!["AAA", "AAA", "BBB"], vec![0, DAY, DAY]),
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()),
            365,
            0xAB,
            0xCD,
        )
        .unwrap();

        assert_eq!(fingerprint.rows, 3);
        assert_eq!(fingerprint.tickers, 2);
        assert_eq!(
            fingerprint.first_timestamp,
            DateTime::from_timestamp_millis(0)
        );
        assert_eq!(
            fingerprint.last_timestamp,
            DateTime::from_timestamp_millis(DAY)
        );
        assert_eq!(fingerprint.splits_digest, 0xAB);
        assert_eq!(fingerprint.boundaries_digest, 0xCD);
    }

    /// The two table sizes are the whole reason the fingerprint is not just a row count: a
    /// re-adjustment restates prices while the window, the rows, and the tickers all stay put.
    #[test]
    fn test_a_restated_fold_changes_the_fingerprint() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
        let rows = frame(vec!["AAA", "BBB"], vec![0, 0]);

        // One ratio revised in place: same table, same row count, different adjusted prices. This
        // is the case a count cannot see, and the whole reason the digest is here.
        let splits = |ratio: f64| {
            DataFrame::new(vec![
                Column::new("ticker".into(), vec!["AAA", "BBB"]),
                Column::new("ratio".into(), vec![2.0_f64, ratio]),
            ])
            .unwrap()
        };
        let before = splits(3.0);
        let after = splits(4.0);
        assert_eq!(before.height(), after.height());

        let before = fingerprint_of(&rows, session, 365, digest_of(&before).unwrap(), 0).unwrap();
        let after = fingerprint_of(&rows, session, 365, digest_of(&after).unwrap(), 0).unwrap();

        assert_ne!(
            before, after,
            "a revised ratio must not read as one dataset"
        );
    }

    /// The digest is what two runs compare, so the same table must produce the same value however
    /// its rows happen to be ordered on the way back out of the archive.
    #[test]
    fn test_the_digest_is_stable_and_order_independent() {
        let ordered = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAA", "BBB"]),
            Column::new("ratio".into(), vec![2.0_f64, 3.0]),
        ])
        .unwrap();
        let reversed = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["BBB", "AAA"]),
            Column::new("ratio".into(), vec![3.0_f64, 2.0]),
        ])
        .unwrap();

        assert_eq!(digest_of(&ordered).unwrap(), digest_of(&ordered).unwrap());
        assert_eq!(digest_of(&ordered).unwrap(), digest_of(&reversed).unwrap());
    }
}
