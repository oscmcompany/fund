//! The frame every laboratory experiment reads, and the identity that makes two runs comparable.
//!
//! Lifted out of the trainer binary because baselines need this frame and no model.

use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::Serialize;
use tracing::warn;

use crate::common::aws::date_partitioned_key;
use crate::common::types::{BarInterval, LiquidityFloor, Screen, ScreenWindow, SessionDate};
use crate::data::{adjust, archive, bars, reference, truncate};
use crate::laboratory::frame::{self, FrameError};
use crate::laboratory::residual::{residual_returns, FactorSpecification, ResidualPanel};

/// Errors building a dataset.
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("failed to read the archive: {0}")]
    Archive(#[from] archive::ArchiveError),
    #[error("dataframe operation failed: {0}")]
    Frame(#[from] PolarsError),
    #[error("failed to shape the study frame: {0}")]
    Shape(#[from] FrameError),
    #[error("failed to residualize returns: {0}")]
    Residual(#[from] crate::laboratory::residual::ResidualError),
    /// A window that cannot produce a dataset, named rather than returned empty.
    #[error("{0}")]
    Window(String),
}

/// The screen the research paths read the archive through, unless a caller declares another.
///
/// Named here rather than repeated at each of the nine callers, and deliberately *not* the same
/// value as [`crate::data::universe::LIVE_SCREEN`]: the live book screens a trailing month, and the
/// archive paths screen every session they load. That gap is the thing task 14's measurement exists
/// to price, so it is written down as two named screens rather than left to emerge from whichever
/// frame each caller happened to pass.
pub const RESEARCH_SCREEN: Screen = Screen::new(LiquidityFloor::CURRENT, ScreenWindow::WholeFrame);

/// What a dataset was built from, recorded so two runs can be told apart.
///
/// Counts and spans catch a different window; the two digests catch the case they cannot, where the
/// archive holds the same raw bars and a table revised in place restates them at read time.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatasetFingerprint {
    pub session: SessionDate,
    pub lookback_days: i64,
    /// The screen the rows were filtered through, or `None` where none was applied.
    ///
    /// Load-bearing rather than descriptive: two runs at different floors differ only in `rows` and
    /// `tickers`, which is also what two runs over different windows differ in. Without this field
    /// the fingerprint cannot tell those apart, and the floor is about to become configurable.
    pub liquidity_floor: Option<LiquidityFloor>,
    /// The stretch of history that floor was applied over, or `None` where no screen was applied.
    ///
    /// Moves with `liquidity_floor` on every path — a screen is a floor *and* a window, and the same
    /// bounds over a trailing month and over two years admit different sets. Read the pair through
    /// [`DatasetFingerprint::screen`] rather than field by field, which is what keeps the two from
    /// being consulted apart.
    pub screen_window: Option<ScreenWindow>,
    pub rows: usize,
    pub tickers: usize,
    pub first_timestamp: Option<DateTime<Utc>>,
    pub last_timestamp: Option<DateTime<Utc>>,
    /// Content of the splits table the prices were folded against.
    pub splits_digest: u64,
    /// Content of the boundary table the series were stitched and bounded against.
    pub boundaries_digest: u64,
    /// Content of the point-in-time universe the bars were classified against, or `None` where none
    /// was joined. On the intraday path it is the universe the screen was judged against.
    ///
    /// The universe *is* part of the result: two runs over the same window and the same splits, one
    /// before the reference backfill extended and one after, measure different sets of names and
    /// would otherwise be indistinguishable.
    pub reference_digest: Option<u64>,
    /// The factor set the returns were residualized against, or `None` on a path that fits none.
    ///
    /// Load-bearing for the reason `liquidity_floor` is: two panels over the same window and a
    /// different volatility lookback are different measurements, and every other field would agree.
    pub factor_specification: Option<FactorSpecification>,
    /// Whether the quote and trade summaries were joined beside the bars.
    ///
    /// Recorded for the reason the screen is: two panels over the same window and the same screen,
    /// one carrying these columns and one not, are different measurements that every other field
    /// reports identically.
    pub microstructure: Microstructure,
    /// Content of the quote summaries that were joined, or `None` where none were.
    ///
    /// Beside `microstructure` for the reason `splits_digest` sits beside the window: knowing the
    /// summaries were joined does not say *which* summaries, and a partition rewritten in place
    /// changes every feature value while leaving the bars and therefore every other field alone.
    /// A year of these was rewritten under this archive in September 2026.
    pub quote_summary_digest: Option<u64>,
    /// Content of the trade summaries that were joined, or `None` where none were.
    pub trade_summary_digest: Option<u64>,
}

impl DatasetFingerprint {
    /// The screen the rows were filtered through, floor and window together.
    ///
    /// `Some` only when both halves are present, so a caller cannot read a window that no floor was
    /// applied over. The two are written together and are meaningless apart; returning them as a
    /// pair is what stops a reader treating `screen_window: None` as "no window" on a screened
    /// frame rather than as "no screen".
    pub fn screen(&self) -> Option<Screen> {
        self.liquidity_floor
            .zip(self.screen_window)
            .map(|(floor, window)| Screen::new(floor, window))
    }
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

/// One session's returns per name, and the identity of the window they came from.
pub struct ReturnsDataset {
    /// Every engineered feature alongside `ticker` and `timestamp`, cleaned and unscaled.
    pub returns: DataFrame,
    pub fingerprint: DatasetFingerprint,
}

/// One intraday window, folded for splits and otherwise raw.
pub struct IntradayDataset {
    /// Every bar in the window at the requested cadence, split-folded and bounded.
    pub bars: DataFrame,
    pub fingerprint: DatasetFingerprint,
}

/// Reads the intraday partitions in the window, folds splits into them, and keeps the screened names.
///
/// The screen is judged on the window's *daily* bars, as the daily path judges it, and a five-minute
/// bar is kept only where its name cleared on that session: a daily floor applied to one bar's
/// volume would empty the window, and an unscreened read measures whatever the archive happens to hold.
pub async fn intraday(
    s3_client: &S3Client,
    bucket: &str,
    interval: BarInterval,
    lookback_days: i64,
    session: SessionDate,
    screen: Screen,
) -> Result<IntradayDataset, DatasetError> {
    match interval {
        BarInterval::OneMinute | BarInterval::FiveMinute => {}
        // The daily partitions are a different shape and a different screen; `returns` reads those.
        BarInterval::OneDay => {
            return Err(DatasetError::Window(
                "intraday reads an intraday cadence; use returns for daily bars".to_string(),
            ))
        }
    }

    let (daily, daily_fingerprint) = read_window(
        s3_client,
        bucket,
        lookback_days,
        session,
        screen,
        Microstructure::Omitted,
    )
    .await?;
    let adjustments = read_adjustments(s3_client, bucket).await?;
    let digests = Digests {
        splits: adjustments.splits_digest,
        boundaries: adjustments.boundaries_digest,
        reference: daily_fingerprint.reference_digest,
    };
    refuse_moved_tables(&daily_fingerprint, &digests)?;

    let unscreened = load_archived_bars(
        s3_client,
        bucket,
        interval,
        lookback_days,
        session,
        &adjustments.splits,
        &adjustments.boundaries,
    )
    .await?;
    let bars = keep_admitted_sessions(unscreened, &daily)?;
    let fingerprint = fingerprint_of(
        &bars,
        session,
        lookback_days,
        Some(screen),
        digests,
        None,
        Microstructure::Omitted,
        None,
    )?;

    Ok(IntradayDataset { bars, fingerprint })
}

/// Refuses a fold whose split or boundary table is not the one the screen was judged under.
///
/// The intraday path reads the tables twice, so a rewrite between the reads would otherwise screen
/// under one and fold under the other while the fingerprint reported only the second.
fn refuse_moved_tables(
    screened: &DatasetFingerprint,
    folding: &Digests,
) -> Result<(), DatasetError> {
    if screened.splits_digest == folding.splits && screened.boundaries_digest == folding.boundaries
    {
        return Ok(());
    }
    Err(DatasetError::Window(
        "the split or boundary table changed between the daily screen and the intraday read"
            .to_string(),
    ))
}

/// Keeps each intraday bar whose `(ticker, session)` appears among the screened daily bars.
///
/// Per session rather than per name, so a per-session screen admits a name only on the sessions it
/// cleared; a whole-frame screen keeps every session of the names it admits, which is the same rows.
fn keep_admitted_sessions(
    intraday: DataFrame,
    screened_daily: &DataFrame,
) -> Result<DataFrame, DatasetError> {
    let mut sessions_by_stamp: std::collections::HashMap<i64, SessionDate> =
        std::collections::HashMap::new();
    let mut session_of = |stamp: i64| -> Result<SessionDate, DatasetError> {
        if let Some(found) = sessions_by_stamp.get(&stamp) {
            return Ok(*found);
        }
        let instant = DateTime::from_timestamp_millis(stamp).ok_or_else(|| {
            DatasetError::Window(format!("bar timestamp {stamp} is not an instant"))
        })?;
        let found = SessionDate::at(instant);
        sessions_by_stamp.insert(stamp, found);
        Ok(found)
    };

    let mut admitted: std::collections::HashMap<String, std::collections::HashSet<SessionDate>> =
        std::collections::HashMap::new();
    let daily_tickers = screened_daily.column("ticker")?.str()?;
    let daily_stamps = screened_daily.column("timestamp")?.i64()?;
    for (ticker, stamp) in daily_tickers.into_iter().zip(daily_stamps) {
        if let (Some(ticker), Some(stamp)) = (ticker, stamp) {
            admitted
                .entry(ticker.to_string())
                .or_default()
                .insert(session_of(stamp)?);
        }
    }

    let tickers = intraday.column("ticker")?.str()?;
    let stamps = intraday.column("timestamp")?.i64()?;
    let mask = tickers
        .into_iter()
        .zip(stamps)
        .map(|(ticker, stamp)| match (ticker, stamp) {
            (Some(ticker), Some(stamp)) => match admitted.get(ticker) {
                Some(sessions) => Ok(sessions.contains(&session_of(stamp)?)),
                None => Ok(false),
            },
            (None, _) | (_, None) => Ok(false),
        })
        .collect::<Result<BooleanChunked, DatasetError>>()?;
    Ok(intraday.filter(&mask)?)
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
    screen: Screen,
    microstructure: Microstructure,
) -> Result<ReturnsDataset, DatasetError> {
    let (filtered, fingerprint) = read_window(
        s3_client,
        bucket,
        lookback_days,
        session,
        screen,
        microstructure,
    )
    .await?;
    // The model's own two steps, not just the first: `clean_data` drops any row holding a null or
    // non-finite value in any continuous column, so skipping it would measure names the model never
    // sees — a missing vendor VWAP costs a row there and none here.
    let cleaned = frame::clean_frame(frame::add_daily_returns(filtered)?)?;

    Ok(ReturnsDataset {
        returns: cleaned,
        fingerprint,
    })
}

/// One window's returns with the cross-section's common movement stripped out.
pub struct ResidualDataset {
    pub panel: ResidualPanel,
    pub fingerprint: DatasetFingerprint,
}

/// Reads the same window as [`returns`] and residualizes it against the declared factor set.
///
/// The fingerprint names the specification, so a study quoting a figure off this panel is quoting
/// the factor set as well as the window. The panel's own refusal counts travel with it, because a
/// residual defined on part of the cross-section reads exactly like one defined on all of it.
pub async fn residuals(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
    screen: Screen,
    specification: FactorSpecification,
    microstructure: Microstructure,
) -> Result<ResidualDataset, DatasetError> {
    let ReturnsDataset {
        returns,
        mut fingerprint,
    } = self::returns(
        s3_client,
        bucket,
        lookback_days,
        session,
        screen,
        microstructure,
    )
    .await?;
    let panel = residual_returns(&returns, specification)?;
    fingerprint.factor_specification = Some(specification);

    Ok(ResidualDataset { panel, fingerprint })
}

/// Whether a window carries the quote and trade summaries beside its bars.
///
/// A parameter rather than a default because a panel with these columns is a different measurement
/// from one without, and every study written before they existed must keep reading the frame it was
/// written against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Microstructure {
    /// Quote and trade summaries left-joined on `(ticker, session)`.
    Joined,
    /// Bars alone, which is what every study before this one read.
    Omitted,
}

/// The daily quote columns a study may read.
///
/// **Basis points and counts only.** `bid_size_mean` and `ask_size_mean` are share counts under a
/// convention that changed on 2025-11-03 — round lots before it, shares after — and the read-time
/// conversion is not built. A basis-point spread is scale-free, so neither that change nor a split
/// can reach it; the sizes arrive when the conversion does.
const QUOTE_COLUMNS: &[&str] = &[
    "quoted_spread_basis_points_mean",
    "quoted_spread_basis_points_median",
    "quoted_spread_basis_points_ninetieth_percentile",
    "quote_count",
    "covered_seconds",
];

/// The daily trade columns a study may read.
///
/// `signed_volume` is the one worth having: order-flow imbalance per name per session, which no
/// daily bar carries. The sizes here are trade sizes rather than quoted depth, so the quote
/// convention above does not touch them.
const TRADE_COLUMNS: &[&str] = &[
    "signed_volume",
    "trade_count",
    "median_trade_size",
    "ninetieth_percentile_trade_size",
];

/// Reads the window's daily quote and trade summaries, joined to each other on `(ticker, session)`.
///
/// All three archives stamp a session at the 16:00 Eastern close, so the key is exact rather than
/// approximate — verified against the stored partitions before this was written. The *symbol* is
/// not exact until `boundaries` is applied: the bars have been stitched onto each company's current
/// ticker and these partitions are still filed under the one in force at the time.
async fn load_microstructure(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
    boundaries: &truncate::BoundaryTable,
) -> Result<(DataFrame, u64, u64), DatasetError> {
    let quotes = load_summaries(
        s3_client,
        bucket,
        &archive::quote_archive_prefix(BarInterval::OneDay),
        QUOTE_COLUMNS,
        lookback_days,
        session,
    )
    .await?;
    let quotes = stitch_summaries(quotes, boundaries, "covered_seconds")?;
    let trades = load_summaries(
        s3_client,
        bucket,
        &archive::trade_archive_prefix(BarInterval::OneDay),
        TRADE_COLUMNS,
        lookback_days,
        session,
    )
    .await?;
    let trades = stitch_summaries(trades, boundaries, "trade_count")?;

    // Digested as loaded, so a partition rewritten in place changes the fingerprint even though the
    // bars did not move. This archive has had a year of quote partitions rewritten under it.
    let quotes_digest = digest_of(&quotes)?;
    let trades_digest = digest_of(&trades)?;

    // Outer: a session can hold one family and not the other, and dropping the rows that only one
    // covers would make a gap in either archive read as a gap in both.
    let joined = quotes
        .lazy()
        .join(
            trades.lazy(),
            [col("ticker"), col("timestamp")],
            [col("ticker"), col("timestamp")],
            JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns),
        )
        .collect()?;
    Ok((joined, quotes_digest, trades_digest))
}

/// The schema an absent family presents, so the join still contributes its columns as nulls.
fn empty_summary_schema(columns: &[&str]) -> Schema {
    let mut schema = Schema::default();
    schema.insert(PlSmallStr::from("ticker"), DataType::String);
    schema.insert(PlSmallStr::from("timestamp"), DataType::Int64);
    for name in columns {
        schema.insert(PlSmallStr::from(*name), DataType::Float64);
    }
    schema
}

/// Moves each summary row onto the symbol its company trades as now, and collapses the collisions.
///
/// Without this a renamed security's bars say `NEW` while its summaries say `OLD`, the join misses,
/// and every reading for its pre-rename sessions is absent rather than wrong — which understates
/// coverage and conditions the result on a set that excludes renames, a set that is anything but
/// random. Two predecessors can land on one successor, so `keep_by` decides which row survives: the
/// one that saw the most of the session, mirroring how a collided bar keeps its highest volume.
fn stitch_summaries(
    frame: DataFrame,
    boundaries: &truncate::BoundaryTable,
    keep_by: &str,
) -> Result<DataFrame, DatasetError> {
    let stitched = truncate::stitch_bars(frame, boundaries)?;
    Ok(stitched
        .lazy()
        // Ascending, so `Last` keeps the fullest row of a collided pair.
        .sort(
            [keep_by],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .unique_stable(
            Some(polars::prelude::Selector::ByName {
                names: vec![PlSmallStr::from("ticker"), PlSmallStr::from("timestamp")].into(),
                strict: false,
            }),
            UniqueKeepStrategy::Last,
        )
        .collect()?)
}

/// Reads one family's daily partitions over the window, projected to `columns` plus the join key.
///
/// An absent partition is skipped rather than refused: these summaries are an addition to a panel
/// the bars already define, so a missing session must leave nulls and never shorten the window.
async fn load_summaries(
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
    columns: &[&str],
    lookback_days: i64,
    session: SessionDate,
) -> Result<DataFrame, DatasetError> {
    let selected: Vec<Expr> = std::iter::once(col("ticker"))
        .chain(std::iter::once(col("timestamp")))
        .chain(
            columns
                .iter()
                .map(|name| col(*name).cast(DataType::Float64)),
        )
        .collect();

    let mut frames: Vec<LazyFrame> = Vec::new();
    let mut unreadable = 0usize;
    let mut date = session.plus_calendar_days(-lookback_days);
    while date <= session {
        if date.is_weekend() {
            date = date.plus_calendar_days(1);
            continue;
        }
        let key = date_partitioned_key(prefix, date.date());
        if let Some(frame) = archive::read_partition(s3_client, bucket, &key).await? {
            match frame.lazy().select(selected.clone()).collect() {
                Ok(projected) => frames.push(projected.lazy()),
                Err(error) => {
                    unreadable += 1;
                    warn!(key, %error, "Skipping a summary partition the projection cannot read");
                }
            }
        }
        date = date.plus_calendar_days(1);
    }

    if unreadable > 0 {
        warn!(
            prefix,
            unreadable,
            loaded = frames.len(),
            "Some summary partitions were skipped; the sessions they cover will read as null"
        );
    }
    // Absent and unreadable are different events. Nothing found is a family this window does not
    // cover, which the contract above says must read as nulls; partitions that failed to project
    // are a schema problem and stay fatal, because silently nulling those would hide it.
    if frames.is_empty() {
        if unreadable > 0 {
            return Err(DatasetError::Window(format!(
                "all {unreadable} partitions under {prefix} failed projection; none could be read"
            )));
        }
        warn!(
            prefix,
            "No partitions under this prefix in the window; its columns will read as null"
        );
        return Ok(DataFrame::empty_with_schema(&empty_summary_schema(columns)));
    }
    Ok(concat(&frames, UnionArgs::default())?.collect()?)
}

/// One window read from the archive, folded and classified, with no screen applied yet.
///
/// Separated from [`read_window`] so a caller can screen one frame several ways and compare the
/// populations, which is otherwise a re-read of the archive per screen and a different frame each
/// time. The screen is the only step between this and a dataset.
pub struct UnscreenedWindow {
    /// Every bar in the window, split-folded, bounded and joined to the point-in-time universe.
    pub bars: DataFrame,
    /// The last session the window holds, which is what a trailing screen over it anchors on.
    ///
    /// Read off the frame rather than taken as `session`: the archive ends where the last fold
    /// wrote, which is weeks before today, so anchoring a trailing window on today would reach back
    /// over a stretch the archive has no bars in and screen the window away.
    pub last_session: Option<SessionDate>,
    reference_digest: u64,
    splits_digest: u64,
    boundaries_digest: u64,
    microstructure: Microstructure,
    /// The quote and trade summary contents, or `None` where none were joined.
    summary_digests: Option<(u64, u64)>,
}

/// Reads and folds one archive window without screening it.
pub async fn unscreened_window(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
    microstructure: Microstructure,
) -> Result<UnscreenedWindow, DatasetError> {
    let adjustments = read_adjustments(s3_client, bucket).await?;

    let equity_bars = load_archived_bars(
        s3_client,
        bucket,
        BarInterval::OneDay,
        lookback_days,
        session,
        &adjustments.splits,
        &adjustments.boundaries,
    )
    .await?;

    // The universe is point-in-time, not a table taken today: 2,159 of the 5,485 common stocks
    // listed on 2021-09-15 no longer exist, and every earlier measurement was made on the survivors.
    let partitions = archive::read_reference_window(
        s3_client,
        bucket,
        session.plus_calendar_days(-lookback_days),
        session,
    )
    .await?;
    let classification = archive::read_newest_classification(s3_client, bucket).await?;
    let industry_codes = archive::read_newest_industry_codes(s3_client, bucket).await?;
    let universe = reference::universe_of(&partitions, &classification, industry_codes.as_ref())?;
    // Journalled beside the digest: the mapping and the SEC codes are applied on every read, so a
    // study repeated after either republishes groups the same names differently.
    tracing::info!(
        classification_as_of = %classification.as_of(),
        industry_codes_as_of = ?industry_codes.as_ref().map(|codes| codes.as_of()),
        coded_by_sec = universe.coded_by_sec(),
        "Classified the study universe"
    );
    let reference_digest = digest_of(universe.rows())?;
    let consolidated = reference::join_point_in_time(frame::prepare_bars(equity_bars)?, &universe)?;
    // The prices arrived restated onto this session's share basis and the counts did not, so their
    // product is out by the split factor until this runs.
    let bars = adjust::adjust_share_counts(consolidated, &adjustments.splits, session)?;
    // Left, and after every fold: these columns are an addition to the panel the bars define, so a
    // session either archive is short of must leave nulls rather than remove the bar row.
    let mut summary_digests: Option<(u64, u64)> = None;
    let bars = match microstructure {
        Microstructure::Omitted => bars,
        Microstructure::Joined => {
            let (summaries, quotes, trades) = load_microstructure(
                s3_client,
                bucket,
                lookback_days,
                session,
                &adjustments.boundaries,
            )
            .await?;
            summary_digests = Some((quotes, trades));
            let before = bars.height();
            let joined = bars
                .lazy()
                .join(
                    summaries.lazy(),
                    [col("ticker"), col("timestamp")],
                    [col("ticker"), col("timestamp")],
                    JoinArgs::new(JoinType::Left),
                )
                .collect()?;
            if joined.height() != before {
                return Err(DatasetError::Window(format!(
                    "joining the quote and trade summaries moved the panel from {before} rows to \
                     {}; a left join on one session stamp cannot do that",
                    joined.height()
                )));
            }
            joined
        }
    };
    let last_session = newest_session(&bars)?;

    Ok(UnscreenedWindow {
        bars,
        last_session,
        reference_digest,
        splits_digest: adjustments.splits_digest,
        boundaries_digest: adjustments.boundaries_digest,
        microstructure,
        summary_digests,
    })
}

/// The Eastern session of the newest bar in `frame`, or `None` where it holds none.
///
/// `None` is unmeasurable rather than a default date: a frame with no bars has no last session, and
/// substituting today's would silently anchor a window on a day the frame cannot reach.
fn newest_session(frame: &DataFrame) -> Result<Option<SessionDate>, DatasetError> {
    let Some(newest) = frame.column("timestamp")?.i64()?.max() else {
        return Ok(None);
    };
    DateTime::from_timestamp_millis(newest)
        .map(|instant| Some(SessionDate::at(instant)))
        .ok_or_else(|| DatasetError::Window(format!("bar timestamp {newest} is not an instant")))
}

/// Everything both paths share: the archive read, the folds applied to it, and its identity.
///
/// The fingerprint is taken here rather than in either caller, so a baseline run and a training run
/// over the same window report the same one and their journal records join.
///
/// A trailing screen anchors on the window's own last session rather than on `session`, which is
/// today and weeks past where the archive ends. That anchor is not a fingerprint field because
/// `last_timestamp` already carries it — deriving it is what stops the two disagreeing.
async fn read_window(
    s3_client: &S3Client,
    bucket: &str,
    lookback_days: i64,
    session: SessionDate,
    screen: Screen,
    microstructure: Microstructure,
) -> Result<(DataFrame, DatasetFingerprint), DatasetError> {
    let window =
        unscreened_window(s3_client, bucket, lookback_days, session, microstructure).await?;
    // An empty window has no last session to anchor on; `session` screens it to the same nothing
    // and reaches the fingerprint below, which is where an empty window is named.
    let anchor = window.last_session.unwrap_or(session);
    let filtered = frame::filter_study_bars(window.bars, screen, anchor)?;

    let fingerprint = fingerprint_of(
        &filtered,
        session,
        lookback_days,
        // Recorded from the value that did the screening, so the fingerprint describes what happened
        // rather than restating what the caller meant.
        Some(screen),
        Digests {
            splits: window.splits_digest,
            boundaries: window.boundaries_digest,
            reference: Some(window.reference_digest),
        },
        None,
        window.microstructure,
        window.summary_digests,
    )?;

    Ok((filtered, fingerprint))
}

/// The tables every archive window is folded through, and the digests that identify them.
///
/// The two digests are carried in named fields rather than returned as a pair because both are
/// `u64`: swapping them would compile, and would silently join two runs' journal records wrongly.
struct Adjustments {
    splits: adjust::SplitTable,
    boundaries: truncate::BoundaryTable,
    splits_digest: u64,
    boundaries_digest: u64,
}

/// Reads the split and boundary tables that both the daily and the intraday path fold through.
///
/// A missing boundary table warns and leaves the series unbounded, but a missing splits table is
/// refused: unadjusted prices carry a false return across every split in the window.
async fn read_adjustments(s3_client: &S3Client, bucket: &str) -> Result<Adjustments, DatasetError> {
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

    Ok(Adjustments {
        splits,
        boundaries,
        splits_digest,
        boundaries_digest,
    })
}

/// Describes the frame the model will be fitted on, before fitting consumes it.
fn fingerprint_of(
    frame: &DataFrame,
    session: SessionDate,
    lookback_days: i64,
    screen: Option<Screen>,
    digests: Digests,
    factor_specification: Option<FactorSpecification>,
    microstructure: Microstructure,
    summary_digests: Option<(u64, u64)>,
) -> Result<DatasetFingerprint, DatasetError> {
    let timestamps = frame.column("timestamp")?.i64()?;
    let tickers = frame.column("ticker")?.str()?;

    Ok(DatasetFingerprint {
        session,
        lookback_days,
        // Taken apart here and only here, so the two can never be set independently.
        liquidity_floor: screen.map(|screen| screen.floor()),
        screen_window: screen.map(|screen| screen.window()),
        rows: frame.height(),
        tickers: tickers
            .into_no_null_iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        first_timestamp: timestamps.min().and_then(DateTime::from_timestamp_millis),
        last_timestamp: timestamps.max().and_then(DateTime::from_timestamp_millis),
        splits_digest: digests.splits,
        boundaries_digest: digests.boundaries,
        reference_digest: digests.reference,
        factor_specification,
        microstructure,
        quote_summary_digest: summary_digests.map(|(quotes, _)| quotes),
        trade_summary_digest: summary_digests.map(|(_, trades)| trades),
    })
}

/// The content digests of every table a window was folded or joined through.
///
/// Passed as one value rather than three parameters for the reason the fingerprint's own fields are
/// named: all three are `u64`, so swapping two would compile and would silently report two different
/// windows as the same one.
#[derive(Debug, Clone, Copy)]
struct Digests {
    splits: u64,
    boundaries: u64,
    /// `None` on a path that joins no universe, distinguishing that from one that joined an empty
    /// universe — the second is a defect and the first is the intraday path working as intended.
    reference: Option<u64>,
}

/// Reads every weekday partition in the window, then stitches, bounds, and folds it once.
///
/// Folded over the concatenated window rather than per partition, because the factor depends on
/// where a bar sits relative to `session` and not on which file it came out of.
async fn load_archived_bars(
    s3_client: &S3Client,
    bucket: &str,
    interval: BarInterval,
    lookback_days: i64,
    session: SessionDate,
    splits: &adjust::SplitTable,
    boundaries: &truncate::BoundaryTable,
) -> Result<DataFrame, DatasetError> {
    let daily_prefix = archive::bar_archive_prefix(interval);
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

    /// A bar survives only where its name cleared the screen on that session. The 20:30 Eastern bar
    /// is already the next UTC day, so a UTC date join would drop it.
    #[test]
    fn test_an_intraday_bar_is_kept_only_on_a_session_its_name_cleared() {
        let first = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 29).unwrap());
        let second = first.plus_calendar_days(1);
        let at = |session: SessionDate, hours: i64| {
            (session.midnight() + chrono::Duration::hours(hours)).timestamp_millis()
        };
        let screened_daily = df![
            "ticker" => ["AAA", "AAA", "BBB"],
            "timestamp" => [at(first, 16), at(second, 16), at(first, 16)],
        ]
        .unwrap();
        let intraday = df![
            "ticker" => ["AAA", "AAA", "BBB", "BBB", "CCC"],
            "timestamp" => [at(first, 10), at(second, 20) + 30 * 60_000, at(first, 10), at(second, 10), at(first, 10)],
        ]
        .unwrap();

        let kept = keep_admitted_sessions(intraday, &screened_daily).unwrap();

        let rows: Vec<(String, i64)> = kept
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .zip(
                kept.column("timestamp")
                    .unwrap()
                    .i64()
                    .unwrap()
                    .into_no_null_iter(),
            )
            .map(|(ticker, stamp)| (ticker.to_string(), stamp))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("AAA".to_string(), at(first, 10)),
                ("AAA".to_string(), at(second, 20) + 30 * 60_000),
                ("BBB".to_string(), at(first, 10)),
            ]
        );
    }

    /// A renamed security's summaries follow its bars onto the successor symbol.
    ///
    /// `load_archived_bars` stitches a bar onto the company's current ticker while these partitions
    /// stay filed under the one in force at the time, so without the relabel the join misses every
    /// pre-rename session — silently, as absence rather than error.
    #[test]
    fn test_a_renamed_security_keeps_its_summaries() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 30).unwrap());
        let renamed_on =
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 6, 15).unwrap());
        let boundaries = truncate::BoundaryTable::from_dataframe(
            &polars::prelude::df![
                "ticker" => ["OLD"],
                "date" => [renamed_on.date().format("%Y-%m-%d").to_string()],
                "reason" => ["renamed"],
                "related_ticker" => ["NEW"],
            ]
            .expect("the boundary fixture must build"),
        )
        .expect("the boundary table must index");

        let summaries = polars::prelude::df![
            "ticker" => ["OLD"],
            "timestamp" => [session.plus_calendar_days(-30).midnight().timestamp_millis()],
            "covered_seconds" => [100.0f64],
        ]
        .expect("the summary fixture must build");

        let stitched = stitch_summaries(summaries, &boundaries, "covered_seconds")
            .expect("the relabel must run");
        let tickers: Vec<&str> = stitched
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();

        assert_eq!(
            tickers,
            vec!["NEW"],
            "a pre-rename summary must carry the symbol its bar now carries"
        );
    }

    /// The two adjustment digests, for tests that vary those and join no universe.
    fn digests(splits: u64, boundaries: u64) -> Digests {
        Digests {
            splits,
            boundaries,
            reference: None,
        }
    }

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

    /// A client that resolves nothing: no profile, no environment, no instance metadata.
    ///
    /// The default chain would run all of those before the first request, which is discovery a test
    /// that never sends a request has no reason to pay for.
    fn unused_s3_client() -> S3Client {
        S3Client::new(
            &aws_config::SdkConfig::builder()
                .region(aws_config::Region::new("us-east-1"))
                .behavior_version(aws_config::BehaviorVersion::latest())
                .build(),
        )
    }

    #[test]
    fn test_the_intraday_reader_refuses_a_daily_cadence() {
        // No network: the guard runs before anything is read, which is the point of it being a
        // guard, so it refuses before the screen's daily read as well.
        let refused = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime")
            .block_on(async {
                let client = unused_s3_client();
                intraday(
                    &client,
                    "bucket-that-is-never-read",
                    BarInterval::OneDay,
                    730,
                    SessionDate::from_date(
                        chrono::NaiveDate::from_ymd_opt(2026, 8, 20).expect("a valid test date"),
                    ),
                    RESEARCH_SCREEN,
                )
                .await
            });

        assert!(
            matches!(refused, Err(DatasetError::Window(ref message)) if message.contains("returns")),
            "a daily cadence must be refused, naming the path that serves it"
        );
    }

    /// The defect this field exists to close: two runs at different floors read different tables,
    /// and every other field the fingerprint carries can be identical across them.
    ///
    /// Not hypothetical — `build_training_dataset` screens before fingerprinting, so a configurable
    /// floor makes this reachable. The rows are held fixed here so that the floor is the *only*
    /// difference, which is what the un-fingerprinted case looked like from the outside.
    #[test]
    fn test_two_floors_over_the_same_rows_do_not_share_a_fingerprint() {
        const DAY: i64 = 86_400_000;
        let rows = frame(vec!["AAA", "AAA", "BBB"], vec![0, DAY, DAY]);
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
        // Literals rather than `LiquidityFloor::CURRENT`, so this keeps failing if the screen moves.
        let strict = LiquidityFloor::new(10.0, 50_000_000.0).unwrap();
        let loose = LiquidityFloor::new(1.0, 1_000_000.0).unwrap();

        let unscreened = fingerprint_of(
            &rows,
            session,
            365,
            None,
            digests(0xAB, 0xCD),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();
        let strictly = fingerprint_of(
            &rows,
            session,
            365,
            Some(Screen::new(strict, ScreenWindow::WholeFrame)),
            digests(0xAB, 0xCD),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();
        let loosely = fingerprint_of(
            &rows,
            session,
            365,
            Some(Screen::new(loose, ScreenWindow::WholeFrame)),
            digests(0xAB, 0xCD),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();

        assert_eq!(
            strictly.rows, loosely.rows,
            "the fixture must isolate the floor"
        );
        assert_eq!(
            strictly.tickers, loosely.tickers,
            "the fixture must isolate the floor"
        );
        assert_ne!(strictly, loosely, "two floors must not share a fingerprint");
        assert_ne!(unscreened, strictly, "a screen and no screen must differ");
        assert_eq!(unscreened.liquidity_floor, None);
    }

    /// The two screens the tree declares are the same bounds over different history.
    ///
    /// Pinned because it is the whole subject of this change: the live book screens a trailing
    /// month and the archive paths screen every session they load, so the traded population and the
    /// trained one are not the same set. Task 14's measurement prices that gap; this test is what
    /// stops it closing or widening silently in the meantime.
    #[test]
    fn test_the_live_and_research_screens_share_a_floor_and_not_a_window() {
        let live = crate::data::universe::LIVE_SCREEN;

        assert_eq!(
            live.floor(),
            RESEARCH_SCREEN.floor(),
            "the bounds are one decision and must not drift apart"
        );
        assert_ne!(
            live.window(),
            RESEARCH_SCREEN.window(),
            "if these ever agree, the gap this change exists to measure has been closed \
             and the measurement should be retired with it"
        );
        // Literals rather than the constants, so moving either shows up here as a decision.
        assert_eq!(
            live.window(),
            ScreenWindow::Trailing(std::num::NonZeroU32::new(30).unwrap())
        );
        assert_eq!(RESEARCH_SCREEN.window(), ScreenWindow::WholeFrame);
    }

    /// The same bounds over a different stretch of history are a different population.
    ///
    /// A name that dipped below the floor once in two years is refused by a whole-frame screen and
    /// admitted by a trailing one, so the window is as load-bearing as the bounds. Without it in the
    /// fingerprint the two runs are indistinguishable, and a study can declare either and pass.
    #[test]
    fn test_two_screen_windows_over_the_same_rows_do_not_share_a_fingerprint() {
        const DAY: i64 = 86_400_000;
        let rows = frame(vec!["AAA", "AAA", "BBB"], vec![0, DAY, DAY]);
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).unwrap();
        let with = |window| {
            fingerprint_of(
                &rows,
                session,
                365,
                Some(Screen::new(floor, window)),
                digests(0xAB, 0xCD),
                None,
                Microstructure::Omitted,
                None,
            )
            .unwrap()
        };

        let trailing = with(ScreenWindow::Trailing(
            std::num::NonZeroU32::new(30).unwrap(),
        ));
        let whole = with(ScreenWindow::WholeFrame);

        assert_eq!(
            trailing.liquidity_floor, whole.liquidity_floor,
            "the fixture must isolate the window"
        );
        assert_ne!(trailing, whole, "two windows must not share a fingerprint");
    }

    /// The floor and the window are written together, so they can only be read together.
    ///
    /// `screen` returning a pair is what stops a reader treating an absent window on a screened
    /// frame as "no window" rather than as the unreachable state it would be.
    #[test]
    fn test_a_screen_is_read_as_a_pair_or_not_at_all() {
        const DAY: i64 = 86_400_000;
        let rows = frame(vec!["AAA", "AAA", "BBB"], vec![0, DAY, DAY]);
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
        let floor = LiquidityFloor::new(10.0, 50_000_000.0).unwrap();

        let screened = fingerprint_of(
            &rows,
            session,
            365,
            Some(Screen::new(floor, ScreenWindow::WholeFrame)),
            digests(0xAB, 0xCD),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();
        assert_eq!(
            screened.screen(),
            Some(Screen::new(floor, ScreenWindow::WholeFrame)),
            "a screened frame reports both halves"
        );

        let unscreened = fingerprint_of(
            &rows,
            session,
            365,
            None,
            digests(0xAB, 0xCD),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();
        assert_eq!(unscreened.screen(), None);
        assert_eq!(unscreened.liquidity_floor, None);
        assert_eq!(unscreened.screen_window, None);
    }

    /// The universe is an input to the result, not a description of it. Two runs over the same
    /// window and the same adjustment tables, one before the reference backfill extended and one
    /// after, measure different sets of names and must not report the same identity.
    #[test]
    fn test_two_universes_over_the_same_rows_do_not_share_a_fingerprint() {
        const DAY: i64 = 86_400_000;
        let rows = frame(vec!["AAA", "AAA", "BBB"], vec![0, DAY, DAY]);
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
        let with = |reference| {
            fingerprint_of(
                &rows,
                session,
                365,
                None,
                Digests {
                    splits: 0xAB,
                    boundaries: 0xCD,
                    reference,
                },
                None,
                Microstructure::Omitted,
                None,
            )
            .unwrap()
        };

        let unjoined = with(None);
        let one = with(Some(0x11));
        let another = with(Some(0x22));

        assert_eq!(
            one.rows, another.rows,
            "the fixture must isolate the universe"
        );
        assert_eq!(
            one.tickers, another.tickers,
            "the fixture must isolate the universe"
        );
        assert_ne!(one, another, "two universes must not share a fingerprint");
        assert_ne!(unjoined, one, "a joined universe and none must differ");
        assert_eq!(unjoined.reference_digest, None);
    }

    /// Either table moving between the screen's read and the fold's is refused; neither moving is not.
    #[test]
    fn test_a_table_that_moved_between_the_reads_is_refused() {
        let screened = fingerprint_of(
            &frame(vec!["AAA"], vec![0]),
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()),
            90,
            None,
            digests(0xAB, 0xCD),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();

        assert!(refuse_moved_tables(&screened, &digests(0xAB, 0xCD)).is_ok());
        assert!(matches!(
            refuse_moved_tables(&screened, &digests(0xAA, 0xCD)),
            Err(DatasetError::Window(_))
        ));
        assert!(matches!(
            refuse_moved_tables(&screened, &digests(0xAB, 0xCE)),
            Err(DatasetError::Window(_))
        ));
    }

    #[test]
    fn test_fingerprint_counts_distinct_tickers_not_rows() {
        const DAY: i64 = 86_400_000;
        let fingerprint = fingerprint_of(
            &frame(vec!["AAA", "AAA", "BBB"], vec![0, DAY, DAY]),
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()),
            365,
            None,
            digests(0xAB, 0xCD),
            None,
            Microstructure::Omitted,
            None,
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

    /// A factor set is an input to the result, exactly as the liquidity floor is. Two panels over
    /// the same window and a different lookback measure different things and must not read as one.
    #[test]
    fn test_two_factor_specifications_over_the_same_rows_do_not_share_a_fingerprint() {
        const DAY: i64 = 86_400_000;
        let rows = frame(vec!["AAA", "AAA", "BBB"], vec![0, DAY, DAY]);
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap());
        let with = |specification| {
            fingerprint_of(
                &rows,
                session,
                365,
                None,
                digests(0xAB, 0xCD),
                specification,
                Microstructure::Omitted,
                None,
            )
            .unwrap()
        };

        // Literals rather than `FactorSpecification::CURRENT`, so this keeps failing if it moves.
        let unfitted = with(None);
        let short = with(FactorSpecification::new(20, 0.5));
        let long = with(FactorSpecification::new(60, 0.5));
        let strict = with(FactorSpecification::new(60, 0.75));

        assert_eq!(
            short.rows, long.rows,
            "the fixture must isolate the factors"
        );
        assert_ne!(short, long, "two lookbacks must not share a fingerprint");
        assert_ne!(
            long, strict,
            "two variance shares must not share one either"
        );
        assert_ne!(unfitted, long, "fitting and not fitting must differ");
        assert_eq!(unfitted.factor_specification, None);
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

        let before = fingerprint_of(
            &rows,
            session,
            365,
            None,
            digests(digest_of(&before).unwrap(), 0),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();
        let after = fingerprint_of(
            &rows,
            session,
            365,
            None,
            digests(digest_of(&after).unwrap(), 0),
            None,
            Microstructure::Omitted,
            None,
        )
        .unwrap();

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
