//! Point-in-time symbol reference: what each instrument *was* on a date, not what it is now.
//!
//! One partition per `as_of`, because these are dated observations rather than a table revised once.

use polars::prelude::*;

use crate::common::types::{EquityReference, SecurityType, SessionDate};
use crate::data::details::UNKNOWN;

/// The column carrying which `as_of` observation a row was classified by.
///
/// Named once because the universe builder writes it and the bar join reads it, and a literal in
/// both places is two names that agree by coincidence.
pub const AS_OF_COLUMN: &str = "reference_as_of";

/// Builds the frame written to one `as_of` partition.
///
/// Every column is nullable except the symbol and the date, because the feed genuinely declines to
/// classify a tail of instruments and a default would make that indistinguishable from an answer.
pub fn reference_to_dataframe(references: &[EquityReference]) -> Result<DataFrame, PolarsError> {
    let mut tickers: Vec<String> = Vec::with_capacity(references.len());
    let mut as_of: Vec<String> = Vec::with_capacity(references.len());
    let mut security_types: Vec<Option<String>> = Vec::with_capacity(references.len());
    let mut sic_codes: Vec<Option<String>> = Vec::with_capacity(references.len());
    let mut sic_descriptions: Vec<Option<String>> = Vec::with_capacity(references.len());
    let mut shares: Vec<Option<f64>> = Vec::with_capacity(references.len());
    let mut capitalizations: Vec<Option<f64>> = Vec::with_capacity(references.len());
    let mut exchanges: Vec<Option<String>> = Vec::with_capacity(references.len());

    for reference in references {
        tickers.push(reference.ticker().as_str().to_string());
        as_of.push(reference.as_of().date().format("%Y-%m-%d").to_string());
        // The stored code rather than the variant name, so a reader maps back through
        // `SecurityType::from_code` and an unmodelled class survives the round trip.
        security_types.push(
            reference
                .security_type()
                .map(|kind| kind.as_code().to_string()),
        );
        sic_codes.push(reference.sic_code().map(|code| code.as_str().to_string()));
        sic_descriptions.push(reference.sic_description().map(str::to_string));
        shares.push(reference.shares_outstanding());
        capitalizations.push(reference.reported_market_capitalization());
        exchanges.push(reference.primary_exchange().map(str::to_string));
    }

    DataFrame::new(vec![
        Column::new("ticker".into(), tickers),
        Column::new("as_of".into(), as_of),
        Column::new("security_type".into(), security_types),
        Column::new("sic_code".into(), sic_codes),
        Column::new("sic_description".into(), sic_descriptions),
        Column::new("shares_outstanding".into(), shares),
        Column::new("reported_market_capitalization".into(), capitalizations),
        Column::new("primary_exchange".into(), exchanges),
    ])
}

/// The tradeable universe each `as_of` observation declares, one row per (ticker, `as_of`).
///
/// Only common stock survives: the reference feed classifies the ETFs, warrants, funds and units
/// that make up 47% of what the archive holds, and a pairs screen handed two index trackers finds
/// them beautifully cointegrated and means nothing by it.
pub fn universe_of(partitions: &[(SessionDate, DataFrame)]) -> Result<DataFrame, PolarsError> {
    let mut frames: Vec<LazyFrame> = Vec::with_capacity(partitions.len());
    for (as_of, frame) in partitions {
        frames.push(
            frame
                .clone()
                .lazy()
                .filter(col("security_type").eq(lit(SecurityType::CommonStock.as_code())))
                .select([
                    col("ticker"),
                    lit(as_of.midnight().timestamp_millis()).alias(AS_OF_COLUMN),
                    // Spelled rather than left null, matching the retired CSV: an unclassified name
                    // stays in the universe and counts against the sector cap as its own group,
                    // because an unknown sector must not be assumed to diversify.
                    col("sic_code")
                        .str()
                        .head(lit(2))
                        .fill_null(lit(UNKNOWN))
                        .alias("sector"),
                    col("sic_code").fill_null(lit(UNKNOWN)).alias("industry"),
                ]),
        );
    }

    if frames.is_empty() {
        return DataFrame::new(vec![
            Column::new("ticker".into(), Vec::<String>::new()),
            Column::new(AS_OF_COLUMN.into(), Vec::<i64>::new()),
            Column::new("sector".into(), Vec::<String>::new()),
            Column::new("industry".into(), Vec::<String>::new()),
        ]);
    }
    concat(frames, UnionArgs::default())?.collect()
}

/// Joins each bar to the classification that was current when it printed.
///
/// A plain equi-join on a bucket assigned first, rather than an as-of join: the bucket is the
/// greatest `as_of` at or before the bar's own timestamp, which is what makes the universe
/// look-ahead-free by construction rather than by a rule someone has to remember.
pub fn join_point_in_time(bars: DataFrame, universe: DataFrame) -> Result<DataFrame, PolarsError> {
    let mut observations: Vec<i64> = universe
        .column(AS_OF_COLUMN)?
        .i64()?
        .into_no_null_iter()
        .collect();
    observations.sort_unstable();
    observations.dedup();

    // Ascending, so each later observation overrides the earlier ones and what survives is the
    // greatest at or before the bar. A bar older than every observation keeps a null and is dropped
    // by the inner join, which is the honest answer: nothing says what it was.
    let mut bucket = lit(NULL).cast(DataType::Int64);
    for observation in observations {
        bucket = when(col("timestamp").gt_eq(lit(observation)))
            .then(lit(observation))
            .otherwise(bucket);
    }

    bars.lazy()
        .with_column(bucket.alias(AS_OF_COLUMN))
        .join(
            universe.lazy(),
            [col("ticker"), col(AS_OF_COLUMN)],
            [col("ticker"), col(AS_OF_COLUMN)],
            JoinArgs::new(JoinType::Inner),
        )
        .select([
            col("ticker"),
            col("timestamp"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("volume"),
            col("volume_weighted_average_price"),
            col("sector"),
            col("industry"),
        ])
        .collect()
}

/// What one `as_of` sweep did, including what it could not answer.
///
/// `absent` and `failed` are separate because they mean different things: the feed having no record
/// for a symbol that traded that session is a finding about the data, and a request that never
/// completed is a finding about the run. Collapsing them would let an outage read as a quiet market.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReferenceSweep {
    pub requested: usize,
    pub found: usize,
    /// Symbols the feed answered `404` for, despite each having traded that session.
    pub absent: Vec<String>,
    /// Symbols whose request never completed, each with the reason it gave.
    pub failed: Vec<ReferenceFailure>,
}

/// One symbol the feed was asked about and did not answer for, and why.
///
/// The reason travels with the symbol rather than only reaching the log, because a sweep is judged
/// after the fact and a log that has rotated cannot say whether an outage or a bad symbol caused it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceFailure {
    pub ticker: String,
    pub reason: String,
}

impl ReferenceSweep {
    /// Whether the sweep answered for every symbol it asked about.
    pub fn is_complete(&self) -> bool {
        self.failed.is_empty() && self.absent.is_empty()
    }

    /// The share of requested symbols the feed had a record for.
    ///
    /// `None` on an empty request rather than a misleading 1.0, because a sweep that asked nothing
    /// did not achieve full coverage — it achieved no coverage.
    pub fn coverage(&self) -> Option<f64> {
        (self.requested > 0).then(|| self.found as f64 / self.requested as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{SecurityType, SessionDate, SicCode, Ticker};

    fn reference(
        ticker: &str,
        security_type: Option<SecurityType>,
        sic: Option<&str>,
        shares: Option<f64>,
    ) -> EquityReference {
        EquityReference::new(
            Ticker::new(ticker).expect("a valid test symbol"),
            SessionDate::from_date("2021-09-15".parse().expect("a valid date")),
            security_type,
            sic.and_then(SicCode::new),
            None,
            shares,
            None,
            Some("XNAS".to_string()),
        )
        .expect("the fixture must be constructible")
    }

    fn partition(
        as_of: (i32, u32, u32),
        rows: &[(&str, &str, Option<&str>)],
    ) -> (SessionDate, DataFrame) {
        let references: Vec<EquityReference> = rows
            .iter()
            .map(|(ticker, code, sic)| {
                EquityReference::new(
                    Ticker::new(ticker).expect("a valid test symbol"),
                    SessionDate::from_date(
                        chrono::NaiveDate::from_ymd_opt(as_of.0, as_of.1, as_of.2)
                            .expect("a valid date"),
                    ),
                    Some(SecurityType::from_code(code)),
                    sic.and_then(SicCode::new),
                    None,
                    None,
                    None,
                    None,
                )
                .expect("the fixture must be constructible")
            })
            .collect();
        (
            SessionDate::from_date(
                chrono::NaiveDate::from_ymd_opt(as_of.0, as_of.1, as_of.2).expect("a valid date"),
            ),
            reference_to_dataframe(&references).expect("the frame must build"),
        )
    }

    fn bars(rows: &[(&str, i64)]) -> DataFrame {
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new("open_price".into(), vec![1.0_f64; rows.len()]),
            Column::new("high_price".into(), vec![1.0_f64; rows.len()]),
            Column::new("low_price".into(), vec![1.0_f64; rows.len()]),
            Column::new("close_price".into(), vec![1.0_f64; rows.len()]),
            Column::new("volume".into(), vec![1_i64; rows.len()]),
            Column::new(
                "volume_weighted_average_price".into(),
                vec![1.0_f64; rows.len()],
            ),
        ])
        .expect("the bar fixture must build")
    }

    fn instant(year: i32, month: u32, day: u32) -> i64 {
        SessionDate::from_date(
            chrono::NaiveDate::from_ymd_opt(year, month, day).expect("a valid date"),
        )
        .midnight()
        .timestamp_millis()
    }

    /// The whole point of joining a classification at all: 47% of what the archive holds is not
    /// common stock, and a pairs screen handed two index trackers finds them cointegrated.
    #[test]
    fn test_only_common_stock_reaches_the_universe() {
        let universe = universe_of(&[partition(
            (2021, 10, 1),
            &[
                ("AAPL", "CS", Some("3571")),
                ("SPY", "ETF", None),
                ("XYZW", "WARRANT", None),
            ],
        )])
        .expect("the universe must build");

        let tickers: Vec<&str> = universe
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(tickers, vec!["AAPL"]);
    }

    #[test]
    fn test_the_sector_is_the_major_group_and_the_industry_the_whole_code() {
        let universe = universe_of(&[partition(
            (2021, 10, 1),
            &[("AAPL", "CS", Some("3571")), ("AGRI", "CS", Some("0100"))],
        )])
        .expect("the universe must build");

        let sectors = universe.column("sector").unwrap().str().unwrap();
        let industries = universe.column("industry").unwrap().str().unwrap();
        assert_eq!(sectors.get(0), Some("35"));
        assert_eq!(industries.get(0), Some("3571"));
        // The leading zero is significant, which is why the code is stored as a string.
        assert_eq!(sectors.get(1), Some("01"));
        assert_eq!(industries.get(1), Some("0100"));
    }

    /// 595 common stocks carry no SIC. Spelling the gap keeps them in the universe and counts them
    /// as one sector, matching the retired CSV; dropping them would shrink the market silently.
    #[test]
    fn test_a_common_stock_without_a_sic_stays_in_the_universe() {
        let universe = universe_of(&[partition((2021, 10, 1), &[("ZZZZ", "CS", None)])])
            .expect("the universe must build");

        assert_eq!(universe.height(), 1);
        assert_eq!(
            universe.column("sector").unwrap().str().unwrap().get(0),
            Some("NOT AVAILABLE")
        );
    }

    #[test]
    fn test_an_empty_archive_yields_an_empty_universe_with_the_same_schema() {
        let empty = universe_of(&[]).expect("an empty universe must build");
        let populated = universe_of(&[partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))])])
            .expect("the universe must build");

        assert_eq!(empty.height(), 0);
        assert_eq!(empty.schema(), populated.schema());
    }

    /// A bar is classified by the observation current when it printed, never by a later one. This
    /// is what makes the universe look-ahead-free by construction rather than by a remembered rule.
    #[test]
    fn test_a_bar_takes_the_observation_current_when_it_printed() {
        let universe = universe_of(&[
            partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))]),
            partition((2022, 1, 3), &[("AAPL", "CS", Some("7372"))]),
        ])
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("AAPL", instant(2021, 11, 15)),
                ("AAPL", instant(2022, 2, 10)),
            ]),
            universe,
        )
        .expect("the join must run");

        let industries = joined.column("industry").unwrap().str().unwrap();
        assert_eq!(joined.height(), 2);
        assert_eq!(industries.get(0), Some("3571"));
        assert_eq!(industries.get(1), Some("7372"));
    }

    /// The survivorship fix itself. A name present in the earlier observation and gone from the
    /// later one keeps its bars up to the delisting and loses them after — where the retired CSV,
    /// taken in 2026, dropped every one of its bars including the ones it traded for.
    #[test]
    fn test_a_delisted_name_keeps_the_sessions_it_traded() {
        let universe = universe_of(&[
            partition(
                (2021, 10, 1),
                &[("AAPL", "CS", Some("3571")), ("TWTR", "CS", Some("7370"))],
            ),
            partition((2022, 1, 3), &[("AAPL", "CS", Some("3571"))]),
        ])
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("TWTR", instant(2021, 11, 15)),
                ("TWTR", instant(2022, 2, 10)),
                ("AAPL", instant(2022, 2, 10)),
            ]),
            universe,
        )
        .expect("the join must run");

        let tickers: Vec<&str> = joined
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(joined.height(), 2);
        assert!(tickers.contains(&"TWTR"), "the session it traded survives");
        assert_eq!(
            tickers.iter().filter(|ticker| **ticker == "TWTR").count(),
            1,
            "and only that one"
        );
    }

    /// A bar older than every observation cannot be classified, and the honest answer is to drop it
    /// rather than reach forward for the first observation that postdates it.
    #[test]
    fn test_a_bar_predating_every_observation_is_dropped() {
        let universe = universe_of(&[partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))])])
            .expect("the universe must build");

        let joined = join_point_in_time(bars(&[("AAPL", instant(2021, 9, 15))]), universe)
            .expect("the join must run");

        assert_eq!(joined.height(), 0);
    }

    #[test]
    fn test_a_bar_for_a_name_outside_the_universe_is_dropped() {
        let universe = universe_of(&[partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))])])
            .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("AAPL", instant(2021, 11, 1)),
                ("SPY", instant(2021, 11, 1)),
            ]),
            universe,
        )
        .expect("the join must run");

        assert_eq!(joined.height(), 1);
        assert_eq!(
            joined.column("ticker").unwrap().str().unwrap().get(0),
            Some("AAPL")
        );
    }

    #[test]
    fn test_the_frame_carries_one_row_per_symbol_with_the_stored_codes() {
        let frame = reference_to_dataframe(&[
            reference(
                "AAPL",
                Some(SecurityType::CommonStock),
                Some("3571"),
                Some(16_530_169_999.0),
            ),
            reference(
                "SPY",
                Some(SecurityType::ExchangeTradedFund),
                None,
                Some(899_630_000.0),
            ),
        ])
        .expect("the frame must build");

        assert_eq!(frame.height(), 2);
        let types = frame.column("security_type").unwrap().str().unwrap();
        assert_eq!(types.get(0), Some("CS"));
        assert_eq!(types.get(1), Some("ETF"));
        let codes = frame.column("sic_code").unwrap().str().unwrap();
        assert_eq!(codes.get(0), Some("3571"));
        // Null rather than an empty string, so a reader can tell "no SIC" from "SIC of nothing".
        assert_eq!(codes.get(1), None);
    }

    /// The tail the feed declines to classify must survive into the partition as null, because a
    /// default would make it indistinguishable from a symbol it did classify.
    #[test]
    fn test_an_unclassified_symbol_is_stored_as_a_null_type() {
        let frame =
            reference_to_dataframe(&[reference("ZZZZ", None, None, None)]).expect("must build");

        assert_eq!(
            frame.column("security_type").unwrap().str().unwrap().get(0),
            None
        );
        assert_eq!(
            frame
                .column("shares_outstanding")
                .unwrap()
                .f64()
                .unwrap()
                .get(0),
            None
        );
        assert_eq!(
            frame.column("ticker").unwrap().str().unwrap().get(0),
            Some("ZZZZ")
        );
    }

    #[test]
    fn test_the_schema_is_the_same_whether_or_not_a_field_was_answered() {
        let populated = reference_to_dataframe(&[reference(
            "AAPL",
            Some(SecurityType::CommonStock),
            Some("3571"),
            Some(1.0),
        )])
        .expect("must build");
        let empty =
            reference_to_dataframe(&[reference("ZZZZ", None, None, None)]).expect("must build");

        // A schema that varied with the data would make two partitions unreadable together.
        assert_eq!(populated.schema(), empty.schema());
    }

    /// The distinction the archive guard turns on: a symbol the feed answered `404` for is an
    /// answer and does not block the write, while one whose request never completed does.
    ///
    /// Collapsing them would either refuse every partition — the absent residual is 7 to 18 symbols
    /// a quarter and never zero — or let an outage silently shorten a complete stored partition.
    #[test]
    fn test_an_absent_symbol_is_not_a_failed_one() {
        let only_absent = ReferenceSweep {
            requested: 100,
            found: 99,
            absent: vec!["TWTR".to_string()],
            failed: Vec::new(),
        };
        let one_failed = ReferenceSweep {
            requested: 100,
            found: 99,
            absent: Vec::new(),
            failed: vec![ReferenceFailure {
                ticker: "AAPL".to_string(),
                reason: "connection reset".to_string(),
            }],
        };

        // Identical coverage, opposite consequences.
        assert_eq!(only_absent.coverage(), one_failed.coverage());
        assert!(
            only_absent.failed.is_empty(),
            "a 404 must not block the write"
        );
        assert!(!one_failed.failed.is_empty(), "an unanswered request must");
    }

    #[test]
    fn test_an_empty_sweep_reports_no_coverage_rather_than_complete_coverage() {
        let nothing = ReferenceSweep::default();

        assert_eq!(nothing.coverage(), None);
        // Vacuously complete on the failure counts, which is why coverage is the figure to read.
        assert!(nothing.is_complete());
    }

    #[test]
    fn test_a_sweep_separates_what_the_feed_lacked_from_what_never_answered() {
        let sweep = ReferenceSweep {
            requested: 10,
            found: 7,
            absent: vec!["TWTR".to_string()],
            failed: vec![
                ReferenceFailure {
                    ticker: "AAPL".to_string(),
                    reason: "connection reset".to_string(),
                },
                ReferenceFailure {
                    ticker: "MSFT".to_string(),
                    reason: "429 slow down".to_string(),
                },
            ],
        };

        assert_eq!(sweep.coverage(), Some(0.7));
        assert!(!sweep.is_complete());
        assert_eq!(sweep.absent.len(), 1);
        assert_eq!(sweep.failed.len(), 2);
        // The cause travels with the symbol, so a sweep can be judged without its log.
        assert_eq!(sweep.failed[0].ticker, "AAPL");
        assert_eq!(sweep.failed[0].reason, "connection reset");
    }
}
