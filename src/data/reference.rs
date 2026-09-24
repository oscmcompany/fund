//! Point-in-time symbol reference: what each instrument *was* on a date, not what it is now.
//!
//! One partition per `as_of`, because these are dated observations rather than a table revised once.

use std::collections::BTreeMap;

use polars::prelude::*;

use crate::common::types::{Cik, EquityReference, SecurityType, SessionDate, SicCode};
use crate::data::classification::{self, ClassificationTable};
use crate::data::classification_table::{Industry, Sector};
use crate::data::details::{industry_code, sector_code};
use crate::data::industry_codes::IndustryCodesTable;

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
    let mut ciks: Vec<Option<String>> = Vec::with_capacity(references.len());

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
        ciks.push(reference.cik().map(|cik| cik.as_str().to_string()));
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
        Column::new("cik".into(), ciks),
    ])
}

/// Observations a symbol keeps its classification across while the feed has no record of it.
///
/// One, not unbounded. The gap being repaired is a transient `404` on a name that traded, which is 7
/// to 18 symbols a quarter; a name the feed has not seen for two consecutive quarters while it keeps
/// printing bars is a different question, and on at least one measured ticker it is the same symbol
/// reused by a different company. Carrying a 2021 sector across five years onto a later listing
/// would classify one company's bars as another's.
const CARRY_FORWARD_OBSERVATIONS: u32 = 1;

/// One symbol as a single `as_of` observation saw it.
///
/// Carries the type alongside the classification because the common-stock filter runs *after* the
/// carry-forward below, and a row that has dropped its type cannot be filtered on later.
#[derive(Clone)]
struct Observation {
    security_type: Option<String>,
    /// `None` where the feed reported no usable SIC code, which is 17% of common stock.
    ///
    /// Never a sentinel bucket: the twelve-industry definition already has an `Other` group that
    /// shares a factor, and folding the unclassified into it would put two meanings in one value.
    sector: Option<Sector>,
    industry: Option<Industry>,
    /// `None` where the feed declined to report it, which is 4% of common stock from 2025-07 and
    /// under 1% before. Never defaulted: a zero share count is not a company with no equity.
    shares_outstanding: Option<f64>,
    /// Observations since the feed last answered for this symbol. Zero on a fresh answer.
    carried: u32,
    /// Whether the code came from the SEC because Massive reported none, which a reader should be
    /// able to count: SEC codes are today's, not the observation's.
    coded_by_sec: bool,
}

/// The tradeable universe, and the grid of observations it was built from.
///
/// The two travel together because the grid is not recoverable from the rows: an observation where
/// every name left common stock contributes no rows at all, and a join that inferred its boundaries
/// from the rows alone would extend the previous observation straight past it.
pub struct Universe {
    rows: DataFrame,
    observations: Vec<i64>,
    coded_by_sec: usize,
}

impl Universe {
    /// One row per (ticker, `as_of`) that was common stock then.
    pub fn rows(&self) -> &DataFrame {
        &self.rows
    }

    /// Rows whose sector came from the SEC's current code because Massive reported none.
    pub fn coded_by_sec(&self) -> usize {
        self.coded_by_sec
    }
}

/// The tradeable universe at each `as_of`, one row per (ticker, `as_of`), common stock only.
///
/// Each observation carries every symbol seen at or before it, not only the symbols that observation
/// answered for. The feed returns a 404 for 7 to 18 symbols a quarter that traded, and an inner join
/// against the raw partitions would drop each of those names for a whole quarter.
pub fn universe_of(
    partitions: &[(SessionDate, DataFrame)],
    table: &ClassificationTable,
    industry_codes: Option<&IndustryCodesTable>,
) -> Result<Universe, PolarsError> {
    // Ascending here rather than trusting the caller: the carry-forward is only correct in order,
    // and a caller that listed the prefix differently would silently invert it.
    let mut ordered: Vec<&(SessionDate, DataFrame)> = partitions.iter().collect();
    ordered.sort_by_key(|(as_of, _frame)| *as_of);

    let mut current: BTreeMap<String, Observation> = BTreeMap::new();
    let mut observations: Vec<i64> = Vec::with_capacity(ordered.len());
    let mut tickers: Vec<String> = Vec::new();
    let mut observed_at: Vec<i64> = Vec::new();
    let mut sectors: Vec<String> = Vec::new();
    let mut industries: Vec<String> = Vec::new();
    let mut shares: Vec<Option<f64>> = Vec::new();
    let mut coded_by_sec = 0usize;

    for (as_of, frame) in ordered {
        // Aged first, so a symbol this observation answers for is reset to zero below and only the
        // ones it stayed silent about advance.
        current.retain(|_ticker, observation| {
            observation.carried += 1;
            observation.carried <= CARRY_FORWARD_OBSERVATIONS
        });
        for (ticker, observation) in observations_of(frame, table, industry_codes)? {
            // A newer observation supersedes an older one, which is what makes a reclassification
            // out of common stock take effect rather than being carried past.
            current.insert(ticker, observation);
        }

        let stamp = as_of.midnight().timestamp_millis();
        observations.push(stamp);
        for (ticker, observation) in &current {
            if observation.security_type.as_deref() != Some(SecurityType::CommonStock.as_code()) {
                continue;
            }
            tickers.push(ticker.clone());
            observed_at.push(stamp);
            sectors.push(sector_code(observation.sector));
            industries.push(industry_code(observation.industry));
            shares.push(observation.shares_outstanding);
            coded_by_sec += usize::from(observation.coded_by_sec);
        }
    }

    Ok(Universe {
        rows: DataFrame::new(vec![
            Column::new("ticker".into(), tickers),
            Column::new(AS_OF_COLUMN.into(), observed_at),
            Column::new("sector".into(), sectors),
            Column::new("industry".into(), industries),
            Column::new("shares_outstanding".into(), shares),
        ])?,
        observations,
        coded_by_sec,
    })
}

/// Reads one stored partition into the per-symbol observations it declares.
fn observations_of(
    frame: &DataFrame,
    table: &ClassificationTable,
    industry_codes: Option<&IndustryCodesTable>,
) -> Result<Vec<(String, Observation)>, PolarsError> {
    let tickers = frame.column("ticker")?.str()?;
    let security_types = frame.column("security_type")?.str()?;
    let sic_codes = frame.column("sic_code")?.str()?;
    let shares = frame.column("shares_outstanding")?.f64()?;
    // Absent from partitions swept before the CIK was recorded, which then fall back to nothing.
    let ciks = frame
        .column("cik")
        .ok()
        .map(|column| column.str().cloned())
        .transpose()?;

    let mut observations = Vec::with_capacity(frame.height());
    for index in 0..frame.height() {
        let Some(ticker) = tickers.get(index) else {
            continue;
        };
        // Massive's point-in-time code wins; the SEC's current one only fills what it left empty.
        let reported = sic_codes.get(index).and_then(SicCode::new);
        let filled = match (&reported, industry_codes, &ciks) {
            (None, Some(industry_codes), Some(ciks)) => ciks
                .get(index)
                .and_then(Cik::new)
                .and_then(|cik| industry_codes.code_of(&cik).cloned()),
            _ => None,
        };
        let coded_by_sec = filled.is_some();
        let sic_code = reported.or(filled);
        observations.push((
            ticker.to_string(),
            Observation {
                security_type: security_types.get(index).map(str::to_string),
                carried: 0,
                // Finite and positive: a stored partition reaches here without passing through
                // `EquityReference::new`, and an infinity would surface as an unmeasured size.
                shares_outstanding: shares
                    .get(index)
                    .filter(|count| count.is_finite() && *count > 0.0),
                // Both lookups are total over four-digit codes, so `None` here carries exactly
                // one meaning: neither Massive nor the SEC reported a usable code.
                sector: sic_code
                    .as_ref()
                    .map(|code| classification::sector_of(table, code)),
                industry: sic_code
                    .as_ref()
                    .map(|code| classification::industry_of(table, code)),
                coded_by_sec,
            },
        ));
    }
    Ok(observations)
}

/// Joins each bar to the classification that was current when it printed.
///
/// A plain equi-join on a bucket assigned first, rather than an as-of join: the bucket is the
/// greatest `as_of` at or before the bar's own timestamp, which is what makes the universe
/// look-ahead-free by construction rather than by a rule someone has to remember.
pub fn join_point_in_time(bars: DataFrame, universe: &Universe) -> Result<DataFrame, PolarsError> {
    let mut observations = universe.observations.clone();
    observations.sort_unstable();
    observations.dedup();

    // Ascending, so each later observation overrides the earlier ones and the greatest at or before
    // the bar survives. A bar older than every observation keeps a null and the join drops it.
    let mut bucket = lit(NULL).cast(DataType::Int64);
    for observation in observations {
        bucket = when(col("timestamp").gt_eq(lit(observation)))
            .then(lit(observation))
            .otherwise(bucket);
    }

    bars.lazy()
        .with_column(bucket.alias(AS_OF_COLUMN))
        .join(
            universe.rows.clone().lazy(),
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
            // Nullable, and deliberately absent from the model's continuous columns: a name the feed
            // has no share count for must lose its size factor, not its bars.
            col("shares_outstanding"),
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

/// Why a sweep left the partition it was writing alone.
///
/// Each variant carries the count that produced it, because "unanswered" and "no records" are read
/// off a failed nightly run by someone who then has to decide whether to retry or to investigate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepRefusal {
    /// Symbols whose request never completed, so any partition written would be partial.
    Unanswered { failed: usize, requested: usize },
    /// Every request completed and the feed had a record for none of them.
    NoRecords { requested: usize },
}

impl std::fmt::Display for SweepRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SweepRefusal::Unanswered { failed, requested } => {
                write!(formatter, "{failed} of {requested} symbols unanswered")
            }
            SweepRefusal::NoRecords { requested } => write!(
                formatter,
                "the feed had no record for any of {requested} symbols"
            ),
        }
    }
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

    /// Whether this sweep replaced the partition.
    ///
    /// Distinct from [`ReferenceSweep::is_complete`], which asks about the symbols rather than the
    /// object. Derived from [`ReferenceSweep::refusal`] so the two answers cannot disagree.
    pub fn wrote_partition(&self) -> bool {
        self.refusal().is_none()
    }

    /// Why the partition was left alone, or `None` if it was replaced.
    ///
    /// The archive refuses the write on any unanswered symbol, and separately has nothing to write
    /// when the feed had no record for any of them. Those are different facts about a night and the
    /// operator reading a failed run needs to be told which one happened.
    pub fn refusal(&self) -> Option<SweepRefusal> {
        if !self.failed.is_empty() {
            return Some(SweepRefusal::Unanswered {
                failed: self.failed.len(),
                requested: self.requested,
            });
        }
        (self.found == 0).then_some(SweepRefusal::NoRecords {
            requested: self.requested,
        })
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
mod sweep_tests {
    use super::*;

    #[test]
    fn test_a_sweep_that_answered_nothing_wrote_nothing() {
        // The feed was reachable, no symbol came back, and the archive preserved what it had.
        let empty = ReferenceSweep {
            requested: 4_000,
            found: 0,
            absent: Vec::new(),
            failed: Vec::new(),
        };
        assert!(!empty.wrote_partition());
        assert_eq!(
            empty.refusal(),
            Some(SweepRefusal::NoRecords { requested: 4_000 })
        );
        assert_eq!(
            empty.refusal().expect("a refusal").to_string(),
            "the feed had no record for any of 4000 symbols"
        );
        assert!(
            empty.is_complete(),
            "no symbol failed or was refused, so the symbol-level question says complete"
        );
    }

    #[test]
    fn test_one_failed_symbol_stops_the_write() {
        let partial = ReferenceSweep {
            requested: 4_000,
            found: 3_999,
            absent: Vec::new(),
            failed: vec![ReferenceFailure {
                ticker: "AAPL".to_string(),
                reason: "timed out".to_string(),
            }],
        };
        assert!(!partial.wrote_partition());
        assert_eq!(
            partial.refusal(),
            Some(SweepRefusal::Unanswered {
                failed: 1,
                requested: 4_000
            })
        );
    }

    #[test]
    fn test_the_refusal_names_the_unanswered_symbols_before_the_empty_feed() {
        // Both conditions at once, which is what an outage mid-sweep looks like. Reporting no
        // records would send the operator to the feed when the run is what failed.
        let outage = ReferenceSweep {
            requested: 4_000,
            found: 0,
            absent: Vec::new(),
            failed: vec![ReferenceFailure {
                ticker: "AAPL".to_string(),
                reason: "timed out".to_string(),
            }],
        };
        assert_eq!(
            outage.refusal(),
            Some(SweepRefusal::Unanswered {
                failed: 1,
                requested: 4_000
            })
        );
    }

    #[test]
    fn test_an_absent_symbol_does_not_stop_the_write() {
        // A 404 for a symbol that traded is the ordinary residual of a whole-market sweep.
        let swept = ReferenceSweep {
            requested: 4_000,
            found: 3_998,
            absent: vec!["ZVZZT".to_string(), "ZXYZ.A".to_string()],
            failed: Vec::new(),
        };
        assert!(swept.wrote_partition());
        assert_eq!(swept.refusal(), None);
        assert!(!swept.is_complete());
    }
}

#[cfg(test)]
mod tests {
    /// The six published runs the universe tests actually exercise, copied from the dataset.
    ///
    /// A fixture rather than the whole mapping, because what is under test here is the
    /// carry-forward and the common-stock filter; the classification is an input to that. Every
    /// other code lands in the fallback, which no test in this module asserts on.
    fn table() -> ClassificationTable {
        use crate::data::classification_table::SicRange;
        ClassificationTable::new(
            chrono::NaiveDate::from_ymd_opt(2026, 9, 22).expect("a real date"),
            vec![
                SicRange {
                    low: 100,
                    high: 999,
                    bucket: Sector::ConsumerNondurables,
                },
                SicRange {
                    low: 3570,
                    high: 3579,
                    bucket: Sector::BusinessEquipment,
                },
                SicRange {
                    low: 7370,
                    high: 7379,
                    bucket: Sector::BusinessEquipment,
                },
            ],
            vec![
                SicRange {
                    low: 100,
                    high: 199,
                    bucket: Industry::Agriculture,
                },
                SicRange {
                    low: 3570,
                    high: 3579,
                    bucket: Industry::Computers,
                },
                SicRange {
                    low: 7370,
                    high: 7372,
                    bucket: Industry::ComputerSoftware,
                },
            ],
        )
        .expect("the fixture runs are disjoint and ascending")
    }

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
        let with_shares: Vec<(&str, &str, Option<&str>, Option<f64>)> = rows
            .iter()
            .map(|(ticker, code, sic)| (*ticker, *code, *sic, None))
            .collect();
        partition_with_shares(as_of, &with_shares)
    }

    fn partition_with_shares(
        as_of: (i32, u32, u32),
        rows: &[(&str, &str, Option<&str>, Option<f64>)],
    ) -> (SessionDate, DataFrame) {
        let references: Vec<EquityReference> = rows
            .iter()
            .map(|(ticker, code, sic, shares)| {
                EquityReference::new(
                    Ticker::new(ticker).expect("a valid test symbol"),
                    SessionDate::from_date(
                        chrono::NaiveDate::from_ymd_opt(as_of.0, as_of.1, as_of.2)
                            .expect("a valid date"),
                    ),
                    Some(SecurityType::from_code(code)),
                    sic.and_then(SicCode::new),
                    None,
                    *shares,
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

    /// A partition whose rows name the filer, which every sweep since the CIK was recorded writes.
    fn partition_with_ciks(
        as_of: (i32, u32, u32),
        rows: &[(&str, Option<&str>, Option<&str>)],
    ) -> (SessionDate, DataFrame) {
        let date = SessionDate::from_date(
            chrono::NaiveDate::from_ymd_opt(as_of.0, as_of.1, as_of.2).expect("a valid date"),
        );
        let references: Vec<EquityReference> = rows
            .iter()
            .map(|(ticker, sic, cik)| {
                EquityReference::new(
                    Ticker::new(ticker).expect("a valid test symbol"),
                    date,
                    Some(SecurityType::CommonStock),
                    sic.and_then(SicCode::new),
                    None,
                    None,
                    None,
                    None,
                )
                .expect("the fixture must be constructible")
                .with_cik(cik.and_then(Cik::new))
            })
            .collect();
        (
            date,
            reference_to_dataframe(&references).expect("the frame must build"),
        )
    }

    fn sectors(universe: &Universe) -> Vec<(String, String)> {
        let tickers = universe.rows().column("ticker").unwrap().str().unwrap();
        let sectors = universe.rows().column("sector").unwrap().str().unwrap();
        (0..universe.rows().height())
            .map(|row| {
                (
                    tickers.get(row).unwrap().to_string(),
                    sectors.get(row).unwrap().to_string(),
                )
            })
            .collect()
    }

    /// The gap this exists for: Massive gives AZN, RY and SPOT no code but does name the filer, and
    /// EDGAR's code for that filer places it. Massive's own code, where it has one, still wins --
    /// it is the observation's, and the SEC's is only today's.
    #[test]
    fn test_the_sec_code_fills_only_what_massive_left_empty() {
        let observation = partition_with_ciks(
            (2026, 7, 1),
            &[
                ("AGRI", Some("0100"), Some("1")),
                ("AZN", None, Some("901832")),
                ("NOCK", None, None),
            ],
        );
        let industry_codes = crate::data::industry_codes::fixture::table(vec![
            crate::data::industry_codes::fixture::code("1", "3571"),
            crate::data::industry_codes::fixture::code("901832", "3571"),
        ]);

        let without = universe_of(&[observation.clone()], &table(), None).unwrap();
        let with = universe_of(&[observation], &table(), Some(&industry_codes)).unwrap();

        let expected_without = vec![
            ("AGRI".to_string(), "ConsumerNondurables".to_string()),
            ("AZN".to_string(), "NOT AVAILABLE".to_string()),
            ("NOCK".to_string(), "NOT AVAILABLE".to_string()),
        ];
        assert_eq!(sectors(&without), expected_without);
        assert_eq!(
            sectors(&with),
            vec![
                ("AGRI".to_string(), "ConsumerNondurables".to_string()),
                ("AZN".to_string(), "BusinessEquipment".to_string()),
                ("NOCK".to_string(), "NOT AVAILABLE".to_string()),
            ],
            "AGRI keeps Massive's code over the SEC's, AZN is filled, and a name with no filer stays empty"
        );
        assert_eq!(without.coded_by_sec(), 0);
        assert_eq!(
            with.coded_by_sec(),
            1,
            "only AZN's sector came from the SEC"
        );
    }

    /// Partitions swept before the CIK was recorded have no `cik` column, and must read as they
    /// always did rather than failing the universe.
    #[test]
    fn test_a_partition_without_a_cik_column_reads_as_before() {
        let (as_of, frame) = partition_with_ciks((2021, 10, 1), &[("AZN", None, Some("901832"))]);
        let frame = frame.drop("cik").expect("the column must drop");
        let industry_codes = crate::data::industry_codes::fixture::table(vec![
            crate::data::industry_codes::fixture::code("901832", "3571"),
        ]);

        let universe = universe_of(&[(as_of, frame)], &table(), Some(&industry_codes)).unwrap();

        assert_eq!(
            sectors(&universe),
            vec![("AZN".to_string(), "NOT AVAILABLE".to_string())]
        );
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
        let universe = universe_of(
            &[partition(
                (2021, 10, 1),
                &[
                    ("AAPL", "CS", Some("3571")),
                    ("SPY", "ETF", None),
                    ("XYZW", "WARRANT", None),
                ],
            )],
            &table(),
            None,
        )
        .expect("the universe must build");

        let tickers: Vec<&str> = universe
            .rows()
            .column("ticker")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(tickers, vec!["AAPL"]);
    }

    /// The whole code is looked up, not sliced.
    ///
    /// The two-digit major group this replaced put electronic computers in `35` beside construction
    /// machinery and prepackaged software in `73` beside advertising. Pinned to literal bucket names
    /// rather than to the lookup, so a table that moves fails here instead of agreeing with itself.
    #[test]
    fn test_the_sector_and_industry_are_looked_up_from_the_whole_code() {
        let universe = universe_of(
            &[partition(
                (2021, 10, 1),
                &[
                    ("AAPL", "CS", Some("3571")),
                    ("SFTW", "CS", Some("7372")),
                    ("AGRI", "CS", Some("0100")),
                ],
            )],
            &table(),
            None,
        )
        .expect("the universe must build");

        let sectors = universe.rows().column("sector").unwrap().str().unwrap();
        let industries = universe.rows().column("industry").unwrap().str().unwrap();

        // Rows come back ordered by ticker, not in the order the partition listed them.
        assert_eq!(sectors.get(0), Some("BusinessEquipment"));
        assert_eq!(industries.get(0), Some("Computers"));
        // The leading zero is significant, which is why the code is stored as a string.
        assert_eq!(sectors.get(1), Some("ConsumerNondurables"));
        assert_eq!(industries.get(1), Some("Agriculture"));
        // Split from AAPL by the major group, joined to it here.
        assert_eq!(sectors.get(2), Some("BusinessEquipment"));
        assert_eq!(industries.get(2), Some("ComputerSoftware"));
    }

    /// 901 of 5,217 common stocks carry no SIC, measured 2026-09-17. Spelling the gap keeps them in
    /// the universe; dropping them would shrink the market silently. What the sentinel means is the
    /// reader's decision, not this one's — the screen pools them and the residual panel refuses them.
    #[test]
    fn test_a_common_stock_without_a_sic_stays_in_the_universe() {
        let universe = universe_of(
            &[partition((2021, 10, 1), &[("ZZZZ", "CS", None)])],
            &table(),
            None,
        )
        .expect("the universe must build");

        assert_eq!(universe.rows().height(), 1);
        assert_eq!(
            universe
                .rows()
                .column("sector")
                .unwrap()
                .str()
                .unwrap()
                .get(0),
            Some("NOT AVAILABLE")
        );
    }

    #[test]
    fn test_an_empty_archive_yields_an_empty_universe_with_the_same_schema() {
        let empty = universe_of(&[], &table(), None).expect("an empty universe must build");
        let populated = universe_of(
            &[partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))])],
            &table(),
            None,
        )
        .expect("the universe must build");

        assert_eq!(empty.rows().height(), 0);
        assert_eq!(empty.rows().schema(), populated.rows().schema());
    }

    /// A bar is classified by the observation current when it printed, never by a later one. This
    /// is what makes the universe look-ahead-free by construction rather than by a remembered rule.
    #[test]
    fn test_a_bar_takes_the_observation_current_when_it_printed() {
        let universe = universe_of(
            &[
                partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))]),
                partition((2022, 1, 3), &[("AAPL", "CS", Some("7372"))]),
            ],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("AAPL", instant(2021, 11, 15)),
                ("AAPL", instant(2022, 2, 10)),
            ]),
            &universe,
        )
        .expect("the join must run");

        let industries = joined.column("industry").unwrap().str().unwrap();
        assert_eq!(joined.height(), 2);
        assert_eq!(industries.get(0), Some("Computers"));
        assert_eq!(industries.get(1), Some("ComputerSoftware"));
    }

    /// The survivorship fix itself. A name that traded and then delisted keeps the sessions it
    /// traded for, where the retired CSV — taken in 2026 — dropped every one of its bars.
    ///
    /// Its absence from the later observation is not what removes it: a delisted name simply stops
    /// printing bars, and a classification carried forward onto no bars costs nothing.
    #[test]
    fn test_a_delisted_name_keeps_the_sessions_it_traded() {
        let universe = universe_of(
            &[
                partition(
                    (2021, 10, 1),
                    &[("AAPL", "CS", Some("3571")), ("TWTR", "CS", Some("7370"))],
                ),
                partition((2022, 1, 3), &[("AAPL", "CS", Some("3571"))]),
            ],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("TWTR", instant(2021, 11, 15)),
                ("AAPL", instant(2022, 2, 10)),
            ]),
            &universe,
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
    }

    /// The feed answers `404` for 7 to 18 symbols a quarter that traded, and a partition is written
    /// anyway. A name missing from one observation but still printing bars must keep its last known
    /// classification rather than vanish for the whole quarter.
    #[test]
    fn test_a_name_absent_from_one_observation_keeps_its_classification() {
        let universe = universe_of(
            &[
                partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))]),
                // AAPL traded through this quarter; the feed simply had no record for it that day.
                partition((2022, 1, 3), &[("MSFT", "CS", Some("7372"))]),
                partition((2022, 4, 1), &[("AAPL", "CS", Some("3571"))]),
            ],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(bars(&[("AAPL", instant(2022, 2, 10))]), &universe)
            .expect("the join must run");

        assert_eq!(joined.height(), 1, "a 404 is not a delisting");
        assert_eq!(
            joined.column("industry").unwrap().str().unwrap().get(0),
            Some("Computers")
        );
    }

    /// The bound on the carry. A symbol the feed has not seen for two consecutive observations
    /// while it keeps printing bars is not a transient `404`; on at least one measured ticker it is
    /// the same symbol reused by a different company, and carrying the old sector onto the new
    /// listing's bars would classify one company's history as another's.
    #[test]
    fn test_a_classification_is_not_carried_past_one_silent_observation() {
        let universe = universe_of(
            &[
                partition((2021, 10, 1), &[("ECHO", "CS", Some("4731"))]),
                partition((2022, 1, 3), &[("AAPL", "CS", Some("3571"))]),
                partition((2022, 4, 1), &[("AAPL", "CS", Some("3571"))]),
            ],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                // One observation of silence: still carried.
                ("ECHO", instant(2022, 2, 10)),
                // Two: the symbol is no longer answered for, and a guess is worse than nothing.
                ("ECHO", instant(2022, 5, 10)),
            ]),
            &universe,
        )
        .expect("the join must run");

        assert_eq!(joined.height(), 1);
        assert_eq!(
            joined.column("timestamp").unwrap().i64().unwrap().get(0),
            Some(instant(2022, 2, 10))
        );
    }

    /// The case that stops the carry-forward from being wrong. A name reclassified out of common
    /// stock is dropped from that observation on, because the newer observation supersedes rather
    /// than being invisible behind the common-stock filter.
    #[test]
    fn test_a_name_reclassified_out_of_common_stock_is_dropped_from_then_on() {
        let universe = universe_of(
            &[
                partition((2021, 10, 1), &[("XYZ", "CS", Some("6726"))]),
                partition((2022, 1, 3), &[("XYZ", "ETF", None)]),
            ],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("XYZ", instant(2021, 11, 15)),
                ("XYZ", instant(2022, 2, 10)),
            ]),
            &universe,
        )
        .expect("the join must run");

        assert_eq!(
            joined.height(),
            1,
            "only the session it was common stock for"
        );
        assert_eq!(
            joined.column("timestamp").unwrap().i64().unwrap().get(0),
            Some(instant(2021, 11, 15))
        );
    }

    #[test]
    fn test_a_bar_carries_the_share_count_current_when_it_printed() {
        let universe = universe_of(
            &[
                partition_with_shares(
                    (2021, 10, 1),
                    &[("AAPL", "CS", Some("3571"), Some(16.53e9))],
                ),
                partition_with_shares((2022, 1, 3), &[("AAPL", "CS", Some("3571"), Some(14.59e9))]),
            ],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("AAPL", instant(2021, 11, 15)),
                ("AAPL", instant(2022, 2, 15)),
            ]),
            &universe,
        )
        .expect("the join must run");

        let shares = joined.column("shares_outstanding").unwrap().f64().unwrap();
        // The point-in-time part of the size factor, and it is not cosmetic: the two readings differ
        // by 13%, so a bar priced against today's count is mis-sized by that much.
        assert_eq!(shares.get(0), Some(16.53e9));
        assert_eq!(shares.get(1), Some(14.59e9));
    }

    /// A share count the feed declines to report must cost the name its size, not its bars.
    ///
    /// The feed stops answering for 4% of common stock from 2025-07, against under 1% before, so a
    /// null here is a dated coverage change rather than a stray row.
    #[test]
    fn test_a_missing_share_count_keeps_the_bar_and_nulls_the_count() {
        let universe = universe_of(
            &[partition_with_shares(
                (2021, 10, 1),
                &[("AAPL", "CS", Some("3571"), None)],
            )],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(bars(&[("AAPL", instant(2021, 11, 15))]), &universe)
            .expect("the join must run");

        assert_eq!(joined.height(), 1);
        assert_eq!(
            joined
                .column("shares_outstanding")
                .unwrap()
                .f64()
                .unwrap()
                .get(0),
            None
        );
    }

    /// An infinity is not a share count either, and it reaches here the same way a zero does.
    ///
    /// Left admitted, it survives to the size factor and is booked as an unmeasured share count —
    /// a real cause attached to the wrong defect.
    #[test]
    fn test_a_non_finite_share_count_is_refused_at_the_read_boundary() {
        for count in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let stored = DataFrame::new(vec![
                Column::new("ticker".into(), vec!["AAPL"]),
                Column::new("as_of".into(), vec!["2021-10-01"]),
                Column::new("security_type".into(), vec![Some("CS")]),
                Column::new("sic_code".into(), vec![Some("3571")]),
                Column::new("sic_description".into(), vec![None::<&str>]),
                Column::new("shares_outstanding".into(), vec![Some(count)]),
                Column::new("reported_market_capitalization".into(), vec![None::<f64>]),
                Column::new("primary_exchange".into(), vec![None::<&str>]),
            ])
            .expect("the stored fixture must build");

            let universe = universe_of(
                &[(
                    SessionDate::from_date(
                        chrono::NaiveDate::from_ymd_opt(2021, 10, 1).expect("a valid date"),
                    ),
                    stored,
                )],
                &table(),
                None,
            )
            .expect("the universe must build");

            let joined = join_point_in_time(bars(&[("AAPL", instant(2021, 11, 15))]), &universe)
                .expect("the join must run");

            assert_eq!(
                joined
                    .column("shares_outstanding")
                    .unwrap()
                    .f64()
                    .unwrap()
                    .get(0),
                None,
                "{count} must not reach the size factor"
            );
        }
    }

    /// Zero shares is not a company with no equity; it is the feed answering with a placeholder.
    ///
    /// Built as a stored frame rather than through `EquityReference::new`, which refuses it at the
    /// write boundary. This guard is the read boundary, where the input is whatever parquet holds.
    #[test]
    fn test_a_non_positive_share_count_is_refused_rather_than_carried() {
        let stored = DataFrame::new(vec![
            Column::new("ticker".into(), vec!["AAPL"]),
            Column::new("as_of".into(), vec!["2021-10-01"]),
            Column::new("security_type".into(), vec![Some("CS")]),
            Column::new("sic_code".into(), vec![Some("3571")]),
            Column::new("sic_description".into(), vec![None::<&str>]),
            Column::new("shares_outstanding".into(), vec![Some(0.0_f64)]),
            Column::new("reported_market_capitalization".into(), vec![None::<f64>]),
            Column::new("primary_exchange".into(), vec![None::<&str>]),
        ])
        .expect("the stored fixture must build");

        let universe = universe_of(
            &[(
                SessionDate::from_date(
                    chrono::NaiveDate::from_ymd_opt(2021, 10, 1).expect("a valid date"),
                ),
                stored,
            )],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(bars(&[("AAPL", instant(2021, 11, 15))]), &universe)
            .expect("the join must run");

        assert_eq!(
            joined
                .column("shares_outstanding")
                .unwrap()
                .f64()
                .unwrap()
                .get(0),
            None
        );
    }

    /// A bar older than every observation cannot be classified, and the honest answer is to drop it
    /// rather than reach forward for the first observation that postdates it.
    #[test]
    fn test_a_bar_predating_every_observation_is_dropped() {
        let universe = universe_of(
            &[partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))])],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(bars(&[("AAPL", instant(2021, 9, 15))]), &universe)
            .expect("the join must run");

        assert_eq!(joined.height(), 0);
    }

    #[test]
    fn test_a_bar_for_a_name_outside_the_universe_is_dropped() {
        let universe = universe_of(
            &[partition((2021, 10, 1), &[("AAPL", "CS", Some("3571"))])],
            &table(),
            None,
        )
        .expect("the universe must build");

        let joined = join_point_in_time(
            bars(&[
                ("AAPL", instant(2021, 11, 1)),
                ("SPY", instant(2021, 11, 1)),
            ]),
            &universe,
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
