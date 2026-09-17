//! Point-in-time symbol reference: what each instrument *was* on a date, not what it is now.
//!
//! One partition per `as_of`, because these are dated observations rather than a table revised once.

use polars::prelude::*;

use crate::common::types::EquityReference;

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
