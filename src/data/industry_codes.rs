//! The SEC's industry code for each filer the archive holds no Massive code for.
//!
//! A fallback beneath Massive's point-in-time code, keyed by CIK and applied when the universe is read.

use std::collections::BTreeMap;

use polars::prelude::*;

use crate::common::types::{Cik, SicCode};

/// Why a published industry-codes table was refused.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum IndustryCodesError {
    /// Refused rather than published: a fetch that found nothing is an outage, and an empty table
    /// would read as every filer having lost its code.
    #[error("the table holds no rows")]
    Empty,
    #[error("CIK {cik} appears more than once")]
    Duplicated { cik: String },
    #[error("row {row} has an unusable CIK {cik:?}")]
    UnusableCik { row: usize, cik: Option<String> },
    #[error("CIK {cik} has an unusable SIC code {code:?}")]
    UnusableCode { cik: String, code: Option<String> },
}

/// One filer's code, as EDGAR reported it when the table was published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndustryCode {
    pub cik: Cik,
    pub sic_code: SicCode,
    pub sic_description: Option<String>,
}

/// Every filer's current code, published as one `as_of` partition when any row changes.
///
/// Current rather than point-in-time, because EDGAR keeps only today's code. That is why it sits
/// beneath Massive's rather than beside it: a code a 2021 observation reported wins over one read
/// now, and this fills only what that observation left empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndustryCodesTable {
    as_of: chrono::NaiveDate,
    codes: BTreeMap<Cik, IndustryCode>,
}

impl IndustryCodesTable {
    /// Validates the rows, refusing an empty table and a CIK that appears twice.
    pub fn new(
        as_of: chrono::NaiveDate,
        rows: Vec<IndustryCode>,
    ) -> Result<Self, IndustryCodesError> {
        if rows.is_empty() {
            return Err(IndustryCodesError::Empty);
        }
        let mut codes = BTreeMap::new();
        for row in rows {
            let cik = row.cik.clone();
            if codes.insert(cik.clone(), row).is_some() {
                return Err(IndustryCodesError::Duplicated {
                    cik: cik.as_str().to_string(),
                });
            }
        }
        Ok(Self { as_of, codes })
    }

    pub fn as_of(&self) -> chrono::NaiveDate {
        self.as_of
    }

    pub fn len(&self) -> usize {
        self.codes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    /// The code EDGAR reports for `cik`, or `None` when the table does not hold it.
    pub fn code_of(&self, cik: &Cik) -> Option<&SicCode> {
        self.codes.get(cik).map(|row| &row.sic_code)
    }

    /// Whether `other` holds exactly these rows, which is what decides a new `as_of` is owed.
    ///
    /// The date is excluded on purpose: two tables fetched on different days with the same rows are
    /// one table, and publishing the second would make `as_of` mean "when we looked".
    pub fn same_rows_as(&self, other: &Self) -> bool {
        self.codes == other.codes
    }

    /// The published shape, sorted by CIK so two publishes of the same rows are row-identical.
    pub fn to_dataframe(&self) -> Result<DataFrame, PolarsError> {
        let rows: Vec<&IndustryCode> = self.codes.values().collect();
        DataFrame::new(vec![
            Column::new(
                "cik".into(),
                rows.iter()
                    .map(|row| row.cik.as_str().to_string())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "sic_code".into(),
                rows.iter()
                    .map(|row| row.sic_code.as_str().to_string())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "sic_description".into(),
                rows.iter()
                    .map(|row| row.sic_description.clone())
                    .collect::<Vec<_>>(),
            ),
        ])
    }

    /// Reads a published partition back, refusing any row that would not have been published.
    pub fn from_dataframe(
        as_of: chrono::NaiveDate,
        frame: &DataFrame,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let ciks = frame.column("cik")?.str()?;
        let codes = frame.column("sic_code")?.str()?;
        let descriptions = frame.column("sic_description")?.str()?;

        let mut rows = Vec::with_capacity(frame.height());
        for row in 0..frame.height() {
            let cik = ciks.get(row).and_then(Cik::new).ok_or_else(|| {
                IndustryCodesError::UnusableCik {
                    row,
                    cik: ciks.get(row).map(str::to_string),
                }
            })?;
            let sic_code = codes.get(row).and_then(SicCode::new).ok_or_else(|| {
                IndustryCodesError::UnusableCode {
                    cik: cik.as_str().to_string(),
                    code: codes.get(row).map(str::to_string),
                }
            })?;
            rows.push(IndustryCode {
                cik,
                sic_code,
                sic_description: descriptions.get(row).map(str::to_string),
            });
        }
        Ok(Self::new(as_of, rows)?)
    }
}

#[cfg(test)]
pub(crate) mod fixture {
    use super::*;

    pub(crate) fn code(cik: &str, sic: &str) -> IndustryCode {
        IndustryCode {
            cik: Cik::new(cik).expect("a usable CIK"),
            sic_code: SicCode::new(sic).expect("a usable SIC code"),
            sic_description: None,
        }
    }

    pub(crate) fn table(rows: Vec<IndustryCode>) -> IndustryCodesTable {
        IndustryCodesTable::new(
            chrono::NaiveDate::from_ymd_opt(2026, 9, 24).expect("a real date"),
            rows,
        )
        .expect("the fixture must be a usable table")
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::*;
    use super::*;

    #[test]
    fn test_a_table_round_trips_through_its_published_shape() {
        let original = table(vec![code("901832", "2834"), code("1000275", "6029")]);

        let frame = original.to_dataframe().unwrap();
        let read = IndustryCodesTable::from_dataframe(original.as_of(), &frame).unwrap();

        assert_eq!(read, original);
        assert_eq!(
            read.code_of(&Cik::new("0000901832").unwrap())
                .map(SicCode::as_str),
            Some("2834"),
            "a CIK finds its code however many leading zeros it was written with"
        );
    }

    #[test]
    fn test_a_repeated_cik_and_an_empty_table_are_refused() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 9, 24).unwrap();

        assert_eq!(
            IndustryCodesTable::new(date, vec![code("1", "2834"), code("0000000001", "6029")]),
            Err(IndustryCodesError::Duplicated {
                cik: "0000000001".to_string()
            })
        );
        assert_eq!(
            IndustryCodesTable::new(date, Vec::new()),
            Err(IndustryCodesError::Empty)
        );
    }

    /// The comparison that decides whether a publish is owed, so it must ignore the date and see a
    /// single changed code.
    #[test]
    fn test_same_rows_ignores_the_date_and_sees_one_changed_code() {
        let today = table(vec![code("901832", "2834")]);
        let tomorrow = IndustryCodesTable::new(
            chrono::NaiveDate::from_ymd_opt(2026, 9, 25).unwrap(),
            vec![code("901832", "2834")],
        )
        .unwrap();
        let moved = table(vec![code("901832", "2836")]);

        assert!(today.same_rows_as(&tomorrow));
        assert!(!today.same_rows_as(&moved));
    }
}
