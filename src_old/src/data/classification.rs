//! What a name's SIC code says it does, at two granularities.
//!
//! The lookup and the loaded ranges; the bucket names are the vocabulary in
//! `classification_table.rs`.

use crate::common::types::SicCode;
use crate::data::classification_table::{Industry, Sector, SicRange};

/// Why a published mapping was refused.
///
/// Each variant carries the values that produced the refusal, because a mapping is rejected while
/// nobody is watching and the message is the whole record of what was wrong with it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClassificationError {
    #[error("{granularity} range {low}-{high} names bucket \"{bucket}\", which is not one this build knows")]
    UnknownBucket {
        granularity: &'static str,
        low: u16,
        high: u16,
        bucket: String,
    },
    #[error("{granularity} range {low}-{high} is inverted")]
    InvertedRange {
        granularity: &'static str,
        low: u16,
        high: u16,
    },
    #[error(
        "{granularity} ranges {previous_low}-{previous_high} and {low}-{high} overlap or descend"
    )]
    Disordered {
        granularity: &'static str,
        previous_low: u16,
        previous_high: u16,
        low: u16,
        high: u16,
    },
    #[error("the mapping holds no {granularity} ranges, which would classify every name as the fallback")]
    Empty { granularity: &'static str },
    #[error("{granularity} range {low}-{high} runs past 9999, which is not a SIC code")]
    OutsideTheSicDomain {
        granularity: &'static str,
        low: u16,
        high: u16,
    },
}

/// The SIC-to-bucket mapping, loaded rather than compiled in.
///
/// A value in scope is proof the ranges are disjoint and ascending within each granularity, which
/// is the invariant [`bucket_of`]'s binary search needs and which nothing downstream re-checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassificationTable {
    as_of: chrono::NaiveDate,
    sector_ranges: Vec<SicRange<Sector>>,
    industry_ranges: Vec<SicRange<Industry>>,
}

impl ClassificationTable {
    /// Builds a table from published rows, refusing anything the lookup could not answer soundly.
    pub fn new(
        as_of: chrono::NaiveDate,
        mut sector_ranges: Vec<SicRange<Sector>>,
        mut industry_ranges: Vec<SicRange<Industry>>,
    ) -> Result<Self, ClassificationError> {
        // Sorted here rather than demanded of the caller: the published order is the source's, and
        // the invariant the lookup needs is that the runs do not overlap, which sorting cannot fake.
        sector_ranges.sort_by_key(|range| (range.low, range.high));
        industry_ranges.sort_by_key(|range| (range.low, range.high));
        check_ordering("sector", &sector_ranges)?;
        check_ordering("industry", &industry_ranges)?;
        Ok(Self {
            as_of,
            sector_ranges,
            industry_ranges,
        })
    }

    /// The date the published definitions this table holds were read.
    pub fn as_of(&self) -> chrono::NaiveDate {
        self.as_of
    }

    /// The runs this table holds, so a test can walk them without a second copy to disagree with.
    #[cfg(test)]
    fn ranges(&self) -> (&[SicRange<Sector>], &[SicRange<Industry>]) {
        (&self.sector_ranges, &self.industry_ranges)
    }
}

/// The largest value a four-digit `SicCode` can hold, and so the largest legal range bound.
const MAXIMUM_SIC_CODE: u16 = 9999;

/// Column values of the `granularity` column, which is what splits one published table in two.
const SECTOR: &str = "sector";
const INDUSTRY: &str = "industry";

impl ClassificationTable {
    /// The published rows, in the shape the dataset stores.
    ///
    /// Both granularities in one frame: they share a schema and are republished together, and two
    /// objects that must agree eventually will not.
    pub fn to_dataframe(&self) -> Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
        use polars::prelude::*;

        let rows = self.sector_ranges.len() + self.industry_ranges.len();
        let mut granularities: Vec<&str> = Vec::with_capacity(rows);
        // u32 rather than u16: polars has no native u16 series, and the cast back on read is what
        // re-establishes the domain width.
        let mut lows: Vec<u32> = Vec::with_capacity(rows);
        let mut highs: Vec<u32> = Vec::with_capacity(rows);
        let mut buckets: Vec<&str> = Vec::with_capacity(rows);

        for range in &self.sector_ranges {
            granularities.push(SECTOR);
            lows.push(u32::from(range.low));
            highs.push(u32::from(range.high));
            buckets.push(range.bucket.as_str());
        }
        for range in &self.industry_ranges {
            granularities.push(INDUSTRY);
            lows.push(u32::from(range.low));
            highs.push(u32::from(range.high));
            buckets.push(range.bucket.as_str());
        }

        DataFrame::new(vec![
            Column::new("granularity".into(), granularities),
            Column::new("low".into(), lows),
            Column::new("high".into(), highs),
            Column::new("bucket".into(), buckets),
        ])
    }

    /// Reads a published frame back, refusing a bucket name this build does not know.
    ///
    /// An unknown name is refused rather than folded into the fallback, which is a real group: a
    /// silent fold would put a name into a factor it has no claim to and report nothing.
    pub fn from_dataframe(
        as_of: chrono::NaiveDate,
        frame: &polars::prelude::DataFrame,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        use polars::prelude::*;

        let granularities = frame.column("granularity")?.str()?;
        // Read at the stored width and narrowed below: polars cannot hold a u16 series at all, so
        // the domain width is re-established here rather than by the cast.
        let lows = frame.column("low")?.cast(&DataType::UInt32)?;
        let lows = lows.u32()?;
        let highs = frame.column("high")?.cast(&DataType::UInt32)?;
        let highs = highs.u32()?;
        let buckets = frame.column("bucket")?.str()?;

        let mut sector_ranges: Vec<SicRange<Sector>> = Vec::new();
        let mut industry_ranges: Vec<SicRange<Industry>> = Vec::new();

        for index in 0..frame.height() {
            let (Some(granularity), Some(low), Some(high), Some(bucket)) = (
                granularities.get(index),
                lows.get(index),
                highs.get(index),
                buckets.get(index),
            ) else {
                return Err(format!("row {index} of the mapping has a null column").into());
            };
            // A SIC code is four digits, so a bound past u16 is not a wide code but a corrupt row.
            let (Ok(low), Ok(high)) = (u16::try_from(low), u16::try_from(high)) else {
                return Err(
                    format!("row {index} has bounds {low}-{high}, outside a SIC code").into(),
                );
            };
            match granularity {
                SECTOR => sector_ranges.push(SicRange {
                    low,
                    high,
                    bucket: sector_from_code(bucket).ok_or(ClassificationError::UnknownBucket {
                        granularity: SECTOR,
                        low,
                        high,
                        bucket: bucket.to_string(),
                    })?,
                }),
                INDUSTRY => industry_ranges.push(SicRange {
                    low,
                    high,
                    bucket: industry_from_code(bucket).ok_or(
                        ClassificationError::UnknownBucket {
                            granularity: INDUSTRY,
                            low,
                            high,
                            bucket: bucket.to_string(),
                        },
                    )?,
                }),
                other => {
                    return Err(format!("row {index} names granularity \"{other}\"").into());
                }
            }
        }

        Ok(Self::new(as_of, sector_ranges, industry_ranges)?)
    }
}

/// Refuses a run set the binary search could answer wrongly.
///
/// Emptiness is a refusal of its own: zero ranges would classify every name as the fallback and
/// report nothing, which reads exactly like a market where nothing is classifiable.
fn check_ordering<Bucket: Copy>(
    granularity: &'static str,
    ranges: &[SicRange<Bucket>],
) -> Result<(), ClassificationError> {
    if ranges.is_empty() {
        return Err(ClassificationError::Empty { granularity });
    }
    for range in ranges {
        if range.low > range.high {
            return Err(ClassificationError::InvertedRange {
                granularity,
                low: range.low,
                high: range.high,
            });
        }
        // A SicCode is exactly four digits, so the bound is <= 9999 and not merely <= u16::MAX.
        // 8000-60000 would otherwise pass every other check and swallow every code from 8000 up.
        if range.high > MAXIMUM_SIC_CODE {
            return Err(ClassificationError::OutsideTheSicDomain {
                granularity,
                low: range.low,
                high: range.high,
            });
        }
    }
    for window in ranges.windows(2) {
        let (previous, next) = (&window[0], &window[1]);
        if previous.high >= next.low {
            return Err(ClassificationError::Disordered {
                granularity,
                previous_low: previous.low,
                previous_high: previous.high,
                low: next.low,
                high: next.high,
            });
        }
    }
    Ok(())
}

/// The bucket `code` falls in, or the source's catch-all when it claims no range.
///
/// A binary search rather than a scan, which the generator's refusal to emit an overlapping or
/// descending range is what makes sound: the runs are disjoint and ascending, so at most one can
/// contain a code and the first hit is the only hit.
fn bucket_of<Bucket: Copy>(
    ranges: &[SicRange<Bucket>],
    code: &SicCode,
    fallback: Bucket,
) -> Bucket {
    // Parsed rather than compared as text, because "0100" sorts before "99" while 100 does not. The
    // unwrap cannot fire: `SicCode` is exactly four ASCII digits, which is always a `u16`.
    let digits: u16 = code
        .as_str()
        .parse()
        .expect("a SicCode is four ASCII digits, which always parses as u16");

    ranges
        .binary_search_by(|range| {
            if range.high < digits {
                std::cmp::Ordering::Less
            } else if range.low > digits {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .map_or(fallback, |index| ranges[index].bucket)
}

/// The sector `code` belongs to.
///
/// Total: every four-digit code lands somewhere, because the source's last bucket claims whatever
/// the others do not. A name with no sector is one with no `SicCode` at all, which is an `Option`
/// this function never sees.
pub fn sector_of(table: &ClassificationTable, code: &SicCode) -> Sector {
    bucket_of(&table.sector_ranges, code, Sector::Other)
}

/// The industry `code` belongs to, at the finer granularity.
pub fn industry_of(table: &ClassificationTable, code: &SicCode) -> Industry {
    bucket_of(&table.industry_ranges, code, Industry::Other)
}

/// Reads a stored sector back. `None` when the text names no bucket.
///
/// Unknown text is refused rather than folded into [`Sector::Other`], which is a real group that
/// would hide a moved table behind a bucket sharing a factor. Case-insensitive because the
/// laboratory reads these back downstream of `clean_data`, which uppercases the column.
pub fn sector_from_code(code: &str) -> Option<Sector> {
    Sector::ALL
        .into_iter()
        .find(|sector| sector.as_str().eq_ignore_ascii_case(code))
}

/// Reads a stored industry back. `None` when the text names no bucket.
pub fn industry_from_code(code: &str) -> Option<Industry> {
    Industry::ALL
        .into_iter()
        .find(|industry| industry.as_str().eq_ignore_ascii_case(code))
}

impl std::fmt::Display for Sector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::fmt::Display for Industry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published runs the examples below exercise, copied from the dataset.
    ///
    /// A fixture, and deliberately a partial one: the runs are data now, so a test that carried all
    /// 647 of them would be re-baking the table it just removed. `--check` in
    /// `tools/fetch-industry-classifications` is what holds the real mapping to the source.
    fn fixture() -> ClassificationTable {
        ClassificationTable::new(
            chrono::NaiveDate::from_ymd_opt(2026, 9, 22).expect("a real date"),
            vec![
                SicRange {
                    low: 2800,
                    high: 2829,
                    bucket: Sector::Chemicals,
                },
                SicRange {
                    low: 2830,
                    high: 2839,
                    bucket: Sector::Healthcare,
                },
                SicRange {
                    low: 3200,
                    high: 3569,
                    bucket: Sector::Manufacturing,
                },
                SicRange {
                    low: 3570,
                    high: 3579,
                    bucket: Sector::BusinessEquipment,
                },
                SicRange {
                    low: 3840,
                    high: 3859,
                    bucket: Sector::Healthcare,
                },
                SicRange {
                    low: 7370,
                    high: 7379,
                    bucket: Sector::BusinessEquipment,
                },
            ],
            vec![
                SicRange {
                    low: 2810,
                    high: 2819,
                    bucket: Industry::Chemicals,
                },
                SicRange {
                    low: 2834,
                    high: 2834,
                    bucket: Industry::PharmaceuticalProducts,
                },
                SicRange {
                    low: 3531,
                    high: 3531,
                    bucket: Industry::Machinery,
                },
                SicRange {
                    low: 3570,
                    high: 3579,
                    bucket: Industry::Computers,
                },
                SicRange {
                    low: 3840,
                    high: 3849,
                    bucket: Industry::MedicalEquipment,
                },
                SicRange {
                    low: 7310,
                    high: 7319,
                    bucket: Industry::BusinessServices,
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

    fn sic(code: &str) -> SicCode {
        SicCode::new(code).expect("the test code is four digits")
    }

    /// The four pairs the two-digit major group got wrong, in both directions.
    ///
    /// Two names it split that share a factor, and two it merged that do not. This is why the
    /// system reads Fama-French rather than the major group, which is worth keeping stated even
    /// though the runs themselves now live in the dataset.
    #[test]
    fn test_the_major_group_pairs_resolve_correctly() {
        let table = fixture();
        // Split by the major group -- computers is 35, prepackaged software is 73 -- joined here.
        assert_eq!(sector_of(&table, &sic("3571")), Sector::BusinessEquipment);
        assert_eq!(sector_of(&table, &sic("7372")), Sector::BusinessEquipment);
        // Merged by the major group: construction machinery is also 35.
        assert_eq!(sector_of(&table, &sic("3531")), Sector::Manufacturing);
        // Split by the major group -- pharmaceuticals is 28, medical instruments is 38.
        assert_eq!(sector_of(&table, &sic("2834")), Sector::Healthcare);
        assert_eq!(sector_of(&table, &sic("3841")), Sector::Healthcare);
        // Merged by the major group: industrial gases is also 28.
        assert_eq!(sector_of(&table, &sic("2813")), Sector::Chemicals);
    }

    /// A code inside no published range lands in the catch-all rather than failing.
    #[test]
    fn test_an_unclaimed_code_falls_to_the_catch_all() {
        // Advertising agencies. The twelve-industry definition claims no range covering it, which
        // is true of the real dataset too: 7311 has an industry run and no sector run.
        assert_eq!(sector_of(&fixture(), &sic("7311")), Sector::Other);
        assert_eq!(
            industry_of(&fixture(), &sic("7311")),
            Industry::BusinessServices
        );
    }

    /// A bound past 9999 is not a wide code but a corrupt row, and it would silently claim every
    /// code above its low bound.
    #[test]
    fn test_a_bound_outside_the_sic_domain_is_refused() {
        let error = ClassificationTable::new(
            date(),
            vec![SicRange {
                low: 8000,
                high: 60000,
                bucket: Sector::Finance,
            }],
            vec![SicRange {
                low: 100,
                high: 999,
                bucket: Industry::Agriculture,
            }],
        )
        .expect_err("a bound past 9999 must be refused");

        assert!(
            matches!(
                error,
                ClassificationError::OutsideTheSicDomain { high: 60000, .. }
            ),
            "got {error}"
        );
    }

    /// The finer granularity separates names the sector deliberately holds together.
    #[test]
    fn test_the_industry_is_finer_than_the_sector() {
        let table = fixture();
        assert_eq!(sector_of(&table, &sic("3571")), Sector::BusinessEquipment);
        assert_eq!(sector_of(&table, &sic("7372")), Sector::BusinessEquipment);
        assert_ne!(
            industry_of(&table, &sic("3571")),
            industry_of(&table, &sic("7372"))
        );
    }

    /// Every four-digit code resolves, so absence can only come from having no code at all.
    ///
    /// The whole domain rather than a sample: this is the property the `Option<Sector>` boundary
    /// rests on, and a gap anywhere in it would put a second meaning into `None`.
    #[test]
    fn test_every_four_digit_code_resolves() {
        let table = fixture();
        let mut resolved = 0_u32;
        for digits in 0..=9999u16 {
            let code = sic(&format!("{digits:04}"));
            // Both calls are the assertion: either would panic on a code it could not place.
            let _ = sector_of(&table, &code);
            let _ = industry_of(&table, &code);
            resolved += 1;
        }
        assert_eq!(resolved, 10_000, "every four-digit code must be exercised");
    }

    /// Every range's own endpoints resolve to the bucket the range declares.
    ///
    /// The search and the table agreeing, rather than the search agreeing with itself: a lookup
    /// that placed a boundary code one bucket over would pass every example test above.
    #[test]
    fn test_each_range_resolves_to_the_bucket_it_declares() {
        let table = fixture();
        let (sector_ranges, industry_ranges) = table.ranges();
        let mut checked = 0_u32;
        for range in sector_ranges {
            for digits in [range.low, range.high] {
                assert_eq!(
                    sector_of(&table, &sic(&format!("{digits:04}"))),
                    range.bucket
                );
                checked += 1;
            }
        }
        for range in industry_ranges {
            for digits in [range.low, range.high] {
                assert_eq!(
                    industry_of(&table, &sic(&format!("{digits:04}"))),
                    range.bucket
                );
                checked += 1;
            }
        }
        // Both endpoints of all thirteen fixture runs; a loop over an empty table asserts nothing.
        assert_eq!(checked, 26, "every run's endpoints must be checked");
    }

    /// The stored form round-trips, so a written row reads back as the bucket that wrote it.
    #[test]
    fn test_the_stored_form_round_trips() {
        for sector in Sector::ALL {
            assert_eq!(sector_from_code(sector.as_str()), Some(sector));
        }
        for industry in Industry::ALL {
            assert_eq!(industry_from_code(industry.as_str()), Some(industry));
        }
    }

    /// The round trip survives the case `clean_frame` writes.
    ///
    /// `laboratory::frame::clean_frame` uppercases the sector and industry columns, and the
    /// laboratory reads them back on the far side of it, so an exact-match decoder would refuse
    /// every classified row and measure nothing.
    #[test]
    fn test_the_stored_form_round_trips_through_an_uppercased_column() {
        for sector in Sector::ALL {
            assert_eq!(
                sector_from_code(&sector.as_str().to_uppercase()),
                Some(sector)
            );
        }
        for industry in Industry::ALL {
            assert_eq!(
                industry_from_code(&industry.as_str().to_uppercase()),
                Some(industry)
            );
        }
    }

    /// Text naming no bucket is refused rather than answered with the catch-all.
    #[test]
    fn test_unknown_stored_text_is_refused() {
        // The value the retired schema wrote for a name the feed declined to classify.
        assert_eq!(sector_from_code("NOT AVAILABLE"), None);
        // A two-digit major group, which is what this table replaces.
        assert_eq!(sector_from_code("35"), None);
        // The source's own short code. Spelled out in the stored form, so the abbreviation names
        // no bucket and a row carrying one is drift rather than a sector.
        assert_eq!(sector_from_code("BusEq"), None);
        assert_eq!(industry_from_code("Hardw"), None);
        assert_eq!(industry_from_code(""), None);
    }

    /// Encoding a table and reading it back changes no name's bucket.
    ///
    /// Every four-digit code, both granularities, rather than a sample: this is what lets the
    /// mapping live in S3 at all, and 10,000 lookups is cheap enough that sampling would be a
    /// choice to know less. The migration's own equivalence — that the published dataset is
    /// row-identical to the table it replaced — was measured once against the real object and
    /// recorded in the pull request, because the compiled ranges no longer exist to compare to.
    #[test]
    fn test_every_sic_code_classifies_identically_after_a_round_trip() {
        let original = fixture();
        let frame = original.to_dataframe().expect("the table encodes");
        let restored = ClassificationTable::from_dataframe(original.as_of(), &frame)
            .expect("the encoded table decodes");

        assert_eq!(restored, original, "the round trip must be the identity");

        let mut compared = 0_u32;
        for code in 0..10_000_u32 {
            let text = format!("{code:04}");
            let sic = SicCode::new(&text).expect("four digits is a SicCode");
            assert_eq!(
                sector_of(&restored, &sic),
                sector_of(&original, &sic),
                "sector for {text}"
            );
            assert_eq!(
                industry_of(&restored, &sic),
                industry_of(&original, &sic),
                "industry for {text}"
            );
            compared += 1;
        }
        // The population beside the result: a loop that ran zero times asserts nothing and passes.
        assert_eq!(compared, 10_000, "every four-digit code must be compared");
    }

    fn range<Bucket>(low: u16, high: u16, bucket: Bucket) -> SicRange<Bucket> {
        SicRange { low, high, bucket }
    }

    /// Overlapping runs break the binary search's premise that the first hit is the only hit.
    #[test]
    fn test_overlapping_ranges_are_refused() {
        let error = ClassificationTable::new(
            date(),
            vec![
                range(100, 999, Sector::ConsumerNondurables),
                range(500, 1500, Sector::Manufacturing),
            ],
            vec![range(100, 999, Industry::Agriculture)],
        )
        .expect_err("an overlap must be refused");

        assert!(
            matches!(error, ClassificationError::Disordered { .. }),
            "got {error}"
        );
    }

    /// A run whose high is below its low matches nothing and would silently shrink the table.
    #[test]
    fn test_inverted_ranges_are_refused() {
        let error = ClassificationTable::new(
            date(),
            vec![range(999, 100, Sector::ConsumerNondurables)],
            vec![range(100, 999, Industry::Agriculture)],
        )
        .expect_err("an inverted range must be refused");

        assert!(
            matches!(error, ClassificationError::InvertedRange { .. }),
            "got {error}"
        );
    }

    /// The failure with no visible symptom: zero ranges answers every code with the fallback, which
    /// reads exactly like a market in which nothing is classifiable.
    #[test]
    fn test_an_empty_granularity_is_refused() {
        let error = ClassificationTable::new(
            date(),
            vec![range(100, 999, Sector::ConsumerNondurables)],
            Vec::new(),
        )
        .expect_err("an empty granularity must be refused");

        assert!(
            matches!(
                error,
                ClassificationError::Empty {
                    granularity: "industry"
                }
            ),
            "got {error}"
        );
    }

    /// A bucket this build cannot name is drift, and folding it into the catch-all would put the
    /// name into a factor it has no claim to.
    #[test]
    fn test_a_bucket_name_this_build_does_not_know_is_refused() {
        use polars::prelude::*;

        let frame = DataFrame::new(vec![
            Column::new("granularity".into(), vec!["sector", "industry"]),
            Column::new("low".into(), vec![100_u32, 100]),
            Column::new("high".into(), vec![999_u32, 999]),
            Column::new("bucket".into(), vec!["CryptoMining", "Agriculture"]),
        ])
        .expect("the fixture frame builds");

        let error = ClassificationTable::from_dataframe(date(), &frame)
            .expect_err("an unknown bucket must be refused");

        assert!(
            error.to_string().contains("CryptoMining"),
            "the refusal must name the bucket it could not place: {error}"
        );
    }

    fn date() -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 9, 17).expect("a real date")
    }
}
