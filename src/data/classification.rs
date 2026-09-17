//! What a name's SIC code says it does, at two granularities.
//!
//! The lookup only; the buckets and ranges are generated into `classification_table.rs`.

use crate::common::types::SicCode;
use crate::data::classification_table::{
    Industry, Sector, SicRange, INDUSTRY_RANGES, SECTOR_RANGES,
};

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
pub fn sector_of(code: &SicCode) -> Sector {
    bucket_of(&SECTOR_RANGES, code, Sector::Other)
}

/// The industry `code` belongs to, at the finer granularity.
pub fn industry_of(code: &SicCode) -> Industry {
    bucket_of(&INDUSTRY_RANGES, code, Industry::Other)
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

    fn sic(code: &str) -> SicCode {
        SicCode::new(code).expect("the test code is four digits")
    }

    /// The four pairs the two-digit major group got wrong, in both directions.
    ///
    /// Two names it split that share a factor, and two it merged that do not. Pinned to literal
    /// buckets rather than to a lookup, so a table that moves fails here instead of agreeing with
    /// itself.
    #[test]
    fn test_the_major_group_pairs_resolve_correctly() {
        // Split by the major group -- computers is 35, prepackaged software is 73 -- and joined here.
        assert_eq!(sector_of(&sic("3571")), Sector::BusinessEquipment);
        assert_eq!(sector_of(&sic("7372")), Sector::BusinessEquipment);

        // Merged by the major group: construction machinery is also 35.
        assert_eq!(sector_of(&sic("3531")), Sector::Manufacturing);

        // Split by the major group -- pharmaceuticals is 28, medical instruments is 38.
        assert_eq!(sector_of(&sic("2834")), Sector::Healthcare);
        assert_eq!(sector_of(&sic("3841")), Sector::Healthcare);

        // Merged by the major group: industrial gases is also 28.
        assert_eq!(sector_of(&sic("2813")), Sector::Chemicals);
    }

    /// A code inside no published range lands in the catch-all rather than failing.
    #[test]
    fn test_an_unclaimed_code_falls_to_the_catch_all() {
        // Advertising agencies. The twelve-industry definition claims no range covering it.
        assert_eq!(sector_of(&sic("7311")), Sector::Other);
    }

    /// The finer granularity separates names the sector deliberately holds together.
    #[test]
    fn test_the_industry_is_finer_than_the_sector() {
        assert_eq!(sector_of(&sic("3571")), Sector::BusinessEquipment);
        assert_eq!(sector_of(&sic("7372")), Sector::BusinessEquipment);
        assert_ne!(industry_of(&sic("3571")), industry_of(&sic("7372")));
    }

    /// Every four-digit code resolves, so absence can only come from having no code at all.
    ///
    /// The whole domain rather than a sample: this is the property the `Option<Sector>` boundary
    /// rests on, and a gap anywhere in it would put a second meaning into `None`.
    #[test]
    fn test_every_four_digit_code_resolves() {
        for digits in 0..=9999u16 {
            let code = sic(&format!("{digits:04}"));
            // Both calls are the assertion: either would panic on a code it could not place.
            let _ = sector_of(&code);
            let _ = industry_of(&code);
        }
    }

    /// Both tables are ascending and disjoint, which is what makes the binary search sound.
    ///
    /// Asserted here as well as refused in the generator, because the search silently returns the
    /// wrong bucket rather than failing if this stops holding.
    #[test]
    fn test_the_ranges_are_ascending_and_disjoint() {
        assert_ascending_and_disjoint(&SECTOR_RANGES, "sector");
        assert_ascending_and_disjoint(&INDUSTRY_RANGES, "industry");
    }

    /// Every range's own bounds first, then every adjacent pair.
    ///
    /// Two loops rather than one: a pairwise walk never reaches the last range's own bounds, so a
    /// table whose final range ran backwards would pass a check that only looked at `windows(2)`.
    fn assert_ascending_and_disjoint<Bucket: Copy>(ranges: &[SicRange<Bucket>], label: &str) {
        for range in ranges {
            assert!(
                range.low <= range.high,
                "{label} range {}-{} runs backwards",
                range.low,
                range.high
            );
        }
        for window in ranges.windows(2) {
            assert!(
                window[0].high < window[1].low,
                "{label} ranges {}-{} and {}-{} overlap or descend",
                window[0].low,
                window[0].high,
                window[1].low,
                window[1].high
            );
        }
    }

    /// Every range's own endpoints resolve to the bucket the range declares.
    ///
    /// The search and the table agreeing, rather than the search agreeing with itself: a lookup
    /// that placed a boundary code one bucket over would pass every example test above.
    #[test]
    fn test_each_range_resolves_to_the_bucket_it_declares() {
        for range in SECTOR_RANGES {
            for digits in [range.low, range.high] {
                assert_eq!(sector_of(&sic(&format!("{digits:04}"))), range.bucket);
            }
        }
        for range in INDUSTRY_RANGES {
            for digits in [range.low, range.high] {
                assert_eq!(industry_of(&sic(&format!("{digits:04}"))), range.bucket);
            }
        }
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

    /// The round trip survives the case `clean_data` writes.
    ///
    /// `models::tide::data::clean_data` uppercases the sector and industry columns, and the
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
}
