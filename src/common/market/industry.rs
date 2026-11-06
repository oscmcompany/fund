//! The twelve Fama-French industries, which group the four-digit industry codes into buckets coarse enough to average
//! over.

use super::security_details::IndustryCode;

/// One of the twelve Fama-French industries.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Industry {
    ConsumerNondurables,
    ConsumerDurables,
    Manufacturing,
    Energy,
    Chemicals,
    BusinessEquipment,
    Telecommunications,
    Utilities,
    WholesaleAndRetail,
    Healthcare,
    Finance,
    Other,
}

/// Inclusive code ranges from Ken French's `Siccodes12` definitions; a code in none of them is `Other`.
const RANGES: [(u16, u16, Industry); 49] = [
    (100, 999, Industry::ConsumerNondurables),
    (2000, 2399, Industry::ConsumerNondurables),
    (2700, 2749, Industry::ConsumerNondurables),
    (2770, 2799, Industry::ConsumerNondurables),
    (3100, 3199, Industry::ConsumerNondurables),
    (3940, 3989, Industry::ConsumerNondurables),
    (2500, 2519, Industry::ConsumerDurables),
    (2590, 2599, Industry::ConsumerDurables),
    (3630, 3659, Industry::ConsumerDurables),
    (3710, 3711, Industry::ConsumerDurables),
    (3714, 3714, Industry::ConsumerDurables),
    (3716, 3716, Industry::ConsumerDurables),
    (3750, 3751, Industry::ConsumerDurables),
    (3792, 3792, Industry::ConsumerDurables),
    (3900, 3939, Industry::ConsumerDurables),
    (3990, 3999, Industry::ConsumerDurables),
    (2520, 2589, Industry::Manufacturing),
    (2600, 2699, Industry::Manufacturing),
    (2750, 2769, Industry::Manufacturing),
    (3000, 3099, Industry::Manufacturing),
    (3200, 3569, Industry::Manufacturing),
    (3580, 3629, Industry::Manufacturing),
    (3700, 3709, Industry::Manufacturing),
    (3712, 3713, Industry::Manufacturing),
    (3715, 3715, Industry::Manufacturing),
    (3717, 3749, Industry::Manufacturing),
    (3752, 3791, Industry::Manufacturing),
    (3793, 3799, Industry::Manufacturing),
    (3830, 3839, Industry::Manufacturing),
    (3860, 3899, Industry::Manufacturing),
    (1200, 1399, Industry::Energy),
    (2900, 2999, Industry::Energy),
    (2800, 2829, Industry::Chemicals),
    (2840, 2899, Industry::Chemicals),
    (3570, 3579, Industry::BusinessEquipment),
    (3660, 3692, Industry::BusinessEquipment),
    (3694, 3699, Industry::BusinessEquipment),
    (3810, 3829, Industry::BusinessEquipment),
    (7370, 7379, Industry::BusinessEquipment),
    (4800, 4899, Industry::Telecommunications),
    (4900, 4949, Industry::Utilities),
    (5000, 5999, Industry::WholesaleAndRetail),
    (7200, 7299, Industry::WholesaleAndRetail),
    (7600, 7699, Industry::WholesaleAndRetail),
    (2830, 2839, Industry::Healthcare),
    (3693, 3693, Industry::Healthcare),
    (3840, 3859, Industry::Healthcare),
    (8000, 8099, Industry::Healthcare),
    (6000, 6999, Industry::Finance),
];

impl Industry {
    /// The industry a code falls in.
    pub fn of(code: IndustryCode) -> Self {
        RANGES
            .iter()
            .find(|(first, last, _)| (*first..=*last).contains(&code.code()))
            .map_or(Self::Other, |(_, _, industry)| *industry)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use strum::IntoEnumIterator;

    use super::*;

    fn industry(code: u16) -> Industry {
        Industry::of(IndustryCode::from_code(code).unwrap())
    }

    #[test]
    fn codes_land_in_their_published_industries() {
        assert_eq!(industry(6029), Industry::Finance);
        assert_eq!(industry(2834), Industry::Healthcare);
        assert_eq!(industry(4832), Industry::Telecommunications);
        assert_eq!(industry(3693), Industry::Healthcare);
        assert_eq!(industry(3692), Industry::BusinessEquipment);
        assert_eq!(industry(3694), Industry::BusinessEquipment);
        assert_eq!(industry(3711), Industry::ConsumerDurables);
        assert_eq!(industry(3713), Industry::Manufacturing);
        assert_eq!(industry(100), Industry::ConsumerNondurables);
        assert_eq!(industry(99), Industry::Other);
        assert_eq!(industry(1000), Industry::Other);
        assert_eq!(industry(9999), Industry::Other);
    }

    #[test]
    fn every_code_falls_in_at_most_one_range() {
        let overlaps: Vec<u16> = (0..=9_999)
            .filter(|code| {
                RANGES
                    .iter()
                    .filter(|(first, last, _)| (*first..=*last).contains(code))
                    .count()
                    > 1
            })
            .collect();
        assert_eq!(overlaps, Vec::<u16>::new());
        assert!(RANGES.iter().all(|(first, last, _)| first <= last));
    }

    #[test]
    fn industries_cover_their_published_widths() {
        let mut widths: BTreeMap<Industry, u32> = BTreeMap::new();
        for code in 0..=9_999 {
            *widths.entry(industry(code)).or_default() += 1;
        }
        assert_eq!(
            widths,
            BTreeMap::from([
                (Industry::ConsumerNondurables, 1_530),
                (Industry::ConsumerDurables, 117),
                (Industry::Manufacturing, 853),
                (Industry::Energy, 300),
                (Industry::Chemicals, 90),
                (Industry::BusinessEquipment, 79),
                (Industry::Telecommunications, 100),
                (Industry::Utilities, 50),
                (Industry::WholesaleAndRetail, 1_200),
                (Industry::Healthcare, 131),
                (Industry::Finance, 1_000),
                (Industry::Other, 4_550),
            ])
        );
    }

    #[test]
    fn names_round_trip() {
        for industry in Industry::iter() {
            assert_eq!(industry.to_string().parse::<Industry>(), Ok(industry));
        }
        assert_eq!(
            Industry::WholesaleAndRetail.to_string(),
            "wholesale_and_retail"
        );
    }
}
