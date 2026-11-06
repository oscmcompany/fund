//! The twelve Fama-French industries, which group the four-digit industry codes into buckets coarse enough to average
//! over, and the codes the SEC's filings assigned names the vendor left without one.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use super::security_details::{CentralIndexKey, IndustryCode, SecurityDetails};

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

/// An EDGAR accession number, such as `0001104659-26-019130`, naming one filing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AccessionNumber(String);

/// Why an accession number was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{raw}` is not ten digits, two digits and six digits joined by hyphens")]
pub struct AccessionNumberRefusal {
    raw: String,
}

impl AccessionNumber {
    pub fn new(raw: &str) -> Result<Self, AccessionNumberRefusal> {
        let lengths: Vec<usize> = raw.split('-').map(str::len).collect();
        let digits = raw
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'-');
        match digits && lengths == [10, 2, 6] {
            true => Ok(Self(raw.to_string())),
            false => Err(AccessionNumberRefusal {
                raw: raw.to_string(),
            }),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The industry code an annual report's header assigned its filer, known from the instant the SEC accepted it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiledIndustryCode {
    filer: CentralIndexKey,
    accession: AccessionNumber,
    industry_code: IndustryCode,
    accepted_at: DateTime<Utc>,
}

impl FiledIndustryCode {
    pub fn new(
        filer: CentralIndexKey,
        accession: AccessionNumber,
        industry_code: IndustryCode,
        accepted_at: DateTime<Utc>,
    ) -> Self {
        Self {
            filer,
            accession,
            industry_code,
            accepted_at,
        }
    }

    pub fn filer(&self) -> CentralIndexKey {
        self.filer
    }

    pub fn accession(&self) -> &AccessionNumber {
        &self.accession
    }

    pub fn industry_code(&self) -> IndustryCode {
        self.industry_code
    }

    pub fn accepted_at(&self) -> DateTime<Utc> {
        self.accepted_at
    }
}

/// Every filed code by filer and acceptance instant, read as of an instant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FiledIndustryCodes(BTreeMap<CentralIndexKey, BTreeMap<DateTime<Utc>, IndustryCode>>);

impl FiledIndustryCodes {
    pub fn new(rows: &[FiledIndustryCode]) -> Self {
        let mut codes: BTreeMap<CentralIndexKey, BTreeMap<DateTime<Utc>, IndustryCode>> =
            BTreeMap::new();
        for row in rows {
            codes
                .entry(row.filer)
                .or_default()
                .insert(row.accepted_at, row.industry_code);
        }
        Self(codes)
    }

    /// The code the latest report accepted at or before `at` assigned `filer`.
    pub fn code(&self, filer: CentralIndexKey, at: DateTime<Utc>) -> Option<IndustryCode> {
        self.0
            .get(&filer)
            .and_then(|filed| filed.range(..=at).next_back())
            .map(|(_, code)| *code)
    }
}

/// Why a name has no industry at an instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IndustryAbsence {
    #[error("the vendor gave no industry code and no filer")]
    NoFiler,
    #[error("filer {} had no annual report accepted by then", .filer.value())]
    NotYetFiled { filer: CentralIndexKey },
}

/// The industry `details` places its name in at `at`: the vendor's code, else the code its filer's latest annual report
/// accepted by `at` assigned.
pub fn industry_at(
    details: &SecurityDetails,
    filed: &FiledIndustryCodes,
    at: DateTime<Utc>,
) -> Result<Industry, IndustryAbsence> {
    match (details.industry_code(), details.central_index_key()) {
        (Some(code), _) => Ok(Industry::of(code)),
        (None, None) => Err(IndustryAbsence::NoFiler),
        (None, Some(filer)) => filed
            .code(filer, at)
            .map(Industry::of)
            .ok_or(IndustryAbsence::NotYetFiled { filer }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::TimeZone;
    use strum::IntoEnumIterator;

    use super::*;
    use crate::common::market::Symbol;

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

    fn instant(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 2, day, hour, 0, 0).unwrap()
    }

    fn filer() -> CentralIndexKey {
        CentralIndexKey::new(901_832).unwrap()
    }

    fn filed(accession: &str, code: u16, day: u32) -> FiledIndustryCode {
        FiledIndustryCode::new(
            filer(),
            AccessionNumber::new(accession).unwrap(),
            IndustryCode::from_code(code).unwrap(),
            instant(day, 21),
        )
    }

    fn details(code: Option<u16>, filer: Option<CentralIndexKey>) -> SecurityDetails {
        SecurityDetails::new(
            Symbol::new("AAA").unwrap(),
            None,
            code.map(|code| IndustryCode::from_code(code).unwrap()),
            None,
            None,
            None,
            None,
            filer,
        )
    }

    #[test]
    fn accession_numbers_keep_the_published_shape() {
        assert_eq!(
            AccessionNumber::new("0001104659-26-019130").map(|number| number.as_str().to_string()),
            Ok("0001104659-26-019130".to_string())
        );
        for raw in [
            "000110465926019130",
            "0001104659-26-01913",
            "00011046a9-26-019130",
            "",
        ] {
            assert!(AccessionNumber::new(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn a_filed_code_is_read_from_its_acceptance_on() {
        let codes = FiledIndustryCodes::new(&[
            filed("0001104659-24-000002", 2834, 20),
            filed("0001104659-24-000001", 6029, 10),
        ]);
        assert_eq!(codes.code(filer(), instant(10, 20)), None);
        assert_eq!(
            codes.code(filer(), instant(10, 21)).map(IndustryCode::code),
            Some(6029)
        );
        assert_eq!(
            codes.code(filer(), instant(20, 20)).map(IndustryCode::code),
            Some(6029)
        );
        assert_eq!(
            codes.code(filer(), instant(25, 0)).map(IndustryCode::code),
            Some(2834)
        );
        assert_eq!(
            codes.code(CentralIndexKey::new(1).unwrap(), instant(25, 0)),
            None
        );
    }

    #[test]
    fn the_vendor_code_wins_and_a_filing_fills_its_gap() {
        let codes = FiledIndustryCodes::new(&[filed("0001104659-24-000001", 2834, 10)]);
        assert_eq!(
            industry_at(&details(Some(6029), Some(filer())), &codes, instant(25, 0)),
            Ok(Industry::Finance)
        );
        assert_eq!(
            industry_at(&details(None, Some(filer())), &codes, instant(25, 0)),
            Ok(Industry::Healthcare)
        );
        assert_eq!(
            industry_at(&details(None, Some(filer())), &codes, instant(5, 0)),
            Err(IndustryAbsence::NotYetFiled { filer: filer() })
        );
        assert_eq!(
            industry_at(&details(None, None), &codes, instant(25, 0)),
            Err(IndustryAbsence::NoFiler)
        );
    }
}
