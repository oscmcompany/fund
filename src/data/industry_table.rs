//! The Fama-French industry definitions, generated — see `tools/fetch-industry-classifications`.
//!
//! Edit the script, never this file. [`FETCHED_ON`] carries the date it was read.

/// The date the published definitions were last read into this file.
///
/// The only line in this file that changes with the clock, which is what lets `--check` compare
/// two generations by ignoring exactly one line.
pub const FETCHED_ON: &str = "2026-09-17";

/// The twelve Fama-French industries, which is what this system means by a sector.
///
/// Coarse on purpose. The screen's concentration cap is a question about how much of the
/// book sits on one common factor, and a partition fine enough to separate two names that
/// move together answers a different question.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Sector {
    /// Consumer Nondurables -- Food, Tobacco, Textiles, Apparel, Leather, Toys
    NoDur,
    /// Consumer Durables -- Cars, TVs, Furniture, Household Appliances
    Durbl,
    /// Manufacturing -- Machinery, Trucks, Planes, Off Furn, Paper, Com Printing
    Manuf,
    /// Oil, Gas, and Coal Extraction and Products
    Enrgy,
    /// Chemicals and Allied Products
    Chems,
    /// Business Equipment -- Computers, Software, and Electronic Equipment
    BusEq,
    /// Telephone and Television Transmission
    Telcm,
    /// Utilities
    Utils,
    /// Wholesale, Retail, and Some Services (Laundries, Repair Shops)
    Shops,
    /// Healthcare, Medical Equipment, and Drugs
    Hlth,
    /// Finance
    Money,
    /// Everything the source assigns to no other bucket.
    ///
    /// A real bucket -- mines, construction, building materials, transport, hotels, business
    /// services, entertainment -- and not a synonym for an unclassified name. A name with no SIC
    /// code at all is `None`, and the two must not be folded together: this one names a group
    /// that shares a factor, and the other names the absence of an answer.
    Other,
}

impl Sector {
    /// The source's own short name, which is the stored form.
    pub fn as_str(&self) -> &'static str {
        match self {
            Sector::NoDur => "NoDur",
            Sector::Durbl => "Durbl",
            Sector::Manuf => "Manuf",
            Sector::Enrgy => "Enrgy",
            Sector::Chems => "Chems",
            Sector::BusEq => "BusEq",
            Sector::Telcm => "Telcm",
            Sector::Utils => "Utils",
            Sector::Shops => "Shops",
            Sector::Hlth => "Hlth",
            Sector::Money => "Money",
            Sector::Other => "Other",
        }
    }

    /// Every bucket, in the source's own order.
    pub const ALL: [Sector; 12] = [
        Sector::NoDur,
        Sector::Durbl,
        Sector::Manuf,
        Sector::Enrgy,
        Sector::Chems,
        Sector::BusEq,
        Sector::Telcm,
        Sector::Utils,
        Sector::Shops,
        Sector::Hlth,
        Sector::Money,
        Sector::Other,
    ];
}

/// The forty-nine Fama-French industries, the subgroup beneath [`Sector`].
///
/// Not nested inside `Sector`: the two are fitted independently by the source, so a name's
/// industry does not determine its sector and neither is derived from the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Industry {
    /// Agriculture
    Agric,
    /// Food Products
    Food,
    /// Candy & Soda
    Soda,
    /// Beer & Liquor
    Beer,
    /// Tobacco Products
    Smoke,
    /// Recreation
    Toys,
    /// Entertainment
    Fun,
    /// Printing and Publishing
    Books,
    /// Consumer Goods
    Hshld,
    /// Apparel
    Clths,
    /// Healthcare
    Hlth,
    /// Medical Equipment
    MedEq,
    /// Pharmaceutical Products
    Drugs,
    /// Chemicals
    Chems,
    /// Rubber and Plastic Products
    Rubbr,
    /// Textiles
    Txtls,
    /// Construction Materials
    BldMt,
    /// Construction
    Cnstr,
    /// Steel Works Etc
    Steel,
    /// Fabricated Products
    FabPr,
    /// Machinery
    Mach,
    /// Electrical Equipment
    ElcEq,
    /// Automobiles and Trucks
    Autos,
    /// Aircraft
    Aero,
    /// Shipbuilding, Railroad Equipment
    Ships,
    /// Defense
    Guns,
    /// Precious Metals
    Gold,
    /// Non-Metallic and Industrial Metal Mining
    Mines,
    /// Coal
    Coal,
    /// Petroleum and Natural Gas
    Oil,
    /// Utilities
    Util,
    /// Communication
    Telcm,
    /// Personal Services
    PerSv,
    /// Business Services
    BusSv,
    /// Computers
    Hardw,
    /// Computer Software
    Softw,
    /// Electronic Equipment
    Chips,
    /// Measuring and Control Equipment
    LabEq,
    /// Business Supplies
    Paper,
    /// Shipping Containers
    Boxes,
    /// Transportation
    Trans,
    /// Wholesale
    Whlsl,
    /// Retail
    Rtail,
    /// Restaurants, Hotels, Motels
    Meals,
    /// Banking
    Banks,
    /// Insurance
    Insur,
    /// Real Estate
    RlEst,
    /// Trading
    Fin,
    /// Everything the source assigns to no other bucket.
    ///
    /// Named "Almost Nothing" in the source, and carrying ranges of its own rather than being
    /// purely a fallback. As with `Sector::Other`, this is a group and not an absence.
    Other,
}

impl Industry {
    /// The source's own short name, which is the stored form.
    pub fn as_str(&self) -> &'static str {
        match self {
            Industry::Agric => "Agric",
            Industry::Food => "Food",
            Industry::Soda => "Soda",
            Industry::Beer => "Beer",
            Industry::Smoke => "Smoke",
            Industry::Toys => "Toys",
            Industry::Fun => "Fun",
            Industry::Books => "Books",
            Industry::Hshld => "Hshld",
            Industry::Clths => "Clths",
            Industry::Hlth => "Hlth",
            Industry::MedEq => "MedEq",
            Industry::Drugs => "Drugs",
            Industry::Chems => "Chems",
            Industry::Rubbr => "Rubbr",
            Industry::Txtls => "Txtls",
            Industry::BldMt => "BldMt",
            Industry::Cnstr => "Cnstr",
            Industry::Steel => "Steel",
            Industry::FabPr => "FabPr",
            Industry::Mach => "Mach",
            Industry::ElcEq => "ElcEq",
            Industry::Autos => "Autos",
            Industry::Aero => "Aero",
            Industry::Ships => "Ships",
            Industry::Guns => "Guns",
            Industry::Gold => "Gold",
            Industry::Mines => "Mines",
            Industry::Coal => "Coal",
            Industry::Oil => "Oil",
            Industry::Util => "Util",
            Industry::Telcm => "Telcm",
            Industry::PerSv => "PerSv",
            Industry::BusSv => "BusSv",
            Industry::Hardw => "Hardw",
            Industry::Softw => "Softw",
            Industry::Chips => "Chips",
            Industry::LabEq => "LabEq",
            Industry::Paper => "Paper",
            Industry::Boxes => "Boxes",
            Industry::Trans => "Trans",
            Industry::Whlsl => "Whlsl",
            Industry::Rtail => "Rtail",
            Industry::Meals => "Meals",
            Industry::Banks => "Banks",
            Industry::Insur => "Insur",
            Industry::RlEst => "RlEst",
            Industry::Fin => "Fin",
            Industry::Other => "Other",
        }
    }

    /// Every bucket, in the source's own order.
    pub const ALL: [Industry; 49] = [
        Industry::Agric,
        Industry::Food,
        Industry::Soda,
        Industry::Beer,
        Industry::Smoke,
        Industry::Toys,
        Industry::Fun,
        Industry::Books,
        Industry::Hshld,
        Industry::Clths,
        Industry::Hlth,
        Industry::MedEq,
        Industry::Drugs,
        Industry::Chems,
        Industry::Rubbr,
        Industry::Txtls,
        Industry::BldMt,
        Industry::Cnstr,
        Industry::Steel,
        Industry::FabPr,
        Industry::Mach,
        Industry::ElcEq,
        Industry::Autos,
        Industry::Aero,
        Industry::Ships,
        Industry::Guns,
        Industry::Gold,
        Industry::Mines,
        Industry::Coal,
        Industry::Oil,
        Industry::Util,
        Industry::Telcm,
        Industry::PerSv,
        Industry::BusSv,
        Industry::Hardw,
        Industry::Softw,
        Industry::Chips,
        Industry::LabEq,
        Industry::Paper,
        Industry::Boxes,
        Industry::Trans,
        Industry::Whlsl,
        Industry::Rtail,
        Industry::Meals,
        Industry::Banks,
        Industry::Insur,
        Industry::RlEst,
        Industry::Fin,
        Industry::Other,
    ];
}

/// One contiguous run of SIC codes, and the bucket the source assigns it to.
///
/// Both bounds are inclusive, and the runs are disjoint and ascending — the generator refuses
/// a published overlap rather than resolving it, so a lookup may stop at the first hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SicRange<Bucket: 'static> {
    pub low: u16,
    pub high: u16,
    pub bucket: Bucket,
}

/// Every SIC run the twelve-industry definition assigns, ascending by `low`.
pub const SECTOR_RANGES: [SicRange<Sector>; 49] = [
    SicRange {
        low: 100,
        high: 999,
        bucket: Sector::NoDur,
    },
    SicRange {
        low: 1200,
        high: 1399,
        bucket: Sector::Enrgy,
    },
    SicRange {
        low: 2000,
        high: 2399,
        bucket: Sector::NoDur,
    },
    SicRange {
        low: 2500,
        high: 2519,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 2520,
        high: 2589,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 2590,
        high: 2599,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 2600,
        high: 2699,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 2700,
        high: 2749,
        bucket: Sector::NoDur,
    },
    SicRange {
        low: 2750,
        high: 2769,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 2770,
        high: 2799,
        bucket: Sector::NoDur,
    },
    SicRange {
        low: 2800,
        high: 2829,
        bucket: Sector::Chems,
    },
    SicRange {
        low: 2830,
        high: 2839,
        bucket: Sector::Hlth,
    },
    SicRange {
        low: 2840,
        high: 2899,
        bucket: Sector::Chems,
    },
    SicRange {
        low: 2900,
        high: 2999,
        bucket: Sector::Enrgy,
    },
    SicRange {
        low: 3000,
        high: 3099,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3100,
        high: 3199,
        bucket: Sector::NoDur,
    },
    SicRange {
        low: 3200,
        high: 3569,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3570,
        high: 3579,
        bucket: Sector::BusEq,
    },
    SicRange {
        low: 3580,
        high: 3629,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3630,
        high: 3659,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 3660,
        high: 3692,
        bucket: Sector::BusEq,
    },
    SicRange {
        low: 3693,
        high: 3693,
        bucket: Sector::Hlth,
    },
    SicRange {
        low: 3694,
        high: 3699,
        bucket: Sector::BusEq,
    },
    SicRange {
        low: 3700,
        high: 3709,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3710,
        high: 3711,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 3712,
        high: 3713,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3714,
        high: 3714,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 3715,
        high: 3715,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3716,
        high: 3716,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 3717,
        high: 3749,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3750,
        high: 3751,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 3752,
        high: 3791,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3792,
        high: 3792,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 3793,
        high: 3799,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3810,
        high: 3829,
        bucket: Sector::BusEq,
    },
    SicRange {
        low: 3830,
        high: 3839,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3840,
        high: 3859,
        bucket: Sector::Hlth,
    },
    SicRange {
        low: 3860,
        high: 3899,
        bucket: Sector::Manuf,
    },
    SicRange {
        low: 3900,
        high: 3939,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 3940,
        high: 3989,
        bucket: Sector::NoDur,
    },
    SicRange {
        low: 3990,
        high: 3999,
        bucket: Sector::Durbl,
    },
    SicRange {
        low: 4800,
        high: 4899,
        bucket: Sector::Telcm,
    },
    SicRange {
        low: 4900,
        high: 4949,
        bucket: Sector::Utils,
    },
    SicRange {
        low: 5000,
        high: 5999,
        bucket: Sector::Shops,
    },
    SicRange {
        low: 6000,
        high: 6999,
        bucket: Sector::Money,
    },
    SicRange {
        low: 7200,
        high: 7299,
        bucket: Sector::Shops,
    },
    SicRange {
        low: 7370,
        high: 7379,
        bucket: Sector::BusEq,
    },
    SicRange {
        low: 7600,
        high: 7699,
        bucket: Sector::Shops,
    },
    SicRange {
        low: 8000,
        high: 8099,
        bucket: Sector::Hlth,
    },
];

/// Every SIC run the forty-nine-industry definition assigns, ascending by `low`.
pub const INDUSTRY_RANGES: [SicRange<Industry>; 598] = [
    SicRange {
        low: 100,
        high: 199,
        bucket: Industry::Agric,
    },
    SicRange {
        low: 200,
        high: 299,
        bucket: Industry::Agric,
    },
    SicRange {
        low: 700,
        high: 799,
        bucket: Industry::Agric,
    },
    SicRange {
        low: 800,
        high: 899,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 910,
        high: 919,
        bucket: Industry::Agric,
    },
    SicRange {
        low: 920,
        high: 999,
        bucket: Industry::Toys,
    },
    SicRange {
        low: 1000,
        high: 1009,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1010,
        high: 1019,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1020,
        high: 1029,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1030,
        high: 1039,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1040,
        high: 1049,
        bucket: Industry::Gold,
    },
    SicRange {
        low: 1050,
        high: 1059,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1060,
        high: 1069,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1070,
        high: 1079,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1080,
        high: 1089,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1090,
        high: 1099,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1100,
        high: 1119,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1200,
        high: 1299,
        bucket: Industry::Coal,
    },
    SicRange {
        low: 1300,
        high: 1300,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1310,
        high: 1319,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1320,
        high: 1329,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1330,
        high: 1339,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1370,
        high: 1379,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1380,
        high: 1380,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1381,
        high: 1381,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1382,
        high: 1382,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1389,
        high: 1389,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 1400,
        high: 1499,
        bucket: Industry::Mines,
    },
    SicRange {
        low: 1500,
        high: 1511,
        bucket: Industry::Cnstr,
    },
    SicRange {
        low: 1520,
        high: 1529,
        bucket: Industry::Cnstr,
    },
    SicRange {
        low: 1530,
        high: 1539,
        bucket: Industry::Cnstr,
    },
    SicRange {
        low: 1540,
        high: 1549,
        bucket: Industry::Cnstr,
    },
    SicRange {
        low: 1600,
        high: 1699,
        bucket: Industry::Cnstr,
    },
    SicRange {
        low: 1700,
        high: 1799,
        bucket: Industry::Cnstr,
    },
    SicRange {
        low: 2000,
        high: 2009,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2010,
        high: 2019,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2020,
        high: 2029,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2030,
        high: 2039,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2040,
        high: 2046,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2047,
        high: 2047,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 2048,
        high: 2048,
        bucket: Industry::Agric,
    },
    SicRange {
        low: 2050,
        high: 2059,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2060,
        high: 2063,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2064,
        high: 2068,
        bucket: Industry::Soda,
    },
    SicRange {
        low: 2070,
        high: 2079,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2080,
        high: 2080,
        bucket: Industry::Beer,
    },
    SicRange {
        low: 2082,
        high: 2082,
        bucket: Industry::Beer,
    },
    SicRange {
        low: 2083,
        high: 2083,
        bucket: Industry::Beer,
    },
    SicRange {
        low: 2084,
        high: 2084,
        bucket: Industry::Beer,
    },
    SicRange {
        low: 2085,
        high: 2085,
        bucket: Industry::Beer,
    },
    SicRange {
        low: 2086,
        high: 2086,
        bucket: Industry::Soda,
    },
    SicRange {
        low: 2087,
        high: 2087,
        bucket: Industry::Soda,
    },
    SicRange {
        low: 2090,
        high: 2092,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2095,
        high: 2095,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2096,
        high: 2096,
        bucket: Industry::Soda,
    },
    SicRange {
        low: 2097,
        high: 2097,
        bucket: Industry::Soda,
    },
    SicRange {
        low: 2098,
        high: 2099,
        bucket: Industry::Food,
    },
    SicRange {
        low: 2100,
        high: 2199,
        bucket: Industry::Smoke,
    },
    SicRange {
        low: 2200,
        high: 2269,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2270,
        high: 2279,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2280,
        high: 2284,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2290,
        high: 2295,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2296,
        high: 2296,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 2297,
        high: 2297,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2298,
        high: 2298,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2299,
        high: 2299,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2300,
        high: 2390,
        bucket: Industry::Clths,
    },
    SicRange {
        low: 2391,
        high: 2392,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 2393,
        high: 2395,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2396,
        high: 2396,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 2397,
        high: 2399,
        bucket: Industry::Txtls,
    },
    SicRange {
        low: 2400,
        high: 2439,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 2440,
        high: 2449,
        bucket: Industry::Boxes,
    },
    SicRange {
        low: 2450,
        high: 2459,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 2490,
        high: 2499,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 2510,
        high: 2519,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 2520,
        high: 2549,
        bucket: Industry::Paper,
    },
    SicRange {
        low: 2590,
        high: 2599,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 2600,
        high: 2639,
        bucket: Industry::Paper,
    },
    SicRange {
        low: 2640,
        high: 2659,
        bucket: Industry::Boxes,
    },
    SicRange {
        low: 2660,
        high: 2661,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 2670,
        high: 2699,
        bucket: Industry::Paper,
    },
    SicRange {
        low: 2700,
        high: 2709,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2710,
        high: 2719,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2720,
        high: 2729,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2730,
        high: 2739,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2740,
        high: 2749,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2750,
        high: 2759,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 2760,
        high: 2761,
        bucket: Industry::Paper,
    },
    SicRange {
        low: 2770,
        high: 2771,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2780,
        high: 2789,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2790,
        high: 2799,
        bucket: Industry::Books,
    },
    SicRange {
        low: 2800,
        high: 2809,
        bucket: Industry::Chems,
    },
    SicRange {
        low: 2810,
        high: 2819,
        bucket: Industry::Chems,
    },
    SicRange {
        low: 2820,
        high: 2829,
        bucket: Industry::Chems,
    },
    SicRange {
        low: 2830,
        high: 2830,
        bucket: Industry::Drugs,
    },
    SicRange {
        low: 2831,
        high: 2831,
        bucket: Industry::Drugs,
    },
    SicRange {
        low: 2833,
        high: 2833,
        bucket: Industry::Drugs,
    },
    SicRange {
        low: 2834,
        high: 2834,
        bucket: Industry::Drugs,
    },
    SicRange {
        low: 2835,
        high: 2835,
        bucket: Industry::Drugs,
    },
    SicRange {
        low: 2836,
        high: 2836,
        bucket: Industry::Drugs,
    },
    SicRange {
        low: 2840,
        high: 2843,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 2844,
        high: 2844,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 2850,
        high: 2859,
        bucket: Industry::Chems,
    },
    SicRange {
        low: 2860,
        high: 2869,
        bucket: Industry::Chems,
    },
    SicRange {
        low: 2870,
        high: 2879,
        bucket: Industry::Chems,
    },
    SicRange {
        low: 2890,
        high: 2899,
        bucket: Industry::Chems,
    },
    SicRange {
        low: 2900,
        high: 2912,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 2950,
        high: 2952,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 2990,
        high: 2999,
        bucket: Industry::Oil,
    },
    SicRange {
        low: 3010,
        high: 3011,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3020,
        high: 3021,
        bucket: Industry::Clths,
    },
    SicRange {
        low: 3031,
        high: 3031,
        bucket: Industry::Rubbr,
    },
    SicRange {
        low: 3041,
        high: 3041,
        bucket: Industry::Rubbr,
    },
    SicRange {
        low: 3050,
        high: 3053,
        bucket: Industry::Rubbr,
    },
    SicRange {
        low: 3060,
        high: 3069,
        bucket: Industry::Rubbr,
    },
    SicRange {
        low: 3070,
        high: 3079,
        bucket: Industry::Rubbr,
    },
    SicRange {
        low: 3080,
        high: 3089,
        bucket: Industry::Rubbr,
    },
    SicRange {
        low: 3090,
        high: 3099,
        bucket: Industry::Rubbr,
    },
    SicRange {
        low: 3100,
        high: 3111,
        bucket: Industry::Clths,
    },
    SicRange {
        low: 3130,
        high: 3131,
        bucket: Industry::Clths,
    },
    SicRange {
        low: 3140,
        high: 3149,
        bucket: Industry::Clths,
    },
    SicRange {
        low: 3150,
        high: 3151,
        bucket: Industry::Clths,
    },
    SicRange {
        low: 3160,
        high: 3161,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3170,
        high: 3171,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3172,
        high: 3172,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3190,
        high: 3199,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3200,
        high: 3200,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3210,
        high: 3211,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3220,
        high: 3221,
        bucket: Industry::Boxes,
    },
    SicRange {
        low: 3229,
        high: 3229,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3230,
        high: 3231,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3240,
        high: 3241,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3250,
        high: 3259,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3260,
        high: 3260,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3261,
        high: 3261,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3262,
        high: 3263,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3264,
        high: 3264,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3269,
        high: 3269,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3270,
        high: 3275,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3280,
        high: 3281,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3290,
        high: 3293,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3295,
        high: 3299,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3300,
        high: 3300,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3310,
        high: 3317,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3320,
        high: 3325,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3330,
        high: 3339,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3340,
        high: 3341,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3350,
        high: 3357,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3360,
        high: 3369,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3370,
        high: 3379,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3390,
        high: 3399,
        bucket: Industry::Steel,
    },
    SicRange {
        low: 3400,
        high: 3400,
        bucket: Industry::FabPr,
    },
    SicRange {
        low: 3410,
        high: 3412,
        bucket: Industry::Boxes,
    },
    SicRange {
        low: 3420,
        high: 3429,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3430,
        high: 3433,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3440,
        high: 3441,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3442,
        high: 3442,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3443,
        high: 3443,
        bucket: Industry::FabPr,
    },
    SicRange {
        low: 3444,
        high: 3444,
        bucket: Industry::FabPr,
    },
    SicRange {
        low: 3446,
        high: 3446,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3448,
        high: 3448,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3449,
        high: 3449,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3450,
        high: 3451,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3452,
        high: 3452,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3460,
        high: 3469,
        bucket: Industry::FabPr,
    },
    SicRange {
        low: 3470,
        high: 3479,
        bucket: Industry::FabPr,
    },
    SicRange {
        low: 3480,
        high: 3489,
        bucket: Industry::Guns,
    },
    SicRange {
        low: 3490,
        high: 3499,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 3510,
        high: 3519,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3520,
        high: 3529,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3530,
        high: 3530,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3531,
        high: 3531,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3532,
        high: 3532,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3533,
        high: 3533,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3534,
        high: 3534,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3535,
        high: 3535,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3536,
        high: 3536,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3537,
        high: 3537,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3538,
        high: 3538,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3540,
        high: 3549,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3550,
        high: 3559,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3560,
        high: 3569,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3570,
        high: 3579,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3580,
        high: 3580,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3581,
        high: 3581,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3582,
        high: 3582,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3585,
        high: 3585,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3586,
        high: 3586,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3589,
        high: 3589,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3590,
        high: 3599,
        bucket: Industry::Mach,
    },
    SicRange {
        low: 3600,
        high: 3600,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3610,
        high: 3613,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3620,
        high: 3621,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3622,
        high: 3622,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3623,
        high: 3629,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3630,
        high: 3639,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3640,
        high: 3644,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3645,
        high: 3645,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3646,
        high: 3646,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3647,
        high: 3647,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3648,
        high: 3649,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3650,
        high: 3651,
        bucket: Industry::Toys,
    },
    SicRange {
        low: 3652,
        high: 3652,
        bucket: Industry::Toys,
    },
    SicRange {
        low: 3660,
        high: 3660,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3661,
        high: 3661,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3662,
        high: 3662,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3663,
        high: 3663,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3664,
        high: 3664,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3665,
        high: 3665,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3666,
        high: 3666,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3669,
        high: 3669,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3670,
        high: 3679,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3680,
        high: 3680,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3681,
        high: 3681,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3682,
        high: 3682,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3683,
        high: 3683,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3684,
        high: 3684,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3685,
        high: 3685,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3686,
        high: 3686,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3687,
        high: 3687,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3688,
        high: 3688,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3689,
        high: 3689,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3690,
        high: 3690,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3691,
        high: 3692,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3693,
        high: 3693,
        bucket: Industry::MedEq,
    },
    SicRange {
        low: 3694,
        high: 3694,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3695,
        high: 3695,
        bucket: Industry::Hardw,
    },
    SicRange {
        low: 3699,
        high: 3699,
        bucket: Industry::ElcEq,
    },
    SicRange {
        low: 3700,
        high: 3700,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3710,
        high: 3710,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3711,
        high: 3711,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3713,
        high: 3713,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3714,
        high: 3714,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3715,
        high: 3715,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3716,
        high: 3716,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3720,
        high: 3720,
        bucket: Industry::Aero,
    },
    SicRange {
        low: 3721,
        high: 3721,
        bucket: Industry::Aero,
    },
    SicRange {
        low: 3723,
        high: 3724,
        bucket: Industry::Aero,
    },
    SicRange {
        low: 3725,
        high: 3725,
        bucket: Industry::Aero,
    },
    SicRange {
        low: 3728,
        high: 3729,
        bucket: Industry::Aero,
    },
    SicRange {
        low: 3730,
        high: 3731,
        bucket: Industry::Ships,
    },
    SicRange {
        low: 3732,
        high: 3732,
        bucket: Industry::Toys,
    },
    SicRange {
        low: 3740,
        high: 3743,
        bucket: Industry::Ships,
    },
    SicRange {
        low: 3750,
        high: 3751,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3760,
        high: 3769,
        bucket: Industry::Guns,
    },
    SicRange {
        low: 3790,
        high: 3791,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3792,
        high: 3792,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3795,
        high: 3795,
        bucket: Industry::Guns,
    },
    SicRange {
        low: 3799,
        high: 3799,
        bucket: Industry::Autos,
    },
    SicRange {
        low: 3800,
        high: 3800,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3810,
        high: 3810,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3811,
        high: 3811,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3812,
        high: 3812,
        bucket: Industry::Chips,
    },
    SicRange {
        low: 3820,
        high: 3820,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3821,
        high: 3821,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3822,
        high: 3822,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3823,
        high: 3823,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3824,
        high: 3824,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3825,
        high: 3825,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3826,
        high: 3826,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3827,
        high: 3827,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3829,
        high: 3829,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3830,
        high: 3839,
        bucket: Industry::LabEq,
    },
    SicRange {
        low: 3840,
        high: 3849,
        bucket: Industry::MedEq,
    },
    SicRange {
        low: 3850,
        high: 3851,
        bucket: Industry::MedEq,
    },
    SicRange {
        low: 3860,
        high: 3861,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3870,
        high: 3873,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3910,
        high: 3911,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3914,
        high: 3914,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3915,
        high: 3915,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3930,
        high: 3931,
        bucket: Industry::Toys,
    },
    SicRange {
        low: 3940,
        high: 3949,
        bucket: Industry::Toys,
    },
    SicRange {
        low: 3950,
        high: 3955,
        bucket: Industry::Paper,
    },
    SicRange {
        low: 3960,
        high: 3962,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3963,
        high: 3965,
        bucket: Industry::Clths,
    },
    SicRange {
        low: 3991,
        high: 3991,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3993,
        high: 3993,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 3995,
        high: 3995,
        bucket: Industry::Hshld,
    },
    SicRange {
        low: 3996,
        high: 3996,
        bucket: Industry::BldMt,
    },
    SicRange {
        low: 4000,
        high: 4013,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4040,
        high: 4049,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4100,
        high: 4100,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4110,
        high: 4119,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4120,
        high: 4121,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4130,
        high: 4131,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4140,
        high: 4142,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4150,
        high: 4151,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4170,
        high: 4173,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4190,
        high: 4199,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4200,
        high: 4200,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4210,
        high: 4219,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4220,
        high: 4229,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 4230,
        high: 4231,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4240,
        high: 4249,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4400,
        high: 4499,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4500,
        high: 4599,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4600,
        high: 4699,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4700,
        high: 4700,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4710,
        high: 4712,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4720,
        high: 4729,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4730,
        high: 4739,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4740,
        high: 4749,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4780,
        high: 4780,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4782,
        high: 4782,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4783,
        high: 4783,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4784,
        high: 4784,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4785,
        high: 4785,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4789,
        high: 4789,
        bucket: Industry::Trans,
    },
    SicRange {
        low: 4800,
        high: 4800,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4810,
        high: 4813,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4820,
        high: 4822,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4830,
        high: 4839,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4840,
        high: 4841,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4880,
        high: 4889,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4890,
        high: 4890,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4891,
        high: 4891,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4892,
        high: 4892,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4899,
        high: 4899,
        bucket: Industry::Telcm,
    },
    SicRange {
        low: 4900,
        high: 4900,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4910,
        high: 4911,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4920,
        high: 4922,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4923,
        high: 4923,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4924,
        high: 4925,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4930,
        high: 4931,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4932,
        high: 4932,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4939,
        high: 4939,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4940,
        high: 4942,
        bucket: Industry::Util,
    },
    SicRange {
        low: 4950,
        high: 4959,
        bucket: Industry::Other,
    },
    SicRange {
        low: 4960,
        high: 4961,
        bucket: Industry::Other,
    },
    SicRange {
        low: 4970,
        high: 4971,
        bucket: Industry::Other,
    },
    SicRange {
        low: 4990,
        high: 4991,
        bucket: Industry::Other,
    },
    SicRange {
        low: 5000,
        high: 5000,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5010,
        high: 5015,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5020,
        high: 5023,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5030,
        high: 5039,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5040,
        high: 5042,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5043,
        high: 5043,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5044,
        high: 5044,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5045,
        high: 5045,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5046,
        high: 5046,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5047,
        high: 5047,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5048,
        high: 5048,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5049,
        high: 5049,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5050,
        high: 5059,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5060,
        high: 5060,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5063,
        high: 5063,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5064,
        high: 5064,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5065,
        high: 5065,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5070,
        high: 5078,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5080,
        high: 5080,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5081,
        high: 5081,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5082,
        high: 5082,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5083,
        high: 5083,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5084,
        high: 5084,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5085,
        high: 5085,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5086,
        high: 5087,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5088,
        high: 5088,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5090,
        high: 5090,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5091,
        high: 5092,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5093,
        high: 5093,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5094,
        high: 5094,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5099,
        high: 5099,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5100,
        high: 5100,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5110,
        high: 5113,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5120,
        high: 5122,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5130,
        high: 5139,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5140,
        high: 5149,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5150,
        high: 5159,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5160,
        high: 5169,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5170,
        high: 5172,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5180,
        high: 5182,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5190,
        high: 5199,
        bucket: Industry::Whlsl,
    },
    SicRange {
        low: 5200,
        high: 5200,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5210,
        high: 5219,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5220,
        high: 5229,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5230,
        high: 5231,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5250,
        high: 5251,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5260,
        high: 5261,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5270,
        high: 5271,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5300,
        high: 5300,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5310,
        high: 5311,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5320,
        high: 5320,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5330,
        high: 5331,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5334,
        high: 5334,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5340,
        high: 5349,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5390,
        high: 5399,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5400,
        high: 5400,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5410,
        high: 5411,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5412,
        high: 5412,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5420,
        high: 5429,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5430,
        high: 5439,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5440,
        high: 5449,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5450,
        high: 5459,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5460,
        high: 5469,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5490,
        high: 5499,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5500,
        high: 5500,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5510,
        high: 5529,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5530,
        high: 5539,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5540,
        high: 5549,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5550,
        high: 5559,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5560,
        high: 5569,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5570,
        high: 5579,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5590,
        high: 5599,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5600,
        high: 5699,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5700,
        high: 5700,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5710,
        high: 5719,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5720,
        high: 5722,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5730,
        high: 5733,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5734,
        high: 5734,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5735,
        high: 5735,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5736,
        high: 5736,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5750,
        high: 5799,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5800,
        high: 5819,
        bucket: Industry::Meals,
    },
    SicRange {
        low: 5820,
        high: 5829,
        bucket: Industry::Meals,
    },
    SicRange {
        low: 5890,
        high: 5899,
        bucket: Industry::Meals,
    },
    SicRange {
        low: 5900,
        high: 5900,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5910,
        high: 5912,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5920,
        high: 5929,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5930,
        high: 5932,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5940,
        high: 5940,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5941,
        high: 5941,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5942,
        high: 5942,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5943,
        high: 5943,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5944,
        high: 5944,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5945,
        high: 5945,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5946,
        high: 5946,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5947,
        high: 5947,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5948,
        high: 5948,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5949,
        high: 5949,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5950,
        high: 5959,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5960,
        high: 5969,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5970,
        high: 5979,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5980,
        high: 5989,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5990,
        high: 5990,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5992,
        high: 5992,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5993,
        high: 5993,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5994,
        high: 5994,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5995,
        high: 5995,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 5999,
        high: 5999,
        bucket: Industry::Rtail,
    },
    SicRange {
        low: 6000,
        high: 6000,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6010,
        high: 6019,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6020,
        high: 6020,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6021,
        high: 6021,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6022,
        high: 6022,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6023,
        high: 6024,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6025,
        high: 6025,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6026,
        high: 6026,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6027,
        high: 6027,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6028,
        high: 6029,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6030,
        high: 6036,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6040,
        high: 6059,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6060,
        high: 6062,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6080,
        high: 6082,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6090,
        high: 6099,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6100,
        high: 6100,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6110,
        high: 6111,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6112,
        high: 6113,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6120,
        high: 6129,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6130,
        high: 6139,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6140,
        high: 6149,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6150,
        high: 6159,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6160,
        high: 6169,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6170,
        high: 6179,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6190,
        high: 6199,
        bucket: Industry::Banks,
    },
    SicRange {
        low: 6200,
        high: 6299,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6300,
        high: 6300,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6310,
        high: 6319,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6320,
        high: 6329,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6330,
        high: 6331,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6350,
        high: 6351,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6360,
        high: 6361,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6370,
        high: 6379,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6390,
        high: 6399,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6400,
        high: 6411,
        bucket: Industry::Insur,
    },
    SicRange {
        low: 6500,
        high: 6500,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6510,
        high: 6510,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6512,
        high: 6512,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6513,
        high: 6513,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6514,
        high: 6514,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6515,
        high: 6515,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6517,
        high: 6519,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6520,
        high: 6529,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6530,
        high: 6531,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6532,
        high: 6532,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6540,
        high: 6541,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6550,
        high: 6553,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6590,
        high: 6599,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6610,
        high: 6611,
        bucket: Industry::RlEst,
    },
    SicRange {
        low: 6700,
        high: 6700,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6710,
        high: 6719,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6720,
        high: 6722,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6723,
        high: 6723,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6724,
        high: 6724,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6725,
        high: 6725,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6726,
        high: 6726,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6730,
        high: 6733,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6740,
        high: 6779,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6790,
        high: 6791,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6792,
        high: 6792,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6793,
        high: 6793,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6794,
        high: 6794,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6795,
        high: 6795,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6798,
        high: 6798,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 6799,
        high: 6799,
        bucket: Industry::Fin,
    },
    SicRange {
        low: 7000,
        high: 7000,
        bucket: Industry::Meals,
    },
    SicRange {
        low: 7010,
        high: 7019,
        bucket: Industry::Meals,
    },
    SicRange {
        low: 7020,
        high: 7021,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7030,
        high: 7033,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7040,
        high: 7049,
        bucket: Industry::Meals,
    },
    SicRange {
        low: 7200,
        high: 7200,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7210,
        high: 7212,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7213,
        high: 7213,
        bucket: Industry::Meals,
    },
    SicRange {
        low: 7214,
        high: 7214,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7215,
        high: 7216,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7217,
        high: 7217,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7218,
        high: 7218,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7219,
        high: 7219,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7220,
        high: 7221,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7230,
        high: 7231,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7240,
        high: 7241,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7250,
        high: 7251,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7260,
        high: 7269,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7270,
        high: 7290,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7291,
        high: 7291,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7292,
        high: 7299,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7300,
        high: 7300,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7310,
        high: 7319,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7320,
        high: 7329,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7330,
        high: 7339,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7340,
        high: 7342,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7349,
        high: 7349,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7350,
        high: 7351,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7352,
        high: 7352,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7353,
        high: 7353,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7359,
        high: 7359,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7360,
        high: 7369,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7370,
        high: 7372,
        bucket: Industry::Softw,
    },
    SicRange {
        low: 7373,
        high: 7373,
        bucket: Industry::Softw,
    },
    SicRange {
        low: 7374,
        high: 7374,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7375,
        high: 7375,
        bucket: Industry::Softw,
    },
    SicRange {
        low: 7376,
        high: 7376,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7377,
        high: 7377,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7378,
        high: 7378,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7379,
        high: 7379,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7380,
        high: 7380,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7381,
        high: 7382,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7383,
        high: 7383,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7384,
        high: 7384,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7385,
        high: 7385,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7389,
        high: 7390,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7391,
        high: 7391,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7392,
        high: 7392,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7393,
        high: 7393,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7394,
        high: 7394,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7395,
        high: 7395,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7396,
        high: 7396,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7397,
        high: 7397,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7399,
        high: 7399,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7500,
        high: 7500,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7510,
        high: 7515,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7519,
        high: 7519,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 7520,
        high: 7529,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7530,
        high: 7539,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7540,
        high: 7549,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7600,
        high: 7600,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7620,
        high: 7620,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7622,
        high: 7622,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7623,
        high: 7623,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7629,
        high: 7629,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7630,
        high: 7631,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7640,
        high: 7641,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7690,
        high: 7699,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 7800,
        high: 7829,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7830,
        high: 7833,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7840,
        high: 7841,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7900,
        high: 7900,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7910,
        high: 7911,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7920,
        high: 7929,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7930,
        high: 7933,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7940,
        high: 7949,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7980,
        high: 7980,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 7990,
        high: 7999,
        bucket: Industry::Fun,
    },
    SicRange {
        low: 8000,
        high: 8099,
        bucket: Industry::Hlth,
    },
    SicRange {
        low: 8100,
        high: 8199,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 8200,
        high: 8299,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 8300,
        high: 8399,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 8400,
        high: 8499,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 8600,
        high: 8699,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 8700,
        high: 8700,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 8710,
        high: 8713,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 8720,
        high: 8721,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 8730,
        high: 8734,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 8740,
        high: 8748,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 8800,
        high: 8899,
        bucket: Industry::PerSv,
    },
    SicRange {
        low: 8900,
        high: 8910,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 8911,
        high: 8911,
        bucket: Industry::BusSv,
    },
    SicRange {
        low: 8920,
        high: 8999,
        bucket: Industry::BusSv,
    },
];
