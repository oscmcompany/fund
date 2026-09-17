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
    ConsumerNondurables,
    /// Consumer Durables -- Cars, TVs, Furniture, Household Appliances
    ConsumerDurables,
    /// Manufacturing -- Machinery, Trucks, Planes, Off Furn, Paper, Com Printing
    Manufacturing,
    /// Oil, Gas, and Coal Extraction and Products
    Energy,
    /// Chemicals and Allied Products
    Chemicals,
    /// Business Equipment -- Computers, Software, and Electronic Equipment
    BusinessEquipment,
    /// Telephone and Television Transmission
    Telecommunications,
    /// Utilities
    Utilities,
    /// Wholesale, Retail, and Some Services (Laundries, Repair Shops)
    WholesaleAndRetail,
    /// Healthcare, Medical Equipment, and Drugs
    Healthcare,
    /// Finance
    Finance,
    /// Everything the source assigns to no other bucket.
    ///
    /// A real bucket -- mines, construction, building materials, transport, hotels, business
    /// services, entertainment -- and not a synonym for an unclassified name. A name with no SIC
    /// code at all is `None`, and the two must not be folded together: this one names a group
    /// that shares a factor, and the other names the absence of an answer.
    Other,
}

impl Sector {
    /// The stored form, which round-trips through the module's decode.
    pub fn as_str(&self) -> &'static str {
        match self {
            Sector::ConsumerNondurables => "ConsumerNondurables",
            Sector::ConsumerDurables => "ConsumerDurables",
            Sector::Manufacturing => "Manufacturing",
            Sector::Energy => "Energy",
            Sector::Chemicals => "Chemicals",
            Sector::BusinessEquipment => "BusinessEquipment",
            Sector::Telecommunications => "Telecommunications",
            Sector::Utilities => "Utilities",
            Sector::WholesaleAndRetail => "WholesaleAndRetail",
            Sector::Healthcare => "Healthcare",
            Sector::Finance => "Finance",
            Sector::Other => "Other",
        }
    }

    /// Every bucket, in the source's own order.
    pub const ALL: [Sector; 12] = [
        Sector::ConsumerNondurables,
        Sector::ConsumerDurables,
        Sector::Manufacturing,
        Sector::Energy,
        Sector::Chemicals,
        Sector::BusinessEquipment,
        Sector::Telecommunications,
        Sector::Utilities,
        Sector::WholesaleAndRetail,
        Sector::Healthcare,
        Sector::Finance,
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
    Agriculture,
    /// Food Products
    FoodProducts,
    /// Candy & Soda
    CandyAndSoda,
    /// Beer & Liquor
    BeerAndLiquor,
    /// Tobacco Products
    TobaccoProducts,
    /// Recreation
    Recreation,
    /// Entertainment
    Entertainment,
    /// Printing and Publishing
    PrintingAndPublishing,
    /// Consumer Goods
    ConsumerGoods,
    /// Apparel
    Apparel,
    /// Healthcare
    Healthcare,
    /// Medical Equipment
    MedicalEquipment,
    /// Pharmaceutical Products
    PharmaceuticalProducts,
    /// Chemicals
    Chemicals,
    /// Rubber and Plastic Products
    RubberAndPlasticProducts,
    /// Textiles
    Textiles,
    /// Construction Materials
    ConstructionMaterials,
    /// Construction
    Construction,
    /// Steel Works Etc
    SteelWorks,
    /// Fabricated Products
    FabricatedProducts,
    /// Machinery
    Machinery,
    /// Electrical Equipment
    ElectricalEquipment,
    /// Automobiles and Trucks
    AutomobilesAndTrucks,
    /// Aircraft
    Aircraft,
    /// Shipbuilding, Railroad Equipment
    ShipbuildingAndRailroadEquipment,
    /// Defense
    Defense,
    /// Precious Metals
    PreciousMetals,
    /// Non-Metallic and Industrial Metal Mining
    NonMetallicAndIndustrialMetalMining,
    /// Coal
    Coal,
    /// Petroleum and Natural Gas
    PetroleumAndNaturalGas,
    /// Utilities
    Utilities,
    /// Communication
    Communication,
    /// Personal Services
    PersonalServices,
    /// Business Services
    BusinessServices,
    /// Computers
    Computers,
    /// Computer Software
    ComputerSoftware,
    /// Electronic Equipment
    ElectronicEquipment,
    /// Measuring and Control Equipment
    MeasuringAndControlEquipment,
    /// Business Supplies
    BusinessSupplies,
    /// Shipping Containers
    ShippingContainers,
    /// Transportation
    Transportation,
    /// Wholesale
    Wholesale,
    /// Retail
    Retail,
    /// Restaurants, Hotels, Motels
    RestaurantsHotelsMotels,
    /// Banking
    Banking,
    /// Insurance
    Insurance,
    /// Real Estate
    RealEstate,
    /// Trading
    Trading,
    /// Everything the source assigns to no other bucket.
    ///
    /// Named "Almost Nothing" in the source, and carrying ranges of its own rather than being
    /// purely a fallback. As with `Sector::Other`, this is a group and not an absence.
    Other,
}

impl Industry {
    /// The stored form, which round-trips through the module's decode.
    pub fn as_str(&self) -> &'static str {
        match self {
            Industry::Agriculture => "Agriculture",
            Industry::FoodProducts => "FoodProducts",
            Industry::CandyAndSoda => "CandyAndSoda",
            Industry::BeerAndLiquor => "BeerAndLiquor",
            Industry::TobaccoProducts => "TobaccoProducts",
            Industry::Recreation => "Recreation",
            Industry::Entertainment => "Entertainment",
            Industry::PrintingAndPublishing => "PrintingAndPublishing",
            Industry::ConsumerGoods => "ConsumerGoods",
            Industry::Apparel => "Apparel",
            Industry::Healthcare => "Healthcare",
            Industry::MedicalEquipment => "MedicalEquipment",
            Industry::PharmaceuticalProducts => "PharmaceuticalProducts",
            Industry::Chemicals => "Chemicals",
            Industry::RubberAndPlasticProducts => "RubberAndPlasticProducts",
            Industry::Textiles => "Textiles",
            Industry::ConstructionMaterials => "ConstructionMaterials",
            Industry::Construction => "Construction",
            Industry::SteelWorks => "SteelWorks",
            Industry::FabricatedProducts => "FabricatedProducts",
            Industry::Machinery => "Machinery",
            Industry::ElectricalEquipment => "ElectricalEquipment",
            Industry::AutomobilesAndTrucks => "AutomobilesAndTrucks",
            Industry::Aircraft => "Aircraft",
            Industry::ShipbuildingAndRailroadEquipment => "ShipbuildingAndRailroadEquipment",
            Industry::Defense => "Defense",
            Industry::PreciousMetals => "PreciousMetals",
            Industry::NonMetallicAndIndustrialMetalMining => "NonMetallicAndIndustrialMetalMining",
            Industry::Coal => "Coal",
            Industry::PetroleumAndNaturalGas => "PetroleumAndNaturalGas",
            Industry::Utilities => "Utilities",
            Industry::Communication => "Communication",
            Industry::PersonalServices => "PersonalServices",
            Industry::BusinessServices => "BusinessServices",
            Industry::Computers => "Computers",
            Industry::ComputerSoftware => "ComputerSoftware",
            Industry::ElectronicEquipment => "ElectronicEquipment",
            Industry::MeasuringAndControlEquipment => "MeasuringAndControlEquipment",
            Industry::BusinessSupplies => "BusinessSupplies",
            Industry::ShippingContainers => "ShippingContainers",
            Industry::Transportation => "Transportation",
            Industry::Wholesale => "Wholesale",
            Industry::Retail => "Retail",
            Industry::RestaurantsHotelsMotels => "RestaurantsHotelsMotels",
            Industry::Banking => "Banking",
            Industry::Insurance => "Insurance",
            Industry::RealEstate => "RealEstate",
            Industry::Trading => "Trading",
            Industry::Other => "Other",
        }
    }

    /// Every bucket, in the source's own order.
    pub const ALL: [Industry; 49] = [
        Industry::Agriculture,
        Industry::FoodProducts,
        Industry::CandyAndSoda,
        Industry::BeerAndLiquor,
        Industry::TobaccoProducts,
        Industry::Recreation,
        Industry::Entertainment,
        Industry::PrintingAndPublishing,
        Industry::ConsumerGoods,
        Industry::Apparel,
        Industry::Healthcare,
        Industry::MedicalEquipment,
        Industry::PharmaceuticalProducts,
        Industry::Chemicals,
        Industry::RubberAndPlasticProducts,
        Industry::Textiles,
        Industry::ConstructionMaterials,
        Industry::Construction,
        Industry::SteelWorks,
        Industry::FabricatedProducts,
        Industry::Machinery,
        Industry::ElectricalEquipment,
        Industry::AutomobilesAndTrucks,
        Industry::Aircraft,
        Industry::ShipbuildingAndRailroadEquipment,
        Industry::Defense,
        Industry::PreciousMetals,
        Industry::NonMetallicAndIndustrialMetalMining,
        Industry::Coal,
        Industry::PetroleumAndNaturalGas,
        Industry::Utilities,
        Industry::Communication,
        Industry::PersonalServices,
        Industry::BusinessServices,
        Industry::Computers,
        Industry::ComputerSoftware,
        Industry::ElectronicEquipment,
        Industry::MeasuringAndControlEquipment,
        Industry::BusinessSupplies,
        Industry::ShippingContainers,
        Industry::Transportation,
        Industry::Wholesale,
        Industry::Retail,
        Industry::RestaurantsHotelsMotels,
        Industry::Banking,
        Industry::Insurance,
        Industry::RealEstate,
        Industry::Trading,
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
        bucket: Sector::ConsumerNondurables,
    },
    SicRange {
        low: 1200,
        high: 1399,
        bucket: Sector::Energy,
    },
    SicRange {
        low: 2000,
        high: 2399,
        bucket: Sector::ConsumerNondurables,
    },
    SicRange {
        low: 2500,
        high: 2519,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 2520,
        high: 2589,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 2590,
        high: 2599,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 2600,
        high: 2699,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 2700,
        high: 2749,
        bucket: Sector::ConsumerNondurables,
    },
    SicRange {
        low: 2750,
        high: 2769,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 2770,
        high: 2799,
        bucket: Sector::ConsumerNondurables,
    },
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
        low: 2840,
        high: 2899,
        bucket: Sector::Chemicals,
    },
    SicRange {
        low: 2900,
        high: 2999,
        bucket: Sector::Energy,
    },
    SicRange {
        low: 3000,
        high: 3099,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3100,
        high: 3199,
        bucket: Sector::ConsumerNondurables,
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
        low: 3580,
        high: 3629,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3630,
        high: 3659,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 3660,
        high: 3692,
        bucket: Sector::BusinessEquipment,
    },
    SicRange {
        low: 3693,
        high: 3693,
        bucket: Sector::Healthcare,
    },
    SicRange {
        low: 3694,
        high: 3699,
        bucket: Sector::BusinessEquipment,
    },
    SicRange {
        low: 3700,
        high: 3709,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3710,
        high: 3711,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 3712,
        high: 3713,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3714,
        high: 3714,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 3715,
        high: 3715,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3716,
        high: 3716,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 3717,
        high: 3749,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3750,
        high: 3751,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 3752,
        high: 3791,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3792,
        high: 3792,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 3793,
        high: 3799,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3810,
        high: 3829,
        bucket: Sector::BusinessEquipment,
    },
    SicRange {
        low: 3830,
        high: 3839,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3840,
        high: 3859,
        bucket: Sector::Healthcare,
    },
    SicRange {
        low: 3860,
        high: 3899,
        bucket: Sector::Manufacturing,
    },
    SicRange {
        low: 3900,
        high: 3939,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 3940,
        high: 3989,
        bucket: Sector::ConsumerNondurables,
    },
    SicRange {
        low: 3990,
        high: 3999,
        bucket: Sector::ConsumerDurables,
    },
    SicRange {
        low: 4800,
        high: 4899,
        bucket: Sector::Telecommunications,
    },
    SicRange {
        low: 4900,
        high: 4949,
        bucket: Sector::Utilities,
    },
    SicRange {
        low: 5000,
        high: 5999,
        bucket: Sector::WholesaleAndRetail,
    },
    SicRange {
        low: 6000,
        high: 6999,
        bucket: Sector::Finance,
    },
    SicRange {
        low: 7200,
        high: 7299,
        bucket: Sector::WholesaleAndRetail,
    },
    SicRange {
        low: 7370,
        high: 7379,
        bucket: Sector::BusinessEquipment,
    },
    SicRange {
        low: 7600,
        high: 7699,
        bucket: Sector::WholesaleAndRetail,
    },
    SicRange {
        low: 8000,
        high: 8099,
        bucket: Sector::Healthcare,
    },
];

/// Every SIC run the forty-nine-industry definition assigns, ascending by `low`.
pub const INDUSTRY_RANGES: [SicRange<Industry>; 598] = [
    SicRange {
        low: 100,
        high: 199,
        bucket: Industry::Agriculture,
    },
    SicRange {
        low: 200,
        high: 299,
        bucket: Industry::Agriculture,
    },
    SicRange {
        low: 700,
        high: 799,
        bucket: Industry::Agriculture,
    },
    SicRange {
        low: 800,
        high: 899,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 910,
        high: 919,
        bucket: Industry::Agriculture,
    },
    SicRange {
        low: 920,
        high: 999,
        bucket: Industry::Recreation,
    },
    SicRange {
        low: 1000,
        high: 1009,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1010,
        high: 1019,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1020,
        high: 1029,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1030,
        high: 1039,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1040,
        high: 1049,
        bucket: Industry::PreciousMetals,
    },
    SicRange {
        low: 1050,
        high: 1059,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1060,
        high: 1069,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1070,
        high: 1079,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1080,
        high: 1089,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1090,
        high: 1099,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1100,
        high: 1119,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1200,
        high: 1299,
        bucket: Industry::Coal,
    },
    SicRange {
        low: 1300,
        high: 1300,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1310,
        high: 1319,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1320,
        high: 1329,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1330,
        high: 1339,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1370,
        high: 1379,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1380,
        high: 1380,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1381,
        high: 1381,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1382,
        high: 1382,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1389,
        high: 1389,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 1400,
        high: 1499,
        bucket: Industry::NonMetallicAndIndustrialMetalMining,
    },
    SicRange {
        low: 1500,
        high: 1511,
        bucket: Industry::Construction,
    },
    SicRange {
        low: 1520,
        high: 1529,
        bucket: Industry::Construction,
    },
    SicRange {
        low: 1530,
        high: 1539,
        bucket: Industry::Construction,
    },
    SicRange {
        low: 1540,
        high: 1549,
        bucket: Industry::Construction,
    },
    SicRange {
        low: 1600,
        high: 1699,
        bucket: Industry::Construction,
    },
    SicRange {
        low: 1700,
        high: 1799,
        bucket: Industry::Construction,
    },
    SicRange {
        low: 2000,
        high: 2009,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2010,
        high: 2019,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2020,
        high: 2029,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2030,
        high: 2039,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2040,
        high: 2046,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2047,
        high: 2047,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 2048,
        high: 2048,
        bucket: Industry::Agriculture,
    },
    SicRange {
        low: 2050,
        high: 2059,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2060,
        high: 2063,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2064,
        high: 2068,
        bucket: Industry::CandyAndSoda,
    },
    SicRange {
        low: 2070,
        high: 2079,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2080,
        high: 2080,
        bucket: Industry::BeerAndLiquor,
    },
    SicRange {
        low: 2082,
        high: 2082,
        bucket: Industry::BeerAndLiquor,
    },
    SicRange {
        low: 2083,
        high: 2083,
        bucket: Industry::BeerAndLiquor,
    },
    SicRange {
        low: 2084,
        high: 2084,
        bucket: Industry::BeerAndLiquor,
    },
    SicRange {
        low: 2085,
        high: 2085,
        bucket: Industry::BeerAndLiquor,
    },
    SicRange {
        low: 2086,
        high: 2086,
        bucket: Industry::CandyAndSoda,
    },
    SicRange {
        low: 2087,
        high: 2087,
        bucket: Industry::CandyAndSoda,
    },
    SicRange {
        low: 2090,
        high: 2092,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2095,
        high: 2095,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2096,
        high: 2096,
        bucket: Industry::CandyAndSoda,
    },
    SicRange {
        low: 2097,
        high: 2097,
        bucket: Industry::CandyAndSoda,
    },
    SicRange {
        low: 2098,
        high: 2099,
        bucket: Industry::FoodProducts,
    },
    SicRange {
        low: 2100,
        high: 2199,
        bucket: Industry::TobaccoProducts,
    },
    SicRange {
        low: 2200,
        high: 2269,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2270,
        high: 2279,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2280,
        high: 2284,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2290,
        high: 2295,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2296,
        high: 2296,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 2297,
        high: 2297,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2298,
        high: 2298,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2299,
        high: 2299,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2300,
        high: 2390,
        bucket: Industry::Apparel,
    },
    SicRange {
        low: 2391,
        high: 2392,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 2393,
        high: 2395,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2396,
        high: 2396,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 2397,
        high: 2399,
        bucket: Industry::Textiles,
    },
    SicRange {
        low: 2400,
        high: 2439,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 2440,
        high: 2449,
        bucket: Industry::ShippingContainers,
    },
    SicRange {
        low: 2450,
        high: 2459,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 2490,
        high: 2499,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 2510,
        high: 2519,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 2520,
        high: 2549,
        bucket: Industry::BusinessSupplies,
    },
    SicRange {
        low: 2590,
        high: 2599,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 2600,
        high: 2639,
        bucket: Industry::BusinessSupplies,
    },
    SicRange {
        low: 2640,
        high: 2659,
        bucket: Industry::ShippingContainers,
    },
    SicRange {
        low: 2660,
        high: 2661,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 2670,
        high: 2699,
        bucket: Industry::BusinessSupplies,
    },
    SicRange {
        low: 2700,
        high: 2709,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2710,
        high: 2719,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2720,
        high: 2729,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2730,
        high: 2739,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2740,
        high: 2749,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2750,
        high: 2759,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 2760,
        high: 2761,
        bucket: Industry::BusinessSupplies,
    },
    SicRange {
        low: 2770,
        high: 2771,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2780,
        high: 2789,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2790,
        high: 2799,
        bucket: Industry::PrintingAndPublishing,
    },
    SicRange {
        low: 2800,
        high: 2809,
        bucket: Industry::Chemicals,
    },
    SicRange {
        low: 2810,
        high: 2819,
        bucket: Industry::Chemicals,
    },
    SicRange {
        low: 2820,
        high: 2829,
        bucket: Industry::Chemicals,
    },
    SicRange {
        low: 2830,
        high: 2830,
        bucket: Industry::PharmaceuticalProducts,
    },
    SicRange {
        low: 2831,
        high: 2831,
        bucket: Industry::PharmaceuticalProducts,
    },
    SicRange {
        low: 2833,
        high: 2833,
        bucket: Industry::PharmaceuticalProducts,
    },
    SicRange {
        low: 2834,
        high: 2834,
        bucket: Industry::PharmaceuticalProducts,
    },
    SicRange {
        low: 2835,
        high: 2835,
        bucket: Industry::PharmaceuticalProducts,
    },
    SicRange {
        low: 2836,
        high: 2836,
        bucket: Industry::PharmaceuticalProducts,
    },
    SicRange {
        low: 2840,
        high: 2843,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 2844,
        high: 2844,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 2850,
        high: 2859,
        bucket: Industry::Chemicals,
    },
    SicRange {
        low: 2860,
        high: 2869,
        bucket: Industry::Chemicals,
    },
    SicRange {
        low: 2870,
        high: 2879,
        bucket: Industry::Chemicals,
    },
    SicRange {
        low: 2890,
        high: 2899,
        bucket: Industry::Chemicals,
    },
    SicRange {
        low: 2900,
        high: 2912,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 2950,
        high: 2952,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 2990,
        high: 2999,
        bucket: Industry::PetroleumAndNaturalGas,
    },
    SicRange {
        low: 3010,
        high: 3011,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3020,
        high: 3021,
        bucket: Industry::Apparel,
    },
    SicRange {
        low: 3031,
        high: 3031,
        bucket: Industry::RubberAndPlasticProducts,
    },
    SicRange {
        low: 3041,
        high: 3041,
        bucket: Industry::RubberAndPlasticProducts,
    },
    SicRange {
        low: 3050,
        high: 3053,
        bucket: Industry::RubberAndPlasticProducts,
    },
    SicRange {
        low: 3060,
        high: 3069,
        bucket: Industry::RubberAndPlasticProducts,
    },
    SicRange {
        low: 3070,
        high: 3079,
        bucket: Industry::RubberAndPlasticProducts,
    },
    SicRange {
        low: 3080,
        high: 3089,
        bucket: Industry::RubberAndPlasticProducts,
    },
    SicRange {
        low: 3090,
        high: 3099,
        bucket: Industry::RubberAndPlasticProducts,
    },
    SicRange {
        low: 3100,
        high: 3111,
        bucket: Industry::Apparel,
    },
    SicRange {
        low: 3130,
        high: 3131,
        bucket: Industry::Apparel,
    },
    SicRange {
        low: 3140,
        high: 3149,
        bucket: Industry::Apparel,
    },
    SicRange {
        low: 3150,
        high: 3151,
        bucket: Industry::Apparel,
    },
    SicRange {
        low: 3160,
        high: 3161,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3170,
        high: 3171,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3172,
        high: 3172,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3190,
        high: 3199,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3200,
        high: 3200,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3210,
        high: 3211,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3220,
        high: 3221,
        bucket: Industry::ShippingContainers,
    },
    SicRange {
        low: 3229,
        high: 3229,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3230,
        high: 3231,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3240,
        high: 3241,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3250,
        high: 3259,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3260,
        high: 3260,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3261,
        high: 3261,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3262,
        high: 3263,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3264,
        high: 3264,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3269,
        high: 3269,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3270,
        high: 3275,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3280,
        high: 3281,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3290,
        high: 3293,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3295,
        high: 3299,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3300,
        high: 3300,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3310,
        high: 3317,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3320,
        high: 3325,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3330,
        high: 3339,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3340,
        high: 3341,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3350,
        high: 3357,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3360,
        high: 3369,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3370,
        high: 3379,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3390,
        high: 3399,
        bucket: Industry::SteelWorks,
    },
    SicRange {
        low: 3400,
        high: 3400,
        bucket: Industry::FabricatedProducts,
    },
    SicRange {
        low: 3410,
        high: 3412,
        bucket: Industry::ShippingContainers,
    },
    SicRange {
        low: 3420,
        high: 3429,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3430,
        high: 3433,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3440,
        high: 3441,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3442,
        high: 3442,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3443,
        high: 3443,
        bucket: Industry::FabricatedProducts,
    },
    SicRange {
        low: 3444,
        high: 3444,
        bucket: Industry::FabricatedProducts,
    },
    SicRange {
        low: 3446,
        high: 3446,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3448,
        high: 3448,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3449,
        high: 3449,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3450,
        high: 3451,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3452,
        high: 3452,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3460,
        high: 3469,
        bucket: Industry::FabricatedProducts,
    },
    SicRange {
        low: 3470,
        high: 3479,
        bucket: Industry::FabricatedProducts,
    },
    SicRange {
        low: 3480,
        high: 3489,
        bucket: Industry::Defense,
    },
    SicRange {
        low: 3490,
        high: 3499,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 3510,
        high: 3519,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3520,
        high: 3529,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3530,
        high: 3530,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3531,
        high: 3531,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3532,
        high: 3532,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3533,
        high: 3533,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3534,
        high: 3534,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3535,
        high: 3535,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3536,
        high: 3536,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3537,
        high: 3537,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3538,
        high: 3538,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3540,
        high: 3549,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3550,
        high: 3559,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3560,
        high: 3569,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3570,
        high: 3579,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3580,
        high: 3580,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3581,
        high: 3581,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3582,
        high: 3582,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3585,
        high: 3585,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3586,
        high: 3586,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3589,
        high: 3589,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3590,
        high: 3599,
        bucket: Industry::Machinery,
    },
    SicRange {
        low: 3600,
        high: 3600,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3610,
        high: 3613,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3620,
        high: 3621,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3622,
        high: 3622,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3623,
        high: 3629,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3630,
        high: 3639,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3640,
        high: 3644,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3645,
        high: 3645,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3646,
        high: 3646,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3647,
        high: 3647,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3648,
        high: 3649,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3650,
        high: 3651,
        bucket: Industry::Recreation,
    },
    SicRange {
        low: 3652,
        high: 3652,
        bucket: Industry::Recreation,
    },
    SicRange {
        low: 3660,
        high: 3660,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3661,
        high: 3661,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3662,
        high: 3662,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3663,
        high: 3663,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3664,
        high: 3664,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3665,
        high: 3665,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3666,
        high: 3666,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3669,
        high: 3669,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3670,
        high: 3679,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3680,
        high: 3680,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3681,
        high: 3681,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3682,
        high: 3682,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3683,
        high: 3683,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3684,
        high: 3684,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3685,
        high: 3685,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3686,
        high: 3686,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3687,
        high: 3687,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3688,
        high: 3688,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3689,
        high: 3689,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3690,
        high: 3690,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3691,
        high: 3692,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3693,
        high: 3693,
        bucket: Industry::MedicalEquipment,
    },
    SicRange {
        low: 3694,
        high: 3694,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3695,
        high: 3695,
        bucket: Industry::Computers,
    },
    SicRange {
        low: 3699,
        high: 3699,
        bucket: Industry::ElectricalEquipment,
    },
    SicRange {
        low: 3700,
        high: 3700,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3710,
        high: 3710,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3711,
        high: 3711,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3713,
        high: 3713,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3714,
        high: 3714,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3715,
        high: 3715,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3716,
        high: 3716,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3720,
        high: 3720,
        bucket: Industry::Aircraft,
    },
    SicRange {
        low: 3721,
        high: 3721,
        bucket: Industry::Aircraft,
    },
    SicRange {
        low: 3723,
        high: 3724,
        bucket: Industry::Aircraft,
    },
    SicRange {
        low: 3725,
        high: 3725,
        bucket: Industry::Aircraft,
    },
    SicRange {
        low: 3728,
        high: 3729,
        bucket: Industry::Aircraft,
    },
    SicRange {
        low: 3730,
        high: 3731,
        bucket: Industry::ShipbuildingAndRailroadEquipment,
    },
    SicRange {
        low: 3732,
        high: 3732,
        bucket: Industry::Recreation,
    },
    SicRange {
        low: 3740,
        high: 3743,
        bucket: Industry::ShipbuildingAndRailroadEquipment,
    },
    SicRange {
        low: 3750,
        high: 3751,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3760,
        high: 3769,
        bucket: Industry::Defense,
    },
    SicRange {
        low: 3790,
        high: 3791,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3792,
        high: 3792,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3795,
        high: 3795,
        bucket: Industry::Defense,
    },
    SicRange {
        low: 3799,
        high: 3799,
        bucket: Industry::AutomobilesAndTrucks,
    },
    SicRange {
        low: 3800,
        high: 3800,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3810,
        high: 3810,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3811,
        high: 3811,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3812,
        high: 3812,
        bucket: Industry::ElectronicEquipment,
    },
    SicRange {
        low: 3820,
        high: 3820,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3821,
        high: 3821,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3822,
        high: 3822,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3823,
        high: 3823,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3824,
        high: 3824,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3825,
        high: 3825,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3826,
        high: 3826,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3827,
        high: 3827,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3829,
        high: 3829,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3830,
        high: 3839,
        bucket: Industry::MeasuringAndControlEquipment,
    },
    SicRange {
        low: 3840,
        high: 3849,
        bucket: Industry::MedicalEquipment,
    },
    SicRange {
        low: 3850,
        high: 3851,
        bucket: Industry::MedicalEquipment,
    },
    SicRange {
        low: 3860,
        high: 3861,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3870,
        high: 3873,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3910,
        high: 3911,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3914,
        high: 3914,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3915,
        high: 3915,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3930,
        high: 3931,
        bucket: Industry::Recreation,
    },
    SicRange {
        low: 3940,
        high: 3949,
        bucket: Industry::Recreation,
    },
    SicRange {
        low: 3950,
        high: 3955,
        bucket: Industry::BusinessSupplies,
    },
    SicRange {
        low: 3960,
        high: 3962,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3963,
        high: 3965,
        bucket: Industry::Apparel,
    },
    SicRange {
        low: 3991,
        high: 3991,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3993,
        high: 3993,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 3995,
        high: 3995,
        bucket: Industry::ConsumerGoods,
    },
    SicRange {
        low: 3996,
        high: 3996,
        bucket: Industry::ConstructionMaterials,
    },
    SicRange {
        low: 4000,
        high: 4013,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4040,
        high: 4049,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4100,
        high: 4100,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4110,
        high: 4119,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4120,
        high: 4121,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4130,
        high: 4131,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4140,
        high: 4142,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4150,
        high: 4151,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4170,
        high: 4173,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4190,
        high: 4199,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4200,
        high: 4200,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4210,
        high: 4219,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4220,
        high: 4229,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 4230,
        high: 4231,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4240,
        high: 4249,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4400,
        high: 4499,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4500,
        high: 4599,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4600,
        high: 4699,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4700,
        high: 4700,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4710,
        high: 4712,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4720,
        high: 4729,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4730,
        high: 4739,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4740,
        high: 4749,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4780,
        high: 4780,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4782,
        high: 4782,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4783,
        high: 4783,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4784,
        high: 4784,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4785,
        high: 4785,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4789,
        high: 4789,
        bucket: Industry::Transportation,
    },
    SicRange {
        low: 4800,
        high: 4800,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4810,
        high: 4813,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4820,
        high: 4822,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4830,
        high: 4839,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4840,
        high: 4841,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4880,
        high: 4889,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4890,
        high: 4890,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4891,
        high: 4891,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4892,
        high: 4892,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4899,
        high: 4899,
        bucket: Industry::Communication,
    },
    SicRange {
        low: 4900,
        high: 4900,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4910,
        high: 4911,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4920,
        high: 4922,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4923,
        high: 4923,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4924,
        high: 4925,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4930,
        high: 4931,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4932,
        high: 4932,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4939,
        high: 4939,
        bucket: Industry::Utilities,
    },
    SicRange {
        low: 4940,
        high: 4942,
        bucket: Industry::Utilities,
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
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5010,
        high: 5015,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5020,
        high: 5023,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5030,
        high: 5039,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5040,
        high: 5042,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5043,
        high: 5043,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5044,
        high: 5044,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5045,
        high: 5045,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5046,
        high: 5046,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5047,
        high: 5047,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5048,
        high: 5048,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5049,
        high: 5049,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5050,
        high: 5059,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5060,
        high: 5060,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5063,
        high: 5063,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5064,
        high: 5064,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5065,
        high: 5065,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5070,
        high: 5078,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5080,
        high: 5080,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5081,
        high: 5081,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5082,
        high: 5082,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5083,
        high: 5083,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5084,
        high: 5084,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5085,
        high: 5085,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5086,
        high: 5087,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5088,
        high: 5088,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5090,
        high: 5090,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5091,
        high: 5092,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5093,
        high: 5093,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5094,
        high: 5094,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5099,
        high: 5099,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5100,
        high: 5100,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5110,
        high: 5113,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5120,
        high: 5122,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5130,
        high: 5139,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5140,
        high: 5149,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5150,
        high: 5159,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5160,
        high: 5169,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5170,
        high: 5172,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5180,
        high: 5182,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5190,
        high: 5199,
        bucket: Industry::Wholesale,
    },
    SicRange {
        low: 5200,
        high: 5200,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5210,
        high: 5219,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5220,
        high: 5229,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5230,
        high: 5231,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5250,
        high: 5251,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5260,
        high: 5261,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5270,
        high: 5271,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5300,
        high: 5300,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5310,
        high: 5311,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5320,
        high: 5320,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5330,
        high: 5331,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5334,
        high: 5334,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5340,
        high: 5349,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5390,
        high: 5399,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5400,
        high: 5400,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5410,
        high: 5411,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5412,
        high: 5412,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5420,
        high: 5429,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5430,
        high: 5439,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5440,
        high: 5449,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5450,
        high: 5459,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5460,
        high: 5469,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5490,
        high: 5499,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5500,
        high: 5500,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5510,
        high: 5529,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5530,
        high: 5539,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5540,
        high: 5549,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5550,
        high: 5559,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5560,
        high: 5569,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5570,
        high: 5579,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5590,
        high: 5599,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5600,
        high: 5699,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5700,
        high: 5700,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5710,
        high: 5719,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5720,
        high: 5722,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5730,
        high: 5733,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5734,
        high: 5734,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5735,
        high: 5735,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5736,
        high: 5736,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5750,
        high: 5799,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5800,
        high: 5819,
        bucket: Industry::RestaurantsHotelsMotels,
    },
    SicRange {
        low: 5820,
        high: 5829,
        bucket: Industry::RestaurantsHotelsMotels,
    },
    SicRange {
        low: 5890,
        high: 5899,
        bucket: Industry::RestaurantsHotelsMotels,
    },
    SicRange {
        low: 5900,
        high: 5900,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5910,
        high: 5912,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5920,
        high: 5929,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5930,
        high: 5932,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5940,
        high: 5940,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5941,
        high: 5941,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5942,
        high: 5942,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5943,
        high: 5943,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5944,
        high: 5944,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5945,
        high: 5945,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5946,
        high: 5946,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5947,
        high: 5947,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5948,
        high: 5948,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5949,
        high: 5949,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5950,
        high: 5959,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5960,
        high: 5969,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5970,
        high: 5979,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5980,
        high: 5989,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5990,
        high: 5990,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5992,
        high: 5992,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5993,
        high: 5993,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5994,
        high: 5994,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5995,
        high: 5995,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 5999,
        high: 5999,
        bucket: Industry::Retail,
    },
    SicRange {
        low: 6000,
        high: 6000,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6010,
        high: 6019,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6020,
        high: 6020,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6021,
        high: 6021,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6022,
        high: 6022,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6023,
        high: 6024,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6025,
        high: 6025,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6026,
        high: 6026,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6027,
        high: 6027,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6028,
        high: 6029,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6030,
        high: 6036,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6040,
        high: 6059,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6060,
        high: 6062,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6080,
        high: 6082,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6090,
        high: 6099,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6100,
        high: 6100,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6110,
        high: 6111,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6112,
        high: 6113,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6120,
        high: 6129,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6130,
        high: 6139,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6140,
        high: 6149,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6150,
        high: 6159,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6160,
        high: 6169,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6170,
        high: 6179,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6190,
        high: 6199,
        bucket: Industry::Banking,
    },
    SicRange {
        low: 6200,
        high: 6299,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6300,
        high: 6300,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6310,
        high: 6319,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6320,
        high: 6329,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6330,
        high: 6331,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6350,
        high: 6351,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6360,
        high: 6361,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6370,
        high: 6379,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6390,
        high: 6399,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6400,
        high: 6411,
        bucket: Industry::Insurance,
    },
    SicRange {
        low: 6500,
        high: 6500,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6510,
        high: 6510,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6512,
        high: 6512,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6513,
        high: 6513,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6514,
        high: 6514,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6515,
        high: 6515,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6517,
        high: 6519,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6520,
        high: 6529,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6530,
        high: 6531,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6532,
        high: 6532,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6540,
        high: 6541,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6550,
        high: 6553,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6590,
        high: 6599,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6610,
        high: 6611,
        bucket: Industry::RealEstate,
    },
    SicRange {
        low: 6700,
        high: 6700,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6710,
        high: 6719,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6720,
        high: 6722,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6723,
        high: 6723,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6724,
        high: 6724,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6725,
        high: 6725,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6726,
        high: 6726,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6730,
        high: 6733,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6740,
        high: 6779,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6790,
        high: 6791,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6792,
        high: 6792,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6793,
        high: 6793,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6794,
        high: 6794,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6795,
        high: 6795,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6798,
        high: 6798,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 6799,
        high: 6799,
        bucket: Industry::Trading,
    },
    SicRange {
        low: 7000,
        high: 7000,
        bucket: Industry::RestaurantsHotelsMotels,
    },
    SicRange {
        low: 7010,
        high: 7019,
        bucket: Industry::RestaurantsHotelsMotels,
    },
    SicRange {
        low: 7020,
        high: 7021,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7030,
        high: 7033,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7040,
        high: 7049,
        bucket: Industry::RestaurantsHotelsMotels,
    },
    SicRange {
        low: 7200,
        high: 7200,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7210,
        high: 7212,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7213,
        high: 7213,
        bucket: Industry::RestaurantsHotelsMotels,
    },
    SicRange {
        low: 7214,
        high: 7214,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7215,
        high: 7216,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7217,
        high: 7217,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7218,
        high: 7218,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7219,
        high: 7219,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7220,
        high: 7221,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7230,
        high: 7231,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7240,
        high: 7241,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7250,
        high: 7251,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7260,
        high: 7269,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7270,
        high: 7290,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7291,
        high: 7291,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7292,
        high: 7299,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7300,
        high: 7300,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7310,
        high: 7319,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7320,
        high: 7329,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7330,
        high: 7339,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7340,
        high: 7342,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7349,
        high: 7349,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7350,
        high: 7351,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7352,
        high: 7352,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7353,
        high: 7353,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7359,
        high: 7359,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7360,
        high: 7369,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7370,
        high: 7372,
        bucket: Industry::ComputerSoftware,
    },
    SicRange {
        low: 7373,
        high: 7373,
        bucket: Industry::ComputerSoftware,
    },
    SicRange {
        low: 7374,
        high: 7374,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7375,
        high: 7375,
        bucket: Industry::ComputerSoftware,
    },
    SicRange {
        low: 7376,
        high: 7376,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7377,
        high: 7377,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7378,
        high: 7378,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7379,
        high: 7379,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7380,
        high: 7380,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7381,
        high: 7382,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7383,
        high: 7383,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7384,
        high: 7384,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7385,
        high: 7385,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7389,
        high: 7390,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7391,
        high: 7391,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7392,
        high: 7392,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7393,
        high: 7393,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7394,
        high: 7394,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7395,
        high: 7395,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7396,
        high: 7396,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7397,
        high: 7397,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7399,
        high: 7399,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7500,
        high: 7500,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7510,
        high: 7515,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7519,
        high: 7519,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 7520,
        high: 7529,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7530,
        high: 7539,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7540,
        high: 7549,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7600,
        high: 7600,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7620,
        high: 7620,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7622,
        high: 7622,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7623,
        high: 7623,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7629,
        high: 7629,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7630,
        high: 7631,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7640,
        high: 7641,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7690,
        high: 7699,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 7800,
        high: 7829,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7830,
        high: 7833,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7840,
        high: 7841,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7900,
        high: 7900,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7910,
        high: 7911,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7920,
        high: 7929,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7930,
        high: 7933,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7940,
        high: 7949,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7980,
        high: 7980,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 7990,
        high: 7999,
        bucket: Industry::Entertainment,
    },
    SicRange {
        low: 8000,
        high: 8099,
        bucket: Industry::Healthcare,
    },
    SicRange {
        low: 8100,
        high: 8199,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 8200,
        high: 8299,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 8300,
        high: 8399,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 8400,
        high: 8499,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 8600,
        high: 8699,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 8700,
        high: 8700,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 8710,
        high: 8713,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 8720,
        high: 8721,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 8730,
        high: 8734,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 8740,
        high: 8748,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 8800,
        high: 8899,
        bucket: Industry::PersonalServices,
    },
    SicRange {
        low: 8900,
        high: 8910,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 8911,
        high: 8911,
        bucket: Industry::BusinessServices,
    },
    SicRange {
        low: 8920,
        high: 8999,
        bucket: Industry::BusinessServices,
    },
];
