//! The Fama-French industry definitions, generated — see `tools/fetch-industry-classifications`.
//!
//! Edit the script, never this file. [`FETCHED_ON`] carries the date it was read.

/// The date the published definitions were last read into this file.
///
/// The only line in this file that changes with the clock, which is what lets `--check` compare
/// two generations by ignoring exactly one line.
pub const FETCHED_ON: &str = "2026-09-22";

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
