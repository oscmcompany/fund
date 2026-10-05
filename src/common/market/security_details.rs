//! What a symbol was on a snapshot date: its kind of security, industry, size and listing, each `None` where the
//! vendor reported nothing, so a snapshot answers questions about the universe as it stood rather than as it stands.

use super::{Dollars, Shares, Symbol};

/// The kind of security a symbol names, in our terms.
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
pub enum SecurityType {
    CommonStock,
    ExchangeTradedFund,
    Warrant,
    DepositaryReceipt,
    Fund,
    Unit,
    StructuredProduct,
    PreferredStock,
    ExchangeTradedSecurity,
    ExchangeTradedNote,
    ExchangeTradedVehicle,
    Right,
    Index,
}

/// A four-digit Standard Industrial Classification code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndustryCode(u16);

/// Why an industry code was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndustryCodeRefusal {
    Malformed { raw: String },
}

impl IndustryCode {
    /// Exactly four digits, as the SEC writes them.
    pub fn new(raw: &str) -> Result<Self, IndustryCodeRefusal> {
        match raw.len() == 4 && raw.bytes().all(|byte| byte.is_ascii_digit()) {
            true => raw
                .parse()
                .map(Self)
                .map_err(|_| IndustryCodeRefusal::Malformed {
                    raw: raw.to_string(),
                }),
            false => Err(IndustryCodeRefusal::Malformed {
                raw: raw.to_string(),
            }),
        }
    }

    pub fn code(self) -> u16 {
        self.0
    }
}

/// An exchange's ISO 10383 market identifier code, four capital letters or digits.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarketIdentifierCode(String);

/// Why a market identifier code was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketIdentifierCodeRefusal {
    Malformed { raw: String },
}

impl MarketIdentifierCode {
    pub fn new(raw: &str) -> Result<Self, MarketIdentifierCodeRefusal> {
        let allowed = |byte: u8| byte.is_ascii_uppercase() || byte.is_ascii_digit();
        match raw.len() == 4 && raw.bytes().all(allowed) {
            true => Ok(Self(raw.to_string())),
            false => Err(MarketIdentifierCodeRefusal::Malformed {
                raw: raw.to_string(),
            }),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One symbol's details on a snapshot date.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityDetails {
    symbol: Symbol,
    security_type: Option<SecurityType>,
    industry_code: Option<IndustryCode>,
    /// The vendor's wording of the industry, which it reports without the code on some rows and the reverse on others.
    industry_description: Option<String>,
    shares_outstanding: Option<Shares>,
    market_capitalization: Option<Dollars>,
    primary_exchange: Option<MarketIdentifierCode>,
    /// The SEC's Central Index Key, the filer identity that survives a rename.
    central_index_key: Option<u64>,
}

impl SecurityDetails {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        symbol: Symbol,
        security_type: Option<SecurityType>,
        industry_code: Option<IndustryCode>,
        industry_description: Option<String>,
        shares_outstanding: Option<Shares>,
        market_capitalization: Option<Dollars>,
        primary_exchange: Option<MarketIdentifierCode>,
        central_index_key: Option<u64>,
    ) -> Self {
        Self {
            symbol,
            security_type,
            industry_code,
            industry_description,
            shares_outstanding,
            market_capitalization,
            primary_exchange,
            central_index_key,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn security_type(&self) -> Option<SecurityType> {
        self.security_type
    }

    pub fn industry_code(&self) -> Option<IndustryCode> {
        self.industry_code
    }

    pub fn industry_description(&self) -> Option<&str> {
        self.industry_description.as_deref()
    }

    pub fn shares_outstanding(&self) -> Option<Shares> {
        self.shares_outstanding
    }

    pub fn market_capitalization(&self) -> Option<Dollars> {
        self.market_capitalization
    }

    pub fn primary_exchange(&self) -> Option<&MarketIdentifierCode> {
        self.primary_exchange.as_ref()
    }

    pub fn central_index_key(&self) -> Option<u64> {
        self.central_index_key
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_each_security_type_round_trips_its_name() {
        let names: Vec<String> = SecurityType::iter().map(|kind| kind.to_string()).collect();
        assert_eq!(names[0], "common_stock");
        assert_eq!(names.len(), 13);
        for kind in SecurityType::iter() {
            assert_eq!(kind.to_string().parse::<SecurityType>(), Ok(kind));
        }
    }

    #[test]
    fn test_codes_hold_only_their_written_form() {
        assert_eq!(IndustryCode::new("0100").map(IndustryCode::code), Ok(100));
        for raw in ["100", "01000", "01a0", ""] {
            assert!(IndustryCode::new(raw).is_err(), "{raw}");
        }
        assert!(MarketIdentifierCode::new("XNAS").is_ok());
        for raw in ["xnas", "XNA", "XNASD"] {
            assert!(MarketIdentifierCode::new(raw).is_err(), "{raw}");
        }
    }
}
