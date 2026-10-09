//! What a symbol was on a snapshot date: its kind of security, industry, size and listing, each `None` where the
//! vendor reported nothing, so a snapshot answers questions about the universe as it stood rather than as it stands.

use std::num::NonZeroU64;

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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndustryCodeRefusal {
    #[error("`{raw}` is not a four-digit industry code")]
    Malformed { raw: String },
    /// A stored code past four digits.
    #[error("{code} is past a four-digit industry code")]
    TooLarge { code: u16 },
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

    /// A code read back from its stored integer, which drops the leading zeros the written form keeps.
    pub fn from_code(code: u16) -> Result<Self, IndustryCodeRefusal> {
        match code <= 9_999 {
            true => Ok(Self(code)),
            false => Err(IndustryCodeRefusal::TooLarge { code }),
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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MarketIdentifierCodeRefusal {
    #[error("`{raw}` is not a four-character market identifier code")]
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

/// The SEC's Central Index Key, the filer identity that survives a rename; the SEC issues no zero key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CentralIndexKey(NonZeroU64);

/// Why a Central Index Key was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CentralIndexKeyRefusal {
    #[error("0 is not a Central Index Key")]
    Zero,
}

impl CentralIndexKey {
    pub fn new(value: u64) -> Result<Self, CentralIndexKeyRefusal> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(CentralIndexKeyRefusal::Zero)
    }

    pub fn value(self) -> u64 {
        self.0.get()
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
    central_index_key: Option<CentralIndexKey>,
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
        central_index_key: Option<CentralIndexKey>,
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

    pub fn central_index_key(&self) -> Option<CentralIndexKey> {
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
        assert_eq!(
            [0, 9_999, 10_000].map(|code| IndustryCode::from_code(code).map(IndustryCode::code)),
            [
                Ok(0),
                Ok(9_999),
                Err(IndustryCodeRefusal::TooLarge { code: 10_000 })
            ]
        );
        assert_eq!(
            [0, 1].map(|value| CentralIndexKey::new(value).map(CentralIndexKey::value)),
            [Err(CentralIndexKeyRefusal::Zero), Ok(1)]
        );
        for raw in ["100", "01000", "01a0", ""] {
            assert!(IndustryCode::new(raw).is_err(), "{raw}");
        }
        assert!(MarketIdentifierCode::new("XNAS").is_ok());
        for raw in ["xnas", "XNA", "XNASD"] {
            assert!(MarketIdentifierCode::new(raw).is_err(), "{raw}");
        }
    }
}
