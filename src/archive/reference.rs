//! Reference tables as Parquet, one file per table and snapshot date, with the fetch's provenance alongside.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Decimal128Builder, StringBuilder, UInt16Builder, UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, StringArray, UInt16Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};

use super::bars::{Provenance, provenance_from};
use super::parquet;
use crate::common::market::security_details::{
    CentralIndexKey, IndustryCode, MarketIdentifierCode, SecurityDetails, SecurityType,
};
use crate::common::market::trade_bars::{Condition, TradeConditions, UpdateRules};
use crate::common::market::{Dollars, Shares, Symbol};
use crate::common::storage::{Key, Provider, ReferenceTable};

/// The file layout this build writes, read back from the metadata before any row.
const LAYOUT_VERSION: &str = "1";

/// The conditions layout, which gained each condition's tape letters and retired flag after its first snapshot.
const CONDITIONS_LAYOUT_VERSION: &str = "2";

const SHARES_TYPE: DataType = DataType::Decimal128(20, 6);
/// Dollars in millionths up to `u64::MAX`, twenty digits.
const DOLLARS_TYPE: DataType = DataType::Decimal128(20, 6);

/// Why a reference table was not written or read.
#[derive(Debug, Clone, PartialEq)]
pub enum ReferenceRefusal {
    /// The key names another table, or is not a reference key at all.
    NotTheTable {
        path: String,
    },
    SubscriptionProvider {
        provenance: Provenance,
        key: Provider,
    },
    Parquet {
        reason: String,
    },
    Schema {
        found: String,
    },
    Metadata {
        name: &'static str,
    },
    Layout {
        version: String,
    },
    Duplicate {
        code: u16,
    },
    DuplicateSymbol {
        symbol: Symbol,
    },
    /// A row that no longer passes its types' own checks.
    Row {
        index: usize,
        reason: String,
    },
}

impl From<parquet::ReadRefusal> for ReferenceRefusal {
    fn from(refusal: parquet::ReadRefusal) -> Self {
        match refusal {
            parquet::ReadRefusal::Parquet { reason } => Self::Parquet { reason },
            parquet::ReadRefusal::Schema { found } => Self::Schema { found },
            parquet::ReadRefusal::Metadata { name } => Self::Metadata { name },
            parquet::ReadRefusal::Layout { version } => Self::Layout { version },
        }
    }
}

/// The provider of a key naming `table`.
fn table_provider(key: &Key, table: ReferenceTable) -> Result<Provider, ReferenceRefusal> {
    match key {
        Key::Reference {
            provider,
            table: named,
            ..
        } if *named == table => Ok(*provider),
        Key::Reference { .. }
        | Key::Bars { .. }
        | Key::Quotes { .. }
        | Key::Trades { .. }
        | Key::RawBars { .. }
        | Key::RawQuotes { .. }
        | Key::RawTrades { .. }
        | Key::Journal { .. }
        | Key::Logs { .. } => Err(ReferenceRefusal::NotTheTable { path: key.path() }),
    }
}

fn conditions_schema() -> Schema {
    Schema::new(vec![
        Field::new("code", DataType::UInt16, false),
        Field::new("updates_volume", DataType::Boolean, false),
        Field::new("updates_high_low", DataType::Boolean, false),
        Field::new("updates_open_close", DataType::Boolean, false),
        Field::new("consolidated_tape_letter", DataType::Utf8, true),
        Field::new("unlisted_trading_letter", DataType::Utf8, true),
        Field::new("retired", DataType::Boolean, false),
    ])
}

/// The conditions table's file for `key`, rows in code order.
pub fn encode_conditions(
    key: &Key,
    conditions: &TradeConditions,
    provenance: &Provenance,
) -> Result<Vec<u8>, ReferenceRefusal> {
    let provider = table_provider(key, ReferenceTable::Conditions)?;
    if provenance.subscription().provider() != provider {
        return Err(ReferenceRefusal::SubscriptionProvider {
            provenance: provenance.clone(),
            key: provider,
        });
    }
    let mut codes = UInt16Builder::new();
    let mut flags: [BooleanBuilder; 4] = std::array::from_fn(|_| BooleanBuilder::new());
    let mut letters: [StringBuilder; 2] = std::array::from_fn(|_| StringBuilder::new());
    for (code, condition) in conditions.conditions() {
        let rules = condition.rules();
        codes.append_value(*code);
        for (builder, flag) in flags.iter_mut().zip([
            rules.volume(),
            rules.high_low(),
            rules.open_close(),
            condition.retired(),
        ]) {
            builder.append_value(flag);
        }
        for (builder, letter) in letters
            .iter_mut()
            .zip([condition.consolidated_tape(), condition.unlisted_trading()])
        {
            builder.append_option(letter.map(String::from));
        }
    }
    let [volume, high_low, open_close, retired] =
        flags.map(|mut builder| Arc::new(builder.finish()) as ArrayRef);
    let [consolidated_tape, unlisted_trading] =
        letters.map(|mut builder| Arc::new(builder.finish()) as ArrayRef);
    let metadata = provenance
        .entries()
        .into_iter()
        .map(|(name, value)| ::parquet::file::metadata::KeyValue::new(name.to_string(), value))
        .collect();
    parquet::write(
        conditions_schema(),
        vec![
            Arc::new(codes.finish()),
            volume,
            high_low,
            open_close,
            consolidated_tape,
            unlisted_trading,
            retired,
        ],
        CONDITIONS_LAYOUT_VERSION,
        metadata,
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The conditions table a file written by `encode_conditions` under `key` holds, with its provenance.
pub fn decode_conditions(
    key: &Key,
    bytes: Vec<u8>,
) -> Result<(TradeConditions, Provenance), ReferenceRefusal> {
    let provider = table_provider(key, ReferenceTable::Conditions)?;
    let (batches, entries) = parquet::read(bytes, &conditions_schema(), CONDITIONS_LAYOUT_VERSION)?;
    let provenance =
        provenance_from(&entries).map_err(|name| ReferenceRefusal::Metadata { name })?;
    if provenance.subscription().provider() != provider {
        return Err(ReferenceRefusal::SubscriptionProvider {
            provenance,
            key: provider,
        });
    }
    let mut rules = BTreeMap::new();
    for batch in batches {
        let codes = parquet::downcast::<UInt16Array>(batch.column(0))?;
        let flags = [
            parquet::downcast::<BooleanArray>(batch.column(1))?,
            parquet::downcast::<BooleanArray>(batch.column(2))?,
            parquet::downcast::<BooleanArray>(batch.column(3))?,
            parquet::downcast::<BooleanArray>(batch.column(6))?,
        ];
        let letters = [
            parquet::downcast::<StringArray>(batch.column(4))?,
            parquet::downcast::<StringArray>(batch.column(5))?,
        ];
        for row in 0..batch.num_rows() {
            let code = codes.value(row);
            let letter = |array: &StringArray| match array.is_valid(row) {
                false => Ok(None),
                true => {
                    let mut characters = array.value(row).chars();
                    match (characters.next(), characters.next()) {
                        (Some(letter), None) => Ok(Some(letter)),
                        (None, _) | (Some(_), Some(_)) => Err(ReferenceRefusal::Row {
                            index: row,
                            reason: format!("condition {code} letter `{}`", array.value(row)),
                        }),
                    }
                }
            };
            let rule = Condition::new(
                UpdateRules::new(
                    flags[0].value(row),
                    flags[1].value(row),
                    flags[2].value(row),
                ),
                letter(letters[0])?,
                letter(letters[1])?,
                flags[3].value(row),
            );
            if rules.insert(code, rule).is_some() {
                return Err(ReferenceRefusal::Duplicate { code });
            }
        }
    }
    Ok((TradeConditions::new(rules), provenance))
}

fn security_details_schema() -> Schema {
    Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("security_type", DataType::Utf8, true),
        Field::new("industry_code", DataType::UInt16, true),
        Field::new("industry_description", DataType::Utf8, true),
        Field::new("shares_outstanding", SHARES_TYPE, true),
        Field::new("market_capitalization", DOLLARS_TYPE, true),
        Field::new("primary_exchange", DataType::Utf8, true),
        Field::new("central_index_key", DataType::UInt64, true),
    ])
}

/// A snapshot's file for `key`, rows in symbol order; a symbol listed twice is refused.
pub fn encode_security_details(
    key: &Key,
    details: &[SecurityDetails],
    provenance: &Provenance,
) -> Result<Vec<u8>, ReferenceRefusal> {
    let provider = table_provider(key, ReferenceTable::SecurityDetails)?;
    if provenance.subscription().provider() != provider {
        return Err(ReferenceRefusal::SubscriptionProvider {
            provenance: provenance.clone(),
            key: provider,
        });
    }
    let mut ordered: Vec<&SecurityDetails> = details.iter().collect();
    ordered.sort_by(|left, right| left.symbol().cmp(right.symbol()));
    if let Some(pair) = ordered
        .windows(2)
        .find(|pair| pair[0].symbol() == pair[1].symbol())
    {
        return Err(ReferenceRefusal::DuplicateSymbol {
            symbol: pair[0].symbol().clone(),
        });
    }
    let mut symbols = StringBuilder::new();
    let mut security_types = StringBuilder::new();
    let mut industry_codes = UInt16Builder::new();
    let mut industry_descriptions = StringBuilder::new();
    let mut shares = Decimal128Builder::new();
    let mut capitalizations = Decimal128Builder::new();
    let mut exchanges = StringBuilder::new();
    let mut central_index_keys = UInt64Builder::new();
    for row in ordered {
        symbols.append_value(row.symbol().as_str());
        security_types.append_option(row.security_type().map(|kind| kind.to_string()));
        industry_codes.append_option(row.industry_code().map(IndustryCode::code));
        industry_descriptions.append_option(row.industry_description());
        shares.append_option(
            row.shares_outstanding()
                .map(|shares| i128::from(shares.units())),
        );
        capitalizations.append_option(
            row.market_capitalization()
                .map(|dollars| i128::from(dollars.millionths())),
        );
        exchanges.append_option(row.primary_exchange().map(MarketIdentifierCode::as_str));
        central_index_keys.append_option(row.central_index_key().map(CentralIndexKey::value));
    }
    let metadata = provenance
        .entries()
        .into_iter()
        .map(|(name, value)| ::parquet::file::metadata::KeyValue::new(name.to_string(), value))
        .collect();
    parquet::write(
        security_details_schema(),
        vec![
            Arc::new(symbols.finish()),
            Arc::new(security_types.finish()),
            Arc::new(industry_codes.finish()),
            Arc::new(industry_descriptions.finish()),
            Arc::new(shares.finish().with_data_type(SHARES_TYPE)),
            Arc::new(capitalizations.finish().with_data_type(DOLLARS_TYPE)),
            Arc::new(exchanges.finish()),
            Arc::new(central_index_keys.finish()),
        ],
        LAYOUT_VERSION,
        metadata,
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The snapshot a file written by `encode_security_details` under `key` holds, every value rebuilt through its type.
pub fn decode_security_details(
    key: &Key,
    bytes: Vec<u8>,
) -> Result<(Vec<SecurityDetails>, Provenance), ReferenceRefusal> {
    let provider = table_provider(key, ReferenceTable::SecurityDetails)?;
    let (batches, entries) = parquet::read(bytes, &security_details_schema(), LAYOUT_VERSION)?;
    let provenance =
        provenance_from(&entries).map_err(|name| ReferenceRefusal::Metadata { name })?;
    if provenance.subscription().provider() != provider {
        return Err(ReferenceRefusal::SubscriptionProvider {
            provenance,
            key: provider,
        });
    }
    let mut details = Vec::new();
    for batch in batches {
        let strings = |index: usize| parquet::downcast::<StringArray>(batch.column(index));
        let decimals = |index: usize| parquet::downcast::<Decimal128Array>(batch.column(index));
        let (symbols, security_types, descriptions, exchanges) =
            (strings(0)?, strings(1)?, strings(3)?, strings(6)?);
        let industry_codes = parquet::downcast::<UInt16Array>(batch.column(2))?;
        let (shares, capitalizations) = (decimals(4)?, decimals(5)?);
        let central_index_keys = parquet::downcast::<UInt64Array>(batch.column(7))?;
        for row in 0..batch.num_rows() {
            let index = details.len();
            let refused = |reason: String| ReferenceRefusal::Row { index, reason };
            let optional = |valid: bool| valid.then_some(());
            let symbol =
                Symbol::new(symbols.value(row)).map_err(|error| refused(format!("{error:?}")))?;
            let security_type = optional(security_types.is_valid(row))
                .map(|()| security_types.value(row).parse::<SecurityType>())
                .transpose()
                .map_err(|error| refused(error.to_string()))?;
            let industry_code = optional(industry_codes.is_valid(row))
                .map(|()| IndustryCode::new(&format!("{:04}", industry_codes.value(row))))
                .transpose()
                .map_err(|error| refused(format!("{error:?}")))?;
            let industry_description =
                optional(descriptions.is_valid(row)).map(|()| descriptions.value(row).to_string());
            let shares_outstanding = optional(shares.is_valid(row))
                .map(|()| u64::try_from(shares.value(row)).map(Shares::from_units))
                .transpose()
                .map_err(|error| refused(error.to_string()))?;
            let market_capitalization = optional(capitalizations.is_valid(row))
                .map(|()| u64::try_from(capitalizations.value(row)).map(Dollars::from_millionths))
                .transpose()
                .map_err(|error| refused(error.to_string()))?;
            let primary_exchange = optional(exchanges.is_valid(row))
                .map(|()| MarketIdentifierCode::new(exchanges.value(row)))
                .transpose()
                .map_err(|error| refused(format!("{error:?}")))?;
            let central_index_key = optional(central_index_keys.is_valid(row))
                .map(|()| CentralIndexKey::new(central_index_keys.value(row)));
            details.push(SecurityDetails::new(
                symbol,
                security_type,
                industry_code,
                industry_description,
                shares_outstanding,
                market_capitalization,
                primary_exchange,
                central_index_key,
            ));
        }
    }
    Ok((details, provenance))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use uuid::Uuid;

    use super::*;
    use crate::archive::bars::Subscription;
    use crate::common::journal::RunId;
    use crate::common::time::SessionDate;

    fn key(table: ReferenceTable) -> Key {
        Key::Reference {
            provider: Provider::Massive,
            table,
            as_of: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 5).unwrap()),
        }
    }

    #[test]
    fn test_conditions_read_back_exactly_and_only_from_their_table() {
        let conditions = TradeConditions::new(BTreeMap::from([
            (
                10,
                Condition::new(UpdateRules::new(true, true, false), None, None, false),
            ),
            (
                15,
                Condition::new(UpdateRules::new(false, false, false), None, None, false),
            ),
            (
                37,
                Condition::new(UpdateRules::new(true, false, false), None, None, false),
            ),
        ]));
        let provenance = Provenance::new(
            Subscription::StocksStarter,
            "2026-10-05T18:00:00Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(3)),
            None,
        );
        let written =
            encode_conditions(&key(ReferenceTable::Conditions), &conditions, &provenance).unwrap();
        assert_eq!(
            decode_conditions(&key(ReferenceTable::Conditions), written),
            Ok((conditions.clone(), provenance.clone()))
        );
        assert_eq!(
            encode_conditions(&key(ReferenceTable::SecurityDetails), &conditions, &provenance),
            Err(ReferenceRefusal::NotTheTable {
                path: "data/equity/stage=parsed/reference/provider=massive/table=security_details/as_of=2026-10-05/data.parquet"
                    .to_string()
            })
        );
    }

    #[test]
    fn test_security_details_read_back_with_every_absence_kept() {
        let details = vec![
            SecurityDetails::new(
                Symbol::new("AAA").unwrap(),
                Some(SecurityType::ExchangeTradedFund),
                None,
                None,
                Some(Shares::whole(400_000).unwrap()),
                None,
                Some(MarketIdentifierCode::new("ARCX").unwrap()),
                None,
            ),
            SecurityDetails::new(
                Symbol::new("A").unwrap(),
                Some(SecurityType::CommonStock),
                Some(IndustryCode::new("3826").unwrap()),
                Some("LABORATORY ANALYTICAL INSTRUMENTS".to_string()),
                Some(Shares::whole(303_000_000).unwrap()),
                Some(Dollars::from_float(51_340_135_490.0).unwrap()),
                Some(MarketIdentifierCode::new("XNYS").unwrap()),
                Some(CentralIndexKey::new(1_090_872)),
            ),
        ];
        let provenance = Provenance::new(
            Subscription::StocksStarter,
            "2026-09-24T14:31:43Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(4)),
            None,
        );
        let key = key(ReferenceTable::SecurityDetails);
        let written = encode_security_details(&key, &details, &provenance).unwrap();
        let (read, read_provenance) = decode_security_details(&key, written).unwrap();
        let symbols: Vec<&str> = read.iter().map(|row| row.symbol().as_str()).collect();
        assert_eq!(symbols, ["A", "AAA"]);
        assert_eq!(read[0], details[1]);
        assert_eq!(read[1], details[0]);
        assert_eq!(read_provenance, provenance);
        let twice = [details[0].clone(), details[0].clone()];
        assert_eq!(
            encode_security_details(&key, &twice, &provenance),
            Err(ReferenceRefusal::DuplicateSymbol {
                symbol: Symbol::new("AAA").unwrap()
            })
        );
    }
}
