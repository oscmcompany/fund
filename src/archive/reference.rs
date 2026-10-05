//! Reference tables as Parquet, one file per table and snapshot date, with the fetch's provenance alongside.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, UInt16Builder};
use arrow_array::{ArrayRef, BooleanArray, UInt16Array};
use arrow_schema::{DataType, Field, Schema};

use super::bars::{Provenance, provenance_from};
use super::parquet;
use crate::common::market::trade_bars::{TradeConditions, UpdateRules};
use crate::common::storage::{Key, Provider, ReferenceTable};

/// The file layout this build writes, read back from the metadata before any row.
const LAYOUT_VERSION: &str = "1";

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

/// The provider of a key naming the conditions table.
fn conditions_provider(key: &Key) -> Result<Provider, ReferenceRefusal> {
    match key {
        Key::Reference {
            provider,
            table: ReferenceTable::Conditions,
            ..
        } => Ok(*provider),
        Key::Reference {
            table: ReferenceTable::Classification | ReferenceTable::SecIndustryCodes,
            ..
        }
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
    ])
}

/// The conditions table's file for `key`, rows in code order.
pub fn encode_conditions(
    key: &Key,
    conditions: &TradeConditions,
    provenance: &Provenance,
) -> Result<Vec<u8>, ReferenceRefusal> {
    let provider = conditions_provider(key)?;
    if provenance.subscription().provider() != provider {
        return Err(ReferenceRefusal::SubscriptionProvider {
            provenance: provenance.clone(),
            key: provider,
        });
    }
    let mut codes = UInt16Builder::new();
    let mut flags: [BooleanBuilder; 3] = std::array::from_fn(|_| BooleanBuilder::new());
    for (code, rules) in conditions.rules() {
        codes.append_value(*code);
        for (builder, flag) in
            flags
                .iter_mut()
                .zip([rules.volume(), rules.high_low(), rules.open_close()])
        {
            builder.append_value(flag);
        }
    }
    let [volume, high_low, open_close] =
        flags.map(|mut builder| Arc::new(builder.finish()) as ArrayRef);
    let metadata = provenance
        .entries()
        .into_iter()
        .map(|(name, value)| ::parquet::file::metadata::KeyValue::new(name.to_string(), value))
        .collect();
    parquet::write(
        conditions_schema(),
        vec![Arc::new(codes.finish()), volume, high_low, open_close],
        LAYOUT_VERSION,
        metadata,
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The conditions table a file written by `encode_conditions` under `key` holds, with its provenance.
pub fn decode_conditions(
    key: &Key,
    bytes: Vec<u8>,
) -> Result<(TradeConditions, Provenance), ReferenceRefusal> {
    let provider = conditions_provider(key)?;
    let (batches, entries) = parquet::read(bytes, &conditions_schema(), LAYOUT_VERSION)?;
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
        ];
        for row in 0..batch.num_rows() {
            let code = codes.value(row);
            let rule = UpdateRules::new(
                flags[0].value(row),
                flags[1].value(row),
                flags[2].value(row),
            );
            if rules.insert(code, rule).is_some() {
                return Err(ReferenceRefusal::Duplicate { code });
            }
        }
    }
    Ok((TradeConditions::new(rules), provenance))
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
            (10, UpdateRules::new(true, true, false)),
            (15, UpdateRules::new(false, false, false)),
            (37, UpdateRules::new(true, false, false)),
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
            encode_conditions(&key(ReferenceTable::Classification), &conditions, &provenance),
            Err(ReferenceRefusal::NotTheTable {
                path: "data/equity/stage=parsed/reference/provider=massive/table=classification/as_of=2026-10-05/data.parquet"
                    .to_string()
            })
        );
    }
}
