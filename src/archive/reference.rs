//! Reference tables as Parquet, one file per table and snapshot date, with the fetch's provenance alongside.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, StringBuilder, UInt16Builder, UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, StringArray, UInt16Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};

use super::bars::{Provenance, provenance_from};
use super::{Archive, parquet};
use crate::common::market::corporate_actions::{
    ActionId, BoundaryChange, BoundaryKind, SeriesBoundary, Split, SplitRatio,
};
use crate::common::market::security_details::{
    CentralIndexKey, IndustryCode, MarketIdentifierCode, SecurityDetails, SecurityType,
};
use crate::common::market::trade_bars::{
    Condition, TradeConditions, UpdateRules, condition_letter,
};
use crate::common::market::{Dollars, Shares, Symbol};
use crate::common::storage::{Key, Provider, ReferenceTable};
use crate::common::time::SessionDate;
use chrono::NaiveDate;

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
    DuplicateAction {
        id: ActionId,
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
    snapshot_provider(key, ReferenceTable::Conditions, provenance)?;
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
        provenance_metadata(provenance),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The conditions table a file written by `encode_conditions` under `key` holds, with its provenance.
pub fn decode_conditions(
    key: &Key,
    bytes: Vec<u8>,
) -> Result<(TradeConditions, Provenance), ReferenceRefusal> {
    table_provider(key, ReferenceTable::Conditions)?;
    let (batches, entries) = parquet::read(bytes, &conditions_schema(), CONDITIONS_LAYOUT_VERSION)?;
    let provenance =
        provenance_from(&entries).map_err(|name| ReferenceRefusal::Metadata { name })?;
    snapshot_provider(key, ReferenceTable::Conditions, &provenance)?;
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
                true => match condition_letter(array.value(row)) {
                    Some(letter) => Ok(Some(letter)),
                    None => Err(ReferenceRefusal::Row {
                        index: row,
                        reason: format!("condition {code} letter `{}`", array.value(row)),
                    }),
                },
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

/// Massive's conditions snapshot taken on `as_of`.
pub fn conditions_key(as_of: SessionDate) -> Key {
    Key::Reference {
        provider: Provider::Massive,
        table: ReferenceTable::Conditions,
        as_of,
    }
}

/// The newest snapshot of `provider`'s `table` dated before `before`, if the archive holds one.
pub async fn latest_snapshot(
    archive: &Archive,
    provider: Provider,
    table: ReferenceTable,
    before: SessionDate,
) -> Result<Option<Key>, String> {
    let series = Key::Reference {
        provider,
        table,
        as_of: before,
    }
    .series();
    Ok(archive
        .list(&series)
        .await
        .map_err(|error| error.to_string())?
        .iter()
        .filter_map(|path| Key::parse(path).ok())
        .filter(|key| key.session() < before)
        .max_by_key(Key::session))
}

/// The newest conditions snapshot the archive holds, with its key.
pub async fn latest_conditions(archive: &Archive) -> Result<(Key, TradeConditions), String> {
    let latest = latest_snapshot(
        archive,
        Provider::Massive,
        ReferenceTable::Conditions,
        SessionDate::from_date(NaiveDate::MAX),
    )
    .await?
    .ok_or("no conditions table in the archive")?;
    let bytes = archive
        .get(&latest)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("{} vanished", latest.path()))?;
    let (conditions, _) =
        decode_conditions(&latest, bytes).map_err(|refusal| format!("{refusal:?}"))?;
    Ok((latest, conditions))
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
    snapshot_provider(key, ReferenceTable::SecurityDetails, provenance)?;
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
        provenance_metadata(provenance),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The snapshot a file written by `encode_security_details` under `key` holds, every value rebuilt through its type.
pub fn decode_security_details(
    key: &Key,
    bytes: Vec<u8>,
) -> Result<(Vec<SecurityDetails>, Provenance), ReferenceRefusal> {
    table_provider(key, ReferenceTable::SecurityDetails)?;
    let (batches, entries) = parquet::read(bytes, &security_details_schema(), LAYOUT_VERSION)?;
    let provenance =
        provenance_from(&entries).map_err(|name| ReferenceRefusal::Metadata { name })?;
    snapshot_provider(key, ReferenceTable::SecurityDetails, &provenance)?;
    let mut details = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
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
            if !seen.insert(symbol.clone()) {
                return Err(ReferenceRefusal::DuplicateSymbol { symbol });
            }
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

/// Days since the Unix epoch, as Arrow's `Date32` counts them.
fn epoch_days(session: SessionDate) -> i32 {
    let days = (session.date() - NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch is a date"))
        .num_days();
    i32::try_from(days).expect("a session date lies within Date32's range")
}

fn from_epoch_days(days: i32) -> Option<SessionDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::TimeDelta::days(i64::from(days))))
        .map(SessionDate::from_date)
}

/// The provider `key` names for `table`, checked against the subscription the provenance says fetched it.
fn snapshot_provider(
    key: &Key,
    table: ReferenceTable,
    provenance: &Provenance,
) -> Result<Provider, ReferenceRefusal> {
    let provider = table_provider(key, table)?;
    match provenance.subscription().provider() == provider {
        true => Ok(provider),
        false => Err(ReferenceRefusal::SubscriptionProvider {
            provenance: provenance.clone(),
            key: provider,
        }),
    }
}

fn provenance_metadata(provenance: &Provenance) -> Vec<::parquet::file::metadata::KeyValue> {
    provenance
        .entries()
        .into_iter()
        .map(|(name, value)| ::parquet::file::metadata::KeyValue::new(name.to_string(), value))
        .collect()
}

/// The first identifier listed twice, which a snapshot refuses.
fn duplicate_action<'a>(ids: impl IntoIterator<Item = &'a ActionId>) -> Option<ActionId> {
    let mut seen = std::collections::BTreeSet::new();
    ids.into_iter().find(|id| !seen.insert(*id)).cloned()
}

fn splits_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("executed_on", DataType::Date32, false),
        Field::new("split_from", SHARES_TYPE, false),
        Field::new("split_to", SHARES_TYPE, false),
    ])
}

/// A splits snapshot's file for `key`, rows in symbol and date order; an action listed twice is refused.
pub fn encode_splits(
    key: &Key,
    splits: &[Split],
    provenance: &Provenance,
) -> Result<Vec<u8>, ReferenceRefusal> {
    snapshot_provider(key, ReferenceTable::Splits, provenance)?;
    if let Some(id) = duplicate_action(splits.iter().map(Split::id)) {
        return Err(ReferenceRefusal::DuplicateAction { id });
    }
    let mut ordered: Vec<&Split> = splits.iter().collect();
    ordered.sort_by(|left, right| {
        (left.symbol(), left.executed_on(), left.id()).cmp(&(
            right.symbol(),
            right.executed_on(),
            right.id(),
        ))
    });
    let mut ids = StringBuilder::new();
    let mut symbols = StringBuilder::new();
    let mut dates = Date32Builder::new();
    let mut froms = Decimal128Builder::new();
    let mut tos = Decimal128Builder::new();
    for split in ordered {
        ids.append_value(split.id().as_str());
        symbols.append_value(split.symbol().as_str());
        dates.append_value(epoch_days(split.executed_on()));
        froms.append_value(i128::from(split.ratio().from().units()));
        tos.append_value(i128::from(split.ratio().to().units()));
    }
    parquet::write(
        splits_schema(),
        vec![
            Arc::new(ids.finish()),
            Arc::new(symbols.finish()),
            Arc::new(dates.finish()),
            Arc::new(froms.finish().with_data_type(SHARES_TYPE)),
            Arc::new(tos.finish().with_data_type(SHARES_TYPE)),
        ],
        LAYOUT_VERSION,
        provenance_metadata(provenance),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The snapshot a file written by `encode_splits` under `key` holds, every value rebuilt through its type.
pub fn decode_splits(
    key: &Key,
    bytes: Vec<u8>,
) -> Result<(Vec<Split>, Provenance), ReferenceRefusal> {
    let (batches, entries) = parquet::read(bytes, &splits_schema(), LAYOUT_VERSION)?;
    let provenance =
        provenance_from(&entries).map_err(|name| ReferenceRefusal::Metadata { name })?;
    snapshot_provider(key, ReferenceTable::Splits, &provenance)?;
    let mut splits = Vec::new();
    for batch in batches {
        let (ids, symbols) = (
            parquet::downcast::<StringArray>(batch.column(0))?,
            parquet::downcast::<StringArray>(batch.column(1))?,
        );
        let dates = parquet::downcast::<Date32Array>(batch.column(2))?;
        let (froms, tos) = (
            parquet::downcast::<Decimal128Array>(batch.column(3))?,
            parquet::downcast::<Decimal128Array>(batch.column(4))?,
        );
        for row in 0..batch.num_rows() {
            let index = splits.len();
            let refused = |reason: String| ReferenceRefusal::Row { index, reason };
            let shares = |array: &Decimal128Array| {
                u64::try_from(array.value(row))
                    .map(Shares::from_units)
                    .map_err(|error| refused(error.to_string()))
            };
            splits.push(Split::new(
                ActionId::new(ids.value(row)).map_err(|error| refused(format!("{error:?}")))?,
                Symbol::new(symbols.value(row)).map_err(|error| refused(format!("{error:?}")))?,
                from_epoch_days(dates.value(row))
                    .ok_or_else(|| refused("date out of range".to_string()))?,
                SplitRatio::new(shares(froms)?, shares(tos)?)
                    .map_err(|error| refused(format!("{error:?}")))?,
            ));
        }
    }
    if let Some(id) = duplicate_action(splits.iter().map(Split::id)) {
        return Err(ReferenceRefusal::DuplicateAction { id });
    }
    Ok((splits, provenance))
}

fn series_boundaries_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("on", DataType::Date32, false),
        Field::new("processed_on", DataType::Date32, false),
        Field::new("change", DataType::Utf8, false),
        Field::new("related_symbol", DataType::Utf8, true),
    ])
}

/// A series boundaries snapshot's file for `key`, rows in symbol and date order; an action listed twice is refused.
pub fn encode_series_boundaries(
    key: &Key,
    boundaries: &[SeriesBoundary],
    provenance: &Provenance,
) -> Result<Vec<u8>, ReferenceRefusal> {
    snapshot_provider(key, ReferenceTable::SeriesBoundaries, provenance)?;
    if let Some(id) = duplicate_action(boundaries.iter().map(SeriesBoundary::id)) {
        return Err(ReferenceRefusal::DuplicateAction { id });
    }
    let mut ordered: Vec<&SeriesBoundary> = boundaries.iter().collect();
    ordered.sort_by(|left, right| {
        (left.symbol(), left.on(), left.id()).cmp(&(right.symbol(), right.on(), right.id()))
    });
    let mut ids = StringBuilder::new();
    let mut symbols = StringBuilder::new();
    let mut dates = Date32Builder::new();
    let mut processed = Date32Builder::new();
    let mut changes = StringBuilder::new();
    let mut related = StringBuilder::new();
    for boundary in ordered {
        ids.append_value(boundary.id().as_str());
        symbols.append_value(boundary.symbol().as_str());
        dates.append_value(epoch_days(boundary.on()));
        processed.append_value(epoch_days(boundary.processed_on()));
        changes.append_value(boundary.change().kind().to_string());
        related.append_option(boundary.change().related().map(Symbol::as_str));
    }
    parquet::write(
        series_boundaries_schema(),
        vec![
            Arc::new(ids.finish()),
            Arc::new(symbols.finish()),
            Arc::new(dates.finish()),
            Arc::new(processed.finish()),
            Arc::new(changes.finish()),
            Arc::new(related.finish()),
        ],
        LAYOUT_VERSION,
        provenance_metadata(provenance),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The snapshot a file written by `encode_series_boundaries` under `key` holds, every value rebuilt through its type.
pub fn decode_series_boundaries(
    key: &Key,
    bytes: Vec<u8>,
) -> Result<(Vec<SeriesBoundary>, Provenance), ReferenceRefusal> {
    let (batches, entries) = parquet::read(bytes, &series_boundaries_schema(), LAYOUT_VERSION)?;
    let provenance =
        provenance_from(&entries).map_err(|name| ReferenceRefusal::Metadata { name })?;
    snapshot_provider(key, ReferenceTable::SeriesBoundaries, &provenance)?;
    let mut boundaries = Vec::new();
    for batch in batches {
        let strings = |index: usize| parquet::downcast::<StringArray>(batch.column(index));
        let dates = |index: usize| parquet::downcast::<Date32Array>(batch.column(index));
        let (ids, symbols, changes, related) = (strings(0)?, strings(1)?, strings(4)?, strings(5)?);
        let (ons, processed) = (dates(2)?, dates(3)?);
        for row in 0..batch.num_rows() {
            let index = boundaries.len();
            let refused = |reason: String| ReferenceRefusal::Row { index, reason };
            let date = |array: &Date32Array| {
                from_epoch_days(array.value(row))
                    .ok_or_else(|| refused("date out of range".to_string()))
            };
            let kind = changes
                .value(row)
                .parse::<BoundaryKind>()
                .map_err(|error| refused(error.to_string()))?;
            let related = related
                .is_valid(row)
                .then(|| Symbol::new(related.value(row)))
                .transpose()
                .map_err(|error| refused(format!("{error:?}")))?;
            let change = BoundaryChange::new(kind, related)
                .map_err(|error| refused(format!("{error:?}")))?;
            boundaries.push(
                SeriesBoundary::new(
                    ActionId::new(ids.value(row)).map_err(|error| refused(format!("{error:?}")))?,
                    Symbol::new(symbols.value(row))
                        .map_err(|error| refused(format!("{error:?}")))?,
                    date(ons)?,
                    date(processed)?,
                    change,
                )
                .map_err(|error| refused(format!("{error:?}")))?,
            );
        }
    }
    if let Some(id) = duplicate_action(boundaries.iter().map(SeriesBoundary::id)) {
        return Err(ReferenceRefusal::DuplicateAction { id });
    }
    Ok((boundaries, provenance))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use uuid::Uuid;

    use super::*;
    use crate::archive::bars::Subscription;
    use crate::common::journal::RunId;
    use crate::common::time::SessionDate;

    fn session() -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 5).unwrap())
    }

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
                6,
                Condition::new(UpdateRules::new(true, false, false), Some('I'), None, true),
            ),
            (
                10,
                Condition::new(
                    UpdateRules::new(true, true, false),
                    Some('4'),
                    Some('X'),
                    false,
                ),
            ),
            (
                15,
                Condition::new(
                    UpdateRules::new(false, false, false),
                    None,
                    Some('W'),
                    false,
                ),
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

    fn snapshot_provenance() -> Provenance {
        Provenance::new(
            Subscription::StocksStarter,
            "2026-09-24T14:31:43Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(4)),
            None,
        )
    }

    #[test]
    fn test_a_file_listing_a_symbol_twice_is_refused_on_read() {
        let rows = || {
            let mut builder = StringBuilder::new();
            builder.append_value("AAA");
            builder.append_value("AAA");
            Arc::new(builder.finish()) as ArrayRef
        };
        let nulls = |data_type: DataType| arrow_array::new_null_array(&data_type, 2);
        let written = parquet::write(
            security_details_schema(),
            vec![
                rows(),
                nulls(DataType::Utf8),
                nulls(DataType::UInt16),
                nulls(DataType::Utf8),
                nulls(SHARES_TYPE),
                nulls(DOLLARS_TYPE),
                nulls(DataType::Utf8),
                nulls(DataType::UInt64),
            ],
            LAYOUT_VERSION,
            snapshot_provenance()
                .entries()
                .into_iter()
                .map(|(name, value)| {
                    ::parquet::file::metadata::KeyValue::new(name.to_string(), value)
                })
                .collect(),
        )
        .unwrap();
        assert_eq!(
            decode_security_details(&key(ReferenceTable::SecurityDetails), written),
            Err(ReferenceRefusal::DuplicateSymbol {
                symbol: Symbol::new("AAA").unwrap()
            })
        );
    }

    proptest::proptest! {
        /// A snapshot of unique symbols reads back exactly, every absence kept, in symbol order.
        #[test]
        fn property_security_details_round_trip(
            rows in proptest::collection::btree_map(
                "[A-Z]{1,5}",
                (
                    proptest::option::of(proptest::sample::select(<SecurityType as strum::IntoEnumIterator>::iter().collect::<Vec<_>>())),
                    proptest::option::of(100_u16..10_000),
                    proptest::option::of("[A-Z ]{1,30}"),
                    proptest::option::of(proptest::prelude::any::<u64>()),
                    proptest::option::of(proptest::prelude::any::<u64>()),
                    proptest::option::of(proptest::sample::select(vec!["XNAS", "XNYS", "ARCX", "BATS", "XASE"])),
                    proptest::option::of(proptest::prelude::any::<u64>()),
                ),
                0..20,
            ),
        ) {
            let details: Vec<SecurityDetails> = rows
                .iter()
                .map(|(symbol, (kind, code, description, shares, capitalization, exchange, central_index_key))| {
                    SecurityDetails::new(
                        Symbol::new(symbol).unwrap(),
                        *kind,
                        code.map(|code| IndustryCode::new(&format!("{code:04}")).unwrap()),
                        description.clone(),
                        shares.map(Shares::from_units),
                        capitalization.map(Dollars::from_millionths),
                        exchange.map(|code| MarketIdentifierCode::new(code).unwrap()),
                        central_index_key.map(CentralIndexKey::new),
                    )
                })
                .collect();
            let key = key(ReferenceTable::SecurityDetails);
            let written = encode_security_details(&key, &details, &snapshot_provenance()).unwrap();
            let (read, provenance) = decode_security_details(&key, written).unwrap();
            proptest::prop_assert_eq!(read, details);
            proptest::prop_assert_eq!(provenance, snapshot_provenance());
        }

        /// A splits snapshot of unique actions reads back exactly, whatever order it was given in.
        #[test]
        fn property_splits_round_trip(
            rows in proptest::collection::btree_map(
                "[a-f0-9]{1,12}",
                ("[A-Z]{1,5}", 0_i64..20_000, 1_u64..1_000_000_000, 1_u64..1_000_000_000),
                0..20,
            ),
        ) {
            let splits: Vec<Split> = rows
                .iter()
                .map(|(id, (symbol, day, from, to))| {
                    Split::new(
                        ActionId::new(id).unwrap(),
                        Symbol::new(symbol).unwrap(),
                        SessionDate::from_date(NaiveDate::from_ymd_opt(1978, 1, 1).unwrap()).plus_calendar_days(*day),
                        SplitRatio::new(Shares::from_units(*from), Shares::from_units(*to)).unwrap(),
                    )
                })
                .collect();
            let key = Key::Reference { provider: Provider::Massive, table: ReferenceTable::Splits, as_of: session() };
            let written = encode_splits(&key, &splits, &snapshot_provenance()).unwrap();
            let (mut read, provenance) = decode_splits(&key, written).unwrap();
            let mut expected = splits.clone();
            read.sort_by(|left, right| left.id().cmp(right.id()));
            expected.sort_by(|left, right| left.id().cmp(right.id()));
            proptest::prop_assert_eq!(read, expected);
            proptest::prop_assert_eq!(provenance, snapshot_provenance());
        }

        /// A boundaries snapshot of unique actions reads back exactly, each change with its related symbol.
        #[test]
        fn property_series_boundaries_round_trip(
            rows in proptest::collection::btree_map(
                "[a-f0-9]{1,12}",
                ("[A-Z]{1,4}", 0_i64..4_000, 0_i64..30, 0_usize..5, "[A-Z]{5}"),
                0..20,
            ),
        ) {
            let boundaries: Vec<SeriesBoundary> = rows
                .iter()
                .map(|(id, (symbol, day, lag, kind, related))| {
                    let on = SessionDate::from_date(NaiveDate::from_ymd_opt(2015, 1, 1).unwrap()).plus_calendar_days(*day);
                    let kind = <BoundaryKind as strum::IntoEnumIterator>::iter().nth(*kind).unwrap();
                    let related = match kind {
                        BoundaryKind::Renamed | BoundaryKind::SpunOff => Some(Symbol::new(related).unwrap()),
                        BoundaryKind::RightsDistributed | BoundaryKind::UnitSeparated | BoundaryKind::Reorganized => None,
                    };
                    SeriesBoundary::new(
                        ActionId::new(id).unwrap(),
                        Symbol::new(symbol).unwrap(),
                        on,
                        on.plus_calendar_days(*lag),
                        BoundaryChange::new(kind, related).unwrap(),
                    )
                    .unwrap()
                })
                .collect();
            let key = Key::Reference { provider: Provider::Alpaca, table: ReferenceTable::SeriesBoundaries, as_of: session() };
            let provenance = Provenance::new(
                Subscription::AlgoTraderPlus,
                "2026-10-08T11:00:00Z".parse().unwrap(),
                RunId::new(Uuid::from_u128(5)),
                None,
            );
            let written = encode_series_boundaries(&key, &boundaries, &provenance).unwrap();
            let (mut read, read_provenance) = decode_series_boundaries(&key, written).unwrap();
            let mut expected = boundaries.clone();
            read.sort_by(|left, right| left.id().cmp(right.id()));
            expected.sort_by(|left, right| left.id().cmp(right.id()));
            proptest::prop_assert_eq!(read, expected);
            proptest::prop_assert_eq!(read_provenance, provenance);
        }
    }

    #[test]
    fn test_a_corporate_action_snapshot_refuses_a_repeated_action_and_another_vendor() {
        let split = |id: &str| {
            Split::new(
                ActionId::new(id).unwrap(),
                Symbol::new("DPU").unwrap(),
                session(),
                SplitRatio::from_floats(50.0, 1.0).unwrap(),
            )
        };
        let key = Key::Reference {
            provider: Provider::Massive,
            table: ReferenceTable::Splits,
            as_of: session(),
        };
        assert_eq!(
            encode_splits(&key, &[split("E1"), split("E1")], &snapshot_provenance()),
            Err(ReferenceRefusal::DuplicateAction {
                id: ActionId::new("E1").unwrap()
            })
        );
        let alpaca = Provenance::new(
            Subscription::AlgoTraderPlus,
            "2026-10-08T11:00:00Z".parse().unwrap(),
            RunId::new(Uuid::from_u128(6)),
            None,
        );
        assert!(matches!(
            encode_splits(&key, &[split("E1")], &alpaca),
            Err(ReferenceRefusal::SubscriptionProvider { .. })
        ));
        assert!(matches!(
            encode_series_boundaries(&key, &[], &snapshot_provenance()),
            Err(ReferenceRefusal::NotTheTable { .. })
        ));
    }
}
