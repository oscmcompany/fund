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
use super::parquet::{ReadRefusal, RowCause};
use super::{Archive, ArchiveError, parquet};
use crate::common::market::corporate_actions::{
    ActionId, BoundaryChange, BoundaryKind, SeriesBoundary, Split, SplitRatio,
};
use crate::common::market::security_details::{
    CentralIndexKey, IndustryCode, MarketIdentifierCode, SecurityDetails, SecurityType,
};
use crate::common::market::trade_bars::{
    Condition, ConditionCode, ConditionLetter, ConditionStatus, TradeConditions, UpdateRules,
};
use crate::common::market::{Dollars, Shares, Symbol};
use crate::common::storage::{Key, Provider, ReferenceKey, ReferenceTable, SeriesPrefix};
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
    /// The key names another table than the codec's.
    NotTheTable {
        named: ReferenceTable,
        expected: ReferenceTable,
    },
    SubscriptionProvider {
        provenance: Provenance,
        key: Provider,
    },
    /// The file was not written.
    Parquet {
        reason: String,
    },
    File(ReadRefusal),
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
        cause: RowCause,
    },
}

impl From<ReadRefusal> for ReferenceRefusal {
    fn from(refusal: ReadRefusal) -> Self {
        Self::File(refusal)
    }
}

/// The provider of a key naming `table`.
fn table_provider(key: &ReferenceKey, table: ReferenceTable) -> Result<Provider, ReferenceRefusal> {
    match key.table() == table {
        true => Ok(key.provider()),
        false => Err(ReferenceRefusal::NotTheTable {
            named: key.table(),
            expected: table,
        }),
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
    key: &ReferenceKey,
    conditions: &TradeConditions,
    provenance: &Provenance,
) -> Result<Vec<u8>, ReferenceRefusal> {
    snapshot_provider(key, ReferenceTable::Conditions, provenance)?;
    let mut codes = UInt16Builder::new();
    let mut flags: [BooleanBuilder; 4] = std::array::from_fn(|_| BooleanBuilder::new());
    let mut letters: [StringBuilder; 2] = std::array::from_fn(|_| StringBuilder::new());
    for (code, condition) in conditions.conditions() {
        let rules = condition.rules();
        codes.append_value(code.get());
        let retired = match condition.status() {
            ConditionStatus::Current => false,
            ConditionStatus::Retired => true,
        };
        for (builder, flag) in
            flags
                .iter_mut()
                .zip([rules.volume, rules.high_low, rules.open_close, retired])
        {
            builder.append_value(flag);
        }
        for (builder, letter) in letters
            .iter_mut()
            .zip([condition.consolidated_tape(), condition.unlisted_trading()])
        {
            builder.append_option(letter.map(|letter| String::from(letter.get())));
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
        provenance.metadata(),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The conditions table a file written by `encode_conditions` under `key` holds, with its provenance.
pub fn decode_conditions(
    key: &ReferenceKey,
    bytes: Vec<u8>,
) -> Result<(TradeConditions, Provenance), ReferenceRefusal> {
    table_provider(key, ReferenceTable::Conditions)?;
    let (batches, entries) = parquet::read(bytes, &conditions_schema(), CONDITIONS_LAYOUT_VERSION)?;
    let provenance = provenance_from(&entries).map_err(|name| ReadRefusal::Metadata { name })?;
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
            parquet::column::<StringArray>(&batch, 4)?,
            parquet::column::<StringArray>(&batch, 5)?,
        ];
        for row in 0..batch.num_rows() {
            let code = codes.value(row);
            let letter = |array: &parquet::Column<'_, StringArray>| match array.is_valid(row) {
                false => Ok(None),
                true => ConditionLetter::new(array.value(row))
                    .map(Some)
                    .map_err(|refusal| ReferenceRefusal::Row {
                        index: row,
                        cause: RowCause::Unparsable {
                            column: format!("{} of condition {code}", array.name()),
                            raw: refusal.raw,
                        },
                    }),
            };
            let status = match flags[3].value(row) {
                false => ConditionStatus::Current,
                true => ConditionStatus::Retired,
            };
            let rule = Condition::new(
                UpdateRules {
                    volume: flags[0].value(row),
                    high_low: flags[1].value(row),
                    open_close: flags[2].value(row),
                },
                letter(&letters[0])?,
                letter(&letters[1])?,
                status,
            );
            if rules.insert(ConditionCode::new(code), rule).is_some() {
                return Err(ReferenceRefusal::Duplicate { code });
            }
        }
    }
    Ok((TradeConditions::new(rules), provenance))
}

/// Massive's conditions snapshot taken on `as_of`.
pub fn conditions_key(as_of: SessionDate) -> ReferenceKey {
    ReferenceKey::new(Provider::Massive, ReferenceTable::Conditions, as_of)
}

/// The newest snapshot of `provider`'s `table` dated before `before`, or of any date when `before` is `None`, if the
/// archive holds one.
pub async fn latest_snapshot(
    archive: &Archive,
    provider: Provider,
    table: ReferenceTable,
    before: Option<SessionDate>,
) -> Result<Option<ReferenceKey>, ArchiveError> {
    let paths = archive
        .list(&SeriesPrefix::reference(provider, table))
        .await?;
    Ok(newest_before(&paths, before))
}

/// The newest reference key among `paths` dated before `before`, when there is one.
fn newest_before(paths: &[String], before: Option<SessionDate>) -> Option<ReferenceKey> {
    paths
        .iter()
        .filter_map(|path| path.parse::<Key>().ok())
        .filter_map(|key| ReferenceKey::try_from(key).ok())
        .filter(|key| before.is_none_or(|before| key.as_of() < before))
        .max_by_key(ReferenceKey::as_of)
}

/// Why no conditions table was read.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SnapshotError {
    #[error("{0}")]
    Archive(ArchiveError),
    #[error("no conditions table in the archive")]
    Absent,
    /// Listed, then gone when read.
    #[error("{} vanished", Key::from(*.key).path())]
    Vanished { key: ReferenceKey },
    #[error("the conditions table did not read: {0:?}")]
    Decode(ReferenceRefusal),
}

/// The newest conditions snapshot the archive holds, with its key.
pub async fn latest_conditions(
    archive: &Archive,
) -> Result<(ReferenceKey, TradeConditions), SnapshotError> {
    let latest = latest_snapshot(archive, Provider::Massive, ReferenceTable::Conditions, None)
        .await
        .map_err(SnapshotError::Archive)?
        .ok_or(SnapshotError::Absent)?;
    let bytes = archive
        .get(&Key::from(latest))
        .await
        .map_err(SnapshotError::Archive)?
        .ok_or(SnapshotError::Vanished { key: latest })?;
    let (conditions, _) = decode_conditions(&latest, bytes).map_err(SnapshotError::Decode)?;
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
    key: &ReferenceKey,
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
        provenance.metadata(),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The snapshot a file written by `encode_security_details` under `key` holds, every value rebuilt through its type.
pub fn decode_security_details(
    key: &ReferenceKey,
    bytes: Vec<u8>,
) -> Result<(Vec<SecurityDetails>, Provenance), ReferenceRefusal> {
    table_provider(key, ReferenceTable::SecurityDetails)?;
    let (batches, entries) = parquet::read(bytes, &security_details_schema(), LAYOUT_VERSION)?;
    let provenance = provenance_from(&entries).map_err(|name| ReadRefusal::Metadata { name })?;
    snapshot_provider(key, ReferenceTable::SecurityDetails, &provenance)?;
    let mut details = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for batch in batches {
        let strings = |index: usize| parquet::column::<StringArray>(&batch, index);
        let decimals = |index: usize| parquet::column::<Decimal128Array>(&batch, index);
        let (symbols, security_types, descriptions, exchanges) =
            (strings(0)?, strings(1)?, strings(3)?, strings(6)?);
        let industry_codes = parquet::column::<UInt16Array>(&batch, 2)?;
        let (shares, capitalizations) = (decimals(4)?, decimals(5)?);
        let central_index_keys = parquet::column::<UInt64Array>(&batch, 7)?;
        let read = |row: usize| -> Result<SecurityDetails, RowCause> {
            let optional = |valid: bool| valid.then_some(());
            let security_type = optional(security_types.is_valid(row))
                .map(|()| {
                    let raw = security_types.value(row);
                    raw.parse::<SecurityType>()
                        .map_err(|_| RowCause::Unparsable {
                            column: security_types.name().to_string(),
                            raw: raw.to_string(),
                        })
                })
                .transpose()?;
            let industry_code = optional(industry_codes.is_valid(row))
                .map(|()| IndustryCode::from_code(industry_codes.value(row)))
                .transpose()
                .map_err(RowCause::IndustryCode)?;
            let shares_outstanding = optional(shares.is_valid(row))
                .map(|()| shares.integer(row).map(Shares::from_units))
                .transpose()?;
            let market_capitalization = optional(capitalizations.is_valid(row))
                .map(|()| capitalizations.integer(row).map(Dollars::from_millionths))
                .transpose()?;
            let primary_exchange = optional(exchanges.is_valid(row))
                .map(|()| MarketIdentifierCode::new(exchanges.value(row)))
                .transpose()
                .map_err(RowCause::MarketIdentifierCode)?;
            Ok(SecurityDetails::new(
                Symbol::new(symbols.value(row)).map_err(RowCause::Symbol)?,
                security_type,
                industry_code,
                optional(descriptions.is_valid(row)).map(|()| descriptions.value(row).to_string()),
                shares_outstanding,
                market_capitalization,
                primary_exchange,
                optional(central_index_keys.is_valid(row))
                    .map(|()| CentralIndexKey::new(central_index_keys.value(row)))
                    .transpose()
                    .map_err(RowCause::CentralIndexKey)?,
            ))
        };
        for row in 0..batch.num_rows() {
            let index = details.len();
            let row = read(row).map_err(|cause| ReferenceRefusal::Row { index, cause })?;
            if !seen.insert(row.symbol().clone()) {
                return Err(ReferenceRefusal::DuplicateSymbol {
                    symbol: row.symbol().clone(),
                });
            }
            details.push(row);
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

/// The session a `Date32` column stores at `row`, refused past what a date holds.
fn session_at(
    dates: &parquet::Column<'_, Date32Array>,
    row: usize,
) -> Result<SessionDate, RowCause> {
    let days = dates.value(row);
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::TimeDelta::days(i64::from(days))))
        .map(SessionDate::from_date)
        .ok_or_else(|| RowCause::OutOfRange {
            column: dates.name().to_string(),
            value: i128::from(days),
        })
}

/// The provider `key` names for `table`, checked against the subscription the provenance says fetched it.
fn snapshot_provider(
    key: &ReferenceKey,
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
    key: &ReferenceKey,
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
        provenance.metadata(),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The snapshot a file written by `encode_splits` under `key` holds, every value rebuilt through its type.
pub fn decode_splits(
    key: &ReferenceKey,
    bytes: Vec<u8>,
) -> Result<(Vec<Split>, Provenance), ReferenceRefusal> {
    let (batches, entries) = parquet::read(bytes, &splits_schema(), LAYOUT_VERSION)?;
    let provenance = provenance_from(&entries).map_err(|name| ReadRefusal::Metadata { name })?;
    snapshot_provider(key, ReferenceTable::Splits, &provenance)?;
    let mut splits = Vec::new();
    for batch in batches {
        let (ids, symbols) = (
            parquet::column::<StringArray>(&batch, 0)?,
            parquet::column::<StringArray>(&batch, 1)?,
        );
        let dates = parquet::column::<Date32Array>(&batch, 2)?;
        let (froms, tos) = (
            parquet::column::<Decimal128Array>(&batch, 3)?,
            parquet::column::<Decimal128Array>(&batch, 4)?,
        );
        let read = |row: usize| -> Result<Split, RowCause> {
            Ok(Split::new(
                ActionId::new(ids.value(row)).map_err(RowCause::ActionId)?,
                Symbol::new(symbols.value(row)).map_err(RowCause::Symbol)?,
                session_at(&dates, row)?,
                SplitRatio::new(
                    Shares::from_units(froms.integer(row)?),
                    Shares::from_units(tos.integer(row)?),
                )
                .map_err(RowCause::SplitRatio)?,
            ))
        };
        for row in 0..batch.num_rows() {
            let index = splits.len();
            splits.push(read(row).map_err(|cause| ReferenceRefusal::Row { index, cause })?);
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
    key: &ReferenceKey,
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
        provenance.metadata(),
    )
    .map_err(|reason| ReferenceRefusal::Parquet { reason })
}

/// The snapshot a file written by `encode_series_boundaries` under `key` holds, every value rebuilt through its type.
pub fn decode_series_boundaries(
    key: &ReferenceKey,
    bytes: Vec<u8>,
) -> Result<(Vec<SeriesBoundary>, Provenance), ReferenceRefusal> {
    let (batches, entries) = parquet::read(bytes, &series_boundaries_schema(), LAYOUT_VERSION)?;
    let provenance = provenance_from(&entries).map_err(|name| ReadRefusal::Metadata { name })?;
    snapshot_provider(key, ReferenceTable::SeriesBoundaries, &provenance)?;
    let mut boundaries = Vec::new();
    for batch in batches {
        let strings = |index: usize| parquet::column::<StringArray>(&batch, index);
        let dates = |index: usize| parquet::column::<Date32Array>(&batch, index);
        let (ids, symbols, changes, related) = (strings(0)?, strings(1)?, strings(4)?, strings(5)?);
        let (ons, processed) = (dates(2)?, dates(3)?);
        let read = |row: usize| -> Result<SeriesBoundary, RowCause> {
            let raw = changes.value(row);
            let kind = raw
                .parse::<BoundaryKind>()
                .map_err(|_| RowCause::Unparsable {
                    column: changes.name().to_string(),
                    raw: raw.to_string(),
                })?;
            let related = related
                .is_valid(row)
                .then(|| Symbol::new(related.value(row)))
                .transpose()
                .map_err(RowCause::Symbol)?;
            SeriesBoundary::new(
                ActionId::new(ids.value(row)).map_err(RowCause::ActionId)?,
                Symbol::new(symbols.value(row)).map_err(RowCause::Symbol)?,
                session_at(&ons, row)?,
                session_at(&processed, row)?,
                BoundaryChange::new(kind, related).map_err(RowCause::BoundaryChange)?,
            )
            .map_err(RowCause::SeriesBoundary)
        };
        for row in 0..batch.num_rows() {
            let index = boundaries.len();
            boundaries.push(read(row).map_err(|cause| ReferenceRefusal::Row { index, cause })?);
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

    fn key(table: ReferenceTable) -> ReferenceKey {
        ReferenceKey::new(Provider::Massive, table, session())
    }

    #[test]
    fn test_the_newest_snapshot_is_bounded_only_when_asked() {
        let dated = |day: u32| {
            let as_of = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, day).unwrap());
            Key::from(conditions_key(as_of)).path()
        };
        let paths = [
            dated(1),
            dated(7),
            dated(3),
            "data/equity/elsewhere".to_string(),
        ];
        let as_of = |key: Option<ReferenceKey>| key.map(|key| key.as_of().to_string());
        assert_eq!(
            as_of(newest_before(&paths, None)),
            Some("2026-10-07".to_string())
        );
        assert_eq!(
            as_of(newest_before(
                &paths,
                Some(key(ReferenceTable::Conditions).as_of())
            )),
            Some("2026-10-03".to_string())
        );
        let first = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 1).unwrap());
        assert_eq!(as_of(newest_before(&paths, Some(first))), None);
    }

    #[test]
    fn test_conditions_read_back_exactly_and_only_from_their_table() {
        let conditions = TradeConditions::new(BTreeMap::from([
            (
                ConditionCode::new(6),
                Condition::new(
                    UpdateRules::VOLUME_ONLY,
                    Some(ConditionLetter::of('I')),
                    None,
                    ConditionStatus::Retired,
                ),
            ),
            (
                ConditionCode::new(10),
                Condition::new(
                    UpdateRules {
                        volume: true,
                        high_low: true,
                        open_close: false,
                    },
                    Some(ConditionLetter::of('4')),
                    Some(ConditionLetter::of('X')),
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(15),
                Condition::new(
                    UpdateRules {
                        volume: false,
                        high_low: false,
                        open_close: false,
                    },
                    None,
                    Some(ConditionLetter::of('W')),
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(37),
                Condition::new(
                    UpdateRules::VOLUME_ONLY,
                    None,
                    None,
                    ConditionStatus::Current,
                ),
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
            encode_conditions(
                &key(ReferenceTable::SecurityDetails),
                &conditions,
                &provenance
            ),
            Err(ReferenceRefusal::NotTheTable {
                named: ReferenceTable::SecurityDetails,
                expected: ReferenceTable::Conditions,
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
                Some(CentralIndexKey::new(1_090_872).unwrap()),
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
            snapshot_provenance().metadata(),
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
        /// A conditions table reads back exactly, any letter and every absence of one kept, retired or current.
        #[test]
        fn property_conditions_round_trip(
            rows in proptest::collection::btree_map(
                proptest::prelude::any::<u16>(),
                (
                    proptest::prelude::any::<(bool, bool, bool, bool)>(),
                    proptest::option::of(proptest::prelude::any::<char>()),
                    proptest::option::of(proptest::prelude::any::<char>()),
                ),
                0..20,
            ),
        ) {
            let conditions = TradeConditions::new(
                rows.into_iter()
                    .map(|(code, ((volume, high_low, open_close, retired), consolidated, unlisted))| {
                        let status = match retired {
                            false => ConditionStatus::Current,
                            true => ConditionStatus::Retired,
                        };
                        let condition = Condition::new(
                            UpdateRules { volume, high_low, open_close },
                            consolidated.map(ConditionLetter::of),
                            unlisted.map(ConditionLetter::of),
                            status,
                        );
                        (ConditionCode::new(code), condition)
                    })
                    .collect(),
            );
            let key = key(ReferenceTable::Conditions);
            let written = encode_conditions(&key, &conditions, &snapshot_provenance()).unwrap();
            proptest::prop_assert_eq!(
                decode_conditions(&key, written),
                Ok((conditions, snapshot_provenance()))
            );
        }

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
                    proptest::option::of(1_u64..),
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
                        code.map(|code| IndustryCode::from_code(code).unwrap()),
                        description.clone(),
                        shares.map(Shares::from_units),
                        capitalization.map(Dollars::from_millionths),
                        exchange.map(|code| MarketIdentifierCode::new(code).unwrap()),
                        central_index_key.map(|value| CentralIndexKey::new(value).unwrap()),
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
            let key = ReferenceKey::new(Provider::Massive, ReferenceTable::Splits, session());
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
            let key = ReferenceKey::new(Provider::Alpaca, ReferenceTable::SeriesBoundaries, session());
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
        let key = ReferenceKey::new(Provider::Massive, ReferenceTable::Splits, session());
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
