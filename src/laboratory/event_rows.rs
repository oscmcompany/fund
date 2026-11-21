//! The event table's rows read at one horizon into `EventRow`s, from the Parquet bytes its statements wrote.

use arrow_array::{
    Array, BooleanArray, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use chrono::{DateTime, TimeDelta, Utc};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use strum::IntoEnumIterator;

use crate::common::book::Side;
use crate::common::cost::FillBound;
use crate::common::laboratory::entry::{
    EventRow, ExitCrossed, ExitQuote, ExitUnread, Resting, RestingLabels, RestingLabelsRefusal,
    TouchAfterArrival, UnknownCause,
};
use crate::common::laboratory::event_table::Horizon;
use crate::common::market::quote_bars::{QuoteSumsRefusal, Spread, StandingQuote};
use crate::common::market::{Price, PriceRefusal, Shares, SharesRefusal, Symbol, SymbolRefusal};
use crate::common::time::SessionDate;

/// A row read before its name's first quote, which has no touch to enter at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Untouched {
    pub symbol: Symbol,
    pub arrival: DateTime<Utc>,
}

/// Why the table was not read, with the column or row at fault.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EventRowsRefusal {
    #[error("the file is not readable Parquet: {reason}")]
    Parquet { reason: String },
    #[error("the table has no column `{name}`")]
    Absent { name: String },
    #[error("the column `{name}` holds {found}, not {expected}")]
    Type {
        name: String,
        expected: &'static str,
        found: String,
    },
    #[error("row {row}: {problem}")]
    Row { row: usize, problem: RowProblem },
}

/// What one row holds that no `EventRow` can.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RowProblem {
    #[error("`{name}` is null")]
    Null { name: String },
    #[error("`{name}`: {refusal}")]
    Price { name: String, refusal: PriceRefusal },
    #[error("`{name}`: {refusal}")]
    Shares {
        name: String,
        refusal: SharesRefusal,
    },
    #[error(transparent)]
    Symbol(SymbolRefusal),
    #[error("{microseconds} microseconds is past the instants a timestamp holds")]
    Arrival { microseconds: i64 },
    #[error("session {written} is not the arrival's {derived}")]
    Session {
        written: String,
        derived: SessionDate,
    },
    #[error(transparent)]
    Touch(QuoteSumsRefusal),
    #[error(transparent)]
    TouchAfterArrival(TouchAfterArrival),
    #[error("{known} of the four fill labels are known")]
    PartlyUnknown { known: usize },
    #[error(transparent)]
    Labels(RestingLabelsRefusal),
    /// The entry price written beside a label is not the one the label implies.
    #[error("`{name}` is {written}, not the {implied} its label implies")]
    EntryPrice {
        name: String,
        written: Price,
        implied: Price,
    },
    #[error(
        "twice the exit's mid, {doubled_mid}, and its spread, {spread}, in millionths place no quote on the grid"
    )]
    ExitOffGrid { doubled_mid: i64, spread: i64 },
    #[error(transparent)]
    Exit(ExitCrossed),
    #[error("`{name}` is {value}, which no finite measurement is")]
    NotFinite { name: String, value: f64 },
    #[error(
        "the trailing spread is {basis_points} basis points, below zero or beyond the largest spread"
    )]
    TrailingSpread { basis_points: f64 },
}

fn label_name(side: Side, label: &str, bound: FillBound) -> String {
    format!("forward_{side}_{label}_{bound}")
}

fn sides_and_bounds() -> impl Iterator<Item = (Side, FillBound)> {
    Side::iter().flat_map(|side| FillBound::iter().map(move |bound| (side, bound)))
}

/// The double columns read at `horizon`; `session` and `ticker` are strings, `arrival` a timestamp and the fill
/// labels booleans.
fn float_names(horizon: Horizon) -> Vec<String> {
    let suffix = horizon.suffix();
    [
        "screen_bid",
        "screen_ask",
        "screen_bid_size",
        "screen_ask_size",
        "screen_quote_age",
        "screen_trailing_spread",
        "forward_bid_at_window",
        "forward_ask_at_window",
    ]
    .map(String::from)
    .into_iter()
    .chain([format!("raw_{suffix}"), format!("forward_spread_{suffix}")])
    .chain(sides_and_bounds().flat_map(|(side, bound)| {
        ["fill_seconds", "entry_price"].map(|label| label_name(side, label, bound))
    }))
    .collect()
}

fn filled_names() -> Vec<String> {
    sides_and_bounds()
        .map(|(side, bound)| label_name(side, "filled", bound))
        .collect()
}

/// The arrival column in either unit DuckDB writes.
enum Arrivals<'a> {
    Microseconds(&'a TimestampMicrosecondArray),
    Nanoseconds(&'a TimestampNanosecondArray),
}

/// One batch's columns, each present and of its type.
struct Columns<'a> {
    batch: &'a RecordBatch,
    arrivals: Arrivals<'a>,
    horizon: Horizon,
}

fn typed<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
    expected: &'static str,
) -> Result<&'a T, EventRowsRefusal> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| EventRowsRefusal::Absent { name: name.into() })?;
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| EventRowsRefusal::Type {
            name: name.into(),
            expected,
            found: array.data_type().to_string(),
        })
}

impl<'a> Columns<'a> {
    fn new(batch: &'a RecordBatch, horizon: Horizon) -> Result<Self, EventRowsRefusal> {
        for name in ["session", "ticker"] {
            typed::<StringArray>(batch, name, "a string")?;
        }
        for name in float_names(horizon) {
            typed::<Float64Array>(batch, &name, "a double")?;
        }
        for name in filled_names() {
            typed::<BooleanArray>(batch, &name, "a boolean")?;
        }
        let arrivals = match typed::<TimestampNanosecondArray>(batch, "arrival", "a timestamp") {
            Ok(nanoseconds) => Arrivals::Nanoseconds(nanoseconds),
            Err(_) => Arrivals::Microseconds(typed(batch, "arrival", "a timestamp")?),
        };
        Ok(Self {
            batch,
            arrivals,
            horizon,
        })
    }

    fn typed<T: Array + 'static>(&self, name: &str) -> &'a T {
        typed(self.batch, name, "").expect("every column is checked when the batch is read")
    }

    /// The value, `None` when null, refused when not finite so no cast below saturates.
    fn float(&self, name: &str, row: usize) -> Result<Option<f64>, RowProblem> {
        let array = self.typed::<Float64Array>(name);
        match array.is_valid(row).then(|| array.value(row)) {
            Some(value) if !value.is_finite() => Err(RowProblem::NotFinite {
                name: name.into(),
                value,
            }),
            value => Ok(value),
        }
    }

    fn string(&self, name: &str, row: usize) -> Result<&'a str, RowProblem> {
        let array = self.typed::<StringArray>(name);
        required(array.is_valid(row).then(|| array.value(row)), name)
    }

    fn filled(&self, side: Side, bound: FillBound, row: usize) -> Option<bool> {
        let array = self.typed::<BooleanArray>(&label_name(side, "filled", bound));
        array.is_valid(row).then(|| array.value(row))
    }

    fn arrival(&self, row: usize) -> Result<DateTime<Utc>, RowProblem> {
        match self.arrivals {
            Arrivals::Microseconds(array) => {
                let microseconds =
                    required(array.is_valid(row).then(|| array.value(row)), "arrival")?;
                DateTime::from_timestamp_micros(microseconds)
                    .ok_or(RowProblem::Arrival { microseconds })
            }
            Arrivals::Nanoseconds(array) => required(
                array
                    .is_valid(row)
                    .then(|| DateTime::from_timestamp_nanos(array.value(row))),
                "arrival",
            ),
        }
    }

    fn required_float(&self, name: &str, row: usize) -> Result<f64, RowProblem> {
        required(self.float(name, row)?, name)
    }

    fn price(&self, name: &str, row: usize) -> Result<Price, RowProblem> {
        let dollars = self.required_float(name, row)?;
        Price::from_dollars(dollars).map_err(|refusal| RowProblem::Price {
            name: name.into(),
            refusal,
        })
    }

    fn shares(&self, name: &str, row: usize) -> Result<Shares, RowProblem> {
        let count = self.required_float(name, row)?;
        Shares::from_float(count).map_err(|refusal| RowProblem::Shares {
            name: name.into(),
            refusal,
        })
    }
}

/// `value`, or the null it is named by.
fn required<T>(value: Option<T>, name: &str) -> Result<T, RowProblem> {
    value.ok_or_else(|| RowProblem::Null { name: name.into() })
}

fn nanoseconds(seconds: f64) -> TimeDelta {
    TimeDelta::nanoseconds((seconds * 1e9).round() as i64)
}

/// The exit quote a mid's return and a spread in basis points of the exit mid imply, from the arrival quote whose bid
/// and ask in millionths sum to `doubled_arrival`.
fn exit_quote(
    doubled_arrival: i64,
    raw: f64,
    spread_basis_points: f64,
) -> Result<ExitQuote, RowProblem> {
    let doubled = doubled_arrival as f64 * (1.0 + raw / 10_000.0);
    let (doubled_mid, spread) = (
        doubled.round() as i64,
        (spread_basis_points / 20_000.0 * doubled).round() as i64,
    );
    if (doubled_mid - spread) % 2 != 0 {
        return Err(RowProblem::ExitOffGrid {
            doubled_mid,
            spread,
        });
    }
    let quoted = |ticks: i64| {
        Price::from_ticks(ticks).map_err(|refusal| RowProblem::Price {
            name: "the exit".into(),
            refusal,
        })
    };
    ExitQuote::new(
        quoted((doubled_mid - spread) / 2)?,
        quoted((doubled_mid + spread) / 2)?,
    )
    .map_err(RowProblem::Exit)
}

/// The session's mean relative spread so far in basis points, as millionths of the arrival mid rounded to the nearest.
fn trailing_spread(basis_points: f64, doubled_arrival: i64) -> Result<Spread, RowProblem> {
    let ticks = (basis_points * doubled_arrival as f64 / 20_000.0).round();
    // `u64::MAX as f64` rounds up to 2^64, so the bound is strict.
    match basis_points >= 0.0 && (0.0..u64::MAX as f64).contains(&ticks) {
        true => Ok(Spread::from_ticks(ticks as u64)),
        false => Err(RowProblem::TrailingSpread { basis_points }),
    }
}

/// The exit at the columns' horizon; a horizon after arrival past the last read has no quote.
fn exit(
    columns: &Columns,
    doubled_arrival: i64,
    row: usize,
) -> Result<Result<ExitQuote, ExitUnread>, RowProblem> {
    let suffix = columns.horizon.suffix();
    let (raw, spread) = (format!("raw_{suffix}"), format!("forward_spread_{suffix}"));
    match (
        columns.float(&raw, row)?,
        columns.float(&spread, row)?,
        columns.horizon,
    ) {
        (None, None, Horizon::After(_)) => Ok(Err(ExitUnread::PastLastRead)),
        (Some(raw), Some(spread), Horizon::After(_) | Horizon::Close) => {
            exit_quote(doubled_arrival, raw, spread).map(Ok)
        }
        (None, Some(_) | None, Horizon::After(_) | Horizon::Close) => {
            Err(RowProblem::Null { name: raw })
        }
        (Some(_), None, Horizon::After(_) | Horizon::Close) => {
            Err(RowProblem::Null { name: spread })
        }
    }
}

/// The resting labels: unknown when all four are null, as when the window passes the last read.
fn resting(
    columns: &Columns,
    row: usize,
) -> Result<Result<RestingLabels, UnknownCause>, RowProblem> {
    let mut fills = Vec::new();
    for (side, bound) in sides_and_bounds() {
        let after = match columns.filled(side, bound, row) {
            Some(true) => Some(Some(nanoseconds(
                columns.required_float(&label_name(side, "fill_seconds", bound), row)?,
            ))),
            Some(false) => Some(None),
            None => None,
        };
        fills.push((side, bound, after));
    }
    let known = fills.iter().filter(|(_, _, after)| after.is_some()).count();
    match known {
        0 => Ok(Err(UnknownCause::WindowPastLastRead)),
        4 => RestingLabels::new(
            columns.price("forward_bid_at_window", row)?,
            columns.price("forward_ask_at_window", row)?,
            |side, bound| {
                fills
                    .iter()
                    .find(|(each_side, each_bound, _)| (*each_side, *each_bound) == (side, bound))
                    .and_then(|(_, _, after)| after.flatten())
            },
        )
        .map(Ok)
        .map_err(RowProblem::Labels),
        known => Err(RowProblem::PartlyUnknown { known }),
    }
}

/// One row: an event row, or an untouched one.
fn decode(columns: &Columns, row: usize) -> Result<Result<EventRow, Untouched>, RowProblem> {
    let symbol = Symbol::new(columns.string("ticker", row)?).map_err(RowProblem::Symbol)?;
    let arrival = columns.arrival(row)?;
    let (written, derived) = (columns.string("session", row)?, SessionDate::at(arrival));
    if written != derived.to_string() {
        return Err(RowProblem::Session {
            written: written.into(),
            derived,
        });
    }
    if columns.float("screen_bid", row)?.is_none() && columns.float("screen_ask", row)?.is_none() {
        return Ok(Err(Untouched { symbol, arrival }));
    }
    let (bid, ask) = (
        columns.price("screen_bid", row)?,
        columns.price("screen_ask", row)?,
    );
    let touch = StandingQuote::new(
        arrival - nanoseconds(columns.required_float("screen_quote_age", row)?),
        bid,
        ask,
        columns.shares("screen_bid_size", row)?,
        columns.shares("screen_ask_size", row)?,
    )
    .map_err(RowProblem::Touch)?;
    let doubled_arrival = bid.ticks() + ask.ticks();
    let event = EventRow::new(
        symbol,
        arrival,
        touch,
        trailing_spread(
            columns.required_float("screen_trailing_spread", row)?,
            doubled_arrival,
        )?,
        exit(columns, doubled_arrival, row)?,
        resting(columns, row)?,
    )
    .map_err(RowProblem::TouchAfterArrival)?;
    for (side, bound) in sides_and_bounds() {
        let implied = match event.resting(side, bound) {
            Resting::Filled { price, .. } => price,
            Resting::Unfilled { cross_at_window } => cross_at_window,
            Resting::Unknown(_) => continue,
        };
        let name = label_name(side, "entry_price", bound);
        let written = columns.price(&name, row)?;
        if written != implied {
            return Err(RowProblem::EntryPrice {
                name,
                written,
                implied,
            });
        }
    }
    Ok(Ok(event))
}

/// Every row of the event table file `bytes` read at `horizon`, in the file's order; a row with no quote at arrival
/// is `Untouched`.
pub fn event_rows(
    bytes: bytes::Bytes,
    horizon: Horizon,
) -> Result<Vec<Result<EventRow, Untouched>>, EventRowsRefusal> {
    let parquet = |error: &dyn std::fmt::Display| EventRowsRefusal::Parquet {
        reason: error.to_string(),
    };
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|error| parquet(&error))?;
    let names: Vec<String> = ["session", "ticker", "arrival"]
        .map(String::from)
        .into_iter()
        .chain(float_names(horizon))
        .chain(filled_names())
        .collect();
    let mask = ProjectionMask::columns(builder.parquet_schema(), names.iter().map(String::as_str));
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|error| parquet(&error))?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| parquet(&error))?;
        let columns = Columns::new(&batch, horizon)?;
        for row in 0..batch.num_rows() {
            let decoded = decode(&columns, row).map_err(|problem| EventRowsRefusal::Row {
                row: rows.len(),
                problem,
            })?;
            rows.push(decoded);
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_the_trailing_spread_is_priced_at_the_arrival_mid() {
        // Twenty basis points of a 100.01 mid is 200,020 millionths.
        assert_eq!(
            trailing_spread(20.0, 200_020_000),
            Ok(Spread::from_ticks(200_020))
        );
        assert_eq!(
            trailing_spread(-0.5, 200_020_000),
            Err(RowProblem::TrailingSpread { basis_points: -0.5 })
        );
        assert_eq!(
            trailing_spread(1e20, 200_000_000),
            Err(RowProblem::TrailingSpread { basis_points: 1e20 })
        );
        // A negative spread too small to round to a tick is still refused.
        assert_eq!(
            trailing_spread(-1e-9, 200_000_000),
            Err(RowProblem::TrailingSpread {
                basis_points: -1e-9
            })
        );
    }

    #[test]
    fn test_a_non_finite_double_is_refused() {
        use arrow_array::ArrayRef;
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter([
            (
                "screen_bid",
                Arc::new(Float64Array::from(vec![f64::INFINITY, f64::NAN])) as ArrayRef,
            ),
            (
                "arrival",
                Arc::new(TimestampMicrosecondArray::from(vec![0_i64, 0])) as ArrayRef,
            ),
        ])
        .unwrap();
        let columns = Columns {
            batch: &batch,
            arrivals: Arrivals::Microseconds(typed(&batch, "arrival", "a timestamp").unwrap()),
            horizon: Horizon::Close,
        };
        assert_eq!(
            columns.float("screen_bid", 0),
            Err(RowProblem::NotFinite {
                name: "screen_bid".into(),
                value: f64::INFINITY,
            })
        );
        // NaN never equals itself, so the NaN row is matched by shape.
        assert!(matches!(
            columns.float("screen_bid", 1),
            Err(RowProblem::NotFinite { value, .. }) if value.is_nan()
        ));
    }

    #[test]
    fn test_the_exit_is_rebuilt_from_its_return_and_spread() {
        // A 100.00 by 100.02 arrival, a mid up ten basis points to 100.11 and a two-cent spread on it.
        let raw = 10_000.0 * (100.11 / 100.01 - 1.0);
        let spread = 10_000.0 * 0.02 / 100.11;
        let exit = exit_quote(200_020_000, raw, spread).unwrap();
        assert_eq!(
            (exit.bid().ticks(), exit.ask().ticks()),
            (100_100_000, 100_120_000)
        );
        assert_eq!(
            exit_quote(200_020_000, raw, 10_000.0 * 0.000_001 / 100.11),
            Err(RowProblem::ExitOffGrid {
                doubled_mid: 200_220_000,
                spread: 1
            })
        );
    }
}
