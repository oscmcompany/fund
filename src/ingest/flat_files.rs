//! Massive's flat files: one gzipped CSV per dataset per session, parsed row by row from any reader of its bytes.

use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::massive::{alpaca_symbol, is_exchange_test_ticker};
use super::{
    OneSidedQuote, ParseRefusal, PrintOrigin, QuoteOrigin, RefusedRow, RowRefusal, priced_side,
};
use crate::common::market::record::Quote;
use crate::common::market::trade_bars::{ConditionCode, Correction, Print};
use crate::common::market::{Price, Shares};

/// A flat quote file's row; codes and lists come comma-joined in one field each.
#[derive(Deserialize)]
struct QuoteRow {
    ticker: String,
    bid_price: f64,
    bid_size: f64,
    ask_price: f64,
    ask_size: f64,
    /// Nanoseconds since the epoch at which the SIP published the quote.
    sip_timestamp: i64,
    bid_exchange: String,
    ask_exchange: String,
    tape: String,
    #[serde(default)]
    conditions: String,
    #[serde(default)]
    indicators: String,
    /// Nanoseconds since the epoch at which the venue stamped the quote, zero when unstamped.
    participant_timestamp: Option<i64>,
    sequence_number: Option<u64>,
}

/// What one quote row became.
#[derive(Debug, Clone, PartialEq)]
pub enum QuoteRowOutcome {
    Quote {
        quote: Quote,
        origin: QuoteOrigin,
    },
    TestTicker,
    /// A side with no price, which is no top of book; the quote standing before it keeps standing.
    OneSided {
        quote: OneSidedQuote,
        origin: QuoteOrigin,
    },
    Refused(RefusedRow),
}

/// Reads a gzipped flat quote file from `gzipped`, handing each row's outcome to `each` in file order.
pub fn read_quotes(
    gzipped: impl std::io::Read,
    mut each: impl FnMut(QuoteRowOutcome),
) -> Result<(), ParseRefusal> {
    let mut reader = csv::Reader::from_reader(flate2::read::GzDecoder::new(gzipped));
    for row in reader.deserialize::<QuoteRow>() {
        let row = row.map_err(|error| ParseRefusal::Malformed {
            line: error.position().map(csv::Position::line),
            reason: error.to_string(),
        })?;
        each(quote_outcome(row));
    }
    Ok(())
}

fn quote_outcome(row: QuoteRow) -> QuoteRowOutcome {
    if is_exchange_test_ticker(&row.ticker) {
        return QuoteRowOutcome::TestTicker;
    }
    let refused = |cause: RowRefusal| {
        QuoteRowOutcome::Refused(RefusedRow {
            ticker: row.ticker.clone(),
            cause,
        })
    };
    let symbol = match alpaca_symbol(&row.ticker) {
        Ok(symbol) => symbol,
        Err(cause) => return refused(RowRefusal::Symbol(cause)),
    };
    let origin = QuoteOrigin {
        bid_exchange: Some(row.bid_exchange.clone()),
        ask_exchange: Some(row.ask_exchange.clone()),
        tape: Some(row.tape.clone()),
        conditions: listed(&row.conditions),
        indicators: listed(&row.indicators),
        participant_timestamp: stamped(row.participant_timestamp),
        sequence: row.sequence_number,
    };
    let timestamp = DateTime::from_timestamp_nanos(row.sip_timestamp);
    match priced_side((row.bid_price, row.bid_size), (row.ask_price, row.ask_size)) {
        Some(Ok(priced)) => {
            return QuoteRowOutcome::OneSided {
                quote: OneSidedQuote {
                    symbol,
                    timestamp,
                    priced,
                },
                origin,
            };
        }
        Some(Err(cause)) => return refused(cause),
        None => {}
    }
    let price = |dollars: f64| Price::from_dollars(dollars).map_err(RowRefusal::Price);
    let size = |shares: f64| Shares::from_float(shares).map_err(RowRefusal::Shares);
    let parts = (|| {
        Ok::<_, RowRefusal>((
            price(row.bid_price)?,
            price(row.ask_price)?,
            size(row.bid_size)?,
            size(row.ask_size)?,
        ))
    })();
    match parts {
        Ok((bid, ask, bid_size, ask_size)) => {
            match Quote::new(symbol, timestamp, bid, ask, bid_size, ask_size) {
                Ok(quote) => QuoteRowOutcome::Quote { quote, origin },
                Err(cause) => refused(RowRefusal::Quote(cause)),
            }
        }
        Err(cause) => refused(cause),
    }
}

/// A flat trade file's row; the condition codes come comma-joined in one field.
#[derive(Deserialize)]
struct TradeRow {
    ticker: String,
    conditions: String,
    /// The SIP's correction indicator, zero or blank for a regular print.
    correction: Option<u32>,
    price: f64,
    size: f64,
    /// Nanoseconds since the epoch at which the SIP published the print.
    sip_timestamp: i64,
    exchange: String,
    id: String,
    tape: String,
    /// Nanoseconds since the epoch at which the venue stamped the print, zero when unstamped.
    participant_timestamp: Option<i64>,
    /// Nanoseconds since the epoch at which a trade reporting facility stamped the print, zero when none did.
    trf_timestamp: Option<i64>,
    sequence_number: Option<u64>,
}

/// What one trade row became.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeRowOutcome {
    Print {
        print: Print,
        conditions: Vec<ConditionCode>,
        correction: Correction,
        origin: PrintOrigin,
    },
    TestTicker,
    Refused(RefusedRow),
}

/// Reads a gzipped flat trade file from `gzipped`, handing each row's outcome to `each` in file order.
pub fn read_trades(
    gzipped: impl std::io::Read,
    mut each: impl FnMut(TradeRowOutcome),
) -> Result<(), ParseRefusal> {
    let mut reader = csv::Reader::from_reader(flate2::read::GzDecoder::new(gzipped));
    for row in reader.deserialize::<TradeRow>() {
        let row = row.map_err(|error| ParseRefusal::Malformed {
            line: error.position().map(csv::Position::line),
            reason: error.to_string(),
        })?;
        each(trade_outcome(row));
    }
    Ok(())
}

fn trade_outcome(row: TradeRow) -> TradeRowOutcome {
    if is_exchange_test_ticker(&row.ticker) {
        return TradeRowOutcome::TestTicker;
    }
    let refused = |cause: RowRefusal| {
        TradeRowOutcome::Refused(RefusedRow {
            ticker: row.ticker.clone(),
            cause,
        })
    };
    let conditions: Result<Vec<ConditionCode>, _> = row
        .conditions
        .split(',')
        .map(str::trim)
        .filter(|code| !code.is_empty())
        .map(|code| code.parse().map(ConditionCode::new))
        .collect();
    let Ok(conditions) = conditions else {
        return refused(RowRefusal::Conditions {
            raw: row.conditions.clone(),
        });
    };
    let symbol = match alpaca_symbol(&row.ticker) {
        Ok(symbol) => symbol,
        Err(cause) => return refused(RowRefusal::Symbol(cause)),
    };
    let price = match Price::from_dollars(row.price) {
        Ok(price) => price,
        Err(cause) => return refused(RowRefusal::Price(cause)),
    };
    let size = match Shares::from_float(row.size) {
        Ok(size) => size,
        Err(cause) => return refused(RowRefusal::Shares(cause)),
    };
    let timestamp = DateTime::from_timestamp_nanos(row.sip_timestamp);
    let correction = match correction(row.correction) {
        Ok(correction) => correction,
        Err(cause) => return refused(cause),
    };
    let print = Print::new(symbol, timestamp, price, size);
    let origin = PrintOrigin {
        exchange: Some(row.exchange),
        tape: row.tape,
        id: Some(row.id),
        participant_timestamp: stamped(row.participant_timestamp),
        trade_reporting_facility_timestamp: stamped(row.trf_timestamp),
        sequence: row.sequence_number,
    };
    TradeRowOutcome::Print {
        print,
        conditions,
        correction,
        origin,
    }
}

/// A nanosecond stamp, where zero or blank means the vendor stamped nothing.
fn stamped(nanoseconds: Option<i64>) -> Option<DateTime<Utc>> {
    nanoseconds
        .filter(|nanoseconds| *nanoseconds != 0)
        .map(DateTime::from_timestamp_nanos)
}

/// A comma-joined field's elements, with blanks left out.
fn listed(joined: &str) -> Vec<String> {
    joined
        .split(',')
        .map(str::trim)
        .filter(|element| !element.is_empty())
        .map(str::to_string)
        .collect()
}

/// Reads the SIP's correction indicator: 12 is the record that replaces a corrected print; 1 marks an original later
/// corrected, 8 one later canceled and 10 the cancel's own record; any other code is refused with itself.
fn correction(indicator: Option<u32>) -> Result<Correction, RowRefusal> {
    match indicator {
        None | Some(0 | 12) => Ok(Correction::Stands),
        Some(1 | 8 | 10) => Ok(Correction::Withdrawn),
        Some(code) => Err(RowRefusal::Correction {
            raw: code.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::PricedSide;

    fn gzipped(text: &str) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(text.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn test_each_quote_row_is_a_quote_a_test_ticker_one_sided_or_refused() {
        // Massive's quote header with invented rows for 2021-08-23 and 2026-09-18: a quote, an empty book, a crossed, a
        // negative-bid and a test row.
        let rows = "ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,indicators,participant_timestamp,sequence_number,sip_timestamp,tape,trf_timestamp
ABC,8,51.0,100,11,50.25,100,\"1,81\",,1629716400001234500,1001,1629716400012345600,1,0
ABC,12,0.0,0,12,0.0,0,\"1,81\",,1789715092000000123,2,1789715092000000456,1,0
ABFpA,11,60.00,200,8,60.25,100,\"1,81\",,1629716446000000256,2001,1629716446000000512,1,0
ABC,8,51.0,100,11,-1.0,100,\"1,81\",,1629716446000000256,2002,1629716446000000512,1,0
ZTST,11,10.0,100,8,9.0,100,\"1,81\",,1629716446000000256,2001,1629716446000000512,1,0
";
        let mut outcomes = Vec::new();
        read_quotes(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        assert_eq!(outcomes.len(), 5);
        match &outcomes[0] {
            QuoteRowOutcome::Quote { quote, origin } => {
                assert_eq!(quote.symbol().as_str(), "ABC");
                assert_eq!(quote.bid().ticks(), 50_250_000);
                assert_eq!(quote.ask_size().units(), 100_000_000);
                assert_eq!(
                    quote.timestamp().to_rfc3339(),
                    "2021-08-23T11:00:00.012345600+00:00"
                );
                assert_eq!(
                    origin,
                    &QuoteOrigin {
                        bid_exchange: Some("11".to_string()),
                        ask_exchange: Some("8".to_string()),
                        tape: Some("1".to_string()),
                        conditions: vec!["1".to_string(), "81".to_string()],
                        indicators: vec![],
                        participant_timestamp: Some(DateTime::from_timestamp_nanos(
                            1_629_716_400_001_234_500
                        )),
                        sequence: Some(1001),
                    }
                );
            }
            other @ (QuoteRowOutcome::TestTicker
            | QuoteRowOutcome::OneSided { .. }
            | QuoteRowOutcome::Refused(_)) => panic!("{other:?}"),
        }
        match &outcomes[1] {
            QuoteRowOutcome::OneSided { quote, .. } => {
                assert_eq!(quote.symbol.as_str(), "ABC");
                assert_eq!(quote.priced, PricedSide::Neither);
            }
            other @ (QuoteRowOutcome::Quote { .. }
            | QuoteRowOutcome::TestTicker
            | QuoteRowOutcome::Refused(_)) => panic!("{other:?}"),
        }
        assert!(matches!(
            &outcomes[2],
            QuoteRowOutcome::Refused(row) if row.ticker() == "ABFpA"
        ));
        // A negative bid is a bad price, not a missing side, as Alpaca's rows read it.
        assert!(matches!(
            &outcomes[3],
            QuoteRowOutcome::Refused(row)
                if row.ticker() == "ABC" && <&'static str>::from(row.cause().kind()) == "price"
        ));
        assert_eq!(outcomes[4], QuoteRowOutcome::TestTicker);
    }

    #[test]
    fn test_each_trade_row_carries_its_conditions_and_correction() {
        // Massive's trade header with invented rows for 2026-09-18: a print, a late corrected copy, a refusal and an unsized print.
        let rows = "ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp
ABC,\"12,37\",0,4,70000000000001,1789718400000000123,50.250000,1001,1789718400000000456,10.000000,1,202,1789718400000000400
ABC,,1,4,70000000000002,1789706400000000000,50.000000,1002,1789718406000000123,0.002500,1,202,1789718406000000100
ABC,\"12,x\",0,4,70000000000003,1789706400000000500,50.000000,1003,1789718406000000789,0.500000,1,202,1789718406000000700
ABC,,,4,70000000000004,1789706400000000500,50.000000,1004,1789718406000000789,0,1,202,1789718406000000700
";
        let mut outcomes = Vec::new();
        read_trades(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        assert_eq!(outcomes.len(), 4);
        match &outcomes[0] {
            TradeRowOutcome::Print {
                print: Print::Trade(trade),
                conditions,
                correction,
                origin,
            } => {
                assert_eq!(
                    origin,
                    &PrintOrigin {
                        exchange: Some("4".to_string()),
                        tape: "1".to_string(),
                        id: Some("70000000000001".to_string()),
                        participant_timestamp: Some(DateTime::from_timestamp_nanos(
                            1_789_718_400_000_000_123
                        )),
                        trade_reporting_facility_timestamp: Some(DateTime::from_timestamp_nanos(
                            1_789_718_400_000_000_400
                        )),
                        sequence: Some(1001),
                    }
                );
                assert_eq!(trade.price().ticks(), 50_250_000);
                assert_eq!(trade.size().units(), 10_000_000);
                assert_eq!(
                    conditions,
                    &[ConditionCode::new(12), ConditionCode::new(37)]
                );
                assert_eq!(correction, &Correction::Stands);
            }
            other @ (TradeRowOutcome::Print { .. }
            | TradeRowOutcome::TestTicker
            | TradeRowOutcome::Refused(_)) => panic!("{other:?}"),
        }
        assert!(matches!(
            &outcomes[1],
            TradeRowOutcome::Print { conditions, correction: Correction::Withdrawn, .. }
                if conditions.is_empty()
        ));
        assert!(matches!(
            &outcomes[2],
            TradeRowOutcome::Refused(row) if <&'static str>::from(row.cause().kind()) == "conditions"
        ));
        assert!(matches!(
            &outcomes[3],
            TradeRowOutcome::Print {
                print: Print::Unsized { .. },
                ..
            }
        ));
    }

    /// A zero or blank stamp is no stamp rather than the epoch, and a joined list drops its blank elements.
    #[test]
    fn test_an_unstamped_time_is_absent_and_blank_codes_are_dropped() {
        assert_eq!(stamped(Some(0)), None);
        assert_eq!(stamped(None), None);
        assert_eq!(
            stamped(Some(1_789_718_400_000_000_123)).map(|instant| instant.to_rfc3339()),
            Some("2026-09-18T08:00:00.000000123+00:00".to_string())
        );
        assert_eq!(listed(""), Vec::<String>::new());
        assert_eq!(listed("1, 81,,"), ["1", "81"]);
    }

    #[test]
    fn test_a_correction_record_stands_and_what_it_replaces_or_cancels_is_withdrawn() {
        // An invented 1,000,000-share print on 2025-11-04 and the later record that replaces it, in Massive's shape.
        let rows = "ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp
ABC,\"53,32,35,41\",1,4,1,1762272000500000000,6.000000,1,1762272000500000123,1000000,3,202,1762272000500000000
ABC,\"53,35,41\",12,4,2,1762286400250000000,6.000000,2,1762286400250000456,1000000,3,202,1762286400250000000
";
        let mut outcomes = Vec::new();
        read_trades(gzipped(rows).as_slice(), |outcome| outcomes.push(outcome)).unwrap();
        let corrections: Vec<_> = outcomes
            .iter()
            .map(|outcome| match outcome {
                TradeRowOutcome::Print { correction, .. } => *correction,
                other @ (TradeRowOutcome::TestTicker | TradeRowOutcome::Refused(_)) => {
                    panic!("{other:?}")
                }
            })
            .collect();
        assert_eq!(corrections, [Correction::Withdrawn, Correction::Stands]);
        let read = [None, Some(0), Some(1), Some(8), Some(10), Some(12), Some(7)].map(correction);
        assert_eq!(
            read,
            [
                Ok(Correction::Stands),
                Ok(Correction::Stands),
                Ok(Correction::Withdrawn),
                Ok(Correction::Withdrawn),
                Ok(Correction::Withdrawn),
                Ok(Correction::Stands),
                Err(RowRefusal::Correction {
                    raw: "7".to_string()
                }),
            ]
        );
    }
}
