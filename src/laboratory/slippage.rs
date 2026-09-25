//! Implementation shortfall on the trader's own entries: each leg's fill against its decision price.
//!
//! Read off `pair_opened` records, which have carried both prices since the journal began.

use std::collections::BTreeMap;

use crate::common::alpaca::OrderSide;
use crate::common::journal::{Observation, ReadRecord};
use crate::common::types::{SessionDate, Ticker};
use crate::laboratory::metrics::{summarize, Distribution};
use rust_decimal::prelude::ToPrimitive;

/// One leg of one opened pair, with what its fill cost against the price the decision was made at.
#[derive(Debug, Clone, PartialEq)]
pub struct LegSlippage {
    pub session: SessionDate,
    pub ticker: Ticker,
    pub side: OrderSide,
    pub decision_price: f64,
    pub fill_price: f64,
    /// Positive is a cost: a buy filled above, or a sell below, its decision price. `None` where the
    /// decision price cannot divide, which is counted rather than read as free.
    pub cost_basis_points: Option<f64>,
}

/// The signed cost of filling at `fill` when the decision was made at `decision`.
pub fn cost_basis_points(side: OrderSide, decision: f64, fill: f64) -> Option<f64> {
    if !(decision.is_finite() && decision > 0.0 && fill.is_finite()) {
        return None;
    }
    let paid = match side {
        OrderSide::Buy => fill - decision,
        OrderSide::Sell | OrderSide::SellShort => decision - fill,
    };
    Some(paid / decision * 10_000.0)
}

/// Both legs of every `pair_opened` record, in the order they were read.
///
/// The long leg bought and the short leg sold, which is what `open_pair` submits.
pub fn legs(records: &[ReadRecord]) -> Vec<LegSlippage> {
    let mut legs = Vec::new();
    for record in records {
        let crate::common::journal::ReadLine::Read(record) = record else {
            continue;
        };
        let Observation::PairOpened(opened) = &record.observation else {
            continue;
        };
        for (ticker, side, decision, fill) in [
            (
                &opened.long_ticker,
                OrderSide::Buy,
                opened.long_decision_price,
                opened.long_fill_price,
            ),
            (
                &opened.short_ticker,
                OrderSide::Sell,
                opened.short_decision_price,
                opened.short_fill_price,
            ),
        ] {
            let fill = fill.to_f64().unwrap_or(f64::NAN);
            legs.push(LegSlippage {
                session: record.session_date,
                ticker: ticker.clone(),
                side,
                decision_price: decision,
                fill_price: fill,
                cost_basis_points: cost_basis_points(side, decision, fill),
            });
        }
    }
    legs
}

/// What the legs cost, summarized over sessions rather than legs.
///
/// Legs in one session share a pass and a market, so the standard error is taken across sessions:
/// counting legs as independent would claim more precision than the record holds.
#[derive(Debug, Clone, PartialEq)]
pub struct SlippageSummary {
    pub legs: usize,
    /// Legs whose decision price could not be divided by, reported beside the estimate.
    pub undefined: usize,
    pub sessions: usize,
    /// The mean of each session's mean leg cost, with its error across sessions.
    pub per_session: Option<Distribution>,
    /// Each session's mean cost and how many legs it rests on.
    pub by_session: BTreeMap<SessionDate, (f64, usize)>,
    /// Each name's mean cost and how many legs it rests on.
    pub by_ticker: BTreeMap<String, (f64, usize)>,
}

pub fn summarize_legs(legs: &[LegSlippage]) -> SlippageSummary {
    let mut by_session: BTreeMap<SessionDate, Vec<f64>> = BTreeMap::new();
    let mut by_ticker: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut undefined = 0;
    for leg in legs {
        let Some(cost) = leg.cost_basis_points else {
            undefined += 1;
            continue;
        };
        by_session.entry(leg.session).or_default().push(cost);
        by_ticker
            .entry(leg.ticker.as_str().to_string())
            .or_default()
            .push(cost);
    }
    let mean = |values: &[f64]| values.iter().sum::<f64>() / values.len() as f64;
    SlippageSummary {
        legs: legs.len(),
        undefined,
        sessions: by_session.len(),
        per_session: summarize(by_session.values().map(|costs| Some(mean(costs)))),
        by_session: by_session
            .iter()
            .map(|(session, costs)| (*session, (mean(costs), costs.len())))
            .collect(),
        by_ticker: by_ticker
            .into_iter()
            .map(|(ticker, costs)| (ticker, (mean(&costs), costs.len())))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_fill_against_the_decision_is_a_cost_on_either_side() {
        let close = |value: Option<f64>, expected: f64| {
            assert!(
                (value.unwrap() - expected).abs() < 1e-9,
                "{value:?} against {expected}"
            )
        };
        close(cost_basis_points(OrderSide::Buy, 100.0, 100.10), 10.0);
        close(cost_basis_points(OrderSide::Sell, 100.0, 99.90), 10.0);
        let improved = cost_basis_points(OrderSide::Buy, 100.0, 99.95).unwrap();
        assert!(
            improved < 0.0,
            "a buy below its decision price is an improvement"
        );
    }

    #[test]
    fn test_a_decision_price_that_cannot_divide_is_undefined_not_free() {
        assert_eq!(cost_basis_points(OrderSide::Buy, 0.0, 10.0), None);
        assert_eq!(cost_basis_points(OrderSide::Sell, f64::NAN, 10.0), None);
    }

    fn leg(session: (i32, u32, u32), ticker: &str, cost: Option<f64>) -> LegSlippage {
        LegSlippage {
            session: SessionDate::from_date(
                chrono::NaiveDate::from_ymd_opt(session.0, session.1, session.2).unwrap(),
            ),
            ticker: Ticker::new(ticker).unwrap(),
            side: OrderSide::Buy,
            decision_price: 10.0,
            fill_price: 10.0,
            cost_basis_points: cost,
        }
    }

    /// Two legs in one session are one observation of that session, so a session with many legs
    /// cannot outvote one with few.
    #[test]
    fn test_the_summary_averages_sessions_not_legs() {
        let summary = summarize_legs(&[
            leg((2026, 8, 18), "AAAA", Some(10.0)),
            leg((2026, 8, 18), "BBBB", Some(10.0)),
            leg((2026, 8, 18), "CCCC", Some(10.0)),
            leg((2026, 8, 19), "AAAA", Some(2.0)),
            leg((2026, 8, 19), "DDDD", None),
        ]);
        assert_eq!(summary.legs, 5);
        assert_eq!(summary.undefined, 1);
        assert_eq!(summary.sessions, 2);
        let per_session = summary.per_session.expect("two sessions summarize");
        assert_eq!(per_session.mean, 6.0);
        assert_eq!(per_session.sessions, 2);
        assert_eq!(summary.by_ticker["AAAA"], (6.0, 2));
        assert_eq!(
            summary
                .by_session
                .values()
                .map(|(_, legs)| *legs)
                .sum::<usize>(),
            4
        );
    }
}
