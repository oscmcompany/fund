//! A result's Sharpe on excess returns, the index's over exactly the same sessions beside it, and the targets a result
//! is gated on, so no Sharpe is read without the market's over the same days.

use std::collections::BTreeMap;

use crate::common::laboratory::score::equal_up_to_rounding;
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::market::Price;
use crate::common::market::Symbol;
use crate::common::market::record::Partition;
use crate::common::market::trade_bars::TradeBar;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

/// Sessions in a trading year, the scale an annualized Sharpe takes.
pub const SESSIONS_PER_YEAR: f64 = 252.0;

/// The fewest sessions a Sharpe is read over, as settled before any result was known.
pub const MINIMUM_SESSIONS: usize = 60;

/// How far a result's Sharpe must clear the index's over the same sessions.
pub const SHARPE_MARGIN_OVER_INDEX: f64 = 2.0;

/// The mean daily net return on capital a result must reach, in basis points.
pub const DAILY_RETURN_TARGET_BASIS_POINTS: f64 = 8.0;

/// An annualized Sharpe ratio with the sessions it was read over and how many of the result's sessions had no reading.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sharpe {
    annualized: f64,
    mean_return: f64,
    sessions: usize,
    undefined: usize,
}

impl Sharpe {
    pub fn annualized(self) -> f64 {
        self.annualized
    }

    /// The mean daily return over the sessions read, before the risk-free rate is taken off.
    pub fn mean_return(self) -> f64 {
        self.mean_return
    }

    pub fn sessions(self) -> usize {
        self.sessions
    }

    pub fn undefined(self) -> usize {
        self.undefined
    }
}

/// Why a Sharpe could not be read.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum SharpeRefusal {
    #[error("{count} sessions is fewer than the {minimum} a Sharpe is read over")]
    TooFewSessions { count: usize, minimum: usize },
    /// No risk-free reading for a session with a return, so its excess cannot be taken.
    #[error("no risk-free rate for {session}")]
    RiskFreeMissing { session: SessionDate },
    #[error("the excess returns over {sessions} sessions do not vary")]
    NoDispersion { sessions: usize },
}

/// The Sharpe of `returns` over `risk_free`, both daily: the mean excess over its sample deviation, annualized; a
/// session whose return is unmeasured counts as undefined and is left out.
pub fn sharpe(returns: &Series, risk_free: &Series) -> Result<Sharpe, SharpeRefusal> {
    let mut excess = Vec::new();
    let mut total = 0.0;
    let mut undefined = 0;
    for (session, reading) in returns.readings() {
        let Some(value) = reading else {
            undefined += 1;
            continue;
        };
        match risk_free.readings().get(session).copied().flatten() {
            Some(rate) => {
                excess.push(value - rate);
                total += value;
            }
            None => return Err(SharpeRefusal::RiskFreeMissing { session: *session }),
        }
    }
    let sessions = excess.len();
    if sessions < MINIMUM_SESSIONS {
        return Err(SharpeRefusal::TooFewSessions {
            count: sessions,
            minimum: MINIMUM_SESSIONS,
        });
    }
    let count = sessions as f64;
    let mean = excess.iter().sum::<f64>() / count;
    let variance = excess
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (count - 1.0);
    // Returns that differ only by rounding have a deviation of noise, which would read as an enormous Sharpe.
    if excess
        .iter()
        .all(|value| equal_up_to_rounding(*value, excess[0]))
    {
        return Err(SharpeRefusal::NoDispersion { sessions });
    }
    Ok(Sharpe {
        annualized: mean / variance.sqrt() * SESSIONS_PER_YEAR.sqrt(),
        mean_return: total / count,
        sessions,
        undefined,
    })
}

/// Each session's close-to-close return of `symbol` from daily trade bars; a price return, since dividends are not in
/// the bars, and a session has none unless `calendar` puts the previous entry of `bars` on the trading day before it
/// and both carry a close.
pub fn index_returns(
    bars: &BTreeMap<SessionDate, Partition<TradeBar>>,
    symbol: &Symbol,
    calendar: &TradingCalendar,
) -> Result<Series, SeriesRefusal> {
    let closes: Vec<(SessionDate, Option<Price>)> = bars
        .iter()
        .map(|(session, partition)| {
            let close = partition
                .rows()
                .iter()
                .find(|bar| bar.symbol() == symbol)
                .and_then(|bar| bar.sums().open_close())
                .map(|open_close| open_close.close().price());
            (*session, close)
        })
        .collect();
    Series::new(closes.windows(2).map(|pair| {
        let ((previous_session, previous), (session, close)) = (pair[0], pair[1]);
        // A missing partition would otherwise stretch one return across two sessions.
        let adjacent = calendar.previous_trading_day(session) == Some(previous_session);
        let reading = previous
            .zip(close)
            .filter(|_| adjacent)
            .map(|(previous, close)| close.ticks() as f64 / previous.ticks() as f64 - 1.0);
        (session, reading)
    }))
}

/// A result's Sharpe and the index's over exactly the sessions the result was measured on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BesideIndex {
    result: Sharpe,
    index: Sharpe,
}

/// Why a result could not be set beside the index.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum BesideIndexRefusal {
    #[error("the result's Sharpe: {0}")]
    Result(SharpeRefusal),
    #[error("the index's Sharpe: {0}")]
    Index(SharpeRefusal),
    /// A session the result was measured on that the index has no return for, which would compare different days.
    #[error("the index has no return for {session}")]
    IndexMissing { session: SessionDate },
}

impl BesideIndex {
    pub fn result(self) -> Sharpe {
        self.result
    }

    pub fn index(self) -> Sharpe {
        self.index
    }

    /// The result's Sharpe less the index's.
    pub fn margin(self) -> f64 {
        self.result.annualized - self.index.annualized
    }
}

/// The result's Sharpe and the index's read over exactly the sessions where the result has a reading.
pub fn beside_index(
    returns: &Series,
    index: &Series,
    risk_free: &Series,
) -> Result<BesideIndex, BesideIndexRefusal> {
    let result = sharpe(returns, risk_free).map_err(BesideIndexRefusal::Result)?;
    let mut matched = Vec::new();
    for (session, reading) in returns.readings() {
        if reading.is_none() {
            continue;
        }
        match index.readings().get(session).copied().flatten() {
            Some(value) => matched.push((*session, Some(value))),
            None => return Err(BesideIndexRefusal::IndexMissing { session: *session }),
        }
    }
    let matched = Series::new(matched).expect("each reading was admitted by the index's series");
    let index = sharpe(&matched, risk_free).map_err(BesideIndexRefusal::Index)?;
    Ok(BesideIndex { result, index })
}

/// Whether a reading reaches its target, and by how much it falls short when it does not.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Gate {
    Clears,
    Short { by: f64 },
}

impl Gate {
    fn of(reading: f64, target: f64) -> Self {
        match reading >= target {
            true => Self::Clears,
            false => Self::Short {
                by: target - reading,
            },
        }
    }
}

/// A result against both targets; a target is a gate a result passes, never a quantity to search toward.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Targets {
    pub sharpe_margin: Gate,
    pub daily_return: Gate,
}

/// The result's Sharpe margin over the index and its mean daily return on capital, each against its target.
pub fn against_targets(beside: BesideIndex) -> Targets {
    Targets {
        sharpe_margin: Gate::of(beside.margin(), SHARPE_MARGIN_OVER_INDEX),
        daily_return: Gate::of(
            beside.result.mean_return * 10_000.0,
            DAILY_RETURN_TARGET_BASIS_POINTS,
        ),
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::record::BarInterval;
    use crate::common::market::trade_bars::{OpenClose, TradeSums};
    use crate::common::market::{DollarVolume, Shares, StampedPrice, TradeCount};
    use crate::common::time::SessionRange;
    use crate::common::time::calendar::TradingSession;

    fn session(offset: u64) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap() + chrono::Days::new(offset),
        )
    }

    fn series(readings: &[f64]) -> Series {
        Series::new(
            readings
                .iter()
                .enumerate()
                .map(|(index, value)| (session(index as u64), Some(*value))),
        )
        .unwrap()
    }

    fn constant(rate: f64, sessions: usize) -> Series {
        series(&vec![rate; sessions])
    }

    /// Sixty sessions alternating +20bp and 0.
    fn alternating() -> Vec<f64> {
        (0..60)
            .map(|index| if index % 2 == 0 { 0.002 } else { 0.0 })
            .collect()
    }

    #[test]
    fn test_a_sharpe_is_the_mean_excess_over_its_sample_deviation_annualized() {
        // Mean 10bp, each reading 10bp from it: deviation 0.001 × √(60/59), so the Sharpe is √(252 × 59 / 60).
        let read = sharpe(&series(&alternating()), &constant(0.0, 60)).unwrap();
        assert!(
            (read.annualized() - 15.74166446091391).abs() < 1e-9,
            "{read:?}"
        );
        assert!((read.mean_return() - 0.001).abs() < 1e-15);
        assert_eq!((read.sessions(), read.undefined()), (60, 0));
    }

    #[test]
    fn test_a_sharpe_refuses_what_it_cannot_read() {
        assert_eq!(
            sharpe(&series(&alternating()[..59]), &constant(0.0, 59)),
            Err(SharpeRefusal::TooFewSessions {
                count: 59,
                minimum: 60
            })
        );
        assert_eq!(
            sharpe(&series(&alternating()), &constant(0.0, 59)),
            Err(SharpeRefusal::RiskFreeMissing {
                session: session(59)
            })
        );
        assert_eq!(
            sharpe(&constant(0.001, 60), &constant(0.0, 60)),
            Err(SharpeRefusal::NoDispersion { sessions: 60 })
        );
    }

    #[test]
    fn test_an_unmeasured_session_is_counted_and_left_out() {
        let mut readings: Vec<(SessionDate, Option<f64>)> = alternating()
            .into_iter()
            .enumerate()
            .map(|(index, value)| (session(index as u64), Some(value)))
            .collect();
        readings.push((session(60), None));
        let read = sharpe(&Series::new(readings).unwrap(), &constant(0.0, 60)).unwrap();
        assert_eq!((read.sessions(), read.undefined()), (60, 1));
        assert!((read.annualized() - 15.74166446091391).abs() < 1e-9);
    }

    #[test]
    fn test_the_index_is_read_over_exactly_the_results_sessions() {
        // Over a 5bp daily rate, the result's excess alternates +15bp and -5bp (Sharpe half the base √(252 × 59 / 60))
        // and the index's +25bp and -15bp (a quarter of it); the index's crash falls on a session the result left
        // unmeasured, so it must not reach the reading.
        let mut result_readings: Vec<(SessionDate, Option<f64>)> = alternating()
            .into_iter()
            .enumerate()
            .map(|(index, value)| (session(index as u64), Some(value)))
            .collect();
        result_readings.push((session(60), None));
        let result = Series::new(result_readings).unwrap();
        let mut index_readings: Vec<f64> = (0..60)
            .map(|index| if index % 2 == 0 { 0.003 } else { -0.001 })
            .collect();
        index_readings.push(-0.5);
        let rate = constant(0.0005, 61);
        let beside = beside_index(&result, &series(&index_readings), &rate).unwrap();
        assert!((beside.result().annualized() - 7.870832230456955).abs() < 1e-9);
        assert!((beside.index().annualized() - 3.9354161152284775).abs() < 1e-9);
        assert_eq!(beside.index().sessions(), 60);
        assert_eq!(
            beside_index(&result, &series(&index_readings[..59]), &rate),
            Err(BesideIndexRefusal::IndexMissing {
                session: session(59)
            })
        );
    }

    #[test]
    fn test_a_result_is_gated_on_both_targets() {
        let beside = BesideIndex {
            result: Sharpe {
                annualized: 3.0,
                mean_return: 0.0010,
                sessions: 60,
                undefined: 0,
            },
            index: Sharpe {
                annualized: 1.5,
                mean_return: 0.0004,
                sessions: 60,
                undefined: 0,
            },
        };
        let targets = against_targets(beside);
        assert_eq!(targets.daily_return, Gate::Clears);
        match targets.sharpe_margin {
            Gate::Short { by } => assert!((by - 0.5).abs() < 1e-12),
            Gate::Clears => panic!("a margin of 1.5 is short of 2"),
        }
        let pinned = [
            SHARPE_MARGIN_OVER_INDEX,
            DAILY_RETURN_TARGET_BASIS_POINTS,
            MINIMUM_SESSIONS as f64,
        ];
        assert_eq!(pinned, [2.0, 8.0, 60.0]);
    }

    #[test]
    fn test_a_reading_exactly_at_its_target_clears() {
        let at_the_targets = BesideIndex {
            result: Sharpe {
                annualized: 3.5,
                mean_return: 0.0008,
                sessions: 60,
                undefined: 0,
            },
            index: Sharpe {
                annualized: 1.5,
                mean_return: 0.0004,
                sessions: 60,
                undefined: 0,
            },
        };
        let reached = against_targets(at_the_targets);
        assert_eq!(
            (reached.sharpe_margin, reached.daily_return),
            (Gate::Clears, Gate::Clears)
        );
    }

    fn daily_bar(symbol: &Symbol, on: SessionDate, close_dollars: f64) -> TradeBar {
        let close = StampedPrice::new(
            on.regular_close(),
            Price::from_dollars(close_dollars).unwrap(),
        );
        let sums = TradeSums::new(
            TradeTotals::new(
                TradeCount::new(1),
                Shares::whole(1).unwrap(),
                DollarVolume::from_units(0),
            ),
            Some(OpenClose::new(close, close).unwrap()),
            None,
        );
        TradeBar::new(
            symbol.clone(),
            BarInterval::OneDay,
            BarInterval::OneDay.bucket(on.regular_close()),
            sums,
        )
        .unwrap()
    }

    #[test]
    fn test_index_returns_run_close_to_close_between_adjacent_sessions() {
        let spy = Symbol::new("SPY").unwrap();
        let other = Symbol::new("QQQ").unwrap();
        let (open, close) = (
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        );
        let calendar = TradingCalendar::new(
            (0..=6)
                .map(|offset| TradingSession::new(session(offset), open, close).unwrap())
                .collect(),
            SessionRange::new(session(0), session(6)).unwrap(),
        )
        .unwrap();
        // Session 2 has no SPY row and session 5 no partition at all, so 6 must not read 390 against 398.
        let bars: BTreeMap<SessionDate, Partition<TradeBar>> = [
            (0, Some(400.0)),
            (1, Some(404.0)),
            (2, None),
            (3, Some(399.0)),
            (4, Some(398.0)),
            (6, Some(390.0)),
        ]
        .into_iter()
        .map(|(offset, close)| {
            let mut rows = vec![daily_bar(&other, session(offset), 100.0)];
            rows.extend(close.map(|close| daily_bar(&spy, session(offset), close)));
            (session(offset), Partition::try_from(rows).unwrap())
        })
        .collect();
        let returns = index_returns(&bars, &spy, &calendar).unwrap();
        let sessions: Vec<SessionDate> = returns.readings().keys().copied().collect();
        assert_eq!(
            sessions,
            vec![session(1), session(2), session(3), session(4), session(6)]
        );
        let readings: Vec<Option<f64>> = returns.readings().values().copied().collect();
        assert!((readings[0].unwrap() - 0.01).abs() < 1e-12);
        assert_eq!((readings[1], readings[2], readings[4]), (None, None, None));
        assert!((readings[3].unwrap() + 0.0025062656641604).abs() < 1e-12);
    }

    proptest! {
        /// Taking a constant rate off the returns is the same as reading it as the risk-free rate.
        #[test]
        fn test_a_risk_free_rate_is_an_excess_return(
            readings in prop::collection::vec(-0.05..0.05f64, 60..120),
            rate in -0.001..0.001f64,
        ) {
            let count = readings.len();
            let over = sharpe(&series(&readings), &constant(rate, count));
            let shifted: Vec<f64> = readings.iter().map(|value| value - rate).collect();
            let under = sharpe(&series(&shifted), &constant(0.0, count));
            match (over, under) {
                (Ok(over), Ok(under)) => prop_assert!((over.annualized() - under.annualized()).abs() < 1e-6),
                (over, under) => prop_assert_eq!(over.is_err(), under.is_err()),
            }
        }

        /// Scaling returns and the rate by one positive factor, as leverage without cost does, leaves the Sharpe alone.
        #[test]
        fn test_a_sharpe_does_not_see_leverage(
            readings in prop::collection::vec(-0.05..0.05f64, 60..120),
            factor in 0.1..10.0f64,
        ) {
            let count = readings.len();
            let base = sharpe(&series(&readings), &constant(0.0001, count));
            let scaled: Vec<f64> = readings.iter().map(|value| value * factor).collect();
            let levered = sharpe(&series(&scaled), &constant(0.0001 * factor, count));
            match (base, levered) {
                (Ok(base), Ok(levered)) => prop_assert!(
                    (base.annualized() - levered.annualized()).abs() < 1e-6 * base.annualized().abs().max(1.0)
                ),
                (base, levered) => prop_assert_eq!(base.is_err(), levered.is_err()),
            }
        }
    }
}
