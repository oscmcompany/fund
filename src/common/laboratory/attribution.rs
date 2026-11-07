//! Where a result's skill comes from: a forecast's gain split into what timed the whole cross-section and what ranked
//! names within it, and a session's return split into the market bet its net exposure made and the rest.

use std::collections::BTreeMap;

use crate::common::book::{Book, Cash, ValuationRefusal};
use crate::common::laboratory::residual::SimpleReturn;
use crate::common::laboratory::score::Pair;
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::market::{Price, Symbol};
use crate::common::time::SessionDate;

/// A forecast's gain over one cross-section, `Σ forecast × outcome`, split exactly: `timing` is the mean forecast times
/// the mean outcome summed over names, and `selection` what each name's deviations from those means earned together.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ForecastGain {
    timing: f64,
    selection: f64,
}

impl ForecastGain {
    /// The part a market-wide move pays, zero for forecasts that sum to zero.
    pub fn timing(self) -> f64 {
        self.timing
    }

    /// The part no shift of every outcome by one amount can change.
    pub fn selection(self) -> f64 {
        self.selection
    }

    pub fn total(self) -> f64 {
        self.timing + self.selection
    }
}

/// The split of `pairs`' gain, or `None` for an empty cross-section, which has no means.
pub fn forecast_gain(pairs: &[Pair]) -> Option<ForecastGain> {
    if pairs.is_empty() {
        return None;
    }
    let count = pairs.len() as f64;
    let forecast_mean = pairs.iter().map(|pair| pair.forecast().get()).sum::<f64>() / count;
    let outcome_mean = pairs.iter().map(|pair| pair.outcome().get()).sum::<f64>() / count;
    let selection = pairs
        .iter()
        .map(|pair| (pair.forecast().get() - forecast_mean) * (pair.outcome().get() - outcome_mean))
        .sum();
    Some(ForecastGain {
        timing: count * forecast_mean * outcome_mean,
        selection,
    })
}

/// Long worth less short worth as a share of the equity the session opened with, the base a session's return is on,
/// so each stretch's market leg adds into that return.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct NetExposure(f64);

impl NetExposure {
    pub fn get(self) -> f64 {
        self.0
    }
}

/// The book's net exposure at `marks` over `session_opening`, or `None` when the session opened with no positive
/// equity to be a share of.
pub fn net_exposure(
    book: &Book,
    marks: impl Fn(&Symbol) -> Option<Price>,
    session_opening: Cash,
) -> Result<Option<NetExposure>, ValuationRefusal> {
    let net = book.value(&marks)?.units() - book.cash().units();
    Ok((session_opening.units() > 0)
        .then(|| NetExposure(net as f64 / session_opening.units() as f64)))
}

/// One stretch a book held unchanged: its net exposure and the market's return across the stretch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Held {
    exposure: NetExposure,
    market_return: SimpleReturn,
}

impl Held {
    pub fn new(exposure: NetExposure, market_return: SimpleReturn) -> Self {
        Self {
            exposure,
            market_return,
        }
    }
}

/// One session's return split into the market leg, each held stretch's exposure times the market's return across it,
/// and the selection leg, the rest; the market leg of stretches held one after another is the sum of theirs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Legs {
    market: f64,
    selection: f64,
}

impl Legs {
    /// A session that held nothing has no market leg, so its whole return is selection.
    pub fn of(total: SimpleReturn, held: &[Held]) -> Self {
        let market = held
            .iter()
            .map(|stretch| stretch.exposure.0 * stretch.market_return.get())
            .sum::<f64>();
        Self {
            market,
            selection: total.get() - market,
        }
    }

    pub fn market(self) -> f64 {
        self.market
    }

    pub fn selection(self) -> f64 {
        self.selection
    }
}

/// A result's returns split session by session into the two legs.
#[derive(Debug, Clone, PartialEq)]
pub struct SplitReturns {
    pub market: Series,
    pub selection: Series,
}

/// Each session of `returns` split by the stretches it held; a session `held` does not name, or with no return of
/// its own, is undefined in both legs.
pub fn split_returns(
    returns: &Series,
    held: &BTreeMap<SessionDate, Vec<Held>>,
) -> Result<SplitReturns, SeriesRefusal> {
    let legs: Vec<(SessionDate, Option<Legs>)> = returns
        .readings()
        .iter()
        .map(|(session, total)| {
            let legs = total.zip(held.get(session)).map(|(total, stretches)| {
                let total = SimpleReturn::new(total)
                    .expect("a series reading lies within the bound a return admits");
                Legs::of(total, stretches)
            });
            (*session, legs)
        })
        .collect();
    Ok(SplitReturns {
        market: Series::new(
            legs.iter()
                .map(|(session, legs)| (*session, legs.map(Legs::market))),
        )?,
        selection: Series::new(
            legs.iter()
                .map(|(session, legs)| (*session, legs.map(Legs::selection))),
        )?,
    })
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;
    use std::num::NonZeroU8;

    use chrono::TimeDelta;

    use crate::common::book::{Fill, Position, Side};
    use crate::common::laboratory::ledger::{Borrow, Ledger, Leverage};
    use crate::common::market::{DollarVolume, Shares};
    use crate::common::risk::Limits;
    use crate::common::time::calendar::SessionPhase;

    /// Cash units in a dollar: price ticks times share millionths.
    const DOLLAR: i128 = 1_000_000_000_000;
    use crate::common::laboratory::score::{Forecast, Outcome};

    fn pairs(readings: &[(f64, f64)]) -> Vec<Pair> {
        readings
            .iter()
            .map(|(forecast, outcome)| {
                Pair::new(
                    Forecast::new(*forecast).unwrap(),
                    Outcome::new(*outcome).unwrap(),
                )
            })
            .collect()
    }

    fn any_pairs() -> impl Strategy<Value = Vec<(f64, f64)>> {
        prop::collection::vec((-10.0..10.0f64, -0.1..0.1f64), 1..40)
    }

    fn returned(value: f64) -> SimpleReturn {
        SimpleReturn::new(value).unwrap()
    }

    fn close(left: f64, right: f64) -> bool {
        (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
    }

    #[test]
    fn test_a_gain_splits_into_timing_and_selection() {
        // Forecasts 1 and 3 (mean 2), outcomes 2% and 4% (mean 3%): timing 2 × 2 × 0.03 = 0.12, selection
        // (-1)(-0.01) + (1)(0.01) = 0.02, and together the gain 0.02 + 0.12 = 0.14.
        let gain = forecast_gain(&pairs(&[(1.0, 0.02), (3.0, 0.04)])).unwrap();
        assert!(
            close(gain.timing(), 0.12) && close(gain.selection(), 0.02),
            "{gain:?}"
        );
        assert!(close(gain.total(), 0.14));
        assert_eq!(forecast_gain(&[]), None);
    }

    #[test]
    fn test_net_exposure_is_long_less_short_over_the_sessions_opening_equity() {
        let symbol = |raw| Symbol::new(raw).unwrap();
        // $17,000 cash left after buying $5,000 of AAA and shorting $2,000 of BBB: net $3,000 on a $20,000 opening.
        let book = Book::reported(
            Cash::from_units(17_000 * DOLLAR),
            [
                (symbol("AAA"), Position::from_units(100_000_000)),
                (symbol("BBB"), Position::from_units(-40_000_000)),
            ],
        );
        let at_fifty = |_: &Symbol| Price::from_dollars(50.0).ok();
        let exposure = net_exposure(&book, at_fifty, Cash::from_units(20_000 * DOLLAR))
            .unwrap()
            .unwrap();
        assert!(close(exposure.get(), 0.15), "{exposure:?}");
        assert_eq!(net_exposure(&book, at_fifty, Cash::from_units(0)), Ok(None));
    }

    #[test]
    fn test_returns_split_into_legs_where_every_input_is_measured() {
        let session = |day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, day).unwrap());
        let returns = Series::new([
            (session(2), Some(0.004)),
            (session(3), Some(0.002)),
            (session(4), None),
            (session(5), Some(0.001)),
        ])
        .unwrap();
        // Session 2 held 0.3 through a 1% market rise; session 3 held nothing; session 5's stretches are unknown.
        let held = BTreeMap::from([
            (
                session(2),
                vec![Held::new(NetExposure(0.3), returned(0.01))],
            ),
            (session(3), Vec::new()),
            (session(4), Vec::new()),
        ]);
        let split = split_returns(&returns, &held).unwrap();
        let legs: Vec<(Option<f64>, Option<f64>)> = split
            .market
            .readings()
            .values()
            .zip(split.selection.readings().values())
            .map(|(market, selection)| (*market, *selection))
            .collect();
        assert_eq!(legs.len(), 4);
        assert!(close(legs[0].0.unwrap(), 0.003) && close(legs[0].1.unwrap(), 0.001));
        assert_eq!(
            &legs[1..],
            [(Some(0.0), Some(0.002)), (None, None), (None, None)]
        );
    }

    #[test]
    fn test_a_ledger_that_only_held_the_market_has_no_selection() {
        let spy = Symbol::new("SPY").unwrap();
        let open = SessionPhase::Open {
            until_close: TimeDelta::hours(3),
        };
        let at = "2026-03-02T15:00:00Z".parse().unwrap();
        let fill = |side, whole: u64, dollars: f64| {
            Fill::new(
                at,
                spy.clone(),
                side,
                Shares::whole(whole).unwrap(),
                Price::from_dollars(dollars).unwrap(),
                DollarVolume::from_units(0),
            )
            .unwrap()
        };
        let limits = Limits::new(
            Cash::from_units(40_000 * DOLLAR),
            Cash::from_units(40_000 * DOLLAR),
            Cash::from_units(1_000 * DOLLAR),
            TimeDelta::minutes(10),
        )
        .unwrap();
        let mut ledger = Ledger::open(
            Cash::from_units(20_000 * DOLLAR),
            limits,
            Leverage::new(NonZeroU8::new(2).unwrap()),
        )
        .unwrap();
        // Hold 40 shares from $400 to $404, then 50 from $404 to $400, reading the exposure as each stretch starts.
        let mut held = Vec::new();
        for (whole, from, to) in [(40, 400.0, 404.0), (10, 404.0, 400.0)] {
            ledger = ledger
                .trade(
                    &fill(Side::Buy, whole, from),
                    open,
                    Borrow::Unavailable,
                    |_| None,
                )
                .unwrap();
            let mark = Price::from_dollars(from).ok();
            let exposure = net_exposure(ledger.book(), |_| mark, ledger.session_opening())
                .unwrap()
                .unwrap();
            held.push(Held::new(exposure, returned(to / from - 1.0)));
        }
        let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap());
        ledger = ledger
            .trade(
                &fill(Side::Sell, 50, 400.0),
                open,
                Borrow::Unavailable,
                |_| None,
            )
            .unwrap()
            .close(session)
            .unwrap();
        let split = split_returns(
            &ledger.returns().unwrap(),
            &BTreeMap::from([(session, held)]),
        )
        .unwrap();
        // $160 then -$200 on $20,000: the whole -0.2% is the market's.
        let market = split.market.readings()[&session].unwrap();
        assert!(close(market, -0.002), "{market}");
        assert!(split.selection.readings()[&session].unwrap().abs() < 1e-15);
    }

    proptest! {
        /// The split is exact: timing and selection add back to the gain.
        #[test]
        fn test_the_split_adds_back_to_the_gain(readings in any_pairs()) {
            let gain = forecast_gain(&pairs(&readings)).unwrap();
            let total: f64 = readings.iter().map(|(forecast, outcome)| forecast * outcome).sum();
            prop_assert!(close(gain.total(), total), "{gain:?} against {total}");
        }

        /// A move of every outcome by one amount, as the market makes, and a shift of every forecast by one amount
        /// both leave selection where it was; only timing sees them.
        #[test]
        fn test_selection_does_not_see_a_common_shift(
            readings in any_pairs(),
            market in -0.05..0.05f64,
            bias in -5.0..5.0f64,
        ) {
            let base = forecast_gain(&pairs(&readings)).unwrap();
            let shifted: Vec<(f64, f64)> =
                readings.iter().map(|(forecast, outcome)| (forecast + bias, outcome + market)).collect();
            let moved = forecast_gain(&pairs(&shifted)).unwrap();
            prop_assert!((moved.selection() - base.selection()).abs() < 1e-9, "{base:?} against {moved:?}");
        }

        /// Forecasts that sum to zero, a dollar-neutral book, time nothing; equal forecasts select nothing.
        #[test]
        fn test_neutral_forecasts_time_nothing_and_equal_ones_select_nothing(
            readings in any_pairs(),
            level in -5.0..5.0f64,
        ) {
            let mean = readings.iter().map(|(forecast, _)| forecast).sum::<f64>() / readings.len() as f64;
            let neutral: Vec<(f64, f64)> = readings.iter().map(|(forecast, outcome)| (forecast - mean, *outcome)).collect();
            prop_assert!(forecast_gain(&pairs(&neutral)).unwrap().timing().abs() < 1e-9);
            let equal: Vec<(f64, f64)> = readings.iter().map(|(_, outcome)| (level, *outcome)).collect();
            prop_assert!(forecast_gain(&pairs(&equal)).unwrap().selection().abs() < 1e-9);
        }

        /// A book that only held the market has no selection leg, and one with no net exposure has no market leg.
        #[test]
        fn test_the_legs_see_the_market_bet_and_only_it(
            market in -0.05..0.05f64,
            exposure in -2.0..2.0f64,
            residual in -0.01..0.01f64,
        ) {
            let stretch = |exposure| [Held::new(NetExposure(exposure), returned(market))];
            let held = Legs::of(returned(exposure * market), &stretch(exposure));
            prop_assert!(held.selection().abs() < 1e-15);
            let neutral = Legs::of(returned(residual), &stretch(0.0));
            prop_assert_eq!((neutral.market(), neutral.selection()), (0.0, residual));
            let both = Legs::of(returned(exposure * market + residual), &stretch(exposure));
            prop_assert!((both.selection() - residual).abs() < 1e-12);
        }

        /// The market leg of stretches held one after another is the sum of each stretch's, so splitting a session
        /// finer moves nothing between the legs.
        #[test]
        fn test_the_market_leg_adds_over_stretches(
            first in prop::collection::vec((-2.0..2.0f64, -0.05..0.05f64), 0..8),
            second in prop::collection::vec((-2.0..2.0f64, -0.05..0.05f64), 0..8),
            total in -0.05..0.05f64,
        ) {
            let stretches = |readings: &[(f64, f64)]| -> Vec<Held> {
                readings
                    .iter()
                    .map(|(exposure, market_return)| Held::new(NetExposure(*exposure), returned(*market_return)))
                    .collect()
            };
            let joined: Vec<(f64, f64)> = first.iter().chain(&second).copied().collect();
            let whole = Legs::of(returned(total), &stretches(&joined)).market();
            let parts = Legs::of(returned(total), &stretches(&first)).market() + Legs::of(returned(total), &stretches(&second)).market();
            prop_assert!((whole - parts).abs() < 1e-12);
            prop_assert_eq!(Legs::of(returned(total), &[]).market(), 0.0);
        }
    }
}
