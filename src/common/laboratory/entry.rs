//! Entering an event row's trade: crossing the touch at arrival, resting at it for the event table's window and
//! crossing at the window's end unfilled, or choosing between the two by the execution pair's forecast.

use chrono::{DateTime, TimeDelta, Utc};
use strum::IntoEnumIterator;

use crate::common::book::Side;
use crate::common::cost::{FillBound, PerShare};
use crate::common::laboratory::admission::half_spread;
use crate::common::learning::execution::OrderForecast;
use crate::common::market::quote_bars::{Spread, StandingQuote};
use crate::common::market::{Price, Shares, Symbol};
use crate::common::time::SessionDate;

/// Why a row's resting labels are unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownCause {
    /// The resting window outlasts the session's last read.
    WindowPastLastRead,
}

/// What became of an order resting at the arrival touch under one fill bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resting {
    Filled { after: TimeDelta, price: Price },
    Unfilled { cross_at_window: Price },
    Unknown(UnknownCause),
}

/// When each side's resting order first filled under each bound, and the quote at the window's end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingLabels {
    window_bid: Price,
    window_ask: Price,
    fills: [[Option<TimeDelta>; 2]; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RestingLabelsRefusal {
    #[error("the window's bid {bid} is above its ask {ask}")]
    WindowCrossed { bid: Price, ask: Price },
    #[error("the {side} order filled {after} before arrival under the {bound} bound")]
    FilledBeforeArrival {
        side: Side,
        bound: FillBound,
        after: TimeDelta,
    },
    /// A print through the limit is also at or through it, so the optimistic fill can come no later.
    #[error(
        "the {side} order's optimistic fill {optimistic:?} is not by its conservative fill {conservative}"
    )]
    BoundsDisagree {
        side: Side,
        optimistic: Option<TimeDelta>,
        conservative: TimeDelta,
    },
}

fn side_index(side: Side) -> usize {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn bound_index(bound: FillBound) -> usize {
    match bound {
        FillBound::Optimistic => 0,
        FillBound::Conservative => 1,
    }
}

impl RestingLabels {
    /// Labels whose `fill` is each side's and bound's time from arrival to its first fill, `None` when unfilled.
    pub fn new(
        window_bid: Price,
        window_ask: Price,
        fill: impl Fn(Side, FillBound) -> Option<TimeDelta>,
    ) -> Result<Self, RestingLabelsRefusal> {
        if window_bid > window_ask {
            return Err(RestingLabelsRefusal::WindowCrossed {
                bid: window_bid,
                ask: window_ask,
            });
        }
        let mut fills = [[None; 2]; 2];
        for side in Side::iter() {
            for bound in FillBound::iter() {
                let after = fill(side, bound);
                if let Some(after) = after
                    && after < TimeDelta::zero()
                {
                    return Err(RestingLabelsRefusal::FilledBeforeArrival { side, bound, after });
                }
                fills[side_index(side)][bound_index(bound)] = after;
            }
            let [optimistic, conservative] = fills[side_index(side)];
            match (optimistic, conservative) {
                (Some(early), Some(late)) if early > late => {
                    return Err(RestingLabelsRefusal::BoundsDisagree {
                        side,
                        optimistic,
                        conservative: late,
                    });
                }
                (None, Some(late)) => {
                    return Err(RestingLabelsRefusal::BoundsDisagree {
                        side,
                        optimistic,
                        conservative: late,
                    });
                }
                (Some(_), Some(_)) | (Some(_), None) | (None, None) => {}
            }
        }
        Ok(Self {
            window_bid,
            window_ask,
            fills,
        })
    }
}

/// The far touch at a horizon after arrival, which an exit takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitQuote {
    bid: Price,
    ask: Price,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the exit's bid {bid} is above its ask {ask}")]
pub struct ExitCrossed {
    pub bid: Price,
    pub ask: Price,
}

impl ExitQuote {
    pub fn new(bid: Price, ask: Price) -> Result<Self, ExitCrossed> {
        match bid <= ask {
            true => Ok(Self { bid, ask }),
            false => Err(ExitCrossed { bid, ask }),
        }
    }

    pub fn bid(self) -> Price {
        self.bid
    }

    pub fn ask(self) -> Price {
        self.ask
    }
}

/// Why a row has no exit quote at its horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitUnread {
    /// The horizon falls after the session's last read.
    PastLastRead,
}

/// One event table row read at one horizon: the touch in force at arrival, the session's trailing spread, the exit
/// at the horizon and the resting labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRow {
    symbol: Symbol,
    arrival: DateTime<Utc>,
    touch: StandingQuote,
    trailing_spread: Spread,
    exit: Result<ExitQuote, ExitUnread>,
    resting: Result<RestingLabels, UnknownCause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the touch stood from {since}, after the arrival at {arrival}")]
pub struct TouchAfterArrival {
    pub since: DateTime<Utc>,
    pub arrival: DateTime<Utc>,
}

impl EventRow {
    pub fn new(
        symbol: Symbol,
        arrival: DateTime<Utc>,
        touch: StandingQuote,
        trailing_spread: Spread,
        exit: Result<ExitQuote, ExitUnread>,
        resting: Result<RestingLabels, UnknownCause>,
    ) -> Result<Self, TouchAfterArrival> {
        if touch.since() > arrival {
            return Err(TouchAfterArrival {
                since: touch.since(),
                arrival,
            });
        }
        Ok(Self {
            symbol,
            arrival,
            touch,
            trailing_spread,
            exit,
            resting,
        })
    }

    /// The session the arrival falls on.
    pub fn session(&self) -> SessionDate {
        SessionDate::at(self.arrival)
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn arrival(&self) -> DateTime<Utc> {
        self.arrival
    }

    pub fn touch(&self) -> &StandingQuote {
        &self.touch
    }

    /// The session's mean relative spread up to arrival, converted to millionths at the arrival mid.
    pub fn trailing_spread(&self) -> Spread {
        self.trailing_spread
    }

    pub fn exit(&self) -> Result<ExitQuote, ExitUnread> {
        self.exit
    }

    /// The `side` order's label under `bound`: a fill is at the touch's own side, an unfilled order crosses to the
    /// window's far side.
    pub fn resting(&self, side: Side, bound: FillBound) -> Resting {
        match &self.resting {
            Err(cause) => Resting::Unknown(*cause),
            Ok(labels) => match labels.fills[side_index(side)][bound_index(bound)] {
                Some(after) => Resting::Filled {
                    after,
                    price: match side {
                        Side::Buy => self.touch.bid(),
                        Side::Sell => self.touch.ask(),
                    },
                },
                None => Resting::Unfilled {
                    cross_at_window: match side {
                        Side::Buy => labels.window_ask,
                        Side::Sell => labels.window_bid,
                    },
                },
            },
        }
    }
}

/// How a trade is entered.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Entry {
    Cross,
    Rest(FillBound),
    /// Rest under `bound` when `rests` prefers it at the touch's half spread, else cross.
    Choose {
        forecast: OrderForecast,
        bound: FillBound,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum How {
    Crossed,
    RestFilled,
    RestThenCrossed,
}

/// The price a trade entered at and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entered {
    price: Price,
    how: How,
}

impl Entered {
    pub fn price(self) -> Price {
        self.price
    }

    pub fn how(self) -> How {
        self.how
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EntryRefusal {
    #[error("the {side} order's resting label under the {bound} bound is unknown: {cause:?}")]
    RestingUnknown {
        side: Side,
        bound: FillBound,
        cause: UnknownCause,
    },
}

/// Enters `side` of `row` by `entry`; an unknown resting label refuses every entry that may rest, whatever it chooses,
/// so each such entry reads the same rows.
pub fn enter(row: &EventRow, side: Side, entry: Entry) -> Result<Entered, EntryRefusal> {
    let crossed = Entered {
        price: match side {
            Side::Buy => row.touch.ask(),
            Side::Sell => row.touch.bid(),
        },
        how: How::Crossed,
    };
    let rested = |bound: FillBound| match row.resting(side, bound) {
        Resting::Filled { price, .. } => Ok(Entered {
            price,
            how: How::RestFilled,
        }),
        Resting::Unfilled { cross_at_window } => Ok(Entered {
            price: cross_at_window,
            how: How::RestThenCrossed,
        }),
        Resting::Unknown(cause) => Err(EntryRefusal::RestingUnknown { side, bound, cause }),
    };
    match entry {
        Entry::Cross => Ok(crossed),
        Entry::Rest(bound) => rested(bound),
        Entry::Choose { forecast, bound } => {
            let resting = rested(bound)?;
            let half = half_spread(&row.touch);
            match rests(forecast, half, half) {
                true => Ok(resting),
                false => Ok(crossed),
            }
        }
    }
}

/// Whether resting beats paying `cross` now: the expected resting value less the unfilled chance times `window_cross`,
/// that charge rounded to the nearest millionth, against minus `cross`, a tie crossing.
pub fn rests(forecast: OrderForecast, cross: PerShare, window_cross: PerShare) -> bool {
    let unfilled = (1.0 - forecast.fill().value()) * window_cross.millionths() as f64;
    forecast.expected_resting_value().millionths() - unfilled.round() as i64 > -cross.millionths()
}

/// The displayed size at the touch `side` takes, which caps what one entry may fill.
pub fn displayed(touch: &StandingQuote, side: Side) -> Shares {
    match side {
        Side::Buy => touch.ask_size(),
        Side::Sell => touch.bid_size(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    use crate::common::learning::execution::FillProbability;

    fn price(ticks: i64) -> Price {
        Price::from_ticks(ticks).unwrap()
    }

    fn per_share(millionths: i64) -> PerShare {
        PerShare::from_millionths(millionths).unwrap()
    }

    fn arrival() -> DateTime<Utc> {
        "2025-06-16T13:40:30Z".parse().unwrap()
    }

    fn touch(bid: i64, ask: i64) -> StandingQuote {
        StandingQuote::new(
            arrival() - TimeDelta::seconds(1),
            price(bid),
            price(ask),
            Shares::whole(300).unwrap(),
            Shares::whole(500).unwrap(),
        )
        .unwrap()
    }

    fn forecast(fill: f64, markout: i64) -> OrderForecast {
        OrderForecast::new(FillProbability::new(fill).unwrap(), per_share(markout))
    }

    /// A row at a 100.00 by 100.02 touch whose buy fills under the optimistic bound alone after ten seconds, whose
    /// sell never fills, and whose window ends at 100.01 by 100.03.
    fn row(resting: Result<RestingLabels, UnknownCause>) -> EventRow {
        EventRow::new(
            Symbol::new("AAA").unwrap(),
            arrival(),
            touch(100_000_000, 100_020_000),
            Spread::from_ticks(20_000),
            Err(ExitUnread::PastLastRead),
            resting,
        )
        .unwrap()
    }

    fn labels() -> RestingLabels {
        RestingLabels::new(
            price(100_010_000),
            price(100_030_000),
            |side, bound| match (side, bound) {
                (Side::Buy, FillBound::Optimistic) => Some(TimeDelta::seconds(10)),
                (Side::Buy, FillBound::Conservative)
                | (Side::Sell, FillBound::Optimistic | FillBound::Conservative) => None,
            },
        )
        .unwrap()
    }

    #[test]
    fn test_each_entry_prices_at_its_own_quote() {
        let row = row(Ok(labels()));
        let entered = |side, entry| enter(&row, side, entry).unwrap();
        assert_eq!(
            entered(Side::Buy, Entry::Cross),
            Entered {
                price: price(100_020_000),
                how: How::Crossed
            }
        );
        assert_eq!(
            entered(Side::Sell, Entry::Cross),
            Entered {
                price: price(100_000_000),
                how: How::Crossed
            }
        );
        assert_eq!(
            entered(Side::Buy, Entry::Rest(FillBound::Optimistic)),
            Entered {
                price: price(100_000_000),
                how: How::RestFilled
            }
        );
        assert_eq!(
            entered(Side::Buy, Entry::Rest(FillBound::Conservative)),
            Entered {
                price: price(100_030_000),
                how: How::RestThenCrossed
            }
        );
        assert_eq!(
            entered(Side::Sell, Entry::Rest(FillBound::Optimistic)),
            Entered {
                price: price(100_010_000),
                how: How::RestThenCrossed
            }
        );
        assert_eq!(
            row.resting(Side::Buy, FillBound::Optimistic),
            Resting::Filled {
                after: TimeDelta::seconds(10),
                price: price(100_000_000)
            }
        );
    }

    /// An unknown label refuses resting and choosing, even a choice that would cross, and never crossing.
    #[test]
    fn test_an_unknown_label_refuses_every_entry_that_may_rest() {
        let row = row(Err(UnknownCause::WindowPastLastRead));
        let refused = Err(EntryRefusal::RestingUnknown {
            side: Side::Sell,
            bound: FillBound::Conservative,
            cause: UnknownCause::WindowPastLastRead,
        });
        assert_eq!(
            enter(&row, Side::Sell, Entry::Rest(FillBound::Conservative)),
            refused
        );
        let choose = Entry::Choose {
            forecast: forecast(0.0, 0),
            bound: FillBound::Conservative,
        };
        assert_eq!(enter(&row, Side::Sell, choose), refused);
        assert_eq!(
            enter(&row, Side::Sell, Entry::Cross).unwrap().how(),
            How::Crossed
        );
    }

    /// At a half spread of 10,000 millionths crossed now or at the window, resting wins at any chance of a fill while
    /// the markout gives back less than the half spread.
    #[test]
    fn test_choose_rests_where_the_forecast_beats_the_half_spread() {
        let row = row(Ok(labels()));
        let chosen = |fill, markout| {
            let entry = Entry::Choose {
                forecast: forecast(fill, markout),
                bound: FillBound::Optimistic,
            };
            enter(&row, Side::Buy, entry).unwrap().how()
        };
        assert_eq!(chosen(1.0, -9_999), How::RestFilled);
        assert_eq!(chosen(1.0, -10_000), How::Crossed);
        assert_eq!(chosen(0.5, -9_998), How::RestFilled);
        assert_eq!(chosen(0.5, -10_000), How::Crossed);
    }

    #[test]
    fn test_displayed_is_the_size_at_the_side_taken() {
        let quote = touch(100_000_000, 100_020_000);
        assert_eq!(displayed(&quote, Side::Buy), Shares::whole(500).unwrap());
        assert_eq!(displayed(&quote, Side::Sell), Shares::whole(300).unwrap());
    }

    #[test]
    fn test_labels_refuse_what_the_table_cannot_write() {
        let window = |bid, ask| RestingLabels::new(price(bid), price(ask), |_, _| None);
        assert_eq!(
            window(2, 1),
            Err(RestingLabelsRefusal::WindowCrossed {
                bid: price(2),
                ask: price(1)
            })
        );
        let filled = |optimistic: Option<i64>, conservative: Option<i64>| {
            RestingLabels::new(price(1), price(2), move |side, bound| match (side, bound) {
                (Side::Sell, FillBound::Optimistic) => optimistic.map(TimeDelta::seconds),
                (Side::Sell, FillBound::Conservative) => conservative.map(TimeDelta::seconds),
                (Side::Buy, FillBound::Optimistic | FillBound::Conservative) => None,
            })
        };
        assert_eq!(
            filled(Some(5), Some(4)),
            Err(RestingLabelsRefusal::BoundsDisagree {
                side: Side::Sell,
                optimistic: Some(TimeDelta::seconds(5)),
                conservative: TimeDelta::seconds(4),
            })
        );
        assert_eq!(
            filled(None, Some(4)),
            Err(RestingLabelsRefusal::BoundsDisagree {
                side: Side::Sell,
                optimistic: None,
                conservative: TimeDelta::seconds(4),
            })
        );
        assert_eq!(
            filled(Some(-1), None),
            Err(RestingLabelsRefusal::FilledBeforeArrival {
                side: Side::Sell,
                bound: FillBound::Optimistic,
                after: TimeDelta::seconds(-1),
            })
        );
        assert!(filled(Some(4), Some(4)).is_ok());
        assert!(filled(Some(4), None).is_ok());
    }

    #[test]
    fn test_a_touch_standing_after_arrival_is_refused() {
        let late = StandingQuote::new(
            arrival() + TimeDelta::nanoseconds(1),
            price(1),
            price(2),
            Shares::whole(1).unwrap(),
            Shares::whole(1).unwrap(),
        )
        .unwrap();
        let refused = EventRow::new(
            Symbol::new("AAA").unwrap(),
            arrival(),
            late,
            Spread::from_ticks(1),
            Err(ExitUnread::PastLastRead),
            Err(UnknownCause::WindowPastLastRead),
        );
        assert_eq!(
            refused,
            Err(TouchAfterArrival {
                since: arrival() + TimeDelta::nanoseconds(1),
                arrival: arrival()
            })
        );
    }

    fn any_side() -> impl Strategy<Value = Side> {
        prop_oneof![Just(Side::Buy), Just(Side::Sell)]
    }

    fn any_bound() -> impl Strategy<Value = FillBound> {
        prop_oneof![Just(FillBound::Optimistic), Just(FillBound::Conservative)]
    }

    /// A row with any uncrossed touch and window, each side filled under neither bound, the optimistic alone, or both.
    fn any_row() -> impl Strategy<Value = EventRow> {
        (
            1..1_000_000_i64,
            0..1_000_i64,
            1..1_000_000_i64,
            0..1_000_i64,
            [0..3_u8, 0..3_u8],
        )
            .prop_map(|(bid, spread, window_bid, window_spread, filled)| {
                let labels = RestingLabels::new(
                    price(window_bid),
                    price(window_bid + window_spread),
                    |side, bound| match (filled[side_index(side)], bound) {
                        (0, FillBound::Optimistic | FillBound::Conservative)
                        | (1, FillBound::Conservative) => None,
                        (1.., FillBound::Optimistic) => Some(TimeDelta::seconds(1)),
                        (2.., FillBound::Conservative) => Some(TimeDelta::seconds(2)),
                    },
                )
                .unwrap();
                row_at(bid, bid + spread, Ok(labels))
            })
    }

    fn row_at(bid: i64, ask: i64, resting: Result<RestingLabels, UnknownCause>) -> EventRow {
        EventRow::new(
            Symbol::new("AAA").unwrap(),
            arrival(),
            touch(bid, ask),
            Spread::from_ticks(0),
            Err(ExitUnread::PastLastRead),
            resting,
        )
        .unwrap()
    }

    proptest! {
        /// A resting fill pays less than crossing whenever the touch has a spread, and a conservative fill implies an
        /// optimistic one.
        #[test]
        fn test_a_resting_fill_beats_crossing_a_spread(row in any_row(), side in any_side()) {
            let crossed = enter(&row, side, Entry::Cross).unwrap().price();
            let locked = row.touch().bid() == row.touch().ask();
            for bound in FillBound::iter() {
                let entered = enter(&row, side, Entry::Rest(bound)).unwrap();
                if entered.how() == How::RestFilled {
                    let better = match side {
                        Side::Buy => entered.price() < crossed,
                        Side::Sell => entered.price() > crossed,
                    };
                    prop_assert!(better || (locked && entered.price() == crossed), "{:?} against {:?}", entered, crossed);
                }
            }
            let optimistic = enter(&row, side, Entry::Rest(FillBound::Optimistic)).unwrap();
            let conservative = enter(&row, side, Entry::Rest(FillBound::Conservative)).unwrap();
            prop_assert!(conservative.how() != How::RestFilled || optimistic.how() == How::RestFilled);
        }

        /// Choosing enters exactly as resting or as crossing does, by `rests` at the touch's half spread.
        #[test]
        fn test_choose_is_rest_or_cross(
            row in any_row(),
            side in any_side(),
            bound in any_bound(),
            fill in 0.0..=1.0_f64,
            markout in -100_000..100_000_i64,
        ) {
            let forecast = forecast(fill, markout);
            let chosen = enter(&row, side, Entry::Choose { forecast, bound }).unwrap();
            let half = half_spread(row.touch());
            let expected = match rests(forecast, half, half) {
                true => enter(&row, side, Entry::Rest(bound)).unwrap(),
                false => enter(&row, side, Entry::Cross).unwrap(),
            };
            prop_assert_eq!(chosen, expected);
        }

        /// A better markout never turns resting into crossing, nor does a cheaper window cross or a dearer cross now.
        #[test]
        fn test_rests_is_monotone(
            fill in 0.0..=1.0_f64,
            markout in -100_000..100_000_i64,
            raise in 0..10_000_i64,
            cross in 0..100_000_i64,
            window_cross in 0..100_000_i64,
        ) {
            let base = rests(forecast(fill, markout), per_share(cross), per_share(window_cross));
            prop_assert!(!base || rests(forecast(fill, markout + raise), per_share(cross), per_share(window_cross)));
            prop_assert!(!base || rests(forecast(fill, markout), per_share(cross + raise), per_share(window_cross)));
            prop_assert!(
                !base || rests(forecast(fill, markout), per_share(cross), per_share((window_cross - raise).max(0)))
            );
        }

        /// A sure fill rests exactly when its markout beats minus the cross, and a sure miss when the window's cross
        /// is cheaper than crossing now.
        #[test]
        fn test_rests_at_the_certain_ends(
            markout in -50..50_i64,
            cross in 0..50_i64,
            window_cross in 0..50_i64,
        ) {
            prop_assert_eq!(rests(forecast(1.0, markout), per_share(cross), per_share(window_cross)), markout > -cross);
            prop_assert_eq!(rests(forecast(0.0, markout), per_share(cross), per_share(window_cross)), window_cross < cross);
        }
    }
}
