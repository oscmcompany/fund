//! A study's account of its capital: a book funded with the opening equity, held to the trader's limits, a leverage on
//! equity and what can be borrowed, so trades are sized and refused as the account would and each session reads as a
//! return on capital.

use std::collections::BTreeMap;
use std::num::NonZeroU8;

use crate::common::book::{Book, Cash, Fill, Position, Side, ValuationRefusal};
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::market::borrow::Borrow;
use crate::common::market::{Price, SHARE_SCALE, Shares, Symbol};
use crate::common::monoid::Monoid;
use crate::common::risk::{Flat, Limit, Limits, NameLosses, Risk};
use crate::common::time::SessionDate;
use crate::common::time::calendar::SessionPhase;

/// How many times its equity an account's gross exposure may reach, as its margin terms set; one is a cash account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leverage(NonZeroU8);

impl Leverage {
    pub fn new(multiple: NonZeroU8) -> Self {
        Self(multiple)
    }

    pub fn multiple(self) -> NonZeroU8 {
        self.0
    }
}

/// A cap an opening trade is checked against.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Bound {
    /// The name's worth, long or short, against the per-name limit.
    PerName,
    /// Every holding's worth against the gross limit.
    Gross,
    /// Every holding's worth against equity times the leverage.
    BuyingPower,
}

/// Why a trade was refused; a trade that only reduces a holding is never refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TradeRefusal {
    #[error("risk allows no new exposure: {0:?}")]
    Flat(Flat),
    #[error("{symbol} cannot be borrowed to sell short")]
    Unborrowable { symbol: Symbol },
    /// `symbol` lost `loss` in the session, at least the per-name loss limit, so it opens nothing until the close.
    #[error("{symbol} is barred for the session by its loss of {:.2} dollars", .loss.dollars())]
    Barred { symbol: Symbol, loss: Cash },
    /// Short sales are whole shares, so a trade that opens or grows a short must leave it whole.
    #[error("{symbol} would be short a fraction of a share: {} millionths", .position.units())]
    FractionalShort { symbol: Symbol, position: Position },
    #[error("{bound} exposure of {:.2} dollars is past the {:.2} allowed", .exposure.dollars(), .allowed.dollars())]
    Exceeds {
        bound: Bound,
        exposure: Cash,
        allowed: Cash,
    },
    #[error("{0}")]
    Unpriced(ValuationRefusal),
}

/// What keeps an opening trade from growing past its room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binding {
    Flat(Flat),
    Unborrowable,
    Barred { loss: Cash },
    Bound(Bound),
}

/// The most shares one side may trade now: what closes the opposite holding, plus what opens before a cap binds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Room {
    shares: Shares,
    binding: Binding,
}

impl Room {
    pub fn shares(self) -> Shares {
        self.shares
    }

    pub fn binding(self) -> Binding {
        self.binding
    }
}

/// Why a ledger could not be opened or a session closed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerRefusal {
    #[error("an account opens with positive equity, not {:.2} dollars", .opening.dollars())]
    Unfunded { opening: Cash },
    /// The limits flatten the book before the close, so a holding past it is a study's error.
    #[error("{session} closed holding {symbols:?}")]
    HeldOverClose {
        session: SessionDate,
        symbols: Vec<Symbol>,
    },
    #[error("{session} does not follow {previous}")]
    Unordered {
        previous: SessionDate,
        session: SessionDate,
    },
}

/// The opening equity, the book and its terms, each name's session so far, and each closed session's equity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    limits: Limits,
    leverage: Leverage,
    opening: Cash,
    book: Book,
    /// Every name opens each session flat, as the book is flat at every close.
    losses: NameLosses,
    closes: BTreeMap<SessionDate, Cash>,
}

/// A position's worth at `price` whichever side it is on, in cash units.
fn exposure(position: Position, price: Price) -> i128 {
    position
        .units()
        .abs()
        .checked_mul(i128::from(price.ticks()))
        .expect("a position's worth fits i128")
}

/// Every holding's exposure summed, refused for the first holding `price` cannot answer.
fn gross(book: &Book, price: &impl Fn(&Symbol) -> Option<Price>) -> Result<i128, ValuationRefusal> {
    book.positions()
        .iter()
        .try_fold(0, |total, (symbol, position)| {
            let price = price(symbol).ok_or_else(|| ValuationRefusal::Unpriced {
                symbol: symbol.clone(),
            })?;
            Ok(total + exposure(*position, price))
        })
}

impl Ledger {
    pub fn open(opening: Cash, limits: Limits, leverage: Leverage) -> Result<Self, LedgerRefusal> {
        if opening.units() <= 0 {
            return Err(LedgerRefusal::Unfunded { opening });
        }
        Ok(Self {
            limits,
            leverage,
            opening,
            book: Book::funded(opening),
            losses: NameLosses::default(),
            closes: BTreeMap::new(),
        })
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn closes(&self) -> &BTreeMap<SessionDate, Cash> {
        &self.closes
    }

    /// The equity the current session opened with: the last close, or the opening before any.
    pub fn session_opening(&self) -> Cash {
        self.closes
            .last_key_value()
            .map_or(self.opening, |(_, equity)| *equity)
    }

    /// What risk allows now, held to the equity the session opened with.
    fn risk(
        &self,
        phase: SessionPhase,
        price: &impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<Risk, ValuationRefusal> {
        self.limits
            .risk(phase, self.session_opening(), &self.book, price)
    }

    /// Each name's session so far with every name barred whose loss at `price` reaches the per-name loss limit.
    fn losses(&self, price: &impl Fn(&Symbol) -> Option<Price>) -> NameLosses {
        let mut losses = self.losses.clone();
        losses.bar(&self.limits, price);
        losses
    }

    /// The cash each bound leaves for new exposure once `released` is closed, in the order ties resolve.
    fn headroom(
        &self,
        same_side: i128,
        released: i128,
        price: &impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<[(Bound, i128); 3], ValuationRefusal> {
        let remaining = gross(&self.book, price)? - released;
        let equity = self.book.value(price)?.units();
        let power = equity.saturating_mul(i128::from(self.leverage.0.get()));
        Ok([
            (
                Bound::PerName,
                self.limits.dollars(Limit::PerName).units() - same_side,
            ),
            (
                Bound::Gross,
                self.limits.dollars(Limit::Gross).units() - remaining,
            ),
            (Bound::BuyingPower, power - remaining),
        ])
    }

    /// The most shares of `symbol` a trade on `side` at `price` may carry before costs, with what binds it; the other
    /// holdings are marked by `marks`.
    pub fn room(
        &self,
        symbol: &Symbol,
        side: Side,
        price: Price,
        phase: SessionPhase,
        borrow: Borrow,
        marks: impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<Room, ValuationRefusal> {
        let price_of = |held: &Symbol| match held == symbol {
            true => Some(price),
            false => marks(held),
        };
        let held = self.book.position(symbol).units();
        let reduces = match side {
            Side::Buy => held < 0,
            Side::Sell => held > 0,
        };
        let (closing, holding) = match reduces {
            true => (held.unsigned_abs(), 0),
            false => (0, held.abs()),
        };
        let same_side = exposure(Position::from_units(holding), price);
        let released = exposure(Position::from_units(closing.cast_signed()), price);
        let barred = self.losses(&price_of).barred().get(symbol).copied();
        let opening_side = match (self.risk(phase, &price_of)?, barred, side, borrow) {
            (Risk::Flat(flat), _, _, _) => Err(Binding::Flat(flat)),
            (Risk::Capped { .. }, Some(loss), _, _) => Err(Binding::Barred { loss }),
            (Risk::Capped { .. }, None, Side::Sell, Borrow::Unavailable) => {
                Err(Binding::Unborrowable)
            }
            (Risk::Capped { .. }, None, Side::Buy, _)
            | (Risk::Capped { .. }, None, Side::Sell, Borrow::Available) => Ok(()),
        };
        let (opening, binding) = match opening_side {
            Err(binding) => (0, binding),
            Ok(()) => {
                let headroom = self.headroom(same_side, released, &price_of)?;
                let least = headroom
                    .iter()
                    .map(|(_, cash)| *cash)
                    .min()
                    .expect("there are three bounds");
                // A short must end on a whole share, so a sale steps the short it grows between whole shares.
                let step = match side {
                    Side::Buy => 1,
                    Side::Sell => i128::from(SHARE_SCALE),
                };
                let ticks = i128::from(price.ticks());
                let most = (holding + least.max(0) / ticks) / step * step;
                let (units, next) = ((most - holding).max(0), most + step - holding);
                // The bound the next step breaks first, in the order a trade checks them.
                let (bound, _) = headroom
                    .into_iter()
                    .find(|(_, cash)| *cash < next * ticks)
                    .expect("the next step breaks the least bound");
                (units.cast_unsigned(), Binding::Bound(bound))
            }
        };
        let total = u64::try_from(closing + opening).unwrap_or(u64::MAX);
        Ok(Room {
            shares: Shares::from_units(total),
            binding,
        })
    }

    /// The ledger after `fill`, refused when it opens exposure risk, the name's loss, borrow or a cap does not allow;
    /// the fill's own symbol is marked at its price and every other holding by `marks`.
    pub fn trade(
        &self,
        fill: &Fill,
        phase: SessionPhase,
        borrow: Borrow,
        marks: impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<Self, TradeRefusal> {
        let symbol = fill.symbol();
        let price_of = |held: &Symbol| match held == symbol {
            true => Some(fill.price()),
            false => marks(held),
        };
        let after = self.book.clone().combine(Book::of(fill));
        let (before, held) = (
            self.book.position(symbol).units(),
            after.position(symbol).units(),
        );
        let opens = held.abs() > before.abs() || held.signum() * before.signum() < 0;
        let mut losses = self.losses(&price_of);
        if opens {
            match self
                .risk(phase, &price_of)
                .map_err(TradeRefusal::Unpriced)?
            {
                Risk::Flat(flat) => return Err(TradeRefusal::Flat(flat)),
                Risk::Capped { .. } => {}
            }
            if let Some(loss) = losses.barred().get(symbol) {
                return Err(TradeRefusal::Barred {
                    symbol: symbol.clone(),
                    loss: *loss,
                });
            }
            if held < 0 {
                match borrow {
                    Borrow::Available => {}
                    Borrow::Unavailable => {
                        return Err(TradeRefusal::Unborrowable {
                            symbol: symbol.clone(),
                        });
                    }
                }
                if held % i128::from(SHARE_SCALE) != 0 {
                    return Err(TradeRefusal::FractionalShort {
                        symbol: symbol.clone(),
                        position: Position::from_units(held),
                    });
                }
            }
            let name = exposure(Position::from_units(held), fill.price());
            let total = gross(&after, &price_of).map_err(TradeRefusal::Unpriced)?;
            let equity = after
                .value(price_of)
                .map_err(TradeRefusal::Unpriced)?
                .units();
            let checks = [
                (
                    Bound::PerName,
                    name,
                    self.limits.dollars(Limit::PerName).units(),
                ),
                (
                    Bound::Gross,
                    total,
                    self.limits.dollars(Limit::Gross).units(),
                ),
                (
                    Bound::BuyingPower,
                    total,
                    equity.saturating_mul(i128::from(self.leverage.0.get())),
                ),
            ];
            if let Some((bound, exposure, allowed)) = checks
                .into_iter()
                .find(|(_, exposure, allowed)| exposure > allowed)
            {
                return Err(TradeRefusal::Exceeds {
                    bound,
                    exposure: Cash::from_units(exposure),
                    allowed: Cash::from_units(allowed),
                });
            }
        }
        losses.take(fill);
        Ok(Self {
            book: after,
            losses,
            ..self.clone()
        })
    }

    /// Records `session`'s closing equity, refused while a holding remains, since the limits flatten by the close.
    pub fn close(&self, session: SessionDate) -> Result<Self, LedgerRefusal> {
        if let Some((previous, _)) = self.closes.last_key_value()
            && *previous >= session
        {
            return Err(LedgerRefusal::Unordered {
                previous: *previous,
                session,
            });
        }
        if !self.book.positions().is_empty() {
            return Err(LedgerRefusal::HeldOverClose {
                session,
                symbols: self.book.positions().keys().cloned().collect(),
            });
        }
        let mut closed = self.clone();
        closed.losses = NameLosses::default();
        closed.closes.insert(session, self.book.cash());
        Ok(closed)
    }

    /// Each closed session's return on the equity it opened with, the series a Sharpe reads.
    pub fn returns(&self) -> Result<Series, SeriesRefusal> {
        let mut start = self.opening;
        Series::new(self.closes.iter().map(|(session, end)| {
            // A session that opened with no equity has no return on it.
            let reading = (start.units() > 0)
                .then(|| (end.units() - start.units()) as f64 / start.units() as f64);
            start = *end;
            (*session, reading)
        }))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::DollarVolume;
    use crate::common::monoid::concatenate;

    /// Cash units in a dollar: price ticks times share millionths.
    const DOLLAR: i128 = 1_000_000_000_000;

    fn dollars(amount: i128) -> Cash {
        Cash::from_units(amount * DOLLAR)
    }

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn price(dollars: i64) -> Price {
        Price::from_ticks(dollars * 1_000_000).unwrap()
    }

    fn at() -> DateTime<Utc> {
        "2026-03-02T15:00:00Z".parse().unwrap()
    }

    fn open() -> SessionPhase {
        SessionPhase::Open {
            until_close: TimeDelta::hours(2),
        }
    }

    fn ledger(gross: i128, per_name: i128, leverage: u8) -> Ledger {
        let limits = Limits::new(
            dollars(gross),
            dollars(per_name),
            dollars(1_000),
            dollars(1_000),
            TimeDelta::minutes(10),
        )
        .unwrap();
        Ledger::open(
            dollars(20_000),
            limits,
            Leverage::new(NonZeroU8::new(leverage).unwrap()),
        )
        .unwrap()
    }

    fn fill(name: &str, side: Side, millionths: u64, at_price: Price, cost_units: u128) -> Fill {
        Fill::new(
            at(),
            symbol(name),
            side,
            Shares::from_units(millionths),
            at_price,
            DollarVolume::from_units(cost_units),
        )
        .unwrap()
    }

    fn shares(whole: u64) -> u64 {
        whole * SHARE_SCALE
    }

    fn none(_: &Symbol) -> Option<Price> {
        None
    }

    #[test]
    fn test_a_buy_within_every_cap_pays_its_notional_and_cost() {
        let bought = ledger(40_000, 5_000, 2)
            .trade(
                &fill("AAA", Side::Buy, shares(100), price(50), DOLLAR as u128),
                open(),
                Borrow::Unavailable,
                none,
            )
            .unwrap();
        assert_eq!(bought.book().cash(), dollars(14_999));
        assert_eq!(
            bought.book().position(&symbol("AAA")).units(),
            100 * i128::from(SHARE_SCALE)
        );
    }

    #[test]
    fn test_each_cap_refuses_with_its_exposure_and_allowance() {
        let refused = |ledger: &Ledger, name: &str, whole: u64| {
            ledger
                .trade(
                    &fill(name, Side::Buy, shares(whole), price(50), 0),
                    open(),
                    Borrow::Available,
                    |_| Some(price(50)),
                )
                .unwrap_err()
        };
        assert_eq!(
            refused(&ledger(40_000, 5_000, 2), "AAA", 101),
            TradeRefusal::Exceeds {
                bound: Bound::PerName,
                exposure: dollars(5_050),
                allowed: dollars(5_000)
            }
        );
        let mut two = ledger(12_000, 5_000, 2);
        for name in ["AAA", "BBB"] {
            two = two
                .trade(
                    &fill(name, Side::Buy, shares(100), price(50), 0),
                    open(),
                    Borrow::Available,
                    |_| Some(price(50)),
                )
                .unwrap();
        }
        assert_eq!(
            refused(&two, "CCC", 100),
            TradeRefusal::Exceeds {
                bound: Bound::Gross,
                exposure: dollars(15_000),
                allowed: dollars(12_000)
            }
        );
        assert_eq!(
            refused(&ledger(40_000, 30_000, 1), "AAA", 401),
            TradeRefusal::Exceeds {
                bound: Bound::BuyingPower,
                exposure: dollars(20_050),
                allowed: dollars(20_000)
            }
        );
    }

    /// Ten AAA bought at $100 and sold at $95 lose $50, the per-name loss limit under a $1,000 daily one: a further
    /// buy of AAA is refused with its loss, its room is none, BBB still trades, and the next session trades AAA again.
    #[test]
    fn test_a_name_past_its_loss_limit_is_refused_until_the_session_closes() {
        let limits = Limits::new(
            dollars(40_000),
            dollars(5_000),
            dollars(1_000),
            dollars(50),
            TimeDelta::minutes(10),
        )
        .unwrap();
        let leverage = Leverage::new(NonZeroU8::new(2).unwrap());
        let mut traded = Ledger::open(dollars(20_000), limits, leverage).unwrap();
        for (side, at_price) in [(Side::Buy, price(100)), (Side::Sell, price(95))] {
            traded = traded
                .trade(
                    &fill("AAA", side, shares(10), at_price, 0),
                    open(),
                    Borrow::Available,
                    none,
                )
                .unwrap();
        }
        let again = fill("AAA", Side::Buy, shares(1), price(95), 0);
        assert_eq!(
            traded.trade(&again, open(), Borrow::Available, none),
            Err(TradeRefusal::Barred {
                symbol: symbol("AAA"),
                loss: dollars(50)
            })
        );
        assert_eq!(
            traded
                .room(
                    &symbol("AAA"),
                    Side::Buy,
                    price(95),
                    open(),
                    Borrow::Available,
                    none
                )
                .unwrap(),
            Room {
                shares: Shares::from_units(0),
                binding: Binding::Barred { loss: dollars(50) }
            }
        );
        assert!(
            traded
                .trade(
                    &fill("BBB", Side::Buy, shares(1), price(95), 0),
                    open(),
                    Borrow::Available,
                    none
                )
                .is_ok()
        );
        let next = traded.close(SessionDate::at(at())).unwrap();
        assert!(next.trade(&again, open(), Borrow::Available, none).is_ok());
    }

    #[test]
    fn test_the_flat_window_refuses_new_exposure_and_lets_a_holding_close() {
        let held = ledger(40_000, 5_000, 2)
            .trade(
                &fill("AAA", Side::Buy, shares(10), price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
            .unwrap();
        let closing = SessionPhase::Open {
            until_close: TimeDelta::minutes(5),
        };
        assert_eq!(
            held.trade(
                &fill("AAA", Side::Buy, shares(1), price(50), 0),
                closing,
                Borrow::Available,
                none
            ),
            Err(TradeRefusal::Flat(Flat::OutsideTradingWindow {
                phase: closing
            }))
        );
        let flat = held
            .trade(
                &fill("AAA", Side::Sell, shares(10), price(50), 0),
                closing,
                Borrow::Unavailable,
                none,
            )
            .unwrap();
        assert!(flat.book().positions().is_empty());
    }

    #[test]
    fn test_a_short_needs_borrow_and_whole_shares() {
        let empty = ledger(40_000, 5_000, 2);
        let short = |ledger: &Ledger, millionths: u64, borrow: Borrow| {
            ledger.trade(
                &fill("AAA", Side::Sell, millionths, price(50), 0),
                open(),
                borrow,
                none,
            )
        };
        assert_eq!(
            short(&empty, shares(10), Borrow::Unavailable),
            Err(TradeRefusal::Unborrowable {
                symbol: symbol("AAA")
            })
        );
        assert_eq!(
            short(&empty, shares(10) + 500_000, Borrow::Available),
            Err(TradeRefusal::FractionalShort {
                symbol: symbol("AAA"),
                position: Position::from_units(-10_500_000)
            })
        );
        let sold = short(&empty, shares(10), Borrow::Available).unwrap();
        assert_eq!(sold.book().position(&symbol("AAA")).units(), -10_000_000);
        assert_eq!(sold.book().cash(), dollars(20_500));
        // Selling through a long holding opens a short, so it needs borrow too.
        let long = empty
            .trade(
                &fill("AAA", Side::Buy, shares(10), price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
            .unwrap();
        assert_eq!(
            short(&long, shares(20), Borrow::Unavailable),
            Err(TradeRefusal::Unborrowable {
                symbol: symbol("AAA")
            })
        );
    }

    #[test]
    fn test_the_daily_loss_is_measured_from_the_last_close() {
        let session = |day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, day).unwrap());
        // Session 2 earns $1,000 (100 shares from $50 to $60) and closes at $21,000; session 3 buys 100 at $50.
        let held = ledger(40_000, 10_000, 2)
            .trade(
                &fill("AAA", Side::Buy, shares(100), price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
            .and_then(|ledger| {
                ledger.trade(
                    &fill("AAA", Side::Sell, shares(100), price(60), 0),
                    open(),
                    Borrow::Available,
                    none,
                )
            })
            .unwrap()
            .close(session(2))
            .unwrap()
            .trade(
                &fill("AAA", Side::Buy, shares(100), price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
            .unwrap();
        assert_eq!(held.session_opening(), dollars(21_000));
        // At $40 the session has lost exactly the $1,000 limit, though equity is still above the $20,000 funded.
        let marked_at = |dollars_each: i64| move |_: &Symbol| Some(price(dollars_each));
        let one_more = fill("AAA", Side::Buy, shares(1), price(40), 0);
        assert_eq!(
            held.trade(&one_more, open(), Borrow::Available, marked_at(40)),
            Err(TradeRefusal::Flat(Flat::LossLimit {
                loss: dollars(1_000)
            }))
        );
        let room = held
            .room(
                &symbol("AAA"),
                Side::Sell,
                price(40),
                open(),
                Borrow::Available,
                marked_at(40),
            )
            .unwrap();
        assert_eq!(
            (room.shares().units(), room.binding()),
            (
                shares(100),
                Binding::Flat(Flat::LossLimit {
                    loss: dollars(1_000)
                })
            )
        );
        assert!(
            held.trade(
                &fill("AAA", Side::Sell, shares(100), price(40), 0),
                open(),
                Borrow::Unavailable,
                marked_at(40)
            )
            .is_ok()
        );
        // A cent higher the loss is $999, and new exposure is still allowed.
        let cent_above = Price::from_ticks(40_010_000).unwrap();
        let below_limit = fill("AAA", Side::Buy, shares(1), cent_above, 0);
        assert!(
            held.trade(&below_limit, open(), Borrow::Available, |_| Some(
                cent_above
            ))
            .is_ok()
        );
    }

    #[test]
    fn test_buying_power_counts_the_fills_cost() {
        // Leverage one on $20,000: the room at $50 is 400 shares, which a costless fill fits exactly.
        let start = ledger(1_000_000, 1_000_000, 1);
        let room = start
            .room(
                &symbol("AAA"),
                Side::Buy,
                price(50),
                open(),
                Borrow::Available,
                none,
            )
            .unwrap();
        assert_eq!(
            (room.shares().units(), room.binding()),
            (shares(400), Binding::Bound(Bound::BuyingPower))
        );
        let buy = |whole, cost_units| fill("AAA", Side::Buy, shares(whole), price(50), cost_units);
        assert!(
            start
                .trade(&buy(400, 0), open(), Borrow::Available, none)
                .is_ok()
        );
        // A dollar of cost leaves $19,999 of equity under $20,000 of exposure.
        assert_eq!(
            start.trade(&buy(400, DOLLAR as u128), open(), Borrow::Available, none),
            Err(TradeRefusal::Exceeds {
                bound: Bound::BuyingPower,
                exposure: dollars(20_000),
                allowed: dollars(19_999)
            })
        );
        assert!(
            start
                .trade(&buy(399, DOLLAR as u128), open(), Borrow::Available, none)
                .is_ok()
        );
    }

    #[test]
    fn test_a_session_closes_only_flat_and_in_order() {
        let session = |day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, day).unwrap());
        let held = ledger(40_000, 5_000, 2)
            .trade(
                &fill("AAA", Side::Buy, shares(10), price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
            .unwrap();
        assert_eq!(
            held.close(session(2)),
            Err(LedgerRefusal::HeldOverClose {
                session: session(2),
                symbols: vec![symbol("AAA")]
            })
        );
        let closed = ledger(40_000, 5_000, 2).close(session(3)).unwrap();
        assert_eq!(closed.session_opening(), dollars(20_000));
        assert_eq!(
            closed.close(session(2)),
            Err(LedgerRefusal::Unordered {
                previous: session(3),
                session: session(2)
            })
        );
        assert_eq!(
            Ledger::open(
                dollars(0),
                *ledger(40_000, 5_000, 1).limits(),
                Leverage::new(NonZeroU8::MIN)
            ),
            Err(LedgerRefusal::Unfunded {
                opening: dollars(0)
            })
        );
    }

    #[test]
    fn test_returns_are_on_the_equity_each_session_opened_with() {
        let session = |day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, day).unwrap());
        // Buy 100 at $50 and sell at $52 (+$200 on $20,000), then buy 100 at $40 and sell at $39 (-$100 on $20,200).
        let mut ledger = ledger(40_000, 5_000, 2);
        for (day, buy, sell) in [(2, 50, 52), (3, 40, 39)] {
            ledger = ledger
                .trade(
                    &fill("AAA", Side::Buy, shares(100), price(buy), 0),
                    open(),
                    Borrow::Available,
                    none,
                )
                .and_then(|ledger| {
                    ledger.trade(
                        &fill("AAA", Side::Sell, shares(100), price(sell), 0),
                        open(),
                        Borrow::Available,
                        none,
                    )
                })
                .unwrap()
                .close(session(day))
                .unwrap();
        }
        let readings: Vec<(SessionDate, Option<f64>)> = ledger
            .returns()
            .unwrap()
            .readings()
            .iter()
            .map(|(session, reading)| (*session, *reading))
            .collect();
        assert_eq!(
            readings,
            [
                (session(2), Some(0.01)),
                (session(3), Some(-100.0 / 20_200.0))
            ]
        );
    }

    #[test]
    fn test_the_room_from_a_fractional_short_ends_on_a_whole_share() {
        let short = ledger(40_000, 5_000, 2)
            .trade(
                &fill("AAA", Side::Sell, shares(10), price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
            .and_then(|ledger| {
                ledger.trade(
                    &fill("AAA", Side::Buy, 500_000, price(50), 0),
                    open(),
                    Borrow::Unavailable,
                    none,
                )
            })
            .unwrap();
        let room = short
            .room(
                &symbol("AAA"),
                Side::Sell,
                price(50),
                open(),
                Borrow::Available,
                none,
            )
            .unwrap();
        assert_eq!(room.shares(), Shares::from_units(90_500_000));
        assert_eq!(room.binding(), Binding::Bound(Bound::PerName));
        let sell = |millionths| {
            short.trade(
                &fill("AAA", Side::Sell, millionths, price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
        };
        assert_eq!(
            sell(90_500_000).unwrap().book().position(&symbol("AAA")),
            Position::from_units(-100_000_000)
        );
        assert_eq!(
            sell(91_500_000),
            Err(TradeRefusal::Exceeds {
                bound: Bound::PerName,
                exposure: dollars(5_050),
                allowed: dollars(5_000)
            })
        );
    }

    #[test]
    fn test_the_room_past_a_closing_trade_is_bound_by_risk_or_borrow() {
        let held = ledger(40_000, 5_000, 2)
            .trade(
                &fill("AAA", Side::Buy, shares(10), price(50), 0),
                open(),
                Borrow::Available,
                none,
            )
            .unwrap();
        let room = |side, phase, borrow| {
            held.room(&symbol("AAA"), side, price(50), phase, borrow, none)
                .unwrap()
        };
        let closing = SessionPhase::Open {
            until_close: TimeDelta::minutes(5),
        };
        let flat = Binding::Flat(Flat::OutsideTradingWindow { phase: closing });
        assert_eq!(
            room(Side::Sell, open(), Borrow::Unavailable),
            Room {
                shares: Shares::from_units(shares(10)),
                binding: Binding::Unborrowable
            }
        );
        assert_eq!(
            room(Side::Sell, closing, Borrow::Available),
            Room {
                shares: Shares::from_units(shares(10)),
                binding: flat
            }
        );
        assert_eq!(
            room(Side::Buy, closing, Borrow::Available),
            Room {
                shares: Shares::from_units(0),
                binding: flat
            }
        );
        assert_eq!(
            held.trade(
                &fill("AAA", Side::Sell, shares(11), price(50), 0),
                open(),
                Borrow::Unavailable,
                none
            ),
            Err(TradeRefusal::Unborrowable {
                symbol: symbol("AAA")
            })
        );
    }

    /// One trade a property drives: a name, a side, whole shares, a price in dollars and a cost in cents under the
    /// one-dollar least notional, so every fill is valid.
    fn any_trade() -> impl Strategy<Value = (usize, bool, u64, i64, u128)> {
        (
            0_usize..3,
            any::<bool>(),
            1_u64..300,
            1_i64..200,
            0_u128..100,
        )
    }

    fn build(index: usize, buying: bool, whole: u64, dollars_each: i64, cents: u128) -> Fill {
        let side = match buying {
            true => Side::Buy,
            false => Side::Sell,
        };
        fill(
            ["AAA", "BBB", "CCC"][index],
            side,
            shares(whole),
            price(dollars_each),
            cents * (DOLLAR as u128 / 100),
        )
    }

    proptest! {
        /// The ledger's book is the funded book combined with each accepted fill's change, whatever it refused, so
        /// refusing a fill is exactly leaving it out.
        #[test]
        fn test_the_book_is_the_opening_combined_with_every_accepted_fill(
            trades in prop::collection::vec(any_trade(), 0..40),
        ) {
            let mut ledger = ledger(40_000, 5_000, 2);
            let mut accepted = vec![Book::funded(dollars(20_000))];
            for (index, buying, whole, dollars_each, cents) in trades {
                let fill = build(index, buying, whole, dollars_each, cents);
                if let Ok(next) = ledger.trade(&fill, open(), Borrow::Available, |_| Some(price(dollars_each))) {
                    accepted.push(Book::of(&fill));
                    ledger = next;
                }
            }
            prop_assert_eq!(ledger.book(), &concatenate(accepted));
        }

        /// A trade that only shrinks a holding passes in any phase and without borrow.
        #[test]
        fn test_a_trade_that_shrinks_a_holding_is_never_refused(
            (index, buying, whole, dollars_each, _) in any_trade(),
            fraction in 1_u64..=100,
        ) {
            let opened = build(index, buying, whole, dollars_each, 0);
            // Every opening fits these limits, so no case is skipped.
            let held = ledger(1_000_000, 1_000_000, 4).trade(&opened, open(), Borrow::Available, none).unwrap();
            let back = build(index, !buying, whole * fraction / 100, dollars_each * 3, 0);
            prop_assume!(back.shares().units() > 0);
            prop_assert!(held.trade(&back, SessionPhase::AfterClose, Borrow::Unavailable, none).is_ok());
        }

        /// The room is exactly what passes: trading it is accepted, and the next unit (a whole share for a short)
        /// is refused by the bound the room names, whichever bound that is.
        #[test]
        fn test_the_room_is_tight(
            (index, buying, whole, dollars_each, _) in any_trade(),
            others in prop::collection::vec((0.0_f64..1.0, any::<bool>()), 2),
            held in 0.0_f64..1.0,
            held_long in any::<bool>(),
            (gross, per_name, leverage) in (1_000_i128..30_000, 500_i128..20_000, 1_u8..4),
        ) {
            let mark = price(dollars_each);
            let name = ["AAA", "BBB", "CCC"][index];
            let mut start = ledger(gross, per_name, leverage);
            // Each holding is a share of the per-name limit, so most are accepted and every bound gets to bind.
            let whole_of = |share: f64| (share * per_name as f64 / dollars_each as f64) as u64;
            let positions = [(index, whole_of(held), held_long)].into_iter().chain(
                others
                    .iter()
                    .enumerate()
                    .map(|(offset, (share, long))| ((index + 1 + offset) % 3, whole_of(*share), *long)),
            );
            for (held_index, held_whole, long) in positions {
                if held_whole == 0 {
                    continue;
                }
                if let Ok(next) = start.trade(&build(held_index, long, held_whole, dollars_each, 0), open(), Borrow::Available, |_| Some(mark)) {
                    start = next;
                }
            }
            let side = build(index, buying, whole, dollars_each, 0).side();
            let room = start.room(&symbol(name), side, mark, open(), Borrow::Available, |_| Some(mark)).unwrap();
            let trade = |units: u64| {
                start.trade(&fill(name, side, units, mark, 0), open(), Borrow::Available, |_| Some(mark))
            };
            if room.shares().units() > 0 {
                prop_assert!(trade(room.shares().units()).is_ok(), "{room:?}");
            }
            let step = match side {
                Side::Buy => 1,
                Side::Sell => SHARE_SCALE,
            };
            match (room.binding(), trade(room.shares().units() + step)) {
                (Binding::Bound(bound), Err(TradeRefusal::Exceeds { bound: refused, .. })) => {
                    prop_assert_eq!(bound, refused)
                }
                (binding, outcome) => prop_assert!(false, "{binding:?} against {outcome:?}"),
            }
        }

        /// Loosening any limit or the leverage never shrinks the room on the same book.
        #[test]
        fn test_the_room_is_monotone_in_the_limits(
            trades in prop::collection::vec(any_trade(), 0..12),
            (index, buying, _, dollars_each, _) in any_trade(),
            (gross, per_name, leverage) in (1_000_i128..30_000, 500_i128..20_000, 1_u8..4),
            (more_gross, more_per_name, more_leverage) in (0_i128..20_000, 0_i128..20_000, 0_u8..3),
        ) {
            let mark = price(dollars_each);
            let mut tight = ledger(gross, per_name, leverage);
            for (held_index, held_buying, whole, _, _) in trades {
                if let Ok(next) = tight.trade(&build(held_index, held_buying, whole, dollars_each, 0), open(), Borrow::Available, |_| Some(mark)) {
                    tight = next;
                }
            }
            let loose = Ledger {
                book: tight.book().clone(),
                ..ledger(gross + more_gross, per_name + more_per_name, leverage + more_leverage)
            };
            let side = build(index, buying, 1, dollars_each, 0).side();
            let room = |ledger: &Ledger| {
                ledger.room(&symbol(["AAA", "BBB", "CCC"][index]), side, mark, open(), Borrow::Available, |_| Some(mark)).unwrap().shares().units()
            };
            prop_assert!(room(&tight) <= room(&loose), "{:?} against {:?}", room(&tight), room(&loose));
        }

        /// Each session's return carries the equity it opened with to its close, so the returns compound to the final
        /// equity over the opening.
        #[test]
        fn test_returns_compound_to_the_final_equity(
            sessions in prop::collection::vec((40_i64..60, 40_i64..60, 0_u128..100), 1..20),
        ) {
            let mut ledger = ledger(40_000, 5_000, 2);
            let first = NaiveDate::from_ymd_opt(2026, 3, 2).unwrap();
            for (offset, (buy, sell, cents)) in sessions.iter().enumerate() {
                let session = SessionDate::from_date(first + TimeDelta::days(offset as i64));
                ledger = ledger
                    .trade(&build(0, true, 10, *buy, *cents), open(), Borrow::Available, none)
                    .and_then(|ledger| ledger.trade(&build(0, false, 10, *sell, 0), open(), Borrow::Available, none))
                    .unwrap()
                    .close(session)
                    .unwrap();
            }
            let returns = ledger.returns().unwrap();
            prop_assert_eq!(returns.readings().len(), sessions.len());
            let compounded = returns
                .readings()
                .values()
                .map(|reading| 1.0 + reading.unwrap())
                .product::<f64>();
            let expected = ledger.session_opening().units() as f64 / dollars(20_000).units() as f64;
            prop_assert!((compounded - expected).abs() < 1e-9, "{compounded} against {expected}");
        }
    }
}
