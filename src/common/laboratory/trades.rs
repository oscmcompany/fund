//! An arm as a list of intraday trades booked through the capital `Ledger`, so overlapping trades net in one book,
//! every entry is sized and refused as the account would, and each session reads as a return on capital.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

use crate::common::book::{Cash, Fill, FillRefusal, Position, Side, ValuationRefusal};
use crate::common::cost::PerShare;
use crate::common::cost::fees::{ChargeRefusal, FeeRegime, Liquidity};
use crate::common::laboratory::entry::{Entered, How};
use crate::common::laboratory::ledger::{
    Binding, Bound, Ledger, LedgerRefusal, Leverage, TradeRefusal,
};
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::market::borrow::Borrow;
use crate::common::market::{DollarVolume, Price, SHARE_SCALE, Shares, Symbol};
use crate::common::risk::{Limit, Limits};
use crate::common::time::SessionDate;
use crate::common::time::calendar::{SessionPhase, TradingCalendar};

/// Where an arm's borrow readings come from; a snapshot is read on sessions other than the one it was fetched on,
/// so its availability is not point in time, and every reading of the arm says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowProvenance {
    Snapshot { fetched: SessionDate },
    PointInTime,
}

impl BorrowProvenance {
    /// The flag a reading is journaled under, naming a snapshot's fetch session and that it is not point in time.
    pub fn flag(self) -> String {
        match self {
            Self::Snapshot { fetched } => format!("snapshot(fetched={fetched}, not point in time)"),
            Self::PointInTime => "point_in_time".to_string(),
        }
    }
}

/// Which names can be borrowed to sell short, under the provenance every reading carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Borrows {
    provenance: BorrowProvenance,
    availability: BTreeMap<Symbol, Borrow>,
}

impl Borrows {
    pub fn new(provenance: BorrowProvenance, availability: BTreeMap<Symbol, Borrow>) -> Self {
        Self {
            provenance,
            availability,
        }
    }

    pub fn provenance(&self) -> BorrowProvenance {
        self.provenance
    }

    /// A name's borrow; one off the list was not listed borrowable, which is a reading, not an absence.
    pub fn availability(&self, symbol: &Symbol) -> Borrow {
        self.availability
            .get(symbol)
            .copied()
            .unwrap_or(Borrow::Unavailable)
    }
}

/// One intraday round trip an arm asks for: an entry already read off an event row and the exit that closes it,
/// both inside one session.
#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    symbol: Symbol,
    side: Side,
    shares: Shares,
    entered_at: DateTime<Utc>,
    entered: Entered,
    exited_at: DateTime<Utc>,
    exit: Price,
}

/// Why a trade is not a round trip, with the instants or shares at fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TradeShape {
    #[error("a trade of no shares trades nothing")]
    NothingTraded,
    #[error("the exit at {exited_at} is not after the entry at {entered_at}")]
    ExitNotAfterEntry {
        entered_at: DateTime<Utc>,
        exited_at: DateTime<Utc>,
    },
    #[error("the entry on {entered} and the exit on {exited} cross a session boundary")]
    AcrossSessions {
        entered: SessionDate,
        exited: SessionDate,
    },
}

impl Trade {
    pub fn new(
        symbol: Symbol,
        side: Side,
        shares: Shares,
        entered_at: DateTime<Utc>,
        entered: Entered,
        exited_at: DateTime<Utc>,
        exit: Price,
    ) -> Result<Self, TradeShape> {
        if shares.is_zero() {
            return Err(TradeShape::NothingTraded);
        }
        if exited_at <= entered_at {
            return Err(TradeShape::ExitNotAfterEntry {
                entered_at,
                exited_at,
            });
        }
        let (entered_on, exited_on) = (SessionDate::at(entered_at), SessionDate::at(exited_at));
        if entered_on != exited_on {
            return Err(TradeShape::AcrossSessions {
                entered: entered_on,
                exited: exited_on,
            });
        }
        Ok(Self {
            symbol,
            side,
            shares,
            entered_at,
            entered,
            exited_at,
            exit,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn shares(&self) -> Shares {
        self.shares
    }

    pub fn entered_at(&self) -> DateTime<Utc> {
        self.entered_at
    }

    pub fn entered(&self) -> Entered {
        self.entered
    }

    pub fn exited_at(&self) -> DateTime<Utc> {
        self.exited_at
    }

    pub fn exit(&self) -> Price {
        self.exit
    }

    pub fn session(&self) -> SessionDate {
        SessionDate::at(self.entered_at)
    }
}

/// The account both arms of a comparison share, so they differ only in their trades.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Terms {
    opening: Cash,
    limits: Limits,
    leverage: Leverage,
    fees: FeeRegime,
}

impl Terms {
    pub fn new(opening: Cash, limits: Limits, leverage: Leverage, fees: FeeRegime) -> Self {
        Self {
            opening,
            limits,
            leverage,
            fees,
        }
    }

    pub fn opening(&self) -> Cash {
        self.opening
    }

    /// The settings a comparison journals, so two readings under different terms are told apart.
    pub fn settings(&self) -> Vec<(&'static str, String)> {
        let limit = |limit: Limit| self.limits.dollars(limit).units().to_string();
        vec![
            ("opening_cash_units", self.opening.units().to_string()),
            ("leverage", self.leverage.multiple().to_string()),
            ("fees", format!("{:?}", self.fees)),
            ("gross_limit_units", limit(Limit::Gross)),
            ("per_name_limit_units", limit(Limit::PerName)),
            ("daily_loss_limit_units", limit(Limit::DailyLoss)),
            ("per_name_loss_limit_units", limit(Limit::PerNameLoss)),
            // Debug spells the seconds and nanoseconds, so two windows differing below a second are told apart.
            (
                "flat_before_close",
                format!("{:?}", self.limits.flat_before_close()),
            ),
        ]
    }
}

/// What kept a taken entry below the shares it asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trimmed {
    /// The ledger's room, with what bound it.
    Room(Binding),
    /// A short opens only whole shares, so a fractional ask is floored.
    WholeShare,
}

/// Why a trade was not taken at all.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RefusalCause {
    /// The name's net holding sits on the other side, and reducing it would break the trades that opened it.
    #[error("the book holds {} units of the name on the other side", .held.units())]
    OpposesHolding { held: Position },
    #[error("the ledger has no room: {binding:?}")]
    NoRoom { binding: Binding },
    #[error("a short sized to {} millionths of a share is under one whole share and opens nothing", .sized.units())]
    ShortBelowOneShare { sized: Shares },
    #[error("the entry's fee could not be charged: {0}")]
    Uncharged(#[from] ChargeRefusal),
    /// The fee exceeds the entry's notional, which no fill can carry.
    #[error("the fee of {} units exceeds the notional of {} units", .fee.units(), .notional.units())]
    FeeBeyondNotional {
        fee: DollarVolume,
        notional: DollarVolume,
    },
    #[error("the ledger refused the entry: {0}")]
    Trade(#[from] TradeRefusal),
}

/// What booking did with one trade.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Taken {
        shares: Shares,
        trimmed: Option<Trimmed>,
        /// The round trip's realized gain in cash units, fees taken out, exact by construction.
        net: Cash,
    },
    Refused {
        cause: RefusalCause,
    },
}

/// One trade and what became of it, in the order the arm asked.
#[derive(Debug, Clone, PartialEq)]
pub struct Booking {
    trade: Trade,
    outcome: Outcome,
}

impl Booking {
    pub fn trade(&self) -> &Trade {
        &self.trade
    }

    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }
}

/// Why an arm could not be booked at all; a single trade the account cannot take is a refusal inside `Booked`,
/// never this.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BookingError {
    #[error("a booking needs at least one session to close")]
    NoSessions,
    #[error("a trade on {session} falls outside the sessions the booking closes")]
    OutsideSessions { session: SessionDate },
    #[error("{session} is not a trading day on the calendar")]
    NotTradingDay { session: SessionDate },
    /// A holding past the close booked as a flat session would be fiction, so the trade list is refused whole.
    #[error("the exit of {symbol} at {exited_at} falls after the session's close at {close}")]
    ExitPastClose {
        symbol: Symbol,
        exited_at: DateTime<Utc>,
        close: DateTime<Utc>,
    },
    #[error("the ledger was refused: {0}")]
    Ledger(#[from] LedgerRefusal),
    #[error("a holding could not be marked: {0}")]
    Unpriced(ValuationRefusal),
    /// An exit only reduces its own entry, so a refusal here is a defect, not a reading.
    #[error("the exit of {symbol} was refused: {cause}")]
    ExitRefused { symbol: Symbol, cause: TradeRefusal },
    #[error("the exit of {symbol} could not be charged its fee: {cause}")]
    ExitUncharged {
        symbol: Symbol,
        cause: ChargeRefusal,
    },
    #[error("the exit of {symbol} carries a fee of {} units beyond its notional of {} units", .fee.units(), .notional.units())]
    ExitFeeBeyondNotional {
        symbol: Symbol,
        fee: DollarVolume,
        notional: DollarVolume,
    },
}

/// An arm booked: its final ledger, what became of each trade, and the borrow provenance every reading carries.
#[derive(Debug, Clone, PartialEq)]
pub struct Booked {
    ledger: Ledger,
    bookings: Vec<Booking>,
    provenance: BorrowProvenance,
    opening: Cash,
}

impl Booked {
    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    pub fn bookings(&self) -> &[Booking] {
        &self.bookings
    }

    pub fn provenance(&self) -> BorrowProvenance {
        self.provenance
    }

    /// Each closed session's return on the equity it opened with.
    pub fn returns(&self) -> Result<Series, SeriesRefusal> {
        self.ledger.returns()
    }

    /// Each taken trade's net gain over the opening equity, at its entry instant, for the block errors.
    pub fn gains(&self) -> Vec<(DateTime<Utc>, Symbol, Option<f64>)> {
        self.bookings
            .iter()
            .filter_map(|booking| match &booking.outcome {
                Outcome::Taken { net, .. } => Some((
                    booking.trade.entered_at,
                    booking.trade.symbol.clone(),
                    Some(net.units() as f64 / self.opening.units() as f64),
                )),
                Outcome::Refused { .. } => None,
            })
            .collect()
    }

    /// How many trades were taken, how many of those below their ask, and how many refused.
    pub fn counts(&self) -> (u64, u64, u64) {
        let mut counts = (0, 0, 0);
        for booking in &self.bookings {
            match &booking.outcome {
                Outcome::Taken { trimmed, .. } => {
                    counts.0 += 1;
                    counts.1 += u64::from(trimmed.is_some());
                }
                Outcome::Refused { .. } => counts.2 += 1,
            }
        }
        counts
    }

    /// The booked arm's whole-period return on its opening equity.
    pub fn net_return(&self) -> f64 {
        (self.ledger.session_opening().units() - self.opening.units()) as f64
            / self.opening.units() as f64
    }
}

/// The liquidity an entry's fee is charged at: a fill that rested added liquidity, and one that crossed, at entry
/// or at the window's end, removed it.
fn liquidity(how: How) -> Liquidity {
    match how {
        How::RestFilled => Liquidity::Adding,
        How::Crossed | How::RestThenCrossed => Liquidity::Removing,
    }
}

/// A fill's fee in dollar-volume units; a rebate past the fee is not credited, so a resting fill never books
/// better than free.
fn fee_units(fee: PerShare, shares: Shares) -> DollarVolume {
    let millionths = u128::try_from(fee.millionths().max(0)).expect("a nonnegative fee");
    DollarVolume::from_units(millionths * u128::from(shares.units()))
}

/// The realized net of one round trip in cash units: the sale's notional less the purchase's, less both fees;
/// exact, and bounded far inside an `i128` by the price and share bounds.
fn net_units(side: Side, shares: Shares, entry: Price, exit: Price, fees: u128) -> i128 {
    let notional = |price: Price| {
        i128::try_from(DollarVolume::of(price, shares).units()).expect("a notional fits an i128")
    };
    let swing = match side {
        Side::Buy => notional(exit) - notional(entry),
        Side::Sell => notional(entry) - notional(exit),
    };
    swing - i128::try_from(fees).expect("two fees at most their notionals fit an i128")
}

/// The largest entry that fits buying power net of its own fee, rebuilt from the refusal's numbers: the gross
/// beside this entry stays fixed while equity loses the fee, so the units solve
/// `ticks x units + others <= leverage x (equity - fee x units)` exactly; `None` when nothing fits.
fn resized(
    trade: &Trade,
    fill: &Fill,
    terms: &Terms,
    exposure: Cash,
    allowed: Cash,
) -> Option<Fill> {
    let ticks = i128::from(trade.entered.price().ticks());
    let units = i128::from(fill.shares().units());
    let leverage = i128::from(terms.leverage.multiple().get());
    let fee = terms
        .fees
        .per_share(
            liquidity(trade.entered.how()),
            trade.side,
            trade.entered.price(),
        )
        .ok()?;
    let fee = i128::from(fee.millionths().max(0));
    let others = exposure.units() - ticks * units;
    let numerator = allowed.units() + leverage * fee * units - others;
    let fitting = numerator.div_euclid(ticks + leverage * fee);
    let fitting = match trade.side {
        Side::Buy => fitting,
        Side::Sell => fitting - fitting.rem_euclid(i128::from(SHARE_SCALE)),
    };
    // A resize only shrinks; anything else says the refusal was not the fee's doing.
    if fitting <= 0 || fitting >= units {
        return None;
    }
    let shares = Shares::from_units(u64::try_from(fitting).ok()?);
    Fill::new(
        fill.filled_against(),
        trade.symbol.clone(),
        trade.side,
        shares,
        trade.entered.price(),
        fee_units(
            PerShare::from_millionths(i64::try_from(fee).ok()?).ok()?,
            shares,
        ),
    )
    .ok()
}

/// One leg of a trade in the event order; exits sort before entries at one instant, so a closed trade frees its
/// room for an entry arriving as it ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Leg {
    Exit,
    Entry,
}

/// A taken entry waiting for its exit.
struct OpenEntry {
    shares: Shares,
    trimmed: Option<Trimmed>,
    fee: DollarVolume,
}

/// Books `trades` through one ledger under `terms`, closing every session of `sessions` in order, eventless ones
/// included, so two arms booked over the same sessions read on the same series. An entry against the name's net
/// holding is refused rather than allowed to reduce other trades' exposure, so every exit only reduces and the book
/// is flat at each close. A trade on a day the calendar does not trade, or exiting after its session's close,
/// refuses the whole booking before anything is booked.
pub fn book(
    terms: &Terms,
    calendar: &TradingCalendar,
    borrows: &Borrows,
    sessions: &BTreeSet<SessionDate>,
    trades: Vec<Trade>,
) -> Result<Booked, BookingError> {
    if sessions.is_empty() {
        return Err(BookingError::NoSessions);
    }
    if let Some(outside) = trades
        .iter()
        .map(Trade::session)
        .find(|session| !sessions.contains(session))
    {
        return Err(BookingError::OutsideSessions { session: outside });
    }
    for trade in &trades {
        let Some(hours) = calendar.session(trade.session()) else {
            return Err(BookingError::NotTradingDay {
                session: trade.session(),
            });
        };
        let (_, close) = hours.hours();
        if trade.exited_at > close {
            return Err(BookingError::ExitPastClose {
                symbol: trade.symbol.clone(),
                exited_at: trade.exited_at,
                close,
            });
        }
    }
    let mut ledger = Ledger::open(terms.opening, terms.limits, terms.leverage)?;
    let mut events: Vec<(DateTime<Utc>, Leg, usize)> = (0..trades.len())
        .flat_map(|index| {
            [
                (trades[index].entered_at, Leg::Entry, index),
                (trades[index].exited_at, Leg::Exit, index),
            ]
        })
        .collect();
    events.sort();
    let mut events = events.into_iter().peekable();
    let mut marks: BTreeMap<Symbol, Price> = BTreeMap::new();
    let mut open: BTreeMap<usize, OpenEntry> = BTreeMap::new();
    let mut outcomes: BTreeMap<usize, Outcome> = BTreeMap::new();
    for session in sessions {
        while let Some((at, leg, index)) =
            events.next_if(|(at, ..)| SessionDate::at(*at) <= *session)
        {
            let trade = &trades[index];
            let phase = calendar.phase_at(at);
            let borrow = borrows.availability(&trade.symbol);
            // Either leg's price is an observation of the tape whichever way the booking goes, so it marks the
            // name for later risk checks, a refused entry's and a skipped exit's included.
            marks.insert(
                trade.symbol.clone(),
                match leg {
                    Leg::Entry => trade.entered.price(),
                    Leg::Exit => trade.exit,
                },
            );
            match leg {
                Leg::Entry => match take(&ledger, trade, phase, borrow, terms.fees, &marks)? {
                    Entry::Filled { fill, trimmed } => {
                        let mut accept = |fill: &Fill, trimmed| {
                            open.insert(
                                index,
                                OpenEntry {
                                    shares: fill.shares(),
                                    trimmed,
                                    fee: fill.cost(),
                                },
                            );
                        };
                        let mut refuse = |cause| {
                            outcomes.insert(
                                index,
                                Outcome::Refused {
                                    cause: RefusalCause::Trade(cause),
                                },
                            );
                        };
                        match ledger.trade(&fill, phase, borrow, |held| marks.get(held).copied()) {
                            Ok(next) => {
                                ledger = next;
                                accept(&fill, trimmed);
                            }
                            // The room was read before the fee, which only buying power feels through equity, so
                            // the entry is resized once to fit net of its own fee and tried again.
                            Err(TradeRefusal::Exceeds {
                                bound: Bound::BuyingPower,
                                exposure,
                                allowed,
                            }) => match resized(trade, &fill, terms, exposure, allowed) {
                                Some(smaller) => {
                                    match ledger.trade(&smaller, phase, borrow, |held| {
                                        marks.get(held).copied()
                                    }) {
                                        Ok(next) => {
                                            ledger = next;
                                            accept(
                                                &smaller,
                                                Some(Trimmed::Room(Binding::Bound(
                                                    Bound::BuyingPower,
                                                ))),
                                            );
                                        }
                                        Err(cause) => refuse(cause),
                                    }
                                }
                                None => refuse(TradeRefusal::Exceeds {
                                    bound: Bound::BuyingPower,
                                    exposure,
                                    allowed,
                                }),
                            },
                            Err(cause) => refuse(cause),
                        }
                    }
                    Entry::Refused { cause } => {
                        outcomes.insert(index, Outcome::Refused { cause });
                    }
                },
                Leg::Exit => {
                    let Some(entry) = open.remove(&index) else {
                        continue;
                    };
                    let exit_side = match trade.side {
                        Side::Buy => Side::Sell,
                        Side::Sell => Side::Buy,
                    };
                    let fee = terms
                        .fees
                        .per_share(Liquidity::Removing, exit_side, trade.exit)
                        .map_err(|cause| BookingError::ExitUncharged {
                            symbol: trade.symbol.clone(),
                            cause,
                        })?;
                    let cost = fee_units(fee, entry.shares);
                    let fill = Fill::new(
                        at,
                        trade.symbol.clone(),
                        exit_side,
                        entry.shares,
                        trade.exit,
                        cost,
                    )
                    .map_err(
                        |FillRefusal::CostBeyondNotional { cost, notional }| {
                            BookingError::ExitFeeBeyondNotional {
                                symbol: trade.symbol.clone(),
                                fee: cost,
                                notional,
                            }
                        },
                    )?;
                    ledger = ledger
                        .trade(&fill, phase, borrow, |held| marks.get(held).copied())
                        .map_err(|cause| BookingError::ExitRefused {
                            symbol: trade.symbol.clone(),
                            cause,
                        })?;
                    let fees = entry.fee.units() + cost.units();
                    let net = net_units(
                        trade.side,
                        entry.shares,
                        trade.entered.price(),
                        trade.exit,
                        fees,
                    );
                    outcomes.insert(
                        index,
                        Outcome::Taken {
                            shares: entry.shares,
                            trimmed: entry.trimmed,
                            net: Cash::from_units(net),
                        },
                    );
                }
            }
        }
        ledger = ledger.close(*session)?;
    }
    let bookings = trades
        .into_iter()
        .enumerate()
        .map(|(index, trade)| Booking {
            trade,
            outcome: outcomes
                .remove(&index)
                .expect("every trade's entry recorded an outcome or its exit did"),
        })
        .collect();
    Ok(Booked {
        ledger,
        bookings,
        provenance: borrows.provenance(),
        opening: terms.opening,
    })
}

/// What `take` decided for one entry before the ledger applies it.
enum Entry {
    Filled {
        fill: Fill,
        trimmed: Option<Trimmed>,
    },
    Refused {
        cause: RefusalCause,
    },
}

/// Sizes one entry: refused against an opposing holding, trimmed to the ledger's room, a short floored to whole
/// shares, and charged its fee at the liquidity its entry path took.
fn take(
    ledger: &Ledger,
    trade: &Trade,
    phase: SessionPhase,
    borrow: Borrow,
    fees: FeeRegime,
    marks: &BTreeMap<Symbol, Price>,
) -> Result<Entry, BookingError> {
    let held = ledger.book().position(&trade.symbol);
    let direction = match trade.side {
        Side::Buy => 1,
        Side::Sell => -1,
    };
    if held.units() != 0 && held.units().signum() != direction {
        return Ok(Entry::Refused {
            cause: RefusalCause::OpposesHolding { held },
        });
    }
    let room = ledger
        .room(
            &trade.symbol,
            trade.side,
            trade.entered.price(),
            phase,
            borrow,
            |held| marks.get(held).copied(),
        )
        .map_err(BookingError::Unpriced)?;
    if room.shares().is_zero() {
        return Ok(Entry::Refused {
            cause: RefusalCause::NoRoom {
                binding: room.binding(),
            },
        });
    }
    let asked = trade.shares.units();
    let roomed = asked.min(room.shares().units());
    let units = match trade.side {
        Side::Buy => roomed,
        // A short opens only whole shares; with no opposing holding, the whole entry opens.
        Side::Sell => roomed - roomed % SHARE_SCALE,
    };
    if units == 0 {
        return Ok(Entry::Refused {
            cause: RefusalCause::ShortBelowOneShare {
                sized: Shares::from_units(roomed),
            },
        });
    }
    let trimmed = match (units < asked, roomed < asked) {
        (false, _) => None,
        (true, true) => Some(Trimmed::Room(room.binding())),
        (true, false) => Some(Trimmed::WholeShare),
    };
    let taken = Shares::from_units(units);
    let fee = match fees.per_share(
        liquidity(trade.entered.how()),
        trade.side,
        trade.entered.price(),
    ) {
        Ok(fee) => fee,
        Err(cause) => {
            return Ok(Entry::Refused {
                cause: RefusalCause::Uncharged(cause),
            });
        }
    };
    let cost = fee_units(fee, taken);
    match Fill::new(
        trade.entered_at,
        trade.symbol.clone(),
        trade.side,
        taken,
        trade.entered.price(),
        cost,
    ) {
        Ok(fill) => Ok(Entry::Filled { fill, trimmed }),
        Err(FillRefusal::CostBeyondNotional { cost, notional }) => Ok(Entry::Refused {
            cause: RefusalCause::FeeBeyondNotional {
                fee: cost,
                notional,
            },
        }),
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// Any valid round trip's net is computed without overflow: any two prices, any share count and any fee pair
    /// bounded by the notionals, as `Fill` bounds each leg's cost. The net's sign is not proven here; the
    /// conservation proptest checks every taken net against the ledger's cash.
    #[kani::proof]
    fn a_round_trips_net_never_overflows() {
        let entry = Price::from_ticks(kani::any());
        let exit = Price::from_ticks(kani::any());
        kani::assume(entry.is_ok() && exit.is_ok());
        let (entry, exit) = (entry.expect("assumed valid"), exit.expect("assumed valid"));
        let shares = Shares::from_units(kani::any());
        let side = match kani::any() {
            true => Side::Buy,
            false => Side::Sell,
        };
        let entry_notional = DollarVolume::of(entry, shares).units();
        let exit_notional = DollarVolume::of(exit, shares).units();
        let fees: u128 = kani::any();
        kani::assume(fees <= entry_notional + exit_notional);
        let net = net_units(side, shares, entry, exit, fees);
        // The net is at most the larger notional and at least minus the sum of both and the fees.
        kani::cover!(net > 0);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU8;

    use chrono::TimeDelta;
    use proptest::prelude::*;

    use super::*;
    use crate::common::cost::fees::{PassThroughSchedule, RegulatoryFees};
    use crate::common::laboratory::entry::{EventRow, ExitUnread, enter};
    use crate::common::market::Dollars;
    use crate::common::market::quote_bars::{Spread, StandingQuote};
    use crate::common::time::calendar::TradingSession;

    /// Cash units in a dollar: price ticks times share millionths.
    const DOLLAR: i128 = 1_000_000_000_000;

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn price(dollars: f64) -> Price {
        Price::from_ticks((dollars * 1e6).round() as i64).unwrap()
    }

    fn session(day: u32) -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 3, day).unwrap())
    }

    /// An instant `minutes` past 10:00 Eastern (15:00 UTC, the winter open phase) on `day`.
    fn at(day: u32, minutes: i64) -> DateTime<Utc> {
        format!("2026-03-{day:02}T15:00:00Z")
            .parse::<DateTime<Utc>>()
            .unwrap()
            + TimeDelta::minutes(minutes)
    }

    fn calendar() -> TradingCalendar {
        let sessions = (2..=6)
            .map(|day| {
                TradingSession::new(
                    session(day),
                    chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                    chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
                )
                .unwrap()
            })
            .collect();
        TradingCalendar::new(
            sessions,
            crate::common::time::SessionRange::new(session(2), session(6)).unwrap(),
        )
        .unwrap()
    }

    fn free() -> FeeRegime {
        FeeRegime::CommissionFree {
            regulatory: RegulatoryFees::new(Dollars::from_millionths(0), PerShare::ZERO).unwrap(),
        }
    }

    fn terms(fees: FeeRegime) -> Terms {
        let limits = Limits::new(
            Cash::from_units(40_000 * DOLLAR),
            Cash::from_units(5_000 * DOLLAR),
            Cash::from_units(100_000 * DOLLAR),
            Cash::from_units(100_000 * DOLLAR),
            TimeDelta::minutes(10),
        )
        .unwrap();
        Terms::new(
            Cash::from_units(20_000 * DOLLAR),
            limits,
            Leverage::new(NonZeroU8::new(2).unwrap()),
            fees,
        )
    }

    fn borrows(available: &[&str]) -> Borrows {
        Borrows::new(
            BorrowProvenance::Snapshot {
                fetched: session(2),
            },
            available
                .iter()
                .map(|name| (symbol(name), Borrow::Available))
                .collect(),
        )
    }

    /// A crossed entry of `whole` shares at a one-cent touch around `entry` dollars, exiting at `exit` dollars
    /// `minutes + 5` in.
    fn trade(
        name: &str,
        side: Side,
        whole: f64,
        entry: f64,
        exit: f64,
        day: u32,
        minutes: i64,
    ) -> Trade {
        let (bid, ask) = (entry - 0.01, entry + 0.01);
        let size = Shares::whole(1_000).unwrap();
        let touch = StandingQuote::new(
            at(day, minutes) - TimeDelta::seconds(1),
            price(bid),
            price(ask),
            size,
            size,
        )
        .unwrap();
        let row = EventRow::new(
            symbol(name),
            at(day, minutes),
            touch,
            Spread::from_ticks(10_000),
            Err(ExitUnread::PastLastRead),
            Err(crate::common::laboratory::entry::UnknownCause::WindowPastLastRead),
        )
        .unwrap();
        let entered = enter(&row, side, crate::common::laboratory::entry::Entry::Cross).unwrap();
        Trade::new(
            symbol(name),
            side,
            Shares::from_units((whole * SHARE_SCALE as f64).round() as u64),
            at(day, minutes),
            entered,
            at(day, minutes + 5),
            price(exit),
        )
        .unwrap()
    }

    fn sessions(days: &[u32]) -> BTreeSet<SessionDate> {
        days.iter().map(|day| session(*day)).collect()
    }

    #[test]
    fn test_a_round_trip_books_both_fills_and_nets_exactly() {
        // Buy 90 at the $50.01 ask, sell at $50.51: 90 x $0.50 = $45, free of fees.
        let booked = book(
            &terms(free()),
            &calendar(),
            &borrows(&[]),
            &sessions(&[2]),
            vec![trade("AAA", Side::Buy, 90.0, 50.0, 50.51, 2, 0)],
        )
        .unwrap();
        let (taken, trimmed, refused) = booked.counts();
        assert_eq!((taken, trimmed, refused), (1, 0, 0));
        match booked.bookings()[0].outcome() {
            Outcome::Taken {
                shares,
                trimmed,
                net,
            } => {
                assert_eq!(shares.units(), 90 * SHARE_SCALE);
                assert_eq!(*trimmed, None);
                assert_eq!(net.units(), 45 * DOLLAR);
            }
            Outcome::Refused { cause } => panic!("{cause}"),
        }
        assert_eq!(booked.ledger().session_opening().units(), 20_045 * DOLLAR);
        assert!((booked.net_return() - 45.0 / 20_000.0).abs() < 1e-15);
        let readings: Vec<_> = booked
            .returns()
            .unwrap()
            .readings()
            .iter()
            .map(|(session, reading)| (*session, *reading))
            .collect();
        assert_eq!(readings, [(session(2), Some(45.0 / 20_000.0))]);
    }

    #[test]
    fn test_overlapping_same_side_trades_net_and_the_second_is_trimmed_by_the_cap() {
        // Two buys of 60 at $50.01 overlap: $5,000 per name holds the second to 39 shares and change.
        let booked = book(
            &terms(free()),
            &calendar(),
            &borrows(&[]),
            &sessions(&[2]),
            vec![
                trade("AAA", Side::Buy, 60.0, 50.0, 50.01, 2, 0),
                trade("AAA", Side::Buy, 60.0, 50.0, 50.01, 2, 1),
            ],
        )
        .unwrap();
        let (taken, trimmed, refused) = booked.counts();
        assert_eq!((taken, trimmed, refused), (2, 1, 0));
        match booked.bookings()[1].outcome() {
            Outcome::Taken {
                shares, trimmed, ..
            } => {
                // $5,000 at $50.01 is 99.98 shares; 60 are held, leaving 39.98.
                assert_eq!(shares.units(), 39_980_003);
                assert_eq!(
                    *trimmed,
                    Some(Trimmed::Room(Binding::Bound(Bound::PerName)))
                );
            }
            Outcome::Refused { cause } => panic!("{cause}"),
        }
    }

    #[test]
    fn test_an_entry_against_the_nets_side_is_refused_and_its_exit_skipped() {
        let booked = book(
            &terms(free()),
            &calendar(),
            &borrows(&["AAA"]),
            &sessions(&[2]),
            vec![
                trade("AAA", Side::Sell, 10.0, 50.0, 49.99, 2, 0),
                trade("AAA", Side::Buy, 5.0, 50.0, 50.01, 2, 1),
            ],
        )
        .unwrap();
        let (taken, _, refused) = booked.counts();
        assert_eq!((taken, refused), (1, 1));
        assert_eq!(
            booked.bookings()[1].outcome(),
            &Outcome::Refused {
                cause: RefusalCause::OpposesHolding {
                    held: Position::from_units(-10 * i128::from(SHARE_SCALE)),
                },
            }
        );
        // The refused trade never exits, and the session still closes flat.
        assert!(booked.ledger().book().positions().is_empty());
    }

    #[test]
    fn test_a_short_floors_to_whole_shares_and_below_one_is_refused() {
        let booked = book(
            &terms(free()),
            &calendar(),
            &borrows(&["AAA", "BBB"]),
            &sessions(&[2]),
            vec![
                trade("AAA", Side::Sell, 10.5, 50.0, 49.99, 2, 0),
                trade("BBB", Side::Sell, 0.5, 50.0, 49.99, 2, 1),
            ],
        )
        .unwrap();
        match booked.bookings()[0].outcome() {
            Outcome::Taken {
                shares, trimmed, ..
            } => {
                assert_eq!(shares.units(), 10 * SHARE_SCALE);
                assert_eq!(*trimmed, Some(Trimmed::WholeShare));
            }
            Outcome::Refused { cause } => panic!("{cause}"),
        }
        assert_eq!(
            booked.bookings()[1].outcome(),
            &Outcome::Refused {
                cause: RefusalCause::ShortBelowOneShare {
                    sized: Shares::from_units(500_000),
                },
            }
        );
    }

    #[test]
    fn test_a_short_without_borrow_is_refused_and_an_unlisted_name_reads_unavailable() {
        let listed = borrows(&["AAA"]);
        assert_eq!(listed.availability(&symbol("AAA")), Borrow::Available);
        assert_eq!(listed.availability(&symbol("ZZZ")), Borrow::Unavailable);
        let booked = book(
            &terms(free()),
            &calendar(),
            &listed,
            &sessions(&[2]),
            vec![trade("ZZZ", Side::Sell, 10.0, 50.0, 49.99, 2, 0)],
        )
        .unwrap();
        assert_eq!(
            booked.bookings()[0].outcome(),
            &Outcome::Refused {
                cause: RefusalCause::NoRoom {
                    binding: Binding::Unborrowable,
                },
            }
        );
    }

    #[test]
    fn test_fees_are_charged_at_the_entrys_own_liquidity() {
        // A commission of 1 cent a share, an access fee of 0.3 and a rebate of 0.2, and a $10-per-million sale levy.
        let schedule = PassThroughSchedule::new(
            PerShare::from_millionths(10_000).unwrap(),
            PerShare::from_millionths(3_000).unwrap(),
            PerShare::from_millionths(2_000).unwrap(),
        )
        .unwrap();
        let regulatory =
            RegulatoryFees::new(Dollars::from_millionths(10_000_000), PerShare::ZERO).unwrap();
        let fees = FeeRegime::PassThrough {
            regulatory,
            schedule,
        };
        // Buy 90 at $50.01 crossing (13,000 per share), sell at $50.51 crossing (13,000 plus the 506 levy).
        let booked = book(
            &terms(fees),
            &calendar(),
            &borrows(&[]),
            &sessions(&[2]),
            vec![trade("AAA", Side::Buy, 90.0, 50.0, 50.51, 2, 0)],
        )
        .unwrap();
        match booked.bookings()[0].outcome() {
            Outcome::Taken { net, .. } => {
                // $45 of swing less 90 shares at 13,000 in and 13,506 out.
                assert_eq!(net.units(), 45 * DOLLAR - 2_385_540_000_000);
            }
            Outcome::Refused { cause } => panic!("{cause}"),
        }
    }

    #[test]
    fn test_an_eventless_session_closes_with_a_zero_return() {
        let booked = book(
            &terms(free()),
            &calendar(),
            &borrows(&[]),
            &sessions(&[2, 3, 4]),
            vec![trade("AAA", Side::Buy, 90.0, 50.0, 50.51, 3, 0)],
        )
        .unwrap();
        let readings: Vec<_> = booked
            .returns()
            .unwrap()
            .readings()
            .iter()
            .map(|(session, reading)| (*session, *reading))
            .collect();
        assert_eq!(
            readings,
            [
                (session(2), Some(0.0)),
                (session(3), Some(45.0 / 20_000.0)),
                (session(4), Some(0.0)),
            ]
        );
    }

    #[test]
    fn test_a_trade_outside_the_sessions_refuses_the_booking() {
        assert_eq!(
            book(
                &terms(free()),
                &calendar(),
                &borrows(&[]),
                &sessions(&[2]),
                vec![trade("AAA", Side::Buy, 1.0, 50.0, 50.01, 3, 0)],
            ),
            Err(BookingError::OutsideSessions {
                session: session(3)
            })
        );
        assert_eq!(
            book(
                &terms(free()),
                &calendar(),
                &borrows(&[]),
                &BTreeSet::new(),
                vec![],
            ),
            Err(BookingError::NoSessions)
        );
    }

    #[test]
    fn test_a_trades_shape_is_validated_at_construction() {
        let built = trade("AAA", Side::Buy, 1.0, 50.0, 50.01, 2, 0);
        assert_eq!(
            Trade::new(
                symbol("AAA"),
                Side::Buy,
                Shares::from_units(0),
                built.entered_at(),
                built.entered(),
                built.exited_at(),
                built.exit(),
            ),
            Err(TradeShape::NothingTraded)
        );
        assert_eq!(
            Trade::new(
                symbol("AAA"),
                Side::Buy,
                built.shares(),
                built.entered_at(),
                built.entered(),
                built.entered_at(),
                built.exit(),
            ),
            Err(TradeShape::ExitNotAfterEntry {
                entered_at: built.entered_at(),
                exited_at: built.entered_at(),
            })
        );
        assert_eq!(
            Trade::new(
                symbol("AAA"),
                Side::Buy,
                built.shares(),
                built.entered_at(),
                built.entered(),
                at(3, 0),
                built.exit(),
            ),
            Err(TradeShape::AcrossSessions {
                entered: session(2),
                exited: session(3),
            })
        );
    }

    #[test]
    fn test_the_borrow_provenance_flags_a_snapshot_as_not_point_in_time() {
        assert_eq!(
            BorrowProvenance::Snapshot {
                fetched: session(2)
            }
            .flag(),
            "snapshot(fetched=2026-03-02, not point in time)"
        );
        assert_eq!(BorrowProvenance::PointInTime.flag(), "point_in_time");
    }

    #[test]
    fn test_gains_are_each_taken_trades_net_over_the_opening() {
        let booked = book(
            &terms(free()),
            &calendar(),
            &borrows(&["AAA"]),
            &sessions(&[2]),
            vec![
                trade("AAA", Side::Sell, 10.0, 50.0, 49.49, 2, 0),
                trade("BBB", Side::Sell, 0.5, 50.0, 49.99, 2, 1),
            ],
        )
        .unwrap();
        // The short sells at $49.99 and covers at $49.49: 10 x $0.50 = $5 on $20,000; the refused half-share
        // short contributes no gain.
        let gains = booked.gains();
        assert_eq!(gains.len(), 1);
        let (gained_at, name, gain) = &gains[0];
        assert_eq!((*gained_at, name.as_str()), (at(2, 0), "AAA"));
        assert!((gain.unwrap() - 5.0 / 20_000.0).abs() < 1e-15);
    }

    /// One drivable trade: a name, a side, whole shares, entry and exit dollars and a minute offset.
    fn any_trade() -> impl Strategy<Value = (usize, bool, u64, f64, f64, i64)> {
        (
            0_usize..3,
            any::<bool>(),
            1_u64..80,
            10.0_f64..100.0,
            10.0_f64..100.0,
            0_i64..60,
        )
    }

    proptest! {
        /// The sum of every taken trade's net is exactly the final equity over the opening, whatever was trimmed
        /// or refused, so a booked arm's reading is its own trades' and nothing else's.
        #[test]
        fn test_taken_nets_sum_to_the_equity_change(
            trades in prop::collection::vec(any_trade(), 0..25),
        ) {
            let built: Vec<Trade> = trades
                .into_iter()
                .map(|(index, buying, whole, entry, exit, minutes)| {
                    let side = match buying {
                        true => Side::Buy,
                        false => Side::Sell,
                    };
                    let (entry, exit) = ((entry * 100.0).round() / 100.0, (exit * 100.0).round() / 100.0);
                    trade(
                        ["AAA", "BBB", "CCC"][index],
                        side,
                        whole as f64,
                        entry,
                        exit,
                        2,
                        minutes,
                    )
                })
                .collect();
            let booked = book(
                &terms(free()),
                &calendar(),
                &borrows(&["AAA", "BBB", "CCC"]),
                &sessions(&[2]),
                built,
            )
            .unwrap();
            let total: i128 = booked
                .bookings()
                .iter()
                .filter_map(|booking| match booking.outcome() {
                    Outcome::Taken { net, .. } => Some(net.units()),
                    Outcome::Refused { .. } => None,
                })
                .sum();
            prop_assert_eq!(
                total,
                booked.ledger().session_opening().units() - 20_000 * DOLLAR
            );
            prop_assert!(booked.ledger().book().positions().is_empty());
        }
    }

    /// Terms with every dollar limit wide, so only `leverage` binds.
    fn wide_terms(leverage: u8, fees: FeeRegime) -> Terms {
        let wide = Cash::from_units(1_000_000 * DOLLAR);
        let limits = Limits::new(wide, wide, wide, wide, TimeDelta::minutes(10)).unwrap();
        Terms::new(
            Cash::from_units(20_000 * DOLLAR),
            limits,
            Leverage::new(NonZeroU8::new(leverage).unwrap()),
            fees,
        )
    }

    /// The room is read before fees, so an entry sized to it can breach buying power by its own fee; the entry is
    /// resized to the largest size whose exposure fits equity net of that fee, exactly.
    #[test]
    fn test_an_entry_at_the_buying_power_edge_is_resized_for_its_fee() {
        // A one-cent commission a share and nothing else.
        let schedule = PassThroughSchedule::new(
            PerShare::from_millionths(10_000).unwrap(),
            PerShare::ZERO,
            PerShare::ZERO,
        )
        .unwrap();
        let fees = FeeRegime::PassThrough {
            regulatory: RegulatoryFees::new(Dollars::from_millionths(0), PerShare::ZERO).unwrap(),
            schedule,
        };
        let booked = book(
            &wide_terms(1, fees),
            &calendar(),
            &borrows(&[]),
            &sessions(&[2]),
            vec![trade("AAA", Side::Buy, 400.0, 50.0, 50.01, 2, 0)],
        )
        .unwrap();
        match booked.bookings()[0].outcome() {
            Outcome::Taken {
                shares, trimmed, ..
            } => {
                // floor(20,000e12 / (50,010,000 ticks + 10,000 fee)) millionths of a share.
                assert_eq!(shares.units(), 399_840_063);
                assert_eq!(
                    *trimmed,
                    Some(Trimmed::Room(Binding::Bound(Bound::BuyingPower)))
                );
            }
            Outcome::Refused { cause } => panic!("{cause}"),
        }
    }

    /// A refused entry's and a skipped exit's prices are still observations of the tape, so they update the marks
    /// later risk checks value other holdings at.
    #[test]
    fn test_a_refused_trades_prices_still_mark_the_name() {
        // The long holds from minute 0 to minute 50, so both later entries meet it on the book.
        let held = trade("AAA", Side::Buy, 90.0, 50.0, 50.01, 2, 0);
        let held = Trade::new(
            held.symbol().clone(),
            held.side(),
            held.shares(),
            held.entered_at(),
            held.entered(),
            at(2, 50),
            held.exit(),
        )
        .unwrap();
        let booked = book(
            &wide_terms(2, free()),
            &calendar(),
            &borrows(&["AAA"]),
            &sessions(&[2]),
            vec![
                held,
                // Opposes the long, so it is refused; its $39.99 entry and $39.00 exit still mark AAA.
                trade("AAA", Side::Sell, 10.0, 40.0, 39.0, 2, 2),
                trade("BBB", Side::Buy, 800.0, 50.0, 50.01, 2, 20),
            ],
        )
        .unwrap();
        let (taken, trimmed, refused) = booked.counts();
        assert_eq!((taken, trimmed, refused), (2, 1, 1));
        match booked.bookings()[2].outcome() {
            Outcome::Taken {
                shares, trimmed, ..
            } => {
                // With AAA marked at the observed $39, equity is $19,009.10: twice that less the $3,510 gross
                // leaves $34,508.20 of buying power at the $50.01 ask.
                assert_eq!(shares.units(), 690_025_994);
                assert_eq!(
                    *trimmed,
                    Some(Trimmed::Room(Binding::Bound(Bound::BuyingPower)))
                );
            }
            Outcome::Refused { cause } => panic!("{cause}"),
        }
    }

    /// An exit after the session's close would report a holding past the close as a flat session, so it refuses
    /// the booking; a trade on a day the calendar does not trade is refused the same way.
    #[test]
    fn test_an_exit_past_the_close_refuses_the_booking() {
        let built = trade("AAA", Side::Buy, 1.0, 50.0, 50.01, 2, 0);
        let late = Trade::new(
            built.symbol().clone(),
            built.side(),
            built.shares(),
            built.entered_at(),
            built.entered(),
            // 370 minutes after 10:00 Eastern is 16:10, past the 16:00 close.
            at(2, 370),
            built.exit(),
        )
        .unwrap();
        assert_eq!(
            book(
                &terms(free()),
                &calendar(),
                &borrows(&[]),
                &sessions(&[2]),
                vec![late],
            ),
            Err(BookingError::ExitPastClose {
                symbol: symbol("AAA"),
                exited_at: at(2, 370),
                close: at(2, 360),
            })
        );
        assert_eq!(
            book(
                &terms(free()),
                &calendar(),
                &borrows(&[]),
                &sessions(&[7]),
                vec![trade("AAA", Side::Buy, 1.0, 50.0, 50.01, 7, 0)],
            ),
            Err(BookingError::NotTradingDay {
                session: session(7)
            })
        );
    }

    /// A rested fill is charged adding liquidity, a crossed one removing, and a rebate past the fee books as free
    /// rather than as a credit.
    #[test]
    fn test_a_rebate_past_the_fee_books_as_free() {
        assert_eq!(liquidity(How::RestFilled), Liquidity::Adding);
        assert_eq!(liquidity(How::Crossed), Liquidity::Removing);
        assert_eq!(liquidity(How::RestThenCrossed), Liquidity::Removing);
        let credit = PerShare::from_millionths(-500).unwrap();
        assert_eq!(
            fee_units(credit, Shares::whole(100).unwrap()),
            DollarVolume::from_units(0)
        );
        let charge = PerShare::from_millionths(500).unwrap();
        assert_eq!(
            fee_units(charge, Shares::whole(100).unwrap()),
            DollarVolume::from_units(500 * 100 * u128::from(SHARE_SCALE))
        );
    }
}
