//! One trading session: each minute's trades read once the minute has ended and folded into minute bars, the market
//! state those bars build, and at each decision bar the strategy's target taken through risk, the guard and the broker,
//! then reconciled, all journaled.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use crate::broker::Broker;
use crate::common::book::{Book, Cash, Fill};
use crate::common::journal::Observation;
use crate::common::market::record::BarInterval;
use crate::common::market::state::{MarketEvent, MarketState, Staleness};
use crate::common::market::trade_bars::{BarBuilt, TradeBar, TradeConditions, TradeFold};
use crate::common::market::{Price, StampedPrice, Symbol};
use crate::common::monoid::{Monoid, Tally, concatenate};
use crate::common::order::{BrokerFailure, OrderSequence};
use crate::common::playbook::Played;
use crate::common::problem::formulate;
use crate::common::reconcile::rounding_allowance;
use crate::common::risk::{Limits, TargetDecided};
use crate::common::solver::Solver;
use crate::common::standing::{HaltCause, SessionHalted};
use crate::common::strategy::Strategy;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;
use crate::execution::{
    JournalFailure, OrderOutcome, Patience, ReconcileFailure, Reconciliation, execute,
    reconcile_and_close,
};
use crate::ingest::FetchError;
use crate::ingest::alpaca::{Alpaca, AlpacaTradeOutcome};
use crate::journal::Journal;

/// How long after a minute's first read falls due it is read once more, to measure what that read missed.
const RECHECK_AFTER: TimeDelta = TimeDelta::minutes(2);

/// How long after the session's first minute ends it waits before being built, since REST serves the opening auction's
/// burst late; no decision runs before it ends, and the first falls on the latest boundary built then.
const OPENING_WAIT: TimeDelta = RECHECK_AFTER;

const MINUTE: TimeDelta = TimeDelta::minutes(1);

pub mod parameters;

/// How often a session decides: the bars it decides at the end of, always within the day.
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
pub enum DecisionInterval {
    OneMinute,
    FiveMinute,
}

impl DecisionInterval {
    fn length(self) -> TimeDelta {
        match self {
            Self::OneMinute => TimeDelta::minutes(1),
            Self::FiveMinute => TimeDelta::minutes(5),
        }
    }

    fn bar_interval(self) -> BarInterval {
        match self {
            Self::OneMinute => BarInterval::OneMinute,
            Self::FiveMinute => BarInterval::FiveMinute,
        }
    }
}

/// How long after a minute ends a session waits before reading its trades: whole seconds, from one to sixty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinuteWait(TimeDelta);

/// Why a minute wait was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MinuteWaitRefusal {
    #[error("a wait of {wait} is not between one and sixty seconds")]
    OutOfRange { wait: TimeDelta },
    #[error("a wait of {wait} is not whole seconds")]
    Fractional { wait: TimeDelta },
}

impl MinuteWait {
    pub fn new(wait: TimeDelta) -> Result<Self, MinuteWaitRefusal> {
        match (
            wait < TimeDelta::seconds(1) || wait > TimeDelta::seconds(60),
            wait.subsec_nanos() != 0,
        ) {
            (true, _) => Err(MinuteWaitRefusal::OutOfRange { wait }),
            (false, true) => Err(MinuteWaitRefusal::Fractional { wait }),
            (false, false) => Ok(Self(wait)),
        }
    }

    pub fn get(self) -> TimeDelta {
        self.0
    }
}

/// How a session trades: how often it decides, its limits, how long an order may stay open, how old a price may be
/// before risk treats it as unpriced, and how long after a minute ends its trades are read.
#[derive(Debug, Clone, Copy)]
pub struct SessionSettings {
    decision: DecisionInterval,
    limits: Limits,
    patience: Patience,
    stale_after: Staleness,
    minute_wait: MinuteWait,
}

impl SessionSettings {
    pub fn new(
        decision: DecisionInterval,
        limits: Limits,
        patience: Patience,
        stale_after: Staleness,
        minute_wait: MinuteWait,
    ) -> Self {
        Self {
            decision,
            limits,
            patience,
            stale_after,
            minute_wait,
        }
    }
}

/// Where a session reads each ended minute's trades.
pub trait MinuteTape {
    /// Every trade of `symbols` stamped in `[start, end)`.
    fn trades_between(
        &self,
        symbols: &[Symbol],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Future<Output = Result<Vec<AlpacaTradeOutcome>, FetchError>> + Send;
}

impl MinuteTape for Alpaca {
    async fn trades_between(
        &self,
        symbols: &[Symbol],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<AlpacaTradeOutcome>, FetchError> {
        Alpaca::trades_between(self, symbols, start, end).await
    }
}

/// The earliest print among `trades`, ignoring refused rows.
fn first_print(trades: &[AlpacaTradeOutcome]) -> Option<DateTime<Utc>> {
    trades
        .iter()
        .filter_map(|outcome| match outcome {
            AlpacaTradeOutcome::Print { print, .. } => Some(print.timestamp()),
            AlpacaTradeOutcome::Refused(_) => None,
        })
        .min()
}

/// Why a session stopped: the journal refused a write, or the broker's book could not be reconciled.
#[derive(Debug)]
pub enum SessionError {
    Journal(JournalFailure),
    Reconcile(ReconcileFailure),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Journal(failed) | Self::Reconcile(ReconcileFailure::Journal(failed)) => write!(
                formatter,
                "the journal refused a write after {} orders: {}",
                failed.outcomes().len(),
                failed.error()
            ),
            Self::Reconcile(ReconcileFailure::Unread(error)) => {
                write!(
                    formatter,
                    "the broker's book was not read to reconcile: {error}"
                )
            }
        }
    }
}

/// One session's trading state; `advance` builds every minute whose wait has passed and decides on the bars built.
pub struct Session<S: Strategy> {
    strategy: Played<S>,
    settings: SessionSettings,
    calendar: TradingCalendar,
    fold: TradeFold,
    symbols: Vec<Symbol>,
    /// The session's open, whose minute waits `OPENING_WAIT` rather than the settings' wait.
    open: DateTime<Utc>,
    /// The end of the last minute built, every minute before it folded in.
    built_through: DateTime<Utc>,
    /// The session's close, past which no minute is built.
    close: DateTime<Utc>,
    /// Built minutes still to be read once more, oldest first.
    rechecks: VecDeque<Recheck>,
    state: MarketState,
    book: Book,
    /// The book's worth when the session opened, from which the daily loss is measured.
    opening: Cash,
    /// Fills since the last reconciliation, which bound how far the broker's cash may stray from the book's.
    fills: Vec<Fill>,
    next_decision: DateTime<Utc>,
    sequence: OrderSequence,
    standing: Standing,
}

/// A built minute starting at `start` and the rows its first read returned.
#[derive(Debug, Clone, Copy)]
struct Recheck {
    start: DateTime<Utc>,
    rows: usize,
}

/// Whether a session still decides, or the cause of its first halt, after which it decides nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Standing {
    Trading,
    Halted(HaltCause),
}

/// A halt on a journal write refused with `error`.
fn journal_refused(error: &std::io::Error) -> HaltCause {
    HaltCause::JournalRefused {
        error: error.to_string(),
    }
}

impl<S: Strategy> Session<S> {
    /// A session for `session`'s tape of `symbols` building from `open`, from a state warmed on earlier sessions and
    /// the book the broker reported at the open, worth `opening`. A session the calendar does not publish has its close
    /// at `open`, so it builds nothing.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is one independent input a session starts from"
    )]
    pub fn new(
        strategy: Played<S>,
        settings: SessionSettings,
        calendar: TradingCalendar,
        session: SessionDate,
        conditions: TradeConditions,
        symbols: Vec<Symbol>,
        warm: MarketState,
        book: Book,
        opening: Cash,
        open: DateTime<Utc>,
    ) -> Self {
        let close = calendar
            .session(session)
            .map_or(open, |trading| trading.hours().1);
        Self {
            next_decision: decision_after(open, settings.decision),
            strategy,
            settings,
            calendar,
            fold: TradeFold::new(session, conditions),
            symbols,
            open,
            built_through: open,
            close,
            rechecks: VecDeque::new(),
            state: warm,
            book,
            opening,
            fills: Vec::new(),
            sequence: OrderSequence::default(),
            standing: Standing::Trading,
        }
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn standing(&self) -> &Standing {
        &self.standing
    }

    /// Whether every minute before the close is built.
    pub fn built_through_close(&self) -> bool {
        self.built_through >= self.close
    }

    /// Builds every minute whose wait has passed by the clock, and once a decision bar is built, trades the book toward
    /// the strategy's target within the limits and reconciles it with the broker's; then reads again each built minute
    /// due a recheck. A late call decides once, for the latest bar built, rather than for each it passed.
    pub async fn advance(
        &mut self,
        clock: &impl Fn() -> DateTime<Utc>,
        tape: &impl MinuteTape,
        broker: &impl Broker,
        journal: &mut Journal,
    ) -> Result<(), SessionError> {
        let started = clock();
        self.fold_in(MarketEvent::Clock(started));
        self.build(started, tape, journal).await?;
        // Building awaits REST, so the decision reads the clock again rather than trusting the instant it began at.
        let now = clock();
        self.fold_in(MarketEvent::Clock(now));
        let decided = self.decide(now, broker, journal).await;
        self.recheck(clock(), tape).await;
        decided
    }

    /// How long the minute starting at `start` waits after it ends before it is first read. The closing minute waits no
    /// longer than any other, since no decision reads it, so its bar may lack closing auction prints REST serves later.
    fn wait(&self, start: DateTime<Utc>) -> TimeDelta {
        match start == self.open {
            true => OPENING_WAIT,
            false => self.settings.minute_wait.get(),
        }
    }

    /// Reads and folds in each minute in turn whose wait has passed by `now`, journaling its bars as built; a minute
    /// that cannot be read, or that no name printed in with no print served after it, stops the building until the next
    /// call retries it.
    async fn build(
        &mut self,
        now: DateTime<Utc>,
        tape: &impl MinuteTape,
        journal: &mut Journal,
    ) -> Result<(), SessionError> {
        // The first print a probe found, which shows every silent minute ending by it was silent rather than unserved.
        let mut served_from: Option<DateTime<Utc>> = None;
        while self.built_through < self.close
            && self.built_through + MINUTE + self.wait(self.built_through) <= now
        {
            let (start, end) = (self.built_through, self.built_through + MINUTE);
            let trades = match tape.trades_between(&self.symbols, start, end).await {
                Ok(trades) => trades,
                Err(cause) => {
                    tracing::warn!(minute = %start, %cause, "Minute not read");
                    return Ok(());
                }
            };
            // A minute no name printed in is held unless the tape serves a print after it, as a halt would leave it.
            if first_print(&trades).is_none() && served_from.is_none_or(|first| first < end) {
                match tape.trades_between(&self.symbols, end, now).await {
                    Ok(after) => match first_print(&after) {
                        Some(first) => served_from = Some(first),
                        None => {
                            tracing::warn!(minute = %start, "Minute read empty");
                            return Ok(());
                        }
                    },
                    Err(cause) => {
                        tracing::warn!(minute = %start, %cause, "Minute read empty, nothing after it read");
                        return Ok(());
                    }
                }
            }
            let mut refused = Tally::default();
            for outcome in &trades {
                match outcome {
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        correction,
                        ..
                    } => self.fold.push_lettered(print, *tape, letters, *correction),
                    AlpacaTradeOutcome::Refused(row) => refused.add(row.cause().kind()),
                }
            }
            if refused.total() > 0 {
                tracing::warn!(minute = %start, %refused, "Minute rows refused");
            }
            for bar in self.fold.drain_through(end) {
                if let Err(error) = journal.append(now, Observation::BarBuilt(BarBuilt::of(&bar))) {
                    self.halt(journal, journal_refused(&error));
                    return Err(SessionError::Journal(JournalFailure::before_any_order(
                        error,
                    )));
                }
                self.fold_in(MarketEvent::Trades(bar));
            }
            self.built_through = end;
            self.rechecks.push_back(Recheck {
                start,
                rows: trades.len(),
            });
        }
        Ok(())
    }

    /// Reads once more the oldest built minute whose first read fell due at least `RECHECK_AFTER` before `now`, logging
    /// both row counts, so the logs show whether the wait let a minute be built before all its trades were served. One
    /// per call, so a run started late does not stall building behind a recheck of every minute it caught up on.
    async fn recheck(&mut self, now: DateTime<Utc>, tape: &impl MinuteTape) {
        if let Some(recheck) = self.rechecks.front().copied().filter(|recheck| {
            recheck.start + MINUTE + self.wait(recheck.start) + RECHECK_AFTER <= now
        }) {
            self.rechecks.pop_front();
            let (start, first) = (recheck.start, recheck.rows);
            match tape
                .trades_between(&self.symbols, start, start + MINUTE)
                .await
            {
                Ok(trades) if trades.len() > first => {
                    tracing::warn!(minute = %start, first, later = trades.len(), "Minute rechecked");
                }
                Ok(trades) => {
                    tracing::info!(minute = %start, first, later = trades.len(), "Minute rechecked");
                }
                Err(cause) => tracing::warn!(minute = %start, %cause, "Minute not rechecked"),
            }
        }
    }

    /// Once a decision bar before the close is built, trades the book toward the strategy's target and reconciles it.
    async fn decide(
        &mut self,
        now: DateTime<Utc>,
        broker: &impl Broker,
        journal: &mut Journal,
    ) -> Result<(), SessionError> {
        match self.standing {
            Standing::Trading => {}
            Standing::Halted(_) => return Ok(()),
        }
        if self.built_through < self.next_decision {
            return Ok(());
        }
        let bar = decision_before(self.built_through, self.settings.decision);
        self.next_decision = decision_after(self.built_through, self.settings.decision);
        // Orders placed past the close would rest until the next session, so a bar built late decides nothing.
        match (bar >= self.close, now >= self.close) {
            (true, _) => return Ok(()),
            (false, true) => {
                tracing::warn!(%bar, "Decision skipped, its bar was built after the close");
                return Ok(());
            }
            (false, false) => {}
        }
        let wanted = self.strategy.decide(&self.state, &self.book);
        let phase = self.calendar.phase_at(now);
        let solver = Solver::Clip;
        let restrained = formulate(
            &self.settings.limits,
            phase,
            self.opening,
            &self.book,
            |symbol| self.fresh_price(symbol, now),
            wanted.clone(),
        )
        .map(|problem| solver.solve(&problem));
        let stretch = self.strategy.stretch_at(&self.state);
        let decided = TargetDecided::new(bar, stretch, wanted, solver, restrained.clone());
        if let Err(error) = journal.append(now, Observation::TargetDecided(decided)) {
            self.halt(journal, journal_refused(&error));
            return Err(SessionError::Journal(JournalFailure::before_any_order(
                error,
            )));
        }
        let restrained = match restrained {
            Ok(restrained) => restrained,
            // An unpriced exposure cannot be capped, so the book is left as it stands; the refusal is journaled above.
            Err(_) => return Ok(()),
        };
        let prices: BTreeMap<Symbol, Price> = self
            .book
            .positions()
            .keys()
            .chain(restrained.target().holdings().keys())
            .filter_map(|symbol| Some((symbol.clone(), self.fresh_price(symbol, now)?)))
            .collect();
        let executed = execute(
            broker,
            journal,
            &mut self.sequence,
            &self.book,
            restrained.target(),
            &prices,
            self.settings.patience,
        )
        .await;
        let outcomes = match executed {
            Ok(outcomes) => outcomes,
            Err(failed) => {
                // The orders already followed are real, so their fills reach the book before the session halts.
                self.take(failed.outcomes(), journal);
                self.halt(journal, journal_refused(failed.error()));
                return Err(SessionError::Journal(failed));
            }
        };
        self.take(&outcomes, journal);
        let reconciliation = match reconcile_and_close(
            broker,
            journal,
            &mut self.sequence,
            &self.book,
            rounding_allowance(&self.fills),
            &prices,
            self.settings.patience,
        )
        .await
        {
            Ok(reconciliation) => reconciliation,
            Err(failed) => {
                let cause = match &failed {
                    ReconcileFailure::Unread(error) => {
                        HaltCause::ReconcileUnread(BrokerFailure::from(error))
                    }
                    ReconcileFailure::Journal(failed) => journal_refused(failed.error()),
                };
                self.halt(journal, cause);
                return Err(SessionError::Reconcile(failed));
            }
        };
        self.fills.clear();
        match reconciliation {
            Reconciliation::Agreed { book } => self.book = book,
            Reconciliation::Diverged { book, .. } => {
                self.book = book;
                self.halt(journal, HaltCause::Diverged);
            }
        }
        Ok(())
    }

    /// Folds each closed order's fill into the book, halting on an order left unresolved, which may still be open at
    /// the broker and would overlap the next decision's orders.
    fn take(&mut self, outcomes: &[OrderOutcome], journal: &mut Journal) {
        for outcome in outcomes {
            match outcome {
                OrderOutcome::Closed(Some(fill)) => {
                    self.book = std::mem::take(&mut self.book).combine(Book::of(fill));
                    self.fills.push(fill.clone());
                }
                OrderOutcome::Unresolved {
                    client_order_id, ..
                } => self.halt(
                    journal,
                    HaltCause::Unresolved {
                        client_order_id: *client_order_id,
                    },
                ),
                OrderOutcome::Closed(None) | OrderOutcome::Guarded(_) | OrderOutcome::Refused => {}
            }
        }
    }

    /// Halts the session for `cause` unless it has halted already, journaling the halt best effort at the wall clock,
    /// since the journal may be what failed; a refused halt record is logged instead.
    fn halt(&mut self, journal: &mut Journal, cause: HaltCause) {
        match self.standing {
            Standing::Trading => {}
            Standing::Halted(_) => return,
        }
        let halted = Observation::SessionHalted(SessionHalted::new(cause.clone()));
        if let Err(error) = journal.append(Utc::now(), halted) {
            tracing::error!(%error, ?cause, "Halt was not journaled");
        }
        self.standing = Standing::Halted(cause);
    }

    fn fold_in(&mut self, event: MarketEvent) {
        let state = std::mem::take(&mut self.state);
        self.state = state.combine(MarketState::of(event));
    }

    /// The symbol's last one-minute close, unless the print that set it is older than the settings allow.
    fn fresh_price(&self, symbol: &Symbol, now: DateTime<Utc>) -> Option<Price> {
        self.state.fresh_price(
            symbol,
            BarInterval::OneMinute,
            now,
            self.settings.stale_after,
        )
    }
}

/// The state a session starts from: the bars of `symbols` folded in, so each series holds its latest.
pub fn warm(bars: impl IntoIterator<Item = TradeBar>, symbols: &BTreeSet<Symbol>) -> MarketState {
    concatenate(
        bars.into_iter()
            .filter(|bar| symbols.contains(bar.symbol()))
            .map(|bar| MarketState::of(MarketEvent::Trades(bar))),
    )
}

/// Each symbol's latest close among `bars`, however many bars without one follow it.
pub fn last_closes(bars: &[TradeBar]) -> BTreeMap<Symbol, Price> {
    let mut closes = BTreeMap::new();
    for bar in bars {
        if let Some(close) = bar.sums().open_close().map(|prices| prices.close()) {
            closes
                .entry(bar.symbol().clone())
                .and_modify(|latest: &mut StampedPrice| *latest = (*latest).max(close))
                .or_insert(close);
        }
    }
    closes
        .into_iter()
        .map(|(symbol, close)| (symbol, close.price()))
        .collect()
}

/// The end of the latest decision bar ended by `instant`, which is at or before it.
fn decision_before(instant: DateTime<Utc>, interval: DecisionInterval) -> DateTime<Utc> {
    decision_after(instant, interval) - interval.length()
}

/// The end of the decision bar containing `instant`, which is after it: a five-minute bar ends on the next multiple of
/// five minutes past the hour.
fn decision_after(instant: DateTime<Utc>, interval: DecisionInterval) -> DateTime<Utc> {
    let minute = instant
        .with_second(0)
        .and_then(|minute| minute.with_nanosecond(0))
        .expect("zero seconds and nanoseconds exist in every minute");
    let start = match interval {
        DecisionInterval::OneMinute => minute,
        DecisionInterval::FiveMinute => minute - TimeDelta::minutes(i64::from(minute.minute() % 5)),
    };
    interval.bar_interval().ends(start)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::Duration;

    use chrono::{NaiveDate, NaiveTime};
    use uuid::Uuid;

    use super::*;
    use crate::broker::{BrokerError, BrokerOrder, BrokerOrderId, Cancel};
    use crate::common::guard::Tradability;
    use crate::common::journal::{ReadLine, Record, RunId, read};
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::record::Trade;
    use crate::common::market::trade_bars::{
        ConditionLetter, Correction, OpenClose, Print, Tape, TradeSums,
    };
    use crate::common::market::{DollarVolume, Shares};
    use crate::common::order::{
        ClientOrderId, OrderEnding, OrderExecution, OrderReport, OrderRequest, OrderStatus,
    };
    use crate::common::strategy::Target;
    use crate::common::time::SessionRange;
    use crate::common::time::calendar::TradingSession;
    use crate::ingest::alpaca::trade_page;
    use crate::ingest::{PrintOrigin, RefusedRow, RowRefusal};

    const DOLLAR: i128 = 1_000_000_000_000;

    fn at(text: &str) -> DateTime<Utc> {
        format!("2026-10-07T{text}Z").parse().unwrap()
    }

    fn spy() -> Symbol {
        Symbol::new("SPY").unwrap()
    }

    /// A strategy that always wants one SPY share.
    struct OneShare;

    impl Strategy for OneShare {
        fn decide(&self, _: &MarketState, _: &Book) -> Target {
            Target::new(BTreeMap::from([(spy(), Shares::whole(1).unwrap())]))
        }
    }

    /// A broker that fills every order at once at `ticks`, keeping its own book, which starts wherever a test puts it.
    struct Filling {
        ticks: i64,
        /// Whether orders fill; when they do not, each stays open and its cancel never takes.
        fills: bool,
        /// Whether reading the book fails, as when the broker answers with a malformed cash field.
        book_unreadable: bool,
        book: Mutex<Book>,
        last: Mutex<Option<BrokerOrder>>,
    }

    impl Filling {
        fn new(ticks: i64, book: Book) -> Self {
            Self {
                ticks,
                fills: true,
                book_unreadable: false,
                book: Mutex::new(book),
                last: Mutex::new(None),
            }
        }
    }

    impl Broker for Filling {
        async fn submit(&self, request: &OrderRequest) -> Result<BrokerOrder, BrokerError> {
            let order = request.order();
            let price = Price::from_ticks(self.ticks).unwrap();
            if !self.fills {
                let open = BrokerOrder::new(
                    BrokerOrderId::new("broker-1".to_string()),
                    OrderReport::new(OrderStatus::Open, None, at("14:05:02")),
                );
                *self.last.lock().unwrap() = Some(open.clone());
                return Ok(open);
            }
            let fill = Fill::new(
                at("14:05:02"),
                order.symbol().clone(),
                order.side(),
                order.shares(),
                price,
                DollarVolume::default(),
            )
            .unwrap();
            let mut book = self.book.lock().unwrap();
            *book = std::mem::take(&mut *book).combine(Book::of(&fill));
            let filled = BrokerOrder::new(
                BrokerOrderId::new("broker-1".to_string()),
                OrderReport::new(
                    OrderStatus::Closed(OrderEnding::Filled),
                    OrderExecution::new(order.shares(), price),
                    at("14:05:02"),
                ),
            );
            *self.last.lock().unwrap() = Some(filled.clone());
            Ok(filled)
        }

        async fn order(&self, _: ClientOrderId) -> Result<BrokerOrder, BrokerError> {
            Ok(self
                .last
                .lock()
                .unwrap()
                .clone()
                .expect("an order was submitted"))
        }

        async fn cancel(&self, _: &BrokerOrderId) -> Result<Cancel, BrokerError> {
            Ok(Cancel::Requested)
        }

        async fn tradability(
            &self,
            symbols: &[Symbol],
        ) -> Result<BTreeMap<Symbol, Tradability>, BrokerError> {
            Ok(symbols
                .iter()
                .map(|symbol| (symbol.clone(), Tradability::Fractionable))
                .collect())
        }

        async fn book(&self) -> Result<Book, BrokerError> {
            if self.book_unreadable {
                return Err(BrokerError::Malformed {
                    field: "cash",
                    raw: "unreadable".to_string(),
                });
            }
            Ok(self.book.lock().unwrap().clone())
        }
    }

    fn session(stale_after: TimeDelta, funded: Book) -> Session<OneShare> {
        session_capped(stale_after, funded, 5_000 * DOLLAR)
    }

    /// `session` with each name capped at `per_name` cash units.
    fn session_capped(stale_after: TimeDelta, funded: Book, per_name: i128) -> Session<OneShare> {
        session_from(
            stale_after,
            funded,
            per_name,
            at("14:00:00"),
            DecisionInterval::FiveMinute,
        )
    }

    /// A session of the 2026-10-07 calendar, which closes at 20:00 UTC, building from `open` with a two-second wait.
    fn session_from(
        stale_after: TimeDelta,
        funded: Book,
        per_name: i128,
        open: DateTime<Utc>,
        decision: DecisionInterval,
    ) -> Session<OneShare> {
        let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 7).unwrap());
        let calendar = TradingCalendar::new(
            vec![
                TradingSession::new(
                    date,
                    NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                    NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
                )
                .unwrap(),
            ],
            SessionRange::single(date),
        )
        .unwrap();
        let dollars = |count: i128| Cash::from_units(count * DOLLAR);
        let limits = Limits::new(
            dollars(10_000),
            Cash::from_units(per_name),
            dollars(1_000),
            TimeDelta::minutes(15),
        )
        .unwrap();
        let patience = Patience::new(Duration::from_secs(1), Duration::from_secs(30)).unwrap();
        let settings = SessionSettings::new(
            decision,
            limits,
            patience,
            Staleness::new(stale_after).unwrap(),
            MinuteWait::new(TimeDelta::seconds(2)).unwrap(),
        );
        Session::new(
            Played::throughout(OneShare),
            settings,
            calendar,
            date,
            TradeConditions::new(BTreeMap::new()),
            vec![spy()],
            MarketState::default(),
            funded,
            dollars(10_000),
            open,
        )
    }

    /// A tape of filed prints that answers each window asked with the prints stamped in it and every refused row filed,
    /// and fails the minutes starting at an instant in `failing`, recording every window asked.
    #[derive(Default)]
    struct Scripted {
        prints: Mutex<Vec<AlpacaTradeOutcome>>,
        failing: Mutex<BTreeSet<DateTime<Utc>>>,
        asked: Mutex<Vec<(DateTime<Utc>, DateTime<Utc>)>>,
    }

    impl Scripted {
        fn file(&self, outcome: AlpacaTradeOutcome) {
            self.prints.lock().unwrap().push(outcome);
        }

        /// How many times the window from `start` to `end` was asked for.
        fn times_asked(&self, start: &str, end: &str) -> usize {
            let window = (at(start), at(end));
            self.asked
                .lock()
                .unwrap()
                .iter()
                .filter(|asked| **asked == window)
                .count()
        }
    }

    impl MinuteTape for Scripted {
        async fn trades_between(
            &self,
            _: &[Symbol],
            start: DateTime<Utc>,
            end: DateTime<Utc>,
        ) -> Result<Vec<AlpacaTradeOutcome>, FetchError> {
            self.asked.lock().unwrap().push((start, end));
            if self.failing.lock().unwrap().contains(&start) {
                return Err(FetchError::Exhausted {
                    attempts: 3,
                    last: "timed out".to_string(),
                });
            }
            Ok(self
                .prints
                .lock()
                .unwrap()
                .iter()
                .filter(|outcome| match outcome {
                    AlpacaTradeOutcome::Print { print, .. } => {
                        start <= print.timestamp() && print.timestamp() < end
                    }
                    AlpacaTradeOutcome::Refused(_) => true,
                })
                .cloned()
                .collect())
        }
    }

    /// A one-share SPY print at `when` for `ticks`, a regular sale on the consolidated tape.
    fn print(when: &str, ticks: i64) -> AlpacaTradeOutcome {
        let trade = Trade::new(
            spy(),
            at(when),
            Price::from_ticks(ticks).unwrap(),
            Shares::whole(1).unwrap(),
        )
        .unwrap();
        AlpacaTradeOutcome::Print {
            print: Print::Trade(trade),
            tape: Tape::ConsolidatedTape,
            letters: vec![ConditionLetter::of(' ')],
            correction: Correction::Stands,
            origin: PrintOrigin {
                exchange: Some("Z".to_string()),
                tape: "A".to_string(),
                id: Some("1".to_string()),
                participant_timestamp: None,
                trade_reporting_facility_timestamp: None,
                sequence: None,
            },
        }
    }

    /// Every record's event type in the order the journal wrote them, across its session files, since the session's fixed
    /// clock and execution's wall clock can file one run under two dates.
    fn journaled(directory: &std::path::Path) -> Vec<&'static str> {
        records(directory)
            .iter()
            .map(|record| record.observation().event_type())
            .collect()
    }

    /// Every record in the order the journal wrote them.
    fn records(directory: &std::path::Path) -> Vec<Record> {
        let mut records: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .flat_map(|path| read(&std::fs::read_to_string(path).unwrap()))
            .map(|line| match line {
                ReadLine::Read(record) => *record,
                ReadLine::Untyped(untyped) => panic!("untyped: {untyped:?}"),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect();
        records.sort_by_key(|record| record.sequence());
        records
    }

    /// Every record's payload in the order the journal wrote them.
    fn payloads(directory: &std::path::Path) -> Vec<serde_json::Value> {
        records(directory)
            .iter()
            .map(|record| serde_json::to_value(record.observation()).unwrap()["payload"].clone())
            .collect()
    }

    fn journal() -> (Journal, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!("fund-trader-{}", Uuid::new_v4()));
        (
            Journal::open(&directory, RunId::new(Uuid::new_v4())).unwrap(),
            directory,
        )
    }

    /// Prints fold into minute bars as each minute's wait passes; nothing is decided before the 14:05 bar is built, and
    /// then the book buys the share the strategy wants at a fresh price, the fill agreeing with the broker's book.
    #[tokio::test(start_paused = true)]
    async fn test_a_built_decision_bar_trades_to_the_target_and_reconciles() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        session
            .advance(&|| at("14:03:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        tape.file(print("14:04:30", 701_000_000));
        session
            .advance(&|| at("14:05:01"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            journaled(&directory),
            [
                "bar_built",
                "bar_built",
                "target_decided",
                "tradability_read",
                "order_submitted",
                "order_closed",
                "book_reconciled"
            ]
        );
        assert_eq!(session.book().position(&spy()).units(), 1_000_000);
        assert_eq!(
            session.book().cash(),
            Cash::from_units(10_000 * DOLLAR - 701 * DOLLAR)
        );
        assert_eq!(session.standing(), &Standing::Trading);
        session
            .advance(&|| at("14:09:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory).len(), 7);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A price older than the settings allow is no price: risk refuses, the decision is journaled with the refusal,
    /// and nothing is sent.
    #[tokio::test(start_paused = true)]
    async fn test_a_stale_price_leaves_the_book_alone() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(1), funded.clone());
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        tape.file(print("14:05:01", 700_000_000));
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built", "target_decided"]);
        assert_eq!(
            payloads(&directory)[1]["stretch"],
            serde_json::json!({"from": "09:30:00", "progress": 1_000_000})
        );
        assert_eq!(session.book(), &funded);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A broker whose book has strayed from the session's halts it after reconciling: no later decision is made.
    #[tokio::test(start_paused = true)]
    async fn test_a_divergent_reconciliation_halts_the_session() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let strayed = Book::funded(Cash::from_units(9_000 * DOLLAR));
        let broker = Filling::new(701_000_000, strayed);
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:04:30", 701_000_000));
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(session.standing(), &Standing::Halted(HaltCause::Diverged));
        let count = |directory, wanted: &str| {
            journaled(directory)
                .into_iter()
                .filter(|event_type| *event_type == wanted)
                .count()
        };
        assert_eq!(count(&directory, "target_decided"), 1);
        assert_eq!(journaled(&directory).last(), Some(&"session_halted"));
        tape.file(print("14:09:30", 701_000_000));
        session
            .advance(&|| at("14:10:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(count(&directory, "target_decided"), 1);
        assert_eq!(count(&directory, "session_halted"), 1);
        assert_eq!(
            session.book().cash(),
            Cash::from_units(9_000 * DOLLAR - 701 * DOLLAR)
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Freshness reads the closing print's own time: a close three seconds old is fresh under a one-minute limit.
    #[tokio::test(start_paused = true)]
    async fn test_a_close_is_as_old_as_its_print() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(1), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:04:59", 701_000_000));
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(session.book().position(&spy()).units(), 1_000_000);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// An order left unresolved may still be open at the broker, so the session halts rather than send another.
    #[tokio::test(start_paused = true)]
    async fn test_an_unresolved_order_halts_the_session() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let mut broker = Filling::new(701_000_000, funded.clone());
        broker.fills = false;
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:04:30", 701_000_000));
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        let Standing::Halted(HaltCause::Unresolved { client_order_id }) = session.standing() else {
            panic!("expected an unresolved halt: {:?}", session.standing());
        };
        let events = journaled(&directory);
        assert_eq!(
            events[events.len() - 3..],
            ["order_unresolved", "session_halted", "book_reconciled"]
        );
        let records = payloads(&directory);
        assert_eq!(
            records[records.len() - 3]["client_order_id"],
            serde_json::json!(client_order_id)
        );
        assert_eq!(
            records[records.len() - 2]["cause"]["unresolved"]["client_order_id"],
            serde_json::json!(client_order_id)
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A journal that refuses a built bar halts the session as well as failing the call.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_step_halts_the_session() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        std::fs::remove_dir_all(&directory).unwrap();
        tape.file(print("14:04:30", 701_000_000));
        let failed = session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await;
        assert!(matches!(failed, Err(SessionError::Journal(_))));
        assert!(matches!(
            session.standing(),
            Standing::Halted(HaltCause::JournalRefused { .. })
        ));
    }

    /// A broker book that cannot be read at reconciliation halts the session on that cause, journaled with it.
    #[tokio::test(start_paused = true)]
    async fn test_an_unread_book_at_reconciliation_halts_the_session() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let mut broker = Filling::new(701_000_000, funded.clone());
        broker.book_unreadable = true;
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:04:30", 701_000_000));
        let failed = session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await;
        assert!(matches!(
            failed,
            Err(SessionError::Reconcile(ReconcileFailure::Unread(_)))
        ));
        assert_eq!(
            session.standing(),
            &Standing::Halted(HaltCause::ReconcileUnread(BrokerFailure::Malformed {
                field: "cash".to_string(),
                raw: "unreadable".to_string(),
            }))
        );
        assert_eq!(journaled(&directory).last(), Some(&"session_halted"));
        assert_eq!(
            payloads(&directory).last(),
            Some(&serde_json::json!({
                "cause": {"reconcile_unread": {"malformed": {"field": "cash", "raw": "unreadable"}}}
            }))
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A halt is stamped when it happens, so it never precedes the records written while the decision traded.
    #[tokio::test(start_paused = true)]
    async fn test_a_halt_is_stamped_after_the_records_that_caused_it() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let strayed = Book::funded(Cash::from_units(9_000 * DOLLAR));
        let broker = Filling::new(701_000_000, strayed);
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:04:30", 701_000_000));
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        let records = records(&directory);
        let stamped = |wanted: &str| {
            records
                .iter()
                .find(|record| record.observation().event_type() == wanted)
                .map(|record| record.timestamp())
                .unwrap()
        };
        assert!(stamped("session_halted") >= stamped("book_reconciled"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Only the first halt is journaled and kept, so a later failure cannot replace the cause that stopped the session.
    #[test]
    fn test_a_session_halts_once_on_its_first_cause() {
        let mut session = session(TimeDelta::minutes(5), Book::default());
        let (mut journal, directory) = journal();
        session.halt(&mut journal, HaltCause::Diverged);
        session.halt(
            &mut journal,
            HaltCause::JournalRefused {
                error: "disk full".to_string(),
            },
        );
        assert_eq!(session.standing(), &Standing::Halted(HaltCause::Diverged));
        assert_eq!(
            payloads(&directory),
            [serde_json::json!({"cause": "diverged"})]
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A journal that refuses the decision itself, with every bar already journaled, halts the session too.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_decision_halts_the_session() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut refusing, gone) = journal();
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 701_000_000));
        tape.file(print("14:05:01", 701_000_000));
        session
            .advance(&|| at("14:03:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        std::fs::remove_dir_all(&gone).unwrap();
        let failed = session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut refusing)
            .await;
        assert!(matches!(failed, Err(SessionError::Journal(_))));
        assert!(matches!(
            session.standing(),
            Standing::Halted(HaltCause::JournalRefused { .. })
        ));
        assert_eq!(session.next_decision, at("14:10:00"));
        assert_eq!(session.book().position(&spy()).units(), 0);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn test_a_decision_bar_ends_on_its_next_boundary() {
        assert_eq!(
            decision_before(at("14:05:00"), DecisionInterval::FiveMinute),
            at("14:05:00")
        );
        assert_eq!(
            decision_before(at("14:09:59"), DecisionInterval::FiveMinute),
            at("14:05:00")
        );
        assert_eq!(
            decision_after(at("14:03:20"), DecisionInterval::FiveMinute),
            at("14:05:00")
        );
        assert_eq!(
            decision_after(at("14:05:00"), DecisionInterval::FiveMinute),
            at("14:10:00")
        );
        assert_eq!(
            decision_after(at("14:03:20"), DecisionInterval::OneMinute),
            at("14:04:00")
        );
        assert_eq!(
            decision_after(at("14:59:59"), DecisionInterval::FiveMinute),
            at("15:00:00")
        );
    }

    fn closing(raw: &str, minute: &str, ticks: i64) -> TradeBar {
        let start = at(minute);
        let close = StampedPrice::new(start, Price::from_ticks(ticks).unwrap());
        TradeBar::new(
            Symbol::new(raw).unwrap(),
            BarInterval::OneMinute,
            start,
            TradeSums::new(
                TradeTotals::default(),
                Some(OpenClose::new(close, close).unwrap()),
                None,
            ),
        )
        .unwrap()
    }

    #[test]
    fn test_warming_folds_only_the_wanted_symbols_and_sets_no_clock() {
        let state = warm(
            [
                closing("SPY", "14:01:00", 2),
                closing("SPY", "14:00:00", 1),
                closing("ABC", "14:01:00", 3),
            ],
            &BTreeSet::from([spy()]),
        );
        assert_eq!(
            state.last_close(&spy(), BarInterval::OneMinute),
            Some(StampedPrice::new(
                at("14:01:00"),
                Price::from_ticks(2).unwrap()
            ))
        );
        assert_eq!(
            state.last_close(&Symbol::new("ABC").unwrap(), BarInterval::OneMinute),
            None
        );
        assert_eq!(state.clock(), None);
    }

    #[test]
    fn test_decision_intervals_read_back_as_written() {
        use strum::IntoEnumIterator;

        let names: Vec<&str> = DecisionInterval::iter().map(Into::into).collect();
        assert_eq!(names, ["one_minute", "five_minute"]);
        for interval in DecisionInterval::iter() {
            assert_eq!(interval.to_string().parse(), Ok(interval));
        }
    }

    /// After-hours minutes carry volume but no close, so more than the retained bars of them push the close out of a
    /// warmed state; the closes read off the bars keep it.
    #[test]
    fn test_a_close_survives_the_closeless_minutes_after_it() {
        let mut bars = vec![closing("SPY", "19:59:00", 7), closing("SPY", "19:58:00", 5)];
        let mut minute = at("20:00:00");
        for _ in 0..=crate::common::market::state::RETAINED_BARS {
            bars.push(
                TradeBar::new(
                    spy(),
                    BarInterval::OneMinute,
                    minute,
                    TradeSums::new(TradeTotals::default(), None, None),
                )
                .unwrap(),
            );
            minute += TimeDelta::minutes(1);
        }
        let symbols = BTreeSet::from([spy()]);
        assert_eq!(
            warm(bars.clone(), &symbols).last_price(&spy(), BarInterval::OneMinute),
            None
        );
        assert_eq!(
            last_closes(&bars),
            BTreeMap::from([(spy(), Price::from_ticks(7).unwrap())])
        );
    }

    /// A per-name cap that trims a share to a sliver leaves a buy under the broker's dollar minimum, which the guard
    /// holds rather than sending, as a cent cap on DIA did on paper.
    #[tokio::test(start_paused = true)]
    async fn test_a_buy_trimmed_under_the_minimum_is_held() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session_capped(TimeDelta::minutes(5), funded.clone(), DOLLAR / 100);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:04:30", 701_000_000));
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            journaled(&directory),
            [
                "bar_built",
                "target_decided",
                "tradability_read",
                "order_guarded",
                "book_reconciled"
            ]
        );
        assert_eq!(session.book(), &funded);
        assert_eq!(session.standing(), &Standing::Trading);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Each built minute is journaled as the bar the fold built, before anything decides on it.
    #[tokio::test(start_paused = true)]
    async fn test_a_built_minute_is_journaled_as_built() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        tape.file(print("14:01:40", 702_000_000));
        session
            .advance(&|| at("14:03:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        let payloads: Vec<serde_json::Value> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .flat_map(|path| {
                std::fs::read_to_string(path)
                    .unwrap()
                    .lines()
                    .map(|line| {
                        serde_json::from_str::<serde_json::Value>(line).unwrap()["payload"].clone()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            payloads,
            [serde_json::json!({
                "symbol": "SPY",
                "interval": "one_minute",
                "timestamp": "2026-10-07T14:01:00Z",
                "trade_count": 2,
                "volume": 2_000_000,
                "dollar_volume": "1402000000000000",
                "opened_at": "2026-10-07T14:01:10Z",
                "open": 700_000_000,
                "closed_at": "2026-10-07T14:01:40Z",
                "close": 702_000_000,
                "high": 702_000_000,
                "low": 700_000_000,
            })]
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A minute is read only once its wait has passed after it ends: the 14:01 minute is not asked for a second early
    /// and is built on the second its wait passes.
    #[tokio::test(start_paused = true)]
    async fn test_a_minute_is_built_only_once_its_wait_has_passed() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:03:10", 700_000_000));
        session
            .advance(&|| at("14:04:01"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), Vec::<&str>::new());
        assert_eq!(tape.times_asked("14:03:00", "14:04:00"), 0);
        session
            .advance(&|| at("14:04:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        assert_eq!(tape.times_asked("14:03:00", "14:04:00"), 1);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// The session's first minute waits as long as a recheck, since REST serves the opening auction late, every minute
    /// after it waits behind it, and its own recheck waits as long again.
    #[tokio::test(start_paused = true)]
    async fn test_the_opening_minute_waits_as_long_as_a_recheck() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:00:10", 700_000_000));
        session
            .advance(&|| at("14:02:59"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            (
                tape.times_asked("14:00:00", "14:01:00"),
                tape.times_asked("14:01:00", "14:02:00")
            ),
            (0, 0)
        );
        session
            .advance(&|| at("14:03:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        assert_eq!(
            (
                tape.times_asked("14:00:00", "14:01:00"),
                tape.times_asked("14:01:00", "14:02:00")
            ),
            (1, 1)
        );
        // Its recheck comes `RECHECK_AFTER` after it was built, not in the same call that built it.
        for (now, asked) in [("14:04:59", 1), ("14:05:00", 2)] {
            session
                .advance(&|| at(now), &tape, &broker, &mut journal)
                .await
                .unwrap();
            assert_eq!(tape.times_asked("14:00:00", "14:01:00"), asked);
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A one-minute session holds the opening minute as long as a five-minute one, and decides once on the latest
    /// boundary built when the hold ends rather than on each boundary it passed.
    #[tokio::test(start_paused = true)]
    async fn test_a_one_minute_session_first_decides_after_the_opening_wait() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session_from(
            TimeDelta::minutes(5),
            funded,
            5_000 * DOLLAR,
            at("14:00:00"),
            DecisionInterval::OneMinute,
        );
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:00:10", 700_000_000));
        tape.file(print("14:02:30", 700_000_000));
        session
            .advance(&|| at("14:02:59"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(tape.times_asked("14:00:00", "14:01:00"), 0);
        session
            .advance(&|| at("14:03:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(session.built_through, at("14:02:00"));
        let decided = journaled(&directory)
            .iter()
            .filter(|event| *event == &"target_decided")
            .count();
        assert_eq!(decided, 1);
        assert_eq!(session.next_decision, at("14:03:00"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A minute that cannot be read holds every minute after it, and the decision behind them, until a later call
    /// reads it.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_minute_is_retried_and_holds_everything_after_it() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        tape.file(print("14:04:30", 701_000_000));
        tape.failing.lock().unwrap().insert(at("14:02:00"));
        session
            .advance(&|| at("14:05:02"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        assert_eq!(session.built_through, at("14:02:00"));
        assert_eq!(tape.times_asked("14:04:00", "14:05:00"), 0);
        tape.failing.lock().unwrap().clear();
        session
            .advance(&|| at("14:05:03"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        // Failed, then read once; its recheck waits behind the older minutes'.
        assert_eq!(tape.times_asked("14:02:00", "14:03:00"), 2);
        assert_eq!(
            journaled(&directory)[..3],
            ["bar_built", "bar_built", "target_decided"]
        );
        assert_eq!(session.book().position(&spy()).units(), 1_000_000);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A minute every name is silent in, with nothing served after it, is a feed serving nothing: it holds every minute
    /// after it until a later call reads its trades, as on 2026-10-15 when REST served 74 minutes empty.
    #[tokio::test(start_paused = true)]
    async fn test_an_empty_minute_with_nothing_served_after_it_is_retried() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        session
            .advance(&|| at("14:04:05"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        assert_eq!(session.built_through, at("14:02:00"));
        assert_eq!(tape.times_asked("14:03:00", "14:04:00"), 0);
        tape.file(print("14:02:30", 700_500_000));
        tape.file(print("14:03:30", 701_000_000));
        session
            .advance(&|| at("14:04:06"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            journaled(&directory),
            ["bar_built", "bar_built", "bar_built"]
        );
        assert_eq!(session.built_through, at("14:04:00"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A minute every name is silent in is built empty once the tape serves a trade after it, as a market-wide halt
    /// would leave it.
    #[tokio::test(start_paused = true)]
    async fn test_an_empty_minute_is_built_once_a_trade_is_served_after_it() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        tape.file(print("14:03:30", 701_000_000));
        session
            .advance(&|| at("14:04:05"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built", "bar_built"]);
        assert_eq!(session.built_through, at("14:04:00"));
        assert_eq!(tape.times_asked("14:02:00", "14:03:00"), 1);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// One probe serves every silent minute ending by the print it found, so a halt costs one read of what followed it
    /// rather than one per minute.
    #[tokio::test(start_paused = true)]
    async fn test_one_probe_serves_every_silent_minute_before_its_print() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        tape.file(print("14:05:00", 701_000_000));
        session
            .advance(&|| at("14:06:05"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(session.built_through, at("14:06:00"));
        assert_eq!(tape.times_asked("14:03:00", "14:06:05"), 1);
        assert_eq!(tape.times_asked("14:04:00", "14:06:05"), 0);
        assert_eq!(tape.times_asked("14:05:00", "14:06:05"), 0);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Refused rows are no print: a minute holding only them, with only them served after it, is held.
    #[tokio::test(start_paused = true)]
    async fn test_refused_rows_alone_do_not_release_a_silent_minute() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        tape.file(AlpacaTradeOutcome::Refused(RefusedRow::new(
            "QQQ",
            RowRefusal::Unrequested,
        )));
        session
            .advance(&|| at("14:04:05"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        assert_eq!(session.built_through, at("14:02:00"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A minute every name is silent in is held, not built, when the read of what was served after it fails.
    #[tokio::test(start_paused = true)]
    async fn test_an_empty_minute_is_held_when_the_read_after_it_fails() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:01:10", 700_000_000));
        tape.file(print("14:03:30", 701_000_000));
        tape.failing.lock().unwrap().insert(at("14:03:00"));
        session
            .advance(&|| at("14:04:05"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        assert_eq!(session.built_through, at("14:02:00"));
        assert_eq!(tape.times_asked("14:03:00", "14:04:05"), 1);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// An empty closing minute is built once a print is served after the close, so the session still builds through it.
    #[tokio::test(start_paused = true)]
    async fn test_an_empty_closing_minute_is_built_from_a_print_after_the_close() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session_from(
            TimeDelta::minutes(5),
            funded,
            5_000 * DOLLAR,
            at("19:57:00"),
            DecisionInterval::FiveMinute,
        );
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("19:57:30", 701_000_000));
        tape.file(print("19:58:30", 701_000_000));
        session
            .advance(&|| at("20:00:20"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert!(!session.built_through_close());
        tape.file(print("20:00:30", 701_000_000));
        session
            .advance(&|| at("20:00:40"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert!(session.built_through_close());
        assert_eq!(journaled(&directory), ["bar_built", "bar_built"]);
        assert_eq!(tape.times_asked("20:00:00", "20:00:40"), 1);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// The minute before the close is built, nothing past the close is asked for, and no decision is made for the bar
    /// that ends at the close.
    #[tokio::test(start_paused = true)]
    async fn test_the_last_minute_is_built_and_no_decision_ends_at_the_close() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session_from(
            TimeDelta::minutes(5),
            funded,
            5_000 * DOLLAR,
            at("19:55:00"),
            DecisionInterval::FiveMinute,
        );
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("19:59:30", 701_000_000));
        session
            .advance(&|| at("20:00:01"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert!(!session.built_through_close());
        session
            .advance(&|| at("20:10:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert!(session.built_through_close());
        assert_eq!(journaled(&directory), ["bar_built"]);
        assert_eq!(payloads(&directory)[0]["timestamp"], "2026-10-07T19:59:00Z");
        assert_eq!(tape.times_asked("20:00:00", "20:01:00"), 0);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A decision bar that ends before the close but is only built after it decides nothing, since its orders would
    /// rest until the next session.
    #[tokio::test(start_paused = true)]
    async fn test_a_decision_bar_built_after_the_close_decides_nothing() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session_from(
            TimeDelta::minutes(5),
            funded,
            5_000 * DOLLAR,
            at("19:50:00"),
            DecisionInterval::FiveMinute,
        );
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("19:54:30", 701_000_000));
        // The 19:55 decision bar's last minute fails until past the close, then the minute after it fails instead.
        tape.failing.lock().unwrap().insert(at("19:54:00"));
        session
            .advance(&|| at("19:59:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert!(journaled(&directory).is_empty());
        tape.failing.lock().unwrap().clear();
        tape.failing.lock().unwrap().insert(at("19:56:00"));
        session
            .advance(&|| at("20:00:30"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A build that starts before the close and finishes after it decides nothing: the decision reads the clock again
    /// rather than reusing the instant the call began at.
    #[tokio::test(start_paused = true)]
    async fn test_a_build_that_crosses_the_close_decides_nothing() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session_from(
            TimeDelta::minutes(5),
            funded,
            5_000 * DOLLAR,
            at("19:50:00"),
            DecisionInterval::FiveMinute,
        );
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("19:54:30", 701_000_000));
        // The first reading starts the build before the close; every later one is past it.
        let readings = Mutex::new(vec![at("20:00:05"), at("19:59:50")]);
        let clock = || {
            let mut readings = readings.lock().unwrap();
            match readings.len() {
                1 => readings[0],
                _ => readings.pop().unwrap(),
            }
        };
        session
            .advance(&clock, &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A built minute is read once more when `RECHECK_AFTER` has passed since its first read fell due, and only once.
    #[tokio::test(start_paused = true)]
    async fn test_a_built_minute_is_read_again_once_after_the_recheck_wait() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:05:10", 700_000_000));
        // Advanced every ten seconds, so the earlier minutes' rechecks, one per call, are drained well before.
        let mut asked = BTreeMap::new();
        let mut now = at("14:06:10");
        while now <= at("14:09:30") {
            session
                .advance(&|| now, &tape, &broker, &mut journal)
                .await
                .unwrap();
            asked.insert(now, tape.times_asked("14:05:00", "14:06:00"));
            now += TimeDelta::seconds(10);
        }
        let at_checkpoints: Vec<usize> = ["14:06:10", "14:08:00", "14:08:10", "14:09:30"]
            .map(|checkpoint| asked[&at(checkpoint)])
            .to_vec();
        assert_eq!(at_checkpoints, [1, 1, 2, 2]);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A run that catches up on many minutes at once rechecks only the oldest due per call, so it never stalls behind
    /// rereading all of them.
    #[tokio::test(start_paused = true)]
    async fn test_a_caught_up_run_rechecks_one_minute_per_call() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        tape.file(print("14:09:30", 700_000_000));
        session
            .advance(&|| at("14:10:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        let reads = |tape: &Scripted| {
            [
                ("14:00:00", "14:01:00"),
                ("14:01:00", "14:02:00"),
                ("14:02:00", "14:03:00"),
            ]
            .map(|(start, end)| tape.times_asked(start, end))
        };
        assert_eq!(reads(&tape), [2, 1, 1]);
        session
            .advance(&|| at("14:10:01"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(reads(&tape), [2, 2, 1]);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A corrected ABC print and an XYZ print canceled beside a standing one, as the REST history labels them, the
    /// replacement keeping its original's number.
    fn corrected_and_canceled() -> Vec<AlpacaTradeOutcome> {
        let pages = [
            (
                "ABC",
                r#"[{"c":["@","I"],"i":4101,"p":50.25,"s":200,"t":"2026-10-07T15:12:07.5Z","u":"corrected","x":"Q","z":"C"},{"c":["@","I"],"i":4101,"p":50.3,"s":250,"t":"2026-10-07T16:40:02.25Z","u":"incorrect","x":"Q","z":"C"}]"#,
            ),
            (
                "XYZ",
                r#"[{"c":[" "],"i":1,"p":20.1,"s":100,"t":"2026-10-07T16:50:10Z","x":"P","z":"B"},{"c":[" ","I"],"i":900001,"p":20.05,"s":700,"t":"2026-10-07T16:50:30.25Z","u":"canceled","x":"N","z":"B"}]"#,
            ),
        ];
        pages
            .into_iter()
            .flat_map(|(ticker, rows)| {
                let body = format!(r#"{{"trades":{{"{ticker}":{rows}}},"next_page_token":null}}"#);
                trade_page(&Symbol::new(ticker).unwrap(), body.as_bytes())
                    .unwrap()
                    .0
            })
            .collect()
    }

    fn bar_payload(bar: &TradeBar) -> serde_json::Value {
        serde_json::to_value(Observation::BarBuilt(BarBuilt::of(bar))).unwrap()["payload"].clone()
    }

    /// The corrected and canceled prints folded whole, as the archive folds them.
    fn archive_bars() -> Vec<serde_json::Value> {
        let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 7).unwrap());
        let mut fold = TradeFold::new(date, TradeConditions::new(BTreeMap::new()));
        for outcome in corrected_and_canceled() {
            match outcome {
                AlpacaTradeOutcome::Print {
                    print,
                    tape,
                    letters,
                    correction,
                    ..
                } => fold.push_lettered(&print, tape, &letters, correction),
                AlpacaTradeOutcome::Refused(row) => panic!("{row:?}"),
            }
        }
        let (bars, counts) = fold.finish();
        assert_eq!(counts.withdrawn(), 2);
        bars.iter().map(bar_payload).collect()
    }

    /// The same prints read minute by minute by a session leave the live bars the archive's: the original withdrawn,
    /// its replacement standing at the correction's instant, and the canceled print gone.
    #[tokio::test(start_paused = true)]
    async fn test_live_bars_agree_with_the_archive_on_a_correction_and_a_cancel() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        let tape = Scripted::default();
        for outcome in corrected_and_canceled() {
            tape.file(outcome);
        }
        session
            .advance(&|| at("19:00:00"), &tape, &broker, &mut journal)
            .await
            .unwrap();
        let live: Vec<serde_json::Value> = records(&directory)
            .iter()
            .filter(|record| record.observation().event_type() == "bar_built")
            .map(|record| serde_json::to_value(record.observation()).unwrap()["payload"].clone())
            .collect();
        std::fs::remove_dir_all(&directory).unwrap();
        let minutes: Vec<(&str, &str)> = live
            .iter()
            .map(|bar| {
                (
                    bar["symbol"].as_str().unwrap(),
                    bar["timestamp"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            minutes,
            [
                ("ABC", "2026-10-07T16:40:00Z"),
                ("XYZ", "2026-10-07T16:50:00Z")
            ]
        );
        assert_eq!(live, archive_bars());
    }
}
