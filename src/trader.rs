//! One trading session: the tape's prints folded into minute bars, the market state those bars build, and at each
//! decision bar the strategy's target taken through risk, the guard and the broker, then reconciled, all journaled.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use crate::broker::Broker;
use crate::common::book::{Book, Cash, Fill};
use crate::common::journal::Observation;
use crate::common::market::record::BarInterval;
use crate::common::market::state::{MarketEvent, MarketState};
use crate::common::market::trade_bars::{
    BarBuilt, LiveTradeFold, TradeBar, TradeConditions, Withdrawal,
};
use crate::common::market::{Price, StampedPrice, Symbol};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::order::{BrokerFailure, OrderSequence};
use crate::common::playbook::Played;
use crate::common::reconcile::rounding_allowance;
use crate::common::risk::{Limits, TargetDecided, risk};
use crate::common::standing::{
    FeedChanged, FeedContinuity, FeedTransition, HaltCause, SessionHalted,
};
use crate::common::strategy::Strategy;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;
use crate::execution::{
    JournalFailure, OrderOutcome, Patience, ReconcileFailure, Reconciliation, execute,
    reconcile_and_close,
};
use crate::ingest::alpaca::AlpacaTradeOutcome;
use crate::ingest::alpaca::feed::FeedEvent;
use crate::ingest::alpaca::stream::{StreamMessage, TradeId};
use crate::journal::Journal;

/// How long after a minute ends its bar waits for prints still on their way before it is folded in.
const SETTLING: TimeDelta = TimeDelta::seconds(2);

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

/// How a session trades: how often it decides, its limits, how long an order may stay open, and how old a price may
/// be before risk treats it as unpriced.
#[derive(Debug, Clone, Copy)]
pub struct SessionSettings {
    decision: DecisionInterval,
    limits: Limits,
    patience: Patience,
    stale_after: TimeDelta,
}

/// The least staleness a session accepts, since a bar is folded only after its minute ends.
const LEAST_STALENESS: TimeDelta = TimeDelta::minutes(1);

/// Why settings were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SettingsRefusal {
    #[error("a staleness of {stale_after} is under a minute")]
    StalenessUnderAMinute { stale_after: TimeDelta },
}

impl SessionSettings {
    pub fn new(
        decision: DecisionInterval,
        limits: Limits,
        patience: Patience,
        stale_after: TimeDelta,
    ) -> Result<Self, SettingsRefusal> {
        if stale_after < LEAST_STALENESS {
            return Err(SettingsRefusal::StalenessUnderAMinute { stale_after });
        }
        Ok(Self {
            decision,
            limits,
            patience,
            stale_after,
        })
    }
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

/// One session's trading state; `observe` folds the tape in and `advance` moves the session to an instant.
pub struct Session<S: Strategy> {
    strategy: Played<S>,
    settings: SessionSettings,
    calendar: TradingCalendar,
    fold: LiveTradeFold<(Symbol, TradeId)>,
    state: MarketState,
    book: Book,
    /// The book's worth when the session opened, from which the daily loss is measured.
    opening: Cash,
    /// Fills since the last reconciliation, which bound how far the broker's cash may stray from the book's.
    fills: Vec<Fill>,
    next_decision: DateTime<Utc>,
    sequence: OrderSequence,
    /// Whether the tape is whole: draining and deciding wait out a gap until a backfill after a reopen covers it.
    continuity: FeedContinuity,
    standing: Standing,
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

/// The transition a feed event makes, for the events that can move the continuity.
fn transition(event: &FeedEvent) -> Option<FeedTransition> {
    match event {
        FeedEvent::Lost { .. } => Some(FeedTransition::Lost),
        FeedEvent::Reopened { attempts } => Some(FeedTransition::Reopened {
            attempts: *attempts,
        }),
        FeedEvent::Backfilled { since, fresh } => Some(FeedTransition::Backfilled {
            since: *since,
            fresh: *fresh,
        }),
        FeedEvent::BackfillFailed { .. } | FeedEvent::Message(_) => None,
    }
}

impl<S: Strategy> Session<S> {
    /// A session for `session`'s tape starting at `now`, from a state warmed on earlier sessions and the book the
    /// broker reported at the open, worth `opening`.
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
        warm: MarketState,
        book: Book,
        opening: Cash,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            next_decision: decision_after(now, settings.decision),
            strategy,
            settings,
            calendar,
            fold: LiveTradeFold::new(session, conditions),
            state: warm,
            book,
            opening,
            fills: Vec::new(),
            sequence: OrderSequence::default(),
            // The feed's first backfill, from where the tape is wanted, makes it whole.
            continuity: FeedContinuity::Reopened,
            standing: Standing::Trading,
        }
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn standing(&self) -> &Standing {
        &self.standing
    }

    /// Folds one feed event in at `now`: a print goes to the fold, which decides what it may set, and the feed's own
    /// events say whether the tape is whole, each change journaled as `feed_changed`.
    pub fn observe(
        &mut self,
        now: DateTime<Utc>,
        event: &FeedEvent,
        journal: &mut Journal,
    ) -> Result<(), SessionError> {
        if let Some(changed) =
            transition(event).and_then(|transition| FeedChanged::of(self.continuity, transition))
        {
            self.continuity = changed.to();
            if let Err(error) = journal.append(now, Observation::FeedChanged(changed)) {
                self.halt(journal, journal_refused(&error));
                return Err(SessionError::Journal(JournalFailure::before_any_order(
                    error,
                )));
            }
        }
        match event {
            FeedEvent::Message(StreamMessage::Trade {
                outcome:
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        correction,
                    },
                id,
            }) => self.fold.push(
                (print.symbol().clone(), *id),
                print.clone(),
                *tape,
                letters.clone(),
                *correction,
            ),
            FeedEvent::Message(StreamMessage::Corrected {
                symbol,
                original,
                replacement,
                outcome,
            }) => {
                self.withdraw(symbol, *original);
                match outcome {
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        correction,
                    } => self.fold.push(
                        (symbol.clone(), *replacement),
                        print.clone(),
                        *tape,
                        letters.clone(),
                        *correction,
                    ),
                    AlpacaTradeOutcome::Refused(_) => {}
                }
            }
            FeedEvent::Message(StreamMessage::Canceled {
                symbol, original, ..
            }) => self.withdraw(symbol, *original),
            FeedEvent::Message(
                StreamMessage::Trade {
                    outcome: AlpacaTradeOutcome::Refused(_),
                    ..
                }
                | StreamMessage::Connected
                | StreamMessage::Authenticated
                | StreamMessage::Subscribed(_)
                | StreamMessage::Quote(_)
                | StreamMessage::Refused { .. }
                | StreamMessage::Unrecognized { .. }
                | StreamMessage::Malformed { .. },
            )
            | FeedEvent::Lost { .. }
            | FeedEvent::Reopened { .. }
            | FeedEvent::Backfilled { .. }
            | FeedEvent::BackfillFailed { .. } => {}
        }
        Ok(())
    }

    /// Withdraws the print a correction or cancel names, logging one no open minute holds, which changes no bar.
    fn withdraw(&mut self, symbol: &Symbol, original: TradeId) {
        match self.fold.withdraw(&(symbol.clone(), original)) {
            Withdrawal::Applied => {}
            Withdrawal::NotHeld => {
                tracing::warn!(%symbol, ?original, "Withdrawal not applied");
            }
        }
    }

    /// Folds in every minute settled by `now`, and once a decision bar has settled, trades the book toward the
    /// strategy's target within the limits and reconciles it with the broker's. While the tape has a gap nothing is
    /// drained or decided, and a late call decides once, for the latest settled bar, rather than for each it passed.
    pub async fn advance(
        &mut self,
        now: DateTime<Utc>,
        broker: &impl Broker,
        journal: &mut Journal,
    ) -> Result<(), SessionError> {
        self.fold_in(MarketEvent::Clock(now));
        match self.continuity {
            FeedContinuity::Whole => {}
            FeedContinuity::Interrupted | FeedContinuity::Reopened => return Ok(()),
        }
        let settled = now - SETTLING;
        for bar in self.fold.drain_through(settled) {
            if let Err(error) = journal.append(now, Observation::BarBuilt(BarBuilt::of(&bar))) {
                self.halt(journal, journal_refused(&error));
                return Err(SessionError::Journal(JournalFailure::before_any_order(
                    error,
                )));
            }
            self.fold_in(MarketEvent::Trades(bar));
        }
        match self.standing {
            Standing::Trading => {}
            Standing::Halted(_) => return Ok(()),
        }
        if settled < self.next_decision {
            return Ok(());
        }
        let bar = decision_before(settled, self.settings.decision);
        self.next_decision = decision_after(settled, self.settings.decision);
        let wanted = self.strategy.decide(&self.state, &self.book);
        let phase = self.calendar.phase_at(now);
        let restrained = risk(
            &self.settings.limits,
            phase,
            self.opening,
            &self.book,
            |symbol| self.fresh_price(symbol, now),
            wanted.clone(),
        );
        let stretch = self.strategy.stretch_at(&self.state);
        let decided = TargetDecided::new(bar, stretch, wanted, restrained.clone());
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
        let close = self.state.last_close(symbol, BarInterval::OneMinute)?;
        (now - close.at() <= self.settings.stale_after).then_some(close.price())
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
    use crate::common::market::trade_bars::TradeFold;
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
    use crate::ingest::alpaca::stream::{TradeId, messages};
    use crate::ingest::alpaca::trade_page;

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
        let settings =
            SessionSettings::new(DecisionInterval::FiveMinute, limits, patience, stale_after)
                .unwrap();
        let mut session = Session::new(
            Played::throughout(OneShare),
            settings,
            calendar,
            date,
            TradeConditions::new(BTreeMap::new()),
            MarketState::default(),
            funded,
            dollars(10_000),
            at("14:00:00"),
        );
        // The startup backfill's record goes to a journal of its own, so each test's journal holds only its own steps.
        let (mut journal, directory) = journal();
        session
            .observe(
                at("14:00:00"),
                &FeedEvent::Backfilled {
                    since: at("13:59:00"),
                    fresh: 0,
                },
                &mut journal,
            )
            .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        session
    }

    /// A one-share SPY print at `when` for `ticks`, a regular sale on the consolidated tape.
    fn print(number: u64, when: &str, ticks: i64) -> FeedEvent {
        let trade = Trade::new(
            spy(),
            at(when),
            Price::from_ticks(ticks).unwrap(),
            Shares::whole(1).unwrap(),
        )
        .unwrap();
        FeedEvent::Message(StreamMessage::Trade {
            id: TradeId::read(&serde_json::json!({"x": "P", "i": number})).unwrap(),
            outcome: AlpacaTradeOutcome::Print {
                print: Print::Trade(trade),
                tape: Tape::ConsolidatedTape,
                letters: vec![ConditionLetter::of(' ')],
                correction: Correction::Stands,
            },
        })
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

    /// Prints fold into minute bars as they settle; nothing is decided before the 14:05 bar settles, and then the book
    /// buys the share the strategy wants at a fresh price, the fill agreeing with the broker's book.
    #[tokio::test(start_paused = true)]
    async fn test_a_settled_decision_bar_trades_to_the_target_and_reconciles() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:01:10", 700_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:03:00"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        session
            .observe(
                at("14:00:00"),
                &print(2, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:01"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        session
            .advance(at("14:05:02"), &broker, &mut journal)
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
            .advance(at("14:09:00"), &broker, &mut journal)
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:01:10", 700_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
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
        session
            .observe(
                at("14:00:00"),
                &print(2, "14:09:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:10:02"), &broker, &mut journal)
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:59", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(session.book().position(&spy()).units(), 1_000_000);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// From a lost stream until a backfill after its reopen, nothing is drained or decided; then the session catches
    /// up and decides on the whole tape.
    #[tokio::test(start_paused = true)]
    async fn test_a_gap_in_the_tape_holds_the_session_until_backfilled() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        session
            .observe(
                at("14:00:00"),
                &FeedEvent::Lost {
                    cause: crate::ingest::alpaca::stream::StreamError::Ended,
                },
                &mut journal,
            )
            .unwrap();
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        session
            .observe(
                at("14:00:00"),
                &FeedEvent::Reopened { attempts: 1 },
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:03"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["feed_changed", "feed_changed"]);
        session
            .observe(
                at("14:00:00"),
                &FeedEvent::Backfilled {
                    since: at("14:04:00"),
                    fresh: 0,
                },
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:04"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            journaled(&directory)[..5],
            [
                "feed_changed",
                "feed_changed",
                "feed_changed",
                "bar_built",
                "target_decided"
            ]
        );
        let changes: Vec<serde_json::Value> = payloads(&directory)[..3].to_vec();
        assert_eq!(
            changes,
            [
                serde_json::json!({"from": "whole", "transition": "lost"}),
                serde_json::json!({"from": "interrupted", "transition": {"reopened": {"attempts": 1}}}),
                serde_json::json!({"from": "reopened", "transition": {"backfilled": {"since": "2026-10-07T14:04:00Z", "fresh": 0}}}),
            ]
        );
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
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

    /// Nothing is drained or decided before the feed's startup backfill arrives.
    #[tokio::test(start_paused = true)]
    async fn test_a_session_waits_for_the_startup_backfill() {
        let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 7).unwrap());
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let started = session(TimeDelta::minutes(5), funded.clone());
        let mut session = Session::new(
            Played::throughout(OneShare),
            started.settings,
            started.calendar,
            date,
            TradeConditions::new(BTreeMap::new()),
            MarketState::default(),
            funded,
            Cash::from_units(10_000 * DOLLAR),
            at("14:00:00"),
        );
        let (mut journal, directory) = journal();
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), Vec::<&str>::new());
        session
            .observe(
                at("14:00:00"),
                &FeedEvent::Backfilled {
                    since: at("14:00:00"),
                    fresh: 0,
                },
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:03"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            journaled(&directory)[..3],
            ["feed_changed", "bar_built", "target_decided"]
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
        std::fs::remove_dir_all(&directory).unwrap();
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        let failed = session.advance(at("14:05:02"), &broker, &mut journal).await;
        assert!(matches!(failed, Err(SessionError::Journal(_))));
        assert!(matches!(
            session.standing(),
            Standing::Halted(HaltCause::JournalRefused { .. })
        ));
    }

    /// A journal that refuses a feed change halts the session and fails the call.
    #[test]
    fn test_a_refused_feed_change_halts_the_session() {
        let mut session = session(TimeDelta::minutes(5), Book::default());
        let (mut journal, directory) = journal();
        std::fs::remove_dir_all(&directory).unwrap();
        let failed = session.observe(
            at("14:00:00"),
            &FeedEvent::Lost {
                cause: crate::ingest::alpaca::stream::StreamError::Ended,
            },
            &mut journal,
        );
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        let failed = session.advance(at("14:05:02"), &broker, &mut journal).await;
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:01:10", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:03:00"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["bar_built"]);
        std::fs::remove_dir_all(&gone).unwrap();
        let failed = session
            .advance(at("14:05:02"), &broker, &mut refusing)
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

    #[test]
    fn test_a_staleness_under_a_minute_is_refused() {
        let limits = Limits::new(
            Cash::from_units(DOLLAR),
            Cash::from_units(DOLLAR),
            Cash::from_units(DOLLAR),
            TimeDelta::zero(),
        )
        .unwrap();
        let patience = Patience::new(Duration::from_secs(1), Duration::from_secs(1)).unwrap();
        let settings = |seconds| {
            SessionSettings::new(
                DecisionInterval::OneMinute,
                limits,
                patience,
                TimeDelta::seconds(seconds),
            )
        };
        assert_eq!(
            settings(59).map(|_| ()),
            Err(SettingsRefusal::StalenessUnderAMinute {
                stale_after: TimeDelta::seconds(59)
            })
        );
        assert!(settings(60).is_ok());
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
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:04:30", 701_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:05:02"), &broker, &mut journal)
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

    /// Each settled minute is journaled as the bar the fold built, before anything decides on it.
    #[tokio::test(start_paused = true)]
    async fn test_a_settled_minute_is_journaled_as_built() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        session
            .observe(
                at("14:00:00"),
                &print(1, "14:01:10", 700_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .observe(
                at("14:00:00"),
                &print(2, "14:01:40", 702_000_000),
                &mut journal,
            )
            .unwrap();
        session
            .advance(at("14:03:00"), &broker, &mut journal)
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

    /// A correction of an ABC print and a cancel of an XYZ print, shaped as the stream sends them.
    const CORRECTED_PRINT: &str = r#"{"T":"t","S":"ABC","i":4101,"x":"Q","p":50.25,"s":200,"c":["@","I"],"t":"2026-10-07T15:12:07.5Z","z":"C"}"#;
    const CORRECTION: &str = r#"{"S":"ABC","T":"c","cc":["@","I"],"ci":4188,"cp":50.3,"cs":250,"oc":["@"],"oi":4101,"op":50.25,"os":200,"t":"2026-10-07T16:40:02.25Z","x":"Q","z":"C"}"#;
    const STANDING_PRINT: &str = r#"{"T":"t","S":"XYZ","i":1,"x":"P","p":20.1,"s":100,"c":[" "],"t":"2026-10-07T16:50:10Z","z":"B"}"#;
    const CANCELED_PRINT: &str = r#"{"T":"t","S":"XYZ","i":900001,"x":"N","p":20.05,"s":700,"c":[" ","I"],"t":"2026-10-07T16:50:30.25Z","z":"B"}"#;
    const CANCEL: &str = r#"{"S":"XYZ","T":"x","a":"C","i":900001,"p":20.05,"s":700,"t":"2026-10-07T16:52:00Z","x":"N","z":"B"}"#;

    /// The same prints as the REST history labels them, the replacement keeping its original's number, folded as the
    /// archive folds them.
    fn archive_bars() -> Vec<serde_json::Value> {
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
        let date = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 7).unwrap());
        let mut fold = TradeFold::new(date, TradeConditions::new(BTreeMap::new()));
        for (ticker, rows) in pages {
            let body = format!(r#"{{"trades":{{"{ticker}":{rows}}},"next_page_token":null}}"#);
            let (outcomes, _) = trade_page(&Symbol::new(ticker).unwrap(), body.as_bytes()).unwrap();
            for outcome in outcomes {
                match outcome {
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        correction,
                    } => fold.push_lettered(&print, tape, &letters, correction),
                    AlpacaTradeOutcome::Refused(row) => panic!("{row:?}"),
                }
            }
        }
        let (bars, counts) = fold.finish();
        assert_eq!(counts.withdrawn(), 2);
        bars.iter()
            .map(|bar| {
                serde_json::to_value(Observation::BarBuilt(BarBuilt::of(bar))).unwrap()["payload"]
                    .clone()
            })
            .collect()
    }

    /// The bars a session journals from `steps`, each frame observed after advancing to the instant beside it, if any.
    async fn live_bars(steps: &[(Option<&str>, &str)]) -> Vec<serde_json::Value> {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        for (advance_to, frame) in steps {
            if let Some(advance_to) = advance_to {
                session
                    .advance(at(advance_to), &broker, &mut journal)
                    .await
                    .unwrap();
            }
            for message in messages(&format!("[{frame}]")).unwrap() {
                session
                    .observe(at("14:00:00"), &FeedEvent::Message(message), &mut journal)
                    .unwrap();
            }
        }
        session
            .advance(at("19:00:00"), &broker, &mut journal)
            .await
            .unwrap();
        let bars = records(&directory)
            .iter()
            .filter(|record| record.observation().event_type() == "bar_built")
            .map(|record| serde_json::to_value(record.observation()).unwrap()["payload"].clone())
            .collect();
        std::fs::remove_dir_all(&directory).unwrap();
        bars
    }

    /// A correction and a cancel arriving while their prints' minutes are open leave the live bars the archive's: the
    /// original withdrawn, its replacement standing at the correction's instant, and the canceled print gone.
    #[tokio::test(start_paused = true)]
    async fn test_live_bars_agree_with_the_archive_on_a_correction_and_a_cancel() {
        let live = live_bars(&[
            (None, CORRECTED_PRINT),
            (None, CORRECTION),
            (None, STANDING_PRINT),
            (None, CANCELED_PRINT),
            (None, CANCEL),
        ])
        .await;
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

    /// A correction or cancel arriving after its print's bar was built leaves that bar as built; the replacement still
    /// stands at the correction's instant.
    #[tokio::test(start_paused = true)]
    async fn test_a_late_correction_or_cancel_leaves_the_built_bar() {
        let live = live_bars(&[
            (None, CORRECTED_PRINT),
            (Some("15:14:00"), CORRECTION),
            (None, STANDING_PRINT),
            (None, CANCELED_PRINT),
            (Some("16:52:00"), CANCEL),
        ])
        .await;
        let volumes: Vec<(&str, &str, i64)> = live
            .iter()
            .map(|bar| {
                (
                    bar["symbol"].as_str().unwrap(),
                    bar["timestamp"].as_str().unwrap(),
                    bar["volume"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            volumes,
            [
                ("ABC", "2026-10-07T15:12:00Z", 200_000_000),
                ("ABC", "2026-10-07T16:40:00Z", 250_000_000),
                ("XYZ", "2026-10-07T16:50:00Z", 800_000_000),
            ]
        );
    }
}
