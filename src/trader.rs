//! One trading session: the tape's prints folded into minute bars, the market state those bars build, and at each
//! decision bar the strategy's target taken through risk, the guard and the broker, then reconciled, all journaled.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use crate::broker::Broker;
use crate::common::book::{Book, Cash, Fill};
use crate::common::journal::Observation;
use crate::common::market::record::BarInterval;
use crate::common::market::state::{MarketEvent, MarketState};
use crate::common::market::trade_bars::{TradeBar, TradeConditions, TradeFold};
use crate::common::market::{Price, Symbol};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::reconcile::rounding_allowance;
use crate::common::risk::{Limits, TargetDecided, risk};
use crate::common::strategy::Strategy;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;
use crate::execution::{
    JournalFailed, OrderOutcome, Patience, ReconcileFailed, execute, reconcile_and_close,
};
use crate::ingest::alpaca::AlpacaTradeOutcome;
use crate::ingest::alpaca::feed::FeedEvent;
use crate::ingest::alpaca::stream::StreamMessage;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsRefusal {
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
    Journal(JournalFailed),
    Reconcile(ReconcileFailed),
}

/// One session's trading state; `observe` folds the tape in and `advance` moves the session to an instant.
pub struct Session<S: Strategy> {
    strategy: S,
    settings: SessionSettings,
    calendar: TradingCalendar,
    fold: TradeFold,
    state: MarketState,
    book: Book,
    /// The book's worth when the session opened, from which the daily loss is measured.
    opening: Cash,
    /// Fills since the last reconciliation, which bound how far the broker's cash may stray from the book's.
    fills: Vec<Fill>,
    next_decision: DateTime<Utc>,
    next_sequence: u32,
    /// Whether the tape is whole: draining and deciding wait out a gap until a backfill after a reopen covers it.
    tape: Tape,
    /// Set once a reconciliation diverges, an order is left unresolved or a step fails; the session then decides
    /// nothing more.
    halted: bool,
}

/// Where the feed stands, so bars are drained only from a whole tape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tape {
    Whole,
    /// The stream was lost; prints may be missing until a reopen and a backfill behind it.
    Interrupted,
    /// The stream is open but the tape has a gap before it, at startup or after a reopen; the next backfill closes it.
    Reopened,
}

impl Tape {
    fn after(self, event: &FeedEvent) -> Self {
        match (self, event) {
            (Self::Whole | Self::Interrupted | Self::Reopened, FeedEvent::Lost { .. }) => {
                Self::Interrupted
            }
            (Self::Interrupted | Self::Reopened, FeedEvent::Reopened { .. }) => Self::Reopened,
            (Self::Reopened, FeedEvent::Backfilled { .. }) => Self::Whole,
            (Self::Whole, FeedEvent::Reopened { .. })
            | (Self::Whole | Self::Interrupted, FeedEvent::Backfilled { .. })
            | (
                Self::Whole | Self::Interrupted | Self::Reopened,
                FeedEvent::BackfillFailed { .. } | FeedEvent::Message(_),
            ) => self,
        }
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
        strategy: S,
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
            fold: TradeFold::new(session, conditions),
            state: warm,
            book,
            opening,
            fills: Vec::new(),
            next_sequence: 0,
            // The feed's first backfill, from where the tape is wanted, makes it whole.
            tape: Tape::Reopened,
            halted: false,
        }
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn halted(&self) -> bool {
        self.halted
    }

    /// Folds one feed event in: a print goes to the fold, which decides what it may set, and the feed's own events say
    /// whether the tape is whole.
    pub fn observe(&mut self, event: &FeedEvent) {
        self.tape = self.tape.after(event);
        match event {
            FeedEvent::Message(StreamMessage::Trade {
                outcome:
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        correction,
                    },
                ..
            }) => self.fold.push_lettered(print, *tape, letters, *correction),
            FeedEvent::Message(
                StreamMessage::Trade {
                    outcome: AlpacaTradeOutcome::Refused(_),
                    ..
                }
                | StreamMessage::Connected
                | StreamMessage::Authenticated
                | StreamMessage::Subscribed { .. }
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
        match self.tape {
            Tape::Whole => {}
            Tape::Interrupted | Tape::Reopened => return Ok(()),
        }
        let settled = now - SETTLING;
        for bar in self.fold.drain_through(settled) {
            self.fold_in(MarketEvent::Trades(bar));
        }
        if self.halted || settled < self.next_decision {
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
        let decided = TargetDecided::new(bar, wanted, restrained.clone());
        if let Err(error) = journal.append(now, Observation::TargetDecided(decided)) {
            self.halted = true;
            return Err(SessionError::Journal(JournalFailed {
                outcomes: Vec::new(),
                error,
            }));
        }
        let restrained = match restrained {
            Ok(restrained) => restrained,
            // An unpriced exposure cannot be capped, so the book is left as it stands until the price returns.
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
            &mut self.next_sequence,
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
                self.take(&failed.outcomes);
                self.halted = true;
                return Err(SessionError::Journal(failed));
            }
        };
        self.take(&outcomes);
        let reconciliation = match reconcile_and_close(
            broker,
            journal,
            &mut self.next_sequence,
            &self.book,
            rounding_allowance(&self.fills),
            &prices,
            self.settings.patience,
        )
        .await
        {
            Ok(reconciliation) => reconciliation,
            Err(failed) => {
                self.halted = true;
                return Err(SessionError::Reconcile(failed));
            }
        };
        self.halted |= !reconciliation.reading.agrees();
        self.book = reconciliation.book;
        self.fills.clear();
        Ok(())
    }

    /// Folds each closed order's fill into the book, halting on an order left unresolved, which may still be open at
    /// the broker and would overlap the next decision's orders.
    fn take(&mut self, outcomes: &[OrderOutcome]) {
        for outcome in outcomes {
            match outcome {
                OrderOutcome::Closed(Some(fill)) => {
                    self.book = std::mem::take(&mut self.book).combine(Book::of(fill));
                    self.fills.push(fill.clone());
                }
                OrderOutcome::Unresolved(_) => self.halted = true,
                OrderOutcome::Closed(None) | OrderOutcome::Guarded(_) | OrderOutcome::Refused => {}
            }
        }
    }

    fn fold_in(&mut self, event: MarketEvent) {
        let state = std::mem::take(&mut self.state);
        self.state = state.combine(MarketState::of(event));
    }

    /// The symbol's last one-minute close, unless the print that set it is older than the settings allow.
    fn fresh_price(&self, symbol: &Symbol, now: DateTime<Utc>) -> Option<Price> {
        let (at, price) = self.state.last_close(symbol, BarInterval::OneMinute)?;
        (now - at <= self.settings.stale_after).then_some(price)
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
        if let Some((at, price)) = bar.sums().open_close().map(|prices| prices.close()) {
            closes
                .entry(bar.symbol().clone())
                .and_modify(|latest: &mut (DateTime<Utc>, Price)| {
                    *latest = (*latest).max((at, price))
                })
                .or_insert((at, price));
        }
    }
    closes
        .into_iter()
        .map(|(symbol, (_, price))| (symbol, price))
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
    use crate::broker::alpaca::{BrokerError, BrokerOrder, BrokerOrderId, Cancel};
    use crate::common::guard::Tradability;
    use crate::common::journal::{ReadLine, RunId, read};
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::record::Trade;
    use crate::common::market::trade_bars::{Correction, OpenClose, Print, Tape, TradeSums};
    use crate::common::market::{DollarVolume, Shares};
    use crate::common::order::{
        ClientOrderId, OrderEnding, OrderExecution, OrderReport, OrderRequest, OrderStatus,
    };
    use crate::common::strategy::Target;
    use crate::common::time::calendar::TradingSession;
    use crate::ingest::alpaca::stream::TradeId;

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
        book: Mutex<Book>,
        last: Mutex<Option<BrokerOrder>>,
    }

    impl Filling {
        fn new(ticks: i64, book: Book) -> Self {
            Self {
                ticks,
                fills: true,
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
            date,
            date,
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
        let patience = Patience {
            poll: Duration::from_secs(1),
            open_for: Duration::from_secs(30),
        };
        let settings =
            SessionSettings::new(DecisionInterval::FiveMinute, limits, patience, stale_after)
                .unwrap();
        let mut session = Session::new(
            OneShare,
            settings,
            calendar,
            date,
            TradeConditions::new(BTreeMap::new()),
            MarketState::default(),
            funded,
            dollars(10_000),
            at("14:00:00"),
        );
        session.observe(&FeedEvent::Backfilled {
            since: at("13:59:00"),
            fresh: 0,
        });
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
                letters: vec![' '],
                correction: Correction::Stands,
            },
        })
    }

    fn journaled(directory: &std::path::Path) -> Vec<&'static str> {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .flat_map(|path| read(&std::fs::read_to_string(path).unwrap()))
            .map(|line| match line {
                ReadLine::Read(record) => record.observation().event_type(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
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
        session.observe(&print(1, "14:01:10", 700_000_000));
        session
            .advance(at("14:03:00"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), Vec::<&str>::new());
        session.observe(&print(2, "14:04:30", 701_000_000));
        session
            .advance(at("14:05:01"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), Vec::<&str>::new());
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            journaled(&directory),
            [
                "target_decided",
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
        assert!(!session.halted());
        session
            .advance(at("14:09:00"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory).len(), 4);
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
        session.observe(&print(1, "14:01:10", 700_000_000));
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), ["target_decided"]);
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
        session.observe(&print(1, "14:04:30", 701_000_000));
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert!(session.halted());
        let decided = journaled(&directory).len();
        session.observe(&print(2, "14:09:30", 701_000_000));
        session
            .advance(at("14:10:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory).len(), decided);
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
        session.observe(&print(1, "14:04:59", 701_000_000));
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
        session.observe(&FeedEvent::Lost {
            cause: "the stream closed".to_string(),
        });
        session.observe(&print(1, "14:04:30", 701_000_000));
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        session.observe(&FeedEvent::Reopened { attempts: 1 });
        session
            .advance(at("14:05:03"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), Vec::<&str>::new());
        session.observe(&FeedEvent::Backfilled {
            since: at("14:04:00"),
            fresh: 0,
        });
        session
            .advance(at("14:05:04"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory)[0], "target_decided");
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
        session.observe(&print(1, "14:04:30", 701_000_000));
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert!(session.halted());
        assert!(journaled(&directory).contains(&"order_unresolved"));
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
            OneShare,
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
        session.observe(&print(1, "14:04:30", 701_000_000));
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory), Vec::<&str>::new());
        session.observe(&FeedEvent::Backfilled {
            since: at("14:00:00"),
            fresh: 0,
        });
        session
            .advance(at("14:05:03"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(journaled(&directory)[0], "target_decided");
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A journal that refuses the decision halts the session as well as failing the call.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_step_halts_the_session() {
        let funded = Book::funded(Cash::from_units(10_000 * DOLLAR));
        let broker = Filling::new(701_000_000, funded.clone());
        let mut session = session(TimeDelta::minutes(5), funded);
        let (mut journal, directory) = journal();
        std::fs::remove_dir_all(&directory).unwrap();
        session.observe(&print(1, "14:04:30", 701_000_000));
        let failed = session.advance(at("14:05:02"), &broker, &mut journal).await;
        assert!(matches!(failed, Err(SessionError::Journal(_))));
        assert!(session.halted());
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
        let patience = Patience {
            poll: Duration::from_secs(1),
            open_for: Duration::from_secs(1),
        };
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
        let (start, price) = (at(minute), Price::from_ticks(ticks).unwrap());
        TradeBar::new(
            Symbol::new(raw).unwrap(),
            BarInterval::OneMinute,
            start,
            TradeSums::new(
                TradeTotals::default(),
                Some(OpenClose::new((start, price), (start, price)).unwrap()),
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
                closing("QQQ", "14:01:00", 3),
            ],
            &BTreeSet::from([spy()]),
        );
        assert_eq!(
            state.last_close(&spy(), BarInterval::OneMinute),
            Some((at("14:01:00"), Price::from_ticks(2).unwrap()))
        );
        assert_eq!(
            state.last_close(&Symbol::new("QQQ").unwrap(), BarInterval::OneMinute),
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
        session.observe(&print(1, "14:04:30", 701_000_000));
        session
            .advance(at("14:05:02"), &broker, &mut journal)
            .await
            .unwrap();
        assert_eq!(
            journaled(&directory),
            ["target_decided", "order_guarded", "book_reconciled"]
        );
        assert_eq!(session.book(), &funded);
        assert!(!session.halted());
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
