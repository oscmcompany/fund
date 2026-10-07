//! One trading session: the tape's prints folded into minute bars, the market state those bars build, and at each
//! decision bar the strategy's target taken through risk, the guard and the broker, then reconciled, all journaled.

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use crate::broker::Broker;
use crate::common::book::{Book, Cash, Fill};
use crate::common::journal::Observation;
use crate::common::market::record::BarInterval;
use crate::common::market::state::{MarketEvent, MarketState};
use crate::common::market::trade_bars::{TradeConditions, TradeFold};
use crate::common::market::{Price, Symbol};
use crate::common::monoid::Monoid;
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

/// How often a session decides: the bars it decides at the end of, always within the day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionInterval {
    OneMinute,
    FiveMinute,
}

impl DecisionInterval {
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

/// Why settings were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsRefusal {
    NegativeStaleness { stale_after: TimeDelta },
}

impl SessionSettings {
    pub fn new(
        decision: DecisionInterval,
        limits: Limits,
        patience: Patience,
        stale_after: TimeDelta,
    ) -> Result<Self, SettingsRefusal> {
        if stale_after < TimeDelta::zero() {
            return Err(SettingsRefusal::NegativeStaleness { stale_after });
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
    /// Set once a reconciliation diverges; the session then decides nothing more.
    halted: bool,
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
            halted: false,
        }
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn halted(&self) -> bool {
        self.halted
    }

    /// Folds one feed event in; only a print changes the session, and the fold decides what it may set.
    pub fn observe(&mut self, event: &FeedEvent) {
        match event {
            FeedEvent::Message(StreamMessage::Trade {
                outcome:
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        corrected,
                    },
                ..
            }) => self.fold.push_lettered(print, *tape, letters, *corrected),
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
    /// strategy's target within the limits and reconciles it with the broker's.
    pub async fn advance(
        &mut self,
        now: DateTime<Utc>,
        broker: &impl Broker,
        journal: &mut Journal,
    ) -> Result<(), SessionError> {
        let settled = now - SETTLING;
        for bar in self.fold.drain_through(settled) {
            self.fold_in(MarketEvent::Trades(bar));
        }
        self.fold_in(MarketEvent::Clock(now));
        if self.halted || settled < self.next_decision {
            return Ok(());
        }
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
        let decided = TargetDecided::new(wanted, restrained.clone());
        if let Err(error) = journal.append(now, Observation::TargetDecided(decided)) {
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
        let outcomes = execute(
            broker,
            journal,
            &mut self.next_sequence,
            &self.book,
            restrained.target(),
            self.settings.patience,
        )
        .await
        .map_err(SessionError::Journal)?;
        for outcome in outcomes {
            match outcome {
                OrderOutcome::Closed(Some(fill)) => {
                    self.book = std::mem::take(&mut self.book).combine(Book::of(&fill));
                    self.fills.push(fill);
                }
                OrderOutcome::Closed(None)
                | OrderOutcome::Guarded(_)
                | OrderOutcome::Refused
                | OrderOutcome::Unresolved(_) => {}
            }
        }
        let reconciliation = reconcile_and_close(
            broker,
            journal,
            &mut self.next_sequence,
            &self.book,
            rounding_allowance(&self.fills),
            self.settings.patience,
        )
        .await
        .map_err(SessionError::Reconcile)?;
        self.halted = !reconciliation.reading.agrees();
        self.book = reconciliation.book;
        self.fills.clear();
        Ok(())
    }

    fn fold_in(&mut self, event: MarketEvent) {
        let state = std::mem::take(&mut self.state);
        self.state = state.combine(MarketState::of(event));
    }

    /// The symbol's last one-minute close, unless it is older than the settings allow.
    fn fresh_price(&self, symbol: &Symbol, now: DateTime<Utc>) -> Option<Price> {
        let (at, price) = self.state.last_close(symbol, BarInterval::OneMinute)?;
        (now - at <= self.settings.stale_after).then_some(price)
    }
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
    use crate::common::market::record::Trade;
    use crate::common::market::trade_bars::{Print, Tape};
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

    /// A broker that fills every order at once at `ticks`, keeping its own book, which `skew` moves off the session's.
    struct Filling {
        ticks: i64,
        book: Mutex<Book>,
        last: Mutex<Option<BrokerOrder>>,
    }

    impl Filling {
        fn new(ticks: i64, book: Book) -> Self {
            Self {
                ticks,
                book: Mutex::new(book),
                last: Mutex::new(None),
            }
        }
    }

    impl Broker for Filling {
        async fn submit(&self, request: &OrderRequest) -> Result<BrokerOrder, BrokerError> {
            let order = request.order();
            let price = Price::from_ticks(self.ticks).unwrap();
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
            dollars(5_000),
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
        Session::new(
            OneShare,
            settings,
            calendar,
            date,
            TradeConditions::new(BTreeMap::new()),
            MarketState::default(),
            funded,
            dollars(10_000),
            at("14:00:00"),
        )
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
                corrected: false,
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

    #[test]
    fn test_a_decision_bar_ends_on_its_next_boundary() {
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
    fn test_a_negative_staleness_is_refused() {
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
        assert!(matches!(
            SessionSettings::new(
                DecisionInterval::OneMinute,
                limits,
                patience,
                TimeDelta::seconds(-1)
            ),
            Err(SettingsRefusal::NegativeStaleness { .. })
        ));
    }
}
