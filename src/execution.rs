//! Takes a book to a target at a broker: each order is submitted, followed to its close, canceled when it outlives its
//! patience, and journaled, and the fills of the orders that closed are handed back for the book.

use std::time::Duration;

use chrono::Utc;
use tokio::time::Instant;

use crate::broker::Broker;
use crate::broker::alpaca::BrokerError;
use crate::common::book::{Book, Fill};
use crate::common::guard::{GuardCause, guard};
use crate::common::journal::Observation;
use crate::common::market::Symbol;
use crate::common::order::{
    ClientOrderId, OrderClosed, OrderExecution, OrderRefused, OrderRequest, OrderState,
    OrderSubmitted, OrderUnresolved,
};
use crate::common::strategy::{Target, orders};
use crate::ingest::FetchError;
use crate::journal::Journal;

/// How often an order is read back, and how long it may stay open before it is canceled.
#[derive(Debug, Clone, Copy)]
pub struct Patience {
    pub poll: Duration,
    pub open_for: Duration,
}

/// Reads after a cancel before an order still open is left unresolved.
const READS_AFTER_CANCEL: u32 = 20;

/// Where an open order's wait stands: before its cancel, or some reads after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Waiting {
    BeforeCancel,
    AfterCancel { reads: u32 },
}

/// What to do before the next read of an open order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Read,
    CancelThenRead,
}

impl Waiting {
    /// The next action and wait once patience has or has not run out; `None` once the reads after a cancel are spent.
    fn next(self, patience_spent: bool) -> Option<(Action, Self)> {
        match (self, patience_spent) {
            (Self::BeforeCancel, false) => Some((Action::Read, self)),
            (Self::BeforeCancel, true) => {
                Some((Action::CancelThenRead, Self::AfterCancel { reads: 1 }))
            }
            (Self::AfterCancel { reads }, true | false) if reads >= READS_AFTER_CANCEL => None,
            (Self::AfterCancel { reads }, true | false) => {
                Some((Action::Read, Self::AfterCancel { reads: reads + 1 }))
            }
        }
    }
}

/// What became of each order: held back by the guard, closed (its fill, if it executed), refused by the broker, or
/// unresolved with the last execution read before its end was lost.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderOutcome {
    Guarded(GuardCause),
    Closed(Option<Fill>),
    Refused,
    Unresolved(Option<OrderExecution>),
}

/// Execution stopped because the journal refused a write; `outcomes` holds every order already followed, the last
/// possibly closed at the broker with its close unrecorded.
#[derive(Debug)]
pub struct JournalFailed {
    pub outcomes: Vec<OrderOutcome>,
    pub error: std::io::Error,
}

/// Sends the orders that take `book` to `target` that the broker's tradability vouches for, one at a time, sells
/// first, journaling each under the journal's run, the held-back ones first, and stops after an unresolved order so none
/// overlaps it; `next_sequence` is advanced past every id drawn, so a later call on the same counter cannot repeat one.
/// A failed tradability read vouches for nothing, so every order is held as unread.
pub async fn execute(
    broker: &impl Broker,
    journal: &mut Journal,
    next_sequence: &mut u32,
    book: &Book,
    target: &Target,
    patience: Patience,
) -> Result<Vec<OrderOutcome>, JournalFailed> {
    let mut outcomes = Vec::new();
    let orders = orders(book, target);
    let symbols: Vec<Symbol> = orders.iter().map(|order| order.symbol().clone()).collect();
    let tradability = broker.tradability(&symbols).await.unwrap_or_default();
    let guarded = guard(orders, &tradability);
    for held in guarded.held() {
        outcomes.push(OrderOutcome::Guarded(held.cause()));
        if let Err(error) = journal.append(Utc::now(), Observation::OrderGuarded(held.clone())) {
            return Err(JournalFailed { outcomes, error });
        }
    }
    for order in guarded.passed().iter().cloned() {
        let sequence = *next_sequence;
        *next_sequence = sequence
            .checked_add(1)
            .expect("a run sends fewer than u32::MAX orders");
        let request = OrderRequest::new(order, ClientOrderId::new(journal.run_id(), sequence));
        if let Err(error) = journal.append(
            Utc::now(),
            Observation::OrderSubmitted(OrderSubmitted::of(&request)),
        ) {
            return Err(JournalFailed { outcomes, error });
        }
        let (observation, outcome) = follow(broker, &request, patience).await;
        let stop = match outcome {
            OrderOutcome::Guarded(_) | OrderOutcome::Closed(_) | OrderOutcome::Refused => false,
            OrderOutcome::Unresolved(_) => true,
        };
        outcomes.push(outcome);
        if let Err(error) = journal.append(Utc::now(), observation) {
            return Err(JournalFailed { outcomes, error });
        }
        if stop {
            break;
        }
    }
    Ok(outcomes)
}

/// Submits one order and follows it to its close, returning what to journal and its outcome. Once the order is found,
/// a failed read, failed cancel or refused report spends its patience, so it is canceled and read back.
async fn follow(
    broker: &impl Broker,
    request: &OrderRequest,
    patience: Patience,
) -> (Observation, OrderOutcome) {
    let id = request.client_order_id();
    let unresolved = |cause: String, executed: Option<OrderExecution>| {
        (
            Observation::OrderUnresolved(OrderUnresolved::new(id, cause, executed)),
            OrderOutcome::Unresolved(executed),
        )
    };
    // The order may be working from the moment it is sent, so its patience runs from then.
    let started = Instant::now();
    let submitted = match broker.submit(request).await {
        Ok(order) => order,
        Err(BrokerError::Fetch(FetchError::Refused { status, body })) => {
            return (
                Observation::OrderRefused(OrderRefused::new(id, status, body)),
                OrderOutcome::Refused,
            );
        }
        // Anything short of a refusal may have landed, so the order is read back by its id rather than resent.
        Err(
            BrokerError::NotPaper
            | BrokerError::Unanswered { .. }
            | BrokerError::Fetch(FetchError::Exhausted { .. } | FetchError::Malformed { .. })
            | BrokerError::Malformed { .. }
            | BrokerError::UnknownStatus { .. },
        ) => match broker.order(id).await {
            Ok(order) => order,
            Err(error) => return unresolved(format!("submitted, then unreadable: {error}"), None),
        },
    };
    let order = request.order();
    let mut state = OrderState::submitted();
    let mut trouble = None;
    let mut report = Some(submitted.report());
    let mut waiting = Waiting::BeforeCancel;
    loop {
        if let Some(report) = report.take() {
            match state.observe(order, report) {
                Ok(next) => state = next,
                Err(refusal) => trouble = Some(format!("{refusal:?}")),
            }
        }
        if state.closed().is_some() {
            break;
        }
        let action;
        (action, waiting) = match waiting
            .next(trouble.is_some() || started.elapsed() >= patience.open_for)
        {
            Some(next) => next,
            None => {
                let trouble = trouble.map_or(String::new(), |trouble| format!(", last {trouble}"));
                return unresolved(
                    format!("open after {READS_AFTER_CANCEL} reads past a cancel{trouble}"),
                    state.executed(),
                );
            }
        };
        match action {
            Action::Read => {}
            Action::CancelThenRead => {
                if let Err(error) = broker.cancel(submitted.id()).await {
                    trouble = Some(format!("cancel failed: {error}"));
                }
            }
        }
        tokio::time::sleep(patience.poll).await;
        // A cancel can race a fill, so the order is always read back rather than assumed canceled.
        match broker.order(id).await {
            Ok(read) => report = Some(read.report()),
            Err(error) => trouble = Some(format!("unreadable: {error}")),
        }
    }
    let closed = OrderClosed::of(id, state).expect("the loop leaves only a closed order");
    (
        Observation::OrderClosed(closed),
        OrderOutcome::Closed(state.fill(order)),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::path::Path;
    use std::sync::Mutex;

    use uuid::Uuid;

    use super::*;
    use crate::broker::alpaca::{BrokerOrder, BrokerOrderId, Cancel, PaperAccount};
    use crate::common::guard::Tradability;
    use crate::common::journal::{ReadLine, RunId, read};
    use crate::common::market::{Price, Shares, Symbol};
    use crate::common::order::{OrderEnding, OrderReport, OrderStatus};
    use crate::ingest::alpaca::Alpaca;

    /// One scripted answer from the broker: where the order stands with the whole shares executed, or a failure.
    #[derive(Debug, Clone, Copy)]
    enum Answer {
        Stands(OrderStatus, u64),
        Unanswered,
        Refused,
        Unreadable,
    }

    /// A broker that answers each submit and read from a script, repeating its last read, and logs every call.
    struct Scripted {
        submits: Mutex<VecDeque<Answer>>,
        reads: Mutex<VecDeque<Answer>>,
        cancel_fails: bool,
        submit_takes: Duration,
        /// The tradability reported, every symbol tradable in any amount when `None`.
        readings: Option<BTreeMap<Symbol, Tradability>>,
        tradability_fails: bool,
        calls: Mutex<Vec<&'static str>>,
    }

    impl Scripted {
        fn new(submits: &[Answer], reads: &[Answer]) -> Self {
            Self {
                submits: Mutex::new(submits.iter().copied().collect()),
                reads: Mutex::new(reads.iter().copied().collect()),
                cancel_fails: false,
                submit_takes: Duration::ZERO,
                readings: None,
                tradability_fails: false,
                calls: Mutex::new(Vec::new()),
            }
        }

        /// The order calls made, leaving out the tradability read that precedes them.
        fn calls(&self) -> Vec<&'static str> {
            let calls = self.calls.lock().unwrap();
            calls
                .iter()
                .copied()
                .filter(|call| *call != "tradability")
                .collect()
        }

        fn answer(answer: Answer) -> Result<BrokerOrder, BrokerError> {
            match answer {
                Answer::Stands(status, whole) => Ok(BrokerOrder::new(
                    BrokerOrderId::new("broker-1".to_string()),
                    OrderReport::new(
                        status,
                        OrderExecution::new(
                            Shares::whole(whole).unwrap(),
                            Price::from_ticks(100_000_000).unwrap(),
                        ),
                        "2026-10-06T14:00:00Z".parse().unwrap(),
                    ),
                )),
                Answer::Unanswered => Err(BrokerError::Unanswered {
                    cause: "timed out".to_string(),
                }),
                Answer::Refused => Err(BrokerError::Fetch(FetchError::Refused {
                    status: 403,
                    body: "insufficient buying power".to_string(),
                })),
                Answer::Unreadable => Err(BrokerError::Malformed {
                    field: "status",
                    raw: String::new(),
                }),
            }
        }
    }

    impl Broker for Scripted {
        async fn submit(&self, _: &OrderRequest) -> Result<BrokerOrder, BrokerError> {
            self.calls.lock().unwrap().push("submit");
            tokio::time::sleep(self.submit_takes).await;
            Self::answer(self.submits.lock().unwrap().pop_front().unwrap())
        }

        async fn order(&self, _: ClientOrderId) -> Result<BrokerOrder, BrokerError> {
            self.calls.lock().unwrap().push("order");
            let mut reads = self.reads.lock().unwrap();
            let answer = match reads.len() {
                0 => panic!("the script has no read"),
                1 => reads[0],
                _ => reads.pop_front().unwrap(),
            };
            Self::answer(answer)
        }

        async fn cancel(&self, _: &BrokerOrderId) -> Result<Cancel, BrokerError> {
            self.calls.lock().unwrap().push("cancel");
            match self.cancel_fails {
                true => Err(BrokerError::Fetch(FetchError::Exhausted {
                    attempts: 3,
                    last: "status 503".to_string(),
                })),
                false => Ok(Cancel::Requested),
            }
        }

        async fn tradability(
            &self,
            symbols: &[Symbol],
        ) -> Result<BTreeMap<Symbol, Tradability>, BrokerError> {
            self.calls.lock().unwrap().push("tradability");
            if self.tradability_fails {
                return Err(BrokerError::Unanswered {
                    cause: "timed out".to_string(),
                });
            }
            let reading = |symbol: &Symbol| {
                self.readings
                    .as_ref()
                    .map_or(Tradability::Fractionable, |readings| readings[symbol])
            };
            Ok(symbols
                .iter()
                .map(|symbol| (symbol.clone(), reading(symbol)))
                .collect())
        }
    }

    const OPEN: OrderStatus = OrderStatus::Open;
    const FILLED: OrderStatus = OrderStatus::Closed(OrderEnding::Filled);
    const CANCELED: OrderStatus = OrderStatus::Closed(OrderEnding::Canceled);

    const PATIENT: Patience = Patience {
        poll: Duration::from_secs(1),
        open_for: Duration::from_secs(3_600),
    };

    /// Buys of `whole` shares of each symbol from an empty book.
    fn buying(symbols: &[&str], whole: u64) -> Target {
        Target::new(
            symbols
                .iter()
                .map(|symbol| (Symbol::new(symbol).unwrap(), Shares::whole(whole).unwrap()))
                .collect(),
        )
    }

    /// Runs `execute` against `broker` into a fresh journal, returning its outcomes and the event types journaled.
    async fn run(
        broker: &Scripted,
        target: &Target,
        patience: Patience,
    ) -> (Vec<OrderOutcome>, Vec<&'static str>) {
        let directory = std::env::temp_dir().join(format!("fund-execution-{}", Uuid::new_v4()));
        let mut journal = Journal::open(&directory, RunId::new(Uuid::new_v4())).unwrap();
        let mut next_sequence = 0;
        let outcomes = execute(
            broker,
            &mut journal,
            &mut next_sequence,
            &Book::default(),
            target,
            patience,
        )
        .await
        .unwrap();
        let events = journaled(&directory);
        std::fs::remove_dir_all(&directory).unwrap();
        (outcomes, events)
    }

    fn journal_lines(directory: &Path) -> Vec<ReadLine> {
        let file = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        read(&std::fs::read_to_string(&file).unwrap())
    }

    fn journaled(directory: &Path) -> Vec<&'static str> {
        journal_lines(directory)
            .iter()
            .map(|line| match line {
                ReadLine::Read(record) => record.observation().event_type(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect()
    }

    fn shares_filled(outcome: &OrderOutcome) -> Option<u64> {
        match outcome {
            OrderOutcome::Closed(fill) => {
                fill.as_ref().map(|fill| fill.shares().units() / 1_000_000)
            }
            OrderOutcome::Guarded(_) | OrderOutcome::Refused | OrderOutcome::Unresolved(_) => None,
        }
    }

    /// An open order is read until its patience runs out, canceled once, then read exactly `READS_AFTER_CANCEL` more
    /// times before it is given up on, whatever the patience says after the cancel.
    #[test]
    fn test_an_open_order_is_canceled_once_then_read_a_bounded_number_of_times() {
        assert_eq!(
            Waiting::BeforeCancel.next(false),
            Some((Action::Read, Waiting::BeforeCancel))
        );
        let mut waiting = Waiting::BeforeCancel;
        let mut actions = Vec::new();
        while let Some((action, next)) = waiting.next(true) {
            actions.push(action);
            waiting = next;
        }
        assert_eq!(actions.len(), 20);
        assert_eq!(actions[0], Action::CancelThenRead);
        assert!(actions[1..].iter().all(|action| *action == Action::Read));
        assert_eq!(waiting, Waiting::AfterCancel { reads: 20 });
        assert_eq!(
            Waiting::AfterCancel { reads: 3 }.next(false),
            Some((Action::Read, Waiting::AfterCancel { reads: 4 }))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_an_order_filled_on_a_read_closes_with_its_fill() {
        let broker = Scripted::new(&[Answer::Stands(OPEN, 0)], &[Answer::Stands(FILLED, 2)]);
        let (outcomes, events) = run(&broker, &buying(&["SPY"], 2), PATIENT).await;
        assert_eq!(
            outcomes.iter().map(shares_filled).collect::<Vec<_>>(),
            [Some(2)]
        );
        assert_eq!(broker.calls(), ["submit", "order"]);
        assert_eq!(events, ["order_submitted", "order_closed"]);
    }

    /// A refusal is the broker's answer, so the next order still goes out.
    #[tokio::test(start_paused = true)]
    async fn test_a_refused_order_is_journaled_and_the_next_one_sent() {
        let broker = Scripted::new(&[Answer::Refused, Answer::Refused], &[]);
        let (outcomes, events) = run(&broker, &buying(&["AAPL", "SPY"], 1), PATIENT).await;
        assert_eq!(outcomes, [OrderOutcome::Refused, OrderOutcome::Refused]);
        assert_eq!(broker.calls(), ["submit", "submit"]);
        assert_eq!(
            events,
            [
                "order_submitted",
                "order_refused",
                "order_submitted",
                "order_refused"
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_an_unanswered_submission_is_read_back_rather_than_resent() {
        let broker = Scripted::new(&[Answer::Unanswered], &[Answer::Stands(FILLED, 1)]);
        let (outcomes, events) = run(&broker, &buying(&["SPY"], 1), PATIENT).await;
        assert_eq!(
            outcomes.iter().map(shares_filled).collect::<Vec<_>>(),
            [Some(1)]
        );
        assert_eq!(broker.calls(), ["submit", "order"]);
        assert_eq!(events, ["order_submitted", "order_closed"]);
    }

    /// An order that may be working and cannot be found stops the run, so no later order overlaps it.
    #[tokio::test(start_paused = true)]
    async fn test_an_unanswered_submission_that_cannot_be_read_stops_the_run() {
        let broker = Scripted::new(
            &[Answer::Unanswered, Answer::Unanswered],
            &[Answer::Unreadable],
        );
        let (outcomes, events) = run(&broker, &buying(&["AAPL", "SPY"], 1), PATIENT).await;
        assert_eq!(outcomes, [OrderOutcome::Unresolved(None)]);
        assert_eq!(broker.calls(), ["submit", "order"]);
        assert_eq!(events, ["order_submitted", "order_unresolved"]);
    }

    /// Reads at one, two and three seconds, then a cancel, and the part executed before the cancel took is the fill.
    #[tokio::test(start_paused = true)]
    async fn test_an_order_past_its_patience_is_canceled_and_keeps_its_partial_fill() {
        let broker = Scripted::new(
            &[Answer::Stands(OPEN, 0)],
            &[
                Answer::Stands(OPEN, 0),
                Answer::Stands(OPEN, 1),
                Answer::Stands(OPEN, 1),
                Answer::Stands(CANCELED, 1),
            ],
        );
        let patience = Patience {
            poll: Duration::from_secs(1),
            open_for: Duration::from_secs(3),
        };
        let (outcomes, events) = run(&broker, &buying(&["SPY"], 2), patience).await;
        assert_eq!(
            outcomes.iter().map(shares_filled).collect::<Vec<_>>(),
            [Some(1)]
        );
        assert_eq!(
            broker.calls(),
            ["submit", "order", "order", "order", "cancel", "order"]
        );
        assert_eq!(events, ["order_submitted", "order_closed"]);
    }

    /// Patience runs from before the submit, so a submit that takes two of three seconds leaves one read before the cancel.
    #[tokio::test(start_paused = true)]
    async fn test_patience_counts_the_time_the_submit_took() {
        let mut broker = Scripted::new(
            &[Answer::Stands(OPEN, 0)],
            &[Answer::Stands(OPEN, 0), Answer::Stands(CANCELED, 0)],
        );
        broker.submit_takes = Duration::from_secs(2);
        let patience = Patience {
            poll: Duration::from_secs(1),
            open_for: Duration::from_secs(3),
        };
        let (outcomes, _) = run(&broker, &buying(&["SPY"], 1), patience).await;
        assert_eq!(outcomes, [OrderOutcome::Closed(None)]);
        assert_eq!(broker.calls(), ["submit", "order", "cancel", "order"]);
    }

    /// A report the order's state refuses, here an execution that shrank, spends its patience like a failed read.
    #[tokio::test(start_paused = true)]
    async fn test_a_refused_report_cancels_the_order_before_its_patience_runs_out() {
        let broker = Scripted::new(
            &[Answer::Stands(OPEN, 1)],
            &[Answer::Stands(OPEN, 0), Answer::Stands(CANCELED, 1)],
        );
        let (outcomes, _) = run(&broker, &buying(&["SPY"], 2), PATIENT).await;
        assert_eq!(
            outcomes.iter().map(shares_filled).collect::<Vec<_>>(),
            [Some(1)]
        );
        assert_eq!(broker.calls(), ["submit", "order", "cancel", "order"]);
    }

    /// A cancel whose answer is lost may still have landed, and the order may have filled, so it is read back.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_cancel_is_followed_by_a_read() {
        let mut broker = Scripted::new(&[Answer::Stands(OPEN, 0)], &[Answer::Stands(FILLED, 1)]);
        broker.cancel_fails = true;
        let patience = Patience {
            poll: Duration::from_secs(1),
            open_for: Duration::ZERO,
        };
        let (outcomes, _) = run(&broker, &buying(&["SPY"], 1), patience).await;
        assert_eq!(
            outcomes.iter().map(shares_filled).collect::<Vec<_>>(),
            [Some(1)]
        );
        assert_eq!(broker.calls(), ["submit", "cancel", "order"]);
    }

    /// A failed read spends the order's patience at once: it is canceled and read back, not abandoned working.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_read_cancels_the_order_before_its_patience_runs_out() {
        let broker = Scripted::new(
            &[Answer::Stands(OPEN, 0)],
            &[Answer::Unreadable, Answer::Stands(CANCELED, 0)],
        );
        let (outcomes, events) = run(&broker, &buying(&["SPY"], 1), PATIENT).await;
        assert_eq!(outcomes, [OrderOutcome::Closed(None)]);
        assert_eq!(broker.calls(), ["submit", "order", "cancel", "order"]);
        assert_eq!(events, ["order_submitted", "order_closed"]);
    }

    /// An order that never closes is canceled once, read twenty times, left unresolved with what it executed, and
    /// stops the run.
    #[tokio::test(start_paused = true)]
    async fn test_an_order_that_never_closes_is_unresolved_and_stops_the_run() {
        let broker = Scripted::new(&[Answer::Stands(OPEN, 1)], &[Answer::Stands(OPEN, 1)]);
        let patience = Patience {
            poll: Duration::from_secs(1),
            open_for: Duration::ZERO,
        };
        let (outcomes, events) = run(&broker, &buying(&["AAPL", "SPY"], 2), patience).await;
        assert!(matches!(
            outcomes.as_slice(),
            [OrderOutcome::Unresolved(Some(execution))] if execution.shares() == Shares::whole(1).unwrap()
        ));
        let calls = broker.calls();
        assert_eq!(
            ["submit", "cancel", "order"]
                .map(|call| calls.iter().filter(|made| **made == call).count()),
            [1, 1, 20]
        );
        assert_eq!(events, ["order_submitted", "order_unresolved"]);
    }

    /// The guard's holds are journaled before any order goes out, and only the vouched-for order is sent.
    #[tokio::test(start_paused = true)]
    async fn test_a_guarded_order_is_journaled_and_never_sent() {
        let broker = Scripted::new(&[Answer::Stands(FILLED, 1)], &[]);
        let directory = std::env::temp_dir().join(format!("fund-execution-{}", Uuid::new_v4()));
        let mut journal = Journal::open(&directory, RunId::new(Uuid::new_v4())).unwrap();
        let mut broker = broker;
        broker.readings = Some(BTreeMap::from([
            (Symbol::new("AAPL").unwrap(), Tradability::Untradable),
            (Symbol::new("SPY").unwrap(), Tradability::Fractionable),
        ]));
        let mut next_sequence = 0;
        let outcomes = execute(
            &broker,
            &mut journal,
            &mut next_sequence,
            &Book::default(),
            &buying(&["AAPL", "SPY"], 1),
            PATIENT,
        )
        .await
        .unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0], OrderOutcome::Guarded(GuardCause::Untradable));
        assert_eq!(shares_filled(&outcomes[1]), Some(1));
        assert_eq!(broker.calls(), ["submit"]);
        assert_eq!(next_sequence, 1);
        assert_eq!(
            journaled(&directory),
            ["order_guarded", "order_submitted", "order_closed"]
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A tradability read that fails vouches for nothing, so every order is held as unread and none is sent.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_tradability_read_holds_every_order() {
        let mut broker = Scripted::new(&[], &[]);
        broker.tradability_fails = true;
        let (outcomes, events) = run(&broker, &buying(&["AAPL", "SPY"], 1), PATIENT).await;
        assert_eq!(
            outcomes,
            [
                OrderOutcome::Guarded(GuardCause::Unread),
                OrderOutcome::Guarded(GuardCause::Unread)
            ]
        );
        assert_eq!(broker.calls(), Vec::<&str>::new());
        assert_eq!(events, ["order_guarded", "order_guarded"]);
    }

    /// A journal that cannot record a submission stops the run before anything is sent.
    #[tokio::test(start_paused = true)]
    async fn test_a_journal_that_refuses_a_write_stops_the_run_before_sending() {
        let broker = Scripted::new(&[Answer::Stands(FILLED, 1)], &[]);
        let directory = std::env::temp_dir().join(format!("fund-execution-{}", Uuid::new_v4()));
        let mut journal = Journal::open(&directory, RunId::new(Uuid::new_v4())).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        let mut next_sequence = 0;
        let halted = execute(
            &broker,
            &mut journal,
            &mut next_sequence,
            &Book::default(),
            &buying(&["SPY"], 1),
            PATIENT,
        )
        .await
        .unwrap_err();
        assert!(halted.outcomes.is_empty());
        assert!(broker.calls().is_empty());
    }

    /// While the market is closed, an order to buy one SPY share stays open past its patience, is canceled, and closes
    /// unfilled: the journal holds its submission and its close, and the paper account's book is unchanged.
    #[tokio::test]
    #[ignore = "trades on the Alpaca paper account; run deliberately under a development secretspec profile"]
    async fn live_an_order_open_past_its_patience_is_canceled_and_journaled() {
        let account =
            PaperAccount::new(Alpaca::from_environment(reqwest::Client::new()).unwrap()).unwrap();
        let before = account.book().await.unwrap();
        // Every other holding is kept, so the run sends only the one SPY buy.
        let mut holdings: BTreeMap<Symbol, Shares> = before
            .positions()
            .iter()
            .map(|(symbol, position)| {
                let units =
                    u64::try_from(position.units()).expect("the paper account holds no short");
                (symbol.clone(), Shares::from_units(units))
            })
            .collect();
        let spy = Symbol::new("SPY").unwrap();
        let held = holdings.get(&spy).copied().unwrap_or_default();
        holdings.insert(spy, held.plus(Shares::whole(1).unwrap()));
        let target = Target::new(holdings);
        let directory = std::env::temp_dir().join(format!("fund-execution-{}", Uuid::new_v4()));
        let mut journal = Journal::open(&directory, RunId::new(Uuid::new_v4())).unwrap();
        let patience = Patience {
            poll: Duration::from_millis(500),
            open_for: Duration::from_secs(2),
        };
        let mut next_sequence = 0;
        let outcomes = execute(
            &account,
            &mut journal,
            &mut next_sequence,
            &before,
            &target,
            patience,
        )
        .await
        .unwrap();
        assert_eq!(next_sequence, 1);
        assert_eq!(
            outcomes,
            [OrderOutcome::Closed(None)],
            "run while the market is closed"
        );
        assert_eq!(journaled(&directory), ["order_submitted", "order_closed"]);
        let closed = journal_lines(&directory)
            .into_iter()
            .find_map(|line| match line {
                ReadLine::Read(record) => match record.observation() {
                    Observation::OrderClosed(closed) => Some(closed.clone()),
                    Observation::ConfigurationResolved(_)
                    | Observation::PartitionWritten(_)
                    | Observation::HealFinished(_)
                    | Observation::DatasetRead(_)
                    | Observation::ExperimentRan(_)
                    | Observation::OrderSubmitted(_)
                    | Observation::OrderRefused(_)
                    | Observation::OrderUnresolved(_)
                    | Observation::OrderGuarded(_) => None,
                },
                ReadLine::Unreadable { .. } => None,
            })
            .unwrap();
        assert_eq!(
            serde_json::to_value(&closed).unwrap()["ending"],
            OrderEnding::Canceled.to_string()
        );
        assert_eq!(account.book().await.unwrap(), before);
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
