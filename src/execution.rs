//! Takes a book to a target on the paper account: each order is submitted, followed to its close, canceled when it
//! outlives its patience, and journaled, and the fills of the orders that closed are handed back for the book.

use std::time::Duration;

use chrono::Utc;
use tokio::time::Instant;

use crate::broker::alpaca::{BrokerError, PaperAccount};
use crate::common::book::{Book, Fill};
use crate::common::journal::Observation;
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

/// What became of each order: closed (its fill, if it executed), refused by the broker, or unresolved with the last
/// execution read before its end was lost.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderOutcome {
    Closed(Option<Fill>),
    Refused,
    Unresolved(Option<OrderExecution>),
}

/// Sends the orders that take `book` to `target`, one at a time, sells first, journaling each under the journal's
/// run; `next_sequence` is advanced past every id drawn, so a later call on the same counter cannot repeat one.
pub async fn execute(
    account: &PaperAccount,
    journal: &mut Journal,
    next_sequence: &mut u32,
    book: &Book,
    target: &Target,
    patience: Patience,
) -> std::io::Result<Vec<OrderOutcome>> {
    let mut outcomes = Vec::new();
    for order in orders(book, target) {
        let sequence = *next_sequence;
        *next_sequence = sequence
            .checked_add(1)
            .expect("a run sends fewer than u32::MAX orders");
        let request = OrderRequest::new(order, ClientOrderId::new(journal.run_id(), sequence));
        journal.append(
            Utc::now(),
            Observation::OrderSubmitted(OrderSubmitted::of(&request)),
        )?;
        let (observation, outcome) = follow(account, &request, patience).await;
        journal.append(Utc::now(), observation)?;
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

/// Submits one order and follows it to its close, returning what to journal and its outcome.
async fn follow(
    account: &PaperAccount,
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
    let submitted = match account.submit(request).await {
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
        ) => match account.order(id).await {
            Ok(order) => order,
            Err(error) => return unresolved(format!("submitted, then unreadable: {error}"), None),
        },
    };
    let order = request.order();
    let mut state = match OrderState::submitted().observe(order, submitted.report()) {
        Ok(state) => state,
        Err(refusal) => return unresolved(format!("{refusal:?}"), None),
    };
    let started = Instant::now();
    let mut waiting = Waiting::BeforeCancel;
    while state.closed().is_none() {
        let action;
        (action, waiting) = match waiting.next(started.elapsed() >= patience.open_for) {
            Some(next) => next,
            None => {
                return unresolved(
                    format!("open after {READS_AFTER_CANCEL} reads past a cancel"),
                    state.executed(),
                );
            }
        };
        match action {
            Action::Read => {}
            Action::CancelThenRead => {
                if let Err(error) = account.cancel(submitted.id()).await {
                    return unresolved(format!("cancel failed: {error}"), state.executed());
                }
            }
        }
        tokio::time::sleep(patience.poll).await;
        // A cancel can race a fill, so the order is always read back rather than assumed canceled.
        let report = match account.order(id).await {
            Ok(order) => order.report(),
            Err(error) => return unresolved(format!("unreadable: {error}"), state.executed()),
        };
        state = match state.observe(order, report) {
            Ok(state) => state,
            Err(refusal) => return unresolved(format!("{refusal:?}"), state.executed()),
        };
    }
    let closed = OrderClosed::of(id, state).expect("the loop leaves only a closed order");
    (
        Observation::OrderClosed(closed),
        OrderOutcome::Closed(state.fill(order)),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use super::*;
    use crate::common::journal::{ReadLine, RunId, read};
    use crate::common::market::{Shares, Symbol};
    use crate::common::order::OrderEnding;
    use crate::ingest::alpaca::Alpaca;

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

    /// While the market is closed, an order to buy one SPY share stays open past its patience, is canceled, and closes
    /// unfilled: the journal holds its submission and its close, and the paper account's book is unchanged.
    #[tokio::test]
    #[ignore = "trades on the Alpaca paper account; run deliberately under a development secretspec profile"]
    async fn live_an_order_open_past_its_patience_is_canceled_and_journaled() {
        let account =
            PaperAccount::new(Alpaca::from_environment(reqwest::Client::new()).unwrap()).unwrap();
        let before = account.book().await.unwrap();
        let spy = Symbol::new("SPY").unwrap();
        let held = Shares::from_units(u64::try_from(before.position(&spy).units()).unwrap());
        let target = Target::new(BTreeMap::from([(
            spy,
            held.plus(Shares::whole(1).unwrap()),
        )]));
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
        let file = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        let events: Vec<&'static str> = read(&std::fs::read_to_string(&file).unwrap())
            .iter()
            .map(|line| match line {
                ReadLine::Read(record) => record.observation().event_type(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect();
        assert_eq!(events, ["order_submitted", "order_closed"]);
        let closed = read(&std::fs::read_to_string(&file).unwrap())
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
                    | Observation::OrderUnresolved(_) => None,
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
