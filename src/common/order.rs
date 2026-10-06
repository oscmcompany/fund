//! An order's life at the broker: the identifier it is sent under, the reports read back about it, and the state
//! those reports fold into, which ends at most once and yields the fill the book takes.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::common::book::Fill;
use crate::common::journal::RunId;
use crate::common::market::{DollarVolume, Price, Shares};
use crate::common::strategy::Order;

/// Alpaca's limit, measured against the paper account on 2026-09-25: 128 characters accepted, 129 refused.
pub const CLIENT_ORDER_ID_MAXIMUM_LENGTH: usize = 128;

const CLIENT_ORDER_ID_PREFIX: &str = "fund";

/// The identifier an order is sent under: the run that sent it and its place in that run, so the broker's order
/// history names the journal that explains each order and a resubmission is refused as a duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientOrderId {
    run: RunId,
    sequence: u32,
}

/// The longest identifier the format writes, a hyphenated UUID and a ten-digit sequence, fits the broker's limit.
const _: () = assert!(
    CLIENT_ORDER_ID_PREFIX.len() + ":".len() + 36 + ":".len() + 10
        <= CLIENT_ORDER_ID_MAXIMUM_LENGTH
);

impl ClientOrderId {
    pub fn new(run: RunId, sequence: u32) -> Self {
        Self { run, sequence }
    }

    /// Reads an identifier back, `None` for any string other than exactly what `Display` writes, so another spelling
    /// of one of ours is not ours.
    pub fn parse(raw: &str) -> Option<Self> {
        let mut fields = raw.split(':');
        let (Some(CLIENT_ORDER_ID_PREFIX), Some(run), Some(sequence), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            return None;
        };
        let parsed = Self {
            run: RunId::new(Uuid::parse_str(run).ok()?),
            sequence: sequence.parse().ok()?,
        };
        (parsed.to_string() == raw).then_some(parsed)
    }

    pub fn run(self) -> RunId {
        self.run
    }

    pub fn sequence(self) -> u32 {
        self.sequence
    }
}

impl std::fmt::Display for ClientOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{CLIENT_ORDER_ID_PREFIX}:{}:{}",
            self.run, self.sequence
        )
    }
}

/// Where the broker says an order stands, collapsed to what a caller acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    /// Accepted, queued or partly filled: the broker still has it.
    Open,
    Closed(OrderEnding),
}

/// How a closed order ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderEnding {
    Filled,
    Canceled,
    /// Ended by its time in force, or done for the day.
    Expired,
    Rejected,
}

/// What has executed: the shares and the broker's average price over them, never zero shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderExecution {
    shares: Shares,
    average_price: Price,
}

impl OrderExecution {
    /// `None` for zero shares, which is no execution rather than one of nothing.
    pub fn new(shares: Shares, average_price: Price) -> Option<Self> {
        (!shares.is_zero()).then_some(Self {
            shares,
            average_price,
        })
    }

    pub fn shares(self) -> Shares {
        self.shares
    }

    pub fn average_price(self) -> Price {
        self.average_price
    }
}

/// One reading of an order at the broker; `executed` is `None` while nothing has filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderReport {
    status: OrderStatus,
    executed: Option<OrderExecution>,
    at: DateTime<Utc>,
}

impl OrderReport {
    pub fn new(status: OrderStatus, executed: Option<OrderExecution>, at: DateTime<Utc>) -> Self {
        Self {
            status,
            executed,
            at,
        }
    }

    pub fn status(self) -> OrderStatus {
        self.status
    }
}

/// Where an order stands, folded from its reports in the order they were read; only `submitted` and `observe` make
/// one, so a filled order holds every share of the order it observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderState(Stage);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Working {
        executed: Option<OrderExecution>,
    },
    /// Closed at `at`; a canceled, expired or rejected order may still have executed part of itself.
    Closed {
        ending: OrderEnding,
        executed: Option<OrderExecution>,
        at: DateTime<Utc>,
    },
}

/// Why a report could not follow the state before it, with the readings that disagreed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderRefusal {
    /// The broker reported fewer shares executed than an earlier reading had.
    ExecutionShrank { held: Shares, reported: Shares },
    /// The broker reported more shares executed than were ordered.
    Overfilled { ordered: Shares, reported: Shares },
    /// The broker reported the order filled short of what was ordered.
    FilledShort { ordered: Shares, reported: Shares },
    /// A closed order was reported differently afterwards: how it closed, beside the report that disagreed.
    ChangedAfterClosing {
        ending: OrderEnding,
        executed: Option<OrderExecution>,
        report: OrderReport,
    },
}

impl OrderState {
    /// A submitted order before any report.
    pub fn submitted() -> Self {
        Self(Stage::Working { executed: None })
    }

    pub fn executed(self) -> Option<OrderExecution> {
        match self.0 {
            Stage::Working { executed } | Stage::Closed { executed, .. } => executed,
        }
    }

    /// How and when the order closed, `None` while it is working.
    pub fn closed(self) -> Option<(OrderEnding, DateTime<Utc>)> {
        match self.0 {
            Stage::Working { .. } => None,
            Stage::Closed { ending, at, .. } => Some((ending, at)),
        }
    }

    /// The state after `report` on `order`; a repeated report of a closed order changes nothing, its time included.
    pub fn observe(self, order: &Order, report: OrderReport) -> Result<Self, OrderRefusal> {
        let ordered = order.shares();
        let held = self
            .executed()
            .map_or(Shares::default(), OrderExecution::shares);
        let reported = report
            .executed
            .map_or(Shares::default(), OrderExecution::shares);
        if reported > ordered {
            return Err(OrderRefusal::Overfilled { ordered, reported });
        }
        let ending = match report.status {
            OrderStatus::Open => None,
            OrderStatus::Closed(OrderEnding::Filled) if reported != ordered => {
                return Err(OrderRefusal::FilledShort { ordered, reported });
            }
            OrderStatus::Closed(ending) => Some(ending),
        };
        match (self.0, ending) {
            (
                Stage::Closed {
                    ending: closed,
                    executed,
                    ..
                },
                Some(ending),
            ) if ending == closed && report.executed == executed => Ok(self),
            (
                Stage::Closed {
                    ending: closed,
                    executed,
                    ..
                },
                Some(_) | None,
            ) => Err(OrderRefusal::ChangedAfterClosing {
                ending: closed,
                executed,
                report,
            }),
            (Stage::Working { .. }, Some(_) | None) if reported < held => {
                Err(OrderRefusal::ExecutionShrank { held, reported })
            }
            (Stage::Working { .. }, None) => Ok(Self(Stage::Working {
                executed: report.executed,
            })),
            (Stage::Working { .. }, Some(ending)) => Ok(Self(Stage::Closed {
                ending,
                executed: report.executed,
                at: report.at,
            })),
        }
    }

    /// The fill a closed order hands the book: its executed shares at the broker's average with no separate cost, as
    /// the spread is in the price; the regulatory fees on a sale are not modeled and reach only the broker's cash.
    /// `None` while working or when nothing executed.
    pub fn fill(self, order: &Order) -> Option<Fill> {
        match self.0 {
            Stage::Working { .. } | Stage::Closed { executed: None, .. } => None,
            Stage::Closed {
                executed: Some(execution),
                at,
                ..
            } => Some(
                Fill::new(
                    at,
                    order.symbol().clone(),
                    order.side(),
                    execution.shares,
                    execution.average_price,
                    DollarVolume::default(),
                )
                .expect("a fill charged nothing costs at most its notional"),
            ),
        }
    }
}

/// An order as sent: what to trade and the identifier it goes under, filled at market for the day.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequest {
    order: Order,
    client_order_id: ClientOrderId,
}

impl OrderRequest {
    pub fn new(order: Order, client_order_id: ClientOrderId) -> Self {
        Self {
            order,
            client_order_id,
        }
    }

    pub fn order(&self) -> &Order {
        &self.order
    }

    pub fn client_order_id(&self) -> ClientOrderId {
        self.client_order_id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::common::book::{Book, Cash, Position, Side};
    use crate::common::market::Symbol;
    use crate::common::monoid::Monoid;
    use crate::common::strategy::{Target, orders};

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn instant(minute: i64) -> DateTime<Utc> {
        "2026-10-05T14:00:00Z".parse::<DateTime<Utc>>().unwrap()
            + chrono::TimeDelta::minutes(minute)
    }

    fn whole(count: u64) -> Shares {
        Shares::whole(count).unwrap()
    }

    fn executed(count: u64, ticks: i64) -> Option<OrderExecution> {
        OrderExecution::new(whole(count), Price::from_ticks(ticks).unwrap())
    }

    fn report(status: OrderStatus, execution: Option<OrderExecution>, minute: i64) -> OrderReport {
        OrderReport::new(status, execution, instant(minute))
    }

    /// A buy of `count` AAPL shares from an empty book.
    fn buy(count: u64) -> Order {
        orders(
            &Book::empty(),
            &Target::new(BTreeMap::from([(symbol("AAPL"), whole(count))])),
        )
        .remove(0)
    }

    fn observe_all(order: &Order, reports: &[OrderReport]) -> Result<OrderState, OrderRefusal> {
        reports
            .iter()
            .try_fold(OrderState::submitted(), |state, report| {
                state.observe(order, *report)
            })
    }

    #[test]
    fn test_a_client_order_id_is_read_back_only_in_the_spelling_written() {
        let run = RunId::new(Uuid::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap());
        let id = ClientOrderId::new(run, 42);
        assert_eq!(
            id.to_string(),
            "fund:67e55044-10b1-426f-9247-bb680e5fe0c8:42"
        );
        assert_eq!(ClientOrderId::parse(&id.to_string()), Some(id));
        assert_eq!((id.run(), id.sequence()), (run, 42));
        for foreign in [
            "fund:67E55044-10B1-426F-9247-BB680E5FE0C8:42",
            "fund:67e55044-10b1-426f-9247-bb680e5fe0c8:042",
            "fund:67e55044-10b1-426f-9247-bb680e5fe0c8:42:extra",
            "pair:67e55044-10b1-426f-9247-bb680e5fe0c8:42",
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
            "",
        ] {
            assert_eq!(ClientOrderId::parse(foreign), None, "{foreign}");
        }
        let longest = ClientOrderId::new(run, u32::MAX).to_string();
        assert_eq!(CLIENT_ORDER_ID_MAXIMUM_LENGTH, 128);
        assert_eq!(longest.len(), 52);
    }

    /// Partly filled, then filled: the fill takes every share at the broker's average, at the closing report's time.
    #[test]
    fn test_an_order_fills_through_its_reports() {
        let order = buy(10);
        let state = observe_all(
            &order,
            &[
                report(OrderStatus::Open, None, 0),
                report(OrderStatus::Open, executed(4, 2_000_000), 1),
                report(
                    OrderStatus::Closed(OrderEnding::Filled),
                    executed(10, 2_100_000),
                    2,
                ),
            ],
        )
        .unwrap();
        assert_eq!(state.closed(), Some((OrderEnding::Filled, instant(2))));
        assert_eq!(OrderState::submitted().closed(), None);
        let fill = state.fill(&order).unwrap();
        assert_eq!(
            (
                fill.filled_against(),
                fill.side(),
                fill.shares(),
                fill.price().ticks(),
                fill.cost()
            ),
            (
                instant(2),
                Side::Buy,
                whole(10),
                2_100_000,
                DollarVolume::default()
            )
        );
        assert_eq!(OrderState::submitted().fill(&order), None);
    }

    /// A canceled order that executed part of itself hands the book that part; one that executed nothing, nothing.
    #[test]
    fn test_a_canceled_order_hands_over_only_what_executed() {
        let order = buy(10);
        let partial = observe_all(
            &order,
            &[report(
                OrderStatus::Closed(OrderEnding::Canceled),
                executed(3, 2_000_000),
                5,
            )],
        )
        .unwrap();
        assert_eq!(
            partial.fill(&order).map(|fill| fill.shares()),
            Some(whole(3))
        );
        let none = observe_all(
            &order,
            &[report(OrderStatus::Closed(OrderEnding::Rejected), None, 5)],
        )
        .unwrap();
        assert_eq!(none.fill(&order), None);
        let reread = partial
            .observe(
                &order,
                report(
                    OrderStatus::Closed(OrderEnding::Canceled),
                    executed(3, 2_000_000),
                    9,
                ),
            )
            .unwrap();
        assert_eq!(
            reread.fill(&order).map(|fill| fill.filled_against()),
            Some(instant(5))
        );
    }

    #[test]
    fn test_an_impossible_report_is_refused_with_its_readings() {
        let order = buy(10);
        let ordered = whole(10);
        assert_eq!(
            observe_all(&order, &[report(OrderStatus::Open, executed(11, 1), 0)]),
            Err(OrderRefusal::Overfilled {
                ordered,
                reported: whole(11)
            })
        );
        assert_eq!(
            observe_all(
                &order,
                &[report(
                    OrderStatus::Closed(OrderEnding::Filled),
                    executed(9, 1),
                    0
                )]
            ),
            Err(OrderRefusal::FilledShort {
                ordered,
                reported: whole(9)
            })
        );
        assert_eq!(
            observe_all(
                &order,
                &[
                    report(OrderStatus::Open, executed(5, 1), 0),
                    report(OrderStatus::Open, executed(4, 1), 1)
                ]
            ),
            Err(OrderRefusal::ExecutionShrank {
                held: whole(5),
                reported: whole(4)
            })
        );
        assert_eq!(
            observe_all(
                &order,
                &[
                    report(
                        OrderStatus::Closed(OrderEnding::Canceled),
                        executed(2, 1),
                        0
                    ),
                    report(OrderStatus::Open, None, 1)
                ]
            ),
            Err(OrderRefusal::ChangedAfterClosing {
                ending: OrderEnding::Canceled,
                executed: executed(2, 1),
                report: report(OrderStatus::Open, None, 1)
            })
        );
        assert_eq!(
            observe_all(
                &order,
                &[
                    report(OrderStatus::Closed(OrderEnding::Canceled), None, 0),
                    report(
                        OrderStatus::Closed(OrderEnding::Canceled),
                        executed(1, 1),
                        1
                    )
                ]
            ),
            Err(OrderRefusal::ChangedAfterClosing {
                ending: OrderEnding::Canceled,
                executed: None,
                report: report(
                    OrderStatus::Closed(OrderEnding::Canceled),
                    executed(1, 1),
                    1
                )
            })
        );
    }

    /// The fills of a broker's closed orders fold into the book the broker reports, the seam reconciliation reads.
    #[test]
    fn test_closed_orders_fold_into_the_reported_book() {
        let order = buy(10);
        let filled = observe_all(
            &order,
            &[report(
                OrderStatus::Closed(OrderEnding::Filled),
                executed(10, 3_000_000),
                0,
            )],
        )
        .unwrap();
        let opening = Cash::from_units(100 * 1_000_000_000_000);
        let folded = Book::funded(opening).combine(Book::of(&filled.fill(&order).unwrap()));
        let reported = Book::reported(
            Cash::from_units(70 * 1_000_000_000_000),
            [
                (symbol("AAPL"), Position::from_units(10_000_000)),
                (symbol("MSFT"), Position::from_units(0)),
            ],
        );
        assert_eq!(folded, reported);
    }

    /// Open reports whose executions never shrink, closed by any ending, as the broker could send them.
    fn arbitrary_reports() -> impl Strategy<Value = (Order, Vec<OrderReport>)> {
        (
            1..1_000u64,
            prop::collection::vec(0..=100u64, 0..6),
            prop::sample::select(vec![
                OrderEnding::Filled,
                OrderEnding::Canceled,
                OrderEnding::Expired,
                OrderEnding::Rejected,
            ]),
            0..=100u64,
            1..1_000_000_000i64,
        )
            .prop_map(|(ordered, mut percents, closing, closing_percent, ticks)| {
                percents.sort_unstable();
                let at = |percent: u64| {
                    OrderExecution::new(
                        whole(ordered * percent / 100),
                        Price::from_ticks(ticks).unwrap(),
                    )
                };
                let last = percents.last().copied().unwrap_or(0);
                let closing_percent = match closing {
                    OrderEnding::Filled => 100,
                    OrderEnding::Canceled | OrderEnding::Expired | OrderEnding::Rejected => {
                        closing_percent.max(last)
                    }
                };
                let mut reports: Vec<OrderReport> = percents
                    .iter()
                    .enumerate()
                    .map(|(minute, percent)| report(OrderStatus::Open, at(*percent), minute as i64))
                    .collect();
                reports.push(report(
                    OrderStatus::Closed(closing),
                    at(closing_percent),
                    10,
                ));
                (buy(ordered), reports)
            })
    }

    proptest! {
        /// Any lawful sequence closes on its last report, and at every point reading the latest report again changes
        /// nothing, so a poll that reads one state twice is harmless.
        #[test]
        fn property_reports_close_once_and_repeat_harmlessly((order, reports) in arbitrary_reports()) {
            let closed = observe_all(&order, &reports).unwrap();
            let last = *reports.last().unwrap();
            prop_assert_eq!(closed.closed().map(|(_, at)| at), Some(last.at));
            prop_assert_eq!(closed.executed(), last.executed);
            prop_assert_eq!(closed.observe(&order, last), Ok(closed));
            for (index, latest) in reports.iter().enumerate() {
                let state = observe_all(&order, &reports[..=index]).unwrap();
                prop_assert_eq!(state.observe(&order, *latest), Ok(state));
            }
        }

        #[test]
        fn property_a_client_order_id_round_trips(bytes in any::<[u8; 16]>(), sequence in any::<u32>()) {
            let id = ClientOrderId::new(RunId::new(Uuid::from_bytes(bytes)), sequence);
            prop_assert_eq!(ClientOrderId::parse(&id.to_string()), Some(id));
        }
    }
}
