//! Alpaca's paper trading API: the account and positions as a `Book`, and orders submitted, read back by their client
//! order id and canceled. Refused for keys that trade live, so nothing here can reach real money.

use chrono::{DateTime, Utc};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::common::book::{Book, Cash, Position, Side};
use crate::common::market::{PRICE_SCALE, Price, SHARE_SCALE, Shares, Symbol};
use crate::common::order::{
    ClientOrderId, OrderEnding, OrderExecution, OrderReport, OrderRequest, OrderStatus,
};
use crate::ingest::FetchError;
use crate::ingest::alpaca::Alpaca;
use crate::ingest::retry::{Outcome, send, with_retries};

/// Digits after the point in a `Cash` unit: ticks times millionths of a share.
const CASH_DIGITS: u32 = 12;
const SHARE_DIGITS: u32 = 6;
const PRICE_DIGITS: u32 = 6;

const _: () = assert!(10_i64.pow(PRICE_DIGITS) == PRICE_SCALE);
const _: () = assert!(10_u64.pow(SHARE_DIGITS) == SHARE_SCALE);

/// Alpaca's paper account, held only for keys that trade against it.
pub struct PaperAccount {
    alpaca: Alpaca,
}

/// Alpaca's own id for an order, which a cancel names.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct BrokerOrderId(String);

/// An order as Alpaca reports it: its id there and where it stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerOrder {
    id: BrokerOrderId,
    report: OrderReport,
}

impl BrokerOrder {
    pub fn id(&self) -> &BrokerOrderId {
        &self.id
    }

    pub fn report(&self) -> OrderReport {
        self.report
    }
}

/// What a cancel achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancel {
    /// Alpaca accepted the request; the order may still fill before it takes effect, so read it back.
    Requested,
    /// The order had already closed, however it ended.
    AlreadyClosed,
}

#[derive(Debug)]
pub enum BrokerError {
    /// The keys trade live; this client refuses them.
    NotPaper,
    /// A submission that got no answer, so the order may or may not exist; read it back by its client order id.
    Unanswered {
        cause: String,
    },
    Fetch(FetchError),
    /// A field that did not read as the documented payload, named with its raw value.
    Malformed {
        field: &'static str,
        raw: String,
    },
    /// A status this client does not map, which a caller should treat as unknown rather than guess.
    UnknownStatus {
        status: String,
    },
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPaper => write!(formatter, "the Alpaca keys trade live, not on paper"),
            Self::Unanswered { cause } => {
                write!(formatter, "the order submission got no answer: {cause}")
            }
            Self::Fetch(error) => write!(formatter, "{error}"),
            Self::Malformed { field, raw } => write!(formatter, "{field} read `{raw}`"),
            Self::UnknownStatus { status } => write!(formatter, "unknown order status `{status}`"),
        }
    }
}

impl std::error::Error for BrokerError {}

impl From<FetchError> for BrokerError {
    fn from(error: FetchError) -> Self {
        Self::Fetch(error)
    }
}

#[derive(Deserialize)]
struct AccountPayload {
    cash: String,
}

#[derive(Deserialize)]
struct PositionPayload {
    symbol: String,
    /// Negative for a short position.
    qty: String,
}

#[derive(Deserialize)]
struct OrderPayload {
    id: BrokerOrderId,
    status: String,
    filled_qty: String,
    filled_avg_price: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct OrderBody<'a> {
    symbol: &'a str,
    qty: String,
    side: &'static str,
    #[serde(rename = "type")]
    order_type: &'static str,
    time_in_force: &'static str,
    client_order_id: String,
}

impl PaperAccount {
    pub fn new(alpaca: Alpaca) -> Result<Self, BrokerError> {
        if alpaca.is_paper() {
            Ok(Self { alpaca })
        } else {
            Err(BrokerError::NotPaper)
        }
    }

    /// The account's cash and positions as a book.
    pub async fn book(&self) -> Result<Book, BrokerError> {
        book(
            &self.get("/v2/account").await?,
            &self.get("/v2/positions").await?,
        )
    }

    /// Sends `request` once as a market order for the day. Never retried: a lost response is answered by reading the
    /// order back by its client order id, and a resend under the same id is refused by Alpaca as a duplicate.
    pub async fn submit(&self, request: &OrderRequest) -> Result<BrokerOrder, BrokerError> {
        let order = request.order();
        let body = OrderBody {
            symbol: order.symbol().as_str(),
            qty: units_to_decimal(u128::from(order.shares().units()), SHARE_DIGITS),
            side: match order.side() {
                Side::Buy => "buy",
                Side::Sell => "sell",
            },
            order_type: "market",
            time_in_force: "day",
            client_order_id: request.client_order_id().to_string(),
        };
        let body = serde_json::to_vec(&body).expect("an order body serializes");
        submitted(
            send(
                self.alpaca
                    .trading(Method::POST, "/v2/orders")
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body),
            )
            .await,
        )
    }

    /// The order sent under `id`, read through transient failures.
    pub async fn order(&self, id: ClientOrderId) -> Result<BrokerOrder, BrokerError> {
        let raw = id.to_string();
        let body = with_retries(|| {
            send(
                self.alpaca
                    .trading(Method::GET, "/v2/orders:by_client_order_id")
                    .query(&[("client_order_id", raw.as_str())]),
            )
        })
        .await?;
        broker_order(&body)
    }

    /// Asks Alpaca to cancel `id`; a 422 means the order had already closed.
    pub async fn cancel(&self, id: &BrokerOrderId) -> Result<Cancel, BrokerError> {
        let path = format!("/v2/orders/{}", id.0);
        let outcome = with_retries(|| send(self.alpaca.trading(Method::DELETE, &path))).await;
        match outcome {
            Ok(_) => Ok(Cancel::Requested),
            Err(FetchError::Refused { status: 422, .. }) => Ok(Cancel::AlreadyClosed),
            Err(error) => Err(error.into()),
        }
    }

    async fn get(&self, path: &str) -> Result<Vec<u8>, BrokerError> {
        Ok(with_retries(|| send(self.alpaca.trading(Method::GET, path))).await?)
    }
}

/// A submission's one attempt as an order, or as `Unanswered` when it may have landed unseen.
fn submitted(outcome: Outcome) -> Result<BrokerOrder, BrokerError> {
    match outcome {
        Outcome::Body(body) => broker_order(&body),
        Outcome::Transient(cause) => Err(BrokerError::Unanswered { cause }),
        Outcome::Refused { status, body } => Err(FetchError::Refused { status, body }.into()),
    }
}

fn parse<Payload: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<Payload, BrokerError> {
    serde_json::from_slice(body).map_err(|error| {
        BrokerError::Fetch(FetchError::Malformed {
            reason: error.to_string(),
        })
    })
}

fn book(account: &[u8], positions: &[u8]) -> Result<Book, BrokerError> {
    let account: AccountPayload = parse(account)?;
    let positions: Vec<PositionPayload> = parse(positions)?;
    let cash = Cash::from_units(decimal(&account.cash, CASH_DIGITS, "cash")?);
    let positions = positions
        .into_iter()
        .map(|position| {
            let symbol = Symbol::new(&position.symbol).map_err(|_| BrokerError::Malformed {
                field: "symbol",
                raw: position.symbol.clone(),
            })?;
            let units = decimal(&position.qty, SHARE_DIGITS, "qty")?;
            Ok((symbol, Position::from_units(units)))
        })
        .collect::<Result<Vec<_>, BrokerError>>()?;
    Ok(Book::reported(cash, positions))
}

fn broker_order(body: &[u8]) -> Result<BrokerOrder, BrokerError> {
    let payload: OrderPayload = parse(body)?;
    let status = order_status(&payload.status)?;
    let filled = decimal(&payload.filled_qty, SHARE_DIGITS, "filled_qty")?;
    let filled = u64::try_from(filled).map_err(|_| BrokerError::Malformed {
        field: "filled_qty",
        raw: payload.filled_qty.clone(),
    })?;
    let executed = match (filled, &payload.filled_avg_price) {
        (0, _) => None,
        (_, None) => {
            return Err(BrokerError::Malformed {
                field: "filled_avg_price",
                raw: String::new(),
            });
        }
        (_, Some(raw)) => {
            let ticks = rounded(raw, PRICE_DIGITS, "filled_avg_price")?;
            let price = i64::try_from(ticks)
                .ok()
                .and_then(|ticks| Price::from_ticks(ticks).ok())
                .ok_or_else(|| BrokerError::Malformed {
                    field: "filled_avg_price",
                    raw: raw.clone(),
                })?;
            OrderExecution::new(Shares::from_units(filled), price)
        }
    };
    Ok(BrokerOrder {
        id: payload.id,
        report: OrderReport::new(status, executed, payload.updated_at),
    })
}

/// Alpaca's statuses, collapsed; `replaced` and anything unlisted is refused, since this client never replaces.
fn order_status(status: &str) -> Result<OrderStatus, BrokerError> {
    match status {
        "new"
        | "accepted"
        | "pending_new"
        | "accepted_for_bidding"
        | "partially_filled"
        | "pending_cancel"
        | "pending_replace"
        | "calculated"
        | "stopped"
        | "suspended"
        | "held" => Ok(OrderStatus::Open),
        "filled" => Ok(OrderStatus::Closed(OrderEnding::Filled)),
        "canceled" => Ok(OrderStatus::Closed(OrderEnding::Canceled)),
        "expired" | "done_for_day" => Ok(OrderStatus::Closed(OrderEnding::Expired)),
        "rejected" => Ok(OrderStatus::Closed(OrderEnding::Rejected)),
        unknown => Err(BrokerError::UnknownStatus {
            status: unknown.to_string(),
        }),
    }
}

/// A signed decimal string as a whole number of `10^-digits`, refused when it holds more digits than that.
fn decimal(raw: &str, digits: u32, field: &'static str) -> Result<i128, BrokerError> {
    scaled(raw, digits, false).ok_or_else(|| BrokerError::Malformed {
        field,
        raw: raw.to_string(),
    })
}

/// As `decimal`, rounding half away from zero past `digits`, for an average that need not sit on the grid.
fn rounded(raw: &str, digits: u32, field: &'static str) -> Result<i128, BrokerError> {
    scaled(raw, digits, true).ok_or_else(|| BrokerError::Malformed {
        field,
        raw: raw.to_string(),
    })
}

fn scaled(raw: &str, digits: u32, round: bool) -> Option<i128> {
    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let is_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !is_digits(whole) || !is_digits(fraction) {
        return None;
    }
    let width = usize::try_from(digits).ok()?;
    let (kept, dropped) = fraction.split_at(fraction.len().min(width));
    if !round && dropped.bytes().any(|byte| byte != b'0') {
        return None;
    }
    let mut units = whole.parse::<i128>().ok()?;
    for byte in kept
        .bytes()
        .chain(std::iter::repeat_n(b'0', width - kept.len()))
    {
        units = units
            .checked_mul(10)?
            .checked_add(i128::from(byte - b'0'))?;
    }
    if round && dropped.bytes().next().is_some_and(|byte| byte >= b'5') {
        units = units.checked_add(1)?;
    }
    Some(if negative { -units } else { units })
}

/// Whole `10^-digits` units as the shortest decimal string Alpaca reads.
fn units_to_decimal(units: u128, digits: u32) -> String {
    let scale = 10_u128.pow(digits);
    let (whole, fraction) = (units / scale, units % scale);
    match fraction {
        0 => whole.to_string(),
        fraction => {
            let width = usize::try_from(digits).expect("a digit count fits usize");
            let padded = format!("{fraction:0width$}");
            format!("{whole}.{}", padded.trim_end_matches('0'))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;
    use crate::common::journal::RunId;
    use crate::common::market::Shares;
    use crate::common::monoid::Monoid;
    use crate::common::order::OrderState;
    use crate::common::strategy::{Target, orders};

    /// A canceled order as the paper account returned it on 2026-10-05, trimmed to the fields read and one beside.
    const CANCELED: &str = r#"{"id":"cfdd3ad8-5f57-44fe-9066-4adbf6bcd221","client_order_id":"c_7885f50f-a68f-4b06-a359-fec24d64b30a","updated_at":"2026-09-25T14:59:42.110148Z","symbol":"SPY","qty":"1","filled_qty":"0","filled_avg_price":null,"side":"buy","status":"canceled"}"#;

    /// A filled order as the paper account returned it on 2026-10-05, trimmed likewise.
    const FILLED: &str = r#"{"id":"a9df02ed-b9da-4955-9b7f-ecd1a427e9cf","updated_at":"2026-08-21T19:45:07.724724Z","qty":"26","filled_qty":"26","filled_avg_price":"37.89","side":"buy","type":"market","status":"filled"}"#;

    #[test]
    fn test_live_keys_are_refused() {
        assert!(matches!(
            PaperAccount::new(Alpaca::unkeyed("false")),
            Err(BrokerError::NotPaper)
        ));
        assert!(PaperAccount::new(Alpaca::unkeyed("TRUE")).is_ok());
    }

    #[test]
    fn test_orders_read_as_the_paper_account_sends_them() {
        let canceled = broker_order(CANCELED.as_bytes()).unwrap();
        assert_eq!(
            canceled.id(),
            &BrokerOrderId("cfdd3ad8-5f57-44fe-9066-4adbf6bcd221".to_string())
        );
        assert_eq!(
            canceled.report(),
            OrderReport::new(
                OrderStatus::Closed(OrderEnding::Canceled),
                None,
                "2026-09-25T14:59:42.110148Z".parse().unwrap()
            )
        );
        let filled = broker_order(FILLED.as_bytes()).unwrap();
        assert_eq!(
            filled.report(),
            OrderReport::new(
                OrderStatus::Closed(OrderEnding::Filled),
                OrderExecution::new(
                    Shares::whole(26).unwrap(),
                    Price::from_ticks(37_890_000).unwrap()
                ),
                "2026-08-21T19:45:07.724724Z".parse().unwrap()
            )
        );
        let unpriced = FILLED.replace(
            r#""filled_avg_price":"37.89""#,
            r#""filled_avg_price":null"#,
        );
        assert!(matches!(
            broker_order(unpriced.as_bytes()),
            Err(BrokerError::Malformed {
                field: "filled_avg_price",
                ..
            })
        ));
    }

    /// The account and an empty positions list as the paper account returned them on 2026-10-05.
    #[test]
    fn test_the_book_reads_as_the_paper_account_sends_it() {
        let account = br#"{"status":"ACTIVE","cash":"19752.73","currency":"USD","equity":"19752.73","buying_power":"79010.92"}"#;
        assert_eq!(
            book(account, b"[]").unwrap(),
            Book::reported(Cash::from_units(19_752_730_000_000_000), [])
        );
        assert!(book(b"", b"[]").is_err());
        assert!(book(account, b"").is_err());
    }

    #[test]
    fn test_every_status_maps_or_is_refused() {
        let mapped: Vec<(&str, OrderStatus)> = [
            "new",
            "accepted",
            "pending_new",
            "accepted_for_bidding",
            "partially_filled",
            "pending_cancel",
            "pending_replace",
            "calculated",
            "stopped",
            "suspended",
            "held",
            "filled",
            "canceled",
            "expired",
            "done_for_day",
            "rejected",
        ]
        .into_iter()
        .map(|status| (status, order_status(status).unwrap()))
        .filter(|(_, status)| *status != OrderStatus::Open)
        .collect();
        assert_eq!(
            mapped,
            [
                ("filled", OrderStatus::Closed(OrderEnding::Filled)),
                ("canceled", OrderStatus::Closed(OrderEnding::Canceled)),
                ("expired", OrderStatus::Closed(OrderEnding::Expired)),
                ("done_for_day", OrderStatus::Closed(OrderEnding::Expired)),
                ("rejected", OrderStatus::Closed(OrderEnding::Rejected)),
            ]
        );
        assert!(
            matches!(order_status("replaced"), Err(BrokerError::UnknownStatus { status }) if status == "replaced")
        );
    }

    #[test]
    fn test_decimals_read_exactly_or_are_refused() {
        assert_eq!(
            decimal("19752.73", CASH_DIGITS, "cash").unwrap(),
            19_752_730_000_000_000
        );
        assert_eq!(decimal("-5", SHARE_DIGITS, "qty").unwrap(), -5_000_000);
        assert_eq!(
            decimal("0.500000000", SHARE_DIGITS, "qty").unwrap(),
            500_000
        );
        assert_eq!(
            rounded("37.8912345", PRICE_DIGITS, "price").unwrap(),
            37_891_235
        );
        assert_eq!(
            rounded("37.8912344", PRICE_DIGITS, "price").unwrap(),
            37_891_234
        );
        assert_eq!(rounded("-0.0000005", PRICE_DIGITS, "price").unwrap(), -1);
        assert_eq!(
            rounded("9.9999995", PRICE_DIGITS, "price").unwrap(),
            10_000_000
        );
        assert_eq!(rounded("-0.5", 0, "price").unwrap(), -1);
        for raw in ["0.1234567", "", ".5", "1e3", "--1", "1,000", " 1"] {
            assert!(decimal(raw, SHARE_DIGITS, "qty").is_err(), "{raw}");
        }
        assert_eq!(units_to_decimal(1_500_000, SHARE_DIGITS), "1.5");
        assert_eq!(units_to_decimal(3_000_000, SHARE_DIGITS), "3");
        assert_eq!(units_to_decimal(1, SHARE_DIGITS), "0.000001");
    }

    #[test]
    fn test_an_unanswered_submission_is_told_apart_from_a_refused_one() {
        assert!(matches!(
            submitted(Outcome::Transient("status 503".to_string())),
            Err(BrokerError::Unanswered { cause }) if cause == "status 503"
        ));
        assert!(matches!(
            submitted(Outcome::Refused {
                status: 422,
                body: "duplicate".to_string()
            }),
            Err(BrokerError::Fetch(FetchError::Refused { status: 422, .. }))
        ));
        assert_eq!(
            submitted(Outcome::Body(CANCELED.as_bytes().to_vec()))
                .unwrap()
                .id(),
            &BrokerOrderId("cfdd3ad8-5f57-44fe-9066-4adbf6bcd221".to_string())
        );
    }

    proptest! {
        #[test]
        fn property_a_quantity_written_reads_back(units in any::<u64>()) {
            let written = units_to_decimal(u128::from(units), SHARE_DIGITS);
            prop_assert_eq!(decimal(&written, SHARE_DIGITS, "qty").unwrap(), i128::from(units));
        }
    }

    /// Submits one SPY share on the paper account while the market is closed, reads it back by its client order id,
    /// cancels it, and folds every report through the order state; the account ends where it began.
    #[tokio::test]
    #[ignore = "trades on the Alpaca paper account; run deliberately under a development secretspec profile"]
    async fn live_a_paper_order_is_submitted_read_back_and_canceled() {
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let clock: serde_json::Value = parse(
            &with_retries(|| send(alpaca.trading(Method::GET, "/v2/clock")))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            clock["is_open"], false,
            "run while the market is closed, so the order cannot fill"
        );
        let account = PaperAccount::new(alpaca).unwrap();
        let before = account.book().await.unwrap();
        let spy = Symbol::new("SPY").unwrap();
        let order = orders(
            &Book::empty(),
            &Target::new(BTreeMap::from([(spy.clone(), Shares::whole(1).unwrap())])),
        )
        .remove(0);
        let id = ClientOrderId::new(RunId::new(Uuid::new_v4()), 0);
        let submitted = account
            .submit(&OrderRequest::new(order.clone(), id))
            .await
            .unwrap();
        let read = account.order(id).await.unwrap();
        assert_eq!(read.id(), submitted.id());
        let cancel = account.cancel(read.id()).await.unwrap();
        let mut last = account.order(id).await.unwrap();
        for _ in 0..20 {
            match last.report().status() {
                OrderStatus::Open => {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    last = account.order(id).await.unwrap();
                }
                OrderStatus::Closed(_) => break,
            }
        }
        let state = [submitted.report(), read.report(), last.report()]
            .into_iter()
            .try_fold(OrderState::submitted(), |state, report| {
                state.observe(&order, report)
            })
            .unwrap();
        println!("cancel {cancel:?}, closed as {state:?}");
        match (state.closed(), state.executed()) {
            (Some(_), None) => {}
            (Some(_), Some(_)) | (None, _) => {
                panic!(
                    "the order did not close unfilled: {state:?}; flatten SPY on the paper account by hand"
                )
            }
        }
        assert_eq!(account.book().await.unwrap(), before);
    }
}
