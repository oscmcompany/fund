//! Order lifecycle typestate machine.
//!
//! The typestate pattern encodes the order lifecycle as type parameters, making
//! invalid state transitions compile errors. A `PendingPair` is consumed by
//! `confirm()`, making a dangling long (short fails after long fills)
//! unrepresentable at the type level.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use uuid::Uuid;

use crate::primitives::{Dollars, Shares};

/// Sealing module: prevents external crates from implementing `OrderState`.
mod private {
    pub trait Sealed {}
}

/// Marker trait for valid order lifecycle states.
///
/// Sealed to prevent external state implementations.
pub trait OrderState: private::Sealed {}

/// Order has been submitted to the broker and is awaiting fill confirmation.
#[derive(Debug, Clone)]
pub struct Pending;

/// Order has been confirmed as filled by the broker.
#[derive(Debug, Clone)]
pub struct Filled;

/// Order was rejected by the broker.
#[derive(Debug, Clone)]
pub struct Rejected;

/// Order was cancelled.
#[derive(Debug, Clone)]
pub struct Cancelled;

impl private::Sealed for Pending {}
impl private::Sealed for Filled {}
impl private::Sealed for Rejected {}
impl private::Sealed for Cancelled {}

impl OrderState for Pending {}
impl OrderState for Filled {}
impl OrderState for Rejected {}
impl OrderState for Cancelled {}

/// A typed order at lifecycle state `S`.
///
/// The `fill_price` field is `None` for `Order<Pending>` and `Some` only after
/// fill confirmation.
#[derive(Debug, Clone)]
pub struct Order<S: OrderState> {
    pub id: Uuid,
    pub ticker: String,
    pub side: String,
    pub quantity: Decimal,
    pub order_type: String,
    pub limit_price: Option<Decimal>,
    pub alpaca_order_id: String,
    pub submitted_at: DateTime<Utc>,
    /// Set on fill confirmation; `None` for pending orders.
    pub fill_price: Option<Decimal>,
    _state: PhantomData<S>,
}

impl Order<Pending> {
    /// Creates a new pending order after it has been submitted to the broker.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        ticker: String,
        side: String,
        quantity: Decimal,
        order_type: String,
        limit_price: Option<Decimal>,
        alpaca_order_id: String,
        submitted_at: DateTime<Utc>,
    ) -> Self {
        Order {
            id,
            ticker,
            side,
            quantity,
            order_type,
            limit_price,
            alpaca_order_id,
            submitted_at,
            fill_price: None,
            _state: PhantomData,
        }
    }
}

/// Represents a request to place an order with the broker, before submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub ticker: String,
    pub side: String,
    pub quantity: Shares,
    pub order_type: String,
    pub limit_price: Option<Dollars>,
}

/// Fill confirmation data returned by the broker for a specific order.
#[derive(Debug, Clone)]
pub struct FilledOrder {
    pub alpaca_order_id: String,
    pub fill_price: Decimal,
    pub filled_quantity: Decimal,
}

/// Error returned when `PendingPair::confirm()` cannot produce a `FilledPair`.
#[derive(Debug, Clone, PartialEq)]
pub enum PairFillError {
    /// The long leg did not fill.
    LongNotFilled,
    /// The short leg did not fill.
    ShortNotFilled,
}

impl std::fmt::Display for PairFillError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PairFillError::LongNotFilled => write!(formatter, "Long order did not fill."),
            PairFillError::ShortNotFilled => write!(formatter, "Short order did not fill."),
        }
    }
}

impl std::error::Error for PairFillError {}

/// A pair of pending orders (long + short) awaiting fill confirmation.
///
/// Consuming this type via `confirm()` is the only way to obtain a `FilledPair`.
/// If either leg fails to fill, the entire `PendingPair` is consumed and dropped,
/// making a dangling long position unrepresentable.
#[derive(Debug)]
pub struct PendingPair {
    pub long: Order<Pending>,
    pub short: Order<Pending>,
    pub long_beta: f64,
    pub short_beta: f64,
}

/// A pair where both the long and short legs have been confirmed as filled.
#[derive(Debug, Clone)]
pub struct FilledPair {
    pub long: Order<Filled>,
    pub short: Order<Filled>,
    pub long_beta: f64,
    pub short_beta: f64,
    pub long_notional: Dollars,
    pub short_notional: Dollars,
}

impl PendingPair {
    /// Confirms both legs of the pair as filled, consuming the `PendingPair`.
    ///
    /// Pass `Some(FilledOrder)` for each leg that filled. Pass `None` for a leg
    /// that did not fill (rejected, cancelled, or timed out). If either leg is
    /// `None`, the entire pair is dropped — a dangling long is unrepresentable.
    pub fn confirm(
        self,
        long_fill: Option<FilledOrder>,
        short_fill: Option<FilledOrder>,
    ) -> Result<FilledPair, PairFillError> {
        let long_fill = long_fill.ok_or(PairFillError::LongNotFilled)?;
        let short_fill = short_fill.ok_or(PairFillError::ShortNotFilled)?;

        let long_notional = Dollars(long_fill.fill_price * long_fill.filled_quantity);
        let short_notional = Dollars(short_fill.fill_price * short_fill.filled_quantity);

        let long = Order {
            id: self.long.id,
            ticker: self.long.ticker,
            side: self.long.side,
            quantity: long_fill.filled_quantity,
            order_type: self.long.order_type,
            limit_price: self.long.limit_price,
            alpaca_order_id: self.long.alpaca_order_id,
            submitted_at: self.long.submitted_at,
            fill_price: Some(long_fill.fill_price),
            _state: PhantomData::<Filled>,
        };

        let short = Order {
            id: self.short.id,
            ticker: self.short.ticker,
            side: self.short.side,
            quantity: short_fill.filled_quantity,
            order_type: self.short.order_type,
            limit_price: self.short.limit_price,
            alpaca_order_id: self.short.alpaca_order_id,
            submitted_at: self.short.submitted_at,
            fill_price: Some(short_fill.fill_price),
            _state: PhantomData::<Filled>,
        };

        Ok(FilledPair {
            long,
            short,
            long_beta: self.long_beta,
            short_beta: self.short_beta,
            long_notional,
            short_notional,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn make_pending_order(ticker: &str, side: &str) -> Order<Pending> {
        Order::<Pending>::new(
            Uuid::new_v4(),
            ticker.to_string(),
            side.to_string(),
            Decimal::from(100),
            "market".to_string(),
            None,
            format!("alpaca-{}-id", ticker.to_lowercase()),
            Utc::now(),
        )
    }

    fn make_filled_order(ticker: &str) -> FilledOrder {
        FilledOrder {
            alpaca_order_id: format!("fill-{}", ticker.to_lowercase()),
            fill_price: Decimal::from(100),
            filled_quantity: Decimal::from(100),
        }
    }

    fn make_pending_pair(long_ticker: &str, short_ticker: &str) -> PendingPair {
        PendingPair {
            long: make_pending_order(long_ticker, "LONG"),
            short: make_pending_order(short_ticker, "SHORT"),
            long_beta: 1.1,
            short_beta: 0.9,
        }
    }

    #[test]
    fn test_order_pending_new() {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let order = Order::<Pending>::new(
            id,
            "AAPL".to_string(),
            "LONG".to_string(),
            Decimal::from(50),
            "market".to_string(),
            None,
            "alpaca-order-1".to_string(),
            now,
        );
        assert_eq!(order.ticker, "AAPL");
        assert_eq!(order.side, "LONG");
        assert_eq!(order.quantity, Decimal::from(50));
        assert!(order.fill_price.is_none());
        assert!(order.limit_price.is_none());
    }

    #[test]
    fn test_order_pending_with_limit_price() {
        let order = Order::<Pending>::new(
            Uuid::new_v4(),
            "MSFT".to_string(),
            "SHORT".to_string(),
            Decimal::from(25),
            "limit".to_string(),
            Some(Decimal::from(420)),
            "alpaca-limit-1".to_string(),
            Utc::now(),
        );
        assert_eq!(order.limit_price, Some(Decimal::from(420)));
    }

    #[test]
    fn test_order_request_construction() {
        let request = OrderRequest {
            ticker: "NVDA".to_string(),
            side: "LONG".to_string(),
            quantity: Shares(Decimal::from(10)),
            order_type: "market".to_string(),
            limit_price: None,
        };
        assert_eq!(request.ticker, "NVDA");
        assert_eq!(request.quantity.0, Decimal::from(10));
    }

    #[test]
    fn test_pending_pair_confirm_both_filled() {
        let pair = make_pending_pair("AAPL", "MSFT");
        let long_ticker = pair.long.ticker.clone();
        let short_ticker = pair.short.ticker.clone();
        let long_alpaca_id = pair.long.alpaca_order_id.clone();
        let short_alpaca_id = pair.short.alpaca_order_id.clone();

        let filled = pair
            .confirm(
                Some(make_filled_order("AAPL")),
                Some(make_filled_order("MSFT")),
            )
            .unwrap();

        assert_eq!(filled.long.ticker, long_ticker);
        assert_eq!(filled.short.ticker, short_ticker);
        assert_eq!(filled.long.fill_price, Some(Decimal::from(100)));
        assert_eq!(filled.short.fill_price, Some(Decimal::from(100)));
        assert_eq!(filled.long_notional.0, Decimal::from(10_000));
        assert_eq!(filled.short_notional.0, Decimal::from(10_000));
        assert_eq!(filled.long_beta, 1.1);
        assert_eq!(filled.short_beta, 0.9);
        // Broker order ID is preserved from the submitted order, not taken from the fill payload.
        assert_eq!(filled.long.alpaca_order_id, long_alpaca_id);
        assert_eq!(filled.short.alpaca_order_id, short_alpaca_id);
    }

    #[test]
    fn test_pending_pair_confirm_long_not_filled() {
        let pair = make_pending_pair("AAPL", "MSFT");
        let error = pair
            .confirm(None, Some(make_filled_order("MSFT")))
            .unwrap_err();
        assert_eq!(error, PairFillError::LongNotFilled);
    }

    #[test]
    fn test_pending_pair_confirm_short_not_filled() {
        let pair = make_pending_pair("AAPL", "MSFT");
        let error = pair
            .confirm(Some(make_filled_order("AAPL")), None)
            .unwrap_err();
        assert_eq!(error, PairFillError::ShortNotFilled);
    }

    #[test]
    fn test_pending_pair_confirm_neither_filled() {
        let pair = make_pending_pair("AAPL", "MSFT");
        let error = pair.confirm(None, None).unwrap_err();
        // Long is checked first
        assert_eq!(error, PairFillError::LongNotFilled);
    }

    #[test]
    fn test_pair_fill_error_display() {
        assert!(format!("{}", PairFillError::LongNotFilled).contains("Long"));
        assert!(format!("{}", PairFillError::ShortNotFilled).contains("Short"));
    }

    #[test]
    fn test_filled_pair_clone() {
        let pair = make_pending_pair("GOOG", "META");
        let filled = pair
            .confirm(
                Some(make_filled_order("GOOG")),
                Some(make_filled_order("META")),
            )
            .unwrap();
        let cloned = filled.clone();
        assert_eq!(cloned.long.ticker, "GOOG");
        assert_eq!(cloned.short.ticker, "META");
    }

    #[test]
    fn test_order_states_exist() {
        // Verify all state types compile and have the expected Debug output
        let _pending = Pending;
        let _filled = Filled;
        let _rejected = Rejected;
        let _cancelled = Cancelled;
        assert_eq!(format!("{:?}", Pending), "Pending");
        assert_eq!(format!("{:?}", Filled), "Filled");
        assert_eq!(format!("{:?}", Rejected), "Rejected");
        assert_eq!(format!("{:?}", Cancelled), "Cancelled");
    }
}
