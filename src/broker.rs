//! Broker clients: where an order leaves the process and a broker's account comes back, mapped into `common` types.

pub mod alpaca;

use std::collections::BTreeMap;

use crate::broker::alpaca::{BrokerError, BrokerOrder, BrokerOrderId, Cancel};
use crate::common::guard::Tradability;
use crate::common::market::Symbol;
use crate::common::order::{ClientOrderId, OrderRequest};

/// What execution asks of a broker, so the order loop runs alike against the paper account and a scripted one.
pub trait Broker {
    /// Sends `request` once, never retried; an answer that may have been lost is `BrokerError::Unanswered`.
    fn submit(
        &self,
        request: &OrderRequest,
    ) -> impl Future<Output = Result<BrokerOrder, BrokerError>> + Send;

    /// The order sent under `id`.
    fn order(
        &self,
        id: ClientOrderId,
    ) -> impl Future<Output = Result<BrokerOrder, BrokerError>> + Send;

    /// Asks the broker to cancel `id`; neither answer proves the order has closed.
    fn cancel(
        &self,
        id: &BrokerOrderId,
    ) -> impl Future<Output = Result<Cancel, BrokerError>> + Send;

    /// What the broker reports of each symbol's trading, read before orders in them go out.
    fn tradability(
        &self,
        symbols: &[Symbol],
    ) -> impl Future<Output = Result<BTreeMap<Symbol, Tradability>, BrokerError>> + Send;
}
