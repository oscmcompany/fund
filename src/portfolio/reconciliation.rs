//! Periodic reconciliation between PostgreSQL pair state and Alpaca broker positions.
//!
//! The [`reconcile`] function is stateless and idempotent: it can run at any
//! cadence — once per rebalance cycle (Phase 0) or every 30 seconds (Phase 2b) —
//! without behavioral changes. All corrective actions check current state before
//! acting, so repeated calls against the same discrepancy are safe.
//!
//! Discrepancy categories:
//! - **Alpaca-only**: Alpaca holds a position the DB does not track → close orphan.
//! - **DB-only**: DB has an open pair but Alpaca has no position → mark pair closed.
//! - **Partial position loss**: One leg of a pair is missing → close surviving leg.
//! - **Stale submitted order**: An order exceeded the staleness threshold → query Alpaca
//!   and either confirm fill or cancel.
//! - **Compensation failure retry**: A prior compensation failure is retried.

use std::collections::{HashMap, HashSet};

use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::domain::trading::{CloseReason, ReconciliationAction, ReconciliationEventType};
use crate::portfolio::alpaca::{Position, Trading};
use crate::portfolio::database::{self, SubmittedOrder, UnresolvedReconciliationEvent};

/// Default staleness threshold for submitted orders (seconds).
const STALE_ORDER_THRESHOLD_SECONDS: i64 = 60;

/// Summary of actions taken during a reconciliation pass.
#[derive(Debug)]
pub struct ReconciliationReport {
    /// Number of orphaned Alpaca positions closed.
    pub orphans_closed: usize,
    /// Number of DB pairs marked closed because Alpaca no longer holds them.
    pub pairs_marked_closed: usize,
    /// Number of partial position losses handled (surviving leg closed).
    pub partial_positions_closed: usize,
    /// Number of stale submitted orders resolved (confirmed or cancelled).
    pub stale_orders_resolved: usize,
    /// Number of compensation failures retried.
    pub compensation_retries: usize,
}

/// Runs a full reconciliation pass comparing DB state against Alpaca positions.
///
/// This function is the single entry point for all reconciliation work. It:
/// 1. Fetches positions from Alpaca and open pairs from the DB.
/// 2. Compares ticker sets and classifies discrepancies.
/// 3. Takes corrective action for clear-cut cases, logs ambiguous ones.
/// 4. Resolves stale submitted orders by querying Alpaca.
/// 5. Retries unresolved compensation failures.
///
/// Returns a [`ReconciliationReport`] summarizing actions taken.
pub async fn reconcile(
    pool: &PgPool,
    alpaca: &dyn Trading,
) -> Result<ReconciliationReport, ReconciliationError> {
    let alpaca_positions = alpaca
        .fetch_positions()
        .await
        .map_err(ReconciliationError::AlpacaFetch)?;
    let open_pairs = database::fetch_open_pairs(pool)
        .await
        .map_err(ReconciliationError::Database)?;
    let stale_threshold = Utc::now() - Duration::seconds(STALE_ORDER_THRESHOLD_SECONDS);
    let stale_orders = database::fetch_submitted_orders(pool, stale_threshold)
        .await
        .map_err(ReconciliationError::Database)?;
    let unresolved_events = database::fetch_unresolved_reconciliation_events(pool)
        .await
        .map_err(ReconciliationError::Database)?;

    // Build lookup structures.
    let alpaca_by_symbol: HashMap<&str, &Position> = alpaca_positions
        .iter()
        .map(|position| (position.symbol.as_str(), position))
        .collect();
    let alpaca_symbols: HashSet<&str> = alpaca_by_symbol.keys().copied().collect();

    // Collect all tickers from open pairs (both long and short legs).
    let mut database_symbols: HashSet<String> = HashSet::new();
    for pair in &open_pairs {
        database_symbols.insert(pair.long_ticker().as_str().to_string());
        database_symbols.insert(pair.short_ticker().as_str().to_string());
    }

    let mut report = ReconciliationReport {
        orphans_closed: 0,
        pairs_marked_closed: 0,
        partial_positions_closed: 0,
        stale_orders_resolved: 0,
        compensation_retries: 0,
    };

    // --- Alpaca-only positions: close orphans ---
    for symbol in &alpaca_symbols {
        if !database_symbols.contains(*symbol) {
            let position = alpaca_by_symbol[symbol];
            warn!(
                ticker = *symbol,
                quantity = position.quantity,
                "Alpaca-only position detected; closing orphan"
            );
            let action = match alpaca.close_position(symbol).await {
                Ok(_) => {
                    report.orphans_closed += 1;
                    ReconciliationAction::ClosedOrphan
                }
                Err(close_error) => {
                    error!(
                        ticker = *symbol,
                        error = %close_error,
                        "Failed to close orphaned position"
                    );
                    ReconciliationAction::LoggedOnly
                }
            };
            let resolved_at = if action == ReconciliationAction::ClosedOrphan {
                Some(Utc::now())
            } else {
                None
            };
            if let Err(error) = database::insert_reconciliation_event(
                pool,
                &ReconciliationEventType::AlpacaOnly,
                symbol,
                None,
                Some(
                    Decimal::try_from(position.quantity).unwrap_or_else(|error| {
                        error!(
                            ticker = *symbol,
                            quantity = position.quantity,
                            error = %error,
                            "Failed to convert position quantity to Decimal"
                        );
                        Decimal::ZERO
                    }),
                ),
                None,
                None,
                &action,
                resolved_at,
            )
            .await
            {
                error!(error = %error, "Failed to persist alpaca_only reconciliation event");
            }
        }
    }

    // --- DB-only pairs: mark closed ---
    // A pair is DB-only when BOTH its tickers are absent from Alpaca.
    // If only one leg is missing, that's a quantity/position issue, not a full pair loss.
    let mut closed_pair_ids: HashSet<uuid::Uuid> = HashSet::new();
    for pair in &open_pairs {
        let long_on_alpaca = alpaca_symbols.contains(pair.long_ticker().as_str());
        let short_on_alpaca = alpaca_symbols.contains(pair.short_ticker().as_str());
        if !long_on_alpaca && !short_on_alpaca && !closed_pair_ids.contains(&pair.id()) {
            error!(
                pair_id = pair.pair_id().as_str(),
                long_ticker = pair.long_ticker().as_str(),
                short_ticker = pair.short_ticker().as_str(),
                "DB pair has no Alpaca positions; marking closed"
            );
            let closed_at = Utc::now();
            let pair_closed = match database::close_equity_pair_with_reason(
                pool,
                pair.id(),
                closed_at,
                &CloseReason::ReconciliationAlpacaMissing,
            )
            .await
            {
                Ok(()) => {
                    closed_pair_ids.insert(pair.id());
                    report.pairs_marked_closed += 1;
                    true
                }
                Err(error) => {
                    error!(error = %error, "Failed to mark DB pair as closed");
                    false
                }
            };
            // Record event for both tickers.
            let action = if pair_closed {
                ReconciliationAction::MarkedPairClosed
            } else {
                ReconciliationAction::LoggedOnly
            };
            let resolved_at = if pair_closed { Some(closed_at) } else { None };
            for ticker in [pair.long_ticker().as_str(), pair.short_ticker().as_str()] {
                if let Err(error) = database::insert_reconciliation_event(
                    pool,
                    &ReconciliationEventType::DatabaseOnly,
                    ticker,
                    None,
                    None,
                    Some(pair.id()),
                    None,
                    &action,
                    resolved_at,
                )
                .await
                {
                    error!(error = %error, "Failed to persist database_only reconciliation event");
                }
            }
        }
    }

    // --- Partial position loss: close surviving leg and mark pair closed ---
    // When only one leg of a pair remains on Alpaca, the hedge is broken and the
    // portfolio carries naked directional risk. Close the surviving leg (if no
    // other open pair also uses that ticker) and mark the pair closed.
    for pair in &open_pairs {
        if closed_pair_ids.contains(&pair.id()) {
            continue;
        }
        let long_on_alpaca = alpaca_symbols.contains(pair.long_ticker().as_str());
        let short_on_alpaca = alpaca_symbols.contains(pair.short_ticker().as_str());
        if long_on_alpaca != short_on_alpaca {
            let missing_ticker = if !long_on_alpaca {
                pair.long_ticker().as_str()
            } else {
                pair.short_ticker().as_str()
            };
            let present_ticker = if long_on_alpaca {
                pair.long_ticker().as_str()
            } else {
                pair.short_ticker().as_str()
            };

            // Check if any other open pair (not already closed) also uses the
            // surviving ticker. If so, skip the close — the position is still
            // needed by the other pair.
            let ticker_shared_by_other_pair = open_pairs.iter().any(|other| {
                other.id() != pair.id()
                    && !closed_pair_ids.contains(&other.id())
                    && (other.long_ticker().as_str() == present_ticker
                        || other.short_ticker().as_str() == present_ticker)
            });

            if ticker_shared_by_other_pair {
                warn!(
                    pair_id = pair.pair_id().as_str(),
                    present_ticker = present_ticker,
                    "Surviving leg shared by another open pair; skipping close, marking pair closed only"
                );
            } else {
                error!(
                    pair_id = pair.pair_id().as_str(),
                    missing_ticker = missing_ticker,
                    present_ticker = present_ticker,
                    "Partial position: one leg missing; closing surviving leg to eliminate directional risk"
                );
            }

            // Close the surviving leg on Alpaca unless another pair needs it.
            let close_succeeded = if ticker_shared_by_other_pair {
                false
            } else {
                match alpaca.close_position(present_ticker).await {
                    Ok(_) => true,
                    Err(close_error) => {
                        error!(
                            ticker = present_ticker,
                            error = %close_error,
                            "Failed to close surviving leg of partial pair"
                        );
                        false
                    }
                }
            };

            // Only mark pair closed in DB when the close succeeds or when the
            // ticker is shared (the position belongs to another pair). If the
            // close failed, leave the pair open so compensation retry picks it up.
            let closed_at = Utc::now();
            let pair_closed = if close_succeeded || ticker_shared_by_other_pair {
                match database::close_equity_pair_with_reason(
                    pool,
                    pair.id(),
                    closed_at,
                    &CloseReason::ReconciliationAlpacaMissing,
                )
                .await
                {
                    Ok(()) => {
                        closed_pair_ids.insert(pair.id());
                        report.pairs_marked_closed += 1;
                        true
                    }
                    Err(error) => {
                        error!(error = %error, "Failed to mark partial pair as closed");
                        false
                    }
                }
            } else {
                false
            };

            let action = if close_succeeded {
                ReconciliationAction::ClosedSurvivingLeg
            } else if pair_closed {
                ReconciliationAction::MarkedPairClosed
            } else {
                ReconciliationAction::LoggedOnly
            };
            let resolved_at = if pair_closed || close_succeeded {
                Some(closed_at)
            } else {
                None
            };

            if let Err(error) = database::insert_reconciliation_event(
                pool,
                &ReconciliationEventType::PartialPositionLoss,
                missing_ticker,
                None,
                None,
                Some(pair.id()),
                None,
                &action,
                resolved_at,
            )
            .await
            {
                error!(error = %error, "Failed to persist partial_position_loss reconciliation event");
            }
            if close_succeeded || pair_closed {
                report.partial_positions_closed += 1;
            }
        }
    }

    // --- Stale submitted orders: query Alpaca and resolve ---
    for order in &stale_orders {
        report.stale_orders_resolved += resolve_stale_order(pool, alpaca, order).await;
    }

    // --- Compensation failure retries ---
    for event in &unresolved_events {
        if event.event_type() == ReconciliationEventType::CompensationFailure.as_str() {
            report.compensation_retries += retry_compensation_failure(pool, alpaca, event).await;
        }
    }

    info!(
        orphans_closed = report.orphans_closed,
        pairs_marked_closed = report.pairs_marked_closed,
        partial_positions_closed = report.partial_positions_closed,
        stale_orders_resolved = report.stale_orders_resolved,
        compensation_retries = report.compensation_retries,
        "Reconciliation pass completed"
    );

    Ok(report)
}

/// Queries Alpaca for a stale submitted order and either confirms the fill
/// or cancels it. Returns 1 if resolved, 0 otherwise.
async fn resolve_stale_order(pool: &PgPool, alpaca: &dyn Trading, order: &SubmittedOrder) -> usize {
    let order_result = alpaca.get_order(order.alpaca_order_id()).await;
    match order_result {
        Ok(fill) if fill.status == "filled" => {
            info!(
                alpaca_order_id = order.alpaca_order_id(),
                ticker = order.ticker(),
                "Stale submitted order was actually filled; confirming"
            );
            // Mark order as filled in DB. allocation_id is None because stale
            // orders resolved by reconciliation have no associated allocation.
            if let Err(error) =
                database::mark_order_filled(pool, order.id(), None, Utc::now()).await
            {
                error!(error = %error, "Failed to mark stale order as filled");
                return 0;
            }
            if let Err(error) = database::insert_reconciliation_event(
                pool,
                &ReconciliationEventType::StaleSubmittedOrder,
                order.ticker(),
                None,
                None,
                None,
                Some(order.alpaca_order_id()),
                &ReconciliationAction::ConfirmedFill,
                Some(Utc::now()),
            )
            .await
            {
                error!(error = %error, "Failed to persist stale_submitted_order event");
            }
            1
        }
        Ok(fill) if is_terminal_non_filled(&fill.status) => {
            info!(
                alpaca_order_id = order.alpaca_order_id(),
                status = fill.status.as_str(),
                "Stale submitted order in terminal non-filled state; marking cancelled"
            );
            if let Err(error) = database::mark_order_cancelled(pool, order.id()).await {
                error!(error = %error, "Failed to mark stale order as cancelled");
                return 0;
            }
            if let Err(error) = database::insert_reconciliation_event(
                pool,
                &ReconciliationEventType::StaleSubmittedOrder,
                order.ticker(),
                None,
                None,
                None,
                Some(order.alpaca_order_id()),
                &ReconciliationAction::CancelledStaleOrder,
                Some(Utc::now()),
            )
            .await
            {
                error!(error = %error, "Failed to persist stale_submitted_order event");
            }
            1
        }
        Ok(fill) => {
            // Order is still open on Alpaca — attempt to cancel it.
            warn!(
                alpaca_order_id = order.alpaca_order_id(),
                status = fill.status.as_str(),
                "Stale submitted order still open; attempting cancel"
            );
            match alpaca.cancel_order(order.alpaca_order_id()).await {
                Ok(true) => {
                    if let Err(error) = database::mark_order_cancelled(pool, order.id()).await {
                        error!(error = %error, "Failed to mark cancelled stale order in DB; will retry");
                        return 0;
                    }
                    if let Err(error) = database::insert_reconciliation_event(
                        pool,
                        &ReconciliationEventType::StaleSubmittedOrder,
                        order.ticker(),
                        None,
                        None,
                        None,
                        Some(order.alpaca_order_id()),
                        &ReconciliationAction::CancelledStaleOrder,
                        Some(Utc::now()),
                    )
                    .await
                    {
                        error!(error = %error, "Failed to persist stale_submitted_order event");
                    }
                    1
                }
                Ok(false) => {
                    // Order is in terminal state now (race between get_order and cancel).
                    // Re-check on next reconciliation pass.
                    warn!(
                        alpaca_order_id = order.alpaca_order_id(),
                        "Cancel returned false; will retry on next reconciliation pass"
                    );
                    0
                }
                Err(error) => {
                    error!(
                        alpaca_order_id = order.alpaca_order_id(),
                        error = %error,
                        "Failed to cancel stale order"
                    );
                    0
                }
            }
        }
        Err(error) => {
            error!(
                alpaca_order_id = order.alpaca_order_id(),
                error = %error,
                "Failed to query Alpaca for stale order status"
            );
            0
        }
    }
}

/// Returns `true` for Alpaca order statuses that are terminal but not filled.
fn is_terminal_non_filled(status: &str) -> bool {
    matches!(
        status,
        "cancelled" | "expired" | "rejected" | "replaced" | "canceled"
    )
}

/// Retries a prior compensation failure by attempting to close the position.
/// Returns 1 if the retry succeeded and the event was resolved, 0 otherwise.
async fn retry_compensation_failure(
    pool: &PgPool,
    alpaca: &dyn Trading,
    event: &UnresolvedReconciliationEvent,
) -> usize {
    info!(
        event_id = event.id(),
        ticker = event.ticker(),
        "Retrying compensation failure"
    );

    // First try to cancel the order if we have an order ID.
    if let Some(alpaca_order_id) = event.alpaca_order_id() {
        match alpaca.cancel_order(alpaca_order_id).await {
            Ok(true) => {
                info!(
                    alpaca_order_id = alpaca_order_id,
                    "Orphaned order cancelled on retry"
                );
                if let Err(error) = database::resolve_reconciliation_event(pool, event.id()).await {
                    error!(error = %error, "Failed to resolve compensation event after cancel; will retry");
                    return 0;
                }
                return 1;
            }
            Ok(false) => {
                // Terminal state — fall through to close position.
            }
            Err(error) => {
                warn!(error = %error, "Cancel retry failed; trying close_position");
            }
        }
    }

    // Fall back to closing the position directly.
    match alpaca.close_position(event.ticker()).await {
        Ok(_) => {
            info!(ticker = event.ticker(), "Orphaned position closed on retry");
            if let Err(error) = database::resolve_reconciliation_event(pool, event.id()).await {
                error!(error = %error, "Failed to resolve compensation event after close; will retry");
                return 0;
            }
            1
        }
        Err(error) => {
            error!(
                ticker = event.ticker(),
                event_id = event.id(),
                error = %error,
                "Compensation retry failed again"
            );
            0
        }
    }
}

/// Error from the reconciliation process.
#[derive(Debug)]
pub enum ReconciliationError {
    /// Failed to fetch positions from Alpaca.
    AlpacaFetch(crate::portfolio::alpaca::ClientError),
    /// Database query failed.
    Database(sqlx::Error),
}

impl std::fmt::Display for ReconciliationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlpacaFetch(error) => {
                write!(formatter, "Alpaca position fetch failed: {error}")
            }
            Self::Database(error) => {
                write!(formatter, "Database error during reconciliation: {error}")
            }
        }
    }
}

impl std::error::Error for ReconciliationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_terminal_non_filled_recognizes_all_terminal_states() {
        assert!(is_terminal_non_filled("cancelled"));
        assert!(is_terminal_non_filled("canceled"));
        assert!(is_terminal_non_filled("expired"));
        assert!(is_terminal_non_filled("rejected"));
        assert!(is_terminal_non_filled("replaced"));
    }

    #[test]
    fn test_is_terminal_non_filled_rejects_non_terminal() {
        assert!(!is_terminal_non_filled("new"));
        assert!(!is_terminal_non_filled("partially_filled"));
        assert!(!is_terminal_non_filled("filled"));
        assert!(!is_terminal_non_filled("accepted"));
    }

    #[test]
    fn test_reconciliation_report_default_zeros() {
        let report = ReconciliationReport {
            orphans_closed: 0,
            pairs_marked_closed: 0,
            partial_positions_closed: 0,
            stale_orders_resolved: 0,
            compensation_retries: 0,
        };
        assert_eq!(report.orphans_closed, 0);
        assert_eq!(report.pairs_marked_closed, 0);
        assert_eq!(report.partial_positions_closed, 0);
        assert_eq!(report.stale_orders_resolved, 0);
        assert_eq!(report.compensation_retries, 0);
    }

    #[test]
    fn test_reconciliation_error_display_alpaca_fetch() {
        let error = ReconciliationError::AlpacaFetch(crate::portfolio::alpaca::ClientError::Parse(
            "connection refused".to_string(),
        ));
        let message = format!("{error}");
        assert!(message.contains("Alpaca position fetch failed"));
        assert!(message.contains("connection refused"));
    }

    #[test]
    fn test_reconciliation_error_display_database() {
        let error = ReconciliationError::Database(sqlx::Error::RowNotFound);
        let message = format!("{error}");
        assert!(message.contains("Database error during reconciliation"));
    }

    #[test]
    fn test_reconciliation_error_is_error_trait() {
        let error = ReconciliationError::AlpacaFetch(crate::portfolio::alpaca::ClientError::Parse(
            "test".to_string(),
        ));
        let _boxed: Box<dyn std::error::Error> = Box::new(error);
    }

    #[test]
    fn test_stale_order_threshold_is_reasonable() {
        const _: () = assert!(STALE_ORDER_THRESHOLD_SECONDS >= 30);
        const _: () = assert!(STALE_ORDER_THRESHOLD_SECONDS <= 300);
    }
}
