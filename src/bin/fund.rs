//! The service: one process, woken entirely by pg_cron.
//!
//! There is no `--module` flag, no in-process scheduler, and no WebSocket. The process connects,
//! replays anything today's cron fired while it was down, then listens on the `events` channel and
//! dispatches. Every unit of work is a row somebody else wrote.
//!
//! Each notification is handled on its own task. The five-minute evaluation and a post-close sync
//! can legitimately overlap, and serializing the listener would mean a slow export delaying an
//! exit. Two instances of the *same* command cannot overlap; that is the in-flight claim in
//! [`fund::handlers`].

use std::sync::Arc;
use std::time::Duration;

use fund::common::events::{self, Notification, Outcome};
use fund::common::{crypto, database, observability};
use fund::handlers::{self, ServiceState};
use sqlx::postgres::PgListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// How long to wait before reconnecting a dropped listener.
///
/// The listener is the only path by which work arrives, so a permanently dead one is a silently
/// idle service. Reconnecting on a fixed short delay is deliberate: there is nothing to back off
/// from — a local database that is down will come back, and the cost of asking every few seconds is
/// nothing next to missing a liquidation.
const LISTENER_RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// How long to wait for running handlers after shutdown is requested.
///
/// The handlers that matter here are the ones with side effects part-applied: a liquidation between
/// two broker orders, an export between S3 and the purge, or any handler that has not yet written
/// its terminal event. Dropping one of those mid-sequence leaves a `_requested` row with no outcome
/// while the process logs a clean stop.
///
/// Sixty seconds is longer than any handler should take and shorter than a process supervisor's
/// patience. A handler still running at the deadline is reported rather than waited on further,
/// because at that point the more useful thing is a log line naming it.
const HANDLER_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crypto::install_default_crypto_provider();
    let _log_guard = observability::init_tracing("fund.log", None, "fund");

    let pool = database::connect_pool().await?;
    let state = Arc::new(ServiceState::from_env(pool).await?);
    info!("Service starting");

    let shutdown = CancellationToken::new();
    spawn_signal_handler(shutdown.clone());

    // Recovery before the listener, not alongside it. A command replayed here and then notified
    // again by a late-arriving cron row is dropped by the in-flight claim, which is the correct
    // outcome; the reverse order would let the listener and the replay both start the same work
    // before either had claimed it.
    handlers::recover(Arc::clone(&state)).await;

    // Handlers are tracked rather than detached, so shutdown can wait for them. `tokio::spawn`
    // returns a handle nobody holds; when `main` returns, the runtime drops and every running task
    // is cancelled at its next await point.
    let mut handlers_in_flight = JoinSet::new();
    listen(state, shutdown, &mut handlers_in_flight).await;
    drain(handlers_in_flight, HANDLER_DRAIN_TIMEOUT).await;

    info!("Service stopped");
    Ok(())
}

/// Waits for running handlers to finish, up to `timeout`.
///
/// The timeout is a parameter rather than a constant read inside so the behaviour is testable
/// without a sixty-second test.
async fn drain(mut handlers_in_flight: JoinSet<()>, timeout: Duration) {
    let running = handlers_in_flight.len();
    if running == 0 {
        return;
    }

    info!(running, "Waiting for running handlers to finish");
    let drained = tokio::time::timeout(timeout, async {
        while handlers_in_flight.join_next().await.is_some() {}
    })
    .await;

    match drained {
        Ok(()) => info!(running, "Handlers finished"),
        Err(_) => warn!(
            remaining = handlers_in_flight.len(),
            timeout_seconds = timeout.as_secs(),
            "Handlers did not finish before the drain timeout; work may be incomplete"
        ),
    }
}

/// Listens for event notifications until cancelled, reconnecting if the connection drops.
async fn listen(
    state: Arc<ServiceState>,
    shutdown: CancellationToken,
    handlers_in_flight: &mut JoinSet<()>,
) {
    loop {
        if shutdown.is_cancelled() {
            return;
        }

        let mut listener = match PgListener::connect_with(state.pool()).await {
            Ok(listener) => listener,
            Err(error) => {
                error!(%error, "Could not open the event listener; retrying");
                if wait_or_shutdown(&shutdown, LISTENER_RECONNECT_DELAY).await {
                    return;
                }
                continue;
            }
        };

        if let Err(error) = listener.listen("events").await {
            error!(%error, "Could not subscribe to the events channel; retrying");
            if wait_or_shutdown(&shutdown, LISTENER_RECONNECT_DELAY).await {
                return;
            }
            continue;
        }

        info!("Listening for events");
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                received = listener.recv() => match received {
                    Ok(notification) => {
                        // Reap anything already finished so the set does not grow across a session.
                        while handlers_in_flight.try_join_next().is_some() {}
                        dispatch(&state, handlers_in_flight, notification.payload()).await
                    }
                    Err(error) => {
                        error!(%error, "Event listener dropped; reconnecting");
                        break;
                    }
                },
            }
        }

        if wait_or_shutdown(&shutdown, LISTENER_RECONNECT_DELAY).await {
            return;
        }
    }
}

/// Turns one notification payload into a handler call.
///
/// Only `_requested` events are acted on. The service writes the terminal outcomes itself, so
/// dispatching on one would re-run the work its own completion announced.
async fn dispatch(state: &Arc<ServiceState>, handlers_in_flight: &mut JoinSet<()>, payload: &str) {
    let Some(notification) = Notification::parse(payload) else {
        warn!(payload, "Unrecognized notification; ignoring");
        return;
    };

    if notification.event_type.outcome() != Outcome::Requested {
        return;
    }

    // The payload of a request carries nothing a handler reads today, but a truncated one is worth
    // saying out loud: it means a request grew past the notification limit, which is a shape change
    // the handler contract does not expect.
    if notification.payload_truncated {
        match events::fetch_payload(state.pool(), notification.event_id).await {
            Ok(payload) => warn!(
                event_id = notification.event_id,
                %payload,
                "Request notification was truncated; read the payload from the row"
            ),
            Err(error) => warn!(
                event_id = notification.event_id,
                %error,
                "Request notification was truncated and the row could not be read"
            ),
        }
    }

    let state = Arc::clone(state);
    let command = notification.event_type.command();
    handlers_in_flight.spawn(async move { handlers::handle(&state, command).await });
}

/// Sleeps, or returns `true` if shutdown came first.
async fn wait_or_shutdown(shutdown: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

/// Cancels the token on `SIGINT` or `SIGTERM`.
///
/// `SIGTERM` as well as `SIGINT` because devenv's process manager stops the service with the
/// former, and a handler that only catches Ctrl-C is one that never runs in production.
fn spawn_signal_handler(shutdown: CancellationToken) {
    tokio::spawn(async move {
        let mut interrupt = match signal(SignalKind::interrupt()) {
            Ok(stream) => stream,
            Err(error) => {
                error!(%error, "Could not install the SIGINT handler");
                return;
            }
        };
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                error!(%error, "Could not install the SIGTERM handler");
                return;
            }
        };

        tokio::select! {
            _ = interrupt.recv() => info!("SIGINT received; shutting down"),
            _ = terminate.recv() => info!("SIGTERM received; shutting down"),
        }
        shutdown.cancel();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;

    /// The property the whole change exists for: work still running when shutdown is requested runs
    /// to completion rather than being dropped at its next await point. Before this, `tokio::spawn`
    /// detached the task and the runtime cancelled it when `main` returned — truncating a
    /// liquidation between broker orders, or a handler before it wrote its terminal event.
    #[tokio::test]
    async fn test_drain_waits_for_a_running_handler_to_finish() {
        let finished = StdArc::new(AtomicUsize::new(0));
        let mut handlers_in_flight = JoinSet::new();

        for _ in 0..3 {
            let finished = StdArc::clone(&finished);
            handlers_in_flight.spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                finished.fetch_add(1, Ordering::SeqCst);
            });
        }

        drain(handlers_in_flight, Duration::from_secs(5)).await;
        assert_eq!(
            finished.load(Ordering::SeqCst),
            3,
            "every handler must have completed before drain returned"
        );
    }

    /// A handler that will not finish must not hold the process open indefinitely. The bound is what
    /// makes the drain safe to await unconditionally.
    #[tokio::test]
    async fn test_drain_gives_up_at_the_timeout() {
        let mut handlers_in_flight = JoinSet::new();
        handlers_in_flight.spawn(async {
            tokio::time::sleep(Duration::from_secs(3_600)).await;
        });

        let started = tokio::time::Instant::now();
        drain(handlers_in_flight, Duration::from_millis(50)).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "drain must return at its timeout rather than waiting on a stuck handler"
        );
    }

    #[tokio::test]
    async fn test_drain_returns_immediately_when_nothing_is_running() {
        let started = tokio::time::Instant::now();
        drain(JoinSet::new(), Duration::from_secs(60)).await;
        assert!(started.elapsed() < Duration::from_millis(50));
    }
}
