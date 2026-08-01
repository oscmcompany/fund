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
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// How long to wait before reconnecting a dropped listener.
///
/// The listener is the only path by which work arrives, so a permanently dead one is a silently
/// idle service. Reconnecting on a fixed short delay is deliberate: there is nothing to back off
/// from — a local database that is down will come back, and the cost of asking every few seconds is
/// nothing next to missing a liquidation.
const LISTENER_RECONNECT_DELAY: Duration = Duration::from_secs(5);

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

    listen(state, shutdown).await;

    info!("Service stopped");
    Ok(())
}

/// Listens for event notifications until cancelled, reconnecting if the connection drops.
async fn listen(state: Arc<ServiceState>, shutdown: CancellationToken) {
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
                    Ok(notification) => dispatch(&state, notification.payload()).await,
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
async fn dispatch(state: &Arc<ServiceState>, payload: &str) {
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
    tokio::spawn(async move { handlers::handle(&state, command).await });
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
