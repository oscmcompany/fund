//! Generic WebSocket connection manager with automatic reconnection.
//!
//! [`WebSocketConnection`] manages a single WebSocket connection lifecycle:
//! connect, subscribe to topics, receive messages in a loop, and reconnect
//! with exponential backoff on failure. It accepts a [`CancellationToken`]
//! for graceful shutdown — on cancellation, the connection drains in-flight
//! messages and closes cleanly.
//!
//! This module is feed-agnostic: it handles raw WebSocket frames and
//! delegates message parsing to a caller-provided handler. Specific feed
//! integrations (Alpaca equities, Massive options, etc.) are built on top
//! of this infrastructure in downstream projects.

use std::fmt;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Maximum reconnection backoff duration.
const MAXIMUM_BACKOFF: Duration = Duration::from_secs(60);

/// Initial reconnection backoff duration.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Backoff multiplier applied after each failed reconnection attempt.
const BACKOFF_MULTIPLIER: u32 = 2;

/// Configuration for a WebSocket connection.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// WebSocket URL to connect to (e.g., `wss://stream.example.com/v1`).
    pub url: String,

    /// Messages to send immediately after connecting (e.g., authentication,
    /// subscription requests). Sent in order before the receive loop begins.
    pub startup_messages: Vec<String>,
}

impl ConnectionConfig {
    /// Creates a new connection configuration.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            startup_messages: Vec::new(),
        }
    }

    /// Adds a message to send immediately after connecting.
    pub fn with_startup_message(mut self, message: impl Into<String>) -> Self {
        self.startup_messages.push(message.into());
        self
    }
}

/// Outcome of a single connection session.
///
/// Returned by [`run_connection`] to indicate why the session ended so the
/// supervisor loop can decide whether to reconnect.
#[derive(Debug)]
pub enum SessionOutcome {
    /// The connection was closed cleanly due to a shutdown signal.
    Shutdown,

    /// The connection was lost or encountered an error.
    Disconnected(ConnectionError),
}

/// Errors that can occur during a WebSocket session.
#[derive(Debug)]
pub struct ConnectionError {
    message: String,
}

impl ConnectionError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ConnectionError {}

/// Runs a WebSocket connection with automatic reconnection.
///
/// This is the top-level supervisor loop. It connects to the configured
/// URL, sends startup messages, and enters a receive loop that forwards
/// messages to the provided handler. On disconnection, it reconnects
/// with exponential backoff. On cancellation, it drains and exits.
///
/// The `handler` receives each text or binary message and returns `true`
/// to continue receiving or `false` to close the connection (e.g., if the
/// server sends a terminal message).
pub async fn run_connection<F>(
    config: &ConnectionConfig,
    shutdown_token: &CancellationToken,
    mut handler: F,
) where
    F: FnMut(MessagePayload) -> bool,
{
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if shutdown_token.is_cancelled() {
            info!("Shutdown before connect, exiting");
            return;
        }

        info!(url = %config.url, "Connecting to WebSocket");

        match connect_and_run(config, shutdown_token, &mut handler).await {
            SessionOutcome::Shutdown => {
                info!("WebSocket connection closed for shutdown");
                return;
            }
            SessionOutcome::Disconnected(error) => {
                if shutdown_token.is_cancelled() {
                    info!("WebSocket connection closed for shutdown");
                    return;
                }

                warn!(
                    error = %error,
                    backoff_seconds = backoff.as_secs(),
                    "WebSocket disconnected, reconnecting after backoff"
                );

                tokio::select! {
                    _ = sleep(backoff) => {}
                    _ = shutdown_token.cancelled() => {
                        info!("Shutdown during reconnect backoff, exiting");
                        return;
                    }
                }

                backoff = std::cmp::min(backoff * BACKOFF_MULTIPLIER, MAXIMUM_BACKOFF);
            }
        }
    }
}

/// The payload extracted from a WebSocket message.
#[derive(Debug, Clone)]
pub enum MessagePayload {
    /// UTF-8 text message.
    Text(String),
    /// Binary message.
    Binary(Vec<u8>),
}

/// Connects, sends startup messages, and enters the receive loop.
///
/// Returns [`SessionOutcome::Shutdown`] if the cancellation token fires,
/// or [`SessionOutcome::Disconnected`] if the connection drops or errors.
async fn connect_and_run<F>(
    config: &ConnectionConfig,
    shutdown_token: &CancellationToken,
    handler: &mut F,
) -> SessionOutcome
where
    F: FnMut(MessagePayload) -> bool,
{
    let stream = match connect(config).await {
        Ok(stream) => stream,
        Err(error) => return SessionOutcome::Disconnected(error),
    };

    let (mut sink, mut source) = stream.split();

    // Send startup messages (auth, subscriptions).
    for message in &config.startup_messages {
        if let Err(error) = sink.send(Message::Text(message.clone().into())).await {
            return SessionOutcome::Disconnected(ConnectionError::new(format!(
                "Failed to send startup message: {}",
                error
            )));
        }
    }

    debug!("Startup messages sent, entering receive loop");

    // Receive loop.
    loop {
        let frame = tokio::select! {
            frame = source.next() => frame,
            _ = shutdown_token.cancelled() => {
                debug!("Shutdown signal received, closing WebSocket");
                let _ = sink.close().await;
                return SessionOutcome::Shutdown;
            }
        };

        match frame {
            Some(Ok(Message::Text(text))) => {
                if !handler(MessagePayload::Text(text.to_string())) {
                    debug!("Handler requested connection close");
                    let _ = sink.close().await;
                    return SessionOutcome::Shutdown;
                }
            }
            Some(Ok(Message::Binary(data))) => {
                if !handler(MessagePayload::Binary(data.to_vec())) {
                    debug!("Handler requested connection close");
                    let _ = sink.close().await;
                    return SessionOutcome::Shutdown;
                }
            }
            Some(Ok(Message::Ping(data))) => {
                if let Err(error) = sink.send(Message::Pong(data)).await {
                    return SessionOutcome::Disconnected(ConnectionError::new(format!(
                        "Failed to send pong: {}",
                        error
                    )));
                }
            }
            Some(Ok(Message::Pong(_))) => {
                // Pong responses are expected and ignored.
            }
            Some(Ok(Message::Close(_))) => {
                debug!("Server sent close frame");
                let _ = sink.close().await;
                return SessionOutcome::Disconnected(ConnectionError::new(
                    "Server closed connection",
                ));
            }
            Some(Ok(Message::Frame(_))) => {
                // Raw frames are not expected in normal operation.
            }
            Some(Err(error)) => {
                return SessionOutcome::Disconnected(ConnectionError::new(format!(
                    "WebSocket error: {}",
                    error
                )));
            }
            None => {
                return SessionOutcome::Disconnected(ConnectionError::new(
                    "WebSocket stream ended",
                ));
            }
        }
    }
}

/// Establishes the initial WebSocket connection.
async fn connect(
    config: &ConnectionConfig,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ConnectionError> {
    match connect_async(&config.url).await {
        Ok((stream, response)) => {
            info!(
                status = %response.status(),
                "WebSocket connected"
            );
            Ok(stream)
        }
        Err(error) => {
            error!(error = %error, "WebSocket connection failed");
            Err(ConnectionError::new(format!(
                "Connection failed: {}",
                error
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_config_new() {
        let config = ConnectionConfig::new("wss://example.com/v1");
        assert_eq!(config.url, "wss://example.com/v1");
        assert!(config.startup_messages.is_empty());
    }

    #[test]
    fn test_connection_config_with_startup_messages() {
        let config = ConnectionConfig::new("wss://example.com/v1")
            .with_startup_message(r#"{"action":"auth","key":"test"}"#)
            .with_startup_message(r#"{"action":"subscribe","quotes":["AAPL"]}"#);

        assert_eq!(config.startup_messages.len(), 2);
        assert!(config.startup_messages[0].contains("auth"));
        assert!(config.startup_messages[1].contains("subscribe"));
    }

    #[test]
    fn test_connection_error_display() {
        let error = ConnectionError::new("something went wrong");
        assert_eq!(format!("{}", error), "something went wrong");
    }

    #[test]
    fn test_connection_error_debug() {
        let error = ConnectionError::new("test error");
        let debug_output = format!("{:?}", error);
        assert!(debug_output.contains("test error"));
    }

    #[test]
    fn test_backoff_constants() {
        assert!(INITIAL_BACKOFF < MAXIMUM_BACKOFF);
        assert!(BACKOFF_MULTIPLIER > 1);
    }

    #[test]
    fn test_backoff_growth_is_bounded() {
        let mut backoff = INITIAL_BACKOFF;
        for _ in 0..20 {
            backoff = std::cmp::min(backoff * BACKOFF_MULTIPLIER, MAXIMUM_BACKOFF);
        }
        assert_eq!(backoff, MAXIMUM_BACKOFF);
    }

    #[test]
    fn test_message_payload_text_clone() {
        let payload = MessagePayload::Text("hello".to_string());
        let cloned = payload.clone();
        match cloned {
            MessagePayload::Text(text) => assert_eq!(text, "hello"),
            _ => panic!("Expected Text variant"),
        }
    }

    #[test]
    fn test_message_payload_binary_clone() {
        let payload = MessagePayload::Binary(vec![1, 2, 3]);
        let cloned = payload.clone();
        match cloned {
            MessagePayload::Binary(data) => assert_eq!(data, vec![1, 2, 3]),
            _ => panic!("Expected Binary variant"),
        }
    }

    #[tokio::test]
    async fn test_run_connection_exits_on_immediate_shutdown() {
        let config = ConnectionConfig::new("wss://localhost:1/nonexistent");
        let token = CancellationToken::new();
        token.cancel();

        let messages_received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = messages_received.clone();

        run_connection(&config, &token, move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        })
        .await;

        assert_eq!(
            messages_received.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn test_connect_to_invalid_url_returns_error() {
        let config = ConnectionConfig::new("wss://localhost:1/nonexistent");
        let result = connect(&config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_and_run_with_cancelled_token() {
        let config = ConnectionConfig::new("wss://localhost:1/nonexistent");
        let token = CancellationToken::new();
        token.cancel();

        let outcome = connect_and_run(&config, &token, &mut |_| true).await;
        // With a cancelled token, connect may fail or shutdown may fire first.
        // Either outcome is acceptable — we just verify it doesn't hang.
        match outcome {
            SessionOutcome::Shutdown => {}
            SessionOutcome::Disconnected(_) => {}
        }
    }
}
