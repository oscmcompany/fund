//! In-memory hot buffer for live market data.
//!
//! [`MarketDataBuffer`] wraps a [`tokio::sync::broadcast`] channel where
//! WebSocket readers publish raw market data and downstream consumers
//! subscribe. All data in this buffer is [`DataBoundary::Ephemeral`] —
//! it lives only during the current process lifetime and is never written
//! to PostgreSQL.

use std::fmt;

use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::data_boundary::DataBoundary;

/// Default broadcast channel capacity.
///
/// Sized for sustained quote throughput during market hours. A lagging
/// consumer that falls more than this many messages behind will receive
/// a [`broadcast::error::RecvError::Lagged`] error and skip to the
/// current position.
const DEFAULT_BUFFER_CAPACITY: usize = 16_384;

/// In-memory broadcast buffer for live market data.
///
/// Owns the [`broadcast::Sender`] and provides [`subscribe`](Self::subscribe)
/// to create new receivers. Multiple consumers can subscribe independently
/// and each receives every message published after their subscription.
///
/// The buffer enforces the [`DataBoundary::Ephemeral`] contract: data
/// published here is never persisted. Downstream consumers that detect
/// a durable signal are responsible for crossing the event boundary
/// themselves via [`crate::common::events::emit_event`].
pub struct MarketDataBuffer<T: Clone + Send + 'static> {
    sender: broadcast::Sender<T>,
    capacity: usize,
}

impl<T: Clone + Send + 'static> MarketDataBuffer<T> {
    /// Creates a new buffer with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    /// Creates a new buffer with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender, capacity }
    }

    /// Publishes a message to all active subscribers.
    ///
    /// Returns `Ok(receiver_count)` on success. Returns `Err` if there
    /// are no active subscribers — this is not an error condition during
    /// startup or shutdown when consumers may not yet be connected.
    pub fn publish(&self, message: T) -> Result<usize, PublishError<T>> {
        self.sender.send(message).map_err(|error| {
            debug!("No active subscribers for published message");
            PublishError(error.0)
        })
    }

    /// Creates a new subscriber that receives all future messages.
    ///
    /// Each subscriber independently tracks its position in the buffer.
    /// If a subscriber falls behind by more than the buffer capacity,
    /// it will receive a lag error and skip to the current position.
    pub fn subscribe(&self) -> BufferSubscriber<T> {
        BufferSubscriber {
            receiver: self.sender.subscribe(),
        }
    }

    /// Returns the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Returns the configured buffer capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the data boundary classification for this buffer.
    ///
    /// Always returns [`DataBoundary::Ephemeral`] — data in the broadcast
    /// channel is never persisted to PostgreSQL.
    pub fn data_boundary(&self) -> DataBoundary {
        DataBoundary::Ephemeral
    }
}

impl<T: Clone + Send + 'static> Default for MarketDataBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Error returned when publishing to a buffer with no active subscribers.
pub struct PublishError<T>(pub T);

impl<T> fmt::Debug for PublishError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PublishError(no active subscribers)")
    }
}

impl<T> fmt::Display for PublishError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("no active subscribers")
    }
}

impl<T: fmt::Debug> std::error::Error for PublishError<T> {}

/// A subscriber to a [`MarketDataBuffer`].
///
/// Wraps a [`broadcast::Receiver`] and handles lag errors transparently
/// by logging a warning and skipping to the current buffer position.
pub struct BufferSubscriber<T: Clone + Send + 'static> {
    receiver: broadcast::Receiver<T>,
}

impl<T: Clone + Send + 'static> BufferSubscriber<T> {
    /// Receives the next message from the buffer.
    ///
    /// If the subscriber has fallen behind, skipped messages are logged
    /// and the subscriber advances to the current position. Returns
    /// `None` when the buffer has been dropped (all senders closed),
    /// indicating shutdown.
    pub async fn receive(&mut self) -> Option<T> {
        loop {
            match self.receiver.recv().await {
                Ok(message) => return Some(message),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        skipped_messages = skipped,
                        "Buffer subscriber lagged, skipping to current position"
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_buffer_has_default_capacity() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        assert_eq!(buffer.capacity(), DEFAULT_BUFFER_CAPACITY);
    }

    #[test]
    fn test_with_capacity() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::with_capacity(256);
        assert_eq!(buffer.capacity(), 256);
    }

    #[test]
    fn test_default_matches_new() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::default();
        assert_eq!(buffer.capacity(), DEFAULT_BUFFER_CAPACITY);
    }

    #[test]
    fn test_subscriber_count_starts_at_zero() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        assert_eq!(buffer.subscriber_count(), 0);
    }

    #[test]
    fn test_subscribe_increments_count() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        let _subscriber_a = buffer.subscribe();
        assert_eq!(buffer.subscriber_count(), 1);
        let _subscriber_b = buffer.subscribe();
        assert_eq!(buffer.subscriber_count(), 2);
    }

    #[test]
    fn test_dropped_subscriber_decrements_count() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        let subscriber = buffer.subscribe();
        assert_eq!(buffer.subscriber_count(), 1);
        drop(subscriber);
        assert_eq!(buffer.subscriber_count(), 0);
    }

    #[test]
    fn test_publish_with_no_subscribers_returns_error() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        let result = buffer.publish(42);
        assert!(result.is_err());
    }

    #[test]
    fn test_publish_returns_subscriber_count() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        let _subscriber = buffer.subscribe();
        let result = buffer.publish(42);
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_data_boundary_is_ephemeral() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        assert_eq!(buffer.data_boundary(), DataBoundary::Ephemeral);
        assert!(buffer.data_boundary().is_ephemeral());
        assert!(!buffer.data_boundary().is_durable());
    }

    #[tokio::test]
    async fn test_subscriber_receives_published_message() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        let mut subscriber = buffer.subscribe();
        buffer.publish(42).unwrap();
        let received = subscriber.receive().await;
        assert_eq!(received, Some(42));
    }

    #[tokio::test]
    async fn test_multiple_subscribers_receive_same_message() {
        let buffer: MarketDataBuffer<String> = MarketDataBuffer::new();
        let mut subscriber_a = buffer.subscribe();
        let mut subscriber_b = buffer.subscribe();
        buffer.publish("hello".to_string()).unwrap();
        assert_eq!(subscriber_a.receive().await, Some("hello".to_string()));
        assert_eq!(subscriber_b.receive().await, Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_subscriber_receives_none_on_buffer_drop() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        let mut subscriber = buffer.subscribe();
        drop(buffer);
        let received = subscriber.receive().await;
        assert_eq!(received, None);
    }

    #[tokio::test]
    async fn test_subscriber_handles_lag() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::with_capacity(4);
        let mut subscriber = buffer.subscribe();

        // Publish more messages than the buffer capacity to force lag.
        for index in 0..10 {
            let _ = buffer.publish(index);
        }

        // The subscriber should skip lagged messages and receive the
        // most recent ones still in the buffer.
        let received = subscriber.receive().await;
        assert!(received.is_some());
    }

    #[tokio::test]
    async fn test_subscriber_receives_messages_in_order() {
        let buffer: MarketDataBuffer<i32> = MarketDataBuffer::new();
        let mut subscriber = buffer.subscribe();

        buffer.publish(1).unwrap();
        buffer.publish(2).unwrap();
        buffer.publish(3).unwrap();

        assert_eq!(subscriber.receive().await, Some(1));
        assert_eq!(subscriber.receive().await, Some(2));
        assert_eq!(subscriber.receive().await, Some(3));
    }

    #[test]
    fn test_publish_error_debug_format() {
        let error = PublishError(42);
        assert_eq!(
            format!("{:?}", error),
            "PublishError(no active subscribers)"
        );
    }

    #[test]
    fn test_publish_error_display_format() {
        let error = PublishError(42);
        assert_eq!(format!("{}", error), "no active subscribers");
    }
}
