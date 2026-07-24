//! Integration tests for the stream module.
//!
//! Verifies the "drop in" property: buffer lifecycle, connection cancellation,
//! and subscriber shutdown behavior.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use fund::stream::buffer::MarketDataBuffer;
use fund::stream::connection::{ConnectionConfiguration, MessagePayload};

#[tokio::test]
async fn test_buffer_subscriber_receives_none_on_drop() {
    let buffer: MarketDataBuffer<String> = MarketDataBuffer::new();
    let mut subscriber = buffer.subscribe();

    // Publish a message, then drop the buffer.
    buffer.publish("hello".to_string()).unwrap();
    let received = subscriber.receive().await;
    assert_eq!(received, Some("hello".to_string()));

    drop(buffer);

    // After the buffer is dropped, the subscriber should receive None.
    let received = subscriber.receive().await;
    assert_eq!(received, None);
}

#[tokio::test]
async fn test_buffer_lifecycle_with_spawned_subscriber() {
    let buffer: Arc<MarketDataBuffer<i32>> = Arc::new(MarketDataBuffer::new());
    let mut subscriber = buffer.subscribe();
    let received_count = Arc::new(AtomicUsize::new(0));
    let received_count_clone = received_count.clone();

    // Spawn a subscriber task that counts received messages.
    let subscriber_handle = tokio::spawn(async move {
        while let Some(_message) = subscriber.receive().await {
            received_count_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    // Publish messages.
    for index in 0..10 {
        buffer.publish(index).unwrap();
    }

    // Small delay to let the subscriber process messages.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drop the buffer — subscriber should exit.
    drop(buffer);
    subscriber_handle.await.unwrap();

    assert_eq!(received_count.load(Ordering::SeqCst), 10);
}

#[tokio::test]
async fn test_multiple_subscribers_all_receive_none_on_drop() {
    let buffer: MarketDataBuffer<String> = MarketDataBuffer::new();
    let mut subscriber_a = buffer.subscribe();
    let mut subscriber_b = buffer.subscribe();

    drop(buffer);

    assert_eq!(subscriber_a.receive().await, None);
    assert_eq!(subscriber_b.receive().await, None);
}

#[tokio::test]
async fn test_run_connection_exits_cleanly_on_cancellation() {
    let config = ConnectionConfiguration::new("wss://localhost:1/nonexistent");
    let token = CancellationToken::new();

    // Cancel immediately — run_connection should exit without hanging.
    token.cancel();

    let messages_received = Arc::new(AtomicUsize::new(0));
    let counter = messages_received.clone();

    fund::stream::connection::run_connection(&config, &token, move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        true
    })
    .await;

    assert_eq!(messages_received.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_run_connection_cancellation_during_backoff() {
    let config = ConnectionConfiguration::new("wss://localhost:1/nonexistent");
    let token = CancellationToken::new();

    // Spawn connection — it will fail to connect and enter backoff.
    let token_clone = token.clone();
    let handle = tokio::spawn(async move {
        fund::stream::connection::run_connection(&config, &token_clone, |_| true).await;
    });

    // Give it time to attempt connection and enter backoff.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel during backoff — should exit promptly.
    token.cancel();

    // Should complete within a reasonable time (not wait for full backoff).
    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        result.is_ok(),
        "Connection should exit promptly on cancellation during backoff"
    );
}

#[tokio::test]
async fn test_buffer_message_payload_lifecycle() {
    let buffer: MarketDataBuffer<MessagePayload> = MarketDataBuffer::new();
    let mut subscriber = buffer.subscribe();

    buffer
        .publish(MessagePayload::Text("market data".to_string()))
        .unwrap();
    buffer
        .publish(MessagePayload::Binary(vec![1, 2, 3]))
        .unwrap();

    match subscriber.receive().await {
        Some(MessagePayload::Text(text)) => assert_eq!(text, "market data"),
        other => panic!("Expected Text, got {:?}", other),
    }

    match subscriber.receive().await {
        Some(MessagePayload::Binary(data)) => assert_eq!(data, vec![1, 2, 3]),
        other => panic!("Expected Binary, got {:?}", other),
    }

    drop(buffer);
    assert_eq!(subscriber.receive().await, None);
}
