//! The noop consumer.

use rust_template::ingestion::noop::NoopBatchConsumer;
use rust_template::ingestion::{BatchConsumer, ConnectionState};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn noop_consumer_idles_connected_until_closed() {
    let consumer = Arc::new(NoopBatchConsumer::new());
    assert_eq!(consumer.connection_state(), ConnectionState::Connected);
    assert_eq!(consumer.bound_epoch(), 1);
    let pending = tokio::spawn({
        let c = consumer.clone();
        async move { c.next_delivery().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !pending.is_finished(),
        "blocks: no transport, no deliveries"
    );
    consumer.close().await;
    let next = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("close unblocks next_delivery")
        .unwrap()
        .unwrap();
    assert!(next.is_none());
    assert_eq!(consumer.connection_state(), ConnectionState::Down);
    // After close, next_delivery returns at once.
    assert!(consumer.next_delivery().await.unwrap().is_none());
}

#[test]
fn connection_state_serialises_as_python_lowercase() {
    for (state, text) in [
        (ConnectionState::Connected, "connected"),
        (ConnectionState::Reconnecting, "reconnecting"),
        (ConnectionState::Down, "down"),
    ] {
        assert_eq!(state.as_str(), text);
        assert_eq!(serde_json::to_value(state).unwrap(), text);
    }
}
