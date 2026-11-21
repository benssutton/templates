//! SolaceBatchConsumer against the real broker (Python
//! tests/test_solace_consumer.py): provisioning, settlement outcomes, poison
//! messages, bind denial and lifecycle. Broker-side facts come from SEMP.

use crate::support::batches::make_batch;
use crate::support::containers::{self, Endpoint, SolaceLease, wait_until};
use crate::support::logs;
use crate::support::semp::Semp;
use crate::support::solace::{
    SolaceNames, consumer_settings, publisher, wait_for_bound, wait_for_depth, wait_for_unbound,
};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use rust_template::ingestion::solace::{RECONNECT_INTERVAL, SolaceBatchConsumer};
use rust_template::ingestion::{BatchConsumer, ConnectionState, Delivery};
use rust_template::settings::Settings;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

type Pending = JoinHandle<anyhow::Result<Option<Delivery>>>;

async fn consumer(settings: &Settings) -> Arc<SolaceBatchConsumer> {
    Arc::new(
        SolaceBatchConsumer::connect(settings)
            .await
            .expect("connect"),
    )
}

/// next_delivery() in the background, as the ingest loop would call it.
fn spawn_next(consumer: &Arc<SolaceBatchConsumer>) -> Pending {
    let consumer = consumer.clone();
    tokio::spawn(async move { consumer.next_delivery().await })
}

async fn delivered(pending: Pending) -> Delivery {
    tokio::time::timeout(Duration::from_secs(20), pending)
        .await
        .expect("a delivery within 20 s")
        .unwrap()
        .expect("next_delivery ok")
        .expect("a delivery, not closed")
}

fn first_id(delivery: &Delivery) -> i64 {
    delivery.batches[0]
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>()
        .value(0)
}

/// Connect, start the first next_delivery (which binds), wait until bound.
async fn bound(
    lease: &SolaceLease,
    names: &SolaceNames,
    semp: &Semp,
) -> (Arc<SolaceBatchConsumer>, Pending) {
    let c = consumer(&consumer_settings(&lease.endpoints, names)).await;
    let pending = spawn_next(&c);
    wait_for_bound(semp, &names.queue).await;
    (c, pending)
}

#[tokio::test]
async fn binding_provisions_the_queue_with_its_topic_subscription_and_close_unbinds() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    assert_eq!(
        semp.queue_subscriptions(&names.queue).await.unwrap(),
        std::slice::from_ref(&names.topic)
    );
    assert_eq!(c.connection_state(), ConnectionState::Connected);
    assert_eq!(c.bound_epoch(), 1, "one successful bind");

    let started = Instant::now();
    c.close().await;
    let next = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .expect("unblocked")
        .unwrap()
        .unwrap();
    assert!(next.is_none());
    assert!(started.elapsed() < Duration::from_secs(10));
    wait_for_unbound(&semp, &names.queue).await;
    assert!(
        semp.queue_exists(&names.queue).await.unwrap(),
        "close detaches; the durable queue stays"
    );
    assert_eq!(c.connection_state(), ConnectionState::Down);
}

#[tokio::test]
async fn accept_removes_the_message_from_the_queue() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[(1, "a", "v1", "upsert")]))
        .await
        .unwrap();

    let d = delivered(pending).await;
    assert_eq!(d.batches[0].num_rows(), 1);
    assert!(!d.redelivered);
    assert_eq!(
        semp.queue_message_count(&names.queue).await.unwrap(),
        1,
        "delivered but unsettled: still spooled"
    );
    d.settle.accept().await.unwrap();
    wait_for_depth(&semp, &names.queue, 0).await;
    p.close().await;
    c.close().await;
}

#[tokio::test]
async fn fail_redelivers_the_message_marked_redelivered() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[(1, "a", "v1", "upsert")]))
        .await
        .unwrap();

    delivered(pending).await.settle.fail().await.unwrap();
    let again = delivered(spawn_next(&c)).await;
    assert!(
        again.redelivered,
        "delivery-count incremented by modified(delivery_failed)"
    );
    assert_eq!(first_id(&again), 1);
    again.settle.accept().await.unwrap();
    wait_for_depth(&semp, &names.queue, 0).await;
    p.close().await;
    c.close().await;
}

#[tokio::test]
async fn reject_removes_the_message_without_redelivery() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[(1, "a", "v1", "upsert")]))
        .await
        .unwrap();

    delivered(pending).await.settle.reject().await.unwrap();
    wait_for_depth(&semp, &names.queue, 0).await;
    // Belt and braces: depth 0 already proves the broker will not redeliver.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), c.next_delivery())
            .await
            .is_err()
    );
    p.close().await;
    c.close().await;
}

/// Spec §6 malformed_payload_is_rejected_not_redelivered.
#[tokio::test]
async fn a_malformed_payload_is_rejected_by_the_consumer_and_never_handed_out() {
    let (logs, _guard) = logs::capture();
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_bytes(b"definitely not arrow ipc".to_vec())
        .await
        .unwrap();
    p.publish_batch(&make_batch(&[(9, "ok", "v1", "upsert")]))
        .await
        .unwrap();

    let d = delivered(pending).await;
    assert_eq!(
        first_id(&d),
        9,
        "the poison message was skipped, not handed out"
    );
    d.settle.accept().await.unwrap();
    wait_for_depth(&semp, &names.queue, 0).await;
    assert_eq!(
        semp.queue_stats(&names.queue).await.unwrap().redelivered,
        0,
        "the broker counted the redelivery"
    );
    assert!(
        logs.contains("WARN") && logs.contains("malformed IPC message rejected"),
        "{}",
        logs.text()
    );
    p.close().await;
    c.close().await;
}

#[tokio::test]
async fn close_before_the_first_delivery_returns_none_promptly() {
    let lease = containers::solace().await;
    let names = SolaceNames::new();
    let c = consumer(&consumer_settings(&lease.endpoints, &names)).await;
    c.close().await;
    let started = Instant::now();
    assert!(c.next_delivery().await.unwrap().is_none());
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// Queue bind denied, at the consumer level: a queue
/// the client may not consume from refuses the attach (UnauthorizedAccess,
/// "403 Permission Not Allowed"). The error is returned and the link is torn
/// down (the next call reopens it); the transport was never lost, so the
/// state stays Connected and the epoch does not rise.
#[tokio::test]
async fn a_denied_bind_is_an_error_and_the_link_is_torn_down() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    // An owner-less no-access queue; we bind anonymously as `default`, a
    // non-owner.
    semp.create_queue(&names.queue, "no-access", None)
        .await
        .unwrap();
    semp.add_subscription(&names.queue, &names.topic)
        .await
        .unwrap();
    let c = consumer(&consumer_settings(&lease.endpoints, &names)).await;
    let err = tokio::time::timeout(Duration::from_secs(30), c.next_delivery())
        .await
        .expect("an answer within 30 s")
        .err()
        .expect("bind refused");
    assert!(format!("{err:#}").contains("bind"), "{err:#}");
    assert_eq!(c.connection_state(), ConnectionState::Connected);
    assert_eq!(c.bound_epoch(), 0, "a refused bind is not a bind");
    c.close().await;
    semp.delete_queue(&names.queue).await;
}

/// Python test_abandoned_deliveries_unbinds_queue_and_rebind_redelivers.
#[tokio::test]
async fn an_unsettled_delivery_is_redelivered_after_a_rebind() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[(5, "x", "v1", "upsert")]))
        .await
        .unwrap();
    let first = delivered(pending).await;
    assert!(!first.redelivered);
    drop(first); // never settled
    c.close().await;
    wait_for_unbound(&semp, &names.queue).await;
    assert_eq!(semp.queue_message_count(&names.queue).await.unwrap(), 1);

    let (c2, pending) = bound(&lease, &names, &semp).await;
    let again = delivered(pending).await;
    assert!(again.redelivered);
    assert_eq!(first_id(&again), 5);
    again.settle.accept().await.unwrap();
    wait_for_depth(&semp, &names.queue, 0).await;
    p.close().await;
    c2.close().await;
}

#[tokio::test]
async fn connect_fails_fast_when_the_broker_is_unreachable() {
    let lease = containers::solace().await;
    let names = SolaceNames::new();
    let closed = Endpoint::closed();
    let settings = Settings {
        solace_port: closed.port,
        connect_max_attempts: 2,
        connect_base_delay: 0.01,
        connect_max_delay: 0.05,
        ..consumer_settings(&lease.endpoints, &names)
    };
    let err = SolaceBatchConsumer::connect(&settings)
        .await
        .err()
        .expect("must fail");
    assert!(format!("{err:#}").contains("Solace"), "{err:#}");
}

/// An operator shuts the queue's egress while bound: the broker unbinds the
/// flow (a recv error), and the consumer must not reopen at round-trip speed.
/// The reopen waits RECONNECT_INTERVAL; the re-attach is then refused
/// ("503 Queue Shutdown") and returned as an error, paced by the service's
/// backoff from there. A broker that accepted the attach and detached again
/// would otherwise spin with the state flipping Connected/Reconnecting.
#[tokio::test]
async fn a_lost_link_is_reopened_no_faster_than_the_reconnect_interval() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    // The clock starts before the PATCH: the unbind cannot precede it.
    let unbound_at = Instant::now();
    semp.set_queue_egress(&names.queue, false).await.unwrap();
    let err = tokio::time::timeout(Duration::from_secs(30), pending)
        .await
        .expect("an answer within 30 s")
        .unwrap()
        .err()
        .expect("the re-bind to a shut queue is refused");
    let paced = unbound_at.elapsed();
    assert!(format!("{err:#}").contains("bind"), "{err:#}");
    assert!(paced >= RECONNECT_INTERVAL, "reopened after {paced:?}");
    assert_eq!(c.bound_epoch(), 1, "no re-bind while egress is shut");
    c.close().await;
    semp.delete_queue(&names.queue).await;
}

/// Python test_buffered_messages_are_not_handed_out_after_close: messages
/// already on the flow (unsettled, in the link's credit window) are never
/// handed out once close() has run; they stay spooled for the next bind.
#[tokio::test]
async fn buffered_messages_are_not_handed_out_after_close() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let (c, pending) = bound(&lease, &names, &semp).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[(1, "a", "v1", "upsert")]))
        .await
        .unwrap();
    let first = delivered(pending).await;
    assert_eq!(first_id(&first), 1);
    for i in 2..=4 {
        p.publish_batch(&make_batch(&[(i, "a", "v1", "upsert")]))
            .await
            .unwrap();
    }
    wait_for_depth(&semp, &names.queue, 4).await;
    wait_until(
        "4 messages delivered to the flow, unacked",
        Duration::from_secs(30),
        || {
            let (semp, queue) = (semp.clone(), names.queue.clone());
            async move { semp.queue_unacked_count(&queue).await.is_ok_and(|n| n == 4) }
        },
    )
    .await;

    c.close().await;
    let next = tokio::time::timeout(Duration::from_secs(20), c.next_delivery())
        .await
        .expect("an answer within 20 s")
        .unwrap();
    assert!(next.is_none());
    drop(first); // never settled
    wait_for_unbound(&semp, &names.queue).await;
    assert_eq!(semp.queue_unacked_count(&names.queue).await.unwrap(), 0);
    assert_eq!(
        semp.queue_message_count(&names.queue).await.unwrap(),
        4,
        "nothing settled or handed out"
    );
    p.close().await;
}
