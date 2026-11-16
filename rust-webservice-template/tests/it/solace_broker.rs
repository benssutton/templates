//! The shared broker fixture and the SEMP client against the real broker.

use crate::support::containers;
use crate::support::semp::Semp;
use crate::support::solace::SolaceNames;

#[tokio::test]
async fn the_shared_broker_starts_once_and_serves_semp() {
    let first = containers::solace().await.endpoints.clone();
    let second = containers::solace().await.endpoints.clone();
    assert_eq!(first, second, "one broker per run");
    let semp = Semp::new(&first.semp_url());
    assert!(!semp.queue_exists("no-such-queue-r3").await.unwrap());
}

#[tokio::test]
async fn semp_creates_inspects_and_deletes_a_queue() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    semp.create_queue(&names.queue, "consume", None)
        .await
        .unwrap();
    semp.add_subscription(&names.queue, &names.topic)
        .await
        .unwrap();
    assert!(semp.queue_exists(&names.queue).await.unwrap());
    assert_eq!(
        semp.queue_subscriptions(&names.queue).await.unwrap(),
        std::slice::from_ref(&names.topic)
    );
    let stats = semp.queue_stats(&names.queue).await.unwrap();
    assert_eq!(
        (stats.spooled, stats.bound, stats.unacked, stats.redelivered),
        (0, 0, 0, 0)
    );
    semp.delete_queue(&names.queue).await;
    assert!(!semp.queue_exists(&names.queue).await.unwrap());
}
