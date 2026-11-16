//! The AMQP test publisher spools persistent messages on a subscribed queue.

use crate::support::batches::make_batch;
use crate::support::containers;
use crate::support::semp::Semp;
use crate::support::solace::{publisher, wait_for_depth, SolaceNames};

#[tokio::test]
async fn a_published_batch_lands_on_a_subscribed_queue() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    semp.create_queue(&names.queue, "consume", None)
        .await
        .unwrap();
    semp.add_subscription(&names.queue, &names.topic)
        .await
        .unwrap();

    let mut publisher = publisher(&lease.endpoints, &names.topic).await;
    publisher
        .publish_batch(&make_batch(&[(1, "a", "v1", "upsert")]))
        .await
        .unwrap();
    publisher
        .publish_bytes(b"raw bytes".to_vec())
        .await
        .unwrap();
    publisher.close().await;

    wait_for_depth(&semp, &names.queue, 2).await;
    semp.delete_queue(&names.queue).await;
}
