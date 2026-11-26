//! Per-test Solace names and settings, plus the polling helpers the Solace
//! tests share.

use super::containers::{SOLACE_ADMIN, SolaceEndpoints, wait_until};
use super::semp::Semp;
use super::{TestApp, app_settings, test_settings};
use rust_template::ingestion::solace::publisher::AmqpPublisher;
use rust_template::settings::{IngestTransport, Settings};
use secrecy::SecretString;
use serde_json::Value;
use std::time::{Duration, Instant};

/// Per-test topic + queue: one shared broker, isolated by naming (the
/// Solace multi-tenant idiom) instead of one broker per test.
#[derive(Debug, Clone)]
pub struct SolaceNames {
    pub topic: String,
    pub queue: String,
}

impl SolaceNames {
    pub fn new() -> Self {
        let uid = &uuid::Uuid::new_v4().simple().to_string()[..12];
        Self {
            topic: format!("test/{uid}/batches"),
            queue: format!("test-{uid}"),
        }
    }
}

/// Bind `base` to the broker and these names.
pub fn solace_overrides(broker: &SolaceEndpoints, names: &SolaceNames, base: Settings) -> Settings {
    Settings {
        ingest_transport: IngestTransport::Solace,
        solace_host: broker.host.clone(),
        solace_port: broker.amqp,
        solace_vpn: "default".into(),
        // SASL rule: empty = ANONYMOUS. SOLACE_ADMIN is for SEMP only.
        solace_username: String::new(),
        solace_password: SecretString::from(""),
        solace_topic: names.topic.clone(),
        solace_queue: names.queue.clone(),
        solace_semp_url: broker.semp_url(),
        solace_semp_username: SOLACE_ADMIN.0.into(),
        solace_semp_password: SecretString::from(SOLACE_ADMIN.1),
        ..base
    }
}

/// An app wired to the shared stores and the broker, with a small LSM
/// store (2 layers of 2 parts), so a few batches exercise a merge.
pub async fn solace_settings(broker: &SolaceEndpoints, names: &SolaceNames) -> Settings {
    let base = Settings {
        lsm_layers: 2,
        lsm_parts_per_layer: 2,
        ..app_settings().await
    };
    solace_overrides(broker, names, base)
}

/// Consumer-level tests: no stores, fast startup retry.
pub fn consumer_settings(broker: &SolaceEndpoints, names: &SolaceNames) -> Settings {
    let base = Settings {
        connect_base_delay: 0.1,
        connect_max_delay: 1.0,
        ..test_settings()
    };
    solace_overrides(broker, names, base)
}

pub async fn publisher(broker: &SolaceEndpoints, topic: &str) -> AmqpPublisher {
    AmqpPublisher::connect(&broker.amqp_target("", ""), topic)
        .await
        .expect("AMQP publisher")
}

/// The consumer has bound its flow: publishing before that would be
/// discarded (no queue or no subscription yet).
pub async fn wait_for_bound(semp: &Semp, queue: &str) {
    wait_until(
        &format!("queue {queue} bound"),
        Duration::from_secs(60),
        || {
            let (semp, queue) = (semp.clone(), queue.to_owned());
            async move {
                semp.queue_exists(&queue).await.unwrap_or(false)
                    && semp.queue_bind_count(&queue).await.is_ok_and(|n| n == 1)
            }
        },
    )
    .await;
}

pub async fn wait_for_unbound(semp: &Semp, queue: &str) {
    wait_until(
        &format!("queue {queue} unbound"),
        Duration::from_secs(60),
        || {
            let (semp, queue) = (semp.clone(), queue.to_owned());
            async move { semp.queue_bind_count(&queue).await.is_ok_and(|n| n == 0) }
        },
    )
    .await;
}

pub async fn wait_for_depth(semp: &Semp, queue: &str, depth: usize) {
    wait_until(
        &format!("queue {queue} depth {depth}"),
        Duration::from_secs(30),
        || {
            let (semp, queue) = (semp.clone(), queue.to_owned());
            async move {
                semp.queue_message_count(&queue)
                    .await
                    .is_ok_and(|n| n == depth)
            }
        },
    )
    .await;
}

/// GET /data/cache?limit=100 until `until(body)` holds.
pub async fn poll_cache(app: &TestApp, until: impl Fn(&Value) -> bool, timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        let (status, body) = app.get_json("/data/cache?limit=100").await;
        if status == 200 && until(&body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "cache never reached the expected state; last {status}: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `{id: value}` of a /data/cache body.
pub fn values(body: &Value) -> std::collections::BTreeMap<u64, String> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["id"].as_u64().unwrap(),
                r["value"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}
