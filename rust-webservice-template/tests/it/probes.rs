use async_trait::async_trait;
use rust_template::health::ingest::{IngestHealthProvider, NoopIngestHealth};
use rust_template::health::probe::{HealthProbe, ProbeError};
use rust_template::health::registry::ProbeRegistry;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct StaticProbe {
    pub name: &'static str,
    pub outcome: Result<(), &'static str>,
    pub delay: Duration,
}

#[async_trait]
impl HealthProbe for StaticProbe {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn check(&self) -> Result<(), ProbeError> {
        tokio::time::sleep(self.delay).await;
        self.outcome
            .map_err(|detail| ProbeError::Unavailable(detail.to_owned()))
    }
}

pub fn probe(
    name: &'static str,
    outcome: Result<(), &'static str>,
    delay_ms: u64,
) -> Arc<dyn HealthProbe> {
    Arc::new(StaticProbe {
        name,
        outcome,
        delay: Duration::from_millis(delay_ms),
    })
}

#[tokio::test]
async fn empty_registry_reports_nothing() {
    assert!(ProbeRegistry::default()
        .run_all(Duration::from_secs(1))
        .await
        .is_empty());
}

#[tokio::test]
async fn results_keep_registration_order_and_shapes() {
    let registry = ProbeRegistry::new(vec![
        probe("postgres", Ok(()), 0),
        probe("redis", Err("redis://:secret@localhost:6379 refused"), 0),
        probe("clickhouse", Ok(()), 0),
    ]);
    let results = registry.run_all(Duration::from_secs(1)).await;
    let names: Vec<_> = results.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["postgres", "redis", "clickhouse"]);
    assert_eq!(
        (results[0].status.as_str(), results[0].error.as_deref()),
        ("up", None)
    );
    assert_eq!(results[1].status, "down");
    assert_eq!(results[1].error.as_deref(), Some("unavailable")); // never the raw detail
    assert_eq!(results[1].latency_ms, 0.0);
}

#[tokio::test]
async fn slow_probe_times_out_and_probes_run_concurrently() {
    let registry = ProbeRegistry::new(vec![
        probe("slow-a", Ok(()), 5_000),
        probe("slow-b", Ok(()), 5_000),
    ]);
    let started = Instant::now();
    let results = registry.run_all(Duration::from_millis(200)).await;
    // Concurrent: two 2 s probes bounded by one 200 ms timeout, not 400 ms+.
    assert!(
        started.elapsed() < Duration::from_millis(1_000),
        "{:?}",
        started.elapsed()
    );
    for r in &results {
        assert_eq!(r.status, "down");
        assert_eq!(r.error.as_deref(), Some("timeout"));
        assert_eq!(r.latency_ms, 200.0);
    }
}

#[test]
fn noop_ingest_health_reports_connected_noop() {
    let h = NoopIngestHealth.ingest_health();
    assert_eq!(h.transport, "noop");
    assert_eq!(h.connection_state, "connected");
    assert!(h.thread_alive);
    assert_eq!(h.rows_ingested_total, 0);
    assert!(h.last_batch_at.is_none() && h.seconds_since_last_batch.is_none() && !h.stale);
}

pub struct PanicProbe;

#[async_trait]
impl HealthProbe for PanicProbe {
    fn name(&self) -> &'static str {
        "panicky"
    }
    async fn check(&self) -> Result<(), ProbeError> {
        panic!("probe blew up");
    }
}

#[tokio::test]
async fn panicking_probe_fails_closed() {
    let registry = ProbeRegistry::new(vec![probe("ok", Ok(()), 0), Arc::new(PanicProbe)]);
    let results = registry.run_all(Duration::from_secs(1)).await;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status, "up");
    assert_eq!(results[1].name, "panicky");
    assert_eq!(results[1].status, "down");
    assert_eq!(results[1].error.as_deref(), Some("unavailable"));
}
