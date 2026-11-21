use async_trait::async_trait;
use rust_template::health::ingest::IngestHealthProvider;
use rust_template::health::probe::{HealthProbe, ProbeError};
use rust_template::health::registry::ProbeRegistry;
use rust_template::ingestion::ConnectionState;
use rust_template::schemas::health::{IngestHealth, ProbeFailure, Status};
use rust_template::settings::IngestTransport;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Ingest health of an idle noop transport: always connected, never ingests.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopIngestHealth;

impl IngestHealthProvider for NoopIngestHealth {
    fn ingest_health(&self) -> IngestHealth {
        IngestHealth {
            transport: IngestTransport::Noop,
            connection_state: ConnectionState::Connected,
            thread_alive: true,
            last_batch_at: None,
            seconds_since_last_batch: None,
            rows_ingested_total: 0,
            stale: false,
            store: Default::default(),
        }
    }
}

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
    assert!(
        ProbeRegistry::default()
            .run_all(Duration::from_secs(1))
            .await
            .is_empty()
    );
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
    assert_eq!((results[0].status, results[0].error), (Status::Up, None));
    assert_eq!(results[1].status, Status::Down);
    assert_eq!(results[1].error, Some(ProbeFailure::Unavailable)); // never the raw detail
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
        assert_eq!(r.status, Status::Down);
        assert_eq!(r.error, Some(ProbeFailure::Timeout));
        assert_eq!(r.latency_ms, 200.0);
    }
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
    assert_eq!(results[0].status, Status::Up);
    assert_eq!(results[1].name, "panicky");
    assert_eq!(results[1].status, Status::Down);
    assert_eq!(results[1].error, Some(ProbeFailure::Unavailable));
}
