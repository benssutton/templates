//! StreamIngestService decides HOW each delivery is settled, backs off, and
//! requests shutdown. Real (non-mock) trait implementations: a scripted
//! consumer, recording settlements, a store whose write always fails, and an
//! LSM store behind a gate.

use crate::probes::NoopIngestHealth;
use crate::support::batches::{batch_without_id, make_batch};
use crate::support::gated_store::GatedStore;
use arrow::array::RecordBatch;
use rust_template::core::last_request::LastRequest;
use rust_template::core::shutdown::Shutdown;

use rust_template::health::registry::ProbeRegistry;
use rust_template::ingestion::ipc::batch_bytes;
use rust_template::ingestion::{BatchConsumer, ConnectionState, Delivery, Settle};
use rust_template::persistence::stream_store::lsm::{LsmConfig, LsmStore};
use rust_template::persistence::stream_store::{QueryResult, StoreError, StreamStore};
use rust_template::schemas::health::Status;
use rust_template::services::health::HealthService;
use rust_template::services::metrics::MetricsService;
use rust_template::services::stream_ingest::{IngestError, StreamIngestService};
use rust_template::settings::{IngestTransport, Settings};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

type Log = Arc<Mutex<Vec<(&'static str, Instant)>>>;

/// Records each settlement; optionally errors on one outcome (a flow that
/// terminated under us).
struct Recorder {
    log: Log,
    error_on: Option<&'static str>,
}

impl Recorder {
    fn record(&self, outcome: &'static str) -> anyhow::Result<()> {
        self.log.lock().unwrap().push((outcome, Instant::now()));
        match self.error_on {
            Some(o) if o == outcome => anyhow::bail!("flow terminated"),
            _ => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl Settle for Recorder {
    async fn accept(self: Box<Self>) -> anyhow::Result<()> {
        self.record("accept")
    }
    async fn fail(self: Box<Self>) -> anyhow::Result<()> {
        self.record("fail")
    }
    async fn reject(self: Box<Self>) -> anyhow::Result<()> {
        self.record("reject")
    }
}

fn delivery(batches: Vec<RecordBatch>, log: &Log) -> anyhow::Result<Delivery> {
    Ok(Delivery {
        batches,
        redelivered: false,
        settle: Box::new(Recorder {
            log: log.clone(),
            error_on: None,
        }),
    })
}

/// Yields the scripted items, then idles until closed. Its state is fixed.
struct ScriptedConsumer {
    script: tokio::sync::Mutex<VecDeque<anyhow::Result<Delivery>>>,
    state: ConnectionState,
    closed: CancellationToken,
}

impl ScriptedConsumer {
    fn new(script: Vec<anyhow::Result<Delivery>>, state: ConnectionState) -> Arc<Self> {
        Arc::new(Self {
            script: tokio::sync::Mutex::new(script.into()),
            state,
            closed: CancellationToken::new(),
        })
    }
}

#[async_trait::async_trait]
impl BatchConsumer for ScriptedConsumer {
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>> {
        if let Some(item) = self.script.lock().await.pop_front() {
            return item.map(Some);
        }
        self.closed.cancelled().await;
        Ok(None)
    }
    async fn close(&self) {
        self.closed.cancel();
    }
    fn connection_state(&self) -> ConnectionState {
        self.state
    }
    fn bound_epoch(&self) -> u64 {
        1
    }
}

/// Every next_delivery() fails, as a denied queue bind does.
#[derive(Default)]
struct ErroringConsumer {
    attempts: AtomicU32,
}

#[async_trait::async_trait]
impl BatchConsumer for ErroringConsumer {
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("queue bind denied")
    }
    async fn close(&self) {}
    fn connection_state(&self) -> ConnectionState {
        ConnectionState::Connected
    }
    fn bound_epoch(&self) -> u64 {
        1
    }
}

/// Errors 4 times, re-binds (the epoch rises), errors 4 more times, then
/// idles until closed: 4 + 4 failures, never 5 consecutive ones.
#[derive(Default)]
struct RebindingConsumer {
    attempts: AtomicU32,
    calls: Mutex<Vec<Instant>>,
    epoch: AtomicU64,
    closed: CancellationToken,
}

#[async_trait::async_trait]
impl BatchConsumer for RebindingConsumer {
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        self.calls.lock().unwrap().push(Instant::now());
        if attempt == 5 {
            // A reconnect bound the queue, then the flow dropped again.
            self.epoch.fetch_add(1, Ordering::SeqCst);
        }
        if attempt <= 8 {
            anyhow::bail!("flow lost (attempt {attempt})");
        }
        self.closed.cancelled().await;
        Ok(None)
    }
    async fn close(&self) {
        self.closed.cancel();
    }
    fn connection_state(&self) -> ConnectionState {
        ConnectionState::Connected
    }
    fn bound_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
}

/// A real store whose write fails: an unexpected, possibly transient error.
struct FailingStore;

#[async_trait::async_trait]
impl StreamStore for FailingStore {
    async fn ingest(&self, _batches: Vec<RecordBatch>) -> Result<(), StoreError> {
        Err(StoreError::Internal("simulated storage failure".into()))
    }
    fn validate(&self, _batches: &[RecordBatch]) -> Result<(), StoreError> {
        Ok(())
    }
    fn query(&self, _limit: usize) -> Result<QueryResult, StoreError> {
        Ok(QueryResult {
            rows: None,
            total: 0,
        })
    }
}

/// The first next_delivery() fails; later calls idle until closed.
#[derive(Default)]
struct FailsThenIdles {
    attempts: AtomicU32,
    closed: CancellationToken,
}

#[async_trait::async_trait]
impl BatchConsumer for FailsThenIdles {
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            anyhow::bail!("service terminally interrupted");
        }
        self.closed.cancelled().await;
        Ok(None)
    }
    async fn close(&self) {
        self.closed.cancel();
    }
    fn connection_state(&self) -> ConnectionState {
        ConnectionState::Connected
    }
    fn bound_epoch(&self) -> u64 {
        1
    }
}

fn settings() -> Settings {
    Settings {
        ingest_transport: IngestTransport::Noop,
        ingest_max_disconnect_seconds: None,
        ingest_max_consecutive_failures: 0,
        ..Settings::default()
    }
}

fn lsm() -> Arc<LsmStore> {
    Arc::new(LsmStore::new(LsmConfig::default()))
}

fn start(
    consumer: Arc<dyn BatchConsumer>,
    store: Arc<dyn StreamStore>,
    settings: Settings,
) -> (Arc<StreamIngestService>, Shutdown) {
    let shutdown = Shutdown::new();
    let svc =
        StreamIngestService::start(Arc::new(settings), consumer, store, shutdown.clone(), None);
    (svc, shutdown)
}

async fn wait_for_settlements(log: &Log, n: usize) -> Vec<&'static str> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let outcomes: Vec<&'static str> = log.lock().unwrap().iter().map(|(o, _)| *o).collect();
        if outcomes.len() >= n {
            return outcomes;
        }
        assert!(Instant::now() < deadline, "only {outcomes:?} settled");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn one_row() -> Vec<RecordBatch> {
    vec![make_batch(&[(1, "a", "v1", "upsert")])]
}

#[tokio::test]
async fn a_successful_write_accepts_once_after_every_batch() {
    let log = Log::default();
    let batches = vec![
        make_batch(&[(1, "a", "v1", "upsert")]),
        make_batch(&[(2, "b", "v1", "upsert")]),
    ];
    let store = lsm();
    let (svc, _) = start(
        ScriptedConsumer::new(vec![delivery(batches, &log)], ConnectionState::Connected),
        store.clone(),
        settings(),
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["accept"]);
    assert_eq!(store.query(100).unwrap().total, 2);
    assert_eq!(svc.ingest_health().rows_ingested_total, 2);
    svc.close().await;
}

#[tokio::test]
async fn an_unmergeable_batch_rejects() {
    let log = Log::default();
    let (svc, _) = start(
        ScriptedConsumer::new(
            vec![delivery(vec![batch_without_id()], &log)],
            ConnectionState::Connected,
        ),
        lsm(),
        settings(),
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["reject"]);
    svc.close().await;
}

#[tokio::test]
async fn an_oversized_batch_rejects_and_writes_nothing() {
    let log = Log::default();
    let store = lsm();
    let (svc, _) = start(
        ScriptedConsumer::new(vec![delivery(one_row(), &log)], ConnectionState::Connected),
        store.clone(),
        Settings {
            max_ingest_batch_bytes: 1,
            ..settings()
        },
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["reject"]);
    assert_eq!(store.query(100).unwrap().total, 0);
    svc.close().await;
}

/// Every batch is validated before any is written.
#[tokio::test]
async fn an_invalid_later_batch_rejects_the_whole_delivery_and_writes_nothing() {
    let log = Log::default();
    let store = lsm();
    let (svc, _) = start(
        ScriptedConsumer::new(
            vec![delivery(
                vec![make_batch(&[(1, "a", "v1", "upsert")]), batch_without_id()],
                &log,
            )],
            ConnectionState::Connected,
        ),
        store.clone(),
        settings(),
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["reject"]);
    assert_eq!(store.query(100).unwrap().total, 0);
    assert_eq!(svc.ingest_health().rows_ingested_total, 0);
    svc.close().await;
}

#[tokio::test]
async fn an_oversized_later_batch_rejects_the_whole_delivery_and_writes_nothing() {
    let log = Log::default();
    let small = make_batch(&[(1, "a", "v1", "upsert")]);
    let large = make_batch(&[(2, "b", "v1", "upsert"), (3, "c", "v1", "upsert")]);
    assert!(batch_bytes(&large) > batch_bytes(&small));
    let store = lsm();
    let (svc, _) = start(
        ScriptedConsumer::new(
            vec![delivery(vec![small.clone(), large], &log)],
            ConnectionState::Connected,
        ),
        store.clone(),
        Settings {
            max_ingest_batch_bytes: batch_bytes(&small),
            ..settings()
        },
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["reject"]);
    assert_eq!(store.query(100).unwrap().total, 0);
    svc.close().await;
}

/// Spec §6: a store error -> fail (redelivery). Not an unmergeable batch:
/// that is InvalidBatch -> reject.
#[tokio::test]
async fn a_store_error_fails_for_redelivery() {
    let log = Log::default();
    let (svc, _) = start(
        ScriptedConsumer::new(vec![delivery(one_row(), &log)], ConnectionState::Connected),
        Arc::new(FailingStore),
        settings(),
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["fail"]);
    svc.close().await;
}

#[tokio::test]
async fn an_erroring_reject_is_not_followed_by_a_second_settlement() {
    let log = Log::default();
    let erroring = Ok(Delivery {
        batches: one_row(),
        redelivered: false,
        settle: Box::new(Recorder {
            log: log.clone(),
            error_on: Some("reject"),
        }),
    });
    let next = delivery(vec![make_batch(&[(2, "b", "v1", "upsert")])], &log);
    let (svc, _) = start(
        ScriptedConsumer::new(vec![erroring, next], ConnectionState::Connected),
        lsm(),
        Settings {
            max_ingest_batch_bytes: 1,
            ..settings()
        },
    );
    // The loop carried on to the next delivery (also oversized -> reject).
    assert_eq!(wait_for_settlements(&log, 2).await, ["reject", "reject"]);
    svc.close().await;
}

/// fail() makes the broker redeliver at once; without a settlement backoff a
/// persistent store error would spin. Delays double: ~1 s, then ~2 s.
#[tokio::test(start_paused = true)]
async fn a_persistent_store_failure_backs_off_between_fails() {
    let log = Log::default();
    let script = (0..3).map(|_| delivery(one_row(), &log)).collect();
    let (svc, _) = start(
        ScriptedConsumer::new(script, ConnectionState::Connected),
        Arc::new(FailingStore),
        settings(),
    );
    wait_for_settlements(&log, 3).await;
    let times: Vec<Instant> = log.lock().unwrap().iter().map(|(_, t)| *t).collect();
    assert!(
        times[1] - times[0] >= Duration::from_secs(1),
        "{:?}",
        times[1] - times[0]
    );
    assert!(
        times[2] - times[1] >= Duration::from_secs(2),
        "{:?}",
        times[2] - times[1]
    );
    svc.close().await;
}

#[tokio::test]
async fn close_during_a_settlement_backoff_is_prompt() {
    let log = Log::default();
    let script = (0..3).map(|_| delivery(one_row(), &log)).collect();
    let (svc, _) = start(
        ScriptedConsumer::new(script, ConnectionState::Connected),
        Arc::new(FailingStore),
        settings(),
    );
    wait_for_settlements(&log, 2).await; // a ~2 s backoff is now pending
    let started = std::time::Instant::now();
    svc.close().await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        log.lock().unwrap().len(),
        2,
        "the pending delivery was not processed"
    );
    assert!(!svc.is_running());
}

#[tokio::test(start_paused = true)]
async fn five_consecutive_consumer_errors_request_shutdown() {
    let consumer = Arc::new(ErroringConsumer::default());
    let (svc, shutdown) = start(
        consumer.clone(),
        lsm(),
        Settings {
            ingest_max_disconnect_seconds: Some(60.0),
            ingest_max_consecutive_failures: 5,
            ..settings()
        },
    );
    tokio::time::timeout(Duration::from_secs(120), shutdown.triggered())
        .await
        .expect("shutdown requested");
    assert_eq!(consumer.attempts.load(Ordering::SeqCst), 5);
    svc.close().await;
}

/// A bind between failures resets the count: 4 failures, a re-bind and 4
/// more failures request no shutdown.
#[tokio::test(start_paused = true)]
async fn a_rebind_resets_the_consecutive_failure_count() {
    let consumer = Arc::new(RebindingConsumer::default());
    let (svc, shutdown) = start(
        consumer.clone(),
        lsm(),
        Settings {
            ingest_max_disconnect_seconds: Some(60.0),
            ingest_max_consecutive_failures: 5,
            ..settings()
        },
    );
    let deadline = Instant::now() + Duration::from_secs(600);
    while consumer.attempts.load(Ordering::SeqCst) < 9 {
        assert!(
            !shutdown.is_triggered(),
            "shutdown after {:?} attempts",
            consumer.attempts
        );
        assert!(
            Instant::now() < deadline,
            "stuck at {:?} attempts",
            consumer.attempts
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert!(!shutdown.is_triggered());
    assert!(svc.is_running());
    // The bump reset the transport backoff too: the wait after the 5th
    // failure is the base delay (1-1.25 s), not the 5th doubling (16 s).
    let calls = consumer.calls.lock().unwrap().clone();
    let after_rebind = calls[5] - calls[4];
    assert!(
        after_rebind >= Duration::from_secs(1) && after_rebind <= Duration::from_millis(1250),
        "{after_rebind:?}"
    );
    svc.close().await;
}

/// A delivery proves the transport works, like a bind: 4 failures, a
/// delivery, 4 more failures is no shutdown.
#[tokio::test(start_paused = true)]
async fn a_delivery_resets_the_consecutive_failure_count() {
    let log = Log::default();
    let errors = || (0..4).map(|i| Err(anyhow::anyhow!("flow lost ({i})")));
    let script: Vec<anyhow::Result<Delivery>> = errors()
        .chain([delivery(one_row(), &log)])
        .chain(errors())
        .collect();
    let consumer = ScriptedConsumer::new(script, ConnectionState::Connected);
    let (svc, shutdown) = start(
        consumer.clone(),
        lsm(),
        Settings {
            ingest_max_disconnect_seconds: Some(60.0),
            ingest_max_consecutive_failures: 5,
            ..settings()
        },
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["accept"]);
    let deadline = Instant::now() + Duration::from_secs(600);
    while !consumer.script.lock().await.is_empty() {
        assert!(
            !shutdown.is_triggered(),
            "shutdown before the script ran out"
        );
        assert!(Instant::now() < deadline, "script never ran out");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert!(!shutdown.is_triggered());
    assert!(svc.is_running());
    svc.close().await;
}

#[tokio::test(start_paused = true)]
async fn a_transport_error_backs_off_then_the_next_call_delivers() {
    let log = Log::default();
    let consumer = ScriptedConsumer::new(
        vec![
            Err(anyhow::anyhow!("service terminally interrupted")),
            delivery(one_row(), &log),
        ],
        ConnectionState::Connected,
    );
    let started = Instant::now();
    let (svc, shutdown) = start(
        consumer.clone(),
        lsm(),
        Settings {
            ingest_max_disconnect_seconds: Some(60.0),
            ingest_max_consecutive_failures: 5,
            ..settings()
        },
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["accept"]);
    let settled_at = log.lock().unwrap()[0].1;
    assert!(
        settled_at - started >= Duration::from_secs(1),
        "transport backoff before the retry: {:?}",
        settled_at - started
    );
    assert!(consumer.script.lock().await.is_empty(), "called twice");
    assert!(!shutdown.is_triggered());
    svc.close().await;
}

#[tokio::test]
async fn close_during_a_transport_backoff_is_prompt() {
    let consumer = Arc::new(FailsThenIdles::default());
    let (svc, _) = start(consumer.clone(), lsm(), settings());
    let deadline = Instant::now() + Duration::from_secs(5);
    while consumer.attempts.load(Ordering::SeqCst) < 1 {
        assert!(Instant::now() < deadline, "first call");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let started = std::time::Instant::now(); // a ~1 s transport backoff is pending
    svc.close().await;
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(consumer.attempts.load(Ordering::SeqCst), 1);
    assert!(!svc.is_running());
}

/// A dropped `ingest_batch` (an HTTP client that disconnects) cannot skip
/// the bookkeeping of a write that commits anyway.
#[tokio::test]
async fn a_dropped_http_ingest_still_records_the_committed_rows() {
    let store = Arc::new(GatedStore::new(LsmStore::new(LsmConfig::default())));
    let metrics = Arc::new(MetricsService::new(&Settings::default()).unwrap());
    let svc = StreamIngestService::start(
        Arc::new(settings()),
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        store.clone(),
        Shutdown::new(),
        Some(metrics.clone()),
    );
    let entered = store.entered.notified();
    let dropped = tokio::time::timeout(
        Duration::from_millis(100),
        svc.ingest(vec![make_batch(&[
            (1, "a", "v1", "upsert"),
            (2, "b", "v1", "upsert"),
        ])]),
    )
    .await;
    assert!(dropped.is_err(), "the write was still pending at the gate");
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("the write reached the store");
    assert_eq!(svc.ingest_health().rows_ingested_total, 0);

    store.gate.add_permits(1); // the write commits after its caller is gone
    let deadline = Instant::now() + Duration::from_secs(5);
    while svc.ingest_health().rows_ingested_total < 2 {
        assert!(
            Instant::now() < deadline,
            "the committed rows were never recorded"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(store.query(0).unwrap().total, 2);
    assert!(svc.ingest_health().last_batch_at.is_some());
    let health = HealthService::new(
        Arc::new(Settings::default()),
        LastRequest::new(),
        ProbeRegistry::default(),
        Arc::new(NoopIngestHealth),
    );
    let text = metrics.render(&health).await;
    assert!(text.contains("ingest_rows_ingested_total 2"), "{text}");
    svc.close().await;
}

/// ingest_max_disconnect_seconds=None disables every automatic shutdown.
#[tokio::test(start_paused = true)]
async fn consumer_errors_never_request_shutdown_when_the_limit_is_unset() {
    let consumer = Arc::new(ErroringConsumer::default());
    let (svc, shutdown) = start(consumer.clone(), lsm(), settings());
    tokio::time::sleep(Duration::from_secs(300)).await;
    assert!(!shutdown.is_triggered());
    assert!(consumer.attempts.load(Ordering::SeqCst) > 5);
    svc.close().await;
}

#[tokio::test(start_paused = true)]
async fn the_watchdog_requests_shutdown_when_the_transport_stays_down() {
    let (svc, shutdown) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Reconnecting),
        lsm(),
        Settings {
            ingest_max_disconnect_seconds: Some(0.2),
            ..settings()
        },
    );
    tokio::time::timeout(Duration::from_secs(3), shutdown.triggered())
        .await
        .expect("watchdog fired");
    svc.close().await;
}

#[tokio::test(start_paused = true)]
async fn the_watchdog_stays_quiet_while_connected() {
    let (svc, shutdown) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        lsm(),
        Settings {
            ingest_max_disconnect_seconds: Some(0.2),
            ..settings()
        },
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!shutdown.is_triggered());
    svc.close().await;
}

#[tokio::test]
async fn ingest_health_tracks_freshness_after_an_http_ingest() {
    let (svc, _) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        lsm(),
        settings(),
    );
    let before = svc.ingest_health();
    assert_eq!(before.transport, IngestTransport::Noop);
    assert_eq!(before.connection_state, ConnectionState::Connected);
    assert!(before.thread_alive);
    assert!(before.last_batch_at.is_none());
    assert_eq!(before.rows_ingested_total, 0);

    svc.ingest(vec![make_batch(&[(1, "a", "v1", "upsert")])])
        .await
        .unwrap();
    let after = svc.ingest_health();
    assert_eq!(after.rows_ingested_total, 1);
    assert!(after.last_batch_at.is_some());
    assert!(after.seconds_since_last_batch.is_some());
    assert!(!after.stale, "no threshold configured");
    svc.close().await;
}

/// The JSON shape of `IngestHealth` in the shared contract, plus `store`.
#[tokio::test]
async fn ingest_health_serialises_with_the_contract_field_names() {
    let (svc, _) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        lsm(),
        settings(),
    );
    svc.ingest(vec![make_batch(&[(1, "a", "v1", "upsert")])])
        .await
        .unwrap();
    let json = serde_json::to_value(svc.ingest_health()).unwrap();
    let mut keys: Vec<&str> = json
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "connection_state",
            "last_batch_at",
            "rows_ingested_total",
            "seconds_since_last_batch",
            "stale",
            "store",
            "thread_alive",
            "transport"
        ]
    );
    // A UTC datetime with at most microseconds, as the siblings write it: ...T12:34:56.123456Z
    let at = json["last_batch_at"].as_str().unwrap();
    assert!(at.ends_with('Z'), "{at}");
    let fraction = at.rsplit('.').next().unwrap().trim_end_matches('Z');
    assert!(fraction.len() <= 6, "{at}");
    assert!(json["seconds_since_last_batch"].is_f64(), "{json}");
    svc.close().await;
}

/// An idle ingest is stale but ready: staleness fails readiness only with
/// ingest_stale_fails_readiness.
#[tokio::test]
async fn staleness_fails_readiness_only_when_configured() {
    for fails_readiness in [false, true] {
        let settings = Settings {
            ingest_staleness_threshold_seconds: Some(0.05),
            ingest_stale_fails_readiness: fails_readiness,
            ..settings()
        };
        let (svc, _) = start(
            ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
            lsm(),
            settings.clone(),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(svc.ingest_health().stale);
        let health = HealthService::new(
            Arc::new(settings),
            LastRequest::new(),
            ProbeRegistry::default(),
            svc.clone(),
        );
        let readiness = health.readiness().await;
        let ingest = readiness
            .checks
            .iter()
            .find(|c| c.name == "ingest")
            .unwrap();
        let expected = if fails_readiness {
            Status::Down
        } else {
            Status::Up
        };
        assert_eq!(ingest.status, expected, "fails_readiness={fails_readiness}");
        svc.close().await;
    }
}

#[tokio::test]
async fn staleness_is_reported_only_when_a_threshold_is_set() {
    let (svc, _) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        lsm(),
        Settings {
            ingest_staleness_threshold_seconds: Some(0.2),
            ..settings()
        },
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        svc.ingest_health().stale,
        "no batch ever, started > 0.2 s ago"
    );
    svc.ingest(vec![make_batch(&[(1, "a", "v1", "upsert")])])
        .await
        .unwrap();
    assert!(!svc.ingest_health().stale, "fresh batch");
    svc.close().await;
}

#[tokio::test]
async fn http_ingest_refuses_oversized_and_invalid_batches() {
    let store = lsm();
    let (svc, _) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        store.clone(),
        Settings {
            max_ingest_batch_bytes: 1,
            ..settings()
        },
    );
    assert!(matches!(
        svc.ingest(vec![make_batch(&[(1, "a", "v1", "upsert")])])
            .await,
        Err(IngestError::TooLarge(_))
    ));
    assert_eq!(store.query(0).unwrap().total, 0);
    let (svc2, _) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        lsm(),
        settings(),
    );
    assert!(matches!(
        svc2.ingest(vec![batch_without_id()]).await,
        Err(IngestError::Invalid(_))
    ));
    svc.close().await;
    svc2.close().await;
}

#[tokio::test]
async fn get_data_returns_python_rows_sorted_by_key() {
    let (svc, _) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        lsm(),
        settings(),
    );
    svc.ingest(vec![make_batch(&[
        (2, "b", "v1", "upsert"),
        (1, "a", "v1", "upsert"),
    ])])
    .await
    .unwrap();
    let page = svc.get_data(1).await.unwrap();
    assert_eq!((page.total, page.limit, page.rows.len()), (2, 1, 1));
    assert_eq!(
        (
            page.rows[0].id,
            page.rows[0].name.as_str(),
            page.rows[0].value.as_str()
        ),
        (1, "a", "v1")
    );
    svc.close().await;
}

#[tokio::test]
async fn close_stops_the_task_and_closes_the_consumer() {
    let consumer = ScriptedConsumer::new(Vec::new(), ConnectionState::Connected);
    let (svc, _) = start(consumer.clone(), lsm(), settings());
    assert!(svc.is_running());
    svc.close().await;
    assert!(consumer.closed.is_cancelled());
    assert!(!svc.is_running());
    assert!(!svc.ingest_health().thread_alive);
}

/// Spec §2 shutdown order: the Shutdown trigger (SIGTERM, watchdog, ...)
/// closes the consumer first, before the HTTP drain finishes.
#[tokio::test]
async fn the_shutdown_trigger_closes_the_consumer() {
    let consumer = ScriptedConsumer::new(Vec::new(), ConnectionState::Connected);
    let (svc, shutdown) = start(consumer.clone(), lsm(), settings());
    shutdown.trigger("test");
    tokio::time::timeout(Duration::from_secs(2), consumer.closed.cancelled())
        .await
        .expect("consumer closed on shutdown");
    svc.close().await;
}

#[tokio::test]
async fn settlements_are_counted_in_metrics() {
    let log = Log::default();
    let metrics = Arc::new(MetricsService::new(&Settings::default()).unwrap());
    let svc = StreamIngestService::start(
        Arc::new(settings()),
        ScriptedConsumer::new(vec![delivery(one_row(), &log)], ConnectionState::Connected),
        lsm(),
        Shutdown::new(),
        Some(metrics.clone()),
    );
    wait_for_settlements(&log, 1).await;
    let health = HealthService::new(
        Arc::new(Settings::default()),
        LastRequest::new(),
        ProbeRegistry::default(),
        Arc::new(NoopIngestHealth),
    );
    let text = metrics.render(&health).await;
    assert!(
        text.contains(r#"ingest_outcomes_total{outcome="accepted"} 1"#),
        "{text}"
    );
    assert!(text.contains("ingest_rows_ingested_total 1"), "{text}");
    svc.close().await;
}

fn capped_lsm(max_rows: usize) -> Arc<LsmStore> {
    Arc::new(LsmStore::new(LsmConfig {
        max_rows,
        ..LsmConfig::default()
    }))
}

/// A full store refuses every later message for good, so each is rejected
/// (never failed for redelivery, never backed off) and none counts toward
/// the consecutive-failure shutdown. Reads keep serving the rows held.
#[tokio::test]
async fn a_full_store_rejects_every_later_message_and_never_requests_shutdown() {
    let log = Log::default();
    let store = capped_lsm(2);
    let script = vec![
        delivery(
            vec![make_batch(&[
                (1, "a", "v1", "upsert"),
                (2, "b", "v1", "upsert"),
            ])],
            &log,
        ),
        delivery(vec![make_batch(&[(3, "c", "v1", "upsert")])], &log),
        delivery(vec![make_batch(&[(1, "a", "v1", "delete")])], &log),
        delivery(vec![make_batch(&[(4, "d", "v1", "upsert")])], &log),
    ];
    let (svc, shutdown) = start(
        ScriptedConsumer::new(script, ConnectionState::Connected),
        store.clone(),
        Settings {
            ingest_max_consecutive_failures: 1,
            ..settings()
        },
    );
    assert_eq!(
        wait_for_settlements(&log, 4).await,
        ["accept", "reject", "reject", "reject"]
    );
    assert!(!shutdown.is_triggered());
    assert!(svc.is_running());
    assert_eq!(store.query(0).unwrap().total, 2);
    let stats = svc.ingest_health().store;
    assert!(stats.full);
    assert_eq!(stats.refused_batches_total, 3);
    svc.close().await;
}

/// A message is all or nothing: a second batch that would overflow the cap
/// rejects the message with nothing of it written.
#[tokio::test]
async fn a_message_whose_second_batch_overflows_rejects_and_writes_nothing() {
    let log = Log::default();
    let store = capped_lsm(2);
    let batches = vec![
        make_batch(&[(1, "a", "v1", "upsert")]),
        make_batch(&[(2, "b", "v1", "upsert"), (3, "c", "v1", "upsert")]),
    ];
    let (svc, _) = start(
        ScriptedConsumer::new(vec![delivery(batches, &log)], ConnectionState::Connected),
        store.clone(),
        settings(),
    );
    assert_eq!(wait_for_settlements(&log, 1).await, ["reject"]);
    assert_eq!(store.query(0).unwrap().total, 0);
    assert_eq!(svc.ingest_health().rows_ingested_total, 0);
    assert!(store.stats().full);
    svc.close().await;
}

/// The HTTP path sees the refusal as `IngestError::Full`, and each refused
/// batch is logged as fatal with what was held, the cap and the batch size.
#[tokio::test]
async fn a_refused_batch_is_full_and_logged_as_fatal() {
    crate::support::logs::ensure_installed();
    let (svc, _) = start(
        ScriptedConsumer::new(Vec::new(), ConnectionState::Connected),
        capped_lsm(1),
        settings(),
    );
    let id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!("full_test", test_id = %id);
    let refused = svc
        .ingest(vec![make_batch(&[
            (1, "a", "v1", "upsert"),
            (2, "b", "v1", "upsert"),
        ])])
        .instrument(span)
        .await;
    assert!(
        matches!(&refused, Err(IngestError::Full(m)) if m.contains("stream store full")),
        "{refused:?}"
    );
    let lines = crate::support::logs::lines_containing(&id);
    let fatal = lines
        .iter()
        .find(|l| l.contains("FATAL:"))
        .unwrap_or_else(|| panic!("no FATAL line: {lines:?}"));
    for field in ["ERROR", "fatal=true", "held=0", "max_rows=1", "rows=2"] {
        assert!(fatal.contains(field), "{field} missing: {fatal}");
    }
    svc.close().await;
}
