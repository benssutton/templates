//! StreamIngestService (Python services/stream_ingest.py): a tokio task that
//! drains a `BatchConsumer`, writes every batch through the `StreamStore`,
//! then settles the delivery exactly once:
//!
//! | situation                                              | settlement |
//! |--------------------------------------------------------|------------|
//! | every batch written                                    | accept     |
//! | store error                                            | fail + jittered, doubling, capped backoff |
//! | InvalidBatch, or decoded size > max_ingest_batch_bytes | reject     |
//! | payload not Arrow IPC                                  | rejected inside the consumer |
//!
//! All-or-nothing: every batch of a delivery is size-checked and validated
//! before any is written, so a rejected delivery leaves nothing in the store.
//!
//! Resilience: five consecutive `next_delivery()` errors, or the transport
//! staying out of CONNECTED longer than `ingest_max_disconnect_seconds`,
//! trigger the app's `Shutdown`. With that setting unset, neither fires. A
//! delivery or a rise in the consumer's `bound_epoch` resets the count.
//!
//! The LSM write (merge, flush, compaction) is CPU-bound; `LsmStore::ingest`
//! runs it on the blocking pool, never on an async worker (Python runs
//! ingest on its own thread). Both ingest paths, the loop and
//! `POST /data/ingest`, go through `write`, which makes each write and its
//! bookkeeping one detached task that a dropped caller cannot cut short.

use crate::core::boundary_timing::timed;
use crate::core::correlation::new_id;
use crate::core::retry::{seconds, Backoff};
use crate::core::shutdown::Shutdown;
use crate::error::AppError;
use crate::health::ingest::IngestHealthProvider;
use crate::ingestion::ipc::batch_bytes;
use crate::ingestion::{BatchConsumer, ConnectionState, Delivery};
use crate::persistence::stream_store::{StoreError, StreamStore};
use crate::schemas::data::{DataRowResponse, DataRowsResponse};
use crate::schemas::health::IngestHealth;
use crate::services::metrics::MetricsService;
use crate::settings::Settings;
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int64Type};
use chrono::{DateTime, SubsecRound, Utc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// Wait before the first retry (transport errors and settlement fails).
pub const INGEST_BASE_DELAY: Duration = Duration::from_secs(1);
/// Retry cap.
pub const INGEST_MAX_DELAY: Duration = Duration::from_secs(60);
/// Consecutive `next_delivery()` failures before shutdown.
pub const INGEST_MAX_FAILURES: u32 = 5;
/// How long `close` waits for each task.
pub const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Accept,
    Fail,
    Reject,
}

impl Outcome {
    /// `ingest_deliveries_total{outcome=...}` label.
    pub fn metric_label(self) -> &'static str {
        match self {
            Self::Accept => "accepted",
            Self::Fail => "failed",
            Self::Reject => "rejected",
        }
    }
}

pub struct StreamIngestService {
    settings: Arc<Settings>,
    consumer: Arc<dyn BatchConsumer>,
    store: Arc<dyn StreamStore>,
    shutdown: Shutdown,
    metrics: Option<Arc<MetricsService>>,
    /// Child of the app's shutdown token: cancelled by `Shutdown::trigger`
    /// or by `close`. Wakes every backoff wait so stopping is prompt.
    stop: CancellationToken,
    started_at: DateTime<Utc>,
    progress: Arc<Progress>,
    loop_task: Mutex<Option<JoinHandle<()>>>,
    watchdog_task: Mutex<Option<JoinHandle<()>>>,
}

/// What `ingest_health` reports about written rows. Shared with each
/// detached write task, which updates it right after its commit.
#[derive(Default)]
struct Progress {
    last_batch_at: Mutex<Option<DateTime<Utc>>>,
    rows_total: AtomicU64,
}

/// A poisoned lock only means another thread panicked mid-update of plain
/// data; the value is still usable (no `unwrap` in `src/`).
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn secs_between(from: DateTime<Utc>, to: DateTime<Utc>) -> f64 {
    (to - from)
        .num_microseconds()
        .map_or(f64::MAX, |us| us as f64 / 1_000_000.0)
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

fn task_failed(what: &str, err: tokio::task::JoinError) -> StoreError {
    StoreError::Internal(format!("{what} task failed: {err}"))
}

impl StreamIngestService {
    /// Spawn the ingest loop (and the watchdog when
    /// `ingest_max_disconnect_seconds` is set). Must run inside a tokio runtime.
    pub fn start(
        settings: Arc<Settings>,
        consumer: Arc<dyn BatchConsumer>,
        store: Arc<dyn StreamStore>,
        shutdown: Shutdown,
        metrics: Option<Arc<MetricsService>>,
    ) -> Arc<Self> {
        let svc = Arc::new(Self {
            stop: shutdown.token().child_token(),
            settings,
            consumer,
            store,
            shutdown,
            metrics,
            started_at: Utc::now(),
            progress: Arc::default(),
            loop_task: Mutex::new(None),
            watchdog_task: Mutex::new(None),
        });
        *lock(&svc.loop_task) = Some(tokio::spawn(svc.clone().run()));
        if let Some(threshold) = svc.settings.ingest_max_disconnect_seconds {
            *lock(&svc.watchdog_task) = Some(tokio::spawn(svc.clone().watchdog(threshold)));
        }
        svc
    }

    /// True while the ingest loop runs (Python's `thread_alive`).
    pub fn is_running(&self) -> bool {
        lock(&self.loop_task)
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }

    /// The consumer's `bound_epoch`, for tests. Deliberately not part of
    /// `IngestHealth`: the /health JSON keeps Python parity.
    #[doc(hidden)]
    pub fn bound_epoch(&self) -> u64 {
        self.consumer.bound_epoch()
    }

    /// Stop the loop and the watchdog, close the consumer, and wait for both
    /// tasks (at most `JOIN_TIMEOUT` each). Idempotent.
    pub async fn close(&self) {
        self.stop.cancel();
        self.consumer.close().await;
        let handles: Vec<JoinHandle<()>> = [
            lock(&self.loop_task).take(),
            lock(&self.watchdog_task).take(),
        ]
        .into_iter()
        .flatten()
        .collect();
        for handle in handles {
            if tokio::time::timeout(JOIN_TIMEOUT, handle).await.is_err() {
                tracing::error!(
                    "ingest task did not stop within {}s; abandoning",
                    JOIN_TIMEOUT.as_secs()
                );
            }
        }
    }

    /// `GET /data/cache` (Python `get_data`).
    pub async fn get_data(&self, limit: u32) -> Result<DataRowsResponse, AppError> {
        let store = self.store.clone();
        let query = tokio::task::spawn_blocking(move || store.query(limit as usize));
        let result = timed("lsm.query", query)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("LSM query task failed: {e}")))?
            .map_err(|e| AppError::Internal(e.into()))?;
        let rows = match &result.rows {
            Some(batch) => to_rows(batch)?,
            None => Vec::new(),
        };
        Ok(DataRowsResponse {
            rows,
            total: result.total as u64,
            limit,
        })
    }

    /// `POST /data/ingest` (Python `ingest_batch`): straight to the LSM, no
    /// `Delivery`. An oversized batch is dropped and logged (`Ok`); an
    /// unmergeable one is `Err(InvalidBatch)` (the router answers 422).
    pub async fn ingest_batch(&self, batch: RecordBatch) -> Result<(), StoreError> {
        timed("ingest.lsm_write", self.record_ingest(batch))
            .await
            .map(|_| ())
    }

    /// True (and logged) if the decoded batch exceeds `max_ingest_batch_bytes`.
    fn oversized(&self, batch: &RecordBatch) -> bool {
        let bytes = batch_bytes(batch);
        let max = self.settings.max_ingest_batch_bytes;
        if bytes > max {
            tracing::error!(
                "dropping ingest batch: {bytes} bytes exceeds max_ingest_batch_bytes ({max})"
            );
            return true;
        }
        false
    }

    /// Write one batch, then record it (`last_batch_at`, the row total,
    /// `ingest_rows_total`). The write and its bookkeeping are one detached
    /// task: if this future is dropped (an HTTP client disconnects) the task
    /// still runs to the end, so a committed batch is always counted. The
    /// caller's span (the delivery's correlation ID) is carried into it.
    async fn write(&self, batch: RecordBatch) -> Result<(), StoreError> {
        let store = self.store.clone();
        let progress = self.progress.clone();
        let metrics = self.metrics.clone();
        let task = async move {
            let rows = batch.num_rows() as u64;
            store.ingest(batch).await?;
            *lock(&progress.last_batch_at) = Some(Utc::now().trunc_subsecs(6));
            progress.rows_total.fetch_add(rows, Ordering::Relaxed);
            if let Some(m) = &metrics {
                m.record_ingested_rows(rows);
            }
            tracing::debug!(rows, "ingested batch");
            Ok(())
        };
        tokio::spawn(task.instrument(tracing::Span::current()))
            .await
            .map_err(|e| task_failed("LSM write", e))?
    }

    /// The store's whole-message check, on the blocking pool (it may run the
    /// merge kernels on a trial frame).
    async fn validate(&self, batches: &[RecordBatch]) -> Result<(), StoreError> {
        let store = self.store.clone();
        let batches = batches.to_vec();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || span.in_scope(|| store.validate(&batches)))
            .await
            .map_err(|e| task_failed("LSM validate", e))?
    }

    /// Write one batch. `Ok(false)` = refused as oversized (logged).
    async fn record_ingest(&self, batch: RecordBatch) -> Result<bool, StoreError> {
        if self.oversized(&batch) {
            return Ok(false);
        }
        self.write(batch).await?;
        Ok(true)
    }

    /// Write every batch and decide the outcome. Never settles. All or
    /// nothing (Python `_decide`): every batch is size-checked and validated
    /// before any is written, so a rejected message normally leaves nothing
    /// in the store. One narrow race remains, as in Python: a concurrent
    /// HTTP write can fix the schema between `validate` and a later batch's
    /// write; that batch is then refused (`InvalidBatch`) and the delivery is
    /// rejected with its earlier batches already written. A failed message
    /// is redelivered whole, so batches written before an unexpected write
    /// error are re-applied (harmless under newest-wins merge semantics).
    async fn decide(&self, batches: &[RecordBatch]) -> Outcome {
        if batches.iter().any(|b| self.oversized(b)) {
            return Outcome::Reject;
        }
        let written = async {
            self.validate(batches).await?;
            for batch in batches {
                self.write(batch.clone()).await?;
            }
            Ok::<(), StoreError>(())
        }
        .await;
        match written {
            Ok(()) => Outcome::Accept,
            Err(StoreError::InvalidBatch(msg)) => {
                tracing::error!(error = %msg, "rejecting unmergeable message");
                Outcome::Reject
            }
            Err(err) => {
                tracing::error!(error = %err, "ingest failed; message will be redelivered");
                Outcome::Fail
            }
        }
    }

    /// Decide, then settle exactly once. `None` when the settlement itself
    /// failed (logged; the broker still holds the message and redelivers it).
    async fn settle(&self, delivery: Delivery) -> Option<Outcome> {
        let Delivery {
            batches, settle, ..
        } = delivery;
        let outcome = self.decide(&batches).await;
        let settled = match outcome {
            Outcome::Accept => settle.accept().await,
            Outcome::Fail => settle.fail().await,
            Outcome::Reject => settle.reject().await,
        };
        match settled {
            Ok(()) => {
                if let Some(m) = &self.metrics {
                    m.record_settlement(outcome.metric_label());
                }
                Some(outcome)
            }
            Err(err) if self.stop.is_cancelled() => {
                tracing::warn!(
                    error = %format!("{err:#}"),
                    "settlement failed during shutdown; broker will redeliver"
                );
                None
            }
            Err(err) => {
                tracing::error!(
                    error = %format!("{err:#}"),
                    "settlement failed; broker will redeliver"
                );
                None
            }
        }
    }

    /// Sleep for `d` unless stopping. Returns false when stopping.
    async fn pause(&self, d: Duration) -> bool {
        tokio::select! {
            biased;
            _ = self.stop.cancelled() => false,
            _ = tokio::time::sleep(d) => true,
        }
    }

    async fn run(self: Arc<Self>) {
        let backoff = Backoff {
            max_attempts: u32::MAX,
            base_delay: INGEST_BASE_DELAY,
            max_delay: INGEST_MAX_DELAY,
        };
        // ingest_max_disconnect_seconds=None disables *all* automatic
        // shutdown: the watchdog and this consecutive-failure trigger.
        let shutdown_on_failure = self.settings.ingest_max_disconnect_seconds.is_some();
        // Transport failures; also the transport backoff's attempt number.
        let mut consecutive_failures = 0_u32;
        let mut last_epoch = self.consumer.bound_epoch();
        // Settlement backoff, separate from the transport backoff: fail()
        // makes the broker redeliver at once, so a persistent store error
        // would otherwise spin.
        let mut consecutive_fails = 0_u32;
        loop {
            let next = tokio::select! {
                biased;
                _ = self.stop.cancelled() => break,
                next = self.consumer.next_delivery() => next,
            };
            // A bind (epoch rise) or a delivery proves the transport works:
            // without this, healed interruptions on a quiet queue would add
            // up to a shutdown over time.
            let epoch = self.consumer.bound_epoch();
            if epoch > last_epoch {
                last_epoch = epoch;
                consecutive_failures = 0;
            }
            match next {
                Ok(None) => break, // the consumer was closed
                Ok(Some(delivery)) => {
                    consecutive_failures = 0;
                    let span = tracing::info_span!("ingest", correlation_id = %new_id());
                    match self.settle(delivery).instrument(span).await {
                        Some(Outcome::Fail) => {
                            consecutive_fails += 1;
                            if !self.pause(backoff.jittered(consecutive_fails)).await {
                                break;
                            }
                        }
                        Some(_) => consecutive_fails = 0,
                        None => {}
                    }
                }
                Err(err) => {
                    consecutive_failures += 1;
                    tracing::error!(
                        error = %format!("{err:#}"),
                        "consumer next_delivery() failed (failure {consecutive_failures}/{INGEST_MAX_FAILURES})"
                    );
                    if shutdown_on_failure && consecutive_failures >= INGEST_MAX_FAILURES {
                        tracing::error!(
                            "ingest: {consecutive_failures} consecutive failures; requesting shutdown"
                        );
                        self.shutdown
                            .trigger("ingest: consecutive consumer failures");
                        break;
                    }
                    if !self.pause(backoff.jittered(consecutive_failures)).await {
                        break;
                    }
                }
            }
        }
        self.consumer.close().await;
    }

    async fn watchdog(self: Arc<Self>, threshold: f64) {
        let poll = seconds((threshold / 2.0).min(5.0));
        let limit = seconds(threshold);
        let mut disconnected_since: Option<tokio::time::Instant> = None;
        loop {
            if !self.pause(poll).await {
                return;
            }
            if self.consumer.connection_state() == ConnectionState::Connected {
                disconnected_since = None;
                continue;
            }
            let now = tokio::time::Instant::now();
            match disconnected_since {
                None => disconnected_since = Some(now),
                // Never once `close()` has begun: a consumer being closed
                // reports DOWN, which is not a disconnect.
                Some(since) if now - since >= limit && !self.stop.is_cancelled() => {
                    tracing::error!(
                        "ingest transport not connected for {threshold:.0}s; requesting shutdown"
                    );
                    self.shutdown
                        .trigger("ingest watchdog: transport disconnected");
                    return;
                }
                Some(_) => {}
            }
        }
    }
}

impl IngestHealthProvider for StreamIngestService {
    fn ingest_health(&self) -> IngestHealth {
        let now = Utc::now();
        let last = *lock(&self.progress.last_batch_at);
        let since = last.map(|t| secs_between(t, now));
        // Stale when configured and either a batch arrived too long ago, or
        // none ever did (time since start stands in).
        let stale = match self.settings.ingest_staleness_threshold_seconds {
            Some(threshold) => {
                since.unwrap_or_else(|| secs_between(self.started_at, now)) > threshold
            }
            None => false,
        };
        IngestHealth {
            transport: self.settings.ingest_transport.as_str().into(),
            connection_state: self.consumer.connection_state().as_str().into(),
            thread_alive: self.is_running(),
            last_batch_at: last,
            seconds_since_last_batch: since.map(round3),
            rows_ingested_total: self.progress.rows_total.load(Ordering::Relaxed),
            stale,
        }
    }
}

/// LSM rows -> Python's `DataRowResponse(**r)`. A row that does not fit
/// (`id` negative or null, a missing column) is a 500, as pydantic's
/// validation error is in Python.
fn to_rows(batch: &RecordBatch) -> Result<Vec<DataRowResponse>, AppError> {
    let col = |name: &str, to: &DataType| -> Result<ArrayRef, AppError> {
        let array = batch.column_by_name(name).ok_or_else(|| {
            AppError::Internal(anyhow::anyhow!("LSM rows have no `{name}` column"))
        })?;
        cast(array, to).map_err(|e| AppError::Internal(e.into()))
    };
    let (ids, names, values) = (
        col("id", &DataType::Int64)?,
        col("name", &DataType::Utf8)?,
        col("value", &DataType::Utf8)?,
    );
    let (ids, names, values) = (
        ids.as_primitive::<Int64Type>(),
        names.as_string::<i32>(),
        values.as_string::<i32>(),
    );
    (0..batch.num_rows())
        .map(|i| {
            if ids.is_null(i) || names.is_null(i) || values.is_null(i) {
                return Err(AppError::Internal(anyhow::anyhow!(
                    "LSM row {i} has a null field"
                )));
            }
            let id = u64::try_from(ids.value(i)).map_err(|_| {
                AppError::Internal(anyhow::anyhow!("LSM row id {} is negative", ids.value(i)))
            })?;
            Ok(DataRowResponse {
                id,
                name: names.value(i).to_owned(),
                value: values.value(i).to_owned(),
            })
        })
        .collect()
}
