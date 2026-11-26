//! StreamIngestService: a tokio task that drains a `BatchConsumer`, writes
//! every batch through the `StreamStore`, then settles the delivery once:
//!
//! | situation                                              | settlement |
//! |--------------------------------------------------------|------------|
//! | every batch written                                    | accept     |
//! | store error                                            | fail + jittered, doubling, capped backoff |
//! | invalid batch, or decoded size > max_ingest_batch_bytes | reject    |
//! | store full (it refuses every new message)              | reject     |
//! | payload not Arrow IPC                                  | rejected inside the consumer |
//!
//! `POST /data/ingest` goes through the same `ingest` call, so both paths
//! apply the same rules: every batch is size-checked and validated (by the
//! store and against the read model `GET /data/cache` serves) before any is
//! written, and the store then writes the message under one lock, all its
//! batches or none, so a refused message leaves nothing in the store.
//!
//! Resilience: five consecutive `next_delivery()` errors, or the transport
//! staying out of CONNECTED longer than `ingest_max_disconnect_seconds`,
//! trigger the app's `Shutdown`. With that setting unset, neither fires. A
//! delivery or a rise in the consumer's `bound_epoch` resets the count.

use crate::core::boundary_timing::timed;
use crate::core::request_id::new_id;
use crate::core::retry::{Backoff, seconds};
use crate::core::shutdown::Shutdown;
use crate::error::AppError;
use crate::ingestion::ipc::batch_bytes;
use crate::ingestion::{BatchConsumer, ConnectionState, Delivery, Settlement};
use crate::persistence::stream_store::lsm::OP_COLUMN;
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

/// How long `close` waits for each task.
pub const JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The settlement an ingest result calls for: bad data is rejected, anything
/// else is retried.
fn outcome_of(result: &Result<(), IngestError>) -> Settlement {
    match result {
        Ok(()) => Settlement::Accept,
        Err(IngestError::TooLarge(_) | IngestError::Invalid(_) | IngestError::Full(_)) => {
            Settlement::Reject
        }
        Err(_) => Settlement::Fail,
    }
}

/// Why a message was not written.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// A decoded batch is over `max_ingest_batch_bytes`; nothing was written.
    #[error("{0}")]
    TooLarge(String),
    /// A batch can never be stored or served; nothing was written.
    #[error("{0}")]
    Invalid(String),
    /// The store is full and refuses every new message until it is
    /// rebuilt; nothing of this one was written.
    #[error("{0}")]
    Full(String),
    /// The write itself failed; may be transient.
    #[error(transparent)]
    Store(StoreError),
}

impl From<StoreError> for IngestError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::InvalidBatch(msg) => Self::Invalid(msg),
            full @ StoreError::Full { .. } => Self::Full(full.to_string()),
            other => Self::Store(other),
        }
    }
}

impl From<IngestError> for AppError {
    fn from(err: IngestError) -> Self {
        match err {
            IngestError::TooLarge(msg) => Self::PayloadTooLarge(msg),
            IngestError::Invalid(msg) => Self::Unprocessable(msg),
            IngestError::Full(msg) => Self::InsufficientStorage(msg),
            IngestError::Store(err) => Self::Internal(err.into()),
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
/// data; the value is still usable.
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

    /// True while the ingest loop runs.
    pub fn is_running(&self) -> bool {
        lock(&self.loop_task)
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }

    /// The consumer's `bound_epoch`, for tests: not part of the health
    /// contract.
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

    /// `GET /data/cache`: the live rows, ordered by key.
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

    /// Admit and write one message: all its batches or none. Shared by the
    /// stream and `POST /data/ingest`.
    pub async fn ingest(&self, batches: Vec<RecordBatch>) -> Result<(), IngestError> {
        self.admit(&batches).await?;
        timed("ingest.lsm_write", self.write(batches)).await?;
        Ok(())
    }

    /// Size, then the store's rules and the read model's, on the blocking
    /// pool (the store may run the merge kernels on a trial frame).
    async fn admit(&self, batches: &[RecordBatch]) -> Result<(), IngestError> {
        let max = self.settings.max_ingest_batch_bytes;
        for batch in batches {
            let bytes = batch_bytes(batch);
            if bytes > max {
                return Err(IngestError::TooLarge(format!(
                    "decoded batch of {bytes} bytes exceeds max_ingest_batch_bytes ({max})"
                )));
            }
        }
        let store = self.store.clone();
        let batches = batches.to_vec();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                store.validate(&batches)?;
                batches.iter().try_for_each(check_read_model)
            })
        })
        .await
        .map_err(|e| task_failed("LSM validate", e))??;
        Ok(())
    }

    /// Write one message, then record it (`last_batch_at`, the row total,
    /// `ingest_rows_ingested_total`). The write and its bookkeeping are one
    /// detached task: if this future is dropped (an HTTP client disconnects)
    /// the task still runs to the end, so a committed message is always
    /// counted. The caller's span (the request id) is carried into it.
    async fn write(&self, batches: Vec<RecordBatch>) -> Result<(), StoreError> {
        let store = self.store.clone();
        let progress = self.progress.clone();
        let metrics = self.metrics.clone();
        let task = async move {
            let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let count = batches.len();
            store.ingest(batches).await?;
            *lock(&progress.last_batch_at) = Some(Utc::now().trunc_subsecs(6));
            progress.rows_total.fetch_add(rows, Ordering::Relaxed);
            if let Some(m) = &metrics {
                m.record_ingested_rows(rows);
            }
            tracing::debug!(rows, batches = count, "ingested batch");
            Ok(())
        };
        tokio::spawn(task.instrument(tracing::Span::current()))
            .await
            .map_err(|e| task_failed("LSM write", e))?
    }

    /// `POST /data/ingest`: ingest, then count the outcome like a delivery.
    pub async fn ingest_http(&self, batches: Vec<RecordBatch>) -> Result<(), IngestError> {
        let result = self.ingest(batches).await;
        if let Some(m) = &self.metrics {
            m.record_outcome(outcome_of(&result).metric_label());
        }
        result
    }

    /// Write every batch and decide the settlement. Never settles.
    async fn decide(&self, batches: Vec<RecordBatch>) -> Settlement {
        let result = self.ingest(batches).await;
        let outcome = outcome_of(&result);
        if let Err(err) = &result {
            if outcome == Settlement::Reject {
                tracing::error!(error = %err, "rejecting message");
            } else {
                tracing::error!(error = %err, "ingest failed; message will be redelivered");
            }
        }
        outcome
    }

    /// Decide, then settle exactly once. `None` when the settlement itself
    /// failed (logged; the broker still holds the message and redelivers it).
    async fn settle(&self, delivery: Delivery) -> Option<Settlement> {
        let Delivery {
            batches, settle, ..
        } = delivery;
        let outcome = self.decide(batches).await;
        let settled = match outcome {
            Settlement::Accept => settle.accept().await,
            Settlement::Fail => settle.fail().await,
            Settlement::Reject => settle.reject().await,
        };
        match settled {
            Ok(()) => {
                if let Some(m) = &self.metrics {
                    m.record_outcome(outcome.metric_label());
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
            base_delay: seconds(self.settings.ingest_retry_base_delay),
            max_delay: seconds(self.settings.ingest_retry_max_delay),
        };
        // 0 disables the failure-count shutdown, independently of the watchdog.
        let max_failures = self.settings.ingest_max_consecutive_failures;
        // Transport failures in a row; also the transport backoff's attempt number.
        let mut transport_failures = 0_u32;
        let mut last_epoch = self.consumer.bound_epoch();
        // FAILED settlements in a row, with their own backoff: fail() makes the
        // broker redeliver at once, so a persistent store error would otherwise spin.
        let mut settle_failures = 0_u32;
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
                transport_failures = 0;
            }
            match next {
                Ok(None) => break, // the consumer was closed
                Ok(Some(delivery)) => {
                    transport_failures = 0;
                    let span = tracing::info_span!("ingest", request_id = %new_id());
                    match self.settle(delivery).instrument(span).await {
                        Some(Settlement::Fail) => {
                            settle_failures += 1;
                            if !self.pause(backoff.jittered(settle_failures)).await {
                                break;
                            }
                        }
                        Some(_) => settle_failures = 0,
                        None => {}
                    }
                }
                Err(err) => {
                    transport_failures += 1;
                    tracing::error!(
                        error = %format!("{err:#}"),
                        "consumer next_delivery() failed (failure {transport_failures}/{max_failures})"
                    );
                    if max_failures > 0 && transport_failures >= max_failures {
                        tracing::error!(
                            "ingest: {transport_failures} consecutive failures; requesting shutdown"
                        );
                        self.shutdown
                            .trigger("ingest: consecutive consumer failures");
                        break;
                    }
                    if !self.pause(backoff.jittered(transport_failures)).await {
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

    /// Cheap, synchronous read of ingest state (no I/O).
    pub fn ingest_health(&self) -> IngestHealth {
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
            transport: self.settings.ingest_transport,
            connection_state: self.consumer.connection_state(),
            thread_alive: self.is_running(),
            last_batch_at: last,
            seconds_since_last_batch: since.map(round3),
            rows_ingested_total: self.progress.rows_total.load(Ordering::Relaxed),
            stale,
            store: self.store.stats(),
        }
    }
}

/// The columns `GET /data/cache` serves: `id` (a non-negative integer),
/// `name`, `value` (strings). Rows that are not tombstones need all three.
const READ_MODEL: [&str; 3] = ["id", "name", "value"];

fn read_model_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef, StoreError> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| StoreError::InvalidBatch(format!("batch has no `{name}` column")))?;
    let (to, ok) = if name == "id" {
        (DataType::Int64, array.data_type().is_integer())
    } else {
        (
            DataType::Utf8,
            matches!(
                array.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ),
        )
    };
    if !ok {
        return Err(StoreError::InvalidBatch(format!(
            "column `{name}` has type {}, which GET /data/cache cannot serve",
            array.data_type()
        )));
    }
    cast(array, &to).map_err(|e| StoreError::InvalidBatch(format!("column `{name}`: {e}")))
}

/// Refuse a batch whose rows `to_rows` could not turn into a response, so
/// one bad write cannot make every later read fail.
fn check_read_model(batch: &RecordBatch) -> Result<(), StoreError> {
    let [ids, names, values] = READ_MODEL.map(|c| read_model_column(batch, c));
    let (ids, names, values) = (ids?, names?, values?);
    let ids = ids.as_primitive::<Int64Type>();
    let op = batch
        .column_by_name(OP_COLUMN)
        .map(|op| cast(op, &DataType::Utf8))
        .transpose()
        .map_err(|e| StoreError::InvalidBatch(format!("column `{OP_COLUMN}`: {e}")))?;
    let op = op.as_ref().map(|op| op.as_string::<i32>());
    for i in 0..batch.num_rows() {
        let tombstone = op.is_some_and(|op| op.is_valid(i) && op.value(i) == "delete");
        if tombstone {
            continue;
        }
        if ids.is_null(i) || ids.value(i) < 0 {
            return Err(StoreError::InvalidBatch(format!(
                "row {i}: `id` must be a non-negative integer"
            )));
        }
        if names.is_null(i) || values.is_null(i) {
            return Err(StoreError::InvalidBatch(format!(
                "row {i}: `name` and `value` must not be null"
            )));
        }
    }
    Ok(())
}

/// LSM rows -> response rows. `check_read_model` admitted every row, so a
/// failure here is a bug (500).
fn to_rows(batch: &RecordBatch) -> Result<Vec<DataRowResponse>, AppError> {
    let column = |name: &str| {
        read_model_column(batch, name).map_err(|e| AppError::Internal(anyhow::anyhow!("{e}")))
    };
    let (ids, names, values) = (column("id")?, column("name")?, column("value")?);
    let (ids, names, values) = (
        ids.as_primitive::<Int64Type>(),
        names.as_string::<i32>(),
        values.as_string::<i32>(),
    );
    (0..batch.num_rows())
        .map(|i| {
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
