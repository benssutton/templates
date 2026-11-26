//! A real LSM store whose writes wait at a gate until the test opens it, so
//! a write can be observed mid-flight (shared by `stream_ingest` and
//! `solace_ingest`).

use arrow::array::RecordBatch;
use rust_template::persistence::stream_store::lsm::LsmStore;
use rust_template::persistence::stream_store::{QueryResult, StoreError, StreamStore};

pub struct GatedStore {
    pub inner: LsmStore,
    /// No permits until the test calls `add_permits`.
    pub gate: tokio::sync::Semaphore,
    /// Notified (with a stored permit) when a write reaches the gate.
    pub entered: tokio::sync::Notify,
}

impl GatedStore {
    /// A closed gate in front of `inner`.
    pub fn new(inner: LsmStore) -> Self {
        Self {
            inner,
            gate: tokio::sync::Semaphore::new(0),
            entered: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl StreamStore for GatedStore {
    async fn ingest(&self, batches: Vec<RecordBatch>) -> Result<(), StoreError> {
        self.entered.notify_one();
        let _permit = self.gate.acquire().await.expect("gate open");
        self.inner.ingest(batches).await
    }
    fn validate(&self, batches: &[RecordBatch]) -> Result<(), StoreError> {
        self.inner.validate(batches)
    }
    fn query(&self, limit: usize) -> Result<QueryResult, StoreError> {
        self.inner.query(limit)
    }
}
