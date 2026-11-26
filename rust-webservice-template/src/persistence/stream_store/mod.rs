//! Stream store: the in-memory LSM that backs `/data/cache`.
//! `StreamIngestService` writes through the `StreamStore` trait, a real
//! seam: tests supply a store whose write fails or blocks, with no patching.

pub mod lsm;

use crate::schemas::health::StreamStoreStats;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The batch can never be merged (a missing key or `op` column, or a
    /// schema that conflicts with the store). Retrying cannot succeed, so
    /// transports reject it and never redeliver it; HTTP answers 422.
    #[error("{0}")]
    InvalidBatch(String),
    /// The store holds `max_rows` rows, or this message would take it over,
    /// and it refuses every new message until it is rebuilt. Redelivery cannot
    /// succeed, so transports reject it; HTTP answers 507.
    #[error("stream store full: {held} rows held, max_rows {max_rows}; message discarded")]
    Full { held: u64, max_rows: u64 },
    /// Anything else. It may be transient, so a transport fails the delivery
    /// for redelivery; HTTP answers 500.
    #[error("stream store failure: {0}")]
    Internal(String),
}

impl From<ArrowError> for StoreError {
    fn from(err: ArrowError) -> Self {
        Self::Internal(err.to_string())
    }
}

/// The result of `query(limit)`: the live rows (the order and `op` columns
/// dropped), sorted by key, at most `limit` of them (0 = all), and the total
/// number of live rows.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Option<RecordBatch>,
    pub total: usize,
}

#[async_trait::async_trait]
pub trait StreamStore: Send + Sync + 'static {
    /// Write one message: all its batches, in order, or none of them.
    /// Readers see the whole message at once. Implementations serialise
    /// concurrent callers: there is a single logical writer.
    async fn ingest(&self, batches: Vec<RecordBatch>) -> Result<(), StoreError>;

    /// Check a whole message before writing any of it: every batch in order,
    /// by the rules `ingest` applies, against the store's fixed schema or, if
    /// none is fixed yet, the first non-empty batch of the slice. Returns the
    /// first `InvalidBatch`. No side effects; `ingest` checks the message
    /// again under its lock, so a concurrent write that fixes the schema in
    /// between makes it refuse the whole message.
    fn validate(&self, batches: &[RecordBatch]) -> Result<(), StoreError>;

    /// Lock-free read of the latest snapshot. It is CPU-bound, so async
    /// callers run it with `spawn_blocking`.
    fn query(&self, limit: usize) -> Result<QueryResult, StoreError>;

    /// Size and capacity counters, for health and metrics. Cheap, no I/O.
    fn stats(&self) -> StreamStoreStats {
        StreamStoreStats::default()
    }
}
