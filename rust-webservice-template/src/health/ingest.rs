use crate::schemas::health::IngestHealth;

/// Cheap, synchronous read of ingest state (no I/O). `StreamIngestService`
/// implements it; tests supply fixed states to drive the readiness rules.
pub trait IngestHealthProvider: Send + Sync {
    fn ingest_health(&self) -> IngestHealth;
}

impl IngestHealthProvider for crate::services::stream_ingest::StreamIngestService {
    fn ingest_health(&self) -> IngestHealth {
        StreamIngestService::ingest_health(self)
    }
}

use crate::services::stream_ingest::StreamIngestService;
