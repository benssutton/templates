use crate::schemas::health::IngestHealth;

/// Cheap, synchronous read of ingest state (no I/O). R3's
/// StreamIngestService implements this; R1 uses `NoopIngestHealth`.
pub trait IngestHealthProvider: Send + Sync {
    fn ingest_health(&self) -> IngestHealth;
}

/// Mirrors Python's `noop` consumer: always connected, never ingests.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopIngestHealth;

impl IngestHealthProvider for NoopIngestHealth {
    fn ingest_health(&self) -> IngestHealth {
        IngestHealth {
            transport: "noop".into(),
            connection_state: "connected".into(),
            thread_alive: true,
            last_batch_at: None,
            seconds_since_last_batch: None,
            rows_ingested_total: 0,
            stale: false,
        }
    }
}
