//! Health seams: dependency probes (each store implements `HealthProbe`)
//! and ingest health (`StreamIngestService` implements `IngestHealthProvider`).

pub mod ingest;
pub mod probe;
pub mod registry;
