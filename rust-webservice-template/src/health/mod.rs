//! Health seams: dependency probes (R2 stores implement `HealthProbe`) and
//! ingest health (R3's StreamIngestService implements `IngestHealthProvider`).

pub mod ingest;
pub mod probe;
pub mod registry;
