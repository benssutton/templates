use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    /// Detail is logged server-side only; clients see "unavailable".
    #[error("{0}")]
    Unavailable(String),
}

/// A dependency health check. Registered only after the dependency has
/// connected and passed its smoke test (spec §5).
#[async_trait]
pub trait HealthProbe: Send + Sync {
    /// Stable name used in `/health/ready`, `/health/status` and the
    /// `dependency_up{name=...}` metric ("postgres", "clickhouse", "redis").
    fn name(&self) -> &'static str;
    async fn check(&self) -> Result<(), ProbeError>;
}
