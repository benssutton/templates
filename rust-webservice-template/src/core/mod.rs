//! Cross-cutting infrastructure: request ids, Server-Timing, CORS, JSON
//! error bodies, authentication, last-request tracking, retry, shutdown,
//! system metrics and telemetry.

pub mod auth;
pub mod boundary_timing;
pub mod cors;
pub mod json_errors;
pub mod last_request;
pub mod request_id;
pub mod retry;
pub mod shutdown;
pub mod system_metrics;
pub mod telemetry;

/// rustls needs one process-wide crypto provider: ring (aws-lc needs extra
/// native tooling on Windows). Every TLS user (the HTTPS server, the SEMP
/// and healthcheck clients) calls this first; repeated calls are no-ops.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
