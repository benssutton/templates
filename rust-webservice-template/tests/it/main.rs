//! The ONE integration-test binary. Every `tests/*.rs` file would be its own
//! cargo binary (and, from R2 on, start its own containers), so all
//! integration tests live here as modules. Add one `mod` per feature.

mod boundary_timing;
mod correlation;
mod cors;
mod docs;
mod health;
mod health_service;
mod mcp;
mod metrics;
mod metrics_service;
mod probes;
mod request_limits;
mod retry;
mod server_timing;
mod settings;
mod startup;
mod support;
mod system_metrics;
mod tls;
