//! The ONE integration-test binary. Every `tests/*.rs` file would be its own
//! cargo binary (and, from R2 on, start its own containers), so all
//! integration tests live here as modules. Add one `mod` per feature.

mod amqp_publisher;
mod boundary_timing;
mod cache;
mod config;
mod correlation;
mod cors;
mod data;
mod docker;
mod docs;
mod health;
mod health_service;
mod http_ingest;
mod mcp;
mod metrics;
mod metrics_ingest;
mod metrics_service;
mod noop_consumer;
mod probes;
mod request_limits;
mod retry;
mod secrets;
mod server_timing;
mod services;
mod settings;
mod smf_interop;
mod solace_broker;
mod solace_consumer;
mod solace_ingest;
mod solace_provision;
mod solace_resilience;
mod startup;
mod store_clickhouse;
mod store_health;
mod store_postgres;
mod store_redis;
mod stores;
mod stream_ingest;
mod support;
mod system_metrics;
mod tls;
