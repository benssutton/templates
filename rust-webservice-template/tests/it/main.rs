#![allow(clippy::unwrap_used, clippy::expect_used)]
//! The ONE integration-test binary. Every `tests/*.rs` file would be its own
//! cargo binary (and start its own containers), so all integration tests
//! live here as modules. Add one `mod` per feature; Solace modules need the
//! `solace` feature.

#[cfg(feature = "solace")]
mod amqp_publisher;
mod auth;
mod boundary_timing;
mod cache;
mod config;
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
mod overload;
mod probes;
mod request_id;
mod request_limits;
mod retry;
mod secrets;
mod server_timing;
mod services;
mod settings;
#[cfg(feature = "solace")]
mod smf_interop;
#[cfg(feature = "solace")]
mod solace_broker;
#[cfg(feature = "solace")]
mod solace_consumer;
#[cfg(feature = "solace")]
mod solace_ingest;
#[cfg(feature = "solace")]
mod solace_provision;
#[cfg(feature = "solace")]
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
