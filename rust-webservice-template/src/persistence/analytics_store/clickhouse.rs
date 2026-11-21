//! Analytics store: ClickHouse over HTTP. `::clickhouse` is the crate; this
//! module shares its name.
//!
//! The crate's HTTP client has no timeouts of its own, so every query is
//! bounded twice: server-side with `max_execution_time` and client-side
//! with `query_timeout`, which also covers a host that stopped answering.
//!
//! Schema: the versioned files in `migrations/clickhouse/` are applied at
//! startup and recorded in `schema_migrations`, so a fresh server needs no
//! init scripts. Every migration must be idempotent (`IF NOT EXISTS`):
//! ClickHouse has no lock to stop two replicas applying one together.

use crate::core::retry::{Backoff, connect_with_backoff, seconds};
use crate::health::probe::{HealthProbe, ProbeError};
use crate::settings::Settings;
use anyhow::anyhow;
use async_trait::async_trait;
use secrecy::ExposeSecret;
use std::future::Future;
use std::time::Duration;

/// `(version, sql)`, in version order.
pub const MIGRATIONS: &[(u32, &str)] = &[(
    1,
    include_str!("../../../migrations/clickhouse/0001_items.sql"),
)];

/// A smoke-tested ClickHouse client. Cheap to clone (shared HTTP pool).
#[derive(Debug, Clone)]
pub struct ClickHouseStore {
    client: ::clickhouse::Client,
    query_timeout: Duration,
}

impl ClickHouseStore {
    /// Build the client and run `SELECT 1`, retrying with backoff.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        let backoff = Backoff::from_settings(settings);
        let query_timeout = seconds(settings.clickhouse_query_timeout_seconds);
        let client = connect_with_backoff("ClickHouse", &backoff, || async {
            let client = build_client(settings);
            bounded(query_timeout, client.query("SELECT 1").fetch_one::<u8>())
                .await
                .map_err(|e| anyhow!("ClickHouse smoke test (SELECT 1) failed: {e}"))?;
            Ok::<_, anyhow::Error>(client)
        })
        .await?;
        Ok(Self {
            client,
            query_timeout,
        })
    }

    pub fn client(&self) -> &::clickhouse::Client {
        &self.client
    }

    pub fn query_timeout(&self) -> Duration {
        self.query_timeout
    }

    /// Apply the migrations this server has not recorded yet.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        let fail = |e: ::clickhouse::error::Error| anyhow!("ClickHouse migrations failed: {e}");
        let t = self.query_timeout;
        bounded(
            t,
            self.client
                .query(
                    "CREATE TABLE IF NOT EXISTS schema_migrations \
                     (version UInt32, applied_at DateTime DEFAULT now()) \
                     ENGINE = MergeTree ORDER BY version",
                )
                .execute(),
        )
        .await
        .map_err(fail)?;
        let applied: Vec<u32> = bounded(
            t,
            self.client
                .query("SELECT DISTINCT version FROM schema_migrations")
                .fetch_all::<u32>(),
        )
        .await
        .map_err(fail)?;
        for (version, sql) in MIGRATIONS {
            if applied.contains(version) {
                continue;
            }
            bounded(t, self.client.query(sql).execute())
                .await
                .map_err(fail)?;
            bounded(
                t,
                self.client
                    .query("INSERT INTO schema_migrations (version) VALUES (?)")
                    .bind(*version)
                    .execute(),
            )
            .await
            .map_err(fail)?;
            tracing::info!(version, "ClickHouse migration applied");
        }
        Ok(())
    }
}

/// `fut`, or `TimedOut` after `timeout`.
pub async fn bounded<T>(
    timeout: Duration,
    fut: impl Future<Output = Result<T, ::clickhouse::error::Error>>,
) -> Result<T, ::clickhouse::error::Error> {
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or(Err(::clickhouse::error::Error::TimedOut))
}

/// The client for `clickhouse_{host,port,user,password,database}`. No I/O.
pub fn build_client(settings: &Settings) -> ::clickhouse::Client {
    let max_execution = settings.clickhouse_query_timeout_seconds.ceil().max(1.0) as u64;
    ::clickhouse::Client::default()
        .with_url(format!(
            "http://{}:{}",
            settings.clickhouse_host, settings.clickhouse_port
        ))
        .with_user(settings.clickhouse_user.clone())
        .with_password(settings.clickhouse_password.expose_secret())
        .with_database(settings.clickhouse_database.clone())
        .with_setting("max_execution_time", max_execution.to_string())
}

#[async_trait]
impl HealthProbe for ClickHouseStore {
    fn name(&self) -> &'static str {
        "clickhouse"
    }

    async fn check(&self) -> Result<(), ProbeError> {
        bounded(
            self.query_timeout,
            self.client.query("SELECT 1").fetch_one::<u8>(),
        )
        .await
        .map(|_| ())
        .map_err(|e| ProbeError::Unavailable(e.to_string()))
    }
}
