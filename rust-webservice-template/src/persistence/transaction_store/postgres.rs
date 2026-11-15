//! Transaction store: Postgres on the pgvector image (Python
//! `persistence/transaction_store/postgres/postgres_client.py`).

use crate::core::retry::{connect_with_backoff, Backoff};
use crate::health::probe::{HealthProbe, ProbeError};
use crate::settings::Settings;
use anyhow::anyhow;
use async_trait::async_trait;
use secrecy::ExposeSecret;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, PgPoolOptions};
use sqlx::Connection;
use std::str::FromStr;
use std::time::Duration;

/// Applied on every startup and idempotent: the `vector` extension first,
/// then the `configuration` table.
pub const SCHEMA_SQL: &str = include_str!("../../../scripts/postgres-init.sql");

/// How long a query waits for a pooled connection before it fails.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Advisory-lock key serialising `init_schema` across connections.
const SCHEMA_LOCK_ID: i64 = 0x7273_5f73_6368_656d;

/// A smoke-tested `PgPool`. Cheap to clone: clones share the pool.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Open the pool (`postgres_pool_{min,max}_size`) and run `SELECT 1`,
    /// retrying with backoff. Returns the last error once
    /// `connect_max_attempts` attempts have failed.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        let backoff = Backoff::from_settings(settings);
        let pool = connect_with_backoff("Postgres", &backoff, || open_pool(settings)).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run `scripts/postgres-init.sql` (Python's lifespan does the same).
    ///
    /// Runs in one transaction under a transaction-scoped advisory lock:
    /// concurrent `CREATE EXTENSION/TABLE IF NOT EXISTS` statements race on
    /// the catalog unique index (replicas starting together, parallel tests).
    pub async fn init_schema(&self) -> anyhow::Result<()> {
        let fail =
            |e: sqlx::Error| anyhow!("Postgres schema (scripts/postgres-init.sql) failed: {e}");
        let mut tx = self.pool.begin().await.map_err(fail)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(SCHEMA_LOCK_ID)
            .execute(&mut *tx)
            .await
            .map_err(fail)?;
        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&mut *tx)
            .await
            .map_err(fail)?;
        tx.commit().await.map_err(fail)?;
        Ok(())
    }

    /// Close the pool. Waits for checked-out connections to be returned.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// One connection attempt. The cause is embedded in the message (not added
/// with `.context`), because `connect_with_backoff` logs `%err`, and an
/// anyhow `Display` shows only the outermost message.
async fn open_pool(settings: &Settings) -> anyhow::Result<PgPool> {
    let options = PgConnectOptions::from_str(settings.postgres_url.expose_secret())
        .map_err(|e| anyhow!("POSTGRES_URL is not a valid postgres:// URL: {e}"))?;
    // One plain connection first: the pool's connect retries a refused port
    // internally until the acquire timeout, which would defeat the backoff.
    let mut conn = PgConnection::connect_with(&options)
        .await
        .map_err(|e| anyhow!("connecting to Postgres: {e}"))?;
    sqlx::query("SELECT 1")
        .execute(&mut conn)
        .await
        .map_err(|e| anyhow!("Postgres smoke test (SELECT 1) failed: {e}"))?;
    // Best effort: the smoke test already passed.
    let _ = conn.close().await;
    let pool = PgPoolOptions::new()
        .min_connections(settings.postgres_pool_min_size)
        .max_connections(settings.postgres_pool_max_size)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_with(options)
        .await
        .map_err(|e| anyhow!("connecting to Postgres: {e}"))?;
    Ok(pool)
}

#[async_trait]
impl HealthProbe for PostgresStore {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn check(&self) -> Result<(), ProbeError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| ProbeError::Unavailable(e.to_string()))
    }
}
