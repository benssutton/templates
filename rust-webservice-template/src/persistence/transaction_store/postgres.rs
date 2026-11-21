//! Transaction store: Postgres (pgvector image), with versioned sqlx
//! migrations from `migrations/postgres/`, embedded at compile time.

use crate::core::retry::{Backoff, connect_with_backoff, seconds};
use crate::health::probe::{HealthProbe, ProbeError};
use crate::settings::Settings;
use anyhow::anyhow;
use async_trait::async_trait;
use secrecy::ExposeSecret;
use sqlx::Connection;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, PgPoolOptions};
use std::str::FromStr;

/// Applied in order at startup; each version runs once (`_sqlx_migrations`).
/// The migrator holds a Postgres advisory lock while it runs, so replicas
/// starting together apply each migration exactly once.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations/postgres");

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

    /// Apply pending migrations from `migrations/postgres/`.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| anyhow!("Postgres migrations failed: {e}"))
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
    let statement_timeout_ms =
        (settings.postgres_statement_timeout_seconds * 1000.0).round() as u64;
    let options = PgConnectOptions::from_str(settings.postgres_url.expose_secret())
        .map_err(|e| anyhow!("POSTGRES_URL is not a valid postgres:// URL: {e}"))?
        // Server-side bound on every statement, so a stuck query frees its
        // connection instead of starving the pool.
        .options([("statement_timeout", statement_timeout_ms.to_string())]);
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
        .acquire_timeout(seconds(settings.postgres_acquire_timeout_seconds))
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
