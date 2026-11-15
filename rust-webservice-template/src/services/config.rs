//! Python `services/config.py`: key/value configuration in Postgres.

use crate::core::boundary_timing::timed;
use crate::error::AppError;
use crate::schemas::config::ConfigEntry;
use anyhow::Context;
use sqlx::postgres::PgPool;

const UPSERT: &str = "INSERT INTO configuration (key, value) \
                      VALUES ($1, $2) \
                      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value \
                      RETURNING key, value";

pub struct ConfigService {
    pool: PgPool,
}

impl ConfigService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Every entry, ordered by key.
    pub async fn get_all(&self) -> Result<Vec<ConfigEntry>, AppError> {
        let rows: Vec<(String, String)> = timed(
            "postgres.config.get_all",
            sqlx::query_as("SELECT key, value FROM configuration ORDER BY key")
                .fetch_all(&self.pool),
        )
        .await
        .context("listing configuration")?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| ConfigEntry { key, value })
            .collect())
    }

    /// Insert or overwrite one entry; returns what is now stored.
    pub async fn set(&self, key: &str, value: &str) -> Result<ConfigEntry, AppError> {
        let (key, value): (String, String) = timed(
            "postgres.config.set",
            sqlx::query_as(UPSERT)
                .bind(key)
                .bind(value)
                .fetch_one(&self.pool),
        )
        .await
        .context("upserting configuration")?;
        Ok(ConfigEntry { key, value })
    }
}
