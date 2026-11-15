//! Python `services/cache.py`: JSON values in Redis (RedisJSON), with an
//! optional TTL.

use crate::error::AppError;
use crate::schemas::cache::CacheEntry;
use anyhow::Context;
use redis::aio::ConnectionManager;
use serde_json::Value;

pub struct CacheService {
    connection: ConnectionManager,
}

impl CacheService {
    pub fn new(connection: ConnectionManager) -> Self {
        Self { connection }
    }

    /// `JSON.SET key $ value`, plus `EXPIRE key ttl` when a TTL is given, in
    /// one MULTI/EXEC (Python: `pipeline(transaction=True)`).
    pub async fn set(
        &self,
        key: &str,
        value: Value,
        ttl_seconds: Option<i64>,
    ) -> Result<CacheEntry, AppError> {
        let json = serde_json::to_string(&value).context("serialising the cache value")?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("JSON.SET")
            .arg(key)
            .arg("$")
            .arg(json)
            .ignore();
        if let Some(ttl) = ttl_seconds {
            pipe.cmd("EXPIRE").arg(key).arg(ttl).ignore();
        }
        let mut connection = self.connection.clone();
        pipe.query_async::<()>(&mut connection)
            .await
            .context("writing the cache entry")?;
        Ok(CacheEntry {
            key: key.to_owned(),
            value,
            ttl_seconds,
        })
    }

    /// The entry and its remaining TTL (`None` when the key never expires),
    /// or `None` when the key is absent.
    pub async fn get(&self, key: &str) -> Result<Option<CacheEntry>, AppError> {
        let mut connection = self.connection.clone();
        // No path: returns the root value as JSON text, like redis-py's `json().get(key)`.
        let raw: Option<String> = redis::cmd("JSON.GET")
            .arg(key)
            .query_async(&mut connection)
            .await
            .context("reading the cache entry")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let value: Value = serde_json::from_str(&raw).context("decoding the cached JSON")?;
        // Python returns None when JSON.GET yields None, so a stored JSON null
        // reads as absent (404). Mirrored on purpose (Deviation 6).
        if value.is_null() {
            return Ok(None);
        }
        let ttl: i64 = redis::cmd("TTL")
            .arg(key)
            .query_async(&mut connection)
            .await
            .context("reading the cache TTL")?;
        Ok(Some(CacheEntry {
            key: key.to_owned(),
            value,
            ttl_seconds: (ttl >= 0).then_some(ttl),
        }))
    }
}
