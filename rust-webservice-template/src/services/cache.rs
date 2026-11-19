//! JSON values in Redis (RedisJSON), with an optional TTL.

use crate::error::AppError;
use crate::schemas::cache::CacheEntry;
use anyhow::Context;
use redis::aio::ConnectionManager;
use serde_json::Value;

/// `TTL` reply for a key that does not exist (Redis >= 2.8).
const TTL_KEY_MISSING: i64 = -2;

pub struct CacheService {
    connection: ConnectionManager,
}

impl CacheService {
    pub fn new(connection: ConnectionManager) -> Self {
        Self { connection }
    }

    /// `JSON.SET key $ value`, plus `EXPIRE key ttl` when a TTL is given, in
    /// one MULTI/EXEC so a reader never sees the value without its expiry.
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
        pipe.query_async::<()>(&mut connection).await?;
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
        // One MULTI/EXEC: read separately, the key could expire between the
        // two commands and be reported as "never expires".
        let (raw, ttl): (Option<String>, i64) = redis::pipe()
            .atomic()
            .cmd("JSON.GET")
            .arg(key)
            .cmd("TTL")
            .arg(key)
            .query_async(&mut connection)
            .await?;
        to_entry(key, raw, ttl)
    }
}

/// The entry for a `JSON.GET` reply and the `TTL` read with it. A stored
/// JSON `null` is indistinguishable from a missing path in RedisJSON's
/// reply, so it reads as absent.
fn to_entry(key: &str, raw: Option<String>, ttl: i64) -> Result<Option<CacheEntry>, AppError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if ttl == TTL_KEY_MISSING {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(&raw).context("decoding the cached JSON")?;
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(CacheEntry {
        key: key.to_owned(),
        value,
        ttl_seconds: (ttl >= 0).then_some(ttl),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_that_expired_with_the_read_is_absent_not_eternal() {
        assert!(to_entry("k", Some("1".into()), TTL_KEY_MISSING)
            .unwrap()
            .is_none());
    }

    #[test]
    fn no_expiry_is_null_and_a_ttl_is_kept() {
        let eternal = to_entry("k", Some("1".into()), -1).unwrap().unwrap();
        assert_eq!(eternal.ttl_seconds, None);
        let expiring = to_entry("k", Some("1".into()), 30).unwrap().unwrap();
        assert_eq!(expiring.ttl_seconds, Some(30));
    }

    #[test]
    fn missing_or_null_value_is_absent() {
        assert!(to_entry("k", None, -2).unwrap().is_none());
        assert!(to_entry("k", Some("null".into()), -1).unwrap().is_none());
    }
}
