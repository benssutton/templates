//! Cache store: Redis with the RedisJSON module. `::redis` is the crate;
//! this module shares its name.

use crate::core::retry::{connect_with_backoff, seconds, Backoff};
use crate::health::probe::{HealthProbe, ProbeError};
use crate::settings::Settings;
use ::redis::aio::{ConnectionManager, ConnectionManagerConfig};
use anyhow::anyhow;
use async_trait::async_trait;
use secrecy::ExposeSecret;

/// A smoke-tested, self-reconnecting connection. Cheap to clone: clones
/// share one multiplexed connection.
#[derive(Clone)]
pub struct RedisStore {
    connection: ConnectionManager,
}

impl RedisStore {
    /// Connect, `PING`, then check RedisJSON, retrying with backoff.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        let backoff = Backoff::from_settings(settings);
        let connection = connect_with_backoff("Redis", &backoff, || open(settings)).await?;
        Ok(Self { connection })
    }

    /// A handle to the shared connection (commands need `&mut`).
    pub fn connection(&self) -> ConnectionManager {
        self.connection.clone()
    }
}

/// One attempt. Causes are embedded in the message: `connect_with_backoff` logs `%err`.
async fn open(settings: &Settings) -> anyhow::Result<ConnectionManager> {
    let client = ::redis::Client::open(settings.redis_url.expose_secret())
        .map_err(|e| anyhow!("REDIS_URL is not a valid redis:// URL: {e}"))?;
    // Our retry loop owns startup retries. With retries > 0 the manager
    // retried a refused port inside each attempt (~4 s each), defeating the
    // backoff; 0 makes the initial connect fail promptly. After startup the
    // manager still reconnects, once per failed command.
    // The connection timeout bounds every connect (startup and the
    // manager's own reconnects); the response timeout bounds every command,
    // so a hung server fails a request instead of blocking it.
    let config = ConnectionManagerConfig::new()
        .set_number_of_retries(0)
        .set_connection_timeout(Some(seconds(settings.redis_connection_timeout_seconds)))
        .set_response_timeout(Some(seconds(settings.redis_response_timeout_seconds)));
    let mut connection = ConnectionManager::new_with_config(client, config)
        .await
        .map_err(|e| anyhow!("connecting to Redis: {e}"))?;
    ::redis::cmd("PING")
        .query_async::<String>(&mut connection)
        .await
        .map_err(|e| anyhow!("Redis smoke test (PING) failed: {e}"))?;
    assert_json_module(&mut connection).await?;
    Ok(connection)
}

/// Fail fast if the server lacks RedisJSON. `CacheService` relies on
/// `JSON.SET`/`JSON.GET`, and stock Redis would only fail at the first write.
async fn assert_json_module(connection: &mut ConnectionManager) -> anyhow::Result<()> {
    let probe_key = format!("__startup_json_probe_{}__", uuid::Uuid::new_v4().simple());
    ::redis::cmd("JSON.SET")
        .arg(&probe_key)
        .arg("$")
        .arg(r#"{"ok":true}"#)
        .query_async::<()>(connection)
        .await
        .map_err(|e| {
            anyhow!(
                "Redis is reachable but the RedisJSON module is missing. This template's \
                 cache requires redis-stack (e.g. the redis/redis-stack-server image). \
                 Original error: {e}"
            )
        })?;
    // A JSON.SET / JSON.GET round trip, not just a write.
    let read_back: Option<String> = ::redis::cmd("JSON.GET")
        .arg(&probe_key)
        .query_async(connection)
        .await
        .map_err(|e| anyhow!("Redis smoke test (JSON.GET probe key) failed: {e}"))?;
    let parsed = read_back
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    if parsed != Some(serde_json::json!({"ok": true})) {
        return Err(anyhow!(
            "Redis smoke test: JSON.GET returned {read_back:?} for the probe key"
        ));
    }
    ::redis::cmd("DEL")
        .arg(&probe_key)
        .query_async::<()>(connection)
        .await
        .map_err(|e| anyhow!("Redis smoke test (DEL probe key) failed: {e}"))?;
    Ok(())
}

#[async_trait]
impl HealthProbe for RedisStore {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn check(&self) -> Result<(), ProbeError> {
        let mut connection = self.connection.clone();
        ::redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|e| ProbeError::Unavailable(e.to_string()))
    }
}
