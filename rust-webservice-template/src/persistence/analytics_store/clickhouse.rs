//! Analytics store: ClickHouse over HTTP (Python
//! `persistence/analytics_store/clickhouse/clickhouse_client.py`).
//! `::clickhouse` is the crate; this module shares its name.

use crate::core::retry::{connect_with_backoff, Backoff};
use crate::health::probe::{HealthProbe, ProbeError};
use crate::settings::Settings;
use anyhow::anyhow;
use async_trait::async_trait;
use secrecy::ExposeSecret;
use std::time::Duration;

/// Bound on one startup smoke test. The crate's HTTP client has no connect
/// timeout, so an unroutable host would otherwise hang startup.
const SMOKE_TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A smoke-tested ClickHouse client. Cheap to clone (shared HTTP pool).
#[derive(Debug, Clone)]
pub struct ClickHouseStore {
    client: ::clickhouse::Client,
}

impl ClickHouseStore {
    /// Build the client and run `SELECT 1`, retrying with backoff.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        let backoff = Backoff::from_settings(settings);
        let client = connect_with_backoff("ClickHouse", &backoff, || async {
            let client = build_client(settings);
            smoke_test(&client).await?;
            Ok::<_, anyhow::Error>(client)
        })
        .await?;
        Ok(Self { client })
    }

    pub fn client(&self) -> &::clickhouse::Client {
        &self.client
    }
}

/// The client for `clickhouse_{host,port,user,password,database}`. No I/O.
pub fn build_client(settings: &Settings) -> ::clickhouse::Client {
    ::clickhouse::Client::default()
        .with_url(format!(
            "http://{}:{}",
            settings.clickhouse_host, settings.clickhouse_port
        ))
        .with_user(settings.clickhouse_user.clone())
        .with_password(settings.clickhouse_password.expose_secret())
        .with_database(settings.clickhouse_database.clone())
}

/// The cause is embedded in the message: `connect_with_backoff` logs `%err`.
async fn smoke_test(client: &::clickhouse::Client) -> anyhow::Result<()> {
    match tokio::time::timeout(
        SMOKE_TEST_TIMEOUT,
        client.query("SELECT 1").fetch_one::<u8>(),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(anyhow!("ClickHouse smoke test (SELECT 1) failed: {e}")),
        Err(_) => Err(anyhow!(
            "ClickHouse smoke test (SELECT 1) timed out after {SMOKE_TEST_TIMEOUT:?}"
        )),
    }
}

#[async_trait]
impl HealthProbe for ClickHouseStore {
    fn name(&self) -> &'static str {
        "clickhouse"
    }

    async fn check(&self) -> Result<(), ProbeError> {
        self.client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map(|_| ())
            .map_err(|e| ProbeError::Unavailable(e.to_string()))
    }
}
