//! Stores. Each store connects through `core::retry::connect_with_backoff`,
//! is smoke-tested inside that retry, and implements `HealthProbe`
//! (registered only after it connected).

pub mod analytics_store;
pub mod cache_store;
pub mod stream_store;
pub mod transaction_store;

use crate::health::probe::HealthProbe;
use crate::settings::Settings;
use analytics_store::clickhouse::ClickHouseStore;
use cache_store::redis::RedisStore;
use std::sync::{Arc, Mutex};
use transaction_store::postgres::PostgresStore;

/// The three stores of one app. Cheap to clone (every store is a handle).
#[derive(Clone)]
pub struct Stores {
    pub postgres: PostgresStore,
    pub clickhouse: ClickHouseStore,
    pub redis: RedisStore,
}

impl Stores {
    /// Connect all three concurrently. The first store to exhaust its retry
    /// budget aborts startup; `try_join!` then drops (cancels) the other
    /// attempts. Afterwards, apply the Postgres and ClickHouse migrations. A
    /// store that had already connected when startup aborts is closed, not
    /// leaked.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        // `try_join!` discards the stores that finished before a sibling
        // failed, so the Postgres pool (the only one needing an explicit
        // close) is also parked here as soon as it is connected.
        let connected_pg: Mutex<Option<PostgresStore>> = Mutex::new(None);
        let pg = async {
            let store = PostgresStore::connect(settings).await?;
            if let Ok(mut slot) = connected_pg.lock() {
                *slot = Some(store.clone());
            }
            Ok(store)
        };
        let joined = tokio::try_join!(
            pg,
            ClickHouseStore::connect(settings),
            RedisStore::connect(settings),
        );
        let (postgres, clickhouse, redis) = match joined {
            Ok(stores) => stores,
            Err(err) => {
                let parked = connected_pg.lock().ok().and_then(|mut slot| slot.take());
                if let Some(postgres) = parked {
                    postgres.close().await;
                }
                return Err(err);
            }
        };
        if let Err(err) = tokio::try_join!(postgres.migrate(), clickhouse.migrate()) {
            postgres.close().await;
            return Err(err);
        }
        tracing::info!("stores connected: postgres, clickhouse, redis");
        Ok(Self {
            postgres,
            clickhouse,
            redis,
        })
    }

    /// One readiness probe per store.
    pub fn probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            Arc::new(self.postgres.clone()) as Arc<dyn HealthProbe>,
            Arc::new(self.clickhouse.clone()) as Arc<dyn HealthProbe>,
            Arc::new(self.redis.clone()) as Arc<dyn HealthProbe>,
        ]
    }

    /// The last step of the shutdown order: close the Postgres
    /// pool. The ClickHouse HTTP client and the Redis connection close
    /// when they are dropped.
    pub async fn close(&self) {
        self.postgres.close().await;
        tracing::info!("stores closed");
    }
}
