use crate::support::containers::{self, Endpoint};
use crate::support::{app_settings, settings_for};
use rust_template::health::probe::HealthProbe;
use rust_template::persistence::cache_store::redis::RedisStore;
use rust_template::settings::Settings;
use std::time::{Duration, Instant};

#[tokio::test]
async fn connects_and_answers_the_probe() {
    let store = RedisStore::connect(&app_settings().await)
        .await
        .expect("connect");
    assert_eq!(store.name(), "redis");
    store.check().await.expect("probe is up");
    let mut connection = store.connection();
    let pong: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(pong, "PONG");
}

/// A stock Redis without RedisJSON is rejected at store level
/// (`stores.rs` repeats it through `create_app`).
#[tokio::test]
async fn stock_redis_without_redisjson_is_rejected() {
    let stock = containers::stock_redis().await;
    let closed = Endpoint::closed();
    let settings = Settings {
        connect_max_attempts: 1,
        ..settings_for(&closed, &closed, &stock.endpoint)
    };
    let text = format!(
        "{:#}",
        RedisStore::connect(&settings)
            .await
            .err()
            .expect("must fail")
    );
    assert!(text.contains("RedisJSON module is missing"), "{text}");
    assert!(text.contains("redis/redis-stack-server"), "{text}");
}

#[tokio::test]
async fn closed_port_fails_after_the_retry_budget() {
    let closed = Endpoint::closed();
    let settings = Settings {
        connect_max_attempts: 2,
        connect_base_delay: 0.01,
        connect_max_delay: 0.02,
        ..settings_for(&closed, &closed, &closed)
    };
    let started = Instant::now();
    let err = RedisStore::connect(&settings)
        .await
        .err()
        .expect("must fail");
    let elapsed = started.elapsed();
    assert!(
        format!("{err:#}").contains("connecting to Redis"),
        "{err:#}"
    );
    // Windows reports a refused loopback connect only after ~2 s; the 2 s
    // connection timeout caps each attempt, so two attempts stay near 4 s.
    // Internal manager retries would add more per attempt.
    assert!(elapsed < Duration::from_secs(6), "took {elapsed:?}");
}
