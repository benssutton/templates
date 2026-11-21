use crate::support::containers::{self, Endpoint};
use crate::support::{settings_for, test_settings};
use rust_template::app::create_app;
use rust_template::settings::Settings;
use std::time::{Duration, Instant};

/// Startup failure, app half: invalid config makes
/// create_app return Err with a readable message; main turns it into a
/// non-zero exit.
#[tokio::test]
async fn create_app_rejects_invalid_settings() {
    let err = create_app(Settings {
        cors_allow_credentials: true, // with the default ["*"] origins
        ..test_settings()
    })
    .await
    .err()
    .expect("must fail");
    assert!(format!("{err:#}").contains("incompatible"), "{err:#}");

    let err = create_app(Settings {
        connect_max_attempts: 0,
        ..test_settings()
    })
    .await
    .err()
    .expect("must fail");
    assert!(
        format!("{err:#}").contains("connect_max_attempts"),
        "{err:#}"
    );
}

/// Startup failure, store half: real Postgres and
/// Redis connect at once; ClickHouse points at a closed port. With a tiny
/// retry budget, create_app must retry, then fail; never hang.
#[tokio::test]
async fn closed_store_port_makes_create_app_fail() {
    let (pg, redis) = tokio::join!(containers::postgres(), containers::redis());
    let settings = Settings {
        connect_max_attempts: 2,
        connect_base_delay: 0.001,
        connect_max_delay: 0.005,
        ..settings_for(&pg, &Endpoint::closed(), &redis)
    };
    let started = Instant::now();
    let err = create_app(settings)
        .await
        .err()
        .expect("create_app must fail");
    assert!(format!("{err:#}").contains("ClickHouse"), "{err:#}");
    // Bounded retry: startup must fail within 15 s.
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "took {:?}",
        started.elapsed()
    );
}

/// A real stock Redis (no RedisJSON) must fail startup with an actionable message.
#[tokio::test]
async fn stock_redis_without_redisjson_aborts_startup() {
    let (pg, ch) = tokio::join!(containers::postgres(), containers::clickhouse());
    let stock = containers::stock_redis().await;
    let settings = Settings {
        connect_max_attempts: 1,
        ..settings_for(&pg, &ch, &stock.endpoint)
    };
    let err = create_app(settings)
        .await
        .err()
        .expect("create_app must fail");
    assert!(format!("{err:#}").contains("RedisJSON"), "{err:#}");
}
