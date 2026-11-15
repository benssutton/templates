use crate::support::containers::{self, Endpoint, CH_PASSWORD};
use crate::support::{app_settings, settings_for};
use rust_template::health::probe::HealthProbe;
use rust_template::persistence::analytics_store::clickhouse::ClickHouseStore;
use rust_template::settings::Settings;
use secrecy::SecretString;
use std::time::{Duration, Instant};

#[tokio::test]
async fn connects_and_answers_the_probe() {
    let store = ClickHouseStore::connect(&app_settings().await)
        .await
        .expect("connect");
    assert_eq!(store.name(), "clickhouse");
    store.check().await.expect("probe is up");
    let total: u64 = store
        .client()
        .query("SELECT count() FROM items")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(total, 3, "the shared ClickHouse holds the 3 seed rows");
}

/// Building the client does no I/O, so a bad credential must be caught by
/// the smoke test inside the retried connect, not by the first request.
#[tokio::test]
async fn wrong_password_fails_the_smoke_test() {
    let ch = containers::clickhouse().await;
    let closed = Endpoint::closed();
    let settings = Settings {
        clickhouse_password: SecretString::from("not-the-password"),
        connect_max_attempts: 1,
        ..settings_for(&closed, &ch, &closed)
    };
    let text = format!(
        "{:#}",
        ClickHouseStore::connect(&settings).await.unwrap_err()
    );
    assert!(
        text.contains("ClickHouse smoke test (SELECT 1) failed"),
        "{text}"
    );
    assert!(
        !text.contains("not-the-password") && !text.contains(CH_PASSWORD),
        "{text}"
    );
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
    let err = ClickHouseStore::connect(&settings).await.unwrap_err();
    let elapsed = started.elapsed();
    eprintln!("clickhouse closed-port elapsed: {elapsed:?}");
    assert!(
        format!("{err:#}").contains("ClickHouse smoke test"),
        "{err:#}"
    );
    // Windows reports a refused loopback connect only after ~2.05 s (OS SYN
    // retry), so two attempts take ~4.6 s. A client retrying internally
    // would add its own timeout (5 s) per attempt and blow this bound.
    assert!(elapsed < Duration::from_secs(6), "took {elapsed:?}");
}
