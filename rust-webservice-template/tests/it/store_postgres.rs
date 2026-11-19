use crate::support::containers::Endpoint;
use crate::support::{app_settings, settings_for, test_settings};
use rust_template::health::probe::HealthProbe;
use rust_template::persistence::transaction_store::postgres::PostgresStore;
use rust_template::settings::Settings;
use secrecy::SecretString;
use std::time::{Duration, Instant};

#[tokio::test]
async fn connects_runs_the_migrations_and_answers_the_probe() {
    let store = PostgresStore::connect(&app_settings().await)
        .await
        .expect("connect");
    store.migrate().await.expect("migrations");
    store
        .migrate()
        .await
        .expect("an applied migration is not run again");
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'configuration'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(tables, 1);
    assert_eq!(store.name(), "postgres");
    store.check().await.expect("probe is up");
}

/// Guards against a revert to a
/// non-pgvector image (CREATE EXTENSION would fail there and abort startup).
#[tokio::test]
async fn pgvector_extension_present() {
    let store = PostgresStore::connect(&app_settings().await)
        .await
        .expect("connect");
    store.migrate().await.expect("migrations");
    let version: Option<String> =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            .fetch_optional(store.pool())
            .await
            .unwrap();
    assert!(
        version.is_some(),
        "the vector extension is missing; is the image pgvector/pgvector?"
    );
}

/// Python `test_postgres_health_check_exception_logs_and_returns_generic_error`:
/// a closed pool makes the probe fail. The registry turns the detail into
/// "unavailable" for clients.
#[tokio::test]
async fn probe_fails_once_the_pool_is_closed() {
    let store = PostgresStore::connect(&app_settings().await)
        .await
        .expect("connect");
    store.close().await;
    assert!(store.pool().is_closed());
    let err = store.check().await.unwrap_err();
    assert!(err.to_string().contains("closed"), "{err}");
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
    let err = PostgresStore::connect(&settings).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("connecting to Postgres"),
        "{err:#}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn invalid_url_is_a_readable_error_without_the_password() {
    let settings = Settings {
        postgres_url: SecretString::from("postgresql://u:hunter2@localhost:notaport/db"),
        connect_max_attempts: 1,
        ..test_settings()
    };
    let text = format!("{:#}", PostgresStore::connect(&settings).await.unwrap_err());
    assert!(
        text.contains("POSTGRES_URL is not a valid postgres:// URL"),
        "{text}"
    );
    assert!(!text.contains("hunter2"), "{text}");
}
