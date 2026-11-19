use crate::support::containers::{self, Endpoint};
use crate::support::{app_settings, settings_for};
use rust_template::persistence::Stores;
use rust_template::settings::Settings;

#[tokio::test]
async fn connects_all_three_and_hands_out_probes_in_python_order() {
    let stores = Stores::connect(&app_settings().await)
        .await
        .expect("connect");
    let probes = stores.probes();
    let names: Vec<&str> = probes.iter().map(|p| p.name()).collect();
    assert_eq!(names, ["postgres", "clickhouse", "redis"]);
    for probe in &probes {
        probe
            .check()
            .await
            .unwrap_or_else(|e| panic!("{} is down: {e}", probe.name()));
    }
    // connect also applied scripts/postgres-init.sql
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'configuration'",
    )
    .fetch_one(stores.postgres.pool())
    .await
    .unwrap();
    assert_eq!(tables, 1);

    stores.close().await;
    assert!(stores.postgres.pool().is_closed());
}

/// Spec §5: the stores connect concurrently (`try_join!`), and the first
/// one to exhaust its retry budget aborts startup.
#[tokio::test]
async fn an_unreachable_store_aborts_connect() {
    let (pg, redis) = tokio::join!(containers::postgres(), containers::redis());
    let settings = Settings {
        connect_max_attempts: 2,
        connect_base_delay: 0.01,
        connect_max_delay: 0.02,
        ..settings_for(&pg, &Endpoint::closed(), &redis)
    };
    let err = Stores::connect(&settings).await.err().expect("must fail");
    assert!(format!("{err:#}").contains("ClickHouse"), "{err:#}");
}

use crate::support::TestApp;

/// `create_app` connects the stores, applies the migrations and registers
/// one probe per store before it returns.
#[tokio::test]
async fn create_app_connects_the_stores_and_registers_their_probes() {
    let app = TestApp::spawn(app_settings().await).await;
    assert_eq!(app.state.health.dependencies().await.len(), 3);
    let version: Option<String> =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            .fetch_optional(app.state.stores.postgres.pool())
            .await
            .unwrap();
    assert!(
        version.is_some(),
        "create_app must apply migrations/postgres"
    );
}

/// Spec §2 shutdown order: ... drain in-flight HTTP -> close the pools.
#[tokio::test]
async fn stopping_the_app_closes_the_postgres_pool() {
    let app = TestApp::spawn(app_settings().await).await;
    let pool = app.state.stores.postgres.pool().clone();
    assert!(!pool.is_closed());
    app.stop().await.expect("clean shutdown");
    assert!(pool.is_closed());
}

use rust_template::app::create_app;
use rust_template::settings::IngestTransport;

/// Python main.py lifespan: the consumer for INGEST_TRANSPORT is entered and
/// StreamIngestService starts; App::close stops it before the pools close.
#[tokio::test]
async fn create_app_starts_ingest_with_the_noop_consumer_and_close_stops_it() {
    let app = TestApp::spawn(app_settings().await).await;
    let ingest = app.state.ingest.clone();
    assert!(ingest.is_running());
    let (_, body) = app.get_json("/health/ready").await;
    let check = body["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "ingest")
        .unwrap()
        .clone();
    assert_eq!(
        (check["transport"].as_str(), check["thread_alive"].as_bool()),
        (Some("noop"), Some(true))
    );
    app.stop().await.expect("clean shutdown");
    assert!(!ingest.is_running());
}

/// Fail fast: an unreachable broker aborts startup after
/// connect_max_attempts, and the already-connected stores are closed.
#[tokio::test]
async fn an_unreachable_broker_makes_create_app_fail() {
    let closed = Endpoint::closed();
    let settings = Settings {
        ingest_transport: IngestTransport::Solace,
        solace_host: closed.host.clone(),
        solace_port: closed.port,
        connect_max_attempts: 2,
        connect_base_delay: 0.01,
        connect_max_delay: 0.05,
        ..app_settings().await
    };
    let err = create_app(settings).await.err().expect("must fail");
    assert!(
        format!("{err:#}").to_lowercase().contains("solace"),
        "{err:#}"
    );
}
