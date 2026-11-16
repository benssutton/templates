//! [VARIANT B] SEMP provisioning against the real broker.

use crate::support::containers::{self, Endpoint};
use crate::support::logs;
use crate::support::semp::Semp;
use crate::support::solace::{consumer_settings, SolaceNames};
use rust_template::ingestion::solace::provision;
use rust_template::ingestion::solace::semp::SempProvisioner;
use rust_template::settings::Settings;
use secrecy::SecretString;

#[tokio::test]
async fn startup_provisioning_creates_the_queue_and_subscription_idempotently() {
    let lease = containers::solace().await;
    let names = SolaceNames::new();
    let settings = consumer_settings(&lease.endpoints, &names);
    provision::prepare(&settings)
        .await
        .expect("first provisioning");
    provision::prepare(&settings)
        .await
        .expect("second provisioning is a no-op");

    let semp = Semp::new(&lease.endpoints.semp_url());
    assert!(semp.queue_exists(&names.queue).await.unwrap());
    assert_eq!(
        semp.queue_subscriptions(&names.queue).await.unwrap(),
        [names.topic.as_str()]
    );
    let again = SempProvisioner::from_settings(&settings)
        .unwrap()
        .provision(&names.queue, &names.topic)
        .await
        .unwrap();
    assert_eq!(again, (false, false), "ALREADY_EXISTS on both calls");
    semp.delete_queue(&names.queue).await;
}

/// Fail fast (spec §2): SEMP unreachable -> startup fails after the budget.
#[tokio::test]
async fn an_unreachable_semp_fails_after_the_retry_budget() {
    let closed = Endpoint::closed();
    let settings = Settings {
        solace_semp_url: format!("http://{}:{}", closed.host, closed.port),
        connect_max_attempts: 2,
        connect_base_delay: 0.01,
        connect_max_delay: 0.05,
        ..Settings::default()
    };
    let err = provision::prepare(&settings).await.unwrap_err();
    assert!(format!("{err:#}").contains("SEMP"), "{err:#}");
    assert!(
        !format!("{err:#}").contains("admin:admin"),
        "credentials leaked: {err:#}"
    );
}

/// Spec §2 "Secrets": a refused SEMP login reports the HTTP status, never
/// the password (plain or as the Basic `Authorization` header value), in
/// the error, its Debug form or the retry log lines.
#[tokio::test]
async fn a_refused_semp_login_reports_the_status_but_never_the_password() {
    const SENTINEL: &str = "semp-sentinel-42";
    /// base64("admin:semp-sentinel-42"): the Basic auth header value.
    const SENTINEL_BASIC: &str = "YWRtaW46c2VtcC1zZW50aW5lbC00Mg==";
    let lease = containers::solace().await;
    let names = SolaceNames::new();
    let settings = Settings {
        solace_semp_password: SecretString::from(SENTINEL),
        connect_max_attempts: 2,
        connect_base_delay: 0.01,
        connect_max_delay: 0.05,
        ..consumer_settings(&lease.endpoints, &names)
    };
    let (logs, _guard) = logs::capture();
    let err = provision::prepare(&settings).await.unwrap_err();
    let (display, debug, logged) = (format!("{err:#}"), format!("{err:?}"), logs.text());
    assert!(display.contains("HTTP 401"), "{display}");
    assert!(
        logged.contains("connection attempt failed"),
        "the retry is logged: {logged}"
    );
    for text in [&display, &debug, &logged] {
        assert!(!text.contains(SENTINEL), "password leaked: {text}");
        assert!(!text.contains(SENTINEL_BASIC), "auth header leaked: {text}");
    }
    let semp = Semp::new(&lease.endpoints.semp_url());
    assert!(
        !semp.queue_exists(&names.queue).await.unwrap(),
        "nothing was provisioned"
    );
}
