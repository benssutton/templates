//! Broker-backed resilience: a real partition (docker pause, twice: reconnect
//! and watchdog) and a denied queue bind, each driving the ingest state
//! machine and the app's Shutdown.
//!
//! The two pause tests hold the write lease (`solace_exclusive`)
//! for their whole run, so no other broker test runs while the broker is
//! frozen; its drop guard unpauses the broker even when a test fails.

use crate::support::TestApp;
use crate::support::batches::make_batch;
use crate::support::containers;
use crate::support::logs;
use crate::support::semp::Semp;
use crate::support::solace::{
    SolaceNames, poll_cache, publisher, solace_settings, values, wait_for_bound,
};
use rust_template::settings::Settings;
use serde_json::Value;
use std::time::{Duration, Instant};

/// Reconnect + re-bind after unpause measured ~0 s;
/// generous headroom.
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(90);
/// A frozen broker surfaces after the 5 s idle timeout (~4.9 s measured).
const DETECT_TIMEOUT: Duration = Duration::from_secs(45);

async fn poll_state(app: &TestApp, state: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let (_, body) = app.get_json("/health/status").await;
        if body["ingest"]["connection_state"] == state {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "ingest never reached {state}: {}",
            body["ingest"]
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The `ingest` entry of a /health/ready body.
fn ingest_check(body: &Value) -> &Value {
    body["checks"]
        .as_array()
        .and_then(|checks| checks.iter().find(|c| c["name"] == "ingest"))
        .unwrap_or_else(|| panic!("no ingest check in {body}"))
}

/// GET /health/ready until `status` comes back and `until` holds for the
/// ingest check.
async fn poll_ready_until(
    app: &TestApp,
    status: u16,
    until: impl Fn(&Value) -> bool,
    timeout: Duration,
) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        let (got, body) = app.get_json("/health/ready").await;
        if got == status && until(ingest_check(&body)) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "/health/ready never reached {status} within {timeout:?}; last {got}: {body}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Spec §6: broker paused -> Reconnecting and /health/ready 503; unpaused ->
/// Connected, re-bound (`bound_epoch` rises) and ready again; a message
/// published afterwards is ingested.
#[tokio::test]
async fn a_paused_broker_reconnects_and_ingest_resumes() {
    let broker = containers::solace_exclusive().await;
    let semp = Semp::new(&broker.endpoints.semp_url());
    let names = SolaceNames::new();
    // No automatic shutdown during the partition.
    let settings = Settings {
        ingest_max_disconnect_seconds: None,
        ingest_max_consecutive_failures: 0,
        ..solace_settings(&broker.endpoints, &names).await
    };
    let app = TestApp::spawn(settings).await;
    wait_for_bound(&semp, &names.queue).await;
    poll_ready_until(&app, 200, |_| true, Duration::from_secs(30)).await;
    let epoch_before = app.state.ingest.bound_epoch();
    assert!(epoch_before >= 1, "bound before the partition");

    broker.pause().await;
    // Wait for the partition to be the cause of the 503, not merely the first one.
    let body = poll_ready_until(
        &app,
        503,
        |ingest| ingest["connection_state"] == "reconnecting",
        DETECT_TIMEOUT,
    )
    .await;
    assert_eq!(ingest_check(&body)["status"], "down", "{body}");
    assert_eq!(body["status"], "not_ready", "{body}");
    broker.unpause().await;

    poll_state(&app, "connected", RECONNECT_TIMEOUT).await;
    let deadline = Instant::now() + RECONNECT_TIMEOUT;
    while app.state.ingest.bound_epoch() <= epoch_before {
        assert!(
            Instant::now() < deadline,
            "no re-bind after the partition healed (epoch {})",
            app.state.ingest.bound_epoch()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    wait_for_bound(&semp, &names.queue).await;
    poll_ready_until(
        &app,
        200,
        |ingest| ingest["connection_state"] == "connected",
        RECONNECT_TIMEOUT,
    )
    .await;

    // A fresh publisher: an old connection would not survive the pause.
    let mut p = publisher(&broker.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[(7, "after", "v1", "upsert")]))
        .await
        .unwrap();
    let body = poll_cache(&app, |b| values(b).contains_key(&7), RECONNECT_TIMEOUT).await;
    assert_eq!(values(&body).get(&7).map(String::as_str), Some("v1"));
    p.close().await;
    app.stop().await.expect("clean shutdown");
    semp.delete_queue(&names.queue).await;
}

/// Spec §6: broker unreachable -> the watchdog cancels Shutdown within
/// ingest_max_disconnect_seconds. SASL rule: there is no per-test username to
/// disable, and disabling `default` would break concurrent tests, so this
/// takes the write lease.
#[tokio::test]
async fn a_paused_broker_trips_the_watchdog() {
    let (logs, _guard) = logs::capture();
    let broker = containers::solace_exclusive().await;
    let semp = Semp::new(&broker.endpoints.semp_url());
    let names = SolaceNames::new();
    let settings = Settings {
        ingest_max_disconnect_seconds: Some(2.0),
        ..solace_settings(&broker.endpoints, &names).await
    };
    let app = TestApp::spawn(settings).await;
    wait_for_bound(&semp, &names.queue).await;
    poll_state(&app, "connected", Duration::from_secs(10)).await;
    assert!(
        !app.state.shutdown.is_triggered(),
        "precondition: connected, no shutdown yet"
    );

    broker.pause().await;
    let paused_at = Instant::now();
    // Detection ~5 s, + the 2 s threshold, + the 1 s watchdog poll, + headroom.
    let tripped =
        tokio::time::timeout(Duration::from_secs(25), app.state.shutdown.triggered()).await;
    let elapsed = paused_at.elapsed();
    broker.unpause().await;
    tripped.unwrap_or_else(|_| panic!("watchdog did not request shutdown within {elapsed:?}"));
    assert!(
        logs.contains("ingest transport not connected"),
        "{}",
        logs.text()
    );
    assert!(
        !logs.contains("consecutive failures; requesting shutdown"),
        "{}",
        logs.text()
    );
    drop(app);
    semp.delete_queue(&names.queue).await;
}

/// Spec §6: queue bind denied -> consecutive failures cancel Shutdown. The
/// connection is up, so the watchdog stays quiet. Real backoff: 1 + 2 + 4 + 8 s
/// (+25 %).
#[tokio::test]
async fn a_denied_bind_requests_shutdown_after_five_consecutive_failures() {
    let (logs, _guard) = logs::capture();
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    // An owner-less no-access queue, bound anonymously as `default`.
    semp.create_queue(&names.queue, "no-access", None)
        .await
        .unwrap();
    semp.add_subscription(&names.queue, &names.topic)
        .await
        .unwrap();
    let settings = Settings {
        ingest_max_disconnect_seconds: Some(60.0),
        ..solace_settings(&lease.endpoints, &names).await
    };
    let app = TestApp::spawn(settings).await;
    // Backoff before failure 5: ~15-19 s.
    tokio::time::timeout(Duration::from_secs(45), app.state.shutdown.triggered())
        .await
        .expect("consecutive failures requested shutdown");
    let max = app.state.settings.ingest_max_consecutive_failures;
    let last_failure = format!("failure {max}/{max}");
    assert!(logs.contains(&last_failure), "{}", logs.text());
    assert!(logs.contains("bind to queue"), "{}", logs.text());
    assert_eq!(
        app.state.ingest.bound_epoch(),
        0,
        "a refused bind is not a bind"
    );
    assert!(
        logs.contains("consecutive failures; requesting shutdown"),
        "{}",
        logs.text()
    );
    assert!(
        !logs.contains("ingest transport not connected"),
        "{}",
        logs.text()
    );
    drop(app);
    semp.delete_queue(&names.queue).await;
}
