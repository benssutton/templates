//! Neither store password appears in any live endpoint. The test-container
//! passwords are long and distinctive (support/containers.rs), so this test
//! always runs.

use crate::support::containers::{CH_PASSWORD, PG_PASSWORD};
use crate::support::{TestApp, app_settings};

#[tokio::test]
async fn store_passwords_never_appear_in_debug_output_or_live_responses() {
    let settings = app_settings().await;
    let debug = format!("{settings:?}");
    assert!(
        !debug.contains(PG_PASSWORD) && !debug.contains(CH_PASSWORD),
        "{debug}"
    );

    let app = TestApp::spawn(settings).await;
    for path in [
        "/",
        "/health/live",
        "/health/ready",
        "/health/status",
        "/config/",
        "/data",
        "/metrics",
        "/api-doc/openapi.json",
    ] {
        let response = app.get(path).await;
        let status = response.status();
        let headers = format!("{:?}", response.headers());
        let text = response.text().await.unwrap();
        // Name the leaked setting, never print its value (CodeQL rust/cleartext-logging).
        for (name, secret) in [("PG_PASSWORD", PG_PASSWORD), ("CH_PASSWORD", CH_PASSWORD)] {
            assert!(
                !text.contains(secret) && !headers.contains(secret),
                "{path} ({status}) leaks the store password {name}"
            );
        }
    }
}
