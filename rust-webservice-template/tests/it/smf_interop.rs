//! Spec §6 SMF interop: an SMF publisher script -> the Rust AMQP consumer.
//! Needs $SMF_PYTHON: a Python with solace-pubsubplus and pyarrow;
//! run_CICD.py sets it to its own interpreter. Without it the test is skipped
//! with a message on stderr, except under CI=true, where it fails.

use crate::support::TestApp;
use crate::support::containers;
use crate::support::semp::Semp;
use crate::support::solace::{
    SolaceNames, poll_cache, solace_settings, values, wait_for_bound, wait_for_depth,
};
use std::time::Duration;

/// Connect plus one acknowledged publish; the SDK's own ack timeout is 15 s.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn a_message_published_over_smf_by_python_is_ingested() {
    let Ok(python) = std::env::var("SMF_PYTHON") else {
        assert!(
            std::env::var_os("CI").is_none_or(|v| v != "true"),
            "CI=true but SMF_PYTHON is unset: run the Rust tests through run_CICD.py, \
             which sets SMF_PYTHON to a Python with solace-pubsubplus and pyarrow"
        );
        eprintln!(
            "SKIPPED smf_interop: set SMF_PYTHON to a Python with solace-pubsubplus and \
             pyarrow (run_CICD.py rust test sets it)"
        );
        return;
    };
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let app = TestApp::spawn(solace_settings(&lease.endpoints, &names).await).await;
    wait_for_bound(&semp, &names.queue).await;

    // -I: no script directory, cwd, user site or PYTHON* variables on sys.path.
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/it/smf_publish.py");
    let mut command = tokio::process::Command::new(&python);
    command
        .arg("-I")
        .arg(script)
        .env("SMF_HOST", &lease.endpoints.host)
        .env("SMF_PORT", lease.endpoints.smf.to_string())
        .env("SMF_TOPIC", &names.topic)
        .kill_on_drop(true);
    let output = tokio::time::timeout(PUBLISH_TIMEOUT, command.output())
        .await
        .unwrap_or_else(|_| panic!("{python} smf_publish.py did not finish in {PUBLISH_TIMEOUT:?}"))
        .unwrap_or_else(|e| panic!("cannot run SMF_PYTHON={python}: {e}"));
    assert!(
        output.status.success(),
        "{python} smf_publish.py failed (SMF_PYTHON needs solace-pubsubplus and pyarrow):\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let body = poll_cache(
        &app,
        |b| values(b).contains_key(&42),
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(values(&body).get(&42).map(String::as_str), Some("v1"));
    assert_eq!(body["rows"][0]["name"], "smf");
    wait_for_depth(&semp, &names.queue, 0).await; // accepted after the write
}
