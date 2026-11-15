use crate::support::test_settings;
use rust_template::app::create_app;
use rust_template::settings::Settings;

/// Spec §3 "startup failure", app half (Deviation 1): invalid config makes
/// create_app return Err with a readable message; main turns it into a
/// non-zero exit. R2 adds the closed-store-port case.
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
