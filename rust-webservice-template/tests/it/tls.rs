use crate::support::{app_settings, client};
use rust_template::app::create_app;
use rust_template::server::{Tls, serve};
use rust_template::settings::Settings;
use std::path::PathBuf;

/// A fresh self-signed cert/key pair (rcgen, ring) in a unique temp dir.
fn self_signed_pem() -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "rust-template-tls-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    (cert, key)
}

#[test]
fn tls_mode_follows_the_ssl_settings() {
    assert_eq!(
        Tls::from_settings(&Settings::default()),
        Tls::Pem {
            cert: "./certs/cert.pem".into(),
            key: "./certs/key.pem".into()
        }
    );
    let plain = Settings {
        ssl_certfile: String::new(),
        ..Settings::default()
    };
    assert_eq!(Tls::from_settings(&plain), Tls::Disabled);
}

#[tokio::test]
async fn serves_https_with_the_configured_certificate() {
    let (cert, key) = self_signed_pem();
    let app = create_app(app_settings().await).await.unwrap();
    let shutdown = app.state.shutdown.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(serve(listener, app, Tls::Pem { cert, key }));

    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client()
            .get(format!("https://127.0.0.1:{port}/health/live"))
            .send(),
    )
    .await
    .expect("HTTPS request timed out")
    .expect("HTTPS request");
    assert_eq!(response.status(), 200);

    shutdown.trigger("test finished");
    tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .expect("HTTPS shutdown timed out")
        .unwrap()
        .expect("clean HTTPS shutdown");
}

#[tokio::test]
async fn missing_certificate_is_a_startup_error() {
    let app = create_app(app_settings().await).await.unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let err = serve(
        listener,
        app,
        Tls::Pem {
            cert: "./no/such/cert.pem".into(),
            key: "./no/such/key.pem".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("loading TLS certificate"),
        "{err:#}"
    );
}
