//! The authentication seam: off by default; when on, an HS256 bearer token
//! is required everywhere except the orchestrator probes, and every failure
//! is 401 `{"detail": "Not authenticated"}` with `WWW-Authenticate: Bearer`.

use crate::support::{app_settings, test_settings, TestApp};
use jsonwebtoken::{encode, EncodingKey, Header};
use rust_template::app::create_app;
use rust_template::settings::Settings;
use secrecy::SecretString;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

const SECRET: &str = "it-jwt-secret-5f2c"; // gitleaks:allow (test-only signing key)
const AUDIENCE: &str = "rust-template-it";

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn token(secret: &str, claims: Value) -> String {
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()).expect("a non-empty test secret"),
    )
    .unwrap()
}

fn valid_claims() -> Value {
    json!({"sub": "it", "exp": now() + 300, "aud": AUDIENCE})
}

async fn secured() -> TestApp {
    TestApp::spawn(Settings {
        auth_enabled: true,
        auth_jwt_secret: SecretString::from(SECRET),
        auth_jwt_audience: Some(AUDIENCE.into()),
        ..app_settings().await
    })
    .await
}

async fn get(app: &TestApp, path: &str, bearer: Option<&str>) -> reqwest::Response {
    let mut request = app.client().get(app.url(path));
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    request.send().await.unwrap()
}

async fn assert_unauthenticated(response: reqwest::Response) {
    assert_eq!(response.status(), 401);
    assert_eq!(response.headers()["www-authenticate"], "Bearer");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body, json!({"detail": "Not authenticated"}));
}

#[tokio::test]
async fn off_by_default_every_route_is_open() {
    let app = TestApp::spawn(app_settings().await).await;
    assert_eq!(get(&app, "/config", None).await.status(), 200);
}

#[tokio::test]
async fn a_valid_token_is_accepted() {
    let app = secured().await;
    let ok = token(SECRET, valid_claims());
    assert_eq!(get(&app, "/config", Some(&ok)).await.status(), 200);
}

#[tokio::test]
async fn a_missing_token_is_401() {
    let app = secured().await;
    assert_unauthenticated(get(&app, "/config", None).await).await;
    // `/mcp` and the API document are covered too.
    assert_unauthenticated(get(&app, "/mcp", None).await).await;
    assert_unauthenticated(get(&app, "/api-doc/openapi.json", None).await).await;
}

#[tokio::test]
async fn a_bad_signature_is_401() {
    let app = secured().await;
    let forged = token("not-the-secret", valid_claims());
    assert_unauthenticated(get(&app, "/config", Some(&forged)).await).await;
}

#[tokio::test]
async fn an_expired_token_is_401() {
    let app = secured().await;
    let expired = token(SECRET, json!({"exp": now() - 60, "aud": AUDIENCE}));
    assert_unauthenticated(get(&app, "/config", Some(&expired)).await).await;
}

#[tokio::test]
async fn a_wrong_audience_is_401() {
    let app = secured().await;
    let other = token(SECRET, json!({"exp": now() + 300, "aud": "someone-else"}));
    assert_unauthenticated(get(&app, "/config", Some(&other)).await).await;
}

#[tokio::test]
async fn the_probes_stay_open() {
    let app = secured().await;
    for path in ["/health/live", "/health/ready"] {
        assert_eq!(get(&app, path, None).await.status(), 200, "{path}");
    }
}

#[tokio::test]
async fn startup_fails_when_enabled_without_a_secret() {
    let settings = Settings {
        auth_enabled: true,
        ..test_settings()
    };
    let err = create_app(settings).await.err().expect("must not start");
    assert!(format!("{err:#}").contains("auth_jwt_secret"), "{err:#}");
}
