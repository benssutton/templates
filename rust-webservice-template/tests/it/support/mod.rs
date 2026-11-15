//! Shared test helpers. R2 adds the shared `OnceCell` containers here.

pub mod logs;

use rust_template::app::{create_app, App};
use rust_template::server::{self, Tls};
use rust_template::settings::{IngestTransport, Settings};
use rust_template::state::AppState;
use tokio::task::JoinHandle;

/// Python's test fixtures: status "testing", noop transport.
pub fn test_settings() -> Settings {
    Settings {
        status: "testing".into(),
        ingest_transport: IngestTransport::Noop,
        ..Settings::default()
    }
}

/// reqwest client for tests. `rustls-no-provider` needs a process-wide
/// rustls provider before any client is built (installing twice is a no-op).
pub fn client() -> reqwest::Client {
    server::install_crypto_provider();
    reqwest::Client::builder()
        .tls_danger_accept_invalid_certs(true) // self-signed dev cert (TLS test)
        .build()
        .expect("reqwest client")
}

/// An isolated app (its own state, metrics registry and shutdown handle)
/// served over plain HTTP on an ephemeral port: the Rust counterpart of
/// Python's `lifespan_test_client(settings)`. Dropping it triggers shutdown.
pub struct TestApp {
    pub base_url: String,
    pub state: AppState,
    client: reqwest::Client,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl TestApp {
    pub async fn spawn(settings: Settings) -> Self {
        let app = create_app(settings).await.expect("create_app");
        Self::serve(app)
    }

    /// Serve an already-built app (e.g. one whose `AppState` has extra probes).
    pub fn serve(app: App) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let base_url = format!("http://{}", listener.local_addr().expect("local addr"));
        let state = app.state.clone();
        let task = tokio::spawn(server::serve(listener, app, Tls::Disabled));
        Self {
            base_url,
            state,
            client: client(),
            task: Some(task),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn get(&self, path: &str) -> reqwest::Response {
        self.client.get(self.url(path)).send().await.expect("GET")
    }

    pub async fn get_json(&self, path: &str) -> (u16, serde_json::Value) {
        let response = self.get(path).await;
        let status = response.status().as_u16();
        (status, response.json().await.expect("JSON body"))
    }

    /// Trigger shutdown and wait for the server to drain and return.
    pub async fn stop(mut self) -> anyhow::Result<()> {
        self.state.shutdown.trigger("test finished");
        match self.task.take() {
            Some(task) => task.await.expect("server task panicked"),
            None => Ok(()),
        }
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        self.state.shutdown.trigger("test app dropped");
    }
}
