//! Serving over plain HTTP (tests, local-dev override) or rustls HTTPS
//! (default: 8443 + ./certs/*.pem), with graceful shutdown driven by the
//! app's `Shutdown` handle.

use crate::app::App;
use crate::core::install_crypto_provider;
use crate::settings::Settings;
use anyhow::Context;
use axum::http::Request;
use axum::ServiceExt;
use axum_server::{tls_rustls::RustlsConfig, Handle};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// How long in-flight requests get to finish once shutdown starts.
pub const DRAIN_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tls {
    Disabled,
    Pem { cert: PathBuf, key: PathBuf },
}

impl Tls {
    /// HTTPS unless either SSL file setting is empty.
    pub fn from_settings(s: &Settings) -> Self {
        if s.ssl_certfile.trim().is_empty() || s.ssl_keyfile.trim().is_empty() {
            Tls::Disabled
        } else {
            Tls::Pem {
                cert: PathBuf::from(&s.ssl_certfile),
                key: PathBuf::from(&s.ssl_keyfile),
            }
        }
    }
}

pub fn bind(s: &Settings) -> anyhow::Result<std::net::TcpListener> {
    std::net::TcpListener::bind((s.server_host.as_str(), s.server_port))
        .with_context(|| format!("binding {}:{}", s.server_host, s.server_port))
}

/// Serve until `app.state.shutdown` fires, drain for up to `DRAIN_GRACE`,
/// then `app.close()`. Returns Err if TLS material cannot be loaded or the
/// server fails.
pub async fn serve(listener: std::net::TcpListener, app: App, tls: Tls) -> anyhow::Result<()> {
    listener
        .set_nonblocking(true)
        .context("setting listener non-blocking")?;
    let addr = listener.local_addr().context("listener address")?;

    let handle: Handle<SocketAddr> = Handle::new();
    let drain = handle.clone();
    let shutdown = app.state.shutdown.clone();
    tokio::spawn(async move {
        shutdown.triggered().await;
        drain.graceful_shutdown(Some(DRAIN_GRACE));
    });

    let service = ServiceExt::<Request<_>>::into_make_service(app.service());
    // Every exit, including early setup errors, must reach `app.close()`,
    // which releases the store pools.
    let served: anyhow::Result<()> = async {
        match tls {
            Tls::Disabled => {
                tracing::info!(%addr, "listening (http)");
                axum_server::from_tcp(listener)
                    .context("creating HTTP server from listener")?
                    .handle(handle)
                    .serve(service)
                    .await
                    .context("HTTP server failed")
            }
            Tls::Pem { cert, key } => {
                install_crypto_provider();
                let config = RustlsConfig::from_pem_file(&cert, &key)
                    .await
                    .with_context(|| {
                        format!(
                            "loading TLS certificate {} and key {}",
                            cert.display(),
                            key.display()
                        )
                    })?;
                tracing::info!(%addr, "listening (https)");
                axum_server::from_tcp_rustls(listener, config)
                    .context("creating HTTPS server from listener")?
                    .handle(handle)
                    .serve(service)
                    .await
                    .context("HTTP server failed")
            }
        }
    }
    .await;
    app.close().await;
    served
}

/// `rust-template healthcheck`: GET `/health/live` on this container's own
/// listener, for a container healthcheck in an image without curl. Exits
/// non-zero unless the answer is 2xx. Certificate checks are skipped: the
/// target is this process's own loopback listener, nothing read back is
/// trusted beyond the status code, and a real deployment certificate would not
/// name 127.0.0.1, so verifying it would make the check fail in production.
pub async fn healthcheck(settings: &Settings) -> anyhow::Result<()> {
    install_crypto_provider();
    let scheme = match Tls::from_settings(settings) {
        Tls::Disabled => "http",
        Tls::Pem { .. } => "https",
    };
    let url = format!("{scheme}://127.0.0.1:{}/health/live", settings.server_port);
    // nosemgrep: rust.lang.security.reqwest-accept-invalid.reqwest-accept-invalid -- loopback self-probe, see the doc comment
    let response = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(3))
        .build()
        .context("building the healthcheck client")?
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    anyhow::ensure!(
        response.status().is_success(),
        "GET {url} answered {}",
        response.status()
    );
    Ok(())
}
