//! Serving over plain HTTP (tests, local-dev override) or rustls HTTPS
//! (default: 443 + ./certs/*.pem, Python's self-signed pattern), with
//! graceful shutdown driven by the app's `Shutdown` handle.

use crate::app::App;
use crate::settings::Settings;
use anyhow::Context;
use axum_server::{tls_rustls::RustlsConfig, Handle};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// How long in-flight requests get to finish once shutdown starts.
pub const DRAIN_GRACE: Duration = Duration::from_secs(10);

/// rustls needs one process-wide crypto provider (Deviation 4: ring).
/// Safe to call repeatedly.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

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

    let service = app.router.clone().into_make_service();
    // Every exit, including early setup errors, must reach `app.close()`
    // (which releases the store pools from R2 on).
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
