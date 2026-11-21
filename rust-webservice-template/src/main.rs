//! Thin entry point: settings -> telemetry -> bind -> create_app -> signals
//! -> serve. The port is bound before the stores connect, so a port clash
//! fails at once instead of after the connect retries. Any startup error
//! exits non-zero with a readable message.
//!
//! `rust-template healthcheck` probes the running server instead (the
//! container healthcheck; the runtime image has no curl).

use rust_template::{app::create_app, core::telemetry, server, settings::Settings};
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let settings = match Settings::from_env() {
        Ok(settings) => settings,
        Err(err) => {
            eprintln!("fatal: {err:#}");
            return ExitCode::FAILURE;
        }
    };
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return match server::healthcheck(&settings).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("unhealthy: {err:#}");
                ExitCode::FAILURE
            }
        };
    }
    let _telemetry = match telemetry::init(&settings) {
        Ok(guard) => guard,
        Err(err) => {
            eprintln!("fatal: {err:#}");
            return ExitCode::FAILURE;
        }
    };
    match run(settings).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!("fatal: {err:#}");
            eprintln!("fatal: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(settings: Settings) -> anyhow::Result<()> {
    tracing::info!(
        app_title = %settings.app_title,
        app_version = %settings.app_version,
        ingest_transport = settings.ingest_transport.as_str(),
        "starting rust-template"
    );
    let tls = server::Tls::from_settings(&settings);
    let listener = server::bind(&settings)?;
    let app = create_app(settings).await?;

    let shutdown = app.state.shutdown.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        shutdown.trigger("termination signal received");
    });
    server::serve(listener, app, tls).await
}

/// Ctrl-C everywhere; SIGTERM too on unix (docker stop / Kubernetes).
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "cannot install SIGTERM handler; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
