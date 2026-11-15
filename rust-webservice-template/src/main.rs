//! Thin entry point (spec §8: keep main.rs a thin call into the library).
//! settings -> create_app -> bind -> signals -> serve; any startup error
//! exits non-zero with a readable message.

use rust_template::{app::create_app, server, settings::Settings};
use std::process::ExitCode;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!("fatal: {err:#}");
            eprintln!("fatal: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env()?;
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
        use tokio::signal::unix::{signal, SignalKind};
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
