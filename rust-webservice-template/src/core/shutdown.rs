//! Process-wide graceful-shutdown trigger.
//! Shutdown order, owned by `server::serve` and `App::close`:
//! stop the consumer -> drain in-flight HTTP -> close the consumer and pools.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Default)]
pub struct Shutdown {
    token: CancellationToken,
    /// Flipped by the first `trigger` caller only, so the reason is logged once.
    fired: Arc<AtomicBool>,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request shutdown. Logs the first reason only; later calls are no-ops.
    pub fn trigger(&self, reason: &str) {
        if !self.fired.swap(true, Ordering::SeqCst) {
            tracing::warn!(reason, "shutdown triggered");
            self.token.cancel();
        }
    }

    pub fn is_triggered(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Resolves once `trigger` has been called (immediately if it already was).
    pub async fn triggered(&self) {
        self.token.cancelled().await;
    }

    /// For components that select on the token directly (the ingest task,
    /// MCP event streams).
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn trigger_is_seen_by_every_clone_and_is_idempotent() {
        let shutdown = Shutdown::new();
        let observer = shutdown.clone();
        assert!(!observer.is_triggered());
        let waiter = tokio::spawn(async move { observer.triggered().await });
        shutdown.trigger("test");
        shutdown.trigger("second call is a no-op");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter woke")
            .unwrap();
        assert!(shutdown.is_triggered());
        assert!(shutdown.token().is_cancelled());
    }
}
