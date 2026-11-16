//! Connects to nothing and idles until closed. Selected with
//! INGEST_TRANSPORT=noop for deployments (and tests) that only need the
//! HTTP ingest path. Mirrors Python's and Java's NoopBatchConsumer.

use super::{BatchConsumer, ConnectionState, Delivery};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub struct NoopBatchConsumer {
    closed: CancellationToken,
    state: watch::Sender<ConnectionState>,
}

impl NoopBatchConsumer {
    pub fn new() -> Self {
        Self {
            closed: CancellationToken::new(),
            state: watch::Sender::new(ConnectionState::Connected),
        }
    }
}

impl Default for NoopBatchConsumer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl BatchConsumer for NoopBatchConsumer {
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>> {
        self.closed.cancelled().await;
        Ok(None)
    }

    async fn close(&self) {
        self.closed.cancel();
        self.state.send_replace(ConnectionState::Down);
    }

    fn connection_state(&self) -> ConnectionState {
        *self.state.borrow()
    }

    /// Never binds anything; constant 1 ("bound once") like the
    /// always-connected state.
    fn bound_epoch(&self) -> u64 {
        1
    }
}
