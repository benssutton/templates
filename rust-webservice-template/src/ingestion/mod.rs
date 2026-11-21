//! Streaming ingest transports. A `BatchConsumer`
//! yields one `Delivery` per broker message; `StreamIngestService` settles
//! each delivery exactly once, AFTER the LSM write:
//!
//! accept() -> ACCEPTED: removed from the queue
//! fail()   -> FAILED:   redelivered (after max-redelivery -> DMQ)
//! reject() -> REJECTED: removed, to the DMQ if configured; never redelivered
//!
//! Redelivery is bounded only by the queue's own policy. Queues created by
//! this service (either provisioning variant) get the broker default of
//! unlimited max-redelivery and no DMQ, so a persistently failing message is
//! retried forever; the service backs off between consecutive fails. For
//! production queues an operator should configure max-redelivery and a DMQ.

pub mod ipc;
pub mod noop;
#[cfg(feature = "solace")]
pub mod solace;

use crate::settings::{IngestTransport, Settings};
use arrow::array::RecordBatch;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionState {
    Connected,
    Reconnecting,
    Down,
}

impl ConnectionState {
    pub const ALL: [Self; 3] = [Self::Connected, Self::Reconnecting, Self::Down];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Reconnecting => "reconnecting",
            Self::Down => "down",
        }
    }
}

/// How a delivery is settled with the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// Removed from the queue.
    Accept,
    /// Redelivered (the broker counts the failure).
    Fail,
    /// Removed and never redelivered (to the DMQ if one is configured).
    Reject,
}

impl Settlement {
    /// `ingest_outcomes_total{outcome=...}` label.
    pub fn metric_label(self) -> &'static str {
        match self {
            Self::Accept => "accepted",
            Self::Fail => "failed",
            Self::Reject => "rejected",
        }
    }
}

/// Broker settlement for one delivery. Each method consumes the box, so a
/// delivery is settled at most once. A failed settlement is logged by the
/// caller, never retried: the broker still holds the message and redelivers it.
///
/// Ordering: settle a delivery BEFORE calling `next_delivery` again. A
/// transport may hold its receiver for the whole of a pending `next_delivery`
/// (Solace: `recv` holds the receiver mutex while it waits), so a concurrent
/// settlement would wait behind it (bounded, then an error).
#[async_trait::async_trait]
pub trait Settle: Send {
    async fn accept(self: Box<Self>) -> anyhow::Result<()>;
    async fn fail(self: Box<Self>) -> anyhow::Result<()>;
    async fn reject(self: Box<Self>) -> anyhow::Result<()>;
}

/// One transport message: every Arrow batch decoded from it, and its settlement.
pub struct Delivery {
    pub batches: Vec<RecordBatch>,
    pub redelivered: bool,
    pub settle: Box<dyn Settle>,
}

/// A streaming transport. Every method takes `&self`, so one
/// `Arc<dyn BatchConsumer>` is shared: the ingest task awaits `next_delivery` while `close` (from `App::close`) and
/// `connection_state` (from the health path) run concurrently.
#[async_trait::async_trait]
pub trait BatchConsumer: Send + Sync {
    /// The next delivery; `Ok(None)` once the consumer is closed. An `Err`
    /// is one failed attempt (counted by the service's consecutive-failure
    /// trigger); calling again retries. Not meant to be called concurrently
    /// with itself, nor while the previous delivery is still unsettled (see
    /// `Settle`: settle first, then ask for the next one).
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>>;

    /// Idempotent. Unblocks a pending `next_delivery` (which returns `Ok(None)`).
    async fn close(&self);

    /// Cheap cached read (a `watch` channel), no I/O.
    fn connection_state(&self) -> ConnectionState;

    /// Count of successful queue binds so far (0 = never bound). A rise
    /// proves the transport works, so the ingest loop resets its
    /// consecutive-failure count and transport backoff: without it, healed
    /// interruptions on a quiet queue would add up to a shutdown over time.
    /// Cheap, no I/O.
    fn bound_epoch(&self) -> u64;
}

/// The consumer for `ingest_transport`, connected with the same startup
/// retry budget as the stores (`connect_with_backoff`).
pub async fn connect(settings: &Settings) -> anyhow::Result<Arc<dyn BatchConsumer>> {
    match settings.ingest_transport {
        IngestTransport::Noop => Ok(Arc::new(noop::NoopBatchConsumer::new())),
        #[cfg(feature = "solace")]
        IngestTransport::Solace => Ok(Arc::new(
            solace::SolaceBatchConsumer::connect(settings).await?,
        )),
        #[cfg(not(feature = "solace"))]
        IngestTransport::Solace => {
            anyhow::bail!("INGEST_TRANSPORT=solace needs a build with the `solace` Cargo feature")
        }
    }
}
