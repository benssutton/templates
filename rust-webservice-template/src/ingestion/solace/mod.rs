//! Solace Guaranteed messaging over AMQP 1.0. Binds the durable exclusive
//! queue `solace_queue`, subscribed to `solace_topic` (provisioned through
//! SEMP at startup, see `provision`), and yields one `Delivery` per message,
//! settled by `StreamIngestService` after the LSM write.
//!
//! Recovery: a lost link or connection moves the state to RECONNECTING and
//! the consumer reconnects forever; the ingest watchdog
//! (`ingest_max_disconnect_seconds`) is the one place that gives up. A
//! refused bind is returned as an error for the consecutive-failure trigger.
//! Dropping a pending `next_delivery` future can strand one in-flight
//! message until the next bind; only shutdown does that.
//!
//! A second receiver on an exclusive queue attaches as a standby without
//! error, so `Connected` means "transport up", never "flow active".

pub mod amqp;
pub mod provision;
pub mod publisher;
pub mod semp;

use self::amqp::AmqpTarget;
use super::ipc::decode_stream;
use super::{BatchConsumer, ConnectionState, Delivery, Settle, Settlement};
use crate::core::retry::{Backoff, connect_with_backoff};
use crate::settings::Settings;
use anyhow::Context;
use arrow::array::RecordBatch;
use fe2o3_amqp::Receiver;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::definitions;
use fe2o3_amqp::types::messaging::{Body, Modified};
use fe2o3_amqp::types::primitives::Value;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio_util::sync::CancellationToken;

/// Pace of reconnect attempts while RECONNECTING.
pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

type SharedReceiver = Arc<Mutex<Receiver>>;

/// One live connection, its session and (once bound) the receiver.
struct Link {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    receiver: Option<SharedReceiver>,
}

impl Link {
    fn new((connection, session): (ConnectionHandle<()>, SessionHandle<()>)) -> Self {
        Self {
            connection,
            session,
            receiver: None,
        }
    }

    /// Best effort, bounded. The receiver is detached (not closed), so the
    /// durable queue is never destroyed by a shutdown. If a settlement still
    /// holds the receiver, the connection close detaches it. The connection is
    /// always closed: a stale one would keep the exclusive queue's flow.
    async fn shutdown(self) {
        if let Some(receiver) = self.receiver
            && let Ok(receiver) = Arc::try_unwrap(receiver)
        {
            let _ =
                tokio::time::timeout(amqp::TEARDOWN_TIMEOUT, receiver.into_inner().detach()).await;
        }
        amqp::teardown(self.connection, self.session).await;
    }
}

/// What one `recv` produced.
enum Received {
    Message(Delivery),
    /// Undecodable payload, already rejected: ask for the next one.
    Poison,
    /// The link or connection is gone: reconnect.
    LinkLost,
    /// `close()` ran first.
    Closed,
}

pub struct SolaceBatchConsumer {
    settings: Arc<Settings>,
    target: AmqpTarget,
    state: watch::Sender<ConnectionState>,
    closed: CancellationToken,
    link: Mutex<Option<Link>>,
    /// Successful binds so far (`BatchConsumer::bound_epoch`).
    epoch: AtomicU64,
}

impl SolaceBatchConsumer {
    /// Provision (before the first attach: an attach to a missing queue
    /// would auto-create it without the subscription), then connect with the
    /// startup retry budget. The broker can accept TCP before its VPN serves
    /// clients, so the open is retried like every other startup dependency.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        provision::prepare(settings)
            .await
            .context("Solace: queue provisioning failed")?;
        let target = AmqpTarget::from_settings(settings);
        let opened = connect_with_backoff("Solace", &Backoff::from_settings(settings), || {
            amqp::open(&target, amqp::container_id(&settings.solace_queue))
        })
        .await
        .context("Solace: connect failed")?;
        tracing::info!(url = %target.url(), queue = %settings.solace_queue, "Solace: connected");
        Ok(Self {
            settings: Arc::new(settings.clone()),
            target,
            state: watch::Sender::new(ConnectionState::Connected),
            closed: CancellationToken::new(),
            link: Mutex::new(Some(Link::new(opened))),
            epoch: AtomicU64::new(0),
        })
    }

    /// Late transitions after close() are ignored: a closed consumer stays DOWN.
    fn set_state(&self, state: ConnectionState) {
        if !self.closed.is_cancelled() {
            self.state.send_replace(state);
        }
    }

    /// `fut`, unless close() runs first (`None`).
    async fn cancellable<F: Future>(&self, fut: F) -> Option<F::Output> {
        tokio::select! {
            biased;
            _ = self.closed.cancelled() => None,
            out = fut => Some(out),
        }
    }

    /// Wait `RECONNECT_INTERVAL`; false if close() ran meanwhile.
    async fn pace(&self) -> bool {
        self.cancellable(tokio::time::sleep(RECONNECT_INTERVAL))
            .await
            .is_some()
    }

    fn link_name(&self) -> String {
        format!("{}-receiver", self.settings.solace_queue.replace('/', "_"))
    }

    /// Step 1: make sure there is a connection. Returns false once closed.
    /// A failed attempt marks RECONNECTING and waits before returning, so
    /// the caller simply loops.
    async fn reconnect(&self, guard: &mut MutexGuard<'_, Option<Link>>) -> bool {
        let opened = self
            .cancellable(amqp::open(
                &self.target,
                amqp::container_id(&self.settings.solace_queue),
            ))
            .await;
        match opened {
            None => false,
            Some(Ok(opened)) => {
                **guard = Some(Link::new(opened));
                // Still CONNECTED: the last bind was refused and tore the
                // link down; the transport never went away.
                if self.connection_state() == ConnectionState::Connected {
                    tracing::debug!("Solace: reopening after bind failure");
                } else {
                    tracing::info!("Solace: reconnected");
                }
                self.set_state(ConnectionState::Connected);
                true
            }
            Some(Err(err)) => {
                self.set_state(ConnectionState::Reconnecting);
                tracing::warn!(error = ?err, "Solace: reconnect attempt failed");
                self.pace().await
            }
        }
    }

    /// Step 2: a bound receiver on `link`. `Ok(None)` once closed; `Err` is
    /// a refused bind.
    async fn bind(&self, link: &mut Link) -> anyhow::Result<Option<SharedReceiver>> {
        if let Some(receiver) = &link.receiver {
            return Ok(Some(receiver.clone()));
        }
        let attached = self
            .cancellable(amqp::attach_receiver(
                &mut link.session,
                self.link_name(),
                provision::source(&self.settings),
            ))
            .await;
        match attached {
            None => Ok(None),
            Some(Ok(receiver)) => {
                let receiver = Arc::new(Mutex::new(receiver));
                link.receiver = Some(receiver.clone());
                // A standby attach counts as bound too: the transport works.
                let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
                tracing::info!(queue = %self.settings.solace_queue, epoch, "Solace: queue bound");
                Ok(Some(receiver))
            }
            Some(Err(err)) => Err(err.context(format!(
                "Solace: bind to queue {} failed",
                self.settings.solace_queue
            ))),
        }
    }

    /// Step 3: the next message, decoded off the async workers.
    async fn receive(&self, receiver: SharedReceiver) -> Received {
        let received = {
            let mut rx = receiver.lock().await;
            self.cancellable(rx.recv::<Body<Value>>()).await
        };
        let delivery = match received {
            None => return Received::Closed,
            Some(Err(err)) => {
                // A frozen broker surfaces here, after the idle timeout, as
                // LinkStateError(SessionStopped(ConnectionStopped(Closed))).
                tracing::warn!(error = ?err, "Solace: link lost; reconnecting");
                return Received::LinkLost;
            }
            Some(Ok(delivery)) => delivery,
        };
        let redelivered = amqp::redelivered(delivery.message().header.as_ref());
        let (info, message) = delivery.into_parts();
        let decoded = decode(amqp::payload_bytes(&message.body)).await;
        if self.closed.is_cancelled() {
            // close() won the race with recv: leave the message unsettled
            // (the broker redelivers it on the next bind).
            return Received::Closed;
        }
        match decoded {
            Ok(batches) => Received::Message(Delivery {
                batches,
                redelivered,
                settle: Box::new(AmqpSettle { receiver, info }),
            }),
            Err(err) => {
                // A poison message can never decode, so never redeliver it.
                tracing::warn!(error = %format!("{err:#}"), "Solace: malformed IPC message rejected");
                if let Err(err) = settle(&receiver, info, Settlement::Reject).await {
                    // Flow already gone: the broker redelivers on the next bind.
                    tracing::debug!(error = ?err, "Solace: could not reject malformed message");
                }
                Received::Poison
            }
        }
    }
}

/// Decoding is CPU work proportional to the payload, so it runs on the
/// blocking pool.
async fn decode(payload: Option<Vec<u8>>) -> anyhow::Result<Vec<RecordBatch>> {
    let bytes = payload.context("message has no binary body")?;
    tokio::task::spawn_blocking(move || decode_stream(&bytes))
        .await
        .context("decode task failed")?
        .map_err(anyhow::Error::from)
}

#[async_trait::async_trait]
impl BatchConsumer for SolaceBatchConsumer {
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>> {
        let mut guard = self.link.lock().await;
        loop {
            if self.closed.is_cancelled() {
                return Ok(None);
            }
            let Some(link) = guard.as_mut() else {
                if !self.reconnect(&mut guard).await {
                    return Ok(None);
                }
                continue;
            };
            let receiver = match self.bind(link).await {
                Ok(Some(receiver)) => receiver,
                Ok(None) => return Ok(None),
                Err(err) => {
                    // Tear down before returning: the next call reconnects.
                    if let Some(old) = guard.take() {
                        old.shutdown().await;
                    }
                    return Err(err);
                }
            };
            match self.receive(receiver).await {
                Received::Message(delivery) => return Ok(Some(delivery)),
                Received::Poison => {}
                Received::Closed => return Ok(None),
                Received::LinkLost => {
                    self.set_state(ConnectionState::Reconnecting);
                    if let Some(old) = guard.take() {
                        old.shutdown().await;
                    }
                    // Pace the reopen: a broker that accepts the open and
                    // the attach, then detaches at once (flow cleared,
                    // egress shut, queue deleted), would otherwise spin at
                    // round-trip speed.
                    if !self.pace().await {
                        return Ok(None);
                    }
                }
            }
        }
    }

    async fn close(&self) {
        self.closed.cancel();
        self.state.send_replace(ConnectionState::Down);
        // next_delivery has returned by now. Every set_state runs under this
        // mutex, so sending DOWN again once it is held makes DOWN the last
        // write even if a set_state passed its `closed` check before cancel().
        let link = {
            let mut guard = self.link.lock().await;
            self.state.send_replace(ConnectionState::Down);
            guard.take()
        };
        if let Some(link) = link {
            link.shutdown().await;
        }
    }

    fn connection_state(&self) -> ConnectionState {
        *self.state.borrow()
    }

    fn bound_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
}

/// AMQP settlement for one delivery.
struct AmqpSettle {
    receiver: SharedReceiver,
    info: DeliveryInfo,
}

/// One settlement on `receiver`, bounded by `amqp::SETTLE_TIMEOUT` (lock
/// included: a settle never waits forever behind a pending `recv`).
async fn settle(
    receiver: &SharedReceiver,
    info: DeliveryInfo,
    outcome: Settlement,
) -> anyhow::Result<()> {
    let what = match outcome {
        Settlement::Accept => "accept",
        Settlement::Fail => "modify(delivery_failed)",
        Settlement::Reject => "reject",
    };
    let attempt = async {
        let rx = receiver.lock().await;
        match outcome {
            Settlement::Accept => rx.accept(info).await,
            // `modified{delivery_failed}`, not `released`, so the broker
            // counts the attempt and its redelivery limit applies.
            Settlement::Fail => {
                let modified = Modified {
                    delivery_failed: Some(true),
                    undeliverable_here: Some(false),
                    message_annotations: None,
                };
                rx.modify(info, modified).await
            }
            Settlement::Reject => rx.reject(info, None::<definitions::Error>).await,
        }
    };
    tokio::time::timeout(amqp::SETTLE_TIMEOUT, attempt)
        .await
        .with_context(|| format!("Solace: {what} timed out after {:?}", amqp::SETTLE_TIMEOUT))?
        .with_context(|| format!("Solace: {what}"))
}

#[async_trait::async_trait]
impl Settle for AmqpSettle {
    async fn accept(self: Box<Self>) -> anyhow::Result<()> {
        settle(&self.receiver, self.info, Settlement::Accept).await
    }

    async fn fail(self: Box<Self>) -> anyhow::Result<()> {
        settle(&self.receiver, self.info, Settlement::Fail).await
    }

    async fn reject(self: Box<Self>) -> anyhow::Result<()> {
        settle(&self.receiver, self.info, Settlement::Reject).await
    }
}
