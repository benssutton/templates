//! Solace Guaranteed messaging over AMQP 1.0 (Python
//! ingestion/solace/client.py; spec §6). Binds the durable exclusive queue
//! `solace_queue`, subscribed to `solace_topic` (provisioned by SEMP at
//! startup, D1: see `provision`), and yields one `Delivery` per message,
//! settled by StreamIngestService after the LSM write.
//!
//! Recovery: a lost link or connection moves the state to RECONNECTING and
//! the consumer reconnects forever (Python's `forever_retry`); the ingest
//! watchdog (`ingest_max_disconnect_seconds`) is the one place that gives up.
//! A refused bind is returned as an error for the consecutive-failure trigger.
//! Dropping a pending `next_delivery` future can strand one in-flight
//! message until the next bind; only shutdown does that.
//!
//! D9: a second receiver on an exclusive queue attaches as a standby without
//! error, so `Connected` means "transport up", never "flow active".

pub mod amqp;
pub mod provision;
pub mod publisher;
pub mod semp;

use self::amqp::AmqpTarget;
use super::ipc::decode_stream;
use super::{BatchConsumer, ConnectionState, Delivery, Settle};
use crate::core::retry::{connect_with_backoff, Backoff};
use crate::settings::Settings;
use anyhow::Context;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::definitions;
use fe2o3_amqp::types::messaging::{Body, Modified};
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp::Receiver;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;

/// Pace of reconnect attempts while RECONNECTING (Python: 1000 ms).
pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

type SharedReceiver = Arc<Mutex<Receiver>>;

/// One live connection, its session and (once bound) the receiver.
struct Link {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    receiver: Option<SharedReceiver>,
}

impl Link {
    /// Best effort, bounded. The receiver is detached (not closed), so the
    /// durable queue is never destroyed by a shutdown. If a settlement still
    /// holds the receiver, the connection close detaches it. D10(3): the old
    /// connection is always closed, so no stale exclusive flow survives.
    async fn shutdown(self) {
        if let Some(receiver) = self.receiver {
            if let Ok(receiver) = Arc::try_unwrap(receiver) {
                let _ =
                    tokio::time::timeout(amqp::TEARDOWN_TIMEOUT, receiver.into_inner().detach())
                        .await;
            }
        }
        amqp::teardown(self.connection, self.session).await;
    }
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
    /// Provision (D1: before the first attach), then connect with the startup
    /// retry budget. The broker can accept TCP before its VPN serves clients,
    /// so the open is retried like every other startup dependency.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Self> {
        provision::prepare(settings)
            .await
            .context("Solace: queue provisioning failed")?;
        let target = AmqpTarget::from_settings(settings);
        let (connection, session) =
            connect_with_backoff("Solace", &Backoff::from_settings(settings), || {
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
            link: Mutex::new(Some(Link {
                connection,
                session,
                receiver: None,
            })),
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

    fn link_name(&self) -> String {
        format!("{}-receiver", self.settings.solace_queue.replace('/', "_"))
    }
}

#[async_trait::async_trait]
impl BatchConsumer for SolaceBatchConsumer {
    async fn next_delivery(&self) -> anyhow::Result<Option<Delivery>> {
        let mut guard = self.link.lock().await;
        loop {
            if self.closed.is_cancelled() {
                return Ok(None);
            }
            // 1. A connection (reconnect forever while RECONNECTING).
            let Some(link) = guard.as_mut() else {
                let opened = self
                    .cancellable(amqp::open(
                        &self.target,
                        amqp::container_id(&self.settings.solace_queue),
                    ))
                    .await;
                match opened {
                    None => return Ok(None),
                    Some(Ok((connection, session))) => {
                        *guard = Some(Link {
                            connection,
                            session,
                            receiver: None,
                        });
                        // Still CONNECTED: the last call's bind was refused
                        // and tore the link down; the transport never went away.
                        if self.connection_state() == ConnectionState::Connected {
                            tracing::debug!("Solace: reopening after bind failure");
                        } else {
                            tracing::info!("Solace: reconnected");
                        }
                        self.set_state(ConnectionState::Connected);
                    }
                    Some(Err(err)) => {
                        self.set_state(ConnectionState::Reconnecting);
                        tracing::warn!(error = ?err, "Solace: reconnect attempt failed");
                        if self
                            .cancellable(tokio::time::sleep(RECONNECT_INTERVAL))
                            .await
                            .is_none()
                        {
                            return Ok(None);
                        }
                    }
                }
                continue;
            };

            // 2. A bound receiver. A refusal is an error for the caller.
            let receiver = match &link.receiver {
                Some(receiver) => receiver.clone(),
                None => {
                    let source = provision::source(&self.settings);
                    let attached = self
                        .cancellable(amqp::attach_receiver(
                            &mut link.session,
                            self.link_name(),
                            source,
                        ))
                        .await;
                    match attached {
                        None => return Ok(None),
                        Some(Ok(receiver)) => {
                            let receiver = Arc::new(Mutex::new(receiver));
                            link.receiver = Some(receiver.clone());
                            // D9: a standby attach counts as bound too.
                            let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
                            tracing::info!(queue = %self.settings.solace_queue, epoch, "Solace: queue bound");
                            receiver
                        }
                        Some(Err(err)) => {
                            // Tear down before returning: the next call reconnects.
                            if let Some(old) = guard.take() {
                                old.shutdown().await;
                            }
                            return Err(err.context(format!(
                                "Solace: bind to queue {} failed",
                                self.settings.solace_queue
                            )));
                        }
                    }
                }
            };

            // 3. The next message.
            let received = {
                let mut rx = receiver.lock().await;
                self.cancellable(rx.recv::<Body<Value>>()).await
            };
            match received {
                None => return Ok(None),
                Some(Err(err)) => {
                    // D6/D10: a frozen broker surfaces here (after the idle
                    // timeout) as LinkStateError(SessionStopped(ConnectionStopped(Closed))).
                    // Every recv error is a lost connection: close the old
                    // connection before reconnecting, so no stale exclusive
                    // flow survives.
                    self.set_state(ConnectionState::Reconnecting);
                    tracing::warn!(error = ?err, "Solace: link lost; reconnecting");
                    drop(receiver);
                    if let Some(old) = guard.take() {
                        old.shutdown().await;
                    }
                    // Pace the reopen: a broker that accepts the open and the
                    // attach, then detaches at once (flow cleared, egress shut,
                    // queue deleted), would otherwise spin at round-trip speed.
                    if self
                        .cancellable(tokio::time::sleep(RECONNECT_INTERVAL))
                        .await
                        .is_none()
                    {
                        return Ok(None);
                    }
                }
                Some(Ok(delivery)) => {
                    let redelivered = amqp::redelivered(delivery.message().header.as_ref());
                    let (info, message) = delivery.into_parts();
                    let decoded = amqp::payload_bytes(&message.body)
                        .context("message has no binary body")
                        .and_then(|bytes| decode_stream(&bytes).map_err(anyhow::Error::from));
                    if self.closed.is_cancelled() {
                        // close() won the race with recv: leave the message
                        // unsettled (the broker redelivers it on the next bind).
                        return Ok(None);
                    }
                    match decoded {
                        Ok(batches) => {
                            return Ok(Some(Delivery {
                                batches,
                                redelivered,
                                settle: Box::new(AmqpSettle { receiver, info }),
                            }));
                        }
                        Err(err) => {
                            // Poison message: it can never decode, so never redeliver it.
                            tracing::warn!(
                                error = %format!("{err:#}"),
                                "Solace: malformed IPC message rejected"
                            );
                            let rejected = settle(&receiver, info, Outcome::Reject).await;
                            if let Err(err) = rejected {
                                // Flow already gone: the broker redelivers on the next bind.
                                tracing::debug!(error = ?err, "Solace: could not reject malformed message");
                            }
                        }
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

/// AMQP outcomes for one delivery (spec §6, D5).
struct AmqpSettle {
    receiver: SharedReceiver,
    info: DeliveryInfo,
}

/// The three AMQP outcomes R3 uses (D5). `released` is never used.
enum Outcome {
    Accept,
    /// `modified{delivery_failed: true}`: redelivered, delivery-count + 1.
    Fail,
    Reject,
}

/// One settlement on `receiver`, bounded by `amqp::SETTLE_TIMEOUT` (lock
/// included: a settle never waits forever behind a pending `recv`).
async fn settle(
    receiver: &SharedReceiver,
    info: DeliveryInfo,
    outcome: Outcome,
) -> anyhow::Result<()> {
    let what = match outcome {
        Outcome::Accept => "accept",
        Outcome::Fail => "modify(delivery_failed)",
        Outcome::Reject => "reject",
    };
    let attempt = async {
        let rx = receiver.lock().await;
        match outcome {
            Outcome::Accept => rx.accept(info).await,
            Outcome::Fail => {
                let modified = Modified {
                    delivery_failed: Some(true),
                    undeliverable_here: Some(false),
                    message_annotations: None,
                };
                rx.modify(info, modified).await
            }
            Outcome::Reject => rx.reject(info, None::<definitions::Error>).await,
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
        settle(&self.receiver, self.info, Outcome::Accept).await
    }

    async fn fail(self: Box<Self>) -> anyhow::Result<()> {
        settle(&self.receiver, self.info, Outcome::Fail).await
    }

    async fn reject(self: Box<Self>) -> anyhow::Result<()> {
        settle(&self.receiver, self.info, Outcome::Reject).await
    }
}
