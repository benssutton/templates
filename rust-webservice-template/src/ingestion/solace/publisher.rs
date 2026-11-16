//! Persistent AMQP publisher to a topic: the tests' publisher (spec §6
//! Tests) and the compose demo publisher (Deviation 10). Python's
//! SolacePublisher waits for the broker's ack; here `send` returns the
//! broker's disposition and only `accepted` counts.

use super::amqp::{self, AmqpTarget, ATTACH_TIMEOUT, TEARDOWN_TIMEOUT};
use crate::ingestion::ipc::encode_stream;
use anyhow::Context;
use arrow::array::RecordBatch;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::{Header, Message, Outcome};
use fe2o3_amqp::types::primitives::Binary;
use fe2o3_amqp::Sender;
use std::time::Duration;

pub const PUBLISH_TIMEOUT: Duration = Duration::from_secs(15);

pub struct AmqpPublisher {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    sender: Sender,
    address: String,
}

impl AmqpPublisher {
    /// [SPIKE-VERIFY] `Sender::attach(&mut session, name, address)`.
    pub async fn connect(target: &AmqpTarget, topic: &str) -> anyhow::Result<Self> {
        let (connection, mut session) = amqp::open(target, amqp::container_id("publisher")).await?;
        let address = amqp::topic_address(topic);
        let link = amqp::container_id("publisher-link");
        let attached = tokio::time::timeout(
            ATTACH_TIMEOUT,
            Sender::attach(&mut session, link, address.clone()),
        )
        .await;
        match attached {
            Ok(Ok(sender)) => Ok(Self {
                connection,
                session,
                sender,
                address,
            }),
            Ok(Err(err)) => {
                amqp::teardown(connection, session).await;
                Err(anyhow::Error::new(err).context(format!("attach sender to {address}")))
            }
            Err(_) => {
                amqp::teardown(connection, session).await;
                anyhow::bail!("attach sender to {address}: timed out")
            }
        }
    }

    /// A persistent (durable header) message with ONE data section. Returns
    /// once the broker has accepted it, i.e. spooled it on every queue whose
    /// subscription matches the topic. [SPIKE-VERIFY] `Message::builder()
    /// .header(..).data(Binary)`, `Sender::send -> Outcome`.
    pub async fn publish_bytes(&mut self, payload: Vec<u8>) -> anyhow::Result<()> {
        let message = Message::builder()
            .header(Header::builder().durable(true).build())
            .data(Binary::from(payload))
            .build();
        let outcome = tokio::time::timeout(PUBLISH_TIMEOUT, self.sender.send(message))
            .await
            .with_context(|| format!("publish to {}: timed out", self.address))?
            .with_context(|| format!("publish to {}", self.address))?;
        match outcome {
            Outcome::Accepted(_) => Ok(()),
            other => anyhow::bail!(
                "broker did not accept the message on {}: {other:?}",
                self.address
            ),
        }
    }

    /// One batch as one Arrow IPC stream message (Python `publish_batch`).
    pub async fn publish_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        self.publish_bytes(encode_stream(std::slice::from_ref(batch))?)
            .await
    }

    pub async fn close(self) {
        let _ = tokio::time::timeout(TEARDOWN_TIMEOUT, self.sender.close()).await;
        amqp::teardown(self.connection, self.session).await;
    }
}
