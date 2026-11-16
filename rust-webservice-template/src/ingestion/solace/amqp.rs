//! fe2o3-amqp plumbing for Solace (spec §6). Every fe2o3 call here is
//! [SPIKE-VERIFY]: it is the call R1 Part S compiled and ran against
//! solace-pubsub-standard 10.26.10 (findings note, "fe2o3-amqp API notes").

use crate::settings::Settings;
use anyhow::Context;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Header, Source};
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp::{Connection, Receiver, Session};
use secrecy::{ExposeSecret, SecretString};
use std::time::Duration;

/// [SPIKE D6] Q5: the local AMQP idle timeout. Mandatory: without it, recv
/// and attach block forever on a frozen broker. With it, the dead peer tears
/// the connection down after ~5 s and `Receiver::recv` fails with
/// `LinkStateError(SessionStopped(ConnectionStopped(Closed)))` (not a
/// top-level `IdleTimeoutElapsed`). Also sent to the broker in Open.
pub const IDLE_TIMEOUT_MS: u32 = 5_000;
/// [SPIKE D3] Publish address for a topic ("Publishing to a topic over AMQP").
pub const TOPIC_ADDRESS_PREFIX: &str = "topic://";
/// TCP connect + SASL + Open + Begin, per attempt.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(10);
/// Receiver/sender attach (a refused bind answers well within this).
pub const ATTACH_TIMEOUT: Duration = Duration::from_secs(15);
/// Best-effort close of a dead or paused connection.
pub const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// One disposition (accept / modify / reject), receiver lock included.
pub const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Where and as whom to connect. AMQP picks the message VPN by listen port
/// (Deviation 3).
#[derive(Debug, Clone)]
pub struct AmqpTarget {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: SecretString,
}

impl AmqpTarget {
    pub fn from_settings(s: &Settings) -> Self {
        Self {
            host: s.solace_host.clone(),
            port: s.solace_port,
            username: s.solace_username.clone(),
            password: s.solace_password.clone(),
        }
    }

    pub fn url(&self) -> String {
        format!("amqp://{}:{}", self.host, self.port)
    }
}

pub fn topic_address(topic: &str) -> String {
    format!("{TOPIC_ADDRESS_PREFIX}{topic}")
}

/// Unique per connection (Deviation 9): `rust-template_ingest-1a2b3c4d`.
pub fn container_id(base: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{}-{}", base.replace('/', "_"), &suffix[..8])
}

/// SASL rule (Task 0): an empty username selects ANONYMOUS, the only
/// mechanism an auth-`none` VPN offers over AMQP; otherwise PLAIN. fe2o3 takes
/// one profile up front and cannot fall back. Divergence: Python and Java
/// always send username/password over SMF.
///
/// The returned profile holds the password in clear: never log it.
pub fn sasl_profile(target: &AmqpTarget) -> SaslProfile {
    if target.username.is_empty() {
        SaslProfile::Anonymous
    } else {
        SaslProfile::Plain {
            username: target.username.clone(),
            password: target.password.expose_secret().to_owned(),
        }
    }
}

/// Open a connection and begin one session, within `OPEN_TIMEOUT`.
pub async fn open(
    target: &AmqpTarget,
    container_id: String,
) -> anyhow::Result<(ConnectionHandle<()>, SessionHandle<()>)> {
    let url = target.url();
    let attempt = async {
        let mut connection = Connection::builder()
            .container_id(container_id)
            .sasl_profile(sasl_profile(target))
            .idle_time_out(IDLE_TIMEOUT_MS)
            .open(url.as_str())
            .await
            // D10: `OpenError`'s Display hides the reason ("Not implemented");
            // Debug carries it (e.g. `"PLAIN" is not supported`).
            .map_err(|e| anyhow::anyhow!("AMQP open {url}: {e:?}"))?;
        match Session::begin(&mut connection).await {
            Ok(session) => Ok((connection, session)),
            Err(err) => {
                let _ = connection.close().await;
                Err(anyhow::Error::new(err).context("AMQP session begin"))
            }
        }
    };
    tokio::time::timeout(OPEN_TIMEOUT, attempt)
        .await
        .with_context(|| format!("AMQP open {url}: no answer within {OPEN_TIMEOUT:?}"))?
}

/// Attach a receiver link. A refused bind (permission, missing queue) is an error.
pub async fn attach_receiver(
    session: &mut SessionHandle<()>,
    link_name: String,
    source: Source,
) -> anyhow::Result<Receiver> {
    let attach = Receiver::builder()
        .name(link_name)
        .source(source)
        .attach(session);
    tokio::time::timeout(ATTACH_TIMEOUT, attach)
        .await
        .context("AMQP receiver attach timed out")?
        .context("AMQP receiver attach refused")
}

/// End the session and close the connection, best effort and bounded.
pub async fn teardown(mut connection: ConnectionHandle<()>, mut session: SessionHandle<()>) {
    let _ = tokio::time::timeout(TEARDOWN_TIMEOUT, session.end()).await;
    let _ = tokio::time::timeout(TEARDOWN_TIMEOUT, connection.close()).await;
}

/// [SPIKE D4] Q3: an SMF binary attachment arrives as ONE `data` section;
/// several sections are concatenated. An `amqp-value` holding binary is
/// accepted too (the Q3 workaround). Anything else has no payload.
pub fn payload_bytes(body: &Body<Value>) -> Option<Vec<u8>> {
    match body {
        Body::Data(_) => {
            let mut bytes = Vec::new();
            for part in body.try_as_data().ok()? {
                bytes.extend_from_slice(part);
            }
            Some(bytes)
        }
        Body::Value(AmqpValue(Value::Binary(bytes))) => Some(bytes.to_vec()),
        _ => None,
    }
}

/// [SPIKE D5] Q4: how the broker marks a redelivery. `modified{delivery_failed}`
/// increments `delivery-count`. Never `first_acquirer` (always false on Solace).
pub fn redelivered(header: Option<&Header>) -> bool {
    header.is_some_and(|h| h.delivery_count > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp::types::primitives::Binary;

    #[test]
    fn container_ids_are_unique_and_slash_free() {
        let (a, b) = (
            container_id("rust-template/ingest"),
            container_id("rust-template/ingest"),
        );
        assert_ne!(a, b);
        assert!(
            a.starts_with("rust-template_ingest-") && a.len() == "rust-template_ingest-".len() + 8,
            "{a}"
        );
    }

    #[test]
    fn target_and_topic_addresses() {
        let s = Settings {
            solace_host: "broker".into(),
            solace_port: 5673,
            ..Settings::default()
        };
        assert_eq!(AmqpTarget::from_settings(&s).url(), "amqp://broker:5673");
        assert_eq!(
            topic_address("ingest/batches"),
            format!("{TOPIC_ADDRESS_PREFIX}ingest/batches")
        );
    }

    #[test]
    fn an_empty_username_selects_anonymous_otherwise_plain() {
        let anonymous = AmqpTarget::from_settings(&Settings::default());
        assert!(matches!(sasl_profile(&anonymous), SaslProfile::Anonymous));
        let named = AmqpTarget {
            username: "svc".into(),
            password: SecretString::from("pw"),
            ..anonymous
        };
        match sasl_profile(&named) {
            SaslProfile::Plain { username, password } => {
                assert_eq!((username.as_str(), password.as_str()), ("svc", "pw"))
            }
            _ => panic!("expected PLAIN"),
        }
    }

    #[test]
    fn the_password_never_appears_in_debug_output() {
        let s = Settings {
            solace_username: "svc".into(),
            solace_password: SecretString::from("hunter2-secret"),
            ..Settings::default()
        };
        let target = AmqpTarget::from_settings(&s);
        let shown = format!("{target:?}");
        assert!(!shown.contains("hunter2-secret"), "{shown}");
        assert!(shown.contains("svc"), "{shown}");
    }

    #[tokio::test]
    async fn a_failed_open_does_not_leak_the_password() {
        // Port 1 on loopback refuses at once: no broker needed.
        let target = AmqpTarget {
            host: "127.0.0.1".into(),
            port: 1,
            username: "svc".into(),
            password: SecretString::from("hunter2-secret"),
        };
        let Err(err) = open(&target, container_id("test")).await else {
            panic!("open must fail without a broker");
        };
        let shown = format!("{err:?} / {err:#}");
        assert!(shown.contains("amqp://127.0.0.1:1"), "{shown}");
        assert!(!shown.contains("hunter2-secret"), "{shown}");
    }

    #[test]
    fn binary_amqp_value_is_a_payload_and_an_empty_body_is_not() {
        let body: Body<Value> =
            Body::Value(AmqpValue(Value::Binary(Binary::from(vec![1_u8, 2, 3]))));
        assert_eq!(payload_bytes(&body), Some(vec![1, 2, 3]));
        assert_eq!(payload_bytes(&Body::<Value>::Empty), None);
        assert_eq!(
            payload_bytes(&Body::Value(AmqpValue(Value::Bool(true)))),
            None
        );
    }

    #[test]
    fn redelivery_follows_the_delivery_count() {
        assert!(!redelivered(None));
        let mut header = Header::builder().durable(true).build();
        assert!(!redelivered(Some(&header)));
        header.delivery_count = 1;
        assert!(redelivered(Some(&header)));
    }
}
