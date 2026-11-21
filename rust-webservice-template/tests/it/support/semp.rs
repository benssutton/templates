//! Thin SEMP v2 client: the broker-side view and the operator actions the
//! tests need. Tests assert on broker state rather than trusting the client's
//! own account. Transport errors are returned, so polling helpers can treat
//! them as "not yet" (a paused broker).

use anyhow::{Context, bail};
use reqwest::Method;
use serde_json::{Value, json};
use std::time::Duration;

/// The queue monitor field that counts redelivered messages.
pub const REDELIVERED_FIELD: &str = "redeliveredMsgCount";

#[derive(Clone)]
pub struct Semp {
    http: reqwest::Client,
    base: String,
    vpn: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueStats {
    /// Spooled messages, including delivered-but-unsettled.
    pub spooled: usize,
    /// Consumer flows bound to the queue.
    pub bound: usize,
    /// Delivered to a flow, not yet settled.
    pub unacked: u64,
    /// Redeliveries counted by the broker.
    pub redelivered: u64,
}

/// Percent-encode a name for one SEMP path segment.
pub fn enc(name: &str) -> String {
    name.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn error_status(v: &Value) -> Option<&str> {
    v["meta"]["error"]["status"].as_str()
}

impl Semp {
    pub fn new(base_url: &str) -> Self {
        rust_template::core::install_crypto_provider(); // reqwest is built with rustls-no-provider
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10)) // a paused broker must not hang a test
                .build()
                .expect("HTTP client"),
            base: format!("{base_url}/SEMP/v2"),
            vpn: "default".into(),
        }
    }

    fn path(&self, kind: &str, collection: &str, name: &str) -> String {
        let base = format!("/{kind}/msgVpns/{}/{collection}", self.vpn);
        if name.is_empty() {
            base
        } else {
            format!("{base}/{}", enc(name))
        }
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> anyhow::Result<(u16, Value)> {
        let mut request = self
            .http
            .request(method.clone(), format!("{}{path}", self.base))
            .basic_auth("admin", Some("admin"));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("SEMP {method} {path}"))?;
        let status = response.status().as_u16();
        let text = response.text().await?;
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    async fn get_ok(&self, path: &str) -> anyhow::Result<Value> {
        let (status, body) = self.call(Method::GET, path, None).await?;
        if status != 200 {
            bail!("SEMP GET {path}: HTTP {status} {body}");
        }
        Ok(body)
    }

    /// 200, or 400 with ALREADY_EXISTS (idempotent create).
    async fn create(&self, path: &str, body: Value) -> anyhow::Result<()> {
        let (status, reply) = self.call(Method::POST, path, Some(body)).await?;
        match (status, error_status(&reply)) {
            (200, _) | (400, Some("ALREADY_EXISTS")) => Ok(()),
            _ => bail!("SEMP POST {path}: HTTP {status} {reply}"),
        }
    }

    /// SEMP v2 reports a missing object as HTTP 400 with NOT_FOUND (not 404).
    pub async fn queue_exists(&self, queue: &str) -> anyhow::Result<bool> {
        let (status, body) = self
            .call(Method::GET, &self.path("config", "queues", queue), None)
            .await?;
        match (status, error_status(&body)) {
            (200, _) => Ok(true),
            (400, Some("NOT_FOUND")) => Ok(false),
            _ => bail!("SEMP queue {queue}: HTTP {status} {body}"),
        }
    }

    pub async fn queue_subscriptions(&self, queue: &str) -> anyhow::Result<Vec<String>> {
        let body = self
            .get_ok(&format!(
                "{}/subscriptions",
                self.path("config", "queues", queue)
            ))
            .await?;
        Ok(body["data"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s["subscriptionTopic"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Counts the monitor `msgs` collection, following the paging cursor.
    /// The scalar `spooledMsgCount` is cumulative and is not used.
    pub async fn queue_message_count(&self, queue: &str) -> anyhow::Result<usize> {
        let path = format!("{}/msgs", self.path("monitor", "queues", queue));
        let mut query = "count=100".to_owned();
        let mut total = 0;
        loop {
            let body = self.get_ok(&format!("{path}?{query}")).await?;
            total += body["data"].as_array().map_or(0, Vec::len);
            let cursor = body["meta"]["paging"]["nextPageUri"]
                .as_str()
                .and_then(|uri| uri.split("cursor=").nth(1))
                .map(|c| c.split('&').next().unwrap_or(c).to_owned());
            match cursor {
                Some(c) if !c.is_empty() => query = format!("count=100&cursor={c}"),
                _ => return Ok(total),
            }
        }
    }

    async fn tx_flows(&self, queue: &str) -> anyhow::Result<Vec<Value>> {
        let body = self
            .get_ok(&format!(
                "{}/txFlows?count=100",
                self.path("monitor", "queues", queue)
            ))
            .await?;
        Ok(body["data"].as_array().cloned().unwrap_or_default())
    }

    pub async fn queue_bind_count(&self, queue: &str) -> anyhow::Result<usize> {
        Ok(self.tx_flows(queue).await?.len())
    }

    pub async fn queue_unacked_count(&self, queue: &str) -> anyhow::Result<u64> {
        Ok(self
            .tx_flows(queue)
            .await?
            .iter()
            .filter_map(|f| f["unackedMsgCount"].as_u64())
            .sum())
    }

    pub async fn queue_stats(&self, queue: &str) -> anyhow::Result<QueueStats> {
        let monitor = self.get_ok(&self.path("monitor", "queues", queue)).await?;
        Ok(QueueStats {
            spooled: self.queue_message_count(queue).await?,
            bound: self.queue_bind_count(queue).await?,
            unacked: self.queue_unacked_count(queue).await?,
            redelivered: monitor["data"][REDELIVERED_FIELD].as_u64().unwrap_or(0),
        })
    }

    /// An exclusive queue. `permission` is what non-owners may do
    /// ("consume", or "no-access" for the bind-denied tests).
    pub async fn create_queue(
        &self,
        queue: &str,
        permission: &str,
        owner: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut body = json!({
            "queueName": queue,
            "accessType": "exclusive",
            "permission": permission,
            "ingressEnabled": true,
            "egressEnabled": true,
        });
        if let Some(owner) = owner {
            body["owner"] = json!(owner);
        }
        self.create(&self.path("config", "queues", ""), body).await
    }

    pub async fn add_subscription(&self, queue: &str, topic: &str) -> anyhow::Result<()> {
        let path = format!("{}/subscriptions", self.path("config", "queues", queue));
        self.create(&path, json!({ "subscriptionTopic": topic }))
            .await
    }

    /// Operator action: shut (or reopen) a queue's egress. The broker unbinds
    /// every flow on it.
    pub async fn set_queue_egress(&self, queue: &str, enabled: bool) -> anyhow::Result<()> {
        let path = self.path("config", "queues", queue);
        let body = json!({ "egressEnabled": enabled });
        let (status, reply) = self.call(Method::PATCH, &path, Some(body)).await?;
        if status != 200 {
            bail!("SEMP PATCH {path}: HTTP {status} {reply}");
        }
        Ok(())
    }

    /// Best effort (cleanup).
    pub async fn delete_queue(&self, queue: &str) {
        let _ = self
            .call(Method::DELETE, &self.path("config", "queues", queue), None)
            .await;
    }

    // No client-username helpers: the default VPN offers only SASL
    // ANONYMOUS over AMQP, so tests connect anonymously as `default`; the bind-denied tests use an
    // owner-less `no-access` queue and the watchdog test pauses the broker.

    /// Enable AMQP plain text on the VPN (not needed on 10.26's default VPN).
    pub async fn enable_amqp(&self, port: u16) -> anyhow::Result<()> {
        let path = format!("/config/msgVpns/{}", self.vpn);
        let body =
            json!({ "serviceAmqpPlainTextEnabled": true, "serviceAmqpPlainTextListenPort": port });
        let (status, reply) = self.call(Method::PATCH, &path, Some(body)).await?;
        if status != 200 {
            bail!("SEMP PATCH {path}: HTTP {status} {reply}");
        }
        Ok(())
    }
}
