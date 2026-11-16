//! [VARIANT B] SEMP v2 provisioning (spec §4 fallback): an exclusive durable
//! queue that non-owners may consume, plus its topic subscription. A 400 with
//! `ALREADY_EXISTS` is success, so provisioning is idempotent.

use crate::settings::Settings;
use anyhow::{bail, Context};
use reqwest::Method;
use secrecy::{ExposeSecret, SecretString};
use serde_json::{json, Value};
use std::time::Duration;

pub struct SempProvisioner {
    http: reqwest::Client,
    /// `<solace_semp_url>/SEMP/v2/config/msgVpns/<vpn>`
    base: String,
    username: String,
    password: SecretString,
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

impl SempProvisioner {
    pub fn from_settings(s: &Settings) -> anyhow::Result<Self> {
        crate::server::install_crypto_provider(); // reqwest is built with rustls-no-provider
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            base: format!(
                "{}/SEMP/v2/config/msgVpns/{}",
                s.solace_semp_url.trim_end_matches('/'),
                enc(&s.solace_vpn)
            ),
            username: s.solace_semp_username.clone(),
            password: s.solace_semp_password.clone(),
        })
    }

    /// `(queue_created, subscription_created)`; `false` = it already existed.
    pub async fn provision(&self, queue: &str, topic: &str) -> anyhow::Result<(bool, bool)> {
        let queue_body = json!({
            "queueName": queue,
            "accessType": "exclusive",
            "permission": "consume",
            "ingressEnabled": true,
            "egressEnabled": true,
        });
        let created_queue = self.create("/queues", queue_body).await?;
        let path = format!("/queues/{}/subscriptions", enc(queue));
        let created_subscription = self
            .create(&path, json!({ "subscriptionTopic": topic }))
            .await?;
        Ok((created_queue, created_subscription))
    }

    /// The error text carries the SEMP path, the HTTP status and the broker's
    /// error object only. The password travels in the `Authorization` header,
    /// never in a URL or a message (spec §2 "Secrets").
    async fn create(&self, path: &str, body: Value) -> anyhow::Result<bool> {
        let response = self
            .http
            .request(Method::POST, format!("{}{path}", self.base))
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .json(&body)
            .send()
            .await
            .with_context(|| format!("SEMP POST {path}"))?;
        let status = response.status().as_u16();
        let reply: Value = response.json().await.unwrap_or(Value::Null);
        match (status, reply["meta"]["error"]["status"].as_str()) {
            (200, _) => Ok(true),
            (400, Some("ALREADY_EXISTS")) => Ok(false),
            _ => bail!("SEMP POST {path}: HTTP {status} {}", reply["meta"]["error"]),
        }
    }
}
