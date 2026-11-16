//! Typed configuration, mirroring python-webservice-template/settings.py
//! (all fields except `flight_*`, same names, same defaults except the
//! service-identity strings).
//!
//! Every field can be overridden by the env var of the same name in upper
//! case (`POSTGRES_URL`, `INGEST_TRANSPORT`, ...). `from_env` loads `.env`
//! first. List fields accept Python's JSON form (`["a","b"]`) or
//! comma-separated values. Optional seconds accept an empty string, `none`
//! or `null` to mean "disabled".

use axum::http::HeaderName;
use secrecy::SecretString;
use serde::{Deserialize, Deserializer};

/// The streaming-ingest transport. Read by R3. In R1 the ingest seam is
/// the noop default whatever this says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IngestTransport {
    #[default]
    Solace,
    Noop,
}

impl IngestTransport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Solace => "solace",
            Self::Noop => "noop",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    #[error("invalid environment configuration: {0}")]
    Env(#[from] envy::Error),
    #[error("invalid settings: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub app_title: String,
    pub app_version: String,
    pub app_description: String,
    pub status: String,

    pub server_host: String,
    pub server_port: u16,
    /// Empty string (either file) => plain HTTP (local dev override).
    pub ssl_keyfile: String,
    pub ssl_certfile: String,

    pub mcp_name: String,
    pub mcp_instructions: String,

    pub postgres_url: SecretString,
    pub postgres_pool_min_size: u32,
    pub postgres_pool_max_size: u32,

    pub clickhouse_host: String,
    pub clickhouse_port: u16,
    pub clickhouse_user: String,
    pub clickhouse_password: SecretString,
    pub clickhouse_database: String,

    pub redis_url: SecretString,

    /// memtable -> run threshold
    pub lsm_flush_rows: usize,
    /// run count -> compaction threshold
    pub lsm_compaction_runs: usize,
    /// merge partition key; single extension point
    #[serde(deserialize_with = "string_list")]
    pub lsm_key_columns: Vec<String>,

    pub ingest_transport: IngestTransport,

    // Connection retry/backoff (every store and the Solace connect).
    pub connect_max_attempts: u32,
    /// seconds
    pub connect_base_delay: f64,
    /// seconds
    pub connect_max_delay: f64,

    // Observability
    pub metrics_enabled: bool,
    /// per-dependency probe timeout, seconds
    pub health_check_timeout_seconds: f64,
    /// None = staleness never reported
    #[serde(deserialize_with = "optional_seconds")]
    pub ingest_staleness_threshold_seconds: Option<f64>,
    /// stale -> 503 only if true
    pub ingest_stale_fails_readiness: bool,
    /// non-CONNECTED longer than this -> shutdown; None disables
    #[serde(deserialize_with = "optional_seconds")]
    pub ingest_max_disconnect_seconds: Option<f64>,

    // Inbound size limits
    /// over-limit HTTP body -> 413
    pub max_request_body_bytes: usize,
    /// over-limit decoded stream batch -> rejected/dropped + logged (R3)
    pub max_ingest_batch_bytes: usize,

    /// Correlation ID header name (inbound adoption + response echo).
    pub correlation_id_header: String,

    // CORS: permissive by default for local UI dev; tighten per deployment.
    #[serde(deserialize_with = "string_list")]
    pub cors_allow_origins: Vec<String>,
    #[serde(deserialize_with = "string_list")]
    pub cors_allow_methods: Vec<String>,
    #[serde(deserialize_with = "string_list")]
    pub cors_allow_headers: Vec<String>,
    /// must stay false while origins contains "*"
    pub cors_allow_credentials: bool,

    // Solace: only used when ingest_transport = solace (R3).
    pub solace_host: String,
    pub solace_port: u16,
    pub solace_vpn: String,
    /// Empty selects SASL ANONYMOUS; otherwise SASL PLAIN with
    /// `solace_username` / `solace_password`.
    pub solace_username: String,
    pub solace_password: SecretString,
    pub solace_topic: String,
    /// Durable exclusive queue this instance binds to. ONE QUEUE PER REPLICA:
    /// the LSM store is a per-instance cache, and an exclusive queue delivers
    /// to one consumer only.
    pub solace_queue: String,
    /// Variant (b): SEMP v2 base URL used at startup to provision
    /// `solace_queue` and its `solace_topic` subscription (idempotent).
    pub solace_semp_url: String,
    pub solace_semp_username: String,
    pub solace_semp_password: SecretString,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            app_title: "Rust Template Service".into(),
            app_version: "1.0.0".into(),
            app_description: "A Rust axum service with REST and MCP endpoints, ready for Claude"
                .into(),
            status: "running".into(),
            server_host: "0.0.0.0".into(),
            server_port: 443,
            ssl_keyfile: "./certs/key.pem".into(),
            ssl_certfile: "./certs/cert.pem".into(),
            mcp_name: "rust-template".into(),
            mcp_instructions: "Tools for this template application.".into(),
            postgres_url: SecretString::from("postgresql://appuser:password@localhost:5432/appdb"),
            postgres_pool_min_size: 2,
            postgres_pool_max_size: 10,
            clickhouse_host: "localhost".into(),
            clickhouse_port: 8123,
            clickhouse_user: "default".into(),
            clickhouse_password: SecretString::from(""),
            clickhouse_database: "default".into(),
            redis_url: SecretString::from("redis://localhost:6379/0"),
            lsm_flush_rows: 1000,
            lsm_compaction_runs: 4,
            lsm_key_columns: vec!["id".into()],
            ingest_transport: IngestTransport::Solace,
            connect_max_attempts: 5,
            connect_base_delay: 1.0,
            connect_max_delay: 30.0,
            metrics_enabled: true,
            health_check_timeout_seconds: 2.0,
            ingest_staleness_threshold_seconds: None,
            ingest_stale_fails_readiness: false,
            ingest_max_disconnect_seconds: Some(60.0),
            max_request_body_bytes: 16 * 1024 * 1024,
            max_ingest_batch_bytes: 16 * 1024 * 1024,
            correlation_id_header: "X-Request-ID".into(),
            cors_allow_origins: vec!["*".into()],
            cors_allow_methods: vec!["*".into()],
            cors_allow_headers: vec!["*".into()],
            cors_allow_credentials: false,
            solace_host: "localhost".into(),
            // AMQP 1.0 plain-text listener (Deviation 3). The VPN is chosen by
            // this port, so `solace_vpn` is only used in SEMP paths.
            solace_port: 5672,
            solace_vpn: "default".into(),
            // Empty = SASL ANONYMOUS, the only AMQP mechanism an auth-`none`
            // VPN offers; set a username to use SASL PLAIN. Divergence:
            // Python defaults to admin/admin over SMF.
            solace_username: String::new(),
            solace_password: SecretString::from(""),
            solace_topic: "ingest/batches".into(),
            solace_queue: "rust-template/ingest".into(),
            solace_semp_url: "http://localhost:8080".into(),
            solace_semp_username: "admin".into(),
            solace_semp_password: SecretString::from("admin"),
        }
    }
}

impl Settings {
    /// Load `.env` (if present), then the process environment, then validate.
    pub fn from_env() -> Result<Self, SettingsError> {
        let _ = dotenvy::dotenv(); // a missing .env is normal
        Self::from_vars(std::env::vars())
    }

    /// Build from explicit `(NAME, value)` pairs, then validate. Tests use
    /// this instead of mutating the process environment.
    pub fn from_vars<I>(vars: I) -> Result<Self, SettingsError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let settings: Settings = envy::from_iter(vars)?;
        settings.validate()?;
        Ok(settings)
    }

    /// Cross-field and numeric-bound checks (Python's `@model_validator`
    /// and `Field(ge=...)`). Collects every problem into one message.
    pub fn validate(&self) -> Result<(), SettingsError> {
        let mut problems: Vec<String> = Vec::new();
        if self.cors_allow_credentials && self.cors_allow_origins.iter().any(|o| o == "*") {
            problems.push(
                "cors_allow_credentials=true is incompatible with cors_allow_origins=[\"*\"]; \
                 specify explicit origins when enabling credentials"
                    .into(),
            );
        }
        if self.connect_max_attempts < 1 {
            problems.push("connect_max_attempts must be >= 1".into());
        }
        for (name, value) in [
            ("connect_base_delay", self.connect_base_delay),
            ("connect_max_delay", self.connect_max_delay),
        ] {
            if !value.is_finite() || value < 0.0 {
                problems.push(format!("{name} must be a finite number >= 0"));
            }
        }
        if !self.health_check_timeout_seconds.is_finite()
            || self.health_check_timeout_seconds <= 0.0
        {
            problems.push("health_check_timeout_seconds must be > 0".into());
        }
        for (name, value) in [
            (
                "ingest_staleness_threshold_seconds",
                self.ingest_staleness_threshold_seconds,
            ),
            (
                "ingest_max_disconnect_seconds",
                self.ingest_max_disconnect_seconds,
            ),
        ] {
            if let Some(v) = value {
                if !v.is_finite() || v <= 0.0 {
                    problems.push(format!("{name} must be > 0 when set"));
                }
            }
        }
        if self.postgres_pool_max_size < 1 {
            problems.push("postgres_pool_max_size must be >= 1".into());
        }
        if self.postgres_pool_min_size > self.postgres_pool_max_size {
            problems.push("postgres_pool_min_size must be <= postgres_pool_max_size".into());
        }
        if self.lsm_flush_rows < 1 {
            problems.push("lsm_flush_rows must be >= 1".into());
        }
        if self.lsm_compaction_runs < 1 {
            problems.push("lsm_compaction_runs must be >= 1".into());
        }
        if self.lsm_key_columns.is_empty() {
            problems.push("lsm_key_columns must name at least one column".into());
        }
        if self.max_request_body_bytes < 1 {
            problems.push("max_request_body_bytes must be >= 1".into());
        }
        if self.max_ingest_batch_bytes < 1 {
            problems.push("max_ingest_batch_bytes must be >= 1".into());
        }
        if HeaderName::from_bytes(self.correlation_id_header.as_bytes()).is_err() {
            problems.push("correlation_id_header must be a valid HTTP header name".into());
        }
        if !(self.solace_semp_url.starts_with("http://")
            || self.solace_semp_url.starts_with("https://"))
        {
            problems.push("solace_semp_url must start with http:// or https://".into());
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(SettingsError::Invalid(problems.join("; ")))
        }
    }
}

/// `["a","b"]` (Python / pydantic-settings) or `a,b` (envy native).
fn string_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    let raw = String::deserialize(d)?;
    let trimmed = raw.trim();
    if trimmed.starts_with('[') {
        return serde_json::from_str(trimmed)
            .map_err(|e| serde::de::Error::custom(format!("invalid list value {raw:?}: {e}")));
    }
    Ok(trimmed
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect())
}

/// Seconds, or "" / "none" / "null" (any case) for disabled.
fn optional_seconds<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    let raw = String::deserialize(d)?;
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("none")
        || trimmed.eq_ignore_ascii_case("null")
    {
        return Ok(None);
    }
    trimmed
        .parse::<f64>()
        .map(Some)
        .map_err(|e| serde::de::Error::custom(format!("invalid seconds value {raw:?}: {e}")))
}
