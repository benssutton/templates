//! Typed configuration. Every field can be overridden by the env var of the
//! same name in upper case (`POSTGRES_URL`, `INGEST_TRANSPORT`, ...);
//! `from_env` loads `.env` first. List fields accept a JSON array
//! (`["a","b"]`) or comma-separated values. Optional numbers accept an empty
//! string, `none` or `null` to mean "unset".
//!
//! Validation is declared on the fields (garde) and runs once, in
//! `from_vars`; `Settings::validate` is the same check for settings built
//! in code (tests), and `create_app` calls it for that reason.

use garde::Validate;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

mod parse;
mod rules;

use parse::{optional_seconds, optional_string, string_list};
use rules::{
    credentials_need_explicit_origins, header_name, http_url, needs_compression, no_store_columns,
    non_negative, not_below, positive, secret_when, transport_compiled_in,
};

/// The streaming-ingest transport. `noop` (the default) consumes nothing;
/// `/data/ingest` over HTTP still feeds the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum IngestTransport {
    #[default]
    Noop,
    /// Needs the `solace` Cargo feature.
    Solace,
}

impl IngestTransport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Solace => "solace",
            Self::Noop => "noop",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Json,
    Text,
}

#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    #[error("could not read settings: {0}")]
    Load(#[from] config::ConfigError),
    #[error("invalid settings: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(default)]
#[garde(allow_unvalidated)]
pub struct Settings {
    #[garde(length(min = 1, max = 128))]
    pub app_title: String,
    #[garde(length(min = 1, max = 64))]
    pub app_version: String,
    pub app_description: String,
    pub status: String,

    pub server_host: String,
    pub server_port: u16,
    /// Empty string (either file) => plain HTTP (local dev override).
    pub ssl_keyfile: String,
    pub ssl_certfile: String,

    /// `EnvFilter` directive, e.g. `info` or `info,rust_template=debug`.
    #[garde(length(min = 1))]
    pub log_level: String,
    pub log_format: LogFormat,
    /// OTLP/HTTP collector base URL (`http://collector:4318`). Unset = no
    /// trace export; spans still feed the logs.
    #[serde(deserialize_with = "optional_string")]
    #[garde(inner(custom(http_url)))]
    pub otel_exporter_otlp_endpoint: Option<String>,
    #[garde(length(min = 1))]
    pub otel_service_name: String,

    #[garde(length(min = 1, max = 128))]
    pub mcp_name: String,
    pub mcp_instructions: String,
    /// `Host` values the /mcp endpoint accepts, and the hosts whose
    /// `http(s)://` origins it accepts (DNS-rebinding protection). An entry
    /// without a port matches any port. Must not be empty: rmcp treats an
    /// empty list as "allow every host".
    #[serde(deserialize_with = "string_list")]
    #[garde(length(min = 1))]
    pub mcp_allowed_hosts: Vec<String>,

    // Authentication seam: off by default. When on, every route except
    // /health/live and /health/ready needs an HS256 bearer token.
    pub auth_enabled: bool,
    #[garde(custom(secret_when(self.auth_enabled)))]
    pub auth_jwt_secret: SecretString,
    #[serde(deserialize_with = "optional_string")]
    pub auth_jwt_issuer: Option<String>,
    #[serde(deserialize_with = "optional_string")]
    pub auth_jwt_audience: Option<String>,

    pub postgres_url: SecretString,
    #[garde(range(max = self.postgres_pool_max_size))]
    pub postgres_pool_min_size: u32,
    #[garde(range(min = 1))]
    pub postgres_pool_max_size: u32,
    /// How long a query waits for a pooled connection.
    #[garde(custom(positive))]
    pub postgres_acquire_timeout_seconds: f64,
    /// Server-side `statement_timeout` for every pooled connection.
    #[garde(custom(positive))]
    pub postgres_statement_timeout_seconds: f64,

    pub clickhouse_host: String,
    pub clickhouse_port: u16,
    pub clickhouse_user: String,
    pub clickhouse_password: SecretString,
    pub clickhouse_database: String,
    /// Client-side bound on every query (the HTTP client has none), also
    /// sent as `max_execution_time`.
    #[garde(custom(positive))]
    pub clickhouse_query_timeout_seconds: f64,

    pub redis_url: SecretString,
    #[garde(custom(positive))]
    pub redis_connection_timeout_seconds: f64,
    #[garde(custom(positive))]
    pub redis_response_timeout_seconds: f64,

    /// `L`: layers of the stream store; the top layer is never merged.
    #[garde(range(min = 2))]
    pub lsm_layers: usize,
    /// `P`: parts a lower layer holds before they merge into the next one.
    #[garde(range(min = 2))]
    pub lsm_parts_per_layer: usize,
    /// Cap on rows held in memory, dead rows still inside parts included.
    /// The store freezes once a message would exceed it: that message and
    /// every later valid one are refused, and reads keep serving the rows held.
    #[garde(range(min = 1))]
    pub lsm_max_rows: usize,
    /// Merge partition key: the single extension point for a composite key.
    /// Never a column the store owns (`op`, `seqno`, `_live`).
    #[serde(deserialize_with = "string_list")]
    #[garde(length(min = 1), custom(no_store_columns))]
    pub lsm_key_columns: Vec<String>,

    #[garde(custom(transport_compiled_in))]
    pub ingest_transport: IngestTransport,

    // Connection retry/backoff (every store and the Solace connect).
    #[garde(range(min = 1))]
    pub connect_max_attempts: u32,
    /// seconds
    #[garde(custom(non_negative))]
    pub connect_base_delay: f64,
    /// seconds
    #[garde(custom(non_negative))]
    pub connect_max_delay: f64,

    // Observability
    pub metrics_enabled: bool,
    /// per-dependency probe timeout, seconds
    #[garde(custom(positive))]
    pub health_check_timeout_seconds: f64,
    /// How long probe results are reused by /health and /metrics; 0 probes
    /// on every call.
    #[garde(custom(non_negative))]
    pub health_cache_ttl_seconds: f64,
    /// None = staleness never reported
    #[serde(deserialize_with = "optional_seconds")]
    #[garde(inner(custom(positive)))]
    pub ingest_staleness_threshold_seconds: Option<f64>,
    /// stale -> 503 only if true
    pub ingest_stale_fails_readiness: bool,
    /// non-CONNECTED longer than this -> shutdown; None disables
    #[serde(deserialize_with = "optional_seconds")]
    #[garde(inner(custom(positive)))]
    pub ingest_max_disconnect_seconds: Option<f64>,
    /// Consecutive transport failures before shutdown; 0 disables.
    pub ingest_max_consecutive_failures: u32,
    /// First transport retry wait, seconds; doubled per failure.
    #[garde(custom(positive))]
    pub ingest_retry_base_delay: f64,
    /// Transport retry wait cap, seconds; not below the base delay.
    #[garde(custom(positive), custom(not_below(self.ingest_retry_base_delay)))]
    pub ingest_retry_max_delay: f64,

    // Request handling
    /// A request still without a response after this is answered 503.
    #[garde(custom(positive))]
    pub request_timeout_seconds: f64,
    /// Requests in flight across the app; over it, new ones are shed with 503.
    #[garde(range(min = 1))]
    pub max_concurrent_requests: usize,
    /// `POST /data/ingest` requests in flight; over it, shed with 503 so
    /// decoded bodies cannot pile up behind the single LSM writer.
    #[garde(range(min = 1))]
    pub ingest_max_in_flight: usize,
    /// over-limit HTTP body -> 413
    #[garde(range(min = 1))]
    pub max_request_body_bytes: usize,
    /// over-limit decoded batch -> 413 over HTTP, rejected on the stream
    #[garde(range(min = 1))]
    pub max_ingest_batch_bytes: usize,

    /// Response compression: zstd preferred, gzip fallback, chosen from the
    /// request's `Accept-Encoding`. zstd and gzip request bodies are always
    /// accepted.
    pub compression_enabled: bool,
    /// Smaller responses go uncompressed: under one TCP segment, compressing
    /// saves bytes but no round trips. Ignored in strict mode.
    pub compression_min_bytes: u64,
    /// Server-side only: high levels cost far more CPU for little gain. The
    /// compression layer applies the same level to gzip, which caps it at 9.
    #[garde(range(min = 1, max = 19))]
    pub compression_zstd_level: i32,
    /// 406 for clients whose `Accept-Encoding` does not allow zstd, except on
    /// /health/live, /health/ready, /metrics and /mcp.
    #[garde(custom(needs_compression(self.compression_enabled)))]
    pub compression_require_zstd: bool,

    /// Request-id header name (inbound adoption + response echo).
    #[garde(custom(header_name))]
    pub correlation_id_header: String,

    // CORS: permissive by default for local UI dev; tighten per deployment.
    #[serde(deserialize_with = "string_list")]
    pub cors_allow_origins: Vec<String>,
    #[serde(deserialize_with = "string_list")]
    pub cors_allow_methods: Vec<String>,
    #[serde(deserialize_with = "string_list")]
    pub cors_allow_headers: Vec<String>,
    /// must stay false while origins contains "*"
    #[garde(custom(credentials_need_explicit_origins(&self.cors_allow_origins)))]
    pub cors_allow_credentials: bool,

    // Solace: only used when ingest_transport = solace.
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
    /// SEMP v2 base URL used at startup to provision `solace_queue` and its
    /// `solace_topic` subscription (idempotent).
    #[garde(custom(http_url))]
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
            server_port: 8443,
            ssl_keyfile: "./certs/key.pem".into(),
            ssl_certfile: "./certs/cert.pem".into(),
            log_level: "info".into(),
            log_format: LogFormat::Json,
            otel_exporter_otlp_endpoint: None,
            otel_service_name: "rust-template".into(),
            mcp_name: "rust-template".into(),
            mcp_instructions: "Tools for this template application.".into(),
            mcp_allowed_hosts: vec!["localhost".into()],
            auth_enabled: false,
            auth_jwt_secret: SecretString::from(""),
            auth_jwt_issuer: None,
            auth_jwt_audience: None,
            postgres_url: SecretString::from("postgresql://appuser:password@localhost:5432/appdb"),
            postgres_pool_min_size: 2,
            postgres_pool_max_size: 10,
            postgres_acquire_timeout_seconds: 5.0,
            postgres_statement_timeout_seconds: 10.0,
            clickhouse_host: "localhost".into(),
            clickhouse_port: 8123,
            clickhouse_user: "default".into(),
            clickhouse_password: SecretString::from(""),
            clickhouse_database: "default".into(),
            clickhouse_query_timeout_seconds: 10.0,
            redis_url: SecretString::from("redis://localhost:6379/0"),
            redis_connection_timeout_seconds: 2.0,
            redis_response_timeout_seconds: 5.0,
            lsm_layers: 4,
            lsm_parts_per_layer: 8,
            lsm_max_rows: 1_000_000,
            lsm_key_columns: vec!["id".into()],
            ingest_transport: IngestTransport::Noop,
            connect_max_attempts: 5,
            connect_base_delay: 1.0,
            connect_max_delay: 30.0,
            metrics_enabled: true,
            health_check_timeout_seconds: 2.0,
            health_cache_ttl_seconds: 2.0,
            ingest_staleness_threshold_seconds: None,
            ingest_stale_fails_readiness: false,
            ingest_max_disconnect_seconds: Some(60.0),
            ingest_max_consecutive_failures: 5,
            ingest_retry_base_delay: 1.0,
            ingest_retry_max_delay: 60.0,
            request_timeout_seconds: 30.0,
            max_concurrent_requests: 1024,
            ingest_max_in_flight: 4,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_ingest_batch_bytes: 16 * 1024 * 1024,
            compression_enabled: true,
            compression_min_bytes: 1024,
            compression_zstd_level: 3,
            compression_require_zstd: false,
            correlation_id_header: "X-Request-ID".into(),
            cors_allow_origins: vec!["*".into()],
            cors_allow_methods: vec!["*".into()],
            cors_allow_headers: vec!["*".into()],
            cors_allow_credentials: false,
            solace_host: "localhost".into(),
            // AMQP 1.0 plain-text listener. The message VPN is chosen by
            // this port, so `solace_vpn` is only used in SEMP paths.
            solace_port: 5672,
            solace_vpn: "default".into(),
            // Empty = SASL ANONYMOUS, the only AMQP mechanism an auth-`none`
            // VPN offers; set a username to use SASL PLAIN.
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
        // Only NAME-shaped variables: the process environment also holds names
        // such as `ProgramFiles(x86)` that are not settings and not valid keys.
        let source = vars
            .into_iter()
            .filter(|(k, _)| {
                k.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            })
            .collect();
        let settings: Settings = config::Config::builder()
            .add_source(config::Environment::default().source(Some(source)))
            .build()?
            .try_deserialize()?;
        settings.check()?;
        Ok(settings)
    }

    /// Every rule declared on the fields; all problems in one message.
    pub fn check(&self) -> Result<(), SettingsError> {
        self.validate()
            .map_err(|report| SettingsError::Invalid(report.to_string().replace('\n', "; ")))
    }
}
