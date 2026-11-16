use rust_template::settings::{IngestTransport, Settings};
use secrecy::{ExposeSecret, SecretString};

fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn defaults_match_python_except_service_identity() {
    let s = Settings::default();
    assert_eq!(s.app_title, "Rust Template Service");
    assert_eq!(s.status, "running");
    assert_eq!(s.server_host, "0.0.0.0");
    assert_eq!(s.server_port, 443);
    assert_eq!(s.ssl_keyfile, "./certs/key.pem");
    assert_eq!(s.ssl_certfile, "./certs/cert.pem");
    assert_eq!(s.mcp_name, "rust-template");
    assert_eq!(
        s.postgres_url.expose_secret(),
        "postgresql://appuser:password@localhost:5432/appdb"
    );
    assert_eq!(
        (s.postgres_pool_min_size, s.postgres_pool_max_size),
        (2, 10)
    );
    assert_eq!(
        (s.clickhouse_host.as_str(), s.clickhouse_port),
        ("localhost", 8123)
    );
    assert_eq!(s.redis_url.expose_secret(), "redis://localhost:6379/0");
    assert_eq!((s.lsm_flush_rows, s.lsm_compaction_runs), (1000, 4));
    assert_eq!(s.lsm_key_columns, vec!["id".to_string()]);
    assert_eq!(s.ingest_transport, IngestTransport::Solace);
    assert_eq!(s.connect_max_attempts, 5);
    assert_eq!((s.connect_base_delay, s.connect_max_delay), (1.0, 30.0));
    assert!(s.metrics_enabled);
    assert_eq!(s.health_check_timeout_seconds, 2.0);
    assert_eq!(s.ingest_staleness_threshold_seconds, None);
    assert!(!s.ingest_stale_fails_readiness);
    assert_eq!(s.ingest_max_disconnect_seconds, Some(60.0));
    assert_eq!(s.max_request_body_bytes, 16 * 1024 * 1024);
    assert_eq!(s.max_ingest_batch_bytes, 16 * 1024 * 1024);
    assert_eq!(s.correlation_id_header, "X-Request-ID");
    assert_eq!(s.cors_allow_origins, vec!["*".to_string()]);
    assert!(!s.cors_allow_credentials);
    // Deviation 3: AMQP 1.0 plain text, not SMF (55555).
    assert_eq!((s.solace_host.as_str(), s.solace_port), ("localhost", 5672));
    // SASL rule (Task 0): an empty username selects SASL ANONYMOUS, the only
    // mechanism an auth-`none` VPN offers over AMQP. Python defaults to
    // admin/admin, which SMF accepts on such a VPN but AMQP does not.
    assert_eq!(s.solace_username, "");
    assert_eq!(s.solace_password.expose_secret(), "");
    assert_eq!(s.solace_topic, "ingest/batches");
    assert_eq!(s.solace_queue, "rust-template/ingest");
    // Variant (b): SEMP provisioning at startup (not in Python).
    assert_eq!(s.solace_semp_url, "http://localhost:8080");
    assert_eq!(s.solace_semp_username, "admin");
    assert_eq!(s.solace_semp_password.expose_secret(), "admin");
    assert!(s.validate().is_ok());
}

#[test]
fn env_vars_override_defaults_with_python_names() {
    let s = Settings::from_vars(vars(&[
        ("STATUS", "testing"),
        ("SERVER_PORT", "8443"),
        ("POSTGRES_URL", "postgresql://u:p@db:5432/x"),
        ("INGEST_TRANSPORT", "noop"),
        ("CONNECT_MAX_ATTEMPTS", "2"),
        ("CONNECT_BASE_DELAY", "0.01"),
        ("METRICS_ENABLED", "false"),
        ("SOLACE_QUEUE", "rust-template/ingest-2"),
    ]))
    .expect("valid settings");
    assert_eq!(s.status, "testing");
    assert_eq!(s.server_port, 8443);
    assert_eq!(s.postgres_url.expose_secret(), "postgresql://u:p@db:5432/x");
    assert_eq!(s.ingest_transport, IngestTransport::Noop);
    assert_eq!(s.connect_max_attempts, 2);
    assert_eq!(s.connect_base_delay, 0.01);
    assert!(!s.metrics_enabled);
    assert_eq!(s.solace_queue, "rust-template/ingest-2");
}

#[test]
fn list_settings_accept_python_json_and_comma_separated() {
    let json = Settings::from_vars(vars(&[(
        "CORS_ALLOW_ORIGINS",
        r#"["https://a.example.com", "https://b.example.com"]"#,
    )]))
    .unwrap();
    assert_eq!(
        json.cors_allow_origins,
        vec!["https://a.example.com", "https://b.example.com"]
    );
    let csv = Settings::from_vars(vars(&[("LSM_KEY_COLUMNS", "id, region")])).unwrap();
    assert_eq!(csv.lsm_key_columns, vec!["id", "region"]);
}

#[test]
fn optional_seconds_accept_empty_or_none_as_disabled() {
    for disabled in ["", "none", "None", "null"] {
        let s = Settings::from_vars(vars(&[("INGEST_MAX_DISCONNECT_SECONDS", disabled)])).unwrap();
        assert_eq!(s.ingest_max_disconnect_seconds, None, "input {disabled:?}");
    }
    let s = Settings::from_vars(vars(&[("INGEST_STALENESS_THRESHOLD_SECONDS", "0.5")])).unwrap();
    assert_eq!(s.ingest_staleness_threshold_seconds, Some(0.5));
}

#[test]
fn cors_credentials_with_wildcard_origin_is_rejected() {
    let err = Settings::from_vars(vars(&[("CORS_ALLOW_CREDENTIALS", "true")])).unwrap_err();
    assert!(err.to_string().contains("incompatible"), "{err}");
    let ok = Settings::from_vars(vars(&[
        ("CORS_ALLOW_CREDENTIALS", "true"),
        ("CORS_ALLOW_ORIGINS", "https://app.example.com"),
    ]));
    assert!(ok.is_ok());
}

#[test]
fn numeric_bounds_are_enforced() {
    for (key, value, needle) in [
        ("CONNECT_MAX_ATTEMPTS", "0", "connect_max_attempts"),
        ("CONNECT_BASE_DELAY", "-1", "connect_base_delay"),
        ("CONNECT_MAX_DELAY", "-0.5", "connect_max_delay"),
        (
            "HEALTH_CHECK_TIMEOUT_SECONDS",
            "0",
            "health_check_timeout_seconds",
        ),
        ("POSTGRES_POOL_MIN_SIZE", "11", "postgres_pool_min_size"),
        ("LSM_FLUSH_ROWS", "0", "lsm_flush_rows"),
        ("MAX_REQUEST_BODY_BYTES", "0", "max_request_body_bytes"),
        (
            "CORRELATION_ID_HEADER",
            "bad header",
            "correlation_id_header",
        ),
        ("SOLACE_SEMP_URL", "solace:8080", "solace_semp_url"),
    ] {
        let err = Settings::from_vars(vars(&[(key, value)])).unwrap_err();
        assert!(err.to_string().contains(needle), "{key}={value}: {err}");
    }
}

#[test]
fn unparseable_value_is_a_readable_error() {
    let err = Settings::from_vars(vars(&[("SERVER_PORT", "not-a-port")])).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("server_port"),
        "{err}"
    );
}

#[test]
fn secrets_are_redacted_in_debug_output() {
    let s = Settings {
        postgres_url: SecretString::from("postgresql://u:pg-sentinel-42@db/x"),
        clickhouse_password: SecretString::from("ch-sentinel-42"),
        redis_url: SecretString::from("redis://:redis-sentinel-42@r:6379/0"),
        solace_password: SecretString::from("solace-sentinel-42"),
        solace_semp_password: SecretString::from("semp-sentinel-42"),
        ..Settings::default()
    };
    let debug = format!("{s:?}");
    for sentinel in [
        "pg-sentinel-42",
        "ch-sentinel-42",
        "redis-sentinel-42",
        "solace-sentinel-42",
        "semp-sentinel-42",
    ] {
        assert!(
            !debug.contains(sentinel),
            "{sentinel} leaked in Debug: {debug}"
        );
    }
    assert!(debug.contains("REDACTED"));
}

const SECRET_VARS: [(&str, &str); 5] = [
    ("POSTGRES_URL", "postgresql://u:pg-sentinel-42@db/x"),
    ("CLICKHOUSE_PASSWORD", "ch-sentinel-42"),
    ("REDIS_URL", "redis://:redis-sentinel-42@r:6379/0"),
    ("SOLACE_PASSWORD", "solace-sentinel-42"),
    ("SOLACE_SEMP_PASSWORD", "semp-sentinel-42"),
];
const SENTINELS: [&str; 5] = [
    "pg-sentinel-42",
    "ch-sentinel-42",
    "redis-sentinel-42",
    "solace-sentinel-42",
    "semp-sentinel-42",
];

fn assert_no_sentinel(err: &impl std::fmt::Debug, display: &str) {
    let debug = format!("{err:?}");
    for s in SENTINELS {
        assert!(!display.contains(s), "{s} leaked in Display: {display}");
        assert!(!debug.contains(s), "{s} leaked in Debug: {debug}");
    }
}

#[test]
fn secrets_stay_out_of_errors() {
    // Parse failure (envy path).
    let mut pairs: Vec<(&str, &str)> = SECRET_VARS.to_vec();
    pairs.push(("SERVER_PORT", "not-a-port"));
    let err = Settings::from_vars(vars(&pairs)).unwrap_err();
    assert_no_sentinel(&err, &err.to_string());

    // Validation failure via from_vars.
    let mut pairs: Vec<(&str, &str)> = SECRET_VARS.to_vec();
    pairs.push(("CONNECT_MAX_ATTEMPTS", "0"));
    let err = Settings::from_vars(vars(&pairs)).unwrap_err();
    assert!(err.to_string().contains("connect_max_attempts"));
    assert_no_sentinel(&err, &err.to_string());

    // Direct validate().
    let s = Settings {
        postgres_url: SecretString::from(SECRET_VARS[0].1),
        clickhouse_password: SecretString::from(SECRET_VARS[1].1),
        redis_url: SecretString::from(SECRET_VARS[2].1),
        solace_password: SecretString::from(SECRET_VARS[3].1),
        solace_semp_password: SecretString::from(SECRET_VARS[4].1),
        connect_max_attempts: 0,
        ..Settings::default()
    };
    let err = s.validate().unwrap_err();
    assert_no_sentinel(&err, &err.to_string());
}

#[test]
fn remaining_bounds_are_enforced() {
    let mut cases: Vec<(&str, &str, &str)> = vec![
        ("POSTGRES_POOL_MAX_SIZE", "0", "postgres_pool_max_size"),
        ("LSM_COMPACTION_RUNS", "0", "lsm_compaction_runs"),
        ("MAX_INGEST_BATCH_BYTES", "0", "max_ingest_batch_bytes"),
        ("LSM_KEY_COLUMNS", "", "lsm_key_columns"),
    ];
    for key in [
        "INGEST_STALENESS_THRESHOLD_SECONDS",
        "INGEST_MAX_DISCONNECT_SECONDS",
    ] {
        for value in ["0", "-1", "nan"] {
            cases.push((key, value, key));
        }
    }
    for (key, value, needle) in cases {
        let err = Settings::from_vars(vars(&[(key, value)])).unwrap_err();
        assert!(
            err.to_string().contains(&needle.to_lowercase()),
            "{key}={value}: {err}"
        );
    }
}

#[test]
fn multiple_problems_are_all_reported() {
    let err = Settings::from_vars(vars(&[
        ("CONNECT_MAX_ATTEMPTS", "0"),
        ("LSM_FLUSH_ROWS", "0"),
    ]))
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("connect_max_attempts"), "{msg}");
    assert!(msg.contains("lsm_flush_rows"), "{msg}");
}

#[test]
fn bad_enum_is_a_readable_error() {
    let err = Settings::from_vars(vars(&[("INGEST_TRANSPORT", "kafka")])).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("ingest_transport") || msg.contains("kafka"),
        "{msg}"
    );
}

#[test]
fn bad_optional_seconds_shows_the_value() {
    let err = Settings::from_vars(vars(&[("INGEST_MAX_DISCONNECT_SECONDS", "abc")])).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("abc"), "{msg}");
}

#[test]
fn malformed_json_list_shows_the_value() {
    let err = Settings::from_vars(vars(&[("CORS_ALLOW_ORIGINS", "[a")])).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("[a"), "{msg}");
}
