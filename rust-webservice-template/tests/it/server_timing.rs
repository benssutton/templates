use crate::support::{app_settings, TestApp};

/// /health/live has no instrumented boundaries, so only `total` appears.
/// (Boundary samples like clickhouse_select arrive with R2's stores.)
#[tokio::test]
async fn request_without_boundaries_has_only_total() {
    let app = TestApp::spawn(app_settings().await).await;
    let r = app.get("/health/live").await;
    assert_eq!(r.status(), 200);
    let header = r.headers()["server-timing"].to_str().unwrap().to_owned();
    assert!(header.starts_with("total;dur="), "{header}");
    for token in ["clickhouse", "lsm", "ingest", "postgres"] {
        assert!(!header.contains(token), "{header}");
    }
    let dur: f64 = header.trim_start_matches("total;dur=").parse().unwrap();
    assert!(dur >= 0.0);
}

/// Matches the k6 parser (tests/performance/lib/serverTiming.js).
#[tokio::test]
async fn header_is_parseable_by_the_k6_grammar() {
    let app = TestApp::spawn(app_settings().await).await;
    let header = app.get("/health/status").await.headers()["server-timing"]
        .to_str()
        .unwrap()
        .to_owned();
    for part in header.split(", ") {
        let (name, dur) = part.split_once(";dur=").expect("name;dur=value");
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "{part}"
        );
        let (int, frac) = dur.split_once('.').unwrap_or((dur, "0"));
        assert!(
            !int.is_empty()
                && !frac.is_empty()
                && int.chars().chain(frac.chars()).all(|c| c.is_ascii_digit()),
            "dur must be digits with an optional .digits fraction: {part}"
        );
    }
}
