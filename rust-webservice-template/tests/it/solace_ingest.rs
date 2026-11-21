//! End to end: Solace Guaranteed message -> settle-after-write -> /data/cache.

use crate::support::TestApp;
use crate::support::batches::{batch_without_id, make_batch};
use crate::support::containers;
use crate::support::containers::wait_until;
use crate::support::gated_store::GatedStore;
use crate::support::logs;
use crate::support::semp::Semp;
use crate::support::solace::{
    SolaceNames, consumer_settings, poll_cache, publisher, solace_settings, values, wait_for_bound,
    wait_for_depth, wait_for_unbound,
};
use rust_template::core::shutdown::Shutdown;
use rust_template::ingestion;
use rust_template::persistence::stream_store::StreamStore;
use rust_template::persistence::stream_store::lsm::LsmStore;
use rust_template::services::stream_ingest::StreamIngestService;
use rust_template::settings::Settings;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_TIMEOUT: Duration = Duration::from_secs(30);

/// lsm_flush_rows=2, lsm_compaction_runs=2 (solace_settings):
///   BATCH_1: 2 rows -> flush -> run1
///   BATCH_2: 2 rows -> flush -> run2 -> compaction: id 1 gets v2
///   BATCH_3: 1 row  -> a memtable tombstone beats id 2 in the compacted run
/// Published ONCE each: the durable queue guarantees delivery.
#[tokio::test]
async fn three_batches_merge_newest_wins_with_a_tombstone() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let app = TestApp::spawn(solace_settings(&lease.endpoints, &names).await).await;
    wait_for_bound(&semp, &names.queue).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    for rows in [
        &[(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")][..],
        &[(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")][..],
        &[(2, "beta", "v1", "delete")][..],
    ] {
        p.publish_batch(&make_batch(rows)).await.unwrap();
    }
    // total == 2 also holds after BATCH_1 alone (2 -> 3 -> 2): wait for the END state.
    let body = poll_cache(
        &app,
        |b| {
            let v = values(b);
            b["total"] == 2 && !v.contains_key(&2) && v.get(&1).map(String::as_str) == Some("v2")
        },
        CACHE_TIMEOUT,
    )
    .await;
    let v = values(&body);
    assert_eq!(
        v.get(&1).map(String::as_str),
        Some("v2"),
        "newest wins across compaction"
    );
    assert!(
        !v.contains_key(&2),
        "the memtable tombstone beats the compacted run"
    );
    assert_eq!(
        v.get(&3).map(String::as_str),
        Some("v1"),
        "an unmodified row survives"
    );
    p.close().await;
}

/// Spec §6 message_acked_only_after_lsm_write, made deterministic.
///
/// SEMP's spooled depth lags an accept by about 5 s, so racing the cache
/// against depth can neither fail reliably on an ack-before-write bug nor
/// pass reliably on correct code. Instead the real Solace consumer feeds a
/// `StreamIngestService` whose LSM store holds every write at a gate:
/// 1. with the write parked at the gate, the broker reports the message as
///    delivered-but-unacked (`unacked == 1`) and still spooled, and keeps
///    doing so for 6 s (longer than the SEMP lag): nothing acked it early;
/// 2. once the gate opens the write commits, and only then does the queue
///    drain (spooled 0, unacked 0).
#[tokio::test]
async fn message_acked_only_after_lsm_write() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let settings = Arc::new(consumer_settings(&lease.endpoints, &names));
    let consumer = ingestion::connect(&settings)
        .await
        .expect("Solace consumer");
    let store = Arc::new(GatedStore::new(LsmStore::new(100, 4, vec!["id".into()])));
    let svc = StreamIngestService::start(settings, consumer, store.clone(), Shutdown::new(), None);
    wait_for_bound(&semp, &names.queue).await;

    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[
        (1, "alpha", "v1", "upsert"),
        (2, "beta", "v1", "upsert"),
    ]))
    .await
    .unwrap();
    tokio::time::timeout(CACHE_TIMEOUT, store.entered.notified())
        .await
        .expect("the write reached the store");

    wait_until(
        "message delivered, unacked",
        Duration::from_secs(15),
        || {
            let (semp, queue) = (semp.clone(), names.queue.clone());
            async move { semp.queue_unacked_count(&queue).await.is_ok_and(|n| n == 1) }
        },
    )
    .await;
    assert_eq!(store.query(0).unwrap().total, 0, "the write is still gated");

    // The "not acked before the write" proof: hold for longer than the SEMP
    // lag. A transient SEMP error skips that sample (one sample is 4 GETs),
    // but good samples must be at most 3 s apart and at least 5 overall.
    let max_gap = Duration::from_secs(3);
    let min_good = 5;
    let hold = Instant::now() + Duration::from_secs(6);
    let mut last_good = Instant::now();
    let mut good = 0;
    loop {
        let sample = semp.queue_stats(&names.queue).await;
        if let Ok(stats) = &sample {
            assert_eq!(
                (stats.unacked, stats.spooled),
                (1, 1),
                "settled before the LSM write committed: {stats:?}"
            );
            last_good = Instant::now();
            good += 1;
        }
        assert!(
            last_good.elapsed() <= max_gap,
            "no good SEMP sample for over {max_gap:?}; last: {sample:?}"
        );
        if Instant::now() >= hold {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        good >= min_good,
        "only {good} good SEMP samples during the hold (need {min_good})"
    );

    store.gate.add_permits(1);
    wait_until(
        "message acked after the write",
        Duration::from_secs(30),
        || {
            let (semp, queue) = (semp.clone(), names.queue.clone());
            async move {
                semp.queue_stats(&queue)
                    .await
                    .is_ok_and(|s| s.spooled == 0 && s.unacked == 0)
            }
        },
    )
    .await;
    assert_eq!(
        store.query(0).unwrap().total,
        2,
        "the batch's rows were written"
    );
    svc.close().await;
    p.close().await;
}

#[tokio::test]
async fn messages_published_while_the_app_is_down_are_delivered_on_start() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let settings = solace_settings(&lease.endpoints, &names).await;
    let first = TestApp::spawn(settings.clone()).await;
    wait_for_bound(&semp, &names.queue).await; // the first start provisions/binds the durable queue
    first.stop().await.expect("clean shutdown");
    wait_for_unbound(&semp, &names.queue).await; // nothing can consume it before the restart

    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[
        (1, "alpha", "v1", "upsert"),
        (2, "beta", "v1", "upsert"),
    ]))
    .await
    .unwrap();
    wait_for_depth(&semp, &names.queue, 1).await; // held by the durable queue

    let second = TestApp::spawn(settings).await;
    poll_cache(&second, |b| b["total"] == 2, CACHE_TIMEOUT).await;
    p.close().await;
}

#[tokio::test]
async fn an_unmergeable_message_is_rejected() {
    let (logs, _guard) = logs::capture();
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let app = TestApp::spawn(solace_settings(&lease.endpoints, &names).await).await;
    wait_for_bound(&semp, &names.queue).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&batch_without_id()).await.unwrap();
    wait_for_depth(&semp, &names.queue, 0).await;
    assert_settled_without_redelivery(&semp, &names.queue).await;
    assert_eq!(app.get_json("/data/cache?limit=100").await.1["total"], 0);
    // Python asserts at caplog level ERROR.
    assert!(
        logs.line_with(&["ERROR", "rejecting message", "missing required columns"])
            .is_some(),
        "{}",
        logs.text()
    );
    p.close().await;
}

/// Spec §6 oversized_batch_is_rejected.
#[tokio::test]
async fn an_oversized_message_is_rejected() {
    let (logs, _guard) = logs::capture();
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let settings = Settings {
        max_ingest_batch_bytes: 1,
        ..solace_settings(&lease.endpoints, &names).await
    };
    let app = TestApp::spawn(settings).await;
    wait_for_bound(&semp, &names.queue).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[(1, "alpha", "v1", "upsert")]))
        .await
        .unwrap();
    wait_for_depth(&semp, &names.queue, 0).await;
    assert_settled_without_redelivery(&semp, &names.queue).await;
    assert_eq!(app.get_json("/data/cache?limit=100").await.1["total"], 0);
    assert!(
        logs.line_with(&["ERROR", "exceeds max_ingest_batch_bytes"])
            .is_some(),
        "{}",
        logs.text()
    );
    p.close().await;
}

/// Readiness, detailed status and metrics with the real transport
/// (Python test_observability: readiness_all_up, status_structure_and_ingest_freshness).
#[tokio::test]
async fn health_and_metrics_report_the_solace_transport() {
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let app = TestApp::spawn(solace_settings(&lease.endpoints, &names).await).await;
    wait_for_bound(&semp, &names.queue).await;
    let (status, ready) = app.get_json("/health/ready").await;
    assert_eq!(status, 200, "{ready}");
    assert_eq!(ready["status"], "ready", "{ready}");
    let checks = ready["checks"].as_array().unwrap();
    for dep in ["postgres", "clickhouse", "redis", "ingest"] {
        let check = checks
            .iter()
            .find(|c| c["name"] == dep)
            .unwrap_or_else(|| panic!("no {dep} check: {ready}"));
        assert_eq!(check["status"], "up", "{ready}");
    }
    let ingest = checks.iter().find(|c| c["name"] == "ingest").unwrap();
    assert_eq!(
        (
            ingest["transport"].as_str(),
            ingest["connection_state"].as_str()
        ),
        (Some("solace"), Some("connected"))
    );

    let mut p = publisher(&lease.endpoints, &names.topic).await;
    p.publish_batch(&make_batch(&[
        (1, "alpha", "v1", "upsert"),
        (2, "beta", "v1", "upsert"),
    ]))
    .await
    .unwrap();
    poll_cache(&app, |b| b["total"] == 2, CACHE_TIMEOUT).await;
    // Settle-after-write: the queue drains only once the write and its
    // bookkeeping are done, so the counters below are already updated.
    wait_for_depth(&semp, &names.queue, 0).await;

    let (_, status) = app.get_json("/health/status").await;
    assert_eq!(status["app"]["status"], "testing", "{status}");
    assert!(
        status["uptime"]["process_seconds"]
            .as_f64()
            .is_some_and(|s| s >= 0.0),
        "{status}"
    );
    assert!(
        status["system"]["process"]["memory_rss_bytes"]
            .as_u64()
            .is_some_and(|b| b > 0),
        "{status}"
    );
    assert_eq!(status["ingest"]["transport"], "solace", "{status}");
    assert_eq!(status["ingest"]["rows_ingested_total"], 2, "{status}");
    assert!(status["ingest"]["last_batch_at"].is_string(), "{status}");
    let text = app.get("/metrics").await.text().await.unwrap();
    assert!(
        text.contains(r#"ingest_outcomes_total{outcome="accepted"} 1"#),
        "{text}"
    );
    assert!(text.contains("ingest_rows_ingested_total 2"), "{text}");
    let connected = text
        .lines()
        .find(|l| {
            l.starts_with("ingest_connection_state{")
                && l.contains(r#"state="connected""#)
                && l.contains(r#"transport="solace""#)
        })
        .unwrap_or_else(|| panic!("{text}"));
    assert!(connected.ends_with(" 1"), "{connected}");
    p.close().await;
}

/// Python test_observability::test_streaming_assigns_distinct_per_message_ids:
/// every delivery gets its own correlation ID, carried onto its
/// "ingested batch" log line. `logs::capture()` is thread-local; a plain
/// `#[tokio::test]` runs the ingest loop and the write task it spawns on
/// this thread's runtime, so the guard sees them.
#[tokio::test]
async fn streaming_assigns_distinct_per_message_ids() {
    let (logs, _guard) = logs::capture();
    let lease = containers::solace().await;
    let semp = Semp::new(&lease.endpoints.semp_url());
    let names = SolaceNames::new();
    let app = TestApp::spawn(solace_settings(&lease.endpoints, &names).await).await;
    wait_for_bound(&semp, &names.queue).await;
    let mut p = publisher(&lease.endpoints, &names.topic).await;
    for rows in [
        &[(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")][..],
        &[(3, "c", "v1", "upsert")][..],
        &[(4, "d", "v1", "upsert")][..],
    ] {
        p.publish_batch(&make_batch(rows)).await.unwrap();
    }
    poll_cache(&app, |b| b["total"] == 4, CACHE_TIMEOUT).await;
    // The cache shows the last write a moment before its log line: wait for all three.
    wait_until(
        "three ingested-batch lines",
        Duration::from_secs(10),
        || {
            let n = ingest_lines(&logs.text()).len();
            async move { n >= 3 }
        },
    )
    .await;

    let text = logs.text();
    let lines = ingest_lines(&text);
    assert_eq!(lines.len(), 3, "{text}");
    let cids: HashSet<&str> = lines
        .iter()
        .map(|l| request_id(l).unwrap_or_else(|| panic!("no request_id: {l}")))
        .collect();
    assert!(
        cids.iter().all(|c| !c.is_empty() && *c != "-"),
        "every message got a real ID: {cids:?}"
    );
    assert_eq!(cids.len(), 3, "a DISTINCT ID per message: {text}");
    p.close().await;
}

/// A rejected message is gone for good: spooled 0, unacked 0 and never
/// redelivered, held continuously for 6 s (SEMP lags about 5 s) within 15 s.
async fn assert_settled_without_redelivery(semp: &Semp, queue: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let hold = Duration::from_secs(6);
    let mut stable_since: Option<Instant> = None;
    loop {
        let stats = semp.queue_stats(queue).await;
        let settled = matches!(
            stats,
            Ok(s) if s.spooled == 0 && s.unacked == 0 && s.redelivered == 0
        );
        if settled {
            let since = *stable_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= hold {
                return;
            }
        } else {
            stable_since = None;
        }
        assert!(
            Instant::now() < deadline,
            "queue {queue} not stably settled without redelivery: {stats:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn ingest_lines(text: &str) -> Vec<&str> {
    text.lines()
        .filter(|l| l.contains("ingested batch"))
        .collect()
}

/// The `request_id=<id>` span field of a captured line.
fn request_id(line: &str) -> Option<&str> {
    let rest = line.split("request_id=").nth(1)?;
    let end = rest
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '-')
        .unwrap_or(rest.len());
    Some(&rest[..end])
}
