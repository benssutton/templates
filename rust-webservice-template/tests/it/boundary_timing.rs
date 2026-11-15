use rust_template::core::boundary_timing::{collect, timed};

#[tokio::test]
async fn timed_records_a_sample_inside_a_scope() {
    let (value, samples) = collect(async { timed("clickhouse.select", async { 7 }).await }).await;
    assert_eq!(value, 7);
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].0, "clickhouse.select");
    assert!(samples[0].1 >= 0.0);
}

#[tokio::test]
async fn timed_is_silent_outside_any_scope() {
    // Must not panic and must return the inner value.
    assert_eq!(timed("clickhouse.select", async { 1 }).await, 1);
}

#[tokio::test]
async fn spawned_background_work_does_not_leak_into_the_scope() {
    let ((), samples) = collect(async {
        tokio::spawn(timed("ingest.lsm_write", async {}))
            .await
            .unwrap();
    })
    .await;
    assert!(samples.is_empty(), "background sample leaked: {samples:?}");
}

#[tokio::test]
async fn concurrent_scopes_are_isolated() {
    let a = collect(async { timed("a", async {}).await });
    let b = collect(async {
        timed("b", async {}).await;
        timed("b", async {}).await;
    });
    let (((), sa), ((), sb)) = tokio::join!(a, b);
    assert_eq!(sa.iter().map(|s| s.0.as_str()).collect::<Vec<_>>(), ["a"]);
    assert_eq!(
        sb.iter().map(|s| s.0.as_str()).collect::<Vec<_>>(),
        ["b", "b"]
    );
}

#[tokio::test]
async fn sample_is_recorded_when_the_timed_future_is_cancelled() {
    use std::time::Duration;
    let ((), samples) = collect(async {
        let _ = tokio::time::timeout(
            Duration::from_millis(20),
            timed("slow", tokio::time::sleep(Duration::from_secs(1))),
        )
        .await;
    })
    .await;
    assert_eq!(samples.len(), 1, "{samples:?}");
    assert_eq!(samples[0].0, "slow");
}
