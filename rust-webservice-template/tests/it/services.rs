use crate::support::app_settings;
use rust_template::core::boundary_timing::collect;
use rust_template::persistence::Stores;
use rust_template::services::cache::CacheService;
use rust_template::services::config::ConfigService;
use rust_template::services::data::DataService;
use serde_json::json;

async fn stores() -> Stores {
    Stores::connect(&app_settings().await)
        .await
        .expect("stores")
}

fn labels(samples: &[(String, f64)]) -> Vec<&str> {
    samples.iter().map(|(label, _)| label.as_str()).collect()
}

#[tokio::test]
async fn config_upserts_and_lists_in_key_order() {
    let svc = ConfigService::new(stores().await.postgres.pool().clone());
    let prefix = format!("svc-{}", uuid::Uuid::new_v4().simple());
    let (b, a) = (format!("{prefix}-b"), format!("{prefix}-a"));

    let created = svc.set(&b, "staging").await.unwrap();
    assert_eq!(
        (created.key.as_str(), created.value.as_str()),
        (b.as_str(), "staging")
    );
    let updated = svc.set(&b, "production").await.unwrap();
    assert_eq!(updated.value, "production");
    svc.set(&a, "one").await.unwrap();

    let (all, samples) = collect(svc.get_all()).await;
    let mine: Vec<(String, String)> = all
        .unwrap()
        .into_iter()
        .filter(|e| e.key.starts_with(&prefix))
        .map(|e| (e.key, e.value))
        .collect();
    assert_eq!(mine, [(a, "one".to_owned()), (b, "production".to_owned())]);
    assert_eq!(labels(&samples), ["postgres.config.get_all"]);
}

#[tokio::test]
async fn data_counts_all_rows_and_limits_the_page() {
    let svc = DataService::new(
        stores().await.clickhouse.client().clone(),
        std::time::Duration::from_secs(10),
    );
    let (page, samples) = collect(svc.get_data(2)).await;
    let page = page.unwrap();
    assert_eq!(page.total, 3);
    assert_eq!(page.limit, 2);
    assert_eq!(page.rows.len(), 2);
    assert_eq!(labels(&samples), ["clickhouse.count", "clickhouse.select"]);

    let all = svc.get_data(10).await.unwrap();
    let mut names: Vec<String> = all.rows.into_iter().map(|r| r.name).collect();
    names.sort();
    assert_eq!(names, ["Alpha", "Beta", "Gamma"]);
}

#[tokio::test]
async fn cache_round_trips_json_with_and_without_ttl() {
    let svc = CacheService::new(stores().await.redis.connection());
    let key = format!("svc-{}", uuid::Uuid::new_v4().simple());

    let stored = svc
        .set(&key, json!({"nested": [1, "two"]}), None)
        .await
        .unwrap();
    assert_eq!(stored.ttl_seconds, None);
    let read = svc.get(&key).await.unwrap().expect("present");
    assert_eq!(read.value, json!({"nested": [1, "two"]}));
    assert_eq!(read.ttl_seconds, None, "no expiry -> TTL -1 -> null");

    svc.set(&key, json!("soon"), Some(60)).await.unwrap();
    let read = svc.get(&key).await.unwrap().expect("present");
    assert_eq!(read.value, json!("soon"));
    let ttl = read.ttl_seconds.expect("ttl");
    assert!((1..=60).contains(&ttl), "{ttl}");
}

#[tokio::test]
async fn cache_missing_key_and_stored_null_read_as_absent() {
    let svc = CacheService::new(stores().await.redis.connection());
    let key = format!("svc-{}", uuid::Uuid::new_v4().simple());
    assert!(svc.get(&key).await.unwrap().is_none());
    // Python: `if result is None: return None`, so a stored JSON null is "not found".
    svc.set(&key, serde_json::Value::Null, None).await.unwrap();
    assert!(svc.get(&key).await.unwrap().is_none());
}
