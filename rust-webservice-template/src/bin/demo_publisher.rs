//! Continuous demo publisher for compose. Settings come from the
//! same env vars as the app (SOLACE_HOST, SOLACE_PORT, SOLACE_USERNAME,
//! SOLACE_PASSWORD, SOLACE_TOPIC); PUBLISH_INTERVAL is seconds between
//! publishes (default 0.1). Thin by design: the publishing code is
//! `AmqpPublisher`, which the tests cover.

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rust_template::core::retry::{connect_with_backoff, seconds, Backoff};
use rust_template::ingestion::solace::amqp::AmqpTarget;
use rust_template::ingestion::solace::publisher::AmqpPublisher;
use rust_template::settings::Settings;
use std::sync::Arc;

fn batch(rows: &[(i64, &str, &str, &str)]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

const DEFAULT_INTERVAL: f64 = 0.1;

/// Seconds between publishes: `PUBLISH_INTERVAL` when it is a finite,
/// non-negative number, otherwise the default (with a warning), so NaN or
/// inf never reaches `seconds`.
fn publish_interval(raw: Option<&str>) -> f64 {
    let Some(raw) = raw else {
        return DEFAULT_INTERVAL;
    };
    match raw.trim().parse::<f64>() {
        Ok(v) if v.is_finite() && v >= 0.0 => v,
        _ => {
            tracing::warn!(
                value = raw,
                default = DEFAULT_INTERVAL,
                "PUBLISH_INTERVAL must be a finite number of seconds >= 0; using the default"
            );
            DEFAULT_INTERVAL
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let settings = Settings::from_env()?;
    let interval = publish_interval(std::env::var("PUBLISH_INTERVAL").ok().as_deref());
    let batches = [
        batch(&[(1, "alpha", "v1", "upsert"), (2, "beta", "v1", "upsert")])?,
        batch(&[(1, "alpha", "v2", "upsert"), (3, "gamma", "v1", "upsert")])?,
        batch(&[(2, "beta", "v1", "delete")])?,
    ];
    let target = AmqpTarget::from_settings(&settings);
    let mut publisher = connect_with_backoff(
        "Solace demo publisher",
        &Backoff::from_settings(&settings),
        || AmqpPublisher::connect(&target, &settings.solace_topic),
    )
    .await?;
    tracing::info!(topic = %settings.solace_topic, interval, "publishing demo batches");
    loop {
        for b in &batches {
            publisher.publish_batch(b).await?;
            tokio::time::sleep(seconds(interval)).await;
        }
    }
}
