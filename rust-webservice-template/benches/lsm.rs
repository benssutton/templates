#![allow(clippy::expect_used)]
//! criterion benchmarks for the layered LSM store: 512 writes that each
//! supersede half the previous write's keys (one full cascade into the top
//! layer at P = 8), and a 100-row ordered read. Run: `cargo bench --bench
//! lsm`. Under `cargo test --all-targets` criterion runs each once, as a
//! smoke test.

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use rust_template::persistence::stream_store::StreamStore;
use rust_template::persistence::stream_store::lsm::{LsmConfig, LsmStore};
use std::hint::black_box;
use std::sync::Arc;

const BATCHES: i64 = 512;
const BATCH_ROWS: i64 = 1_000;

/// Batch `n`: `BATCH_ROWS` upserts whose ids overlap half of batch `n - 1`'s.
fn batch(n: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, true),
    ]));
    let ids: Vec<i64> = (0..BATCH_ROWS).map(|i| n * BATCH_ROWS / 2 + i).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.clone())),
        Arc::new(StringArray::from_iter_values(
            ids.iter().map(|i| format!("name-{i}")),
        )),
        Arc::new(StringArray::from_iter_values(
            ids.iter().map(|_| format!("v{n}")),
        )),
        Arc::new(StringArray::from_iter_values(ids.iter().map(|_| "upsert"))),
    ];
    RecordBatch::try_new(schema, columns).expect("valid batch")
}

fn filled(runtime: &tokio::runtime::Runtime, batches: &[RecordBatch]) -> LsmStore {
    let store = LsmStore::new(LsmConfig::default());
    runtime.block_on(async {
        for b in batches {
            store.ingest(b.clone()).await.expect("ingest");
        }
    });
    store
}

fn bench_lsm(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let batches: Vec<RecordBatch> = (0..BATCHES).map(batch).collect();
    let mut group = c.benchmark_group("lsm");
    group.sample_size(10);
    group.bench_function("ingest 512 x 1k rows, half superseded", |b| {
        b.iter(|| filled(&runtime, black_box(&batches)))
    });
    let store = filled(&runtime, &batches);
    group.bench_function("query limit 100", |b| {
        b.iter(|| store.query(black_box(100)).expect("query"))
    });
    group.finish();
}

criterion_group!(benches, bench_lsm);
criterion_main!(benches);
