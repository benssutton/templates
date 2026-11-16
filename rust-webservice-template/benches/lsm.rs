//! criterion benchmark for the LSM merge and full-compaction paths (spec
//! goal 2). Run: `cargo bench --bench lsm`. Under `cargo test --all-targets`
//! criterion runs each benchmark once, as a smoke test.

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rust_template::persistence::stream_store::lsm::{compact, merge, ORDER_COLUMN};
use std::hint::black_box;
use std::sync::Arc;

const RUN_ROWS: i64 = 10_000;
const RUNS: i64 = 4;

/// `RUNS` frames of `RUN_ROWS` rows each. Each run rewrites half of the
/// previous run's keys, and 1 row in 10 is a tombstone.
fn frames() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, true),
        Field::new(ORDER_COLUMN, DataType::UInt64, false),
    ]));
    (0..RUNS)
        .map(|run| {
            let ids: Vec<i64> = (0..RUN_ROWS).map(|i| run * RUN_ROWS / 2 + i).collect();
            let seq: Vec<u64> = (0..RUN_ROWS).map(|i| (run * RUN_ROWS + i) as u64).collect();
            let ops: Vec<&str> = ids
                .iter()
                .map(|id| if id % 10 == 0 { "delete" } else { "upsert" })
                .collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|i| format!("name-{i}")),
                )),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|_| format!("v{run}")),
                )),
                Arc::new(StringArray::from(ops)),
                Arc::new(UInt64Array::from(seq)),
            ];
            RecordBatch::try_new(schema.clone(), columns).expect("valid frame")
        })
        .collect()
}

fn bench_lsm(c: &mut Criterion) {
    let keys = vec!["id".to_owned()];
    let input = frames();
    c.bench_function("lsm/merge 4x10k rows", |b| {
        b.iter(|| merge(black_box(&input), &keys).expect("merge"))
    });
    c.bench_function("lsm/compact 4x10k rows", |b| {
        b.iter_batched(
            || input.clone(),
            |runs| compact(&runs, &keys).expect("compact"),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_lsm);
criterion_main!(benches);
