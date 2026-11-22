#![allow(clippy::expect_used)]
//! zstd levels 1, 3 and 9 on a representative JSON body (a row list like
//! `GET /data`'s). Run: `cargo bench --bench compression`; criterion reports
//! the time per level, and each level's compressed size is printed first.
//! Under `cargo test --all-targets` criterion runs each benchmark once.

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn body() -> Vec<u8> {
    let categories = ["alpha", "beta", "gamma"];
    let rows: Vec<serde_json::Value> = (0..500usize)
        .map(|i| {
            serde_json::json!({
                "id": i,
                "name": format!("item-{i}"),
                "category": categories[i % categories.len()],
                "value": i as f64 * 1.5,
                "updated_at": "2026-11-22T10:00:00Z",
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({ "rows": rows })).expect("json")
}

fn levels(c: &mut Criterion) {
    let body = body();
    let mut group = c.benchmark_group("zstd");
    for level in [1, 3, 9] {
        let size = zstd::bulk::compress(&body, level).expect("compress").len();
        println!("zstd level {level}: {} -> {size} bytes", body.len());
        group.bench_function(format!("level {level}"), |b| {
            b.iter(|| zstd::bulk::compress(black_box(&body), level).expect("compress"))
        });
    }
    group.finish();
}

criterion_group!(benches, levels);
criterion_main!(benches);
