use super::*;
use arrow::array::{Array, ArrayRef, AsArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type};
use std::collections::BTreeMap;

/// Rows of (id, name, value, op).
fn batch(rows: &[(i64, &str, &str, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
        ],
    )
    .unwrap()
}

fn store(layers: usize, parts_per_layer: usize) -> LsmStore {
    LsmStore::new(LsmConfig {
        layers,
        parts_per_layer,
        ..LsmConfig::default()
    })
}

/// One upsert per id, every row with value `v`.
fn upserts(ids: std::ops::Range<i64>, v: &str) -> RecordBatch {
    let rows: Vec<(i64, &str, &str, &str)> = ids.map(|i| (i, "n", v, "upsert")).collect();
    batch(&rows)
}

/// `_live` of every part, oldest data first.
fn live_bits(s: &LsmStore) -> Vec<Vec<bool>> {
    s.live_batches()
        .unwrap()
        .iter()
        .map(|b| {
            b.column_by_name(LIVE_COLUMN)
                .unwrap()
                .as_boolean()
                .iter()
                .map(Option::unwrap)
                .collect()
        })
        .collect()
}

/// (id, value) of every live row, in query order.
fn values(store: &LsmStore) -> Vec<(i64, String)> {
    let result = store.query(0).unwrap();
    let Some(rows) = result.rows else {
        return Vec::new();
    };
    let ids = rows
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>();
    let vals = rows.column_by_name("value").unwrap().as_string::<i32>();
    (0..rows.num_rows())
        .map(|i| (ids.value(i), vals.value(i).to_owned()))
        .collect()
}

#[tokio::test]
async fn a_batch_without_the_key_column_is_invalid_and_writes_nothing() {
    let s = store(4, 8);
    let no_id = batch(&[(1, "x", "v", "upsert")])
        .project(&[1, 2, 3])
        .unwrap();
    let err = s.ingest(no_id).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m == "batch missing required columns: ['id']"),
        "{err}"
    );
    assert_eq!(s.query(100).unwrap().total, 0);
}

#[tokio::test]
async fn a_batch_without_op_is_invalid() {
    let s = store(4, 8);
    let no_op = batch(&[(1, "x", "v", "upsert")])
        .project(&[0, 1, 2])
        .unwrap();
    let err = s.ingest(no_op).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m.contains("['op']")),
        "{err}"
    );
}

/// Once the first batch fixes the schema, a conflicting one
/// is rejected up front instead of poisoning every later merge.
#[tokio::test]
async fn a_batch_with_a_conflicting_schema_is_invalid() {
    let s = store(4, 8);
    s.ingest(batch(&[(1, "a", "v", "upsert")])).await.unwrap();
    let b = batch(&[(2, "b", "v", "upsert")]);
    let text_ids = arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("op", DataType::Utf8, true),
        ])),
        vec![
            text_ids,
            b.column(1).clone(),
            b.column(2).clone(),
            b.column(3).clone(),
        ],
    )
    .unwrap();
    let err = s.ingest(wrong_type).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m.contains("does not match")),
        "{err}"
    );
    let fewer = batch(&[(3, "c", "v", "upsert")])
        .project(&[0, 1, 3])
        .unwrap();
    assert!(matches!(
        s.ingest(fewer).await,
        Err(StoreError::InvalidBatch(_))
    ));
    assert_eq!(s.query(0).unwrap().total, 1);
}

/// A first batch whose `op` type the merge kernels cannot compare with
/// "delete" is rejected and does not fix the schema.
#[tokio::test]
async fn a_first_batch_the_kernels_cannot_merge_is_invalid_and_fixes_nothing() {
    use arrow::array::{ListArray, StructArray};
    use arrow::datatypes::Int32Type;
    let s = store(4, 8);
    let base = batch(&[(1, "a", "v", "upsert")]);
    let op = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new(
            "x",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        )),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
            vec![Some(1)],
        )])) as ArrayRef,
    )])) as ArrayRef;
    let bad = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("op", op.data_type().clone(), true),
        ])),
        vec![base.column(0).clone(), op],
    )
    .unwrap();
    let err = s.ingest(bad).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m.contains("cannot be merged")),
        "{err}"
    );
    s.ingest(base).await.unwrap();
    assert_eq!(values(&s), [(1, "v".to_owned())]);
}

/// An incoming `seqno` is overwritten: the store's own
/// write order decides, not the publisher's.
#[tokio::test]
async fn an_incoming_seqno_column_is_replaced_by_the_write_order() {
    let s = store(4, 8);
    s.ingest(batch(&[(1, "a", "v1", "upsert")])).await.unwrap();
    let b = batch(&[(1, "a", "v2", "upsert")]);
    let mut fields: Vec<Field> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(ORDER_COLUMN, DataType::UInt64, false));
    let mut columns = b.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from(vec![0_u64])));
    let stale = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    s.ingest(stale).await.unwrap();
    assert_eq!(values(&s), [(1, "v2".to_owned())]);
}

/// The all-or-nothing check: a message whose second
/// batch is unmergeable is refused before anything is written.
#[tokio::test]
async fn validate_rejects_a_slice_whose_second_batch_lacks_op_and_writes_nothing() {
    let s = store(4, 8);
    let good = batch(&[(1, "a", "v", "upsert")]);
    let no_op = batch(&[(2, "b", "v", "upsert")])
        .project(&[0, 1, 2])
        .unwrap();
    let err = s.validate(&[good, no_op]).unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m == "batch missing required columns: ['op']"),
        "{err}"
    );
    assert_eq!(s.query(0).unwrap().total, 0);
    assert_eq!((s.stats().rows, s.stats().parts), (0, 0));
}

/// On an empty store the slice's first batch sets the schema the rest
/// must match, so batches that disagree with each other are refused.
#[tokio::test]
async fn validate_rejects_a_slice_whose_batches_disagree_on_an_empty_store() {
    let s = store(4, 8);
    let first = batch(&[(1, "a", "v", "upsert")]);
    let fewer = batch(&[(2, "b", "v", "upsert")])
        .project(&[0, 1, 3])
        .unwrap();
    let err = s.validate(&[first, fewer]).unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m.contains("does not match")),
        "{err}"
    );
    assert_eq!(s.query(0).unwrap().total, 0);
}

#[tokio::test]
async fn validate_checks_against_the_schema_the_store_already_fixed() {
    let s = store(4, 8);
    s.ingest(batch(&[(1, "a", "v", "upsert")])).await.unwrap();
    let fewer = batch(&[(2, "b", "v", "upsert")])
        .project(&[0, 1, 3])
        .unwrap();
    assert!(matches!(
        s.validate(&[fewer]),
        Err(StoreError::InvalidBatch(_))
    ));
}

#[tokio::test]
async fn validate_accepts_a_valid_slice_and_writes_nothing() {
    let s = store(4, 8);
    let slice = [
        batch(&[(1, "a", "v1", "upsert")]),
        batch(&[(1, "a", "v1", "delete")])
            .project(&[3, 2, 1, 0])
            .unwrap(),
    ];
    s.validate(&slice).unwrap();
    assert_eq!(s.query(0).unwrap().total, 0);
    assert_eq!((s.stats().rows, s.stats().parts), (0, 0));
    // Nothing was fixed either: a different schema is still accepted.
    s.ingest(batch(&[(1, "a", "v", "upsert")]).project(&[0, 3]).unwrap())
        .await
        .unwrap();
    assert_eq!(s.query(0).unwrap().total, 1);
}

/// The `key_columns` extension point: with `["id", "version"]` the
/// key is the pair, so a newer (1, 1) does not shadow (1, 2).
#[tokio::test]
async fn a_multi_column_key_partitions_by_every_key_column() {
    let s = LsmStore::new(LsmConfig {
        key_columns: vec!["id".into(), "version".into()],
        ..LsmConfig::default()
    });
    let rows = |r: &[(i64, i64, &str)]| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("version", DataType::Int64, true),
                Field::new("value", DataType::Utf8, true),
                Field::new("op", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.0))),
                Arc::new(Int64Array::from_iter_values(r.iter().map(|x| x.1))),
                Arc::new(StringArray::from_iter_values(r.iter().map(|x| x.2))),
                Arc::new(StringArray::from_iter_values(r.iter().map(|_| "upsert"))),
            ],
        )
        .unwrap()
    };
    s.ingest(rows(&[(1, 2, "v2-old"), (1, 1, "v1-old")]))
        .await
        .unwrap();
    s.ingest(rows(&[(1, 1, "v1-new")])).await.unwrap();
    let out = s.query(0).unwrap().rows.unwrap();
    let versions = out
        .column_by_name("version")
        .unwrap()
        .as_primitive::<Int64Type>();
    let vals = out.column_by_name("value").unwrap().as_string::<i32>();
    let got: Vec<(i64, &str)> = (0..out.num_rows())
        .map(|i| (versions.value(i), vals.value(i)))
        .collect();
    assert_eq!(got, [(1, "v1-new"), (2, "v2-old")]);
}

/// Null keys form one key: the newer null-id row wins, and nulls sort
/// first.
#[tokio::test]
async fn null_keys_are_one_key_and_sort_first() {
    let s = store(4, 8);
    let rows = |r: &[(Option<i64>, &str)]| {
        RecordBatch::try_new(
            batch(&[]).schema(),
            vec![
                Arc::new(Int64Array::from(r.iter().map(|x| x.0).collect::<Vec<_>>())),
                Arc::new(StringArray::from_iter_values(r.iter().map(|_| "n"))),
                Arc::new(StringArray::from_iter_values(r.iter().map(|x| x.1))),
                Arc::new(StringArray::from_iter_values(r.iter().map(|_| "upsert"))),
            ],
        )
        .unwrap()
    };
    s.ingest(rows(&[(Some(1), "one"), (None, "null-old")]))
        .await
        .unwrap();
    s.ingest(rows(&[(None, "null-new")])).await.unwrap();
    let out = s.query(0).unwrap().rows.unwrap();
    let ids = out
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>();
    let vals = out.column_by_name("value").unwrap().as_string::<i32>();
    let got: Vec<(Option<i64>, &str)> = (0..out.num_rows())
        .map(|i| ((!ids.is_null(i)).then(|| ids.value(i)), vals.value(i)))
        .collect();
    assert_eq!(got, [(None, "null-new"), (Some(1), "one")]);
}

#[tokio::test]
async fn a_batch_with_duplicate_column_names_is_invalid() {
    let s = store(4, 8);
    let b = batch(&[(1, "a", "v", "upsert")]);
    let dup = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("op", DataType::Utf8, true),
        ])),
        vec![
            b.column(0).clone(),
            b.column(1).clone(),
            b.column(2).clone(),
            b.column(3).clone(),
        ],
    )
    .unwrap();
    let err = s.ingest(dup.clone()).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m == "batch has duplicate column names: ['value']"),
        "{err}"
    );
    assert!(matches!(
        s.validate(&[dup]),
        Err(StoreError::InvalidBatch(_))
    ));
    assert_eq!(s.query(0).unwrap().total, 0);
}

#[tokio::test]
async fn column_order_may_differ_between_batches() {
    let s = store(4, 8);
    s.ingest(batch(&[(1, "a", "v1", "upsert")])).await.unwrap();
    let reordered = batch(&[(1, "a", "v2", "upsert")])
        .project(&[3, 2, 1, 0])
        .unwrap();
    s.ingest(reordered).await.unwrap();
    assert_eq!(values(&s), [(1, "v2".to_owned())]);
}

/// Snapshot isolation under a concurrent write: every read sees
/// exactly the rows of some prefix of the writes, never a half-applied
/// supersede or merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readers_see_a_consistent_snapshot_while_the_writer_merges() {
    let s = Arc::new(store(3, 2));
    // The writer starts only after the reader's first query, so the
    // reader sees the empty store and then at least one more snapshot.
    let (started, start) = tokio::sync::oneshot::channel::<()>();
    let writer = {
        let s = s.clone();
        tokio::spawn(async move {
            start.await.unwrap();
            for i in 0..300_i64 {
                s.ingest(batch(&[(i, "n", "v", "upsert")])).await.unwrap();
            }
        })
    };
    let reader = {
        let s = s.clone();
        tokio::task::spawn_blocking(move || {
            let mut started = Some(started);
            let mut last_total = 0;
            let mut reads = 0;
            while last_total < 300 {
                let result = s.query(0).unwrap();
                assert!(result.total >= last_total, "total went backwards");
                if let Some(rows) = result.rows {
                    let ids = rows
                        .column_by_name("id")
                        .unwrap()
                        .as_primitive::<Int64Type>();
                    let expected: Vec<i64> = (0..result.total as i64).collect();
                    assert_eq!(
                        ids.values().to_vec(),
                        expected,
                        "not a prefix of the writes"
                    );
                }
                last_total = result.total;
                reads += 1;
                if let Some(tx) = started.take() {
                    tx.send(()).unwrap();
                }
            }
            reads
        })
    };
    writer.await.unwrap();
    let reads = reader.await.unwrap();
    assert!(reads > 1, "the first read precedes every write");
    // 300 batches with P = 2, L = 3: layer digits 0, 0 and 300 / 4 top parts.
    assert_eq!(s.layer_parts(), [0, 0, 75]);
}

/// A newer version kills the old row at write time: the old part's bit is
/// cleared, so no read has to deduplicate.
#[tokio::test]
async fn a_newer_version_supersedes_the_old_row_on_write() {
    let s = store(4, 8);
    s.ingest(batch(&[(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")]))
        .await
        .unwrap();
    s.ingest(batch(&[(1, "a", "v2", "upsert")])).await.unwrap();
    assert_eq!(values(&s), [(1, "v2".to_owned()), (2, "v1".to_owned())]);
    assert_eq!(live_bits(&s), [vec![false, true], vec![true]]);
}

/// A delete kills the live version and is not stored itself.
#[tokio::test]
async fn a_delete_hides_the_key_and_is_not_stored() {
    let s = store(4, 8);
    s.ingest(batch(&[(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")]))
        .await
        .unwrap();
    s.ingest(batch(&[(1, "a", "", "delete"), (3, "c", "", "delete")]))
        .await
        .unwrap();
    assert_eq!(values(&s), [(2, "v1".to_owned())]);
    assert_eq!(s.stats().rows, 2, "the deletes were not stored");
    assert_eq!(s.query(0).unwrap().total, 1);
}

#[tokio::test]
async fn reinsert_after_delete_wins() {
    let s = store(2, 2);
    s.ingest(batch(&[(1, "a", "v1", "upsert")])).await.unwrap();
    s.ingest(batch(&[(1, "a", "", "delete")])).await.unwrap();
    s.ingest(batch(&[(1, "a", "v3", "upsert")])).await.unwrap();
    assert_eq!(values(&s), [(1, "v3".to_owned())]);
}

/// Within one batch the last row for a key wins; the others are never stored.
#[tokio::test]
async fn the_last_row_for_a_key_within_a_batch_wins() {
    let s = store(4, 8);
    s.ingest(batch(&[
        (1, "a", "first", "upsert"),
        (1, "a", "second", "upsert"),
        (2, "b", "x", "upsert"),
    ]))
    .await
    .unwrap();
    assert_eq!(values(&s), [(1, "second".to_owned()), (2, "x".to_owned())]);
    assert_eq!(s.stats().rows, 2);
}

/// Parts per layer follow a base-P counter of the batches received, except
/// that the top layer is never merged: with P = 3, L = 3, after `n` batches
/// of new keys the layers hold (n % 3, n / 3 % 3, n / 9) parts.
#[tokio::test]
async fn layers_count_batches_in_base_p_and_only_the_top_layer_overflows() {
    let s = store(3, 3);
    for n in 1..=40_i64 {
        s.ingest(upserts(n * 10..n * 10 + 5, "v")).await.unwrap();
        let n = n as usize;
        assert_eq!(
            s.layer_parts(),
            [n % 3, n / 3 % 3, n / 9],
            "after {n} batches"
        );
    }
    assert_eq!(
        s.layer_parts()[2],
        4,
        "the top layer holds more than P parts"
    );
    assert_eq!(s.query(0).unwrap().total, 200);
}

/// Each row is written once on arrival and once per merge into layers 1 to
/// L-1, so write amplification never exceeds L.
#[tokio::test]
async fn write_amplification_is_at_most_the_layer_count() {
    let s = store(3, 4);
    // 128 = 8 x 4^2 batches: every row has crossed both merges.
    for n in 0..128_i64 {
        s.ingest(upserts(n * 10..n * 10 + 10, "v")).await.unwrap();
    }
    let ingested = 128 * 10;
    assert_eq!(s.rows_written(), ingested * 3);
    assert_eq!(s.layer_parts(), [0, 0, 8]);
}

/// A merge keeps only live rows: superseded versions are not rewritten.
#[tokio::test]
async fn a_merge_drops_dead_rows() {
    let s = store(3, 2);
    s.ingest(upserts(0..10, "old")).await.unwrap();
    s.ingest(upserts(5..15, "new")).await.unwrap();
    assert_eq!(s.layer_parts(), [0, 1, 0]);
    assert_eq!(
        s.stats().rows,
        15,
        "the 5 superseded rows were not rewritten"
    );
    let rows = values(&s);
    assert_eq!(rows.len(), 15);
    assert_eq!(rows[4], (4, "old".to_owned()));
    assert_eq!(rows[5], (5, "new".to_owned()));
}

/// A part whose rows are all superseded is dropped at once.
#[tokio::test]
async fn a_part_with_no_live_rows_is_dropped() {
    let s = store(4, 8);
    s.ingest(upserts(0..3, "old")).await.unwrap();
    s.ingest(upserts(0..3, "new")).await.unwrap();
    assert_eq!(s.layer_parts(), [1, 0, 0, 0]);
    assert_eq!(s.stats().rows, 3);
}

fn capped(max_rows: usize) -> LsmStore {
    LsmStore::new(LsmConfig {
        max_rows,
        ..LsmConfig::default()
    })
}

/// A batch that would take the rows held over `max_rows` is refused whole:
/// no index change, no kill, no part. The store then freezes: every later
/// non-empty batch is refused too, even one that would fit or only deletes,
/// while reads keep serving what was received.
#[tokio::test]
async fn a_batch_over_max_rows_is_refused_whole_and_freezes_the_store() {
    let s = capped(10);
    write(&s, upserts(0..8, "a")).await;
    // Ids 6 and 7 would supersede rows of the first part, 8..12 are new.
    let err = s.ingest(upserts(6..12, "b")).await.unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::Full {
                held: 8,
                max_rows: 10
            }
        ),
        "{err}"
    );
    check(&s);
    assert_eq!(
        values(&s),
        (0..8).map(|i| (i, "a".to_owned())).collect::<Vec<_>>()
    );
    assert_eq!(live_bits(&s), [vec![true; 8]], "nothing was killed");
    let stats = s.stats();
    assert_eq!((stats.rows, stats.parts), (8, 1));
    assert!(stats.full);
    assert_eq!(stats.refused_batches_total, 1);
    // Frozen: a batch that fits, and one that only deletes, are refused.
    for frozen in [upserts(20..21, "c"), batch(&[(1, "n", "", "delete")])] {
        assert!(matches!(
            s.ingest(frozen).await,
            Err(StoreError::Full {
                held: 8,
                max_rows: 10
            })
        ));
        check(&s);
    }
    // An empty batch carries nothing to refuse.
    write(&s, batch(&[])).await;
    assert_eq!(s.stats().refused_batches_total, 3);
    assert_eq!(ids(&s), (0..8).collect::<Vec<_>>());
}

/// The cap is on rows held, not live rows: a superseded row stays held in
/// its part, so it counts until that part merges or dies.
#[tokio::test]
async fn dead_rows_count_toward_max_rows() {
    let s = capped(4);
    write(&s, upserts(0..3, "a")).await;
    write(&s, upserts(0..1, "b")).await;
    assert_eq!((s.stats().rows, s.stats().live_rows), (4, 3));
    // 3 live + 1 would fit a live-row cap; 4 held + 1 does not.
    assert!(matches!(
        s.ingest(upserts(5..6, "c")).await,
        Err(StoreError::Full {
            held: 4,
            max_rows: 4
        })
    ));
    assert!(s.stats().full);
}

/// A batch is measured by the rows it would store, after the newest-per-key
/// dedup and without its deletes, so one that fills the cap exactly is
/// accepted, and so is a delete-only batch at the cap.
#[tokio::test]
async fn a_batch_is_measured_by_the_rows_it_would_store() {
    let s = capped(3);
    write(
        &s,
        batch(&[
            (1, "n", "old", "upsert"),
            (1, "n", "new", "upsert"),
            (2, "n", "v", "upsert"),
            (9, "n", "", "delete"),
        ]),
    )
    .await;
    assert_eq!(s.stats().rows, 2);
    write(
        &s,
        batch(&[(3, "n", "v", "upsert"), (2, "n", "", "delete")]),
    )
    .await;
    assert_eq!((s.stats().rows, s.stats().live_rows), (3, 2));
    write(&s, batch(&[(1, "n", "", "delete")])).await;
    assert_eq!(ids(&s), [3]);
    assert!(!s.stats().full);
    assert_eq!(s.stats().refused_batches_total, 0);
}

/// Refusal is checked after validation: an invalid batch is still invalid,
/// and a batch too big for an empty store fixes no schema.
#[tokio::test]
async fn a_first_batch_over_max_rows_fixes_no_schema() {
    let s = capped(2);
    assert!(matches!(
        s.ingest(upserts(0..3, "a")).await,
        Err(StoreError::Full {
            held: 0,
            max_rows: 2
        })
    ));
    check(&s);
    assert!(s.core.snapshot.load().schema.is_none());
    let no_op = batch(&[(1, "x", "v", "upsert")])
        .project(&[0, 1, 2])
        .unwrap();
    assert!(matches!(
        s.ingest(no_op).await,
        Err(StoreError::InvalidBatch(_))
    ));
}

/// Rows come back in key order across parts, whatever the write order; a
/// limit pages, 0 returns every row, and total counts every live row.
#[tokio::test]
async fn reads_merge_parts_in_key_order_and_page() {
    let s = store(4, 8);
    s.ingest(batch(&[(5, "e", "v", "upsert"), (1, "a", "v", "upsert")]))
        .await
        .unwrap();
    s.ingest(batch(&[(3, "c", "v", "upsert"), (4, "d", "v", "upsert")]))
        .await
        .unwrap();
    s.ingest(batch(&[(2, "b", "v", "upsert")])).await.unwrap();
    let ids = |limit: usize| -> Vec<i64> {
        let result = s.query(limit).unwrap();
        assert_eq!(result.total, 5);
        result
            .rows
            .unwrap()
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>()
            .values()
            .to_vec()
    };
    assert_eq!(ids(0), [1, 2, 3, 4, 5]);
    assert_eq!(ids(2), [1, 2]);
}

#[tokio::test]
async fn query_drops_the_order_and_op_columns() {
    let s = store(4, 8);
    s.ingest(batch(&[(1, "a", "v", "upsert")])).await.unwrap();
    let rows = s.query(0).unwrap().rows.unwrap();
    let names: Vec<&str> = rows
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["id", "name", "value"]);
}

#[tokio::test]
async fn an_empty_store_has_no_rows() {
    let result = store(4, 8).query(10).unwrap();
    assert!(result.rows.is_none());
    assert_eq!(result.total, 0);
}

/// Query engines get each part with a `_live` column; the data columns are
/// the store's own buffers, not copies.
#[tokio::test]
async fn live_batches_share_the_stores_buffers() {
    let s = store(4, 8);
    s.ingest(upserts(0..4, "v")).await.unwrap();
    let first = s.live_batches().unwrap();
    let second = s.live_batches().unwrap();
    let id_buffer =
        |b: &RecordBatch| b.column_by_name("id").unwrap().to_data().buffers()[0].as_ptr();
    assert_eq!(id_buffer(&first[0]), id_buffer(&second[0]));
    let live = first[0].column_by_name(LIVE_COLUMN).unwrap().as_boolean();
    assert_eq!(live.true_count(), 4);
}

/// `_live` is the column `live_batches` appends, so a data column of that
/// name would shadow the store's bitmap: it is refused.
#[tokio::test]
async fn a_batch_with_a_live_column_is_invalid() {
    let s = store(4, 8);
    let b = batch(&[(1, "a", "v", "upsert")]);
    let mut fields: Vec<Field> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(LIVE_COLUMN, DataType::Boolean, true));
    let mut columns = b.columns().to_vec();
    columns.push(Arc::new(arrow::array::BooleanArray::from(vec![true])));
    let reserved = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let err = s.ingest(reserved.clone()).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::InvalidBatch(m) if m == "batch uses reserved column names: ['_live']"),
        "{err}"
    );
    assert!(matches!(
        s.validate(&[reserved]),
        Err(StoreError::InvalidBatch(_))
    ));
    assert_eq!(s.query(0).unwrap().total, 0);
}

/// Copy-on-write: what a reader took before a write still reads the same
/// rows after it, although the write cleared bits in the same part.
#[tokio::test]
async fn a_view_taken_before_a_write_is_unchanged_by_it() {
    let s = store(4, 8);
    s.ingest(upserts(0..4, "old")).await.unwrap();
    let before = s.live_batches().unwrap();
    s.ingest(upserts(0..2, "new")).await.unwrap();
    let live = |b: &RecordBatch| -> Vec<bool> {
        b.column_by_name(LIVE_COLUMN)
            .unwrap()
            .as_boolean()
            .iter()
            .map(Option::unwrap)
            .collect()
    };
    assert_eq!(live(&before[0]), [true, true, true, true]);
    assert_eq!(live_bits(&s)[0], [false, false, true, true]);
}

// ---- invariants ----

/// Panics unless the writer's index, bitmaps, layers and published snapshot
/// agree: one index entry per live row, each pointing at a set bit holding
/// that key; each part's live count equal to its popcount and above zero; no
/// layer below the top at `P` parts or more; rows held within `max_rows`;
/// the store full exactly when it has refused a batch; the snapshot in step.
fn check(s: &LsmStore) {
    let Ok(w) = s.writer.try_lock() else {
        panic!("a write is in progress");
    };
    let parts: HashMap<u64, &Part> = w.layers.iter().flatten().map(|p| (p.id, p)).collect();
    let live: usize = parts.values().map(|p| p.live_rows()).sum();
    assert_eq!(w.index.len(), live, "index entries != live rows");
    for (key, loc) in &w.index {
        let Some(part) = parts.get(&loc.part) else {
            panic!("{loc:?} names no part");
        };
        assert!(part.is_live(loc.row), "{loc:?} points at a dead row");
        assert_eq!(
            part.key(loc.row).as_ref(),
            &**key,
            "{loc:?} holds another key"
        );
    }
    for part in parts.values() {
        assert_eq!(
            part.live_rows(),
            part.live().count_set_bits(),
            "part {}",
            part.id
        );
        assert!(part.live_rows() > 0, "part {} has no live rows", part.id);
    }
    let top = w.layers.len() - 1;
    for (i, layer) in w.layers[..top].iter().enumerate() {
        assert!(
            layer.len() < s.core.config.parts_per_layer || w.merge_failed_at[i].is_some(),
            "layer {i} holds {} parts with no failed merge",
            layer.len()
        );
    }
    let held: usize = parts.values().map(|p| p.rows()).sum();
    assert!(
        held <= s.core.config.max_rows,
        "{held} rows held over the cap"
    );
    let stats = s.stats();
    assert_eq!(
        stats.full,
        stats.refused_batches_total > 0,
        "full without a refusal, or a refusal without full"
    );
    let snap = s.core.snapshot.load();
    assert_eq!(snap.live_rows, live, "snapshot live rows");
    assert_eq!(snap.held_rows, held, "snapshot held rows");
    assert_eq!(snap.parts.len(), parts.len(), "snapshot parts");
}

/// `ingest`, then `check`.
async fn write(s: &LsmStore, b: RecordBatch) {
    s.ingest(b).await.unwrap();
    check(s);
}

fn ids(s: &LsmStore) -> Vec<i64> {
    values(s).into_iter().map(|(id, _)| id).collect()
}

/// After a merge the index points into the merged part: a newer version
/// kills the merged row, not a row of the parts that were merged.
#[tokio::test]
async fn a_newer_version_after_a_merge_supersedes_the_merged_row() {
    let s = store(3, 2);
    write(&s, upserts(0..4, "a")).await;
    write(&s, upserts(10..14, "a")).await;
    assert_eq!(s.layer_parts(), [0, 1, 0]);
    write(&s, upserts(2..3, "b")).await;
    assert_eq!(values(&s)[2], (2, "b".to_owned()));
    assert_eq!(
        live_bits(&s),
        [
            vec![true, true, false, true, true, true, true, true],
            vec![true]
        ]
    );
}

#[tokio::test]
async fn a_delete_after_a_merge_kills_the_merged_row() {
    let s = store(3, 2);
    write(&s, upserts(0..4, "a")).await;
    write(&s, upserts(10..14, "a")).await;
    write(&s, batch(&[(3, "n", "", "delete")])).await;
    assert_eq!(ids(&s), [0, 1, 2, 10, 11, 12, 13]);
    assert_eq!(s.layer_parts(), [0, 1, 0], "the delete stored nothing");
    assert_eq!(s.stats().rows, 8, "the dead row stays held in its part");
    assert_eq!(
        s.stats().live_rows,
        7,
        "rows counts held rows, live_rows the live ones"
    );
}

#[tokio::test]
async fn an_empty_batch_adds_no_part_and_fixes_no_schema() {
    let s = store(4, 8);
    write(&s, batch(&[])).await;
    assert_eq!((s.stats().parts, s.layer_parts()), (0, vec![0, 0, 0, 0]));
    // A different schema is still accepted: the empty batch fixed nothing.
    write(
        &s,
        batch(&[(1, "a", "v", "upsert")]).project(&[0, 3]).unwrap(),
    )
    .await;
    assert_eq!(s.stats().parts, 1);
}

#[tokio::test]
async fn an_all_delete_batch_adds_no_part() {
    let s = store(4, 8);
    write(&s, upserts(0..3, "v")).await;
    write(&s, batch(&[(1, "n", "", "delete"), (7, "n", "", "delete")])).await;
    assert_eq!((s.stats().parts, s.layer_parts()), (1, vec![1, 0, 0, 0]));
    assert_eq!(ids(&s), [0, 2]);
}

/// A fixed-seed xorshift generator: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// Random upserts, deletes and in-batch duplicates over a small key range,
/// with small layers and a small cap, against a `BTreeMap` model of the
/// same rules: last write wins, a delete removes, and once a batch would
/// take the rows held over the cap it is refused and the store freezes, so
/// every later batch is refused too. The invariants are checked after every
/// write.
#[tokio::test]
async fn random_writes_match_a_model() {
    // With this seed the store fills after about 100 batches, so the run
    // covers both merges into the top layer and a long frozen stretch.
    let max_rows = 100;
    let s = LsmStore::new(LsmConfig {
        layers: 3,
        parts_per_layer: 2,
        max_rows,
        ..LsmConfig::default()
    });
    let mut model: BTreeMap<i64, String> = BTreeMap::new();
    let mut frozen = false;
    let mut refused = 0_u64;
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for step in 0..500 {
        let rows: Vec<(i64, String, &str)> = (0..1 + rng.below(8))
            .map(|i| {
                let op = if rng.below(4) == 0 {
                    "delete"
                } else {
                    "upsert"
                };
                (rng.below(40) as i64, format!("s{step}r{i}"), op)
            })
            .collect();
        // What the batch would store: the last row of each key, unless that
        // row is a delete.
        let mut last: BTreeMap<i64, &str> = BTreeMap::new();
        for (id, _, op) in &rows {
            last.insert(*id, op);
        }
        let stored = last.values().filter(|op| **op != "delete").count();
        let held = s.stats().rows as usize;
        frozen |= held + stored > max_rows;
        let refs: Vec<(i64, &str, &str, &str)> = rows
            .iter()
            .map(|(id, value, op)| (*id, "n", value.as_str(), *op))
            .collect();
        let result = s.ingest(batch(&refs)).await;
        check(&s);
        if frozen {
            refused += 1;
            assert!(
                matches!(result, Err(StoreError::Full { held: h, .. }) if h == held as u64),
                "step {step}: {result:?}"
            );
        } else {
            result.unwrap();
            for (id, value, op) in &rows {
                if *op == "delete" {
                    model.remove(id);
                } else {
                    model.insert(*id, value.clone());
                }
            }
        }
        let want: Vec<(i64, String)> = model.iter().map(|(k, v)| (*k, v.clone())).collect();
        assert_eq!(values(&s), want, "step {step}");
        assert_eq!(s.query(0).unwrap().total, model.len(), "step {step}");
        assert_eq!(s.stats().live_rows, model.len() as u64, "step {step}");
        assert_eq!(s.stats().refused_batches_total, refused, "step {step}");
    }
    assert!(s.stats().full, "the run never became full");
    assert!(
        refused <= 400,
        "the store froze after {} batches",
        500 - refused
    );
    assert!(
        s.layer_parts()[2] > 0,
        "the run never merged into the top layer"
    );
}

/// A failed merge leaves the parts where they are (nothing is lost), is
/// counted, and is retried only once its layer's part count has doubled
/// since the failure. A permanent failure then costs a logarithmic number
/// of whole-layer copies, not one per write.
#[tokio::test]
async fn a_failed_merge_is_retried_only_once_its_layer_has_doubled() {
    let s = store(3, 2);
    s.core.fail_merges.store(true, Ordering::Relaxed);
    write(&s, upserts(0..2, "a")).await;
    write(&s, upserts(2..4, "a")).await;
    assert_eq!(s.layer_parts(), [2, 0, 0]);
    assert_eq!(s.stats().merge_failures_total, 1);
    // No new part, then 3 parts (< 2 x 2): no retry.
    write(&s, batch(&[(99, "n", "", "delete")])).await;
    write(&s, upserts(4..6, "a")).await;
    assert_eq!(s.layer_parts(), [3, 0, 0]);
    assert_eq!(s.stats().merge_failures_total, 1);
    // 4 parts: retried, and it fails again; the next retry waits for 8.
    write(&s, upserts(6..8, "a")).await;
    assert_eq!(s.stats().merge_failures_total, 2);
    s.core.fail_merges.store(false, Ordering::Relaxed);
    for n in 4..7_i64 {
        write(&s, upserts(n * 2..n * 2 + 2, "a")).await;
    }
    assert_eq!(s.layer_parts(), [7, 0, 0], "still waiting for 8 parts");
    assert_eq!(ids(&s), (0..14).collect::<Vec<_>>());
    write(&s, upserts(14..16, "a")).await;
    assert_eq!(s.layer_parts(), [0, 1, 0]);
    assert_eq!(s.stats().merge_failures_total, 2);
    assert_eq!(ids(&s), (0..16).collect::<Vec<_>>());
}

/// The writer mutex does not poison, so a panic after the index changed
/// would leave later writes running on corrupt state. The store poisons
/// itself instead: every later write is refused, readers keep the last
/// published snapshot, and `stats` reports it for readiness.
#[tokio::test]
async fn a_panicked_write_poisons_the_store() {
    let s = store(4, 8);
    write(&s, upserts(0..2, "a")).await;
    s.core.panic_next_write.store(true, Ordering::Relaxed);
    let err = s.ingest(upserts(0..1, "b")).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::Internal(m) if m.contains("panicked")),
        "{err}"
    );
    assert!(s.stats().poisoned);
    let err = s.ingest(upserts(5..6, "c")).await.unwrap_err();
    assert!(
        matches!(&err, StoreError::Internal(m) if m == "LSM store poisoned by a panicked write"),
        "{err}"
    );
    assert_eq!(values(&s), [(0, "a".to_owned()), (1, "a".to_owned())]);
}
