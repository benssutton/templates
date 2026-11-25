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

/// Over `max_rows` live rows the oldest writes are marked dead down to 90%
/// of the cap, counted and no longer read; the newest survive.
#[tokio::test]
async fn over_the_cap_the_oldest_rows_are_evicted_to_ninety_percent() {
    let s = LsmStore::new(LsmConfig {
        max_rows: 20,
        ..LsmConfig::default()
    });
    assert_eq!(s.ingest(upserts(0..15, "a")).await.unwrap(), 0);
    // 25 live > 20: down to 18, so the 7 oldest (ids 0..7) go.
    assert_eq!(s.ingest(upserts(100..110, "b")).await.unwrap(), 7);
    assert_eq!(s.query(0).unwrap().total, 18);
    assert_eq!(values(&s).first().map(|r| r.0), Some(7));
    assert_eq!(s.stats().evicted_rows_total, 7);
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
/// layer below the top at `P` parts or more; the snapshot in step.
fn check(s: &LsmStore) {
    let Ok(w) = s.writer.try_lock() else {
        panic!("a write is in progress");
    };
    let parts: HashMap<u64, &Part> = w.layers.iter().flatten().map(|p| (p.id, p)).collect();
    let live: usize = parts.values().map(|p| p.live_rows).sum();
    assert_eq!(w.index.len(), live, "index entries != live rows");
    for (key, loc) in &w.index {
        let Some(part) = parts.get(&loc.part) else {
            panic!("{loc:?} names no part");
        };
        assert!(part.is_live(loc.row), "{loc:?} points at a dead row");
        assert_eq!(
            part.key(loc.row).as_ref(),
            key.row().as_ref(),
            "{loc:?} holds another key"
        );
    }
    for part in parts.values() {
        assert_eq!(
            part.live_rows,
            part.live.count_set_bits(),
            "part {}",
            part.id
        );
        assert!(part.live_rows > 0, "part {} has no live rows", part.id);
    }
    let top = w.layers.len() - 1;
    for (i, layer) in w.layers[..top].iter().enumerate() {
        assert!(
            layer.len() < s.core.config.parts_per_layer,
            "layer {i} holds {} parts",
            layer.len()
        );
    }
    let snap = s.core.snapshot.load();
    assert_eq!(snap.live_rows, live, "snapshot live rows");
    assert_eq!(snap.parts.len(), parts.len(), "snapshot parts");
}

/// `ingest`, then `check`.
async fn write(s: &LsmStore, b: RecordBatch) -> u64 {
    let evicted = s.ingest(b).await.unwrap();
    check(s);
    evicted
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
}

/// Eviction takes the oldest rows across layers (the top first) and drops a
/// part it empties; an evicted key can be written again, and deleting one
/// is a no-op.
#[tokio::test]
async fn eviction_crosses_layers_drops_emptied_parts_and_forgets_the_keys() {
    let s = LsmStore::new(LsmConfig {
        layers: 3,
        parts_per_layer: 2,
        max_rows: 10,
        ..LsmConfig::default()
    });
    write(&s, upserts(0..4, "a")).await;
    write(&s, upserts(4..8, "a")).await;
    assert_eq!(s.layer_parts(), [0, 1, 0]);
    // 18 live > 10: down to 9, so all 8 rows of the layer-1 part (dropped)
    // and the oldest row of the new layer-0 part (id 100) go.
    assert_eq!(write(&s, upserts(100..110, "b")).await, 9);
    assert_eq!(s.layer_parts(), [1, 0, 0]);
    assert_eq!(s.stats().parts, 1);
    assert_eq!(ids(&s), (101..110).collect::<Vec<_>>());
    write(&s, upserts(0..1, "again")).await;
    assert_eq!(values(&s)[0], (0, "again".to_owned()));
    write(
        &s,
        batch(&[(1, "n", "", "delete"), (100, "n", "", "delete")]),
    )
    .await;
    assert_eq!(s.query(0).unwrap().total, 10);
    assert_eq!(s.stats().evicted_rows_total, 9);
}

#[tokio::test]
async fn an_empty_batch_adds_no_part_and_fixes_no_schema() {
    let s = store(4, 8);
    assert_eq!(write(&s, batch(&[])).await, 0);
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
/// same rules (last write wins; over the cap, drop the oldest writes down to
/// 90%). The invariants are checked after every write.
#[tokio::test]
async fn random_writes_match_a_model() {
    let max_rows = 24;
    let s = LsmStore::new(LsmConfig {
        layers: 3,
        parts_per_layer: 2,
        max_rows,
        ..LsmConfig::default()
    });
    let mut model: BTreeMap<i64, (String, u64)> = BTreeMap::new();
    let mut seqno = 0_u64;
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
        for (id, value, op) in &rows {
            if *op == "delete" {
                model.remove(id);
            } else {
                model.insert(*id, (value.clone(), seqno));
            }
            seqno += 1;
        }
        let mut expected_evicted = 0;
        if model.len() > max_rows {
            let mut by_age: Vec<(u64, i64)> = model.iter().map(|(k, (_, q))| (*q, *k)).collect();
            by_age.sort_unstable();
            let excess = model.len() - (max_rows - max_rows / 10);
            for (_, key) in &by_age[..excess] {
                model.remove(key);
            }
            expected_evicted = excess as u64;
        }
        let refs: Vec<(i64, &str, &str, &str)> = rows
            .iter()
            .map(|(id, value, op)| (*id, "n", value.as_str(), *op))
            .collect();
        assert_eq!(
            write(&s, batch(&refs)).await,
            expected_evicted,
            "step {step}"
        );
        let want: Vec<(i64, String)> = model.iter().map(|(k, (v, _))| (*k, v.clone())).collect();
        assert_eq!(values(&s), want, "step {step}");
        assert_eq!(s.query(0).unwrap().total, model.len(), "step {step}");
    }
    assert!(s.stats().evicted_rows_total > 0, "the run never evicted");
    assert!(
        s.layer_parts()[2] > 0,
        "the run never merged into the top layer"
    );
}
