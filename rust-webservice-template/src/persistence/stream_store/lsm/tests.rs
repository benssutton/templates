use super::*;
use arrow::array::{Array, StringArray};
use arrow::array::{AsArray, Int64Array};
use arrow::datatypes::Int64Type;
use arrow::datatypes::{DataType, Field};

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

fn store(flush_rows: usize, compaction_runs: usize) -> LsmStore {
    LsmStore::new(flush_rows, compaction_runs, vec!["id".into()])
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

/// Over the bound, shadowed versions and tombstones go first: rewriting
/// the same keys never evicts anything.
#[tokio::test]
async fn the_memory_bound_compacts_before_it_evicts() {
    let s = store(100, 100).with_max_rows(4);
    for round in 0..5 {
        let value = format!("v{round}");
        let evicted = s
            .ingest(batch(&[
                (1, "a", &value, "upsert"),
                (2, "b", &value, "upsert"),
            ]))
            .await
            .unwrap();
        assert_eq!(evicted, 0, "round {round}");
    }
    assert!(s.stats().rows <= 4, "{:?}", s.stats());
    assert_eq!(values(&s), [(1, "v4".to_owned()), (2, "v4".to_owned())]);
    assert_eq!(s.stats().evicted_rows_total, 0);
}

/// Distinct live keys beyond the bound: the oldest writes are evicted
/// and counted; the newest survive.
#[tokio::test]
async fn the_memory_bound_evicts_the_oldest_live_rows_and_counts_them() {
    let s = store(100, 100).with_max_rows(3);
    let mut evicted = 0;
    for id in 1..=5 {
        evicted += s.ingest(batch(&[(id, "n", "v", "upsert")])).await.unwrap();
    }
    assert_eq!(evicted, 2);
    assert_eq!(s.stats().evicted_rows_total, 2);
    assert_eq!(s.stats().max_rows, 3);
    let ids: Vec<i64> = values(&s).into_iter().map(|(id, _)| id).collect();
    assert_eq!(ids, [3, 4, 5]);
}

/// The merged view is computed once per published snapshot and reused.
#[tokio::test]
async fn repeated_queries_on_one_snapshot_reuse_the_merged_view() {
    let s = store(100, 4);
    s.ingest(batch(&[(1, "a", "v", "upsert")])).await.unwrap();
    let first = s.query(0).unwrap().rows.unwrap();
    let second = s.query(0).unwrap().rows.unwrap();
    assert!(
        first
            .column(0)
            .to_data()
            .ptr_eq(&second.column(0).to_data())
    );
    s.ingest(batch(&[(2, "b", "v", "upsert")])).await.unwrap();
    assert_eq!(
        s.query(0).unwrap().total,
        2,
        "a new snapshot is merged again"
    );
}

#[tokio::test]
async fn newest_version_wins_within_the_memtable() {
    let s = store(100, 4);
    s.ingest(batch(&[(1, "a", "v1", "upsert"), (1, "a", "v2", "upsert")]))
        .await
        .unwrap();
    s.ingest(batch(&[(1, "a", "v3", "upsert")])).await.unwrap();
    assert_eq!(values(&s), [(1, "v3".to_owned())]);
    assert_eq!(s.query(0).unwrap().total, 1);
}

/// Three batches with flush_rows=2,
/// compaction_runs=2: run1, run2 -> compaction (id 1 = v2), then a
/// memtable tombstone beats id 2 in the compacted run.
#[tokio::test]
async fn newest_wins_across_compaction_and_a_memtable_tombstone_shadows_a_run() {
    let s = store(2, 2);
    s.ingest(batch(&[
        (1, "alpha", "v1", "upsert"),
        (2, "beta", "v1", "upsert"),
    ]))
    .await
    .unwrap();
    assert_eq!((s.run_count(), s.memtable_rows()), (1, 0));
    s.ingest(batch(&[
        (1, "alpha", "v2", "upsert"),
        (3, "gamma", "v1", "upsert"),
    ]))
    .await
    .unwrap();
    assert_eq!(s.run_count(), 1, "two runs compacted into one");
    s.ingest(batch(&[(2, "beta", "v1", "delete")]))
        .await
        .unwrap();
    assert_eq!(s.memtable_rows(), 1);
    assert_eq!(values(&s), [(1, "v2".to_owned()), (3, "v1".to_owned())]);
}

#[tokio::test]
async fn compaction_drops_tombstones() {
    let s = store(1, 2);
    s.ingest(batch(&[(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")]))
        .await
        .unwrap();
    s.ingest(batch(&[(2, "b", "v1", "delete")])).await.unwrap();
    assert_eq!(s.query(100).unwrap().total, 1);
    assert_eq!(values(&s), [(1, "v1".to_owned())]);
    assert_eq!(
        s.run_rows(),
        1,
        "the tombstone for id 2 is physically reclaimed"
    );
}

#[tokio::test]
async fn a_tombstone_in_an_uncompacted_run_shadows_older_runs() {
    let s = store(1, 10);
    s.ingest(batch(&[(5, "x", "v1", "upsert")])).await.unwrap();
    s.ingest(batch(&[(5, "x", "v1", "delete")])).await.unwrap();
    assert_eq!(s.query(0).unwrap().total, 0);
    assert_eq!(
        (s.run_count(), s.run_rows()),
        (2, 2),
        "not reclaimed before compaction"
    );
}

#[tokio::test]
async fn reinsert_after_delete_wins_even_after_compaction() {
    let s = store(1, 2);
    for (value, op) in [("v1", "upsert"), ("v1", "delete"), ("v2", "upsert")] {
        s.ingest(batch(&[(500, "rk", value, op)])).await.unwrap();
    }
    assert_eq!(values(&s), [(500, "v2".to_owned())]);
}

#[tokio::test]
async fn flush_and_compaction_happen_at_their_thresholds() {
    let s = store(3, 2);
    s.ingest(batch(&[(1, "a", "v", "upsert"), (2, "b", "v", "upsert")]))
        .await
        .unwrap();
    assert_eq!(
        (s.run_count(), s.memtable_rows()),
        (0, 2),
        "below flush_rows"
    );
    s.ingest(batch(&[(3, "c", "v", "upsert")])).await.unwrap();
    assert_eq!(
        (s.run_count(), s.memtable_rows()),
        (1, 0),
        "flushed at flush_rows"
    );
    s.ingest(batch(&[
        (4, "d", "v", "upsert"),
        (5, "e", "v", "upsert"),
        (6, "f", "v", "upsert"),
    ]))
    .await
    .unwrap();
    assert_eq!(s.run_count(), 1, "compacted at compaction_runs");
    assert_eq!(s.query(0).unwrap().total, 6);
}

/// Limit 0 returns every row; a limit pages: rows come back sorted by key whatever the write order.
#[tokio::test]
async fn limit_zero_returns_every_row_and_a_limit_pages_in_key_order() {
    let s = store(100, 4);
    s.ingest(batch(&[
        (3, "c", "v", "upsert"),
        (1, "a", "v", "upsert"),
        (2, "b", "v", "upsert"),
    ]))
    .await
    .unwrap();
    let all = s.query(0).unwrap();
    assert_eq!((all.total, all.rows.unwrap().num_rows()), (3, 3));
    let page = s.query(2).unwrap();
    assert_eq!(page.total, 3);
    let rows = page.rows.unwrap();
    let ids = rows
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!(ids.values().to_vec(), [1, 2]);
}

#[tokio::test]
async fn query_drops_the_order_and_op_columns() {
    let s = store(100, 4);
    s.ingest(batch(&[(1, "a", "v", "upsert")])).await.unwrap();
    let rows = s.query(0).unwrap().rows.unwrap();
    let names: Vec<String> = rows
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["id", "name", "value"]);
}

#[tokio::test]
async fn an_empty_store_has_no_rows() {
    let result = store(100, 4).query(10).unwrap();
    assert!(result.rows.is_none());
    assert_eq!(result.total, 0);
}

#[tokio::test]
async fn a_batch_without_the_key_column_is_invalid_and_writes_nothing() {
    let s = store(100, 4);
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
    let s = store(100, 4);
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
    let s = store(100, 4);
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
    let s = store(1, 2);
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
    let s = store(100, 4);
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
    let s = store(1, 2);
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
    assert_eq!((s.run_count(), s.memtable_rows()), (0, 0));
}

/// On an empty store the slice's first batch sets the schema the rest
/// must match, so batches that disagree with each other are refused.
#[tokio::test]
async fn validate_rejects_a_slice_whose_batches_disagree_on_an_empty_store() {
    let s = store(100, 4);
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
    let s = store(100, 4);
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
    let s = store(1, 2);
    let slice = [
        batch(&[(1, "a", "v1", "upsert")]),
        batch(&[(1, "a", "v1", "delete")])
            .project(&[3, 2, 1, 0])
            .unwrap(),
    ];
    s.validate(&slice).unwrap();
    assert_eq!(s.query(0).unwrap().total, 0);
    assert_eq!((s.run_count(), s.memtable_rows()), (0, 0));
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
    let s = LsmStore::new(100, 4, vec!["id".into(), "version".into()]);
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
    let s = store(100, 4);
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
    let s = store(100, 4);
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
    let s = store(1, 2);
    s.ingest(batch(&[(1, "a", "v1", "upsert")])).await.unwrap();
    let reordered = batch(&[(1, "a", "v2", "upsert")])
        .project(&[3, 2, 1, 0])
        .unwrap();
    s.ingest(reordered).await.unwrap();
    assert_eq!(values(&s), [(1, "v2".to_owned())]);
}

/// Snapshot isolation under a concurrent write: every read sees
/// exactly the rows of some prefix of the writes, never a half-applied
/// flush or compaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readers_see_a_consistent_snapshot_while_the_writer_flushes_and_compacts() {
    let s = Arc::new(store(7, 3));
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
    assert!(s.run_count() <= 3);
}
