//! In-memory LSM (Log-Structured Merge) store for Arrow record batches: a
//! port of python persistence/stream_store/lsm_store.py onto arrow-rs kernels.
//!
//! Single logical writer: `ingest` holds a `tokio::sync::Mutex` over the
//! writer state for the whole write, so the ingest task and the HTTP ingest
//! path are serialised. The lock is awaited on the async side and the CPU
//! work (merge, flush, compaction) runs on the blocking pool. Readers load an immutable `Arc<Snapshot>` from an
//! `ArcSwap` and never block; every write publishes a new snapshot at its end.
//!
//! Lifecycle: memtable -> sorted run (flush at `flush_rows`) -> one compacted
//! run (once there are `compaction_runs` runs). Compaction is the only point
//! at which delete tombstones are reclaimed; until then a tombstone shadows
//! older versions of its key in every frame.

use super::{QueryResult, StoreError, StreamStore};
use crate::settings::Settings;
use arc_swap::ArcSwap;
use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow::compute::kernels::cmp::neq;
use arrow::compute::{
    cast, concat_batches, filter_record_batch, lexsort_to_indices, take_record_batch, SortColumn,
    SortOptions,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use std::sync::Arc;

/// Monotonic per-row write order (Python `ORDER_COLUMN`).
pub const ORDER_COLUMN: &str = "seqno";
/// Upsert/delete marker column (`"upsert"` | `"delete"`).
pub const OP_COLUMN: &str = "op";
pub const DELETE_OP: &str = "delete";

/// What readers see: immutable, swapped atomically after every write.
#[derive(Debug, Default)]
struct Snapshot {
    runs: Vec<RecordBatch>,
    memtable: Vec<RecordBatch>,
    /// The writer's fixed schema, so `validate` reads it without the lock.
    schema: Option<SchemaRef>,
}

/// Writer-private working set, guarded by the writer mutex.
#[derive(Debug, Default)]
struct Writer {
    seqno: u64,
    /// Fixed by the first non-empty batch: data columns (nullable) + `seqno`.
    schema: Option<SchemaRef>,
    memtable: Vec<RecordBatch>,
    memtable_rows: usize,
    runs: Vec<RecordBatch>,
}

pub struct LsmStore {
    core: Arc<Core>,
    /// Shared so `ingest` can move the owned guard into its blocking task.
    writer: Arc<tokio::sync::Mutex<Writer>>,
}

/// Configuration and the published snapshot: everything the blocking write
/// task needs besides the writer state, so it can own an `Arc` of it.
struct Core {
    flush_rows: usize,
    compaction_runs: usize,
    key_columns: Vec<String>,
    snapshot: ArcSwap<Snapshot>,
}

impl LsmStore {
    /// `key_columns` is the merge partition key: Python's single extension
    /// point (`["id"]` today, `["id", "version"]` later with no other change).
    pub fn new(flush_rows: usize, compaction_runs: usize, key_columns: Vec<String>) -> Self {
        Self {
            core: Arc::new(Core {
                flush_rows: flush_rows.max(1),
                compaction_runs: compaction_runs.max(1),
                key_columns: if key_columns.is_empty() {
                    vec!["id".into()]
                } else {
                    key_columns
                },
                snapshot: ArcSwap::from_pointee(Snapshot::default()),
            }),
            writer: Arc::new(tokio::sync::Mutex::new(Writer::default())),
        }
    }

    pub fn from_settings(s: &Settings) -> Self {
        Self::new(
            s.lsm_flush_rows,
            s.lsm_compaction_runs,
            s.lsm_key_columns.clone(),
        )
    }

    /// Immutable runs in the current snapshot.
    pub fn run_count(&self) -> usize {
        self.core.snapshot.load().runs.len()
    }

    /// Rows physically retained across the runs (memtable excluded): the
    /// only observable trace of tombstone reclamation (Python's
    /// `_run_row_count` test helper).
    pub fn run_rows(&self) -> usize {
        self.core
            .snapshot
            .load()
            .runs
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    pub fn memtable_rows(&self) -> usize {
        self.core
            .snapshot
            .load()
            .memtable
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }
}

impl Core {
    fn check_required(&self, schema: &Schema) -> Result<(), StoreError> {
        let missing: Vec<&str> = self
            .key_columns
            .iter()
            .map(String::as_str)
            .chain([OP_COLUMN])
            .filter(|c| schema.column_with_name(c).is_none())
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(StoreError::InvalidBatch(format!(
                "batch missing required columns: {}",
                py_list(&missing)
            )))
        }
    }

    fn publish(&self, w: &Writer) {
        self.snapshot.store(Arc::new(Snapshot {
            runs: w.runs.clone(),
            memtable: w.memtable.clone(),
            schema: w.schema.clone(),
        }));
    }

    /// Every rule a batch must pass before it is written, shared by
    /// `validate` and `ingest`: the key and `op` columns are present, the
    /// batch conforms to `fixed` (the store's schema, if one is fixed), and a
    /// non-empty batch that would fix the schema can go through the merge
    /// kernels. Returns the conformed frame schema and data columns.
    fn check(
        &self,
        batch: &RecordBatch,
        fixed: Option<&SchemaRef>,
    ) -> Result<(SchemaRef, Vec<ArrayRef>), StoreError> {
        // Validate up front: an un-keyed frame would only fail later inside
        // compaction, poisoning every subsequent query.
        self.check_required(&batch.schema())?;
        let (schema, columns) = conform(batch, fixed)?;
        let rows = batch.num_rows();
        if fixed.is_none() && rows > 0 {
            // This batch fixes the column types. Run the merge kernels on it
            // once, so a key or `op` type they cannot handle is rejected now
            // instead of failing every later flush, compaction and query.
            let mut trial = columns.clone();
            trial.push(Arc::new(UInt64Array::from_iter_values(0..rows as u64)) as ArrayRef);
            let frame = RecordBatch::try_new(schema.clone(), trial)?;
            compact(std::slice::from_ref(&frame), &self.key_columns)
                .map_err(|e| StoreError::InvalidBatch(format!("batch cannot be merged: {e}")))?;
        }
        Ok((schema, columns))
    }

    /// The CPU half of `ingest`, run on the blocking pool with the writer
    /// lock held: check, build, flush, compact, commit and publish.
    fn apply(&self, w: &mut Writer, batch: &RecordBatch) -> Result<(), StoreError> {
        // Re-checked under the lock: HTTP callers never call `validate`, and
        // the schema may have been fixed since a caller's `validate`.
        let (schema, mut columns) = self.check(batch, w.schema.as_ref())?;
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let start = w.seqno;
        columns
            .push(Arc::new(UInt64Array::from_iter_values(start..start + rows as u64)) as ArrayRef);
        let frame = RecordBatch::try_new(schema.clone(), columns)?;
        // Build the next state aside and commit it only once every step has
        // succeeded: a failed flush or compaction leaves the writer untouched.
        let mut memtable = w.memtable.clone();
        memtable.push(frame);
        let mut memtable_rows = w.memtable_rows + rows;
        let mut runs = w.runs.clone();
        if memtable_rows >= self.flush_rows {
            runs.push(sorted_run(&schema, &memtable, &self.key_columns)?);
            memtable.clear();
            memtable_rows = 0;
            if runs.len() >= self.compaction_runs {
                // Full compaction collapses every run into one, so a tombstone
                // has finished shadowing older versions and is reclaimed. A
                // later re-insert of the same key carries a higher seqno and
                // wins anyway.
                runs = compact(&runs, &self.key_columns)?;
            }
        }
        w.schema = Some(schema);
        w.seqno += rows as u64;
        w.memtable = memtable;
        w.memtable_rows = memtable_rows;
        w.runs = runs;
        self.publish(w);
        Ok(())
    }
}

#[async_trait::async_trait]
impl StreamStore for LsmStore {
    /// The writer lock is taken here, on the async side; only the CPU work
    /// (`Core::apply`) runs on the blocking pool, so no async worker is held
    /// by a merge or compaction. The owned guard moves into the task, so the
    /// commit stays atomic and writes stay serialised. The task cannot be
    /// cancelled: once started, a write commits even if this future is
    /// dropped.
    async fn ingest(&self, batch: RecordBatch) -> Result<(), StoreError> {
        let mut guard = self.writer.clone().lock_owned().await;
        let core = self.core.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || span.in_scope(|| core.apply(&mut guard, &batch)))
            .await
            .map_err(|e| StoreError::Internal(format!("LSM write task failed: {e}")))?
    }

    fn validate(&self, batches: &[RecordBatch]) -> Result<(), StoreError> {
        // Lock-free: the fixed schema is read from the published snapshot.
        let mut fixed = self.core.snapshot.load().schema.clone();
        for batch in batches {
            let (schema, _) = self.core.check(batch, fixed.as_ref())?;
            // As in `ingest`, only a non-empty batch fixes the schema, so
            // later batches of the slice are checked against it.
            if fixed.is_none() && batch.num_rows() > 0 {
                fixed = Some(schema);
            }
        }
        Ok(())
    }

    fn query(&self, limit: usize) -> Result<QueryResult, StoreError> {
        let snap = self.core.snapshot.load_full(); // atomic, lock-free
        let frames: Vec<RecordBatch> = snap
            .runs
            .iter()
            .chain(snap.memtable.iter())
            .cloned()
            .collect();
        let Some(winners) = merge(&frames, &self.core.key_columns)? else {
            return Ok(QueryResult {
                rows: None,
                total: 0,
            });
        };
        let live_rows = live(&winners)?;
        let total = live_rows.num_rows();
        let page = if limit > 0 && limit < total {
            live_rows.slice(0, limit)
        } else {
            live_rows
        };
        Ok(QueryResult {
            rows: Some(drop_columns(&page, &[ORDER_COLUMN, OP_COLUMN])?),
            total,
        })
    }
}

/// Newest version of every key across `frames`, tombstones included, sorted
/// by key ascending. Python's `_merge_frame` ranks by seqno descending over
/// the key and keeps rank 1; here `lexsort_to_indices` sorts on (key asc,
/// seqno desc) and the first row of each run of equal keys is kept.
pub fn merge(
    frames: &[RecordBatch],
    key_columns: &[String],
) -> Result<Option<RecordBatch>, StoreError> {
    let Some(first) = frames.first() else {
        return Ok(None);
    };
    let combined = concat_batches(&first.schema(), frames)?;
    let sorted = sort_by_key_newest_first(&combined, key_columns)?;
    if sorted.num_rows() == 0 {
        return Ok(Some(sorted));
    }
    let keys = key_columns
        .iter()
        .map(|k| column(&sorted, k))
        .collect::<Result<Vec<_>, _>>()?;
    let converter = RowConverter::new(
        keys.iter()
            .map(|a| SortField::new(a.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(&keys)?;
    let first_of_key: BooleanArray = (0..sorted.num_rows())
        .map(|i| Some(i == 0 || rows.row(i) != rows.row(i - 1)))
        .collect();
    Ok(Some(filter_record_batch(&sorted, &first_of_key)?))
}

/// Drop tombstones (`op == "delete"`). A null `op` compares as null and is
/// dropped as well, like polars' `pl.col("op") != "delete"` filter.
pub fn live(batch: &RecordBatch) -> Result<RecordBatch, StoreError> {
    let op = cast(&column(batch, OP_COLUMN)?, &DataType::Utf8)?;
    let keep = neq(&op, &StringArray::new_scalar(DELETE_OP))?;
    Ok(filter_record_batch(batch, &keep)?)
}

/// Full compaction (Python `_compact`): every run merged into one, with
/// tombstones reclaimed. No run at all when nothing is live.
pub fn compact(
    runs: &[RecordBatch],
    key_columns: &[String],
) -> Result<Vec<RecordBatch>, StoreError> {
    match merge(runs, key_columns)? {
        None => Ok(Vec::new()),
        Some(merged) => {
            let kept = live(&merged)?;
            Ok(if kept.num_rows() > 0 {
                vec![kept]
            } else {
                Vec::new()
            })
        }
    }
}

/// The memtable as one run, sorted by (key asc, seqno desc).
fn sorted_run(
    schema: &SchemaRef,
    memtable: &[RecordBatch],
    keys: &[String],
) -> Result<RecordBatch, StoreError> {
    let run = concat_batches(schema, memtable)?;
    sort_by_key_newest_first(&run, keys)
}

fn sort_by_key_newest_first(
    batch: &RecordBatch,
    keys: &[String],
) -> Result<RecordBatch, StoreError> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let mut columns = keys
        .iter()
        .map(|k| {
            Ok(SortColumn {
                values: column(batch, k)?,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    columns.push(SortColumn {
        values: column(batch, ORDER_COLUMN)?,
        options: Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    });
    let indices = lexsort_to_indices(&columns, None)?;
    Ok(take_record_batch(batch, &indices)?)
}

fn column(batch: &RecordBatch, name: &str) -> Result<ArrayRef, StoreError> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| StoreError::Internal(format!("frame has no `{name}` column")))
}

fn drop_columns(batch: &RecordBatch, names: &[&str]) -> Result<RecordBatch, StoreError> {
    let keep: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !names.contains(&f.name().as_str()))
        .map(|(i, _)| i)
        .collect();
    Ok(batch.project(&keep)?)
}

/// The store's frame schema (data columns, all nullable, then `seqno`) and the
/// batch's data columns in that schema's order. The first batch fixes the
/// schema; later batches must match it by name and type (Deviation 5). An
/// incoming `seqno` column is ignored: Python's `with_columns` overwrites it.
/// Types are compared with `DataType` equality, so nested field names and
/// nullability are part of schema equality (top-level nullability is not).
/// A batch with duplicate column names is invalid: names address columns.
fn conform(
    batch: &RecordBatch,
    existing: Option<&SchemaRef>,
) -> Result<(SchemaRef, Vec<ArrayRef>), StoreError> {
    let incoming = batch.schema();
    let mut seen = std::collections::HashSet::new();
    let mut duplicates: Vec<&str> = Vec::new();
    for f in incoming.fields() {
        if !seen.insert(f.name().as_str()) && !duplicates.contains(&f.name().as_str()) {
            duplicates.push(f.name());
        }
    }
    if !duplicates.is_empty() {
        return Err(StoreError::InvalidBatch(format!(
            "batch has duplicate column names: {}",
            py_list(&duplicates)
        )));
    }
    let data: Vec<(&Field, &ArrayRef)> = incoming
        .fields()
        .iter()
        .map(|f| f.as_ref())
        .zip(batch.columns())
        .filter(|(f, _)| f.name() != ORDER_COLUMN)
        .collect();
    let Some(schema) = existing else {
        let mut fields: Vec<Field> = data
            .iter()
            .map(|(f, _)| Field::new(f.name(), f.data_type().clone(), true))
            .collect();
        fields.push(Field::new(ORDER_COLUMN, DataType::UInt64, false));
        let columns = data.into_iter().map(|(_, c)| c.clone()).collect();
        return Ok((Arc::new(Schema::new(fields)), columns));
    };
    let expected = &schema.fields()[..schema.fields().len() - 1]; // every field but seqno
    if data.len() != expected.len() {
        return Err(mismatch(schema, &incoming));
    }
    let mut columns = Vec::with_capacity(expected.len());
    for field in expected {
        match data.iter().find(|(f, _)| f.name() == field.name()) {
            Some((f, c)) if f.data_type() == field.data_type() => columns.push((*c).clone()),
            _ => return Err(mismatch(schema, &incoming)),
        }
    }
    Ok((schema.clone(), columns))
}

fn mismatch(store: &Schema, batch: &Schema) -> StoreError {
    let describe = |s: &Schema| {
        s.fields()
            .iter()
            .filter(|f| f.name() != ORDER_COLUMN)
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    StoreError::InvalidBatch(format!(
        "batch schema does not match the store: expected [{}], got [{}]",
        describe(store),
        describe(batch)
    ))
}

/// Python's list repr, for message parity: `['id', 'op']`.
fn py_list(names: &[&str]) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    format!("[{}]", quoted.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::Int64Type;

    /// Python `tests/publishers/batches.py::make_batch`: rows of (id, name, value, op).
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

    /// Python tests/test_solace_ingest.py's three batches with flush_rows=2,
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

    /// Python test_lsm_store.py::test_compaction_drops_tombstones.
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

    /// Python test_http_ingest.py::test_reinsert_after_delete_wins_over_http.
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

    /// Python test_http_ingest.py::test_lsm_query_limit_zero_returns_all_rows,
    /// plus paging: rows come back sorted by key whatever the write order.
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

    /// Python test_lsm_store.py::test_ingest_rejects_batch_missing_key_column.
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

    /// Python test_lsm_store.py::test_ingest_rejects_batch_missing_op_column.
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

    /// Deviation 5: once the first batch fixes the schema, a conflicting one
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

    /// Python's `with_columns` overwrites an incoming `seqno`: the store's own
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

    /// Python stream_ingest's all-or-nothing check: a message whose second
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

    /// Python's `key_columns` extension point: with `["id", "version"]` the
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

    /// Null keys form one key (polars `over` groups nulls together): the
    /// newer null-id row wins, and nulls sort first, as polars' `sort` does.
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

    /// Spec §6 "snapshot isolation under a concurrent write": every read sees
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
}
