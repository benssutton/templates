//! In-memory LSM (Log-Structured Merge) store for Arrow record batches, on
//! arrow-rs compute kernels.
//!
//! Single logical writer: `ingest` holds a `tokio::sync::Mutex` over the
//! writer state for the whole write, so the ingest task and the HTTP ingest
//! path are serialised. The lock is awaited on the async side and the CPU
//! work (merge, flush, compaction, eviction) runs on the blocking pool.
//! Readers load an immutable `Arc<Snapshot>` from an `ArcSwap` and never
//! block; every write publishes a new snapshot at its end.
//!
//! Lifecycle: memtable -> sorted run (flush at `flush_rows`) -> one compacted
//! run (once there are `compaction_runs` runs). Compaction is the only point
//! at which delete tombstones are reclaimed; until then a tombstone shadows
//! older versions of its key in every frame.
//!
//! Memory bound: once the store holds more than `max_rows` rows, the writer
//! first compacts everything (dropping shadowed versions and tombstones,
//! which loses nothing) and only then evicts the oldest live rows. Evicted
//! rows are counted.
//!
//! Reads: the merged, tombstone-free view of a snapshot is built on the
//! first query and cached on the snapshot, so later queries of the same
//! snapshot only slice it. A write publishes a new snapshot with an empty
//! cache.

use super::{QueryResult, StoreError, StreamStore};
use crate::schemas::health::StreamStoreStats;
use crate::settings::Settings;
use arc_swap::ArcSwap;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{Schema, SchemaRef};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Monotonic per-row write order.
pub const ORDER_COLUMN: &str = "seqno";
/// Upsert/delete marker column (`"upsert"` | `"delete"`).
pub const OP_COLUMN: &str = "op";
pub const DELETE_OP: &str = "delete";

/// The live rows of a snapshot, sorted by key, order and op columns dropped.
type MergedView = Result<Option<RecordBatch>, String>;

/// What readers see: immutable, swapped atomically after every write.
#[derive(Debug, Default)]
struct Snapshot {
    runs: Vec<RecordBatch>,
    memtable: Vec<RecordBatch>,
    /// The writer's fixed schema, so `validate` reads it without the lock.
    schema: Option<SchemaRef>,
    rows: usize,
    merged: OnceLock<MergedView>,
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

impl Writer {
    fn rows(&self) -> usize {
        self.memtable_rows + self.runs.iter().map(RecordBatch::num_rows).sum::<usize>()
    }
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
    max_rows: usize,
    key_columns: Vec<String>,
    snapshot: ArcSwap<Snapshot>,
    evicted_total: AtomicU64,
}

impl LsmStore {
    /// `key_columns` is the merge partition key: the single extension point
    /// for a composite key (`["id", "version"]`). Unbounded until
    /// `with_max_rows`.
    pub fn new(flush_rows: usize, compaction_runs: usize, key_columns: Vec<String>) -> Self {
        Self {
            core: Arc::new(Core {
                flush_rows: flush_rows.max(1),
                compaction_runs: compaction_runs.max(1),
                max_rows: usize::MAX,
                key_columns: if key_columns.is_empty() {
                    vec!["id".into()]
                } else {
                    key_columns
                },
                snapshot: ArcSwap::from_pointee(Snapshot::default()),
                evicted_total: AtomicU64::new(0),
            }),
            writer: Arc::new(tokio::sync::Mutex::new(Writer::default())),
        }
    }

    /// Bound the rows held; see the module docs for the policy.
    pub fn with_max_rows(self, max_rows: usize) -> Self {
        let core = Core {
            flush_rows: self.core.flush_rows,
            compaction_runs: self.core.compaction_runs,
            max_rows: max_rows.max(1),
            key_columns: self.core.key_columns.clone(),
            snapshot: ArcSwap::from_pointee(Snapshot::default()),
            evicted_total: AtomicU64::new(0),
        };
        Self {
            core: Arc::new(core),
            writer: self.writer,
        }
    }

    pub fn from_settings(s: &Settings) -> Self {
        Self::new(
            s.lsm_flush_rows,
            s.lsm_compaction_runs,
            s.lsm_key_columns.clone(),
        )
        .with_max_rows(s.lsm_max_rows)
    }

    #[cfg(test)]
    fn run_count(&self) -> usize {
        self.core.snapshot.load().runs.len()
    }

    /// Rows physically retained across the runs (memtable excluded): the
    /// only observable trace of tombstone reclamation.
    #[cfg(test)]
    fn run_rows(&self) -> usize {
        self.core
            .snapshot
            .load()
            .runs
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    #[cfg(test)]
    fn memtable_rows(&self) -> usize {
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
                quoted_list(&missing)
            )))
        }
    }

    fn publish(&self, w: &Writer) {
        self.snapshot.store(Arc::new(Snapshot {
            runs: w.runs.clone(),
            memtable: w.memtable.clone(),
            schema: w.schema.clone(),
            rows: w.rows(),
            merged: OnceLock::new(),
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
    /// lock held: check, build, flush, compact, bound, commit and publish.
    /// Returns the number of live rows evicted by the memory bound.
    fn apply(&self, w: &mut Writer, batch: &RecordBatch) -> Result<u64, StoreError> {
        // Re-checked under the lock: the schema may have been fixed since a
        // caller's `validate`.
        let (schema, mut columns) = self.check(batch, w.schema.as_ref())?;
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
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
        let held = memtable_rows + runs.iter().map(RecordBatch::num_rows).sum::<usize>();
        let mut evicted = 0;
        if held > self.max_rows {
            if memtable_rows > 0 {
                runs.push(sorted_run(&schema, &memtable, &self.key_columns)?);
                memtable.clear();
                memtable_rows = 0;
            }
            runs = compact(&runs, &self.key_columns)?;
            if let Some(run) = runs.pop() {
                let (kept, dropped) = keep_newest(&run, self.max_rows, &self.key_columns)?;
                evicted = dropped as u64;
                if kept.num_rows() > 0 {
                    runs.push(kept);
                }
            }
        }
        w.schema = Some(schema);
        w.seqno += rows as u64;
        w.memtable = memtable;
        w.memtable_rows = memtable_rows;
        w.runs = runs;
        self.publish(w);
        if evicted > 0 {
            self.evicted_total.fetch_add(evicted, Ordering::Relaxed);
            tracing::warn!(
                evicted,
                max_rows = self.max_rows,
                "LSM store over its row bound after compaction; evicted the oldest rows"
            );
        }
        Ok(evicted)
    }

    fn merged_view(&self, snap: &Snapshot) -> MergedView {
        let frames: Vec<RecordBatch> = snap
            .runs
            .iter()
            .chain(snap.memtable.iter())
            .cloned()
            .collect();
        let merge_live = || -> Result<Option<RecordBatch>, StoreError> {
            let Some(winners) = merge(&frames, &self.key_columns)? else {
                return Ok(None);
            };
            let live_rows = live(&winners)?;
            Ok(Some(drop_columns(&live_rows, &[ORDER_COLUMN, OP_COLUMN])?))
        };
        merge_live().map_err(|e| e.to_string())
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
    async fn ingest(&self, batch: RecordBatch) -> Result<u64, StoreError> {
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
        let merged = snap
            .merged
            .get_or_init(|| self.core.merged_view(&snap))
            .clone()
            .map_err(StoreError::Internal)?;
        let Some(live_rows) = merged else {
            return Ok(QueryResult {
                rows: None,
                total: 0,
            });
        };
        let total = live_rows.num_rows();
        let page = if limit > 0 && limit < total {
            live_rows.slice(0, limit)
        } else {
            live_rows
        };
        Ok(QueryResult {
            rows: Some(page),
            total,
        })
    }

    fn stats(&self) -> StreamStoreStats {
        StreamStoreStats {
            rows: self.core.snapshot.load().rows as u64,
            max_rows: self.core.max_rows as u64,
            evicted_rows_total: self.core.evicted_total.load(Ordering::Relaxed),
        }
    }
}

mod kernels;
pub use kernels::{compact, live, merge};
use kernels::{conform, drop_columns, keep_newest, quoted_list, sorted_run};

#[cfg(test)]
mod tests;
