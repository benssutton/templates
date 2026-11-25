//! In-memory layered LSM store for Arrow record batches (the root README's
//! "Stream store (LSM)" decision).
//!
//! Supersede on write: a writer-private index maps every key to the part
//! and row holding its live version. A newer version, or a delete, clears
//! the old row's bit in that part's `live` bitmap; deletes are never stored.
//! So reads never deduplicate and merges only drop dead rows.
//!
//! Layers: each incoming batch becomes one part in layer 0, sorted by key.
//! When layer `i < L-1` holds `P` parts they are merged (live rows only)
//! into one part appended to layer `i+1`. The top layer is never merged: it
//! keeps accepting parts, so overflow happens only there. With batches
//! counted as they arrive:
//! - coverage: a layer-`i` part covers at least `P^i` batches;
//! - lower layers: at most `(L-1)(P-1)` parts sit below the top layer;
//! - top layer: at most `floor(n / P^(L-1))` parts after `n` batches;
//! - write amplification: every row is written at most `L` times.
//!
//! Coverage and the top-layer count are exact until a batch stores nothing
//! (all deletes) or a part is dropped because all its rows died: either way
//! that layer fills more slowly. A failed merge is retried only once its
//! layer has gained a part, so the lower-layer bound waits for it.
//!
//! Parts do not store `op`: every stored row is an upsert, since a delete
//! only clears a bit.
//!
//! Retention: over `max_rows` live rows, the oldest rows (lowest seqno) are
//! marked dead down to 90% of the cap. A part with no live rows is dropped.
//! Dead rows in a part stay in memory until the part merges or dies.
//!
//! Reads: `query` k-way merges the sorted parts and stops at `limit`; the
//! total is a maintained count. `live_batches` hands query engines every
//! part with a `_live` column, zero-copy.
//!
//! Concurrency: one writer (a `tokio::sync::Mutex`; the CPU work runs on the
//! blocking pool). Readers load an immutable snapshot from an `ArcSwap` and
//! never block.

mod kernels;
mod part;
mod read;
#[cfg(test)]
mod tests;

use super::{QueryResult, StoreError, StreamStore};
use crate::schemas::health::StreamStoreStats;
use crate::settings::Settings;
use arc_swap::ArcSwap;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::interleave_record_batch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::row::RowConverter;
use kernels::{conform, drop_columns, key_converter, key_rows, merge_parts, prepare, quoted_list};
pub use part::LIVE_COLUMN;
use part::Part;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Monotonic per-row write order.
pub const ORDER_COLUMN: &str = "seqno";
/// Upsert/delete marker column (`"upsert"` | `"delete"`).
pub const OP_COLUMN: &str = "op";
pub const DELETE_OP: &str = "delete";

/// The store's shape; see the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmConfig {
    /// `L`, at least 2.
    pub layers: usize,
    /// `P`, at least 2.
    pub parts_per_layer: usize,
    /// Live rows held; over it the oldest are evicted down to 90%.
    pub max_rows: usize,
    /// The merge key: the single extension point for a composite key
    /// (`["id", "version"]`).
    pub key_columns: Vec<String>,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            layers: 4,
            parts_per_layer: 8,
            max_rows: usize::MAX,
            key_columns: vec!["id".into()],
        }
    }
}

impl LsmConfig {
    pub fn from_settings(s: &Settings) -> Self {
        Self {
            layers: s.lsm_layers,
            parts_per_layer: s.lsm_parts_per_layer,
            max_rows: s.lsm_max_rows,
            key_columns: s.lsm_key_columns.clone(),
        }
    }
}

/// Where the live version of a key is held.
#[derive(Debug, Clone, Copy)]
struct Loc {
    part: u64,
    row: usize,
}

/// What readers see: immutable, swapped atomically after every write.
#[derive(Debug, Default)]
struct Snapshot {
    /// Every part, oldest data first: the top layer, then down to layer 0,
    /// each layer oldest part first.
    parts: Vec<Part>,
    /// Parts per layer, layer 0 first.
    layer_parts: Vec<usize>,
    /// The fixed schema, so `validate` reads it without the writer lock.
    schema: Option<SchemaRef>,
    live_rows: usize,
    /// Rows physically held, dead ones included.
    held_rows: usize,
}

/// Writer-private state, guarded by the writer mutex.
#[derive(Debug, Default)]
struct Writer {
    seqno: u64,
    next_part: u64,
    /// Fixed by the first non-empty batch: data columns (nullable) + `seqno`.
    schema: Option<SchemaRef>,
    /// Encodes keys for the index and for ordered reads; fixed with the schema.
    converter: Option<RowConverter>,
    /// `layers[0]` is layer 0; each layer's parts oldest first.
    layers: Vec<Vec<Part>>,
    /// Every live key (row-format bytes) -> its row. Absent once deleted or
    /// evicted. Looked up by `Row` bytes, so only a new key allocates.
    index: HashMap<Box<[u8]>, Loc>,
    /// Per layer: the part count at which its merge last failed. The merge
    /// is retried only once the layer holds more parts than that.
    merge_failed_at: Vec<Option<usize>>,
}

impl Writer {
    fn new(layers: usize) -> Self {
        Self {
            layers: vec![Vec::new(); layers],
            merge_failed_at: vec![None; layers],
            ..Self::default()
        }
    }

    fn next_part_id(&mut self) -> u64 {
        let id = self.next_part;
        self.next_part += 1;
        id
    }

    fn live_rows(&self) -> usize {
        self.layers.iter().flatten().map(Part::live_rows).sum()
    }

    /// Clear rows (part id -> rows), copying each touched bitmap once, then
    /// drop every part left with no live rows.
    fn kill(&mut self, dead: &HashMap<u64, Vec<usize>>) {
        if dead.is_empty() {
            return;
        }
        for part in self.layers.iter_mut().flatten() {
            if let Some(rows) = dead.get(&part.id) {
                part.kill(rows);
            }
        }
        for layer in &mut self.layers {
            layer.retain(|p| p.live_rows() > 0);
        }
    }
}

pub struct LsmStore {
    core: Arc<Core>,
    /// Shared so `ingest` can move the owned guard into its blocking task.
    writer: Arc<tokio::sync::Mutex<Writer>>,
}

/// Configuration, the published snapshot and counters: everything the
/// blocking write task needs besides the writer state.
struct Core {
    config: LsmConfig,
    snapshot: ArcSwap<Snapshot>,
    evicted_total: AtomicU64,
    /// Rows written into parts, merges included. Over the rows writes stored,
    /// this is the write amplification.
    written_total: AtomicU64,
    merge_failures_total: AtomicU64,
    /// Set when a write panicked. The writer mutex does not poison, and a
    /// panic can leave the index and bitmaps half-updated, so every later
    /// write is refused.
    poisoned: AtomicBool,
    /// Makes every merge fail, to test the retry rule.
    #[cfg(test)]
    fail_merges: AtomicBool,
    /// Makes the next write panic once it has changed the index.
    #[cfg(test)]
    panic_next_write: AtomicBool,
}

impl LsmStore {
    /// Values below their minimum are raised to it: `layers` and
    /// `parts_per_layer` to 2, `max_rows` to 1; no key columns means `["id"]`.
    pub fn new(config: LsmConfig) -> Self {
        let config = LsmConfig {
            layers: config.layers.max(2),
            parts_per_layer: config.parts_per_layer.max(2),
            max_rows: config.max_rows.max(1),
            key_columns: if config.key_columns.is_empty() {
                vec!["id".into()]
            } else {
                config.key_columns
            },
        };
        let layers = config.layers;
        Self {
            core: Arc::new(Core {
                config,
                snapshot: ArcSwap::from_pointee(Snapshot {
                    layer_parts: vec![0; layers],
                    ..Snapshot::default()
                }),
                evicted_total: AtomicU64::new(0),
                written_total: AtomicU64::new(0),
                merge_failures_total: AtomicU64::new(0),
                poisoned: AtomicBool::new(false),
                #[cfg(test)]
                fail_merges: AtomicBool::new(false),
                #[cfg(test)]
                panic_next_write: AtomicBool::new(false),
            }),
            writer: Arc::new(tokio::sync::Mutex::new(Writer::new(layers))),
        }
    }

    pub fn from_settings(s: &Settings) -> Self {
        Self::new(LsmConfig::from_settings(s))
    }

    /// Parts per layer, layer 0 first.
    pub fn layer_parts(&self) -> Vec<usize> {
        self.core.snapshot.load().layer_parts.clone()
    }

    /// Every part as an Arrow batch with a `_live` boolean column
    /// (`LIVE_COLUMN`), oldest data first. The data columns are the store's
    /// own buffers, so this copies nothing. Filter on `_live` for the current
    /// rows: Polars and DataFusion push that filter into their scan.
    pub fn live_batches(&self) -> Result<Vec<RecordBatch>, StoreError> {
        let snap = self.core.snapshot.load_full();
        snap.parts
            .iter()
            .map(|p| p.with_live_column().map_err(StoreError::from))
            .collect()
    }

    #[cfg(test)]
    fn rows_written(&self) -> u64 {
        self.core.written_total.load(Ordering::Relaxed)
    }
}

impl Core {
    fn check_required(&self, schema: &Schema) -> Result<(), StoreError> {
        let missing: Vec<&str> = self
            .config
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

    /// Every rule a batch must pass before it is written, shared by
    /// `validate` and `ingest`: the key and `op` columns are present, the
    /// batch conforms to `fixed` (the store's schema, if one is fixed), and a
    /// non-empty batch that would fix the schema can go through the write
    /// kernels. Returns the conformed frame schema and data columns.
    fn check(
        &self,
        batch: &RecordBatch,
        fixed: Option<&SchemaRef>,
    ) -> Result<(SchemaRef, Vec<ArrayRef>), StoreError> {
        // Validate up front: an un-keyed frame would only fail later, inside
        // a merge, poisoning every subsequent write.
        self.check_required(&batch.schema())?;
        // `live_batches` appends `_live`; a data column of that name would
        // shadow the store's bitmap in every query engine.
        if batch.schema().column_with_name(LIVE_COLUMN).is_some() {
            return Err(StoreError::InvalidBatch(format!(
                "batch uses reserved column names: {}",
                quoted_list(&[LIVE_COLUMN])
            )));
        }
        let (schema, columns) = conform(batch, fixed)?;
        let rows = batch.num_rows();
        if fixed.is_none() && rows > 0 {
            // This batch fixes the column types. Run the write kernels on it
            // once, so a key or `op` type they cannot handle is refused now
            // instead of failing every later write and merge.
            let keys = &self.config.key_columns;
            let mut trial = columns.clone();
            trial.push(Arc::new(UInt64Array::from_iter_values(0..rows as u64)) as ArrayRef);
            let frame = RecordBatch::try_new(schema.clone(), trial)?;
            let run = || -> Result<(), StoreError> {
                prepare(&frame, keys, &key_converter(&schema, keys)?)?;
                Ok(())
            };
            run().map_err(|e| StoreError::InvalidBatch(format!("batch cannot be merged: {e}")))?;
        }
        Ok((schema, columns))
    }

    /// `apply`, refused once poisoned. A panic in it poisons the store
    /// before the writer lock is released, so no queued write runs on the
    /// state it left behind.
    fn write(&self, w: &mut Writer, batch: &RecordBatch) -> Result<u64, StoreError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(StoreError::Internal(
                "LSM store poisoned by a panicked write".into(),
            ));
        }
        std::panic::catch_unwind(AssertUnwindSafe(|| self.apply(w, batch))).unwrap_or_else(|_| {
            self.poisoned.store(true, Ordering::Release);
            tracing::error!("LSM write panicked; the store refuses every later write");
            Err(StoreError::Internal(
                "LSM write panicked; the store is poisoned".into(),
            ))
        })
    }

    /// The CPU half of `ingest`, run on the blocking pool with the writer
    /// lock held. Returns the number of live rows evicted by retention.
    fn apply(&self, w: &mut Writer, batch: &RecordBatch) -> Result<u64, StoreError> {
        let keys = &self.config.key_columns;
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
        // Every fallible step runs before the first change to the writer, so
        // a refused or failed write leaves the store as it was. The first
        // write's converter is kept only once the write has gone through.
        let mut fresh = None;
        let converter = match &w.converter {
            Some(converter) => converter,
            None => fresh.insert(key_converter(&schema, keys)?),
        };
        let prepared = prepare(&frame, keys, converter)?;
        if let Some(converter) = fresh {
            w.converter = Some(converter);
        }
        w.schema = Some(schema);
        w.seqno += rows as u64;

        // Supersede: the previous version of every key written here dies; a
        // delete is not stored.
        let part_id = w.next_part_id();
        let mut dead: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut stored_row = 0;
        for &(i, is_delete) in &prepared.newest {
            let key = prepared.key(i);
            let old = if is_delete {
                w.index.remove(key.as_ref())
            } else {
                let new = Loc {
                    part: part_id,
                    row: stored_row,
                };
                stored_row += 1;
                match w.index.get_mut(key.as_ref()) {
                    Some(loc) => Some(std::mem::replace(loc, new)),
                    None => w.index.insert(key.as_ref().into(), new),
                }
            };
            if let Some(old) = old {
                dead.entry(old.part).or_default().push(old.row);
            }
        }
        w.kill(&dead);
        #[cfg(test)]
        if self.panic_next_write.swap(false, Ordering::Relaxed) {
            panic!("injected write panic");
        }
        let stored = prepared.stored.num_rows();
        if stored > 0 {
            w.layers[0].push(Part::new(part_id, prepared.stored, prepared.stored_keys));
            self.written_total
                .fetch_add(stored as u64, Ordering::Relaxed);
        }
        self.cascade(w);
        let evicted = self.evict(w);
        self.publish(w);
        Ok(evicted)
    }

    /// Merge every lower layer that holds `P` parts into the next one up.
    /// A failed merge loses nothing (the parts stay where they are) and is
    /// counted. It is retried only once the layer has gained a part since:
    /// a deterministic failure (an offset overflow in a huge layer, say)
    /// would otherwise re-copy the whole layer on every write.
    fn cascade(&self, w: &mut Writer) {
        let top = w.layers.len() - 1;
        for layer in 0..top {
            let parts = w.layers[layer].len();
            if parts < self.config.parts_per_layer {
                w.merge_failed_at[layer] = None;
                continue;
            }
            if w.merge_failed_at[layer].is_some_and(|failed| parts <= failed) {
                continue;
            }
            match self.merge_layer(w, layer) {
                Ok(()) => w.merge_failed_at[layer] = None,
                Err(err) => {
                    w.merge_failed_at[layer] = Some(parts);
                    self.merge_failures_total.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        error = %err,
                        layer,
                        parts,
                        "LSM merge failed; retried once the layer gains a part"
                    );
                }
            }
        }
    }

    fn merge_layer(&self, w: &mut Writer, layer: usize) -> Result<(), StoreError> {
        #[cfg(test)]
        if self.fail_merges.load(Ordering::Relaxed) {
            return Err(StoreError::Internal("injected merge failure".into()));
        }
        let keys = &self.config.key_columns;
        let merged = merge_parts(&w.layers[layer], keys)?;
        let merged_keys = match &w.converter {
            Some(converter) => key_rows(converter, &merged, keys)?,
            None => {
                return Err(StoreError::Internal(
                    "merge before the schema is fixed".into(),
                ));
            }
        };
        let id = w.next_part_id();
        w.layers[layer].clear();
        let rows = merged.num_rows();
        if rows > 0 {
            // Every merged row is live, so its key is indexed already: move
            // the entry, allocating nothing.
            for row in 0..rows {
                let indexed = w.index.get_mut(merged_keys.row(row).as_ref());
                debug_assert!(indexed.is_some(), "a live row was not indexed");
                if let Some(loc) = indexed {
                    *loc = Loc { part: id, row };
                }
            }
            self.written_total.fetch_add(rows as u64, Ordering::Relaxed);
            w.layers[layer + 1].push(Part::new(id, merged, merged_keys));
        }
        Ok(())
    }

    /// Over `max_rows` live rows, mark the oldest dead down to 90% of the
    /// cap. Parts are visited oldest data first (top layer down to layer 0,
    /// each layer oldest part first); every part covers a contiguous run of
    /// writes, so this takes the globally oldest rows.
    fn evict(&self, w: &mut Writer) -> u64 {
        let max = self.config.max_rows;
        let live = w.live_rows();
        if live <= max {
            return 0;
        }
        let mut excess = live - (max - max / 10);
        let mut dead: HashMap<u64, Vec<usize>> = HashMap::new();
        let Writer { layers, index, .. } = &mut *w;
        for part in layers.iter().rev().flatten() {
            if excess == 0 {
                break;
            }
            let rows = part.oldest_live_rows(excess);
            for &row in &rows {
                index.remove(part.key(row).as_ref());
            }
            excess -= rows.len();
            dead.insert(part.id, rows);
        }
        let evicted = dead.values().map(Vec::len).sum::<usize>() as u64;
        w.kill(&dead);
        self.evicted_total.fetch_add(evicted, Ordering::Relaxed);
        tracing::warn!(
            evicted,
            max_rows = max,
            "LSM store over its live-row cap; evicted the oldest rows"
        );
        evicted
    }

    fn publish(&self, w: &Writer) {
        let parts: Vec<Part> = w.layers.iter().rev().flatten().cloned().collect();
        self.snapshot.store(Arc::new(Snapshot {
            live_rows: parts.iter().map(Part::live_rows).sum(),
            held_rows: parts.iter().map(Part::rows).sum(),
            layer_parts: w.layers.iter().map(Vec::len).collect(),
            schema: w.schema.clone(),
            parts,
        }));
    }
}

#[async_trait::async_trait]
impl StreamStore for LsmStore {
    /// The writer lock is taken here, on the async side; only the CPU work
    /// (`Core::write`) runs on the blocking pool, so no async worker is held
    /// by a merge. The owned guard moves into the task, so the commit stays
    /// atomic and writes stay serialised. The task cannot be cancelled: once
    /// started, a write commits even if this future is dropped.
    async fn ingest(&self, batch: RecordBatch) -> Result<u64, StoreError> {
        let mut guard = self.writer.clone().lock_owned().await;
        let core = self.core.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || span.in_scope(|| core.write(&mut guard, &batch)))
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
        if snap.live_rows == 0 {
            return Ok(QueryResult {
                rows: None,
                total: 0,
            });
        }
        let picks = read::ordered_live(&snap.parts, limit);
        // Project first, so the interleave copies only the columns served.
        let data = snap
            .parts
            .iter()
            .map(|p| drop_columns(&p.batch, &[ORDER_COLUMN]))
            .collect::<Result<Vec<_>, _>>()?;
        let batches: Vec<&RecordBatch> = data.iter().collect();
        Ok(QueryResult {
            rows: Some(interleave_record_batch(&batches, &picks)?),
            total: snap.live_rows,
        })
    }

    fn stats(&self) -> StreamStoreStats {
        let snap = self.core.snapshot.load();
        StreamStoreStats {
            rows: snap.held_rows as u64,
            live_rows: snap.live_rows as u64,
            parts: snap.parts.len() as u64,
            max_rows: self.core.config.max_rows as u64,
            evicted_rows_total: self.core.evicted_total.load(Ordering::Relaxed),
            merge_failures_total: self.core.merge_failures_total.load(Ordering::Relaxed),
            poisoned: self.core.poisoned.load(Ordering::Acquire),
        }
    }
}
