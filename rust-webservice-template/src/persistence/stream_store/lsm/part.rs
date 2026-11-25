//! One part of the layered store: an immutable batch sorted by key, the
//! encoded keys, and a copy-on-write bitmap of which rows are still live.

use super::ORDER_COLUMN;
use arrow::array::{AsArray, BooleanArray, BooleanBufferBuilder, RecordBatch};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, UInt64Type};
use arrow::error::ArrowError;
use arrow::row::{Row, Rows};
use std::sync::Arc;

/// The boolean column `LsmStore::live_batches` appends: true for live rows.
pub const LIVE_COLUMN: &str = "_live";

#[derive(Debug, Clone)]
pub(super) struct Part {
    /// Unique per store; the index locates rows by it.
    pub(super) id: u64,
    /// Data columns plus `seqno`, sorted by key. Never changes.
    pub(super) batch: RecordBatch,
    /// Every row's key in the writer's row format: ordered reads compare
    /// them, merges re-index with them.
    keys: Arc<Rows>,
    /// Bit `i` is set while row `i` is the live version of its key.
    pub(super) live: BooleanBuffer,
    pub(super) live_rows: usize,
}

impl Part {
    /// A part whose rows are all live.
    pub(super) fn new(id: u64, batch: RecordBatch, keys: Rows) -> Self {
        let rows = batch.num_rows();
        Self {
            id,
            batch,
            keys: Arc::new(keys),
            live: BooleanBuffer::new_set(rows),
            live_rows: rows,
        }
    }

    pub(super) fn rows(&self) -> usize {
        self.batch.num_rows()
    }

    pub(super) fn key(&self, row: usize) -> Row<'_> {
        self.keys.row(row)
    }

    pub(super) fn is_live(&self, row: usize) -> bool {
        self.live.value(row)
    }

    /// Clear the bits of `rows`. The bitmap is rebuilt rather than changed in
    /// place, so a published snapshot keeps seeing the old one.
    pub(super) fn kill(&mut self, rows: &[usize]) {
        let mut bits = BooleanBufferBuilder::new(self.rows());
        bits.append_buffer(&self.live);
        for &row in rows {
            bits.set_bit(row, false);
        }
        self.live = bits.finish();
        self.live_rows = self.live.count_set_bits();
    }

    /// Up to `n` live rows with the lowest seqno, i.e. the oldest writes.
    pub(super) fn oldest_live_rows(&self, n: usize) -> Vec<usize> {
        let Some(seqno) = self
            .batch
            .column_by_name(ORDER_COLUMN)
            .and_then(|c| c.as_primitive_opt::<UInt64Type>())
        else {
            return Vec::new();
        };
        let mut live: Vec<(u64, usize)> = self
            .live
            .set_indices()
            .map(|row| (seqno.value(row), row))
            .collect();
        if live.len() > n {
            live.select_nth_unstable(n);
            live.truncate(n);
        }
        live.into_iter().map(|(_, row)| row).collect()
    }

    /// The batch with `_live` appended. The data columns are the part's own
    /// buffers (shared, not copied); filter on `_live` for current rows.
    pub(super) fn with_live_column(&self) -> Result<RecordBatch, ArrowError> {
        let mut fields: Vec<FieldRef> = self.batch.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(LIVE_COLUMN, DataType::Boolean, false)));
        let mut columns = self.batch.columns().to_vec();
        columns.push(Arc::new(BooleanArray::new(self.live.clone(), None)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}
