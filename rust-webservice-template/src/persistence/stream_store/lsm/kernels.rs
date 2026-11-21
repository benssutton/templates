//! Arrow compute kernels for the LSM store: merge, tombstone removal, compaction, schema checks.

use super::{DELETE_OP, OP_COLUMN, ORDER_COLUMN, StoreError};
use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow::compute::kernels::cmp::neq;
use arrow::compute::{
    SortColumn, SortOptions, cast, concat_batches, filter_record_batch, lexsort_to_indices,
    sort_to_indices, take_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use std::sync::Arc;

/// `run` (compacted: one row per key, sorted by key) cut down to its
/// `max_rows` most recently written rows, still sorted by key; and how many
/// rows were dropped.
pub(super) fn keep_newest(
    run: &RecordBatch,
    max_rows: usize,
    key_columns: &[String],
) -> Result<(RecordBatch, usize), StoreError> {
    let rows = run.num_rows();
    if rows <= max_rows {
        return Ok((run.clone(), 0));
    }
    let newest_first = sort_to_indices(
        &column(run, ORDER_COLUMN)?,
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
        Some(max_rows),
    )?;
    let kept = take_record_batch(run, &newest_first)?;
    Ok((
        sort_by_key_newest_first(&kept, key_columns)?,
        rows - max_rows,
    ))
}

/// Newest version of every key across `frames`, tombstones included, sorted
/// by key ascending: `lexsort_to_indices` sorts on (key asc, seqno desc)
/// and the first row of each run of equal keys is kept.
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
/// dropped as well.
pub fn live(batch: &RecordBatch) -> Result<RecordBatch, StoreError> {
    let op = cast(&column(batch, OP_COLUMN)?, &DataType::Utf8)?;
    let keep = neq(&op, &StringArray::new_scalar(DELETE_OP))?;
    Ok(filter_record_batch(batch, &keep)?)
}

/// Full compaction: every run merged into one, with
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
pub(super) fn sorted_run(
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

pub(super) fn column(batch: &RecordBatch, name: &str) -> Result<ArrayRef, StoreError> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| StoreError::Internal(format!("frame has no `{name}` column")))
}

pub(super) fn drop_columns(batch: &RecordBatch, names: &[&str]) -> Result<RecordBatch, StoreError> {
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
/// schema; later batches must match it by name and type. An incoming
/// `seqno` column is ignored: the store assigns the write order.
/// Types are compared with `DataType` equality, so nested field names and
/// nullability are part of schema equality (top-level nullability is not).
/// A batch with duplicate column names is invalid: names address columns.
pub(super) fn conform(
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
            quoted_list(&duplicates)
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

/// `['id', 'op']`: the same wording the sibling services use.
pub(super) fn quoted_list(names: &[&str]) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    format!("[{}]", quoted.join(", "))
}
